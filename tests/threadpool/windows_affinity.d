module windows_affinity;

// Live Windows process-affinity regression (`dub run -c affinity-test`). The
// process is narrowed to two CPUs before discovery; every placement must
// then keep its workers inside them, and each worker reads its own affinity
// back: the whole process mask when unconfined, the set under a group mask,
// one CPU when pinned.

version (Windows)
{
import core.atomic : atomicLoad, atomicOp, atomicStore;
import core.sys.windows.basetsd : ULONG_PTR;
import core.sys.windows.winbase : GetCurrentProcess, GetCurrentThread, GetProcessAffinityMask,
    SetProcessAffinityMask;
import core.thread : Thread;
import core.time : MonoTime, msecs, seconds;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln, writeln;
import threadpool;
import threadpool.sys.win_bindings : GetProcessGroupAffinity, GetThreadGroupAffinity,
    GetCurrentProcessorNumberEx, GROUP_AFFINITY, PROCESSOR_NUMBER;

__gshared shared(ulong) observed;
/// Each worker's own affinity mask, read on the worker.
__gshared shared(ulong)[64] workerMask;

bool recordCpu(WorkerSelf* w) nothrow @nogc @system
{
    PROCESSOR_NUMBER processor;
    GetCurrentProcessorNumberEx(&processor);
    atomicOp!"|="(observed, 1UL << processor.Number);
    GROUP_AFFINITY affinity;
    if (w.workerIndex < workerMask.length && GetThreadGroupAffinity(GetCurrentThread(), &affinity))
        atomicStore(workerMask[w.workerIndex], cast(ulong) affinity.Mask);
    return false;
}

/// Start a pool with `options`, and check its worker count, where its
/// callbacks ran (`ranOn`) and what affinity each worker has.
void checkPool(PoolOptions options, ulong setMask, ulong ranOn, size_t workers)
{
    atomicStore(observed, 0UL);
    foreach (ref m; workerMask) atomicStore(m, 0UL);
    options.workerBody = &recordCpu;
    auto pool = new CacheAwarePool(options);
    pool.start();
    scope(exit) pool.shutdown();
    enforce(pool.workerCount == workers, "worker selection ignored process affinity");
    immutable placement = pool.resolution.placement;
    auto director = pool.director();
    director.spin();
    auto deadline = MonoTime.currTime + 2.seconds;
    bool complete()
    {
        if (atomicLoad(observed) != ranOn) return false;
        foreach (i; 0 .. workers)
            if (atomicLoad(workerMask[i]) == 0) return false;
        return true;
    }
    while (!complete() && MonoTime.currTime < deadline)
        Thread.sleep(msecs(1));
    auto actual = atomicLoad(observed);
    enforce(actual == ranOn, "callbacks missed an allowed CPU or executed outside affinity");

    ULONG_PTR processMask, system;
    enforce(GetProcessAffinityMask(GetCurrentProcess(), &processMask, &system) != 0);
    ulong pinnedUnion;
    foreach (i; 0 .. workers)
    {
        immutable mask = atomicLoad(workerMask[i]);
        final switch (placement)
        {
            case WorkerPlacement.automatic:
            case WorkerPlacement.free:
                enforce(mask == processMask, "an unconfined worker does not have the process mask");
                break;
            case WorkerPlacement.group:
                enforce(mask == setMask, "a group worker's mask is not its set");
                break;
            case WorkerPlacement.pin:
                enforce(mask != 0 && (mask & (mask - 1)) == 0 && (mask & setMask) == mask
                    && (pinnedUnion & mask) == 0, "a pinned worker is not alone on one CPU of its set");
                pinnedUnion |= mask;
                break;
        }
    }
    writefln("PASS affinity mask=%x workers=%s placement=%s observed=%x", setMask, workers, placement, actual);
}

void main(string[] args)
{
    auto process = GetCurrentProcess();
    ushort[64] groups;
    ushort count = cast(ushort) groups.length;
    enforce(GetProcessGroupAffinity(process, &count, groups.ptr) != 0);
    if (count != 1)
    {
        writeln("SKIP affinity regression: requires a single processor group");
        return;
    }
    ULONG_PTR original, system;
    enforce(GetProcessAffinityMask(process, &original, &system) != 0 && original != 0);
    ulong mask;
    uint selectedCount;
    foreach (bit; 0 .. 64)
        if ((original & (1UL << bit)) && selectedCount < 2)
        {
            mask |= 1UL << bit;
            ++selectedCount;
        }
    if (args.length > 1) mask = to!ulong(args[1]);
    enforce(mask != 0 && (mask & ~original) == 0, "requested mask is not initially allowed");
    enforce(SetProcessAffinityMask(process, cast(ULONG_PTR) mask) != 0);
    scope(exit) enforce(SetProcessAffinityMask(process, original) != 0);

    auto snapshot = CacheAwarePool.topology();
    ushort[] allowed;
    const(LogicalProcessor)* excluded;
    foreach (ref cpu; snapshot.processors)
        if (cpu.group == groups[0] && (mask & (1UL << cpu.lpIndex)))
        {
            enforce(!cpu.affinityExcluded);
            allowed ~= cpu.lpIndex;
        }
        else
        {
            enforce(cpu.affinityExcluded);
            excluded = &cpu;
        }
    enforce(allowed.length != 0);
    // The count is of the CPUs the process may use: two leave none unstaffed.
    immutable workers = autoWorkersOf(allowed.length);
    static immutable placements = [WorkerPlacement.automatic, WorkerPlacement.free,
        WorkerPlacement.group, WorkerPlacement.pin];
    foreach (placement; placements)
    {
        PoolOptions options;
        options.placement = placement;
        checkPool(options, mask, mask, workers);
    }

    if (excluded !is null)
    {
        enforce(pinProcessor(*excluded) == PinOutcome.outsideOsScope,
            "direct pin widened the frozen OS scope");
        foreach (placement; placements)
        {
            PoolOptions options;
            options.placement = placement;
            foreach (lp; allowed) options.processors ~= ProcessorId(groups[0], lp);
            options.processors ~= ProcessorId(excluded.group, excluded.lpIndex);
            checkPool(options, mask, mask, workers);
        }
    }

    if (allowed.length > 1)
    {
        immutable live = 1UL << allowed[0];
        enforce(SetProcessAffinityMask(process, cast(ULONG_PTR) live) != 0);
        // Still inside the frozen scope, but the live OS mask refuses it.
        enforce(pinProcessor(ProcessorId(groups[0], allowed[1])) == PinOutcome.osRejected,
            "live process-mask narrowing was not reported as osRejected");
        // A mask or a pin that the live process mask no longer allows is
        // refused: the pool never widens a narrowed process.
        foreach (placement; [WorkerPlacement.group, WorkerPlacement.pin])
        {
            PoolOptions options;
            options.placement = placement;
            auto stale = new CacheAwarePool(options);
            bool rejected;
            try { stale.start(); }
            catch (Exception) { rejected = true; }
            stale.shutdown();
            enforce(rejected, "cached topology widened a subsequently narrowed process mask ("
                ~ placement.to!string ~ ")");
        }
        // Unconfined workers ask the OS for nothing, so they start, and run
        // on the one CPU the process still has.
        PoolOptions unconfined;
        unconfined.placement = WorkerPlacement.free;
        checkPool(unconfined, mask, live, workers);
        writeln("PASS affinity: a narrowed process refuses masks and pins, and holds unconfined workers");
        enforce(SetProcessAffinityMask(process, cast(ULONG_PTR) mask) != 0);
        checkPool(PoolOptions.init, mask, mask, workers);
    }
}
}
else
{
    void main()
    {
        import std.stdio : writeln;
        writeln("SKIP Windows process-affinity regression");
    }
}
