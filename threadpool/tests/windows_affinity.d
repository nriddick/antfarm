module windows_affinity;

version (Windows)
{
import core.atomic : atomicLoad, atomicOp, atomicStore;
import core.sys.windows.basetsd : ULONG_PTR;
import core.sys.windows.winbase : GetCurrentProcess, GetProcessAffinityMask, SetProcessAffinityMask;
import core.thread : Thread;
import core.time : MonoTime, msecs, seconds;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln, writeln;
import threadpool;
import threadpool.pin : PinTarget, pinToLogicalProcessor;
import threadpool.sys.win_bindings : GetProcessGroupAffinity,
    GetCurrentProcessorNumberEx, PROCESSOR_NUMBER;

__gshared shared(ulong) observed;

bool recordCpu(WorkerSelf*) nothrow @nogc @system
{
    PROCESSOR_NUMBER processor;
    GetCurrentProcessorNumberEx(&processor);
    atomicOp!"|="(observed, 1UL << processor.Number);
    return false;
}

void checkPool(PoolOptions options, ulong mask, size_t workers)
{
    atomicStore(observed, 0UL);
    options.workerBody = &recordCpu;
    auto pool = new CacheAwarePool(options);
    pool.start();
    scope(exit) pool.shutdown();
    enforce(pool.workerCount == workers, "worker selection ignored process affinity");
    auto director = pool.director();
    director.spin();
    auto deadline = MonoTime.currTime + 2.seconds;
    while (atomicLoad(observed) != mask && MonoTime.currTime < deadline)
        Thread.sleep(msecs(1));
    auto actual = atomicLoad(observed);
    enforce(actual == mask, "callbacks missed an allowed CPU or executed outside affinity");
    writefln("PASS affinity mask=%x workers=%s observed=%x", mask, workers, actual);
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
    PoolOptions options;
    checkPool(options, mask, allowed.length);

    if (excluded !is null)
    {
        enforce(!pinToLogicalProcessor(PinTarget(excluded.cpuSetId, excluded.group, excluded.lpIndex)),
            "direct pin widened process affinity");
        options.onlyLps = allowed ~ excluded.lpIndex;
        checkPool(options, mask, allowed.length);
        options.onlyLps = [excluded.lpIndex];
        foreach (lp; allowed) options.onlyProcessors ~= ProcessorId(groups[0], lp);
        options.onlyProcessors ~= ProcessorId(excluded.group, excluded.lpIndex);
        checkPool(options, mask, allowed.length);
    }

    if (allowed.length > 1)
    {
        enforce(SetProcessAffinityMask(process, cast(ULONG_PTR)(1UL << allowed[0])) != 0);
        auto stale = new CacheAwarePool;
        bool rejected;
        try { stale.start(); }
        catch (Exception) { rejected = true; }
        scope(exit) stale.shutdown();
        enforce(rejected, "cached topology widened a subsequently narrowed process mask");
        enforce(SetProcessAffinityMask(process, cast(ULONG_PTR) mask) != 0);
        checkPool(PoolOptions.init, mask, allowed.length);
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
