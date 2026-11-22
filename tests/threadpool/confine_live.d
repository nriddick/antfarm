module confine_live;

// Live caller-affinity and pool-stop regression (`dub run -c confine-test`).
// Windows and Linux; on Linux also run it under `taskset -c 0-3`.
//
// confineCallingThread narrows the calling thread to two OS-scope CPUs and
// restoreCallingThread puts back the exact mask it captured. stopRequested
// turns true when a managed pump throws, and is false on an unstarted or
// shut-down pool. Every worker placement is started in turn and each worker
// reads its own affinity back: the whole OS scope when unconfined (even when
// the thread that starts the pool is narrowed), its set under a group mask,
// one CPU when pinned.

version (linux) version = ConfineTestOs;
version (Windows) version = ConfineTestOs;

version (ConfineTestOs)
{
import core.atomic : atomicLoad, atomicOp, atomicStore, cas;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.exception : enforce;
import std.stdio : writeln;

import threadpool;
import threadpool.pin : ThreadAffinity, confineCallingThread, restoreCallingThread;

__gshared shared(int) gThrown;

ManagedPumpResult throwOnce(WorkerSelf*)
{
    if (cas(&gThrown, 0, 1))
        throw new Exception("confine-test: injected pump failure");
    return ManagedPumpResult.idle();
}

ProcessorId[] scopeCpus()
{
    auto snap = discover();
    ProcessorId[] eligible;
    foreach (ref p; snap.processors)
        if (snap.isEligible(p))
            eligible ~= ProcessorId.of(p);
    // Windows confinement takes one group. Prefer the caller's current group
    // so this probe also works on machines with several processor groups.
    version (Windows)
    {
        auto current = callerMask();
        if (current.length == 0) return null;
        ProcessorId[] sameGroup;
        foreach (id; eligible)
            if (id.group == current[0].group) sameGroup ~= id;
        return sameGroup;
    }
    return eligible;
}

ProcessorId[] callerMask()
{
    version (Windows)
    {
        import core.sys.windows.winbase : GetCurrentThread;
        import threadpool.sys.win_bindings : GROUP_AFFINITY, GetThreadGroupAffinity;
        GROUP_AFFINITY affinity;
        enforce(GetThreadGroupAffinity(GetCurrentThread(), &affinity) != 0,
            "query caller affinity");
        ProcessorId[] result;
        foreach (ushort lp; 0 .. 64)
            if (affinity.Mask & (1UL << lp))
                result ~= ProcessorId(affinity.Group, lp);
        return result;
    }
    else return liveOsMask();
}

void confineAndRestore(const(ProcessorId)[] cpus)
{
    auto before = callerMask();
    auto processBefore = liveOsMask();
    auto target = cpus[0 .. 2];
    ThreadAffinity previous;
    enforce(confineCallingThread(target, previous) == PinOutcome.pinned, "confine two CPUs");
    scope (exit) enforce(restoreCallingThread(previous), "restore on failure");
    enforce(previous.captured, "previous affinity captured");
    enforce(callerMask() == target, "caller mask is the confined set: " ~ formatCpuList(callerMask()));
    version (Windows)
        enforce(liveOsMask() == processBefore, "caller confinement leaves process affinity unchanged");
    enforce(restoreCallingThread(previous), "restore");
    enforce(callerMask() == before, "restored mask equals the original: " ~ formatCpuList(callerMask()));

    ThreadAffinity untouched;
    enforce(confineCallingThread(null, untouched) == PinOutcome.unknownProcessor
        && !untouched.captured, "empty set rejected before capture");
    enforce(confineCallingThread([ProcessorId(0, ushort.max)], untouched) == PinOutcome.unknownProcessor
        && !untouched.captured, "unknown CPU rejected before capture");
    enforce(!restoreCallingThread(untouched), "restore without capture fails");

    // Under taskset: a discovered CPU outside the frozen scope.
    auto snap = discover();
    foreach (ref p; snap.processors)
    {
        auto id = ProcessorId.of(p);
        if (snap.inOsScope(id)) continue;
        enforce(confineCallingThread([cpus[0], id], untouched) == PinOutcome.outsideOsScope
            && !untouched.captured, "out-of-scope CPU rejected before capture");
        writeln("PASS confine rejects out-of-scope ", formatCpuList([id]));
        break;
    }
    enforce(callerMask() == before, "rejections leave the mask alone");
    writeln("PASS confine ", formatCpuList(target), " and restore ", formatCpuList(before));
}

__gshared ProcessorId[][] gWorkerMasks; // index == workerIndex
__gshared shared(int) gMasksRead;

void readOwnMask(WorkerSelf* w)
{
    gWorkerMasks[w.workerIndex] = callerMask();
    atomicOp!"+="(gMasksRead, 1);
}

ManagedPumpResult idlePump(WorkerSelf*)
{
    return ManagedPumpResult.idle();
}

void placements(const(ProcessorId)[] cpus)
{
    import std.conv : to;

    // Up to four CPUs of one physical group.
    auto snap = discover();
    immutable llc = snap.byId(cpus[0]).llcIndex;
    ProcessorId[] set;
    foreach (id; cpus)
        if (snap.byId(id).llcIndex == llc && set.length < 4) set ~= id;
    immutable workers = autoWorkersOf(set.length);
    auto unconfined = callerMask();
    foreach (placement; [WorkerPlacement.free, WorkerPlacement.group, WorkerPlacement.pin])
    {
        PoolOptions options;
        options.processors = set;
        options.placement = placement;
        options.managedWorker = ManagedWorkerHooks(&readOwnMask, &idlePump, null, null);
        gWorkerMasks = new ProcessorId[][](workers);
        atomicStore(gMasksRead, 0);
        auto pool = new CacheAwarePool(options);
        // A host narrows its caller before it starts the pool. That must not
        // leak into the workers: start with the caller on one CPU.
        ThreadAffinity previous;
        enforce(confineCallingThread(set[0 .. 1], previous) == PinOutcome.pinned, "narrow the caller");
        {
            scope (exit) enforce(restoreCallingThread(previous), "restore the caller");
            pool.start();
        }
        scope (exit) pool.shutdown();
        enforce(pool.workerCount == workers && pool.resolution.started
            && pool.resolution.placement == placement, "started " ~ pool.resolution.summary());
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad(gMasksRead) < workers)
        {
            enforce(MonoTime.currTime < deadline, "workers never reported their affinity");
            Thread.yield();
        }
        foreach (i, mask; gWorkerMasks)
        {
            const(ProcessorId)[] expected = unconfined;
            if (placement == WorkerPlacement.group) expected = set;
            if (placement == WorkerPlacement.pin)
            {
                enforce(pool.resolution.workers[i].pinned
                    && pool.resolution.workers[i].processor == set[i], "worker i takes the set's CPU i");
                expected = set[i .. i + 1];
            }
            enforce(mask == expected, "placement " ~ placement.to!string ~ ": worker " ~ i.to!string
                ~ " has " ~ formatCpuList(mask) ~ ", expected " ~ formatCpuList(expected));
        }
        writeln("PASS placement ", placement, ": ", workers, " workers of {", formatCpuList(set), "} each on ",
            placement == WorkerPlacement.pin ? "its own CPU"
                : formatCpuList(placement == WorkerPlacement.group ? set : unconfined));
    }
}

void stopRequestedOnFailure(ProcessorId cpu)
{
    PoolOptions options;
    options.processors = [cpu];
    options.managedWorker = ManagedWorkerHooks(null, &throwOnce, null, null);
    auto pool = new CacheAwarePool(options);
    enforce(!pool.stopRequested, "unstarted pool");
    pool.start();
    auto other = new CacheAwarePool();
    auto deadline = MonoTime.currTime + 10.seconds;
    while (!pool.stopRequested)
    {
        enforce(MonoTime.currTime < deadline, "stopRequested never turned true");
        Thread.yield();
    }
    enforce(!other.stopRequested, "a pool that is not live");
    pool.shutdown();
    enforce(!pool.stopRequested, "shut-down pool");
    auto failures = pool.workerFailures();
    enforce(failures.length == 1 && failures[0].msg == "confine-test: injected pump failure",
        "the injected failure is reported");
    writeln("PASS stopRequested after a managed pump failure");
}

int main()
{
    auto cpus = scopeCpus();
    if (cpus.length < 2)
    {
        writeln("SKIP confine-test: needs two CPUs in one processor group in the OS scope");
        return 0;
    }
    confineAndRestore(cpus);
    placements(cpus);
    stopRequestedOnFailure(cpus[0]);
    return 0;
}
}
else
{
import std.stdio : writeln;

int main()
{
    writeln("SKIP confine-test: Windows and Linux only");
    return 0;
}
}
