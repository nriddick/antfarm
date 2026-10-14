module confine_live;

// Live caller-affinity and pool-stop regression (`dub run -c confine-test`).
// Linux only; also run it under `taskset -c 0-3`.
//
// confineCallingThread narrows the calling thread to two OS-scope CPUs and
// restoreCallingThread puts back the exact mask it captured. stopRequested
// turns true when a managed pump throws, and is false on an unstarted or
// shut-down pool.

version (linux)
{
import core.atomic : atomicLoad, cas;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.exception : enforce;
import std.stdio : writeln;

import threadpool;
import threadpool.pin : ThreadAffinity, confineCallingThread, restoreCallingThread;

shared int gThrown;

ManagedPumpResult throwOnce(WorkerSelf*)
{
    if (cas(&gThrown, 0, 1))
        throw new Exception("confine-test: injected pump failure");
    return ManagedPumpResult.idle();
}

ProcessorId[] scopeCpus()
{
    auto snap = discover();
    if (snap.osScopeSource != OsScopeSource.unrestricted)
        return snap.osScopeAllowed.dup;
    ProcessorId[] eligible;
    foreach (ref p; snap.processors)
        if (snap.isEligible(p))
            eligible ~= ProcessorId.of(p);
    return eligible;
}

void confineAndRestore(const(ProcessorId)[] cpus)
{
    auto before = liveOsMask();
    auto target = cpus[0 .. 2];
    ThreadAffinity previous;
    enforce(confineCallingThread(target, previous) == PinOutcome.pinned, "confine two CPUs");
    enforce(previous.captured, "previous affinity captured");
    enforce(liveOsMask() == target, "live mask is the confined set: " ~ formatCpuList(liveOsMask()));
    enforce(restoreCallingThread(previous), "restore");
    enforce(liveOsMask() == before, "restored mask equals the original: " ~ formatCpuList(liveOsMask()));

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
    enforce(liveOsMask() == before, "rejections leave the mask alone");
    writeln("PASS confine ", formatCpuList(target), " and restore ", formatCpuList(before));
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
        writeln("SKIP confine-test: needs two CPUs in the OS scope");
        return 0;
    }
    confineAndRestore(cpus);
    stopRequestedOnFailure(cpus[0]);
    return 0;
}
}
else
{
import std.stdio : writeln;

int main()
{
    writeln("SKIP confine-test: Linux only");
    return 0;
}
}
