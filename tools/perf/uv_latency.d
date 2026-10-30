/++
 + Round-trip latency through `ant_uv`: from `UvLoop.submit` of a `nop` until
 + the waiter runs again, which covers the request ring, the loop's wake
 + (`uv_async_send`), one loop iteration, publication and the waiter's wake.
 +
 +   make -C tools/perf uv_latency
 +   ./uv_latency --loop-cpu 1 --cpu 2 --samples 20000
 +
 + Three waiters, each timed per request with `MonoTime`:
 + - `thread`: a plain thread pinned to `--cpu` spinning on `done`, the
 +   loop's own cost with no scheduler in the way;
 + - `worker`: a managed Fiber on a `GroupHost` worker parked on a
 +   `FiberEvent` (`submitAndWait`), which adds the event, the lane's
 +   publication and a parked worker's wake; the loop's CPU is kept out of
 +   the worker set, as the design deploys it;
 + - `loop`: a Fiber on the loop thread (`UvFibers`), which resumes where it
 +   issued the request.
 + Each prints p50, p90, p99 and max in microseconds after a warm-up.
 +/
module uv_latency;

import ant_fibers;
import ant_uv;
import core.atomic;
import core.stdc.stdio : printf;
import core.thread : Thread;
import core.time;
import std.algorithm.sorting : sort;
import std.conv : to;
import threadpool.groups : GroupHost, GroupHostOptions;
import threadpool.host : CallerPlacement, HostGroups;
import threadpool.pin : pinProcessor, PinOutcome;
import threadpool.topology : ProcessorId;

enum warmup = 2000;

void report(string name, long[] nanos)
{
    sort(nanos);
    double at(double q) { return nanos[cast(size_t)(q * (nanos.length - 1))] / 1000.0; }
    printf("%-7.*s samples %6zu  p50 %8.2f  p90 %8.2f  p99 %8.2f  max %9.2f us\n",
        cast(int) name.length, name.ptr, nanos.length, at(0.50), at(0.90), at(0.99),
        nanos[$ - 1] / 1000.0);
}

long[] threadWaiter(UvLoop loop, uint cpu, size_t samples)
{
    auto nanos = new long[samples];
    auto t = new Thread(() {
        if (pinProcessor(ProcessorId(0, cast(ushort) cpu)) != PinOutcome.pinned)
            printf("warning: could not pin the submitter to %u\n", cpu);
        UvRequest r;
        r.op = UvOp.nop;
        foreach (i; 0 .. warmup + samples)
        {
            immutable t0 = MonoTime.currTime;
            while (loop.submit(&r) != UvSubmit.accepted) {}
            while (!r.done) core.atomic.pause();
            if (i >= warmup) nanos[i - warmup] = (MonoTime.currTime - t0).total!"nsecs";
        }
    });
    t.start();
    t.join();
    return nanos;
}

long[] workerWaiter(UvLoop loop, uint loopCpu, size_t samples)
{
    import threadpool.pool : CacheAwarePool;
    auto nanos = new long[samples];
    auto domain = new FiberDomain;
    auto fibers = new FiberGroups(domain);
    GroupHostOptions o;
    o.caller = CallerPlacement.unchanged;
    o.attachments = [fibers.attachment];
    // The deployment shape: the loop's CPU is kept out of the worker set.
    const snap = CacheAwarePool.topology();
    ProcessorId[] workerCpus;
    foreach (ref p; snap.processors)
        if (snap.isEligible(p) && p.lpIndex != loopCpu) workerCpus ~= ProcessorId.of(p);
    o.pool.processors = workerCpus;
    auto host = new GroupHost(o);
    fibers.spawn(() {
        UvRequest r;
        r.op = UvOp.nop;
        r.event = new FiberEvent(domain);
        foreach (i; 0 .. warmup + samples)
        {
            immutable t0 = MonoTime.currTime;
            submitAndWait(loop, &r);
            if (i >= warmup) nanos[i - warmup] = (MonoTime.currTime - t0).total!"nsecs";
        }
    });
    while (!domain.drained) Thread.sleep(1.msecs);
    cast(void) domain.retire();
    host.close();
    domain.beginShutdown();
    return nanos;
}

int main(string[] args)
{
    uint loopCpu = 1, cpu = 2;
    size_t samples = 20_000;
    for (size_t i = 1; i + 1 < args.length; i += 2)
    {
        if (args[i] == "--loop-cpu") loopCpu = args[i + 1].to!uint;
        else if (args[i] == "--cpu") cpu = args[i + 1].to!uint;
        else if (args[i] == "--samples") samples = args[i + 1].to!size_t;
    }

    UvLoopOptions o;
    o.cpu = ProcessorId(0, cast(ushort) loopCpu);
    auto loop = UvLoop.create(o);
    report("thread", threadWaiter(loop, cpu, samples));
    report("worker", workerWaiter(loop, loopCpu, samples));
    loop.close();

    // A Fiber on the loop thread, timed inside the Fiber.
    auto nanos = new long[samples];
    shared bool finished;
    UvFibers fibers;
    o.start = () {
        auto self = UvLoop.current;
        fibers = new UvFibers(self);
        fibers.spawn(() {
            UvRequest r;
            r.op = UvOp.nop;
            r.event = new FiberEvent(fibers.domain);
            foreach (i; 0 .. warmup + samples)
            {
                immutable t0 = MonoTime.currTime;
                submitAndWait(self, &r);
                if (i >= warmup) nanos[i - warmup] = (MonoTime.currTime - t0).total!"nsecs";
            }
            atomicStore(finished, true);
        });
    };
    o.step = () => fibers.step();
    o.stop = () { fibers.shutdown(); };
    loop = UvLoop.create(o);
    while (!atomicLoad(finished)) Thread.sleep(1.msecs);
    loop.close();
    report("loop", nanos);
    return 0;
}
