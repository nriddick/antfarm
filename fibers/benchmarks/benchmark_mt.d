/++ Multithreaded fiber throughput benchmark.
 +
 + One FiberLane per accepted LLC group, workers driven by the threadpool
 + managed-worker hooks. The control thread burst-spawns round-robin across lanes (waking the
 + pool once per burst — per-spawn wakeAll would measure futex traffic, not
 + the scheduler), then waits for every lane to drain. Fibers never suspend,
 + so no cross-worker fiber migration occurs.
 +
 + Second generation re-spawns from the recycling pool: zero GC allocation,
 + zero stack mmap per fiber.
 +
 + Usage: benchmark_mt [tasks] [private|shared|assist]
 +   private  (default) one domain per lane;
 +   shared   one domain over every lane, native service only (no edges);
 +   assist   one domain plus an explicit `assistRing` over the accepted
 +            groups, the former automatic ring: the A/B against `shared`.
 +/
// Package scope permits wake-batched admission to an explicit shared-domain
// lane, without timing a per-task FiberLane.spawn wake broadcast.
module antfarm_fibers.benchmark_mt;

import antfarm;
import antfarm_fibers;
import core.atomic;
import core.thread : Thread;
import core.time : Duration, MonoTime;
import std.conv : to;
import std.exception : enforce;
import std.stdio : writefln;
import threadpool;

shared long ran;

/// Print one timed phase. A phase whose work had already finished when its
/// timer started, or a zero interval, has no rate: report it as such
/// instead of dividing by (almost) nothing.
void report(string label, size_t count, Duration elapsed, bool measurable = true)
{
    immutable seconds = cast(double) elapsed.total!"nsecs" / 1_000_000_000.0;
    if (!measurable || seconds == 0)
        writefln("%s %s fibers: not measurable (finished before the timer started)",
                 label, count);
    else
        writefln("%s %s fibers in %.6f s: %.2f fibers/s",
                 label, count, seconds, count / seconds);
}

void main(string[] args)
{
    immutable taskCount = args.length > 1 ? args[1].to!size_t : 100_000;

    immutable mode = args.length > 2 ? args[2] : "private";
    enforce(mode == "private" || mode == "shared" || mode == "assist",
        "usage: benchmark_mt [tasks] [private|shared|assist]");
    immutable sharedDomain = mode != "private";
    auto domain = sharedDomain ? new FiberDomain : null;

    PoolOptions options;
    options.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(options);
    auto plan = new FiberServicePlan(pool.resolve());
    ushort[] groups;
    foreach (ref group; plan.resolution.acceptedGroups)
        groups ~= group.llcIndex;
    // Assist edges add persistent remote consumers and producers.
    if (mode == "assist")
        plan.assistRing(groups);

    // Size each Farm from the plan: one consumer view and one small producer
    // per native worker, plus one of each per assisting worker.
    enum ln = 1 << 22;
    enum k = 8;
    enum segCap = ln / k;
    AntFarm*[] farms;
    FiberLane[] lanes;
    scope (exit)
    {
        uninstallFiberPlan();
        foreach (farm; farms) farm.destroy();
    }
    foreach (group; groups)
    {
        auto requirement = plan.requirement(group);
        immutable slots = requirement.smallProducerSlots;
        immutable quotaHeadroom = (k - 1) * segCap / slots;
        immutable quotaSmall = quotaHeadroom > 16_384 ? 16_384 : quotaHeadroom;
        auto farm = AntFarm.create(ringMiBFromUlongs(ln), k,
                                   requirement.consumerViews, 0, 0,
                                   slots, quotaSmall);
        farms ~= farm;
        lanes ~= sharedDomain ? new FiberLane(domain, farm) : new FiberLane(farm);
        plan.bind(group, lanes[$ - 1]);
    }

    installFiberPlan(plan, pool);
    pool.start();
    if (sharedDomain)
        domain.reserve(taskCount);
    else
        foreach (lane; lanes)
            lane.domain.reserve((taskCount + lanes.length - 1) / lanes.length);
    writefln("workers: %s across %s LLC group(s) hugePages=%s mode=%s edges=%s",
             pool.workerCount, lanes.length, farms[0].usedLargePages, mode,
             plan.edges.length);

    void burstSpawn()
    {
        void function() body = { atomicFetchAdd(ran, 1L); };
        foreach (i; 0 .. taskCount)
        {
            auto lane = lanes[i % lanes.length];
            lane.domain.spawnOn(lane, body);
        }
        pool.wakeAll();
    }

    bool allDrained()
    {
        foreach (lane; lanes)
            if (!lane.drained) return false;
        return true;
    }

    void waitDrained()
    {
        while (!allDrained())
            Thread.yield();
    }

    // Workers may drain the burst while it is still being spawned; then the
    // run phase measured nothing.
    auto createStart = MonoTime.currTime;
    burstSpawn();
    auto runStart = MonoTime.currTime;
    immutable runMeasurable = !allDrained();
    waitDrained();
    auto end = MonoTime.currTime;
    report("create:", taskCount, runStart - createStart);
    report("run:   ", taskCount, end - runStart, runMeasurable);

    // Second generation: every spawn recycles a pooled task/fiber/stack.
    foreach (lane; lanes)
        lane.domain.releaseAll(lane.domain.takeCompletions());
    auto warmCreateStart = MonoTime.currTime;
    burstSpawn();
    auto warmRunStart = MonoTime.currTime;
    immutable warmRunMeasurable = !allDrained();
    waitDrained();
    auto warmEnd = MonoTime.currTime;
    report("create (warm pool):", taskCount, warmRunStart - warmCreateStart);
    report("run    (warm pool):", taskCount, warmEnd - warmRunStart,
           warmRunMeasurable);

    assert(atomicLoad(ran) == 2 * taskCount, "fiber executions lost");
    pool.shutdown();
}
