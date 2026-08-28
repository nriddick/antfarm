/++ Multithreaded fiber throughput benchmark.
 +
 + One FiberLane per LLC, workers driven by the threadpool managed-worker
 + hooks. The control thread burst-spawns round-robin across lanes (waking the
 + pool once per burst — per-spawn wakeAll would measure futex traffic, not
 + the scheduler), then waits for every lane to drain. Fibers never suspend,
 + so no cross-worker fiber migration occurs.
 +
 + Second generation re-spawns from the recycling pool: zero GC allocation,
 + zero stack mmap per fiber.
 +/
module benchmark_mt;

import antfarm;
import antfarm_fibers;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime;
import std.conv : to;
import std.stdio : writefln;
import threadpool;

shared long ran;

void main(string[] args)
{
    immutable taskCount = args.length > 1 ? args[1].to!size_t : 100_000;

    auto topology = CacheAwarePool.topology();
    auto perLlc = new size_t[topology.llcCount];
    foreach (ref lp; topology.processors)
    {
        version (linux)
            if (lp.parkedAtDiscovery) continue;
        perLlc[lp.llcIndex]++;
    }

    // Each worker on an LLC subscribes one consumer and registers one small
    // producer against its lane's farm; size both slot sets accordingly.
    enum ln = 1 << 22;
    enum k = 8;
    enum segCap = ln / k;
    AntFarm*[] farms;
    FiberLane[] lanes;
    foreach (i; 0 .. topology.llcCount)
    {
        immutable slots = perLlc[i] == 0 ? 1 : perLlc[i];
        immutable quotaHeadroom = (k - 1) * segCap / slots;
        immutable quotaSmall = quotaHeadroom > 16_384 ? 16_384 : quotaHeadroom;
        auto farm = AntFarm.create(ln, k, cast(uint) slots, 0, 0,
                                   cast(uint) slots, quotaSmall);
        farms ~= farm;
        lanes ~= new FiberLane(farm);
    }
    scope (exit)
    {
        uninstallFiberLanes();
        foreach (farm; farms) farm.destroy();
    }

    PoolOptions options;
    options.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(options);
    installFiberLanes(lanes, pool);
    pool.start();
    foreach (lane; lanes)
        lane.backend.reserve((taskCount + lanes.length - 1) / lanes.length);
    writefln("workers: %s across %s LLC(s) hugePages=%s",
             pool.workerCount, lanes.length, farms[0].usedLargePages);

    void burstSpawn()
    {
        foreach (i; 0 .. taskCount)
            lanes[i % lanes.length].backend.spawn({ atomicFetchAdd(ran, 1L); });
        pool.wakeAll();
    }

    void waitDrained()
    {
        for (;;)
        {
            bool allDrained = true;
            foreach (lane; lanes)
                if (!lane.drained) { allDrained = false; break; }
            if (allDrained) return;
            Thread.yield();
        }
    }

    auto createStart = MonoTime.currTime;
    burstSpawn();
    auto runStart = MonoTime.currTime;
    waitDrained();
    auto end = MonoTime.currTime;
    auto createSeconds = cast(double)(runStart - createStart).total!("nsecs")
        / 1_000_000_000.0;
    auto runSeconds = cast(double)(end - runStart).total!("nsecs")
        / 1_000_000_000.0;
    writefln("create: %s fibers in %.6f s: %.2f fibers/s",
             taskCount, createSeconds, taskCount / createSeconds);
    writefln("run:    %s fibers in %.6f s: %.2f fibers/s",
             taskCount, runSeconds, taskCount / runSeconds);

    // Second generation: every spawn recycles a pooled task/fiber/stack.
    foreach (lane; lanes)
        lane.backend.releaseAll(lane.backend.takeCompletions());
    auto warmCreateStart = MonoTime.currTime;
    burstSpawn();
    auto warmRunStart = MonoTime.currTime;
    waitDrained();
    auto warmEnd = MonoTime.currTime;
    auto warmCreateSeconds = cast(double)(warmRunStart - warmCreateStart)
        .total!("nsecs") / 1_000_000_000.0;
    auto warmRunSeconds = cast(double)(warmEnd - warmRunStart)
        .total!("nsecs") / 1_000_000_000.0;
    writefln("create (warm pool): %s fibers in %.6f s: %.2f fibers/s",
             taskCount, warmCreateSeconds, taskCount / warmCreateSeconds);
    writefln("run    (warm pool): %s fibers in %.6f s: %.2f fibers/s",
             taskCount, warmRunSeconds, taskCount / warmRunSeconds);

    assert(atomicLoad(ran) == 2 * taskCount, "fiber executions lost");
    pool.shutdown();
}
