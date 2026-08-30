module benchmark;

import antfarm;
import antfarm_fibers;
import core.atomic;
import core.time : MonoTime;
import std.conv : to;
import std.stdio : writefln;

shared long ran;

void main(string[] args)
{
    immutable taskCount = args.length > 1 ? args[1].to!size_t : 1_000;
    immutable flushMax = args.length > 2 ? args[2].to!size_t : 256;
    immutable avgCost = args.length > 3 ? args[3].to!uint : 0;
    immutable stackSize = args.length > 4 ? args[4].to!size_t : 0;
    // 0 = burst-spawn then drain (split create/run). 1 = flush during spawn
    // so Farm write overlaps the burst; still drains after.
    immutable pipeline = args.length > 5 ? args[5].to!int : 0;

    auto farm = AntFarm.create(1 << 20, 8, 1, 0, 0, 1, 16_384);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    backend.reserve(taskCount);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView consumer;
    auto subscribed = subscribeOrThrow(consumer, farm);
    scope (exit) consumer.unsubscribe();

    writefln("config: n=%s flush=%s avgCost=%s stack=%s pipeline=%s hugePages=%s",
             taskCount, flushMax, avgCost, stackSize, pipeline, farm.usedLargePages);

    void burst(bool recycle)
    {
        if (recycle)
            backend.releaseAll(backend.takeCompletions());
        auto createStart = MonoTime.currTime;
        foreach (_; 0 .. taskCount)
        {
            backend.spawn({ atomicFetchAdd(ran, 1L); }, stackSize);
            if (pipeline && backend.ready >= flushMax)
                backend.flush(token, flushMax, avgCost);
        }
        auto runStart = MonoTime.currTime;
        drainUntilEmpty(backend, token, consumer, flushMax, avgCost);
        auto end = MonoTime.currTime;
        auto createSeconds = cast(double)(runStart - createStart).total!("nsecs")
            / 1_000_000_000.0;
        auto runSeconds = cast(double)(end - runStart).total!("nsecs")
            / 1_000_000_000.0;
        immutable label = recycle ? " (warm pool)" : "";
        writefln("create%s: %s fibers in %.6f s: %.2f fibers/s",
                 label, taskCount, createSeconds, taskCount / createSeconds);
        writefln("run   %s: %s fibers in %.6f s: %.2f fibers/s",
                 label, taskCount, runSeconds, taskCount / runSeconds);
        writefln("total %s: %.2f fibers/s",
                 label, taskCount / (createSeconds + runSeconds));
    }

    burst(false);
    burst(true);
}
