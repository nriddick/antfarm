module hello_fibers;

import antfarm;
import antfarm_fibers;
import core.atomic;
import core.time : msecs;
import std.stdio : writeln;

shared int steps;

void fiberBody()
{
    atomicFetchAdd(steps, 1);
    FiberDomain.yieldReady();
    atomicFetchAdd(steps, 1);
    FiberDomain.sleepFor(msecs(1));
    atomicFetchAdd(steps, 1);
}

void main()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();

    auto domain = new FiberDomain(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    ConsumerView consumer;
    subscribeOrThrow(consumer, farm);
    scope (exit) consumer.unsubscribe();

    auto task = domain.spawn(&fiberBody);
    drainUntilEmpty(domain, token, consumer, 256, 0);

    assert(task.outcome == FiberOutcome.completed);
    assert(atomicLoad(steps) == 3);
    domain.releaseAll(domain.takeCompletions());
    writeln("fiber yielded, slept, and completed");
}
