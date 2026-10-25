module hello_fibers;

import antfarm;
import ant_fibers;
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
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();

    // A terminal task is handed to onCompletion, then recycled.
    FiberOutcome outcome;
    FiberDomainOptions options;
    options.onCompletion = (FiberTask task) { outcome = task.outcome; };
    auto domain = new FiberDomain(farm, options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    ConsumerView consumer;
    subscribeOrThrow(consumer, farm);
    scope (exit) consumer.unsubscribe();

    domain.spawn(&fiberBody);
    // Runs every task to its end, retiring as it goes.
    drainUntilEmpty(domain, token, consumer, 256, avgCostForChunk(32));

    assert(outcome == FiberOutcome.completed);
    assert(atomicLoad(steps) == 3);
    writeln("fiber yielded, slept, and completed");
}
