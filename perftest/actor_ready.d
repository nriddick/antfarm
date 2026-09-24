/++
 + Autonomous actor backlog benchmark. Build with `make -C perftest actor_ready`.
 + Arguments: actors (default 16384), rounds (100), flush batch (32).
 + Construction and waking are outside the timed region; each round measures
 + flushing a prequeued burst and consuming its callbacks on the same thread.
 +/
module actor_ready;

import actors;
import antfarm : ConsumerView;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.stdio : writefln;

private struct State
{
    ulong* calls;
}

private void activate(scope ref ActorBorrow!State actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++*actor.value.calls;
}

void main(string[] args)
{
    immutable count = args.length > 1 ? to!size_t(args[1]) : 16_384;
    immutable rounds = args.length > 2 ? to!size_t(args[2]) : 100;
    immutable batch = args.length > 3 ? to!size_t(args[3]) : 32;
    if (count == 0 || rounds == 0 || batch == 0 || batch > 256)
        throw new Exception("actors/rounds must be positive; batch must be 1..256");

    auto farm = AntFarm.create(1 << 20, 8, 1, 0, 0, 1, 8192);
    if (farm is null) throw new Exception("Farm allocation failed");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, count);
    if (runtime is null) throw new Exception("actor runtime allocation failed");
    scope (exit) runtime.destroy();
    auto owners = new ActorOwner!State[count];
    auto calls = new ulong[count];
    foreach (i; 0 .. count)
    {
        owners[i] = runtime.createActor!(State, activate)(State(&calls[i]));
        if (!owners[i].valid) throw new Exception("actor creation failed");
    }
    scope (exit) foreach (ref owner; owners)
    {
        owner.requestRetire();
        if (owner.reclaim() != ActorReclaimResult.reclaimed)
            throw new Exception("actor reclamation failed");
    }

    ConsumerView view;
    if (view.subscribe(farm) < 0) throw new Exception("consumer subscribe failed");
    scope (exit) view.unsubscribe();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) throw new Exception("producer registration failed");
    scope (exit) farm.unregisterProducer(token);

    enum warmups = 5;
    long elapsedTicks;
    ulong flushes;
    foreach (round; 0 .. rounds + warmups)
    {
        foreach (ref owner; owners)
            if (owner.handle.wake() != ActorWakeResult.queued)
                throw new Exception("actor wake failed");
        immutable start = MonoTime.currTime;
        immutable deadline = start + 30.seconds;
        size_t published;
        ulong roundFlushes;
        while (published < count)
        {
            published += runtime.flush(token, batch, 0);
            ++roundFlushes;
            while (view.consumeNext()) {}
            if (MonoTime.currTime >= deadline)
                throw new Exception("actor drain timed out");
        }
        immutable ticks = MonoTime.currTime.ticks - start.ticks;
        if (round >= warmups)
        {
            elapsedTicks += ticks;
            flushes += roundFlushes;
        }
        foreach (n; calls)
            if (n != round + 1) throw new Exception("callback count mismatch");
        if (runtime.ready != 0) throw new Exception("ready backlog not drained");
    }
    immutable nanos = cast(double) elapsedTicks * 1e9 / MonoTime.ticksPerSecond;
    writefln("actors=%s rounds=%s batch=%s ns/actor=%.2f Mactor/s=%.2f flushes=%s",
        count, rounds, batch, nanos / (cast(double) count * rounds),
        cast(double) count * rounds * 1e3 / nanos, flushes);
}
