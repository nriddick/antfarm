/++
 + Cold actor creation and bounded Fiber lifecycle-drain characterization.
 + See README.md for setup, timing boundaries, and comparison commands.
 +/
module scaling;

import actors;
import antfarm;
import antfarm_fibers;
import core.time : MonoTime;
import std.conv : to;
import std.stdio : writefln;

private struct State { ulong value; }
private void actorHandler(scope ref ActorBorrow!State, scope ref ActorContext)
    nothrow @nogc @system {}
private void fiberBody() {}

private double millisecondsSince(MonoTime start)
{
    return (MonoTime.currTime - start).total!"nsecs" / 1e6;
}

void main(string[] args)
{
    if (args.length < 3 || (args[1] != "create" && args[1] != "lifecycle"))
        throw new Exception("usage: scaling create <actors> | lifecycle <fibers> <batch> [handle|take]");
    immutable n = to!size_t(args[2]);
    if (n == 0 || n > cast(size_t) long.max / 4)
        throw new Exception("count must be positive and fit lifecycle reservation accounting");
    auto farm = AntFarm.create(1 << 20, 8, 1, 0, 0, 1, 16384);
    if (farm is null) throw new Exception("Farm allocation failed");
    scope (exit) farm.destroy();
    if (args[1] == "create")
    {
        auto runtime = ActorRuntime.create(farm, n);
        if (runtime is null) throw new Exception("runtime allocation failed");
        scope (exit) runtime.destroy();
        auto owners = new ActorOwner!State[n];
        immutable start = MonoTime.currTime;
        foreach (i; 0 .. n)
        {
            owners[i] = runtime.createActor!(State, actorHandler)(State.init);
            if (!owners[i].valid) throw new Exception("creation failed");
        }
        writefln("actor create n=%s ms=%.3f", n, millisecondsSince(start));
        foreach (ref owner; owners)
        {
            owner.requestRetire();
            if (owner.reclaim() != ActorReclaimResult.reclaimed)
                throw new Exception("reclaim failed");
        }
        return;
    }
    if (args.length < 4)
        throw new Exception("lifecycle mode requires a batch size");
    immutable batch = to!size_t(args[3]);
    immutable mode = args.length > 4 ? args[4] : "handle";
    if (batch == 0 || (mode != "handle" && mode != "take"))
        throw new Exception("batch must be positive; mode must be handle or take");
    auto domain = new FiberDomain(farm);
    domain.reserve(n);
    domain.enableLifecycleEvents(n * 4);
    foreach (_; 0 .. n) domain.spawn(&fiberBody, 4096);
    size_t calls;
    FiberLifecycleHandler sink = (ref const FiberLifecycleEvent event) { ++calls; };
    immutable start = MonoTime.currTime;
    while (domain.pendingEvents != 0)
    {
        if (mode == "handle")
            domain.handleLifecycleEvents(sink, batch);
        else
            calls += domain.takeLifecycleEvents(batch).length;
    }
    writefln("lifecycle n=%s batch=%s mode=%s ms=%.3f",
        n, batch, mode, millisecondsSince(start));
    if (calls != n) throw new Exception("bad admitted count");
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) throw new Exception("producer registration failed");
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();
    drainUntilEmpty(domain, token, view, 256, 0);
    domain.handleLifecycleEvents(sink);
    domain.releaseAll(domain.takeCompletions());
}
