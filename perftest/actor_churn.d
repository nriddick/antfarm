/++
 + Sustained create -> dispatch -> retire -> reclaim benchmark.
 + Farm, runtime, metadata, and consumer threads survive across cycles; actor
 + state is allocated and reclaimed every cycle. See README.md for timing details.
 +/
module actor_churn;

import actors;
import antfarm : ConsumerView, fatal;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, pause;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.array : split;
import std.exception : enforce;
import std.math : isFinite;
import std.stdio : writefln;
import threadpool.pin : PinTarget, pinToLogicalProcessor;

version (AntfarmMimallocV3)
{
    import actors.mimalloc : mimallocV3ActorAllocator;
    private extern(C) int mi_version() nothrow @nogc @system;

    // Diagnostic control: match cRuntime's rounded size and 64-byte alignment.
    // The ordinary mimalloc mode uses the adapter's exact size/alignment.
    private void* mimalloc64Allocate(void*, size_t bytes, size_t alignment)
        nothrow @nogc @system
    {
        if (alignment > 64) return null;
        immutable rounded = (bytes + 63) & ~cast(size_t) 63;
        return mimallocV3ActorAllocator().allocate(null, rounded, 64);
    }

    private void mimalloc64Deallocate(void*, void* memory, size_t bytes,
            size_t alignment) nothrow @nogc @system
    {
        immutable rounded = (bytes + 63) & ~cast(size_t) 63;
        mimallocV3ActorAllocator().deallocate(null, memory, rounded, 64);
    }
}

// Separate completion lines avoid one contended global callback counter.
align(64) private struct Completion
{
    shared ulong stamp;
    ulong calls;
    ubyte[48] padding;
}

private struct State
{
    Completion* completion;
    ulong cycle;
}

private void operate(scope ref ActorBorrow!State actor) nothrow @nogc @system
{
    auto completion = actor.value.completion;
    ++completion.calls;
    atomicStore!(MemoryOrder.rel)(completion.stamp, actor.value.cycle);
}

private void activate(scope ref ActorBorrow!State actor,
        scope ref ActorContext) nothrow @nogc @system
{
    operate(actor);
}

private PrivateDisposition privateActivate(scope ref PrivateBorrow!State actor,
        scope ref PrivateContext) nothrow @nogc @system
{
    auto completion = actor.value.completion;
    ++completion.calls;
    atomicStore!(MemoryOrder.rel)(completion.stamp, actor.value.cycle);
    return PrivateDisposition.finish;
}

private void privateOperate(scope ref PrivateBorrow!State actor,
        scope ref PrivateContext context) nothrow @nogc @system
{
    privateActivate(actor, context);
}

private struct TreeState
{
    Completion* completions;
    size_t count;
    ulong cycle;
    PrivateChildPark!State* children;
}

private PrivateDisposition treeActivate(scope ref PrivateBorrow!TreeState actor,
        scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = context.createChildren!State(actor.value.count);
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. actor.value.count)
            if (!actor.value.children.spawn(context,
                    State(actor.value.completions + i, actor.value.cycle)))
                fatal("private child allocation failed");
        if (!actor.value.children.start!privateOperate(context, true))
            fatal("private child wave failed");
        return PrivateDisposition.again;
    }
    return actor.value.children.finished(context)
        ? PrivateDisposition.finish : PrivateDisposition.again;
}

private class Consumer
{
    AntFarm* farm;
    shared int started;
    shared int stop;
    int cpu;

    this(AntFarm* farm, int cpu) { this.farm = farm; this.cpu = cpu; }

    void run()
    {
        if (cpu >= 0 && !pinToLogicalProcessor(PinTarget(0, 0, cast(ushort) cpu)))
        {
            atomicStore!(MemoryOrder.rel)(started, -1);
            return;
        }
        ConsumerView view;
        if (view.subscribe(farm) < 0)
        {
            atomicStore!(MemoryOrder.rel)(started, -1);
            return;
        }
        scope (exit) view.unsubscribe();
        atomicStore!(MemoryOrder.rel)(started, 1);
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            if (!view.consumeNext()) pause();
        while (view.consumeNext()) {}
    }
}

private double elapsedSeconds(long ticks)
{
    return cast(double) ticks / MonoTime.ticksPerSecond;
}

void main(string[] args)
{
    enforce(args.length >= 2 && args.length <= 9
            && (args[1] == "actor" || args[1] == "wave" || args[1] == "private" || args[1] == "private-remote" || args[1] == "tree"),
        "usage: actor_churn actor|wave|private|private-remote|tree [actors=4096] [seconds=3] [consumers=0] [batch=256] [warmups=5] [crt|mimalloc|mimalloc64] [controller-cpu,consumer-cpus|-]");
    immutable waveMode = args[1] == "wave";
    immutable remoteMode = args[1] == "private-remote";
    immutable privateMode = args[1] == "private" || remoteMode;
    immutable treeMode = args[1] == "tree";
    immutable count = args.length > 2 ? to!size_t(args[2]) : 4096;
    immutable duration = args.length > 3 ? to!double(args[3]) : 3.0;
    immutable consumers = args.length > 4 ? to!uint(args[4]) : 0;
    immutable batch = args.length > 5 ? to!size_t(args[5]) : 256;
    immutable warmups = args.length > 6 ? to!size_t(args[6]) : 5;
    version (AntfarmMimallocV3) enum defaultAllocator = "mimalloc";
    else enum defaultAllocator = "crt";
    immutable allocatorName = args.length > 7 ? args[7] : defaultAllocator;
    enforce(count > 0 && count <= uint.max && isFinite(duration)
            && duration > 0 && consumers <= 64 && batch > 0
            && (waveMode || batch <= 256),
        "actors/seconds/batch must be positive; consumers must be 0..64; actor batch must be <=256");
    ushort[] cpus;
    if (args.length > 8 && args[8] != "-")
    {
        foreach (cpu; args[8].split(",")) cpus ~= cpu.to!ushort;
        enforce(cpus.length == consumers + 1,
            "CPU list must name the controller followed by each consumer");
        enforce(pinToLogicalProcessor(PinTarget(0, 0, cpus[0])),
            "controller CPU pin failed");
    }
    auto allocator = ActorAllocator.cRuntime();
    if (allocatorName != "crt")
    {
        enforce(allocatorName == "mimalloc" || allocatorName == "mimalloc64",
            "allocator must be crt, mimalloc, or mimalloc64");
        version (AntfarmMimallocV3)
            allocator = allocatorName == "mimalloc"
                ? mimallocV3ActorAllocator()
                : ActorAllocator(null, &mimalloc64Allocate, &mimalloc64Deallocate);
        else
            enforce(false, "mimalloc requires the actor_churn_mimalloc build");
    }

    auto farm = AntFarm.create(1 << 20, 8, consumers ? consumers : 1,
        0, 0, 1, 16384);
    enforce(farm !is null, "Farm allocation failed");
    version (AntfarmMimallocV3)
        if (allocatorName != "crt") writefln("mimalloc_version=%s", mi_version());
    scope (exit) farm.destroy();
    auto runtime = privateMode || treeMode ? null : ActorRuntime.create(farm, count, allocator);
    enforce(privateMode || treeMode || runtime !is null, "actor runtime allocation failed");
    scope (exit) if (runtime !is null) runtime.destroy();
    auto park = privateMode && !remoteMode ? PrivateRootPark!(State, privateActivate).create(farm, count, allocator) : null;
    enforce(!privateMode || remoteMode || park !is null, "private park allocation failed");
    auto remotePark = remoteMode ? PrivateRootPark!(State, privateActivate, PrivateRootReclamation.consumer).create(farm, count, allocator) : null;
    enforce(!remoteMode || remotePark !is null, "remote private park allocation failed");
    scope(exit) if (remotePark !is null) remotePark.destroy();
    scope (exit) if (park !is null) park.destroy();
    immutable parents = (count + batch - 1) / batch;
    auto trees = treeMode ? PrivateRootPark!(TreeState, treeActivate).create(farm, parents, allocator) : null;
    enforce(!treeMode || trees !is null, "private tree park allocation failed");
    scope (exit) if (trees !is null) trees.destroy();
    auto owners = new ActorOwner!State[count];
    auto handles = new ActorHandle!State[count];
    auto completions = new Completion[count];
    auto token = farm.registerProducer(Tier.small);
    enforce(token.valid, "producer registration failed");
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    if (consumers == 0)
        enforce(view.subscribe(farm) >= 0, "consumer subscription failed");
    scope (exit) if (consumers == 0) view.unsubscribe();
    auto jobs = new Consumer[consumers];
    auto threads = new Thread[consumers];
    scope (exit)
    {
        foreach (job; jobs)
            if (job !is null) atomicStore!(MemoryOrder.rel)(job.stop, 1);
        foreach (thread; threads)
            if (thread !is null) thread.join();
    }
    foreach (i; 0 .. consumers)
    {
        jobs[i] = new Consumer(farm, cpus.length == 0 ? -1 : cast(int) cpus[i + 1]);
        threads[i] = new Thread(&jobs[i].run);
        threads[i].start();
        while (atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 0)
            Thread.yield();
        enforce(atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 1,
            "worker pin or subscription failed");
    }

    ActorWave wave;
    ulong cycle;
    long createTicks, dispatchTicks, retireTicks;
    void consume()
    {
        if (consumers == 0)
            while (view.consumeNext()) {}
        else
            pause();
    }

    void runCycle(bool measured)
    {
        ++cycle;
        immutable start = MonoTime.currTime;
        immutable deadline = start + 60.seconds;
        if (privateMode || treeMode)
        {
            if (privateMode)
                foreach (i; 0 .. count)
                    enforce(remoteMode ? remotePark.spawn(State(&completions[i], cycle))
                        : park.spawn(State(&completions[i], cycle)), "private creation failed");
            else
                foreach (i; 0 .. parents)
                {
                    immutable offset = i * batch;
                    immutable children = count - offset < batch ? count - offset : batch;
                    enforce(trees.spawn(TreeState(completions.ptr + offset, children, cycle, null)),
                        "private parent creation failed");
                }
            while (privateMode ? (remoteMode ? remotePark.live != 0 : park.live != 0) : trees.live != 0)
            {
                if (privateMode) { if (remoteMode) remotePark.pump(token, batch); else park.pump(token, batch); }
                else trees.pump(token, 256);
                consume();
                enforce(MonoTime.currTime < deadline, "private lifecycle timed out");
            }
            foreach (ref completion; completions)
                enforce(atomicLoad!(MemoryOrder.acq)(completion.stamp) == cycle
                    && completion.calls == cycle, "private count mismatch");
            return;
        }
        foreach (i; 0 .. count)
        {
            owners[i] = runtime.createActor!(State, activate)(
                State(&completions[i], cycle));
            enforce(owners[i].valid, "actor creation failed");
            handles[i] = owners[i].handle;
        }
        immutable created = MonoTime.currTime;
        if (waveMode)
        {
            wave.begin(farm);
            size_t published;
            while (published < count)
            {
                immutable limit = count - published < batch
                    ? count : published + batch;
                published += wave.publish!operate(handles[published .. limit],
                    token, 0);
                enforce(!wave.handle.failed, "wave publication failed");
                consume();
                enforce(MonoTime.currTime < deadline, "wave publication timed out");
            }
            auto completion = wave.seal();
            while (!completion.finished)
            {
                consume();
                enforce(MonoTime.currTime < deadline, "wave completion timed out");
            }
            enforce(!completion.failed, "wave execution failed");
        }
        else
        {
            foreach (handle; handles)
                enforce(handle.wake() == ActorWakeResult.queued, "wake failed");
            size_t published;
            while (published < count)
            {
                published += runtime.flush(token, batch, 0);
                consume();
                enforce(MonoTime.currTime < deadline, "actor publication timed out");
            }
            foreach (ref completion; completions)
                while (atomicLoad!(MemoryOrder.acq)(completion.stamp) != cycle)
                {
                    consume();
                    enforce(MonoTime.currTime < deadline, "actor completion timed out");
                }
        }
        immutable dispatched = MonoTime.currTime;
        foreach (ref owner; owners)
            enforce(owner.requestRetire() == ActorRetireResult.requested,
                "actor retirement failed");
        foreach (i, ref owner; owners)
        {
            ActorReclaimResult result;
            while ((result = owner.reclaim()) == ActorReclaimResult.busy)
            {
                runtime.flush(token, batch, 0);
                consume();
                enforce(MonoTime.currTime < deadline, "actor reclamation timed out");
            }
            enforce(result == ActorReclaimResult.reclaimed && !owner.valid,
                "actor reclamation failed");
            enforce(atomicLoad!(MemoryOrder.acq)(completions[i].stamp) == cycle
                    && completions[i].calls == cycle,
                "callback count or generation mismatch");
        }
        enforce(runtime.live == 0 && runtime.ready == 0
                && runtime.staleActivations == 0,
            "cycle left live, queued, or stale actors");
        immutable retired = MonoTime.currTime;
        if (measured)
        {
            createTicks += created.ticks - start.ticks;
            dispatchTicks += dispatched.ticks - created.ticks;
            retireTicks += retired.ticks - dispatched.ticks;
        }
    }

    foreach (_; 0 .. warmups) runCycle(false);
    immutable start = MonoTime.currTime;
    ulong rounds;
    double elapsed;
    do
    {
        runCycle(true);
        ++rounds;
        elapsed = elapsedSeconds(MonoTime.currTime.ticks - start.ticks);
    } while (elapsed < duration);
    immutable actorCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s state=%s stateAlign=%s allocator=%s ringMiB=8 hugePages=%s cpus=%s",
        args[1], count, consumers, batch, warmups, State.sizeof, State.alignof,
        allocatorName, farm.usedLargePages, cpus);
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f cycles/s=%.2f",
        rounds, elapsed, actorCycles / elapsed / 1e6,
        elapsed * 1e9 / actorCycles, rounds / elapsed);
    if (privateMode || treeMode)
    {
        writefln("verified_actor_cycles=%s extra_parents_per_cycle=%s retirement=%s phases=overlap",
            cycle * count, treeMode ? parents : 0,
            treeMode ? "children_consumer_free" : remoteMode ? "consumer_free" : "emitter_free");
        return;
    }
    writefln("create_s=%.6f dispatch_s=%.6f retire_reclaim_verify_s=%.6f verified_actor_cycles=%s",
        elapsedSeconds(createTicks), elapsedSeconds(dispatchTicks),
        elapsedSeconds(retireTicks), cycle * count);
}
