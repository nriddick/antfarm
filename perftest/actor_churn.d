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

private void inboxActivate(scope ref ActorBorrow!State actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    while (auto node = context.popInbox())
    {
        operate(actor);
        node.complete();
    }
}

private PrivateRootDisposition privateActivate(scope ref PrivateBorrow!State actor,
        scope ref PrivateContext) nothrow @nogc @system
{
    auto completion = actor.value.completion;
    ++completion.calls;
    atomicStore!(MemoryOrder.rel)(completion.stamp, actor.value.cycle);
    return PrivateRootDisposition.finish;
}

private void privateOperate(scope ref PrivateBorrow!State actor,
        scope ref PrivateContext context) nothrow @nogc @system
{
    privateActivate(actor, context);
}

private struct DtorState
{
    Completion* completion;
    ulong cycle;
    @disable this(this);
    ~this() nothrow @nogc
    {
        if (completion !is null)
        {
            ++completion.calls;
            atomicStore!(MemoryOrder.rel)(completion.stamp, cycle);
        }
    }
}

private PrivateRootDisposition dtorActivate(scope ref PrivateBorrow!DtorState,
    scope ref PrivateContext) nothrow @nogc @system
{
    return PrivateRootDisposition.finish;
}

private struct TreeState
{
    Completion* completions;
    size_t count;
    ulong cycle;
    PrivateChildPark!State* children;
}

private PrivateRootDisposition treeActivate(bool waitChildren)(scope ref PrivateBorrow!TreeState actor,
        scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = context.createChildren!State(actor.value.count);
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. actor.value.count)
            if (actor.value.children.spawn(context,
                    State(actor.value.completions + i, actor.value.cycle)) != PrivateSpawnResult.spawned)
                fatal("private child allocation failed");
        if (actor.value.children.startFinal!privateOperate(context) != PrivateStartResult.started)
            fatal("private child wave failed");
        return waitChildren ? PrivateRootDisposition.waitChildren : PrivateRootDisposition.again;
    }
    static if (waitChildren) return PrivateRootDisposition.finish;
    else return actor.value.children.finished(context)
        ? PrivateRootDisposition.finish : PrivateRootDisposition.again;
}

private enum foldFanout = 8;
private struct FoldMid
{
    Completion* completions;
    size_t count;
    ulong cycle;
    PrivateChildPark!(State, ulong)* leaves;
}
private ulong foldLeaf(scope ref PrivateBorrow!State b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    privateOperate(b, c);
    return b.value.cycle;
}
private void foldLaunch(scope ref PrivateBorrow!FoldMid b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    b.value.leaves = c.createChildren!(State, ulong)(b.value.count);
    if (b.value.leaves is null) fatal("fold leaf park allocation failed");
    foreach (i; 0 .. b.value.count)
        if (b.value.leaves.spawn(c, State(b.value.completions + i, b.value.cycle))
                != PrivateSpawnResult.spawned)
            fatal("fold leaf spawn failed");
    if (b.value.leaves.startFinal!foldLeaf(c) != PrivateStartResult.started)
        fatal("fold leaf wave failed");
}
private ulong foldValue(scope ref PrivateBorrow!FoldMid b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    ulong sum;
    foreach (value; b.value.leaves.results(c)) sum += value;
    return sum;
}
private PrivateChildStep!ulong foldContinue(scope ref PrivateBorrow!FoldMid b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.leaves is null)
    {
        foldLaunch(b, c);
        return typeof(return).waitChildren();
    }
    return typeof(return).done(foldValue(b, c));
}
private struct FoldRoot
{
    Completion* completions;
    size_t count;
    ulong cycle;
    PrivateChildPark!(FoldMid, ulong)* mids;
    uint phase;
}
private PrivateRootDisposition foldRoot(bool continuation)(scope ref PrivateBorrow!FoldRoot b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.phase++ == 0)
    {
        immutable mids = (b.value.count - 1) / foldFanout + 1;
        b.value.mids = c.createChildren!(FoldMid, ulong)(mids);
        if (b.value.mids is null) fatal("fold middle park allocation failed");
        foreach (i; 0 .. mids)
        {
            immutable offset = i * foldFanout;
            immutable leaves = b.value.count - offset < foldFanout
                ? b.value.count - offset : foldFanout;
            if (b.value.mids.spawn(c, FoldMid(b.value.completions + offset, leaves,
                    b.value.cycle)) != PrivateSpawnResult.spawned)
                fatal("fold middle spawn failed");
        }
        static if (continuation)
            auto started = b.value.mids.startFinal!foldContinue(c);
        else
            auto started = b.value.mids.start!foldLaunch(c);
        if (started != PrivateStartResult.started) fatal("fold middle wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    static if (!continuation)
        if (b.value.phase == 2)
        {
            if (b.value.mids.startFinal!foldValue(c) != PrivateStartResult.started)
                fatal("fold result wave failed");
            return PrivateRootDisposition.waitChildren;
        }
    ulong sum;
    foreach (value; b.value.mids.results(c)) sum += value;
    if (sum != b.value.count * b.value.cycle) fatal("fold result mismatch");
    return PrivateRootDisposition.finish;
}

private void runFoldTree(bool continuation)(AntFarm* farm, ActorAllocator allocator,
    ref Token token, ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ushort)[] cpus)
{
    immutable count = completions.length;
    immutable parents = (count - 1) / batch + 1;
    auto park = PrivateRootPark!(FoldRoot, foldRoot!continuation).create(farm, parents, batch, allocator);
    enforce(park !is null, "fold root park allocation failed");
    scope(exit) park.destroy();
    ulong cycle;
    void runCycle()
    {
        ++cycle;
        auto deadline = MonoTime.currTime + 60.seconds;
        foreach (i; 0 .. parents)
        {
            immutable offset = i * batch;
            immutable leaves = count - offset < batch ? count - offset : batch;
            enforce(park.spawn(FoldRoot(completions.ptr + offset, leaves, cycle))
                == PrivateSpawnResult.spawned, "fold root spawn failed");
        }
        while (park.live != 0)
        {
            park.pump(token);
            if (consumers == 0) while (view.consumeNext()) {}
            else pause();
            enforce(MonoTime.currTime < deadline, "fold tree timed out");
        }
        foreach (ref completion; completions)
            enforce(atomicLoad!(MemoryOrder.acq)(completion.stamp) == cycle
                && completion.calls == cycle, "fold leaf count mismatch");
    }
    foreach (_; 0 .. warmups) runCycle();
    immutable start = MonoTime.currTime;
    ulong rounds;
    double elapsed;
    do
    {
        runCycle();
        ++rounds;
        elapsed = elapsedSeconds(MonoTime.currTime.ticks - start.ticks);
    } while (elapsed < duration);
    immutable leafCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s fanout=%s",
        mode, count, consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus, foldFanout);
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f verified_actor_cycles=%s root_waves_per_cycle=%s",
        rounds, elapsed, leafCycles / elapsed / 1e6, elapsed * 1e9 / leafCycles,
        cycle * count, parents * (continuation ? 1 : 2));
}

private enum SubsetKind : ubyte { selected, reservedWhole, plainWhole }
private struct SubsetControl
{
    shared ulong requested, completed;
    shared uint ready, stop;
}
private struct SubsetState
{
    PrivateChildPark!uint* children;
    SubsetControl* control;
    const(uint)[] members;
    size_t count;
    ulong completed;
    SubsetKind kind;
    bool inFlight;
}
private void subsetOperate(scope ref PrivateBorrow!uint, scope ref PrivateContext)
    nothrow @nogc @system {}

private PrivateRootDisposition subsetActivate(scope ref PrivateBorrow!SubsetState b,
    scope ref PrivateContext context) nothrow @nogc @system
{
    auto s = &b.value();
    if (s.children is null)
    {
        s.children = context.createChildren!uint(s.count,
            PrivateCohortOptions(0, s.kind != SubsetKind.plainWhole));
        if (s.children is null) fatal("subset park allocation failed");
        foreach (i; 0 .. s.count)
            if (s.children.spawn(context, cast(uint) i) != PrivateSpawnResult.spawned)
                fatal("subset member allocation failed");
        atomicStore!(MemoryOrder.rel)(s.control.ready, 1u);
        return PrivateRootDisposition.waitChildren;
    }
    if (s.inFlight)
    {
        s.inFlight = false;
        atomicStore!(MemoryOrder.rel)(s.control.completed, ++s.completed);
    }
    if (atomicLoad!(MemoryOrder.acq)(s.control.stop) != 0)
        return PrivateRootDisposition.finish;
    if (atomicLoad!(MemoryOrder.acq)(s.control.requested) == s.completed)
        return PrivateRootDisposition.waitChildren;
    auto result = s.kind == SubsetKind.selected
        ? s.children.startSubset!subsetOperate(context, s.members)
        : s.children.start!subsetOperate(context);
    if (result != PrivateStartResult.started) fatal("subset benchmark wave failed");
    s.inFlight = true;
    return PrivateRootDisposition.waitChildren;
}

private void runSubset(AntFarm* farm, ActorAllocator allocator, ref Token token,
    ref ConsumerView view, string mode, size_t count, double duration, uint consumers,
    size_t batch, size_t warmups, string allocatorName, const(ushort)[] cpus, uint percent)
{
    auto kind = mode == "subset" ? SubsetKind.selected
        : mode == "subset-whole" ? SubsetKind.reservedWhole : SubsetKind.plainWhole;
    immutable selected = kind == SubsetKind.selected ? cast(size_t) (cast(ulong) count * percent / 100) : count;
    enforce(selected != 0, "subset percentage must select at least one member");
    auto members = new uint[selected];
    foreach (i; 0 .. selected) members[i] = cast(uint) (count - 1 - i);
    SubsetControl control;
    auto park = PrivateRootPark!(SubsetState, subsetActivate).create(farm, 1, batch, allocator);
    enforce(park !is null, "subset root allocation failed");
    scope(exit) park.destroy();
    enforce(park.spawn(SubsetState(null, &control, members, count, 0, kind)) == PrivateSpawnResult.spawned,
        "subset root spawn failed");
    void pump()
    {
        park.pump(token);
        if (consumers == 0) while (view.consumeNext()) {}
        else pause();
    }
    auto deadline = MonoTime.currTime + 60.seconds;
    while (atomicLoad!(MemoryOrder.acq)(control.ready) == 0)
    {
        pump();
        enforce(MonoTime.currTime < deadline, "subset setup timed out");
    }
    ulong waves;
    void runWave()
    {
        atomicStore!(MemoryOrder.rel)(control.requested, ++waves);
        auto waveDeadline = MonoTime.currTime + 60.seconds;
        while (atomicLoad!(MemoryOrder.acq)(control.completed) != waves)
        {
            pump();
            enforce(MonoTime.currTime < waveDeadline, "subset wave timed out");
        }
    }
    foreach (_; 0 .. warmups) runWave();
    immutable start = MonoTime.currTime;
    ulong rounds;
    double elapsed;
    do
    {
        runWave();
        ++rounds;
        elapsed = elapsedSeconds(MonoTime.currTime.ticks - start.ticks);
    } while (elapsed < duration);
    atomicStore!(MemoryOrder.rel)(control.stop, 1u);
    deadline = MonoTime.currTime + 60.seconds;
    while (park.live)
    {
        pump();
        enforce(MonoTime.currTime < deadline, "subset teardown timed out");
    }
    enforce(atomicLoad!(MemoryOrder.acq)(control.completed) == warmups + rounds,
        "subset joined-wave count mismatch");
    writefln("mode=%s actors=%s selected=%s percent=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s setup=excluded teardown=excluded",
        mode, count, selected, kind == SubsetKind.selected ? percent : 100,
        consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus);
    writefln("rounds=%s elapsed_s=%.6f waves/s=%.6f Mselected_callbacks/s=%.6f ns/wave=%.2f verified_waves=%s",
        rounds, elapsed, rounds / elapsed, cast(double) selected * rounds / elapsed / 1e6,
        elapsed * 1e9 / rounds, waves);
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
    enforce(args.length >= 2 && args.length <= 12
            && (args[1] == "tree-cont" || args[1] == "tree-pipelined" || args[1] == "subset" || args[1] == "subset-whole" || args[1] == "subset-whole-unreserved" || args[1] == "dtor" || args[1] == "inbox" || args[1] == "actor" || args[1] == "wave" || args[1] == "private" || args[1] == "private-remote" || args[1] == "tree" || args[1] == "tree-wait"),
        "usage: actor_churn actor|inbox|wave|private|private-remote|tree|tree-wait|tree-cont|tree-pipelined|dtor|subset|subset-whole|subset-whole-unreserved [actors=4096] [seconds=3] [consumers=0] [batch=256] [warmups=5] [crt|mimalloc|mimalloc64] [controller-cpu,consumer-cpus|-] [small|bulk] [quota-words=16384] [subset-percent=100]");
    immutable foldTreeMode = args[1] == "tree-cont" || args[1] == "tree-pipelined";
    immutable subsetMode = args[1] == "subset" || args[1] == "subset-whole"
        || args[1] == "subset-whole-unreserved";
    immutable percent = args.length > 11 ? to!uint(args[11]) : 100;
    enforce((subsetMode || args.length <= 11) && (percent == 1 || percent == 10 || percent == 100),
        "subset-percent must be 1, 10 or 100, and is only for subset modes");
    immutable inboxMode = args[1] == "inbox";
    immutable waveMode = args[1] == "wave";
    immutable remoteMode = args[1] == "private-remote";
    immutable dtorMode = args[1] == "dtor";
    immutable privateMode = args[1] == "private" || remoteMode || dtorMode;
    immutable treeWaitMode = args[1] == "tree-wait";
    immutable treeMode = args[1] == "tree" || treeWaitMode;
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
            && (waveMode || privateMode || treeMode || foldTreeMode || batch <= 256),
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

    immutable tierName = args.length > 9 ? args[9] : "small";
    enforce(tierName == "small" || tierName == "bulk", "tier must be small or bulk");
    immutable tier = tierName == "small" ? Tier.small : Tier.bulk;
    immutable quota = args.length > 10 ? to!ulong(args[10]) : 16384;
    enforce(quota > 0, "quota must be positive");
    auto farm = AntFarm.create(8, 8, consumers ? consumers : 1,
        tier == Tier.bulk ? 1 : 0, tier == Tier.bulk ? quota : 0,
        tier == Tier.small ? 1 : 0, tier == Tier.small ? quota : 0);
    enforce(farm !is null, "Farm allocation failed");
    writefln("batchLimit=%s tier=%s quota_words=%s", batch, tierName, quota);
    version (AntfarmMimallocV3)
        if (allocatorName != "crt") writefln("mimalloc_version=%s", mi_version());
    scope (exit) farm.destroy();
    auto runtime = privateMode || treeMode || subsetMode || foldTreeMode ? null : ActorRuntime.create(farm, count, allocator);
    enforce(privateMode || treeMode || subsetMode || foldTreeMode || runtime !is null, "actor runtime allocation failed");
    scope (exit) if (runtime !is null) runtime.destroy();
    auto park = privateMode && !remoteMode && !dtorMode ? PrivateRootPark!(State, privateActivate).create(farm, count, batch, allocator) : null;
    enforce(!privateMode || remoteMode || dtorMode || park !is null, "private park allocation failed");
    auto dtorPark = dtorMode ? PrivateRootPark!(DtorState, dtorActivate).create(farm, count, batch, allocator) : null;
    enforce(!dtorMode || dtorPark !is null, "destructor park allocation failed");
    scope(exit) if (dtorPark !is null) dtorPark.destroy();
    auto remotePark = remoteMode ? PrivateRootPark!(State, privateActivate, PrivateRootReclamation.consumer).create(farm, count, batch, allocator) : null;
    enforce(!remoteMode || remotePark !is null, "remote private park allocation failed");
    scope(exit) if (remotePark !is null) remotePark.destroy();
    scope (exit) if (park !is null) park.destroy();
    immutable parents = (count - 1) / batch + 1;
    auto trees = treeMode && !treeWaitMode ? PrivateRootPark!(TreeState, treeActivate!false).create(farm, parents, batch, allocator) : null;
    auto waitingTrees = treeWaitMode ? PrivateRootPark!(TreeState, treeActivate!true).create(farm, parents, batch, allocator) : null;
    enforce(!treeMode || (treeWaitMode ? waitingTrees !is null : trees !is null), "private tree park allocation failed");
    scope (exit) if (trees !is null) trees.destroy();
    scope (exit) if (waitingTrees !is null) waitingTrees.destroy();
    auto owners = new ActorOwner!State[count];
    auto handles = new ActorHandle!State[count];
    auto completions = new Completion[count];
    auto nodes = inboxMode ? new ActorInboxNode[count] : null;
    foreach (ref node; nodes) node.initialize();
    auto token = farm.registerProducer(tier);
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

    if (subsetMode)
    {
        runSubset(farm, allocator, token, view, args[1], count, duration, consumers,
            batch, warmups, allocatorName, cpus, percent);
        return;
    }

    if (foldTreeMode)
    {
        if (args[1] == "tree-cont")
            runFoldTree!true(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus);
        else
            runFoldTree!false(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus);
        return;
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
            if (dtorMode)
                foreach (i; 0 .. count)
                    enforce(dtorPark.spawn(DtorState(&completions[i], cycle)) == PrivateSpawnResult.spawned,
                        "destructor creation failed");
            else if (privateMode)
                foreach (i; 0 .. count)
                    enforce(remoteMode ? remotePark.spawn(State(&completions[i], cycle)) == PrivateSpawnResult.spawned
                        : park.spawn(State(&completions[i], cycle)) == PrivateSpawnResult.spawned, "private creation failed");
            else
                foreach (i; 0 .. parents)
                {
                    immutable offset = i * batch;
                    immutable children = count - offset < batch ? count - offset : batch;
                    auto initial = TreeState(completions.ptr + offset, children, cycle, null);
                    enforce((treeWaitMode ? waitingTrees.spawn(initial) : trees.spawn(initial)) == PrivateSpawnResult.spawned,
                        "private parent creation failed");
                }
            while (privateMode ? (dtorMode ? dtorPark.live != 0 : remoteMode ? remotePark.live != 0 : park.live != 0) : (treeWaitMode ? waitingTrees.live != 0 : trees.live != 0))
            {
                if (privateMode) { if (dtorMode) dtorPark.pump(token, batch); else if (remoteMode) remotePark.pump(token, batch); else park.pump(token, batch); }
                else if (treeWaitMode) waitingTrees.pump(token);
                else trees.pump(token);
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
            owners[i] = inboxMode
                ? runtime.createActor!(State, inboxActivate)(State(&completions[i], cycle))
                : runtime.createActor!(State, activate)(State(&completions[i], cycle));
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
            foreach (i, handle; handles)
                if (inboxMode)
                    enforce(handle.send(&nodes[i]) == ActorSendResult.queued, "send failed");
                else
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
        foreach (ref node; nodes)
            while (!node.available)
            {
                consume();
                enforce(MonoTime.currTime < deadline, "inbox completion timed out");
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
