/++
 + Sustained create -> dispatch -> retire -> reclaim benchmark.
 + Farm, runtime, metadata, and consumer threads survive across cycles; actor
 + state is allocated and reclaimed every cycle. See README.md for timing details.
 +/
module actor_churn;

import antfarm.actors;
import antfarm : ConsumerView, MAX_CHUNK, MAX_AVG_COST, avgCostForChunk,
    DEFAULT_SMALL_TABLE_THRESHOLD, fatal;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad, atomicStore, pause;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.array : split;
import std.exception : enforce;
import std.math : isFinite;
import std.stdio : writefln;
import threadpool.host : CallerPlacement, FarmHost, FarmHostOptions, WorkerIdle;
import threadpool.pin : PinOutcome, pinProcessor;
import threadpool.pool : PoolOptions;
import threadpool.topology : ProcessorId, parseCpuList;

version (AntfarmMimallocV3)
{
    import antfarm.actors.mimalloc : mimallocV3ActorAllocator;
    import core.stdc.config : c_long;
    private extern(C) int mi_version() nothrow @nogc @system;
    private extern(C) void mi_thread_set_in_threadpool() nothrow @nogc @system;
    private extern(C) c_long mi_option_get(int option) nothrow @nogc @system;

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

// Option-screen hook: ANTFARM_CHURN_MI_THREADPOOL=1 marks the controller and
// every consumer as mimalloc threadpool threads before their first actor
// allocation. Other options use MIMALLOC_* environment variables, which
// mimalloc reads before any theap caches them.
private bool threadpoolRequested() nothrow @nogc @system
{
    import core.stdc.stdlib : getenv;
    auto flag = getenv("ANTFARM_CHURN_MI_THREADPOOL");
    return flag !is null && flag[0] == '1' && flag[1] == 0;
}

private void markThreadpoolThread() nothrow @nogc @system
{
    version (AntfarmMimallocV3)
        if (threadpoolRequested()) mi_thread_set_in_threadpool();
}

version (AntfarmMimallocV3)
{
    // Diagnostic allocator (never for timing): the mimalloc adapter plus a
    // prefix holding the allocating thread, so it can count frees issued by a
    // different thread than the allocation's. That is antfarm's cross-thread
    // free traffic; mimalloc may still free locally or remotely depending on
    // page ownership. The prefix changes size classes and adds shared counters.
    private shared ulong countAllocations, countFrees, countCrossFrees, countCrossBytes;
    private shared size_t countThreads;
    private size_t countThread; // thread-local, assigned on first use

    private size_t countingThread() nothrow @nogc @system
    {
        if (countThread == 0)
            countThread = atomicFetchAdd!(MemoryOrder.raw)(countThreads, 1) + 1;
        return countThread;
    }

    private size_t countingPrefix(size_t alignment) nothrow @nogc @system
    {
        return alignment > 16 ? alignment : 16;
    }

    private void* countingAllocate(void*, size_t bytes, size_t alignment) nothrow @nogc @system
    {
        immutable prefix = countingPrefix(alignment);
        if (bytes > size_t.max - prefix) return null;
        auto raw = cast(ubyte*) mimallocV3ActorAllocator().allocate(null, bytes + prefix, prefix);
        if (raw is null) return null;
        *cast(size_t*) raw = countingThread();
        atomicFetchAdd!(MemoryOrder.raw)(countAllocations, 1);
        return raw + prefix;
    }

    private void countingDeallocate(void*, void* memory, size_t bytes, size_t alignment)
        nothrow @nogc @system
    {
        immutable prefix = countingPrefix(alignment);
        auto raw = cast(ubyte*) memory - prefix;
        atomicFetchAdd!(MemoryOrder.raw)(countFrees, 1);
        if (*cast(size_t*) raw != countingThread())
        {
            atomicFetchAdd!(MemoryOrder.raw)(countCrossFrees, 1);
            atomicFetchAdd!(MemoryOrder.raw)(countCrossBytes, bytes);
        }
        mimallocV3ActorAllocator().deallocate(null, raw, bytes + prefix, prefix);
    }
}

// Peak resident set (VmHWM) and, in mimalloc builds, the effective values of
// the screened options, printed once at exit. Option indices are mi_option_t
// ordinals from the pinned v3.5.3 header.
private void reportProcess(string allocatorName)
{
    version (linux)
    {
        import std.file : readText;
        import std.string : splitLines, strip;
        import std.algorithm : startsWith;
        foreach (line; readText("/proc/self/status").splitLines)
            if (line.startsWith("VmHWM:"))
                writefln("peak_rss_kib=%s", line["VmHWM:".length .. $].strip.split(" ")[0]);
    }
    version (AntfarmMimallocV3)
        writefln("mi_threadpool=%s mi_page_reclaim_on_free=%s mi_page_full_retain=%s mi_page_max_reclaim=%s mi_page_cross_thread_max_reclaim=%s",
            threadpoolRequested() ? 1 : 0, mi_option_get(35), mi_option_get(36),
            mi_option_get(41), mi_option_get(42));
    version (AntfarmMimallocV3)
        if (allocatorName == "mimalloc-count")
            writefln("alloc_calls=%s free_calls=%s cross_thread_frees=%s cross_thread_free_bytes=%s",
                atomicLoad(countAllocations), atomicLoad(countFrees),
                atomicLoad(countCrossFrees), atomicLoad(countCrossBytes));
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

// Opt-in cohort slabs (`*-slab` modes). This source also builds against
// runtimes without PrivateStateStorage; there the slab modes run perObject
// cohorts and print storage=perObject, so a base/candidate gate on a slab mode
// compares per-object (base) with slab (candidate) storage on one harness.
static if (__traits(compiles, PrivateStateStorage.cohortSlab))
{
    enum slabSupported = true;
    alias ChildPark(T, R, bool slab) = PrivateChildPark!(T, R,
        slab ? PrivateStateStorage.cohortSlab : PrivateStateStorage.perObject);
    ChildPark!(T, R, slab)* createCohort(T, R, bool slab)(scope ref PrivateContext context,
        size_t capacity) nothrow @nogc @system
    {
        static if (slab)
            return context.createChildren!(T, R, PrivateStateStorage.cohortSlab)(capacity,
                PrivateCohortOptions(0, false), capacity * T.sizeof);
        else
            return context.createChildren!(T, R)(capacity);
    }
}
else
{
    enum slabSupported = false;
    alias ChildPark(T, R, bool slab) = PrivateChildPark!(T, R);
    ChildPark!(T, R, slab)* createCohort(T, R, bool slab)(scope ref PrivateContext context,
        size_t capacity) nothrow @nogc @system
    {
        return context.createChildren!(T, R)(capacity);
    }
}

private string storageName(bool slab)
{
    return slab && slabSupported ? "cohortSlab" : "perObject";
}

private struct TreeState(bool slab = false)
{
    Completion* completions;
    size_t count;
    ulong cycle;
    ChildPark!(State, void, slab)* children;
}

private PrivateRootDisposition treeActivate(bool waitChildren, bool slab = false)(
        scope ref PrivateBorrow!(TreeState!slab) actor,
        scope ref PrivateContext context) nothrow @nogc @system
{
    if (actor.value.children is null)
    {
        actor.value.children = createCohort!(State, void, slab)(context, actor.value.count);
        if (actor.value.children is null) fatal("child park allocation failed");
        foreach (i; 0 .. actor.value.count)
            if (actor.value.children.spawn(context,
                    State(actor.value.completions + i, actor.value.cycle)) != PrivateSpawnResult.spawned)
                fatal("private child allocation failed");
        if (actor.value.children.startFinal!privateOperate(context, g_childHint)
                != PrivateStartResult.started)
            fatal("private child wave failed");
        return waitChildren ? PrivateRootDisposition.waitChildren : PrivateRootDisposition.again;
    }
    static if (waitChildren) return PrivateRootDisposition.finish;
    else return actor.value.children.finished(context)
        ? PrivateRootDisposition.finish : PrivateRootDisposition.again;
}

private enum foldFanout = 8;
private struct FoldMid(bool slab)
{
    Completion* completions;
    size_t count;
    ulong cycle;
    ChildPark!(State, ulong, slab)* leaves;
}
private ulong foldLeaf(scope ref PrivateBorrow!State b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    privateOperate(b, c);
    return b.value.cycle;
}
private void foldLaunch(bool slab)(scope ref PrivateBorrow!(FoldMid!slab) b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    b.value.leaves = createCohort!(State, ulong, slab)(c, b.value.count);
    if (b.value.leaves is null) fatal("fold leaf park allocation failed");
    foreach (i; 0 .. b.value.count)
        if (b.value.leaves.spawn(c, State(b.value.completions + i, b.value.cycle))
                != PrivateSpawnResult.spawned)
            fatal("fold leaf spawn failed");
    if (b.value.leaves.startFinal!foldLeaf(c, g_childHint) != PrivateStartResult.started)
        fatal("fold leaf wave failed");
}
private ulong foldValue(bool slab)(scope ref PrivateBorrow!(FoldMid!slab) b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    ulong sum;
    foreach (value; b.value.leaves.results(c)) sum += value;
    return sum;
}
private PrivateChildStep!ulong foldContinue(bool slab)(scope ref PrivateBorrow!(FoldMid!slab) b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.leaves is null)
    {
        foldLaunch!slab(b, c);
        return typeof(return).waitChildren();
    }
    return typeof(return).done(foldValue!slab(b, c));
}
private struct FoldRoot(bool slab)
{
    Completion* completions;
    size_t count;
    ulong cycle;
    ChildPark!(FoldMid!slab, ulong, slab)* mids;
    uint phase;
}
private PrivateRootDisposition foldRoot(bool continuation, bool slab)(scope ref PrivateBorrow!(FoldRoot!slab) b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.phase++ == 0)
    {
        immutable mids = (b.value.count - 1) / foldFanout + 1;
        b.value.mids = createCohort!(FoldMid!slab, ulong, slab)(c, mids);
        if (b.value.mids is null) fatal("fold middle park allocation failed");
        foreach (i; 0 .. mids)
        {
            immutable offset = i * foldFanout;
            immutable leaves = b.value.count - offset < foldFanout
                ? b.value.count - offset : foldFanout;
            if (b.value.mids.spawn(c, FoldMid!slab(b.value.completions + offset, leaves,
                    b.value.cycle)) != PrivateSpawnResult.spawned)
                fatal("fold middle spawn failed");
        }
        static if (continuation)
            auto started = b.value.mids.startFinal!(foldContinue!slab)(c, g_childHint);
        else
            auto started = b.value.mids.start!(foldLaunch!slab)(c, false, g_childHint);
        if (started != PrivateStartResult.started) fatal("fold middle wave failed");
        return PrivateRootDisposition.waitChildren;
    }
    static if (!continuation)
        if (b.value.phase == 2)
        {
            if (b.value.mids.startFinal!(foldValue!slab)(c, g_childHint) != PrivateStartResult.started)
                fatal("fold result wave failed");
            return PrivateRootDisposition.waitChildren;
        }
    ulong sum;
    foreach (value; b.value.mids.results(c)) sum += value;
    if (sum != b.value.count * b.value.cycle) fatal("fold result mismatch");
    return PrivateRootDisposition.finish;
}

private void runFoldTree(bool continuation, bool slab = false)(AntFarm* farm, ActorAllocator allocator,
    ref Token token, ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus,
    uint rootHint)
{
    immutable count = completions.length;
    immutable parents = (count - 1) / batch + 1;
    auto park = PrivateRootPark!(FoldRoot!slab, foldRoot!(continuation, slab)).create(farm, parents, batch, allocator);
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
            enforce(park.spawn(FoldRoot!slab(completions.ptr + offset, leaves, cycle))
                == PrivateSpawnResult.spawned, "fold root spawn failed");
        }
        while (park.live != 0)
        {
            pumpRoot(park, token, batch, rootHint);
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
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s fanout=%s storage=%s",
        mode, count, consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus, foldFanout,
        storageName(slab));
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f verified_actor_cycles=%s root_waves_per_cycle=%s",
        rounds, elapsed, leafCycles / elapsed / 1e6, elapsed * 1e9 / leafCycles,
        cycle * count, parents * (continuation ? 1 : 2));
}

// tree-slab / tree-wait-slab: the tree and tree-wait workloads with
// cohortSlab children (perObject on runtimes without it). Same verification
// and rate as tree; the parent's slab is reserved at create and freed when the
// parent finishes.
private void runTree(bool waitChildren, bool slab)(AntFarm* farm, ActorAllocator allocator,
    ref Token token, ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus,
    uint rootHint)
{
    immutable count = completions.length;
    immutable parents = (count - 1) / batch + 1;
    auto park = PrivateRootPark!(TreeState!slab, treeActivate!(waitChildren, slab))
        .create(farm, parents, batch, allocator);
    enforce(park !is null, "private tree park allocation failed");
    scope(exit) park.destroy();
    ulong cycle;
    void runCycle()
    {
        ++cycle;
        auto deadline = MonoTime.currTime + 60.seconds;
        foreach (i; 0 .. parents)
        {
            immutable offset = i * batch;
            immutable children = count - offset < batch ? count - offset : batch;
            enforce(park.spawn(TreeState!slab(completions.ptr + offset, children, cycle, null))
                == PrivateSpawnResult.spawned, "private parent creation failed");
        }
        while (park.live != 0)
        {
            pumpRoot(park, token, batch, rootHint);
            if (consumers == 0) while (view.consumeNext()) {}
            else pause();
            enforce(MonoTime.currTime < deadline, "private lifecycle timed out");
        }
        foreach (ref completion; completions)
            enforce(atomicLoad!(MemoryOrder.acq)(completion.stamp) == cycle
                && completion.calls == cycle, "private count mismatch");
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
    immutable actorCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s state=%s stateAlign=%s allocator=%s ringMiB=8 hugePages=%s cpus=%s storage=%s",
        mode, count, consumers, batch, warmups, State.sizeof, State.alignof,
        allocatorName, farm.usedLargePages, cpus, storageName(slab));
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f cycles/s=%.2f",
        rounds, elapsed, actorCycles / elapsed / 1e6, elapsed * 1e9 / actorCycles, rounds / elapsed);
    writefln("verified_actor_cycles=%s extra_parents_per_cycle=%s retirement=children_consumer_free phases=overlap",
        cycle * count, parents);
}

// tree-refill / tree-refill-slab: each parent creates one cohort and refills
// it refillWaves times: spawn every member, startFinal, waitChildren, then
// respawn into the slots the joined retiring wave released. One cohort
// create/dispose (and, for slab storage, one slab allocation/free) serves
// refillWaves full waves. The rate counts leaf lifetimes, including the
// parents' creation and cohort setup/teardown.
private enum refillWaves = 8;
private struct RefillState(bool slab)
{
    Completion* completions;
    size_t count;
    ulong cycle;
    uint wave;
    ChildPark!(State, void, slab)* children;
}
private PrivateRootDisposition refillActivate(bool slab)(scope ref PrivateBorrow!(RefillState!slab) b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    with (b.value)
    {
        if (children is null)
        {
            children = createCohort!(State, void, slab)(c, count);
            if (children is null) fatal("refill park allocation failed");
        }
        else if (++wave == refillWaves)
            return PrivateRootDisposition.finish;
        foreach (i; 0 .. count)
            if (children.spawn(c, State(completions + i, cycle)) != PrivateSpawnResult.spawned)
                fatal("refill child spawn failed");
        if (children.startFinal!privateOperate(c, g_childHint) != PrivateStartResult.started)
            fatal("refill wave failed");
        return PrivateRootDisposition.waitChildren;
    }
}

private void runRefill(bool slab)(AntFarm* farm, ActorAllocator allocator, ref Token token,
    ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus,
    uint rootHint)
{
    immutable count = completions.length;
    immutable parents = (count - 1) / batch + 1;
    auto park = PrivateRootPark!(RefillState!slab, refillActivate!slab).create(farm, parents, batch, allocator);
    enforce(park !is null, "refill root park allocation failed");
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
            enforce(park.spawn(RefillState!slab(completions.ptr + offset, leaves, cycle))
                == PrivateSpawnResult.spawned, "refill root spawn failed");
        }
        while (park.live != 0)
        {
            pumpRoot(park, token, batch, rootHint);
            if (consumers == 0) while (view.consumeNext()) {}
            else pause();
            enforce(MonoTime.currTime < deadline, "refill tree timed out");
        }
        foreach (ref completion; completions)
            enforce(atomicLoad!(MemoryOrder.acq)(completion.stamp) == cycle
                && completion.calls == cycle * refillWaves, "refill leaf count mismatch");
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
    immutable leafCycles = cast(double) count * refillWaves * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s refill_waves=%s storage=%s",
        mode, count, consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus,
        refillWaves, storageName(slab));
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f verified_actor_cycles=%s cohorts_per_cycle=%s",
        rounds, elapsed, leafCycles / elapsed / 1e6, elapsed * 1e9 / leafCycles,
        cycle * count * refillWaves, parents);
}

// tree-cancel: each root retires its leaves with startFinal in eight tables.
// The middle leaf cancels its peers, so the emitter retires the rest of the
// wave with cleanup tables and the root joins as usual.
private enum cancelTreeTables = 8;
private struct CancelLeaf
{
    Completion* completion;
    ulong cycle;
    bool trigger;
}
private void cancelLeaf(scope ref PrivateBorrow!CancelLeaf b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    ++b.value.completion.calls;
    atomicStore!(MemoryOrder.rel)(b.value.completion.stamp, b.value.cycle);
    if (b.value.trigger) c.cancelPeers();
}
private struct CancelTreeState
{
    Completion* completions;
    size_t count;
    ulong cycle;
    ulong* dispatched;
    PrivateChildPark!CancelLeaf* children;
}
private PrivateRootDisposition cancelTreeActivate(scope ref PrivateBorrow!CancelTreeState b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    with (b.value)
    {
        if (children is null)
        {
            children = c.createChildren!CancelLeaf(count, (count - 1) / cancelTreeTables + 1);
            if (children is null) fatal("cancel tree park allocation failed");
            foreach (i; 0 .. count)
                if (children.spawn(c, CancelLeaf(completions + i, cycle, i == count / 2))
                        != PrivateSpawnResult.spawned)
                    fatal("cancel tree leaf spawn failed");
            if (children.startFinal!cancelLeaf(c, g_childHint) != PrivateStartResult.started)
                fatal("cancel tree wave failed");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = children.outcome(c);
        if (!o.cancelled || o.participants != count || o.dispatched <= count / 2)
            fatal("cancel tree outcome mismatch");
        *dispatched += o.dispatched;
        return PrivateRootDisposition.finish;
    }
}

private void runCancelTree(AntFarm* farm, ActorAllocator allocator, ref Token token,
    ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus,
    uint rootHint)
{
    immutable count = completions.length;
    immutable parents = (count - 1) / batch + 1;
    auto park = PrivateRootPark!(CancelTreeState, cancelTreeActivate).create(farm, parents, batch, allocator);
    enforce(park !is null, "cancel tree root park allocation failed");
    scope(exit) park.destroy();
    auto dispatched = new ulong[parents];
    ulong cycle;
    void runCycle()
    {
        ++cycle;
        auto deadline = MonoTime.currTime + 60.seconds;
        foreach (i; 0 .. parents)
        {
            immutable offset = i * batch;
            immutable leaves = count - offset < batch ? count - offset : batch;
            enforce(park.spawn(CancelTreeState(completions.ptr + offset, leaves, cycle,
                &dispatched[i])) == PrivateSpawnResult.spawned, "cancel tree root spawn failed");
        }
        while (park.live != 0)
        {
            pumpRoot(park, token, batch, rootHint);
            if (consumers == 0) while (view.consumeNext()) {}
            else pause();
            enforce(MonoTime.currTime < deadline, "cancel tree timed out");
        }
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
    ulong calls, operated;
    foreach (ref completion; completions) calls += completion.calls;
    foreach (value; dispatched) operated += value;
    enforce(calls == operated, "cancel tree dispatched count mismatch");
    immutable leafCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s tables=%s",
        mode, count, consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus, cancelTreeTables);
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f dispatched_fraction=%.4f verified_operated=%s",
        rounds, elapsed, leafCycles / elapsed / 1e6, elapsed * 1e9 / leafCycles,
        operated / (cast(double) count * cycle), operated);
}

// private-tracked (U5 churn): one tracked root per request with a deadline an
// hour away, so every cancel check reads the clock. The root checks cancelled
// once, operates and finishes; the drain pumps and takes each outcome and
// verifies it completed. Compare with `private` for the cost of tracking.
private PrivateRootDisposition trackedActivate(scope ref PrivateBorrow!State actor,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (context.cancelled) fatal("tracked root cancelled before its deadline");
    return privateActivate(actor, context);
}

private void runTracked(AntFarm* farm, ActorAllocator allocator, ref Token token,
    ref ConsumerView view, string mode, Completion[] completions, double duration,
    uint consumers, size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus,
    uint rootHint)
{
    immutable count = completions.length;
    PrivateRootOptions options;
    options.batchLimit = batch;
    options.allocator = allocator;
    options.tracking = true;
    auto park = PrivateRootPark!(State, trackedActivate).create(farm, count, options);
    enforce(park !is null, "tracked root park allocation failed");
    scope(exit) park.destroy();
    ulong cycle, outcomes;
    void runCycle()
    {
        ++cycle;
        auto deadline = MonoTime.currTime + 60.seconds;
        immutable due = MonoTime.currTime + 3600.seconds;
        foreach (i; 0 .. count)
        {
            PrivateRootHandle root;
            enforce(park.spawn(State(&completions[i], cycle), root, due) == PrivateSpawnResult.spawned,
                "tracked root spawn failed");
        }
        while (park.live != 0)
        {
            pumpRoot(park, token, batch, rootHint);
            if (consumers == 0) while (view.consumeNext()) {}
            else pause();
            PrivateRootOutcome outcome;
            while (park.takeOutcome(outcome))
            {
                if (outcome.status != PrivateOutcomeStatus.completed) fatal("tracked root outcome mismatch");
                ++outcomes;
            }
            enforce(MonoTime.currTime < deadline, "tracked lifecycle timed out");
        }
        foreach (ref completion; completions)
            enforce(atomicLoad!(MemoryOrder.acq)(completion.stamp) == cycle
                && completion.calls == cycle, "tracked count mismatch");
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
    enforce(outcomes == cycle * count, "tracked outcome count mismatch");
    immutable actorCycles = cast(double) count * rounds;
    writefln("mode=%s actors=%s consumers=%s batch=%s warmups=%s allocator=%s hugePages=%s cpus=%s",
        mode, count, consumers, batch, warmups, allocatorName, farm.usedLargePages, cpus);
    writefln("rounds=%s elapsed_s=%.6f Mactor_cycles/s=%.6f ns/actor_cycle=%.2f cycles/s=%.2f verified_outcomes=%s",
        rounds, elapsed, actorCycles / elapsed / 1e6, elapsed * 1e9 / actorCycles, rounds / elapsed, outcomes);
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
        ? s.children.startSubset!subsetOperate(context, s.members, g_childHint)
        : s.children.start!subsetOperate(context, false, g_childHint);
    if (result != PrivateStartResult.started) fatal("subset benchmark wave failed");
    s.inFlight = true;
    return PrivateRootDisposition.waitChildren;
}

private void runSubset(AntFarm* farm, ActorAllocator allocator, ref Token token,
    ref ConsumerView view, string mode, size_t count, double duration, uint consumers,
    size_t batch, size_t warmups, string allocatorName, const(ProcessorId)[] cpus, uint percent,
    uint rootHint)
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
        pumpRoot(park, token, batch, rootHint);
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
    ProcessorId cpu;
    bool unpinned;
    PinOutcome pin;

    this(AntFarm* farm, ProcessorId cpu, bool unpinned)
    {
        this.farm = farm;
        this.cpu = cpu;
        this.unpinned = unpinned;
    }

    void run()
    {
        markThreadpoolThread();
        if (!unpinned)
        {
            pin = pinProcessor(cpu);
            if (pin != PinOutcome.pinned)
            {
                atomicStore!(MemoryOrder.rel)(started, -1);
                return;
            }
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

// Root pump under the configured cost-hint policy: "auto" (the default)
// passes no hint so the runtime derives one per table from its entry count
// and the live consumer count; an explicit 0..MAX_AVG_COST keeps its literal
// writeTracked meaning, so a fixed hint names a different chunk on runtimes
// with different MAX_CHUNK (class k is chunk 32 >> k before MAX_CHUNK rose to
// 128 and 128 >> k after). The sentinel is local so
// this source also builds against pre-derivation runtimes; it is never
// passed to pump.
private enum uint rootHintAuto = uint.max;

// Public flush/wave work-cost class. Unset keeps the runtime default
// (ActorRuntime.flush / ActorWave.publish, chunk 8); a value pins
// MAX_CHUNK >> class. Option-screen hook so one binary can compare the
// default against an explicit class such as chunk 32 (class 2 at MAX_CHUNK
// 128, class 0 before MAX_CHUNK rose to 128).
private enum uint publicHintDefault = uint.max;

private uint publicHintFromEnv()
{
    import core.stdc.stdlib : getenv, strtoul;
    auto flag = getenv("ANTFARM_PUBLIC_HINT");
    if (flag is null) return publicHintDefault;
    char* end;
    immutable value = strtoul(flag, &end, 10);
    enforce(*end == 0 && value <= MAX_AVG_COST,
        "ANTFARM_PUBLIC_HINT must be 0..MAX_AVG_COST");
    return cast(uint) value;
}

// Child-wave work-cost class. Unset keeps the private-actor default (chunk
// 32); a value pins MAX_CHUNK >> class. Option-screen hook for the per-wave
// child cost hint on start/startFinal/startSubset.
private uint g_childHint = avgCostForChunk(32);

private uint childHintFromEnv()
{
    import core.stdc.stdlib : getenv, strtoul;
    auto flag = getenv("ANTFARM_CHILD_HINT");
    if (flag is null) return g_childHint;
    char* end;
    immutable value = strtoul(flag, &end, 10);
    enforce(*end == 0 && value <= MAX_AVG_COST,
        "ANTFARM_CHILD_HINT must be 0..MAX_AVG_COST");
    return cast(uint) value;
}

size_t pumpRoot(Park)(Park park, ref Token token, size_t batchLimit, uint hint)
{
    return hint == rootHintAuto ? park.pump(token, batchLimit)
        : park.pump(token, batchLimit, hint);
}

// Effective class a runtime publishes for a default-hint root table of
// `count` entries at `consumers` live consumers. Derivation runtimes expose
// rootCostHintFor; pre-derivation runtimes publish class 0 unconditionally.
// The benchmark always creates its Farm with the default small-table
// threshold.
private uint autoRootHint(size_t count, uint consumers)
{
    // Derivation runtimes expose rootCostHintFor in antfarm; the selective
    // top-level import would not build against pre-derivation runtimes, so
    // probe and import it locally. Pre-derivation runtimes publish class 0
    // unconditionally.
    static if (__traits(compiles, { import antfarm : rootCostHintFor; }))
    {
        import antfarm : rootCostHintFor;
        return rootCostHintFor(count, cast(ulong) consumers,
            DEFAULT_SMALL_TABLE_THRESHOLD);
    }
    else
        return 0;
}

private double elapsedSeconds(long ticks)
{
    return cast(double) ticks / MonoTime.ticksPerSecond;
}

void main(string[] args)
{
    markThreadpoolThread();
    enforce(args.length >= 2 && args.length <= 13
            && (args[1] == "private-tracked" || args[1] == "tree-cont" || args[1] == "tree-slab" || args[1] == "tree-wait-slab" || args[1] == "tree-cont-slab" || args[1] == "tree-refill" || args[1] == "tree-refill-slab" || args[1] == "tree-cancel" || args[1] == "tree-pipelined" || args[1] == "subset" || args[1] == "subset-whole" || args[1] == "subset-whole-unreserved" || args[1] == "dtor" || args[1] == "inbox" || args[1] == "actor" || args[1] == "wave" || args[1] == "private" || args[1] == "private-remote" || args[1] == "tree" || args[1] == "tree-wait"),
        "usage: actor_churn actor|inbox|wave|private|private-remote|private-tracked|tree|tree-wait|tree-cont|tree-slab|tree-wait-slab|tree-cont-slab|tree-refill|tree-refill-slab|tree-cancel|tree-pipelined|dtor|subset|subset-whole|subset-whole-unreserved [actors=4096] [seconds=3] [consumers=0] [batch=256] [warmups=5] [crt|mimalloc|mimalloc64|mimalloc-count] [controller-cpu,consumer-cpus (parseCpuList: 0,1-7 or g:lp; ordered)|-] [small|bulk] [quota-words=16384] [subset-percent=100] [root-hint=auto|0..7]");
    // Tree modes with their own run function, including the slab and refill
    // variants.
    immutable foldTreeMode = args[1] == "tree-cont" || args[1] == "tree-pipelined"
        || args[1] == "tree-cancel" || args[1] == "tree-slab" || args[1] == "tree-wait-slab"
        || args[1] == "tree-cont-slab" || args[1] == "tree-refill" || args[1] == "tree-refill-slab";
    immutable subsetMode = args[1] == "subset" || args[1] == "subset-whole"
        || args[1] == "subset-whole-unreserved";
    immutable trackedMode = args[1] == "private-tracked";
    immutable percent = subsetMode && args.length > 11 ? to!uint(args[11]) : 100;
    enforce(!subsetMode || args.length <= 11 || percent == 1 || percent == 10 || percent == 100,
        "subset-percent must be 1, 10 or 100, and is only for subset modes");
    immutable inboxMode = args[1] == "inbox";
    immutable waveMode = args[1] == "wave";
    immutable remoteMode = args[1] == "private-remote";
    immutable dtorMode = args[1] == "dtor";
    immutable privateMode = args[1] == "private" || remoteMode || dtorMode;
    immutable treeWaitMode = args[1] == "tree-wait";
    immutable treeMode = args[1] == "tree" || treeWaitMode;
    // Root work-cost hint policy: the trailing argument after subset-percent.
    // "auto" (default) publishes the runtime's derived hint; 0..MAX_AVG_COST pins the
    // literal writeTracked class (chunk MAX_CHUNK >> class).
    immutable rootHintArg = args.length > (subsetMode ? 12 : 11)
        ? args[subsetMode ? 12 : 11] : "auto";
    uint rootHint = rootHintAuto;
    if (rootHintArg != "auto")
    {
        rootHint = to!uint(rootHintArg);
        enforce(rootHint <= MAX_AVG_COST, "root-hint must be auto or 0..MAX_AVG_COST");
    }
    immutable publicHint = publicHintFromEnv();
    g_childHint = childHintFromEnv();
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
            && (waveMode || privateMode || treeMode || foldTreeMode || trackedMode || batch <= 256)
            && ((privateMode || treeMode || foldTreeMode || trackedMode || subsetMode)
                || args.length <= (subsetMode ? 12 : 11)),
        "actors/seconds/batch must be positive; consumers must be 0..64; actor batch must be <=256; root-hint is only for private-family modes");
    ProcessorId[] cpus;
    if (args.length > 8 && args[8] != "-")
    {
        cpus = parseCpuList(args[8]);
        enforce(cpus.length == consumers + 1,
            "CPU list must name the controller followed by each consumer");
        immutable controllerPin = pinProcessor(cpus[0]);
        enforce(controllerPin == PinOutcome.pinned,
            "controller CPU " ~ cpus[0].to!string ~ " pin failed: " ~ controllerPin.to!string);
    }
    scope (exit) reportProcess(allocatorName);
    auto allocator = ActorAllocator.cRuntime();
    if (allocatorName != "crt")
    {
        enforce(allocatorName == "mimalloc" || allocatorName == "mimalloc64"
                || allocatorName == "mimalloc-count",
            "allocator must be crt, mimalloc, mimalloc64, or mimalloc-count");
        version (AntfarmMimallocV3)
            allocator = allocatorName == "mimalloc" ? mimallocV3ActorAllocator()
                : allocatorName == "mimalloc64"
                    ? ActorAllocator(null, &mimalloc64Allocate, &mimalloc64Deallocate)
                    : ActorAllocator(null, &countingAllocate, &countingDeallocate);
        else
            enforce(false, "mimalloc requires the actor_churn_mimalloc build");
    }

    immutable tierName = args.length > 9 ? args[9] : "small";
    enforce(tierName == "small" || tierName == "bulk", "tier must be small or bulk");
    immutable tier = tierName == "small" ? Tier.small : Tier.bulk;
    immutable quota = args.length > 10 ? to!ulong(args[10]) : 16384;
    enforce(quota > 0, "quota must be positive");
    // ANTFARM_CHURN_HOST=1 (small tier, pinned consumers only): a FarmHost owns the
    // Farm, the control token and the pinned spinning consumers that the
    // hand-written block below otherwise creates.
    import core.stdc.stdlib : getenv;
    immutable hostSwitch = getenv("ANTFARM_CHURN_HOST") !is null && getenv("ANTFARM_CHURN_HOST")[0] == '1';
    immutable useHost = hostSwitch && tier == Tier.small && consumers > 0 && cpus.length == consumers + 1;
    enforce(!hostSwitch || useHost, "ANTFARM_CHURN_HOST=1 needs the small tier, consumers > 0 and a CPU list");
    FarmHost host;
    AntFarm* farm;
    if (useHost)
    {
        FarmHostOptions hostOptions;
        hostOptions.workers = consumers;
        hostOptions.caller = CallerPlacement.unchanged; // the controller is already pinned
        hostOptions.idle = WorkerIdle.spin;
        hostOptions.pool = PoolOptions.init;
        hostOptions.pool.processors = cpus[1 .. $];
        hostOptions.farm.ringMiB = 8;
        hostOptions.farm.k = 8;
        hostOptions.farm.quotaSmall = quota;
        host = new FarmHost(hostOptions);
        farm = host.farm;
        writefln("farm_host=1 %s", host.summary());
    }
    else
        farm = AntFarm.create(8, 8, consumers ? consumers : 1,
            tier == Tier.bulk ? 1 : 0, tier == Tier.bulk ? quota : 0,
            tier == Tier.small ? 1 : 0, tier == Tier.small ? quota : 0);
    enforce(farm !is null, "Farm allocation failed");
    writefln("batchLimit=%s tier=%s quota_words=%s public_hint=%s child_hint=%s", batch, tierName,
        quota, publicHint == publicHintDefault ? "default" : to!string(publicHint), g_childHint);
    // Effective root work-cost hint for this configuration's nominal first
    // root table (subset roots publish one entry; tree-family roots publish
    // min(batch, parents); private-family roots publish min(batch, actors)).
    // With consumers=0 the controller's inline view is the one subscriber.
    if (privateMode || treeMode || foldTreeMode || trackedMode || subsetMode)
    {
        immutable nominal = subsetMode ? 1
            : treeMode || foldTreeMode
                ? ((count - 1) / batch + 1 < batch ? (count - 1) / batch + 1 : batch)
            : count < batch ? count : batch;
        immutable hint = rootHint == rootHintAuto
            ? autoRootHint(nominal, consumers ? consumers : 1) : rootHint;
        immutable chunk = MAX_CHUNK >> hint;
        writefln("root_hint=%s root_cost_hint=%s root_hint_table=%s root_hint_claims=%s",
            rootHintArg, hint, nominal, (nominal + chunk - 1) / chunk);
    }
    version (AntfarmMimallocV3)
        if (allocatorName != "crt") writefln("mimalloc_version=%s", mi_version());
    scope (exit) { if (host !is null) host.close(); else farm.destroy(); }
    immutable noRuntime = privateMode || treeMode || subsetMode || foldTreeMode || trackedMode;
    auto runtime = noRuntime ? null : ActorRuntime.create(farm, count, allocator);
    enforce(noRuntime || runtime !is null, "actor runtime allocation failed");
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
    auto trees = treeMode && !treeWaitMode ? PrivateRootPark!(TreeState!false, treeActivate!false).create(farm, parents, batch, allocator) : null;
    auto waitingTrees = treeWaitMode ? PrivateRootPark!(TreeState!false, treeActivate!true).create(farm, parents, batch, allocator) : null;
    enforce(!treeMode || (treeWaitMode ? waitingTrees !is null : trees !is null), "private tree park allocation failed");
    scope (exit) if (trees !is null) trees.destroy();
    scope (exit) if (waitingTrees !is null) waitingTrees.destroy();
    auto owners = new ActorOwner!State[count];
    auto handles = new ActorHandle!State[count];
    auto completions = new Completion[count];
    auto nodes = inboxMode ? new ActorInboxNode[count] : null;
    foreach (ref node; nodes) node.initialize();
    Token ownToken;
    if (!useHost)
    {
        ownToken = farm.registerProducer(tier);
        enforce(ownToken.valid, "producer registration failed");
    }
    scope (exit) if (!useHost) farm.unregisterProducer(ownToken);
    ref Token token = useHost ? host.token : ownToken;
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
    foreach (i; 0 .. (useHost ? 0 : consumers))
    {
        jobs[i] = cpus.length == 0
            ? new Consumer(farm, ProcessorId.init, true)
            : new Consumer(farm, cpus[i + 1], false);
        threads[i] = new Thread(&jobs[i].run);
        threads[i].start();
        while (atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 0)
            Thread.yield();
        enforce(atomicLoad!(MemoryOrder.acq)(jobs[i].started) == 1,
            jobs[i].unpinned || jobs[i].pin == PinOutcome.pinned
                ? "consumer subscription failed"
                : "consumer CPU " ~ jobs[i].cpu.to!string ~ " pin failed: " ~ jobs[i].pin.to!string);
    }

    if (subsetMode)
    {
        runSubset(farm, allocator, token, view, args[1], count, duration, consumers,
            batch, warmups, allocatorName, cpus, percent, rootHint);
        return;
    }

    if (trackedMode)
    {
        runTracked(farm, allocator, token, view, args[1], completions, duration,
            consumers, batch, warmups, allocatorName, cpus, rootHint);
        return;
    }

    if (foldTreeMode)
    {
        if (args[1] == "tree-cancel")
            runCancelTree(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-slab")
            runTree!(false, true)(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-wait-slab")
            runTree!(true, true)(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-cont-slab")
            runFoldTree!(true, true)(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-refill")
            runRefill!false(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-refill-slab")
            runRefill!true(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else if (args[1] == "tree-cont")
            runFoldTree!true(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
        else
            runFoldTree!false(farm, allocator, token, view, args[1], completions, duration,
                consumers, batch, warmups, allocatorName, cpus, rootHint);
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

    size_t flushPublic(ActorRuntime* runtime, ref Token token, size_t maximum)
    {
        return publicHint == publicHintDefault
            ? runtime.flush(token, maximum)
            : runtime.flush(token, maximum, publicHint);
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
                    auto initial = TreeState!false(completions.ptr + offset, children, cycle, null);
                    enforce((treeWaitMode ? waitingTrees.spawn(initial) : trees.spawn(initial)) == PrivateSpawnResult.spawned,
                        "private parent creation failed");
                }
            while (privateMode ? (dtorMode ? dtorPark.live != 0 : remoteMode ? remotePark.live != 0 : park.live != 0) : (treeWaitMode ? waitingTrees.live != 0 : trees.live != 0))
            {
                if (privateMode) { if (dtorMode) pumpRoot(dtorPark, token, batch, rootHint); else if (remoteMode) pumpRoot(remotePark, token, batch, rootHint); else pumpRoot(park, token, batch, rootHint); }
                else if (treeWaitMode) pumpRoot(waitingTrees, token, batch, rootHint);
                else pumpRoot(trees, token, batch, rootHint);
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
                published += publicHint == publicHintDefault
                    ? wave.publish!operate(handles[published .. limit], token)
                    : wave.publish!operate(handles[published .. limit], token, publicHint);
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
                published += flushPublic(runtime, token, batch);
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
                flushPublic(runtime, token, batch);
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
