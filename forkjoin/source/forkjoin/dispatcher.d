// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// The dispatcher: pinned `CacheAwarePool` workers plus the calling thread
/// as participant 0, one `GroupBin` per last-level-cache group installed with
/// `makeBins`/`install` and found with `home!GroupBin` on workers and
/// `at!GroupBin` on the caller. A bin names its Farm and its participants.
/// With `DispatcherOptions.farmPerGroup` every group has its own Farm,
/// consumed only by its own participants; otherwise all bins share one.
///
/// Scheduling order on every participant: pop the own deque (LIFO), poll the
/// home Farm every `pollEvery` pops or when the deque is empty, then steal:
/// the hot victim first, then the SMT sibling, then one random victim,
/// same-group with probability (1 - 1/crossDiv) and another group otherwise.
/// A full deque spills into the home Farm; a refusing Farm runs the children
/// inline.
///
/// Parking: after `spinSteps` consecutive failed steps a worker returns
/// `false` to the pool, which arms its wait slot and its bit in the pool's
/// armed bitmap, rechecks once through the body and parks it on a futex
/// (`Director.wait`). A push onto an empty deque, a spill and a root
/// submission each wake one armed worker (`wakeOneArmed`: clear its bit,
/// `wakeWorker`); `wakeWorker` exchanges the armed word, so a wake that lands
/// before the futex wait is not lost, and a woken worker that forks wakes
/// the next one. Threads that are not participants submit
/// roots with `submitRoot` and wait on a `RootCompletion`.
///
/// Counters on the hot path (`pushed`, `popped`, `dequeHighWater`) are
/// compiled in only with `-version=ForkjoinHotStats` (`-d-version=` under LDC);
/// they cost 1% on fib
/// and 6% on skynet at 2 threads.
module forkjoin.dispatcher;

import antfarm;
import threadpool;
import threadpool.groups : GroupHost, GroupHostOptions, GroupSlot, groupFarmOf, groupToken;
import threadpool.host : HostGroups, CallerPlacement, WorkerIdle;
import forkjoin.node;
import forkjoin.deque;
import core.atomic;
import core.sync.event : Event;
import core.time : Duration;
version (linux) import threadpool.sys.linux_wait : wakeWorker;
version (Windows) import threadpool.sys.win_wait : wakeWorker;

enum maxParticipants = 128;

/// Tuning. Every field has a measured default from the fork-join benchmarks
/// on a two-CCD Ryzen 9 5900XT; see docs/design/forkjoin.md.
struct DispatcherOptions
{
    /// Ring size per Farm. Only roots, spills and remote submissions use it.
    uint ringMiB = 8;
    /// Small-tier quota per producer token, in ulongs.
    uint smallQuota = 4096;
    /// Deque capacity per participant, rounded up to a power of two.
    uint dequeCapacity = 4096;
    /// Local pops between Farm polls on a busy participant. Also keeps a
    /// busy participant's view advancing its reclamation pins.
    uint pollEvery = 256;
    /// Work-cost class of spilled tables (7: chunk 1, so siblings can split).
    uint avgCost = 7;
    /// One random steal probe in N goes to another LLC group; 0 picks
    /// victims uniformly over every participant.
    uint crossDiv = 8;
    /// Probe the SMT sibling's deque before a random victim.
    bool siblingFirst = true;
    /// Scheduling steps per worker-body call before returning to the pool.
    uint bodySteps = 1024;
    /// Cap of the exponential pause backoff after failed steps; 0 pauses once.
    uint idleMax = 0;
    /// Park idle workers on the pool's futex after `spinSteps`; false spins.
    bool park = true;
    /// Consecutive failed steps (whole idle bursts) before a worker parks.
    uint spinSteps = 4096;
    /// One Farm per LLC group instead of one shared Farm.
    bool farmPerGroup = false;
    /// The calling thread subscribes a view and polls the Farm too.
    bool callerConsumes = true;
    /// Test knob: deque overflow runs inline instead of going to the Farm.
    bool spillInline = false;
}

__gshared GroupHost g_host;
__gshared AntFarm*[maxParticipants] g_farms;    // g_host.farms
__gshared size_t g_farmCount;
__gshared uint[][maxParticipants] g_members;    // per LLC group: participant ids (caller is 0)
__gshared Deque*[maxParticipants] g_deques;
__gshared uint[][maxParticipants] g_cross;  // per participant: ids in other groups
__gshared uint[maxParticipants] g_sibling;  // per participant: the SMT sibling's id, or uint.max
__gshared size_t g_participants;
__gshared DispatcherOptions g_opt;
__gshared Token g_submitToken;              // shared by foreign submitters under g_submitLock
shared bool g_submitLock;
shared uint g_epoch;             // bumped before the timed run; counts from epoch 0 are discarded

struct Stats
{
    ulong pushed, popped, stealAttempts, stealSuccesses, crossAttempts,
        crossSuccesses, siblingSuccesses, spilledTables, spilledChildren, inlineChildren,
        soloChildren, farmPolls, farmHits, dequeHighWater, parks, wakes, submits;
}
shared Stats g_stats;

// Thread-local participant state.
Deque* t_deque;
AntFarm* t_farm;      // home Farm
uint[] t_same;        // participant ids in the home group
Token* t_token;       // home Farm token, owned by the GroupHost per thread
uint t_id;
ConsumerView t_view;
bool t_consumes;
uint t_pops;
uint t_hotVictim = uint.max;
ulong t_rng;
uint t_epoch;
uint t_idle; // consecutive failed steps, drives the backoff
bool t_recheck; // the next body call is the pool's armed recheck before parking
uint t_failed; // consecutive failed steps across body calls, drives parking
Stats t_stats;

/// The one Farm callback: a one-word body holding a `Node*`.
extern (C) void nodeCall(const(ulong)* body, size_t) nothrow @nogc @system
{
    runChain(cast(Node*) body[0]);
}

/// Publishes `kids` as children of `parent` and returns the one to run
/// next on this thread. After this the caller must not touch `parent`
/// except through the returned child's execution. The hot path is the push
/// alone; the single-participant case, a full deque and the wake of a parked
/// worker share one out-of-line slow path, since each rare branch left in
/// here showed up as a few percent on fib through code layout.
Node* fork(Node* parent, Node*[] kids) nothrow @nogc @system
{
    if (kids.length == 0) fatal("fork without children");
    auto depth = parent.depth + 1;
    foreach (k; kids)
    {
        k.parent = parent;
        k.depth = depth;
    }
    atomicStore!(MemoryOrder.raw)(parent.pending, cast(uint) kids.length);
    auto deferred = kids[0 .. $ - 1];
    if (deferred.length == 0) return kids[$ - 1];
    if (g_participants == 1)
    {
        // No thief exists, so the deque would only add barriers: run the
        // deferred children here, as citor's single-participant path does.
        // Stack depth is then the tree depth, as in the payload port. Kept
        // inline: as a separate function this cost 12% more instructions.
        t_stats.soloChildren += deferred.length;
        foreach (k; deferred)
            runChain(k);
        return kids[$ - 1];
    }
    long depthNow;
    auto n = t_deque.pushAll(deferred, depthNow);
    version (ForkjoinHotStats)
    {
        t_stats.pushed += n;
        if (cast(ulong) depthNow > t_stats.dequeHighWater) t_stats.dequeHighWater = depthNow;
    }
    if (n < deferred.length || (cast(size_t) depthNow == n && anyParked()))
        forkSlow(deferred[n .. $], cast(size_t) depthNow == n);
    return kids[$ - 1];
}

/// Deque overflow and the empty-deque wake, out of the hot path.
pragma(inline, false)
private void forkSlow(Node*[] refused, bool wasEmpty) nothrow @nogc @system
{
    if (refused.length) spill(refused);
    if (wasEmpty && anyParked()) wakeOne(); // new stealable work for a sleeper
}

private struct OneWordBodies
{
    Node*[] nodes;
    bool empty() const nothrow @nogc @safe { return nodes.length == 0; }
    PayloadBody front() const nothrow @nogc @trusted
    {
        return (cast(const(ulong)*) &nodes[0])[0 .. 1];
    }
    void popFront() nothrow @nogc @safe { nodes = nodes[1 .. $]; }
    OneWordBodies save() const nothrow @nogc @trusted { return OneWordBodies(cast(Node*[]) nodes); }
    size_t length() const nothrow @nogc @safe { return nodes.length; }
}

/// Deque overflow: one table into the home Farm, then inline for what it refuses.
private void spill(Node*[] kids) nothrow @nogc @system
{
    size_t done;
    while (!g_opt.spillInline && done < kids.length)
    {
        auto n = cast(size_t) t_farm.write(&nodeCall, OneWordBodies(kids[done .. $]), 1,
            *t_token, g_opt.avgCost);
        if (n == 0) break;
        done += n;
        t_stats.spilledTables++;
    }
    t_stats.spilledChildren += done;
    if (done && anyParked()) wakeOne();
    if (done < kids.length)
    {
        t_stats.inlineChildren += kids.length - done;
        foreach (k; kids[done .. $])
            runChain(k);
    }
}

private ulong xorshift(ref ulong s) nothrow @nogc @safe
{
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    return s;
}

private Node* stealFrom(uint v) nothrow @nogc @system
{
    if (auto x = g_deques[v].steal())
    {
        t_hotVictim = v;
        t_stats.stealSuccesses++;
        return x;
    }
    return null;
}

private Node* steal() nothrow @nogc @system
{
    if (g_participants < 2) return null;
    t_stats.stealAttempts++;
    if (t_hotVictim != uint.max)
    {
        if (auto x = g_deques[t_hotVictim].steal())
        {
            t_stats.stealSuccesses++;
            return x;
        }
        t_hotVictim = uint.max;
    }
    // The SMT sibling shares L1 and L2: work taken from it is work on
    // neighbouring data. Probe it before any random victim.
    if (g_opt.siblingFirst && g_sibling[t_id] != uint.max)
        if (auto x = stealFrom(g_sibling[t_id]))
        {
            t_stats.siblingSuccesses++;
            return x;
        }
    auto r = xorshift(t_rng);
    auto cross = g_cross[t_id];
    auto same = t_same;
    bool goCross;
    if (g_opt.crossDiv == 0)
        goCross = (r >> 32) % g_participants < cross.length;     // uniform over everyone
    else
        goCross = cross.length != 0 && (same.length < 2 || (r >> 32) % g_opt.crossDiv == 0);
    if (goCross)
    {
        t_stats.crossAttempts++;
        auto v = cross[r % cross.length];
        auto x = stealFrom(v);
        if (x !is null) t_stats.crossSuccesses++;
        return x;
    }
    if (same.length < 2) return null;
    auto v = same[r % same.length];
    if (v == t_id) v = same[(r + 1) % same.length];
    return stealFrom(v);
}

private bool pollFarm() nothrow @nogc @system
{
    if (!t_consumes) return false;
    t_stats.farmPolls++;
    if (t_view.consumeNext())
    {
        t_stats.farmHits++;
        return true;
    }
    return false;
}

/// Discards this thread's counters from before the current epoch.
private void syncEpoch() nothrow @nogc @system
{
    auto epoch = atomicLoad!(MemoryOrder.raw)(g_epoch);
    if (epoch != t_epoch)
    {
        t_epoch = epoch;
        t_stats = Stats.init;
    }
}

/// One scheduling step; returns whether it ran anything. Kept out of line:
/// inlined into the worker body's loop it cost 8% more cycles on fib at 32
/// threads through register pressure, at the same instruction count.
pragma(inline, false)
bool step() nothrow @nogc @system
{
    syncEpoch();
    auto n = t_deque.pop();
    if (n !is null)
    {
        version (ForkjoinHotStats) t_stats.popped++;
        if (++t_pops >= g_opt.pollEvery)
        {
            t_pops = 0;
            pollFarm();
        }
        runChain(n);
        return true;
    }
    if (pollFarm()) return true;
    n = steal();
    if (n is null) return false;
    runChain(n);
    return true;
}

/// Exponential pause backoff after consecutive failed steps, capped at
/// `g_opt.idleMax` pauses per burst. Idle spinners otherwise take issue slots
/// from the SMT sibling that is doing the work.
private void idlePause() nothrow @nogc @system
{
    uint n = 1u << (t_idle < 12 ? t_idle : 12);
    if (n > g_opt.idleMax) n = g_opt.idleMax;
    if (n == 0) n = 1;
    foreach (_; 0 .. n) pause();
    if (t_idle < 12) t_idle++;
}

/// True on a participant: the caller after `start`, or a pool worker.
bool isParticipant() nothrow @nogc @system
{
    return t_deque !is null;
}

private struct RootWait
{
    Node sentinel;
    shared uint done;
}

private Node* rootWaitJoin(Node* node) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)((cast(RootWait*) node).done, 1u);
    return null;
}

/// Runs `root` on the calling participant, then helps until this root
/// completes. Waits on the root's own completion, not on every root, so it
/// may be called from inside a node's `run` (the frame of the enclosing
/// node stays on the stack until the inner tree joins; prefer `fork`).
void runRoot(Node* root) nothrow @nogc @system
{
    if (t_deque is null) fatal("forkjoin: runRoot off a participant; use submitRoot");
    syncEpoch();
    RootWait w;
    w.sentinel.join = &rootWaitJoin;
    atomicStore!(MemoryOrder.raw)(w.sentinel.pending, 1u);
    root.parent = &w.sentinel;
    root.depth = 0;
    atomicOp!"+="(g_rootsLive, 1);
    runChain(root);
    while (atomicLoad!(MemoryOrder.acq)(w.done) == 0)
    {
        if (step()) t_idle = 0;
        else idlePause();
    }
}

// ---------------------------------------------------------------------------
// Parking and waking: the pool keeps one bit per armed or parked worker
// (threadpool.hybrid.gArmedBits); `anyArmed` is the hot-path test and
// `wakeOneArmed` clears a bit and wakes that worker.

private bool anyParked() nothrow @nogc @system
{
    return anyArmed();
}

/// Wakes one parked participant, if any.
void wakeOne() nothrow @nogc @system
{
    if (!g_opt.park) return;
    if (wakeOneArmed()) t_stats.wakes++;
}

/// Completion of a root submitted from a thread that is not a participant.
struct RootCompletion
{
    Node sentinel;
    Event done;
}

private Node* completionJoin(Node* node) nothrow @nogc @system
{
    auto c = cast(RootCompletion*) node;
    c.done.setIfInitialized();
    return null;
}

/// Publishes `root` from a thread that is not a participant and returns at
/// once; `waitRoot` blocks until it completes. The root goes into the first
/// Farm as a one-entry table through a token shared under a spinlock, then
/// one parked worker is woken; an awake worker finds it on its Farm poll.
void submitRoot(Node* root, RootCompletion* c) nothrow @nogc @system
{
    if (t_deque !is null) fatal("submitRoot from a participant; use runRoot");
    if (g_farmCount == 0) fatal("submitRoot before start");
    c.sentinel = Node.init;
    c.sentinel.join = &completionJoin;
    atomicStore!(MemoryOrder.raw)(c.sentinel.pending, 1u);
    c.done.initialize(true, false);
    root.parent = &c.sentinel;
    root.depth = 0;
    atomicOp!"+="(g_rootsLive, 1);
    Node*[1] one = [root];
    while (cas(&g_submitLock, false, true) == false) pause();
    for (;;)
    {
        auto n = g_farms[0].write(&nodeCall, OneWordBodies(one[]), 1, g_submitToken, g_opt.avgCost);
        if (n == 1) break;
        pause();
    }
    atomicStore!(MemoryOrder.rel)(g_submitLock, false);
    wakeOne();
}

void waitRoot(RootCompletion* c) nothrow @nogc @system
{
    c.done.wait();
    c.done.terminate();
}

/// Publishes `root` from a thread that is not a participant and does not
/// wait. The root's own `join` is its completion callback and owns the
/// frame's lifetime. `Dispatcher.stop` drains outstanding roots first.
void submitDetached(Node* root) nothrow @nogc @system
{
    if (t_deque !is null) fatal("submitDetached from a participant; use fork or runRoot");
    if (g_farmCount == 0) fatal("submitDetached before start");
    root.parent = null;
    root.depth = 0;
    atomicOp!"+="(g_rootsLive, 1);
    Node*[1] one = [root];
    while (cas(&g_submitLock, false, true) == false) pause();
    for (;;)
    {
        auto n = g_farms[0].write(&nodeCall, OneWordBodies(one[]), 1, g_submitToken, g_opt.avgCost);
        if (n == 1) break;
        pause();
    }
    atomicStore!(MemoryOrder.rel)(g_submitLock, false);
    wakeOne();
}

// ---------------------------------------------------------------------------
// Threadpool worker hooks

/// Runs bursts of `g_opt.bodySteps` steps, so the pool's loop and this frame are
/// paid once per burst rather than once per node (returning after every
/// step cost 12% on fib). Returns `true` after a burst that ran work. It
/// returns `false` only through the parking protocol: after `g_opt.spinSteps`
/// consecutive failed steps (whole idle bursts, counted across calls), so the
/// pool arms the wait slot and calls back once; the recheck sets the parked
/// bit and returns `false` again, and the pool parks the worker. With
/// parking off it returns `false` after one idle burst and the pool's spin
/// policy runs.
private bool workerBody(WorkerSelf* w) nothrow @nogc @system
{
    if (t_deque is null)
        enterThread(w.workerIndex + 1, w.llcIndex, true);
    if (t_recheck)
    {
        // The pool armed this worker's wait slot (and its bit) before this
        // call; returning false now parks it.
        t_recheck = false;
        foreach (_; 0 .. 4)
            if (step())
            {
                t_idle = 0;
                t_failed = 0;
                return true;
            }
        t_stats.parks++;
        return false;
    }
    // The burst loop is kept exactly in this shape: adding a per-step
    // thread-local store and a second jump to it cost 7% on fib at 32
    // threads. Failure accounting happens at burst boundaries: a burst that
    // ran nothing is g_opt.bodySteps consecutive failures.
    uint idleSteps; // touched only on the cold path, so `any` needs no store per step
    foreach (_; 0 .. g_opt.bodySteps)
    {
        if (step())
            t_idle = 0;
        else
        {
            idlePause();
            idleSteps++;
        }
    }
    if (idleSteps != g_opt.bodySteps)
    {
        t_failed = 0;
        return true;
    }
    if (!g_opt.park) return false;
    t_failed += g_opt.bodySteps;
    if (t_failed >= g_opt.spinSteps)
    {
        t_failed = 0;
        t_recheck = true;
        return false;
    }
    return true; // not at the budget yet: keep the pool calling without arming
}

private void workerStop(WorkerSelf*) nothrow @nogc @system
{
    leaveThread();
}

private void enterThread(uint id, ushort llc, bool consumes) nothrow @nogc @system
{
    t_id = id;
    t_farm = groupFarmOf(llc);
    if (t_farm is null) fatal("participant without a group Farm");
    t_same = g_members[llc];
    t_deque = g_deques[id];
    t_rng = 0x9E3779B97F4A7C15UL * (id + 1) | 1;
    t_hotVictim = uint.max;
    t_pops = 0;
    t_token = &groupToken(llc);
    t_consumes = consumes;
    if (consumes && t_view.subscribe(t_farm) < 0)
        fatal("consumer subscription failed");
}

private void leaveThread() nothrow @nogc @system
{
    if (t_deque is null) return;
    if (t_epoch == atomicLoad(g_epoch) && t_epoch != 0)
    {
        static foreach (m; __traits(allMembers, Stats))
            atomicOp!"+="(__traits(getMember, g_stats, m), __traits(getMember, t_stats, m));
    }
    t_stats = Stats.init;
    if (t_consumes) t_view.unsubscribe();
    t_consumes = false;
    t_token = null; // released by the GroupHost after this hook
    t_deque = null;
    t_farm = null;
    t_same = null;
}

// ---------------------------------------------------------------------------

struct Dispatcher
{
    GroupHost host;

    /// Starts the dispatcher on a `GroupHost`. `cpus[0]` is the calling
    /// thread's processor, pinned here; the rest become pool workers. Throws
    /// if the host does not accept every requested processor.
    void start(const(ProcessorId)[] cpus, DispatcherOptions opt = DispatcherOptions.init)
    {
        import std.exception : enforce;
        enforce(cpus.length >= 1 && cpus.length <= maxParticipants, "forkjoin: bad participant count");
        enforce(g_participants == 0, "forkjoin: a dispatcher is already running");
        g_opt = opt;
        immutable callerConsumes = opt.callerConsumes || cpus.length == 1;

        GroupHostOptions ho;
        ho.workers = cast(uint)(cpus.length - 1);
        ho.groups = HostGroups.all();
        ho.callerProcessor = cpus[0];
        ho.callerConsumes = callerConsumes;
        ho.idle = opt.park ? WorkerIdle.park : WorkerIdle.spin;
        ho.idleTimeout = Duration.max; // park until woken; wakeOne does the waking
        ho.pool.skipSmtSiblings = false;
        ho.pool.processors = cpus[1 .. $];
        ho.pool.pSpinIters = 1;
        ho.pool.eSpinIters = 1;
        ho.farm.ringMiB = opt.ringMiB;
        ho.farm.quotaSmall = opt.smallQuota;
        ho.farm.extraSmallProducers = 1; // the shared submit token
        ho.farmPerGroup = opt.farmPerGroup;
        // Workers run their first body call inside the host constructor, so
        // every table they read is built from the resolution beforehand.
        auto res = GroupHost.resolve(ho);
        enforce(res.workers.length == cpus.length - 1,
            "forkjoin: the threadpool accepted " ~ res.summary());
        auto topo = CacheAwarePool.topology();

        // Participant ids: caller 0, pool worker i -> i + 1; group of each.
        g_participants = cpus.length;
        auto idOf = new ProcessorId[cpus.length];
        idOf[0] = cpus[0];
        foreach (i, id; res.workers) idOf[i + 1] = id;
        auto llcOfId = new ushort[cpus.length];
        auto coreOf = new ushort[cpus.length];
        foreach (i; 0 .. cpus.length)
        {
            auto lp = topo.byId(idOf[i]);
            llcOfId[i] = lp.llcIndex;
            coreOf[i] = lp.coreIndex;
            g_deques[i] = Deque.create(opt.dequeCapacity);
        }
        foreach (ref m; g_members) m = null;
        foreach (i; 0 .. cpus.length)
            g_members[llcOfId[i]] ~= cast(uint) i;
        foreach (i; 0 .. cpus.length)
        {
            g_cross[i] = null;
            foreach (j; 0 .. cpus.length)
                if (llcOfId[j] != llcOfId[i]) g_cross[i] ~= cast(uint) j;
            g_sibling[i] = uint.max;
            foreach (j; 0 .. cpus.length)
                if (j != i && coreOf[j] == coreOf[i]) g_sibling[i] = cast(uint) j;
        }
        host = new GroupHost(ho, &workerBody, &workerStop);
        g_host = host;
        enforce(host.resolution.workers == res.workers, "forkjoin: the host started a different selection");
        g_farmCount = host.farms.length;
        foreach (i, f; host.farms) g_farms[i] = f;
        g_submitToken = g_farms[0].registerProducer(Tier.small);
        if (!g_submitToken.valid) fatal("submit token registration failed");
        enterThread(0, llcOfId[0], callerConsumes);
    }

    /// Drains every outstanding root (detached roots included), then closes
    /// the host (pool shutdown runs each worker's stop hook), leaves, and
    /// destroys the deques.
    void stop()
    {
        while (atomicLoad!(MemoryOrder.acq)(g_rootsLive) != 0)
        {
            if (step()) t_idle = 0;
            else idlePause();
        }
        g_farms[0].unregisterProducer(g_submitToken);
        leaveThread();
        host.close(); // joins workers, releases tokens, destroys the Farms
        g_host = null;
        foreach (i; 0 .. g_participants)
        {
            g_deques[i].destroy();
            g_deques[i] = null;
        }
        g_farmCount = 0;
        g_participants = 0;
    }
}

/// Discards every participant's counters so far: counts belong to the epoch
/// in which they were flushed, `counters` sums only the current one, and the
/// sum itself restarts from zero. Call before a measured region, while no
/// participant is leaving (participants flush only in `Dispatcher.stop`).
void discardCounters() nothrow @nogc
{
    atomicOp!"+="(g_epoch, 1);
    static foreach (m; __traits(allMembers, Stats))
        atomicStore(__traits(getMember, g_stats, m), 0UL);
}

/// Counters summed over every participant that has left (after
/// `Dispatcher.stop`) for the current epoch. `pushed`, `popped` and
/// `dequeHighWater` need `-version=ForkjoinHotStats`.
Stats counters() nothrow @nogc
{
    return cast(Stats) g_stats;
}
