// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// The dispatcher: a `threadpool.groups.GroupHost` owns the pinned pool, the
/// caller's pin and the Farms; its workers plus the calling thread are the
/// participants, the caller as participant 0. Each participant finds its
/// home Farm through the host's `GroupSlot` registry and holds one producer
/// token per Farm it writes. With `DispatcherOptions.farmPerGroup` every
/// last-level-cache group has its own Farm, consumed only by its own
/// participants; otherwise all groups share one.
///
/// Scheduling order on every participant: pop the own deque (LIFO), poll the
/// home Farm every `pollEvery` pops or when the deque is empty, then steal:
/// the hot victim first, then the SMT sibling, then one random victim,
/// same-group with probability (1 - 1/crossDiv) and another group otherwise.
/// A full deque spills into the home Farm; a refusing Farm runs the children
/// inline, and so does a thread whose home Farm nobody else consumes (the
/// caller alone in its group without `callerConsumes`).
///
/// Parking: after `spinSteps` consecutive failed steps a worker returns
/// `false` to the pool, which arms its wait slot and its bit in the pool's
/// armed bitmap, rechecks once through the body and parks it on a futex
/// (`Director.wait`). A push onto an empty deque, a spill and a root
/// submission each wake one armed worker (`wakeOneArmed`: clear its bit,
/// `wakeWorker`); `wakeWorker` exchanges the armed word, so a wake that lands
/// before the futex wait is not lost, and a woken worker that forks wakes
/// the next one. A sleeper arms with locked RMWs and then rechecks; a waker
/// publishes with a plain release store and then reads the bitmap, which
/// x86 may reorder (store buffering), so both could miss. A spill and a root
/// submission, whose work no awake thread may otherwise take, fence before
/// the read; a push only loses parallelism if it misses, since its owner
/// pops it. Threads that are not participants submit
/// roots with `submitRoot` and wait on a `RootCompletion`.
///
/// Counters on the hot path (`pushed`, `popped`, `dequeHighWater`) are
/// compiled in only with `-version=ForkjoinHotStats` (`-d-version=` under LDC);
/// they cost 1% on fib
/// and 6% on skynet at 2 threads.
module forkjoin.dispatcher;

import antfarm;
import threadpool;
import threadpool.groups : GroupAttachment, GroupHost, GroupHostOptions, GroupSlot, groupFarmOf, groupToken;
import threadpool.host : HostGroups, CallerPlacement, WorkerIdle;
import forkjoin.frames : releaseFrames;
import forkjoin.node;
import forkjoin.deque;
import core.atomic;
import core.sync.event : Event;
import core.time : Duration;
version (linux) import threadpool.sys.linux_wait : wakeWorker;
version (Windows) import threadpool.sys.win_wait : wakeWorker;

/// Participants a dispatcher can have: the caller plus pool workers.
/// `Dispatcher.start()` without a processor list takes the first this many.
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
    /// The calling thread subscribes a view and polls the Farm during
    /// `runRoot`. Leave it false unless the caller keeps running roots: an
    /// idle subscribed view pins ring reclamation, and a stream of
    /// `submitRoot` calls from other threads then stalls once the ring laps.
    bool callerConsumes = false;
    /// Deque overflow runs inline on the forking thread (the default since
    /// 2.1.1); the children already on the deque stay stealable. `false`
    /// publishes the refused suffix into the home Farm as one table, and what
    /// the Farm refuses still runs inline. A subtree delivered through the
    /// Farm runs inside a Farm callback and cannot spill again, so on a
    /// 64-wide tree the Farm path measured 5 to 80% slower than inline at
    /// every thread count (fwbench `widetree`).
    bool spillInline = true;

    /// The documented default shape: one Farm shared by every group, workers
    /// pinned, idle workers parked. Equal to `DispatcherOptions.init`.
    static DispatcherOptions oneFarm() nothrow @nogc @safe { return DispatcherOptions.init; }

    /// The scaling shape: one Farm per LLC group, consumed only by its own
    /// participants, so spills and submissions stay inside a cache domain.
    /// `Dispatcher.start()` with no arguments uses it.
    static DispatcherOptions perGroup() nothrow @nogc @safe
    {
        DispatcherOptions o;
        o.farmPerGroup = true;
        return o;
    }
}

private __gshared GroupHost g_host;
private __gshared AntFarm*[maxParticipants] g_farms;    // g_host.farms
private __gshared size_t g_farmCount;
private __gshared uint[][] g_members;    // per LLC index: participant ids (caller is 0)
private __gshared Deque*[maxParticipants] g_deques;
private __gshared uint[][maxParticipants] g_cross;  // per participant: ids in other groups
private __gshared uint[maxParticipants] g_sibling;  // per participant: the SMT sibling's id, or uint.max
private __gshared size_t g_participants;
private __gshared DispatcherOptions g_opt;
private __gshared ushort g_submitLlc;         // the group whose Farm takes submitted roots
private __gshared Token g_submitToken;              // shared by foreign submitters under g_submitLock
private shared bool g_submitLock;
private shared uint g_epoch = 1;         // bumped by discardCounters; counts from earlier epochs are dropped

/// Counters summed over participants by `counters()`. The first three need
/// `-version=ForkjoinHotStats`.
struct Stats
{
    ulong pushed;           /// children pushed onto deques
    ulong popped;           /// nodes popped by their owners
    ulong stealAttempts;    /// steal probes
    ulong stealSuccesses;   /// probes that took a node
    ulong crossAttempts;    /// probes aimed at another LLC group
    ulong crossSuccesses;   /// of those, successful
    ulong siblingSuccesses; /// nodes taken from the SMT sibling
    ulong spilledTables;    /// Farm tables written on deque overflow
    ulong spilledChildren;  /// children in those tables
    ulong inlineChildren;   /// children run inline when the Farm refused
    ulong soloChildren;     /// children run inline with one participant
    ulong farmPolls;        /// `consumeNext` calls
    ulong farmHits;         /// of those, one that ran a payload
    ulong dequeHighWater;   /// sum over participants of each deque's deepest point
    ulong parks;            /// times a worker parked
    ulong wakes;            /// successful `wakeOne` calls
    ulong submits;          /// reserved
}
private shared Stats g_stats;

// Thread-local participant state.
private Deque* t_deque;
private AntFarm* t_farm;      // home Farm
private uint[] t_same;        // participant ids in the home group
private Token* t_token;       // home Farm token, owned by the GroupHost per thread
private ushort t_llc;         // home group
private bool t_inCallback;    // inside nodeCall: the view walk is running, do not re-enter it
private uint t_id;
private ConsumerView t_view;
private bool t_consumes;
private bool t_canSpill;      // some participant consumes t_farm: this one, or another in its group
private uint t_pops;
private uint t_hotVictim = uint.max;
private ulong t_rng;
private uint t_epoch;
private uint t_idle; // consecutive failed steps, drives the backoff
private bool t_recheck; // the next body call is the pool's armed recheck before parking
private uint t_failed; // consecutive failed steps across body calls, drives parking
private Stats t_stats;

/// The one Farm callback: a one-word body holding a `Node*`.
private extern (C) void nodeCall(const(ulong)* body, size_t) nothrow @nogc @system
{
    // A node arriving through the Farm runs inside consumeNext. A nested
    // runRoot from here steps without polling the view (SPEC 7c).
    immutable outer = t_inCallback;
    t_inCallback = true;
    runChain(cast(Node*) body[0]);
    t_inCallback = outer;
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
    // Off the hot path's code layout in release builds, where a call from a
    // non-participant still crashes on the null deque.
    version (assert)
        if (t_deque is null)
            fatal("forkjoin: fork off a participant; use runRoot, submitRoot or runRootAny");
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

/// Deque overflow: one table into the home Farm, then inline for what it
/// refuses. Inside a Farm callback everything runs inline: the nested wait
/// cannot poll the view that is running, so a child in the Farm could be
/// reachable by no one when this is the only active consumer. So does a
/// thread that does not consume its home Farm when no other participant
/// does either: the caller alone in its group under `farmPerGroup`, whose
/// Farm the host still creates.
private void spill(Node*[] kids) nothrow @nogc @system
{
    size_t done;
    while (!g_opt.spillInline && !t_inCallback && t_canSpill && done < kids.length)
    {
        auto n = cast(size_t) t_farm.write(&nodeCall, OneWordBodies(kids[done .. $]), 1,
            *t_token, g_opt.avgCost);
        if (n == 0) break;
        done += n;
        t_stats.spilledTables++;
    }
    t_stats.spilledChildren += done;
    if (done)
    {
        // Order the table's publication before the bitmap read (see the
        // module comment): the spiller may not consume its own Farm.
        atomicFence!(MemoryOrder.seq)();
        if (anyParked()) wakeOneIn(t_llc);
    }
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
    if (!t_consumes || t_inCallback) return false;
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

/// Wakes one parked participant of group `llc` if it has one, else any: a
/// table in that group's Farm is visible only to that group's participants.
private void wakeOneIn(ushort llc) nothrow @nogc @system
{
    if (!g_opt.park) return;
    if (wakeOneArmedIn(llc) || wakeOneArmed()) t_stats.wakes++;
}

/// Completion of a root submitted from a thread that is not a participant.
struct RootCompletion
{
    Node sentinel; /// the root's parent; its join signals `done`
    Event done;    /// set when the root completes; `waitRoot` blocks on it
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
    publishRoot(root);
}

/// Writes one root into the submit Farm under the submit lock. A refused
/// write means the ring is full of unconsumed tables: wake a consumer of
/// that group and retry, so parked workers drain it.
private void publishRoot(Node* root) nothrow @nogc @system
{
    Node*[1] one = [root];
    while (cas(&g_submitLock, false, true) == false) pause();
    for (;;)
    {
        auto n = g_farms[0].write(&nodeCall, OneWordBodies(one[]), 1, g_submitToken, g_opt.avgCost);
        if (n == 1) break;
        wakeOneIn(g_submitLlc);
        pause();
    }
    atomicStore!(MemoryOrder.rel)(g_submitLock, false);
    // Order the root's publication before the bitmap read (see the module
    // comment): no awake thread is bound to poll this Farm, and the caller
    // never does by default.
    atomicFence!(MemoryOrder.seq)();
    wakeOneIn(g_submitLlc);
}

/// Blocks until the root submitted with `c` completes.
void waitRoot(RootCompletion* c) nothrow @nogc @system
{
    c.done.wait();
    c.done.terminate();
}

/// Publishes `root` from a thread that is not a participant and does not
/// wait. Nothing signals its completion: as for every node, the root's
/// `join` runs after its children, so a root that forks finds out there,
/// while a root that forks nothing never has its `join` called and must
/// finish (release its frame, record its result) in `run` before calling
/// `complete`. `Dispatcher.stop` drains outstanding roots first.
void submitDetached(Node* root) nothrow @nogc @system
{
    if (t_deque !is null) fatal("submitDetached from a participant; use fork or runRoot");
    if (g_farmCount == 0) fatal("submitDetached before start");
    root.parent = null;
    root.depth = 0;
    atomicOp!"+="(g_rootsLive, 1);
    publishRoot(root);
}

/// Runs `root` to completion from any thread: a participant runs it and
/// helps (`runRoot`), any other thread submits it and waits (`submitRoot`,
/// `waitRoot`). The loops and `forkJoinInvoke` are built on it.
void runRootAny(Node* root) nothrow @nogc @system
{
    if (t_deque !is null)
        runRoot(root);
    else
    {
        RootCompletion c;
        submitRoot(root, &c);
        waitRoot(&c);
    }
}

/// A batch of roots submitted from a thread that is not a participant and
/// waited for together. `add` publishes a root at once; `wait` blocks until
/// every root added since the last `wait` has completed, after which the
/// group may be reused. Each root's frame is the caller's; its own `join`
/// runs when it completes, before the group is signalled. Wait before
/// `Dispatcher.stop`, which drains outstanding roots.
struct TaskGroup
{
    private Node sentinel;  // the roots' parent; holds one count for the group itself until wait
    private Event done;
    private bool open;

    // Members point at `sentinel`, and `done` is an OS event: a copy would
    // be a second group the roots never signal.
    @disable this(this);

    /// Publishes `root` as a member of the group and returns at once.
    void add(Node* root) nothrow @nogc @system
    {
        if (t_deque !is null) fatal("TaskGroup.add from a participant; use fork or runRoot");
        if (g_farmCount == 0) fatal("TaskGroup.add before start");
        if (!open)
        {
            open = true;
            sentinel = Node.init;
            sentinel.join = &groupJoin;
            atomicStore!(MemoryOrder.raw)(sentinel.pending, 1u); // the group's own hold
            done.initialize(true, false);
            atomicOp!"+="(g_rootsLive, 1); // one live root per armed group
        }
        atomicOp!"+="(sentinel.pending, 1u);
        root.parent = &sentinel;
        root.depth = 0;
        publishRoot(root);
    }

    /// Blocks until every root added since the last `wait` has completed.
    /// Returns at once when nothing was added.
    void wait() nothrow @nogc @system
    {
        if (!open) return;
        if (atomicOp!"-="(sentinel.pending, 1u) == 0)
            atomicOp!"-="(g_rootsLive, 1); // every root had finished; nobody ran the sentinel's join
        else
            done.wait();
        done.terminate();
        open = false;
    }
}

private Node* groupJoin(Node* node) nothrow @nogc @system
{
    auto g = cast(TaskGroup*) node;
    g.done.setIfInitialized();
    return null;
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
    t_llc = llc;
    t_farm = groupFarmOf(llc);
    if (t_farm is null) fatal("participant without a group Farm");
    t_same = g_members[llc];
    t_deque = g_deques[id];
    t_rng = 0x9E3779B97F4A7C15UL * (id + 1) | 1;
    t_hotVictim = uint.max;
    t_pops = 0;
    t_token = &groupToken(llc);
    t_consumes = consumes;
    // Workers always consume; without farmPerGroup every worker consumes the
    // one Farm, with it only the home group's members do.
    t_canSpill = consumes || (g_opt.farmPerGroup ? t_same.length > 1 : g_participants > 1);
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
    t_canSpill = false;
    releaseFrames(); // park this thread's frame slot for the next participant
    t_token = null; // released by the GroupHost after this hook
    t_deque = null;
    t_farm = null;
    t_same = null;
}

// ---------------------------------------------------------------------------

/// The dispatcher: start it, run or submit roots, stop it. One per process.
///
/// Two ways to run it. `start()`/`start(cpus, opt)` build a `GroupHost` the
/// dispatcher owns, with the caller pinned to `cpus[0]`. Or compose it on an
/// application-owned host: put `attachment(opt)` in `GroupHostOptions.
/// attachments` beside other attachments (`ant_fibers.groups.FiberGroups`,
/// or through `FarmHostOptions.attachments`), construct the host, then call
/// `start(host)` on the host's caller thread, which becomes participant 0.
/// That host must plan `farm.extraSmallProducers >= 1` for the shared submit
/// token, and `callerConsumes` to match `DispatcherOptions.callerConsumes`.
struct Dispatcher
{
    /// The host that owns the pool, the Farms and the caller's pin.
    GroupHost host;
    private bool ownsHost_;

    /// Starts the dispatcher on every eligible processor (the first
    /// `maxParticipants` in topology order): the calling thread on the
    /// first, a pool worker on each of the rest, one Farm per LLC group
    /// (`DispatcherOptions.perGroup()`).
    void start()
    {
        start(DispatcherOptions.perGroup());
    }

    /// As `start()`, with `opt` as given: `DispatcherOptions.oneFarm()`
    /// shares one Farm, `.perGroup()` gives each group its own.
    void start(DispatcherOptions opt)
    {
        auto topo = CacheAwarePool.topology();
        ProcessorId[] cpus;
        foreach (ref p; topo.processors)
            if (topo.isEligible(p) && cpus.length < maxParticipants) cpus ~= ProcessorId.of(p);
        start(cpus, opt);
    }

    /// The dispatcher as a `GroupAttachment` for an application-owned host.
    /// Its `install` builds the participant tables from the host's
    /// resolution (the caller is participant 0, pool worker i is i + 1) and
    /// registers the submit token on the first Farm; `uninstall` frees them
    /// after the pool stopped. Call `start(host)` once the host exists.
    GroupAttachment attachment(DispatcherOptions opt = DispatcherOptions.init)
    {
        import std.exception : enforce;
        enforce(g_participants == 0, "forkjoin: a dispatcher is already running");
        // Zero steps per body call would return to the pool without ever
        // stepping, so workers would spin and never take work.
        enforce(opt.bodySteps > 0, "forkjoin: bodySteps must be at least 1");
        g_opt = opt;
        GroupAttachment a;
        a.name = "forkjoin";
        a.body = &workerBody;
        a.stop = &workerStop;
        a.consumes = true;
        a.install = (GroupHost h) { installOn(h); };
        a.uninstall = (GroupHost h) { uninstallFrom(h); };
        return a;
    }

    /// Enter the calling thread as participant 0 of a host that installed
    /// `attachment`. The host stays the application's: `stop()` drains the
    /// roots and leaves, and the host's `close()` frees the dispatcher's
    /// tables through `uninstall`.
    void start(GroupHost host)
    {
        import std.exception : enforce;
        enforce(host !is null && g_participants != 0 && g_host is null,
            "forkjoin: start(host) needs a host that installed the dispatcher's attachment");
        ownsHost_ = this.host is host && ownsHost_; // set by start(cpus) for its own host
        this.host = host;
        g_host = host;
        enterThread(0, host.callerGroup, g_opt.callerConsumes || g_participants == 1);
    }

    private static void installOn(GroupHost host)
    {
        import std.exception : enforce;
        auto res = host.resolution;
        immutable count = res.workers.length + 1;
        enforce(count <= maxParticipants, "forkjoin: too many participants for the dispatcher");
        auto topo = CacheAwarePool.topology();
        // Participant ids: caller 0, pool worker i -> i + 1; group of each.
        auto llcOfId = new ushort[count];
        auto coreOf = new ulong[count];
        llcOfId[0] = host.callerGroup;
        coreOf[0] = ulong.max; // a confined caller has no one core: no SMT sibling
        if (host.callerCpus.length == 1)
            if (auto lp = topo.byId(host.callerCpus[0]))
                coreOf[0] = (cast(ulong) lp.group << 32) | (cast(ulong) lp.packageIndex << 16) | lp.coreIndex;
        foreach (i, id; res.workers)
        {
            auto lp = topo.byId(id);
            enforce(lp !is null, "forkjoin: a pool worker's processor is unknown");
            llcOfId[i + 1] = lp.llcIndex;
            // A core is unique per (group, package, core index): Linux core
            // ids repeat across packages, Windows ones across groups.
            coreOf[i + 1] = (cast(ulong) lp.group << 32) | (cast(ulong) lp.packageIndex << 16) | lp.coreIndex;
        }
        foreach (i; 0 .. count)
            g_deques[i] = Deque.create(g_opt.dequeCapacity);
        g_members = new uint[][](topo.llcCount);
        foreach (i; 0 .. count)
            g_members[llcOfId[i]] ~= cast(uint) i;
        foreach (i; 0 .. count)
        {
            g_cross[i] = null;
            foreach (j; 0 .. count)
                if (llcOfId[j] != llcOfId[i]) g_cross[i] ~= cast(uint) j;
            g_sibling[i] = uint.max;
            foreach (j; 0 .. count)
                if (j != i && coreOf[i] != ulong.max && coreOf[j] == coreOf[i]) g_sibling[i] = cast(uint) j;
        }
        g_farmCount = host.farms.length;
        foreach (i, f; host.farms) g_farms[i] = f;
        g_submitLlc = host.groups[0];
        g_submitToken = g_farms[0].registerProducer(Tier.small);
        enforce(g_submitToken.valid,
            "forkjoin: the host must plan farm.extraSmallProducers >= 1 for the submit token");
        g_participants = count;
    }

    private static void uninstallFrom(GroupHost host)
    {
        // After the pool stopped (workers left) and before the Farms die.
        if (g_submitToken.valid) g_farms[0].unregisterProducer(g_submitToken);
        g_submitToken = Token.init;
        foreach (i; 0 .. g_participants)
            if (g_deques[i] !is null) { g_deques[i].destroy(); g_deques[i] = null; }
        g_farms[] = null;
        g_farmCount = 0;
        g_participants = 0;
        g_host = null;
    }

    /// Starts the dispatcher on a `GroupHost` it owns. `cpus[0]` is the
    /// calling thread's processor, pinned here; the rest become pool workers.
    /// Throws if the host does not accept every requested processor.
    void start(const(ProcessorId)[] cpus, DispatcherOptions opt = DispatcherOptions.init)
    {
        import std.exception : enforce;
        enforce(cpus.length >= 1 && cpus.length <= maxParticipants, "forkjoin: bad participant count");
        immutable callerConsumes = opt.callerConsumes || cpus.length == 1;
        auto a = attachment(opt);
        scope (failure)
        {
            // Undo every stage so a later start can succeed. close joins the
            // workers and runs uninstall; without a host nothing ran install.
            if (host !is null) { host.close(); host = null; ownsHost_ = false; }
            g_host = null;
        }

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
        // every table they read is built in the attachment's install, from
        // the resolution, beforehand.
        auto res = GroupHost.resolve(ho);
        enforce(res.workers.length == cpus.length - 1,
            "forkjoin: the threadpool accepted " ~ res.summary());
        auto topo = CacheAwarePool.topology();
        foreach (id; cpus)
        {
            auto lp = topo.byId(id);
            enforce(lp !is null && topo.isEligible(*lp),
                "forkjoin: processor " ~ formatCpuList((&id)[0 .. 1]) ~ " is unknown or ineligible");
        }
        // The dispatcher is the host's one attachment: the raw lane installs
        // `workerBody` directly, so the burst loop is unchanged.
        ho.attachments = [a];
        // Owned from construction: a failure below closes it (joining the
        // workers) before the failure path frees the participant tables.
        host = new GroupHost(ho);
        ownsHost_ = true;
        enforce(host.resolution.workers == res.workers, "forkjoin: the host started a different selection: "
            ~ "planned " ~ res.summary() ~ "; started " ~ host.resolution.summary());
        start(host);
    }

    /// Drains every outstanding root (detached roots included) and leaves.
    /// On a host the dispatcher owns it then closes the host (pool shutdown
    /// runs each worker's stop hook) and frees the deques. On an
    /// application's host the workers keep running the host's other
    /// attachments; the host's `close()` frees the dispatcher's tables.
    void stop()
    {
        // Never started, a failed start, or stopped already: nothing to undo.
        if (g_participants == 0 || g_host is null) return;
        if (t_deque is null || t_id != 0) fatal("forkjoin: Dispatcher.stop off the thread that started it");
        while (atomicLoad!(MemoryOrder.acq)(g_rootsLive) != 0)
        {
            if (step()) t_idle = 0;
            else idlePause();
        }
        leaveThread();
        if (ownsHost_)
            host.close(); // joins workers, releases tokens, uninstalls, destroys the Farms
        host = null;
        ownsHost_ = false;
    }
}

/// Prints the counters as `name: value` lines, one per field, for a first
/// look at a run without glue code.
void printCounters(ref const Stats s) nothrow @nogc
{
    import core.stdc.stdio : printf;
    static foreach (m; __traits(allMembers, Stats))
        printf("%s: %llu\n", m.ptr, cast(ulong) __traits(getMember, s, m));
}

/// Discards every participant's counters so far: counts belong to the epoch
/// in which they were flushed, `counters` sums only the current one, and the
/// sum itself restarts from zero. Call before a measured region (after a
/// warmup), while no participant is leaving (participants flush only in
/// `Dispatcher.stop`). Without it, `counters` sums every run since the
/// process started.
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
