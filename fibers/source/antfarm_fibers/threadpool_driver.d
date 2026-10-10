/++ Tight binding between Ant Farm fiber lanes and threadpool_llc workers. +/
module antfarm_fibers.threadpool_driver;

import antfarm;
import antfarm_fibers.cacheline;
import antfarm_fibers.scheduler;
import core.atomic;
import threadpool;

/// Who converts an external fiber readiness transition into a pool wake.
/// `broadcast` preserves the eager compatibility behavior. `directorOwned`
/// records events but leaves worker selection and signalling to the thread
/// which owns threadpool's Director.
enum FiberWakePolicy : ubyte
{
    broadcast,
    directorOwned,
}

/// Readiness classes accumulated for a Director-side policy loop.
enum FiberWakeEvent : uint
{
    none         = 0,
    spawned      = 1 << 0,
    signalled    = 1 << 1,
    cancelled    = 1 << 2,
    shutdown     = 1 << 3,
    published    = 1 << 4,
    lifecycle    = 1 << 5,
    remoteReady  = 1 << 6,
}

enum FiberCoverage : int
{
    noResponder = -2,
    noWorkers = -1,
    pending = 0,
    covered = 1,
}

/// Starting points for the two observed workload extremes. They tune only
/// worker selection and Farm table sizing; callers retain every other option.
enum FiberTopologyPreset : ubyte
{
    fiberThroughput,
    payloadThroughput,
}

/// Called once for each worker during managed start. Returning true makes the
/// worker responsible for timer polling/deadlines and active Farm-leftover
/// sweeping. Null selects every worker, preserving the original behavior.
alias FiberResponderSelector = bool delegate(WorkerSelf* worker);

/// Optional edge notification for a Director/control event loop. The hook is
/// called after the wake mailbox changes atomically from empty to nonempty.
/// It may only signal caller-owned machinery; it cannot allocate or throw.
alias FiberWakeNotifier = void function(void* context) nothrow @nogc;

align(64) private struct FiberLaneControl
{
    shared uint pendingWakeEvents;
    ubyte[60] padding;
    shared uint startedWorkers;
    shared uint responderCount;
    shared int coverageState;
    ubyte[52] coveragePadding;
}
static assert(FiberLaneControl.sizeof == 2 * cacheLineSize);

align(64) private struct FiberCoverageControl
{
    shared uint startedWorkers;
    shared int invalidActiveLane;
    shared int ready;
    ubyte[52] padding;
}
static assert(FiberCoverageControl.sizeof == cacheLineSize);

private __gshared FiberLane[] installedFiberLanes;
private __gshared CacheAwarePool installedFiberPool;
private __gshared FiberCoverageControl* fiberCoverageControl;

/// One scheduler/farm pairing installed on an LLC bin. The caller owns the
/// farm lifetime and must keep it alive until the pool has stopped.
final class FiberLane : FiberReadyLane
{
    private FiberLaneControl* control;
    FiberBackend backend;
    private CacheAwarePool pool;
    size_t flushBatch = 32;
    uint avgCost = avgCostForChunk(8);
    FiberWakePolicy wakePolicy = FiberWakePolicy.broadcast;
    FiberResponderSelector responderSelector;
    private FiberWakeNotifier wakeNotifier;
    private void* wakeNotifierContext;
    private FiberLane[] coveredLanes_;
    private FiberLane[] coveringLanes_;

    @property private ref shared(uint) pendingWakeEvents() const return nothrow @nogc
    { return (cast(FiberLaneControl*) control).pendingWakeEvents; }

    @property FiberCoverage coverage() const nothrow @nogc
    {
        return cast(FiberCoverage) atomicLoad!(MemoryOrder.acq)(
            control.coverageState);
    }

    @property uint managedWorkers() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(control.startedWorkers);
    }

    @property uint responders() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(control.responderCount);
    }

    @property FiberDomain domain() nothrow @nogc { return backend; }

    /// Remote Farms this lane's responders also consume and flush.
    @property FiberLane[] coveredLanes() nothrow @nogc { return coveredLanes_; }

    /// Lanes whose responders sweep this Farm when native workers are absent
    /// or delayed.
    @property FiberLane[] coveringLanes() nothrow @nogc { return coveringLanes_; }

    /// Setup-time only: this lane's responders become persistent remote
    /// consumers of `lane`. Both lanes must share a `FiberDomain`.
    void cover(FiberLane lane)
    {
        if (lane is null)
            throw new Exception("antfarm_fibers: cannot cover a null lane");
        if (lane is this)
            throw new Exception("antfarm_fibers: a lane cannot cover itself");
        if (lane.backend !is backend)
            throw new Exception(
                "antfarm_fibers: remote sweepers must share a domain");
        addCoverage(lane);
    }

    package void addCoverage(FiberLane lane)
    {
        foreach (existing; coveredLanes_)
            if (existing is lane) return;
        coveredLanes_ ~= lane;
        foreach (existing; lane.coveringLanes_)
            if (existing is this) return;
        lane.coveringLanes_ ~= this;
    }

    this(AntFarm* farm, size_t accumulatorCapacity = 65_536)
    {
        super(farm);
        control = allocateCacheLine!FiberLaneControl();
        // Retained for source compatibility; intrusive queues are unbounded.
        cast(void) accumulatorCapacity;
        backend = new FiberDomain;
        backend.attachReadyLane(this);
    }

    /// Attach this LLC-local transport to an existing scheduler domain.
    this(FiberDomain domain, AntFarm* farm)
    {
        super(farm);
        if (domain is null)
            throw new Exception("antfarm_fibers: FiberLane requires a domain");
        control = allocateCacheLine!FiberLaneControl();
        backend = domain;
        backend.attachReadyLane(this);
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

    /// Associate the lane with its worker pool so external readiness changes
    /// wake workers without taking ownership of Director policy.
    void bindPool(CacheAwarePool pool)
    {
        if (installedFiberLanes.length != 0 && installedFiberPool !is pool)
            resetFiberCoverage(pool);
        else
            this.pool = pool;
    }

    /// Configure the empty-to-nonempty mailbox edge notification. Setup-time
    /// only: do not replace the hook/context while producers or workers run.
    void setWakeNotifier(FiberWakeNotifier notifier, void* context = null)
        nothrow @nogc
    {
        wakeNotifierContext = context;
        wakeNotifier = notifier;
    }

    FiberTask spawn(void delegate() body, size_t stackSize = 0)
    {
        enforceSpawnCoverage();
        auto task = backend.spawnOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return task;
    }

    FiberTask spawn(void function() body, size_t stackSize = 0)
    {
        enforceSpawnCoverage();
        auto task = backend.spawnOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return task;
    }

    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        enforceSpawnCoverage();
        auto handle = backend.spawnHandleOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return handle;
    }

    TaskHandle spawnHandle(void function() body, size_t stackSize = 0)
    {
        enforceSpawnCoverage();
        auto handle = backend.spawnHandleOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return handle;
    }

    void enableLifecycleEvents(size_t retentionLimit = defaultLifecycleRetention)
    {
        backend.enableLifecycleEvents(retentionLimit);
    }

    FiberLifecycleEvent[] takeLifecycleEvents(size_t maximum = size_t.max)
    {
        return backend.takeLifecycleEvents(maximum);
    }

    protected override void noteExternalReady() nothrow @nogc
    {
        noteWake(FiberWakeEvent.signalled);
    }

    size_t signal(Signal signal)
    {
        return backend.signal(signal);
    }

    bool cancel(FiberTask task)
    {
        return requestCancel(task) == CancellationSubmission.queued;
    }

    bool cancel(TaskHandle handle)
    {
        return requestCancel(handle) == CancellationSubmission.queued;
    }

    CancellationSubmission requestCancel(FiberTask task)
    {
        return task is null ? CancellationSubmission.staleHandle
                            : requestCancel(task.handle);
    }

    CancellationSubmission requestCancel(TaskHandle handle)
    {
        immutable submitted = backend.requestCancel(handle);
        if (submitted == CancellationSubmission.queued)
            noteWake(FiberWakeEvent.cancelled);
        return submitted;
    }

    /// Director-side application of all queued remote requests. Each result
    /// names the exact generation which was submitted.
    CancellationApplication[] drainCancellationRequests()
    {
        return backend.drainCancellationRequests();
    }

    void beginShutdown(bool cancelActive = false)
    {
        backend.beginShutdown(cancelActive);
        if (cancelActive) noteWake(FiberWakeEvent.shutdown);
    }

    /// Atomically take all readiness classes recorded since the previous
    /// call. Intended for the same control thread which owns the Director:
    /// it can map these bits to filtered `Selection.signal()` calls.
    FiberWakeEvent takeWakeEvents() nothrow @nogc
    {
        return cast(FiberWakeEvent) atomicExchange!(MemoryOrder.acq_rel)(
            &control.pendingWakeEvents, 0u);
    }

    package void noteWake(FiberWakeEvent event, bool relay = true) nothrow @nogc
    {
        immutable bits = cast(uint) event;
        auto observed = atomicLoad!(MemoryOrder.acq)(control.pendingWakeEvents);
        while (true)
        {
            immutable combined = observed | bits;
            if (combined == observed) break;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &control.pendingWakeEvents, observed, combined))
            {
                // A take racing this edge may empty the mailbox before the
                // call. That yields a harmless spurious notification; a later
                // producer will create and notify its own 0→nonzero edge.
                if (observed == 0 && wakeNotifier !is null)
                    wakeNotifier(wakeNotifierContext);
                break;
            }
            observed = atomicLoad!(MemoryOrder.acq)(control.pendingWakeEvents);
        }
        if (wakePolicy == FiberWakePolicy.broadcast && pool !is null)
            pool.wakeAll();
        if (relay)
        {
            foreach (covering; coveringLanes_)
                covering.noteWake(FiberWakeEvent.remoteReady, false);
        }
    }

    private void enforceSpawnCoverage()
    {
        final switch (coverage)
        {
        case FiberCoverage.noWorkers:
            throw new Exception("antfarm_fibers: lane has no managed workers");
        case FiberCoverage.noResponder:
            throw new Exception("antfarm_fibers: lane has no responder/sweeper");
        case FiberCoverage.pending:
        case FiberCoverage.covered:
            return;
        }
    }

    @property bool drained() const nothrow @nogc { return backend.drained; }
}

/// Apply a measured workload preset before constructing/starting the pool.
/// Fiber throughput uses every logical processor and larger zero-cost tables;
/// payload throughput avoids SMT siblings and uses smaller default tables.
void applyFiberTopologyPreset(FiberTopologyPreset preset, FiberLane[] lanes,
                              ref PoolOptions options)
{
    options.managedWorker = fiberWorkerHooks();
    final switch (preset)
    {
    case FiberTopologyPreset.fiberThroughput:
        options.skipSmtSiblings = false;
        foreach (lane; lanes)
        {
            if (lane is null)
                throw new Exception("antfarm_fibers: null lane in topology preset");
            lane.flushBatch = 256;
            lane.avgCost = avgCostForChunk(32);
        }
        break;
    case FiberTopologyPreset.payloadThroughput:
        options.skipSmtSiblings = true;
        foreach (lane; lanes)
        {
            if (lane is null)
                throw new Exception("antfarm_fibers: null lane in topology preset");
            lane.flushBatch = 32;
            lane.avgCost = avgCostForChunk(8);
        }
        break;
    }
}

/// Persistent remote Farm subscription owned by one responder.
private final class RemoteSweepState
{
    FiberLane lane;
    ConsumerView consumer;
    Token producer;
    bool subscribed;
    bool registered;
}

/// Persistent state owned by one pinned worker from managed start through
/// managed stop.
final class FiberWorkerState
{
    FiberLane lane;
    ConsumerView consumer;
    Token producer;
    bool subscribed;
    bool registered;
    bool responder;
    RemoteSweepState[] remotes;
    // Worker-owned scheduling state: start/pump/stop are serial. No new
    // shared state or memory-order edges are introduced.
    private size_t nextRemote;
    private uint localPumpsSinceRemote;
}

/// Install exactly one lane per LLC. Call before starting the pool and do not
/// uninstall until every worker has stopped.
void installFiberLanes(FiberLane[] lanes, CacheAwarePool pool = null)
{
    installedFiberLanes = lanes.dup;
    if (fiberCoverageControl is null)
        fiberCoverageControl = allocateCacheLine!FiberCoverageControl();
    installRemoteCoverageRing(lanes);
    resetFiberCoverage(pool);
    install!FiberLane(lanes);
}

/// Default sweeper graph: for each shared domain with two or more lanes and
/// no explicit `cover` edges, responders of lane i also sweep lane i+1
/// (wrapping). Private one-lane domains are left uncovered.
void installRemoteCoverageRing(FiberLane[] lanes)
{
    auto seen = new bool[lanes.length];
    foreach (i, lane; lanes)
    {
        if (lane is null || seen[i]) continue;
        FiberLane[] group;
        bool explicitGraph;
        foreach (j, other; lanes)
        {
            if (other is null || other.backend !is lane.backend) continue;
            seen[j] = true;
            group ~= other;
            if (other.coveredLanes.length != 0) explicitGraph = true;
        }
        if (explicitGraph || group.length < 2) continue;
        foreach (g, member; group)
            member.addCoverage(group[(g + 1) % group.length]);
    }
}

void uninstallFiberLanes()
{
    uninstall!FiberLane();
    installedFiberLanes = null;
    installedFiberPool = null;
    freeCacheLine(fiberCoverageControl);
    fiberCoverageControl = null;
}

private void resetFiberCoverage(CacheAwarePool pool)
{
    installedFiberPool = pool;
    if (fiberCoverageControl !is null)
    {
        atomicStore!(MemoryOrder.raw)(fiberCoverageControl.startedWorkers, 0u);
        atomicStore!(MemoryOrder.raw)(fiberCoverageControl.invalidActiveLane, 0);
        atomicStore!(MemoryOrder.raw)(fiberCoverageControl.ready, 0);
    }
    foreach (lane; installedFiberLanes)
    {
        lane.pool = pool;
        atomicStore!(MemoryOrder.raw)(lane.control.startedWorkers, 0u);
        atomicStore!(MemoryOrder.raw)(lane.control.responderCount, 0u);
        atomicStore!(MemoryOrder.rel)(lane.control.coverageState,
            FiberCoverage.pending);
    }
}

private bool laneHasSweeper(FiberLane lane) nothrow @nogc
{
    if (atomicLoad!(MemoryOrder.acq)(lane.control.responderCount) != 0)
        return true;
    foreach (covering; lane.coveringLanes)
        if (atomicLoad!(MemoryOrder.acq)(covering.control.responderCount) != 0)
            return true;
    return false;
}

private void finalizeFiberCoverage() nothrow @nogc
{
    bool invalidActiveLane;
    foreach (lane; installedFiberLanes)
    {
        immutable workers = atomicLoad!(MemoryOrder.acq)(
            lane.control.startedWorkers);
        FiberCoverage state;
        if (laneHasSweeper(lane))
            state = FiberCoverage.covered;
        else if (workers == 0)
            state = FiberCoverage.noWorkers;
        else
            state = FiberCoverage.noResponder;
        atomicStore!(MemoryOrder.rel)(lane.control.coverageState, state);
        if (state == FiberCoverage.noWorkers && lane.backend.active != 0)
            invalidActiveLane = true;
    }
    atomicStore!(MemoryOrder.rel)(fiberCoverageControl.invalidActiveLane,
        invalidActiveLane ? 1 : 0);
    atomicStore!(MemoryOrder.rel)(fiberCoverageControl.ready, 1);
}

/// Managed lifecycle hooks suitable for `PoolOptions.managedWorker`.
ManagedWorkerHooks fiberWorkerHooks()
{
    return ManagedWorkerHooks(&fiberWorkerStart, &fiberWorkerPump,
                              &fiberWorkerStop);
}

private void fiberWorkerStart(WorkerSelf* worker)
{
    auto lane = home!FiberLane();
    if (lane is null)
        throw new Exception("antfarm_fibers: no FiberLane installed for worker LLC");
    auto state = new FiberWorkerState;
    state.lane = *lane;
    state.responder = state.lane.responderSelector is null
        || state.lane.responderSelector(worker);
    if (installedFiberPool is null && state.lane.responderSelector !is null)
        throw new Exception(
            "antfarm_fibers: custom responder selection requires a bound pool");
    if (state.consumer.subscribe(state.lane.farm) < 0)
        throw new Exception("antfarm_fibers: consumer subscription failed");
    state.subscribed = true;
    state.producer = state.lane.farm.registerProducer(Tier.small);
    if (!state.producer.valid)
    {
        state.consumer.unsubscribe();
        state.subscribed = false;
        throw new Exception("antfarm_fibers: producer registration failed");
    }
    state.registered = true;
    worker.context = cast(void*) state;
    if (state.responder)
    {
        try
        {
            foreach (remoteLane; state.lane.coveredLanes)
            {
                auto sweep = new RemoteSweepState;
                sweep.lane = remoteLane;
                if (sweep.consumer.subscribe(remoteLane.farm) < 0)
                    throw new Exception(
                        "antfarm_fibers: remote consumer subscription failed");
                sweep.subscribed = true;
                sweep.producer = remoteLane.farm.registerProducer(Tier.small);
                if (!sweep.producer.valid)
                {
                    sweep.consumer.unsubscribe();
                    sweep.subscribed = false;
                    throw new Exception(
                        "antfarm_fibers: remote producer registration failed");
                }
                sweep.registered = true;
                state.remotes ~= sweep;
            }
        }
        catch (Exception failure)
        {
            fiberWorkerStop(worker);
            worker.context = null;
            throw failure;
        }
    }
    atomicFetchAdd!(MemoryOrder.rel)(state.lane.control.startedWorkers, 1u);
    if (state.responder)
        atomicFetchAdd!(MemoryOrder.rel)(state.lane.control.responderCount, 1u);
    if (installedFiberPool is null)
    {
        // Without the pool object the default selector makes every started
        // worker a responder, so the lane validates immediately.
        atomicStore!(MemoryOrder.rel)(state.lane.control.coverageState,
            FiberCoverage.covered);
    }
    else
    {
        immutable started = atomicFetchAdd!(MemoryOrder.acq_rel)(
            fiberCoverageControl.startedWorkers, 1u) + 1;
        if (started == installedFiberPool.workerCount)
            finalizeFiberCoverage();
    }
}

// One remote visit per 16 busy home pumps; with R subscriptions each is
// visited within 16*R home pumps. Empty-home scans can only shorten the bound.
private enum remoteServiceInterval = 16;

private ManagedPumpResult fiberWorkerPump(WorkerSelf* worker)
{
    auto state = cast(FiberWorkerState) worker.context;
    if (state is null)
        throw new Exception("antfarm_fibers: worker state is missing");

    if (installedFiberPool !is null && fiberCoverageControl !is null
        && atomicLoad!(MemoryOrder.acq)(fiberCoverageControl.ready) == 0)
        return ManagedPumpResult.again;

    if (fiberCoverageControl !is null
        && atomicLoad!(MemoryOrder.acq)(
            fiberCoverageControl.invalidActiveLane) != 0)
        throw new Exception(
            "antfarm_fibers: runnable work exists on a lane with no managed workers");

    final switch (state.lane.coverage)
    {
    case FiberCoverage.pending:
        return ManagedPumpResult.again;
    case FiberCoverage.noResponder:
        throw new Exception("antfarm_fibers: active lane has no responder/sweeper");
    case FiberCoverage.noWorkers:
        throw new Exception("antfarm_fibers: worker entered an unavailable lane");
    case FiberCoverage.covered:
        break;
    }

    immutable timersWoken = state.responder
        ? state.lane.backend.pollTimers() : 0;
    bool consumed = state.consumer.consumeNext();
    immutable triggersDelivered =
        state.lane.backend.pollGenerationTriggers();
    if (state.lane.backend.pendingEvents != 0)
        state.lane.noteWake(FiberWakeEvent.lifecycle);
    if (state.lane.backend.fatal)
        throw state.lane.backend.fatalException;
    size_t flushed;
    immutable ready = state.lane.ready;
    if (state.lane.flushBatch == 0)
        throw new Exception("antfarm_fibers: flushBatch must be greater than zero");
    // Accumulate while useful farm work is flowing. Flush a full batch, or a
    // short tail immediately before this worker would otherwise idle/cadence.
    if (ready >= state.lane.flushBatch || (!consumed && ready != 0))
        flushed = state.lane.flush(
            state.producer, state.lane.flushBatch, state.lane.avgCost);
    if (flushed != 0)
        state.lane.noteWake(FiberWakeEvent.published);

    bool remoteWork;
    if (state.responder && state.remotes.length != 0)
    {
        if (!consumed || ++state.localPumpsSinceRemote >= remoteServiceInterval)
        {
            immutable attempts = consumed ? 1 : state.remotes.length;
            state.localPumpsSinceRemote = 0;
            foreach (_; 0 .. attempts)
            {
                auto remote = state.remotes[state.nextRemote];
                // Rotate on misses AND successes. A busy view must not hide
                // a later subscriber whose old pins block its Farm's writes.
                if (++state.nextRemote == state.remotes.length)
                    state.nextRemote = 0;
                if (remote.consumer.consumeNext())
                {
                    consumed = true;
                    break;
                }
            }
        }
        foreach (remote; state.remotes)
        {
            immutable remoteReady = remote.lane.ready;
            if (remoteReady >= state.lane.flushBatch || remoteReady != 0)
            {
                immutable remoteFlushed = remote.lane.flush(
                    remote.producer, state.lane.flushBatch, state.lane.avgCost);
                if (remoteFlushed != 0)
                {
                    remote.lane.noteWake(FiberWakeEvent.published);
                    remoteWork = true;
                }
            }
            if (remote.lane.ready != 0 || remote.lane.published != 0)
                remoteWork = true;
        }
    }

    // consumeNext() is false at a hole or at Wt. Neither means the farm is
    // empty while published-but-not-entered activations remain, or while a
    // covered remote lane still has transport. Table size (flushBatch) bounds
    // a visit; a claimed table is consumed according to the Farm contract.
    if (timersWoken != 0 || triggersDelivered != 0
        || consumed || flushed != 0 || remoteWork
        || state.lane.ready != 0 || state.lane.published != 0)
        return ManagedPumpResult.again;
    long deadline;
    return state.responder
        && state.lane.backend.nextTimerDeadline(deadline)
        ? ManagedPumpResult.until(deadline) : ManagedPumpResult.idle;
}

private void fiberWorkerStop(WorkerSelf* worker)
{
    auto state = cast(FiberWorkerState) worker.context;
    if (state is null) return;
    foreach (remote; state.remotes)
    {
        if (remote.subscribed)
        {
            remote.consumer.unsubscribe();
            remote.subscribed = false;
        }
        if (remote.registered)
        {
            remote.lane.farm.unregisterProducer(remote.producer);
            remote.registered = false;
        }
    }
    if (state.subscribed)
    {
        state.consumer.unsubscribe();
        state.subscribed = false;
    }
    if (state.registered)
    {
        state.lane.farm.unregisterProducer(state.producer);
        state.registered = false;
    }
}

version (unittest)
{
    private void testWakeNotifier(void* context) nothrow @nogc
    {
        auto count = cast(shared(uint)*) context;
        atomicFetchAdd!(MemoryOrder.rel)(*count, 1u);
    }

    unittest
    {
        import core.thread : Thread;

        enum rounds = 10_000;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 128,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto lane = new FiberLane(farm);
        lane.wakePolicy = FiberWakePolicy.directorOwned;
        shared uint requested;
        shared uint acknowledged;
        shared uint notifications;
        lane.setWakeNotifier(&testWakeNotifier, cast(void*) &notifications);
        auto producer = new Thread({
            foreach (round; 1 .. rounds + 1)
            {
                while (atomicLoad!(MemoryOrder.acq)(requested) < round)
                    Thread.yield();
                lane.noteWake(FiberWakeEvent.spawned);
                while (atomicLoad!(MemoryOrder.acq)(acknowledged) < round)
                    Thread.yield();
            }
        });
        producer.start();
        foreach (round; 1 .. rounds + 1)
        {
            atomicStore!(MemoryOrder.rel)(requested, round);
            while (atomicLoad!(MemoryOrder.acq)(notifications) < round)
                Thread.yield();
            auto events = lane.takeWakeEvents();
            assert(events == FiberWakeEvent.spawned);
            atomicStore!(MemoryOrder.rel)(acknowledged, round);
        }
        producer.join();
        assert(atomicLoad!(MemoryOrder.acq)(notifications) == rounds);
    }
}

version (unittest)
{
    // Drive the real managed pump on one thread: no timing, topology, pool or
    // Fiber stack switch is involved in these transport-liveness regressions.
    private final class RemoteServiceFixture
    {
        FiberWorkerState state;
        WorkerSelf worker;
        AntFarm*[] farms;
        PayloadHeader header;
        ulong[1] word = [7];
        PayloadBody[1024] bodies;

        this(size_t remoteCount)
        {
            // Module tests run before smoke main installs managed lanes.
            assert(installedFiberPool is null && fiberCoverageControl is null);
            state = new FiberWorkerState;
            state.responder = true;
            auto domain = new FiberDomain;
            foreach (i; 0 .. remoteCount + 1)
            {
                auto farm = AntFarm.create(2, 8, 2, 0, 0, 2, 4096,
                    DEFAULT_SMALL_TABLE_THRESHOLD, false);
                farms ~= farm;
                auto lane = new FiberLane(domain, farm);
                atomicStore!(MemoryOrder.rel)(lane.control.coverageState,
                    FiberCoverage.covered);
                if (i == 0)
                {
                    state.lane = lane;
                    subscribeOrThrow(state.consumer, farm);
                    state.subscribed = true;
                    state.producer = farm.registerProducer(Tier.small);
                    state.registered = true;
                    assert(state.producer.valid);
                }
                else
                {
                    auto remote = new RemoteSweepState;
                    remote.lane = lane;
                    state.lane.cover(lane);
                    subscribeOrThrow(remote.consumer, farm);
                    remote.subscribed = true;
                    remote.producer = farm.registerProducer(Tier.small);
                    remote.registered = true;
                    assert(remote.producer.valid);
                    state.remotes ~= remote;
                }
            }
            worker.context = cast(void*) state;
            header.maxCs = header.done = 1;
            header.call = &noopRemoteService;
            foreach (ref body; bodies) body = word[];
        }

        size_t write(size_t lane, size_t count = 1)
        {
            if (lane == 0)
                return farms[0].write(header, bodies[0 .. count], 1,
                    state.producer);
            return farms[lane].write(header, bodies[0 .. count], 1,
                state.remotes[lane - 1].producer);
        }

        void pump(bool busyLocal)
        {
            if (busyLocal) assert(write(0) == 1);
            auto before = state.consumer.nextSeq;
            fiberWorkerPump(&worker);
            if (busyLocal)
                assert(state.consumer.nextSeq > before,
                    "fixture must keep the local consume successful");
        }

        void close()
        {
            fiberWorkerStop(&worker);
            fiberWorkerStop(&worker); // Stop/rollback remains idempotent.
            foreach (farm; farms)
            {
                assert(atomicLoad(farm.Cf) == 0);
                farm.destroy();
            }
        }
    }

    private long noopRemoteService(PayloadHeader*, PayloadBody, ulong)
        nothrow @nogc @system { return 0; }

    unittest
    {
        import std.stdio : writefln;
        auto fixture = new RemoteServiceFixture(2);
        scope (exit) fixture.close();
        ConsumerView native;
        subscribeOrThrow(native, fixture.farms[2]);
        scope (exit) native.unsubscribe();
        size_t tables;
        while (tables < 10_000)
        {
            auto admitted = fixture.write(2, 1024);
            while (native.consumeNext()) {}
            if (admitted == 0) break;
            ++tables;
        }
        assert(tables > 0 && tables < 10_000);
        assert(fixture.state.remotes[1].consumer.oldestEi == 0);
        writefln("remote service: pinned at epoch %s after %s tables",
            atomicLoad(fixture.farms[2].Wt) / fixture.farms[2].segCap, tables);
        // Independent contract value, intentionally not derived from the
        // implementation: weakening the service interval must fail this test.
        enum bound = 16 * 2;
        foreach (_; 0 .. bound) fixture.pump(true);
        assert(fixture.state.remotes[1].consumer.nextSeq > 0,
            "busy local lane starved its later remote past 16*R pumps");
        foreach (_; 0 .. bound * (tables + 2)) fixture.pump(true);
        auto admitted = fixture.write(2, 1024);
        assert(admitted != 0, "bounded remote service must restore admission");
        assert(fixture.state.remotes[1].consumer.oldestEi > 0);
        writefln("remote service: admission resumed (%s entries)", admitted);
        while (native.consumeNext()) {}
    }

    unittest
    {
        auto fixture = new RemoteServiceFixture(2);
        scope (exit) fixture.close();
        // Earlier remote is successful on every old-driver pump. The later
        // remote must still be visited when local is empty.
        foreach (_; 0 .. 64) assert(fixture.write(1) == 1);
        assert(fixture.write(2) == 1);
        foreach (_; 0 .. 2) fixture.pump(false);
        assert(fixture.state.remotes[1].consumer.nextSeq > 0,
            "remote round-robin must advance past a successful earlier view");
    }

    unittest
    {
        auto fixture = new RemoteServiceFixture(2);
        scope (exit) fixture.close();
        // An empty earlier remote must not hide a later view, even when
        // native consumers have already completed that view's tables.
        ConsumerView native;
        subscribeOrThrow(native, fixture.farms[2]);
        scope (exit) native.unsubscribe();
        assert(fixture.write(2) == 1);
        while (native.consumeNext()) {}
        fixture.pump(false);
        assert(fixture.state.remotes[1].consumer.nextSeq > 0);
        // Stop with unconsumed work preserves Sub0 so a new view drains it.
        assert(fixture.write(1) == 1);
        fiberWorkerStop(&fixture.worker);
        ConsumerView replacement;
        subscribeOrThrow(replacement, fixture.farms[1]);
        assert(replacement.consumeNext());
        replacement.unsubscribe();
    }
}
