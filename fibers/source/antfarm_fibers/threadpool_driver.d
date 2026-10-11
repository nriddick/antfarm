/++ Tight binding between Ant Farm fiber lanes and threadpool_llc workers. +/
module antfarm_fibers.threadpool_driver;

import antfarm;
import antfarm_fibers.cacheline;
import antfarm_fibers.scheduler;
import antfarm_fibers.service_plan;
import core.atomic;
import threadpool;

/// Who converts an external fiber readiness transition into a pool wake.
/// `broadcast` preserves the eager compatibility behavior. `directorOwned`
/// records events but leaves worker selection and signalling to the thread
/// which owns threadpool's Director. `servingGroups` (opt-in) wakes only the
/// physical groups that serve the lane: its own group and its rescue/assist
/// server groups; a newly armed earliest timer also wakes the groups hosting
/// the domain's responders.
enum FiberWakePolicy : ubyte
{
    broadcast,
    directorOwned,
    servingGroups,
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

/// Runtime admission and liveness of a lane, separate from its planned
/// `LaneService`. `pending` and `open` admit work; the others refuse it.
enum FiberLaneState : int
{
    pending = 0,  /// installed; served lanes admit work before start
    open = 1,     /// every planned server started and the domain has a responder
    closed = -1,  /// planned without service (unstaffed or disabled)
    stopped = -2, /// every server that started has stopped
    failed = -3,  /// served, but no responder in its domain or no live server
}

/// Thrown by `FiberLane.spawn*` when the lane does not admit work.
class FiberLaneUnavailable : Exception
{
    FiberLaneState state;  /// the lane state that refused admission
    LaneService service;   /// the lane's planned service
    ushort group;          /// the lane's physical group, ushort.max if not installed

    this(FiberLaneState state, LaneService service, ushort group,
         string file = __FILE__, size_t line = __LINE__)
    {
        import std.conv : to;

        this.state = state;
        this.service = service;
        this.group = group;
        super("antfarm_fibers: lane unavailable (state " ~ state.to!string
            ~ ", service " ~ service.to!string ~ ", group "
            ~ (group == ushort.max ? "none" : group.to!string) ~ ")",
            file, line);
    }
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
    shared int state;          // FiberLaneState
    shared uint liveServers;   // started-and-not-stopped native and edge servers
    LaneService service;       // setup-time only, before the pool starts
    ubyte[47] statePadding;
}
static assert(FiberLaneControl.sizeof == 2 * cacheLineSize);

align(64) private struct FiberStartControl
{
    shared uint startedWorkers;
    shared int invalidActiveLane;
    shared int ready;
    ubyte[52] padding;
}
static assert(FiberStartControl.sizeof == cacheLineSize);

private __gshared FiberLane[] installedFiberLanes; // index == physical group
private __gshared CacheAwarePool installedFiberPool;
private __gshared FiberServicePlan installedFiberPlan;
private __gshared FiberStartControl* fiberStartControl;
// Thread-local: the domain this managed worker polls timers for, if it is a
// responder; null on every other thread.
private FiberDomain responderDomain;

/// One scheduler/farm pairing, bound to a physical group by a
/// `FiberServicePlan`. The caller owns the farm lifetime and must keep it
/// alive until the pool has stopped.
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
    // Plan-populated, fixed from install until uninstall: lanes this lane's
    // responders serve by an edge, and lanes whose responders serve this one.
    private FiberLane[] servedLanes_;
    private FiberLane[] servingLanes_;
    // Plan-populated with the edge lists, for `servingGroups`: this lane's
    // group plus its edge server groups, and the groups whose native lanes
    // share this lane's domain (a superset of its responder hosts).
    private ushort[] wakeGroups_;
    private ushort[] timerWakeGroups_;
    private ushort group_ = ushort.max;

    @property private ref shared(uint) pendingWakeEvents() const return nothrow @nogc
    { return (cast(FiberLaneControl*) control).pendingWakeEvents; }

    /// Runtime admission state; see `FiberLaneState`.
    @property FiberLaneState state() const nothrow @nogc
    {
        return cast(FiberLaneState) atomicLoad!(MemoryOrder.acq)(control.state);
    }

    /// Planned service, set at installation; `absent` until installed.
    @property LaneService service() const nothrow @nogc
    {
        return control.service;
    }

    /// Physical group this lane is installed for; ushort.max until installed.
    @property ushort group() const nothrow @nogc { return group_; }

    /// Servers (native workers and edge responders) that started and have
    /// not stopped.
    @property uint liveServers() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(control.liveServers);
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

    this(AntFarm* farm, size_t accumulatorCapacity = 65_536)
    {
        super(farm);
        control = allocateCacheLine!FiberLaneControl();
        control.service = LaneService.absent;
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
        control.service = LaneService.absent;
        backend = domain;
        backend.attachReadyLane(this);
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

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
        enforceAdmission();
        auto task = backend.spawnOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return task;
    }

    FiberTask spawn(void function() body, size_t stackSize = 0)
    {
        enforceAdmission();
        auto task = backend.spawnOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return task;
    }

    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        enforceAdmission();
        auto handle = backend.spawnHandleOn(this, body, stackSize);
        noteWake(FiberWakeEvent.spawned);
        return handle;
    }

    TaskHandle spawnHandle(void function() body, size_t stackSize = 0)
    {
        enforceAdmission();
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

    // A responder of this domain needs no wake for a timer its own fiber
    // armed: the fiber ran inside that worker's pump, which then returns
    // `again`, and the pump before it parks reads `nextTimerDeadline` into
    // `until`. Any other worker (a custom `responderSelector` that excludes
    // it) would leave a parked responder unaware of the earlier deadline.
    protected override void noteTimerArmed() nothrow @nogc
    {
        if (responderDomain is backend) return;
        noteWake(FiberWakeEvent.signalled, false);
        if (wakePolicy == FiberWakePolicy.servingGroups && pool !is null)
            foreach (g; timerWakeGroups_)
                pool.wakeLlc(g);
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

    // `servingGroups` completeness: every worker needed for progress after
    // an event is woken, by this wake set or by an existing mechanism.
    // - Ready or published work on this lane is consumed by native workers
    //   of `group_` and by the responders of its rescue/assist server groups
    //   (they flush `ready` and sweep the Farm): both are in `wakeGroups_`.
    // - Timers: a responder of the domain arms its own deadline before it
    //   parks; any other worker arming a new earliest timer calls
    //   `noteTimerArmed`, which wakes `timerWakeGroups_`, every native group
    //   of this domain. An expired timer is moved to its task's lane, whose
    //   `noteExternalReady` wakes that lane's set.
    // - Generation triggers: `enqueueGenerationTrigger` calls
    //   `noteExternalReady` on every lane of the domain, so every native group
    //   of the domain is woken, and every worker polls triggers.
    // - Cancellation and shutdown: the Director applies them; a waiting task
    //   becomes ready through `publishParkedWaiter`, i.e. this lane's
    //   `noteExternalReady`. A ready or running task is already in service.
    // - Remote-ready relay: the relay targets are `servingLanes_`, the lanes
    //   of this lane's edge server groups (`bindPlanLanes`), and those groups
    //   are in `wakeGroups_`. A relayed call therefore only records
    //   `remoteReady` and adds no wake under `servingGroups`. A
    //   `directorOwned` origin leaves its wakes to the Director.
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
        if (pool !is null)
        {
            if (wakePolicy == FiberWakePolicy.broadcast)
                pool.wakeAll();
            else if (wakePolicy == FiberWakePolicy.servingGroups && relay)
                foreach (g; wakeGroups_)
                    pool.wakeLlc(g);
        }
        if (relay)
        {
            foreach (serving; servingLanes_)
                serving.noteWake(FiberWakeEvent.remoteReady, false);
        }
    }

    // Admission is a single acquire load; nothing is locked against stop.
    // `pending` is only set by `installFiberPlan` (setup thread, before
    // start) and `open` only once a server has started. A lane leaves
    // `pending`/`open` for `stopped` only in the stop hook of its last live
    // server, which runs after that worker's final pump. A spawn which loads
    // `pending`/`open` therefore strands its task only if its push lands
    // after the final pump of every live server: it raced the last server's
    // stop, i.e. a `pool.shutdown` the caller started before closing domain
    // admission. That is the documented race; afterwards `state == stopped`
    // and `domain.active` still counts the task. The required order is
    // Director `beginShutdown` -> drain -> `pool.shutdown`.
    private void enforceAdmission()
    {
        final switch (state)
        {
        case FiberLaneState.pending:
        case FiberLaneState.open:
            return;
        case FiberLaneState.closed:
        case FiberLaneState.stopped:
        case FiberLaneState.failed:
            throw new FiberLaneUnavailable(state, service, group_);
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
    bool live; // counted in liveServers of the home lane and every remote
    bool idle; // disabled group: never registered, never serves
    RemoteSweepState[] remotes;
    // Worker-owned scheduling state: start/pump/stop are serial. No new
    // shared state or memory-order edges are introduced.
    private size_t nextRemote;
    private uint localPumpsSinceRemote;
}

/// Validate `plan` and install it: the plan's lanes become the
/// physical-group-indexed `FiberLane` bin table, each lane's served and
/// serving lanes come from `plan.edges`, and every lane gets its planned
/// service and initial state (`pending` when served, `closed` otherwise).
/// Call before starting the pool and do not uninstall until every worker has
/// stopped. Installing replaces any installed plan.
///
/// With a pool, the plan's resolution must select the same workers as
/// `pool.resolve()`. `pool is null` is the plan-only form for tests that drive
/// lanes without a pool; it is fully validated, only the pool comparison is
/// skipped, and each lane opens when its first server starts.
void installFiberPlan(FiberServicePlan plan, CacheAwarePool pool)
{
    if (plan is null)
        throw new Exception("antfarm_fibers: installFiberPlan needs a plan");
    plan.validate();
    if (pool !is null && !plan.resolution.sameSelection(pool.resolve()))
        throw new FiberPlanException([
            "the plan's resolution (" ~ plan.resolution.summary()
            ~ ") does not select the pool's workers (" ~ pool.resolve().summary()
            ~ "); build the plan from pool.resolve()"]);
    if (installedFiberPlan !is null)
        uninstallFiberPlan();
    installedFiberLanes = bindPlanLanes(plan);
    installedFiberPlan = plan;
    if (fiberStartControl is null)
        fiberStartControl = allocateCacheLine!FiberStartControl();
    resetLaneRuntime(pool);
    install!FiberLane(installedFiberLanes.dup, plan.resolution);
}

/// Remove the installed plan after every worker has stopped. Lanes keep their
/// group, service and final state for inspection.
void uninstallFiberPlan()
{
    uninstall!FiberLane();
    foreach (lane; installedFiberLanes)
    {
        if (lane is null) continue;
        lane.pool = null;
        lane.servedLanes_ = null;
        lane.servingLanes_ = null;
        lane.wakeGroups_ = null;
        lane.timerWakeGroups_ = null;
    }
    installedFiberLanes = null;
    installedFiberPool = null;
    installedFiberPlan = null;
    freeCacheLine(fiberStartControl);
    fiberStartControl = null;
}

/// Bind every lane of a validated plan (group, service and edge lists) and
/// return them indexed by physical group.
private FiberLane[] bindPlanLanes(FiberServicePlan plan)
{
    import std.algorithm.searching : canFind;

    immutable groups = plan.resolution.physicalGroupCount;
    auto lanes = new FiberLane[groups];
    foreach (ushort g; 0 .. groups)
    {
        auto lane = plan.laneOf(g);
        lanes[g] = lane;
        if (lane is null) continue;
        lane.group_ = g;
        lane.control.service = plan.service(g);
        lane.servedLanes_ = null;
        lane.servingLanes_ = null;
        lane.wakeGroups_ = [g];
        lane.timerWakeGroups_ = null;
    }
    foreach (ref e; plan.edges)
    {
        lanes[e.server].servedLanes_ ~= lanes[e.served];
        lanes[e.served].servingLanes_ ~= lanes[e.server];
        if (!lanes[e.served].wakeGroups_.canFind(e.server))
            lanes[e.served].wakeGroups_ ~= e.server;
    }
    foreach (lane; lanes)
    {
        if (lane is null) continue;
        foreach (h, host; lanes)
            if (host !is null && host.backend is lane.backend
                && host.service == LaneService.native)
                lane.timerWakeGroups_ ~= cast(ushort) h;
    }
    return lanes;
}

/// Setup-time: bind `pool` and reset every installed lane's runtime state.
private void resetLaneRuntime(CacheAwarePool pool)
{
    installedFiberPool = pool;
    atomicStore!(MemoryOrder.raw)(fiberStartControl.startedWorkers, 0u);
    atomicStore!(MemoryOrder.raw)(fiberStartControl.invalidActiveLane, 0);
    atomicStore!(MemoryOrder.raw)(fiberStartControl.ready, 0);
    foreach (lane; installedFiberLanes)
    {
        if (lane is null) continue;
        lane.pool = pool;
        atomicStore!(MemoryOrder.raw)(lane.control.startedWorkers, 0u);
        atomicStore!(MemoryOrder.raw)(lane.control.responderCount, 0u);
        atomicStore!(MemoryOrder.raw)(lane.control.liveServers, 0u);
        atomicStore!(MemoryOrder.rel)(lane.control.state,
            isServed(lane.control.service)
                ? FiberLaneState.pending : FiberLaneState.closed);
    }
}

/// Run once by the last managed start. A served lane opens when its domain
/// has at least one responder (timers are domain-wide) and it has a live
/// server; otherwise it fails. Queued work on a lane that did not open is a
/// lane-local check: another lane's work in the same domain is not this
/// lane's problem.
private void finalizeFiberLanes() nothrow @nogc
{
    bool invalidActiveLane;
    foreach (lane; installedFiberLanes)
    {
        if (lane is null) continue;
        if (isServed(lane.control.service))
        {
            bool domainResponder;
            foreach (other; installedFiberLanes)
                if (other !is null && other.backend is lane.backend
                    && atomicLoad!(MemoryOrder.acq)(
                        other.control.responderCount) != 0)
                    domainResponder = true;
            immutable live = atomicLoad!(MemoryOrder.acq)(
                lane.control.liveServers) != 0;
            immutable next = domainResponder && live
                ? FiberLaneState.open : FiberLaneState.failed;
            // A server that already stopped (shutdown racing start) moved
            // the lane to `stopped`; keep that.
            int expected = FiberLaneState.pending;
            cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &lane.control.state, &expected, cast(int) next);
        }
        if (lane.state != FiberLaneState.open
            && (lane.ready != 0 || lane.published != 0))
            invalidActiveLane = true;
    }
    atomicStore!(MemoryOrder.rel)(fiberStartControl.invalidActiveLane,
        invalidActiveLane ? 1 : 0);
    atomicStore!(MemoryOrder.rel)(fiberStartControl.ready, 1);
}

/// Count a started server on `lane`. Without a pool there is no
/// finalization, so the first server opens a pending lane directly.
private void noteServerStarted(FiberLane lane) nothrow @nogc
{
    atomicFetchAdd!(MemoryOrder.acq_rel)(lane.control.liveServers, 1u);
    if (installedFiberPool is null)
    {
        int expected = FiberLaneState.pending;
        cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
            &lane.control.state, &expected, cast(int) FiberLaneState.open);
    }
}

/// The last live server of a pending or open lane moves it to `stopped`, so
/// later spawns throw instead of stranding work. `closed` and `failed` keep
/// their diagnosis.
private void noteServerStopped(FiberLane lane) nothrow @nogc
{
    if (atomicFetchSub!(MemoryOrder.acq_rel)(lane.control.liveServers, 1u) != 1)
        return;
    int observed = atomicLoad!(MemoryOrder.acq)(lane.control.state);
    while (observed == FiberLaneState.pending || observed == FiberLaneState.open)
    {
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&lane.control.state,
                &observed, cast(int) FiberLaneState.stopped))
            break;
    }
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
    if (state.lane.service == LaneService.disabled)
    {
        // A disabled group's workers idle: no Farm registration, no service,
        // not responders. They still count towards finalization.
        state.idle = true;
        worker.context = cast(void*) state;
        noteWorkerStarted();
        return;
    }
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
            foreach (remoteLane; state.lane.servedLanes_)
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
    {
        atomicFetchAdd!(MemoryOrder.rel)(state.lane.control.responderCount, 1u);
        responderDomain = state.lane.backend;
    }
    noteServerStarted(state.lane);
    foreach (remote; state.remotes)
        noteServerStarted(remote.lane);
    state.live = true;
    noteWorkerStarted();
}

/// The last managed start of a bound pool finalizes every installed lane. Each
/// starter's lane counts precede its acq_rel increment, so the finalizer sees
/// them all.
private void noteWorkerStarted() nothrow @nogc
{
    if (installedFiberPool is null) return;
    immutable started = atomicFetchAdd!(MemoryOrder.acq_rel)(
        fiberStartControl.startedWorkers, 1u) + 1;
    if (started == installedFiberPool.workerCount)
        finalizeFiberLanes();
}

// One remote visit per 16 busy home pumps; with R subscriptions each is
// visited within 16*R home pumps. Empty-home scans can only shorten the bound.
private enum remoteServiceInterval = 16;

private ManagedPumpResult fiberWorkerPump(WorkerSelf* worker)
{
    auto state = cast(FiberWorkerState) worker.context;
    if (state is null)
        throw new Exception("antfarm_fibers: worker state is missing");

    if (state.idle)
        return ManagedPumpResult.idle;

    if (installedFiberPool !is null && fiberStartControl !is null
        && atomicLoad!(MemoryOrder.acq)(fiberStartControl.ready) == 0)
        return ManagedPumpResult.again;

    if (fiberStartControl !is null
        && atomicLoad!(MemoryOrder.acq)(
            fiberStartControl.invalidActiveLane) != 0)
        throw new Exception(
            "antfarm_fibers: runnable work is queued on a lane that did not open");

    final switch (state.lane.state)
    {
    case FiberLaneState.pending:
        return ManagedPumpResult.again;
    case FiberLaneState.failed:
        throw new Exception(
            "antfarm_fibers: lane failed: no responder in its domain or no live server");
    case FiberLaneState.closed:
    case FiberLaneState.stopped:
        throw new Exception("antfarm_fibers: worker entered an unavailable lane");
    case FiberLaneState.open:
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
    // served remote lane still has transport. Table size (flushBatch) bounds
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
    responderDomain = null;
    if (state.live)
    {
        // First, so admission closes as early as possible after the last pump.
        state.live = false;
        noteServerStopped(state.lane);
        foreach (remote; state.remotes)
            noteServerStopped(remote.lane);
    }
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
            assert(installedFiberPool is null && fiberStartControl is null);
            state = new FiberWorkerState;
            state.responder = true;
            auto domain = new FiberDomain;
            FiberLane[] lanes;
            foreach (i; 0 .. remoteCount + 1)
            {
                farms ~= AntFarm.create(2, 8, 2, 0, 0, 2, 4096,
                    DEFAULT_SMALL_TABLE_THRESHOLD, false);
                lanes ~= new FiberLane(domain, farms[i]);
            }
            // Group 0 holds the one worker and rescues every other group.
            auto plan = new FiberServicePlan(
                fixtureResolution(cast(ushort)(remoteCount + 1)));
            foreach (g, lane; lanes)
                plan.bind(cast(ushort) g, lane);
            foreach (g; 1 .. remoteCount + 1)
                plan.rescue(cast(ushort) g, 0);
            plan.validate();
            bindPlanLanes(plan);
            assert(lanes[0].servedLanes_ == lanes[1 .. $]);
            // servingGroups: a rescued group wakes itself and its server;
            // only group 0 is native, so it hosts every responder.
            immutable ushort[] zero = [0];
            assert(lanes[0].wakeGroups_ == zero);
            foreach (g, lane; lanes)
            {
                immutable ushort[] served = [cast(ushort) g, 0];
                assert(g == 0 || lane.wakeGroups_ == served);
                assert(lane.timerWakeGroups_ == zero);
            }
            foreach (i, lane; lanes)
            {
                auto farm = farms[i];
                atomicStore!(MemoryOrder.rel)(lane.control.state,
                    FiberLaneState.open);
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

    // Group 0 has one accepted CPU; every other group's one CPU is excluded.
    private PoolResolution fixtureResolution(ushort groups)
    {
        import threadpool.os_scope : OsScope;
        import threadpool.topology : GroupingSource, LogicalProcessor,
            TopologySnapshot, assembleSnapshot;

        LogicalProcessor[] processors;
        foreach (ushort g; 0 .. groups)
        {
            LogicalProcessor p;
            p.lpIndex = p.coreIndex = p.llcIndex = g;
            p.affinityExcluded = g != 0;
            processors ~= p;
        }
        auto snapshot = new TopologySnapshot;
        *snapshot = assembleSnapshot("synthetic", processors, null, null,
            groups, 64, new GroupingSource[groups]);
        OsScope unrestricted;
        return resolvePool(*snapshot, unrestricted, PoolOptions.init);
    }

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
