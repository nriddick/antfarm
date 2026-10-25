/++
 + FarmHost: one Farm, the calling thread's producer token and N pinned
 + `CacheAwarePool` workers that consume it, placed on one physical group
 + (LLC) by default.
 +
 + Placement is decided up front: `FarmHost.resolve` returns the exact worker
 + selection the constructor will start, as a `PoolResolution` in which every
 + CPU that passed the filters but was left out is `Reject.notSelected`.
 + Every compromise is a note in `FarmHost.toString` rather than a silent
 + choice. Spanning groups is an explicit opt-in (`HostGroups.span`),
 + labelled `crossGroup` in `FarmHost.summary`.
 +
 + This module is not re-exported by `import threadpool;`; it depends on the
 + `antfarm` package. Import it as `import threadpool.host;`.
 +/
module threadpool.host;

import core.atomic : MemoryOrder, atomicLoad, atomicOp, atomicStore, cas;
import core.thread : Thread;
import core.time : Duration, MonoTime, msecs;
import std.typecons : Rebindable;

import antfarm.actors : ActorAllocator, ActorHandle, ActorReclaimResult, ActorRetireMode,
    ActorRetireResult, ActorRuntime, ActorWave, PrivateRootOptions, PrivateRootPark,
    PrivateRootReclamation, PrivateStateStorage;
import antfarm.parallel;
import antfarm : AntFarm, ConsumerView, FarmCreateError, FarmCreateFailure, Tier, Token,
    DEFAULT_SMALL_TABLE_THRESHOLD, DEFAULT_HUGE_PAGES, MAX_CONSUMERS_LIMIT, fatal;
import threadpool.bins : home;
import threadpool.groups : GroupAttachment, GroupHost, GroupHostOptions, GroupSlot;
import threadpool.hybrid : ManagedWorkerHooks, WorkerSelf, gWorkers;
import threadpool.worker : requestStop;
import threadpool.pool : CacheAwarePool, Director, PoolOptions;
import threadpool.resolution : PoolResolution, Reject, reasonsText, resolvePool;
import threadpool.topology : ProcessorId, TopologySnapshot, formatCpuList, liveTopology;

/// `FarmHostOptions.workers` sentinel: every CPU the host can staff in the
/// selected group(s), after the caller reservation.
enum uint autoWorkers = uint.max;

/// What an idle pool worker does after its final empty check.
/// Non-exhaustive: do not `final switch` on it.
enum WorkerIdle : ubyte
{
    /// Park. Host publication wrappers and `wake()` wake workers at once;
    /// raw-token publications are picked up within `idleTimeout`.
    park,
    /// Busy-poll with `pause` (Director `spin`). No wake cost on submit;
    /// every worker burns its CPU for the host's lifetime.
    spin,
}

/// Where the calling (control) thread may run while the host has workers.
/// Non-exhaustive: do not `final switch` on it.
enum CallerPlacement : ubyte
{
    /// Restrict the caller's affinity to the home group's online, in-scope
    /// CPUs that no worker owns (SMT siblings of workers included). When
    /// every such CPU is a worker, the whole home group, noted as "caller
    /// shares worker CPUs" in `FarmHost.summary`. Restored by close.
    confine,
    /// Pin the caller to one CPU: the reserved one, else a free home-group
    /// CPU, else the last home worker CPU (shared). Restored by close.
    pin,
    /// Leave the caller's affinity alone. `FarmHost.toString` notes that
    /// the caller is an external participant that may run in another group.
    unchanged,
}

/// Which physical groups (llcIndex) supply workers. `.init` is `automatic`.
struct HostGroups
{
    /// The accepted group with the most accepted CPUs; ties go to the lowest index.
    static HostGroups automatic() pure nothrow @safe
    {
        return HostGroups.init;
    }

    /// Exactly this group. `FarmHostResolveException` if it has no accepted CPU.
    static HostGroups only(ushort llc) pure nothrow @safe
    {
        HostGroups g;
        g.kind = Kind.only;
        g.llcs = [llc];
        return g;
    }

    /// One Farm consumed from several groups: deliberately cross-group.
    /// `llcs[0]` is the home group (caller placement, the Farm's home). Every
    /// worker from another group is a foreign consumer in `FarmHost.summary`.
    static HostGroups span(const(ushort)[] llcs) pure @safe
    {
        HostGroups g;
        g.kind = Kind.span;
        g.llcs = llcs.idup;
        return g;
    }

    /// Every group with an accepted CPU, the one with the most first (home).
    /// With `farmPerGroup` (`GroupHostOptions`, `FarmHostOptions`) each group
    /// gets its own Farm; otherwise every group consumes the one Farm.
    static HostGroups all() pure nothrow @safe
    {
        HostGroups g;
        g.kind = Kind.all;
        return g;
    }

    package(threadpool) enum Kind : ubyte { automatic, only, span, all }
    package(threadpool) Kind kind;
    package(threadpool) const(ushort)[] llcs;
}

/// Farm geometry. Participant counts are computed by the host; the fields
/// here only add to them.
struct FarmSizing
{
    uint  ringMiB  = 8;                              /// as AntFarm.create
    uint  k        = 8;                              /// as AntFarm.create
    ulong quotaSmall = 4096;                         /// as AntFarm.create
    uint  extraSmallProducers = 0;                   /// tokens from registerProducer(Tier.small)
    uint  bulkProducers = 0;                         /// maxBulk; 0 disables the bulk tier
    ulong quotaBulk = 0;                             /// as AntFarm.create
    /// Views subscribed outside the host. They must unsubscribe before
    /// `close()`/`abandon()`; the host cannot see them.
    uint  extraConsumers = 0;
    uint  smallThreshold = DEFAULT_SMALL_TABLE_THRESHOLD; /// as AntFarm.create
    bool  hugePages = DEFAULT_HUGE_PAGES;            /// environment-overridable as today
}

/// `PoolOptions.init` with `skipSmtSiblings = true`: one worker per core,
/// siblings left to the confined caller and the OS.
enum PoolOptions defaultHostPoolOptions = () {
    PoolOptions o;
    o.skipSmtSiblings = true;
    return o;
}();

/// Everything `FarmHost.resolve` and the constructor read. `.init` is a
/// sensible default.
struct FarmHostOptions
{
    /// Pool workers. 0 = no pool: the calling thread is the only consumer.
    uint workers = autoWorkers;
    /// Which group(s) supply workers. Ignored when `workers == 0`.
    HostGroups groups;
    /// One Farm per group that supplies workers, consumed by that group's
    /// workers only, instead of one Farm every group consumes. Runtimes and
    /// parks are then bound to one group's Farm (`createRuntime` and
    /// `createPark` take the group; the default is the caller's) and the
    /// host emits each through that Farm's control token (`tokenOf`).
    /// `farm`, `token` and the caller's view are the caller group's.
    /// Ignored when `workers == 0` or one group supplies every worker.
    bool farmPerGroup = false;
    /// Effective only with workers; `unchanged` when `workers == 0`.
    CallerPlacement caller = CallerPlacement.confine;
    /// The caller also owns a ConsumerView and consumes once per `step()`.
    /// Forced true when `workers == 0`.
    bool callerConsumes = false;
    /// Idle behavior of pool workers.
    WorkerIdle idle = WorkerIdle.park;
    /// Park bound for `WorkerIdle.park` (Director `sleep`). `Duration.max`
    /// parks until woken (Director `wait`); then raw-token publications need `wake()`.
    Duration idleTimeout = 1.msecs;
    /// Opt-in deadline applied afresh to every `waitUntil` call.
    /// `Duration.max` (default) = no deadline.
    Duration waitTimeout = Duration.max;
    /// CPU filters and QoS: processors, skipSmtSiblings, enable{P,E}Cores,
    /// spin iterations. The hook fields must be empty; the host owns them.
    PoolOptions pool = defaultHostPoolOptions;
    /// Farm geometry and extra participants.
    FarmSizing farm;
    /// Maximum emission rounds per step, before waking workers and consuming
    /// once on the caller (or yielding). Each round visits every emitter;
    /// a round with no publications ends the burst early. Must be positive.
    /// 1 restores single-round stepping; larger bursts amortize park/wake cost.
    uint maxEmitRounds = 16;
    /// Further `GroupAttachment`s for the host's `GroupHost`, stepped after
    /// the host's own consuming body (`ant_fibers.groups.FiberGroups`, the
    /// fork-join dispatcher's attachment). A GC-enabled hook among them puts
    /// the workers on the pool's managed lane. Ignored when `workers == 0`.
    GroupAttachment[] attachments;
}

/// Internal: the full host placement `planHost` computes. The public views
/// are `FarmHost.resolve`, `resolution`, `callerCpus`, `summary` and
/// `toString`.
package(threadpool) final class HostPlan
{
    const FarmHostOptions options;     /// as given
    const PoolResolution  candidates;  /// user filters over every group
    /// `candidates` narrowed to the workers, in start (snapshot) order;
    /// every other candidate is `Reject.notSelected`. Never null.
    PoolResolution        resolution;
    ushort                farmGroup;   /// 0 and meaningless when workers == 0
    const(ushort)[]       groups;      /// groups supplying workers, home first
    CallerPlacement       caller;      /// effective (unchanged when workers == 0)
    const(ProcessorId)[]  callerCpus;  /// confine/pin set; empty when unchanged
    bool                  callerSharesWorkerCpu; /// no free home-group CPU was left
    bool                  callerConsumes;        /// the caller owns a ConsumerView
    uint nativeConsumers;   /// workers in the home group
    uint foreignConsumers;  /// workers in another group (span only)
    uint externalConsumers; /// caller view + FarmSizing.extraConsumers
    uint expectedConsumers; /// passed to AntFarm.create
    uint maxSmall;          /// control token + extraSmallProducers
    uint maxBulk;           /// FarmSizing.bulkProducers
    const(string)[] notes;  /// e.g. "group 1 not used: osExcluded"

    private this(const FarmHostOptions options, const PoolResolution candidates)
    {
        this.options = options;
        this.candidates = candidates;
    }

    @property uint workers() const @nogc nothrow
    {
        return cast(uint) resolution.workers.length;
    }

    @property bool crossGroup() const @nogc nothrow
    {
        return foreignConsumers != 0;
    }

    string summary() const
    {
        import std.format : format;

        string s = workers == 0
            ? "FarmHost: no workers, the caller consumes"
            : format("FarmHost: %s worker%s on group%s %s {%s}%s, idle %s",
                workers, workers == 1 ? "" : "s", groups.length == 1 ? "" : "s",
                groupsText(groups), formatCpuList(resolution.workers),
                crossGroup ? " (crossGroup)" : "", idleText());
        if (workers != 0)
            s ~= format("; caller %s%s%s", caller,
                callerCpus.length ? " {" ~ formatCpuList(callerCpus) ~ "}" : "",
                callerSharesWorkerCpu ? " (shares worker CPUs)" : "");
        s ~= format("; consumers %s native + %s foreign + %s external, producers %s small + %s bulk",
            nativeConsumers, foreignConsumers, externalConsumers, maxSmall, maxBulk);
        s ~= format("; Farm %s MiB, k %s%s", options.farm.ringMiB, options.farm.k,
            options.farmPerGroup && groups.length > 1 ? ", one per group" : "");
        return s;
    }

    private string idleText() const
    {
        import std.format : format;

        if (options.idle == WorkerIdle.spin) return "spin";
        if (options.idleTimeout == Duration.max) return "park (until woken)";
        return format("park (%s)", options.idleTimeout);
    }
}

/// A host operation failed. The message carries the plan summary when one
/// exists.
class FarmHostException : Exception
{
    /// `next` is the underlying exception, if any.
    this(string msg, Throwable next = null, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, file, line, next);
    }
}

/// Ordered close was refused before teardown. Repair the precondition and retry.
class FarmHostNotDrained : FarmHostException
{
    /// The host remains open and usable; the message names each busy resource.
    this(string msg, string file = __FILE__, size_t line = __LINE__) @safe pure nothrow
    {
        super(msg, null, file, line);
    }
}

/// The options cannot be satisfied on this topology; nothing was created.
/// Thrown by `FarmHost.resolve` and the constructor.
class FarmHostResolveException : FarmHostException
{
    const(string)[] problems; /// every problem found, in check order

    /// Message: "FarmHost options rejected: " and the problems.
    this(const(string)[] problems, string file = __FILE__, size_t line = __LINE__)
        @safe pure
    {
        import std.array : join;

        super("FarmHost options rejected: " ~ problems.join("; "), null, file, line);
        this.problems = problems;
    }
}

/// A `waitUntil` deadline expired. The host is still open with consumers
/// running: escalate (cancel) and wait again, or `abandon()`.
class FarmHostTimeout : FarmHostException
{
    Duration waited; /// the deadline that expired

    /// `waited` is the timeout that applied.
    this(string msg, Duration waited, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, null, file, line);
        this.waited = waited;
    }
}

/// Pool workers failed (a hook threw); the consumers are gone.
class FarmHostFailed : FarmHostException
{
    Throwable[] workerFailures; /// as `CacheAwarePool.workerFailures`

    /// `next` is the first worker failure, if any.
    this(string msg, Throwable[] workerFailures, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, workerFailures.length ? workerFailures[0] : null, file, line);
        this.workerFailures = workerFailures;
    }
}

/// Lifecycle of a `FarmHost`. Non-exhaustive: do not `final switch` on it.
enum HostState : ubyte
{
    open,      /// running: consumers live, every method usable
    failed,    /// a pool worker failed; only close()/abandon() remain
    closed,    /// close() completed the ordered teardown
    abandoned, /// abandon() stopped the consumers and leaked the rest
    stopping,  /// teardown has published non-open; in-flight wake() calls are draining
}

/// One emitter or service attached to the host Farm, driven by `step()`.
struct HostAttachment
{
    string name; /// for reports
    /// Run on the control thread with the host token, once per emission round
    /// in step(). Returns items published; zero does not imply drained.
    /// May be called again in the same step when any emitter published.
    /// Must return promptly, including on backpressure; never wait for a worker.
    /// May throw; the host stays open.
    size_t delegate(ref Token token) emit;
    /// close() precondition. Null = always drained.
    bool delegate() drained;
    /// After consumers stop, before runtimes/parks/Farm are destroyed. May be null.
    void delegate() finish;
}

// Worker hooks: CacheAwarePool takes context-free function pointers, so the
// host's worker state is process-global, owned by whichever host holds
// gHostClaimed (taken before any of the others is written). A FarmHost with
// workers runs on a GroupHost, which takes the claim; its workers find the
// one Farm through the GroupHost registry.
package(threadpool) shared bool gHostClaimed;
private shared uint gHostSubscribed;
private ConsumerView tHostView; // TLS: one cursor per worker

version (FarmHostTestHooks)
{
    /// Test-only (version FarmHostTestHooks): the worker index whose first
    /// visit records a start failure and stops the pool, or -1. Every other
    /// worker idles until the pool stops without subscribing, as a worker
    /// that exits before subscribing would never count.
    __gshared int hostTestFailStart = -1;
    /// Allocated on the control thread before start(); workers only store it.
    __gshared Throwable hostTestStartFailure;
    /// Test-only observer after a consumer poll, before the idle decision.
    /// Install before construction and clear only after the workers stop.
    __gshared void function(WorkerSelf*, ref ConsumerView, bool) nothrow @nogc hostTestAfterConsume;
}

// Subscribe failures cannot throw from workerBody (@nogc nothrow). The
// constructor's startup wait ends on this counter or on pool stop.
private shared uint gHostStartFailed;
private shared long gHostSubscribeResult;
private bool tHostSubscribed; // TLS: this worker holds a live view

private void noteWorkerFailure(WorkerSelf* self, Throwable failure) @nogc nothrow
{
    if (failure is null || self is null) return;
    if (self.workerIndex < gWorkers.length)
        gWorkers[self.workerIndex].failure.record(failure);
    requestStop();
}

// The entire worker hot path. workerBody has no start hook, so the first
// visit subscribes; a failure increments gHostStartFailed and stops the
// pool. An Error from a callback (an assert in a debug build) is recorded
// on the worker and stops the pool, which step/waitUntil surface as
// FarmHostFailed. The TLS flag is one predictable branch after the first visit.
private bool hostBody(WorkerSelf* self) @nogc nothrow
{
    version (FarmHostTestHooks)
    {
        if (hostTestFailStart >= 0)
        {
            if (self.workerIndex == hostTestFailStart)
                noteWorkerFailure(self, hostTestStartFailure);
            return false;
        }
    }
    if (!tHostSubscribed)
    {
        auto slot = home!GroupSlot();
        if (slot is null || slot.farm is null)
            return false;
        immutable result = tHostView.subscribe(slot.farm);
        if (result < 0)
        {
            atomicStore(gHostSubscribeResult, result);
            atomicOp!"+="(gHostStartFailed, 1u);
            requestStop();
            return false;
        }
        tHostSubscribed = true;
        atomicOp!"+="(gHostSubscribed, 1u);
    }
    try
    {
        immutable consumed = tHostView.consumeNext();
        version (FarmHostTestHooks)
            if (hostTestAfterConsume !is null)
                hostTestAfterConsume(self, tHostView, consumed);
        return consumed;
    }
    catch (Throwable t)
    {
        noteWorkerFailure(self, t);
        return false;
    }
}

private void hostStop(WorkerSelf*) @nogc nothrow
{
    if (!tHostSubscribed) return;
    tHostView.unsubscribe();
    tHostSubscribed = false;
}

/++
 + One Farm, its control token and its consumers: N pinned pool workers on
 + one LLC by default, or the calling thread alone (`workers == 0`).
 +
 + The constructing thread is the control thread. Work advances only while it
 + is in `step`/`waitUntil` (or an extra-token holder flushes): the host has
 + no background emitter. Control-thread methods are checked by `assert` in
 + debug builds. `resolution`, `callerCpus`, `summary`, `toString`, `farm`,
 + `state` and `wake` may be called from any thread while the host is open.
 + `wake` may overlap `close()`, `abandon()` or a worker-failure shutdown:
 + teardown stores a non-open state, then waits until in-flight `wake` calls
 + have returned, before stopping the pool.
 +
 + Runtimes and parks from `createRuntime`/`createPark` belong to the host and
 + are destroyed by `close()`, after the consumer join. There is no
 + destructor; write
 + ---
 + auto host = new FarmHost(options);
 + scope (exit) host.closeOrAbandon();
 + ---
 + so a refused close abandons (stopping the consumers) before its exception
 + propagates, including on normal return with undrained work.
 + Pool workers are non-daemon threads: a host with workers left open
 + (neither closed nor abandoned) hangs process exit.
 + While it has workers, a host runs on a `GroupHost` (one shared Farm, or one
 + per group with `farmPerGroup`; its consuming worker body the host's one
 + attachment) and so owns the process's
 + single `CacheAwarePool`; a second host with workers, a `GroupHost`, or one
 + started beside an application pool, is rejected. Without workers it is a
 + standalone Farm and may exist beside any host. Before concurrent construction or resolve calls, complete one
 + `FarmHost.resolve()` call: first topology discovery is an unlocked setup
 + operation, including for hosts with `workers == 0`. `resolve()` warms
 + that discovery even when it throws `FarmHostResolveException`.
 +
 + For maximum throughput on consumer-bound fine-grained workloads, a
 + hand-written consumer loop on the raw Farm can beat the host's pool
 + workers; see docs/guide/hosting.md (performance and tuning).
 +/
final class FarmHost
{
    /// The worker selection the constructor will start with `options`:
    /// discovers topology (a threadpool setup operation), then resolves as
    /// the overload below. Throws `FarmHostResolveException`. A throwing call
    /// still completes discovery, so it warms the cache for a later
    /// constructor or a SKIP probe such as `workersFit()`.
    static PoolResolution resolve(const FarmHostOptions options = FarmHostOptions.init)
    {
        return resolve(liveTopology(), options);
    }

    /// The worker selection for an explicit snapshot, whose OS scope fields
    /// apply: deterministic, no OS queries (the test seam, like
    /// `resolvePool`). The workers are accepted; every other CPU that passed
    /// the `options.pool` filters is `Reject.notSelected`; `requests` are
    /// `options.pool.processors`. Never null: with `workers == 0` there are
    /// no workers. Throws `FarmHostResolveException` listing every problem
    /// found.
    static PoolResolution resolve(ref const TopologySnapshot snap, const FarmHostOptions options)
    {
        return planHost(snap, options).resolution;
    }

    /// Resolve, create the Farm, register the control token, start and
    /// subscribe workers, place the caller. All or nothing: on any throw,
    /// everything created so far is undone in reverse order. Throws
    /// `FarmHostResolveException` (nothing created), `FarmHostFailed` (a
    /// worker failed to start) or `FarmHostException`.
    this(FarmHostOptions options = FarmHostOptions.init)
    {
        import std.format : format;

        director_ = Director.init;
        plan_ = planHost(liveTopology(), options);
        resolution_ = plan_.resolution;
        control_ = Thread.getThis();
        parkMode_ = plan_.workers != 0 && options.idle == WorkerIdle.park;
        scope (failure) undoConstruction();

        if (plan_.workers != 0)
            startWorkers(options); // the GroupHost creates the Farm and places the caller
        else
        {
            FarmCreateError error;
            const f = options.farm;
            farm_ = AntFarm.tryCreate(f.ringMiB, f.k, plan_.expectedConsumers, plan_.maxBulk,
                f.quotaBulk, plan_.maxSmall, f.quotaSmall, f.smallThreshold, f.hugePages, &error);
            if (farm_ is null)
                throw new FarmHostException(format("FarmHost: Farm creation failed (%s%s): %s [%s]",
                    error.kind, error.osError ? format(", os error %s", error.osError) : "",
                    error.detail, plan_.summary()));
        }
        if (group_ !is null)
            // The group host's caller token: planned as the caller's
            // participant slot and released by the group host at close.
            tokenRef_ = &group_.token(group_.callerGroup);
        else
        {
            token_ = farm_.registerProducer(Tier.small);
            if (!token_.valid)
                throw new FarmHostException("FarmHost: control token registration failed");
        }
        extraSmallBy_ = new uint[farms.length];
        extraBulkBy_ = new uint[farms.length];
        if (plan_.callerConsumes)
        {
            immutable result = view_.subscribe(farm_);
            if (result < 0)
                throw new FarmHostException(format("FarmHost: caller view could not subscribe (%s)", result));
            viewSubscribed_ = true;
        }
        if (plan_.workers != 0)
            awaitWorkers();
        atomicStore!(MemoryOrder.rel)(state_, HostState.open);
    }

    /// Default options with `workers` set.
    this(uint workers)
    {
        FarmHostOptions options;
        options.workers = workers;
        this(options);
    }

    // ---- inspection --------------------------------------------------------

    /// The host's worker selection, fixed for its life and kept after
    /// `close()`/`abandon()`. Before the constructor returns it is the
    /// resolved selection; afterwards the started one, with every worker's
    /// `StartState` and `PinOutcome`, and the same object as
    /// `pool.resolution`. Never null: with `workers == 0` it has no workers
    /// and every filter-passing CPU is `Reject.notSelected`. Any thread.
    @property const(PoolResolution) resolution() const @nogc nothrow
    {
        return resolution_;
    }

    /// The CPUs the caller is confined or pinned to; empty when its
    /// affinity is left unchanged (including `workers == 0`). Any thread.
    @property const(ProcessorId)[] callerCpus() const @nogc nothrow
    {
        return plan_.callerCpus;
    }

    /// One line: workers and where, caller placement, Farm participants
    /// (consumers native, foreign and external; small and bulk producers)
    /// and Farm sizing. Any thread.
    string summary() const
    {
        return plan_.summary();
    }

    /// `summary()`, every placement note (unused groups and why, caller
    /// compromises), then `resolution.summary()`. Any thread.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        sink(plan_.summary());
        sink("\n");
        foreach (note; plan_.notes)
        {
            sink("  note: ");
            sink(note);
            sink("\n");
        }
        sink("  resolution: ");
        sink(resolution_.summary());
        sink("\n");
    }

    /// The caller group's Farm, fixed for the host's life: the one Farm
    /// without `farmPerGroup`, the one `token` publishes into and the
    /// caller's view consumes with it. Any thread.
    @property AntFarm* farm() @nogc nothrow
    {
        return farm_;
    }

    /// Every Farm of the host: one, or one per group with `farmPerGroup`, in
    /// `groups` order. Any thread while the host is open.
    @property AntFarm*[] farms() @nogc nothrow
    {
        if (group_ !is null) return group_.farms;
        return farm_ is null ? null : (&farm_)[0 .. 1];
    }

    /// Group `llc`'s Farm, or null for a group without one on this host.
    /// Without `farmPerGroup` every selected group maps to `farm`. Any thread.
    AntFarm* farmOf(ushort llc) @nogc nothrow
    {
        if (group_ !is null) return group_.farmOf(llc);
        return llc == plan_.farmGroup || llc == defaultGroup ? farm_ : null;
    }

    /// The groups that supply workers, the caller's first; with `workers == 0`
    /// the caller's group alone. Any thread.
    @property const(ushort)[] groups() const @nogc nothrow
    {
        return group_ !is null ? group_.groups : plan_.groups;
    }

    /// The caller's group: the one `farm` and `token` belong to. Any thread.
    @property ushort callerGroup() const @nogc nothrow
    {
        return group_ !is null ? group_.callerGroup : plan_.farmGroup;
    }

    /// True when the host has more than one Farm. Any thread while open.
    @property bool farmPerGroup() @nogc nothrow
    {
        return farms.length > 1;
    }

    /// The `llc` argument that means the caller's group.
    enum ushort defaultGroup = ushort.max;

    /// Current lifecycle state (an acquire load). Any thread.
    @property HostState state() const @nogc nothrow
    {
        return atomicLoad!(MemoryOrder.acq)(state_);
    }

    // ---- control-thread token ---------------------------------------------

    /// The host-owned control token. Pass it by ref
    /// (`runtime.flush(host.token)`); do not `move` it out. Publications
    /// through it skip the park-mode wake: they are picked up within
    /// `idleTimeout`, or call `wake()`.
    @property ref Token token() return @nogc nothrow
    {
        return control();
    }

    /// The control token on group `llc`'s Farm; `token` is the caller
    /// group's. With `farmPerGroup` the host registers one per Farm on first
    /// use and releases them at `close()`; without it every group names
    /// `token`. Control thread only; pass it by ref. Throws for a group
    /// without a Farm on this host.
    ref Token tokenOf(ushort llc) return
    {
        requireOpen("tokenOf");
        version (assert) assertControl();
        return tokenFor(resolveGroup(llc, "tokenOf"));
    }

    /// An extra producer on group `llc`'s Farm (default the caller's),
    /// counted against that Farm's `FarmSizing.extraSmallProducers` or
    /// `bulkProducers`. Move-only; return it with `unregisterProducer`
    /// before `close()`. Throws when the tier's planned slots are used up.
    Token registerProducer(Tier tier = Tier.small, ushort llc = defaultGroup)
    {
        import std.format : format;

        requireOpen("registerProducer");
        immutable g = resolveGroup(llc, "registerProducer");
        immutable index = farmIndexOf(g);
        immutable planned = tier == Tier.small
            ? plan_.options.farm.extraSmallProducers : plan_.options.farm.bulkProducers;
        auto outstanding = tier == Tier.small ? &extraSmallBy_[index] : &extraBulkBy_[index];
        if (*outstanding >= planned)
            throw new FarmHostException(format("FarmHost.registerProducer: all %s planned %s "
                ~ "producer slots of group %s's Farm are in use (FarmSizing)", planned, tier, g));
        auto result = farms[index].registerProducer(tier);
        if (!result.valid)
            throw new FarmHostException(format("FarmHost.registerProducer: %s registration failed", tier));
        ++*outstanding;
        return result;
    }

    /// Return a token from `registerProducer`, to the Farm that issued it.
    /// Throws on an invalid token or one from another Farm.
    void unregisterProducer(ref Token token)
    {
        requireOpen("unregisterProducer");
        if (!token.valid)
            throw new FarmHostException("FarmHost.unregisterProducer: invalid token");
        size_t index = size_t.max;
        foreach (i, f; farms)
            if (f.farmId == token.issuer) index = i;
        if (index == size_t.max)
            throw new FarmHostException("FarmHost.unregisterProducer: token from another Farm");
        immutable tier = token.tier;
        farms[index].unregisterProducer(token);
        if (tier == Tier.small) --extraSmallBy_[index];
        else --extraBulkBy_[index];
    }

    /// Outstanding extra producer tokens over every Farm.
    private uint extraOutstanding() const @nogc nothrow
    {
        uint n;
        foreach (c; extraSmallBy_) n += c;
        foreach (c; extraBulkBy_) n += c;
        return n;
    }

    /// Index of group `llc`'s Farm in `farms`.
    private size_t farmIndexOf(ushort llc) @nogc nothrow
    {
        auto farm = farmOf(llc);
        foreach (i, f; farms)
            if (f is farm) return i;
        return 0;
    }

    /// `llc`, or the caller's group for `defaultGroup`; throws for a group
    /// without a Farm on this host.
    private ushort resolveGroup(ushort llc, string what)
    {
        import std.format : format;

        if (llc == defaultGroup) return callerGroup;
        if (farmOf(llc) is null)
            throw new FarmHostException(format("FarmHost.%s: group %s has no Farm on this host",
                what, llc));
        return llc;
    }

    /// The control thread's token on group `llc`'s Farm (a resolved group).
    private ref Token tokenFor(ushort llc) return @nogc nothrow
    {
        return group_ !is null ? group_.token(llc) : control();
    }

    /// The group an owned runtime or park was created on; the caller's for
    /// anything else.
    private ushort groupOf(const(void)* object) const @nogc nothrow
    {
        foreach (ref o; owned_)
            if (o.object is object) return o.llc;
        return callerGroup;
    }

    /// Wake the workers that consume group `llc`'s Farm (park mode only).
    private void wakeGroup(ushort llc) @nogc nothrow
    {
        if (!parkMode_) return;
        if (farmPerGroup) group_.wakeLlc(llc);
        else group_.wakeAll();
    }

    // ---- host-owned infrastructure ------------------------------------------

    /// An `ActorRuntime` on group `llc`'s Farm (default the caller's; with
    /// one Farm every group names it), flushed by `step()` through that
    /// Farm's control token and destroyed by `close()` after the consumer
    /// join. The pointer is borrowed: never destroy it yourself. Throws for
    /// a group without a Farm on this host.
    ActorRuntime* createRuntime(size_t capacity,
        ActorAllocator allocator = ActorAllocator.cRuntime(), ushort llc = defaultGroup)
    {
        import std.format : format;

        requireOpen("createRuntime");
        immutable g = resolveGroup(llc, "createRuntime");
        auto runtime = ActorRuntime.create(farmOf(g), capacity, allocator);
        if (runtime is null)
            throw new FarmHostException(format("FarmHost.createRuntime(%s): invalid capacity "
                ~ "or allocation failure", capacity));
        immutable name = farmPerGroup
            ? format("runtime#%s on group %s", runtimes_++, g) : format("runtime#%s", runtimes_++);
        auto owned = Owned(
            (ref Token token) {
                size_t total;
                foreach (_; 0 .. flushRounds)
                {
                    immutable n = runtime.flush(token);
                    total += n;
                    if (n == 0) break;
                }
                return total;
            },
            () => runtime.quiescent ? null
                : format("%s live=%s ready=%s", name, runtime.live, runtime.ready),
            () { runtime.destroy(); });
        owned.llc = g;
        owned.object = runtime;
        owned_ ~= owned;
        return runtime;
    }

    /// ditto, with the C runtime allocator.
    ActorRuntime* createRuntime(size_t capacity, ushort llc)
    {
        return createRuntime(capacity, ActorAllocator.cRuntime(), llc);
    }

    /// A `PrivateRootPark` on group `llc`'s Farm (default the caller's; with
    /// one Farm every group names it), pumped by `step()` through that
    /// Farm's control token and destroyed by `close()` after the consumer
    /// join; its roots and child waves run on that group's workers. The
    /// pointer is borrowed. The host never calls `takeOutcome`: drain
    /// tracked outcomes in a `waitUntil` predicate. `storage` selects
    /// per-object or slab root state (a slab needs `options.slabByteLimit`).
    /// Throws on `batchLimit == 0`, a rejected slab cap, allocation failure
    /// or a group without a Farm on this host.
    PrivateRootPark!(T, step, reclamation, storage)* createPark(T, alias step,
        PrivateRootReclamation reclamation = PrivateRootReclamation.emitter,
        PrivateStateStorage storage = PrivateStateStorage.perObject)(
        size_t capacity, PrivateRootOptions options = PrivateRootOptions.init,
        ushort llc = defaultGroup)
    {
        import std.format : format;

        requireOpen("createPark");
        if (options.batchLimit == 0)
            throw new FarmHostException("FarmHost.createPark: batchLimit must be > 0");
        immutable g = resolveGroup(llc, "createPark");
        auto park = PrivateRootPark!(T, step, reclamation, storage).create(farmOf(g), capacity, options);
        if (park is null)
            throw new FarmHostException(format("FarmHost.createPark(%s): invalid capacity "
                ~ "or allocation failure", capacity));
        immutable name = farmPerGroup
            ? format("park#%s on group %s", parks_++, g) : format("park#%s", parks_++);
        immutable tracked = options.tracking;
        auto owned = Owned(
            (ref Token token) => park.pumpCounted(token).tables,
            () => park.live == 0 ? null
                : format("%s live=%s%s", name, park.live, tracked ? " (tracked: drain takeOutcome)" : ""),
            () { park.destroy(); },
            true);
        owned.llc = g;
        owned.object = park;
        owned_ ~= owned;
        return park;
    }

    /// Drive `attachment` from every `step()`; see `HostAttachment`.
    void attach(HostAttachment attachment)
    {
        requireOpen("attach");
        attachments_ ~= attachment;
    }

    // ---- publication with the control token (+ wake in park mode) ----------

    /// `runtime.flush(token, maximum)` with the token of the Farm the
    /// runtime was created on, waking that Farm's parked workers when it
    /// wrote. Aborts rather than throwing `FarmHostException` once the host
    /// is not open: after `close()`, after `abandon()`, or after a worker
    /// failure.
    size_t flush(ActorRuntime* runtime, size_t maximum = 32) @nogc nothrow
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != HostState.open)
            fatal("FarmHost.flush after close/abandon or worker failure");
        version (assert) assertControl();
        immutable g = groupOf(runtime);
        immutable n = runtime.flush(tokenFor(g), maximum);
        if (n != 0) wakeGroup(g);
        return n;
    }

    /// `park.pump(token)`, waking parked workers when any table (child or
    /// root) was written. Returns the root count, as `pump` does. Aborts
    /// rather than throwing `FarmHostException` once the host is not open:
    /// after `close()`, after `abandon()`, or after a worker failure.
    size_t pump(Park)(Park* park) @nogc nothrow
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != HostState.open)
            fatal("FarmHost.pump after close/abandon or worker failure");
        version (assert) assertControl();
        immutable g = groupOf(park);
        immutable count = park.pumpCounted(tokenFor(g));
        if (count.tables != 0) wakeGroup(g);
        return count.roots;
    }

    /// Publish every actor into `wave`, waking after each accepted prefix and
    /// stepping on backpressure. False if the wave failed (seal it anyway,
    /// as with `ActorWave.publish`). Aborts rather than throwing
    /// `FarmHostException` once the host is not open: after `close()`, after
    /// `abandon()`, or after a worker failure.
    bool publish(alias operation, T)(ref ActorWave wave, scope ActorHandle!T[] actors,
        ushort llc = defaultGroup)
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != HostState.open)
            fatal("FarmHost.publish after close/abandon or worker failure");
        version (assert) assertControl();
        immutable g = resolveGroup(llc, "publish");
        size_t offset;
        while (offset < actors.length)
        {
            immutable written = wave.publish!operation(actors[offset .. $], tokenFor(g));
            if (written != 0)
            {
                offset += written;
                wakeGroup(g);
                continue;
            }
            if (wave.handle.failed) return false;
            step();
        }
        return true;
    }

    /// Wake parked workers. Any thread while the host is open. A call that
    /// overlaps `close()`, `abandon()` or a worker-failure shutdown is a
    /// no-op once teardown has stored a non-open state, and an in-flight
    /// call finishes before the pool's wait slots are destroyed. No-op
    /// without workers, in spin mode, or once the host is not open.
    void wake() @nogc nothrow
    {
        if (!parkMode_) return;
        // Increment before the state load so teardown's drain cannot miss
        // a wake that has already decided the host is open. Both sides are
        // seq_cst so that decision and the drain observe one order.
        atomicOp!"+="(wakers_, 1u);
        scope (exit) atomicOp!"-="(wakers_, 1u);
        if (atomicLoad!(MemoryOrder.seq)(state_) == HostState.open)
            group_.wakeAll();
    }

    /// Options for an asynchronous `antfarm.parallel.parallelFor` region
    /// wired to this host's wake: every accepted write wakes parked workers.
    /// Drive the region with `step()` until its handle reports completion.
    /// The host must outlive the region. Replaces the supplied wake
    /// callback/context. Blocking loops run on the fork-join dispatcher.
    antfarm.parallel.Options parallelOptions(
        antfarm.parallel.Options options = antfarm.parallel.Options.init) @nogc nothrow
    {
        options.wake = &wakeParallel;
        options.wakeContext = cast(void*) this;
        return options;
    }

    private static void wakeParallel(void* context) @nogc nothrow
    {
        (cast(FarmHost) context).wake();
    }

    // ---- progress -----------------------------------------------------------

    /// One step: emit every runtime, park and attachment for at most
    /// maxEmitRounds rounds, stopping after a round with no publications.
    /// Wake parked workers even if nothing was published, then one
    /// `consumeNext` on the caller's view (a yield when it found nothing, or
    /// without a view). A zero-publication round never implies idle or done.
    /// Throws `FarmHostFailed` if a pool worker failed (state `failed`). The
    /// caller stays confined until `close()` or `abandon()` restores affinity.
    void step()
    {
        requireOpen("step");
        if (!control().valid)
            throw new FarmHostException("control token was moved out of host.token");
        foreach (round; 0 .. plan_.options.maxEmitRounds)
        {
            bool published;
            foreach (ref o; owned_)
                published |= o.emit(tokenFor(o.llc)) != 0;
            foreach (ref a; attachments_)
                if (a.emit !is null)
                    published |= a.emit(control()) != 0;
            // Do not spin here on backpressure: the workers may need the
            // wake below to release completed epochs before another write.
            if (!published) break;
        }
        // A peer can finish an epoch after other consumers' final idle
        // sweeps. Those parked consumers still pin its storage, so even a
        // refused publication needs polls before the producer can retry.
        if (parkMode_)
            group_.wakeAll();
        if (poolRunning_ && group_.pool.stopRequested)
            failed();
        if (!viewSubscribed_ || !view_.consumeNext())
            Thread.yield();
    }

    /// `while (!done()) step();` with the failure check and the opt-in
    /// `waitTimeout` deadline, fresh for this call. `done` runs on the control
    /// thread before every step and may drain `takeOutcome`. Throws
    /// `FarmHostTimeout` (host still open) or `FarmHostFailed`. After a worker
    /// failure, caller affinity stays confined until `close()` or `abandon()`.
    void waitUntil(scope bool delegate() done)
    {
        waitUntil(done, plan_.options.waitTimeout);
    }

    /// ditto, with an explicit deadline for this call (`Duration.max` = none).
    void waitUntil(scope bool delegate() done, Duration timeout)
    {
        import std.format : format;

        requireOpen("waitUntil");
        immutable bounded = timeout != Duration.max;
        immutable deadline = bounded ? MonoTime.currTime + timeout : MonoTime.max;
        while (!done())
        {
            if (poolRunning_ && group_.pool.stopRequested)
                failed();
            if (bounded && MonoTime.currTime >= deadline)
                throw new FarmHostTimeout(format("FarmHost.waitUntil: not done after %s; the host "
                    ~ "is still open (escalate, wait again, or abandon)", timeout), timeout);
            step();
        }
    }

    /// Request retirement of every owner (drain mode), wait until all are
    /// retired, then reclaim all. Senders must be quiescent. Throws
    /// `FarmHostException` naming the owner index and result on a rejected
    /// request (`waveOwned`: a wave still holds the owner; wait for its
    /// `completion.finished` first) or a failed reclaim (`busy` when a racing
    /// retirement finished while a wave still held aggregate membership).
    void retire(Owners...)(ref Owners owners)
    {
        retire(ActorRetireMode.drain, owners);
    }

    /// ditto, in `mode`.
    void retire(Owners...)(ActorRetireMode mode, ref Owners owners)
    {
        import std.format : format;

        requireOpen("retire");
        foreach (i, ref owner; owners)
        {
            immutable result = owner.requestRetire(mode);
            if (result != ActorRetireResult.requested && result != ActorRetireResult.alreadyRetired)
                throw new FarmHostException(format("FarmHost.retire: owner %s: request %s", i, result));
        }
        waitUntil(() {
            foreach (ref owner; owners)
                if (!owner.retired) return false;
            return true;
        });
        foreach (i, ref owner; owners)
        {
            immutable result = owner.reclaim();
            if (result != ActorReclaimResult.reclaimed)
                throw new FarmHostException(format("FarmHost.retire: owner %s: reclaim %s", i, result));
        }
    }

    // ---- advanced -----------------------------------------------------------

    /// The pool; null when `workers == 0`. Do not call start/shutdown on it.
    @property CacheAwarePool pool()
    {
        return group_ is null ? null : group_.pool;
    }

    /// The pool's single Director, held for the host's life (cadence, labels,
    /// idle policy). Empty when `workers == 0`. Lend it; do not `move` it out.
    @property ref Director director() return
    {
        return group_ is null ? director_ : group_.director;
    }

    /// The `GroupHost` this host runs on; null when `workers == 0`. Its
    /// `attachments` are this host's consuming body; do not close it.
    @property GroupHost groupHost() @nogc nothrow
    {
        return group_;
    }

    // ---- teardown -----------------------------------------------------------

    /// Ordered teardown: stop consumers, finish attachments, destroy parks and
    /// runtimes, restore the caller's affinity, unregister the token, destroy
    /// the Farm. Throws `FarmHostNotDrained` with no side effects when anything
    /// is not drained (the host stays open and usable). After a worker failure
    /// (state `failed`) it acts like `abandon()` and does not throw again.
    /// A no-op once closed or abandoned. Errors in later cleanup stages are
    /// rethrown after every stage ran. A host with workers that is never
    /// closed or abandoned hangs process exit (non-daemon pool threads).
    void close()
    {
        import std.array : join;
        import std.format : format;

        version (assert) assertControl();
        immutable current = state;
        if (current == HostState.closed || current == HostState.abandoned)
            return;
        if (current == HostState.failed || current == HostState.stopping)
        {
            abandon();
            return;
        }
        requireOpen("close");
        string[] offenders;
        bool hasParks;
        foreach (ref o; owned_)
        {
            hasParks |= o.park;
            if (auto why = o.busy())
                offenders ~= why;
        }
        foreach (ref a; attachments_)
            if (a.drained !is null && !a.drained())
                offenders ~= a.name ~ " not drained";
        if (immutable extra = extraOutstanding())
            offenders ~= format("%s extra producer token%s outstanding (unregisterProducer first)",
                extra, extra == 1 ? "" : "s");
        if (!control().valid)
            offenders ~= "the control token was moved out of host.token";
        if (offenders.length)
            throw new FarmHostNotDrained("FarmHost.close: not drained: " ~ offenders.join("; ")
                ~ (hasParks ? "; join every thread that may call requestCancel before close" : ""));

        // Non-open before the pool is torn down, so a concurrent wake()
        // either finishes first or does not call wakeAll.
        publishShutdown(HostState.stopping);
        auto failures = stopConsumers();
        Throwable[] errors;
        foreach_reverse (ref a; attachments_)
            if (a.finish !is null)
            {
                try a.finish();
                catch (Exception e) errors ~= e;
            }
        foreach_reverse (ref o; owned_)
            o.destroy();
        owned_ = null;
        attachments_ = null;
        if (group_ is null) farm_.unregisterProducer(token_);
        if (group_ !is null)
        {
            // Restores the caller's affinity, releases the registry, the
            // caller's tokens and the pool claim, destroys the Farm.
            try group_.close(); // group_ stays: `pool` still reports the stopped pool
            catch (Exception e) errors ~= e;
        }
        else
            farm_.destroy();
        farm_ = null;
        releaseClaim();
        atomicStore!(MemoryOrder.rel)(state_, HostState.closed);
        if (failures.length)
            throw new FarmHostFailed("FarmHost.close: pool workers failed: " ~ messages(failures), failures);
        if (errors.length)
            throw new FarmHostException("FarmHost.close: " ~ messages(errors), errors[0]);
    }

    /// Try ordered teardown with `close()`. If it throws, call `abandon()`
    /// to stop consumers and restore affinity, then rethrow the close error.
    /// Use `scope (exit) host.closeOrAbandon();` so undrained work cannot
    /// leave non-daemon workers running when the control thread exits.
    /// Use `close()` directly when a refused drain should remain retryable.
    ///
    /// The abandon-on-failure path is cleanup-only: one `scope(exit)` guard
    /// that calls `abandon()`. Do not use `scope(failure)` or `catch` here.
    /// Under LDC -O1 and above, a guard that runs while an exception is
    /// already unwinding (this function called from a `scope(exit)` guard,
    /// `close()` throwing because work is undrained) can be skipped, leaving
    /// the host open and its non-daemon workers hanging process exit. Keep the
    /// abandon call in the first guard to run and unconditioned: `abandon()`
    /// is idempotent, so it is a no-op once close() has already closed the
    /// host, and gating it on a later-declared success flag only makes it
    /// skippable.
    void closeOrAbandon()
    {
        // Unconditional: abandon() is a no-op once close() has closed the
        // host, and this guard must run even when close() throws from a
        // scope(exit) during unwinding. Gating it on a scope(success) flag
        // declared after it makes this the outer guard, which LDC -O1 skips
        // in that case.
        scope (exit) abandon();
        close();
    }

    /// Stop consumers, restore the caller's affinity, unregister the control
    /// token, and leak every runtime, park, attachment and the Farm. For a
    /// drain that will never finish, and for `scope (failure)`. External
    /// views must already be unsubscribed. A no-op once closed or abandoned.
    void abandon()
    {
        version (assert) assertControl();
        immutable current = state;
        if (current == HostState.closed || current == HostState.abandoned)
            return;
        if (current == HostState.open)
            publishShutdown(HostState.stopping);
        else
            drainWakers(); // failed or stopping: state is already non-open
        cast(void) stopConsumers();
        if (group_ is null && token_.valid)
            farm_.unregisterProducer(token_);
        if (group_ !is null)
            group_.abandon(); // affinity, registry, caller tokens and claim back; the Farm leaks
        releaseClaim();
        atomicStore!(MemoryOrder.rel)(state_, HostState.abandoned);
    }

private:
    enum flushRounds = 4;

    struct Owned
    {
        size_t delegate(ref Token) emit;
        string delegate() busy; // null when destroy would not abort
        void delegate() destroy;
        bool park;
        ushort llc;   // the group whose Farm and control token it uses
        void* object; // the runtime or park, for flush/pump lookups
    }

    HostPlan plan_;
    Rebindable!(const PoolResolution) resolution_;
    AntFarm* farm_;
    Token token_;      // standalone (workers == 0): the host's own control token
    Token* tokenRef_;  // hosted: the group host's caller token
    ConsumerView view_;
    bool viewSubscribed_;
    GroupHost group_;   // with workers: owns pool, Farm, caller placement, claim; kept after close
    Director director_; // empty: the GroupHost holds the live one
    bool poolRunning_, parkMode_;
    shared HostState state_;
    shared uint wakers_; // in-flight wake() calls; drained before pool shutdown
    Owned[] owned_;
    HostAttachment[] attachments_;
    uint runtimes_, parks_;
    uint[] extraSmallBy_, extraBulkBy_; // outstanding extra tokens per Farm, `farms` order
    // Keep layout and initialization identical across assertion modes: clients
    // can instantiate templates against a release-built threadpool library.
    Thread control_;

    ref Token control() return @nogc nothrow
    {
        return tokenRef_ !is null ? *tokenRef_ : token_;
    }

    void assertControl() @nogc nothrow
    {
        assert(Thread.getThis() is control_, "FarmHost: control-thread method called from another thread");
    }

    void requireOpen(string what)
    {
        version (assert) assertControl();
        immutable current = state;
        if (current == HostState.failed)
            throw new FarmHostException("FarmHost." ~ what ~ ": host failed: a pool worker failed; "
                ~ "only close() or abandon() remain");
        if (current != HostState.open)
            throw new FarmHostException("FarmHost." ~ what ~ ": host is " ~ stateText(current));
    }

    void startWorkers(FarmHostOptions options)
    {
        import std.algorithm.searching : canFind;

        atomicStore(gHostSubscribed, 0u);
        atomicStore(gHostStartFailed, 0u);
        atomicStore(gHostSubscribeResult, 0);
        version (FarmHostTestHooks)
        {
            if (hostTestFailStart >= 0)
                hostTestStartFailure = new FarmHostException(
                    "FarmHost test hook: injected start failure");
        }
        GroupHostOptions go;
        go.workers = options.workers;
        go.groups = options.groups;
        go.caller = options.caller;
        go.callerConsumes = options.callerConsumes;
        go.idle = options.idle;
        go.idleTimeout = options.idleTimeout;
        go.pool = options.pool;
        go.farm = options.farm;
        go.farmPerGroup = options.farmPerGroup;
        // Alone, the host's workers only consume, so the Farm plans exactly
        // 1 + extraSmallProducers and refuses a producer beyond that; other
        // attachments' workers may publish (`groupToken`).
        go.workerTokens = options.attachments.length != 0;
        // workerBody/workerStop, not the managed lane: the hot path is one
        // bool return, and a single raw attachment's body is installed as is.
        go.attachments = GroupAttachment("FarmHost", &hostBody, &hostStop, true) ~ options.attachments;
        try
            group_ = new GroupHost(go);
        catch (FarmHostException e)
        {
            if (e.msg.canFind("another GroupHost is open") || e.msg.canFind("another host owns the pool"))
                throw new FarmHostException("FarmHost: another host owns the pool [" ~ plan_.summary() ~ "]", e);
            throw new FarmHostException("FarmHost: pool start failed: " ~ e.msg ~ " [" ~ plan_.summary() ~ "]", e);
        }
        poolRunning_ = true;
        farm_ = group_.farmOf(group_.callerGroup);
        resolution_ = group_.resolution;
    }

    /// Wait until every worker subscribed its view. A subscribe failure or
    /// a worker Error stops the pool; workers that have not visited yet then
    /// exit without subscribing, so stop also ends the wait. The counter
    /// covers a failure that has been published but whose stop has not yet
    /// been observed.
    void awaitWorkers()
    {
        import std.format : format;

        auto pool = group_.pool;
        while (atomicLoad!(MemoryOrder.acq)(gHostSubscribed) < plan_.workers
            && !pool.stopRequested
            && atomicLoad!(MemoryOrder.acq)(gHostStartFailed) == 0)
            Thread.yield();
        if (pool.stopRequested || atomicLoad(gHostStartFailed) != 0)
        {
            auto failures = stopConsumers();
            if (failures.length == 0 && atomicLoad(gHostStartFailed) != 0)
                failures ~= new FarmHostException(format(
                    "FarmHost worker could not subscribe (%s)",
                    atomicLoad(gHostSubscribeResult)));
            throw new FarmHostFailed("FarmHost: workers failed to start: " ~ messages(failures), failures);
        }
    }

    /// Join the workers (if this host's pool still runs) and unsubscribe the
    /// caller view. Returns the pool's worker failures.
    Throwable[] stopConsumers()
    {
        Throwable[] failures;
        if (poolRunning_)
        {
            group_.stopWorkers(); // releases the Director, joins the workers
            poolRunning_ = false;
            failures = group_.pool.workerFailures();
        }
        if (viewSubscribed_)
        {
            view_.unsubscribe();
            viewSubscribed_ = false;
        }
        return failures;
    }

    void releaseClaim()
    {
        // The GroupHost releases the pool claim; the worker counters are ours.
        atomicStore(gHostSubscribed, 0u);
        atomicStore(gHostStartFailed, 0u);
        atomicStore(gHostSubscribeResult, 0);
    }

    void failed()
    {
        // failed before stop, so wake() stops calling wakeAll and the
        // drain below waits out any call that already passed the check.
        publishShutdown(HostState.failed);
        auto failures = stopConsumers();
        throw new FarmHostFailed("FarmHost: pool workers failed: " ~ messages(failures), failures);
    }

    /// Store a non-open state, then wait until every wake() that could
    /// still call wakeAll has returned. Control thread only.
    void publishShutdown(HostState next) @nogc nothrow
    {
        atomicStore!(MemoryOrder.seq)(state_, next);
        drainWakers();
    }

    void drainWakers() @nogc nothrow
    {
        while (atomicLoad!(MemoryOrder.seq)(wakers_) != 0)
            Thread.yield();
    }

    // Constructor unwinding, strictly in reverse.
    void undoConstruction()
    {
        cast(void) stopConsumers();
        if (group_ is null && farm_ !is null && token_.valid)
            farm_.unregisterProducer(token_);
        if (group_ !is null)
            group_.close(); // affinity, registry, caller tokens, claim, Farm
        else if (farm_ !is null)
            farm_.destroy();
        farm_ = null;
        releaseClaim();
        atomicStore!(MemoryOrder.rel)(state_, HostState.closed);
    }

    static string stateText(HostState s)
    {
        import std.conv : to;

        return s.to!string;
    }

    static string messages(const(Throwable)[] errors)
    {
        string s;
        foreach (i, e; errors)
            s ~= (i ? "; " : "") ~ e.msg;
        return s.length ? s : "(no failure recorded)";
    }
}

/// The host placement for `options` on `snap`; throws
/// `FarmHostResolveException` listing every problem found.
package(threadpool) HostPlan planHost(ref const TopologySnapshot snap, const FarmHostOptions options)
{
    import std.algorithm.searching : canFind;
    import std.format : format;

    string[] problems;
    string[] notes;

    // Hooks belong to the host.
    const hooks = options.pool;
    if (hooks.workerBody !is null) problems ~= "pool.workerBody must be null (the host owns worker hooks)";
    if (hooks.workerStop !is null) problems ~= "pool.workerStop must be null (the host owns worker hooks)";
    if (hooks.managedWorker.start !is null || hooks.managedWorker.pump !is null
        || hooks.managedWorker.stop !is null || hooks.managedWorker.poolStopped !is null)
        problems ~= "pool.managedWorker must be empty (the host owns worker hooks)";
    if (options.idle == WorkerIdle.park && options.idleTimeout <= Duration.zero)
        problems ~= "idleTimeout must be positive (Duration.max parks until woken)";
    if (options.maxEmitRounds == 0)
        problems ~= "maxEmitRounds must be positive";

    const candidates = resolvePool(snap, options.pool);
    immutable callerConsumes = options.callerConsumes || options.workers == 0;
    auto caller = CallerPlacement.unchanged;
    const(ProcessorId)[] callerCpus;
    bool callerShares;
    ushort farmGroup;
    ProcessorId[] chosen;     // home-first preference order
    ushort[] groups;
    if (options.workers != 0)
    {
        groups = selectGroups(candidates, options.groups, problems);
        if (groups.length == 0)
            throw new FarmHostResolveException(problems);
        immutable home = groups[0];
        farmGroup = home;
        const homeAccepted = candidates.workersIn(home);
        immutable reserve = options.caller != CallerPlacement.unchanged;

        size_t available;
        foreach (g; groups)
            available += candidates.workersIn(g).length;
        size_t n;
        if (options.workers == autoWorkers)
            n = reserve && homeAccepted.length >= 2 ? available - 1 : available;
        else if (options.workers > available)
            problems ~= format("%s workers requested but group%s %s ha%s %s accepted CPU%s; "
                ~ "request fewer workers or opt into HostGroups.span to add a group (cross-group)",
                options.workers, groups.length == 1 ? "" : "s", groupsText(groups),
                groups.length == 1 ? "s" : "ve", available, available == 1 ? "" : "s");
        else
            n = options.workers;

        // Home group first; with autoWorkers and a reservation, the home
        // group's last accepted CPU stays free for the caller.
        immutable homeTake = options.workers == autoWorkers && n < available
            ? homeAccepted.length - 1
            : (n < homeAccepted.length ? n : homeAccepted.length);
        chosen ~= homeAccepted[0 .. homeTake];
        foreach (g; groups[1 .. $])
            foreach (id; candidates.workersIn(g))
                if (chosen.length < n) chosen ~= id;

        if (groups.length > 1)
        {
            ushort processorGroup = chosen.length ? chosen[0].group : 0;
            foreach (id; chosen)
                if (id.group != processorGroup)
                {
                    problems ~= "HostGroups.span/all across Windows processor groups is not supported yet";
                    break;
                }
        }

        if (reserve && n != 0)
        {
            // Home-group CPUs in the frozen OS scope and online, minus workers.
            ProcessorId[] homeEligible, free;
            foreach (ref c; candidates.cpus)
                if (c.llcIndex == home && !(c.reasons & (Reject.osExcluded | Reject.offline)))
                {
                    homeEligible ~= c.id;
                    if (!chosen.canFind(c.id)) free ~= c.id;
                }
            caller = options.caller;
            callerShares = free.length == 0;
            if (callerShares)
                notes ~= format("caller shares worker CPUs: every CPU of group %s is a worker", home);
            if (options.caller == CallerPlacement.confine)
                callerCpus = callerShares ? homeEligible : free;
            else if (homeTake < homeAccepted.length)
                callerCpus = [homeAccepted[$ - 1]]; // the reserved CPU
            else
                callerCpus = [free.length ? free[$ - 1] : homeAccepted[$ - 1]];
        }
        else if (n != 0)
            notes ~= format("caller placement unchanged: the control thread is an external "
                ~ "participant and may run outside group %s", home);

        // Every physical group not supplying workers, with its reason.
        foreach (ref gd; candidates.groups)
        {
            if (groups.canFind(gd.llcIndex) || gd.physical == 0) continue;
            notes ~= gd.isAccepted
                ? format("group %s not used: %s accepted CPU%s not selected", gd.llcIndex,
                    gd.accepted, gd.accepted == 1 ? "" : "s")
                : format("group %s not used: %s", gd.llcIndex, groupWhy(candidates, gd.llcIndex));
        }
    }
    else if (options.groups.kind != HostGroups.Kind.automatic)
        notes ~= "groups ignored: workers == 0 starts no pool";

    if (problems.length)
        throw new FarmHostResolveException(problems);

    auto result = new HostPlan(options, candidates);
    result.resolution = candidates.narrowedTo(chosen);
    result.farmGroup = farmGroup;
    result.groups = groups;
    result.caller = caller;
    result.callerCpus = callerCpus;
    result.callerSharesWorkerCpu = callerShares;
    result.callerConsumes = callerConsumes;

    foreach (id; result.resolution.workers)
    {
        if (candidates[id].llcIndex == farmGroup) result.nativeConsumers++;
        else result.foreignConsumers++;
    }
    immutable external = cast(ulong)(callerConsumes ? 1 : 0) + options.farm.extraConsumers;
    immutable consumers = external + chosen.length;
    immutable smallProducers = 1UL + options.farm.extraSmallProducers;
    result.externalConsumers = cast(uint) external;

    if (consumers < 1 || consumers > MAX_CONSUMERS_LIMIT)
        problems ~= format("%s consumers (%s workers + %s external) exceed the Farm's %s-view limit",
            consumers, chosen.length, external, MAX_CONSUMERS_LIMIT);
    if (smallProducers > uint.max)
        problems ~= "extraSmallProducers overflows the small producer count";
    if (problems.length)
        throw new FarmHostResolveException(problems);
    result.expectedConsumers = cast(uint) consumers;
    result.maxSmall = cast(uint) smallProducers;
    result.maxBulk = options.farm.bulkProducers;

    const f = options.farm;
    immutable geometry = AntFarm.checkGeometry(f.ringMiB, f.k, result.expectedConsumers,
        result.maxBulk, f.quotaBulk, result.maxSmall, f.quotaSmall);
    if (geometry.kind != FarmCreateFailure.none)
        throw new FarmHostResolveException(["Farm sizing: " ~ geometry.detail]);
    result.notes = notes;
    return result;
}

/// The groups supplying workers, home first; empty plus problems when none.
private ushort[] selectGroups(const PoolResolution candidates, const HostGroups wanted,
    ref string[] problems)
{
    import std.format : format;

    final switch (wanted.kind)
    {
        case HostGroups.Kind.automatic:
        case HostGroups.Kind.all:
            ushort best;
            uint most;
            foreach (ref gd; candidates.groups)
                if (gd.accepted > most)
                {
                    most = gd.accepted;
                    best = gd.llcIndex;
                }
            if (most == 0)
            {
                problems ~= "no group has an accepted CPU: " ~ candidates.summary();
                return null;
            }
            if (wanted.kind == HostGroups.Kind.automatic)
                return [best];
            ushort[] every = [best];
            foreach (ref gd; candidates.groups)
                if (gd.isAccepted && gd.llcIndex != best) every ~= gd.llcIndex;
            return every;
        case HostGroups.Kind.only:
        case HostGroups.Kind.span:
            if (wanted.llcs.length == 0)
            {
                problems ~= "HostGroups.span needs at least one group";
                return null;
            }
            ushort[] result;
            foreach (i, g; wanted.llcs)
            {
                if (g >= candidates.groups.length)
                    problems ~= format("group %s does not exist (%s physical groups)",
                        g, candidates.groups.length);
                else if (!candidates.groups[g].isAccepted)
                    problems ~= format("group %s has no accepted CPU: %s", g, groupWhy(candidates, g));
                else
                {
                    bool duplicate;
                    foreach (earlier; wanted.llcs[0 .. i])
                        if (earlier == g) duplicate = true;
                    if (duplicate)
                        problems ~= format("group %s is listed twice", g);
                    else
                        result ~= g;
                }
            }
            return result.length == wanted.llcs.length ? result : null;
    }
}

/// Why group `g` has no worker: its common reasons, or every reason seen
/// when its CPUs disagree.
private string groupWhy(const PoolResolution candidates, ushort g)
{
    const gd = candidates.groups[g];
    if (gd.common != Reject.none)
        return reasonsText(gd.common);
    Reject any;
    foreach (ref c; candidates.cpus)
        if (c.llcIndex == g)
            any |= c.reasons;
    return "mixed " ~ reasonsText(any);
}

private string groupsText(const(ushort)[] groups)
{
    import std.conv : to;

    string s;
    foreach (i, g; groups)
        s ~= (i ? "," : "") ~ g.to!string;
    return s;
}
