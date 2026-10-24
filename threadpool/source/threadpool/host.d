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
import threadpool.hybrid : ManagedWorkerHooks, WorkerSelf, gWorkers;
import threadpool.worker : requestStop;
import threadpool.pin : PinOutcome, ThreadAffinity, confineCallingThread, restoreCallingThread;
import threadpool.pool : CacheAwarePool, Director, PoolOptions;
import threadpool.resolution : PoolResolution, Reject, reasonsText, resolvePool;
import threadpool.topology : ProcessorId, TopologySnapshot, formatCpuList;

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

    private enum Kind : ubyte { automatic, only, span }
    private Kind kind;
    private const(ushort)[] llcs;
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
        s ~= format("; Farm %s MiB, k %s", options.farm.ringMiB, options.farm.k);
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
// gHostClaimed (taken before any of the others is written).
private shared bool gHostClaimed;
private __gshared AntFarm* gHostFarm;
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
        if (gHostFarm is null)
            return false;
        immutable result = tHostView.subscribe(gHostFarm);
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
 + While it has workers, a host owns the process's single `CacheAwarePool`; a
 + second host with workers, or one started beside an application pool, is
 + rejected. Before concurrent construction or resolve calls, complete one
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
        const snap = CacheAwarePool.topology();
        return resolve(snap, options);
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
        const snap = CacheAwarePool.topology();
        plan_ = planHost(snap, options);
        resolution_ = plan_.resolution;
        control_ = Thread.getThis();
        parkMode_ = plan_.workers != 0 && options.idle == WorkerIdle.park;
        scope (failure) undoConstruction();

        FarmCreateError error;
        const f = options.farm;
        farm_ = AntFarm.tryCreate(f.ringMiB, f.k, plan_.expectedConsumers, plan_.maxBulk,
            f.quotaBulk, plan_.maxSmall, f.quotaSmall, f.smallThreshold, f.hugePages, &error);
        if (farm_ is null)
            throw new FarmHostException(format("FarmHost: Farm creation failed (%s%s): %s [%s]",
                error.kind, error.osError ? format(", os error %s", error.osError) : "",
                error.detail, plan_.summary()));
        token_ = farm_.registerProducer(Tier.small);
        if (!token_.valid)
            throw new FarmHostException("FarmHost: control token registration failed");
        if (plan_.callerConsumes)
        {
            immutable result = view_.subscribe(farm_);
            if (result < 0)
                throw new FarmHostException(format("FarmHost: caller view could not subscribe (%s)", result));
            viewSubscribed_ = true;
        }
        if (plan_.workers != 0)
            startWorkers(options);
        if (plan_.caller != CallerPlacement.unchanged)
        {
            immutable outcome = confineCallingThread(plan_.callerCpus, previousAffinity_);
            affinityChanged_ = previousAffinity_.captured;
            if (outcome != PinOutcome.pinned)
                throw new FarmHostException(format("FarmHost: caller %s to {%s} failed: %s [%s]",
                    plan_.caller, formatCpuList(plan_.callerCpus), outcome, plan_.summary()));
        }
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

    /// The host Farm, fixed for the host's life. Any thread.
    @property AntFarm* farm() @nogc nothrow
    {
        return farm_;
    }

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
        return token_;
    }

    /// An extra producer, counted against `FarmSizing.extraSmallProducers`
    /// or `bulkProducers`. Move-only; return it with `unregisterProducer`
    /// before `close()`. Throws when the tier's planned slots are used up.
    Token registerProducer(Tier tier = Tier.small)
    {
        import std.format : format;

        requireOpen("registerProducer");
        immutable planned = tier == Tier.small
            ? plan_.options.farm.extraSmallProducers : plan_.options.farm.bulkProducers;
        auto outstanding = tier == Tier.small ? &extraSmall_ : &extraBulk_;
        if (*outstanding >= planned)
            throw new FarmHostException(format("FarmHost.registerProducer: all %s planned %s "
                ~ "producer slots are in use (FarmSizing)", planned, tier));
        auto result = farm_.registerProducer(tier);
        if (!result.valid)
            throw new FarmHostException(format("FarmHost.registerProducer: %s registration failed", tier));
        ++*outstanding;
        return result;
    }

    /// Return a token from `registerProducer`. Throws on an invalid token.
    void unregisterProducer(ref Token token)
    {
        requireOpen("unregisterProducer");
        if (!token.valid)
            throw new FarmHostException("FarmHost.unregisterProducer: invalid token");
        immutable tier = token.tier;
        farm_.unregisterProducer(token);
        if (tier == Tier.small) --extraSmall_;
        else --extraBulk_;
    }

    // ---- host-owned infrastructure ------------------------------------------

    /// An `ActorRuntime` on the host Farm, flushed by `step()` and destroyed
    /// by `close()` after the consumer join. The pointer is borrowed: never
    /// destroy it yourself.
    ActorRuntime* createRuntime(size_t capacity,
        ActorAllocator allocator = ActorAllocator.cRuntime())
    {
        import std.format : format;

        requireOpen("createRuntime");
        auto runtime = ActorRuntime.create(farm_, capacity, allocator);
        if (runtime is null)
            throw new FarmHostException(format("FarmHost.createRuntime(%s): invalid capacity "
                ~ "or allocation failure", capacity));
        immutable name = format("runtime#%s", runtimes_++);
        owned_ ~= Owned(
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
        return runtime;
    }

    /// A `PrivateRootPark` on the host Farm, pumped by `step()` and destroyed
    /// by `close()` after the consumer join. The pointer is borrowed. The
    /// host never calls `takeOutcome`: drain tracked outcomes in a
    /// `waitUntil` predicate. `storage` selects per-object or slab root
    /// state (a slab needs `options.slabByteLimit`). Throws on
    /// `batchLimit == 0`, a rejected slab cap or allocation failure.
    PrivateRootPark!(T, step, reclamation, storage)* createPark(T, alias step,
        PrivateRootReclamation reclamation = PrivateRootReclamation.emitter,
        PrivateStateStorage storage = PrivateStateStorage.perObject)(
        size_t capacity, PrivateRootOptions options = PrivateRootOptions.init)
    {
        import std.format : format;

        requireOpen("createPark");
        if (options.batchLimit == 0)
            throw new FarmHostException("FarmHost.createPark: batchLimit must be > 0");
        auto park = PrivateRootPark!(T, step, reclamation, storage).create(farm_, capacity, options);
        if (park is null)
            throw new FarmHostException(format("FarmHost.createPark(%s): invalid capacity "
                ~ "or allocation failure", capacity));
        immutable name = format("park#%s", parks_++);
        immutable tracked = options.tracking;
        owned_ ~= Owned(
            (ref Token token) => park.pumpCounted(token).tables,
            () => park.live == 0 ? null
                : format("%s live=%s%s", name, park.live, tracked ? " (tracked: drain takeOutcome)" : ""),
            () { park.destroy(); },
            true);
        return park;
    }

    /// Drive `attachment` from every `step()`; see `HostAttachment`.
    void attach(HostAttachment attachment)
    {
        requireOpen("attach");
        attachments_ ~= attachment;
    }

    // ---- publication with the control token (+ wake in park mode) ----------

    /// `runtime.flush(token, maximum)`, waking parked workers when it wrote.
    /// Aborts rather than throwing `FarmHostException` once the host is not
    /// open: after `close()`, after `abandon()`, or after a worker failure.
    size_t flush(ActorRuntime* runtime, size_t maximum = 32) @nogc nothrow
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != HostState.open)
            fatal("FarmHost.flush after close/abandon or worker failure");
        version (assert) assertControl();
        immutable n = runtime.flush(token_, maximum);
        if (n != 0 && parkMode_) pool_.wakeAll();
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
        immutable count = park.pumpCounted(token_);
        if (count.tables != 0 && parkMode_) pool_.wakeAll();
        return count.roots;
    }

    /// Publish every actor into `wave`, waking after each accepted prefix and
    /// stepping on backpressure. False if the wave failed (seal it anyway,
    /// as with `ActorWave.publish`). Aborts rather than throwing
    /// `FarmHostException` once the host is not open: after `close()`, after
    /// `abandon()`, or after a worker failure.
    bool publish(alias operation, T)(ref ActorWave wave, scope ActorHandle!T[] actors)
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != HostState.open)
            fatal("FarmHost.publish after close/abandon or worker failure");
        version (assert) assertControl();
        size_t offset;
        while (offset < actors.length)
        {
            immutable written = wave.publish!operation(actors[offset .. $], token_);
            if (written != 0)
            {
                offset += written;
                if (parkMode_) pool_.wakeAll();
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
            pool_.wakeAll();
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
        if (!token_.valid)
            throw new FarmHostException("control token was moved out of host.token");
        foreach (round; 0 .. plan_.options.maxEmitRounds)
        {
            bool published;
            foreach (ref o; owned_)
                published |= o.emit(token_) != 0;
            foreach (ref a; attachments_)
                if (a.emit !is null)
                    published |= a.emit(token_) != 0;
            // Do not spin here on backpressure: the workers may need the
            // wake below to release completed epochs before another write.
            if (!published) break;
        }
        // A peer can finish an epoch after other consumers' final idle
        // sweeps. Those parked consumers still pin its storage, so even a
        // refused publication needs polls before the producer can retry.
        if (parkMode_)
            pool_.wakeAll();
        if (poolRunning_ && pool_.stopRequested)
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
            if (poolRunning_ && pool_.stopRequested)
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
        return pool_;
    }

    /// The pool's single Director, held for the host's life (cadence, labels,
    /// idle policy). Empty when `workers == 0`. Lend it; do not `move` it out.
    @property ref Director director() return
    {
        return director_;
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
        if (extraSmall_ + extraBulk_ != 0)
            offenders ~= format("%s extra producer token%s outstanding (unregisterProducer first)",
                extraSmall_ + extraBulk_, extraSmall_ + extraBulk_ == 1 ? "" : "s");
        if (!token_.valid)
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
        if (affinityChanged_ && !restoreCallingThread(previousAffinity_))
            errors ~= new FarmHostException("FarmHost.close: the caller's affinity could not be restored");
        affinityChanged_ = false;
        farm_.unregisterProducer(token_);
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
        if (affinityChanged_)
            cast(void) restoreCallingThread(previousAffinity_);
        affinityChanged_ = false;
        if (token_.valid)
            farm_.unregisterProducer(token_);
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
    }

    HostPlan plan_;
    Rebindable!(const PoolResolution) resolution_;
    AntFarm* farm_;
    Token token_;
    ConsumerView view_;
    bool viewSubscribed_;
    CacheAwarePool pool_;
    Director director_;
    bool claimed_, poolRunning_, parkMode_, affinityChanged_;
    ThreadAffinity previousAffinity_;
    shared HostState state_;
    shared uint wakers_; // in-flight wake() calls; drained before pool shutdown
    Owned[] owned_;
    HostAttachment[] attachments_;
    uint runtimes_, parks_, extraSmall_, extraBulk_;
    // Keep layout and initialization identical across assertion modes: clients
    // can instantiate templates against a release-built threadpool library.
    Thread control_;

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
        import std.format : format;

        if (!cas(&gHostClaimed, false, true))
            throw new FarmHostException("FarmHost: another FarmHost owns the pool [" ~ plan_.summary() ~ "]");
        claimed_ = true;
        gHostFarm = farm_;
        atomicStore(gHostSubscribed, 0u);
        atomicStore(gHostStartFailed, 0u);
        atomicStore(gHostSubscribeResult, 0);
        version (FarmHostTestHooks)
        {
            if (hostTestFailStart >= 0)
                hostTestStartFailure = new FarmHostException(
                    "FarmHost test hook: injected start failure");
        }

        // The pool starts from the host's own resolution, so pool.resolution
        // is the host's. processors = workers keeps pool.resolve() selecting
        // the same CPUs.
        auto poolOptions = options.pool;
        poolOptions.processors = plan_.resolution.workers.dup;
        // workerBody/workerStop, not the managed lane: the hot path is one
        // bool return. Subscribe is lazy (no start hook). User hooks were
        // rejected by planHost; clear them so a copied options struct cannot
        // select both lanes.
        poolOptions.workerBody = &hostBody;
        poolOptions.workerStop = &hostStop;
        poolOptions.managedWorker = ManagedWorkerHooks.init;
        auto pool = new CacheAwarePool(poolOptions);
        try
            pool.start(plan_.resolution);
        catch (Exception e)
            throw new FarmHostException("FarmHost: pool start failed: " ~ e.msg
                ~ " [" ~ plan_.summary() ~ "]", e);
        // Only a pool this constructor started is ever shut down.
        pool_ = pool;
        poolRunning_ = true;
        director_ = pool_.director();
        resolution_ = pool_.resolution;
        // A subscribe failure or a worker Error stops the pool; workers
        // that have not visited yet then exit without subscribing, so stop
        // also ends the wait. The counter covers a failure that has been
        // published but whose stop has not yet been observed.
        while (atomicLoad!(MemoryOrder.acq)(gHostSubscribed) < plan_.workers
            && !pool_.stopRequested
            && atomicLoad!(MemoryOrder.acq)(gHostStartFailed) == 0)
            Thread.yield();
        if (pool_.stopRequested || atomicLoad(gHostStartFailed) != 0)
        {
            auto failures = stopConsumers();
            if (failures.length == 0 && atomicLoad(gHostStartFailed) != 0)
                failures ~= new FarmHostException(format(
                    "FarmHost worker could not subscribe (%s)",
                    atomicLoad(gHostSubscribeResult)));
            throw new FarmHostFailed("FarmHost: workers failed to start: " ~ messages(failures), failures);
        }
        if (options.idle == WorkerIdle.spin)
            director_.spin();
        else if (options.idleTimeout == Duration.max)
            director_.wait();
        else
            director_.sleep(options.idleTimeout);
    }

    /// Join the workers (if this host's pool still runs) and unsubscribe the
    /// caller view. Returns the pool's worker failures.
    Throwable[] stopConsumers()
    {
        Throwable[] failures;
        if (poolRunning_)
        {
            director_ = Director.init; // releases the director flag before shutdown
            pool_.shutdown();
            poolRunning_ = false;
            failures = pool_.workerFailures();
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
        if (!claimed_) return;
        gHostFarm = null;
        atomicStore(gHostSubscribed, 0u);
        atomicStore(gHostStartFailed, 0u);
        atomicStore(gHostSubscribeResult, 0);
        claimed_ = false;
        atomicStore!(MemoryOrder.rel)(gHostClaimed, false);
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
        if (affinityChanged_)
            cast(void) restoreCallingThread(previousAffinity_);
        affinityChanged_ = false;
        cast(void) stopConsumers();
        releaseClaim();
        if (farm_ !is null)
        {
            if (token_.valid)
                farm_.unregisterProducer(token_);
            farm_.destroy();
            farm_ = null;
        }
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
                    problems ~= "HostGroups.span across Windows processor groups is not supported yet";
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
            return [best];
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
