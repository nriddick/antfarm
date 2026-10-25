/++
 + The per-group host: the one owner of the pool, caller placement, Farms and
 + tokens that `FarmHost` and the fork-join dispatcher build on.
 +
 + A `GroupHost` resolves workers with `FarmHost`'s planner, creates one Farm
 + per selected LLC group (`farmPerGroup`) or one Farm for all of them, pins
 + or confines the caller, starts the pool with the attached worker hooks
 + and installs one `GroupSlot` per group so a worker finds its Farm with
 + `home!GroupSlot()` and a control thread with `farmOf(llc)`. Every thread
 + gets a lazily registered token per Farm from `token(llc)`; a worker's
 + tokens are released after its stop hook, the caller's in `close`.
 + Teardown is ordered: pool shutdown (stop hooks run), the caller's tokens,
 + the registry, the Farms, the caller's affinity.
 +
 + The worker loop is composed from `GroupAttachment`s. With only `@nogc`
 + attachments the pool runs its raw `WorkerBody` lane, and a single
 + attachment's body is installed as is; an attachment with a GC-enabled
 + hook (`install`, `start`, `serve`, `finish`) selects the pool's managed
 + lane, whose pump runs every attachment and parks until the earliest
 + deadline the service hooks report. A consuming attachment subscribes its
 + own view of the worker's home Farm; when none does, the host consumes it.
 + One host owns the process's single pool; a `FarmHost` and a `GroupHost`
 + exclude each other.
 +/
module threadpool.groups;

import antfarm;
import core.atomic;
import core.stdc.stdlib : malloc, free;
import core.thread : Thread;
import core.time : Duration, msecs;
import std.exception : enforce;
import std.typecons : Rebindable;
import threadpool.bins : at, home, install, uninstall, makeBins;
import threadpool.host : autoWorkers, CallerPlacement, FarmHostOptions, FarmHostException,
    FarmHostResolveException, FarmSizing, gHostClaimed, HostGroups, HostPlan, planHost,
    WorkerIdle, defaultHostPoolOptions;
import threadpool.hybrid : anyArmed, ManagedPumpResult, ManagedWorkerHooks, wakeOneArmed,
    wakeOneArmedIn, WorkerBody, WorkerSelf, WorkerStop;
import threadpool.pin : PinOutcome, pinProcessor, ThreadAffinity, confineCallingThread,
    restoreCallingThread;
import threadpool.pool : CacheAwarePool, Director, PoolOptions;
import threadpool.resolution : GroupDisposition, PoolResolution;
import threadpool.topology : noLlcIndex, ProcessorId, TopologySnapshot;

/// One LLC group's Farm, installed on the threadpool registry.
struct GroupSlot
{
    AntFarm* farm;  /// this group's Farm (shared by every group without `farmPerGroup`)
    ushort llc;     /// the group
    uint farmIndex; /// index into `GroupHost.farms`
}

/// What a GC-enabled service hook did in one step.
struct GroupServiceResult
{
    /// Something ran, was published, delivered or retired.
    bool progress;
    /// Earliest deadline the worker should wake for, in `MonoTime` ticks;
    /// `long.max` for none.
    long deadlineTicks = long.max;
}

/// One participant in the host's worker loop. `@nogc` hooks run in either
/// lane; the GC-enabled hooks select the managed lane.
struct GroupAttachment
{
    /// For reports.
    string name;
    /// `@nogc` per-worker step; true means it made progress. Keeps the pool's
    /// arm/recheck contract: the pool arms the worker's wait slot and calls
    /// once more before parking.
    WorkerBody body;
    /// `@nogc`, once per worker after its loop, before its tokens are released.
    WorkerStop stop;
    /// `body` subscribes the worker's own view of its home Farm (and `stop`
    /// unsubscribes it). When no attachment consumes, the host does.
    bool consumes;
    /// Control thread, after the Farms and registry exist and before the pool
    /// starts; build per-Farm state here. Managed lane.
    void delegate(GroupHost host) install;
    /// Control thread, after the pool stopped and before the Farms are
    /// destroyed. Managed lane.
    void delegate(GroupHost host) uninstall;
    /// Once per worker before its first step. Managed lane.
    void delegate(WorkerSelf* worker) start;
    /// Per step, after the `@nogc` bodies. Managed lane.
    GroupServiceResult delegate(WorkerSelf* worker) serve;
    /// Once per worker after its loop, before `stop`. Managed lane.
    void delegate(WorkerSelf* worker) finish;

    /// Needs the pool's managed lane.
    @property bool managed() const @nogc nothrow
    {
        return install !is null || uninstall !is null || start !is null
            || serve !is null || finish !is null;
    }
}

/// Everything `GroupHost` reads. `.init` is one Farm, automatic group,
/// autoWorkers, confined caller, parked workers.
struct GroupHostOptions
{
    /// The worker loop's participants, in step order. Required when
    /// `workers != 0`; the `(options, body, stop)` constructor prepends one.
    GroupAttachment[] attachments;
    /// Pool workers in total across the selected groups. 0 = no pool.
    uint workers = autoWorkers;
    /// Which groups supply workers: `automatic`, `only`, `span`, `all`.
    HostGroups groups;
    /// Caller placement; `callerProcessor` overrides it with one exact CPU.
    CallerPlacement caller = CallerPlacement.confine;
    /// Pin the caller to this processor and keep it out of the worker set.
    /// `ProcessorId(ushort.max, ushort.max)` leaves placement to `caller`.
    ProcessorId callerProcessor = ProcessorId(ushort.max, ushort.max);
    /// The caller will subscribe a view (counted in the Farm's consumers).
    bool callerConsumes = false;
    /// Idle policy of pool workers.
    WorkerIdle idle = WorkerIdle.park;
    /// Park bound for `WorkerIdle.park`; `Duration.max` parks until woken.
    Duration idleTimeout = 1.msecs;
    /// CPU filters and QoS. Hooks must be empty; the host installs them.
    PoolOptions pool = defaultHostPoolOptions;
    /// Farm geometry, applied to every Farm. `extraSmallProducers` is on
    /// top of one token per participant per Farm: the caller and, with
    /// `workerTokens`, every worker.
    FarmSizing farm;
    /// Workers may publish through `groupToken`, so every Farm plans one
    /// small-tier token per worker. False plans only the caller's token plus
    /// `extraSmallProducers` (`FarmHost`, whose workers only consume), so the
    /// Farm itself refuses a producer beyond the plan.
    bool workerTokens = true;
    /// One Farm per selected group, consumed only by that group's workers;
    /// false shares one Farm among every selected group.
    bool farmPerGroup = false;
}

private __gshared GroupAttachment[] gAttachments;
private __gshared bool gHostConsumes; // no attachment consumes: the host's view does
private ConsumerView tHostView;
private bool tHostSubscribed;

// Raw lane, one attachment: its body is installed directly, so the fork-join
// dispatcher's loop is byte-identical to a pool it ran alone on.
private void groupStop(WorkerSelf* self) @nogc nothrow
{
    foreach (ref a; gAttachments)
        if (a.stop !is null) a.stop(self);
    releaseThreadTokens();
}

// Raw lane, several `@nogc` attachments.
private bool groupRawBody(WorkerSelf* self) @nogc nothrow
{
    bool progress;
    foreach (ref a; gAttachments)
        if (a.body !is null && a.body(self)) progress = true;
    return progress;
}

private bool hostConsume() @nogc nothrow
{
    if (!tHostSubscribed)
    {
        auto slot = home!GroupSlot();
        if (slot is null || slot.farm is null) return false;
        if (tHostView.subscribe(slot.farm) < 0) fatal("GroupHost: worker view subscription failed");
        tHostSubscribed = true;
    }
    return tHostView.consumeNext();
}

// Managed lane: every attachment per step; park until the earliest deadline.
private void groupManagedStart(WorkerSelf* self)
{
    foreach (ref a; gAttachments)
        if (a.start !is null) a.start(self);
}

private ManagedPumpResult groupManagedPump(WorkerSelf* self)
{
    bool progress;
    long deadline = long.max;
    if (gHostConsumes && hostConsume()) progress = true;
    foreach (ref a; gAttachments)
        if (a.body !is null && a.body(self)) progress = true;
    foreach (ref a; gAttachments)
        if (a.serve !is null)
        {
            auto r = a.serve(self);
            if (r.progress) progress = true;
            if (r.deadlineTicks < deadline) deadline = r.deadlineTicks;
        }
    if (progress) return ManagedPumpResult.again();
    return deadline != long.max ? ManagedPumpResult.until(deadline) : ManagedPumpResult.idle();
}

private void groupManagedStop(WorkerSelf* self)
{
    foreach (ref a; gAttachments)
        if (a.finish !is null) a.finish(self);
    foreach (ref a; gAttachments)
        if (a.stop !is null) a.stop(self);
    if (tHostSubscribed) { tHostView.unsubscribe(); tHostSubscribed = false; }
    releaseThreadTokens();
}

// Per thread: one lazily registered token per Farm.
private Token* tTokens;
private size_t tTokenCount;
private __gshared AntFarm*[] gFarms;

/// Releases the calling thread's tokens on every Farm. Called for workers
/// after their stop hook and for the caller in `close`; call it yourself
/// on any other thread that used `token` before the host closes.
void releaseThreadTokens() @nogc nothrow
{
    if (tTokens is null) return;
    foreach (i; 0 .. tTokenCount)
        if (tTokens[i].valid && i < gFarms.length && gFarms[i] !is null)
            gFarms[i].unregisterProducer(tTokens[i]);
    free(tTokens);
    tTokens = null;
    tTokenCount = 0;
}

/// The Farm of group `llc` from the installed registry, or null. Usable on
/// a worker before the constructor that started it has returned.
AntFarm* groupFarmOf(ushort llc) @nogc nothrow
{
    auto slot = at!GroupSlot(llc);
    return slot is null ? null : slot.farm;
}

/// The calling thread's small-tier token on group `llc`'s Farm, registered
/// on first use and released by `releaseThreadTokens` (after a worker's stop
/// hook, in `close` for the caller). Fatal when the Farm has no slot left.
ref Token groupToken(ushort llc) @nogc nothrow
{
    import core.lifetime : emplace;
    auto slot = at!GroupSlot(llc);
    if (slot is null) fatal("GroupHost.token: group has no Farm");
    if (tTokens is null)
    {
        tTokenCount = gFarms.length;
        tTokens = cast(Token*) malloc(tTokenCount * Token.sizeof);
        if (tTokens is null) fatal("GroupHost.token: allocation failed");
        foreach (i; 0 .. tTokenCount) emplace(&tTokens[i]);
    }
    auto t = &tTokens[slot.farmIndex];
    if (!t.valid)
    {
        *t = slot.farm.registerProducer(Tier.small);
        if (!t.valid) fatal("GroupHost.token: producer registration failed (extraSmallProducers)");
    }
    return *t;
}

/// The host. Construct it with attachments (or one worker body); `close` it
/// from the same thread.
final class GroupHost
{
    /// The worker selection `options` would start (the test seam is
    /// `FarmHost.resolve` with the mapped options).
    static PoolResolution resolve(const GroupHostOptions options)
    {
        const snap = CacheAwarePool.topology();
        return planHost(snap, mapOptions(options)).resolution;
    }

    /// As `this(options)` with one consuming `@nogc` attachment, `body`/`stop`,
    /// prepended to `options.attachments`.
    this(GroupHostOptions options, WorkerBody body, WorkerStop stop = null)
    {
        enforce(body !is null, "GroupHost: null worker body");
        options.attachments = GroupAttachment("worker body", body, stop, true) ~ options.attachments;
        this(options);
    }

    /// Plan, create the Farms, install the registry, place the caller, run
    /// the attachments' `install` hooks and start the pool. All or nothing.
    this(GroupHostOptions options)
    {
        import std.format : format;

        director_ = Director.init;
        options_ = options;
        foreach (ref a; options.attachments)
            enforce(a.body !is null || a.managed, "GroupHost: an attachment needs a body or a managed hook");
        if (options.workers != 0 && options.attachments.length == 0)
            throw new FarmHostException("GroupHost: workers need at least one attachment");
        const snap = CacheAwarePool.topology();
        auto mapped = mapOptions(options);
        plan_ = planHost(snap, mapped);
        resolution_ = plan_.resolution;
        control_ = Thread.getThis();
        groups_ = plan_.groups.dup;
        immutable explicitCaller = options.callerProcessor.lp != ushort.max;
        if (groups_.length == 0)
        {
            // No workers: the caller's own group, for the one Farm.
            auto lp = explicitCaller ? snap.byId(options.callerProcessor) : null;
            groups_ = [lp !is null ? lp.llcIndex : cast(ushort) 0];
        }
        callerGroup_ = groups_[0];
        if (explicitCaller)
        {
            auto lp = snap.byId(options.callerProcessor);
            enforce(lp !is null && snap.isEligible(*lp),
                format("GroupHost: callerProcessor %s is unknown or ineligible", options.callerProcessor));
            foreach (id; resolution_.workers)
                enforce(id != options.callerProcessor, "GroupHost: callerProcessor is also a worker");
            callerCpus_ = [options.callerProcessor];
            callerPlacement_ = CallerPlacement.pin;
            callerGroup_ = lp.llcIndex;
            // A caller alone in its group still needs a Farm there.
            bool present;
            foreach (gi; groups_) if (gi == callerGroup_) present = true;
            if (!present) groups_ ~= callerGroup_;
        }
        else
        {
            callerCpus_ = plan_.callerCpus.dup;
            callerPlacement_ = plan_.caller;
        }
        // Claim before the failure guard: undoConstruction releases the
        // calling thread's tokens and clears the Farm registry, which belong
        // to whichever host is open, so a rejected host must leave first.
        // The registry is process-wide even without a pool to claim.
        if (gFarms !is null)
            throw new FarmHostException("GroupHost: another GroupHost is open [" ~ plan_.summary() ~ "]");
        if (plan_.workers != 0)
        {
            if (!cas(&gHostClaimed, false, true))
                throw new FarmHostException("GroupHost: another host owns the pool [" ~ plan_.summary() ~ "]");
            claimed_ = true;
        }
        scope (failure) undoConstruction();

        createFarms();
        installSlots();
        if (callerPlacement_ != CallerPlacement.unchanged)
        {
            immutable outcome = confineCallingThread(callerCpus_, previousAffinity_);
            affinityChanged_ = previousAffinity_.captured;
            if (outcome != PinOutcome.pinned)
                throw new FarmHostException(format("GroupHost: caller %s to %s failed: %s",
                    callerPlacement_, callerCpus_, outcome));
        }
        foreach (ref a; options_.attachments)
            if (a.install !is null) { a.install(this); ++installedAttachments_; }
        if (plan_.workers != 0)
            startPool();
        open_ = true;
    }

    /// The worker loop's participants.
    @property const(GroupAttachment)[] attachments() const @nogc nothrow { return options_.attachments; }
    /// The pool runs its managed lane (a GC-enabled attachment hook exists).
    @property bool managedLane() const @nogc nothrow
    {
        foreach (ref a; options_.attachments) if (a.managed) return true;
        return false;
    }

    // ---- inspection ----------------------------------------------------------

    /// The started worker selection (the pool's own after start).
    @property const(PoolResolution) resolution() const @nogc nothrow { return resolution_; }
    /// The caller's pin or confine set; empty when unchanged.
    @property const(ProcessorId)[] callerCpus() const @nogc nothrow { return callerCpus_; }
    /// Selected groups, home first.
    @property const(ushort)[] groups() const @nogc nothrow { return groups_; }
    /// The group the caller is placed in (the home group, or the group of
    /// `callerProcessor`).
    @property ushort callerGroup() const @nogc nothrow { return callerGroup_; }
    /// Pool workers started.
    @property uint workers() const @nogc nothrow { return cast(uint) resolution_.workers.length; }
    /// The pool, or null with no workers.
    @property CacheAwarePool pool() @nogc nothrow { return pool_; }
    /// The pool's Director, held for the host's life; empty without workers
    /// or after `stopWorkers`. Lend it; do not `move` it out.
    @property ref Director director() return @nogc nothrow { return director_; }
    /// False once `close` ran.
    @property bool isOpen() const @nogc nothrow { return open_; }
    /// The planner's one-line summary.
    string summary() const { return plan_.summary(); }

    /// The Farms, one per group with `farmPerGroup`, otherwise one.
    @property AntFarm*[] farms() @nogc nothrow { return farms_; }
    /// The Farm group `llc` publishes into and consumes from; null for a
    /// group the host did not select.
    AntFarm* farmOf(ushort llc) @nogc nothrow
    {
        return groupFarmOf(llc);
    }
    /// The calling worker's Farm (`home!GroupSlot`), or the caller's.
    AntFarm* homeFarm() @nogc nothrow
    {
        if (auto slot = home!GroupSlot()) return slot.farm;
        return farmOf(callerGroup_);
    }

    // ---- tokens ---------------------------------------------------------------

    /// The calling thread's small-tier token on group `llc`'s Farm,
    /// registered on first use (`groupToken`). Fatal when the Farm has no
    /// slot left: the planned count is participants plus
    /// `FarmSizing.extraSmallProducers`.
    ref Token token(ushort llc) @nogc nothrow
    {
        return groupToken(llc);
    }

    // ---- waking -------------------------------------------------------------

    /// Wakes one armed or parked worker; false when none is armed.
    bool wakeOne() @nogc nothrow { return wakeOneArmed(); }
    /// Wakes one armed or parked worker of group `llc`, which is the only
    /// kind that will poll that group's Farm; false when none is armed.
    bool wakeOneIn(ushort llc) @nogc nothrow { return wakeOneArmedIn(llc); }
    /// The hot-path test before `wakeOne`.
    bool anyWorkerArmed() @nogc nothrow { return anyArmed(); }
    /// Wakes every worker.
    void wakeAll() @nogc nothrow { if (pool_ !is null) pool_.wakeAll(); }
    /// Wakes the workers of group `llc`.
    void wakeLlc(ushort llc) @nogc nothrow { if (pool_ !is null) pool_.wakeLlc(llc); }

    // ---- teardown -------------------------------------------------------------

    /// Stop and join the workers, keeping the registry and Farms alive so a
    /// native host can return borrowed views/tokens before `close`. Does not
    /// drain application work; establish application completion first. Only
    /// the control thread may call it. Repeated calls are harmless.
    void stopWorkers()
    {
        enforce(Thread.getThis() is control_, "GroupHost.stopWorkers: control thread required");
        if (poolRunning_)
        {
            director_ = Director.init;
            pool_.shutdown();
            poolRunning_ = false;
        }
    }

    /// Shut the pool down (stop hooks run and release worker tokens), release
    /// the caller's tokens, uninstall the registry, destroy the Farms and
    /// restore the caller's affinity. Control thread only; a no-op once closed.
    void close()
    {
        if (!open_) return;
        open_ = false;
        undoConstruction();
    }

    /// `close` that leaks the Farms: stop and join the workers, run the
    /// attachments' `uninstall`, release the caller's tokens, uninstall the
    /// registry, restore the caller's affinity and release the pool claim,
    /// but leave every Farm alive for borrowers that were never drained
    /// (`FarmHost.abandon`). Control thread only; a no-op once closed.
    void abandon()
    {
        if (!open_) return;
        open_ = false;
        leakFarms_ = true;
        undoConstruction();
    }

private:
    GroupHostOptions options_;
    HostPlan plan_;
    Rebindable!(const PoolResolution) resolution_;
    ushort[] groups_;
    ushort callerGroup_;
    CallerPlacement callerPlacement_;
    const(ProcessorId)[] callerCpus_;
    AntFarm*[] farms_;
    GroupSlot[] slots_;
    bool installed_, claimed_, poolRunning_, affinityChanged_, open_, leakFarms_;
    size_t installedAttachments_;
    CacheAwarePool pool_;
    Director director_;
    ThreadAffinity previousAffinity_;
    Thread control_;

    static FarmHostOptions mapOptions(const GroupHostOptions o)
    {
        FarmHostOptions m;
        m.workers = o.workers;
        m.groups = cast(HostGroups) o.groups;
        immutable explicitCaller = o.callerProcessor.lp != ushort.max;
        m.caller = explicitCaller ? CallerPlacement.unchanged : o.caller;
        m.callerConsumes = o.callerConsumes;
        m.idle = o.idle;
        m.idleTimeout = o.idleTimeout;
        m.pool = cast(PoolOptions) o.pool;
        if (explicitCaller && m.pool.processors.length == 0)
        {
            // Keep the caller's CPU out of the worker set when the filter is open.
            const snap = CacheAwarePool.topology();
            ProcessorId[] rest;
            foreach (ref p; snap.processors)
                if (snap.isEligible(p) && ProcessorId.of(p) != o.callerProcessor) rest ~= ProcessorId.of(p);
            m.pool.processors = rest;
        }
        m.farm = o.farm;
        return m;
    }

    void createFarms()
    {
        immutable callerConsumes = options_.callerConsumes || plan_.workers == 0;
        immutable participants = (options_.workerTokens ? cast(uint) resolution_.workers.length : 0) + 1;
        immutable smallProducers = participants + options_.farm.extraSmallProducers;
        const f = options_.farm;
        if (options_.farmPerGroup)
        {
            foreach (g; groups_)
            {
                uint consumers = cast(uint) resolution_.workersIn(g).length
                    + (callerConsumes && g == callerGroup_ ? 1 : 0) + f.extraConsumers;
                farms_ ~= createOne(consumers == 0 ? 1 : consumers, smallProducers, g);
            }
        }
        else
        {
            uint consumers = cast(uint) resolution_.workers.length + (callerConsumes ? 1 : 0) + f.extraConsumers;
            farms_ ~= createOne(consumers == 0 ? 1 : consumers, smallProducers, groups_[0]);
        }
        gFarms = farms_;
    }

    AntFarm* createOne(uint consumers, uint smallProducers, ushort llc)
    {
        import std.format : format;

        const f = options_.farm;
        FarmCreateError error;
        auto farm = AntFarm.tryCreate(f.ringMiB, f.k, consumers, f.bulkProducers, f.quotaBulk,
            smallProducers, f.quotaSmall, f.smallThreshold, f.hugePages, &error);
        if (farm is null)
            throw new FarmHostException(format("GroupHost: Farm for group %s failed (%s): %s",
                llc, error.kind, error.detail));
        return farm;
    }

    void installSlots()
    {
        auto groups = groups_;
        auto farms = farms_;
        immutable perGroup = options_.farmPerGroup;
        slots_ = makeBins!GroupSlot(resolution_, (ref const GroupDisposition g) {
            GroupSlot s;
            s.llc = g.llcIndex;
            foreach (i, llc; groups)
                if (llc == g.llcIndex)
                {
                    s.farmIndex = perGroup ? cast(uint) i : 0;
                    s.farm = farms[s.farmIndex];
                }
            return s;
        });
        install(slots_, resolution_);
        installed_ = true;
    }

    void startPool()
    {
        auto poolOptions = options_.pool;
        poolOptions.processors = resolution_.workers.dup;
        gAttachments = options_.attachments;
        gHostConsumes = true;
        foreach (ref a; gAttachments) if (a.consumes) gHostConsumes = false;
        poolOptions.workerBody = null;
        poolOptions.workerStop = null;
        poolOptions.managedWorker = ManagedWorkerHooks.init;
        if (managedLane)
        {
            poolOptions.managedWorker.start = &groupManagedStart;
            poolOptions.managedWorker.pump = &groupManagedPump;
            poolOptions.managedWorker.stop = &groupManagedStop;
        }
        else
        {
            enforce(!gHostConsumes, "GroupHost: a raw-lane host needs a consuming attachment");
            poolOptions.workerBody = gAttachments.length == 1 ? gAttachments[0].body : &groupRawBody;
            poolOptions.workerStop = &groupStop;
        }
        pool_ = new CacheAwarePool(poolOptions);
        try
            pool_.start(cast(PoolResolution) resolution_);
        catch (Exception e)
            throw new FarmHostException("GroupHost: pool start failed: " ~ e.msg, e);
        poolRunning_ = true;
        director_ = pool_.director();
        resolution_ = pool_.resolution;
        if (options_.idle == WorkerIdle.spin)
            director_.spin();
        else if (options_.idleTimeout == Duration.max)
            director_.wait();
        else
            director_.sleep(options_.idleTimeout);
    }

    void undoConstruction()
    {
        stopWorkers();
        // Reverse order; only the hooks whose install ran.
        foreach_reverse (i; 0 .. installedAttachments_)
            if (options_.attachments[i].uninstall !is null) options_.attachments[i].uninstall(this);
        installedAttachments_ = 0;
        gAttachments = null;
        releaseThreadTokens(); // the caller's
        if (installed_)
        {
            uninstall!GroupSlot();
            installed_ = false;
        }
        gFarms = null;
        if (!leakFarms_)
            foreach (f; farms_) if (f !is null) f.destroy();
        farms_ = null;
        if (affinityChanged_)
            cast(void) restoreCallingThread(previousAffinity_);
        affinityChanged_ = false;
        if (claimed_)
        {
            claimed_ = false;
            atomicStore!(MemoryOrder.rel)(gHostClaimed, false);
        }
    }
}
