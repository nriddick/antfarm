/++
 + The per-group host: the one owner of the pool, caller placement, Farms and
 + tokens that `FarmHost` and the fork-join dispatcher build on.
 +
 + A `GroupHost` resolves workers with `FarmHost`'s planner, creates one Farm
 + per selected LLC group (`farmPerGroup`) or one Farm for all of them, pins
 + or confines the caller, starts the pool with the caller's raw worker body
 + and installs one `GroupSlot` per group so a worker finds its Farm with
 + `home!GroupSlot()` and a control thread with `farmOf(llc)`. Every thread
 + gets a lazily registered token per Farm from `token(llc)`; a worker's
 + tokens are released after its stop hook, the caller's in `close`.
 + Teardown is ordered: pool shutdown (stop hooks run), the caller's tokens,
 + the registry, the Farms, the caller's affinity.
 +
 + The host owns no consumer views and no queue: the worker body subscribes
 + its own view (and unsubscribes it in the stop hook) and decides what to
 + pump, as `CacheAwarePool` already required. One host owns the process's
 + single pool; a `FarmHost` and a `GroupHost` exclude each other.
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
import threadpool.hybrid : anyArmed, wakeOneArmed, WorkerBody, WorkerSelf, WorkerStop;
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

/// Everything `GroupHost` reads. `.init` is one Farm, automatic group,
/// autoWorkers, confined caller, parked workers.
struct GroupHostOptions
{
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
    /// top of one token per participant per Farm.
    FarmSizing farm;
    /// One Farm per selected group, consumed only by that group's workers;
    /// false shares one Farm among every selected group.
    bool farmPerGroup = false;
}

private __gshared WorkerStop gGroupUserStop;

private void groupStop(WorkerSelf* self) @nogc nothrow
{
    if (gGroupUserStop !is null) gGroupUserStop(self);
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

/// The host. Construct it with a worker body; `close` it from the same thread.
final class GroupHost
{
    /// The worker selection `options` would start (the test seam is
    /// `FarmHost.resolve` with the mapped options).
    static PoolResolution resolve(const GroupHostOptions options)
    {
        const snap = CacheAwarePool.topology();
        return planHost(snap, mapOptions(options)).resolution;
    }

    /// Plan, create the Farms, install the registry, place the caller and
    /// start the pool with `body`/`stop`. All or nothing.
    this(GroupHostOptions options, WorkerBody body, WorkerStop stop = null)
    {
        import std.format : format;

        director_ = Director.init;
        options_ = options;
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
        }
        else
        {
            callerCpus_ = plan_.callerCpus.dup;
            callerPlacement_ = plan_.caller;
        }
        scope (failure) undoConstruction();

        if (plan_.workers != 0)
        {
            if (!cas(&gHostClaimed, false, true))
                throw new FarmHostException("GroupHost: another host owns the pool [" ~ plan_.summary() ~ "]");
            claimed_ = true;
        }
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
        if (plan_.workers != 0)
            startPool(body, stop);
        open_ = true;
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
    /// The hot-path test before `wakeOne`.
    bool anyWorkerArmed() @nogc nothrow { return anyArmed(); }
    /// Wakes every worker.
    void wakeAll() @nogc nothrow { if (pool_ !is null) pool_.wakeAll(); }
    /// Wakes the workers of group `llc`.
    void wakeLlc(ushort llc) @nogc nothrow { if (pool_ !is null) pool_.wakeLlc(llc); }

    // ---- teardown -------------------------------------------------------------

    /// Shut the pool down (stop hooks run and release worker tokens), release
    /// the caller's tokens, uninstall the registry, destroy the Farms and
    /// restore the caller's affinity. Control thread only; a no-op once closed.
    void close()
    {
        if (!open_) return;
        open_ = false;
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
    bool installed_, claimed_, poolRunning_, affinityChanged_, open_;
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
        immutable participants = cast(uint) resolution_.workers.length + 1;
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

    void startPool(WorkerBody body, WorkerStop stop)
    {
        auto poolOptions = options_.pool;
        poolOptions.processors = resolution_.workers.dup;
        gGroupUserStop = stop;
        poolOptions.workerBody = body;
        poolOptions.workerStop = &groupStop;
        poolOptions.managedWorker = typeof(poolOptions.managedWorker).init;
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
        if (poolRunning_)
        {
            director_ = Director.init;
            pool_.shutdown();
            poolRunning_ = false;
        }
        releaseThreadTokens(); // the caller's
        if (installed_)
        {
            uninstall!GroupSlot();
            installed_ = false;
        }
        gFarms = null;
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
