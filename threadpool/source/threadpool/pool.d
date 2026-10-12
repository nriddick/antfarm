module threadpool.pool;

import core.atomic;
import core.thread;
import core.time : Duration, MonoTime, msecs;
import std.exception : enforce;

import threadpool.hybrid;
import threadpool.os_scope : OsScope, osScope;
import threadpool.pin : PinOutcome;
import threadpool.resolution;
import threadpool.stats;
import threadpool.topology;
import threadpool.worker;

version (Windows)
    version = ThreadpoolOs;
version (linux)
    version = ThreadpoolOs;

version (Windows)
{
    import threadpool.sys.win_wait;
}
else version (linux)
{
    import threadpool.sys.linux_wait;
}

/// Best-effort wake grid for idle workers. Late workers pull by default.
struct Cadence
{
    bool skipMissed = false;
    Duration spinLead;
}

struct PoolOptions
{
    bool skipSmtSiblings = false;
    bool enablePCores    = true;
    bool enableECores    = true;
    bool highQosOnP      = true;
    bool ecoQosOnE       = true;
    uint pSpinIters      = 128;
    uint eSpinIters      = 16;
    /// Exact CPUs to consider, group-qualified. Empty = no CPU-list filter.
    /// Build with `parseCpuList("0-7,16-23")` or topology helpers. Duplicates
    /// collapse to one worker; `resolve().requests` reports them.
    const(ProcessorId)[] processors;
    /// Called on the pinned worker. `true` = more work, `false` = idle.
    WorkerBody workerBody;
    /// Optional; called once on each pinned worker after its `workerBody`
    /// loop exits during `shutdown`. Release per-worker state here, for
    /// example by unsubscribing a thread-local `ConsumerView`.
    WorkerStop workerStop;
    /// Optional GC-enabled/throwing lifecycle lane. Mutually exclusive with
    /// `workerBody`; `pump` must be non-null when this lane is selected.
    ManagedWorkerHooks managedWorker;
}

shared int gRunning;
__gshared CacheAwarePool gLivePool;

/// Non-owning filter over workers. Valid only while the issuing `Director` lives.
struct Selection
{
    private CacheAwarePool pool;
    private bool filterLlc;
    private ushort llcIndex;
    private bool wantP;
    private bool wantE;
    private bool filterLabel;
    private string label;
    private bool filterNumber;
    private long number;

    Selection llc(ushort i)
    {
        auto s = this;
        s.filterLlc = true;
        s.llcIndex = i;
        return s;
    }

    @property Selection classP()
    {
        auto s = this;
        s.wantP = true;
        return s;
    }

    @property Selection classE()
    {
        auto s = this;
        s.wantE = true;
        return s;
    }

    Selection labeled(string name)
    {
        auto s = this;
        s.filterLabel = true;
        s.label = name;
        return s;
    }

    Selection numbered(long n)
    {
        auto s = this;
        s.filterNumber = true;
        s.number = n;
        return s;
    }

    void spin(uint iters = 0)
    {
        enforceOwner();
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            immutable n = iters != 0 ? iters : (r.isP ? pool.options.pSpinIters : pool.options.eSpinIters);
            atomicStore(r.spinIters, n);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.spin);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    void sleep(Duration d)
    {
        enforceOwner();
        immutable ns = d.total!"nsecs";
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicStore(r.sleepNs, ns > 0 ? cast(ulong) ns : 0);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.sleep);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    void sleepUntil(MonoTime t)
    {
        enforceOwner();
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicStore(r.sleepUntilTicks, t.ticks);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.sleepUntil);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    void wait()
    {
        enforceOwner();
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.wait);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    void cadence(Duration period)
    {
        cadence(MonoTime.currTime, period, Cadence.init);
    }

    void cadence(Duration period, Cadence opt)
    {
        cadence(MonoTime.currTime, period, opt);
    }

    void cadence(MonoTime epoch, Duration period, Cadence opt = Cadence.init)
    {
        enforceOwner();
        enforce(period > Duration.zero, "threadpool: cadence period must be > 0");
        immutable tps = MonoTime.ticksPerSecond;
        immutable pns = period.total!"nsecs";
        immutable periodTicks = pns > 0 ? pns * tps / 1_000_000_000L : 0;
        enforce(periodTicks > 0, "threadpool: cadence period is shorter than one tick");
        immutable lns = opt.spinLead.total!"nsecs";
        immutable leadTicks = lns > 0 ? lns * tps / 1_000_000_000L : 0;
        immutable seq = ++pool.driftSeq;
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicStore(r.cadenceEpochTicks, epoch.ticks);
            atomicStore(r.cadencePeriodTicks, periodTicks);
            atomicStore(r.cadenceDriftTicks, 0);
            atomicStore(r.cadenceDriftUntilTicks, 0);
            atomicStore(r.cadenceDriftSeq, seq);
            atomicStore(r.cadenceSkip, opt.skipMissed ? 1u : 0u);
            atomicStore(r.cadenceSpinLeadTicks, leadTicks);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.cadence);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    void signal()
    {
        enforceOwner();
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            wakeWorker(cast(uint) i);
        }
    }

    /// One-shot phase step. Positive delays beats; negative pulls them sooner.
    void nudge(Duration phase)
    {
        enforceOwner();
        immutable ticks = durationToTicks(phase);
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicFetchAdd(r.cadenceEpochTicks, ticks);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

    /// Sequenced slew: new sequence, add `perBeat` to the period until
    /// `|period-delta|` accumulates to `until`, then return to nominal.
    /// `until <= 0` or `perBeat == 0` is a no-op slew.
    void drift(Duration perBeat, Duration until)
    {
        enforceOwner();
        immutable driftTicks = durationToTicks(perBeat);
        immutable untilTicks = absTicks(durationToTicks(until));
        immutable seq = ++pool.driftSeq;
        foreach (i, ref r; gWorkers)
        {
            if (!matches(r)) continue;
            atomicStore(r.cadenceDriftTicks, driftTicks);
            atomicStore(r.cadenceDriftUntilTicks, untilTicks);
            atomicStore!(MemoryOrder.rel)(r.cadenceDriftSeq, seq);
            bump(r);
            wakeWorker(cast(uint) i);
        }
    }

private:
    void enforceOwner()
    {
        enforce(pool !is null && pool.directorHeld, "threadpool: director selection is dead");
    }

    bool matches(ref const WorkerRec r) const
    {
        if (filterLlc && r.llcIndex != llcIndex) return false;
        if (wantP && !r.isP) return false;
        if (wantE && r.isP) return false;
        if (filterLabel && r.label != label) return false;
        if (filterNumber && (!r.hasNumber || r.number != number)) return false;
        return true;
    }

    static void bump(ref WorkerRec r) @nogc nothrow
    {
        atomicFetchAdd(r.epoch, 1);
    }

    static long durationToTicks(Duration d) @nogc nothrow
    {
        immutable tps = MonoTime.ticksPerSecond;
        return d.total!"nsecs" * tps / 1_000_000_000L;
    }

    static long absTicks(long t) @nogc nothrow
    {
        return t >= 0 ? t : -t;
    }
}

/// Single-owner idle-policy handle. Copy transfers; the source is emptied.
struct Director
{
    private CacheAwarePool pool;

    @disable this();
    @disable this(this);

    this(ref return scope Director rhs)
    {
        pool = rhs.pool;
        rhs.pool = null;
    }

    ~this()
    {
        if (pool !is null)
        {
            pool.releaseDirector();
            pool = null;
        }
    }

    void opAssign(Director rhs)
    {
        if (pool !is null)
            pool.releaseDirector();
        pool = rhs.pool;
        rhs.pool = null;
    }

    @property Selection all()
    {
        enforce(pool !is null, "threadpool: empty director");
        Selection s;
        s.pool = pool;
        return s;
    }

    Selection llc(ushort i) { return all.llc(i); }
    @property Selection classP() { return all.classP; }
    @property Selection classE() { return all.classE; }
    Selection labeled(string name) { return all.labeled(name); }
    Selection numbered(long n) { return all.numbered(n); }

    void spin(uint iters = 0) { all.spin(iters); }
    void sleep(Duration d) { all.sleep(d); }
    void sleepUntil(MonoTime t) { all.sleepUntil(t); }
    void wait() { all.wait(); }
    void signal() { all.signal(); }
    void cadence(Duration period) { all.cadence(period); }
    void cadence(Duration period, Cadence opt) { all.cadence(period, opt); }
    void cadence(MonoTime epoch, Duration period, Cadence opt = Cadence.init)
    {
        all.cadence(epoch, period, opt);
    }
    void nudge(Duration phase) { all.nudge(phase); }
    void drift(Duration perBeat, Duration until) { all.drift(perBeat, until); }

    private this(CacheAwarePool p)
    {
        pool = p;
    }
}

final class CacheAwarePool
{
    private PoolOptions options;
    private Thread[] threads;
    private const(TopologySnapshot)* snap;
    private bool directorHeld;
    private uint driftSeq;
    private Throwable[] failures;
    private PoolResolution lastResolution;

    this(PoolOptions options = PoolOptions.init)
    {
        this.options = options;
    }

    /// Thread-safe after the first completed `discover()`; the first call is setup.
    static const(TopologySnapshot) topology() @trusted
    {
        return discover();
    }

    /// Preview: the resolution `start()` will use, without starting anything.
    /// Discovers topology on first use (a setup operation).
    PoolResolution resolve() const
    {
        auto snap = discover();
        auto scope_ = osScope();
        return resolvePool(snap, scope_, options);
    }

    /// After `start`: the resolution including each worker's `StartState` and
    /// `PinOutcome`. After a failed start or a shutdown: the last attempt's
    /// resolution. Null before the first `start`.
    @property const(PoolResolution) resolution() const
    {
        return lastResolution;
    }

    /// Not thread-safe vs another `start` / `shutdown`. One live pool.
    /// Call after registry setup (`install` / `setLabel` / `installExchange`).
    void start()
    {
        version (ThreadpoolOs)
        {
            enforce(atomicLoad(gRunning) == 0, "threadpool: a pool is already running");
            enforce((options.workerBody is null && options.workerStop is null)
                    || options.managedWorker.pump is null,
                "threadpool: workerBody/workerStop and managedWorker are mutually exclusive");
            enforce((options.managedWorker.start is null && options.managedWorker.stop is null)
                    || options.managedWorker.pump !is null,
                "threadpool: managedWorker start/stop require a pump");

            auto res = resolve();
            lastResolution = res;
            snap = cachedSnapshot();
            enforce(snap !is null, "threadpool: topology not discovered");
            auto selected = res.workers;
            enforce(selected.length > 0,
                "threadpool: no logical processors selected; " ~ res.summary());

            ushort classCount = snap.classCount;
            if (classCount == 0) classCount = 1;

            resetStats();
            gWorkerBody = options.workerBody;
            gWorkerStop = options.workerStop;
            gManagedWorkerHooks = options.managedWorker;
            failures = null;
            gWorkers = new WorkerRec[](selected.length);
            gPinOutcome = new PinOutcome[](selected.length);
            createWaitSlots(cast(uint) selected.length);
            resetWorkerHandshake();
            directorHeld = false;
            atomicStore(gRunning, 1);

            gLivePool = this;
            try
            {
                foreach (i, id; selected)
                {
                    auto lp = snap.byId(id);
                    auto tc = tableClassIndex(lp.efficiencyClass, snap.maxEfficiencyClass, classCount);
                    auto isP = (classCount <= 1) || (tc == classP);
                    WorkerSelf self;
                    self.llcIndex = lp.llcIndex;
                    self.numaIndex = lp.numaIndex;
                    self.classIndex = tc;
                    self.cpuSetId = lp.cpuSetId;
                    self.workerIndex = cast(uint) i;
                    self.spinIters = isP ? options.pSpinIters : options.eSpinIters;
                    self.isP = isP;
                    bindNumaNeighborhood(self, *snap);

                    gWorkers[i].group = lp.group;
                    gWorkers[i].lpIndex = lp.lpIndex;
                    gWorkers[i].llcIndex = lp.llcIndex;
                    gWorkers[i].classIndex = tc;
                    gWorkers[i].isP = isP;
                    atomicStore(gWorkers[i].kind, IdleKind.wait);
                    atomicStore(gWorkers[i].spinIters, self.spinIters);

                    SpawnSpec spec;
                    spec.self = self;
                    spec.processor = lp;
                    spec.ecoQos = !isP && options.ecoQosOnE;
                    spec.applyQos = isP ? options.highQosOnP : options.ecoQosOnE;
                    if (isP && options.highQosOnP)
                        spec.ecoQos = false;
                    threads ~= spawnWorker(spec);
                }

                while (atomicLoad(gPinDone) < cast(int) threads.length)
                    Thread.sleep(msecs(1));

                lastResolution = res.withStart(gPinOutcome);
                if (atomicLoad(gPinOk) != cast(int) threads.length)
                    throw new Exception("threadpool: failed to pin worker(s) "
                        ~ lastResolution.pinFailures() ~ "; " ~ lastResolution.summary());
                releaseWorkers();
            }
            catch (Exception e)
            {
                abortStart();
                throw e;
            }
        }
        else
        {
            throw new Error("threadpool: pool start is not implemented in v1 on this OS");
        }
    }

    /// Stop every worker and join it. Each worker finishes its current
    /// `workerBody` call and then runs `workerStop`, if set; the pool has no
    /// queue of its own, so any draining of application work must happen
    /// before this call. Not thread-safe vs `start` or a second `shutdown`
    /// on another thread.
    void shutdown()
    {
        version (ThreadpoolOs)
        {
            if (atomicLoad(gRunning) == 0)
                return;
            requestStop();
            joinAll();
            foreach (ref rec; gWorkers)
                if (rec.failure !is null)
                    failures ~= rec.failure;
            destroyWaitSlots();
            gWorkers = null;
            gWorkerBody = null;
            gWorkerStop = null;
            if (gManagedWorkerHooks.poolStopped !is null)
                gManagedWorkerHooks.poolStopped();
            gManagedWorkerHooks = ManagedWorkerHooks.init;
            directorHeld = false;
            threads.length = 0;
            if (gLivePool is this)
                gLivePool = null;
            atomicStore(gRunning, 0);
        }
    }

    /// The pool owns no queue, so there is nothing to drain or discard.
    deprecated("CacheAwarePool owns no queue to drain; call shutdown()")
    void shutdown(bool drain)
    {
        cast(void) drain;
        shutdown();
    }

    /// Equivalent to `shutdown`; retained for compatibility.
    void shutdownNow()
    {
        shutdown();
    }

    /// Exclusive idle-policy token. One live director per running pool.
    Director director()
    {
        enforce(atomicLoad(gRunning) != 0, "threadpool: director requires a running pool");
        enforce(!directorHeld, "threadpool: director already held");
        directorHeld = true;
        return Director(this);
    }

    /// Tag a spawned worker. Not thread-safe vs another tag on the same LP.
    void tag(ProcessorId id, string label)
    {
        auto r = findRec(id);
        enforce(r !is null, "threadpool: no worker on that processor");
        r.label = label;
    }

    /// Tag a spawned worker with an integer.
    void tag(ProcessorId id, long n)
    {
        auto r = findRec(id);
        enforce(r !is null, "threadpool: no worker on that processor");
        r.hasNumber = true;
        r.number = n;
    }

    /// Wake every worker (does not change policy). Safe for producer threads.
    void wakeAll() @nogc nothrow
    {
        version (ThreadpoolOs)
        {
            foreach (i; 0 .. gWorkers.length)
                wakeWorker(cast(uint) i);
        }
    }

    /// Wake every worker assigned to physical group `llc` (does not change
    /// policy). Safe for producer threads; a group without workers is a no-op.
    void wakeLlc(ushort llc) @nogc nothrow
    {
        version (ThreadpoolOs)
        {
            foreach (i, ref r; gWorkers)
                if (r.llcIndex == llc)
                    wakeWorker(cast(uint) i);
        }
    }

    void releaseDirector() @nogc nothrow
    {
        directorHeld = false;
    }

    @property ushort workerCount() const @nogc nothrow
    {
        return cast(ushort) threads.length;
    }

    const(LogicalProcessor)* currentThreadMapping() const @nogc nothrow
    {
        auto s = cachedSnapshot();
        if (s is null) return null;
        return s.current();
    }

    PoolStats snapshotStats() const nothrow
    {
        return copyCounters();
    }

    /// Failures captured from managed worker lifecycle hooks after all workers
    /// have joined. Call after `shutdown`.
    Throwable[] workerFailures()
    {
        enforce(atomicLoad(gRunning) == 0,
            "threadpool: workerFailures requires a stopped pool");
        return failures.dup;
    }

    shared static ~this()
    {
        if (gLivePool !is null && atomicLoad(gRunning))
            gLivePool.shutdownNow();
    }

private:
    WorkerRec* findRec(ProcessorId id) @nogc nothrow
    {
        foreach (ref r; gWorkers)
            if (r.group == id.group && r.lpIndex == id.lp)
                return &r;
        return null;
    }

    void joinAll()
    {
        foreach (t; threads)
        {
            if (t !is null)
                t.join();
        }
    }

    void abortStart()
    {
        version (ThreadpoolOs)
        {
            requestStop();
            joinAll();
            destroyWaitSlots();
            gWorkers = null;
            gWorkerBody = null;
            gWorkerStop = null;
            if (gManagedWorkerHooks.poolStopped !is null)
                gManagedWorkerHooks.poolStopped();
            gManagedWorkerHooks = ManagedWorkerHooks.init;
            directorHeld = false;
            threads.length = 0;
            gLivePool = null;
            atomicStore(gRunning, 0);
        }
    }
}

version (unittest)
private PoolResolution resolveTest(ref const TopologySnapshot snap, PoolOptions opt)
{
    OsScope unrestricted;
    return resolvePool(snap, unrestricted, opt);
}

unittest
{
    TopologySnapshot snap;
    snap.classCount = 1;
    snap.maxEfficiencyClass = 0;
    LogicalProcessor a, b, c;
    a.group = 0; a.lpIndex = 0;
    b.group = 1; b.lpIndex = 0;
    c.group = 1; c.lpIndex = 1;
    snap.processors = [a, b, c];

    PoolOptions all;
    auto got = resolveTest(snap, all);
    assert(got.workers == [ProcessorId(0, 0), ProcessorId(1, 0), ProcessorId(1, 1)]);

    PoolOptions byId;
    byId.processors = [ProcessorId(1, 0)];
    got = resolveTest(snap, byId);
    assert(got.workers == [ProcessorId(1, 0)]);
    assert(got[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(got[ProcessorId(1, 1)].reasons == Reject.notRequested);
    assert(got.requests.length == 1 && got.requests[0].accepted);
}

unittest
{
    TopologySnapshot snap;
    snap.classCount = 1;
    LogicalProcessor live, parked, offline;
    live.lpIndex = 0;
    parked.lpIndex = 1;
    parked.parkedAtDiscovery = true;
    offline.lpIndex = 2;
    offline.offlineAtDiscovery = true;
    snap.processors = [live, parked, offline];

    PoolOptions opt;
    auto got = resolveTest(snap, opt);
    assert(got.workers == [ProcessorId(0, 0), ProcessorId(0, 1)],
        "parking is informational; offline is skipped");
    assert(got[ProcessorId(0, 1)].osParked && got[ProcessorId(0, 1)].accepted);
    assert(got[ProcessorId(0, 2)].reasons == Reject.offline);
}

unittest
{
    TopologySnapshot snap;
    snap.classCount = 1;
    LogicalProcessor allowed, excluded;
    allowed.lpIndex = 0;
    excluded.lpIndex = 1;
    excluded.affinityExcluded = true;
    snap.processors = [allowed, excluded];

    PoolOptions opt;
    auto res = resolveTest(snap, opt);
    assert(res.workers == [ProcessorId(0, 0)]);
    assert(res[ProcessorId(0, 1)].reasons == Reject.osExcluded);
    opt.processors = [ProcessorId(0, 1)];
    res = resolveTest(snap, opt);
    assert(res.workers.length == 0);
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(res[ProcessorId(0, 1)].reasons == Reject.osExcluded,
        "an explicitly listed CPU outside the OS scope stays excluded");
    opt.processors = [ProcessorId(0, 0), ProcessorId(0, 1)];
    res = resolveTest(snap, opt);
    assert(res.workers == [ProcessorId(0, 0)]);
    assert(res.requests[0].accepted && res.requests[1].known && !res.requests[1].accepted);
}

shared int gDirTicks;

extern (D) bool directorTestBody(WorkerSelf*) @nogc nothrow
{
    atomicFetchAdd(gDirTicks, 1);
    return false;
}

unittest
{
    atomicStore(gDirTicks, 0);
    PoolOptions opt;
    opt.workerBody = &directorTestBody;
    opt.processors = [ProcessorId(0, 0)];
    auto pool = new CacheAwarePool(opt);
    assert(pool.resolution is null);
    auto preview = pool.resolve();
    pool.start();
    scope (exit) pool.shutdown();
    assert(pool.workerCount >= 1);
    assert(pool.resolution.started);
    assert(pool.resolution.sameSelection(preview));
    assert(pool.resolution[ProcessorId(0, 0)].start == StartState.pinned);

    auto d = pool.director();
    d.spin(8);
    bool second;
    try
        cast(void) pool.director();
    catch (Exception)
        second = true;
    assert(second, "second director() must fail while one is held");

    auto d2 = d;
    d2.wait();
    bool dead;
    try
        d.spin();
    catch (Exception)
        dead = true;
    assert(dead, "moved-from director must not apply policy");

    pool.tag(ProcessorId(0, 0), "drain");
    pool.tag(ProcessorId(0, 0), 3);
    d2.labeled("drain").spin(4);
    d2.numbered(3).wait();
    d2.classP.llc(0).signal();
    pool.wakeAll();

    foreach (_; 0 .. 200)
    {
        if (atomicLoad(gDirTicks) > 0)
            break;
        Thread.sleep(msecs(5));
    }
    assert(atomicLoad(gDirTicks) > 0, "worker body never ran");
}

shared int gLlcWakeTicks;
shared int gLlcWakeGroup = -1;

extern (D) bool llcWakeTestBody(WorkerSelf* w) @nogc nothrow
{
    atomicStore(gLlcWakeGroup, cast(int) w.llcIndex);
    atomicFetchAdd(gLlcWakeTicks, 1);
    return false;
}

unittest
{
    atomicStore(gLlcWakeTicks, 0);
    PoolOptions opt;
    opt.workerBody = &llcWakeTestBody;
    opt.processors = [ProcessorId(0, 0)];
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown();

    // Parked: the body ran and its final pre-park check, then nothing moved.
    int parked()
    {
        foreach (_; 0 .. 200)
        {
            immutable seen = atomicLoad(gLlcWakeTicks);
            Thread.sleep(msecs(20));
            if (seen >= 2 && atomicLoad(gLlcWakeTicks) == seen)
                return seen;
        }
        return -1;
    }

    immutable before = parked();
    if (before < 0)
    {
        pool.shutdown();
        assert(false, "worker never parked");
    }
    immutable group = cast(ushort) atomicLoad(gLlcWakeGroup);
    pool.wakeLlc(cast(ushort)(group + 1));
    Thread.sleep(msecs(20));
    immutable stayedParked = atomicLoad(gLlcWakeTicks) == before;
    pool.wakeLlc(group);
    foreach (_; 0 .. 200)
    {
        if (atomicLoad(gLlcWakeTicks) > before)
            break;
        Thread.sleep(msecs(5));
    }
    immutable woke = atomicLoad(gLlcWakeTicks) > before;
    // AssertError need not unwind the stack; stop the worker before asserting.
    pool.shutdown();
    assert(stayedParked, "wakeLlc woke a worker assigned to another group");
    assert(woke, "wakeLlc missed its group's worker");
}

shared int gManagedStarts;
shared int gManagedPumps;
shared int gManagedStops;
shared int gManagedPoolStopped;

void managedTestPoolStopped() nothrow
{
    assert(atomicLoad(gManagedStops) == atomicLoad(gManagedStarts));
    atomicFetchAdd(gManagedPoolStopped, 1);
}

void managedTestStart(WorkerSelf* w)
{
    auto state = new uint;
    *state = w.workerIndex;
    w.context = cast(void*) state;
    atomicFetchAdd(gManagedStarts, 1);
}

ManagedPumpResult managedTestPump(WorkerSelf* w)
{
    assert(w.context !is null);
    atomicFetchAdd(gManagedPumps, 1);
    throw new Exception("managed pump failure");
}

void managedTestStop(WorkerSelf* w)
{
    assert(w.context !is null);
    assert(*cast(uint*) w.context == w.workerIndex);
    atomicFetchAdd(gManagedStops, 1);
}

unittest
{
    atomicStore(gManagedStarts, 0);
    atomicStore(gManagedPumps, 0);
    atomicStore(gManagedStops, 0);
    atomicStore(gManagedPoolStopped, 0);
    PoolOptions opt;
    opt.processors = [ProcessorId(0, 0)];
    opt.managedWorker = ManagedWorkerHooks(
        &managedTestStart, &managedTestPump, &managedTestStop,
        &managedTestPoolStopped);
    auto pool = new CacheAwarePool(opt);
    pool.start();
    foreach (_; 0 .. 200)
    {
        if (atomicLoad(gManagedStops) != 0)
            break;
        Thread.sleep(msecs(5));
    }
    pool.shutdown();

    assert(atomicLoad(gManagedPoolStopped) == 1,
        "poolStopped runs once, after every worker has joined");
    assert(atomicLoad(gManagedStarts) == 1);
    assert(atomicLoad(gManagedPumps) == 1);
    assert(atomicLoad(gManagedStops) == 1,
        "managed stop must follow a successful start exactly once");
    auto failures = pool.workerFailures();
    assert(failures.length == 1);
    assert(failures[0].msg == "managed pump failure");
}
