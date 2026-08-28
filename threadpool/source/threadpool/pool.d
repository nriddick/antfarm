module threadpool.pool;

import core.atomic;
import core.thread;
import core.time : Duration, MonoTime, msecs;
import std.exception : enforce;

import threadpool.hybrid;
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

enum PinScope : ubyte
{
    logicalProcessor,
    l2Cluster,
    llc,
}

/// One logical processor in a Windows processor group. Linux `group` is 0.
struct ProcessorId
{
    ushort group;
    ushort lp;
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
    PinScope pinScope    = PinScope.logicalProcessor;
    uint pSpinIters      = 128;
    uint eSpinIters      = 16;
    /// Match `lpIndex` in any group. Ignored when `onlyProcessors` is set.
    const(ushort)[] onlyLps;
    /// Match `(group, lp)`. Wins over `onlyLps` when non-empty.
    const(ProcessorId)[] onlyProcessors;
    /// Called on the pinned worker. `true` = more work, `false` = idle.
    WorkerBody workerBody;
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

    this(PoolOptions options = PoolOptions.init)
    {
        this.options = options;
    }

    /// Thread-safe after the first completed `discover()`; the first call is setup.
    static const(TopologySnapshot) topology() @trusted
    {
        return discover();
    }

    /// Not thread-safe vs another `start` / `shutdown`. One live pool.
    /// Call after registry setup (`install` / `setLabel` / `installExchange`).
    void start()
    {
        version (ThreadpoolOs)
        {
            enforce(atomicLoad(gRunning) == 0, "threadpool: a pool is already running");
            enforce(options.pinScope == PinScope.logicalProcessor,
                "threadpool: v1 only implements PinScope.logicalProcessor");
            enforce(options.workerBody is null || options.managedWorker.pump is null,
                "threadpool: workerBody and managedWorker are mutually exclusive");
            enforce((options.managedWorker.start is null && options.managedWorker.stop is null)
                    || options.managedWorker.pump !is null,
                "threadpool: managedWorker start/stop require a pump");

            discover();
            snap = cachedSnapshot();
            enforce(snap !is null, "threadpool: topology not discovered");
            auto selected = selectLps(*snap, options);
            enforce(selected.length > 0, "threadpool: no logical processors selected");
            enforce(selected.length <= snap.logicalProcessorCount,
                "threadpool: worker count exceeds logical processors");

            ushort classCount = snap.classCount;
            if (classCount == 0) classCount = 1;

            resetStats();
            gWorkerBody = options.workerBody;
            gManagedWorkerHooks = options.managedWorker;
            failures = null;
            gWorkers = new WorkerRec[](selected.length);
            createWaitSlots(cast(uint) selected.length);
            resetWorkerHandshake();
            directorHeld = false;
            atomicStore(gRunning, 1);

            gLivePool = this;
            try
            {
                foreach (i, ref lp; selected)
                {
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
                    spec.cpuSetId = lp.cpuSetId;
                    spec.group = lp.group;
                    spec.lpIndex = lp.lpIndex;
                    spec.ecoQos = !isP && options.ecoQosOnE;
                    spec.applyQos = isP ? options.highQosOnP : options.ecoQosOnE;
                    if (isP && options.highQosOnP)
                        spec.ecoQos = false;
                    threads ~= spawnWorker(spec);
                }

                while (atomicLoad(gPinDone) < cast(int) threads.length)
                    Thread.sleep(msecs(1));

                if (atomicLoad(gPinOk) != cast(int) threads.length)
                    throw new Exception("threadpool: failed to pin one or more workers");
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

    /// Not thread-safe vs `start` or a second `shutdown` on another thread.
    void shutdown(bool drain = true)
    {
        version (ThreadpoolOs)
        {
            if (atomicLoad(gRunning) == 0)
                return;
            cast(void) drain;
            requestStop();
            joinAll();
            foreach (ref rec; gWorkers)
                if (rec.failure !is null)
                    failures ~= rec.failure;
            destroyWaitSlots();
            gWorkers = null;
            gWorkerBody = null;
            gManagedWorkerHooks = ManagedWorkerHooks.init;
            directorHeld = false;
            threads.length = 0;
            if (gLivePool is this)
                gLivePool = null;
            atomicStore(gRunning, 0);
        }
    }

    /// Not thread-safe vs `start`. See `shutdown`.
    void shutdownNow()
    {
        shutdown(false);
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

    void releaseDirector() @nogc nothrow
    {
        directorHeld = false;
    }

    @property ushort llcCount() const @nogc nothrow
    {
        auto s = cachedSnapshot();
        return s is null ? 0 : s.llcCount;
    }

    @property ushort workerCount() const @nogc nothrow
    {
        return cast(ushort) threads.length;
    }

    const(LlcDomain) llc(ushort llcIndex) const
    {
        auto s = cachedSnapshot();
        enforce(s !is null, "threadpool: topology not discovered");
        auto d = s.domain(llcIndex);
        enforce(d !is null, "threadpool: llcIndex out of range");
        return *d;
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
            gManagedWorkerHooks = ManagedWorkerHooks.init;
            directorHeld = false;
            threads.length = 0;
            gLivePool = null;
            atomicStore(gRunning, 0);
        }
    }
}

private LogicalProcessor[] selectLps(ref const(TopologySnapshot) snap, ref const PoolOptions opt)
{
    LogicalProcessor[] selected;
    foreach (ref lp; snap.processors)
    {
        if (opt.onlyProcessors.length)
        {
            bool wanted;
            foreach (v; opt.onlyProcessors)
                if (v.group == lp.group && v.lp == lp.lpIndex) { wanted = true; break; }
            if (!wanted) continue;
        }
        else if (opt.onlyLps.length)
        {
            bool wanted;
            foreach (v; opt.onlyLps)
                if (v == lp.lpIndex) { wanted = true; break; }
            if (!wanted) continue;
        }
        auto tc = tableClassIndex(lp.efficiencyClass, snap.maxEfficiencyClass, snap.classCount);
        if (snap.classCount <= 1)
        {
            if (!opt.enablePCores) continue;
        }
        else
        {
            if (tc == classP && !opt.enablePCores) continue;
            if (tc == classE && !opt.enableECores) continue;
        }
        if (opt.skipSmtSiblings && lp.smtSibling) continue;
        version (linux)
        {
            if (lp.parkedAtDiscovery) continue;
        }
        selected ~= lp;
    }
    return selected;
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
    auto got = selectLps(snap, all);
    assert(got.length == 3);

    PoolOptions byLp;
    byLp.onlyLps = [ushort(0)];
    got = selectLps(snap, byLp);
    assert(got.length == 2);
    assert(got[0].group == 0 && got[0].lpIndex == 0);
    assert(got[1].group == 1 && got[1].lpIndex == 0);

    PoolOptions byId;
    byId.onlyProcessors = [ProcessorId(1, 0)];
    got = selectLps(snap, byId);
    assert(got.length == 1);
    assert(got[0].group == 1 && got[0].lpIndex == 0);

    PoolOptions both;
    both.onlyLps = [ushort(0)];
    both.onlyProcessors = [ProcessorId(1, 1)];
    got = selectLps(snap, both);
    assert(got.length == 1);
    assert(got[0].group == 1 && got[0].lpIndex == 1);
}

unittest
{
    TopologySnapshot snap;
    snap.classCount = 1;
    LogicalProcessor live, parked;
    live.lpIndex = 0;
    parked.lpIndex = 1;
    parked.parkedAtDiscovery = true;
    snap.processors = [live, parked];

    PoolOptions opt;
    auto got = selectLps(snap, opt);
    version (linux)
    {
        assert(got.length == 1);
        assert(got[0].lpIndex == 0);
    }
    else
    {
        assert(got.length == 2);
    }
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
    opt.onlyLps = [ushort(0)];
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown(true);
    assert(pool.workerCount >= 1);

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

shared int gManagedStarts;
shared int gManagedPumps;
shared int gManagedStops;

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
    PoolOptions opt;
    opt.onlyLps = [ushort(0)];
    opt.managedWorker = ManagedWorkerHooks(
        &managedTestStart, &managedTestPump, &managedTestStop);
    auto pool = new CacheAwarePool(opt);
    pool.start();
    foreach (_; 0 .. 200)
    {
        if (atomicLoad(gManagedStops) != 0)
            break;
        Thread.sleep(msecs(5));
    }
    pool.shutdown();

    assert(atomicLoad(gManagedStarts) == 1);
    assert(atomicLoad(gManagedPumps) == 1);
    assert(atomicLoad(gManagedStops) == 1,
        "managed stop must follow a successful start exactly once");
    auto failures = pool.workerFailures();
    assert(failures.length == 1);
    assert(failures[0].msg == "managed pump failure");
}
