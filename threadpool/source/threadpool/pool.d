module threadpool.pool;

import core.atomic;
import core.lifetime : move;
import core.thread;
import core.time : Duration, MonoTime, msecs;
import std.exception : enforce;

import threadpool.hybrid;
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
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            immutable n = iters != 0 ? iters : (r.isP ? pool.options.pSpinIters : pool.options.eSpinIters);
            atomicStore(r.spinIters, n);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.spin);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
    }

    void sleep(Duration d)
    {
        enforceOwner();
        immutable ns = scaleClock(d.total!"hnsecs", 10_000_000L, 1_000_000_000L);
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore(r.sleepNs, ns > 0 ? cast(ulong) ns : 0);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.sleep);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
    }

    void sleepUntil(MonoTime t)
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore(r.sleepUntilTicks, t.ticks);
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.sleepUntil);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
    }

    void wait()
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore!(MemoryOrder.rel)(r.kind, IdleKind.wait);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
    }

    // The cadence, nudge and drift verbs have no in-tree user; they stay
    // behind `version (ThreadpoolCadence)` with their tests until one appears
    // (cleanup plan 2.0, step 2).
    version (ThreadpoolCadence)
    {
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
        immutable periodTicks = Selection.durationToTicks(period);
        enforce(periodTicks > 0, "threadpool: cadence period is shorter than one tick");
        immutable leadTicks = opt.spinLead > Duration.zero
            ? Selection.durationToTicks(opt.spinLead) : 0;
        immutable seq = ++pool.driftSeq;
        forEachSelected!((size_t i, ref WorkerRec r)
        {
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
        })(this);
    }

    } // version (ThreadpoolCadence)

    void signal()
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            wakeWorker(cast(uint) i);
        })(this);
    }

    version (ThreadpoolCadence)
    {
    /// One-shot phase step. Positive delays beats; negative pulls them sooner.
    void nudge(Duration phase)
    {
        enforceOwner();
        immutable ticks = durationToTicks(phase);
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicFetchAdd(r.cadenceEpochTicks, ticks);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
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
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore(r.cadenceDriftTicks, driftTicks);
            atomicStore(r.cadenceDriftUntilTicks, untilTicks);
            atomicStore!(MemoryOrder.rel)(r.cadenceDriftSeq, seq);
            bump(r);
            wakeWorker(cast(uint) i);
        })(this);
    }

    } // version (ThreadpoolCadence)

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
        // Duration counts hnsecs; total!"nsecs" would itself wrap past ~29 years.
        return scaleClock(d.total!"hnsecs", 10_000_000L, MonoTime.ticksPerSecond);
    }

    static long absTicks(long t) @nogc nothrow
    {
        return t >= 0 ? t : -t;
    }
}

pragma(inline, true)
private void forEachSelected(alias action)(ref Selection sel)
{
    foreach (i, ref r; gWorkers)
    {
        if (!sel.matches(r)) continue;
        action(i, r);
    }
}

/// Single-owner idle-policy handle, move-only like Ant Farm's producer
/// `Token` and actor owners: copying and lvalue assignment do not compile.
/// Transfer with `move` (std.algorithm or core.lifetime); lending should
/// take the handle by `ref`. `opAssign` accepts only an rvalue and
/// overwrites are releasing. The source of a `move` is emptied.
struct Director
{
    private CacheAwarePool pool;

    @disable this();
    @disable this(this);
    @disable this(ref return scope Director);

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
    version (ThreadpoolCadence)
    {
    void cadence(Duration period) { all.cadence(period); }
    void cadence(Duration period, Cadence opt) { all.cadence(period, opt); }
    void cadence(MonoTime epoch, Duration period, Cadence opt = Cadence.init)
    {
        all.cadence(epoch, period, opt);
    }
    void nudge(Duration phase) { all.nudge(phase); }
    void drift(Duration perBeat, Duration until) { all.drift(perBeat, until); }
    }

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
        return resolvePool(snap, options);
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
        start(resolve());
    }

    /// Start the workers of `planned`, a resolution against the discovered
    /// snapshot (FarmHost passes its narrowed selection, so
    /// `pool.resolution` reports its `notSelected` CPUs).
    package(threadpool) void start(PoolResolution planned)
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

            auto res = planned;
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
            gArmedBits = new shared(ulong)[(selected.length + 63) / 64];
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

                    import threadpool.failure : FailureCapture;
                    gWorkers[i].failure = new FailureCapture;
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

                version (unittest)
                    if (gInjectStartError)
                    {
                        gInjectStartError = false;
                        throw new Error("threadpool: injected start Error");
                    }
                lastResolution = res.withStart(gPinOutcome);
                if (atomicLoad(gPinOk) != cast(int) threads.length)
                    throw new Exception("threadpool: failed to pin worker(s) "
                        ~ lastResolution.pinFailures() ~ "; " ~ lastResolution.summary());
                releaseWorkers();
            }
            catch (Throwable e)
            {
                // Errors too (OutOfMemoryError from FailureCapture, spawn
                // or withStart): spawned workers must be stopped and joined,
                // or the pool stays started and process exit hangs. A
                // failure while aborting is chained, not lost.
                try
                    abortStart();
                catch (Throwable t)
                    e = Throwable.chainTogether(e, t);
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
                if (rec.failure !is null && rec.failure.value !is null)
                    failures ~= rec.failure.value;
            clearWorkerState();
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

    /// Wake one armed or parked worker, if any (does not change policy).
    /// Safe for producer threads: a producer that made new work visible
    /// calls this instead of `wakeAll` when one worker is enough, and a woken
    /// worker that finds more work wakes the next (a chain wake). Returns
    /// whether a worker was woken.
    bool wakeOne() @nogc nothrow
    {
        return wakeOneArmed();
    }

    /// True when some worker is armed or parked; cheap enough for a hot
    /// path's "is anyone asleep" check before `wakeOne`.
    bool anyWorkerArmed() @nogc nothrow
    {
        return anyArmed();
    }

    /// Wake every worker (does not change policy). Safe for producer threads.
    /// Loads the worker slice once so a concurrent shutdown cannot tear
    /// the length away from the pointer between the bound and the index.
    void wakeAll() @nogc nothrow
    {
        version (ThreadpoolOs)
        {
            auto workers = gWorkers;
            foreach (i; 0 .. workers.length)
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

    /// True once a worker failure (or `shutdown`) requested stop on this
    /// running pool: one acquire load. False for a pool that is not the live
    /// pool: never started, already shut down, or rejected by `start`.
    @property bool stopRequested() const @nogc nothrow
    {
        version (ThreadpoolOs)
            return gLivePool is this && atomicLoad!(MemoryOrder.acq)(gRunFlag) == 0;
        else
            return false;
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

    /// Owned `WorkerFailure!Error` / `WorkerFailure!Exception` diagnostics from
    /// worker failures. Call after `shutdown`. Message/file are bounded copies;
    /// original subclasses, chains and stack traces are not retained.
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

    void clearWorkerState()
    {
        version (ThreadpoolOs)
        {
            destroyWaitSlots();
            gArmedBits = null;
            gWorkers = null;
            gWorkerBody = null;
            gWorkerStop = null;
            if (gManagedWorkerHooks.poolStopped !is null)
                gManagedWorkerHooks.poolStopped();
            gManagedWorkerHooks = ManagedWorkerHooks.init;
            directorHeld = false;
            threads.length = 0;
        }
    }

    void abortStart()
    {
        version (ThreadpoolOs)
        {
            requestStop();
            joinAll();
            clearWorkerState();
            gLivePool = null;
            atomicStore(gRunning, 0);
        }
    }
}

version (unittest)
private PoolResolution resolveTest(ref const TopologySnapshot snap, PoolOptions opt)
{
    return resolvePool(snap, opt);
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
version (unittest) __gshared bool gInjectStartError;

unittest
{
    // An Error after workers are spawned still stops and joins them; the
    // pool is not left started and a new pool can start.
    PoolOptions opt;
    opt.processors = [ProcessorId(0, 0)];
    auto pool = new CacheAwarePool(opt);
    gInjectStartError = true;
    // Disarm even if start fails before the injection point.
    scope (exit) gInjectStartError = false;
    bool caught;
    try
        pool.start();
    catch (Error e)
        caught = e.msg == "threadpool: injected start Error";
    assert(caught);
    assert(atomicLoad(gRunning) == 0 && gLivePool is null && !pool.stopRequested);
    auto again = new CacheAwarePool(opt);
    again.start();
    again.shutdown();
}

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
    // The single-owner handle is move-only: a copy (explicit, by-value
    // parameter, or through a range algorithm) must not compile; an
    // explicit move transfers and empties the source.
    static assert(!__traits(compiles, { auto copy = d; }));
    static assert(!__traits(compiles,
    {
        void takesByValue(Director taken) {}
        takesByValue(d);
    }));
    static assert(__traits(compiles, { auto moved = move(d); }));
    d.spin(8);
    bool second;
    try
        cast(void) pool.director();
    catch (Exception)
        second = true;
    assert(second, "second director() must fail while one is held");

    auto d2 = move(d);
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

ManagedPumpResult managedTlsFailure(WorkerSelf*)
{
    import core.exception : onAssertErrorMsg;
    onAssertErrorMsg("pump.d", 31, "managed TLS failure");
    return ManagedPumpResult.idle();
}

void managedTlsStop(WorkerSelf*)
{
    import core.exception : onAssertErrorMsg;
    // Overwrites the same TLS Error before this worker even exits.
    onAssertErrorMsg("stop.d", 47, "later stop failure");
}

unittest
{
    import threadpool.failure : WorkerFailure;
    PoolOptions opt;
    opt.processors = [ProcessorId(0, 0)];
    opt.managedWorker = ManagedWorkerHooks(null, &managedTlsFailure, &managedTlsStop, null);
    auto pool = new CacheAwarePool(opt);
    pool.start();
    foreach (_; 0 .. 200)
    {
        if (pool.stopRequested) break;
        Thread.sleep(msecs(5));
    }
    pool.shutdown();
    auto failures = pool.workerFailures();
    assert(failures.length == 1);
    auto failure = cast(WorkerFailure!Error) failures[0];
    assert(failure !is null && failure.msg == "managed TLS failure");
    assert(failure.file == "pump.d" && failure.line == 31);
    assert(failure.sourceType == "core.exception.AssertError");
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

unittest
{
    auto saved = gWorkers;
    scope (exit) gWorkers = saved;
    gWorkers = new WorkerRec[6];
    foreach (ref rec; gWorkers)
    {
        rec.isP = true;
        rec.label = "selected";
        rec.hasNumber = true;
        rec.number = 7;
    }
    gWorkers[1].isP = false;
    gWorkers[2].llcIndex = 1;
    gWorkers[3].label = "other";
    gWorkers[4].number = 8;
    gWorkers[5].hasNumber = false;
    auto pool = new CacheAwarePool(PoolOptions.init);
    pool.directorHeld = true;
    Selection all;
    all.pool = pool;
    auto selected = all.llc(0).classP.labeled("selected").numbered(7);
    auto rec = &gWorkers[0];
    selected.spin(11);
    assert(atomicLoad(rec.spinIters) == 11 && atomicLoad(rec.kind) == IdleKind.spin);
    selected.sleep(msecs(-1));
    assert(atomicLoad(rec.sleepNs) == 0);
    selected.sleep(msecs(2));
    assert(atomicLoad(rec.sleepNs) == 2_000_000 && atomicLoad(rec.kind) == IdleKind.sleep);
    auto epoch = MonoTime.currTime;
    selected.sleepUntil(epoch);
    assert(atomicLoad(rec.sleepUntilTicks) == epoch.ticks
        && atomicLoad(rec.kind) == IdleKind.sleepUntil);
    selected.wait();
    assert(atomicLoad(rec.kind) == IdleKind.wait);
    version (ThreadpoolCadence)
    {
    selected.cadence(epoch, msecs(10), Cadence(true, msecs(1)));
    assert(atomicLoad(rec.kind) == IdleKind.cadence);
    assert(atomicLoad(rec.cadenceEpochTicks) == epoch.ticks);
    assert(atomicLoad(rec.cadencePeriodTicks) == Selection.durationToTicks(msecs(10)));
    assert(atomicLoad(rec.cadenceSkip) == 1);
    assert(atomicLoad(rec.cadenceSpinLeadTicks) == Selection.durationToTicks(msecs(1)));
    selected.nudge(msecs(-1));
    assert(atomicLoad(rec.cadenceEpochTicks) == epoch.ticks - Selection.durationToTicks(msecs(1)));
    selected.drift(msecs(2), msecs(4));
    assert(atomicLoad(rec.cadenceDriftTicks) == Selection.durationToTicks(msecs(2)));
    assert(atomicLoad(rec.cadenceDriftUntilTicks) == Selection.durationToTicks(msecs(4)));
    assert(atomicLoad(rec.cadenceDriftSeq) == pool.driftSeq);
    }
    // spin, sleep, sleep, sleepUntil, wait bump the epoch; cadence, nudge
    // and drift add three more when they are compiled in.
    version (ThreadpoolCadence) enum expectedEpoch = 8;
    else enum expectedEpoch = 5;
    assert(atomicLoad(rec.epoch) == expectedEpoch);
    selected.signal();
    assert(atomicLoad(rec.epoch) == expectedEpoch);
    foreach (ref excluded; gWorkers[1 .. $])
        assert(atomicLoad(excluded.epoch) == 0 && atomicLoad(excluded.kind) == IdleKind.wait);
    all.classE.spin();
    assert(atomicLoad(gWorkers[1].spinIters) == pool.options.eSpinIters);
    assert(atomicLoad(gWorkers[1].epoch) == 1);
    assert(!all.classP.classE.matches(*rec));

    // Spans past long.max / ticksPerSecond nanoseconds (~9.2 s on POSIX) wrapped.
    import core.time : hours, seconds;
    version (ThreadpoolCadence)
    {
    selected.cadence(epoch, seconds(30), Cadence(false, seconds(12)));
    assert(atomicLoad(rec.cadencePeriodTicks) == 30 * MonoTime.ticksPerSecond);
    assert(atomicLoad(rec.cadenceSpinLeadTicks) == 12 * MonoTime.ticksPerSecond);
    }
    assert(Selection.durationToTicks(hours(1)) == 3600 * MonoTime.ticksPerSecond);
    assert(Selection.durationToTicks(-seconds(10)) == -10 * MonoTime.ticksPerSecond);
    selected.sleep(seconds(30));
    assert(atomicLoad(rec.sleepNs) == 30_000_000_000UL);
}
