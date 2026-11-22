module threadpool.pool;

import core.atomic;
import core.lifetime : move;
import core.thread;
import core.time : Duration, MonoTime, msecs;
import std.conv : to;
import std.exception : enforce;

import threadpool.hybrid;
import threadpool.pin : PinOutcome, WorkerPlacement, WorkerQos;
import threadpool.resolution;
import threadpool.stats;
import threadpool.timing : TimingStats, WorkerTiming, gTiming, microsToTicks;
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

/// `PoolOptions.workers` and `FarmHostOptions.workers` sentinel: the
/// automatic count of every set. A set of one or two CPUs is fully staffed;
/// a larger one leaves one CPU in eight, and at least one, without a worker
/// (`autoWorkersOf`).
enum uint autoWorkers = uint.max;

struct PoolOptions
{
    bool skipSmtSiblings = false;
    bool enablePCores    = true;
    bool enableECores    = true;
    /// What the OS is told about where a worker may run.
    WorkerPlacement placement = WorkerPlacement.automatic;
    /// Workers in total. `autoWorkers` gives every set its automatic count.
    /// Any other number is taken exactly, filling the sets in group order,
    /// and may leave a set no CPU without a worker; `start` throws when the
    /// accepted CPUs are fewer. A host has its own count
    /// (`FarmHostOptions.workers`) and requires this one left alone.
    uint workers = autoWorkers;
    /// Thread QoS of every worker.
    WorkerQos qos        = WorkerQos.high;
    /// PAUSE iterations of one `spin` round.
    uint spinIters       = 128;
    /// Exact CPUs to consider, group-qualified. Empty = no CPU-list filter.
    /// Build with `parseCpuList("0-7,16-23")` or topology helpers. Duplicates
    /// collapse to one CPU; `resolve().requests` reports them.
    const(ProcessorId)[] processors;
    /// Called on the worker. `true` = more work, `false` = idle.
    WorkerBody workerBody;
    /// Optional; called once on each worker after its `workerBody`
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
    private bool filterLabel;
    private string wantLabel;
    private bool filterNumber;
    private long wantNumber;
    private size_t skipCount;
    private size_t takeCount = size_t.max;

    Selection llc(ushort i)
    {
        auto s = this;
        s.filterLlc = true;
        s.llcIndex = i;
        return s;
    }

    Selection labeled(string name)
    {
        auto s = this;
        s.filterLabel = true;
        s.wantLabel = name;
        return s;
    }

    Selection numbered(long n)
    {
        auto s = this;
        s.filterNumber = true;
        s.wantNumber = n;
        return s;
    }

    /// At most `n` of the workers the other filters match, in worker order.
    /// `skip` applies before `take` whatever the order they are written in.
    Selection take(size_t n)
    {
        auto s = this;
        s.takeCount = n;
        return s;
    }

    /// All but the first `n` of the workers the other filters match.
    Selection skip(size_t n)
    {
        auto s = this;
        s.skipCount = n;
        return s;
    }

    /// Label the selected workers; `labeled(name)` then selects them.
    void label(string name)
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r) { r.label = name; })(this);
    }

    /// Number the selected workers; `numbered(n)` then selects them.
    void number(long n)
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r) { r.hasNumber = true; r.number = n; })(this);
    }

    /// Workers this selection matches now.
    size_t count()
    {
        enforceOwner();
        size_t n;
        forEachSelected!((size_t i, ref WorkerRec r) { n++; })(this);
        return n;
    }

    void spin(uint iters = 0)
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            immutable n = iters != 0 ? iters : pool.options.spinIters;
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

    void signal()
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            wakeWorker(cast(uint) i);
        })(this);
    }

    /// A burst of publication is expected at `atNanos`: `MonoTime` in
    /// nanoseconds (`monoNanos`, the clock `ant_uv`'s `UvLoop.nanos` reads).
    ///
    /// Each selected worker that is idle parks precisely until the lead
    /// before the instant, polls without parking until the hold after it
    /// (`readiness`), and then goes back to its policy (`wait`, `sleep`).
    /// Work that is available is never delayed: a wake ends the park, and a
    /// polling worker sees work within one round. A worker under `spin`
    /// polls all the time and ignores this.
    ///
    /// The pool keeps no period and no beat: the caller says when each burst
    /// is due, and a caller that stops calling leaves the workers on their
    /// policy. A worker finishes the window it is in, so the next instant
    /// may be given before or after the burst of this one. Giving it while
    /// the workers are still busy with the burst or polling costs one store
    /// and one exchange for each; a worker that has already parked is woken
    /// to take the new instant up.
    ///
    /// A polling worker costs processor time for the length of the window,
    /// so select the workers that should be ready (`take`), not all of them.
    void expect(long atNanos)
    {
        enforceOwner();
        immutable at = scaleClock(atNanos, 1_000_000_000L, MonoTime.ticksPerSecond);
        immutable now = MonoTime.currTime.ticks;
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            immutable old = atomicExchange(&r.expectTicks, at);
            // A worker parked to the old instant's lead wakes in time for
            // the new one only when the old is still ahead and no later.
            if (old <= now || at < old)
                wakeWorker(cast(uint) i);
        })(this);
    }

    /// Withdraw the instant given to `expect`. A worker in a window
    /// finishes it; one parked to the lead wakes there and finds nothing.
    void expectNothing()
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore(r.expectTicks, expectNone);
        })(this);
    }

    /// How long before an expected instant the selected workers start to
    /// poll, and how long after it they go on polling when nothing has
    /// come. Both in microseconds.
    ///
    /// The lead has to cover how late the park brings a worker back, which
    /// `timing().parkOvershoot` reports; the hold, how late the burst is
    /// published, which `timing().burstArrival` reports.
    void readiness(uint leadMicros = defaultLeadMicros, uint holdMicros = defaultHoldMicros)
    {
        enforceOwner();
        immutable lead = microsToTicks(leadMicros);
        immutable hold = microsToTicks(holdMicros);
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicStore(r.leadTicks, lead);
            atomicStore(r.holdTicks, hold);
            wakeWorker(cast(uint) i);
        })(this);
    }

    /// What the selected workers have recorded about their timing, summed.
    /// Read while they run: each count is whole, and the counts of one
    /// worker may be a few events apart.
    TimingStats timing()
    {
        enforceOwner();
        TimingStats sum;
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            if (i < gTiming.length) gTiming[i].addTo(sum);
        })(this);
        return sum;
    }

    /// Start the selected workers' timing records again from zero. Each
    /// worker clears its own record the next time it writes to it, so a
    /// `timing()` read straight after may still show the old counts.
    void resetTiming()
    {
        enforceOwner();
        forEachSelected!((size_t i, ref WorkerRec r)
        {
            atomicFetchAdd(r.timingEpoch, 1);
        })(this);
    }

private:
    void enforceOwner()
    {
        enforce(pool !is null && pool.directorHeld, "threadpool: director selection is dead");
    }

    bool matches(ref const WorkerRec r) const
    {
        if (filterLlc && r.llcIndex != llcIndex) return false;
        if (filterLabel && r.label != wantLabel) return false;
        if (filterNumber && (!r.hasNumber || r.number != wantNumber)) return false;
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

}

pragma(inline, true)
private void forEachSelected(alias action)(ref Selection sel)
{
    size_t seen;
    foreach (i, ref r; gWorkers)
    {
        if (!sel.matches(r)) continue;
        immutable k = seen++;
        if (k < sel.skipCount) continue;
        if (k - sel.skipCount >= sel.takeCount) break;
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
    Selection labeled(string name) { return all.labeled(name); }
    Selection numbered(long n) { return all.numbered(n); }
    Selection take(size_t n) { return all.take(n); }
    Selection skip(size_t n) { return all.skip(n); }

    void spin(uint iters = 0) { all.spin(iters); }
    void sleep(Duration d) { all.sleep(d); }
    void wait() { all.wait(); }
    void signal() { all.signal(); }
    void expect(long atNanos) { all.expect(atNanos); }
    void expectNothing() { all.expectNothing(); }
    void readiness(uint leadMicros = defaultLeadMicros, uint holdMicros = defaultHoldMicros)
    {
        all.readiness(leadMicros, holdMicros);
    }
    TimingStats timing() { return all.timing(); }
    void resetTiming() { all.resetTiming(); }
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
        return resolvePool(liveTopology(), options);
    }

    /// After `start`: the resolution including each worker's `StartState` and
    /// placement outcome. After a failed start or a shutdown: the last attempt's
    /// resolution. Null before the first `start`.
    @property const(PoolResolution) resolution() const
    {
        return lastResolution;
    }

    /// Not thread-safe vs another `start` / `shutdown`. One live pool.
    /// Call after registry setup (`install` / `setLabel` / `installExchange`).
    void start()
    {
        auto planned = resolve();
        enforce(options.workers == autoWorkers || planned.workerCount == options.workers,
            "threadpool: " ~ options.workers.to!string ~ " workers requested but the accepted CPUs take "
            ~ planned.workerCount.to!string ~ "; " ~ planned.summary());
        start(planned);
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
            immutable count = res.workers.length;
            enforce(count > 0,
                "threadpool: no logical processors selected; " ~ res.summary());
            // One affinity mask names one Windows processor group, so a set
            // that spans two cannot be masked. Say so before any thread starts.
            version (Windows)
                if (res.placement == WorkerPlacement.group)
                    foreach (ref set; res.sets)
                        foreach (id; set.processors)
                            enforce(id.group == set.processors[0].group && id.lp < 64,
                                "threadpool: the set of physical group " ~ set.llcIndex.to!string
                                ~ " spans Windows processor groups and cannot be masked;"
                                ~ " use WorkerPlacement.free or pin; " ~ res.summary());

            resetStats();
            gWorkerBody = options.workerBody;
            gWorkerStop = options.workerStop;
            gManagedWorkerHooks = options.managedWorker;
            failures = null;
            gWorkers = new WorkerRec[](count);
            gTiming = new WorkerTiming[](count);
            gPinOutcome = new PinOutcome[](count);
            createWaitSlots(cast(uint) count);
            gArmedBits = new shared(ulong)[(count + 63) / 64];
            resetWorkerHandshake();
            directorHeld = false;
            atomicStore(gRunning, 1);

            // What an unconfined worker may run on: the frozen OS scope.
            ProcessorId[] scopeProcessors;
            foreach (ref p; snap.processors)
                if (snap.isEligible(p)) scopeProcessors ~= ProcessorId.of(p);

            gLivePool = this;
            try
            {
                foreach (i, ref w; res.workers)
                {
                    auto set = res.setOf(w.llcIndex);
                    enforce(set !is null && set.processors.length != 0, "threadpool: worker without a set");
                    auto first = snap.byId(set.processors[0]);
                    enforce(first !is null, "threadpool: a set names an unknown processor");
                    WorkerSelf self;
                    self.llcIndex = w.llcIndex;
                    self.numaIndex = first.numaIndex;
                    self.workerIndex = cast(uint) i;
                    self.spinIters = options.spinIters;
                    bindNumaNeighborhood(self, *snap);

                    import threadpool.failure : FailureCapture;
                    gWorkers[i].failure = new FailureCapture;
                    gWorkers[i].llcIndex = w.llcIndex;
                    atomicStore(gWorkers[i].kind, IdleKind.wait);
                    atomicStore(gWorkers[i].spinIters, self.spinIters);
                    atomicStore(gWorkers[i].leadTicks, microsToTicks(defaultLeadMicros));
                    atomicStore(gWorkers[i].holdTicks, microsToTicks(defaultHoldMicros));

                    SpawnSpec spec;
                    spec.self = self;
                    spec.placement = res.placement;
                    spec.processors = set.processors;
                    spec.scopeProcessors = scopeProcessors;
                    spec.processor = w.pinned ? snap.byId(w.processor) : null;
                    spec.qos = options.qos;
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
                    throw new Exception("threadpool: failed to place worker(s) "
                        ~ lastResolution.placeFailures() ~ "; " ~ lastResolution.summary());
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
    /// before this call. A pool that is not the live one (never started,
    /// rejected by `start`, already shut down) returns at once. Not
    /// thread-safe vs `start` or a second `shutdown` on another thread.
    void shutdown()
    {
        version (ThreadpoolOs)
        {
            // The worker records, hooks and run flag are process-wide and
            // belong to the live pool; only it may stop and clear them.
            if (atomicLoad(gRunning) == 0 || gLivePool !is this)
                return;
            requestStop();
            joinAll();
            foreach (ref rec; gWorkers)
                if (rec.failure !is null && rec.failure.value !is null)
                    failures ~= rec.failure.value;
            clearWorkerState();
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
            gTiming = null;
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
    assert(got.processorsIn(0) == [ProcessorId(0, 0), ProcessorId(1, 0), ProcessorId(1, 1)]);
    assert(got.workerCount == 2, "three CPUs: one is left unstaffed");

    PoolOptions byId;
    byId.processors = [ProcessorId(1, 0)];
    got = resolveTest(snap, byId);
    assert(got.processorsIn(0) == [ProcessorId(1, 0)] && got.workerCount == 1);
    assert(got[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(got[ProcessorId(1, 1)].reasons == Reject.notRequested);
    assert(got.requests.length == 1 && got.requests[0].accepted);

    // An explicit count is taken exactly, and may staff every CPU.
    PoolOptions exact;
    exact.workers = 3;
    got = resolveTest(snap, exact);
    assert(got.workerCount == 3 && got.sets[0].unstaffed == 0);
    exact.workers = 1;
    got = resolveTest(snap, exact);
    assert(got.workerCount == 1 && got.processorsIn(0).length == 3, "fewer workers, the same set");
    // More than the accepted CPUs: the preview shows what fits, start refuses.
    exact.workers = 4;
    got = resolveTest(snap, exact);
    assert(got.workerCount == 3);
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
    assert(got.processorsIn(0) == [ProcessorId(0, 0), ProcessorId(0, 1)] && got.workerCount == 2,
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
    assert(res.processorsIn(0) == [ProcessorId(0, 0)] && res.workerCount == 1);
    assert(res[ProcessorId(0, 1)].reasons == Reject.osExcluded);
    opt.processors = [ProcessorId(0, 1)];
    res = resolveTest(snap, opt);
    assert(res.workers.length == 0 && res.sets.length == 0);
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(res[ProcessorId(0, 1)].reasons == Reject.osExcluded,
        "an explicitly listed CPU outside the OS scope stays excluded");
    opt.processors = [ProcessorId(0, 0), ProcessorId(0, 1)];
    res = resolveTest(snap, opt);
    assert(res.processorsIn(0) == [ProcessorId(0, 0)] && res.workerCount == 1);
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
    assert(pool.resolution.workers[0].start == StartState.placed);

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

    d2.all.label("drain");
    d2.all.number(3);
    assert(d2.labeled("drain").count == 1 && d2.numbered(4).count == 0);
    assert(d2.take(0).count == 0 && d2.skip(1).count == 0 && d2.skip(0).take(5).count == 1);
    d2.labeled("drain").spin(4);
    d2.numbered(3).wait();
    d2.llc(0).signal();
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
        rec.label = "selected";
        rec.hasNumber = true;
        rec.number = 7;
    }
    gWorkers[1].llcIndex = 2;
    gWorkers[2].llcIndex = 1;
    gWorkers[3].label = "other";
    gWorkers[4].number = 8;
    gWorkers[5].hasNumber = false;
    auto pool = new CacheAwarePool(PoolOptions.init);
    pool.directorHeld = true;
    Selection all;
    all.pool = pool;
    auto selected = all.llc(0).labeled("selected").numbered(7);
    auto rec = &gWorkers[0];
    selected.spin(11);
    assert(atomicLoad(rec.spinIters) == 11 && atomicLoad(rec.kind) == IdleKind.spin);
    selected.sleep(msecs(-1));
    assert(atomicLoad(rec.sleepNs) == 0);
    selected.sleep(msecs(2));
    assert(atomicLoad(rec.sleepNs) == 2_000_000 && atomicLoad(rec.kind) == IdleKind.sleep);
    selected.wait();
    assert(atomicLoad(rec.kind) == IdleKind.wait);
    // spin, sleep, sleep and wait each bump the epoch.
    enum expectedEpoch = 4;

    // expect stores the instant in ticks; readiness, the lead and the hold.
    assert(atomicLoad(rec.expectTicks) == expectNone);
    immutable soon = monoNanos() + 5_000_000_000;
    selected.expect(soon);
    assert(atomicLoad(rec.expectTicks) == scaleClock(soon, 1_000_000_000L, MonoTime.ticksPerSecond));
    assert(atomicLoad(gWorkers[1].expectTicks) == expectNone, "only the selected workers");
    selected.readiness(700, 90);
    assert(atomicLoad(rec.leadTicks) == microsToTicks(700) && atomicLoad(rec.holdTicks) == microsToTicks(90));
    selected.expectNothing();
    assert(atomicLoad(rec.expectTicks) == expectNone);
    // timing sums the selection's records, and reset asks each worker to clear.
    auto savedTiming = gTiming;
    scope (exit) gTiming = savedTiming;
    gTiming = new WorkerTiming[](gWorkers.length);
    atomicStore(gTiming[0].windows, 3UL);
    atomicStore(gTiming[1].windows, 5UL);
    assert(selected.timing().windows == 3 && selected.timing().workers == 1);
    assert(all.timing().windows == 8 && all.timing().workers == 6);
    selected.resetTiming();
    assert(atomicLoad(rec.timingEpoch) == 1 && atomicLoad(gWorkers[1].timingEpoch) == 0);
    assert(atomicLoad(rec.epoch) == expectedEpoch);
    selected.signal();
    assert(atomicLoad(rec.epoch) == expectedEpoch);
    foreach (ref excluded; gWorkers[1 .. $])
        assert(atomicLoad(excluded.epoch) == 0 && atomicLoad(excluded.kind) == IdleKind.wait);
    all.llc(2).spin();
    assert(atomicLoad(gWorkers[1].spinIters) == pool.options.spinIters);
    assert(atomicLoad(gWorkers[1].epoch) == 1);
    assert(!all.llc(1).llc(0).labeled("other").matches(*rec));

    // Spans past long.max / ticksPerSecond nanoseconds (~9.2 s on POSIX) wrapped.
    import core.time : hours, seconds;
    assert(Selection.durationToTicks(hours(1)) == 3600 * MonoTime.ticksPerSecond);
    assert(Selection.durationToTicks(-seconds(10)) == -10 * MonoTime.ticksPerSecond);
    selected.sleep(seconds(30));
    assert(atomicLoad(rec.sleepNs) == 30_000_000_000UL);
}
