module threadpool.hybrid;

import core.atomic;
import core.time : MonoTime;
import core.thread : Thread;
import threadpool.stats;
import threadpool.topology : TopologySnapshot, fillNumaLlcs, maxNumaLlcs;

version (Windows)
    version = ThreadpoolOs;
version (linux)
    version = ThreadpoolOs;

version (Windows)
{
    import threadpool.sys.win_wait : parkWorker;
    enum uint kWaitForever = 0xFFFF_FFFF;
}
else version (linux)
{
    import threadpool.sys.linux_wait : parkWorker;
    enum uint kWaitForever = uint.max;
}

version (LDC)
{
    import ldc.attributes : optStrategy;
    enum eLoopUda = optStrategy("optsize");
}
else
{
    enum eLoopUda;
}

/// Idle policy written by the director, read by the owning worker.
enum IdleKind : uint
{
    spin = 0,
    sleep = 1,
    sleepUntil = 2,
    wait = 3,
    cadence = 4,
}

/// Per-spawned-worker row. Director matching uses identity/tags; workers
/// only load policy fields.
struct WorkerRec
{
    ushort group;
    ushort lpIndex;
    ushort llcIndex;
    ubyte  classIndex;
    bool   isP;
    bool   hasNumber;
    long   number;
    string label;
    Throwable failure;

    shared uint  kind = IdleKind.wait;
    shared uint  spinIters = 64;
    shared ulong sleepNs;
    shared long  sleepUntilTicks;
    shared long  cadenceEpochTicks;
    shared long  cadencePeriodTicks;
    shared uint  cadenceDriftSeq;
    shared long  cadenceDriftTicks;
    shared long  cadenceDriftUntilTicks;
    shared uint  cadenceSkip;
    shared long  cadenceSpinLeadTicks;
    shared uint  epoch;
}

__gshared WorkerRec[] gWorkers;

/// `true` = did work, retry immediately. `false` = idle, apply director policy.
alias WorkerBody = bool function(WorkerSelf* w) @nogc nothrow;

__gshared WorkerBody gWorkerBody;

/// GC-enabled, throwing lifecycle lane. A completed `start` is paired with
/// exactly one `stop` on the same pinned worker. `pump` has the same retry/idle
/// result convention as `WorkerBody`.
struct ManagedWorkerHooks
{
    void function(WorkerSelf* w) start;
    ManagedPumpResult function(WorkerSelf* w) pump;
    void function(WorkerSelf* w) stop;
}

/// Result of one managed worker visit. `deadlineTicks` uses `MonoTime.ticks`
/// and bounds the Director's park deadline without replacing its policy.
struct ManagedPumpResult
{
    bool retry;
    bool hasDeadline;
    long deadlineTicks;

    static ManagedPumpResult idle() pure nothrow @safe
    {
        return ManagedPumpResult(false, false, 0);
    }

    static ManagedPumpResult again() pure nothrow @safe
    {
        return ManagedPumpResult(true, false, 0);
    }

    static ManagedPumpResult until(long ticks) pure nothrow @safe
    {
        return ManagedPumpResult(false, true, ticks);
    }
}

__gshared ManagedWorkerHooks gManagedWorkerHooks;

/// Pinned pool thread. A locator into the CPU-mapped hierarchy of C.
/// Push/pull are methods of C (`home!C()`), not of this type.
struct WorkerSelf
{
    ushort llcIndex;
    ushort numaIndex;
    ubyte  classIndex;
    uint   cpuSetId;
    uint   workerIndex;
    uint   spinIters;
    bool   isP;
    long   lastCadenceBeat;
    uint   driftSeqSeen;
    long   driftAccum;
    void*  context;       /// caller-owned per-worker managed state
    ushort numaLlcCount;
    ushort[maxNumaLlcs] numaLlcs;

    @property ushort homeLlc() const @nogc nothrow { return llcIndex; }

    @property ushort foreignLlcCount() const @nogc nothrow
    {
        return numaLlcCount > 0 ? cast(ushort)(numaLlcCount - 1) : 0;
    }

    bool llcIsHome(ushort llc) const @nogc nothrow
    {
        return llc == llcIndex;
    }

    bool llcIsNumaLocal(ushort llc) const @nogc nothrow
    {
        foreach (i; 0 .. numaLlcCount)
            if (numaLlcs[i] == llc)
                return true;
        return false;
    }
}

/// Not thread-safe vs concurrent mutation of `self`. Called from `start` per worker.
void bindNumaNeighborhood(ref WorkerSelf self, ref const TopologySnapshot snap) @nogc nothrow
{
    self.numaLlcCount = fillNumaLlcs(self.llcIndex, self.numaIndex, snap, self.numaLlcs[]);
}

/// TLS locator for the calling pool thread. Null on non-pool threads.
WorkerSelf* currentWorker() @nogc nothrow
{
    return tlsWorker;
}

void setCurrentWorker(WorkerSelf* w) @nogc nothrow
{
    tlsWorker = w;
}

private WorkerSelf* tlsWorker;

void atomicPause() @nogc nothrow
{
    pause();
}

private bool idleBody(WorkerSelf*) @nogc nothrow
{
    return false;
}

/// Last grid point `<= now` (`epoch` if `now < epoch`) and the next strictly future point.
void cadenceGrid(long now, long epoch, long period, out long beat, out long next) @nogc nothrow pure
{
    if (period <= 0)
    {
        beat = now;
        next = now;
        return;
    }
    if (now < epoch)
    {
        beat = epoch;
        next = epoch;
        return;
    }
    immutable n = (now - epoch) / period;
    beat = epoch + n * period;
    next = beat + period;
}

/// `runNow` → body pulls immediately. Else park until `deadline`.
struct CadenceDecision
{
    bool runNow;
    long deadline;
    long beat;
}

CadenceDecision decideCadence(long now, long epoch, long period,
    bool skipMissed, long lastBeat) @nogc nothrow pure
{
    CadenceDecision d;
    long beat, next;
    cadenceGrid(now, epoch, period, beat, next);
    d.beat = beat;
    if (period <= 0)
    {
        d.runNow = true;
        d.deadline = now;
        return d;
    }
    if (now < epoch)
    {
        d.runNow = false;
        d.deadline = epoch;
        return d;
    }
    // Default: if a whole period passed since we last aligned, pull now
    // (late homogeneous consumer). Skip: always wait for the next beat.
    if (!skipMissed && lastBeat != 0 && now >= lastBeat + period)
    {
        d.runNow = true;
        d.deadline = now;
        return d;
    }
    d.runNow = false;
    d.deadline = next;
    return d;
}

/// Apply a sequenced slew: on a new `seq`, reset the accumulator and add
/// `drift` to `period` each beat until `|period-delta|` reaches `until`.
/// `until <= 0` or `drift == 0` means nominal period.
long effectiveCadencePeriod(long period, long drift, long until, uint seq,
    ref uint seqSeen, ref long accum) @nogc nothrow
{
    if (seq != seqSeen)
    {
        seqSeen = seq;
        accum = 0;
    }
    if (period < 1)
        period = 1;
    if (until <= 0 || drift == 0)
        return period;
    if (accum >= until)
        return period;
    long eff = period + drift;
    if (eff < 1)
        eff = 1;
    immutable step = drift >= 0 ? drift : -drift;
    accum += step;
    return eff;
}

void applyIdlePolicy(ref WorkerSelf self, ref shared(int) runFlag,
    bool hasManagedDeadline = false, long managedDeadline = 0) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        if (self.workerIndex >= gWorkers.length) return;
        auto rec = &gWorkers[self.workerIndex];
        immutable kind = atomicLoad!(MemoryOrder.acq)(rec.kind);
        if (kind == IdleKind.spin)
        {
            immutable n = atomicLoad(rec.spinIters);
            foreach (i; 0 .. n)
            {
                atomicPause();
                if (self.isP)
                    atomicFetchAdd(gSpinsP, 1);
                else
                    atomicFetchAdd(gSpinsE, 1);
            }
            return;
        }
        if (self.isP)
            atomicFetchAdd(gParksP, 1);
        else
            atomicFetchAdd(gParksE, 1);
        if (kind == IdleKind.wait)
        {
            if (hasManagedDeadline)
                parkUntilTicks(self.workerIndex, runFlag, managedDeadline);
            else
                parkWorker(self.workerIndex, runFlag, kWaitForever);
            return;
        }
        if (kind == IdleKind.sleep)
        {
            immutable ns = atomicLoad(rec.sleepNs);
            if (hasManagedDeadline)
            {
                auto nominal = MonoTime.currTime.ticks
                    + cast(long)(ns * MonoTime.ticksPerSecond / 1_000_000_000UL);
                parkUntilTicks(self.workerIndex, runFlag,
                    managedDeadline < nominal ? managedDeadline : nominal);
            }
            else
                parkMs(self.workerIndex, runFlag, nsToMs(ns));
            return;
        }
        if (kind == IdleKind.sleepUntil)
        {
            immutable ticks = atomicLoad(rec.sleepUntilTicks);
            auto now = MonoTime.currTime.ticks;
            if (ticks <= now)
                return;
            immutable deadline = hasManagedDeadline && managedDeadline < ticks
                ? managedDeadline : ticks;
            parkUntilTicks(self.workerIndex, runFlag, deadline);
            return;
        }
        if (kind == IdleKind.cadence)
        {
            immutable epoch = atomicLoad(rec.cadenceEpochTicks);
            immutable period = atomicLoad(rec.cadencePeriodTicks);
            immutable drift = atomicLoad(rec.cadenceDriftTicks);
            immutable until = atomicLoad(rec.cadenceDriftUntilTicks);
            immutable dseq = atomicLoad(rec.cadenceDriftSeq);
            immutable skip = atomicLoad(rec.cadenceSkip) != 0;
            immutable lead = atomicLoad(rec.cadenceSpinLeadTicks);
            auto now = MonoTime.currTime.ticks;
            immutable eff = effectiveCadencePeriod(period, drift, until, dseq,
                self.driftSeqSeen, self.driftAccum);
            auto d = decideCadence(now, epoch, eff, skip, self.lastCadenceBeat);
            self.lastCadenceBeat = d.beat;
            if (d.runNow)
                return;
            immutable deadline = hasManagedDeadline && managedDeadline < d.deadline
                ? managedDeadline : d.deadline;
            parkCadence(self, runFlag, deadline, lead);
        }
    }
}

private void parkUntilTicks(uint workerIndex, ref shared(int) runFlag, long ticks) @nogc nothrow
{
    auto now = MonoTime.currTime.ticks;
    if (ticks <= now)
        return;
    immutable tps = MonoTime.ticksPerSecond;
    immutable ns = (ticks - now) * 1_000_000_000L / tps;
    parkMs(workerIndex, runFlag, nsToMs(ns > 0 ? cast(ulong) ns : 0));
}

private void parkCadence(ref WorkerSelf self, ref shared(int) runFlag,
    long deadline, long spinLead) @nogc nothrow
{
    auto now = MonoTime.currTime.ticks;
    if (deadline <= now)
        return;
    if (spinLead <= 0)
    {
        parkUntilTicks(self.workerIndex, runFlag, deadline);
        return;
    }
    long parkUntil = deadline;
    if (deadline - now > spinLead)
        parkUntil = deadline - spinLead;
    parkUntilTicks(self.workerIndex, runFlag, parkUntil);
    if (!atomicLoad!(MemoryOrder.acq)(runFlag))
        return;
    while (atomicLoad!(MemoryOrder.acq)(runFlag) && MonoTime.currTime.ticks < deadline)
    {
        atomicPause();
        if (self.isP)
            atomicFetchAdd(gSpinsP, 1);
        else
            atomicFetchAdd(gSpinsE, 1);
    }
}

private uint nsToMs(ulong ns) @nogc nothrow
{
    if (ns == 0) return 0;
    immutable ms = ns / 1_000_000UL;
    if (ms == 0) return 1;
    if (ms >= uint.max) return uint.max - 1;
    return cast(uint) ms;
}

private void parkMs(uint workerIndex, ref shared(int) runFlag, uint ms) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        if (ms == 0)
        {
            Thread.yield();
            return;
        }
        parkWorker(workerIndex, runFlag, ms);
    }
}

private void workerLoop(ref WorkerSelf self, ref shared(int) runFlag) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        auto body_ = gWorkerBody;
        if (body_ is null)
            body_ = &idleBody;
        while (atomicLoad!(MemoryOrder.acq)(runFlag))
        {
            if (body_(&self))
                continue;
            applyIdlePolicy(self, runFlag);
        }
    }
}

void pCoreWorkerLoop(ref WorkerSelf self, ref shared(int) runFlag) @nogc nothrow
{
    workerLoop(self, runFlag);
}

@eLoopUda
void eCoreWorkerLoop(ref WorkerSelf self, ref shared(int) runFlag) @nogc nothrow
{
    workerLoop(self, runFlag);
}

unittest
{
    long beat, next;
    cadenceGrid(5, 0, 10, beat, next);
    assert(beat == 0 && next == 10);
    cadenceGrid(10, 0, 10, beat, next);
    assert(beat == 10 && next == 20);
    cadenceGrid(25, 0, 10, beat, next);
    assert(beat == 20 && next == 30);
    cadenceGrid(3, 10, 10, beat, next);
    assert(beat == 10 && next == 10);

    auto onGrid = decideCadence(5, 0, 10, false, 0);
    assert(!onGrid.runNow && onGrid.deadline == 10);

    auto lateDefault = decideCadence(25, 0, 10, false, 10);
    assert(lateDefault.runNow);

    auto lateSkip = decideCadence(25, 0, 10, true, 10);
    assert(!lateSkip.runNow && lateSkip.deadline == 30);

    auto firstIdle = decideCadence(25, 0, 10, false, 0);
    assert(!firstIdle.runNow && firstIdle.deadline == 30);

    uint seqSeen;
    long accum;
    assert(effectiveCadencePeriod(10, 2, 6, 1, seqSeen, accum) == 12);
    assert(accum == 2);
    assert(effectiveCadencePeriod(10, 2, 6, 1, seqSeen, accum) == 12);
    assert(accum == 4);
    assert(effectiveCadencePeriod(10, 2, 6, 1, seqSeen, accum) == 12);
    assert(accum == 6);
    assert(effectiveCadencePeriod(10, 2, 6, 1, seqSeen, accum) == 10);
    assert(effectiveCadencePeriod(10, 3, 6, 2, seqSeen, accum) == 13);
    assert(accum == 3);
}
