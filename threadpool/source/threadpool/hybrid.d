module threadpool.hybrid;

import core.atomic;
import core.time : MonoTime;
import core.thread : Thread;
import threadpool.stats;
import threadpool.failure : FailureCapture;
import threadpool.timing : WorkerTiming, count, countArrival, countLate, countOvershoot, gTiming,
    microsToTicks;
import threadpool.topology : TopologySnapshot, fillNumaLlcs, maxNumaLlcs;

version (Windows)
    version = ThreadpoolOs;
version (linux)
    version = ThreadpoolOs;

version (Windows)
{
    import threadpool.sys.win_wait : parkWorker, parkWorkerUntil, prepareWorkerWait, cancelWorkerWait,
        wakeWorker;
    enum uint kWaitForever = 0xFFFF_FFFF;
}
else version (linux)
{
    import threadpool.sys.linux_wait : parkWorker, parkWorkerUntil, prepareWorkerWait, cancelWorkerWait,
        wakeWorker;
    enum uint kWaitForever = uint.max;
}

/// Idle policy written by the director, read by the owning worker.
enum IdleKind : uint
{
    spin = 0,
    sleep = 1,
    wait = 3,
}

/// Per-spawned-worker row. Director matching uses identity/tags; workers
/// only load policy fields.
struct WorkerRec
{
    ushort llcIndex;
    bool   hasNumber;
    long   number;
    string label;
    FailureCapture failure;

    shared uint  kind = IdleKind.wait;
    shared uint  spinIters = 64;
    shared ulong sleepNs;
    shared uint  epoch;
    /// The instant given to `expect`, in `MonoTime` ticks; `expectNone` for none.
    shared long  expectTicks = expectNone;
    /// How long before that instant to be polling, and how long after it.
    shared long  leadTicks;
    shared long  holdTicks;
    /// Bumped by `resetTiming`; the worker clears its record when it sees it.
    shared uint  timingEpoch;
}

/// `WorkerRec.expectTicks` when nothing is expected.
enum long expectNone = long.min;

/// The defaults of `Selection.readiness`, in microseconds.
enum uint defaultLeadMicros = 1_500;
enum uint defaultHoldMicros = 250; /// ditto

/// What an idle worker does about the instant it was told to expect.
enum Readiness : ubyte
{
    none, /// nothing is expected, or the hold has run out: the base policy
    park, /// the instant is more than the lead away: park until the lead before it
    poll, /// inside the window: poll without parking
}

/// `at`, `lead` and `hold` in `MonoTime` ticks. The window is
/// `[at - lead, at + hold)`; with no lead and no hold there is none, and the
/// instant is ignored.
Readiness readinessAt(long now, long at, long lead, long hold) @nogc nothrow pure @safe
{
    if (at == expectNone || (lead <= 0 && hold <= 0)) return Readiness.none;
    if (now < saturatingAdd(at, -lead)) return Readiness.park;
    if (now < saturatingAdd(at, hold)) return Readiness.poll;
    return Readiness.none;
}

/// `MonoTime` in nanoseconds: the clock `Selection.expect` takes its instant
/// on, and the one `ant_uv`'s `UvLoop.nanos` reads.
long monoNanos() @nogc nothrow
{
    import core.time : convClockFreq;
    return convClockFreq(MonoTime.currTime.ticks, MonoTime.ticksPerSecond, 1_000_000_000);
}

__gshared WorkerRec[] gWorkers;

/// One bit per worker, set while its wait slot is armed (between
/// `prepareWorkerWait` and `cancelWorkerWait`). `wakeOneArmed` clears a bit
/// before waking so two wakers pick two workers; a bit left set by a wake
/// from elsewhere costs one spurious `wakeWorker`, which is an exchange on
/// an unarmed word. Sized by `start`.
__gshared shared(ulong)[] gArmedBits;

void armWorkerBit(uint index) @nogc nothrow
{
    if (index / 64 < gArmedBits.length)
        atomicOp!"|="(gArmedBits[index / 64], 1UL << (index % 64));
}

void disarmWorkerBit(uint index) @nogc nothrow
{
    if (index / 64 < gArmedBits.length)
        atomicOp!"&="(gArmedBits[index / 64], ~(1UL << (index % 64)));
}

/// True when some worker is armed or parked. One or two loads of a line
/// that is written only on arm, disarm and wake.
bool anyArmed() @nogc nothrow
{
    foreach (ref w; gArmedBits)
        if (atomicLoad!(MemoryOrder.raw)(w) != 0) return true;
    return false;
}

/// Wakes one armed or parked worker of group `llc` and returns true, or
/// returns false when none of that group is armed. For work that only that
/// group's workers can see, such as a table in that group's Farm.
bool wakeOneArmedIn(ushort llc) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        import core.bitop : bsf;
        foreach (w; 0 .. gArmedBits.length)
        {
            auto word = atomicLoad!(MemoryOrder.raw)(gArmedBits[w]);
            auto scan = word; // bits still to look at; `word` stays the CAS expectation
            while (scan != 0)
            {
                auto bit = scan & -scan;
                scan &= ~bit;
                immutable index = cast(uint)(w * 64 + bsf(bit));
                if (index >= gWorkers.length || gWorkers[index].llcIndex != llc) continue;
                if (cas(&gArmedBits[w], word, word & ~bit))
                {
                    wakeWorker(index);
                    return true;
                }
                // Lost a race on this word: reload and rescan it.
                word = atomicLoad!(MemoryOrder.raw)(gArmedBits[w]);
                scan = word;
            }
        }
    }
    return false;
}

unittest
{
    // Mixed groups in one word: waking group 1 clears only group 1's bit.
    auto savedWorkers = gWorkers;
    auto savedBits = gArmedBits;
    scope (exit) { gWorkers = savedWorkers; gArmedBits = savedBits; }
    gWorkers = new WorkerRec[3];
    gWorkers[0].llcIndex = 0;
    gWorkers[1].llcIndex = 1;
    gWorkers[2].llcIndex = 0;
    gArmedBits = new shared(ulong)[1];
    atomicStore(gArmedBits[0], 0b111UL);
    assert(wakeOneArmedIn(1));
    assert(atomicLoad(gArmedBits[0]) == 0b101UL);
    assert(!wakeOneArmedIn(1), "no armed worker of group 1 left");
    assert(wakeOneArmedIn(0));
    assert(wakeOneArmedIn(0));
    assert(!wakeOneArmedIn(0));
    assert(atomicLoad(gArmedBits[0]) == 0);
    assert(!anyArmed());
    atomicStore(gArmedBits[0], 0b100UL);
    assert(anyArmed() && wakeOneArmed() && !anyArmed());
}

/// Wakes one armed or parked worker and returns true, or returns false when
/// none is armed. A worker that is armed but not yet asleep returns from its
/// park at once, so a wake that lands in that window is not lost.
bool wakeOneArmed() @nogc nothrow
{
    version (ThreadpoolOs)
    {
        import core.bitop : bsf;
        foreach (w; 0 .. gArmedBits.length)
        {
            auto bits = atomicLoad!(MemoryOrder.raw)(gArmedBits[w]);
            while (bits != 0)
            {
                auto bit = bits & -bits;
                if (cas(&gArmedBits[w], bits, bits & ~bit))
                {
                    wakeWorker(cast(uint)(w * 64 + bsf(bit)));
                    return true;
                }
                bits = atomicLoad!(MemoryOrder.raw)(gArmedBits[w]);
            }
        }
    }
    return false;
}

/// `true` = did work, retry immediately. `false` = idle, apply director policy.
alias WorkerBody = bool function(WorkerSelf* w) @nogc nothrow;

__gshared WorkerBody gWorkerBody;

/// Called once on each pinned worker after its `WorkerBody` loop exits and
/// before the worker thread ends. Use it to release per-worker resources such
/// as a thread-local Farm `ConsumerView`; `shutdown` joins every worker, so
/// all stop hooks have returned when `shutdown` returns.
alias WorkerStop = void function(WorkerSelf* w) @nogc nothrow;

__gshared WorkerStop gWorkerStop;

/// GC-enabled, throwing lifecycle lane. A completed `start` is paired with
/// exactly one `stop` on the same pinned worker. `pump` has the same retry/idle
/// result convention as `WorkerBody`.
///
/// `poolStopped` runs once per pool run, on the thread that calls `shutdown`
/// (or on which `start` fails), after every worker has joined: every `stop`
/// that will ever run has returned. It runs even when no worker reached
/// `start`, e.g. a shutdown right after `start` or a start failure, so
/// per-run state that only a worker's `stop` would settle can still be
/// settled.
struct ManagedWorkerHooks
{
    void function(WorkerSelf* w) start;
    ManagedPumpResult function(WorkerSelf* w) pump;
    void function(WorkerSelf* w) stop;
    void function() nothrow poolStopped;
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

/// A pool worker's own record: a locator into the CPU-mapped hierarchy of C.
/// The worker belongs to physical group `llcIndex`; which processor it runs
/// on is not part of it.
/// Push/pull are methods of C (`home!C()`), not of this type.
struct WorkerSelf
{
    ushort llcIndex;
    ushort numaIndex;
    uint   workerIndex;
    uint   spinIters;
    void*  context;       /// caller-owned per-worker managed state
    // The worker's own bookkeeping for readiness windows and its timing
    // record. Nothing else reads or writes these.
    package long windowEnd;        /// ticks; 0 when it is in no window
    package long windowAt;         /// the instant of the window it is in
    package long lastPoll;         /// ticks of its last idle poll in that window
    package long parkedFor = expectNone; /// the instant whose lead its last deadline park ran to
    package long parkEnded;        /// ticks at which that park returned
    package long lastStallSample;  /// ticks of the last spin-policy sample; 0 for none
    package uint spinDebt;         /// PAUSEs since that sample
    package uint timingEpoch;      /// the `WorkerRec.timingEpoch` its record belongs to
    package bool windowSawWork;    /// work was found in the window it is in
    package bool windowCrossed;    /// the window's instant has been accounted for
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

/// Convert `n` units of a `fromPerSecond` clock to units of a `toPerSecond`
/// clock, rounding toward zero and saturating at `long.max`/`-long.max`.
/// `n * to / from` wraps once `n * to` passes `long.max`: with POSIX
/// `MonoTime.ticksPerSecond == 1e9` that is any span over ~9.2 s. Whole
/// seconds and the remainder are scaled separately instead; the result is
/// exactly `n * to / from` whenever that fits. Both rates must be in
/// `1 .. 4e9` so the remainder product fits in a `ulong`.
long scaleClock(long n, long fromPerSecond, long toPerSecond) @nogc nothrow pure @safe
{
    if (n < 0)
        return -scaleClock(n == long.min ? long.max : -n, fromPerSecond, toPerSecond);
    immutable whole = n / fromPerSecond;
    immutable rem = n % fromPerSecond;
    if (whole > long.max / toPerSecond)
        return long.max;
    immutable hi = whole * toPerSecond;
    immutable lo = cast(long)(cast(ulong) rem * cast(ulong) toPerSecond / cast(ulong) fromPerSecond);
    return hi > long.max - lo ? long.max : hi + lo;
}

/// `a + b` clamped to `long.min .. long.max`.
long saturatingAdd(long a, long b) @nogc nothrow pure @safe
{
    if (b > 0 && a > long.max - b) return long.max;
    if (b < 0 && a < long.min - b) return long.min;
    return a + b;
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

/// What `pollRound` tells the worker loop. The loop keeps the two facts it
/// needs on the productive path, "work was done since the last idle poll" and
/// "tell me about the next work found", in locals, so a body call that finds
/// work costs it no load or store of the worker's record.
enum Poll : uint
{
    park = 0,  /// go on to arm the wake slot and park
    again = 1, /// a round of PAUSEs was done: pump again
    watch = 2, /// in a window that has seen no work yet: report the first with `noteBurst`
}

/// The first work found in a readiness window: when it came, against the
/// window's instant. Finding work is a look too, so when it is the first
/// look after the instant it settles whether the worker was ready for it.
void noteBurst(ref WorkerSelf self) @nogc nothrow
{
    self.windowSawWork = true;
    if (self.workerIndex >= gTiming.length) return;
    auto t = &gTiming[self.workerIndex];
    immutable now = MonoTime.currTime.ticks;
    countArrival(*t, now - self.windowAt);
    if (self.windowCrossed || now < self.windowAt) return;
    self.windowCrossed = true;
    // Late only if the worker was away: a burst published late is not the
    // worker's lateness, and its last idle poll then came after the instant.
    if (now - self.lastPoll <= microsToTicks(100)
        || !countLate(t.spinningLate, t.spinningLate1ms, t.worstSpinningLate, now - self.windowAt))
        count(t.ready);
}

/// The worker's own timing record, cleared first if `resetTiming` asked.
private WorkerTiming* timingOf(ref WorkerSelf self, WorkerRec* rec) @nogc nothrow
{
    auto t = &gTiming[self.workerIndex];
    immutable epoch = atomicLoad!(MemoryOrder.raw)(rec.timingEpoch);
    if (epoch != self.timingEpoch)
    {
        self.timingEpoch = epoch;
        t.clear();
    }
    return t;
}

private void spinPauses(uint n) @nogc nothrow
{
    foreach (i; 0 .. n)
    {
        atomicPause();
        // A process-wide counter per pause cost 7.8% on consumer-bound
        // workloads (hosting.md); counted only when built for it.
        version (ThreadpoolStats)
            atomicFetchAdd(gSpins, 1);
    }
}

/// An idle worker that polls instead of parking: under the `spin` policy, or
/// inside a readiness window. Does one round of PAUSEs and returns
/// `Poll.again`, and the loop then pumps again; returns `Poll.park` when the
/// worker should go on to arm its wake slot and park. Called before the slot
/// is armed, so a polling worker is never armed and waking it is one
/// exchange with no system call. `worked`: the body made progress since the
/// last call.
///
/// Reads the clock only when an instant is set or a window is open.
uint pollRound(ref WorkerSelf self, bool worked) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        if (self.workerIndex >= gWorkers.length || self.workerIndex >= gTiming.length) return Poll.park;
        auto rec = &gWorkers[self.workerIndex];
        immutable n = atomicLoad!(MemoryOrder.raw)(rec.spinIters);
        if (atomicLoad!(MemoryOrder.acq)(rec.kind) == IdleKind.spin)
        {
            spinPauses(n);
            sampleStall(self, rec, n, worked);
            return Poll.again;
        }
        immutable at = atomicLoad!(MemoryOrder.raw)(rec.expectTicks);
        if (at == expectNone && self.windowEnd == 0)
        {
            self.parkedFor = expectNone; // withdrawn: no window is owed
            return Poll.park;
        }
        immutable now = MonoTime.currTime.ticks;
        if (self.windowEnd != 0 && now >= self.windowEnd)
            leaveWindow(self, rec, now, worked);
        if (self.windowEnd == 0)
        {
            immutable hold = atomicLoad!(MemoryOrder.raw)(rec.holdTicks);
            bool entered;
            immutable old = self.parkedFor;
            if (old != expectNone && self.parkEnded >= old)
            {
                // The park that prepared for `old` returned after that
                // instant but inside its window (one past the window is
                // counted where it returned). `expect` may already name the
                // next instant: a publisher names it as soon as the burst is
                // out, and that call is what woke the worker. The worker
                // serves the rest of the window it parked for, and its
                // lateness is recorded there.
                if (now < saturatingAdd(old, hold))
                {
                    enterWindow(self, rec, old, hold, now);
                    entered = true;
                }
                else
                {
                    // That window closed before the worker polled. If its
                    // park was late, it missed the window; otherwise it was
                    // back in time and ran work past the window's end.
                    auto t = timingOf(self, rec);
                    count(t.windows);
                    if (countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, self.parkEnded - old))
                        count(t.missedWindows);
                    else
                        count(t.busy);
                    self.parkedFor = expectNone;
                }
            }
            else if (old != at)
                self.parkedFor = expectNone; // replaced before it came
            if (!entered)
            {
                if (readinessAt(now, at, atomicLoad!(MemoryOrder.raw)(rec.leadTicks), hold) != Readiness.poll)
                    return Poll.park;
                enterWindow(self, rec, at, hold, now);
            }
        }
        else if (!self.windowCrossed && now >= self.windowAt)
        {
            // The instant passed between the last poll and this one.
            auto t = timingOf(self, rec);
            self.windowCrossed = true;
            if (worked)
                count(t.busy);
            else if (!countLate(t.spinningLate, t.spinningLate1ms, t.worstSpinningLate, now - self.windowAt))
                count(t.ready);
        }
        self.lastPoll = now;
        spinPauses(n);
        return self.windowSawWork ? Poll.again : Poll.again | Poll.watch;
    }
    else
        return Poll.park;
}

private void enterWindow(ref WorkerSelf self, WorkerRec* rec, long at, long hold, long now) @nogc nothrow
{
    auto t = timingOf(self, rec);
    count(t.windows);
    self.windowEnd = saturatingAdd(at, hold);
    self.windowAt = at;
    self.windowSawWork = false;
    self.windowCrossed = now >= at;
    if (self.windowCrossed)
    {
        // It came into the window after the instant. If its last park ran to
        // this window's lead, the park was late; otherwise it was working.
        if (self.parkedFor != at)
            count(t.busy);
        else if (!countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, now - at))
            count(t.ready);
    }
    self.parkedFor = expectNone;
}

private void leaveWindow(ref WorkerSelf self, WorkerRec* rec, long now, bool worked) @nogc nothrow
{
    auto t = timingOf(self, rec);
    if (!self.windowSawWork) count(t.emptyWindows);
    // It did not look between the instant and the window's end. Either it
    // was running work it had found before the instant, or it was away from
    // its processor for the rest of the window.
    if (!self.windowCrossed)
    {
        if (worked)
            count(t.busy);
        else if (!countLate(t.spinningLate, t.spinningLate1ms, t.worstSpinningLate, now - self.windowAt))
            count(t.ready);
    }
    self.windowEnd = 0;
}

/// A `spin` worker reads the clock about every 256 PAUSEs and counts a gap
/// of more than 100 µs with no work in it as a stall. A round of more than
/// 1,024 PAUSEs is itself tens of microseconds, so there it does not sample.
private void sampleStall(ref WorkerSelf self, WorkerRec* rec, uint n, bool worked) @nogc nothrow
{
    if (n > 1_024) return;
    // Work since the last poll: the gap being measured is not idle time.
    if (worked) self.lastStallSample = 0;
    self.spinDebt += n ? n : 1;
    if (self.spinDebt < 256) return;
    self.spinDebt = 0;
    immutable now = MonoTime.currTime.ticks;
    if (self.lastStallSample != 0)
    {
        auto t = timingOf(self, rec);
        countLate(t.stalls, t.stalls1ms, t.worstStall, now - self.lastStallSample);
    }
    self.lastStallSample = now;
}

/// The park of an idle worker, after it armed its wake slot and found
/// nothing a second time.
void applyIdlePolicy(ref WorkerSelf self, ref shared(int) runFlag,
    bool hasManagedDeadline = false, long managedDeadline = 0) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        if (self.workerIndex >= gWorkers.length || self.workerIndex >= gTiming.length) return;
        auto rec = &gWorkers[self.workerIndex];
        immutable kind = atomicLoad!(MemoryOrder.acq)(rec.kind);
        if (kind == IdleKind.spin)
        {
            // The policy turned to spin after `pollRound` looked.
            spinPauses(atomicLoad(rec.spinIters));
            return;
        }
        version (ThreadpoolStats)
            atomicFetchAdd(gParks, 1);

        // A deadline is held precisely: by the worker's timer on Windows, by
        // an absolute futex deadline on Linux. There are two kinds, the lead
        // before an expected instant and a managed deadline, and the park
        // takes the earlier.
        long deadline;
        bool hasDeadline;
        long leadOf = expectNone;
        immutable at = atomicLoad!(MemoryOrder.raw)(rec.expectTicks);
        if (at != expectNone)
        {
            immutable lead = atomicLoad!(MemoryOrder.raw)(rec.leadTicks);
            final switch (readinessAt(MonoTime.currTime.ticks, at, lead,
                atomicLoad!(MemoryOrder.raw)(rec.holdTicks)))
            {
                case Readiness.poll:
                    return; // the window opened since `pollRound` looked
                case Readiness.park:
                    deadline = saturatingAdd(at, -lead);
                    hasDeadline = true;
                    leadOf = at;
                    break;
                case Readiness.none:
                    break;
            }
        }
        if (hasManagedDeadline && (!hasDeadline || managedDeadline < deadline))
        {
            deadline = managedDeadline;
            hasDeadline = true;
            leadOf = expectNone;
        }

        // A `sleep` policy's re-poll bound stays on the system timer tick,
        // as the wait's own timeout, so one park ends at whichever of the
        // deadline and the bound comes first.
        uint bound = uint.max;
        if (kind == IdleKind.sleep)
        {
            bound = nsToMs(atomicLoad(rec.sleepNs));
            if (bound == 0)
            {
                Thread.yield();
                return;
            }
        }
        auto t = timingOf(self, rec);
        if (!hasDeadline)
        {
            count(t.parks);
            parkWorker(self.workerIndex, runFlag, bound == uint.max ? kWaitForever : bound);
            return;
        }
        count(t.timedParks);
        parkWorkerUntil(self.workerIndex, runFlag, deadline, bound);
        // A park that ran to its deadline says how late the timer and the
        // scheduler brought the worker back. One that a wake or the bound
        // ended early says nothing.
        immutable after = MonoTime.currTime.ticks;
        if (after >= deadline)
        {
            countOvershoot(*t, after - deadline);
            self.parkedFor = leadOf;
            self.parkEnded = after;
            // A park that ran past the whole window: the worker was never
            // ready for that instant, and no window will be entered for it.
            if (leadOf != expectNone
                && after >= saturatingAdd(leadOf, atomicLoad!(MemoryOrder.raw)(rec.holdTicks)))
            {
                count(t.windows);
                count(t.missedWindows);
                countLate(t.parkedLate, t.parkedLate1ms, t.worstParkedLate, after - leadOf);
                self.parkedFor = expectNone;
            }
        }
    }
}

private uint nsToMs(ulong ns) @nogc nothrow pure @safe
{
    if (ns == 0) return 0;
    immutable ms = ns / 1_000_000UL;
    if (ms == 0) return 1;
    if (ms >= uint.max) return uint.max - 1;
    return cast(uint) ms;
}

void workerLoop(ref WorkerSelf self, ref shared(int) runFlag) @nogc nothrow
{
    version (ThreadpoolOs)
    {
        auto body_ = gWorkerBody;
        if (body_ is null)
            body_ = &idleBody;
        bool worked, watch;
        while (atomicLoad!(MemoryOrder.acq)(runFlag))
        {
            if (body_(&self))
            {
                worked = true;
                if (watch) { watch = false; noteBurst(self); }
                continue;
            }
            // Polling, under `spin` or in a readiness window: no wake slot.
            immutable poll = pollRound(self, worked);
            worked = false;
            watch = (poll & Poll.watch) != 0;
            if (poll & Poll.again) continue;
            if (!prepareWorkerWait(self.workerIndex)) break;
            armWorkerBit(self.workerIndex);
            scope (exit) { disarmWorkerBit(self.workerIndex); cancelWorkerWait(self.workerIndex); }
            if (!atomicLoad!(MemoryOrder.acq)(runFlag)) break;
            if (body_(&self))
            {
                worked = true;
                continue;
            }
            applyIdlePolicy(self, runFlag);
        }
        if (auto stop = gWorkerStop)
            stop(&self);
    }
}

unittest
{
    // scaleClock is exact wherever the naive product fits...
    enum ns = 1_000_000_000L;
    foreach (n; [0L, 1, 999, 123_456_789, 9_000_000_000])
        foreach (tps; [10_000_000L, ns, 3_579_545])
            assert(scaleClock(n, ns, tps) == n * tps / ns);
    assert(scaleClock(-1_500_000, ns, 10_000_000) == -15_000);
    // ...and correct where it wrapped: POSIX ticks are nanoseconds, so any
    // span past long.max / 1e9 (~9.2 s) overflowed `n * 1e9`.
    foreach (secs; [10L, 30, 3600])
    {
        assert(scaleClock(secs * ns, ns, ns) == secs * ns);
        assert(scaleClock(secs * 10_000_000, 10_000_000, ns) == secs * ns);
        assert(scaleClock(secs * ns, ns, 10_000_000) == secs * 10_000_000);
    }
    assert(scaleClock(long.max, 10_000_000, ns) == long.max);
    assert(scaleClock(long.min, 10_000_000, ns) == -long.max);
    assert(saturatingAdd(long.max - 1, 5) == long.max);
    assert(saturatingAdd(long.min + 1, -5) == long.min);
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
}

unittest
{
    // The window is [at - lead, at + hold).
    assert(readinessAt(0, expectNone, 15, 3) == Readiness.none);
    assert(readinessAt(84, 100, 15, 3) == Readiness.park);
    assert(readinessAt(85, 100, 15, 3) == Readiness.poll);
    assert(readinessAt(100, 100, 15, 3) == Readiness.poll);
    assert(readinessAt(102, 100, 15, 3) == Readiness.poll);
    assert(readinessAt(103, 100, 15, 3) == Readiness.none, "the hold has run out");
    assert(readinessAt(100, 100, 0, 0) == Readiness.none, "no lead and no hold: no window");
    assert(readinessAt(50, 100, 0, 0) == Readiness.none, "and no park to the instant either");
    assert(readinessAt(50, 100, 0, 3) == Readiness.park && readinessAt(100, 100, 0, 3) == Readiness.poll,
        "a hold alone: wake at the instant, then poll");
    // Instants near the ends of the clock do not wrap.
    assert(readinessAt(0, long.max, 15, 3) == Readiness.park);
    assert(readinessAt(long.max - 1, long.max, 15, long.max) == Readiness.poll);
    assert(readinessAt(0, long.min + 1, long.max, 3) == Readiness.none);

    immutable a = monoNanos();
    immutable b = monoNanos();
    assert(b >= a && a > 0);
}
