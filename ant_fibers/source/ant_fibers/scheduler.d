/++
 + DRuntime Fiber scheduling layer for Ant Farm.
 +
 + Ant Farm is only the ready-work transport. Waits are arbitrated by a claim
 + word per task (see FiberTask.armWait) and never enter the Farm,
 + and newly-ready fibers collect in `PublishAccumulator` until a producer
 + flushes a useful table. No external thread-pool package is required.
 +/
module ant_fibers.scheduler;

import antfarm;
import ant_fibers.cacheline;
import core.atomic;
import core.memory : GC;
import core.sync.event : Event;
import core.sync.mutex : Mutex;
import core.thread : Fiber, Thread;
import core.time : Duration, MonoTime;


version (TSan)
{
    import ldc.attributes : llvmAttr;

    private extern(C) nothrow @nogc
    {
        void* __tsan_get_current_fiber();
        void* __tsan_create_fiber(uint flags);
        void __tsan_destroy_fiber(void* fiber);
        void __tsan_switch_to_fiber(void* fiber, uint flags);
    }
}

alias Signal = ulong;
// Application signals keep the high bit clear (reserved, as it always was).
private enum Signal timerSignalBit = 1UL << 63;
/// Default `FiberDomainOptions.recordRetention`: the worst-case records of
/// 4,096 tasks that are live or terminal but not yet retired (about 1.2 MiB
/// per domain, 128 KiB of it scanned by the GC).
enum size_t defaultRecordRetention = 16_384;
private enum uint lifecycleRecordsPerTask = 4;
// Cost class of a Fiber flush table: claims of 8 entries.
private enum uint flushAvgCost = avgCostForChunk(8);

/// How a wait ended, in the low bits of a task's wait word. The word's high
/// bits count waits; see FiberTask.armWait.
private enum WaitCause : ubyte
{
    none,
    armed,      // the wait is live and unclaimed
    signalled,  // an event, semaphore, signal or join completion claimed it
    timedOut,   // its timer claimed it
    cancelled,  // cancellation claimed it
    withdrawn,  // its own fiber claimed it before parking
}
private enum uint waitCauseBits = 3;
private enum ulong waitCauseMask = (1UL << waitCauseBits) - 1;
static assert(WaitCause.max <= waitCauseMask);

enum FiberOutcome : ubyte
{
    pending,
    completed,
    failed,
    cancelled,
}

enum FiberJoinStatus : ubyte
{
    pending,
    completed,
    failed,
    cancelled,
    timedOut,
    wouldBlock,
    wrongDomain,
    staleHandle,
}

/// Generation-stable observation returned by poll and external joins.
struct FiberJoinResult
{
    TaskHandle task;
    FiberJoinStatus status;
    FiberOutcome outcome;
    CancellationDisposition cancellationDisposition;
    Throwable failure;

    @property bool terminal() const nothrow @nogc
    {
        return status == FiberJoinStatus.completed
            || status == FiberJoinStatus.failed
            || status == FiberJoinStatus.cancelled;
    }
}

enum CancellationResult : ubyte
{
    staleHandle,
    alreadyTerminal,
    alreadyRequested,
    wonBeforeEntry,
    requestedRunning,
    requestedReady,
    requestedWaiting,
}

/// Result of submitting a generation-paired request to the domain control
/// lane. Submission does not imply that cancellation beat terminal entry.
enum CancellationSubmission : ubyte
{
    queued,
    staleHandle,
}

/// Application result for one queued remote cancellation request.
struct CancellationApplication
{
    TaskHandle task;
    CancellationResult result;
}

enum CancellationDisposition : ubyte
{
    none,
    pending,
    acknowledged,
    resultAbrogated,
    cleanupFailed,
}

enum FiberLifecycleKind : ubyte
{
    admitted,
    cancellation,
    terminal,
    failure,
}

/// Immutable snapshot copied out of the ordered lifecycle lane. `sequence` is
/// domain-local and assigned by the Director-side drain in queue order.
struct FiberLifecycleEvent
{
    ulong sequence;
    FiberLifecycleKind kind;
    TaskHandle task;
    FiberOutcome outcome;
    CancellationResult cancellation;
    CancellationDisposition cancellationDisposition;
    Throwable failure;
}

/// Receives each lifecycle record during `FiberDomain.retire`, by value: the
/// copy, including its failure reference, is the handler's to keep.
alias FiberLifecycleHandler = void delegate(FiberLifecycleEvent);

/// Receives each terminal task during `FiberDomain.retire`, before the task
/// is recycled. The task is valid only for the duration of the call.
alias FiberCompletionHandler = void delegate(FiberTask);

/// Which threads may retire a domain's records and terminal tasks.
enum FiberRetirement : ubyte
{
    /// Only the constructing (Director) thread: a `FiberTask` stays valid
    /// until the Director's own `retire` hands it to `onCompletion`.
    director,
    /// Any thread running a service pass. A task may be recycled at any time
    /// after it terminates, so identify tasks by `TaskHandle`. Handlers run
    /// one at a time on whichever service thread retires, must not block,
    /// and a throwing handler fails that thread's service pass.
    serviceThreads,
}

/// Construction-time configuration of a `FiberDomain`.
struct FiberDomainOptions
{
    /// Lifecycle record capacity. Spawn reserves room for 4 records per task,
    /// held from admission until termination (unused ones are returned then)
    /// and for emitted records until they are retired, and throws
    /// `FiberLifecycleBackpressure` past this limit. It therefore bounds
    /// concurrent fibers as well as unretired ones, at retention / 4. At
    /// least 4.
    size_t recordRetention = defaultRecordRetention;
    /// Each terminal task, before it is recycled; null recycles silently.
    FiberCompletionHandler onCompletion;
    /// Each lifecycle record, in sequence order. Null: no record is emitted
    /// (there would be no reader), while admission still reserves and
    /// bounds as if they were.
    FiberLifecycleHandler onRecord;
    /// Who may retire records and terminal tasks.
    FiberRetirement retirement = FiberRetirement.director;
}

/// Parameters of one `FiberReadyLane.serve` pass.
struct FiberServicePass
{
    /// Poll timers and report the earliest deadline.
    bool responder = true;
    /// The caller consumed Farm work just before this pass: flush only a full
    /// batch, so publication accumulates while work flows.
    bool consumedWork;
    /// Ready tasks per flushed table; must be nonzero.
    size_t flushBatch = 32;
    /// Ant Farm cost class of a flushed table.
    uint avgCost = flushAvgCost;
}

/// What one service pass did.
struct FiberServiceResult
{
    /// Anything was applied, woken, delivered, flushed or retired.
    bool progress;
    /// Ready tasks published to the Farm.
    size_t flushed;
    /// Earliest armed timer deadline in MonoTime ticks (responders only),
    /// or long.max.
    long deadline = long.max;
}

final class FiberCancelled : Exception
{
    this() { super("fiber cancelled"); }
}

/// Preallocated so the Farm callback can fail a protocol-breaking `Fiber.yield`
/// without allocating.
private Exception rawYieldFailure;

shared static this()
{
    rawYieldFailure = new Exception(
        "ant_fibers: Fiber.yield without scheduler suspend");
}

final class FiberLifecycleBackpressure : Exception
{
    immutable size_t limit;
    immutable size_t reserved;

    this(size_t limit, size_t reserved)
    {
        this.limit = limit;
        this.reserved = reserved;
        super("fiber lifecycle retention limit reached");
    }
}

private ulong taskDiagnosticId(FiberTask slot, ulong generation) nothrow @nogc
{
    // SplitMix64 finalizer over the stable slot address and logical generation.
    // This is a trace key, not an identity proof or security boundary.
    ulong x = cast(ulong) cast(void*) slot;
    x ^= generation + 0x9E37_79B9_7F4A_7C15UL + (x << 6) + (x >> 2);
    x = (x ^ (x >> 30)) * 0xBF58_476D_1CE4_E5B9UL;
    x = (x ^ (x >> 27)) * 0x94D0_49BB_1331_11EBUL;
    return x ^ (x >> 31);
}

/// Stable logical identity for recyclable task storage. Equality and validity
/// use the exact slot/generation pair; diagnosticId is only a compact trace key
/// and is never used as proof of identity.
struct TaskHandle
{
    private FiberTask slot;
    ulong generation;
    ulong diagnosticId;

    /// Non-blocking, generation-stable status and terminal-data snapshot.
    FiberJoinResult poll() const nothrow @nogc
    {
        FiberJoinResult result;
        result.task = TaskHandle(cast(FiberTask) slot, generation, diagnosticId);
        auto task = cast(FiberTask) slot;
        if (task is null || generation == 0)
        {
            result.status = FiberJoinStatus.staleHandle;
            return result;
        }
        immutable opening = atomicLoad!(MemoryOrder.acq)(task.generationState);
        if (opening != generation)
        {
            result.status = FiberJoinStatus.staleHandle;
            return result;
        }
        if (phaseOf(atomicLoad!(MemoryOrder.acq)(task.controlWord))
            != Phase.terminated)
        {
            result.status = atomicLoad!(MemoryOrder.acq)(task.generationState)
                == generation ? FiberJoinStatus.pending
                              : FiberJoinStatus.staleHandle;
            return result;
        }
        FiberOutcome outcome;
        Throwable failure;
        CancellationDisposition disposition;
        if (!terminalSnapshot(outcome, failure, disposition))
        {
            result.status = FiberJoinStatus.staleHandle;
            return result;
        }
        result.outcome = outcome;
        result.failure = failure;
        result.cancellationDisposition = disposition;
        final switch (outcome)
        {
        case FiberOutcome.pending:
            result.status = FiberJoinStatus.pending;
            break;
        case FiberOutcome.completed:
            result.status = FiberJoinStatus.completed;
            break;
        case FiberOutcome.failed:
            result.status = FiberJoinStatus.failed;
            break;
        case FiberOutcome.cancelled:
            result.status = FiberJoinStatus.cancelled;
            break;
        }
        return result;
    }

    /// Block an external OS thread until this generation terminates or becomes
    /// stale. Managed fibers must use a scheduler-aware join instead of
    /// blocking their worker.
    FiberJoinResult join() const nothrow @nogc
    {
        if (FiberDomain.currentTask !is null)
            return FiberJoinResult(
                TaskHandle(cast(FiberTask) slot, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        auto initial = poll();
        if (initial.status != FiberJoinStatus.pending) return initial;
        auto task = cast(FiberTask) slot;
        if (!task.registerJoinWaiter(generation)) return poll();
        scope (exit) task.unregisterJoinWaiter();
        auto afterRegistration = poll();
        if (afterRegistration.status != FiberJoinStatus.pending)
            return afterRegistration;
        task.joinControl.terminalEvent.wait();
        return poll();
    }

    /// Timed external blocking join. `timedOut` is returned only while the
    /// same generation remains live after the timeout.
    FiberJoinResult join(Duration timeout) const nothrow @nogc
    {
        if (FiberDomain.currentTask !is null)
            return FiberJoinResult(
                TaskHandle(cast(FiberTask) slot, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        auto initial = poll();
        if (initial.status != FiberJoinStatus.pending) return initial;
        auto task = cast(FiberTask) slot;
        if (!task.registerJoinWaiter(generation)) return poll();
        scope (exit) task.unregisterJoinWaiter();
        auto afterRegistration = poll();
        if (afterRegistration.status != FiberJoinStatus.pending)
            return afterRegistration;
        if (!task.joinControl.terminalEvent.wait(timeout))
        {
            auto afterTimeout = poll();
            if (afterTimeout.status == FiberJoinStatus.pending)
                afterTimeout.status = FiberJoinStatus.timedOut;
            return afterTimeout;
        }
        return poll();
    }

    /// Suspend the current managed Fiber without blocking its worker until a
    /// task in the same domain terminates. Completion registration and the
    /// terminal recheck are one target-list transaction, so no wake is lost.
    FiberJoinResult joinFiber()
    {
        return joinFiberImpl(MonoTime.init, false);
    }

    /// Scheduler-aware timed join. Timeout wakeup shares the existing indexed
    /// timer arbitration with signal waits and cancellation.
    FiberJoinResult joinFiber(Duration timeout)
    {
        return joinFiberImpl(MonoTime.currTime + timeout, true);
    }

    private FiberJoinResult joinFiberImpl(MonoTime deadline, bool timed)
    {
        auto initial = poll();
        if (initial.status != FiberJoinStatus.pending) return initial;
        auto waiter = FiberDomain.currentTask;
        if (waiter is null)
            return FiberJoinResult(
                TaskHandle(cast(FiberTask) slot, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        auto target = cast(FiberTask) slot;
        if (target is waiter)
            return FiberJoinResult(
                TaskHandle(target, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        if (target !is null && target.domain !is waiter.domain)
            return FiberJoinResult(
                TaskHandle(target, generation, diagnosticId),
                FiberJoinStatus.wrongDomain);

        auto domain = waiter.domain;
        while (true)
        {
            auto observed = poll();
            if (observed.status != FiberJoinStatus.pending) return observed;
            if (timed && MonoTime.currTime >= deadline)
            {
                observed.status = FiberJoinStatus.timedOut;
                return observed;
            }
            if (waiter.cancellationRequested) throw new FiberCancelled;
            immutable armed = waiter.armWait();
            if (waiter.cancellationRequested)
            {
                waiter.withdrawWait(armed);
                throw new FiberCancelled;
            }
            // False: the target finished (or was recycled) first; re-poll.
            if (!target.registerManagedJoinWaiter(waiter, generation))
            {
                waiter.withdrawWait(armed);
                continue;
            }
            if (timed)
            {
                try
                    domain.armTimer(waiter, deadline, armed);
                catch (Throwable failure)
                {
                    target.removeManagedJoinWaiter(waiter);
                    throw failure;
                }
            }
            Fiber.yield();
            // No-op when completion unlinked us; required after a timeout or
            // cancellation, before this fiber can wait again.
            target.removeManagedJoinWaiter(waiter);
            if (timed) domain.disarmTimer(waiter);
            if (waiter.cancellationRequested) throw new FiberCancelled;
        }
    }

    @property bool current() const nothrow @nogc
    {
        return slot !is null && slot.generation == generation;
    }

    /// Read a terminal outcome only if this handle still names the slot's
    /// current logical generation. False is stale or not yet terminal.
    bool tryOutcome(out FiberOutcome result) const nothrow @nogc
    {
        Throwable ignored;
        CancellationDisposition ignoredCancellation;
        FiberOutcome snapshot;
        if (!terminalSnapshot(snapshot, ignored, ignoredCancellation)) return false;
        result = snapshot;
        return true;
    }

    /// Ditto for the retained failure. A successful read may return null for
    /// completed/cancelled tasks.
    bool tryException(out Throwable result) const nothrow @nogc
    {
        FiberOutcome ignored;
        CancellationDisposition ignoredCancellation;
        Throwable snapshot;
        if (!terminalSnapshot(ignored, snapshot, ignoredCancellation)) return false;
        result = snapshot;
        return true;
    }

    bool tryCancellationDisposition(out CancellationDisposition result) const
        nothrow @nogc
    {
        FiberOutcome ignoredOutcome;
        Throwable ignoredFailure;
        CancellationDisposition snapshot;
        if (!terminalSnapshot(ignoredOutcome, ignoredFailure, snapshot))
            return false;
        result = snapshot;
        return true;
    }

    private bool terminalSnapshot(out FiberOutcome result, out Throwable error,
        out CancellationDisposition disposition)
        const nothrow @nogc
    {
        auto task = slot;
        if (task is null || generation == 0) return false;
        immutable opening = atomicLoad!(MemoryOrder.acq)(task.generationState);
        if (opening != generation
            || phaseOf(atomicLoad!(MemoryOrder.acq)(task.controlWord))
                != Phase.terminated)
            return false;
        auto observedOutcome = cast(FiberOutcome)
            atomicLoad!(MemoryOrder.acq)(task.outcomeState);
        auto observedFailure = cast(Throwable)
            atomicLoad!(MemoryOrder.acq)(task.failure);
        auto observedDisposition = cast(CancellationDisposition)
            atomicLoad!(MemoryOrder.acq)(task.cancellationDispositionState);
        immutable closing = atomicLoad!(MemoryOrder.acq)(task.generationState);
        if (closing != generation) return false;
        result = observedOutcome;
        error = observedOutcome == FiberOutcome.failed ? observedFailure : null;
        disposition = observedDisposition;
        return true;
    }

    bool opEquals(const TaskHandle rhs) const nothrow @nogc
    {
        return slot is rhs.slot && generation == rhs.generation;
    }
}

static assert(size_t.sizeof == 8, "ant_fibers is 64-bit only");

/// Exclusive-owner execution phase. Cancelled/signalled twins live in the
/// event fields of `controlWord`, not here.
private enum Phase : uint
{
    ready,
    running,
    waiting,
    terminating,
    terminated,
}

/// `FiberTaskControl.controlWord` layout (one atomic ulong):
/// [63:32] PHASE, [31:24] PARK, [23:16] WAKE, [15:8] ENTER, [7:0] CANCEL.
/// Event fields are fetch_add-only while a generation is live. Phase changes
/// are fetch_add of a shifted delta so they compose with in-flight CANCEL.
private enum ulong ctrlCancel = 1UL;
private enum ulong ctrlEnter  = 1UL << 8;
private enum ulong ctrlWake   = 1UL << 16;
private enum ulong ctrlPark   = 1UL << 24;
private enum uint  ctrlPhaseShift = 32;
private enum ulong ctrlCancelMask = 0xFFUL;
private enum ulong ctrlEnterMask  = 0xFFUL << 8;
private enum ulong ctrlWakeMask   = 0xFFUL << 16;
private enum ulong ctrlParkMask   = 0xFFUL << 24;
private enum ulong ctrlParkWake   = ctrlPark + ctrlWake;

static assert(ctrlEnter == ctrlCancel << 8);
static assert(ctrlWake == ctrlEnter << 8);
static assert(ctrlPark == ctrlWake << 8);

private ulong controlWordFor(Phase phase) pure nothrow @nogc
{
    return (cast(ulong) phase) << ctrlPhaseShift;
}

private Phase phaseOf(ulong word) pure nothrow @nogc
{
    return cast(Phase)(word >> ctrlPhaseShift);
}

private uint ctrlField(ulong word, ulong addend) pure nothrow @nogc
{
    return cast(uint)((word / addend) & 0xFFUL);
}

align(64) private struct FiberTaskControl
{
    shared ulong controlWord;
    shared ubyte outcomeState;
    shared ulong generationState;
    shared ulong cancelGeneration;
    shared ulong lastResumeIdentityState;
    shared ulong resumeCountState;
    shared ulong migrationCountState;
    shared ubyte cancellationEventEmitted;
    shared ubyte cancellationDispositionState;
}
static assert(FiberTaskControl.sizeof == cacheLineSize);

/// DRuntime's Event contains only OS synchronization storage/handles, not GC
/// references. Keep its cross-thread mutations off the task object's GC line.
align(64) private struct FiberJoinControl
{
    Event terminalEvent;
    shared uint state;
}
static assert(FiberJoinControl.sizeof % cacheLineSize == 0);

/// First line owns this task's inbound managed-join list. The second line is
/// this task's outbound membership when it joins another task. Raw task words
/// are safe outside GC scans because every participating task remains an
/// active domain root until the membership is detached.
align(64) private struct FiberManagedJoinControl
{
    shared int gate;
    ubyte[4] gateAlignment;
    shared size_t headWord;
    shared uint waiterCount;
    ubyte[44] listPadding;

    shared size_t targetWord;
    shared size_t nextWord;
    shared size_t previousWord;
    shared ulong targetGeneration;
    // This task's wait claim word (FiberTask.armWait). Every waker of any
    // container CASes it; it shares the line with the outbound membership.
    shared ulong waitWord;
    ubyte[24] membershipPadding;
}
static assert(FiberManagedJoinControl.sizeof == 2 * cacheLineSize);
static assert(FiberManagedJoinControl.targetWord.offsetof == cacheLineSize);

align(64) private struct WaitSetControl
{
    shared long waiterCount;
    ubyte[56] padding;
}
static assert(WaitSetControl.sizeof == cacheLineSize);

align(64) private struct PublishDrainControl
{
    shared int gate;
    ubyte[60] padding;
}
static assert(PublishDrainControl.sizeof == cacheLineSize);

align(64) private struct FiberDomainControl
{
    shared int accepting;
    ubyte[60] acceptingPadding;
    shared long activeCount;
    ubyte[56] activePadding;
    shared ulong timerSequence;
    ubyte[56] timerPadding;
    shared int fatalState;
    ubyte[60] fatalPadding;
    shared long waitingCount; // armed, unclaimed waits of every kind
    ubyte[56] waitingPadding;
    shared long lifecycleReservationCount;
    ubyte[56] lifecycleReservationPadding;
    shared size_t cancellationRequestCount;
    ubyte[cacheLineSize - size_t.sizeof] cancellationRequestPadding;
    // Retirement: one gate holder; the backlog count is read by any thread.
    shared int retireGate;
    shared uint retireBacklogCount;
    ubyte[56] retirePadding;
}
static assert(FiberDomainControl.sizeof == 8 * cacheLineSize);
static assert(FiberDomainControl.waitingCount.offsetof
    == 4 * cacheLineSize);
static assert(FiberDomainControl.lifecycleReservationCount.offsetof
    == 5 * cacheLineSize);

align(64) private struct RecordRingControl
{
    shared ulong tail; // next ticket; fetch_add by emitters
    ubyte[56] tailPadding;
    shared ulong head; // next ticket to consume; retirement gate holder
    ubyte[56] headPadding;
}
static assert(RecordRingControl.sizeof == 2 * cacheLineSize);

align(64) private struct FiberReadyLaneControl
{
    shared long publishedCount;
    ubyte[56] publishedPadding;
}
static assert(FiberReadyLaneControl.sizeof == cacheLineSize);

/// A resumable unit. Fiber objects may migrate between worker threads while
/// suspended, as permitted by druntime. Carrying TLS-derived state across a
/// suspension is undefined behavior: callers and their dependencies must not
/// retain TLS addresses, references, cached accesses, or thread-relative
/// resources across scheduler boundaries.
final class FiberTask
{
    private enum uint joinClosed = 1u << 31;
    private enum uint joinWaiterMask = joinClosed - 1;
    private FiberTaskControl* control;
    private FiberJoinControl* joinControl;
    private FiberManagedJoinControl* managedJoinControl;
    package FiberDomain domain;
    package FiberReadyLane readyLane;
    package Fiber fiber;
    version (TSan) private void* tsanContext;
    package Signal waitSignal;
    package shared(Throwable) failure;
    /// Publish chain link; owned by a PublishAccumulator or its drainer.
    package shared(FiberTask) queueNext;
    /// Waiter-queue link (WaitSet bucket, FiberEvent, FiberSemaphore), and
    /// pool freelist link; each container holds a task at disjoint times,
    /// under its own lock. A waker that claimed a detached waiter also uses
    /// it as a private chain until it adds WAKE.
    package FiberTask waitNext;
    /// Waiter-queue predecessor, for O(1) unlink by the waiter itself.
    package FiberTask waitPrev;
    /// The container whose waiter queue links this task, or null. Written
    /// only under that container's lock. The waiter unlinks itself after
    /// every wait, before it can arm another, so a linked task's current
    /// wait word is the one it linked with.
    package Object waitOwner;
    /// Timer heap position, protected by TimerSet.mutex. size_t.max means the
    /// task has no live timer registration.
    package size_t timerIndex = size_t.max;
    /// The task IS its Ant Farm payload: flush publishes this word verbatim.
    package ulong payloadWord;
    /// Requested stack size; recycled tasks keep their original stack.
    package size_t stackSize;
    /// Position in the domain's roots array; maintained under rootsMutex so
    /// completion draining is an O(1) swap-removal per task.
    package size_t rootsIndex;
    /// Double-release guard for the recycling pool (pool mutex held).
    package bool pooledFlag;
    package bool rootedFlag;

    @property package ref shared(ulong) controlWord() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).controlWord; }

    package ulong loadControl() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(controlWord);
    }

    package Phase phase() const nothrow @nogc
    {
        return phaseOf(loadControl());
    }

    /// fetch_add an event addend. Returns the previous word.
    package ulong addCtrl(ulong addend) nothrow @nogc
    {
        debug
        {
            immutable before = ctrlField(loadControl(), addend);
            if (addend == ctrlEnter || addend == ctrlPark || addend == ctrlWake)
                assert(before == 0, "ant_fibers: control field already raised");
            else if (addend == ctrlCancel)
                assert(before < 2,
                    "ant_fibers: more than local+external CANCEL sources");
        }
        return atomicFetchAdd!(MemoryOrder.acq_rel)(controlWord, addend);
    }

    /// Exclusive-owner phase change. Composes with concurrent event adds.
    package ulong addPhase(Phase from, Phase to) nothrow @nogc
    {
        debug assert(phaseOf(loadControl()) == from,
            "ant_fibers: phase add from mismatched current phase");
        immutable delta = (cast(ulong) to << ctrlPhaseShift)
            - (cast(ulong) from << ctrlPhaseShift);
        return atomicFetchAdd!(MemoryOrder.acq_rel)(controlWord, delta);
    }

    /// Recycle/construct only: the slot is unpublished.
    package void storeControl(ulong word) nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(controlWord, word);
    }

    package void subParkWake() nothrow @nogc
    {
        debug
        {
            immutable word = loadControl();
            assert(ctrlField(word, ctrlPark) == 1);
            assert(ctrlField(word, ctrlWake) == 1);
        }
        atomicFetchAdd!(MemoryOrder.acq_rel)(controlWord, 0UL - ctrlParkWake);
    }
    @property package ref shared(ubyte) outcomeState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).outcomeState; }
    @property package ref shared(ulong) generationState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).generationState; }
    /// Greatest generation for which cancellation has been requested. This is
    /// monotonic so a stale request can never clear a newer generation's bit.
    @property package ref shared(ulong) cancelGeneration() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).cancelGeneration; }
    // Opaque identity of the OS thread which most recently entered this
    // fiber. These counters are diagnostics, not scheduling affinity.
    @property package ref shared(ulong) lastResumeIdentityState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).lastResumeIdentityState; }
    @property package ref shared(ulong) resumeCountState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).resumeCountState; }
    @property package ref shared(ulong) migrationCountState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).migrationCountState; }
    @property package ref shared(ubyte) cancellationEventEmitted() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).cancellationEventEmitted; }
    @property package ref shared(ubyte) cancellationDispositionState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).cancellationDispositionState; }
    @property private ref shared(uint) joinState() const return nothrow @nogc
    { return (cast(FiberJoinControl*) joinControl).state; }

    this(FiberDomain domain, FiberReadyLane readyLane,
         void delegate() body, size_t stackSize = 0)
    {
        initializeTask(domain, readyLane, stackSize);
        fiber = stackSize == 0 ? new Fiber(body) : new Fiber(body, stackSize);
    }

    this(FiberDomain domain, FiberReadyLane readyLane,
         void function() body, size_t stackSize = 0)
    {
        initializeTask(domain, readyLane, stackSize);
        fiber = stackSize == 0 ? new Fiber(body) : new Fiber(body, stackSize);
    }

    /// PublishAccumulator stub node: never resumed, never published.
    private this() nothrow @nogc
    {
        initializeControl();
        managedJoinControl = allocateCacheLine!FiberManagedJoinControl();
    }

    ~this() nothrow @nogc
    {
        version (TSan) clearTsanContext();
        if (joinControl !is null)
        {
            joinControl.terminalEvent.terminate();
            freeCacheLine(joinControl);
            joinControl = null;
        }
        freeCacheLine(managedJoinControl);
        managedJoinControl = null;
        freeCacheLine(control);
        control = null;
    }

    version (TSan)
    {
        // LDC 1.43's druntime switches native stacks, but only has ASan hooks.
        // TSan must retain its shadow call stack with the task on migration.
        // Nothing instrumented may run between the TSan and native switches:
        // use druntime's non-template call overload (already compiled without
        // TSan), and keep this bridge uninlined and wholly uninstrumented.
        @llvmAttr("disable_sanitizer_instrumentation")
        pragma(inline, false)
        private Throwable callWithTsan() nothrow @nogc
        {
            if (tsanContext is null) tsanContext = __tsan_create_fiber(0);
            auto caller = __tsan_get_current_fiber();
            // The caller and task run sequentially: carry that ordering across
            // the switch, including the caller's currentTask TLS assignment.
            __tsan_switch_to_fiber(tsanContext, 0);
            // Rethrow.no cannot throw or allocate; the runtime-argument overload
            // cannot express these attributes because Rethrow.yes can throw.
            auto call = cast(Throwable delegate(Fiber.Rethrow) nothrow @nogc)
                &fiber.call;
            auto failure = call(Fiber.Rethrow.no);
            __tsan_switch_to_fiber(caller, 0);
            return failure;
        }

        private void clearTsanContext() nothrow @nogc
        {
            if (tsanContext is null) return;
            __tsan_destroy_fiber(tsanContext);
            tsanContext = null;
        }
    }

    private void initializeControl() nothrow @nogc
    {
        control = allocateCacheLine!FiberTaskControl();
        atomicStore!(MemoryOrder.raw)(control.controlWord, controlWordFor(Phase.ready));
        atomicStore!(MemoryOrder.raw)(control.outcomeState, FiberOutcome.pending);
    }

    private void initializeTask(FiberDomain domain, FiberReadyLane readyLane,
                                size_t stackSize)
    {
        initializeControl();
        initializeJoinControl();
        managedJoinControl = allocateCacheLine!FiberManagedJoinControl();
        this.domain = domain;
        this.readyLane = readyLane;
        this.stackSize = stackSize;
        payloadWord = cast(ulong) cast(void*) this;
    }

    private void initializeJoinControl() nothrow @nogc
    {
        joinControl = allocateCacheLine!FiberJoinControl();
        joinControl.terminalEvent.initialize(true, false);
    }

    private bool registerJoinWaiter(ulong expectedGeneration) nothrow @nogc
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(joinState);
        while (true)
        {
            if ((observed & joinClosed) != 0
                || (observed & joinWaiterMask) == joinWaiterMask)
                return false;
            immutable replacement = observed + 1;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &joinControl.state, observed, replacement))
                break;
            observed = atomicLoad!(MemoryOrder.acq)(joinState);
        }
        if (atomicLoad!(MemoryOrder.acq)(generationState) == expectedGeneration)
            return true;
        unregisterJoinWaiter();
        return false;
    }

    private void unregisterJoinWaiter() nothrow @nogc
    {
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(joinState, 1u);
        assert((previous & joinWaiterMask) != 0);
    }

    private void closeJoinWaiters() nothrow @nogc
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(joinState);
        while ((observed & joinClosed) == 0)
        {
            immutable replacement = observed | joinClosed;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &joinControl.state, observed, replacement))
                return;
            observed = atomicLoad!(MemoryOrder.acq)(joinState);
        }
    }

    private void lockManagedJoinList() nothrow @nogc
    {
        int expected;
        while (!cas!(MemoryOrder.acq, MemoryOrder.raw)(
                &managedJoinControl.gate, expected, 1))
        {
            expected = 0;
            Thread.yield();
        }
    }

    private void unlockManagedJoinList() nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(managedJoinControl.gate, 0);
    }

    private FiberTask managedJoinTarget() nothrow @nogc
    {
        return cast(FiberTask) cast(void*) atomicLoad!(MemoryOrder.acq)(
            managedJoinControl.targetWord);
    }

    private bool registerManagedJoinWaiter(FiberTask waiter,
        ulong expectedGeneration) nothrow @nogc
    {
        lockManagedJoinList();
        scope (exit) unlockManagedJoinList();
        if (atomicLoad!(MemoryOrder.acq)(generationState) != expectedGeneration
            || phaseOf(atomicLoad!(MemoryOrder.acq)(controlWord))
                == Phase.terminated)
            return false;
        // The waiter armed its wait first; this only links it.
        assert(atomicLoad!(MemoryOrder.raw)(
            waiter.managedJoinControl.targetWord) == 0);
        immutable oldHeadWord = atomicLoad!(MemoryOrder.raw)(
            managedJoinControl.headWord);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.targetWord,
            cast(size_t) cast(void*) this);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.targetGeneration,
            expectedGeneration);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.nextWord,
            oldHeadWord);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.previousWord,
            size_t.init);
        if (oldHeadWord != 0)
        {
            auto oldHead = cast(FiberTask) cast(void*) oldHeadWord;
            atomicStore!(MemoryOrder.raw)(
                oldHead.managedJoinControl.previousWord,
                cast(size_t) cast(void*) waiter);
        }
        atomicStore!(MemoryOrder.raw)(managedJoinControl.headWord,
            cast(size_t) cast(void*) waiter);
        atomicFetchAdd!(MemoryOrder.rel)(managedJoinControl.waiterCount, 1u);
        return true;
    }

    private bool removeManagedJoinWaiter(FiberTask waiter) nothrow @nogc
    {
        lockManagedJoinList();
        scope (exit) unlockManagedJoinList();
        if (atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.targetWord)
                != cast(size_t) cast(void*) this
            || atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.targetGeneration) != generation)
            return false;
        immutable nextWord = atomicLoad!(MemoryOrder.raw)(
            waiter.managedJoinControl.nextWord);
        immutable previousWord = atomicLoad!(MemoryOrder.raw)(
            waiter.managedJoinControl.previousWord);
        if (previousWord == 0)
        {
            if (atomicLoad!(MemoryOrder.raw)(managedJoinControl.headWord)
                    != cast(size_t) cast(void*) waiter)
                return false;
            atomicStore!(MemoryOrder.raw)(managedJoinControl.headWord, nextWord);
        }
        else
        {
            auto previous = cast(FiberTask) cast(void*) previousWord;
            atomicStore!(MemoryOrder.raw)(
                previous.managedJoinControl.nextWord, nextWord);
        }
        if (nextWord != 0)
        {
            auto next = cast(FiberTask) cast(void*) nextWord;
            atomicStore!(MemoryOrder.raw)(
                next.managedJoinControl.previousWord, previousWord);
        }
        clearManagedJoinMembership(waiter);
        immutable previousCount = atomicFetchSub!(MemoryOrder.rel)(
            managedJoinControl.waiterCount, 1u);
        assert(previousCount != 0);
        return true;
    }

    private static void clearManagedJoinMembership(FiberTask waiter)
        nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(waiter.managedJoinControl.targetWord,
            size_t.init);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.nextWord,
            size_t.init);
        atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.previousWord,
            size_t.init);
        atomicStore!(MemoryOrder.raw)(
            waiter.managedJoinControl.targetGeneration, 0UL);
    }

    @property bool terminated() const nothrow @nogc
    {
        return phase() == Phase.terminated;
    }

    @property FiberOutcome outcome() const nothrow @nogc
    {
        return cast(FiberOutcome) atomicLoad!(MemoryOrder.acq)(outcomeState);
    }

    /// Non-null only when `outcome == FiberOutcome.failed` and publication of
    /// the terminal state has acquired.
    @property Throwable exception() nothrow @nogc
    {
        if (outcome != FiberOutcome.failed) return null;
        return cast(Throwable) atomicLoad!(MemoryOrder.acq)(failure);
    }

    @property bool cancellationRequested() const nothrow @nogc
    {
        immutable word = loadControl();
        immutable g = generation;
        // Once terminal entry has taken its atomic cutoff, a later external
        // add is counted but semantically ineffective. Only an accepted,
        // generation-tagged request remains observable in that phase.
        if (phaseOf(word) == Phase.terminating
            || phaseOf(word) == Phase.terminated)
            return g != 0
                && atomicLoad!(MemoryOrder.acq)(cancelGeneration) == g;
        if ((word & ctrlCancelMask) != 0)
            return true;
        return g != 0
            && atomicLoad!(MemoryOrder.acq)(cancelGeneration) == g;
    }

    @property CancellationDisposition cancellationDisposition() const
        nothrow @nogc
    {
        return cast(CancellationDisposition) atomicLoad!(MemoryOrder.acq)(
            cancellationDispositionState);
    }

    @property ulong generation() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(generationState);
    }

    /// Capture this logical generation. The raw FiberTask remains recyclable;
    /// callers retaining identity across release should retain this value.
    @property TaskHandle handle() nothrow @nogc
    {
        immutable g = generation;
        return TaskHandle(this, g, taskDiagnosticId(this, g));
    }

    /// Number of successful scheduler entries, including the first one.
    @property ulong resumeCount() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(resumeCountState);
    }

    /// Number of entries whose OS-thread identity differed from the previous
    /// entry. This is observational only: Farm tasks have no thread affinity.
    @property ulong migrationCount() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(migrationCountState);
    }

    /// Opaque identity of the OS thread which most recently resumed the task.
    @property ulong lastResumeIdentity() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(lastResumeIdentityState);
    }

    @property private ref shared(ulong) waitWord() const return nothrow @nogc
    { return (cast(FiberManagedJoinControl*) managedJoinControl).waitWord; }

    /// Running fiber only: start a wait and return its armed word. Exactly
    /// one `claimWait` on that word succeeds, and only its winner adds WAKE,
    /// so containers, timers and cancellation need no removal protocol among
    /// themselves. The count in the high bits never resets (not even on
    /// recycling), so a stale container entry or timer can never claim a
    /// later wait. The fence pairs with the one cancellation issues between
    /// its CANCEL add and its claim: either the canceller claims this wait,
    /// or the caller's next `cancellationRequested` check sees the request.
    package ulong armWait() nothrow @nogc
    {
        immutable old = atomicLoad!(MemoryOrder.raw)(waitWord);
        assert((old & waitCauseMask) != WaitCause.armed);
        immutable word = ((old & ~waitCauseMask) + (1UL << waitCauseBits))
            | WaitCause.armed;
        addPhase(Phase.running, Phase.waiting);
        atomicFetchAdd!(MemoryOrder.rel)(domain.waitingCount, 1L);
        atomicStore!(MemoryOrder.rel)(waitWord, word);
        atomicFence!(MemoryOrder.seq)();
        return word;
    }

    /// Any waker: claim the wait `armed` names. True means the caller owns
    /// the wait's single WAKE and must deliver it (`makeWaiterReady`).
    package bool claimWait(ulong armed, WaitCause cause) nothrow @nogc
    {
        assert((armed & waitCauseMask) == WaitCause.armed);
        auto expected = armed;
        if (!cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&managedJoinControl.waitWord, expected,
                (armed & ~waitCauseMask) | cause))
            return false;
        atomicFetchSub!(MemoryOrder.rel)(domain.waitingCount, 1L);
        return true;
    }

    /// Claim whatever wait is armed now. Used under a container lock (where
    /// a linked task's current wait is its linked one) and by cancellation.
    package bool claimCurrentWait(WaitCause cause) nothrow @nogc
    {
        immutable word = atomicLoad!(MemoryOrder.acq)(waitWord);
        return (word & waitCauseMask) == WaitCause.armed
            && claimWait(word, cause);
    }

    /// The cause that ended the most recent wait; the waiter reads it after
    /// it resumes.
    @property package WaitCause waitCause() const nothrow @nogc
    {
        return cast(WaitCause)(atomicLoad!(MemoryOrder.acq)(waitWord)
            & waitCauseMask);
    }

    /// Running fiber only: end the armed wait `armed` without parking. If a
    /// waker claimed it first, consume that waker's WAKE by suspending once.
    /// Either way the task is running again with no wake outstanding. Returns
    /// true when a waker had claimed the wait.
    package bool withdrawWait(ulong armed)
    {
        if (claimWait(armed, WaitCause.withdrawn))
        {
            addPhase(Phase.waiting, Phase.running);
            return false;
        }
        Fiber.yield();
        return true;
    }
}

/// Intrusive FIFO of waiting tasks through waitNext/waitPrev. The owning
/// container serializes every operation with its lock; `owner` tags each
/// member so the waiter can unlink itself after any wake.
private struct WaiterQueue
{
    FiberTask head;
    FiberTask tail;
    size_t length;

    void pushBack(FiberTask task, Object owner) nothrow @nogc
    {
        assert(task.waitOwner is null);
        task.waitOwner = owner;
        task.waitNext = null;
        task.waitPrev = tail;
        if (tail !is null) tail.waitNext = task;
        else head = task;
        tail = task;
        ++length;
    }

    void unlink(FiberTask task) nothrow @nogc
    {
        auto prev = task.waitPrev;
        auto next = task.waitNext;
        if (prev !is null) prev.waitNext = next;
        else head = next;
        if (next !is null) next.waitPrev = prev;
        else tail = prev;
        task.waitNext = null;
        task.waitPrev = null;
        task.waitOwner = null;
        --length;
    }

    /// Unlink members oldest first until `limit` claims succeed. Members that
    /// another waker already claimed are dropped: their own resume finds them
    /// unlinked. Winners return as a chain through waitNext; each stays
    /// suspended until its WAKE, so the chain is stable after the lock drops.
    FiberTask claimFront(size_t limit, out size_t claimed) nothrow @nogc
    {
        FiberTask first, last;
        while (claimed < limit && head !is null)
        {
            auto task = head;
            unlink(task);
            if (!task.claimCurrentWait(WaitCause.signalled)) continue;
            if (last !is null) last.waitNext = task;
            else first = task;
            last = task;
            ++claimed;
        }
        return first;
    }
}

/// Fibers parked on application signals (`FiberDomain.await`). Waiters of
/// one signal share a bucket in a mutex-striped map; FiberEvent and
/// FiberSemaphore keep their own queues instead. The mutexes are deliberately
/// off the Ant Farm hot path: waits and notifications are scheduler events.
final class WaitSet
{
    private enum stripeBits = 6;
    private enum stripeCount = 1 << stripeBits;

    private Mutex[stripeCount] mutexes;
    private WaiterQueue[Signal][stripeCount] buckets;

    this()
    {
        foreach (ref m; mutexes) m = new Mutex;
    }

    private static size_t stripe(Signal signal) nothrow @nogc
    {
        return cast(size_t)(signal * 0x9E37_79B9_7F4A_7C15UL) >> (64 - stripeBits);
    }

    /// Link the running `task` under `task.waitSignal` and arm its wait
    /// in one step under the stripe lock, returning the armed word. A signal
    /// is an edge with no state to recheck, so the wait must not count in
    /// `FiberDomain.waiting` before delivery can find it: a caller that waits
    /// for that count and then signals would otherwise lose the signal. The
    /// bucket exists before the task is tagged, so a failed allocation leaves
    /// nothing linked or armed.
    package ulong linkArmed(FiberTask task)
    {
        immutable signal = task.waitSignal;
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            auto p = signal in buckets[s];
            if (p is null)
            {
                buckets[s][signal] = WaiterQueue.init;
                p = signal in buckets[s];
            }
            p.pushBack(task, this);
            return task.armWait();
        }
    }

    /// Claim every waiter of `signal`. Each is unlinked under the stripe lock,
    /// so a waiter woken by another cause (which unlinks itself) never edits
    /// the returned chain, and a later wait of the same signal starts a new
    /// bucket. `count` receives the number of claims won.
    package FiberTask claimAll(Signal signal, out size_t count)
    {
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            auto p = signal in buckets[s];
            if (p is null) return null;
            auto chain = p.claimFront(size_t.max, count);
            buckets[s].remove(signal);
            return chain;
        }
    }

    /// The waiter itself, after it resumes: unlink if a signal did not.
    package void unlink(FiberTask task)
    {
        immutable signal = task.waitSignal;
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            if (task.waitOwner !is this) return;
            auto p = signal in buckets[s];
            assert(p !is null);
            p.unlink(task);
            if (p.length == 0) buckets[s].remove(signal);
        }
    }
}

private struct TimerEntry
{
    long deadlineTicks;
    ulong sequence;
    FiberTask task;
    ulong waitWord; // the armed wait this timer may claim
}

/// Managed timer registry. Expiration is polled by lane workers; the nearest
/// deadline is handed to threadpool so an idle worker wakes precisely for it.
final class TimerSet
{
    private Mutex mutex;
    // The heap is storage[0 .. count]. An explicit count keeps the block
    // across removals; shrinking a slice would make the next append copy the
    // entire heap into a new block.
    private TimerEntry[] storage;
    private size_t count;

    @property private inout(TimerEntry)[] entries() inout return nothrow @nogc
    {
        return storage[0 .. count];
    }
    // The heap top's deadline, long.max when empty. Written under the mutex
    // whenever the top can change; read without it so an idle poll and the
    // pre-park deadline query take no lock. A stale read races exactly as
    // the locked read did: a later top only wakes early, and an insert that
    // becomes the top after the read raises `noteTimerArmed`.
    private shared long earliestTicks = long.max;

    private void publishEarliest() nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(earliestTicks,
            entries.length != 0 ? entries[0].deadlineTicks : long.max);
    }

    /// The earliest armed deadline, or long.max; lock-free and possibly stale.
    @property long earliest() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(earliestTicks);
    }

    this() { mutex = new Mutex; }

    private static bool before(ref const TimerEntry a, ref const TimerEntry b)
        nothrow @nogc
    {
        return a.deadlineTicks < b.deadlineTicks
            || (a.deadlineTicks == b.deadlineTicks && a.sequence < b.sequence);
    }

    private void swapEntries(size_t a, size_t b) nothrow @nogc
    {
        auto tmp = entries[a];
        entries[a] = entries[b];
        entries[b] = tmp;
        entries[a].task.timerIndex = a;
        entries[b].task.timerIndex = b;
    }

    private void siftUp(size_t i) nothrow @nogc
    {
        while (i != 0)
        {
            immutable parent = (i - 1) / 2;
            if (!before(entries[i], entries[parent])) break;
            swapEntries(i, parent);
            i = parent;
        }
    }

    private void siftDown(size_t i) nothrow @nogc
    {
        while (true)
        {
            immutable left = i * 2 + 1;
            if (left >= entries.length) break;
            immutable right = left + 1;
            size_t child = left;
            if (right < entries.length && before(entries[right], entries[left]))
                child = right;
            if (!before(entries[child], entries[i])) break;
            swapEntries(i, child);
            i = child;
        }
    }

    /// Returns true when the new entry became the earliest deadline.
    bool insert(FiberTask task, long deadlineTicks, ulong sequence,
        ulong waitWord)
    {
        synchronized (mutex)
        {
            assert(task.timerIndex == size_t.max);
            if (count == storage.length)
                storage.length = count < 8 ? 8 : count * 2;
            task.timerIndex = count;
            storage[count++] = TimerEntry(deadlineTicks, sequence, task,
                waitWord);
            siftUp(task.timerIndex);
            publishEarliest();
            return task.timerIndex == 0;
        }
    }

    private FiberTask removeAt(size_t i) nothrow @nogc
    {
        auto removed = entries[i].task;
        immutable last = entries.length - 1;
        if (i != last)
        {
            entries[i] = entries[last];
            entries[i].task.timerIndex = i;
        }
        storage[last] = TimerEntry.init; // the GC scans the whole block
        count = last;
        removed.timerIndex = size_t.max;
        if (i < entries.length)
        {
            if (i != 0 && before(entries[i], entries[(i - 1) / 2]))
                siftUp(i);
            else
                siftDown(i);
        }
        publishEarliest();
        return removed;
    }

    bool remove(FiberTask task) nothrow @nogc
    {
        mutex.lock_nothrow();
        scope (exit) mutex.unlock_nothrow();
        immutable i = task.timerIndex;
        if (i >= entries.length || entries[i].task !is task) return false;
        removeAt(i);
        return true;
    }

    /// Pop the earliest entry if it has expired. The caller claims
    /// `waitWord` after the mutex drops: the word names the exact wait this
    /// timer was armed for, so a claim that lost to another waker (whose
    /// waiter may already wait again) fails instead of ending a later wait.
    bool takeExpired(long nowTicks, out FiberTask task, out ulong waitWord)
        nothrow @nogc
    {
        mutex.lock_nothrow();
        scope (exit) mutex.unlock_nothrow();
        if (count == 0 || storage[0].deadlineTicks > nowTicks)
            return false;
        waitWord = storage[0].waitWord;
        task = removeAt(0);
        return true;
    }

    bool nextDeadline(out long ticks) nothrow @nogc
    {
        ticks = earliest;
        return ticks != long.max;
    }
}

/// FIFO chain detached from a PublishAccumulator. Nodes are linked
/// oldest-first through queueNext and exclusively owned by the holder until
/// requeued with putBackChain.
package struct PublishChain
{
    FiberTask first;
    FiberTask last;
    size_t length;
}

/// Intrusive MPSC staging area used to amortize Ant Farm publication. Push is
/// a single atomic exchange plus a release store: no allocation, no capacity
/// failure, no CAS retry loop. Draining is gated to one consumer at a time
/// (a contended drain is a cheap empty result, not a spin) and unlinks nodes
/// with plain loads/stores once the chain is private.
final class PublishAccumulator
{
    private PublishDrainControl* drainControl;
    private shared(FiberTask) head;
    private FiberTask tail; // consumer-side marker; drain gate held
    private FiberTask stub; // dedicated empty-queue marker node
    private shared long queued;

    @property private ref shared(int) drainGate() const return nothrow @nogc
    { return (cast(PublishDrainControl*) drainControl).gate; }

    this()
    {
        drainControl = allocateCacheLine!PublishDrainControl();
        stub = new FiberTask;
        tail = stub;
        atomicStore!(MemoryOrder.raw)(head, cast(shared(FiberTask)) stub);
    }

    ~this() nothrow @nogc { freeCacheLine(drainControl); }

    void push(FiberTask task) nothrow @nogc
    {
        // Count before the node becomes visible so a fast drainer can never
        // pull `queued` negative; a transient overcount is harmless.
        atomicFetchAdd!(MemoryOrder.rel)(queued, 1L);
        atomicStore!(MemoryOrder.raw)(task.queueNext, cast(shared(FiberTask)) null);
        auto prev = cast(FiberTask) atomicExchange!(MemoryOrder.acq_rel)(
            &head, cast(shared(FiberTask)) task);
        atomicStore!(MemoryOrder.rel)(prev.queueNext, cast(shared(FiberTask)) task);
    }

    /// Detach up to `maximum` tasks in FIFO order. Returns empty when the
    /// queue is empty, when another consumer holds the drain gate, or when a
    /// producer's link store is still in flight; all three resolve by
    /// retrying.
    package PublishChain stealChain(size_t maximum) nothrow @nogc
    {
        PublishChain chain;
        if (maximum == 0) return chain;
        int expected = 0;
        if (!cas!(MemoryOrder.acq, MemoryOrder.raw)(
                &drainControl.gate, expected, 1))
            return chain;
        scope (exit) atomicStore!(MemoryOrder.rel)(drainGate, 0);
        while (chain.length < maximum)
        {
            auto t = tail;
            auto next = cast(FiberTask) atomicLoad!(MemoryOrder.acq)(t.queueNext);
            if (t is stub)
            {
                if (next is null) break; // empty
                tail = next;
                t = next;
                next = cast(FiberTask) atomicLoad!(MemoryOrder.acq)(t.queueNext);
            }
            if (next is null)
            {
                // t may be the final node, or its producer has exchanged head
                // but not yet linked t.queueNext.
                if (t !is cast(FiberTask) atomicLoad!(MemoryOrder.acq)(head))
                    break; // link in flight; retry on a later drain
                // Rotate the stub through head to detach the final node.
                atomicStore!(MemoryOrder.raw)(stub.queueNext, cast(shared(FiberTask)) null);
                auto prev = cast(FiberTask) atomicExchange!(MemoryOrder.acq_rel)(
                    &head, cast(shared(FiberTask)) stub);
                atomicStore!(MemoryOrder.rel)(prev.queueNext, cast(shared(FiberTask)) stub);
                next = cast(FiberTask) atomicLoad!(MemoryOrder.acq)(t.queueNext);
                if (next is null) break; // unreachable; stay consistent
            }
            tail = next;
            atomicStore!(MemoryOrder.raw)(t.queueNext, cast(shared(FiberTask)) null);
            if (chain.first is null)
                chain.first = t;
            else
                atomicStore!(MemoryOrder.raw)(chain.last.queueNext, cast(shared(FiberTask)) t);
            chain.last = t;
            ++chain.length;
        }
        if (chain.length != 0)
            atomicFetchSub!(MemoryOrder.rel)(queued, cast(long) chain.length);
        return chain;
    }

    /// Requeue a previously stolen chain (or its unpublished suffix) at the
    /// newest end, preserving order. One atomic exchange for the whole chain.
    package void putBackChain(FiberTask first, FiberTask last, size_t count) nothrow @nogc
    {
        if (first is null) return;
        atomicFetchAdd!(MemoryOrder.rel)(queued, cast(long) count);
        atomicStore!(MemoryOrder.raw)(last.queueNext, cast(shared(FiberTask)) null);
        auto prev = cast(FiberTask) atomicExchange!(MemoryOrder.acq_rel)(
            &head, cast(shared(FiberTask)) last);
        atomicStore!(MemoryOrder.rel)(prev.queueNext, cast(shared(FiberTask)) first);
    }

    @property size_t length() nothrow @nogc
    {
        immutable n = atomicLoad!(MemoryOrder.acq)(queued);
        return n < 0 ? 0 : cast(size_t) n;
    }
}

/// Forward range of one-word bodies over a detached snapshot of stolen tasks.
/// It is passed to Ant Farm's common-header/fixed-length write overload, so
/// sizing is arithmetic and does not evaluate each task body.
private struct TaskBodySnapshotRange
{
    private FiberTask[] tasks;

    @property bool empty() nothrow @nogc
    {
        return tasks.length == 0;
    }

    @property PayloadBody front() nothrow @nogc @system
    {
        return (&tasks[0].payloadWord)[0 .. 1];
    }

    void popFront() nothrow @nogc
    {
        tasks = tasks[1 .. $];
    }

    @property TaskBodySnapshotRange save() nothrow @nogc @system
    {
        return this;
    }

    @property size_t length() const pure nothrow @nogc @safe
    {
        return tasks.length;
    }
}

/// Ordered, allocation-free MPSC publication for the task's four reusable
/// lifecycle nodes. The aligned head is a raw word; task roots remain installed
/// until every emitted node and the completion have both been drained.
version (unittest)
    /// Scheduling seam: runs on the emitting thread after it takes a record
    /// ticket and before it publishes the record.
    package __gshared void function(FiberLifecycleKind) nothrow @nogc
        recordAfterTicketHook;

/// One lifecycle record by value. The failure reference lives in the ring's
/// separate scanned array; this struct sits in unscanned memory.
private struct RecordSlot
{
    shared ulong stamp; // lap marker: `t` free for ticket t, `t + 1` published
    FiberLifecycleKind kind;
    FiberOutcome outcome;
    CancellationResult cancellation;
    CancellationDisposition disposition;
    TaskHandle task;
}

/// Bounded multi-producer, single-consumer ring of lifecycle records, with
/// the per-slot stamps of a bounded MPMC queue. Emitters never see it full:
/// admission reserves every record's slot in the domain's budget first, so
/// a ticket's slot was consumed a lap earlier. The stamp acquire orders that
/// consumption's clearing before the emitter's writes.
///
/// Record data is in a NO_SCAN block so a large ring adds nothing to marking.
/// Its `TaskHandle` points at a task object the domain retains (roots or
/// recycle pool) for its whole lifetime; only `failures` needs scanning.
private final class RecordRing
{
    private RecordRingControl* control;
    private RecordSlot* slots;
    private Throwable[] failures;
    private size_t mask;

    this(size_t capacity)
    {
        assert(capacity != 0 && (capacity & (capacity - 1)) == 0);
        control = allocateCacheLine!RecordRingControl();
        slots = cast(RecordSlot*) GC.calloc(capacity * RecordSlot.sizeof,
            GC.BlkAttr.NO_SCAN);
        foreach (i; 0 .. capacity)
            atomicStore!(MemoryOrder.raw)(slots[i].stamp, cast(ulong) i);
        failures = new Throwable[capacity];
        mask = capacity - 1;
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

    @property size_t capacity() const nothrow @nogc { return mask + 1; }

    void emit(FiberLifecycleKind kind, TaskHandle task, FiberOutcome outcome,
        CancellationResult cancellation, CancellationDisposition disposition,
        Throwable failure) nothrow @nogc
    {
        immutable t = atomicFetchAdd!(MemoryOrder.raw)(control.tail, 1UL);
        version (unittest)
            if (recordAfterTicketHook !is null)
                recordAfterTicketHook(kind);
        auto slot = &slots[t & mask];
        // The previous lap was consumed (reservations bound the tickets in
        // flight); this waits out only its stamp store.
        while (atomicLoad!(MemoryOrder.acq)(slot.stamp) != t)
            Thread.yield();
        slot.kind = kind;
        slot.outcome = outcome;
        slot.cancellation = cancellation;
        slot.disposition = disposition;
        slot.task = task;
        failures[t & mask] = failure;
        atomicStore!(MemoryOrder.rel)(slot.stamp, t + 1);
    }

    /// Consumer: copy the oldest record if it is published. False at an empty
    /// ring or at a slot whose emitter has its ticket but has not published.
    bool front(out FiberLifecycleEvent event) nothrow @nogc
    {
        immutable h = atomicLoad!(MemoryOrder.raw)(control.head);
        auto slot = &slots[h & mask];
        if (atomicLoad!(MemoryOrder.acq)(slot.stamp) != h + 1)
            return false;
        event = FiberLifecycleEvent(h + 1, slot.kind, slot.task, slot.outcome,
            slot.cancellation, slot.disposition, failures[h & mask]);
        return true;
    }

    /// Consumer: retire the record `front` returned and free its slot for
    /// the next lap.
    void popFront() nothrow @nogc
    {
        immutable h = atomicLoad!(MemoryOrder.raw)(control.head);
        auto slot = &slots[h & mask];
        slot.task = TaskHandle.init;
        failures[h & mask] = null; // the ring must not retain exceptions
        atomicStore!(MemoryOrder.rel)(slot.stamp, h + capacity);
        atomicStore!(MemoryOrder.rel)(control.head, h + 1);
    }

    /// Records ticketed and not yet consumed, including ones still being
    /// written.
    @property size_t length() const nothrow @nogc
    {
        immutable h = atomicLoad!(MemoryOrder.acq)(control.head);
        immutable t = atomicLoad!(MemoryOrder.acq)(control.tail);
        return cast(size_t)(t - h);
    }
}

/// One ready-work transport owned by an LLC-local scheduler lane. A lane owns
/// its Farm and accumulation queue; it does not own task generations, waits,
/// timers, roots, completions, or lifecycle records.
class FiberReadyLane
{
    package AntFarm* farm;
    package FiberDomain ownerDomain;
    private FiberReadyLaneControl* readyControl;
    private PublishAccumulator publishable;
    private Callback resumeHeader;
    private enum snapshotCap = 256;

    this(AntFarm* farm)
    {
        if (farm is null)
            throw new Exception("ant_fibers: ready lane requires a farm");
        this.farm = farm;
        readyControl = allocateCacheLine!FiberReadyLaneControl();
        publishable = new PublishAccumulator;

        resumeHeader = &resumeCallback;
    }

    ~this() nothrow @nogc { freeCacheLine(readyControl); }

    package void push(FiberTask task) nothrow @nogc
    {
        assert(task !is null && task.readyLane is this);
        publishable.push(task);
    }

    /// Called when readiness originates outside the lane's current worker
    /// flow (signal, timer, cancellation, or a cross-lane join completion).
    /// Threadpool lanes override this to wake their own responder set.
    protected void noteExternalReady() nothrow @nogc {}

    /// Queued cancellation must wake domain service even when an embedding
    /// delegates ordinary readiness wakes to a control thread.
    protected void noteCancellationRequested() nothrow @nogc {}

    /// Called by a running fiber of this lane after it armed its domain's
    /// earliest timer deadline. No ready transition happens until the timer
    /// is polled, so threadpool lanes override this to wake a worker which
    /// polls the domain's timers.
    protected void noteTimerArmed() nothrow @nogc {}

    /// Retirement is pending under `FiberRetirement.director` on a thread
    /// that may not retire. No worker is woken for it; the Director polls
    /// `retirementPending`. A lane may record it for diagnostics.
    protected void noteRetirementPending() nothrow @nogc {}

    private enum size_t retirePerPass = 256;

    /// One service pass of this lane by a thread holding `token` on its Farm.
    /// In order: apply queued cancellation requests, poll timers (responders),
    /// deliver generation triggers, flush ready tasks, retire (when this
    /// thread may: `serviceThreads`, or the Director), and report the
    /// earliest deadline (responders). It never consumes Farm work: a host's
    /// loop is `service`, consume its views, and park until `deadline` when
    /// neither made progress. Throws the domain's fatal failure.
    FiberServiceResult serve(ref Token token,
        FiberServicePass pass = FiberServicePass.init)
    {
        if (pass.flushBatch == 0)
            throw new Exception("ant_fibers: flushBatch must be greater than zero");
        auto domain = ownerDomain;
        FiberServiceResult result;
        if (domain.pendingCancellationRequests != 0
            && domain.drainCancellationRequestsCounted() != 0)
            result.progress = true;
        if (pass.responder && domain.pollTimers() != 0)
            result.progress = true;
        if (domain.pollGenerationTriggers() != 0)
            result.progress = true;
        if (domain.fatal)
            throw domain.fatalException;
        // Accumulate while Farm work flows: flush a full batch, or a short
        // tail when this thread would otherwise idle.
        immutable waiting = ready;
        if (waiting >= pass.flushBatch || (!pass.consumedWork && waiting != 0))
            result.flushed = flush(token, pass.flushBatch, pass.avgCost);
        if (result.flushed != 0)
            result.progress = true;
        if (domain.retirementPending)
        {
            if (domain.options_.retirement == FiberRetirement.serviceThreads
                || domain.isDirectorThread)
            {
                size_t recordsRetired;
                if (domain.retireUnchecked(retirePerPass, recordsRetired) != 0
                    || recordsRetired != 0)
                    result.progress = true;
            }
            else
                noteRetirementPending();
        }
        long deadline;
        if (pass.responder && domain.nextTimerDeadline(deadline))
            result.deadline = deadline;
        return result;
    }

    /// Publish up to `maximum` accumulated activations using a caller-owned
    /// producer token for this lane's Farm.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = flushAvgCost)
    {
        if (maximum > snapshotCap) maximum = snapshotCap;
        size_t written;
        auto chain = publishable.stealChain(maximum);
        if (chain.first !is null)
        {
            FiberTask[snapshotCap] nodes = void;
            size_t n;
            for (auto task = chain.first; task !is null; ++n)
            {
                assert(n < snapshotCap && task.readyLane is this);
                nodes[n] = task;
                auto next = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
                    task.queueNext);
                atomicStore!(MemoryOrder.raw)(task.queueNext,
                    cast(shared(FiberTask)) null);
                task = next;
            }
            auto bodies = TaskBodySnapshotRange(nodes[0 .. n]);
            // Count before publishing: a consumer may enter an activation
            // (noteEntered) before write() returns. Counting afterwards let
            // that decrement run first and drive the count negative. A
            // transient overcount only makes `published` conservative.
            notePublished(n);
            written = cast(size_t) farm.write(
                resumeHeader, bodies, 1, token, avgCost);
            if (written < n)
            {
                foreach (i; written .. n - 1)
                    atomicStore!(MemoryOrder.raw)(nodes[i].queueNext,
                        cast(shared(FiberTask)) nodes[i + 1]);
                publishable.putBackChain(nodes[written], nodes[n - 1], n - written);
                retractPublished(n - written);
            }
        }
        return written;
    }

    @property size_t ready() nothrow @nogc { return publishable.length; }

    /// Activations written to this lane's Farm and not yet entered. A hole
    /// (`consumeNext` false) is not emptiness while this is nonzero.
    @property size_t published() nothrow @nogc
    {
        immutable n = atomicLoad!(MemoryOrder.acq)(readyControl.publishedCount);
        return n < 0 ? 0 : cast(size_t) n;
    }

    private void notePublished(size_t count) nothrow @nogc
    {
        atomicFetchAdd!(MemoryOrder.rel)(
            readyControl.publishedCount, cast(long) count);
    }

    private void retractPublished(size_t count) nothrow @nogc
    {
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
            readyControl.publishedCount, cast(long) count);
        assert(previous >= cast(long) count);
    }

    package void noteEntered() nothrow @nogc
    {
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
            readyControl.publishedCount, 1L);
        assert(previous > 0);
    }

    private static extern(C) void resumeCallback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
    {
        auto body = bodyPtr[0 .. bodyWords];
        if (body.length != 1) return;
        auto task = cast(FiberTask) cast(void*) body[0];
        task.readyLane.noteEntered();
        task.domain.resume(task);
        return;
    }

}


/// Owns task generations, roots, waits, timers, cancellation, completion, and
/// lifecycle delivery across one or more ready lanes. Worker creation,
/// affinity, Farm consumption, cadence, and parking remain the embedding's
/// responsibility.
final class FiberDomain
{
    private FiberDomainControl* control;
    private WaitSet waits;
    private TimerSet timers;
    private FiberReadyLane[] readyLanes;
    private FiberReadyLane defaultReadyLane;
    private shared(FiberGenerationTrigger) deferredTriggerHead;
    private PublishAccumulator completions;
    // Completions taken but not yet retired after a throwing handler: a FIFO
    // through queueNext, owned by the retirement gate holder.
    private FiberTask retireBacklogFirst;
    private FiberTask retireBacklogLast;
    private RecordRing records;
    private FiberDomainOptions options_; // set once by the constructor
    private shared(Throwable) fatalFailure;
    private Mutex admissionMutex;
    private Mutex directorMutex;
    private Thread directorThread;
    private Mutex cancellationRequestMutex;
    private TaskHandle[] cancellationRequests;
    private Mutex rootsMutex;
    private FiberTask[] roots; // keep tasks alive while only ring pointers exist
    private bool domainRooted; // roots this owner while roots[] is nonempty
    private Mutex poolMutex;
    private FiberTask[size_t] pool; // stack size → freelist of terminated tasks
    private static FiberTask currentTask;

    @property private ref shared(int) accepting() const return nothrow @nogc
    { return (cast(FiberDomainControl*) control).accepting; }
    @property private ref shared(long) activeCount() const return nothrow @nogc
    { return (cast(FiberDomainControl*) control).activeCount; }
    @property private ref shared(ulong) timerSequence() const return nothrow @nogc
    { return (cast(FiberDomainControl*) control).timerSequence; }
    @property private ref shared(int) fatalState() const return nothrow @nogc
    { return (cast(FiberDomainControl*) control).fatalState; }
    @property private ref shared(long) waitingCount() const
        return nothrow @nogc
    { return (cast(FiberDomainControl*) control).waitingCount; }
    @property private ref shared(long) lifecycleReservationCount() const
        return nothrow @nogc
    { return (cast(FiberDomainControl*) control).lifecycleReservationCount; }
    @property private ref shared(int) retireGate() const return nothrow @nogc
    { return (cast(FiberDomainControl*) control).retireGate; }
    @property private ref shared(uint) retireBacklogCount() const
        return nothrow @nogc
    { return (cast(FiberDomainControl*) control).retireBacklogCount; }

    /// A domain without a lane; lanes attach before admission. The
    /// constructing thread is the Director.
    this(FiberDomainOptions options = FiberDomainOptions.init)
    {
        if (options.recordRetention < lifecycleRecordsPerTask
            || options.recordRetention > (size_t(1) << 40))
            throw new Exception(
                "ant_fibers: recordRetention must be 4 .. 2^40 records");
        options_ = options;
        size_t capacity = 1;
        while (capacity < options.recordRetention) capacity <<= 1;
        records = new RecordRing(capacity);
        control = allocateCacheLine!FiberDomainControl();
        atomicStore!(MemoryOrder.raw)(control.accepting, 1);
        directorMutex = new Mutex;
        directorThread = Thread.getThis();
        cancellationRequestMutex = new Mutex;
        waits = new WaitSet;
        timers = new TimerSet;
        completions = new PublishAccumulator;
        admissionMutex = new Mutex;
        rootsMutex = new Mutex;
        poolMutex = new Mutex;
    }

    /// A one-lane domain over `farm`.
    this(AntFarm* farm, FiberDomainOptions options = FiberDomainOptions.init)
    {
        this(options);
        defaultReadyLane = new FiberReadyLane(farm);
        attachReadyLane(defaultReadyLane);
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

    /// Attach a ready lane during setup, before admission. The first lane is
    /// the default used by `spawn`, `flush` and `serve`; multi-lane
    /// embeddings (`ant_fibers.groups`, or a custom one with a
    /// `FiberReadyLane` per Farm) spawn on and serve the selected lane.
    void attachReadyLane(FiberReadyLane lane)
    {
        if (lane is null)
            throw new Exception("ant_fibers: cannot attach a null lane");
        synchronized (admissionMutex)
        {
            if (active != 0 || roots.length != 0)
                throw new Exception(
                    "ant_fibers: attach ready lanes before spawning");
            if (lane.ownerDomain !is null && lane.ownerDomain !is this)
                throw new Exception(
                    "ant_fibers: ready lane already belongs to a domain");
            foreach (existing; readyLanes)
                if (existing is lane) return;
            lane.ownerDomain = this;
            readyLanes ~= lane;
            if (defaultReadyLane is null) defaultReadyLane = lane;
        }
    }

    /// Pre-size the GC roots array so a known spawn burst does not realloc it.
    void reserve(size_t n)
    {
        synchronized (rootsMutex) roots.reserve(n);
    }

    @property size_t laneCount() const nothrow @nogc
    {
        return readyLanes.length;
    }

    package FiberReadyLane laneFor(TaskHandle handle) nothrow @nogc
    {
        auto task = handle.slot;
        return task !is null && task.domain is this
            && task.generation == handle.generation
            ? task.readyLane : null;
    }

    /// Enable ordered lifecycle records with a hard reservation bound.
    /// Setup-time only, before any spawn. Each admitted task reserves its
    /// maximum four records until records are acknowledged and unused terminal
    /// capacity is returned.
    /// This domain's construction-time configuration.
    @property FiberDomainOptions options() nothrow @nogc
    {
        return options_;
    }

    /// Lifecycle records published or being written and not yet retired.
    @property size_t pendingRecords() const nothrow @nogc
    {
        return records.length;
    }

    /// Record slots reserved by admitted tasks or held by unretired records;
    /// spawn refuses past `options.recordRetention`.
    @property size_t reservedRecords() const nothrow @nogc
    {
        immutable count = atomicLoad!(MemoryOrder.acq)(
            lifecycleReservationCount);
        return count < 0 ? 0 : cast(size_t) count;
    }

    /// Terminal tasks not yet retired.
    @property size_t pendingCompletions() nothrow @nogc
    {
        return completions.length
            + atomicLoad!(MemoryOrder.acq)(retireBacklogCount);
    }

    /// Records or terminal tasks await `retire`.
    @property bool retirementPending() nothrow @nogc
    {
        return pendingRecords != 0 || pendingCompletions != 0;
    }

    @property bool fatal() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(fatalState) != 0;
    }

    @property Throwable fatalException() const nothrow @nogc
    {
        if (!fatal) return null;
        return cast(Throwable) atomicLoad!(MemoryOrder.acq)(fatalFailure);
    }

    /// Spawn reuses a released task of matching stack size when the pool has
    /// one: no GC allocation, no fiber stack mmap. The recycled task's Fiber
    /// is reset in place, so it retains (and is bounded by) its original
    /// stack; stackSize only steers pool matching on the reuse path.
    FiberTask spawn(void delegate() body, size_t stackSize = 0)
    {
        return spawnOn(defaultReadyLane, body, stackSize);
    }

    /// Function-pointer form avoids constructing a delegate or closure for a
    /// module-level/static body.
    FiberTask spawn(void function() body, size_t stackSize = 0)
    {
        return spawnOn(defaultReadyLane, body, stackSize);
    }

    /// Admit a task whose runnable activations prefer `lane`.
    /// Admit a task whose runnable activations prefer `lane`, one of this
    /// domain's attached lanes.
    FiberTask spawnOn(Body)(FiberReadyLane lane, Body body,
                            size_t stackSize = 0)
        if (is(Body : void delegate()) || is(Body : void function()))
    {
        TaskHandle unused;
        return admit(lane, body, stackSize, unused);
    }

    // `handle` is captured before publication: under serviceThreads
    // retirement the task can terminate and be recycled before admit returns.
    private FiberTask admit(Body)(FiberReadyLane lane, Body body,
        size_t stackSize, out TaskHandle handle)
    {
        if (lane is null || lane.ownerDomain !is this)
            throw new Exception(
                "ant_fibers: spawn requires a lane in this domain");
        FiberTask task;
        bool recycled;
        synchronized (admissionMutex)
        {
            if (atomicLoad!(MemoryOrder.acq)(accepting) == 0)
                throw new Exception("ant_fibers: domain is shutting down");
            immutable reserved = reservedRecords;
            if (reserved > options_.recordRetention - lifecycleRecordsPerTask)
                throw new FiberLifecycleBackpressure(
                    options_.recordRetention, reserved);
            synchronized (poolMutex)
            {
                // An emptied size class keeps its key (null head), so a
                // steady spawn/retire cycle never deletes and reinserts it.
                auto head = stackSize in pool;
                if (head !is null && *head !is null)
                {
                    task = *head;
                    *head = task.waitNext;
                    task.waitNext = null;
                    task.pooledFlag = false;
                    recycled = true;
                }
            }
            if (task is null)
                task = new FiberTask(this, lane, body, stackSize);
            auto nextGeneration = task.generation + 1;
            // Generation zero is reserved for an unadmitted slot.
            if (nextGeneration == 0) nextGeneration = 1;
            // Invalidate old handles before touching recyclable terminal data.
            atomicStore!(MemoryOrder.rel)(task.generationState, nextGeneration);
            if (recycled)
            {
                assert(atomicLoad!(MemoryOrder.acq)(
                        task.managedJoinControl.waiterCount) == 0
                    && atomicLoad!(MemoryOrder.acq)(
                        task.managedJoinControl.headWord) == 0
                    && task.managedJoinTarget() is null);
                assert(atomicLoad!(MemoryOrder.acq)(task.joinState)
                    == FiberTask.joinClosed);
                task.joinControl.terminalEvent.reset();
                atomicStore!(MemoryOrder.rel)(task.joinState, 0u);
                atomicStore!(MemoryOrder.rel)(task.failure,
                    cast(shared(Throwable)) null);
                task.waitSignal = 0;
                task.waitPrev = null;
                assert(task.waitOwner is null && task.waitCause != WaitCause.armed);
                task.timerIndex = size_t.max;
                atomicStore!(MemoryOrder.rel)(task.outcomeState, FiberOutcome.pending);
                task.storeControl(controlWordFor(Phase.ready));
                atomicStore!(MemoryOrder.rel)(task.lastResumeIdentityState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.resumeCountState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.migrationCountState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.cancellationEventEmitted,
                    cast(ubyte) 0);
                atomicStore!(MemoryOrder.rel)(task.cancellationDispositionState,
                    cast(ubyte) CancellationDisposition.none);
                task.readyLane = lane;
                if (task.fiber.state == Fiber.State.TERM)
                    task.fiber.reset(body);
                else
                    // A task cancelled before first entry still has a HOLD
                    // Fiber. druntime only resets TERM Fibers, so replace this
                    // rare stack rather than ever running the rejected body.
                    task.fiber = stackSize == 0
                        ? new Fiber(body) : new Fiber(body, stackSize);
            }
            synchronized (rootsMutex)
            {
                if (!domainRooted)
                {
                    // The Farm stores task addresses as integer payload words,
                    // so root this GC owner before the first such publication.
                    GC.addRoot(cast(void*) this);
                    domainRooted = true;
                }
                task.rootsIndex = roots.length;
                task.rootedFlag = true;
                roots ~= task;
                // Worst case: admitted, cancellation, terminal, failure.
                // finishTerminal returns the unused part.
                atomicFetchAdd!(MemoryOrder.rel)(lifecycleReservationCount,
                    cast(long) lifecycleRecordsPerTask);
                atomicFetchAdd(activeCount, 1L);
            }
            emitLifecycle(task, FiberLifecycleKind.admitted);
            handle = task.handle;
            // Publication is inside the admission transaction. Shutdown either
            // precedes this entire generation or observes it in roots.
            task.readyLane.push(task);
        }
        return task;
    }

    /// Spawn and capture the admitted generation in one API operation. Under
    /// `FiberRetirement.serviceThreads` this is the safe form: the task may
    /// be recycled as soon as it terminates.
    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        return spawnHandleOn(defaultReadyLane, body, stackSize);
    }

    /// ditto
    TaskHandle spawnHandle(void function() body, size_t stackSize = 0)
    {
        return spawnHandleOn(defaultReadyLane, body, stackSize);
    }

    /// `spawnOn` returning the generation handle.
    TaskHandle spawnHandleOn(Body)(FiberReadyLane lane,
        Body body, size_t stackSize = 0)
        if (is(Body : void delegate()) || is(Body : void function()))
    {
        TaskHandle handle;
        cast(void) admit(lane, body, stackSize, handle);
        return handle;
    }

    /// Retire up to `maximum` lifecycle records, then up to `maximum`
    /// terminal tasks, and return the number of tasks recycled. Records go to
    /// `options.onRecord` (or are discarded) in sequence order. Each terminal
    /// task goes to `options.onCompletion` while it is still valid and is
    /// recycled when the handler returns; the task, and any `FiberTask`
    /// reference to it, must not be used afterwards.
    ///
    /// Under `FiberRetirement.director` only the Director may call this. One
    /// retirer runs at a time: a concurrent call returns 0 at once. A handler
    /// that throws leaves its record or task, and everything after it, queued
    /// in order for the next call; tasks already handled are recycled.
    /// Handlers run without scheduler locks and may spawn, but must not call
    /// `retire`.
    size_t retire(size_t maximum = size_t.max)
    {
        if (options_.retirement == FiberRetirement.director)
            requireDirector();
        size_t recordsRetired;
        return retireUnchecked(maximum, recordsRetired);
    }

    package size_t retireUnchecked(size_t maximum, out size_t recordsRetired)
    {
        int expected = 0;
        if (!cas!(MemoryOrder.acq, MemoryOrder.raw)(
                &control.retireGate, expected, 1))
            return 0;
        scope (exit) atomicStore!(MemoryOrder.rel)(retireGate, 0);
        // Requests for these generations then apply before reuse and report
        // alreadyTerminal rather than a stale handle.
        if (pendingCancellationRequests != 0)
            cast(void) drainCancellationRequestsCounted();
        recordsRetired = retireRecords(maximum);
        return retireCompletions(maximum);
    }

    private size_t retireRecords(size_t maximum)
    {
        auto handler = options_.onRecord;
        FiberLifecycleEvent event;
        size_t n;
        for (; n < maximum && records.front(event); ++n)
        {
            if (handler !is null) handler(event);
            records.popFront();
            releaseRecordBudget(1);
        }
        return n;
    }

    private size_t retireCompletions(size_t maximum)
    {
        enum batchSize = 64;
        auto handler = options_.onCompletion;
        size_t retired;
        while (retired < maximum)
        {
            immutable want = maximum - retired < batchSize
                ? maximum - retired : batchSize;
            FiberTask first, last;
            immutable taken = takeRetirementBatch(want, first, last);
            if (taken == 0) break;
            auto task = first;
            size_t handled;
            try
            {
                for (; handled < taken; ++handled)
                {
                    auto next = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
                        task.queueNext);
                    if (handler !is null) handler(task);
                    task = next;
                }
            }
            catch (Throwable failure)
            {
                // `task` threw: it and the rest of the batch go back first.
                restoreRetirementBacklog(task, last, taken - handled);
                recycle(first, handled);
                throw failure;
            }
            recycle(first, handled);
            retired += handled;
        }
        return retired;
    }

    // Oldest first: a backlog left by a throwing handler, then new
    // completions. The batch is a chain through queueNext ending in null.
    private size_t takeRetirementBatch(size_t want, out FiberTask first,
        out FiberTask last) nothrow @nogc
    {
        if (retireBacklogFirst is null)
        {
            auto chain = completions.stealChain(want);
            first = chain.first;
            last = chain.last;
            return chain.length;
        }
        first = last = retireBacklogFirst;
        size_t n = 1;
        for (FiberTask next; n < want
            && (next = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
                last.queueNext)) !is null; ++n)
            last = next;
        retireBacklogFirst = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
            last.queueNext);
        if (retireBacklogFirst is null) retireBacklogLast = null;
        atomicStore!(MemoryOrder.raw)(last.queueNext, cast(shared(FiberTask)) null);
        atomicFetchSub!(MemoryOrder.rel)(retireBacklogCount, cast(uint) n);
        return n;
    }

    private void restoreRetirementBacklog(FiberTask first, FiberTask last,
        size_t count) nothrow @nogc
    {
        if (count == 0) return;
        atomicStore!(MemoryOrder.raw)(last.queueNext,
            cast(shared(FiberTask)) retireBacklogFirst);
        if (retireBacklogFirst is null) retireBacklogLast = last;
        retireBacklogFirst = first;
        atomicFetchAdd!(MemoryOrder.rel)(retireBacklogCount, cast(uint) count);
    }

    // Unroot and pool `count` handled tasks from a queueNext chain. Admission
    // excludes generation reuse while a detached cancellation still applies.
    private void recycle(FiberTask first, size_t count)
    {
        if (count == 0) return;
        synchronized (admissionMutex)
        {
            synchronized (rootsMutex)
            {
                auto task = first;
                foreach (_; 0 .. count)
                {
                    releaseTaskRootLocked(task);
                    task = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
                        task.queueNext);
                }
            }
            synchronized (poolMutex)
            {
                auto task = first;
                foreach (_; 0 .. count)
                {
                    auto next = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(
                        task.queueNext);
                    atomicStore!(MemoryOrder.raw)(task.queueNext,
                        cast(shared(FiberTask)) null);
                    recycleLocked(task);
                    task = next;
                }
            }
        }
    }

    private void recycleLocked(FiberTask task)
    {
        if (task.domain !is this || !task.terminated)
            antfarm.fatal("ant_fibers: retired a task that is not terminal in this domain");
        if (task.pooledFlag)
            antfarm.fatal("ant_fibers: task retired twice");
        task.closeJoinWaiters();
        while (atomicLoad!(MemoryOrder.acq)(task.joinState)
                != FiberTask.joinClosed)
            Thread.yield();
        task.pooledFlag = true;
        task.waitNext = pool.get(task.stackSize, null);
        pool[task.stackSize] = task;
    }

    /// Tasks currently held by the recycling pool.
    @property size_t pooled()
    {
        synchronized (poolMutex)
        {
            size_t n;
            foreach (head; pool.byValue)
                for (auto t = head; t !is null; t = t.waitNext) ++n;
            return n;
        }
    }

    /// Called by a running managed fiber. The wait registration precedes the
    /// yield; the state handshake makes a concurrent signal either publish
    /// after the yield completes or wake an already-parked task exactly once.
    static void await(Signal signal)
    {
        if ((signal & timerSignalBit) != 0)
            throw new Exception("ant_fibers: high-bit signals are reserved for timers");
        parkOn(signal);
    }

    /// Park the current fiber until `signal` is delivered or cancellation.
    package static void parkOn(Signal signal)
    {
        auto task = currentTask;
        assert(task !is null && task.fiber is Fiber.getThis());
        if (task.cancellationRequested)
            throw new FiberCancelled;
        task.waitSignal = signal;
        auto waits = task.domain.waits;
        immutable armed = waits.linkArmed(task); // may allocate a bucket
        if (task.cancellationRequested)
        {
            task.withdrawWait(armed);
            waits.unlink(task);
            throw new FiberCancelled;
        }
        Fiber.yield();
        waits.unlink(task);
        if (task.cancellationRequested)
            throw new FiberCancelled;
    }

    /// Cooperative yield without entering the long-wait container.
    static void yieldReady()
    {
        auto task = currentTask;
        assert(task !is null && task.fiber is Fiber.getThis());
        if ((task.loadControl() & ctrlCancelMask) != 0)
            throw new FiberCancelled;
        task.addPhase(Phase.running, Phase.ready);
        Fiber.yield();
        if (task.cancellationRequested)
            throw new FiberCancelled;
    }

    /// Suspend the current managed fiber until an absolute monotonic deadline.
    static void sleepUntil(MonoTime deadline)
    {
        auto task = currentTask;
        assert(task !is null && task.fiber is Fiber.getThis());
        if (task.cancellationRequested)
            throw new FiberCancelled;
        immutable armed = task.armWait();
        if (task.cancellationRequested)
        {
            task.withdrawWait(armed);
            throw new FiberCancelled;
        }
        task.domain.armTimer(task, deadline, armed);
        Fiber.yield();
        task.domain.disarmTimer(task);
        if (task.cancellationRequested)
            throw new FiberCancelled;
    }

    /// Running fiber only, after `armWait`: let the timer claim `armed` at
    /// `deadline`. On failure (heap growth) the wait is withdrawn.
    package void armTimer(FiberTask task, MonoTime deadline, ulong armed)
    {
        immutable seq = atomicFetchAdd(timerSequence, 1UL);
        bool earliest;
        try
            earliest = timers.insert(task, deadline.ticks, seq, armed);
        catch (Throwable failure)
        {
            task.withdrawWait(armed);
            throw failure;
        }
        if (earliest)
            task.readyLane.noteTimerArmed();
    }

    /// The waiter, after it resumes from a wait it armed a timer for. A timer
    /// that claimed the wait has already left the heap.
    package void disarmTimer(FiberTask task) nothrow @nogc
    {
        if (task.waitCause != WaitCause.timedOut)
            timers.remove(task);
    }

    static void sleepFor(Duration duration)
    {
        sleepUntil(MonoTime.currTime + duration);
    }

    static bool currentCancellationRequested() nothrow @nogc
    {
        auto task = currentTask;
        return task !is null && task.cancellationRequested;
    }

    /// Opaque identity of the current OS thread. It deliberately follows the
    /// worker rather than the fiber and is intended for diagnostics/tests.
    static ulong currentWorkerIdentity() nothrow @nogc
    {
        return cast(ulong) cast(void*) Thread.getThis();
    }

    /// Wake all fibers registered on a signal. Returns the number matched.
    size_t signal(Signal signal)
    {
        if ((signal & timerSignalBit) != 0)
            throw new Exception("ant_fibers: high-bit signals are reserved for timers");
        return wakeWaiters(signal);
    }

    /// Deliver generation advances which originated inside `nothrow @nogc`
    /// payload callbacks. Managed threadpool workers call this after payload
    /// consumption; custom pumps must call it as part of their service loop.
    size_t pollGenerationTriggers()
    {
        if (atomicLoad!(MemoryOrder.acq)(deferredTriggerHead) is null)
            return 0;
        auto trigger = cast(FiberGenerationTrigger) atomicExchange!(
            MemoryOrder.acq_rel)(&deferredTriggerHead,
                cast(shared(FiberGenerationTrigger)) null);
        size_t delivered;
        while (trigger !is null)
        {
            auto next = cast(FiberGenerationTrigger) atomicLoad!(
                MemoryOrder.acq)(trigger.queueNext_);
            atomicStore!(MemoryOrder.raw)(trigger.queueNext_,
                cast(shared(FiberGenerationTrigger)) null);
            // Permit a concurrent later generation to enqueue this node
            // before delivering the generation(s) already represented here.
            atomicStore!(MemoryOrder.rel)(trigger.queued_, 0);
            trigger.deliver();
            trigger = next;
            ++delivered;
        }
        return delivered;
    }

    package void enqueueGenerationTrigger(FiberGenerationTrigger trigger)
        nothrow @nogc
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(deferredTriggerHead);
        while (true)
        {
            atomicStore!(MemoryOrder.raw)(trigger.queueNext_, observed);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &deferredTriggerHead, observed,
                    cast(shared(FiberGenerationTrigger)) trigger))
                break;
            observed = atomicLoad!(MemoryOrder.acq)(deferredTriggerHead);
        }
        // The worker currently dispatching a payload will observe the queue
        // on return. This edge also wakes a responder if delivery originated
        // elsewhere or the worker is about to park.
        foreach (lane; readyLanes)
            lane.noteExternalReady();
    }

    package size_t wakeWaiters(Signal signal)
    {
        size_t matched;
        auto chain = waits.claimAll(signal, matched);
        version (unittest)
            if (wakeAfterTakeHook !is null)
                wakeAfterTakeHook(signal);
        wakeClaimed(chain);
        return matched;
    }

    /// Deliver WAKE to a chain of claimed waiters (WaiterQueue.claimFront).
    /// Read each link before the wake: a woken task may run and wait again.
    package void wakeClaimed(FiberTask chain) nothrow @nogc
    {
        while (chain !is null)
        {
            auto task = chain;
            chain = task.waitNext;
            task.waitNext = null;
            makeWaiterReady(task);
        }
    }

    /// Turn expired timers into ordinary ready fibers. Called by managed lane
    /// workers before consuming/publishing work.
    size_t pollTimers()
    {
        // Every responder calls this on every pump: an empty heap costs one
        // load, with no clock read and no mutex.
        if (timers.earliest == long.max) return 0;
        return pollTimers(MonoTime.currTime);
    }

    /// ditto
    size_t pollTimers(MonoTime now)
    {
        if (timers.earliest > now.ticks) return 0;
        size_t woken;
        FiberTask task;
        ulong armed;
        while (timers.takeExpired(now.ticks, task, armed))
        {
            // A lost claim is a wait some other waker already ended.
            if (!task.claimWait(armed, WaitCause.timedOut)) continue;
            version (unittest)
                if (timerAfterClaimHook !is null)
                    timerAfterClaimHook(task);
            makeWaiterReady(task);
            ++woken;
        }
        return woken;
    }

    bool nextTimerDeadline(out long ticks) nothrow @nogc
    {
        return timers.nextDeadline(ticks);
    }

    /// Verify the construction thread which owns synchronous cancellation,
    /// shutdown and release/recycle for this domain.
    /// The calling thread constructed this domain.
    @property bool isDirectorThread() const nothrow @nogc
    {
        return Thread.getThis() is directorThread;
    }

    private void requireDirector()
    {
        auto self = Thread.getThis();
        synchronized (directorMutex)
        {
            if (directorThread !is self)
                throw new Exception(
                    "ant_fibers: Director operation from a non-owner thread");
        }
    }

    /// Queue a remote, generation-paired cancellation request. Any thread may
    /// submit; a worker pump or explicit drainer applies the request.
    CancellationSubmission requestCancel(TaskHandle handle)
    {
        auto task = handle.slot;
        if (task is null || task.domain !is this || handle.generation == 0
            || task.generation != handle.generation)
            return CancellationSubmission.staleHandle;
        synchronized (cancellationRequestMutex)
        {
            cancellationRequests ~= handle;
            // Publish the empty-queue hint after append. The mutex owns the
            // array; acquire readers use the count only to avoid an empty lock.
            atomicStore!(MemoryOrder.rel)(control.cancellationRequestCount,
                cancellationRequests.length);
        }
        // The lane list is fixed at setup. Do not read task.readyLane here:
        // the unlocked generation check can race release/reuse on another lane.
        foreach (lane; readyLanes)
            lane.noteCancellationRequested();
        return CancellationSubmission.queued;
    }

    CancellationSubmission requestCancel(FiberTask task)
    {
        return task is null ? CancellationSubmission.staleHandle
                            : requestCancel(task.handle);
    }

    /// Apply a snapshot of queued requests from any thread. Concurrent drains
    /// serialize; each caller receives only the requests it applied.
    CancellationApplication[] drainCancellationRequests()
    {
        CancellationApplication[] results;
        applyCancellationRequests(&results);
        return results;
    }

    /// Same drain without per-request results; returns how many requests it
    /// applied and allocates nothing. The managed pump and other discarding
    /// callers use this; lifecycle records retain accepted cancels.
    package(ant_fibers) size_t drainCancellationRequestsCounted()
    {
        return applyCancellationRequests(null);
    }

    private size_t applyCancellationRequests(CancellationApplication[]* results)
    {
        // Empty service passes cost one acquire load, no lock or allocation.
        // A racing append publishes before waking the pump; the pool arms its
        // wait before a final service pass, so that race cannot lose a wake.
        if (pendingCancellationRequests == 0) return 0;
        synchronized (admissionMutex)
        {
            // Serialize external CANCEL adds (at most one plus cancelCurrent),
            // generation validation, and application against spawn/release.
            // Never run a fiber under this lock. Wake notifiers are signal-only.
            TaskHandle[] requests;
            synchronized (cancellationRequestMutex)
            {
                requests = cancellationRequests;
                cancellationRequests = null;
                atomicStore!(MemoryOrder.rel)(control.cancellationRequestCount, 0);
            }
            if (results !is null)
                *results = new CancellationApplication[requests.length];
            foreach (i, handle; requests)
            {
                immutable result = applyCancellation(handle, true);
                if (results !is null)
                    (*results)[i] = CancellationApplication(handle, result);
            }
            return requests.length;
        }
    }

    /// Queued requests, excluding a batch currently being applied. Release
    /// holds admissionMutex, so its check also waits for in-flight application.
    @property size_t pendingCancellationRequests() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(control.cancellationRequestCount);
    }

    /// Direct cancellation by the currently executing managed Fiber. The
    /// external applier serializes separately; only this local owner may race it.
    static CancellationResult cancelCurrent()
    {
        auto task = currentTask;
        if (task is null)
            return CancellationResult.staleHandle;
        return task.domain.applyCancellation(task.handle, false);
    }

    /// Director-only synchronous cancellation, also used for shutdown.
    bool directorCancel(TaskHandle handle)
    {
        immutable result = directorCancelDetailed(handle);
        return result != CancellationResult.staleHandle
            && result != CancellationResult.alreadyTerminal;
    }

    bool directorCancel(FiberTask task)
    {
        return task !is null && directorCancel(task.handle);
    }

    CancellationResult directorCancelDetailed(TaskHandle handle)
    {
        requireDirector();
        synchronized (admissionMutex)
            return applyCancellation(handle, true);
    }

    /// Counted fetch_add arbitration shared by the local owner and the
    /// serialized external applier (Director or request drainer).
    /// The old phase returned by the add decides whether this request beat the
    /// terminal cutoff. A late external add is harmless and is not rolled back.
    private CancellationResult applyCancellation(TaskHandle handle,
                                                   bool external)
    {
        auto task = handle.slot;
        immutable requestedGeneration = handle.generation;
        if (task is null || task.domain !is this || requestedGeneration == 0
            || task.generation != requestedGeneration)
            return CancellationResult.staleHandle;

        immutable opening = task.loadControl();
        immutable openingPhase = phaseOf(opening);
        if (openingPhase == Phase.terminating
            || openingPhase == Phase.terminated)
            return CancellationResult.alreadyTerminal;

        // An accepted request from this generation already owns the semantic
        // cancellation. At most a simultaneously racing local/external pair
        // can pass this check and contribute two counted addends.
        if (atomicLoad!(MemoryOrder.acq)(task.cancelGeneration)
                == requestedGeneration)
            return CancellationResult.alreadyRequested;

        immutable old = task.addCtrl(ctrlCancel);
        immutable ph = phaseOf(old);
        if (ph == Phase.terminating || ph == Phase.terminated)
            return CancellationResult.alreadyTerminal;

        immutable entered = (old & ctrlEnterMask) != 0;
        CancellationResult result;
        if (!entered)
            result = CancellationResult.wonBeforeEntry;
        else if (ph == Phase.waiting)
            result = CancellationResult.requestedWaiting;
        else if (ph == Phase.ready)
            result = CancellationResult.requestedReady;
        else
            result = CancellationResult.requestedRunning;

        acceptCancellation(task, requestedGeneration, result);
        if (external)
        {
            // Pairs with the fence in armWait. The CANCEL add above precedes
            // it, so a wait armed after this load sees the request and
            // withdraws; one armed before is claimed here. The claim is
            // exactly-once against every other waker, whichever container
            // the wait is in, so the cancelled waiter unlinks itself.
            atomicFence!(MemoryOrder.seq)();
            if (task.claimCurrentWait(WaitCause.cancelled))
            {
                makeWaiterReady(task);
                // The destination lane may have parked after the enqueue wake
                // but before this drain. Cancellation service also wakes it
                // under a Director-owned ordinary readiness policy.
                task.readyLane.noteCancellationRequested();
            }
        }
        return result;
    }

    private static void tagCancellation(FiberTask task, ulong generation)
        nothrow @nogc
    {
        // Atomic max without a global counter. Generations are per slot.
        auto observed = atomicLoad!(MemoryOrder.acq)(task.cancelGeneration);
        while (observed < generation)
        {
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &task.control.cancelGeneration, observed, generation))
                return;
            observed = atomicLoad!(MemoryOrder.acq)(task.cancelGeneration);
        }
    }

    private CancellationResult acceptCancellation(FiberTask task,
        ulong generation, CancellationResult result) nothrow @nogc
    {
        tagCancellation(task, generation);
        ubyte expected;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &task.control.cancellationEventEmitted, expected, cast(ubyte) 1))
        {
            atomicStore!(MemoryOrder.rel)(task.cancellationDispositionState,
                cast(ubyte) CancellationDisposition.pending);
            emitLifecycle(task, FiberLifecycleKind.cancellation,
                FiberOutcome.pending, result, null,
                CancellationDisposition.pending);
            atomicStore!(MemoryOrder.rel)(task.cancellationEventEmitted,
                cast(ubyte) 2);
        }
        return result;
    }

    private void emitLifecycle(FiberTask task, FiberLifecycleKind kind,
        FiberOutcome outcome = FiberOutcome.pending,
        CancellationResult cancellation = CancellationResult.staleHandle,
        Throwable failure = null,
        CancellationDisposition disposition = CancellationDisposition.none)
        nothrow @nogc
    {
        // Without a handler every record would be discarded unread at
        // retirement, so none is emitted: the reservation accounting (and
        // the live-task bound) is unchanged, the ring traffic is not paid.
        if (options_.onRecord is null) return;
        // The slot was reserved at admission; emission cannot fail.
        records.emit(kind, task.handle, outcome, cancellation, disposition,
            failure);
    }

    private void releaseRecordBudget(uint count) nothrow @nogc
    {
        if (count == 0) return;
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
            lifecycleReservationCount, cast(long) count);
        assert(previous >= count);
    }

    /// Reject future spawns. With cancellation, request cancellation for all
    /// active tasks and wake parked ones so fiber cleanup scopes can run.
    void beginShutdown(bool cancelActive = false)
    {
        requireDirector();
        cast(void) drainCancellationRequestsCounted();
        synchronized (admissionMutex)
            atomicStore!(MemoryOrder.rel)(accepting, 0);
        if (!cancelActive) return;
        FiberTask[] snapshot;
        synchronized (rootsMutex) snapshot = roots.dup;
        foreach (task; snapshot) directorCancel(task);
    }

    @property bool drained() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(activeCount) == 0;
    }

    @property long active() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(activeCount);
    }

    /// `FiberReadyLane.serve` on a one-lane domain's lane.
    FiberServiceResult serve(ref Token token,
        FiberServicePass pass = FiberServicePass.init)
    {
        if (defaultReadyLane is null)
            throw new Exception("ant_fibers: domain has no ready lane");
        return defaultReadyLane.serve(token, pass);
    }

    /// Compatibility flush for a one-lane domain. Multi-lane embeddings flush
    /// each selected `FiberReadyLane` with a token registered on its Farm.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = flushAvgCost)
    {
        if (defaultReadyLane is null)
            throw new Exception("ant_fibers: domain has no ready lane");
        return defaultReadyLane.flush(token, maximum, avgCost);
    }

    /// Armed waits not yet claimed by a wake, timeout or cancellation.
    @property size_t waiting() const nothrow @nogc
    {
        immutable n = atomicLoad!(MemoryOrder.acq)(waitingCount);
        return n < 0 ? 0 : cast(size_t) n;
    }
    @property size_t ready() nothrow @nogc
    {
        size_t total;
        foreach (lane; readyLanes) total += lane.ready;
        return total;
    }

    private void releaseTaskRootLocked(FiberTask task)
    {
        assert(task.rootedFlag && roots.length != 0);
        immutable i = task.rootsIndex;
        auto moved = roots[$ - 1];
        roots[i] = moved;
        moved.rootsIndex = i;
        // Clear the vacated slot (the GC scans the whole block), then keep
        // the block: no other slice aliases roots (shutdown copies it), and
        // without assumeSafeAppend the next spawn's append would copy the
        // entire array into a new block.
        roots[$ - 1] = null;
        roots.length = roots.length - 1;
        roots.assumeSafeAppend();
        task.rootedFlag = false;
        if (roots.length == 0 && domainRooted)
        {
            GC.removeRoot(cast(void*) this);
            domainRooted = false;
        }
    }

    private void resume(FiberTask task) nothrow @nogc
    {
        auto word = task.loadControl();
        auto ph = phaseOf(word);
        if (ph == Phase.terminating || ph == Phase.terminated)
            return;

        if ((word & ctrlEnterMask) == 0)
        {
            immutable old = task.addCtrl(ctrlEnter);
            if ((old & ctrlCancelMask) != 0 && (old & ctrlEnterMask) == 0)
            {
                task.addPhase(Phase.ready, Phase.terminating);
                finishTerminal(task, null, true, true);
                return;
            }
            ph = Phase.ready;
        }

        if (ph != Phase.ready)
            return;
        task.addPhase(Phase.ready, Phase.running);

        immutable identity = currentWorkerIdentity();
        immutable previous = atomicExchange!(MemoryOrder.acq_rel)(
            &task.control.lastResumeIdentityState, identity);
        if (previous != 0 && previous != identity)
            atomicFetchAdd!(MemoryOrder.rel)(task.migrationCountState, 1UL);
        atomicFetchAdd!(MemoryOrder.rel)(task.resumeCountState, 1UL);
        currentTask = task;
        scope (exit) currentTask = null;
        version (TSan)
            auto failure = task.callWithTsan();
        else
            auto failure = task.fiber.call!(Fiber.Rethrow.no)();

        word = task.loadControl();
        ph = phaseOf(word);
        if (task.fiber.state == Fiber.State.TERM)
        {
            assert(ph == Phase.running);
            immutable terminalOld = task.addPhase(
                Phase.running, Phase.terminating);
            finishTerminal(task, failure,
                (terminalOld & ctrlCancelMask) != 0);
            return;
        }
        if (ph == Phase.ready)
        {
            task.readyLane.push(task);
        }
        else if (ph == Phase.waiting)
        {
            immutable old = task.addCtrl(ctrlPark);
            if (ctrlField(old, ctrlWake) != 0)
                publishParkedWaiter(task);
        }
        else if (ph == Phase.running)
        {
            // HOLD without a scheduler suspend (raw Fiber.yield). Fail rather
            // than drop the only activation.
            immutable terminalOld = task.addPhase(
                Phase.running, Phase.terminating);
            finishTerminal(task, rawYieldFailure,
                (terminalOld & ctrlCancelMask) != 0);
        }
    }

    private void finishTerminal(FiberTask task, Throwable failure,
                                bool cancellationWon, bool preEntry = false)
        nothrow @nogc
    {
        // Also discard suspended shadow frames after a rejected raw yield.
        // Recycled tasks start with a fresh context, including pre-entry cancel.
        version (TSan) task.clearTsanContext();
        // CANCEL can become visible before acceptCancellation has published
        // even its pending disposition. Wait for the entire cancellation
        // record, not merely an in-progress publisher, so it cannot overwrite
        // the terminal disposition after this function.
        if (cancellationWon)
            while (atomicLoad!(MemoryOrder.acq)(
                    task.cancellationEventEmitted) != 2)
                Thread.yield();
        atomicStore!(MemoryOrder.rel)(task.failure,
            cast(shared(Throwable)) failure);
        FiberOutcome outcome;
        if (failure !is null && cast(FiberCancelled) failure is null)
            outcome = FiberOutcome.failed;
        else if (cancellationWon || failure !is null)
            outcome = FiberOutcome.cancelled;
        else
            outcome = FiberOutcome.completed;
        CancellationDisposition disposition;
        immutable requested = cancellationWon
            || cast(FiberCancelled) failure !is null;
        if (requested)
        {
            if (failure !is null && cast(FiberCancelled) failure is null)
                disposition = CancellationDisposition.cleanupFailed;
            else if (preEntry || cast(FiberCancelled) failure !is null)
                disposition = CancellationDisposition.acknowledged;
            else
                disposition = CancellationDisposition.resultAbrogated;
        }
        atomicStore!(MemoryOrder.rel)(task.cancellationDispositionState,
            cast(ubyte) disposition);
        atomicStore!(MemoryOrder.rel)(task.outcomeState, outcome);
        task.addPhase(Phase.terminating, Phase.terminated);
        task.joinControl.terminalEvent.setIfInitialized();
        wakeManagedJoiners(task);
        if (cast(Error) failure !is null)
        {
            atomicStore!(MemoryOrder.rel)(fatalFailure,
                cast(shared(Throwable)) failure);
            atomicStore!(MemoryOrder.rel)(accepting, 0);
            atomicStore!(MemoryOrder.rel)(fatalState, 1);
        }
        emitLifecycle(task, FiberLifecycleKind.terminal, outcome,
            CancellationResult.staleHandle, null, disposition);
        if (outcome == FiberOutcome.failed)
            emitLifecycle(task, FiberLifecycleKind.failure, outcome,
                CancellationResult.staleHandle, failure, disposition);
        // Return the unused part of the admission reservation: the task holds
        // no record slot from here on, so recycling never waits on records.
        uint emitted;
        if (options_.onRecord !is null)
        {
            emitted = 2; // admitted + terminal
            if (atomicLoad!(MemoryOrder.acq)(task.cancellationEventEmitted) == 2)
                ++emitted;
            if (outcome == FiberOutcome.failed) ++emitted;
            assert(emitted <= lifecycleRecordsPerTask);
        }
        releaseRecordBudget(lifecycleRecordsPerTask - emitted);
        // Publish completion after its terminal record and before dropping
        // activeCount, so an observer of drained can retire every task.
        completions.push(task);
        atomicFetchSub(activeCount, 1L);
    }

    private void wakeManagedJoiners(FiberTask target) nothrow @nogc
    {
        target.lockManagedJoinList();
        auto word = atomicLoad!(MemoryOrder.raw)(
            target.managedJoinControl.headWord);
        atomicStore!(MemoryOrder.raw)(target.managedJoinControl.headWord,
            size_t.init);
        atomicStore!(MemoryOrder.rel)(target.managedJoinControl.waiterCount, 0u);
        // Unlink and claim every joiner before dropping the list lock. A
        // joiner a timer or cancellation already claimed loses here and, on
        // resume, finds itself unlinked. Winners are rechained through
        // nextWord; each stays suspended until its WAKE below.
        size_t winners;
        size_t winnersTail;
        while (word != 0)
        {
            auto waiter = cast(FiberTask) cast(void*) word;
            word = atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.nextWord);
            FiberTask.clearManagedJoinMembership(waiter);
            if (!waiter.claimCurrentWait(WaitCause.signalled)) continue;
            if (winnersTail != 0)
                atomicStore!(MemoryOrder.raw)((cast(FiberTask) cast(void*)
                    winnersTail).managedJoinControl.nextWord,
                    cast(size_t) cast(void*) waiter);
            else
                winners = cast(size_t) cast(void*) waiter;
            winnersTail = cast(size_t) cast(void*) waiter;
        }
        target.unlockManagedJoinList();

        while (winners != 0)
        {
            auto waiter = cast(FiberTask) cast(void*) winners;
            winners = atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.nextWord);
            atomicStore!(MemoryOrder.raw)(waiter.managedJoinControl.nextWord,
                size_t.init);
            waiter.domain.makeWaiterReady(waiter);
        }
    }

    private void makeWaiterReady(FiberTask task) nothrow @nogc
    {
        immutable old = task.addCtrl(ctrlWake);
        if (ctrlField(old, ctrlPark) != 0)
            publishParkedWaiter(task);
    }

    /// Unique publisher for the PARK/WAKE handshake. Both addends have already
    /// been raised; this side owns the ready publication.
    private void publishParkedWaiter(FiberTask task) nothrow @nogc
    {
        task.addPhase(Phase.waiting, Phase.ready);
        task.subParkWake();
        task.readyLane.push(task);
        task.readyLane.noteExternalReady();
    }

}

enum FiberEventMode : ubyte
{
    autoReset,
    manualReset,
}

version (unittest)
    // TLS scheduling seam; absent from production and from object layouts.
    package void delegate(FiberEvent) eventAfterRegisterHook;

version (unittest)
{
    // TLS scheduling seams for the wait-race tests: after a signal detaches
    // its waiters and before it wakes them, and after timer expiry claims a
    // wait and before it publishes the waiter.
    package void delegate(Signal) wakeAfterTakeHook;
    package void delegate(FiberTask) timerAfterClaimHook;
}

/// The waiter queue of a FiberEvent or FiberSemaphore: a FIFO under its own
/// mutex, so waits need no signal key, bucket or map allocation.
private struct UnitWaiters
{
    Mutex lock;
    WaiterQueue queue;

    void initialize() { lock = new Mutex; }

    void link(FiberTask task, Object owner) nothrow @nogc
    {
        lock.lock_nothrow();
        queue.pushBack(task, owner);
        lock.unlock_nothrow();
    }

    void unlink(FiberTask task, Object owner) nothrow @nogc
    {
        lock.lock_nothrow();
        if (task.waitOwner is owner) queue.unlink(task);
        lock.unlock_nothrow();
    }

    /// Wake up to `limit` waiters, oldest first, skipping any whose wait
    /// another waker already ended.
    void wake(FiberDomain domain, size_t limit) nothrow @nogc
    {
        size_t claimed;
        lock.lock_nothrow();
        auto chain = queue.claimFront(limit, claimed);
        lock.unlock_nothrow();
        domain.wakeClaimed(chain);
    }
}

/// Shared wait of FiberEvent and FiberSemaphore. `Owner` supplies tryTake
/// (consume one unit), refund (return a unit taken by a cancelled wait),
/// passOn (wake another waiter if units remain) and its waiter queue.
///
/// Each wake goes to one claimed waiter, so a woken waiter that leaves
/// without a unit (cancelled, or its timer arm failed) passes the wake on.
/// Returns false only for a timed wait whose deadline passed with no unit
/// taken.
private bool waitForUnit(Owner)(Owner owner, string misuse, bool timed,
    MonoTime deadline)
{
    auto task = FiberDomain.currentTask;
    auto domain = owner.domain;
    if (task is null || task.domain !is domain)
        throw new Exception(misuse);
    while (true)
    {
        if (owner.tryTake()) return true;
        if (task.cancellationRequested)
            throw new FiberCancelled;
        if (timed && MonoTime.currTime >= deadline)
            return false;
        immutable armed = task.armWait();
        if (task.cancellationRequested)
        {
            task.withdrawWait(armed);
            throw new FiberCancelled;
        }
        owner.waiters.link(task, owner);
        owner.afterRegister();
        if (owner.tryTake())
        {
            // A waker that claimed this wait first spent one wake, and this
            // waiter took one unit: nothing is owed onward. Every unit's wake
            // went to a waiter linked when it arrived, and a waiter that
            // linked later rechecks the units itself.
            task.withdrawWait(armed);
            owner.waiters.unlink(task, owner);
            if (task.cancellationRequested)
            {
                // Return the unit with release publication; the refund wakes
                // a waiter that parked meanwhile.
                owner.refund();
                throw new FiberCancelled;
            }
            return true;
        }
        if (timed)
        {
            try
                domain.armTimer(task, deadline, armed);
            catch (Throwable failure)
            {
                // armTimer withdrew the wait, consuming any wake that beat it.
                owner.waiters.unlink(task, owner);
                if (task.waitCause == WaitCause.signalled) owner.passOn();
                throw failure;
            }
        }
        Fiber.yield();
        owner.waiters.unlink(task, owner);
        if (timed) domain.disarmTimer(task);
        immutable cause = task.waitCause;
        if (task.cancellationRequested)
        {
            if (cause == WaitCause.signalled) owner.passOn();
            throw new FiberCancelled;
        }
        // A wake that lost to this timeout went to another waiter, or left
        // its unit for this last attempt.
        if (cause == WaitCause.timedOut)
            return owner.tryTake();
    }
}

/// Scheduler-aware event. `wait` parks a managed fiber; `set`/`reset` may run
/// on any thread. Auto-reset consumes the posted state for one waiter and
/// wakes one waiter per `set`. Manual-reset stays posted until `reset` and
/// wakes every waiter.
final class FiberEvent
{
    private FiberDomain domain;
    private FiberEventMode mode;
    private shared int posted;
    private UnitWaiters waiters;

    this(FiberDomain domain, FiberEventMode mode = FiberEventMode.autoReset)
    {
        if (domain is null)
            throw new Exception("ant_fibers: FiberEvent requires a domain");
        this.domain = domain;
        this.mode = mode;
        waiters.initialize();
    }

    @property bool isSet() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(posted) != 0;
    }

    void set() nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(posted, 1);
        waiters.wake(domain,
            mode == FiberEventMode.manualReset ? size_t.max : 1);
    }

    void reset() nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(posted, 0);
    }

    /// Park the current managed fiber until this event is posted.
    void wait()
    {
        cast(void) waitForUnit(this, misuse, false, MonoTime.init);
    }

    /// Park until posted or `timeout` elapses; false on timeout.
    bool wait(Duration timeout)
    {
        return waitUntil(MonoTime.currTime + timeout);
    }

    /// Park until posted or `deadline`; false if the deadline passed first.
    bool waitUntil(MonoTime deadline)
    {
        return waitForUnit(this, misuse, true, deadline);
    }

    private enum misuse =
        "ant_fibers: FiberEvent.wait requires a managed fiber of this domain";

    private bool tryTake() nothrow @nogc
    {
        if (mode == FiberEventMode.manualReset)
            return atomicLoad!(MemoryOrder.acq)(posted) != 0;
        int expected = 1;
        return cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&posted, expected, 0);
    }

    // A manual-reset wait consumes nothing. An auto-reset refund may coalesce
    // with another set or follow a concurrent reset.
    private void refund() nothrow @nogc
    {
        if (mode == FiberEventMode.autoReset) set();
    }

    private void passOn() nothrow @nogc
    {
        if (mode == FiberEventMode.autoReset && isSet)
            waiters.wake(domain, 1);
    }

    private void afterRegister()
    {
        version (unittest)
            if (eventAfterRegisterHook !is null)
                eventAfterRegisterHook(this);
    }
}

/// Single-consumer, monotonically counted trigger for completions originating
/// in an allocation-free payload callback. `advance` only performs atomics and
/// enqueues this preallocated node. A scheduler pump later converts that edge
/// into an ordinary FiberEvent wake outside the payload callback.
///
/// Calls to `waitNext` must be serialized by one managed Fiber. Advances may
/// precede waits and may coalesce in the deferred queue without being lost.
final class FiberGenerationTrigger
{
    private FiberDomain domain_;
    private FiberEvent event_;
    private shared ulong completed_;
    private ulong awaited_;
    package shared int queued_;
    package shared(FiberGenerationTrigger) queueNext_;

    this(FiberDomain domain)
    {
        if (domain is null)
            throw new Exception(
                "ant_fibers: generation trigger requires a domain");
        domain_ = domain;
        event_ = new FiberEvent(domain, FiberEventMode.autoReset);
    }

    @property ulong completed() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(completed_);
    }

    /// Record one completion from any thread or payload callback.
    void advance() nothrow @nogc
    {
        immutable before = atomicFetchAdd!(MemoryOrder.acq_rel)(completed_, 1UL);
        if (before == ulong.max)
            fatal("ant_fibers: generation trigger counter wrapped");
        if (atomicExchange!(MemoryOrder.acq_rel)(&queued_, 1) == 0)
            domain_.enqueueGenerationTrigger(this);
    }

    /// Wait for exactly the next generation assigned to this consumer.
    void waitNext()
    {
        auto task = FiberDomain.currentTask;
        if (task is null || task.domain !is domain_)
            throw new Exception(
                "ant_fibers: generation trigger requires a managed fiber of this domain");
        immutable target = awaited_ + 1;
        if (target == 0)
            throw new Exception(
                "ant_fibers: generation trigger waiter wrapped");
        while (completed < target)
            event_.wait();
        awaited_ = target;
    }

    package void deliver()
    {
        event_.set();
    }
}

version (unittest)
    // TLS scheduling seam; absent from production and from object layouts.
    package void delegate(FiberSemaphore) semaphoreAfterRegisterHook;

/// Counting semaphore. `wait` parks a managed fiber when the count is zero;
/// `post` may run on any thread and wakes one waiter per permit.
final class FiberSemaphore
{
    private FiberDomain domain;
    private shared long permits;
    private UnitWaiters waiters;

    this(FiberDomain domain, long initial = 0)
    {
        if (domain is null)
            throw new Exception("ant_fibers: FiberSemaphore requires a domain");
        if (initial < 0)
            throw new Exception("ant_fibers: semaphore count must be >= 0");
        this.domain = domain;
        waiters.initialize();
        atomicStore!(MemoryOrder.raw)(permits, initial);
    }

    @property long available() const nothrow @nogc
    {
        immutable n = atomicLoad!(MemoryOrder.acq)(permits);
        return n < 0 ? 0 : n;
    }

    void post(long count = 1) nothrow @nogc
    {
        if (count <= 0) return;
        atomicFetchAdd!(MemoryOrder.rel)(permits, count);
        waiters.wake(domain, cast(size_t) count);
    }

    void wait()
    {
        cast(void) waitForUnit(this, misuse, false, MonoTime.init);
    }

    /// Wait for a permit or until `timeout` elapses; false on timeout.
    bool wait(Duration timeout)
    {
        return waitUntil(MonoTime.currTime + timeout);
    }

    /// Wait for a permit until `deadline`; false if it passed first.
    bool waitUntil(MonoTime deadline)
    {
        return waitForUnit(this, misuse, true, deadline);
    }

    private enum misuse =
        "ant_fibers: FiberSemaphore.wait requires a managed fiber of this domain";

    private bool tryTake() nothrow @nogc
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(permits);
        while (observed > 0)
        {
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &permits, observed, observed - 1))
                return true;
            observed = atomicLoad!(MemoryOrder.acq)(permits);
        }
        return false;
    }

    private void refund() nothrow @nogc { post(); }

    private void passOn() nothrow @nogc
    {
        if (available > 0) waiters.wake(domain, 1);
    }

    private void afterRegister()
    {
        version (unittest)
            if (semaphoreAfterRegisterHook !is null)
                semaphoreAfterRegisterHook(this);
    }
}

/// Subscribe a unique ConsumerView and keep the failure check in optimized
/// builds. The caller must unsubscribe the same instance.
long subscribeOrThrow(ref ConsumerView consumer, AntFarm* farm)
{
    // subscribe retries a moving frontier itself; capacity and misuse are not transient.
    immutable subscription = consumer.subscribe(farm);
    if (subscription == SUBSCRIBE_FULL)
        throw new Exception("ant_fibers: Farm already has MAX_CONSUMERS_LIMIT subscribers");
    if (subscription < 0)
        throw new Exception("ant_fibers: invalid consumer subscription (null Farm or view already subscribed)");
    return subscription;
}

/// Drive a one-lane domain on the calling thread until every admitted task is
/// terminal, retiring as it goes when this thread may (the Director, or any
/// thread under `serviceThreads`), and once more at the end. Inspect results
/// through `options.onCompletion`, `options.onRecord` or a `TaskHandle`, not
/// a `FiberTask` reference kept across the drain. `consumer` is passed by
/// reference because a ConsumerView is a unique cursor and must never be
/// copied.
void drainUntilEmpty(FiberDomain domain, ref Token token,
                     ref ConsumerView consumer, size_t flushBatch = 32,
                     uint avgCost = flushAvgCost)
{
    if (domain is null)
        throw new Exception("ant_fibers: cannot drain a null domain");
    auto pass = FiberServicePass(true, false, flushBatch, avgCost);
    while (!domain.drained)
    {
        immutable progress = domain.serve(token, pass).progress;
        if (!consumer.consumeNext() && !progress)
            Thread.yield();
    }
    if (domain.options.retirement == FiberRetirement.serviceThreads
        || domain.isDirectorThread)
    {
        size_t recordsRetired;
        cast(void) domain.retireUnchecked(size_t.max, recordsRetired);
    }
}

version (unittest)
{
    unittest
    {
        // Waiter self-unlink and whole-signal claim on one deliberately
        // crowded bucket; claims skip waits another waker already ended.
        enum count = 100_000;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto domain = new FiberDomain(farm);
        auto waits = new WaitSet;
        FiberTask[] tasks;
        tasks.reserve(count);
        foreach (i; 0 .. count)
        {
            auto task = new FiberTask;
            assert((cast(size_t) task.control % cacheLineSize) == 0);
            task.domain = domain;
            task.waitSignal = 7;
            task.storeControl(controlWordFor(Phase.running));
            immutable armed = waits.linkArmed(task);
            assert(armed == ((1UL << waitCauseBits) | WaitCause.armed));
            tasks ~= task;
        }
        foreach (i; 0 .. count)
            if (i % 4 == 0)
            {
                // Woken elsewhere (say, a timeout), then unlinks itself.
                assert(tasks[i].claimCurrentWait(WaitCause.timedOut));
                waits.unlink(tasks[i]);
                assert(tasks[i].waitOwner is null);
            }
            else if (i % 4 == 1)
                // Claimed elsewhere but not yet resumed: still linked.
                assert(tasks[i].claimCurrentWait(WaitCause.cancelled));
        assert(domain.waiting == count / 2);
        size_t claimed;
        auto chain = waits.claimAll(7, claimed);
        assert(claimed == count / 2 && domain.waiting == 0);
        size_t seen;
        for (auto task = chain; task !is null; task = task.waitNext)
        {
            assert(task is tasks[(seen / 2) * 4 + 2 + seen % 2], "oldest first");
            assert(task.waitCause == WaitCause.signalled && task.waitOwner is null);
            ++seen;
        }
        assert(seen == claimed);
        foreach (task; tasks)
        {
            assert(task.waitOwner is null);
            waits.unlink(task); // late self-unlink after a claim is a no-op
        }
        assert(waits.claimAll(7, claimed) is null && claimed == 0);
    }

    unittest
    {
        // PARK/WAKE fetch_add composition: exactly one publisher either order.
        shared ulong word;
        auto old = atomicFetchAdd(word, ctrlPark);
        assert(ctrlField(old, ctrlWake) == 0);
        old = atomicFetchAdd(word, ctrlWake);
        assert(ctrlField(old, ctrlPark) == 1);
        atomicStore(word, 0UL);
        old = atomicFetchAdd(word, ctrlWake);
        assert(ctrlField(old, ctrlPark) == 0);
        old = atomicFetchAdd(word, ctrlPark);
        assert(ctrlField(old, ctrlWake) == 1);
        assert(phaseOf(atomicLoad(word)) == Phase.ready);

        atomicStore(word, 0UL);
        old = atomicFetchAdd(word, ctrlCancel);
        assert((old & ctrlEnterMask) == 0);
        old = atomicFetchAdd(word, ctrlEnter);
        assert((old & ctrlCancelMask) != 0 && (old & ctrlEnterMask) == 0);
        atomicStore(word, 0UL);
        old = atomicFetchAdd(word, ctrlEnter);
        assert((old & ctrlCancelMask) == 0);
        old = atomicFetchAdd(word, ctrlCancel);
        assert((old & ctrlEnterMask) != 0);
        assert(phaseOf(atomicLoad(word)) == Phase.ready);

        // Counted cancellation and terminal entry are ordered by their
        // returned old words. A late add remains counted but cannot change the
        // terminal cutoff snapshot.
        atomicStore(word, controlWordFor(Phase.running) | ctrlEnter);
        auto terminalOld = atomicFetchAdd(word,
            controlWordFor(Phase.terminating)
                - controlWordFor(Phase.running));
        old = atomicFetchAdd(word, ctrlCancel);
        assert(phaseOf(old) == Phase.terminating);
        assert((terminalOld & ctrlCancelMask) == 0);

        atomicStore(word, controlWordFor(Phase.running) | ctrlEnter);
        old = atomicFetchAdd(word, ctrlCancel);
        assert(phaseOf(old) == Phase.running);
        terminalOld = atomicFetchAdd(word,
            controlWordFor(Phase.terminating)
                - controlWordFor(Phase.running));
        assert(ctrlField(terminalOld, ctrlCancel) == 1);
    }

    unittest
    {
        // Heap order and arbitrary O(log N) cancellation, including stable
        // sequence ordering for equal deadlines.
        enum count = 100_000;
        auto timers = new TimerSet;
        FiberTask[] tasks;
        tasks.reserve(count);
        foreach_reverse (i; 0 .. count)
        {
            auto task = new FiberTask;
            task.payloadWord = i;
            tasks ~= task;
            timers.insert(task, cast(long)(i % 257), i, 0);
        }
        foreach (task; tasks)
            if ((task.payloadWord % 3) == 0)
            {
                auto removed = timers.remove(task);
                assert(removed);
            }
        long lastDeadline = long.min;
        ulong lastSequence;
        FiberTask task;
        size_t popped;
        ulong unusedWord;
        while (timers.takeExpired(long.max, task, unusedWord))
        {
            immutable deadline = cast(long)(task.payloadWord % 257);
            immutable sequence = task.payloadWord;
            assert(deadline > lastDeadline
                || (deadline == lastDeadline && sequence > lastSequence));
            lastDeadline = deadline;
            lastSequence = sequence;
            assert(task.timerIndex == size_t.max);
            ++popped;
        }
        assert(popped == count - (count + 2) / 3);
        long unused;
        auto hasDeadline = timers.nextDeadline(unused);
        assert(!hasDeadline);
    }

    unittest
    {
        // The pump's counted drain applies queued requests without
        // allocating on the worker.
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto domain = new FiberDomain(farm);
        auto token = farm.registerProducer(Tier.small);
        scope (exit) farm.unregisterProducer(token);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        auto first = domain.spawnHandle({});
        auto second = domain.spawnHandle({});
        assert(domain.requestCancel(first) == CancellationSubmission.queued);
        assert(domain.requestCancel(first) == CancellationSubmission.queued);
        assert(domain.requestCancel(second) == CancellationSubmission.queued);
        immutable before = GC.allocatedInCurrentThread;
        immutable applied = domain.drainCancellationRequestsCounted();
        assert(GC.allocatedInCurrentThread == before);
        assert(applied == 3 && domain.pendingCancellationRequests == 0);
        assert(domain.drainCancellationRequestsCounted() == 0);
        drainUntilEmpty(domain, token, view);
        // Retired, not yet reused: the handles still read their generation.
        assert(first.poll().status == FiberJoinStatus.cancelled);
        assert(second.poll().status == FiberJoinStatus.cancelled);
    }

    unittest
    {
        // Removal keeps the backing blocks. A timer insert after a removal and
        // a spawn after a root release append in place; a shrunk slice would
        // copy the whole heap or roots array into a new block every time.
        auto timers = new TimerSet;
        auto sleepers = new FiberTask[64];
        foreach (i, ref t; sleepers)
        {
            t = new FiberTask;
            timers.insert(t, i, i, 0);
        }
        auto heap = timers.storage.ptr;
        foreach (i, t; sleepers[0 .. 32])
        {
            assert(timers.remove(t));
            timers.insert(t, 100 + i, 100 + i, 0);
        }
        assert(timers.storage.ptr is heap);
        FiberTask expired;
        ulong expiredWord;
        while (timers.takeExpired(long.max, expired, expiredWord)) {}
        assert(timers.earliest == long.max);
        assert(timers.storage.ptr is heap);
        foreach (t; timers.storage) assert(t.task is null);

        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto domain = new FiberDomain(farm);
        auto token = farm.registerProducer(Tier.small);
        scope (exit) farm.unregisterProducer(token);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        // Run without retiring (flush and consume only): terminal tasks
        // stay rooted until `retire`.
        void run()
        {
            while (!domain.drained)
            {
                domain.flush(token);
                view.consumeNext();
            }
        }
        foreach (i; 0 .. 64) domain.spawn({});
        run();
        assert(domain.roots.length == 64);
        auto rootsBlock = domain.roots.ptr;
        foreach (i; 0 .. 32)
        {
            assert(domain.retire(1) == 1);
            domain.spawn({});
            run();
        }
        assert(domain.roots.length == 64);
        assert(domain.roots.ptr is rootsBlock);
        assert(domain.retire() == 64 && domain.roots.length == 0);
    }
}
