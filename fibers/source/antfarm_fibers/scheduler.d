/++
 + DRuntime Fiber scheduling layer for Ant Farm.
 +
 + Ant Farm is only the ready-work transport. Long waits live in `WaitSet`,
 + and newly-ready fibers collect in `PublishAccumulator` until a producer
 + flushes a useful table. No external thread-pool package is required.
 +/
module antfarm_fibers.scheduler;

import antfarm;
import antfarm_fibers.cacheline;
import core.atomic;
import core.memory : GC;
import core.sync.event : Event;
import core.sync.mutex : Mutex;
import core.thread : Fiber, Thread;
import core.time : Duration, MonoTime;


alias Signal = ulong;
private enum Signal timerSignalBit = 1UL << 63;
enum size_t defaultLifecycleRetention = 65_536;
private enum uint lifecycleRecordsPerTask = 4;

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
    wrongBackend,
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

/// Result of submitting a generation-paired request to the Director control
/// lane. Submission does not imply that cancellation beat terminal entry.
enum CancellationSubmission : ubyte
{
    queued,
    staleHandle,
}

/// Director-side result for one queued remote cancellation request.
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
/// backend-local and assigned by the Director-side drain in queue order.
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

alias FiberLifecycleHandler = void delegate(ref const FiberLifecycleEvent);

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
        "antfarm_fibers: Fiber.yield without scheduler suspend");
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
        if (FiberBackend.currentTask !is null)
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
        if (FiberBackend.currentTask !is null)
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
    /// task in the same backend terminates. Completion registration and the
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
        auto waiter = FiberBackend.currentTask;
        if (waiter is null)
            return FiberJoinResult(
                TaskHandle(cast(FiberTask) slot, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        auto target = cast(FiberTask) slot;
        if (target is waiter)
            return FiberJoinResult(
                TaskHandle(target, generation, diagnosticId),
                FiberJoinStatus.wouldBlock);
        if (target !is null && target.backend !is waiter.backend)
            return FiberJoinResult(
                TaskHandle(target, generation, diagnosticId),
                FiberJoinStatus.wrongBackend);

        while (true)
        {
            auto observed = poll();
            if (observed.status != FiberJoinStatus.pending) return observed;
            if (timed && MonoTime.currTime >= deadline)
            {
                observed.status = FiberJoinStatus.timedOut;
                return observed;
            }
            if (!target.registerManagedJoinWaiter(waiter, generation))
            {
                if (waiter.cancellationRequested) throw new FiberCancelled;
                continue;
            }
            if (timed)
            {
                auto backend = waiter.backend;
                immutable seq = atomicFetchAdd(backend.timerSequence, 1UL);
                try
                    backend.timers.insert(waiter, deadline.ticks, seq);
                catch (Throwable failure)
                {
                    target.removeManagedJoinWaiter(waiter);
                    backend.timers.remove(waiter);
                    backend.restoreRunningAfterWaitSetupFailure(waiter);
                    throw failure;
                }
                // Completion can detach and wake us before timer insertion.
                if (!target.hasManagedJoinWaiter(waiter, generation))
                    backend.timers.remove(waiter);
            }
            Fiber.yield();
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

private final class LifecycleEventNode
{
    FiberTask owner;
    LifecycleEventNode next;
    FiberLifecycleEvent event;

    this(FiberTask owner) { this.owner = owner; }
}

static assert(size_t.sizeof == 8, "antfarm_fibers is 64-bit only");

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
    shared uint pendingLifecycleEvents;
    shared ubyte cancellationEventEmitted;
    shared ubyte completionTaken;
    shared ubyte cancellationDispositionState;
    shared ubyte lifecycleReservations;
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
/// active backend root until the membership is detached.
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
    ubyte[32] membershipPadding;
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

align(64) private struct FiberBackendControl
{
    shared int accepting;
    ubyte[60] acceptingPadding;
    shared long activeCount;
    ubyte[56] activePadding;
    shared ulong timerSequence;
    ubyte[56] timerPadding;
    shared int fatalState;
    ubyte[60] fatalPadding;
    shared long managedJoinWaiterCount;
    ubyte[56] managedJoinWaiterPadding;
    shared long lifecycleReservationCount;
    ubyte[56] lifecycleReservationPadding;
}
static assert(FiberBackendControl.sizeof == 6 * cacheLineSize);
static assert(FiberBackendControl.managedJoinWaiterCount.offsetof
    == 4 * cacheLineSize);
static assert(FiberBackendControl.lifecycleReservationCount.offsetof
    == 5 * cacheLineSize);

align(64) private struct LifecycleQueueControl
{
    shared size_t headWord;
    shared long count;
    ubyte[48] padding;
}
static assert(LifecycleQueueControl.sizeof == cacheLineSize);

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
    package FiberBackend backend;
    package FiberReadyLane readyLane;
    package Fiber fiber;
    package Signal waitSignal;
    package shared(Throwable) failure;
    /// Publish chain link; owned by a PublishAccumulator or its drainer.
    package shared(FiberTask) queueNext;
    /// WaitSet bucket link, and pool freelist link; each container holds a
    /// task at disjoint times, both under their own lock.
    package FiberTask waitNext;
    /// WaitSet-only predecessor. Together with waitNext this permits O(1)
    /// cancellation from a shared-signal bucket under its stripe lock.
    package FiberTask waitPrev;
    /// Generation installed in the WaitSet. Guards an unlink against a stale
    /// membership observation after task recycling.
    package ulong waitGeneration;
    /// Timer heap position, protected by TimerSet.mutex. size_t.max means the
    /// task has no live timer registration.
    package size_t timerIndex = size_t.max;
    /// The task IS its Ant Farm payload: flush publishes this word verbatim.
    package ulong payloadWord;
    /// Requested stack size; recycled tasks keep their original stack.
    package size_t stackSize;
    /// Position in the backend's roots array; maintained under rootsMutex so
    /// completion draining is an O(1) swap-removal per task.
    package size_t rootsIndex;
    /// Double-release guard for the recycling pool (pool mutex held).
    package bool pooledFlag;
    package bool rootedFlag;
    private LifecycleEventNode[4] lifecycleNodes;

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
                assert(before == 0, "antfarm_fibers: control field already raised");
            else if (addend == ctrlCancel)
                assert(before < 2,
                    "antfarm_fibers: more than local+Director CANCEL sources");
        }
        return atomicFetchAdd!(MemoryOrder.acq_rel)(controlWord, addend);
    }

    /// Exclusive-owner phase change. Composes with concurrent event adds.
    package ulong addPhase(Phase from, Phase to) nothrow @nogc
    {
        debug assert(phaseOf(loadControl()) == from,
            "antfarm_fibers: phase add from mismatched current phase");
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
    @property package ref shared(uint) pendingLifecycleEvents() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).pendingLifecycleEvents; }
    @property package ref shared(ubyte) cancellationEventEmitted() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).cancellationEventEmitted; }
    @property package ref shared(ubyte) completionTaken() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).completionTaken; }
    @property package ref shared(ubyte) cancellationDispositionState() const return nothrow @nogc
    { return (cast(FiberTaskControl*) control).cancellationDispositionState; }
    @property private ref shared(ubyte) lifecycleReservations() const
        return nothrow @nogc
    { return (cast(FiberTaskControl*) control).lifecycleReservations; }
    @property private ref shared(uint) joinState() const return nothrow @nogc
    { return (cast(FiberJoinControl*) joinControl).state; }

    this(FiberBackend backend, FiberReadyLane readyLane,
         void delegate() body, size_t stackSize = 0)
    {
        initializeTask(backend, readyLane, stackSize);
        fiber = stackSize == 0 ? new Fiber(body) : new Fiber(body, stackSize);
    }

    this(FiberBackend backend, FiberReadyLane readyLane,
         void function() body, size_t stackSize = 0)
    {
        initializeTask(backend, readyLane, stackSize);
        fiber = stackSize == 0 ? new Fiber(body) : new Fiber(body, stackSize);
    }

    /// PublishAccumulator stub node: never resumed, never published.
    private this() nothrow @nogc { initializeControl(); }

    ~this() nothrow @nogc
    {
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

    private void initializeControl() nothrow @nogc
    {
        control = allocateCacheLine!FiberTaskControl();
        atomicStore!(MemoryOrder.raw)(control.controlWord, controlWordFor(Phase.ready));
        atomicStore!(MemoryOrder.raw)(control.outcomeState, FiberOutcome.pending);
    }

    private void initializeTask(FiberBackend backend, FiberReadyLane readyLane,
                                size_t stackSize)
    {
        initializeControl();
        initializeJoinControl();
        managedJoinControl = allocateCacheLine!FiberManagedJoinControl();
        this.backend = backend;
        this.readyLane = readyLane;
        this.stackSize = stackSize;
        payloadWord = cast(ulong) cast(void*) this;
        if (backend.lifecycleEventsEnabled) ensureLifecycleNodes();
    }

    private void initializeJoinControl() nothrow @nogc
    {
        joinControl = allocateCacheLine!FiberJoinControl();
        joinControl.terminalEvent.initialize(true, false);
    }

    private void ensureLifecycleNodes()
    {
        foreach (i, ref node; lifecycleNodes)
            if (node is null) node = new LifecycleEventNode(this);
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
        if ((waiter.loadControl() & ctrlCancelMask) != 0)
            return false;
        waiter.addPhase(Phase.running, Phase.waiting);
        if ((waiter.loadControl() & ctrlCancelMask) != 0)
        {
            waiter.addPhase(Phase.waiting, Phase.running);
            return false;
        }
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
        atomicFetchAdd!(MemoryOrder.rel)(backend.managedJoinWaiterCount, 1L);
        return true;
    }

    private bool hasManagedJoinWaiter(FiberTask waiter,
        ulong expectedGeneration) nothrow @nogc
    {
        lockManagedJoinList();
        scope (exit) unlockManagedJoinList();
        return atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.targetWord)
                == cast(size_t) cast(void*) this
            && atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.targetGeneration)
                == expectedGeneration;
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
        atomicFetchSub!(MemoryOrder.rel)(backend.managedJoinWaiterCount, 1L);
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
        // Once terminal entry has taken its atomic cutoff, a later Director
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
}

/// Fibers parked for an indefinitely distant signal. The mutex is deliberately
/// off the Ant Farm hot path: waits and notifications are scheduler events.
final class WaitSet
{
    private WaitSetControl* control;
    private enum stripeBits = 6;
    private enum stripeCount = 1 << stripeBits;

    private struct Bucket
    {
        FiberTask head; // intrusive chain through waitNext, newest first
        size_t count;
    }

    private Mutex[stripeCount] mutexes;
    private Bucket[Signal][stripeCount] buckets;

    @property private ref shared(long) waiterCount() const return nothrow @nogc
    { return (cast(WaitSetControl*) control).waiterCount; }

    this()
    {
        control = allocateCacheLine!WaitSetControl();
        foreach (ref m; mutexes) m = new Mutex;
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

    private static size_t stripe(Signal signal) nothrow @nogc
    {
        return cast(size_t)(signal * 0x9E37_79B9_7F4A_7C15UL) >> (64 - stripeBits);
    }

    package bool insert(FiberTask task, Signal signal)
    {
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            // Phase add stays inside the stripe lock: a signal that takes
            // the bucket must never observe waiting before the node is linked.
            task.addPhase(Phase.running, Phase.waiting);
            if ((task.loadControl() & ctrlCancelMask) != 0)
            {
                task.addPhase(Phase.waiting, Phase.running);
                return false;
            }
            if (auto p = signal in buckets[s])
            {
                task.waitNext = p.head;
                task.waitPrev = null;
                p.head.waitPrev = task;
                p.head = task;
                ++p.count;
            }
            else
            {
                task.waitNext = null;
                task.waitPrev = null;
                buckets[s][signal] = Bucket(task, 1);
            }
            task.waitGeneration = task.generation;
            atomicFetchAdd!(MemoryOrder.rel)(waiterCount, 1L);
            return true;
        }
    }

    /// Remove and return the intrusive chain of all fibers waiting for
    /// `signal`; `count` receives the chain length.
    package FiberTask take(Signal signal, out size_t count)
    {
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            if (auto p = signal in buckets[s])
            {
                auto bucket = *p;
                buckets[s].remove(signal);
                atomicFetchSub!(MemoryOrder.rel)(waiterCount, cast(long) bucket.count);
                count = bucket.count;
                return bucket.head;
            }
        }
        count = 0;
        return null;
    }

    /// Remove one particular waiter without disturbing other fibers sharing
    /// the signal. False means signal delivery already won the race.
    package bool remove(FiberTask task, Signal signal)
    {
        immutable s = stripe(signal);
        synchronized (mutexes[s])
        {
            auto p = signal in buckets[s];
            if (p is null || task.waitSignal != signal
                || task.waitGeneration != task.generation)
                return false;
            if (phaseOf(task.loadControl()) != Phase.waiting)
                return false;
            auto prev = task.waitPrev;
            auto next = task.waitNext;
            if (prev is null)
            {
                if (p.head !is task) return false;
                p.head = next;
            }
            else
            {
                if (prev.waitNext !is task) return false;
                prev.waitNext = next;
            }
            if (next !is null) next.waitPrev = prev;
            task.waitNext = null;
            task.waitPrev = null;
            task.waitGeneration = 0;
            if (--p.count == 0) buckets[s].remove(signal);
            atomicFetchSub!(MemoryOrder.rel)(waiterCount, 1L);
            return true;
        }
        return false;
    }

    @property size_t length() nothrow @nogc
    {
        return cast(size_t) atomicLoad!(MemoryOrder.acq)(waiterCount);
    }
}

private struct TimerEntry
{
    long deadlineTicks;
    ulong sequence;
    FiberTask task;
}

/// Managed timer registry. Expiration is polled by lane workers; the nearest
/// deadline is handed to threadpool so an idle worker wakes precisely for it.
final class TimerSet
{
    private Mutex mutex;
    private TimerEntry[] entries;

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

    void insert(FiberTask task, long deadlineTicks, ulong sequence)
    {
        synchronized (mutex)
        {
            assert(task.timerIndex == size_t.max);
            task.timerIndex = entries.length;
            entries ~= TimerEntry(deadlineTicks, sequence, task);
            siftUp(task.timerIndex);
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
        entries = entries[0 .. last];
        removed.timerIndex = size_t.max;
        if (i < entries.length)
        {
            if (i != 0 && before(entries[i], entries[(i - 1) / 2]))
                siftUp(i);
            else
                siftDown(i);
        }
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

    bool takeExpired(long nowTicks, out FiberTask task)
    {
        synchronized (mutex)
        {
            if (entries.length == 0 || entries[0].deadlineTicks > nowTicks)
            {
                task = null;
                return false;
            }
            task = removeAt(0);
            return true;
        }
    }

    bool nextDeadline(out long ticks)
    {
        synchronized (mutex)
        {
            if (entries.length == 0) return false;
            ticks = entries[0].deadlineTicks;
            return true;
        }
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
private final class LifecycleEventQueue
{
    private LifecycleQueueControl* control;

    this() { control = allocateCacheLine!LifecycleQueueControl(); }
    ~this() nothrow @nogc { freeCacheLine(control); }

    void push(LifecycleEventNode node) nothrow @nogc
    {
        atomicFetchAdd!(MemoryOrder.rel)(control.count, 1L);
        auto observed = atomicLoad!(MemoryOrder.acq)(control.headWord);
        while (true)
        {
            node.next = cast(LifecycleEventNode) cast(void*) observed;
            immutable replacement = cast(size_t) cast(void*) node;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &control.headWord, observed, replacement))
                return;
            observed = atomicLoad!(MemoryOrder.acq)(control.headWord);
        }
    }

    LifecycleEventNode takeAll() nothrow @nogc
    {
        auto word = atomicExchange!(MemoryOrder.acq_rel)(
            &control.headWord, size_t.init);
        auto node = cast(LifecycleEventNode) cast(void*) word;
        LifecycleEventNode ordered;
        size_t count;
        while (node !is null)
        {
            auto next = node.next;
            node.next = ordered;
            ordered = node;
            node = next;
            ++count;
        }
        if (count != 0)
            atomicFetchSub!(MemoryOrder.rel)(control.count, cast(long) count);
        return ordered;
    }

    @property size_t length() const nothrow @nogc
    {
        immutable count = atomicLoad!(MemoryOrder.acq)(control.count);
        return count < 0 ? 0 : cast(size_t) count;
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
    private PayloadHeader resumeHeader;
    private enum snapshotCap = 256;

    this(AntFarm* farm)
    {
        if (farm is null)
            throw new Exception("antfarm_fibers: ready lane requires a farm");
        this.farm = farm;
        readyControl = allocateCacheLine!FiberReadyLaneControl();
        publishable = new PublishAccumulator;
        resumeHeader.maxCs = 1;
        resumeHeader.done = 1;
        resumeHeader.plen = 1;
        resumeHeader.call = &resumeCallback;
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

    /// Publish up to `maximum` accumulated activations using a caller-owned
    /// producer token for this lane's Farm.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = 2)
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
            written = cast(size_t) farm.write(
                resumeHeader, bodies, 1, token, avgCost);
            if (written < n)
            {
                foreach (i; written .. n - 1)
                    atomicStore!(MemoryOrder.raw)(nodes[i].queueNext,
                        cast(shared(FiberTask)) nodes[i + 1]);
                publishable.putBackChain(nodes[written], nodes[n - 1], n - written);
            }
            if (written != 0) notePublished(written);
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

    package void noteEntered() nothrow @nogc
    {
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
            readyControl.publishedCount, 1L);
        assert(previous > 0);
    }

    private static long resumeCallback(PayloadHeader*, PayloadBody body, ulong)
        nothrow @nogc @system
    {
        if (body.length != 1) return 0;
        auto task = cast(FiberTask) cast(void*) body[0];
        task.readyLane.noteEntered();
        task.backend.resume(task);
        return 1;
    }

}


/// Owns task generations, roots, waits, timers, cancellation, completion, and
/// lifecycle delivery across one or more ready lanes. Worker creation,
/// affinity, Farm consumption, cadence, and parking remain the embedding's
/// responsibility.
final class FiberDomain
{
    private FiberBackendControl* control;
    private WaitSet waits;
    private TimerSet timers;
    private FiberReadyLane[] readyLanes;
    private FiberReadyLane defaultReadyLane;
    private PublishAccumulator completions;
    private LifecycleEventQueue lifecycleQueue;
    private LifecycleEventNode lifecycleBacklog;
    private size_t lifecycleBacklogCount;
    private bool lifecycleEventsEnabled;
    private size_t lifecycleRetentionLimitState;
    private ulong lifecycleDrainSequence;
    private shared(Throwable) fatalFailure;
    private Mutex admissionMutex;
    private Mutex directorMutex;
    private Thread directorThread;
    private Mutex cancellationRequestMutex;
    private TaskHandle[] cancellationRequests;
    private Mutex rootsMutex;
    private FiberTask[] roots; // keep tasks alive while only ring pointers exist
    private bool backendRooted; // roots this owner while roots[] is nonempty
    private Mutex poolMutex;
    private FiberTask[size_t] pool; // stack size → freelist of terminated tasks
    private static FiberTask currentTask;

    @property private ref shared(int) accepting() const return nothrow @nogc
    { return (cast(FiberBackendControl*) control).accepting; }
    @property private ref shared(long) activeCount() const return nothrow @nogc
    { return (cast(FiberBackendControl*) control).activeCount; }
    @property private ref shared(ulong) timerSequence() const return nothrow @nogc
    { return (cast(FiberBackendControl*) control).timerSequence; }
    @property private ref shared(int) fatalState() const return nothrow @nogc
    { return (cast(FiberBackendControl*) control).fatalState; }
    @property private ref shared(long) managedJoinWaiterCount() const
        return nothrow @nogc
    { return (cast(FiberBackendControl*) control).managedJoinWaiterCount; }
    @property private ref shared(long) lifecycleReservationCount() const
        return nothrow @nogc
    { return (cast(FiberBackendControl*) control).lifecycleReservationCount; }

    this()
    {
        control = allocateCacheLine!FiberBackendControl();
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

    /// Compatibility construction for a one-lane domain.
    this(AntFarm* farm, size_t accumulatorCapacity = 65_536)
    {
        this();
        // Retained for source compatibility; intrusive queues are unbounded.
        cast(void) accumulatorCapacity;
        defaultReadyLane = new FiberReadyLane(farm);
        attachReadyLane(defaultReadyLane);
    }

    ~this() nothrow @nogc { freeCacheLine(control); }

    /// Attach a ready lane during setup. The first lane is the compatibility
    /// default used by `spawn` and `flush`; lane-aware embeddings call
    /// `spawnOn` and flush the selected lane directly.
    package void attachReadyLane(FiberReadyLane lane)
    {
        if (lane is null)
            throw new Exception("antfarm_fibers: cannot attach a null lane");
        synchronized (admissionMutex)
        {
            if (active != 0 || roots.length != 0)
                throw new Exception(
                    "antfarm_fibers: attach ready lanes before spawning");
            if (lane.ownerDomain !is null && lane.ownerDomain !is this)
                throw new Exception(
                    "antfarm_fibers: ready lane already belongs to a domain");
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
        return task !is null && task.backend is this
            && task.generation == handle.generation
            ? task.readyLane : null;
    }

    /// Enable ordered lifecycle records with a hard reservation bound.
    /// Setup-time only, before any spawn. Each admitted task reserves its
    /// maximum four records until records are acknowledged and unused terminal
    /// capacity is returned.
    void enableLifecycleEvents(
        size_t retentionLimit = defaultLifecycleRetention)
    {
        if (retentionLimit < lifecycleRecordsPerTask
            || retentionLimit > cast(size_t) long.max)
            throw new Exception(
                "antfarm_fibers: lifecycle retention limit must be 4..long.max");
        synchronized (admissionMutex)
        {
            if (active != 0 || roots.length != 0)
                throw new Exception(
                    "antfarm_fibers: enable lifecycle events before spawning");
            if (!lifecycleEventsEnabled)
            {
                lifecycleQueue = new LifecycleEventQueue;
                lifecycleEventsEnabled = true;
                lifecycleRetentionLimitState = retentionLimit;
            }
            else if (lifecycleRetentionLimitState != retentionLimit)
                throw new Exception(
                    "antfarm_fibers: lifecycle retention already configured");
        }
    }

    @property bool lifecycleEnabled() const nothrow @nogc
    {
        return lifecycleEventsEnabled;
    }

    @property size_t pendingEvents() const nothrow @nogc
    {
        return lifecycleQueue is null ? 0
            : lifecycleBacklogCount + lifecycleQueue.length;
    }

    @property size_t lifecycleReserved() const nothrow @nogc
    {
        immutable count = atomicLoad!(MemoryOrder.acq)(
            lifecycleReservationCount);
        return count < 0 ? 0 : cast(size_t) count;
    }

    @property size_t lifecycleRetentionLimit() const nothrow @nogc
    {
        return lifecycleRetentionLimitState;
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

    /// Drain all currently published lifecycle records in MPSC enqueue order.
    /// Director/control-side only. The returned copy owns any failure refs;
    /// draining may release the last scheduler root of completed tasks.
    FiberLifecycleEvent[] takeLifecycleEvents(size_t maximum = size_t.max)
    {
        if (lifecycleQueue is null || maximum == 0) return null;
        auto chain = takeLifecycleChain();
        if (chain is null) return null;
        size_t available;
        for (auto node = chain; node !is null; node = node.next) ++available;
        immutable count = available < maximum ? available : maximum;
        auto result = new FiberLifecycleEvent[count];
        size_t i;
        while (chain !is null && i != count)
        {
            auto node = chain;
            chain = node.next;
            node.next = null;
            assignLifecycleSequence(node);
            result[i++] = node.event;
            acknowledgeLifecycleNode(node);
        }
        retainLifecycleBacklog(chain, available - count);
        return result;
    }

    /// Invoke a GC-enabled Director-side handler in lifecycle order. A record
    /// is acknowledged only after the handler returns normally. If it throws,
    /// that record and every later record remain queued for retry with stable
    /// sequence numbers.
    size_t handleLifecycleEvents(FiberLifecycleHandler handler,
        size_t maximum = size_t.max)
    {
        if (handler is null)
            throw new Exception("antfarm_fibers: null lifecycle handler");
        if (lifecycleQueue is null || maximum == 0) return 0;
        auto chain = takeLifecycleChain();
        if (chain is null) return 0;
        size_t available;
        for (auto node = chain; node !is null; node = node.next) ++available;
        size_t handled;
        while (chain !is null && handled != maximum)
        {
            auto node = chain;
            assignLifecycleSequence(node);
            try handler(node.event);
            catch (Throwable failure)
            {
                retainLifecycleBacklog(node, available - handled);
                throw failure;
            }
            chain = node.next;
            node.next = null;
            acknowledgeLifecycleNode(node);
            ++handled;
        }
        retainLifecycleBacklog(chain, available - handled);
        return handled;
    }

    private LifecycleEventNode takeLifecycleChain() nothrow @nogc
    {
        if (lifecycleBacklog !is null)
        {
            auto chain = lifecycleBacklog;
            lifecycleBacklog = null;
            lifecycleBacklogCount = 0;
            return chain;
        }
        return lifecycleQueue.takeAll();
    }

    private void retainLifecycleBacklog(LifecycleEventNode chain, size_t count)
        nothrow @nogc
    {
        assert((chain is null) == (count == 0));
        lifecycleBacklog = chain;
        lifecycleBacklogCount = count;
    }

    private void assignLifecycleSequence(LifecycleEventNode node)
        nothrow @nogc
    {
        if (node.event.sequence == 0)
            node.event.sequence = ++lifecycleDrainSequence;
    }

    private void acknowledgeLifecycleNode(LifecycleEventNode node)
    {
        node.event = FiberLifecycleEvent.init;
        immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
            node.owner.pendingLifecycleEvents, 1u);
        assert(previous != 0);
        releaseLifecycleReservations(node.owner, 1);
        if (previous == 1
            && atomicLoad!(MemoryOrder.acq)(node.owner.completionTaken) != 0)
            releaseTaskRoot(node.owner);
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
    package FiberTask spawnOn(Body)(FiberReadyLane lane, Body body,
                                    size_t stackSize = 0)
        if (is(Body == void delegate()) || is(Body == void function()))
    {
        if (lane is null || lane.ownerDomain !is this)
            throw new Exception(
                "antfarm_fibers: spawn requires a lane in this domain");
        FiberTask task;
        bool recycled;
        synchronized (admissionMutex)
        {
            if (atomicLoad!(MemoryOrder.acq)(accepting) == 0)
                throw new Exception("antfarm_fibers: backend is shutting down");
            if (lifecycleEventsEnabled)
            {
                immutable reserved = lifecycleReserved;
                if (reserved > lifecycleRetentionLimitState
                        - lifecycleRecordsPerTask)
                    throw new FiberLifecycleBackpressure(
                        lifecycleRetentionLimitState, reserved);
            }
            synchronized (poolMutex)
            {
                if (auto head = stackSize in pool)
                {
                    task = *head;
                    auto next = task.waitNext;
                    if (next is null)
                        pool.remove(stackSize);
                    else
                        *head = next;
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
                assert(atomicLoad!(MemoryOrder.acq)(task.pendingLifecycleEvents) == 0);
                assert(atomicLoad!(MemoryOrder.acq)(
                    task.lifecycleReservations) == 0);
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
                task.waitGeneration = 0;
                task.timerIndex = size_t.max;
                atomicStore!(MemoryOrder.rel)(task.outcomeState, FiberOutcome.pending);
                task.storeControl(controlWordFor(Phase.ready));
                atomicStore!(MemoryOrder.rel)(task.lastResumeIdentityState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.resumeCountState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.migrationCountState, 0UL);
                atomicStore!(MemoryOrder.rel)(task.cancellationEventEmitted,
                    cast(ubyte) 0);
                atomicStore!(MemoryOrder.rel)(task.completionTaken,
                    cast(ubyte) 0);
                atomicStore!(MemoryOrder.rel)(task.cancellationDispositionState,
                    cast(ubyte) CancellationDisposition.none);
                task.readyLane = lane;
                if (lifecycleEventsEnabled) task.ensureLifecycleNodes();
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
                if (!backendRooted)
                {
                    // The Farm stores task addresses as integer payload words,
                    // so root this GC owner before the first such publication.
                    GC.addRoot(cast(void*) this);
                    backendRooted = true;
                }
                task.rootsIndex = roots.length;
                task.rootedFlag = true;
                roots ~= task;
                if (lifecycleEventsEnabled)
                {
                    atomicStore!(MemoryOrder.rel)(task.lifecycleReservations,
                        cast(ubyte) lifecycleRecordsPerTask);
                    atomicFetchAdd!(MemoryOrder.rel)(lifecycleReservationCount,
                        cast(long) lifecycleRecordsPerTask);
                }
                atomicFetchAdd(activeCount, 1L);
            }
            emitLifecycle(task, FiberLifecycleKind.admitted);
            // Publication is inside the admission transaction. Shutdown either
            // precedes this entire generation or observes it in roots.
            task.readyLane.push(task);
        }
        return task;
    }

    /// Spawn and capture the admitted generation in one API operation.
    TaskHandle spawnHandle(void delegate() body, size_t stackSize = 0)
    {
        return spawn(body, stackSize).handle;
    }

    TaskHandle spawnHandle(void function() body, size_t stackSize = 0)
    {
        return spawn(body, stackSize).handle;
    }

    package TaskHandle spawnHandleOn(Body)(FiberReadyLane lane,
        Body body, size_t stackSize = 0)
        if (is(Body == void delegate()) || is(Body == void function()))
    {
        return spawnOn(lane, body, stackSize).handle;
    }

    /// Return a terminated task to the recycling pool. After release the
    /// handle must not be read again: a later spawn hands the same object to
    /// a new fiber. Releasing a live or already-released task is a contract
    /// violation caught by assert.
    void release(FiberTask task)
    {
        requireDirector();
        requireCancellationRequestsDrained();
        releaseOne(task);
    }

    private void releaseOne(FiberTask task)
    {
        assert(task !is null && task.backend is this && task.terminated,
            "antfarm_fibers: only terminated tasks of this backend can be released");
        assert(atomicLoad!(MemoryOrder.acq)(task.completionTaken) != 0
            && atomicLoad!(MemoryOrder.acq)(task.pendingLifecycleEvents) == 0
            && atomicLoad!(MemoryOrder.acq)(task.lifecycleReservations) == 0
            && !task.rootedFlag,
            "antfarm_fibers: drain completion and lifecycle events before release");
        synchronized (poolMutex)
        {
            assert(!task.pooledFlag, "antfarm_fibers: task released twice");
            task.closeJoinWaiters();
            while (atomicLoad!(MemoryOrder.acq)(task.joinState)
                    != FiberTask.joinClosed)
                Thread.yield();
            task.pooledFlag = true;
            task.waitNext = pool.get(task.stackSize, null);
            pool[task.stackSize] = task;
        }
    }

    /// ditto, for a batch of completions.
    void releaseAll(FiberTask[] tasks)
    {
        requireDirector();
        requireCancellationRequestsDrained();
        foreach (task; tasks) releaseOne(task);
    }

    private void requireCancellationRequestsDrained()
    {
        if (pendingCancellationRequests != 0)
            throw new Exception(
                "antfarm_fibers: drain Director cancellation requests before release");
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
            throw new Exception("antfarm_fibers: high-bit signals are reserved for timers");
        parkOn(signal);
    }

    /// Park the current fiber on a scheduler-private wait key.
    package static void parkOn(Signal signal)
    {
        auto task = currentTask;
        assert(task !is null && task.fiber is Fiber.getThis());
        if (task.cancellationRequested)
            throw new FiberCancelled;
        task.waitSignal = signal;
        if (!task.backend.waits.insert(task, signal))
            throw new FiberCancelled;
        Fiber.yield();
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
    /// The high half of `Signal` is reserved for these internal timer keys.
    static void sleepUntil(MonoTime deadline)
    {
        auto task = currentTask;
        assert(task !is null && task.fiber is Fiber.getThis());
        if (task.cancellationRequested)
            throw new FiberCancelled;
        auto backend = task.backend;
        immutable seq = atomicFetchAdd(backend.timerSequence, 1UL);
        immutable key = timerSignalBit | (seq & ~timerSignalBit);
        task.waitSignal = key;
        if (!backend.waits.insert(task, key))
            throw new FiberCancelled;
        backend.timers.insert(task, deadline.ticks, seq);
        // Cancellation may have removed the waiter just before this insert.
        // In that ordering it could not remove a not-yet-present heap entry;
        // close the registration handshake from the running fiber side.
        if (task.cancellationRequested)
            backend.timers.remove(task);
        Fiber.yield();
        if (task.cancellationRequested)
            throw new FiberCancelled;
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
            throw new Exception("antfarm_fibers: high-bit signals are reserved for timers");
        return wakeWaiters(signal);
    }

    package Signal allocateWaitKey() nothrow @nogc
    {
        immutable seq = atomicFetchAdd(timerSequence, 1UL);
        return timerSignalBit | (seq & ~timerSignalBit);
    }

    package size_t wakeWaiters(Signal signal)
    {
        size_t matched;
        auto chain = waits.take(signal, matched);
        while (chain !is null)
        {
            auto task = chain;
            chain = task.waitNext;
            task.waitNext = null;
            task.waitPrev = null;
            task.waitGeneration = 0;
            makeWaiterReady(task);
        }
        return matched;
    }

    /// Turn expired timers into ordinary ready fibers. Called by managed lane
    /// workers before consuming/publishing work.
    size_t pollTimers(MonoTime now = MonoTime.currTime)
    {
        size_t woken;
        FiberTask task;
        while (timers.takeExpired(now.ticks, task))
        {
            auto target = task.managedJoinTarget();
            immutable removed = target !is null
                ? target.removeManagedJoinWaiter(task)
                : waits.remove(task, task.waitSignal);
            if (removed)
            {
                makeWaiterReady(task);
                ++woken;
            }
        }
        return woken;
    }

    bool nextTimerDeadline(out long ticks)
    {
        return timers.nextDeadline(ticks);
    }

    /// Verify the construction thread which owns control application and
    /// release/recycle for this domain.
    private void requireDirector()
    {
        auto self = Thread.getThis();
        synchronized (directorMutex)
        {
            if (directorThread !is self)
                throw new Exception(
                    "antfarm_fibers: Director operation from a non-owner thread");
        }
    }

    /// Queue a remote, generation-paired cancellation request. Any thread may
    /// submit; only the Director mutates the target control word.
    CancellationSubmission requestCancel(TaskHandle handle)
    {
        auto task = handle.slot;
        if (task is null || task.backend !is this || handle.generation == 0
            || task.generation != handle.generation)
            return CancellationSubmission.staleHandle;
        synchronized (cancellationRequestMutex)
            cancellationRequests ~= handle;
        return CancellationSubmission.queued;
    }

    CancellationSubmission requestCancel(FiberTask task)
    {
        return task is null ? CancellationSubmission.staleHandle
                            : requestCancel(task.handle);
    }

    /// Apply every queued request on the Director thread. Duplicate remote
    /// requests for one generation coalesce before another CANCEL add.
    CancellationApplication[] drainCancellationRequests()
    {
        requireDirector();
        TaskHandle[] requests;
        synchronized (cancellationRequestMutex)
        {
            requests = cancellationRequests;
            cancellationRequests = null;
        }
        auto results = new CancellationApplication[requests.length];
        foreach (i, handle; requests)
            results[i] = CancellationApplication(
                handle, applyCancellation(handle, true));
        return results;
    }

    @property size_t pendingCancellationRequests()
    {
        synchronized (cancellationRequestMutex)
            return cancellationRequests.length;
    }

    /// Direct cancellation by the currently executing managed Fiber. This is
    /// the only non-Director path which may add CANCEL to a task control word.
    static CancellationResult cancelCurrent()
    {
        auto task = currentTask;
        if (task is null)
            return CancellationResult.staleHandle;
        return task.backend.applyCancellation(task.handle, false);
    }

    /// Director-only synchronous cancellation, used for shutdown and for
    /// applying generation-paired requests from the control queue.
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
        return applyCancellation(handle, true);
    }

    /// Counted fetch_add arbitration shared by the local owner and Director.
    /// The old phase returned by the add decides whether this request beat the
    /// terminal cutoff. A late Director add is harmless and is not rolled back.
    private CancellationResult applyCancellation(TaskHandle handle,
                                                   bool director)
    {
        auto task = handle.slot;
        immutable requestedGeneration = handle.generation;
        if (task is null || task.backend !is this || requestedGeneration == 0
            || task.generation != requestedGeneration)
            return CancellationResult.staleHandle;

        immutable opening = task.loadControl();
        immutable openingPhase = phaseOf(opening);
        if (openingPhase == Phase.terminating
            || openingPhase == Phase.terminated)
            return CancellationResult.alreadyTerminal;

        // An accepted request from this generation already owns the semantic
        // cancellation. At most a simultaneously racing local/Director pair
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
        if (director && ph == Phase.waiting)
        {
            // First or later: WaitSet/join-list remove is exactly-once.
            auto target = task.managedJoinTarget();
            immutable removed = target !is null
                ? target.removeManagedJoinWaiter(task)
                : waits.remove(task, task.waitSignal);
            if (removed)
            {
                timers.remove(task);
                makeWaiterReady(task);
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
            auto expected = observed;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &task.control.cancelGeneration, expected, generation))
                return;
            observed = expected;
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
        if (!lifecycleEventsEnabled) return;
        assert(atomicLoad!(MemoryOrder.acq)(task.lifecycleReservations) != 0,
            "antfarm_fibers: lifecycle event exceeded its reservation");
        auto node = task.lifecycleNodes[cast(size_t) kind];
        assert(node !is null && node.next is null);
        node.event = FiberLifecycleEvent(0, kind, task.handle, outcome,
                                         cancellation, disposition, failure);
        atomicFetchAdd!(MemoryOrder.rel)(task.pendingLifecycleEvents, 1u);
        lifecycleQueue.push(node);
    }

    private void releaseLifecycleReservations(FiberTask task, uint count)
        nothrow @nogc
    {
        if (count == 0) return;
        immutable previousTask = atomicFetchSub!(MemoryOrder.acq_rel)(
            task.lifecycleReservations, cast(ubyte) count);
        assert(previousTask >= count);
        immutable previousBackend = atomicFetchSub!(MemoryOrder.acq_rel)(
            lifecycleReservationCount, cast(long) count);
        assert(previousBackend >= count);
    }

    /// Reject future spawns. With cancellation, request cancellation for all
    /// active tasks and wake parked ones so fiber cleanup scopes can run.
    void beginShutdown(bool cancelActive = false)
    {
        requireDirector();
        cast(void) drainCancellationRequests();
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

    /// Compatibility flush for a one-lane domain. Multi-lane embeddings flush
    /// each selected `FiberReadyLane` with a token registered on its Farm.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = 2)
    {
        if (defaultReadyLane is null)
            throw new Exception("antfarm_fibers: domain has no ready lane");
        return defaultReadyLane.flush(token, maximum, avgCost);
    }

    @property size_t waiting()
    {
        immutable managed = atomicLoad!(MemoryOrder.acq)(managedJoinWaiterCount);
        return waits.length + (managed < 0 ? 0 : cast(size_t) managed);
    }
    @property size_t ready() nothrow @nogc
    {
        size_t total;
        foreach (lane; readyLanes) total += lane.ready;
        return total;
    }

    /// Drain terminal tasks for control-thread reporting and release the
    /// backend's GC roots. Returned tasks remain alive while the caller holds
    /// them and expose their outcome/exception.
    FiberTask[] takeCompletions(size_t maximum = size_t.max)
    {
        auto chain = completions.stealChain(maximum);
        if (chain.first is null) return null;
        auto result = new FiberTask[chain.length];
        size_t n;
        for (auto task = chain.first; task !is null;
             task = cast(FiberTask) atomicLoad!(MemoryOrder.raw)(task.queueNext))
            result[n++] = task;
        synchronized (rootsMutex)
        {
            foreach (done; result)
            {
                atomicStore!(MemoryOrder.rel)(done.completionTaken,
                    cast(ubyte) 1);
                if (atomicLoad!(MemoryOrder.acq)(
                        done.pendingLifecycleEvents) == 0)
                    releaseTaskRootLocked(done);
            }
        }
        return result;
    }

    private void releaseTaskRoot(FiberTask task)
    {
        synchronized (rootsMutex)
        {
            if (task.rootedFlag
                && atomicLoad!(MemoryOrder.acq)(task.completionTaken) != 0
                && atomicLoad!(MemoryOrder.acq)(
                    task.pendingLifecycleEvents) == 0)
                releaseTaskRootLocked(task);
        }
    }

    private void releaseTaskRootLocked(FiberTask task)
    {
        assert(task.rootedFlag && roots.length != 0);
        immutable i = task.rootsIndex;
        auto moved = roots[$ - 1];
        roots[i] = moved;
        moved.rootsIndex = i;
        roots.length = roots.length - 1;
        task.rootedFlag = false;
        if (roots.length == 0 && backendRooted)
        {
            GC.removeRoot(cast(void*) this);
            backendRooted = false;
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
        if (lifecycleEventsEnabled)
        {
            uint emitted = 2; // admitted + terminal
            if (atomicLoad!(MemoryOrder.acq)(task.cancellationEventEmitted) == 2)
                ++emitted;
            if (outcome == FiberOutcome.failed) ++emitted;
            assert(emitted <= lifecycleRecordsPerTask);
            releaseLifecycleReservations(task,
                lifecycleRecordsPerTask - emitted);
        }
        // Publish completion before dropping activeCount so an observer of
        // drained can take every terminal task.
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
        size_t detachedCount;
        // Mark every detached node before dropping the list lock. Cancellation
        // or expiry can then observe either linked membership and remove it,
        // or detached membership which this waker exclusively owns; it can
        // never edit the detached chain while it is being traversed.
        for (auto detached = word; detached != 0; )
        {
            ++detachedCount;
            auto waiter = cast(FiberTask) cast(void*) detached;
            detached = atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.nextWord);
            atomicStore!(MemoryOrder.rel)(
                waiter.managedJoinControl.targetWord, size_t.init);
        }
        if (detachedCount != 0)
            atomicFetchSub!(MemoryOrder.rel)(managedJoinWaiterCount,
                cast(long) detachedCount);
        target.unlockManagedJoinList();

        while (word != 0)
        {
            auto waiter = cast(FiberTask) cast(void*) word;
            word = atomicLoad!(MemoryOrder.raw)(
                waiter.managedJoinControl.nextWord);
            FiberTask.clearManagedJoinMembership(waiter);
            waiter.backend.timers.remove(waiter);
            waiter.backend.makeWaiterReady(waiter);
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

    private void restoreRunningAfterWaitSetupFailure(FiberTask task)
        nothrow @nogc
    {
        immutable word = task.loadControl();
        immutable ph = phaseOf(word);
        if (ph == Phase.running)
            return;
        assert(ph == Phase.waiting);
        task.addPhase(Phase.waiting, Phase.running);
    }

}

enum FiberEventMode : ubyte
{
    autoReset,
    manualReset,
}

/// Scheduler-aware event. `wait` parks a managed fiber; `set`/`reset` may run
/// on any thread. Auto-reset consumes the posted state for one waiter; extras
/// that were woken re-park. Manual-reset stays posted until `reset`.
final class FiberEvent
{
    private FiberDomain domain;
    private Signal key;
    private FiberEventMode mode;
    private shared int posted;

    this(FiberDomain domain, FiberEventMode mode = FiberEventMode.autoReset)
    {
        if (domain is null)
            throw new Exception("antfarm_fibers: FiberEvent requires a domain");
        this.domain = domain;
        this.mode = mode;
        this.key = domain.allocateWaitKey();
    }

    @property bool isSet() const nothrow @nogc
    {
        return atomicLoad!(MemoryOrder.acq)(posted) != 0;
    }

    void set()
    {
        atomicStore!(MemoryOrder.rel)(posted, 1);
        domain.wakeWaiters(key);
    }

    void reset() nothrow @nogc
    {
        atomicStore!(MemoryOrder.rel)(posted, 0);
    }

    /// Park the current managed fiber until this event is posted.
    void wait()
    {
        auto task = FiberDomain.currentTask;
        if (task is null || task.backend !is domain)
            throw new Exception(
                "antfarm_fibers: FiberEvent.wait requires a managed fiber of this domain");
        while (true)
        {
            if (consumePosted()) return;
            if (task.cancellationRequested)
                throw new FiberCancelled;
            task.waitSignal = key;
            if (!domain.waits.insert(task, key))
                throw new FiberCancelled;
            if (consumePosted())
            {
                if (domain.waits.remove(task, key))
                    domain.restoreRunningAfterWaitSetupFailure(task);
                else
                    Fiber.yield();
                if (task.cancellationRequested)
                    throw new FiberCancelled;
                return;
            }
            Fiber.yield();
            if (task.cancellationRequested)
                throw new FiberCancelled;
        }
    }

    private bool consumePosted() nothrow @nogc
    {
        if (mode == FiberEventMode.manualReset)
            return atomicLoad!(MemoryOrder.acq)(posted) != 0;
        int expected = 1;
        return cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(&posted, expected, 0);
    }
}

/// Counting semaphore. `wait` parks a managed fiber when the count is zero;
/// `post` may run on any thread. Extra waiters woken by a broadcast re-park
/// if they lose the decrement race.
final class FiberSemaphore
{
    private FiberDomain domain;
    private Signal key;
    private shared long permits;

    this(FiberDomain domain, long initial = 0)
    {
        if (domain is null)
            throw new Exception("antfarm_fibers: FiberSemaphore requires a domain");
        if (initial < 0)
            throw new Exception("antfarm_fibers: semaphore count must be >= 0");
        this.domain = domain;
        this.key = domain.allocateWaitKey();
        atomicStore!(MemoryOrder.raw)(permits, initial);
    }

    @property long available() const nothrow @nogc
    {
        immutable n = atomicLoad!(MemoryOrder.acq)(permits);
        return n < 0 ? 0 : n;
    }

    void post(long count = 1)
    {
        if (count <= 0) return;
        atomicFetchAdd!(MemoryOrder.rel)(permits, count);
        domain.wakeWaiters(key);
    }

    void wait()
    {
        auto task = FiberDomain.currentTask;
        if (task is null || task.backend !is domain)
            throw new Exception(
                "antfarm_fibers: FiberSemaphore.wait requires a managed fiber of this domain");
        while (true)
        {
            if (tryAcquire()) return;
            if (task.cancellationRequested)
                throw new FiberCancelled;
            task.waitSignal = key;
            if (!domain.waits.insert(task, key))
                throw new FiberCancelled;
            if (tryAcquire())
            {
                if (domain.waits.remove(task, key))
                    domain.restoreRunningAfterWaitSetupFailure(task);
                else
                    Fiber.yield();
                if (task.cancellationRequested)
                    throw new FiberCancelled;
                return;
            }
            Fiber.yield();
            if (task.cancellationRequested)
                throw new FiberCancelled;
        }
    }

    private bool tryAcquire() nothrow @nogc
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(permits);
        while (observed > 0)
        {
            auto expected = observed;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &permits, expected, observed - 1))
                return true;
            observed = expected;
        }
        return false;
    }
}

/// Source-compatible name for the former one-Farm scheduler owner. New code
/// may construct `FiberDomain()` and attach multiple `FiberLane`s.
alias FiberBackend = FiberDomain;

/// Subscribe a unique ConsumerView and keep the failure check in optimized
/// builds. The caller must unsubscribe the same instance.
long subscribeOrThrow(ref ConsumerView consumer, AntFarm* farm)
{
    immutable subscription = consumer.subscribe(farm);
    if (subscription < 0)
        throw new Exception("antfarm_fibers: consumer subscription failed");
    return subscription;
}

/// Drive a one-lane domain on the calling thread until every admitted task is
/// terminal. `consumer` is deliberately passed by reference because a
/// ConsumerView is a unique cursor and must never be copied.
void drainUntilEmpty(FiberDomain domain, ref Token token,
                     ref ConsumerView consumer, size_t flushBatch = 32,
                     uint avgCost = 2)
{
    if (domain is null)
        throw new Exception("antfarm_fibers: cannot drain a null domain");
    if (flushBatch == 0)
        throw new Exception("antfarm_fibers: flushBatch must be greater than zero");
    while (!domain.drained)
    {
        if (domain.pendingCancellationRequests != 0)
            cast(void) domain.drainCancellationRequests();
        domain.pollTimers();
        immutable flushed = domain.flush(token, flushBatch, avgCost);
        immutable consumed = consumer.consumeNext();
        if (!consumed && flushed == 0)
            Thread.yield();
    }
}

version (unittest)
{
    unittest
    {
        // Exercise indexed removal from one deliberately crowded bucket.
        enum count = 100_000;
        auto waits = new WaitSet;
        assert((cast(size_t) waits.control % cacheLineSize) == 0);
        FiberTask[] tasks;
        tasks.reserve(count);
        foreach (i; 0 .. count)
        {
            auto task = new FiberTask;
            assert((cast(size_t) task.control % cacheLineSize) == 0);
            atomicStore!(MemoryOrder.rel)(task.generationState, i + 1);
            task.storeControl(controlWordFor(Phase.running));
            task.waitSignal = 7;
            waits.insert(task, 7);
            tasks ~= task;
        }
        foreach (i; 0 .. count)
            if ((i & 1) == 0)
            {
                auto removed = waits.remove(tasks[i], 7);
                assert(removed);
            }
        assert(waits.length == count / 2);
        size_t remaining;
        auto chain = waits.take(7, remaining);
        assert(remaining == count / 2 && waits.length == 0);
        FiberTask previous;
        size_t seen;
        for (auto task = chain; task !is null; task = task.waitNext)
        {
            assert(task.waitPrev is previous);
            previous = task;
            ++seen;
        }
        assert(seen == remaining);
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
            timers.insert(task, cast(long)(i % 257), i);
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
        while (timers.takeExpired(long.max, task))
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
}
