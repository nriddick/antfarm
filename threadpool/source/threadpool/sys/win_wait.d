module threadpool.sys.win_wait;

version (Windows):

import core.atomic;
import core.sys.windows.basetsd : HANDLE;
import core.sys.windows.winbase : CreateEventW, INFINITE, ResetEvent, SetEvent, SetWaitableTimer,
    WaitForMultipleObjects;
import core.sys.windows.windef : DWORD, FALSE;
import core.sys.windows.winnt : LARGE_INTEGER;
import core.time : MonoTime;
import std.exception : enforce;
import threadpool.sys.win_bindings;

struct WaitSlot
{
    shared uint wakeWord;
}

__gshared WaitSlot[] gWait;

/// A worker's wake word. ARMED: it may be, or is about to be, in
/// `WaitOnAddress`. TIMED: it may be, or is about to be, waiting on its timer
/// and its wake event, so a waker signals the event instead.
private enum uint ACTIVE = 0, ARMED = 1, TIMED = 2;

/// What a park with a precise deadline waits on.
private struct TimedHandles
{
    HANDLE event; /// auto-reset: a waker's signal
    HANDLE timer; /// high-resolution waitable timer where the system has one
}

/// Workers with an index below this can park to a precise deadline; the rest
/// fall back to a millisecond timeout.
private enum maxTimedWorkers = 1024;

/// One pair per worker index, created on first use and kept for the life of
/// the process: a waker that outlives its pool then signals a live handle of
/// a later worker, which costs that worker one early return, and never a
/// closed one. A static array, so the waker's read cannot see it move.
private __gshared TimedHandles[maxTimedWorkers] gTimed;

private extern (Windows) HANDLE CreateWaitableTimerExW(void* attributes, const(wchar)* name,
    DWORD flags, DWORD access) @nogc nothrow;
private enum DWORD CREATE_WAITABLE_TIMER_HIGH_RESOLUTION = 0x2;
private enum DWORD TIMER_ALL_ACCESS = 0x1F0003;

/// One owning worker per slot. Arm before the final work/stop check.
bool prepareWorkerWait(uint workerIndex) @nogc nothrow
{
    if (workerIndex >= gWait.length) return false;
    atomicExchange!(MemoryOrder.acq_rel)(&gWait[workerIndex].wakeWord, ARMED);
    return true;
}

void cancelWorkerWait(uint workerIndex) @nogc nothrow
{
    if (workerIndex < gWait.length)
        atomicExchange!(MemoryOrder.acq_rel)(&gWait[workerIndex].wakeWord, ACTIVE);
}

void createWaitSlots(uint n)
{
    resolveOptionalApis();
    enforce(pWaitOnAddress !is null,
        "threadpool: WaitOnAddress unavailable (need Windows 10 1803+)");
    destroyWaitSlots();
    // A worker without a pair parks to the millisecond, as before; so does
    // one whose system has no high-resolution timer and gets an ordinary one.
    foreach (i; 0 .. n < maxTimedWorkers ? n : maxTimedWorkers)
    {
        auto h = &gTimed[i];
        if (h.event is null)
            h.event = CreateEventW(null, FALSE, FALSE, null);
        if (h.timer is null)
            h.timer = CreateWaitableTimerExW(null, null, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
                TIMER_ALL_ACCESS);
        if (h.timer is null)
            h.timer = CreateWaitableTimerExW(null, null, 0, TIMER_ALL_ACCESS);
    }
    gWait = new WaitSlot[](n);
}

void destroyWaitSlots()
{
    gWait = null;
}

/// True when worker `workerIndex` can park to a precise deadline.
bool hasTimedWait(uint workerIndex) @nogc nothrow
{
    return workerIndex < maxTimedWorkers && gTimed[workerIndex].event !is null
        && gTimed[workerIndex].timer !is null;
}

private void wake(WaitSlot* slot, uint workerIndex, bool all) @nogc nothrow
{
    // Even ACTIVE -> ACTIVE publishes into the worker's next arming RMW.
    immutable was = atomicExchange!(MemoryOrder.acq_rel)(&slot.wakeWord, ACTIVE);
    if (was == ARMED)
    {
        if (all) pWakeByAddressAll(cast(void*)&slot.wakeWord);
        else pWakeByAddressSingle(cast(void*)&slot.wakeWord);
    }
    else if (was == TIMED)
        SetEvent(gTimed[workerIndex].event); // TIMED implies the pair exists
}

void wakeWorker(uint workerIndex) @nogc nothrow
{
    // One load: length and pointer stay paired, and the local keeps the
    // slot array alive if destroyWaitSlots nulls gWait during this call.
    auto slots = gWait;
    if (workerIndex >= slots.length) return;
    wake(&slots[workerIndex], workerIndex, false);
}

void wakeAllForStop() @nogc nothrow
{
    foreach (i, ref slot; gWait)
        wake(&slot, cast(uint) i, true);
}

/// Requires prepareWorkerWait followed by a final empty work/stop check.
/// The caller cancels the armed state on every exit, including exceptions.
/// Returns false if the pool is stopping.
bool parkWorker(uint workerIndex, ref shared(int) runFlag, uint timeoutMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    if (atomicLoad!(MemoryOrder.acq)(gWait[workerIndex].wakeWord) != ARMED)
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    uint cmp = ARMED;
    pWaitOnAddress(cast(void*)&gWait[workerIndex].wakeWord, &cmp, 4, timeoutMs);
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}

/// As `parkWorker`, to a deadline. The park ends at a wake, at `deadlineTicks`
/// on `MonoTime` (held by the worker's timer: late by about half a millisecond
/// at the median, whatever the system timer period), or after `boundMs`
/// milliseconds (the wait's own timeout, which ends on the system timer tick;
/// `uint.max` for no bound), whichever comes first.
///
/// The worker moves its word from ARMED to TIMED before it waits. A wake
/// that lands before that makes the exchange fail and the park return at
/// once; one that lands after it signals the event. The event is reset
/// before the exchange, never after, so a signal meant for this park is
/// never cleared; a signal left over from an earlier park that arrives
/// after the reset ends this one early, which the caller's loop absorbs.
bool parkWorkerUntil(uint workerIndex, ref shared(int) runFlag, long deadlineTicks,
    uint boundMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    auto slot = &gWait[workerIndex];
    if (atomicLoad!(MemoryOrder.acq)(slot.wakeWord) != ARMED)
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    immutable remaining = deadlineTicks - MonoTime.currTime.ticks;
    if (remaining <= 0)
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    immutable tps = MonoTime.ticksPerSecond;
    if (!hasTimedWait(workerIndex))
        return parkWorker(workerIndex, runFlag, coarseMs(remaining, tps, boundMs));

    auto h = &gTimed[workerIndex];
    // Relative due time in 100 ns units, negative; at least one unit.
    LARGE_INTEGER due;
    immutable hnsecs = remaining / tps * 10_000_000 + remaining % tps * 10_000_000 / tps;
    due.QuadPart = hnsecs > 0 ? -hnsecs : -1;
    ResetEvent(h.event);
    if (!SetWaitableTimer(h.timer, &due, 0, null, null, FALSE))
        return parkWorker(workerIndex, runFlag, coarseMs(remaining, tps, boundMs));
    if (!cas(&slot.wakeWord, ARMED, TIMED))
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    HANDLE[2] handles = [h.event, h.timer];
    WaitForMultipleObjects(2, handles.ptr, FALSE, boundMs == uint.max ? INFINITE : boundMs);
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}

/// The millisecond timeout that stands in for a deadline `remaining` ticks
/// away when there is no timer: rounded up, at least 1, at most `boundMs`.
private uint coarseMs(long remaining, long ticksPerSecond, uint boundMs) @nogc nothrow pure @safe
{
    immutable whole = remaining / ticksPerSecond;
    if (whole >= boundMs / 1000 + 1) return boundMs;
    immutable ms = whole * 1000 + (remaining % ticksPerSecond * 1000 + ticksPerSecond - 1) / ticksPerSecond;
    if (ms <= 0) return 1 < boundMs ? 1 : boundMs;
    return ms < boundMs ? cast(uint) ms : boundMs;
}

unittest
{
    enum tps = 10_000_000L;
    assert(coarseMs(1, tps, uint.max) == 1);
    assert(coarseMs(tps / 1000, tps, uint.max) == 1);
    assert(coarseMs(tps / 1000 + 1, tps, uint.max) == 2);
    assert(coarseMs(5 * tps, tps, uint.max) == 5000);
    assert(coarseMs(5 * tps, tps, 20) == 20);
    assert(coarseMs(long.max, tps, uint.max) == uint.max);
    assert(coarseMs(long.max, tps, 7) == 7);
}

unittest
{
    // One thread plays the worker and the waker in turn.
    import core.time : msecs, seconds;

    auto savedSlots = gWait;
    scope (exit) gWait = savedSlots;
    createWaitSlots(1);
    assert(hasTimedWait(0));
    shared int run = 1;

    // A wake before the worker moves to TIMED: the park does not wait.
    assert(prepareWorkerWait(0));
    wakeWorker(0);
    auto began = MonoTime.currTime;
    assert(parkWorkerUntil(0, run, (began + 5.seconds).ticks, uint.max));
    assert(MonoTime.currTime - began < 1.seconds, "a wake before the park was lost");
    cancelWorkerWait(0);

    // A signal left by a waker of an earlier park: reset before this park
    // commits, so it does not end it. The park runs to its deadline.
    atomicStore(gWait[0].wakeWord, TIMED);
    wakeWorker(0);
    assert(atomicLoad(gWait[0].wakeWord) == ACTIVE);
    assert(prepareWorkerWait(0));
    began = MonoTime.currTime;
    immutable deadline = began + 30.msecs;
    assert(parkWorkerUntil(0, run, deadline.ticks, uint.max));
    assert(MonoTime.currTime >= deadline, "a stale signal ended a later park");
    assert(atomicLoad(gWait[0].wakeWord) == TIMED, "the timer, not a waker, ended the park");
    cancelWorkerWait(0);

    // The bound ends a park whose deadline is far off.
    assert(prepareWorkerWait(0));
    began = MonoTime.currTime;
    assert(parkWorkerUntil(0, run, (began + 5.seconds).ticks, 20));
    assert(MonoTime.currTime - began < 1.seconds, "the bound did not end the park");
    cancelWorkerWait(0);

    // A deadline already past, and a stopped pool, return at once.
    assert(prepareWorkerWait(0));
    assert(parkWorkerUntil(0, run, MonoTime.currTime.ticks - 1, uint.max));
    atomicStore(run, 0);
    assert(!parkWorkerUntil(0, run, (MonoTime.currTime + 5.seconds).ticks, uint.max));
    cancelWorkerWait(0);
}
