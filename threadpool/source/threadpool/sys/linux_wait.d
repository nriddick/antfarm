module threadpool.sys.linux_wait;

version (linux):

import core.atomic;
import core.sys.posix.time : timespec;
import core.time : MonoTime;
import threadpool.sys.linux_bindings : futexWaitTimeout, futexWaitUntil, futexWake;

struct WaitSlot
{
    shared uint wakeWord;
}

__gshared WaitSlot[] gWait;

private enum uint ACTIVE = 0, ARMED = 1;

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
    destroyWaitSlots();
    gWait = new WaitSlot[](n);
}

void destroyWaitSlots()
{
    gWait = null;
}

void wakeWorker(uint workerIndex) @nogc nothrow
{
    // One load: a concurrent destroyWaitSlots can null gWait between a
    // length check and the index, which is an ArrayIndexError. The local
    // keeps the old array alive for this call.
    auto slots = gWait;
    if (workerIndex >= slots.length) return;
    auto word = &slots[workerIndex].wakeWord;
    // Even ACTIVE -> ACTIVE publishes into the worker's next arming RMW.
    if (atomicExchange!(MemoryOrder.acq_rel)(word, ACTIVE) != ARMED) return;
    futexWake(word, 1);
}

void wakeAllForStop() @nogc nothrow
{
    foreach (ref slot; gWait)
    {
        if (atomicExchange!(MemoryOrder.acq_rel)(&slot.wakeWord, ACTIVE) != ARMED)
            continue;
        futexWake(&slot.wakeWord, int.max);
    }
}

/// Requires prepareWorkerWait followed by a final empty work/stop check.
/// The caller cancels the armed state on every exit, including exceptions.
/// `timeoutMs == uint.max` waits forever. Returns false if the pool is stopping.
bool parkWorker(uint workerIndex, ref shared(int) runFlag, uint timeoutMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    if (atomicLoad!(MemoryOrder.acq)(gWait[workerIndex].wakeWord) != ARMED)
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    if (timeoutMs == uint.max)
        futexWaitTimeout(&gWait[workerIndex].wakeWord, ARMED, null);
    else
    {
        timespec ts;
        ts.tv_sec = timeoutMs / 1000;
        ts.tv_nsec = (timeoutMs % 1000) * 1_000_000L;
        futexWaitTimeout(&gWait[workerIndex].wakeWord, ARMED, &ts);
    }
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}

/// Every worker can park to a precise deadline: the futex takes one.
bool hasTimedWait(uint workerIndex) @nogc nothrow
{
    return true;
}

/// As `parkWorker`, to a deadline. The park ends at a wake, at `deadlineTicks`
/// on `MonoTime`, or after `boundMs` milliseconds (`uint.max` for no bound),
/// whichever comes first. One futex wait with an absolute `CLOCK_MONOTONIC`
/// deadline, which is the clock `MonoTime` reads; the wake path is the same.
bool parkWorkerUntil(uint workerIndex, ref shared(int) runFlag, long deadlineTicks,
    uint boundMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    if (atomicLoad!(MemoryOrder.acq)(gWait[workerIndex].wakeWord) != ARMED)
        return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
    enum long nsPerSecond = 1_000_000_000;
    immutable tps = MonoTime.ticksPerSecond;
    static long toNanos(long ticks, long tps) @nogc nothrow pure @safe
    {
        // Whole seconds and the remainder apart, so the product cannot wrap.
        return ticks / tps * nsPerSecond + ticks % tps * nsPerSecond / tps;
    }
    long deadline = toNanos(deadlineTicks, tps);
    if (boundMs != uint.max)
    {
        immutable bound = toNanos(MonoTime.currTime.ticks, tps) + cast(long) boundMs * 1_000_000;
        if (bound < deadline) deadline = bound;
    }
    if (deadline < 0) deadline = 0;
    timespec ts;
    ts.tv_sec = cast(typeof(ts.tv_sec))(deadline / nsPerSecond);
    ts.tv_nsec = cast(typeof(ts.tv_nsec))(deadline % nsPerSecond);
    futexWaitUntil(&gWait[workerIndex].wakeWord, ARMED, &ts);
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}
