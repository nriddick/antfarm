module threadpool.sys.linux_wait;

version (linux):

import core.atomic;
import core.sys.posix.time : timespec;
import threadpool.sys.linux_bindings : futexWaitTimeout, futexWake;

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
    if (gWait.length == 0 || workerIndex >= gWait.length) return;
    // Even ACTIVE -> ACTIVE publishes into the worker's next arming RMW.
    if (atomicExchange!(MemoryOrder.acq_rel)(
            &gWait[workerIndex].wakeWord, ACTIVE) != ARMED) return;
    futexWake(&gWait[workerIndex].wakeWord, 1);
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
