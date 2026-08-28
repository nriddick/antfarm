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
    atomicFetchAdd(gWait[workerIndex].wakeWord, 1);
    futexWake(&gWait[workerIndex].wakeWord, 1);
}

void wakeAllForStop() @nogc nothrow
{
    foreach (ref slot; gWait)
    {
        atomicFetchAdd(slot.wakeWord, 1);
        futexWake(&slot.wakeWord, int.max);
    }
}

/// `timeoutMs == uint.max` waits forever. Returns false if the pool is stopping.
bool parkWorker(uint workerIndex, ref shared(int) runFlag, uint timeoutMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    auto observed = atomicLoad!(MemoryOrder.acq)(gWait[workerIndex].wakeWord);
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (timeoutMs == uint.max)
        futexWaitTimeout(&gWait[workerIndex].wakeWord, observed, null);
    else
    {
        timespec ts;
        ts.tv_sec = timeoutMs / 1000;
        ts.tv_nsec = (timeoutMs % 1000) * 1_000_000L;
        futexWaitTimeout(&gWait[workerIndex].wakeWord, observed, &ts);
    }
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}
