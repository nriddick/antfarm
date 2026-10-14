module threadpool.sys.win_wait;

version (Windows):

import core.atomic;
import std.exception : enforce;
import threadpool.sys.win_bindings;

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
    resolveOptionalApis();
    enforce(pWaitOnAddress !is null,
        "threadpool: WaitOnAddress unavailable (need Windows 10 1803+)");
    destroyWaitSlots();
    gWait = new WaitSlot[](n);
}

void destroyWaitSlots()
{
    gWait = null;
}

void wakeWorker(uint workerIndex) @nogc nothrow
{
    // One load: length and pointer stay paired, and the local keeps the
    // slot array alive if destroyWaitSlots nulls gWait during this call.
    auto slots = gWait;
    if (workerIndex >= slots.length) return;
    auto slot = &slots[workerIndex];
    // Even ACTIVE -> ACTIVE publishes into the worker's next arming RMW.
    if (atomicExchange!(MemoryOrder.acq_rel)(&slot.wakeWord, ACTIVE) != ARMED) return;
    pWakeByAddressSingle(cast(void*)&slot.wakeWord);
}

void wakeAllForStop() @nogc nothrow
{
    foreach (ref slot; gWait)
    {
        if (atomicExchange!(MemoryOrder.acq_rel)(&slot.wakeWord, ACTIVE) != ARMED)
            continue;
        pWakeByAddressAll(cast(void*)&slot.wakeWord);
    }
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
