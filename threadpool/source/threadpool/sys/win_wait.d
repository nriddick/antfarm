module threadpool.sys.win_wait;

version (Windows):

import core.atomic;
import core.sys.windows.winbase : CloseHandle, CreateEventW, INFINITE, SetEvent, WaitForMultipleObjects;
import core.sys.windows.windef : DWORD, FALSE, HANDLE, TRUE;
import threadpool.sys.win_bindings;

struct WaitSlot
{
    shared uint wakeWord;
    HANDLE      workEvent;
}

__gshared WaitSlot[] gWait;
__gshared HANDLE     gStopEvent;

void createWaitSlots(uint n)
{
    resolveOptionalApis();
    destroyWaitSlots();
    gWait = new WaitSlot[](n);
    if (pWaitOnAddress is null)
    {
        foreach (ref slot; gWait)
        {
            slot.workEvent = CreateEventW(null, FALSE, FALSE, null);
            if (slot.workEvent is null)
                throw new Exception("threadpool: CreateEventW failed for wait slot");
        }
        gStopEvent = CreateEventW(null, TRUE, FALSE, null);
        if (gStopEvent is null)
            throw new Exception("threadpool: CreateEventW failed for stop event");
    }
}

void destroyWaitSlots()
{
    foreach (ref slot; gWait)
    {
        if (slot.workEvent !is null)
        {
            CloseHandle(slot.workEvent);
            slot.workEvent = null;
        }
    }
    if (gStopEvent !is null)
    {
        CloseHandle(gStopEvent);
        gStopEvent = null;
    }
    gWait = null;
}

void wakeWorker(uint workerIndex) @nogc nothrow
{
    if (gWait.length == 0 || workerIndex >= gWait.length) return;
    atomicFetchAdd(gWait[workerIndex].wakeWord, 1);
    if (pWakeByAddressSingle !is null)
        pWakeByAddressSingle(cast(void*)&gWait[workerIndex].wakeWord);
    else if (gWait[workerIndex].workEvent !is null)
        SetEvent(gWait[workerIndex].workEvent);
}

void wakeAllForStop() @nogc nothrow
{
    foreach (ref slot; gWait)
    {
        atomicFetchAdd(slot.wakeWord, 1);
        if (pWakeByAddressAll !is null)
            pWakeByAddressAll(cast(void*)&slot.wakeWord);
        else if (slot.workEvent !is null)
            SetEvent(slot.workEvent);
    }
    if (gStopEvent !is null)
        SetEvent(gStopEvent);
}

/// Park this worker until `signal`, stop, or `timeoutMs` (INFINITE = forever).
/// Returns false if the pool is stopping.
bool parkWorker(uint workerIndex, ref shared(int) runFlag, uint timeoutMs) @nogc nothrow
{
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (workerIndex >= gWait.length) return false;
    auto observed = atomicLoad!(MemoryOrder.acq)(gWait[workerIndex].wakeWord);
    if (!atomicLoad!(MemoryOrder.acq)(runFlag)) return false;
    if (pWaitOnAddress !is null)
    {
        uint cmp = observed;
        pWaitOnAddress(cast(void*)&gWait[workerIndex].wakeWord, &cmp, 4, timeoutMs);
    }
    else
    {
        HANDLE[2] h = [gWait[workerIndex].workEvent, gStopEvent];
        WaitForMultipleObjects(2, h.ptr, FALSE, timeoutMs);
    }
    return atomicLoad!(MemoryOrder.acq)(runFlag) != 0;
}
