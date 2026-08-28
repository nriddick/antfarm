module threadpool.worker;

import core.atomic;
import core.thread;
import threadpool.hybrid;
import threadpool.topology;

version (Windows)
    version = ThreadpoolOs;
version (linux)
    version = ThreadpoolOs;

version (ThreadpoolOs)
{
    import threadpool.pin;
}

version (Windows)
{
    import threadpool.sys.win_wait : wakeAllForStop;
}
else version (linux)
{
    import threadpool.sys.linux_wait : wakeAllForStop;
}

shared int gRunFlag;
shared int gWorkersGo;
shared int gPinOk;
shared int gPinDone;

struct SpawnSpec
{
    WorkerSelf self;
    uint       cpuSetId;
    ushort     group;
    ushort     lpIndex;
    bool       ecoQos;
    bool       applyQos;
}

void resetWorkerHandshake() @nogc nothrow
{
    atomicStore(gRunFlag, 1);
    atomicStore(gWorkersGo, 0);
    atomicStore(gPinOk, 0);
    atomicStore(gPinDone, 0);
}

void releaseWorkers() @nogc nothrow
{
    atomicStore!(MemoryOrder.rel)(gWorkersGo, 1);
}

void requestStop() @nogc nothrow
{
    version (ThreadpoolOs)
    {
        atomicStore!(MemoryOrder.rel)(gRunFlag, 0);
        wakeAllForStop();
        atomicStore!(MemoryOrder.rel)(gWorkersGo, 1);
    }
}

Thread spawnWorker(SpawnSpec spec)
{
    auto t = new Thread({
        workerMain(spec);
    });
    t.isDaemon = false;
    t.start();
    return t;
}

private void workerMain(SpawnSpec spec)
{
    version (ThreadpoolOs)
    {
        PinTarget pin;
        pin.cpuSetId = spec.cpuSetId;
        pin.group = spec.group;
        pin.lpIndex = spec.lpIndex;
        bool pinned = pinToLogicalProcessor(pin);
        if (pinned && spec.applyQos)
            applyPowerThrottling(spec.ecoQos);
        if (pinned)
            atomicFetchAdd(gPinOk, 1);
        atomicFetchAdd(gPinDone, 1);
        if (!pinned)
            return;
        while (!atomicLoad!(MemoryOrder.acq)(gWorkersGo)
            && atomicLoad!(MemoryOrder.acq)(gRunFlag))
        {
            Thread.yield();
        }
        if (!atomicLoad!(MemoryOrder.acq)(gRunFlag))
            return;
        setCurrentWorker(&spec.self);
        scope (exit) setCurrentWorker(null);
        if (gManagedWorkerHooks.pump !is null)
            managedWorkerLoop(spec.self);
        else if (spec.self.isP)
            pCoreWorkerLoop(spec.self, gRunFlag);
        else
            eCoreWorkerLoop(spec.self, gRunFlag);
    }
    else
    {
        throw new Error("threadpool: workers are not implemented in v1 on this OS");
    }
}

private void managedWorkerLoop(ref WorkerSelf self)
{
    auto hooks = gManagedWorkerHooks;
    bool started;
    Throwable failure;
    try
    {
        if (hooks.start !is null)
            hooks.start(&self);
        started = true;
        while (atomicLoad!(MemoryOrder.acq)(gRunFlag))
        {
            auto result = hooks.pump(&self);
            if (result.retry)
                continue;
            applyIdlePolicy(self, gRunFlag, result.hasDeadline,
                            result.deadlineTicks);
        }
    }
    catch (Throwable t)
    {
        failure = t;
        requestStop();
    }
    if (started && hooks.stop !is null)
    {
        try
            hooks.stop(&self);
        catch (Throwable t)
        {
            if (failure is null)
                failure = t;
            requestStop();
        }
    }
    self.context = null;
    if (failure !is null && self.workerIndex < gWorkers.length)
        gWorkers[self.workerIndex].failure = failure;
}
