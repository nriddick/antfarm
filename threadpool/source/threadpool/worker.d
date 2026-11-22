module threadpool.worker;

import core.atomic;
import core.thread;
import threadpool.hybrid;
import threadpool.pin : PinOutcome, WorkerPlacement, WorkerQos;
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
    import threadpool.sys.win_wait : wakeAllForStop, prepareWorkerWait, cancelWorkerWait;
}
else version (linux)
{
    import threadpool.sys.linux_wait : wakeAllForStop, prepareWorkerWait, cancelWorkerWait;
}

shared int gRunFlag;
shared int gWorkersGo;
shared int gPinOk;
shared int gPinDone;
/// One slot per worker, written before that worker's `gPinDone` increment.
__gshared PinOutcome[] gPinOutcome;

struct SpawnSpec
{
    WorkerSelf self;
    /// `free`, `group` or `pin`.
    WorkerPlacement placement;
    /// The worker's set: what a `group` worker is confined to.
    const(ProcessorId)[] processors;
    /// Every processor of the frozen OS scope: what a `free` worker may use.
    const(ProcessorId)[] scopeProcessors;
    /// Under `pin`: the one processor, in the published topology snapshot,
    /// which lives for the process. Null otherwise.
    const(LogicalProcessor)* processor;
    WorkerQos  qos;
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
        auto outcome = placeCallingWorker(spec);
        bool pinned = outcome == PinOutcome.pinned;
        if (pinned && spec.qos != WorkerQos.unchanged)
            applyPowerThrottling(spec.qos == WorkerQos.eco);
        gPinOutcome[spec.self.workerIndex] = outcome;
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
        else
            workerLoop(spec.self, gRunFlag);
    }
    else
    {
        throw new Error("threadpool: workers are not implemented in v1 on this OS");
    }
}

/// Apply the worker's placement to the calling thread.
///
/// `free` leaves a Windows thread alone: it starts with the process's
/// affinity whatever its creator's is. A Linux thread inherits its creator's
/// mask, and a host confines or pins its caller before it starts the pool,
/// so there a `free` worker takes the whole frozen OS scope explicitly.
private PinOutcome placeCallingWorker(ref SpawnSpec spec)
{
    version (ThreadpoolOs)
    {
        final switch (spec.placement)
        {
            case WorkerPlacement.automatic:
            case WorkerPlacement.free:
                version (linux)
                    return confineStartingWorker(spec.scopeProcessors);
                else
                    return PinOutcome.pinned;
            case WorkerPlacement.group:
                return confineStartingWorker(spec.processors);
            case WorkerPlacement.pin:
                return spec.processor is null ? PinOutcome.unknownProcessor
                    : pinProcessor(*spec.processor);
        }
    }
    else
        return PinOutcome.osRejected;
}

private void managedWorkerLoop(ref WorkerSelf self)
{
    auto hooks = gManagedWorkerHooks;
    bool started;
    try
    {
        if (hooks.start !is null)
            hooks.start(&self);
        started = true;
        bool worked, watch;
        while (atomicLoad!(MemoryOrder.acq)(gRunFlag))
        {
            auto result = hooks.pump(&self);
            if (result.retry)
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
            version (ThreadpoolOs)
            {
                if (!prepareWorkerWait(self.workerIndex)) break;
                armWorkerBit(self.workerIndex);
                scope (exit) { disarmWorkerBit(self.workerIndex); cancelWorkerWait(self.workerIndex); }
                if (!atomicLoad!(MemoryOrder.acq)(gRunFlag)) break;
                result = hooks.pump(&self);
                if (result.retry)
                {
                    worked = true;
                    continue;
                }
                applyIdlePolicy(self, gRunFlag, result.hasDeadline,
                                result.deadlineTicks);
            }
            else
                applyIdlePolicy(self, gRunFlag, result.hasDeadline,
                                result.deadlineTicks);
        }
    }
    catch (Throwable t)
    {
        gWorkers[self.workerIndex].failure.record(t);
        requestStop();
    }
    if (started && hooks.stop !is null)
    {
        try
            hooks.stop(&self);
        catch (Throwable t)
        {
            gWorkers[self.workerIndex].failure.record(t);
            requestStop();
        }
    }
    self.context = null;
}
