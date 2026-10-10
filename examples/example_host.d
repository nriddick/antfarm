/++
 + Example scaffolding for the programs in this directory. Not a scheduler.
 +
 + It spins or yields, uses no topology pinning, and puts a deadline on
 + progress. It does not infer completion from `consumeNext() == false`, and
 + it never calls `pump`: a zero pump count is not idle. Callers drain on
 + their own condition (`live`, retirement, node availability, wave
 + `finished`) and use `progress` only to run or yield one step.
 +
 + Startup may use the GC. Each worker owns its `ConsumerView`. The caller
 + owns the producer token and must keep flushing or pumping until the drain
 + condition holds, then `stopConsumers` before destroying the Farm.
 +/
module example_host;

import antfarm;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad, atomicStore;
import core.thread : Thread;
import core.time : MonoTime, seconds;

final class ExampleHost
{
    AntFarm* farm;
    private Thread[] workers;
    private ConsumerView local;
    private shared uint stopping;
    private shared uint subscribed;
    private MonoTime deadline;

    /// `workerCount == 0` consumes on the calling thread. Otherwise that many
    /// druntime threads each own one view. Construction waits until they have
    /// subscribed, under the same deadline as later drains.
    this(uint workerCount)
    {
        deadline = MonoTime.currTime + 30.seconds;
        immutable consumers = workerCount == 0 ? 1 : workerCount;
        farm = AntFarm.create(ringMiB: 2, expectedConsumers: consumers,
            maxBulk: 0, maxSmall: 1, quotaSmall: 4096);
        if (workerCount == 0)
            subscribe(local);
        foreach (_; 0 .. workerCount)
        {
            auto worker = new Thread(&runWorker);
            workers ~= worker;
            worker.start();
        }
        while (atomicLoad!(MemoryOrder.acq)(subscribed) < workerCount)
            progress();
    }

    /// Start a fresh progress deadline for another application request.
    void resetDeadline()
    {
        deadline = MonoTime.currTime + 30.seconds;
    }

    /// One consume or yield. Returning is not idle, not `live == 0`, and not
    /// wave completion. A stalled caller hits the deadline instead of hanging.
    void progress()
    {
        if (MonoTime.currTime > deadline)
            fatal("example host timed out; a drain made no progress");
        if (workers.length == 0)
            local.consumeNext();
        else
            Thread.yield();
    }

    /// Join workers after they unsubscribe, or unsubscribe the calling thread.
    /// Call this only once the caller's actor drain no longer needs consumers.
    void stopConsumers()
    {
        atomicStore!(MemoryOrder.rel)(stopping, 1u);
        foreach (worker; workers)
            worker.join();
        if (workers.length == 0)
            local.unsubscribe();
    }

    private void subscribe(ref ConsumerView view) nothrow @nogc @system
    {
        long result;
        do
            result = view.subscribe(farm);
        while (result == SUBSCRIBE_RETRY);
        if (result < 0)
            fatal("example host consumer subscription failed");
    }

    private void runWorker()
    {
        ConsumerView view;
        subscribe(view);
        atomicFetchAdd!(MemoryOrder.rel)(subscribed, 1u);
        while (atomicLoad!(MemoryOrder.acq)(stopping) == 0)
            if (!view.consumeNext())
                Thread.yield();
        view.unsubscribe();
    }
}
