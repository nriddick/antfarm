module ant_fibers.semaphore_tests;

import antfarm;
import ant_fibers.scheduler;
import core.thread : Thread;
import core.time : MonoTime, msecs;

void semaphoreCancellationSmoke()
{
    // Control case, then acquired-before-cancel with the other waiter already
    // re-parked: the refund must restore the count and deliver a new wake.
    foreach (schedule; 0 .. 2)
    {
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto domain = new FiberDomain(farm);
        auto token = farm.registerProducer(Tier.small);
        scope (exit) farm.unregisterProducer(token);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        scope (exit) semaphoreAfterRegisterHook = null;

        // Flush just one FIFO activation, then consume until it has run. No
        // worker threads can resume a waiter while the driver applies cancel.
        void runOne(FiberTask expected)
        {
            immutable before = expected.resumeCount;
            immutable deadline = MonoTime.currTime + msecs(2_000);
            size_t flushed;
            while (flushed == 0 && MonoTime.currTime < deadline)
                flushed = domain.flush(token, 1);
            assert(flushed == 1, "semaphore test could not publish activation");
            while (expected.resumeCount == before
                && MonoTime.currTime < deadline)
            {
                view.consumeNext();
                Thread.yield();
            }
            assert(expected.resumeCount == before + 1,
                "semaphore test did not dispatch the expected FIFO waiter");
        }

        auto sem = new FiberSemaphore(domain);
        bool survivorReturned, cancelledReturned;
        auto survivor = domain.spawn({
            sem.wait();
            survivorReturned = true;
        });
        runOne(survivor);
        assert(domain.waiting == 1 && sem.available == 0);

        bool postedInRegistration;
        if (schedule != 0)
            semaphoreAfterRegisterHook = (FiberSemaphore registered) {
                assert(registered is sem && domain.waiting == 2);
                semaphoreAfterRegisterHook = null;
                // Force the real post between registration and the second
                // tryAcquire. WAKE reaches this running waiter before PARK.
                sem.post();
                postedInRegistration = true;
            };
        auto cancelled = domain.spawn({
            sem.wait();
            cancelledReturned = true;
        });
        runOne(cancelled);

        if (schedule == 0)
        {
            assert(domain.waiting == 2 && !postedInRegistration);
            sem.post();
            // An ordinary wake grants no permit: cancellation precedes retry.
            assert(sem.available == 1);
        }
        else
        {
            assert(postedInRegistration && sem.available == 0);
            assert(domain.waiting == 0 && domain.ready == 2);
            // The already-parked survivor is published by post; the acquiring
            // waiter is published only after yielding and completing PARK.
            runOne(survivor);
            assert(survivor.resumeCount == 2 && !survivorReturned);
            assert(domain.waiting == 1 && domain.ready == 1);
        }
        assert(!cancelledReturned && !survivorReturned);
        auto cancellation = domain.directorCancelDetailed(cancelled.handle);
        assert(cancellation == CancellationResult.requestedReady);
        assert(cancelled.resumeCount == 1);

        immutable deadline = MonoTime.currTime + msecs(2_000);
        while (!domain.drained && MonoTime.currTime < deadline)
        {
            domain.flush(token, 1);
            view.consumeNext();
            Thread.yield();
        }
        assert(survivorReturned,
            "cancelled semaphore wait lost its permit; second waiter stranded");
        assert(domain.drained && domain.waiting == 0 && domain.ready == 0);
        assert(!cancelledReturned && cancelled.outcome == FiberOutcome.cancelled);
        assert(cancelled.cancellationDisposition
            == CancellationDisposition.acknowledged);
        assert(survivor.outcome == FiberOutcome.completed && sem.available == 0);
        auto completed = domain.takeCompletions();
        assert(completed.length == 2);
        domain.releaseAll(completed);
    }
}
