module antfarm_fibers.event_tests;

import antfarm;
import antfarm_fibers.scheduler;
import core.thread : Thread;
import core.time : MonoTime, msecs;

void eventCancellationSmoke()
{
    enum Schedule { ordinaryWake, registrationConsume, retainedRefund,
                    manualPosted, manualReset }
    foreach (schedule; [Schedule.ordinaryWake, Schedule.registrationConsume,
                        Schedule.retainedRefund, Schedule.manualPosted,
                        Schedule.manualReset])
    {
        immutable twoWaiters = schedule == Schedule.ordinaryWake
            || schedule == Schedule.registrationConsume;
        immutable manual = schedule == Schedule.manualPosted
            || schedule == Schedule.manualReset;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto domain = new FiberDomain(farm);
        auto token = farm.registerProducer(Tier.small);
        scope (exit) farm.unregisterProducer(token);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        scope (exit) eventAfterRegisterHook = null;

        // One driver controls FIFO activation and cancellation; no worker can
        // resume a waiter while the driver checks the forced interleaving.
        void runOne(FiberTask expected)
        {
            immutable before = expected.resumeCount;
            immutable deadline = MonoTime.currTime + msecs(2_000);
            size_t flushed;
            while (flushed == 0 && MonoTime.currTime < deadline)
                flushed = domain.flush(token, 1);
            assert(flushed == 1, "event test could not publish activation");
            while (expected.resumeCount == before
                && MonoTime.currTime < deadline)
            {
                view.consumeNext();
                Thread.yield();
            }
            assert(expected.resumeCount == before + 1,
                "event test did not dispatch the expected FIFO waiter");
        }

        auto event = new FiberEvent(domain, manual
            ? FiberEventMode.manualReset : FiberEventMode.autoReset);
        bool survivorReturned, cancelledReturned;
        FiberTask survivor;
        if (twoWaiters)
        {
            survivor = domain.spawn({
                event.wait();
                survivorReturned = true;
            });
            runOne(survivor);
            assert(domain.waiting == 1 && !event.isSet);
        }

        bool setInRegistration;
        if (schedule != Schedule.ordinaryWake)
            eventAfterRegisterHook = (FiberEvent registered) {
                assert(registered is event);
                assert(domain.waiting == (twoWaiters ? 2 : 1));
                eventAfterRegisterHook = null;
                // Real set between registration and consumePosted: WAKE
                // reaches this running waiter before PARK.
                event.set();
                setInRegistration = true;
            };
        auto cancelled = domain.spawn({
            event.wait();
            cancelledReturned = true;
        });
        runOne(cancelled);

        if (schedule == Schedule.ordinaryWake)
        {
            assert(domain.waiting == 2 && !setInRegistration);
            event.set();
            // Ordinary wake grants no ownership; cancel precedes retry.
            assert(event.isSet);
        }
        else
        {
            assert(setInRegistration && event.isSet == manual);
            assert(domain.waiting == 0 && domain.ready == (twoWaiters ? 2 : 1));
            if (twoWaiters)
            {
                // B is published by set, A only after yielding. B must
                // re-park so a refund must wake it as well as restore posted.
                runOne(survivor);
                assert(survivor.resumeCount == 2 && !survivorReturned);
                assert(domain.waiting == 1 && domain.ready == 1 && !event.isSet);
            }
            if (schedule == Schedule.manualReset)
                // A already observed posted before yielding. Its cancellation
                // must not undo a later explicit reset of a manual event.
                event.reset();
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
        if (twoWaiters)
            assert(survivorReturned,
                "cancelled event wait lost its set; second waiter stranded");
        assert(domain.drained && domain.waiting == 0 && domain.ready == 0);
        assert(!cancelledReturned && cancelled.outcome == FiberOutcome.cancelled);
        assert(cancelled.cancellationDisposition
            == CancellationDisposition.acknowledged);
        if (twoWaiters)
            assert(survivor.outcome == FiberOutcome.completed);
        assert(event.isSet == (schedule == Schedule.retainedRefund
                              || schedule == Schedule.manualPosted));
        auto completed = domain.takeCompletions();
        assert(completed.length == (twoWaiters ? 2 : 1));
        domain.releaseAll(completed);
    }
}
