module ant_fibers.semaphore_tests;

import antfarm;
import ant_fibers.scheduler;
import core.thread : Thread;
import core.time : MonoTime, hours, msecs;

/// FiberSemaphore wake ownership: one wake per permit, a cancelled woken
/// waiter passes its wake on, and a permit taken by a registration recheck
/// is refunded when cancellation follows. One driver thread controls FIFO
/// activation, so no worker can resume a waiter mid-schedule.
void semaphoreCancellationSmoke()
{
    oneWakePerPermit();
    wokenWaiterCancelledPassesOn();
    registrationConsumeRefund();
    recheckClaimedBySignal();
    refundWakesReparkedWaiter();
    timedSemaphoreWaits();
}

private struct Rig
{
    AntFarm* farm;
    FiberDomain domain;
    Token token;
    ConsumerView view;

    void open()
    {
        farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                              DEFAULT_SMALL_TABLE_THRESHOLD, false);
        domain = new FiberDomain(farm);
        token = farm.registerProducer(Tier.small);
        subscribeOrThrow(view, farm);
    }

    void close()
    {
        domain.retire();
        view.unsubscribe();
        farm.unregisterProducer(token);
        farm.destroy();
    }

    /// Flush the oldest activation and consume until `expected` has run.
    void runOne(FiberTask expected)
    {
        immutable before = expected.resumeCount;
        immutable deadline = MonoTime.currTime + msecs(2_000);
        size_t flushed;
        while (flushed == 0 && MonoTime.currTime < deadline)
            flushed = domain.flush(token, 1);
        assert(flushed == 1, "semaphore test could not publish activation");
        while (expected.resumeCount == before && MonoTime.currTime < deadline)
        {
            view.consumeNext();
            Thread.yield();
        }
        assert(expected.resumeCount == before + 1,
            "semaphore test did not dispatch the expected FIFO waiter");
    }

    void drain()
    {
        immutable deadline = MonoTime.currTime + msecs(2_000);
        while (!domain.drained && MonoTime.currTime < deadline)
        {
            domain.pollTimers();
            domain.flush(token, 1);
            view.consumeNext();
            Thread.yield();
        }
        assert(domain.drained, "semaphore test fibers did not finish");
        assert(domain.waiting == 0 && domain.ready == 0);
    }
}

private void oneWakePerPermit()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto sem = new FiberSemaphore(domain);
    size_t returned;
    FiberTask[3] waiters;
    foreach (ref w; waiters)
    {
        w = domain.spawn({ sem.wait(); ++returned; });
        rig.runOne(w);
    }
    assert(domain.waiting == 3);

    sem.post();
    assert(domain.waiting == 2 && domain.ready == 1 && sem.available == 1,
        "one permit wakes exactly one waiter");
    rig.runOne(waiters[0]);
    assert(returned == 1 && sem.available == 0);
    sem.post(2);
    assert(domain.waiting == 0 && domain.ready == 2);
    rig.drain();
    assert(returned == 3 && sem.available == 0);
}

private void wokenWaiterCancelledPassesOn()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto sem = new FiberSemaphore(domain);
    bool aReturned, bReturned;
    auto a = domain.spawn({ sem.wait(); aReturned = true; });
    rig.runOne(a);
    auto b = domain.spawn({ sem.wait(); bReturned = true; });
    rig.runOne(b);

    sem.post();
    assert(domain.directorCancelDetailed(a.handle)
        == CancellationResult.requestedReady);
    assert(domain.waiting == 1 && domain.ready == 1);
    rig.drain();
    assert(!aReturned && a.outcome == FiberOutcome.cancelled);
    assert(a.cancellationDisposition == CancellationDisposition.acknowledged);
    assert(bReturned, "a cancelled waiter swallowed the permit's wake; B stranded");
    assert(sem.available == 0);
}

/// A post between registration and the recheck claims the registering
/// waiter, and the recheck takes the permit, so the waiter suspends once for
/// the post's WAKE. Cancelled while ready, it must return the permit.
private void registrationConsumeRefund()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) semaphoreAfterRegisterHook = null;
    auto domain = rig.domain;
    auto sem = new FiberSemaphore(domain);
    bool postedInRegistration, returned;
    semaphoreAfterRegisterHook = (FiberSemaphore registered) {
        assert(registered is sem && domain.waiting == 1);
        semaphoreAfterRegisterHook = null;
        sem.post();
        postedInRegistration = true;
    };
    auto waiter = domain.spawn({ sem.wait(); returned = true; });
    rig.runOne(waiter);
    assert(postedInRegistration && sem.available == 0);
    assert(domain.waiting == 0 && domain.ready == 1);
    assert(domain.directorCancelDetailed(waiter.handle)
        == CancellationResult.requestedReady);
    rig.drain();
    assert(!returned && waiter.outcome == FiberOutcome.cancelled);
    assert(waiter.cancellationDisposition == CancellationDisposition.acknowledged);
    assert(sem.available == 1, "cancelled semaphore wait kept its permit");
}

/// A waiter whose registration recheck takes a permit after a post already
/// claimed it consumes that post's wake by suspending once and returns with
/// the permit. It owes no pass-on: with one waiter nothing is left over, and
/// with A parked first, two posts claim A then W and each takes one permit.
/// A post that claims only A leaves W's recheck its permit; A re-parks and
/// the next post reaches it.
private void recheckClaimedBySignal()
{
    foreach (schedule; 0 .. 3)
    {
        Rig rig;
        rig.open();
        scope (exit) rig.close();
        scope (exit) semaphoreAfterRegisterHook = null;
        auto domain = rig.domain;
        auto sem = new FiberSemaphore(domain);
        bool aReturned, wReturned;
        FiberTask a;
        if (schedule != 0)
        {
            a = domain.spawn({ sem.wait(); aReturned = true; });
            rig.runOne(a);
        }
        semaphoreAfterRegisterHook = (FiberSemaphore registered) {
            semaphoreAfterRegisterHook = null;
            sem.post();
            if (schedule == 1) sem.post();
        };
        auto w = domain.spawn({ sem.wait(); wReturned = true; });
        rig.runOne(w);
        final switch (schedule)
        {
        case 0:
            // Claimed by the post, so the recheck's success still suspends.
            assert(!wReturned && domain.waiting == 0 && domain.ready == 1);
            rig.drain();
            assert(wReturned && w.resumeCount == 2);
            break;
        case 1:
            assert(!wReturned && domain.waiting == 0 && domain.ready == 2);
            assert(sem.available == 1);
            rig.drain();
            assert(aReturned && wReturned && w.resumeCount == 2);
            break;
        case 2:
            // The post claimed A; W withdrew its own unclaimed wait.
            assert(wReturned && w.resumeCount == 1 && domain.ready == 1);
            rig.runOne(a);
            assert(!aReturned && a.resumeCount == 2 && domain.waiting == 1);
            sem.post();
            rig.drain();
            assert(aReturned);
            break;
        }
        assert(sem.available == 0 && domain.waiting == 0 && domain.ready == 0);
    }
}

/// A is parked. In C's registration window a post claims A, and the Director
/// cancels C, claiming C's wait too. C's recheck takes the permit, so A wakes
/// to nothing and re-parks; C's cancellation refund must wake A.
private void refundWakesReparkedWaiter()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) semaphoreAfterRegisterHook = null;
    auto domain = rig.domain;
    auto sem = new FiberSemaphore(domain);
    bool aReturned, cReturned;
    auto a = domain.spawn({ sem.wait(); aReturned = true; });
    rig.runOne(a);
    FiberTask c;
    semaphoreAfterRegisterHook = (FiberSemaphore registered) {
        assert(domain.waiting == 2);
        semaphoreAfterRegisterHook = null;
        sem.post();
        assert(domain.directorCancelDetailed(c.handle)
            == CancellationResult.requestedWaiting);
        assert(domain.waiting == 0);
    };
    c = domain.spawn({ sem.wait(); cReturned = true; });
    rig.runOne(c);
    assert(sem.available == 0 && domain.ready == 2);
    rig.runOne(a);
    assert(a.resumeCount == 2 && !aReturned && domain.waiting == 1);
    rig.drain();
    assert(aReturned, "cancelled semaphore wait lost its permit; A stranded");
    assert(!cReturned && c.outcome == FiberOutcome.cancelled);
    assert(sem.available == 0);
}

private void timedSemaphoreWaits()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto sem = new FiberSemaphore(domain);

    bool expired = true;
    domain.spawn({ expired = !sem.wait(msecs(1)); });
    rig.drain();
    assert(expired && sem.available == 0);

    bool woken;
    auto parked = domain.spawn({ woken = sem.wait(hours(1)); });
    rig.runOne(parked);
    long deadline;
    assert(domain.nextTimerDeadline(deadline));
    sem.post();
    rig.drain();
    assert(woken && sem.available == 0 && !domain.nextTimerDeadline(deadline));
}
