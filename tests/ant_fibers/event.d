module ant_fibers.event_tests;

import antfarm;
import ant_fibers.scheduler;
import core.thread : Thread;
import core.time : MonoTime, hours, msecs;

/// FiberEvent wake ownership. An auto-reset `set` wakes one waiter; a woken
/// waiter that is cancelled passes the wake on; a registration recheck that
/// consumed the posted state refunds it when cancellation follows, waking a
/// waiter that re-parked meanwhile. One driver thread controls FIFO
/// activation, so no worker can resume a waiter mid-schedule.
void eventCancellationSmoke()
{
    oneWakePerSet();
    wokenWaiterCancelledPassesOn();
    registrationConsumeRefund(false, false);
    registrationConsumeRefund(true, false);
    registrationConsumeRefund(true, true);
    recheckClaimedBySignal();
    refundWakesReparkedWaiter();
    timedEventWaits();
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
        domain.releaseAll(domain.takeCompletions());
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
        assert(flushed == 1, "event test could not publish activation");
        while (expected.resumeCount == before && MonoTime.currTime < deadline)
        {
            view.consumeNext();
            Thread.yield();
        }
        assert(expected.resumeCount == before + 1,
            "event test did not dispatch the expected FIFO waiter");
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
        assert(domain.drained, "event test fibers did not finish");
        assert(domain.waiting == 0 && domain.ready == 0);
    }
}

private void oneWakePerSet()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto event = new FiberEvent(domain);
    bool aReturned, bReturned;
    auto a = domain.spawn({ event.wait(); aReturned = true; });
    rig.runOne(a);
    auto b = domain.spawn({ event.wait(); bReturned = true; });
    rig.runOne(b);
    assert(domain.waiting == 2);

    event.set();
    assert(domain.waiting == 1 && domain.ready == 1 && event.isSet,
        "an auto-reset set wakes exactly one waiter");
    rig.runOne(a);
    assert(aReturned && !bReturned && !event.isSet && domain.waiting == 1);
    event.set();
    rig.drain();
    assert(bReturned && !event.isSet);
}

private void wokenWaiterCancelledPassesOn()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto event = new FiberEvent(domain);
    bool aReturned, bReturned;
    auto a = domain.spawn({ event.wait(); aReturned = true; });
    rig.runOne(a);
    auto b = domain.spawn({ event.wait(); bReturned = true; });
    rig.runOne(b);

    event.set();
    // The set's wake belongs to A; cancellation loses the claim but A is
    // already ready, so it unwinds instead of consuming the event.
    assert(domain.directorCancelDetailed(a.handle)
        == CancellationResult.requestedReady);
    assert(domain.waiting == 1 && domain.ready == 1);
    rig.drain();
    assert(!aReturned && a.outcome == FiberOutcome.cancelled);
    assert(a.cancellationDisposition == CancellationDisposition.acknowledged);
    assert(bReturned, "a cancelled waiter swallowed the set; B stranded");
    assert(!event.isSet);
}

/// The registering waiter is claimed by a set that runs between its
/// registration and its recheck. The recheck consumes the event, so the
/// waiter must suspend once for the set's WAKE; cancelled while ready, it
/// returns an auto-reset unit. A manual event is untouched by the wait, and a
/// later explicit reset must stay in effect.
private void registrationConsumeRefund(bool manual, bool resetAfter)
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) eventAfterRegisterHook = null;
    auto domain = rig.domain;
    auto event = new FiberEvent(domain, manual
        ? FiberEventMode.manualReset : FiberEventMode.autoReset);
    bool setInRegistration, returned;
    eventAfterRegisterHook = (FiberEvent registered) {
        assert(registered is event && domain.waiting == 1);
        eventAfterRegisterHook = null;
        event.set();
        setInRegistration = true;
    };
    auto waiter = domain.spawn({ event.wait(); returned = true; });
    rig.runOne(waiter);
    assert(setInRegistration && domain.waiting == 0 && domain.ready == 1);
    assert(event.isSet == manual);
    if (resetAfter) event.reset();
    assert(domain.directorCancelDetailed(waiter.handle)
        == CancellationResult.requestedReady);
    assert(waiter.resumeCount == 1);
    rig.drain();
    assert(!returned && waiter.outcome == FiberOutcome.cancelled);
    assert(waiter.cancellationDisposition == CancellationDisposition.acknowledged);
    assert(event.isSet == !resetAfter);
}

/// A set between registration and recheck claims the only waiter, whose
/// recheck then consumes the event: it suspends once for the set's WAKE and
/// returns, leaving the event clear and nothing else woken or owed.
private void recheckClaimedBySignal()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) eventAfterRegisterHook = null;
    auto domain = rig.domain;
    auto event = new FiberEvent(domain);
    bool returned;
    eventAfterRegisterHook = (FiberEvent registered) {
        eventAfterRegisterHook = null;
        event.set();
    };
    auto waiter = domain.spawn({ event.wait(); returned = true; });
    rig.runOne(waiter);
    assert(!returned && !event.isSet && domain.waiting == 0 && domain.ready == 1);
    rig.drain();
    assert(returned && waiter.resumeCount == 2 && !event.isSet);
}

/// A is parked. Two sets in C's registration window claim A and then C, and
/// coalesce into one posted unit, which C's recheck consumes. A wakes to
/// nothing and re-parks. C's cancellation refund must wake A.
private void refundWakesReparkedWaiter()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) eventAfterRegisterHook = null;
    auto domain = rig.domain;
    auto event = new FiberEvent(domain);
    bool aReturned, cReturned;
    auto a = domain.spawn({ event.wait(); aReturned = true; });
    rig.runOne(a);
    eventAfterRegisterHook = (FiberEvent registered) {
        assert(domain.waiting == 2);
        eventAfterRegisterHook = null;
        event.set();
        event.set();
    };
    auto c = domain.spawn({ event.wait(); cReturned = true; });
    rig.runOne(c);
    assert(domain.waiting == 0 && domain.ready == 2 && !event.isSet);
    rig.runOne(a);
    assert(a.resumeCount == 2 && !aReturned);
    assert(domain.waiting == 1 && domain.ready == 1);
    assert(domain.directorCancelDetailed(c.handle)
        == CancellationResult.requestedReady);
    rig.drain();
    assert(aReturned, "cancelled event wait lost its set; A stranded");
    assert(!cReturned && c.outcome == FiberOutcome.cancelled);
    assert(!event.isSet);
}

private void timedEventWaits()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto event = new FiberEvent(domain);

    bool expired = true;
    domain.spawn({ expired = !event.wait(msecs(1)); });
    rig.drain();
    assert(expired, "an unposted timed wait must time out");

    bool posted;
    event.set();
    domain.spawn({ posted = event.wait(hours(1)); });
    rig.drain();
    assert(posted && !event.isSet);

    bool woken;
    auto parked = domain.spawn({ woken = event.wait(hours(1)); });
    rig.runOne(parked);
    long deadline;
    assert(domain.waiting == 1 && domain.nextTimerDeadline(deadline));
    event.set();
    rig.drain();
    assert(woken && !domain.nextTimerDeadline(deadline),
        "a set before the deadline must remove the wait's timer");
}
