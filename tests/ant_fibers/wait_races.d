module ant_fibers.wait_race_tests;

import antfarm;
import ant_fibers.scheduler;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime, hours, msecs, seconds;

/// Deterministic schedules for the two arbitration windows a wake leaves
/// open after it has won: a signal that detached its waiters but has not
/// woken them yet, and a timer expiry that claimed a wait but has not
/// published the waiter yet. Each pauses there through a unittest seam and
/// runs the competing operation from the same thread.
void waitRaceSmoke()
{
    detachedWaiterCancellation();
    timerClaimBeforeJoinCompletion();
    timerClaimBeforeCancellation();
    signalAfterWaitingCount();
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

    /// Publishes one activation and consumes until `expected` has run.
    void runOne(FiberTask expected)
    {
        immutable before = expected.resumeCount;
        immutable deadline = MonoTime.currTime + msecs(2_000);
        size_t flushed;
        while (flushed == 0 && MonoTime.currTime < deadline)
            flushed = domain.flush(token, 1);
        assert(flushed == 1, "wait race test could not publish activation");
        while (expected.resumeCount == before && MonoTime.currTime < deadline)
            view.consumeNext();
        assert(expected.resumeCount == before + 1,
            "wait race test did not dispatch the expected fiber");
    }

    /// Publishes and consumes until every task in `tasks` has terminated.
    void runToEnd(FiberTask[] tasks...)
    {
        immutable deadline = MonoTime.currTime + msecs(2_000);
        bool done;
        while (!done && MonoTime.currTime < deadline)
        {
            domain.flush(token);
            view.consumeNext();
            done = true;
            foreach (task; tasks)
                done = done && task.terminated;
        }
        assert(done, "wait race test fibers did not terminate");
    }
}

/// A and B wait on S; a signal detaches both. Before it wakes them, C parks
/// on S (a new bucket) and A is cancelled. The cancellation must see A as
/// detached: it may neither unlink A from the chain being woken nor take
/// C's count. The original signal wakes A and B, the next one wakes C.
private void detachedWaiterCancellation()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) wakeAfterTakeHook = null;
    auto domain = rig.domain;
    enum Signal key = 9;

    bool aReturned, bReturned, cReturned;
    auto a = domain.spawn({ FiberDomain.await(key); aReturned = true; });
    rig.runOne(a);
    auto b = domain.spawn({ FiberDomain.await(key); bReturned = true; });
    rig.runOne(b);
    assert(domain.waiting == 2);

    FiberTask c;
    CancellationResult cancelA;
    wakeAfterTakeHook = (Signal signal) {
        assert(signal == key);
        wakeAfterTakeHook = null;
        c = domain.spawn({ FiberDomain.await(key); cReturned = true; });
        rig.runOne(c);
        assert(domain.waiting == 1, "C is the only linked waiter");
        cancelA = domain.directorCancelDetailed(a.handle);
        assert(domain.waiting == 1,
            "cancelling a detached waiter must not remove another bucket's waiter");
        assert(domain.ready == 0,
            "the signal, not the cancellation, owns the detached waiter's wake");
    };
    assert(domain.signal(key) == 2);
    assert(cancelA == CancellationResult.requestedWaiting);
    assert(domain.ready == 2);
    assert(domain.signal(key) == 1, "C is still linked under its own bucket");
    rig.runToEnd(a, b, c);
    assert(a.terminated && a.outcome == FiberOutcome.cancelled && !aReturned);
    assert(b.terminated && bReturned);
    assert(c.terminated && cReturned);
    assert(domain.waiting == 0 && domain.ready == 0);
}

/// W joins T with a timeout. Expiry claims W's join membership; before it
/// publishes W, T completes. Completion must find W already claimed and not
/// wake it, so W cannot resume and register its next wait (an hour-long
/// sleep) before the expiry publishes. The sleep must then actually park.
private void timerClaimBeforeJoinCompletion()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) timerAfterClaimHook = null;
    auto domain = rig.domain;
    enum Signal targetKey = 10;

    auto target = domain.spawn({ FiberDomain.await(targetKey); });
    rig.runOne(target);
    bool joined, slept;
    auto waiter = domain.spawn({
        target.handle.joinFiber(msecs(1));
        joined = true;
        FiberDomain.sleepFor(hours(1));
        slept = true;
    });
    rig.runOne(waiter);
    assert(domain.waiting == 2);

    immutable now = MonoTime.currTime;
    timerAfterClaimHook = (FiberTask claimed) {
        assert(claimed is waiter);
        timerAfterClaimHook = null;
        assert(domain.signal(targetKey) == 1);
        rig.runOne(target);
        assert(target.terminated);
        assert(domain.ready == 0,
            "target completion must not wake a joiner the expiry claimed");
    };
    assert(domain.pollTimers(now + msecs(2)) == 1);
    assert(domain.ready == 1);
    rig.runOne(waiter);
    assert(joined && !slept, "the hour-long sleep returned at once");
    assert(domain.waiting == 1);
    long deadline;
    assert(domain.nextTimerDeadline(deadline));
    assert(domain.pollTimers(now + msecs(4)) == 0,
        "no stale expiry may wake the later sleep");

    assert(domain.directorCancel(waiter));
    rig.runOne(waiter);
    assert(waiter.terminated && waiter.outcome == FiberOutcome.cancelled
        && !slept);
    assert(!domain.nextTimerDeadline(deadline));
}

/// Expiry claims a sleeper; before it publishes, the Director cancels it.
/// The cancellation must lose (no second publication), so the slot cannot
/// be consumed, released and respawned while the expiry still holds it.
private void timerClaimBeforeCancellation()
{
    Rig rig;
    rig.open();
    scope (exit) rig.close();
    scope (exit) timerAfterClaimHook = null;
    auto domain = rig.domain;

    bool slept;
    auto sleeper = domain.spawn({
        FiberDomain.sleepFor(hours(1));
        slept = true;
    });
    rig.runOne(sleeper);
    immutable generation = sleeper.generation;

    CancellationResult cancel;
    timerAfterClaimHook = (FiberTask claimed) {
        assert(claimed is sleeper);
        timerAfterClaimHook = null;
        cancel = domain.directorCancelDetailed(sleeper.handle);
        assert(domain.ready == 0,
            "cancellation must not publish a sleeper the expiry claimed");
        assert(!sleeper.terminated);
    };
    assert(domain.pollTimers(MonoTime.currTime + hours(2)) == 1);
    assert(cancel == CancellationResult.requestedWaiting);
    assert(domain.ready == 1);
    rig.runOne(sleeper);
    assert(sleeper.generation == generation);
    assert(sleeper.terminated && sleeper.outcome == FiberOutcome.cancelled
        && !slept);
}

/// A signal is an edge: `await` must be deliverable as soon as it counts in
/// `waiting`. A worker thread runs each waiter while the Director spins on the
/// count and signals once; a wait counted before it was linked would miss its
/// signal (matched 0) and never finish.
private void signalAfterWaitingCount()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    shared int stop;
    auto worker = new Thread({
        auto token = farm.registerProducer(Tier.small);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
        {
            immutable flushed = domain.flush(token);
            if (!view.consumeNext() && flushed == 0) Thread.yield();
        }
        while (view.consumeNext()) {}
        view.unsubscribe();
        farm.unregisterProducer(token);
    });
    worker.start();
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stop, 1);
        worker.join();
    }
    immutable deadline = MonoTime.currTime + seconds(20);
    foreach (i; 0 .. 2_000)
    {
        immutable Signal key = 100 + i;
        domain.spawn({ FiberDomain.await(key); });
        while (domain.waiting != 1)
        {
            assert(MonoTime.currTime < deadline, "await never registered");
            Thread.yield();
        }
        assert(domain.signal(key) == 1, "a counted await missed its signal");
        while (!domain.drained)
        {
            assert(MonoTime.currTime < deadline, "signalled await never ran");
            Thread.yield();
        }
        domain.retire();
    }
}
