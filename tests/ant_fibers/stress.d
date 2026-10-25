module stress;

import antfarm;
import antfarm.actors;
import ant_fibers;
import core.atomic;
import core.lifetime : move;
import core.thread : Thread;
import core.time : MonoTime, hours, msecs, seconds, usecs;
import std.exception : enforce;
import service_plan_tests : synthetic;

enum taskCount = 256;
shared int subscribed;
shared int stopConsumers;
shared int completedNormally;

struct NotifierContext
{
    FiberDomain domain;
    FiberTask[] tasks;
    size_t lane;
    size_t width;
}

struct TimerCancellationRaceContext
{
    FiberDomain domain;
    shared TaskHandle handle;
    MonoTime expireAt;
    shared uint command;
    shared uint submissionDone;
    shared uint cancellationDone;
    shared uint timerDone;
    shared int cancellationResult;
    shared uint expiredCount;
    shared int stop;
}

Thread notifierThread(NotifierContext* context)
{
    return new Thread({
        for (size_t i = context.lane; i < taskCount; i += context.width)
        {
            if ((i & 1) == 0)
                assert(context.domain.requestCancel(context.tasks[i])
                    == CancellationSubmission.queued);
            context.domain.signal(cast(Signal) i + 1);
        }
    });
}

void handleReuseStress()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto first = domain.spawn({});
    auto flushed = domain.flush(token);
    assert(flushed == 1);
    // This table contains exactly one Fiber activation.
    auto consumed = view.consumeNext();
    assert(consumed);
    auto completed = domain.takeCompletions();
    assert(completed.length == 1);
    auto oldHandle = first.handle;
    domain.release(first);

    shared int stop;
    shared int violation;
    auto reader = new Thread({
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
        {
            auto joined = oldHandle.poll();
            if (joined.status != FiberJoinStatus.completed
                && joined.status != FiberJoinStatus.staleHandle)
                atomicStore!(MemoryOrder.rel)(violation, 1);
            FiberOutcome outcome;
            if (oldHandle.tryOutcome(outcome)
                && outcome != FiberOutcome.completed)
                atomicStore!(MemoryOrder.rel)(violation, 1);
            Throwable failure;
            if (oldHandle.tryException(failure) && failure !is null)
                atomicStore!(MemoryOrder.rel)(violation, 1);
        }
    });
    reader.start();
    foreach (i; 0 .. 2_000)
    {
        auto task = domain.spawn({});
        assert(task is first);
        flushed = domain.flush(token);
        assert(flushed == 1);
        consumed = view.consumeNext();
        assert(consumed);
        completed = domain.takeCompletions();
        assert(completed.length == 1 && completed[0] is task);
        domain.release(task);
    }
    atomicStore!(MemoryOrder.rel)(stop, 1);
    reader.join();
    assert(atomicLoad!(MemoryOrder.acq)(violation) == 0);
}

void shutdownAdmissionStress()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 4, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();
    foreach (round; 0 .. 50)
    {
        auto domain = new FiberDomain(farm);
        shared int start;
        shared long admitted;
        Thread[4] producers;
        foreach (ref producer; producers)
        {
            producer = new Thread({
                while (atomicLoad!(MemoryOrder.acq)(start) == 0) Thread.yield();
                foreach (_; 0 .. 16)
                {
                    try
                    {
                        domain.spawn({});
                        atomicFetchAdd(admitted, 1L);
                    }
                    catch (Exception)
                        break;
                }
            });
            producer.start();
        }
        atomicStore!(MemoryOrder.rel)(start, 1);
        Thread.yield();
        domain.beginShutdown(true);
        foreach (producer; producers) producer.join();
        foreach (spin; 0 .. 1_000_000)
        {
            if (domain.ready != 0) domain.flush(token);
            view.consumeNext();
            if (domain.drained) break;
            Thread.yield();
        }
        assert(domain.drained);
        auto completed = domain.takeCompletions();
        assert(completed.length == cast(size_t)
            atomicLoad!(MemoryOrder.acq)(admitted));
    }
}

void cancellationReturnRaceStress()
{
    enum rounds = 2_000;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    shared uint entered;
    shared uint gate;
    shared int stop;
    auto consumer = new Thread({
        ConsumerView view;
        auto subscription = subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            if (!view.consumeNext()) Thread.yield();
        while (view.consumeNext()) {}
    });
    consumer.start();
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stop, 1);
        consumer.join();
    }

    foreach (round; 1 .. rounds + 1)
    {
        auto task = domain.spawn({
            atomicStore!(MemoryOrder.rel)(entered, round);
            while (atomicLoad!(MemoryOrder.acq)(gate) < round) Thread.yield();
        });
        auto flushed = domain.flush(token);
        assert(flushed == 1);
        while (atomicLoad!(MemoryOrder.acq)(entered) < round) Thread.yield();
        CancellationResult cancellation;
        if ((round & 1) == 0)
        {
            cancellation = domain.directorCancelDetailed(task.handle);
            assert(cancellation == CancellationResult.requestedRunning);
            atomicStore!(MemoryOrder.rel)(gate, round);
        }
        else
        {
            atomicStore!(MemoryOrder.rel)(gate, round);
            cancellation = domain.directorCancelDetailed(task.handle);
            assert(cancellation == CancellationResult.requestedRunning
                || cancellation == CancellationResult.alreadyTerminal);
        }
        while (!domain.drained) Thread.yield();
        if (cancellation == CancellationResult.alreadyTerminal)
        {
            assert(task.outcome == FiberOutcome.completed);
            assert(task.cancellationDisposition == CancellationDisposition.none);
        }
        else
        {
            assert(task.outcome == FiberOutcome.cancelled);
            assert(task.cancellationDisposition
                == CancellationDisposition.resultAbrogated);
        }
        auto completed = domain.takeCompletions();
        assert(completed.length == 1 && completed[0] is task);
        domain.release(task);
    }
}

void managedJoinCompletionRaceStress()
{
    enum rounds = 2_000;
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    shared int stop;
    Thread[2] consumers;
    foreach (ref consumer; consumers)
    {
        consumer = new Thread({
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            scope (exit) view.unsubscribe();
            while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
                if (!view.consumeNext()) Thread.yield();
            while (view.consumeNext()) {}
        });
        consumer.start();
    }
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stop, 1);
        foreach (consumer; consumers) consumer.join();
    }

    shared uint gate;
    shared uint joinStarted;
    foreach (round; 1 .. rounds + 1)
    {
        FiberJoinStatus joinedStatus;
        auto target = domain.spawn({
            while (atomicLoad!(MemoryOrder.acq)(gate) < round) Thread.yield();
        });
        auto targetHandle = target.handle;
        auto joiner = domain.spawn({
            atomicStore!(MemoryOrder.rel)(joinStarted, round);
            joinedStatus = targetHandle.joinFiber().status;
        });
        size_t published;
        while (published != 2)
        {
            published += domain.flush(token, 1, DEFAULT_AVG_COST);
            if (published != 2) Thread.yield();
        }
        while (atomicLoad!(MemoryOrder.acq)(joinStarted) < round)
            Thread.yield();
        // Completion may now win before, during, or after waiter insertion.
        atomicStore!(MemoryOrder.rel)(gate, round);
        while (!domain.drained)
        {
            if (domain.ready != 0) domain.flush(token, 2, DEFAULT_AVG_COST);
            Thread.yield();
        }
        assert(target.terminated && joiner.terminated);
        assert(joinedStatus == FiberJoinStatus.completed);
        auto completed = domain.takeCompletions();
        assert(completed.length == 2);
        domain.releaseAll(completed);
    }
}

void managedJoinFanoutCancellationStress()
{
    enum rounds = 100;
    enum width = 32;
    auto farm = AntFarm.create(4, 8, 4, 0, 0, 1, 16_384,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    shared int stop;
    Thread[4] consumers;
    foreach (ref consumer; consumers)
    {
        consumer = new Thread({
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            scope (exit) view.unsubscribe();
            while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
                if (!view.consumeNext()) Thread.yield();
            while (view.consumeNext()) {}
        });
        consumer.start();
    }
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stop, 1);
        foreach (consumer; consumers) consumer.join();
    }

    shared uint gate;
    shared uint started;
    foreach (round; 1 .. rounds + 1)
    {
        auto target = domain.spawn({
            while (atomicLoad!(MemoryOrder.acq)(gate) < round) Thread.yield();
        });
        auto handle = target.handle;
        FiberTask[] joiners;
        joiners.reserve(width);
        foreach (_; 0 .. width)
            joiners ~= domain.spawn({
                atomicFetchAdd(started, 1u);
                cast(void) handle.joinFiber();
            });

        size_t published;
        while (published != width + 1)
        {
            published += domain.flush(token, 1, DEFAULT_AVG_COST);
            if (published != width + 1) Thread.yield();
        }
        immutable expectedStarted = cast(uint)(round * width);
        while (atomicLoad!(MemoryOrder.acq)(started) < expectedStarted)
            Thread.yield();

        if ((round & 1) == 0)
            foreach (i, joiner; joiners)
                if ((i & 1) == 0) domain.directorCancel(joiner.handle);
        atomicStore!(MemoryOrder.rel)(gate, round);
        if ((round & 1) != 0)
            foreach (i, joiner; joiners)
                if ((i & 1) == 0) domain.directorCancel(joiner.handle);

        while (!domain.drained)
        {
            if (domain.ready != 0) domain.flush(token, 32, DEFAULT_AVG_COST);
            Thread.yield();
        }
        assert(target.outcome == FiberOutcome.completed);
        foreach (joiner; joiners)
            assert(joiner.outcome == FiberOutcome.completed
                || joiner.outcome == FiberOutcome.cancelled);
        auto completed = domain.takeCompletions();
        assert(completed.length == width + 1);
        domain.releaseAll(completed);
    }
}

void timerCancellationRaceStress()
{
    enum rounds = 2_000;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    TimerCancellationRaceContext context;
    context.domain = domain;
    context.expireAt = MonoTime.currTime + hours(2);
    auto canceller = new Thread({
        foreach (round; 1 .. rounds + 1)
        {
            while (atomicLoad!(MemoryOrder.acq)(context.command) < round)
            {
                if (atomicLoad!(MemoryOrder.acq)(context.stop) != 0) return;
                Thread.yield();
            }
            // One third forces expiry first, one third races freely, and one
            // third lets cancellation remove the heap entry first.
            if (round % 3 == 2)
                while (atomicLoad!(MemoryOrder.acq)(context.timerDone) < round)
                {
                    if (atomicLoad!(MemoryOrder.acq)(context.stop) != 0) return;
                    Thread.yield();
                }
            auto handle = cast(TaskHandle) context.handle;
            auto submitted = context.domain.requestCancel(handle);
            assert(submitted == CancellationSubmission.queued);
            atomicStore!(MemoryOrder.rel)(context.submissionDone, round);
        }
    });
    auto timerPoller = new Thread({
        foreach (round; 1 .. rounds + 1)
        {
            while (atomicLoad!(MemoryOrder.acq)(context.command) < round)
            {
                if (atomicLoad!(MemoryOrder.acq)(context.stop) != 0) return;
                Thread.yield();
            }
            if (round % 3 == 1)
            {
                while (atomicLoad!(MemoryOrder.acq)(context.cancellationDone)
                        < round)
                {
                    if (atomicLoad!(MemoryOrder.acq)(context.stop) != 0) return;
                    Thread.yield();
                }
            }
            auto expired = context.domain.pollTimers(context.expireAt);
            atomicStore!(MemoryOrder.rel)(context.expiredCount,
                cast(uint) expired);
            atomicStore!(MemoryOrder.rel)(context.timerDone, round);
        }
    });
    canceller.start();
    timerPoller.start();
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(context.stop, 1);
        canceller.join();
        timerPoller.join();
    }

    size_t waitingWins;
    size_t readyWins;
    foreach (round; 1 .. rounds + 1)
    {
        auto task = domain.spawn({ FiberDomain.sleepFor(hours(1)); });
        auto flushed = domain.flush(token);
        assert(flushed == 1);
        auto consumed = view.consumeNext();
        assert(consumed && domain.waiting == 1 && domain.ready == 0);

        context.handle = cast(shared TaskHandle) task.handle;
        atomicStore!(MemoryOrder.rel)(context.command, round);
        while (atomicLoad!(MemoryOrder.acq)(context.submissionDone) < round)
            Thread.yield();
        auto applications = domain.drainCancellationRequests();
        assert(applications.length == 1);
        atomicStore!(MemoryOrder.rel)(context.cancellationResult,
            cast(int) applications[0].result);
        atomicStore!(MemoryOrder.rel)(context.cancellationDone, round);
        while (atomicLoad!(MemoryOrder.acq)(context.timerDone) < round)
            Thread.yield();

        auto cancellation = cast(CancellationResult)
            atomicLoad!(MemoryOrder.acq)(context.cancellationResult);
        immutable expired = atomicLoad!(MemoryOrder.acq)(context.expiredCount);
        assert(expired <= 1);
        if (cancellation == CancellationResult.requestedWaiting)
            ++waitingWins;
        else if (cancellation == CancellationResult.requestedReady)
            ++readyWins;
        else
            assert(false, "timer/cancellation race returned an impossible phase");
        assert(domain.waiting == 0 && domain.ready == 1);
        long unusedDeadline;
        auto timerRemains = domain.nextTimerDeadline(unusedDeadline);
        assert(!timerRemains);

        flushed = domain.flush(token);
        assert(flushed == 1);
        consumed = view.consumeNext();
        assert(consumed && task.terminated && domain.drained);
        assert(task.outcome == FiberOutcome.cancelled);
        assert(task.cancellationDisposition
            == CancellationDisposition.acknowledged);
        auto completed = domain.takeCompletions();
        assert(completed.length == 1 && completed[0] is task);
        domain.release(task);
    }
    assert(waitingWins != 0 && readyWins != 0);
}

void lifecycleRetentionStress()
{
    enum producerCount = 4;
    enum tasksPerProducer = 250;
    enum totalTasks = producerCount * tasksPerProducer;
    enum retentionLimit = 256;
    auto farm = AntFarm.create(4, 8, 1, 0, 0, 1, 16_384,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    auto domain = new FiberDomain(farm);
    domain.enableLifecycleEvents(retentionLimit);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared uint producersDone;
    shared uint admitted;
    shared uint backpressured;
    shared int stopProducers;
    Thread[producerCount] producers;
    foreach (ref producer; producers)
    {
        producer = new Thread({
            size_t produced;
            while (produced != tasksPerProducer
                && atomicLoad!(MemoryOrder.acq)(stopProducers) == 0)
            {
                try
                {
                    domain.spawn({});
                    ++produced;
                    atomicFetchAdd(admitted, 1u);
                }
                catch (FiberLifecycleBackpressure)
                {
                    atomicFetchAdd(backpressured, 1u);
                    Thread.yield();
                }
            }
            atomicFetchAdd(producersDone, 1u);
        });
        producer.start();
    }
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stopProducers, 1);
        foreach (producer; producers) producer.join();
    }

    while (atomicLoad!(MemoryOrder.acq)(backpressured) == 0
        && atomicLoad!(MemoryOrder.acq)(producersDone) != producerCount)
        Thread.yield();
    ulong previousSequence;
    while (atomicLoad!(MemoryOrder.acq)(producersDone) != producerCount
        || !domain.drained || domain.pendingEvents != 0)
    {
        if (domain.ready != 0) domain.flush(token);
        view.consumeNext();
        auto events = domain.takeLifecycleEvents(3);
        foreach (event; events)
        {
            assert(event.sequence == previousSequence + 1);
            previousSequence = event.sequence;
        }
        assert(domain.lifecycleReserved <= retentionLimit);
        if (events.length == 0) Thread.yield();
    }
    assert(atomicLoad!(MemoryOrder.acq)(admitted) == totalTasks);
    assert(atomicLoad!(MemoryOrder.acq)(backpressured) != 0);
    assert(domain.lifecycleReserved == 0 && domain.pendingEvents == 0);
    auto completed = domain.takeCompletions();
    assert(completed.length == totalTasks);
    domain.releaseAll(completed);
}

void sharedDomainLaneStress()
{
    enum tasksPerLane = 500;
    auto farmA = AntFarm.create(4, 8, 2, 0, 0, 1, 16_384,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(4, 8, 2, 0, 0, 1, 16_384,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    auto domain = new FiberDomain;
    auto laneA = new FiberLane(domain, farmA);
    auto laneB = new FiberLane(domain, farmB);
    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
    scope (exit)
    {
        farmA.unregisterProducer(tokenA);
        farmB.unregisterProducer(tokenB);
    }
    shared uint ran;
    FiberTask[] tasksA;
    FiberTask[] tasksB;
    tasksA.reserve(tasksPerLane);
    tasksB.reserve(tasksPerLane);
    foreach (_; 0 .. tasksPerLane)
    {
        tasksA ~= laneA.spawn({
            FiberDomain.yieldReady();
            atomicFetchAdd(ran, 1u);
        });
        tasksB ~= laneB.spawn({
            FiberDomain.yieldReady();
            atomicFetchAdd(ran, 1u);
        });
    }

    shared uint subscribed;
    shared int stopConsumers;
    enum consumersPerFarm = 2;
    Thread consumeFarm(AntFarm* farm)
    {
        return new Thread({
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            atomicFetchAdd(subscribed, 1u);
            scope (exit) view.unsubscribe();
            while (atomicLoad!(MemoryOrder.acq)(stopConsumers) == 0)
            {
                if (!view.consumeNext())
                    Thread.yield();
            }
            while (view.consumeNext()) {}
        });
    }
    Thread[] consumers;
    foreach (_; 0 .. consumersPerFarm)
    {
        consumers ~= consumeFarm(farmA);
        consumers ~= consumeFarm(farmB);
    }
    foreach (consumer; consumers) consumer.start();
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
        foreach (consumer; consumers) consumer.join();
    }
    while (atomicLoad!(MemoryOrder.acq)(subscribed) != consumers.length)
        Thread.yield();
    auto spinStart = MonoTime.currTime;
    ulong spins;
    while (!domain.drained)
    {
        // Always yield after a flush attempt. A release-build producer that
        // never yields can delay the only consumer of a lane long enough
        // that leftover Farm tables look like a stranded activation.
        if (laneA.ready != 0) laneA.flush(tokenA);
        if (laneB.ready != 0) laneB.flush(tokenB);
        Thread.yield();
        ++spins;
        if ((spins & 0x3FFF) == 0
            && MonoTime.currTime - spinStart > seconds(8))
        {
            import std.stdio : stderr, writefln;
            size_t termA, termB, resumeA, resumeB;
            foreach (task; tasksA)
            {
                if (task.terminated) ++termA;
                resumeA += task.resumeCount;
            }
            foreach (task; tasksB)
            {
                if (task.terminated) ++termB;
                resumeB += task.resumeCount;
            }
            stderr.writefln(
                "sharedDomainLaneStress stall: active=%s waiting=%s ran=%s " ~
                "readyA=%s readyB=%s publishedA=%s publishedB=%s " ~
                "termA=%s/%s termB=%s/%s resumesA=%s resumesB=%s",
                domain.active, domain.waiting, atomicLoad!(MemoryOrder.acq)(ran),
                laneA.ready, laneB.ready, laneA.published, laneB.published,
                termA, tasksPerLane, termB, tasksPerLane,
                resumeA, resumeB);
            assert(false, "sharedDomainLaneStress: drain stalled");
        }
    }
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    assert(atomicLoad!(MemoryOrder.acq)(ran) == tasksPerLane * 2);
    assert(laneA.ready == 0 && laneB.ready == 0 && domain.ready == 0);
    auto completed = domain.takeCompletions();
    assert(completed.length == tasksPerLane * 2);
    domain.releaseAll(completed);
}

void remoteSweeperStress()
{
    enum tasksPerLane = 400;
    auto farmA = AntFarm.create(4, 8, 2, 0, 0, 2, 16_384,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(4, 8, 2, 0, 0, 2, 16_384,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    auto domain = new FiberDomain;
    auto laneA = new FiberLane(domain, farmA);
    auto laneB = new FiberLane(domain, farmB);
    // Group 0 (A) has the two covering workers and rescues group 1 (B), which
    // has no native consumer.
    installFiberPlan(new FiberServicePlan(synthetic([2, 0]))
        .bind(0, laneA).bind(1, laneB).rescue(1, 0), null);
    scope (exit) uninstallFiberPlan();
    shared uint ran;
    FiberTask[] tasks;
    tasks.reserve(tasksPerLane * 2);
    foreach (_; 0 .. tasksPerLane)
    {
        tasks ~= laneA.spawn({
            FiberDomain.yieldReady();
            atomicFetchAdd(ran, 1u);
        });
        tasks ~= laneB.spawn({
            FiberDomain.yieldReady();
            atomicFetchAdd(ran, 1u);
        });
    }

    shared uint subscribed;
    shared int stopConsumers;
    // Native B is disabled. Covering workers own home (A) and remote (B)
    // ConsumerViews plus producer tokens, matching a managed responder.
    Thread coveringWorker()
    {
        return new Thread({
            auto tokenA = farmA.registerProducer(Tier.small);
            auto tokenB = farmB.registerProducer(Tier.small);
            if (!tokenA.valid || !tokenB.valid)
                throw new Exception("remoteSweeperStress: producer registration failed");
            scope (exit)
            {
                farmA.unregisterProducer(tokenA);
                farmB.unregisterProducer(tokenB);
            }
            ConsumerView home;
            ConsumerView remote;
            subscribeOrThrow(home, farmA);
            scope (exit) home.unsubscribe();
            subscribeOrThrow(remote, farmB);
            scope (exit) remote.unsubscribe();
            atomicFetchAdd(subscribed, 1u);
            while (atomicLoad!(MemoryOrder.acq)(stopConsumers) == 0)
            {
                immutable consumedHome = home.consumeNext();
                if (laneA.ready != 0) laneA.flush(tokenA);
                immutable consumedRemote = remote.consumeNext();
                if (laneB.ready != 0) laneB.flush(tokenB);
                if (!consumedHome && !consumedRemote
                    && laneA.ready == 0 && laneB.ready == 0
                    && laneA.published == 0 && laneB.published == 0)
                    Thread.yield();
            }
            while (home.consumeNext()) {}
            while (remote.consumeNext()) {}
        });
    }
    auto consumers = [coveringWorker(), coveringWorker()];
    foreach (consumer; consumers) consumer.start();
    scope (exit)
    {
        atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
        foreach (consumer; consumers) consumer.join();
    }
    while (atomicLoad!(MemoryOrder.acq)(subscribed) != consumers.length)
        Thread.yield();
    auto spinStart = MonoTime.currTime;
    ulong spins;
    while (!domain.drained)
    {
        Thread.yield();
        ++spins;
        if ((spins & 0x3FFF) == 0
            && MonoTime.currTime - spinStart > seconds(8))
        {
            import std.stdio : stderr, writefln;
            stderr.writefln(
                "remoteSweeperStress stall: active=%s ran=%s readyA=%s readyB=%s " ~
                "publishedA=%s publishedB=%s",
                domain.active, atomicLoad!(MemoryOrder.acq)(ran),
                laneA.ready, laneB.ready, laneA.published, laneB.published);
            assert(false, "remoteSweeperStress: drain stalled");
        }
    }
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    assert(atomicLoad!(MemoryOrder.acq)(ran) == tasksPerLane * 2);
    assert(laneA.ready == 0 && laneB.ready == 0);
    assert(laneA.published == 0 && laneB.published == 0);
    foreach (task; tasks)
        assert(task.terminated && task.resumeCount == 2);
    auto completed = domain.takeCompletions();
    assert(completed.length == tasksPerLane * 2);
    domain.releaseAll(completed);
}

private struct ActorWaveStressResult
{
    ulong visits;
    ulong checksum;
}

private struct ActorWaveStressState
{
    size_t actorId;
    ulong visits;
    ActorWaveStressResult* result;
}

private ulong actorWaveStressChecksum(size_t actorId, ulong visits)
    pure nothrow @nogc @safe
{
    return (cast(ulong) actorId + 1) * 0x9e37_79b9_7f4a_7c15UL
        ^ visits * 0xbf58_476d_1ce4_e5b9UL;
}

private void actorWaveStressDormant(scope ref ActorBorrow!ActorWaveStressState,
        scope ref ActorContext) nothrow @nogc @system
{
}

private void actorWaveStressOperation(
        scope ref ActorBorrow!ActorWaveStressState actor)
    nothrow @nogc @system
{
    ++actor.value.visits;
    actor.value.result.visits = actor.value.visits;
    actor.value.result.checksum = actorWaveStressChecksum(
        actor.value.actorId, actor.value.visits);
}

private final class ActorWaveStressProducer
{
    enum size_t actorsPerPublication = 127;

    AntFarm* farm;
    Token producer;
    ActorHandle!ActorWaveStressState[] actors;
    ActorWave wave;
    ActorWaveTrigger trigger;
    size_t generations;
    ulong tables;

    this(FiberDomain domain, AntFarm* farm, Token producer,
            ActorHandle!ActorWaveStressState[] actors, size_t generations)
    {
        this.farm = farm;
        this.producer = move(producer);
        this.actors = actors;
        this.generations = generations;
        trigger = new ActorWaveTrigger(domain);
    }

    void run()
    {
        foreach (generation; 1 .. generations + 1)
        {
            if (generation == 1)
                enforce(!wave.handle.valid,
                    "actor-wave stress wave began initialized");
            else
                enforce(wave.handle.finished,
                    "actor-wave stress reused an unfinished wave");
            wave.begin(farm, trigger.hook);
            size_t offset;
            while (offset != actors.length)
            {
                immutable candidateEnd = offset + actorsPerPublication;
                immutable end = candidateEnd < actors.length
                    ? candidateEnd : actors.length;
                immutable written = wave.publish!actorWaveStressOperation(
                    actors[offset .. end], producer, avgCostForChunk(32));
                enforce(!wave.handle.failed,
                    "actor-wave stress publication failed");
                if (written == 0)
                    FiberDomain.yieldReady();
                else
                    offset += written;
            }

            auto completion = wave.seal();
            trigger.waitNext();
            enforce(completion.finished && !completion.failed,
                "actor-wave stress completion failed");
            enforce(completion.progress == completion.length,
                "actor-wave stress table completion mismatch");
            enforce(trigger.completed == generation,
                "actor-wave stress trigger generation mismatch");
            tables += completion.length;

            // Change which streams tend to publish and finish together.
            if ((generation & 7) == 0)
                FiberDomain.yieldReady();
        }
    }
}

/// Exercise the complete actor-wave completion bridge rather than its pieces:
/// multi-table wave completion, concurrent deferred-trigger publication,
/// parked Fiber wakeup, and reuse of both wave and trigger generations.
void actorWaveGenerationStress()
{
    enum streamCount = 2;
    enum actorsPerStream = 1_024;
    enum generations = 512;
    enum consumerCount = 4;
    enum totalActors = streamCount * actorsPerStream;

    auto farm = AntFarm.create(8, 8, consumerCount, 0, 0,
        streamCount + 1, 16_384, DEFAULT_SMALL_TABLE_THRESHOLD, false);
    enforce(farm !is null, "actor-wave stress Farm allocation failed");
    scope (exit) farm.destroy();

    auto runtime = ActorRuntime.create(farm, totalActors);
    enforce(runtime !is null,
        "actor-wave stress runtime allocation failed");
    auto domain = new FiberDomain(farm);

    auto results = new ActorWaveStressResult[totalActors];
    auto owners = new ActorOwner!ActorWaveStressState[totalActors];
    auto sets = new ActorHandle!ActorWaveStressState[][streamCount];
    foreach (stream; 0 .. streamCount)
        sets[stream] = new ActorHandle!ActorWaveStressState[actorsPerStream];
    foreach (i; 0 .. totalActors)
    {
        ActorWaveStressState initial;
        initial.actorId = i;
        initial.result = &results[i];
        owners[i] = runtime.createActor!(ActorWaveStressState,
            actorWaveStressDormant)(initial);
        enforce(owners[i].valid,
            "actor-wave stress actor allocation failed");
        sets[i / actorsPerStream][i % actorsPerStream] = owners[i].handle;
    }

    auto schedulerProducer = farm.registerProducer(Tier.small);
    enforce(schedulerProducer.valid,
        "actor-wave stress scheduler producer registration failed");
    ActorWaveStressProducer[streamCount] jobs;
    FiberTask[streamCount] tasks;
    foreach (stream; 0 .. streamCount)
    {
        auto producer = farm.registerProducer(Tier.small);
        enforce(producer.valid,
            "actor-wave stress wave producer registration failed");
        jobs[stream] = new ActorWaveStressProducer(domain, farm,
            move(producer), sets[stream], generations);
        tasks[stream] = domain.spawn(&jobs[stream].run);
    }

    // Publish the initial Fiber activation table before starting consumers.
    // Its Fibers each publish and seal a wave before the director begins
    // polling deferred completion edges. Subsequent generations naturally
    // mix parked wakeups with valid preposted completions.
    immutable initiallyFlushed = domain.flush(
        schedulerProducer, streamCount, avgCostForChunk(32));
    enforce(initiallyFlushed == streamCount,
        "actor-wave stress initial Fiber publication failed");

    shared uint consumersSubscribed;
    shared int stopConsumers;
    Thread[consumerCount] consumers;
    foreach (ref consumer; consumers)
    {
        consumer = new Thread({
            ConsumerView view;
            subscribeOrThrow(view, farm);
            atomicFetchAdd(consumersSubscribed, 1u);
            while (atomicLoad!(MemoryOrder.acq)(stopConsumers) == 0)
            {
                if (!view.consumeNext()) Thread.yield();
            }
            view.unsubscribe();
        });
        consumer.start();
    }
    while (atomicLoad!(MemoryOrder.acq)(consumersSubscribed) != consumerCount)
        Thread.yield();
    bool consumersJoined;
    scope (exit)
    {
        if (!consumersJoined)
        {
            atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
            foreach (consumer; consumers) consumer.join();
        }
    }

    auto deadline = MonoTime.currTime + 20.seconds;
    while (!domain.drained)
    {
        domain.pollGenerationTriggers();
        if (domain.ready != 0)
        {
            if (domain.flush(schedulerProducer, 32, avgCostForChunk(32)) == 0)
                Thread.yield();
        }
        else
            Thread.yield();
        if (MonoTime.currTime >= deadline)
            enforce(false, "actorWaveGenerationStress: drain stalled");
    }
    domain.pollGenerationTriggers();
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    foreach (consumer; consumers) consumer.join();
    consumersJoined = true;

    foreach (stream; 0 .. streamCount)
    {
        if (tasks[stream].outcome != FiberOutcome.completed)
        {
            import std.stdio : stderr;
            stderr.writefln("actor-wave stress stream %s: outcome=%s error=%s",
                stream, tasks[stream].outcome, tasks[stream].exception);
        }
    }
    foreach (stream; 0 .. streamCount)
    {
        enforce(tasks[stream].outcome == FiberOutcome.completed,
            "actor-wave stress orchestration Fiber failed");
        enforce(jobs[stream].trigger.completed == generations,
            "actor-wave stress final trigger generation mismatch");
        enforce(jobs[stream].tables
            >= generations * ((actorsPerStream
                + ActorWaveStressProducer.actorsPerPublication - 1)
                / ActorWaveStressProducer.actorsPerPublication),
            "actor-wave stress did not produce the expected physical tables");
        farm.unregisterProducer(jobs[stream].producer);
    }
    domain.releaseAll(domain.takeCompletions());
    farm.unregisterProducer(schedulerProducer);

    foreach (i; 0 .. totalActors)
    {
        enforce(results[i].visits == generations,
            "actor-wave stress actor visit count mismatch");
        enforce(results[i].checksum == actorWaveStressChecksum(i, generations),
            "actor-wave stress actor checksum mismatch");
        immutable retirement = owners[i].requestRetire();
        enforce(retirement == ActorRetireResult.requested,
            "actor-wave stress retirement failed");
        enforce(owners[i].retired,
            "actor-wave stress idle actor did not retire synchronously");
        immutable reclamation = owners[i].reclaim();
        enforce(reclamation == ActorReclaimResult.reclaimed,
            "actor-wave stress reclamation failed");
    }
    enforce(runtime.live == 0 && runtime.ready == 0,
        "actor-wave stress runtime did not quiesce");
    runtime.destroy();
}

void syncPrimitiveStress()
{
    enum waiters = 32;
    auto farm = AntFarm.create(4, 8, 2, 0, 0, 1, 16_384,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto sem = new FiberSemaphore(domain, 0);
    shared uint ran;
    foreach (_; 0 .. waiters)
        domain.spawn({
            sem.wait();
            atomicFetchAdd(ran, 1u);
        });

    auto spinStart = MonoTime.currTime;
    ulong spins;
    while (domain.waiting != waiters)
    {
        domain.flush(token);
        if (!view.consumeNext())
            Thread.yield();
        ++spins;
        if ((spins & 0x3FFF) == 0
            && MonoTime.currTime - spinStart > seconds(8))
            assert(false, "syncPrimitiveStress: waiters stalled");
    }
    sem.post(waiters);
    while (!domain.drained)
    {
        domain.flush(token);
        if (!view.consumeNext())
            Thread.yield();
        ++spins;
        if ((spins & 0x3FFF) == 0
            && MonoTime.currTime - spinStart > seconds(8))
            assert(false, "syncPrimitiveStress: drain stalled");
    }
    assert(atomicLoad(ran) == waiters);
    domain.releaseAll(domain.takeCompletions());
}

/// Permit conservation under concurrent service: four worker threads pump the
/// domain (timers, cancellation drain, flush, consume) while producers post
/// and consumers acquire, half untimed and half with short timeouts. The
/// Director keeps cancelling consumers and admitting replacements, so woken,
/// parked and registering waiters are all cancelled. Once every permit is
/// posted, cancellation stops and consumers wait untimed, so the run ends
/// only if the parked consumers are woken for every remaining permit. The
/// final count must account for every posted permit. Looping consumers can
/// absorb a swallowed wake mid-run; semaphore.d pins pass-on itself.
void semaphoreConservationStress()
{
    enum producers = 8;
    enum perProducer = 4_000;
    enum total = producers * perProducer;
    enum consumers = 16;
    // One small producer token per worker.
    auto farm = AntFarm.create(4, 8, 4, 0, 0, 4, 16_384,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    auto sem = new FiberSemaphore(domain, 0);
    shared long taken, posted;
    shared int stop;

    void delegate() consumer(bool timed)
    {
        return () {
            while (atomicLoad(taken) < total)
            {
                if (timed && atomicLoad(posted) < total)
                {
                    if (!sem.wait(usecs(50))) continue;
                }
                else
                    sem.wait();
                atomicFetchAdd(taken, 1L);
            }
        };
    }

    Thread[4] workers;
    foreach (ref worker; workers)
    {
        worker = new Thread({
            auto token = farm.registerProducer(Tier.small);
            ConsumerView view;
            subscribeOrThrow(view, farm);
            while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            {
                domain.pollTimers();
                if (domain.pendingCancellationRequests != 0)
                    cast(void) domain.drainCancellationRequests();
                immutable flushed = domain.flush(token);
                if (!view.consumeNext() && flushed == 0) Thread.yield();
            }
            while (view.consumeNext()) {}
            view.unsubscribe();
            farm.unregisterProducer(token);
        });
        worker.start();
    }

    TaskHandle[consumers] live;
    foreach (i, ref h; live)
        h = domain.spawnHandle(consumer(i % 2 == 1));
    foreach (_; 0 .. producers)
        domain.spawn({
            foreach (i; 0 .. perProducer)
            {
                sem.post();
                atomicFetchAdd(posted, 1L);
                if (i % 64 == 0) FiberDomain.yieldReady();
            }
        });

    ulong cancelled, rng = 0x9E37_79B9_7F4A_7C15UL;
    immutable start = MonoTime.currTime;
    while (atomicLoad(taken) < total)
    {
        enforce(MonoTime.currTime - start < seconds(20),
            "semaphore conservation stress stalled: a permit's wake was lost");
        if (atomicLoad(posted) == total)
        {
            Thread.sleep(msecs(1));
            continue;
        }
        rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
        immutable i = cast(size_t)(rng % consumers);
        if (domain.requestCancel(live[i]) == CancellationSubmission.queued)
        {
            ++cancelled;
            live[i] = domain.spawnHandle(consumer(i % 2 == 1));
        }
        Thread.sleep(usecs(200));
    }
    // Every consumer still waiting is parked or polling with no permit left.
    foreach (h; live) cast(void) domain.requestCancel(h);
    immutable drainStart = MonoTime.currTime;
    while (!domain.drained)
    {
        enforce(MonoTime.currTime - drainStart < seconds(10),
            "semaphore conservation stress: shutdown drain stalled");
        Thread.sleep(msecs(1));
    }
    atomicStore!(MemoryOrder.rel)(stop, 1);
    foreach (worker; workers) worker.join();
    enforce(atomicLoad(taken) + sem.available == total,
        "semaphore conservation stress: permits created or lost");
    enforce(sem.available == 0 && domain.waiting == 0,
        "semaphore conservation stress: residual permits or waits");
    enforce(cancelled != 0, "semaphore conservation stress cancelled nothing");
    domain.releaseAll(domain.takeCompletions());
}

void main()
{
    auto farm = AntFarm.create(4, 8, 4, 0, 0, 1, 16_384,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    domain.enableLifecycleEvents();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    FiberTask[] tasks;
    foreach (i; 0 .. taskCount)
    {
        immutable key = cast(Signal) i + 1;
        tasks ~= domain.spawn({
            FiberDomain.await(key);
            atomicFetchAdd(completedNormally, 1);
        });
    }

    Thread[4] consumers;
    foreach (i; 0 .. consumers.length)
    {
        consumers[i] = new Thread({
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            atomicFetchAdd(subscribed, 1);
            while (atomicLoad!(MemoryOrder.acq)(stopConsumers) == 0)
            {
                if (!view.consumeNext()) Thread.yield();
            }
            while (view.consumeNext()) {}
            view.unsubscribe();
        });
        consumers[i].start();
    }
    while (atomicLoad!(MemoryOrder.acq)(subscribed) != consumers.length)
        Thread.yield();

    while (domain.ready != 0)
    {
        if (domain.flush(token, 256, DEFAULT_AVG_COST) == 0) Thread.yield();
    }
    while (domain.waiting != taskCount) Thread.yield();

    // Cancellation and signal delivery compete over the same wait registry.
    Thread[4] notifiers;
    NotifierContext[4] notifierContexts;
    foreach (n; 0 .. notifiers.length)
    {
        notifierContexts[n] = NotifierContext(domain, tasks, n, notifiers.length);
        notifiers[n] = notifierThread(&notifierContexts[n]);
        notifiers[n].start();
    }
    foreach (thread; notifiers) thread.join();

    auto applications = domain.drainCancellationRequests();
    assert(applications.length == taskCount / 2);

    while (!domain.drained)
    {
        if (domain.ready != 0 && domain.flush(token, 256, DEFAULT_AVG_COST) == 0)
            Thread.yield();
    }
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    foreach (thread; consumers) thread.join();

    auto outcomes = domain.takeCompletions();
    assert(outcomes.length == taskCount);
    size_t cancelled, succeeded;
    foreach (task; outcomes)
    {
        if (task.outcome == FiberOutcome.cancelled) ++cancelled;
        if (task.outcome == FiberOutcome.completed) ++succeeded;
        assert(task.outcome != FiberOutcome.failed);
    }
    assert(cancelled == taskCount / 2);
    assert(succeeded == taskCount / 2);
    assert(atomicLoad(completedNormally) == taskCount / 2);
    auto events = domain.takeLifecycleEvents();
    size_t admittedEvents, cancellationEvents, terminalEvents, failureEvents;
    ubyte[ulong] phase;
    foreach (event; events)
    {
        auto key = event.task.diagnosticId;
        final switch (event.kind)
        {
        case FiberLifecycleKind.admitted:
            assert(phase.get(key, 0) == 0);
            phase[key] = 1;
            ++admittedEvents;
            break;
        case FiberLifecycleKind.cancellation:
            assert(phase.get(key, 0) == 1);
            phase[key] = 2;
            ++cancellationEvents;
            break;
        case FiberLifecycleKind.terminal:
            assert(phase.get(key, 0) == 1 || phase.get(key, 0) == 2);
            assert(event.cancellationDisposition
                == (event.outcome == FiberOutcome.cancelled
                    ? CancellationDisposition.acknowledged
                    : CancellationDisposition.none));
            phase[key] = 3;
            ++terminalEvents;
            break;
        case FiberLifecycleKind.failure:
            ++failureEvents;
            break;
        }
    }
    assert(admittedEvents == taskCount);
    assert(cancellationEvents == taskCount / 2);
    assert(terminalEvents == taskCount);
    assert(failureEvents == 0);
    assert(domain.lifecycleReserved == 0);

    handleReuseStress();
    shutdownAdmissionStress();
    cancellationReturnRaceStress();
    managedJoinCompletionRaceStress();
    managedJoinFanoutCancellationStress();
    timerCancellationRaceStress();
    lifecycleRetentionStress();
    sharedDomainLaneStress();
    remoteSweeperStress();
    actorWaveGenerationStress();
    syncPrimitiveStress();
    semaphoreConservationStress();

    import core.stdc.stdio : printf;
    printf("fiber stress passed\n");
}
