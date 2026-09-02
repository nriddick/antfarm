module smoke;

import antfarm_fibers;
import antfarm;
import core.atomic;
import core.thread : Fiber, Thread;
import core.time : msecs;

shared int steps;
shared int barePayloadRuns;
shared int functionSpawnRuns;

void functionSpawnBody()
{
    atomicFetchAdd(functionSpawnRuns, 1);
    FiberDomain.yieldReady();
    atomicFetchAdd(functionSpawnRuns, 1);
}

void wakeNotifier(void* context) nothrow @nogc
{
    auto count = cast(shared(int)*) context;
    atomicFetchAdd(*count, 1);
}

long barePayloadCallback(PayloadHeader*, PayloadBody body, ulong)
    nothrow @nogc @system
{
    assert(body.length == 1 && body[0] == 0xA17F_A4FUL);
    atomicFetchAdd(barePayloadRuns, 1);
    return 1;
}

final class MigrationBox
{
    int value;
    this(int value) { this.value = value; }
}

void uxHelpersSmoke()
{
    import threadpool : PoolOptions;

    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto lane = new FiberLane(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    atomicStore(functionSpawnRuns, 0);
    auto first = lane.spawn(&functionSpawnBody);
    drainUntilEmpty(lane.domain, token, view);
    assert(atomicLoad(functionSpawnRuns) == 2);
    lane.domain.releaseAll(lane.domain.takeCompletions());

    auto recycled = lane.spawn(&functionSpawnBody);
    assert(recycled is first);
    drainUntilEmpty(lane.domain, token, view, 256, 0);
    assert(atomicLoad(functionSpawnRuns) == 4);
    lane.domain.releaseAll(lane.domain.takeCompletions());

    PoolOptions options;
    applyFiberTopologyPreset(
        FiberTopologyPreset.fiberThroughput, [lane], options);
    assert(!options.skipSmtSiblings && options.managedWorker.pump !is null);
    assert(lane.flushBatch == 256 && lane.avgCost == 0);
    applyFiberTopologyPreset(
        FiberTopologyPreset.payloadThroughput, [lane], options);
    assert(options.skipSmtSiblings);
    assert(lane.flushBatch == 32 && lane.avgCost == 2);
}

void cancellationOwnershipSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    backend.enableLifecycleEvents();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int nonDirectorRejected;
    auto intruder = new Thread({
        try cast(void) backend.drainCancellationRequests();
        catch (Exception)
            atomicStore(nonDirectorRejected, 1);
    });
    intruder.start();
    intruder.join();
    assert(atomicLoad(nonDirectorRejected) == 1);

    CancellationResult localResult;
    auto local = backend.spawn({
        localResult = FiberDomain.cancelCurrent();
        FiberDomain.yieldReady();
    });
    drainUntilEmpty(backend, token, view);
    assert(localResult == CancellationResult.requestedRunning);
    assert(local.outcome == FiberOutcome.cancelled
        && local.cancellationDisposition == CancellationDisposition.acknowledged);
    auto events = backend.takeLifecycleEvents();
    size_t cancellationEvents;
    foreach (event; events)
        if (event.kind == FiberLifecycleKind.cancellation)
            ++cancellationEvents;
    assert(cancellationEvents == 1);
    backend.releaseAll(backend.takeCompletions());

    auto remote = backend.spawn({});
    assert(backend.requestCancel(remote.handle) == CancellationSubmission.queued);
    assert(backend.requestCancel(remote.handle) == CancellationSubmission.queued);
    auto applications = backend.drainCancellationRequests();
    assert(applications.length == 2);
    assert(applications[0].result == CancellationResult.wonBeforeEntry);
    assert(applications[1].result == CancellationResult.alreadyRequested);
    drainUntilEmpty(backend, token, view);
    assert(remote.outcome == FiberOutcome.cancelled);
    events = backend.takeLifecycleEvents();
    cancellationEvents = 0;
    foreach (event; events)
        if (event.kind == FiberLifecycleKind.cancellation)
            ++cancellationEvents;
    assert(cancellationEvents == 1);
    auto remoteHandle = remote.handle;
    backend.releaseAll(backend.takeCompletions());

    auto replacement = backend.spawn({});
    assert(replacement is remote);
    assert(backend.requestCancel(remoteHandle)
        == CancellationSubmission.staleHandle);
    drainUntilEmpty(backend, token, view);
    assert(replacement.outcome == FiberOutcome.completed
        && replacement.cancellationDisposition == CancellationDisposition.none);
    backend.takeLifecycleEvents();
    backend.releaseAll(backend.takeCompletions());

    auto terminal = backend.spawn({});
    auto terminalHandle = terminal.handle;
    drainUntilEmpty(backend, token, view);
    assert(backend.requestCancel(terminalHandle)
        == CancellationSubmission.queued);
    auto terminalCompletions = backend.takeCompletions();
    assert(terminalCompletions.length == 1
        && terminalCompletions[0] is terminal);
    backend.takeLifecycleEvents();
    bool pendingRejected;
    try backend.release(terminal);
    catch (Exception)
        pendingRejected = true;
    assert(pendingRejected);
    applications = backend.drainCancellationRequests();
    assert(applications.length == 1
        && applications[0].result == CancellationResult.alreadyTerminal);
    assert(terminal.outcome == FiberOutcome.completed
        && terminal.cancellationDisposition == CancellationDisposition.none);
    backend.release(terminal);
}

void manualSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto task = backend.spawn({
        atomicFetchAdd(steps, 1);
        FiberBackend.await(42);
        atomicFetchAdd(steps, 1);
        FiberBackend.yieldReady();
        atomicFetchAdd(steps, 1);
    });

    auto flushed = backend.flush(token);
    assert(flushed == 1);
    // One resume per call: test artifact, not the worker visit.
    auto consumed = view.consumeQuantum();
    assert(consumed);
    assert(atomicLoad(steps) == 1 && backend.waiting == 1);
    auto signalled = backend.signal(42);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(atomicLoad(steps) == 2 && backend.ready == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(atomicLoad(steps) == 3 && task.terminated);
    auto completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is task);
    assert(task.outcome == FiberOutcome.completed && task.exception is null);
}

void defaultBatchSmoke()
{
    enum count = 40;
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();
    foreach (_; 0 .. count) backend.spawn({});
    auto flushed = backend.flush(token);
    assert(flushed == 32);
    flushed = backend.flush(token);
    assert(flushed == count - 32);
    foreach (_; 0 .. 100_000)
    {
        view.consumeNext();
        if (backend.drained) break;
    }
    assert(backend.drained);
    assert(backend.takeCompletions().length == count);
}

void recycleSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int runs;
    auto task = backend.spawn({ atomicFetchAdd(runs, 1); });
    auto firstHandle = task.handle;
    assert(firstHandle.current && firstHandle.generation != 0);
    auto flushed = backend.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed);
    assert(task.terminated && task.outcome == FiberOutcome.completed);
    auto done = backend.takeCompletions();
    assert(done.length == 1 && done[0] is task);
    FiberOutcome observedOutcome;
    auto observed = firstHandle.tryOutcome(observedOutcome);
    assert(observed);
    assert(observedOutcome == FiberOutcome.completed);
    backend.release(task);
    assert(backend.pooled == 1);

    // The pool hands the same object back with fresh scheduling state.
    auto recycled = backend.spawn({ atomicFetchAdd(runs, 1); });
    auto secondHandle = recycled.handle;
    assert(recycled is task);
    assert(secondHandle.generation != firstHandle.generation);
    assert(secondHandle.diagnosticId != firstHandle.diagnosticId);
    assert(!firstHandle.current && secondHandle.current);
    auto staleCancelled = backend.directorCancel(firstHandle);
    assert(!staleCancelled);
    assert(backend.pooled == 0);
    assert(!recycled.terminated && recycled.outcome == FiberOutcome.pending);
    assert(recycled.exception is null && !recycled.cancellationRequested);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(recycled.terminated && recycled.outcome == FiberOutcome.completed);
    assert(atomicLoad(runs) == 2);
    assert(backend.drained);
    done = backend.takeCompletions();
    assert(done.length == 1 && done[0] is recycled);
    backend.release(recycled);

    // A valid newer cancellation is a generation tag, not a resettable bool.
    // A stale request cannot overwrite/clear it after another reuse.
    auto cancelledTask = backend.spawn({ atomicFetchAdd(runs, 1); });
    auto thirdHandle = cancelledTask.handle;
    auto currentCancelled = backend.directorCancel(thirdHandle);
    staleCancelled = backend.directorCancel(secondHandle);
    assert(currentCancelled);
    assert(!staleCancelled);
    assert(cancelledTask.cancellationRequested);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(cancelledTask.outcome == FiberOutcome.cancelled);
    assert(atomicLoad(runs) == 2); // cancellation won before user-body entry
    done = backend.takeCompletions();
    assert(done.length == 1 && done[0] is cancelledTask);
}

void externalJoinSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto task = backend.spawn({});
    auto handle = task.handle;
    auto pending = handle.poll();
    assert(pending.status == FiberJoinStatus.pending);
    auto timed = handle.join(msecs(1));
    assert(timed.status == FiberJoinStatus.timedOut);

    shared int joinStarted;
    shared int joinFinished;
    shared int joinedStatus;
    auto joiner = new Thread({
        atomicStore(joinStarted, 1);
        auto joined = handle.join();
        atomicStore(joinedStatus, cast(int) joined.status);
        atomicStore(joinFinished, 1);
    });
    joiner.start();
    while (atomicLoad(joinStarted) == 0) Thread.yield();
    Thread.sleep(msecs(2));
    assert(atomicLoad(joinFinished) == 0);

    auto flushed = backend.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed && backend.drained);
    auto completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is task);
    // release closes old-generation waiter admission and cannot reset the
    // manual-reset event until an already registered joiner has awakened.
    backend.release(task);
    joiner.join();
    assert(atomicLoad(joinFinished) == 1);
    assert(atomicLoad(joinedStatus) == FiberJoinStatus.completed);

    TaskHandle selfHandle;
    shared int managedJoinStatus;
    auto recycled = backend.spawn({
        atomicStore(managedJoinStatus, cast(int) selfHandle.join().status);
    });
    selfHandle = recycled.handle;
    assert(recycled is task);
    auto stalePoll = handle.poll();
    auto staleJoin = handle.join();
    assert(stalePoll.status == FiberJoinStatus.staleHandle);
    assert(staleJoin.status == FiberJoinStatus.staleHandle);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && backend.drained);
    completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is recycled);
    assert(atomicLoad(managedJoinStatus) == FiberJoinStatus.wouldBlock);
    auto joined = recycled.handle.join();
    assert(joined.terminal && joined.status == FiberJoinStatus.completed);
    assert(joined.outcome == FiberOutcome.completed && joined.failure is null);
    backend.release(recycled);
}

void fiberJoinSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    FiberJoinResult joined;
    auto target = backend.spawn({ FiberBackend.await(0x4A01); });
    auto targetHandle = target.handle;
    auto joiner = backend.spawn({ joined = targetHandle.joinFiber(); });
    auto flushed = backend.flush(token);
    assert(flushed == 2);
    auto consumed = view.consumeQuantum();
    assert(consumed);
    consumed = view.consumeQuantum();
    assert(backend.waiting == 2 && !joiner.terminated);

    auto signalled = backend.signal(0x4A01);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && target.terminated && backend.ready == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && joiner.terminated && backend.drained);
    assert(joined.status == FiberJoinStatus.completed && joined.terminal);
    auto completed = backend.takeCompletions();
    assert(completed.length == 2);
    backend.releaseAll(completed);

    FiberJoinResult timedJoin;
    auto timedTarget = backend.spawn({ FiberBackend.await(0x4A02); });
    auto timedHandle = timedTarget.handle;
    auto timedJoiner = backend.spawn({
        timedJoin = timedHandle.joinFiber(msecs(1));
    });
    flushed = backend.flush(token);
    assert(flushed == 2);
    consumed = view.consumeQuantum();
    assert(consumed);
    consumed = view.consumeQuantum();
    assert(backend.waiting == 2 && !timedJoiner.terminated);
    Thread.sleep(msecs(2));
    auto expired = backend.pollTimers();
    assert(expired == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && timedJoiner.terminated);
    assert(timedJoin.status == FiberJoinStatus.timedOut);
    signalled = backend.signal(0x4A02);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && timedTarget.terminated && backend.drained);
    completed = backend.takeCompletions();
    assert(completed.length == 2);
    backend.releaseAll(completed);

    FiberJoinResult timerCancelledJoin;
    auto earlyTarget = backend.spawn({ FiberBackend.await(0x4A04); });
    auto earlyHandle = earlyTarget.handle;
    auto earlyJoiner = backend.spawn({
        timerCancelledJoin = earlyHandle.joinFiber(msecs(100));
    });
    flushed = backend.flush(token);
    assert(flushed == 2);
    consumed = view.consumeQuantum();
    assert(consumed);
    consumed = view.consumeQuantum();
    assert(backend.waiting == 2 && !earlyJoiner.terminated);
    signalled = backend.signal(0x4A04);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && earlyTarget.terminated && backend.ready == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && earlyJoiner.terminated && backend.drained);
    assert(timerCancelledJoin.status == FiberJoinStatus.completed);
    long unusedDeadline;
    auto timerRemains = backend.nextTimerDeadline(unusedDeadline);
    assert(!timerRemains);
    completed = backend.takeCompletions();
    assert(completed.length == 2);
    backend.releaseAll(completed);

    shared int joinCleanup;
    auto cancelTarget = backend.spawn({ FiberBackend.await(0x4A03); });
    auto cancelHandle = cancelTarget.handle;
    auto cancelledJoiner = backend.spawn({
        scope (exit) atomicFetchAdd(joinCleanup, 1);
        cast(void) cancelHandle.joinFiber();
    });
    flushed = backend.flush(token);
    assert(flushed == 2);
    consumed = view.consumeQuantum();
    assert(consumed);
    consumed = view.consumeQuantum();
    assert(backend.waiting == 2 && !cancelledJoiner.terminated);
    auto cancellation = backend.directorCancelDetailed(cancelledJoiner.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && cancelledJoiner.terminated);
    assert(cancelledJoiner.outcome == FiberOutcome.cancelled);
    assert(atomicLoad(joinCleanup) == 1);
    signalled = backend.signal(0x4A03);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && cancelTarget.terminated && backend.drained);
    completed = backend.takeCompletions();
    assert(completed.length == 2);
    backend.releaseAll(completed);

    TaskHandle selfHandle;
    FiberJoinStatus selfStatus;
    auto self = backend.spawn({ selfStatus = selfHandle.joinFiber().status; });
    selfHandle = self.handle;
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && self.terminated && backend.drained);
    assert(selfStatus == FiberJoinStatus.wouldBlock);
    completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is self);
    backend.release(self);

    auto otherBackend = new FiberBackend(farm);
    auto foreignTarget = otherBackend.spawn({});
    auto foreignHandle = foreignTarget.handle;
    FiberJoinStatus foreignStatus;
    auto foreignProbe = backend.spawn({
        foreignStatus = foreignHandle.joinFiber().status;
    });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && foreignProbe.terminated && backend.drained);
    assert(foreignStatus == FiberJoinStatus.wrongBackend);
    completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is foreignProbe);
    backend.release(foreignProbe);

    cancellation = otherBackend.directorCancelDetailed(foreignHandle);
    assert(cancellation == CancellationResult.wonBeforeEntry);
    flushed = otherBackend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && foreignTarget.terminated && otherBackend.drained);
    completed = otherBackend.takeCompletions();
    assert(completed.length == 1 && completed[0] is foreignTarget);
    otherBackend.release(foreignTarget);
}

void cancellationSemanticsSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int entered;
    auto preEntry = backend.spawn({ atomicFetchAdd(entered, 1); });
    auto cancellation = backend.directorCancelDetailed(preEntry.handle);
    assert(cancellation == CancellationResult.wonBeforeEntry);
    auto flushed = backend.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed);
    assert(preEntry.outcome == FiberOutcome.cancelled);
    assert(preEntry.cancellationDisposition
        == CancellationDisposition.acknowledged);
    assert(atomicLoad(entered) == 0);
    auto done = backend.takeCompletions();
    assert(done.length == 1);
    cancellation = backend.directorCancelDetailed(preEntry.handle);
    assert(cancellation == CancellationResult.alreadyTerminal);
    backend.release(preEntry);
    // A HOLD Fiber rejected before entry is replaced safely on pool reuse.
    auto replacement = backend.spawn({ atomicFetchAdd(entered, 1); });
    assert(replacement is preEntry);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(replacement.outcome == FiberOutcome.completed);
    assert(replacement.cancellationDisposition == CancellationDisposition.none);
    assert(atomicLoad(entered) == 1);
    done = backend.takeCompletions();
    backend.release(replacement);

    auto ready = backend.spawn({ FiberBackend.yieldReady(); });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    cancellation = backend.directorCancelDetailed(ready.handle);
    assert(cancellation == CancellationResult.requestedReady);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(ready.outcome == FiberOutcome.cancelled);
    assert(ready.cancellationDisposition
        == CancellationDisposition.acknowledged);
    done = backend.takeCompletions();
    backend.release(ready);

    shared int runningEntered;
    auto running = backend.spawn({
        atomicStore!(MemoryOrder.rel)(runningEntered, 1);
        while (!FiberBackend.currentCancellationRequested()) Thread.yield();
    });
    flushed = backend.flush(token);
    assert(flushed == 1);
    Throwable consumerFailure;
    auto consumer = new Thread({
        try
        {
            ConsumerView runningView;
            auto runningSubscription = subscribeOrThrow(runningView, farm);
            scope (exit) runningView.unsubscribe();
            while (!running.terminated)
            {
                if (!runningView.consumeNext()) Thread.yield();
            }
        }
        catch (Throwable failure)
            consumerFailure = failure;
    });
    consumer.start();
    while (atomicLoad!(MemoryOrder.acq)(runningEntered) == 0) Thread.yield();
    cancellation = backend.directorCancelDetailed(running.handle);
    assert(cancellation == CancellationResult.requestedRunning);
    consumer.join();
    if (consumerFailure !is null) throw consumerFailure;
    assert(running.outcome == FiberOutcome.cancelled);
    assert(running.cancellationDisposition
        == CancellationDisposition.resultAbrogated);
    done = backend.takeCompletions();
    backend.release(running);
    while (view.consumeNext()) {}

    auto cleanupFailure = backend.spawn({
        scope (failure) throw new Exception("cleanup failure");
        FiberBackend.await(0xCA11);
    });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    cancellation = backend.directorCancelDetailed(cleanupFailure.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(cleanupFailure.cancellationRequested);
    assert(cleanupFailure.outcome == FiberOutcome.failed);
    assert(cleanupFailure.cancellationDisposition
        == CancellationDisposition.cleanupFailed);
    assert(cleanupFailure.exception !is null
        && cleanupFailure.exception.msg == "cleanup failure");
    done = backend.takeCompletions();
    assert(done.length == 1 && done[0] is cleanupFailure);
}

void lifecycleEventSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    backend.enableLifecycleEvents();
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto completedTask = backend.spawn({});
    auto events = backend.takeLifecycleEvents();
    assert(events.length == 1);
    assert(events[0].kind == FiberLifecycleKind.admitted);
    assert(events[0].task == completedTask.handle);
    immutable firstSequence = events[0].sequence;
    auto flushed = backend.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed);
    auto completions = backend.takeCompletions();
    assert(completions.length == 1);
    // The terminal event retains the scheduler root after completion drain.
    assert(backend.pendingEvents == 1);
    events = backend.takeLifecycleEvents();
    assert(events.length == 1);
    assert(events[0].kind == FiberLifecycleKind.terminal);
    assert(events[0].outcome == FiberOutcome.completed);
    assert(events[0].sequence == firstSequence + 1);
    backend.release(completedTask);

    auto failed = backend.spawn({
        scope (failure) throw new Exception("event cleanup failure");
        FiberBackend.await(0xE7E7);
    });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    auto cancellation = backend.directorCancelDetailed(failed.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    assert(failed.outcome == FiberOutcome.failed);
    completions = backend.takeCompletions();
    assert(completions.length == 1 && completions[0] is failed);
    events = backend.takeLifecycleEvents();
    assert(events.length == 4);
    assert(events[0].kind == FiberLifecycleKind.admitted);
    assert(events[1].kind == FiberLifecycleKind.cancellation);
    assert(events[1].cancellation == CancellationResult.requestedWaiting);
    assert(events[1].cancellationDisposition
        == CancellationDisposition.pending);
    assert(events[2].kind == FiberLifecycleKind.terminal);
    assert(events[2].outcome == FiberOutcome.failed);
    assert(events[2].cancellationDisposition
        == CancellationDisposition.cleanupFailed);
    assert(events[3].kind == FiberLifecycleKind.failure);
    assert(events[3].failure !is null
        && events[3].failure.msg == "event cleanup failure");
    assert(events[3].cancellationDisposition
        == CancellationDisposition.cleanupFailed);
    foreach (i; 1 .. events.length)
        assert(events[i].sequence == events[i - 1].sequence + 1);
    auto retainedFailure = events[3].failure;
    auto oldHandle = events[3].task;
    backend.release(failed);

    auto reused = backend.spawn({});
    assert(reused is failed && !oldHandle.current);
    assert(retainedFailure.msg == "event cleanup failure");
    // Drain the reused generation fully to leave no explicit backend root.
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed);
    completions = backend.takeCompletions();
    events = backend.takeLifecycleEvents();
    assert(completions.length == 1 && events.length == 2);
    backend.release(reused);

    auto fatalTask = backend.spawn({ throw new Error("fatal event"); });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed); // Error never unwinds through the Farm callback
    assert(backend.fatal);
    assert(backend.fatalException !is null
        && backend.fatalException.msg == "fatal event");
    bool admissionRejected;
    try backend.spawn({});
    catch (Exception) admissionRejected = true;
    assert(admissionRejected);
    completions = backend.takeCompletions();
    events = backend.takeLifecycleEvents();
    assert(completions.length == 1 && completions[0] is fatalTask);
    assert(events.length == 3);
    assert(events[0].kind == FiberLifecycleKind.admitted);
    assert(events[1].kind == FiberLifecycleKind.terminal);
    assert(events[2].kind == FiberLifecycleKind.failure);
    assert(cast(Error) events[2].failure !is null);
    assert(backend.lifecycleReserved == 0);
    backend.release(fatalTask);
}

void lifecycleRetentionSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    bool invalidLimit;
    try backend.enableLifecycleEvents(3);
    catch (Exception) invalidLimit = true;
    assert(invalidLimit);
    backend.enableLifecycleEvents(8);
    assert(backend.lifecycleRetentionLimit == 8);
    bool reconfigurationRejected;
    try backend.enableLifecycleEvents(12);
    catch (Exception) reconfigurationRejected = true;
    assert(reconfigurationRejected);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto first = backend.spawn({});
    auto second = backend.spawn({});
    assert(backend.lifecycleReserved == 8 && backend.pendingEvents == 2);
    bool rejected;
    try backend.spawn({});
    catch (FiberLifecycleBackpressure failure)
    {
        rejected = true;
        assert(failure.limit == 8 && failure.reserved == 8);
    }
    assert(rejected);

    auto flushed = backend.flush(token, 1);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed && first.terminated);
    assert(backend.lifecycleReserved == 6 && backend.pendingEvents == 3);

    auto one = backend.takeLifecycleEvents(1);
    assert(one.length == 1 && one[0].kind == FiberLifecycleKind.admitted);
    assert(backend.lifecycleReserved == 5 && backend.pendingEvents == 2);
    rejected = false;
    try backend.spawn({});
    catch (FiberLifecycleBackpressure) rejected = true;
    assert(rejected);

    auto two = backend.takeLifecycleEvents(1);
    assert(two.length == 1 && two[0].kind == FiberLifecycleKind.admitted);
    assert(two[0].sequence == one[0].sequence + 1);
    assert(backend.lifecycleReserved == 4 && backend.pendingEvents == 1);
    auto third = backend.spawn({});
    assert(backend.lifecycleReserved == 8);

    flushed = backend.flush(token);
    assert(flushed == 2);
    consumed = view.consumeQuantum();
    assert(consumed);
    consumed = view.consumeQuantum();
    assert(second.terminated && third.terminated && backend.drained);
    assert(backend.lifecycleReserved == 4);
    FiberLifecycleEvent[] remainder;
    while (backend.pendingEvents != 0)
        remainder ~= backend.takeLifecycleEvents(2);
    assert(remainder.length == 4 && backend.lifecycleReserved == 0);
    foreach (i; 1 .. remainder.length)
        assert(remainder[i].sequence == remainder[i - 1].sequence + 1);
    auto completed = backend.takeCompletions();
    assert(completed.length == 3);
    backend.releaseAll(completed);

    auto handlerBackend = new FiberBackend(farm);
    handlerBackend.enableLifecycleEvents(8);
    auto handledTask = handlerBackend.spawn({});
    flushed = handlerBackend.flush(token);
    assert(flushed == 1);
    consumed = view.consumeQuantum();
    assert(consumed && handledTask.terminated && handlerBackend.drained);
    completed = handlerBackend.takeCompletions();
    assert(completed.length == 1 && completed[0] is handledTask);
    assert(handlerBackend.pendingEvents == 2
        && handlerBackend.lifecycleReserved == 2);

    size_t calls;
    ulong failedSequence;
    FiberLifecycleKind failedKind;
    FiberLifecycleHandler failing =
        (ref const FiberLifecycleEvent event)
        {
            ++calls;
            failedSequence = event.sequence;
            failedKind = event.kind;
            throw new Exception("handler retry");
        };
    bool handlerFailed;
    try handlerBackend.handleLifecycleEvents(failing, 1);
    catch (Exception failure)
    {
        handlerFailed = failure.msg == "handler retry";
    }
    assert(handlerFailed && calls == 1);
    assert(handlerBackend.pendingEvents == 2
        && handlerBackend.lifecycleReserved == 2);

    ulong retriedSequence;
    FiberLifecycleKind retriedKind;
    FiberLifecycleHandler retry =
        (ref const FiberLifecycleEvent event)
        {
            ++calls;
            retriedSequence = event.sequence;
            retriedKind = event.kind;
        };
    auto handled = handlerBackend.handleLifecycleEvents(retry, 1);
    assert(handled == 1 && calls == 2);
    assert(retriedSequence == failedSequence && retriedKind == failedKind);
    assert(handlerBackend.pendingEvents == 1
        && handlerBackend.lifecycleReserved == 1);
    auto terminal = handlerBackend.takeLifecycleEvents(1);
    assert(terminal.length == 1
        && terminal[0].kind == FiberLifecycleKind.terminal);
    assert(terminal[0].sequence == retriedSequence + 1);
    assert(handlerBackend.pendingEvents == 0
        && handlerBackend.lifecycleReserved == 0);
    handlerBackend.release(handledTask);
}

void sharedDomainLaneSmoke()
{
    auto farmA = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    auto domain = new FiberDomain;
    auto laneA = new FiberLane(domain, farmA);
    auto laneB = new FiberLane(domain, farmB);
    assert(domain.laneCount == 2);
    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
    scope (exit)
    {
        farmA.unregisterProducer(tokenA);
        farmB.unregisterProducer(tokenB);
    }
    ConsumerView viewA;
    ConsumerView viewB;
    auto subscriptionA = subscribeOrThrow(viewA, farmA);
    auto subscriptionB = subscribeOrThrow(viewB, farmB);
    scope (exit)
    {
        viewA.unsubscribe();
        viewB.unsubscribe();
    }

    FiberJoinResult joined;
    auto target = laneA.spawn({ FiberDomain.await(0xD04A); });
    auto joiner = laneB.spawn({ joined = target.handle.joinFiber(); });
    assert(laneA.ready == 1 && laneB.ready == 1 && domain.ready == 2);
    auto wakeA = laneA.takeWakeEvents();
    auto wakeB = laneB.takeWakeEvents();
    assert(wakeA == FiberWakeEvent.spawned);
    assert(wakeB == FiberWakeEvent.spawned);

    auto flushed = laneA.flush(tokenA);
    assert(flushed == 1);
    auto consumed = viewA.consumeQuantum();
    assert(consumed);
    assert(domain.waiting == 1 && laneA.ready == 0 && laneB.ready == 1);
    flushed = laneB.flush(tokenB);
    assert(flushed == 1);
    consumed = viewB.consumeQuantum();
    assert(consumed);
    assert(domain.waiting == 2 && laneA.ready == 0 && laneB.ready == 0);

    auto signalled = domain.signal(0xD04A);
    assert(signalled == 1);
    assert(laneA.ready == 1 && laneB.ready == 0);
    wakeA = laneA.takeWakeEvents();
    wakeB = laneB.takeWakeEvents();
    assert(wakeA == FiberWakeEvent.signalled);
    assert(wakeB == FiberWakeEvent.none);
    flushed = laneA.flush(tokenA);
    assert(flushed == 1);
    consumed = viewA.consumeQuantum();
    assert(consumed);
    assert(target.terminated && laneA.ready == 0 && laneB.ready == 1);
    wakeB = laneB.takeWakeEvents();
    assert(wakeB == FiberWakeEvent.signalled);
    flushed = laneB.flush(tokenB);
    assert(flushed == 1);
    consumed = viewB.consumeQuantum();
    assert(consumed);
    assert(joiner.terminated && domain.drained);
    assert(joined.status == FiberJoinStatus.completed
        && joined.task == target.handle);

    auto completed = domain.takeCompletions();
    assert(completed.length == 2);
    domain.releaseAll(completed);
}

void remoteCoverageRingSmoke()
{
    auto farmA = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    auto farmC = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmC.destroy();
    auto domain = new FiberDomain;
    auto laneA = new FiberLane(domain, farmA);
    auto laneB = new FiberLane(domain, farmB);
    auto privateLane = new FiberLane(farmC);
    installRemoteCoverageRing([laneA, laneB, privateLane]);
    assert(laneA.coveredLanes.length == 1 && laneA.coveredLanes[0] is laneB);
    assert(laneB.coveredLanes.length == 1 && laneB.coveredLanes[0] is laneA);
    assert(privateLane.coveredLanes.length == 0);
    installRemoteCoverageRing([laneA, laneB, privateLane]);
    assert(laneA.coveredLanes.length == 1);
}

void remoteSweeperSmoke()
{
    auto farmA = AntFarm.create(1 << 18, 8, 1, 0, 0, 2, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(1 << 18, 8, 1, 0, 0, 2, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    auto domain = new FiberDomain;
    auto laneA = new FiberLane(domain, farmA);
    auto laneB = new FiberLane(domain, farmB);
    laneA.cover(laneB);
    assert(laneA.coveredLanes[0] is laneB);
    assert(laneB.coveringLanes[0] is laneA);

    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
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

    laneA.takeWakeEvents();
    laneB.takeWakeEvents();
    shared uint ran;
    enum count = 32;
    FiberTask[] tasks;
    foreach (_; 0 .. count)
        tasks ~= laneB.spawn({
            FiberDomain.yieldReady();
            atomicFetchAdd(ran, 1u);
        });
    auto wakeB = laneB.takeWakeEvents();
    auto wakeA = laneA.takeWakeEvents();
    assert(wakeB == FiberWakeEvent.spawned);
    assert((cast(uint) wakeA & cast(uint) FiberWakeEvent.remoteReady) != 0);

    // Native B is disabled: only A's home consumer plus its remote view of B
    // may drain B's activations.
    foreach (_; 0 .. 200_000)
    {
        home.consumeNext();
        if (laneA.ready != 0) laneA.flush(tokenA);
        remote.consumeNext();
        if (laneB.ready != 0) laneB.flush(tokenB);
        if (domain.drained) break;
        Thread.yield();
    }
    assert(domain.drained);
    assert(atomicLoad(ran) == count);
    assert(laneA.published == 0 && laneB.published == 0);
    foreach (task; tasks)
        assert(task.terminated && task.resumeCount == 2);
    auto completed = domain.takeCompletions();
    assert(completed.length == count);
    domain.releaseAll(completed);
}

/// Consume exactly one newly-published resume from a fresh OS thread. Using a
/// new sole consumer for every step makes migration deterministic rather than
/// relying on the threadpool race to happen to choose another worker.
/// `consumeQuantum` is a test artifact here: the worker path uses `consumeNext`.
void consumeOnFreshThread(AntFarm* farm, FiberTask task, ulong expectedResumes,
                          ref Thread[] retainedThreads)
{
    import core.thread : Thread;

    Throwable failure;
    auto thread = new Thread({
        try
        {
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            scope (exit) view.unsubscribe();
            foreach (_; 0 .. 100_000)
            {
                view.consumeQuantum();
                if (task.resumeCount >= expectedResumes) return;
                Thread.yield();
            }
            assert(false, "fresh consumer did not resume the fiber");
        }
        catch (Throwable t)
            failure = t;
    });
    // currentWorkerIdentity() is the druntime Thread object's address. Keep
    // completed wrappers alive so the GC cannot recycle an address and make
    // two deliberately distinct workers look identical under optimization.
    retainedThreads ~= thread;
    thread.start();
    thread.join();
    if (failure !is null) throw failure;
}

void migrationSmoke()
{
    import core.memory : GC;
    import core.thread : Thread;
    import core.time : msecs;

    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    ulong[5] identities;
    Thread[] retainedThreads;
    int preservedValue;
    auto task = backend.spawn({
        // `box` is deliberately not stored outside the fiber: across each
        // suspension its only root is on the suspended stack.
        auto box = new MigrationBox(41);
        identities[0] = FiberBackend.currentWorkerIdentity();
        FiberBackend.yieldReady();
        assert(box.value++ == 41);
        identities[1] = FiberBackend.currentWorkerIdentity();
        FiberBackend.await(77);
        assert(box.value++ == 42);
        identities[2] = FiberBackend.currentWorkerIdentity();
        FiberBackend.sleepFor(msecs(2));
        assert(box.value++ == 43);
        identities[3] = FiberBackend.currentWorkerIdentity();
        scope (exit)
        {
            preservedValue = box.value;
            identities[4] = FiberBackend.currentWorkerIdentity();
        }
        FiberBackend.await(88); // cancellation wakes and unwinds this wait
    });

    auto flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 1, retainedThreads);
    GC.collect();

    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 2, retainedThreads);
    GC.collect();

    auto signalled = backend.signal(77);
    assert(signalled == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 3, retainedThreads);
    GC.collect();

    Thread.sleep(msecs(4));
    auto expired = backend.pollTimers();
    assert(expired == 1);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 4, retainedThreads);

    auto cancelled = backend.directorCancel(task);
    assert(cancelled);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 5, retainedThreads);

    assert(task.outcome == FiberOutcome.cancelled);
    assert(task.resumeCount == 5 && task.migrationCount == 4);
    assert(preservedValue == 44);
    foreach (i; 0 .. identities.length)
    {
        assert(identities[i] != 0);
        if (i != 0) assert(identities[i] != identities[i - 1]);
    }
    auto completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is task);

    // An exception retained by druntime must cross back from a migrated
    // fiber without escaping the @nogc/nothrow Ant Farm callback.
    auto failed = backend.spawn({
        FiberBackend.yieldReady();
        throw new Exception("failure after migration");
    });
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, failed, 1, retainedThreads);
    flushed = backend.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, failed, 2, retainedThreads);
    assert(failed.migrationCount == 1);
    assert(failed.outcome == FiberOutcome.failed);
    assert(failed.exception !is null
        && failed.exception.msg == "failure after migration");
    completed = backend.takeCompletions();
    assert(completed.length == 1 && completed[0] is failed);
}

void wakePolicySmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto lane = new FiberLane(farm);
    lane.wakePolicy = FiberWakePolicy.directorOwned;
    shared int notifications;
    lane.setWakeNotifier(&wakeNotifier, cast(void*) &notifications);

    auto task = lane.spawn({});
    assert(atomicLoad(notifications) == 1);
    auto spawnEvents = cast(uint) lane.takeWakeEvents();
    assert((spawnEvents & cast(uint) FiberWakeEvent.spawned) != 0);
    auto clearedEvents = lane.takeWakeEvents();
    assert(clearedEvents == FiberWakeEvent.none);

    auto submitted = lane.requestCancel(task);
    assert(submitted == CancellationSubmission.queued);
    auto applications = lane.drainCancellationRequests();
    assert(applications.length == 1
        && applications[0].result == CancellationResult.wonBeforeEntry);
    lane.beginShutdown(true);
    // Cancellation creates one new empty→nonempty edge; shutdown coalesces.
    assert(atomicLoad(notifications) == 2);
    immutable events = cast(uint) lane.takeWakeEvents();
    assert((events & cast(uint) FiberWakeEvent.cancelled) != 0);
    assert((events & cast(uint) FiberWakeEvent.shutdown) != 0);

    // Complete the cancelled activation so the backend GC root is retired
    // before the Farm is destroyed.
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();
    auto flushed = lane.backend.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeQuantum();
    assert(consumed);
    auto completions = lane.backend.takeCompletions();
    assert(completions.length == 1);
}

void mixedPayloadSmoke()
{
    enum count = 96;
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    atomicStore(barePayloadRuns, 0);
    shared int fiberRuns;
    FiberTask[] tasks;
    foreach (_; 0 .. count)
        tasks ~= backend.spawn({
            atomicFetchAdd(fiberRuns, 1);
            FiberBackend.yieldReady();
            atomicFetchAdd(fiberRuns, 1);
        });

    PayloadHeader header;
    header.maxCs = 1;
    header.done = 1;
    header.plen = 1;
    header.call = &barePayloadCallback;
    ulong[count] bodies = 0xA17F_A4FUL;
    PayloadEntry[count] bare;
    foreach (i; 0 .. count)
        bare[i] = PayloadEntry(&header, bodies[i .. i + 1]);

    size_t bareOffset;
    foreach (_; 0 .. 200_000)
    {
        if (bareOffset < bare.length)
            bareOffset += farm.write(bare[bareOffset .. $], token, 1);
        if (backend.ready != 0)
            backend.flush(token, 17, 1);
        view.consumeNext();
        if (bareOffset == count && backend.drained
            && atomicLoad(barePayloadRuns) == count)
            break;
        Thread.yield();
    }
    assert(bareOffset == count);
    assert(atomicLoad(barePayloadRuns) == count);
    assert(atomicLoad(fiberRuns) == count * 2);
    assert(backend.drained);
    auto completed = backend.takeCompletions();
    assert(completed.length == count);
    foreach (task; completed)
        assert(task.outcome == FiberOutcome.completed);
}

void drainUntil(FiberBackend backend, ref Token token, ref ConsumerView view,
                size_t waitFor = 0)
{
    foreach (spin; 0 .. 200_000)
    {
        backend.pollGenerationTriggers();
        backend.flush(token);
        view.consumeNext();
        if (waitFor == 0)
        {
            if (backend.drained) return;
        }
        else if (backend.waiting >= waitFor || backend.drained)
            return;
        Thread.yield();
    }
    assert(false, "fiber drain stalled");
}

void syncPrimitiveSmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto posted = new FiberEvent(backend);
    shared uint ran;
    backend.spawn({
        posted.wait();
        atomicFetchAdd(ran, 1u);
    });
    drainUntil(backend, token, view, 1);
    assert(backend.waiting == 1 && atomicLoad(ran) == 0);
    posted.set();
    drainUntil(backend, token, view);
    assert(atomicLoad(ran) == 1);
    backend.releaseAll(backend.takeCompletions());

    auto generations = new FiberGenerationTrigger(backend);
    shared uint generationsSeen;
    backend.spawn({
        generations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
        generations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
    });
    drainUntil(backend, token, view, 1);
    generations.advance();
    drainUntil(backend, token, view, 1);
    assert(atomicLoad(generationsSeen) == 1);
    generations.advance();
    drainUntil(backend, token, view);
    assert(atomicLoad(generationsSeen) == 2
        && generations.completed == 2);
    backend.releaseAll(backend.takeCompletions());

    // Advances may coalesce before the consumer Fiber begins waiting.
    auto prepostedGenerations = new FiberGenerationTrigger(backend);
    prepostedGenerations.advance();
    prepostedGenerations.advance();
    atomicStore(generationsSeen, 0u);
    backend.spawn({
        prepostedGenerations.waitNext();
        prepostedGenerations.waitNext();
        atomicFetchAdd(generationsSeen, 2u);
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(generationsSeen) == 2
        && prepostedGenerations.completed == 2);
    backend.releaseAll(backend.takeCompletions());

    // Cancellation removes a waiter but retains the trigger's empty bucket;
    // a later serialized consumer can register on that same key again.
    auto cancelledGenerations = new FiberGenerationTrigger(backend);
    auto cancelledWaiter = backend.spawn({ cancelledGenerations.waitNext(); });
    drainUntil(backend, token, view, 1);
    assert(backend.directorCancel(cancelledWaiter));
    drainUntil(backend, token, view);
    assert(cancelledWaiter.outcome == FiberOutcome.cancelled);
    backend.releaseAll(backend.takeCompletions());
    atomicStore(generationsSeen, 0u);
    backend.spawn({
        cancelledGenerations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
    });
    drainUntil(backend, token, view, 1);
    cancelledGenerations.advance();
    drainUntil(backend, token, view);
    assert(atomicLoad(generationsSeen) == 1);
    backend.releaseAll(backend.takeCompletions());

    auto ready = new FiberEvent(backend);
    ready.set();
    atomicStore(ran, 0u);
    backend.spawn({
        ready.wait();
        atomicFetchAdd(ran, 1u);
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(ran) == 1);
    backend.releaseAll(backend.takeCompletions());

    auto gate = new FiberEvent(backend, FiberEventMode.manualReset);
    shared uint manualRan;
    backend.spawn({
        gate.wait();
        atomicFetchAdd(manualRan, 1u);
    });
    backend.spawn({
        gate.wait();
        atomicFetchAdd(manualRan, 1u);
    });
    drainUntil(backend, token, view, 2);
    assert(backend.waiting == 2);
    gate.set();
    drainUntil(backend, token, view);
    assert(atomicLoad(manualRan) == 2);
    backend.releaseAll(backend.takeCompletions());

    auto sem = new FiberSemaphore(backend, 0);
    shared uint semRan;
    backend.spawn({
        sem.wait();
        atomicFetchAdd(semRan, 1u);
    });
    backend.spawn({
        sem.wait();
        atomicFetchAdd(semRan, 1u);
    });
    drainUntil(backend, token, view, 2);
    assert(backend.waiting == 2 && atomicLoad(semRan) == 0);
    sem.post(2);
    drainUntil(backend, token, view);
    assert(atomicLoad(semRan) == 2);
    backend.releaseAll(backend.takeCompletions());

    auto permits = new FiberSemaphore(backend, 2);
    shared uint permitRan;
    backend.spawn({
        permits.wait();
        permits.wait();
        atomicFetchAdd(permitRan, 1u);
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(permitRan) == 1 && permits.available == 0);
    backend.releaseAll(backend.takeCompletions());
}

void sharedSignalAndTimerCancellationSmoke()
{
    import core.time : hours;

    enum count = 64;
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int resumed;
    FiberTask[] tasks;
    foreach (_; 0 .. count)
        tasks ~= backend.spawn({
            FiberBackend.await(1234);
            atomicFetchAdd(resumed, 1);
        });
    auto timer = backend.spawn({ FiberBackend.sleepFor(hours(1)); });

    while (backend.ready != 0)
    {
        backend.flush(token);
        view.consumeNext();
    }
    while (backend.waiting != count + 1)
        view.consumeNext();

    foreach (i; 0 .. count / 2)
    {
        auto cancelled = backend.directorCancel(tasks[i]);
        assert(cancelled);
    }
    auto timerCancellation = backend.directorCancelDetailed(timer.handle);
    assert(timerCancellation == CancellationResult.requestedWaiting);
    auto signalled = backend.signal(1234);
    assert(signalled == count / 2);

    foreach (_; 0 .. 200_000)
    {
        if (backend.ready != 0) backend.flush(token);
        view.consumeNext();
        if (backend.drained) break;
        Thread.yield();
    }
    assert(backend.drained && backend.waiting == 0);
    assert(atomicLoad(resumed) == count / 2);
    long unusedDeadline;
    auto hasDeadline = backend.nextTimerDeadline(unusedDeadline);
    assert(!hasDeadline);
    auto completed = backend.takeCompletions();
    assert(completed.length == count + 1);
    assert(timer.outcome == FiberOutcome.cancelled);
    assert(timer.cancellationDisposition
        == CancellationDisposition.acknowledged);
}

void threadpoolSmoke()
{
    import core.thread : Thread;
    import core.time : MonoTime, msecs;
    import threadpool;

    auto topology = CacheAwarePool.topology();
    AntFarm*[] farms;
    FiberLane[] lanes;
    foreach (_; 0 .. topology.llcCount)
    {
        auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 2, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        farms ~= farm;
        lanes ~= new FiberLane(farm);
    }
    scope (exit)
    {
        uninstallFiberLanes();
        foreach (farm; farms) farm.destroy();
    }
    ushort selectedLp;
    ushort selectedGroup;
    ushort selectedLlc;
    bool found;
    foreach (ref lp; topology.processors)
    {
        version (linux)
            if (lp.parkedAtDiscovery) continue;
        selectedLp = lp.lpIndex;
        selectedGroup = lp.group;
        selectedLlc = lp.llcIndex;
        found = true;
        break;
    }
    assert(found);

    shared int ran;
    shared int observedLlc = -1;
    PoolOptions options;
    options.onlyProcessors = [ProcessorId(selectedGroup, selectedLp)];
    options.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(options);
    installFiberLanes(lanes, pool);
    auto task = lanes[selectedLlc].spawn({
        auto worker = currentWorker();
        assert(worker !is null);
        atomicStore(observedLlc, worker.llcIndex);
        atomicFetchAdd(ran, 1);
    });
    pool.start();
    foreach (_; 0 .. 400)
    {
        if (task.terminated) break;
        Thread.sleep(msecs(5));
    }
    pool.shutdown();
    assert(task.terminated && atomicLoad(ran) == 1);
    assert(atomicLoad(observedLlc) == selectedLlc);
    assert(pool.workerFailures.length == 0);

    shared int enteredFailure;
    // Start idle, then spawn externally: FiberLane must wake the waiting pool.
    auto pool2 = new CacheAwarePool(options);
    foreach (lane; lanes) lane.bindPool(pool2);
    pool2.start();
    Thread.sleep(msecs(10));
    auto failed = lanes[selectedLlc].spawn({
        atomicFetchAdd(enteredFailure, 1);
        throw new Exception("fiber failure");
    });
    foreach (_; 0 .. 400)
    {
        if (failed.terminated) break;
        Thread.sleep(msecs(5));
    }
    pool2.shutdown();
    assert(atomicLoad(enteredFailure) == 1);
    assert(failed.outcome == FiberOutcome.failed);
    assert(failed.exception !is null && failed.exception.msg == "fiber failure");
    auto outcomes = lanes[selectedLlc].backend.takeCompletions();
    assert(outcomes.length >= 1);

    // The managed pump hands its nearest deadline to the default wait policy;
    // no timer thread or cadence polling is involved.
    auto pool3 = new CacheAwarePool(options);
    foreach (lane; lanes) lane.bindPool(pool3);
    pool3.start();
    MonoTime timerStart;
    MonoTime timerEnd;
    auto timed = lanes[selectedLlc].spawn({
        timerStart = MonoTime.currTime;
        FiberBackend.sleepFor(msecs(30));
        timerEnd = MonoTime.currTime;
    });
    foreach (_; 0 .. 400)
    {
        if (timed.terminated) break;
        Thread.sleep(msecs(5));
    }
    pool3.shutdown();
    assert(timed.outcome == FiberOutcome.completed);
    assert(timerEnd - timerStart >= msecs(20));
    auto timedCompletions = lanes[selectedLlc].backend.takeCompletions();
    assert(timedCompletions.length == 1 && timedCompletions[0] is timed);

    // A custom selector which chooses no responder is rejected by the
    // managed-start coverage barrier instead of silently parking live work.
    foreach (lane; lanes)
        lane.responderSelector = (WorkerSelf*) => false;
    auto pool4 = new CacheAwarePool(options);
    installFiberLanes(lanes, pool4);
    pool4.start();
    foreach (_; 0 .. 100_000)
    {
        if (lanes[selectedLlc].coverage != FiberCoverage.pending) break;
        Thread.yield();
    }
    assert(lanes[selectedLlc].managedWorkers == 1);
    assert(lanes[selectedLlc].responders == 0);
    assert(lanes[selectedLlc].coverage == FiberCoverage.noResponder);
    pool4.shutdown();
    assert(pool4.workerFailures.length == 1);
    foreach (lane; lanes) lane.responderSelector = null;

    // Installed lanes outside the pool's selected processors are explicitly
    // unavailable after validation and reject new work.
    foreach (llcIndex, lane; lanes)
    {
        if (llcIndex == selectedLlc) continue;
        assert(lane.coverage == FiberCoverage.noWorkers);
        bool rejected;
        try lane.spawn({});
        catch (Exception) rejected = true;
        assert(rejected);
    }

    // Cancellation removes a long waiter, republishes it, and unwinds the
    // fiber through FiberCancelled without turning it into a worker failure.
    foreach (lane; lanes) lane.bindPool(null);
    auto cancelFarm = farms[selectedLlc];
    auto cancelLane = lanes[selectedLlc];
    ConsumerView cancelView;
    auto cancelSubscription = subscribeOrThrow(cancelView, cancelFarm);
    auto cancelToken = cancelFarm.registerProducer(Tier.small);
    while (cancelView.consumeNext()) {}
    bool cleaned;
    auto cancelled = cancelLane.backend.spawn({
        scope (exit) cleaned = true;
        FiberBackend.await(999);
    });
    auto cancelFlushed = cancelLane.backend.flush(cancelToken);
    assert(cancelFlushed == 1);
    auto cancelConsumed = cancelView.consumeQuantum();
    assert(cancelConsumed);
    assert(cancelLane.backend.waiting == 1);
    cancelLane.beginShutdown(true);
    cancelFlushed = cancelLane.backend.flush(cancelToken);
    assert(cancelFlushed == 1);
    cancelConsumed = cancelView.consumeQuantum();
    assert(cancelConsumed);
    assert(cancelled.outcome == FiberOutcome.cancelled && cleaned);
    assert(cancelLane.drained);
    auto cancelledCompletions = cancelLane.backend.takeCompletions();
    assert(cancelledCompletions.length == 1);
    cancelView.unsubscribe();
    cancelFarm.unregisterProducer(cancelToken);

    // Fatal Error is retained in the Fiber callback, then rethrown only from
    // the managed pump so threadpool records the failure and requests stop.
    auto fatalLane = new FiberLane(cancelFarm);
    fatalLane.enableLifecycleEvents();
    lanes[selectedLlc] = fatalLane;
    auto pool5 = new CacheAwarePool(options);
    installFiberLanes(lanes, pool5);
    pool5.start();
    auto fatalTask = fatalLane.spawn({ throw new Error("managed fatal"); });
    foreach (_; 0 .. 400)
    {
        if (fatalTask.terminated) break;
        Thread.sleep(msecs(5));
    }
    pool5.shutdown();
    assert(fatalTask.terminated && fatalLane.backend.fatal);
    assert(pool5.workerFailures.length == 1);
    assert(cast(Error) pool5.workerFailures[0] !is null
        && pool5.workerFailures[0].msg == "managed fatal");
    auto fatalCompletions = fatalLane.backend.takeCompletions();
    auto fatalEvents = fatalLane.takeLifecycleEvents();
    assert(fatalCompletions.length == 1 && fatalEvents.length == 3);
    fatalLane.backend.release(fatalTask);
    foreach (farm; farms)
        assert(atomicLoad(farm.Cf) == 0, "fiber lane leaked a consumer");
}

void fiberParitySmoke()
{
    auto farm = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto backend = new FiberBackend(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int cleaned;
    backend.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(cleaned) == 1);
    backend.releaseAll(backend.takeCompletions());

    backend.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        FiberDomain.yieldReady();
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(cleaned) == 2);
    backend.releaseAll(backend.takeCompletions());

    auto waiting = backend.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        FiberDomain.await(0xCE);
    });
    drainUntil(backend, token, view, 1);
    assert(backend.waiting == 1);
    backend.directorCancel(waiting);
    drainUntil(backend, token, view);
    assert(atomicLoad(cleaned) == 3);
    assert(waiting.outcome == FiberOutcome.cancelled);
    backend.releaseAll(backend.takeCompletions());

    auto failed = backend.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        throw new Exception("parity failure");
    });
    drainUntil(backend, token, view);
    assert(atomicLoad(cleaned) == 4);
    assert(failed.outcome == FiberOutcome.failed
        && failed.exception !is null
        && failed.exception.msg == "parity failure");
    backend.releaseAll(backend.takeCompletions());

    auto nested = backend.spawn({
        FiberDomain.yieldReady();
        FiberDomain.await(0xAE);
        FiberDomain.sleepFor(msecs(1));
    });
    drainUntil(backend, token, view, 1);
    assert(backend.waiting == 1);
    backend.signal(0xAE);
    foreach (_; 0 .. 200_000)
    {
        backend.flush(token);
        backend.pollTimers();
        view.consumeNext();
        if (nested.terminated) break;
        Thread.yield();
    }
    assert(nested.outcome == FiberOutcome.completed);
    backend.releaseAll(backend.takeCompletions());

    auto first = backend.spawn({});
    drainUntil(backend, token, view);
    backend.releaseAll(backend.takeCompletions());
    shared int recycledRuns;
    auto recycled = backend.spawn({
        FiberDomain.yieldReady();
        atomicFetchAdd(recycledRuns, 1);
    });
    assert(recycled is first);
    drainUntil(backend, token, view);
    assert(atomicLoad(recycledRuns) == 1);
    backend.releaseAll(backend.takeCompletions());

    auto raw = backend.spawn({ Fiber.yield(); });
    drainUntil(backend, token, view);
    assert(raw.outcome == FiberOutcome.failed);
    assert(raw.exception !is null
        && raw.exception.msg == "antfarm_fibers: Fiber.yield without scheduler suspend");
    backend.releaseAll(backend.takeCompletions());
}

void main()
{
    uxHelpersSmoke();
    cancellationOwnershipSmoke();
    manualSmoke();
    fiberParitySmoke();
    defaultBatchSmoke();
    recycleSmoke();
    externalJoinSmoke();
    fiberJoinSmoke();
    cancellationSemanticsSmoke();
    lifecycleEventSmoke();
    lifecycleRetentionSmoke();
    sharedDomainLaneSmoke();
    remoteCoverageRingSmoke();
    remoteSweeperSmoke();
    migrationSmoke();
    wakePolicySmoke();
    mixedPayloadSmoke();
    syncPrimitiveSmoke();
    sharedSignalAndTimerCancellationSmoke();
    threadpoolSmoke();
}
