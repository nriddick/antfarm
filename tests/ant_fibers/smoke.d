module smoke;

// This executable has a substantial main() suite as well as imported unittests.
// Under TSan, disable druntime's parallel GC marking: its mark-stack growth
// races intermittently and is reported before the smoke suite runs.
version (TSan)
    extern(C) __gshared string[] rt_options = ["testmode=run-main", "gcopt=parallel:0"];
else
    extern(C) __gshared string[] rt_options = ["testmode=run-main"];

import ant_fibers;
import antfarm;
import core.atomic;
import core.stdc.stdio : printf;
import host_attach_tests;
import ant_fibers.semaphore_tests : semaphoreCancellationSmoke;
import ant_fibers.event_tests : eventCancellationSmoke;
import ant_fibers.wait_race_tests : waitRaceSmoke;
import ant_fibers.record_tests : recordSmoke;
import ant_fibers.group_host_tests : groupHostFiberSmoke;
import threadpool.bins : home;
import threadpool.groups : GroupHost, GroupHostOptions, GroupSlot;
import threadpool.host : HostGroups;
import core.thread : Fiber, Thread;
import core.time : Duration, msecs;

shared int steps;
shared int barePayloadRuns;
shared int functionSpawnRuns;

void functionSpawnBody()
{
    atomicFetchAdd(functionSpawnRuns, 1);
    FiberDomain.yieldReady();
    atomicFetchAdd(functionSpawnRuns, 1);
}

extern(C) void barePayloadCallback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    assert(body.length == 1 && body[0] == 0xA17F_A4FUL);
    atomicFetchAdd(barePayloadRuns, 1);
    return;
}

final class MigrationBox
{
    int value;
    this(int value) { this.value = value; }
}

/// What `onCompletion` saw for one retired task. The handler copies what a
/// test inspects while the task is still valid; `retire` recycles the task
/// when the handler returns, so `task` is kept for identity comparison only.
struct Retired
{
    FiberTask task;
    TaskHandle handle;
    FiberOutcome outcome;
    Throwable exception;
    CancellationDisposition cancellationDisposition;
}

/// Collects the tasks and lifecycle records a domain retires, in order.
final class RetireLog
{
    Retired[] completions;
    FiberLifecycleEvent[] records;

    FiberDomainOptions options(size_t recordRetention = defaultRecordRetention)
    {
        FiberDomainOptions result;
        result.recordRetention = recordRetention;
        result.onCompletion = (FiberTask task) {
            completions ~= Retired(task, task.handle, task.outcome,
                task.exception, task.cancellationDisposition);
        };
        result.onRecord = (FiberLifecycleEvent event) { records ~= event; };
        return result;
    }

    /// `domain.retire(maximum)`, returning the completions it delivered.
    Retired[] retire(FiberDomain domain, size_t maximum = size_t.max)
    {
        immutable recycled = domain.retire(maximum);
        auto taken = takeCompleted();
        assert(taken.length == recycled);
        return taken;
    }

    Retired[] takeCompleted()
    {
        auto taken = completions;
        completions = null;
        return taken;
    }

    FiberLifecycleEvent[] takeRecords()
    {
        auto taken = records;
        records = null;
        return taken;
    }
}

// The function-pointer spawn form and `drainUntilEmpty` on a one-lane domain:
// the drain retires on the Director, and the next spawn reuses the pooled task
// under the default and a wider flush batch.
void uxHelpersSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    assert(domain.laneCount == 1);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    atomicStore(functionSpawnRuns, 0);
    auto first = domain.spawn(&functionSpawnBody);
    // drainUntilEmpty retires on the Director: `first` is recycled by the
    // time it returns and is used below for identity only.
    drainUntilEmpty(domain, token, view);
    assert(atomicLoad(functionSpawnRuns) == 2);
    assert(!domain.retirementPending && domain.pooled == 1);

    auto recycled = domain.spawn(&functionSpawnBody);
    assert(recycled is first);
    drainUntilEmpty(domain, token, view, 256, avgCostForChunk(32));
    assert(atomicLoad(functionSpawnRuns) == 4);
    assert(!domain.retirementPending && domain.pooled == 1);
}

void cancellationOwnershipSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    // Under the default `director` retirement only the constructing thread
    // may retire.
    shared int nonDirectorRejected;
    auto intruder = new Thread({
        try domain.retire();
        catch (Exception)
            atomicStore(nonDirectorRejected, 1);
    });
    intruder.start();
    intruder.join();
    assert(atomicLoad(nonDirectorRejected) == 1);

    size_t cancellationRecords()
    {
        size_t n;
        foreach (record; log.takeRecords())
            if (record.kind == FiberLifecycleKind.cancellation) ++n;
        return n;
    }

    // drainUntilEmpty retires: results come from the completion log, and
    // handles are captured before the drain recycles their tasks.
    CancellationResult localResult;
    auto local = domain.spawn({
        localResult = FiberDomain.cancelCurrent();
        FiberDomain.yieldReady();
    });
    drainUntilEmpty(domain, token, view);
    assert(localResult == CancellationResult.requestedRunning);
    auto done = log.takeCompleted();
    assert(done.length == 1 && done[0].task is local);
    assert(done[0].outcome == FiberOutcome.cancelled
        && done[0].cancellationDisposition == CancellationDisposition.acknowledged);
    assert(cancellationRecords() == 1);

    auto remote = domain.spawn({});
    auto remoteHandle = remote.handle;
    assert(domain.requestCancel(remoteHandle) == CancellationSubmission.queued);
    assert(domain.requestCancel(remoteHandle) == CancellationSubmission.queued);
    auto applications = domain.drainCancellationRequests();
    assert(applications.length == 2);
    assert(applications[0].result == CancellationResult.wonBeforeEntry);
    assert(applications[1].result == CancellationResult.alreadyRequested);
    drainUntilEmpty(domain, token, view);
    done = log.takeCompleted();
    assert(done.length == 1 && done[0].handle == remoteHandle
        && done[0].outcome == FiberOutcome.cancelled);
    assert(cancellationRecords() == 1);

    // Two worker-style drainers and the Director can contend for one batch.
    // Queue more duplicates than the 8-bit CANCEL field can represent: only
    // one external application may win, with all later requests coalescing.
    auto concurrent = domain.spawn({});
    foreach (_; 0 .. 512)
        assert(domain.requestCancel(concurrent.handle)
            == CancellationSubmission.queued);
    shared int go;
    CancellationApplication[] left, right;
    auto firstDrainer = new Thread({
        while (atomicLoad(go) == 0) Thread.yield();
        left = domain.drainCancellationRequests();
    });
    auto secondDrainer = new Thread({
        while (atomicLoad(go) == 0) Thread.yield();
        right = domain.drainCancellationRequests();
    });
    firstDrainer.start();
    secondDrainer.start();
    atomicStore(go, 1);
    auto direct = domain.directorCancelDetailed(concurrent.handle);
    firstDrainer.join();
    secondDrainer.join();
    assert(left.length + right.length == 512);
    size_t winners = direct == CancellationResult.wonBeforeEntry ? 1 : 0;
    assert(direct == CancellationResult.wonBeforeEntry
        || direct == CancellationResult.alreadyRequested);
    foreach (batch; [left, right])
        foreach (application; batch)
        {
            assert(application.task == concurrent.handle);
            if (application.result == CancellationResult.wonBeforeEntry)
                ++winners;
            else assert(application.result == CancellationResult.alreadyRequested);
        }
    assert(winners == 1 && domain.pendingCancellationRequests == 0);
    drainUntilEmpty(domain, token, view);
    done = log.takeCompleted();
    assert(done.length == 1 && done[0].task is concurrent
        && done[0].outcome == FiberOutcome.cancelled);
    assert(cancellationRecords() == 1);

    auto replacement = domain.spawn({});
    assert(replacement is remote);
    assert(domain.requestCancel(remoteHandle)
        == CancellationSubmission.staleHandle);
    drainUntilEmpty(domain, token, view);
    done = log.takeCompleted();
    assert(done.length == 1 && done[0].task is replacement
        && done[0].outcome == FiberOutcome.completed
        && done[0].cancellationDisposition == CancellationDisposition.none);
    log.takeRecords();

    // drainUntil does not retire, so `terminal` stays valid until retire.
    // A request for a terminal, unretired generation applies as
    // alreadyTerminal and leaves its outcome alone.
    auto terminal = domain.spawn({});
    auto terminalHandle = terminal.handle;
    drainUntil(domain, token, view);
    assert(domain.requestCancel(terminalHandle)
        == CancellationSubmission.queued);
    applications = domain.drainCancellationRequests();
    assert(applications.length == 1
        && applications[0].result == CancellationResult.alreadyTerminal);
    assert(terminal.outcome == FiberOutcome.completed
        && terminal.cancellationDisposition == CancellationDisposition.none);
    // A queued request no longer blocks retirement (release refused it):
    // retire applies it before the generation can be reused.
    assert(domain.requestCancel(terminalHandle)
        == CancellationSubmission.queued);
    done = log.retire(domain);
    assert(domain.pendingCancellationRequests == 0);
    assert(done.length == 1 && done[0].task is terminal
        && done[0].outcome == FiberOutcome.completed
        && done[0].cancellationDisposition == CancellationDisposition.none);
}

void manualSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto task = domain.spawn({
        atomicFetchAdd(steps, 1);
        FiberDomain.await(42);
        atomicFetchAdd(steps, 1);
        FiberDomain.yieldReady();
        atomicFetchAdd(steps, 1);
    });

    auto flushed = domain.flush(token);
    assert(flushed == 1);
    // This table contains exactly one Fiber activation.
    auto consumed = view.consumeNext();
    assert(consumed);
    assert(atomicLoad(steps) == 1 && domain.waiting == 1);
    auto signalled = domain.signal(42);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(atomicLoad(steps) == 2 && domain.ready == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(atomicLoad(steps) == 3 && task.terminated);
    assert(task.outcome == FiberOutcome.completed && task.exception is null);
    assert(domain.pendingCompletions == 1);
    auto completed = log.retire(domain);
    assert(completed.length == 1 && completed[0].task is task);
    assert(completed[0].outcome == FiberOutcome.completed
        && completed[0].exception is null);
}

void defaultBatchSmoke()
{
    enum count = 40;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();
    foreach (_; 0 .. count) domain.spawn({});
    auto flushed = domain.flush(token);
    assert(flushed == 32);
    flushed = domain.flush(token);
    assert(flushed == count - 32);
    foreach (_; 0 .. 100_000)
    {
        view.consumeNext();
        if (domain.drained) break;
    }
    assert(domain.drained);
    assert(domain.pendingCompletions == count && domain.retire() == count);
}

void recycleSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int runs;
    auto task = domain.spawn({ atomicFetchAdd(runs, 1); });
    auto firstHandle = task.handle;
    assert(firstHandle.current && firstHandle.generation != 0);
    auto flushed = domain.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeNext();
    assert(consumed);
    assert(task.terminated && task.outcome == FiberOutcome.completed);
    FiberOutcome observedOutcome;
    auto observed = firstHandle.tryOutcome(observedOutcome);
    assert(observed);
    assert(observedOutcome == FiberOutcome.completed);
    auto done = log.retire(domain);
    assert(done.length == 1 && done[0].task is task);
    assert(domain.pooled == 1);
    // Retired but not yet reused: the handle still reads its generation.
    observed = firstHandle.tryOutcome(observedOutcome);
    assert(observed && observedOutcome == FiberOutcome.completed);

    // The pool hands the same object back with fresh scheduling state.
    auto recycled = domain.spawn({ atomicFetchAdd(runs, 1); });
    auto secondHandle = recycled.handle;
    assert(recycled is task);
    assert(secondHandle.generation != firstHandle.generation);
    assert(secondHandle.diagnosticId != firstHandle.diagnosticId);
    assert(!firstHandle.current && secondHandle.current);
    auto staleCancelled = domain.directorCancel(firstHandle);
    assert(!staleCancelled);
    assert(domain.pooled == 0);
    assert(!recycled.terminated && recycled.outcome == FiberOutcome.pending);
    assert(recycled.exception is null && !recycled.cancellationRequested);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(recycled.terminated && recycled.outcome == FiberOutcome.completed);
    assert(atomicLoad(runs) == 2);
    assert(domain.drained);
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is recycled);

    // A valid newer cancellation is a generation tag, not a resettable bool.
    // A stale request cannot overwrite/clear it after another reuse.
    auto cancelledTask = domain.spawn({ atomicFetchAdd(runs, 1); });
    auto thirdHandle = cancelledTask.handle;
    auto currentCancelled = domain.directorCancel(thirdHandle);
    staleCancelled = domain.directorCancel(secondHandle);
    assert(currentCancelled);
    assert(!staleCancelled);
    assert(cancelledTask.cancellationRequested);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(cancelledTask.outcome == FiberOutcome.cancelled);
    assert(atomicLoad(runs) == 2); // cancellation won before user-body entry
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is cancelledTask
        && done[0].outcome == FiberOutcome.cancelled);
}

void externalJoinSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto task = domain.spawn({});
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

    auto flushed = domain.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeNext();
    assert(consumed && domain.drained);
    // retire closes old-generation waiter admission and cannot reset the
    // manual-reset event until an already registered joiner has awakened.
    auto completed = log.retire(domain);
    assert(completed.length == 1 && completed[0].task is task);
    joiner.join();
    assert(atomicLoad(joinFinished) == 1);
    assert(atomicLoad(joinedStatus) == FiberJoinStatus.completed);

    TaskHandle selfHandle;
    shared int managedJoinStatus;
    auto recycled = domain.spawn({
        atomicStore(managedJoinStatus, cast(int) selfHandle.join().status);
    });
    selfHandle = recycled.handle;
    assert(recycled is task);
    auto stalePoll = handle.poll();
    auto staleJoin = handle.join();
    assert(stalePoll.status == FiberJoinStatus.staleHandle);
    assert(staleJoin.status == FiberJoinStatus.staleHandle);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && domain.drained);
    assert(atomicLoad(managedJoinStatus) == FiberJoinStatus.wouldBlock);
    auto joined = recycled.handle.join();
    assert(joined.terminal && joined.status == FiberJoinStatus.completed);
    assert(joined.outcome == FiberOutcome.completed && joined.failure is null);
    completed = log.retire(domain);
    assert(completed.length == 1 && completed[0].task is recycled);
}

void fiberJoinSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    FiberJoinResult joined;
    auto target = domain.spawn({ FiberDomain.await(0x4A01); });
    auto targetHandle = target.handle;
    auto joiner = domain.spawn({ joined = targetHandle.joinFiber(); });
    auto flushed = domain.flush(token);
    assert(flushed == 2);
    auto consumed = view.consumeNext();
    assert(consumed);
    consumed = view.consumeNext();
    assert(domain.waiting == 2 && !joiner.terminated);

    auto signalled = domain.signal(0x4A01);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && target.terminated && domain.ready == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && joiner.terminated && domain.drained);
    assert(joined.status == FiberJoinStatus.completed && joined.terminal);
    assert(log.retire(domain).length == 2);

    FiberJoinResult timedJoin;
    auto timedTarget = domain.spawn({ FiberDomain.await(0x4A02); });
    auto timedHandle = timedTarget.handle;
    auto timedJoiner = domain.spawn({
        timedJoin = timedHandle.joinFiber(msecs(1));
    });
    flushed = domain.flush(token);
    assert(flushed == 2);
    consumed = view.consumeNext();
    assert(consumed);
    consumed = view.consumeNext();
    assert(domain.waiting == 2 && !timedJoiner.terminated);
    Thread.sleep(msecs(2));
    auto expired = domain.pollTimers();
    assert(expired == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && timedJoiner.terminated);
    assert(timedJoin.status == FiberJoinStatus.timedOut);
    signalled = domain.signal(0x4A02);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && timedTarget.terminated && domain.drained);
    assert(log.retire(domain).length == 2);

    FiberJoinResult timerCancelledJoin;
    auto earlyTarget = domain.spawn({ FiberDomain.await(0x4A04); });
    auto earlyHandle = earlyTarget.handle;
    auto earlyJoiner = domain.spawn({
        timerCancelledJoin = earlyHandle.joinFiber(msecs(100));
    });
    flushed = domain.flush(token);
    assert(flushed == 2);
    consumed = view.consumeNext();
    assert(consumed);
    consumed = view.consumeNext();
    assert(domain.waiting == 2 && !earlyJoiner.terminated);
    signalled = domain.signal(0x4A04);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && earlyTarget.terminated && domain.ready == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && earlyJoiner.terminated && domain.drained);
    assert(timerCancelledJoin.status == FiberJoinStatus.completed);
    long unusedDeadline;
    auto timerRemains = domain.nextTimerDeadline(unusedDeadline);
    assert(!timerRemains);
    assert(log.retire(domain).length == 2);

    shared int joinCleanup;
    auto cancelTarget = domain.spawn({ FiberDomain.await(0x4A03); });
    auto cancelHandle = cancelTarget.handle;
    auto cancelledJoiner = domain.spawn({
        scope (exit) atomicFetchAdd(joinCleanup, 1);
        cast(void) cancelHandle.joinFiber();
    });
    flushed = domain.flush(token);
    assert(flushed == 2);
    consumed = view.consumeNext();
    assert(consumed);
    consumed = view.consumeNext();
    assert(domain.waiting == 2 && !cancelledJoiner.terminated);
    auto cancellation = domain.directorCancelDetailed(cancelledJoiner.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && cancelledJoiner.terminated);
    assert(cancelledJoiner.outcome == FiberOutcome.cancelled);
    assert(atomicLoad(joinCleanup) == 1);
    signalled = domain.signal(0x4A03);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && cancelTarget.terminated && domain.drained);
    assert(log.retire(domain).length == 2);

    TaskHandle selfHandle;
    FiberJoinStatus selfStatus;
    auto self = domain.spawn({ selfStatus = selfHandle.joinFiber().status; });
    selfHandle = self.handle;
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && self.terminated && domain.drained);
    assert(selfStatus == FiberJoinStatus.wouldBlock);
    auto retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is self);

    auto otherLog = new RetireLog;
    auto otherDomain = new FiberDomain(farm, otherLog.options);
    auto foreignTarget = otherDomain.spawn({});
    auto foreignHandle = foreignTarget.handle;
    FiberJoinStatus foreignStatus;
    auto foreignProbe = domain.spawn({
        foreignStatus = foreignHandle.joinFiber().status;
    });
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && foreignProbe.terminated && domain.drained);
    assert(foreignStatus == FiberJoinStatus.wrongDomain);
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is foreignProbe);

    cancellation = otherDomain.directorCancelDetailed(foreignHandle);
    assert(cancellation == CancellationResult.wonBeforeEntry);
    flushed = otherDomain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && foreignTarget.terminated && otherDomain.drained);
    retired = otherLog.retire(otherDomain);
    assert(retired.length == 1 && retired[0].task is foreignTarget);
}

void cancellationSemanticsSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int entered;
    auto preEntry = domain.spawn({ atomicFetchAdd(entered, 1); });
    auto cancellation = domain.directorCancelDetailed(preEntry.handle);
    assert(cancellation == CancellationResult.wonBeforeEntry);
    auto flushed = domain.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeNext();
    assert(consumed);
    assert(preEntry.outcome == FiberOutcome.cancelled);
    assert(preEntry.cancellationDisposition
        == CancellationDisposition.acknowledged);
    assert(atomicLoad(entered) == 0);
    cancellation = domain.directorCancelDetailed(preEntry.handle);
    assert(cancellation == CancellationResult.alreadyTerminal);
    auto done = log.retire(domain);
    assert(done.length == 1 && done[0].task is preEntry);
    // A HOLD Fiber rejected before entry is replaced safely on pool reuse.
    auto replacement = domain.spawn({ atomicFetchAdd(entered, 1); });
    assert(replacement is preEntry);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(replacement.outcome == FiberOutcome.completed);
    assert(replacement.cancellationDisposition == CancellationDisposition.none);
    assert(atomicLoad(entered) == 1);
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is replacement);

    auto ready = domain.spawn({ FiberDomain.yieldReady(); });
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    cancellation = domain.directorCancelDetailed(ready.handle);
    assert(cancellation == CancellationResult.requestedReady);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(ready.outcome == FiberOutcome.cancelled);
    assert(ready.cancellationDisposition
        == CancellationDisposition.acknowledged);
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is ready);

    shared int runningEntered;
    auto running = domain.spawn({
        atomicStore!(MemoryOrder.rel)(runningEntered, 1);
        while (!FiberDomain.currentCancellationRequested()) Thread.yield();
    });
    flushed = domain.flush(token);
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
    cancellation = domain.directorCancelDetailed(running.handle);
    assert(cancellation == CancellationResult.requestedRunning);
    consumer.join();
    if (consumerFailure !is null) throw consumerFailure;
    assert(running.outcome == FiberOutcome.cancelled);
    assert(running.cancellationDisposition
        == CancellationDisposition.resultAbrogated);
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is running);
    while (view.consumeNext()) {}

    auto cleanupFailure = domain.spawn({
        scope (failure) throw new Exception("cleanup failure");
        FiberDomain.await(0xCA11);
    });
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    cancellation = domain.directorCancelDetailed(cleanupFailure.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(cleanupFailure.cancellationRequested);
    assert(cleanupFailure.outcome == FiberOutcome.failed);
    assert(cleanupFailure.cancellationDisposition
        == CancellationDisposition.cleanupFailed);
    assert(cleanupFailure.exception !is null
        && cleanupFailure.exception.msg == "cleanup failure");
    done = log.retire(domain);
    assert(done.length == 1 && done[0].task is cleanupFailure);
    assert(done[0].exception !is null
        && done[0].exception.msg == "cleanup failure"
        && done[0].cancellationDisposition
            == CancellationDisposition.cleanupFailed);
}

void lifecycleEventSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto completedTask = domain.spawn({});
    auto completedHandle = completedTask.handle;
    assert(domain.pendingRecords == 1 && domain.reservedRecords == 4);
    // Nothing is terminal yet, so retire delivers only the admitted record.
    assert(domain.retire() == 0);
    auto events = log.takeRecords();
    assert(events.length == 1);
    assert(events[0].kind == FiberLifecycleKind.admitted);
    assert(events[0].task == completedHandle);
    // Sequences are assigned at emission, from 1 in each domain.
    assert(events[0].sequence == 1);
    auto flushed = domain.flush(token);
    assert(flushed == 1);
    auto consumed = view.consumeNext();
    assert(consumed);
    // Termination returned the two unused reservations; the terminal record
    // holds the last one until it is retired, ahead of the completion.
    assert(domain.pendingRecords == 1 && domain.pendingCompletions == 1);
    assert(domain.reservedRecords == 1);
    auto completions = log.retire(domain);
    assert(completions.length == 1 && completions[0].task is completedTask);
    events = log.takeRecords();
    assert(events.length == 1);
    assert(events[0].kind == FiberLifecycleKind.terminal);
    assert(events[0].outcome == FiberOutcome.completed);
    assert(events[0].task == completedHandle && events[0].sequence == 2);
    assert(domain.reservedRecords == 0);

    auto failed = domain.spawn({
        scope (failure) throw new Exception("event cleanup failure");
        FiberDomain.await(0xE7E7);
    });
    auto oldHandle = failed.handle;
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    auto cancellation = domain.directorCancelDetailed(failed.handle);
    assert(cancellation == CancellationResult.requestedWaiting);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    assert(failed.outcome == FiberOutcome.failed);
    // Records no longer root their task: retire(1) retires one record, then
    // recycles `failed` while its cancellation, terminal and failure records
    // are still queued.
    assert(domain.pendingRecords == 4 && domain.pendingCompletions == 1);
    completions = log.retire(domain, 1);
    assert(completions.length == 1 && completions[0].task is failed);
    assert(domain.pendingRecords == 3 && domain.pendingCompletions == 0);
    assert(domain.pooled == 1);

    auto reused = domain.spawn({});
    assert(reused is failed && !oldHandle.current);
    // The old generation's remaining records, then the reuse's admission.
    assert(domain.retire() == 0);
    events = log.takeRecords();
    assert(events.length == 5);
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
    foreach (i; 0 .. 4)
        assert(events[i].task == oldHandle);
    assert(events[4].kind == FiberLifecycleKind.admitted
        && events[4].task == reused.handle);
    assert(events[0].sequence == 3);
    foreach (i; 1 .. events.length)
        assert(events[i].sequence == events[i - 1].sequence + 1);
    // A record outlives its recycled task: the handle it carries is stale,
    // but its outcome and failure are its own.
    assert(!events[3].task.current);
    auto retainedFailure = events[3].failure;
    // Drain the reused generation fully to leave no explicit domain root.
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed);
    completions = log.retire(domain);
    events = log.takeRecords();
    assert(completions.length == 1 && completions[0].task is reused);
    assert(events.length == 1 && events[0].kind == FiberLifecycleKind.terminal);
    assert(retainedFailure.msg == "event cleanup failure");

    auto fatalTask = domain.spawn({ throw new Error("fatal event"); });
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed); // Error never unwinds through the Farm callback
    assert(domain.fatal);
    assert(domain.fatalException !is null
        && domain.fatalException.msg == "fatal event");
    bool admissionRejected;
    try domain.spawn({});
    catch (Exception) admissionRejected = true;
    assert(admissionRejected);
    completions = log.retire(domain);
    events = log.takeRecords();
    assert(completions.length == 1 && completions[0].task is fatalTask);
    assert(cast(Error) completions[0].exception !is null);
    assert(events.length == 3);
    assert(events[0].kind == FiberLifecycleKind.admitted);
    assert(events[1].kind == FiberLifecycleKind.terminal);
    assert(events[2].kind == FiberLifecycleKind.failure);
    assert(cast(Error) events[2].failure !is null);
    assert(domain.reservedRecords == 0 && domain.pendingRecords == 0);
}

void lifecycleRetentionSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    // Retention is construction-time configuration of at least 4 records.
    bool invalidLimit;
    try cast(void) new FiberDomain(farm, log.options(3));
    catch (Exception) invalidLimit = true;
    assert(invalidLimit);
    auto domain = new FiberDomain(farm, log.options(8));
    assert(domain.options.recordRetention == 8);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    auto first = domain.spawn({});
    auto second = domain.spawn({});
    assert(domain.reservedRecords == 8 && domain.pendingRecords == 2);
    bool rejected;
    try domain.spawn({});
    catch (FiberLifecycleBackpressure failure)
    {
        rejected = true;
        assert(failure.limit == 8 && failure.reserved == 8);
    }
    assert(rejected);

    auto flushed = domain.flush(token, 1);
    assert(flushed == 1);
    auto consumed = view.consumeNext();
    assert(consumed && first.terminated);
    assert(domain.reservedRecords == 6 && domain.pendingRecords == 3);

    // retire(1) retires one record and then one terminal task: `first` is
    // recycled while its terminal record is still queued.
    auto retired = log.retire(domain, 1);
    assert(retired.length == 1 && retired[0].task is first);
    auto one = log.takeRecords();
    assert(one.length == 1 && one[0].kind == FiberLifecycleKind.admitted);
    assert(domain.reservedRecords == 5 && domain.pendingRecords == 2);
    rejected = false;
    try domain.spawn({});
    catch (FiberLifecycleBackpressure) rejected = true;
    assert(rejected);

    assert(domain.retire(1) == 0);
    auto two = log.takeRecords();
    assert(two.length == 1 && two[0].kind == FiberLifecycleKind.admitted);
    assert(two[0].sequence == one[0].sequence + 1);
    assert(domain.reservedRecords == 4 && domain.pendingRecords == 1);
    // The refused spawns left the pool alone: `first`'s storage is reused.
    auto third = domain.spawn({});
    assert(third is first);
    assert(domain.reservedRecords == 8);

    flushed = domain.flush(token);
    assert(flushed == 2);
    consumed = view.consumeNext();
    assert(consumed);
    consumed = view.consumeNext();
    assert(second.terminated && third.terminated && domain.drained);
    assert(domain.reservedRecords == 4);
    // Each retire(2) retires up to two records, then up to two tasks.
    size_t recycled = retired.length;
    while (domain.pendingRecords != 0)
        recycled += log.retire(domain, 2).length;
    auto remainder = log.takeRecords();
    assert(remainder.length == 4 && domain.reservedRecords == 0);
    assert(remainder[0].sequence == two[0].sequence + 1);
    foreach (i; 1 .. remainder.length)
        assert(remainder[i].sequence == remainder[i - 1].sequence + 1);
    assert(recycled == 3 && domain.pendingCompletions == 0);

    // A throwing record handler leaves that record, and every later one,
    // queued with the same sequence numbers; that call recycles no task.
    size_t calls;
    bool failRecords = true;
    ulong failedSequence;
    FiberLifecycleKind failedKind;
    FiberLifecycleEvent[] handled;
    FiberTask[] handledTasks;
    FiberDomainOptions handlerOptions;
    handlerOptions.recordRetention = 8;
    handlerOptions.onRecord = (FiberLifecycleEvent event)
    {
        ++calls;
        if (failRecords)
        {
            failedSequence = event.sequence;
            failedKind = event.kind;
            throw new Exception("handler retry");
        }
        handled ~= event;
    };
    handlerOptions.onCompletion = (FiberTask task) { handledTasks ~= task; };
    auto handlerDomain = new FiberDomain(farm, handlerOptions);
    auto handledTask = handlerDomain.spawn({});
    flushed = handlerDomain.flush(token);
    assert(flushed == 1);
    consumed = view.consumeNext();
    assert(consumed && handledTask.terminated && handlerDomain.drained);
    assert(handlerDomain.pendingRecords == 2
        && handlerDomain.reservedRecords == 2
        && handlerDomain.pendingCompletions == 1);

    bool handlerFailed;
    try handlerDomain.retire(1);
    catch (Exception failure)
    {
        handlerFailed = failure.msg == "handler retry";
    }
    assert(handlerFailed && calls == 1);
    assert(handlerDomain.pendingRecords == 2
        && handlerDomain.reservedRecords == 2);
    assert(handlerDomain.pendingCompletions == 1 && handledTasks.length == 0);

    // The retry sees the same record. retire counts tasks: one record, then
    // the one terminal task.
    failRecords = false;
    assert(handlerDomain.retire(1) == 1 && calls == 2);
    assert(handled.length == 1 && handled[0].sequence == failedSequence
        && handled[0].kind == failedKind);
    assert(handledTasks.length == 1 && handledTasks[0] is handledTask);
    assert(handlerDomain.pendingRecords == 1
        && handlerDomain.reservedRecords == 1);
    assert(handlerDomain.retire(1) == 0);
    assert(handled.length == 2
        && handled[1].kind == FiberLifecycleKind.terminal);
    assert(handled[1].sequence == failedSequence + 1);
    assert(handlerDomain.pendingRecords == 0
        && handlerDomain.reservedRecords == 0);
}

void lifecycleBacklogSmoke()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope (exit) farm.destroy();
    // onRecord is fixed at construction; it forwards to `check`, which the
    // test swaps between phases.
    FiberLifecycleHandler check;
    FiberDomainOptions options;
    options.recordRetention = 4 * 128;
    options.onRecord = (FiberLifecycleEvent event) { check(event); };
    auto domain = new FiberDomain(farm, options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    FiberTask[] tasks;
    foreach (_; 0 .. 96) tasks ~= domain.spawn({});
    size_t seen;
    ulong sequence;
    FiberLifecycleHandler checkAdmitted = (FiberLifecycleEvent event)
    {
        assert(event.kind == FiberLifecycleKind.admitted);
        assert(event.task == tasks[seen].handle);
        assert(event.sequence == sequence + 1);
        sequence = event.sequence;
        ++seen;
    };
    check = checkAdmitted;
    assert(domain.retire(3) == 0 && seen == 3);
    // Records emitted after a partial retire queue behind the unretired ones.
    foreach (_; 0 .. 17) tasks ~= domain.spawn({});
    assert(domain.pendingRecords == tasks.length - seen);
    assert(domain.retire(0) == 0 && seen == 3);
    bool failed;
    ulong failedSequence;
    check = (FiberLifecycleEvent event)
    {
        if (seen == 19)
        {
            failedSequence = event.sequence;
            throw new Exception("retry in a partially retired ring");
        }
        checkAdmitted(event);
    };
    try domain.retire(32);
    catch (Exception) failed = true;
    assert(failed && seen == 19 && failedSequence == sequence + 1);
    assert(domain.pendingRecords == tasks.length - seen);
    check = checkAdmitted;
    while (domain.pendingRecords != 0)
    {
        auto before = seen;
        assert(domain.retire(5) == 0 && seen - before <= 5);
        assert(domain.pendingRecords == tasks.length - seen);
        before = seen;
        assert(domain.retire(11) == 0 && seen - before <= 11);
        assert(domain.pendingRecords == tasks.length - seen);
    }
    assert(seen == tasks.length);
    assert(domain.retire(1) == 0 && seen == tasks.length);

    // drainUntil does not retire: the terminal records and tasks wait for
    // the retire(7) batches below, each of which retires records first.
    drainUntil(domain, token, view);
    assert(domain.pendingCompletions == tasks.length);
    size_t terminals;
    check = (FiberLifecycleEvent event)
    {
        assert(event.kind == FiberLifecycleKind.terminal);
        assert(event.outcome == FiberOutcome.completed);
        ++sequence;
        assert(event.sequence == sequence);
        ++terminals;
    };
    size_t recycled;
    while (domain.pendingRecords != 0)
    {
        recycled += domain.retire(7);
        assert(domain.pendingRecords == tasks.length - terminals);
    }
    recycled += domain.retire();
    assert(terminals == tasks.length && recycled == tasks.length);
    assert(domain.reservedRecords == 0 && domain.pendingCompletions == 0);
}

void sharedDomainLaneSmoke()
{
    auto farmA = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmA.destroy();
    auto farmB = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farmB.destroy();
    // Two ready lanes on two Farms share one domain: waits, signals and joins
    // are domain-wide, while readiness lands on the spawning lane.
    auto domain = new FiberDomain;
    auto laneA = new FiberReadyLane(farmA);
    auto laneB = new FiberReadyLane(farmB);
    domain.attachReadyLane(laneA);
    domain.attachReadyLane(laneB);
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
    auto target = domain.spawnOn(laneA, { FiberDomain.await(0xD04A); });
    auto joiner = domain.spawnOn(laneB, { joined = target.handle.joinFiber(); });
    assert(laneA.ready == 1 && laneB.ready == 1 && domain.ready == 2);

    auto flushed = laneA.flush(tokenA);
    assert(flushed == 1 && laneA.published == 1);
    auto consumed = viewA.consumeNext();
    assert(consumed);
    assert(domain.waiting == 1 && laneA.ready == 0 && laneB.ready == 1);
    assert(laneA.published == 0);
    flushed = laneB.flush(tokenB);
    assert(flushed == 1);
    consumed = viewB.consumeNext();
    assert(consumed);
    assert(domain.waiting == 2 && laneA.ready == 0 && laneB.ready == 0);

    // The signal readies the target on its own lane only.
    auto signalled = domain.signal(0xD04A);
    assert(signalled == 1);
    assert(laneA.ready == 1 && laneB.ready == 0);
    flushed = laneA.flush(tokenA);
    assert(flushed == 1);
    consumed = viewA.consumeNext();
    assert(consumed);
    // The target's completion readies the cross-lane joiner on lane B.
    assert(target.terminated && laneA.ready == 0 && laneB.ready == 1);
    flushed = laneB.flush(tokenB);
    assert(flushed == 1);
    consumed = viewB.consumeNext();
    assert(consumed);
    assert(joiner.terminated && domain.drained);
    assert(joined.status == FiberJoinStatus.completed
        && joined.task == target.handle);
    assert(laneA.published == 0 && laneB.published == 0);

    assert(domain.retire() == 2);
}

/// Consume the next single-activation table from a fresh OS thread. Publishing
/// one resume per table and using a new sole consumer for every step makes
/// migration deterministic rather than relying on the threadpool race to
/// happen to choose another worker.
void consumeOnFreshThread(AntFarm* farm, FiberTask task, ulong expectedResumes,
                          ref Thread[] retainedThreads)
{
    import core.thread : Thread;

    Throwable failure;
    auto thread = new Thread({
        try
        {
            version (TSan) auto nativeContext = __tsan_get_current_fiber();
            ConsumerView view;
            auto subscription = subscribeOrThrow(view, farm);
            scope (exit) view.unsubscribe();
            foreach (_; 0 .. 100_000)
            {
                view.consumeNext();
                version (TSan) assert(__tsan_get_current_fiber() == nativeContext);
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

    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    ulong[5] identities;
    Thread[] retainedThreads;
    int preservedValue;
    auto task = domain.spawn({
        // `box` is deliberately not stored outside the fiber: across each
        // suspension its only root is on the suspended stack.
        auto box = new MigrationBox(41);
        version (TSan) auto taskContext = __tsan_get_current_fiber();
        identities[0] = FiberDomain.currentWorkerIdentity();
        FiberDomain.yieldReady();
        version (TSan) assert(__tsan_get_current_fiber() == taskContext);
        assert(box.value++ == 41);
        identities[1] = FiberDomain.currentWorkerIdentity();
        FiberDomain.await(77);
        version (TSan) assert(__tsan_get_current_fiber() == taskContext);
        assert(box.value++ == 42);
        identities[2] = FiberDomain.currentWorkerIdentity();
        FiberDomain.sleepFor(msecs(2));
        version (TSan) assert(__tsan_get_current_fiber() == taskContext);
        assert(box.value++ == 43);
        identities[3] = FiberDomain.currentWorkerIdentity();
        scope (exit)
        {
            version (TSan) assert(__tsan_get_current_fiber() == taskContext);
            preservedValue = box.value;
            identities[4] = FiberDomain.currentWorkerIdentity();
        }
        FiberDomain.await(88); // cancellation wakes and unwinds this wait
    });

    auto flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 1, retainedThreads);
    GC.collect();

    flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 2, retainedThreads);
    GC.collect();

    auto signalled = domain.signal(77);
    assert(signalled == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 3, retainedThreads);
    GC.collect();

    Thread.sleep(msecs(4));
    auto expired = domain.pollTimers();
    assert(expired == 1);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, task, 4, retainedThreads);

    auto cancelled = domain.directorCancel(task);
    assert(cancelled);
    flushed = domain.flush(token);
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
    auto completed = log.retire(domain);
    assert(completed.length == 1 && completed[0].task is task);

    // An exception retained by druntime must cross back from a migrated
    // fiber without escaping the @nogc/nothrow Ant Farm callback. `failed`
    // reuses the storage `task` was retired to.
    auto failed = domain.spawn({
        FiberDomain.yieldReady();
        throw new Exception("failure after migration");
    });
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, failed, 1, retainedThreads);
    flushed = domain.flush(token);
    assert(flushed == 1);
    consumeOnFreshThread(farm, failed, 2, retainedThreads);
    assert(failed.migrationCount == 1);
    assert(failed.outcome == FiberOutcome.failed);
    assert(failed.exception !is null
        && failed.exception.msg == "failure after migration");
    completed = log.retire(domain);
    assert(completed.length == 1 && completed[0].task is failed);
}

void mixedPayloadSmoke()
{
    enum count = 96;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto log = new RetireLog;
    auto domain = new FiberDomain(farm, log.options);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    atomicStore(barePayloadRuns, 0);
    shared int fiberRuns;
    FiberTask[] tasks;
    foreach (_; 0 .. count)
        tasks ~= domain.spawn({
            atomicFetchAdd(fiberRuns, 1);
            FiberDomain.yieldReady();
            atomicFetchAdd(fiberRuns, 1);
        });

    Callback header;

    header = &barePayloadCallback;
    ulong[count] bodies = 0xA17F_A4FUL;
    PayloadEntry[count] bare;
    foreach (i; 0 .. count)
        bare[i] = PayloadEntry(header, bodies[i .. i + 1]);

    size_t bareOffset;
    foreach (_; 0 .. 200_000)
    {
        if (bareOffset < bare.length)
            bareOffset += farm.write(bare[bareOffset .. $], token, DEFAULT_AVG_COST);
        if (domain.ready != 0)
            domain.flush(token, 17, DEFAULT_AVG_COST);
        view.consumeNext();
        if (bareOffset == count && domain.drained
            && atomicLoad(barePayloadRuns) == count)
            break;
        Thread.yield();
    }
    assert(bareOffset == count);
    assert(atomicLoad(barePayloadRuns) == count);
    assert(atomicLoad(fiberRuns) == count * 2);
    assert(domain.drained);
    auto completed = log.retire(domain);
    assert(completed.length == count);
    foreach (retired; completed)
        assert(retired.outcome == FiberOutcome.completed);
}

void drainUntil(FiberDomain domain, ref Token token, ref ConsumerView view,
                size_t waitFor = 0)
{
    foreach (spin; 0 .. 200_000)
    {
        domain.pollGenerationTriggers();
        domain.flush(token);
        view.consumeNext();
        if (waitFor == 0)
        {
            if (domain.drained) return;
        }
        else if (domain.waiting >= waitFor || domain.drained)
            return;
        Thread.yield();
    }
    assert(false, "fiber drain stalled");
}

final class GenerationProducer
{
    FiberGenerationTrigger trigger;
    shared(uint)* ready;
    shared(uint)* released;
    this(FiberGenerationTrigger trigger, shared(uint)* ready, shared(uint)* released)
    {
        this.trigger = trigger;
        this.ready = ready;
        this.released = released;
    }
    void run()
    {
        atomicFetchAdd(*ready, 1u);
        while (atomicLoad!(MemoryOrder.acq)(*released) == 0) Thread.yield();
        trigger.advance();
    }
}

void syncPrimitiveSmoke()
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

    auto posted = new FiberEvent(domain);
    shared uint ran;
    domain.spawn({
        posted.wait();
        atomicFetchAdd(ran, 1u);
    });
    drainUntil(domain, token, view, 1);
    assert(domain.waiting == 1 && atomicLoad(ran) == 0);
    posted.set();
    drainUntil(domain, token, view);
    assert(atomicLoad(ran) == 1);
    assert(domain.retire() == 1);

    auto generations = new FiberGenerationTrigger(domain);
    shared uint generationsSeen;
    domain.spawn({
        generations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
        generations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
    });
    drainUntil(domain, token, view, 1);
    generations.advance();
    drainUntil(domain, token, view, 1);
    assert(atomicLoad(generationsSeen) == 1);
    generations.advance();
    drainUntil(domain, token, view);
    assert(atomicLoad(generationsSeen) == 2
        && generations.completed == 2);
    assert(domain.retire() == 1);

    // Advances may coalesce before the consumer Fiber begins waiting.
    auto prepostedGenerations = new FiberGenerationTrigger(domain);
    prepostedGenerations.advance();
    prepostedGenerations.advance();
    atomicStore(generationsSeen, 0u);
    domain.spawn({
        prepostedGenerations.waitNext();
        prepostedGenerations.waitNext();
        atomicFetchAdd(generationsSeen, 2u);
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(generationsSeen) == 2
        && prepostedGenerations.completed == 2);
    assert(domain.retire() == 1);

    // Distinct completion callbacks may publish into the deferred MPSC stack
    // concurrently. Force a burst of producers so a failed head CAS must
    // reload the comparison value before retrying.
    enum triggerCount = 64;
    FiberGenerationTrigger[triggerCount] concurrentGenerations;
    Thread[] triggerProducers;
    shared uint triggerProducersReady;
    shared uint releaseTriggerProducers;
    // A separate context keeps each producer bound to its own trigger.
    foreach (i; 0 .. triggerCount)
    {
        concurrentGenerations[i] = new FiberGenerationTrigger(domain);
        auto job = new GenerationProducer(concurrentGenerations[i],
            &triggerProducersReady, &releaseTriggerProducers);
        triggerProducers ~= new Thread(&job.run);
        triggerProducers[$ - 1].start();
    }
    while (atomicLoad!(MemoryOrder.acq)(triggerProducersReady) != triggerCount)
        Thread.yield();
    atomicStore!(MemoryOrder.rel)(releaseTriggerProducers, 1u);
    foreach (producer; triggerProducers)
        producer.join();
    assert(domain.pollGenerationTriggers() == triggerCount);
    foreach (trigger; concurrentGenerations)
        assert(trigger.completed == 1);

    // Cancellation removes a waiter but retains the trigger's empty bucket;
    // a later serialized consumer can register on that same key again.
    auto cancelledGenerations = new FiberGenerationTrigger(domain);
    auto cancelledWaiter = domain.spawn({ cancelledGenerations.waitNext(); });
    drainUntil(domain, token, view, 1);
    assert(domain.directorCancel(cancelledWaiter));
    drainUntil(domain, token, view);
    assert(cancelledWaiter.outcome == FiberOutcome.cancelled);
    assert(domain.retire() == 1);
    atomicStore(generationsSeen, 0u);
    domain.spawn({
        cancelledGenerations.waitNext();
        atomicFetchAdd(generationsSeen, 1u);
    });
    drainUntil(domain, token, view, 1);
    cancelledGenerations.advance();
    drainUntil(domain, token, view);
    assert(atomicLoad(generationsSeen) == 1);
    assert(domain.retire() == 1);

    auto ready = new FiberEvent(domain);
    ready.set();
    atomicStore(ran, 0u);
    domain.spawn({
        ready.wait();
        atomicFetchAdd(ran, 1u);
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(ran) == 1);
    assert(domain.retire() == 1);

    auto gate = new FiberEvent(domain, FiberEventMode.manualReset);
    shared uint manualRan;
    domain.spawn({
        gate.wait();
        atomicFetchAdd(manualRan, 1u);
    });
    domain.spawn({
        gate.wait();
        atomicFetchAdd(manualRan, 1u);
    });
    drainUntil(domain, token, view, 2);
    assert(domain.waiting == 2);
    gate.set();
    drainUntil(domain, token, view);
    assert(atomicLoad(manualRan) == 2);
    assert(domain.retire() == 2);

    auto sem = new FiberSemaphore(domain, 0);
    shared uint semRan;
    domain.spawn({
        sem.wait();
        atomicFetchAdd(semRan, 1u);
    });
    domain.spawn({
        sem.wait();
        atomicFetchAdd(semRan, 1u);
    });
    drainUntil(domain, token, view, 2);
    assert(domain.waiting == 2 && atomicLoad(semRan) == 0);
    sem.post(2);
    drainUntil(domain, token, view);
    assert(atomicLoad(semRan) == 2);
    assert(domain.retire() == 2);

    auto permits = new FiberSemaphore(domain, 2);
    shared uint permitRan;
    domain.spawn({
        permits.wait();
        permits.wait();
        atomicFetchAdd(permitRan, 1u);
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(permitRan) == 1 && permits.available == 0);
    assert(domain.retire() == 1);
}

void sharedSignalAndTimerCancellationSmoke()
{
    import core.time : hours;

    enum count = 64;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                               DEFAULT_SMALL_TABLE_THRESHOLD, false);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto subscription = subscribeOrThrow(view, farm);
    scope (exit) view.unsubscribe();

    shared int resumed;
    FiberTask[] tasks;
    foreach (_; 0 .. count)
        tasks ~= domain.spawn({
            FiberDomain.await(1234);
            atomicFetchAdd(resumed, 1);
        });
    auto timer = domain.spawn({ FiberDomain.sleepFor(hours(1)); });

    while (domain.ready != 0)
    {
        domain.flush(token);
        view.consumeNext();
    }
    while (domain.waiting != count + 1)
        view.consumeNext();

    foreach (i; 0 .. count / 2)
    {
        auto cancelled = domain.directorCancel(tasks[i]);
        assert(cancelled);
    }
    auto timerCancellation = domain.directorCancelDetailed(timer.handle);
    assert(timerCancellation == CancellationResult.requestedWaiting);
    auto signalled = domain.signal(1234);
    assert(signalled == count / 2);

    foreach (_; 0 .. 200_000)
    {
        if (domain.ready != 0) domain.flush(token);
        view.consumeNext();
        if (domain.drained) break;
        Thread.yield();
    }
    assert(domain.drained && domain.waiting == 0);
    assert(atomicLoad(resumed) == count / 2);
    long unusedDeadline;
    auto hasDeadline = domain.nextTimerDeadline(unusedDeadline);
    assert(!hasDeadline);
    assert(timer.outcome == FiberOutcome.cancelled);
    assert(timer.cancellationDisposition
        == CancellationDisposition.acknowledged);
    assert(domain.retire() == count + 1);
}

/// `GroupHostOptions` for the Fiber pool tests: every group, one Farm per
/// group when there are several, and workers that park until woken so only
/// the lanes' wakes and the earliest timer deadline can end a park.
GroupHostOptions fiberHostOptions()
{
    GroupHostOptions o;
    o.groups = HostGroups.all();
    o.farmPerGroup = GroupHost.resolve(o).groups.length > 1;
    o.idleTimeout = Duration.max;
    o.farm.ringMiB = 2;
    return o;
}

/// True after printing SKIP when the default resolution starts no pool worker.
bool skipWithoutPoolWorkers(string test)
{
    import std.string : toStringz;

    GroupHostOptions probe;
    probe.groups = HostGroups.all();
    if (GroupHost.resolve(probe).workers.length != 0) return false;
    printf("SKIP %s: no pool workers\n", test.toStringz);
    return true;
}

void waitTerminated(FiberTask task)
{
    import core.time : msecs;
    foreach (_; 0 .. 400)
    {
        if (task.terminated) return;
        Thread.sleep(msecs(5));
    }
}

// Fibers on a GroupHost through `FiberGroups`: activations run on the workers
// consuming the target group's Farm; an await is released by a Director
// signal; a timer wakes a parked worker (no idle timeout) and fires on time; a
// fiber joins one spawned on another group's lane; a failing fiber records its
// exception without failing its worker; under `director` retirement the
// workers leave every terminal task for the Director; and `beginShutdown(true)`
// unwinds a parked waiter through the workers' service passes.
void threadpoolSmoke()
{
    import core.time : MonoTime;

    if (skipWithoutPoolWorkers("threadpool smoke")) return;
    auto log = new RetireLog;
    auto domain = new FiberDomain(log.options);
    auto fibers = new FiberGroups(domain);
    auto o = fiberHostOptions();
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    scope (exit) host.close();
    assert(host.managedLane && host.workers != 0);
    assert(fibers.host is host && fibers.domain is domain);
    assert(fibers.lanes.length == host.farms.length);
    assert(domain.laneCount == host.farms.length);
    foreach (llc; host.groups)
    {
        auto lane = fibers.laneOf(llc);
        assert(lane !is null);
        // One Farm per group maps each group to its own lane; one shared Farm
        // maps every group to the home group's lane.
        assert(lane.llc == (host.farms.length > 1 ? llc : host.groups[0]));
    }
    immutable ushort groupA = host.groups[0];
    immutable ushort groupB = host.groups[$ - 1];

    // A spawn on a group's lane runs on a worker whose home Farm is that
    // group's Farm.
    shared int ran;
    shared bool homeFarmMatched;
    auto onA = fibers.spawnOn(groupA, {
        auto slot = home!GroupSlot();
        atomicStore(homeFarmMatched, slot !is null && slot.farm is host.farmOf(groupA));
        atomicFetchAdd(ran, 1);
    });
    waitTerminated(onA);
    assert(onA.terminated && atomicLoad(ran) == 1);
    assert(atomicLoad(homeFarmMatched), "a fiber ran on a worker of another Farm");
    assert(onA.outcome == FiberOutcome.completed);
    // Under `director` retirement workers never retire: the terminal task is
    // still valid and waits for the Director. The terminal phase is published
    // just before the completion is pushed, so wait for the push.
    foreach (_; 0 .. 2_000)
    {
        if (domain.retirementPending) break;
        Thread.sleep(msecs(1));
    }
    assert(domain.pendingCompletions == 1 && domain.retirementPending);
    auto retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is onA);

    // A parked waiter released by the Director's signal through the lane wake.
    shared int signalled;
    auto waiter = fibers.spawnOn(groupB, {
        FiberDomain.await(0xD04A);
        atomicFetchAdd(signalled, 1);
    });
    foreach (_; 0 .. 2_000)
    {
        if (domain.waiting == 1) break;
        Thread.sleep(msecs(1));
    }
    assert(domain.waiting == 1 && !waiter.terminated);
    Thread.sleep(msecs(10)); // let the workers park without a deadline
    assert(domain.signal(0xD04A) == 1);
    waitTerminated(waiter);
    assert(waiter.terminated && atomicLoad(signalled) == 1);
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is waiter);

    // A timer armed on a worker: the lane wakes a worker to adopt the
    // deadline, and the pool parks until it.
    MonoTime timerStart;
    MonoTime timerEnd;
    auto timed = fibers.spawnOn(groupB, {
        timerStart = MonoTime.currTime;
        FiberDomain.sleepFor(msecs(30));
        timerEnd = MonoTime.currTime;
    });
    waitTerminated(timed);
    assert(timed.terminated && timed.outcome == FiberOutcome.completed);
    assert(timerEnd - timerStart >= msecs(20));
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is timed);

    // A join across lanes: the target sleeps on group A, the joiner on group
    // B is readied by the target's completion.
    FiberJoinResult joined;
    auto sleeper = fibers.spawnHandleOn(groupA, { FiberDomain.sleepFor(msecs(20)); });
    auto joiner = fibers.spawnOn(groupB, { joined = sleeper.joinFiber(); });
    waitTerminated(joiner);
    assert(joiner.terminated && joined.status == FiberJoinStatus.completed
        && joined.task == sleeper);
    foreach (_; 0 .. 400)
    {
        if (domain.drained) break;
        Thread.sleep(msecs(5));
    }
    assert(domain.drained);
    retired = log.retire(domain);
    assert(retired.length == 2);

    // A failing fiber: its exception is retained on the task; the worker
    // continues (no worker failure), so later work still runs.
    shared int enteredFailure;
    auto failed = fibers.spawn({
        atomicFetchAdd(enteredFailure, 1);
        throw new Exception("fiber failure");
    });
    waitTerminated(failed);
    assert(failed.terminated && atomicLoad(enteredFailure) == 1);
    assert(failed.outcome == FiberOutcome.failed);
    assert(failed.exception !is null && failed.exception.msg == "fiber failure");
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is failed
        && retired[0].outcome == FiberOutcome.failed);
    auto afterFailure = fibers.spawn({ atomicFetchAdd(ran, 1); });
    waitTerminated(afterFailure);
    assert(afterFailure.terminated && atomicLoad(ran) == 2);
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is afterFailure);

    // Cancellation by shutdown removes a long waiter, republishes it on its
    // lane, and unwinds the fiber through FiberCancelled on a worker without
    // turning it into a worker failure.
    shared bool cleaned;
    auto cancelled = fibers.spawnOn(groupB, {
        scope (exit) atomicStore(cleaned, true);
        FiberDomain.await(999);
    });
    foreach (_; 0 .. 2_000)
    {
        if (domain.waiting == 1) break;
        Thread.sleep(msecs(1));
    }
    assert(domain.waiting == 1);
    domain.beginShutdown(true);
    waitTerminated(cancelled);
    assert(cancelled.terminated && cancelled.outcome == FiberOutcome.cancelled);
    assert(atomicLoad(cleaned));
    assert(domain.drained);
    retired = log.retire(domain);
    assert(retired.length == 1 && retired[0].task is cancelled
        && retired[0].outcome == FiberOutcome.cancelled);
    assert(!domain.retirementPending);

    host.stopWorkers();
    assert(host.pool.workerFailures.length == 0);
    printf("threadpool smoke: %u workers, %u Farm(s), groups %u and %u\n",
        host.workers, cast(uint) host.farms.length, cast(uint) groupA, cast(uint) groupB);
}

// T9: the constructing Director only joins the requesting third thread while
// a pool worker must apply cancellation and unwind the parked fiber itself.
// The request arrives while the workers are parked without a deadline, so the
// lane's cancellation wake is what gets it applied.
void workerCancellationSmoke()
{
    import core.time : MonoTime;

    if (skipWithoutPoolWorkers("T9 worker cancellation")) return;
    foreach (throughHandle; [false, true])
    {
        auto domain = new FiberDomain;
        auto fibers = new FiberGroups(domain);
        auto o = fiberHostOptions();
        o.attachments = [fibers.attachment];
        auto host = new GroupHost(o);
        scope (exit) host.close();
        shared int observedCancellation;
        auto task = fibers.spawn({
            try FiberDomain.await(0xCA09);
            catch (FiberCancelled cancelled)
            {
                atomicStore(observedCancellation, 1);
                throw cancelled;
            }
        });
        auto handle = task.handle;
        shared int succeeded;
        auto requester = new Thread({
            immutable deadline = MonoTime.currTime + msecs(2_000);
            while (domain.waiting != 1 && MonoTime.currTime < deadline)
                Thread.sleep(msecs(1));
            if (domain.waiting != 1) return;
            // Let the idle workers park; correctness also covers a request
            // arriving between a worker's final service pass and parking.
            Thread.sleep(msecs(10));
            immutable submitted = throughHandle
                ? domain.requestCancel(handle)
                : domain.requestCancel(task);
            if (submitted != CancellationSubmission.queued) return;
            auto joined = handle.join(msecs(2_000));
            if (joined.status == FiberJoinStatus.timedOut)
                printf("T9: remote join timed out; observed cancellation = %d\n",
                    atomicLoad(observedCancellation));
            if (joined.status == FiberJoinStatus.cancelled
                && joined.outcome == FiberOutcome.cancelled
                && atomicLoad(observedCancellation) == 1)
                atomicStore(succeeded, 1);
        });
        requester.start();
        requester.join(); // No Director service while cancellation is pending.
        immutable passed = atomicLoad(succeeded) == 1;
        // Cleanup also makes a pre-fix timeout finite rather than stranding a
        // live fiber or leaking the domain root after the failed assertion.
        if (!passed) domain.beginShutdown(true);
        waitTerminated(task);
        assert(task.terminated && domain.drained);
        assert(domain.retire() == 1);
        host.stopWorkers();
        assert(host.pool.workerFailures.length == 0);
        assert(passed, "worker cancellation bounded wait expired without Director service");
    }
}

// 2-LLC: a fiber on the second group's lane arms a new earliest timer while
// every worker is parked without a deadline. Every worker polls the domain's
// timers, so the lane's timer wake must reach one of them and the timer must
// fire on time.
void offResponderTimerSmoke(string test)
{
    import core.time : MonoTime;
    import std.string : toStringz;

    if (skipWithoutPoolWorkers(test)) return;
    auto o = fiberHostOptions();
    if (GroupHost.resolve(o).groups.length < 2)
    {
        printf("SKIP %s (needs 2 LLCs)\n", test.toStringz);
        return;
    }
    auto domain = new FiberDomain;
    auto fibers = new FiberGroups(domain);
    o.attachments = [fibers.attachment];
    auto host = new GroupHost(o);
    scope (exit) host.close();
    assert(host.groups.length >= 2 && host.farms.length >= 2);
    immutable ushort groupA = host.groups[0];
    immutable ushort groupB = host.groups[1];
    assert(fibers.laneOf(groupB) !is fibers.laneOf(groupA));

    shared long sleptMs = -1;
    auto task = fibers.spawnOn(groupB, {
        // Outlast every wake caused by the spawn and its publication, so
        // the other workers are parked without a deadline when the timer arms.
        Thread.sleep(msecs(50));
        immutable before = MonoTime.currTime;
        FiberDomain.sleepFor(msecs(20));
        atomicStore(sleptMs, (MonoTime.currTime - before).total!"msecs");
    });
    waitTerminated(task);
    assert(task.terminated, "a timer armed on the second group's lane never fired");
    immutable slept = atomicLoad(sleptMs);
    assert(slept >= 20 && slept < 250, "the second group's timer fired late");
    assert(domain.drained);
    assert(domain.retire() == 1);
    host.stopWorkers();
    assert(host.pool.workerFailures.length == 0);
    printf("%s: group %u timer fired after %lld ms beside group %u\n",
        test.toStringz, cast(uint) groupB, slept, cast(uint) groupA);
}

void fiberParitySmoke()
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

    shared int cleaned;
    domain.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(cleaned) == 1);
    assert(domain.retire() == 1);

    domain.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        FiberDomain.yieldReady();
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(cleaned) == 2);
    assert(domain.retire() == 1);

    auto waiting = domain.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        FiberDomain.await(0xCE);
    });
    drainUntil(domain, token, view, 1);
    assert(domain.waiting == 1);
    domain.directorCancel(waiting);
    drainUntil(domain, token, view);
    assert(atomicLoad(cleaned) == 3);
    assert(waiting.outcome == FiberOutcome.cancelled);
    assert(domain.retire() == 1);

    auto failed = domain.spawn({
        scope (exit) atomicFetchAdd(cleaned, 1);
        throw new Exception("parity failure");
    });
    drainUntil(domain, token, view);
    assert(atomicLoad(cleaned) == 4);
    assert(failed.outcome == FiberOutcome.failed
        && failed.exception !is null
        && failed.exception.msg == "parity failure");
    assert(domain.retire() == 1);

    auto nested = domain.spawn({
        FiberDomain.yieldReady();
        FiberDomain.await(0xAE);
        FiberDomain.sleepFor(msecs(1));
    });
    drainUntil(domain, token, view, 1);
    assert(domain.waiting == 1);
    domain.signal(0xAE);
    foreach (_; 0 .. 200_000)
    {
        domain.flush(token);
        domain.pollTimers();
        view.consumeNext();
        if (nested.terminated) break;
        Thread.yield();
    }
    assert(nested.outcome == FiberOutcome.completed);
    assert(domain.retire() == 1);

    auto first = domain.spawn({});
    drainUntil(domain, token, view);
    assert(domain.retire() == 1);
    shared int recycledRuns;
    auto recycled = domain.spawn({
        FiberDomain.yieldReady();
        atomicFetchAdd(recycledRuns, 1);
    });
    assert(recycled is first);
    drainUntil(domain, token, view);
    assert(atomicLoad(recycledRuns) == 1);
    assert(domain.retire() == 1);

    auto raw = domain.spawn({ Fiber.yield(); });
    drainUntil(domain, token, view);
    assert(raw.outcome == FiberOutcome.failed);
    assert(raw.exception !is null
        && raw.exception.msg == "ant_fibers: Fiber.yield without scheduler suspend");
    assert(domain.retire() == 1);
}

// A native thread and each suspended task need separate TSan call stacks.
// Without this, migration can pop frames from another thread's shadow stack.
private void tsanContextSmoke()
{
    version (TSan)
    {
        auto nativeContext = __tsan_get_current_fiber();
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096,
                                   DEFAULT_SMALL_TABLE_THRESHOLD, false);
        scope (exit) farm.destroy();
        auto log = new RetireLog;
        auto domain = new FiberDomain(farm, log.options);
        auto token = farm.registerProducer(Tier.small);
        scope (exit) farm.unregisterProducer(token);
        ConsumerView view;
        subscribeOrThrow(view, farm);
        scope (exit) view.unsubscribe();
        // drainUntilEmpty retires: results come from the completion log.
        void checkRetired(size_t expected)
        {
            auto retired = log.takeCompleted();
            assert(retired.length == expected);
            foreach (task; retired)
            {
                if (task.exception !is null) throw task.exception;
                assert(task.outcome == FiberOutcome.completed);
            }
        }
        void* firstContext;
        domain.spawn({
            firstContext = __tsan_get_current_fiber();
            assert(firstContext != nativeContext,
                "scheduler fiber must have its own TSan context");
            FiberDomain.yieldReady();
            assert(__tsan_get_current_fiber() == firstContext);
        });
        domain.spawn({
            auto secondContext = __tsan_get_current_fiber();
            assert(secondContext != nativeContext && secondContext != firstContext);
            FiberDomain.yieldReady();
            assert(__tsan_get_current_fiber() == secondContext);
        });
        drainUntilEmpty(domain, token, view);
        assert(__tsan_get_current_fiber() == nativeContext,
            "scheduler must restore its caller's TSan context");
        checkRetired(2);
        // Reuse a task after termination as well as after suspension.
        domain.spawn({ assert(__tsan_get_current_fiber() != nativeContext); });
        drainUntilEmpty(domain, token, view);
        assert(__tsan_get_current_fiber() == nativeContext);
        checkRetired(1);
    }
}

version (TSan)
    private extern(C) void* __tsan_get_current_fiber() nothrow @nogc;

void main(string[] args)
{
    if (args.length == 2 && args[1] == "--event-cancellation-only")
    {
        eventCancellationSmoke();
        printf("event cancellation smoke passed\n");
        return;
    }
    if (args.length == 2 && args[1] == "--semaphore-cancellation-only")
    {
        semaphoreCancellationSmoke();
        printf("semaphore cancellation smoke passed\n");
        return;
    }
    if (args.length == 2 && args[1] == "--tsan-context-only")
    {
        tsanContextSmoke();
        printf("TSan context smoke passed\n");
        return;
    }
    if (args.length == 2 && args[1] == "--migration-only")
    {
        migrationSmoke();
        printf("migration smoke passed\n");
        return;
    }
    if (args.length == 2 && args[1] == "--worker-cancellation-only")
    {
        workerCancellationSmoke();
        printf("worker cancellation smoke passed\n");
        return;
    }
    if (args.length == 2 && args[1] == "--cancellation-only")
    {
        workerCancellationSmoke();
        cancellationOwnershipSmoke();
        cancellationSemanticsSmoke();
        fiberJoinSmoke();
        sharedSignalAndTimerCancellationSmoke();
        waitRaceSmoke();
        printf("cancellation smoke passed\n");
        return;
    }
    tsanContextSmoke();
    workerCancellationSmoke();
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
    lifecycleBacklogSmoke();
    sharedDomainLaneSmoke();
    migrationSmoke();
    mixedPayloadSmoke();
    syncPrimitiveSmoke();
    eventCancellationSmoke();
    semaphoreCancellationSmoke();
    sharedSignalAndTimerCancellationSmoke();
    waitRaceSmoke();
    recordSmoke();
    groupHostFiberSmoke();
    threadpoolSmoke();
    hostAttachSmoke();
    offResponderTimerSmoke("T1 second-group timer");

    printf("fiber smoke passed\n");
}
