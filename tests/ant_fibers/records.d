module ant_fibers.record_tests;

import antfarm;
import ant_fibers.scheduler;
import core.atomic;
import core.memory : GC;
import core.thread : Thread;
import core.time : MonoTime, seconds;

/// Lifecycle records live in a domain-owned ring, independent of task
/// storage. Retirement stops at a ticketed but unpublished record; a record
/// outlives its recycled task and keeps its own failure across collections;
/// throwing handlers leave their record or task queued in order; and
/// concurrent retirers under `serviceThreads` deliver every record once, in
/// sequence, one handler at a time.
void recordSmoke()
{
    unpublishedRecordStopsRetirement();
    recordOutlivesRecycledTask();
    throwingRecordHandler();
    throwingCompletionHandler();
    serviceThreadRetirement();
}

private struct Rig
{
    AntFarm* farm;
    FiberDomain domain;
    Token token;
    ConsumerView view;

    void open(FiberDomainOptions options, uint producers = 1)
    {
        farm = AntFarm.create(2, 8, 4, 0, 0, producers, 4096,
                              DEFAULT_SMALL_TABLE_THRESHOLD, false);
        domain = new FiberDomain(farm, options);
        token = farm.registerProducer(Tier.small);
        subscribeOrThrow(view, farm);
    }

    void close()
    {
        if (domain.options.retirement == FiberRetirement.serviceThreads
            || domain.isDirectorThread)
            domain.retire();
        view.unsubscribe();
        farm.unregisterProducer(token);
        farm.destroy();
    }

    /// Run every admitted task to its end without retiring anything.
    void run()
    {
        immutable deadline = MonoTime.currTime + 5.seconds;
        while (!domain.drained)
        {
            assert(MonoTime.currTime < deadline, "record test fibers stalled");
            domain.flush(token);
            if (!view.consumeNext()) Thread.yield();
        }
    }
}

private shared int holdTerminal, terminalHeld, releaseTerminal;

private void holdTerminalHook(FiberLifecycleKind kind) nothrow @nogc
{
    if (kind != FiberLifecycleKind.terminal) return;
    int armed = 1;
    if (!cas(&holdTerminal, armed, 0)) return;
    atomicStore(terminalHeld, 1);
    while (atomicLoad(releaseTerminal) == 0) Thread.yield();
}

/// X's terminal record (sequence 2) is ticketed on a worker that pauses
/// before publishing it; meanwhile the Director admits Y (sequence 3).
/// Retirement delivers sequence 1 and stops at 2 rather than skipping ahead
/// or spinning; X's completion is not yet pushed, so nothing is recycled.
/// Once the worker publishes, the next retirement resumes in order.
private void unpublishedRecordStopsRetirement()
{
    FiberLifecycleEvent[] seen;
    size_t recycled;
    FiberDomainOptions options;
    options.onRecord = (FiberLifecycleEvent e) { seen ~= e; };
    options.onCompletion = (FiberTask) { ++recycled; };
    Rig rig;
    rig.open(options);
    scope (exit) rig.close();
    auto domain = rig.domain;
    atomicStore(holdTerminal, 0);
    atomicStore(terminalHeld, 0);
    atomicStore(releaseTerminal, 0);
    recordAfterTicketHook = &holdTerminalHook;
    scope (exit) recordAfterTicketHook = null;

    shared int stop;
    auto farm = rig.farm;
    auto worker = new Thread({
        ConsumerView view;
        subscribeOrThrow(view, farm);
        while (atomicLoad(stop) == 0)
            if (!view.consumeNext()) Thread.yield();
        while (view.consumeNext()) {}
        view.unsubscribe();
    });
    worker.start();
    scope (exit) { atomicStore(stop, 1); worker.join(); }

    auto x = domain.spawnHandle({});
    atomicStore(holdTerminal, 1);
    immutable deadline = MonoTime.currTime + 5.seconds;
    while (domain.flush(rig.token) == 0)
        assert(MonoTime.currTime < deadline);
    while (atomicLoad(terminalHeld) == 0)
    {
        assert(MonoTime.currTime < deadline, "X never reached its terminal record");
        Thread.yield();
    }
    auto y = domain.spawnHandle({});
    assert(domain.retire() == 0, "X's completion follows its unpublished record");
    assert(seen.length == 1 && seen[0].sequence == 1
        && seen[0].kind == FiberLifecycleKind.admitted && seen[0].task == x);
    assert(domain.pendingRecords == 2, "ticket 2 in flight, ticket 3 published");

    atomicStore(releaseTerminal, 1);
    while (!domain.drained)
    {
        assert(MonoTime.currTime < deadline, "Y never ran");
        domain.flush(rig.token);
        Thread.yield();
    }
    assert(domain.retire() == 2 && recycled == 2);
    assert(seen.length == 4);
    foreach (i, ref e; seen) assert(e.sequence == i + 1, "sequences are contiguous");
    assert(seen[1].kind == FiberLifecycleKind.terminal && seen[1].task == x);
    assert(seen[2].kind == FiberLifecycleKind.admitted && seen[2].task == y);
    assert(seen[3].kind == FiberLifecycleKind.terminal && seen[3].task == y);
    assert(domain.reservedRecords == 0 && domain.pendingRecords == 0);
}

/// A task is recycled while its terminal and failure records are still
/// queued, then its object is reused. The records still carry their own
/// outcome and failure, which survive collections even though the reused
/// task no longer references the exception; their handle reports stale.
private void recordOutlivesRecycledTask()
{
    FiberLifecycleEvent[] seen;
    FiberDomainOptions options;
    options.onRecord = (FiberLifecycleEvent e) { seen ~= e; };
    Rig rig;
    rig.open(options);
    scope (exit) rig.close();
    auto domain = rig.domain;

    auto failed = domain.spawnHandle({
        throw new Exception("record-held failure");
    });
    rig.run();
    // Records first: the admitted record, then the task is recycled.
    assert(domain.retire(1) == 1);
    assert(seen.length == 1 && domain.pendingRecords == 2);
    auto reused = domain.spawnHandle({});
    assert(!failed.current, "the recycled object now runs a new generation");
    rig.run();
    GC.collect();
    GC.collect();
    assert(domain.retire() == 1);
    assert(seen.length == 5);
    assert(seen[1].kind == FiberLifecycleKind.terminal && seen[1].task == failed
        && !seen[1].task.current && seen[1].outcome == FiberOutcome.failed);
    assert(seen[2].kind == FiberLifecycleKind.failure && seen[2].task == failed);
    assert(seen[2].failure !is null && seen[2].failure.msg == "record-held failure",
        "the ring keeps the failure reachable after its task is reused");
    assert(seen[3].task == reused && seen[4].task == reused);
}

private void throwingRecordHandler()
{
    ulong[] delivered;
    bool throwOnce = true;
    FiberDomainOptions options;
    options.onRecord = (FiberLifecycleEvent e) {
        if (throwOnce && e.sequence == 2)
        {
            throwOnce = false;
            throw new Exception("record handler");
        }
        delivered ~= e.sequence;
    };
    Rig rig;
    rig.open(options);
    scope (exit) rig.close();
    auto domain = rig.domain;
    domain.spawn({});
    domain.spawn({});
    rig.run();
    bool threw;
    try
        domain.retire();
    catch (Exception e)
        threw = e.msg == "record handler";
    assert(threw && delivered == [1]);
    assert(domain.pendingRecords == 3 && domain.pendingCompletions == 2,
        "a throwing record handler retires no task");
    assert(domain.retire() == 2);
    assert(delivered == [1, 2, 3, 4], "the retried record keeps its sequence");
}

/// The second handler call throws: the first task is recycled, the second
/// and third stay queued ahead of a fourth that completes before the retry.
private void throwingCompletionHandler()
{
    TaskHandle[] order;
    TaskHandle failOn;
    FiberDomainOptions options;
    options.onCompletion = (FiberTask task) {
        if (task.handle == failOn)
        {
            failOn = TaskHandle.init;
            throw new Exception("completion handler");
        }
        order ~= task.handle;
    };
    Rig rig;
    rig.open(options);
    scope (exit) rig.close();
    auto domain = rig.domain;
    TaskHandle[4] spawned;
    foreach (i; 0 .. 3)
        spawned[i] = domain.spawnHandle({});
    failOn = spawned[1];
    rig.run();
    bool threw;
    try
        domain.retire();
    catch (Exception e)
        threw = e.msg == "completion handler";
    assert(threw && order == [spawned[0]]);
    assert(domain.pooled == 1 && domain.pendingCompletions == 2);
    spawned[3] = domain.spawnHandle({});
    rig.run();
    assert(domain.retire() == 3);
    assert(order == spawned[], "the backlog precedes newer completions");
}

/// Two worker threads serve and consume the domain under `serviceThreads`,
/// each retiring when it wins the gate, while the Director admits tasks
/// against a small retention and cancels some. Every record arrives once, in
/// sequence order, never in two handlers at a time, and per task in
/// admitted, cancellation, terminal order; all reservations return.
private void serviceThreadRetirement()
{
    enum tasks = 2_000;
    struct Seen { ulong sequence; FiberLifecycleKind kind; TaskHandle task; }
    Seen[] seen;
    shared int inHandler;
    shared size_t recycled;
    FiberDomainOptions options;
    options.recordRetention = 64;
    options.retirement = FiberRetirement.serviceThreads;
    options.onRecord = (FiberLifecycleEvent e) {
        int idle = 0;
        assert(cas(&inHandler, idle, 1), "two retirers ran handlers at once");
        seen ~= Seen(e.sequence, e.kind, e.task);
        atomicStore(inHandler, 0);
    };
    options.onCompletion = (FiberTask) { atomicFetchAdd(recycled, 1); };
    Rig rig;
    rig.open(options, 3);
    scope (exit) rig.close();
    auto domain = rig.domain;
    auto farm = rig.farm;

    shared int stop;
    Thread[2] workers;
    foreach (ref worker; workers)
    {
        worker = new Thread({
            auto token = farm.registerProducer(Tier.small);
            ConsumerView view;
            subscribeOrThrow(view, farm);
            while (atomicLoad(stop) == 0)
            {
                immutable progress = domain.serve(token).progress;
                if (!view.consumeNext() && !progress) Thread.yield();
            }
            while (view.consumeNext()) {}
            view.unsubscribe();
            farm.unregisterProducer(token);
        });
        worker.start();
    }

    immutable deadline = MonoTime.currTime + 20.seconds;
    size_t backpressured;
    ulong rng = 0x2545_F491_4F6C_DD1DUL;
    foreach (i; 0 .. tasks)
    {
        TaskHandle h;
        while (true)
        {
            assert(MonoTime.currTime < deadline, "admission never recovered");
            try
            {
                h = domain.spawnHandle({
                    FiberDomain.yieldReady();
                    FiberDomain.yieldReady();
                });
                break;
            }
            catch (FiberLifecycleBackpressure)
            {
                ++backpressured;
                Thread.yield();
            }
        }
        rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
        if (rng % 4 == 0) cast(void) domain.requestCancel(h);
    }
    while (!domain.drained || domain.retirementPending)
    {
        assert(MonoTime.currTime < deadline, "service threads never retired everything");
        Thread.yield();
    }
    atomicStore(stop, 1);
    foreach (worker; workers) worker.join();

    assert(backpressured != 0, "retention 64 never pushed back");
    assert(atomicLoad(recycled) == tasks);
    assert(domain.reservedRecords == 0 && domain.pendingRecords == 0);
    foreach (i, ref s; seen) assert(s.sequence == i + 1, "records arrive in sequence");
    // Per task: admitted, then an optional cancellation, then terminal.
    ubyte[ulong] stage; // by diagnosticId: one per slot generation
    size_t terminals;
    foreach (ref s; seen)
    {
        auto p = s.task.diagnosticId in stage;
        final switch (s.kind)
        {
        case FiberLifecycleKind.admitted:
            assert(p is null);
            stage[s.task.diagnosticId] = 1;
            break;
        case FiberLifecycleKind.cancellation:
            assert(p !is null && *p == 1);
            *p = 2;
            break;
        case FiberLifecycleKind.terminal:
            assert(p !is null && *p != 3);
            *p = 3;
            ++terminals;
            break;
        case FiberLifecycleKind.failure:
            assert(false, "no task fails here");
        }
    }
    assert(terminals == tasks && stage.length == tasks);
}
