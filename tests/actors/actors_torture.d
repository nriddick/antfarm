/++
 + Deterministic interleaving and sustained-contention tests for
 + antfarm.actors.actor. Build with AntfarmActorTestHooks and, for the allocator
 + contract test, AntfarmMimallocV3.
 +/
module actors_torture;

import antfarm;
import antfarm.actors;
import antfarm.allocation : allocateAligned64, freeAligned64;
version (AntfarmMimallocV3)
    import antfarm.actors.mimalloc : mimallocV3ActorAllocator;
import core.atomic;
import core.lifetime : move;
import core.memory : GC;
import core.stdc.stdio : fflush, fprintf, printf, stderr, stdout;
import core.stdc.stdlib : abort, free, malloc;
import core.stdc.string : memset;
import core.sync.barrier : Barrier;
import core.thread : Thread;
import core.time : MonoTime, seconds;

version (AntfarmActorTestHooks) {}
else static assert(false,
    "actors_torture requires AntfarmActorTestHooks");

private void check(bool condition, const(char)* message)
{
    if (condition) return;
    fprintf(stderr, "FAIL: %s\n", message);
    fflush(stderr);
    abort();
}

private bool accepted(ActorSendResult result) pure nothrow @nogc @safe
{
    return result == ActorSendResult.queued
        || result == ActorSendResult.coalesced;
}

private struct CountedAllocator
{
    size_t allocations;
    size_t deallocations;
}

private void* countedAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (alignment > 64) return null;
    auto counts = cast(CountedAllocator*) context;
    ++counts.allocations;
    return allocateAligned64(bytes);
}

private void countedDeallocate(void* context, void* memory, size_t,
        size_t) nothrow @nogc @system
{
    auto counts = cast(CountedAllocator*) context;
    ++counts.deallocations;
    freeAligned64(memory);
}

private ActorAllocator countedPolicy(ref CountedAllocator counts)
    nothrow @nogc @system
{
    return ActorAllocator(&counts, &countedAllocate, &countedDeallocate);
}

// Keep released storage mapped and poisoned until the paused wave returns.
// A trailing access after dropping membership is then deterministic, without
// relying on the system allocator to reuse or unmap the freed slot array.
private struct Quarantine
{
    void*[8] pointers;
    size_t[8] sizes;
    size_t count;
}

private void* quarantineAllocate(void*, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    return alignment <= 64 ? allocateAligned64(bytes) : null;
}

private void quarantineFree(void* context, void* memory, size_t bytes, size_t)
    nothrow @nogc @system
{
    auto q = cast(Quarantine*) context;
    if (q.count >= q.pointers.length) abort();
    q.pointers[q.count] = memory;
    q.sizes[q.count++] = bytes;
    memset(memory, 0xDD, bytes);
}

// -------------------------------------------------------------------------
// Deterministic send/retire interleavings
// -------------------------------------------------------------------------

private struct BoundaryResult
{
    ulong activations;
    ulong consumed;
    ulong sum;
    ulong bad;
}

private struct BoundaryState
{
    BoundaryResult* output;
}

private struct BoundaryMessage
{
    ActorInboxNode node;
    ulong a;
    ulong b;
    ulong c;
    ulong d;
}

private void boundaryActor(scope ref ActorBorrow!BoundaryState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    auto output = actor.value.output;
    ++output.activations;
    while (true)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto message = cast(BoundaryMessage*) node.payload;
        if (message is null
                || message.a != 0x0123_4567_89ab_cdefUL
                || message.b != ~message.a
                || message.c != 0xfedc_ba98_7654_3210UL
                || message.d != message.a + message.c
                || node.tag != 0xa55a_a55a_a55a_a55aUL)
            ++output.bad;
        ++output.consumed;
        output.sum += message.a ^ message.c;
        node.complete();
    }
}

private __gshared shared(int) g_hookTarget;
private __gshared shared(int) g_hookArrived;
private __gshared shared(int) g_hookRelease;
private __gshared ActorInboxNode* g_hookNode;
private __gshared shared(int) g_raceArrived;
private __gshared shared(int) g_raceRelease;
private __gshared ActorInboxNode* g_raceNode;
private __gshared shared(int) g_waveReserveArrived;
private __gshared shared(int) g_waveReserveRelease;
private __gshared shared(int) g_retirementIdleArrived;
private __gshared shared(int) g_retirementIdleRelease;

private void blockingActorHook(ActorTestPoint point, ActorInboxNode* node)
    nothrow @nogc @system
{
    if (node != g_hookNode
            || cast(int) point != atomicLoad!(MemoryOrder.acq)(g_hookTarget))
        return;
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_hookRelease) == 0) {}
}

private void claimRaceHook(ActorTestPoint point, ActorInboxNode* node)
    nothrow @nogc @system
{
    if (point != ActorTestPoint.submissionReserved || node != g_raceNode)
        return;
    atomicFetchAdd!(MemoryOrder.acq_rel)(g_raceArrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_raceRelease) == 0) {}
}

private void waveReserveHook(ActorTestPoint point, ActorInboxNode*)
    nothrow @nogc @system
{
    if (point != ActorTestPoint.waveLifecycleReserved) return;
    atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_waveReserveRelease) == 0) {}
}

private void retirementJoinHook(ActorTestPoint point, ActorInboxNode* node)
    nothrow @nogc @system
{
    if (point == ActorTestPoint.retirementIdleObserved)
    {
        atomicStore!(MemoryOrder.rel)(g_retirementIdleArrived, 1);
        while (atomicLoad!(MemoryOrder.acq)(g_retirementIdleRelease) == 0) {}
    }
    else blockingActorHook(point, node);
}

private void waveReleaseHook(ActorTestPoint point, ActorInboxNode*)
    nothrow @nogc @system
{
    if (point != ActorTestPoint.waveMembershipReleased) return;
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_hookRelease) == 0) {}
}

private class BoundarySender
{
    ActorHandle!BoundaryState handle;
    ActorInboxNode* node;
    shared int done;
    ActorSendResult result;

    this(ActorHandle!BoundaryState initialHandle, ActorInboxNode* initialNode)
    {
        handle = initialHandle;
        node = initialNode;
    }

    void run()
    {
        result = handle.send(node);
        atomicStore!(MemoryOrder.rel)(done, 1);
    }
}

private void waitFlag(ref shared int flag, MonoTime deadline,
        const(char)* failure)
{
    while (atomicLoad!(MemoryOrder.acq)(flag) == 0)
    {
        if (MonoTime.currTime >= deadline)
            check(false, failure);
        Thread.yield();
    }
}

private void initializeBoundaryMessage(ref BoundaryMessage message)
    nothrow @nogc @system
{
    message.a = 0x0123_4567_89ab_cdefUL;
    message.b = ~message.a;
    message.c = 0xfedc_ba98_7654_3210UL;
    message.d = message.a + message.c;
    message.node.initialize(&message, 0xa55a_a55a_a55a_a55aUL);
}

private void drainBoundary(ActorRuntime* runtime, ref Token token,
        ref ConsumerView view, ref ActorOwner!BoundaryState owner,
        MonoTime deadline)
{
    while (!owner.retired)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "deterministic boundary drain timeout");
        Thread.yield();
    }
}

private void testCloseBoundary(ActorTestPoint point)
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "boundary Farm allocation");
    scope (exit) farm.destroy();

    CountedAllocator counts;
    auto runtime = ActorRuntime.create(farm, 1, countedPolicy(counts));
    check(runtime !is null, "boundary runtime allocation");
    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    check(owner.valid, "boundary actor allocation");
    auto handle = owner.handle;

    ConsumerView view;
    check(view.subscribe(farm) >= 0, "boundary consumer subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "boundary producer registration");

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    BoundaryMessage rejected;
    initializeBoundaryMessage(rejected);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget, cast(int) point);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);

    auto sender = new BoundarySender(handle, &message.node);
    auto thread = new Thread(&sender.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline, "sender did not reach forced boundary");

    check(owner.requestRetire() == ActorRetireResult.requested,
        "retirement request at forced boundary");
    check(!owner.retired, "in-flight reservation prevents early retirement");
    check(handle.send(&rejected.node) == ActorSendResult.closed,
        "close rejects a later sender while reservation is paused");

    if (point == ActorTestPoint.inboxPublished
            || point == ActorTestPoint.activationSignalled)
    {
        // The head publication is sufficient for the actor to consume the
        // node; the reservation still prevents final retirement.
        if (point == ActorTestPoint.inboxPublished)
            check(handle.wake() == ActorWakeResult.closed,
                "ordinary wake is closed after retirement");
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        check(output.consumed == 1,
            "published node visible before sender releases reservation");
        check(!owner.retired,
            "processed node still waits for sender reservation release");
    }

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "pre-close reservation remains accepted");
    drainBoundary(runtime, token, view, owner, deadline);

    check(output.consumed == 1 && output.bad == 0,
        "forced-boundary payload consumed exactly once and intact");
    check(output.sum == (message.a ^ message.c),
        "forced-boundary payload checksum");
    check(message.node.available && rejected.node.available,
        "forced-boundary node ownership returned");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "forced-boundary actor reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.live == 0 && runtime.ready == 0,
        "forced-boundary runtime drained");
    runtime.destroy();
    check(counts.allocations == counts.deallocations,
        "forced-boundary allocator balance");
}

private void testClaimIsNotPublication()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "claim/publication Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "claim/publication runtime allocation");
    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    auto handle = owner.handle;
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "claim/publication subscribe");
    auto token = farm.registerProducer(Tier.small);

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.nodeClaimed);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto sender = new BoundarySender(handle, &message.node);
    auto thread = new Thread(&sender.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline, "sender did not pause after node claim");

    check(!message.node.available, "claimed node is caller-inaccessible");
    check(handle.wake() == ActorWakeResult.queued,
        "independent activation while claimed node is unpublished");
    while (output.activations == 0)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "unpublished-node activation timeout");
    }
    check(output.consumed == 0,
        "node claim alone does not expose partially published payload");

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "claimed node send completes");
    check(owner.requestRetire() == ActorRetireResult.requested,
        "claim/publication retirement");
    drainBoundary(runtime, token, view, owner, deadline);
    check(output.consumed == 1 && output.bad == 0 && message.node.available,
        "published payload appears once and intact");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "claim/publication reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
}

private void testReleasedReservationIsQuiescent()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "released-reservation Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "released-reservation runtime allocation");
    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    auto handle = owner.handle;
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "released-reservation subscribe");
    auto token = farm.registerProducer(Tier.small);

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReleased);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto sender = new BoundarySender(handle, &message.node);
    auto thread = new Thread(&sender.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "sender did not pause after reservation release");

    check(owner.requestRetire() == ActorRetireResult.requested,
        "retire after submission reservation release");
    drainBoundary(runtime, token, view, owner, deadline);
    check(output.consumed == 1 && output.bad == 0 && message.node.available,
        "released reservation no longer delays drain or retirement");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "old generation reclaims while send frame returns locally");

    // Reuse the stable slot while the old send is stopped after its final
    // slot access. This pins down the exact end of submission quiescence.
    auto replacement = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    check(replacement.valid
            && replacement.handle.generation != handle.generation,
        "slot reuse after sender reservation release");
    check(replacement.requestRetire() == ActorRetireResult.requested
            && replacement.retired,
        "replacement idle retirement");
    check(replacement.reclaim() == ActorReclaimResult.reclaimed,
        "replacement reclaim");

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result),
        "old accepted send returns after safe slot reuse");
    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.ready == 0 && runtime.live == 0,
        "released-reservation runtime drained");
    runtime.destroy();
}

private void testContendedNodeClaimDuringClose()
{
    enum senderCount = 8;
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "claim-race Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "claim-race runtime allocation");
    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    auto handle = owner.handle;
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "claim-race subscribe");
    auto token = farm.registerProducer(Tier.small);

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    g_raceNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_raceArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_raceRelease, 0);
    setActorTestHook(&claimRaceHook);
    BoundarySender[senderCount] senders;
    Thread[senderCount] threads;
    foreach (i; 0 .. senderCount)
    {
        senders[i] = new BoundarySender(handle, &message.node);
        threads[i] = new Thread(&senders[i].run);
        threads[i].start();
    }
    auto deadline = MonoTime.currTime + 15.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_raceArrived) != senderCount)
    {
        if (MonoTime.currTime >= deadline)
            check(false, "claim-race reservations did not rendezvous");
        Thread.yield();
    }

    check(owner.requestRetire() == ActorRetireResult.requested,
        "claim-race close after all reservations");
    check(!owner.retired,
        "claim-race reservations prevent premature retirement");
    atomicStore!(MemoryOrder.rel)(g_raceRelease, 1);
    foreach (thread; threads) thread.join();
    setActorTestHook(null);
    g_raceNode = null;

    size_t acceptedCount;
    size_t busyCount;
    foreach (sender; senders)
    {
        if (accepted(sender.result)) ++acceptedCount;
        else if (sender.result == ActorSendResult.nodeBusy) ++busyCount;
    }
    check(acceptedCount == 1 && busyCount == senderCount - 1,
        "exactly one pre-close reservation claims a contended node");
    drainBoundary(runtime, token, view, owner, deadline);
    check(output.consumed == 1 && output.bad == 0 && message.node.available,
        "contended node delivered and completed exactly once");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "claim-race reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
}

private struct WavePinResult
{
    ulong waveCalls;
    ulong autonomousCalls;
    ulong inboxNodes;
}

private struct WavePinState
{
    WavePinResult* result;
}

private void wavePinDormant(scope ref ActorBorrow!WavePinState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++actor.value.result.autonomousCalls;
    while (auto node = context.popInbox())
    {
        ++actor.value.result.inboxNodes;
        node.complete();
    }
}

private void wavePinOperation(scope ref ActorBorrow!WavePinState actor)
    nothrow @nogc @system
{
    ++actor.value.result.waveCalls;
}

private final class WaveRacePublisher
{
    AntFarm* farm;
    ActorHandle!WavePinState actor;
    ActorWave wave;
    size_t written;
    ActorWaveHandle completion;
    shared int published;
    shared int sealRequested;
    shared int done;

    this(AntFarm* initialFarm, ActorHandle!WavePinState initialActor)
    {
        farm = initialFarm;
        actor = initialActor;
    }

    void run()
    {
        auto token = farm.registerProducer(Tier.small);
        check(token.valid, "wave-race publisher token");
        wave.begin(farm);
        ActorHandle!WavePinState[1] members;
        members[0] = actor;
        written = cast(size_t) wave.publish!wavePinOperation(
            members[], token, avgCostForChunk(32));
        atomicStore!(MemoryOrder.rel)(published, 1);
        while (atomicLoad!(MemoryOrder.acq)(sealRequested) == 0)
            Thread.yield();
        completion = wave.seal();
        farm.unregisterProducer(token);
        atomicStore!(MemoryOrder.rel)(done, 1);
    }
}

/// Force either a signal in the CAS/owner-store gap, or an accepted sender
/// whose zero-owner check preceded admission and whose signal follows it.
private void testWaveSignalRace(bool send, bool lateSender = false)
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "wave signal Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "wave signal runtime allocation");
    scope (exit) runtime.destroy();
    WavePinResult output;
    auto owner = runtime.createActor!(WavePinState, wavePinDormant)(
        WavePinState(&output));
    check(owner.valid, "wave signal actor allocation");
    auto handle = owner.handle;
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "wave signal subscribe");
    scope (exit) view.unsubscribe();
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "wave signal producer token");
    scope (exit) farm.unregisterProducer(token);
    auto deadline = MonoTime.currTime + 15.seconds;
    ActorInboxNode node;
    node.initialize(null);
    ActorSendResult sent;
    Thread sender;
    if (lateSender)
    {
        g_hookNode = &node;
        // Release/acquire hook flags publish the selected seam and join the
        // sender's owner check before wave admission, then resume its signal.
        atomicStore!(MemoryOrder.rel)(g_hookTarget,
            cast(int) ActorTestPoint.submissionReserved);
        atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
        atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
        setActorTestHook(&blockingActorHook);
        sender = new Thread({ sent = handle.send(&node); });
        sender.start();
        waitFlag(g_hookArrived, deadline, "wave late sender reservation timeout");
    }
    else
    {
        // The hook's release/acquire flags order our signal strictly after
        // lifecycle reservation and before the publisher's owner store.
        atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 0);
        atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 0);
        setActorTestHook(&waveReserveHook);
    }
    auto publisher = new WaveRacePublisher(farm, handle);
    auto thread = new Thread(&publisher.run);
    thread.start();
    if (lateSender)
    {
        waitFlag(publisher.published, deadline, "wave late sender publication timeout");
        atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
        sender.join();
    }
    else
    {
        waitFlag(g_waveReserveArrived, deadline, "wave signal reservation timeout");
        if (send) sent = handle.send(&node);
        else check(handle.wake() == ActorWakeResult.coalesced,
            "wave reservation wake coalesces");
        atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 1);
        waitFlag(publisher.published, deadline, "wave signal publication timeout");
    }
    setActorTestHook(null);
    g_hookNode = null;
    if (send) check(sent == ActorSendResult.coalesced,
        "wave reservation send coalesces");
    check(publisher.written == 1, "wave signal member admitted");
    // Join the publisher before reading its completion handle; consumption
    // remains on this thread so the post-wave activation cannot run early.
    atomicStore!(MemoryOrder.rel)(publisher.sealRequested, 1);
    thread.join();
    while (!publisher.completion.finished)
    {
        while (view.consumeNext()) {}
        check(MonoTime.currTime < deadline, "wave signal completion timeout");
    }
    check(output.waveCalls == 1 && output.autonomousCalls == 0,
        "wave operation completes before autonomous activation");
    check(runtime.ready == 1, "wave signal queues post-wave activation");
    while (output.autonomousCalls == 0)
    {
        runtime.flush(token, 1, avgCostForChunk(32));
        while (view.consumeNext()) {}
        check(MonoTime.currTime < deadline, "wave signal autonomous timeout");
    }
    check(output.autonomousCalls == 1 && output.inboxNodes == (send ? 1 : 0),
        "wave signal honoured exactly once");
    if (send) check(node.available && node.outcome == ActorNodeOutcome.completed,
        "wave signal send completed");
    check(!publisher.completion.failed,
        "benign wave signal must not fail the completed wave");
    check(publisher.completion.interrupted,
        "wave records the observed signal independently of failure");
    if (lateSender)
    {
        auto old = publisher.completion;
        publisher.wave.begin(farm);
        check(!old.valid && !old.interrupted && !old.failed && !old.finished,
            "reused wave invalidates old outcome observations");
        auto fresh = publisher.wave.handle;
        check(!fresh.interrupted && !fresh.failed,
            "new generation clears sticky outcomes");
        ActorHandle!WavePinState[2] duplicates = [handle, handle];
        check(publisher.wave.publish!wavePinOperation(duplicates[], token) == 0,
            "duplicate wave member rejected");
        auto rejected = publisher.wave.seal();
        check(rejected.finished && rejected.failed && !rejected.interrupted,
            "duplicate rejection remains structural failure");
    }
    check(owner.requestRetire() == ActorRetireResult.requested && owner.retired,
        "wave signal actor retires after drain");
    check(owner.reclaim() == ActorReclaimResult.reclaimed, "wave signal reclaim");
    printf("actor wave signal race OK (send=%d late=%d)\n", send, lateSender);
}

private void testWaveMembershipPinsRetiredActor()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "wave-race Farm allocation");
    scope (exit) farm.destroy();
    Quarantine quarantine;
    auto runtime = ActorRuntime.create(farm, 1,
        ActorAllocator(&quarantine, &quarantineAllocate, &quarantineFree));
    check(runtime !is null, "wave-race runtime allocation");
    WavePinResult output;
    auto owner = runtime.createActor!(WavePinState, wavePinDormant)(
        WavePinState(&output));
    check(owner.valid, "wave-race actor allocation");

    ConsumerView view;
    check(view.subscribe(farm) >= 0, "wave-race consumer subscribe");
    auto retirementToken = farm.registerProducer(Tier.small);
    check(retirementToken.valid, "wave-race retirement token");

    atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 0);
    setActorTestHook(&waveReserveHook);
    auto publisher = new WaveRacePublisher(farm, owner.handle);
    auto thread = new Thread(&publisher.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_waveReserveArrived, deadline,
        "wave publisher did not pause after lifecycle reservation");

    // The lifecycle reservation won, but waveOwner is deliberately not yet
    // visible. Retirement may therefore close this actor and must be joined
    // through the wave callback's pending path.
    check(owner.requestRetire() == ActorRetireResult.requested,
        "wave-race retirement wins before membership publication");
    check(!owner.retired,
        "reserved wave payload prevents immediate retirement");

    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 1);
    waitFlag(publisher.published, deadline,
        "wave-race table publication timeout");
    setActorTestHook(null);
    check(publisher.written == 1,
        "retirement race preserves reserved wave payload");

    while (publisher.wave.handle.progress != 1)
    {
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "wave-race payload completion timeout");
        Thread.yield();
    }
    while (!owner.retired)
    {
        runtime.flush(retirementToken, 1, avgCostForChunk(32));
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "wave-race retirement drain timeout");
        Thread.yield();
    }

    check(output.waveCalls == 1 && output.autonomousCalls == 0,
        "wave-race operation executes before retirement without overlap");
    check(!publisher.wave.handle.finished,
        "open aggregate retains completed actor membership");
    check(owner.reclaim() == ActorReclaimResult.busy,
        "wave membership pins retired actor state and runtime");

    g_hookNode = null;
    atomicStore(g_hookTarget, cast(int) ActorTestPoint.waveMembershipReleased);
    atomicStore(g_hookArrived, 0);
    atomicStore(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    atomicStore!(MemoryOrder.rel)(publisher.sealRequested, 1);
    waitFlag(g_hookArrived, deadline, "wave membership release boundary timeout");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "owner release immediately permits actor reclamation");
    check(runtime.live == 0 && runtime.ready == 0,
        "wave-race runtime drained");
    runtime.destroy();
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    waitFlag(publisher.done, deadline, "wave-race seal timeout");
    thread.join();
    setActorTestHook(null);
    check(publisher.completion.finished && !publisher.completion.failed
            && publisher.completion.interrupted,
        "raced retirement finishes the sealed wave as interrupted");
    check(quarantine.count == 3, "actor state, slots and runtime quarantined");
    foreach (i; 0 .. quarantine.count)
    {
        foreach (byte_; (cast(ubyte*) quarantine.pointers[i])[0 .. quarantine.sizes[i]])
            check(byte_ == 0xDD, "wave touched reclaimed runtime storage");
        freeAligned64(quarantine.pointers[i]);
    }

    view.unsubscribe();
    farm.unregisterProducer(retirementToken);
}

private void testWaveReleaseAllowsImmediateReuse()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "wave release Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 2);
    check(runtime !is null, "wave release runtime allocation");
    scope (exit) runtime.destroy();
    WavePinResult[2] outputs;
    ActorOwner!WavePinState[2] owners;
    foreach (i; 0 .. owners.length)
    {
        owners[i] = runtime.createActor!(WavePinState, wavePinDormant)(
            WavePinState(&outputs[i]));
        check(owners[i].valid, "wave release actor allocation");
    }
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "wave release consumer subscribe");
    scope (exit) view.unsubscribe();
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "wave release producer token");
    scope (exit) farm.unregisterProducer(token);

    auto publisher = new WaveRacePublisher(farm, owners[0].handle);
    auto thread = new Thread(&publisher.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(publisher.published, deadline, "wave release publication timeout");
    while (publisher.wave.handle.progress != 1)
    {
        while (view.consumeNext()) {}
        check(MonoTime.currTime < deadline, "wave release completion timeout");
    }
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&waveReleaseHook);
    atomicStore!(MemoryOrder.rel)(publisher.sealRequested, 1);
    waitFlag(g_hookArrived, deadline, "wave release did not pause after unpin");

    // Releasing the pin permits immediate reuse, even before the previous
    // wave has published aggregate completion. Make the reused slot the new
    // list head, with another member behind it: any late clearing of its
    // waveNext would lose that member and leave its lifetime pin stuck.
    ActorWave next;
    next.begin(farm);
    ActorHandle!WavePinState[2] members =
        [owners[1].handle, owners[0].handle];
    check(next.publish!wavePinOperation(members[], token, avgCostForChunk(32)) == 2,
        "released actor accepts a new wave immediately");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    check(publisher.completion.finished && !publisher.completion.failed,
        "previous wave completes after actor reuse");

    auto completion = next.seal();
    while (!completion.finished)
    {
        while (view.consumeNext()) {}
        check(MonoTime.currTime < deadline, "replacement wave completion timeout");
    }
    check(!completion.failed && outputs[0].waveCalls == 2
            && outputs[1].waveCalls == 1,
        "both generations of wave operations run exactly once");
    foreach (ref owner; owners)
    {
        check(owner.requestRetire() == ActorRetireResult.requested,
            "replacement wave releases every actor membership");
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "replacement wave permits actor reclamation");
    }
}

private void testRetirementJoinsLateSignal()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "retirement join Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "retirement join runtime allocation");
    scope (exit) runtime.destroy();
    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    check(owner.valid, "retirement join actor allocation");
    BoundaryMessage message;
    initializeBoundaryMessage(message);
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "retirement join producer");
    scope (exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "retirement join consumer");
    scope (exit) view.unsubscribe();

    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 0);
    setActorTestHook(&retirementJoinHook);
    auto sender = new BoundarySender(owner.handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    immutable deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline, "retirement join sender reservation");

    ActorRetireResult result;
    auto retireThread = new Thread({ result = owner.requestRetire(); });
    retireThread.start();
    waitFlag(g_retirementIdleArrived, deadline,
        "retirement did not sample idle before joining submissions");

    // L was sampled idle, but this accepted sender queues an activation and
    // drops the last reservation before retirement reads the closed gate.
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    check(accepted(sender.result), "retirement join accepted sender");
    check(runtime.ready == 1 && !owner.retired,
        "late signal remains queued before retirement resumes");
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 1);
    retireThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(result == ActorRetireResult.requested && !owner.retired,
        "idle sample must be refreshed after the submission join");
    drainBoundary(runtime, token, view, owner, deadline);
    check(output.consumed == 1 && output.bad == 0 && message.node.available,
        "retirement join preserves the accepted message");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "retirement join reclamation");
}

private void waveCreditHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
{
    if (point != ActorTestPoint.waveTableProgressRecorded) return;
    if (cas(&g_hookArrived, 0, 1))
        while (atomicLoad!(MemoryOrder.acq)(g_hookRelease) == 0) {}
}

private void testWaveCompletionCreditPinsDescriptor(bool twoTables)
{
    auto farm = AntFarm.create(2, 4, 2, 0, 0, 1, 256);
    scope(exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 2);
    scope(exit) runtime.destroy();
    WavePinResult[2] outputs;
    ActorOwner!WavePinState[2] owners;
    foreach (i; 0 .. 2)
        owners[i] = runtime.createActor!(WavePinState, wavePinDormant)(WavePinState(&outputs[i]));
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ActorWave wave;
    wave.begin(farm);
    ActorHandle!WavePinState[2] members = [owners[0].handle, owners[1].handle];
    atomicStore(g_hookArrived, 0);
    atomicStore(g_hookRelease, 0);
    setActorTestHook(&waveCreditHook);
    shared int started;
    auto worker = new Thread({
        ConsumerView view;
        check(view.subscribe(farm) >= 0, "credit regression subscribe");
        scope(exit) view.unsubscribe();
        atomicStore(started, 1);
        while (!view.consumeNext()) Thread.yield();
    });
    worker.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(started, deadline, "credit regression worker start");
    check(wave.publish!wavePinOperation(members[0 .. 1], token, avgCostForChunk(32)) == 1, "credit regression publish");
    waitFlag(g_hookArrived, deadline, "credit regression notification pause");
    check(wave.handle.progress == 1 && wave.handle.length == 1, "credit regression progress recorded");
    if (twoTables)
        check(wave.publish!wavePinOperation(members[1 .. 2], token, avgCostForChunk(32)) == 1, "credit regression second table");
    auto old = wave.seal();
    check(!old.finished, "progress equality must not release active table notification");
    if (twoTables)
    {
        auto other = new Thread({
            ConsumerView view;
            check(view.subscribe(farm) >= 0, "credit regression second subscribe");
            scope(exit) view.unsubscribe();
            while (wave.handle.progress != 2)
            {
                view.consumeNext();
                check(MonoTime.currTime < deadline, "credit regression second consumer timeout");
            }
        });
        other.start();
        other.join(); // the second hook has returned, the first is still paused
        check(old.progress == 2 && old.length == 2 && !old.finished,
            "another hook must not finish while a peer still holds credit");
    }
    check(owners[0].handle.wake() == ActorWakeResult.waveOwned, "active notification retains membership");
    atomicStore(g_hookRelease, 1);
    while (!old.finished) check(MonoTime.currTime < deadline, "credit regression finish timeout");
    // Reuse before joining the old notification's thread: dropping the final
    // credit must really be its last descriptor access.
    wave.begin(farm);
    check(wave.seal().finished && !old.valid, "credit regression immediate descriptor reuse");
    worker.join();
    setActorTestHook(null);
    check(outputs[0].waveCalls == 1 && outputs[1].waveCalls == (twoTables ? 1 : 0),
        "credit regression exactly once");
    foreach (ref owner; owners)
    {
        check(owner.requestRetire() == ActorRetireResult.requested, "credit regression retire");
        check(owner.reclaim() == ActorReclaimResult.reclaimed, "credit regression reclaim");
    }
}

// Contract characterization, not a bug reproduction. Public `finished` is
// stored before the optional user completion callback runs. An owner may
// observe `finished` and `begin` a new generation while that callback is
// still executing. The callback's context must outlive the callback; an old
// callback must not touch a reused descriptor without its own synchronization.
private struct CompletionContractProbe
{
    ActorWave* wave;
    shared int entered;
    shared int go;
    ulong observedGeneration;
}

private void completionContractCallback(void* p) nothrow @nogc @system
{
    auto probe = cast(CompletionContractProbe*) p;
    atomicStore!(MemoryOrder.rel)(probe.entered, 1);
    while (atomicLoad!(MemoryOrder.acq)(probe.go) == 0) pause();
    probe.observedGeneration = probe.wave.handle.generation;
}

private void testCompletionCallbackRunsAfterFinished()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    ActorWave wave;
    CompletionContractProbe probe;
    probe.wave = &wave;
    wave.begin(farm, ActorWaveCompletionHook(&probe, &completionContractCallback));
    auto old = wave.handle;
    check(old.generation == 1, "completion contract starts at generation 1");
    auto sealer = new Thread({ wave.seal(); });
    sealer.start();
    immutable deadline = MonoTime.currTime + 15.seconds;
    waitFlag(probe.entered, deadline, "completion contract callback did not start");
    check(old.finished, "finished is visible before the completion callback returns");
    wave.begin(farm);
    check(wave.handle.generation == 2 && wave.handle.generation != old.generation,
        "begin is allowed while the old completion callback is still running");
    check(!old.valid, "old handle does not track the new generation");
    atomicStore!(MemoryOrder.rel)(probe.go, 1);
    sealer.join();
    check(probe.observedGeneration == 2,
        "old completion callback can observe the new generation");
    check(wave.seal().finished, "completion contract seals the new generation");
    printf("actor completion-callback contract OK (callback may outlive finished)\n");
    fflush(stdout);
}

private void testDeterministicInterleavings()
{
    testClaimIsNotPublication();
    testCloseBoundary(ActorTestPoint.submissionReserved);
    testCloseBoundary(ActorTestPoint.nodeClaimed);
    testCloseBoundary(ActorTestPoint.inboxPublished);
    testCloseBoundary(ActorTestPoint.activationSignalled);
    testContendedNodeClaimDuringClose();
    testReleasedReservationIsQuiescent();
    testWaveSignalRace(false);
    testWaveSignalRace(true);
    testWaveSignalRace(true, true);
    testWaveCompletionCreditPinsDescriptor(false);
    testWaveCompletionCreditPinsDescriptor(true);
    testWaveMembershipPinsRetiredActor();
    testWaveReleaseAllowsImmediateReuse();
    testRetirementJoinsLateSignal();
    printf("actor deterministic interleavings OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Engine-owned module-generation aggregate unload fence
// -------------------------------------------------------------------------

/// Test-side model of the fence an engine module-generation owner needs.
/// `actors` deliberately does not own this hierarchy: the resident
/// registry combines erased actor owners with admissions for module code
/// frames and ownership which has escaped an actor callback.
private enum GenerationClaimKind : size_t
{
    destructor,
    sender,
    inboxNode,
    count,
}

private enum ulong generationAdmissionClosed = 1UL << 63;
private enum ulong generationAdmissionCount = generationAdmissionClosed - 1;
private enum size_t generationActorCapacity = 4;

private final class TestModuleGeneration
{
    private shared ulong admissionGate_;
    private shared ulong[GenerationClaimKind.count] claims_;
    private ActorErasedOwner[generationActorCapacity] actors_;
    private size_t actorCount_;
    private shared int unloaded_;

    void adopt(ref ActorErasedOwner owner)
    {
        check(owner.valid, "module generation adopts a live actor owner");
        check((atomicLoad!(MemoryOrder.acq)(admissionGate_)
                & generationAdmissionClosed) == 0,
            "module generation actor admission remains open");
        foreach (ref slot; actors_)
        {
            if (slot.valid) continue;
            slot = move(owner);
            ++actorCount_;
            return;
        }
        check(false, "module generation actor registry capacity");
    }

    bool acquire(GenerationClaimKind kind) nothrow @nogc @system
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        while (true)
        {
            if ((observed & generationAdmissionClosed) != 0)
                return false;
            if ((observed & generationAdmissionCount)
                    == generationAdmissionCount)
                fatal("module generation admission count wrap");
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &admissionGate_, observed, observed + 1))
            {
                atomicFetchAdd!(MemoryOrder.rel)(claims_[kind], 1UL);
                return true;
            }
            observed = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        }
    }

    void release(GenerationClaimKind kind) nothrow @nogc @system
    {
        immutable priorKind = atomicFetchSub!(MemoryOrder.acq_rel)(
            claims_[kind], 1UL);
        if (priorKind == 0)
            fatal("module generation claim underflow");
        immutable priorGate = atomicFetchSub!(MemoryOrder.acq_rel)(
            admissionGate_, 1UL);
        if ((priorGate & generationAdmissionCount) == 0)
            fatal("module generation admission underflow");
    }

    void close()
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        while ((observed & generationAdmissionClosed) == 0)
        {
            immutable replacement = observed | generationAdmissionClosed;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &admissionGate_, observed, replacement))
                break;
            observed = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        }
        foreach (ref actor; actors_)
        {
            if (!actor.valid) continue;
            immutable result = actor.requestRetire();
            check(result == ActorRetireResult.requested
                    || result == ActorRetireResult.alreadyRetired,
                "module generation requests actor retirement");
        }
    }

    /// A successful result is the test's simulated `dlclose` boundary.
    /// Closing admission makes a zero observation stable: no new claimant can
    /// appear after this acquire load.
    bool tryUnload()
    {
        if (atomicLoad!(MemoryOrder.acq)(unloaded_) != 0)
            return true;
        immutable opening = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        if ((opening & generationAdmissionClosed) == 0)
            return false;

        bool actorBusy;
        foreach (ref actor; actors_)
        {
            if (!actor.valid) continue;
            immutable retired = actor.retired;
            if (!retired)
            {
                actorBusy = true;
                continue;
            }
            immutable reclaimed = actor.reclaim();
            check(reclaimed == ActorReclaimResult.reclaimed,
                "module generation reclaims retired actor");
            --actorCount_;
        }
        if (actorBusy) return false;

        immutable closing = atomicLoad!(MemoryOrder.acq)(admissionGate_);
        if ((closing & generationAdmissionCount) != 0)
            return false;
        foreach (kind; GenerationClaimKind.destructor
                .. GenerationClaimKind.count)
            check(atomicLoad!(MemoryOrder.acq)(claims_[kind]) == 0,
                "zero aggregate claim count has zero diagnostics");
        atomicStore!(MemoryOrder.rel)(unloaded_, 1);
        return true;
    }

    @property size_t actorCount() const nothrow @nogc @system
    {
        return actorCount_;
    }

    ulong claims(GenerationClaimKind kind) const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(claims_[kind]);
    }

    @property bool unloaded() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(unloaded_) != 0;
    }

    @property shared(int)* unloadMarker() nothrow @nogc @system
    {
        return &unloaded_;
    }
}

private struct ModuleCallbackState
{
    shared int* arrived;
    shared int* release;
    shared int* unloaded;
    shared int* bad;
}

private void stalledModuleCallback(
        scope ref ActorBorrow!ModuleCallbackState actor,
        scope ref ActorContext) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)(*actor.value.arrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(*actor.value.release) == 0) {}
    if (atomicLoad!(MemoryOrder.acq)(*actor.value.unloaded) != 0)
        atomicFetchAdd!(MemoryOrder.rel)(*actor.value.bad, 1);
}

private class SingleActorConsumer
{
    AntFarm* farm;
    shared int done;

    this(AntFarm* actorFarm)
    {
        farm = actorFarm;
    }

    void run()
    {
        ConsumerView view;
        check(view.subscribe(farm) >= 0,
            "module generation consumer subscribe");
        while (!view.consumeNext()) Thread.yield();
        view.unsubscribe();
        atomicStore!(MemoryOrder.rel)(done, 1);
    }
}

private void adoptActor(T)(TestModuleGeneration generation,
        ref ActorOwner!T owner)
{
    auto erased = owner.intoErased();
    generation.adopt(erased);
    check(!owner.valid && !erased.valid,
        "actor ownership transferred into module registry");
}

private void testCallbackBlocksAggregateUnload()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "callback-fence Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "callback-fence actor runtime allocation");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "callback-fence producer registration");

    shared int arrived;
    shared int release;
    shared int bad;
    auto generation = new TestModuleGeneration;
    auto owner = runtime.createActor!(ModuleCallbackState,
        stalledModuleCallback)(ModuleCallbackState(
            &arrived, &release, generation.unloadMarker, &bad));
    check(owner.valid, "callback-fence actor creation");
    auto handle = owner.handle;
    adoptActor(generation, owner);

    auto consumer = new SingleActorConsumer(farm);
    auto consumerThread = new Thread(&consumer.run);
    consumerThread.start();
    check(handle.wake() == ActorWakeResult.queued,
        "callback-fence actor wake");
    auto deadline = MonoTime.currTime + 15.seconds;
    while (runtime.flush(token, 1, DEFAULT_AVG_COST) == 0)
    {
        if (MonoTime.currTime >= deadline)
            check(false, "callback-fence publication timeout");
        Thread.yield();
    }
    waitFlag(arrived, deadline, "old-generation callback did not stall");

    generation.close();
    check(!generation.acquire(GenerationClaimKind.sender),
        "closed module generation rejects new code admission");
    check(generation.actorCount == 1 && !generation.tryUnload()
            && !generation.unloaded,
        "running old-generation callback blocks aggregate unload");

    atomicStore!(MemoryOrder.rel)(release, 1);
    consumerThread.join();
    check(atomicLoad!(MemoryOrder.acq)(consumer.done) == 1,
        "callback-fence consumer returned");
    check(generation.tryUnload() && generation.actorCount == 0,
        "aggregate unload crosses after old callback retirement");
    check(atomicLoad!(MemoryOrder.acq)(bad) == 0,
        "old callback never observed premature unload");

    farm.unregisterProducer(token);
    check(runtime.live == 0 && runtime.ready == 0,
        "callback-fence runtime drained");
    runtime.destroy();
}

private class ModuleDestructorJob
{
    TestModuleGeneration generation;
    shared int arrived;
    shared int release;
    shared int bad;

    this(TestModuleGeneration initialGeneration)
    {
        generation = initialGeneration;
    }

    void run()
    {
        check(generation.acquire(GenerationClaimKind.destructor),
            "old-generation destructor admitted before close");
        atomicStore!(MemoryOrder.rel)(arrived, 1);
        while (atomicLoad!(MemoryOrder.acq)(release) == 0) {}
        if (generation.unloaded)
            atomicFetchAdd!(MemoryOrder.rel)(bad, 1);
        generation.release(GenerationClaimKind.destructor);
    }
}

private void testDestructorBlocksAggregateUnload()
{
    auto generation = new TestModuleGeneration;
    auto destructor = new ModuleDestructorJob(generation);
    auto thread = new Thread(&destructor.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(destructor.arrived, deadline,
        "old-generation destructor did not stall");

    generation.close();
    check(generation.claims(GenerationClaimKind.destructor) == 1
            && !generation.tryUnload() && !generation.unloaded,
        "old-generation destructor blocks aggregate unload");
    atomicStore!(MemoryOrder.rel)(destructor.release, 1);
    thread.join();
    check(atomicLoad!(MemoryOrder.acq)(destructor.bad) == 0,
        "old-generation destructor never observed premature unload");
    check(generation.tryUnload(),
        "aggregate unload crosses after destructor return");
}

private class ModuleSenderJob
{
    TestModuleGeneration generation;
    ActorErasedHandle handle;
    ActorInboxNode* node;
    ActorSendResult result;
    shared int bad;

    this(TestModuleGeneration initialGeneration,
            ActorErasedHandle initialHandle, ActorInboxNode* initialNode)
    {
        generation = initialGeneration;
        handle = initialHandle;
        node = initialNode;
    }

    void run()
    {
        check(generation.acquire(GenerationClaimKind.sender),
            "old-generation sender admitted before close");
        result = handle.send(node);
        if (generation.unloaded)
            atomicFetchAdd!(MemoryOrder.rel)(bad, 1);
        generation.release(GenerationClaimKind.sender);
    }
}

private void testSenderBlocksAggregateUnload()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "sender-fence Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "sender-fence actor runtime allocation");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "sender-fence producer registration");
    ConsumerView view;
    check(view.subscribe(farm) >= 0,
        "sender-fence consumer subscribe");

    BoundaryResult output;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(
        BoundaryState(&output));
    check(owner.valid, "sender-fence actor creation");
    auto handle = owner.handle.erased;
    auto generation = new TestModuleGeneration;
    adoptActor(generation, owner);

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReleased);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto sender = new ModuleSenderJob(generation, handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "old-generation sender did not stall after actor release");

    generation.close();
    while (runtime.live != 0)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        generation.tryUnload();
        if (MonoTime.currTime >= deadline)
            check(false, "sender-fence actor retirement timeout");
        Thread.yield();
    }
    check(generation.actorCount == 0
            && generation.claims(GenerationClaimKind.sender) == 1,
        "sender remains sole aggregate claimant after actor reclamation");
    check(!generation.tryUnload() && !generation.unloaded,
        "old-generation sender frame blocks aggregate unload");

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result)
            && atomicLoad!(MemoryOrder.acq)(sender.bad) == 0,
        "old-generation sender returns before unload");
    check(output.consumed == 1 && output.bad == 0
            && message.node.available,
        "sender-fence interaction completed before actor reclamation");
    check(generation.tryUnload(),
        "aggregate unload crosses after sender frame return");

    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.live == 0 && runtime.ready == 0,
        "sender-fence runtime drained");
    runtime.destroy();
}

private struct DetachedNodeState
{
    shared int* popped;
    shared int* bad;
    ActorInboxNode** detached;
}

private void detachInboxNode(scope ref ActorBorrow!DetachedNodeState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    auto node = context.popInbox();
    if (node is null)
    {
        atomicFetchAdd!(MemoryOrder.rel)(*actor.value.bad, 1);
        return;
    }
    *actor.value.detached = node;
    atomicStore!(MemoryOrder.rel)(*actor.value.popped, 1);
    // Deliberate transfer to a resident completion queue. The actor returns
    // without calling complete(), so actor retirement alone no longer
    // describes this node's old-generation ownership.
}

private class DetachedNodeCompletionJob
{
    TestModuleGeneration generation;
    ActorInboxNode* node;
    shared int arrived;
    shared int release;
    shared int bad;

    this(TestModuleGeneration initialGeneration, ActorInboxNode* initialNode)
    {
        generation = initialGeneration;
        node = initialNode;
    }

    void run()
    {
        atomicStore!(MemoryOrder.rel)(arrived, 1);
        while (atomicLoad!(MemoryOrder.acq)(release) == 0) {}
        if (generation.unloaded)
            atomicFetchAdd!(MemoryOrder.rel)(bad, 1);
        node.complete();
        generation.release(GenerationClaimKind.inboxNode);
    }
}

private void testDetachedNodeBlocksAggregateUnload()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "detached-node-fence Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null,
        "detached-node-fence actor runtime allocation");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "detached-node-fence producer registration");
    ConsumerView view;
    check(view.subscribe(farm) >= 0,
        "detached-node-fence consumer subscribe");

    shared int popped;
    shared int bad;
    ActorInboxNode* detached;
    auto owner = runtime.createActor!(DetachedNodeState, detachInboxNode)(
        DetachedNodeState(&popped, &bad, &detached));
    check(owner.valid, "detached-node-fence actor creation");
    auto handle = owner.handle;
    auto generation = new TestModuleGeneration;
    adoptActor(generation, owner);
    check(generation.acquire(GenerationClaimKind.inboxNode),
        "inbox node ownership admitted before send");

    BoundaryMessage message;
    initializeBoundaryMessage(message);
    check(accepted(handle.send(&message.node)),
        "detached-node-fence interaction accepted");
    auto deadline = MonoTime.currTime + 15.seconds;
    while (atomicLoad!(MemoryOrder.acq)(popped) == 0)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "detached-node-fence pop timeout");
        Thread.yield();
    }
    check(detached is &message.node && !message.node.available,
        "popped node remains processing after actor callback returns");

    auto completion = new DetachedNodeCompletionJob(
        generation, detached);
    auto completionThread = new Thread(&completion.run);
    completionThread.start();
    waitFlag(completion.arrived, deadline,
        "detached inbox-node completion did not stall");
    generation.close();
    check(!generation.tryUnload(),
        "detached node first allows actor retirement before unload");
    check(generation.actorCount == 0 && runtime.live == 0
            && generation.claims(GenerationClaimKind.inboxNode) == 1,
        "popped node remains sole aggregate claimant");
    check(!generation.tryUnload() && !generation.unloaded,
        "popped-but-uncompleted node blocks aggregate unload");

    atomicStore!(MemoryOrder.rel)(completion.release, 1);
    completionThread.join();
    check(atomicLoad!(MemoryOrder.acq)(completion.bad) == 0
            && atomicLoad!(MemoryOrder.acq)(bad) == 0
            && message.node.available,
        "detached node completes before unload");
    check(generation.tryUnload(),
        "aggregate unload crosses after detached-node completion");

    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.ready == 0 && runtime.live == 0,
        "detached-node-fence runtime drained");
    runtime.destroy();
}

private void testAggregateModuleUnloadFence()
{
    testCallbackBlocksAggregateUnload();
    testDestructorBlocksAggregateUnload();
    testSenderBlocksAggregateUnload();
    testDetachedNodeBlocksAggregateUnload();
    printf("actor module-generation aggregate unload fence OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Multi-actor, multi-producer sustained contention and generation churn
// -------------------------------------------------------------------------

private enum size_t stressActorCount = 8;
private enum size_t stressProducerCount = 6;
private enum size_t stressMessagesPerActor = 96;
version (TSan)
    private enum size_t stressRounds = 4;
else
    private enum size_t stressRounds = 32;
private enum size_t stressMessageCount = stressActorCount
    * stressProducerCount * stressMessagesPerActor;

private struct StressResult
{
    ulong consumed;
    ulong sum;
    ulong bad;
    ulong activations;
}

private struct StressState
{
    size_t actorId;
    ulong[stressProducerCount] nextSequence;
    ulong consumed;
    ulong sum;
    ulong bad;
    ulong activations;
    StressResult* output;
}

private struct StressMessage
{
    ActorInboxNode node;
    ulong actorId;
    ulong producerId;
    ulong sequence;
    ulong value;
    ulong inverse;
}

private size_t stressIndex(size_t producer, size_t actor, size_t sequence)
    pure nothrow @nogc @safe
{
    return (producer * stressActorCount + actor)
        * stressMessagesPerActor + sequence;
}

private ulong stressValue(size_t round, size_t actor, size_t producer,
        size_t sequence) pure nothrow @nogc @safe
{
    ulong value = cast(ulong) round * 0x9e37_79b9_7f4a_7c15UL;
    value ^= cast(ulong) actor * 0xd1b5_4a32_d192_ed03UL;
    value ^= cast(ulong) producer * 0x94d0_49bb_1331_11ebUL;
    value ^= cast(ulong) sequence * 0xbf58_476d_1ce4_e5b9UL;
    return value;
}

private void stressActor(scope ref ActorBorrow!StressState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++actor.value.activations;
    immutable limit = 1 + (actor.value.actorId * 5) % 23;
    foreach (_; 0 .. limit)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto message = cast(StressMessage*) node.payload;
        bool good = message !is null
            && message.actorId == actor.value.actorId
            && message.producerId < stressProducerCount
            && message.inverse == ~message.value
            && node.tag == message.value;
        if (good)
        {
            immutable producer = cast(size_t) message.producerId;
            if (message.sequence != actor.value.nextSequence[producer])
                good = false;
            else
                ++actor.value.nextSequence[producer];
        }
        if (!good) ++actor.value.bad;
        ++actor.value.consumed;
        actor.value.sum += message.value;
        node.complete();
    }
    actor.value.output.consumed = actor.value.consumed;
    actor.value.output.sum = actor.value.sum;
    actor.value.output.bad = actor.value.bad;
    actor.value.output.activations = actor.value.activations;
}

private __gshared shared(size_t) g_stressCommand;
private __gshared shared(size_t) g_stressDone;
private __gshared shared(long) g_stressUnexpected;

private class StressProducer
{
    size_t id;
    ActorHandle!StressState* handles;
    StressMessage* messages;

    this(size_t producerId, ActorHandle!StressState* actorHandles,
            StressMessage* storage)
    {
        id = producerId;
        handles = actorHandles;
        messages = storage;
    }

    void run()
    {
        foreach (round; 1 .. stressRounds + 1)
        {
            while (atomicLoad!(MemoryOrder.acq)(g_stressCommand) < round) {}
            immutable step = 1 + 2 * ((id + round) % 4);
            immutable offset = (id * 3 + round) % stressActorCount;
            foreach (sequence; 0 .. stressMessagesPerActor)
            {
                foreach (j; 0 .. stressActorCount)
                {
                    immutable actor = (offset + step * j)
                        % stressActorCount;
                    auto message = &messages[stressIndex(
                        id, actor, sequence)];
                    if (!accepted(handles[actor].send(&message.node)))
                        atomicFetchAdd!(MemoryOrder.rel)(
                            g_stressUnexpected, 1L);
                }
            }
            atomicFetchAdd!(MemoryOrder.rel)(g_stressDone, 1UL);
        }
    }
}

private class StressConsumer
{
    AntFarm* farm;
    shared int* stop;

    this(AntFarm* actorFarm, shared int* stopFlag)
    {
        farm = actorFarm;
        stop = stopFlag;
    }

    void run()
    {
        ConsumerView view;
        if (view.subscribe(farm) < 0)
            check(false, "stress consumer subscribe");
        while (atomicLoad!(MemoryOrder.acq)(*stop) == 0)
        {
            if (!view.consumeNext()) Thread.yield();
        }
        while (view.consumeNext()) {}
        view.unsubscribe();
    }
}

private void testSustainedContention()
{
    enum consumerCount = 6;
    auto farm = AntFarm.create(4, 8, consumerCount,
        0, 0, 1, 8192);
    check(farm !is null, "stress Farm allocation");
    scope (exit) farm.destroy();
    CountedAllocator counts;
    auto runtime = ActorRuntime.create(farm, stressActorCount,
        countedPolicy(counts));
    check(runtime !is null, "stress actor runtime allocation");

    auto messages = cast(StressMessage*) malloc(
        stressMessageCount * StressMessage.sizeof);
    check(messages !is null, "stress message allocation");
    scope (exit) free(messages);
    memset(messages, 0, stressMessageCount * StressMessage.sizeof);

    ActorOwner!StressState[stressActorCount] owners;
    ActorHandle!StressState[stressActorCount] handles;
    StressResult[stressActorCount] results;
    ulong[stressActorCount] expectedSums;

    shared int stopConsumers;
    StressConsumer[consumerCount] consumerJobs;
    Thread[consumerCount] consumerThreads;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new StressConsumer(farm, &stopConsumers);
        consumerThreads[i] = new Thread(&consumerJobs[i].run);
        consumerThreads[i].start();
    }

    StressProducer[stressProducerCount] producerJobs;
    Thread[stressProducerCount] producerThreads;
    foreach (i; 0 .. stressProducerCount)
    {
        producerJobs[i] = new StressProducer(i, handles.ptr, messages);
        producerThreads[i] = new Thread(&producerJobs[i].run);
        producerThreads[i].start();
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "stress producer registration");
    atomicStore!(MemoryOrder.raw)(g_stressCommand, 0UL);
    atomicStore!(MemoryOrder.raw)(g_stressDone, 0UL);
    atomicStore!(MemoryOrder.raw)(g_stressUnexpected, 0L);

    GC.disable();
    foreach (round; 1 .. stressRounds + 1)
    {
        results[] = StressResult.init;
        expectedSums[] = 0;
        foreach (actor; 0 .. stressActorCount)
        {
            StressState initial;
            initial.actorId = actor;
            initial.output = &results[actor];
            owners[actor] = runtime.createActor!(StressState, stressActor)(
                initial);
            check(owners[actor].valid, "stress actor creation");
            handles[actor] = owners[actor].handle;
        }
        immutable warmAllocations = counts.allocations;

        foreach (producer; 0 .. stressProducerCount)
        {
            foreach (actor; 0 .. stressActorCount)
            {
                foreach (sequence; 0 .. stressMessagesPerActor)
                {
                    auto message = &messages[stressIndex(
                        producer, actor, sequence)];
                    message.actorId = actor;
                    message.producerId = producer;
                    message.sequence = sequence;
                    message.value = stressValue(
                        round, actor, producer, sequence);
                    message.inverse = ~message.value;
                    message.node.initialize(message, message.value);
                    expectedSums[actor] += message.value;
                }
            }
        }

        atomicStore!(MemoryOrder.rel)(g_stressCommand, round);
        auto deadline = MonoTime.currTime + 30.seconds;
        immutable targetDone = round * stressProducerCount;
        while (atomicLoad!(MemoryOrder.acq)(g_stressDone) != targetDone)
        {
            runtime.flush(token, 64, avgCostForChunk(8));
            if (MonoTime.currTime >= deadline)
                check(false, "stress producer timeout");
            Thread.yield();
        }
        foreach (actor; 0 .. stressActorCount)
            check(owners[actor].requestRetire()
                    == ActorRetireResult.requested,
                "stress actor retirement request");

        bool allRetired;
        while (!allRetired)
        {
            runtime.flush(token, 64, avgCostForChunk(8));
            allRetired = true;
            foreach (actor; 0 .. stressActorCount)
                allRetired = allRetired && owners[actor].retired;
            if (MonoTime.currTime >= deadline)
                check(false, "stress retirement timeout");
            Thread.yield();
        }

        foreach (actor; 0 .. stressActorCount)
        {
            check(results[actor].consumed
                    == stressProducerCount * stressMessagesPerActor,
                "stress actor exact consumption");
            check(results[actor].sum == expectedSums[actor],
                "stress actor payload publication checksum");
            check(results[actor].bad == 0,
                "stress actor FIFO and payload integrity");
            check(results[actor].activations != 0,
                "stress actor activated");
        }
        check(atomicLoad!(MemoryOrder.acq)(g_stressUnexpected) == 0,
            "stress sends accepted while admission open");
        check(counts.allocations == warmAllocations,
            "stress warm path has no actor allocations");
        foreach (i; 0 .. stressMessageCount)
            check(messages[i].node.available,
                "stress node returned before generation reuse");
        foreach (actor; 0 .. stressActorCount)
            check(owners[actor].reclaim() == ActorReclaimResult.reclaimed,
                "stress actor reclaim");
        check(runtime.live == 0, "stress generation fully reclaimed");
    }
    GC.enable();

    foreach (thread; producerThreads) thread.join();
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    foreach (thread; consumerThreads) thread.join();
    farm.unregisterProducer(token);
    check(runtime.ready == 0 && runtime.live == 0,
        "stress runtime drained");
    runtime.destroy();
    check(counts.allocations == counts.deallocations,
        "stress actor allocator balance");
    printf("actor sustained contention: %llu rounds, %llu messages OK\n",
        cast(ulong) stressRounds,
        cast(ulong) stressRounds * stressMessageCount);
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Bounded ready snapshots, concurrent drainers, and Farm backpressure
// -------------------------------------------------------------------------

private struct ReadyState
{
    ulong remaining;
    ulong* calls;
}

private void readyActor(scope ref ActorBorrow!ReadyState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++*actor.value.calls;
    if (--actor.value.remaining == 0)
        context.retire();
    else
        context.republish();
}

private class ReadyFlusher
{
    AntFarm* farm;
    ActorRuntime* runtime;
    shared int* stop;
    size_t batch;
    size_t published;

    void run()
    {
        auto token = farm.registerProducer(Tier.small);
        check(token.valid, "ready flusher producer registration");
        scope (exit) farm.unregisterProducer(token);
        do
        {
            published += runtime.flush(token, batch, avgCostForChunk(32));
            if (stop is null) break;
            Thread.yield();
        }
        while (atomicLoad!(MemoryOrder.acq)(*stop) == 0);
    }
}

private extern(C) void readyPadding(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system { return; }

private void testReadyDrainHandoff()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "ready handoff Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 3);
    check(runtime !is null, "ready handoff runtime allocation");
    ActorOwner!ReadyState[3] owners;
    ulong[3] calls;
    foreach (i; 0 .. owners.length)
    {
        owners[i] = runtime.createActor!(ReadyState, readyActor)(
            ReadyState(1, &calls[i]));
        check(owners[i].valid, "ready handoff actor creation");
    }
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "ready handoff producer registration");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "ready handoff subscribe");

    foreach (i; 0 .. 2)
        check(owners[i].handle.wake() == ActorWakeResult.queued,
            "ready handoff wake");
    g_hookNode = null;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.readyDetached);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto flusher = new ReadyFlusher;
    flusher.farm = farm;
    flusher.runtime = runtime;
    flusher.batch = 1;
    auto thread = new Thread(&flusher.run);
    thread.start();
    waitFlag(g_hookArrived, MonoTime.currTime + 15.seconds,
        "ready flusher did not pause after detach");
    check(runtime.ready == 2, "detached backlog remains counted");
    check(runtime.flush(token, 1, avgCostForChunk(32)) == 0,
        "contended ready drain does not wait");
    check(owners[2].handle.wake() == ActorWakeResult.queued,
        "producer publishes while ready drain is held");
    check(runtime.ready == 3, "concurrent wake counted with detached backlog");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    check(flusher.published == 1 && runtime.ready == 2,
        "bounded snapshot leaves carry");
    check(runtime.flush(token, 0, avgCostForChunk(32)) == 0 && runtime.ready == 2,
        "zero maximum preserves carry");
    // The next holder must see both the prior thread's carry and the newer
    // head, and fill one snapshot across that boundary.
    check(runtime.flush(token, 2, avgCostForChunk(32)) == 2 && runtime.ready == 0,
        "ready carry transfers between threads and joins new head");
    auto deadline = MonoTime.currTime + 15.seconds;
    foreach (ref owner; owners)
    {
        while (!owner.retired)
        {
            view.consumeNext();
            check(MonoTime.currTime < deadline,
                "ready handoff consumption timeout");
        }
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "ready handoff reclaim");
    }
    foreach (n; calls) check(n == 1, "ready handoff exact callback count");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
}

private void testReadyBacklog()
{
    enum actorCount = 1024;
    enum flusherCount = 4;
    enum repetitions = 7;
    auto farm = AntFarm.create(2, 4, 2, 0, 0, flusherCount + 1, 512);
    check(farm !is null, "ready backlog Farm allocation");
    scope (exit) farm.destroy();
    CountedAllocator counts;
    auto runtime = ActorRuntime.create(farm, actorCount, countedPolicy(counts));
    check(runtime !is null, "ready backlog runtime allocation");
    ActorOwner!ReadyState[actorCount] owners;
    ulong[actorCount] calls;
    foreach (i; 0 .. actorCount)
    {
        owners[i] = runtime.createActor!(ReadyState, readyActor)(
            ReadyState(repetitions, &calls[i]));
        check(owners[i].valid, "ready backlog actor creation");
        check(owners[i].handle.wake() == ActorWakeResult.queued,
            "ready backlog wake");
    }
    immutable warmAllocations = counts.allocations;
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "ready backlog producer registration");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "ready backlog subscribe");
    check(runtime.flush(token, 0, avgCostForChunk(32)) == 0 && runtime.ready == actorCount,
        "zero maximum preserves head");

    // Withhold consumers until the Farm is full. A deliberately oversized
    // request exercises snapshot clamping and partial table acceptance.
    size_t published = runtime.flush(token, size_t.max, avgCostForChunk(32));
    check(published > 0 && published < 256
            && runtime.ready == actorCount - published,
        "partial table acceptance returns unwritten actors");
    auto deadline = MonoTime.currTime + 30.seconds;
    Callback paddingHeader;

    paddingHeader = &readyPadding;
    ulong[400] paddingBody;
    PayloadEntry[1] padding = [PayloadEntry(paddingHeader, paddingBody[])];
    while (farm.write(padding[], token, avgCostForChunk(32)) != 0)
        check(MonoTime.currTime < deadline, "ready backlog padding timeout");
    while (true)
    {
        immutable before = runtime.ready;
        immutable n = runtime.flush(token, size_t.max, avgCostForChunk(32));
        check(n <= 256 && runtime.ready == before - n,
            "partial flush preserves exact ready count");
        published += n;
        if (n == 0) break;
        check(MonoTime.currTime < deadline, "ready backlog fill timeout");
    }
    check(published > 0 && published < actorCount,
        "ready backlog reaches backpressure");
    foreach (_; 0 .. 3)
    {
        immutable before = runtime.ready;
        check(runtime.flush(token, 1, avgCostForChunk(32)) == 0 && runtime.ready == before,
            "backpressured snapshot is requeued without loss");
    }

    shared int stop;
    ReadyFlusher[flusherCount] jobs;
    Thread[flusherCount] threads;
    foreach (i; 0 .. flusherCount)
    {
        jobs[i] = new ReadyFlusher;
        jobs[i].farm = farm;
        jobs[i].runtime = runtime;
        jobs[i].stop = &stop;
        jobs[i].batch = i == 0 ? 1 : 256 / i;
        threads[i] = new Thread(&jobs[i].run);
    }
    auto consumer = new StressConsumer(farm, &stop);
    auto consumerThread = new Thread(&consumer.run);
    GC.disable();
    foreach (thread; threads) thread.start();
    consumerThread.start();
    foreach (ref owner; owners)
    {
        while (!owner.retired)
        {
            if (!view.consumeNext()) Thread.yield();
            check(MonoTime.currTime < deadline,
                "concurrent ready backlog drain timeout");
        }
    }
    atomicStore!(MemoryOrder.rel)(stop, 1);
    foreach (thread; threads) thread.join();
    consumerThread.join();
    GC.enable();
    foreach (job; jobs) published += job.published;
    check(published == actorCount * repetitions,
        "concurrent flush exact publication count");
    foreach (n; calls)
        check(n == repetitions, "concurrent ready exact callback count");
    check(runtime.ready == 0 && runtime.staleActivations == 0,
        "concurrent ready backlog drained without duplicate activations");
    check(counts.allocations == warmAllocations,
        "bounded ready path allocates nothing");
    foreach (ref owner; owners)
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "ready backlog reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
    check(counts.allocations == counts.deallocations,
        "ready backlog allocator balance");
    printf("actor ready handoff, backlog, and concurrent flushers OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Free-slot allocation failure and concurrent cross-thread reuse
// -------------------------------------------------------------------------

private struct SlotState { ulong marker; }

private void slotActor(scope ref ActorBorrow!SlotState,
        scope ref ActorContext) nothrow @nogc @system {}

private struct FailingSlotAllocator
{
    CountedAllocator counts;
    bool failNext;
}

private void* failingSlotAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    auto state = cast(FailingSlotAllocator*) context;
    if (state.failNext)
    {
        state.failNext = false;
        return null;
    }
    return countedAllocate(&state.counts, bytes, alignment);
}

private void failingSlotDeallocate(void* context, void* memory,
        size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto state = cast(FailingSlotAllocator*) context;
    countedDeallocate(&state.counts, memory, bytes, alignment);
}

private void testSlotAllocationFailure()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    scope (exit) farm.destroy();
    FailingSlotAllocator state;
    auto runtime = ActorRuntime.create(farm, 4,
        ActorAllocator(&state, &failingSlotAllocate, &failingSlotDeallocate));
    check(runtime !is null, "free-slot runtime creation");
    ActorOwner!SlotState[4] owners;
    ActorHandle!SlotState[4] stale;
    foreach (_; 0 .. 8)
    {
        foreach (i; 0 .. owners.length)
        {
            // Repeated failure must return its reserved slot every time.
            foreach (attempt; 0 .. 3)
            {
                state.failNext = true;
                auto failed = runtime.createActor!(SlotState, slotActor)(SlotState.init);
                check(!failed.valid && runtime.live == i,
                    "failed state allocation returns slot without a live actor");
            }
            owners[i] = runtime.createActor!(SlotState, slotActor)(SlotState(i));
            check(owners[i].valid, "free-slot allocation recovers after failure");
        }
        auto extra = runtime.createActor!(SlotState, slotActor)(SlotState.init);
        check(!extra.valid, "free-slot exhaustion is exact");
        foreach (handle; stale)
            check(handle.wake() == ActorWakeResult.staleHandle,
                "failed allocations and reuse preserve stale generations");
        foreach (i, ref owner; owners)
        {
            stale[i] = owner.handle;
            check(owner.requestRetire() == ActorRetireResult.requested,
                "free-slot actor retires");
            check(owner.reclaim() == ActorReclaimResult.reclaimed,
                "free-slot actor reclaimed");
        }
        check(runtime.live == 0, "free-slot generation drained");
    }
    runtime.destroy();
    check(state.counts.allocations == state.counts.deallocations,
        "free-slot failure allocator balance");
}

private class SlotReuseJob
{
    enum batch = 16;
    ActorRuntime* runtime;
    Barrier barrier;
    SlotReuseJob next;
    ActorOwner!SlotState[batch] owners;
    ActorHandle!SlotState[batch] stale;

    void run()
    {
        foreach (round; 0 .. 64)
        {
            foreach (ref owner; owners)
            {
                owner = runtime.createActor!(SlotState, slotActor)(SlotState(round));
                check(owner.valid, "concurrent free-slot allocation");
            }
            barrier.wait();
            auto extra = runtime.createActor!(SlotState, slotActor)(SlotState.init);
            check(!extra.valid, "concurrent free-slot capacity exhaustion");
            foreach (handle; stale)
                check(handle.wake() == ActorWakeResult.staleHandle,
                    "concurrent free-slot stale handle after reuse");
            // Every owner moves to the next thread through the barrier.
            barrier.wait();
            foreach (i, ref owner; next.owners)
            {
                stale[i] = owner.handle;
                check(owner.requestRetire() == ActorRetireResult.requested,
                    "foreign-thread slot retirement");
                check(owner.reclaim() == ActorReclaimResult.reclaimed,
                    "foreign-thread slot reclaim");
            }
            barrier.wait();
            check(runtime.live == 0, "all concurrent slots returned");
            barrier.wait();
        }
    }
}

private void testConcurrentSlotReuse()
{
    enum workers = 8;
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, workers * SlotReuseJob.batch);
    check(runtime !is null, "concurrent free-slot runtime creation");
    auto barrier = new Barrier(workers);
    SlotReuseJob[workers] jobs;
    Thread[workers] threads;
    foreach (ref job; jobs)
    {
        job = new SlotReuseJob;
        job.runtime = runtime;
        job.barrier = barrier;
    }
    foreach (i, job; jobs)
    {
        job.next = jobs[(i + 1) % workers];
        threads[i] = new Thread(&job.run);
    }
    foreach (thread; threads) thread.start();
    foreach (thread; threads) thread.join();
    check(runtime.live == 0 && runtime.ready == 0,
        "concurrent free-slot runtime drained");
    runtime.destroy();
    printf("actor free-slot failure and concurrent cross-thread reuse OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// mimalloc v3 adapter contracts: local ABI stub and pinned real library
// -------------------------------------------------------------------------

version (AntfarmMimallocV3)
{
    private align(128) struct MimallocAlignedState
    {
        ulong marker;
    }

    private void mimallocAlignedActor(
            scope ref ActorBorrow!MimallocAlignedState actor,
            scope ref ActorContext) nothrow @nogc @system
    {
        ++actor.value.marker;
    }
}

version (AntfarmMimallocStub)
{
    private struct MimallocRecord
    {
        void* memory;
        size_t bytes;
        size_t alignment;
    }

    private __gshared MimallocRecord[16] g_miAllocations;
    private __gshared MimallocRecord[16] g_miDeallocations;
    private __gshared size_t g_miAllocationCount;
    private __gshared size_t g_miDeallocationCount;

    extern (C) void* mi_malloc_aligned(size_t bytes, size_t alignment)
        nothrow @nogc @system
    {
        if (alignment == 0 || (alignment & (alignment - 1)) != 0
                || bytes > size_t.max - alignment - (void*).sizeof)
            return null;
        immutable effectiveAlignment = alignment < (void*).alignof
            ? (void*).alignof : alignment;
        auto raw = malloc(bytes + effectiveAlignment + (void*).sizeof);
        if (raw is null) return null;
        immutable address = (cast(size_t) raw + (void*).sizeof
            + effectiveAlignment - 1) & ~(effectiveAlignment - 1);
        auto memory = cast(void*) address;
        (cast(void**) memory)[-1] = raw;
        immutable index = g_miAllocationCount++;
        if (index < g_miAllocations.length)
            g_miAllocations[index] = MimallocRecord(
                memory, bytes, alignment);
        return memory;
    }

    extern (C) void mi_free_size_aligned(void* memory, size_t bytes,
            size_t alignment) nothrow @nogc @system
    {
        immutable index = g_miDeallocationCount++;
        if (index < g_miDeallocations.length)
            g_miDeallocations[index] = MimallocRecord(
                memory, bytes, alignment);
        if (memory !is null)
            free((cast(void**) memory)[-1]);
    }

    private void testMimallocV3AdapterContract()
    {
        g_miAllocationCount = 0;
        g_miDeallocationCount = 0;
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        check(farm !is null, "mimalloc contract Farm allocation");
        scope (exit) farm.destroy();
        auto policy = mimallocV3ActorAllocator();
        check(policy.valid, "mimalloc v3 actor policy");
        auto runtime = ActorRuntime.create(farm, 3, policy);
        check(runtime !is null, "mimalloc v3 runtime allocation");
        auto owner = runtime.createActor!(MimallocAlignedState,
            mimallocAlignedActor)(MimallocAlignedState(7));
        check(owner.valid, "mimalloc v3 actor allocation");
        check(owner.requestRetire() == ActorRetireResult.requested
                && owner.retired,
            "mimalloc v3 idle retirement");
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "mimalloc v3 actor reclaim");
        runtime.destroy();

        check(g_miAllocationCount == 3 && g_miDeallocationCount == 3,
            "mimalloc v3 adapter covers runtime, slots, and state");
        bool sawOverAlignedState;
        foreach (allocation; g_miAllocations[0 .. g_miAllocationCount])
        {
            check(allocation.memory !is null && allocation.bytes != 0,
                "mimalloc v3 receives a nonempty allocation");
            check((cast(size_t) allocation.memory
                    & (allocation.alignment - 1)) == 0,
                "mimalloc v3 allocation alignment");
            sawOverAlignedState = sawOverAlignedState
                || (allocation.bytes == MimallocAlignedState.sizeof
                    && allocation.alignment == MimallocAlignedState.alignof);
            bool matched;
            foreach (deallocation;
                    g_miDeallocations[0 .. g_miDeallocationCount])
                matched = matched
                    || (deallocation.memory == allocation.memory
                        && deallocation.bytes == allocation.bytes
                        && deallocation.alignment == allocation.alignment);
            check(matched,
                "mimalloc v3 sized free preserves pointer, size, alignment");
        }
        check(sawOverAlignedState,
            "mimalloc v3 policy supports actor state aligned beyond 64 bytes");
        printf("mimalloc v3 actor adapter contract OK\n");
        fflush(stdout);
    }
}

version (AntfarmMimallocReal)
{
    private enum int pinnedMimallocVersion = 30_503;
    private enum size_t crossThreadActorCount = 512;
    private enum size_t crossThreadReclaimerCount = 8;
    private enum size_t crossThreadRounds = 16;

    extern (C) int mi_version() nothrow @nogc @system;

    private class MimallocReclaimJob
    {
        ActorOwner!MimallocAlignedState* owners;
        size_t first;
        size_t pastLast;
        shared(size_t)* reclaimed;
        shared(long)* failures;

        this(ActorOwner!MimallocAlignedState* actorOwners,
                size_t begin, size_t end, shared(size_t)* reclaimedCount,
                shared(long)* failureCount)
        {
            owners = actorOwners;
            first = begin;
            pastLast = end;
            reclaimed = reclaimedCount;
            failures = failureCount;
        }

        void run()
        {
            foreach (i; first .. pastLast)
            {
                if (owners[i].reclaim() == ActorReclaimResult.reclaimed)
                    atomicFetchAdd!(MemoryOrder.rel)(*reclaimed, 1UL);
                else
                    atomicFetchAdd!(MemoryOrder.rel)(*failures, 1L);
            }
        }
    }

    private class MimallocDestroyJob
    {
        ActorRuntime* runtime;
        shared int done;

        this(ActorRuntime* actorRuntime)
        {
            runtime = actorRuntime;
        }

        void run()
        {
            runtime.destroy();
            runtime = null;
            atomicStore!(MemoryOrder.rel)(done, 1);
        }
    }

    private void testPinnedMimallocCrossThreadFree()
    {
        check(mi_version() == pinnedMimallocVersion,
            "real actor lane linked pinned mimalloc v3.5.3");
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        check(farm !is null, "real mimalloc Farm allocation");
        scope (exit) farm.destroy();
        auto policy = mimallocV3ActorAllocator();
        check(policy.valid, "real mimalloc actor policy");

        ActorOwner!MimallocAlignedState[crossThreadActorCount] owners;
        foreach (round; 0 .. crossThreadRounds)
        {
            // Runtime, stable slots, and every 128-byte-aligned state are
            // allocated by this main thread.
            auto runtime = ActorRuntime.create(
                farm, crossThreadActorCount, policy);
            check(runtime !is null, "real mimalloc runtime allocation");
            foreach (i; 0 .. crossThreadActorCount)
            {
                owners[i] = runtime.createActor!(MimallocAlignedState,
                    mimallocAlignedActor)(MimallocAlignedState(round + i));
                check(owners[i].valid,
                    "real mimalloc cross-thread actor allocation");
                check(owners[i].requestRetire()
                        == ActorRetireResult.requested
                        && owners[i].retired,
                    "real mimalloc cross-thread idle retirement");
            }

            shared size_t reclaimed;
            shared long failures;
            MimallocReclaimJob[crossThreadReclaimerCount] jobs;
            Thread[crossThreadReclaimerCount] threads;
            foreach (worker; 0 .. crossThreadReclaimerCount)
            {
                immutable first = worker * crossThreadActorCount
                    / crossThreadReclaimerCount;
                immutable pastLast = (worker + 1) * crossThreadActorCount
                    / crossThreadReclaimerCount;
                jobs[worker] = new MimallocReclaimJob(owners.ptr,
                    first, pastLast, &reclaimed, &failures);
                threads[worker] = new Thread(&jobs[worker].run);
                threads[worker].start();
            }
            foreach (thread; threads) thread.join();
            check(atomicLoad!(MemoryOrder.acq)(reclaimed)
                    == crossThreadActorCount
                    && atomicLoad!(MemoryOrder.acq)(failures) == 0,
                "foreign threads reclaimed every mimalloc actor state");
            check(runtime.live == 0 && runtime.ready == 0,
                "real mimalloc runtime quiescent before foreign destroy");

            // A ninth foreign thread frees the slot array and runtime object,
            // so all three ActorAllocator allocation categories cross threads.
            auto destroyJob = new MimallocDestroyJob(runtime);
            auto destroyThread = new Thread(&destroyJob.run);
            destroyThread.start();
            destroyThread.join();
            check(atomicLoad!(MemoryOrder.acq)(destroyJob.done) == 1,
                "foreign thread destroyed mimalloc actor runtime");
        }

        printf("pinned mimalloc v3.5.3 cross-thread free: %llu states OK\n",
            cast(ulong) crossThreadRounds * crossThreadActorCount);
        fflush(stdout);
    }
}

// -------------------------------------------------------------------------
// Public inbox-node outcomes (ActorNodeOutcome, fail, cancel)
// -------------------------------------------------------------------------

private enum NodeAction : uint { complete, fail, cancel }

private struct NodeOutcomeMessage
{
    ActorInboxNode node;
    uint action;
    uint code;
}

private struct NodeOutcomeState
{
    shared int* handled;
}

private void nodeOutcomeActor(scope ref ActorBorrow!NodeOutcomeState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    while (true)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto message = cast(NodeOutcomeMessage*) node.payload;
        // Count before the terminal call: after it the sender owns the node
        // and may rewrite the message.
        atomicFetchAdd!(MemoryOrder.rel)(*actor.value.handled, 1);
        immutable action = message.action;
        immutable code = message.code;
        if (action == NodeAction.fail) node.fail(code);
        else if (action == NodeAction.cancel) node.cancel();
        else node.complete();
    }
}

/// One actor, one consumer thread. The test thread is the only sender and
/// the only flusher, so it can read an outcome the moment `available` flips
/// without joining or otherwise synchronizing with the consumer thread.
private final class NodeOutcomeRig
{
    AntFarm* farm;
    ActorRuntime* runtime;
    Token token;
    ActorOwner!NodeOutcomeState owner;
    ActorHandle!NodeOutcomeState handle;
    shared int handled;
    shared int stop;
    Thread consumer;

    this()
    {
        farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        check(farm !is null, "node outcome Farm allocation");
        runtime = ActorRuntime.create(farm, 1);
        check(runtime !is null, "node outcome runtime allocation");
        token = farm.registerProducer(Tier.small);
        check(token.valid, "node outcome producer registration");
        owner = runtime.createActor!(NodeOutcomeState, nodeOutcomeActor)(
            NodeOutcomeState(&handled));
        check(owner.valid, "node outcome actor creation");
        handle = owner.handle;
        auto job = new StressConsumer(farm, &stop);
        consumer = new Thread(&job.run);
        consumer.start();
    }

    void send(ref NodeOutcomeMessage message, NodeAction action, uint code)
    {
        message.action = action;
        message.code = code;
        check(accepted(handle.send(&message.node)),
            "node outcome interaction accepted");
        runtime.flush(token, 8, DEFAULT_AVG_COST);
    }

    /// Wait for completion by polling `available` on the sending thread
    /// only. The consumer thread is deliberately left running and unjoined.
    void awaitAvailable(ref NodeOutcomeMessage message, MonoTime deadline)
    {
        size_t spins;
        while (!message.node.available)
        {
            if ((++spins & 0xfff) == 0)
            {
                runtime.flush(token, 8, DEFAULT_AVG_COST);
                if (MonoTime.currTime >= deadline)
                    check(false, "node outcome completion timeout");
                Thread.yield();
            }
        }
    }

    void roundTrip(ref NodeOutcomeMessage message, NodeAction action,
            uint code, MonoTime deadline)
    {
        send(message, action, code);
        awaitAvailable(message, deadline);
    }

    void close()
    {
        auto deadline = MonoTime.currTime + 15.seconds;
        check(owner.requestRetire() == ActorRetireResult.requested,
            "node outcome retirement");
        while (!owner.retired)
        {
            runtime.flush(token, 8, DEFAULT_AVG_COST);
            if (MonoTime.currTime >= deadline)
                check(false, "node outcome retirement timeout");
            Thread.yield();
        }
        atomicStore!(MemoryOrder.rel)(stop, 1);
        consumer.join();
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "node outcome reclaim");
        farm.unregisterProducer(token);
        runtime.destroy();
        farm.destroy();
    }
}

private struct NodeStep
{
    NodeAction action;
    uint code;
    ActorNodeOutcome outcome;
    uint expectedCode;
}

private immutable NodeStep[5] nodeSteps = [
    NodeStep(NodeAction.complete, 0, ActorNodeOutcome.completed, 0),
    NodeStep(NodeAction.fail, 7, ActorNodeOutcome.failed, 7),
    NodeStep(NodeAction.cancel, 0, ActorNodeOutcome.cancelled, 0),
    NodeStep(NodeAction.fail, 0, ActorNodeOutcome.failed, 0),
    NodeStep(NodeAction.fail, 0xFF_FFFF, ActorNodeOutcome.failed,
        0xFF_FFFF),
];

/// P1 (reuse): every ordered pair of {complete, fail(7), cancel, fail(0),
/// fail(0xFFFFFF)} on one reused node. Each outcome is read on the sender
/// thread straight after `available`, before anything joins the consumer.
///
/// Mutations caught:
///  - complete()/cancel() skip the completion_ write: a failed->completed or
///    failed->cancelled pair shows the stale outcome or code.
///  - cancel()/complete() write the outcome byte but not the whole word
///    (stale code bits survive): the failed(7|0xFFFFFF)->cancelled and
///    ->completed pairs show a nonzero failureCode.
///  - completion_ updated with |= instead of =: any pair whose second step
///    has fewer set bits than its first.
///  - fail() omits the outcome byte when the code is 0: fail(0) reads back
///    as completed.
///  - fail() shifts or masks the code to fewer than 24 bits: the 0xFFFFFF
///    steps read back truncated.
private void testNodeOutcomeTransitions()
{
    auto rig = new NodeOutcomeRig;
    NodeOutcomeMessage message;
    memset(&message, 0xff, message.sizeof);
    message.node.initialize(&message, 0x1234);
    // initialize resets the word even over arbitrary storage contents.
    check(message.node.outcome == ActorNodeOutcome.completed
            && message.node.failureCode == 0,
        "initialize clears the completion word over dirty storage");

    auto deadline = MonoTime.currTime + 60.seconds;
    foreach (first; nodeSteps)
    {
        foreach (second; nodeSteps)
        {
            rig.roundTrip(message, first.action, first.code, deadline);
            check(message.node.outcome == first.outcome
                    && message.node.failureCode == first.expectedCode,
                "first outcome visible after available");
            rig.roundTrip(message, second.action, second.code, deadline);
            check(message.node.outcome == second.outcome
                    && message.node.failureCode == second.expectedCode,
                "second outcome replaces the first, code cleared");
            check(message.node.payload is &message
                    && message.node.tag == 0x1234,
                "outcome publication leaves application fields intact");
        }
    }
    check(atomicLoad!(MemoryOrder.acq)(rig.handled)
            == 2 * nodeSteps.length * nodeSteps.length,
        "every interaction reached the actor exactly once");

    // initialize on a node that last failed resets the word.
    rig.roundTrip(message, NodeAction.fail, 0xFF_FFFF, deadline);
    check(message.node.failureCode == 0xFF_FFFF,
        "failure code staged before re-initialize");
    message.node.initialize(&message, 0x1234);
    // Mutation caught: initialize() leaves completion_ untouched.
    check(message.node.outcome == ActorNodeOutcome.completed
            && message.node.failureCode == 0,
        "initialize clears the completion word of a failed node");

    rig.close();
}

/// P1 (deterministic ordering). The actor thread is parked at
/// `actorNodeBeforeCompletionStore`, which fires after the plain completion_
/// write and before the release store of FREE. In correct code the node is
/// therefore still unavailable while parked. After release the sender sees
/// `available` and must read the new outcome on the sender thread, with no
/// join in between, over a stale outcome left by the previous interaction.
///
/// Mutations caught:
///  - M1: release FREE; hook; completion_ write (the write moved after the
///    release, hook kept between them). The node is available while the
///    actor is parked, so the `!available` check fails deterministically
///    (and the stale outcome would be read if it continued).
///  - M1': hook; release FREE; completion_ write (hook left before FREE).
///    Not deterministic here; caught by the TSan lane of the handoff stress
///    test, where the post-acquire read races the late plain write.
private void testNodeCompletionStoreOrdering(NodeAction staleAction,
        uint staleCode, NodeAction action, uint code,
        ActorNodeOutcome outcome)
{
    auto rig = new NodeOutcomeRig;
    NodeOutcomeMessage message;
    message.node.initialize(&message, 0xa5a5);
    auto deadline = MonoTime.currTime + 30.seconds;

    // Leave a stale, different outcome behind from an earlier interaction.
    rig.roundTrip(message, staleAction, staleCode, deadline);
    check(message.node.outcome != outcome || message.node.failureCode != code,
        "stale outcome differs from the expected outcome");

    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.actorNodeBeforeCompletionStore);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);

    rig.send(message, action, code);
    while (atomicLoad!(MemoryOrder.acq)(g_hookArrived) == 0)
    {
        rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
        if (MonoTime.currTime >= deadline)
            check(false, "actor did not reach the completion-store seam");
        Thread.yield();
    }
    // The completion word is written but FREE is not yet published.
    foreach (_; 0 .. 1000)
    {
        check(!message.node.available,
            "node published FREE before its completion word (M1)");
        Thread.yield();
    }

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    rig.awaitAvailable(message, deadline);
    // Read on the sending thread right after the acquire; the consumer
    // thread is still running and has not been joined.
    check(message.node.outcome == outcome
            && message.node.failureCode == code,
        "outcome visible after available, not the stale one");

    setActorTestHook(null);
    g_hookNode = null;
    rig.close();
}

/// P1 (stress). Ping-pong one node through all three terminals with a
/// varying code; the sender reads immediately after observing FREE.
///
/// Mutation caught: M1' (hook; release FREE; completion_ write). The late
/// plain write races the sender's post-acquire read, which TSan reports and
/// which also surfaces as a stale outcome on a fast interleaving.
private void testNodeOutcomeHandoffStress()
{
    auto rig = new NodeOutcomeRig;
    NodeOutcomeMessage message;
    message.node.initialize(&message, 0);
    auto deadline = MonoTime.currTime + 120.seconds;
    enum rounds = 20_000;
    foreach (round; 0 .. rounds)
    {
        immutable action = cast(NodeAction)(round % 3);
        immutable code = action == NodeAction.fail
            ? cast(uint)((round * 2_654_435_761UL) & 0xFF_FFFF) : 0;
        rig.roundTrip(message, action, code, deadline);
        immutable outcome = action == NodeAction.fail
            ? ActorNodeOutcome.failed
            : action == NodeAction.cancel
                ? ActorNodeOutcome.cancelled : ActorNodeOutcome.completed;
        if (message.node.outcome != outcome
                || message.node.failureCode != code)
            check(false, "stress outcome mismatch after available");
    }
    check(atomicLoad!(MemoryOrder.acq)(rig.handled) == rounds,
        "stress interactions each handled once");
    rig.close();
}

private void testNodeOutcomes()
{
    testNodeOutcomeTransitions();
    testNodeCompletionStoreOrdering(NodeAction.fail, 7,
        NodeAction.complete, 0, ActorNodeOutcome.completed);
    testNodeCompletionStoreOrdering(NodeAction.cancel, 0,
        NodeAction.fail, 9, ActorNodeOutcome.failed);
    testNodeCompletionStoreOrdering(NodeAction.fail, 7,
        NodeAction.cancel, 0, ActorNodeOutcome.cancelled);
    testNodeCompletionStoreOrdering(NodeAction.cancel, 0,
        NodeAction.fail, 0xFF_FFFF, ActorNodeOutcome.failed);
    testNodeOutcomeHandoffStress();
    printf("actor node outcomes OK\n");
    fflush(stdout);
}

private struct OwnerProbeState
{
    ulong* activations;
}

private void ownerProbeActor(scope ref ActorBorrow!OwnerProbeState actor,
        scope ref ActorContext) nothrow @nogc @system
{
    ++*actor.value.activations;
}

private void testOwnerMoveOnly()
{
    // Copy and lvalue assignment must not compile: a range algorithm that
    // takes elements by value (owners.all!(o => o.retired)) silently
    // invalidated owners[0] under the old transfer-on-copy owners.
    static assert(!__traits(compiles,
    {
        ActorOwner!OwnerProbeState src;
        ActorOwner!OwnerProbeState dst = src;
    }));
    static assert(!__traits(compiles,
    {
        ActorOwner!OwnerProbeState src;
        ActorOwner!OwnerProbeState dst;
        dst = src;
    }));
    static assert(!__traits(compiles,
    {
        void takesByValue(T)(T taken) {}
        ActorOwner!OwnerProbeState src;
        takesByValue(src);
    }));
    static assert(!__traits(compiles,
    {
        ActorErasedOwner src;
        ActorErasedOwner dst = src;
    }));
    // Transfer is explicit: move into a local or an invalid destination,
    // for both the typed and the erased owner.
    static assert(__traits(compiles,
    {
        ActorOwner!OwnerProbeState src;
        ActorOwner!OwnerProbeState dst = move(src);
        ActorOwner!OwnerProbeState reassigned;
        reassigned = move(src);
        ActorErasedOwner erasedSrc;
        ActorErasedOwner erasedDst = move(erasedSrc);
    }));

    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "move-only owner Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "move-only owner runtime allocation");
    OwnerProbeState output;
    ulong activations;
    output.activations = &activations;
    auto owner = runtime.createActor!(OwnerProbeState, ownerProbeActor)(
        output);
    check(owner.valid, "move-only owner creation");

    ActorOwner!OwnerProbeState moved = move(owner);
    check(!owner.valid, "move consumes the source owner");
    check(moved.valid, "moved-into owner stays valid");
    check(!moved.retired, "moved owner still addresses its actor");

    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "move-only owner subscribe");
    auto handle = moved.handle;
    check(handle.wake() == ActorWakeResult.queued,
        "wake through the moved owner's handle");
    while (activations == 0)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
    }
    check(moved.requestRetire() == ActorRetireResult.requested,
        "retire through a moved owner");
    while (!moved.retired)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
    }
    check(moved.reclaim() == ActorReclaimResult.reclaimed,
        "reclaim through a moved owner");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
    printf("actor move-only owners OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Request/reply completion with an integrated sender wake
// -------------------------------------------------------------------------

private struct RequestReplyResult
{
    int roundsDone;
    int errors;
    int activationsWithoutReply;
    uint wakeResults; // bitmask of the wake results the replier observed
}

private struct RequestReplySenderState
{
    RequestReplyMessage* message;
    ActorHandle!RequestReplyReplierState replier;
    RequestReplyResult* output;
}

private struct RequestReplyReplierState
{
    RequestReplyResult* output;
}

private struct RequestReplyMessage
{
    ActorInboxNode node;
    ActorHandle!RequestReplySenderState sender;
    long request;
    long reply;
    int sentSeen;
    ActorNodeOutcome expected;
}

private void requestReplySender(
        scope ref ActorBorrow!RequestReplySenderState actor,
        scope ref ActorContext) nothrow @nogc @system
{
    RequestReplyMessage* message = actor.value.message;
    RequestReplyResult* output = actor.value.output;
    if (!message.sentSeen)
    {
        immutable result = actor.value.replier.send(&message.node);
        if (result == ActorSendResult.queued
                || result == ActorSendResult.coalesced)
            message.sentSeen = 1;
        else
            ++output.errors;
        // No republish: every later activation of this actor must come
        // from the replier's completeAndWake/failAndWake/cancelAndWake.
        return;
    }
    if (!message.node.available)
    {
        ++output.activationsWithoutReply;
        return;
    }
    if (message.node.outcome != message.expected)
        ++output.errors;
    else if (message.expected == ActorNodeOutcome.completed
            && message.reply != message.request + 1)
        ++output.errors;
    else if (message.expected == ActorNodeOutcome.failed
            && message.node.failureCode != 7)
        ++output.errors;
    message.sentSeen = 0;
    ++output.roundsDone;
}

private void requestReplyReplier(
        scope ref ActorBorrow!RequestReplyReplierState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    auto node = context.popInbox();
    if (node is null) return;
    RequestReplyMessage* message = node.payloadAs!RequestReplyMessage;
    ActorWakeResult woken;
    final switch (message.expected)
    {
    case ActorNodeOutcome.completed:
        message.reply = message.request + 1;
        woken = node.completeAndWake(message.sender);
        break;
    case ActorNodeOutcome.failed:
        woken = node.failAndWake(message.sender, 7);
        break;
    case ActorNodeOutcome.cancelled:
        woken = node.cancelAndWake(message.sender);
        break;
    }
    actor.value.output.wakeResults |= 1U << woken;
}

private void testRequestReplyWake()
{
    // The completion-with-wake variants accept both handle forms and
    // reject types that are not actor handles.
    static assert(__traits(compiles,
    {
        RequestReplyMessage message;
        ActorHandle!RequestReplySenderState handle;
        message.node.completeAndWake(handle);
        message.node.completeAndWake(handle.erased);
        message.node.failAndWake(handle, 1);
        message.node.failAndWake(handle.erased, 1);
        message.node.cancelAndWake(handle);
        message.node.cancelAndWake(handle.erased);
    }));
    static assert(!__traits(compiles,
    {
        struct NotAHandle { void wake() {} }
        RequestReplyMessage message;
        NotAHandle notAHandle;
        message.node.completeAndWake(notAHandle);
    }));

    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "request/reply Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 2);
    check(runtime !is null, "request/reply runtime allocation");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "request/reply producer registration");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "request/reply subscribe");

    RequestReplyResult output;
    RequestReplyMessage message;
    message.node.initialize(&message, 0);
    auto replierOwner =
        runtime.createActor!(RequestReplyReplierState, requestReplyReplier)(
            RequestReplyReplierState(&output));
    auto senderOwner =
        runtime.createActor!(RequestReplySenderState, requestReplySender)(
            RequestReplySenderState(&message, replierOwner.handle, &output));
    check(replierOwner.valid && senderOwner.valid,
        "request/reply actor creation");
    message.sender = senderOwner.handle;
    auto senderHandle = senderOwner.handle;

    immutable long[3] requests = [41, -1, 0];
    immutable ActorNodeOutcome[3] outcomes = [
        ActorNodeOutcome.completed, ActorNodeOutcome.failed,
        ActorNodeOutcome.cancelled];
    foreach (round; 0 .. 3)
    {
        message.node.initialize(&message, cast(ulong) round);
        message.request = requests[round];
        message.expected = outcomes[round];
        check(senderHandle.wake() == ActorWakeResult.queued,
            "request/reply round wake");
        auto deadline = MonoTime.currTime + 15.seconds;
        while (output.roundsDone == round)
        {
            runtime.flush(token, 8, DEFAULT_AVG_COST);
            while (view.consumeNext()) {}
            check(MonoTime.currTime < deadline,
                "request/reply round timeout");
            Thread.yield();
        }
    }
    check(output.errors == 0, "request/reply outcomes and replies");
    check(output.activationsWithoutReply == 0,
        "completeAndWake scheduled the sender only after completion");
    check(output.wakeResults == 1U << ActorWakeResult.queued,
        "request/reply wake results");

    check(replierOwner.requestRetire() == ActorRetireResult.requested,
        "request/reply replier retirement");
    check(senderOwner.requestRetire() == ActorRetireResult.requested,
        "request/reply sender retirement");
    while (!replierOwner.retired || !senderOwner.retired)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
    }
    check(replierOwner.reclaim() == ActorReclaimResult.reclaimed,
        "request/reply replier reclaim");
    check(senderOwner.reclaim() == ActorReclaimResult.reclaimed,
        "request/reply sender reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    runtime.destroy();
    printf("actor request/reply completeAndWake OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// F4b public actor cancel-mode retirement
// -------------------------------------------------------------------------

private struct CancelProbeResult
{
    ulong activations;       // autonomous handler invocations
    ulong cancelActivations; // invocations that observed context.cancelling
    ulong cancelledNodes;    // nodes cancelled through cancelInbox
    ulong completedNodes;    // nodes completed during drain activations
    ulong waveCalls;         // wave operation invocations
}

private struct CancelProbeState
{
    CancelProbeResult* output;
    shared int* activationArrived; // signalled after the budgeted pops
    shared int* activationRelease; // gates the drain activation's return
    ulong popBudget;               // zero: pop everything per activation
}

/// Branch on `context.cancelling` exactly as the design prescribes: cancel
/// the whole inbox (including any carried remainder) or drain by popping
/// and completing, honouring an optional per-activation pop budget so a
/// first activation can deliberately strand a carry.
private void cancelProbeActor(scope ref ActorBorrow!CancelProbeState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    auto output = actor.value.output;
    ++output.activations;
    if (context.cancelling)
    {
        ++output.cancelActivations;
        output.cancelledNodes += context.cancelInbox();
        return;
    }
    immutable budget = actor.value.popBudget == 0
        ? ulong.max : actor.value.popBudget;
    foreach (_; 0 .. budget)
    {
        auto node = context.popInbox();
        if (node is null) break;
        node.complete();
        ++output.completedNodes;
    }
    if (actor.value.activationArrived !is null)
    {
        atomicStore!(MemoryOrder.rel)(*actor.value.activationArrived, 1);
        while (atomicLoad!(MemoryOrder.acq)(
                *actor.value.activationRelease) == 0) {}
    }
}

private void cancelProbeWaveOperation(
        scope ref ActorBorrow!CancelProbeState actor) nothrow @nogc @system
{
    ++actor.value.output.waveCalls;
}

private struct CancelMessage
{
    ActorInboxNode node;
    ulong value;
}

private final class CancelSender
{
    ActorHandle!CancelProbeState handle;
    ActorInboxNode* node;
    ActorSendResult result;
    shared int done;

    this(ActorHandle!CancelProbeState initialHandle,
            ActorInboxNode* initialNode)
    {
        handle = initialHandle;
        node = initialNode;
    }

    void run()
    {
        result = handle.send(node);
        atomicStore!(MemoryOrder.rel)(done, 1);
    }
}

/// One actor, one producer token, and a subscribed view driven by the
/// calling thread, matching the deterministic-interleaving rigs above.
private final class CancelRig
{
    AntFarm* farm;
    ActorRuntime* runtime;
    Token token;
    ActorOwner!CancelProbeState owner;
    ActorHandle!CancelProbeState handle;
    ConsumerView view;
    CancelProbeResult output;

    this(size_t capacity = 1, uint farmConsumers = 1,
            shared int* activationArrived = null,
            shared int* activationRelease = null, ulong popBudget = 0)
    {
        farm = AntFarm.create(2, 4, farmConsumers, 0, 0, 1, 256);
        check(farm !is null, "cancel probe Farm allocation");
        runtime = ActorRuntime.create(farm, capacity);
        check(runtime !is null, "cancel probe runtime allocation");
        owner = runtime.createActor!(CancelProbeState, cancelProbeActor)(
            CancelProbeState(&output, activationArrived, activationRelease,
                popBudget));
        check(owner.valid, "cancel probe actor allocation");
        handle = owner.handle;
        token = farm.registerProducer(Tier.small);
        check(token.valid, "cancel probe producer registration");
        check(view.subscribe(farm) >= 0, "cancel probe subscribe");
    }

    void drain(MonoTime deadline, const(char)* failure)
    {
        while (!owner.retired)
        {
            runtime.flush(token, 8, DEFAULT_AVG_COST);
            while (view.consumeNext()) {}
            if (MonoTime.currTime >= deadline)
                check(false, failure);
            Thread.yield();
        }
    }

    void close()
    {
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "cancel probe reclaim");
        view.unsubscribe();
        farm.unregisterProducer(token);
        check(runtime.live == 0 && runtime.ready == 0,
            "cancel probe runtime drained");
        runtime.destroy();
        farm.destroy();
    }
}

private void sendCancelMessages(ActorHandle!CancelProbeState handle,
        CancelMessage[] messages)
{
    foreach (i, ref message; messages)
    {
        message.value = i;
        message.node.initialize(&message, i);
        check(accepted(handle.send(&message.node)),
            "cancel-mode interaction accepted");
    }
}

/// P2 drain control. The identical 32-node backlog retired with the default
/// drain mode must complete every node, and the handler must never observe
/// `cancelling`. Catches `cancelling` reading retireBit instead of cancelBit:
/// the handler would then cancelInbox a merely-draining actor.
private void testCancelBacklogDrainControl()
{
    enum nodeCount = 32;
    auto rig = new CancelRig;
    CancelMessage[nodeCount] messages;
    sendCancelMessages(rig.handle, messages[]);
    check(rig.owner.requestRetire() == ActorRetireResult.requested,
        "drain-control retirement");
    rig.drain(MonoTime.currTime + 15.seconds,
        "drain-control retirement timeout");
    check(rig.output.completedNodes == nodeCount
            && rig.output.cancelledNodes == 0,
        "drain retirement completes the whole backlog");
    check(rig.output.cancelActivations == 0,
        "drain retirement never reports cancelling");
    foreach (message; messages)
        check(message.node.available
                && message.node.outcome == ActorNodeOutcome.completed,
            "drain-control sender observes completed");
    rig.close();
}

/// P2. A 32-node backlog queued before requestRetire(cancel) must be
/// cancelled by the handler's cancelInbox in a single cancelling activation;
/// every sender then reads `cancelled` after `available`. Catches
/// `cancelling` stuck false (the backlog would complete instead).
private void testCancelBacklog()
{
    enum nodeCount = 32;
    auto rig = new CancelRig;
    CancelMessage[nodeCount] messages;
    sendCancelMessages(rig.handle, messages[]);
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel-backlog retirement");
    check(!rig.owner.retired, "cancel backlog waits for the queued nodes");
    rig.drain(MonoTime.currTime + 15.seconds,
        "cancel-backlog retirement timeout");
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == nodeCount
            && rig.output.completedNodes == 0,
        "cancelInbox cancels exactly the whole backlog");
    foreach (message; messages)
        check(message.node.available
                && message.node.outcome == ActorNodeOutcome.cancelled,
            "cancel-backlog sender observes cancelled");
    rig.close();
}

/// P2 carry. The first activation pops and completes one node and returns
/// with the remaining two as the actor-private carry. After the cancel
/// request, the next activation's cancelInbox must cancel the carried
/// remainder, not just a freshly published inbox.
private void testCancelInboxCarry()
{
    auto rig = new CancelRig(1, 1, null, null, 1);
    CancelMessage[3] messages;
    sendCancelMessages(rig.handle, messages[]);
    auto deadline = MonoTime.currTime + 15.seconds;
    rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
    check(rig.view.consumeNext(), "cancel carry first activation");
    check(rig.output.completedNodes == 1,
        "budgeted activation completes exactly one node");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel-carry retirement");
    rig.drain(deadline, "cancel-carry retirement timeout");
    check(rig.output.cancelledNodes == 2 && rig.output.completedNodes == 1,
        "cancelInbox includes the carried remainder");
    check(messages[0].node.outcome == ActorNodeOutcome.completed
            && messages[1].node.outcome == ActorNodeOutcome.cancelled
            && messages[2].node.outcome == ActorNodeOutcome.cancelled,
        "carry outcomes split by activation");
    rig.close();
}

/// P3. Sequential drain-then-cancel escalation on a queued backlog: the
/// cancel request returns requested, a repeated cancel is alreadyRetired,
/// a later drain stays requested without clearing the cancel mode, and the
/// next handler activation observes cancelling. Catches markActorRetiring
/// early-returning once retireBit is set: the escalation would be silently
/// lost and the backlog completed instead of cancelled.
private void testCancelEscalationQueued()
{
    enum nodeCount = 4;
    auto rig = new CancelRig;
    CancelMessage[nodeCount] messages;
    sendCancelMessages(rig.handle, messages[]);
    check(rig.owner.requestRetire() == ActorRetireResult.requested,
        "drain request before escalation");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel escalation returns requested");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.alreadyRetired,
        "repeated cancel is alreadyRetired");
    check(rig.owner.requestRetire() == ActorRetireResult.requested,
        "drain after cancel stays requested");
    rig.drain(MonoTime.currTime + 15.seconds,
        "cancel escalation retirement timeout");
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == nodeCount
            && rig.output.completedNodes == 0,
        "escalated cancel observed after the drain request");
    foreach (message; messages)
        check(message.node.available
                && message.node.outcome == ActorNodeOutcome.cancelled,
            "escalation cancels the queued backlog");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.alreadyRetired,
        "cancel after RETIRED is alreadyRetired");
    check(rig.owner.requestRetire() == ActorRetireResult.alreadyRetired,
        "drain after RETIRED is alreadyRetired");
    rig.close();
}

/// Erased owner. requestRetire(cancel) through ActorErasedOwner must forward
/// the mode: the handler sees cancelling and cancels the backlog. With
/// `escalate`, a drain request precedes the cancel and a later drain leaves
/// the actor cancelling. Catches the erased owner dropping the mode.
private void testCancelThroughErasedOwner(bool escalate)
{
    enum nodeCount = 4;
    auto rig = new CancelRig;
    CancelMessage[nodeCount] messages;
    sendCancelMessages(rig.handle, messages[]);
    auto erased = rig.owner.intoErased();
    check(erased.valid && !rig.owner.valid, "erased owner transfer");
    if (escalate)
        check(erased.requestRetire() == ActorRetireResult.requested,
            "erased drain request before escalation");
    check(erased.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "erased cancel request");
    check(erased.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.alreadyRetired,
        "erased repeated cancel is alreadyRetired");
    if (escalate)
        check(erased.requestRetire() == ActorRetireResult.requested,
            "erased drain after cancel stays requested");
    auto deadline = MonoTime.currTime + 15.seconds;
    while (!erased.retired)
    {
        rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
        while (rig.view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "erased cancel retirement timeout");
        Thread.yield();
    }
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == nodeCount
            && rig.output.completedNodes == 0,
        "erased owner forwards the cancel mode to the handler");
    foreach (message; messages)
        check(message.node.available
                && message.node.outcome == ActorNodeOutcome.cancelled,
            "erased cancel sender observes cancelled");
    check(erased.reclaim() == ActorReclaimResult.reclaimed,
        "erased cancel reclaim");
    rig.view.unsubscribe();
    rig.farm.unregisterProducer(rig.token);
    check(rig.runtime.live == 0 && rig.runtime.ready == 0,
        "erased cancel runtime drained");
    rig.runtime.destroy();
    rig.farm.destroy();
}

/// P3, consumer-entry side. The sender is paused after its activation
/// signal (node published, actor scheduled, reservation still held) while
/// the owner sequentially requests drain and then cancel. The scheduled
/// entry must carry the escalated mode into the handler.
private void testCancelEscalationAfterSignal()
{
    auto rig = new CancelRig;
    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.activationSignalled);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto sender = new CancelSender(rig.handle, &message.node);
    auto thread = new Thread(&sender.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "escalation sender did not pause after its activation signal");
    check(rig.owner.requestRetire() == ActorRetireResult.requested,
        "drain request with a signalled activation");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel escalation while the activation is scheduled");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "escalation sender remains accepted");
    rig.drain(deadline, "escalation after signal retirement timeout");
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == 1
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "entry after escalation observes cancelling");
    rig.close();
}

/// P3, idle-join side. A drain request parked at retirementIdleObserved
/// while an accepted sender still holds its reservation; the sender's late
/// signal re-schedules the actor, the retirement re-observes the lifecycle,
/// and the owner then escalates to cancel. The escalated mode must reach
/// the handler across both transitions.
private void testCancelEscalationAcrossIdleJoin()
{
    auto rig = new CancelRig;
    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 0);
    setActorTestHook(&retirementJoinHook);
    auto sender = new CancelSender(rig.handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "idle-join escalation sender reservation");
    ActorRetireResult drained;
    auto retireThread = new Thread({ drained = rig.owner.requestRetire(); });
    retireThread.start();
    waitFlag(g_retirementIdleArrived, deadline,
        "idle-join drain did not sample idle");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    check(accepted(sender.result), "idle-join sender accepted");
    check(rig.runtime.ready == 1 && !rig.owner.retired,
        "late signal queued the actor before retirement resumed");
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 1);
    retireThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(drained == ActorRetireResult.requested,
        "drain re-observes the lifecycle after its idle sample");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel escalation after the joined drain request");
    rig.drain(deadline, "idle-join escalation retirement timeout");
    check(rig.output.cancelActivations == 1
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "escalation across the idle join reaches the handler");
    rig.close();
}

/// P5. requestRetire(cancel) on a SCHEDULED actor with a queued inbox (send,
/// no flush yet, cancel, then flush and consume). The scheduled-to-running
/// entry word must preserve the cancel mode; the handler observes
/// cancelling and cancels the message. Catches S2 keeping only retireBit.
private void testCancelWhileScheduled()
{
    auto rig = new CancelRig;
    CancelMessage message;
    message.node.initialize(&message, 1);
    check(accepted(rig.handle.send(&message.node)),
        "scheduled cancel send accepted");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "scheduled cancel request");
    rig.drain(MonoTime.currTime + 15.seconds,
        "scheduled cancel retirement timeout");
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == 1
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "scheduled entry preserves the cancel mode");
    rig.close();
}

/// P6. Escalate drain-then-cancel while the actor is RUNNING with a
/// stranded carry. Activation one completes a single node and then blocks
/// inside the handler until the owner thread has issued both requests;
/// its exit word must preserve the cancel mode, and the next activation
/// must observe cancelling and cancel the carried remainder. Catches S3
/// dropping cancelBit.
private void testCancelEscalationWhileRunning()
{
    enum nodeCount = 3;
    shared int arrived;
    shared int release;
    auto rig = new CancelRig(1, 2, &arrived, &release, 1);
    CancelMessage[nodeCount] messages;
    sendCancelMessages(rig.handle, messages[]);
    shared int stop;
    auto consumerJob = new StressConsumer(rig.farm, &stop);
    auto consumer = new Thread(&consumerJob.run);
    consumer.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    while (atomicLoad!(MemoryOrder.acq)(arrived) == 0)
    {
        rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
        if (MonoTime.currTime >= deadline)
            check(false, "running activation did not stall in the handler");
        Thread.yield();
    }
    check(rig.output.completedNodes == 1,
        "running activation completed one node before stalling");
    check(rig.owner.requestRetire() == ActorRetireResult.requested,
        "drain request while running");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel escalation while running");
    atomicStore!(MemoryOrder.rel)(release, 1);
    while (!rig.owner.retired)
    {
        rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
        if (MonoTime.currTime >= deadline)
            check(false, "running escalation retirement timeout");
        Thread.yield();
    }
    atomicStore!(MemoryOrder.rel)(stop, 1);
    consumer.join();
    check(rig.output.cancelActivations == 1
            && rig.output.completedNodes == 1
            && rig.output.cancelledNodes == 2,
        "exit after escalation cancels the carried remainder");
    foreach (i, message; messages)
        check(message.node.available
                && message.node.outcome == (i == 0
                    ? ActorNodeOutcome.completed : ActorNodeOutcome.cancelled),
            "running escalation node outcomes");
    rig.close();
}

/// S1/S5. The sender holds a submission reservation on an otherwise idle,
/// empty actor. The owner's cancel request cannot retire through the idle
/// fast path (S5: reschedule with the undrained gate), and the consumer's
/// empty-inbox entry reschedules again (S1) rather than running the
/// handler. Once the sender finally publishes and signals, the running
/// entry must observe cancelling and cancel the late message.
private void testCancelIdleReschedule()
{
    auto rig = new CancelRig;
    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    auto sender = new CancelSender(rig.handle, &message.node);
    auto thread = new Thread(&sender.run);
    thread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "idle-reschedule sender did not hold its reservation");
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "idle-reschedule cancel request");
    check(!rig.owner.retired && rig.runtime.ready == 1,
        "undrained gate reschedules the cancelling actor");
    rig.runtime.flush(rig.token, 8, DEFAULT_AVG_COST);
    check(rig.view.consumeNext(), "idle-reschedule consumer entry");
    check(rig.output.activations == 0 && !rig.owner.retired
            && rig.runtime.ready == 1,
        "empty-inbox entry reschedules without a handler call");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "idle-reschedule sender accepted");
    rig.drain(deadline, "idle-reschedule retirement timeout");
    check(rig.output.cancelActivations == 1
            && rig.output.cancelledNodes == 1
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "late message cancelled after the reschedules");
    rig.close();
}

/// S4. The owner's cancel request is parked at retirementIdleObserved with
/// the lifecycle still IDLE|RETIRE|CANCEL when the paused sender publishes
/// and signals: signalActor's IDLE-to-SCHEDULED word must keep the cancel
/// mode, and after the retirement re-observes and re-signals, the handler
/// must observe cancelling. Catches S4 keeping only retireBit.
private void testCancelSignalDuringIdleRetirement()
{
    auto rig = new CancelRig;
    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 0);
    setActorTestHook(&retirementJoinHook);
    auto sender = new CancelSender(rig.handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline,
        "idle-retirement sender reservation");
    ActorRetireResult cancelled;
    auto retireThread = new Thread(
        { cancelled = rig.owner.requestRetire(ActorRetireMode.cancel); });
    retireThread.start();
    waitFlag(g_retirementIdleArrived, deadline,
        "cancel retirement did not sample idle");
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    check(accepted(sender.result), "idle-retirement sender accepted");
    check(rig.runtime.ready == 1 && !rig.owner.retired,
        "late signal queued the cancelling actor");
    atomicStore!(MemoryOrder.rel)(g_retirementIdleRelease, 1);
    retireThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(cancelled == ActorRetireResult.requested,
        "cancel retirement re-observes after its idle sample");
    rig.drain(deadline, "idle-retirement drain timeout");
    check(rig.output.cancelActivations == 1
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "signalled cancellation reaches the handler");
    rig.close();
}

/// P8. An idle, empty actor cancelled directly retires without any handler
/// invocation: no final handler call is guaranteed.
private void testCancelIdleEmpty()
{
    auto rig = new CancelRig;
    check(rig.owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "idle empty cancel request");
    check(rig.owner.retired, "idle empty actor retires immediately");
    check(rig.output.activations == 0,
        "idle empty cancellation makes no handler call");
    rig.close();
}

private void waveReserveJoinHook(ActorTestPoint point, ActorInboxNode* node)
    nothrow @nogc @system
{
    if (point == ActorTestPoint.waveLifecycleReserved)
    {
        atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 1);
        while (atomicLoad!(MemoryOrder.acq)(g_waveReserveRelease) == 0) {}
        return;
    }
    blockingActorHook(point, node);
}

private final class CancelWavePublisher
{
    AntFarm* farm;
    ActorHandle!CancelProbeState[2] members;
    size_t memberCount;
    ActorWave wave;
    size_t written;
    ActorWaveHandle completion;
    shared int published;
    shared int sealRequested;
    shared int done;

    this(AntFarm* initialFarm, ActorHandle!CancelProbeState first)
    {
        farm = initialFarm;
        members[0] = first;
        memberCount = 1;
    }

    this(AntFarm* initialFarm, ActorHandle!CancelProbeState first,
            ActorHandle!CancelProbeState second)
    {
        farm = initialFarm;
        members[0] = first;
        members[1] = second;
        memberCount = 2;
    }

    void run()
    {
        auto token = farm.registerProducer(Tier.small);
        check(token.valid, "cancel-wave publisher token");
        wave.begin(farm);
        written = cast(size_t) wave.publish!cancelProbeWaveOperation(
            members[0 .. memberCount], token, avgCostForChunk(32));
        atomicStore!(MemoryOrder.rel)(published, 1);
        while (atomicLoad!(MemoryOrder.acq)(sealRequested) == 0)
            Thread.yield();
        completion = wave.seal();
        farm.unregisterProducer(token);
        atomicStore!(MemoryOrder.rel)(done, 1);
    }
}

/// P7 admission path. The owner cancels between the wave's lifecycle
/// reservation and its membership publication, while a sender that already
/// passed the zero waveOwnerWord check is paused at submissionReserved.
/// The wave runs its operation and exits interrupted (S7): the exit word
/// must keep the cancel mode, hand the actor back to autonomous
/// scheduling, and the handler must cancel the late message rather than
/// complete it. Catches S7 keeping only retireBit.
private void testCancelDuringWaveAdmission()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "cancel wave admission Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "cancel wave admission runtime allocation");
    CancelProbeResult output;
    auto owner = runtime.createActor!(CancelProbeState, cancelProbeActor)(
        CancelProbeState(&output));
    check(owner.valid, "cancel wave admission actor allocation");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "cancel wave admission subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "cancel wave admission producer token");

    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 0);
    setActorTestHook(&waveReserveJoinHook);
    auto publisher = new CancelWavePublisher(farm, owner.handle);
    auto publisherThread = new Thread(&publisher.run);
    publisherThread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_waveReserveArrived, deadline,
        "admission publisher did not pause after the lifecycle reservation");

    auto sender = new CancelSender(owner.handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    waitFlag(g_hookArrived, deadline,
        "admission sender did not pause after its reservation");

    check(owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel between wave reservation and membership publication");
    check(!owner.retired, "reserved wave payload prevents retirement");

    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 1);
    waitFlag(publisher.published, deadline,
        "admission wave table publication timeout");
    check(publisher.written == 1,
        "admission wave publishes the reserved actor");

    while (publisher.wave.handle.progress != 1)
    {
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "admission wave payload completion timeout");
        Thread.yield();
    }
    check(output.waveCalls == 1, "admission wave operation ran exactly once");

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "admission sender accepted");

    while (!owner.retired)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "admission autonomous retirement timeout");
        Thread.yield();
    }
    check(output.cancelActivations == 1 && output.cancelledNodes == 1,
        "wave exit preserves the cancel mode into the handler");
    check(message.node.available
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "admission sender observes cancelled, not completed");
    check(owner.reclaim() == ActorReclaimResult.busy,
        "wave membership still pins the retired actor");
    atomicStore!(MemoryOrder.rel)(publisher.sealRequested, 1);
    waitFlag(publisher.done, deadline, "admission wave seal timeout");
    publisherThread.join();
    check(publisher.completion.finished && !publisher.completion.failed
            && publisher.completion.interrupted,
        "cancelled wave finishes interrupted without structural failure");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "admission reclaim after wave release");
    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.live == 0 && runtime.ready == 0,
        "admission runtime drained");
    runtime.destroy();
}

/// P7 rollback path. The wave reserves the target, then fails to reserve
/// the non-idle blocker and rolls the target back through
/// cancelActorWavePayload (S6) while the owner has already cancelled. The
/// rollback word must keep the cancel mode, so the sender that passed the
/// zero waveOwnerWord check before the reservation observes its message
/// cancelled once the actor returns to autonomous scheduling. Catches S6
/// keeping only retireBit.
private void testCancelDuringWaveRollback()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 2, 256);
    check(farm !is null, "cancel wave rollback Farm allocation");
    scope (exit) farm.destroy();
    auto runtime = ActorRuntime.create(farm, 2);
    check(runtime !is null, "cancel wave rollback runtime allocation");
    CancelProbeResult[2] outputs;
    auto target = runtime.createActor!(CancelProbeState, cancelProbeActor)(
        CancelProbeState(&outputs[0]));
    auto blocker = runtime.createActor!(CancelProbeState, cancelProbeActor)(
        CancelProbeState(&outputs[1]));
    check(target.valid && blocker.valid,
        "cancel wave rollback actor allocation");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "cancel wave rollback subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "cancel wave rollback producer token");
    // The blocker stays scheduled, so the wave's second reservation fails
    // and the first is rolled back before any table is published.
    check(blocker.handle.wake() == ActorWakeResult.queued,
        "rollback blocker is scheduled");

    CancelMessage message;
    message.node.initialize(&message, 1);
    g_hookNode = &message.node;
    atomicStore!(MemoryOrder.rel)(g_hookTarget,
        cast(int) ActorTestPoint.submissionReserved);
    atomicStore!(MemoryOrder.rel)(g_hookArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_hookRelease, 0);
    atomicStore!(MemoryOrder.rel)(g_waveReserveArrived, 0);
    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 0);
    setActorTestHook(&waveReserveJoinHook);
    auto publisher = new CancelWavePublisher(farm, target.handle,
        blocker.handle);
    auto publisherThread = new Thread(&publisher.run);
    publisherThread.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_waveReserveArrived, deadline,
        "rollback publisher did not pause after the lifecycle reservation");

    auto sender = new CancelSender(target.handle, &message.node);
    auto senderThread = new Thread(&sender.run);
    senderThread.start();
    waitFlag(g_hookArrived, deadline,
        "rollback sender did not pause after its reservation");

    check(target.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "cancel before the wave membership rollback");
    check(!target.retired, "reserved wave payload prevents retirement");

    atomicStore!(MemoryOrder.rel)(g_waveReserveRelease, 1);
    waitFlag(publisher.published, deadline,
        "rollback wave publication timeout");
    check(publisher.written == 0, "rollback wave publishes nothing");

    atomicStore!(MemoryOrder.rel)(g_hookRelease, 1);
    senderThread.join();
    setActorTestHook(null);
    g_hookNode = null;
    check(accepted(sender.result), "rollback sender accepted");

    while (!target.retired)
    {
        runtime.flush(token, 8, DEFAULT_AVG_COST);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "rollback autonomous retirement timeout");
        Thread.yield();
    }
    check(outputs[0].cancelActivations == 1 && outputs[0].cancelledNodes == 1
            && outputs[0].waveCalls == 0,
        "rollback exits with the cancel mode preserved and no wave call");
    check(message.node.available
            && message.node.outcome == ActorNodeOutcome.cancelled,
        "rollback sender observes cancelled, not completed");
    check(target.reclaim() == ActorReclaimResult.reclaimed,
        "rollback released wave membership");

    atomicStore!(MemoryOrder.rel)(publisher.sealRequested, 1);
    waitFlag(publisher.done, deadline, "rollback wave seal timeout");
    publisherThread.join();
    check(publisher.completion.finished && publisher.completion.failed,
        "rolled-back wave finishes failed");

    check(blocker.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "rollback blocker cancel retirement");
    check(blocker.retired && blocker.reclaim() == ActorReclaimResult.reclaimed,
        "rollback blocker reclaim");
    view.unsubscribe();
    farm.unregisterProducer(token);
    check(runtime.live == 0 && runtime.ready == 0,
        "rollback runtime drained");
    runtime.destroy();
}

private void testCancelModeRetirement()
{
    testCancelBacklogDrainControl();
    testCancelBacklog();
    testCancelInboxCarry();
    testCancelEscalationQueued();
    testCancelThroughErasedOwner(false);
    testCancelThroughErasedOwner(true);
    testCancelEscalationAfterSignal();
    testCancelEscalationAcrossIdleJoin();
    testCancelWhileScheduled();
    testCancelEscalationWhileRunning();
    testCancelIdleReschedule();
    testCancelSignalDuringIdleRetirement();
    testCancelDuringWaveAdmission();
    testCancelDuringWaveRollback();
    testCancelIdleEmpty();
    printf("actor cancel-mode retirement OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Child-process diagnostics: `actors_torture death` re-executes
// /proc/self/exe as "child:<mode>" once per childCases row and checks the
// exit status and stderr. Rows describe fatal sites (abort) and debug
// diagnostics (warn); silent rows prove a path stays quiet.
// -------------------------------------------------------------------------

private enum ChildExpect
{
    abort,  // nonzero exit; stderr contains `text`
    warn,   // exit 0; stderr contains `text`
    silent, // exit 0; stderr empty
}

private struct ChildCase
{
    string mode;
    ChildExpect expect;
    string text;
    bool assertOnly; // the diagnostic exists only in version(assert) builds
    void function() run;
}

private enum ownerDropText = "ActorOwner dropped without reclaim";

private immutable ChildCase[] childCases = [
    ChildCase("node-complete-unowned", ChildExpect.abort,
        "antfarm fatal: complete actor inbox node not owned by actor", false,
        &childNodeUnowned!(NodeAction.complete)),
    ChildCase("node-fail-unowned", ChildExpect.abort,
        "antfarm fatal: fail actor inbox node not owned by actor", false,
        &childNodeUnowned!(NodeAction.fail)),
    ChildCase("node-cancel-unowned", ChildExpect.abort,
        "antfarm fatal: cancel actor inbox node not owned by actor", false,
        &childNodeUnowned!(NodeAction.cancel)),
    ChildCase("node-fail-oversize", ChildExpect.abort,
        "antfarm fatal: actor inbox node failure code out of range", false,
        &childNodeDispatch!false),
    ChildCase("node-reinitialize-queued", ChildExpect.abort,
        "antfarm fatal: actor inbox node state corrupt", false,
        &childNodeDispatch!true),
    ChildCase("erased-borrow-mismatch", ChildExpect.abort,
        "antfarm fatal: type-erased actor borrow type mismatch", false,
        &childErasedBorrowMismatch),
    ChildCase("owner-overwrite-live", ChildExpect.abort,
        "antfarm fatal: overwrite live ActorOwner", false,
        &childOwnerOverwrite!false),
    ChildCase("erased-owner-overwrite-live", ChildExpect.abort,
        "antfarm fatal: overwrite live ActorErasedOwner", false,
        &childOwnerOverwrite!true),
    ChildCase("actor-generation-wrap", ChildExpect.abort,
        "antfarm fatal: actor generation wrap", false,
        &childActorGenerationWrap),
    ChildCase("actor-generation-preset-live", ChildExpect.abort,
        "antfarm fatal: preset actor generation without a quiescent free slot", true,
        &childActorGenerationPresetLive),
    ChildCase("actor-generation-preset-oversize", ChildExpect.abort,
        "antfarm fatal: preset actor generation out of range", true,
        &childActorGenerationPresetOversize),
    ChildCase("runtime-destroy-live", ChildExpect.abort,
        "antfarm fatal: destroy actor runtime with live actors", false,
        &childRuntimeDestroyLive),
    ChildCase("wave-null-farm", ChildExpect.abort,
        "antfarm fatal: actor wave requires a Farm", false,
        &childWaveNullFarm),
    ChildCase("wave-reuse-unfinished", ChildExpect.abort,
        "antfarm fatal: reuse unfinished actor wave", false,
        &childWaveReuseUnfinished),
    ChildCase("wave-generation-wrap", ChildExpect.abort,
        "antfarm fatal: actor wave generation wrap", false,
        &childWaveGenerationWrap),
    ChildCase("wave-generation-preset-unfinished", ChildExpect.abort,
        "antfarm fatal: preset actor wave generation requires a finished wave and nonzero generation", true,
        &childWaveGenerationPresetUnfinished),
    ChildCase("wave-publish-uninitialized", ChildExpect.abort,
        "antfarm fatal: publish uninitialized actor wave", false,
        &childWavePublish!false),
    ChildCase("wave-publish-sealed", ChildExpect.abort,
        "antfarm fatal: publish sealed actor wave", false,
        &childWavePublish!true),
    ChildCase("wave-seal-uninitialized", ChildExpect.abort,
        "antfarm fatal: seal uninitialized actor wave", false,
        &childWaveSeal!false),
    ChildCase("wave-seal-twice", ChildExpect.abort,
        "antfarm fatal: seal actor wave twice", false,
        &childWaveSeal!true),
    ChildCase("owner-double-reclaim", ChildExpect.silent, null, false,
        &childOwnerDoubleReclaim),
    ChildCase("owner-drop-live", ChildExpect.warn, ownerDropText, true,
        &childOwnerDropLive),
    ChildCase("owner-move-scope-exit", ChildExpect.silent, null, false,
        &childOwnerMoveScopeExit),
    ChildCase("owner-reclaim-scope-exit", ChildExpect.silent, null, false,
        &childOwnerReclaimScopeExit),
    ChildCase("owner-leak-scope-exit", ChildExpect.silent, null, false,
        &childOwnerLeakScopeExit),
];

// Child bodies that drop or leak an actor also leak its runtime and Farm:
// destroying a runtime with a live actor is itself fatal.
private ActorRuntime* childRuntime(size_t capacity = 1)
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    check(farm !is null, "child Farm allocation");
    auto runtime = ActorRuntime.create(farm, capacity);
    check(runtime !is null, "child runtime allocation");
    return runtime;
}

private ActorOwner!OwnerProbeState childOwner(ActorRuntime* runtime)
{
    auto owner = runtime.createActor!(OwnerProbeState, ownerProbeActor)(
        OwnerProbeState(null));
    check(owner.valid, "child actor creation");
    return owner;
}

private void childRetireAndReclaim(ref ActorOwner!OwnerProbeState owner)
{
    check(owner.requestRetire(ActorRetireMode.cancel)
            == ActorRetireResult.requested,
        "child idle actor retire");
    check(owner.retired, "child idle actor retires immediately");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "child actor reclaim");
}

private void childNodeUnowned(NodeAction action)()
{
    ActorInboxNode node;
    node.initialize();
    static if (action == NodeAction.complete) node.complete();
    else static if (action == NodeAction.fail) node.fail(0);
    else node.cancel();
}

private void childNodeDispatch(bool reinitialize)()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "child node runtime allocation");
    shared int handled;
    auto owner = runtime.createActor!(NodeOutcomeState, nodeOutcomeActor)(
        NodeOutcomeState(&handled));
    check(owner.valid, "child node actor creation");
    NodeOutcomeMessage message;
    message.action = NodeAction.fail;
    message.code = 0x100_0000;
    message.node.initialize(&message);
    check(accepted(owner.handle.send(&message.node)), "child node send");
    static if (reinitialize)
        message.node.initialize(&message);
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "child node subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "child node producer");
    check(runtime.flush(token) == 1, "child node flush");
    while (view.consumeNext()) {}
}

private void childMismatchedErasedActor(scope ref ActorErasedBorrow actor,
        scope ref ActorContext) nothrow @nogc @system
{
    actor.value!ubyte() = 0;
}

private void childErasedBorrowMismatch()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "child erased runtime allocation");
    ulong initial;
    auto adapter = runtime.erasedAdapter;
    auto owner = adapter.createActor(&initial, initial.sizeof,
        ulong.alignof, &childMismatchedErasedActor);
    check(owner.valid, "child erased actor creation");
    check(owner.handle.wake() == ActorWakeResult.queued, "child erased wake");
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "child erased subscribe");
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "child erased producer");
    check(runtime.flush(token) == 1, "child erased flush");
    while (view.consumeNext()) {}
}

private void childOwnerOverwrite(bool erased)()
{
    auto runtime = childRuntime(2);
    auto first = childOwner(runtime);
    auto second = childOwner(runtime);
    static if (erased)
    {
        auto target = first.intoErased();
        auto source = second.intoErased();
        target = move(source);
    }
    else first = move(second);
}

private void childActorGenerationWrap()
{
    auto runtime = childRuntime();
    runtime.testPresetFreeSlotGeneration((ulong.max >> 8) - 1);
    auto owner = childOwner(runtime);
    check(owner.handle.generation == ulong.max >> 8,
        "child final representable actor generation");
    childRetireAndReclaim(owner);
    auto wrapped = childOwner(runtime);
}

private void childActorGenerationPresetLive()
{
    auto runtime = childRuntime();
    auto owner = childOwner(runtime);
    runtime.testPresetFreeSlotGeneration(1);
}

private void childActorGenerationPresetOversize()
{
    childRuntime().testPresetFreeSlotGeneration(ulong.max);
}

private void childRuntimeDestroyLive()
{
    auto runtime = childRuntime();
    auto owner = childOwner(runtime);
    runtime.destroy();
}

private void childWaveNullFarm()
{
    ActorWave wave;
    wave.begin(null);
}

private void childWaveReuseUnfinished()
{
    auto farm = AntFarm.create();
    check(farm !is null, "child wave Farm allocation");
    ActorWave wave;
    wave.begin(farm);
    wave.begin(farm);
}

private void childWaveGenerationPresetUnfinished()
{
    auto farm = AntFarm.create();
    check(farm !is null, "child wave Farm allocation");
    ActorWave wave;
    wave.begin(farm);
    wave.testPresetGeneration(1);
}

private void childWaveGenerationWrap()
{
    auto farm = AntFarm.create();
    check(farm !is null, "child wave Farm allocation");
    ActorWave wave;
    wave.begin(farm);
    check(wave.seal().finished, "child wave initial finish");
    wave.testPresetGeneration(ulong.max - 1);
    wave.begin(farm);
    check(wave.handle.generation == ulong.max,
        "child final representable wave generation");
    check(wave.seal().finished, "child wave final finish");
    wave.begin(farm);
}

private void childWaveOperation(scope ref ActorBorrow!OwnerProbeState)
    nothrow @nogc @system
{
}

private void childWavePublish(bool sealed)()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null, "child wave runtime allocation");
    auto owner = childOwner(runtime);
    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "child wave producer");
    ActorWave wave;
    static if (sealed)
    {
        wave.begin(farm);
        check(wave.seal().finished, "child wave sealed");
    }
    ActorHandle!OwnerProbeState[1] handles = [owner.handle];
    wave.publish!childWaveOperation(handles[], token);
}

private void childWaveSeal(bool twice)()
{
    ActorWave wave;
    static if (twice)
    {
        auto farm = AntFarm.create();
        check(farm !is null, "child wave Farm allocation");
        wave.begin(farm);
        check(wave.seal().finished, "child wave first seal");
    }
    wave.seal();
}

private void childOwnerDoubleReclaim()
{
    auto owner = childOwner(childRuntime());
    childRetireAndReclaim(owner);
    check(owner.reclaim() == ActorReclaimResult.staleOwner,
        "child second reclaim is a stale owner");
}

private void childOwnerDropLive()
{
    auto owner = childOwner(childRuntime());
}

private void childOwnerMoveScopeExit()
{
    auto runtime = childRuntime();
    ActorOwner!OwnerProbeState moved;
    {
        auto owner = childOwner(runtime);
        moved = move(owner);
    }
    check(moved.valid, "child moved-into owner stays valid");
    childRetireAndReclaim(moved);
}

private void childOwnerReclaimScopeExit()
{
    auto owner = childOwner(childRuntime());
    childRetireAndReclaim(owner);
}

private void childOwnerLeakScopeExit()
{
    auto owner = childOwner(childRuntime());
    owner.leak();
    check(!owner.valid, "leak invalidates the owner");
}

private void runChildMode(string mode)
{
    foreach (ref row; childCases)
        if (row.mode == mode)
        {
            row.run();
            fflush(stdout);
            fflush(stderr);
            return;
        }
    check(false, "unknown actor torture child mode");
}

private void testChildCases()
{
    version (linux)
    {
        import std.algorithm.searching : canFind;
        import std.process : Redirect, pipeProcess, wait;

        foreach (ref row; childCases)
        {
            version (assert) {}
            else if (row.assertOnly)
            {
                printf("SKIP child %.*s: version(assert) diagnostic, "
                    ~ "absent under -release\n",
                    cast(int) row.mode.length, row.mode.ptr);
                continue;
            }
            auto pipes = pipeProcess(["/proc/self/exe", "child:" ~ row.mode],
                Redirect.stderr);
            string text;
            foreach (chunk; pipes.stderr.byChunk(4096))
                text ~= cast(const(char)[]) chunk;
            immutable status = wait(pipes.pid);
            bool ok;
            final switch (row.expect)
            {
            case ChildExpect.abort:
                ok = status != 0 && text.canFind(row.text);
                break;
            case ChildExpect.warn:
                ok = status == 0 && text.canFind(row.text);
                break;
            case ChildExpect.silent:
                ok = status == 0 && text.length == 0;
                break;
            }
            if (!ok)
                fprintf(stderr, "child %.*s: status %d, stderr:\n%.*s",
                    cast(int) row.mode.length, row.mode.ptr, status,
                    cast(int) text.length, text.ptr);
            check(ok, "child mode exit status or stderr mismatch");
            printf("PASS child %.*s\n", cast(int) row.mode.length, row.mode.ptr);
        }
        printf("actor child diagnostics OK\n");
    }
    else
        printf("SKIP actor child diagnostics: /proc/self/exe re-exec "
            ~ "requires Linux\n");
    fflush(stdout);
}



private void verifyReplacementAdmission(AntFarm* farm, ActorRuntime* runtime,
        ref ActorOwner!BoundaryState replacement)
{
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0, "replacement subscription");
    BoundaryMessage message;
    initializeBoundaryMessage(message);
    check(replacement.handle.send(&message.node) == ActorSendResult.queued,
        "new generation admission survives delayed old owner");
    check(replacement.handle.wake() == ActorWakeResult.coalesced,
        "new generation wake survives delayed old owner");
    auto deadline = MonoTime.currTime + 15.seconds;
    while (!message.node.available)
    {
        runtime.flush(token, 1, DEFAULT_AVG_COST);
        view.consumeNext();
        check(MonoTime.currTime < deadline, "replacement message completion timeout");
    }
    check(replacement.requestRetire() == ActorRetireResult.requested &&
        replacement.reclaim() == ActorReclaimResult.reclaimed,
        "replacement remains independently reclaimable");
    view.unsubscribe();
    farm.unregisterProducer(token);
}


private struct PinnedRunningState
{
    shared int* entered;
    shared int* release;
}
private void pinnedRunningActor(scope ref ActorBorrow!PinnedRunningState actor,
        scope ref ActorContext) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)(*actor.value.entered, 1);
    while (atomicLoad!(MemoryOrder.acq)(*actor.value.release) == 0) {}
}

private void replacementRetirementHook(ActorTestPoint point, ActorInboxNode*)
    nothrow @nogc @system
{
    if (point != ActorTestPoint.retirementBeforeGateClose) return;
    atomicStore!(MemoryOrder.rel)(g_raceArrived, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_raceRelease) == 0) {}
}

private void testRetirementFinalAccess()
{
    // Idle terminal publication releases the pin itself. Reuse the slot while the
    // old retirement call is still on its stack; it must not later clear a
    // new generation's pin or close its submission gate.
    {
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        auto runtime = ActorRuntime.create(farm, 1);
        BoundaryResult result;
        auto typed = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
        auto owner = typed.intoErased();
        auto copy = testDuplicateActorOwner(owner);
        g_hookNode = null;
        atomicStore(g_hookTarget, cast(int) ActorTestPoint.retirementAfterPublish);
        atomicStore(g_hookArrived, 0);
        atomicStore(g_hookRelease, 0);
        setActorTestHook(&blockingActorHook);
        ActorRetireResult retired;
        auto thread = new Thread({ retired = owner.requestRetire(); });
        thread.start();
        waitFlag(g_hookArrived, MonoTime.currTime + 15.seconds, "idle retirement final access");
        atomicStore(g_hookTarget, -1);
        check(copy.retired && copy.reclaim() == ActorReclaimResult.reclaimed,
            "terminal publication releases idle pin");
        auto replacement = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
        auto replacementOwner = replacement.intoErased();
        auto replacementCopy = testDuplicateActorOwner(replacementOwner);
        atomicStore(g_raceArrived, 0);
        atomicStore(g_raceRelease, 0);
        setActorTestHook(&replacementRetirementHook);
        auto nextRequest = new Thread({
            check(replacementOwner.requestRetire() == ActorRetireResult.requested,
                "replacement retirement completes");
        });
        nextRequest.start();
        waitFlag(g_raceArrived, MonoTime.currTime + 15.seconds, "replacement pin acquired");
        atomicStore(g_hookRelease, 1);
        thread.join();
        check(retired == ActorRetireResult.requested, "old retirement returns after reuse");
        check(owner.reclaim() == ActorReclaimResult.staleOwner, "old owner remains stale");
        check(replacementCopy.requestRetire() == ActorRetireResult.busy,
            "old retirement cannot clear the replacement generation's pin");
        atomicStore(g_raceRelease, 1);
        nextRequest.join();
        setActorTestHook(null);
        check(replacementCopy.reclaim() == ActorReclaimResult.reclaimed,
            "replacement keeps sole reclaim authority");
        auto finalOwner = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
        verifyReplacementAdmission(farm, runtime, finalOwner);
        runtime.destroy();
        farm.destroy();
    }
    // A running callback may publish RETIRED before the external request
    // finishes. Its transition must retain the pin until the separate unpin.
    {
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        auto runtime = ActorRuntime.create(farm, 1);
        shared int entered, release;
        auto typed = runtime.createActor!(PinnedRunningState, pinnedRunningActor)(
            PinnedRunningState(&entered, &release));
        auto owner = typed.intoErased();
        auto copy = testDuplicateActorOwner(owner);
        auto token = farm.registerProducer(Tier.small);
        check(owner.handle.wake() == ActorWakeResult.queued, "running pin wake");
        auto consumer = new Thread({
            ConsumerView view;
            check(view.subscribe(farm) >= 0, "running pin subscription");
            while (atomicLoad!(MemoryOrder.acq)(entered) == 0) view.consumeNext();
            view.unsubscribe();
        });
        consumer.start();
        auto deadline = MonoTime.currTime + 15.seconds;
        while (atomicLoad!(MemoryOrder.acq)(entered) == 0)
        {
            runtime.flush(token, 1);
            check(MonoTime.currTime < deadline, "running pin entry timeout");
        }
        g_hookNode = null;
        atomicStore(g_hookTarget, cast(int) ActorTestPoint.retirementBeforeUnpin);
        atomicStore(g_hookArrived, 0);
        atomicStore(g_hookRelease, 0);
        setActorTestHook(&blockingActorHook);
        ActorRetireResult retired;
        auto thread = new Thread({ retired = owner.requestRetire(); });
        thread.start();
        waitFlag(g_hookArrived, deadline, "running retirement before unpin");
        check(copy.requestRetire() == ActorRetireResult.busy, "running exclusive retirement pin");
        atomicStore!(MemoryOrder.rel)(release, 1);
        consumer.join();
        check(copy.retired && copy.reclaim() == ActorReclaimResult.busy,
            "callback retirement retains external pin");
        atomicStore(g_hookRelease, 1);
        thread.join();
        setActorTestHook(null);
        check(retired == ActorRetireResult.requested
            && copy.reclaim() == ActorReclaimResult.reclaimed, "running final unpin");
        farm.unregisterProducer(token);
        BoundaryResult result;
        auto replacement = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
        verifyReplacementAdmission(farm, runtime, replacement);
        runtime.destroy();
        farm.destroy();
    }
}

private __gshared ActorRuntime* g_reservationRuntime;
private __gshared size_t g_reservationChunks;
private __gshared size_t[65] g_reservationOccupancy;

private void reservationChunkHook(ActorTestPoint point, ActorInboxNode* node)
    nothrow @nogc @system
{
    if (point != ActorTestPoint.freeSlotsChunkReleased) return;
    immutable index = g_reservationChunks++;
    if (index >= g_reservationOccupancy.length)
        fatal("too many reservation chunks");
    g_reservationOccupancy[index] = g_reservationRuntime.live;
    if (index == 0) blockingActorHook(point, node);
}

private void testChunkedSlotReservation()
{
    enum size_t capacity = 4097;
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    auto runtime = ActorRuntime.create(farm, capacity);
    BoundaryResult result;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
    check(owner.requestRetire() == ActorRetireResult.requested,
        "reservation competitor retired");
    g_reservationRuntime = runtime;
    g_reservationChunks = 0;
    g_hookNode = null;
    atomicStore(g_hookTarget, cast(int) ActorTestPoint.freeSlotsChunkReleased);
    atomicStore(g_hookArrived, 0);
    atomicStore(g_hookRelease, 0);
    setActorTestHook(&reservationChunkHook);
    size_t reserved;
    auto creator = new Thread({ reserved = testActorSlotReservation(runtime, capacity); });
    creator.start();
    auto deadline = MonoTime.currTime + 15.seconds;
    waitFlag(g_hookArrived, deadline, "reservation first chunk released");
    check(runtime.live == 65, "first chunk counts exactly 64 reservations");
    shared int reclaimed;
    auto reclaimer = new Thread({
        check(owner.reclaim() == ActorReclaimResult.reclaimed,
            "remote reclaim between reservation chunks");
        atomicStore!(MemoryOrder.rel)(reclaimed, 1);
    });
    reclaimer.start();
    waitFlag(reclaimed, deadline, "reclaim must finish while batch is paused");
    reclaimer.join();
    check(runtime.live == 64, "remote reclaim preserves reserved occupancy");
    atomicStore(g_hookRelease, 1);
    creator.join();
    setActorTestHook(null);
    check(reserved == capacity && g_reservationChunks == 65,
        "batch includes returned slot without losing reservations");
    foreach (i; 0 .. g_reservationChunks)
        check(g_reservationOccupancy[i] ==
            (i == 0 ? 65 : i == 64 ? capacity : (i + 1) * 64),
            "each gate hold detaches at most 64 slots");
    check(runtime.quiescent, "reservation rollback restores empty runtime");
    check(testActorSlotReservation(runtime, capacity + 1) == capacity
        && runtime.quiescent, "partial reservation rollback retains every slot");
    runtime.destroy();
    farm.destroy();
    g_reservationRuntime = null;
}


private void testOccupancyFinalGate()
{
    auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
    auto runtime = ActorRuntime.create(farm, 1);
    BoundaryResult result;
    auto owner = runtime.createActor!(BoundaryState, boundaryActor)(BoundaryState(&result));
    check(runtime.live == 1, "occupied actor count");
    check(owner.requestRetire() == ActorRetireResult.requested, "occupancy retirement");
    g_hookNode = null;
    atomicStore(g_hookTarget, cast(int) ActorTestPoint.freeSlotBeforeUnlock);
    atomicStore(g_hookArrived, 0);
    atomicStore(g_hookRelease, 0);
    setActorTestHook(&blockingActorHook);
    ActorReclaimResult reclaimed;
    auto thread = new Thread({ reclaimed = owner.reclaim(); });
    thread.start();
    waitFlag(g_hookArrived, MonoTime.currTime + 15.seconds, "occupancy final gate boundary");
    check(runtime.live == 0 && !runtime.quiescent,
        "zero occupancy cannot hide an unreleased free-slot gate");
    atomicStore(g_hookRelease, 1);
    thread.join();
    setActorTestHook(null);
    check(reclaimed == ActorReclaimResult.reclaimed && runtime.quiescent,
        "released free-slot gate completes occupancy accounting");
    runtime.destroy();
    farm.destroy();
}

private void testCopiedOwners()
{
    testChunkedSlotReservation();
    testOccupancyFinalGate();
    testRetirementFinalAccess();
    // Pause every final-access boundary of a retirement request. A copied
    // owner must neither close a reused slot nor free state beneath this call.
    foreach (point; [ActorTestPoint.retirementBeforeGateClose,
            ActorTestPoint.retirementBeforePublish])
    {
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        auto runtime = ActorRuntime.create(farm, 1);
        BoundaryResult result;
        auto typed = runtime.createActor!(BoundaryState, boundaryActor)(
            BoundaryState(&result));
        auto owner = typed.intoErased();
        auto copy = testDuplicateActorOwner(owner);
        g_hookNode = null;
        atomicStore(g_hookTarget, cast(int) point);
        atomicStore(g_hookArrived, 0);
        atomicStore(g_hookRelease, 0);
        setActorTestHook(&blockingActorHook);
        ActorRetireResult retired;
        auto thread = new Thread({ retired = owner.requestRetire(); });
        thread.start();
        waitFlag(g_hookArrived, MonoTime.currTime + 15.seconds,
            "copied retire did not reach boundary");
        BoundaryMessage rejected;
        initializeBoundaryMessage(rejected);
        check(copy.handle.wake() == ActorWakeResult.closed
            && copy.handle.send(&rejected.node) == ActorSendResult.closed
            && rejected.node.available,
            "closing idle lifecycle rejects wake and new sender without mutation");
        check(copy.reclaim() == ActorReclaimResult.busy,
            "retirement pin blocks copied reclaim");
        auto competing = copy.requestRetire();
        check(competing == ActorRetireResult.busy ||
                competing == ActorRetireResult.alreadyRetired,
            "copied retirement cannot enter pinned generation");
        atomicStore(g_hookRelease, 1);
        thread.join();
        setActorTestHook(null);
        check(retired == ActorRetireResult.requested && copy.retired, "pinned retirement completes");
        check(copy.reclaim() == ActorReclaimResult.reclaimed,
            "copied reclaim after unpin");
        check(owner.reclaim() == ActorReclaimResult.staleOwner,
            "vacant generation is stale");
        auto replacement = runtime.createActor!(BoundaryState, boundaryActor)(
            BoundaryState(&result));
        check(owner.requestRetire() == ActorRetireResult.staleOwner,
            "old retirement cannot affect replacement");
        verifyReplacementAdmission(farm, runtime, replacement);
        runtime.destroy();
        farm.destroy();
    }
    foreach (point; [ActorTestPoint.reclaimBeforeClaim,
            ActorTestPoint.reclaimClaimed])
    {
        auto farm = AntFarm.create(2, 4, 1, 0, 0, 1, 256);
        auto runtime = ActorRuntime.create(farm, 1);
        BoundaryResult result;
        auto typed = runtime.createActor!(BoundaryState, boundaryActor)(
            BoundaryState(&result));
        auto owner = typed.intoErased();
        auto copy = testDuplicateActorOwner(owner);
        check(owner.requestRetire() == ActorRetireResult.requested, "reclaim race retirement");
        g_hookNode = null;
        atomicStore(g_hookTarget, cast(int) point);
        atomicStore(g_hookArrived, 0);
        atomicStore(g_hookRelease, 0);
        setActorTestHook(&blockingActorHook);
        ActorReclaimResult reclaimed;
        auto thread = new Thread({ reclaimed = owner.reclaim(); });
        thread.start();
        waitFlag(g_hookArrived, MonoTime.currTime + 15.seconds,
            "copied reclaim did not reach boundary");
        atomicStore(g_hookTarget, -1); // Only the first caller stays paused.
        if (point == ActorTestPoint.reclaimBeforeClaim)
        {
            check(copy.reclaim() == ActorReclaimResult.reclaimed,
                "second caller wins unclaimed generation");
            auto replacement = runtime.createActor!(BoundaryState, boundaryActor)(
                BoundaryState(&result));
            atomicStore(g_hookRelease, 1);
            thread.join();
            check(reclaimed == ActorReclaimResult.busy,
                "old exact-word CAS cannot claim reused generation");
            verifyReplacementAdmission(farm, runtime, replacement);
        }
        else
        {
            check(copy.reclaim() == ActorReclaimResult.busy,
                "exclusive reclaim blocks duplicate free");
            check(copy.handle.wake() == ActorWakeResult.closed,
                "reclaiming generation rejects wake");
            check(copy.requestRetire() == ActorRetireResult.alreadyRetired,
                "reclaiming generation rejects gate writes");
            atomicStore(g_hookRelease, 1);
            thread.join();
            check(reclaimed == ActorReclaimResult.reclaimed,
                "exclusive reclaimer finishes");
            check(copy.reclaim() == ActorReclaimResult.staleOwner,
                "losing copy becomes stale");
        }
        setActorTestHook(null);
        runtime.destroy();
        farm.destroy();
    }
}

void main(string[] args)
{
    if (args.length == 2 && args[1].length > 6 && args[1][0 .. 6] == "child:")
    {
        runChildMode(args[1][6 .. $]);
        return;
    }
    if (args.length == 2)
    {
        switch (args[1])
        {
        case "--owners": testCopiedOwners(); break;
        case "death": testChildCases(); break;
        case "--wave-wake": testWaveSignalRace(false); break;
        case "--wave-send": testWaveSignalRace(true); break;
        case "--wave-late-send": testWaveSignalRace(true, true); break;
        case "--waves":
            testWaveSignalRace(false);
            testWaveSignalRace(true);
            testWaveSignalRace(true, true);
            testWaveCompletionCreditPinsDescriptor(false);
            testWaveCompletionCreditPinsDescriptor(true);
            testWaveMembershipPinsRetiredActor();
            testWaveReleaseAllowsImmediateReuse();
            testCompletionCallbackRunsAfterFinished();
            testCancelDuringWaveAdmission();
            testCancelDuringWaveRollback();
            break;
        default: check(false, "unknown actor torture selector");
        }
        printf("SELECTED ACTOR TORTURE TESTS PASSED\n");
        return;
    }
    check(args.length == 1, "expected at most one actor torture selector");
    testCopiedOwners();
    testDeterministicInterleavings();
    testNodeOutcomes();
    testOwnerMoveOnly();
    testRequestReplyWake();
    testCancelModeRetirement();
    testCompletionCallbackRunsAfterFinished();
    testAggregateModuleUnloadFence();
    testSustainedContention();
    testReadyDrainHandoff();
    testReadyBacklog();
    testSlotAllocationFailure();
    testConcurrentSlotReuse();
    version (AntfarmMimallocStub)
        testMimallocV3AdapterContract();
    version (AntfarmMimallocReal)
        testPinnedMimallocCrossThreadFree();
    printf("ALL ACTOR TORTURE TESTS PASSED\n");
    fflush(stdout);
}
