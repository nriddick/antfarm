/++
 + Deterministic interleaving and sustained-contention tests for
 + antfarm_actor. Build with AntfarmActorTestHooks and, for the allocator
 + contract test, AntfarmMimallocV3.
 +/
module actor_torture;

import antfarm;
import antfarm_actor;
import antfarm_allocation : allocateAligned64, freeAligned64;
version (AntfarmMimallocV3)
    import antfarm_actor_mimalloc : mimallocV3ActorAllocator;
import core.atomic;
import core.memory : GC;
import core.stdc.stdio : fflush, fprintf, printf, stderr, stdout;
import core.stdc.stdlib : abort, free, malloc;
import core.stdc.string : memset;
import core.thread : Thread;
import core.time : MonoTime, seconds;

version (AntfarmActorTestHooks) {}
else static assert(false,
    "actor_torture requires AntfarmActorTestHooks");

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
        runtime.flush(token, 8, 1);
        while (view.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            check(false, "deterministic boundary drain timeout");
        Thread.yield();
    }
}

private void testCloseBoundary(ActorTestPoint point)
{
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
        runtime.flush(token, 8, 1);
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
        runtime.flush(token, 8, 1);
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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

private void testDeterministicInterleavings()
{
    testClaimIsNotPublication();
    testCloseBoundary(ActorTestPoint.submissionReserved);
    testCloseBoundary(ActorTestPoint.nodeClaimed);
    testCloseBoundary(ActorTestPoint.inboxPublished);
    testCloseBoundary(ActorTestPoint.activationSignalled);
    testContendedNodeClaimDuringClose();
    testReleasedReservationIsQuiescent();
    printf("actor deterministic interleavings OK\n");
    fflush(stdout);
}

// -------------------------------------------------------------------------
// Engine-owned module-generation aggregate unload fence
// -------------------------------------------------------------------------

/// Test-side model of the fence an engine module-generation owner needs.
/// `antfarm_actor` deliberately does not own this hierarchy: the resident
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
            slot = owner;
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
    while (runtime.flush(token, 1, 1) == 0)
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
        runtime.flush(token, 8, 1);
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
    auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
        runtime.flush(token, 8, 1);
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
    auto farm = AntFarm.create(1 << 19, 8, consumerCount,
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
            runtime.flush(token, 64, 2);
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
            runtime.flush(token, 64, 2);
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
        auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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
    private enum int pinnedMimallocVersion = 30_500;
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
            "real actor lane linked pinned mimalloc v3.5.0");
        auto farm = AntFarm.create(1 << 18, 4, 1, 0, 0, 1, 256);
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

        printf("pinned mimalloc v3.5.0 cross-thread free: %llu states OK\n",
            cast(ulong) crossThreadRounds * crossThreadActorCount);
        fflush(stdout);
    }
}

void main()
{
    testDeterministicInterleavings();
    testAggregateModuleUnloadFence();
    testSustainedContention();
    version (AntfarmMimallocStub)
        testMimallocV3AdapterContract();
    version (AntfarmMimallocReal)
        testPinnedMimallocCrossThreadFree();
    printf("ALL ACTOR TORTURE TESTS PASSED\n");
    fflush(stdout);
}
