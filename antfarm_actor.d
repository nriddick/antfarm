/++
 + Experimental non-GC actor activations for Ant Farm.
 +
 + Actor state lives outside the ring in caller-allocator storage. The ring
 + carries only a stable control-slot address and generation to one permanent
 + trampoline. `ActorHandle` can request execution but cannot dereference the
 + state; only the trampoline can construct an `ActorBorrow` after winning the
 + slot's scheduled-to-running transition.
 +
 + This is the A1 vertical slice described by ACTOR_ROADMAP.md. Construction is
 + deliberately restricted to POD state while the ownership and publication
 + protocol is evaluated. The A2 slice adds allocator-neutral intrusive actor
 + inboxes and submission reservations. Adopted storage, general placement
 + construction, and aggregate retirement fences remain follow-on work.
 +/
module antfarm_actor;

public import antfarm : AntFarm, PayloadBody, PayloadHeader, Tier, Token;

import antfarm_allocation : allocateAligned64, freeAligned64;
import antfarm : fatal;
import core.atomic : MemoryOrder, atomicExchange, atomicFetchAdd,
    atomicFetchSub, atomicLoad, atomicStore, cas;
import core.stdc.string : memcpy, memset;

/// Non-GC allocation policy used for the runtime, stable slots, and actor
/// state. `deallocate` must match `allocate`; both may be backed by an arena,
/// in which case per-object deallocation may intentionally do nothing.
alias ActorAllocate = void* function(void* context, size_t bytes,
    size_t alignment) nothrow @nogc @system;
alias ActorDeallocate = void function(void* context, void* memory,
    size_t bytes, size_t alignment) nothrow @nogc @system;

struct ActorAllocator
{
    void* context;
    ActorAllocate allocate;
    ActorDeallocate deallocate;

    @property bool valid() const pure nothrow @nogc @safe
    {
        return allocate !is null && deallocate !is null;
    }

    /// Baseline allocator. It uses the repository's paired 64-byte aligned
    /// C-runtime path and therefore supports alignments through 64 bytes.
    static ActorAllocator cRuntime() pure nothrow @nogc @safe
    {
        return ActorAllocator(null, &cRuntimeAllocate, &cRuntimeDeallocate);
    }
}

private void* cRuntimeAllocate(void*, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (alignment > 64) return null;
    return allocateAligned64(bytes);
}

private void cRuntimeDeallocate(void*, void* memory, size_t, size_t)
    nothrow @nogc @system
{
    freeAligned64(memory);
}

enum ActorWakeResult : ubyte
{
    queued,
    coalesced,
    staleHandle,
    closed,
}

enum ActorSendResult : ubyte
{
    queued,
    coalesced,
    staleHandle,
    closed,
    nodeBusy,
}

enum ActorRetireResult : ubyte
{
    requested,
    alreadyRetired,
    staleOwner,
}

enum ActorReclaimResult : ubyte
{
    reclaimed,
    busy,
    staleOwner,
}

version (AntfarmActorTestHooks)
{
    /// Deterministic synchronization seams for the out-of-tree actor torture
    /// suite. Production builds do not contain the hook or its branches.
    enum ActorTestPoint : ubyte
    {
        submissionReserved,
        nodeClaimed,
        inboxPublished,
        activationSignalled,
        submissionReleased,
    }

    alias ActorTestHook = void function(ActorTestPoint point,
        ActorInboxNode* node) nothrow @nogc @system;

    private __gshared ActorTestHook actorTestHook_;

    /// Install a process-wide test hook. The caller must change it only while
    /// no actor send can be executing it.
    void setActorTestHook(ActorTestHook hook) nothrow @nogc @system
    {
        actorTestHook_ = hook;
    }

    private void actorTestPoint(ActorTestPoint point, ActorInboxNode* node)
        nothrow @nogc @system
    {
        auto hook = actorTestHook_;
        if (hook !is null) hook(point, node);
    }
}

private enum uint inboxNodeFree = 0;
private enum uint inboxNodeQueued = 1;
private enum uint inboxNodeProcessing = 2;

/// Intrusive header for one caller-owned interaction. `payload` and `tag` are
/// application fields; the node itself must remain stable from a successful
/// send until the actor either calls `complete` or otherwise takes ownership
/// of its storage. A node may belong to only one inbox at a time.
struct ActorInboxNode
{
    private shared size_t nextWord_;
    private shared uint state_;
    private uint reserved_;
    void* payload;
    ulong tag;

    /// Initialize freshly allocated, caller-exclusive storage. Do not call
    /// this while the node is queued, processing, or observable by a sender.
    void initialize(void* initialPayload = null, ulong initialTag = 0)
        nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.raw)(nextWord_, size_t.init);
        atomicStore!(MemoryOrder.raw)(state_, inboxNodeFree);
        reserved_ = 0;
        payload = initialPayload;
        tag = initialTag;
    }

    @property bool available() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(state_) == inboxNodeFree;
    }

    /// Return a node handed to the actor by `ActorContext.popInbox` to its
    /// caller-defined free state. Read/process payload fields before this
    /// release; another producer may reuse them after observing availability.
    void complete() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != inboxNodeProcessing)
            fatal("complete actor inbox node not owned by actor");
        atomicStore!(MemoryOrder.rel)(state_, inboxNodeFree);
    }
}
static assert(ActorInboxNode.sizeof == 32);

/// Callback-local control. Requests take effect only after the application
/// callback returns and its ActorBorrow has ended.
struct ActorContext
{
    private bool republish_;
    private bool retire_;
    private ActorSlot* slot_;
    private ActorInboxNode* inboxLocal_;

    void republish() pure nothrow @nogc @safe { republish_ = true; }
    void retire() pure nothrow @nogc @safe { retire_ = true; }

    /// True after external or callback-local retirement has closed this
    /// generation. Accepted interactions must be drained before RETIRED.
    @property bool closing() const nothrow @nogc @system
    {
        if (retire_) return true;
        return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & retireBit) != 0;
    }

    /// Detach interactions in producer linearization order and return one to
    /// the exclusive actor. Unpopped nodes stay actor-private across the next
    /// activation and force republishing; producers never touch that carry.
    ActorInboxNode* popInbox() nothrow @nogc @system
    {
        return popInboxNode(slot_, inboxLocal_);
    }
}

/// The only mutable view of actor state. The actor trampoline constructs it
/// after acquiring serial ownership and passes it by ref to the application
/// callback. Escaping `value` is outside the @system ownership contract.
struct ActorBorrow(T)
{
    private T* state_;
    @disable this(this);

    @property ref T value() return nothrow @nogc @system
    {
        return *state_;
    }
}

private alias ActorDispatch = void function(void* state, ActorContext* context)
    nothrow @nogc @system;

private enum ulong phaseMask = 0x7;
private enum ulong retireBit = 0x8;
private enum ulong pendingBit = 0x10;
private enum uint generationShift = 8;
private enum ulong maxGeneration = ulong.max >> generationShift;

private enum ulong phaseVacant = 0;
private enum ulong phaseConstructing = 1;
private enum ulong phaseIdle = 2;
private enum ulong phaseScheduled = 3;
private enum ulong phaseRunning = 4;
private enum ulong phaseRetired = 5;

private enum ulong submissionClosedBit = 1UL << 63;
private enum ulong submissionCountMask = submissionClosedBit - 1;

private ulong lifecycleWord(ulong generation, ulong flags)
    pure nothrow @nogc @safe
{
    return (generation << generationShift) | flags;
}

private ulong wordGeneration(ulong word) pure nothrow @nogc @safe
{
    return word >> generationShift;
}

private ulong wordPhase(ulong word) pure nothrow @nogc @safe
{
    return word & phaseMask;
}

/// One stable identity slot. Slots are never individually freed or moved;
/// they remain addressable until ActorRuntime.destroy.
private align(64) struct ActorSlot
{
    shared ulong lifecycle;
    ActorRuntime* runtime;
    void* state;
    ActorDispatch dispatch;
    shared size_t queueNextWord;
    ulong[2] payloadWords;
    ulong reserved;
    shared size_t inboxHeadWord;
    size_t inboxCarryWord;       // exclusive actor ownership
    shared ulong submissionGate; // closed bit | in-flight send reservations
    ulong[5] inboxPadding;
}
static assert(ActorSlot.sizeof == 128);
static assert(ActorSlot.alignof == 64);

/// Copyable, non-owning submission capability. It deliberately exposes no
/// actor-state pointer or borrowing operation.
struct ActorHandle(T)
{
    private ActorSlot* slot_;
    private ulong generation_;

    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    ActorWakeResult wake() nothrow @nogc @system
    {
        return wakeActor(slot_, generation_);
    }

    /// Submit one caller-owned interaction and schedule this actor. A
    /// successful result transfers the node to the actor until it completes
    /// or explicitly assumes responsibility for the node storage.
    ActorSendResult send(ActorInboxNode* node) nothrow @nogc @system
    {
        return sendActor(slot_, generation_, node);
    }
}

/// Move-only authority for retiring and reclaiming one actor generation.
struct ActorOwner(T)
{
    private ActorSlot* slot_;
    private ulong generation_;

    /// Transfer-only copy, matching Ant Farm's producer Token convention.
    this(ref ActorOwner src) pure nothrow @nogc @system
    {
        slot_ = src.slot_;
        generation_ = src.generation_;
        src.slot_ = null;
        src.generation_ = 0;
    }

    ref ActorOwner opAssign(ActorOwner src) nothrow @nogc @system
    {
        if (slot_ !is null)
            fatal("overwrite live ActorOwner");
        slot_ = src.slot_;
        generation_ = src.generation_;
        return this;
    }

    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    @property ActorHandle!T handle() pure nothrow @nogc @system
    {
        ActorHandle!T result;
        result.slot_ = slot_;
        result.generation_ = generation_;
        return result;
    }

    ActorRetireResult requestRetire() nothrow @nogc @system
    {
        return requestActorRetire(slot_, generation_);
    }

    @property bool retired() const nothrow @nogc @system
    {
        if (!valid) return false;
        immutable word = atomicLoad!(MemoryOrder.acq)(slot_.lifecycle);
        return wordGeneration(word) == generation_
            && wordPhase(word) == phaseRetired;
    }

    /// Destroy POD state storage and make the stable slot available to a new
    /// generation. The owner is invalidated on success.
    ActorReclaimResult reclaim() nothrow @nogc @system
    {
        static assert(__traits(isPOD, T),
            "antfarm_actor A1 supports POD actor state only");
        if (!valid) return ActorReclaimResult.staleOwner;
        immutable word = atomicLoad!(MemoryOrder.acq)(slot_.lifecycle);
        if (wordGeneration(word) != generation_)
            return ActorReclaimResult.staleOwner;
        if (wordPhase(word) != phaseRetired)
            return ActorReclaimResult.busy;
        immutable gate = atomicLoad!(MemoryOrder.acq)(slot_.submissionGate);
        if ((gate & submissionClosedBit) == 0
            || (gate & submissionCountMask) != 0
            || atomicLoad!(MemoryOrder.acq)(slot_.inboxHeadWord) != 0
            || slot_.inboxCarryWord != 0)
            fatal("reclaim actor with live inbox ownership");

        auto runtime = slot_.runtime;
        auto state = slot_.state;
        slot_.state = null;
        slot_.dispatch = null;
        slot_.payloadWords[] = 0;
        runtime.allocator.deallocate(runtime.allocator.context, state,
            T.sizeof, T.alignof);
        atomicFetchSub!(MemoryOrder.acq_rel)(runtime.liveCount, 1UL);
        atomicStore!(MemoryOrder.rel)(slot_.lifecycle,
            lifecycleWord(generation_, phaseVacant));
        slot_ = null;
        generation_ = 0;
        return ActorReclaimResult.reclaimed;
    }
}

private struct ActorBodyRange
{
    ActorSlot*[] slots;

    @property bool empty() const pure nothrow @nogc @safe
    {
        return slots.length == 0;
    }

    @property PayloadBody front() nothrow @nogc @system
    {
        return cast(PayloadBody) slots[0].payloadWords[];
    }

    void popFront() nothrow @nogc { slots = slots[1 .. $]; }

    @property ActorBodyRange save() nothrow @nogc @system { return this; }

    @property size_t length() const pure nothrow @nogc @safe
    {
        return slots.length;
    }
}

/// Fixed-capacity non-GC actor runtime associated with one Farm. Runtime and
/// slots use the supplied allocator and require explicit destruction.
align(64) struct ActorRuntime
{
    @disable this(this);

    private AntFarm* farm;
    private ActorAllocator allocator;
    private ActorSlot* slots;
    private size_t capacity_;
    private shared size_t readyHeadWord;
    private shared ulong readyCount;
    private shared ulong liveCount;
    private shared ulong staleActivations_;
    private PayloadHeader actorHeader;

    static ActorRuntime* create(AntFarm* farm, size_t capacity,
            ActorAllocator allocator = ActorAllocator.cRuntime())
        nothrow @nogc @system
    {
        if (farm is null || capacity == 0 || !allocator.valid)
            return null;
        if (capacity > size_t.max / ActorSlot.sizeof)
            return null;
        auto runtimeMemory = allocator.allocate(allocator.context,
            ActorRuntime.sizeof, ActorRuntime.alignof);
        if (runtimeMemory is null) return null;
        memset(runtimeMemory, 0, ActorRuntime.sizeof);
        auto runtime = cast(ActorRuntime*) runtimeMemory;
        auto slotMemory = allocator.allocate(allocator.context,
            capacity * ActorSlot.sizeof, ActorSlot.alignof);
        if (slotMemory is null)
        {
            allocator.deallocate(allocator.context, runtimeMemory,
                ActorRuntime.sizeof, ActorRuntime.alignof);
            return null;
        }
        memset(slotMemory, 0, capacity * ActorSlot.sizeof);
        runtime.farm = farm;
        runtime.allocator = allocator;
        runtime.slots = cast(ActorSlot*) slotMemory;
        runtime.capacity_ = capacity;
        runtime.actorHeader.maxCs = 1;
        runtime.actorHeader.done = 1;
        runtime.actorHeader.plen = 2;
        runtime.actorHeader.call = &actorPayloadCallback;
        foreach (i; 0 .. capacity)
            runtime.slots[i].runtime = runtime;
        return runtime;
    }

    @property size_t capacity() const pure nothrow @nogc @safe
    {
        return capacity_;
    }

    @property size_t ready() const nothrow @nogc @system
    {
        return cast(size_t) atomicLoad!(MemoryOrder.acq)(readyCount);
    }

    @property size_t live() const nothrow @nogc @system
    {
        return cast(size_t) atomicLoad!(MemoryOrder.acq)(liveCount);
    }

    @property ulong staleActivations() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(staleActivations_);
    }

    /// Allocate and copy-construct POD actor state. Returns an invalid owner
    /// when capacity or allocator storage is unavailable.
    ActorOwner!T createActor(T, alias handler)(T initial)
        nothrow @nogc @system
    {
        static assert(__traits(isPOD, T),
            "antfarm_actor A1 supports POD actor state only");
        static assert(is(typeof(&handler) : void function(
                ref ActorBorrow!T, ref ActorContext) nothrow @nogc @system),
            "actor handler must be void function(ref ActorBorrow!T, ref ActorContext) nothrow @nogc @system");

        ActorSlot* slot;
        ulong generation;
        foreach (i; 0 .. capacity_)
        {
            auto candidate = &slots[i];
            auto observed = atomicLoad!(MemoryOrder.acq)(candidate.lifecycle);
            if (wordPhase(observed) != phaseVacant) continue;
            generation = wordGeneration(observed) + 1;
            if (generation == 0 || generation > maxGeneration)
                fatal("actor generation wrap");
            immutable replacement = lifecycleWord(
                generation, phaseConstructing);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &candidate.lifecycle, observed, replacement))
            {
                slot = candidate;
                break;
            }
        }
        if (slot is null) return ActorOwner!T.init;

        auto state = allocator.allocate(allocator.context, T.sizeof, T.alignof);
        if (state is null)
        {
            atomicStore!(MemoryOrder.rel)(slot.lifecycle,
                lifecycleWord(generation, phaseVacant));
            return ActorOwner!T.init;
        }
        memcpy(state, cast(const void*) &initial, T.sizeof);
        atomicStore!(MemoryOrder.raw)(slot.inboxHeadWord, size_t.init);
        slot.inboxCarryWord = 0;
        atomicStore!(MemoryOrder.raw)(slot.submissionGate, 0UL);
        slot.state = state;
        slot.dispatch = &actorDispatch!(T, handler);
        slot.payloadWords[0] = cast(ulong) cast(void*) slot;
        slot.payloadWords[1] = generation;
        atomicFetchAdd!(MemoryOrder.rel)(liveCount, 1UL);
        atomicStore!(MemoryOrder.rel)(slot.lifecycle,
            lifecycleWord(generation, phaseIdle));

        ActorOwner!T result;
        result.slot_ = slot;
        result.generation_ = generation;
        return result;
    }

    /// Publish up to 256 queued actors. Unwritten activations are returned to
    /// the intrusive queue, so Farm backpressure cannot drop an actor.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = 2)
        nothrow @nogc @system
    {
        enum size_t snapshotCapacity = 256;
        if (maximum > snapshotCapacity) maximum = snapshotCapacity;
        if (maximum == 0) return 0;

        auto word = atomicExchange!(MemoryOrder.acq_rel)(
            &readyHeadWord, size_t.init);
        if (word == 0) return 0;

        ActorSlot*[snapshotCapacity] snapshot;
        size_t count;
        while (word != 0 && count < maximum)
        {
            auto slot = cast(ActorSlot*) cast(void*) word;
            word = atomicLoad!(MemoryOrder.raw)(slot.queueNextWord);
            atomicStore!(MemoryOrder.raw)(slot.queueNextWord, size_t.init);
            snapshot[count++] = slot;
            atomicFetchSub!(MemoryOrder.rel)(readyCount, 1UL);
        }
        // The detached remainder was removed from readyHead but not from the
        // count. Remove and requeue each node so accounting stays exact.
        while (word != 0)
        {
            auto slot = cast(ActorSlot*) cast(void*) word;
            word = atomicLoad!(MemoryOrder.raw)(slot.queueNextWord);
            atomicStore!(MemoryOrder.raw)(slot.queueNextWord, size_t.init);
            atomicFetchSub!(MemoryOrder.rel)(readyCount, 1UL);
            pushReady(slot);
        }

        auto bodies = ActorBodyRange(snapshot[0 .. count]);
        immutable written = cast(size_t) farm.write(
            actorHeader, bodies, 2, token, avgCost);
        foreach (i; written .. count)
            pushReady(snapshot[i]);
        return written;
    }

    /// Requires every actor to have been retired and reclaimed. Handles are
    /// invalid after runtime destruction.
    void destroy() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.acq)(liveCount) != 0)
            fatal("destroy actor runtime with live actors");
        if (atomicLoad!(MemoryOrder.acq)(readyCount) != 0
            || atomicLoad!(MemoryOrder.acq)(readyHeadWord) != 0)
            fatal("destroy actor runtime with queued actors");
        auto savedAllocator = allocator;
        auto savedSlots = slots;
        immutable savedCapacity = capacity_;
        savedAllocator.deallocate(savedAllocator.context, savedSlots,
            savedCapacity * ActorSlot.sizeof, ActorSlot.alignof);
        savedAllocator.deallocate(savedAllocator.context, cast(void*) &this,
            ActorRuntime.sizeof, ActorRuntime.alignof);
    }

private:
    void pushReady(ActorSlot* slot) nothrow @nogc @system
    {
        immutable replacement = cast(size_t) cast(void*) slot;
        // Count before publishing the node so a fast flusher cannot detach it
        // and decrement an as-yet-unincremented count. A transient overcount
        // while the link CAS is in flight is harmless.
        atomicFetchAdd!(MemoryOrder.rel)(readyCount, 1UL);
        auto observed = atomicLoad!(MemoryOrder.acq)(readyHeadWord);
        while (true)
        {
            atomicStore!(MemoryOrder.raw)(slot.queueNextWord, observed);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &readyHeadWord, observed, replacement))
                return;
            observed = atomicLoad!(MemoryOrder.acq)(readyHeadWord);
        }
    }

    void enter(ActorSlot* slot, ulong generation) nothrow @nogc @system
    {
        auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
        while (true)
        {
            if (wordGeneration(observed) != generation
                || wordPhase(observed) != phaseScheduled)
            {
                atomicFetchAdd!(MemoryOrder.rel)(staleActivations_, 1UL);
                return;
            }
            immutable retiring = (observed & retireBit) != 0;
            immutable hasInbox = slot.inboxCarryWord != 0
                || atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord) != 0;
            if (retiring && !hasInbox)
            {
                immutable gate = atomicLoad!(MemoryOrder.acq)(
                    slot.submissionGate);
                if ((gate & submissionClosedBit) != 0
                    && (gate & submissionCountMask) == 0)
                {
                    immutable retiredWord = lifecycleWord(
                        generation, phaseRetired);
                    if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                            &slot.lifecycle, observed, retiredWord))
                        return;
                }
                else
                {
                    immutable scheduledWord = lifecycleWord(
                        generation, phaseScheduled) | retireBit;
                    if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                            &slot.lifecycle, observed, scheduledWord))
                    {
                        pushReady(slot);
                        return;
                    }
                }
            }
            else
            {
                immutable runningWord = lifecycleWord(
                    generation, phaseRunning)
                    | (retiring ? retireBit : 0);
                if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                        &slot.lifecycle, observed, runningWord))
                    break;
            }
            observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
        }

        ActorContext context;
        context.slot_ = slot;
        context.inboxLocal_ = cast(ActorInboxNode*) cast(void*)
            slot.inboxCarryWord;
        slot.inboxCarryWord = 0;
        slot.dispatch(slot.state, &context);
        slot.inboxCarryWord = cast(size_t) cast(void*) context.inboxLocal_;

        if (context.retire_)
        {
            if (!markActorRetiring(slot, generation))
                fatal("running actor lost generation during self-retirement");
            closeSubmissionGate(slot);
        }

        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
        while (true)
        {
            if (wordGeneration(observed) != generation
                || wordPhase(observed) != phaseRunning)
            {
                atomicFetchAdd!(MemoryOrder.rel)(staleActivations_, 1UL);
                return;
            }
            immutable mustRetire = (observed & retireBit) != 0;
            immutable hasInbox = slot.inboxCarryWord != 0
                || atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord) != 0;
            ulong nextPhase;
            if (mustRetire)
            {
                immutable gate = atomicLoad!(MemoryOrder.acq)(
                    slot.submissionGate);
                immutable canRetire = (gate & submissionClosedBit) != 0
                    && (gate & submissionCountMask) == 0 && !hasInbox;
                nextPhase = canRetire ? phaseRetired : phaseScheduled;
            }
            else
            {
                immutable mustRepublish = context.republish_
                    || (observed & pendingBit) != 0 || hasInbox;
                nextPhase = mustRepublish ? phaseScheduled : phaseIdle;
            }
            immutable replacement = lifecycleWord(generation, nextPhase)
                | (mustRetire && nextPhase != phaseRetired ? retireBit : 0);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &slot.lifecycle, observed, replacement))
            {
                if (nextPhase == phaseScheduled)
                    pushReady(slot);
                return;
            }
            observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
        }
    }
}

private void closeSubmissionGate(ActorSlot* slot) nothrow @nogc @system
{
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.submissionGate);
    while ((observed & submissionClosedBit) == 0)
    {
        immutable replacement = observed | submissionClosedBit;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.submissionGate, observed, replacement))
            return;
        observed = atomicLoad!(MemoryOrder.acq)(slot.submissionGate);
    }
}

private bool markActorRetiring(ActorSlot* slot, ulong generation)
    nothrow @nogc @system
{
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    while (true)
    {
        if (wordGeneration(observed) != generation)
            return false;
        immutable phase = wordPhase(observed);
        if (phase == phaseRetired)
            return true;
        if (phase == phaseVacant || phase == phaseConstructing)
            return false;
        if ((observed & retireBit) != 0)
            return true;
        immutable replacement = observed | retireBit;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.lifecycle, observed, replacement))
            return true;
        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    }
}

private ActorWakeResult signalActor(ActorSlot* slot, ulong generation,
        bool allowClosing) nothrow @nogc @system
{
    if (slot is null || generation == 0)
        return ActorWakeResult.staleHandle;
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    while (true)
    {
        if (wordGeneration(observed) != generation)
            return ActorWakeResult.staleHandle;
        immutable phase = wordPhase(observed);
        immutable closing = (observed & retireBit) != 0;
        if (phase == phaseRetired || (closing && !allowClosing))
            return ActorWakeResult.closed;
        if (phase == phaseVacant || phase == phaseConstructing)
            return ActorWakeResult.staleHandle;

        ulong replacement;
        ActorWakeResult result;
        if (phase == phaseIdle)
        {
            replacement = lifecycleWord(generation, phaseScheduled)
                | (closing ? retireBit : 0);
            result = ActorWakeResult.queued;
        }
        else
        {
            // Always perform an RMW, even when pending is already set. A
            // mailbox producer needs its publication ordered against the
            // callback's scheduled-to-running acquire.
            replacement = observed | pendingBit;
            result = ActorWakeResult.coalesced;
        }
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.lifecycle, observed, replacement))
        {
            if (phase == phaseIdle)
                slot.runtime.pushReady(slot);
            return result;
        }
        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    }
}

private ActorWakeResult wakeActor(ActorSlot* slot, ulong generation)
    nothrow @nogc @system
{
    return signalActor(slot, generation, false);
}

private void driveActorRetirement(ActorSlot* slot, ulong generation)
    nothrow @nogc @system
{
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    while (true)
    {
        if (wordGeneration(observed) != generation)
            return;
        immutable phase = wordPhase(observed);
        if (phase == phaseRetired || phase == phaseVacant)
            return;
        if ((observed & retireBit) == 0)
            return;
        if (phase == phaseIdle)
        {
            immutable gate = atomicLoad!(MemoryOrder.acq)(
                slot.submissionGate);
            immutable empty = (gate & submissionClosedBit) != 0
                && (gate & submissionCountMask) == 0
                && atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord) == 0
                && slot.inboxCarryWord == 0;
            immutable nextPhase = empty ? phaseRetired : phaseScheduled;
            immutable replacement = lifecycleWord(generation, nextPhase)
                | (empty ? 0 : retireBit);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &slot.lifecycle, observed, replacement))
            {
                if (!empty) slot.runtime.pushReady(slot);
                return;
            }
        }
        else
        {
            // This same-word RMW orders the closed submission gate against a
            // scheduled entry or forces a running exit CAS to reload it.
            cast(void) signalActor(slot, generation, true);
            return;
        }
        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    }
}

private ActorRetireResult requestActorRetire(ActorSlot* slot,
        ulong generation) nothrow @nogc @system
{
    if (slot is null || generation == 0)
        return ActorRetireResult.staleOwner;
    immutable opening = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(opening) != generation
        || wordPhase(opening) == phaseVacant
        || wordPhase(opening) == phaseConstructing)
        return ActorRetireResult.staleOwner;
    if (wordPhase(opening) == phaseRetired)
        return ActorRetireResult.alreadyRetired;
    if (!markActorRetiring(slot, generation))
        return ActorRetireResult.staleOwner;
    closeSubmissionGate(slot);
    driveActorRetirement(slot, generation);
    return ActorRetireResult.requested;
}

private bool reserveSubmission(ActorSlot* slot, ulong generation,
        out ActorSendResult failure) nothrow @nogc @system
{
    if (slot is null || generation == 0)
    {
        failure = ActorSendResult.staleHandle;
        return false;
    }
    immutable opening = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(opening) != generation
        || wordPhase(opening) == phaseVacant
        || wordPhase(opening) == phaseConstructing)
    {
        failure = ActorSendResult.staleHandle;
        return false;
    }
    if (wordPhase(opening) == phaseRetired || (opening & retireBit) != 0)
    {
        failure = ActorSendResult.closed;
        return false;
    }

    auto gate = atomicLoad!(MemoryOrder.acq)(slot.submissionGate);
    while (true)
    {
        if ((gate & submissionClosedBit) != 0)
        {
            failure = ActorSendResult.closed;
            return false;
        }
        if ((gate & submissionCountMask) == submissionCountMask)
            fatal("actor inbox submission count wrap");
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.submissionGate, gate, gate + 1))
            break;
        gate = atomicLoad!(MemoryOrder.acq)(slot.submissionGate);
    }

    immutable confirmed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(confirmed) == generation
        && wordPhase(confirmed) != phaseVacant
        && wordPhase(confirmed) != phaseConstructing
        && wordPhase(confirmed) != phaseRetired)
        return true; // reservation linearized before any concurrent close

    immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
        slot.submissionGate, 1UL);
    if ((previous & submissionCountMask) == 0)
        fatal("actor inbox submission underflow");
    failure = wordGeneration(confirmed) == generation
        ? ActorSendResult.closed : ActorSendResult.staleHandle;
    return false;
}

private void releaseSubmission(ActorSlot* slot) nothrow @nogc @system
{
    immutable previous = atomicFetchSub!(MemoryOrder.acq_rel)(
        slot.submissionGate, 1UL);
    if ((previous & submissionCountMask) == 0)
        fatal("actor inbox submission underflow");
}

private void pushInbox(ActorSlot* slot, ActorInboxNode* node)
    nothrow @nogc @system
{
    immutable replacement = cast(size_t) cast(void*) node;
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord);
    while (true)
    {
        atomicStore!(MemoryOrder.raw)(node.nextWord_, observed);
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.inboxHeadWord, observed, replacement))
            return;
        observed = atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord);
    }
}

private ActorSendResult sendActor(ActorSlot* slot, ulong generation,
        ActorInboxNode* node) nothrow @nogc @system
{
    if (node is null) return ActorSendResult.nodeBusy;
    ActorSendResult failure;
    if (!reserveSubmission(slot, generation, failure))
        return failure;
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.submissionReserved, node);

    uint expected = inboxNodeFree;
    if (!cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
            &node.state_, expected, inboxNodeQueued))
    {
        releaseSubmission(slot);
        return ActorSendResult.nodeBusy;
    }
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.nodeClaimed, node);

    pushInbox(slot, node);
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.inboxPublished, node);
    immutable signalled = signalActor(slot, generation, true);
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.activationSignalled, node);
    releaseSubmission(slot);
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.submissionReleased, node);
    if (signalled == ActorWakeResult.staleHandle
        || signalled == ActorWakeResult.closed)
        fatal("accepted actor inbox send lost its generation");
    return signalled == ActorWakeResult.queued
        ? ActorSendResult.queued : ActorSendResult.coalesced;
}

private ActorInboxNode* popInboxNode(ActorSlot* slot,
        ref ActorInboxNode* local) nothrow @nogc @system
{
    if (local is null)
    {
        auto word = atomicExchange!(MemoryOrder.acq_rel)(
            &slot.inboxHeadWord, size_t.init);
        ActorInboxNode* ordered;
        while (word != 0)
        {
            auto node = cast(ActorInboxNode*) cast(void*) word;
            word = atomicLoad!(MemoryOrder.raw)(node.nextWord_);
            atomicStore!(MemoryOrder.raw)(node.nextWord_,
                cast(size_t) cast(void*) ordered);
            ordered = node;
        }
        local = ordered;
    }
    if (local is null) return null;
    auto result = local;
    local = cast(ActorInboxNode*) cast(void*)
        atomicLoad!(MemoryOrder.raw)(result.nextWord_);
    atomicStore!(MemoryOrder.raw)(result.nextWord_, size_t.init);
    if (atomicLoad!(MemoryOrder.raw)(result.state_) != inboxNodeQueued)
        fatal("actor inbox node state corrupt");
    atomicStore!(MemoryOrder.raw)(result.state_, inboxNodeProcessing);
    return result;
}

private void actorDispatch(T, alias handler)(void* state, ActorContext* context)
    nothrow @nogc @system
{
    ActorBorrow!T borrow;
    borrow.state_ = cast(T*) state;
    handler(borrow, *context);
}

private long actorPayloadCallback(PayloadHeader*, PayloadBody body, ulong)
    nothrow @nogc @system
{
    if (body.length != 2) return 0;
    immutable slotWord = atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + 0));
    immutable generation = atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + 1));
    auto slot = cast(ActorSlot*) cast(void*) slotWord;
    if (slot is null) return 0;
    slot.runtime.enter(slot, generation);
    return 1;
}
