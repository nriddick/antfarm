/++
 + Non-GC actor activations for Ant Farm.
 +
 + Responsibility: the public actor runtime, stable control slots,
 + handles/owners, the callback borrow and context, the intrusive inbox and
 + the ready queue. It depends on `antfarm` and `antfarm.allocation`;
 + `antfarm.actors.wave` and `antfarm.actors.private_actor` build on it.
 + Canonical docs: docs/guide/public-actors.md (usage),
 + docs/reference/actor-lifecycle.md (contract) and
 + docs/design/public-actor-ordering.md (proofs); docs/README.md is the map.
 + Section map: allocator and result enums, node/context/borrow/handle/owner
 + types, ActorRuntime (creation, wake/send/flush, lifecycle dispatch), then
 + the slot, reclaim and wave-payload machinery used by the other modules.
 + The AntfarmActorTestHooks seams are production-elided: ordinary builds
 + contain neither the hooks nor their branches.
 +
 + Actor state lives outside the ring in caller-allocator storage. The ring
 + carries only a stable control-slot address and generation to one permanent
 + trampoline. `ActorHandle` can request execution but cannot dereference the
 + state; only the trampoline can construct an `ActorBorrow` after winning the
 + slot's scheduled-to-running transition.
 +
 + Construction is restricted to POD state, and the provided intrusive actor
 + inboxes are allocator-neutral with submission reservations. Adopted storage,
 + general placement construction, and aggregate retirement fences remain
 + deferred.
 +/
module antfarm.actors.actor;

public import antfarm : AntFarm, PayloadBody, Callback, Tier, Token;

import antfarm.allocation : allocateAligned64, freeAligned64;
import antfarm : avgCostForChunk, fatal;
import core.atomic : MemoryOrder, atomicExchange, atomicFetchAdd,
    atomicFetchSub, atomicLoad, atomicStore, cas, pause;
import core.lifetime : move;
import core.stdc.stdio : fprintf, stderr;
import core.stdc.string : memcpy, memset;

// Cost class of the public flush table: claims of 8 entries.
private enum uint flushAvgCost = avgCostForChunk(8);

// ==== Section: allocator and result enums ====
/// Non-GC allocation policy used for the runtime, stable slots, and actor
/// state. `deallocate` must match `allocate`; both may be backed by an arena,
/// in which case per-object deallocation may intentionally do nothing.
alias ActorAllocate = void* function(void* context, size_t bytes,
    size_t alignment) nothrow @nogc @system;
/// The matching deallocation half of ActorAllocator. `memory`, `bytes` and
/// `alignment` repeat the values passed to `allocate`.
alias ActorDeallocate = void function(void* context, void* memory,
    size_t bytes, size_t alignment) nothrow @nogc @system;

/// Value-type allocator vtable: one context pointer plus the allocate and
/// deallocate halves. Copyable and equality-free; pass it by value to
/// runtime and park constructors. `valid` checks that both halves are set.
/// See_Also:
///     docs/guide/sizing-and-allocation.md (choosing an allocator)
struct ActorAllocator
{
    /// Opaque state passed back to both function pointers.
    void* context;
    /// Allocation half; returns null on failure.
    ActorAllocate allocate;
    /// Deallocation half; must free what `allocate` returned.
    ActorDeallocate deallocate;

    /// True when both function pointers are set.
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

    /// Register allocations as GC root ranges until free. inner must outlive
    /// the adapter and every allocation; callback threads must be registered
    /// with druntime.
    static ActorAllocator gcRooted(const(ActorAllocator)* inner) pure nothrow @nogc @system
    {
        return ActorAllocator(cast(void*) inner, &gcRootedAllocate, &gcRootedDeallocate);
    }
}

private void* gcRootedAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    import core.memory : GC;
    auto inner = cast(const(ActorAllocator)*) context;
    auto memory = inner.allocate(cast(void*) inner.context, bytes, alignment);
    if (memory !is null) GC.addRange(memory, bytes);
    return memory;
}

private void gcRootedDeallocate(void* context, void* memory, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    import core.memory : GC;
    auto inner = cast(const(ActorAllocator)*) context;
    GC.removeRange(memory);
    inner.deallocate(cast(void*) inner.context, memory, bytes, alignment);
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

/// Typed result of ActorHandle.wake and ActorErasedHandle.wake. No result
/// is fatal; every failure leaves the actor's lifecycle untouched.
enum ActorWakeResult : ubyte
{
    /// The actor was idle and this call published its activation.
    queued,
    /// The actor was already scheduled or running; the activation persists.
    coalesced,
    /// A wave currently owns this actor generation; retry after the wave.
    waveOwned,
    /// The handle's generation no longer matches the actor (or is invalid).
    staleHandle,
    /// The actor is retiring or retired; admission is closed.
    closed,
}

/// Typed result of ActorHandle.send and ActorErasedHandle.send. On every
/// value other than `queued`/`coalesced` the node was not transferred.
enum ActorSendResult : ubyte
{
    /// The node was accepted and this call published the activation.
    queued,
    /// The node was accepted; the actor was already scheduled or running.
    coalesced,
    /// A wave currently owns this actor generation; retry after the wave.
    waveOwned,
    /// The handle's generation no longer matches the actor (or is invalid).
    staleHandle,
    /// The actor is retiring or retired; admission is closed.
    closed,
    /// The node was null or not in its caller-defined free state.
    nodeBusy,
}

/// Terminal state of one inbox interaction, read after the sender observes
/// the node available again. Initialization alone is not a reply.
enum ActorNodeOutcome : ubyte
{
    /// The actor returned the node with ActorInboxNode.complete.
    completed,
    /// The actor returned the node with fail(code).
    failed,
    /// The actor returned the node with ActorInboxNode.cancel.
    cancelled,
}

/// Typed result of ActorOwner.requestRetire and ActorErasedOwner.requestRetire.
enum ActorRetireResult : ubyte
{
    /// Retirement in the requested mode is recorded; a drain request on an
    /// actor already draining or cancelling also returns requested. See
    /// ActorRetireMode for what each mode does with accepted interactions.
    requested,
    /// A wave currently owns this actor generation; retry after the wave.
    waveOwned,
    /// Returned for a cancel request on an already-cancelling actor, or for
    /// any request once the actor is retired.
    alreadyRetired,
    /// The owner's generation no longer matches the actor (or is invalid).
    staleOwner,
}

/// True for `queued` and `coalesced`: an activation was published or an
/// existing activation persists. Acceptance does not mean execution finished.
bool accepted(ActorWakeResult result) pure nothrow @nogc @safe
{
    final switch (result)
    {
        case ActorWakeResult.queued:
        case ActorWakeResult.coalesced:
            return true;
        case ActorWakeResult.waveOwned:
        case ActorWakeResult.staleHandle:
        case ActorWakeResult.closed:
            return false;
    }
}

/// True for `queued` and `coalesced`: the node transferred to the actor,
/// whether this send published an activation or joined an existing one.
bool accepted(ActorSendResult result) pure nothrow @nogc @safe
{
    final switch (result)
    {
        case ActorSendResult.queued:
        case ActorSendResult.coalesced:
            return true;
        case ActorSendResult.waveOwned:
        case ActorSendResult.staleHandle:
        case ActorSendResult.closed:
        case ActorSendResult.nodeBusy:
            return false;
    }
}

/// True for `requested` and `alreadyRetired`: retirement was recorded or
/// the actor was already cancelling/retired. Acceptance does not prove `retired`.
bool accepted(ActorRetireResult result) pure nothrow @nogc @safe
{
    final switch (result)
    {
        case ActorRetireResult.requested:
        case ActorRetireResult.alreadyRetired:
            return true;
        case ActorRetireResult.waveOwned:
        case ActorRetireResult.staleOwner:
            return false;
    }
}

@safe pure nothrow @nogc unittest
{
    import std.traits : EnumMembers;

    static foreach (m; EnumMembers!ActorWakeResult)
        assert((cast(ActorWakeResult) m).accepted == (m == ActorWakeResult.queued
            || m == ActorWakeResult.coalesced));
    static foreach (m; EnumMembers!ActorSendResult)
        assert((cast(ActorSendResult) m).accepted == (m == ActorSendResult.queued
            || m == ActorSendResult.coalesced));
    static foreach (m; EnumMembers!ActorRetireResult)
        assert((cast(ActorRetireResult) m).accepted == (m == ActorRetireResult.requested
            || m == ActorRetireResult.alreadyRetired));
}

/// Drain lets handlers process accepted interactions. Cancel additionally
/// makes `ActorContext.cancelling` true so handlers can `cancelInbox`; it is
/// cooperative, and the library never discards nodes itself.
enum ActorRetireMode : ubyte
{
    /// Close admission, then retire once accepted interactions are handled.
    drain,
    /// Also mark the generation cancelling so handlers cancel remaining
    /// inbox nodes instead of processing them.
    cancel,
}

/// Typed result of ActorOwner.reclaim and ActorErasedOwner.reclaim.
enum ActorReclaimResult : ubyte
{
    /// The POD state was deallocated and the slot is reusable; the owner
    /// became invalid.
    reclaimed,
    /// The actor has not retired yet, or a wave still holds membership.
    busy,
    /// The owner's generation no longer matches the actor (or is invalid).
    staleOwner,
}

// Test-only synchronization seams, elided in production builds.
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
        waveLifecycleReserved,
        waveMembershipReleased,
        waveTableProgressRecorded,
        privateRootReturnBeforeCas,
        privateWaveCreditReleased,
        privateWaveBeforeIdlePublish,
        privateRootCustodyBeforeRelease,
        privateRootCreditBeforeRelease,
        privateRootAfterIdleBeforeCredit,
        privateRootReadyBeforeCas,
        privateChildCustodyBeforeRelease,
        privateChildCustodyAfterRelease,
        privateChildReturnBeforeCas,
        privateResumeBeforeUnschedule,
        privateResumeAfterUnschedule,
        readyDetached,
        retirementIdleObserved,
        actorNodeBeforeCompletionStore,
        privateEmitBeforeCancelCheck,
        privateTruncateBeforeSeal,
        privateEmitBeforeSeal,
        privateRootCancelBeforeCas,
        privateRootTerminalBeforeCas,
        privateTakeBeforeVacantStore,
    }

    alias ActorTestHook = void function(ActorTestPoint point,
        ActorInboxNode* node) nothrow @nogc @system;

    private __gshared ActorTestHook actorTestHook_;

    /// Install a process-wide test hook. The caller must change it only while
    /// no actor operation can be executing it.
    void setActorTestHook(ActorTestHook hook) nothrow @nogc @system
    {
        actorTestHook_ = hook;
    }

    package void actorTestPoint(ActorTestPoint point, ActorInboxNode* node)
        nothrow @nogc @system
    {
        auto hook = actorTestHook_;
        if (hook !is null) hook(point, node);
    }
}

private enum uint inboxNodeFree = 0;
private enum uint inboxNodeQueued = 1;
private enum uint inboxNodeProcessing = 2;

// ==== Section: node, context, borrow, handle and owner types ====
/// Intrusive header for one caller-owned interaction. `payload` and `tag` are
/// application fields; the node itself must remain stable from a successful
/// send until the actor calls `complete`, `fail`, or `cancel`, or takes ownership
/// of its storage. A node may belong to only one inbox at a time.
/// See_Also:
///     docs/guide/public-actors.md (inbox usage and the ownership contract)
struct ActorInboxNode
{
    private shared size_t nextWord_;
    private shared uint state_;
    private uint completion_;
    /// Application payload pointer; never touched by the library.
    void* payload;
    /// Application tag; never touched by the library.
    ulong tag;

    /// Initialize freshly allocated, caller-exclusive storage. Do not call
    /// this while the node is queued, processing, or observable by a sender.
    void initialize(void* initialPayload = null, ulong initialTag = 0)
        nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.raw)(nextWord_, size_t.init);
        atomicStore!(MemoryOrder.raw)(state_, inboxNodeFree);
        completion_ = 0;
        payload = initialPayload;
        tag = initialTag;
    }

    /// True while the node is in its caller-defined free state, i.e. not
    /// queued for or owned by an actor.
    @property bool available() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(state_) == inboxNodeFree;
    }

    /// The application payload as a pointer to `T`. A plain cast of
    /// `payload`: the caller owns the storage and the layout, so no size
    /// or alignment is checked here (a debug-only size check would change
    /// the node layout between debug and release builds).
    T* payloadAs(T)() return nothrow @nogc @system
    {
        return cast(T*) payload;
    }

    /// Return a node handed to the actor by `ActorContext.popInbox` to its
    /// caller-defined free state. Read/process payload fields before this
    /// release; another producer may reuse them after observing availability.
    void complete() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != inboxNodeProcessing)
            fatal("complete actor inbox node not owned by actor");
        completion_ = cast(uint) ActorNodeOutcome.completed;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.actorNodeBeforeCompletionStore, &this);
        atomicStore!(MemoryOrder.rel)(state_, inboxNodeFree);
    }

    /// Return a processing node with a domain failure code of at most 24 bits.
    void fail(uint code) nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != inboxNodeProcessing)
            fatal("fail actor inbox node not owned by actor");
        if (code > 0xFF_FFFF)
            fatal("actor inbox node failure code out of range");
        completion_ = (code << 8) | cast(uint) ActorNodeOutcome.failed;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.actorNodeBeforeCompletionStore, &this);
        atomicStore!(MemoryOrder.rel)(state_, inboxNodeFree);
    }

    /// Return a processing node as cancelled, with code zero.
    void cancel() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(state_) != inboxNodeProcessing)
            fatal("cancel actor inbox node not owned by actor");
        completion_ = cast(uint) ActorNodeOutcome.cancelled;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.actorNodeBeforeCompletionStore, &this);
        atomicStore!(MemoryOrder.rel)(state_, inboxNodeFree);
    }

    // The completion variants that also schedule the sender: the release
    // store inside the completion happens before the wake, so a sender
    // activation published by the wake observes `available` and the
    // recorded outcome. Passing a reply target here would grow the node
    // and fix one sender per node, so the sender handle travels in the
    // payload instead.

    /// Complete this node as `complete` would, then schedule `sender` with
    /// the same contract as `ActorHandle.wake`. Accepts `ActorHandle!U`
    /// for any `U`, and the erased handle. Returns the wake result;
    /// `staleHandle` and `closed` report a sender that no longer accepts
    /// activation, not a lost reply. Read every payload field before this
    /// call; the node must not be touched afterwards, because the woken
    /// sender may reuse it as soon as it observes availability.
    ActorWakeResult completeAndWake(H)(H sender) nothrow @nogc @system
        if (is(H == ActorErasedHandle) || is(H == ActorHandle!U, U))
    {
        complete();
        return sender.wake();
    }

    /// Fail this node as `fail` would (same 24-bit code limit), then
    /// schedule `sender`. Same ordering and after-call contract as
    /// `completeAndWake`; the failure code is observable to the woken
    /// sender through `failureCode`.
    ActorWakeResult failAndWake(H)(H sender, uint code) nothrow @nogc @system
        if (is(H == ActorErasedHandle) || is(H == ActorHandle!U, U))
    {
        fail(code);
        return sender.wake();
    }

    /// Cancel this node as `cancel` would, then schedule `sender`. Same
    /// ordering and after-call contract as `completeAndWake`; the woken
    /// sender observes `ActorNodeOutcome.cancelled`.
    ActorWakeResult cancelAndWake(H)(H sender) nothrow @nogc @system
        if (is(H == ActorErasedHandle) || is(H == ActorHandle!U, U))
    {
        cancel();
        return sender.wake();
    }

    /// Read after available acquired completion for this interaction, before
    /// reusing or transferring the node. Initialization alone is not a reply.
    @property ActorNodeOutcome outcome() const nothrow @nogc @system
    {
        return cast(ActorNodeOutcome)(completion_ & 0xFF);
    }

    /// Same ownership/acquire requirement as outcome; zero unless failed.
    @property uint failureCode() const nothrow @nogc @system
    {
        return completion_ >> 8;
    }
}
static assert(ActorInboxNode.sizeof == 32
    && ActorInboxNode.completion_.offsetof == 12);

/// Callback-local control. Actor handlers receive this as `scope ref`.
/// Requests take effect only after the application callback returns and its
/// ActorBorrow has ended.
/// See_Also:
///     docs/guide/public-actors.md, docs/reference/actor-lifecycle.md
struct ActorContext
{
    private bool republish_;
    private bool retire_;
    private ActorSlot* slot_;
    private ActorInboxNode* inboxLocal_;

    /// Request that this actor be scheduled again after the callback
    /// returns (for example because the handler deferred part of its work).
    void republish() pure nothrow @nogc @safe { republish_ = true; }

    /// Request that this actor retire after the callback returns. The
    /// handler must still drain or cancel its accepted interactions.
    void retire() pure nothrow @nogc @safe { retire_ = true; }

    /// True after external or callback-local retirement has closed this
    /// generation. Accepted interactions must be drained before RETIRED.
    @property bool closing() const nothrow @nogc @system
    {
        if (retire_) return true;
        return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & retireBit) != 0;
    }

    /// True after a cancel-mode retirement request; implies `closing`. The
    /// handler should `cancelInbox` rather than process accepted work.
    @property bool cancelling() const nothrow @nogc @system
    {
        return (atomicLoad!(MemoryOrder.acq)(slot_.lifecycle) & cancelBit) != 0;
    }

    /// Pop every queued interaction, including the carried remainder, and
    /// `cancel` it. Returns the number of nodes cancelled.
    size_t cancelInbox() nothrow @nogc @system
    {
        size_t count;
        for (auto node = popInboxNode(slot_, inboxLocal_); node !is null;
                node = popInboxNode(slot_, inboxLocal_))
        {
            node.cancel();
            ++count;
        }
        return count;
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
/// after acquiring serial ownership and passes it by `scope ref` to the
/// application callback. `scope` records the callback-local capability in the
/// function type; escaping `value` remains outside this @system ownership
/// contract and is not made safe by the qualifier.
struct ActorBorrow(T)
{
    package T* state_;
    @disable this(this);

    /// The actor state for the duration of this callback. Field access
    /// through `actor.value.field` needs no parentheses; binding the state
    /// to a reference does: `ref T s = actor.value;` does not compile, while
    /// `ref T s = actor.value();` and `auto p = &actor.value();` do.
    @property ref T value() return nothrow @nogc @system
    {
        return *state_;
    }
}

/// Callback-local, type-erased view used at a resident/module boundary. The
/// requested POD type must exactly match the size and alignment supplied at
/// creation. As with `ActorBorrow`, retaining the returned reference is
/// outside this @system ownership contract.
struct ActorErasedBorrow
{
    private void* state_;
    private size_t stateSize_;
    private size_t stateAlignment_;
    @disable this(this);

    /// The type-erased actor state for the duration of this callback.
    /// The requested POD type must exactly match the size and alignment
    /// supplied at creation; any mismatch is fatal.
    @property ref T value(T)() return nothrow @nogc @system
    {
        static assert(__traits(isPOD, T),
            "type-erased actor state remains POD-only");
        if (T.sizeof != stateSize_ || T.alignof != stateAlignment_)
            fatal("type-erased actor borrow type mismatch");
        return *cast(T*) state_;
    }

    /// The state size recorded at creation.
    @property size_t stateSize() const pure nothrow @nogc @safe
    {
        return stateSize_;
    }

    /// The state alignment recorded at creation.
    @property size_t stateAlignment() const pure nothrow @nogc @safe
    {
        return stateAlignment_;
    }
}

/// Exact non-template dispatch ABI for reloadable-module adapters. The
/// function pointer may name module code, so its generation must remain loaded
/// until the corresponding erased owner has retired and been reclaimed.
alias ActorErasedHandler = void function(scope ref ActorErasedBorrow,
        scope ref ActorContext)
    nothrow @nogc @system;

private enum ulong phaseMask = 0x7;
private enum ulong retireBit = 0x8;
private enum ulong pendingBit = 0x10;
private enum ulong cancelBit = 0x20;
// Sticky across every nonterminal lifecycle rebuild; RETIRED/VACANT clear it.
private enum ulong retireModeMask = retireBit | cancelBit;
private enum uint generationShift = 8;
private enum ulong maxGeneration = ulong.max >> generationShift;
static assert((cancelBit & (phaseMask | retireBit | pendingBit)) == 0
    && cancelBit < (1UL << generationShift));

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
package align(64) struct ActorSlot
{
    shared ulong lifecycle;
    ActorRuntime* runtime;
    void* state;
    ActorErasedHandler dispatch;
    shared size_t queueNextWord;
    ulong[2] payloadWords;
    size_t stateSize;
    size_t stateAlignment;
    shared size_t inboxHeadWord;
    size_t inboxCarryWord;       // exclusive actor ownership
    shared ulong submissionGate; // closed bit | in-flight send reservations
    shared size_t waveOwnerWord; // stable ActorWave address while a member
    ActorSlot* waveNext;         // orchestrator-owned intrusive member list
    ActorSlot* freeNext;         // runtime free-slot gate held; vacant only
    ulong inboxPadding;
}
static assert(ActorSlot.sizeof == 128);
static assert(ActorSlot.alignof == 64);

/// Copyable, non-owning submission capability. It deliberately exposes no
/// actor-state pointer or borrowing operation.
/// See_Also:
///     docs/guide/public-actors.md (waking, sending and retirement)
struct ActorHandle(T)
{
    package ActorSlot* slot_;
    package ulong generation_;

    /// True for a non-default handle. Does not check liveness; wake/send on
    /// a reclaimed or recycled actor report staleHandle.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    /// The generation this handle addresses; matches ActorOwner.generation
    /// of the owner it was derived from.
    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    /// Schedule this actor. Thread-safe; never fatal. Actors reserved by a
    /// wave report waveOwned until the wave finishes.
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

    /// Copy this non-owning capability into its resident, type-erased form.
    @property ActorErasedHandle erased() pure nothrow @nogc @system
    {
        ActorErasedHandle result;
        result.slot_ = slot_;
        result.generation_ = generation_;
        return result;
    }
}

/// Copyable, non-owning actor capability whose ABI does not depend on `T`.
/// The resident-module counterpart of ActorHandle: share it across a
/// module boundary that must not carry the state type.
struct ActorErasedHandle
{
    package ActorSlot* slot_;
    package ulong generation_;

    /// True for a non-default handle. Does not check liveness; wake/send on
    /// a reclaimed or recycled actor report staleHandle.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    /// The generation this handle addresses.
    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    /// Same contract as ActorHandle.wake.
    ActorWakeResult wake() nothrow @nogc @system
    {
        return wakeActor(slot_, generation_);
    }

    /// Same contract as ActorHandle.send.
    ActorSendResult send(ActorInboxNode* node) nothrow @nogc @system
    {
        return sendActor(slot_, generation_, node);
    }
}

/// Move-only, type-erased retirement and reclamation authority. This is the
/// owner form intended for a resident engine registry spanning reloadable
/// module types. Ownership transfer is explicit: copy and lvalue assignment
/// do not compile; use `move` (std.algorithm or core.lifetime) to transfer,
/// or pass the owner by `ref` to lend it.
struct ActorErasedOwner
{
    private ActorSlot* slot_;
    private ulong generation_;

    /// Copying is disabled: transfer ownership with an explicit `move`.
    @disable this(ref typeof(this));

    /// Move-assignment from an explicit `move` source (an rvalue).
    /// Overwriting a live owner is fatal.
    ref ActorErasedOwner opAssign(ActorErasedOwner src)
        nothrow @nogc @system
    {
        if (slot_ !is null)
            fatal("overwrite live ActorErasedOwner");
        slot_ = src.slot_;
        generation_ = src.generation_;
        return this;
    }

    /// True while this owner still holds its actor generation.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    /// The owned actor generation.
    @property ulong generation() const pure nothrow @nogc @safe
    {
        return generation_;
    }

    /// A non-owning submission capability for this generation.
    @property ActorErasedHandle handle() pure nothrow @nogc @system
    {
        ActorErasedHandle result;
        result.slot_ = slot_;
        result.generation_ = generation_;
        return result;
    }

    /// Same contract as `ActorOwner.requestRetire`: cancel mode or a later
    /// cancel after drain sets `cancelling`; cancel returns `alreadyRetired`
    /// once cancelling, even if not yet `retired`; `waveOwned` during a wave,
    /// so retry after it. Drain on a cancelling actor returns `requested` and
    /// never downgrades cancel. No final handler call is guaranteed. Requests
    /// on one owner must not overlap.
    pragma(inline, true)
    ActorRetireResult requestRetire(
            ActorRetireMode mode = ActorRetireMode.drain) nothrow @nogc @system
    {
        return requestActorRetire(slot_, generation_, mode);
    }

    /// True once this generation has reached RETIRED: accepted work drained
    /// or cancelled and every sender's final signal joined.
    @property bool retired() const nothrow @nogc @system
    {
        return actorRetired(slot_, generation_);
    }

    /// Same contract as ActorOwner.reclaim.
    ActorReclaimResult reclaim() nothrow @nogc @system
    {
        return reclaimActor(slot_, generation_);
    }
}

/// Move-only authority for retiring and reclaiming one actor generation.
/// The owner is not copyable: copying one (including through a range
/// algorithm that takes elements by value) is a compile error, and a copy
/// would silently invalidate the source. Transfer ownership with
/// `move` (std.algorithm or core.lifetime), or lend with a `ref` parameter;
/// iterate owner arrays with `foreach (ref o; owners)`. `opAssign` accepts
/// only an rvalue, so `owner = otherOwner` does not compile either; write
/// `owner = move(otherOwner)`. Overwriting a live owner with a moved value
/// is fatal. Dropping a live owner leaks its slot; assertion builds print a
/// diagnostic to stderr. Call `leak()` where reclaim is impossible.
/// See_Also:
///     docs/guide/public-actors.md, docs/reference/actor-lifecycle.md (the
///     drain and shutdown obligations)
struct ActorOwner(T)
{
    private ActorSlot* slot_;
    private ulong generation_;

    /// Copying is disabled: transfer ownership with an explicit `move`.
    @disable this(ref typeof(this));

    /// Move-assignment from an explicit `move` source (an rvalue).
    /// Overwriting a live owner is fatal.
    ref ActorOwner opAssign(ActorOwner src) nothrow @nogc @system
    {
        if (slot_ !is null)
            fatal("overwrite live ActorOwner");
        slot_ = src.slot_;
        generation_ = src.generation_;
        // `src` is a by-value parameter: leave it inert for its destructor.
        src.slot_ = null;
        src.generation_ = 0;
        return this;
    }

    // Assertion builds only, so release builds keep a trivially destructible
    // owner. A diagnostic, not fatal: this also runs while an exception
    // unwinds. Moved-from, reclaimed, leaked, erased and default owners are
    // inert. An owner field in a GC object reports when the collector
    // finalizes it, which may be at program exit.
    version (assert)
    ~this() nothrow @nogc @trusted
    {
        if (slot_ !is null)
            fprintf(stderr, "antfarm warning: ActorOwner dropped without "
                ~ "reclaim (slot %p, generation %llu); call reclaim() or "
                ~ "leak()\n", cast(void*) slot_, cast(ulong) generation_);
    }

    /// True while this owner still holds its actor generation.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return slot_ !is null && generation_ != 0;
    }

    /// A copyable, non-owning submission capability for this generation.
    @property ActorHandle!T handle() pure nothrow @nogc @system
    {
        ActorHandle!T result;
        result.slot_ = slot_;
        result.generation_ = generation_;
        return result;
    }

    /// Close admission and retire once accepted interactions are handled.
    /// Cancel mode, or a later cancel after drain, also sets `cancelling`.
    /// Cancel returns `alreadyRetired` once cancelling, even if not yet
    /// `retired`; `waveOwned` during a wave, so retry after it. Drain on a
    /// cancelling actor returns `requested` and never downgrades cancel. No
    /// final handler call is guaranteed. Requests on one owner must not overlap.
    pragma(inline, true)
    ActorRetireResult requestRetire(
            ActorRetireMode mode = ActorRetireMode.drain) nothrow @nogc @system
    {
        return requestActorRetire(slot_, generation_, mode);
    }

    /// True once this generation has reached RETIRED: accepted work drained
    /// or cancelled and every sender's final signal joined.
    @property bool retired() const nothrow @nogc @system
    {
        return actorRetired(slot_, generation_);
    }

    /// Deallocate POD state storage and make the stable slot available to a
    /// new generation. The owner is invalidated on success.
    ActorReclaimResult reclaim() nothrow @nogc @system
    {
        static assert(__traits(isPOD, T),
            "antfarm actors currently support POD actor state only");
        return reclaimActor(slot_, generation_);
    }

    /// Transfer the unique owner into the resident, type-erased form.
    ActorErasedOwner intoErased() nothrow @nogc @system
    {
        ActorErasedOwner result;
        result.slot_ = slot_;
        result.generation_ = generation_;
        slot_ = null;
        generation_ = 0;
        return result;
    }

    /// Give up ownership without reclaiming. The actor's slot and state are
    /// leaked on purpose and stay allocated until the runtime itself is
    /// leaked or abandoned; existing handles are not invalidated. Use only
    /// where reclaim is impossible, such as after `FarmHost.abandon()` or a
    /// worker failure. Assertion builds otherwise report a live owner
    /// dropped at scope exit. The owner is invalid afterwards.
    void leak() pure nothrow @nogc @safe
    {
        slot_ = null;
        generation_ = 0;
    }
}

/// Non-owning creation adapter suitable for passing across a resident engine
/// boundary. It contains no `T`-dependent layout or template entry point. The
/// runtime must outlive the adapter and every handle/owner created through it.
private alias ActorErasedCreate = ActorErasedOwner function(void* context,
        scope const(void)* initialState, size_t stateSize,
        size_t stateAlignment, ActorErasedHandler handler)
    nothrow @nogc @system;

/// Non-owning creation adapter suitable for passing across a resident
/// engine boundary. It contains no `T`-dependent layout or template entry
/// point. The runtime must outlive the adapter and every handle/owner
/// created through it.
struct ActorErasedAdapter
{
    private void* context_;
    private ActorErasedCreate create_;

    /// True when this adapter is bound to a live runtime.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return context_ !is null && create_ !is null;
    }

    /// Allocate and byte-copy POD state through the runtime's allocator.
    /// Invalid metadata, capacity exhaustion, or allocator failure returns an
    /// invalid owner. The handler and any module code it calls must remain
    /// resident until the returned owner has retired and been reclaimed.
    /// The state lives in allocator-owned storage the garbage collector
    /// does not scan; keep any GC allocation reachable from it alive
    /// elsewhere, or use non-GC memory.
    ActorErasedOwner createActor(scope const(void)* initialState,
            size_t stateSize, size_t stateAlignment,
            ActorErasedHandler handler) nothrow @nogc @system
    {
        if (!valid) return ActorErasedOwner.init;
        return create_(context_, initialState, stateSize, stateAlignment,
            handler);
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

// ==== Section: ActorRuntime (creation, wake/send/flush, lifecycle) ====
/// Fixed-capacity non-GC actor runtime associated with one Farm. Runtime and
/// slots use the supplied allocator and require explicit destruction.
/// See_Also:
///     docs/guide/public-actors.md (creation, pumping with flush, teardown)
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
    private size_t readyCarryWord; // detached backlog; readyDrainGate held
    private shared uint readyDrainGate;
    private shared uint freeSlotGate;
    private ActorSlot* freeSlots;
    private Callback actorHeader;

    /// Create a runtime over `farm` with `capacity` actor slots, allocating
    /// the runtime and slots through `allocator`. Returns null on invalid
    /// arguments, capacity overflow, or allocation failure. The runtime is
    /// not thread-safe to create or destroy; it must outlive every actor,
    /// handle and owner created through it. Release it with destroy() after
    /// every actor has been retired and reclaimed.
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

        runtime.actorHeader = &actorPayloadCallback;
        foreach (i; 0 .. capacity)
        {
            runtime.slots[i].runtime = runtime;
            runtime.slots[i].freeNext = i + 1 < capacity
                ? &runtime.slots[i + 1] : null;
        }
        runtime.freeSlots = runtime.slots;
        return runtime;
    }

    /// The configured actor slot capacity.
    @property size_t capacity() const pure nothrow @nogc @safe
    {
        return capacity_;
    }

    version (AntfarmActorTestHooks)
    {
        /// Preset the previous generation of the next vacant slot. Call only
        /// on a quiescent runtime with no concurrent operations.
        void testPresetFreeSlotGeneration(ulong generation)
            nothrow @nogc @system
        {
            // Debug-only test-hook precondition; valid tests need no release checks.
            version (assert)
                if (!quiescent || freeSlots is null)
                    fatal("preset actor generation without a quiescent free slot");
            // Debug-only test-hook range diagnostic, outside production paths.
            version (assert)
                if (generation > maxGeneration)
                    fatal("preset actor generation out of range");
            atomicStore!(MemoryOrder.raw)(freeSlots.lifecycle,
                lifecycleWord(generation, phaseVacant));
        }
    }

    /// Actors currently queued for Farm publication (an acquire load).
    @property size_t ready() const nothrow @nogc @system
    {
        return cast(size_t) atomicLoad!(MemoryOrder.acq)(readyCount);
    }

    /// Actors currently created and not yet reclaimed (an acquire load).
    @property size_t live() const nothrow @nogc @system
    {
        return cast(size_t) atomicLoad!(MemoryOrder.acq)(liveCount);
    }

    /// True when `destroy` would not abort: no live actor and nothing queued
    /// for publication (the same acquire loads `destroy` checks). Consumers
    /// must still be joined before `destroy`; this proves only the counts.
    @property bool quiescent() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(liveCount) == 0
            && atomicLoad!(MemoryOrder.acq)(readyCount) == 0
            && atomicLoad!(MemoryOrder.acq)(readyHeadWord) == 0
            && readyCarryWord == 0;
    }

    /// Diagnostic counter of payload arrivals whose actor generation had
    /// already moved on; grows only under handle misuse or recycling races.
    @property ulong staleActivations() const nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(staleActivations_);
    }

    /// Return the non-owning, non-template creation boundary intended for a
    /// resident engine to share with reloadable modules.
    @property ActorErasedAdapter erasedAdapter()
        pure nothrow @nogc @system
    {
        ActorErasedAdapter result;
        result.context_ = &this;
        result.create_ = &createActorFromAdapter;
        return result;
    }

    /// Allocate and copy-construct POD actor state. Returns an invalid owner
    /// when capacity or allocator storage is unavailable. The copy lives in
    /// allocator-owned storage the garbage collector does not scan: POD
    /// state holding GC slices or pointers compiles, but the actor does not
    /// keep those allocations alive. Keep them reachable elsewhere for the
    /// actor's lifetime, or store non-GC memory in actor state.
    ActorOwner!T createActor(T, alias handler)(T initial)
        nothrow @nogc @system
    {
        static assert(__traits(isPOD, T),
            "antfarm actors currently support POD actor state only");
        static assert(is(typeof(&handler) : void function(
                scope ref ActorBorrow!T, scope ref ActorContext)
                nothrow @nogc @system),
            "actor handler must be void function(scope ref ActorBorrow!T, scope ref ActorContext) nothrow @nogc @system");

        auto erased = createActorErased(cast(const(void)*) &initial,
            T.sizeof, T.alignof, &actorDispatch!(T, handler));
        ActorOwner!T result;
        result.slot_ = erased.slot_;
        result.generation_ = erased.generation_;
        erased.slot_ = null;
        erased.generation_ = 0;
        return move(result);
    }

private:
    ActorErasedOwner createActorErased(scope const(void)* initialState,
            size_t stateSize, size_t stateAlignment,
            ActorErasedHandler handler) nothrow @nogc @system
    {
        if (initialState is null || stateSize == 0 || handler is null
                || stateAlignment == 0
                || (stateAlignment & (stateAlignment - 1)) != 0)
            return ActorErasedOwner.init;

        auto slot = takeFreeSlot();
        if (slot is null) return ActorErasedOwner.init;
        immutable observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
        // Release guard: one comparison prevents overwriting a live slot's state.
        if (wordPhase(observed) != phaseVacant)
            fatal("create actor from non-vacant free slot");
        immutable generation = wordGeneration(observed) + 1;
        if (generation == 0 || generation > maxGeneration)
            fatal("actor generation wrap");
        atomicStore!(MemoryOrder.rel)(slot.lifecycle,
            lifecycleWord(generation, phaseConstructing));

        auto state = allocator.allocate(allocator.context,
            stateSize, stateAlignment);
        if (state is null)
        {
            returnFreeSlot(slot, generation);
            return ActorErasedOwner.init;
        }
        memcpy(state, initialState, stateSize);
        atomicStore!(MemoryOrder.raw)(slot.inboxHeadWord, size_t.init);
        slot.inboxCarryWord = 0;
        atomicStore!(MemoryOrder.raw)(slot.submissionGate, 0UL);
        atomicStore!(MemoryOrder.raw)(slot.waveOwnerWord, size_t.init);
        slot.waveNext = null;
        slot.state = state;
        slot.dispatch = handler;
        slot.stateSize = stateSize;
        slot.stateAlignment = stateAlignment;
        slot.payloadWords[0] = cast(ulong) cast(void*) slot;
        slot.payloadWords[1] = generation;
        atomicFetchAdd!(MemoryOrder.rel)(liveCount, 1UL);
        atomicStore!(MemoryOrder.rel)(slot.lifecycle,
            lifecycleWord(generation, phaseIdle));

        ActorErasedOwner result;
        result.slot_ = slot;
        result.generation_ = generation;
        return move(result);
    }

public:

    /// Publish up to `maximum` queued actors (default 32, clamped to 256)
    /// and return how many were written. Snapshot work is bounded by
    /// `maximum`, regardless of the backlog. A concurrent snapshot returns zero rather
    /// than waiting; callers retry as for Farm backpressure. Unwritten
    /// activations are returned to the intrusive queue. `avgCost` is the
    /// table's Farm work-cost class (`0 .. MAX_AVG_COST`, consumers claim
    /// `MAX_CHUNK >> avgCost` entries at a time); the default claims 8.
    size_t flush(ref Token token, size_t maximum = 32, uint avgCost = flushAvgCost)
        nothrow @nogc @system
    {
        enum size_t snapshotCapacity = 256;
        if (maximum > snapshotCapacity) maximum = snapshotCapacity;
        if (maximum == 0) return 0;

        ActorSlot*[snapshotCapacity] snapshot = void;
        immutable count = takeReady(snapshot[0 .. maximum]);
        if (count == 0) return 0;

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
            || atomicLoad!(MemoryOrder.acq)(readyHeadWord) != 0
            || readyCarryWord != 0)
            fatal("destroy actor runtime with queued actors");
        auto savedAllocator = allocator;
        auto savedSlots = slots;
        immutable savedCapacity = capacity_;
        savedAllocator.deallocate(savedAllocator.context, savedSlots,
            savedCapacity * ActorSlot.sizeof, ActorSlot.alignof);
        savedAllocator.deallocate(savedAllocator.context, cast(void*) &this,
            ActorRuntime.sizeof, ActorRuntime.alignof);
    }

// ==== Section: slot, reclaim and wave-payload machinery ====
private:
    // Only constant-size list operations run under this gate. Allocation,
    // deallocation, and all actor callbacks execute after it is released.
    // Serializing the list avoids ABA on concurrently recycled slot pointers.
    void lockFreeSlots() nothrow @nogc @system
    {
        while (!cas!(MemoryOrder.acq, MemoryOrder.raw)(
                &freeSlotGate, 0U, 1U))
            while (atomicLoad!(MemoryOrder.raw)(freeSlotGate) != 0) pause();
    }

    ActorSlot* takeFreeSlot() nothrow @nogc @system
    {
        lockFreeSlots();
        scope (exit) atomicStore!(MemoryOrder.rel)(freeSlotGate, 0U);
        auto slot = freeSlots;
        if (slot !is null)
        {
            freeSlots = slot.freeNext;
            slot.freeNext = null;
        }
        return slot;
    }

    void returnFreeSlot(ActorSlot* slot, ulong generation)
        nothrow @nogc @system
    {
        lockFreeSlots();
        scope (exit) atomicStore!(MemoryOrder.rel)(freeSlotGate, 0U);
        atomicStore!(MemoryOrder.rel)(slot.lifecycle,
            lifecycleWord(generation, phaseVacant));
        slot.freeNext = freeSlots;
        freeSlots = slot;
    }

    size_t takeReady(scope ActorSlot*[] snapshot)
        nothrow @nogc @system
    {
        if (!cas!(MemoryOrder.acq, MemoryOrder.raw)(
                &readyDrainGate, 0U, 1U))
            return 0;
        scope (exit) atomicStore!(MemoryOrder.rel)(readyDrainGate, 0U);

        size_t count;
        while (count < snapshot.length)
        {
            if (readyCarryWord == 0)
            {
                readyCarryWord = atomicExchange!(MemoryOrder.acq_rel)(
                    &readyHeadWord, size_t.init);
                if (readyCarryWord == 0) break;
                version (AntfarmActorTestHooks)
                    actorTestPoint(ActorTestPoint.readyDetached, null);
            }
            auto slot = cast(ActorSlot*) cast(void*) readyCarryWord;
            readyCarryWord = atomicLoad!(MemoryOrder.raw)(slot.queueNextWord);
            atomicStore!(MemoryOrder.raw)(slot.queueNextWord, size_t.init);
            snapshot[count++] = slot;
        }
        // The untouched carry stays counted and exclusively owned by the
        // next drain-gate holder. Release the gate before Farm publication:
        // other flushers may publish their disjoint snapshots concurrently.
        if (count != 0)
            atomicFetchSub!(MemoryOrder.rel)(readyCount, cast(ulong) count);
        return count;
    }

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
                        generation, phaseScheduled)
                        | (observed & retireModeMask);
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
                    | (observed & retireModeMask);
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
        ActorErasedBorrow borrow;
        borrow.state_ = slot.state;
        borrow.stateSize_ = slot.stateSize;
        borrow.stateAlignment_ = slot.stateAlignment;
        slot.dispatch(borrow, context);
        slot.inboxCarryWord = cast(size_t) cast(void*) context.inboxLocal_;

        if (context.retire_)
        {
            if (markActorRetiring(slot, generation, retireBit)
                    == ActorRetireResult.staleOwner)
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
                | (nextPhase != phaseRetired ? observed & retireModeMask : 0);
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

private ActorErasedOwner createActorFromAdapter(void* context,
        scope const(void)* initialState, size_t stateSize,
        size_t stateAlignment, ActorErasedHandler handler)
    nothrow @nogc @system
{
    auto runtime = cast(ActorRuntime*) context;
    return runtime.createActorErased(initialState, stateSize,
        stateAlignment, handler);
}

private bool actorRetired(const(ActorSlot)* slot, ulong generation)
    nothrow @nogc @system
{
    if (slot is null || generation == 0) return false;
    immutable word = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    return wordGeneration(word) == generation
        && wordPhase(word) == phaseRetired;
}

pragma(inline, true)
private ActorReclaimResult reclaimActor(ref ActorSlot* slot,
        ref ulong generation) nothrow @nogc @system
{
    if (slot is null || generation == 0)
        return ActorReclaimResult.staleOwner;
    immutable word = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(word) != generation)
        return ActorReclaimResult.staleOwner;
    if (wordPhase(word) != phaseRetired)
        return ActorReclaimResult.busy;
    // A wave may retain aggregate membership after this actor's own work and
    // a racing retirement have completed. Membership pins the stable slot,
    // state, live count, and therefore runtime until aggregate release.
    if (atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord) != 0)
        return ActorReclaimResult.busy;
    immutable gate = atomicLoad!(MemoryOrder.acq)(slot.submissionGate);
    if ((gate & submissionClosedBit) == 0
            || (gate & submissionCountMask) != 0
            || atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord) != 0
            || slot.inboxCarryWord != 0)
        fatal("reclaim actor with live inbox or submission");

    auto runtime = slot.runtime;
    auto state = slot.state;
    immutable stateSize = slot.stateSize;
    immutable stateAlignment = slot.stateAlignment;
    slot.state = null;
    slot.dispatch = null;
    slot.stateSize = 0;
    slot.stateAlignment = 0;
    slot.payloadWords[] = 0;
    slot.waveNext = null;
    runtime.allocator.deallocate(runtime.allocator.context, state,
        stateSize, stateAlignment);
    atomicFetchSub!(MemoryOrder.acq_rel)(runtime.liveCount, 1UL);
    runtime.returnFreeSlot(slot, generation);
    slot = null;
    generation = 0;
    return ActorReclaimResult.reclaimed;
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

// modeBits is retireBit (drain) or retireModeMask (cancel). Drain keeps its
// historical `requested` for an already closing or retired generation; cancel
// reports `alreadyRetired` once its bit is present or the actor has retired.
// Escalation CASes against the full current word rather than returning early
// on retireBit alone.
private ActorRetireResult markActorRetiring(ActorSlot* slot, ulong generation,
        ulong modeBits) nothrow @nogc @system
{
    immutable satisfied = (modeBits & cancelBit) != 0
        ? ActorRetireResult.alreadyRetired : ActorRetireResult.requested;
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    while (true)
    {
        if (wordGeneration(observed) != generation)
            return ActorRetireResult.staleOwner;
        immutable phase = wordPhase(observed);
        if (phase == phaseRetired)
            return satisfied;
        if (phase == phaseVacant || phase == phaseConstructing)
            return ActorRetireResult.staleOwner;
        if ((observed & modeBits) == modeBits)
            return satisfied;
        immutable replacement = observed | modeBits;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.lifecycle, observed, replacement))
            return ActorRetireResult.requested;
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
                | (observed & retireModeMask);
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
    if (slot is null || generation == 0)
        return ActorWakeResult.staleHandle;
    immutable opening = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(opening) != generation
            || wordPhase(opening) == phaseVacant
            || wordPhase(opening) == phaseConstructing)
        return ActorWakeResult.staleHandle;
    if (wordPhase(opening) == phaseRetired
            || (opening & retireBit) != 0)
        return ActorWakeResult.closed;
    if (atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord) != 0)
        return ActorWakeResult.waveOwned;
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
            version (AntfarmActorTestHooks)
                actorTestPoint(ActorTestPoint.retirementIdleObserved, null);
            immutable gate = atomicLoad!(MemoryOrder.acq)(
                slot.submissionGate);
            if (gate == submissionClosedBit)
            {
                // The closed, zero-count gate joins every accepted sender's
                // final signal. Recheck L *after* that acquire: an earlier
                // idle observation may predate a sender's queued activation.
                auto joined = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
                if (joined != observed)
                {
                    observed = joined;
                    continue;
                }
                // RETIRE blocks ordinary wakes and wave reservations. With
                // no sender left and L still idle, no callback or other
                // lifecycle writer can exist. A release store suffices.
                // Debug-only: gate join proves emptiness; avoid an extra retirement load.
                version (assert)
                    if (atomicLoad!(MemoryOrder.acq)(slot.inboxHeadWord) != 0
                        || slot.inboxCarryWord != 0)
                        fatal("idle actor retirement with live inbox");
                atomicStore!(MemoryOrder.rel)(slot.lifecycle,
                    lifecycleWord(generation, phaseRetired));
                return;
            }
            immutable replacement = lifecycleWord(generation, phaseScheduled)
                | (observed & retireModeMask);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                    &slot.lifecycle, observed, replacement))
            {
                slot.runtime.pushReady(slot);
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

// Requests on one owner must be serialized, as the idle fast path's blind
// RETIRED store assumes no other lifecycle writer. An aliased concurrent cancel
// landing after its second load would be overwritten; the inbox is empty
// there, so only that caller's result code (requested) would be inaccurate.
private ActorRetireResult requestActorRetire(ActorSlot* slot,
        ulong generation, ActorRetireMode mode) nothrow @nogc @system
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
    if (atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord) != 0)
        return ActorRetireResult.waveOwned;
    immutable marked = markActorRetiring(slot, generation,
        mode == ActorRetireMode.cancel ? retireModeMask : retireBit);
    if (marked != ActorRetireResult.requested)
        return marked;
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
    if (atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord) != 0)
    {
        releaseSubmission(slot);
        return ActorSendResult.waveOwned;
    }
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

/// Reserve one idle actor for a wave without publishing it to the autonomous
/// ready queue. The scheduled lifecycle pins state through payload execution;
/// aggregate membership then prevents reclamation until the whole wave has
/// finished. Cancellation restores the lifecycle immediately.
package bool reserveActorWavePayload(ActorSlot* slot, ulong generation,
        AntFarm* expectedFarm, size_t stateSize, size_t stateAlignment,
        void* waveOwner, ref ActorSlot* memberHead)
    nothrow @nogc @system
{
    if (slot is null || generation == 0 || expectedFarm is null
            || waveOwner is null
            || slot.runtime is null || slot.runtime.farm !is expectedFarm
            || slot.stateSize != stateSize
            || slot.stateAlignment != stateAlignment)
        return false;

    ulong expected = lifecycleWord(generation, phaseIdle);
    immutable replacement = lifecycleWord(generation, phaseScheduled);
    if (!cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
            &slot.lifecycle, expected, replacement))
        return false;

    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.waveLifecycleReserved, null);

    if (atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord) != 0)
    {
        cancelActorWavePayload(slot, generation, null);
        return false;
    }
    slot.waveNext = memberHead;
    atomicStore!(MemoryOrder.rel)(slot.waveOwnerWord,
        cast(size_t) waveOwner);
    memberHead = slot;
    return true;
}

/// Undo a wave reservation that the Farm did not accept. A wake, send, or
/// retirement racing the reservation is materialized onto the autonomous
/// ready queue instead of being lost.
package void cancelActorWavePayload(ActorSlot* slot, ulong generation,
        void* waveOwner)
    nothrow @nogc @system
{
    if (waveOwner !is null)
    {
        immutable ownerWord = atomicLoad!(MemoryOrder.acq)(
            slot.waveOwnerWord);
        if (ownerWord != cast(size_t) waveOwner)
            fatal("cancel actor wave payload lost membership");
        slot.waveNext = null;
        atomicStore!(MemoryOrder.rel)(slot.waveOwnerWord, size_t.init);
    }
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    bool queue;
    while (true)
    {
        if (wordGeneration(observed) != generation
                || wordPhase(observed) != phaseScheduled)
            fatal("cancel actor wave payload lost reservation");
        immutable interrupted = (observed & (retireBit | pendingBit)) != 0;
        immutable replacement = lifecycleWord(generation,
            interrupted ? phaseScheduled : phaseIdle)
            | (observed & retireModeMask);
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.lifecycle, observed, replacement))
        {
            queue = interrupted;
            break;
        }
        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    }
    if (queue) slot.runtime.pushReady(slot);
}

/// Release membership after the aggregate wave has joined every table. This
/// is the final slot access made by the wave; clearing the owner may allow
/// reclamation and eventual runtime destruction to proceed immediately.
package void releaseActorWaveMembership(ActorSlot* slot, void* waveOwner)
    nothrow @nogc @system
{
    immutable ownerWord = atomicLoad!(MemoryOrder.acq)(slot.waveOwnerWord);
    if (ownerWord != cast(size_t) waveOwner)
        fatal("actor wave completion lost membership");
    slot.waveNext = null;
    atomicStore!(MemoryOrder.rel)(slot.waveOwnerWord, size_t.init);
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.waveMembershipReleased, null);
}

/// Execute a wave's phase-specific operation under the reservation made
/// before table publication. False means another actor control path requested
/// wake/send/retirement during the operation; the operation still completed,
/// and that request is handed back to the autonomous ready queue. The wave
/// reports this as interrupted, not as a structural admission failure.
///
/// Unlike autonomous dispatch, the wave keeps the lifecycle in `scheduled`
/// while the callback runs. The wave reservation is the unique source of a
/// payload for that state, and `waveOwnerWord` prevents later autonomous
/// publication. The acquire verification below joins the prior actor release;
/// the exit CAS both publishes this operation and detects a racing pending or
/// retirement request. A second scheduled-to-running RMW would add no owner.
package bool dispatchActorWavePayload(T, alias operation)(ActorSlot* slot,
        ulong generation) nothrow @nogc @system
{
    auto observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    if (wordGeneration(observed) != generation
            || wordPhase(observed) != phaseScheduled)
        fatal("actor wave dispatch lost reservation");
    bool clean = (observed & (retireBit | pendingBit)) == 0;

    ActorBorrow!T borrow;
    borrow.state_ = cast(T*) slot.state;
    operation(borrow);

    observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    bool queue;
    while (true)
    {
        if (wordGeneration(observed) != generation
                || wordPhase(observed) != phaseScheduled)
            fatal("actor wave operation lost reservation");
        immutable interrupted = (observed & (retireBit | pendingBit)) != 0;
        clean = clean && !interrupted;
        immutable replacement = lifecycleWord(generation,
            interrupted ? phaseScheduled : phaseIdle)
            | (observed & retireModeMask);
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &slot.lifecycle, observed, replacement))
        {
            queue = interrupted;
            break;
        }
        observed = atomicLoad!(MemoryOrder.acq)(slot.lifecycle);
    }
    if (queue) slot.runtime.pushReady(slot);
    return clean;
}

private void actorDispatch(T, alias handler)(
        scope ref ActorErasedBorrow erased,
        scope ref ActorContext context)
    nothrow @nogc @system
{
    ActorBorrow!T borrow;
    borrow.state_ = cast(T*) erased.state_;
    handler(borrow, context);
}

private extern(C) void actorPayloadCallback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    if (body.length != 2) return;
    immutable slotWord = atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + 0));
    immutable generation = atomicLoad!(MemoryOrder.raw)(
        *cast(shared ulong*) (body.ptr + 1));
    auto slot = cast(ActorSlot*) cast(void*) slotWord;
    if (slot is null) return;
    slot.runtime.enter(slot, generation);
    return;
}
