/++
 + Private actors. Roots have scheduling custody in a single-
 + emitter park; children have one private parent and run only in its waves.
 + The emitter reads metadata, never actor state. See PRIVATE_ACTORS.md for
 + the @system borrowing contract and the deliberately limited first slice.
 +/
module actors.private_actor;

import actors.actor : ActorAllocator;
version (AntfarmActorTestHooks)
    import actors.actor : ActorTestPoint, actorTestPoint;
import antfarm : AntFarm, Token, PayloadHeader, PayloadBody,
    TableCompletionHook, fatal;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, atomicExchange,
    atomicFetchAdd, atomicFetchSub, cas;
import core.stdc.string : memcpy, memset;
import core.lifetime : moveEmplace;
import std.traits : hasElaborateDestructor, hasNested, ReturnType;

enum defaultPrivateBatchLimit = 256;

struct PrivateCohortOptions
{
    size_t batchLimit; // 0 inherits the creating context's effective limit
    bool subsets;     // reserve selection and inverse-position storage
}

enum PrivateRootDisposition : ubyte { again, finish, waitChildren, finishAfterChildren }
enum PrivateChildDisposition : ubyte { done, waitChildren }

struct PrivateChildStep(R)
{
    static assert(!is(R == void), "void cohorts return PrivateChildDisposition");
    static assert(!is(R == PrivateChildDisposition) && !is(R == PrivateChildStep!X, X),
        "result type is ambiguous with a continuation step");
private:
    R value_;
    PrivateChildDisposition disposition_;
public:
    static PrivateChildStep done(R value) nothrow @nogc @system
    {
        PrivateChildStep step;
        step.value_ = value;
        step.disposition_ = PrivateChildDisposition.done;
        return step;
    }
    static PrivateChildStep waitChildren() nothrow @nogc @system
    {
        PrivateChildStep step;
        step.disposition_ = PrivateChildDisposition.waitChildren;
        return step;
    }
}

private enum OpKind { plain, result, continuation, resultContinuation }
private template opKind(T, R, alias operation)
{
    alias Ret = ReturnType!operation;
    static if (is(Ret == void)) enum opKind = OpKind.plain;
    else static if (is(Ret == PrivateChildDisposition)) enum opKind = OpKind.continuation;
    else static if (!is(R == void) && is(Ret == PrivateChildStep!R))
        enum opKind = OpKind.resultContinuation;
    else static if (!is(R == void) && is(Ret == R)) enum opKind = OpKind.result;
    else static assert(0, "operation must return void, R, PrivateChildDisposition or PrivateChildStep!R");
    static assert(is(typeof(&operation) : Ret function(scope ref PrivateBorrow!T,
        scope ref PrivateContext) nothrow @nogc @system), "operation must be nothrow @nogc");
}

enum PrivateSpawnResult : ubyte { spawned, capacityFull, allocationFailed, waveActive, retiring }
enum PrivateStartResult : ubyte { started, empty, waveActive, retiring }
enum PrivateRootReclamation : ubyte { emitter, consumer }

/// Snapshot of the last nonempty wave of a cohort, acquired after its join.
struct PrivateWaveOutcome
{
    size_t participants;   // members selected by the last nonempty start/startFinal/startSubset
    size_t dispatched;     // participant prefix that ran the operation (== participants unless truncated)
    uint failures;         // number of fail() reports by participants during the wave
    uint firstFailureCode; // valid iff failures != 0
    size_t firstFailurePosition; // participant position (spawn index, or selection position for a subset)
    bool cancelled;        // this wave's own scope was cancelled by its parent or a peer
    @property bool succeeded() const pure nothrow @nogc @safe
    { return failures == 0 && dispatched == participants; }
}

template stateAllowed(T)
{
    enum stateAllowed = !is(T == class) && !is(T == interface)
        && !hasNested!T
        && (!hasElaborateDestructor!T || __traits(compiles, () nothrow @nogc {
                T* p; destroy!false(*p); }))
        // Only the moving (non-POD) spawn path calls moveEmplace.
        && (__traits(isPOD, T) || __traits(compiles, () nothrow @nogc {
                T a = void; T b = void; moveEmplace(a, b); }));
}

/// Valid only within the callback. Neither this borrow nor pointers/references
/// into its state may escape. Initializers must not retain mutable aliases.
struct PrivateBorrow(T)
{
private:
    T* state_;
    this(T* state) nothrow @nogc @system { state_ = state; }
    @disable this(this);
public:
    @property ref T value() return nothrow @nogc @system { return *state_; }
}

/// Constructed only by a private dispatch. Child-park operations check this
/// authority against the park's immutable parent identity.
/// This context and references/pointers to it must not escape the callback.
struct PrivateContext
{
private:
    Entry* actor_;
    Wave* enclosing_;
    bool retiring_;
    bool ownJoin_; // waves started here join this member, not the enclosing wave
    size_t batchLimit_;
    this(Entry* actor, Wave* enclosing, bool retiring, size_t batchLimit) nothrow @nogc @system
    { actor_ = actor; enclosing_ = enclosing; retiring_ = retiring; batchLimit_ = batchLimit; }
    @disable this(this);
    // A root context has no enclosing wave and reads its Entry; a member's
    // service lives in the ChildBase of the wave that dispatched it.
    Service* service() nothrow @nogc @system
    {
        return enclosing_ is null ? rootService(actor_, enclosing_) : ChildBase.fromWave(enclosing_).service;
    }
public:
    /// R is the per-member result type; void (the default) creates a
    /// signal-only cohort with no result storage.
    PrivateChildPark!(T, R)* createChildren(T, R = void)(size_t capacity)
        nothrow @nogc @system
    {
        if (actor_ is null) fatal("private child park: unauthorized context");
        if (retiring_) return null;
        return PrivateChildPark!(T, R).create(actor_, service, enclosing_ is null, capacity, batchLimit_);
    }
    PrivateChildPark!(T, R)* createChildren(T, R = void)(size_t capacity, size_t childBatchLimit)
        nothrow @nogc @system
    {
        if (actor_ is null) fatal("private child park: unauthorized context");
        if (childBatchLimit == 0) fatal("invalid private child batch limit");
        if (retiring_) return null;
        return PrivateChildPark!(T, R).create(actor_, service, enclosing_ is null, capacity, childBatchLimit);
    }
    PrivateChildPark!(T, R)* createChildren(T, R = void)(size_t capacity, PrivateCohortOptions options)
        nothrow @nogc @system
    {
        if (actor_ is null) fatal("private child park: unauthorized context");
        if (retiring_) return null;
        return PrivateChildPark!(T, R).create(actor_, service, enclosing_ is null, capacity,
            options.batchLimit == 0 ? batchLimit_ : options.batchLimit, options.subsets);
    }

    /// Record a domain failure. Does not end the activation; return normally.
    /// A member aggregates the report on its enclosing wave (call at most once
    /// per member per wave; extra reports are counted and not diagnosed).
    /// A root sets the tracked root's outcome, first code wins; fatal on an
    /// untracked park until root tracking exists.
    void fail(uint code) nothrow @nogc @system
    {
        if (actor_ is null) fatal("private fail: unauthorized context");
        if (enclosing_ is null)
        {
            if (!rootParkTracked(actor_))
                fatal("private root fail: untracked park");
            return; // F3 records the first code on the root record
        }
        recordWaveFailure(enclosing_, actor_, code);
    }

    /// True if a scope containing this activation is cancelled: an enclosing
    /// wave on the path to the root, cancelled by its parent or a peer. Roots
    /// see only root-level cancellation, which needs root tracking. Advisory:
    /// may become true at any time; never becomes false within an activation.
    @property bool cancelled() nothrow @nogc @system
    {
        if (actor_ is null) fatal("private cancelled: unauthorized context");
        if (enclosing_ is null) return false; // root records arrive with tracking
        return scopeCancelled(enclosing_);
    }

    /// Cancel the active wave of every cohort this actor owns, and therefore
    /// their descendants. Idle cohorts are unaffected. Roots and members.
    void cancelChildren() nothrow @nogc @system
    {
        if (actor_ is null) fatal("private cancelChildren: unauthorized context");
        for (auto child = actor_.children; child !is null; child = child.next)
            if (atomicLoad!(MemoryOrder.acq)(child.wave.active) != 0)
                cancelWave(&child.wave);
    }

    /// Cancel this member's enclosing wave: its siblings and their
    /// descendants. Fatal on a root context.
    void cancelPeers() nothrow @nogc @system
    {
        if (actor_ is null) fatal("private cancelPeers: unauthorized context");
        if (enclosing_ is null) fatal("private cancelPeers: root context");
        cancelWave(enclosing_);
    }
}

// Child entries reach the Service through ChildBase.service; their word at
// offset 16 is child-only metadata, never a Service pointer.
private enum ChildStep : ubyte { done, waitChildren, doneRetire }
private enum ubyte childHolding = 1, childCleanup = 2;

static assert(PrivateContext.sizeof == 4 * size_t.sizeof);

private struct Entry
{
    void* state;
    ChildBase* children;
    union
    {
        Service* service; // roots only
        struct // children only
        {
            ChildStep childStep;
            ubyte childFlags;
            ushort childReserved;
            shared uint childPending;
        }
    }
    Entry* next;
    // Mutually exclusive metadata for root versus child entries.
    union
    {
        Wave* enclosing;
        struct
        {
            PrivateRootDisposition disposition;
            bool hasJoinCredits; // dispatch writes; collection reads after table join
            bool cleanup; // emitter sets after all credits join
            shared bool subtreeCancel; // sticky in-tree cancel hint; emitter resets at spawn
            shared uint pending; // custody + direct-wave notification tails
        }
    }
}
version (D_LP64)
{
    static assert(Entry.sizeof == 40 && Entry.alignof == 8);
    static assert(Entry.state.offsetof == 0 && Entry.children.offsetof == 8
        && Entry.service.offsetof == 16 && Entry.next.offsetof == 24);
    static assert(Entry.childStep.offsetof == 16 && Entry.childFlags.offsetof == 17
        && Entry.childReserved.offsetof == 18 && Entry.childPending.offsetof == 20);
    static assert(Entry.enclosing.offsetof == 32 && Entry.disposition.offsetof == 32
        && Entry.hasJoinCredits.offsetof == 33 && Entry.cleanup.offsetof == 34
        && Entry.subtreeCancel.offsetof == 35 && Entry.pending.offsetof == 36);
}

// Only roots have a Service word. Every read of Entry.service goes through
// here with root identity proved by the calling context, not by the word's bits.
private Service* rootService(Entry* entry, Wave* enclosing) nothrow @nogc @system
{
    version (assert)
        if (enclosing !is null || entry.service is null)
            fatal("private entry: service read on a child");
    return entry.service;
}

private struct ChildBase
{
    ChildBase* next;
    Entry* parent;
    Service* service;
    Entry* root; // immutable: the parent if it is a root, else the parent's cohort root
    Wave wave;
    void function(ChildBase*) nothrow @nogc @system dispose;
    Emittable resume;
    Entry* resumeHead, resumeTail;
    size_t resumeCount;
    bool resumeScheduled;

    static ChildBase* fromWave(Wave* wave) nothrow @nogc @system
    {
        return cast(ChildBase*) (cast(ubyte*) wave - ChildBase.wave.offsetof);
    }
}

// A unit the Service pump schedules: it emits at most one table per pass. A
// cohort owns one for its wave; other emitters embed their own.
private struct Emittable
{
    Emittable* next;
    // Returns true only when publication is sealed; can destroy the owner on return.
    bool function(Emittable*, ref Token) nothrow @nogc @system emit;
}

private struct Wave
{
    Emittable emittable; // first: the pump hands this back to the wave's emit
    shared ulong pending;
    shared uint active;
    shared uint failures; // report count; occupies the former padding
    Wave* upstream;
    TableCompletionHook hook;
    void function(Wave*) nothrow @nogc @system finish;
    shared bool cancel; // this wave's own scope; reset by each start
    uint firstFailureCode; // valid iff failures != 0
    Entry* firstFailed;
}
static assert(Wave.emittable.offsetof == 0);
version (D_LP64)
{
    static assert(Wave.sizeof == 80);
    static assert(Wave.pending.offsetof == 16 && Wave.active.offsetof == 24
        && Wave.failures.offsetof == 28 && Wave.upstream.offsetof == 32
        && Wave.hook.offsetof == 40 && Wave.finish.offsetof == 56
        && Wave.cancel.offsetof == 64 && Wave.firstFailureCode.offsetof == 68
        && Wave.firstFailed.offsetof == 72);
    static assert(ChildBase.sizeof == 168);
}

// F3 installs per-root records and returns records !is null. Until then
// every park is untracked, so root fail has no application outcome slot.
private bool rootParkTracked(Entry*) nothrow @nogc @system
{
    return false;
}

// Callable on a consumer (context) or the emitter (emit). w is active and
// pinned by the caller's credit, and so is every ancestor wave and parent
// Entry the walk reads (ChildBase.parent/root, child Entry.enclosing are
// immutable while pinned). The summary byte keeps the uncancelled check at
// one acquire load; after the first in-tree cancel the root's tree walks.
private bool scopeCancelled(Wave* w) nothrow @nogc @system
{
    auto base = ChildBase.fromWave(w);
    auto root = base.root;
    if (!atomicLoad!(MemoryOrder.acq)(root.subtreeCancel)) return false;
    while (true)
    {
        if (atomicLoad!(MemoryOrder.acq)(w.cancel)) return true;
        auto parent = base.parent;
        if (parent is root) return false; // never read the root union as a pointer
        w = parent.enclosing;
        base = ChildBase.fromWave(w);
    }
}

// Idempotent single-value stores: the flag first, then the sticky summary.
// The summary orders this writer's flag only, not every independent writer's.
// The parent may store onto a wave that went idle after its active check;
// that is harmless (the next start resets it), so never walk from here.
private void cancelWave(Wave* w) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)(w.cancel, true);
    atomicStore!(MemoryOrder.rel)(ChildBase.fromWave(w).root.subtreeCancel, true);
}

// Checked CAS, like retainJoinCredit: uint.max is fatal before the store so a
// wrap cannot observe zero and elect a second plain (code, Entry) writer.
private void recordWaveFailure(Wave* wave, Entry* member, uint code) nothrow @nogc @system
{
    auto old = atomicLoad!(MemoryOrder.raw)(wave.failures);
    while (true)
    {
        if (old == uint.max) fatal("private failure count overflow");
        if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&wave.failures, old, old + 1))
        {
            old = atomicLoad!(MemoryOrder.raw)(wave.failures);
            continue;
        }
        if (old == 0)
        {
            wave.firstFailureCode = code;
            version (AntfarmActorTestHooks)
                privateFailPairSplit();
            wave.firstFailed = member;
        }
        return;
    }
}

// Increment a join counter under an existing hold; reject overflow before changing it.
private void retainJoinCredit(ref shared uint pending) nothrow @nogc @system
{
    auto old = atomicLoad!(MemoryOrder.raw)(pending);
    while (true)
    {
        if (old == uint.max) fatal("private join credit overflow");
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&pending, old, old + 1)) return;
        old = atomicLoad!(MemoryOrder.raw)(pending);
    }
}

version (AntfarmActorTestHooks)
{
    /// Test-only seam: overwrite the running root's join counter so a
    /// legitimate retain can reach the exhaustion check. Never compiled into
    /// production builds; the caller must be inside that root's callback.
    void privateTestSetRootCredits(scope ref PrivateContext context, uint credits)
        nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.raw)(context.actor_.pending, credits);
    }

    void privateTestSetChildCredits(scope ref PrivateContext context, uint credits)
        nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.raw)(context.actor_.childPending, credits);
    }

    /// Test-only: the Entry word at offset 16 of the context's own Entry,
    /// unchecked. Child continuation metadata or the root Service pointer.
    ulong privateTestServiceWord(scope ref PrivateContext context) nothrow @nogc @system
    {
        return *cast(ulong*) &context.actor_.service;
    }

    /// Test-only: the context's own Entry through the checked accessor. A
    /// child Entry is fatal in debug builds.
    void* privateTestRootService(scope ref PrivateContext context) nothrow @nogc @system
    {
        return rootService(context.actor_, context.enclosing_);
    }

    /// Test-only: the Service that createChildren would hand a new cohort.
    void* privateTestContextService(scope ref PrivateContext context) nothrow @nogc @system
    {
        return context.service;
    }

    /// Test-only: seed the enclosing wave's failure count so a legitimate
    /// report can reach the overflow check. The caller must be a member.
    /// A non-zero count also records that member as the representative, so
    /// outcome does not pair the count with an unset Entry.
    void privateTestSetWaveFailures(scope ref PrivateContext context, uint count)
        nothrow @nogc @system
    {
        if (context.actor_ is null || context.enclosing_ is null)
            fatal("private test: wave failures require a member context");
        auto wave = context.enclosing_;
        atomicStore!(MemoryOrder.raw)(wave.failures, count);
        if (count == 0)
            wave.firstFailed = null;
        else
        {
            wave.firstFailureCode = 1;
            wave.firstFailed = context.actor_;
        }
    }

    /// Test-only: the sticky in-tree cancel summary of the context's root.
    bool privateTestSubtreeCancel(scope ref PrivateContext context) nothrow @nogc @system
    {
        auto root = context.enclosing_ is null ? context.actor_
            : ChildBase.fromWave(context.enclosing_).root;
        return atomicLoad!(MemoryOrder.acq)(root.subtreeCancel);
    }

    shared uint privateFailPairPause;
    shared uint privateFailPairWaiting;

    void privateFailPairSplit() nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(privateFailPairPause) != 1) return;
        atomicStore!(MemoryOrder.rel)(privateFailPairWaiting, 1u);
        while (atomicLoad!(MemoryOrder.acq)(privateFailPairPause) == 1) {}
    }

    /// Pause the winning fail between the code store and the Entry store.
    void privateTestArmFailPairPause() nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.raw)(privateFailPairWaiting, 0u);
        atomicStore!(MemoryOrder.rel)(privateFailPairPause, 1u);
    }

    void privateTestReleaseFailPairPause() nothrow @nogc @system
    {
        atomicStore!(MemoryOrder.rel)(privateFailPairPause, 0u);
    }

    bool privateTestFailPairWaiting() nothrow @nogc @system
    {
        return atomicLoad!(MemoryOrder.acq)(privateFailPairWaiting) == 1;
    }
}

private void releaseRoot(Entry* entry) nothrow @nogc @system
{
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.privateRootCreditBeforeRelease, null);
    immutable old = atomicFetchSub!(MemoryOrder.acq_rel)(entry.pending, 1u);
    if (old == 0) fatal("private root credit underflow");
    if (old != 1) return; // no entry access after relinquishing a nonfinal credit
    // Only root batches and child waves whose parent is a root call releaseRoot.
    auto returns = &rootService(entry, null).rootReturns;
    auto head = atomicLoad!(MemoryOrder.raw)(*returns);
    while (true)
    {
        entry.next = cast(Entry*) head;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateRootReadyBeforeCas, null);
        if (cas!(MemoryOrder.rel, MemoryOrder.raw)(returns, head, cast(shared(Entry)*) entry))
            return; // final entry AND service access
        head = atomicLoad!(MemoryOrder.raw)(*returns);
    }
}

private void pushChildReturn(Entry* member) nothrow @nogc @system
{
    auto returns = &ChildBase.fromWave(member.enclosing).service.childReturns;
    auto head = atomicLoad!(MemoryOrder.raw)(*returns);
    while (true)
    {
        member.next = cast(Entry*) head;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateChildReturnBeforeCas, null);
        if (cas!(MemoryOrder.rel, MemoryOrder.raw)(returns, head, cast(shared(Entry)*) member))
            return; // final Entry AND Service access
        head = atomicLoad!(MemoryOrder.raw)(*returns);
    }
}

private void releaseMember(Entry* member) nothrow @nogc @system
{
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.privateChildCustodyBeforeRelease, null);
    immutable old = atomicFetchSub!(MemoryOrder.acq_rel)(member.childPending, 1u);
    if (old == 0) fatal("private child credit underflow");
    if (old != 1) return; // relinquishing a nonfinal credit is the last Entry access
    final switch (member.childStep)
    {
        case ChildStep.waitChildren:
            pushChildReturn(member);
            break;
        case ChildStep.doneRetire:
            member.childFlags |= childCleanup;
            pushChildReturn(member);
            break;
        case ChildStep.done:
            auto wave = member.enclosing;
            member.childFlags = 0;
            releaseWave(wave); // drop the activation hold as the last owner access
            break;
    }
}

private void releaseWave(Wave* wave) nothrow @nogc @system
{
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.privateWaveCreditReleased, null);
    immutable old = atomicFetchSub!(MemoryOrder.acq_rel)(wave.pending, 1UL);
    if (old == 0) fatal("private wave credit underflow");
    if (old == 1) wave.finish(wave);
    // No access after the RMW unless we owned the final credit.
}

private void waveCompleted(void* context) nothrow @nogc @system
{
    releaseWave(cast(Wave*) context);
}

private struct Service
{
    AntFarm* farm;
    ActorAllocator allocator;
    size_t batchLimit;
    shared(Emittable*) incoming;
    Emittable* waiting;
    shared(Entry*) rootReturns;
    // Members that finish waiting, pushed by their final credit release.
    shared(Entry*) childReturns;

    void enqueue(Emittable* emittable) nothrow @nogc @system
    {
        auto head = atomicLoad!(MemoryOrder.raw)(incoming);
        while (true)
        {
            emittable.next = cast(Emittable*) head;
            if (cas!(MemoryOrder.rel, MemoryOrder.raw)(
                    &incoming, head, cast(shared(Emittable)*) emittable)) return;
            head = atomicLoad!(MemoryOrder.raw)(incoming);
        }
    }

    void pump(ref Token token) nothrow @nogc @system
    {
        auto fresh = cast(Emittable*) atomicExchange!(MemoryOrder.acq_rel)(&incoming, cast(shared(Emittable)*) null);
        while (fresh !is null)
        {
            auto next = fresh.next;
            fresh.next = waiting;
            waiting = fresh;
            fresh = next;
        }
        if (atomicLoad!(MemoryOrder.raw)(childReturns) !is null)
        {
            auto returned = cast(Entry*) atomicExchange!(MemoryOrder.acq_rel)(
                &childReturns, cast(shared(Entry)*) null);
            Entry* fifo;
            while (returned !is null)
            {
                auto next = returned.next;
                returned.next = fifo;
                fifo = returned;
                returned = next;
            }
            while (fifo !is null)
            {
                auto member = fifo;
                fifo = member.next;
                auto base = ChildBase.fromWave(member.enclosing);
                member.next = null;
                if (base.resumeHead is null) base.resumeHead = member;
                else base.resumeTail.next = member;
                base.resumeTail = member;
                ++base.resumeCount;
                if (!base.resumeScheduled)
                {
                    base.resumeScheduled = true;
                    atomicFetchAdd!(MemoryOrder.acq_rel)(base.wave.pending, 1UL);
                    base.resume.next = waiting;
                    waiting = &base.resume;
                }
            }
        }
        // One pass, one table per descriptor: backpressure never blocks a worker.
        auto current = waiting;
        waiting = null;
        while (current !is null)
        {
            auto next = current.next;
            if (!current.emit(current, token))
            {
                current.next = waiting;
                waiting = current;
            }
            current = next;
        }
    }
}

private bool childrenIdle(Entry* entry) nothrow @nogc @system
{
    for (auto child = entry.children; child !is null; child = child.next)
        if (atomicLoad!(MemoryOrder.acq)(child.wave.active) != 0) return false;
    return true;
}

private void disposeChildren(Entry* entry) nothrow @nogc @system
{
    if (!childrenIdle(entry)) fatal("retire private actor with active child wave");
    auto child = entry.children;
    entry.children = null;
    while (child !is null)
    {
        auto next = child.next;
        child.dispose(child);
        child = next;
    }
}

private void destroyState(T)(Entry* entry) nothrow @nogc @system
{
    static if (hasElaborateDestructor!T)
        destroy!false(*cast(T*) entry.state);
}

// Shared retirement hook for terminal dispatches and child cleanup activations.
private void retireMember(T)(Service* service, Entry* entry) nothrow @nogc @system
{
    disposeChildren(entry);
    destroyState!T(entry);
    freeMemory(service, entry.state, T.sizeof, T.alignof);
    entry.state = null;
}

private void* allocateZero(Service* service, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    auto memory = service.allocator.allocate(service.allocator.context, bytes, alignment);
    if (memory !is null) memset(memory, 0, bytes);
    return memory;
}

private void freeMemory(Service* service, void* memory, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (memory !is null)
        service.allocator.deallocate(service.allocator.context, memory, bytes, alignment);
}

private struct Batch
{
    TableCompletionHook hook;
    Batch* next;
    shared(Batch*)* returns;
    size_t accepted;
    Entry** entries;
}
static assert(Batch.sizeof == 6 * size_t.sizeof,
    "private actor descriptor-overflow test assumes six machine words per Batch");

private void rootsCompleted(void* context) nothrow @nogc @system
{
    auto batch = cast(Batch*) context;
    auto returns = batch.returns;
    auto head = atomicLoad!(MemoryOrder.raw)(*returns);
    while (true)
    {
        batch.next = cast(Batch*) head;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateRootReturnBeforeCas, null);
        if (cas!(MemoryOrder.rel, MemoryOrder.raw)(returns, head, cast(shared(Batch)*) batch))
            return; // Publication is the last access to batch AND returns.
        head = atomicLoad!(MemoryOrder.raw)(*returns);
    }
}

private struct RootBodies
{
    Entry*[] entries;
    Batch* batch;
    ulong[1] scratch;
    @property bool empty() const nothrow @nogc { return entries.length == 0; }
    @property size_t length() const nothrow @nogc { return entries.length; }
    @property RootBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    {
        scratch[0] = cast(ulong) entries[0];
        return scratch[];
    }
    void popFront() nothrow @nogc { entries = entries[1 .. $]; }
    bool prepareTableFrontN(size_t n) nothrow @nogc
    {
        batch.accepted = n;
        return true;
    }
}

/// Single-emitter scheduling custody for self-owned roots. spawn
/// moves non-POD state (copies POD initializer bytes); it returns no
/// actor handle. All methods on the park must be called by one emitter
/// at a time (including live and destroy), without
/// re-entry from callbacks or allocator hooks. Returning finish while a child
/// wave is active is a fatal contract error. waitChildren joins all direct waves
/// and their descendants before exactly one later user callback; finishAfterChildren
/// joins them before an internal consumer cleanup dispatch with no further user call.
struct PrivateRootPark(T, alias step,
    PrivateRootReclamation reclamation = PrivateRootReclamation.emitter)
{
    static assert(is(typeof(&step) : PrivateRootDisposition function(
        scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system));
private:
    Service service_;
    Entry* entries_;
    size_t capacity_;
    size_t live_;
    Entry* free_;
    Entry* ready_;
    Entry* readyTail_;
    Batch* batches_;
    Entry** batchEntries_;
    size_t batchCount_;
    size_t storageLimit_;
    Batch* spare_;
    shared(Batch*) returned_;
    version (assert) bool entered_;
    @disable this(this);
public:
    static PrivateRootPark* create(AntFarm* farm, size_t capacity,
        size_t batchLimit = defaultPrivateBatchLimit,
        ActorAllocator allocator = ActorAllocator.cRuntime()) nothrow @nogc @system
    {
        if (batchLimit == 0) fatal("invalid private root batch limit");
        if (farm is null || !allocator.valid || capacity == 0
            || capacity > size_t.max / Entry.sizeof) return null;
        immutable storageLimit = batchLimit < capacity ? batchLimit : capacity;
        immutable batchCount = (capacity - 1) / storageLimit + 1;
        if (batchCount > size_t.max / Batch.sizeof
            || storageLimit > size_t.max / batchCount) return null;
        immutable batchSlots = batchCount * storageLimit;
        if (batchSlots > size_t.max / (Entry*).sizeof) return null;
        auto park = cast(PrivateRootPark*) allocator.allocate(allocator.context,
            PrivateRootPark.sizeof, PrivateRootPark.alignof);
        if (park is null) return null;
        memset(park, 0, PrivateRootPark.sizeof);
        park.service_ = Service(farm, allocator, batchLimit);
        park.capacity_ = capacity;
        park.batchCount_ = batchCount;
        park.storageLimit_ = storageLimit;
        park.entries_ = cast(Entry*) allocateZero(&park.service_, capacity * Entry.sizeof, Entry.alignof);
        park.batches_ = cast(Batch*) allocateZero(&park.service_, park.batchCount_ * Batch.sizeof, Batch.alignof);
        park.batchEntries_ = cast(Entry**) allocateZero(&park.service_, batchSlots * (Entry*).sizeof, (Entry*).alignof);
        if (park.entries_ is null || park.batches_ is null || park.batchEntries_ is null)
        {
            freeMemory(&park.service_, park.entries_, capacity * Entry.sizeof, Entry.alignof);
            freeMemory(&park.service_, park.batches_, park.batchCount_ * Batch.sizeof, Batch.alignof);
            freeMemory(&park.service_, park.batchEntries_, batchSlots * (Entry*).sizeof, (Entry*).alignof);
            allocator.deallocate(allocator.context, park, PrivateRootPark.sizeof, PrivateRootPark.alignof);
            return null;
        }
        foreach (i; 0 .. capacity)
        {
            park.entries_[i].service = &park.service_; // root-only write
            park.entries_[i].next = park.free_;
            park.free_ = &park.entries_[i];
        }
        foreach (i; 0 .. park.batchCount_)
        {
            auto batch = &park.batches_[i];
            batch.entries = park.batchEntries_ + i * storageLimit;
            batch.hook = TableCompletionHook(batch, &rootsCompleted);
            batch.returns = &park.returned_;
            batch.next = park.spare_;
            park.spare_ = batch;
        }
        return park;
    }

    PrivateSpawnResult spawn(T initial) nothrow @nogc @system
    {
        static assert(stateAllowed!T, "private actor state: struct/scalar with nothrow @nogc destructor");
        enter();
        scope(exit) { version (assert) entered_ = false; }
        static if (hasElaborateDestructor!T)
            bool spawned;
        scope(exit)
        {
            static if (hasElaborateDestructor!T)
                // object.destroy is exactly-once; core.lifetime.move of an
                // over-aligned T runs the destructor during the move on DMD.
                if (!spawned) .destroy(initial);
        }
        if (free_ is null) return PrivateSpawnResult.capacityFull;
        auto state = service_.allocator.allocate(service_.allocator.context, T.sizeof, T.alignof);
        if (state is null) return PrivateSpawnResult.allocationFailed;
        static if (__traits(isPOD, T))
            memcpy(state, &initial, T.sizeof);
        else
            moveEmplace(initial, *cast(T*) state);
        static if (hasElaborateDestructor!T)
            spawned = true;
        auto entry = free_;
        free_ = entry.next;
        entry.state = state;
        entry.disposition = PrivateRootDisposition.again;
        entry.hasJoinCredits = false;
        entry.cleanup = false;
        // Every in-tree writer of the previous root joined before collection.
        atomicStore!(MemoryOrder.raw)(entry.subtreeCancel, false);
        atomicStore!(MemoryOrder.raw)(entry.pending, 1u);
        appendReady(entry);
        ++live_;
        return PrivateSpawnResult.spawned;
    }

    /// Acquire completed physical tables and root wakes, recycle joined terminal
    /// roots, and append ready roots to the emitter's unpublished list. Never publishes work
    /// or executes consumers; poll is an alias.
    void collectCompleted() nothrow @nogc @system
    {
        enter();
        scope(exit) { version (assert) entered_ = false; }
        collectCompletedImpl();
    }
    alias poll = collectCompleted;

private:
    void collectCompletedImpl() nothrow @nogc @system
    {
        auto batch = cast(Batch*) atomicExchange!(MemoryOrder.acq_rel)(&returned_, cast(shared(Batch)*) null);
        while (batch !is null)
        {
            auto next = batch.next;
            foreach (entry; batch.entries[0 .. batch.accepted])
            {
                if (entry.disposition == PrivateRootDisposition.again)
                    appendReady(entry); // custody retained across again
                else if (entry.hasJoinCredits || entry.disposition != PrivateRootDisposition.finish)
                {
                    version (AntfarmActorTestHooks)
                        actorTestPoint(ActorTestPoint.privateRootCustodyBeforeRelease, null);
                    releaseRoot(entry); // final collector Entry access; Batch is independent
                }
                else reclaim(entry); // leaf finish avoids atomic RMW
            }
            batch.next = spare_;
            spare_ = batch;
            batch = next;
        }
        // Acquire and reverse the MPSC handoff into FIFO. No producer uses
        // an Entry/service after its successful publication CAS.
        // Empty hint avoids an extra atomic RMW in parks not using joins.
        // A concurrent push after the hint stays queued for the next collect.
        Entry* fresh;
        if (atomicLoad!(MemoryOrder.raw)(service_.rootReturns) !is null)
            fresh = cast(Entry*) atomicExchange!(MemoryOrder.acq_rel)(
                &service_.rootReturns, cast(shared(Entry)*) null);
        Entry* fifo;
        while (fresh !is null)
        {
            auto next = fresh.next;
            fresh.next = fifo;
            fifo = fresh;
            fresh = next;
        }
        while (fifo !is null)
        {
            auto entry = fifo;
            fifo = entry.next;
            if (entry.disposition == PrivateRootDisposition.finish) reclaim(entry);
            else
            {
                atomicStore!(MemoryOrder.raw)(entry.pending, 1u);
                entry.hasJoinCredits = false;
                entry.cleanup = entry.disposition == PrivateRootDisposition.finishAfterChildren;
                appendReady(entry);
            }
        }
    }

    void reclaim(Entry* entry) nothrow @nogc @system
    {
        static if (reclamation == PrivateRootReclamation.emitter)
            freeMemory(&service_, entry.state, T.sizeof, T.alignof);
        entry.state = null;
        entry.next = free_;
        free_ = entry;
        --live_;
    }

public:
    /// Emits at most one root table and one table per pending child wave.
    /// Returns a count of root activations, including deferred cleanup; zero
    /// does not imply idle. Continue pumping to collect/publish wakes. Never runs
    /// consumers. At most ceil(capacity / configured batchLimit) root tables
    /// can be in flight, including when a call requests a smaller batch.
    /// By default terminal root state is freed here on the emitter; the
    /// consumer policy frees it in the callback. Child frees run on consumers.
    size_t pump(ref Token token) nothrow @nogc @system
    {
        return pump(token, service_.batchLimit);
    }
    size_t pump(ref Token token, size_t batchLimit, uint avgCost = 0)
        nothrow @nogc @system
    {
        enter();
        scope(exit) { version (assert) entered_ = false; }
        if (batchLimit == 0 || batchLimit > service_.batchLimit) fatal("invalid private root batch limit");
        collectCompletedImpl();
        service_.pump(token);
        if (spare_ is null || ready_ is null) return 0;
        auto batch = spare_;
        spare_ = batch.next;
        size_t count;
        while (count < batchLimit && count < storageLimit_ && ready_ !is null)
        {
            auto entry = ready_;
            ready_ = entry.next;
            if (ready_ is null) readyTail_ = null;
            batch.entries[count++] = entry;
        }
        // Detached pointer values, not borrowed state. Even batch may be on
        // the return queue by writeTracked's return, but only this emitter can
        // acquire/reuse it, and pump is non-reentrant.
        PayloadHeader header;
        header.maxCs = header.done = 1;
        header.plen = 1;
        header.call = &runRoot;
        auto bodies = RootBodies(batch.entries[0 .. count], batch);
        immutable n = cast(size_t) service_.farm.writeTracked(header, bodies, 1,
            &batch.hook, token, avgCost);
        foreach (entry; batch.entries[n .. count])
        {
            appendReady(entry);
        }
        if (n == 0)
        {
            batch.next = spare_;
            spare_ = batch;
        }
        return n;
    }

    /// Emitter-local roots, including sleepers and completed callbacks with
    /// outstanding child-notification tails. Drain through collection to zero.
    @property size_t live() const pure nothrow @nogc @safe { return live_; }
    @property size_t capacity() const pure nothrow @nogc @safe { return capacity_; }

    /// Collect once and require no live roots or pending work. This is not a drain.
    /// Use the member call; object.destroy(park) does not free the park.
    void destroy() nothrow @nogc @system
    {
        enter();
        collectCompletedImpl();
        if (live_ != 0 || service_.waiting !is null
            || atomicLoad!(MemoryOrder.acq)(service_.incoming) !is null)
            fatal("destroy nonempty private root park");
        auto allocator = service_.allocator;
        freeMemory(&service_, entries_, capacity_ * Entry.sizeof, Entry.alignof);
        freeMemory(&service_, batches_, batchCount_ * Batch.sizeof, Batch.alignof);
        freeMemory(&service_, batchEntries_, batchCount_ * storageLimit_ * (Entry*).sizeof, (Entry*).alignof);
        allocator.deallocate(allocator.context, &this, PrivateRootPark.sizeof, PrivateRootPark.alignof);
    }
private:
    void enter() nothrow @nogc @system
    {
        version (assert)
        {
            if (entered_) fatal("private root park: reentrant entry");
            entered_ = true;
        }
    }

    void appendReady(Entry* entry) nothrow @nogc @system
    {
        entry.next = null;
        if (readyTail_ is null) ready_ = entry;
        else readyTail_.next = entry;
        readyTail_ = entry;
    }

    static long runRoot(PayloadHeader*, PayloadBody body, ulong) nothrow @nogc @system
    {
        auto entry = cast(Entry*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
        auto borrow = PrivateBorrow!T(cast(T*) entry.state);
        auto service = rootService(entry, null);
        auto context = PrivateContext(entry, null, false, service.batchLimit);
        auto disposition = entry.cleanup ? PrivateRootDisposition.finish : step(borrow, context);
        entry.disposition = disposition;
        if (disposition == PrivateRootDisposition.finish)
        {
            disposeChildren(entry);
            destroyState!T(entry);
            static if (reclamation == PrivateRootReclamation.consumer)
                freeMemory(service, entry.state, T.sizeof, T.alignof);
        }
        return 1;
    }
}

private struct ChildBodies
{
    Entry[] entries;
    ulong[1] scratch;
    @property bool empty() const nothrow @nogc { return entries.length == 0; }
    @property size_t length() const nothrow @nogc { return entries.length; }
    @property ChildBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    {
        scratch[0] = cast(ulong) &entries[0];
        return scratch[];
    }
    void popFront() nothrow @nogc { entries = entries[1 .. $]; }
}

private struct SubsetBodies
{
    Entry* entries;
    const(uint)[] members;
    ulong[1] scratch;
    @property bool empty() const nothrow @nogc { return members.length == 0; }
    @property size_t length() const nothrow @nogc { return members.length; }
    @property SubsetBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    {
        scratch[0] = cast(ulong) &entries[members[0]];
        return scratch[];
    }
    void popFront() nothrow @nogc { members = members[1 .. $]; }
}

private struct ResumeBodies
{
    ChildBase* park;
    Entry* cursor;
    size_t remaining;
    ulong[1] scratch;
    @property bool empty() const nothrow @nogc { return remaining == 0; }
    @property size_t length() const nothrow @nogc { return remaining; }
    @property ResumeBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    {
        scratch[0] = cast(ulong) cursor;
        return scratch[];
    }
    void popFront() nothrow @nogc @system { cursor = cursor.next; --remaining; }
    bool prepareTableFrontN(size_t n) nothrow @nogc @system
    {
        auto next = cursor;
        foreach (_; 0 .. n) next = next.next;
        park.resumeHead = next;
        park.resumeCount -= n;
        if (park.resumeCount == 0) park.resumeTail = null;
        version (assert)
            if ((park.resumeCount == 0) != (park.resumeHead is null)
                || (park.resumeHead is null) != (park.resumeTail is null))
                fatal("private resume FIFO invariant");
        // Farm copies from our independent cursor after prepare, before visibility.
        return true;
    }
}

/// Opaque homogeneous cohort. The parent may create members and request a
/// whole-cohort wave, but can never borrow a child's state. No overlapping
/// waves, external dispatches, removal, or reparenting in this first slice.
struct PrivateChildPark(T, R = void)
{
    enum hasResults = !is(R == void);
    static if (hasResults)
    {
        static assert(!is(R == const) && !is(R == immutable)
            && !is(R == shared) && !is(R == inout),
            "private child result type must be unqualified (not const/immutable/shared/inout)");
        static assert(__traits(isPOD, R), "private child results require POD values");
        static assert(!is(R == PrivateChildDisposition) && !is(R == PrivateChildStep!X, X),
            "result type is ambiguous with a continuation step");
    }
private:
    ChildBase base_; // first: erased disposal and wave-to-park conversion
    Entry* entries_;
    size_t capacity_, count_, cursor_, batchLimit_;
    // Last nonempty wave. The parent writes both before the enqueue; the
    // emitter alone lowers dispatched_ on truncation, before its seal.
    size_t participants_, dispatched_;
    bool retire_, isSubset_;
    bool parentIsRoot_; // fixed at creation: the creating context had no enclosing wave
    uint* sel_;
    size_t k_;
    PayloadHeader header_;
    static if (hasResults)
    {
        R* results_;         // capacity_ slots, in participant order
        size_t resultCount_; // members in the last nonempty wave; parent-only
        version (assert) enum resultBytesPerMember = R.sizeof + 1;
        else enum resultBytesPerMember = R.sizeof;

        version (assert)
        {
            ubyte* resultWritten() nothrow @nogc @system
            {
                return cast(ubyte*) (results_ + capacity_);
            }
            void checkResultsWritten() nothrow @nogc @system
            {
                foreach (i; 0 .. resultPrefix)
                    if (resultWritten[i] == 0) fatal("private child result: unwritten slot");
            }
        }
        // Only the dispatched prefix ran the operation. Independent of count_,
        // so a retiring wave's results survive count_ = 0.
        @property size_t resultPrefix() const nothrow @nogc @system
        {
            return resultCount_ < dispatched_ ? resultCount_ : dispatched_;
        }
        void writeResult(size_t slot, R value) nothrow @nogc @system
        {
            version (assert)
            {
                if (resultWritten[slot] != 0) fatal("private child result: slot written twice");
                resultWritten[slot] = 1;
            }
            results_[slot] = value;
        }
    }
    static assert(base_.offsetof == 0,
        "PrivateChildPark.fromWave requires base_ at offset zero");
    @disable this(this);

    static PrivateChildPark* create(Entry* parent, Service* service, bool parentIsRoot,
        size_t capacity, size_t batchLimit, bool subsets = false) nothrow @nogc @system
    {
        static assert(stateAllowed!T, "private actor state: struct/scalar with nothrow @nogc destructor");
        if (capacity == 0 || capacity > size_t.max / Entry.sizeof) return null;
        if (subsets && capacity > uint.max) return null;
        static if (hasResults)
            if (capacity > size_t.max / resultBytesPerMember) return null;
        auto park = cast(PrivateChildPark*) allocateZero(service, PrivateChildPark.sizeof, PrivateChildPark.alignof);
        if (park is null) return null;
        park.entries_ = cast(Entry*) allocateZero(service, capacity * Entry.sizeof, Entry.alignof);
        static if (hasResults)
            if (park.entries_ !is null)
            {
                park.results_ = cast(R*) allocateZero(service, capacity * resultBytesPerMember, R.alignof);
                if (park.results_ is null)
                {
                    freeMemory(service, park.entries_, capacity * Entry.sizeof, Entry.alignof);
                    park.entries_ = null;
                }
            }
        if (park.entries_ is null)
        {
            freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
            return null;
        }
        if (subsets)
        {
            park.sel_ = cast(uint*) allocateZero(service, capacity * 2 * uint.sizeof, uint.alignof);
            if (park.sel_ is null)
            {
                static if (hasResults)
                    freeMemory(service, park.results_, capacity * resultBytesPerMember, R.alignof);
                freeMemory(service, park.entries_, capacity * Entry.sizeof, Entry.alignof);
                freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
                return null;
            }
        }
        park.capacity_ = capacity;
        park.batchLimit_ = batchLimit;
        park.base_.parent = parent;
        park.base_.service = service;
        park.base_.root = parentIsRoot ? parent : ChildBase.fromWave(parent.enclosing).root;
        park.parentIsRoot_ = parentIsRoot;
        park.base_.dispose = &dispose;
        park.base_.wave.hook = TableCompletionHook(&park.base_.wave, &waveCompleted);
        park.base_.wave.emittable.emit = &emit;
        park.base_.wave.finish = &finish;
        park.base_.resume.emit = &emitResume;
        park.base_.next = parent.children;
        parent.children = &park.base_;
        return park;
    }

    void authorized(scope ref PrivateContext context) nothrow @nogc @system
    {
        if (context.actor_ is null || context.actor_ !is base_.parent)
            fatal("private child park: unauthorized context");
    }
public:
    /// Acquires the current join; true before the first start. Unauthorized
    /// contexts are fatal. Read joined results after this returns true.
    /// A post-idle root notification may still pin the root Entry and live count.
    bool finished(scope ref PrivateContext context) nothrow @nogc @system
    {
        authorized(context);
        return atomicLoad!(MemoryOrder.acq)(base_.wave.active) == 0;
    }

    /// Acquires the join. All zero before the first nonempty start. Fatal
    /// while a wave is active or with wrong authority. Valid until the next
    /// nonempty start/startFinal/startSubset.
    PrivateWaveOutcome outcome(scope ref PrivateContext context) nothrow @nogc @system
    {
        authorized(context);
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0)
            fatal("private child outcome: wave active");
        PrivateWaveOutcome result;
        result.participants = participants_;
        result.dispatched = dispatched_;
        result.failures = atomicLoad!(MemoryOrder.raw)(base_.wave.failures);
        result.cancelled = atomicLoad!(MemoryOrder.raw)(base_.wave.cancel);
        if (result.failures != 0)
        {
            result.firstFailureCode = base_.wave.firstFailureCode;
            auto index = base_.wave.firstFailed - entries_;
            result.firstFailurePosition = isSubset_ ? sel_[capacity_ + index] : index;
        }
        return result;
    }

    /// Cancel this cohort's active wave and therefore its descendants:
    /// undispatched tables are skipped and running members observe
    /// context.cancelled. Returns false, with no effect, if the cohort is
    /// observed idle. A wave that finishes concurrently still reports
    /// outcome.cancelled: a cancel was requested, not that work was skipped.
    bool cancel(scope ref PrivateContext context) nothrow @nogc @system
    {
        authorized(context);
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) == 0) return false;
        cancelWave(&base_.wave);
        return true;
    }

    static if (hasResults)
    {
        /// Results of the last nonempty wave, in participant order;
        /// empty before the first such wave or after a void-operation wave.
        /// A cancelled wave returns only its dispatched prefix.
        /// Acquires the join. Calling while a wave is active is fatal. Valid
        /// only within this callback and until the next nonempty start/startFinal/startSubset.
        const(R)[] results(scope ref PrivateContext context) nothrow @nogc @system
        {
            authorized(context);
            if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0)
                fatal("private child results: wave active");
            version (assert) checkResultsWritten();
            return results_[0 .. resultPrefix];
        }
    }

    /// Selection for the last nonempty wave, or empty for a whole wave.
    /// Valid within this callback until the next nonempty start.
    const(uint)[] subsetMembers(scope ref PrivateContext context) nothrow @nogc @system
    {
        authorized(context);
        return isSubset_ ? sel_[0 .. k_] : null;
    }

    PrivateSpawnResult spawn(scope ref PrivateContext context, T initial)
        nothrow @nogc @system
    {
        static assert(stateAllowed!T, "private actor state: struct/scalar with nothrow @nogc destructor");
        authorized(context);
        if (context.retiring_) return PrivateSpawnResult.retiring;
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0) return PrivateSpawnResult.waveActive;
        if (count_ == capacity_) return PrivateSpawnResult.capacityFull;
        auto service = base_.service;
        auto state = service.allocator.allocate(service.allocator.context, T.sizeof, T.alignof);
        if (state is null) return PrivateSpawnResult.allocationFailed;
        static if (__traits(isPOD, T))
            memcpy(state, &initial, T.sizeof);
        else
            moveEmplace(initial, *cast(T*) state);
        auto entry = &entries_[count_++];
        entry.state = state;
        entry.enclosing = &base_.wave;
        return PrivateSpawnResult.spawned;
    }

    /// Enqueue a wave under the parent's dispatch authority. Completion also
    /// joins descendant waves started by this operation. retire=true frees
    /// members after their final activation and join. Plain/result terminal
    /// callbacks cannot launch descendants; continuation callbacks can.
    PrivateStartResult start(alias operation)(scope ref PrivateContext context, bool retire = false)
        nothrow @nogc @system
    {
        enum kind = opKind!(T, R, operation);
        enum producesResults = kind == OpKind.result || kind == OpKind.resultContinuation;
        authorized(context);
        if (context.retiring_) return PrivateStartResult.retiring;
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0) return PrivateStartResult.waveActive;
        if (count_ == 0) return PrivateStartResult.empty;
        retire_ = retire;
        isSubset_ = false;
        base_.wave.emittable.emit = &emit;
        cursor_ = 0;
        resetOutcome(count_);
        static if (hasResults)
        {
            resultCount_ = producesResults ? count_ : 0;
            version (assert) memset(resultWritten, 0, resultCount_);
        }
        header_ = PayloadHeader.init;
        header_.maxCs = header_.done = 1;
        header_.plen = 1;
        header_.call = &runChild!operation;
        publishWave(context);
        return PrivateStartResult.started;
    }
    /// Starts only the listed spawn-order indices. No subset retirement.
    PrivateStartResult startSubset(alias operation)(scope ref PrivateContext context,
        scope const(uint)[] members) nothrow @nogc @system
    {
        enum kind = opKind!(T, R, operation);
        enum producesResults = kind == OpKind.result || kind == OpKind.resultContinuation;
        authorized(context);
        if (sel_ is null) fatal("private child subset: storage not reserved");
        if (context.retiring_) return PrivateStartResult.retiring;
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0) return PrivateStartResult.waveActive;
        if (members.length == 0) return PrivateStartResult.empty;
        if (members.length > count_) fatal("private child subset: too many members");
        foreach (j, i; members)
        {
            if (i >= count_) fatal("private child subset: member out of range");
            auto p = sel_[capacity_ + i];
            if (p < j && sel_[p] == i) fatal("private child subset: duplicate member");
            sel_[j] = i;
            sel_[capacity_ + i] = cast(uint) j;
        }
        k_ = members.length;
        cursor_ = 0;
        retire_ = false;
        isSubset_ = true;
        resetOutcome(k_);
        static if (hasResults)
        {
            resultCount_ = producesResults ? k_ : 0;
            version (assert) memset(resultWritten, 0, resultCount_);
        }
        header_ = PayloadHeader.init;
        header_.maxCs = header_.done = 1;
        header_.plen = 1;
        header_.call = &runChild!(operation, true);
        base_.wave.emittable.emit = &emitSubset;
        publishWave(context);
        return PrivateStartResult.started;
    }
    PrivateStartResult startFinal(alias operation)(scope ref PrivateContext context)
        nothrow @nogc @system
    {
        return start!operation(context, true);
    }
private:
    // After active/retiring/empty rejection, before the enqueue release.
    // A rejected start must preserve the last nonempty wave's outcome.
    void resetOutcome(size_t participants) nothrow @nogc @system
    {
        auto wave = &base_.wave;
        atomicStore!(MemoryOrder.raw)(wave.failures, 0u);
        atomicStore!(MemoryOrder.raw)(wave.cancel, false);
        wave.firstFailed = null;
        participants_ = participants;
        dispatched_ = participants;
    }

    // Choose the wave's one immutable credit target and take that credit
    // BEFORE enqueue / visibility: the enclosing wave, or the parent's join
    // counter when the wave joins the parent directly (a root today).
    void publishWave(scope ref PrivateContext context) nothrow @nogc @system
    {
        auto wave = &base_.wave;
        wave.upstream = context.ownJoin_ ? null : context.enclosing_;
        atomicStore!(MemoryOrder.raw)(wave.pending, 1UL); // producer hold
        atomicStore!(MemoryOrder.raw)(wave.active, 1u);
        if (wave.upstream !is null)
            atomicFetchAdd!(MemoryOrder.acq_rel)(wave.upstream.pending, 1UL);
        else if (parentIsRoot_)
        {
            retainJoinCredit(base_.parent.pending);
            base_.parent.hasJoinCredits = true;
        }
        else retainJoinCredit(base_.parent.childPending);
        base_.service.enqueue(&wave.emittable);
    }
    static PrivateChildPark* fromWave(Wave* wave) nothrow @nogc @system
    {
        return cast(PrivateChildPark*) ChildBase.fromWave(wave);
    }
    static bool emit(Emittable* emittable, ref Token token) nothrow @nogc @system
    {
        auto wave = cast(Wave*) emittable;
        auto park = fromWave(wave);
        if (park.truncated(wave)) return true;
        // A truncated retiring wave covers its remaining members with cleanup
        // tables. The header is local: emitResume still publishes header_.
        PayloadHeader cleanupHeader = void;
        const(PayloadHeader)* header = &park.header_;
        if (park.cursor_ >= park.dispatched_)
        {
            cleanupHeader = park.header_;
            cleanupHeader.call = &runSkippedRetire;
            header = &cleanupHeader;
        }
        auto remaining = park.count_ - park.cursor_;
        auto n = remaining > park.batchLimit_ ? park.batchLimit_ : remaining;
        auto bodies = ChildBodies(park.entries_[park.cursor_ .. park.cursor_ + n]);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = cast(size_t) park.base_.service.farm.writeTracked(
            *header, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave); // producer hold still protects it
        park.cursor_ += written;
        if (park.cursor_ < park.count_) return false;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateEmitBeforeSeal, null);
        releaseWave(wave); // seal: last access, including park and its owner
        return true;
    }
    static bool emitSubset(Emittable* emittable, ref Token token) nothrow @nogc @system
    {
        auto wave = cast(Wave*) emittable;
        auto park = fromWave(wave);
        if (park.truncated(wave)) return true; // subsets never retire
        auto remaining = park.k_ - park.cursor_;
        auto n = remaining > park.batchLimit_ ? park.batchLimit_ : remaining;
        auto bodies = SubsetBodies(park.entries_, park.sel_[park.cursor_ .. park.cursor_ + n]);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = cast(size_t) park.base_.service.farm.writeTracked(
            park.header_, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave);
        park.cursor_ += written;
        if (park.cursor_ < park.k_) return false;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateEmitBeforeSeal, null);
        releaseWave(wave); // seal: last park access
        return true;
    }
    // Before each initial table: once the wave's scope is cancelled, the
    // published prefix becomes final. dispatched_ is stored while the producer
    // hold still pins the park; a non-retiring wave then seals at once, and
    // that release is its last park access. cursor_ < dispatched_ holds
    // until truncation and never again afterwards, so this runs once per table.
    bool truncated(Wave* wave) nothrow @nogc @system
    {
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateEmitBeforeCancelCheck, null);
        if (cursor_ >= dispatched_ || !scopeCancelled(wave)) return false;
        dispatched_ = cursor_; // BEFORE the seal
        if (retire_) return false;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateTruncateBeforeSeal, null);
        releaseWave(wave); // seal: last park access
        return true;
    }
    static bool emitResume(Emittable* emittable, ref Token token) nothrow @nogc @system
    {
        auto base = cast(ChildBase*) (cast(ubyte*) emittable - ChildBase.resume.offsetof);
        auto park = cast(PrivateChildPark*) base;
        auto wave = &base.wave;
        auto n = base.resumeCount > park.batchLimit_ ? park.batchLimit_ : base.resumeCount;
        auto bodies = ResumeBodies(base, base.resumeHead, n);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = park.base_.service.farm.writeTracked(
            park.header_, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave); // P still pins the park
        if (base.resumeCount != 0) return false;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateResumeBeforeUnschedule, null);
        base.resumeScheduled = false;
        releaseWave(wave); // drop P: final park access
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateResumeAfterUnschedule, null);
        return true;
    }
    static void finish(Wave* wave) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto upstream = wave.upstream;
        auto parent = park.base_.parent;
        immutable parentIsRoot = park.parentIsRoot_; // snapshots before final cohort access
        static if (hasResults)
            version (assert) park.checkResultsWritten();
        if (park.retire_) park.count_ = 0;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateWaveBeforeIdlePublish, null);
        atomicStore!(MemoryOrder.rel)(wave.active, 0u); // last park access
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateRootAfterIdleBeforeCredit, null);
        if (upstream !is null) releaseWave(upstream);
        else if (parentIsRoot) releaseRoot(parent);
        else
        {
            releaseMember(parent); // final parent access; idle alone does not pin Entry
            version (AntfarmActorTestHooks)
                actorTestPoint(ActorTestPoint.privateChildCustodyAfterRelease, null);
        }
    }
    static long runChild(alias operation, bool subset = false)(PayloadHeader*, PayloadBody body, ulong)
        nothrow @nogc @system
    {
        enum kind = opKind!(T, R, operation);
        auto entry = cast(Entry*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
        auto wave = entry.enclosing;
        auto park = fromWave(wave);
        immutable retiring = park.retire_;
        static if (kind == OpKind.continuation || kind == OpKind.resultContinuation)
        {
            immutable flags = entry.childFlags;
            if (flags & childCleanup)
            {
                retireMember!T(park.base_.service, entry);
                entry.childFlags = 0;
                releaseWave(wave); // H; the resume table credit fences callback return
                return 1;
            }
            atomicStore!(MemoryOrder.raw)(entry.childPending, 1u);
            auto borrow = PrivateBorrow!T(cast(T*) entry.state);
            auto context = PrivateContext(entry, wave, false, park.batchLimit_);
            context.ownJoin_ = true;
            auto step = operation(borrow, context);
            static if (kind == OpKind.resultContinuation)
                auto disposition = step.disposition_;
            else
                auto disposition = step;
            ChildStep next;
            if (disposition == PrivateChildDisposition.done)
            {
                static if (kind == OpKind.resultContinuation)
                {
                    static if (subset)
                        park.writeResult(park.sel_[park.capacity_ + (entry - park.entries_)], step.value_);
                    else
                        park.writeResult(entry - park.entries_, step.value_);
                }
                if (atomicLoad!(MemoryOrder.acq)(entry.childPending) == 1)
                {
                    if (retiring) retireMember!T(park.base_.service, entry);
                    if (flags & childHolding)
                    {
                        entry.childFlags = 0;
                        releaseWave(wave);
                    }
                    return 1;
                }
                next = retiring ? ChildStep.doneRetire : ChildStep.done;
            }
            else next = ChildStep.waitChildren;
            entry.childStep = next;
            if (!(flags & childHolding))
            {
                atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
                entry.childFlags = childHolding;
            }
            releaseMember(entry); // custody release: final Entry access unless owner
            version (AntfarmActorTestHooks)
                actorTestPoint(ActorTestPoint.privateChildCustodyAfterRelease, null);
        }
        else
        {
            auto borrow = PrivateBorrow!T(cast(T*) entry.state);
            auto context = PrivateContext(entry, wave, retiring, park.batchLimit_);
            static if (kind == OpKind.plain)
                operation(borrow, context);
            else
            {
                static if (subset)
                    park.writeResult(park.sel_[park.capacity_ + (entry - park.entries_)], operation(borrow, context));
                else
                    park.writeResult(entry - park.entries_, operation(borrow, context));
            }
            if (retiring) retireMember!T(park.base_.service, entry);
        }
        return 1;
    }
    // Cleanup header for undispatched members of a truncated retiring wave:
    // retire without calling the op or writing a result. The previous wave's
    // join included all descendants, so the member's children are idle.
    static long runSkippedRetire(PayloadHeader*, PayloadBody body, ulong) nothrow @nogc @system
    {
        auto entry = cast(Entry*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
        retireMember!T(fromWave(entry.enclosing).base_.service, entry);
        return 1;
    }
    static void dispose(ChildBase* base) nothrow @nogc @system
    {
        auto park = cast(PrivateChildPark*) base;
        auto service = base.service;
        foreach (ref entry; park.entries_[0 .. park.count_])
        {
            static if (__traits(isPOD, T))
            {
                disposeChildren(&entry);
                freeMemory(service, entry.state, T.sizeof, T.alignof);
            }
            else if (entry.state !is null)
                retireMember!T(service, &entry);
        }
        freeMemory(service, park.entries_, park.capacity_ * Entry.sizeof, Entry.alignof);
        static if (hasResults)
            freeMemory(service, park.results_, park.capacity_ * resultBytesPerMember, R.alignof);
        if (park.sel_ !is null)
            freeMemory(service, park.sel_, park.capacity_ * 2 * uint.sizeof, uint.alignof);
        freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
    }
}
