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
import std.traits : hasElaborateDestructor, hasNested;

enum defaultPrivateBatchLimit = 256;

struct PrivateCohortOptions
{
    size_t batchLimit; // 0 inherits the creating context's effective limit
    bool subsets;     // reserve selection and inverse-position storage
}

enum PrivateRootDisposition : ubyte { again, finish, waitChildren, finishAfterChildren }
enum PrivateSpawnResult : ubyte { spawned, capacityFull, allocationFailed, waveActive, retiring }
enum PrivateStartResult : ubyte { started, empty, waveActive, retiring }
enum PrivateRootReclamation : ubyte { emitter, consumer }

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
    size_t batchLimit_;
    this(Entry* actor, Wave* enclosing, bool retiring, size_t batchLimit) nothrow @nogc @system
    { actor_ = actor; enclosing_ = enclosing; retiring_ = retiring; batchLimit_ = batchLimit; }
    @disable this(this);
    // A root context has no enclosing wave and reads its Entry; a member's
    // service lives in the ChildBase of the wave that dispatched it.
    Service* service() nothrow @nogc @system
    {
        return enclosing_ is null ? rootService(actor_) : ChildBase.fromWave(enclosing_).service;
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
}

// Child entries reach the Service through ChildBase.service; their word at
// offset 16 is child-only metadata, never a Service pointer.
private enum ChildStep : ubyte { done, waitChildren, doneRetire }

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
        && Entry.pending.offsetof == 36);
}

// Only roots have a Service word. Every read of Entry.service goes through
// here, so a child Entry's metadata can never be misread as a pointer. A child
// word is zero until continuations use it; they must then extend this check.
private Service* rootService(Entry* entry) nothrow @nogc @system
{
    version (assert)
        if (entry.service is null) fatal("private entry: service read on a child");
    return entry.service;
}

private struct ChildBase
{
    ChildBase* next;
    Entry* parent;
    Service* service;
    Wave wave;
    void function(ChildBase*) nothrow @nogc @system dispose;

    static ChildBase* fromWave(Wave* wave) nothrow @nogc @system
    {
        return cast(ChildBase*) (cast(ubyte*) wave - ChildBase.wave.offsetof);
    }
}

private struct Wave
{
    shared ulong pending;
    shared uint active;
    Wave* upstream;
    Wave* next;
    TableCompletionHook hook;
    // Returns true only when publication is sealed; can destroy w on return.
    bool function(Wave*, ref Token) nothrow @nogc @system emit;
    void function(Wave*) nothrow @nogc @system finish;
}

// Increment under an existing hold; reject overflow before changing the counter.
private void retainRootCredit(ref shared uint pending) nothrow @nogc @system
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

    /// Test-only: the Entry word at offset 16 of the context's own Entry,
    /// unchecked. Zero for a child; the Service pointer for a root.
    ulong privateTestServiceWord(scope ref PrivateContext context) nothrow @nogc @system
    {
        return *cast(ulong*) &context.actor_.service;
    }

    /// Test-only: the context's own Entry through the checked accessor. A
    /// child Entry is fatal in debug builds.
    void* privateTestRootService(scope ref PrivateContext context) nothrow @nogc @system
    {
        return rootService(context.actor_);
    }

    /// Test-only: the Service that createChildren would hand a new cohort.
    void* privateTestContextService(scope ref PrivateContext context) nothrow @nogc @system
    {
        return context.service;
    }
}

private void releaseRoot(Entry* entry) nothrow @nogc @system
{
    version (AntfarmActorTestHooks)
        actorTestPoint(ActorTestPoint.privateRootCreditBeforeRelease, null);
    immutable old = atomicFetchSub!(MemoryOrder.acq_rel)(entry.pending, 1u);
    if (old == 0) fatal("private root credit underflow");
    if (old != 1) return; // no entry access after relinquishing a nonfinal credit
    auto returns = &rootService(entry).rootReturns;
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
    shared(Wave*) incoming;
    Wave* waiting;
    shared(Entry*) rootReturns;

    void enqueue(Wave* wave) nothrow @nogc @system
    {
        auto head = atomicLoad!(MemoryOrder.raw)(incoming);
        while (true)
        {
            wave.next = cast(Wave*) head;
            if (cas!(MemoryOrder.rel, MemoryOrder.raw)(
                    &incoming, head, cast(shared(Wave)*) wave)) return;
            head = atomicLoad!(MemoryOrder.raw)(incoming);
        }
    }

    void pump(ref Token token) nothrow @nogc @system
    {
        auto fresh = cast(Wave*) atomicExchange!(MemoryOrder.acq_rel)(&incoming, cast(shared(Wave)*) null);
        while (fresh !is null)
        {
            auto next = fresh.next;
            fresh.next = waiting;
            waiting = fresh;
            fresh = next;
        }
        // One pass, one table per wave: backpressure never blocks a worker.
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

// Shared retirement hook for terminal dispatches and future child cleanup activations.
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
        auto service = rootService(entry);
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
    }
private:
    ChildBase base_; // first: erased disposal and wave-to-park conversion
    Entry* entries_;
    size_t capacity_, count_, cursor_, batchLimit_;
    bool retire_, isSubset_;
    bool parentIsRoot_; // fixed at creation: the creating context had no enclosing wave
    uint* sel_;
    size_t k_;
    PayloadHeader header_;
    static if (hasResults)
    {
        R* results_;         // capacity_ slots, in participant order
        size_t resultCount_; // members in the last nonempty wave; parent-only
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
            if (capacity > size_t.max / R.sizeof) return null;
        auto park = cast(PrivateChildPark*) allocateZero(service, PrivateChildPark.sizeof, PrivateChildPark.alignof);
        if (park is null) return null;
        park.entries_ = cast(Entry*) allocateZero(service, capacity * Entry.sizeof, Entry.alignof);
        static if (hasResults)
            if (park.entries_ !is null)
            {
                park.results_ = cast(R*) allocateZero(service, capacity * R.sizeof, R.alignof);
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
                    freeMemory(service, park.results_, capacity * R.sizeof, R.alignof);
                freeMemory(service, park.entries_, capacity * Entry.sizeof, Entry.alignof);
                freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
                return null;
            }
        }
        park.capacity_ = capacity;
        park.batchLimit_ = batchLimit;
        park.base_.parent = parent;
        park.base_.service = service;
        park.parentIsRoot_ = parentIsRoot;
        park.base_.dispose = &dispose;
        park.base_.wave.hook = TableCompletionHook(&park.base_.wave, &waveCompleted);
        park.base_.wave.emit = &emit;
        park.base_.wave.finish = &finish;
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

    static if (hasResults)
    {
        /// Results of the last nonempty wave, in participant order;
        /// empty before the first such wave or after a void-operation wave.
        /// Acquires the join. Calling while a wave is active is fatal. Valid
        /// only within this callback and until the next nonempty start/startFinal/startSubset.
        const(R)[] results(scope ref PrivateContext context) nothrow @nogc @system
        {
            authorized(context);
            if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0)
                fatal("private child results: wave active");
            return results_[0 .. resultCount_];
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
    /// members on their final dispatch and forbids launching descendants.
    PrivateStartResult start(alias operation)(scope ref PrivateContext context, bool retire = false)
        nothrow @nogc @system
    {
        enum producesResults = !is(typeof(&operation) : void function(
            scope ref PrivateBorrow!T, scope ref PrivateContext) @system);
        static if (producesResults)
        {
            static assert(hasResults && is(typeof(&operation) : R function(
                scope ref PrivateBorrow!T, scope ref PrivateContext) @system),
                "operation must return void or the cohort's result type");
            static assert(is(typeof(&operation) : R function(
                scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system),
                "operation must be nothrow @nogc");
        }
        else
            static assert(is(typeof(&operation) : void function(
                scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system),
                "operation must be nothrow @nogc");
        authorized(context);
        if (context.retiring_) return PrivateStartResult.retiring;
        if (atomicLoad!(MemoryOrder.acq)(base_.wave.active) != 0) return PrivateStartResult.waveActive;
        if (count_ == 0) return PrivateStartResult.empty;
        retire_ = retire;
        isSubset_ = false;
        base_.wave.emit = &emit;
        cursor_ = 0;
        static if (hasResults)
            resultCount_ = producesResults ? count_ : 0;
        header_ = PayloadHeader.init;
        header_.maxCs = header_.done = 1;
        header_.plen = 1;
        header_.call = &runChild!operation;
        auto wave = &base_.wave;
        wave.upstream = context.enclosing_;
        atomicStore!(MemoryOrder.raw)(wave.pending, 1UL); // producer hold
        atomicStore!(MemoryOrder.raw)(wave.active, 1u);
        if (wave.upstream !is null)
            atomicFetchAdd!(MemoryOrder.acq_rel)(wave.upstream.pending, 1UL);
        else
        {
            retainRootCredit(base_.parent.pending); // BEFORE enqueue / visibility
            base_.parent.hasJoinCredits = true;
        }
        base_.service.enqueue(wave);
        return PrivateStartResult.started;
    }
    /// Starts only the listed spawn-order indices. No subset retirement.
    PrivateStartResult startSubset(alias operation)(scope ref PrivateContext context,
        scope const(uint)[] members) nothrow @nogc @system
    {
        enum producesResults = !is(typeof(&operation) : void function(
            scope ref PrivateBorrow!T, scope ref PrivateContext) @system);
        static if (producesResults)
            static assert(hasResults && is(typeof(&operation) : R function(
                scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system),
                "operation must return void or the cohort's result type and be nothrow @nogc");
        else
            static assert(is(typeof(&operation) : void function(
                scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system),
                "operation must be nothrow @nogc");
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
        static if (hasResults)
            resultCount_ = producesResults ? k_ : 0;
        header_ = PayloadHeader.init;
        header_.maxCs = header_.done = 1;
        header_.plen = 1;
        header_.call = &runChild!(operation, true);
        auto wave = &base_.wave;
        wave.emit = &emitSubset;
        wave.upstream = context.enclosing_;
        atomicStore!(MemoryOrder.raw)(wave.pending, 1UL);
        atomicStore!(MemoryOrder.raw)(wave.active, 1u);
        if (wave.upstream !is null)
            atomicFetchAdd!(MemoryOrder.acq_rel)(wave.upstream.pending, 1UL);
        else
        {
            retainRootCredit(base_.parent.pending);
            base_.parent.hasJoinCredits = true;
        }
        base_.service.enqueue(wave);
        return PrivateStartResult.started;
    }
    PrivateStartResult startFinal(alias operation)(scope ref PrivateContext context)
        nothrow @nogc @system
    {
        return start!operation(context, true);
    }
private:
    static PrivateChildPark* fromWave(Wave* wave) nothrow @nogc @system
    {
        return cast(PrivateChildPark*) ChildBase.fromWave(wave);
    }
    static bool emit(Wave* wave, ref Token token) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto remaining = park.count_ - park.cursor_;
        auto n = remaining > park.batchLimit_ ? park.batchLimit_ : remaining;
        auto bodies = ChildBodies(park.entries_[park.cursor_ .. park.cursor_ + n]);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = cast(size_t) park.base_.service.farm.writeTracked(
            park.header_, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave); // producer hold still protects it
        park.cursor_ += written;
        if (park.cursor_ < park.count_) return false;
        releaseWave(wave); // seal: last access, including park and its owner
        return true;
    }
    static bool emitSubset(Wave* wave, ref Token token) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto remaining = park.k_ - park.cursor_;
        auto n = remaining > park.batchLimit_ ? park.batchLimit_ : remaining;
        auto bodies = SubsetBodies(park.entries_, park.sel_[park.cursor_ .. park.cursor_ + n]);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = cast(size_t) park.base_.service.farm.writeTracked(
            park.header_, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave);
        park.cursor_ += written;
        if (park.cursor_ < park.k_) return false;
        releaseWave(wave); // seal: last park access
        return true;
    }
    static void finish(Wave* wave) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto upstream = wave.upstream;
        auto parent = park.base_.parent; // snapshot before final cohort access
        if (park.retire_) park.count_ = 0;
        version (AntfarmActorTestHooks)
            actorTestPoint(ActorTestPoint.privateWaveBeforeIdlePublish, null);
        atomicStore!(MemoryOrder.rel)(wave.active, 0u); // last park access
        if (upstream !is null) releaseWave(upstream);
        else
        {
            version (AntfarmActorTestHooks)
                actorTestPoint(ActorTestPoint.privateRootAfterIdleBeforeCredit, null);
            releaseRoot(parent); // final parent access; idle alone does not pin Entry
        }
    }
    static long runChild(alias operation, bool subset = false)(PayloadHeader*, PayloadBody body, ulong)
        nothrow @nogc @system
    {
        auto entry = cast(Entry*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
        auto park = fromWave(entry.enclosing);
        immutable retiring = park.retire_;
        auto borrow = PrivateBorrow!T(cast(T*) entry.state);
        auto context = PrivateContext(entry, entry.enclosing, retiring, park.batchLimit_);
        static if (is(typeof(operation(borrow, context)) == void))
            operation(borrow, context);
        else
        {
            static if (subset)
                park.results_[park.sel_[park.capacity_ + (entry - park.entries_)]] = operation(borrow, context);
            else
                park.results_[entry - park.entries_] = operation(borrow, context);
        }
        if (retiring)
        {
            retireMember!T(park.base_.service, entry);
        }
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
            freeMemory(service, park.results_, park.capacity_ * R.sizeof, R.alignof);
        if (park.sel_ !is null)
            freeMemory(service, park.sel_, park.capacity_ * 2 * uint.sizeof, uint.alignof);
        freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
    }
}
