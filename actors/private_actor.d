/++
 + Experimental private actors. Roots have scheduling custody in a single-
 + emitter park; children have one private parent and run only in its waves.
 + The emitter reads metadata, never actor state. See PRIVATE_ACTORS.md for
 + the @system borrowing contract and the deliberately limited first slice.
 +/
module actors.private_actor;

import actors.actor : ActorAllocator;
import antfarm : AntFarm, Token, PayloadHeader, PayloadBody,
    TableCompletionHook, fatal;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, atomicExchange,
    atomicFetchAdd, atomicFetchSub, cas;
import core.stdc.string : memcpy, memset;

enum PrivateDisposition : ubyte { again, finish }
enum PrivateRootReclamation : ubyte { emitter, consumer }

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
    this(Entry* actor, Wave* enclosing, bool retiring) nothrow @nogc @system
    { actor_ = actor; enclosing_ = enclosing; retiring_ = retiring; }
    @disable this(this);
public:
    PrivateChildPark!T* createChildren(T)(size_t capacity)
        nothrow @nogc @system
    {
        if (actor_ is null || retiring_) return null;
        return PrivateChildPark!T.create(actor_, capacity);
    }
}

private struct Entry
{
    void* state;
    ChildBase* children;
    Service* service;
    Entry* next;
    // Mutually exclusive metadata for root versus child entries.
    union { Wave* enclosing; bool rootTerminal; }
}

private struct ChildBase
{
    ChildBase* next;
    Entry* parent;
    Wave wave;
    void function(ChildBase*) nothrow @nogc @system dispose;
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

private void releaseWave(Wave* wave) nothrow @nogc @system
{
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
    shared(Wave*) incoming;
    Wave* waiting;

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
    Entry*[256] entries;
}

private void rootsCompleted(void* context) nothrow @nogc @system
{
    auto batch = cast(Batch*) context;
    auto returns = batch.returns;
    auto head = atomicLoad!(MemoryOrder.raw)(*returns);
    while (true)
    {
        batch.next = cast(Batch*) head;
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

/// Single-emitter scheduling custody for self-owned roots. spawn transfers a
/// POD initializer; it returns no actor handle. All methods on the park must
/// be called by one emitter at a time (including live and destroy), without
/// re-entry from callbacks or allocator hooks. Returning finish while a child
/// wave is active is a fatal contract error; return again until it joins.
struct PrivateRootPark(T, alias step,
    PrivateRootReclamation reclamation = PrivateRootReclamation.emitter)
{
    static assert(__traits(isPOD, T), "private actors require POD state");
    static assert(is(typeof(&step) : PrivateDisposition function(
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
    size_t batchCount_;
    Batch* spare_;
    shared(Batch*) returned_;
    @disable this(this);
public:
    static PrivateRootPark* create(AntFarm* farm, size_t capacity,
        ActorAllocator allocator = ActorAllocator.cRuntime()) nothrow @nogc @system
    {
        if (farm is null || !allocator.valid || capacity == 0
            || capacity > size_t.max / Entry.sizeof) return null;
        auto park = cast(PrivateRootPark*) allocator.allocate(allocator.context,
            PrivateRootPark.sizeof, PrivateRootPark.alignof);
        if (park is null) return null;
        memset(park, 0, PrivateRootPark.sizeof);
        park.service_ = Service(farm, allocator);
        park.capacity_ = capacity;
        park.batchCount_ = (capacity + 255) / 256;
        park.entries_ = cast(Entry*) allocateZero(&park.service_, capacity * Entry.sizeof, Entry.alignof);
        park.batches_ = cast(Batch*) allocateZero(&park.service_, park.batchCount_ * Batch.sizeof, Batch.alignof);
        if (park.entries_ is null || park.batches_ is null)
        {
            freeMemory(&park.service_, park.entries_, capacity * Entry.sizeof, Entry.alignof);
            freeMemory(&park.service_, park.batches_, park.batchCount_ * Batch.sizeof, Batch.alignof);
            allocator.deallocate(allocator.context, park, PrivateRootPark.sizeof, PrivateRootPark.alignof);
            return null;
        }
        foreach (i; 0 .. capacity)
        {
            park.entries_[i].service = &park.service_;
            park.entries_[i].next = park.free_;
            park.free_ = &park.entries_[i];
        }
        foreach (i; 0 .. park.batchCount_)
        {
            auto batch = &park.batches_[i];
            batch.hook = TableCompletionHook(batch, &rootsCompleted);
            batch.returns = &park.returned_;
            batch.next = park.spare_;
            park.spare_ = batch;
        }
        return park;
    }

    bool spawn()(scope auto ref const(T) initial) nothrow @nogc @system
    {
        if (free_ is null) return false;
        auto state = service_.allocator.allocate(service_.allocator.context, T.sizeof, T.alignof);
        if (state is null) return false;
        memcpy(state, &initial, T.sizeof);
        auto entry = free_;
        free_ = entry.next;
        entry.state = state;
        entry.rootTerminal = false;
        appendReady(entry);
        ++live_;
        return true;
    }

    /// Acquire completed physical tables, recycle terminal roots, and return
    /// continuing roots to the emitter's unpublished list.
    void poll() nothrow @nogc @system
    {
        auto batch = cast(Batch*) atomicExchange!(MemoryOrder.acq_rel)(&returned_, cast(shared(Batch)*) null);
        while (batch !is null)
        {
            auto next = batch.next;
            foreach (entry; batch.entries[0 .. batch.accepted])
            {
                if (entry.rootTerminal)
                {
                    static if (reclamation == PrivateRootReclamation.emitter)
                        freeMemory(&service_, entry.state, T.sizeof, T.alignof);
                    entry.state = null;
                    entry.next = free_;
                    free_ = entry;
                    --live_;
                }
                else
                {
                    appendReady(entry);
                }
            }
            batch.next = spare_;
            spare_ = batch;
            batch = next;
        }
    }

    /// Emits at most one root table and one table per pending child wave.
    /// By default terminal root state is freed here on the emitter; the
    /// consumer policy frees it in the callback. Child frees run on consumers.
    size_t pump(ref Token token, size_t batchLimit = 256, uint avgCost = 0)
        nothrow @nogc @system
    {
        if (batchLimit == 0 || batchLimit > 256) fatal("invalid private root batch limit");
        poll();
        service_.pump(token);
        if (spare_ is null || ready_ is null) return 0;
        auto batch = spare_;
        spare_ = batch.next;
        size_t count;
        while (count < batchLimit && ready_ !is null)
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

    @property size_t live() const nothrow @nogc { return live_; }

    void destroy() nothrow @nogc @system
    {
        poll();
        if (live_ != 0 || service_.waiting !is null
            || atomicLoad!(MemoryOrder.acq)(service_.incoming) !is null)
            fatal("destroy nonempty private root park");
        auto allocator = service_.allocator;
        freeMemory(&service_, entries_, capacity_ * Entry.sizeof, Entry.alignof);
        freeMemory(&service_, batches_, batchCount_ * Batch.sizeof, Batch.alignof);
        allocator.deallocate(allocator.context, &this, PrivateRootPark.sizeof, PrivateRootPark.alignof);
    }
private:
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
        auto context = PrivateContext(entry, null, false);
        if (step(borrow, context) == PrivateDisposition.finish)
        {
            disposeChildren(entry);
            static if (reclamation == PrivateRootReclamation.consumer)
                freeMemory(entry.service, entry.state, T.sizeof, T.alignof);
            entry.rootTerminal = true;
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

/// Opaque homogeneous cohort. The parent may create members and request a
/// whole-cohort wave, but can never borrow a child's state. No overlapping
/// waves, external dispatches, removal, or reparenting in this first slice.
struct PrivateChildPark(T)
{
    static assert(__traits(isPOD, T), "private actors require POD state");
private:
    ChildBase base_; // first: erased disposal and wave-to-park conversion
    Entry* entries_;
    size_t capacity_, count_, cursor_;
    bool retire_;
    PayloadHeader header_;
    static assert(base_.offsetof == 0,
        "PrivateChildPark.fromWave requires base_ at offset zero");
    @disable this(this);

    static PrivateChildPark* create(Entry* parent, size_t capacity)
        nothrow @nogc @system
    {
        if (capacity == 0 || capacity > size_t.max / Entry.sizeof) return null;
        auto service = parent.service;
        auto park = cast(PrivateChildPark*) allocateZero(service, PrivateChildPark.sizeof, PrivateChildPark.alignof);
        if (park is null) return null;
        park.entries_ = cast(Entry*) allocateZero(service, capacity * Entry.sizeof, Entry.alignof);
        if (park.entries_ is null)
        {
            freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
            return null;
        }
        park.capacity_ = capacity;
        park.base_.parent = parent;
        park.base_.dispose = &dispose;
        park.base_.wave.hook = TableCompletionHook(&park.base_.wave, &waveCompleted);
        park.base_.wave.emit = &emit;
        park.base_.wave.finish = &finish;
        park.base_.next = parent.children;
        parent.children = &park.base_;
        return park;
    }

    bool authorized(scope ref PrivateContext context) nothrow @nogc @system
    {
        return context.actor_ !is null && context.actor_ is base_.parent;
    }
public:
    bool finished(scope ref PrivateContext context) nothrow @nogc @system
    {
        return authorized(context) && atomicLoad!(MemoryOrder.acq)(base_.wave.active) == 0;
    }

    bool spawn()(scope ref PrivateContext context, scope auto ref const(T) initial)
        nothrow @nogc @system
    {
        if (context.retiring_ || !finished(context) || count_ == capacity_) return false;
        auto service = base_.parent.service;
        auto state = service.allocator.allocate(service.allocator.context, T.sizeof, T.alignof);
        if (state is null) return false;
        memcpy(state, &initial, T.sizeof);
        auto entry = &entries_[count_++];
        entry.state = state;
        entry.service = service;
        entry.enclosing = &base_.wave;
        return true;
    }

    /// Enqueue a wave under the parent's dispatch authority. Completion also
    /// joins descendant waves started by this operation. retire=true frees
    /// members on their final dispatch and forbids launching descendants.
    bool start(alias operation)(scope ref PrivateContext context, bool retire = false)
        nothrow @nogc @system
    {
        static assert(is(typeof(&operation) : void function(
            scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system));
        if (context.retiring_ || !finished(context)) return false;
        if (count_ == 0) return true;
        retire_ = retire;
        cursor_ = 0;
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
        base_.parent.service.enqueue(wave);
        return true;
    }
private:
    static PrivateChildPark* fromWave(Wave* wave) nothrow @nogc @system
    {
        return cast(PrivateChildPark*) (cast(ubyte*) wave - ChildBase.wave.offsetof);
    }
    static bool emit(Wave* wave, ref Token token) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto remaining = park.count_ - park.cursor_;
        auto n = remaining > 256 ? 256 : remaining;
        auto bodies = ChildBodies(park.entries_[park.cursor_ .. park.cursor_ + n]);
        atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);
        immutable written = cast(size_t) park.base_.parent.service.farm.writeTracked(
            park.header_, bodies, 1, &wave.hook, token, 0);
        if (written == 0) releaseWave(wave); // producer hold still protects it
        park.cursor_ += written;
        if (park.cursor_ < park.count_) return false;
        releaseWave(wave); // seal: last access, including park and its owner
        return true;
    }
    static void finish(Wave* wave) nothrow @nogc @system
    {
        auto park = fromWave(wave);
        auto upstream = wave.upstream;
        if (park.retire_) park.count_ = 0;
        atomicStore!(MemoryOrder.rel)(wave.active, 0u); // last park access
        if (upstream !is null) releaseWave(upstream);
    }
    static long runChild(alias operation)(PayloadHeader*, PayloadBody body, ulong)
        nothrow @nogc @system
    {
        auto entry = cast(Entry*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
        auto park = fromWave(entry.enclosing);
        immutable retiring = park.retire_;
        auto borrow = PrivateBorrow!T(cast(T*) entry.state);
        auto context = PrivateContext(entry, entry.enclosing, retiring);
        operation(borrow, context);
        if (retiring)
        {
            disposeChildren(entry);
            freeMemory(entry.service, entry.state, T.sizeof, T.alignof);
            entry.state = null;
        }
        return 1;
    }
    static void dispose(ChildBase* base) nothrow @nogc @system
    {
        auto park = cast(PrivateChildPark*) base;
        auto service = base.parent.service;
        foreach (ref entry; park.entries_[0 .. park.count_])
        {
            disposeChildren(&entry);
            freeMemory(service, entry.state, T.sizeof, T.alignof);
        }
        freeMemory(service, park.entries_, park.capacity_ * Entry.sizeof, Entry.alignof);
        freeMemory(service, park, PrivateChildPark.sizeof, PrivateChildPark.alignof);
    }
}
