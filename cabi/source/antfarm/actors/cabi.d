// Runtime-free public actor exports. D actor ordering and ownership remain authoritative.
module antfarm.actors.cabi;
import antfarm : fatal, inFarmCallback, MAX_AVG_COST;
import antfarm.actors.actor;
import antfarm.actors.wave;
import antfarm : TableCompletionHook;
import antfarm.c.core;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, atomicOp, cas;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;

extern(C) alias CActorHandler = void function(void*, void*, size_t, af_actor_context*) nothrow @nogc @system;
extern(C) alias CActorAllocate = void* function(void*, size_t, size_t) nothrow @nogc @system;
extern(C) alias CActorDeallocate = void function(void*, void*, size_t, size_t) nothrow @nogc @system;
struct af_actor_allocator { void* context; CActorAllocate allocate; CActorDeallocate deallocate; }
struct af_actor_handle { void* _slot; ulong _generation; }
struct af_actor_runtime
{
    ActorRuntime* impl;
    af_farm* farm;
    bool owned, custom;
    af_actor_allocator allocator;
    shared size_t kindHeadWord;
    af_actor_runtime* next; // Control-thread host list.
}

extern(C) alias CActorInitializer = uint function(void*, void*, size_t, size_t) nothrow @nogc @system;
extern(C) alias CActorDestructor = void function(void*, void*, size_t) nothrow @nogc @system;
struct af_actor_kind_options
{
    CActorHandler handler;
    void* user;
    size_t state_bytes, state_alignment;
    CActorDestructor destroy;
}
struct af_actor_kind
{
    ActorKind value;
    af_actor_runtime* runtime;
    af_actor_kind_options options;
    af_actor_kind* next;
}
struct af_actor_owner { void* _slot; ulong _generation; }
struct af_actor_handle_span { const(af_actor_handle)* data; size_t stride, count; }
struct af_actor_owner_span { const(af_actor_owner)* data; size_t stride, count; }
struct af_actor_owner_output_span { af_actor_owner* data; size_t stride, count; }
struct af_actor_state_span { const(void)* data; size_t stride, count; }
struct af_actor_result_span { uint* data; size_t stride, count; }
struct af_actor_batch_result { size_t count; uint status; }
enum uint AF_ACTOR_INITIALIZER_FAILED = 8;
enum uint retireBase = 16, reclaimBase = 32;
static assert(af_actor_owner.sizeof == 16 && af_actor_owner.alignof == 8);
static assert(af_actor_kind_options.sizeof == 40 && af_actor_kind_options.destroy.offsetof == 32);
static assert(af_actor_handle_span.sizeof == 24 && af_actor_batch_result.sizeof == 16);

struct af_actor_context { ActorContext* value; }
struct af_actor_message { ActorInboxNode value; }

private af_actor_context* activeContext;
version (AntfarmCActorTestHooks)
{
    private __gshared extern(C) void function() nothrow @nogc afterCompletion;
    extern(C) void af_test_actor_after_completion(typeof(afterCompletion) hook) nothrow @nogc { afterCompletion = hook; }
}
static assert(af_actor_handle.sizeof == 16 && af_actor_handle.alignof == 8);
static assert(af_actor_allocator.sizeof == 24);
static assert(af_actor_message.value.offsetof == 0 && af_actor_message.sizeof == 32
    && af_actor_message.sizeof == ActorInboxNode.sizeof && af_actor_message.alignof == 8);
static assert(cast(uint) ActorWakeResult.queued == 0 && cast(uint) ActorWakeResult.closed == 4);
static assert(cast(uint) ActorSendResult.nodeBusy == 5);
static assert(cast(uint) ActorRetireMode.cancel == 1 && cast(uint) ActorRetireResult.staleOwner == 3);
static assert(cast(uint) ActorReclaimResult.staleOwner == 2 && cast(uint) ActorNodeOutcome.cancelled == 2);

private T* allocate(T)() nothrow @nogc @system
{
    auto p = cast(T*) malloc(T.sizeof);
    if (p !is null)
    {
        static immutable initial = T.init;
        memcpy(p, &initial, T.sizeof);
    }
    return p;
}
private void requireRuntime(const(af_actor_runtime)* r) nothrow @nogc @system
{
    if (r is null || r.impl is null) fatal("C ABI null actor runtime");
}
private void requireMessage(const(af_actor_message)* m) nothrow @nogc @system
{
    if (m is null) fatal("C ABI null actor message");
}
private void requireContext(const(af_actor_context)* c) nothrow @nogc @system
{
    if (c is null || c !is activeContext) fatal("C ABI actor context outside its callback");
}
private ActorErasedHandle handle(const(af_actor_handle)* h) nothrow @nogc @system
{
    return h is null ? ActorErasedHandle.init :
        ActorErasedHandle(cast(ActorSlot*) h._slot, h._generation);
}
private void* customAllocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto r = cast(af_actor_runtime*) context;
    auto p = r.allocator.allocate(r.allocator.context, bytes, alignment);
    if (p !is null && cast(size_t) p % alignment) fatal("C ABI actor allocator returned misaligned storage");
    return p;
}
private void customDeallocate(void* context, void* memory, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto r = cast(af_actor_runtime*) context;
    r.allocator.deallocate(r.allocator.context, memory, bytes, alignment);
}
private void dispatch(scope ref ActorErasedBorrow borrow, scope ref ActorContext ctx) nothrow @nogc @system
{
    auto kind = cast(af_actor_kind*) ctx.rawKind();
    af_actor_context c = af_actor_context(&ctx);
    auto previous = activeContext;
    activeContext = &c;
    kind.options.handler(kind.options.user, borrow.rawState(), borrow.stateSize, &c);
    activeContext = previous;
}

private ActorAllocator runtimeAllocator(af_actor_runtime* r) nothrow @nogc @system
{
    return r.custom ? ActorAllocator(r, &customAllocate, &customDeallocate) : ActorAllocator.cRuntime();
}
private bool emptyOwner(const(af_actor_owner)* o) nothrow @nogc @system
{ return o._slot is null && o._generation == 0; }
private bool validOwner(af_actor_runtime* r, const(af_actor_owner)* o) nothrow @nogc @system
{
    return r !is null && r.impl !is null && o !is null &&
        (emptyOwner(o) || (o._generation != 0 && r.impl.ownsSlot(o._slot)));
}
private bool validSpan(const(void)* data, size_t stride, size_t count,
        size_t bytes, size_t alignment) nothrow @nogc @system
{
    if (count == 0) return true;
    immutable base = cast(size_t) data;
    if (data is null || base % alignment || stride < bytes || stride % alignment ||
            base > size_t.max - bytes) return false;
    return count - 1 <= (size_t.max - base - bytes) / stride;
}
private T* element(T)(T* data, size_t stride, size_t i) nothrow @nogc @system
{ return cast(T*) (cast(size_t) data + stride * i); }
private bool validKind(af_actor_runtime* r, const(af_actor_kind)* kind) nothrow @nogc @system
{
    // Kind pointers are live capabilities, like runtime pointers. Registration
    // is quiescent; immutable metadata can be read concurrently by creators.
    return r !is null && r.impl !is null && kind !is null && kind.runtime is r;
}
private void destroyState(void* identity, void* state, size_t bytes) nothrow @nogc @system
{
    auto k = cast(af_actor_kind*) identity;
    k.options.destroy(k.options.user, state, bytes);
}
private struct Initializer { CActorInitializer call; void* user; }
private bool initializeState(void* context, void* state, size_t bytes, size_t index) nothrow @nogc @system
{
    auto init = cast(Initializer*) context;
    return init.call(init.user, state, bytes, index) == AF_OK;
}
private af_actor_batch_result createActors(af_actor_runtime* r, const(af_actor_kind)* kind,
        af_actor_state_span input, CActorInitializer initialize, void* user,
        size_t firstIndex, af_actor_owner_output_span output) nothrow @nogc @system
{
    immutable count = initialize is null ? input.count : output.count;
    if (!validKind(r, kind) || output.count < count ||
        !validSpan(output.data, output.stride, output.count, af_actor_owner.sizeof, af_actor_owner.alignof) ||
        (count && firstIndex > size_t.max - (count - 1)) ||
        (initialize is null && !validSpan(input.data, input.stride, count,
            kind.options.state_bytes, 1)))
        return af_actor_batch_result(0, setError(AF_INVALID, "invalid actor creation span or kind"));
    foreach (i; 0 .. count)
        if (!emptyOwner(element(output.data, output.stride, i)))
            return af_actor_batch_result(0, setError(AF_INVALID, "actor output must be empty"));
    size_t reserved;
    auto slot = r.impl.reserveSlots(count, reserved);
    auto remaining = reserved;
    // All admission happens before callbacks. Return the unused suffix once.
    scope(exit) r.impl.returnReservations(slot, remaining);
    Initializer init = Initializer(initialize, user);
    foreach (i; 0 .. reserved)
    {
        auto next = slot.freeNext;
        ulong generation;
        auto status = r.impl.constructReserved(slot,
            initialize is null ? cast(const(void)*) element(cast(const(ubyte)*) input.data, input.stride, i) : null,
            initialize is null ? null : &initializeState, &init, firstIndex + i,
            kind.options.state_bytes, kind.options.state_alignment, &dispatch,
            cast(ActorKind*) &kind.value, generation);
        if (status != ActorConstructResult.created)
            return af_actor_batch_result(i, setError(status == ActorConstructResult.allocationFailed
                ? AF_ALLOCATION_FAILED : AF_ACTOR_INITIALIZER_FAILED, "actor construction failed"));
        slot.freeNext = null;
        *element(output.data, output.stride, i) = af_actor_owner(slot, generation);
        slot = next;
        --remaining;
    }
    return af_actor_batch_result(reserved, setError(reserved == count ? AF_OK : AF_FULL,
        reserved == count ? null : "actor capacity full"));
}
// Internal bridge for the optional host. Borrowed wrappers are destroyed only
// after the host joins consumers. Keep the allocator context alive through destroy.
uint createActorRuntime(af_farm* f, size_t capacity, const(af_actor_allocator)* a,
    bool owned, af_actor_runtime** output) nothrow @nogc @system
{
    if (output is null) return setError(AF_INVALID, "null actor runtime output");
    *output = null;
    if (inFarmCallback()) fatal("C ABI actor runtime creation inside Farm callback");
    if (f is null || f.impl is null || (owned && !f.owned) || !capacity ||
        capacity > size_t.max / ActorSlot.sizeof ||
        (a !is null && (a.allocate is null || a.deallocate is null)))
        return setError(AF_INVALID, "invalid actor runtime options or borrowed Farm");
    auto r = allocate!af_actor_runtime();
    if (r is null) return setError(AF_ALLOCATION_FAILED, "actor runtime handle allocation failed");
    r.farm = f;
    r.owned = owned;
    r.custom = a !is null;
    if (a !is null) r.allocator = af_actor_allocator(cast(void*) a.context, a.allocate, a.deallocate);
    auto allocator = r.custom ? ActorAllocator(r, &customAllocate, &customDeallocate) : ActorAllocator.cRuntime();
    r.impl = ActorRuntime.create(f.impl, capacity, allocator);
    if (r.impl is null) { free(r); return setError(AF_ALLOCATION_FAILED, "actor runtime allocation failed"); }
    if (owned) atomicOp!"+="(f.actorRuntimes, 1u);
    *output = r;
    return setError(AF_OK);
}
bool actorRuntimeDrained(af_actor_runtime* r) nothrow @nogc @system
{
    return r.impl.quiescent;
}
void finishActorRuntime(af_actor_runtime* r) nothrow @nogc @system
{
    if (!actorRuntimeDrained(r)) fatal("C ABI destroy actor runtime with live owners or reservations");
    auto allocator = runtimeAllocator(r);
    for (auto k = cast(af_actor_kind*) atomicLoad!(MemoryOrder.acq)(r.kindHeadWord); k !is null;)
    {
        auto next = k.next;
        allocator.deallocate(allocator.context, k, af_actor_kind.sizeof, af_actor_kind.alignof);
        k = next;
    }
    atomicStore!(MemoryOrder.raw)(r.kindHeadWord, size_t.init);
    r.impl.destroy();
    r.impl = null;
}


extern(C) alias CWaveOperation = void function(void*, const(af_actor_kind)*, void*, size_t) nothrow @nogc @system;
struct af_actor_wave
{
    ActorWave value;
    af_farm* farm;
    CWaveOperation operation;
    void* user;
    TableCompletionHook completion;
    shared uint callbackDone;
}
struct af_actor_wave_handle { af_actor_wave* _wave; ulong _generation; }
static assert(af_actor_wave_handle.sizeof == 16 && af_actor_wave_handle.alignof == 8);

private struct CActorHandleRange
{
    af_actor_handle_span values;
    @property size_t length() const pure nothrow @nogc @safe { return values.count; }
    ActorErasedHandle opIndex(size_t i) nothrow @nogc @system
    { return handle(element(values.data, values.stride, i)); }
    CActorHandleRange opSlice(size_t lo, size_t hi) nothrow @nogc @system
    { return CActorHandleRange(af_actor_handle_span(element(values.data, values.stride, lo), values.stride, hi - lo)); }
    size_t opDollar() const pure nothrow @nogc @safe { return values.count; }
}
private ActorWaveHandle waveHandle(const(af_actor_wave_handle)* h) nothrow @nogc @system
{
    return h is null || h._wave is null ? ActorWaveHandle.init :
        ActorWaveHandle(cast(ActorWave*) &h._wave.value, h._generation);
}
private void requireWave(af_actor_wave* w) nothrow @nogc @system
{
    if (w is null) fatal("C ABI null actor wave");
}
private bool waveReady(af_actor_wave* w) nothrow @nogc @system
{
    return w.value.handle.finished && atomicLoad!(MemoryOrder.acq)(w.callbackDone) != 0;
}
private void dispatchWave(scope ref ActorErasedBorrow borrow, void* context) nothrow @nogc @system
{
    auto w = cast(af_actor_wave*) context;
    w.operation(w.user, cast(af_actor_kind*) borrow.rawKind(), borrow.rawState(), borrow.stateSize);
}
private void waveCompleted(void* context) nothrow @nogc @system
{
    auto w = cast(af_actor_wave*) context;
    auto completion = w.completion;
    if (completion.call !is null) completion.call(completion.context);
    // Final wrapper access. Readiness joins the user callback as well as the
    // underlying wave; a waiter may now reuse or destroy the stable storage.
    atomicStore!(MemoryOrder.rel)(w.callbackDone, 1u);
}

extern(C) nothrow @nogc @system:
uint af_actor_runtime_create(af_farm* f, size_t capacity, const(af_actor_allocator)* a, af_actor_runtime** output)
{
    return createActorRuntime(f, capacity, a, true, output);
}
size_t af_actor_runtime_live(const(af_actor_runtime)* r) { requireRuntime(r); return r.impl.live; }
size_t af_actor_runtime_ready(const(af_actor_runtime)* r) { requireRuntime(r); return r.impl.ready; }
size_t af_actor_runtime_flush(af_actor_runtime* r, af_token* t, size_t maximum, uint cost)
{
    requireRuntime(r);
    if (t is null || t.owner !is r.farm) fatal("C ABI foreign or null actor producer token");
    if (cost > MAX_AVG_COST) fatal("avgCost out of range");
    return r.impl.flush(t.value, maximum, cost);
}
void af_actor_runtime_destroy(af_actor_runtime* r)
{
    if (r is null) return;
    requireRuntime(r);
    if (inFarmCallback()) fatal("C ABI actor runtime destruction inside Farm callback");
    if (!r.owned) fatal("C ABI cannot destroy a host-owned actor runtime");
    finishActorRuntime(r);
    atomicOp!"-="(r.farm.actorRuntimes, 1u);
    free(r);
}

uint af_actor_kind_register(af_actor_runtime* r, const(af_actor_kind_options)* options,
        const(af_actor_kind)** output)
{
    if (output is null) return setError(AF_INVALID, "null actor kind output");
    *output = null;
    if (r is null || r.impl is null || options is null || options.handler is null ||
        options.state_bytes == 0 || options.state_alignment == 0 ||
        (options.state_alignment & (options.state_alignment - 1)) ||
        (!r.custom && options.state_alignment > 64) ||
        options.state_bytes > size_t.max - (options.state_alignment - 1))
        return setError(AF_INVALID, "invalid actor kind");
    auto allocator = runtimeAllocator(r);
    auto k = cast(af_actor_kind*) allocator.allocate(allocator.context, af_actor_kind.sizeof, af_actor_kind.alignof);
    if (k is null) return setError(AF_ALLOCATION_FAILED, "actor kind allocation failed");
    *k = af_actor_kind.init;
    k.runtime = r;
    k.options = af_actor_kind_options(options.handler, cast(void*) options.user,
        options.state_bytes, options.state_alignment, options.destroy);
    k.value = ActorKind(k, options.destroy is null ? null : &destroyState);
    // Registration only appends immutable nodes. No node is removed before
    // externally quiescent destruction, so this head cannot suffer ABA.
    auto observed = atomicLoad!(MemoryOrder.acq)(r.kindHeadWord);
    do
    {
        k.next = cast(af_actor_kind*) observed;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.acq)(
                &r.kindHeadWord, observed, cast(size_t) k)) break;
        observed = atomicLoad!(MemoryOrder.acq)(r.kindHeadWord);
    } while (true);
    *output = k;
    return setError(AF_OK);
}
uint af_actor_create_copy(af_actor_runtime* r, const(af_actor_kind)* kind,
        const(void)* initial, af_actor_owner* output)
{
    auto result = createActors(r, kind,
        af_actor_state_span(initial, kind is null ? 0 : kind.options.state_bytes, 1),
        null, null, 0, af_actor_owner_output_span(output, af_actor_owner.sizeof, 1));
    return result.status;
}
uint af_actor_create_init(af_actor_runtime* r, const(af_actor_kind)* kind,
        CActorInitializer initialize, void* user, size_t index, af_actor_owner* output)
{
    if (initialize is null) return setError(AF_INVALID, "null actor initializer");
    return createActors(r, kind, af_actor_state_span.init, initialize, user, index,
        af_actor_owner_output_span(output, af_actor_owner.sizeof, 1)).status;
}
af_actor_batch_result af_actor_create_copy_batch(af_actor_runtime* r, const(af_actor_kind)* kind,
        af_actor_state_span input, af_actor_owner_output_span output)
{ return createActors(r, kind, input, null, null, 0, output); }
af_actor_batch_result af_actor_create_init_batch(af_actor_runtime* r, const(af_actor_kind)* kind,
        CActorInitializer initialize, void* user, size_t firstIndex, af_actor_owner_output_span output)
{
    if (initialize is null) return af_actor_batch_result(0, setError(AF_INVALID, "null actor initializer"));
    return createActors(r, kind, af_actor_state_span.init, initialize, user, firstIndex, output);
}
void af_actor_owner_handle(const(af_actor_owner)* o, af_actor_handle* output)
{
    if (o is null || output is null) fatal("C ABI null actor identity");
    *output = af_actor_handle(cast(void*) o._slot, o._generation);
}
uint af_actor_wake(const(af_actor_handle)* h) { return cast(uint) handle(h).wake(); }
uint af_actor_send(const(af_actor_handle)* h, af_actor_message* m)
{
    requireMessage(m);
    return cast(uint) handle(h).send(&m.value);
}
uint af_actor_request_retire(af_actor_runtime* r, const(af_actor_owner)* o, uint mode)
{
    if (!validOwner(r, o) || mode > 1) return setError(AF_INVALID, "invalid actor owner or retirement mode");
    return retireBase + cast(uint) requestActorRetire(cast(ActorSlot*) o._slot, o._generation, cast(ActorRetireMode) mode);
}
uint af_actor_retired(af_actor_runtime* r, const(af_actor_owner)* o, uint* output)
{
    if (!validOwner(r, o) || output is null) return setError(AF_INVALID, "invalid actor owner or query output");
    *output = actorRetired(cast(ActorSlot*) o._slot, o._generation);
    return setError(AF_OK);
}
uint af_actor_reclaim(af_actor_runtime* r, af_actor_owner* o)
{
    if (!validOwner(r, o)) return setError(AF_INVALID, "invalid actor owner");
    auto slot = cast(ActorSlot*) o._slot;
    auto generation = o._generation;
    auto result = reclaimActor(slot, generation);
    if (result == ActorReclaimResult.reclaimed || result == ActorReclaimResult.staleOwner)
        *o = af_actor_owner.init;
    return reclaimBase + cast(uint) result;
}
af_actor_batch_result af_actor_wake_batch(af_actor_handle_span input, af_actor_result_span output)
{
    if (output.count < input.count ||
        !validSpan(input.data, input.stride, input.count, af_actor_handle.sizeof, af_actor_handle.alignof) ||
        !validSpan(output.data, output.stride, output.count, uint.sizeof, uint.alignof))
        return af_actor_batch_result(0, setError(AF_INVALID, "invalid actor wake span"));
    size_t count;
    foreach (i; 0 .. input.count)
    {
        auto result = af_actor_wake(element(input.data, input.stride, i));
        *element(output.data, output.stride, i) = result;
        if (result <= cast(uint) ActorWakeResult.coalesced) ++count;
    }
    return af_actor_batch_result(count, setError(AF_OK));
}
af_actor_batch_result af_actor_request_retire_batch(af_actor_runtime* r, af_actor_owner_span input,
        uint mode, af_actor_result_span output)
{
    if (r is null || r.impl is null || mode > 1 || output.count < input.count ||
        !validSpan(input.data, input.stride, input.count, af_actor_owner.sizeof, af_actor_owner.alignof) ||
        !validSpan(output.data, output.stride, output.count, uint.sizeof, uint.alignof))
        return af_actor_batch_result(0, setError(AF_INVALID, "invalid actor retirement span"));
    size_t count;
    foreach (i; 0 .. input.count)
    {
        auto result = af_actor_request_retire(r, element(input.data, input.stride, i), mode);
        *element(output.data, output.stride, i) = result;
        if (result == retireBase + cast(uint) ActorRetireResult.requested ||
            result == retireBase + cast(uint) ActorRetireResult.alreadyRetired) ++count;
    }
    return af_actor_batch_result(count, setError(AF_OK));
}
af_actor_batch_result af_actor_reclaim_batch(af_actor_runtime* r, af_actor_owner_output_span input,
        af_actor_result_span output)
{
    if (r is null || r.impl is null || output.count < input.count ||
        !validSpan(input.data, input.stride, input.count, af_actor_owner.sizeof, af_actor_owner.alignof) ||
        !validSpan(output.data, output.stride, output.count, uint.sizeof, uint.alignof))
        return af_actor_batch_result(0, setError(AF_INVALID, "invalid actor reclaim span"));
    size_t count;
    foreach (i; 0 .. input.count)
    {
        auto result = af_actor_reclaim(r, element(input.data, input.stride, i));
        *element(output.data, output.stride, i) = result;
        if (result == reclaimBase + cast(uint) ActorReclaimResult.reclaimed ||
            result == reclaimBase + cast(uint) ActorReclaimResult.staleOwner) ++count;
    }
    return af_actor_batch_result(count, setError(AF_OK));
}
void af_actor_republish(af_actor_context* c) { requireContext(c); c.value.republish(); }
void af_actor_retire(af_actor_context* c) { requireContext(c); c.value.retire(); }
uint af_actor_closing(const(af_actor_context)* c) { requireContext(c); return c.value.closing; }
uint af_actor_cancelling(const(af_actor_context)* c) { requireContext(c); return c.value.cancelling; }
af_actor_message* af_actor_pop(af_actor_context* c)
{
    requireContext(c);
    return cast(af_actor_message*) c.value.popInbox();
}
size_t af_actor_cancel_inbox(af_actor_context* c) { requireContext(c); return c.value.cancelInbox(); }
void af_actor_message_init(af_actor_message* m, void* payload, ulong tag)
{
    requireMessage(m);
    m.value.initialize(payload, tag);
}
void af_actor_message_set(af_actor_message* m, void* payload, ulong tag)
{
    requireMessage(m);
    if (!m.value.available) fatal("C ABI modify busy actor message");
    m.value.payload = payload;
    m.value.tag = tag;
}
void* af_actor_message_payload(const(af_actor_message)* m) { requireMessage(m); return cast(void*) m.value.payload; }
ulong af_actor_message_tag(const(af_actor_message)* m) { requireMessage(m); return m.value.tag; }
uint af_actor_message_available(const(af_actor_message)* m) { requireMessage(m); return m.value.available; }
uint af_actor_message_outcome(const(af_actor_message)* m) { requireMessage(m); return cast(uint) m.value.outcome; }
uint af_actor_message_failure(const(af_actor_message)* m) { requireMessage(m); return m.value.failureCode; }
void af_actor_message_complete(af_actor_message* m) { requireMessage(m); m.value.complete(); }
void af_actor_message_fail(af_actor_message* m, uint code) { requireMessage(m); m.value.fail(code); }
void af_actor_message_cancel(af_actor_message* m) { requireMessage(m); m.value.cancel(); }
uint af_actor_message_complete_and_wake(af_actor_message* m, const(af_actor_handle)* sender)
{
    requireMessage(m);
    // Snapshot before completion: sender may itself be stored in the payload
    // that another thread frees as soon as the node's release is observed.
    auto target = handle(sender);
    m.value.complete();
    version (AntfarmCActorTestHooks) if (afterCompletion !is null) afterCompletion();
    return cast(uint) target.wake();
}
uint af_actor_message_fail_and_wake(af_actor_message* m, uint code, const(af_actor_handle)* sender)
{
    requireMessage(m);
    auto target = handle(sender);
    m.value.fail(code);
    version (AntfarmCActorTestHooks) if (afterCompletion !is null) afterCompletion();
    return cast(uint) target.wake();
}
uint af_actor_message_cancel_and_wake(af_actor_message* m, const(af_actor_handle)* sender)
{
    requireMessage(m);
    auto target = handle(sender);
    m.value.cancel();
    version (AntfarmCActorTestHooks) if (afterCompletion !is null) afterCompletion();
    return cast(uint) target.wake();
}

uint af_actor_wave_create(af_farm* f, af_actor_wave** output)
{
    if (output is null) return setError(AF_INVALID, "null actor wave output");
    *output = null;
    if (inFarmCallback()) fatal("C ABI actor wave creation inside Farm callback");
    if (f is null || f.impl is null) return setError(AF_INVALID, "null actor wave Farm");
    auto allocator = ActorAllocator.cRuntime();
    auto w = cast(af_actor_wave*) allocator.allocate(null, af_actor_wave.sizeof, af_actor_wave.alignof);
    if (w is null) return setError(AF_ALLOCATION_FAILED, "actor wave allocation failed");
    static immutable initial = af_actor_wave.init;
    memcpy(w, &initial, af_actor_wave.sizeof);
    w.farm = f;
    atomicOp!"+="(f.waves, size_t(1));
    *output = w;
    return setError(AF_OK);
}
void af_actor_wave_begin(af_actor_wave* w, CWaveOperation operation, void* user,
        const(TableCompletionHook)* completion)
{
    requireWave(w);
    if (inFarmCallback()) fatal("C ABI actor wave begin inside Farm callback");
    if (w.value.handle.generation != 0 && !waveReady(w))
        fatal("C ABI reuse unfinished actor wave or completion callback");
    if (operation is null) fatal("C ABI null actor wave operation");
    if (completion !is null && completion.call is null) fatal("C ABI hook requires a callback");
    w.operation = operation;
    w.user = user;
    w.completion = completion is null ? TableCompletionHook.init :
        TableCompletionHook(cast(void*) completion.context, completion.call);
    atomicStore!(MemoryOrder.rel)(w.callbackDone, 0u);
    w.value.begin(w.farm.impl, ActorWaveCompletionHook(w, &waveCompleted));
}
void af_actor_wave_observe(af_actor_wave* w, af_actor_wave_handle* output)
{
    requireWave(w);
    if (output is null) fatal("C ABI null actor wave handle output");
    *output = af_actor_wave_handle(w, w.value.handle.generation);
}
size_t af_actor_wave_publish(af_actor_wave* w, af_actor_handle_span actors,
        af_token* token, uint cost)
{
    requireWave(w);
    if (token is null || token.owner !is w.farm) fatal("C ABI foreign or null wave producer token");
    if (cost > MAX_AVG_COST) fatal("avgCost out of range");
    if (!validSpan(actors.data, actors.stride, actors.count, af_actor_handle.sizeof, af_actor_handle.alignof))
        fatal("C ABI invalid wave span");
    return w.value.publishErased(CActorHandleRange(actors),
        token.value, &dispatchWave, w, cost);
}
void af_actor_wave_seal(af_actor_wave* w)
{
    requireWave(w);
    w.value.seal();
}
uint af_actor_wave_valid(const(af_actor_wave_handle)* h) { return waveHandle(h).valid; }
uint af_actor_wave_finished(const(af_actor_wave_handle)* h) { return waveHandle(h).finished; }
uint af_actor_wave_failed(const(af_actor_wave_handle)* h) { return waveHandle(h).failed; }
uint af_actor_wave_interrupted(const(af_actor_wave_handle)* h) { return waveHandle(h).interrupted; }
ulong af_actor_wave_progress(const(af_actor_wave_handle)* h) { return waveHandle(h).progress; }
ulong af_actor_wave_length(const(af_actor_wave_handle)* h) { return waveHandle(h).length; }
uint af_actor_wave_ready(const(af_actor_wave_handle)* h)
{
    auto observed = waveHandle(h);
    if (!observed.valid) return false;
    immutable done = atomicLoad!(MemoryOrder.acq)(h._wave.callbackDone);
    return done != 0 && observed.finished;
}
void af_actor_wave_destroy(af_actor_wave* w)
{
    if (w is null) return;
    if (inFarmCallback()) fatal("C ABI actor wave destruction inside Farm callback");
    if (w.value.handle.generation != 0 && !waveReady(w))
        fatal("C ABI destroy unfinished actor wave or completion callback");
    atomicOp!"-="(w.farm.waves, size_t(1));
    auto allocator = ActorAllocator.cRuntime();
    allocator.deallocate(null, w, af_actor_wave.sizeof, af_actor_wave.alignof);
}
