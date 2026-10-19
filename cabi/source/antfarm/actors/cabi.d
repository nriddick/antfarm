// Runtime-free public actor exports. D actor ordering and ownership remain authoritative.
module antfarm.actors.cabi;
import antfarm : fatal, inFarmCallback, MAX_AVG_COST;
import antfarm.actors.actor;
import antfarm.actors.wave;
import antfarm : TableCompletionHook;
import antfarm.c.core;
import core.atomic : MemoryOrder, atomicLoad, atomicStore, atomicOp, cas;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy, memset;

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
    shared size_t owners; // Includes in-flight creation reservations until reclaim returns.
    af_actor_runtime* next; // Control-thread host list.
}
struct af_actor_owner { af_actor_runtime* runtime; ActorErasedOwner value; }
struct af_actor_context { ActorContext* value; }
struct af_actor_message { ActorInboxNode value; }
private struct StatePrefix { CActorHandler call; void* user; size_t bytes, offset; }
private af_actor_context* activeContext;
version (AntfarmCActorTestHooks)
{
    private __gshared extern(C) void function() nothrow @nogc afterCompletion;
    extern(C) void af_test_actor_after_completion(typeof(afterCompletion) hook) nothrow @nogc { afterCompletion = hook; }
}
static assert(af_actor_handle.sizeof == 16 && af_actor_handle.alignof == 8);
static assert(af_actor_allocator.sizeof == 24);
static assert(af_actor_message.value.offsetof == 0 && af_actor_message.sizeof == ActorInboxNode.sizeof);
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
private void requireOwner(const(af_actor_owner)* o) nothrow @nogc @system
{
    if (o is null) fatal("C ABI null actor owner");
    requireRuntime(o.runtime);
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
    auto p = cast(StatePrefix*) borrow.rawState();
    af_actor_context c = af_actor_context(&ctx);
    auto previous = activeContext;
    activeContext = &c;
    p.call(p.user, cast(ubyte*) p + p.offset, p.bytes, &c);
    activeContext = previous;
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
    return atomicLoad(r.owners) == 0 && r.impl.quiescent;
}
void finishActorRuntime(af_actor_runtime* r) nothrow @nogc @system
{
    if (atomicLoad(r.owners)) fatal("C ABI destroy actor runtime with live owners");
    r.impl.destroy();
    r.impl = null;
}


extern(C) alias CWaveOperation = void function(void*, void*, size_t) nothrow @nogc @system;
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
    const(af_actor_handle)[] values;
    @property size_t length() const pure nothrow @nogc @safe { return values.length; }
    ActorErasedHandle opIndex(size_t i) pure nothrow @nogc @system
    { return ActorErasedHandle(cast(ActorSlot*) values[i]._slot, values[i]._generation); }
    CActorHandleRange opSlice(size_t lo, size_t hi) pure nothrow @nogc @safe
    { return CActorHandleRange(values[lo .. hi]); }
    size_t opDollar() const pure nothrow @nogc @safe { return values.length; }
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
    auto p = cast(StatePrefix*) borrow.rawState();
    w.operation(w.user, cast(ubyte*) p + p.offset, p.bytes);
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
uint af_actor_create(af_actor_runtime* r, const(void)* initial, size_t bytes, size_t alignment,
    CActorHandler call, void* user, af_actor_owner** output)
{
    if (output is null) return setError(AF_INVALID, "null actor owner output");
    *output = null;
    if (r is null || r.impl is null || initial is null || !bytes || call is null ||
        !alignment || (alignment & (alignment - 1)) || (!r.custom && alignment > 64) ||
        cast(size_t) initial > size_t.max - bytes ||
        alignment - 1 > size_t.max - StatePrefix.sizeof)
        return setError(AF_INVALID, "invalid actor state or handler");
    immutable offset = (StatePrefix.sizeof + alignment - 1) & ~(alignment - 1);
    if (bytes > size_t.max - offset) return setError(AF_INVALID, "actor state size overflow");
    immutable total = offset + bytes;
    // The reservation remains until the D free-slot return completes. Hence a
    // successful reservation guarantees capacity; a failed D create means OOM.
    for (;;)
    {
        auto count = atomicLoad(r.owners);
        if (count >= r.impl.capacity) return setError(AF_FULL, "actor capacity full");
        if (cas(&r.owners, count, count + 1)) break;
    }
    auto o = allocate!af_actor_owner();
    auto temp = cast(StatePrefix*) malloc(total);
    if (o is null || temp is null)
    {
        free(o); free(temp); atomicOp!"-="(r.owners, size_t(1));
        return setError(AF_ALLOCATION_FAILED, "actor creation buffer allocation failed");
    }
    memset(temp, 0, total);
    *temp = StatePrefix(call, user, bytes, offset);
    memcpy(cast(ubyte*) temp + offset, initial, bytes);
    o.value = r.impl.erasedAdapter.createActor(temp, total,
        alignment < StatePrefix.alignof ? StatePrefix.alignof : alignment, &dispatch);
    free(temp);
    if (!o.value.valid)
    {
        free(o); atomicOp!"-="(r.owners, size_t(1));
        return setError(AF_ALLOCATION_FAILED, "actor state allocation failed");
    }
    o.runtime = r;
    *output = o;
    return setError(AF_OK);
}
void af_actor_owner_handle(af_actor_owner* o, af_actor_handle* output)
{
    requireOwner(o);
    if (output is null) fatal("C ABI null actor handle output");
    auto h = o.value.handle;
    *output = af_actor_handle(h.slot_, h.generation_);
}
uint af_actor_wake(const(af_actor_handle)* h) { return cast(uint) handle(h).wake(); }
uint af_actor_send(const(af_actor_handle)* h, af_actor_message* m)
{
    requireMessage(m);
    return cast(uint) handle(h).send(&m.value);
}
uint af_actor_request_retire(af_actor_owner* o, uint mode)
{
    requireOwner(o);
    if (mode > 1) fatal("C ABI invalid actor retire mode");
    return cast(uint) o.value.requestRetire(cast(ActorRetireMode) mode);
}
uint af_actor_retired(const(af_actor_owner)* o) { requireOwner(o); return o.value.retired; }
uint af_actor_reclaim(af_actor_owner* o)
{
    requireOwner(o);
    auto result = o.value.reclaim();
    if (result == ActorReclaimResult.reclaimed)
    {
        atomicOp!"-="(o.runtime.owners, size_t(1));
        free(o);
    }
    return cast(uint) result;
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
uint af_actor_message_create(void* payload, ulong tag, af_actor_message** output)
{
    if (output is null) return setError(AF_INVALID, "null actor message output");
    *output = allocate!af_actor_message();
    if (*output is null) return setError(AF_ALLOCATION_FAILED, "actor message allocation failed");
    (*output).value.initialize(payload, tag);
    return setError(AF_OK);
}
void af_actor_message_destroy(af_actor_message* m)
{
    if (m is null) return;
    if (!m.value.available) fatal("C ABI destroy busy actor message");
    free(m);
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
size_t af_actor_wave_publish(af_actor_wave* w, const(af_actor_handle)* actors,
        size_t count, af_token* token, uint cost)
{
    requireWave(w);
    if (token is null || token.owner !is w.farm) fatal("C ABI foreign or null wave producer token");
    if (cost > MAX_AVG_COST) fatal("avgCost out of range");
    if (count != 0)
    {
        if (actors is null) fatal("C ABI null nonempty wave span");
        if (cast(size_t) actors % af_actor_handle.alignof) fatal("C ABI misaligned wave span");
        if (count > size_t.max / af_actor_handle.sizeof ||
            cast(size_t) actors > size_t.max - count * af_actor_handle.sizeof)
            fatal("C ABI wave span size overflow");
    }
    return w.value.publishErased(CActorHandleRange(actors[0 .. count]),
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
