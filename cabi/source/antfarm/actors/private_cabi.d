// Private actor C ABI. BetterC; uses the existing private scheduling protocol.
module antfarm.actors.private_cabi;
import antfarm : fatal, inFarmCallback, rootCostHintAuto;
import antfarm.c.core;
import antfarm.actors.cabi : af_actor_allocator;
import antfarm.actors.actor : ActorAllocator;
import antfarm.actors.private_actor;
import core.atomic : atomicOp;
import core.stdc.string : memcpy;

struct af_private_roots
{
    ErasedRootPark* impl;
    af_farm* farm;
    af_actor_allocator allocator;
    bool entered, tracking;
}
alias af_private_children = ErasedChildPark;
struct af_private_root_handle { PrivateRootHandle value; }
struct af_private_root_outcome { af_private_root_handle root; uint status, cause, code, reserved; }
struct af_private_wave_outcome
{
    size_t participants, dispatched;
    uint failures, first_failure_code;
    size_t first_failure_position;
    uint cancelled, reserved;
}
struct af_private_pump_count { size_t roots, tables; }
struct af_private_results { const(void)* data; size_t stride, count; }
struct af_private_state_options
{
    size_t bytes, alignment;
    ErasedPrivateDestructor destroy;
    void* destroy_user;
}
struct af_private_root_options
{
    uint struct_size, abi_version;
    size_t capacity, batch_limit;
    af_private_state_options state;
    ErasedRootOperation step;
    void* user;
    uint tracking, reclamation;
}
struct af_private_child_options
{
    uint struct_size, abi_version;
    size_t capacity, batch_limit;
    af_private_state_options state;
    size_t result_bytes, result_alignment, slab_byte_limit;
    uint storage, subsets;
}
static assert(af_private_root_handle.sizeof == 16 && af_private_root_handle.alignof == 8);
static assert(af_private_root_outcome.sizeof == 32 && af_private_wave_outcome.sizeof == 40);
static assert(af_private_root_options.sizeof == 80 && af_private_root_options.step.offsetof == 56);
static assert(af_private_child_options.sizeof == 88 && af_private_child_options.storage.offsetof == 80);
static assert(af_private_results.sizeof == 24 && af_private_pump_count.sizeof == 16);

enum uint AF_PRIVATE_RETIRING = 9, AF_PRIVATE_EMPTY = 10;
private bool geometry(size_t bytes, size_t alignment) nothrow @nogc
{ return bytes != 0 && alignment != 0 && (alignment & (alignment - 1)) == 0 && bytes <= size_t.max - (alignment - 1); }
private ErasedStateLayout layout(const af_private_state_options s) nothrow @nogc @system
{ return ErasedStateLayout(s.bytes, s.alignment, (s.bytes + s.alignment - 1) & ~(s.alignment - 1), s.destroy, cast(void*) s.destroy_user); }
private void* allocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto p = cast(af_private_roots*) context;
    auto memory = p.allocator.allocate !is null
        ? p.allocator.allocate(p.allocator.context, bytes, alignment)
        : ActorAllocator.cRuntime().allocate(null, bytes, alignment);
    if (memory !is null && cast(size_t) memory % alignment)
        fatal("C ABI private allocator returned misaligned storage");
    return memory;
}
private void deallocate(void* context, void* memory, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto p = cast(af_private_roots*) context;
    if (p.allocator.deallocate !is null) p.allocator.deallocate(p.allocator.context, memory, bytes, alignment);
    else ActorAllocator.cRuntime().deallocate(null, memory, bytes, alignment);
}
private void enter(af_private_roots* p) nothrow @nogc @system
{
    if (p is null) fatal("C ABI null private root park");
    if (inFarmCallback()) fatal("C ABI private root operation inside Farm callback");
    if (p.entered) fatal("C ABI private root park reentry");
    p.entered = true;
}
private PrivateContext* context(void* c) nothrow @nogc @system { return checkedErasedContext(c); }
private void requireChildren(af_private_children* p) nothrow @nogc @system
{ if (p is null) fatal("C ABI null private child cohort"); }
private uint spawnStatus(uint result) nothrow @nogc @system
{
    switch (result)
    {
        case 0: return setError(AF_OK);
        case 1: return setError(AF_FULL, "private park full");
        case 2: return setError(AF_ALLOCATION_FAILED, "private state allocation failed");
        case 3: return setError(AF_BUSY, "private child wave active");
        case 4: return setError(AF_PRIVATE_RETIRING, "private parent retiring");
        case 5: return setError(8, "private state initializer failed");
        default: fatal("invalid private spawn result"); return AF_INVALID;
    }
}
private uint startStatus(PrivateStartResult result) nothrow @nogc @system
{
    final switch (result)
    {
        case PrivateStartResult.started: return setError(AF_OK);
        case PrivateStartResult.empty: return setError(AF_PRIVATE_EMPTY, "empty private wave");
        case PrivateStartResult.waveActive: return setError(AF_BUSY, "private child wave active");
        case PrivateStartResult.retiring: return setError(AF_PRIVATE_RETIRING, "private parent retiring");
    }
}
private uint spawnRoot(af_private_roots* p, const(void)* initial, ErasedPrivateInitializer init,
    void* user, size_t index, long deadline, af_private_root_handle* handle) nothrow @nogc @system
{
    if (handle !is null) *handle = af_private_root_handle.init;
    enter(p);
    scope(exit) p.entered = false;
    if ((!p.tracking && (handle !is null || deadline != long.max)) || (initial is null && init is null))
        return setError(AF_INVALID, "invalid private spawn input or untracked deadline/handle");
    return spawnStatus(erasedRootSpawn(p.impl, initial, init, user, index,
        handle is null ? null : &handle.value, deadline));
}

extern(C) nothrow @nogc @system:
void af_private_root_options_default(af_private_root_options* o)
{
    if (o is null) fatal("C ABI null private root options");
    *o = af_private_root_options.init;
    o.struct_size = af_private_root_options.sizeof;
    o.abi_version = AF_ABI_VERSION;
    o.capacity = 1024; o.batch_limit = defaultPrivateBatchLimit;
    o.state.alignment = 8;
}
void af_private_child_options_default(af_private_child_options* o)
{
    if (o is null) fatal("C ABI null private child options");
    *o = af_private_child_options.init;
    o.struct_size = af_private_child_options.sizeof;
    o.abi_version = AF_ABI_VERSION;
    o.capacity = 64; o.state.alignment = 8; o.result_alignment = 1;
}
uint af_private_roots_create(af_farm* farm, const(af_private_root_options)* o,
    const(af_actor_allocator)* a, af_private_roots** output)
{
    if (output is null) return setError(AF_INVALID, "null private park output");
    *output = null;
    if (inFarmCallback()) fatal("C ABI private root operation inside Farm callback");
    if (farm is null || o is null || o.struct_size != af_private_root_options.sizeof)
        return setError(AF_INVALID, "invalid private Farm/options size");
    if (o.abi_version != AF_ABI_VERSION || o.tracking > 1 || o.reclamation > 1 || o.step is null
        || !geometry(o.state.bytes, o.state.alignment) || !erasedRootGeometry(o.capacity, o.batch_limit, o.tracking != 0)
        || (a is null && o.state.alignment > 64)
        || (a !is null && (a.allocate is null || a.deallocate is null)))
        return setError(AF_INVALID, "invalid private root options or allocator");
    auto p = cast(af_private_roots*) (a is null
        ? ActorAllocator.cRuntime().allocate(null, af_private_roots.sizeof, af_private_roots.alignof)
        : a.allocate(cast(void*) a.context, af_private_roots.sizeof, af_private_roots.alignof));
    if (p is null) return setError(AF_ALLOCATION_FAILED, "private park handle allocation failed");
    if (cast(size_t) p % af_private_roots.alignof) fatal("C ABI private allocator returned misaligned storage");
    *p = af_private_roots.init;
    if (a !is null) p.allocator = af_actor_allocator(cast(void*) a.context, a.allocate, a.deallocate);
    p.entered = true; p.farm = farm; p.tracking = o.tracking != 0;
    auto allocator = ActorAllocator(p, &allocate, &deallocate);
    p.impl = erasedRootCreate(farm.impl, o.capacity, PrivateRootOptions(o.batch_limit, allocator, p.tracking),
        layout(o.state), o.step, cast(void*) o.user, o.reclamation != 0);
    if (p.impl is null)
    {
        // Snapshot before freeing the bridge that owns the allocator vtable.
        auto saved = p.allocator;
        if (saved.deallocate !is null) saved.deallocate(saved.context, p, af_private_roots.sizeof, af_private_roots.alignof);
        else ActorAllocator.cRuntime().deallocate(null, p, af_private_roots.sizeof, af_private_roots.alignof);
        return setError(AF_ALLOCATION_FAILED, "private park allocation failed");
    }
    p.entered = false;
    atomicOp!"+="(farm.privateParks, 1UL);
    *output = p;
    return setError(AF_OK);
}
size_t af_private_roots_live(af_private_roots* p)
{ enter(p); scope(exit) p.entered = false; return p.impl.live; }
void af_private_roots_collect(af_private_roots* p)
{ enter(p); scope(exit) p.entered = false; p.impl.collectCompleted(); }
af_private_pump_count af_private_roots_pump(af_private_roots* p, af_token* token, size_t limit, uint cost)
{
    enter(p); scope(exit) p.entered = false;
    if (token is null || token.owner !is p.farm) fatal("C ABI foreign or null private producer token");
    if (cost > 7 && cost != rootCostHintAuto) fatal("avgCost out of range");
    auto n = erasedRootPump(p.impl, token.value, limit, cost);
    return af_private_pump_count(n.roots, n.tables);
}
uint af_private_root_spawn_copy(af_private_roots* p, const(void)* initial, long deadline, af_private_root_handle* handle)
{ return spawnRoot(p, initial, null, null, 0, deadline, handle); }
uint af_private_root_spawn_init(af_private_roots* p, ErasedPrivateInitializer init, void* user,
    size_t index, long deadline, af_private_root_handle* handle)
{ return spawnRoot(p, null, init, user, index, deadline, handle); }
uint af_private_root_take_outcome(af_private_roots* p, af_private_root_outcome* output)
{
    enter(p); scope(exit) p.entered = false;
    if (output is null) fatal("C ABI null private outcome output");
    *output = af_private_root_outcome.init;
    PrivateRootOutcome result;
    if (!p.impl.takeOutcome(result)) return false;
    output.root.value = result.root;
    output.status = result.status; output.cause = result.cause; output.code = result.code;
    return true;
}
size_t af_private_roots_cancel_all(af_private_roots* p)
{ enter(p); scope(exit) p.entered = false; return p.impl.cancelAll(); }
uint af_private_root_request_cancel(const(af_private_root_handle)* h)
{ auto value = h is null ? PrivateRootHandle.init : cast(PrivateRootHandle) h.value; return value.requestCancel(); }
size_t af_private_root_index(const(af_private_root_handle)* h)
{ if (h is null) fatal("private root handle: invalid"); return h.value.index; }
long af_private_clock_now() { return erasedPrivateClockNow(); }
long af_private_clock_frequency() { return erasedPrivateClockFrequency(); }
void af_private_roots_destroy(af_private_roots* p)
{
    if (p is null) return;
    enter(p);
    p.impl.destroy();
    auto farm = p.farm;
    auto saved = p.allocator;
    if (saved.deallocate !is null) saved.deallocate(saved.context, p, af_private_roots.sizeof, af_private_roots.alignof);
    else ActorAllocator.cRuntime().deallocate(null, p, af_private_roots.sizeof, af_private_roots.alignof);
    atomicOp!"-="(farm.privateParks, 1UL);
}
uint af_private_children_create(void* c, const(af_private_child_options)* o, af_private_children** output)
{
    auto ctx = context(c);
    if (output is null) return setError(AF_INVALID, "null private child output");
    *output = null;
    if (o is null || o.struct_size != af_private_child_options.sizeof)
        return setError(AF_INVALID, "invalid private child options size");
    if (o.abi_version != AF_ABI_VERSION || o.storage > 1 || o.subsets > 1
        || !geometry(o.state.bytes, o.state.alignment)
        || (o.result_bytes ? !geometry(o.result_bytes, o.result_alignment) : o.result_alignment != 1))
        return setError(AF_INVALID, "invalid private child options");
    auto l = ErasedCohortLayout(layout(o.state), o.result_bytes, o.result_alignment,
        o.result_bytes ? (o.result_bytes + o.result_alignment - 1) & ~(o.result_alignment - 1) : 0, o.storage != 0);
    if (!erasedChildGeometry(o.capacity, o.subsets != 0, l))
        return setError(AF_INVALID, "private child size overflow or invalid capacity");
    if (erasedContextRetiring(*ctx)) return setError(AF_PRIVATE_RETIRING, "private parent retiring");
    if (l.slab && o.capacity * l.state.stride > o.slab_byte_limit)
        return setError(AF_ALLOCATION_FAILED, "private slab byte limit exceeded");
    auto p = erasedChildCreate(*ctx, o.capacity, PrivateCohortOptions(o.batch_limit, o.subsets != 0), l);
    if (p is null) return setError(AF_ALLOCATION_FAILED, "private cohort allocation failed");
    *output = p;
    return setError(AF_OK);
}
uint af_private_child_spawn_copy(af_private_children* p, void* c, const(void)* initial)
{
    auto ctx = context(c); requireChildren(p);
    if (initial is null) return setError(AF_INVALID, "null private child initializer");
    return spawnStatus(erasedChildSpawn(p, *ctx, initial, null, null, 0));
}
uint af_private_child_spawn_init(af_private_children* p, void* c, ErasedPrivateInitializer init, void* user, size_t index)
{
    auto ctx = context(c); requireChildren(p);
    if (init is null) return setError(AF_INVALID, "null private child initializer");
    return spawnStatus(erasedChildSpawn(p, *ctx, null, init, user, index));
}
uint af_private_children_start(af_private_children* p, void* c, ErasedChildOperation op,
    void* user, uint retire, uint continuation, uint cost)
{
    auto ctx = context(c); requireChildren(p);
    if (op is null || retire > 1 || continuation > 1 || cost > 7)
        return setError(AF_INVALID, "invalid private child wave options");
    return startStatus(erasedChildStart(p, *ctx, op, user, continuation != 0, retire != 0, false, null, cost));
}
uint af_private_children_start_subset(af_private_children* p, void* c, ErasedChildOperation op,
    void* user, const(uint)* members, size_t count, uint continuation, uint cost)
{
    auto ctx = context(c); requireChildren(p);
    if (op is null || continuation > 1 || cost > 7 || (count && (members is null
        || cast(size_t) members % uint.alignof || count > size_t.max / uint.sizeof
        || cast(size_t) members > size_t.max - count * uint.sizeof)))
        return setError(AF_INVALID, "invalid private subset options");
    return startStatus(erasedChildStart(p, *ctx, op, user, continuation != 0, false, true,
        count ? members[0 .. count] : null, cost));
}
uint af_private_children_finished(af_private_children* p, void* c)
{ auto ctx = context(c); requireChildren(p); return p.finished(*ctx); }
uint af_private_children_cancel(af_private_children* p, void* c)
{ auto ctx = context(c); requireChildren(p); return p.cancel(*ctx); }
void af_private_children_outcome(af_private_children* p, void* c, af_private_wave_outcome* output)
{
    auto ctx = context(c); requireChildren(p);
    if (output is null) fatal("C ABI null private wave outcome");
    auto r = p.outcome(*ctx);
    *output = af_private_wave_outcome(r.participants, r.dispatched, r.failures, r.firstFailureCode,
        r.firstFailurePosition, r.cancelled, 0);
}
af_private_results af_private_children_results(af_private_children* p, void* c)
{
    auto ctx = context(c); requireChildren(p);
    size_t count;
    auto data = erasedChildResults(p, *ctx, count);
    return af_private_results(data, erasedChildResultStride(p), count);
}
const(uint)* af_private_children_subset(af_private_children* p, void* c, size_t* count)
{
    auto ctx = context(c); requireChildren(p);
    if (count is null) fatal("C ABI null private subset count");
    auto members = p.subsetMembers(*ctx); *count = members.length; return members.ptr;
}
void af_private_fail(void* c, uint code) { context(c).fail(code); }
uint af_private_cancelled(void* c) { return context(c).cancelled; }
void af_private_cancel_children(void* c) { context(c).cancelChildren(); }
void af_private_cancel_peers(void* c) { context(c).cancelPeers(); }
