// C ABI 1. Built once with BetterC; the optional runtime links this same core.
module antfarm.c.core;
import antfarm;
import core.atomic : atomicOp, atomicLoad, MemoryOrder;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;

enum uint AF_ABI_VERSION = 1;
enum uint AF_OK = 0, AF_INVALID = 1, AF_FULL = 2, AF_RETRY = 3,
    AF_ALLOCATION_FAILED = 4, AF_SYSTEM_ERROR = 5, AF_BUSY = 6, AF_RUNTIME_ERROR = 7;

struct af_farm
{
    AntFarm* impl;
    bool owned;
    shared uint tokens, views, actorRuntimes;
    shared size_t waves;
}
struct af_token { af_farm* owner; Token value; }
struct af_view { af_farm* owner; ConsumerView value; }
struct af_entry { Callback call; const(ulong)* body; size_t body_words; }
struct af_body { const(ulong)* words; size_t count; }
struct af_options
{
    uint struct_size, abi_version;
    uint ring_mib, segments, expected_consumers, max_bulk_producers;
    ulong bulk_quota_words;
    uint max_small_producers, small_table_threshold;
    ulong small_quota_words;
    uint huge_pages, reserved;
}
static assert((void*).sizeof == 8 && size_t.sizeof == 8);
static assert(af_options.sizeof == 56 && af_options.small_quota_words.offsetof == 40);
static assert(af_entry.sizeof == 24 && af_entry.body_words.offsetof == 16);
static assert(af_body.sizeof == 16 && af_body.count.offsetof == 8);
static assert(TableCompletionHook.sizeof == 16 && TableCompletionHook.context.offsetof == 0 &&
    TableCompletionHook.call.offsetof == 8);

private char[512] diagnostic;
private int osDiagnostic;
// Shared by the runtime wrapper, which must not keep a second diagnostic copy.
uint setError(uint status, const(char)[] message = null, int osError = 0) nothrow @nogc @system
{
    immutable n = message.length < diagnostic.length - 1 ? message.length : diagnostic.length - 1;
    if (n) memcpy(diagnostic.ptr, message.ptr, n);
    diagnostic[n] = 0;
    osDiagnostic = osError;
    return status;
}

private T* allocate(T)() nothrow @nogc @system
{
    auto p = cast(T*) malloc(T.sizeof);
    if (p !is null)
    {
        // Copy the initial representation, including move-only structs.
        static immutable initial = T.init;
        memcpy(p, &initial, T.sizeof);
    }
    return p;
}

// A borrowed wrapper for FarmHost; its Farm remains owned by the host.
af_farm* wrapFarm(AntFarm* f, bool owned) nothrow @nogc @system
{
    auto p = allocate!af_farm();
    if (p !is null) { p.impl = f; p.owned = owned; }
    return p;
}
bool hasParticipants(af_farm* f) nothrow @nogc @system
{
    return atomicLoad!(MemoryOrder.acq)(f.tokens) != 0 || atomicLoad!(MemoryOrder.acq)(f.views) != 0;
}
void releaseBorrowedFarm(af_farm* f) nothrow @nogc @system
{
    if (f is null || f.owned || hasParticipants(f)) fatal("invalid borrowed C Farm release");
    free(f);
}

private void span(T)(const(T)* p, size_t n) nothrow @nogc @system
{
    if (!n) return;
    if (p is null) fatal("C ABI null nonempty span");
    if (cast(size_t) p % T.alignof) fatal("C ABI misaligned span");
    if (n > size_t.max / T.sizeof || cast(size_t) p > size_t.max - n * T.sizeof)
        fatal("C ABI span size overflow");
}
private PayloadBody body(const(ulong)* p, size_t n) nothrow @nogc @system
{
    span(p, n);
    return n ? p[0 .. n] : null;
}
private void requireFarm(af_farm* f) nothrow @nogc @system
{
    if (f is null || f.impl is null) fatal("C ABI null Farm");
}
private void requireWrite(af_farm* f, af_token* t, uint cost,
    const(TableCompletionHook)* hook) nothrow @nogc @system
{
    requireFarm(f);
    if (t is null || t.owner !is f) fatal("C ABI foreign or null producer token");
    if (cost > MAX_AVG_COST) fatal("avgCost out of range");
    if (hook !is null && hook.call is null) fatal("table completion hook requires a callback");
}
private void requireWalk() nothrow @nogc @system
{
    if (inFarmCallback()) fatal("C ABI consumer operation inside Farm callback");
}
private struct Entries
{
    const(af_entry)* p; size_t n;
    @property bool empty() const nothrow @nogc { return n == 0; }
    @property Entries save() const nothrow @nogc { return Entries(p, n); }
    @property PayloadEntry front() nothrow @nogc @system
    { return PayloadEntry(p.call, body(p.body, p.body_words)); }
    void popFront() nothrow @nogc @system { ++p; --n; }
}
private struct Bodies
{
    const(af_body)* p; size_t n;
    @property bool empty() const nothrow @nogc { return n == 0; }
    @property Bodies save() const nothrow @nogc { return Bodies(p, n); }
    @property PayloadBody front() nothrow @nogc @system { return body(p.words, p.count); }
    void popFront() nothrow @nogc @system { ++p; --n; }
}
private struct FixedBodies
{
    const(ulong)* p; size_t width, n;
    @property bool empty() const nothrow @nogc { return n == 0; }
    @property size_t length() const nothrow @nogc { return n; }
    @property FixedBodies save() const nothrow @nogc { return FixedBodies(p, width, n); }
    @property PayloadBody front() nothrow @nogc @system { return width ? p[0 .. width] : null; }
    void popFront() nothrow @nogc @system { if (width) p += width; --n; }
}

extern(C) nothrow @nogc @system:
uint af_abi_version() { return AF_ABI_VERSION; }
void af_options_default(af_options* o)
{
    if (o is null) fatal("C ABI null options output");
    *o = af_options(af_options.sizeof, AF_ABI_VERSION, 8, 8, 4, 2, 0, 16,
        DEFAULT_SMALL_TABLE_THRESHOLD, 4096, 0, 0);
}
uint af_create(const(af_options)* options, af_farm** outFarm)
{
    if (outFarm is null) return setError(AF_INVALID, "null Farm output");
    *outFarm = null;
    af_options defaults;
    if (options is null) { af_options_default(&defaults); options = &defaults; }
    // Read only the size prefix before trusting the supplied layout.
    if (options.struct_size != af_options.sizeof) return setError(AF_INVALID, "invalid options size");
    if (options.abi_version != AF_ABI_VERSION || options.reserved || options.huge_pages > 1)
        return setError(AF_INVALID, "invalid options version or reserved fields");
    FarmCreateError error;
    auto f = AntFarm.tryCreate(options.ring_mib, options.segments, options.expected_consumers,
        options.max_bulk_producers, options.bulk_quota_words, options.max_small_producers,
        options.small_quota_words, options.small_table_threshold, options.huge_pages != 0, &error);
    if (f is null)
    {
        uint status = error.kind == FarmCreateFailure.geometry || error.kind == FarmCreateFailure.hugePagesSetting
            ? AF_INVALID : error.kind == FarmCreateFailure.allocation ? AF_ALLOCATION_FAILED : AF_SYSTEM_ERROR;
        return setError(status, error.detail, error.osError);
    }
    auto handle = wrapFarm(f, true);
    if (handle is null) { f.destroy(); return setError(AF_ALLOCATION_FAILED, "Farm handle allocation failed"); }
    *outFarm = handle;
    return setError(AF_OK);
}
void af_destroy(af_farm* f)
{
    if (f is null) return;
    if (!f.owned) fatal("C ABI cannot destroy a borrowed host Farm");
    if (atomicLoad(f.actorRuntimes)) fatal("C ABI destroy Farm with actor runtimes");
    if (atomicLoad(f.waves)) fatal("C ABI destroy Farm with actor waves");
    f.impl.destroy();
    free(f);
}
uint af_can_fit(const(af_farm)* f, Callback call, size_t words, uint tier)
{
    return f !is null && f.impl !is null && tier <= 1 && f.impl.canFit(call, words, cast(Tier) tier);
}
uint af_register_producer(af_farm* f, uint tier, af_token** outToken)
{
    if (outToken is null) return setError(AF_INVALID, "null producer output");
    *outToken = null;
    if (f is null || tier > 1) return setError(AF_INVALID, "invalid Farm or tier");
    auto t = allocate!af_token();
    if (t is null) return setError(AF_ALLOCATION_FAILED, "producer handle allocation failed");
    t.value = f.impl.registerProducer(cast(Tier) tier);
    if (!t.value.valid) { free(t); return setError(AF_FULL, "producer tier full"); }
    t.owner = f;
    atomicOp!"+="(f.tokens, 1u);
    *outToken = t;
    return setError(AF_OK);
}
void af_unregister_producer(af_farm* f, af_token* t)
{
    requireFarm(f);
    if (t is null || t.owner !is f) fatal("C ABI foreign or null producer token");
    f.impl.unregisterProducer(t.value);
    atomicOp!"-="(f.tokens, 1u);
    free(t);
}
size_t af_write(af_farm* f, const(af_entry)* entries, size_t n, af_token* t,
    uint cost, const(TableCompletionHook)* hook)
{
    requireWrite(f, t, cost, hook);
    span(entries, n);
    return f.impl.write(Entries(entries, n), t.value, cost, cast(TableCompletionHook*) hook);
}
size_t af_write_bodies(af_farm* f, Callback call, const(af_body)* bodies, size_t n,
    af_token* t, uint cost, const(TableCompletionHook)* hook)
{
    requireWrite(f, t, cost, hook);
    if (call is null) fatal("null payload callback");
    span(bodies, n);
    return f.impl.write(call, Bodies(bodies, n), t.value, cost, cast(TableCompletionHook*) hook);
}
size_t af_write_fixed(af_farm* f, Callback call, const(ulong)* bodies, size_t width,
    size_t n, af_token* t, uint cost, const(TableCompletionHook)* hook)
{
    requireWrite(f, t, cost, hook);
    if (call is null) fatal("null payload callback");
    if (width && n > size_t.max / width) fatal("C ABI fixed body size overflow");
    span(bodies, width * n);
    return f.impl.write(call, FixedBodies(bodies, width, n), width, t.value, cost,
        cast(TableCompletionHook*) hook);
}
uint af_view_create(af_view** outView)
{
    if (outView is null) return setError(AF_INVALID, "null view output");
    *outView = allocate!af_view();
    return *outView is null ? setError(AF_ALLOCATION_FAILED, "view handle allocation failed") : setError(AF_OK);
}
uint af_subscribe(af_view* v, af_farm* f)
{
    if (v is null || f is null || v.value.hasRef) return setError(AF_INVALID, "invalid view or Farm");
    immutable result = v.value.subscribe(f.impl);
    if (result == SUBSCRIBE_FULL) return setError(AF_FULL, "consumer capacity full");
    if (result == SUBSCRIBE_RETRY) return setError(AF_RETRY, "subscribe frontier changed; retry");
    if (result < 0) return setError(AF_INVALID, "invalid subscription");
    v.owner = f;
    atomicOp!"+="(f.views, 1u);
    return setError(AF_OK);
}
uint af_consume_next(af_view* v)
{
    requireWalk();
    if (v is null) fatal("C ABI null view");
    return v.value.consumeNext();
}
void af_unsubscribe(af_view* v)
{
    requireWalk();
    if (v is null) fatal("C ABI null view");
    if (!v.value.hasRef) return;
    v.value.unsubscribe();
    atomicOp!"-="(v.owner.views, 1u);
    v.owner = null;
}
void af_view_destroy(af_view* v)
{
    if (v is null) return;
    if (v.value.hasRef) fatal("C ABI destroy subscribed view");
    free(v);
}
const(char)* af_last_error_message() { return diagnostic.ptr; }
int af_last_os_error() { return osDiagnostic; }
void af_set_fatal_handler(FatalHandler handler, void* context) { setFatalHandler(handler, context); }
