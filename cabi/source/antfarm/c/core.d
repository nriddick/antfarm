// C ABI 2, a superset of ABI 1. Built once with BetterC; the optional runtime
// links this same core.
module antfarm.c.core;
import antfarm;
import core.atomic : atomicOp, atomicLoad, MemoryOrder;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;

enum uint AF_ABI_VERSION = 2, AF_ABI_VERSION_MIN = 1;
enum uint AF_COST_AUTO = uint.max;
enum uint AF_FEATURE_RUNTIME = 1, AF_FEATURE_IO_URING = 2, AF_FEATURE_MIMALLOC = 4, AF_FEATURE_TSAN = 8;
/// What af_build_info reports; the CHANGELOG's development series.
enum string AF_LIBRARY_VERSION = "2.2.1-dev";
enum uint AF_OK = 0, AF_INVALID = 1, AF_FULL = 2, AF_RETRY = 3,
    AF_ALLOCATION_FAILED = 4, AF_SYSTEM_ERROR = 5, AF_BUSY = 6, AF_RUNTIME_ERROR = 7;
/// ABI 2 accepts every option layout from ABI 1 on; the structure's own
/// version says which one the caller filled. Shared by every wrapper.
bool abiVersionOk(uint abiVersion) nothrow @nogc @safe
{
    return abiVersion >= AF_ABI_VERSION_MIN && abiVersion <= AF_ABI_VERSION;
}

struct af_farm
{
    AntFarm* impl;
    bool owned;
    shared uint tokens, views, actorRuntimes;
    shared size_t waves, privateParks;
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
struct af_build_info
{
    uint struct_size, abi_version;
    const(char)* library_version;
    const(char)* compiler;
    uint features, reserved;
}
static assert(af_build_info.sizeof == 32 && af_build_info.features.offsetof == 24);
// The runtime archive reports itself at af_druntime_init; the BetterC core has
// no other way to see it.
private __gshared bool runtimeLinked;
void markRuntimeLinked() nothrow @nogc @system { runtimeLinked = true; }
private static immutable string libraryVersion = AF_LIBRARY_VERSION;
// "LDC 2.112" or "Digital Mars D 2.112": the vendor and the front-end version,
// built at compile time without the runtime.
private char[32] compilerTextOf() pure nothrow @nogc @safe
{
    char[32] text = 0;
    size_t n;
    foreach (c; __VENDOR__) text[n++] = c;
    text[n++] = ' ';
    enum uint v = __VERSION__;
    text[n++] = cast(char) ('0' + v / 1000);
    text[n++] = '.';
    text[n++] = cast(char) ('0' + v / 100 % 10);
    text[n++] = cast(char) ('0' + v / 10 % 10);
    text[n++] = cast(char) ('0' + v % 10);
    return text;
}
private static immutable char[32] compilerText = compilerTextOf();
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
    if (cost > MAX_AVG_COST && cost != AF_COST_AUTO) fatal("avgCost out of range");
    if (hook !is null && hook.call is null) fatal("table completion hook requires a callback");
}
// AF_COST_AUTO: the class the private root pump would derive for this table.
private uint resolveCost(af_farm* f, uint cost, size_t count) nothrow @nogc @system
{
    if (cost != AF_COST_AUTO) return cost;
    return rootCostHintFor(count, cast(ulong) atomicLoad!(MemoryOrder.raw)(f.impl.Cf),
        f.impl.smallThreshold);
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
    if (!abiVersionOk(options.abi_version) || options.reserved || options.huge_pages > 1)
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
    if (atomicLoad(f.privateParks)) fatal("C ABI destroy Farm with private parks");
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
    return f.impl.write(Entries(entries, n), t.value, resolveCost(f, cost, n), hook);
}
size_t af_write_bodies(af_farm* f, Callback call, const(af_body)* bodies, size_t n,
    af_token* t, uint cost, const(TableCompletionHook)* hook)
{
    requireWrite(f, t, cost, hook);
    if (call is null) fatal("null payload callback");
    span(bodies, n);
    return f.impl.write(call, Bodies(bodies, n), t.value, resolveCost(f, cost, n), hook);
}
size_t af_write_fixed(af_farm* f, Callback call, const(ulong)* bodies, size_t width,
    size_t n, af_token* t, uint cost, const(TableCompletionHook)* hook)
{
    requireWrite(f, t, cost, hook);
    if (call is null) fatal("null payload callback");
    if (width && n > size_t.max / width) fatal("C ABI fixed body size overflow");
    span(bodies, width * n);
    return f.impl.write(call, FixedBodies(bodies, width, n), width, t.value,
        resolveCost(f, cost, n), hook);
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
    immutable result = v.value.subscribe(f.impl); // retries the frontier race; AF_RETRY stays registered, unused
    if (result == SUBSCRIBE_FULL) return setError(AF_FULL, "consumer capacity full");
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
uint af_build_info_get(af_build_info* info)
{
    if (info is null) return setError(AF_INVALID, "null build info");
    if (info.struct_size != af_build_info.sizeof) return setError(AF_INVALID, "invalid build info size");
    if (!abiVersionOk(info.abi_version)) return setError(AF_INVALID, "invalid build info version");
    info.library_version = libraryVersion.ptr;
    info.compiler = compilerText.ptr;
    uint features;
    if (runtimeLinked) features |= AF_FEATURE_RUNTIME;
    version (AntfarmMimallocV3) features |= AF_FEATURE_MIMALLOC;
    version (TSan) features |= AF_FEATURE_TSAN;
    info.features = features;
    info.reserved = 0;
    return setError(AF_OK);
}
uint af_cost_for_chunk(uint chunk) { return avgCostForChunk(chunk); }
uint af_root_cost_hint(size_t count, ulong consumers, uint smallThreshold)
{
    return rootCostHintFor(count, consumers, smallThreshold);
}
uint af_used_large_pages(const(af_farm)* f)
{
    return f !is null && f.impl !is null && f.impl.usedLargePages;
}
