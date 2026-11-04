// Optional runtime distribution. Never compile antfarm/package.d into this archive.
module antfarm.c.runtime;
import antfarm : AntFarm, fatal, inFarmCallback, DEFAULT_SMALL_TABLE_THRESHOLD, Token, rootCostHintAuto;
import antfarm.c.core;
import antfarm.actors.cabi;
import antfarm.actors.private_cabi : af_private_roots, af_private_roots_live, pumpPark, destroyPark;
import antfarm.c.api_runtime : installRuntimeSections;
import core.atomic : atomicLoad, atomicOp;
import core.lifetime : move;
import core.memory : GC;
import core.runtime : Runtime;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy, strlen;
import core.thread : Thread, thread_attachThis, thread_detachThis;
import core.time : Duration, dur;
import threadpool.host : FarmHost, FarmHostOptions, FarmHostResolveException, FarmHostNotDrained,
    FarmHostTimeout, FarmHostFailed, HostGroups, CallerPlacement, WorkerIdle, HostState, HostAttachment;
import threadpool.pool : CacheAwarePool;

// GroupHost keeps GC arrays of AntFarm pointers. DMD emits their TypeInfo in
// the runtime archive but otherwise leaves the imported Farm's pointer bitmap
// to its defining module, built with BetterC here. Materialize that metadata
// here without compiling a second Farm implementation or identity allocator.
private immutable farmGcLayout = RTInfo!AntFarm;

extern(C) void rt_moduleTlsCtor();
extern(C) void rt_moduleTlsDtor();

struct af_host_options
{
    uint struct_size, abi_version;
    uint workers, caller_consumes, caller_placement, idle;
    ulong idle_timeout_ns;
    uint ring_mib, segments, extra_small_producers, bulk_producers;
    ulong bulk_quota_words, small_quota_words;
    uint extra_consumers, small_table_threshold, huge_pages, skip_smt_siblings;
    ulong reserved;
    // ABI 2
    ulong wait_timeout_ns;
    uint max_emit_rounds, group_mode, group_index, farm_per_group;
    ulong reserved2;
}
static assert(af_host_options.sizeof == 120 && af_host_options.idle_timeout_ns.offsetof == 24
    && af_host_options.wait_timeout_ns.offsetof == 88);
/// The ABI 1 image, the fields through reserved, accepted by its own size and
/// version; the ABI 2 fields then take their defaults.
struct af_host_options_v1
{
    uint struct_size, abi_version;
    uint workers, caller_consumes, caller_placement, idle;
    ulong idle_timeout_ns;
    uint ring_mib, segments, extra_small_producers, bulk_producers;
    ulong bulk_quota_words, small_quota_words;
    uint extra_consumers, small_table_threshold, huge_pages, skip_smt_siblings;
    ulong reserved;
}
static assert(af_host_options_v1.sizeof == 88);
struct af_processor { ushort group, processor, llc, reserved; }
enum uint AF_HOST_TIMED_OUT = 112, AF_HOST_WORKER_FAILED = 113;
extern(C) alias AttachmentEmit = size_t function(void*, af_token*) nothrow @nogc;
extern(C) alias AttachmentDrained = uint function(void*) nothrow @nogc;
extern(C) alias AttachmentFinish = void function(void*) nothrow @nogc;
extern(C) alias HostPredicate = uint function(void*) nothrow @nogc;
struct af_attachment
{
    uint struct_size, abi_version;
    const(char)* name;
    AttachmentEmit emit;
    AttachmentDrained drained;
    AttachmentFinish finish;
    void* context;
    ulong reserved;
}
static assert(af_attachment.sizeof == 56 && af_attachment.context.offsetof == 40);
// A C attachment, or a park transferred to the host. The host's control
// token is lent to the C side through `token` for each call and moved back
// after it (a Token cannot be copied), so its quota accounting follows the
// writes and the host never sees two live tokens.
private struct AttachmentRecord
{
    af_attachment spec;
    af_private_roots* park;
    af_token token;
    af_host* host;
    AttachmentRecord* next;
}
struct af_host
{
    FarmHost value;
    af_farm* farm;
    af_actor_runtime* actors;
    AttachmentRecord* attachments;
    uint attachedParks;
}
private __gshared bool active, attachedOwner, retired;
private __gshared Thread owner;
private __gshared uint hosts;
private shared uint attachments;
private shared uint services;
private uint callbackDepth, threadResources;
private bool attachedHere, ownsAttachment;

// Shared by the additive dispatcher and GroupHost adapters. Their native
// handles must keep the explicit runtime session alive, including completions
// created on foreign threads.
package uint runtimeAttached() nothrow @nogc { return attached(); }
package uint runtimeControl() nothrow @nogc { return control(); }
package uint runtimeException(Throwable e) nothrow @nogc { return exception(e); }
package void runtimeRetain() nothrow @nogc { atomicOp!"+="(services, 1u); }
package void runtimeRelease() nothrow @nogc { atomicOp!"-="(services, 1u); }
package void runtimeEnterCallback() nothrow @nogc { ++callbackDepth; }
package void runtimeLeaveCallback() nothrow @nogc { --callbackDepth; }
package void runtimeAcquireThreadResource() nothrow @nogc { ++threadResources; }
package void runtimeReleaseThreadResource() nothrow @nogc { --threadResources; }

private uint attached() nothrow @nogc @system
{
    if (!active) return setError(AF_INVALID, "runtime is not initialized");
    if (Thread.getThis() is null) return setError(AF_INVALID, "foreign runtime thread must attach");
    return AF_OK;
}
private uint control() nothrow @nogc @system
{
    if (auto status = attached()) return status;
    if (Thread.getThis() !is owner) return setError(AF_INVALID, "runtime operation requires the control thread");
    if (inFarmCallback()) fatal("C runtime control operation inside Farm callback");
    if (callbackDepth) fatal("C runtime control operation inside runtime callback");
    return AF_OK;
}
// ABI 2: a D Error never becomes a status. The scheduler or the host it
// came from is in no state to hand back to a caller; the handler sees the
// message, then abort follows.
private __gshared char[512] errorText;
private void abortOnError(Throwable e) nothrow @nogc @system
{
    enum prefix = "C ABI: D Error inside an export: ";
    size_t n;
    foreach (c; prefix) errorText[n++] = c;
    foreach (c; e.msg) { if (n == errorText.length) break; errorText[n++] = c; }
    fatal(errorText[0 .. n]);
}
private uint exception(Throwable e) nothrow @nogc @system
{
    if (cast(Error) e !is null) abortOnError(e);
    return setError(cast(FarmHostResolveException) e !is null ? AF_INVALID :
        cast(FarmHostNotDrained) e !is null ? AF_BUSY : AF_RUNTIME_ERROR, e.msg);
}
private void releaseHandle(af_host* h) nothrow @nogc @system
{
    for (auto r = h.actors; r !is null;)
    {
        auto next = r.next;
        // Abandon deliberately leaks actor storage and its allocator context.
        if (r.impl is null) free(r);
        r = next;
    }
    for (auto a = h.attachments; a !is null;)
    {
        auto next = a.next;
        free(a); // a park an abandoned host still holds stays leaked, as its roots do
        a = next;
    }
    releaseBorrowedFarm(h.farm);
    GC.removeRange(h);
    free(h);
    --hosts;
}
private uint hostControl(af_host* h) nothrow @nogc @system
{
    if (auto status = control()) return status;
    return h is null ? setError(AF_INVALID, "null host") : AF_OK;
}
private uint gcControl() nothrow @nogc @system
{
    if (auto status = attached()) return status;
    if (inFarmCallback()) fatal("C runtime GC control inside Farm callback");
    if (callbackDepth) fatal("C runtime GC control inside runtime callback");
    return AF_OK;
}
// Accept an option image by its own size and version: the ABI 1 layout (the
// fields through reserved) or the current one. Null selects the defaults.
// Shared with the group host, whose options embed this structure.
package uint hostOptionsFrom(const(void)* supplied, out af_host_options o) nothrow @nogc @system
{
    if (supplied is null)
        af_host_options_default(&o);
    else
    {
        // Read only the size and version before trusting the layout.
        auto head = cast(const(uint)*) supplied;
        if (head[1] == 1 && head[0] == af_host_options_v1.sizeof)
        {
            af_host_options_default(&o);
            memcpy(&o, supplied, af_host_options_v1.sizeof);
            o.struct_size = af_host_options.sizeof;
        }
        else if (head[1] == AF_ABI_VERSION && head[0] == af_host_options.sizeof)
            o = *cast(const(af_host_options)*) supplied;
        else
            return setError(AF_INVALID, "invalid host options size or version");
    }
    if (o.reserved || o.reserved2 || o.caller_consumes > 1 || o.caller_placement > 2 || o.idle > 1 ||
        o.huge_pages > 1 || o.skip_smt_siblings > 1 ||
        (o.idle_timeout_ns != ulong.max && o.idle_timeout_ns > long.max) ||
        (o.wait_timeout_ns != ulong.max && o.wait_timeout_ns > long.max) ||
        o.max_emit_rounds == 0 || o.group_mode > 2 || o.group_index >= ushort.max ||
        (o.group_mode != 2 && o.group_index != 0) || o.farm_per_group > 1)
        return setError(AF_INVALID, "invalid host options");
    return AF_OK;
}
// The plain host's C surface reaches one Farm, the caller group's, so one
// Farm per group would leave every other group's workers without work from
// C. Refused until a per-group surface exists; the group host has one.
// (The group host ignores its embedded host's farm_per_group.)
private uint plainHostOptions(ref const af_host_options o) nothrow @nogc @system
{
    if (o.farm_per_group != 0)
        return setError(AF_INVALID, "farm_per_group 1 is not available on the plain host; "
            ~ "use the group host (af_group_start) for one Farm per group");
    return AF_OK;
}
package void toFarmHostOptions(ref const af_host_options o, ref FarmHostOptions options) nothrow @system
{
    options.workers = o.workers;
    options.callerConsumes = o.caller_consumes != 0;
    options.caller = o.caller_placement == 0 ? CallerPlacement.unchanged :
        o.caller_placement == 1 ? CallerPlacement.confine : CallerPlacement.pin;
    options.idle = cast(WorkerIdle) o.idle;
    options.idleTimeout = o.idle_timeout_ns == ulong.max ? Duration.max : dur!"nsecs"(cast(long) o.idle_timeout_ns);
    options.waitTimeout = o.wait_timeout_ns == ulong.max ? Duration.max : dur!"nsecs"(cast(long) o.wait_timeout_ns);
    options.maxEmitRounds = o.max_emit_rounds;
    options.groups = o.group_mode == 0 ? HostGroups.automatic() : o.group_mode == 1 ? HostGroups.all()
        : HostGroups.only(cast(ushort) o.group_index);
    options.farmPerGroup = o.farm_per_group != 0;
    options.pool.skipSmtSiblings = o.skip_smt_siblings != 0;
    options.farm.ringMiB = o.ring_mib;
    options.farm.k = o.segments;
    options.farm.extraSmallProducers = o.extra_small_producers;
    options.farm.bulkProducers = o.bulk_producers;
    options.farm.quotaBulk = o.bulk_quota_words;
    options.farm.quotaSmall = o.small_quota_words;
    options.farm.extraConsumers = o.extra_consumers;
    options.farm.smallThreshold = o.small_table_threshold;
    options.farm.hugePages = o.huge_pages != 0;
}
private AttachmentRecord* newRecord(af_host* h) nothrow @nogc @system
{
    auto rec = cast(AttachmentRecord*) malloc(AttachmentRecord.sizeof);
    if (rec is null) return null;
    static immutable initial = AttachmentRecord.init; // a Token cannot be assigned
    memcpy(rec, &initial, AttachmentRecord.sizeof);
    rec.host = h;
    rec.token.owner = h.farm;
    return rec;
}
private size_t emitBridge(AttachmentRecord* rec, ref Token token) nothrow @system
{
    move(token, rec.token.value);
    scope(exit) move(rec.token.value, token);
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    return rec.spec.emit(rec.spec.context, &rec.token);
}
private bool drainedBridge(AttachmentRecord* rec) nothrow @system
{
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    return rec.spec.drained(rec.spec.context) != 0;
}
private void finishBridge(AttachmentRecord* rec) nothrow @system
{
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    rec.spec.finish(rec.spec.context);
}
private size_t parkPump(AttachmentRecord* rec, ref Token token) nothrow @nogc @system
{
    move(token, rec.token.value);
    scope(exit) move(rec.token.value, token);
    return pumpPark(rec.park, &rec.token, 0, rootCostHintAuto).tables;
}
private void parkFinish(AttachmentRecord* rec) nothrow @nogc @system
{
    destroyPark(rec.park);
    rec.park = null;
    --rec.host.attachedParks;
}

extern(C) nothrow @system:
uint af_druntime_init()
{
    if (active) return setError(AF_BUSY, "runtime session already initialized");
    if (retired) return setError(AF_INVALID, "runtime session cannot restart after teardown");
    bool acquired, tls;
    try
    {
        if (!Runtime.initialize()) return setError(AF_RUNTIME_ERROR, "druntime initialization failed");
        acquired = true;
        if (Thread.getThis() is null)
        {
            thread_attachThis();
            attachedOwner = true;
            rt_moduleTlsCtor();
            tls = true;
        }
        // Discovery is an unlocked setup operation in the existing pool.
        // Warm it here, before any C host or foreign runtime caller can enter.
        CacheAwarePool.topology();
        owner = Thread.getThis();
        active = true;
        markRuntimeLinked();
        installRuntimeSections();
        return setError(AF_OK);
    }
    catch (Throwable e)
    {
        immutable status = exception(e);
        if (attachedOwner)
        {
            if (tls) { try rt_moduleTlsDtor(); catch (Throwable) {} }
            thread_detachThis(); attachedOwner = false;
        }
        if (acquired) { retired = true; try Runtime.terminate(); catch (Throwable) {} }
        return status;
    }
}
uint af_druntime_shutdown()
{
    if (auto status = control()) return status;
    if (hosts || atomicLoad(attachments) || atomicLoad(services))
        return setError(AF_BUSY, "runtime still has hosts, services or thread attachments");
    active = false;
    retired = true;
    owner = null;
    bool tlsFailed;
    if (attachedOwner)
    {
        try rt_moduleTlsDtor(); catch (Throwable e) { exception(e); tlsFailed = true; }
        thread_detachThis(); attachedOwner = false;
    }
    try
    {
        if (!Runtime.terminate()) return setError(AF_RUNTIME_ERROR, "druntime termination failed");
        return tlsFailed ? AF_RUNTIME_ERROR : setError(AF_OK);
    }
    catch (Throwable e) { return exception(e); }
}
uint af_druntime_attach_thread()
{
    if (!active) return setError(AF_INVALID, "runtime is not initialized");
    if (attachedHere) return setError(AF_BUSY, "thread already has an ABI attachment");
    bool added;
    try
    {
        if (Thread.getThis() is null)
        {
            thread_attachThis(); added = true;
            rt_moduleTlsCtor();
        }
        ownsAttachment = added;
        attachedHere = true;
        atomicOp!"+="(attachments, 1u);
        return setError(AF_OK);
    }
    catch (Throwable e)
    {
        immutable status = exception(e);
        if (added) thread_detachThis();
        return status;
    }
}
uint af_druntime_detach_thread()
{
    if (!active || !attachedHere) return setError(AF_INVALID, "no ABI thread attachment");
    if (callbackDepth || threadResources) return setError(AF_BUSY, "thread still has runtime callbacks or tokens");
    uint result = AF_OK;
    if (ownsAttachment)
    {
        try rt_moduleTlsDtor(); catch (Throwable e) { result = exception(e); }
        thread_detachThis();
    }
    attachedHere = ownsAttachment = false;
    atomicOp!"-="(attachments, 1u);
    return result == AF_OK ? setError(AF_OK) : result;
}
void af_host_options_default(af_host_options* o) @nogc
{
    if (o is null) fatal("C ABI null host options output");
    *o = af_host_options(af_host_options.sizeof, AF_ABI_VERSION, uint.max, 1, 0, 0,
        1_000_000, 8, 8, 1, 0, 0, 4096, 0, DEFAULT_SMALL_TABLE_THRESHOLD, 0, 1, 0,
        ulong.max, 16, 0, 0, 0, 0);
}
uint af_host_create(const(af_host_options)* supplied, af_host** outHost)
{
    if (outHost is null) return setError(AF_INVALID, "null host output");
    *outHost = null;
    if (auto status = control()) return status;
    af_host_options o;
    if (auto status = hostOptionsFrom(supplied, o)) return status;
    if (auto status = plainHostOptions(o)) return status;
    auto h = cast(af_host*) malloc(af_host.sizeof);
    if (h is null) return setError(AF_ALLOCATION_FAILED, "host handle allocation failed");
    *h = af_host.init;
    GC.addRange(h, af_host.sizeof); // C ownership must keep the D host GC-reachable.
    try
    {
        FarmHostOptions options;
        toFarmHostOptions(o, options);
        h.value = new FarmHost(options);
        h.farm = wrapFarm(h.value.farm, false);
        if (h.farm is null)
        {
            h.value.closeOrAbandon();
            GC.removeRange(h); free(h);
            return setError(AF_ALLOCATION_FAILED, "borrowed Farm handle allocation failed");
        }
        ++hosts;
        *outHost = h;
        return setError(AF_OK);
    }
    catch (Throwable e)
    {
        immutable status = exception(e);
        if (h.value !is null) { try h.value.closeOrAbandon(); catch (Throwable) {} }
        if (h.farm !is null) releaseBorrowedFarm(h.farm);
        GC.removeRange(h); free(h);
        return status;
    }
}
af_farm* af_host_farm(af_host* h) @nogc
{
    if (hostControl(h) != AF_OK) return null;
    if (h.value.state != HostState.open) { setError(AF_INVALID, "host is not open"); return null; }
    return h.farm;
}
uint af_host_register_producer(af_host* h, uint tier, af_token** outToken) @nogc
{
    if (outToken is null) return setError(AF_INVALID, "null producer output");
    *outToken = null;
    if (auto status = hostControl(h)) return status;
    if (h.value.state != HostState.open) return setError(AF_INVALID, "host is not open");
    return af_register_producer(h.farm, tier, outToken);
}
uint af_host_create_actors(af_host* h, size_t capacity, const(af_actor_allocator)* allocator,
    af_actor_runtime** output)
{
    if (output is null) return setError(AF_INVALID, "null actor runtime output");
    *output = null;
    if (auto status = hostControl(h)) return status;
    if (h.value.state != HostState.open) return setError(AF_INVALID, "host is not open");
    af_actor_runtime* r;
    if (auto status = createActorRuntime(h.farm, capacity, allocator, false, &r)) return status;
    try
    {
        h.value.attach(HostAttachment("C actors",
            (ref Token token) => r.impl.flush(token),
            () => actorRuntimeDrained(r),
            () { finishActorRuntime(r); }));
    }
    catch (Throwable e)
    {
        finishActorRuntime(r);
        free(r);
        return exception(e);
    }
    r.next = h.actors;
    h.actors = r;
    *output = r;
    return setError(AF_OK);
}
uint af_host_step(af_host* h)
{
    if (auto status = hostControl(h)) return status;
    try { h.value.step(); return setError(AF_OK); }
    catch (Throwable e) { return exception(e); }
}
uint af_host_wake(af_host* h) @nogc
{
    if (h is null) return setError(AF_INVALID, "null host");
    h.value.wake();
    return setError(AF_OK);
}
uint af_host_state(const(af_host)* h, uint* output) @nogc
{
    if (h is null || output is null) return setError(AF_INVALID, "null host or state output");
    *output = cast(uint) h.value.state;
    return setError(AF_OK);
}
uint af_host_close(af_host* h)
{
    if (auto status = hostControl(h)) return status;
    if (hasParticipants(h.farm)) return setError(AF_BUSY, "host has external producers or views");
    if (atomicLoad(h.farm.waves)) return setError(AF_BUSY, "host has actor waves");
    if (atomicLoad(h.farm.privateParks) > h.attachedParks) return setError(AF_BUSY, "host has private parks");
    if (h.value.state == HostState.open)
        for (auto r = h.actors; r !is null; r = r.next)
            if (!actorRuntimeDrained(r)) return setError(AF_BUSY, "host has live or queued actors");
    try { h.value.close(); releaseHandle(h); return setError(AF_OK); }
    catch (Throwable e) { return exception(e); }
}
uint af_host_abandon(af_host* h)
{
    if (auto status = hostControl(h)) return status;
    if (hasParticipants(h.farm)) return setError(AF_BUSY, "host has external producers or views");
    try { h.value.abandon(); releaseHandle(h); return setError(AF_OK); }
    catch (Throwable e) { return exception(e); }
}

// ---- ABI 2 ------------------------------------------------------------------
uint af_host_attach(af_host* h, const(af_attachment)* a)
{
    if (auto status = hostControl(h)) return status;
    if (a is null) return setError(AF_INVALID, "null attachment");
    if (a.struct_size != af_attachment.sizeof || !abiVersionOk(a.abi_version) || a.reserved)
        return setError(AF_INVALID, "invalid attachment layout");
    if (a.emit is null && a.drained is null && a.finish is null)
        return setError(AF_INVALID, "attachment needs at least one callback");
    if (h.value.state != HostState.open) return setError(AF_INVALID, "host is not open");
    auto rec = newRecord(h);
    if (rec is null) return setError(AF_ALLOCATION_FAILED, "attachment record allocation failed");
    memcpy(&rec.spec, a, af_attachment.sizeof);
    try
    {
        immutable name = a.name is null ? "C attachment" : a.name[0 .. strlen(a.name)].idup;
        h.value.attach(HostAttachment(name,
            a.emit is null ? null : (ref Token token) => emitBridge(rec, token),
            a.drained is null ? null : () => drainedBridge(rec),
            a.finish is null ? null : () => finishBridge(rec)));
    }
    catch (Throwable e) { free(rec); return exception(e); }
    rec.next = h.attachments;
    h.attachments = rec;
    return setError(AF_OK);
}
uint af_host_attach_park(af_host* h, af_private_roots* park)
{
    if (auto status = hostControl(h)) return status;
    if (park is null) return setError(AF_INVALID, "null private park");
    if (park.farm !is h.farm) return setError(AF_INVALID, "park was not created on this host's Farm");
    // One owner: a second transfer would count the park twice against the
    // close check and destroy it twice.
    if (park.hostOwned) return setError(AF_INVALID, "park was already transferred to a host");
    if (h.value.state != HostState.open) return setError(AF_INVALID, "host is not open");
    auto rec = newRecord(h);
    if (rec is null) return setError(AF_ALLOCATION_FAILED, "park record allocation failed");
    rec.park = park;
    try
    {
        h.value.attach(HostAttachment("C park",
            (ref Token token) => parkPump(rec, token),
            () => af_private_roots_live(rec.park) == 0,
            () => parkFinish(rec)));
    }
    catch (Throwable e) { free(rec); return exception(e); }
    park.hostOwned = true;
    ++h.attachedParks;
    rec.next = h.attachments;
    h.attachments = rec;
    return setError(AF_OK);
}
uint af_host_wait_until(af_host* h, HostPredicate done, void* context, ulong timeoutNs)
{
    if (auto status = hostControl(h)) return status;
    if (done is null) return setError(AF_INVALID, "null wait predicate");
    if (timeoutNs != ulong.max && timeoutNs > long.max) return setError(AF_INVALID, "invalid wait timeout");
    bool predicate() nothrow
    {
        runtimeEnterCallback();
        scope(exit) runtimeLeaveCallback();
        return done(context) != 0;
    }
    try
    {
        if (timeoutNs == ulong.max) h.value.waitUntil(&predicate);
        else h.value.waitUntil(&predicate, dur!"nsecs"(cast(long) timeoutNs));
        return setError(AF_OK);
    }
    catch (FarmHostTimeout e) { return setError(AF_HOST_TIMED_OUT, e.msg); }
    catch (FarmHostFailed e) { return setError(AF_HOST_WORKER_FAILED, e.msg); }
    catch (Throwable e) { return exception(e); }
}
uint af_host_resolve(const(af_host_options)* supplied, af_processor* output, size_t capacity, size_t* count)
{
    if (count is null) return setError(AF_INVALID, "null host resolution count");
    *count = 0;
    if (auto status = control()) return status;
    if (output is null && capacity) return setError(AF_INVALID, "null host resolution output");
    af_host_options o;
    if (auto status = hostOptionsFrom(supplied, o)) return status;
    if (auto status = plainHostOptions(o)) return status;
    try
    {
        FarmHostOptions options;
        toFarmHostOptions(o, options);
        auto res = FarmHost.resolve(options);
        *count = res.workers.length;
        if (output is null && capacity == 0) return setError(AF_OK);
        if (capacity < *count) return setError(AF_FULL, "host resolution output too small");
        auto topo = CacheAwarePool.topology();
        foreach (i, id; res.workers) output[i] = af_processor(id.group, id.lp, topo.byId(id).llcIndex, 0);
        return setError(AF_OK);
    }
    catch (Throwable e) { return exception(e); }
}
uint af_host_summary(const(af_host)* h, char* output, size_t capacity, size_t* len)
{
    if (len is null) return setError(AF_INVALID, "null summary length");
    *len = 0;
    if (auto status = attached()) return status;
    if (h is null || (output is null && capacity)) return setError(AF_INVALID, "null host or summary output");
    try
    {
        auto text = h.value.summary();
        *len = text.length;
        if (capacity < text.length + 1) return setError(AF_FULL, "summary output too small");
        memcpy(output, text.ptr, text.length);
        output[text.length] = 0;
        return setError(AF_OK);
    }
    catch (Throwable e) { return exception(e); }
}
uint af_druntime_gc_collect()
{
    if (auto status = gcControl()) return status;
    GC.collect();
    return setError(AF_OK);
}
uint af_druntime_gc_minimize()
{
    if (auto status = gcControl()) return status;
    GC.minimize();
    return setError(AF_OK);
}
uint af_druntime_gc_enable()
{
    if (auto status = gcControl()) return status;
    GC.enable();
    return setError(AF_OK);
}
uint af_druntime_gc_disable()
{
    if (auto status = gcControl()) return status;
    GC.disable();
    return setError(AF_OK);
}
