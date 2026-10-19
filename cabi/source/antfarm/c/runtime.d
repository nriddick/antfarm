// Optional runtime distribution. Never compile antfarm/package.d into this archive.
module antfarm.c.runtime;
import antfarm : AntFarm, fatal, inFarmCallback, DEFAULT_SMALL_TABLE_THRESHOLD, Token;
import antfarm.c.core;
import antfarm.actors.cabi;
import core.atomic : atomicLoad, atomicOp;
import core.memory : GC;
import core.runtime : Runtime;
import core.stdc.stdlib : malloc, free;
import core.thread : Thread, thread_attachThis, thread_detachThis;
import core.time : Duration, dur;
import threadpool.host : FarmHost, FarmHostOptions, FarmHostResolveException,
    CallerPlacement, WorkerIdle, HostState, HostAttachment;
import threadpool.pool : CacheAwarePool;

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
}
static assert(af_host_options.sizeof == 88 && af_host_options.idle_timeout_ns.offsetof == 24);
struct af_host { FarmHost value; af_farm* farm; af_actor_runtime* actors; }
private __gshared bool active, attachedOwner, retired;
private __gshared Thread owner;
private __gshared uint hosts;
private shared uint attachments;
private bool attachedHere, ownsAttachment;

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
    return AF_OK;
}
private uint exception(Throwable e) nothrow @nogc @system
{
    return setError(cast(FarmHostResolveException) e !is null ? AF_INVALID : AF_RUNTIME_ERROR, e.msg);
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

extern(C) nothrow @system:
uint af_runtime_init()
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
uint af_runtime_shutdown()
{
    if (auto status = control()) return status;
    if (hosts || atomicLoad(attachments)) return setError(AF_BUSY, "runtime still has hosts or thread attachments");
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
uint af_runtime_attach_thread()
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
uint af_runtime_detach_thread()
{
    if (!active || !attachedHere) return setError(AF_INVALID, "no ABI thread attachment");
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
        1_000_000, 8, 8, 1, 0, 0, 4096, 0, DEFAULT_SMALL_TABLE_THRESHOLD, 0, 1, 0);
}
uint af_host_create(const(af_host_options)* supplied, af_host** outHost)
{
    if (outHost is null) return setError(AF_INVALID, "null host output");
    *outHost = null;
    if (auto status = control()) return status;
    af_host_options defaults;
    if (supplied is null) { af_host_options_default(&defaults); supplied = &defaults; }
    if (supplied.struct_size != af_host_options.sizeof) return setError(AF_INVALID, "invalid host options size");
    const o = *supplied;
    if (o.abi_version != AF_ABI_VERSION || o.reserved || o.caller_consumes > 1 ||
        o.caller_placement > 2 || o.idle > 1 || o.huge_pages > 1 || o.skip_smt_siblings > 1 ||
        (o.idle_timeout_ns != ulong.max && o.idle_timeout_ns > long.max))
        return setError(AF_INVALID, "invalid host options");
    auto h = cast(af_host*) malloc(af_host.sizeof);
    if (h is null) return setError(AF_ALLOCATION_FAILED, "host handle allocation failed");
    *h = af_host.init;
    GC.addRange(h, af_host.sizeof); // C ownership must keep the D host GC-reachable.
    try
    {
        FarmHostOptions options;
        options.workers = o.workers;
        options.callerConsumes = o.caller_consumes != 0;
        options.caller = o.caller_placement == 0 ? CallerPlacement.unchanged :
            o.caller_placement == 1 ? CallerPlacement.confine : CallerPlacement.pin;
        options.idle = cast(WorkerIdle) o.idle;
        options.idleTimeout = o.idle_timeout_ns == ulong.max ? Duration.max : dur!"nsecs"(cast(long) o.idle_timeout_ns);
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
    if (auto status = attached()) return status;
    if (h is null) return setError(AF_INVALID, "null host");
    h.value.wake();
    return setError(AF_OK);
}
uint af_host_state(af_host* h) @nogc
{
    if (attached() != AF_OK || h is null) fatal("invalid C host state query");
    return cast(uint) h.value.state;
}
uint af_host_close(af_host* h)
{
    if (auto status = hostControl(h)) return status;
    if (hasParticipants(h.farm)) return setError(AF_BUSY, "host has external producers or views");
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
