// SPDX-License-Identifier: MIT
module antfarm.c.groups;
import antfarm : fatal;
import core.thread : Thread;
import antfarm.c.core;
import antfarm.c.runtime;
import antfarm.c.forkjoin : af_processor;
import core.atomic;
import core.memory : GC;
import core.stdc.stdlib : malloc, free;
import core.time : Duration, dur;
import threadpool.groups : GroupHost, GroupHostOptions, groupFarmOf;
import threadpool.host : HostGroups, CallerPlacement, WorkerIdle;
import threadpool.hybrid : WorkerSelf;
import threadpool.pool : CacheAwarePool;

struct af_group_options
{
    uint struct_size, abi_version;
    af_host_options host;
    uint group_mode, group_index, farm_per_group, reserved;
}
static assert(af_group_options.sizeof == 112);
extern(C) alias GroupBody = uint function(af_group_host*, uint, uint, void*) nothrow @nogc;
extern(C) alias GroupStop = void function(af_group_host*, uint, uint, void*) nothrow @nogc;
private struct FarmLink { af_farm* farm; FarmLink* next; }
private struct TokenLink { af_token* token; TokenLink* next; }
struct af_group_host
{
    GroupHost value;
    GroupBody body;
    GroupStop stop;
    void* context;
    FarmLink* farms;
    shared bool lock;
    bool stopped;
}
private __gshared af_group_host* current;
private TokenLink* tokens;

private uint options(const(af_group_options)* supplied, out GroupHostOptions opt) nothrow
{
    af_group_options defaults;
    if (supplied is null) { af_group_options_default(&defaults); supplied = &defaults; }
    if (supplied.struct_size != af_group_options.sizeof) return setError(AF_INVALID, "invalid group options size");
    const o = *supplied;
    const h = o.host;
    if (o.abi_version != AF_ABI_VERSION || o.reserved || o.farm_per_group > 1 || o.group_mode > 2 ||
        o.group_index >= ushort.max || (o.group_mode != 2 && o.group_index != 0) ||
        h.struct_size != af_host_options.sizeof || h.abi_version != AF_ABI_VERSION || h.reserved ||
        h.caller_consumes > 1 || h.caller_placement > 2 || h.idle > 1 || h.huge_pages > 1 ||
        h.skip_smt_siblings > 1 || (h.idle_timeout_ns != ulong.max && h.idle_timeout_ns > long.max))
        return setError(AF_INVALID, "invalid group or host options");
    opt = GroupHostOptions.init;
    opt.workers = h.workers;
    opt.groups = o.group_mode == 0 ? HostGroups.automatic() : o.group_mode == 1 ? HostGroups.all() : HostGroups.only(cast(ushort)o.group_index);
    opt.caller = h.caller_placement == 0 ? CallerPlacement.unchanged : h.caller_placement == 1 ? CallerPlacement.confine : CallerPlacement.pin;
    opt.callerConsumes = h.caller_consumes != 0;
    opt.idle = cast(WorkerIdle)h.idle;
    opt.idleTimeout = h.idle_timeout_ns == ulong.max ? Duration.max : dur!"nsecs"(cast(long)h.idle_timeout_ns);
    opt.pool.skipSmtSiblings = h.skip_smt_siblings != 0;
    opt.farm.ringMiB = h.ring_mib; opt.farm.k = h.segments;
    opt.farm.extraSmallProducers = h.extra_small_producers; opt.farm.bulkProducers = h.bulk_producers;
    opt.farm.quotaBulk = h.bulk_quota_words; opt.farm.quotaSmall = h.small_quota_words;
    opt.farm.extraConsumers = h.extra_consumers; opt.farm.smallThreshold = h.small_table_threshold;
    opt.farm.hugePages = h.huge_pages != 0;
    opt.farmPerGroup = o.farm_per_group != 0;
    return AF_OK;
}
private uint valid(af_group_host* h) nothrow @nogc
{
    if (auto s = runtimeAttached()) return s;
    if (h is null || h !is current) return setError(AF_INVALID, "invalid group host");
    return AF_OK;
}
private void releaseTokens() nothrow @nogc
{
    if (tokens is null) return;
    while (tokens !is null)
    {
        auto t = tokens; tokens = t.next;
        af_unregister_producer(t.token.owner, t.token);
        free(t);
    }
    runtimeReleaseThreadResource();
}
private bool bodyBridge(WorkerSelf* w) nothrow @nogc
{
    auto h = current;
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    auto result = h.body(h, w.workerIndex, w.llcIndex, h.context);
    if (result > 1) fatal("C GroupHost: worker body must return 0 or 1");
    return result != 0;
}
private void stopBridge(WorkerSelf* w) nothrow @nogc
{
    auto h = current;
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    if (h.stop !is null) h.stop(h, w.workerIndex, w.llcIndex, h.context);
    releaseTokens();
}
private void releaseFarms(af_group_host* h) nothrow @nogc
{
    while (h.farms !is null)
    {
        auto f = h.farms; h.farms = f.next;
        releaseBorrowedFarm(f.farm); free(f);
    }
}
extern(C) void af_group_options_default(af_group_options* outOptions) nothrow @nogc
{
    if (outOptions is null) fatal("C GroupHost: null options output");
    *outOptions = af_group_options.init;
    outOptions.struct_size = af_group_options.sizeof;
    outOptions.abi_version = AF_ABI_VERSION;
    af_host_options_default(&outOptions.host);
    outOptions.host.caller_consumes = 0;
    outOptions.host.caller_placement = 1;
    outOptions.farm_per_group = 1;
}
extern(C) uint af_group_resolve(const(af_group_options)* supplied, af_processor* output, size_t capacity, size_t* count) nothrow
{
    if (count is null) return setError(AF_INVALID, "null group resolution count");
    *count = 0;
    if (auto s = runtimeControl()) return s;
    if (output is null && capacity) return setError(AF_INVALID, "null group resolution output");
    GroupHostOptions opt;
    if (auto s = options(supplied, opt)) return s;
    try
    {
        auto res = GroupHost.resolve(opt);
        *count = res.workers.length;
        if (output is null && capacity == 0) return setError(AF_OK);
        if (capacity < *count) return setError(AF_FULL, "group resolution output too small");
        auto topo = CacheAwarePool.topology();
        foreach (i, id; res.workers) output[i] = af_processor(id.group, id.lp, topo.byId(id).llcIndex, 0);
        return setError(AF_OK);
    }
    catch (Throwable e) { return runtimeException(e); }
}
extern(C) uint af_group_start(const(af_group_options)* supplied, GroupBody body, GroupStop stop, void* context, af_group_host** output) nothrow
{
    if (output is null) return setError(AF_INVALID, "null group host output");
    *output = null;
    if (auto s = runtimeControl()) return s;
    if (current !is null) return setError(AF_BUSY, "a C GroupHost is already open");
    if (body is null) return setError(AF_INVALID, "null worker body");
    GroupHostOptions opt;
    if (auto s = options(supplied, opt)) return s;
    auto h = cast(af_group_host*) malloc(af_group_host.sizeof);
    if (h is null) return setError(AF_ALLOCATION_FAILED, "group handle allocation failed");
    *h = af_group_host.init;
    h.body = body; h.stop = stop; h.context = context;
    GC.addRange(h, af_group_host.sizeof);
    // Workers can run before the constructor returns. Their bridge only uses
    // immutable callback fields and the already-installed GroupSlot registry.
    current = h;
    try
    {
        h.value = new GroupHost(opt, &bodyBridge, &stopBridge);
        runtimeRetain(); *output = h;
        return setError(AF_OK);
    }
    catch (Throwable e)
    {
        releaseFarms(h); current = null; GC.removeRange(h); free(h);
        return runtimeException(e);
    }
}
extern(C) uint af_group_caller_llc(af_group_host* h, uint* output) nothrow @nogc
{
    if (auto s = runtimeControl()) return s;
    if (auto s = valid(h)) return s;
    if (output is null || h.value is null) return setError(AF_INVALID, "null caller LLC output");
    *output = h.value.callerGroup;
    return setError(AF_OK);
}
extern(C) uint af_group_farm(af_group_host* h, uint llc, af_farm** output) nothrow @nogc
{
    if (output is null) return setError(AF_INVALID, "null group Farm output");
    *output = null;
    if (auto s = valid(h)) return s;
    if (llc >= ushort.max) return setError(AF_INVALID, "invalid LLC index");
    auto farm = groupFarmOf(cast(ushort)llc);
    if (farm is null) return setError(AF_INVALID, "unselected LLC group");
    while (!cas(&h.lock, false, true)) Thread.yield();
    scope(exit) atomicStore!(MemoryOrder.rel)(h.lock, false);
    for (auto f = h.farms; f !is null; f = f.next)
        if (f.farm.impl is farm) { *output = f.farm; return setError(AF_OK); }
    auto link = cast(FarmLink*)malloc(FarmLink.sizeof);
    if (link is null) return setError(AF_ALLOCATION_FAILED, "Farm wrapper allocation failed");
    link.farm = wrapFarm(farm, false);
    if (link.farm is null) { free(link); return setError(AF_ALLOCATION_FAILED, "Farm wrapper allocation failed"); }
    link.next = h.farms; h.farms = link; *output = link.farm;
    return setError(AF_OK);
}
extern(C) uint af_group_token(af_group_host* h, uint llc, af_token** output) nothrow @nogc
{
    if (output is null) return setError(AF_INVALID, "null group token output");
    *output = null;
    af_farm* f;
    if (auto s = af_group_farm(h, llc, &f)) return s;
    for (auto t = tokens; t !is null; t = t.next)
        if (t.token.owner is f) { *output = t.token; return setError(AF_OK); }
    auto link = cast(TokenLink*)malloc(TokenLink.sizeof);
    if (link is null) return setError(AF_ALLOCATION_FAILED, "token cache allocation failed");
    if (auto s = af_register_producer(f, 0, &link.token)) { free(link); return s; }
    if (tokens is null) runtimeAcquireThreadResource();
    link.next = tokens; tokens = link; *output = link.token;
    return setError(AF_OK);
}
extern(C) uint af_group_release_tokens(af_group_host* h) nothrow @nogc
{
    if (auto s = valid(h)) return s;
    releaseTokens(); return setError(AF_OK);
}
extern(C) uint af_group_wake(af_group_host* h, uint llc) nothrow @nogc
{
    if (h is null || h !is current || h.value is null || h.stopped) return setError(AF_INVALID, "group workers are not running");
    if (llc != uint.max && (llc >= ushort.max || groupFarmOf(cast(ushort)llc) is null))
        return setError(AF_INVALID, "invalid or unselected wake group");
    atomicFence!(MemoryOrder.seq)(); // publish before the pool's armed-word read
    if (llc == uint.max) h.value.wakeAll();
    else h.value.wakeLlc(cast(ushort)llc);
    return setError(AF_OK);
}
extern(C) uint af_group_stop_workers(af_group_host* h) nothrow
{
    if (auto s = runtimeControl()) return s;
    if (auto s = valid(h)) return s;
    if (h.stopped) return setError(AF_OK);
    try { h.value.stopWorkers(); h.stopped = true; return setError(AF_OK); }
    catch (Throwable e) { return runtimeException(e); }
}
extern(C) uint af_group_close(af_group_host* h) nothrow
{
    if (auto s = runtimeControl()) return s;
    if (auto s = valid(h)) return s;
    if (!h.stopped) return setError(AF_BUSY, "stop workers before closing GroupHost");
    releaseTokens();
    for (auto f = h.farms; f !is null; f = f.next)
        if (hasParticipants(f.farm) || atomicLoad(f.farm.actorRuntimes) || atomicLoad(f.farm.waves) || atomicLoad(f.farm.privateParks))
            return setError(AF_BUSY, "GroupHost still has borrowed resources");
    try
    {
        h.value.close(); releaseFarms(h); current = null;
        GC.removeRange(h); free(h); runtimeRelease();
        return setError(AF_OK);
    }
    catch (Throwable e) { return runtimeException(e); }
}
