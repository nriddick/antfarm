// SPDX-License-Identifier: MIT
// Additive ABI 1 adapter; one core and one process-wide dispatcher.
module antfarm.c.forkjoin;
import antfarm : fatal;
import antfarm.c.core;
import antfarm.c.runtime;
import core.atomic;
import core.memory : GC;
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;
import forkjoin.dispatcher;
import forkjoin.node;
import forkjoin.frames : allocFrame, freeFrame;
import threadpool.pool : CacheAwarePool;
import threadpool.topology : ProcessorId;

extern(C) alias NodeCallback = af_node* function(af_node*) nothrow @nogc;
extern(C) alias ForCallback = void function(void*, size_t) nothrow @nogc;
extern(C) alias ReduceLeaf = void function(void*, size_t, size_t, const(void)*, void*) nothrow @nogc;
extern(C) alias ReduceJoin = void function(void*, const(void)*, const(void)*, void*) nothrow @nogc;
struct af_node { ulong[4] private_words; NodeCallback run, join; void* context; }
struct af_processor { ushort group, processor, llc, reserved; }
struct af_dispatcher_options
{
    uint struct_size, abi_version;
    uint ring_mib, small_quota, deque_capacity, poll_every, avg_cost, cross_div;
    uint sibling_first, body_steps, idle_max, park, spin_steps;
    uint farm_per_group, caller_consumes, spill_inline;
    ulong reserved;
}
struct af_dispatcher_stats
{
    ulong pushed, popped, steal_attempts, steal_successes;
    ulong cross_attempts, cross_successes, sibling_successes;
    ulong spilled_tables, spilled_children, inline_children, solo_children;
    ulong farm_polls, farm_hits, deque_high_water, parks, wakes, submits;
}
static assert(Node.sizeof == 32 && Node.alignof == af_node.alignof);
static assert(af_node.sizeof == 56 && af_node.run.offsetof == 32);
static assert(af_dispatcher_options.sizeof == 72);
static assert(af_dispatcher_stats.sizeof == Stats.sizeof);
static foreach (i; 0 .. Stats.tupleof.length)
    static assert(af_dispatcher_stats.tupleof[i].offsetof == Stats.tupleof[i].offsetof);
struct af_dispatcher { Dispatcher value; af_dispatcher_stats stats; uint participants; bool running, callerConsumes; }
struct af_root_completion { RootCompletion value; bool pending; }
private __gshared af_dispatcher* current;
private af_node* activeNode;
private bool activeJoin, transferred;
private Node* lastTransfer; // what af_fork/af_complete returned to the active callback

// A C callback that neither completes nor forks, or returns anything but the
// value af_fork/af_complete gave it, would leave the tree incomplete (every
// wait spins) or hand the trampoline an arbitrary pointer. Diagnose both in
// every build.
private Node* checkedReturn(Node* result, bool join) nothrow @nogc
{
    if (transferred)
    {
        if (result !is lastTransfer)
            fatal(join ? "C forkjoin: join must return the value af_fork returned"
                       : "C forkjoin: run must return the value af_fork/af_complete returned");
    }
    else if (join)
    {
        if (result !is null) fatal("C forkjoin: join without af_fork must return NULL");
    }
    else
        fatal("C forkjoin: run returned without af_complete or af_fork");
    return result;
}

// Range callbacks (each/leaf/join) run inside a scheduler-owned frame, not a
// C node: no C node is active while they run, so a leaf cannot complete or
// fork the node that called af_forkjoin_for/reduce.
private struct NoActiveNode
{
    af_node* node; bool join, moved; Node* last;
    this(int) nothrow @nogc
    {
        node = activeNode; join = activeJoin; moved = transferred; last = lastTransfer;
        activeNode = null; activeJoin = false; transferred = false; lastTransfer = null;
    }
    ~this() nothrow @nogc
    {
        activeNode = node; activeJoin = join; transferred = moved; lastTransfer = last;
    }
}

private Node* callRun(Node* p) nothrow @nogc
{
    auto n = cast(af_node*) p;
    auto outer = activeNode;
    const outerJoin = activeJoin, outerTransferred = transferred;
    activeNode = n; activeJoin = false; transferred = false;
    scope(exit) { activeNode = outer; activeJoin = outerJoin; transferred = outerTransferred; }
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    return checkedReturn(cast(Node*) n.run(n), false);
}
private Node* callJoin(Node* p) nothrow @nogc
{
    auto n = cast(af_node*) p;
    if (n.join is null) fatal("C forkjoin: parent requires join callback");
    auto outer = activeNode;
    const outerJoin = activeJoin, outerTransferred = transferred;
    activeNode = n; activeJoin = true; transferred = false;
    scope(exit) { activeNode = outer; activeJoin = outerJoin; transferred = outerTransferred; }
    runtimeEnterCallback();
    scope(exit) runtimeLeaveCallback();
    return checkedReturn(cast(Node*) n.join(n), true);
}
private void requireNode(af_node* n) nothrow @nogc
{
    if (n is null || cast(size_t)n % af_node.alignof || n.run is null ||
        (cast(Node*) n).run != &callRun)
        fatal("C forkjoin: node must be initialized with af_node_init");
}
private uint running(af_dispatcher* d) nothrow @nogc
{
    if (auto s = runtimeAttached()) return s;
    if (d is null || !d.running || d !is current) return setError(AF_INVALID, "dispatcher is not running");
    return AF_OK;
}
private uint foreign(af_dispatcher* d) nothrow @nogc
{
    if (auto s = running(d)) return s;
    if (isParticipant()) return setError(AF_INVALID, "submit requires a non-participant; use af_run_root");
    if (d.participants < 2 || d.callerConsumes)
        return setError(AF_INVALID, "foreign submission requires workers and caller_consumes=0");
    return AF_OK;
}
private uint anyThread(af_dispatcher* d) nothrow @nogc
{
    if (auto s = running(d)) return s;
    return isParticipant() ? AF_OK : foreign(d);
}

extern(C) uint af_processors(af_processor* outCpus, size_t capacity, size_t* count) nothrow
{
    if (count is null) return setError(AF_INVALID, "null processor count");
    *count = 0;
    if (auto s = runtimeAttached()) return s;
    if (outCpus is null && capacity) return setError(AF_INVALID, "null processor output");
    try
    {
        auto topo = CacheAwarePool.topology();
        foreach (ref p; topo.processors) if (topo.isEligible(p)) ++*count;
        if (outCpus is null && capacity == 0) return setError(AF_OK);
        if (capacity < *count) return setError(AF_FULL, "processor output too small");
        size_t i;
        foreach (ref p; topo.processors)
            if (topo.isEligible(p)) outCpus[i++] = af_processor(p.group, p.lpIndex, p.llcIndex, 0);
        return setError(AF_OK);
    }
    catch (Throwable e) { return runtimeException(e); }
}
extern(C) void af_dispatcher_options_default(af_dispatcher_options* o) nothrow @nogc
{
    if (o is null) fatal("C forkjoin: null options output");
    *o = af_dispatcher_options(72, AF_ABI_VERSION, 8, 4096, 4096, 256, 7, 8,
        1, 1024, 0, 1, 4096, 0, 0, 1, 0); // spill_inline = 1, as DispatcherOptions.init
}
extern(C) uint af_dispatcher_start(const(af_dispatcher_options)* supplied,
    const(af_processor)* cpus, size_t count, af_dispatcher** output) nothrow
{
    if (output is null) return setError(AF_INVALID, "null dispatcher output");
    *output = null;
    if (auto s = runtimeControl()) return s;
    if (current !is null) return setError(AF_BUSY, "a C dispatcher is already running");
    af_dispatcher_options defaults;
    if (supplied is null) { af_dispatcher_options_default(&defaults); supplied = &defaults; }
    if (supplied.struct_size != af_dispatcher_options.sizeof) return setError(AF_INVALID, "invalid dispatcher options size");
    const o = *supplied;
    if (o.abi_version != AF_ABI_VERSION || o.reserved || o.sibling_first > 1 || o.park > 1 ||
        o.farm_per_group > 1 || o.caller_consumes > 1 || o.spill_inline > 1 || o.body_steps == 0 ||
        o.avg_cost > 7 || o.deque_capacity == 0 || o.deque_capacity > (1u << 30) ||
        count > maxParticipants || (cpus is null && count) || (cpus !is null && count == 0))
        return setError(AF_INVALID, "invalid dispatcher options or processor span");
    auto d = cast(af_dispatcher*) malloc(af_dispatcher.sizeof);
    if (d is null) return setError(AF_ALLOCATION_FAILED, "dispatcher handle allocation failed");
    *d = af_dispatcher.init;
    GC.addRange(d, af_dispatcher.sizeof);
    try
    {
        DispatcherOptions opt;
        opt.ringMiB = o.ring_mib; opt.smallQuota = o.small_quota;
        opt.dequeCapacity = o.deque_capacity; opt.pollEvery = o.poll_every;
        opt.avgCost = o.avg_cost; opt.crossDiv = o.cross_div; opt.siblingFirst = o.sibling_first != 0;
        opt.bodySteps = o.body_steps; opt.idleMax = o.idle_max; opt.park = o.park != 0;
        opt.spinSteps = o.spin_steps; opt.farmPerGroup = o.farm_per_group != 0;
        opt.callerConsumes = o.caller_consumes != 0; opt.spillInline = o.spill_inline != 0;
        ProcessorId[] ids;
        if (count)
        {
            foreach (c; cpus[0 .. count])
            {
                if (c.reserved) { GC.removeRange(d); free(d); return setError(AF_INVALID, "reserved processor field"); }
                ids ~= ProcessorId(c.group, c.processor);
            }
        }
        else
        {
            auto topo = CacheAwarePool.topology();
            foreach (ref p; topo.processors)
                if (topo.isEligible(p) && ids.length < maxParticipants) ids ~= ProcessorId.of(p);
        }
        d.value.start(ids, opt);
        d.participants = cast(uint) ids.length;
        d.callerConsumes = opt.callerConsumes;
        d.running = true;
        current = d;
        discardCounters();
        runtimeRetain();
        *output = d;
        return setError(AF_OK);
    }
    catch (Throwable e)
    {
        GC.removeRange(d); free(d);
        return runtimeException(e);
    }
}
extern(C) uint af_dispatcher_stop(af_dispatcher* d) nothrow
{
    if (auto s = runtimeControl()) return s;
    if (d is null) return setError(AF_INVALID, "null dispatcher");
    if (!d.running) return setError(AF_OK);
    try
    {
        d.value.stop();
        auto s = counters();
        memcpy(&d.stats, &s, s.sizeof);
        d.running = false;
        current = null;
        return setError(AF_OK);
    }
    catch (Throwable e) { return runtimeException(e); }
}
extern(C) uint af_dispatcher_counters(const(af_dispatcher)* d, af_dispatcher_stats* outStats) nothrow @nogc
{
    if (auto s = runtimeControl()) return s;
    if (d is null || outStats is null) return setError(AF_INVALID, "null dispatcher or counters");
    if (d.running) return setError(AF_BUSY, "counters are complete only after stop");
    *outStats = d.stats;
    return setError(AF_OK);
}
extern(C) void af_dispatcher_destroy(af_dispatcher* d) nothrow @nogc
{
    if (d is null) return;
    if (runtimeControl() || d.running) fatal("C forkjoin: destroy requires control thread and stopped dispatcher");
    GC.removeRange(d); free(d); runtimeRelease();
}
extern(C) void af_node_init(af_node* n, NodeCallback run, NodeCallback join, void* context) nothrow @nogc
{
    if (n is null || run is null || cast(size_t)n % af_node.alignof) fatal("C forkjoin: invalid node initializer");
    *n = af_node.init;
    n.run = run; n.join = join; n.context = context;
    (cast(Node*)n).run = &callRun;
    (cast(Node*)n).join = &callJoin;
}
extern(C) af_node* af_fork(af_node* parent, af_node** children, size_t count) nothrow @nogc
{
    if (!isParticipant()) fatal("C forkjoin: fork requires a participant");
    requireNode(parent);
    if (activeNode !is parent || transferred) fatal("C forkjoin: fork requires the current untransferred callback");
    if (parent.join is null) fatal("C forkjoin: parent requires join callback");
    if (!count || count > uint.max || children is null || cast(size_t)children % (void*).alignof ||
        cast(size_t)children > size_t.max - count * (void*).sizeof)
        fatal("C forkjoin: invalid children span");
    foreach (i; 0 .. count) { requireNode(children[i]); if (children[i] is parent) fatal("C forkjoin: self child"); }
    transferred = true;
    lastTransfer = fork(cast(Node*)parent, (cast(Node**)children)[0 .. count]);
    return cast(af_node*) lastTransfer;
}
extern(C) af_node* af_complete(af_node* n) nothrow @nogc
{
    if (!isParticipant()) fatal("C forkjoin: complete requires a participant");
    requireNode(n);
    if (activeNode !is n || activeJoin || transferred) fatal("C forkjoin: complete requires the current uncompleted run callback");
    transferred = true;
    lastTransfer = complete(cast(Node*)n);
    return cast(af_node*) lastTransfer;
}
extern(C) uint af_run_root(af_dispatcher* d, af_node* root) nothrow @nogc
{
    if (auto s = running(d)) return s;
    if (!isParticipant()) return setError(AF_INVALID, "run_root requires a participant");
    requireNode(root);
    runRoot(cast(Node*)root);
    return setError(AF_OK);
}
extern(C) uint af_root_completion_create(af_root_completion** output) nothrow @nogc
{
    if (output is null) return setError(AF_INVALID, "null completion output");
    *output = null;
    if (auto s = runtimeAttached()) return s;
    auto c = cast(af_root_completion*) malloc(af_root_completion.sizeof);
    if (c is null) return setError(AF_ALLOCATION_FAILED, "completion allocation failed");
    static immutable initial = af_root_completion.init;
    memcpy(c, &initial, af_root_completion.sizeof);
    runtimeRetain(); *output = c;
    return setError(AF_OK);
}
extern(C) uint af_submit_root(af_dispatcher* d, af_node* root, af_root_completion* c) nothrow @nogc
{
    if (auto s = foreign(d)) return s;
    if (c is null) return setError(AF_INVALID, "null completion");
    if (c.pending) return setError(AF_BUSY, "completion must be waited before reuse");
    requireNode(root);
    c.pending = true;
    submitRoot(cast(Node*)root, &c.value);
    return setError(AF_OK);
}
extern(C) uint af_wait_root(af_root_completion* c) nothrow @nogc
{
    if (auto s = runtimeAttached()) return s;
    if (c is null || !c.pending || isParticipant()) return setError(AF_INVALID, "wait requires pending completion and non-participant");
    waitRoot(&c.value); c.pending = false;
    return setError(AF_OK);
}
extern(C) uint af_root_completion_destroy(af_root_completion* c) nothrow @nogc
{
    if (auto s = runtimeAttached()) return s;
    if (c is null) return setError(AF_OK);
    if (c.pending) return setError(AF_BUSY, "completion must be waited before destruction");
    free(c); runtimeRelease();
    return setError(AF_OK);
}
extern(C) uint af_submit_detached(af_dispatcher* d, af_node* root) nothrow @nogc
{
    if (auto s = foreign(d)) return s;
    requireNode(root);
    submitDetached(cast(Node*)root);
    return setError(AF_OK);
}

private struct RangeJob
{
    size_t grain, bytes, stride;
    ForCallback each;
    ReduceLeaf leaf;
    ReduceJoin join;
    const(void)* seed;
    void* context;
}
private struct RangeFrame
{
    Node node;
    size_t begin, end;
    RangeJob* job;
    RangeFrame* kids;
}
private enum resultOffset = (RangeFrame.sizeof + 15) & ~size_t(15);
private void* resultOf(RangeFrame* f) nothrow @nogc { return cast(ubyte*)f + resultOffset; }
private RangeFrame* rightOf(RangeFrame* f) nothrow @nogc { return cast(RangeFrame*)(cast(ubyte*)f.kids + f.job.stride); }
private Node* rangeJoin(Node* node) nothrow @nogc
{
    auto f = cast(RangeFrame*)node;
    if (f.job.leaf !is null)
    {
        auto guard = NoActiveNode(0);
        runtimeEnterCallback();
        scope(exit) runtimeLeaveCallback();
        f.job.join(f.job.context, resultOf(f.kids), resultOf(rightOf(f)), resultOf(f));
    }
    freeFrame(f.kids);
    return null;
}
private Node* rangeRun(Node* node) nothrow @nogc
{
    auto f = cast(RangeFrame*)node;
    auto j = f.job;
    if (f.end - f.begin <= j.grain)
    {
        auto guard = NoActiveNode(0);
        runtimeEnterCallback();
        scope(exit) runtimeLeaveCallback();
        if (j.leaf !is null) j.leaf(j.context, f.begin, f.end, j.seed, resultOf(f));
        else foreach (i; f.begin .. f.end) j.each(j.context, i);
        return complete(node);
    }
    f.kids = cast(RangeFrame*) allocFrame(2 * j.stride);
    if (f.kids is null) fatal("C forkjoin: range frame allocation failed");
    auto left = f.kids, right = rightOf(f);
    *left = RangeFrame.init; *right = RangeFrame.init;
    const mid = f.begin + (f.end - f.begin) / 2;
    left.begin = f.begin; left.end = mid; right.begin = mid; right.end = f.end;
    left.job = right.job = j;
    left.node.run = right.node.run = &rangeRun;
    f.node.join = &rangeJoin;
    Node*[2] kids = [&left.node, &right.node];
    return fork(node, kids[]);
}
private uint rangeExecute(af_dispatcher* d, size_t begin, size_t end, ref RangeJob j, void* output) nothrow @nogc
{
    if (auto s = anyThread(d)) return s;
    if (end < begin || j.grain == 0) return setError(AF_INVALID, "invalid range or zero grain");
    if (begin == end) { if (j.bytes) memcpy(output, j.seed, j.bytes); return setError(AF_OK); }
    // malloc gives the synchronous root storage no frame-pool TLS ownership on
    // foreign threads; all recursive frames are allocated by participants.
    auto root = cast(RangeFrame*) malloc(j.stride);
    if (root is null) return setError(AF_ALLOCATION_FAILED, "range root allocation failed");
    *root = RangeFrame.init;
    root.node.run = &rangeRun; root.begin = begin; root.end = end; root.job = &j;
    runRootAny(&root.node);
    if (j.bytes) memcpy(output, resultOf(root), j.bytes);
    free(root);
    return setError(AF_OK);
}
extern(C) uint af_forkjoin_for(af_dispatcher* d, size_t begin, size_t end, size_t grain, ForCallback fn, void* context) nothrow @nogc
{
    if (fn is null) return setError(AF_INVALID, "null range callback");
    RangeJob j;
    j.grain = grain; j.each = fn; j.context = context; j.stride = resultOffset;
    return rangeExecute(d, begin, end, j, null);
}
extern(C) uint af_forkjoin_reduce(af_dispatcher* d, size_t begin, size_t end, size_t grain, size_t bytes,
    const(void)* seed, ReduceLeaf leaf, ReduceJoin join, void* context, void* output) nothrow @nogc
{
    // The frame allocator rounds large requests to a 64 KiB chunk.
    if (!bytes || bytes > (size_t.max / 2 - resultOffset - 65536) || seed is null || output is null || leaf is null || join is null)
        return setError(AF_INVALID, "invalid reduction storage or callbacks");
    RangeJob j;
    j.grain = grain; j.bytes = bytes; j.stride = (resultOffset + bytes + 15) & ~size_t(15);
    j.seed = seed; j.leaf = leaf; j.join = join; j.context = context;
    return rangeExecute(d, begin, end, j, output);
}
