module private_actor_tests;
import actors;
import antfarm;
import antfarm_allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import core.stdc.stdlib : abort;
import core.stdc.stdio : fprintf, stderr;
import std.algorithm.searching : canFind;
import std.process : Redirect, pipeProcess, wait;
import std.stdio : writeln;
import std.exception : enforce;

void check(bool ok, string file = __FILE__, size_t line = __LINE__) nothrow @nogc @system
{
    if (!ok) { fprintf(stderr, "FAIL %.*s:%zu\n", cast(int) file.length, file.ptr, line); abort(); }
}
struct Counts { shared long allocated, freed, budget = long.max; }
void* allocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto c = cast(Counts*) ctx;
    if (atomicFetchSub(c.budget, 1L) <= 0 || alignment > 64) return null;
    auto p = allocateAligned64(bytes);
    if (p !is null) atomicFetchAdd(c.allocated, 1L);
    return p;
}
void deallocate(void* ctx, void* ptr, size_t, size_t) nothrow @nogc @system
{
    atomicFetchAdd((cast(Counts*) ctx).freed, 1L);
    freeAligned64(ptr);
}
ActorAllocator policy(ref Counts c) nothrow @nogc @system
{ return ActorAllocator(&c, &allocate, &deallocate); }

class Workers
{
    shared uint stop, started;
    Thread[] threads;
    this(AntFarm* farm, uint count)
    {
        foreach (_; 0 .. count)
        {
            auto t = new Thread({
                ConsumerView view;
                check(view.subscribe(farm) >= 0);
                scope(exit) view.unsubscribe();
                atomicFetchAdd(started, 1u);
                while (atomicLoad(stop) == 0)
                    if (!view.consumeNext()) pause();
                while (view.consumeNext()) {}
            });
            threads ~= t;
            t.start();
        }
        while (atomicLoad(started) < count) Thread.yield();
    }
    void join() { atomicStore(stop, 1u); foreach (t; threads) t.join(); }
}

struct State { ulong* output; ulong iterations, n; }
PrivateRootDisposition repeat(scope ref PrivateBorrow!State b, scope ref PrivateContext)
    nothrow @nogc @system
{
    check(*b.value.output == b.value.n);
    ++b.value.n;
    *b.value.output = b.value.n;
    return b.value.n == b.value.iterations ? PrivateRootDisposition.finish : PrivateRootDisposition.again;
}
alias Park = PrivateRootPark!(State, repeat);
static assert(!__traits(compiles, PrivateDisposition));
static assert(__traits(compiles, (Park* p) pure nothrow @nogc @safe { return p.live + p.capacity; }));
static assert(!__traits(compiles, (Park p) { auto copy = p; }));
static assert(!__traits(compiles, (PrivateBorrow!State b) { return b.state_; }));
static assert(!__traits(compiles, (Park* p) { return p.entries_; }));
static assert(!__traits(compiles, (PrivateContext c) { auto copy = c; }));
static assert(!__traits(compiles, (PrivateBorrow!State b) { auto copy = b; }));

// Reject copying immutable pointees into mutable state through const(T).
struct ConstInitState { int* data; }
PrivateRootDisposition constInitStep(scope ref PrivateBorrow!ConstInitState b, scope ref PrivateContext)
    nothrow @nogc @system
{
    ++*b.value.data;
    return PrivateRootDisposition.finish;
}
alias ConstInitPark = PrivateRootPark!(ConstInitState, constInitStep);
immutable int constInitNumber = 7;
immutable(ConstInitState) constInitImmutable = {data: &constInitNumber};
ConstInitState constInitMutable;
static assert(!__traits(compiles, (ConstInitPark* p) { p.spawn(constInitImmutable); }));
static assert(__traits(compiles, (ConstInitPark* p) { p.spawn(constInitMutable); }));
static assert(!__traits(compiles,
    (PrivateChildPark!ConstInitState* p, ref PrivateContext c) { p.spawn(c, constInitImmutable); }));
static assert(__traits(compiles,
    (PrivateChildPark!ConstInitState* p, ref PrivateContext c) { p.spawn(c, constInitMutable); }));

void fifoProgress()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto park = Park.create(farm, 2);
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    ulong[2] output;
    foreach (i; 0 .. 2) check((park.spawn(State(&output[i], 2, 0))) == PrivateSpawnResult.spawned);
    foreach (_; 0 .. 2)
    {
        park.pump(token, 1);
        while (view.consumeNext()) {}
    }
    check(output[0] == 1 && output[1] == 1);
    while (park.live) { park.pump(token, 1); while (view.consumeNext()) {} }
    park.destroy();
}

void roots(PrivateRootReclamation reclamation = PrivateRootReclamation.emitter)
    (uint consumers, size_t count, size_t iterations, size_t batch)
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, consumers ? consumers : 1, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto park = PrivateRootPark!(State, repeat, reclamation).create(farm, count, batch, policy(counts));
    check(park !is null);
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    auto workers = new Workers(farm, consumers);
    if (!consumers) check(view.subscribe(farm) >= 0);
    scope(exit) { workers.join(); if (!consumers) view.unsubscribe(); }
    auto output = new ulong[count];
    foreach (round; 0 .. 8)
    {
        output[] = 0;
        foreach (i; 0 .. count) check((park.spawn(State(&output[i], iterations, 0))) == PrivateSpawnResult.spawned);
        check(park.spawn(State.init) == PrivateSpawnResult.capacityFull);
        auto deadline = MonoTime.currTime + 30.seconds;
        while (park.live)
        {
            park.pump(token, batch);
            if (!consumers) while (view.consumeNext()) {}
            enforce(MonoTime.currTime < deadline, "root progress timeout");
        }
        foreach (x; output) check(x == iterations);
    }
    park.destroy();
    check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}

struct Leaf { ulong* output; ulong generation; shared uint* gate; }
void leafStep(scope ref PrivateBorrow!Leaf b, scope ref PrivateContext)
    nothrow @nogc @system
{
    while (atomicLoad!(MemoryOrder.raw)(*b.value.gate) == 0) pause();
    // Unshared data: wave completion must supply the happens-before edge.
    check(*b.value.output == b.value.generation);
    foreach (_; 0 .. 100) pause();
    *b.value.output = ++b.value.generation;
}
struct Node { PrivateChildPark!Leaf* leaves; ulong* results; ulong generation; shared uint* gate; }
void nodeStep(scope ref PrivateBorrow!Node b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.leaves is null)
    {
        b.value.leaves = ctx.createChildren!Leaf(4);
        check(b.value.leaves !is null);
        check(b.value.leaves.finished(ctx));
        check(b.value.leaves.start!leafStep(ctx) == PrivateStartResult.empty);
        foreach (i; 0 .. 4) check((b.value.leaves.spawn(ctx, Leaf(&b.value.results[i], 0, b.value.gate))) == PrivateSpawnResult.spawned);
        check(b.value.leaves.spawn(ctx, Leaf.init) == PrivateSpawnResult.capacityFull);
    }
    foreach (i; 0 .. 4) check(b.value.results[i] == b.value.generation);
    ++b.value.generation;
    atomicStore!(MemoryOrder.raw)(*b.value.gate, 0u);
    check((b.value.leaves.start!leafStep(ctx)) == PrivateStartResult.started);
    check(b.value.leaves.start!leafStep(ctx) == PrivateStartResult.waveActive); // no overlapping lease
    check(b.value.leaves.spawn(ctx, Leaf.init) == PrivateSpawnResult.waveActive);
    atomicStore!(MemoryOrder.raw)(*b.value.gate, 1u);
}
void nodeRetire(scope ref PrivateBorrow!Node b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    check(b.value.generation == 3);
    foreach (i; 0 .. 4) check(b.value.results[i] == 3);
    check(ctx.createChildren!Leaf(1) is null);
    check(b.value.leaves.start!leafStep(ctx) == PrivateStartResult.retiring);
    check(b.value.leaves.spawn(ctx, Leaf.init) == PrivateSpawnResult.retiring);
}
struct Parent { PrivateChildPark!Node* nodes; ulong* results; uint phase; shared uint* gates; }
PrivateRootDisposition parentStep(scope ref PrivateBorrow!Parent b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.nodes is null)
    {
        b.value.nodes = ctx.createChildren!Node(17);
        check(b.value.nodes !is null);
        foreach (i; 0 .. 17)
            check((b.value.nodes.spawn(ctx, Node(null, b.value.results + i * 4, 0, b.value.gates + i))) == PrivateSpawnResult.spawned);
    }
    if (!b.value.nodes.finished(ctx)) return PrivateRootDisposition.again;
    foreach (i; 0 .. 68) check(b.value.results[i] == (b.value.phase < 3 ? b.value.phase : 3));
    if (b.value.phase == 4) return PrivateRootDisposition.finish;
    check((b.value.phase == 3 ? b.value.nodes.startFinal!nodeRetire(ctx)
        : b.value.nodes.start!nodeStep(ctx)) == PrivateStartResult.started);
    ++b.value.phase;
    return PrivateRootDisposition.again;
}
void trees(uint consumers = 6)
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 6, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto park = PrivateRootPark!(Parent, parentStep).create(farm, 48, defaultPrivateBatchLimit, policy(counts));
    check(park !is null);
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, consumers);
    ConsumerView view;
    if (!consumers) check(view.subscribe(farm) >= 0);
    scope(exit) { workers.join(); if (!consumers) view.unsubscribe(); }
    version (AntfarmWriteAuditHooks)
    {
        if (!consumers) { fastView = &view; writeAuditHook = &consumeBeforeReturn; }
    }
    scope(exit) { version (AntfarmWriteAuditHooks) { writeAuditHook = null; fastView = null; } }
    auto output = new ulong[48 * 68];
    auto gates = new shared(uint)[48 * 17];
    foreach (round; 0 .. 12)
    {
        output[] = 0;
        foreach (i; 0 .. 48) check((park.spawn(Parent(null, output.ptr + i * 68, 0, gates.ptr + i * 17))) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 30.seconds;
        while (park.live)
        {
            park.pump(token, 13);
            if (!consumers) while (view.consumeNext()) {}
            enforce(MonoTime.currTime < deadline, "nested tree progress timeout");
        }
        foreach (x; output) check(x == 3);
    }
    park.destroy();
    check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}

version (AntfarmWriteAuditHooks)
{
    __gshared ConsumerView* fastView;
    __gshared size_t earlyTables;
    void consumeBeforeReturn(AntFarm*, WriteAuditPhase phase, ulong, ulong)
        nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        ++earlyTables;
        while (fastView.consumeNext()) {}
    }
    void earlyCompletion()
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto park = PrivateRootPark!(State, repeat, PrivateRootReclamation.consumer).create(farm, 300);
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        fastView = &view;
        writeAuditHook = &consumeBeforeReturn;
        scope(exit) { writeAuditHook = null; fastView = null; }
        ulong[300] results;
        foreach (i; 0 .. 300) check((park.spawn(State(&results[i], 1, 0))) == PrivateSpawnResult.spawned);
        while (park.live) park.pump(token);
        foreach (n; results) check(n == 1);
        park.destroy();
        check(earlyTables > 0);
    }
}

void failures()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    foreach (budget; 0 .. 4)
    {
        Counts counts;
        atomicStore(counts.budget, cast(long) budget);
        check(Park.create(farm, 4, defaultPrivateBatchLimit, policy(counts)) is null);
        check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
    Counts counts;
    auto park = Park.create(farm, 4, defaultPrivateBatchLimit, policy(counts));
    check(park !is null);
    check(park.capacity == 4);
    park.collectCompleted();
    park.poll();
    atomicStore(counts.budget, 0L);
    check(park.spawn(State.init) == PrivateSpawnResult.allocationFailed);
    check(park.live == 0);
    park.destroy();
    check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}

void backpressure()
{
    enum count = 20000;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto park = Park.create(farm, count, 256);
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto output = new ulong[count];
    foreach (i; 0 .. count) check((park.spawn(State(&output[i], 1, 0))) == PrivateSpawnResult.spawned);
    size_t submitted;
    bool partial;
    // Keep the subscriber parked until admission returns zero.
    while (submitted < count)
    {
        auto n = park.pump(token);
        if (n == 0) break;
        partial |= n < 256;
        submitted += n;
    }
    check(submitted > 0 && submitted < count && partial);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        while (view.consumeNext()) {}
        park.pump(token);
        enforce(MonoTime.currTime < deadline, "backpressure recovery timeout");
    }
    foreach (x; output) check(x == 1);
    park.destroy();
}

void shardedCreators()
{
    Counts counts;
    auto farm = AntFarm.create(8, 8, 6, 0, 0, 4, 8192);
    scope(exit) farm.destroy();
    auto workers = new Workers(farm, 6);
    scope(exit) workers.join();
    Thread[] emitters;
    foreach (_; 0 .. 4)
    {
        auto thread = new Thread({
            auto park = Park.create(farm, 1024, defaultPrivateBatchLimit, policy(counts));
            check(park !is null);
            auto token = farm.registerProducer(Tier.small);
            check(token.valid);
            scope(exit) farm.unregisterProducer(token);
            auto output = new ulong[1024];
            foreach (round; 0 .. 20)
            {
                output[] = 0;
                foreach (i; 0 .. 1024) check((park.spawn(State(&output[i], 3, 0))) == PrivateSpawnResult.spawned);
                auto deadline = MonoTime.currTime + 30.seconds;
                while (park.live)
                {
                    park.pump(token);
                    enforce(MonoTime.currTime < deadline, "sharded creation timeout");
                }
                foreach (x; output) check(x == 3);
            }
            park.destroy();
        });
        emitters ~= thread;
        thread.start();
    }
    foreach (thread; emitters) thread.join();
    check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}

struct JoinProbe { shared uint entered, done; ulong[2] values; }
struct JoinBodies
{
    JoinProbe* probe; size_t index; ulong[2] scratch;
    @property bool empty() const nothrow @nogc { return index == 2; }
    @property size_t length() const nothrow @nogc { return 2 - index; }
    @property JoinBodies save() nothrow @nogc { return this; }
    @property PayloadBody front() nothrow @nogc @system
    { scratch[0] = cast(ulong) probe; scratch[1] = index; return scratch[]; }
    void popFront() nothrow @nogc { ++index; }
}
long joinWork(PayloadHeader*, PayloadBody body, ulong) nothrow @nogc @system
{
    auto probe = cast(JoinProbe*) atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) body.ptr);
    auto index = atomicLoad!(MemoryOrder.raw)(*cast(shared ulong*) (body.ptr + 1));
    atomicFetchAdd!(MemoryOrder.raw)(probe.entered, 1u);
    while (atomicLoad!(MemoryOrder.raw)(probe.entered) != 2) pause();
    probe.values[index] = index + 1; // no test release after this write
    return 1;
}
void joined(void* p) nothrow @nogc @system
{
    auto probe = cast(JoinProbe*) p;
    check(probe.values[0] == 1 && probe.values[1] == 2);
    atomicStore!(MemoryOrder.rel)(probe.done, 1u);
}
void sameShardJoin()
{
    // Both consumers must claim different 1-payload runs of the SAME shard.
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 8192, uint.max);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 2);
    scope(exit) workers.join();
    foreach (_; 0 .. 30)
    {
        JoinProbe probe;
        TableCompletionHook hook = TableCompletionHook(&probe, &joined);
        PayloadHeader header;
        header.maxCs = header.done = 1;
        header.plen = 2;
        header.call = &joinWork;
        auto n = farm.writeTracked(header, JoinBodies(&probe), 2, &hook, token, MAX_AVG_COST);
        check(n == 2);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.acq)(probe.done) == 0)
            enforce(MonoTime.currTime < deadline, "join timeout");
    }
}
struct BatchChild { ulong* output; }
void batchChildOperation(scope ref PrivateBorrow!BatchChild b, scope ref PrivateContext)
    nothrow @nogc @system { ++*b.value.output; }
struct BatchParent
{
    PrivateChildPark!BatchChild* children;
    ulong* output;
    size_t count, limit;
    uint phase;
}
PrivateRootDisposition batchParentStep(scope ref PrivateBorrow!BatchParent b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.children is null)
    {
        b.value.children = ctx.createChildren!BatchChild(b.value.count, b.value.limit);
        check(b.value.children !is null);
        foreach (i; 0 .. b.value.count)
            check(b.value.children.spawn(ctx, BatchChild(b.value.output + i)) == PrivateSpawnResult.spawned);
    }
    if (!b.value.children.finished(ctx)) return PrivateRootDisposition.again;
    foreach (i; 0 .. b.value.count) check(b.value.output[i] == b.value.phase);
    if (b.value.phase == 3) return PrivateRootDisposition.finish;
    auto result = b.value.phase == 2 ? b.value.children.startFinal!batchChildOperation(ctx)
        : b.value.children.start!batchChildOperation(ctx);
    check(result == PrivateStartResult.started);
    ++b.value.phase;
    return PrivateRootDisposition.again;
}

// Private Batch is module-private, so Batch.sizeof is unreachable here.
// Authorized layout in actors/private_actor.d (locked there by
// static assert(Batch.sizeof == 6 * size_t.sizeof)):
//   TableCompletionHook hook; // context + call, two pointers
//   Batch* next;
//   shared(Batch*)* returns;
//   size_t accepted;
//   Entry** entries;
enum privateBatchWords = 6;
static assert(TableCompletionHook.sizeof == 2 * size_t.sizeof);
static assert((void*).sizeof == size_t.sizeof);
enum descriptorOverflowCapacity = size_t.max / (privateBatchWords * size_t.sizeof) + 1;
static assert(descriptorOverflowCapacity > 1);
static assert(size_t.sizeof == 8); // 1UL<<32 must survive conversion to size_t

// One request above 1MiB fails closed, so a mutated full-limit allocation
// returns null instead of OOMing. bytesAllocated/bytesFreed balance sizes.
struct BoundedCounts
{
    shared long requests, bytesAllocated, bytesFreed;
    size_t maxRequest = 1 << 20;
}
void* boundedAllocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto c = cast(BoundedCounts*) ctx;
    atomicFetchAdd(c.requests, 1L);
    if (bytes > c.maxRequest || alignment > 64) return null;
    auto p = allocateAligned64(bytes);
    if (p is null) return null;
    atomicFetchAdd(c.bytesAllocated, cast(long) bytes);
    return p;
}
void boundedDeallocate(void* ctx, void* ptr, size_t bytes, size_t) nothrow @nogc @system
{
    if (ptr is null) return;
    atomicFetchAdd((cast(BoundedCounts*) ctx).bytesFreed, cast(long) bytes);
    freeAligned64(ptr);
}
ActorAllocator boundedPolicy(ref BoundedCounts c) nothrow @nogc @system
{ return ActorAllocator(&c, &boundedAllocate, &boundedDeallocate); }
void checkBoundedBalance(ref BoundedCounts counts, string label) nothrow @nogc @system
{
    check(atomicLoad(counts.bytesAllocated) == atomicLoad(counts.bytesFreed), label, 0);
}

void hugeConfiguredLimits()
{
    BoundedCounts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();

    // Smallest capacity whose limit-1 descriptor array overflows Batch.sizeof.
    auto overflowStarted = MonoTime.currTime;
    check(Park.create(farm, descriptorOverflowCapacity, 1, boundedPolicy(counts)) is null,
        "descriptor array overflow", 0);
    check(MonoTime.currTime - overflowStarted < 2.seconds, "descriptor overflow prompt", 0);
    check(atomicLoad(counts.requests) == 0, "descriptor overflow before allocation", 0);
    checkBoundedBalance(counts, "descriptor overflow balance");

    static void exercise(size_t capacity, size_t configured, ref BoundedCounts counts,
        AntFarm* farm, ref Token token, ref ConsumerView view, string label)
    {
        auto started = MonoTime.currTime;
        auto park = Park.create(farm, capacity, configured, boundedPolicy(counts));
        check(park !is null, label, 0);
        check(MonoTime.currTime - started < 2.seconds, "huge configured limit prompt", 0);
        check(park.capacity == capacity);
        ulong[3] outputs;
        foreach (i; 0 .. capacity)
            check(park.spawn(State(&outputs[i], 1, 0)) == PrivateSpawnResult.spawned);
        // Configured limit may exceed capacity; validation accepts it and copies clamp.
        check(park.pump(token, configured) == capacity, label, 0);
        auto deadline = MonoTime.currTime + 5.seconds;
        while (park.live)
        {
            while (view.consumeNext()) {}
            park.collectCompleted();
            if (park.live) park.pump(token, configured);
            check(MonoTime.currTime < deadline, label, 0);
        }
        foreach (i; 0 .. capacity) check(outputs[i] == 1, label, 0);
        // Per-call limit 1 remains valid at the low end of 1..configured.
        foreach (i; 0 .. capacity)
        {
            outputs[i] = 0;
            check(park.spawn(State(&outputs[i], 1, 0)) == PrivateSpawnResult.spawned);
        }
        foreach (i; 0 .. capacity)
        {
            check(park.pump(token, 1) == 1, label, 0);
            while (view.consumeNext()) {}
            park.collectCompleted();
        }
        check(park.live == 0, label, 0);
        foreach (i; 0 .. capacity) check(outputs[i] == 1, label, 0);
        park.destroy();
        checkBoundedBalance(counts, label);
    }
    exercise(1, size_t.max, counts, farm, token, view, "size_t.max root limit");
    exercise(3, 1UL << 32, counts, farm, token, view, "1<<32 root limit");
    writeln("PASS huge configured root limits and descriptor overflow");
}

// Same-thread publication hook. Zero-count root and intermediate tables are
// ignored so only target callbacks contribute to the limit and table count.
// Controls stay in the test scope; callbacks do not save PrivateContext.
version (AntfarmWriteAuditHooks)
{
    struct TargetTableCount
    {
        ulong* slots;
        size_t count, limit, tables;
    }
    __gshared TargetTableCount* targetTableCount;
    void countTargetPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong)
        nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto c = targetTableCount;
        size_t before;
        foreach (i; 0 .. c.count) before += c.slots[i];
        while (fastView.consumeNext()) {}
        size_t after;
        foreach (i; 0 .. c.count) after += c.slots[i];
        immutable n = after - before;
        if (n == 0) return;
        // Mutations caught on the first oversized table (quota 4096 admits it):
        //   * `park.batchLimit_ = batchLimit` -> `= 256` in PrivateChildPark.create
        //     emits 50 explicit-limit callbacks in one table, not ceil(50/7)=8.
        //   * no-argument createChildren passes actor_.service.batchLimit
        //     instead of PrivateContext.batchLimit_ (old root-only inheritance).
        //     Root limit 31 then emits 31 grandchild callbacks, not 10 tables of <=5.
        check(n <= c.limit, c.limit == 7
            ? "explicit child table exceeds limit 7"
            : "grandchild table exceeds immediate parent limit 5", 0);
        ++c.tables;
    }

    void drainCounted(Root, alias step)(size_t rootLimit, ulong[] slots, size_t childLimit,
        size_t expectTables, string label)
    {
        Counts counts;
        // One subscriber, no worker: the hook consumes each table before
        // writeTracked returns. One fresh 4096 grant covers every table, so
        // quota cannot split a 5-, 7-, or 256-wide cohort into extra tables.
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        TargetTableCount tally;
        tally.slots = slots.ptr;
        tally.count = slots.length;
        tally.limit = childLimit;
        targetTableCount = &tally;
        scope(exit) targetTableCount = null;
        fastView = &view;
        writeAuditHook = &countTargetPublished;
        scope(exit) { writeAuditHook = null; fastView = null; }
        auto park = PrivateRootPark!(Root, step).create(farm, 1, rootLimit, policy(counts));
        check(park !is null, label, 0);
        check(park.spawn(Root(null, slots.ptr)) == PrivateSpawnResult.spawned, label, 0);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.pump(token);
            check(MonoTime.currTime < deadline, label, 0);
        }
        check(tally.tables == expectTables, label, 0);
        foreach (slot; slots) check(slot == 1, label, 0);
        park.destroy();
        check(atomicLoad(counts.allocated) == atomicLoad(counts.freed), label, 0);
    }

    struct ExplicitRoot
    {
        PrivateChildPark!BatchChild* children;
        ulong* slots;
    }
    PrivateRootDisposition explicitRootStep(scope ref PrivateBorrow!ExplicitRoot b,
        scope ref PrivateContext ctx) nothrow @nogc @system
    {
        enum children = 50;
        enum limit = 7;
        if (b.value.children is null)
        {
            // Root configured limit is 256; the explicit child limit must win.
            b.value.children = ctx.createChildren!BatchChild(children, limit);
            check(b.value.children !is null, "explicit child create", 0);
            foreach (i; 0 .. children)
                check(b.value.children.spawn(ctx, BatchChild(b.value.slots + i))
                    == PrivateSpawnResult.spawned, "explicit child spawn", 0);
            check(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.started,
                "explicit child start", 0);
            return PrivateRootDisposition.again;
        }
        if (!b.value.children.finished(ctx)) return PrivateRootDisposition.again;
        foreach (i; 0 .. children) check(b.value.slots[i] == 1, "explicit child joined", 0);
        return PrivateRootDisposition.finish;
    }
    void explicitChildLimit()
    {
        enum children = 50;
        enum limit = 7;
        enum tables = (children + limit - 1) / limit;
        static assert(tables == 8);
        ulong[children] slots;
        drainCounted!(ExplicitRoot, explicitRootStep)(256, slots[], limit, tables,
            "explicit child table count");
        writeln("PASS explicit child limit 7 emits 8 tables");
    }

    struct MiddleActor
    {
        PrivateChildPark!BatchChild* children;
        ulong* slots;
    }
    void middleStep(scope ref PrivateBorrow!MiddleActor b, scope ref PrivateContext ctx)
        nothrow @nogc @system
    {
        enum grandchildren = 50;
        // No explicit limit and no saved context: inherit this cohort's 5,
        // not the root configuration (31).
        b.value.children = ctx.createChildren!BatchChild(grandchildren);
        check(b.value.children !is null, "inherited grandchild create", 0);
        foreach (i; 0 .. grandchildren)
            check(b.value.children.spawn(ctx, BatchChild(b.value.slots + i))
                == PrivateSpawnResult.spawned, "grandchild spawn", 0);
        check(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.started,
            "grandchild start", 0);
    }
    struct InheritRoot
    {
        PrivateChildPark!MiddleActor* middle;
        ulong* slots;
    }
    PrivateRootDisposition inheritRootStep(scope ref PrivateBorrow!InheritRoot b,
        scope ref PrivateContext ctx) nothrow @nogc @system
    {
        enum grandchildren = 50;
        if (b.value.middle is null)
        {
            b.value.middle = ctx.createChildren!MiddleActor(1, 5);
            check(b.value.middle !is null, "middle create", 0);
            check(b.value.middle.spawn(ctx, MiddleActor(null, b.value.slots))
                == PrivateSpawnResult.spawned, "middle spawn", 0);
            check(b.value.middle.start!middleStep(ctx) == PrivateStartResult.started,
                "middle start", 0);
            return PrivateRootDisposition.again;
        }
        if (!b.value.middle.finished(ctx)) return PrivateRootDisposition.again;
        foreach (i; 0 .. grandchildren)
            check(b.value.slots[i] == 1, "grandchild joined", 0);
        return PrivateRootDisposition.finish;
    }
    void inheritedChildLimit()
    {
        enum grandchildren = 50;
        enum parentLimit = 5;
        enum tables = (grandchildren + parentLimit - 1) / parentLimit;
        static assert(tables == 10);
        ulong[grandchildren] slots;
        // Root limit 31 is what old root-only inheritance would emit.
        drainCounted!(InheritRoot, inheritRootStep)(31, slots[], parentLimit, tables,
            "inherited grandchild table count");
        writeln("PASS inherited child limit 5 emits 10 grandchild tables");
    }
}
void configuredBatches()
{
    foreach (limit; [1UL, 255, 256, 257, 1024, 4096])
    {
        // There is no positive capacity below one; capacity zero is rejected below.
        roots(0, limit == 1 ? 1 : limit - 1, 2, limit);
        roots(2, limit + 3, 2, limit);
    }
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    check(Park.create(farm, 0, 1) is null);
    check(Park.create(farm, size_t.max, 1) is null);
    // Root storage is min(configured limit, capacity). A huge configured limit
    // must create, pump, collect, drain and destroy; see hugeConfiguredLimits.
    hugeConfiguredLimits();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    enum count = 1025;
    ulong[count] outputs;
    auto park = Park.create(farm, count, 4096);
    check(park !is null);
    foreach (i; 0 .. count)
        check(park.spawn(State(&outputs[i], 1, 0)) == PrivateSpawnResult.spawned);
    auto admitted = park.pump(token);
    check(admitted > 0 && admitted < count); // Farm quota splits the configured batch.
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        while (view.consumeNext()) {}
        park.pump(token);
        enforce(MonoTime.currTime < deadline, "configured batch drain timeout");
    }
    foreach (value; outputs) check(value == 1);
    park.destroy();

    // A non-default child limit spans multiple Farm tables, including partial prefixes.
    enum childCount = 1537;
    ulong[childCount] childOutputs;
    auto parent = PrivateRootPark!(BatchParent, batchParentStep).create(farm, 1, 257);
    check(parent !is null);
    check(parent.spawn(BatchParent(null, childOutputs.ptr, childCount, 1024, 0)) == PrivateSpawnResult.spawned);
    auto workers = new Workers(farm, 1);
    deadline = MonoTime.currTime + 30.seconds;
    while (parent.live)
    {
        parent.pump(token);
        enforce(MonoTime.currTime < deadline, "large child cohort timeout");
    }
    workers.join();
    foreach (value; childOutputs) check(value == 3);
    parent.destroy();
}
void rootBatchDeath(bool createZero, bool overLimit)
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    auto park = Park.create(farm, 1, createZero ? 0 : 257);
    check(park !is null);
    park.pump(token, overLimit ? 258 : 0);
}
PrivateRootDisposition childBatchZero(scope ref PrivateBorrow!State, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    ctx.createChildren!State(1, 0);
    return PrivateRootDisposition.finish;
}
void childBatchDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(State, childBatchZero).create(farm, 1);
    check(park !is null);
    check(park.spawn(State.init) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}

enum AuthorityMisuse { spawn, start, finished, defaultFinished, defaultCreate }
struct AuthorityProbe { PrivateChildPark!State* children; AuthorityMisuse misuse; }
struct AuthorityRoot { AuthorityProbe* probe; bool owner; }
void authorityOperation(scope ref PrivateBorrow!State, scope ref PrivateContext)
    nothrow @nogc @system {}
PrivateRootDisposition authorityStep(scope ref PrivateBorrow!AuthorityRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto probe = b.value.probe;
    if (b.value.owner)
    {
        if (probe.children is null) probe.children = ctx.createChildren!State(1);
        check(probe.children !is null);
        return PrivateRootDisposition.again;
    }
    check(probe.children !is null);
    PrivateContext empty;
    final switch (probe.misuse)
    {
        case AuthorityMisuse.spawn: probe.children.spawn(ctx, State.init); break;
        case AuthorityMisuse.start: probe.children.start!authorityOperation(ctx); break;
        case AuthorityMisuse.finished: probe.children.finished(ctx); break;
        case AuthorityMisuse.defaultFinished: probe.children.finished(empty); break;
        case AuthorityMisuse.defaultCreate: empty.createChildren!State(1); break;
    }
    return PrivateRootDisposition.finish;
}
void authorityDeath(AuthorityMisuse misuse)
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    AuthorityProbe probe;
    probe.misuse = misuse;
    auto park = PrivateRootPark!(AuthorityRoot, authorityStep).create(farm, 2);
    check(park !is null);
    check((park.spawn(AuthorityRoot(&probe, true))) == PrivateSpawnResult.spawned);
    check((park.spawn(AuthorityRoot(&probe, false))) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
    // Reaching this point means the fatal contract was not enforced.
}

version (assert)
{
    __gshared Park* reentrantPark;
    void* reentrantAllocate(void*, size_t bytes, size_t alignment) nothrow @nogc @system
    {
        if (reentrantPark !is null) reentrantPark.spawn(State.init);
        return ActorAllocator.cRuntime().allocate(null, bytes, alignment);
    }
    void reentrantDeath()
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto allocator = ActorAllocator.cRuntime();
        allocator.allocate = &reentrantAllocate;
        reentrantPark = Park.create(farm, 1, defaultPrivateBatchLimit, allocator);
        check(reentrantPark !is null);
        reentrantPark.spawn(State.init);
    }
}

enum fatalSamplePrefix = "fatal-sample:";
enum deathSampleText = "private death harness sample";

version (linux)
{
    struct DeathProbe
    {
        int status;       // exit code, or -signal when the child was killed
        string stderrText;
    }

    // Keep stderr separate: stdout cannot satisfy the fatal-message check.
    DeathProbe probeDeath(string mode)
    {
        auto pipes = pipeProcess(["/proc/self/exe", mode], Redirect.stderr);
        string text;
        foreach (line; pipes.stderr.byLine)
            text ~= line ~ "\n";
        auto status = wait(pipes.pid);
        return DeathProbe(status, text);
    }

    bool diedWithMessage(string mode, string expected)
    {
        auto probe = probeDeath(mode);
        return probe.status != 0 && probe.stderrText.canFind(expected);
    }


}

void deathHarness()
{
    version (linux)
    {
        auto mode = fatalSamplePrefix ~ deathSampleText;
        auto expected = "antfarm fatal: " ~ deathSampleText;
        check(diedWithMessage(mode, expected));
        writeln("PASS fatal sample dies nonzero with expected stderr message");
        foreach (modeName; ["wrong-parent-spawn", "wrong-parent-start", "wrong-parent-finished",
            "default-finished", "default-create"])
            check(diedWithMessage(modeName, "antfarm fatal: private child park: unauthorized context"));
        writeln("PASS wrong-parent and default authority are fatal");
        foreach (modeName; ["batch-create-zero", "batch-pump-zero", "batch-pump-over"])
            check(diedWithMessage(modeName, "antfarm fatal: invalid private root batch limit"));
        check(diedWithMessage("batch-child-zero", "antfarm fatal: invalid private child batch limit"));
        writeln("PASS invalid configured and per-call batch limits are fatal");
        version (assert)
        {
            check(diedWithMessage("reentrant-spawn", "antfarm fatal: private root park: reentrant entry"));
            writeln("PASS debug reentrancy guard");
        }
    }
    else
        writeln("SKIP death harness: /proc/self/exe re-exec requires Linux");
}

void deathNegativeControl()
{
    version (linux)
    {
        auto expected = "antfarm fatal: " ~ deathSampleText;
        // The same predicate the positive harness relies on must reject a
        // survivor (exit zero) and a nonzero death with the wrong message.
        check(!diedWithMessage("death-exits-zero", expected));
        check(!diedWithMessage(fatalSamplePrefix ~ "private death harness wrong text", expected));
        writeln("PASS death negative control: survivor and wrong-message child rejected");
    }
    else
        writeln("SKIP death negative control: /proc/self/exe re-exec requires Linux");
}

void runSuite()
{
    configuredBatches(); writeln("PASS configured root and child limits, overflow and quota splitting");
    version (AntfarmWriteAuditHooks)
    {
        explicitChildLimit();
        inheritedChildLimit();
    }
    fifoProgress(); writeln("PASS bounded FIFO progress");
    version (AntfarmWriteAuditHooks)
    {
        earlyCompletion();
        trees(0);
        writeln("PASS completion/free before publication returns, including nested waves");
    }
    failures(); writeln("PASS allocation failures");
    backpressure(); writeln("PASS partial-prefix/zero-admission recovery");
    sameShardJoin(); writeln("PASS same-shard plain-write completion join");
    roots(0, 4097, 3, 256); writeln("PASS roots single thread, partial tables and reuse");
    roots(1, 4097, 7, 17); writeln("PASS roots one consumer, small batches");
    roots(6, 16385, 7, 256); writeln("PASS roots six consumers, ring reuse/backpressure");
    roots!(PrivateRootReclamation.consumer)(6, 4097, 3, 256);
    writeln("PASS optional consumer-side root reclamation");
    trees(); writeln("PASS nested waves, authority, retirement, allocation balance");
    shardedCreators(); writeln("PASS four independent creators sharing the Farm");
}

void main(string[] args)
{
    auto mode = args.length > 1 ? args[1] : "";
    switch (mode)
    {
        case "batch-create-zero": rootBatchDeath(true, false); return;
        case "batch-pump-zero": rootBatchDeath(false, false); return;
        case "batch-pump-over": rootBatchDeath(false, true); return;
        case "batch-child-zero": childBatchDeath(); return;
        case "reentrant-spawn":
            version (assert) reentrantDeath();
            return;
        case "wrong-parent-spawn": authorityDeath(AuthorityMisuse.spawn); return;
        case "wrong-parent-start": authorityDeath(AuthorityMisuse.start); return;
        case "wrong-parent-finished": authorityDeath(AuthorityMisuse.finished); return;
        case "default-finished": authorityDeath(AuthorityMisuse.defaultFinished); return;
        case "default-create": authorityDeath(AuthorityMisuse.defaultCreate); return;
        case "huge-root-limit": hugeConfiguredLimits(); return;
        case "explicit-child-limit":
            version (AntfarmWriteAuditHooks) explicitChildLimit();
            return;
        case "inherited-child-limit":
            version (AntfarmWriteAuditHooks) inheritedChildLimit();
            return;
        case "join-only": sameShardJoin(); return;
        case "death": deathHarness(); return;
        case "death-negative-control": deathNegativeControl(); return;
        case "death-exits-zero":
            fprintf(stderr, "antfarm fatal: private death harness sample\n");
            return; // right message, wrong exit status
        default: break;
    }
    if (mode.length >= fatalSamplePrefix.length
        && mode[0 .. fatalSamplePrefix.length] == fatalSamplePrefix)
    {
        fatal(mode[fatalSamplePrefix.length .. $]);
        return;
    }
    runSuite();
}
