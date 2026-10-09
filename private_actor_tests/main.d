module private_actor_tests;
import actors;
import antfarm;
import antfarm_allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime, msecs, seconds, usecs;
import core.stdc.stdlib : abort;
import core.stdc.stdio : fprintf, stderr;
import std.algorithm.searching : canFind;
import std.process : Redirect, pipeProcess, wait;
import std.stdio : writeln;
import std.exception : enforce;
import std.traits : hasNested;

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
    version (TSan) enum rounds = 2;
    else enum rounds = 8;
    foreach (round; 0 .. rounds)
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
    version (TSan) enum rounds = 3;
    else enum rounds = 12;
    foreach (round; 0 .. rounds)
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
            version (TSan) enum rounds = 4;
            else enum rounds = 20;
            foreach (round; 0 .. rounds)
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
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 713);
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
    // Compact ST geometry (SPEC 2f): the 713-word quota admits 225 of a
    // 1025-member batch, so a single pump cannot take the whole batch.
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

// ---- Cross-Farm token provenance deaths -------------------------------------
//
// A Token is bound to the Farm that issued it: write, flush, and unregister
// on any other Farm must be fatal in debug and -release. These are child
// bodies for the death harness; each must abort before any cleanup runs.

void crossFarmWriteDeath()
{
    auto farmA = AntFarm.create();
    auto farmB = AntFarm.create();
    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
    check(tokenA.valid && tokenB.valid);
    // requireToken runs before payload handling, so even an empty write
    // must reject the foreign ticket.
    farmA.write(cast(PayloadEntry[]) null, tokenB);
    // Reaching this point means the fatal contract was not enforced.
}

void crossFarmUnregisterDeath()
{
    auto farmA = AntFarm.create();
    auto farmB = AntFarm.create();
    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
    check(tokenA.valid && tokenB.valid);
    farmA.unregisterProducer(tokenB);
    // Reaching this point means the fatal contract was not enforced.
}

struct CrossFarmFlushState { uint* completed; }
void crossFarmFlushWork(scope ref ActorBorrow!CrossFarmFlushState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++*actor.value.completed;
}

// The originally reproduced probe: one small token registered on each of
// two new Farms, an actor woken on Farm A's runtime, then flush() handed
// Farm B's token.
void crossFarmFlushDeath()
{
    auto farmA = AntFarm.create();
    auto farmB = AntFarm.create();
    auto tokenA = farmA.registerProducer(Tier.small);
    auto tokenB = farmB.registerProducer(Tier.small);
    check(tokenA.valid && tokenB.valid);
    auto runtime = ActorRuntime.create(farmA, 1);
    check(runtime !is null);
    uint completed;
    auto owner = runtime.createActor!(CrossFarmFlushState, crossFarmFlushWork)(
        CrossFarmFlushState(&completed));
    check(owner.valid);
    owner.handle.wake();
    runtime.flush(tokenB); // Deliberately wrong Farm's token.
    // Reaching this point means the fatal contract was not enforced.
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

// ---- F4a: public inbox-node fatal contracts ---------------------------------
// Child modes are "node-death:<name>". The parent (nodeDeath, run by
// deathHarness in LDC/DMD debug and -release) requires a nonzero death, the
// exact fatal text, and that no "survived" marker was printed. Every misuse
// is checked on a node whose preceding legal call succeeded (or on a node in a
// distinct non-processing state), so a death for the wrong reason cannot pass.
//
// Mutations caught:
//  - drop the `code > 0xFF_FFFF` check in ActorInboxNode.fail (oversize);
//  - drop the processing-state check in fail/cancel (free, queued);
//  - make a terminal call idempotent or reopen the node (double).
enum NodeDeathAct : ubyte { complete, fail, cancel }
enum NodeDeathMode : ubyte { oversize, twice }

struct NodeDeathState { NodeDeathMode mode; NodeDeathAct first, second; }

void nodeDeathApply(ActorInboxNode* node, NodeDeathAct act) nothrow @nogc @system
{
    final switch (act)
    {
        case NodeDeathAct.complete: node.complete(); break;
        case NodeDeathAct.fail: node.fail(3); break;
        case NodeDeathAct.cancel: node.cancel(); break;
    }
}

void nodeDeathActor(scope ref ActorBorrow!NodeDeathState actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    final switch (actor.value.mode)
    {
        case NodeDeathMode.oversize:
            auto a = context.popInbox();
            auto b = context.popInbox();
            if (a is null || b is null) return;
            a.fail(0xFF_FFFF);
            fprintf(stderr, "node death: fail 0xFFFFFF accepted\n");
            b.fail(1 << 24);
            fprintf(stderr, "node death: survived oversize fail\n");
            break;
        case NodeDeathMode.twice:
            auto node = context.popInbox();
            if (node is null) return;
            nodeDeathApply(node, actor.value.first);
            nodeDeathApply(node, actor.value.second);
            fprintf(stderr, "node death: survived second terminal call\n");
            break;
    }
}

string nodeDeathActName(NodeDeathAct act) nothrow pure @safe
{
    final switch (act)
    {
        case NodeDeathAct.complete: return "complete";
        case NodeDeathAct.fail: return "fail";
        case NodeDeathAct.cancel: return "cancel";
    }
}

void nodeDeathRun(NodeDeathState state, bool sendTwo)
{
    auto farm = AntFarm.create();
    check(farm !is null);
    auto token = farm.registerProducer(Tier.small);
    check(token.valid);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto runtime = ActorRuntime.create(farm, 1);
    check(runtime !is null);
    auto owner = runtime.createActor!(NodeDeathState, nodeDeathActor)(state);
    check(owner.valid);
    ActorInboxNode[2] nodes;
    foreach (ref node; nodes) node.initialize();
    check(owner.handle.send(&nodes[0]) == ActorSendResult.queued);
    if (sendTwo)
    {
        // The actor is already signalled, so the second node coalesces.
        auto result = owner.handle.send(&nodes[1]);
        check(result == ActorSendResult.queued || result == ActorSendResult.coalesced);
    }
    foreach (_; 0 .. 64)
    {
        runtime.flush(token);
        while (view.consumeNext()) {}
    }
    fprintf(stderr, "node death: survived pump\n");
}

// Standalone terminal misuse on a node that is not processing.
void nodeDeathNotProcessing(NodeDeathAct act, bool queued)
{
    ActorInboxNode node;
    node.initialize();
    if (queued)
    {
        // Queued (claimed and published) but never popped: still not the
        // actor's to complete.
        auto farm = AntFarm.create();
        check(farm !is null);
        auto runtime = ActorRuntime.create(farm, 1);
        check(runtime !is null);
        auto owner = runtime.createActor!(NodeDeathState, nodeDeathActor)(
            NodeDeathState.init);
        check(owner.valid);
        check(owner.handle.send(&node) == ActorSendResult.queued);
        check(!node.available);
        nodeDeathApply(&node, act);
    }
    else
        nodeDeathApply(&node, act);
    fprintf(stderr, "node death: survived not-processing call\n");
}

void nodeDeathChild(string name)
{
    if (name == "oversize")
    {
        nodeDeathRun(NodeDeathState(NodeDeathMode.oversize), true);
        return;
    }
    foreach (act; [NodeDeathAct.complete, NodeDeathAct.fail, NodeDeathAct.cancel])
    {
        if (name == nodeDeathActName(act) ~ "-free")
        { nodeDeathNotProcessing(act, false); return; }
        if (name == nodeDeathActName(act) ~ "-queued")
        { nodeDeathNotProcessing(act, true); return; }
        foreach (second; [NodeDeathAct.complete, NodeDeathAct.fail, NodeDeathAct.cancel])
            if (name == "twice-" ~ nodeDeathActName(act) ~ "-" ~ nodeDeathActName(second))
            {
                nodeDeathRun(NodeDeathState(NodeDeathMode.twice, act, second), false);
                return;
            }
    }
    fprintf(stderr, "node death: unknown mode\n");
}

void nodeDeath()
{
    version (linux)
    {
        auto oversize = probeDeath("node-death:oversize");
        check(oversize.status != 0);
        check(oversize.stderrText.canFind("node death: fail 0xFFFFFF accepted"));
        check(oversize.stderrText.canFind("antfarm fatal: actor inbox node failure code out of range"));
        check(!oversize.stderrText.canFind("survived"));
        auto acts = [NodeDeathAct.complete, NodeDeathAct.fail, NodeDeathAct.cancel];
        foreach (act; acts)
        {
            auto text = "antfarm fatal: " ~ nodeDeathActName(act) ~ " actor inbox node not owned by actor";
            foreach (where; ["-free", "-queued"])
            {
                auto probe = probeDeath("node-death:" ~ nodeDeathActName(act) ~ where);
                check(probe.status != 0);
                check(probe.stderrText.canFind(text));
                check(!probe.stderrText.canFind("survived"));
            }
            foreach (second; acts)
            {
                // The second terminal call is the one that must die, with its own name.
                auto twiceText = "antfarm fatal: " ~ nodeDeathActName(second) ~ " actor inbox node not owned by actor";
                auto probe = probeDeath("node-death:twice-" ~ nodeDeathActName(act) ~ "-" ~ nodeDeathActName(second));
                check(probe.status != 0);
                check(probe.stderrText.canFind(twiceText));
                check(!probe.stderrText.canFind("survived"));
            }
        }
        writeln("PASS node death: oversize fail code, non-processing and repeated terminal calls are fatal");
    }
    else
        writeln("SKIP node death: /proc/self/exe re-exec requires Linux");
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
        portsDeath();
        nodeDeath();
        dtorDeath();
        subsetDeath();
        version (AntfarmActorTestHooks) entryWordDeath();
        foreach (modeName; ["batch-create-zero", "batch-pump-zero", "batch-pump-over"])
            check(diedWithMessage(modeName, "antfarm fatal: invalid private root batch limit"));
        check(diedWithMessage("batch-child-zero", "antfarm fatal: invalid private child batch limit"));
        writeln("PASS invalid configured and per-call batch limits are fatal");
        check(diedWithMessage("destroy-live", "antfarm fatal: destroy nonempty private root park"));
        check(diedWithMessage("finish-with-active-child", "antfarm fatal: retire private actor with active child wave"));
        writeln("PASS destroy-live and finish-with-active-child are fatal");
        foreach (modeName; ["cross-farm-write", "cross-farm-flush"])
            check(diedWithMessage(modeName, "antfarm fatal: write token from another farm"));
        check(diedWithMessage("cross-farm-unregister", "antfarm fatal: unregister token from another farm"));
        writeln("PASS cross-Farm token write, flush, and unregister are fatal");
        e1CounterExhaustionDeath();
        contDeath();
        check(diedWithMessage("outcome-active", "antfarm fatal: private child outcome: wave active"));
        check(diedWithMessage("root-fail-untracked", "antfarm fatal: private root fail: untracked park"));
        check(diedWithMessage("failure-overflow", "antfarm fatal: private failure count overflow"));
        check(diedWithMessage("fail-unauthorized", "antfarm fatal: private fail: unauthorized context"));
        check(diedWithMessage("outcome-unauthorized", "antfarm fatal: private child park: unauthorized context"));
        writeln("PASS outcome active, untracked root fail, failure overflow, and unauthorized fail/outcome");
        cancelDeath();
        rootTrackingDeath();
        slabDeath();
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

// Phase D controls and outputs belong to the test, never to borrowed actor state.
void namedCheck(bool ok, string name) nothrow @nogc @system
{ check(ok, name, 0); }
void awaitRaw(ref shared uint flag, uint value, string name) nothrow @nogc @system
{
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad!(MemoryOrder.raw)(flag) != value)
    {
        namedCheck(MonoTime.currTime < deadline, name);
        pause();
    }
}
version (AntfarmActorTestHooks)
{
    // Raw flags deliberately do not join callback result writes. The library
    // return queue / wave idle acquire must provide those happens-before edges.
    struct PrivateHookControl
    {
        shared uint armed, arrived, release, calls, creditCalls;
        ActorTestPoint point;
    }
    __gshared PrivateHookControl* privateControl;
    void privateHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        auto c = privateControl;
        if (point == ActorTestPoint.privateWaveCreditReleased)
            atomicFetchAdd!(MemoryOrder.raw)(c.creditCalls, 1u);
        if (point != c.point) return;
        atomicFetchAdd!(MemoryOrder.raw)(c.calls, 1u);
        if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&c.armed, 1u, 0u)) return;
        atomicStore!(MemoryOrder.raw)(c.arrived, 1u);
        awaitRaw(c.release, 1u, "private hook release timeout");
    }
}

struct CohortControl { ulong* results; size_t count; uint completed, busy; }
struct CohortRoot { PrivateChildPark!BatchChild* children; CohortControl* control; uint phase; }
PrivateRootDisposition cohortStep(scope ref PrivateBorrow!CohortRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.control;
    if (b.value.children is null)
    {
        b.value.children = ctx.createChildren!BatchChild(c.count);
        namedCheck(b.value.children !is null, "cohort create");
        foreach (i; 0 .. c.count)
            namedCheck(b.value.children.spawn(ctx, BatchChild(c.results + i)) == PrivateSpawnResult.spawned,
                "cohort spawn");
        namedCheck(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.started,
            "cohort first start");
        b.value.phase = 1;
        return PrivateRootDisposition.again;
    }
    if (!b.value.children.finished(ctx))
    {
        ++c.busy;
        return PrivateRootDisposition.again;
    }
    foreach (i; 0 .. c.count) namedCheck(c.results[i] == b.value.phase, "cohort exactly once per generation");
    c.completed = b.value.phase;
    if (b.value.phase == 3) return PrivateRootDisposition.finish;
    auto result = b.value.phase == 2 ? b.value.children.startFinal!batchChildOperation(ctx)
        : b.value.children.start!batchChildOperation(ctx);
    namedCheck(result == PrivateStartResult.started, "cohort reuse start");
    ++b.value.phase;
    return PrivateRootDisposition.again;
}

void childLargeCohort(size_t count)
{
    Counts counts;
    // Compact ST geometry (SPEC 2f, one-word bodies: 38 + 3*n words): a fresh
    // 713-word quota admits 225 of a requested 256 children and leaves zero,
    // so the following 41-word root needs writeImpl's automatic quota refresh.
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 713);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto results = new ulong[count];
    CohortControl control = CohortControl(results.ptr, count);
    auto parent = PrivateRootPark!(CohortRoot, cohortStep).create(farm, 1, 256, policy(counts));
    check(parent !is null);
    check(parent.spawn(CohortRoot(null, &control)) == PrivateSpawnResult.spawned);
    check(parent.pump(token) == 1);
    while (view.consumeNext()) {}

    enum fillCount = 100000;
    auto fillResults = new ulong[fillCount];
    auto filler = Park.create(farm, fillCount, 256);
    check(filler !is null);
    foreach (i; 0 .. fillCount) check(filler.spawn(State(fillResults.ptr + i, 1, 0)) == PrivateSpawnResult.spawned);
    size_t admitted;
    auto deadline = MonoTime.currTime + 30.seconds;
    while (true)
    {
        auto n = filler.pump(token);
        if (n == 0) break;
        admitted += n;
        namedCheck(MonoTime.currTime < deadline, "cohort prefill timeout");
    }
    namedCheck(admitted > 0 && admitted < fillCount, "cohort Farm prefilled");
    version (AntfarmActorTestHooks)
    {
        PrivateHookControl hook;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        // Mutation: delete `if (written == 0) releaseWave(wave);` in emit.
        // A rejected child table must release its speculative table credit.
        namedCheck(parent.pump(token) == 0, "full Farm rejects child and polling root tables");
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.creditCalls) == 1, "child zero admission credit released");
        setActorTestHook(null);
        privateControl = null;
    }
    else namedCheck(parent.pump(token) == 0, "full Farm rejects child and polling root tables");
    foreach (value; results) namedCheck(value == 0, "zero admission invokes no child");
    // Drain the filler independently so the pending child gets a known fresh quota.
    while (filler.live)
    {
        while (view.consumeNext()) {}
        filler.pump(token);
        namedCheck(MonoTime.currTime < deadline, "cohort prefill drain timeout");
    }
    filler.destroy();
    // All filler tables are drained and the sole view has released its confirmed
    // prefix, so the next segment ahead is free (32768 words > Exmax=713).
    // The child table is smaller than a segment; crossing into the next segment
    // still leaves its following segment free. Make root admission explicit
    // before relying on its live callback's deterministic busy observation.
    namedCheck(parent.pump(token) == 1, "partial child polling root admission after quota refresh");
    while (view.consumeNext()) {}
    size_t prefix;
    foreach (value; results) { namedCheck(value <= 1, "partial child prefix no duplicates"); prefix += value; }
    // Mutation: replace `park.cursor_ += written;` with `park.cursor_ += n;`.
    namedCheck(prefix > 0 && prefix < 256 && prefix < count, "child partial prefix below configured limit");
    namedCheck(control.completed == 0 && control.busy > 0, "partial child wave stays active");
    while (parent.live)
    {
        parent.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "child-large-cohort retry timeout");
    }
    foreach (value; results) namedCheck(value == 3, "child-large-cohort three exactly-once waves");
    namedCheck(control.completed == 3, "child-large-cohort terminal join");
    parent.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "child-large-cohort allocation balance");
}

struct AllocRoot { Counts* counts; ulong* result; }
PrivateRootDisposition childAllocStep(scope ref PrivateBorrow!AllocRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.counts;
    auto allocated = atomicLoad(c.allocated);
    auto freed = atomicLoad(c.freed);
    namedCheck(ctx.createChildren!BatchChild(0) is null, "child zero capacity");
    namedCheck(ctx.createChildren!BatchChild(size_t.max) is null, "child capacity overflow");
    namedCheck(atomicLoad(c.allocated) == allocated, "invalid child capacity allocates nothing");
    foreach (budget; 0 .. 2)
    {
        atomicStore(c.budget, cast(long) budget);
        namedCheck(ctx.createChildren!BatchChild(1) is null, "child object/entry allocation failure");
        namedCheck(atomicLoad(c.allocated) - allocated == atomicLoad(c.freed) - freed,
            "failed child create allocation balance");
    }
    atomicStore(c.budget, long.max);
    auto children = ctx.createChildren!BatchChild(1);
    namedCheck(children !is null, "child create after failure");
    atomicStore(c.budget, 0L);
    namedCheck(children.spawn(ctx, BatchChild(b.value.result)) == PrivateSpawnResult.allocationFailed,
        "child state allocation failure");
    namedCheck(children.finished(ctx), "failed child spawn stays idle");
    namedCheck(children.start!batchChildOperation(ctx) == PrivateStartResult.empty, "failed slot stays empty");
    atomicStore(c.budget, long.max);
    namedCheck(children.spawn(ctx, BatchChild(b.value.result)) == PrivateSpawnResult.spawned, "failed child slot reusable");
    namedCheck(children.spawn(ctx, BatchChild(b.value.result)) == PrivateSpawnResult.capacityFull, "recovered child capacity full");
    return PrivateRootDisposition.finish; // idle member disposal is also balanced
}
void childAllocationFailures()
{
    Counts counts;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    ulong result;
    auto park = PrivateRootPark!(AllocRoot, childAllocStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(AllocRoot(&counts, &result)) == PrivateSpawnResult.spawned);
    check(park.pump(token) == 1);
    while (view.consumeNext()) {}
    park.collectCompleted();
    namedCheck(park.live == 0 && result == 0, "child failure root retires without dispatch");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "child failures total allocation balance");
}

void mixedTable(PrivateRootReclamation reclamation)(bool early)
{
    Counts counts;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(State, repeat, reclamation).create(farm, 2, 2, policy(counts));
    check(park !is null);
    ulong[2] result;
    check(park.spawn(State(&result[0], 1, 0)) == PrivateSpawnResult.spawned);
    check(park.spawn(State(&result[1], 2, 0)) == PrivateSpawnResult.spawned);
    auto freed = atomicLoad(counts.freed);
    version (AntfarmWriteAuditHooks)
    {
        if (early) { fastView = &view; earlyTables = 0; writeAuditHook = &consumeBeforeReturn; }
    }
    // Mutation: replace `batch.accepted = n;` with `batch.accepted = 0;`
    // in prepareTableFrontN. A fully completed mixed table then loses both roots.
    check(park.pump(token) == 2);
    version (AntfarmWriteAuditHooks)
    {
        if (early)
        {
            writeAuditHook = null; fastView = null;
            namedCheck(earlyTables == 1, "mixed table completed before write returned");
        }
    }
    if (!early) while (view.consumeNext()) {}
    namedCheck(result[0] == 1 && result[1] == 1 && park.live == 2, "mixed table live waits for collection");
    static if (reclamation == PrivateRootReclamation.consumer)
        namedCheck(atomicLoad(counts.freed) == freed + 1, "mixed consumer frees only terminal root");
    else
        namedCheck(atomicLoad(counts.freed) == freed, "mixed emitter defers root free");
    park.collectCompleted();
    namedCheck(park.live == 1 && atomicLoad(counts.freed) == freed + 1, "mixed table one terminal collected");
    check(park.spawn(State(&result[0], 2, 1)) == PrivateSpawnResult.spawned);
    check(park.pump(token) == 2);
    while (view.consumeNext()) {}
    park.collectCompleted();
    namedCheck(park.live == 0 && result[0] == 2 && result[1] == 2, "mixed continuing root requeued and slot reused");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "mixed allocation balance");
}

struct RefillControl { ulong result; shared uint busyObserved, observeBusy; }
struct RefillRoot { PrivateChildPark!BatchChild* children; RefillControl* control; uint phase; }
PrivateRootDisposition refillStep(scope ref PrivateBorrow!RefillRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.control;
    if (b.value.children is null)
    {
        b.value.children = ctx.createChildren!BatchChild(1);
        namedCheck(b.value.children !is null, "refill create");
        namedCheck(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.empty, "initial empty start");
        namedCheck(b.value.children.startFinal!batchChildOperation(ctx) == PrivateStartResult.empty, "initial empty final");
        namedCheck(b.value.children.finished(ctx), "empty starts remain idle");
        namedCheck(b.value.children.spawn(ctx, BatchChild(&c.result)) == PrivateSpawnResult.spawned, "refill initial spawn");
        namedCheck(b.value.children.startFinal!batchChildOperation(ctx) == PrivateStartResult.started, "refill first final");
        b.value.phase = 1;
        return PrivateRootDisposition.again;
    }
    bool finished = b.value.children.finished(ctx);
    if (b.value.phase == 1 && atomicLoad!(MemoryOrder.raw)(c.observeBusy) != 0)
    {
        namedCheck(!finished, "delayed idle: authorized parent must still see active");
        namedCheck(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.waveActive,
            "delayed idle rejects overlapping wave");
        atomicStore!(MemoryOrder.raw)(c.busyObserved, 1u);
        return PrivateRootDisposition.again;
    }
    if (!finished) return PrivateRootDisposition.again;
    namedCheck(c.result == b.value.phase, "refill joined plain result");
    if (b.value.phase == 3) return PrivateRootDisposition.finish;
    if (b.value.phase == 1)
    {
        // Mutation: delete `if (park.retire_) park.count_ = 0;` in finish.
        namedCheck(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.empty, "post-final empty start");
        namedCheck(b.value.children.finished(ctx), "post-final empty start remains idle");
        namedCheck(b.value.children.spawn(ctx, BatchChild(&c.result)) == PrivateSpawnResult.spawned, "refill terminal slot");
        namedCheck(b.value.children.start!batchChildOperation(ctx) == PrivateStartResult.started, "refill normal wave");
    }
    else
        namedCheck(b.value.children.startFinal!batchChildOperation(ctx) == PrivateStartResult.started, "refill second final");
    ++b.value.phase;
    return PrivateRootDisposition.again;
}
void refillAfterFinal()
{
    Counts counts;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    RefillControl c;
    auto park = PrivateRootPark!(RefillRoot, refillStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(RefillRoot(null, &c)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "refill-after-final timeout");
    }
    namedCheck(c.result == 3, "refill generations exactly once");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "refill allocation balance");
}

version (AntfarmActorTestHooks)
{
    void delayedFinalCredit()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        RefillControl c;
        auto park = PrivateRootPark!(RefillRoot, refillStep).create(farm, 1, 256, policy(counts));
        check(park !is null);
        check(park.spawn(RefillRoot(null, &c)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {}
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateWaveBeforeIdlePublish;
        hook.armed = 1;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        shared uint ready, go, leave;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "delayed idle worker start timeout");
            auto deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad!(MemoryOrder.raw)(hook.arrived) == 0)
            {
                other.consumeNext();
                namedCheck(MonoTime.currTime < deadline, "delayed idle child claim timeout");
            }
            awaitRaw(leave, 1u, "delayed idle retirement before worker join timeout");
        });
        worker.start();
        awaitRaw(ready, 1u, "delayed idle subscriber timeout");
        // SqCs remains one with two subscribed views: each singleton costs
        // 41 compact words (SPEC 2f). Initial root + child + polling root total
        // only 123 of 4096.
        namedCheck(park.pump(token) == 1, "delayed idle polling root admission");
        atomicStore!(MemoryOrder.raw)(go, 1u);
        awaitRaw(hook.arrived, 1u, "delayed idle final-credit hook timeout");
        atomicStore!(MemoryOrder.raw)(c.observeBusy, 1u);
        // Mutation: move the wave.active release-store above privateWaveBeforeIdlePublish.
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.raw)(c.busyObserved) == 0)
        {
            view.consumeNext();
            namedCheck(MonoTime.currTime < deadline, "delayed idle authorized observation timeout");
        }
        atomicStore!(MemoryOrder.raw)(c.observeBusy, 0u);
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "delayed idle immediate refill/retirement timeout");
        }
        namedCheck(c.result == 3, "delayed idle reused and retired before worker join");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "delayed idle allocation balance");
    }

    void returnCasRetry()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        // capacity two / batch limit one creates exactly two descriptor slots.
        auto park = Park.create(farm, 2, 1, policy(counts));
        check(park !is null);
        ulong[2] results;
        foreach (i; 0 .. 2) check(park.spawn(State(&results[i], 1, 0)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        check(park.pump(token) == 1);
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateRootReturnBeforeCas;
        hook.armed = 1;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        shared uint ready, go, leave, published;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "return CAS worker start timeout");
            auto deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad!(MemoryOrder.raw)(hook.arrived) == 0)
            {
                other.consumeNext();
                namedCheck(MonoTime.currTime < deadline, "return CAS first claim timeout");
            }
            atomicStore!(MemoryOrder.raw)(published, 1u);
            awaitRaw(leave, 1u, "return CAS collection before worker join timeout");
        });
        worker.start();
        awaitRaw(ready, 1u, "return CAS subscriber timeout");
        atomicStore!(MemoryOrder.raw)(go, 1u);
        awaitRaw(hook.arrived, 1u, "return CAS first head load timeout");
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.raw)(hook.calls) < 2)
        {
            view.consumeNext();
            namedCheck(MonoTime.currTime < deadline, "return CAS competing publication timeout");
        }
        // Mutation: replace the retry head reload with `return;` in rootsCompleted.
        // The paused CAS has the old null head; table two wins, forcing a retry.
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        // Do not detach table two until the paused publisher has retried:
        // otherwise exchange(null) could restore its original expected head.
        awaitRaw(published, 1u, "return CAS retry publication timeout");
        while (park.live)
        {
            park.collectCompleted();
            namedCheck(MonoTime.currTime < deadline, "return CAS both tables collected timeout");
        }
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 3, "return CAS exactly one retry");
        namedCheck(results[0] == 1 && results[1] == 1, "return CAS both results joined exactly once");
        foreach (i; 0 .. 2) check(park.spawn(State(&results[i], 2, 1)) == PrivateSpawnResult.spawned);
        // Disable pausing with armed already zero; demonstrate both slots reusable.
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "return CAS recycled slots timeout");
        }
        namedCheck(results[0] == 2 && results[1] == 2, "return CAS reusable slots exactly once");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "return CAS allocation balance");
    }
}

version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
{
    struct EarlyChildControl { shared uint ready, go, consumed, leave; uint tables; }
    __gshared EarlyChildControl* earlyChildControl;
    void earlyChildPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto c = earlyChildControl;
        if (++c.tables != 1) return;
        // Mutation: set wave.pending to 0UL instead of 1UL in start (remove producer hold).
        // A separate consumer completes the child while writeTracked is still on stack.
        atomicStore!(MemoryOrder.raw)(c.go, 1u);
        awaitRaw(c.consumed, 1u, "child completion before seal timeout");
        namedCheck(atomicLoad!(MemoryOrder.raw)(privateControl.calls) == 0,
            "early child completion must not publish idle before emitter seal");
        namedCheck(atomicLoad!(MemoryOrder.raw)(privateControl.creditCalls) == 1,
            "early child table credit completed before emitter seal");
    }
    void childBeforeSeal()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        ulong result;
        CohortControl control = CohortControl(&result, 1);
        auto park = PrivateRootPark!(CohortRoot, cohortStep).create(farm, 1, 256, policy(counts));
        check(park !is null);
        check(park.spawn(CohortRoot(null, &control)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {}
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateWaveBeforeIdlePublish;
        EarlyChildControl c;
        privateControl = &hook;
        earlyChildControl = &c;
        setActorTestHook(&privateHook);
        writeAuditHook = &earlyChildPublished;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(c.ready, 1u);
            awaitRaw(c.go, 1u, "early child worker start timeout");
            auto deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad!(MemoryOrder.raw)(hook.creditCalls) == 0)
            {
                other.consumeNext();
                namedCheck(MonoTime.currTime < deadline, "early child claim timeout");
            }
            atomicStore!(MemoryOrder.raw)(c.consumed, 1u);
            awaitRaw(c.leave, 1u, "early child reuse before worker join timeout");
        });
        worker.start();
        awaitRaw(c.ready, 1u, "early child subscriber timeout");
        park.pump(token); // audit hook blocks the emitter until the child table completed
        writeAuditHook = null; // worker only consumes, and never enters a write path
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 1, "early child emitter seal publishes idle");
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            while (view.consumeNext()) {}
            park.pump(token);
            namedCheck(MonoTime.currTime < deadline, "early child normal/terminal reuse timeout");
        }
        namedCheck(result == 3 && control.completed == 3, "early child immediate normal/terminal reuse");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(c.leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        earlyChildControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "early child allocation balance");
    }
}

struct DeepMember { PrivateChildPark!DeepMember* next; ulong* results; size_t depth; }
void deepOperation(scope ref PrivateBorrow!DeepMember b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto d = b.value.depth;
    namedCheck(d < 64 && b.value.results[d] == 0, "depth64 member exactly once");
    b.value.results[d] = d + 1;
    if (d == 63) return;
    b.value.next = ctx.createChildren!DeepMember(1);
    namedCheck(b.value.next !is null, "depth64 child create");
    namedCheck(b.value.next.spawn(ctx, DeepMember(null, b.value.results, d + 1)) == PrivateSpawnResult.spawned,
        "depth64 child spawn");
    namedCheck(b.value.next.start!deepOperation(ctx) == PrivateStartResult.started, "depth64 descendant start");
}
void deepRetire(scope ref PrivateBorrow!DeepMember b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    namedCheck(b.value.depth == 0, "depth64 terminal top member");
    namedCheck(ctx.createChildren!DeepMember(1) is null, "depth64 terminal authority");
    foreach (i; 0 .. 64) namedCheck(b.value.results[i] == i + 1, "depth64 descendant results joined");
    // Idle descendant parks are recursively disposed by terminal member retirement.
}
struct DeepRoot { PrivateChildPark!DeepMember* chain; ulong* results; uint phase; }
PrivateRootDisposition deepStep(scope ref PrivateBorrow!DeepRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.chain is null)
    {
        b.value.chain = ctx.createChildren!DeepMember(1);
        namedCheck(b.value.chain !is null, "depth64 root child create");
        namedCheck(b.value.chain.spawn(ctx, DeepMember(null, b.value.results, 0)) == PrivateSpawnResult.spawned,
            "depth64 root child spawn");
        namedCheck(b.value.chain.start!deepOperation(ctx) == PrivateStartResult.started, "depth64 chain start");
        b.value.phase = 1;
        return PrivateRootDisposition.again;
    }
    if (!b.value.chain.finished(ctx)) return PrivateRootDisposition.again;
    foreach (i; 0 .. 64) namedCheck(b.value.results[i] == i + 1, "depth64 root transitive join");
    if (b.value.phase == 2) return PrivateRootDisposition.finish;
    namedCheck(b.value.chain.startFinal!deepRetire(ctx) == PrivateStartResult.started, "depth64 chain retirement");
    b.value.phase = 2;
    return PrivateRootDisposition.again;
}
void depth64()
{
    // Completion and disposal recurse: this protects the documented stack-bound
    // depth64 case without claiming an arbitrary-depth stack guarantee.
    Counts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ulong[64] results;
    auto park = PrivateRootPark!(DeepRoot, deepStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(DeepRoot(null, results.ptr)) == PrivateSpawnResult.spawned);
    auto workers = new Workers(farm, 2);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "depth64 join/retirement timeout");
    }
    // Read before joining workers: visibility must come from the library.
    foreach (i; 0 .. 64) namedCheck(results[i] == i + 1, "depth64 final joined result");
    park.destroy();
    workers.join();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "depth64 allocation balance");
}

PrivateRootDisposition activeChildFinish(scope ref PrivateBorrow!State, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto children = ctx.createChildren!State(1);
    check(children !is null);
    check(children.spawn(ctx, State.init) == PrivateSpawnResult.spawned);
    check(children.start!authorityOperation(ctx) == PrivateStartResult.started);
    return PrivateRootDisposition.finish;
}
void activeChildDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(State, activeChildFinish).create(farm, 1);
    check(park !is null);
    check(park.spawn(State.init) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
void destroyLiveDeath()
{
    auto farm = AntFarm.create();
    auto park = Park.create(farm, 1);
    check(park !is null);
    check(park.spawn(State.init) == PrivateSpawnResult.spawned);
    park.destroy();
}
void phaseD()
{
    childLargeCohort(257); childLargeCohort(513);
    writeln("PASS child-large-cohort zero/partial admission, retry and three waves");
    childAllocationFailures(); writeln("PASS child allocation failures, capacity overflow and reusable slot");
    mixedTable!(PrivateRootReclamation.emitter)(false);
    mixedTable!(PrivateRootReclamation.consumer)(false);
    version (AntfarmWriteAuditHooks)
    {
        mixedTable!(PrivateRootReclamation.emitter)(true);
        mixedTable!(PrivateRootReclamation.consumer)(true);
    }
    writeln("PASS mixed-table again/finish and early completion under both reclamation policies");
    refillAfterFinal(); writeln("PASS refill-after-final and empty starts");
    version (AntfarmActorTestHooks)
    {
        delayedFinalCredit(); writeln("PASS delayed-final-credit authorized observation and immediate reuse/retirement");
        returnCasRetry(); writeln("PASS deterministic return-CAS retry and slot reuse");
        version (AntfarmWriteAuditHooks)
        {
            childBeforeSeal(); writeln("PASS child completion before emitter seal and immediate reuse");
        }
    }
    depth64(); writeln("PASS depth64 transitive joins, retirement and allocation balance");
}

// E1 raw barriers are scheduling controls only: result visibility must come
// from library credit/table/ready handoffs, never a test-side release edge.
struct E1Control
{
    ulong[2] result;
    uint calls;
    shared uint inCall, childArrived, childGo, rootArrived, rootGo;
    uint rounds = 1;
    bool empty, terminal, holdRoot, holdChild, polling, instant;
}
struct E1Child { E1Control* c; size_t index; }
struct E1Nested { E1Control* c; }
void e1Leaf(scope ref PrivateBorrow!E1Child b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto c = b.value.c;
    if (c.holdChild)
    {
        atomicStore!(MemoryOrder.raw)(c.childArrived, 1u);
        awaitRaw(c.childGo, 1u, "E1 child barrier");
    }
    ++c.result[b.value.index];
}
void e1Nested(scope ref PrivateBorrow!E1Nested b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto p = ctx.createChildren!E1Child(1);
    namedCheck(p !is null, "E1 descendant create");
    namedCheck(p.spawn(ctx, E1Child(b.value.c, 1)) == PrivateSpawnResult.spawned, "E1 descendant spawn");
    namedCheck(p.start!e1Leaf(ctx) == PrivateStartResult.started, "E1 descendant start");
}
struct E1Root
{
    E1Control* c;
    PrivateChildPark!E1Child* first;
    PrivateChildPark!E1Nested* second;
    uint generation;
    bool peer;
    bool extraWait;
}
PrivateRootDisposition e1Step(scope ref PrivateBorrow!E1Root b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.c;
    if (b.value.peer)
    {
        atomicStore!(MemoryOrder.raw)(c.rootArrived, 1u);
        awaitRaw(c.rootGo, 1u, "E1 table peer barrier");
        return PrivateRootDisposition.finish;
    }
    namedCheck(atomicFetchAdd!(MemoryOrder.raw)(c.inCall, 1u) == 0, "E1 no concurrent root dispatch");
    scope(exit) atomicFetchSub!(MemoryOrder.raw)(c.inCall, 1u);
    ++c.calls;
    if (c.instant) return PrivateRootDisposition.finish;
    if (b.value.generation && c.polling
        && (!b.value.first.finished(ctx) || !b.value.second.finished(ctx)))
        return PrivateRootDisposition.again;
    if (b.value.generation)
    {
        namedCheck(c.result[0] == (c.empty ? 0 : b.value.generation), "E1 joined plain result zero");
        namedCheck(c.result[1] == (c.empty ? 0 : b.value.generation), "E1 descendant plain result one");
        if (b.value.generation == c.rounds) return PrivateRootDisposition.finish;
    }
    if (b.value.first is null)
    {
        b.value.first = ctx.createChildren!E1Child(1);
        b.value.second = ctx.createChildren!E1Nested(1);
        namedCheck(b.value.first !is null && b.value.second !is null, "E1 two cohorts");
        if (!c.empty)
        {
            namedCheck(b.value.first.spawn(ctx, E1Child(c, 0)) == PrivateSpawnResult.spawned, "E1 direct spawn");
            namedCheck(b.value.second.spawn(ctx, E1Nested(c)) == PrivateSpawnResult.spawned, "E1 nested spawn");
        }
    }
    auto a = b.value.first.start!e1Leaf(ctx);
    auto z = b.value.second.start!e1Nested(ctx);
    namedCheck(a == (c.empty ? PrivateStartResult.empty : PrivateStartResult.started), "E1 first wave");
    namedCheck(z == a, "E1 second wave");
    ++b.value.generation;
    if (c.holdRoot)
    {
        atomicStore!(MemoryOrder.raw)(c.rootArrived, 1u);
        awaitRaw(c.rootGo, 1u, "E1 callback barrier");
    }
    return c.terminal ? PrivateRootDisposition.finishAfterChildren
        : c.polling ? PrivateRootDisposition.again : PrivateRootDisposition.waitChildren;
}

// Selector: e1-basic.
// Catching mutation (E1-M1): delete the collector's `releaseRoot(entry);` in
// collectCompletedImpl (no custody release). Waiting roots never resume; the
// bounded loop fails "E1 completion deadline" instead of hanging the runner.
void e1Basic(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(E1Root, e1Step, reclaim).create(farm, 4, 4, policy(counts));
    E1Control c;
    foreach (kind; 0 .. 4)
    {
        c = E1Control.init;
        c.empty = kind == 1;
        c.terminal = kind == 2;
        c.rounds = kind == 3 ? 7 : 1;
        check(park.spawn(E1Root(&c)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "E1 completion deadline");
        }
        namedCheck(c.calls == (c.terminal ? 1 : c.rounds + 1), "E1 exact user call count");
        namedCheck(c.result[0] == (c.empty ? 0 : c.rounds), "E1 final direct result");
        namedCheck(c.result[1] == (c.empty ? 0 : c.rounds), "E1 final descendant result");
    }
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "E1 allocation balance");
}

// Child join BEFORE root table custody release, including callback still in
// flight and table peer still in flight. The collector is the final credit
// owner here, and the zero-admission checks pin the root physical table.
// Selector: e1-before-custody.
// Catching mutation (E1-M2): in collectCompletedImpl replace `releaseRoot(entry);`
// with a bare `atomicFetchSub!(MemoryOrder.acq_rel)(entry.pending, 1u);`, i.e.
// the collector decrement that never enqueues on zero. Both children already
// joined, so the root is never resumed: "E1 before custody completion".
void e1BeforeCustody(bool peer)
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    E1Control c; c.holdRoot = !peer;
    auto park = PrivateRootPark!(E1Root, e1Step).create(farm, 2, 2);
    check(park.spawn(E1Root(&c)) == PrivateSpawnResult.spawned);
    if (peer) check(park.spawn(E1Root(&c, null, null, 0, true)) == PrivateSpawnResult.spawned);
    version (AntfarmActorTestHooks)
    {
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateRootCreditBeforeRelease;
        privateControl = &hook; setActorTestHook(&privateHook);
        scope(exit) { setActorTestHook(null); privateControl = null; }
    }
    shared uint ready, go, leave;
    auto worker = new Thread({
        ConsumerView other;
        check(other.subscribe(farm) >= 0);
        scope(exit) other.unsubscribe();
        atomicStore!(MemoryOrder.raw)(ready, 1u);
        awaitRaw(go, 1u, "E1 custody worker go");
        while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
    });
    worker.start(); awaitRaw(ready, 1u, "E1 custody worker ready");
    check(park.pump(token) == (peer ? 2 : 1));
    atomicStore!(MemoryOrder.raw)(go, 1u);
    awaitRaw(c.rootArrived, 1u, "E1 root/peer blocked");
    // The root callback has enqueued both cohorts, but its table has no return.
    // Drive children, including descendants, from the second consumer.
    foreach (_; 0 .. 6)
    {
        namedCheck(park.pump(token) == 0, "E1 no early requeue before table join");
        while (view.consumeNext()) {}
    }
    version (AntfarmActorTestHooks)
        awaitRaw(hook.calls, 2u, "E1 both direct notifications before custody");
    atomicStore!(MemoryOrder.raw)(c.rootGo, 1u);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token); while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "E1 before custody completion");
    }
    namedCheck(c.calls == 2, "E1 before custody exactly two calls");
    park.destroy();
    atomicStore!(MemoryOrder.raw)(leave, 1u); worker.join();
}

// AFTER collector release, hold the last descendant notification after idle.
// poll must not publish work, and the sleeper must not retain its only Batch.
// The child wave finisher is the final credit owner and must enqueue the root.
// Selector: e1-after-custody.
// Catching mutation (E1-M3): in PrivateChildPark.finish replace the root-wave
// `releaseRoot(parent);` with a bare `atomicFetchSub!(MemoryOrder.acq_rel)(
// parent.pending, 1u);` (wave decrement that never enqueues on zero). The
// sleeping root is never resumed: "E1 after custody completion".
void e1AfterCustody()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    E1Control c; c.holdChild = true;
    auto park = PrivateRootPark!(E1Root, e1Step).create(farm, 2, 2);
    check(park.spawn(E1Root(&c)) == PrivateSpawnResult.spawned);
    check(park.pump(token) == 1); while (view.consumeNext()) {}
    // Explicitly park before publishing children.
    park.poll();
    shared uint ready, go, leave;
    auto worker = new Thread({
        ConsumerView other; check(other.subscribe(farm) >= 0);
        scope(exit) other.unsubscribe();
        atomicStore!(MemoryOrder.raw)(ready, 1u);
        awaitRaw(go, 1u, "E1 after worker go");
        while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
    });
    worker.start(); awaitRaw(ready, 1u, "E1 after ready");
    namedCheck(park.pump(token) == 0, "E1 wait not published");
    atomicStore!(MemoryOrder.raw)(go, 1u);
    awaitRaw(c.childArrived, 1u, "E1 after child blocked");
    // Capacity=2/batch=2 => one physical Batch. It must be available to a
    // newly spawned root while the first root sleeps with active children.
    E1Control empty; empty.empty = true;
    check(park.spawn(E1Root(&empty)) == PrivateSpawnResult.spawned);
    namedCheck(park.pump(token) == 1, "E1 sleeping root recycled only Batch");
    atomicStore!(MemoryOrder.raw)(c.childGo, 1u);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token); while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "E1 after custody completion");
    }
    namedCheck(c.calls == 2 && empty.calls == 2, "E1 after custody exactly two calls");
    park.destroy(); atomicStore!(MemoryOrder.raw)(leave, 1u); worker.join();
}

struct E1StrictRoot { E1Control* c; PrivateChildPark!E1Child* child; }
PrivateRootDisposition e1StrictStep(scope ref PrivateBorrow!E1StrictRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    ++b.value.c.calls;
    if (b.value.child is null)
    {
        b.value.child = ctx.createChildren!E1Child(1);
        namedCheck(b.value.child.spawn(ctx, E1Child(b.value.c, 0)) == PrivateSpawnResult.spawned, "E1 strict child spawn");
        namedCheck(b.value.child.start!e1Leaf(ctx) == PrivateStartResult.started, "E1 strict child start");
        return PrivateRootDisposition.again;
    }
    if (!b.value.child.finished(ctx)) return PrivateRootDisposition.again;
    namedCheck(b.value.c.result[0] == 1, "E1 strict idle acquire joins result");
    return PrivateRootDisposition.finish;
}
// Selector: e1-late-strict.
// Catching mutation (E1-M4): in collectCompletedImpl delete `entry.hasJoinCredits ||`
// from the join-credit branch condition, so strict finish reclaims the Entry
// without waiting for the paused notification tail. Fails
// "E1 strict Entry pinned past state cleanup".
version (AntfarmActorTestHooks)
void e1LateStrict(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view; check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    E1Control c;
    auto park = PrivateRootPark!(E1StrictRoot, e1StrictStep, reclaim).create(farm, 1, 1, policy(counts));
    check(park.spawn(E1StrictRoot(&c)) == PrivateSpawnResult.spawned);
    check(park.pump(token) == 1); while (view.consumeNext()) {}
    PrivateHookControl hook;
    hook.point = ActorTestPoint.privateRootAfterIdleBeforeCredit; hook.armed = 1;
    privateControl = &hook; setActorTestHook(&privateHook);
    shared uint ready, go, leave;
    auto worker = new Thread({
        ConsumerView other; check(other.subscribe(farm) >= 0);
        scope(exit) other.unsubscribe();
        atomicStore!(MemoryOrder.raw)(ready, 1u);
        awaitRaw(go, 1u, "E1 strict worker go");
        while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
    });
    worker.start(); awaitRaw(ready, 1u, "E1 strict worker ready");
    check(park.pump(token) == 1); // child then polling root
    atomicStore!(MemoryOrder.raw)(go, 1u);
    awaitRaw(hook.arrived, 1u, "E1 strict idle tail paused");
    while (view.consumeNext()) {}
    park.poll();
    namedCheck(c.calls == 2, "E1 strict finished callback after idle");
    namedCheck(park.live == 1, "E1 strict Entry pinned past state cleanup");
    namedCheck(park.spawn(E1StrictRoot(&c)) == PrivateSpawnResult.capacityFull, "E1 strict no premature reuse");
    atomicStore!(MemoryOrder.raw)(hook.release, 1u);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.poll(); namedCheck(MonoTime.currTime < deadline, "E1 strict tail completion");
    }
    c.result[0] = 0;
    // Verify reuse while the notifier thread is still alive.
    check(park.spawn(E1StrictRoot(&c)) == PrivateSpawnResult.spawned);
    while (park.live)
    {
        park.pump(token); while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "E1 strict reused completion");
    }
    park.destroy(); atomicStore!(MemoryOrder.raw)(leave, 1u); worker.join();
    setActorTestHook(null); privateControl = null;
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "E1 strict allocation balance");
}

PrivateRootDisposition e1JoinedStep(scope ref PrivateBorrow!E1Root b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto d = e1Step(b, ctx);
    if (d == PrivateRootDisposition.finish && !b.value.extraWait)
    {
        b.value.extraWait = true;
        return PrivateRootDisposition.waitChildren; // all waves already joined
    }
    return d;
}
// Selector: e1-joined-mixed.
// Catching mutation (E1-M5): in collectCompletedImpl delete
// `|| entry.disposition != PrivateRootDisposition.finish` so a waitChildren
// root with no active waves (hasJoinCredits false) is reclaimed as a leaf finish
// instead of getting its one later callback. Fails
// "E1 already joined wait exactly one later call".
void e1JoinedAndMixed(PrivateRootReclamation reclaim)()
{
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view; check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto workers = new Workers(farm, 5);
    E1Control c;
    auto joined = PrivateRootPark!(E1Root, e1JoinedStep, reclaim).create(farm, 1, 1);
    check(joined.spawn(E1Root(&c)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (joined.live)
    {
        joined.pump(token); while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "E1 already joined wait deadline");
    }
    namedCheck(c.calls == 3, "E1 already joined wait exactly one later call");
    joined.destroy();
    auto mixed = PrivateRootPark!(E1Root, e1Step, reclaim).create(farm, 4, 4);
    E1Control[4] controls;
    controls[1].polling = true; controls[2].instant = true; controls[3].terminal = true;
    foreach (ref control; controls) check(mixed.spawn(E1Root(&control)) == PrivateSpawnResult.spawned);
    while (mixed.live)
    {
        mixed.pump(token); while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "E1 mixed disposition deadline");
    }
    namedCheck(controls[0].calls == 2 && controls[1].calls >= 2
        && controls[2].calls == 1 && controls[3].calls == 1, "E1 mixed exact calls");
    mixed.destroy(); workers.join();
}

// Minimal waiting root: first callback launches one direct child and waits,
// second callback checks the joined plain result and finishes.
struct E1CasControl
{
    shared uint arrived, go;
    ulong result;
    uint calls, rank;
    uint* nextRank;
}
struct E1CasChild { E1CasControl* c; }
void e1CasLeaf(scope ref PrivateBorrow!E1CasChild b, scope ref PrivateContext) nothrow @nogc @system
{
    auto c = b.value.c;
    atomicStore!(MemoryOrder.raw)(c.arrived, 1u);
    awaitRaw(c.go, 1u, "E1 CAS child barrier");
    ++c.result; // plain write; visible to the resumed root only through library joins
}
struct E1CasRoot { E1CasControl* c; PrivateChildPark!E1CasChild* child; }
PrivateRootDisposition e1CasStep(scope ref PrivateBorrow!E1CasRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.c;
    if (++c.calls == 2)
    {
        namedCheck(c.result == 1, "E1 CAS joined plain result");
        c.rank = ++*c.nextRank;
        return PrivateRootDisposition.finish;
    }
    namedCheck(c.calls == 1, "E1 CAS exactly two user calls");
    b.value.child = ctx.createChildren!E1CasChild(1);
    namedCheck(b.value.child !is null, "E1 CAS child create");
    namedCheck(b.value.child.spawn(ctx, E1CasChild(c)) == PrivateSpawnResult.spawned, "E1 CAS child spawn");
    namedCheck(b.value.child.start!e1CasLeaf(ctx) == PrivateStartResult.started, "E1 CAS child start");
    return PrivateRootDisposition.waitChildren;
}

version (AntfarmActorTestHooks)
{
    struct E1CasWorker { shared uint returns; uint id; }
    // Thread-local: identifies the consumer a hook callback runs on.
    E1CasWorker* e1CasSelf;
    struct E1CasHookControl { shared uint armed, arrived, release, calls; }
    __gshared E1CasHookControl* e1CasHookControl;
    void e1CasHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        if (point != ActorTestPoint.privateRootReadyBeforeCas) return;
        auto h = e1CasHookControl;
        atomicFetchAdd!(MemoryOrder.raw)(h.calls, 1u);
        if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&h.armed, 1u, 0u)) return;
        atomicStore!(MemoryOrder.raw)(h.arrived, e1CasSelf.id); // worker index + 1
        awaitRaw(h.release, 1u, "E1 CAS hook release timeout");
    }

    struct E1CasPool { shared uint ready, go, stop, parked, leave; }
    // A function, not a loop-body delegate: each worker needs its own frame.
    Thread e1CasWorkerThread(AntFarm* farm, E1CasWorker* me, E1CasPool* pool)
    {
        return new Thread({
            e1CasSelf = me;
            ConsumerView other; check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicFetchAdd!(MemoryOrder.raw)(pool.ready, 1u);
            awaitRaw(pool.go, 1u, "E1 CAS worker go");
            while (atomicLoad!(MemoryOrder.raw)(pool.stop) == 0)
            {
                other.consumeNext();
                atomicFetchAdd!(MemoryOrder.raw)(me.returns, 1u);
            }
            atomicFetchAdd!(MemoryOrder.raw)(pool.parked, 1u);
            awaitRaw(pool.leave, 1u, "E1 CAS worker leave");
        });
    }

    // Two roots each wait on one child. Their final credits are released by two
    // different consumers, so both race on the root-ready MPSC stack, distinct
    // from the Batch return stack. Worker X reads the null head and pauses before
    // its publication CAS; worker Y publishes its root; X's CAS must fail, reload
    // and republish. Both roots resume exactly once, in publication order.
    // Selector: e1-root-ready-cas-retry.
    // Catching mutation (E1-M6): in releaseRoot replace the failed-CAS reload
    // `head = atomicLoad!(MemoryOrder.raw)(*returns);` with `return;`. The retry
    // never happens and the root is dropped: "E1 CAS exactly one retry".
    void e1RootReadyCasRetry()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view; check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        uint rank;
        E1CasControl[2] c;
        foreach (ref x; c) x.nextRank = &rank;
        auto park = PrivateRootPark!(E1CasRoot, e1CasStep).create(farm, 2, 2, policy(counts));
        check(park !is null);
        foreach (ref x; c) check(park.spawn(E1CasRoot(&x)) == PrivateSpawnResult.spawned);
        namedCheck(park.pump(token) == 2, "E1 CAS roots published in one table");
        while (view.consumeNext()) {} // both roots start their wave on this thread
        namedCheck(c[0].calls == 1 && c[1].calls == 1, "E1 CAS first callbacks");
        E1CasHookControl hook; hook.armed = 1;
        e1CasHookControl = &hook; setActorTestHook(&e1CasHook);
        scope(exit) { setActorTestHook(null); e1CasHookControl = null; }
        E1CasWorker[2] workers;
        E1CasPool pool;
        Thread[2] threads;
        foreach (i; 0 .. 2)
        {
            workers[i].id = cast(uint) i + 1;
            threads[i] = e1CasWorkerThread(farm, &workers[i], &pool);
            threads[i].start();
        }
        awaitRaw(pool.ready, 2u, "E1 CAS workers ready");
        // The collector releases custody; each wave tail is then the last credit.
        namedCheck(park.pump(token) == 0, "E1 CAS waves emitted, no root ready");
        atomicStore!(MemoryOrder.raw)(pool.go, 1u);
        awaitRaw(c[0].arrived, 1u, "E1 CAS first child running");
        awaitRaw(c[1].arrived, 1u, "E1 CAS second child running");
        // Each worker is blocked inside its child's consumeNext, so its return
        // counter is frozen until that call (including the root release) returns.
        atomicStore!(MemoryOrder.raw)(c[0].go, 1u);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.raw)(hook.arrived) == 0)
        {
            namedCheck(MonoTime.currTime < deadline, "E1 CAS first publisher paused");
            pause();
        }
        auto paused = &workers[atomicLoad!(MemoryOrder.raw)(hook.arrived) - 1];
        auto other = &workers[2 - atomicLoad!(MemoryOrder.raw)(hook.arrived)];
        immutable pausedBase = atomicLoad!(MemoryOrder.raw)(paused.returns);
        immutable otherBase = atomicLoad!(MemoryOrder.raw)(other.returns);
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 1, "E1 CAS one head load before competitor");
        atomicStore!(MemoryOrder.raw)(c[1].go, 1u);
        while (atomicLoad!(MemoryOrder.raw)(other.returns) == otherBase)
        {
            namedCheck(MonoTime.currTime < deadline, "E1 CAS competing publication timeout");
            pause();
        }
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 2, "E1 CAS competitor published once");
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        while (atomicLoad!(MemoryOrder.raw)(paused.returns) == pausedBase)
        {
            namedCheck(MonoTime.currTime < deadline, "E1 CAS retry publication timeout");
            pause();
        }
        // The collector stayed idle until the retry published, so no exchange
        // can restore the stale null head.
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 3, "E1 CAS exactly one retry");
        atomicStore!(MemoryOrder.raw)(pool.stop, 1u);
        awaitRaw(pool.parked, 2u, "E1 CAS workers parked");
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "E1 CAS both roots resumed");
        }
        namedCheck(c[0].calls == 2 && c[1].calls == 2, "E1 CAS each root resumed exactly once");
        namedCheck(c[1].rank == 1 && c[0].rank == 2, "E1 CAS FIFO follows publication order");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(pool.leave, 1u);
        foreach (t; threads) t.join();
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "E1 CAS allocation balance");
    }
}

version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
{
    struct E1SealControl
    {
        shared uint ready, go, consumed, leave, creditCalls, creditOnEmitter, readyCalls, readyOnEmitter;
        uint tables;
    }
    __gshared E1SealControl* e1Seal;
    bool e1OnEmitter; // thread-local
    void e1SealHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        auto s = e1Seal;
        if (point == ActorTestPoint.privateRootCreditBeforeRelease)
        {
            atomicFetchAdd!(MemoryOrder.raw)(s.creditCalls, 1u);
            if (e1OnEmitter) atomicFetchAdd!(MemoryOrder.raw)(s.creditOnEmitter, 1u);
        }
        else if (point == ActorTestPoint.privateRootReadyBeforeCas)
        {
            atomicFetchAdd!(MemoryOrder.raw)(s.readyCalls, 1u);
            if (e1OnEmitter) atomicFetchAdd!(MemoryOrder.raw)(s.readyOnEmitter, 1u);
        }
    }
    void e1SealPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto s = e1Seal;
        if (++s.tables != 1) return; // first table after installation is the child's
        atomicStore!(MemoryOrder.raw)(s.go, 1u);
        awaitRaw(s.consumed, 1u, "E1 seal child completion before seal timeout");
        // Child table fully joined, emitter still inside writeTracked: the only
        // root credit released so far is the collector's custody release, and the
        // root has not been woken.
        namedCheck(atomicLoad!(MemoryOrder.raw)(s.creditCalls) == 1, "E1 seal only custody released before seal");
        namedCheck(atomicLoad!(MemoryOrder.raw)(s.readyCalls) == 0, "E1 seal no root wake before seal");
    }

    // The child table joins completely while the emitter is still inside the
    // wave's writeTracked, after the collector released root custody. The only
    // remaining wave credit is the producer hold, so the emitter's seal in
    // PrivateChildPark.emit owns the wave's finish, the root's final credit and
    // the root-ready publication, all on the emitter thread, with no consumer
    // notification left to wake the root.
    // Selector: e1-seal-final-owner.
    // Catching mutation (E1-M7): in PrivateChildPark.emit replace the seal
    // `releaseWave(wave);` by a bare `atomicFetchSub!(MemoryOrder.acq_rel)(
    // wave.pending, 1UL);`. The wave never finishes, no wake is published, and the
    // test fails "E1 seal emitter released the final root credit".
    void e1SealFinalOwner()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view; check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        uint rank;
        E1CasControl c;
        c.nextRank = &rank;
        atomicStore!(MemoryOrder.raw)(c.go, 1u); // child never blocks
        auto park = PrivateRootPark!(E1CasRoot, e1CasStep).create(farm, 1, 1, policy(counts));
        check(park !is null);
        check(park.spawn(E1CasRoot(&c)) == PrivateSpawnResult.spawned);
        namedCheck(park.pump(token) == 1, "E1 seal root published");
        while (view.consumeNext()) {} // first callback starts the wave on this thread
        namedCheck(c.calls == 1, "E1 seal first callback");
        E1SealControl s;
        e1Seal = &s;
        setActorTestHook(&e1SealHook);
        writeAuditHook = &e1SealPublished;
        e1OnEmitter = true;
        auto worker = new Thread({
            ConsumerView other; check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(s.ready, 1u);
            awaitRaw(s.go, 1u, "E1 seal worker go");
            do other.consumeNext(); while (atomicLoad!(MemoryOrder.raw)(c.arrived) == 0);
            atomicStore!(MemoryOrder.raw)(s.consumed, 1u);
            awaitRaw(s.leave, 1u, "E1 seal worker leave");
        });
        worker.start();
        awaitRaw(s.ready, 1u, "E1 seal worker ready");
        // Collect (custody release), emit the wave table, run the audit hook.
        namedCheck(park.pump(token) == 0, "E1 seal wake not published by the same pump");
        writeAuditHook = null;
        namedCheck(atomicLoad!(MemoryOrder.raw)(s.creditCalls) == 2
            && atomicLoad!(MemoryOrder.raw)(s.creditOnEmitter) == 2, "E1 seal emitter released the final root credit");
        namedCheck(atomicLoad!(MemoryOrder.raw)(s.readyCalls) == 1
            && atomicLoad!(MemoryOrder.raw)(s.readyOnEmitter) == 1, "E1 seal emitter published the root wake");
        namedCheck(c.calls == 1 && park.live == 1, "E1 seal root asleep until collected");
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "E1 seal root resumption timeout");
        }
        namedCheck(c.calls == 2, "E1 seal exactly one resumed callback");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(s.leave, 1u);
        worker.join();
        setActorTestHook(null);
        e1Seal = null;
        e1OnEmitter = false;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "E1 seal allocation balance");
    }
}

// Counter exhaustion. The root's join counter is a uint at Entry offset 36.
// The test-only seam forces uint.max - 1 inside the root callback; the first
// legitimate retain (a direct wave start) must reach uint.max and the next one
// must die with the checked-retain message instead of wrapping to zero.
version (AntfarmActorTestHooks)
{
    struct E1ExhaustRoot { E1Control* c; PrivateChildPark!E1Child* first, second; }
    PrivateRootDisposition e1ExhaustStep(scope ref PrivateBorrow!E1ExhaustRoot b, scope ref PrivateContext ctx)
        nothrow @nogc @system
    {
        b.value.first = ctx.createChildren!E1Child(1);
        b.value.second = ctx.createChildren!E1Child(1);
        namedCheck(b.value.first !is null && b.value.second !is null, "E1 exhaustion cohorts");
        namedCheck(b.value.first.spawn(ctx, E1Child(b.value.c, 0)) == PrivateSpawnResult.spawned, "E1 exhaustion spawn one");
        namedCheck(b.value.second.spawn(ctx, E1Child(b.value.c, 1)) == PrivateSpawnResult.spawned, "E1 exhaustion spawn two");
        privateTestSetRootCredits(ctx, uint.max - 1);
        namedCheck(b.value.first.start!e1Leaf(ctx) == PrivateStartResult.started, "E1 exhaustion retain below limit");
        fprintf(stderr, "e1 exhaustion: retain at uint.max - 1 accepted\n");
        b.value.second.start!e1Leaf(ctx); // retain at uint.max: must be fatal
        fprintf(stderr, "e1 exhaustion: retain at uint.max survived\n");
        return PrivateRootDisposition.waitChildren;
    }
    // Child process body for the death harness (mode e1-counter-exhaustion).
    void e1CounterExhaustion()
    {
        auto farm = AntFarm.create();
        auto token = farm.registerProducer(Tier.small);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        E1Control c;
        auto park = PrivateRootPark!(E1ExhaustRoot, e1ExhaustStep).create(farm, 1);
        check(park !is null);
        check(park.spawn(E1ExhaustRoot(&c)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {}
        // Reached only if the checked retain did not kill the callback; collecting
        // makes a wrapped counter die with a different (underflow) message.
        park.poll();
    }
}
// Selector: e1-counter-exhaustion-death (parent side; debug and -release binaries
// via make run-death). Checks the nonzero death, the exact fatal text, and that
// the retain just below the limit was accepted first.
// Catching mutation (E1-M8): delete `if (old == uint.max) fatal("private join
// credit overflow");` from retainRootCredit. The counter wraps to zero, the child
// dies later with the underflow text (or survives) and the check fails.
void e1CounterExhaustionDeath()
{
    version (linux) version (AntfarmActorTestHooks)
    {
        auto probe = probeDeath("e1-counter-exhaustion");
        check(probe.status != 0);
        check(probe.stderrText.canFind("e1 exhaustion: retain at uint.max - 1 accepted"));
        check(probe.stderrText.canFind("antfarm fatal: private join credit overflow"));
        check(!probe.stderrText.canFind("retain at uint.max survived"));
        writeln("PASS E1 join counter exhaustion is fatal after the last legal retain");
        return;
    }
    writeln("SKIP E1 counter exhaustion death: requires Linux and AntfarmActorTestHooks");
}

void phaseE1()
{
    e1Basic!(PrivateRootReclamation.emitter)();
    e1Basic!(PrivateRootReclamation.consumer)();
    e1JoinedAndMixed!(PrivateRootReclamation.emitter)();
    e1JoinedAndMixed!(PrivateRootReclamation.consumer)();
    e1BeforeCustody(false); e1BeforeCustody(true); e1AfterCustody();
    version (AntfarmActorTestHooks)
    {
        e1LateStrict!(PrivateRootReclamation.emitter)();
        e1LateStrict!(PrivateRootReclamation.consumer)();
        e1RootReadyCasRetry();
        version (AntfarmWriteAuditHooks) e1SealFinalOwner();
    }
    writeln("PASS E1 waits, two direct cohorts/descendants, repeated/empty waves, both frees, cleanup, custody before/after and Batch recycle");
}

// ===========================================================================
// Typed result ports (E2 slice 1), design §8: T1-T8, D1-D2.
//
// Every result is generation- and index-tagged: (wave << 32) | spawnIndex,
// where `wave` is the member's own dispatch count (a void wave also counts)
// and `spawnIndex` is stored in the member. A slot written by the wrong
// member, or a member dispatched twice, therefore fails a tag comparison.
// Raw (relaxed) flags are scheduling controls only: result visibility must
// come from the library's finished/results acquire and join chain.
// Each test comment names the mutation it catches (P-M1 .. P-M13) and the
// concrete outcome observed when that mutation was applied by hand.
// ===========================================================================
struct PMember { uint index, wave; }
ulong portsValue(uint wave, size_t index) pure nothrow @nogc @safe
{ return (cast(ulong) wave << 32) | index; }
ulong portsTag(scope ref PrivateBorrow!PMember m, scope ref PrivateContext) nothrow @nogc @system
{ return portsValue(++m.value.wave, m.value.index); }
void portsTouch(scope ref PrivateBorrow!PMember m, scope ref PrivateContext) nothrow @nogc @system
{ ++m.value.wave; }

ubyte portsThreadMarker; // thread-local: its address identifies the running consumer
ulong portsThreadId() nothrow @nogc @system { return cast(ulong) &portsThreadMarker; }

// ---- T8: compile-time contract and layout ------------------------------------
// Mutation: any change to the createChildren/start/results template
// constraints (drop the POD static assert on R, accept any return type, or
// expose `results` on a void cohort) turns one of these asserts false.
struct PItem { ulong input; }
struct PNotPod { int x; this(this) {} }
struct PWide { ulong[16] words; }
version (assert) enum pWideStorage = PWide.sizeof + 1;
else enum pWideStorage = PWide.sizeof;
version (assert) enum ulongStorage = ulong.sizeof + 1;
else enum ulongStorage = ulong.sizeof;
ulong pSquare(scope ref PrivateBorrow!PItem, scope ref PrivateContext) nothrow @nogc @system { return 0; }
void pVoidOp(scope ref PrivateBorrow!PItem, scope ref PrivateContext) nothrow @nogc @system {}
uint pWrongType(scope ref PrivateBorrow!PItem, scope ref PrivateContext) nothrow @nogc @system { return 0; }
void pVoidNotNothrow(scope ref PrivateBorrow!PItem, scope ref PrivateContext) @nogc @system {}
void pVoidNotNogc(scope ref PrivateBorrow!PItem, scope ref PrivateContext) nothrow @system {}
ulong pNotNothrow(scope ref PrivateBorrow!PItem, scope ref PrivateContext) @system { return 0; }
alias PVoidCohort = PrivateChildPark!PItem;
alias PUlongCohort = PrivateChildPark!(PItem, ulong);
static assert(is(PVoidCohort == PrivateChildPark!(PItem, void)));
static assert(!PVoidCohort.hasResults && PUlongCohort.hasResults);
static assert(!__traits(compiles, PrivateChildPark!(PItem, const ulong)));
static assert(!__traits(compiles, PrivateChildPark!(PItem, immutable ulong)));
static assert(!__traits(compiles, PrivateChildPark!(PItem, shared ulong)));
static assert(!__traits(compiles, PrivateChildPark!(PItem, shared const ulong)));
static assert(!__traits(compiles, PrivateChildPark!(PItem, inout ulong)));
static assert(!__traits(compiles, PrivateChildPark!(PItem, PNotPod)));       // non-POD R
// non-POD T (postblit) is accepted since E2-1; see the stateAllowed probes in the destructor section.
static assert(__traits(compiles, PrivateChildPark!(PNotPod, ulong)));
static assert(__traits(compiles, PrivateChildPark!(PItem, PWide)));          // POD aggregate R
static assert(!__traits(hasMember, PVoidCohort, "results"));                 // absent on a void cohort
static assert(__traits(hasMember, PUlongCohort, "results"));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.results(c)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.start!pSquare(c)));      // R op, void cohort
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.startFinal!pSquare(c)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pWrongType(c)));  // wrong return type
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.startFinal!pWrongType(c)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pNotNothrow(c)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.start!pVoidNotNothrow(c)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.start!pVoidNotNogc(c)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pVoidNotNothrow(c)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pVoidNotNogc(c)));
static assert(!__traits(compiles,
    (PrivateChildPark!(PItem, uint)* p, ref PrivateContext c) => p.start!pSquare(c)));                  // R mismatch
static assert(__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pVoidOp(c)));      // void op on result cohort
static assert(__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.start!pSquare(c)));
static assert(__traits(compiles, (PUlongCohort* p, ref PrivateContext c) => p.startFinal!pSquare(c)));
static assert(__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.start!pVoidOp(c)));       // unchanged today
static assert(is(typeof(PrivateContext.init.createChildren!PItem(1)) == PVoidCohort*));
static assert(is(typeof(PrivateContext.init.createChildren!(PItem, ulong)(1)) == PUlongCohort*));
static assert(is(typeof(PrivateContext.init.createChildren!(PItem, ulong)(1, 2)) == PUlongCohort*));
static assert(!__traits(compiles, PrivateContext.init.createChildren!(PItem, PNotPod)(1)));
// Layout: subset selection adds two words per cohort to main 3441b8b's
// 264-byte signal-only park. Results add exactly two more words. F1 adds
// 16 bytes of wave failure fields and two participant counters; F2 adds
// the immutable ChildBase.root word.
// Entry's own 40-byte layout is asserted next to its definition and checked
// through the allocation sizes in portsAllocationFailures.
static if (size_t.sizeof == 8)
{
    static assert(PVoidCohort.sizeof == 376);
    // The debug-only reentrancy flag (`version (assert) bool entered_`) adds a word.
    // Service.childReturns (E2-3) accounts for one word of both.
    // E2-4 reserves 48 more bytes in ChildBase for the resume descriptor/FIFO.
    // F3 adds 32: Service.records/rootEntries and the outcome FIFO head/tail.
    version (assert) static assert(PrivateRootPark!(State, repeat).sizeof == 208);
    else static assert(PrivateRootPark!(State, repeat).sizeof == 200);
}
static assert(PUlongCohort.sizeof == PVoidCohort.sizeof + 2 * size_t.sizeof);
static assert(PrivateChildPark!(PItem, ubyte).sizeof == PVoidCohort.sizeof + 2 * size_t.sizeof);
static assert(PrivateChildPark!(PItem, PWide).sizeof == PVoidCohort.sizeof + 2 * size_t.sizeof);
static assert(PrivateChildPark!(PItem, PWide).alignof == PVoidCohort.alignof);

// E2-2 compile-time contract: the options overload and subset API follow the
// same type rules as start. Mutation: dropping the R/nothrow/@nogc constraints
// of startSubset, or exposing it elsewhere, turns one of these asserts false.
static assert(is(typeof(PrivateContext.init.createChildren!PItem(1, PrivateCohortOptions(0, true))) == PVoidCohort*));
static assert(is(typeof(PrivateContext.init.createChildren!(PItem, ulong)(1, PrivateCohortOptions(3, true))) == PUlongCohort*));
static assert(!__traits(compiles, PrivateContext.init.createChildren!(PItem, PNotPod)(1, PrivateCohortOptions(0, true))));
static assert(PrivateCohortOptions.init.batchLimit == 0 && !PrivateCohortOptions.init.subsets);
static assert(is(typeof({ PVoidCohort* p; PrivateContext c; return p.subsetMembers(c); }()) == const(uint)[]));
static assert(__traits(compiles, (PVoidCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pVoidOp(c, m)));
static assert(__traits(compiles, (PVoidCohort* p, ref PrivateContext c, uint[] m) => p.startSubset!pVoidOp(c, m)));
static assert(__traits(compiles, (PUlongCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pSquare(c, m)));
static assert(__traits(compiles, (PUlongCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pVoidOp(c, m)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pSquare(c, m)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pWrongType(c, m)));
static assert(!__traits(compiles, (PUlongCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pNotNothrow(c, m)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pVoidNotNothrow(c, m)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c, const(uint)[] m) => p.startSubset!pVoidNotNogc(c, m)));
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c, const(ulong)[] m) => p.startSubset!pVoidOp(c, m))); // indices are uint
static assert(!__traits(compiles, (PVoidCohort* p, ref PrivateContext c) => p.startSubset!pVoidOp(c)));                       // a list is required
static assert(!__traits(hasMember, PVoidCohort, "startFinalSubset"));                                                         // no subset retirement

// ---- T1: ports-basic ------------------------------------------------------------
// Selector: ports-basic. U1-U3: empty before the first wave, waitChildren
// read, polling (again/finished) read, late spawn keeps length, void wave
// clears, startFinal results survive member retirement, empty start keeps
// them, refill then new results, both reclamation policies, allocation balance.
// Catching mutations (each applied by hand to actors/private_actor.d):
//   P-M1 result length derived from the live member count instead of the
//        count stored at start. Applied three ways:
//        (a) `results` returns results_[0 .. count_]: fails first at
//            "ports spawn alone produces no results" (spawn raised count_).
//        (b) finish clears `resultCount_` when it retires members (the
//            "final length" bug): fails "ports final wave results survive retirement".
//        (c) start's store deleted and finish sets `resultCount_ = count_`
//            after the retire zeroing: fails "ports void wave clears results".
//   P-M2 start sets `resultCount_ = producesResults ? count_ : resultCount_`
//        (a void wave does not clear): fails "ports void wave clears results".
//   P-M3 the `resultCount_ = ...` store moved above the `count_ == 0` check
//        (an empty start clears after startFinal): fails "ports empty start keeps results".
//   P-M4 dispose omits the `results_` free: fails "ports basic allocation balance".
struct PBasic { PrivateChildPark!(PMember, ulong)* c; uint phase; size_t n; shared ulong* done; }
PrivateRootDisposition portsBasicStep(scope ref PrivateBorrow!PBasic b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    switch (s.phase++)
    {
        case 0:
            s.c = ctx.createChildren!(PMember, ulong)(s.n, 64);
            namedCheck(s.c !is null, "ports basic create");
            namedCheck(s.c.finished(ctx), "ports basic idle before first wave");
            namedCheck(s.c.results(ctx).length == 0, "ports results empty before the first wave");
            namedCheck(s.c.start!portsTag(ctx) == PrivateStartResult.empty, "ports basic empty start");
            namedCheck(s.c.results(ctx).length == 0, "ports results still empty after empty start");
            foreach (i; 0 .. s.n - 1)
                namedCheck(s.c.spawn(ctx, PMember(cast(uint) i)) == PrivateSpawnResult.spawned, "ports basic spawn");
            namedCheck(s.c.results(ctx).length == 0, "ports spawn alone produces no results");
            namedCheck(s.c.start!portsTag(ctx) == PrivateStartResult.started, "ports basic wave 1");
            return PrivateRootDisposition.waitChildren;
        case 1:
        {
            auto r = s.c.results(ctx); // waitChildren resumption read
            namedCheck(r.length == s.n - 1, "ports wave 1 length");
            foreach (i, v; r) namedCheck(v == portsValue(1, i), "ports wave 1 tag");
            // A spawn into the idle cohort leaves the last results untouched.
            namedCheck(s.c.spawn(ctx, PMember(cast(uint) (s.n - 1), 1)) == PrivateSpawnResult.spawned, "ports late spawn");
            namedCheck(s.c.spawn(ctx, PMember.init) == PrivateSpawnResult.capacityFull, "ports cohort full");
            namedCheck(s.c.results(ctx).length == s.n - 1, "ports late spawn keeps result length");
            namedCheck(s.c.start!portsTag(ctx) == PrivateStartResult.started, "ports basic wave 2");
            return PrivateRootDisposition.again; // continue on the polling path
        }
        case 2:
        {
            if (!s.c.finished(ctx)) { --s.phase; return PrivateRootDisposition.again; }
            auto r = s.c.results(ctx);
            namedCheck(r.length == s.n, "ports wave 2 length includes the late member");
            foreach (i, v; r) namedCheck(v == portsValue(2, i), "ports wave 2 tag");
            namedCheck(s.c.start!portsTouch(ctx) == PrivateStartResult.started, "ports basic void wave");
            return PrivateRootDisposition.waitChildren;
        }
        case 3:
            namedCheck(s.c.results(ctx).length == 0, "ports void wave clears results");
            namedCheck(s.c.startFinal!portsTag(ctx) == PrivateStartResult.started, "ports basic final wave");
            return PrivateRootDisposition.waitChildren;
        case 4:
        {
            auto r = s.c.results(ctx);
            namedCheck(r.length == s.n, "ports final wave results survive retirement");
            foreach (i, v; r) namedCheck(v == portsValue(4, i), "ports final wave tag");
            namedCheck(s.c.start!portsTag(ctx) == PrivateStartResult.empty, "ports start after final is empty");
            namedCheck(s.c.results(ctx).length == s.n, "ports empty start keeps results");
            namedCheck(s.c.spawn(ctx, PMember(0)) == PrivateSpawnResult.spawned, "ports refill after final");
            namedCheck(s.c.results(ctx).length == s.n, "ports refill spawn keeps results");
            namedCheck(s.c.start!portsTag(ctx) == PrivateStartResult.started, "ports basic refill wave");
            return PrivateRootDisposition.waitChildren;
        }
        case 5:
        {
            auto r = s.c.results(ctx);
            namedCheck(r.length == 1 && r[0] == portsValue(1, 0), "ports refill wave result replaces old results");
            atomicFetchAdd(*s.done, 1UL); // roots finish on different consumers
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "ports basic phases");
            return PrivateRootDisposition.finish;
    }
}
void portsBasic(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 4, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto workers = new Workers(farm, 3);
    scope(exit) workers.join();
    auto park = PrivateRootPark!(PBasic, portsBasicStep, reclaim).create(farm, 3, 2, policy(counts));
    check(park !is null);
    shared ulong done;
    foreach (n; [2UL, 5, 67]) // 67 > the child limit 64: several tables per wave
        check(park.spawn(PBasic(null, 0, n, &done)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "ports basic timeout");
    }
    namedCheck(atomicLoad(done) == 3, "ports basic all roots completed");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports basic allocation balance");
}

// ---- shared control for the single-cohort polling tests (T2, T4) -----------------------
struct PtControl
{
    shared uint observeBusy, busySeen;
    shared ulong writerTid;
    ulong readerTid;
    uint calls, reads;
    bool sameConsumerOk; // set when no consumer is held back, so any consumer may read
}
struct PtMember { PtControl* ctl; uint index, wave; }
ulong ptTag(scope ref PrivateBorrow!PtMember m, scope ref PrivateContext) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.raw)(m.value.ctl.writerTid, portsThreadId());
    return portsValue(++m.value.wave, m.value.index);
}
struct PtRoot { PrivateChildPark!(PtMember, ulong)* c; PtControl* ctl; }
enum ptMembers = 3;
PrivateRootDisposition ptStep(scope ref PrivateBorrow!PtRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto k = b.value.ctl;
    ++k.calls;
    if (b.value.c is null)
    {
        b.value.c = ctx.createChildren!(PtMember, ulong)(ptMembers);
        namedCheck(b.value.c !is null, "ports poll create");
        foreach (i; 0 .. ptMembers)
            namedCheck(b.value.c.spawn(ctx, PtMember(k, cast(uint) i)) == PrivateSpawnResult.spawned, "ports poll spawn");
        namedCheck(b.value.c.start!ptTag(ctx) == PrivateStartResult.started, "ports poll start");
        return PrivateRootDisposition.again;
    }
    if (!b.value.c.finished(ctx))
    {
        if (atomicLoad!(MemoryOrder.raw)(k.observeBusy) != 0) atomicStore!(MemoryOrder.raw)(k.busySeen, 1u);
        return PrivateRootDisposition.again;
    }
    // Plain slot reads on this consumer, written on another one.
    auto r = b.value.c.results(ctx);
    namedCheck(r.length == ptMembers, "ports poll result length");
    foreach (i, v; r) namedCheck(v == portsValue(1, i), "ports poll joined tagged slot");
    immutable writer = atomicLoad!(MemoryOrder.raw)(k.writerTid);
    namedCheck(writer != 0 && (k.sameConsumerOk || writer != portsThreadId()),
        "ports poll reader is not the writer's consumer");
    k.readerTid = portsThreadId();
    ++k.reads;
    return PrivateRootDisposition.finish;
}

version (AntfarmActorTestHooks)
{
    // ---- T2: ports-poll-visibility (TSan, deterministic) --------------------------------
    // Selector: ports-poll-visibility. A polling root runs on this consumer
    // (A); the cohort's wave runs on consumer B, held at
    // privateWaveBeforeIdlePublish, i.e. after every slot write and after the
    // last table credit but before the idle release. The root observes
    // `finished == false` through raw flags, B is released with a raw flag,
    // and the root then reads the plain slots. Only the library's
    // finished/results acquire can order B's slot writes before A's reads.
    // Catching mutation P-M5: `finished` and `results` loads changed from
    // MemoryOrder.acq to MemoryOrder.raw. The TSan binary reports a data race
    // between the slot write in runChild (B) and the plain slot read in ptStep
    // (A) in 5 of 5 runs (ports-poll-visibility), 3 of 3 in ports-basic and
    // 3 of 3 in ports-late-tail; the unmutated TSan binary is clean. Non-TSan
    // builds cannot see it (x86 keeps the order).
    void portsPollVisibility()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        PtControl c;
        auto park = PrivateRootPark!(PtRoot, ptStep).create(farm, 1, 256, policy(counts));
        check(park !is null);
        check(park.spawn(PtRoot(null, &c)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {} // first callback starts the wave on this consumer
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateWaveBeforeIdlePublish;
        hook.armed = 1;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        shared uint ready, go, leave;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "ports poll worker go timeout");
            auto deadline = MonoTime.currTime + 10.seconds;
            while (atomicLoad!(MemoryOrder.raw)(hook.arrived) == 0)
            {
                other.consumeNext();
                namedCheck(MonoTime.currTime < deadline, "ports poll child claim timeout");
            }
            awaitRaw(leave, 1u, "ports poll worker leave timeout");
        });
        worker.start();
        awaitRaw(ready, 1u, "ports poll worker ready");
        namedCheck(park.pump(token) == 1, "ports poll polling root admission"); // child table, then root table
        atomicStore!(MemoryOrder.raw)(go, 1u);
        awaitRaw(hook.arrived, 1u, "ports poll final-credit hook");
        atomicStore!(MemoryOrder.raw)(c.observeBusy, 1u);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.raw)(c.busySeen) == 0)
        {
            view.consumeNext();
            namedCheck(MonoTime.currTime < deadline, "ports poll busy observation timeout");
        }
        namedCheck(c.reads == 0, "ports poll has not read before release");
        atomicStore!(MemoryOrder.raw)(c.observeBusy, 0u);
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "ports poll completion timeout");
        }
        namedCheck(c.reads == 1 && c.readerTid == portsThreadId(), "ports poll read on the root's consumer");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports poll allocation balance");
    }
}

// ---- T3: ports-wait-visibility (TSan, deterministic) ------------------------------------------
// Root with two direct result cohorts: `first` (one leaf) and `second` (one
// member that starts a grandchild result cohort). Phase 1 reads both result
// slices after waitChildren and starts a final fold wave in which the
// `second` member reads its grandchild's slot; phase 2 reads the fold.
struct PwControl
{
    uint calls;
    shared uint inCall, childArrived, childGo, rootArrived, rootGo, ran, workerDone;
    shared ulong[3] tid; // leaf, nested member, grandchild leaf
    bool holdRoot, holdLeaf;
}
struct PwLeaf { PwControl* c; uint index, wave, tidSlot; bool hold; }
ulong pwLeaf(scope ref PrivateBorrow!PwLeaf b, scope ref PrivateContext) nothrow @nogc @system
{
    auto c = b.value.c;
    if (b.value.hold)
    {
        atomicStore!(MemoryOrder.raw)(c.childArrived, 1u);
        awaitRaw(c.childGo, 1u, "ports wait child barrier");
    }
    atomicStore!(MemoryOrder.raw)(c.tid[b.value.tidSlot], portsThreadId());
    atomicFetchAdd!(MemoryOrder.raw)(c.ran, 1u);
    return portsValue(++b.value.wave, b.value.index);
}
struct PwNested { PwControl* c; PrivateChildPark!(PwLeaf, ulong)* leaves; uint wave; }
ulong pwNested(scope ref PrivateBorrow!PwNested b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto c = b.value.c;
    auto p = ctx.createChildren!(PwLeaf, ulong)(1);
    namedCheck(p !is null, "ports wait grandchild create");
    b.value.leaves = p;
    namedCheck(p.spawn(ctx, PwLeaf(c, 7, 0, 2, false)) == PrivateSpawnResult.spawned, "ports wait grandchild spawn");
    namedCheck(p.startFinal!pwLeaf(ctx) == PrivateStartResult.started, "ports wait grandchild start");
    atomicStore!(MemoryOrder.raw)(c.tid[1], portsThreadId());
    atomicFetchAdd!(MemoryOrder.raw)(c.ran, 1u);
    return portsValue(++b.value.wave, 5);
}
ulong pwFold(scope ref PrivateBorrow!PwNested b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto r = b.value.leaves.results(ctx); // retiring context, grandchild joined by the previous wave
    namedCheck(r.length == 1 && r[0] == portsValue(1, 7), "ports wait grandchild slot visible to the fold");
    return portsValue(++b.value.wave, 6);
}
struct PwRoot
{
    PwControl* c;
    PrivateChildPark!(PwLeaf, ulong)* first;
    PrivateChildPark!(PwNested, ulong)* second;
}
PrivateRootDisposition pwStep(scope ref PrivateBorrow!PwRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto c = b.value.c;
    namedCheck(atomicFetchAdd!(MemoryOrder.raw)(c.inCall, 1u) == 0, "ports wait no concurrent root dispatch");
    scope(exit) atomicFetchSub!(MemoryOrder.raw)(c.inCall, 1u);
    switch (++c.calls)
    {
        case 1:
            b.value.first = ctx.createChildren!(PwLeaf, ulong)(1);
            b.value.second = ctx.createChildren!(PwNested, ulong)(1);
            namedCheck(b.value.first !is null && b.value.second !is null, "ports wait cohorts");
            namedCheck(b.value.first.spawn(ctx, PwLeaf(c, 0, 0, 0, c.holdLeaf)) == PrivateSpawnResult.spawned, "ports wait leaf spawn");
            namedCheck(b.value.second.spawn(ctx, PwNested(c)) == PrivateSpawnResult.spawned, "ports wait nested spawn");
            namedCheck(b.value.first.start!pwLeaf(ctx) == PrivateStartResult.started, "ports wait first start");
            namedCheck(b.value.second.start!pwNested(ctx) == PrivateStartResult.started, "ports wait second start");
            if (c.holdRoot)
            {
                atomicStore!(MemoryOrder.raw)(c.rootArrived, 1u);
                awaitRaw(c.rootGo, 1u, "ports wait callback barrier");
            }
            return PrivateRootDisposition.waitChildren;
        case 2:
        {
            auto first = b.value.first.results(ctx);
            auto second = b.value.second.results(ctx);
            namedCheck(first.length == 1 && first[0] == portsValue(1, 0), "ports wait joined direct result");
            namedCheck(second.length == 1 && second[0] == portsValue(1, 5), "ports wait joined nested member result");
            foreach (i; 0 .. 3)
            {
                immutable writer = atomicLoad!(MemoryOrder.raw)(c.tid[i]);
                namedCheck(writer != 0 && writer != portsThreadId(), "ports wait root resumed on a consumer that wrote nothing");
            }
            namedCheck(b.value.second.startFinal!pwFold(ctx) == PrivateStartResult.started, "ports wait fold start");
            return PrivateRootDisposition.waitChildren;
        }
        case 3:
        {
            auto second = b.value.second.results(ctx);
            namedCheck(second.length == 1 && second[0] == portsValue(2, 6), "ports wait fold result");
            auto first = b.value.first.results(ctx);
            namedCheck(first.length == 1 && first[0] == portsValue(1, 0), "ports wait untouched cohort keeps results");
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "ports wait exactly three root calls");
            return PrivateRootDisposition.finish;
    }
}

version (AntfarmActorTestHooks)
{
    // Selector: ports-wait-visibility. Two E1 schedules with result reads.
    //  BEFORE custody: the root's first callback runs on consumer W and is held
    //  after both waves were started; every child and grandchild runs on this
    //  consumer (M) and joins (both direct waves release their root credit)
    //  before the callback returns. After release only W consumes, so the
    //  collector's custody release is the final credit and the root resumes
    //  on W, never on a writer's consumer. The fold also runs on W.
    //  AFTER custody: the first callback and the custody release happen on M,
    //  every child and grandchild runs on W (held at a raw barrier first) and
    //  W's last wave finisher publishes the root wake; once W has run its
    //  three operations only M consumes, so the root resumes on M.
    // Catching mutations:
    //   P-M6 a `memset(results_, 0, count_ * R.sizeof)` added in start after
    //        `service.enqueue(wave)` (a plausible "clear stale results" bug):
    //        the TSan binary reports the memset racing the child's slot write
    //        in runChild in 4 of 4 runs here and 2 of 2 in ports-basic. The
    //        non-TSan binaries still PASS (the clear finishes microseconds
    //        before any child can run), so this one is TSan-only evidence.
    //   E1-M1 (delete the collector's `releaseRoot(entry)`) and E1-M2 (bare
    //        decrement there) fail "ports wait completion deadline"; E1-M3
    //        (bare decrement in PrivateChildPark.finish) fails "ports wait
    //        callback barrier" because the root's both-waves-joined wait
    //        never completes. All three applied by hand to this selector.
    void portsWaitVisibility(bool beforeCustody)
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        PwControl c;
        c.holdRoot = beforeCustody;
        c.holdLeaf = !beforeCustody;
        auto park = PrivateRootPark!(PwRoot, pwStep).create(farm, 1, 1, policy(counts));
        check(park !is null);
        check(park.spawn(PwRoot(&c)) == PrivateSpawnResult.spawned);
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateRootCreditBeforeRelease;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        scope(exit) { setActorTestHook(null); privateControl = null; }
        shared uint ready, go, leave;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "ports wait worker go");
            auto deadline = MonoTime.currTime + 20.seconds;
            if (beforeCustody)
                while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
            else
            {
                while (atomicLoad!(MemoryOrder.raw)(c.ran) < 3)
                {
                    other.consumeNext();
                    namedCheck(MonoTime.currTime < deadline, "ports wait worker progress");
                }
                atomicStore!(MemoryOrder.raw)(c.workerDone, 1u);
                awaitRaw(leave, 1u, "ports wait worker leave");
            }
        });
        auto deadline = MonoTime.currTime + 20.seconds;
        if (beforeCustody)
        {
            worker.start(); awaitRaw(ready, 1u, "ports wait worker ready");
            namedCheck(park.pump(token) == 1, "ports wait root published");
            atomicStore!(MemoryOrder.raw)(go, 1u); // W claims the root table
            awaitRaw(c.rootArrived, 1u, "ports wait root callback held");
            while (atomicLoad!(MemoryOrder.raw)(hook.calls) < 2)
            {
                namedCheck(park.pump(token) == 0, "ports wait no root requeue before table join");
                while (view.consumeNext()) {}
                namedCheck(MonoTime.currTime < deadline, "ports wait both waves joined before custody");
            }
            atomicStore!(MemoryOrder.raw)(c.rootGo, 1u);
            while (park.live)
            {
                park.pump(token); // this consumer never consumes again
                namedCheck(MonoTime.currTime < deadline, "ports wait completion deadline");
            }
        }
        else
        {
            namedCheck(park.pump(token) == 1, "ports wait root published");
            while (view.consumeNext()) {} // first callback on this consumer
            namedCheck(c.calls == 1, "ports wait first callback ran here");
            worker.start(); awaitRaw(ready, 1u, "ports wait worker ready");
            namedCheck(park.pump(token) == 0, "ports wait custody released, waves published, no root ready");
            namedCheck(atomicLoad!(MemoryOrder.raw)(hook.calls) == 1, "ports wait only custody released before child join");
            atomicStore!(MemoryOrder.raw)(go, 1u);
            awaitRaw(c.childArrived, 1u, "ports wait leaf held on the worker");
            atomicStore!(MemoryOrder.raw)(c.childGo, 1u);
            while (atomicLoad!(MemoryOrder.raw)(c.workerDone) == 0)
            {
                park.pump(token); // emits the grandchild wave for the worker
                namedCheck(MonoTime.currTime < deadline, "ports wait worker finished its operations");
            }
            while (park.live)
            {
                park.pump(token);
                while (view.consumeNext()) {}
                namedCheck(MonoTime.currTime < deadline, "ports wait completion deadline");
            }
        }
        namedCheck(c.calls == 3, "ports wait exactly three root calls");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports wait allocation balance");
    }

    // ---- T4: ports-late-tail (TSan, deterministic) ----------------------------------------
    // Selector: ports-late-tail. The wave finisher (consumer W) is paused at
    // privateRootAfterIdleBeforeCredit: the idle release is out, the root credit
    // is not. The polling parent (this consumer) sees `finished`, reads the
    // results and returns strict `finish`, which frees the cohort, its entry
    // array AND its result slots while W is still inside `finish`. The Entry
    // stays pinned (`live == 1`, no premature reuse) until W releases. Both
    // reclamation policies, then a second generation reusing the root slot.
    // Catching mutation P-M7: result bookkeeping moved into PrivateChildPark.finish
    // after the idle store (`park.resultCount_ = park.count_;`), applied once
    // between the store and the hook and once after the hook. The TSan binary
    // reports (a) a data race between that write and the parent's read of
    // `resultCount_` in `results` (4 of 4) and (b) a heap-use-after-free of
    // the cohort the parent already disposed (4 of 4). Non-TSan builds pass.
    // (A self-assignment `resultCount_ = resultCount_` is optimised away, so it
    // is not a valid mutant.)
    void portsLateTail(PrivateRootReclamation reclaim)()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        PtControl c;
        auto park = PrivateRootPark!(PtRoot, ptStep, reclaim).create(farm, 1, 256, policy(counts));
        check(park !is null);
        check(park.spawn(PtRoot(null, &c)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {}
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateRootAfterIdleBeforeCredit;
        hook.armed = 1;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        shared uint ready, go, leave;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "ports tail worker go");
            while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
        });
        worker.start();
        awaitRaw(ready, 1u, "ports tail worker ready");
        check(park.pump(token) == 1); // child table, then polling root
        immutable freedBefore = atomicLoad(counts.freed);
        atomicStore!(MemoryOrder.raw)(go, 1u);
        awaitRaw(hook.arrived, 1u, "ports tail idle tail paused");
        while (view.consumeNext()) {}
        park.poll();
        namedCheck(c.reads == 1 && c.calls == 2, "ports tail parent read results after idle");
        // Three cohort blocks (park, entries, results) plus three member states.
        namedCheck(atomicLoad(counts.freed) >= freedBefore + 6, "ports tail cohort and slots freed before the tail credit");
        namedCheck(park.live == 1, "ports tail Entry pinned past cohort disposal");
        namedCheck(park.spawn(PtRoot(null, &c)) == PrivateSpawnResult.capacityFull, "ports tail no premature reuse");
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.poll();
            namedCheck(MonoTime.currTime < deadline, "ports tail completion");
        }
        // Second generation through the recycled Entry while W is still alive.
        PtControl again;
        again.sameConsumerOk = true;
        check(park.spawn(PtRoot(null, &again)) == PrivateSpawnResult.spawned);
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "ports tail reused completion");
        }
        namedCheck(again.reads == 1, "ports tail reused root read results");
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports tail allocation balance");
    }
}

// ---- T5: ports-large-cohort ----------------------------------------------------------------------
struct PlControl { size_t count, limit; ulong* ran; uint completed, busy; }
struct PlMember { uint index, wave; ulong* ran; }
ulong plOp(scope ref PrivateBorrow!PlMember m, scope ref PrivateContext) nothrow @nogc @system
{
    ++m.value.ran[m.value.index]; // external counter: observable while the wave is active
    return portsValue(++m.value.wave, m.value.index);
}
struct PlRoot { PrivateChildPark!(PlMember, ulong)* c; PlControl* ctl; uint phase; }
PrivateRootDisposition plStep(scope ref PrivateBorrow!PlRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto k = b.value.ctl;
    if (b.value.c is null)
    {
        b.value.c = ctx.createChildren!(PlMember, ulong)(k.count, k.limit);
        namedCheck(b.value.c !is null, "ports large create");
        foreach (i; 0 .. k.count)
            namedCheck(b.value.c.spawn(ctx, PlMember(cast(uint) i, 0, k.ran)) == PrivateSpawnResult.spawned, "ports large spawn");
        namedCheck(b.value.c.start!plOp(ctx) == PrivateStartResult.started, "ports large first start");
        b.value.phase = 1;
        return PrivateRootDisposition.again;
    }
    if (!b.value.c.finished(ctx))
    {
        ++k.busy;
        return PrivateRootDisposition.again;
    }
    immutable wave = b.value.phase;
    auto r = b.value.c.results(ctx);
    namedCheck(r.length == k.count, "ports large result length");
    foreach (i, v; r)
    {
        // Mutation P-M8 (slot index = cursor_ or position within the table)
        // and P-M9 (a requeued prefix dispatched twice) both break this tag.
        namedCheck(v == portsValue(wave, i), "ports large generation/index tag");
        namedCheck(k.ran[i] == wave, "ports large exactly once per wave");
    }
    k.completed = wave;
    if (wave == 4) return PrivateRootDisposition.finish;
    auto result = wave == 3 ? b.value.c.startFinal!plOp(ctx) : b.value.c.start!plOp(ctx);
    namedCheck(result == PrivateStartResult.started, "ports large reuse start");
    ++b.value.phase;
    return PrivateRootDisposition.again;
}

// Selector: ports-large-cohort. Same Farm pre-fill scaffolding as
// childLargeCohort: a zero-admission emit (credit released, no child runs),
// then a partial table once quota is refreshed, retry, then three waves plus
// a final one; every member tag of every wave is checked, at several child
// batch limits (including 1 and 64) and 257, 513 and 1025 members.
// Catching mutations:
//   P-M8 runChild writes `park.results_[park.cursor_]` or
//        `[(entry - park.entries_) % park.batchLimit_]` (position within the
//        table): both fail "ports large generation/index tag" on the first
//        multi-table wave (limit < count); the second also fails
//        "ports wave 1 tag" in ports-basic (67 members, limit 64).
//   P-M9 emit advances `cursor_` by `written - 1` for a partial prefix (the
//        last accepted member is requeued and dispatched twice, so its
//        own wave counter reads 2): fails "ports large generation/index tag"
//        in the limit-256 runs, where the quota admits a strict prefix. The
//        older `cursor_ += n` mutation fails the same check.
void portsLargeCohort(size_t count, size_t limit)
{
    Counts counts;
    // Compact ST geometry (SPEC 2f): a 256-child one-word ST table costs
    // 38 + 3*256 = 806 words, so a 713-word quota admits a strict 225-word
    // prefix. Other limits fit whole and are checked only against the limit.
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 713);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto ran = new ulong[count];
    PlControl control = PlControl(count, limit, ran.ptr);
    auto parent = PrivateRootPark!(PlRoot, plStep).create(farm, 1, 256, policy(counts));
    check(parent !is null);
    check(parent.spawn(PlRoot(null, &control)) == PrivateSpawnResult.spawned);
    check(parent.pump(token) == 1);
    while (view.consumeNext()) {}

    enum fillCount = 100000;
    auto fillResults = new ulong[fillCount];
    auto filler = Park.create(farm, fillCount, 256);
    check(filler !is null);
    foreach (i; 0 .. fillCount) check(filler.spawn(State(fillResults.ptr + i, 1, 0)) == PrivateSpawnResult.spawned);
    size_t admitted;
    auto deadline = MonoTime.currTime + 30.seconds;
    while (true)
    {
        auto n = filler.pump(token);
        if (n == 0) break;
        admitted += n;
        namedCheck(MonoTime.currTime < deadline, "ports large prefill timeout");
    }
    namedCheck(admitted > 0 && admitted < fillCount, "ports large Farm prefilled");
    version (AntfarmActorTestHooks)
    {
        PrivateHookControl hook;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        namedCheck(parent.pump(token) == 0, "ports large full Farm rejects child and polling root tables");
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.creditCalls) == 1, "ports large zero admission credit released");
        setActorTestHook(null);
        privateControl = null;
    }
    else namedCheck(parent.pump(token) == 0, "ports large full Farm rejects child and polling root tables");
    foreach (v; ran) namedCheck(v == 0, "ports large zero admission invokes no child");
    while (filler.live)
    {
        while (view.consumeNext()) {}
        filler.pump(token);
        namedCheck(MonoTime.currTime < deadline, "ports large prefill drain timeout");
    }
    filler.destroy();
    namedCheck(parent.pump(token) == 1, "ports large partial child admission after quota refresh");
    while (view.consumeNext()) {}
    size_t prefix;
    foreach (v; ran) { namedCheck(v <= 1, "ports large partial prefix no duplicates"); prefix += v; }
    namedCheck(prefix > 0 && prefix < count && prefix <= limit, "ports large first table bounded by the child limit");
    if (limit == 256) namedCheck(prefix < limit, "ports large quota admits a strict prefix");
    namedCheck(control.completed == 0 && control.busy > 0, "ports large partial wave stays active");
    while (parent.live)
    {
        parent.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "ports large retry timeout");
    }
    namedCheck(control.completed == 4, "ports large three waves plus final");
    foreach (v; ran) namedCheck(v == 4, "ports large every member dispatched four times");
    parent.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports large allocation balance");
}
void portsLargeCohorts()
{
    foreach (count; [257UL, 513, 1025])
        foreach (limit; [1UL, 64, 256])
        {
            version (TSan) if (limit == 1 && count > 257) continue; // 4100 tables each: covered by limit 64
            portsLargeCohort(count, limit);
        }
}

// ---- T6: ports-nested ------------------------------------------------------------------------------
// Depth-3 pipelined reduction (design §7.3, U4) on five workers. L1 members
// own L2 cohorts which own L3 leaves. Wave 1 builds the tree (each level
// starts its children). Wave 2 starts the L2 cohort's FINAL fold: each L2
// member, a retiring context, folds its own leaves' results. Wave 3 is the
// L1 cohort's FINAL wave: each L1 member, also retiring, folds the L2
// results written in wave 2 (joined by wave 2). The root reads L1 results
// after every wave and checks the closed-form sum.
// Catching mutation P-M10: `results` rejects a retiring context. Applied as
// a fatal ("private child results: retiring": the wave-2 fold dies) and as an
// empty slice (fails "ports nested L2 fold length").
enum n3Top = 3, n3Mid = 4, n3Leaf = 5;
ulong n3LeafOf(size_t mid, size_t k) pure nothrow @nogc @safe { return mid * n3Leaf + k + 1; }
ulong n3MidSum(size_t mid) pure nothrow @nogc @safe
{ ulong s; foreach (k; 0 .. n3Leaf) s += n3LeafOf(mid, k); return s; }
ulong n3TopSum(size_t top) pure nothrow @nogc @safe
{ ulong s; foreach (j; 0 .. n3Mid) s += n3MidSum(top * n3Mid + j); return s; }
struct N3Leaf { ulong value; }
ulong n3LeafValue(scope ref PrivateBorrow!N3Leaf b, scope ref PrivateContext) nothrow @nogc @system
{ return b.value.value; }
struct N3Mid { PrivateChildPark!(N3Leaf, ulong)* leaves; uint id, wave; }
ulong n3MidLaunch(scope ref PrivateBorrow!N3Mid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    b.value.leaves = ctx.createChildren!(N3Leaf, ulong)(n3Leaf);
    namedCheck(b.value.leaves !is null, "ports nested leaf cohort");
    foreach (k; 0 .. n3Leaf)
        namedCheck(b.value.leaves.spawn(ctx, N3Leaf(n3LeafOf(b.value.id, k))) == PrivateSpawnResult.spawned, "ports nested leaf spawn");
    namedCheck(b.value.leaves.startFinal!n3LeafValue(ctx) == PrivateStartResult.started, "ports nested leaf wave");
    return portsValue(++b.value.wave, b.value.id);
}
ulong n3MidFold(scope ref PrivateBorrow!N3Mid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto r = b.value.leaves.results(ctx); // retiring context folding its own cohort
    namedCheck(r.length == n3Leaf, "ports nested L2 fold length");
    ulong sum;
    foreach (k, v; r) { namedCheck(v == n3LeafOf(b.value.id, k), "ports nested L3 value and order"); sum += v; }
    return sum;
}
struct N3Top { PrivateChildPark!(N3Mid, ulong)* mids; uint id, wave; }
ulong n3TopLaunch(scope ref PrivateBorrow!N3Top b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    b.value.mids = ctx.createChildren!(N3Mid, ulong)(n3Mid);
    namedCheck(b.value.mids !is null, "ports nested mid cohort");
    foreach (j; 0 .. n3Mid)
        namedCheck(b.value.mids.spawn(ctx, N3Mid(null, cast(uint) (b.value.id * n3Mid + j))) == PrivateSpawnResult.spawned, "ports nested mid spawn");
    namedCheck(b.value.mids.start!n3MidLaunch(ctx) == PrivateStartResult.started, "ports nested mid wave");
    return portsValue(++b.value.wave, b.value.id);
}
ulong n3TopFoldMids(scope ref PrivateBorrow!N3Top b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto r = b.value.mids.results(ctx); // wave 1 tags, joined transitively by the root's wave 1
    namedCheck(r.length == n3Mid, "ports nested L2 wave 1 results visible");
    foreach (j, v; r) namedCheck(v == portsValue(1, b.value.id * n3Mid + j), "ports nested L2 wave 1 tag");
    namedCheck(b.value.mids.startFinal!n3MidFold(ctx) == PrivateStartResult.started, "ports nested final fold wave");
    return portsValue(++b.value.wave, b.value.id);
}
ulong n3TopRead(scope ref PrivateBorrow!N3Top b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto r = b.value.mids.results(ctx); // final fold results, retiring L1 context
    namedCheck(r.length == n3Mid, "ports nested L1 read length");
    ulong sum;
    foreach (j, v; r) { namedCheck(v == n3MidSum(b.value.id * n3Mid + j), "ports nested L2 fold value"); sum += v; }
    return sum;
}
struct N3Root { PrivateChildPark!(N3Top, ulong)* tops; uint phase; shared ulong* total; }
PrivateRootDisposition n3Step(scope ref PrivateBorrow!N3Root b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.tops = ctx.createChildren!(N3Top, ulong)(n3Top);
        namedCheck(s.tops !is null, "ports nested top cohort");
        foreach (i; 0 .. n3Top)
            namedCheck(s.tops.spawn(ctx, N3Top(null, cast(uint) i)) == PrivateSpawnResult.spawned, "ports nested top spawn");
        namedCheck(s.tops.start!n3TopLaunch(ctx) == PrivateStartResult.started, "ports nested wave 1");
    }
    else if (s.phase == 1)
    {
        auto r = s.tops.results(ctx);
        namedCheck(r.length == n3Top, "ports nested wave 1 length");
        foreach (i, v; r) namedCheck(v == portsValue(1, i), "ports nested wave 1 tag");
        namedCheck(s.tops.start!n3TopFoldMids(ctx) == PrivateStartResult.started, "ports nested wave 2");
    }
    else if (s.phase == 2)
    {
        auto r = s.tops.results(ctx);
        namedCheck(r.length == n3Top, "ports nested wave 2 length");
        foreach (i, v; r) namedCheck(v == portsValue(2, i), "ports nested wave 2 tag");
        namedCheck(s.tops.startFinal!n3TopRead(ctx) == PrivateStartResult.started, "ports nested wave 3");
    }
    else
    {
        auto r = s.tops.results(ctx);
        namedCheck(r.length == n3Top, "ports nested root read length");
        ulong sum;
        foreach (i, v; r) { namedCheck(v == n3TopSum(i), "ports nested pipelined fold"); sum += v; }
        atomicStore(*s.total, sum);
        return PrivateRootDisposition.finish;
    }
    ++s.phase;
    return PrivateRootDisposition.waitChildren;
}
void portsPipeline()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 6, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 5);
    scope(exit) workers.join();
    auto park = PrivateRootPark!(N3Root, n3Step).create(farm, 4, 4, policy(counts));
    check(park !is null);
    ulong expected;
    foreach (i; 0 .. n3Top) expected += n3TopSum(i);
    version (TSan) enum rounds = 3;
    else enum rounds = 20;
    foreach (round; 0 .. rounds)
    {
        shared ulong[4] totals;
        foreach (ref t; totals) check(park.spawn(N3Root(null, 0, &t)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 30.seconds;
        while (park.live)
        {
            park.pump(token);
            namedCheck(MonoTime.currTime < deadline, "ports nested pipeline timeout");
        }
        foreach (ref t; totals) namedCheck(atomicLoad(t) == expected, "ports nested pipeline total");
    }
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports nested allocation balance");
}

// Depth 64 with R = ulong: every level reads its child's results from the
// next wave of its own cohort (the child cohort is idle by the transitive
// join), and the retiring top member reads in the final wave.
// P-M10 is also caught here by the retiring top-level read in rDeepFinal;
// the mutation run recorded above stops at the first (L2 fold) failure.
struct RDeep { PrivateChildPark!(RDeep, ulong)* next; size_t depth; }
ulong rDeepDown(scope ref PrivateBorrow!RDeep b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto d = b.value.depth;
    if (d < 63)
    {
        b.value.next = ctx.createChildren!(RDeep, ulong)(1);
        namedCheck(b.value.next !is null, "ports depth64 create");
        namedCheck(b.value.next.spawn(ctx, RDeep(null, d + 1)) == PrivateSpawnResult.spawned, "ports depth64 spawn");
        namedCheck(b.value.next.start!rDeepDown(ctx) == PrivateStartResult.started, "ports depth64 descend");
    }
    return portsValue(1, d);
}
ulong rDeepCheck(scope ref PrivateBorrow!RDeep b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto d = b.value.depth;
    if (d < 63)
    {
        auto r = b.value.next.results(ctx);
        namedCheck(r.length == 1 && r[0] == portsValue(1, d + 1), "ports depth64 child result joined");
        namedCheck(b.value.next.start!rDeepCheck(ctx) == PrivateStartResult.started, "ports depth64 verify descend");
    }
    return portsValue(2, d);
}
ulong rDeepFinal(scope ref PrivateBorrow!RDeep b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto r = b.value.next.results(ctx);
    namedCheck(b.value.depth == 0 && r.length == 1 && r[0] == portsValue(2, 1), "ports depth64 retiring top reads child");
    return portsValue(3, 0);
}
struct RDeepRoot { PrivateChildPark!(RDeep, ulong)* chain; uint phase; }
PrivateRootDisposition rDeepStep(scope ref PrivateBorrow!RDeepRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.chain is null)
    {
        s.chain = ctx.createChildren!(RDeep, ulong)(1);
        namedCheck(s.chain !is null && s.chain.spawn(ctx, RDeep(null, 0)) == PrivateSpawnResult.spawned, "ports depth64 root cohort");
        namedCheck(s.chain.start!rDeepDown(ctx) == PrivateStartResult.started, "ports depth64 first wave");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    auto r = s.chain.results(ctx);
    namedCheck(r.length == 1 && r[0] == portsValue(s.phase, 0), "ports depth64 root read");
    if (s.phase == 1)
        namedCheck(s.chain.start!rDeepCheck(ctx) == PrivateStartResult.started, "ports depth64 second wave");
    else if (s.phase == 2)
        namedCheck(s.chain.startFinal!rDeepFinal(ctx) == PrivateStartResult.started, "ports depth64 final wave");
    else
    {
        namedCheck(s.phase == 3, "ports depth64 phases");
        return PrivateRootDisposition.finish;
    }
    ++s.phase;
    return PrivateRootDisposition.waitChildren;
}
void portsDepth64()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto park = PrivateRootPark!(RDeepRoot, rDeepStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(RDeepRoot()) == PrivateSpawnResult.spawned);
    auto workers = new Workers(farm, 2);
    scope(exit) workers.join();
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "ports depth64 timeout");
    }
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "ports depth64 allocation balance");
}

// ---- T7: ports-alloc-fail ----------------------------------------------------------------------------
// Fault budget walks the cohort allocations of createChildren!(T, ulong):
// the park, entries_, then results_. Each failure returns null with a
// balanced allocator count and the parent retries successfully; overflow of
// capacity * R.sizeof returns null before any allocation. The request
// sizes also pin the layout claims: a void cohort makes exactly two
// requests (park, capacity * 40-byte Entry), a result cohort a third of
// capacity * R.sizeof.
// Catching mutation P-M11: in create, the failed `results_` allocation
// frees `park` but not `entries_` (the inner `freeMemory(... entries_ ...)`
// deleted): the budget-2 attempt leaves allocated - freed == 1 and fails
// "ports alloc failed create balance" (observed).
struct SizeLog
{
    Counts counts;
    shared size_t requests;
    size_t[6] bytes;
}
void* sizeLogAllocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto log = cast(SizeLog*) ctx;
    auto i = atomicFetchAdd(log.requests, cast(size_t) 1);
    if (i < log.bytes.length) log.bytes[i] = bytes;
    return allocate(&log.counts, bytes, alignment);
}
void sizeLogDeallocate(void* ctx, void* ptr, size_t bytes, size_t alignment) nothrow @nogc @system
{ deallocate(&(cast(SizeLog*) ctx).counts, ptr, bytes, alignment); }
struct PaRoot { SizeLog* log; PrivateChildPark!(PMember, ulong)* c; uint phase; }
PrivateRootDisposition paStep(scope ref PrivateBorrow!PaRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto log = b.value.log;
    auto counts = &log.counts;
    if (b.value.phase++ == 1)
    {
        auto r = b.value.c.results(ctx);
        namedCheck(r.length == 3, "ports alloc retry result length");
        foreach (i, v; r) namedCheck(v == portsValue(1, i), "ports alloc retry tag");
        return PrivateRootDisposition.finish;
    }
    // Request sizes.
    static if (size_t.sizeof == 8)
    {
        atomicStore(log.requests, cast(size_t) 0);
        auto plain = ctx.createChildren!PMember(8);
        namedCheck(plain !is null && atomicLoad(log.requests) == 2, "ports void cohort makes two allocations");
        namedCheck(log.bytes[0] == PrivateChildPark!PMember.sizeof && log.bytes[1] == 8 * 40, "ports void cohort allocation sizes");
        atomicStore(log.requests, cast(size_t) 0);
        auto typed = ctx.createChildren!(PMember, PWide)(8);
        namedCheck(typed !is null && atomicLoad(log.requests) == 3, "ports result cohort makes three allocations");
        namedCheck(log.bytes[0] == PrivateChildPark!(PMember, PWide).sizeof && log.bytes[1] == 8 * 40
            && log.bytes[2] == 8 * pWideStorage, "ports result cohort allocation sizes");
    }
    immutable allocated = atomicLoad(counts.allocated);
    immutable freed = atomicLoad(counts.freed);
    namedCheck(ctx.createChildren!(PMember, ulong)(0) is null, "ports alloc zero capacity");
    namedCheck(ctx.createChildren!(PMember, ulong)(size_t.max) is null, "ports alloc entry overflow");
    // Overflows capacity * PWide.sizeof (128) but not capacity * Entry.sizeof (40).
    namedCheck(ctx.createChildren!(PMember, PWide)(size_t.max / 64) is null, "ports alloc result overflow");
    namedCheck(ctx.createChildren!(PMember, PWide)(size_t.max / 64, 3) is null, "ports alloc result overflow explicit limit");
    namedCheck(atomicLoad(counts.allocated) == allocated, "ports alloc invalid capacity allocates nothing");
    foreach (budget; 0 .. 3)
    {
        atomicStore(counts.budget, cast(long) budget);
        namedCheck(ctx.createChildren!(PMember, ulong)(4) is null, "ports alloc cohort/entries/results failure");
        namedCheck(atomicLoad(counts.allocated) - allocated == atomicLoad(counts.freed) - freed,
            "ports alloc failed create balance");
    }
    atomicStore(counts.budget, long.max);
    auto children = ctx.createChildren!(PMember, ulong)(3);
    namedCheck(children !is null, "ports alloc retry create");
    b.value.c = children;
    atomicStore(counts.budget, 0L);
    namedCheck(children.spawn(ctx, PMember(0)) == PrivateSpawnResult.allocationFailed, "ports alloc member state failure");
    namedCheck(children.results(ctx).length == 0, "ports alloc failed spawn leaves no results");
    namedCheck(children.start!portsTag(ctx) == PrivateStartResult.empty, "ports alloc failed slot stays empty");
    atomicStore(counts.budget, long.max);
    foreach (i; 0 .. 3)
        namedCheck(children.spawn(ctx, PMember(cast(uint) i)) == PrivateSpawnResult.spawned, "ports alloc retry spawn");
    namedCheck(children.startFinal!portsTag(ctx) == PrivateStartResult.started, "ports alloc retry start");
    return PrivateRootDisposition.waitChildren;
}
void portsAllocationFailures()
{
    SizeLog log;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(PaRoot, paStep).create(farm, 1, 256,
        ActorAllocator(&log, &sizeLogAllocate, &sizeLogDeallocate));
    check(park !is null);
    check(park.spawn(PaRoot(&log)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "ports alloc timeout");
    }
    park.destroy();
    namedCheck(atomicLoad(log.counts.allocated) == atomicLoad(log.counts.freed), "ports alloc total allocation balance");
}

// ---- D1/D2: death modes -----------------------------------------------------------------------------------
// Selectors ports-read-active, ports-wrong-parent-results,
// ports-default-results (run through deathHarness: LDC and DMD, debug and
// -release). The parent harness checks the nonzero death and the exact
// fatal text; the negative control is unchanged.
// P-M12 (D1): delete the `wave.active` check from `results`: the callback
//   survives, prints "read survived" and the harness check fails (observed in
//   the LDC and DMD, debug and -release binaries: ports-death aborts at the
//   exit-status/message check for ports-read-active).
// P-M13 (D2): delete `authorized(context)` from `results`: both a wrong
//   parent's context and a default context read an idle cohort and survive;
//   the harness check for ports-wrong-parent-results fails (observed in all
//   four binaries).
struct PdRoot { PrivateChildPark!(PMember, ulong)* c; }
PrivateRootDisposition portsReadActiveStep(scope ref PrivateBorrow!PdRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    b.value.c = ctx.createChildren!(PMember, ulong)(1);
    namedCheck(b.value.c !is null && b.value.c.spawn(ctx, PMember(0)) == PrivateSpawnResult.spawned, "ports death setup");
    namedCheck(b.value.c.results(ctx).length == 0, "ports death idle read is legal");
    // The wave is enqueued but cannot be emitted before this callback returns
    // (no worker consumers, single emitter), so it is active here.
    namedCheck(b.value.c.start!portsTag(ctx) == PrivateStartResult.started, "ports death start");
    fprintf(stderr, "ports read-active: wave started\n");
    cast(void) b.value.c.results(ctx); // fatal: wave active
    fprintf(stderr, "ports read-active: read survived\n");
    return PrivateRootDisposition.waitChildren;
}
void portsReadActiveDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(PdRoot, portsReadActiveStep).create(farm, 1);
    check(park !is null);
    check(park.spawn(PdRoot()) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
struct PdProbe { PrivateChildPark!(PMember, ulong)* children; bool useDefault; }
struct PdMisuse { PdProbe* probe; bool owner; }
PrivateRootDisposition portsMisuseStep(scope ref PrivateBorrow!PdMisuse b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto probe = b.value.probe;
    if (b.value.owner)
    {
        if (probe.children is null) probe.children = ctx.createChildren!(PMember, ulong)(1);
        namedCheck(probe.children !is null, "ports misuse owner cohort");
        return PrivateRootDisposition.again;
    }
    namedCheck(probe.children !is null, "ports misuse cohort exists");
    PrivateContext empty;
    if (probe.useDefault) cast(void) probe.children.results(empty);
    else cast(void) probe.children.results(ctx);
    fprintf(stderr, "ports misuse: read survived\n");
    return PrivateRootDisposition.finish;
}
void portsMisuseDeath(bool useDefault)
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    PdProbe probe;
    probe.useDefault = useDefault;
    auto park = PrivateRootPark!(PdMisuse, portsMisuseStep).create(farm, 2);
    check(park !is null);
    check(park.spawn(PdMisuse(&probe, true)) == PrivateSpawnResult.spawned);
    check(park.spawn(PdMisuse(&probe, false)) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
void portsDeath()
{
    version (linux)
    {
        auto probe = probeDeath("ports-read-active");
        check(probe.status != 0);
        check(probe.stderrText.canFind("ports read-active: wave started"));
        check(probe.stderrText.canFind("antfarm fatal: private child results: wave active"));
        check(!probe.stderrText.canFind("read survived"));
        foreach (modeName; ["ports-wrong-parent-results", "ports-default-results"])
        {
            auto misuse = probeDeath(modeName);
            check(misuse.status != 0);
            check(misuse.stderrText.canFind("antfarm fatal: private child park: unauthorized context"));
            check(!misuse.stderrText.canFind("read survived"));
        }
        writeln("PASS ports early read, wrong-parent and default-context results are fatal");
    }
    else
        writeln("SKIP ports death modes: /proc/self/exe re-exec requires Linux");
}

void phasePorts()
{
    portsBasic!(PrivateRootReclamation.emitter)();
    portsBasic!(PrivateRootReclamation.consumer)();
    writeln("PASS ports-basic results lifecycle under both reclamation policies");
    version (AntfarmActorTestHooks)
    {
        portsPollVisibility(); writeln("PASS ports-poll-visibility deterministic raw-barrier schedule");
        portsWaitVisibility(true); portsWaitVisibility(false);
        writeln("PASS ports-wait-visibility before/after custody schedules with result reads");
        portsLateTail!(PrivateRootReclamation.emitter)();
        portsLateTail!(PrivateRootReclamation.consumer)();
        writeln("PASS ports-late-tail results read and freed under a paused idle tail");
    }
    portsLargeCohorts(); writeln("PASS ports-large-cohort 257/513/1025 members, child limits 1/64/256");
    portsPipeline(); writeln("PASS ports-nested depth-3 pipelined reduction");
    portsDepth64(); writeln("PASS ports-nested depth64 results");
    portsAllocationFailures(); writeln("PASS ports-alloc-fail cohort/entries/results failures and overflow");
}

// ===========================================================================
// E2-1: destructors and non-POD state (design 86 section 5, tests 6.3 D1-D5).
// Every destructor records (id, thread) in a global log; id 0 is a moved-from
// or .init instance. The stored actor instance must be destroyed exactly once,
// on a consumer, never on the emitter. Caller-side values (failed-spawn
// arguments, moved-from parameters) are destroyed by the language on the
// calling thread and are counted separately (review 89, finding 5).
// Raw flags never supply a happens-before edge: log reads happen only after
// the library's join (park.live == 0 or a callback after `waitChildren`).
// Mutations are applied by hand to actors/private_actor.d; see each test.
// ===========================================================================
import core.lifetime : move;
import core.memory : GC;

enum dtOp = 1u << 20; // op-ran marker: id + dtOp
struct DtEvent { shared uint id; shared ulong tid; }
enum dtUnwritten = uint.max;
struct DtLog { shared size_t n; DtEvent[512] events; }
__gshared DtLog dtLog;
__gshared ulong dtEmitter; // the test's emitter thread
__gshared ulong dtCaller;  // the thread running the current parent callback

void dtReset() nothrow @nogc @system
{
    foreach (ref e; dtLog.events) atomicStore!(MemoryOrder.raw)(e.id, dtUnwritten);
    atomicStore(dtLog.n, cast(size_t) 0);
}
void dtRecord(uint id) nothrow @nogc @system
{
    auto i = atomicFetchAdd(dtLog.n, cast(size_t) 1);
    namedCheck(i < dtLog.events.length, "dtor log capacity");
    atomicStore!(MemoryOrder.raw)(dtLog.events[i].tid, portsThreadId());
    atomicStore!(MemoryOrder.rel)(dtLog.events[i].id, id);
}
// Published events only; a slot reserved by a concurrent destructor is skipped.
uint dtId(size_t i) nothrow @nogc @system { return atomicLoad!(MemoryOrder.acq)(dtLog.events[i].id); }
size_t dtCount(uint id) nothrow @nogc @system
{
    size_t c;
    foreach (i; 0 .. atomicLoad(dtLog.n)) if (dtId(i) == id) ++c;
    return c;
}
// Index/thread of the first event with this id; size_t.max / 0 if absent.
size_t dtPos(uint id) nothrow @nogc @system
{
    foreach (i; 0 .. atomicLoad(dtLog.n)) if (dtId(i) == id) return i;
    return size_t.max;
}
ulong dtTid(uint id) nothrow @nogc @system
{
    auto p = dtPos(id);
    return p == size_t.max ? 0 : atomicLoad!(MemoryOrder.raw)(dtLog.events[p].tid);
}
// Thread of the nth (0-based) event with this id.
ulong dtTidAt(uint id, size_t nth) nothrow @nogc @system
{
    foreach (i; 0 .. atomicLoad(dtLog.n))
        if (dtId(i) == id && nth-- == 0) return atomicLoad!(MemoryOrder.raw)(dtLog.events[i].tid);
    return 0;
}
// Stored instance: exactly one destructor, on a consumer (never the emitter).
void dtStoredOnConsumer(uint id, string what) nothrow @nogc @system
{
    namedCheck(dtCount(id) == 1, what);
    namedCheck(dtTid(id) != dtEmitter, what);
}
// Caller-side: exactly one destructor, on the thread that made the call.
void dtOnCaller(uint id, ulong caller, string what) nothrow @nogc @system
{
    namedCheck(dtCount(id) == 1, what);
    namedCheck(dtTid(id) == caller, what);
}

struct DtObj { uint id; ~this() nothrow @nogc @system { dtRecord(id); } }
struct DtKid { uint id; ~this() nothrow @nogc @system { dtRecord(id); } }
struct DtUnique
{
    uint id;
    @disable this(this);
    ~this() nothrow @nogc @system { dtRecord(id); }
}
void dtKidOp(scope ref PrivateBorrow!DtKid b, scope ref PrivateContext) nothrow @nogc @system
{ dtRecord(b.value.id + dtOp); }
ulong dtKidTag(scope ref PrivateBorrow!DtKid b, scope ref PrivateContext) nothrow @nogc @system
{ dtRecord(b.value.id + dtOp); return portsValue(1, b.value.id); }
void dtKidNop(scope ref PrivateBorrow!DtKid, scope ref PrivateContext) nothrow @nogc @system {}

PrivateRootDisposition dtFinish(T)(scope ref PrivateBorrow!T, scope ref PrivateContext)
    nothrow @nogc @system { return PrivateRootDisposition.finish; }

struct DtHarness
{
    AntFarm* farm;
    Token token;
    Workers workers;
    Counts counts;
    this(uint consumers)
    {
        farm = AntFarm.create(2, 8, consumers, 0, 0, 1, 4096);
        token = farm.registerProducer(Tier.small);
        workers = new Workers(farm, consumers);
        dtReset();
        dtEmitter = portsThreadId();
    }
    void drain(P)(P* park, string name)
    {
        auto deadline = MonoTime.currTime + 30.seconds;
        while (park.live)
        {
            park.pump(token);
            namedCheck(MonoTime.currTime < deadline, name);
        }
    }
    void close()
    {
        workers.join();
        farm.unregisterProducer(token);
        farm.destroy();
    }
}

// ---- D1a: roots: finish, finishAfterChildren, finish with never-dispatched members
// Selector: dtor-root. Both reclamation policies.
// Kind 0: `again` twice then `finish` (no children). Kind 1: starts a wave of
// two members and returns finishAfterChildren (cleanup activation destroys
// children, then the root). Kind 2: creates two members, never starts them,
// `finish`es (members destroyed by disposal while never dispatched).
// Catching mutations (each applied alone to actors/private_actor.d):
//   D-M1b runRoot's finish branch deletes `destroyState!T(entry);`: "dtor-root
//         stored exactly once" (observed: count 0).
//   D-M2  runRoot deletes `destroyState!T(entry);` and PrivateRootPark.reclaim
//         starts with `destroyState!T(entry);` (destructor on the emitter under
//         emitter reclamation): "dtor-root stored exactly once" (placement).
//   D-M1  (dispose loop skipping ~T) is caught by dtor-order, D-M3b by dtor-order.
struct DtRoot
{
    uint id;
    ubyte kind, step;
    PrivateChildPark!DtKid* kids;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
PrivateRootDisposition dtRootStep(scope ref PrivateBorrow!DtRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    dtRecord(s.id + dtOp);
    if (s.kind == 0) return ++s.step < 3 ? PrivateRootDisposition.again : PrivateRootDisposition.finish;
    s.kids = ctx.createChildren!DtKid(2);
    namedCheck(s.kids !is null, "dtor-root cohort");
    foreach (k; 1 .. 3)
        namedCheck(s.kids.spawn(ctx, DtKid(s.id * 10 + k)) == PrivateSpawnResult.spawned, "dtor-root spawn");
    if (s.kind == 2) return PrivateRootDisposition.finish;
    namedCheck(s.kids.start!dtKidOp(ctx) == PrivateStartResult.started, "dtor-root start");
    return PrivateRootDisposition.finishAfterChildren;
}
void dtorRoot(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto park = PrivateRootPark!(DtRoot, dtRootStep, reclaim).create(h.farm, 8, 8, policy(h.counts));
    check(park !is null);
    foreach (kind; 0 .. 3) foreach (r; 0 .. 2)
        check(park.spawn(DtRoot(cast(uint) ((kind + 1) * 10 + r), cast(ubyte) kind)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-root completion");
    foreach (kind; 0 .. 3) foreach (r; 0 .. 2)
    {
        immutable id = cast(uint) ((kind + 1) * 10 + r);
        dtStoredOnConsumer(id, "dtor-root stored exactly once");
        namedCheck(dtCount(id + dtOp) == (kind == 0 ? 3 : 1), "dtor-root exact user calls");
        if (kind == 0) continue;
        foreach (k; 1 .. 3)
        {
            immutable kid = id * 10 + k;
            dtStoredOnConsumer(kid, "dtor-root member stored exactly once");
            namedCheck(dtPos(kid) < dtPos(id), "dtor-root postorder");
            namedCheck(dtCount(kid + dtOp) == (kind == 1 ? 1 : 0), "dtor-root never-dispatched members stay idle");
            if (kind == 1) namedCheck(dtPos(kid + dtOp) < dtPos(kid), "dtor-root op before destructor");
        }
    }
    // 6 root and 8 member spawns, each leaves one moved-from .init destroyed by its caller.
    namedCheck(dtCount(0) == 6 + 8, "dtor-root moved-from sources destroyed on the caller");
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-root allocation balance");
}

// ---- D1b: child startFinal, plain and result ops
// Selector: dtor-child. Both reclamation policies.
// Phase 0: two cohorts (plain, R = ulong), non-final waves: no destructor
// yet. Phase 1: startFinal on both. Phase 2 (after the join): every member
// is already destroyed, on a consumer, after its op; the root then finishes.
// Catching mutations:
//   D-M1c retireMember deletes `destroyState!T(entry);`: "dtor-child final
//         destroyed exactly once before the join" (count 0).
//   D-M4  runChild's retiring branch calls `destroyState!T(entry);` once more
//         before `retireMember!T(entry);` (double destruction): the same check
//         (count 2).
//   D-M4b runChild calls `destroyState!T(entry);` after every operation, not
//         only when retiring: "dtor-child non-final waves never destroy".
struct DtParent
{
    uint id, phase;
    PrivateChildPark!DtKid* plain;
    PrivateChildPark!(DtKid, ulong)* typed;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
PrivateRootDisposition dtParentStep(scope ref PrivateBorrow!DtParent b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    switch (s.phase++)
    {
        case 0:
            s.plain = ctx.createChildren!DtKid(3);
            s.typed = ctx.createChildren!(DtKid, ulong)(3);
            namedCheck(s.plain !is null && s.typed !is null, "dtor-child cohorts");
            foreach (k; 1 .. 4)
            {
                namedCheck(s.plain.spawn(ctx, DtKid(s.id * 10 + k)) == PrivateSpawnResult.spawned, "dtor-child plain spawn");
                namedCheck(s.typed.spawn(ctx, DtKid(s.id * 10 + 3 + k)) == PrivateSpawnResult.spawned, "dtor-child typed spawn");
            }
            namedCheck(s.plain.start!dtKidNop(ctx) == PrivateStartResult.started, "dtor-child plain wave");
            namedCheck(s.typed.start!dtKidTag(ctx) == PrivateStartResult.started, "dtor-child typed wave");
            return PrivateRootDisposition.waitChildren;
        case 1:
            foreach (k; 1 .. 7)
                namedCheck(dtCount(s.id * 10 + k) == 0, "dtor-child non-final waves never destroy");
            namedCheck(s.plain.startFinal!dtKidOp(ctx) == PrivateStartResult.started, "dtor-child plain final");
            namedCheck(s.typed.startFinal!dtKidTag(ctx) == PrivateStartResult.started, "dtor-child typed final");
            return PrivateRootDisposition.waitChildren;
        case 2:
            foreach (k; 1 .. 7)
                namedCheck(dtCount(s.id * 10 + k) == 1, "dtor-child final destroyed exactly once before the join");
            auto r = s.typed.results(ctx);
            namedCheck(r.length == 3, "dtor-child results survive retirement");
            foreach (i, v; r) namedCheck(v == portsValue(1, s.id * 10 + 3 + 1 + cast(uint) i), "dtor-child result value");
            return PrivateRootDisposition.finish;
        default:
            namedCheck(false, "dtor-child phases");
            return PrivateRootDisposition.finish;
    }
}
void dtorChild(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto park = PrivateRootPark!(DtParent, dtParentStep, reclaim).create(h.farm, 2, 2, policy(h.counts));
    check(park !is null);
    foreach (id; 1 .. 3) check(park.spawn(DtParent(cast(uint) id)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-child completion");
    foreach (id; 1 .. 3) foreach (k; 1 .. 7)
    {
        immutable kid = cast(uint) (id * 10 + k);
        dtStoredOnConsumer(kid, "dtor-child member stored exactly once");
        namedCheck(dtCount(kid + dtOp) == 1 + (k > 3 ? 1 : 0), "dtor-child exact op count");
        namedCheck(dtPos(kid + dtOp) < dtPos(kid), "dtor-child op before destructor");
        namedCheck(dtPos(kid) < dtPos(cast(uint) id), "dtor-child members before the parent");
    }
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-child allocation balance");
}

// ---- D2: three-level postorder
// Selector: dtor-order. Both policies, three retirement paths.
// Top -> two mids -> two leaves each. Mid 10's leaves are never started
// (never-dispatched); mid 11's leaves ran a plain wave. Mode 0: top `finish`
// disposes the cohort tree. Mode 1: `startFinal` retires the mids first, the
// top is destroyed afterwards. Mode 2: finishAfterChildren cleanup activation.
// Catching mutations:
//   D-M1  in PrivateChildPark.dispose replace `retireMember!T(&entry)` by
//         disposeChildren + freeMemory + `entry.state = null` (no ~T): "dtor-order
//         stored exactly once" (never-dispatched leaves 100/101 included).
//   D-M3  retireMember runs `destroyState!T(entry);` before `disposeChildren(entry);`:
//         "dtor-order leaves before mid".
//   D-M3b the same swap in runRoot's finish branch: "dtor-order mids before top".
struct DtLeaf { uint id; ~this() nothrow @nogc @system { dtRecord(id); } }
struct DtMid
{
    uint id;
    PrivateChildPark!DtLeaf* leaves;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
struct DtTop
{
    uint id;
    ubyte mode, phase;
    PrivateChildPark!DtMid* mids;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
void dtLeafOp(scope ref PrivateBorrow!DtLeaf b, scope ref PrivateContext) nothrow @nogc @system
{ dtRecord(b.value.id + dtOp); }
void dtMidOp(scope ref PrivateBorrow!DtMid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto m = &b.value();
    m.leaves = ctx.createChildren!DtLeaf(2);
    namedCheck(m.leaves !is null, "dtor-order leaf cohort");
    foreach (k; 0 .. 2)
        namedCheck(m.leaves.spawn(ctx, DtLeaf(m.id * 10 + k)) == PrivateSpawnResult.spawned, "dtor-order leaf spawn");
    if (m.id == 11) // mid 10 keeps never-dispatched leaves
        namedCheck(m.leaves.start!dtLeafOp(ctx) == PrivateStartResult.started, "dtor-order leaf wave");
}
void dtMidNop(scope ref PrivateBorrow!DtMid, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition dtTopStep(scope ref PrivateBorrow!DtTop b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    switch (s.phase++)
    {
        case 0:
            s.mids = ctx.createChildren!DtMid(2);
            namedCheck(s.mids !is null, "dtor-order mid cohort");
            foreach (k; 0 .. 2)
                namedCheck(s.mids.spawn(ctx, DtMid(10 + k)) == PrivateSpawnResult.spawned, "dtor-order mid spawn");
            namedCheck(s.mids.start!dtMidOp(ctx) == PrivateStartResult.started, "dtor-order mid wave");
            return s.mode == 2 ? PrivateRootDisposition.finishAfterChildren : PrivateRootDisposition.waitChildren;
        case 1:
            if (s.mode == 0) return PrivateRootDisposition.finish;
            namedCheck(s.mids.startFinal!dtMidNop(ctx) == PrivateStartResult.started, "dtor-order mid retirement");
            return PrivateRootDisposition.waitChildren;
        default:
            foreach (id; [10u, 11u, 100u, 101u, 110u, 111u])
                namedCheck(dtCount(id) == 1 && dtCount(1) == 0, "dtor-order retired before the top's last callback");
            return PrivateRootDisposition.finish;
    }
}
void dtorOrder(PrivateRootReclamation reclaim)(ubyte mode)
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto park = PrivateRootPark!(DtTop, dtTopStep, reclaim).create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(DtTop(1, mode)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-order completion");
    foreach (id; [1u, 10u, 11u, 100u, 101u, 110u, 111u])
        dtStoredOnConsumer(id, "dtor-order stored exactly once");
    foreach (leaf; [100u, 101u]) namedCheck(dtPos(leaf) < dtPos(10), "dtor-order leaves before mid");
    foreach (leaf; [110u, 111u]) namedCheck(dtPos(leaf) < dtPos(11), "dtor-order leaves before mid");
    foreach (mid; [10u, 11u]) namedCheck(dtPos(mid) < dtPos(1), "dtor-order mids before top");
    foreach (leaf; [100u, 101u]) namedCheck(dtCount(leaf + dtOp) == 0, "dtor-order never-dispatched leaves stay idle");
    foreach (leaf; [110u, 111u])
    {
        namedCheck(dtCount(leaf + dtOp) == 1, "dtor-order dispatched leaves ran once");
        namedCheck(dtPos(leaf + dtOp) < dtPos(leaf), "dtor-order op before destructor");
    }
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-order allocation balance");
}

// ---- D3: failed spawn: the language destroys the argument on the caller
// Selector: dtor-failed-spawn. Both policies.
// Roots (caller = emitter): capacityFull, allocationFailed. Members (caller =
// the consumer running the parent callback): capacityFull, waveActive,
// allocationFailed. The argument is destroyed exactly once, on the calling
// thread, immediately; it is never stored, so disposal does not repeat it.
// Catching mutations:
//   D-M8guard  delete the `if (!spawned) .destroy(initial);` scope(exit)
//         in PrivateRootPark.spawn: the root failure produces only the owning
//         destructor (after entered_ clears), no [id, 0] pair:
//         "dtor-failed-spawn root failure: owner, then moved-from .init" /
//         the event-count check.
//   D-M8root / D-M8child  the capacityFull return in the root / child spawn
//         first calls `object.destroy!false(initial)` (an explicit extra
//         destruction of the argument): "dtor-failed-spawn ... capacityFull
//         argument destroyed once on the caller" (count 2). A spawn that stored
//         or leaked the argument would likewise change the count.
struct DtCaller { Counts* counts; PrivateChildPark!DtKid* a, b; }
PrivateRootDisposition dtCallerStep(scope ref PrivateBorrow!DtCaller b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    dtCaller = portsThreadId();
    s.a = ctx.createChildren!DtKid(1);
    s.b = ctx.createChildren!DtKid(2);
    namedCheck(s.a !is null && s.b !is null, "dtor-failed-spawn cohorts");
    namedCheck(s.a.spawn(ctx, DtKid(201)) == PrivateSpawnResult.spawned, "dtor-failed-spawn first member");
    namedCheck(s.a.spawn(ctx, DtKid(202)) == PrivateSpawnResult.capacityFull, "dtor-failed-spawn child capacityFull");
    dtOnCaller(202, dtCaller, "dtor-failed-spawn child capacityFull argument destroyed once on the caller");
    namedCheck(s.a.start!dtKidOp(ctx) == PrivateStartResult.started, "dtor-failed-spawn wave");
    namedCheck(s.a.spawn(ctx, DtKid(203)) == PrivateSpawnResult.waveActive, "dtor-failed-spawn child waveActive");
    dtOnCaller(203, dtCaller, "dtor-failed-spawn child waveActive argument destroyed once on the caller");
    atomicStore(s.counts.budget, 0L);
    namedCheck(s.b.spawn(ctx, DtKid(204)) == PrivateSpawnResult.allocationFailed, "dtor-failed-spawn child allocationFailed");
    atomicStore(s.counts.budget, long.max);
    dtOnCaller(204, dtCaller, "dtor-failed-spawn child allocationFailed argument destroyed once on the caller");
    namedCheck(s.b.start!dtKidOp(ctx) == PrivateStartResult.empty, "dtor-failed-spawn failed slot stays empty");
    return PrivateRootDisposition.finishAfterChildren;
}
// A failed root spawn destroys the argument through object.destroy inside
// spawn (before the reentrancy flag clears), which also resets the parameter
// to .init; that moved-from value is then destroyed on return: exactly
// [id, 0], both on the emitter, nothing else. (Child spawn has no guard: the
// live argument is destroyed on return.)
void dtRootFailure(size_t before, uint id, string what) nothrow @nogc @system
{
    namedCheck(atomicLoad(dtLog.n) == before + 2, what);
    namedCheck(dtId(before) == id && dtId(before + 1) == 0, "dtor-failed-spawn root failure: owner, then moved-from .init");
    foreach (i; before .. before + 2)
        namedCheck(atomicLoad!(MemoryOrder.raw)(dtLog.events[i].tid) == dtEmitter, "dtor-failed-spawn root failure on the emitter");
}
void dtorFailedSpawn(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    // Root level.
    auto roots = PrivateRootPark!(DtObj, dtFinish!DtObj, reclaim).create(h.farm, 2, 2, policy(h.counts));
    check(roots !is null);
    check(roots.spawn(DtObj(1)) == PrivateSpawnResult.spawned);
    check(roots.spawn(DtObj(2)) == PrivateSpawnResult.spawned);
    auto before = atomicLoad(dtLog.n);
    check(roots.spawn(DtObj(3)) == PrivateSpawnResult.capacityFull);
    dtOnCaller(3, dtEmitter, "dtor-failed-spawn root capacityFull argument destroyed once on the caller");
    dtRootFailure(before, 3, "capacityFull");
    h.drain(roots, "dtor-failed-spawn root drain");
    atomicStore(h.counts.budget, 0L);
    before = atomicLoad(dtLog.n);
    check(roots.spawn(DtObj(4)) == PrivateSpawnResult.allocationFailed);
    atomicStore(h.counts.budget, long.max);
    dtOnCaller(4, dtEmitter, "dtor-failed-spawn root allocationFailed argument destroyed once on the caller");
    dtRootFailure(before, 4, "allocationFailed");
    namedCheck(roots.live == 0, "dtor-failed-spawn failed spawn stores nothing");
    check(roots.spawn(DtObj(5)) == PrivateSpawnResult.spawned);
    h.drain(roots, "dtor-failed-spawn root reuse");
    foreach (id; [1u, 2u, 5u]) dtStoredOnConsumer(id, "dtor-failed-spawn stored exactly once");
    dtOnCaller(3, dtEmitter, "dtor-failed-spawn failed root argument not destroyed again");
    dtOnCaller(4, dtEmitter, "dtor-failed-spawn failed root argument not destroyed again");
    roots.destroy();
    // Member level.
    auto park = PrivateRootPark!(DtCaller, dtCallerStep, reclaim).create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(DtCaller(&h.counts)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-failed-spawn member drain");
    dtStoredOnConsumer(201, "dtor-failed-spawn member stored exactly once");
    foreach (id; [202u, 203u, 204u])
        dtOnCaller(id, dtCaller, "dtor-failed-spawn failed member argument not destroyed again");
    namedCheck(dtCaller != dtEmitter, "dtor-failed-spawn member caller is a consumer");
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-failed-spawn allocation balance");
}

// ---- D3b: over-aligned copyable state through the failure paths
// Selector: dtor-aligned. Both policies, align(32) and align(64). Every
// destructor frees an owning token: the first free flips the token's cell and
// counts as owning; a free of an already-freed cell counts as a bad (double)
// free. A moved-from .init destructor owns nothing and records id 0.
// DMD 2.113's core.lifetime.move runs ~T of a struct with align(32) or
// align(64) during the move while the destination keeps the resource, so the
// failed-root-spawn guard must destroy through object.destroy, never move.
// Catching mutation:
//   D-M8move  in PrivateRootPark.spawn's scope(exit) guard restore
//         `T doomed = move(initial);`: on DMD every failed root spawn below
//         destroys its argument twice ("dtor-aligned root ... argument
//         destroyed once on the caller" count 2) and double-frees the token
//         ("dtor-aligned root no double frees"). LDC's move is correct, so
//         this is a DMD-only mutation; dtor-aligned passes there either way.
struct DtAlignedLog { shared uint[8] tokens; shared size_t owning, bad; }
__gshared DtAlignedLog dtAligned;
void dtAlignedReset() nothrow @nogc @system
{
    foreach (ref token; dtAligned.tokens) atomicStore!(MemoryOrder.raw)(token, 1u);
    atomicStore(dtAligned.owning, cast(size_t) 0);
    atomicStore(dtAligned.bad, cast(size_t) 0);
}
void dtAlignedDtor(uint id, shared uint* token) nothrow @nogc @system
{
    if (token is null) { dtRecord(0); return; }
    if (atomicExchange!(MemoryOrder.acq_rel)(token, 0u) != 1u)
        atomicFetchAdd(dtAligned.bad, cast(size_t) 1);
    else
        atomicFetchAdd(dtAligned.owning, cast(size_t) 1);
    dtRecord(id);
}
void dtAlignedCheckOwning(size_t live, string whatOwning, string whatBad) nothrow @nogc @system
{
    namedCheck(atomicLoad(dtAligned.owning) == live, whatOwning);
    namedCheck(atomicLoad(dtAligned.bad) == 0, whatBad);
}
align(32) struct DtAligned32
{
    uint id;
    shared uint* token;
    ~this() nothrow @nogc @system { dtAlignedDtor(id, token); }
}
align(64) struct DtAligned64
{
    uint id;
    shared uint* token;
    ~this() nothrow @nogc @system { dtAlignedDtor(id, token); }
}
static assert(DtAligned32.alignof == 32 && DtAligned64.alignof == 64);
static assert(stateAllowed!DtAligned32 && stateAllowed!DtAligned64);
static assert(rootSpawnOk!DtAligned32 && childSpawnOk!DtAligned32 && childCreateOk!DtAligned32);
static assert(rootSpawnOk!DtAligned64 && childSpawnOk!DtAligned64 && childCreateOk!DtAligned64);

void dtorAlignedRoot(T, PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    dtAlignedReset();
    auto park = PrivateRootPark!(T, dtFinish!T, reclaim).create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(T(11, &dtAligned.tokens[0])) == PrivateSpawnResult.spawned);
    auto before = atomicLoad(dtLog.n);
    check(park.spawn(T(12, &dtAligned.tokens[1])) == PrivateSpawnResult.capacityFull);
    dtRootFailure(before, 12, "dtor-aligned root capacityFull");
    dtOnCaller(12, dtEmitter, "dtor-aligned root capacityFull argument destroyed once on the caller");
    h.drain(park, "dtor-aligned root capacityFull drain");
    dtStoredOnConsumer(11, "dtor-aligned root stored exactly once");
    atomicStore(h.counts.budget, 0L);
    before = atomicLoad(dtLog.n);
    check(park.spawn(T(13, &dtAligned.tokens[2])) == PrivateSpawnResult.allocationFailed);
    atomicStore(h.counts.budget, long.max);
    dtRootFailure(before, 13, "dtor-aligned root allocationFailed");
    dtOnCaller(13, dtEmitter, "dtor-aligned root allocationFailed argument destroyed once on the caller");
    park.destroy();
    dtAlignedCheckOwning(3,
        "dtor-aligned root owning destructors equal live values",
        "dtor-aligned root no double frees");
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-aligned root allocation balance");
}

struct DtAlignedCaller(T)
{
    Counts* counts;
    PrivateChildPark!T* cohort;
}
void dtAlignedNop(T)(scope ref PrivateBorrow!T, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition dtAlignedStep(T)(scope ref PrivateBorrow!(DtAlignedCaller!T) b,
    scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto s = &b.value();
    dtCaller = portsThreadId();
    s.cohort = ctx.createChildren!T(2);
    namedCheck(s.cohort !is null, "dtor-aligned cohort");
    namedCheck(s.cohort.spawn(ctx, T(21, &dtAligned.tokens[3])) == PrivateSpawnResult.spawned,
        "dtor-aligned member stored");
    atomicStore(s.counts.budget, 0L);
    namedCheck(s.cohort.spawn(ctx, T(22, &dtAligned.tokens[4])) == PrivateSpawnResult.allocationFailed,
        "dtor-aligned child allocationFailed");
    atomicStore(s.counts.budget, long.max);
    dtOnCaller(22, dtCaller, "dtor-aligned child allocationFailed argument destroyed once on the caller");
    namedCheck(s.cohort.spawn(ctx, T(23, &dtAligned.tokens[5])) == PrivateSpawnResult.spawned,
        "dtor-aligned second member stored");
    namedCheck(s.cohort.spawn(ctx, T(24, &dtAligned.tokens[6])) == PrivateSpawnResult.capacityFull,
        "dtor-aligned child capacityFull");
    dtOnCaller(24, dtCaller, "dtor-aligned child capacityFull argument destroyed once on the caller");
    namedCheck(s.cohort.startFinal!(dtAlignedNop!T)(ctx) == PrivateStartResult.started,
        "dtor-aligned final wave");
    return PrivateRootDisposition.finishAfterChildren;
}
void dtorAlignedChild(T, PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    dtAlignedReset();
    auto park = PrivateRootPark!(DtAlignedCaller!T, dtAlignedStep!T, reclaim)
        .create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(DtAlignedCaller!T(&h.counts)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-aligned child drain");
    dtStoredOnConsumer(21, "dtor-aligned member stored exactly once");
    dtStoredOnConsumer(23, "dtor-aligned second member stored exactly once");
    park.destroy();
    dtAlignedCheckOwning(4,
        "dtor-aligned child owning destructors equal live values",
        "dtor-aligned child no double frees");
    namedCheck(dtCaller != dtEmitter, "dtor-aligned child caller is a consumer");
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-aligned child allocation balance");
}

// ---- D4: moved-from accounting and non-copyable owners
// Selector: dtor-moved-from. A successful spawn leaves one moved-from .init
// argument that the caller's language scope destroys (a destructor with
// observable actions runs once per spawn on the caller); the stored instance
// is destroyed exactly once, on a consumer. Failures destroy the live argument
// instead (no .init event). `@disable this(this)` owners are spawned via `move`
// or as rvalues.
// Catching mutations:
//   D-M7root / D-M7child  replace `moveEmplace(initial, *cast(T*) state);` by
//         `memcpy(state, &initial, T.sizeof);` in the root / child spawn: the
//         source keeps its identity, so no .init destructor runs per spawn
//         ("dtor-moved-from root/member .init event") and the id would later
//         be destroyed twice.
struct DtMover { Counts* counts; PrivateChildPark!DtUnique* uniq; PrivateChildPark!DtKid* kids; }
PrivateRootDisposition dtMoverStep(scope ref PrivateBorrow!DtMover b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    dtCaller = portsThreadId();
    s.kids = ctx.createChildren!DtKid(2);
    s.uniq = ctx.createChildren!DtUnique(2);
    namedCheck(s.kids !is null && s.uniq !is null, "dtor-moved-from cohorts");
    auto before = dtCount(0);
    namedCheck(s.kids.spawn(ctx, DtKid(301)) == PrivateSpawnResult.spawned, "dtor-moved-from kid spawn");
    namedCheck(dtCount(0) == before + 1 && dtTidAt(0, before) == dtCaller, "dtor-moved-from member .init event on the caller");
    namedCheck(s.kids.spawn(ctx, DtKid(302)) == PrivateSpawnResult.spawned, "dtor-moved-from kid spawn 2");
    namedCheck(dtCount(0) == before + 2, "dtor-moved-from member per-spawn .init event");
    namedCheck(s.kids.spawn(ctx, DtKid(303)) == PrivateSpawnResult.capacityFull, "dtor-moved-from kid capacityFull");
    namedCheck(dtCount(0) == before + 2, "dtor-moved-from failed spawn leaves no .init event");
    dtOnCaller(303, dtCaller, "dtor-moved-from failed member argument destroyed once on the caller");
    auto owner = DtUnique(311);
    namedCheck(s.uniq.spawn(ctx, move(owner)) == PrivateSpawnResult.spawned, "dtor-moved-from unique move spawn");
    namedCheck(owner.id == 0, "dtor-moved-from unique source is .init");
    namedCheck(s.uniq.spawn(ctx, DtUnique(312)) == PrivateSpawnResult.spawned, "dtor-moved-from unique rvalue spawn");
    namedCheck(dtCount(311) == 0 && dtCount(312) == 0, "dtor-moved-from unique owners are not destroyed by spawn");
    return PrivateRootDisposition.finish; // both cohorts disposed while idle
}
void dtorMovedFrom(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto roots = PrivateRootPark!(DtUnique, dtFinish!DtUnique, reclaim).create(h.farm, 4, 4, policy(h.counts));
    check(roots !is null);
    foreach (i; 0 .. 2)
    {
        auto before = dtCount(0);
        check(roots.spawn(DtUnique(400 + i)) == PrivateSpawnResult.spawned);
        namedCheck(dtCount(0) == before + 1 && dtTidAt(0, before) == dtEmitter, "dtor-moved-from root .init event on the emitter");
    }
    auto owner = DtUnique(402);
    check(roots.spawn(move(owner)) == PrivateSpawnResult.spawned);
    namedCheck(owner.id == 0, "dtor-moved-from root unique source is .init");
    namedCheck(dtCount(0) == 3 && dtCount(402) == 0, "dtor-moved-from root: only .init destroyed on the emitter");
    h.drain(roots, "dtor-moved-from root drain");
    foreach (id; [400u, 401u, 402u]) dtStoredOnConsumer(id, "dtor-moved-from root stored exactly once");
    roots.destroy();
    auto park = PrivateRootPark!(DtMover, dtMoverStep, reclaim).create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(DtMover(&h.counts)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-moved-from member drain");
    foreach (id; [301u, 302u, 311u, 312u]) dtStoredOnConsumer(id, "dtor-moved-from member stored exactly once");
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-moved-from allocation balance");
}

// ---- D5: recursive non-POD members (T holds a pointer to its own cohort)
// Selector: dtor-recursive. The existing DeepMember/RDeep tests keep covering
// the recursive POD case. Chain of six members; mode 0: root `finish` disposes
// the chain; mode 1: `startFinal` on the top cohort retires it. Deepest first.
// Catching mutations: D-M3 (retireMember destroys the parent before its
// cohorts): "dtor-recursive deepest first"; D-M1 (dispose skips ~T).
struct DtRec
{
    uint id;
    PrivateChildPark!DtRec* next;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
void dtRecOp(scope ref PrivateBorrow!DtRec b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto m = &b.value();
    dtRecord(m.id + dtOp);
    if (m.id == 205) return;
    m.next = ctx.createChildren!DtRec(1);
    namedCheck(m.next !is null && m.next.spawn(ctx, DtRec(m.id + 1)) == PrivateSpawnResult.spawned, "dtor-recursive spawn");
    namedCheck(m.next.start!dtRecOp(ctx) == PrivateStartResult.started, "dtor-recursive descend");
}
void dtRecNop(scope ref PrivateBorrow!DtRec, scope ref PrivateContext) nothrow @nogc @system {}
struct DtRecRoot
{
    uint id, phase;
    ubyte mode;
    PrivateChildPark!DtRec* chain;
    ~this() nothrow @nogc @system { dtRecord(id); }
}
PrivateRootDisposition dtRecStep(scope ref PrivateBorrow!DtRecRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    switch (s.phase++)
    {
        case 0:
            s.chain = ctx.createChildren!DtRec(1);
            namedCheck(s.chain !is null && s.chain.spawn(ctx, DtRec(200)) == PrivateSpawnResult.spawned, "dtor-recursive top");
            namedCheck(s.chain.start!dtRecOp(ctx) == PrivateStartResult.started, "dtor-recursive first wave");
            return PrivateRootDisposition.waitChildren;
        case 1:
            if (s.mode == 0) return PrivateRootDisposition.finish;
            namedCheck(s.chain.startFinal!dtRecNop(ctx) == PrivateStartResult.started, "dtor-recursive retire");
            return PrivateRootDisposition.waitChildren;
        default:
            return PrivateRootDisposition.finish;
    }
}
void dtorRecursive(PrivateRootReclamation reclaim)(ubyte mode)
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto park = PrivateRootPark!(DtRecRoot, dtRecStep, reclaim).create(h.farm, 1, 1, policy(h.counts));
    check(park !is null);
    check(park.spawn(DtRecRoot(1, 0, mode)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-recursive completion");
    foreach (id; 200u .. 206u) dtStoredOnConsumer(id, "dtor-recursive stored exactly once");
    dtStoredOnConsumer(1, "dtor-recursive root stored exactly once");
    foreach (id; 200u .. 205u) namedCheck(dtPos(id + 1) < dtPos(id), "dtor-recursive deepest first");
    namedCheck(dtPos(200) < dtPos(1), "dtor-recursive root last");
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-recursive allocation balance");
}

// ---- D6: result slot is written before the destructor runs
// Selector: dtor-results. The allocator records the base of the cohort's
// result array (capacity * R.sizeof, plus one debug write tag/member). A member's
// destructor, running inside its own final dispatch, requires its slot to
// already hold the operation's result.
// Catching mutation:
//   D-M6 runChild computes the result, calls `retireMember!T(entry)`, and only
//        then stores `park.results_[entry - park.entries_] = result`: the
//        member's destructor sees the zeroed slot: "dtor-results slot written
//        before ~T". (Exact edit in the task report.)
enum dtSlotCapacity = 9; // 72 bytes release / 81 debug: unique allocation size
struct DtSlotAlloc { Counts counts; size_t slotBytes; shared size_t hits; shared ulong* base; }
__gshared DtSlotAlloc* dtSlotProbe;
void* dtSlotAllocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto a = cast(DtSlotAlloc*) ctx;
    auto p = allocate(&a.counts, bytes, alignment);
    if (p !is null && bytes == a.slotBytes)
    {
        atomicFetchAdd(a.hits, cast(size_t) 1);
        atomicStore(a.base, cast(shared(ulong)*) p);
    }
    return p;
}
void dtSlotDeallocate(void* ctx, void* ptr, size_t bytes, size_t alignment) nothrow @nogc @system
{ deallocate(&(cast(DtSlotAlloc*) ctx).counts, ptr, bytes, alignment); }
ulong dtSlotValue(uint index) nothrow @nogc @system { return 0xD500 + index; }
struct DtSlot
{
    uint tag; // member index + 1; 0 for .init
    ~this() nothrow @nogc @system
    {
        if (tag == 0) return;
        auto base = cast(ulong*) atomicLoad(dtSlotProbe.base);
        namedCheck(base !is null && base[tag - 1] == dtSlotValue(tag - 1), "dtor-results slot written before ~T");
        dtRecord(tag + 1000);
    }
}
ulong dtSlotOp(scope ref PrivateBorrow!DtSlot b, scope ref PrivateContext) nothrow @nogc @system
{ return dtSlotValue(b.value.tag - 1); }
struct DtSlotRoot { PrivateChildPark!(DtSlot, ulong)* kids; uint phase; }
PrivateRootDisposition dtSlotStep(scope ref PrivateBorrow!DtSlotRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase++ == 0)
    {
        s.kids = ctx.createChildren!(DtSlot, ulong)(dtSlotCapacity);
        namedCheck(s.kids !is null, "dtor-results cohort");
        foreach (i; 1 .. dtSlotCapacity + 1)
            namedCheck(s.kids.spawn(ctx, DtSlot(cast(uint) i)) == PrivateSpawnResult.spawned, "dtor-results spawn");
        namedCheck(s.kids.startFinal!dtSlotOp(ctx) == PrivateStartResult.started, "dtor-results final wave");
        return PrivateRootDisposition.waitChildren;
    }
    auto r = s.kids.results(ctx);
    namedCheck(r.length == dtSlotCapacity, "dtor-results length");
    foreach (i, v; r) namedCheck(v == dtSlotValue(cast(uint) i), "dtor-results values intact after destruction");
    return PrivateRootDisposition.finish;
}
void dtorResults(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    DtSlotAlloc alloc;
    version (assert) alloc.slotBytes = dtSlotCapacity * (ulong.sizeof + 1);
    else alloc.slotBytes = dtSlotCapacity * ulong.sizeof;
    dtSlotProbe = &alloc;
    scope(exit) dtSlotProbe = null;
    auto park = PrivateRootPark!(DtSlotRoot, dtSlotStep, reclaim).create(h.farm, 1, 1,
        ActorAllocator(&alloc, &dtSlotAllocate, &dtSlotDeallocate));
    check(park !is null);
    check(park.spawn(DtSlotRoot()) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-results completion");
    namedCheck(atomicLoad(alloc.hits) == 1, "dtor-results unique slot allocation");
    foreach (i; 1 .. dtSlotCapacity + 1) dtStoredOnConsumer(cast(uint) i + 1000, "dtor-results every member destroyed once");
    park.destroy();
    namedCheck(atomicLoad(alloc.counts.allocated) == atomicLoad(alloc.counts.freed), "dtor-results allocation balance");
}

// ---- D7: GC references held by actor state survive concurrent collection
// Selector: dtor-gc. State holds class references; allocations come from
// ActorAllocator.gcRooted(inner). A separate thread loops GC.collect() the
// whole time. Every destructor first waits for two further collections and
// then requires that no GcObj was finalized, so a reference that is only held
// by allocator-backed state (children's objects are nulled in the parent) must
// still be rooted while ~T runs. Both policies.
// Catching mutation:
//   D-M5  destroyState removes the GC root range before calling ~T:
//         removing the root range first lets the collections
//         during the destructor's wait finalize the (now unrooted) objects:
//         "dtor-gc referenced objects survive until destroyed".
// The documented negative control (no gcRooted adapter) is not part of the suite.
shared uint gcRounds, gcStop;
__gshared uint gcRun; // runs so far; one per policy instantiation
shared ulong gcFinalized; // bit n: the GcObj constructed with idx n was finalized
class GcObj
{
    uint magic = 0x600D, idx;
    this(uint idx) { this.idx = idx; }
    ~this() { atomicOp!"|="(gcFinalized, 1UL << idx); }
}
void gcWaitRounds(uint n) nothrow @nogc @system
{
    immutable target = atomicLoad(gcRounds) + n;
    auto deadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad(gcRounds) < target)
    {
        namedCheck(MonoTime.currTime < deadline, "dtor-gc collector progress");
        pause();
    }
}
// `bit` lives in the state, not the object, so a collected object cannot be consulted.
// The reference is read from its slot only after the wait: a copy held in this
// frame during the wait would root the object by itself.
void gcRequireAlive(const(GcObj)* slot, uint bit) nothrow @nogc @system
{
    gcWaitRounds(2);
    namedCheck((atomicLoad(gcFinalized) & (1UL << bit)) == 0, "dtor-gc referenced objects survive until destroyed");
    auto o = *slot;
    namedCheck(o !is null && o.magic == 0x600D, "dtor-gc referenced object intact");
}
// The pads keep allocator free-list links off the fields read by ~T.
struct GcKid
{
    ulong[2] pad;
    GcObj obj;
    uint id, bit;
    ~this() nothrow @nogc @system { if (id != 0) { gcRequireAlive(&obj, bit); dtRecord(id); } }
}
struct GcRoot
{
    ulong[2] pad;
    GcObj[2] held;
    GcObj spare;
    PrivateChildPark!GcKid* kids;
    uint id, phase, startRound;
    ~this() nothrow @nogc @system { if (id != 0) { gcRequireAlive(&spare, id * 4 + 2); dtRecord(id); } }
}
void gcKidOp(scope ref PrivateBorrow!GcKid b, scope ref PrivateContext) nothrow @nogc @system
{ namedCheck(b.value.obj !is null && b.value.obj.magic == 0x600D, "dtor-gc member op reads its object"); }
PrivateRootDisposition gcStep(scope ref PrivateBorrow!GcRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!GcKid(2);
        namedCheck(s.kids !is null, "dtor-gc cohort");
        foreach (i; 0 .. 2)
        {
            namedCheck(s.kids.spawn(ctx, GcKid([0, 0], s.held[i], 700 + s.id * 10 + i, s.id * 4 + i)) == PrivateSpawnResult.spawned,
                "dtor-gc spawn");
            s.held[i] = null; // only the member state references it now
        }
        namedCheck(s.kids.start!gcKidOp(ctx) == PrivateStartResult.started, "dtor-gc wave");
        s.startRound = atomicLoad(gcRounds);
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    // Keep the roots and members alive across many forced collections.
    if (atomicLoad(gcRounds) < s.startRound + 6) return PrivateRootDisposition.again;
    return PrivateRootDisposition.finish;
}
GcRoot gcMakeRoot(uint id)
{
    // Kept out of the test's frame so no stack slot or register of the emitter references the objects.
    return GcRoot([0, 0], [new GcObj(id * 4), new GcObj(id * 4 + 1)], new GcObj(id * 4 + 2), null, id);
}
void dtorGc(PrivateRootReclamation reclaim)()
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    // Objects of earlier runs may be finalized at any time: every run uses its own
    // root ids, hence its own finalization bits (id * 4 + n < 64).
    immutable first = 1 + 4 * gcRun++;
    atomicStore(gcRounds, 0u); atomicStore(gcStop, 0u);
    auto inner = policy(h.counts);
    auto park = PrivateRootPark!(GcRoot, gcStep, reclaim).create(h.farm, 2, 2, ActorAllocator.gcRooted(&inner));
    check(park !is null);
    auto collector = new Thread({
        while (atomicLoad(gcStop) == 0) { GC.collect(); atomicFetchAdd(gcRounds, 1u); }
    });
    collector.start();
    scope(exit) { atomicStore(gcStop, 1u); collector.join(); }
    foreach (i; first .. first + 2) check(park.spawn(gcMakeRoot(i)) == PrivateSpawnResult.spawned);
    h.drain(park, "dtor-gc completion");
    foreach (i; first .. first + 2)
    {
        dtStoredOnConsumer(i, "dtor-gc root stored exactly once");
        foreach (k; 0 .. 2) dtStoredOnConsumer(700 + i * 10 + k, "dtor-gc member stored exactly once");
    }
    namedCheck(atomicLoad(gcRounds) >= 6, "dtor-gc collections ran while the state lived");
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "dtor-gc allocation balance");
}

// ThreadSanitizer reports druntime's stop-the-world suspend signal handler
// ("signal handler spoils errno") as soon as GC.collect runs concurrently
// with the Farm threads, independent of this library: skip under TSan.
void dtorGcAll()
{
    version (TSan)
        writeln("SKIP dtor-gc: druntime GC suspend signals are not TSan-clean");
    else
    {
        dtorGc!(PrivateRootReclamation.emitter)();
        dtorGc!(PrivateRootReclamation.consumer)();
        writeln("PASS dtor-gc rooted class references survive concurrent forced collection");
    }
}

// ---- compile-time contract (stateAllowed), checked through create/spawn
// stateAllowed is evaluated inside spawn/create (not at struct scope, where a
// recursive T is still incomplete), so each probe invokes those entry points.
// Catching mutations (each applied alone; the class probe flips and the
// static assert below fails to compile, on LDC and DMD):
//   CT-root   delete `static assert(stateAllowed!T, ...)` in PrivateRootPark.spawn
//             (first occurrence): rootSpawnOk!DtClass becomes true;
//   CT-create delete it in PrivateChildPark.create: childCreateResultOk!DtClass;
//   CT-child  delete it in PrivateChildPark.spawn: childSpawnOk!DtClass.
template rootSpawnOk(T)
{
    enum rootSpawnOk = __traits(compiles, (PrivateRootPark!(T, dtFinish!T)* p) { p.spawn(T.init); });
}
template childSpawnOk(T)
{
    enum childSpawnOk = __traits(compiles, (PrivateChildPark!T* p, ref PrivateContext c) { p.spawn(c, T.init); });
}
template childCreateOk(T)
{
    enum childCreateOk = __traits(compiles, (ref PrivateContext c) { c.createChildren!T(1); });
}
template childCreateResultOk(T)
{
    enum childCreateResultOk = __traits(compiles, (ref PrivateContext c) { c.createChildren!(T, ulong)(1); });
}
template allPathsOk(T)
{
    enum allPathsOk = rootSpawnOk!T && childSpawnOk!T && childCreateOk!T && childCreateResultOk!T;
}
template anyPathOk(T)
{
    enum anyPathOk = rootSpawnOk!T || childSpawnOk!T || childCreateOk!T || childCreateResultOk!T;
}
class DtClass { int x; }
interface DtInterface {}
struct DtThrows { ~this() { throw new Exception("x"); } }
struct DtGcAlloc { int[] a; ~this() { a = new int[1]; } }
struct DtNoDtor { int a; }
struct DtPostblit { int a; this(this) {} }
struct DtNoCopyDtor { @disable this(this); int a; ~this() nothrow @nogc @system {} }
struct DtArrayHolder { DtObj[2] pair; }
static assert(allPathsOk!int && allPathsOk!DtNoDtor && allPathsOk!DtPostblit);
static assert(allPathsOk!PNotPod);           // obsolete since E2-1: postblit state is allowed
static assert(allPathsOk!DtObj && allPathsOk!DtNoCopyDtor && allPathsOk!DtArrayHolder);
static assert(allPathsOk!(DtObj[2]));
// Pre-E2-1 accepted every POD, including a struct with a disabled default
// constructor; stateAllowed must not reject it (mutation: probe `T a;`).
struct DtNoDefault { int x; @disable this(); this(int v) nothrow @nogc { x = v; } }
static assert(stateAllowed!DtNoDefault && allPathsOk!DtNoDefault);
struct DtNoDefaultDtor { uint id; @disable this(); this(uint v) nothrow @nogc { id = v; } ~this() nothrow @nogc @system {} }
static assert(stateAllowed!DtNoDefaultDtor && allPathsOk!DtNoDefaultDtor);
static assert(!rootSpawnOk!DtClass);          // CT-root
static assert(!childSpawnOk!DtClass);         // CT-child
static assert(!childCreateResultOk!DtClass);  // CT-create
static assert(!childCreateOk!DtClass);
static assert(!rootSpawnOk!DtInterface && !childSpawnOk!DtInterface && !childCreateOk!DtInterface);
static assert(!anyPathOk!DtThrows && !anyPathOk!DtGcAlloc);  // destructor must be nothrow @nogc
static assert(!stateAllowed!DtClass && !stateAllowed!DtInterface && !stateAllowed!DtThrows && !stateAllowed!DtGcAlloc);
static assert(stateAllowed!int && stateAllowed!PNotPod && stateAllowed!DtObj && stateAllowed!(DtObj[2]));
// Regression: const/immutable POD state compiled on main and must stay
// accepted; the moveEmplace clause applies only to the moving (non-POD) spawn
// path (mutation: drop `__traits(isPOD, T) ||` from that clause — these
// asserts then fail to compile).
static assert(stateAllowed!(const(State)) && stateAllowed!(immutable(State)));
static assert(rootSpawnOk!(const(State)) && rootSpawnOk!(immutable(State)));
static assert(childSpawnOk!(const(State)) && childCreateOk!(const(State))
    && childCreateResultOk!(const(State)));
static assert(childSpawnOk!(immutable(State)) && childCreateOk!(immutable(State))
    && childCreateResultOk!(immutable(State)));
// Recursive states (a member pointing at their own cohort) are not nested.
static assert(stateAllowed!DeepMember && stateAllowed!RDeep && stateAllowed!DtRec);
void dtNestedProbe()
{
    int local;
    struct DtNested { int get() { return local; } }
    static assert(__traits(isNested, DtNested) && !stateAllowed!DtNested);
    static assert(!childSpawnOk!DtNested && !childCreateOk!DtNested && !childCreateResultOk!DtNested);
    // Containment: hasNested also rejects members carrying a context pointer,
    // including through static arrays, which the old top-level
    // `is(T == struct) && __traits(isNested, T)` clause let through
    // (mutation: revert stateAllowed to that clause — these asserts fail).
    struct DtNestedMember { DtNested inner; }
    struct DtNestedMemberArray { DtNested[2] inner; }
    static assert(!__traits(isNested, DtNestedMember) && hasNested!DtNestedMember);
    static assert(!stateAllowed!DtNestedMember && !anyPathOk!DtNestedMember);
    static assert(!stateAllowed!DtNestedMemberArray && !anyPathOk!DtNestedMemberArray);
    static assert(!stateAllowed!(DtNested[2]) && !anyPathOk!(DtNested[2]));
}
// PrivateChildPark/PrivateRootPark remain usable at a bare instantiation of
// every allowed type; PNotPod child instantiation is checked above, and R stays POD.
static assert(__traits(compiles, PrivateRootPark!(DtObj, dtFinish!DtObj)));

// ---- death tests (debug and -release, LDC and DMD through deathHarness)
// Selectors dtor-destroy-live, dtor-finish-active-child. The state's
// destructor prints a marker: neither fatal may run it.
// Catching mutations:
//   D-M9  PrivateRootPark.destroy destroys every live root's state before its
//         live check (implicit destruction): "dtor ran" appears on stderr.
//   D-M10 runRoot's finish branch calls `destroyState!T(entry);` before
//         `disposeChildren(entry);` (a destructor with an active child wave):
//         "dtor ran" appears before the fatal.
struct DtNoisy
{
    uint id;
    ~this() nothrow @nogc @system { if (id != 0) fprintf(stderr, "dtor ran %u\n", id); }
}
void dtorDestroyLiveDeath()
{
    auto farm = AntFarm.create();
    auto park = PrivateRootPark!(DtNoisy, dtFinish!DtNoisy).create(farm, 1);
    check(park !is null);
    check(park.spawn(DtNoisy(9)) == PrivateSpawnResult.spawned);
    park.destroy();
}
PrivateRootDisposition dtActiveStep(scope ref PrivateBorrow!DtNoisy, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto kids = ctx.createChildren!DtNoisy(1);
    check(kids !is null && kids.spawn(ctx, DtNoisy(8)) == PrivateSpawnResult.spawned);
    check(kids.start!dtNoisyOp(ctx) == PrivateStartResult.started);
    return PrivateRootDisposition.finish; // active child wave: fatal
}
void dtNoisyOp(scope ref PrivateBorrow!DtNoisy, scope ref PrivateContext) nothrow @nogc @system {}
void dtorActiveChildDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(DtNoisy, dtActiveStep).create(farm, 1);
    check(park !is null);
    check(park.spawn(DtNoisy(7)) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
// Debug only (the reentrancy flag is an assert-mode field). An owning
// destructor re-entering the same root park from a failed spawn dies: the
// guard destroys it while the park is still entered. A moved-from .init
// destructor runs after entered_ clears and is not a supported reentry point,
// so this destructor re-enters only when it owns an id.
// Catching mutation: D-M8guard (remove the failed-spawn guard): the owning
// destructor then runs after entered_ clears, the reentry survives and the
// child prints "reentry survived" and exits zero.
struct DtReenter
{
    uint id;
    ~this() nothrow @nogc @system { if (id == 2) dtReenterNow(); }
}
alias DtReenterPark = PrivateRootPark!(DtReenter, dtFinish!DtReenter);
__gshared DtReenterPark* dtReenterPark;
void dtReenterNow() nothrow @nogc @system
{
    fprintf(stderr, "dtor-reenter: owning destructor re-enters spawn\n");
    dtReenterPark.spawn(DtReenter(0)); // fatal in debug: the park is still entered
    fprintf(stderr, "dtor-reenter: reentry survived\n");
}
void dtorReenterDeath()
{
    auto farm = AntFarm.create();
    dtReenterPark = DtReenterPark.create(farm, 1);
    check(dtReenterPark !is null);
    check(dtReenterPark.spawn(DtReenter(1)) == PrivateSpawnResult.spawned);
    check(dtReenterPark.spawn(DtReenter(2)) == PrivateSpawnResult.capacityFull); // dies in debug
}
void dtorDeath()
{
    version (linux)
    {
        auto live = probeDeath("dtor-destroy-live");
        check(live.status != 0);
        check(live.stderrText.canFind("antfarm fatal: destroy nonempty private root park"));
        check(!live.stderrText.canFind("dtor ran"));
        auto active = probeDeath("dtor-finish-active-child");
        check(active.status != 0);
        check(active.stderrText.canFind("antfarm fatal: retire private actor with active child wave"));
        check(!active.stderrText.canFind("dtor ran 7") && !active.stderrText.canFind("dtor ran 8"));
        version (assert)
        {
            auto reenter = probeDeath("dtor-reenter-failed-spawn");
            check(reenter.status != 0);
            check(reenter.stderrText.canFind("dtor-reenter: owning destructor re-enters spawn"));
            check(reenter.stderrText.canFind("antfarm fatal: private root park: reentrant entry"));
            check(!reenter.stderrText.canFind("reentry survived"));
        }
        writeln("PASS dtor death modes: destroy-live and active-child retirement run no destructor; debug failed-spawn reentry is fatal");
    }
    else
        writeln("SKIP dtor death modes: /proc/self/exe re-exec requires Linux");
}

void phaseDtor()
{
    dtorRoot!(PrivateRootReclamation.emitter)();
    dtorRoot!(PrivateRootReclamation.consumer)();
    writeln("PASS dtor-root exactly-once consumer destruction: finish, finishAfterChildren, never-dispatched members");
    dtorChild!(PrivateRootReclamation.emitter)();
    dtorChild!(PrivateRootReclamation.consumer)();
    writeln("PASS dtor-child startFinal plain/result members destroyed on consumers after op and result");
    foreach (mode; 0 .. 3)
    {
        dtorOrder!(PrivateRootReclamation.emitter)(cast(ubyte) mode);
        dtorOrder!(PrivateRootReclamation.consumer)(cast(ubyte) mode);
    }
    writeln("PASS dtor-order depth-3 postorder across disposal, startFinal and finishAfterChildren");
    dtorFailedSpawn!(PrivateRootReclamation.emitter)();
    dtorFailedSpawn!(PrivateRootReclamation.consumer)();
    writeln("PASS dtor-failed-spawn caller-side destruction for capacityFull/waveActive/allocationFailed");
    dtorAlignedRoot!(DtAligned32, PrivateRootReclamation.emitter)();
    dtorAlignedRoot!(DtAligned32, PrivateRootReclamation.consumer)();
    dtorAlignedRoot!(DtAligned64, PrivateRootReclamation.emitter)();
    dtorAlignedRoot!(DtAligned64, PrivateRootReclamation.consumer)();
    dtorAlignedChild!(DtAligned32, PrivateRootReclamation.emitter)();
    dtorAlignedChild!(DtAligned32, PrivateRootReclamation.consumer)();
    dtorAlignedChild!(DtAligned64, PrivateRootReclamation.emitter)();
    dtorAlignedChild!(DtAligned64, PrivateRootReclamation.consumer)();
    writeln("PASS dtor-aligned align(32)/align(64) owning tokens: root and child capacityFull/allocationFailed exactly-once");
    dtorMovedFrom!(PrivateRootReclamation.emitter)();
    dtorMovedFrom!(PrivateRootReclamation.consumer)();
    writeln("PASS dtor-moved-from per-spawn .init accounting and non-copyable owners via move");
    foreach (mode; 0 .. 2)
    {
        dtorRecursive!(PrivateRootReclamation.emitter)(cast(ubyte) mode);
        dtorRecursive!(PrivateRootReclamation.consumer)(cast(ubyte) mode);
    }
    writeln("PASS dtor-recursive non-POD chain destroyed deepest first");
    dtorResults!(PrivateRootReclamation.emitter)();
    dtorResults!(PrivateRootReclamation.consumer)();
    writeln("PASS dtor-results slot written before ~T");
    dtorGcAll();
}

// ===========================================================================
// E2-2: subset waves (design 86 section 4, tests 6.2 S1-S5, SD1-SD4).
// startSubset starts only the listed spawn-order indices. Results come back in
// participant (list) order, subsetMembers reports the list, and an empty list
// is `empty` with no credit and no state change. Tags are (wave << 32) | index
// as in the ports tests; the expected wave of a member is the number of times
// the test's model says it has run. Every deterministic test names the
// mutations it catches (Q-M*); each was applied by hand to
// actors/private_actor.d and the outcome recorded in the evidence file.
// ===========================================================================

struct SbMember { ulong* ran; uint index, wave; }
ulong sbTag(scope ref PrivateBorrow!SbMember m, scope ref PrivateContext) nothrow @nogc @system
{
    ++m.value.ran[m.value.index]; // external counter: runs are visible per member
    return portsValue(++m.value.wave, m.value.index);
}
void sbTouch(scope ref PrivateBorrow!SbMember m, scope ref PrivateContext) nothrow @nogc @system
{
    ++m.value.ran[m.value.index];
    ++m.value.wave;
}
alias SbCohort = PrivateChildPark!(SbMember, ulong);

// Results and members of the last nonempty subset wave, against the model:
// results[j] belongs to members[j] and carries that member's run count.
void sbCheckSubset(SbCohort* c, scope ref PrivateContext ctx, const(ulong)[] ran, const(uint)[] model,
    const(uint)[] members, string what) nothrow @nogc @system
{
    auto r = c.results(ctx);
    namedCheck(c.subsetMembers(ctx) == members, what);
    namedCheck(r.length == members.length, what);
    foreach (j, i; members) namedCheck(r[j] == portsValue(model[i], i), what);
    foreach (i; 0 .. ran.length) namedCheck(ran[i] == model[i], what);
}
// Results of the last whole wave: spawn order, no members.
void sbCheckWhole(SbCohort* c, scope ref PrivateContext ctx, const(ulong)[] ran, const(uint)[] model,
    string what) nothrow @nogc @system
{
    auto r = c.results(ctx);
    namedCheck(c.subsetMembers(ctx).length == 0, what);
    namedCheck(r.length == ran.length, what);
    foreach (i; 0 .. ran.length)
    {
        namedCheck(r[i] == portsValue(model[i], i), what);
        namedCheck(ran[i] == model[i], what);
    }
}

// ---- Q1: subset-basic -----------------------------------------------------------------------
// Selector: subset-basic. One cohort walks a plan of sparse, dense, full
// (reversed), singleton, overlapping-reversed, void-op and whole waves, each
// resumed through waitChildren. After every wave the root checks the results
// order, subsetMembers and every member's run count against a model, then
// probes an empty subset (must be `empty`, take no credit and leave results
// and members intact). The list is passed through a buffer that is poisoned
// right after the call. The tail then retires the cohort with startFinal,
// refills it and runs a subset and a whole wave on the refilled members.
// A void cohort walks subset, whole, subset. Both reclamation policies.
// Catching mutations:
//   Q-M1  subset runChild writes `results_[entry - entries_]` (slot by member
//         index): fails "subset basic sparse" (tag in the wrong slot).
//   Q-M2  startSubset stores `resultCount_` before the empty-list return: the
//         empty probe clears the results: fails "subset basic whole results
//         length" (the check inside the probe).
//   Q-M3  start does not reset `isSubset_`: fails "subset void whole resets
//         members" (stale subsetMembers after a whole wave).
//   Q-M3b start does not reset `wave.emit` (a whole wave is emitted as a
//         subset over the stale selection): fails "subset void whole ran
//         everyone".
//   Q-M4  empty list takes the credit (sets `active`) before returning: fails
//         "subset basic fresh empty takes no credit" (finished is false).
//   Q-M5  the copy `sel_[j] = i` is skipped (the list is not stored): the
//         zeroed selection reports "private child subset: duplicate member".
//   Q-M6  startSubset omits `retire_ = false`: after startFinal and refill the
//         subset wave retires its members: fails "subset basic refill whole".
//   Q-M7  duplicate test trusts a stale inverse position (`p < j` without the
//         `sel_[p] == i` confirmation): a false "duplicate member" fatal on the
//         first overlapping list.
enum sbN = 10;
enum SbKind : ubyte { whole, subset, voidSubset }
struct SbStep { SbKind kind; immutable(uint)[] members; }
immutable SbStep[] sbPlan = [
    SbStep(SbKind.whole),
    SbStep(SbKind.subset, [7, 2, 9, 0]),
    SbStep(SbKind.voidSubset, [1, 3]),
    SbStep(SbKind.subset, [9, 8, 7, 6, 5, 4, 3, 2, 1]),
    SbStep(SbKind.subset, [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SbStep(SbKind.subset, [5, 4, 3]),
    SbStep(SbKind.subset, [3, 4, 5]),
    SbStep(SbKind.subset, [4]),
    SbStep(SbKind.whole),
    SbStep(SbKind.subset, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    SbStep(SbKind.whole),
];
struct SbControl
{
    ulong[sbN] ran;
    uint[sbN] model;
    ulong[3] refillRan;
    ulong[6] voidRan;
    shared ulong done;
}
struct SbRoot
{
    SbCohort* c;
    SbControl* k;
    uint next, stage;
    SbKind lastKind;
    uint lastLen;
    uint[sbN] last;
    uint[sbN] scratch;
}
void sbStart(ref SbRoot s, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto st = sbPlan[s.next++];
    auto k = s.k;
    s.lastKind = st.kind;
    if (st.kind == SbKind.whole)
    {
        namedCheck(s.c.start!sbTag(ctx) == PrivateStartResult.started, "subset basic whole start");
        s.lastLen = sbN;
        foreach (i; 0 .. sbN) { ++k.model[i]; s.last[i] = cast(uint) i; }
        return;
    }
    immutable n = st.members.length;
    foreach (j; 0 .. n) s.scratch[j] = st.members[j];
    immutable r = st.kind == SbKind.subset
        ? s.c.startSubset!sbTag(ctx, s.scratch[0 .. n])
        : s.c.startSubset!sbTouch(ctx, s.scratch[0 .. n]);
    s.scratch[] = uint.max; // the runtime must have copied the list
    namedCheck(r == PrivateStartResult.started, "subset basic subset start");
    s.lastLen = cast(uint) n;
    foreach (j; 0 .. n) { s.last[j] = st.members[j]; ++k.model[st.members[j]]; }
}
void sbVerify(ref SbRoot s, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto k = s.k;
    auto r = s.c.results(ctx);
    if (s.lastKind == SbKind.whole)
    {
        namedCheck(s.c.subsetMembers(ctx).length == 0, "subset basic whole after subset");
        namedCheck(r.length == sbN, "subset basic whole results length");
        foreach (i; 0 .. sbN) namedCheck(r[i] == portsValue(k.model[i], i), "subset basic whole tag");
    }
    else
    {
        namedCheck(s.c.subsetMembers(ctx) == s.last[0 .. s.lastLen], "subset basic members");
        if (s.lastKind == SbKind.voidSubset)
            namedCheck(r.length == 0, "subset basic void subset clears results");
        else
        {
            namedCheck(r.length == s.lastLen, "subset basic sparse results length");
            foreach (j; 0 .. s.lastLen)
                namedCheck(r[j] == portsValue(k.model[s.last[j]], s.last[j]), "subset basic sparse");
        }
    }
    foreach (i; 0 .. sbN) namedCheck(k.ran[i] == k.model[i], "subset basic exactly the selected members ran");
}
void sbEmptyProbe(ref SbRoot s, scope ref PrivateContext ctx) nothrow @nogc @system
{
    namedCheck(s.c.startSubset!sbTag(ctx, null) == PrivateStartResult.empty, "subset basic empty list");
    namedCheck(s.c.finished(ctx), "subset basic empty takes no credit");
    sbVerify(s, ctx); // results and members unchanged
    foreach (i; 0 .. sbN) namedCheck(s.k.ran[i] == s.k.model[i], "subset basic empty keeps state");
}
PrivateRootDisposition sbStep(scope ref PrivateBorrow!SbRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto k = s.k;
    if (s.c is null)
    {
        s.c = ctx.createChildren!(SbMember, ulong)(sbN, PrivateCohortOptions(3, true));
        namedCheck(s.c !is null, "subset basic create");
        namedCheck(s.c.subsetMembers(ctx).length == 0 && s.c.results(ctx).length == 0, "subset basic fresh cohort");
        namedCheck(s.c.startSubset!sbTag(ctx, null) == PrivateStartResult.empty, "subset basic empty on a fresh cohort");
        namedCheck(s.c.finished(ctx), "subset basic fresh empty takes no credit");
        foreach (i; 0 .. sbN)
            namedCheck(s.c.spawn(ctx, SbMember(k.ran.ptr, cast(uint) i)) == PrivateSpawnResult.spawned, "subset basic spawn");
        namedCheck(s.c.startSubset!sbTag(ctx, null) == PrivateStartResult.empty, "subset basic empty with idle members");
        namedCheck(s.c.subsetMembers(ctx).length == 0 && s.c.results(ctx).length == 0, "subset basic empty before any wave");
        sbStart(*s, ctx);
        return PrivateRootDisposition.waitChildren;
    }
    switch (s.stage)
    {
        case 0:
            sbVerify(*s, ctx);
            sbEmptyProbe(*s, ctx);
            if (s.next < sbPlan.length)
            {
                sbStart(*s, ctx);
                return PrivateRootDisposition.waitChildren;
            }
            namedCheck(s.c.startFinal!sbTag(ctx) == PrivateStartResult.started, "subset basic final start");
            s.lastKind = SbKind.whole;
            s.lastLen = sbN;
            foreach (i; 0 .. sbN) ++k.model[i];
            s.stage = 1;
            return PrivateRootDisposition.waitChildren;
        case 1:
        {
            sbVerify(*s, ctx); // results survive retirement; a whole wave has no members
            namedCheck(s.c.startSubset!sbTag(ctx, null) == PrivateStartResult.empty, "subset basic empty after final");
            foreach (i; 0 .. 3)
                namedCheck(s.c.spawn(ctx, SbMember(k.refillRan.ptr, cast(uint) i)) == PrivateSpawnResult.spawned, "subset basic refill spawn");
            s.scratch[0] = 2; s.scratch[1] = 0;
            namedCheck(s.c.startSubset!sbTag(ctx, s.scratch[0 .. 2]) == PrivateStartResult.started, "subset basic refill subset");
            s.stage = 2;
            return PrivateRootDisposition.waitChildren;
        }
        case 2:
        {
            auto r = s.c.results(ctx);
            namedCheck(r.length == 2 && r[0] == portsValue(1, 2) && r[1] == portsValue(1, 0), "subset basic refill results");
            namedCheck(s.c.subsetMembers(ctx) == s.scratch[0 .. 2], "subset basic refill members");
            namedCheck(k.refillRan[0] == 1 && k.refillRan[1] == 0 && k.refillRan[2] == 1, "subset basic refill ran");
            namedCheck(s.c.start!sbTag(ctx) == PrivateStartResult.started, "subset basic refill whole");
            s.stage = 3;
            return PrivateRootDisposition.waitChildren;
        }
        case 3:
        {
            auto r = s.c.results(ctx);
            namedCheck(s.c.subsetMembers(ctx).length == 0, "subset basic refill whole members");
            namedCheck(r.length == 3 && r[0] == portsValue(2, 0) && r[1] == portsValue(1, 1)
                && r[2] == portsValue(2, 2), "subset basic refill whole results");
            atomicFetchAdd(k.done, 1UL);
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "subset basic stages");
            return PrivateRootDisposition.finish;
    }
}

alias SbVoidCohort = PrivateChildPark!SbMember;
struct SvRoot { SbVoidCohort* c; SbControl* k; uint stage; uint[3] scratch; }
PrivateRootDisposition svStep(scope ref PrivateBorrow!SvRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto k = s.k;
    switch (s.stage++)
    {
        case 0:
            s.c = ctx.createChildren!SbMember(6, PrivateCohortOptions(0, true));
            namedCheck(s.c !is null, "subset void create");
            foreach (i; 0 .. 6)
                namedCheck(s.c.spawn(ctx, SbMember(k.voidRan.ptr, cast(uint) i)) == PrivateSpawnResult.spawned, "subset void spawn");
            s.scratch = [5, 0, 3];
            namedCheck(s.c.startSubset!sbTouch(ctx, s.scratch[]) == PrivateStartResult.started, "subset void start");
            return PrivateRootDisposition.waitChildren;
        case 1:
            namedCheck(s.c.subsetMembers(ctx) == s.scratch[], "subset void members");
            foreach (i; 0 .. 6)
                namedCheck(k.voidRan[i] == (i == 5 || i == 0 || i == 3 ? 1 : 0), "subset void exactly the selected members ran");
            namedCheck(s.c.startSubset!sbTouch(ctx, null) == PrivateStartResult.empty, "subset void empty");
            namedCheck(s.c.subsetMembers(ctx) == s.scratch[], "subset void empty keeps members");
            namedCheck(s.c.start!sbTouch(ctx) == PrivateStartResult.started, "subset void whole");
            return PrivateRootDisposition.waitChildren;
        case 2:
        {
            namedCheck(s.c.subsetMembers(ctx).length == 0, "subset void whole resets members");
            foreach (i; 0 .. 6)
                namedCheck(k.voidRan[i] == (i == 5 || i == 0 || i == 3 ? 2 : 1), "subset void whole ran everyone");
            uint[1] one = [1];
            namedCheck(s.c.startSubset!sbTouch(ctx, one[]) == PrivateStartResult.started, "subset void singleton");
            return PrivateRootDisposition.waitChildren;
        }
        case 3:
        {
            uint[1] one = [1];
            namedCheck(s.c.subsetMembers(ctx) == one[], "subset void singleton members");
            foreach (i; 0 .. 6)
                namedCheck(k.voidRan[i] == (i == 5 || i == 0 || i == 3 ? 2 : 1) + (i == 1 ? 1 : 0), "subset void singleton ran");
            atomicFetchAdd(k.done, 1UL);
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "subset void stages");
            return PrivateRootDisposition.finish;
    }
}
void subsetBasic(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 4, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto workers = new Workers(farm, 3);
    scope(exit) workers.join();
    SbControl control;
    auto park = PrivateRootPark!(SbRoot, sbStep, reclaim).create(farm, 1, 2, policy(counts));
    auto voidPark = PrivateRootPark!(SvRoot, svStep, reclaim).create(farm, 1, 2, policy(counts));
    check(park !is null && voidPark !is null);
    check(park.spawn(SbRoot(null, &control)) == PrivateSpawnResult.spawned);
    check(voidPark.spawn(SvRoot(null, &control)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live || voidPark.live)
    {
        park.pump(token);
        voidPark.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "subset basic timeout");
    }
    namedCheck(atomicLoad(control.done) == 2, "subset basic both roots completed");
    park.destroy();
    voidPark.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "subset basic allocation balance");
}

// ---- Q2: subset-active ------------------------------------------------------------------------
// Selector: subset-active. startSubset on a cohort with an active wave returns
// waveActive and has no effect at all: not even the selection, inverse
// positions, count, flag, header or emit function may be touched. The check
// order matters: a bad list (duplicate, out of range, empty) must still just
// return waveActive. Single thread: with no worker consumer and one emitter
// the wave cannot be emitted before the callback returns, so it is active for
// every probe (as in ports-read-active). The first (subset) wave is started and, in the same
// callback, probed with an overlapping reordered longer list; the second
// (whole) wave is probed with another subset. After each join the wave that
// actually ran must be exactly the one that was started.
// Catching mutations:
//   Q-M8a the `waveActive` return moved below the validation/copy loop: the
//         duplicate list [2, 2] is fatal while the wave is active.
//   Q-M8b `isSubset_ = true` stored before the active check: fails "subset
//         active whole keeps no members" (stale flag on the whole wave).
//   Q-M8c `wave.emit = &emitSubset` stored before the active check: the whole
//         wave is emitted as a subset: fails "subset active whole wave results".
//   Q-M8d `k_` stored before the active check: fails "subset active members
//         unchanged".
struct SxControl { ulong[6] ran; uint[6] model; }
struct SxRoot { SbCohort* c; SxControl* k; uint stage; }
immutable uint[2] sxA = [3, 1];
immutable uint[3] sxB = [1, 3, 0];
immutable uint[2] sxDup = [2, 2];
immutable uint[1] sxBad = [9];
immutable uint[1] sxOne = [4];
PrivateRootDisposition sxStep(scope ref PrivateBorrow!SxRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto k = s.k;
    switch (s.stage++)
    {
        case 0:
            s.c = ctx.createChildren!(SbMember, ulong)(6, PrivateCohortOptions(2, true));
            namedCheck(s.c !is null, "subset active create");
            foreach (i; 0 .. 6)
                namedCheck(s.c.spawn(ctx, SbMember(k.ran.ptr, cast(uint) i)) == PrivateSpawnResult.spawned, "subset active spawn");
            namedCheck(s.c.startSubset!sbTag(ctx, sxA[]) == PrivateStartResult.started, "subset active first start");
            foreach (i; sxA) ++k.model[i];
            // The wave is enqueued and cannot be emitted before this callback returns.
            namedCheck(s.c.startSubset!sbTag(ctx, sxB[]) == PrivateStartResult.waveActive, "subset active overlapping list");
            namedCheck(s.c.startSubset!sbTag(ctx, sxDup[]) == PrivateStartResult.waveActive, "subset active duplicate list is not fatal");
            namedCheck(s.c.startSubset!sbTag(ctx, sxBad[]) == PrivateStartResult.waveActive, "subset active bad index is not fatal");
            namedCheck(s.c.startSubset!sbTag(ctx, null) == PrivateStartResult.waveActive, "subset active empty list");
            namedCheck(s.c.start!sbTag(ctx) == PrivateStartResult.waveActive, "subset active whole start");
            namedCheck(s.c.startFinal!sbTag(ctx) == PrivateStartResult.waveActive, "subset active final start");
            namedCheck(s.c.subsetMembers(ctx) == sxA[], "subset active members unchanged");
            return PrivateRootDisposition.waitChildren;
        case 1:
            sbCheckSubset(s.c, ctx, k.ran[], k.model[], sxA[], "subset active first wave");
            namedCheck(s.c.start!sbTag(ctx) == PrivateStartResult.started, "subset active whole after subset");
            foreach (i; 0 .. 6) ++k.model[i];
            namedCheck(s.c.startSubset!sbTag(ctx, sxOne[]) == PrivateStartResult.waveActive, "subset active over whole wave");
            namedCheck(s.c.subsetMembers(ctx).length == 0, "subset active whole keeps no members");
            return PrivateRootDisposition.waitChildren;
        case 2:
            sbCheckWhole(s.c, ctx, k.ran[], k.model[], "subset active whole wave results");
            return PrivateRootDisposition.finish;
        default:
            namedCheck(false, "subset active stages");
            return PrivateRootDisposition.finish;
    }
}
void subsetActive(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    SxControl control;
    auto park = PrivateRootPark!(SxRoot, sxStep, reclaim).create(farm, 1, 2, policy(counts));
    check(park !is null);
    check(park.spawn(SxRoot(null, &control)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "subset active timeout");
    }
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "subset active allocation balance");
}

// ---- Q3: subset-large -------------------------------------------------------------------------
// Selector: subset-large. 1025 members, child batch limits 1, 64 and 256, and
// the pre-filled Farm scaffolding of ports-large-cohort: the first wave is a
// dense shuffled subset of 1024 members; the zero-admission emit must release
// its table credit and run no member; after the quota refresh a strict prefix
// is admitted and EXACTLY the first `prefix` listed members have run. Then:
// a singleton, a sparse 64-member list, a full reversed list, a whole wave
// and the sparse list again. Every wave is checked for results order,
// subsetMembers and exactly-once dispatch of the selected members only.
// Catching mutations:
//   Q-M11 emitSubset slices `entries_` instead of `sel_` (cursor over spawn
//         order): fails "subset large partial prefix is the listed prefix"
//         (unselected member ran) and, in subset-basic, "subset void exactly
//         the selected members ran".
//   Q-M12 emitSubset advances `cursor_ += n` instead of `written` (as P-M9):
//         fails the same prefix check or "subset large exactly once".
//   Q-M13 delete `if (written == 0) releaseWave(wave);` in emitSubset: fails
//         "subset large zero admission credit released" (credit count).
//   Q-M14 runChild's subset slot uses `park.cursor_` instead of the inverse
//         position: fails "subset large sparse".
struct SlWave { uint[] members; bool whole; }
struct SlControl
{
    size_t count, limit;
    ulong* ran;
    uint* model;
    SlWave[] waves;
    uint completed, busy;
}
struct SlRoot { SbCohort* c; SlControl* ctl; uint started; }
void slStart(SlControl* k, SbCohort* c, scope ref PrivateContext ctx, size_t w) nothrow @nogc @system
{
    auto wave = k.waves[w];
    if (wave.whole)
    {
        namedCheck(c.start!sbTag(ctx) == PrivateStartResult.started, "subset large whole start");
        foreach (i; 0 .. k.count) ++k.model[i];
    }
    else
    {
        namedCheck(c.startSubset!sbTag(ctx, wave.members) == PrivateStartResult.started, "subset large subset start");
        foreach (i; wave.members) ++k.model[i];
    }
}
PrivateRootDisposition slStep(scope ref PrivateBorrow!SlRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto k = s.ctl;
    if (s.c is null)
    {
        s.c = ctx.createChildren!(SbMember, ulong)(k.count, PrivateCohortOptions(k.limit, true));
        namedCheck(s.c !is null, "subset large create");
        foreach (i; 0 .. k.count)
            namedCheck(s.c.spawn(ctx, SbMember(k.ran, cast(uint) i)) == PrivateSpawnResult.spawned, "subset large spawn");
        slStart(k, s.c, ctx, 0);
        s.started = 1;
        return PrivateRootDisposition.again;
    }
    if (!s.c.finished(ctx))
    {
        ++k.busy;
        return PrivateRootDisposition.again;
    }
    auto wave = k.waves[s.started - 1];
    auto ran = k.ran[0 .. k.count];
    auto model = k.model[0 .. k.count];
    if (wave.whole) sbCheckWhole(s.c, ctx, ran, model, "subset large whole");
    else sbCheckSubset(s.c, ctx, ran, model, wave.members, "subset large sparse");
    k.completed = s.started;
    if (s.started == k.waves.length) return PrivateRootDisposition.finish;
    slStart(k, s.c, ctx, s.started);
    ++s.started;
    return PrivateRootDisposition.again;
}
void subsetLargeCohort(size_t count, size_t limit)
{
    Counts counts;
    // Compact ST geometry (SPEC 2f): a 256-member one-word ST table costs
    // 38 + 3*256 = 806 words, so a 713-word quota admits a strict 225-member
    // prefix. Other limits fit whole and are checked only against the limit.
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 713);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto ran = new ulong[count];
    auto model = new uint[count];
    // Dense shuffled: a multiplicative permutation (389 is coprime to 1025), one member left out.
    uint[] dense, sparse, full, one = [cast(uint) (count - 1)];
    foreach (j; 0 .. count - 1) dense ~= cast(uint) ((j * 389 + 17) % count);
    foreach (j; 0 .. 64) sparse ~= cast(uint) ((63 - j) * 16 + 5);
    foreach (j; 0 .. count) full ~= cast(uint) (count - 1 - j);
    SlControl control = SlControl(count, limit, ran.ptr, model.ptr,
        [SlWave(dense), SlWave(one), SlWave(sparse), SlWave(full), SlWave(null, true), SlWave(sparse)]);
    auto parent = PrivateRootPark!(SlRoot, slStep).create(farm, 1, 256, policy(counts));
    check(parent !is null);
    check(parent.spawn(SlRoot(null, &control)) == PrivateSpawnResult.spawned);
    check(parent.pump(token) == 1);
    while (view.consumeNext()) {}

    enum fillCount = 100000;
    auto fillResults = new ulong[fillCount];
    auto filler = Park.create(farm, fillCount, 256);
    check(filler !is null);
    foreach (i; 0 .. fillCount) check(filler.spawn(State(fillResults.ptr + i, 1, 0)) == PrivateSpawnResult.spawned);
    size_t admitted;
    auto deadline = MonoTime.currTime + 30.seconds;
    while (true)
    {
        auto n = filler.pump(token);
        if (n == 0) break;
        admitted += n;
        namedCheck(MonoTime.currTime < deadline, "subset large prefill timeout");
    }
    namedCheck(admitted > 0 && admitted < fillCount, "subset large Farm prefilled");
    version (AntfarmActorTestHooks)
    {
        PrivateHookControl hook;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        namedCheck(parent.pump(token) == 0, "subset large full Farm rejects child and polling root tables");
        namedCheck(atomicLoad!(MemoryOrder.raw)(hook.creditCalls) == 1, "subset large zero admission credit released");
        setActorTestHook(null);
        privateControl = null;
    }
    else namedCheck(parent.pump(token) == 0, "subset large full Farm rejects child and polling root tables");
    foreach (v; ran) namedCheck(v == 0, "subset large zero admission invokes no member");
    while (filler.live)
    {
        while (view.consumeNext()) {}
        filler.pump(token);
        namedCheck(MonoTime.currTime < deadline, "subset large prefill drain timeout");
    }
    filler.destroy();
    namedCheck(parent.pump(token) == 1, "subset large partial admission after quota refresh");
    while (view.consumeNext()) {}
    size_t prefix;
    foreach (v; ran) { namedCheck(v <= 1, "subset large partial prefix no duplicates"); prefix += v; }
    namedCheck(prefix > 0 && prefix < dense.length && prefix <= limit, "subset large first table bounded by the child limit");
    if (limit == 256) namedCheck(prefix < limit, "subset large quota admits a strict prefix");
    // Exactly the first `prefix` listed members, in list order, and nobody else.
    auto listed = new bool[count];
    foreach (j; 0 .. prefix) listed[dense[j]] = true;
    foreach (i; 0 .. count)
        namedCheck(ran[i] == (listed[i] ? 1 : 0), "subset large partial prefix is the listed prefix");
    namedCheck(control.completed == 0 && control.busy > 0, "subset large partial wave stays active");
    while (parent.live)
    {
        parent.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "subset large retry timeout");
    }
    namedCheck(control.completed == control.waves.length, "subset large all waves joined");
    parent.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "subset large allocation balance");
}
void subsetLargeCohorts()
{
    foreach (limit; [1UL, 64, 256]) subsetLargeCohort(1025, limit);
}

// ---- Q4: subset-nested ---------------------------------------------------------------------------
// Selector: subset-nested. Two top members each own a leaf cohort and start a
// subset wave on it from inside their operation, so the subset wave joins the
// top wave (the `wave.upstream` credit) and through it the root's
// waitChildren. One leaf is held at a raw barrier; the test thread pumps for a
// while and requires that the root has NOT resumed. Then the held leaf is
// released, the root resumes, checks the leaf run counts and starts a final
// fold in which each top member reads its leaves' participant-ordered results
// and members in a retiring context.
// Catching mutation Q-M15: startSubset omits the
// `atomicFetchAdd(wave.upstream.pending, 1)`: the nested wave is not joined,
// the root resumes while a leaf is held: fails "subset nested leaves ran after
// the join" and "subset nested root resumed before the nested subset joined".
struct SnControl { ulong[12] ran; shared uint arrived, release, rootCalls; }
struct SnLeaf { SnControl* c; uint index, wave, mid; }
ulong snLeaf(scope ref PrivateBorrow!SnLeaf b, scope ref PrivateContext) nothrow @nogc @system
{
    auto c = b.value.c;
    if (b.value.mid == 0 && b.value.index == 5)
    {
        atomicStore!(MemoryOrder.raw)(c.arrived, 1u);
        awaitRaw(c.release, 1u, "subset nested leaf release");
    }
    ++c.ran[b.value.mid * 6 + b.value.index];
    return portsValue(++b.value.wave, b.value.index);
}
struct SnMid { SnControl* c; PrivateChildPark!(SnLeaf, ulong)* leaves; uint mid, wave; }
immutable uint[3] snList = [5, 1, 3];
ulong snBuild(scope ref PrivateBorrow!SnMid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto m = &b.value();
    m.leaves = ctx.createChildren!(SnLeaf, ulong)(6, PrivateCohortOptions(2, true));
    namedCheck(m.leaves !is null, "subset nested leaf create");
    foreach (i; 0 .. 6)
        namedCheck(m.leaves.spawn(ctx, SnLeaf(m.c, cast(uint) i, 0, m.mid)) == PrivateSpawnResult.spawned, "subset nested leaf spawn");
    namedCheck(m.leaves.startSubset!snLeaf(ctx, snList[]) == PrivateStartResult.started, "subset nested leaf subset");
    return portsValue(++m.wave, m.mid);
}
ulong snFold(scope ref PrivateBorrow!SnMid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto m = &b.value();
    auto r = m.leaves.results(ctx); // retiring context; the subset wave was joined by the previous wave
    namedCheck(m.leaves.subsetMembers(ctx) == snList[], "subset nested fold members");
    namedCheck(r.length == 3 && r[0] == portsValue(1, 5) && r[1] == portsValue(1, 1) && r[2] == portsValue(1, 3),
        "subset nested fold results");
    return portsValue(++m.wave, m.mid);
}
struct SnRoot { PrivateChildPark!(SnMid, ulong)* mids; SnControl* c; uint phase; }
PrivateRootDisposition snStep(scope ref PrivateBorrow!SnRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto c = s.c;
    atomicFetchAdd!(MemoryOrder.raw)(c.rootCalls, 1u);
    switch (s.phase++)
    {
        case 0:
            s.mids = ctx.createChildren!(SnMid, ulong)(2);
            namedCheck(s.mids !is null, "subset nested create");
            foreach (i; 0 .. 2)
                namedCheck(s.mids.spawn(ctx, SnMid(c, null, cast(uint) i)) == PrivateSpawnResult.spawned, "subset nested spawn");
            namedCheck(s.mids.start!snBuild(ctx) == PrivateStartResult.started, "subset nested build wave");
            return PrivateRootDisposition.waitChildren;
        case 1:
        {
            foreach (mid; 0 .. 2) foreach (i; 0 .. 6)
                namedCheck(c.ran[mid * 6 + i] == (i == 5 || i == 1 || i == 3 ? 1 : 0), "subset nested leaves ran after the join");
            auto r = s.mids.results(ctx);
            namedCheck(r.length == 2 && r[0] == portsValue(1, 0) && r[1] == portsValue(1, 1), "subset nested build results");
            namedCheck(s.mids.startFinal!snFold(ctx) == PrivateStartResult.started, "subset nested fold wave");
            return PrivateRootDisposition.waitChildren;
        }
        case 2:
        {
            auto r = s.mids.results(ctx);
            namedCheck(r.length == 2 && r[0] == portsValue(2, 0) && r[1] == portsValue(2, 1), "subset nested fold results");
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "subset nested phases");
            return PrivateRootDisposition.finish;
    }
}
void subsetNested(PrivateRootReclamation reclaim)()
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 4, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto workers = new Workers(farm, 3);
    scope(exit) workers.join();
    SnControl control;
    auto park = PrivateRootPark!(SnRoot, snStep, reclaim).create(farm, 1, 2, policy(counts));
    check(park !is null);
    check(park.spawn(SnRoot(null, &control)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad!(MemoryOrder.raw)(control.arrived) == 0)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "subset nested leaf held");
    }
    // The top wave is complete except for the held nested subset wave: keep
    // pumping and collecting; the root must not be resumed.
    auto window = MonoTime.currTime + 200.msecs;
    while (MonoTime.currTime < window)
    {
        park.pump(token);
        namedCheck(atomicLoad!(MemoryOrder.raw)(control.rootCalls) == 1,
            "subset nested root resumed before the nested subset joined");
    }
    atomicStore!(MemoryOrder.raw)(control.release, 1u);
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "subset nested timeout");
    }
    namedCheck(atomicLoad(control.rootCalls) == 3, "subset nested three root calls");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "subset nested allocation balance");
}

// ---- Q5: subset-alloc-fail ---------------------------------------------------------------------
// Selector: subset-alloc-fail. Reserving subset storage costs exactly ONE extra
// allocation of 2 * capacity uints (selection and inverse positions share
// it). Sizes and counts are logged; a failure injected at each allocation of
// a void and a result cohort returns null with a balanced ledger; the budget
// equal to the allocation count succeeds (so no hidden extra allocation);
// capacity beyond uint.max or size_t overflow allocates nothing; a cohort
// without subsets still makes two (void) allocations. The surviving cohort
// is used for a subset wave, and the balance proves its disposal frees the
// selection storage.
// Catching mutations:
//   Q-M16 dispose omits the `sel_` free: fails "subset alloc total allocation
//         balance" (and the failed-create balance when the free is missing
//         from the error path).
//   Q-M17 the sel_ failure path omits freeing `results_` (R cohort), Q-M17b
//         `entries_`: both fail "subset alloc failed create balance".
//   Q-M18 a further (separate) allocation for the reserve: fails "subset alloc
//         reserved void cohort makes three allocations".
//   Q-M19 delete `capacity > uint.max` check: fails "subset alloc
//         uint.max capacity allocates nothing".
struct SfRoot { SizeLog* log; ulong* ran; SbCohort* c; uint phase; }
PrivateRootDisposition sfStep(scope ref PrivateBorrow!SfRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto log = s.log;
    auto counts = &log.counts;
    if (s.phase++ == 1)
    {
        auto r = s.c.results(ctx);
        namedCheck(r.length == 2 && r[0] == portsValue(1, 2) && r[1] == portsValue(1, 0), "subset alloc retry results");
        return PrivateRootDisposition.finish;
    }
    enum subsets = PrivateCohortOptions(0, true);
    static if (size_t.sizeof == 8)
    {
        atomicStore(log.requests, cast(size_t) 0);
        auto plain = ctx.createChildren!SbMember(8, PrivateCohortOptions(5, false));
        namedCheck(plain !is null && atomicLoad(log.requests) == 2, "subset alloc unreserved cohort makes two allocations");
        atomicStore(log.requests, cast(size_t) 0);
        auto reserved = ctx.createChildren!SbMember(8, subsets);
        namedCheck(reserved !is null && atomicLoad(log.requests) == 3, "subset alloc reserved void cohort makes three allocations");
        namedCheck(log.bytes[0] == SbVoidCohort.sizeof && log.bytes[1] == 8 * 40 && log.bytes[2] == 8 * 2 * uint.sizeof,
            "subset alloc one combined selection allocation");
        atomicStore(log.requests, cast(size_t) 0);
        auto typed = ctx.createChildren!(SbMember, ulong)(8, subsets);
        namedCheck(typed !is null && atomicLoad(log.requests) == 4, "subset alloc reserved result cohort makes four allocations");
        namedCheck(log.bytes[0] == SbCohort.sizeof && log.bytes[1] == 8 * 40 && log.bytes[2] == 8 * ulongStorage
            && log.bytes[3] == 8 * 2 * uint.sizeof, "subset alloc result cohort allocation sizes");
    }
    immutable allocated = atomicLoad(counts.allocated);
    immutable freed = atomicLoad(counts.freed);
    namedCheck(ctx.createChildren!(SbMember, ulong)(0, subsets) is null, "subset alloc zero capacity");
    namedCheck(ctx.createChildren!(SbMember, ulong)(size_t.max, subsets) is null, "subset alloc capacity overflow");
    static if (size_t.sizeof == 8)
    {
        // Every allocation attempt is counted and refused: the check must come first.
        atomicStore(counts.budget, 0L);
        atomicStore(log.requests, cast(size_t) 0);
        namedCheck(ctx.createChildren!SbMember(cast(size_t) uint.max + 1, subsets) is null, "subset alloc uint.max capacity");
        namedCheck(ctx.createChildren!(SbMember, ulong)(cast(size_t) uint.max + 1, subsets) is null, "subset alloc uint.max result capacity");
        namedCheck(atomicLoad(log.requests) == 0, "subset alloc uint.max capacity allocates nothing");
        atomicStore(counts.budget, long.max);
    }
    namedCheck(atomicLoad(counts.allocated) == allocated, "subset alloc invalid capacity allocates nothing");
    foreach (budget; 0 .. 3)
    {
        atomicStore(counts.budget, cast(long) budget);
        namedCheck(ctx.createChildren!SbMember(4, subsets) is null, "subset alloc void cohort/entries/selection failure");
        namedCheck(atomicLoad(counts.allocated) - allocated == atomicLoad(counts.freed) - freed, "subset alloc failed create balance");
    }
    foreach (budget; 0 .. 4)
    {
        atomicStore(counts.budget, cast(long) budget);
        namedCheck(ctx.createChildren!(SbMember, ulong)(4, subsets) is null, "subset alloc result cohort failure");
        namedCheck(atomicLoad(counts.allocated) - allocated == atomicLoad(counts.freed) - freed, "subset alloc failed create balance");
    }
    atomicStore(counts.budget, 3L);
    auto exactVoid = ctx.createChildren!SbMember(4, subsets);
    namedCheck(exactVoid !is null, "subset alloc budget equal to the allocation count succeeds");
    atomicStore(counts.budget, 4L);
    auto children = ctx.createChildren!(SbMember, ulong)(3, subsets);
    namedCheck(children !is null, "subset alloc budget equal to the result allocation count succeeds");
    atomicStore(counts.budget, long.max);
    s.c = children;
    foreach (i; 0 .. 3)
        namedCheck(children.spawn(ctx, SbMember(s.ran, cast(uint) i)) == PrivateSpawnResult.spawned, "subset alloc retry spawn");
    immutable uint[2] list = [2, 0];
    namedCheck(children.startSubset!sbTag(ctx, list[]) == PrivateStartResult.started, "subset alloc retry start");
    return PrivateRootDisposition.waitChildren;
}
void subsetAllocationFailures()
{
    SizeLog log;
    ulong[3] ran;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SfRoot, sfStep).create(farm, 1, 256,
        ActorAllocator(&log, &sizeLogAllocate, &sizeLogDeallocate));
    check(park !is null);
    check(park.spawn(SfRoot(&log, ran.ptr)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "subset alloc timeout");
    }
    park.destroy();
    namedCheck(atomicLoad(log.counts.allocated) == atomicLoad(log.counts.freed), "subset alloc total allocation balance");
}

// ---- Q6: subset-dtor ------------------------------------------------------------------------------
// Selector: subset-dtor. Non-POD members (the E2-1 DtKid logger) with a subset
// wave: the wave runs the operation only for the selected members and
// destroys NOBODY, selected or not. Mode 0: the root finishes and cohort
// disposal destroys every member exactly once, on a consumer, after its op.
// Mode 1: a whole startFinal retires every member (selected, unselected)
// exactly once. Both reclamation policies.
// Catching mutations:
//   Q-M20 startSubset sets `retire_ = true`: the selected members are
//         destroyed by the subset wave: fails "subset dtor wave destroys nobody".
//   Q-M22 dispose walks only `k_` entries after a subset wave instead of
//         every member: fails "subset dtor unselected member destroyed once".
struct SgRoot { PrivateChildPark!(DtKid, ulong)* kids; uint phase; ubyte mode; }
immutable uint[2] sgList = [3, 0]; // member ids 4 and 1 (id = index + 1)
PrivateRootDisposition sgStep(scope ref PrivateBorrow!SgRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    switch (s.phase++)
    {
        case 0:
            s.kids = ctx.createChildren!(DtKid, ulong)(6, PrivateCohortOptions(0, true));
            namedCheck(s.kids !is null, "subset dtor create");
            foreach (id; 1 .. 7)
                namedCheck(s.kids.spawn(ctx, DtKid(cast(uint) id)) == PrivateSpawnResult.spawned, "subset dtor spawn");
            namedCheck(s.kids.startSubset!dtKidTag(ctx, sgList[]) == PrivateStartResult.started, "subset dtor subset wave");
            return PrivateRootDisposition.waitChildren;
        case 1:
        {
            foreach (id; 1 .. 7)
            {
                namedCheck(dtCount(id) == 0, "subset dtor wave destroys nobody");
                namedCheck(dtCount(id + dtOp) == (id == 4 || id == 1 ? 1 : 0), "subset dtor only selected members ran");
            }
            auto r = s.kids.results(ctx);
            namedCheck(r.length == 2 && r[0] == portsValue(1, 4) && r[1] == portsValue(1, 1), "subset dtor results");
            namedCheck(s.kids.subsetMembers(ctx) == sgList[], "subset dtor members");
            if (s.mode == 0) return PrivateRootDisposition.finish;
            namedCheck(s.kids.startFinal!dtKidTag(ctx) == PrivateStartResult.started, "subset dtor final wave");
            return PrivateRootDisposition.waitChildren;
        }
        case 2:
        {
            foreach (id; 1 .. 7)
            {
                namedCheck(dtCount(id) == 1, "subset dtor final destroyed every member before the join");
                namedCheck(dtCount(id + dtOp) == (id == 4 || id == 1 ? 2 : 1), "subset dtor final op count");
            }
            namedCheck(s.kids.results(ctx).length == 6, "subset dtor final results");
            namedCheck(s.kids.subsetMembers(ctx).length == 0, "subset dtor final members");
            return PrivateRootDisposition.finish;
        }
        default:
            namedCheck(false, "subset dtor phases");
            return PrivateRootDisposition.finish;
    }
}
void subsetDtor(PrivateRootReclamation reclaim)(ubyte mode)
{
    auto h = DtHarness(2);
    scope(exit) h.close();
    auto park = PrivateRootPark!(SgRoot, sgStep, reclaim).create(h.farm, 1, 2, policy(h.counts));
    check(park !is null);
    check(park.spawn(SgRoot(null, 0, mode)) == PrivateSpawnResult.spawned);
    h.drain(park, "subset dtor completion");
    foreach (id; 1 .. 7)
    {
        dtStoredOnConsumer(id, "subset dtor unselected member destroyed once");
        if (id == 4 || id == 1) namedCheck(dtPos(id + dtOp) < dtPos(id), "subset dtor op before destructor");
    }
    park.destroy();
    namedCheck(atomicLoad(h.counts.allocated) == atomicLoad(h.counts.freed), "subset dtor allocation balance");
}

// ---- Q7: subset-late-tail (TSan, deterministic) ----------------------------------------------
// Selector: subset-late-tail. The generation fence: the wave finisher (worker
// W) is paused at privateRootAfterIdleBeforeCredit, after its idle release and
// before the root credit. The polling parent sees `finished`, checks the
// results and members, and immediately starts a DIFFERENT, longer subset (it
// rewrites sel_, the inverse positions, k_, the header and the flags) while
// W's tail is still running. The second wave runs on this consumer (W is
// paused), is joined, checked, and the root finishes strictly, which frees
// the whole cohort (selection storage included) before W resumes. W then
// releases the credit and the Entry is recycled.
// Catching mutation Q-M23: `finish` reads `k_` between the idle store and the
// hook (Q-M23a: TSan reports the data race with the second start's write of
// `k_`), or reads `sel_[0]` after the hook (Q-M23b: TSan reports the heap use
// after free of the disposed cohort). Non-TSan builds pass: this is TSan-only
// evidence, like P-M7.
struct StControl { ulong[8] ran; uint[8] model; uint calls, stage, reads; }
struct StRoot { SbCohort* c; StControl* k; }
immutable uint[3] stA = [6, 2, 4];
immutable uint[4] stB = [2, 6, 1, 5];
PrivateRootDisposition stStep(scope ref PrivateBorrow!StRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    auto k = s.k;
    ++k.calls;
    if (s.c is null)
    {
        s.c = ctx.createChildren!(SbMember, ulong)(8, PrivateCohortOptions(256, true));
        namedCheck(s.c !is null, "subset tail create");
        foreach (i; 0 .. 8)
            namedCheck(s.c.spawn(ctx, SbMember(k.ran.ptr, cast(uint) i)) == PrivateSpawnResult.spawned, "subset tail spawn");
        namedCheck(s.c.startSubset!sbTag(ctx, stA[]) == PrivateStartResult.started, "subset tail first subset");
        foreach (i; stA) ++k.model[i];
        return PrivateRootDisposition.again;
    }
    if (!s.c.finished(ctx)) return PrivateRootDisposition.again;
    if (k.stage == 0)
    {
        sbCheckSubset(s.c, ctx, k.ran[], k.model[], stA[], "subset tail first wave");
        namedCheck(s.c.startSubset!sbTag(ctx, stB[]) == PrivateStartResult.started, "subset tail second subset");
        foreach (i; stB) ++k.model[i];
        k.stage = 1;
        return PrivateRootDisposition.again;
    }
    sbCheckSubset(s.c, ctx, k.ran[], k.model[], stB[], "subset tail second wave");
    ++k.reads;
    return PrivateRootDisposition.finish;
}
version (AntfarmActorTestHooks)
{
    void subsetLateTail(PrivateRootReclamation reclaim)()
    {
        Counts counts;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        StControl c;
        auto park = PrivateRootPark!(StRoot, stStep, reclaim).create(farm, 1, 256, policy(counts));
        check(park !is null);
        check(park.spawn(StRoot(null, &c)) == PrivateSpawnResult.spawned);
        check(park.pump(token) == 1);
        while (view.consumeNext()) {} // first callback starts the first subset on this consumer
        PrivateHookControl hook;
        hook.point = ActorTestPoint.privateRootAfterIdleBeforeCredit;
        hook.armed = 1;
        privateControl = &hook;
        setActorTestHook(&privateHook);
        shared uint ready, go, leave;
        auto worker = new Thread({
            ConsumerView other;
            check(other.subscribe(farm) >= 0);
            scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(ready, 1u);
            awaitRaw(go, 1u, "subset tail worker go");
            while (atomicLoad!(MemoryOrder.raw)(leave) == 0) other.consumeNext();
        });
        worker.start();
        awaitRaw(ready, 1u, "subset tail worker ready");
        check(park.pump(token) == 1); // child table, then polling root
        atomicStore!(MemoryOrder.raw)(go, 1u);
        awaitRaw(hook.arrived, 1u, "subset tail idle tail paused");
        auto deadline = MonoTime.currTime + 10.seconds;
        while (c.reads == 0)
        {
            while (view.consumeNext()) {}
            park.pump(token);
            namedCheck(MonoTime.currTime < deadline, "subset tail second wave while the first tail is paused");
        }
        namedCheck(c.stage == 1 && c.reads == 1, "subset tail parent started and joined a new generation");
        park.poll();
        namedCheck(park.live == 1, "subset tail Entry pinned by the paused tail");
        atomicStore!(MemoryOrder.raw)(hook.release, 1u);
        while (park.live)
        {
            park.poll();
            namedCheck(MonoTime.currTime < deadline, "subset tail completion");
        }
        park.destroy();
        atomicStore!(MemoryOrder.raw)(leave, 1u);
        worker.join();
        setActorTestHook(null);
        privateControl = null;
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "subset tail allocation balance");
    }
}

// ---- Q8: subset-limits ---------------------------------------------------------------------------
// Selector: subset-limits (audit hooks). `PrivateCohortOptions.batchLimit`
// through the options overload: explicit 7 splits a full 50-member subset into
// 8 tables of at most 7; 0 inherits the creating context's effective limit (5
// for a grandchild cohort created in a limit-5 member): 10 tables.
// Catching mutations:
//   Q-M24 the options overload ignores `options.batchLimit` (always inherits):
//         fails "explicit child table exceeds limit 7".
//   Q-M25 the options overload does not inherit on 0 (passes the root's limit
//         through): fails "inherited subset table count".
static immutable uint[50] smReverse = () {
    uint[50] a;
    foreach (i; 0 .. 50) a[i] = cast(uint) (49 - i);
    return a;
}();
version (AntfarmWriteAuditHooks)
{
    struct SmExplicitRoot { PrivateChildPark!BatchChild* children; ulong* slots; }
    PrivateRootDisposition smExplicitStep(scope ref PrivateBorrow!SmExplicitRoot b,
        scope ref PrivateContext ctx) nothrow @nogc @system
    {
        enum children = 50;
        enum limit = 7;
        if (b.value.children is null)
        {
            b.value.children = ctx.createChildren!BatchChild(children, PrivateCohortOptions(limit, true));
            check(b.value.children !is null, "subset explicit create", 0);
            foreach (i; 0 .. children)
                check(b.value.children.spawn(ctx, BatchChild(b.value.slots + i)) == PrivateSpawnResult.spawned,
                    "subset explicit spawn", 0);
            check(b.value.children.startSubset!batchChildOperation(ctx, smReverse[]) == PrivateStartResult.started,
                "subset explicit start", 0);
            return PrivateRootDisposition.again;
        }
        if (!b.value.children.finished(ctx)) return PrivateRootDisposition.again;
        foreach (i; 0 .. children) check(b.value.slots[i] == 1, "subset explicit joined", 0);
        return PrivateRootDisposition.finish;
    }
    struct SmMiddle { PrivateChildPark!BatchChild* children; ulong* slots; }
    void smMiddleStep(scope ref PrivateBorrow!SmMiddle b, scope ref PrivateContext ctx) nothrow @nogc @system
    {
        enum grandchildren = 50;
        // Options with batchLimit 0: inherit this cohort's 5, not the root's 31.
        b.value.children = ctx.createChildren!BatchChild(grandchildren, PrivateCohortOptions(0, true));
        check(b.value.children !is null, "subset inherited create", 0);
        foreach (i; 0 .. grandchildren)
            check(b.value.children.spawn(ctx, BatchChild(b.value.slots + i)) == PrivateSpawnResult.spawned,
                "subset inherited spawn", 0);
        check(b.value.children.startSubset!batchChildOperation(ctx, smReverse[]) == PrivateStartResult.started,
            "subset inherited start", 0);
    }
    struct SmInheritRoot { PrivateChildPark!SmMiddle* middle; ulong* slots; }
    PrivateRootDisposition smInheritStep(scope ref PrivateBorrow!SmInheritRoot b,
        scope ref PrivateContext ctx) nothrow @nogc @system
    {
        if (b.value.middle is null)
        {
            b.value.middle = ctx.createChildren!SmMiddle(1, 5);
            check(b.value.middle !is null, "subset middle create", 0);
            check(b.value.middle.spawn(ctx, SmMiddle(null, b.value.slots)) == PrivateSpawnResult.spawned,
                "subset middle spawn", 0);
            check(b.value.middle.start!smMiddleStep(ctx) == PrivateStartResult.started, "subset middle start", 0);
            return PrivateRootDisposition.again;
        }
        if (!b.value.middle.finished(ctx)) return PrivateRootDisposition.again;
        foreach (i; 0 .. 50) check(b.value.slots[i] == 1, "subset inherited joined", 0);
        return PrivateRootDisposition.finish;
    }
    void subsetLimits()
    {
        ulong[50] slots;
        drainCounted!(SmExplicitRoot, smExplicitStep)(256, slots[], 7, 8, "explicit subset table count");
        slots[] = 0;
        drainCounted!(SmInheritRoot, smInheritStep)(31, slots[], 5, 10, "inherited subset table count");
    }
}

// ---- SD1-SD4: death modes ------------------------------------------------------------------------
// Selectors subset-dup-adjacent, subset-dup-gap, subset-range, subset-range-max,
// subset-too-many, subset-unreserved, subset-unreserved-empty,
// subset-unreserved-options, subset-wrong-parent, subset-default-context,
// subset-members-wrong-parent (run through deathHarness: LDC and DMD, debug
// and -release). The harness needs a nonzero death AND the exact fatal text,
// and that the survivor marker never printed.
// Catching mutations:
//   Q-M26 duplicate check removed: the duplicate lists start a double-dispatch
//         wave and the root dies with "retire private actor with active child
//         wave" instead of the duplicate-member text.
//   Q-M27 range check `i >= capacity_` instead of `i >= count_`: the list
//         [0, 4] on four members of capacity eight dies with the active-child
//         text instead of the range text.
//   Q-M28 the range check deleted: [0, 4] as above; [uint.max] segfaults.
//   Q-M29 `members.length > count_` check deleted: [0,1,2,3,3] reaches the
//         duplicate check instead (wrong fatal text).
//   Q-M30 `sel_ is null` fatal deleted: the unreserved modes segfault, and the
//         empty-list mode survives ("subset misuse: survived").
//   Q-M31 `authorized(context)` deleted from startSubset / subsetMembers.
enum SubsetMisuse : ubyte
{
    dupAdjacent, dupGap, range, rangeMax, tooMany, unreserved, unreservedEmpty,
    unreservedOptions, wrongParent, defaultContext, membersWrongParent,
}
struct SdProbe { SbCohort* children; ulong* ran; SubsetMisuse misuse; }
struct SdRoot { SdProbe* probe; bool owner; }
immutable uint[2] sdDupAdjacent = [2, 2];
immutable uint[3] sdDupGap = [1, 3, 1];
immutable uint[2] sdRange = [0, 4];
immutable uint[1] sdRangeMax = [uint.max];
immutable uint[5] sdTooMany = [0, 1, 2, 3, 3];
immutable uint[1] sdOne = [0];
PrivateRootDisposition sdStep(scope ref PrivateBorrow!SdRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto probe = b.value.probe;
    immutable misuse = probe.misuse;
    immutable foreign = misuse == SubsetMisuse.wrongParent || misuse == SubsetMisuse.defaultContext
        || misuse == SubsetMisuse.membersWrongParent;
    if (b.value.owner)
    {
        if (probe.children is null)
        {
            probe.children = misuse == SubsetMisuse.unreserved || misuse == SubsetMisuse.unreservedEmpty
                ? ctx.createChildren!(SbMember, ulong)(8)
                : ctx.createChildren!(SbMember, ulong)(8, PrivateCohortOptions(0, misuse != SubsetMisuse.unreservedOptions));
            namedCheck(probe.children !is null, "subset death owner cohort");
            foreach (i; 0 .. 4)
                namedCheck(probe.children.spawn(ctx, SbMember(probe.ran, cast(uint) i)) == PrivateSpawnResult.spawned, "subset death spawn");
        }
        if (foreign) return PrivateRootDisposition.again;
        final switch (misuse)
        {
            case SubsetMisuse.dupAdjacent: probe.children.startSubset!sbTag(ctx, sdDupAdjacent[]); break;
            case SubsetMisuse.dupGap: probe.children.startSubset!sbTag(ctx, sdDupGap[]); break;
            case SubsetMisuse.range: probe.children.startSubset!sbTag(ctx, sdRange[]); break;
            case SubsetMisuse.rangeMax: probe.children.startSubset!sbTag(ctx, sdRangeMax[]); break;
            case SubsetMisuse.tooMany: probe.children.startSubset!sbTag(ctx, sdTooMany[]); break;
            case SubsetMisuse.unreserved:
            case SubsetMisuse.unreservedOptions:
                probe.children.startSubset!sbTag(ctx, sdOne[]); break;
            case SubsetMisuse.unreservedEmpty: probe.children.startSubset!sbTag(ctx, null); break;
            case SubsetMisuse.wrongParent:
            case SubsetMisuse.defaultContext:
            case SubsetMisuse.membersWrongParent: break;
        }
        fprintf(stderr, "subset misuse: survived\n");
        return PrivateRootDisposition.finish;
    }
    namedCheck(probe.children !is null, "subset death cohort exists");
    PrivateContext empty;
    final switch (misuse)
    {
        case SubsetMisuse.wrongParent: probe.children.startSubset!sbTag(ctx, sdOne[]); break;
        case SubsetMisuse.defaultContext: probe.children.startSubset!sbTag(empty, sdOne[]); break;
        case SubsetMisuse.membersWrongParent: cast(void) probe.children.subsetMembers(ctx); break;
        case SubsetMisuse.dupAdjacent:
        case SubsetMisuse.dupGap:
        case SubsetMisuse.range:
        case SubsetMisuse.rangeMax:
        case SubsetMisuse.tooMany:
        case SubsetMisuse.unreserved:
        case SubsetMisuse.unreservedEmpty:
        case SubsetMisuse.unreservedOptions: break;
    }
    fprintf(stderr, "subset misuse: survived\n");
    return PrivateRootDisposition.finish;
}
void subsetMisuseDeath(SubsetMisuse misuse)
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    ulong[8] ran;
    SdProbe probe;
    probe.ran = ran.ptr;
    probe.misuse = misuse;
    auto park = PrivateRootPark!(SdRoot, sdStep).create(farm, 2);
    check(park !is null);
    check(park.spawn(SdRoot(&probe, true)) == PrivateSpawnResult.spawned);
    if (misuse == SubsetMisuse.wrongParent || misuse == SubsetMisuse.defaultContext
        || misuse == SubsetMisuse.membersWrongParent)
        check(park.spawn(SdRoot(&probe, false)) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
    // Reaching this point means the fatal contract was not enforced.
}
struct SubsetDeathMode { string selector; SubsetMisuse misuse; string text; }
immutable SubsetDeathMode[] subsetDeathModes = [
    SubsetDeathMode("subset-dup-adjacent", SubsetMisuse.dupAdjacent, "antfarm fatal: private child subset: duplicate member"),
    SubsetDeathMode("subset-dup-gap", SubsetMisuse.dupGap, "antfarm fatal: private child subset: duplicate member"),
    SubsetDeathMode("subset-range", SubsetMisuse.range, "antfarm fatal: private child subset: member out of range"),
    SubsetDeathMode("subset-range-max", SubsetMisuse.rangeMax, "antfarm fatal: private child subset: member out of range"),
    SubsetDeathMode("subset-too-many", SubsetMisuse.tooMany, "antfarm fatal: private child subset: too many members"),
    SubsetDeathMode("subset-unreserved", SubsetMisuse.unreserved, "antfarm fatal: private child subset: storage not reserved"),
    SubsetDeathMode("subset-unreserved-empty", SubsetMisuse.unreservedEmpty, "antfarm fatal: private child subset: storage not reserved"),
    SubsetDeathMode("subset-unreserved-options", SubsetMisuse.unreservedOptions, "antfarm fatal: private child subset: storage not reserved"),
    SubsetDeathMode("subset-wrong-parent", SubsetMisuse.wrongParent, "antfarm fatal: private child park: unauthorized context"),
    SubsetDeathMode("subset-default-context", SubsetMisuse.defaultContext, "antfarm fatal: private child park: unauthorized context"),
    SubsetDeathMode("subset-members-wrong-parent", SubsetMisuse.membersWrongParent, "antfarm fatal: private child park: unauthorized context"),
];
void subsetDeath()
{
    version (linux)
    {
        foreach (mode; subsetDeathModes)
        {
            auto probe = probeDeath(mode.selector);
            check(probe.status != 0);
            check(probe.stderrText.canFind(mode.text));
            check(!probe.stderrText.canFind("subset misuse: survived"));
        }
        writeln("PASS subset duplicate, out-of-range, too-many, unreserved and wrong-parent are fatal");
    }
    else
        writeln("SKIP subset death modes: /proc/self/exe re-exec requires Linux");
}

void phaseSubset()
{
    subsetBasic!(PrivateRootReclamation.emitter)();
    subsetBasic!(PrivateRootReclamation.consumer)();
    writeln("PASS subset-basic sparse/dense/full/repeated/void/whole waves, empty list, refill, both policies");
    subsetActive!(PrivateRootReclamation.emitter)();
    subsetActive!(PrivateRootReclamation.consumer)();
    writeln("PASS subset-active start over an active wave returns waveActive without side effects");
    subsetLargeCohorts(); writeln("PASS subset-large 1025 members, child limits 1/64/256, zero and partial quota admission");
    subsetNested!(PrivateRootReclamation.emitter)();
    subsetNested!(PrivateRootReclamation.consumer)();
    writeln("PASS subset-nested subset wave joins its parent wave and the root's waitChildren");
    subsetAllocationFailures(); writeln("PASS subset-alloc-fail one selection allocation, balanced failures");
    foreach (mode; 0 .. 2)
    {
        subsetDtor!(PrivateRootReclamation.emitter)(cast(ubyte) mode);
        subsetDtor!(PrivateRootReclamation.consumer)(cast(ubyte) mode);
    }
    writeln("PASS subset-dtor subset waves destroy no member; disposal and startFinal destroy all once");
    version (AntfarmActorTestHooks)
    {
        subsetLateTail!(PrivateRootReclamation.emitter)();
        subsetLateTail!(PrivateRootReclamation.consumer)();
        writeln("PASS subset-late-tail new subset generation started under a paused idle tail");
    }
    version (AntfarmWriteAuditHooks)
    {
        subsetLimits(); writeln("PASS subset-limits options.batchLimit explicit and inherited");
    }
}

// ---- E2-3: child entries carry no Service pointer ----------------------------------------------
// Selectors: entry-word, entry-word-death (debug builds; run through deathHarness).
// Roots keep their Service in the Entry; a child member's word at that offset
// is not a pointer, and every member reaches the Service through its ChildBase,
// at depth one and two. Catching mutations:
//   E3-M1 PrivateContext.service reads rootService(actor_) for members too: the
//         members' contexts die with "service read on a child" (debug), or
//         return a null service in -release ("entry word mid derives the
//         root's service").
//   E3-M2 dispose reads rootService(base.parent): the nested cohort, whose
//         parent is a member, is fatal in debug builds.
//   E3-M3 emit reads the old base_.parent.service for a nested cohort: a
//         null Service is dereferenced (crash).
version (AntfarmActorTestHooks)
{
    struct EwControl { void* service; uint midChecks, leafChecks; bool misuse; }
    struct EwLeaf { EwControl* c; }
    void ewLeaf(scope ref PrivateBorrow!EwLeaf b, scope ref PrivateContext ctx) nothrow @nogc @system
    {
        auto c = b.value.c;
        namedCheck(privateTestServiceWord(ctx) == 0, "entry word leaf is not a pointer");
        namedCheck(privateTestContextService(ctx) is c.service, "entry word leaf derives the root's service");
        ++c.leafChecks;
    }
    struct EwMid { EwControl* c; PrivateChildPark!EwLeaf* leaves; }
    void ewMid(scope ref PrivateBorrow!EwMid b, scope ref PrivateContext ctx) nothrow @nogc @system
    {
        auto m = &b.value();
        auto c = m.c;
        if (c.misuse) cast(void) privateTestRootService(ctx);
        namedCheck(privateTestServiceWord(ctx) == 0, "entry word mid is not a pointer");
        namedCheck(privateTestContextService(ctx) is c.service, "entry word mid derives the root's service");
        ++c.midChecks;
        m.leaves = ctx.createChildren!EwLeaf(2);
        namedCheck(m.leaves !is null, "entry word leaf cohort");
        foreach (i; 0 .. 2)
            namedCheck(m.leaves.spawn(ctx, EwLeaf(c)) == PrivateSpawnResult.spawned, "entry word leaf spawn");
        namedCheck(m.leaves.start!ewLeaf(ctx) == PrivateStartResult.started, "entry word leaf wave");
    }
    struct EwRoot { EwControl* c; PrivateChildPark!EwMid* mids; uint phase; }
    PrivateRootDisposition ewStep(scope ref PrivateBorrow!EwRoot b, scope ref PrivateContext ctx)
        nothrow @nogc @system
    {
        auto s = &b.value();
        auto c = s.c;
        if (s.phase++ != 0)
        {
            namedCheck(c.midChecks == 2 && c.leafChecks == 4, "entry word members ran");
            return PrivateRootDisposition.finish;
        }
        namedCheck(privateTestServiceWord(ctx) != 0, "entry word root holds its service");
        c.service = privateTestRootService(ctx);
        namedCheck(cast(ulong) c.service == privateTestServiceWord(ctx), "entry word root accessor returns the word");
        namedCheck(privateTestContextService(ctx) is c.service, "entry word root derives its service");
        s.mids = ctx.createChildren!EwMid(2);
        namedCheck(s.mids !is null, "entry word mid cohort");
        foreach (i; 0 .. 2)
            namedCheck(s.mids.spawn(ctx, EwMid(c)) == PrivateSpawnResult.spawned, "entry word mid spawn");
        namedCheck(s.mids.start!ewMid(ctx) == PrivateStartResult.started, "entry word mid wave");
        return PrivateRootDisposition.waitChildren;
    }
    void entryWord(bool misuse)
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        EwControl control;
        control.misuse = misuse;
        auto park = PrivateRootPark!(EwRoot, ewStep).create(farm, 1);
        check(park !is null);
        check(park.spawn(EwRoot(&control)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 30.seconds;
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "entry word timeout");
        }
        park.destroy();
        if (misuse) fprintf(stderr, "entry word misuse: survived\n");
    }
    void entryWordDeath()
    {
        version (linux)
        {
            version (assert)
            {
                auto probe = probeDeath("entry-word-misuse");
                check(probe.status != 0);
                check(probe.stderrText.canFind("antfarm fatal: private entry: service read on a child"));
                check(!probe.stderrText.canFind("entry word misuse: survived"));
                writeln("PASS entry-word-death child service read is fatal in debug builds");
            }
        }
        else
            writeln("SKIP entry-word death: /proc/self/exe re-exec requires Linux");
    }
}
void phaseEntryWord()
{
    version (AntfarmActorTestHooks)
    {
        entryWord(false);
        writeln("PASS entry-word roots hold the Service, members read it through ChildBase at depth two");
    }
}

// ---- E2-4 continuations -----------------------------------------------------
// Raw barriers below control scheduling only. The runtime join/return/table
// chains must carry actor writes; no test release/acquire publishes them.
struct CtControl
{
    uint[256] calls, destroyed;
    ulong[256] writes;
    uint width = 2, depth = 2;
    ulong emitter;
    }
struct CtNode
{
    CtControl* c;
    PrivateChildPark!(CtNode, ulong)* children;
    uint id, depth, phase;
    ~this() nothrow @nogc @system
    {
        if (c is null) return; // moved-from caller values
        namedCheck(portsThreadId() != c.emitter, "cont stored destructor on consumer");
        foreach (i; 0 .. (depth ? c.width : 0))
            namedCheck(c.destroyed[id * c.width + i + 1] == 1, "cont post-order destructor");
        namedCheck(++c.destroyed[id] == 1, "cont destructor exactly once");
    }
}
PrivateChildStep!ulong ctFold(scope ref PrivateBorrow!CtNode b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto n = &b.value(); auto c = n.c;
    ++c.calls[n.id];
    if (!n.depth) { c.writes[n.id] = portsValue(1, n.id); return typeof(return).done(c.writes[n.id]); }
    if (n.phase++ == 0)
    {
        n.children = ctx.createChildren!(CtNode, ulong)(c.width);
        namedCheck(n.children !is null, "cont tree children create");
        foreach (i; 0 .. c.width)
            check(n.children.spawn(ctx, CtNode(c, null, n.id * c.width + i + 1, n.depth - 1)) == PrivateSpawnResult.spawned);
        check(n.children.startFinal!ctFold(ctx) == PrivateStartResult.started);
        return typeof(return).waitChildren();
    }
    ulong sum; foreach (v; n.children.results(ctx)) sum += v;
    c.writes[n.id] = sum;
    return typeof(return).done(sum);
}
struct CtRoot { CtControl* c; PrivateChildPark!(CtNode, ulong)* children; uint phase; }
PrivateRootDisposition ctRoot(scope ref PrivateBorrow!CtRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto r = &b.value(); auto c = r.c;
    if (r.phase++ == 0)
    {
        r.children = ctx.createChildren!(CtNode, ulong)(1);
        check(r.children !is null);
        check(r.children.spawn(ctx, CtNode(c, null, 0, c.depth)) == PrivateSpawnResult.spawned);
        check(r.children.startFinal!ctFold(ctx) == PrivateStartResult.started);
        return PrivateRootDisposition.waitChildren;
    }
    auto values = r.children.results(ctx);
    uint total = c.width == 1 ? c.depth + 1 : (1u << (c.depth + 1)) - 1;
    ulong expected;
    if (c.width == 1) expected = portsValue(1, c.depth);
    else foreach (i; total / 2 .. total) expected += portsValue(1, i);
    namedCheck(values.length == 1 && values[0] == expected && c.writes[0] == expected, "cont tree independent leaf sum");
    check(c.destroyed[0] == 1);
    check(r.children.startFinal!ctFold(ctx) == PrivateStartResult.empty);
    return PrivateRootDisposition.finish;
}
// C1/C7/C12: C-M1 loses the resume; C-M7b calls the op on cleanup.
void contTree(PrivateRootReclamation reclaim)(uint depth = 2, uint width = 2)
{
    Counts counts; CtControl c; c.depth = depth; c.width = width; c.emitter = portsThreadId();
    auto farm = AntFarm.create(); scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 5); scope(exit) workers.join();
    auto park = PrivateRootPark!(CtRoot, ctRoot, reclaim).create(farm, 1, 64, policy(counts));
    check(park.spawn(CtRoot(&c)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 15.seconds;
    while (park.live) { park.pump(token); namedCheck(MonoTime.currTime < deadline, "cont tree completion deadline"); }
    uint total = width == 1 ? depth + 1 : (1u << (depth + 1)) - 1;
    foreach (i; 0 .. total)
    {
        check(c.destroyed[i] == 1);
        check(c.calls[i] == (i >= total / 2 && width == 2 || i == depth && width == 1 ? 1 : 2));
    }
    park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}
struct CyControl { uint calls; ulong value; }
struct CyMember { CyControl* c; uint phase; }
uint contDepth; // TLS: inline recursive dispatch must fail
PrivateChildDisposition cyYield(scope ref PrivateBorrow!CyMember b, scope ref PrivateContext)
    nothrow @nogc @system
{
    namedCheck(++contDepth == 1, "cont yield never inline"); scope(exit) --contDepth;
    ++b.value.c.calls;
    namedCheck(b.value.c.value == b.value.phase, "cont prior activation visible");
    b.value.c.value = ++b.value.phase;
    return b.value.phase == 4 ? PrivateChildDisposition.done : PrivateChildDisposition.waitChildren;
}
struct CyRoot { CyControl* c; PrivateChildPark!CyMember* members; uint phase; }
PrivateRootDisposition cyRoot(scope ref PrivateBorrow!CyRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.phase++ == 0)
    {
        b.value.members = ctx.createChildren!CyMember(1);
        check(b.value.members.spawn(ctx, CyMember(b.value.c)) == PrivateSpawnResult.spawned);
        check(b.value.members.start!cyYield(ctx) == PrivateStartResult.started);
        return PrivateRootDisposition.waitChildren;
    }
    check(b.value.c.calls == 4 && b.value.c.value == 4);
    return PrivateRootDisposition.finish;
}
// C2: C-M2 inline re-call violates the later-pump observation.
void contYield(PrivateRootReclamation reclaim)()
{
    Counts counts; CyControl c;
    auto farm = AntFarm.create(); scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
    ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(CyRoot, cyRoot, reclaim).create(farm, 1, 64, policy(counts));
    check(park.spawn(CyRoot(&c)) == PrivateSpawnResult.spawned);
    park.pump(token); while (view.consumeNext()) {}
    foreach (expected; 1 .. 5)
    {
        park.pump(token); while (view.consumeNext()) {}
        namedCheck(c.calls == expected, "cont yield exactly one activation per pump");
        while (view.consumeNext()) {}
        check(c.calls == expected);
    }
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live) { park.pump(token); while (view.consumeNext()) {} check(MonoTime.currTime < deadline); }
    park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}
enum CiMode { early, late, hold, lateHold, done, finalDone, beforeP, afterP, overflow, serviceWord, finishSuspended }
struct CiControl
{
    CiMode mode;
    shared uint midCalls, leafArrived, leafGo, custodyReleased, hookArrived, hookGo;
    shared uint activeSeen, rootDestroyed, midDestroyed, leafDestroyed, returnCalls, leafReturned, midReturned;
    ulong value, result;
    ulong emitter; bool seal;
    shared uint emitterReturns, pPaused, descendantReleased, armPolling, pollEntered, midFrees, leafFrees;
    void* midState; void* leafState;
}
// Identify actor state allocations by the addresses observed in callbacks,
// so root metadata reclamation does not get mistaken for a child-state free.
struct CiAlloc { Counts* counts; CiControl* c; }
void* ciAllocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{ return allocate((cast(CiAlloc*) context).counts, bytes, alignment); }
void ciDeallocate(void* context, void* ptr, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto a = cast(CiAlloc*) context; auto c = a.c;
    if (ptr is c.midState || ptr is c.leafState)
    {
        namedCheck(portsThreadId() != c.emitter, "cont child state free on consumer");
        if (ptr is c.midState) namedCheck(atomicFetchAdd!(MemoryOrder.raw)(c.midFrees, 1u) == 0, "cont mid state freed once");
        else namedCheck(atomicFetchAdd!(MemoryOrder.raw)(c.leafFrees, 1u) == 0, "cont leaf state freed once");
    }
    deallocate(a.counts, ptr, bytes, alignment);
}
ActorAllocator ciPolicy(ref CiAlloc a) nothrow @nogc @system
{ return ActorAllocator(&a, &ciAllocate, &ciDeallocate); }
uint ciRole; // TLS: 1=member, 2=leaf, 3=polling parent
bool ciEmitter, ciSawLeaf;
uint ciMidsInConsume;
struct CiLeaf
{
    CiControl* c;
    ~this() nothrow @nogc @system
    { if (c !is null) { check(portsThreadId() != c.emitter); atomicFetchAdd!(MemoryOrder.raw)(c.leafDestroyed, 1u); } }
}
ulong ciLeaf(scope ref PrivateBorrow!CiLeaf b, scope ref PrivateContext) nothrow @nogc @system
{
    ciRole = 2; ciSawLeaf = true;
    b.value.c.leafState = &b.value();
    atomicStore!(MemoryOrder.raw)(b.value.c.leafArrived, 1u);
    awaitRaw(b.value.c.leafGo, 1u, "cont leaf gate");
    b.value.c.value = portsValue(7, 13);
    return b.value.c.value;
}
struct CiMid
{
    CiControl* c; PrivateChildPark!(CiLeaf, ulong)* leaves; uint phase;
    ~this() nothrow @nogc @system
    {
        if (c is null) return;
        namedCheck(portsThreadId() != c.emitter, "cont final stored destructor on consumer");
        check(atomicLoad!(MemoryOrder.raw)(c.leafDestroyed) == 1 || c.mode == CiMode.lateHold || c.mode == CiMode.serviceWord);
        namedCheck(atomicFetchAdd!(MemoryOrder.raw)(c.midDestroyed, 1u) == 0, "cont final stored instance once");
    }
}
PrivateChildStep!ulong ciMid(scope ref PrivateBorrow!CiMid b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    ciRole = 1; ++ciMidsInConsume; auto m = &b.value(); auto c = m.c;
    atomicFetchAdd!(MemoryOrder.raw)(c.midCalls, 1u);
    if (m.phase++ == 0)
    {
        c.midState = m;
        version (AntfarmActorTestHooks)
        {
            if (c.mode == CiMode.serviceWord)
            {
                check(privateTestServiceWord(ctx) != 0); // custody occupies the word
                cast(void) privateTestRootService(ctx);
                return typeof(return).done(0);
            }
        }
        if (c.mode == CiMode.lateHold) return typeof(return).waitChildren();
        m.leaves = ctx.createChildren!(CiLeaf, ulong)(1);
        check(m.leaves.spawn(ctx, CiLeaf(c)) == PrivateSpawnResult.spawned);
        version (AntfarmActorTestHooks)
            if (c.mode == CiMode.overflow) privateTestSetChildCredits(ctx, uint.max);
        check(m.leaves.startFinal!ciLeaf(ctx) == PrivateStartResult.started);
        if (c.mode == CiMode.done || c.mode == CiMode.finalDone) return typeof(return).done(portsValue(9, 1));
        return typeof(return).waitChildren();
    }
    if (c.mode == CiMode.lateHold) return typeof(return).done(31);
    auto r = m.leaves.results(ctx);
    namedCheck(r.length == 1 && r[0] == portsValue(7, 13) && c.value == r[0], "cont descendant result visible");
    return typeof(return).done(r[0]);
}
struct CiRoot
{
    CiControl* c; PrivateChildPark!(CiMid, ulong)* mids; uint phase; bool checked;
    ~this() nothrow @nogc @system
    { if (c !is null) atomicStore!(MemoryOrder.raw)(c.rootDestroyed, 1u); }
}
PrivateRootDisposition ciRoot(scope ref PrivateBorrow!CiRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    ciRole = 3; auto r = &b.value(); auto c = r.c;
    if (r.phase++ == 0)
    {
        r.mids = ctx.createChildren!(CiMid, ulong)(1, 1);
        check(r.mids.spawn(ctx, CiMid(c)) == PrivateSpawnResult.spawned);
        auto start = c.mode == CiMode.finalDone ? r.mids.startFinal!ciMid(ctx) : r.mids.start!ciMid(ctx);
        check(start == PrivateStartResult.started);
        return c.seal ? PrivateRootDisposition.waitChildren : PrivateRootDisposition.again;
    }
    if ((c.mode == CiMode.beforeP || c.mode == CiMode.afterP) && atomicLoad!(MemoryOrder.raw)(c.armPolling))
    {
        atomicStore!(MemoryOrder.raw)(c.pollEntered, 1u);
        auto deadline = MonoTime.currTime + 15.seconds;
        while (!r.mids.finished(ctx))
        {
            if (!r.checked && c.mode == CiMode.beforeP && atomicLoad!(MemoryOrder.raw)(c.pPaused))
            {
                check(r.mids.start!ciMid(ctx) == PrivateStartResult.waveActive);
                r.checked = true; atomicStore!(MemoryOrder.raw)(c.activeSeen, 1u);
            }
            namedCheck(MonoTime.currTime < deadline, "cont polling parent join deadline"); pause();
        }
    }
    if (!r.mids.finished(ctx))
    {
        if (!r.checked && ((c.mode == CiMode.hold || c.mode == CiMode.done || c.mode == CiMode.finalDone)
            && atomicLoad!(MemoryOrder.raw)(c.custodyReleased)
            || c.mode == CiMode.lateHold && atomicLoad!(MemoryOrder.raw)(c.midReturned) >= 1))
        {
            namedCheck(r.mids.start!ciMid(ctx) == PrivateStartResult.waveActive, "cont suspended wave rejects start");
            r.checked = true; atomicStore!(MemoryOrder.raw)(c.activeSeen, 1u);
        }
        if (c.mode == CiMode.finishSuspended && atomicLoad!(MemoryOrder.raw)(c.custodyReleased))
            return PrivateRootDisposition.finish;
        return PrivateRootDisposition.again;
    }
    auto values = r.mids.results(ctx); check(values.length == 1);
    c.result = values[0];
    auto expected = c.mode == CiMode.done || c.mode == CiMode.finalDone ? portsValue(9, 1)
        : c.mode == CiMode.lateHold ? 31 : portsValue(7, 13);
    check(c.result == expected);
    check(atomicLoad!(MemoryOrder.raw)(c.midCalls) == (c.mode == CiMode.done || c.mode == CiMode.finalDone ? 1 : 2));
    if (c.mode == CiMode.finalDone) check(r.mids.start!ciMid(ctx) == PrivateStartResult.empty);
    return PrivateRootDisposition.finish;
}
version (AntfarmActorTestHooks)
{
    __gshared CiControl* ciControl;
    void ciHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        auto c = ciControl;
        if (c.mode == CiMode.early && point == ActorTestPoint.privateChildCustodyAfterRelease
            && atomicLoad!(MemoryOrder.raw)(c.leafDestroyed))
            atomicStore!(MemoryOrder.raw)(c.descendantReleased, 1u);
        if (point == ActorTestPoint.privateChildCustodyAfterRelease && ciRole == 1)
        {
            atomicStore!(MemoryOrder.raw)(c.custodyReleased, 1u);
            if (c.mode == CiMode.lateHold && atomicLoad!(MemoryOrder.raw)(c.midCalls) == 1)
            {
                atomicStore!(MemoryOrder.raw)(c.hookArrived, 1u);
                awaitRaw(c.hookGo, 1u, "cont late H seam");
            }
        }
        bool block = c.mode == CiMode.early && point == ActorTestPoint.privateChildCustodyBeforeRelease
            || c.mode == CiMode.late && point == ActorTestPoint.privateRootAfterIdleBeforeCredit;
        if (block && cas!(MemoryOrder.raw, MemoryOrder.raw)(&c.hookArrived, 0u, 1u))
        {
            if (c.mode == CiMode.late)
            {
                namedCheck(!ciEmitter, "cont late descendant finish owned by consumer");
                namedCheck(atomicLoad!(MemoryOrder.raw)(c.custodyReleased) == 1,
                    "cont late activation custody released before descendant finish");
                check(atomicLoad!(MemoryOrder.raw)(c.leafArrived) == 1);
                check(atomicLoad!(MemoryOrder.raw)(c.leafGo) == 1);
            }
            auto deadline = MonoTime.currTime + 10.seconds;
            while (!atomicLoad!(MemoryOrder.raw)(c.hookGo))
            {
                if (MonoTime.currTime >= deadline)
                {
                    fprintf(stderr, "CI stalled mode=%u emitter=%u leafArrived=%u leafGo=%u leafDestroyed=%u leafReturned=%u midCalls=%u custodyReleased=%u hookGo=%u returnCalls=%u midReturned=%u\n",
                        cast(uint)c.mode, ciEmitter, atomicLoad!(MemoryOrder.raw)(c.leafArrived), atomicLoad!(MemoryOrder.raw)(c.leafGo),
                        atomicLoad!(MemoryOrder.raw)(c.leafDestroyed), atomicLoad!(MemoryOrder.raw)(c.leafReturned), atomicLoad!(MemoryOrder.raw)(c.midCalls),
                        atomicLoad!(MemoryOrder.raw)(c.custodyReleased), atomicLoad!(MemoryOrder.raw)(c.hookGo),
                        atomicLoad!(MemoryOrder.raw)(c.returnCalls), atomicLoad!(MemoryOrder.raw)(c.midReturned));
                    namedCheck(false, "cont final owner seam");
                }
                pause();
            }
        }
        if (point == ActorTestPoint.privateChildReturnBeforeCas)
        {
            atomicFetchAdd!(MemoryOrder.raw)(c.returnCalls, 1u);
            if (ciEmitter) atomicFetchAdd!(MemoryOrder.raw)(c.emitterReturns, 1u);
        }
        if ((c.mode == CiMode.beforeP && point == ActorTestPoint.privateResumeBeforeUnschedule)
            || (c.mode == CiMode.afterP && point == ActorTestPoint.privateResumeAfterUnschedule))
        {
            atomicStore!(MemoryOrder.raw)(c.pPaused, 1u);
            awaitRaw(c.midReturned, 2u, "cont P callbacks complete");
            if (c.mode == CiMode.beforeP)
            {
                auto deadline = MonoTime.currTime + 10.seconds;
                while (atomicLoad!(MemoryOrder.raw)(c.activeSeen) == 0)
                { pause(); namedCheck(MonoTime.currTime < deadline, "cont P pins wave before unschedule"); }
                check(atomicLoad!(MemoryOrder.raw)(c.rootDestroyed) == 0);
            }
            else awaitRaw(c.rootDestroyed, 1u, "cont park disposed after P release");
        }
    }
}
// C3/C4/C5/C6/C7/C9. Mutations C-M3/C-M4 lose final-owner dispatch;
// C-M5a/b join early; C-M6 leaks H; C-M7 retires on the final owner;
// C-M7b re-runs the op; C-M9a/b violate P lifetime at these seams.
void contInterleave(PrivateRootReclamation reclaim)(CiMode mode)
{
    version (AntfarmActorTestHooks)
    {
        Counts counts; CiControl c; c.mode = mode; c.emitter = portsThreadId();
        auto farm = AntFarm.create(); scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
        CiAlloc allocation = CiAlloc(&counts, &c);
        auto park = PrivateRootPark!(CiRoot, ciRoot, reclaim).create(farm, 1, 1, ciPolicy(allocation));
        check(park.spawn(CiRoot(&c)) == PrivateSpawnResult.spawned);
        ciControl = &c; setActorTestHook(&ciHook);
        scope(exit) { setActorTestHook(null); ciControl = null; }
        shared uint stop;
        Thread[] workers;
        foreach (_; 0 .. 5)
        {
            auto worker = new Thread({
                ConsumerView view; check(view.subscribe(farm) >= 0);
                scope(exit) view.unsubscribe();
                while (atomicLoad!(MemoryOrder.raw)(stop) == 0)
                {
                    ciRole = 0; ciSawLeaf = false; ciMidsInConsume = 0;
                    auto consumed = view.consumeNext(); // idle sweeps can run callbacks even when false
                    if (ciSawLeaf) atomicStore!(MemoryOrder.raw)(c.leafReturned, 1u);
                    if (ciMidsInConsume) atomicFetchAdd!(MemoryOrder.raw)(c.midReturned, ciMidsInConsume);
                    if (!consumed) pause();
                }
            });
            workers ~= worker; worker.start();
        }
        scope(exit) { atomicStore!(MemoryOrder.raw)(stop, 1u); foreach (worker; workers) worker.join(); }
        ciEmitter = true; scope(exit) ciEmitter = false;
        auto deadline = MonoTime.currTime + 15.seconds;
        bool opened, unblocked;
        while (park.live)
        {
            park.pump(token);
            if (mode == CiMode.early)
            {
                if (atomicLoad!(MemoryOrder.raw)(c.hookArrived))
                {
                    atomicStore!(MemoryOrder.raw)(c.leafGo, 1u);
                    if (atomicLoad!(MemoryOrder.raw)(c.descendantReleased) && !unblocked)
                    {
                        // The no-access finish seam follows the notification decrement.
                        // returnCalls remains zero while activation custody is held.
                        check(atomicLoad!(MemoryOrder.raw)(c.returnCalls) == 0);
                        atomicStore!(MemoryOrder.raw)(c.hookGo, 1u); unblocked = true;
                    }
                }
            }
            else if (mode == CiMode.lateHold)
            {
                // A user-call entry count is too early: the resumer must have
                // dropped H and returned its table before the old epilogue runs.
                if (atomicLoad!(MemoryOrder.raw)(c.midReturned) >= 1 && !unblocked)
                {
                    namedCheck(atomicLoad!(MemoryOrder.raw)(c.rootDestroyed) == 0,
                        "cont old activation table still pins cohort after resume");
                    if (atomicLoad!(MemoryOrder.raw)(c.activeSeen))
                    { atomicStore!(MemoryOrder.raw)(c.hookGo, 1u); unblocked = true; }
                }
            }
            else
            {
                // C4 must hold a consumer-owned descendant finish. Observing
                // activation custody alone can open leafGo before the leaf
                // table is published, letting emitter seal own finish and
                // block this pump on a hook only the emitter can release.
                // leafArrived observed AFTER pump returns proves publication
                // and producer seal completed while the leaf gate stayed shut.
                if (mode != CiMode.finishSuspended && atomicLoad!(MemoryOrder.raw)(c.custodyReleased) && !opened
                    && (mode != CiMode.late || atomicLoad!(MemoryOrder.raw)(c.leafArrived)))
                {
                    if (mode == CiMode.beforeP || mode == CiMode.afterP)
                    {
                        if (atomicLoad!(MemoryOrder.raw)(c.midReturned) == 0) continue;
                        atomicStore!(MemoryOrder.raw)(c.armPolling, 1u);
                        if (atomicLoad!(MemoryOrder.raw)(c.pollEntered) == 0) continue;
                    }
                    if (mode == CiMode.hold || mode == CiMode.done || mode == CiMode.finalDone)
                    {
                        if (atomicLoad!(MemoryOrder.raw)(c.activeSeen) == 0) continue;
                        check(atomicLoad!(MemoryOrder.raw)(c.midCalls) == 1);
                    }
                    atomicStore!(MemoryOrder.raw)(c.leafGo, 1u); opened = true;
                }
                if (mode == CiMode.late && atomicLoad!(MemoryOrder.raw)(c.hookArrived) && !unblocked)
                {
                    check(atomicLoad!(MemoryOrder.raw)(c.midCalls) == 1);
                    atomicStore!(MemoryOrder.raw)(c.hookGo, 1u); unblocked = true;
                }
            }
            namedCheck(MonoTime.currTime < deadline, "cont interleave completion deadline");
        }
        if (mode == CiMode.late)
            namedCheck(opened && unblocked && atomicLoad!(MemoryOrder.raw)(c.hookArrived) == 1,
                "cont late descendant notification held and released");
        park.destroy();
        check(atomicLoad!(MemoryOrder.raw)(c.midDestroyed) == 1 && atomicLoad!(MemoryOrder.raw)(c.midFrees) == 1);
        check(atomicLoad!(MemoryOrder.raw)(c.leafFrees) == (mode == CiMode.lateHold ? 0 : 1));
        check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
}

struct CrControl
{
    uint* calls; uint* waves; ulong* writes;
    size_t count = 8, limit = 64;
    uint rounds = 6, completed;
    bool subset, admission, casTest, fifoRace;
    shared uint firstAtCas, firstGo, secondGo, firstReturned, secondReturned, casCalls, firstCalls, secondCalls, secondStarted;
}
uint crIndex; bool crMember, crSawFirst, crSawSecond;
struct CrMember { CrControl* c; uint index, phase, wave; }
PrivateChildStep!ulong crResult(scope ref PrivateBorrow!CrMember b, scope ref PrivateContext)
    nothrow @nogc @system
{
    auto m = &b.value(); auto c = m.c; crMember = true; crIndex = m.index;
    if (m.index == 0) { crSawFirst = true; atomicFetchAdd!(MemoryOrder.raw)(c.firstCalls, 1u); }
    if (m.index == 1) { crSawSecond = true; atomicFetchAdd!(MemoryOrder.raw)(c.secondCalls, 1u); }
    ++c.calls[m.index];
    if ((c.casTest || c.fifoRace) && m.index == 1 && m.phase == 0)
    {
        atomicStore!(MemoryOrder.raw)(c.secondStarted, 1u);
        awaitRaw(c.secondGo, 1u, "cont second member gate");
    }
    // Admission tests suspend every member twice; other waves mix immediate done.
    if ((c.admission || c.casTest || m.index % 2 == 0) && m.phase++ < 2)
    {
        c.writes[m.index] = portsValue(m.wave + 1, m.index);
        return typeof(return).waitChildren();
    }
    if (c.admission || c.casTest || m.index % 2 == 0)
        check(c.writes[m.index] == portsValue(m.wave + 1, m.index));
    m.phase = 0; c.waves[m.index] = ++m.wave;
    return typeof(return).done(portsValue(m.wave, m.index));
}
PrivateChildDisposition crVoid(scope ref PrivateBorrow!CrMember b, scope ref PrivateContext)
    nothrow @nogc @system
{
    auto m = &b.value(); ++m.c.calls[m.index];
    if (m.phase++ < 2) return PrivateChildDisposition.waitChildren;
    m.phase = 0; m.c.waves[m.index] = ++m.wave;
    return PrivateChildDisposition.done;
}
ulong crPlainResult(scope ref PrivateBorrow!CrMember b, scope ref PrivateContext)
    nothrow @nogc @system
{ ++b.value.c.calls[b.value.index]; b.value.c.waves[b.value.index] = ++b.value.wave; return portsValue(b.value.wave, b.value.index); }
void crPlain(scope ref PrivateBorrow!CrMember b, scope ref PrivateContext)
    nothrow @nogc @system
{ ++b.value.c.calls[b.value.index]; b.value.c.waves[b.value.index] = ++b.value.wave; }
struct CrRoot { CrControl* c; PrivateChildPark!(CrMember, ulong)* members; uint phase; }
PrivateRootDisposition crRoot(scope ref PrivateBorrow!CrRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto r = &b.value(); auto c = r.c;
    if (r.phase)
    {
        auto values = r.members.results(ctx);
        bool resultWave = (r.phase - 1) % 4 < 2;
        uint[4] selected = [7, 0, 5, 2];
        auto members = r.members.subsetMembers(ctx);
        if (c.subset) check(members == selected[]);
        else check(members.length == 0);
        namedCheck(values.length == (resultWave ? c.subset ? 4 : c.count : 0), "cont void operation clears results");
        foreach (i, value; values)
        {
            auto index = c.subset ? selected[i] : i;
            namedCheck(value == portsValue(c.waves[index], index), "cont participant ordered final result tag");
        }
        ++c.completed;
        if (r.phase == c.rounds) return PrivateRootDisposition.finish;
    }
    else
    {
        r.members = ctx.createChildren!(CrMember, ulong)(c.count, PrivateCohortOptions(c.limit, c.subset));
        check(r.members !is null);
        foreach (i; 0 .. c.count) check(r.members.spawn(ctx, CrMember(c, cast(uint) i)) == PrivateSpawnResult.spawned);
    }
    uint[4] selected = [7, 0, 5, 2];
    PrivateStartResult start;
    final switch (r.phase++ % 4)
    {
        case 0: start = c.subset ? r.members.startSubset!crResult(ctx, selected[]) : r.members.start!crResult(ctx); break;
        case 1: start = c.subset ? r.members.startSubset!crPlainResult(ctx, selected[]) : r.members.start!crPlainResult(ctx); break;
        case 2: start = c.subset ? r.members.startSubset!crVoid(ctx, selected[]) : r.members.start!crVoid(ctx); break;
        case 3: start = c.subset ? r.members.startSubset!crPlain(ctx, selected[]) : r.members.start!crPlain(ctx); break;
    }
    check(start == PrivateStartResult.started);
    return PrivateRootDisposition.waitChildren;
}
// CT: all four operation shapes, both cohort kinds, and ambiguous results.
static assert(__traits(compiles, (PrivateChildPark!(CrMember, ulong)* p, ref PrivateContext c) { p.start!crResult(c); p.startFinal!crResult(c); p.start!crVoid(c); p.start!crPlainResult(c); p.start!crPlain(c); }));
static assert(__traits(compiles, (PrivateChildPark!CrMember* p, ref PrivateContext c) { p.start!crVoid(c); p.start!crPlain(c); }));
static assert(!__traits(compiles, (PrivateChildPark!CrMember* p, ref PrivateContext c) { p.start!crResult(c); }));
static assert(!__traits(compiles, (PrivateChildPark!(CrMember, uint)* p, ref PrivateContext c) { p.start!crResult(c); }));
static assert(!__traits(compiles, PrivateChildPark!(CrMember, PrivateChildDisposition).init));
static assert(!__traits(compiles, PrivateChildPark!(CrMember, PrivateChildStep!ulong).init));
static assert(!__traits(compiles, PrivateChildStep!void.init));
static assert(!__traits(compiles, PrivateChildStep!(PrivateChildStep!int).init));
static assert(!__traits(compiles, PrivateChildStep!PrivateChildDisposition.init));
// C10/C11: C-M10 writes at suspension; C-M11 omits activation custody reset.
void contResults(PrivateRootReclamation reclaim)(bool subset)
{
    Counts counts; uint[8] calls, waves; ulong[8] writes;
    CrControl c = CrControl(calls.ptr, waves.ptr, writes.ptr); c.subset = subset;
    auto farm = AntFarm.create(); scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 5); scope(exit) workers.join();
    auto park = PrivateRootPark!(CrRoot, crRoot, reclaim).create(farm, 1, 64, policy(counts));
    check(park.spawn(CrRoot(&c)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 15.seconds;
    while (park.live) { park.pump(token); check(MonoTime.currTime < deadline); }
    foreach (i; 0 .. 8)
    {
        bool selected = !subset || i == 7 || i == 0 || i == 5 || i == 2;
        check(waves[i] == (selected ? 6 : 0));
        check(calls[i] == (selected ? i % 2 == 0 ? 12 : 8 : 0));
    }
    park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}
version (AntfarmWriteAuditHooks)
{
    __gshared uint* contPublications;
    void contPublicationHook(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    { if (phase == WriteAuditPhase.tablePublished) ++*contPublications; }
}
// C8/C14: C-M8 reads published next links, C-M8b leaks rejected table
// credits, C-M13 retains the empty tail, C-M14 aliases the ownership cursor.
void contAdmission(PrivateRootReclamation reclaim)(size_t count, size_t limit)
{
    Counts counts;
    auto calls = new uint[count]; auto waves = new uint[count]; auto writes = new ulong[count];
    CrControl c = CrControl(calls.ptr, waves.ptr, writes.ptr, count, limit, 1); c.admission = true;
    // A fresh compact ST quota (SPEC 2f, one-word bodies: 38 + 3*n words)
    // admits 54 payloads of a requested 64 at 200 words; 225 of a requested
    // 256 at 713 words. Limit 1 necessarily admits zero or one.
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, limit == 64 ? 200 : limit == 256 ? 713 : 4096); scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
    ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(CrRoot, crRoot, reclaim).create(farm, 1, 256, policy(counts));
    check(park.spawn(CrRoot(&c)) == PrivateSpawnResult.spawned);
    park.pump(token); while (view.consumeNext()) {}
    // Publish the initial wave without consumers running: no return can be
    // drained yet. The large ring holds even 1025 one-member tables.
    auto deadline = MonoTime.currTime + 30.seconds;
    foreach (_; 0 .. count + 2) park.pump(token);
    while (view.consumeNext()) {}
    foreach (n; calls) check(n == 1);
    enum fillCount = 100000;
    auto fillerResults = new ulong[fillCount];
    auto filler = Park.create(farm, fillCount, limit == 64 ? 64 : 256);
    foreach (i; 0 .. fillCount) check(filler.spawn(State(fillerResults.ptr + i, 1)) == PrivateSpawnResult.spawned);
    size_t admitted;
    while (true) { auto n = filler.pump(token); if (!n) break; admitted += n; check(MonoTime.currTime < deadline); }
    check(admitted > 0 && admitted < fillCount);
    auto before = calls.dup;
    version (AntfarmWriteAuditHooks)
    {
        uint publications;
        contPublications = &publications; writeAuditHook = &contPublicationHook;
        park.pump(token); // full ring: no resume table may be published
        writeAuditHook = null; contPublications = null;
        namedCheck(publications == 0, "cont full Farm gives zero resume admission");
    }
    else park.pump(token);
    check(calls == before);
    while (filler.live) { while (view.consumeNext()) {} filler.pump(token); check(MonoTime.currTime < deadline); }
    filler.destroy();
    park.pump(token); while (view.consumeNext()) {}
    if (limit > 1)
    {
        size_t changed; foreach (i; 0 .. count) changed += calls[i] != before[i];
        namedCheck(changed > 0 && changed < limit, "cont partial resume admission accepted prefix");
    }
    while (park.live) { park.pump(token); while (view.consumeNext()) {} namedCheck(MonoTime.currTime < deadline, "cont admission retry deadline"); }
    foreach (i; 0 .. count) check(calls[i] == 3 && waves[i] == 1);
    check(c.completed == 1);
    park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}
version (AntfarmActorTestHooks)
{
    __gshared CrControl* crControl;
    void crCasHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        if (point != ActorTestPoint.privateChildReturnBeforeCas) return;
        auto c = crControl;
        atomicFetchAdd!(MemoryOrder.raw)(c.casCalls, 1u);
        if (crIndex == 0 && cas!(MemoryOrder.raw, MemoryOrder.raw)(&c.firstAtCas, 0u, 1u))
            awaitRaw(c.firstGo, 1u, "cont first CAS release");
    }
}
// C13: C-M12 omits head reload after a forced failed CAS.
void contCasRetry(PrivateRootReclamation reclaim)()
{
    version (AntfarmActorTestHooks)
    {
        Counts counts; uint[2] calls, waves; ulong[2] writes;
        CrControl c = CrControl(calls.ptr, waves.ptr, writes.ptr, 2, 1, 1); c.casTest = true;
        auto farm = AntFarm.create(); scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
        auto park = PrivateRootPark!(CrRoot, crRoot, reclaim).create(farm, 1, 64, policy(counts));
        check(park.spawn(CrRoot(&c)) == PrivateSpawnResult.spawned);
        crControl = &c; setActorTestHook(&crCasHook);
        scope(exit) { setActorTestHook(null); crControl = null; }
        shared uint stop; Thread[] threads;
        foreach (_; 0 .. 2)
        {
            auto t = new Thread({
                ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
                while (!atomicLoad!(MemoryOrder.raw)(stop))
                {
                    crMember = false; crSawFirst = false; crSawSecond = false;
                    auto consumed = view.consumeNext();
                    if (crSawFirst) atomicStore!(MemoryOrder.raw)(c.firstReturned, 1u);
                    if (crSawSecond) atomicStore!(MemoryOrder.raw)(c.secondReturned, 1u);
                    if (!consumed) pause();
                }
            }); threads ~= t; t.start();
        }
        scope(exit) { atomicStore!(MemoryOrder.raw)(stop, 1u); foreach (t; threads) t.join(); }
        auto deadline = MonoTime.currTime + 15.seconds;
        while (!atomicLoad!(MemoryOrder.raw)(c.firstAtCas)) { park.pump(token); check(MonoTime.currTime < deadline); }
        // A can reach its CAS before B's table is published; keep emitting until B starts.
        while (!atomicLoad!(MemoryOrder.raw)(c.secondStarted)) { park.pump(token); check(MonoTime.currTime < deadline); }
        atomicStore!(MemoryOrder.raw)(c.secondGo, 1u);
        awaitRaw(c.secondReturned, 1u, "cont competing CAS published");
        atomicStore!(MemoryOrder.raw)(c.firstGo, 1u);
        // Keep the competing head in place until the first producer returns.
        // Merely observing another CAS attempt would let the emitter clear the
        // head, making the stale expected-null value valid again (C-M12).
        awaitRaw(c.firstReturned, 1u, "cont first CAS retry completed before drain");
        check(atomicLoad!(MemoryOrder.raw)(c.casCalls) >= 3);
        while (park.live) { park.pump(token); check(MonoTime.currTime < deadline); }
        check(calls[0] == 3 && calls[1] == 3);
        park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
}

version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
{
    struct CsControl { CiControl* c; shared uint work, rootReturned, stop, ready; uint tables, allPublications; bool armed; }
    __gshared CsControl* csControl;
    void csPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto s = csControl;
        ++s.allPublications;
        if (!s.armed || ++s.tables != 1) return;
        atomicStore!(MemoryOrder.raw)(s.work, 3u);
        awaitRaw(s.c.leafReturned, 1u, "cont seal callback returned before emitter seal");
        check(atomicLoad!(MemoryOrder.raw)(s.c.midDestroyed) == 0);
        check(atomicLoad!(MemoryOrder.raw)(s.c.returnCalls) == 0);
    }
    // C7 deferred terminal cleanup: C-M7 runs ~T on emitter; C-M7b calls op.
    void contSealFinalOwner(PrivateRootReclamation reclaim)()
    {
        Counts counts; CiControl c; c.mode = CiMode.finalDone; c.emitter = portsThreadId(); c.seal = true;
        CsControl s; s.c = &c;
        auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 4096); scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
        ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
        CiAlloc allocation = CiAlloc(&counts, &c);
        auto park = PrivateRootPark!(CiRoot, ciRoot, reclaim).create(farm, 1, 1, ciPolicy(allocation));
        check(park.spawn(CiRoot(&c)) == PrivateSpawnResult.spawned);
        ciControl = &c; setActorTestHook(&ciHook); ciEmitter = true;
        scope(exit) { setActorTestHook(null); ciControl = null; ciEmitter = false; }
        csControl = &s; writeAuditHook = &csPublished; // installed before subscribe reads it
        auto worker = new Thread({
            ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
            atomicStore!(MemoryOrder.raw)(s.ready, 1u);
            awaitRaw(s.work, 1u, "cont seal root go");
            do view.consumeNext(); while (ciRole != 3);
            atomicStore!(MemoryOrder.raw)(s.rootReturned, 1u);
            awaitRaw(s.work, 2u, "cont seal member go");
            do view.consumeNext(); while (ciRole != 1);
            atomicStore!(MemoryOrder.raw)(c.midReturned, 1u);
            auto deadline = MonoTime.currTime + 20.seconds;
            while (atomicLoad!(MemoryOrder.raw)(s.work) != 3)
            {
                if (atomicLoad!(MemoryOrder.raw)(s.work) == 4) { ciRole = 0; view.consumeNext(); }
                else pause();
                namedCheck(MonoTime.currTime < deadline, "cont seal leaf go");
            }
            do view.consumeNext(); while (ciRole != 2);
            atomicStore!(MemoryOrder.raw)(c.leafReturned, 1u);
            while (!atomicLoad!(MemoryOrder.raw)(s.stop)) { ciRole = 0; if (!view.consumeNext()) pause(); }
        }); worker.start(); awaitRaw(s.ready, 1u, "cont seal worker subscribed");
        scope(exit) { atomicStore!(MemoryOrder.raw)(s.stop, 1u); worker.join(); writeAuditHook = null; csControl = null; }
        park.pump(token); atomicStore!(MemoryOrder.raw)(s.work, 1u);
        awaitRaw(s.rootReturned, 1u, "cont seal root started children");
        park.pump(token); atomicStore!(MemoryOrder.raw)(s.work, 2u);
        awaitRaw(c.midReturned, 1u, "cont seal done with outstanding tail");
        // No descendant table has been emitted yet. Force its first attempt to
        // zero, then drain only filler while the actor consumer remains parked.
        enum fillCount = 100000; auto results = new ulong[fillCount];
        auto filler = Park.create(farm, fillCount, 256);
        foreach (i; 0 .. fillCount) check(filler.spawn(State(results.ptr + i, 1)) == PrivateSpawnResult.spawned);
        while (filler.pump(token)) {}
        auto before = s.allPublications;
        park.pump(token);
        namedCheck(s.allPublications == before, "cont seal descendant zero admission");
        check(atomicLoad!(MemoryOrder.raw)(c.leafArrived) == 0);
        atomicStore!(MemoryOrder.raw)(s.work, 4u); // advance the parked view through filler
        while (filler.live) { while (view.consumeNext()) {} filler.pump(token); }
        filler.destroy();
        atomicStore!(MemoryOrder.raw)(c.leafGo, 1u);
        s.armed = true;
        park.pump(token);
        namedCheck(atomicLoad!(MemoryOrder.raw)(c.emitterReturns) == 1, "cont emitter seal owns terminal child return");
        auto deadline = MonoTime.currTime + 15.seconds;
        while (park.live) { park.pump(token); check(MonoTime.currTime < deadline); }
        check(atomicLoad!(MemoryOrder.raw)(c.midCalls) == 1);
        check(atomicLoad!(MemoryOrder.raw)(c.midDestroyed) == 1);
        check(atomicLoad!(MemoryOrder.raw)(c.midFrees) == 1 && atomicLoad!(MemoryOrder.raw)(c.leafFrees) == 1);
        park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
}
void contDeath()
{
    version (linux) version (AntfarmActorTestHooks)
    {
        check(diedWithMessage("cont-counter-exhaustion", "antfarm fatal: private join credit overflow"));
        check(diedWithMessage("cont-finish-suspended", "antfarm fatal: retire private actor with active child wave"));
        version (assert)
            check(diedWithMessage("cont-child-service-word", "antfarm fatal: private entry: service read on a child"));
        writeln("PASS cont counter exhaustion, suspended finish, and nonzero child service-word deaths");
    }
}

version (AntfarmWriteAuditHooks)
{
    struct CfControl { CrControl* c; shared uint ready, go, returned, stop; uint publications; }
    __gshared CfControl* cfControl;
    void cfPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto c = cfControl;
        if (++c.publications != 1) return;
        atomicStore!(MemoryOrder.raw)(c.go, 1u);
        awaitRaw(c.returned, 1u, "cont FIFO accepted prefix returned before writeTracked returns");
        if (c.c.limit == 1)
            namedCheck(atomicLoad!(MemoryOrder.raw)(c.c.firstCalls) == 2 && atomicLoad!(MemoryOrder.raw)(c.c.secondCalls) == 1,
                "cont partial FIFO accepts A and retains B");
    }
    // C14: independent cursor and accepted-prefix detachment. Run full drain
    // (two members) and partial drain ([A,B,C] accepts [A,B]); accepted entries
    // republish while the emitter is still inside writeTracked. C-M8 races
    // next links; C-M13 fails empty-tail invariant; C-M14 skips accepted A.
    void contFifo(PrivateRootReclamation reclaim)(size_t count, size_t limit = 2)
    {
        Counts counts; uint[3] calls, waves; ulong[3] writes;
        CrControl c = CrControl(calls.ptr, waves.ptr, writes.ptr, count, limit, 1); c.admission = true;
        CfControl f; f.c = &c;
        auto farm = AntFarm.create(); scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
        ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(CrRoot, crRoot, reclaim).create(farm, 1, 64, policy(counts));
        check(park.spawn(CrRoot(&c)) == PrivateSpawnResult.spawned);
        park.pump(token); while (view.consumeNext()) {}
        foreach (_; 0 .. 4) park.pump(token);
        while (view.consumeNext()) {}
        foreach (i; 0 .. count) check(calls[i] == 1);
        cfControl = &f; writeAuditHook = &cfPublished; // immutable while worker subscribes
        auto worker = new Thread({
            ConsumerView other; check(other.subscribe(farm) >= 0); scope(exit) other.unsubscribe();
            atomicStore!(MemoryOrder.raw)(f.ready, 1u);
            awaitRaw(f.go, 1u, "cont FIFO publication go");
            while (!atomicLoad!(MemoryOrder.raw)(f.stop))
            {
                auto consumed = other.consumeNext();
                if (atomicLoad!(MemoryOrder.raw)(c.firstCalls) >= 2
                    && (limit == 1 || atomicLoad!(MemoryOrder.raw)(c.secondCalls) >= 2))
                    atomicStore!(MemoryOrder.raw)(f.returned, 1u);
                if (!consumed) pause();
            }
        }); worker.start(); awaitRaw(f.ready, 1u, "cont FIFO worker ready");
        scope(exit) { atomicStore!(MemoryOrder.raw)(f.stop, 1u); worker.join(); writeAuditHook = null; cfControl = null; }
        park.pump(token);
        auto deadline = MonoTime.currTime + 15.seconds;
        while (park.live) { park.pump(token); check(MonoTime.currTime < deadline); }
        foreach (i; 0 .. count) check(calls[i] == 3 && waves[i] == 1);
        park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
}

version (AntfarmActorTestHooks) version (AntfarmWriteAuditHooks)
{
    struct CfrControl { CrControl* c; shared uint ready, go, stop; bool armed; uint publications; }
    __gshared CfrControl* cfrControl;
    void cfrHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        auto c = cfrControl.c;
        if (point == ActorTestPoint.privateChildCustodyAfterRelease)
        {
            // These no-access seams prove the respective CAS has published.
            if (crIndex == 0 && atomicLoad!(MemoryOrder.raw)(c.firstCalls) == 1)
                atomicStore!(MemoryOrder.raw)(c.firstReturned, 1u);
            if (crIndex == 1 && atomicLoad!(MemoryOrder.raw)(c.secondCalls) == 1)
                atomicStore!(MemoryOrder.raw)(c.secondReturned, 1u);
        }
        if (point == ActorTestPoint.privateChildReturnBeforeCas && crIndex == 0
            && atomicLoad!(MemoryOrder.raw)(c.firstCalls) == 2
            && cas!(MemoryOrder.raw, MemoryOrder.raw)(&c.firstAtCas, 0u, 1u))
            awaitRaw(c.firstGo, 1u, "cont FIFO A republish release");
    }
    void cfrPublished(AntFarm*, WriteAuditPhase phase, ulong, ulong) nothrow @nogc @system
    {
        if (phase != WriteAuditPhase.tablePublished) return;
        auto f = cfrControl; if (!f.armed || ++f.publications != 1) return;
        // A's entire FIFO was detached before publication. A now writes its
        // return link, while B's first activation has not yet returned.
        awaitRaw(f.c.firstAtCas, 1u, "cont full FIFO A republishing");
        atomicStore!(MemoryOrder.raw)(f.c.secondGo, 1u);
        awaitRaw(f.c.secondReturned, 1u, "cont full FIFO later B return published");
    }
    // C14's full-drain/reuse schedule: FIFO [A] is fully drained; A pauses
    // while republishing; B returns and the next pump appends it to the empty
    // FIFO before A's return CAS completes. C-M13 catches the stale empty tail,
    // and C-M8 catches reading A.next after that first resume publication.
    void contFifoOverlap(PrivateRootReclamation reclaim)()
    {
        Counts counts; uint[2] calls, waves; ulong[2] writes;
        CrControl c = CrControl(calls.ptr, waves.ptr, writes.ptr, 2, 1, 1); c.admission = true; c.fifoRace = true;
        CfrControl f; f.c = &c;
        auto farm = AntFarm.create(); scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small); scope(exit) farm.unregisterProducer(token);
        ConsumerView view; check(view.subscribe(farm) >= 0); scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(CrRoot, crRoot, reclaim).create(farm, 1, 64, policy(counts));
        cfrControl = &f; setActorTestHook(&cfrHook); writeAuditHook = &cfrPublished;
        Thread[] workers;
        foreach (_; 0 .. 2)
        {
            auto t = new Thread({
                ConsumerView other; check(other.subscribe(farm) >= 0); scope(exit) other.unsubscribe();
                atomicFetchAdd!(MemoryOrder.raw)(f.ready, 1u);
                awaitRaw(f.go, 1u, "cont FIFO overlap initial go");
                while (!atomicLoad!(MemoryOrder.raw)(f.stop)) if (!other.consumeNext()) pause();
            }); workers ~= t; t.start();
        }
        scope(exit)
        {
            atomicStore!(MemoryOrder.raw)(f.stop, 1u); foreach (t; workers) t.join();
            setActorTestHook(null); writeAuditHook = null; cfrControl = null;
        }
        awaitRaw(f.ready, 2u, "cont FIFO overlap consumers ready");
        check(park.spawn(CrRoot(&c)) == PrivateSpawnResult.spawned);
        park.pump(token); while (view.consumeNext()) {}
        park.pump(token); park.pump(token); // both initial one-member tables
        atomicStore!(MemoryOrder.raw)(f.go, 1u);
        awaitRaw(c.firstReturned, 1u, "cont FIFO only A returned initially");
        awaitRaw(c.secondStarted, 1u, "cont FIFO B activation held");
        check(atomicLoad!(MemoryOrder.raw)(c.secondReturned) == 0);
        f.armed = true;
        park.pump(token); // drains [A]; audit releases B while A's CAS is held
        check(atomicLoad!(MemoryOrder.raw)(c.firstGo) == 0);
        park.pump(token); // append B to the empty FIFO, then publish B
        atomicStore!(MemoryOrder.raw)(c.firstGo, 1u);
        auto deadline = MonoTime.currTime + 15.seconds;
        while (park.live) { park.pump(token); check(MonoTime.currTime < deadline); }
        check(calls[0] == 3 && calls[1] == 3 && waves[0] == 1 && waves[1] == 1);
        park.destroy(); check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
}

void phaseCont()
{
    contTree!(PrivateRootReclamation.emitter)();
    contTree!(PrivateRootReclamation.consumer)();
    contYield!(PrivateRootReclamation.emitter)();
    contYield!(PrivateRootReclamation.consumer)();
    foreach (mode; [CiMode.early, CiMode.late, CiMode.hold, CiMode.lateHold, CiMode.done, CiMode.finalDone, CiMode.beforeP, CiMode.afterP])
    { contInterleave!(PrivateRootReclamation.emitter)(mode); contInterleave!(PrivateRootReclamation.consumer)(mode); }
    foreach (subset; [false, true]) { contResults!(PrivateRootReclamation.emitter)(subset); contResults!(PrivateRootReclamation.consumer)(subset); }
    contTree!(PrivateRootReclamation.emitter)(64, 1); contTree!(PrivateRootReclamation.consumer)(64, 1);
    foreach (count; [513, 1025]) foreach (limit; [1, 64, 256])
    { contAdmission!(PrivateRootReclamation.emitter)(count, limit); contAdmission!(PrivateRootReclamation.consumer)(count, limit); }
    version (AntfarmWriteAuditHooks) foreach (n; [2, 3])
    { contFifo!(PrivateRootReclamation.emitter)(n); contFifo!(PrivateRootReclamation.consumer)(n); }
    version (AntfarmWriteAuditHooks)
    { contFifo!(PrivateRootReclamation.emitter)(2, 1); contFifo!(PrivateRootReclamation.consumer)(2, 1); }
    version (AntfarmActorTestHooks) version (AntfarmWriteAuditHooks)
    { contFifoOverlap!(PrivateRootReclamation.emitter)(); contFifoOverlap!(PrivateRootReclamation.consumer)(); }
    contCasRetry!(PrivateRootReclamation.emitter)(); contCasRetry!(PrivateRootReclamation.consumer)();
    version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
    { contSealFinalOwner!(PrivateRootReclamation.emitter)(); contSealFinalOwner!(PrivateRootReclamation.consumer)(); }
    writeln("PASS cont core, deterministic handoffs, results, admission, CAS and depth64 both policies");
}

// ---- F1 outcomes (design §12.1) ------------------------------------------------
// O1 applied mutation: move the winner's code and Entry stores out of
// `if (old == 0)` and leave privateFailPairSplit inside that branch.
// Member 5 overwrites the code during the pause; the winner still stores
// its Entry. Observed failure: FAIL main.d:7201
// (`app.code - 100 == app.position`). Dropping the gate around the pause
// too would hang member 5 in the hook and time out at "o1 second report".
// O2 reset removed from start: wave 2 still reports wave 1's failures.
// O3 position taken from the spawn index instead of sel_[capacity + index].
// O4 outcome() while active is fatal (death); a continuation counts one report.
// O5 plain increment of failures: TSan race and/or a short count.
// O6 behavioural propagation leaf -> mid -> root -> application: the root's
// fail on a tracked park ends in takeOutcome (untracked root fail is O7).
// O7 missing untracked-park check: root fail returns.
// O8 wrapping fetchAdd: the second report near uint.max is not fatal.
// O9 resets before empty/active rejection: last nonempty outcome/results change.
// The plain-start sibling catches resetOutcome(count_) placed above start's
// empty check: startFinal has already zeroed count_, so that reset clears the
// joined failure snapshot before returning empty.

struct OcMember { uint index; }
shared uint ocAllowSecond, ocSecondDone;
void ocFail(scope ref PrivateBorrow!OcMember b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.index == 5)
        while (atomicLoad!(MemoryOrder.acq)(ocAllowSecond) == 0) {}
    if (b.value.index == 2 || b.value.index == 5) c.fail(100 + b.value.index);
    if (b.value.index == 5) atomicStore!(MemoryOrder.rel)(ocSecondDone, 1u);
}
struct OcApp
{
    uint failures, code;
    size_t position, participants, dispatched;
    bool cancelled, succeeded, saw;
}
struct OcRoot { PrivateChildPark!OcMember* kids; OcApp* app; }
PrivateRootDisposition ocRoot(scope ref PrivateBorrow!OcRoot b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!OcMember(8, 1);
        namedCheck(b.value.kids !is null, "o1 cohort");
        foreach (i; 0 .. 8)
            namedCheck(b.value.kids.spawn(c, OcMember(cast(uint) i)) == PrivateSpawnResult.spawned, "o1 spawn");
        namedCheck(b.value.kids.start!ocFail(c) == PrivateStartResult.started, "o1 start");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.kids.outcome(c);
    b.value.app.failures = o.failures;
    b.value.app.code = o.firstFailureCode;
    b.value.app.position = o.firstFailurePosition;
    b.value.app.participants = o.participants;
    b.value.app.dispatched = o.dispatched;
    b.value.app.cancelled = o.cancelled;
    b.value.app.succeeded = o.succeeded;
    b.value.app.saw = true;
    return PrivateRootDisposition.finish;
}
void outcomeO1()
{
    OcApp app;
    atomicStore!(MemoryOrder.raw)(ocAllowSecond, 0u);
    atomicStore!(MemoryOrder.raw)(ocSecondDone, 0u);
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 2);
    scope(exit) workers.join();
    privateTestArmFailPairPause();
    scope(exit)
    {
        privateTestReleaseFailPairPause();
        atomicStore!(MemoryOrder.rel)(ocAllowSecond, 1u);
    }
    auto park = PrivateRootPark!(OcRoot, ocRoot).create(farm, 1, 1);
    check(park !is null && park.spawn(OcRoot(null, &app)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 15.seconds;
    while (!privateTestFailPairWaiting())
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "o1 winner pause");
    }
    atomicStore!(MemoryOrder.rel)(ocAllowSecond, 1u);
    while (atomicLoad!(MemoryOrder.acq)(ocSecondDone) == 0)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "o1 second report");
    }
    privateTestReleaseFailPairPause();
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "o1 drain");
    }
    park.destroy();
    check(app.saw && app.failures == 2 && app.code - 100 == app.position);
    check(app.participants == 8 && app.dispatched == 8 && !app.cancelled && !app.succeeded);
}

struct O2Member { uint wave; }
void o2Op(scope ref PrivateBorrow!O2Member b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.wave == 0) c.fail(9);
    b.value.wave = 1;
}
struct O2App { uint failures; bool zeroBefore, clean, saw; }
struct O2Root { PrivateChildPark!O2Member* kids; O2App* app; ubyte phase; }
PrivateRootDisposition o2Root(scope ref PrivateBorrow!O2Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!O2Member(1);
        namedCheck(b.value.kids.spawn(c, O2Member.init) == PrivateSpawnResult.spawned, "o2 spawn");
        auto idle = b.value.kids.outcome(c);
        b.value.app.zeroBefore = idle.failures == 0 && idle.participants == 0
            && idle.dispatched == 0 && !idle.cancelled && idle.succeeded;
        namedCheck(b.value.kids.start!o2Op(c) == PrivateStartResult.started, "o2 wave 1");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.kids.outcome(c);
    if (b.value.phase == 0)
    {
        namedCheck(o.failures == 1 && o.firstFailureCode == 9 && o.firstFailurePosition == 0, "o2 wave 1 outcome");
        b.value.phase = 1;
        namedCheck(b.value.kids.start!o2Op(c) == PrivateStartResult.started, "o2 wave 2");
        return PrivateRootDisposition.waitChildren;
    }
    b.value.app.failures = o.failures;
    b.value.app.clean = o.succeeded && o.participants == 1 && o.dispatched == 1 && !o.cancelled;
    b.value.app.saw = true;
    return PrivateRootDisposition.finish;
}

struct O3Member { uint index; }
void o3Op(scope ref PrivateBorrow!O3Member b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.index == 5) c.fail(5);
}
struct O3App { uint failures, code; size_t position; bool saw; }
struct O3Root { PrivateChildPark!O3Member* kids; O3App* app; }
PrivateRootDisposition o3Root(scope ref PrivateBorrow!O3Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!O3Member(8, PrivateCohortOptions(0, true));
        namedCheck(b.value.kids !is null, "o3 cohort");
        foreach (i; 0 .. 8)
            namedCheck(b.value.kids.spawn(c, O3Member(cast(uint) i)) == PrivateSpawnResult.spawned, "o3 spawn");
        uint[3] sel = [7, 2, 5];
        namedCheck(b.value.kids.startSubset!o3Op(c, sel[]) == PrivateStartResult.started, "o3 subset");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.kids.outcome(c);
    b.value.app.failures = o.failures;
    b.value.app.code = o.firstFailureCode;
    b.value.app.position = o.firstFailurePosition;
    b.value.app.saw = true;
    namedCheck(o.participants == 3 && o.dispatched == 3, "o3 counts");
    return PrivateRootDisposition.finish;
}

struct O4Member { ubyte phase; }
PrivateChildDisposition o4Op(scope ref PrivateBorrow!O4Member b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.phase == 0)
    {
        c.fail(3);
        b.value.phase = 1;
        return PrivateChildDisposition.waitChildren;
    }
    return PrivateChildDisposition.done;
}
struct O4App { uint failures, code; bool saw; }
struct O4Root { PrivateChildPark!O4Member* kids; O4App* app; }
PrivateRootDisposition o4Root(scope ref PrivateBorrow!O4Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!O4Member(1);
        namedCheck(b.value.kids.spawn(c, O4Member.init) == PrivateSpawnResult.spawned, "o4 spawn");
        namedCheck(b.value.kids.start!o4Op(c) == PrivateStartResult.started, "o4 start");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.kids.outcome(c);
    b.value.app.failures = o.failures;
    b.value.app.code = o.firstFailureCode;
    b.value.app.saw = o.firstFailurePosition == 0 && o.participants == 1 && o.dispatched == 1;
    return PrivateRootDisposition.finish;
}

struct O5Member { uint index; }
void o5Op(scope ref PrivateBorrow!O5Member b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.index % 3 == 0) c.fail(b.value.index);
}
struct O5App { uint failures, code; size_t position, participants; bool saw; }
struct O5Root { PrivateChildPark!O5Member* kids; O5App* app; }
PrivateRootDisposition o5Root(scope ref PrivateBorrow!O5Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        enum n = 4096;
        b.value.kids = c.createChildren!O5Member(n, 256);
        namedCheck(b.value.kids !is null, "o5 cohort");
        foreach (i; 0 .. n)
            namedCheck(b.value.kids.spawn(c, O5Member(cast(uint) i)) == PrivateSpawnResult.spawned, "o5 spawn");
        namedCheck(b.value.kids.start!o5Op(c) == PrivateStartResult.started, "o5 start");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.kids.outcome(c);
    b.value.app.failures = o.failures;
    b.value.app.code = o.firstFailureCode;
    b.value.app.position = o.firstFailurePosition;
    b.value.app.participants = o.participants;
    b.value.app.saw = o.dispatched == o.participants && !o.cancelled && !o.succeeded;
    return PrivateRootDisposition.finish;
}

enum o6Code = 42u;
struct O6Leaf { uint code; }
void o6Leaf(scope ref PrivateBorrow!O6Leaf b, scope ref PrivateContext c) nothrow @nogc @system
{
    c.fail(b.value.code);
}
struct O6Mid { PrivateChildPark!O6Leaf* leaves; }
PrivateChildDisposition o6Mid(scope ref PrivateBorrow!O6Mid b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.leaves is null)
    {
        b.value.leaves = c.createChildren!O6Leaf(1);
        namedCheck(b.value.leaves !is null
            && b.value.leaves.spawn(c, O6Leaf(o6Code)) == PrivateSpawnResult.spawned, "o6 leaf");
        namedCheck(b.value.leaves.start!o6Leaf(c) == PrivateStartResult.started, "o6 leaf wave");
        return PrivateChildDisposition.waitChildren;
    }
    auto o = b.value.leaves.outcome(c);
    namedCheck(o.failures == 1 && o.firstFailureCode == o6Code && o.firstFailurePosition == 0, "o6 leaf outcome");
    c.fail(o.firstFailureCode);
    return PrivateChildDisposition.done;
}
struct O6App { bool failed; uint code; }
struct O6Root { PrivateChildPark!O6Mid* mids; O6App* app; }
PrivateRootDisposition o6Root(scope ref PrivateBorrow!O6Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.mids is null)
    {
        b.value.mids = c.createChildren!O6Mid(1);
        namedCheck(b.value.mids !is null
            && b.value.mids.spawn(c, O6Mid.init) == PrivateSpawnResult.spawned, "o6 mid");
        namedCheck(b.value.mids.start!o6Mid(c) == PrivateStartResult.started, "o6 mid wave");
        return PrivateRootDisposition.waitChildren;
    }
    auto o = b.value.mids.outcome(c);
    b.value.app.failed = o.failures != 0;
    b.value.app.code = o.firstFailureCode;
    namedCheck(o.firstFailurePosition == 0 && o.participants == 1, "o6 mid outcome");
    c.fail(o.firstFailureCode); // root -> application outcome (tracked park)
    return PrivateRootDisposition.finish;
}

struct O9Member { ubyte phase; uint index; }
shared uint o9Failed, o9Release;
uint o9Result(scope ref PrivateBorrow!O9Member b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.index == 0 && b.value.phase == 0)
    {
        c.fail(7);
        b.value.phase = 1;
    }
    return 11;
}
void o9Void(scope ref PrivateBorrow!O9Member, scope ref PrivateContext) nothrow @nogc @system {}
PrivateChildStep!uint o9Cont(scope ref PrivateBorrow!O9Member b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.phase < 2)
    {
        c.fail(9);
        b.value.phase = 2;
        atomicStore!(MemoryOrder.rel)(o9Failed, 1u);
        while (atomicLoad!(MemoryOrder.acq)(o9Release) == 0) {}
        return typeof(return).waitChildren();
    }
    return typeof(return).done(13);
}
struct O9App { bool emptyOk, rejected, finalOk; }
struct O9Root
{
    PrivateChildPark!(O9Member, uint)* kids;
    O9App* app;
    ubyte phase;
}
PrivateRootDisposition o9Root(scope ref PrivateBorrow!O9Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!(O9Member, uint)(2, PrivateCohortOptions(0, true));
        namedCheck(b.value.kids !is null, "o9 cohort");
        foreach (i; 0 .. 2)
            namedCheck(b.value.kids.spawn(c, O9Member(0, cast(uint) i)) == PrivateSpawnResult.spawned, "o9 spawn");
        namedCheck(b.value.kids.start!o9Result(c) == PrivateStartResult.started, "o9 result wave");
        return PrivateRootDisposition.waitChildren;
    }
    if (b.value.phase == 0)
    {
        auto o = b.value.kids.outcome(c);
        auto r = b.value.kids.results(c);
        namedCheck(o.failures == 1 && o.firstFailureCode == 7 && o.firstFailurePosition == 0, "o9 result outcome");
        namedCheck(r.length == 2 && r[0] == 11 && r[1] == 11, "o9 result slots");
        b.value.phase = 1;
        namedCheck(b.value.kids.start!o9Void(c) == PrivateStartResult.started, "o9 void wave");
        return PrivateRootDisposition.waitChildren;
    }
    if (b.value.phase == 1)
    {
        auto before = b.value.kids.outcome(c);
        auto results = b.value.kids.results(c);
        namedCheck(results.length == 0 && before.failures == 0 && before.participants == 2
            && before.dispatched == 2 && before.succeeded && !before.cancelled, "o9 void outcome");
        namedCheck(b.value.kids.startSubset!o9Result(c, null) == PrivateStartResult.empty, "o9 empty");
        auto after = b.value.kids.outcome(c);
        b.value.app.emptyOk = b.value.kids.results(c).length == 0
            && after.failures == before.failures && after.participants == before.participants
            && after.dispatched == before.dispatched && after.cancelled == before.cancelled
            && after.firstFailureCode == before.firstFailureCode;
        b.value.phase = 2;
        uint[1] only = [0];
        namedCheck(b.value.kids.startSubset!o9Cont(c, only[]) == PrivateStartResult.started, "o9 continuation");
        return PrivateRootDisposition.again;
    }
    if (!b.value.kids.finished(c))
    {
        if (atomicLoad!(MemoryOrder.acq)(o9Failed) != 0 && !b.value.app.rejected)
        {
            namedCheck(b.value.kids.start!o9Void(c) == PrivateStartResult.waveActive, "o9 active reject");
            b.value.app.rejected = true;
            atomicStore!(MemoryOrder.rel)(o9Release, 1u);
        }
        return PrivateRootDisposition.again;
    }
    auto o = b.value.kids.outcome(c);
    auto r = b.value.kids.results(c);
    b.value.app.finalOk = o.failures == 1 && o.firstFailureCode == 9 && o.firstFailurePosition == 0
        && o.participants == 1 && o.dispatched == 1
        && r.length == 1 && r[0] == 13;
    return PrivateRootDisposition.finish;
}

void drainOutcome(P)(P* park, ref Token token, string name)
{
    auto deadline = MonoTime.currTime + 20.seconds;
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, name);
    }
    park.destroy();
}
void phaseOutcomes()
{
    version (AntfarmActorTestHooks) outcomeO1();
    {
        O2App app;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(O2Root, o2Root).create(farm, 1);
        check(park.spawn(O2Root(null, &app)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "o2 drain");
        }
        park.destroy();
        check(app.zeroBefore && app.saw && app.failures == 0 && app.clean);
    }
    {
        O3App app;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(O3Root, o3Root).create(farm, 1);
        check(park.spawn(O3Root(null, &app)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live) { park.pump(token); while (view.consumeNext()) {} namedCheck(MonoTime.currTime < deadline, "o3 drain"); }
        park.destroy();
        check(app.saw && app.failures == 1 && app.code == 5 && app.position == 2);
    }
    {
        O4App app;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(O4Root, o4Root).create(farm, 1);
        check(park.spawn(O4Root(null, &app)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live) { park.pump(token); while (view.consumeNext()) {} namedCheck(MonoTime.currTime < deadline, "o4 drain"); }
        park.destroy();
        check(app.saw && app.failures == 1 && app.code == 3);
    }
    {
        O5App app;
        auto farm = AntFarm.create(4, 8, 4, 0, 0, 1, 65536);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        auto workers = new Workers(farm, 4);
        scope(exit) workers.join();
        auto park = PrivateRootPark!(O5Root, o5Root).create(farm, 1, 256);
        check(park.spawn(O5Root(null, &app)) == PrivateSpawnResult.spawned);
        drainOutcome(park, token, "o5 drain");
        check(app.saw && app.failures == 1366 && app.participants == 4096);
        check(app.position == app.code && app.code % 3 == 0);
    }
    {
        O6App app;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(O6Root, o6Root).create(farm, 1, trackedOptions());
        PrivateRootHandle h;
        check(park.spawn(O6Root(null, &app), h) == PrivateSpawnResult.spawned);
        PrivateRootOutcome[] taken;
        drainTracked(park, token, view, taken, "o6 drain");
        park.destroy();
        check(app.failed && app.code == o6Code);
        check(taken.length == 1 && taken[0].root == h && taken[0].status == PrivateOutcomeStatus.failed
            && taken[0].code == o6Code && taken[0].cause == PrivateCancelCause.none);
    }
    {
        O9App app;
        atomicStore!(MemoryOrder.raw)(o9Failed, 0u);
        atomicStore!(MemoryOrder.raw)(o9Release, 0u);
        scope(exit) atomicStore!(MemoryOrder.rel)(o9Release, 1u);
        auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 8192);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        auto workers = new Workers(farm, 2);
        scope(exit) workers.join();
        auto park = PrivateRootPark!(O9Root, o9Root).create(farm, 1, 1);
        check(park.spawn(O9Root(null, &app)) == PrivateSpawnResult.spawned);
        drainOutcome(park, token, "o9 drain");
        check(app.emptyOk && app.rejected && app.finalOk);
    }
    {
        O9EmptyApp app;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(O9EmptyRoot, o9EmptyRoot).create(farm, 1);
        check(park.spawn(O9EmptyRoot(null, &app)) == PrivateSpawnResult.spawned);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (park.live)
        {
            park.pump(token);
            while (view.consumeNext()) {}
            namedCheck(MonoTime.currTime < deadline, "o9 empty-start drain");
        }
        park.destroy();
        check(app.preserved);
    }
    writeln("PASS private wave outcomes O1-O6 and O9");
}

// Mutation: move resetOutcome(count_) above start's `count_ == 0` return.
// startFinal has retired the members, so the misplaced reset stores
// participants 0 and failures 0, then returns empty.
struct O9EmptyMember { uint n; }
void o9EmptyOp(scope ref PrivateBorrow!O9EmptyMember, scope ref PrivateContext c)
    nothrow @nogc @system
{
    c.fail(17);
}
struct O9EmptyApp { bool preserved; }
struct O9EmptyRoot { PrivateChildPark!O9EmptyMember* kids; O9EmptyApp* app; }
PrivateRootDisposition o9EmptyRoot(scope ref PrivateBorrow!O9EmptyRoot b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.kids is null)
    {
        b.value.kids = c.createChildren!O9EmptyMember(1);
        namedCheck(b.value.kids !is null
            && b.value.kids.spawn(c, O9EmptyMember.init) == PrivateSpawnResult.spawned,
            "o9 empty-start spawn");
        namedCheck(b.value.kids.startFinal!o9EmptyOp(c) == PrivateStartResult.started,
            "o9 empty-start final");
        return PrivateRootDisposition.waitChildren;
    }
    auto before = b.value.kids.outcome(c);
    namedCheck(before.failures == 1 && before.firstFailureCode == 17
        && before.firstFailurePosition == 0 && before.participants == 1
        && before.dispatched == 1 && !before.cancelled, "o9 empty-start joined");
    namedCheck(b.value.kids.start!o9EmptyOp(c) == PrivateStartResult.empty,
        "o9 empty-start rejects");
    auto after = b.value.kids.outcome(c);
    b.value.app.preserved = after.failures == before.failures
        && after.firstFailureCode == before.firstFailureCode
        && after.firstFailurePosition == before.firstFailurePosition
        && after.participants == before.participants
        && after.dispatched == before.dispatched
        && after.cancelled == before.cancelled;
    return PrivateRootDisposition.finish;
}

struct OutcomeDeathRoot { PrivateChildPark!int* kids; }
void outcomeDeathNop(scope ref PrivateBorrow!int, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition outcomeActiveStep(scope ref PrivateBorrow!OutcomeDeathRoot b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    b.value.kids = c.createChildren!int(1);
    namedCheck(b.value.kids.spawn(c, 0) == PrivateSpawnResult.spawned, "outcome active spawn");
    namedCheck(b.value.kids.start!outcomeDeathNop(c) == PrivateStartResult.started, "outcome active start");
    b.value.kids.outcome(c);
    fprintf(stderr, "outcome active survived\n");
    return PrivateRootDisposition.finish;
}
PrivateRootDisposition rootFailStep(scope ref PrivateBorrow!int, scope ref PrivateContext c)
    nothrow @nogc @system
{
    c.fail(1);
    fprintf(stderr, "root fail survived\n");
    return PrivateRootDisposition.finish;
}
struct OverflowMember { uint n; }
void overflowOp(scope ref PrivateBorrow!OverflowMember, scope ref PrivateContext c) nothrow @nogc @system
{
    privateTestSetWaveFailures(c, uint.max - 1);
    c.fail(1);
    c.fail(2);
    fprintf(stderr, "failure overflow survived\n");
}
struct OverflowRoot { PrivateChildPark!OverflowMember* kids; ubyte phase; }
PrivateRootDisposition overflowStep(scope ref PrivateBorrow!OverflowRoot b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.phase != 0) return PrivateRootDisposition.finish;
    b.value.kids = c.createChildren!OverflowMember(1);
    namedCheck(b.value.kids.spawn(c, OverflowMember.init) == PrivateSpawnResult.spawned, "overflow spawn");
    namedCheck(b.value.kids.start!overflowOp(c) == PrivateStartResult.started, "overflow start");
    b.value.phase = 1;
    return PrivateRootDisposition.waitChildren;
}
void outcomeActiveDeath()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(OutcomeDeathRoot, outcomeActiveStep).create(farm, 1);
    check(park.spawn(OutcomeDeathRoot.init) == PrivateSpawnResult.spawned);
    while (park.live) { park.pump(token); while (view.consumeNext()) {} }
}
void rootFailDeath()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(int, rootFailStep).create(farm, 1);
    check(park.spawn(0) == PrivateSpawnResult.spawned);
    while (park.live) { park.pump(token); while (view.consumeNext()) {} }
}
void failureOverflowDeath()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(OverflowRoot, overflowStep).create(farm, 1);
    check(park.spawn(OverflowRoot.init) == PrivateSpawnResult.spawned);
    while (park.live) { park.pump(token); while (view.consumeNext()) {} }
}
void failUnauthorizedDeath()
{
    PrivateContext empty;
    empty.fail(1);
}
void outcomeUnauthorizedDeath()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    AuthorityProbe probe;
    auto park = PrivateRootPark!(AuthorityRoot, outcomeAuthorityStep).create(farm, 1);
    check(park.spawn(AuthorityRoot(&probe, true)) == PrivateSpawnResult.spawned);
    while (park.live) { park.pump(token); while (view.consumeNext()) {} }
}
PrivateRootDisposition outcomeAuthorityStep(scope ref PrivateBorrow!AuthorityRoot b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    auto kids = c.createChildren!State(1);
    namedCheck(kids !is null, "outcome unauthorized cohort");
    PrivateContext empty;
    kids.outcome(empty);
    return PrivateRootDisposition.finish;
}

// ---------------------------------------------------------------------------
// F2 in-tree cancellation (design 170 §4, tests §12.2). Each deterministic
// test names the mutation of actors/private_actor.d that it catches; the
// schedule below aborts only when that mutation is applied (normal runs pass).
//
// Most schedules use a lockstep consumer: a second thread runs callbacks only
// while the emitter waits in lockstepDrain. Table publication, cancellation
// and completion therefore interleave in one fixed order, and no callback
// ever runs on the emitter thread.

ubyte cancelThreadMarker; // thread-local: its address identifies the thread
size_t cancelTid() nothrow @nogc @system { return cast(size_t) &cancelThreadMarker; }

struct Lockstep { shared uint requested, served, stop; }
void lockstepDrain(Lockstep* l) nothrow @nogc @system
{
    immutable target = atomicFetchAdd!(MemoryOrder.acq_rel)(l.requested, 1u) + 1;
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad!(MemoryOrder.acq)(l.served) < target)
    {
        namedCheck(MonoTime.currTime < deadline, "lockstep drain timeout");
        pause();
    }
}
Thread startLockstep(AntFarm* farm, Lockstep* l)
{
    shared uint ready;
    auto t = new Thread({
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        atomicStore!(MemoryOrder.rel)(ready, 1u);
        uint served;
        while (atomicLoad!(MemoryOrder.acq)(l.stop) == 0)
        {
            immutable want = atomicLoad!(MemoryOrder.acq)(l.requested);
            if (want == served) { pause(); continue; }
            while (view.consumeNext()) {}
            served = want;
            atomicStore!(MemoryOrder.rel)(l.served, served);
        }
    });
    t.start();
    while (atomicLoad!(MemoryOrder.acq)(ready) == 0) Thread.yield();
    return t;
}
void stopLockstep(Lockstep* l, Thread t)
{
    atomicStore!(MemoryOrder.rel)(l.stop, 1u);
    t.join();
}
// One pump, then every published callback, until no root is live.
void lockstepRun(P)(P* park, ref Token token, Lockstep* l, string name)
{
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        lockstepDrain(l);
        namedCheck(MonoTime.currTime < deadline, name);
    }
    park.destroy();
}

// Stored-instance destructor counter; `live` is false in moved-from values.
shared uint cancelDtors, cancelDtorsOnEmitter;
shared size_t cancelEmitterTid;
void resetCancelDtors() nothrow @nogc @system
{
    atomicStore!(MemoryOrder.raw)(cancelDtors, 0u);
    atomicStore!(MemoryOrder.raw)(cancelDtorsOnEmitter, 0u);
    atomicStore!(MemoryOrder.raw)(cancelEmitterTid, cancelTid());
}
struct CancelTracked
{
    uint index;
    bool live;
    ~this() nothrow @nogc @system
    {
        if (!live) return;
        atomicFetchAdd!(MemoryOrder.raw)(cancelDtors, 1u);
        if (cancelTid() == atomicLoad!(MemoryOrder.raw)(cancelEmitterTid))
            atomicFetchAdd!(MemoryOrder.raw)(cancelDtorsOnEmitter, 1u);
    }
}

// C1. Polling root, batchLimit 2, six result members. The root cancels on
// its second activation, after the first table; the second emit truncates.
// A second wave then runs all six, and the per-member run counts show the
// undispatched members were untouched.
// Mutation C1a: delete `dispatched_ = cursor_` in truncated(). The debug build
// dies "private child result: unwritten slot" at finish; release fails
// "c1 truncated outcome" (dispatched 6).
// Mutation C1b: truncate without sealing (`return true` instead of the
// releaseWave): the wave never joins, "c1 drain" times out.
struct C1Member { uint index, runs; }
uint c1Op(scope ref PrivateBorrow!C1Member b, scope ref PrivateContext) nothrow @nogc @system
{
    ++b.value.runs;
    return b.value.index * 100 + b.value.runs;
}
struct C1App { bool truncatedOk, secondOk; }
struct C1Root { PrivateChildPark!(C1Member, uint)* kids; C1App* app; ubyte phase; }
PrivateRootDisposition c1Root(scope ref PrivateBorrow!C1Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!(C1Member, uint)(6, 2);
            namedCheck(kids !is null, "c1 cohort");
            foreach (i; 0 .. 6)
                namedCheck(kids.spawn(c, C1Member(cast(uint) i)) == PrivateSpawnResult.spawned, "c1 spawn");
            namedCheck(kids.start!c1Op(c) == PrivateStartResult.started, "c1 start");
            phase = 1;
            return PrivateRootDisposition.again;
        }
        if (phase == 1)
        {
            namedCheck(!c.cancelled, "c1 root is not cancelled by its own child wave");
            namedCheck(kids.cancel(c), "c1 cancel active wave");
            phase = 2;
            return PrivateRootDisposition.again;
        }
        if (!kids.finished(c)) return PrivateRootDisposition.again;
        auto o = kids.outcome(c);
        auto r = kids.results(c);
        if (phase == 2)
        {
            namedCheck(o.participants == 6 && o.dispatched == 2 && o.cancelled && !o.succeeded
                && r.length == 2 && r[0] == 1 && r[1] == 101, "c1 truncated outcome");
            app.truncatedOk = true;
            namedCheck(!kids.cancel(c), "c1 cancel idle cohort");
            namedCheck(kids.start!c1Op(c) == PrivateStartResult.started, "c1 second start");
            phase = 3;
            return PrivateRootDisposition.again;
        }
        namedCheck(o.participants == 6 && o.dispatched == 6 && !o.cancelled && o.succeeded
            && r.length == 6, "c1 second outcome");
        foreach (i; 0 .. 6)
            namedCheck(r[i] == i * 100 + (i < 2 ? 2 : 1), "c1 undispatched members untouched");
        app.secondOk = true;
        return PrivateRootDisposition.finish;
    }
}
void cancelC1()
{
    C1App app;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C1Root, c1Root).create(farm, 1);
    check(park.spawn(C1Root(null, &app)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c1 drain");
    check(app.truncatedOk && app.secondOk);
}

// C2. startFinal with a counting ~T, batchLimit 2; member 0 cancels its
// peers. Members 2..5 are retired by cleanup tables. Every stored member is
// destroyed exactly once, all on the consumer thread.
// Mutation C2a: seal a truncated retiring wave like a non-retiring one
// (drop the `if (retire_) return false;`): finish zeroes count_ and members
// 2..5 are never destroyed, "c2 every member destroyed once" (2 of 6).
// Mutation C2b: seal at truncation and retire the skipped members in finish:
// the seal is the final credit, so finish and ~T run on the emitter,
// "c2 destructors on consumers".
void c2Op(scope ref PrivateBorrow!CancelTracked b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value.index != 0) return;
    c.cancelPeers();
    namedCheck(c.cancelled, "c2 member sees its wave cancelled");
}
struct C2App { bool ok; }
struct C2Root { PrivateChildPark!CancelTracked* kids; C2App* app; }
PrivateRootDisposition c2Root(scope ref PrivateBorrow!C2Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!CancelTracked(6, 2);
            namedCheck(kids !is null, "c2 cohort");
            foreach (i; 0 .. 6)
                namedCheck(kids.spawn(c, CancelTracked(cast(uint) i, true)) == PrivateSpawnResult.spawned, "c2 spawn");
            namedCheck(kids.startFinal!c2Op(c) == PrivateStartResult.started, "c2 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        namedCheck(o.participants == 6 && o.dispatched == 2 && o.cancelled, "c2 outcome");
        namedCheck(kids.start!c2Op(c) == PrivateStartResult.empty, "c2 retiring wave emptied the cohort");
        app.ok = true;
        return PrivateRootDisposition.finish;
    }
}
void cancelC2()
{
    C2App app;
    Counts counts;
    resetCancelDtors();
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        Lockstep l;
        auto t = startLockstep(farm, &l);
        scope(exit) stopLockstep(&l, t);
        auto park = PrivateRootPark!(C2Root, c2Root).create(farm, 1, 256, policy(counts));
        check(park.spawn(C2Root(null, &app)) == PrivateSpawnResult.spawned);
        lockstepRun(park, token, &l, "c2 drain");
    }
    check(app.ok);
    namedCheck(atomicLoad(cancelDtors) == 6, "c2 every member destroyed once");
    namedCheck(atomicLoad(cancelDtorsOnEmitter) == 0, "c2 destructors on consumers");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "c2 allocation balance");
}

// C3. Retiring result-continuation wave, batchLimit 2. Members 0 and 1 each
// start a one-leaf wave and suspend; member 0 also cancels its peers. The
// next emit truncates: members 2..5 get cleanup tables while members 0 and 1
// are resumed into their op, write their results and retire.
// Mutation C3: switch `park.header_.call` to runSkippedRetire instead of
// using the local header. The resume tables then run the cleanup callback:
// members 0 and 1 never return done, their activation holds are never
// dropped, and "c3 drain" times out (debug builds may first die on the
// unwritten-slot check).
struct C3Member
{
    uint index, activations;
    PrivateChildPark!uint* leaves;
    bool live;
    ~this() nothrow @nogc @system
    {
        if (live) atomicFetchAdd!(MemoryOrder.raw)(cancelDtors, 1u);
    }
}
shared uint c3LeafRuns;
void c3Leaf(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(c3LeafRuns, 1u);
}
PrivateChildStep!uint c3Op(scope ref PrivateBorrow!C3Member b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        ++activations;
        if (leaves is null)
        {
            leaves = c.createChildren!uint(1);
            namedCheck(leaves !is null && leaves.spawn(c, 0) == PrivateSpawnResult.spawned, "c3 leaf");
            namedCheck(leaves.start!c3Leaf(c) == PrivateStartResult.started, "c3 leaf wave");
            if (index == 0) c.cancelPeers();
            return typeof(return).waitChildren();
        }
        namedCheck(c.cancelled, "c3 resumed member sees cancellation");
        namedCheck(activations == 2, "c3 resumed exactly once");
        return typeof(return).done(100 + index);
    }
}
struct C3App { bool ok; }
struct C3Root { PrivateChildPark!(C3Member, uint)* kids; C3App* app; }
PrivateRootDisposition c3Root(scope ref PrivateBorrow!C3Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!(C3Member, uint)(6, 2);
            namedCheck(kids !is null, "c3 cohort");
            foreach (i; 0 .. 6) // rvalues: moved into storage, no live copy remains
                namedCheck(kids.spawn(c, C3Member(cast(uint) i, 0, null, true)) == PrivateSpawnResult.spawned, "c3 spawn");
            namedCheck(kids.startFinal!c3Op(c) == PrivateStartResult.started, "c3 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        auto r = kids.results(c);
        namedCheck(o.participants == 6 && o.dispatched == 2 && o.cancelled
            && r.length == 2 && r[0] == 100 && r[1] == 101, "c3 resumed results");
        app.ok = true;
        return PrivateRootDisposition.finish;
    }
}
void cancelC3()
{
    C3App app;
    Counts counts;
    resetCancelDtors();
    atomicStore!(MemoryOrder.raw)(c3LeafRuns, 0u);
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        Lockstep l;
        auto t = startLockstep(farm, &l);
        scope(exit) stopLockstep(&l, t);
        auto park = PrivateRootPark!(C3Root, c3Root).create(farm, 1, 256, policy(counts));
        check(park.spawn(C3Root(null, &app)) == PrivateSpawnResult.spawned);
        lockstepRun(park, token, &l, "c3 drain");
    }
    check(app.ok);
    // Both leaf waves start after member 0's cancel and are truncated at zero.
    namedCheck(atomicLoad(c3LeafRuns) == 0, "c3 leaves of a cancelled scope skipped");
    namedCheck(atomicLoad(cancelDtors) == 6, "c3 every member retired once");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "c3 allocation balance");
}

// C4. Four continuation members, one per table. Members 1..3 each start a
// one-member grandchild wave and suspend. Member 0 waits until all three
// grandchildren are running, then cancels its peers. The grandchildren
// observe `cancelled` through the walk: their own wave is not cancelled, but
// their parent's enclosing wave is.
// Mutation C4: omit the subtreeCancel store in cancelWave. The fast path
// stays false and "c4 grandchildren observe cancellation" fails (0 of 3).
shared uint c4Started, c4Cancelled, c4Saw;
struct C4Member { uint index; PrivateChildPark!uint* kids; }
void c4Grandchild(scope ref PrivateBorrow!uint, scope ref PrivateContext c) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.acq_rel)(c4Started, 1u);
    awaitRaw(c4Cancelled, 1u, "c4 grandchild waits for the cancel");
    atomicFence!(MemoryOrder.acq)();
    if (c.cancelled) atomicFetchAdd!(MemoryOrder.raw)(c4Saw, 1u);
}
PrivateChildDisposition c4Op(scope ref PrivateBorrow!C4Member b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (index == 0)
        {
            awaitRaw(c4Started, 3u, "c4 grandchildren running");
            c.cancelPeers();
            atomicStore!(MemoryOrder.rel)(c4Cancelled, 1u);
            return PrivateChildDisposition.done;
        }
        if (kids !is null) return PrivateChildDisposition.done;
        kids = c.createChildren!uint(1);
        namedCheck(kids !is null && kids.spawn(c, 0) == PrivateSpawnResult.spawned, "c4 grandchild");
        namedCheck(kids.start!c4Grandchild(c) == PrivateStartResult.started, "c4 grandchild wave");
        return PrivateChildDisposition.waitChildren;
    }
}
struct C4Root { PrivateChildPark!C4Member* kids; bool* ok; }
PrivateRootDisposition c4Root(scope ref PrivateBorrow!C4Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!C4Member(4, 1);
            namedCheck(kids !is null, "c4 cohort");
            foreach (i; 0 .. 4)
                namedCheck(kids.spawn(c, C4Member(cast(uint) i)) == PrivateSpawnResult.spawned, "c4 spawn");
            namedCheck(kids.start!c4Op(c) == PrivateStartResult.started, "c4 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        *ok = o.cancelled && o.participants == 4 && o.dispatched == 4;
        return PrivateRootDisposition.finish;
    }
}
void cancelC4()
{
    bool ok;
    atomicStore!(MemoryOrder.raw)(c4Started, 0u);
    atomicStore!(MemoryOrder.raw)(c4Cancelled, 0u);
    atomicStore!(MemoryOrder.raw)(c4Saw, 0u);
    auto farm = AntFarm.create(2, 8, 5, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 5);
    scope(exit) workers.join();
    auto park = PrivateRootPark!(C4Root, c4Root).create(farm, 1);
    check(park.spawn(C4Root(null, &ok)) == PrivateSpawnResult.spawned);
    drainOutcome(park, token, "c4 drain");
    check(ok);
    namedCheck(atomicLoad(c4Saw) == 3, "c4 grandchildren observe cancellation");
}

// C5. Capacity-one park: root A cancels its tree and finishes; root B
// reuses the slot and must start with a clear summary byte.
// Mutation C5: delete the `subtreeCancel = false` reset in spawn. Root B
// sees the stale byte, "c5 recycled root starts clear".
struct C5Root { PrivateChildPark!uint* kids; bool second; bool* ok; }
void c5Op(scope ref PrivateBorrow!uint, scope ref PrivateContext c) nothrow @nogc @system
{
    namedCheck(!c.cancelled, "c5 recycled tree is not cancelled");
}
PrivateRootDisposition c5Root(scope ref PrivateBorrow!C5Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            version (AntfarmActorTestHooks)
                if (second)
                    namedCheck(!privateTestSubtreeCancel(c), "c5 recycled root starts clear");
            kids = c.createChildren!uint(2);
            namedCheck(kids !is null, "c5 cohort");
            foreach (i; 0 .. 2) namedCheck(kids.spawn(c, 0) == PrivateSpawnResult.spawned, "c5 spawn");
            namedCheck(kids.start!c5Op(c) == PrivateStartResult.started, "c5 start");
            if (!second) c.cancelChildren();
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        version (AntfarmActorTestHooks)
            namedCheck(privateTestSubtreeCancel(c) == !second, "c5 summary is sticky for the root's life");
        *ok = second ? o.dispatched == 2 && !o.cancelled : o.dispatched == 0 && o.cancelled;
        return PrivateRootDisposition.finish;
    }
}
void cancelC5()
{
    bool first, second;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C5Root, c5Root).create(farm, 1);
    check(park.spawn(C5Root(null, false, &first)) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        lockstepDrain(&l);
        namedCheck(MonoTime.currTime < deadline, "c5 first drain");
    }
    check(park.spawn(C5Root(null, true, &second)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c5 second drain");
    check(first && second);
}

// C6. Cancel wave 1 before its first table, then start wave 2 on the same
// cohort: wave 2 runs every member.
// Mutation C6: delete the `cancel` reset from resetOutcome. Wave 2 is
// truncated at zero, "c6 second wave runs fully".
shared uint c6Runs;
void c6Op(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(c6Runs, 1u);
}
struct C6Root { PrivateChildPark!uint* kids; ubyte phase; bool* ok; }
PrivateRootDisposition c6Root(scope ref PrivateBorrow!C6Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (phase == 0)
        {
            kids = c.createChildren!uint(4, 1);
            namedCheck(kids !is null, "c6 cohort");
            foreach (i; 0 .. 4) namedCheck(kids.spawn(c, 0) == PrivateSpawnResult.spawned, "c6 spawn");
            namedCheck(kids.start!c6Op(c) == PrivateStartResult.started, "c6 first start");
            namedCheck(kids.cancel(c), "c6 cancel");
            phase = 1;
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        if (phase == 1)
        {
            namedCheck(o.dispatched == 0 && o.cancelled && atomicLoad(c6Runs) == 0, "c6 first wave skipped");
            namedCheck(kids.start!c6Op(c) == PrivateStartResult.started, "c6 second start");
            phase = 2;
            return PrivateRootDisposition.waitChildren;
        }
        namedCheck(o.dispatched == 4 && !o.cancelled && o.succeeded, "c6 second wave runs fully");
        *ok = atomicLoad(c6Runs) == 4;
        return PrivateRootDisposition.finish;
    }
}
void cancelC6()
{
    bool ok;
    atomicStore!(MemoryOrder.raw)(c6Runs, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C6Root, c6Root).create(farm, 1);
    check(park.spawn(C6Root(null, 0, &ok)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c6 drain");
    check(ok);
}

// C7. The truncating seal is the final W credit, and the parent disposes the
// cohort while the emitter is still inside that seal's finish. A quarantining
// allocator poisons every freed block with an offset-dependent pattern and
// verifies it at the end, so any later park write is detected.
// Mutation C7: store dispatched_ after the truncating releaseWave. It writes
// into the freed cohort: "c7 freed memory untouched".
struct Quarantine
{
    void*[64] blocks;
    size_t[64] sizes;
    shared uint count;
    shared size_t watch;
    shared uint watchFreed;
}
ubyte poisonByte(size_t i) nothrow @nogc @safe { return cast(ubyte) (i * 37 + 0x5B); }
void* quarantineAllocate(void*, size_t bytes, size_t alignment) nothrow @nogc @system
{
    return alignment > 64 ? null : allocateAligned64(bytes);
}
void quarantineDeallocate(void* context, void* ptr, size_t bytes, size_t) nothrow @nogc @system
{
    auto q = cast(Quarantine*) context;
    foreach (i; 0 .. bytes) (cast(ubyte*) ptr)[i] = poisonByte(i);
    immutable slot = atomicFetchAdd!(MemoryOrder.raw)(q.count, 1u);
    namedCheck(slot < q.blocks.length, "c7 quarantine full");
    q.blocks[slot] = ptr;
    q.sizes[slot] = bytes;
    if (cast(size_t) ptr == atomicLoad!(MemoryOrder.raw)(q.watch))
        atomicStore!(MemoryOrder.rel)(q.watchFreed, 1u);
}
version (AntfarmActorTestHooks)
{
    __gshared Quarantine* c7Quarantine;
    shared uint c7RootCancelled, c7Armed, c7TruncatingSeals;
    __gshared PrivateWaveOutcome c7Outcome; // read after the workers join
    void c7Hook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        if (atomicLoad!(MemoryOrder.raw)(c7Armed) == 0) return;
        if (point == ActorTestPoint.privateEmitBeforeCancelCheck)
            awaitRaw(c7RootCancelled, 1u, "c7 root cancels before the first check");
        else if (point == ActorTestPoint.privateTruncateBeforeSeal)
            atomicFetchAdd!(MemoryOrder.raw)(c7TruncatingSeals, 1u);
        else if (point == ActorTestPoint.privateRootAfterIdleBeforeCredit)
        {
            // Inside the truncating seal's finish, on the emitter.
            awaitRaw(c7Quarantine.watchFreed, 1u, "c7 parent disposes the cohort");
            atomicStore!(MemoryOrder.raw)(c7Armed, 0u);
        }
    }
    struct C7Root { PrivateChildPark!uint* kids; }
    void c7Op(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system
    {
        namedCheck(false, "c7 cancelled member never runs");
    }
    PrivateRootDisposition c7Root(scope ref PrivateBorrow!C7Root b, scope ref PrivateContext c)
        nothrow @nogc @system
    {
        auto kids = c.createChildren!uint(4, 1);
        namedCheck(kids !is null, "c7 cohort");
        foreach (i; 0 .. 4) namedCheck(kids.spawn(c, 0) == PrivateSpawnResult.spawned, "c7 spawn");
        atomicStore!(MemoryOrder.raw)(c7Quarantine.watch, cast(size_t) kids);
        namedCheck(kids.start!c7Op(c) == PrivateStartResult.started, "c7 start");
        namedCheck(kids.cancel(c), "c7 cancel");
        atomicStore!(MemoryOrder.rel)(c7RootCancelled, 1u);
        auto deadline = MonoTime.currTime + 10.seconds;
        while (!kids.finished(c)) namedCheck(MonoTime.currTime < deadline, "c7 wave joins");
        c7Outcome = kids.outcome(c);
        return PrivateRootDisposition.finish; // disposes the cohort on this consumer
    }
    void cancelC7()
    {
        auto q = new Quarantine;
        c7Quarantine = q;
        atomicStore!(MemoryOrder.raw)(c7RootCancelled, 0u);
        atomicStore!(MemoryOrder.raw)(c7TruncatingSeals, 0u);
        atomicStore!(MemoryOrder.raw)(c7Armed, 1u);
        setActorTestHook(&c7Hook);
        {
            scope(exit) { setActorTestHook(null); atomicStore!(MemoryOrder.raw)(c7Armed, 0u); }
            auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 8192);
            scope(exit) farm.destroy();
            auto token = farm.registerProducer(Tier.small);
            scope(exit) farm.unregisterProducer(token);
            auto workers = new Workers(farm, 1);
            scope(exit) workers.join();
            auto park = PrivateRootPark!(C7Root, c7Root).create(farm, 1, 256,
                ActorAllocator(q, &quarantineAllocate, &quarantineDeallocate));
            check(park.spawn(C7Root.init) == PrivateSpawnResult.spawned);
            drainOutcome(park, token, "c7 drain");
        }
        namedCheck(atomicLoad(q.watchFreed) == 1, "c7 cohort freed during the seal");
        namedCheck(atomicLoad(c7TruncatingSeals) == 1, "c7 one truncating seal");
        foreach (slot; 0 .. atomicLoad(q.count))
        {
            auto bytes = cast(ubyte*) q.blocks[slot];
            foreach (i; 0 .. q.sizes[slot])
                namedCheck(bytes[i] == poisonByte(i), "c7 freed memory untouched");
            freeAligned64(q.blocks[slot]);
        }
        c7Quarantine = null;
        namedCheck(c7Outcome.dispatched == 0 && c7Outcome.participants == 4 && c7Outcome.cancelled, "c7 outcome");
    }
}

// C8. Waiting root; member 0 cancels its peers. The root is resumed exactly
// once, by the unchanged join, and only after the wave is idle.
// Mutation C8: an early wake. In cancelPeers, when the cancelled wave's
// parent is the root, also `releaseRoot(root)`. The root is pushed twice
// (once by the cancel, once by the join) and reclaimed twice, so live never
// reaches zero: "c8 drain" times out. A schedule that resumes it before the
// truncating seal fails "c8 resumed after the join".
shared uint c8Activations;
void c8Op(scope ref PrivateBorrow!uint b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value == 0) c.cancelPeers();
}
struct C8Root { PrivateChildPark!uint* kids; bool* ok; }
PrivateRootDisposition c8Root(scope ref PrivateBorrow!C8Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(c8Activations, 1u);
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!uint(4, 1);
            namedCheck(kids !is null, "c8 cohort");
            foreach (i; 0 .. 4) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "c8 spawn");
            namedCheck(kids.start!c8Op(c) == PrivateStartResult.started, "c8 start");
            return PrivateRootDisposition.waitChildren;
        }
        namedCheck(kids.finished(c), "c8 resumed after the join");
        auto o = kids.outcome(c);
        *ok = o.cancelled && o.dispatched == 1 && o.participants == 4;
        return PrivateRootDisposition.finish;
    }
}
void cancelC8()
{
    bool ok;
    atomicStore!(MemoryOrder.raw)(c8Activations, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C8Root, c8Root).create(farm, 1);
    check(park.spawn(C8Root(null, &ok)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c8 drain");
    check(ok);
    namedCheck(atomicLoad(c8Activations) == 2, "c8 exactly one resumption");
}

// C9. The cancel lands between the last table write and the seal: a hook at
// privateEmitBeforeSeal runs the published table, whose member 0 cancels its
// peers. The seal then joins the wave once; nothing is truncated.
// Mutation C9: evaluate truncation again at the seal, before the final
// releaseWave: `if (scopeCancelled(wave)) { dispatched_ = cursor_;
// releaseWave(wave); }`. The second release dies "private wave credit underflow".
version (AntfarmActorTestHooks)
{
    __gshared Lockstep* c9Lockstep;
    shared uint c9Armed;
    void c9Hook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
    {
        if (point != ActorTestPoint.privateEmitBeforeSeal) return;
        if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&c9Armed, 1u, 0u)) return;
        lockstepDrain(c9Lockstep); // the last table runs before the seal
    }
    struct C9Root { PrivateChildPark!uint* kids; bool* ok; }
    void c9Op(scope ref PrivateBorrow!uint b, scope ref PrivateContext c) nothrow @nogc @system
    {
        if (b.value == 0) c.cancelPeers();
    }
    PrivateRootDisposition c9Root(scope ref PrivateBorrow!C9Root b, scope ref PrivateContext c)
        nothrow @nogc @system
    {
        with (b.value)
        {
            if (kids is null)
            {
                kids = c.createChildren!uint(2, 2);
                namedCheck(kids !is null, "c9 cohort");
                foreach (i; 0 .. 2) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "c9 spawn");
                namedCheck(kids.start!c9Op(c) == PrivateStartResult.started, "c9 start");
                atomicStore!(MemoryOrder.raw)(c9Armed, 1u);
                return PrivateRootDisposition.waitChildren;
            }
            auto o = kids.outcome(c);
            *ok = o.cancelled && o.dispatched == 2 && o.participants == 2;
            return PrivateRootDisposition.finish;
        }
    }
    void cancelC9()
    {
        bool ok;
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        Lockstep l;
        auto t = startLockstep(farm, &l);
        scope(exit) stopLockstep(&l, t);
        c9Lockstep = &l;
        atomicStore!(MemoryOrder.raw)(c9Armed, 0u);
        setActorTestHook(&c9Hook);
        scope(exit) { setActorTestHook(null); c9Lockstep = null; }
        auto park = PrivateRootPark!(C9Root, c9Root).create(farm, 1);
        check(park.spawn(C9Root(null, &ok)) == PrivateSpawnResult.spawned);
        lockstepRun(park, token, &l, "c9 drain");
        check(ok);
        namedCheck(atomicLoad(c9Armed) == 0, "c9 hook ran");
    }
}

// C10. The root starts cohort A, leaves cohort B idle, calls cancelChildren
// and returns finishAfterChildren. A is truncated at zero, the cleanup
// activation runs once and disposes both cohorts on the consumer.
// Root-outcome half (F3), on tracked parks: the root fails, is cancelled
// through its handle before its first activation, or both. takeOutcome
// reports failed(31), cancelled(requested), and failed (failed outranks
// cancelled). Behavioural.
enum C10Mode : ubyte { untracked, failed, cancelled, failedAndCancelled }
shared uint c10Activations, c10Runs;
void c10Op(scope ref PrivateBorrow!CancelTracked, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(c10Runs, 1u);
}
struct C10Root { PrivateChildPark!CancelTracked* active, idle; C10Mode mode; bool* sawCancelled; }
PrivateRootDisposition c10Root(scope ref PrivateBorrow!C10Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(c10Activations, 1u);
    with (b.value)
    {
        if (mode != C10Mode.untracked) *sawCancelled = c.cancelled;
        if (mode == C10Mode.failed || mode == C10Mode.failedAndCancelled) c.fail(31);
        active = c.createChildren!CancelTracked(4, 1);
        idle = c.createChildren!CancelTracked(2);
        namedCheck(active !is null && idle !is null, "c10 cohorts");
        foreach (i; 0 .. 4)
            namedCheck(active.spawn(c, CancelTracked(cast(uint) i, true)) == PrivateSpawnResult.spawned, "c10 spawn");
        foreach (i; 0 .. 2)
            namedCheck(idle.spawn(c, CancelTracked(cast(uint) i, true)) == PrivateSpawnResult.spawned, "c10 spawn idle");
        namedCheck(active.start!c10Op(c) == PrivateStartResult.started, "c10 start");
        c.cancelChildren();
        namedCheck(!idle.cancel(c) && idle.outcome(c) == PrivateWaveOutcome.init, "c10 idle cohort unaffected");
        return PrivateRootDisposition.finishAfterChildren;
    }
}
void cancelC10(C10Mode mode = C10Mode.untracked)
{
    Counts counts;
    bool sawCancelled;
    PrivateRootOutcome[] taken;
    resetCancelDtors();
    atomicStore!(MemoryOrder.raw)(c10Activations, 0u);
    atomicStore!(MemoryOrder.raw)(c10Runs, 0u);
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        Lockstep l;
        auto t = startLockstep(farm, &l);
        scope(exit) stopLockstep(&l, t);
        if (mode == C10Mode.untracked)
        {
            auto park = PrivateRootPark!(C10Root, c10Root).create(farm, 1, 256, policy(counts));
            check(park.spawn(C10Root.init) == PrivateSpawnResult.spawned);
            lockstepRun(park, token, &l, "c10 drain");
        }
        else
        {
            PrivateRootOptions options = trackedOptions();
            options.allocator = policy(counts);
            auto park = PrivateRootPark!(C10Root, c10Root).create(farm, 1, options);
            PrivateRootHandle h;
            check(park.spawn(C10Root(null, null, mode, &sawCancelled), h) == PrivateSpawnResult.spawned);
            if (mode != C10Mode.failed)
                namedCheck(h.requestCancel() == PrivateCancelResult.requested, "c10 handle cancel");
            lockstepRunTracked(park, token, &l, taken, "c10 tracked drain");
            namedCheck(taken.length == 1 && taken[0].root == h, "c10 one outcome");
        }
    }
    final switch (mode)
    {
        case C10Mode.untracked: break;
        case C10Mode.failed:
            namedCheck(!sawCancelled && taken[0].status == PrivateOutcomeStatus.failed
                && taken[0].code == 31 && taken[0].cause == PrivateCancelCause.none, "c10 root outcome failed");
            break;
        case C10Mode.cancelled:
            namedCheck(sawCancelled && taken[0].status == PrivateOutcomeStatus.cancelled
                && taken[0].cause == PrivateCancelCause.requested, "c10 root outcome cancelled");
            break;
        case C10Mode.failedAndCancelled:
            namedCheck(sawCancelled && taken[0].status == PrivateOutcomeStatus.failed
                && taken[0].code == 31 && taken[0].cause == PrivateCancelCause.none, "c10 failed outranks cancelled");
            break;
    }
    namedCheck(atomicLoad(c10Activations) == 1, "c10 one user activation");
    namedCheck(atomicLoad(c10Runs) == 0, "c10 cancelled wave skipped");
    namedCheck(atomicLoad(cancelDtors) == 6 && atomicLoad(cancelDtorsOnEmitter) == 0,
        "c10 cleanup disposes both cohorts once on the consumer");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "c10 allocation balance");
}

// C11. Partial construction (U7): a mid-level member spawns four leaves and
// the allocator fails the fourth. It reports fail(77) and returns done. The
// root reads the failure and finishes; post-order disposal destroys the
// three built leaves, and spawn destroyed the rejected argument. No fatal.
// Behavioural.
shared uint c11FailNext;
void* c11Allocate(void* context, size_t bytes, size_t alignment) nothrow @nogc @system
{
    if (cas!(MemoryOrder.raw, MemoryOrder.raw)(&c11FailNext, 1u, 0u)) return null;
    return allocate(context, bytes, alignment);
}
struct C11Mid { uint unused; }
void c11Mid(scope ref PrivateBorrow!C11Mid b, scope ref PrivateContext c) nothrow @nogc @system
{
    auto leaves = c.createChildren!CancelTracked(4);
    namedCheck(leaves !is null, "c11 leaves");
    foreach (i; 0 .. 3)
        namedCheck(leaves.spawn(c, CancelTracked(cast(uint) i, true)) == PrivateSpawnResult.spawned, "c11 spawn");
    atomicStore!(MemoryOrder.raw)(c11FailNext, 1u);
    namedCheck(leaves.spawn(c, CancelTracked(3, true)) == PrivateSpawnResult.allocationFailed, "c11 fourth spawn fails");
    namedCheck(atomicLoad!(MemoryOrder.raw)(cancelDtors) == 1, "c11 rejected argument destroyed by spawn");
    c.fail(77);
    namedCheck(leaves.finished(c), "c11 no wave of this member is active");
}
struct C11Root { PrivateChildPark!C11Mid* mids; bool* ok; }
PrivateRootDisposition c11Root(scope ref PrivateBorrow!C11Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (mids is null)
        {
            mids = c.createChildren!C11Mid(1);
            namedCheck(mids !is null && mids.spawn(c, C11Mid.init) == PrivateSpawnResult.spawned, "c11 mid");
            namedCheck(mids.start!c11Mid(c) == PrivateStartResult.started, "c11 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = mids.outcome(c);
        *ok = o.failures == 1 && o.firstFailureCode == 77 && o.dispatched == 1;
        return PrivateRootDisposition.finish;
    }
}
void cancelC11()
{
    bool ok;
    Counts counts;
    resetCancelDtors();
    atomicStore!(MemoryOrder.raw)(c11FailNext, 0u);
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        Lockstep l;
        auto t = startLockstep(farm, &l);
        scope(exit) stopLockstep(&l, t);
        auto park = PrivateRootPark!(C11Root, c11Root).create(farm, 1, 256,
            ActorAllocator(&counts, &c11Allocate, &deallocate));
        check(park.spawn(C11Root(null, &ok)) == PrivateSpawnResult.spawned);
        lockstepRun(park, token, &l, "c11 drain");
    }
    // 3 stored leaves + the rejected argument, destroyed inside spawn.
    check(ok);
    namedCheck(atomicLoad(cancelDtors) == 4, "c11 three leaves and the argument destroyed");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "c11 allocation balance");
}

// C12. Subset [9,3,6] of ten, batchLimit 1, cancelled after the first table.
// Mutation C12: initialize participants_/dispatched_ from count_ instead of
// k_ in startSubset: "c12 subset outcome" (participants 10).
uint c12Op(scope ref PrivateBorrow!uint b, scope ref PrivateContext) nothrow @nogc @system
{
    return b.value + 1000;
}
struct C12Root { PrivateChildPark!(uint, uint)* kids; ubyte phase; bool* ok; }
PrivateRootDisposition c12Root(scope ref PrivateBorrow!C12Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (phase == 0)
        {
            kids = c.createChildren!(uint, uint)(10, PrivateCohortOptions(1, true));
            namedCheck(kids !is null, "c12 cohort");
            foreach (i; 0 .. 10) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "c12 spawn");
            uint[3] sel = [9, 3, 6];
            namedCheck(kids.startSubset!c12Op(c, sel[]) == PrivateStartResult.started, "c12 start");
            phase = 1;
            return PrivateRootDisposition.again;
        }
        if (phase == 1)
        {
            namedCheck(kids.cancel(c), "c12 cancel");
            phase = 2;
            return PrivateRootDisposition.again;
        }
        if (!kids.finished(c)) return PrivateRootDisposition.again;
        auto o = kids.outcome(c);
        auto r = kids.results(c);
        namedCheck(o.participants == 3 && o.dispatched == 1 && o.cancelled, "c12 subset outcome");
        *ok = r.length == 1 && r[0] == 1009;
        return PrivateRootDisposition.finish;
    }
}
void cancelC12()
{
    bool ok;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C12Root, c12Root).create(farm, 1);
    check(park.spawn(C12Root(null, 0, &ok)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c12 drain");
    check(ok);
}

// C13. A truncated startFinal result wave, then an empty start: the outcome
// is unchanged and the result prefix survives count_ = 0.
// Mutation C13: compute the result prefix from count_ instead of
// dispatched_. Debug dies "private child result: unwritten slot" in finish
// (count_ is still 4 there); release fails "c13 prefix survives retirement".
uint c13Op(scope ref PrivateBorrow!uint b, scope ref PrivateContext c) nothrow @nogc @system
{
    if (b.value == 0) c.cancelPeers();
    return b.value + 50;
}
struct C13Root { PrivateChildPark!(uint, uint)* kids; bool* ok; }
PrivateRootDisposition c13Root(scope ref PrivateBorrow!C13Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!(uint, uint)(4, 2);
            namedCheck(kids !is null, "c13 cohort");
            foreach (i; 0 .. 4) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "c13 spawn");
            namedCheck(kids.startFinal!c13Op(c) == PrivateStartResult.started, "c13 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto before = kids.outcome(c);
        auto r = kids.results(c);
        namedCheck(before.participants == 4 && before.dispatched == 2 && before.cancelled
            && r.length == 2 && r[0] == 50 && r[1] == 51, "c13 prefix survives retirement");
        namedCheck(kids.start!c13Op(c) == PrivateStartResult.empty, "c13 empty start");
        auto after = kids.outcome(c);
        auto again = kids.results(c);
        *ok = after == before && again.length == 2 && again[0] == 50 && again[1] == 51;
        return PrivateRootDisposition.finish;
    }
}
void cancelC13()
{
    bool ok;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep l;
    auto t = startLockstep(farm, &l);
    scope(exit) stopLockstep(&l, t);
    auto park = PrivateRootPark!(C13Root, c13Root).create(farm, 1);
    check(park.spawn(C13Root(null, &ok)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &l, "c13 drain");
    check(ok);
}

// Cancellation torture (§12.5): random trees up to depth 4 on 4 consumers.
// Members randomly fail, cancel their peers or their children; polling roots
// randomly cancel their cohort or cancel and finishAfterChildren. Invariants:
// every root finishes, each stored state is destroyed exactly once,
// allocations balance, outcomes are consistent with their result prefix,
// and `cancelled` never reverts within an activation.
shared uint tortureSpawned, tortureDtors, tortureTruncated, tortureCancelled, tortureFailed;
ulong tortureNext(ref ulong s) nothrow @nogc @safe
{
    s ^= s << 13; s ^= s >> 7; s ^= s << 17;
    return s;
}
struct TortureNode
{
    ulong seed;
    uint depth, index, count;
    void* kids; // PrivateChildPark!(TortureNode, uint)*
    bool live;
    ~this() nothrow @nogc @system
    {
        if (live) atomicFetchAdd!(MemoryOrder.raw)(tortureDtors, 1u);
    }
}
alias TortureCohort = PrivateChildPark!(TortureNode, uint);

// Starts a random wave (start, startFinal or startSubset) of a fresh cohort.
void tortureLaunch(ref ulong seed, uint depth, ref void* kidsOut, ref uint countOut,
    scope ref PrivateContext c) nothrow @nogc @system
{
    auto r = tortureNext(seed);
    immutable n = cast(uint) (1 + r % 6);
    auto kids = c.createChildren!(TortureNode, uint)(n,
        PrivateCohortOptions(cast(size_t) (1 + (r >> 8) % 3), true));
    namedCheck(kids !is null, "torture cohort");
    foreach (i; 0 .. n)
    {
        namedCheck(kids.spawn(c, TortureNode(tortureNext(seed) | 1, depth + 1, i, 0, null, true))
            == PrivateSpawnResult.spawned, "torture spawn");
        atomicFetchAdd!(MemoryOrder.raw)(tortureSpawned, 1u);
    }
    kidsOut = kids;
    countOut = n;
    final switch ((r >> 4) % 3)
    {
        case 0:
            namedCheck(kids.start!tortureOp(c) == PrivateStartResult.started, "torture start");
            break;
        case 1:
            namedCheck(kids.startFinal!tortureOp(c) == PrivateStartResult.started, "torture final");
            break;
        case 2:
            uint[6] sel;
            size_t k;
            foreach_reverse (i; 0 .. n) if (i % 2 == 0 || (r >> 30) & 1) sel[k++] = i;
            namedCheck(kids.startSubset!tortureOp(c, sel[0 .. k]) == PrivateStartResult.started, "torture subset");
            break;
    }
}

// Joined-wave invariants: prefix length, slot values, position validity.
void tortureCheck(TortureCohort* kids, scope ref PrivateContext c) nothrow @nogc @system
{
    auto o = kids.outcome(c);
    auto sel = kids.subsetMembers(c);
    auto r = kids.results(c);
    namedCheck(o.dispatched <= o.participants && o.participants != 0, "torture outcome counts");
    namedCheck(r.length == o.dispatched, "torture result prefix");
    foreach (j, value; r)
        namedCheck(value == (sel.length ? sel[j] : j) + 1, "torture result slot");
    namedCheck(o.succeeded == (o.failures == 0 && o.dispatched == o.participants), "torture succeeded");
    if (o.dispatched < o.participants) atomicFetchAdd!(MemoryOrder.raw)(tortureTruncated, 1u);
    if (o.cancelled) atomicFetchAdd!(MemoryOrder.raw)(tortureCancelled, 1u);
    if (o.failures != 0) atomicFetchAdd!(MemoryOrder.raw)(tortureFailed, 1u);
    if (o.failures != 0)
        namedCheck(o.firstFailurePosition < o.participants && o.firstFailureCode
            == (sel.length ? sel[o.firstFailurePosition] : o.firstFailurePosition) + 1000,
            "torture failure pair");
}

PrivateChildStep!uint tortureOp(scope ref PrivateBorrow!TortureNode b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    immutable before = c.cancelled;
    with (b.value)
    {
        auto r = tortureNext(seed);
        if (kids !is null)
        {
            tortureCheck(cast(TortureCohort*) kids, c);
            namedCheck(!before || c.cancelled, "torture cancelled is monotonic");
            return typeof(return).done(index + 1);
        }
        bool waiting;
        if (depth < 4 && r % 3 != 0)
        {
            tortureLaunch(seed, depth, kids, count, c);
            if ((r >> 12) % 5 == 0) c.cancelChildren();
            waiting = true;
        }
        if ((r >> 16) % 7 == 0) c.cancelPeers();
        if ((r >> 20) % 9 == 0) c.fail(1000 + index);
        namedCheck(!before || c.cancelled, "torture cancelled is monotonic");
        return waiting ? typeof(return).waitChildren() : typeof(return).done(index + 1);
    }
}

struct TortureRoot
{
    ulong seed;
    void* kids;
    uint count;
    bool polling, live;
    ~this() nothrow @nogc @system
    {
        if (live) atomicFetchAdd!(MemoryOrder.raw)(tortureDtors, 1u);
    }
}
PrivateRootDisposition tortureRoot(scope ref PrivateBorrow!TortureRoot b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        auto r = tortureNext(seed);
        if (kids is null)
        {
            tortureLaunch(seed, 0, kids, count, c);
            polling = (r & 1) != 0;
            if ((r >> 8) % 6 == 0) c.cancelChildren();
            return polling ? PrivateRootDisposition.again : PrivateRootDisposition.waitChildren;
        }
        auto cohort = cast(TortureCohort*) kids;
        if (!cohort.finished(c))
        {
            namedCheck(polling, "torture waiting root resumed before its join");
            if ((r >> 4) % 4 == 0) cohort.cancel(c);
            if ((r >> 8) % 16 == 0)
            {
                c.cancelChildren();
                return PrivateRootDisposition.finishAfterChildren;
            }
            return PrivateRootDisposition.again;
        }
        tortureCheck(cohort, c);
        return PrivateRootDisposition.finish;
    }
}

void cancelTorture(size_t roots, uint seed)
{
    Counts counts;
    atomicStore!(MemoryOrder.raw)(tortureSpawned, 0u);
    atomicStore!(MemoryOrder.raw)(tortureDtors, 0u);
    atomicStore!(MemoryOrder.raw)(tortureTruncated, 0u);
    atomicStore!(MemoryOrder.raw)(tortureCancelled, 0u);
    atomicStore!(MemoryOrder.raw)(tortureFailed, 0u);
    {
        auto farm = AntFarm.create(2, 8, 4, 0, 0, 1, 8192);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        auto workers = new Workers(farm, 4);
        scope(exit) workers.join();
        auto park = PrivateRootPark!(TortureRoot, tortureRoot).create(farm, 64, 8, policy(counts));
        check(park !is null);
        ulong s = seed | 1;
        size_t spawned;
        auto deadline = MonoTime.currTime + 120.seconds;
        while (spawned < roots || park.live)
        {
            while (spawned < roots && park.live < park.capacity)
            {
                check(park.spawn(TortureRoot(tortureNext(s) | 1, null, 0, false, true)) == PrivateSpawnResult.spawned);
                atomicFetchAdd!(MemoryOrder.raw)(tortureSpawned, 1u);
                ++spawned;
            }
            park.pump(token);
            namedCheck(MonoTime.currTime < deadline, "torture drain");
        }
        park.destroy();
    }
    namedCheck(atomicLoad(tortureDtors) == atomicLoad(tortureSpawned), "torture destroyed exactly once");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "torture allocation balance");
    namedCheck(atomicLoad(tortureTruncated) != 0 && atomicLoad(tortureFailed) != 0, "torture exercised truncation and failure");
    writeln("torture: ", roots, " roots, ", atomicLoad(tortureSpawned), " actors, ", atomicLoad(tortureCancelled),
        " cancelled and ", atomicLoad(tortureTruncated), " truncated joined waves, ", atomicLoad(tortureFailed), " with failures");
}

// Cancellation misuse is fatal in debug and release.
PrivateRootDisposition cancelPeersRootStep(scope ref PrivateBorrow!int, scope ref PrivateContext c)
    nothrow @nogc @system
{
    c.cancelPeers();
    fprintf(stderr, "root cancelPeers survived\n");
    return PrivateRootDisposition.finish;
}
PrivateRootDisposition parkCancelAuthorityStep(scope ref PrivateBorrow!int, scope ref PrivateContext c)
    nothrow @nogc @system
{
    auto kids = c.createChildren!int(1);
    namedCheck(kids !is null, "park cancel unauthorized cohort");
    PrivateContext empty;
    kids.cancel(empty);
    fprintf(stderr, "unauthorized park cancel survived\n");
    return PrivateRootDisposition.finish;
}
void cancelDeathRun(alias step)()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(int, step).create(farm, 1);
    check(park.spawn(0) == PrivateSpawnResult.spawned);
    while (park.live) { park.pump(token); while (view.consumeNext()) {} }
}
void cancelDefaultContextDeath(string which)
{
    PrivateContext empty;
    if (which == "cancelled") cast(void) empty.cancelled;
    else if (which == "children") empty.cancelChildren();
    else empty.cancelPeers();
    fprintf(stderr, "default-context cancellation survived\n");
}
void cancelDeath()
{
    version (linux)
    {
        check(diedWithMessage("cancel-peers-root", "antfarm fatal: private cancelPeers: root context"));
        check(diedWithMessage("cancel-park-unauthorized", "antfarm fatal: private child park: unauthorized context"));
        check(diedWithMessage("cancel-default-cancelled", "antfarm fatal: private cancelled: unauthorized context"));
        check(diedWithMessage("cancel-default-children", "antfarm fatal: private cancelChildren: unauthorized context"));
        check(diedWithMessage("cancel-default-peers", "antfarm fatal: private cancelPeers: unauthorized context"));
        writeln("PASS cancelPeers on a root and unauthorized cancellation are fatal");
    }
    else
        writeln("SKIP cancellation death tests: /proc/self/exe re-exec requires Linux");
}

void phaseCancel()
{
    cancelC1();
    cancelC2();
    cancelC3();
    cancelC4();
    cancelC5();
    cancelC6();
    version (AntfarmActorTestHooks) cancelC7();
    cancelC8();
    version (AntfarmActorTestHooks) cancelC9();
    foreach (mode; [C10Mode.untracked, C10Mode.failed, C10Mode.cancelled, C10Mode.failedAndCancelled])
        cancelC10(mode);
    cancelC11();
    cancelC12();
    cancelC13();
    writeln("PASS private in-tree cancellation C1-C13");
    version (TSan) cancelTorture(300, 17);
    else cancelTorture(3000, 17);
    writeln("PASS private cancellation torture");
}

// ---------------------------------------------------------------------------
// F3 root tracking, deadlines and X1a (design 170 §5, tests §12.3). Each
// deterministic test names the mutation of actors/private_actor.d it catches;
// the schedule below aborts only when that mutation is applied (normal runs pass).
// Tracked parks hold every terminal root until takeOutcome, so each drain
// pumps and takes outcomes; pumping alone never brings live to zero (T7).

PrivateRootOptions trackedOptions(size_t batchLimit = defaultPrivateBatchLimit)
{
    PrivateRootOptions options;
    options.batchLimit = batchLimit;
    options.tracking = true;
    return options;
}

// Pump, run published callbacks inline, and take every outcome until no
// root is live. Outcomes are appended to `taken` in take order.
void drainTracked(P)(P* park, ref Token token, ref ConsumerView view,
    ref PrivateRootOutcome[] taken, string name)
{
    auto deadline = MonoTime.currTime + 20.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        PrivateRootOutcome o;
        while (park.takeOutcome(o)) taken ~= o;
        namedCheck(MonoTime.currTime < deadline, name);
    }
}

// The same with the lockstep consumer, so no callback runs on the emitter.
void lockstepRunTracked(P)(P* park, ref Token token, Lockstep* l,
    ref PrivateRootOutcome[] taken, string name)
{
    auto deadline = MonoTime.currTime + 10.seconds;
    while (park.live)
    {
        park.pump(token);
        lockstepDrain(l);
        PrivateRootOutcome o;
        while (park.takeOutcome(o)) taken ~= o;
        namedCheck(MonoTime.currTime < deadline, name);
    }
    park.destroy();
}

// Thread-local: only the designated foreign canceller pauses in T1's hook.
bool rtIsCanceller;
shared uint rtPaused, rtResume, rtDone, rtArmed;
shared ubyte rtResult;
__gshared PrivateRootHandle rtHandle;

// T1. A foreign canceller loads root A's LIVE(g) word and pauses at
// privateRootCancelBeforeCas. Meanwhile the emitter collects A, takes its
// outcome and respawns the slot as root B (LIVE(g+1)). On resume the CAS
// fails and the reload sees another generation: staleHandle. B never
// observes cancelled and completes.
// Mutation T1a: the canceller does a load+store instead of the CAS
// (`atomicStore(record_.control, w | cancelled | cause); return requested;`).
// It overwrites B's word: "t1 resumed canceller is stale" fails.
// Mutation T1b: takeOutcome does not bump the generation (stores
// `generation << shift`). B's LIVE(g) word equals the stale expected word,
// the CAS succeeds (ABA): "t1 resumed canceller is stale" fails.
struct T1Root { ubyte kind; }
shared uint t1Stop, t1SawCancelled, t1BActivations;
version (AntfarmActorTestHooks)
void t1Hook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
{
    if (point != ActorTestPoint.privateRootCancelBeforeCas || !rtIsCanceller) return;
    if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&rtArmed, 1u, 0u)) return;
    atomicStore!(MemoryOrder.rel)(rtPaused, 1u);
    while (atomicLoad!(MemoryOrder.acq)(rtResume) == 0) pause();
}
PrivateRootDisposition t1Root(scope ref PrivateBorrow!T1Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    if (b.value.kind == 0) return PrivateRootDisposition.finish;
    atomicFetchAdd!(MemoryOrder.raw)(t1BActivations, 1u);
    if (c.cancelled) atomicStore!(MemoryOrder.raw)(t1SawCancelled, 1u);
    return atomicLoad!(MemoryOrder.acq)(t1Stop) != 0
        ? PrivateRootDisposition.finish : PrivateRootDisposition.again;
}
version (AntfarmActorTestHooks)
void rootTrackingT1()
{
    atomicStore(rtPaused, 0u);
    atomicStore(rtResume, 0u);
    atomicStore(rtDone, 0u);
    atomicStore(rtArmed, 1u);
    atomicStore(t1Stop, 0u);
    atomicStore(t1SawCancelled, 0u);
    atomicStore(t1BActivations, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    setActorTestHook(&t1Hook);
    scope(exit) setActorTestHook(null);
    auto park = PrivateRootPark!(T1Root, t1Root).create(farm, 1, trackedOptions());
    PrivateRootHandle a;
    check(park.spawn(T1Root(0), a) == PrivateSpawnResult.spawned);
    rtHandle = a;
    auto canceller = new Thread({
        rtIsCanceller = true;
        atomicStore!(MemoryOrder.rel)(rtResult, cast(ubyte) rtHandle.requestCancel());
        atomicStore!(MemoryOrder.rel)(rtDone, 1u);
    });
    canceller.start();
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad!(MemoryOrder.acq)(rtPaused) == 0)
        namedCheck(MonoTime.currTime < deadline, "t1 canceller paused");
    PrivateRootOutcome o;
    while (!park.takeOutcome(o))
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "t1 collect A");
    }
    namedCheck(o.root == a && o.status == PrivateOutcomeStatus.completed
        && o.cause == PrivateCancelCause.none, "t1 A completed");
    PrivateRootHandle b;
    check(park.spawn(T1Root(1), b) == PrivateSpawnResult.spawned);
    namedCheck(b.index == a.index, "t1 slot recycled");
    while (atomicLoad(t1BActivations) < 2)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "t1 B running");
    }
    atomicStore!(MemoryOrder.rel)(rtResume, 1u);
    canceller.join();
    namedCheck(atomicLoad(rtDone) == 1
        && atomicLoad(rtResult) == PrivateCancelResult.staleHandle, "t1 resumed canceller is stale");
    namedCheck(b != a, "t1 new generation");
    atomicStore!(MemoryOrder.rel)(t1Stop, 1u);
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t1 drain B");
    namedCheck(a.requestCancel() == PrivateCancelResult.staleHandle
        && b.requestCancel() == PrivateCancelResult.staleHandle, "t1 taken handles are stale");
    park.destroy(); // handles are undefined from here
    namedCheck(taken.length == 1 && taken[0].root == b
        && taken[0].status == PrivateOutcomeStatus.completed
        && taken[0].cause == PrivateCancelCause.none, "t1 B completed");
    namedCheck(atomicLoad(t1SawCancelled) == 0, "t1 B never cancelled");
}

// T2. Cancel against the emitter's terminal CAS, in both orders.
// Order 1: the emitter pauses at privateRootTerminalBeforeCas after loading
// the LIVE word; a foreign thread's requestCancel succeeds (requested); the
// emitter's CAS fails, re-reads and records cancelled.
// Order 2: the request lands after the terminal CAS, while take is paused
// before the VACANT store: finished, outcome completed; after take, stale.
// Never {requested, completed}.
// Mutation T2: the terminal transition is a plain store computed from the
// first load (`atomicStore(record.control, w & ~controlLive)` and no loop).
// Order 1 then yields {requested, completed}: "t2 cancel before terminal CAS".
shared uint t2Point; // ActorTestPoint + 1 to pause at; 0 disarms
version (AntfarmActorTestHooks)
void t2Hook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
{
    if (point + 1 != atomicLoad!(MemoryOrder.raw)(t2Point)) return;
    if (!cas!(MemoryOrder.raw, MemoryOrder.raw)(&t2Point, point + 1u, 0u)) return;
    atomicStore!(MemoryOrder.rel)(rtPaused, 1u);
    while (atomicLoad!(MemoryOrder.acq)(rtDone) == 0) pause();
}
PrivateRootDisposition t2Root(scope ref PrivateBorrow!int, scope ref PrivateContext)
    nothrow @nogc @system
{
    return PrivateRootDisposition.finish;
}
version (AntfarmActorTestHooks)
PrivateRootOutcome t2Run(ActorTestPoint point, ref PrivateCancelResult result)
{
    atomicStore(rtPaused, 0u);
    atomicStore(rtDone, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    setActorTestHook(&t2Hook);
    scope(exit) setActorTestHook(null);
    auto park = PrivateRootPark!(int, t2Root).create(farm, 1, trackedOptions());
    PrivateRootHandle h;
    check(park.spawn(0, h) == PrivateSpawnResult.spawned);
    rtHandle = h;
    atomicStore!(MemoryOrder.rel)(t2Point, point + 1u);
    auto canceller = new Thread({
        auto deadline = MonoTime.currTime + 10.seconds;
        while (atomicLoad!(MemoryOrder.acq)(rtPaused) == 0)
            namedCheck(MonoTime.currTime < deadline, "t2 emitter paused");
        atomicStore!(MemoryOrder.rel)(rtResult, cast(ubyte) rtHandle.requestCancel());
        atomicStore!(MemoryOrder.rel)(rtDone, 1u);
    });
    canceller.start();
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t2 drain");
    canceller.join();
    namedCheck(h.requestCancel() == PrivateCancelResult.staleHandle, "t2 taken handle is stale");
    park.destroy();
    namedCheck(atomicLoad(t2Point) == 0 && taken.length == 1 && taken[0].root == h, "t2 hook ran");
    result = cast(PrivateCancelResult) atomicLoad(rtResult);
    return taken[0];
}
version (AntfarmActorTestHooks)
void rootTrackingT2()
{
    PrivateCancelResult r;
    auto o = t2Run(ActorTestPoint.privateRootTerminalBeforeCas, r);
    namedCheck(r == PrivateCancelResult.requested && o.status == PrivateOutcomeStatus.cancelled
        && o.cause == PrivateCancelCause.requested, "t2 cancel before terminal CAS");
    o = t2Run(ActorTestPoint.privateTakeBeforeVacantStore, r);
    namedCheck(r == PrivateCancelResult.finished && o.status == PrivateOutcomeStatus.completed
        && o.cause == PrivateCancelCause.none, "t2 cancel after terminal CAS");
}

// T3. A deadline already past at spawn. The root starts a 64-member wave at
// batchLimit 4 and waits without checking; the emitter's first cancel check
// observes the deadline, converts it, and truncates at zero.
// Mutation T3: drop the records branch from scopeCancelled. The emitter
// publishes all 64: "t3 first emit truncates".
shared uint t3MemberRuns;
void t3Op(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(t3MemberRuns, 1u);
}
struct T3App { size_t participants, dispatched; bool waveCancelled, rootCancelled, saw; }
struct T3Root { PrivateChildPark!uint* kids; T3App* app; }
PrivateRootDisposition t3Root(scope ref PrivateBorrow!T3Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (kids is null)
        {
            kids = c.createChildren!uint(64, 4);
            namedCheck(kids !is null, "t3 cohort");
            foreach (i; 0 .. 64) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "t3 spawn");
            namedCheck(kids.start!t3Op(c) == PrivateStartResult.started, "t3 start");
            return PrivateRootDisposition.waitChildren;
        }
        auto o = kids.outcome(c);
        app.participants = o.participants;
        app.dispatched = o.dispatched;
        app.waveCancelled = o.cancelled;
        app.rootCancelled = c.cancelled;
        app.saw = true;
        return PrivateRootDisposition.finish;
    }
}
void rootTrackingT3()
{
    T3App app;
    atomicStore(t3MemberRuns, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(T3Root, t3Root).create(farm, 1, trackedOptions());
    PrivateRootHandle h;
    check(park.spawn(T3Root(null, &app), h, MonoTime.currTime - 1.seconds) == PrivateSpawnResult.spawned);
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t3 drain");
    park.destroy();
    namedCheck(app.saw && app.participants == 64 && app.dispatched == 0
        && atomicLoad(t3MemberRuns) == 0, "t3 first emit truncates");
    namedCheck(!app.waveCancelled && app.rootCancelled, "t3 root scope, not the wave, is cancelled");
    namedCheck(taken.length == 1 && taken[0].root == h && taken[0].status == PrivateOutcomeStatus.cancelled
        && taken[0].cause == PrivateCancelCause.deadline, "t3 cause deadline");
}

// T4. Without a deadline (spawn(T), or MonoTime.max) no check reads the
// clock: not the root's, not its members', not the emitter's. A control
// root with a deadline an hour away reads it and still completes.
// Mutation T4: ignore the armed flag in rootCancelledNow (drop
// `!(w & controlArmed) ||`): "t4 no clock read without a deadline".
shared uint t4Checks, t4SawCancelled;
void t4Op(scope ref PrivateBorrow!uint, scope ref PrivateContext c) nothrow @nogc @system
{
    if (c.cancelled) atomicStore!(MemoryOrder.raw)(t4SawCancelled, 1u);
    atomicFetchAdd!(MemoryOrder.raw)(t4Checks, 1u);
}
struct T4Root { PrivateChildPark!uint* kids; uint rounds; }
PrivateRootDisposition t4Root(scope ref PrivateBorrow!T4Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (c.cancelled) atomicStore!(MemoryOrder.raw)(t4SawCancelled, 1u);
        atomicFetchAdd!(MemoryOrder.raw)(t4Checks, 1u);
        if (kids is null)
        {
            kids = c.createChildren!uint(8, 2);
            namedCheck(kids !is null, "t4 cohort");
            foreach (i; 0 .. 8) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "t4 spawn");
        }
        if (!kids.finished(c)) return PrivateRootDisposition.again;
        if (rounds++ == 3) return PrivateRootDisposition.finish;
        namedCheck(kids.start!t4Op(c) == PrivateStartResult.started, "t4 start");
        return PrivateRootDisposition.again;
    }
}
ulong t4Run(bool withDeadline)
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(T4Root, t4Root).create(farm, 3, trackedOptions());
    PrivateRootHandle h;
    immutable before = atomicLoad(privateClockReads);
    if (withDeadline)
        check(park.spawn(T4Root.init, h, MonoTime.currTime + 3600.seconds) == PrivateSpawnResult.spawned);
    else
    {
        check(park.spawn(T4Root.init) == PrivateSpawnResult.spawned);
        check(park.spawn(T4Root.init, h) == PrivateSpawnResult.spawned);
        check(park.spawn(T4Root.init, h, MonoTime.max) == PrivateSpawnResult.spawned);
    }
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t4 drain");
    park.destroy();
    foreach (o; taken)
        namedCheck(o.status == PrivateOutcomeStatus.completed && o.cause == PrivateCancelCause.none, "t4 completed");
    namedCheck(taken.length == (withDeadline ? 1 : 3), "t4 outcomes");
    return atomicLoad(privateClockReads) - before;
}
void rootTrackingT4()
{
    atomicStore(t4SawCancelled, 0u);
    atomicStore(t4Checks, 0u);
    namedCheck(t4Run(false) == 0, "t4 no clock read without a deadline");
    namedCheck(atomicLoad(t4Checks) >= 3 * (3 * 8 + 4), "t4 checks ran");
    namedCheck(t4Run(true) != 0, "t4 armed deadline reads the clock");
    namedCheck(atomicLoad(t4SawCancelled) == 0, "t4 nothing cancelled");
}

// T5. Untaken outcomes hold their slots: a full park of terminal roots
// returns capacityFull until one is taken. destroy with an untaken outcome
// is fatal (death test tracked-destroy-untaken).
// Mutation T5: recycle at collection (drop the `return` after retainOutcome
// in reclaim): "t5 capacityFull while outcomes held".
shared uint t5Finished;
PrivateRootDisposition t5Root(scope ref PrivateBorrow!int, scope ref PrivateContext)
    nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(t5Finished, 1u);
    return PrivateRootDisposition.finish;
}
void rootTrackingT5()
{
    atomicStore(t5Finished, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(int, t5Root).create(farm, 2, trackedOptions());
    check(park.spawn(1) == PrivateSpawnResult.spawned && park.spawn(2) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(t5Finished) < 2)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "t5 roots finish");
    }
    park.collectCompleted();
    namedCheck(park.live == 2 && park.spawn(3) == PrivateSpawnResult.capacityFull,
        "t5 capacityFull while outcomes held");
    PrivateRootOutcome o;
    namedCheck(park.takeOutcome(o) && park.live == 1, "t5 take frees one slot");
    namedCheck(park.spawn(3) == PrivateSpawnResult.spawned, "t5 spawn into the taken slot");
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t5 drain");
    park.destroy();
    namedCheck(taken.length == 2, "t5 remaining outcomes");
}

// T6. X1a edge: each root's final activation writes application storage
// plainly on a worker; the emitter reads it right after takeOutcome returns
// that root. TSan checks the edge on both terminal paths (leaf finish and
// joined finish) and with consumer-side reclamation. Edge check, no mutation.
enum t6Roots = 2000;
__gshared ulong[t6Roots] t6Output;
void t6Op(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system {}
struct T6Root { uint id; PrivateChildPark!uint* kids; }
PrivateRootDisposition t6Root(scope ref PrivateBorrow!T6Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        if (id % 2 == 0 && kids is null)
        {
            kids = c.createChildren!uint(4, 2);
            namedCheck(kids !is null, "t6 cohort");
            foreach (i; 0 .. 4) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "t6 spawn");
            namedCheck(kids.start!t6Op(c) == PrivateStartResult.started, "t6 start");
            return PrivateRootDisposition.waitChildren;
        }
        t6Output[id] = id * 7919UL + 1;
        return PrivateRootDisposition.finish;
    }
}
void rootTrackingT6(PrivateRootReclamation reclaim)()
{
    t6Output[] = 0;
    auto farm = AntFarm.create(4, 8, 4, 0, 0, 1, 65536);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 4);
    scope(exit) workers.join();
    auto park = PrivateRootPark!(T6Root, t6Root, reclaim).create(farm, 64, trackedOptions(16));
    uint[64] slotId;
    uint spawned, taken;
    auto deadline = MonoTime.currTime + 60.seconds;
    while (taken < t6Roots)
    {
        while (spawned < t6Roots && park.live < park.capacity)
        {
            PrivateRootHandle h;
            check(park.spawn(T6Root(spawned), h) == PrivateSpawnResult.spawned);
            slotId[h.index] = spawned++;
        }
        park.pump(token);
        PrivateRootOutcome o;
        while (park.takeOutcome(o))
        {
            immutable id = slotId[o.root.index];
            namedCheck(o.status == PrivateOutcomeStatus.completed, "t6 completed");
            namedCheck(t6Output[id] == id * 7919UL + 1, "t6 root output visible after takeOutcome");
            ++taken;
        }
        namedCheck(MonoTime.currTime < deadline, "t6 drain");
    }
    park.destroy();
}

// T7. Shutdown with cancelAll and 1000 roots at mixed states, run inline so
// the states are exact. Before cancelAll, kinds 0 and 1 are terminal and
// their outcomes held; the others are live. cancelAll skips held outcomes,
// returns the live count, and records cause deadline for the armed root
// whose deadline passed. Pumping alone then finishes every computation but
// leaves live at 1000; taking outcomes drains it.
// kind 0 finish: completed. kind 1 fail(1000+i), finish: failed.
// kind 2 polls until cancelled: cancelled(requested).
// kind 3 polls with an active child wave; on cancel, cancelChildren and
// finishAfterChildren: cancelled(requested).
// kind 4 fails twice (first code wins), polls until cancelled: failed.
// kind 5 expired deadline, polls without checking until cancelAll: cancelled(deadline).
// Mutation T7a: drop the takeOutcome loop from the recipe: "t7 drained"
// times out (documented; pumping alone is checked by "t7 pumping holds outcomes").
// Mutation T7b: cancelled outranks failed in retainOutcome: "t7 statuses".
// Mutation T7c: root fail keeps the last code: "t7 statuses" (kind 4).
enum t7Roots = 1000;
shared uint t7Final, t7AfterCancelAll;
void t7Op(scope ref PrivateBorrow!uint, scope ref PrivateContext) nothrow @nogc @system {}
struct T7Root { uint id; PrivateChildPark!uint* kids; uint activations; }
PrivateRootDisposition t7Root(scope ref PrivateBorrow!T7Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    with (b.value)
    {
        immutable kind = id % 6;
        ++activations;
        final switch (kind)
        {
            case 0:
                atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                return PrivateRootDisposition.finish;
            case 1:
                c.fail(1000 + id);
                atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                return PrivateRootDisposition.finish;
            case 2:
                if (!c.cancelled) return PrivateRootDisposition.again;
                atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                return PrivateRootDisposition.finish;
            case 3:
                if (kids is null)
                {
                    kids = c.createChildren!uint(4, 1);
                    namedCheck(kids !is null, "t7 cohort");
                    foreach (i; 0 .. 4) namedCheck(kids.spawn(c, cast(uint) i) == PrivateSpawnResult.spawned, "t7 spawn");
                }
                if (c.cancelled)
                {
                    atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                    c.cancelChildren();
                    return PrivateRootDisposition.finishAfterChildren;
                }
                if (kids.finished(c))
                    namedCheck(kids.start!t7Op(c) == PrivateStartResult.started, "t7 start");
                return PrivateRootDisposition.again;
            case 4:
                if (activations == 1) c.fail(2000 + id);
                if (activations == 2) c.fail(3000 + id);
                if (!c.cancelled) return PrivateRootDisposition.again;
                atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                return PrivateRootDisposition.finish;
            case 5:
                if (atomicLoad!(MemoryOrder.raw)(t7AfterCancelAll) == 0 || !c.cancelled)
                    return PrivateRootDisposition.again;
                atomicFetchAdd!(MemoryOrder.raw)(t7Final, 1u);
                return PrivateRootDisposition.finish;
        }
    }
}
void rootTrackingT7()
{
    atomicStore(t7Final, 0u);
    atomicStore(t7AfterCancelAll, 0u);
    auto farm = AntFarm.create(4, 8, 1, 0, 0, 1, 65536);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(T7Root, t7Root).create(farm, t7Roots, trackedOptions(64));
    auto handles = new PrivateRootHandle[t7Roots];
    auto past = MonoTime.currTime - 1.seconds;
    size_t live;
    foreach (i; 0 .. t7Roots)
    {
        check(park.spawn(T7Root(cast(uint) i), handles[i], i % 6 == 5 ? past : MonoTime.max)
            == PrivateSpawnResult.spawned);
        if (i % 6 > 1) ++live;
    }
    enum terminalEarly = (t7Roots + 5) / 6 + (t7Roots + 4) / 6; // kinds 0 and 1
    auto deadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad(t7Final) < terminalEarly)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "t7 early roots");
    }
    foreach (_; 0 .. 4) { park.pump(token); while (view.consumeNext()) {} }
    park.collectCompleted();
    namedCheck(park.live == t7Roots, "t7 early outcomes held");
    namedCheck(park.cancelAll() == live, "t7 cancelAll counts live unfinished roots");
    namedCheck(park.cancelAll() == 0, "t7 cancelAll is idempotent");
    atomicStore!(MemoryOrder.raw)(t7AfterCancelAll, 1u);
    while (atomicLoad(t7Final) < t7Roots)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, "t7 computations finish");
    }
    foreach (_; 0 .. 16) { park.pump(token); while (view.consumeNext()) {} }
    namedCheck(park.live == t7Roots, "t7 pumping holds outcomes");
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t7 drained");
    park.destroy();
    namedCheck(taken.length == t7Roots, "t7 every root taken once");
    auto seen = new bool[t7Roots];
    foreach (o; taken)
    {
        size_t id = t7Roots;
        foreach (i, h; handles) if (h == o.root) { id = i; break; }
        namedCheck(id < t7Roots && !seen[id], "t7 outcome identity");
        seen[id] = true;
        bool ok;
        final switch (id % 6)
        {
            case 0: ok = o.status == PrivateOutcomeStatus.completed && o.cause == PrivateCancelCause.none; break;
            case 1: ok = o.status == PrivateOutcomeStatus.failed && o.code == 1000 + id; break;
            case 2: case 3:
                ok = o.status == PrivateOutcomeStatus.cancelled && o.cause == PrivateCancelCause.requested; break;
            case 4: ok = o.status == PrivateOutcomeStatus.failed && o.code == 2000 + id
                && o.cause == PrivateCancelCause.none; break;
            case 5: ok = o.status == PrivateOutcomeStatus.cancelled && o.cause == PrivateCancelCause.deadline; break;
        }
        namedCheck(ok, "t7 statuses");
    }
    // Held kinds 0 and 1 were collected first, in collection order.
    foreach (o; taken[0 .. terminalEarly])
    {
        size_t id;
        foreach (i, h; handles) if (h == o.root) { id = i; break; }
        namedCheck(id % 6 <= 1, "t7 outcome FIFO order");
    }
}

// T8. Handles: equality, density of index, recycling, default handles.
// Behavioural.
PrivateRootDisposition t8Root(scope ref PrivateBorrow!int, scope ref PrivateContext)
    nothrow @nogc @system
{
    return PrivateRootDisposition.finish;
}
void rootTrackingT8()
{
    PrivateRootHandle none;
    namedCheck(!none.valid && none == PrivateRootHandle.init
        && none.requestCancel() == PrivateCancelResult.staleHandle, "t8 default handle");
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(int, t8Root).create(farm, 4, trackedOptions());
    PrivateRootHandle[4] h;
    bool[4] used;
    foreach (i; 0 .. 4)
    {
        check(park.spawn(cast(int) i, h[i]) == PrivateSpawnResult.spawned);
        namedCheck(h[i].valid && h[i].index < 4 && !used[h[i].index], "t8 dense distinct index");
        used[h[i].index] = true;
        auto copy = h[i];
        namedCheck(copy == h[i], "t8 copies are equal");
        foreach (j; 0 .. i) namedCheck(h[j] != h[i], "t8 distinct roots differ");
    }
    PrivateRootHandle extra;
    namedCheck(park.spawn(9, extra) == PrivateSpawnResult.capacityFull && !extra.valid, "t8 full park issues no handle");
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t8 drain");
    namedCheck(taken.length == 4, "t8 outcomes");
    foreach (o; taken)
    {
        size_t matches;
        foreach (x; h) if (x == o.root) ++matches;
        namedCheck(matches == 1 && o.status == PrivateOutcomeStatus.completed, "t8 outcome names its root");
    }
    PrivateRootHandle again;
    check(park.spawn(4, again) == PrivateSpawnResult.spawned);
    size_t sameIndex;
    foreach (x; h)
    {
        namedCheck(x != again && x.requestCancel() == PrivateCancelResult.staleHandle, "t8 recycled slot, new identity");
        if (x.index == again.index) ++sameIndex;
    }
    namedCheck(sameIndex == 1, "t8 recycled index");
    taken = null;
    drainTracked(park, token, view, taken, "t8 drain again");
    park.destroy();
}

// T9 (death tests root-generation-wrap). A seam seeds the root's generation.
// At 2^48 - 2 the take succeeds and the slot's next generation is the last
// one; taking that generation is fatal before the store would wrap.
// Mutation T9: delete the wrap check; the second take stores VACANT(0) and
// returns: "root generation wrap survived".
struct T9Root { ulong generation; }
PrivateRootDisposition t9Root(scope ref PrivateBorrow!T9Root b, scope ref PrivateContext c)
    nothrow @nogc @system
{
    version (AntfarmActorTestHooks)
        if (b.value.generation != 0) privateTestSetRootGeneration(c, b.value.generation);
    return PrivateRootDisposition.finish;
}
void rootGenerationWrapDeath()
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(T9Root, t9Root).create(farm, 1, trackedOptions());
    check(park.spawn(T9Root((1UL << 48) - 2)) == PrivateSpawnResult.spawned);
    PrivateRootOutcome[] taken;
    drainTracked(park, token, view, taken, "t9 first generation");
    check(taken.length == 1 && park.spawn(T9Root(0)) == PrivateSpawnResult.spawned);
    drainTracked(park, token, view, taken, "t9 last generation");
    fprintf(stderr, "root generation wrap survived\n");
}

// Tracking-only API on an untracked park, invalid handles and untaken
// outcomes are fatal in debug and release.
PrivateRootDisposition finishStep(scope ref PrivateBorrow!int, scope ref PrivateContext)
    nothrow @nogc @system
{
    return PrivateRootDisposition.finish;
}
void rootTrackingMisuse(string which)
{
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto untracked = PrivateRootPark!(int, finishStep).create(farm, 1);
    PrivateRootHandle h;
    PrivateRootOutcome o;
    if (which == "spawn-handle") untracked.spawn(0, h);
    else if (which == "take") untracked.takeOutcome(o);
    else if (which == "cancel-all") untracked.cancelAll();
    else if (which == "handle-index") cast(void) h.index;
    else if (which == "destroy-untaken")
    {
        auto park = PrivateRootPark!(int, finishStep).create(farm, 1, trackedOptions());
        check(park.spawn(0) == PrivateSpawnResult.spawned);
        foreach (_; 0 .. 8) { park.pump(token); while (view.consumeNext()) {} }
        park.destroy();
    }
    fprintf(stderr, "root tracking misuse survived\n");
}
void rootTrackingDeath()
{
    version (linux)
    {
        check(diedWithMessage("root-tracking-misuse:spawn-handle",
            "antfarm fatal: private root spawn handle: untracked park"));
        check(diedWithMessage("root-tracking-misuse:take", "antfarm fatal: private root takeOutcome: untracked park"));
        check(diedWithMessage("root-tracking-misuse:cancel-all", "antfarm fatal: private root cancelAll: untracked park"));
        check(diedWithMessage("root-tracking-misuse:handle-index", "antfarm fatal: private root handle: invalid"));
        check(diedWithMessage("root-tracking-misuse:destroy-untaken", "antfarm fatal: destroy nonempty private root park"));
        version (AntfarmActorTestHooks)
            check(diedWithMessage("root-generation-wrap", "antfarm fatal: private root generation wrap"));
        writeln("PASS tracking-only API on untracked parks, invalid handles, untaken outcomes and generation wrap are fatal");
    }
    else
        writeln("SKIP root tracking death tests: /proc/self/exe re-exec requires Linux");
}

// Tracked torture (§12.5): the cancellation torture's trees under tracked
// roots with random deadlines (none, past, within 2 ms, an hour away), root
// fail, a foreign thread cancelling random published handles (most of them
// stale by then) and occasional cancelAll. The emitter pumps and takes
// outcomes. Invariants, beyond cancelTorture's: one outcome per root, named by
// its handle; failed exactly when the root failed, with its code; cancelled
// carries a cause, and deadline only for a root that had one; a root that
// observed cancelled never completes; a `requested` handle result never ends
// completed; live reaches zero only by taking outcomes.
struct TrackedTortureRoot
{
    ulong seed;
    void* kids;
    uint count, id;
    bool polling, live;
    ~this() nothrow @nogc @system
    {
        if (live) atomicFetchAdd!(MemoryOrder.raw)(tortureDtors, 1u);
    }
}
__gshared bool[] ttFailed, ttSawCancelled;
PrivateRootDisposition trackedTortureRoot(scope ref PrivateBorrow!TrackedTortureRoot b,
    scope ref PrivateContext c) nothrow @nogc @system
{
    with (b.value)
    {
        auto r = tortureNext(seed);
        immutable cancelled = c.cancelled;
        if (cancelled) ttSawCancelled[id] = true;
        if ((r >> 24) % 8 == 0)
        {
            c.fail(5000 + id);
            ttFailed[id] = true;
        }
        if (kids is null)
        {
            tortureLaunch(seed, 0, kids, count, c);
            polling = (r & 1) != 0;
            if (cancelled || (r >> 8) % 6 == 0) c.cancelChildren();
            return polling ? PrivateRootDisposition.again : PrivateRootDisposition.waitChildren;
        }
        auto cohort = cast(TortureCohort*) kids;
        if (!cohort.finished(c))
        {
            namedCheck(polling, "tracked torture waiting root resumed before its join");
            if (cancelled)
            {
                c.cancelChildren();
                return PrivateRootDisposition.finishAfterChildren;
            }
            if ((r >> 4) % 4 == 0) cohort.cancel(c);
            return PrivateRootDisposition.again;
        }
        tortureCheck(cohort, c);
        return PrivateRootDisposition.finish;
    }
}
void trackedTorture(size_t roots, uint seed)
{
    Counts counts;
    atomicStore!(MemoryOrder.raw)(tortureSpawned, 0u);
    atomicStore!(MemoryOrder.raw)(tortureDtors, 0u);
    atomicStore!(MemoryOrder.raw)(tortureTruncated, 0u);
    atomicStore!(MemoryOrder.raw)(tortureCancelled, 0u);
    atomicStore!(MemoryOrder.raw)(tortureFailed, 0u);
    ttFailed = new bool[roots];
    ttSawCancelled = new bool[roots];
    auto hasDeadline = new bool[roots];
    auto status = new PrivateOutcomeStatus[roots];
    auto requested = new shared(ubyte)[roots];
    auto handles = new PrivateRootHandle[roots];
    shared size_t published;
    shared uint stop;
    size_t[PrivateOutcomeStatus.max + 1] byStatus;
    size_t byDeadline, cancelAlls;
    {
        auto farm = AntFarm.create(2, 8, 4, 0, 0, 1, 8192);
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        auto workers = new Workers(farm, 4);
        scope(exit) workers.join();
        PrivateRootOptions options = trackedOptions(8);
        options.allocator = policy(counts);
        auto park = PrivateRootPark!(TrackedTortureRoot, trackedTortureRoot).create(farm, 64, options);
        check(park !is null);
        auto canceller = new Thread({
            ulong r = seed * 7919UL | 1;
            while (atomicLoad!(MemoryOrder.acq)(stop) == 0)
            {
                immutable n = atomicLoad!(MemoryOrder.acq)(published);
                foreach (_; 0 .. 2000) pause();
                if (n == 0) continue;
                immutable id = cast(size_t) (tortureNext(r) % n);
                if (handles[id].requestCancel() == PrivateCancelResult.requested)
                    atomicStore!(MemoryOrder.raw)(requested[id], cast(ubyte) 1);
            }
        });
        canceller.start();
        auto slotId = new size_t[park.capacity];
        ulong s = seed | 1;
        size_t spawned, taken;
        auto deadline = MonoTime.currTime + 120.seconds;
        while (taken < roots)
        {
            while (spawned < roots && park.live < park.capacity)
            {
                immutable r = tortureNext(s);
                auto now = MonoTime.currTime;
                MonoTime due = MonoTime.max;
                switch (r % 10)
                {
                    case 5: due = now - 1.seconds; break;
                    case 6: case 7: due = now + usecs(cast(long) ((r >> 8) % 2000)); break;
                    case 8: case 9: due = now + 3600.seconds; break;
                    default: break;
                }
                hasDeadline[spawned] = due != MonoTime.max;
                PrivateRootHandle h;
                check(park.spawn(TrackedTortureRoot(tortureNext(s) | 1, null, 0, cast(uint) spawned, false, true),
                    h, due) == PrivateSpawnResult.spawned);
                atomicFetchAdd!(MemoryOrder.raw)(tortureSpawned, 1u);
                handles[spawned] = h;
                slotId[h.index] = spawned;
                atomicStore!(MemoryOrder.rel)(published, ++spawned);
            }
            if (tortureNext(s) % 2048 == 0)
            {
                park.cancelAll();
                ++cancelAlls;
            }
            park.pump(token);
            PrivateRootOutcome o;
            while (park.takeOutcome(o))
            {
                immutable id = slotId[o.root.index];
                namedCheck(o.root == handles[id], "tracked torture outcome identity");
                namedCheck((o.status == PrivateOutcomeStatus.failed) == ttFailed[id]
                    && (!ttFailed[id] || o.code == 5000 + id), "tracked torture failure");
                namedCheck((o.status == PrivateOutcomeStatus.cancelled) == (o.cause != PrivateCancelCause.none),
                    "tracked torture cause");
                namedCheck(o.cause != PrivateCancelCause.deadline || hasDeadline[id], "tracked torture deadline cause");
                namedCheck(!ttSawCancelled[id] || o.status != PrivateOutcomeStatus.completed,
                    "tracked torture observed cancel never completes");
                status[id] = o.status;
                ++byStatus[o.status];
                if (o.cause == PrivateCancelCause.deadline) ++byDeadline;
                ++taken;
            }
            namedCheck(MonoTime.currTime < deadline, "tracked torture drain");
        }
        namedCheck(park.live == 0, "tracked torture live reaches zero");
        atomicStore!(MemoryOrder.rel)(stop, 1u);
        canceller.join();
        park.destroy();
    }
    foreach (id; 0 .. roots)
        namedCheck(atomicLoad(requested[id]) == 0 || status[id] != PrivateOutcomeStatus.completed,
            "tracked torture requested never completes");
    namedCheck(atomicLoad(tortureDtors) == atomicLoad(tortureSpawned), "tracked torture destroyed exactly once");
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "tracked torture allocation balance");
    namedCheck(byStatus[PrivateOutcomeStatus.failed] != 0 && byStatus[PrivateOutcomeStatus.cancelled] != 0
        && byStatus[PrivateOutcomeStatus.completed] != 0 && byDeadline != 0, "tracked torture exercised every status");
    writeln("tracked torture: ", roots, " roots (", byStatus[PrivateOutcomeStatus.completed], " completed, ",
        byStatus[PrivateOutcomeStatus.failed], " failed, ", byStatus[PrivateOutcomeStatus.cancelled], " cancelled, ",
        byDeadline, " by deadline; ", cancelAlls, " cancelAll), ", atomicLoad(tortureSpawned), " actors, ",
        atomicLoad(tortureTruncated), " truncated joined waves");
}

void phaseRootTracking()
{
    version (AntfarmActorTestHooks)
    {
        rootTrackingT1();
        rootTrackingT2();
    }
    rootTrackingT3();
    rootTrackingT4();
    rootTrackingT5();
    rootTrackingT6!(PrivateRootReclamation.emitter)();
    rootTrackingT6!(PrivateRootReclamation.consumer)();
    rootTrackingT7();
    rootTrackingT8();
    writeln("PASS private root tracking T1-T8");
    version (TSan) trackedTorture(300, 23);
    else trackedTorture(3000, 23);
    writeln("PASS private tracked torture");
}

// G4: the derived default root work-cost hint. A table below the small
// threshold is claimed wholesale by shard 0 in ceil(count / (MAX_CHUNK >>
// hint)) claims, so the hint determines how many consumers can share one
// small root table; a table at least the threshold wide is sharded and
// keeps the cheapest class.
void rootCostHintRule()
{
    static uint claimCount(size_t count, uint hint) pure nothrow @nogc @safe
    {
        immutable chunk = MAX_CHUNK >> hint;
        return cast(uint) ((count + chunk - 1) / chunk);
    }
    check(rootCostHintFor(0, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 0);
    check(rootCostHintFor(1, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 0);  // one claim already
    check(rootCostHintFor(8, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 5);   // chunk 1: eight claims
    check(rootCostHintFor(15, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 4);  // chunk 2: eight claims
    check(rootCostHintFor(16, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 4);  // chunk 2: eight claims
    check(rootCostHintFor(31, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 3);  // chunk 4: eight claims
    check(rootCostHintFor(63, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 2);  // chunk 8: eight claims
    check(rootCostHintFor(64, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 0);  // sharded: cheapest class
    check(rootCostHintFor(256, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 0);
    check(rootCostHintFor(1024, 6, DEFAULT_SMALL_TABLE_THRESHOLD) == 0);
    check(rootCostHintFor(63, 6, 0) == 2);          // auto rule falls back to 64
    check(rootCostHintFor(64, 6, 128) == 2);        // wider threshold: 64 is wholesale
    check(rootCostHintFor(100, 6, 128) == 1);       // chunk 16: seven claims
    // Pins rootCostHintMaxSplits = 8: a 46-entry table needs chunk 4
    // (12 claims) to span eight target claims; with a bound of 6 the
    // cheapest class 2 (six claims) would already reach the target.
    check(rootCostHintFor(46, 64, 128) == 3);
    foreach (immutable consumers; [0UL, 1, 2, 6, 8, 64])
        foreach (immutable threshold; [0U, 16U, DEFAULT_SMALL_TABLE_THRESHOLD, 128U, 256U])
            foreach (immutable count; [
                1U, 2U, 7U, 8U, 9U, 15U, 16U, 17U, 31U, 32U, 33U, 63U, 64U,
                100U, 127U, 128U, 255U, 256U, 257U, 1023U, 1024U])
            {
                immutable hint = rootCostHintFor(count, consumers, threshold);
                check(hint <= MAX_AVG_COST);
                immutable splits = consumers < 1 ? cast(ulong) 1
                    : consumers > rootCostHintMaxSplits ? cast(ulong) rootCostHintMaxSplits
                    : consumers;
                immutable target = count < splits ? count : cast(size_t) splits;
                // Sharded tables, and any table whose cheapest class already
                // provides the target claims, keep the cheapest class.
                immutable effThresh = threshold != 0 ? threshold : DEFAULT_SMALL_TABLE_THRESHOLD;
                if (count >= effThresh || claimCount(count, 0) >= target)
                    check(hint == 0);
                else
                {
                    // The rule's guarantee: at least min(count, splits) claims
                    // on a wholesale table, via the cheapest such class.
                    check(claimCount(count, hint) >= target);
                    check(claimCount(count, hint - 1) < target);
                }
                // One consumer never needs more than the cheapest class.
                if (consumers <= 1) check(hint == 0);
            }
}

// ===========================================================================
// Cohort slab storage. Selectors: slab (all), slab-retire, slab-retain,
// slab-refill, slab-continuation, slab-cancel, slab-idle, slab-create-fail,
// slab-gc, slab-reclaim, slab-results, slab-per-object; death modes are
// slab-unauthorized, slab-active-dispose and slab-dirty-slot (slab-death).
// Each case names the mutation of actors/private_actor.d it catches.
// Allocation checks use a pointer+size+alignment ledger. It is single-threaded
// except where a lockstep acquire publishes the consumer's frees.
// ===========================================================================

static assert(is(PrivateChildPark!(ulong, ulong) ==
    PrivateChildPark!(ulong, ulong, PrivateStateStorage.perObject)));
static assert(is(PrivateChildPark!(ulong, void) ==
    PrivateChildPark!(ulong, void, PrivateStateStorage.perObject)));
static assert(PrivateChildPark!(ulong, ulong, PrivateStateStorage.cohortSlab).sizeof
    == PrivateChildPark!(ulong, ulong).sizeof + 8);
static assert(!__traits(compiles, (ref PrivateContext ctx) {
    ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(4);
}));
static assert(!__traits(compiles, (ref PrivateContext ctx) {
    ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(4, 8);
}));
static assert(__traits(compiles, (ref PrivateContext ctx) {
    ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(4,
        PrivateCohortOptions(0, false), 4 * ulong.sizeof);
}));
// The cap is a slab-only argument: PrivateCohortOptions keeps its layout, a
// slab cohort cannot be created without a cap, and perObject takes none.
static assert(PrivateCohortOptions.sizeof == 2 * size_t.sizeof);
static assert(!__traits(compiles, (ref PrivateContext ctx) {
    ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(4, PrivateCohortOptions(0, false));
}));
static assert(!__traits(compiles, (ref PrivateContext ctx) {
    ctx.createChildren!(ulong, ulong)(4, PrivateCohortOptions(0, false), 4 * ulong.sizeof);
}));

struct SlabExact
{
    struct Rec { void* ptr; size_t bytes, alignment; bool live; }
    Rec[160] recs;
    uint[160] stack;
    uint n, top, live, frees, calls, failAt;
    bool strictLifo;
}

void* slabExactAllocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto a = cast(SlabExact*) ctx;
    a.calls++;
    if (a.failAt != 0 && a.calls == a.failAt) return null;
    auto p = ActorAllocator.cRuntime().allocate(null, bytes, alignment);
    if (p is null) return null;
    namedCheck(a.n < a.recs.length && a.top < a.stack.length, "slab exact capacity");
    a.recs[a.n] = SlabExact.Rec(p, bytes, alignment, true);
    a.stack[a.top++] = a.n;
    a.n++;
    a.live++;
    return p;
}

void slabExactDeallocate(void* ctx, void* ptr, size_t bytes, size_t alignment) nothrow @nogc @system
{
    if (ptr is null) return;
    auto a = cast(SlabExact*) ctx;
    auto addr = cast(size_t) ptr;
    // The C runtime recycles addresses. Match the live record, not an older
    // generation of the same pointer.
    uint found = uint.max;
    foreach (i; 0 .. a.n)
        if (a.recs[i].live && cast(size_t) a.recs[i].ptr == addr) { found = cast(uint) i; break; }
    if (found == uint.max)
    {
        foreach (i; 0 .. a.n)
            if (!a.recs[i].live && cast(size_t) a.recs[i].ptr == addr)
                fatal("slab exact: double free");
        foreach (i; 0 .. a.n)
        {
            if (!a.recs[i].live) continue;
            auto base = cast(size_t) a.recs[i].ptr;
            if (addr > base && addr < base + a.recs[i].bytes)
                fatal("slab exact: interior pointer");
        }
        fatal("slab exact: unknown pointer");
    }
    if (a.recs[found].bytes != bytes || a.recs[found].alignment != alignment)
        fatal("slab exact: mismatched triple");
    if (a.strictLifo && (a.top == 0 || a.stack[a.top - 1] != found))
        fatal("slab exact: unwind order");
    uint pos = uint.max;
    foreach (i; 0 .. a.top)
        if (a.stack[i] == found) { pos = cast(uint) i; break; }
    if (pos == uint.max) fatal("slab exact: free of unstacked pointer");
    foreach (i; pos + 1 .. a.top) a.stack[i - 1] = a.stack[i];
    a.top--;
    a.recs[found].live = false;
    a.live--;
    a.frees++;
    ActorAllocator.cRuntime().deallocate(null, ptr, bytes, alignment);
}

ActorAllocator slabExactPolicy(ref SlabExact exact) nothrow @nogc @system
{ return ActorAllocator(&exact, &slabExactAllocate, &slabExactDeallocate); }

uint slabExactSizeCount(ref SlabExact exact, size_t bytes) nothrow @nogc @system
{
    uint c;
    foreach (i; 0 .. exact.n)
        if (exact.recs[i].bytes == bytes) c++;
    return c;
}

bool slabExactFreedOnce(ref SlabExact exact, void* ptr) nothrow @nogc @system
{
    if (ptr is null) return false;
    bool saw;
    foreach (i; 0 .. exact.n)
        if (exact.recs[i].ptr is ptr)
        {
            if (exact.recs[i].live) return false;
            saw = true;
        }
    return saw;
}
bool slabExactFreedSize(ref SlabExact exact, void* ptr, size_t bytes) nothrow @nogc @system
{
    if (ptr is null) return false;
    foreach (i; 0 .. exact.n)
        if (exact.recs[i].ptr is ptr && exact.recs[i].bytes == bytes)
            return !exact.recs[i].live;
    return false;
}

void slabDrain(P)(P* park, ref Token token, ref ConsumerView view, string name) nothrow @nogc @system
{
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, name);
    }
}

void slabPumpUntil(P)(P* park, ref Token token, ref ConsumerView view, shared uint* flag, string name)
    nothrow @nogc @system
{
    auto deadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad(*flag) == 0)
    {
        namedCheck(park.live != 0, name);
        park.pump(token);
        while (view.consumeNext()) {}
        namedCheck(MonoTime.currTime < deadline, name);
    }
}

// ---- slab-retire -----------------------------------------------------------------
// Final retiring wave of N non-POD members. Each destructor runs once. The
// ledger sees one slab allocation and one free of that exact pointer, and no
// per-member allocation.
// Catching mutations:
//   retireMember's cohortSlab branch also calls
//     freeMemory(service, entry.state, T.sizeof, T.alignof)
//     (slot 0 is a mismatched triple, later slots are interior): the run
//     aborts "slab exact: mismatched triple" or "slab exact: interior pointer".
//   dispose walks capacity_ and freeMemory's each slot before the slab free:
//     the same abort, even though the retiring wave left count_ at 0.
enum slabOnceN = 5;
struct SlabOnce
{
    uint id;
    uint pad;
    shared uint* hits;
    ~this() nothrow @nogc @system { if (id != 0) atomicFetchAdd(hits[id - 1], 1u); }
}
static assert(SlabOnce.sizeof == 16);
alias SlabOnceKids = PrivateChildPark!(SlabOnce, void, PrivateStateStorage.cohortSlab);
struct SlabOnceRoot
{
    SlabOnceKids* kids;
    shared uint* hits;
    void* slab;
    uint phase;
}
void slabOnceOp(scope ref PrivateBorrow!SlabOnce b, scope ref PrivateContext) nothrow @nogc @system
{ namedCheck(b.value.id != 0, "slab-retire op sees a live member"); }
PrivateRootDisposition slabOnceStep(scope ref PrivateBorrow!SlabOnceRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase++ == 0)
    {
        s.kids = ctx.createChildren!(SlabOnce, void, PrivateStateStorage.cohortSlab)(
            slabOnceN, PrivateCohortOptions(0, false), slabOnceN * SlabOnce.sizeof);
        namedCheck(s.kids !is null, "slab-retire cohort");
        s.slab = privateTestSlabPointer(s.kids);
        namedCheck(s.slab !is null, "slab-retire slab pointer");
        foreach (i; 0 .. slabOnceN)
            namedCheck(s.kids.spawn(ctx, SlabOnce(cast(uint) i + 1, 0, s.hits)) == PrivateSpawnResult.spawned,
                "slab-retire spawn");
        namedCheck(s.kids.startFinal!slabOnceOp(ctx) == PrivateStartResult.started, "slab-retire final");
        return PrivateRootDisposition.waitChildren;
    }
    foreach (i; 0 .. slabOnceN)
    {
        namedCheck(atomicLoad(s.hits[i]) == 1, "slab-retire destructor once");
        namedCheck(privateTestSlabSlotZero(s.kids, i), "slab-retire slot zero after join");
        namedCheck(privateTestEntryStateNull(s.kids, i), "slab-retire state null after join");
    }
    return PrivateRootDisposition.finish;
}
void slabRetire()
{
    SlabExact exact;
    auto hits = new shared uint[slabOnceN];
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabOnceRoot, slabOnceStep).create(farm, 1, 256, slabExactPolicy(exact));
    check(park !is null);
    void* slab;
    check(park.spawn(SlabOnceRoot(null, hits.ptr, null)) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-retire drain");
    // The root copied the pointer into state; recover it from the ledger.
    namedCheck(slabExactSizeCount(exact, slabOnceN * SlabOnce.sizeof) == 1, "slab-retire one slab allocation");
    foreach (i; 0 .. exact.n)
        if (exact.recs[i].bytes == slabOnceN * SlabOnce.sizeof) slab = exact.recs[i].ptr;
    namedCheck(slabExactFreedOnce(exact, slab), "slab-retire slab freed once");
    namedCheck(slabExactSizeCount(exact, SlabOnce.sizeof) == 0, "slab-retire no per-member allocation");
    park.destroy();
    namedCheck(exact.live == 0 && exact.top == 0, "slab-retire zero live allocations");
    writeln("PASS slab-retire one slab free and exactly-once destructors");
}

// ---- slab-retain -----------------------------------------------------------------
// Nonretiring whole wave, then a subset wave, then parent disposal with the
// members still retained. POD and non-POD. Destructors (non-POD) run once,
// at disposal; the slab is freed once.
// Catching mutations:
//   POD dispose drops `static if (!slab)` and freeMemory's entry.state:
//     "slab exact: interior pointer" or "slab exact: mismatched triple".
//   non-POD dispose skips `retireMember` for retained members: destructor
//     count stays 0, "slab-retain disposed once".
enum slabRetainN = 4;
struct SlabPod
{
    ulong tag;
    ulong generation;
    ulong spare;
}
static assert(SlabPod.sizeof == 24);
alias SlabPodKids = PrivateChildPark!(SlabPod, void, PrivateStateStorage.cohortSlab);
shared uint slabPodRuns;
struct SlabPodRoot { SlabPodKids* kids; uint phase; void* slab; ulong pad; }
void slabPodOp(scope ref PrivateBorrow!SlabPod b, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd(slabPodRuns, 1u);
    b.value.generation = b.value.tag;
}
PrivateRootDisposition slabPodStep(scope ref PrivateBorrow!SlabPodRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!(SlabPod, void, PrivateStateStorage.cohortSlab)(
            slabRetainN, PrivateCohortOptions(0, true), slabRetainN * SlabPod.sizeof);
        namedCheck(s.kids !is null, "slab-retain pod cohort");
        s.slab = privateTestSlabPointer(s.kids);
        foreach (i; 0 .. slabRetainN)
            namedCheck(s.kids.spawn(ctx, SlabPod(i + 1)) == PrivateSpawnResult.spawned, "slab-retain pod spawn");
        namedCheck(s.kids.start!slabPodOp(ctx) == PrivateStartResult.started, "slab-retain pod wave");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    if (s.phase == 1)
    {
        namedCheck(atomicLoad(slabPodRuns) == slabRetainN, "slab-retain pod whole wave ran");
        uint[2] sel = [0, 2];
        namedCheck(s.kids.startSubset!slabPodOp(ctx, sel[]) == PrivateStartResult.started, "slab-retain pod subset");
        s.phase = 2;
        return PrivateRootDisposition.waitChildren;
    }
    namedCheck(atomicLoad(slabPodRuns) == slabRetainN + 2, "slab-retain pod subset ran");
    namedCheck(s.kids.subsetMembers(ctx).length == 2, "slab-retain pod selection kept");
    return PrivateRootDisposition.finish;
}
struct SlabHeld
{
    uint id;
    uint pad;
    shared uint* hits;
    ~this() nothrow @nogc @system { if (id != 0) atomicFetchAdd(hits[id - 1], 1u); }
}
static assert(SlabHeld.sizeof == 16);
alias SlabHeldKids = PrivateChildPark!(SlabHeld, void, PrivateStateStorage.cohortSlab);
struct SlabHeldRoot { SlabHeldKids* kids; shared uint* hits; void* slab; uint phase; }
void slabHeldOp(scope ref PrivateBorrow!SlabHeld, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition slabHeldStep(scope ref PrivateBorrow!SlabHeldRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!(SlabHeld, void, PrivateStateStorage.cohortSlab)(
            slabRetainN, PrivateCohortOptions(0, true), slabRetainN * SlabHeld.sizeof);
        namedCheck(s.kids !is null, "slab-retain cohort");
        s.slab = privateTestSlabPointer(s.kids);
        foreach (i; 0 .. slabRetainN)
            namedCheck(s.kids.spawn(ctx, SlabHeld(cast(uint) i + 1, 0, s.hits)) == PrivateSpawnResult.spawned,
                "slab-retain spawn");
        namedCheck(s.kids.start!slabHeldOp(ctx) == PrivateStartResult.started, "slab-retain wave");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    if (s.phase == 1)
    {
        foreach (i; 0 .. slabRetainN)
            namedCheck(atomicLoad(s.hits[i]) == 0, "slab-retain nonretiring keeps members");
        uint[2] sel = [1, 3];
        namedCheck(s.kids.startSubset!slabHeldOp(ctx, sel[]) == PrivateStartResult.started, "slab-retain subset");
        s.phase = 2;
        return PrivateRootDisposition.waitChildren;
    }
    foreach (i; 0 .. slabRetainN)
        namedCheck(atomicLoad(s.hits[i]) == 0, "slab-retain subset keeps members");
    return PrivateRootDisposition.finish;
}
void slabRetain()
{
    atomicStore(slabPodRuns, 0u);
    SlabExact podExact;
    // slab pointer is filled in during the callback; the ledger identifies it.
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(SlabPodRoot, slabPodStep).create(farm, 1, 256, slabExactPolicy(podExact));
        check(park !is null);
        check(park.spawn(SlabPodRoot.init) == PrivateSpawnResult.spawned);
        slabDrain(park, token, view, "slab-retain pod drain");
        void* slab;
        namedCheck(slabExactSizeCount(podExact, slabRetainN * SlabPod.sizeof) == 1, "slab-retain pod one slab");
        foreach (i; 0 .. podExact.n)
            if (podExact.recs[i].bytes == slabRetainN * SlabPod.sizeof) slab = podExact.recs[i].ptr;
        namedCheck(slabExactFreedOnce(podExact, slab), "slab-retain pod slab freed once");
        namedCheck(slabExactSizeCount(podExact, SlabPod.sizeof) == 0, "slab-retain pod no per-member allocation");
        park.destroy();
        namedCheck(podExact.live == 0, "slab-retain pod zero live allocations");
    }
    auto hits = new shared uint[slabRetainN];
    SlabExact heldExact;
    {
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto park = PrivateRootPark!(SlabHeldRoot, slabHeldStep).create(farm, 1, 256, slabExactPolicy(heldExact));
        check(park !is null);
        check(park.spawn(SlabHeldRoot(null, hits.ptr)) == PrivateSpawnResult.spawned);
        slabDrain(park, token, view, "slab-retain held drain");
        foreach (i; 0 .. slabRetainN)
            namedCheck(atomicLoad(hits[i]) == 1, "slab-retain disposed once");
        void* slab;
        namedCheck(slabExactSizeCount(heldExact, slabRetainN * SlabHeld.sizeof) == 1, "slab-retain one slab");
        foreach (i; 0 .. heldExact.n)
            if (heldExact.recs[i].bytes == slabRetainN * SlabHeld.sizeof) slab = heldExact.recs[i].ptr;
        namedCheck(slabExactFreedOnce(heldExact, slab), "slab-retain slab freed once");
        namedCheck(slabExactSizeCount(heldExact, SlabHeld.sizeof) == 0, "slab-retain no per-member allocation");
        park.destroy();
        namedCheck(heldExact.live == 0, "slab-retain zero live allocations");
    }
    writeln("PASS slab-retain waves keep members; disposal frees the slab once");
}

// ---- slab-refill -----------------------------------------------------------------
// Hundreds of spawn / startFinal / join / respawn cycles into the same slots.
// Results and per-slot destructor counts are checked every cycle. No
// allocation after create.
// Catching mutations:
//   slab spawn takes the perObject allocate path: "slab-refill no allocation
//     after create".
//   retireMember drops the slot memset: the next spawn dies in debug with
//     "private child slab: spawn into an uncleared slot" (the check this
//     loop exercises; release still checks results and destructor counts).
version (TSan) enum slabRefillRounds = 40;
else enum slabRefillRounds = 1000;
enum slabRefillN = 8;
struct SlabRefill
{
    uint slot; // 1-based; 0 is the moved-from argument
    uint cycle;
    shared uint* hits;
    ~this() nothrow @nogc @system { if (slot != 0) atomicFetchAdd(hits[slot - 1], 1u); }
}
alias SlabRefillKids = PrivateChildPark!(SlabRefill, ulong, PrivateStateStorage.cohortSlab);
struct SlabRefillRoot
{
    SlabRefillKids* kids;
    shared uint* hits;
    Counts* counts;
    long allocated, freed;
    uint cycle;
}
ulong slabRefillOp(scope ref PrivateBorrow!SlabRefill b, scope ref PrivateContext) nothrow @nogc @system
{ return cast(ulong) b.value.cycle * 1000 + b.value.slot; }
PrivateRootDisposition slabRefillStep(scope ref PrivateBorrow!SlabRefillRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.kids is null)
    {
        s.kids = ctx.createChildren!(SlabRefill, ulong, PrivateStateStorage.cohortSlab)(
            slabRefillN, PrivateCohortOptions(0, false), slabRefillN * SlabRefill.sizeof);
        namedCheck(s.kids !is null, "slab-refill cohort");
        s.allocated = atomicLoad(s.counts.allocated);
        s.freed = atomicLoad(s.counts.freed);
    }
    else
    {
        auto r = s.kids.results(ctx);
        namedCheck(r.length == slabRefillN, "slab-refill result length");
        foreach (i, v; r)
            namedCheck(v == cast(ulong) s.cycle * 1000 + (i + 1), "slab-refill result");
        foreach (i; 0 .. slabRefillN)
            namedCheck(atomicLoad(s.hits[i]) == s.cycle, "slab-refill destructor per cycle");
        if (s.cycle == slabRefillRounds)
        {
            namedCheck(atomicLoad(s.counts.allocated) == s.allocated, "slab-refill no allocation after create");
            namedCheck(atomicLoad(s.counts.freed) == s.freed, "slab-refill no free until dispose");
            return PrivateRootDisposition.finish;
        }
    }
    s.cycle++;
    foreach (i; 0 .. slabRefillN)
    {
        auto result = s.kids.spawn(ctx, SlabRefill(cast(uint) i + 1, s.cycle, s.hits));
        namedCheck(result == PrivateSpawnResult.spawned, "slab-refill spawn");
    }
    namedCheck(s.kids.startFinal!slabRefillOp(ctx) == PrivateStartResult.started, "slab-refill final");
    return PrivateRootDisposition.waitChildren;
}
void slabRefill()
{
    Counts counts;
    auto hits = new shared uint[slabRefillN];
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabRefillRoot, slabRefillStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(SlabRefillRoot(null, hits.ptr, &counts)) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-refill drain");
    foreach (i; 0 .. slabRefillN)
        namedCheck(atomicLoad(hits[i]) == slabRefillRounds, "slab-refill destructor total");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "slab-refill allocation balance");
    writeln("PASS slab-refill ", slabRefillRounds, " cycles reuse cleared slots without allocating");
}

// ---- slab-continuation -----------------------------------------------------------
// A retiring continuation launches a nested slab grandchild cohort and returns
// done before that wave joins, so retirement is the deferred cleanup activation.
// Catching mutations:
//   the childCleanup branch calls retireMember!(T, PrivateStateStorage.perObject)
//     (policy lost after disposeChildren): "slab exact: interior pointer" or
//     "slab exact: mismatched triple" on the member slot.
//   retireMember runs when the continuation returns done, before the
//     childPending == 1 join: disposeChildren dies
//     "retire private actor with active child wave", or the destructor sees a
//     zeroed token ("slab-continuation field live in destructor").
struct SlabGrand
{
    uint id;
    uint pad;
    shared uint* hits;
    ~this() nothrow @nogc @system { if (id != 0) atomicFetchAdd(hits[id - 1], 1u); }
}
static assert(SlabGrand.sizeof == 16);
alias SlabGrandKids = PrivateChildPark!(SlabGrand, void, PrivateStateStorage.cohortSlab);
__gshared bool slabContArmed;
struct SlabCont
{
    uint token;
    uint pad;
    shared uint* hits;
    SlabGrandKids* kids;
    ~this() nothrow @nogc @system
    {
        if (!slabContArmed) return;
        namedCheck(token != 0, "slab-continuation field live in destructor");
        atomicFetchAdd(hits[0], 1u);
    }
}
static assert(SlabCont.sizeof == 24);
alias SlabContKids = PrivateChildPark!(SlabCont, void, PrivateStateStorage.cohortSlab);
enum slabGrandN = 5; // 80 bytes: not a root Batch (48) or the 24-byte member slab
__gshared void* slabContGrandSlab, slabContMemberSlab;
__gshared shared(uint)* slabContGrandHits;
void slabGrandOp(scope ref PrivateBorrow!SlabGrand b, scope ref PrivateContext) nothrow @nogc @system
{ namedCheck(b.value.id != 0, "slab-continuation grandchild ran"); }
PrivateChildDisposition slabContOp(scope ref PrivateBorrow!SlabCont b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto m = &b.value();
    namedCheck(m.kids is null, "slab-continuation single activation before cleanup");
    m.kids = ctx.createChildren!(SlabGrand, void, PrivateStateStorage.cohortSlab)(
        slabGrandN, PrivateCohortOptions(0, false), slabGrandN * SlabGrand.sizeof);
    namedCheck(m.kids !is null, "slab-continuation grandchild cohort");
    slabContGrandSlab = privateTestSlabPointer(m.kids);
    foreach (i; 0 .. slabGrandN)
        namedCheck(m.kids.spawn(ctx, SlabGrand(cast(uint) i + 1, 0, slabContGrandHits)) == PrivateSpawnResult.spawned,
            "slab-continuation grandchild spawn");
    namedCheck(m.kids.start!slabGrandOp(ctx) == PrivateStartResult.started, "slab-continuation grandchild wave");
    return PrivateChildDisposition.done;
}
struct SlabContRoot { SlabContKids* kids; shared uint* hits; ulong pad; uint phase; }
PrivateRootDisposition slabContStep(scope ref PrivateBorrow!SlabContRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase++ == 0)
    {
        s.kids = ctx.createChildren!(SlabCont, void, PrivateStateStorage.cohortSlab)(
            2, PrivateCohortOptions(0, false), 2 * SlabCont.sizeof);
        namedCheck(s.kids !is null, "slab-continuation cohort");
        slabContMemberSlab = privateTestSlabPointer(s.kids);
        namedCheck(s.kids.spawn(ctx, SlabCont(7, 0, s.hits)) == PrivateSpawnResult.spawned, "slab-continuation spawn");
        slabContArmed = true;
        namedCheck(s.kids.startFinal!slabContOp(ctx) == PrivateStartResult.started, "slab-continuation final");
        return PrivateRootDisposition.waitChildren;
    }
    namedCheck(privateTestSlabSlotZero(s.kids, 0), "slab-continuation slot zero after join");
    namedCheck(privateTestEntryStateNull(s.kids, 0), "slab-continuation state null after join");
    namedCheck(atomicLoad(s.hits[0]) == 1, "slab-continuation member destroyed once");
    foreach (i; 0 .. slabGrandN)
        namedCheck(atomicLoad(slabContGrandHits[i]) == 1, "slab-continuation grandchild destroyed once");
    return PrivateRootDisposition.finish;
}
void slabContinuation()
{
    slabContArmed = false;
    slabContGrandSlab = null;
    slabContMemberSlab = null;
    auto memberHits = new shared uint[1];
    auto grandHits = new shared uint[slabGrandN];
    slabContGrandHits = grandHits.ptr;
    scope(exit) slabContGrandHits = null;
    SlabExact exact;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabContRoot, slabContStep).create(farm, 1, 256, slabExactPolicy(exact));
    check(park !is null);
    check(park.spawn(SlabContRoot(null, memberHits.ptr)) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-continuation drain");
    namedCheck(slabExactFreedSize(exact, slabContGrandSlab, slabGrandN * SlabGrand.sizeof),
        "slab-continuation grandchild slab freed once");
    namedCheck(slabExactSizeCount(exact, SlabGrand.sizeof) == 0, "slab-continuation no grandchild per-member allocation");
    namedCheck(slabExactFreedSize(exact, slabContMemberSlab, 2 * SlabCont.sizeof),
        "slab-continuation member slab freed once");
    park.destroy();
    namedCheck(exact.live == 0, "slab-continuation zero live allocations");
    writeln("PASS slab-continuation deferred cleanup zeros the slot and frees the grandchild slab once");
}

// ---- slab-cancel -----------------------------------------------------------------
// Retiring wave, batch 1. Member 0 cancelPeers. The hook at
// privateEmitBeforeCancelCheck drains the published table before the next
// truncation decision, so the undispatched suffix is retired by runSkippedRetire.
// After the join the slots are refilled with a new generation.
// Catching mutations:
//   runSkippedRetire calls retireMember!(T, PrivateStateStorage.perObject):
//     "slab exact: interior pointer" or "slab exact: mismatched triple".
//   truncated() seals a retiring wave like a non-retiring one (suffix not
//     retired before count_ rewinds): "slab-cancel destroyed once before refill"
//     or "slab-cancel slot zero before refill".
enum slabCancelN = 4;
struct SlabSkip
{
    uint id;
    uint generation;
    shared uint* hits;
    ~this() nothrow @nogc @system { if (id != 0) atomicFetchAdd(hits[id - 1], 1u); }
}
alias SlabSkipKids = PrivateChildPark!(SlabSkip, ulong, PrivateStateStorage.cohortSlab);
struct SlabSkipRoot { SlabSkipKids* kids; shared uint* hits; ubyte phase; }
ulong slabSkipOp(scope ref PrivateBorrow!SlabSkip b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    if (b.value.generation == 1 && b.value.id == 1) ctx.cancelPeers();
    return b.value.generation;
}
PrivateRootDisposition slabSkipStep(scope ref PrivateBorrow!SlabSkipRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!(SlabSkip, ulong, PrivateStateStorage.cohortSlab)(
            slabCancelN, PrivateCohortOptions(1, false), slabCancelN * SlabSkip.sizeof);
        namedCheck(s.kids !is null, "slab-cancel cohort");
        foreach (i; 0 .. slabCancelN)
            namedCheck(s.kids.spawn(ctx, SlabSkip(cast(uint) i + 1, 1, s.hits)) == PrivateSpawnResult.spawned,
                "slab-cancel spawn");
        namedCheck(s.kids.startFinal!slabSkipOp(ctx) == PrivateStartResult.started, "slab-cancel final");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    if (s.phase == 1)
    {
        auto o = s.kids.outcome(ctx);
        namedCheck(o.participants == slabCancelN && o.dispatched == 1 && o.cancelled, "slab-cancel truncated");
        auto prefix = s.kids.results(ctx);
        namedCheck(prefix.length == 1 && prefix[0] == 1, "slab-cancel prefix result");
        foreach (i; 0 .. slabCancelN)
        {
            namedCheck(atomicLoad(s.hits[i]) == 1, "slab-cancel destroyed once before refill");
            namedCheck(privateTestSlabSlotZero(s.kids, i), "slab-cancel slot zero before refill");
            namedCheck(privateTestEntryStateNull(s.kids, i), "slab-cancel state null before refill");
        }
        foreach (i; 0 .. slabCancelN)
            namedCheck(s.kids.spawn(ctx, SlabSkip(cast(uint) i + 1, 2, s.hits)) == PrivateSpawnResult.spawned,
                "slab-cancel refill");
        namedCheck(s.kids.startFinal!slabSkipOp(ctx) == PrivateStartResult.started, "slab-cancel refill final");
        s.phase = 2;
        return PrivateRootDisposition.waitChildren;
    }
    auto o = s.kids.outcome(ctx);
    auto r = s.kids.results(ctx);
    namedCheck(o.dispatched == slabCancelN && o.succeeded && r.length == slabCancelN, "slab-cancel refill outcome");
    foreach (v; r) namedCheck(v == 2, "slab-cancel fresh generation");
    foreach (i; 0 .. slabCancelN)
        namedCheck(atomicLoad(s.hits[i]) == 2, "slab-cancel refill destroyed once");
    return PrivateRootDisposition.finish;
}
__gshared Lockstep* slabCancelLock;
shared uint slabCancelChecks;
void slabCancelHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
{
    if (point != ActorTestPoint.privateEmitBeforeCancelCheck) return;
    immutable n = atomicFetchAdd(slabCancelChecks, 1u);
    // Second cancel-check: the first table is published. Drain it so member 0's
    // cancelPeers happens-before this truncation decision.
    if (n == 1 && slabCancelLock !is null) lockstepDrain(slabCancelLock);
}
void slabCancel()
{
    auto hits = new shared uint[slabCancelN];
    SlabExact exact;
    atomicStore(slabCancelChecks, 0u);
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    Lockstep lockstep;
    auto consumer = startLockstep(farm, &lockstep);
    scope(exit) stopLockstep(&lockstep, consumer);
    slabCancelLock = &lockstep;
    setActorTestHook(&slabCancelHook);
    scope(exit) { setActorTestHook(null); slabCancelLock = null; }
    auto park = PrivateRootPark!(SlabSkipRoot, slabSkipStep).create(farm, 1, 256, slabExactPolicy(exact));
    check(park !is null);
    check(park.spawn(SlabSkipRoot(null, hits.ptr)) == PrivateSpawnResult.spawned);
    lockstepRun(park, token, &lockstep, "slab-cancel drain");
    namedCheck(atomicLoad(slabCancelChecks) >= 2, "slab-cancel truncation hook ran");
    namedCheck(slabExactSizeCount(exact, SlabSkip.sizeof) == 0, "slab-cancel no per-member allocation");
    namedCheck(exact.live == 0, "slab-cancel zero live allocations");
    writeln("PASS slab-cancel skipped suffix retired on the slab path and refilled");
}

// ---- slab-idle -------------------------------------------------------------------
// At privateWaveBeforeIdlePublish (after count_ rewinds, before the idle
// store) every retired slot is zero and entry.state is null. The destructor
// still sees its nonzero token.
// Catching mutations:
//   move the cohortSlab memset to after actorTestPoint(privateWaveBeforeIdlePublish)
//     in finish: "slab-idle slots zero".
//   move that memset above destroyState: "slab-idle field live in destructor".
enum slabIdleN = 4;
struct SlabIdleMember
{
    uint token;
    ~this() nothrow @nogc @system
    {
        if (!slabIdleArmed) return;
        namedCheck(token != 0, "slab-idle field live in destructor");
    }
}
alias SlabIdleKids = PrivateChildPark!(SlabIdleMember, void, PrivateStateStorage.cohortSlab);
__gshared bool slabIdleArmed;
__gshared SlabIdleKids* slabIdleKids;
__gshared size_t slabIdleCount;
shared uint slabIdleSaw;
struct SlabIdleRoot { SlabIdleKids* kids; uint phase; }
void slabIdleOp(scope ref PrivateBorrow!SlabIdleMember b, scope ref PrivateContext) nothrow @nogc @system
{ namedCheck(b.value.token != 0, "slab-idle op sees a live token"); }
void slabIdleHook(ActorTestPoint point, ActorInboxNode*) nothrow @nogc @system
{
    if (point != ActorTestPoint.privateWaveBeforeIdlePublish) return;
    auto kids = slabIdleKids;
    if (kids is null) return;
    // finish has already rewound count_; the saved count is the retired prefix.
    foreach (i; 0 .. slabIdleCount)
    {
        namedCheck(privateTestSlabSlotZero(kids, i), "slab-idle slots zero");
        namedCheck(privateTestEntryStateNull(kids, i), "slab-idle state null");
    }
    atomicFetchAdd(slabIdleSaw, 1u);
}
PrivateRootDisposition slabIdleStep(scope ref PrivateBorrow!SlabIdleRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase++ == 0)
    {
        s.kids = ctx.createChildren!(SlabIdleMember, void, PrivateStateStorage.cohortSlab)(
            slabIdleN, PrivateCohortOptions(0, false), slabIdleN * SlabIdleMember.sizeof);
        namedCheck(s.kids !is null, "slab-idle cohort");
        foreach (i; 0 .. slabIdleN)
            namedCheck(s.kids.spawn(ctx, SlabIdleMember(cast(uint) i + 1)) == PrivateSpawnResult.spawned,
                "slab-idle spawn");
        slabIdleKids = s.kids;
        slabIdleCount = slabIdleN;
        slabIdleArmed = true;
        namedCheck(s.kids.startFinal!slabIdleOp(ctx) == PrivateStartResult.started, "slab-idle final");
        return PrivateRootDisposition.waitChildren;
    }
    namedCheck(atomicLoad(slabIdleSaw) == 1, "slab-idle hook observed the retiring join");
    return PrivateRootDisposition.finish;
}
void slabIdle()
{
    slabIdleArmed = false;
    slabIdleKids = null;
    slabIdleCount = 0;
    atomicStore(slabIdleSaw, 0u);
    setActorTestHook(&slabIdleHook);
    scope(exit) { setActorTestHook(null); slabIdleKids = null; slabIdleArmed = false; }
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabIdleRoot, slabIdleStep).create(farm, 1);
    check(park !is null);
    check(park.spawn(SlabIdleRoot.init) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-idle drain");
    namedCheck(atomicLoad(slabIdleSaw) == 1, "slab-idle hook ran once");
    park.destroy();
    writeln("PASS slab-idle slots are zero at idle publication and live in the destructor");
}

// ---- slab-create-fail ------------------------------------------------------------
// Fault the k-th allocation of create, with and without results and subsets.
// Also the byte cap, capacity overflow, and align(128) under cRuntime.
// Catching mutation: the slab-failure path frees park but not entries_ (or
// frees in an order other than the reverse of this attempt's allocations):
// "slab-create-fail live allocations" or "slab exact: unwind order".
struct SlabWide { ulong[8] w; }
static assert(SlabWide.sizeof == 64);
static assert(size_t.max / SlabWide.sizeof + 1 <= size_t.max / 40);
align(128) struct SlabOveraligned { ulong x; }
static assert(SlabOveraligned.alignof == 128);

void slabFailOne(T, R, bool subsets)(scope ref PrivateContext ctx, SlabExact* exact,
    size_t capacity, uint allocs) nothrow @nogc @system
{
    immutable live = exact.live;
    immutable top = exact.top;
    foreach (k; 1 .. allocs + 1)
    {
        exact.calls = 0;
        exact.failAt = k;
        exact.strictLifo = true;
        auto p = ctx.createChildren!(T, R, PrivateStateStorage.cohortSlab)(capacity,
            PrivateCohortOptions(0, subsets), capacity * T.sizeof);
        namedCheck(p is null, "slab-create-fail returns null");
        namedCheck(exact.live == live, "slab-create-fail live allocations");
        namedCheck(exact.top == top, "slab-create-fail unwind restored");
        exact.strictLifo = false;
        exact.failAt = 0;
    }
}

struct SlabFailRoot { SlabExact* exact; uint phase; }
PrivateRootDisposition slabFailStep(scope ref PrivateBorrow!SlabFailRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto exact = b.value.exact;
    if (b.value.phase++ != 0) return PrivateRootDisposition.finish;
    slabFailOne!(ulong, void, false)(ctx, exact, 4, 3);
    slabFailOne!(ulong, ulong, false)(ctx, exact, 4, 4);
    slabFailOne!(ulong, void, true)(ctx, exact, 4, 4);
    slabFailOne!(ulong, ulong, true)(ctx, exact, 4, 5);

    immutable live = exact.live;
    exact.calls = 0;
    namedCheck(ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, false), 0) is null, "slab-create-fail limit 0");
    namedCheck(ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, true), 4 * ulong.sizeof - 1) is null, "slab-create-fail under limit");
    namedCheck(exact.calls == 0 && exact.live == live, "slab-create-fail rejected cap allocates nothing");
    exact.calls = 0;
    namedCheck(ctx.createChildren!(SlabWide, void, PrivateStateStorage.cohortSlab)(
        size_t.max / SlabWide.sizeof + 1, PrivateCohortOptions(0, false), size_t.max) is null,
        "slab-create-fail capacity overflow");
    namedCheck(exact.calls == 0 && exact.live == live, "slab-create-fail overflow allocates nothing");

    exact.calls = 0;
    exact.strictLifo = true;
    immutable top = exact.top;
    namedCheck(ctx.createChildren!(SlabOveraligned, void, PrivateStateStorage.cohortSlab)(
        1, PrivateCohortOptions(0, false), SlabOveraligned.sizeof) is null, "slab-create-fail over-aligned");
    namedCheck(exact.live == live && exact.top == top, "slab-create-fail over-aligned balance");
    exact.strictLifo = false;

    exact.calls = 0;
    auto plain = ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, false), 4 * ulong.sizeof);
    namedCheck(plain !is null && exact.calls == 3, "slab-create-fail plain allocation count");
    namedCheck(privateTestSlabSlotZero(plain, 0), "slab-create-fail plain slab zero");
    exact.calls = 0;
    auto typed = ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, false), 4 * ulong.sizeof);
    namedCheck(typed !is null && exact.calls == 4, "slab-create-fail result allocation count");
    exact.calls = 0;
    auto subset = ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, true), 4 * ulong.sizeof);
    namedCheck(subset !is null && exact.calls == 4, "slab-create-fail subset allocation count");
    exact.calls = 0;
    auto both = ctx.createChildren!(ulong, ulong, PrivateStateStorage.cohortSlab)(
        4, PrivateCohortOptions(0, true), 4 * ulong.sizeof);
    namedCheck(both !is null && exact.calls == 5, "slab-create-fail result+subset allocation count");
    namedCheck(privateTestSlabPointer(both) !is null
        && privateTestSlabPointer(both) != privateTestEntriesPointer(both)
        && privateTestSlabPointer(both) != privateTestResultsPointer(both)
        && privateTestSlabPointer(both) != privateTestSelectionPointer(both),
        "slab-create-fail slab is its own allocation");
    return PrivateRootDisposition.finish;
}
void slabCreateFail()
{
    SlabExact exact;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabFailRoot, slabFailStep).create(farm, 1, 256, slabExactPolicy(exact));
    check(park !is null);
    check(park.spawn(SlabFailRoot(&exact)) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-create-fail drain");
    park.destroy();
    namedCheck(exact.live == 0 && exact.top == 0, "slab-create-fail zero live allocations");
    writeln("PASS slab-create-fail unwind, byte cap, overflow and over-aligned rejection");
}

// ---- slab-gc --------------------------------------------------------------------
// gcRooted slab. A retired member's class reference is finalized while the
// parent still holds the cohort (the slab allocation is still live). An
// unspawned tail filled with that class's address before allocateZero must
// also be finalized, and a retained member must not be.
// Catching mutations:
//   retireMember drops the post-destroy memset: "slab-gc retired slot released
//     its reference".
//   the slab uses allocate instead of allocateZero: "slab-gc unspawned tail was zeroed".
// Skipped under TSan, like dtor-gc: druntime's suspend signal is not TSan-clean.
shared uint slabGcPoisonFlag, slabGcLiveFlag;
shared uint slabGcCreated, slabGcSpawned, slabGcRetired, slabGcGoSpawn, slabGcGoRetire, slabGcGoFinish;
__gshared uint slabPoisonLo, slabPoisonHi;
enum slabPoisonHiMask = 0xA5A5A5A5u;
__gshared SlabGcObj slabGcHandoff;

class SlabPoisonObj
{
    uint magic = 0xA150;
    ~this() { atomicStore(slabGcPoisonFlag, 1u); }
}
class SlabGcObj
{
    uint magic = 0x51AB;
    ~this() { atomicStore(slabGcLiveFlag, 1u); }
}
struct SlabGcMember
{
    ulong[2] pad;
    SlabGcObj obj;
    uint id;
    ~this() nothrow @nogc @system
    {
        if (id == 0) return;
        namedCheck(obj !is null && obj.magic == 0x51AB, "slab-gc destructor sees its reference");
    }
}
alias SlabGcKids = PrivateChildPark!(SlabGcMember, void, PrivateStateStorage.cohortSlab);
struct SlabGcRoot { SlabGcKids* kids; uint phase; }
struct SlabPoisonChain { ActorAllocator raw; }

void slabPoisonStore(void* p) nothrow @nogc @system
{
    auto u = cast(ulong) p;
    slabPoisonLo = cast(uint) u;
    slabPoisonHi = cast(uint)(u >> 32) ^ slabPoisonHiMask;
}
void* slabPoisonLoad() nothrow @nogc @system
{
    if (slabPoisonLo == 0 && slabPoisonHi == 0) return null;
    auto hi = slabPoisonHi ^ slabPoisonHiMask;
    return cast(void*)(cast(ulong) slabPoisonLo | (cast(ulong) hi << 32));
}
void* slabPoisonAllocate(void* ctx, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto chain = cast(SlabPoisonChain*) ctx;
    auto p = chain.raw.allocate(chain.raw.context, bytes, alignment);
    if (p is null) return null;
    auto addr = slabPoisonLoad();
    if (addr !is null)
        foreach (i; 0 .. bytes / (void*).sizeof)
            (cast(void**) p)[i] = addr;
    return p;
}
void slabPoisonDeallocate(void* ctx, void* ptr, size_t bytes, size_t alignment) nothrow @nogc @system
{
    auto chain = cast(SlabPoisonChain*) ctx;
    chain.raw.deallocate(chain.raw.context, ptr, bytes, alignment);
}
pragma(inline, false) void slabArmPoison()
{
    auto obj = new SlabPoisonObj();
    GC.addRoot(cast(void*) obj);
    slabPoisonStore(cast(void*) obj);
}
pragma(inline, false) void slabDropPoisonRoot()
{
    auto p = slabPoisonLoad();
    if (p !is null) GC.removeRoot(p);
    slabPoisonLo = 0;
    slabPoisonHi = 0;
}
pragma(inline, false) void slabGcPlantHandoff()
{
    slabGcHandoff = new SlabGcObj();
}
pragma(inline, false) void slabGcCollectTwice()
{
    ulong[64] scrub = 0;
    if (scrub[63] == 1) return;
    GC.collect();
    GC.collect();
}
void slabGcNop(scope ref PrivateBorrow!SlabGcMember, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition slabGcStep(scope ref PrivateBorrow!SlabGcRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!(SlabGcMember, void, PrivateStateStorage.cohortSlab)(
            4, PrivateCohortOptions(0, false), 4 * SlabGcMember.sizeof);
        namedCheck(s.kids !is null, "slab-gc cohort");
        atomicStore(slabGcCreated, 1u);
        s.phase = 1;
        return PrivateRootDisposition.again;
    }
    if (s.phase == 1)
    {
        if (atomicLoad(slabGcGoSpawn) == 0) return PrivateRootDisposition.again;
        namedCheck(slabGcHandoff !is null, "slab-gc handoff");
        auto obj = slabGcHandoff;
        slabGcHandoff = null;
        namedCheck(s.kids.spawn(ctx, SlabGcMember([0, 0], obj, 1)) == PrivateSpawnResult.spawned, "slab-gc spawn");
        atomicStore(slabGcSpawned, 1u);
        s.phase = 2;
        return PrivateRootDisposition.again;
    }
    if (s.phase == 2)
    {
        if (atomicLoad(slabGcGoRetire) == 0) return PrivateRootDisposition.again;
        namedCheck(s.kids.startFinal!slabGcNop(ctx) == PrivateStartResult.started, "slab-gc final");
        s.phase = 3;
        return PrivateRootDisposition.waitChildren;
    }
    atomicStore(slabGcRetired, 1u);
    if (atomicLoad(slabGcGoFinish) == 0) return PrivateRootDisposition.again;
    return PrivateRootDisposition.finish;
}
void slabGc()
{
    version (TSan)
    {
        writeln("SKIP slab-gc: druntime GC suspend signals are not TSan-clean");
        return;
    }
    else
    {
        atomicStore(slabGcPoisonFlag, 0u);
        atomicStore(slabGcLiveFlag, 0u);
        atomicStore(slabGcCreated, 0u);
        atomicStore(slabGcSpawned, 0u);
        atomicStore(slabGcRetired, 0u);
        atomicStore(slabGcGoSpawn, 0u);
        atomicStore(slabGcGoRetire, 0u);
        atomicStore(slabGcGoFinish, 0u);
        slabPoisonLo = 0;
        slabPoisonHi = 0;
        slabGcHandoff = null;
        slabArmPoison();
        Counts counts;
        SlabPoisonChain chain;
        chain.raw = policy(counts);
        auto filler = ActorAllocator(&chain, &slabPoisonAllocate, &slabPoisonDeallocate);
        auto farm = AntFarm.create();
        scope(exit) farm.destroy();
        auto token = farm.registerProducer(Tier.small);
        scope(exit) farm.unregisterProducer(token);
        ConsumerView view;
        check(view.subscribe(farm) >= 0);
        scope(exit) view.unsubscribe();
        auto rooted = ActorAllocator.gcRooted(&filler);
        auto park = PrivateRootPark!(SlabGcRoot, slabGcStep).create(farm, 1, 256, rooted);
        check(park !is null);
        check(park.spawn(SlabGcRoot.init) == PrivateSpawnResult.spawned);
        slabPumpUntil(park, token, view, &slabGcCreated, "slab-gc created");
        slabDropPoisonRoot();
        slabGcCollectTwice();
        namedCheck(atomicLoad(slabGcPoisonFlag) == 1, "slab-gc unspawned tail was zeroed");
        slabGcPlantHandoff();
        atomicStore(slabGcGoSpawn, 1u);
        slabPumpUntil(park, token, view, &slabGcSpawned, "slab-gc spawned");
        slabGcCollectTwice();
        namedCheck(atomicLoad(slabGcLiveFlag) == 0, "slab-gc live member retained");
        atomicStore(slabGcGoRetire, 1u);
        slabPumpUntil(park, token, view, &slabGcRetired, "slab-gc retired");
        // Cohort is still alive: the root has not finished, so the slab is allocated.
        namedCheck(park.live == 1, "slab-gc parent keeps the cohort");
        slabGcCollectTwice();
        namedCheck(atomicLoad(slabGcLiveFlag) == 1, "slab-gc retired slot released its reference");
        atomicStore(slabGcGoFinish, 1u);
        slabDrain(park, token, view, "slab-gc drain");
        park.destroy();
        namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "slab-gc allocation balance");
        writeln("PASS slab-gc dead slots release references and the unspawned tail is zero");
    }
}

// ---- slab-reclaim ----------------------------------------------------------------
// A two-level slab tree under both root reclamation policies, with workers.
// Catching mutation: dispose skips the one slab free (or freeMemory's each
// slot instead): "slab-reclaim allocation balance" under emitter and consumer.
struct SlabLeaf { ulong v; }
struct SlabMid
{
    PrivateChildPark!(SlabLeaf, ulong, PrivateStateStorage.cohortSlab)* leaves;
    ulong base;
}
alias SlabMidKids = PrivateChildPark!(SlabMid, void, PrivateStateStorage.cohortSlab);
shared uint slabReclaimLeaves;
ulong slabLeafOp(scope ref PrivateBorrow!SlabLeaf b, scope ref PrivateContext) nothrow @nogc @system
{
    atomicFetchAdd(slabReclaimLeaves, 1u);
    return b.value.v + 1;
}
void slabMidOp(scope ref PrivateBorrow!SlabMid b, scope ref PrivateContext ctx) nothrow @nogc @system
{
    auto m = &b.value();
    enum n = 3;
    m.leaves = ctx.createChildren!(SlabLeaf, ulong, PrivateStateStorage.cohortSlab)(
        n, PrivateCohortOptions(0, false), n * SlabLeaf.sizeof);
    namedCheck(m.leaves !is null, "slab-reclaim leaves");
    foreach (i; 0 .. n)
        namedCheck(m.leaves.spawn(ctx, SlabLeaf(m.base + i)) == PrivateSpawnResult.spawned, "slab-reclaim leaf spawn");
    namedCheck(m.leaves.startFinal!slabLeafOp(ctx) == PrivateStartResult.started, "slab-reclaim leaf final");
}
void slabMidNop(scope ref PrivateBorrow!SlabMid, scope ref PrivateContext) nothrow @nogc @system {}
struct SlabReclaimRoot { SlabMidKids* mids; uint phase; }
PrivateRootDisposition slabReclaimStep(scope ref PrivateBorrow!SlabReclaimRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        enum n = 3;
        s.mids = ctx.createChildren!(SlabMid, void, PrivateStateStorage.cohortSlab)(
            n, PrivateCohortOptions(0, false), n * SlabMid.sizeof);
        namedCheck(s.mids !is null, "slab-reclaim mids");
        foreach (i; 0 .. n)
            namedCheck(s.mids.spawn(ctx, SlabMid(null, i * 10)) == PrivateSpawnResult.spawned, "slab-reclaim mid spawn");
        namedCheck(s.mids.start!slabMidOp(ctx) == PrivateStartResult.started, "slab-reclaim mid wave");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    if (s.phase == 1)
    {
        namedCheck(s.mids.startFinal!slabMidNop(ctx) == PrivateStartResult.started, "slab-reclaim mid final");
        s.phase = 2;
        return PrivateRootDisposition.waitChildren;
    }
    return PrivateRootDisposition.finish;
}
void slabReclaim(PrivateRootReclamation reclaim)()
{
    atomicStore(slabReclaimLeaves, 0u);
    Counts counts;
    auto farm = AntFarm.create(2, 8, 2, 0, 0, 1, 4096);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    auto workers = new Workers(farm, 2);
    scope(exit) workers.join();
    auto park = PrivateRootPark!(SlabReclaimRoot, slabReclaimStep, reclaim).create(
        farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(SlabReclaimRoot.init) == PrivateSpawnResult.spawned);
    auto deadline = MonoTime.currTime + 30.seconds;
    while (park.live)
    {
        park.pump(token);
        namedCheck(MonoTime.currTime < deadline, "slab-reclaim drain");
    }
    namedCheck(atomicLoad(slabReclaimLeaves) == 9, "slab-reclaim tree ran");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "slab-reclaim allocation balance");
}
void slabReclaimAll()
{
    slabReclaim!(PrivateRootReclamation.emitter)();
    slabReclaim!(PrivateRootReclamation.consumer)();
    writeln("PASS slab-reclaim tree under emitter and consumer reclamation");
}

// ---- slab-results ----------------------------------------------------------------
// Results stay readable after the retiring wave zeros slots. Entry, result and
// selection storage are distinct from the slab and survive slot clearing.
// Catching mutation: finish of a retiring wave memsets the result storage
// (or retireMember zeros past the slot into the result array):
// "slab-results readable after retirement".
enum slabResultN = 4;
struct SlabRes { ulong input; }
alias SlabResKids = PrivateChildPark!(SlabRes, ulong, PrivateStateStorage.cohortSlab);
struct SlabResRoot { SlabResKids* kids; uint phase; }
ulong slabResOp(scope ref PrivateBorrow!SlabRes b, scope ref PrivateContext) nothrow @nogc @system
{ return 0x51AB_0000 + b.value.input; }
PrivateRootDisposition slabResStep(scope ref PrivateBorrow!SlabResRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase == 0)
    {
        s.kids = ctx.createChildren!(SlabRes, ulong, PrivateStateStorage.cohortSlab)(
            slabResultN, PrivateCohortOptions(0, true), slabResultN * SlabRes.sizeof);
        namedCheck(s.kids !is null, "slab-results cohort");
        foreach (i; 0 .. slabResultN)
            namedCheck(s.kids.spawn(ctx, SlabRes(i + 1)) == PrivateSpawnResult.spawned, "slab-results spawn");
        namedCheck(s.kids.startFinal!slabResOp(ctx) == PrivateStartResult.started, "slab-results final");
        s.phase = 1;
        return PrivateRootDisposition.waitChildren;
    }
    if (s.phase == 1)
    {
        auto r = s.kids.results(ctx);
        namedCheck(r.length == slabResultN, "slab-results length after retirement");
        foreach (i, v; r) namedCheck(v == 0x51AB_0000 + i + 1, "slab-results readable after retirement");
        auto slab = privateTestSlabPointer(s.kids);
        auto entries = privateTestEntriesPointer(s.kids);
        auto results = privateTestResultsPointer(s.kids);
        auto sel = privateTestSelectionPointer(s.kids);
        namedCheck(slab !is null && entries !is null && results !is null && sel !is null, "slab-results storage present");
        namedCheck(slab != entries && slab != results && slab != sel
            && entries != results && results != sel && entries != sel, "slab-results storage independent");
        foreach (i; 0 .. slabResultN)
            namedCheck(privateTestSlabSlotZero(s.kids, i), "slab-results slot cleared");
        foreach (i; 0 .. slabResultN)
            namedCheck(s.kids.spawn(ctx, SlabRes(i + 10)) == PrivateSpawnResult.spawned, "slab-results refill");
        uint[2] selIdx = [3, 1];
        namedCheck(s.kids.startSubset!slabResOp(ctx, selIdx[]) == PrivateStartResult.started, "slab-results subset");
        s.phase = 2;
        return PrivateRootDisposition.waitChildren;
    }
    auto r = s.kids.results(ctx);
    auto members = s.kids.subsetMembers(ctx);
    namedCheck(r.length == 2 && members.length == 2, "slab-results subset length");
    namedCheck(members[0] == 3 && members[1] == 1, "slab-results selection after slot clearing");
    namedCheck(r[0] == 0x51AB_0000 + 13 && r[1] == 0x51AB_0000 + 11, "slab-results subset values");
    return PrivateRootDisposition.finish;
}
void slabResults()
{
    Counts counts;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabResRoot, slabResStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(SlabResRoot.init) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-results drain");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "slab-results allocation balance");
    writeln("PASS slab-results outlive slot clearing; entry and selection storage stay independent");
}

// ---- slab-per-object -------------------------------------------------------------
// The default policy still allocates one state per spawn and frees it at
// retirement. The compile-time checks above pin the default type, the rejected
// no-options slab overload, and the extra word.
// Catching mutation: perObject spawn stops allocating (returns a static slot):
// "slab-per-object one state per spawn". Freeing that state only at dispose,
// not in retireMember: "slab-per-object frees each state at retirement".
enum slabPerObjectN = 4;
struct SlabPlain { ulong n; }
alias SlabPlainKids = PrivateChildPark!(SlabPlain, void);
struct SlabPlainRoot { SlabPlainKids* kids; Counts* counts; long allocated, freed; uint phase; }
void slabPlainOp(scope ref PrivateBorrow!SlabPlain, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition slabPlainStep(scope ref PrivateBorrow!SlabPlainRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto s = &b.value();
    if (s.phase++ == 0)
    {
        s.kids = ctx.createChildren!SlabPlain(slabPerObjectN);
        namedCheck(s.kids !is null, "slab-per-object cohort");
        s.allocated = atomicLoad(s.counts.allocated);
        foreach (i; 0 .. slabPerObjectN)
            namedCheck(s.kids.spawn(ctx, SlabPlain(i + 1)) == PrivateSpawnResult.spawned, "slab-per-object spawn");
        namedCheck(atomicLoad(s.counts.allocated) == s.allocated + slabPerObjectN,
            "slab-per-object one state per spawn");
        s.freed = atomicLoad(s.counts.freed);
        namedCheck(s.kids.startFinal!slabPlainOp(ctx) == PrivateStartResult.started, "slab-per-object final");
        return PrivateRootDisposition.waitChildren;
    }
    namedCheck(atomicLoad(s.counts.freed) == s.freed + slabPerObjectN,
        "slab-per-object frees each state at retirement");
    return PrivateRootDisposition.finish;
}
void slabPerObject()
{
    Counts counts;
    auto farm = AntFarm.create();
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto park = PrivateRootPark!(SlabPlainRoot, slabPlainStep).create(farm, 1, 256, policy(counts));
    check(park !is null);
    check(park.spawn(SlabPlainRoot(null, &counts)) == PrivateSpawnResult.spawned);
    slabDrain(park, token, view, "slab-per-object drain");
    park.destroy();
    namedCheck(atomicLoad(counts.allocated) == atomicLoad(counts.freed), "slab-per-object allocation balance");
    writeln("PASS slab-per-object still allocates and frees one state per member");
}

// ---- death -----------------------------------------------------------------------
// slab-unauthorized, slab-active-dispose: debug and release.
// slab-dirty-slot: debug dies; release survives because the zero-check is
// version(assert).
// Catching mutations:
//   spawn drops authorized(): the unauthorized child prints "spawn survived".
//   disposeChildren drops the active-wave fatal: the active-dispose child exits 0.
//   the uncleared-slot fatal is not under version(assert): the release child dies.
//   the uncleared-slot check is deleted: the debug child prints "release survived".
alias SlabAuthKids = PrivateChildPark!(ulong, void, PrivateStateStorage.cohortSlab);
struct SlabAuthProbe { SlabAuthKids* kids; }
struct SlabAuthRoot { SlabAuthProbe* probe; bool owner; }
void slabAuthOp(scope ref PrivateBorrow!ulong, scope ref PrivateContext) nothrow @nogc @system {}
PrivateRootDisposition slabAuthStep(scope ref PrivateBorrow!SlabAuthRoot b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto probe = b.value.probe;
    if (b.value.owner)
    {
        if (probe.kids is null)
        {
            probe.kids = ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
                2, PrivateCohortOptions(0, false), 2 * ulong.sizeof);
            namedCheck(probe.kids !is null, "slab unauthorized cohort");
        }
        return PrivateRootDisposition.again;
    }
    namedCheck(probe.kids !is null, "slab unauthorized cohort exists");
    fprintf(stderr, "slab unauthorized: other root calls spawn\n");
    probe.kids.spawn(ctx, 1);
    fprintf(stderr, "slab unauthorized: spawn survived\n");
    return PrivateRootDisposition.finish;
}
void slabUnauthorizedDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    SlabAuthProbe probe;
    auto park = PrivateRootPark!(SlabAuthRoot, slabAuthStep).create(farm, 2);
    check(park !is null);
    check(park.spawn(SlabAuthRoot(&probe, true)) == PrivateSpawnResult.spawned);
    check(park.spawn(SlabAuthRoot(&probe, false)) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
PrivateRootDisposition slabActiveDisposeStep(scope ref PrivateBorrow!ulong, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto kids = ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
        1, PrivateCohortOptions(0, false), ulong.sizeof);
    namedCheck(kids !is null && kids.spawn(ctx, 1) == PrivateSpawnResult.spawned, "slab active-dispose setup");
    namedCheck(kids.start!slabAuthOp(ctx) == PrivateStartResult.started, "slab active-dispose wave");
    fprintf(stderr, "slab active-dispose: returning finish\n");
    return PrivateRootDisposition.finish;
}
void slabActiveDisposeDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(ulong, slabActiveDisposeStep).create(farm, 1);
    check(park !is null);
    check(park.spawn(0) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
PrivateRootDisposition slabDirtyStep(scope ref PrivateBorrow!ulong, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    auto kids = ctx.createChildren!(ulong, void, PrivateStateStorage.cohortSlab)(
        1, PrivateCohortOptions(0, false), ulong.sizeof);
    namedCheck(kids !is null, "slab dirty-slot cohort");
    privateTestDirtySlabSlot(kids, 0);
    fprintf(stderr, "slab dirty-slot: spawning into a dirty slot\n");
    auto result = kids.spawn(ctx, 7);
    namedCheck(result == PrivateSpawnResult.spawned, "slab dirty-slot release spawn");
    fprintf(stderr, "slab dirty-slot: release survived\n");
    return PrivateRootDisposition.finish;
}
void slabDirtySlotDeath()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    auto park = PrivateRootPark!(ulong, slabDirtyStep).create(farm, 1);
    check(park !is null);
    check(park.spawn(0) == PrivateSpawnResult.spawned);
    park.pump(token);
    while (view.consumeNext()) {}
}
void slabDeath()
{
    version (linux)
    {
        auto unauthorized = probeDeath("slab-unauthorized");
        check(unauthorized.status != 0);
        check(unauthorized.stderrText.canFind("slab unauthorized: other root calls spawn"));
        check(unauthorized.stderrText.canFind("antfarm fatal: private child park: unauthorized context"));
        check(!unauthorized.stderrText.canFind("survived"));
        auto active = probeDeath("slab-active-dispose");
        check(active.status != 0);
        check(active.stderrText.canFind("slab active-dispose: returning finish"));
        check(active.stderrText.canFind("antfarm fatal: retire private actor with active child wave"));
        version (assert)
        {
            auto dirty = probeDeath("slab-dirty-slot");
            check(dirty.status != 0);
            check(dirty.stderrText.canFind("slab dirty-slot: spawning into a dirty slot"));
            check(dirty.stderrText.canFind("antfarm fatal: private child slab: spawn into an uncleared slot"));
            check(!dirty.stderrText.canFind("survived"));
            writeln("PASS slab death: unauthorized context, active disposal, and debug uncleared-slot spawn");
        }
        else
        {
            auto dirty = probeDeath("slab-dirty-slot");
            check(dirty.status == 0);
            check(dirty.stderrText.canFind("slab dirty-slot: release survived"));
            writeln("PASS slab death: unauthorized context and active disposal; uncleared-slot check compiled out of release");
        }
    }
    else
        writeln("SKIP slab death: /proc/self/exe re-exec requires Linux");
}

void phaseSlab()
{
    slabRetire();
    slabRetain();
    slabRefill();
    slabContinuation();
    slabCancel();
    slabIdle();
    slabCreateFail();
    slabGc();
    slabReclaimAll();
    slabResults();
    slabPerObject();
}

void runSuite()
{
    rootCostHintRule(); writeln("PASS derived root cost hint rule");
    phaseCancel();
    phaseRootTracking();
    phaseOutcomes();
    phaseCont();
    phaseE1();
    phaseD();
    phasePorts();
    phaseDtor();
    phaseSlab();
    phaseSubset();
    phaseEntryWord();
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
        case "cont-descendants-first": contInterleave!(PrivateRootReclamation.emitter)(CiMode.early); return;
        case "cont-activation-first": contInterleave!(PrivateRootReclamation.emitter)(CiMode.late);
            contInterleave!(PrivateRootReclamation.consumer)(CiMode.late); return;
        case "cont-seal-final-owner": version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
            { contSealFinalOwner!(PrivateRootReclamation.emitter)(); contSealFinalOwner!(PrivateRootReclamation.consumer)(); } return;
        case "cont-hold-before-table": contInterleave!(PrivateRootReclamation.emitter)(CiMode.hold); contInterleave!(PrivateRootReclamation.emitter)(CiMode.lateHold); return;
        case "cont-done-tails": contInterleave!(PrivateRootReclamation.emitter)(CiMode.done); return;
        case "cont-final-retire": contTree!(PrivateRootReclamation.emitter)(); contInterleave!(PrivateRootReclamation.emitter)(CiMode.finalDone);
            version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks) contSealFinalOwner!(PrivateRootReclamation.emitter)(); return;
        case "cont-counter-exhaustion": contInterleave!(PrivateRootReclamation.emitter)(CiMode.overflow); return;
        case "cont-finish-suspended": contInterleave!(PrivateRootReclamation.emitter)(CiMode.finishSuspended); return;
        case "cont-child-service-word": contInterleave!(PrivateRootReclamation.emitter)(CiMode.serviceWord); return;
        case "cont-counter-exhaustion-death": case "cont-death": contDeath(); return;
        case "cont-unschedule-before": contInterleave!(PrivateRootReclamation.emitter)(CiMode.beforeP); return;
        case "cont-unschedule-after": contInterleave!(PrivateRootReclamation.emitter)(CiMode.afterP); return;
        case "cont-hold-after-custody": contInterleave!(PrivateRootReclamation.emitter)(CiMode.lateHold); return;
        case "cont-fifo-return-overlap": version (AntfarmActorTestHooks) version (AntfarmWriteAuditHooks) contFifoOverlap!(PrivateRootReclamation.emitter)(); return;
        case "cont-fifo-partial": version (AntfarmWriteAuditHooks) contFifo!(PrivateRootReclamation.emitter)(2, 1); return;
        case "cont-unschedule-race": contInterleave!(PrivateRootReclamation.emitter)(CiMode.beforeP); contInterleave!(PrivateRootReclamation.emitter)(CiMode.afterP); return;
        case "cont-results": case "cont-multi-gen": contResults!(PrivateRootReclamation.emitter)(false); contResults!(PrivateRootReclamation.consumer)(true); return;
        case "cont-nested": contTree!(PrivateRootReclamation.emitter)(2, 2); contTree!(PrivateRootReclamation.consumer)(64, 1); return;
        case "cont-resume-admission": foreach (count; [513, 1025]) foreach (limit; [1, 64, 256]) { contAdmission!(PrivateRootReclamation.emitter)(count, limit); contAdmission!(PrivateRootReclamation.consumer)(count, limit); } return;
        case "cont-fifo-drain-reuse": version (AntfarmWriteAuditHooks) foreach (n; [2, 3]) { contFifo!(PrivateRootReclamation.emitter)(n); contFifo!(PrivateRootReclamation.consumer)(n); } return;
        case "cont-return-cas-retry": contCasRetry!(PrivateRootReclamation.emitter)(); contCasRetry!(PrivateRootReclamation.consumer)(); return;
        case "cont": phaseCont(); return;
        case "cont-basic": contTree!(PrivateRootReclamation.emitter)(); contTree!(PrivateRootReclamation.consumer)(); return;
        case "cont-yield": contYield!(PrivateRootReclamation.emitter)(); contYield!(PrivateRootReclamation.consumer)(); return;
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
        case "phase-e1": phaseE1(); return;
        case "e1-basic":
            e1Basic!(PrivateRootReclamation.emitter)();
            e1Basic!(PrivateRootReclamation.consumer)();
            writeln("PASS e1-basic"); return;
        case "e1-before-custody":
            e1BeforeCustody(false); e1BeforeCustody(true);
            writeln("PASS e1-before-custody"); return;
        case "e1-after-custody": e1AfterCustody(); writeln("PASS e1-after-custody"); return;
        case "e1-late-strict":
            version (AntfarmActorTestHooks)
            {
                e1LateStrict!(PrivateRootReclamation.emitter)();
                e1LateStrict!(PrivateRootReclamation.consumer)();
                writeln("PASS e1-late-strict");
            }
            return;
        case "e1-joined-mixed":
            e1JoinedAndMixed!(PrivateRootReclamation.emitter)();
            e1JoinedAndMixed!(PrivateRootReclamation.consumer)();
            writeln("PASS e1-joined-mixed"); return;
        case "e1-root-ready-cas-retry":
            version (AntfarmActorTestHooks)
            {
                e1RootReadyCasRetry();
                writeln("PASS e1-root-ready-cas-retry");
            }
            return;
        case "e1-seal-final-owner":
            version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks)
            {
                e1SealFinalOwner();
                writeln("PASS e1-seal-final-owner");
            }
            return;
        case "e1-counter-exhaustion":
            version (AntfarmActorTestHooks) e1CounterExhaustion();
            return;
        case "e1-counter-exhaustion-death": e1CounterExhaustionDeath(); return;
        case "phase-d": phaseD(); return;
        case "phase-ports": phasePorts(); return;
        case "phase-dtor": phaseDtor(); return;
        case "dtor-root":
            dtorRoot!(PrivateRootReclamation.emitter)(); dtorRoot!(PrivateRootReclamation.consumer)();
            writeln("PASS dtor-root"); return;
        case "dtor-child":
            dtorChild!(PrivateRootReclamation.emitter)(); dtorChild!(PrivateRootReclamation.consumer)();
            writeln("PASS dtor-child"); return;
        case "dtor-order":
            foreach (m; 0 .. 3)
            {
                dtorOrder!(PrivateRootReclamation.emitter)(cast(ubyte) m);
                dtorOrder!(PrivateRootReclamation.consumer)(cast(ubyte) m);
            }
            writeln("PASS dtor-order"); return;
        case "dtor-failed-spawn":
            dtorFailedSpawn!(PrivateRootReclamation.emitter)(); dtorFailedSpawn!(PrivateRootReclamation.consumer)();
            writeln("PASS dtor-failed-spawn"); return;
        case "dtor-aligned":
            dtorAlignedRoot!(DtAligned32, PrivateRootReclamation.emitter)();
            dtorAlignedRoot!(DtAligned32, PrivateRootReclamation.consumer)();
            dtorAlignedRoot!(DtAligned64, PrivateRootReclamation.emitter)();
            dtorAlignedRoot!(DtAligned64, PrivateRootReclamation.consumer)();
            dtorAlignedChild!(DtAligned32, PrivateRootReclamation.emitter)();
            dtorAlignedChild!(DtAligned32, PrivateRootReclamation.consumer)();
            dtorAlignedChild!(DtAligned64, PrivateRootReclamation.emitter)();
            dtorAlignedChild!(DtAligned64, PrivateRootReclamation.consumer)();
            writeln("PASS dtor-aligned"); return;
        case "dtor-moved-from":
            dtorMovedFrom!(PrivateRootReclamation.emitter)(); dtorMovedFrom!(PrivateRootReclamation.consumer)();
            writeln("PASS dtor-moved-from"); return;
        case "dtor-recursive":
            foreach (m; 0 .. 2)
            {
                dtorRecursive!(PrivateRootReclamation.emitter)(cast(ubyte) m);
                dtorRecursive!(PrivateRootReclamation.consumer)(cast(ubyte) m);
            }
            writeln("PASS dtor-recursive"); return;
        case "dtor-results":
            dtorResults!(PrivateRootReclamation.emitter)(); dtorResults!(PrivateRootReclamation.consumer)();
            writeln("PASS dtor-results"); return;
        case "dtor-gc": dtorGcAll(); return;
        case "dtor-destroy-live": dtorDestroyLiveDeath(); return;
        case "dtor-finish-active-child": dtorActiveChildDeath(); return;
        case "dtor-reenter-failed-spawn":
            version (assert) dtorReenterDeath();
            return;
        case "dtor-death": dtorDeath(); return;
        case "phase-subset": phaseSubset(); return;
        case "subset-basic":
            subsetBasic!(PrivateRootReclamation.emitter)(); subsetBasic!(PrivateRootReclamation.consumer)();
            writeln("PASS subset-basic"); return;
        case "subset-active":
            subsetActive!(PrivateRootReclamation.emitter)(); subsetActive!(PrivateRootReclamation.consumer)();
            writeln("PASS subset-active"); return;
        case "subset-large": subsetLargeCohorts(); writeln("PASS subset-large"); return;
        case "subset-nested":
            subsetNested!(PrivateRootReclamation.emitter)(); subsetNested!(PrivateRootReclamation.consumer)();
            writeln("PASS subset-nested"); return;
        case "subset-alloc-fail": subsetAllocationFailures(); writeln("PASS subset-alloc-fail"); return;
        case "subset-dtor":
            foreach (m; 0 .. 2)
            {
                subsetDtor!(PrivateRootReclamation.emitter)(cast(ubyte) m);
                subsetDtor!(PrivateRootReclamation.consumer)(cast(ubyte) m);
            }
            writeln("PASS subset-dtor"); return;
        case "subset-late-tail":
            version (AntfarmActorTestHooks)
            {
                subsetLateTail!(PrivateRootReclamation.emitter)();
                subsetLateTail!(PrivateRootReclamation.consumer)();
                writeln("PASS subset-late-tail");
            }
            return;
        case "subset-limits":
            version (AntfarmWriteAuditHooks) { subsetLimits(); writeln("PASS subset-limits"); }
            return;
        case "entry-word": phaseEntryWord(); return;
        case "entry-word-misuse":
            version (AntfarmActorTestHooks) entryWord(true);
            return;
        case "entry-word-death":
            version (AntfarmActorTestHooks) entryWordDeath();
            return;
        case "subset-death": subsetDeath(); return;
        case "subset-dup-adjacent": subsetMisuseDeath(SubsetMisuse.dupAdjacent); return;
        case "subset-dup-gap": subsetMisuseDeath(SubsetMisuse.dupGap); return;
        case "subset-range": subsetMisuseDeath(SubsetMisuse.range); return;
        case "subset-range-max": subsetMisuseDeath(SubsetMisuse.rangeMax); return;
        case "subset-too-many": subsetMisuseDeath(SubsetMisuse.tooMany); return;
        case "subset-unreserved": subsetMisuseDeath(SubsetMisuse.unreserved); return;
        case "subset-unreserved-empty": subsetMisuseDeath(SubsetMisuse.unreservedEmpty); return;
        case "subset-unreserved-options": subsetMisuseDeath(SubsetMisuse.unreservedOptions); return;
        case "subset-wrong-parent": subsetMisuseDeath(SubsetMisuse.wrongParent); return;
        case "subset-default-context": subsetMisuseDeath(SubsetMisuse.defaultContext); return;
        case "subset-members-wrong-parent": subsetMisuseDeath(SubsetMisuse.membersWrongParent); return;
        case "ports-basic":
            portsBasic!(PrivateRootReclamation.emitter)();
            portsBasic!(PrivateRootReclamation.consumer)();
            writeln("PASS ports-basic"); return;
        case "ports-poll-visibility":
            version (AntfarmActorTestHooks) { portsPollVisibility(); writeln("PASS ports-poll-visibility"); }
            return;
        case "ports-wait-visibility":
            version (AntfarmActorTestHooks)
            {
                portsWaitVisibility(true); portsWaitVisibility(false);
                writeln("PASS ports-wait-visibility");
            }
            return;
        case "ports-late-tail":
            version (AntfarmActorTestHooks)
            {
                portsLateTail!(PrivateRootReclamation.emitter)();
                portsLateTail!(PrivateRootReclamation.consumer)();
                writeln("PASS ports-late-tail");
            }
            return;
        case "ports-large-cohort": portsLargeCohorts(); writeln("PASS ports-large-cohort"); return;
        case "ports-nested": portsPipeline(); portsDepth64(); writeln("PASS ports-nested"); return;
        case "ports-alloc-fail": portsAllocationFailures(); writeln("PASS ports-alloc-fail"); return;
        case "ports-read-active": portsReadActiveDeath(); return;
        case "ports-wrong-parent-results": portsMisuseDeath(false); return;
        case "ports-default-results": portsMisuseDeath(true); return;
        case "ports-death": portsDeath(); return;
        case "child-large-cohort": childLargeCohort(257); childLargeCohort(513); return;
        case "mixed-table":
            mixedTable!(PrivateRootReclamation.emitter)(true);
            mixedTable!(PrivateRootReclamation.consumer)(true); return;
        case "refill-after-final": refillAfterFinal(); return;
        case "delayed-final-credit":
            version (AntfarmActorTestHooks) delayedFinalCredit(); return;
        case "return-cas-retry":
            version (AntfarmActorTestHooks) returnCasRetry(); return;
        case "child-before-seal":
            version (AntfarmWriteAuditHooks) version (AntfarmActorTestHooks) childBeforeSeal(); return;
        case "child-alloc-fail": childAllocationFailures(); return;
        case "depth-64": depth64(); return;
        case "destroy-live": destroyLiveDeath(); return;
        case "finish-with-active-child": activeChildDeath(); return;
        case "cross-farm-write": crossFarmWriteDeath(); return;
        case "cross-farm-unregister": crossFarmUnregisterDeath(); return;
        case "cross-farm-flush": crossFarmFlushDeath(); return;
        case "outcomes": phaseOutcomes(); return;
        case "cancel": phaseCancel(); return;
        case "cancel-c1": cancelC1(); return;
        case "cancel-c2": cancelC2(); return;
        case "cancel-c3": cancelC3(); return;
        case "cancel-c4": cancelC4(); return;
        case "cancel-c5": cancelC5(); return;
        case "cancel-c6": cancelC6(); return;
        case "cancel-c7": version (AntfarmActorTestHooks) cancelC7(); return;
        case "cancel-c8": cancelC8(); return;
        case "cancel-c9": version (AntfarmActorTestHooks) cancelC9(); return;
        case "cancel-c10":
            foreach (c10Mode; [C10Mode.untracked, C10Mode.failed, C10Mode.cancelled, C10Mode.failedAndCancelled])
                cancelC10(c10Mode);
            return;
        case "root-tracking": phaseRootTracking(); return;
        case "root-t1": version (AntfarmActorTestHooks) rootTrackingT1(); return;
        case "root-t2": version (AntfarmActorTestHooks) rootTrackingT2(); return;
        case "root-t3": rootTrackingT3(); return;
        case "root-t4": rootTrackingT4(); return;
        case "root-t5": rootTrackingT5(); return;
        case "root-t6":
            rootTrackingT6!(PrivateRootReclamation.emitter)();
            rootTrackingT6!(PrivateRootReclamation.consumer)();
            return;
        case "root-t7": rootTrackingT7(); return;
        case "root-t8": rootTrackingT8(); return;
        case "root-tracking-death": rootTrackingDeath(); return;
        case "root-generation-wrap": rootGenerationWrapDeath(); return;
        case "tracked-torture":
            import std.conv : to;
            trackedTorture(args.length > 2 ? args[2].to!size_t : 3000, args.length > 3 ? args[3].to!uint : 23);
            writeln("PASS private tracked torture");
            return;
        case "cancel-c11": cancelC11(); return;
        case "cancel-c12": cancelC12(); return;
        case "cancel-c13": cancelC13(); return;
        case "cancel-death": cancelDeath(); return;
        case "cancel-peers-root": cancelDeathRun!cancelPeersRootStep(); return;
        case "cancel-park-unauthorized": cancelDeathRun!parkCancelAuthorityStep(); return;
        case "cancel-default-cancelled": cancelDefaultContextDeath("cancelled"); return;
        case "cancel-default-children": cancelDefaultContextDeath("children"); return;
        case "cancel-default-peers": cancelDefaultContextDeath("peers"); return;
        case "cancel-torture":
            import std.conv : to;
            cancelTorture(args.length > 2 ? args[2].to!size_t : 3000, args.length > 3 ? args[3].to!uint : 17);
            writeln("PASS private cancellation torture");
            return;
        case "outcomes-o9-empty":
        {
            O9EmptyApp app;
            auto farm = AntFarm.create();
            scope(exit) farm.destroy();
            auto token = farm.registerProducer(Tier.small);
            scope(exit) farm.unregisterProducer(token);
            ConsumerView view;
            check(view.subscribe(farm) >= 0);
            scope(exit) view.unsubscribe();
            auto park = PrivateRootPark!(O9EmptyRoot, o9EmptyRoot).create(farm, 1);
            check(park.spawn(O9EmptyRoot(null, &app)) == PrivateSpawnResult.spawned);
            auto deadline = MonoTime.currTime + 10.seconds;
            while (park.live)
            {
                park.pump(token);
                while (view.consumeNext()) {}
                namedCheck(MonoTime.currTime < deadline, "o9 empty-start drain");
            }
            park.destroy();
            check(app.preserved);
            return;
        }
        case "outcome-active": outcomeActiveDeath(); return;
        case "root-fail-untracked": rootFailDeath(); return;
        case "failure-overflow": failureOverflowDeath(); return;
        case "fail-unauthorized": failUnauthorizedDeath(); return;
        case "outcome-unauthorized": outcomeUnauthorizedDeath(); return;
        case "slab": phaseSlab(); return;
        case "slab-retire": slabRetire(); return;
        case "slab-retain": slabRetain(); return;
        case "slab-refill": slabRefill(); return;
        case "slab-continuation": slabContinuation(); return;
        case "slab-cancel": slabCancel(); return;
        case "slab-idle": slabIdle(); return;
        case "slab-create-fail": slabCreateFail(); return;
        case "slab-gc": slabGc(); return;
        case "slab-reclaim": slabReclaimAll(); return;
        case "slab-results": slabResults(); return;
        case "slab-per-object": slabPerObject(); return;
        case "slab-death": slabDeath(); return;
        case "slab-unauthorized": slabUnauthorizedDeath(); return;
        case "slab-active-dispose": slabActiveDisposeDeath(); return;
        case "slab-dirty-slot": slabDirtySlotDeath(); return;
        case "death": deathHarness(); return;
        case "node-death": nodeDeath(); return;
        case "death-negative-control": deathNegativeControl(); return;
        case "death-exits-zero":
            fprintf(stderr, "antfarm fatal: private death harness sample\n");
            return; // right message, wrong exit status
        default: break;
    }
    enum misusePrefix = "root-tracking-misuse:";
    if (mode.length > misusePrefix.length && mode[0 .. misusePrefix.length] == misusePrefix)
    {
        rootTrackingMisuse(mode[misusePrefix.length .. $]);
        return;
    }
    enum nodeDeathPrefix = "node-death:";
    if (mode.length > nodeDeathPrefix.length
        && mode[0 .. nodeDeathPrefix.length] == nodeDeathPrefix)
    {
        nodeDeathChild(mode[nodeDeathPrefix.length .. $]);
        return;
    }
    if (mode.length >= fatalSamplePrefix.length
        && mode[0 .. fatalSamplePrefix.length] == fatalSamplePrefix)
    {
        fatal(mode[fatalSamplePrefix.length .. $]);
        return;
    }
    runSuite();
}
