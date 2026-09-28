module private_actor_tests;
import actors;
import antfarm;
import antfarm_allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import core.stdc.stdlib : abort;
import core.stdc.stdio : fprintf, stderr;
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
PrivateDisposition repeat(scope ref PrivateBorrow!State b, scope ref PrivateContext)
    nothrow @nogc @system
{
    check(*b.value.output == b.value.n);
    ++b.value.n;
    *b.value.output = b.value.n;
    return b.value.n == b.value.iterations ? PrivateDisposition.finish : PrivateDisposition.again;
}
alias Park = PrivateRootPark!(State, repeat);
static assert(!__traits(compiles, (Park p) { auto copy = p; }));
static assert(!__traits(compiles, (PrivateBorrow!State b) { return b.state_; }));
static assert(!__traits(compiles, (Park* p) { return p.entries_; }));
static assert(!__traits(compiles, (PrivateContext c) { auto copy = c; }));
static assert(!__traits(compiles, (PrivateBorrow!State b) { auto copy = b; }));

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
    foreach (i; 0 .. 2) check(park.spawn(State(&output[i], 2, 0)));
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
    auto park = PrivateRootPark!(State, repeat, reclamation).create(farm, count, policy(counts));
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
        foreach (i; 0 .. count) check(park.spawn(State(&output[i], iterations, 0)));
        check(!park.spawn(State.init));
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
        foreach (i; 0 .. 4) check(b.value.leaves.spawn(ctx, Leaf(&b.value.results[i], 0, b.value.gate)));
        check(!b.value.leaves.spawn(ctx, Leaf.init));
    }
    foreach (i; 0 .. 4) check(b.value.results[i] == b.value.generation);
    ++b.value.generation;
    atomicStore!(MemoryOrder.raw)(*b.value.gate, 0u);
    check(b.value.leaves.start!leafStep(ctx));
    check(!b.value.leaves.start!leafStep(ctx)); // no overlapping lease
    check(!b.value.leaves.spawn(ctx, Leaf.init));
    atomicStore!(MemoryOrder.raw)(*b.value.gate, 1u);
}
void nodeRetire(scope ref PrivateBorrow!Node b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    check(b.value.generation == 3);
    foreach (i; 0 .. 4) check(b.value.results[i] == 3);
    check(ctx.createChildren!Leaf(1) is null);
    check(!b.value.leaves.start!leafStep(ctx)); // terminal activation
}
struct Parent { PrivateChildPark!Node* nodes; ulong* results; uint phase; shared uint* gates; }
PrivateDisposition parentStep(scope ref PrivateBorrow!Parent b, scope ref PrivateContext ctx)
    nothrow @nogc @system
{
    if (b.value.nodes is null)
    {
        b.value.nodes = ctx.createChildren!Node(17);
        check(b.value.nodes !is null);
        foreach (i; 0 .. 17)
            check(b.value.nodes.spawn(ctx, Node(null, b.value.results + i * 4, 0, b.value.gates + i)));
        PrivateContext fake;
        check(!b.value.nodes.finished(fake));
        check(!b.value.nodes.spawn(fake, Node.init));
        check(!b.value.nodes.start!nodeStep(fake));
    }
    if (!b.value.nodes.finished(ctx)) return PrivateDisposition.again;
    foreach (i; 0 .. 68) check(b.value.results[i] == (b.value.phase < 3 ? b.value.phase : 3));
    if (b.value.phase == 4) return PrivateDisposition.finish;
    check(b.value.phase == 3 ? b.value.nodes.start!nodeRetire(ctx, true)
        : b.value.nodes.start!nodeStep(ctx));
    ++b.value.phase;
    return PrivateDisposition.again;
}
void trees(uint consumers = 6)
{
    Counts counts;
    auto farm = AntFarm.create(2, 8, 6, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto park = PrivateRootPark!(Parent, parentStep).create(farm, 48, policy(counts));
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
        foreach (i; 0 .. 48) check(park.spawn(Parent(null, output.ptr + i * 68, 0, gates.ptr + i * 17)));
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
        foreach (i; 0 .. 300) check(park.spawn(State(&results[i], 1, 0)));
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
    foreach (budget; 0 .. 3)
    {
        Counts counts;
        atomicStore(counts.budget, cast(long) budget);
        check(Park.create(farm, 4, policy(counts)) is null);
        check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
    }
    Counts counts;
    auto park = Park.create(farm, 4, policy(counts));
    check(park !is null);
    atomicStore(counts.budget, 0L);
    check(!park.spawn(State.init));
    check(park.live == 0);
    park.destroy();
    check(atomicLoad(counts.allocated) == atomicLoad(counts.freed));
}

void backpressure()
{
    enum count = 20000;
    auto farm = AntFarm.create(2, 8, 1, 0, 0, 1, 8192);
    scope(exit) farm.destroy();
    auto park = Park.create(farm, count);
    auto token = farm.registerProducer(Tier.small);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    check(view.subscribe(farm) >= 0);
    scope(exit) view.unsubscribe();
    auto output = new ulong[count];
    foreach (i; 0 .. count) check(park.spawn(State(&output[i], 1, 0)));
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
            auto park = Park.create(farm, 1024, policy(counts));
            check(park !is null);
            auto token = farm.registerProducer(Tier.small);
            check(token.valid);
            scope(exit) farm.unregisterProducer(token);
            auto output = new ulong[1024];
            foreach (round; 0 .. 20)
            {
                output[] = 0;
                foreach (i; 0 .. 1024) check(park.spawn(State(&output[i], 3, 0)));
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
void main(string[] args)
{
    if (args.length > 1 && args[1] == "join-only") { sameShardJoin(); return; }
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
