/++
 + Tests for the Ant Farm. Run: dmd -g -O -inline antfarm.d antfarm_templates.d antfarm_test.d -of=antfarm_test && ./antfarm_test
 +/
module antfarm_test;

import antfarm_templates;
import antfarm_actor;
import antfarm_allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdio : fflush, stdout;
import core.stdc.string : memset;
import std.range : hasLength, iota, isForwardRange, isInputRange, popFrontN;
import std.typecons : Tuple, tuple;
import core.stdc.stdlib : malloc, free;

static assert(!DEFAULT_HUGE_PAGES, "ordinary 4 KiB backing is the public default");

// ---------------------------------------------------------------------
// Shared test state
// ---------------------------------------------------------------------

__gshared shared(long)* g_calls;   // per-payload call counts
__gshared size_t g_npayloads;
__gshared shared(long) g_totalCalls;
__gshared ulong* g_keepalive;      // keeps GC away (we malloc instead)

struct ActorAllocCounts
{
    size_t allocations;
    size_t deallocations;
}

void* actorTestAllocate(void* context, size_t bytes, size_t alignment)
    nothrow @nogc @system
{
    if (alignment > 64) return null;
    auto counts = cast(ActorAllocCounts*) context;
    ++counts.allocations;
    return allocateAligned64(bytes);
}

void actorTestDeallocate(void* context, void* memory, size_t, size_t)
    nothrow @nogc @system
{
    auto counts = cast(ActorAllocCounts*) context;
    ++counts.deallocations;
    freeAligned64(memory);
}

struct ActorCounterState
{
    ulong mutations;
    ulong target;
}

__gshared shared(long) g_actorEntries;
__gshared shared(long) g_actorConcurrent;
__gshared shared(long) g_actorCalls;

void actorCounter(ref ActorBorrow!ActorCounterState actor,
        ref ActorContext context) nothrow @nogc @system
{
    immutable entered = atomicFetchAdd!(MemoryOrder.acq_rel)(
        g_actorEntries, 1L);
    if (entered != 0)
        atomicFetchAdd!(MemoryOrder.rel)(g_actorConcurrent, 1L);

    atomicFetchAdd!(MemoryOrder.rel)(g_actorCalls, 1L);
    ++actor.value.mutations;
    immutable finished = actor.value.mutations == actor.value.target;

    atomicFetchSub!(MemoryOrder.rel)(g_actorEntries, 1L);
    if (finished)
        context.retire();
    else
        context.republish();
}

struct ActorInboxState
{
    ulong consumed;
    ulong sum;
}

struct ActorInboxMessage
{
    ActorInboxNode node;
    ulong value;
}

__gshared shared(long) g_inboxEntries;
__gshared shared(long) g_inboxConcurrent;
__gshared shared(long) g_inboxActivations;
__gshared shared(long) g_inboxConsumed;
__gshared shared(ulong) g_inboxSum;
__gshared shared(long) g_inboxAccepted;
__gshared shared(long) g_inboxClosed;
__gshared shared(long) g_inboxUnexpected;

void inboxActor(ref ActorBorrow!ActorInboxState actor,
        ref ActorContext context) nothrow @nogc @system
{
    enum drainLimit = 7;
    immutable entered = atomicFetchAdd!(MemoryOrder.acq_rel)(
        g_inboxEntries, 1L);
    if (entered != 0)
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxConcurrent, 1L);
    atomicFetchAdd!(MemoryOrder.rel)(g_inboxActivations, 1L);

    foreach (_; 0 .. drainLimit)
    {
        auto node = context.popInbox();
        if (node is null) break;
        auto message = cast(ActorInboxMessage*) node.payload;
        ++actor.value.consumed;
        actor.value.sum += message.value;
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxConsumed, 1L);
        atomicFetchAdd!(MemoryOrder.rel)(g_inboxSum, message.value);
        node.complete();
    }

    atomicFetchSub!(MemoryOrder.rel)(g_inboxEntries, 1L);
}

private long incCalls(size_t idx) nothrow @nogc @system
{
    atomicFetchAdd(g_calls[idx], 1L);
    atomicFetchAdd(g_totalCalls, 1L);
    return 1;
}

long testCb(size_t idx, ulong done) nothrow @nogc @system
{
    return incCalls(idx);
}

long testCb1(size_t idx) nothrow @nogc @system
{
    return incCalls(idx);
}

void allocCalls(size_t n)
{
    g_calls = cast(shared(long)*) malloc(n * long.sizeof);
    g_npayloads = n;
    foreach (i; 0 .. n)
        g_calls[i] = 0;
    atomicStore!(MemoryOrder.raw)(g_totalCalls, 0L);
}

void freeCalls() { free(cast(void*) g_calls); g_calls = null; }

void check(bool cond, const(char)[] msg, string file = __FILE__, int line = __LINE__)
{
    if (!cond)
    {
        fprintf(stderr, "CHECK FAILED %s:%d: %.*s\n", file.ptr, line, cast(int) msg.length, msg.ptr);
        import core.stdc.stdlib : abort;
        abort();
    }
}

ulong lowHalf(AntFarm* f, uint ki)
{
    return atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & LOWMASK;
}

ulong highHalf(AntFarm* f, uint ki)
{
    return atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) >> 32;
}

void checkNoLeftoverSub(AntFarm* f, const(char)[] msg)
{
    foreach (ki; 0 .. f.K)
        check(highHalf(f, cast(uint) ki) == 0, msg);
}

// ---------------------------------------------------------------------
// Test 1: arithmetic
// ---------------------------------------------------------------------

void testMagicWrap()
{
    void probe(bool huge, const(char)[] label)
    {
        auto f = AntFarm.create(1 << 18, 8, 1, 1, 2048, 1, 512,
            DEFAULT_SMALL_TABLE_THRESHOLD, huge);
        scope (exit) f.destroy();
        if (huge && !f.usedLargePages)
            return; // ANTFARM_HUGE_PAGES=0
        auto p = cast(ulong*) f.buf;
        p[0] = 0x1111_2222_3333_4444UL;
        p[f.Ln - 1] = 0xAAAA_BBBB_CCCC_DDDDUL;
        check(p[f.Ln] == p[0], label);
        check(p[2 * f.Ln - 1] == p[f.Ln - 1], label);
        p[f.Ln] = 0x5555_6666_7777_8888UL;
        check(p[0] == 0x5555_6666_7777_8888UL, label);
    }
    probe(false, "wrap 4K");
    probe(true, "wrap large pages");
    printf("testMagicWrap OK\n"); fflush(stdout);
}

void testArithmetic()
{
    check(sqcsOf(1) == 1, "sqcsOf(1)");
    check(sqcsOf(2) == 1, "sqcsOf(2)");
    check(sqcsOf(3) == 2, "sqcsOf(3)");
    check(sqcsOf(4) == 2, "sqcsOf(4)");
    check(sqcsOf(5) == 3, "sqcsOf(5)");
    check(sqcsOf(100) == 10, "sqcsOf(100)");
    check(sqcsOf(128) == 12, "sqcsOf(128)");
    check(sentinelOf(0) != sentinelOf(1), "sentinel distinct");
    check(sentinelOf(0) != 0, "sentinel nonzero");
    // shard partition (spec 5e-e/f) sums to Tlen
    foreach (tlen; 64 .. 200)
        foreach (sq; 1 .. 13)
        {
            immutable shbase = tlen / sq, shrm = tlen % sq;
            ulong sum = 0;
            foreach (shi; 0 .. sq)
                sum += shi < shrm ? shbase + 1 : shbase;
            check(sum == tlen, "shard partition");
        }
    printf("testArithmetic OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 2: single-threaded end to end, Sub0 lifecycle, resubscription
// ---------------------------------------------------------------------

void testSingleThreaded()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 8192, 4, 2048);
    scope (exit) f.destroy();

    // Epoch 0: Sub0 pulse present, blocking reclamation of segment 0.
    check(lowHalf(f, 0) == SUB0, "Sub0 at construction");
    foreach (ki; 1 .. 8)
        check(lowHalf(f, cast(uint) ki) == 0, "other Rt zero");

    enum N = 20;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    foreach (i; 0 .. N)
    {
        immutable uint maxCs = (i >= 18) ? 3 : 1;   // two MT payloads
        immutable uint done = (i >= 18) ? 5 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
    }
    immutable expected = 18 * 1 + 2 * 5;

    ConsumerView v;
    immutable r = v.subscribe(f);
    check(r == 0, "first subscribe returns epoch 0");
    // First subscriber cleared Sub0 and holds one consumer reference.
    check(lowHalf(f, 0) == 1, "Sub0 cleared, one consumer ref");
    checkNoLeftoverSub(f, "no leftover Sub after first subscribe");

    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "register small producer");
    check(f.write(entries[0 .. 10], tok) == 10, "write first 10");
    check(f.write(entries[10 .. 20], tok) == 10, "write second 10");

    // Consume the two tables; third call finds nothing published.
    check(v.consumeNext(), "consume table 1");
    check(v.consumeNext(), "consume table 2");
    check(!v.consumeNext(), "no third table");

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "total calls");

    v.unsubscribe();
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf back to 0");
    // Last unsubscriber deposited Sub0 at its current segment: exactly one
    // pulse in the whole farm.
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "exactly one Sub0 pulse after unsubscribe");

    // Resubscribe: succeeds, clears Sub0 again, catches up to the write tail.
    ConsumerView v2;
    check(v2.subscribe(f) >= 0, "resubscribe");
    foreach (ki; 0 .. 8)
        check(lowHalf(f, cast(uint) ki) < SUB0, "Sub0 cleared on resubscribe");
    checkNoLeftoverSub(f, "no leftover Sub after resubscribe");
    while (v2.consumeNext()) {}
    // No payload was re-executed: tables were already complete.
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "no re-execution");
    v2.unsubscribe();
    f.unregisterProducer(tok);

    printf("testSingleThreaded OK\n"); fflush(stdout);
}

// Input-range write overload: a simple input range over the same malloc'd
// entries, not a D slice.
struct PayloadRange
{
    PayloadEntry* p;
    size_t len;
    size_t i;

    @property bool empty() const nothrow @nogc @safe { return i >= len; }
    @property ref PayloadEntry front() nothrow @nogc @system { return p[i]; }
    void popFront() nothrow @nogc @system { ++i; }
    @property PayloadRange save() nothrow @nogc @system { return this; }
}

// Copying this valid input range shares its cursor. It deliberately has no
// save checkpoint and must therefore be rejected by write's forward contract.
final class SharedPayloadInputRange
{
    PayloadEntry* p;
    size_t len;
    size_t i;

    this(PayloadEntry* entries, size_t count) nothrow @nogc
    {
        p = entries;
        len = count;
    }

    @property bool empty() const nothrow @nogc @safe { return i >= len; }
    @property ref PayloadEntry front() nothrow @nogc @system { return p[i]; }
    void popFront() nothrow @nogc @safe { ++i; }
}
static assert(isInputRange!SharedPayloadInputRange);
static assert(!isForwardRange!SharedPayloadInputRange);

struct CountingArgRange
{
    size_t i;
    size_t n;
    size_t* frontCalls;

    @property bool empty() const pure nothrow @nogc @safe { return i >= n; }
    @property size_t front() nothrow @nogc @system
    {
        ++*frontCalls;
        return i;
    }
    void popFront() pure nothrow @nogc @safe { ++i; }
    @property CountingArgRange save() pure nothrow @nogc @safe { return this; }
    @property size_t length() const pure nothrow @nogc @safe { return n - i; }
}

struct CountingBodyRange
{
    ulong* words;
    size_t i;
    size_t n;
    size_t* frontCalls;

    @property bool empty() const pure nothrow @nogc @safe { return i >= n; }
    @property PayloadBody front() nothrow @nogc @system
    {
        ++*frontCalls;
        return words[i .. i + 1];
    }
    void popFront() pure nothrow @nogc @safe { ++i; }
    @property CountingBodyRange save() pure nothrow @nogc @safe { return this; }
    @property size_t length() const pure nothrow @nogc @safe { return n - i; }
}

void testInputRangeWrite()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 8192, 4, 2048);
    scope (exit) f.destroy();

    enum N = 20;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable uint maxCs = (i >= 18) ? 3 : 1;   // two MT payloads
        immutable uint done = (i >= 18) ? 5 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
        expected += done;
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before range write");
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "register small producer");

    auto singlePass = new SharedPayloadInputRange(entries, N);
    static assert(!__traits(compiles, f.write(singlePass, tok)),
        "write must reject input ranges that cannot checkpoint");

    PayloadRange range = PayloadRange(entries, N);
    check(f.write(range, tok) == N, "write input range");
    check(v.consumeNext(), "consume range-written table");
    check(!v.consumeNext(), "no extra range table");

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "range exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "range total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testInputRangeWrite OK\n"); fflush(stdout);
}

void testSeparateRangesAndQuantum()
{
    auto f = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum N = 40;
    allocCalls(N);
    scope (exit) freeCalls();
    PayloadHeader[N] headers;
    ulong[N] words;
    PayloadBody[N] bodies;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!testCb1(&headers[i], 1, 1);
        words[i] = i;
        bodies[i] = words[i .. i + 1];
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before separate-range write");
    auto tok = f.registerProducer(Tier.small);
    check(f.write(headers[], bodies[], tok, 1) == N,
          "write separate header/body ranges");

    check(v.consumeQuantum(), "quantum saw table");
    immutable afterOne = atomicLoad!(MemoryOrder.raw)(g_totalCalls);
    check(afterOne > 0 && afterOne <= 16, "quantum consumed one chunk");
    while (v.consumeNext()) {}
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N,
          "idle sweep completed quantum leftovers");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testSeparateRangesAndQuantum OK\n"); fflush(stdout);
}

void testPayloadRange()
{
    auto f = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum N = 40;
    allocCalls(2 * N);
    scope (exit) freeCalls();

    // Tuple form: fields map onto (idx, done) of testCb.
    struct PairRange
    {
        size_t i, n;
        @property bool empty() const pure nothrow @nogc @safe
        {
            return i >= n;
        }
        @property Tuple!(size_t, ulong) front() pure nothrow @nogc @safe
        {
            return tuple(i + N, 1UL);
        }
        void popFront() pure nothrow @nogc @safe { ++i; }
        @property PairRange save() pure nothrow @nogc @safe { return this; }
    }

    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before payloadRange write");
    auto tok = f.registerProducer(Tier.small);

    // Single-parameter form: element is the packed parameter itself.
    // ElementType of std.range.iota plugs straight into the single-param path.
    auto singles = payloadRange!testCb1(iota(0, N));
    check(f.write(singles, tok) == N, "payloadRange single-param write");

    // Tuple form: any arity, fields in declaration order.
    auto pairs = payloadRange!testCb(PairRange(0, N));
    check(f.write(pairs, tok) == N, "payloadRange tuple write");

    while (v.consumeNext()) {}
    foreach (i; 0 .. 2 * N)
        check(g_calls[i] == 1, "payloadRange exact per-payload call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == 2 * N,
          "payloadRange total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testPayloadRange OK\n"); fflush(stdout);
}

void testUniformRanges()
{
    enum N = 300; // exceeds one 4096-word small-producer table
    enum M = 24;
    enum F = 24;
    enum K = 8;
    allocCalls(N + M + F + K);
    scope (exit) freeCalls();

    auto f = AntFarm.create(1 << 18, 8, 1, 0, 0, 1, 4096);
    scope (exit) f.destroy();
    ConsumerView v;
    check(v.subscribe(f) == 0, "subscribe before uniform writes");
    auto tok = f.registerProducer(Tier.small);

    size_t frontCalls;
    auto jobs = payloadRange!testCb1(CountingArgRange(0, N, &frontCalls));
    static assert(isForwardRange!(typeof(jobs)));
    static assert(hasLength!(typeof(jobs)));

    size_t written;
    size_t tables;
    while (!jobs.empty)
    {
        immutable n = f.write(jobs, tok);
        if (n == 0)
        {
            while (v.consumeNext()) {}
            continue;
        }
        ++tables;
        written += n;
        check(jobs.popFrontN(n) == n, "uniform popFrontN");
        while (v.consumeNext()) {}
    }
    check(written == N && tables > 1, "uniform partial-write retry");
    check(frontCalls == N, "uniform sizing never evaluates front");

    PayloadHeader mutableHeader;
    initPayloadHeader!testCb1(&mutableHeader, 1, 1);
    const PayloadHeader commonHeader = mutableHeader;
    ulong[M + F] words;
    PayloadBody[M] bodies;
    foreach (i; 0 .. M)
    {
        words[i] = N + i;
        bodies[i] = words[i .. i + 1];
    }
    auto broadcast = broadcastPayloads(commonHeader, bodies[]);
    static assert(isForwardRange!(typeof(broadcast)));
    static assert(hasLength!(typeof(broadcast)));
    auto constPair = pairPayloads((&commonHeader)[0 .. 1], bodies[0 .. 1]);
    static assert(isForwardRange!(typeof(constPair)));
    check(constPair.front.header.call is mutableHeader.call,
        "const header range pairing");
    auto runtimeConfigured = payloadRange!testCb1(iota(0, 0), 1u, 1u);
    static assert(isForwardRange!(typeof(runtimeConfigured)));
    check(runtimeConfigured.empty, "runtime payloadRange configuration");
    check(f.write(commonHeader, bodies[], tok) == M,
        "const common-header broadcast write");
    while (v.consumeNext()) {}

    foreach (i; 0 .. F)
        words[M + i] = N + M + i;
    size_t bodyFrontCalls;
    auto fixedBodies = CountingBodyRange(
        words.ptr + M, 0, F, &bodyFrontCalls);
    check(f.write(commonHeader, fixedBodies, 1, tok) == F,
        "constant call-and-length write");
    check(bodyFrontCalls == F,
        "fixed-length sizing never evaluates body front");
    while (v.consumeNext()) {}

    Tuple!(size_t, ulong)[K] mtArgs;
    foreach (i; 0 .. K)
        mtArgs[i] = tuple(cast(size_t)(N + M + F + i), 2UL);
    auto mtJobs = payloadRange!(testCb, 2, 2)(mtArgs[]);
    check(f.write(mtJobs, tok) == K, "uniform MT index write");
    check(mtJobs.popFrontN(K) == K, "uniform MT popFrontN");
    while (v.consumeNext()) {}

    foreach (i; 0 .. N + M + F)
        check(g_calls[i] == 1, "uniform exact call count");
    foreach (i; N + M + F .. N + M + F + K)
        check(g_calls[i] == 2, "uniform MT exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == N + M + F + 2 * K,
        "uniform total");

    v.unsubscribe();
    f.unregisterProducer(tok);
    printf("testUniformRanges OK\n"); fflush(stdout);
}

 // ---------------------------------------------------------------------
 // Test 3: concurrent producers and consumers, exact call accounting
 // ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Test 3: concurrent producers and consumers, exact call accounting
// ---------------------------------------------------------------------

struct ProdCtx
{
    AntFarm* f;
    PayloadEntry* entries;
    size_t count;
    Tier tier;
}

struct ConsCtx
{
    AntFarm* f;
    long expected;
    MonoTime deadline;
}

void producerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid) fatal("producer overregistration");
    size_t off;
    while (off < c.count)
    {
        immutable n = c.f.write(c.entries[off .. c.count], tok);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    c.f.unregisterProducer(tok);
}

void consumerMain(ConsCtx* c)
{
    ConsumerView v;
    if (v.subscribe(c.f) < 0) fatal("subscribe failed");
    for (;;)
    {
        if (!v.consumeNext())
        {
            if (atomicLoad!(MemoryOrder.acq)(g_totalCalls) >= c.expected)
                break;
            if (MonoTime.currTime > c.deadline)
            {
                fprintf(stderr, "consumer watchdog timeout\n");
                import core.stdc.stdlib : abort;
                abort();
            }
            Thread.yield();
        }
    }
    v.unsubscribe();
}

void testConcurrent()
{
    auto f = AntFarm.create(1 << 18, 8, 4, 1, 16384, 8, 4096);
    scope (exit) f.destroy();

    enum N = 6000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 5 == 0;
        immutable uint maxCs = mt ? 3 : 1;
        immutable uint done = mt ? 4 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
        expected += done;
    }

    auto p1 = new Thread({ producerMain(&p1ctx); });
    auto p2 = new Thread({ producerMain(&p2ctx); });
    p1ctx = ProdCtx(f, entries, N / 2, Tier.bulk);
    p2ctx = ProdCtx(f, entries + N / 2, N - N / 2, Tier.small);

    Thread[3] consumers;
    ConsCtx[3] cctx;
    foreach (i; 0 .. 3)
    {
        cctx[i] = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    foreach (t; consumers) t.start();
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    foreach (t; consumers) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "concurrent exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "concurrent total");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "Sub0 pulse after all consumers left");

    printf("testConcurrent OK\n"); fflush(stdout);
}

__gshared ProdCtx p1ctx, p2ctx;

// ---------------------------------------------------------------------
// Test 4: wraparound over several laps; consumer starts late (producer
// stalls on the Sub0 pulse until the consumer arrives and keeps up).
// ---------------------------------------------------------------------

void testWraparound()
{
    auto f = AntFarm.create(1 << 18, 8, 1, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    // ~20 ulongs/payload (tiny bodies pack ~17/table); 40000 payloads
    // write ~790K ulongs, lapping the 2^18 ring ~3x.
    enum N = 40000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 7 == 0;
        immutable uint maxCs = mt ? 2 : 1;
        immutable uint done = mt ? 3 : 1;
        immutable plen = 1 + i % 3;
        auto body = bodies[i * 3 .. i * 3 + plen];
        if (plen == 1)
            entries[i] = payloadEntryRuntime!testCb1(&headers[i], body, maxCs, done, i);
        else
        {
            entries[i] = payloadEntryRuntime!testCb(&headers[i], body, maxCs, done, i, done);
            entries[i].body = body;
        }
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N, Tier.small);
    auto p = new Thread({ producerMain(&p1ctx); });
    auto cctx = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });

    p.start();
    Thread.sleep(50.msecs); // let the producer run ahead and stall on Sub0
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 0, "producer ran ahead");
    check(atomicLoad!(MemoryOrder.raw)(f.Wt) < f.Ln, "producer blocked before lapping Sub0");
    c.start();
    p.join();
    c.join();

    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 2 * f.Ln, "wrapped multiple laps");
    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "wraparound exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");

    printf("testWraparound OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 5: subscription cap (spec 5a-a) and immediate reuse of slots
// ---------------------------------------------------------------------

void testSubscriptionCap()
{
    auto f = AntFarm.create(1 << 18, 8, 4, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    auto views = cast(ConsumerView*) malloc(MAX_CONSUMERS_LIMIT * ConsumerView.sizeof);
    scope (exit) free(views);
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
    {
        views[i] = ConsumerView.init;
        check(views[i].subscribe(f) >= 0, "subscribe within cap");
    }
    ConsumerView extra;
    check(extra.subscribe(f) < 0, "oversubscription fails");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == MAX_CONSUMERS_LIMIT, "Cf at cap");
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
        views[i].unsubscribe();
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "single Sub0 pulse after mass unsubscribe");
    // Slot reuse works after leaving.
    check(views[0].subscribe(f) >= 0, "resubscribe after cap");
    views[0].unsubscribe();
    printf("testSubscriptionCap OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 6: subscribe/unsubscribe churn while production is ongoing.
// Steady consumers drain the stream; churners keep joining and leaving
// (exercising the first/last subscriber and Sub0 handoff paths).
// ---------------------------------------------------------------------

__gshared shared(int) g_churnStop;

struct ChurnCtx { AntFarm* f; MonoTime deadline; }

void churnerMain(ChurnCtx* c)
{
    while (atomicLoad!(MemoryOrder.raw)(g_churnStop) == 0)
    {
        ConsumerView v;
        if (v.subscribe(c.f) >= 0)
        {
            foreach (_; 0 .. 50)
            {
                v.consumeNext();
                if (MonoTime.currTime > c.deadline)
                {
                    fprintf(stderr, "churner watchdog timeout\n");
                    import core.stdc.stdlib : abort;
                    abort();
                }
            }
            v.unsubscribe();
        }
        Thread.yield();
    }
}

void testChurn(size_t nsteady)
{
    auto f = AntFarm.create(1 << 18, 8, 6, 1, 4096, 4, 2048);
    scope (exit) f.destroy();

    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 4 == 0;
        immutable uint maxCs = mt ? 4 : 1;
        immutable uint done = mt ? 6 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
        expected += done;
    }

    atomicStore!(MemoryOrder.raw)(g_churnStop, 0);
    p1ctx = ProdCtx(f, entries, N, Tier.bulk);
    auto p = new Thread({ producerMain(&p1ctx); });

    Thread[] steady = new Thread[nsteady];
    ConsCtx[] sctx = new ConsCtx[nsteady];
    auto deadline = MonoTime.currTime + 90.seconds;
    // With the tertiary sweep, even a single steady consumer drains every
    // table; with more, shard coverage is a performance property, not a
    // completion requirement.
    foreach (i; 0 .. nsteady)
    {
        sctx[i] = ConsCtx(f, expected, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[3] churners;
    ChurnCtx[3] hctx;
    foreach (i; 0 .. 3)
    {
        hctx[i] = ChurnCtx(f, deadline);
        auto hc = &hctx[i];
        churners[i] = new Thread({ churnerMain(hc); });
    }

    foreach (t; steady) t.start();
    foreach (t; churners) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();   // steady consumers leave once all work is done
    atomicStore!(MemoryOrder.raw)(g_churnStop, 1);
    foreach (t; churners) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "churn exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained after churn");
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses == 1, "single Sub0 pulse after churn");

    printf("testChurn(%zu) OK\n", nsteady); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 7: lossless backlog. Producer outruns consumers; A consumes part
// and unsubscribes (planting Sub0 at its earliest unconfirmed segment);
// B subscribes later and drains the rest. Every payload executes exactly
// its Done times.
// ---------------------------------------------------------------------

void testBacklog()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    enum N = 3000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 6 == 0;
        immutable uint maxCs = mt ? 2 : 1;
        immutable uint done = mt ? 3 : 1;
        immutable plen = 1 + i % 3;
        auto body = bodies[i * 3 .. i * 3 + plen];
        if (plen == 1)
            entries[i] = payloadEntryRuntime!testCb1(&headers[i], body, maxCs, done, i);
        else
        {
            entries[i] = payloadEntryRuntime!testCb(&headers[i], body, maxCs, done, i, done);
            entries[i].body = body;
        }
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N, Tier.small);
    auto p = new Thread({ producerMain(&p1ctx); });
    p.start();
    Thread.sleep(30.msecs); // producer fills the buffer and stalls on Sub0

    ConsumerView a;
    check(a.subscribe(f) >= 0, "A subscribes");
    foreach (_; 0 .. 20)
        a.consumeNext();
    a.unsubscribe();
    // Last-releaser Sub0: every abandoned unconfirmed segment may pulse.
    uint pulses;
    foreach (ki; 0 .. 8)
        if (lowHalf(f, cast(uint) ki) >= SUB0) ++pulses;
    check(pulses >= 1, "Sub0 planted on abandoned unconfirmed segments");

    ConsumerView b;
    check(b.subscribe(f) >= 0, "B subscribes");
    auto deadline = MonoTime.currTime + 60.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_totalCalls) < expected)
    {
        b.consumeNext();
        if (MonoTime.currTime > deadline)
        {
            fprintf(stderr, "backlog watchdog timeout\n");
            import core.stdc.stdlib : abort;
            abort();
        }
    }
    b.unsubscribe();
    p.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "backlog exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");

    printf("testBacklog OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Test 8: tables spanning segment boundaries, including one that fully
// spans a segment. Verifies exact execution and that no references leak.
// ---------------------------------------------------------------------

void testSpannedTables()
{
    // segCap = 2^18/8 = 32768; bulk quota 110000 allows a 100k table to
    // span ~3 segments and a 34k table to span a boundary.
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 110000, 2, 1024);
    scope (exit) f.destroy();

    enum N = 64;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    scope (exit) free(headers);
    // One huge body (~100000 ulongs -> its table fully spans segments),
    // one large body (~34000 -> spans a boundary), the rest small.
    auto huge = cast(ulong*) malloc(100000 * ulong.sizeof);
    auto large = cast(ulong*) malloc(34000 * ulong.sizeof);
    auto small = cast(ulong*) malloc((N - 2) * 2 * ulong.sizeof);
    scope (exit) { free(huge); free(large); free(small); }
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) free(entries);

    long expected;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!testCb(&headers[i], 1, 1);
        ++expected;
    }
    foreach (i; 0 .. 100000) huge[i] = (i == 0) ? 0 : 1;    // payload 0: body[0] = index
    foreach (i; 0 .. 34000) large[i] = (i == 0) ? 1 : 1;    // payload 1
    huge[0] = 0; large[0] = 1;
    entries[0].header = &headers[0]; entries[0].body = huge[0 .. 100000];
    entries[1].header = &headers[1]; entries[1].body = large[0 .. 34000];
    foreach (i; 2 .. N)
    {
        small[(i - 2) * 2] = i;
        small[(i - 2) * 2 + 1] = 1;
        entries[i].header = &headers[i]; entries[i].body = small[(i - 2) * 2 .. (i - 2) * 2 + 2];
    }
    // Order: some small tables, the huge one, the large one, more small.
    // (entries already in that order: 0 huge, 1 large, then small.)

    auto cctx = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });
    c.start();
    auto tok = f.registerProducer(Tier.bulk);
    check(tok.valid, "register bulk");
    size_t off;
    while (off < N)
    {
        immutable n = f.write(entries[off .. N], tok);
        off += n;
        if (n == 0) { Thread.yield(); }
    }
    f.unregisterProducer(tok);
    c.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == 1, "spanned exact call count");
    foreach (ki; 0 .. 8)
        check((atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & COUNTMASK) == 0, "no leaked refs");
    printf("testSpannedTables OK\n"); fflush(stdout);
}

// Writes at most 12 entries per call, so every table is a small table.
void smallProducerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid) fatal("producer overregistration");
    size_t off;
    while (off < c.count)
    {
        immutable batch = c.count - off < 12 ? c.count - off : 12;
        immutable n = c.f.write(c.entries[off .. off + batch], tok);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    c.f.unregisterProducer(tok);
}

// Small tables only: starvation of the single shard 0 under churn, covered
// by the carried sweeper role (and the idle re-walk backstop).
void testSmallTableChurn()
{
    auto f = AntFarm.create(1 << 18, 8, 6, 1, 4096, 4, 2048);
    scope (exit) f.destroy();

    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 4 == 0;
        immutable uint maxCs = mt ? 4 : 1;
        immutable uint done = mt ? 6 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
        expected += done;
    }

    atomicStore!(MemoryOrder.raw)(g_churnStop, 0);
    p1ctx = ProdCtx(f, entries, N, Tier.bulk);
    auto p = new Thread({ smallProducerMain(&p1ctx); });

    Thread[2] steady;
    ConsCtx[2] sctx;
    auto deadline = MonoTime.currTime + 90.seconds;
    foreach (i; 0 .. 2)
    {
        sctx[i] = ConsCtx(f, expected, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[2] churners;
    ChurnCtx[2] hctx;
    foreach (i; 0 .. 2)
    {
        hctx[i] = ChurnCtx(f, deadline);
        auto hc = &hctx[i];
        churners[i] = new Thread({ churnerMain(hc); });
    }

    foreach (t; steady) t.start();
    foreach (t; churners) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();
    atomicStore!(MemoryOrder.raw)(g_churnStop, 1);
    foreach (t; churners) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "small-table churn exact call count");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained after small-table churn");
    foreach (ki; 0 .. 8)
        check((atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]) & COUNTMASK) == 0, "no leaked refs");

    printf("testSmallTableChurn OK\n"); fflush(stdout);
}

// H1: two small producers after the C1 idle-pin migrate. Previously a stale
// position pin on a spanning table's start starved refreshQuota.
void testMultiSmallProducers()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 8192, 2, 4096);
    scope (exit) f.destroy();

    enum N = 2000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        immutable mt = i % 5 == 0;
        immutable uint maxCs = mt ? 3 : 1;
        immutable uint done = mt ? 4 : 1;
        entries[i] = payloadEntryRuntime!testCb(
            &headers[i], bodies[i * 2 .. i * 2 + 2], maxCs, done, i, done);
        expected += done;
    }

    p1ctx = ProdCtx(f, entries, N / 2, Tier.small);
    p2ctx = ProdCtx(f, entries + N / 2, N - N / 2, Tier.small);
    auto p1 = new Thread({ producerMain(&p1ctx); });
    auto p2 = new Thread({ producerMain(&p2ctx); });
    ConsCtx[2] cctx;
    Thread[2] consumers;
    foreach (i; 0 .. 2)
    {
        cctx[i] = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    foreach (t; consumers) t.start();
    p1.start();
    p2.start();
    p1.join();
    p2.join();
    foreach (t; consumers) t.join();

    foreach (i; 0 .. N)
        check(g_calls[i] == headers[i].done, "multi-small exact call count");
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "multi-small total");
    check(atomicLoad!(MemoryOrder.raw)(f.Cf) == 0, "Cf drained");
    printf("testMultiSmallProducers OK\n"); fflush(stdout);
}

// ---------------------------------------------------------------------
// Experimental actor payload: non-GC storage, serial mutation, retirement
// ---------------------------------------------------------------------

class ActorConsumerJob
{
    AntFarm* farm;
    shared int* stop;
    MonoTime deadline;

    this(AntFarm* actorFarm, shared int* stopFlag, MonoTime limit)
    {
        farm = actorFarm;
        stop = stopFlag;
        deadline = limit;
    }

    void run()
    {
        ConsumerView view;
        if (view.subscribe(farm) < 0)
            fatal("actor consumer subscribe failed");
        while (atomicLoad!(MemoryOrder.acq)(*stop) == 0)
        {
            if (!view.consumeNext())
            {
                if (MonoTime.currTime >= deadline)
                    fatal("actor consumer timeout");
                Thread.yield();
            }
        }
        while (view.consumeNext()) {}
        view.unsubscribe();
    }
}

class ActorInboxProducerJob
{
    ActorHandle!ActorInboxState handle;
    ActorInboxMessage* messages;
    size_t acceptedCount;
    size_t closedCount;
    shared long* ready;
    shared int* releaseClosedPhase;

    this(ActorHandle!ActorInboxState actorHandle,
            ActorInboxMessage* first, size_t accepted,
            size_t closed, shared long* readyCount,
            shared int* releaseFlag)
    {
        handle = actorHandle;
        messages = first;
        acceptedCount = accepted;
        closedCount = closed;
        ready = readyCount;
        releaseClosedPhase = releaseFlag;
    }

    void run()
    {
        foreach (i; 0 .. acceptedCount)
        {
            immutable result = handle.send(&messages[i].node);
            if (result == ActorSendResult.queued
                    || result == ActorSendResult.coalesced)
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxAccepted, 1L);
            else
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxUnexpected, 1L);
        }

        atomicFetchAdd!(MemoryOrder.rel)(*ready, 1L);
        while (atomicLoad!(MemoryOrder.acq)(*releaseClosedPhase) == 0)
            Thread.yield();

        foreach (i; acceptedCount .. acceptedCount + closedCount)
        {
            immutable result = handle.send(&messages[i].node);
            if (result == ActorSendResult.closed)
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxClosed, 1L);
            else
                atomicFetchAdd!(MemoryOrder.rel)(g_inboxUnexpected, 1L);
        }
    }
}

void testActorPayload()
{
    enum consumerCount = 4;
    enum target = 500;
    auto farm = AntFarm.create(1 << 18, 8, consumerCount,
        0, 0, 1, 4096);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, 1, allocator);
    check(actors !is null, "actor runtime allocation");

    atomicStore!(MemoryOrder.raw)(g_actorEntries, 0L);
    atomicStore!(MemoryOrder.raw)(g_actorConcurrent, 0L);
    atomicStore!(MemoryOrder.raw)(g_actorCalls, 0L);
    auto owner = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, target));
    check(owner.valid, "actor creation");
    auto exhausted = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(!exhausted.valid, "actor capacity exhaustion is explicit");
    auto oldHandle = owner.handle;
    immutable warmAllocations = counts.allocations;
    check(oldHandle.wake() == ActorWakeResult.queued,
        "first actor wake queues");

    shared int stop;
    auto deadline = MonoTime.currTime + 30.seconds;
    ActorConsumerJob[consumerCount] consumerJobs;
    Thread[consumerCount] consumers;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new ActorConsumerJob(farm, &stop, deadline);
        consumers[i] = new Thread(&consumerJobs[i].run);
        consumers[i].start();
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "actor producer registration");
    GC.disable();
    while (!owner.retired)
    {
        actors.flush(token, 32, 2);
        // Exercise coalescing while the actor is already queued or running.
        immutable wakeResult = oldHandle.wake();
        check(wakeResult == ActorWakeResult.coalesced
                || wakeResult == ActorWakeResult.closed,
            "actor repeated wake coalesces");
        if (MonoTime.currTime >= deadline)
            fatal("actor retirement timeout");
        Thread.yield();
    }
    GC.enable();
    atomicStore!(MemoryOrder.rel)(stop, 1);
    foreach (thread; consumers) thread.join();
    farm.unregisterProducer(token);

    check(atomicLoad!(MemoryOrder.acq)(g_actorEntries) == 0,
        "actor entry counter drained");
    check(atomicLoad!(MemoryOrder.acq)(g_actorConcurrent) == 0,
        "one exclusive actor borrow at a time");
    check(atomicLoad!(MemoryOrder.acq)(g_actorCalls) == target,
        "actor external state mutated exactly once per activation");
    check(counts.allocations == warmAllocations,
        "actor warm path performs no allocator calls");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "actor explicit reclamation");
    check(oldHandle.wake() == ActorWakeResult.staleHandle,
        "reclaimed actor handle is stale");

    // Reuse the stable slot and prove the old generation cannot affect it.
    auto replacement = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(replacement.valid, "actor slot reuse");
    auto replacementHandle = replacement.handle;
    check(replacementHandle.generation != oldHandle.generation,
        "actor generation advances on reuse");
    check(oldHandle.wake() == ActorWakeResult.staleHandle,
        "old actor generation stays stale after reuse");

    ConsumerView replacementView;
    check(replacementView.subscribe(farm) >= 0,
        "replacement actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    check(replacementHandle.wake() == ActorWakeResult.queued,
        "replacement actor wake");
    while (!replacement.retired)
    {
        actors.flush(token);
        while (replacementView.consumeNext()) {}
    }
    replacementView.unsubscribe();
    farm.unregisterProducer(token);
    check(replacement.reclaim() == ActorReclaimResult.reclaimed,
        "replacement actor reclamation");

    // Retirement of a scheduled actor closes it without entering user code.
    auto closed = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 10));
    auto closedHandle = closed.handle;
    immutable callsBeforeClose = atomicLoad!(MemoryOrder.acq)(g_actorCalls);
    check(closedHandle.wake() == ActorWakeResult.queued,
        "scheduled actor before close");
    check(closed.requestRetire() == ActorRetireResult.requested,
        "scheduled actor retirement requested");
    ConsumerView closedView;
    check(closedView.subscribe(farm) >= 0,
        "closed actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    while (!closed.retired)
    {
        actors.flush(token);
        while (closedView.consumeNext()) {}
    }
    closedView.unsubscribe();
    farm.unregisterProducer(token);
    check(atomicLoad!(MemoryOrder.acq)(g_actorCalls) == callsBeforeClose,
        "closed scheduled actor does not enter user code");
    check(closed.reclaim() == ActorReclaimResult.reclaimed,
        "closed actor reclamation");

    // An idle actor retires synchronously because no activation owns it.
    auto idle = actors.createActor!(ActorCounterState, actorCounter)(
        ActorCounterState(0, 1));
    check(idle.requestRetire() == ActorRetireResult.requested
            && idle.retired,
        "idle actor retirement");
    check(idle.reclaim() == ActorReclaimResult.reclaimed,
        "idle actor reclamation");
    check(actors.live == 0 && actors.ready == 0,
        "actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "actor allocator pairing");

    printf("testActorPayload OK\n"); fflush(stdout);
}

void testActorInbox()
{
    enum consumerCount = 4;
    enum producerCount = 4;
    enum acceptedPerProducer = 256;
    enum closedPerProducer = 128;
    enum messagesPerProducer = acceptedPerProducer + closedPerProducer;
    enum messageCount = producerCount * messagesPerProducer;

    auto farm = AntFarm.create(1 << 18, 8, consumerCount,
        0, 0, 1, 4096);
    scope (exit) farm.destroy();

    ActorAllocCounts counts;
    auto allocator = ActorAllocator(&counts,
        &actorTestAllocate, &actorTestDeallocate);
    auto actors = ActorRuntime.create(farm, 1, allocator);
    check(actors !is null, "inbox actor runtime allocation");
    auto owner = actors.createActor!(ActorInboxState, inboxActor)(
        ActorInboxState.init);
    check(owner.valid, "inbox actor creation");
    auto handle = owner.handle;
    immutable warmAllocations = counts.allocations;

    auto messages = cast(ActorInboxMessage*) malloc(
        messageCount * ActorInboxMessage.sizeof);
    check(messages !is null, "inbox message allocation");
    scope (exit) free(messages);
    memset(messages, 0, messageCount * ActorInboxMessage.sizeof);
    ulong expectedSum;
    foreach (producer; 0 .. producerCount)
    {
        foreach (i; 0 .. messagesPerProducer)
        {
            immutable index = producer * messagesPerProducer + i;
            messages[index].value = index + 1;
            messages[index].node.initialize(&messages[index], index);
            if (i < acceptedPerProducer)
                expectedSum += messages[index].value;
        }
    }

    atomicStore!(MemoryOrder.raw)(g_inboxEntries, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxConcurrent, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxActivations, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxConsumed, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxSum, 0UL);
    atomicStore!(MemoryOrder.raw)(g_inboxAccepted, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxClosed, 0L);
    atomicStore!(MemoryOrder.raw)(g_inboxUnexpected, 0L);

    ActorInboxMessage busyProbe;
    busyProbe.value = messageCount + 1;
    busyProbe.node.initialize(&busyProbe, messageCount);
    immutable probeResult = handle.send(&busyProbe.node);
    check(probeResult == ActorSendResult.queued
            || probeResult == ActorSendResult.coalesced,
        "first inbox node submission accepted");
    atomicFetchAdd!(MemoryOrder.rel)(g_inboxAccepted, 1L);
    expectedSum += busyProbe.value;
    check(handle.send(&busyProbe.node) == ActorSendResult.nodeBusy,
        "owned inbox node cannot be submitted twice");

    shared int stopConsumers;
    shared int releaseClosedPhase;
    shared long producersReady;
    auto deadline = MonoTime.currTime + 30.seconds;
    ActorConsumerJob[consumerCount] consumerJobs;
    Thread[consumerCount] consumers;
    foreach (i; 0 .. consumerCount)
    {
        consumerJobs[i] = new ActorConsumerJob(
            farm, &stopConsumers, deadline);
        consumers[i] = new Thread(&consumerJobs[i].run);
    }
    ActorInboxProducerJob[producerCount] producerJobs;
    Thread[producerCount] producers;
    foreach (i; 0 .. producerCount)
    {
        producerJobs[i] = new ActorInboxProducerJob(handle,
            messages + i * messagesPerProducer,
            acceptedPerProducer, closedPerProducer,
            &producersReady, &releaseClosedPhase);
        producers[i] = new Thread(&producerJobs[i].run);
    }

    auto token = farm.registerProducer(Tier.small);
    check(token.valid, "inbox producer registration");
    foreach (thread; consumers) thread.start();
    foreach (thread; producers) thread.start();

    GC.disable();
    while (atomicLoad!(MemoryOrder.acq)(producersReady) != producerCount)
    {
        actors.flush(token, 32, 2);
        if (MonoTime.currTime >= deadline)
            fatal("actor inbox producer timeout");
        Thread.yield();
    }

    check(owner.requestRetire() == ActorRetireResult.requested,
        "inbox actor retirement closes admission");
    atomicStore!(MemoryOrder.rel)(releaseClosedPhase, 1);
    while (!owner.retired)
    {
        actors.flush(token, 32, 2);
        if (MonoTime.currTime >= deadline)
            fatal("actor inbox retirement timeout");
        Thread.yield();
    }
    GC.enable();

    foreach (thread; producers) thread.join();
    atomicStore!(MemoryOrder.rel)(stopConsumers, 1);
    foreach (thread; consumers) thread.join();
    farm.unregisterProducer(token);

    immutable acceptedTotal = producerCount * acceptedPerProducer + 1;
    immutable closedTotal = producerCount * closedPerProducer;
    check(atomicLoad!(MemoryOrder.acq)(g_inboxUnexpected) == 0,
        "inbox sends have deterministic outcomes");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxAccepted) == acceptedTotal,
        "all open-generation sends accepted");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxClosed) == closedTotal,
        "all post-close sends rejected");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxConsumed) == acceptedTotal,
        "all accepted interactions consumed exactly once");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxSum) == expectedSum,
        "inbox payload contents published to actor");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxEntries) == 0
            && atomicLoad!(MemoryOrder.acq)(g_inboxConcurrent) == 0,
        "inbox actor retains exclusive callback ownership");
    check(atomicLoad!(MemoryOrder.acq)(g_inboxActivations) > 1,
        "bounded inbox drain republishes activations");
    foreach (i; 0 .. messageCount)
        check(messages[i].node.available,
            "accepted and rejected inbox nodes returned to caller");
    check(busyProbe.node.available,
        "busy-probe inbox node returned to caller");
    check(handle.send(&messages[0].node) == ActorSendResult.closed,
        "retired generation rejects an available node");
    check(counts.allocations == warmAllocations,
        "inbox warm path performs no actor allocator calls");
    check(owner.reclaim() == ActorReclaimResult.reclaimed,
        "inbox actor explicit reclamation");
    check(handle.send(&messages[0].node) == ActorSendResult.staleHandle,
        "reclaimed inbox handle is stale");

    // Leave one accepted interaction queued while retirement closes the
    // generation. With no consumer active, RETIRED must wait for that node.
    auto drainOwner = actors.createActor!(ActorInboxState, inboxActor)(
        ActorInboxState.init);
    check(drainOwner.valid, "inbox actor slot reuse");
    auto drainHandle = drainOwner.handle;
    ActorInboxMessage retirementProbe;
    retirementProbe.value = 1;
    retirementProbe.node.initialize(&retirementProbe);
    immutable drainSend = drainHandle.send(&retirementProbe.node);
    check(drainSend == ActorSendResult.queued
            || drainSend == ActorSendResult.coalesced,
        "pre-close inbox interaction accepted");
    check(drainOwner.requestRetire() == ActorRetireResult.requested
            && !drainOwner.retired,
        "retirement waits for accepted inbox interaction");
    check(drainHandle.send(&messages[0].node) == ActorSendResult.closed,
        "closing reused generation rejects new interaction");

    ConsumerView drainView;
    check(drainView.subscribe(farm) >= 0,
        "closing inbox actor consumer subscribe");
    token = farm.registerProducer(Tier.small);
    check(token.valid, "closing inbox producer registration");
    while (!drainOwner.retired)
    {
        actors.flush(token);
        while (drainView.consumeNext()) {}
        if (MonoTime.currTime >= deadline)
            fatal("closing inbox drain timeout");
    }
    drainView.unsubscribe();
    farm.unregisterProducer(token);
    check(retirementProbe.node.available,
        "accepted interaction completed before retirement");
    check(drainOwner.reclaim() == ActorReclaimResult.reclaimed,
        "closing inbox actor reclamation");
    check(actors.live == 0 && actors.ready == 0,
        "inbox actor runtime drained");
    actors.destroy();
    check(counts.allocations == counts.deallocations,
        "inbox actor allocator pairing");

    printf("testActorInbox OK\n"); fflush(stdout);
}

void main()
{
    testMagicWrap();
    testArithmetic();
    testSingleThreaded();
    testInputRangeWrite();
    testPayloadRange();
    testUniformRanges();
    testSeparateRangesAndQuantum();
    testConcurrent();
    testWraparound();
    testSubscriptionCap();
    testChurn(3);
    testChurn(1);
    testBacklog();
    testSpannedTables();
    testSmallTableChurn();
    testMultiSmallProducers();
    testActorPayload();
    testActorInbox();
    printf("ALL TESTS PASSED\n");
}
