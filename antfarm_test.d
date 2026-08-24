/++
 + Tests for the Ant Farm. Run: dmd -g -O -inline antfarm.d antfarm_templates.d antfarm_test.d -of=antfarm_test && ./antfarm_test
 +/
module antfarm_test;

import antfarm_templates;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdio : fflush, stdout;
import core.stdc.stdlib : malloc, free;

// ---------------------------------------------------------------------
// Shared test state
// ---------------------------------------------------------------------

__gshared shared(long)* g_calls;   // per-payload call counts
__gshared size_t g_npayloads;
__gshared shared(long) g_totalCalls;
__gshared ulong* g_keepalive;      // keeps GC away (we malloc instead)

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

void main()
{
    testMagicWrap();
    testArithmetic();
    testSingleThreaded();
    testInputRangeWrite();
    testConcurrent();
    testWraparound();
    testSubscriptionCap();
    testChurn(3);
    testChurn(1);
    testBacklog();
    testSpannedTables();
    testSmallTableChurn();
    testMultiSmallProducers();
    printf("ALL TESTS PASSED\n");
}
