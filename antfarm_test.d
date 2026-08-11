/++
 + Tests for the Ant Farm. Run: dmd -g -O -inline antfarm.d antfarm_test.d -of=antfarm_test && ./antfarm_test
 +/
module antfarm_test;

import antfarm;
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

long testCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    atomicFetchAdd(g_calls[cast(size_t) b[0]], 1L);
    atomicFetchAdd(g_totalCalls, 1L);
    return 1;
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

// ---------------------------------------------------------------------
// Test 1: arithmetic
// ---------------------------------------------------------------------

void testArithmetic()
{
    check(sqcsOf(1) == 1, "sqcsOf(1)");
    check(sqcsOf(4) == 1, "sqcsOf(4)");
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
    auto f = AntFarm.create(1 << 16, 8, 2, 1, 8192, 4, 2048);
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
        headers[i] = PayloadHeader.init;
        headers[i].maxCs = (i >= 18) ? 3 : 1;   // two MT payloads
        headers[i].done = (i >= 18) ? 5 : 1;
        headers[i].call = &testCb;
        bodies[i * 2] = i;
        bodies[i * 2 + 1] = headers[i].done;
        entries[i].header = &headers[i]; entries[i].body = bodies[i * 2 .. i * 2 + 2];
    }
    immutable expected = 18 * 1 + 2 * 5;

    ConsumerView v;
    immutable r = v.subscribe(f);
    check(r == 0, "first subscribe returns epoch 0");
    // First subscriber cleared Sub0 and holds one consumer reference.
    check(lowHalf(f, 0) == 1, "Sub0 cleared, one consumer ref");

    check(f.registerProducer(Tier.small) >= 0, "register small producer");
    ulong exi = 0;
    check(f.write(entries[0 .. 10], exi, Tier.small) == 10, "write first 10");
    check(f.write(entries[10 .. 20], exi, Tier.small) == 10, "write second 10");

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
    while (v2.consumeNext()) {}
    // No payload was re-executed: tables were already complete.
    check(atomicLoad!(MemoryOrder.raw)(g_totalCalls) == expected, "no re-execution");
    v2.unsubscribe();

    printf("testSingleThreaded OK\n"); fflush(stdout);
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
    auto reg = c.f.registerProducer(c.tier);
    if (reg < 0) fatal("producer overregistration");
    ulong exi = 0;
    size_t off;
    while (off < c.count)
    {
        immutable n = c.f.write(c.entries[off .. c.count], exi, c.tier);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    c.f.unregisterProducer(c.tier);
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
        headers[i] = PayloadHeader.init;
        immutable mt = i % 5 == 0;
        headers[i].maxCs = mt ? 3 : 1;
        headers[i].done = mt ? 4 : 1;
        headers[i].call = &testCb;
        bodies[i * 2] = i;
        bodies[i * 2 + 1] = headers[i].done;
        entries[i].header = &headers[i]; entries[i].body = bodies[i * 2 .. i * 2 + 2];
        expected += headers[i].done;
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
    auto f = AntFarm.create(1 << 14, 8, 1, 1, 2048, 2, 1024);
    scope (exit) f.destroy();

    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();

    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * 3 * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    scope (exit) { free(headers); free(bodies); free(entries); }
    long expected;
    foreach (i; 0 .. N)
    {
        headers[i] = PayloadHeader.init;
        immutable mt = i % 7 == 0;
        headers[i].maxCs = mt ? 2 : 1;
        headers[i].done = mt ? 3 : 1;
        headers[i].call = &testCb;
        immutable plen = 1 + i % 3;
        bodies[i * 3] = i;
        bodies[i * 3 + 1] = headers[i].done;
        entries[i].header = &headers[i]; entries[i].body = bodies[i * 3 .. i * 3 + plen];
        expected += headers[i].done;
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
    auto f = AntFarm.create(1 << 14, 8, 4, 1, 2048, 2, 1024);
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
    auto f = AntFarm.create(1 << 16, 8, 6, 1, 4096, 4, 2048);
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
        headers[i] = PayloadHeader.init;
        immutable mt = i % 4 == 0;
        headers[i].maxCs = mt ? 4 : 1;
        headers[i].done = mt ? 6 : 1;
        headers[i].call = &testCb;
        bodies[i * 2] = i;
        bodies[i * 2 + 1] = headers[i].done;
        entries[i].header = &headers[i]; entries[i].body = bodies[i * 2 .. i * 2 + 2];
        expected += headers[i].done;
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

void main()
{
    testArithmetic();
    testSingleThreaded();
    testConcurrent();
    testWraparound();
    testSubscriptionCap();
    testChurn(3);
    testChurn(1);
    printf("ALL TESTS PASSED\n");
}
