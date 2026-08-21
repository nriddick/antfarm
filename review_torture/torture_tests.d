/++
 + Adversarial torture suite for antfarm (gen6 / spec2).
 +
 +   make -C review_torture run
 +
 + Exit 0 = all green. Exit 2 = correctness green, defect probes confirmed.
 + Abort/1 = correctness failure.
 +/
module torture_tests;

import antfarm_templates;
import torture_common;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, abort, exit;

__gshared shared(int) g_defects;

// T21: payload-body content verification over several laps of the ring.
// Shared counters must be __gshared shared(T) (see torture_common.d).
__gshared shared(long) g_contentSum;
__gshared shared(long) g_contentBad;

private enum size_t T21_BODY_LEN = 32;
private enum size_t T21_N = 30000;

long contentSumCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    // Count the call first so a corrupted body still advances the global
    // total and consumers do not spin until the watchdog.
    atomicFetchAdd(g_totalCalls, 1L);

    // Corrupted plen could make the body slice empty or huge; never index
    // before validating the length.
    if (b.length != T21_BODY_LEN)
    {
        atomicFetchAdd(g_contentBad, 1L);
        return 0;
    }

    immutable idx = cast(size_t) b[0];
    if (idx < g_npayloads)
        atomicFetchAdd(g_calls[idx], 1L);
    else
        atomicFetchAdd(g_contentBad, 1L);

    // Body is a run of numbers: b[k] == idx + k for all k.
    ulong sum = 0;
    bool ok = true;
    foreach (k, v; b)
    {
        sum += v;
        if (v != cast(ulong) idx + k)
            ok = false;
    }
    if (!ok)
        atomicFetchAdd(g_contentBad, 1L);
    atomicFetchAdd(g_contentSum, cast(long) sum);
    return 1;
}

void defect(const(char)[] id, const(char)[] msg)
{
    atomicFetchAdd(g_defects, 1);
    fprintf(stderr, "DEFECT %.*s: %.*s\n",
        cast(int) id.length, id.ptr, cast(int) msg.length, msg.ptr);
    fflush(stderr);
}

// ---------------- defect probes (non-fatal) ----------------

void t01_pcount_field_carry()
{
    // Retarget: write() must fatal (abort) on Done/MaxCs > 512. A clean
    // return is a miss; SIGILL/SIGSEGV is still a smash.
    import core.sys.posix.unistd : fork, _exit;
    import core.sys.posix.sys.wait : waitpid;
    import core.sys.posix.signal : SIGABRT;

    auto f = AntFarm.create(1 << 18, 4, 1, 1, 2048, 1, 512);
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "T01 reg");

    bool expectAbort(uint done, uint maxCs, const(char)[] label)
    {
        PayloadHeader h;
        initPayloadHeader!countingCb(&h, maxCs, done);
        ulong body = 0;
        PayloadEntry e;
        e.header = &h;
        e.body = (&body)[0 .. 1];
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            _exit(0);
        }
        int st;
        waitpid(pid, &st, 0);
        immutable signaled = ((st & 0x7f) != 0) && ((st & 0x7f) != 0x7f);
        immutable sig = st & 0x7f;
        if (signaled && sig == SIGABRT)
            return true;
        char[96] buf;
        auto n = snprintf(buf.ptr, buf.length, "%.*s: st=%d signaled=%d sig=%d",
            cast(int) label.length, label.ptr, st, signaled, sig);
        defect("T01", buf[0 .. n]);
        return false;
    }

    immutable okDone = expectAbort(MAX_PAYLOAD_ITERS + 1, 1, "Done>512");
    immutable okCs = expectAbort(1, MAX_PAYLOAD_ITERS + 1, "MaxCs>512");
    f.unregisterProducer(tok);
    f.destroy();
    say((okDone && okCs) ? "T01 write rejects >512 OK" : "T01 DEFECT confirmed");
}

void t02_done_wrap_overexec()
{
    auto f = AntFarm.create(1 << 18, 8, 8, 1, 8192, 4, 2048);
    scope (exit) f.destroy();
    allocCalls(1);
    scope (exit) freeCalls();

    enum uint DONE = MAX_PAYLOAD_ITERS;
    auto batch = makeBatch(1, (size_t i, ref PayloadHeader h, ref size_t plen) {
        h.maxCs = 32;
        h.done = DONE;
        plen = 2;
    });
    scope (exit) freeBatch(batch);

    enum NC = 16;
    ConsumerView[NC] vs;
    foreach (i; 0 .. NC)
        check(vs[i].subscribe(f) >= 0, "sub");
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "reg");
    while (f.write(batch.entries[0 .. 1], tok) == 0)
        Thread.yield();
    f.unregisterProducer(tok);

    auto deadline = MonoTime.currTime + 15.seconds;
    while (MonoTime.currTime < deadline)
    {
        foreach (i; 0 .. NC)
            vs[i].consumeNext();
        if (atomicLoad(g_totalCalls) > DONE + 1000)
            break;
    }
    foreach (i; 0 .. NC)
        vs[i].unsubscribe();

    immutable got = atomicLoad(g_calls[0]);
    check(got == DONE, "T02 exact calls at Done=512");
    say("T02 Done=512 exact OK");
}

void t16_position_ref_stall()
{
    // Large Exmax stall geometry: segCap = Ln/K = 2^18/8 = 32768 and
    // Exmax = 2*81920 = 163840 ~= 5*segCap, so the idle re-walk must
    // migrate a confirmed position pin forward or refreshQuota's forward
    // sweep breaks at it and the producer stalls.
    auto f = AntFarm.create(1 << 18, 8, 2, 2, 81920, 0, 0);
    scope (exit) f.destroy();

    enum N = 120;
    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    ulong*[] bodies;
    bodies.length = N;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!countingCb(&headers[i], 1, 1);
        bodies[i] = cast(ulong*) malloc(800 * ulong.sizeof);
        bodies[i][0] = i;
        entries[i].header = &headers[i];
        entries[i].body = bodies[i][0 .. 800];
    }
    allocCalls(N);
    scope (exit) freeCalls();

    ConsumerView[2] cs;
    foreach (i; 0 .. 2)
        check(cs[i].subscribe(f) >= 0, "sub");

    shared size_t written;
    shared int prod_done;
    auto prod = new Thread({
        auto tok = f.registerProducer(Tier.bulk);
        check(tok.valid, "reg");
        size_t off, stall;
        while (off < N)
        {
            immutable n = f.write(entries[off .. N], tok);
            off += n;
            atomicStore(written, off);
            if (n == 0)
            {
                if (++stall >= 200_000)
                    break;
                Thread.yield();
            }
            else
                stall = 0;
        }
        f.unregisterProducer(tok);
        atomicStore(prod_done, off >= N ? 1 : -1);
    });
    prod.start();

    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(prod_done) == 0 && MonoTime.currTime < deadline)
    {
        foreach (i; 0 .. 2)
            cs[i].consumeNext();
        Thread.yield();
    }
    foreach (_; 0 .. 5000)
        foreach (i; 0 .. 2)
            cs[i].consumeNext();
    prod.join();

    if (atomicLoad(prod_done) != 1)
    {
        char[160] buf;
        auto n = snprintf(buf.ptr, buf.length,
            "producer stalled idle-at-Wt written=%zu/%d total=%lld next=%llu trailN=%u holdKi=%u",
            atomicLoad(written), N, atomicLoad(g_totalCalls),
            cs[0].nextSeq, cs[0].trailN, cs[0].curKi);
        fprintf(stderr, "T16 FAIL %.*s\n", n, buf.ptr);
        foreach (ki; 0 .. 8)
            fprintf(stderr, "  ki=%u rt=%llx es=%lld seqt=%llu sd=%llu\n",
                ki, atomicLoad(f.Rt[ki][0]), atomicLoad(f.stats[ki].es),
                atomicLoad(f.stats[ki].seqt), atomicLoad(f.stats[ki].sd));
        abort();
    }
    say("T16 producer completed OK");

    foreach (i; 0 .. 2)
        cs[i].unsubscribe();
    foreach (i; 0 .. N)
        free(bodies[i]);
    free(headers);
    free(entries);
}

// ---------------- correctness ----------------

void runPair(AntFarm* f, PayloadBatch* batch, uint nCons, Tier tier, size_t maxBatch, int wdSecs)
{
    Thread[] consumers = new Thread[nCons];
    ConsCtx[] cctx = new ConsCtx[nCons];
    auto deadline = MonoTime.currTime + wdSecs.seconds;
    foreach (i; 0 .. nCons)
    {
        cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    ProdCtx pctx = ProdCtx(f, batch.entries, batch.count, tier, maxBatch);
    auto p = new Thread({ producerMain(&pctx); });
    foreach (t; consumers) t.start();
    p.start();
    p.join();
    foreach (t; consumers) t.join();
}

void t03_concurrent_exact()
{
    // Closest to antfarm_test.testConcurrent geometry.
    auto f = AntFarm.create(1 << 18, 8, 4, 1, 16384, 8, 4096);
    scope (exit) f.destroy();
    enum N = 6000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 5 == 0;
        h.maxCs = mt ? 3 : 1;
        h.done = mt ? 4 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);

    // split bulk+small like original
    Thread[3] consumers;
    ConsCtx[3] cctx;
    auto deadline = MonoTime.currTime + 90.seconds;
    foreach (i; 0 .. 3)
    {
        cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    ProdCtx p1 = ProdCtx(f, batch.entries, N / 2, Tier.bulk);
    ProdCtx p2 = ProdCtx(f, batch.entries + N / 2, N - N / 2, Tier.small);
    auto t1 = new Thread({ producerMain(&p1); });
    auto t2 = new Thread({ producerMain(&p2); });
    foreach (t; consumers) t.start();
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    foreach (t; consumers) t.join();

    expectExactCalls(&batch, "T03");
    check(countSub0Pulses(f) == 1, "T03 pulse");
    expectNoLiveConsumerRefs(f, "T03");
    say("T03 concurrent exact OK");
}

void t04_many_consumers_two_producers()
{
    // High consumer count + bulk/small producers. Multi-small-producer
    // geometries repeatedly stalled near completion (see review); kept out
    // of the green path until the position-ref / leftover-claim story is fixed.
    auto f = AntFarm.create(1 << 18, 8, 16, 1, 16384, 8, 4096);
    scope (exit) f.destroy();
    enum N = 3000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 7 == 0;
        h.maxCs = mt ? 4 : 1;
        h.done = mt ? 5 : 1;
        plen = 2 + (i % 4);
    });
    scope (exit) freeBatch(batch);

    enum NC = 12;
    Thread[NC] consumers;
    ConsCtx[NC] cctx;
    auto deadline = MonoTime.currTime + 120.seconds;
    foreach (i; 0 .. NC)
    {
        cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    ProdCtx p1 = ProdCtx(f, batch.entries, N / 2, Tier.bulk, 24);
    ProdCtx p2 = ProdCtx(f, batch.entries + N / 2, N - N / 2, Tier.small, 6);
    auto t1 = new Thread({ producerMain(&p1); });
    auto t2 = new Thread({ producerMain(&p2); });
    foreach (t; consumers) t.start();
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    foreach (t; consumers) t.join();
    expectExactCalls(&batch, "T04");
    expectNoLiveConsumerRefs(f, "T04");
    say("T04 many consumers two producers OK");
}

void t05_zero_consumer_gap()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 2048, 2, 1024);
    scope (exit) f.destroy();
    enum N = 1600;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 6 == 0;
        h.maxCs = mt ? 2 : 1;
        h.done = mt ? 3 : 1;
        plen = 2 + (i % 3);
    });
    scope (exit) freeBatch(batch);

    ConsumerView a;
    check(a.subscribe(f) >= 0, "A");
    ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.small);
    auto p = new Thread({ producerMain(&pctx); });
    p.start();
    Thread.sleep(25.msecs);
    foreach (_; 0 .. 40)
        a.consumeNext();
    a.unsubscribe();
    check(atomicLoad(f.Cf) == 0, "gap");
    check(countSub0Pulses(f) >= 1, "pulse");
    Thread.sleep(40.msecs);

    ConsCtx cctx = ConsCtx(f, batch.expectedCalls, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });
    c.start();
    p.join();
    c.join();
    expectExactCalls(&batch, "T05");
    expectNoLiveConsumerRefs(f, "T05");
    say("T05 zero-consumer gap OK");
}

__gshared shared(int) g_stormStop;

// Explicit class instead of a loop-local delegate: D closures capture
// loop-body locals by reference, so the old `auto hc = &hctx[i];
// new Thread({ stormerMain(hc); })` made every thread use the last
// hctx slot and race on its `cycles`.  A class instance per thread has
// its own fields and is joined before the main thread reads them.
class StormJob
{
    AntFarm* f;
    MonoTime deadline;
    uint burst;
    bool varyBurst;
    int cycles;

    this(AntFarm* farm, MonoTime limit, uint burstCount, bool vary = false)
    {
        f = farm;
        deadline = limit;
        burst = burstCount;
        varyBurst = vary;
    }

    void run()
    {
        int n;
        while (!atomicLoad(g_stormStop))
        {
            ConsumerView v;
            if (v.subscribe(f) >= 0)
            {
                immutable count = varyBurst
                    ? 6 + cast(uint)(cast(size_t)&v & 7)
                    : burst;
                foreach (_; 0 .. count)
                {
                    v.consumeNext();
                    if (MonoTime.currTime > deadline)
                        abort();
                }
                v.unsubscribe();
                ++n;
            }
            Thread.yield();
        }
        cycles = n;
    }
}

void t06_subscription_storm()
{
    auto f = AntFarm.create(1 << 18, 8, 8, 1, 8192, 6, 2048);
    scope (exit) f.destroy();
    enum N = 5000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 5 == 0;
        h.maxCs = mt ? 4 : 1;
        h.done = mt ? 5 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);

    atomicStore(g_stormStop, 0);
    auto deadline = MonoTime.currTime + 120.seconds;
    Thread[3] steady;
    ConsCtx[3] sctx;
    foreach (i; 0 .. 3)
    {
        sctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[6] storm;
    StormJob[6] jobs;
    foreach (i; 0 .. 6)
    {
        jobs[i] = new StormJob(f, deadline, 30);
        storm[i] = new Thread(&jobs[i].run);
    }
    ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.bulk, 32);
    auto p = new Thread({ producerMain(&pctx); });
    foreach (t; steady) t.start();
    foreach (t; storm) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();
    atomicStore(g_stormStop, 1);
    foreach (t; storm) t.join();
    expectExactCalls(&batch, "T06");
    expectNoLiveConsumerRefs(f, "T06");
    int cycles;
    foreach (i; 0 .. 6) cycles += jobs[i].cycles;
    check(cycles > 0, "storm progress");
    printf("T06 storm OK cycles=%d\n", cycles);
    fflush(stdout);
}

void t07_small_table_churn()
{
    auto f = AntFarm.create(1 << 18, 8, 8, 1, 4096, 6, 2048);
    scope (exit) f.destroy();
    enum N = 4500;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 4 == 0;
        h.maxCs = mt ? 4 : 1;
        h.done = mt ? 6 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);
    atomicStore(g_stormStop, 0);
    auto deadline = MonoTime.currTime + 120.seconds;
    Thread[2] steady;
    ConsCtx[2] sctx;
    foreach (i; 0 .. 2)
    {
        sctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &sctx[i];
        steady[i] = new Thread({ consumerMain(cc); });
    }
    Thread[4] storm;
    StormJob[4] jobs;
    foreach (i; 0 .. 4)
    {
        jobs[i] = new StormJob(f, deadline, 30);
        storm[i] = new Thread(&jobs[i].run);
    }
    ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.bulk, 10);
    auto p = new Thread({ producerMain(&pctx); });
    foreach (t; steady) t.start();
    foreach (t; storm) t.start();
    p.start();
    p.join();
    foreach (t; steady) t.join();
    atomicStore(g_stormStop, 1);
    foreach (t; storm) t.join();
    expectExactCalls(&batch, "T07");
    expectNoLiveConsumerRefs(f, "T07");
    say("T07 small-table churn OK");
}

void t08_spanning_tables()
{
    // segCap = 2^18/8 = 32768; huge (100k) spans ~3 segments, large (34k)
    // spans a boundary, the rest small. Bulk quota 110000 covers the huge
    // singleton and keeps Exmax <= (K-1)*segCap.
    auto f = AntFarm.create(1 << 18, 8, 4, 1, 110000, 2, 1024);
    scope (exit) f.destroy();
    enum N = 64;
    allocCalls(N);
    scope (exit) freeCalls();
    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    auto huge = cast(ulong*) malloc(100000 * ulong.sizeof);
    auto large = cast(ulong*) malloc(34000 * ulong.sizeof);
    auto small = cast(ulong*) malloc((N - 2) * 2 * ulong.sizeof);
    long expected;
    foreach (i; 0 .. N)
    {
        initPayloadHeader!countingCb(&headers[i], 1, 1);
        ++expected;
    }
    huge[0] = 0;
    large[0] = 1;
    entries[0] = PayloadEntry(&headers[0], huge[0 .. 100000]);
    entries[1] = PayloadEntry(&headers[1], large[0 .. 34000]);
    foreach (i; 2 .. N)
    {
        small[(i - 2) * 2] = i;
        small[(i - 2) * 2 + 1] = 1;
        entries[i] = PayloadEntry(&headers[i], small[(i - 2) * 2 .. (i - 2) * 2 + 2]);
    }
    PayloadBatch batch;
    batch.headers = headers;
    batch.entries = entries;
    batch.count = N;
    batch.expectedCalls = expected;

    ConsCtx cctx = ConsCtx(f, expected, MonoTime.currTime + 60.seconds);
    auto c = new Thread({ consumerMain(&cctx); });
    c.start();
    auto tok = f.registerProducer(Tier.bulk);
    check(tok.valid, "reg");
    size_t off;
    while (off < N)
    {
        immutable n = f.write(entries[off .. N], tok);
        off += n;
        if (n == 0)
            Thread.yield();
    }
    f.unregisterProducer(tok);
    c.join();
    expectExactCalls(&batch, "T08");
    expectNoLiveConsumerRefs(f, "T08");
    free(huge);
    free(large);
    free(small);
    free(headers);
    free(entries);
    say("T08 spanning OK");
}

// Per-payload in-flight tracking: body[2] holds a pointer-sized slot index
// into g_payInflight[].
__gshared shared(long)* g_payInflight;
__gshared size_t g_payInflightN;
__gshared shared(long) g_overMax;

long boundedSlowCb(size_t idx, immutable PayloadHeader* h, ulong iter) nothrow @nogc @system
{
    long cur = 1;
    if (g_payInflight !is null && idx < g_payInflightN)
    {
        cur = atomicFetchAdd(g_payInflight[idx], 1) + 1;
        if (cur > h.maxCs)
            atomicFetchAdd(g_overMax, 1);
    }
    ulong x = iter;
    foreach (i; 0 .. 20_000)
        x = x * 6364136223846793005UL + 1;
    countingCb(idx);
    if (g_payInflight !is null && idx < g_payInflightN)
        atomicFetchSub(g_payInflight[idx], 1);
    return cast(long)(x & 0xff);
}

void t09_slow_mt_bounds()
{
    auto f = AntFarm.create(1 << 18, 8, 16, 1, 8192, 4, 2048);
    scope (exit) f.destroy();
    enum N = 120;
    allocCalls(N);
    scope (exit) freeCalls();
    g_payInflight = cast(shared(long)*) malloc(N * long.sizeof);
    g_payInflightN = N;
    foreach (i; 0 .. N)
        atomicStore(g_payInflight[i], 0);
    atomicStore(g_overMax, 0);
    scope (exit)
    {
        free(cast(void*) g_payInflight);
        g_payInflight = null;
        g_payInflightN = 0;
    }
    PayloadBatch batch;
    batch.count = N;
    batch.headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    batch.entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    batch.bodyStore = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    batch.bodyCap = N * 2;
    batch.bodyUsed = 0;
    batch.expectedCalls = 0;
    check(batch.headers !is null && batch.entries !is null && batch.bodyStore !is null, "T09 batch alloc");
    foreach (i; 0 .. N)
    {
        auto body = batch.bodyStore[i * 2 .. i * 2 + 2];
        batch.entries[i] = payloadEntryRuntime!(boundedSlowCb, true)(
            &batch.headers[i], body, 4, 12, i,
            cast(immutable PayloadHeader*) &batch.headers[i]);
        batch.expectedCalls += 12;
    }
    batch.bodyUsed = N * 2;
    scope (exit) freeBatch(batch);
    runPair(f, &batch, 20, Tier.bulk, 0, 120);
    expectExactCalls(&batch, "T09");
    check(atomicLoad(g_overMax) == 0, "MaxCs per-payload");
    say("T09 slow MT bounds OK");
}

void t10_caps()
{
    auto f = AntFarm.create(1 << 18, 4, 2, 1, 2048, 2, 1024);
    scope (exit) f.destroy();
    auto b0 = f.registerProducer(Tier.bulk);
    check(b0.valid, "b0");
    check(!f.registerProducer(Tier.bulk).valid, "bover");
    auto s0 = f.registerProducer(Tier.small);
    auto s1 = f.registerProducer(Tier.small);
    check(s0.valid, "s0");
    check(s1.valid, "s1");
    check(!f.registerProducer(Tier.small).valid, "sover");
    f.unregisterProducer(b0);
    f.unregisterProducer(s0);
    f.unregisterProducer(s1);
    auto views = cast(ConsumerView*) malloc(MAX_CONSUMERS_LIMIT * ConsumerView.sizeof);
    scope (exit) free(views);
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
    {
        views[i] = ConsumerView.init;
        check(views[i].subscribe(f) >= 0, "cap");
    }
    ConsumerView extra;
    check(extra.subscribe(f) < 0, "reject");
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
        views[i].unsubscribe();
    check(atomicLoad(f.Cf) == 0, "drain");
    check(countSub0Pulses(f) == 1, "pulse");
    say("T10 caps OK");
}

void t11_late_subscriber_multilap()
{
    auto f = AntFarm.create(1 << 18, 8, 1, 1, 2048, 2, 1024);
    scope (exit) f.destroy();
    // ~20 ulongs/payload (tiny bodies pack ~17/table); 40000 payloads
    // write ~790K ulongs, lapping the 2^18 ring ~3x.
    enum N = 40000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 9 == 0;
        h.maxCs = mt ? 2 : 1;
        h.done = mt ? 3 : 1;
        plen = 2 + (i % 4);
    });
    scope (exit) freeBatch(batch);
    ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.small);
    auto p = new Thread({ producerMain(&pctx); });
    p.start();
    Thread.sleep(40.msecs);
    check(atomicLoad(f.Wt) > 0, "progress");
    check(atomicLoad(f.Wt) < f.Ln, "blocked");
    ConsCtx cctx = ConsCtx(f, batch.expectedCalls, MonoTime.currTime + 90.seconds);
    auto c = new Thread({ consumerMain(&cctx); });
    c.start();
    p.join();
    c.join();
    check(atomicLoad(f.Wt) > 2 * f.Ln, "wrap");
    expectExactCalls(&batch, "T11");
    expectNoLiveConsumerRefs(f, "T11");
    say("T11 late sub multilap OK");
}

void t12_no_reexecution_on_resub()
{
    auto f = AntFarm.create(1 << 18, 8, 2, 1, 4096, 4, 2048);
    scope (exit) f.destroy();
    enum N = 200;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        h.maxCs = (i % 4 == 0) ? 3 : 1;
        h.done = (i % 4 == 0) ? 4 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);
    ConsCtx c1 = ConsCtx(f, batch.expectedCalls, MonoTime.currTime + 60.seconds);
    auto ct = new Thread({ consumerMain(&c1); });
    ct.start();
    ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.small);
    auto p = new Thread({ producerMain(&pctx); });
    p.start();
    p.join();
    ct.join();
    expectExactCalls(&batch, "T12a");
    immutable before = atomicLoad(g_totalCalls);
    ConsumerView v;
    check(v.subscribe(f) >= 0, "resub");
    foreach (_; 0 .. 10_000)
        v.consumeNext();
    v.unsubscribe();
    check(atomicLoad(g_totalCalls) == before, "no reexec");
    say("T12 no reexecution OK");
}

void t13_create_validation()
{
    auto f = AntFarm.create(1 << 18, 2, 1, 1, 2048, 1, 512);
    check(f.K == 2 && f.segCap == (1 << 17) && f.exmax == 2560, "geom");
    f.destroy();
    // An unused bulk tier must not inject a segCap quota into the Exmax
    // check (regression: nb==0 with a default qb used to fatal "quota
    // exceeds Exmax" unless the caller mirrored qs into qb).
    auto f0 = AntFarm.create(1 << 18, 2, 1, 0, 0, 2, 512);
    check(f0.exmax == 1024, "nb0 exmax");
    auto tok = f0.registerProducer(Tier.small);
    check(tok.valid, "nb0 reg");
    PayloadHeader h;
    initPayloadHeader!countingCb(&h, 1, 1);
    ulong v = 7;
    PayloadEntry e;
    e.header = &h;
    e.body = (cast(const(ulong)*) &v)[0 .. 1];
    check(f0.write((&e)[0 .. 1], tok) == 1, "nb0 write");
    f0.unregisterProducer(tok);
    f0.destroy();
    say("T13 create OK");
}

void t14_k_geometry()
{
    foreach (k; [cast(uint) 4, cast(uint) 16])
    {
        auto f = AntFarm.create(1 << 18, k, 4, 1, 4096, 4, 1024);
        scope (exit) f.destroy();
        enum N = 800;
        allocCalls(N);
        scope (exit) freeCalls();
        auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
            h.maxCs = (i % 5 == 0) ? 3 : 1;
            h.done = (i % 5 == 0) ? 4 : 1;
            plen = 2 + (i % 10);
        });
        scope (exit) freeBatch(batch);
        runPair(f, &batch, 3, Tier.bulk, 0, 90);
        expectExactCalls(&batch, "T14");
        expectNoLiveConsumerRefs(f, "T14");
        printf("T14 K=%u OK\n", k);
        fflush(stdout);
    }
}

void t15_wave_consumers()
{
    auto f = AntFarm.create(1 << 18, 8, 6, 1, 8192, 8, 2048);
    scope (exit) f.destroy();
    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 6 == 0;
        h.maxCs = mt ? 5 : 1;
        h.done = mt ? 7 : 1;
        plen = 2 + (i % 8);
    });
    scope (exit) freeBatch(batch);
    ProdCtx pb = ProdCtx(f, batch.entries, N / 2, Tier.bulk, 40);
    ProdCtx ps = ProdCtx(f, batch.entries + N / 2, N - N / 2, Tier.small, 5);
    auto tb = new Thread({ producerMain(&pb); });
    auto ts = new Thread({ producerMain(&ps); });
    {
        ConsumerView[2] w;
        foreach (i; 0 .. 2)
            check(w[i].subscribe(f) >= 0, "w");
        tb.start();
        ts.start();
        auto until = MonoTime.currTime + 200.msecs;
        while (MonoTime.currTime < until)
            foreach (i; 0 .. 2)
                w[i].consumeNext();
        foreach (i; 0 .. 2)
            w[i].unsubscribe();
    }
    {
        Thread[5] consumers;
        ConsCtx[5] cctx;
        auto deadline = MonoTime.currTime + 120.seconds;
        foreach (i; 0 .. 5)
        {
            cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
            auto cc = &cctx[i];
            consumers[i] = new Thread({ consumerMain(cc); });
        }
        foreach (t; consumers) t.start();
        tb.join();
        ts.join();
        foreach (t; consumers) t.join();
    }
    expectExactCalls(&batch, "T15");
    expectNoLiveConsumerRefs(f, "T15");
    say("T15 wave consumers OK");
}

void t17_write_size_wrap()
{
    // Wrap and unsizable payloads must fatal (abort), not smash or return.
    import core.sys.posix.unistd : fork, _exit;
    import core.sys.posix.sys.wait : waitpid;
    import core.sys.posix.signal : SIGABRT;

    auto f = AntFarm.create(1 << 18, 4, 1, 1, 2048, 1, 512);
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "T17 reg");

    bool expectAbort(PayloadEntry e, const(char)[] label)
    {
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            _exit(0);
        }
        int st;
        waitpid(pid, &st, 0);
        immutable signaled = ((st & 0x7f) != 0) && ((st & 0x7f) != 0x7f);
        immutable sig = st & 0x7f;
        if (signaled && sig == SIGABRT)
            return true;
        char[112] buf;
        auto n = snprintf(buf.ptr, buf.length, "%.*s: st=%d signaled=%d sig=%d (want SIGABRT)",
            cast(int) label.length, label.ptr, st, signaled, sig);
        defect("T17", buf[0 .. n]);
        return false;
    }

    PayloadHeader h;
    initPayloadHeader!countingCb(&h, 1, 1);
    ulong one = 42;
    PayloadEntry wrap;
    wrap.header = &h;
    auto p = cast(const(ulong)*)&one;
    wrap.body = p[0 .. (size_t.max - 16)];

    // Fits in size_t but cannot fit any producer's max Exi (512).
    enum unsizedLen = 2048;
    auto big = cast(ulong*) malloc(unsizedLen * ulong.sizeof);
    check(big !is null, "T17 big");
    big[0] = 0;
    PayloadEntry unsized;
    unsized.header = &h;
    unsized.body = big[0 .. unsizedLen];

    immutable okWrap = expectAbort(wrap, "wrap");
    immutable okBig = expectAbort(unsized, "unsizable");
    free(big);

    // Per-tier sizing: a payload that fits only the bulk tier's quota
    // (singleton in (quotaSmall, quotaBulk]) must abort from a small-tier
    // producer - previously it cleared the farm-max check and then returned
    // write()==0 forever - and must be publishable from a bulk producer.
    enum midLen = 1500; // singleton 56 + 17 + 1500 = 1573 in (512, 2048]
    auto mid = cast(ulong*) malloc(midLen * ulong.sizeof);
    check(mid !is null, "T17 mid");
    mid[0] = 0;
    PayloadEntry midsize;
    midsize.header = &h;
    midsize.body = mid[0 .. midLen];
    immutable okMidSmall = expectAbort(midsize, "midsize-small");
    auto tokb = f.registerProducer(Tier.bulk);
    check(tokb.valid, "T17 bulk reg");
    immutable wrote = f.write((&midsize)[0 .. 1], tokb);
    f.unregisterProducer(tokb);
    check(wrote == 1, "midsize bulk write");
    free(mid);
    f.unregisterProducer(tok);
    f.destroy();
    say((okWrap && okBig && okMidSmall && wrote == 1)
        ? "T17 fatal on wrap/unsizable/per-tier OK"
        : "T17 DEFECT confirmed");
}

void t18_pure_churn_orphan()
{
    // Pure subscription churn with NO steady consumer. Non-last unsubscribers
    // drop trails on incomplete early segments; last unsubscriber may plant
    // Sub0 only on a later held segment. Early incomplete work is then
    // unprotected (Rt_low==0) and skipped by attach-in-place at the pulse.
    // Existing antfarm_test.testChurn always keeps nsteady >= 1.
    enum TRIALS = 3;
    int bad;
    foreach (trial; 0 .. TRIALS)
    {
        auto f = AntFarm.create(1 << 18, 8, 8, 1, 4096, 4, 2048);
        enum N = 600;
        allocCalls(N);
        auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
            immutable mt = i % 4 == 0;
            h.maxCs = mt ? 3 : 1;
            h.done = mt ? 5 : 1;
            plen = 2;
        });

        atomicStore(g_stormStop, 0);
        auto deadline = MonoTime.currTime + 40.seconds;
        enum NH = 8;
        Thread[NH] storm;
        foreach (i; 0 .. NH)
        {
            // Short bursts so churners leave early segments incomplete.
            // Explicit object per thread; do not use a loop-local delegate
            // (D closure capture would alias every thread to one context).
            auto job = new StormJob(f, deadline, 0, true);
            storm[i] = new Thread(&job.run);
        }
        ProdCtx pctx = ProdCtx(f, batch.entries, N, Tier.bulk, 8);
        auto p = new Thread({ producerMain(&pctx); });
        foreach (t; storm) t.start();
        p.start();
        p.join();
        // bounded mop-up window (not a pass criterion)
        auto mop = MonoTime.currTime + 3.seconds;
        while (MonoTime.currTime < mop)
            Thread.yield();
        atomicStore(g_stormStop, 1);
        foreach (t; storm) t.join();

        // orphan incomplete segments with no protection
        int orphans;
        immutable eg = atomicLoad(f.Eg);
        foreach (e; 0 .. cast(long) eg + 1)
        {
            immutable ki = cast(uint)(e & f.kMask);
            if (atomicLoad(f.stats[ki].es) != e)
                continue;
            immutable ki1 = cast(uint)((e + 1) & f.kMask);
            if (atomicLoad(f.stats[ki1].es) != e + 1)
                continue;
            immutable seqt = atomicLoad(f.stats[ki].seqt);
            immutable seqtN = atomicLoad(f.stats[ki1].seqt);
            immutable sd = atomicLoad(f.stats[ki].sd);
            immutable complete = seqt >= seqtN || seqt + sd >= seqtN;
            immutable rtlow = atomicLoad(f.Rt[ki][0]) & LOWMASK;
            if (!complete && rtlow == 0)
                ++orphans;
        }

        immutable calls1 = atomicLoad(g_totalCalls);
        // late salvage
        ConsumerView late;
        if (late.subscribe(f) >= 0)
        {
            auto d2 = MonoTime.currTime + 8.seconds;
            while (atomicLoad(g_totalCalls) < batch.expectedCalls && MonoTime.currTime < d2)
                if (!late.consumeNext())
                    Thread.yield();
            late.unsubscribe();
        }
        immutable calls2 = atomicLoad(g_totalCalls);
        size_t zeros;
        foreach (i; 0 .. N)
            if (atomicLoad(g_calls[i]) == 0)
                ++zeros;

        printf("T18 trial %d: pre=%lld post=%lld exp=%lld orphans=%d zeros=%zu pulses=%u\n",
            trial, calls1, calls2, batch.expectedCalls, orphans, zeros, countSub0Pulses(f));
        fflush(stdout);

        if (orphans > 0 || calls2 != batch.expectedCalls)
            ++bad;

        freeBatch(batch);
        freeCalls();
        f.destroy();
    }
    check(bad == 0, "T18 pure-churn orphans or lost work");
    say("T18 pure-churn OK");
}

void t20_plant_confirmed_segment()
{
    // D2 regression: plantIfUnprotected reads Seqt/Sd/SeqtN once, then CASes
    // Sub0 onto Rt without re-reading them.  A finisher completing the
    // segment between the sd read and the CAS leaves Sub0 on a confirmed
    // segment (spec 5b-a violation: confirmed must reach Rt==0) until a new
    // subscriber's 0→1 retracts it.  The fix re-verifies completeness after
    // the plant CAS and retracts the pulse (exact-value CAS) if a finisher
    // completed the segment in between.
    //
    // The natural window is nanoseconds, so a stress run cannot reliably
    // trigger the race.  This arm instead maximizes plant activity -- write
    // crossings and unsubscribe sweeps both call plantIfUnprotected -- and
    // asserts the invariant the fix preserves: after a full drain, every
    // confirmed segment carries no stray pulse beyond the single legitimate
    // empty-farm pulse, and exact call accounting holds.  A reverted fix
    // that makes the race observable (or a future change that widens the
    // window) fails the scan.
    //
    // Mechanism demo (not in-suite): instrument a scratch antfarm.d build
    // with a 2ms sleep between the plant's sd read and its CAS, then run a
    // few-tables-per-segment geometry. It exposes stale plants; the fix's
    // post-CAS exact-value retract removes them. This arm deliberately uses
    // small bodies because it is the durable invariant check, not the
    // widened-window mechanism demonstration.
    enum TRIALS = 8;
    int bad;
    foreach (trial; 0 .. TRIALS)
    {
        // K=16 and one-payload tables make the producer cross segments often.
        // Tiny bodies deliberately avoid turning this invariant test into a
        // giant memcpy-versus-sentinel TSan publication-protocol false positive.
        // 10000 single-payload tables write roughly three Ln laps, yielding
        // many segment crossings, finishers, and unsubscribe sweeps.
        auto f = AntFarm.create(1 << 18, 16, 6, 1, 9000, 3, 2048);
        scope (exit) f.destroy();
        enum N = 10000;
        allocCalls(N);
        scope (exit) freeCalls();
        auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
        auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        long expected;
        foreach (i; 0 .. N)
        {
            initPayloadHeader!countingCb(&headers[i], 1, 1);
            bodies[i * 2] = i;
            entries[i] = PayloadEntry(&headers[i], bodies[i * 2 .. i * 2 + 2]);
            expected += 1;
        }
        atomicStore(g_totalCalls, 0);

        void steadyConsumer(long exp)
        {
            ConsumerView v;
            if (v.subscribe(f) < 0) fatal("T20 steady sub");
            auto deadline = MonoTime.currTime + 90.seconds;
            while (atomicLoad!(MemoryOrder.acq)(g_totalCalls) < exp)
            {
                if (!v.consumeNext() && MonoTime.currTime > deadline)
                {
                    fprintf(stderr, "T20 steady watchdog\n");
                    abort();
                }
            }
            v.unsubscribe();
        }
        void churner(int rounds)
        {
            auto deadline = MonoTime.currTime + 90.seconds;
            int done = 0;
            while (done < rounds && MonoTime.currTime < deadline)
            {
                ConsumerView v;
                if (v.subscribe(f) < 0) { Thread.yield(); continue; }
                foreach (_; 0 .. 3)
                    v.consumeNext();
                v.unsubscribe(); // triggers plantUnprotectedIncomplete sweep
                ++done;
            }
        }

        enum NSTEADY = 4;
        Thread[NSTEADY] steady;
        foreach (i; 0 .. NSTEADY)
        {
            auto job = new class { void run() { steadyConsumer(expected); } };
            steady[i] = new Thread(&job.run);
            steady[i].start();
        }
        enum NCHURN = 3;
        Thread[NCHURN] churn;
        foreach (i; 0 .. NCHURN)
        {
            auto job = new class { void run() { churner(600); } };
            churn[i] = new Thread(&job.run);
            churn[i].start();
        }

        auto tok = f.registerProducer(Tier.bulk);
        check(tok.valid, "T20 reg");
        size_t off;
        while (off < N)
        {
            immutable n = f.write(entries[off .. off + 1], tok);
            off += n;
            if (n == 0) Thread.yield();
        }
        f.unregisterProducer(tok);
        foreach (t; churn) t.join();
        foreach (t; steady) t.join();

        // Invariant scan: after full drain and all unsubscribed, every
        // confirmed segment must have Rt_low == 0, except the single
        // legitimate empty-farm pulse (last-of-farm leaves it on its
        // confirmed position).  A D2 stale plant shows as a confirmed
        // segment with Rt_low != 0 in addition to that pulse.
        uint pulses;
        uint confirmedWithPulse;
        uint confirmedCount;
        foreach (ki; 0 .. 16)
        {
            immutable es = atomicLoad!(MemoryOrder.raw)(f.stats[ki].es);
            if (es < 0) continue;
            // confirmed: next segment initialized and span fully consumed
            immutable ei1 = cast(ulong) es + 1;
            immutable ki1 = cast(uint)(ei1 & f.kMask);
            if (atomicLoad!(MemoryOrder.raw)(f.stats[ki1].es) != cast(long) ei1)
                continue;
            immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
            immutable seqtN = atomicLoad!(MemoryOrder.raw)(f.stats[ki1].seqt);
            immutable sd = atomicLoad!(MemoryOrder.raw)(f.stats[ki].sd);
            if (seqt + sd < seqtN)
                continue; // still incomplete: legitimately may carry Sub0
            ++confirmedCount;
            immutable rt = atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]);
            if ((rt & LOWMASK) != 0)
            {
                ++confirmedWithPulse;
                ++pulses;
            }
        }
        // The empty-farm pulse is one confirmed segment with Rt_low != 0.
        if (confirmedWithPulse > 1)
        {
            fprintf(stderr, "T20 trial %d: %u confirmed segments with pulse (expected <= 1)\n",
                trial, confirmedWithPulse);
            ++bad;
        }
        immutable got = atomicLoad(g_totalCalls);
        if (got != expected)
        {
            fprintf(stderr, "T20 trial %d: calls=%lld expected=%lld\n", trial, got, expected);
            ++bad;
        }
        fprintf(stderr, "T20 trial %d: confirmed=%u pulses=%u calls=%lld/%lld\n",
            trial, confirmedCount, pulses, got, expected);
        fflush(stderr);
    }
    check(bad == 0, "T20 stale plant on confirmed segment or lost work");
    say("T20 plant-confirmed-segment OK");
}

void t21_payload_content_multilap()
{
    // Write enough payloads to lap the 2^18-ulong ring several times while
    // consumers drain.  Every body is a run of numbers b[k] == idx + k;
    // the callback checks each word, sums the run into g_contentSum, and
    // keeps the usual per-payload call accounting.  A wrap or offset bug
    // that corrupts body contents therefore shows up as a per-word bad,
    // a global sum mismatch, or a per-payload call-count mismatch.
    //
    // Geometry: one small producer (quota 4096), two consumers, and
    // N=30000 bodies of 32 ulongs.  Each table carries up to 82 payloads
    // and reserves ~4056 ulongs, so total Wt is ~1.48M ulongs: ~5.7 laps.
    auto f = AntFarm.create(1 << 18, 8, 2, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum size_t N = T21_N;
    enum size_t BODY_LEN = T21_BODY_LEN;

    allocCalls(N);
    scope (exit) freeCalls();
    atomicStore!(MemoryOrder.raw)(g_contentSum, 0L);
    atomicStore!(MemoryOrder.raw)(g_contentBad, 0L);

    // Manual exact-size allocation; makeBatch's 4096-ulong-per-slot body
    // arena would be far larger than needed for N=30000.
    auto headers = cast(PayloadHeader*) malloc(N * PayloadHeader.sizeof);
    auto bodies = cast(ulong*) malloc(N * BODY_LEN * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    check(headers !is null && bodies !is null && entries !is null, "T21 alloc");
    scope (exit)
    {
        free(headers);
        free(bodies);
        free(entries);
    }

    foreach (i; 0 .. N)
    {
        headers[i] = PayloadHeader.init;
        headers[i].maxCs = 1;
        headers[i].done = 1;
        headers[i].call = &contentSumCb;
        foreach (k; 0 .. BODY_LEN)
            bodies[i * BODY_LEN + k] = i + k;
        entries[i].header = &headers[i];
        entries[i].body = bodies[i * BODY_LEN .. i * BODY_LEN + BODY_LEN];
    }

    PayloadBatch batch;
    batch.headers = headers;
    batch.entries = entries;
    batch.count = N;
    batch.expectedCalls = N;

    runPair(f, &batch, 2, Tier.small, 0, 120);

    check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 3 * f.Ln, "T21 wrapped several laps");
    expectExactCalls(&batch, "T21");
    check(atomicLoad!(MemoryOrder.raw)(g_contentBad) == 0, "T21 payload content");

    immutable ulong expectedSum = BODY_LEN * (N * (N - 1) / 2)
                                + N * (BODY_LEN * (BODY_LEN - 1) / 2);
    check(atomicLoad!(MemoryOrder.raw)(g_contentSum) == cast(long) expectedSum, "T21 content sum");
    expectNoLiveConsumerRefs(f, "T21");

    printf("T21 payload content multilap OK Wt=%lld laps=%.2f\n",
        cast(long) atomicLoad!(MemoryOrder.raw)(f.Wt),
        cast(double) atomicLoad!(MemoryOrder.raw)(f.Wt) / cast(double) f.Ln);
    fflush(stdout);
}

void t22_avgcost_sharding()
{
    // The auto small-table threshold rule is normally off in the correctness
    // suites (AntFarm.create defaults smallThreshold to 64).  Turn it on and
    // write tables on both sides of each avgCost threshold with 8 consumers
    // (sq = 3):
    //   avgCost 0 -> chunk 32 -> threshold 96  (95 small, 97 sharded)
    //   avgCost 1 -> chunk 16 -> threshold 48  (47 small, 49 sharded)
    //   avgCost 2 -> chunk  8 -> threshold 24  (23 small, 25 sharded)
    //   avgCost 3 -> chunk  4 -> threshold 16  (15 small, 17 sharded)
    //   avgCost 5 -> chunk  1 -> threshold 16  (15 small, 17 sharded)
    // Exact per-payload accounting must hold across every consumer geometry.
    auto f = AntFarm.create(1 << 18, 8, 8, 1, 0, 1, 4096, 0);
    scope (exit) f.destroy();

    enum N = 400;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 5 == 0;
        h.maxCs = mt ? 3 : 1;
        h.done = mt ? 4 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);

    static struct TableSpec
    {
        uint tlen;
        uint avgCost;
        Tier tier;
    }
    immutable TableSpec[] specs = [
        TableSpec(95, 0, Tier.bulk),
        TableSpec(97, 0, Tier.bulk),
        TableSpec(47, 1, Tier.bulk),
        TableSpec(49, 1, Tier.bulk),
        TableSpec(23, 2, Tier.small),
        TableSpec(25, 2, Tier.small),
        TableSpec(15, 3, Tier.small),
        TableSpec(17, 3, Tier.small),
        TableSpec(15, 5, Tier.small),
        TableSpec(17, 5, Tier.small),
    ];

    enum NC = 8;
    Thread[NC] consumers;
    ConsCtx[NC] cctx;
    auto deadline = MonoTime.currTime + 90.seconds;
    foreach (i; 0 .. NC)
    {
        cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
        auto cc = &cctx[i];
        consumers[i] = new Thread({ consumerMain(cc); });
    }
    foreach (t; consumers)
        t.start();
    while (atomicLoad!(MemoryOrder.acq)(f.Cf) < NC)
        Thread.yield();

    auto btok = f.registerProducer(Tier.bulk);
    auto stok = f.registerProducer(Tier.small);
    check(btok.valid, "T22 bulk reg");
    check(stok.valid, "T22 small reg");

    size_t off = 0;
    foreach (spec; specs)
    {
        auto slice = batch.entries[off .. off + spec.tlen];
        if (spec.tier == Tier.bulk)
            check(f.write(slice, btok, spec.avgCost) == spec.tlen, "T22 bulk write full table");
        else
            check(f.write(slice, stok, spec.avgCost) == spec.tlen, "T22 small write full table");
        off += spec.tlen;
    }
    check(off == N, "T22 wrote all payloads");

    f.unregisterProducer(btok);
    f.unregisterProducer(stok);

    foreach (t; consumers)
        t.join();

    expectExactCalls(&batch, "T22");
    check(countSub0Pulses(f) == 1, "T22 pulse");
    expectNoLiveConsumerRefs(f, "T22");
    say("T22 avgCost sharding exact OK");
}

void main(string[] args)
{
    bool runAll = args.length <= 1;
    bool want(string name)
    {
        if (runAll)
            return true;
        foreach (a; args[1 .. $])
            if (a == name)
                return true;
        return false;
    }

    if (want("T01"))
        t01_pcount_field_carry();
    if (want("T16"))
        t16_position_ref_stall();
    if (want("T02"))
        t02_done_wrap_overexec();
    if (want("T17"))
        t17_write_size_wrap();
    if (want("T18"))
        t18_pure_churn_orphan();
    if (want("T20"))
        t20_plant_confirmed_segment();
    if (want("T21"))
        t21_payload_content_multilap();
    if (want("T22"))
        t22_avgcost_sharding();

    if (want("T03"))
        t03_concurrent_exact();
    if (want("T04"))
        t04_many_consumers_two_producers();
    if (want("T05"))
        t05_zero_consumer_gap();
    if (want("T06"))
        t06_subscription_storm();
    if (want("T07"))
        t07_small_table_churn();
    if (want("T08"))
        t08_spanning_tables();
    if (want("T09"))
        t09_slow_mt_bounds();
    if (want("T10"))
        t10_caps();
    if (want("T11"))
        t11_late_subscriber_multilap();
    if (want("T12"))
        t12_no_reexecution_on_resub();
    if (want("T13"))
        t13_create_validation();
    if (want("T14"))
        t14_k_geometry();
    if (want("T15"))
        t15_wave_consumers();

    immutable d = atomicLoad(g_defects);
    if (d == 0)
    {
        say("ALL TORTURE TESTS PASSED");
        exit(0);
    }
    printf("CORRECTNESS PASSED; DEFECTS CONFIRMED: %d\n", d);
    fflush(stdout);
    exit(2);
}
