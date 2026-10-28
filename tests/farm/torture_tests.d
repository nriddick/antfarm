/++
 + Adversarial torture suite for antfarm (SPEC.md).
 +
 +   make -C tests/farm run
 +
 + Exit 0 = every selected check passed. Invalid selectors exit 64;
 + any contract or correctness failure fails the process.
 +/
module torture_tests;

import antfarm.templates;
import torture_common;
import core.atomic;
import core.thread;
import core.time;
import core.sync.barrier : Barrier;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, abort, exit;

// T21: payload-body content verification over several laps of the ring.
// Shared counters must be __gshared shared(T) (see torture_common.d).
__gshared shared(long) g_contentSum;
__gshared shared(long) g_contentBad;
__gshared shared(long) g_claimContractBad;

private extern(C) void claimContractCb(bool alternate)(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto body = bodyPtr[0 .. bodyWords];
    if (body.length < 1) fatal("claim callback body too short");
    countingCb(cast(size_t) body[0]);
}

private enum size_t T21_BODY_LEN = 32;
private enum size_t T21_N = 30000;

extern(C) void contentSumCb(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto b = bodyPtr[0 .. bodyWords];
    // Count the call first so a corrupted body still advances the global
    // total and consumers do not spin until the watchdog.
    atomicFetchAdd(g_totalCalls, 1L);

    // Corrupted plen could make the body slice empty or huge; never index
    // before validating the length.
    if (b.length != T21_BODY_LEN)
    {
        atomicFetchAdd(g_contentBad, 1L);
        return;
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
    return;
}

version (Posix)
{
    import core.sys.posix.sys.types : pid_t;

    // A rejected write must abort deliberately, not segfault, return, or hang.
    // Bound every child wait, including acceptance cases at the quota boundary.
    void expectChild(pid_t pid, bool wantAbort, const(char)[] label)
    {
        import core.sys.posix.sys.wait : waitpid, WNOHANG;
        import core.sys.posix.signal : SIGABRT, SIGKILL, kill;
        import core.stdc.errno : errno, EINTR, ESRCH;

        check(pid > 0, "contract probe fork succeeded");
        int status;
        immutable deadline = MonoTime.currTime + 10.seconds;
        for (;;)
        {
            immutable waited = waitpid(pid, &status, WNOHANG);
            if (waited == pid) break;
            if (waited < 0 && errno == EINTR) continue;
            check(waited >= 0, "contract probe waitpid succeeded");
            if (MonoTime.currTime > deadline)
            {
                check(kill(pid, SIGKILL) == 0 || errno == ESRCH, "contract probe timeout kill");
                while (waitpid(pid, &status, 0) < 0)
                    check(errno == EINTR, "contract probe timeout reap");
                fprintf(stderr, "%.*s: child watchdog (no exit)\n", cast(int) label.length, label.ptr);
                abort();
            }
            Thread.sleep(1.msecs);
        }
        if (wantAbort ? (status & 0x7f) == SIGABRT : status == 0) return;
        fprintf(stderr, "%.*s: child status=%d (want %s)\n",
            cast(int) label.length, label.ptr, status, wantAbort ? "SIGABRT".ptr : "exit 0".ptr);
        abort();
    }
}

// ---------------- contract checks (failures are fatal to the suite) ----------------

__gshared uint g_harnessCallback;
long harnessCallback(size_t i) nothrow @nogc @system
{
    ++g_harnessCallback;
    return countingCb(i);
}

void t00_harness_oracles()
{
    allocCalls(2);
    scope (exit) freeCalls();
    auto batch = makeBatch(2, (size_t i, ref Callback h, ref size_t plen) {
        h = callbackFor!harnessCallback();
        plen = 1;
    });
    scope (exit) freeBatch(batch);
    g_harnessCallback = 0;
    foreach (e; batch.entries[0 .. batch.count])
        e.call(e.body.ptr, e.body.length);
    check(g_harnessCallback == 2, "T00 batch preserves the supplied callback");
    expectExactCalls(&batch, "T00 exact ledger");
    atomicStore(g_calls[0], 0L);
    atomicStore(g_calls[1], 2L);
    check(atomicLoad(g_totalCalls) == batch.expectedCalls, "T00 totals still balance");
    check(!exactCallRange(0, 2), "T00 lost/duplicate pair cannot pass the per-id oracle");
    say("T00 harness callback and loss/duplicate negative control OK");
}

void t01_bad_header()
{
    version (Windows)
    {
        say("T01 skipped (fork abort-probe)");
        return;
    }
    else version (Posix)
    {
    // write() must fatal (abort) on a null callback. A clean
    // return is a miss; SIGILL/SIGSEGV is still a smash.
    import core.sys.posix.unistd : fork, _exit;

    auto f = AntFarm.create(2, 4, 1, 1, 2048, 1, 512);
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "T01 reg");

    void expectAbort(const(char)[] label)
    {
        Callback h;
        h = callbackFor!countingCb();
        h = null;
        ulong body = 0;
        PayloadEntry e;
        e.call = h;
        e.body = (&body)[0 .. 1];
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            _exit(0);
        }
        expectChild(pid, true, label);
    }

    expectAbort("T01 null callback");
    f.unregisterProducer(tok);
    f.destroy();
    say("T01 write rejects null callback OK");
    }
}

void t02_single_shot_revisit()
{
    auto f = AntFarm.create(2, 8, 8, 1, 8192, 4, 2048);
    scope (exit) f.destroy();
    allocCalls(1);
    scope (exit) freeCalls();

    enum uint DONE = 1;
    auto batch = makeBatch(1, (size_t i, ref Callback h, ref size_t plen) {
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

    foreach (round; 0 .. 1000)
    {
        foreach (i; 0 .. NC)
            vs[i].consumeNext();
        if (atomicLoad(g_totalCalls) > DONE + 1000)
            break;
    }
    foreach (i; 0 .. NC)
        vs[i].unsubscribe();

    immutable got = atomicLoad(g_calls[0]);
    check(got == DONE, "T02 exact calls at single-shot revisit");
    say("T02 single-shot revisit exact OK");
}

void t16_position_ref_stall()
{
    // Large Exmax stall geometry: segCap = Ln/K = 2^18/8 = 32768 and
    // Exmax = 2*81920 = 163840 ~= 5*segCap, so the idle re-walk must
    // migrate a confirmed position pin forward or refreshQuota's forward
    // sweep breaks at it and the producer stalls.
    auto f = AntFarm.create(2, 8, 2, 2, 81920, 0, 0);
    scope (exit) f.destroy();

    enum N = 120;
    auto headers = cast(Callback*) malloc(N * Callback.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    ulong*[] bodies;
    bodies.length = N;
    foreach (i; 0 .. N)
    {
        headers[i] = callbackFor!countingCb();
        bodies[i] = cast(ulong*) malloc(800 * ulong.sizeof);
        bodies[i][0] = i;
        entries[i].call = headers[i];
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
            "producer stalled idle-at-Wt written=%zu/%d total=%lld next=%llu oldest=%llu newest=%llu posKi=%u",
            atomicLoad(written), N, atomicLoad(g_totalCalls),
            cs[0].nextSeq, cs[0].oldestEi, cs[0].newestEi, cs[0].posKi());
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
    expectCallRange(0, N, "T16 every written payload executed once");
    expectDrainedFarm(f, "T16 drained tallies");
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
    auto f = AntFarm.create(2, 8, 4, 1, 16384, 8, 4096);
    scope (exit) f.destroy();
    enum N = 6000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    check(countLive(f) == 1, "T03 only the frontier Live");
    expectNoLiveConsumerRefs(f, "T03");
    say("T03 concurrent exact OK");
}

void t04_many_consumers_two_producers()
{
    // High consumer count + bulk/small producers. T24 separately exercises
    // the multi-small-producer topologies that exposed earlier stalls.
    auto f = AntFarm.create(2, 8, 16, 1, 16384, 8, 4096);
    scope (exit) f.destroy();
    enum N = 3000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 8, 2, 1, 2048, 2, 1024);
    scope (exit) f.destroy();
    enum N = 1600;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    check(countLive(f) >= 1, "frontier Live");
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
    auto f = AntFarm.create(2, 8, 8, 1, 8192, 6, 2048);
    scope (exit) f.destroy();
    enum N = 5000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 8, 8, 1, 4096, 6, 2048);
    scope (exit) f.destroy();
    enum N = 4500;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 8, 4, 1, 110000, 2, 1024);
    scope (exit) f.destroy();
    enum N = 64;
    allocCalls(N);
    scope (exit) freeCalls();
    auto headers = cast(Callback*) malloc(N * Callback.sizeof);
    auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    auto huge = cast(ulong*) malloc(100000 * ulong.sizeof);
    auto large = cast(ulong*) malloc(34000 * ulong.sizeof);
    auto small = cast(ulong*) malloc((N - 2) * 2 * ulong.sizeof);
    long expected;
    foreach (i; 0 .. N)
    {
        headers[i] = callbackFor!countingCb();
        ++expected;
    }
    huge[0] = 0;
    large[0] = 1;
    entries[0] = PayloadEntry(headers[0], huge[0 .. 100000]);
    entries[1] = PayloadEntry(headers[1], large[0 .. 34000]);
    foreach (i; 2 .. N)
    {
        small[(i - 2) * 2] = i;
        small[(i - 2) * 2 + 1] = 1;
        entries[i] = PayloadEntry(headers[i], small[(i - 2) * 2 .. (i - 2) * 2 + 2]);
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

long boundedSlowCb(size_t idx) nothrow @nogc @system
{
    long cur = 1;
    if (g_payInflight !is null && idx < g_payInflightN)
    {
        cur = atomicFetchAdd(g_payInflight[idx], 1) + 1;
        if (cur > 1)
            atomicFetchAdd(g_overMax, 1);
    }
    ulong x = idx;
    foreach (i; 0 .. 20_000)
        x = x * 6364136223846793005UL + 1;
    countingCb(idx);
    if (g_payInflight !is null && idx < g_payInflightN)
        atomicFetchSub(g_payInflight[idx], 1);
    return cast(long)(x & 0xff);
}

void t09_slow_single_shot()
{
    auto f = AntFarm.create(2, 8, 16, 1, 8192, 4, 2048);
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
    batch.headers = cast(Callback*) malloc(N * Callback.sizeof);
    batch.entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
    batch.bodyStore = cast(ulong*) malloc(N * 2 * ulong.sizeof);
    batch.bodyCap = N * 2;
    batch.bodyUsed = 0;
    batch.expectedCalls = 0;
    check(batch.headers !is null && batch.entries !is null && batch.bodyStore !is null, "T09 batch alloc");
    foreach (i; 0 .. N)
    {
        auto body = batch.bodyStore[i * 2 .. i * 2 + 2];
        batch.entries[i] = payloadEntry!boundedSlowCb(body, i);
        ++batch.expectedCalls;
    }
    batch.bodyUsed = N * 2;
    scope (exit) freeBatch(batch);
    runPair(f, &batch, 20, Tier.bulk, 0, 120);
    expectExactCalls(&batch, "T09");
    check(atomicLoad(g_overMax) == 0, "one concurrent callback per payload");
    say("T09 slow single-shot OK");
}

void t10_caps()
{
    auto f = AntFarm.create(2, 4, 2, 1, 2048, 2, 1024);
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
    check(extra.subscribeOnce(f) == SUBSCRIBE_FULL, "capacity rejects with FULL");
    check(!extra.hasRef && atomicLoad(f.Cf) == MAX_CONSUMERS_LIMIT,
        "FULL leaves membership and the attempted view balanced");
    foreach (i; 0 .. MAX_CONSUMERS_LIMIT)
        views[i].unsubscribe();
    check(atomicLoad(f.Cf) == 0, "drain");
    expectDrainedFarm(f, "T10 only the frontier Live, all tallies empty");
    say("T10 caps OK");
}

void t11_late_subscriber_multilap()
{
    auto f = AntFarm.create(2, 8, 1, 1, 2048, 2, 1024);
    scope (exit) f.destroy();
    // Variable body lengths exercise indexed uniform tables over multiple laps.
    enum N = 300000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 8, 2, 1, 4096, 4, 2048);
    scope (exit) f.destroy();
    enum N = 200;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 2, 1, 1, 2048, 1, 512);
    check(f.K == 2 && f.segCap == (1 << 17) && f.exmax == 2560, "geom");
    f.destroy();
    // An unused bulk tier must not inject a segCap quota into the Exmax
    // check (regression: nb==0 with a default qb used to fatal "quota
    // exceeds Exmax" unless the caller mirrored qs into qb).
    auto f0 = AntFarm.create(2, 2, 1, 0, 0, 2, 512);
    check(f0.exmax == 1024, "nb0 exmax");
    auto tok = f0.registerProducer(Tier.small);
    check(tok.valid, "nb0 reg");
    Callback h;
    h = callbackFor!countingCb();
    ulong v = 7;
    PayloadEntry e;
    e.call = h;
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
        auto f = AntFarm.create(2, k, 4, 1, 4096, 4, 1024);
        scope (exit) f.destroy();
        enum N = 800;
        allocCalls(N);
        scope (exit) freeCalls();
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    auto f = AntFarm.create(2, 8, 6, 1, 8192, 8, 2048);
    scope (exit) f.destroy();
    enum N = 4000;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
    version (Windows)
    {
        say("T17 skipped (fork abort-probe)");
        return;
    }
    else version (Posix)
    {
    // Wrap and unsizable payloads must fatal (abort), not smash or return.
    import core.sys.posix.unistd : fork, _exit;

    auto f = AntFarm.create(2, 4, 1, 1, 2048, 1, 512);
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "T17 reg");

    void expectAbort(PayloadEntry e, const(char)[] label)
    {
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            _exit(0);
        }
        expectChild(pid, true, label);
    }

    Callback h;
    h = callbackFor!countingCb();
    ulong one = 42;
    PayloadEntry wrap;
    wrap.call = h;
    auto p = cast(const(ulong)*)&one;
    wrap.body = p[0 .. (size_t.max - 16)];

    // Fits in size_t but cannot fit any producer's max Exi (512).
    enum unsizedLen = 2048;
    auto big = cast(ulong*) malloc(unsizedLen * ulong.sizeof);
    check(big !is null, "T17 big");
    big[0] = 0;
    PayloadEntry unsized;
    unsized.call = h;
    unsized.body = big[0 .. unsizedLen];

    expectAbort(wrap, "T17 wrap");
    expectAbort(unsized, "T17 unsizable");
    free(big);

    // The arithmetic fixed-length path must reject a caller-supplied body
    // length whose singleton cost overflows, before inspecting body storage.
    Callback mh;
    mh = callbackFor!countingCb();
    PayloadBody[1] fixedBodies = [p[0 .. 1]];
    {
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write(mh, fixedBodies[], ulong.max - 17, tok);
            _exit(0);
        }
        expectChild(pid, true, "T17 fixed-wrap");
    }

    // Per-tier sizing: a payload that fits only the bulk tier's quota
    // (singleton in (quotaSmall, quotaBulk]) must abort from a small-tier
    // producer - previously it cleared the farm-max check and then returned
    // write()==0 forever - and must be publishable from a bulk producer.
    enum midLen = 1500; // compact singleton 37 + 1500 = 1537 in (512, 2048]
    auto mid = cast(ulong*) malloc(midLen * ulong.sizeof);
    check(mid !is null, "T17 mid");
    mid[0] = 0;
    PayloadEntry midsize;
    midsize.call = h;
    midsize.body = mid[0 .. midLen];
    expectAbort(midsize, "T17 midsize-small");
    auto tokb = f.registerProducer(Tier.bulk);
    check(tokb.valid, "T17 bulk reg");
    immutable wrote = f.write((&midsize)[0 .. 1], tokb);
    f.unregisterProducer(tokb);
    check(wrote == 1, "midsize bulk write");
    free(mid);
    f.unregisterProducer(tok);
    f.destroy();

    foreach (tier; [Tier.small, Tier.bulk])
        foreach (over; [false, true])
        {
            auto boundaryFarm = AntFarm.create(2, 4, 1, 1, 2048, 1, 512);
            scope (exit) boundaryFarm.destroy();
            Callback header;
            header = callbackFor!countingCb();
            immutable quota = tier == Tier.small ? 512UL : 2048UL;
            immutable overhead = 37UL;
            immutable bodyWords = quota - overhead + (over ? 1 : 0);
            auto body = cast(ulong*) malloc(bodyWords * ulong.sizeof);
            check(body !is null, "T17 boundary allocation");
            scope (exit) free(body);
            body[0 .. bodyWords] = 0;
            PayloadEntry entry = PayloadEntry(header, body[0 .. bodyWords]);
            immutable fits = boundaryFarm.canFit(header, bodyWords, tier);
            check(fits == !over, "T17 canFit boundary");

            // Each overload/case gets a child: an acceptance must exit 0,
            // an oversized singleton must die with SIGABRT, even in release.
            foreach (form; 0 .. 3)
            {
                auto pid = fork();
                if (pid == 0)
                {
                    auto token = boundaryFarm.registerProducer(tier);
                    if (!token.valid) _exit(2);
                    ulong wroteBoundary;
                    PayloadBody[1] bodies = [entry.body];
                    if (form == 0)
                        wroteBoundary = boundaryFarm.write((&entry)[0 .. 1], token);
                    else if (form == 1)
                        wroteBoundary = boundaryFarm.write(header, bodies[], token);
                    else
                        wroteBoundary = boundaryFarm.write(header, bodies[], bodyWords, token);
                    _exit(wroteBoundary == 1 ? 0 : 1);
                }
                expectChild(pid, !fits, "T17 canFit agrees with write acceptance/fatal");
                printf("T17 canFit %s %s bodyWords=%llu -> %s, write %s (form=%d)\n",
                    (tier == Tier.small ? "small" : "bulk").ptr,
                    "ST".ptr, bodyWords,
                    (fits ? "true" : "false").ptr,
                    (fits ? "accepted" : "SIGABRT").ptr, form);
                fflush(stdout);
            }
        }
    say("T17 fatal on wrap/unsizable/per-tier OK");
    }
}

// SPEC 2f singleton contract: an ST singleton is compact
// (base + 1 index + 1 Call + Plen), so a body at the compact limit is
// accepted while one word more fatals.
void t26_compact_singleton_contract()
{
    version (Windows)
    {
        say("T26 skipped (fork abort-probe)");
        return;
    }
    else version (Posix)
    {
    import core.sys.posix.unistd : fork, _exit;

    enum size_t Lmax = 100;        // ST compact singleton == Qsmall.
    enum ulong Qsmall = 37 + Lmax; // base 35 + index 1 + Call 1 + Plen.
    enum ulong Qbulk = 56 + Lmax;  // A larger tier does not relax the small-tier contract.
    auto f = AntFarm.create(2, 4, 1, 1, Qbulk, 1, Qsmall);
    auto tok = f.registerProducer(Tier.small);
    check(tok.valid, "T26 reg");

    void expectAbort(PayloadEntry e, const(char)[] label)
    {
        auto pid = fork();
        if (pid == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            _exit(0);
        }
        expectChild(pid, true, label);
    }

    auto okBody = cast(ulong*) malloc(Lmax * ulong.sizeof);
    check(okBody !is null, "T26 ok body");
    foreach (i, ref w; okBody[0 .. Lmax]) w = i;
    Callback st;
    st = callbackFor!countingCb();
    PayloadEntry exAccept;
    exAccept.call = st;
    exAccept.body = okBody[0 .. Lmax];
    immutable wrote = f.write((&exAccept)[0 .. 1], tok);
    check(wrote == 1, "T26 exact-quota compact singleton accepted");

    auto overBody = cast(ulong*) malloc((Lmax + 1) * ulong.sizeof);
    check(overBody !is null, "T26 over body");
    foreach (i, ref w; overBody[0 .. Lmax + 1]) w = i;
    PayloadEntry stOver;
    stOver.call = st;
    stOver.body = overBody[0 .. Lmax + 1];
    expectAbort(stOver, "T26 compact-one-word-over");

    free(okBody);
    free(overBody);
    f.unregisterProducer(tok);
    f.destroy();
    say("T26 compact singleton contract OK");
    }
}

void t18_pure_churn_orphan()
{
    // Pure subscription churn with NO steady consumer. Churners leave early
    // epochs incomplete, departing in any order. Every incomplete epoch must
    // keep the Live it has had since initialization (no Rt_low == 0 orphan),
    // and a late subscriber must find the earliest one and salvage all work.
    // Existing antfarm_test.testChurn always keeps nsteady >= 1.
    enum TRIALS = 3;
    int bad;
    foreach (trial; 0 .. TRIALS)
    {
        auto f = AntFarm.create(2, 8, 8, 1, 4096, 4, 2048);
        enum N = 600;
        allocCalls(N);
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
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
        immutable eg = cast(long)(atomicLoad(f.Wt) >> f.segShift);
        foreach (e; 0 .. eg + 1)
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

        printf("T18 trial %d: pre=%lld post=%lld exp=%lld orphans=%d zeros=%zu live=%u\n",
            trial, calls1, calls2, batch.expectedCalls, orphans, zeros, countLive(f));
        fflush(stdout);

        if (orphans > 0 || calls2 != batch.expectedCalls)
            ++bad;

        expectExactCalls(&batch, "T18 abandoned backlog recovered exactly once");
        expectDrainedFarm(f, "T18 recovery leaves no pins or confirmed Live");

        freeBatch(batch);
        freeCalls();
        f.destroy();
    }
    check(bad == 0, "T18 pure-churn orphans or lost work");
    say("T18 pure-churn OK");
}

void t20_confirmed_discharged()
{
    // Stress Live raise and discharge: many crossings, finishers, churners
    // and departures. Unsubscribe leaves Live on every unconfirmed epoch it
    // held and creates no protection of its own. At quiescence, after a full
    // drain by steady consumers that pass every epoch, no confirmed epoch may
    // still be Live: a confirmed epoch that kept Live would block reuse with
    // no holder obliged to discharge it (spec 6c).
    enum TRIALS = 8;
    int bad;
    foreach (trial; 0 .. TRIALS)
    {
        // K=16 and one-payload tables make the producer cross segments often.
        // Tiny bodies keep this an invariant scan (many crossings, finishers,
        // Live raises and discharges) rather than a memcpy bench. 10000 single-payload
        // tables write roughly three Ln laps.
        auto f = AntFarm.create(2, 16, 6, 1, 9000, 3, 2048);
        scope (exit) f.destroy();
        enum N = 10000;
        allocCalls(N);
        scope (exit) freeCalls();
        auto headers = cast(Callback*) malloc(N * Callback.sizeof);
        auto bodies = cast(ulong*) malloc(N * 2 * ulong.sizeof);
        auto entries = cast(PayloadEntry*) malloc(N * PayloadEntry.sizeof);
        scope (exit) { free(headers); free(bodies); free(entries); }
        long expected;
        foreach (i; 0 .. N)
        {
            headers[i] = callbackFor!countingCb();
            bodies[i * 2] = i;
            entries[i] = PayloadEntry(headers[i], bodies[i * 2 .. i * 2 + 2]);
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
                v.unsubscribe(); // unconfirmed epochs it held keep their Live
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
        // confirmed segment must have Rt_low == 0: discharged, with no
        // reference left. Only the unconfirmed frontier may stay Live.
        uint confirmedProtected;
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
                continue; // still incomplete: legitimately Live
            ++confirmedCount;
            immutable rt = atomicLoad!(MemoryOrder.raw)(f.Rt[ki][0]);
            if ((rt & LOWMASK) != 0)
                ++confirmedProtected;
        }
        if (confirmedProtected != 0)
        {
            fprintf(stderr, "T20 trial %d: %u confirmed segments still Live or referenced\n",
                trial, confirmedProtected);
            ++bad;
        }
        if (countLive(f) != 1)
        {
            fprintf(stderr, "T20 trial %d: %u Live slots at quiescence (expected the frontier only)\n",
                trial, countLive(f));
            ++bad;
        }
        immutable got = atomicLoad(g_totalCalls);
        expectCallRange(0, N, "T20 per-id completion");
        expectDrainedFarm(f, "T20 drained roots, leaves, and membership");
        if (got != expected)
        {
            fprintf(stderr, "T20 trial %d: calls=%lld expected=%lld\n", trial, got, expected);
            ++bad;
        }
        fprintf(stderr, "T20 trial %d: confirmed=%u live=%u calls=%lld/%lld\n",
            trial, confirmedCount, countLive(f), got, expected);
        fflush(stderr);
    }
    check(bad == 0, "T20 confirmed segment left Live or lost work");
    say("T20 confirmed segments discharged OK");
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
    auto f = AntFarm.create(2, 8, 2, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    enum size_t N = T21_N;
    enum size_t BODY_LEN = T21_BODY_LEN;

    allocCalls(N);
    scope (exit) freeCalls();
    atomicStore!(MemoryOrder.raw)(g_contentSum, 0L);
    atomicStore!(MemoryOrder.raw)(g_contentBad, 0L);

    // Custom bodies carry the index-dependent pattern checked by contentSumCb.
    auto headers = cast(Callback*) malloc(N * Callback.sizeof);
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
        headers[i] = Callback.init;

        headers[i] = &contentSumCb;
        foreach (k; 0 .. BODY_LEN)
            bodies[i * BODY_LEN + k] = i + k;
        entries[i].call = headers[i];
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
    // (sq = 3), for every class 0 .. MAX_AVG_COST:
    //   avgCost 0 -> chunk 128 -> threshold 256 (255 small, 257 sharded; 3 * 128 clamps to 256)
    //   avgCost 1 -> chunk  64 -> threshold 192 (191 small, 193 sharded)
    //   avgCost 2 -> chunk  32 -> threshold  96  (95 small, 97 sharded)
    //   avgCost 3 -> chunk  16 -> threshold  48  (47 small, 49 sharded)
    //   avgCost 4 -> chunk   8 -> threshold  24  (23 small, 25 sharded)
    //   avgCost 5 -> chunk   4 -> threshold  16  (15 small, 17 sharded)
    //   avgCost 6 -> chunk   2 -> threshold  16  (15 small, 17 sharded)
    //   avgCost 7 -> chunk   1 -> threshold  16  (15 small, 17 sharded)
    // Class 0 is the first with chunks of 128: the 255-entry table is claimed
    // wholesale by shard 0 as two claims (128 and 127); the 257-entry table
    // shards 86/86/85, one claim per shard.
    // Exact per-payload accounting must hold across every consumer geometry.
    auto f = AntFarm.create(2, 8, 8, 1, 0, 1, 4096, 0);
    scope (exit) f.destroy();

    enum sq = 3;
    static struct TableSpec
    {
        uint tlen;
        uint avgCost;
        Tier tier;
    }
    TableSpec[2 * (MAX_AVG_COST + 1)] specs;
    size_t N;
    foreach (avgCost; 0 .. MAX_AVG_COST + 1)
    {
        immutable want = sq * (MAX_CHUNK >> avgCost);
        immutable threshold = want < 16 ? 16 : (want > 256 ? 256 : want);
        // The classes 0 .. 2 tables (up to 257 entries) use the bulk tier;
        // the small tier's 4096-word quota is reserved for the class 3 and
        // smaller tables (49 entries and fewer).
        immutable tier = threshold > 48 ? Tier.bulk : Tier.small;
        specs[2 * avgCost] = TableSpec(threshold - 1, avgCost, tier);
        specs[2 * avgCost + 1] = TableSpec(threshold + 1, avgCost, tier);
        N += 2 * threshold;
    }
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
        plen = 2;
    });
    scope (exit) freeBatch(batch);

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
    check(countLive(f) == 1, "T22 only the frontier Live");
    expectNoLiveConsumerRefs(f, "T22");
    say("T22 avgCost sharding exact OK");
}

void t25_single_shot_claim_contract()
{
    // Primary/sweeper visits compete for the same table shard claims. Check
    // exact execution and borrowed metadata across ring reuse, including
    // chunk size one where claim contention is greatest.
    foreach (avgCost; [0U, avgCostForChunk(32), MAX_AVG_COST])
    {
        auto f = AntFarm.create(2, 8, 8, 1, 16384, 1, 4096, 0);
        scope (exit) f.destroy();
        enum N = 12000;
        allocCalls(N);
        scope (exit) freeCalls();
        atomicStore!(MemoryOrder.raw)(g_claimContractBad, 0L);
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
            plen = 47 + i % 3;
        });
        scope (exit) freeBatch(batch);
        foreach (i; 0 .. N) batch.entries[i].call = i % 2
            ? &claimContractCb!true : &claimContractCb!false;

        enum NC = 8;
        Thread[NC] consumers;
        ConsCtx[NC] contexts;
        auto deadline = MonoTime.currTime + 90.seconds;
        foreach (i; 0 .. NC)
        {
            contexts[i] = ConsCtx(f, batch.expectedCalls, deadline);
            auto context = &contexts[i];
            consumers[i] = new Thread({ consumerMain(context); });
        }
        ProdCtx first = ProdCtx(f, batch.entries, N / 2, Tier.bulk);
        ProdCtx second = ProdCtx(f, batch.entries + N / 2, N / 2, Tier.small);
        first.avgCost = second.avgCost = avgCost;
        auto p1 = new Thread({ producerMain(&first); });
        auto p2 = new Thread({ producerMain(&second); });
        foreach (thread; consumers) thread.start();
        while (atomicLoad!(MemoryOrder.acq)(f.Cf) != NC)
        {
            check(MonoTime.currTime < deadline, "T25 subscription timeout");
            Thread.yield();
        }
        p1.start();
        p2.start();
        p1.join();
        p2.join();
        foreach (thread; consumers) thread.join();

        ConsumerView revisit;
        check(revisit.subscribe(f) >= 0, "T25 resubscribe");
        while (revisit.consumeNext()) {}
        revisit.unsubscribe();
        expectExactCalls(&batch, "T25");
        check(atomicLoad!(MemoryOrder.raw)(g_claimContractBad) == 0,
            "T25 borrowed callback metadata");
        check(atomicLoad!(MemoryOrder.raw)(f.Wt) > 2 * f.Ln, "T25 multiple ring laps");
        expectNoLiveConsumerRefs(f, "T25");
    }
    say("T25 single-shot claim contract and mixed-callback exact execution OK");
}

void t23_late_attach_under_wrap()
{
    // Protocol hole T11/T06/T18 miss: attach once, no retry, while the ring
    // is already wrapping. Producers cannot lap a quiet farm (epoch 0 is Live),
    // so one primer consumer starts with them; late subscribers join after
    // Wt > Ln. Fail on watchdog, lost work, leftover Sub, or unprotected
    // incomplete segments.
    enum TRIALS = 3;
    // Enough two-word ST payloads to wrap the 2 MiB ring even in the
    // uniform layout (SPEC 2g kind 2: 2 ring words per entry plus tables).
    enum N = 160000;
    enum NP = 6;
    enum NLATE = 4;
    foreach (trial; 0 .. TRIALS)
    {
        auto f = AntFarm.create(2, 8, 1 + NLATE, 0, 0, NP, 4096);
        scope (exit) f.destroy();
        allocCalls(N);
        scope (exit) freeCalls();
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {

            plen = 2;
        });
        scope (exit) freeBatch(batch);

        auto deadline = MonoTime.currTime + 90.seconds;
        ProdCtx[NP] pctx;
        auto prodJobs = new ProdJob[](NP);
        Thread[NP] producers;
        foreach (i; 0 .. NP)
        {
            immutable remain = N - (N / NP) * i;
            immutable share = (i + 1 == NP) ? remain : (N / NP);
            pctx[i] = ProdCtx(f, batch.entries + (N / NP) * i, share,
                              Tier.small, 64);
            prodJobs[i] = new ProdJob(&pctx[i]);
            producers[i] = new Thread(&prodJobs[i].run);
        }

        ConsCtx primer = ConsCtx(f, batch.expectedCalls, deadline);
        auto primerJob = new ConsJob(&primer);
        auto primerT = new Thread(&primerJob.run);
        primerT.start();
        foreach (t; producers)
            t.start();

        auto wrapDeadline = MonoTime.currTime + 30.seconds;
        while (atomicLoad!(MemoryOrder.raw)(f.Wt) <= f.Ln)
        {
            if (MonoTime.currTime > wrapDeadline)
            {
                fprintf(stderr, "T23 trial %d: no wrap Wt=%llu\n",
                    trial, cast(ulong) atomicLoad!(MemoryOrder.raw)(f.Wt));
                abort();
            }
            Thread.yield();
        }

        ConsCtx[NLATE] lateCtx;
        auto lateJobs = new ConsJob[](NLATE);
        Thread[NLATE] late;
        foreach (i; 0 .. NLATE)
        {
            lateCtx[i] = ConsCtx(f, batch.expectedCalls, deadline);
            lateJobs[i] = new ConsJob(&lateCtx[i]);
            late[i] = new Thread(&lateJobs[i].run);
            late[i].start();
        }

        foreach (t; producers)
            t.join();
        primerT.join();
        foreach (t; late)
            t.join();

        expectExactCalls(&batch, "T23");
        expectNoLiveConsumerRefs(f, "T23");
        expectNoLeftoverSub(f, "T23");
        check(countMissingLive(f) == 0, "T23 every unconfirmed epoch remains Live");
        printf("T23 trial %d OK Wt=%llu\n",
            trial, cast(ulong) atomicLoad!(MemoryOrder.raw)(f.Wt));
        fflush(stdout);
    }
    say("T23 late attach under wrap OK");
}

void t24_topology_hang_canary()
{
    // Scale-down of the throughput grid cells that deadlocked at nc=4,
    // ns=9/11 (body=1, batch=256, runs of 32: avgCost=0 when MAX_CHUNK was
    // 32). Subscribe-before-write like the harness: hang here is the same
    // failure mode whether or not it is wrap-during-subscribe.
    foreach (ns; [9u, 11u])
    {
        enum NC = 4;
        enum N = 40000;
        enum BATCH = 256;
        auto f = AntFarm.create(2, 8, NC, 0, 0, ns, 4096);
        scope (exit) f.destroy();
        allocCalls(N);
        scope (exit) freeCalls();
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {

            plen = 1;
        });
        scope (exit) freeBatch(batch);

        auto deadline = MonoTime.currTime + 90.seconds;
        ConsCtx[NC] cctx;
        auto consJobs = new ConsJob[](NC);
        Thread[NC] consumers;
        foreach (i; 0 .. NC)
        {
            cctx[i] = ConsCtx(f, batch.expectedCalls, deadline);
            consJobs[i] = new ConsJob(&cctx[i]);
            consumers[i] = new Thread(&consJobs[i].run);
            consumers[i].start();
        }
        while (atomicLoad!(MemoryOrder.acq)(f.Cf) < NC)
        {
            if (MonoTime.currTime > deadline)
            {
                fprintf(stderr, "T24 ns=%u subscribe timeout\n", ns);
                abort();
            }
            Thread.yield();
        }

        auto pctx = new ProdCtx[](ns);
        auto prodJobs = new ProdJob[](ns);
        Thread[] producers = new Thread[ns];
        size_t cursor;
        foreach (i; 0 .. ns)
        {
            immutable remain = N - cursor;
            immutable share = remain / (ns - i);
            pctx[i] = ProdCtx(f, batch.entries + cursor, share, Tier.small, BATCH, null, avgCostForChunk(32));
            prodJobs[i] = new ProdJob(&pctx[i]);
            producers[i] = new Thread(&prodJobs[i].run);
            producers[i].start();
            cursor += share;
        }
        foreach (t; producers)
            t.join();
        foreach (t; consumers)
            t.join();

        expectExactCalls(&batch, "T24");
        expectNoLiveConsumerRefs(f, "T24");
        expectNoLeftoverSub(f, "T24");
        check(countMissingLive(f) == 0, "T24 every unconfirmed epoch remains Live");
        printf("T24 ns=%u OK Wt=%llu\n",
            ns, cast(ulong) atomicLoad!(MemoryOrder.raw)(f.Wt));
        fflush(stdout);
    }
    say("T24 topology hang canary OK");
}

// T28: a complete cohort joins, drains, and departs before the next cohort
// can subscribe. No steady consumer or final rescue can cover a bad attach.
// Barriers control phases only; this is a lifetime/progress test, not a test
// of the callback publication happens-before edges (those have separate lanes).
private enum HERD_ROUNDS = 4;
private enum HERD_PAYLOADS = 4096;

private class HerdRound
{
    AntFarm* farm;
    Barrier gate;
    MonoTime deadline;
    shared long target;
    shared bool finished;

    this(AntFarm* f, uint members)
    {
        farm = f;
        gate = new Barrier(members + 1);
        deadline = MonoTime.currTime + DEFAULT_WATCHDOG_SECS.seconds;
    }

    void watchdog()
    {
        while (!atomicLoad(finished))
        {
            if (MonoTime.currTime > deadline)
            {
                fprintf(stderr, "T28 cohort watchdog: calls=%lld target=%lld Cf=%lld Wt=%llu\n",
                    atomicLoad(g_totalCalls), atomicLoad(target), atomicLoad(farm.Cf), atomicLoad(farm.Wt));
                abort();
            }
            Thread.sleep(5.msecs);
        }
    }
}

private class HerdMember
{
    HerdRound round;
    uint completed;
    this(HerdRound r) { round = r; }

    void run()
    {
        foreach (_; 0 .. HERD_ROUNDS)
        {
            round.gate.wait(); // controller finished the optional no-member fill
            ConsumerView view;
            long result;
            do
            {
                result = view.subscribeOnce(round.farm);
                if (result == SUBSCRIBE_RETRY) Thread.yield();
            } while (result == SUBSCRIBE_RETRY);
            check(result >= 0, "T28 admitted herd member attaches");
            round.gate.wait(); // all members attached, none consuming yet
            round.gate.wait(); // controller starts remaining production
            while (atomicLoad(g_totalCalls) < atomicLoad(round.target))
                if (!view.consumeNext()) Thread.yield();
            while (view.consumeNext()) {}
            round.gate.wait(); // all callbacks returned; keep these views alive
            round.gate.wait(); // controller checked per-id completion before departure
            view.unsubscribe();
            ++completed;
            round.gate.wait(); // controller can inspect quiescent state
        }
    }
}

void t28_subscription_herds()
{
    foreach (k; [2u, 16u]) foreach (members; [16u, 32u])
    {
        immutable segCap = (1UL << ULONGS_PER_MIB_SHIFT) / k;
        auto f = AntFarm.create(1, k, members, 0, 0, 2, segCap / 2);
        scope (exit) f.destroy();
        enum N = HERD_ROUNDS * HERD_PAYLOADS;
        allocCalls(N);
        scope (exit) freeCalls();
        auto batch = makeBatch(N, (size_t i, ref Callback h, ref size_t plen) {
            plen = 32;
        });
        scope (exit) freeBatch(batch);
        auto round = new HerdRound(f, members);
        auto watchdog = new Thread(&round.watchdog);
        watchdog.start();
        auto jobs = new HerdMember[members];
        auto threads = new Thread[members];
        foreach (i; 0 .. members)
        {
            jobs[i] = new HerdMember(round);
            threads[i] = new Thread(&jobs[i].run);
            threads[i].start();
        }
        foreach (cycle; 0 .. HERD_ROUNDS)
        {
            immutable first = cycle * HERD_PAYLOADS;
            immutable end = first + HERD_PAYLOADS;
            enum half = HERD_PAYLOADS / 2;
            size_t[2] written;
            atomicStore(round.target, cast(long) end);
            if ((cycle & 1) == 0)
            {
                // Wake into backpressure: fill until both producers refuse,
                // while no view exists. Each producer owns a disjoint ID range.
                auto q0 = f.registerProducer(Tier.small);
                auto q1 = f.registerProducer(Tier.small);
                check(q0.valid && q1.valid, "T28 prefill tokens");
                Token*[2] tokens = [&q0, &q1];
                for (;;)
                {
                    ulong progress;
                    foreach (p; 0 .. 2)
                    {
                        immutable start = first + p * half + written[p];
                        immutable count = half - written[p] < 64 ? half - written[p] : 64;
                        immutable n = f.write(batch.entries[start .. start + count], *tokens[p]);
                        written[p] += n;
                        progress += n;
                    }
                    if (progress == 0) break;
                }
                f.unregisterProducer(q0);
                f.unregisterProducer(q1);
                check(written[0] + written[1] > 0 && written[0] + written[1] < HERD_PAYLOADS,
                    "T28 wake begins with a refused write and unfinished backlog");
                check(atomicLoad(g_totalCalls) == first, "T28 sleeping cohort executed nothing");
                expectNoLiveConsumerRefs(f, "T28 backlog is protected without any consumers");
                check(countMissingLive(f) == 0, "T28 sleeping backlog remains Live");
            }
            round.gate.wait();
            round.gate.wait();
            check(atomicLoad(f.Cf) == members, "T28 entire cohort is attached");
            expectNoLeftoverSub(f, "T28 successful attachments dropped every transient pin");

            ProdCtx[2] contexts;
            Thread[2] producers;
            foreach (p; 0 .. 2)
            {
                contexts[p] = ProdCtx(f, batch.entries + first + p * half + written[p],
                    half - written[p], Tier.small, 64);
                auto job = new ProdJob(&contexts[p]);
                producers[p] = new Thread(&job.run);
                producers[p].start();
            }
            round.gate.wait();
            foreach (p; producers) p.join();
            round.gate.wait();
            expectCallRange(0, end, "T28 existing cohort completed every published id exactly once");
            foreach (i; end .. N)
                check(atomicLoad(g_calls[i]) == 0, "T28 unpublished future payload never executed");
            check(atomicLoad(g_totalCalls) == end, "T28 no extra callback execution");
            round.gate.wait();
            round.gate.wait();
            expectDrainedFarm(f, "T28 last departures leave only the Live frontier");
        }
        foreach (t; threads) t.join();
        atomicStore(round.finished, true);
        watchdog.join();
        foreach (job; jobs) check(job.completed == HERD_ROUNDS, "T28 every thread finished every cycle");
        check(atomicLoad(f.Wt) > 3 * f.Ln, "T28 actual ring reuse across cohort changes");
        expectExactCalls(&batch, "T28 final ledger, without a fresh subscriber");
        printf("T28 K=%u members=%u wake/drained cohorts OK\n", k, members);
        fflush(stdout);
    }
}

void main(string[] args)
{
    immutable names = ["T00", "T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08",
        "T09", "T10", "T11", "T12", "T13", "T14", "T15", "T16", "T17", "T18",
        "T20", "T21", "T22", "T23", "T24", "T25", "T26", "T28"];
    foreach (arg; args[1 .. $])
    {
        bool valid;
        foreach (name; names) if (arg == name) valid = true;
        if (!valid)
        {
            fprintf(stderr, "unknown torture selector: %.*s (T19 and T27 have separate binaries)\n",
                cast(int) arg.length, arg.ptr);
            exit(64);
        }
    }
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

    if (want("T00"))
        t00_harness_oracles();
    if (want("T28"))
        t28_subscription_herds();
    if (want("T01"))
        t01_bad_header();
    if (want("T16"))
        t16_position_ref_stall();
    if (want("T02"))
        t02_single_shot_revisit();
    if (want("T17"))
        t17_write_size_wrap();
    if (want("T26"))
        t26_compact_singleton_contract();
    if (want("T18"))
        t18_pure_churn_orphan();
    if (want("T20"))
        t20_confirmed_discharged();
    if (want("T21"))
        t21_payload_content_multilap();
    if (want("T22"))
        t22_avgcost_sharding();
    if (want("T23"))
        t23_late_attach_under_wrap();
    if (want("T24"))
        t24_topology_hang_canary();
    if (want("T25"))
        t25_single_shot_claim_contract();

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
        t09_slow_single_shot();
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

    say("ALL TORTURE TESTS PASSED");
}
