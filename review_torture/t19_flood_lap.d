/++
 + T19: interleaved production/churn and producer-quota integrity.
 +
 + This is a standalone stress driver, separate from torture_tests.d. It
 + exercises a bulk dump, optional 1-payload small-tier mid-tick writes,
 + subscription churn, and an optional steady consumer.
 +
 + Important: each producer keeps one persistent Exi variable for its token.
 + Resetting Exi manually is not a valid producer API violation: Exi is an
 + explicit caller-owned quota and write() may refresh it after checking the
 + farm. The earlier throwaway flood probe copied Exi per write and therefore
 + manufactured a false product failure.
 +/
module t19_flood_lap;

import antfarm_templates;
import torture_common;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : abort;

__gshared shared(int) g_stop;
__gshared shared(long) g_sentinelCalls;

long sentinelCb(ulong marker) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(g_sentinelCalls, 1L);
    return 1;
}

class ProducerJob
{
    AntFarm* f;
    PayloadEntry* entries;
    size_t count;
    Tier tier;
    size_t maxBatch;

    this(AntFarm* farm, PayloadEntry* input, size_t n, Tier producerTier, size_t batch)
    {
        f = farm;
        entries = input;
        count = n;
        tier = producerTier;
        maxBatch = batch;
    }

    void run()
    {
        auto tok = f.registerProducer(tier);
        check(tok.valid, "T19 producer registration");
        size_t off;
        while (off < count)
        {
            immutable remain = count - off;
            immutable take = maxBatch == 0 || maxBatch >= remain ? remain : maxBatch;
            immutable n = f.write(entries[off .. off + take], tok);
            off += n;
            if (n == 0)
                Thread.yield();
        }
        f.unregisterProducer(tok);
    }
}

class ChurnJob
{
    AntFarm* f;
    bool consume;
    uint burst;
    MonoTime deadline;

    this(AntFarm* farm, bool doConsume, uint burstCount, MonoTime limit)
    {
        f = farm;
        consume = doConsume;
        burst = burstCount;
        deadline = limit;
    }

    void run()
    {
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0)
        {
            ConsumerView v;
            if (v.subscribe(f) >= 0)
            {
                if (consume)
                {
                    foreach (_; 0 .. burst)
                    {
                        v.consumeNext();
                        if (MonoTime.currTime > deadline)
                            abort();
                    }
                }
                v.unsubscribe();
            }
            Thread.yield();
        }
    }
}

class SteadyJob
{
    AntFarm* f;

    this(AntFarm* farm) { f = farm; }

    void run()
    {
        ConsumerView v;
        check(v.subscribe(f) >= 0, "T19 steady subscribe");
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0)
        {
            v.consumeNext();
            Thread.yield();
        }
        foreach (_; 0 .. 10_000)
            if (!v.consumeNext())
                break;
        v.unsubscribe();
    }
}

class SentinelJob
{
    AntFarm* f;
    uint yieldCount;

    this(AntFarm* farm, uint yields)
    {
        f = farm;
        yieldCount = yields;
    }

    void run()
    {
        auto tok = f.registerProducer(Tier.small);
        check(tok.valid, "T19 sentinel registration");

        PayloadHeader h;
        ulong body = ulong.max;
        PayloadEntry e = payloadEntry!sentinelCb(&h, (&body)[0 .. 1], body);

        // This variable must persist across the entire producer lifetime.
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0)
        {
            cast(void) f.write((&e)[0 .. 1], tok);
            foreach (_; 0 .. yieldCount)
                Thread.yield();
        }
        f.unregisterProducer(tok);
    }
}

enum Arm : ubyte
{
    noMidTick,
    pureChurn,
    consumingChurn,
    steady,
}

void runArm(Arm arm)
{
    auto f = AntFarm.create(1 << 18, 8, 8, 1, 4096, 4, 2048);
    scope (exit) f.destroy();

    enum N = 600;
    allocCalls(N);
    scope (exit) freeCalls();
    auto batch = makeBatch(N, (size_t i, ref PayloadHeader h, ref size_t plen) {
        immutable mt = i % 4 == 0;
        h.maxCs = mt ? 3 : 1;
        h.done = mt ? 5 : 1;
        plen = 2;
    });
    scope (exit) freeBatch(batch);

    atomicStore!(MemoryOrder.raw)(g_stop, 0);
    atomicStore!(MemoryOrder.raw)(g_sentinelCalls, 0L);
    auto deadline = MonoTime.currTime + 60.seconds;

    Thread steadyThread;
    if (arm == Arm.steady)
    {
        auto job = new SteadyJob(f);
        steadyThread = new Thread(&job.run);
        steadyThread.start();
    }

    immutable nchurn = arm == Arm.steady ? 3 : 6;
    Thread[] churnThreads;
    churnThreads.length = nchurn;
    foreach (i; 0 .. nchurn)
    {
        immutable doConsume = arm != Arm.pureChurn;
        auto job = new ChurnJob(f, doConsume, cast(uint)(5 + i % 4), deadline);
        churnThreads[i] = new Thread(&job.run);
        churnThreads[i].start();
    }

    auto dumpJob = new ProducerJob(f, batch.entries, N, Tier.bulk, 8);
    auto dumpThread = new Thread(&dumpJob.run);
    dumpThread.start();

    Thread sentinelThread;
    if (arm != Arm.noMidTick)
    {
        auto job = new SentinelJob(f, 300);
        sentinelThread = new Thread(&job.run);
        sentinelThread.start();
    }

    dumpThread.join();
    Thread.sleep(500.msecs);
    atomicStore!(MemoryOrder.raw)(g_stop, 1);

    foreach (t; churnThreads)
        t.join();
    if (steadyThread !is null)
        steadyThread.join();
    if (sentinelThread !is null)
        sentinelThread.join();

    ConsumerView late;
    check(late.subscribe(f) >= 0, "T19 late subscribe");
    auto salvageDeadline = MonoTime.currTime + 30.seconds;
    while (atomicLoad!(MemoryOrder.acq)(g_totalCalls) < batch.expectedCalls)
    {
        late.consumeNext();
        if (MonoTime.currTime > salvageDeadline)
            break;
    }
    late.unsubscribe();

    expectExactCalls(&batch, "T19 exact batch calls");
    expectNoLiveConsumerRefs(f, "T19 reference cleanup");
    check(countSub0Pulses(f) >= 1, "T19 pulse");

    immutable sent = atomicLoad!(MemoryOrder.raw)(g_sentinelCalls);
    if (arm == Arm.steady)
        check(sent > 32, "T19 steady arm must sustain mid-tick writes");

    printf("T19 arm=%u batch=%lld/%lld sent=%lld OK\n",
        cast(uint) arm,
        atomicLoad!(MemoryOrder.raw)(g_totalCalls), batch.expectedCalls, sent);
    fflush(stdout);
}

void testQuotaResetRejected()
{
    version (Posix)
    {
        import core.sys.posix.unistd : fork, _exit;
        import core.sys.posix.sys.wait : waitpid;
        import core.sys.posix.signal : SIGABRT;

        immutable pid = fork();
        check(pid >= 0, "T19 fork");
        if (pid == 0)
        {
            auto f = AntFarm.create(1 << 18, 8, 1, 1, 2048, 1, 512);
            auto tok = f.registerProducer(Tier.small);
            if (!tok.valid)
                _exit(90);

            PayloadHeader h;
            ulong body = 0;
            PayloadEntry e = payloadEntry!sentinelCb(&h, (&body)[0 .. 1], body);

            // Drain the granted quota first: the ledger shrinks below quota.
            while (f.write((&e)[0 .. 1], tok) != 0) {}
            if (f.write((&e)[0 .. 1], tok) != 0)
                _exit(91);

            // Hostile caller forges a fresh quota by overwriting the private
            // mirror past what the farm's ledger still holds. write() must
            // fatal (SIGABRT) rather than accept the forged excursion.
            union Overlay
            {
                Token tok;
                struct Fields
                {
                    Tier tier;
                    uint slot;
                    ulong hash;
                    ulong quotaLeft;
                }
                Fields fld;
            }
            Overlay ov;
            ov.tok = tok;
            ov.fld.quotaLeft = f.quotaSmall; // exceed the remaining ledger
            auto forged = ov.tok;
            cast(void) f.write((&e)[0 .. 1], forged);
            _exit(0); // failure: forged quota was accepted
        }

        int status;
        waitpid(pid, &status, 0);
        immutable signaled = ((status & 0x7f) != 0) && ((status & 0x7f) != 0x7f);
        immutable sig = status & 0x7f;
        check(signaled && sig == SIGABRT, "T19 forged quota must SIGABRT");
        printf("T19 forged quota rejected OK\n");
        fflush(stdout);
    }
}

void main()
{
    runArm(Arm.noMidTick);
    runArm(Arm.pureChurn);
    runArm(Arm.consumingChurn);
    runArm(Arm.steady);
    testQuotaResetRejected();
    say("T19 ALL TESTS PASSED");
}
