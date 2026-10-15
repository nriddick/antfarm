/++
 + Hello: CacheAwarePool workers install one Ant Farm per accepted physical
 + group (LLC), then in every group the first worker publishes four payloads
 + into `home!FarmBin()` and that group's workers drain them. Groups without
 + accepted workers (e.g. outside `taskset`) get no Farm and are reported.
 +
 + From the threadpool directory:
 +   dmd -g -i -I.. -Isource examples/hello_antfarm.d -of=hello_antfarm
 +   ANTFARM_HUGE_PAGES=0 ./hello_antfarm
 +
 + (`ldc2` accepts the same arguments; add `.exe` to the output on Windows.)
 +/
module hello_antfarm;

import antfarm;
import core.atomic;
import core.thread;
import core.time : msecs;
import core.stdc.stdio;
import threadpool;

struct FarmBin
{
    AntFarm* farm;
}

enum perGroup = 4;

// Indexed by physical group (llcIndex); sized from the resolution.
__gshared shared(int)[] g_produced;
__gshared shared(long)[] g_received;
__gshared shared(long) g_sum;

// Thread-local: each pinned worker owns one persistent consumer cursor.
ConsumerView t_view;
bool t_subscribed;

long helloCb(PayloadHeader*, PayloadBody b, ulong) nothrow @nogc @system
{
    atomicFetchAdd(g_received[cast(size_t) b[0]], 1L);
    atomicFetchAdd(g_sum, cast(long) b[1]);
    return 1;
}

bool pump(WorkerSelf* w) nothrow @nogc @system
{
    auto slot = home!FarmBin();
    if (slot is null || slot.farm is null)
        return false;

    if (!t_subscribed)
    {
        if (t_view.subscribe(slot.farm) < 0)
            return false;
        t_subscribed = true;
    }

    if (cas(&g_produced[w.llcIndex], 0, 1))
    {
        auto tok = slot.farm.registerProducer(Tier.small);
        if (tok.valid)
        {
            PayloadHeader hdr;
            hdr.maxCs = 1;
            hdr.done = 1;
            hdr.call = &helloCb;
            ulong[2] buf = [w.llcIndex, 42];
            PayloadEntry e;
            e.header = &hdr;
            e.body = buf[];
            PayloadEntry[perGroup] batch = [e, e, e, e];
            ulong left = batch.length;
            while (left)
            {
                immutable n = slot.farm.write(batch[0 .. left], tok);
                if (n == 0)
                    Thread.yield();
                else
                    left -= n;
            }
            slot.farm.unregisterProducer(tok);
        }
    }

    return t_view.consumeNext();
}

// Runs once on each worker during pool shutdown, after its last pump.
void stopWorker(WorkerSelf* w) nothrow @nogc @system
{
    if (t_subscribed)
    {
        t_view.unsubscribe();
        t_subscribed = false;
    }
}

int main()
{
    auto topo = CacheAwarePool.topology();
    printf("hello_antfarm  os=%.*s  physical groups=%u  lps=%u\n",
        cast(int) topo.os.length, topo.os.ptr,
        topo.llcCount, topo.logicalProcessorCount);

    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.workerBody = &pump;
    opt.workerStop = &stopWorker;
    auto pool = new CacheAwarePool(opt);

    // The table is keyed by physical group; only accepted groups get a Farm,
    // sized for exactly that group's workers.
    auto res = pool.resolve();
    auto summary = res.summary();
    printf("%.*s\n", cast(int) summary.length, summary.ptr);
    auto bins = makeBins!FarmBin(res, (ref const g) => g.isAccepted
        ? FarmBin(AntFarm.create(16, 8, g.accepted, 0, 0, 8, 4096,
            DEFAULT_SMALL_TABLE_THRESHOLD, false))
        : FarmBin.init);
    install(bins, res);
    scope (exit)
    {
        uninstall!FarmBin();
        foreach (ref b; bins)
            if (b.farm !is null)
                b.farm.destroy();
    }
    foreach (ref g; res.rejectedGroups)
    {
        auto why = reasonsText(g.common);
        printf("group %u skipped: %.*s\n", g.llcIndex, cast(int) why.length, why.ptr);
    }

    g_produced = new shared(int)[](res.physicalGroupCount);
    g_received = new shared(long)[](res.physicalGroupCount);
    immutable long expect = perGroup * res.acceptedGroupCount;
    pool.start();
    scope (exit) pool.shutdown();

    {
        auto d = pool.director();
        d.spin();
        foreach (_; 0 .. 400)
        {
            long total;
            foreach (ref r; g_received)
                total += atomicLoad(r);
            if (total >= expect)
                break;
            Thread.sleep(msecs(5));
        }
    }
    // Workers unsubscribe in stopWorker before shutdown returns, so the
    // scope(exit) blocks can then uninstall the bins and destroy the Farms.
    pool.shutdown();

    bool ok = atomicLoad(g_sum) == expect * 42;
    long total;
    foreach (ref g; res.groups)
    {
        immutable got = atomicLoad(g_received[g.llcIndex]);
        total += got;
        immutable want = g.isAccepted ? perGroup : 0;
        if (g.isAccepted)
            printf("group %u: %u workers received %lld payloads (want %d)\n",
                g.llcIndex, g.accepted, got, want);
        ok = ok && got == want;
    }
    printf("received %lld payloads in %u groups  sum=%lld (want %lld x 42)\n",
        total, res.acceptedGroupCount, atomicLoad(g_sum), expect);
    if (!ok)
    {
        fprintf(stderr, "hello_antfarm: unexpected counts\n");
        return 1;
    }
    printf("hello_antfarm: pinned workers consumed payloads in each accepted LLC group\n");
    return 0;
}
