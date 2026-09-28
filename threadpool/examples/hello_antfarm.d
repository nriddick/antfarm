/++
 + Hello: CacheAwarePool workers install one Ant Farm per LLC, then
 + `home!FarmBin()` both publishes and drains a few payloads.
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

__gshared shared(long) g_calls;
__gshared shared(long) g_sum;
__gshared shared(int) g_didProduce;
__gshared uint g_expect;

// Thread-local: each pinned worker owns one persistent consumer cursor.
ConsumerView t_view;
bool t_subscribed;

long helloCb(PayloadHeader*, PayloadBody b, ulong) nothrow @nogc @system
{
    atomicFetchAdd(g_calls, 1L);
    if (b.length)
        atomicFetchAdd(g_sum, cast(long) b[0]);
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

    if (cas(&g_didProduce, 0, 1))
    {
        auto tok = slot.farm.registerProducer(Tier.small);
        if (tok.valid)
        {
            PayloadHeader hdr;
            hdr.maxCs = 1;
            hdr.done = 1;
            hdr.call = &helloCb;
            ulong[1] buf = [42];
            PayloadEntry e;
            e.header = &hdr;
            e.body = buf[];
            PayloadEntry[4] batch = [e, e, e, e];
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
    printf("hello_antfarm  os=%.*s  llc=%u  lps=%u\n",
        cast(int) topo.os.length, topo.os.ptr,
        topo.llcCount, topo.logicalProcessorCount);

    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.workerBody = &pump;
    opt.workerStop = &stopWorker;

    auto bins = new FarmBin[](topo.llcCount);
    foreach (i, ref b; bins)
    {
        uint ncons;
        foreach (ref p; topo.processors)
        {
            if (p.llcIndex != i) continue;
            if (opt.skipSmtSiblings && p.smtSibling) continue;
            ++ncons;
        }
        if (ncons == 0)
            ncons = 1;
        b.farm = AntFarm.create(1UL << 21, 8, ncons, 0, 0, 8, 4096,
            DEFAULT_SMALL_TABLE_THRESHOLD, false);
    }
    install(bins);
    setLabel!FarmBin(cast(ushort) 0, "hello");
    scope (exit)
    {
        uninstall!FarmBin();
        foreach (ref b; bins)
            if (b.farm !is null)
                b.farm.destroy();
    }

    g_expect = 4;
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown();

    {
        auto d = pool.director();
        d.spin();
        foreach (_; 0 .. 400)
        {
            if (atomicLoad(g_calls) >= g_expect)
                break;
            Thread.sleep(msecs(5));
        }
    }
    // Workers unsubscribe in stopWorker before shutdown returns, so the
    // scope(exit) blocks can then uninstall the bins and destroy the Farms.
    pool.shutdown();

    printf("received %lld payloads  sum=%lld (want %u x 42 = %lld)\n",
        atomicLoad(g_calls), atomicLoad(g_sum),
        g_expect, cast(long) g_expect * 42);
    if (atomicLoad(g_calls) != g_expect || atomicLoad(g_sum) != g_expect * 42)
    {
        fprintf(stderr, "hello_antfarm: unexpected counts\n");
        return 1;
    }
    printf("hello_antfarm: ok\n");
    return 0;
}
