module iota_sum;

import antfarm_templates;
import core.atomic;
import core.thread;
import std.range : iota;
import std.stdio;
import threadpool;

enum ulong nTotal = 50_000;

__gshared shared(long) g_sum;
__gshared shared(ulong) g_done;

void sumJob(ulong x) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.rel)(g_sum, cast(long) x);
    atomicFetchAdd!(MemoryOrder.rel)(g_done, 1UL);
}

struct FarmBin
{
    AntFarm* farm;
}

bool pump(WorkerSelf* w) nothrow @nogc @system
{
    static ConsumerView v;
    static bool subscribed;

    if (atomicLoad!(MemoryOrder.acq)(g_done) >= nTotal)
    {
        if (subscribed)
        {
            v.unsubscribe();
            subscribed = false;
        }
        return false;
    }

    auto slot = home!FarmBin();
    if (slot is null)
        return false;
    if (!subscribed)
        subscribed = v.subscribe(slot.farm) >= 0;
    if (!subscribed)
        return false;

    return v.consumeNext();
}

void main()
{
    auto topo = CacheAwarePool.topology();

    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.workerBody = &pump;

    uint ncons;
    foreach (ref p; topo.processors)
    {
        if (opt.skipSmtSiblings && p.smtSibling) continue;
        ++ncons;
    }

    auto f = AntFarm.create(1UL << 18, 8, ncons, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    auto bins = new FarmBin[](topo.llcCount);
    foreach (ref b; bins)
        b.farm = f;
    install(bins);
    scope (exit) uninstall!FarmBin();

    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown(true);
    pool.director().spin();

    auto tok = f.registerProducer(Tier.small);
    auto payloads = payloadRange!sumJob(iota(1, nTotal + 1));
    for (ulong left = nTotal;;)
    {
        immutable w = f.write(payloads, tok);
        if (w == 0)
            Thread.yield();
        else
        {
            immutable popped = payloads.popFrontN(w);
            if (popped != w)
                throw new Exception("payload range advanced by the wrong count");
            left -= w;
            if (left == 0) break;
        }
    }
    f.unregisterProducer(tok);

    while (atomicLoad!(MemoryOrder.acq)(g_done) < nTotal)
        Thread.sleep(msecs(1));

    immutable expected = nTotal * (nTotal + 1) / 2;
    assert(atomicLoad!(MemoryOrder.acq)(g_sum) == cast(long) expected);
    writeln("summed 1..", nTotal, " to ",
        atomicLoad!(MemoryOrder.acq)(g_sum));
}
