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

// Thread-local: each pinned worker owns one persistent consumer cursor.
ConsumerView t_view;
bool t_subscribed;

bool pump(WorkerSelf* w) nothrow @nogc @system
{
    auto slot = home!FarmBin();
    if (slot is null)
        return false;
    if (!t_subscribed)
        t_subscribed = t_view.subscribe(slot.farm) >= 0;
    if (!t_subscribed)
        return false;

    return t_view.consumeNext();
}

// Runs once on each worker during pool shutdown, after its last pump. The
// Farm may be destroyed only after every worker has unsubscribed here.
void stopWorker(WorkerSelf* w) nothrow @nogc @system
{
    if (t_subscribed)
    {
        t_view.unsubscribe();
        t_subscribed = false;
    }
}

void main()
{
    auto topo = CacheAwarePool.topology();

    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.workerBody = &pump;
    opt.workerStop = &stopWorker;

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
    scope (exit) pool.shutdown();
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
