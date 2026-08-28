/++
 + Pin Ant Farm consumers using threadpool topology on this hybrid CPU.
 + P-only vs P+E, one 16 MiB farm (fits the 24 MiB L3).
 +
 + From the threadpool directory:
 +   dmd -g -i examples/live_hybrid.d ../antfarm/antfarm.d ../antfarm/antfarm_templates.d -Isource "-oflive_hybrid.exe"
 +/
module live_hybrid;

import antfarm;
import antfarm_templates;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio;
import threadpool;
import threadpool.pin;

__gshared shared(long) g_calls;
__gshared shared(int) g_stop;

long liveCb(PayloadHeader*, PayloadBody, ulong) nothrow @nogc @system
{
    atomicFetchAdd(g_calls, 1L);
    return 1;
}

struct ConsJob
{
    AntFarm* f;
    LogicalProcessor lp;
    bool eco;
    void run()
    {
        PinTarget t;
        t.cpuSetId = lp.cpuSetId;
        t.group = lp.group;
        t.lpIndex = lp.lpIndex;
        if (!pinToLogicalProcessor(t))
            fprintf(stderr, "pin failed lp=%u group=%u\n", lp.lpIndex, lp.group);
        applyPowerThrottling(eco);
        ConsumerView v;
        if (v.subscribe(f) < 0)
            return;
        while (!atomicLoad(g_stop))
            cast(void) v.consumeNext();
        v.unsubscribe();
    }
}

double runScene(const(TopologySnapshot) snap, bool includeE, ulong nPay)
{
    LogicalProcessor[] lps;
    foreach (ref p; snap.processors)
    {
        auto tc = tableClassIndex(p.efficiencyClass, snap.maxEfficiencyClass, snap.classCount);
        if (p.smtSibling) continue;
        if (tc == classP)
            lps ~= p;
        else if (includeE && tc == classE)
            lps ~= p;
    }
    if (lps.length == 0)
        return 0;

    auto f = AntFarm.create(1UL << 21, 8, cast(uint) lps.length, 1, 0, 0, 4096);
    scope (exit) f.destroy();
    printf("  usedLargePages=%d consumers=%u includeE=%d\n",
        cast(int) f.usedLargePages, cast(uint) lps.length, cast(int) includeE);

    auto p0 = cast(ulong*) f.buf;
    p0[0] = 1;
    if (p0[f.Ln] != 1)
        fatal("wrap alias failed");

    atomicStore(g_calls, 0L);
    atomicStore(g_stop, 0);

    Thread[] th;
    foreach (ref lp; lps)
    {
        auto tc = tableClassIndex(lp.efficiencyClass, snap.maxEfficiencyClass, snap.classCount);
        auto job = ConsJob(f, lp, tc == classE);
        th ~= new class Thread
        {
            ConsJob j;
            this() { j = job; super(&run); }
            void run() { j.run(); }
        };
    }
    foreach (t; th) t.start();
    while (atomicLoad(f.Cf) < cast(long) lps.length)
        Thread.yield();

    PayloadHeader h;
    h.maxCs = 1;
    h.done = 1;
    h.call = &liveCb;
    ulong[2] body = [1, 2];
    PayloadEntry e;
    e.header = &h;
    e.body = body[];

    auto tok = f.registerProducer(Tier.bulk);
    if (!tok.valid) fatal("register");
    auto t0 = MonoTime.currTime;
    ulong left = nPay;
    while (left)
    {
        PayloadEntry[64] batch;
        auto n = left > 64 ? 64 : left;
        foreach (i; 0 .. n)
            batch[i] = e;
        auto w = f.write(batch[0 .. n], tok);
        if (w == 0) Thread.yield();
        else left -= w;
    }
    f.unregisterProducer(tok);
    while (atomicLoad(g_calls) < cast(long) nPay)
        Thread.yield();
    auto secs = (MonoTime.currTime - t0).total!"nsecs" / 1e9;
    atomicStore(g_stop, 1);
    foreach (t; th) t.join();
    return nPay / secs / 1e6;
}

void main()
{
    auto snap = CacheAwarePool.topology();
    printf("os=%.*s lps=%u llc=%u pCores=%u eCores=%u\n",
        cast(int) snap.os.length, snap.os.ptr,
        snap.logicalProcessorCount, snap.llcCount, snap.pCoreCount, snap.eCoreCount);

    enum nPay = 2_000_000UL;
    auto pOnly = runScene(snap, false, nPay);
    printf("P-only  %.1f Mpps\n", pOnly);
    auto pe = runScene(snap, true, nPay);
    printf("P+E     %.1f Mpps\n", pe);
}
