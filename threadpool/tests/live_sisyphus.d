module live_sisyphus;

import core.atomic;
import core.thread;
import core.time : msecs;
import std.stdio;
import threadpool;

version (Windows)
    import threadpool.sys.win_bindings : PROCESSOR_NUMBER, GetCurrentProcessorNumberEx;
else version (linux)
    import core.sys.linux.sched : sched_getcpu;

private uint currentLp() @nogc nothrow
{
    version (Windows)
    {
        PROCESSOR_NUMBER pn;
        GetCurrentProcessorNumberEx(&pn);
        return pn.Number;
    }
    else version (linux)
    {
        auto cpu = sched_getcpu();
        return cpu < 0 ? uint.max : cast(uint) cpu;
    }
    else
        return uint.max;
}

struct LiveC1 { int x = 1; }
struct LiveC2 { int y = 2; }

struct WorkBag
{
    shared int pLeft;
    shared int eLeft;
}

__gshared uint ranP = uint.max;
__gshared uint ranE = uint.max;
__gshared ushort homeP = ushort.max;
__gshared ushort homeE = ushort.max;
__gshared ushort numaCountP;
__gshared ushort foreignP;
shared int gBodyTicks;

extern (D) bool liveBody(WorkerSelf* w) @nogc nothrow
{
    atomicFetchAdd(gBodyTicks, 1);
    auto bag = home!WorkBag();
    if (bag is null) return false;
    if (w.isP)
    {
        auto n = atomicLoad(bag.pLeft);
        if (n == 0) return false;
        if (cas(&bag.pLeft, n, n - 1))
        {
            ranP = currentLp();
            homeP = w.homeLlc;
            numaCountP = w.numaLlcCount;
            foreignP = w.foreignLlcCount;
            auto c1 = home!LiveC1();
            auto c2 = home!LiveC2();
            if (c1 is null || c1.x != 1) return false;
            if (c2 is null || c2.y != 2) return false;
            if (search!(LiveC1, string)("llc0") !is c1) return false;
            return true;
        }
        return true;
    }
    auto n = atomicLoad(bag.eLeft);
    if (n == 0) return false;
    if (cas(&bag.eLeft, n, n - 1))
    {
        ranE = currentLp();
        homeE = w.homeLlc;
        return true;
    }
    return true;
}

extern (D) bool tickBody(WorkerSelf*) @nogc nothrow
{
    atomicFetchAdd(gBodyTicks, 1);
    return false;
}

void dump(const(TopologySnapshot) s)
{
    writeln("os=", s.os,
        " lps=", s.logicalProcessorCount,
        " llcCount=", s.llcCount,
        " pCores=", s.pCoreCount,
        " eCores=", s.eCoreCount,
        " classCount=", s.classCount,
        " line=", s.cacheLineSize);
    foreach (ref p; s.processors)
    {
        writefln("  lp=%s group=%s core=%s llc=%s llcInGroup=%s eff=%s smtSib=%s parked=%s cpuSet=%s l2=%s l3=%s mod=%s",
            p.lpIndex, p.group, p.coreIndex, p.llcIndex, p.llcIndexInGroup,
            p.efficiencyClass, p.smtSibling, p.parkedAtDiscovery, p.cpuSetId,
            p.l2.sizeBytes, p.l3.sizeBytes, p.moduleIndex);
    }
    foreach (ref d; s.llcDomains)
        writefln("  domain %s l3=%s lps=%s pCores=%s eCores=%s",
            d.llcIndex, d.l3SizeBytes, d.lpIndices, d.pCoreIndices, d.eCoreIndices);
}

bool looksLikeSisyphus(const(TopologySnapshot) s)
{
    return s.logicalProcessorCount == 20
        && s.llcCount == 1
        && s.pCoreCount == 6
        && s.eCoreCount == 8
        && s.classCount == 2;
}

int main()
{
    auto s = CacheAwarePool.topology();
    dump(s);
    if (!looksLikeSisyphus(s))
    {
        writeln("skip: this host is not the i7-12700H SISYPHUS fixture");
        return 0;
    }

    assert(s.cacheLineSize == 64);
    assert(s.llcDomains[0].l3SizeBytes == 24 * 1024 * 1024);
    foreach (ref p; s.processors)
    {
        assert(p.llcIndex == 0);
        assert(p.llcIndexInGroup == 0);
        if (p.lpIndex <= 11)
            assert(p.efficiencyClass == 1);
        else
            assert(p.efficiencyClass == 0);
    }

    auto c1s = new LiveC1[](s.llcCount);
    auto c2s = new LiveC2[](s.llcCount);
    auto bags = new WorkBag[](s.llcCount);
    bags[0].pLeft = 1;
    bags[0].eLeft = 1;
    install(c1s);
    install(c2s);
    install(bags);
    setLabel!LiveC1(cast(ushort) 0, "llc0");
    scope (exit)
    {
        uninstall!LiveC1();
        uninstall!LiveC2();
        uninstall!WorkBag();
    }

    {
        PoolOptions opt;
        opt.workerBody = &liveBody;
        auto pool = new CacheAwarePool(opt);
        pool.start();
        scope (exit) pool.shutdown(true);
        assert(pool.workerCount == 20);
        auto d = pool.director();
        d.spin();
        foreach (_; 0 .. 200)
        {
            if (ranP != uint.max && ranE != uint.max)
                break;
            Thread.sleep(msecs(5));
        }
    }
    writefln("P job on LP %s, E job on LP %s", ranP, ranE);
    assert(ranP != uint.max, "P job did not run");
    assert(ranE != uint.max, "E job did not run");
    assert(ranP <= 11, "P job must run on a P-core LP");
    assert(ranE >= 12 && ranE <= 19, "E job must run on an E-core LP");
    assert(homeP == 0 && homeE == 0, "SISYPHUS workers have LLC 0 as home");
    assert(numaCountP == 1 && foreignP == 0, "one NUMA, one LLC: no foreign bins");

    {
        atomicStore(gBodyTicks, 0);
        PoolOptions opt;
        opt.workerBody = &tickBody;
        auto pool = new CacheAwarePool(opt);
        pool.start();
        pool.shutdown(true);
        pool.start();
        assert(pool.workerCount == 20);
        {
            auto d = pool.director();
            d.spin();
            foreach (_; 0 .. 200)
            {
                if (atomicLoad(gBodyTicks) > 0)
                    break;
                Thread.sleep(msecs(5));
            }
        }
        pool.shutdown(true);
        assert(atomicLoad(gBodyTicks) > 0, "restarted pool did not run the worker body");
        writeln("restart ok");
    }

    {
        PoolOptions opt;
        opt.enableECores = false;
        opt.workerBody = &tickBody;
        auto pool = new CacheAwarePool(opt);
        pool.start();
        scope (exit) pool.shutdown(true);
        assert(pool.workerCount == 12, "P-only pool should be 12 SMT P LPs");
        writeln("enableECores=false has no E workers; shutdown(true) returned");
    }

    writeln("live-sisyphus: pass");
    return 0;
}
