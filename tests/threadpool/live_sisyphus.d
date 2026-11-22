module live_sisyphus;

// Live hardware check for the i7-12700H fixture (`dub run -c live-test`): one
// LLC group of 20 logical processors, 6 SMT P-cores (LP 0 to 11) and 8
// E-cores (LP 12 to 19). Other hosts print the topology and skip.
//
// It checks the discovered shape, the automatic worker counts, the bins and
// labels a worker finds, a restart, and that a class filter is enforced by a
// group mask and a pin and only sets the count for unconfined workers.

import core.atomic;
import core.thread;
import core.time : MonoTime, msecs;
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
    shared int left;
}

__gshared uint ranOn = uint.max;
__gshared ushort homeOf = ushort.max;
__gshared ushort numaCount;
__gshared ushort foreign;
__gshared bool binsFound;
shared int gBodyTicks;
shared ulong gSeenLps; // one bit per LP a body call ran on

extern (D) bool liveBody(WorkerSelf* w) @nogc nothrow
{
    atomicFetchAdd(gBodyTicks, 1);
    auto bag = home!WorkBag();
    if (bag is null) return false;
    auto n = atomicLoad(bag.left);
    if (n == 0) return false;
    if (cas(&bag.left, n, n - 1))
    {
        ranOn = currentLp();
        homeOf = w.homeLlc;
        numaCount = w.numaLlcCount;
        foreign = w.foreignLlcCount;
        auto c1 = home!LiveC1();
        auto c2 = home!LiveC2();
        binsFound = c1 !is null && c1.x == 1 && c2 !is null && c2.y == 2
            && search!(LiveC1, string)("llc0") is c1;
    }
    return true;
}

extern (D) bool tickBody(WorkerSelf*) @nogc nothrow
{
    atomicFetchAdd(gBodyTicks, 1);
    immutable lp = currentLp();
    if (lp < 64) atomicOp!"|="(gSeenLps, 1UL << lp);
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
        writefln("  lp=%s group=%s core=%s llc=%s llcInGroup=%s eff=%s smtSib=%s parked=%s offline=%s eligible=%s cpuSet=%s l2=%s l3=%s mod=%s",
            p.lpIndex, p.group, p.coreIndex, p.llcIndex, p.llcIndexInGroup,
            p.efficiencyClass, p.smtSibling, p.parkedAtDiscovery, p.offlineAtDiscovery,
            s.isEligible(p), p.cpuSetId,
            p.l2.sizeBytes, p.l3.sizeBytes, p.moduleIndex);
    }
    foreach (ref d; s.llcDomains)
        writefln("  domain %s source=%s l3=%s cpus=%s pCores=%s eCores=%s",
            d.llcIndex, d.source, d.l3SizeBytes, formatCpuList(d.processors),
            d.pCoreIndices, d.eCoreIndices);
}

bool looksLikeSisyphus(const(TopologySnapshot) s)
{
    return s.logicalProcessorCount == 20
        && s.llcCount == 1
        && s.pCoreCount == 6
        && s.eCoreCount == 8
        && s.classCount == 2;
}

/// Start a spinning pool with `opt`, let it run, and return the LPs its body
/// calls were seen on as a mask.
ulong seenUnder(PoolOptions opt, uint expectedWorkers, WorkerPlacement expectedPlacement)
{
    atomicStore(gSeenLps, 0UL);
    atomicStore(gBodyTicks, 0);
    opt.workerBody = &tickBody;
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown();
    assert(pool.workerCount == expectedWorkers, pool.resolution.summary());
    assert(pool.resolution.placement == expectedPlacement, pool.resolution.summary());
    auto d = pool.director();
    d.spin();
    Thread.sleep(msecs(300));
    assert(atomicLoad(gBodyTicks) > 0, "no worker body ran");
    return atomicLoad(gSeenLps);
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
        assert(p.llcIndex == (p.offlineAtDiscovery ? noLlcIndex : 0));
        assert(p.llcIndexInGroup == (p.offlineAtDiscovery ? noLlcIndex : 0));
        if (p.lpIndex <= 11)
            assert(p.efficiencyClass == 1);
        else
            assert(p.efficiencyClass == 0);
    }

    // The default pool: one set of all 20 LPs, two left without a worker,
    // unconfined since the process reaches one group.
    PoolOptions liveOpt;
    liveOpt.workerBody = &liveBody;
    auto livePool = new CacheAwarePool(liveOpt);
    auto res = livePool.resolve();
    assert(res.placement == WorkerPlacement.free && res.sets.length == 1);
    assert(res.sets[0].processors.length == 20 && res.workerCount == 18 && res.sets[0].unstaffed == 2);
    auto c1s = makeBins!LiveC1(res, (ref const g) => LiveC1.init);
    auto c2s = makeBins!LiveC2(res, (ref const g) => LiveC2.init);
    auto bags = makeBins!WorkBag(res, (ref const g) => WorkBag.init);
    bags[0].left = 1;
    install(c1s, res);
    install(c2s, res);
    install(bags, res);
    setLabel!LiveC1(cast(ushort) 0, "llc0");
    scope (exit)
    {
        uninstall!LiveC1();
        uninstall!LiveC2();
        uninstall!WorkBag();
    }

    {
        auto pool = livePool;
        pool.start();
        scope (exit) pool.shutdown();
        assert(pool.workerCount == 18);
        auto d = pool.director();
        d.spin();
        foreach (_; 0 .. 200)
        {
            if (ranOn != uint.max)
                break;
            Thread.sleep(msecs(5));
        }
    }
    writefln("the job ran on LP %s", ranOn);
    assert(ranOn != uint.max, "the job did not run");
    assert(binsFound, "a worker finds its group's bins and the labelled one");
    assert(homeOf == 0, "SISYPHUS workers have LLC 0 as home");
    assert(numaCount == 1 && foreign == 0, "one NUMA, one LLC: no foreign bins");

    {
        atomicStore(gBodyTicks, 0);
        PoolOptions opt;
        opt.workerBody = &tickBody;
        auto pool = new CacheAwarePool(opt);
        pool.start();
        pool.shutdown();
        pool.start();
        assert(pool.workerCount == 18);
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
        pool.shutdown();
        assert(atomicLoad(gBodyTicks) > 0, "restarted pool did not run the worker body");
        writeln("restart ok");
    }

    // A class filter. Twelve P LPs leave one unstaffed; eight E LPs, one.
    enum ulong pMask = 0xFFF, eMask = 0xFF000;
    {
        PoolOptions pOnly;
        pOnly.enableECores = false;

        // A group mask and a pin enforce the filter.
        pOnly.placement = WorkerPlacement.group;
        auto seen = seenUnder(pOnly, 11, WorkerPlacement.group);
        assert((seen & ~pMask) == 0, "a P-only group mask let a worker onto an E-core");
        pOnly.placement = WorkerPlacement.pin;
        seen = seenUnder(pOnly, 11, WorkerPlacement.pin);
        assert(seen == 0x7FF, "eleven pinned workers take the set's first eleven LPs");

        // Unconfined, it sets the count and nothing else.
        pOnly.placement = WorkerPlacement.automatic;
        seen = seenUnder(pOnly, 11, WorkerPlacement.free);
        writefln("P-only, unconfined: 11 workers seen on LPs %b (the filter sets the count only)", seen);

        PoolOptions eOnly;
        eOnly.enablePCores = false;
        eOnly.placement = WorkerPlacement.group;
        seen = seenUnder(eOnly, 7, WorkerPlacement.group);
        assert(seen != 0 && (seen & ~eMask) == 0, "an E-only group mask let a worker onto a P-core");
        writeln("class filters: enforced by a group mask and a pin");
    }

    writeln("live-sisyphus: pass");
    return 0;
}
