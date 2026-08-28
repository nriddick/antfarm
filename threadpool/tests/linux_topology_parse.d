module linux_topology_parse;

version (linux):

import core.atomic;
import core.thread;
import core.time : msecs;
import std.algorithm : sort;
import std.file : mkdirRecurse, rmdirRecurse, tempDir, write;
import std.format : format;
import std.path : buildPath, dirName;
import std.process : thisProcessID;

import threadpool;
import threadpool.hybrid : WorkerSelf, bindNumaNeighborhood;
import threadpool.sys.linux_topology;
import threadpool.topology;

unittest
{
    assert(parseLinuxCpuList("").length == 0);
    assert(parseLinuxCpuList("0") == [ushort(0)]);
    assert(parseLinuxCpuList("0-3") == [ushort(0), 1, 2, 3]);
    assert(parseLinuxCpuList("0,6") == [ushort(0), 6]);
    assert(parseLinuxCpuList("0-3,8-11") == [ushort(0), 1, 2, 3, 8, 9, 10, 11]);
    assert(parseLinuxCpuList(" 4-4, 1 ") == [ushort(4), 1]);
    assert(parseLinuxCacheSize("32K") == 32 * 1024);
    assert(parseLinuxCacheSize("16384K") == 16 * 1024 * 1024);
    assert(parseLinuxCacheSize("16M") == 16 * 1024 * 1024);
    assert(parseLinuxCacheSize("64") == 64);
}

private struct TestCache
{
    ubyte level;
    string type;
    string size;
    ushort line = 64;
    ushort ways = 8;
    string share;
    ushort id;
}

private struct TestCpu
{
    ushort cpu;
    ushort core;
    ushort pkg;
    string siblings;
    uint capacity = 1024;
    bool online = true;
    bool omitOnlineFile = false;
    ushort numa;
    TestCache[] caches;
}

private uint gTreeSeq;

private string writeTree(TestCpu[] cpus, string present, string online,
    string[ushort] nodeLists, string coreList = null, string atomList = null)
{
    auto root = buildPath(tempDir, format("tp-sysfs-%s-%s", thisProcessID, ++gTreeSeq));
    auto cpuRoot = buildPath(root, "cpu");
    auto nodeRoot = buildPath(root, "node");
    mkdirRecurse(cpuRoot);
    mkdirRecurse(nodeRoot);
    writeFile(buildPath(cpuRoot, "present"), present);
    writeFile(buildPath(cpuRoot, "online"), online);

    ushort[] nodeIds;
    foreach (n; nodeLists.byKey)
        nodeIds ~= n;
    nodeIds.sort();
    string nodeOnline;
    foreach (i, n; nodeIds)
    {
        if (i) nodeOnline ~= ",";
        nodeOnline ~= toString(n);
        writeFile(buildPath(nodeRoot, "node" ~ toString(n), "cpulist"), nodeLists[n]);
    }
    writeFile(buildPath(nodeRoot, "online"), nodeOnline);

    foreach (c; cpus)
    {
        auto dir = buildPath(cpuRoot, "cpu" ~ toString(c.cpu));
        if (!c.omitOnlineFile)
            writeFile(buildPath(dir, "online"), c.online ? "1" : "0");
        writeFile(buildPath(dir, "cpu_capacity"), toString(c.capacity));
        writeFile(buildPath(dir, "topology", "core_id"), toString(c.core));
        writeFile(buildPath(dir, "topology", "physical_package_id"), toString(c.pkg));
        writeFile(buildPath(dir, "topology", "thread_siblings_list"), c.siblings);
        foreach (i, cache; c.caches)
        {
            auto idx = buildPath(dir, "cache", "index" ~ toString(cast(uint) i));
            writeFile(buildPath(idx, "level"), toString(cache.level));
            writeFile(buildPath(idx, "type"), cache.type);
            writeFile(buildPath(idx, "size"), cache.size);
            writeFile(buildPath(idx, "coherency_line_size"), toString(cache.line));
            writeFile(buildPath(idx, "ways_of_associativity"), toString(cache.ways));
            writeFile(buildPath(idx, "shared_cpu_list"), cache.share);
            writeFile(buildPath(idx, "id"), toString(cache.id));
        }
    }

    string corePath, atomPath;
    if (coreList !is null)
    {
        corePath = buildPath(root, "cpu_core", "cpus");
        writeFile(corePath, coreList);
    }
    if (atomList !is null)
    {
        atomPath = buildPath(root, "cpu_atom", "cpus");
        writeFile(atomPath, atomList);
    }
    // Stash extra roots on the cpu present file's parent via side files.
    writeFile(buildPath(root, ".core"), corePath is null ? "" : corePath);
    writeFile(buildPath(root, ".atom"), atomPath is null ? "" : atomPath);
    return root;
}

private void writeFile(string path, string content)
{
    mkdirRecurse(dirName(path));
    write(path, content ~ "\n");
}

private string toString(ulong v)
{
    return format("%s", v);
}

private TestCache[] zen3Caches(string l2Share, ushort l2Id, string l3Share)
{
    return [
        TestCache(1, "Data", "32K", 64, 8, l2Share, l2Id),
        TestCache(1, "Instruction", "32K", 64, 8, l2Share, l2Id),
        TestCache(2, "Unified", "512K", 64, 8, l2Share, l2Id),
        TestCache(3, "Unified", "16384K", 64, 16, l3Share, 0),
    ];
}

private TestCpu[] ryzen5500Cpus()
{
    TestCpu[] cpus;
    // Linux numbers SMT as (0,6)(1,7)… not adjacent pairs.
    foreach (core; 0 .. 6)
    {
        auto a = cast(ushort) core;
        auto b = cast(ushort)(core + 6);
        auto sibs = format("%s,%s", a, b);
        auto cachesA = zen3Caches(sibs, cast(ushort) core, "0-11");
        auto cachesB = zen3Caches(sibs, cast(ushort) core, "0-11");
        TestCpu ca;
        ca.cpu = a;
        ca.core = cast(ushort) core;
        ca.siblings = sibs;
        ca.omitOnlineFile = (a == 0);
        ca.caches = cachesA;
        cpus ~= ca;
        TestCpu cb;
        cb.cpu = b;
        cb.core = cast(ushort) core;
        cb.siblings = sibs;
        cb.caches = cachesB;
        cpus ~= cb;
    }
    return cpus;
}

unittest
{
    auto root = writeTree(ryzen5500Cpus(), "0-11", "0-11", [ushort(0): "0-11"]);
    scope (exit) rmdirRecurse(root);
    auto snap = parseSysfsTopology(buildPath(root, "cpu"), buildPath(root, "node"));

    assert(snap.os == "linux");
    assert(snap.llcCount == 1);
    assert(snap.logicalProcessorCount == 12);
    assert(snap.pCoreCount == 6);
    assert(snap.eCoreCount == 0);
    assert(snap.classCount == 1);
    assert(snap.maxEfficiencyClass == 0);
    assert(snap.cacheLineSize == 64);
    assert(snap.numaNodes.length == 1);
    assert(snap.llcDomains[0].l3SizeBytes == 16 * 1024 * 1024);
    assert(snap.llcDomains[0].lineSize == 64);
    assert(snap.llcDomains[0].lpIndices.length == 12);
    assert(snap.llcDomains[0].eCoreIndices.length == 0);
    assert(snap.l2Clusters.length == 6);

    bool[ushort] sawPrimary, sawSib;
    foreach (ref p; snap.processors)
    {
        assert(p.group == 0);
        assert(p.llcIndex == 0);
        assert(p.numaIndex == 0);
        assert(p.efficiencyClass == 0);
        assert(p.l1d.sizeBytes == 32 * 1024);
        assert(p.l2.sizeBytes == 512 * 1024);
        assert(p.l3.sizeBytes == 16 * 1024 * 1024);
        assert(!p.parkedAtDiscovery);
        if (p.lpIndex < 6)
        {
            assert(!p.smtSibling, "lowest sibling is the primary");
            assert(p.coreIndex == p.lpIndex);
            sawPrimary[p.lpIndex] = true;
        }
        else
        {
            assert(p.smtSibling);
            assert(p.coreIndex == p.lpIndex - 6);
            sawSib[p.lpIndex] = true;
        }
    }
    assert(sawPrimary.length == 6);
    assert(sawSib.length == 6);

    auto homeLlcs = snap.llcsInNuma(0);
    assert(homeLlcs == [ushort(0)]);
    ushort[maxNumaLlcs] buf;
    assert(fillNumaLlcs(0, 0, snap, buf[]) == 1);
}

unittest
{
    // Two CCDs, one NUMA: the LLC axis the laptop Windows dump cannot cover.
    TestCpu[] cpus;
    foreach (ccd; 0 .. 2)
    {
        foreach (core; 0 .. 2)
        {
            auto id = cast(ushort)(ccd * 4 + core * 2);
            auto sib = cast(ushort)(id + 1);
            auto shareL2 = format("%s,%s", id, sib);
            auto shareL3 = ccd == 0 ? "0-3" : "4-7";
            TestCache[] caches = [
                TestCache(1, "Data", "32K", 64, 8, shareL2, id),
                TestCache(1, "Instruction", "32K", 64, 8, shareL2, id),
                TestCache(2, "Unified", "512K", 64, 8, shareL2, id),
                TestCache(3, "Unified", "32768K", 64, 16, shareL3, cast(ushort) ccd),
            ];
            TestCpu a, b;
            a.cpu = id;
            a.core = cast(ushort)(ccd * 2 + core);
            a.siblings = shareL2;
            a.caches = caches;
            b.cpu = sib;
            b.core = a.core;
            b.siblings = shareL2;
            b.caches = caches;
            cpus ~= a;
            cpus ~= b;
        }
    }
    auto root = writeTree(cpus, "0-7", "0-7", [ushort(0): "0-7"]);
    scope (exit) rmdirRecurse(root);
    auto snap = parseSysfsTopology(buildPath(root, "cpu"), buildPath(root, "node"));
    assert(snap.llcCount == 2);
    assert(snap.logicalProcessorCount == 8);
    assert(snap.classCount == 1);
    assert(snap.numaNodes.length == 1);

    ushort[] llcA, llcB;
    foreach (ref p; snap.processors)
    {
        if (p.lpIndex < 4) llcA ~= p.llcIndex;
        else llcB ~= p.llcIndex;
        assert(p.llcIndexInGroup == (p.lpIndex < 4 ? 0 : 1));
    }
    foreach (v; llcA) assert(v == llcA[0]);
    foreach (v; llcB) assert(v == llcB[0]);
    assert(llcA[0] != llcB[0], "two L3 sharing sets must not alias");

    auto numaLlcs = snap.llcsInNuma(0);
    numaLlcs.sort();
    assert(numaLlcs.length == 2);
    assert(snap.llcBelongsToNuma(llcA[0], 0));
    assert(snap.llcBelongsToNuma(llcB[0], 0));

    ushort[maxNumaLlcs] buf;
    auto n = fillNumaLlcs(llcA[0], 0, snap, buf[]);
    assert(n == 2);
    assert(buf[0] == llcA[0]);
    assert(buf[1] == llcB[0]);

    WorkerSelf w;
    w.llcIndex = llcA[0];
    w.numaIndex = 0;
    bindNumaNeighborhood(w, snap);
    assert(w.homeLlc == llcA[0]);
    assert(w.llcIsNumaLocal(llcB[0]));
    assert(!w.llcIsNumaLocal(99));
}

unittest
{
    // Hybrid + two NUMA: P on node 0, E on node 1, distinct L3s.
    TestCpu[] cpus;
    foreach (i; 0 .. 4)
    {
        auto cpu = cast(ushort) i;
        auto sib = cast(ushort)(i ^ 1);
        if (i % 2 == 1) sib = cast(ushort)(i - 1);
        else sib = cast(ushort)(i + 1);
        auto shareL2 = format("%s,%s", cpu < sib ? cpu : sib, cpu < sib ? sib : cpu);
        TestCpu c;
        c.cpu = cpu;
        c.core = cast(ushort)(i / 2);
        c.siblings = shareL2;
        c.capacity = 1024;
        c.numa = 0;
        c.caches = [
            TestCache(1, "Data", "48K", 64, 12, shareL2, c.core),
            TestCache(1, "Instruction", "32K", 64, 8, shareL2, c.core),
            TestCache(2, "Unified", "1280K", 64, 10, shareL2, c.core),
            TestCache(3, "Unified", "24576K", 64, 12, "0-3", 0),
        ];
        cpus ~= c;
    }
    foreach (i; 0 .. 4)
    {
        auto cpu = cast(ushort)(4 + i);
        TestCpu c;
        c.cpu = cpu;
        c.core = cpu;
        c.siblings = toString(cpu);
        c.capacity = 400;
        c.numa = 1;
        c.caches = [
            TestCache(1, "Data", "32K", 64, 8, toString(cpu), cpu),
            TestCache(1, "Instruction", "64K", 64, 8, toString(cpu), cpu),
            TestCache(2, "Unified", "2048K", 64, 16, format("%s-%s", 4 + (i / 2) * 2, 5 + (i / 2) * 2), cpu),
            TestCache(3, "Unified", "4096K", 64, 8, "4-7", 1),
        ];
        cpus ~= c;
    }
    auto root = writeTree(cpus, "0-7", "0-7",
        [ushort(0): "0-3", ushort(1): "4-7"],
        "0-3", "4-7");
    scope (exit) rmdirRecurse(root);

    string corePath, atomPath;
    {
        import std.file : readText;
        import std.string : strip;
        corePath = readText(buildPath(root, ".core")).strip;
        atomPath = readText(buildPath(root, ".atom")).strip;
    }
    auto snap = parseSysfsTopology(buildPath(root, "cpu"), buildPath(root, "node"),
        corePath, atomPath);
    assert(snap.llcCount == 2);
    assert(snap.classCount == 2);
    assert(snap.maxEfficiencyClass == 1);
    assert(snap.pCoreCount == 2);
    assert(snap.eCoreCount == 4);
    assert(snap.numaNodes.length == 2);

    foreach (ref p; snap.processors)
    {
        if (p.lpIndex < 4)
        {
            assert(p.efficiencyClass == 1);
            assert(p.numaIndex == 0);
        }
        else
        {
            assert(p.efficiencyClass == 0);
            assert(p.numaIndex == 1);
            assert(!p.smtSibling);
        }
    }

    auto pLps = snap.pLps();
    auto eLps = snap.eLps();
    pLps.sort();
    eLps.sort();
    assert(pLps == [ushort(0), 1, 2, 3]);
    assert(eLps == [ushort(4), 5, 6, 7]);
}

unittest
{
    // If only cpu_atom is published, unlisted CPUs should still be classified
    // as P rather than being incorrectly defaulted to E.
    TestCpu[] cpus;
    foreach (i; 0 .. 8)
    {
        TestCpu c;
        c.cpu = cast(ushort) i;
        c.core = cast(ushort) i;
        c.siblings = toString(i);
        c.capacity = i < 4 ? 400 : 1024;
        c.numa = 0;
        c.caches = [
            TestCache(1, "Data", "32K", 64, 8, toString(i), cast(ushort) i),
            TestCache(1, "Instruction", "32K", 64, 8, toString(i), cast(ushort) i),
            TestCache(2, "Unified", "512K", 64, 8, toString(i), cast(ushort) i),
            TestCache(3, "Unified", "16384K", 64, 16, "0-7", 0),
        ];
        cpus ~= c;
    }
    auto root = writeTree(cpus, "0-7", "0-7",
        [ushort(0): "0-7"], null, "0-3");
    scope (exit) rmdirRecurse(root);

    string atomPath;
    {
        import std.file : readText;
        import std.string : strip;
        atomPath = readText(buildPath(root, ".atom")).strip;
    }
    auto snap = parseSysfsTopology(buildPath(root, "cpu"), buildPath(root, "node"),
        null, atomPath);
    assert(snap.classCount == 2);
    assert(snap.maxEfficiencyClass == 1);
    assert(snap.pCoreCount == 4);
    assert(snap.eCoreCount == 4);
    foreach (ref p; snap.processors)
    {
        if (p.lpIndex < 4)
            assert(p.efficiencyClass == 0);
        else
            assert(p.efficiencyClass == 1);
    }
}

unittest
{
    // Live host. Loose contract for any Linux box; tight if this is the 5500.
    auto snap = parseSysfsTopology("/sys/devices/system/cpu", "/sys/devices/system/node");
    assert(snap.os == "linux");
    assert(snap.logicalProcessorCount >= 1);
    assert(snap.llcCount >= 1);
    assert(snap.cacheLineSize >= 32);
    assert(snap.processors.length == snap.logicalProcessorCount);
    assert(snap.llcDomains.length == snap.llcCount);
    foreach (ref p; snap.processors)
        assert(p.llcIndex < snap.llcCount);

    auto live = discover();
    assert(live.os == "linux");
    assert(live.logicalProcessorCount == snap.logicalProcessorCount);
    auto here = live.current();
    assert(here !is null, "sched_getcpu must resolve to a snapshot LP");
    assert(here.llcIndex < live.llcCount);

    if (looksLikeRyzen5500(live))
    {
        assert(live.llcCount == 1);
        assert(live.logicalProcessorCount == 12);
        assert(live.pCoreCount == 6);
        assert(live.eCoreCount == 0);
        assert(live.classCount == 1);
        assert(live.llcDomains[0].l3SizeBytes == 16 * 1024 * 1024);
        ushort[6] primaries = [0, 1, 2, 3, 4, 5];
        ushort[6] siblings = [6, 7, 8, 9, 10, 11];
        foreach (i, lp; primaries)
        {
            auto p = live.byLp(0, lp);
            assert(p !is null);
            assert(!p.smtSibling);
            assert(p.coreIndex == i);
            auto s = live.byLp(0, siblings[i]);
            assert(s !is null);
            assert(s.smtSibling);
            assert(s.coreIndex == i);
        }
    }
}

private bool looksLikeRyzen5500(ref const TopologySnapshot snap)
{
    import std.file : exists, readText;
    import std.string : indexOf;

    if (!exists("/proc/cpuinfo")) return false;
    try
    {
        auto info = readText("/proc/cpuinfo");
        return info.indexOf("Ryzen 5 5500") >= 0
            && snap.logicalProcessorCount == 12;
    }
    catch (Exception)
        return false;
}

shared int gLiveJobRan;

extern (D) bool liveBody(WorkerSelf*) @nogc nothrow
{
    atomicStore(gLiveJobRan, 1);
    return false;
}

unittest
{
    atomicStore(gLiveJobRan, 0);
    PoolOptions opt;
    opt.workerBody = &liveBody;
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown(true);
    assert(pool.workerCount > 0);
    foreach (_; 0 .. 400)
    {
        if (atomicLoad(gLiveJobRan) == 1) break;
        Thread.sleep(msecs(5));
    }
    assert(atomicLoad(gLiveJobRan) == 1, "pinned Linux worker did not run the worker body");

    if (auto w = currentWorker())
        assert(false, "currentWorker is TLS of a pool thread, not the tester");
    auto here = CacheAwarePool.topology().current();
    assert(here !is null);
}
