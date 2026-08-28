module topology_parse;

import std.algorithm : sort;
import threadpool.hybrid : WorkerSelf, bindNumaNeighborhood;
import threadpool.topology;

version (Windows)
{
import threadpool.sys.win_topology : parseWindowsTopology;

immutable cpuSetsBlob = cast(immutable(ubyte)[]) import("captured/sisyphus_cpusets.bin");
immutable slpiexBlob  = cast(immutable(ubyte)[]) import("captured/sisyphus_slpiex.bin");
}

version (Windows):

private void putU8(ref ubyte[] b, ubyte v) { b ~= v; }

private void putU16(ref ubyte[] b, ushort v)
{
    b ~= cast(ubyte) v;
    b ~= cast(ubyte)(v >> 8);
}

private void putU32(ref ubyte[] b, uint v)
{
    b ~= cast(ubyte) v;
    b ~= cast(ubyte)(v >> 8);
    b ~= cast(ubyte)(v >> 16);
    b ~= cast(ubyte)(v >> 24);
}

private void putU64(ref ubyte[] b, ulong v)
{
    foreach (i; 0 .. 8)
        b ~= cast(ubyte)(v >> (8 * i));
}

private void putZeros(ref ubyte[] b, size_t n)
{
    foreach (i; 0 .. n)
        b ~= 0;
}

/// Hand-built two-group / two-LLC fixture. Both groups report LastLevelCacheIndex==0.
ubyte[] makeTwoGroupLlcCpuSets()
{
    ubyte[] b;
    foreach (g; 0 .. 2)
    {
        foreach (lp; 0 .. 4)
        {
            putU32(b, 32);
            putU32(b, 0);
            putU32(b, 256 + g * 256 + lp);
            putU16(b, cast(ushort) g);
            putU8(b, cast(ubyte) lp);
            putU8(b, cast(ubyte) lp); // core
            putU8(b, 0);              // LastLevelCacheIndex — same in both groups
            putU8(b, 0);
            putU8(b, 1);              // efficiency class
            putU8(b, 0);
            putU32(b, 0);
            putU64(b, 0);
        }
    }
    return b;
}

ubyte[] makeTwoGroupLlcSlpiex()
{
    ubyte[] b;
    foreach (g; 0 .. 2)
    {
        putU32(b, 2);  // RelationCache
        putU32(b, 56);
        putU8(b, 3);   // L3
        putU8(b, 12);
        putU16(b, 64);
        putU32(b, 24 * 1024 * 1024);
        putU32(b, 0);  // unified
        putZeros(b, 18);
        putU16(b, 1);  // GroupCount
        putU64(b, 0xF); // mask of 4 LPs
        putU16(b, cast(ushort) g);
        putU16(b, 0);
        putU16(b, 0);
        putU16(b, 0);
    }
    return b;
}

private void putCpuSet(ref ubyte[] b, ushort group, ubyte lp, ubyte core, ubyte llc,
    ubyte numa, ubyte eff, ubyte flags = 0)
{
    putU32(b, 32);
    putU32(b, 0);
    putU32(b, 256 + group * 256 + lp);
    putU16(b, group);
    putU8(b, lp);
    putU8(b, core);
    putU8(b, llc);
    putU8(b, numa);
    putU8(b, eff);
    putU8(b, flags);
    putU32(b, 0);
    putU64(b, 0);
}

/// RelationCache. `oldLayout` is pre-19041: Reserved[20], GroupCount reads as 0.
private void putCacheRecord(ref ubyte[] b, ubyte level, ulong mask, ushort group,
    uint sizeBytes, bool oldLayout = false)
{
    putU32(b, 2);
    putU32(b, 56);
    putU8(b, level);
    putU8(b, 12);
    putU16(b, 64);
    putU32(b, sizeBytes);
    putU32(b, 0);
    if (oldLayout)
        putZeros(b, 20);
    else
    {
        putZeros(b, 18);
        putU16(b, 1);
    }
    putU64(b, mask);
    putU16(b, group);
    putU16(b, 0);
    putU16(b, 0);
    putU16(b, 0);
}

private void putPackageRecord(ref ubyte[] b, ulong mask, ushort group)
{
    putU32(b, 3); // RelationProcessorPackage
    putU32(b, 48);
    putU8(b, 0);
    putU8(b, 0);
    putZeros(b, 20);
    putU16(b, 1);
    putU64(b, mask);
    putU16(b, group);
    putU16(b, 0);
    putU16(b, 0);
    putU16(b, 0);
}

unittest
{
    auto snap = parseWindowsTopology(cpuSetsBlob, slpiexBlob);
    assert(snap.llcCount == 1);
    assert(snap.logicalProcessorCount == 20);
    assert(snap.pCoreCount == 6);
    assert(snap.eCoreCount == 8);
    assert(snap.cacheLineSize == 64);
    assert(snap.classCount == 2);
    assert(snap.maxEfficiencyClass == 1);

    uint smtP, eCores;
    foreach (ref p; snap.processors)
    {
        assert(p.llcIndex == 0);
        assert(p.llcIndexInGroup == 0);
        if (p.lpIndex <= 11)
        {
            assert(p.efficiencyClass == 1);
            if (p.lpIndex % 2 == 1)
                assert(p.smtSibling);
            else
                assert(!p.smtSibling);
            smtP++;
        }
        else
        {
            assert(p.efficiencyClass == 0);
            assert(!p.smtSibling);
            eCores++;
        }
        if (p.l3.sizeBytes)
            assert(p.l3.sizeBytes == 24 * 1024 * 1024);
    }
    assert(smtP == 12);
    assert(eCores == 8);

    ushort[][ushort] byModule;
    foreach (ref p; snap.processors)
    {
        if (p.lpIndex >= 12)
            byModule[p.moduleIndex] ~= p.lpIndex;
    }
    assert(byModule.length == 2);
    bool sawA, sawB;
    foreach (mod, lps; byModule)
    {
        lps.sort();
        if (lps == [ushort(12), 13, 14, 15]) sawA = true;
        if (lps == [ushort(16), 17, 18, 19]) sawB = true;
    }
    assert(sawA && sawB);

    assert(snap.packages.length == 1);
    foreach (ref p; snap.processors)
        assert(p.packageIndex == 0);

    assert(snap.l2Clusters.length >= 2);
    bool clusterA, clusterB;
    foreach (ref c; snap.l2Clusters)
    {
        if (c.lpIndices == [ushort(12), 13, 14, 15])
        {
            clusterA = true;
            assert(c.l2SizeBytes == 2048 * 1024);
        }
        if (c.lpIndices == [ushort(16), 17, 18, 19])
        {
            clusterB = true;
            assert(c.l2SizeBytes == 2048 * 1024);
        }
    }
    assert(clusterA && clusterB);

    auto homeLlcs = snap.llcsInNuma(0);
    assert(homeLlcs.length == 1);
    assert(homeLlcs[0] == 0);
    assert(snap.llcBelongsToNuma(0, 0));
    assert(!snap.llcBelongsToNuma(1, 0));

    ushort[maxNumaLlcs] buf;
    auto n = fillNumaLlcs(0, 0, snap, buf[]);
    assert(n == 1);
    assert(buf[0] == 0);
}

unittest
{
    auto cpu = makeTwoGroupLlcCpuSets();
    auto slp = makeTwoGroupLlcSlpiex();
    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.llcCount == 2);
    assert(snap.logicalProcessorCount == 8);
    assert(snap.groupCount >= 1);

    ushort[] llcG0, llcG1;
    foreach (ref p; snap.processors)
    {
        assert(p.llcIndexInGroup == 0);
        if (p.group == 0) llcG0 ~= p.llcIndex;
        if (p.group == 1) llcG1 ~= p.llcIndex;
    }
    assert(llcG0.length == 4);
    assert(llcG1.length == 4);
    foreach (v; llcG0)
        assert(v == llcG0[0]);
    foreach (v; llcG1)
        assert(v == llcG1[0]);
    assert(llcG0[0] != llcG1[0], "group-relative LastLevelCacheIndex must not alias two L3s");

    auto numaLlcs = snap.llcsInNuma(0);
    numaLlcs.sort();
    assert(numaLlcs.length == 2);
    assert(numaLlcs[0] != numaLlcs[1]);
    assert(snap.llcBelongsToNuma(llcG0[0], 0));
    assert(snap.llcBelongsToNuma(llcG1[0], 0));

    ushort[maxNumaLlcs] buf;
    auto n = fillNumaLlcs(llcG0[0], 0, snap, buf[]);
    assert(n == 2);
    assert(buf[0] == llcG0[0], "home LLC is first");
    assert(buf[1] == llcG1[0], "foreign LLC in the same NUMA follows");

    WorkerSelf w;
    w.llcIndex = llcG0[0];
    w.numaIndex = 0;
    w.classIndex = 0;
    bindNumaNeighborhood(w, snap);
    assert(w.homeLlc == llcG0[0]);
    assert(w.llcIsNumaLocal(llcG1[0]));
    assert(!w.llcIsNumaLocal(99));
}

unittest
{
    ubyte[] cpu;
    foreach (lp; 0 .. 8)
        putCpuSet(cpu, 0, cast(ubyte) lp, cast(ubyte) lp, 0, 0, 1);
    ubyte[] slp;
    putCacheRecord(slp, 3, 0x0F, 0, 16 * 1024 * 1024);
    putCacheRecord(slp, 3, 0xF0, 0, 16 * 1024 * 1024);
    putPackageRecord(slp, 0x0F, 0);
    putPackageRecord(slp, 0xF0, 0);

    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.packages.length == 2);
    assert(snap.llcCount == 2);
    foreach (ref p; snap.processors)
    {
        if (p.lpIndex < 4)
            assert(p.packageIndex == 0);
        else
            assert(p.packageIndex == 1);
    }
    auto llcA = snap.processors[0].llcIndex;
    auto llcB = snap.processors[4].llcIndex;
    assert(llcA != llcB);
    foreach (ref p; snap.processors)
        assert(p.llcIndex == (p.lpIndex < 4 ? llcA : llcB));
}

unittest
{
    ubyte[] cpu;
    foreach (lp; 0 .. 4)
        putCpuSet(cpu, 0, cast(ubyte) lp, cast(ubyte) lp, lp < 2 ? 0 : 1, 0, 1);
    auto snap = parseWindowsTopology(cpu, null);
    assert(snap.llcCount == 2);
    assert(snap.processors[0].llcIndex == snap.processors[1].llcIndex);
    assert(snap.processors[2].llcIndex == snap.processors[3].llcIndex);
    assert(snap.processors[0].llcIndex != snap.processors[2].llcIndex);
}

unittest
{
    ubyte[] cpu;
    foreach (lp; 0 .. 4)
        putCpuSet(cpu, 0, cast(ubyte) lp, cast(ubyte) lp, 0, 0, 1);
    ubyte[] slp;
    putCacheRecord(slp, 3, 0x0F, 0, 8 * 1024 * 1024);
    putCacheRecord(slp, 2, 0x03, 0, 256 * 1024);
    putCacheRecord(slp, 2, 0x0C, 0, 256 * 1024);

    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.l2Clusters.length == 2);
    bool sawA, sawB;
    foreach (ref c; snap.l2Clusters)
    {
        if (c.lpIndices == [ushort(0), 1]) sawA = true;
        if (c.lpIndices == [ushort(2), 3]) sawB = true;
        assert(c.l2SizeBytes == 256 * 1024);
    }
    assert(sawA && sawB);
    assert(snap.processors[0].moduleIndex == snap.processors[1].moduleIndex);
    assert(snap.processors[2].moduleIndex == snap.processors[3].moduleIndex);
    assert(snap.processors[0].moduleIndex != snap.processors[2].moduleIndex);
}

unittest
{
    ubyte[] cpu;
    putCpuSet(cpu, 0, 0, 0, 0, 0, 0);
    putCpuSet(cpu, 0, 1, 1, 0, 0, 1);
    putCpuSet(cpu, 0, 2, 2, 0, 0, 2);
    auto snap = parseWindowsTopology(cpu, null);
    assert(snap.maxEfficiencyClass == 2);
    assert(snap.classCount == 2);
    assert(tableClassIndex(2, snap.maxEfficiencyClass, snap.classCount) == classP);
    assert(tableClassIndex(1, snap.maxEfficiencyClass, snap.classCount) == classE);
    assert(tableClassIndex(0, snap.maxEfficiencyClass, snap.classCount) == classE);
}

unittest
{
    ubyte[] cpu;
    foreach (lp; 0 .. 4)
        putCpuSet(cpu, 0, cast(ubyte) lp, cast(ubyte) lp, 0, 0, 1);
    ubyte[] slp;
    putCacheRecord(slp, 3, 0x0F, 0, 24 * 1024 * 1024, true);

    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.llcCount == 1);
    assert(snap.cacheLineSize == 64);
    foreach (ref p; snap.processors)
    {
        assert(p.llcIndex == 0);
        assert(p.l3.sizeBytes == 24 * 1024 * 1024);
    }
}

unittest
{
    ubyte[] cpu;
    putCpuSet(cpu, 0, 0, 0, 0, 0, 1, 0);
    putCpuSet(cpu, 0, 1, 1, 0, 0, 1, 0x1);
    auto snap = parseWindowsTopology(cpu, null);
    assert(!snap.processors[0].parkedAtDiscovery);
    assert(snap.processors[1].parkedAtDiscovery);
}
