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
    // Both groups belong to NUMA node 0 (their CPU sets both say 0, but that
    // index is group-relative; only this record makes it one node).
    putNumaRecord(b, 0, [GroupMask(0, 0xF), GroupMask(1, 0xF)]);
    return b;
}

private struct GroupMask
{
    ushort group;
    ulong mask;
}

private void putGroupMask(ref ubyte[] b, GroupMask m)
{
    putU64(b, m.mask);
    putU16(b, m.group);
    putZeros(b, 6);
}

/// RelationNumaNode. `oldLayout` is the single-mask form (Reserved[20],
/// GroupCount reads as 0) returned before Windows 11 / Server 2022.
private void putNumaRecord(ref ubyte[] b, uint node, const(GroupMask)[] masks,
    bool oldLayout = false)
{
    assert(!oldLayout || masks.length == 1);
    putU32(b, 1);
    putU32(b, cast(uint)(8 + 24 + 16 * masks.length));
    putU32(b, node);
    if (oldLayout)
        putZeros(b, 20);
    else
    {
        putZeros(b, 18);
        putU16(b, cast(ushort) masks.length);
    }
    foreach (m; masks)
        putGroupMask(b, m);
}

/// RelationProcessorPackage spanning several groups.
private void putPackageRecordGroups(ref ubyte[] b, const(GroupMask)[] masks)
{
    putU32(b, 3);
    putU32(b, cast(uint)(8 + 24 + 16 * masks.length));
    putU8(b, 0);
    putU8(b, 0);
    putZeros(b, 20);
    putU16(b, cast(ushort) masks.length);
    foreach (m; masks)
        putGroupMask(b, m);
}

/// RelationGroup with `count` full groups of 64.
private void putFullGroups(ref ubyte[] b, ushort count)
{
    putU32(b, 4);
    putU32(b, 32 + count * 48);
    putU16(b, count);
    putU16(b, count);
    putZeros(b, 20);
    foreach (_; 0 .. count)
    {
        putU8(b, 64);
        putU8(b, 64);
        putZeros(b, 38);
        putU64(b, ulong.max);
    }
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

/// Synthetic Windows buffers: two full groups, including LP 63 in each.
/// No discovery, pinning, or worker startup is performed by these checks.
void checkFullProcessorGroups()
{
    import threadpool.pool : PoolOptions;
    import threadpool.resolution : resolvePool, Reject;

    ubyte[] cpu, slp;
    foreach (g; 0 .. 2)
    {
        foreach (lp; 0 .. 64)
            putCpuSet(cpu, cast(ushort) g, cast(ubyte) lp,
                cast(ubyte)(lp / 2), 0, cast(ubyte) g, 1);
        putCacheRecord(slp, 3, ulong.max, cast(ushort) g, 32 * 1024 * 1024);
        putPackageRecord(slp, ulong.max, cast(ushort) g);
    }
    // RelationGroup with two PROCESSOR_GROUP_INFO records (48 bytes each).
    putU32(slp, 4);
    putU32(slp, 32 + 2 * 48);
    putU16(slp, 2);
    putU16(slp, 2);
    putZeros(slp, 20);
    foreach (_; 0 .. 2)
    {
        putU8(slp, 64);
        putU8(slp, 64);
        putZeros(slp, 38);
        putU64(slp, ulong.max);
    }

    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.logicalProcessorCount == 128);
    assert(snap.groupCount == 2 && snap.cores.length == 64);
    assert(snap.llcCount == 2 && snap.packages.length == 2);
    assertCoresSorted(snap);
    foreach (g; 0 .. 2)
    {
        assert(snap.groups[g].activeProcessors == 64);
        assert(snap.groups[g].activeMask == ulong.max);
        auto ids = snap.processorsInLlc(cast(ushort) g);
        assert(ids.length == 64);
        assert(ids[0] == ProcessorId(cast(ushort) g, 0));
        assert(ids[$ - 1] == ProcessorId(cast(ushort) g, 63));
        assert(snap.byId(ids[$ - 1]).l3.sizeBytes == 32 * 1024 * 1024);
        assert(snap.byId(ids[$ - 1]).smtSibling);
    }
    PoolOptions opt;
    auto res = resolvePool(snap, opt);
    assert(res.workers.length == 128 && res.acceptedGroupCount == 2);
    opt.skipSmtSiblings = true;
    res = resolvePool(snap, opt);
    assert(res.workers.length == 64);
    assert(res[ProcessorId(1, 63)].reasons == Reject.smtSecondary);

    auto restricted = parseWindowsTopology(cpu, slp,
        [affinityKey(0, 63), affinityKey(1, 0), affinityKey(1, 63)]);
    assert(restricted.logicalProcessorCount == 128);
    assert(restricted.eligibleProcessorsInLlc(0) == [ProcessorId(0, 63)]);
    assert(restricted.eligibleProcessorsInLlc(1) == [ProcessorId(1, 0), ProcessorId(1, 63)]);
    opt = PoolOptions.init;
    opt.processors = parseCpuList("0:63,1:63,1:63,0:0,2:0");
    res = resolvePool(restricted, opt);
    assert(res.workers == [ProcessorId(0, 63), ProcessorId(1, 63)]);
    assert(res.requests[2].duplicate && res.requests[2].accepted);
    assert(res[ProcessorId(0, 0)].reasons == Reject.osExcluded);
    assert(res.requests[4].unknown);
    assert(parseCpuList(formatCpuList(res.workers)) == res.workers);
}

/// An LLC is not necessarily confined to one Windows processor group.
void checkCacheAcrossProcessorGroups()
{
    ubyte[] cpu, slp;
    putCpuSet(cpu, 0, 63, 31, 0, 0, 1);
    putCpuSet(cpu, 1, 0, 0, 0, 0, 1);
    putU32(slp, 2); // RelationCache, two GROUP_AFFINITY entries.
    putU32(slp, 72);
    putU8(slp, 3);
    putU8(slp, 12);
    putU16(slp, 64);
    putU32(slp, 32 * 1024 * 1024);
    putU32(slp, 0);
    putZeros(slp, 18);
    putU16(slp, 2);
    foreach (g; 0 .. 2)
    {
        putU64(slp, g == 0 ? 1UL << 63 : 1UL);
        putU16(slp, cast(ushort) g);
        putZeros(slp, 6);
    }
    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.groupCount == 2 && snap.llcCount == 1);
    assert(snap.groupingSource == GroupingSource.l3);
    assert(snap.processorsInLlc(0) == [ProcessorId(0, 63), ProcessorId(1, 0)]);
    foreach (ref p; snap.processors)
        assert(p.l3.sizeBytes == 32 * 1024 * 1024 && p.llcIndex == 0);
}

/// How the simulated host reports NUMA in SLPIEx.
enum SimulatedNuma
{
    /// RelationAll on Windows 11 / Server 2022: one record per node with a
    /// mask for every group it spans.
    perNode,
    /// The single-mask layout, one record per (node, group), as on older
    /// Windows; the same NodeNumber appears in two records.
    singleMask,
    /// No NUMA records at all (a capture that omitted them).
    none,
}

/// A simulated large Windows host: two sockets, each with 64 SMT cores (128
/// logical processors), which Windows splits into four processor groups of
/// 64, two per socket. Each socket is one NUMA node (NPS1) spanning its two
/// groups; eight-core CCDs give each group four L3s. Every CPU-set field
/// Windows defines as relative to the processor group (LogicalProcessorIndex,
/// CoreIndex, LastLevelCacheIndex, NumaNodeIndex) repeats in each group, so
/// NumaNodeIndex is 0 on every processor and only the RelationNumaNode
/// records tell the sockets apart. No discovery, pinning or worker startup.
struct SimulatedHost
{
    uint sockets = 2;
    uint groupsPerSocket = 2;
    uint coresPerGroup = 32;
    uint coresPerLlc = 8;
    SimulatedNuma numa = SimulatedNuma.perNode;

    @property uint groups() const { return sockets * groupsPerSocket; }
    @property uint lpsPerGroup() const { return coresPerGroup * 2; }

    void build(out ubyte[] cpu, out ubyte[] slp) const
    {
        assert(lpsPerGroup <= 64 && coresPerGroup % coresPerLlc == 0);
        immutable ulong full = lpsPerGroup == 64 ? ulong.max : (1UL << lpsPerGroup) - 1;
        immutable lpsPerLlc = coresPerLlc * 2;
        foreach (g; 0 .. groups)
        {
            foreach (lp; 0 .. lpsPerGroup)
                putCpuSet(cpu, cast(ushort) g, cast(ubyte) lp, cast(ubyte)(lp / 2),
                    cast(ubyte)(lp / lpsPerLlc), 0, 1);
            foreach (llc; 0 .. coresPerGroup / coresPerLlc)
                putCacheRecord(slp, 3, ((1UL << lpsPerLlc) - 1) << (llc * lpsPerLlc),
                    cast(ushort) g, 32 * 1024 * 1024);
        }
        foreach (socket; 0 .. sockets)
        {
            GroupMask[] masks;
            foreach (h; 0 .. groupsPerSocket)
                masks ~= GroupMask(cast(ushort)(socket * groupsPerSocket + h), full);
            putPackageRecordGroups(slp, masks);
            final switch (numa)
            {
            case SimulatedNuma.perNode:
                putNumaRecord(slp, socket, masks);
                break;
            case SimulatedNuma.singleMask:
                foreach (m; masks)
                    putNumaRecord(slp, socket, [m], true);
                break;
            case SimulatedNuma.none:
                break;
            }
        }
        putFullGroups(slp, cast(ushort) groups);
    }
}

private ProcessorId[] groupProcessors(uint firstGroup, uint groupCount, uint lps)
{
    ProcessorId[] ids;
    foreach (g; firstGroup .. firstGroup + groupCount)
        foreach (lp; 0 .. lps)
            ids ~= ProcessorId(cast(ushort) g, cast(ushort) lp);
    return ids;
}

/// NUMA identity on the simulated host follows the socket under both record
/// layouts, so a node spanning two groups stays one neighbourhood and the
/// other socket's LLCs stay remote despite the shared local index 0. Without
/// records the parser cannot tell, and keeps every group's node separate.
void checkSimulatedFourGroupHost()
{
    import threadpool.pool : PoolOptions;
    import threadpool.resolution : resolvePool;

    foreach (layout; [SimulatedNuma.perNode, SimulatedNuma.singleMask])
    {
        SimulatedHost h;
        h.numa = layout;
        ubyte[] cpu, slp;
        h.build(cpu, slp);
        auto snap = parseWindowsTopology(cpu, slp);
        assert(snap.logicalProcessorCount == 256 && snap.groupCount == 4);
        assert(snap.packages.length == 2 && snap.llcCount == 16);
        assert(snap.cores.length == 128, "group-relative CoreIndex must not merge cores across groups");
        assertCoresSorted(snap);
        assert(snap.numaNodes.length == 2, "group-relative NumaNodeIndex 0 must not merge the sockets");
        foreach (ref p; snap.processors)
        {
            assert(p.numaIndex == p.group / 2);
            assert(p.packageIndex == snap.byId(ProcessorId(cast(ushort)(p.group & ~1), 0)).packageIndex);
        }
        foreach (ref n; snap.numaNodes)
        {
            assert(n.processors == groupProcessors(n.numaIndex * 2, 2, 64));
            auto llcs = snap.llcsInNuma(n.numaIndex);
            assert(llcs.length == 8);
            foreach (llc; llcs)
                foreach (id; snap.processorsInLlc(llc))
                    assert(id.group / 2 == n.numaIndex);
        }
        foreach (llc; 0 .. snap.llcCount)
        {
            auto ids = snap.processorsInLlc(cast(ushort) llc);
            assert(ids.length == 16);
            foreach (id; ids)
                assert(id.group == ids[0].group);
        }

        auto home = snap.byId(ProcessorId(0, 0));
        WorkerSelf w;
        w.llcIndex = home.llcIndex;
        w.numaIndex = home.numaIndex;
        bindNumaNeighborhood(w, snap);
        assert(w.numaLlcCount == 8 && w.homeLlc == home.llcIndex);
        assert(w.llcIsNumaLocal(snap.byId(ProcessorId(1, 63)).llcIndex),
            "the socket's other group is in the same NUMA neighbourhood");
        foreach (g; 2 .. 4)
            foreach (lp; [0, 63])
                assert(!w.llcIsNumaLocal(snap.byId(ProcessorId(cast(ushort) g, cast(ushort) lp)).llcIndex),
                    "the other socket's LLCs are remote");
        assert(snap.byId(ProcessorId(3, 63)).smtSibling && !snap.byId(ProcessorId(3, 62)).smtSibling);

        PoolOptions opt;
        auto res = resolvePool(snap, opt);
        assert(res.workers.length == 256 && res.acceptedGroupCount == 16);
    }

    SimulatedHost h;
    h.numa = SimulatedNuma.none;
    ubyte[] cpu, slp;
    h.build(cpu, slp);
    auto snap = parseWindowsTopology(cpu, slp);
    assert(snap.numaNodes.length == 4, "without records each group's local node stays its own");
    bool[ushort] ids;
    foreach (ref n; snap.numaNodes)
    {
        assert(n.processors.length == 64);
        ids[n.numaIndex] = true;
    }
    assert(ids.length == 4);
    WorkerSelf w;
    auto home = snap.byId(ProcessorId(0, 0));
    w.llcIndex = home.llcIndex;
    w.numaIndex = home.numaIndex;
    bindNumaNeighborhood(w, snap);
    assert(w.numaLlcCount == 4);
    assert(!w.llcIsNumaLocal(snap.byId(ProcessorId(1, 0)).llcIndex));
}

/// One group: a CPU set's NUMA index is used as is when no record covers it,
/// and a record's NodeNumber wins when one does.
void checkSingleGroupNumaIndex()
{
    ubyte[] cpu;
    foreach (lp; 0 .. 4)
        putCpuSet(cpu, 0, cast(ubyte) lp, cast(ubyte) lp, 0, cast(ubyte)(lp / 2), 1);
    auto bare = parseWindowsTopology(cpu, null);
    assert(bare.numaNodes.length == 2);
    foreach (ref p; bare.processors)
        assert(p.numaIndex == p.lpIndex / 2);

    ubyte[] slp;
    putNumaRecord(slp, 5, [GroupMask(0, 0x3)]);
    putNumaRecord(slp, 3, [GroupMask(0, 0xC)], true);
    auto recorded = parseWindowsTopology(cpu, slp);
    assert(recorded.byId(ProcessorId(0, 1)).numaIndex == 5);
    assert(recorded.byId(ProcessorId(0, 2)).numaIndex == 3);
    assert(recorded.llcsInNuma(5) == [ushort(0)] && recorded.llcsInNuma(3) == [ushort(0)]);
}

unittest { checkFullProcessorGroups(); }
unittest { checkCacheAcrossProcessorGroups(); }
unittest { checkSimulatedFourGroupHost(); }
unittest { checkSingleGroupNumaIndex(); }

// `TopologySnapshot.cores` is sorted by each core's first ProcessorId.
private void assertCoresSorted(ref const TopologySnapshot snap)
{
    assert(snap.cores.length != 0);
    foreach (i, ref c; snap.cores)
    {
        assert(c.processors.length != 0);
        if (i != 0)
            assert(snap.cores[i - 1].processors[0] < c.processors[0]);
    }
}

unittest
{
    auto snap = parseWindowsTopology(cpuSetsBlob, slpiexBlob);
    assertCoresSorted(snap);
    assert(snap.llcCount == 1);
    assert(snap.logicalProcessorCount == 20);
    assert(snap.pCoreCount == 6);
    assert(snap.eCoreCount == 8);
    assert(snap.cacheLineSize == 64);
    assert(snap.classCount == 2);
    assert(snap.maxEfficiencyClass == 1);
    assert(snap.groupingSource == GroupingSource.l3);
    assert(snap.llcDomains[0].source == GroupingSource.l3);
    assert(snap.processorsInLlc(0) == parseCpuList("0-19"));
    assert(snap.processorsOfClass(classP) == parseCpuList("0-11"));
    assert(snap.processorsOfClass(classE) == parseCpuList("12-19"));

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
    assertCoresSorted(snap);
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
    assert(snap.groupingSource == GroupingSource.l3);
    assert(snap.processorsInLlc(llcG0[0]) == parseCpuList("0:0-3"));
    assert(snap.processorsInLlc(llcG1[0]) == parseCpuList("1:0-3"));
    assert(snap.numaNodes[0].processors == parseCpuList("0-3,1:0-3"));

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
    assert(snap.groupingSource == GroupingSource.cpuSetLlcIndex);
    foreach (ref d; snap.llcDomains)
        assert(d.source == GroupingSource.cpuSetLlcIndex);
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
    assert(!snap.processors[1].affinityExcluded);
}

unittest
{
    ubyte[] cpu;
    putCpuSet(cpu, 0, 0, 0, 0, 0, 1, 0x1);
    putCpuSet(cpu, 0, 1, 1, 0, 0, 1, 0);
    putCpuSet(cpu, 1, 0, 0, 0, 0, 1, 0);
    auto snap = parseWindowsTopology(cpu, null, [affinityKey(0, 0)]);
    assert(snap.processors[0].parkedAtDiscovery);
    assert(!snap.processors[0].affinityExcluded);
    assert(snap.isEligible(snap.processors[0]), "parking is informational");
    assert(snap.processors[1].affinityExcluded);
    assert(!snap.processors[1].parkedAtDiscovery);
    assert(!snap.isEligible(snap.processors[1]));
    assert(snap.processors[2].affinityExcluded);
    foreach (ref p; snap.processors)
        assert(!p.offlineAtDiscovery);
}
