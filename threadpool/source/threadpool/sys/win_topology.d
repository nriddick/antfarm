module threadpool.sys.win_topology;

version (Windows):

import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy;
import std.algorithm : sort;
import std.exception : enforce;

import threadpool.topology;
import threadpool.sys.win_bindings;

private T load(T)(const(ubyte)* p) @trusted
{
    T v;
    memcpy(&v, p, T.sizeof);
    return v;
}

private ulong lpKey(ushort group, ushort lp) @nogc nothrow
{
    return (cast(ulong) group << 16) | lp;
}

private struct CpuSetRec
{
    uint   id;
    ushort group;
    ubyte  lp;
    ubyte  core;
    ubyte  llc;
    ubyte  numa;
    ubyte  eff;
    ubyte  flags;
}

private CpuSetRec[] parseCpuSetRecords(const(ubyte)[] buf)
{
    CpuSetRec[] recs;
    size_t off;
    while (off + 8 <= buf.length)
    {
        auto size = load!uint(buf.ptr + off);
        if (size < 24 || off + size > buf.length)
            break;
        auto type = load!uint(buf.ptr + off + 4);
        if (type == CPU_SET_INFORMATION_TYPE.CpuSet && size >= 32)
        {
            CpuSetRec r;
            r.id = load!uint(buf.ptr + off + 8);
            r.group = load!ushort(buf.ptr + off + 12);
            r.lp = buf[off + 14];
            r.core = buf[off + 15];
            r.llc = buf[off + 16];
            r.numa = buf[off + 17];
            r.eff = buf[off + 18];
            r.flags = buf[off + 19];
            recs ~= r;
        }
        off += size;
    }
    return recs;
}

private struct AffinityMask
{
    ushort group;
    ulong  mask;
}

private AffinityMask[] decodeProcessorMasks(const(ubyte)[] payload)
{
    AffinityMask[] out_;
    if (payload.length < 24) return out_;
    auto groupCount = load!ushort(payload.ptr + 22);
    if (groupCount == 0 || groupCount > 64) return out_;
    auto need = 24 + cast(size_t) groupCount * GROUP_AFFINITY.sizeof;
    if (need > payload.length) return out_;
    foreach (g; 0 .. groupCount)
    {
        auto base = payload.ptr + 24 + g * GROUP_AFFINITY.sizeof;
        AffinityMask m;
        m.mask = cast(ulong) load!KAFFINITY(base);
        m.group = load!ushort(base + KAFFINITY.sizeof);
        out_ ~= m;
    }
    return out_;
}

private AffinityMask[] decodeCacheMasks(const(ubyte)[] payload)
{
    AffinityMask[] out_;
    if (payload.length < 32) return out_;
    auto groupCount = load!ushort(payload.ptr + 30);
    size_t maskOff = 32;
    if (groupCount == 0 || groupCount > 64 ||
        32 + cast(size_t) groupCount * GROUP_AFFINITY.sizeof > payload.length)
    {
        groupCount = 1;
        maskOff = 32;
        if (payload.length < maskOff + GROUP_AFFINITY.sizeof) return out_;
    }
    foreach (g; 0 .. groupCount)
    {
        auto base = payload.ptr + maskOff + g * GROUP_AFFINITY.sizeof;
        if (base + GROUP_AFFINITY.sizeof > payload.ptr + payload.length) break;
        AffinityMask m;
        m.mask = cast(ulong) load!KAFFINITY(base);
        m.group = load!ushort(base + KAFFINITY.sizeof);
        out_ ~= m;
    }
    return out_;
}

private void eachBit(ushort group, ulong mask, scope void delegate(ushort group, ushort lp) dg)
{
    foreach (bit; 0 .. 64)
    {
        if (mask & (1UL << bit))
            dg(group, cast(ushort) bit);
    }
}

private void walkSlpiex(const(ubyte)[] buf,
    scope void delegate(uint relationship, const(ubyte)[] payload, uint recSize) dg)
{
    size_t off;
    while (off + 8 <= buf.length)
    {
        auto rel = load!uint(buf.ptr + off);
        auto size = load!uint(buf.ptr + off + 4);
        if (size < 8 || off + size > buf.length)
            break;
        dg(rel, buf[off + 8 .. off + size], size);
        off += size;
    }
}

/// Parse captured or live buffers. Does not call Win32.
TopologySnapshot parseWindowsTopology(const(ubyte)[] cpuSets, const(ubyte)[] slpiex)
{
    enforce(cpuSets.length >= 32, "threadpool: empty CPU-set buffer");

    auto recs = parseCpuSetRecords(cpuSets);
    enforce(recs.length > 0, "threadpool: no CPU-set records");

    LogicalProcessor[] procs;
    procs.length = recs.length;
    foreach (i, r; recs)
    {
        procs[i].group = r.group;
        procs[i].lpIndex = r.lp;
        procs[i].cpuSetId = r.id;
        procs[i].coreIndex = r.core;
        procs[i].llcIndexInGroup = r.llc;
        procs[i].numaIndex = r.numa;
        procs[i].packageIndex = 0;
        procs[i].moduleIndex = ushort.max;
        procs[i].efficiencyClass = r.eff;
        procs[i].parkedAtDiscovery = (r.flags & SYSTEM_CPU_SET_INFORMATION_PARKED) != 0;
        procs[i].smtSibling = false;
    }

    size_t[ulong] indexByLp;
    foreach (i, ref p; procs)
        indexByLp[lpKey(p.group, p.lpIndex)] = i;

    LogicalProcessor* lpAt(ushort group, ushort lp)
    {
        auto k = lpKey(group, lp);
        if (auto ip = k in indexByLp)
            return &procs[*ip];
        return null;
    }

    ushort nextLlc;
    ushort[ulong] llcOfLp;
    bool assignedFromCache;

    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationCache) return;
        if (payload.length < 12) return;
        auto level = payload[0];
        if (level != 3) return;
        auto masks = decodeCacheMasks(payload);
        auto llc = nextLlc++;
        assignedFromCache = true;
        foreach (m; masks)
        {
            eachBit(m.group, m.mask, (g, lp) {
                auto k = lpKey(g, lp);
                if (k !in llcOfLp)
                    llcOfLp[k] = llc;
            });
        }
    });

    if (!assignedFromCache || nextLlc == 0)
    {
        llcOfLp = llcOfLp.init;
        nextLlc = 0;
        ushort[ulong] clusterToDense;
        foreach (ref p; procs)
        {
            auto ck = (cast(ulong) p.group << 8) | p.llcIndexInGroup;
            if (ck !in clusterToDense)
                clusterToDense[ck] = nextLlc++;
            llcOfLp[lpKey(p.group, p.lpIndex)] = clusterToDense[ck];
        }
    }
    else
    {
        ushort[ulong] clusterToDense;
        foreach (ref p; procs)
        {
            auto k = lpKey(p.group, p.lpIndex);
            if (k in llcOfLp) continue;
            auto ck = (cast(ulong) p.group << 8) | p.llcIndexInGroup;
            if (ck !in clusterToDense)
                clusterToDense[ck] = nextLlc++;
            llcOfLp[k] = clusterToDense[ck];
        }
    }

    foreach (ref p; procs)
        p.llcIndex = llcOfLp.get(lpKey(p.group, p.lpIndex), cast(ushort) 0);

    ushort maxLine;
    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationCache) return;
        if (payload.length < 12) return;
        auto level = payload[0];
        auto assoc = payload[1];
        auto lineSize = load!ushort(payload.ptr + 2);
        auto cacheSize = load!uint(payload.ptr + 4);
        auto cacheType = load!uint(payload.ptr + 8);
        if (lineSize > maxLine) maxLine = lineSize;

        CacheKind kind = CacheKind.other;
        if (level == 1 && cacheType == PROCESSOR_CACHE_TYPE.CacheData)
            kind = CacheKind.l1d;
        else if (level == 1 && cacheType == PROCESSOR_CACHE_TYPE.CacheInstruction)
            kind = CacheKind.l1i;
        else if (level == 2)
            kind = CacheKind.l2;
        else if (level == 3)
            kind = CacheKind.l3;

        CacheInfo info;
        info.kind = kind;
        info.level = level;
        info.lineSize = lineSize;
        info.sizeBytes = cacheSize;
        info.associativity = assoc;

        auto masks = decodeCacheMasks(payload);
        foreach (m; masks)
        {
            eachBit(m.group, m.mask, (g, lp) {
                auto proc = lpAt(g, lp);
                if (proc is null) return;
                info.llcIndex = proc.llcIndex;
                final switch (kind)
                {
                case CacheKind.l1d: proc.l1d = info; break;
                case CacheKind.l1i: proc.l1i = info; break;
                case CacheKind.l2:  proc.l2  = info; break;
                case CacheKind.l3:  proc.l3  = info; break;
                case CacheKind.other: break;
                }
            });
        }
    });

    // SMT from shared CoreIndex, confirmed by RelationProcessorCore flags.
    bool[ulong] smtByCoreRel;
    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorCore) return;
        if (payload.length < 1) return;
        auto flags = payload[0];
        auto masks = decodeProcessorMasks(payload);
        foreach (m; masks)
        {
            eachBit(m.group, m.mask, (g, lp) {
                auto proc = lpAt(g, lp);
                if (proc is null) return;
                auto ck = (cast(ulong) g << 16) | proc.coreIndex;
                if (flags & LTP_PC_SMT)
                    smtByCoreRel[ck] = true;
            });
        }
    });

    size_t[][ulong] lpsByCore;
    foreach (i, ref p; procs)
    {
        auto ck = (cast(ulong) p.group << 16) | p.coreIndex;
        lpsByCore[ck] ~= i;
    }
    foreach (ck, idxs; lpsByCore)
    {
        idxs.sort!((a, b) => procs[a].lpIndex < procs[b].lpIndex);
        auto smt = idxs.length > 1;
        if (ck in smtByCoreRel)
            smt = smtByCoreRel[ck] || smt;
        foreach (n, i; idxs)
            procs[i].smtSibling = smt && n != 0;
    }

    ushort moduleId;
    L2Cluster[] clusters;
    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorModule) return;
        auto masks = decodeProcessorMasks(payload);
        L2Cluster c;
        c.moduleIndex = moduleId;
        uint l2sz;
        ushort llc;
        foreach (m; masks)
        {
            eachBit(m.group, m.mask, (g, lp) {
                auto proc = lpAt(g, lp);
                if (proc is null) return;
                proc.moduleIndex = moduleId;
                c.lpIndices ~= proc.lpIndex;
                l2sz = proc.l2.sizeBytes;
                llc = proc.llcIndex;
            });
        }
        c.l2SizeBytes = l2sz;
        c.llcIndex = llc;
        c.lpIndices.sort();
        clusters ~= c;
        moduleId++;
    });

    if (clusters.length == 0)
    {
        bool[ulong] seenL2;
        walkSlpiex(slpiex, (rel, payload, recSize) {
            if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationCache) return;
            if (payload.length < 12) return;
            if (payload[0] != 2) return;
            auto masks = decodeCacheMasks(payload);
            L2Cluster c;
            c.moduleIndex = moduleId;
            foreach (m; masks)
            {
                eachBit(m.group, m.mask, (g, lp) {
                    auto proc = lpAt(g, lp);
                    if (proc is null) return;
                    proc.moduleIndex = moduleId;
                    c.lpIndices ~= proc.lpIndex;
                    c.l2SizeBytes = proc.l2.sizeBytes;
                    c.llcIndex = proc.llcIndex;
                });
            }
            c.lpIndices.sort();
            auto k = (cast(ulong) c.llcIndex << 32) ^ c.lpIndices.hashOf;
            if (k in seenL2) return;
            seenL2[k] = true;
            clusters ~= c;
            moduleId++;
        });
    }

    ushort nextPackage;
    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorPackage) return;
        auto masks = decodeProcessorMasks(payload);
        auto pkg = nextPackage++;
        foreach (m; masks)
        {
            eachBit(m.group, m.mask, (g, lp) {
                auto proc = lpAt(g, lp);
                if (proc !is null)
                    proc.packageIndex = pkg;
            });
        }
    });

    ProcGroup[] groups;
    walkSlpiex(slpiex, (rel, payload, recSize) {
        if (rel != LOGICAL_PROCESSOR_RELATIONSHIP.RelationGroup) return;
        if (payload.length < 4) return;
        auto maxG = load!ushort(payload.ptr + 0);
        auto actG = load!ushort(payload.ptr + 2);
        auto infoOff = 4 + 20;
        foreach (g; 0 .. actG)
        {
            auto base = payload.ptr + infoOff + g * PROCESSOR_GROUP_INFO.sizeof;
            if (base + PROCESSOR_GROUP_INFO.sizeof > payload.ptr + payload.length) break;
            ProcGroup pg;
            pg.group = cast(ushort) g;
            pg.maxProcessors = *base;
            pg.activeProcessors = *(base + 1);
            pg.activeMask = cast(ulong) load!KAFFINITY(base + 40);
            groups ~= pg;
        }
        if (groups.length == 0 && maxG > 0)
        {
            ProcGroup pg;
            pg.group = 0;
            groups ~= pg;
        }
    });
    if (groups.length == 0)
    {
        bool[ushort] seen;
        foreach (ref p; procs)
            seen[p.group] = true;
        foreach (g; seen.byKey)
        {
            ProcGroup pg;
            pg.group = g;
            groups ~= pg;
        }
    }

    ubyte maxClass;
    foreach (ref p; procs)
        if (p.efficiencyClass > maxClass)
            maxClass = p.efficiencyClass;
    bool hasLower;
    foreach (ref p; procs)
        if (p.efficiencyClass < maxClass)
            hasLower = true;
    auto classCount = hasLower ? ushort(2) : ushort(1);

    PhysicalCore[] cores;
    foreach (ck, idxs; lpsByCore)
    {
        if (idxs.length == 0) continue;
        auto p0 = procs[idxs[0]];
        PhysicalCore c;
        c.coreIndex = p0.coreIndex;
        c.llcIndex = p0.llcIndex;
        c.moduleIndex = p0.moduleIndex;
        c.efficiencyClass = p0.efficiencyClass;
        c.smt = idxs.length > 1;
        foreach (i; idxs)
            c.lpIndices ~= procs[i].lpIndex;
        cores ~= c;
    }

    LlcDomain[] domains;
    domains.length = nextLlc;
    foreach (i; 0 .. nextLlc)
        domains[i].llcIndex = cast(ushort) i;
    bool[ulong] coreOnDomain;
    foreach (ref p; procs)
    {
        if (p.llcIndex >= domains.length) continue;
        auto d = &domains[p.llcIndex];
        d.lpIndices ~= p.lpIndex;
        if (p.l3.sizeBytes) d.l3SizeBytes = p.l3.sizeBytes;
        if (p.l3.lineSize) d.lineSize = p.l3.lineSize;
        auto ck = (cast(ulong) p.group << 16) | p.coreIndex;
        if (ck in coreOnDomain) continue;
        coreOnDomain[ck] = true;
        if (p.efficiencyClass == maxClass)
            d.pCoreIndices ~= p.coreIndex;
        else
            d.eCoreIndices ~= p.coreIndex;
    }
    foreach (ref d; domains)
    {
        d.lpIndices.sort();
        d.pCoreIndices.sort();
        d.eCoreIndices.sort();
    }

    NumaNode[] numa;
    size_t[ushort] numaIdx;
    foreach (ref p; procs)
    {
        if (p.numaIndex !in numaIdx)
        {
            numaIdx[p.numaIndex] = numa.length;
            NumaNode n;
            n.numaIndex = p.numaIndex;
            numa ~= n;
        }
        numa[numaIdx[p.numaIndex]].lpIndices ~= p.lpIndex;
    }

    Package[] packages;
    size_t[ushort] pkgIdx;
    foreach (ref p; procs)
    {
        if (p.packageIndex !in pkgIdx)
        {
            pkgIdx[p.packageIndex] = packages.length;
            Package pkg;
            pkg.packageIndex = p.packageIndex;
            packages ~= pkg;
        }
        packages[pkgIdx[p.packageIndex]].lpIndices ~= p.lpIndex;
    }

    ushort pCores, eCores;
    foreach (ref c; cores)
    {
        if (c.efficiencyClass == maxClass) pCores++;
        else eCores++;
    }

    TopologySnapshot snap;
    snap.os = "windows";
    snap.cacheLineSize = maxLine ? maxLine : 64;
    snap.llcCount = nextLlc;
    snap.pCoreCount = pCores;
    snap.eCoreCount = eCores;
    snap.logicalProcessorCount = cast(ushort) procs.length;
    snap.groupCount = cast(ushort) groups.length;
    snap.maxEfficiencyClass = maxClass;
    snap.classCount = classCount;
    snap.processors = procs;
    snap.cores = cores;
    snap.l2Clusters = clusters;
    snap.llcDomains = domains;
    snap.numaNodes = numa;
    snap.packages = packages;
    snap.groups = groups;
    return snap;
}

/// Windows 10 1803+ (`GetSystemCpuSetInformation`). No older-Windows fallback.
TopologySnapshot discoverWindows()
{
    import core.sys.windows.winbase : GetCurrentProcess, GetLastError;
    import core.sys.windows.winerror : ERROR_INSUFFICIENT_BUFFER;

    resolveOptionalApis();

    uint cpuNeed;
    GetSystemCpuSetInformation(null, 0, &cpuNeed, GetCurrentProcess(), 0);
    enforce(cpuNeed > 0, "threadpool: GetSystemCpuSetInformation size query failed");
    auto cpuBuf = cast(ubyte*) malloc(cpuNeed);
    enforce(cpuBuf !is null, "threadpool: malloc cpu-set buffer failed");
    scope (exit) free(cpuBuf);
    uint cpuGot;
    auto cpuOk = GetSystemCpuSetInformation(cpuBuf, cpuNeed, &cpuGot, GetCurrentProcess(), 0);
    enforce(cpuOk && cpuGot > 0, "threadpool: GetSystemCpuSetInformation failed");

    uint slpNeed;
    GetLogicalProcessorInformationEx(LOGICAL_PROCESSOR_RELATIONSHIP.RelationAll, null, &slpNeed);
    enforce(slpNeed > 0, "threadpool: GetLogicalProcessorInformationEx size query failed");
    auto slpBuf = cast(ubyte*) malloc(slpNeed);
    enforce(slpBuf !is null, "threadpool: malloc SLPIEx buffer failed");
    scope (exit) free(slpBuf);
    uint slpGot = slpNeed;
    auto slpOk = GetLogicalProcessorInformationEx(
        LOGICAL_PROCESSOR_RELATIONSHIP.RelationAll, slpBuf, &slpGot);
    enforce(slpOk && slpGot > 0, "threadpool: GetLogicalProcessorInformationEx failed");

    return parseWindowsTopology(cpuBuf[0 .. cpuGot], slpBuf[0 .. slpGot]);
}
