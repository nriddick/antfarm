module threadpool.topology;

enum CacheKind : ubyte
{
    l1d,
    l1i,
    l2,
    l3,
    other,
}

struct CacheInfo
{
    CacheKind kind;
    ubyte     level;
    ushort    lineSize;
    uint      sizeBytes;
    ushort    associativity;
    ushort    llcIndex;
}

struct LogicalProcessor
{
    ushort group;
    ushort lpIndex;
    uint   cpuSetId;
    ushort coreIndex;
    ushort llcIndex;
    ushort llcIndexInGroup;
    ushort numaIndex;
    ushort packageIndex;
    ushort moduleIndex;
    ubyte  efficiencyClass;
    bool   smtSibling;
    bool   parkedAtDiscovery;
    CacheInfo l1d, l1i, l2, l3;
}

struct PhysicalCore
{
    ushort coreIndex;
    ushort llcIndex;
    ushort moduleIndex;
    ubyte  efficiencyClass;
    bool   smt;
    ushort[] lpIndices;
}

struct L2Cluster
{
    ushort moduleIndex;
    ushort llcIndex;
    uint   l2SizeBytes;
    ushort[] lpIndices;
}

struct LlcDomain
{
    ushort llcIndex;
    uint   l3SizeBytes;
    ushort lineSize;
    ushort[] lpIndices;
    ushort[] pCoreIndices;
    ushort[] eCoreIndices;
}

struct NumaNode
{
    ushort numaIndex;
    ushort[] lpIndices;
}

struct Package
{
    ushort packageIndex;
    ushort[] lpIndices;
}

struct ProcGroup
{
    ushort group;
    ushort maxProcessors;
    ushort activeProcessors;
    ulong  activeMask;
}

struct TopologySnapshot
{
    string os;
    ushort cacheLineSize;
    ushort llcCount;
    ushort pCoreCount;
    ushort eCoreCount;
    ushort logicalProcessorCount;
    ushort groupCount;
    ubyte  maxEfficiencyClass;
    ushort classCount;

    LogicalProcessor[] processors;
    PhysicalCore[]     cores;
    L2Cluster[]        l2Clusters;
    LlcDomain[]        llcDomains;
    NumaNode[]         numaNodes;
    Package[]          packages;
    ProcGroup[]        groups;

    const(LogicalProcessor)* byLp(ushort group, ushort lpIndex) const @nogc nothrow
    {
        foreach (i; 0 .. processors.length)
        {
            if (processors[i].group == group && processors[i].lpIndex == lpIndex)
                return &processors[i];
        }
        return null;
    }

    const(LlcDomain)* domain(ushort llcIndex) const @nogc nothrow
    {
        if (llcIndex >= llcDomains.length) return null;
        return &llcDomains[llcIndex];
    }

    /// Thread-safe. Best-effort “this LP right now” (unpinned threads may migrate).
    const(LogicalProcessor)* current() const @nogc nothrow
    {
        version (Windows)
        {
            import threadpool.sys.win_bindings : PROCESSOR_NUMBER, GetCurrentProcessorNumberEx;

            PROCESSOR_NUMBER pn;
            GetCurrentProcessorNumberEx(&pn);
            return byLp(pn.Group, pn.Number);
        }
        else version (linux)
        {
            import core.sys.linux.sched : sched_getcpu;

            int cpu = sched_getcpu();
            if (cpu < 0) return null;
            return byLp(0, cast(ushort) cpu);
        }
        else
        {
            return null;
        }
    }

    ushort[] lpsInLlc(ushort llcIndex) const
    {
        auto d = domain(llcIndex);
        if (d is null) return null;
        return d.lpIndices.dup;
    }

    ushort[] pLps() const
    {
        ushort[] r;
        foreach (ref p; processors)
            if (p.efficiencyClass == maxEfficiencyClass)
                r ~= p.lpIndex;
        return r;
    }

    ushort[] eLps() const
    {
        ushort[] r;
        if (classCount <= 1) return r;
        foreach (ref p; processors)
            if (p.efficiencyClass < maxEfficiencyClass)
                r ~= p.lpIndex;
        return r;
    }

    /// Distinct process-wide llcIndex values whose LPs sit on this NUMA node.
    ushort[] llcsInNuma(ushort numaIndex) const
    {
        ushort[] r;
        foreach (ref p; processors)
        {
            if (p.numaIndex != numaIndex) continue;
            bool seen;
            foreach (v; r)
                if (v == p.llcIndex) { seen = true; break; }
            if (!seen)
                r ~= p.llcIndex;
        }
        return r;
    }

    bool llcBelongsToNuma(ushort llcIndex, ushort numaIndex) const @nogc nothrow
    {
        foreach (ref p; processors)
            if (p.llcIndex == llcIndex && p.numaIndex == numaIndex)
                return true;
        return false;
    }
}

enum maxNumaLlcs = 16;

/// dest[0] = homeLlc, dest[1..] = other LLCs on the same NUMA node. Returns count.
ushort fillNumaLlcs(ushort homeLlc, ushort numaIndex,
                    ref const TopologySnapshot snap, ushort[] dest) @nogc nothrow
{
    if (dest.length == 0) return 0;
    dest[0] = homeLlc;
    ushort n = 1;
    foreach (ref p; snap.processors)
    {
        if (p.numaIndex != numaIndex) continue;
        if (p.llcIndex == homeLlc) continue;
        bool seen;
        foreach (i; 0 .. n)
            if (dest[i] == p.llcIndex) { seen = true; break; }
        if (seen) continue;
        if (n >= dest.length) break;
        dest[n++] = p.llcIndex;
    }
    return n;
}

enum classE = ubyte(0);
enum classP = ubyte(1);

ubyte tableClassIndex(ubyte efficiencyClass, ubyte maxClass, ushort classCount) @nogc nothrow
{
    if (classCount <= 1) return 0;
    return efficiencyClass >= maxClass ? classP : classE;
}

private __gshared const(TopologySnapshot)* gTopology;

/// Discover once. Subsequent calls return the same immutable snapshot.
/// First publication is not internally locked — call from `shared static this()`
/// or a single thread before workers start. After `gTopology` is set, further
/// calls and `cachedSnapshot()` are thread-safe reads.
const(TopologySnapshot) discover()
{
    if (gTopology !is null)
        return *gTopology;

    version (Windows)
    {
        import threadpool.sys.win_topology : discoverWindows;

        auto snap = discoverWindows();
        auto heap = new TopologySnapshot;
        *heap = snap;
        gTopology = heap;
        return *gTopology;
    }
    else version (linux)
    {
        import threadpool.sys.linux_topology : discoverLinux;

        auto snap = discoverLinux();
        auto heap = new TopologySnapshot;
        *heap = snap;
        gTopology = heap;
        return *gTopology;
    }
    else
    {
        throw new Error("threadpool: topology discovery is not implemented in v1");
    }
}

/// Build the derived arrays (cores, LLC domains, NUMA, packages, counts)
/// from a filled `LogicalProcessor[]`. OS parsers share this so Windows
/// group-relative ids and Linux sysfs walks do not each invent a table layout.
TopologySnapshot assembleSnapshot(
    string os,
    LogicalProcessor[] procs,
    L2Cluster[] clusters,
    ProcGroup[] groups,
    ushort llcCount,
    ushort cacheLineSize)
{
    import std.algorithm : sort;

    ubyte maxClass;
    foreach (ref p; procs)
        if (p.efficiencyClass > maxClass)
            maxClass = p.efficiencyClass;
    bool hasLower;
    foreach (ref p; procs)
        if (p.efficiencyClass < maxClass)
            hasLower = true;
    auto classCount = hasLower ? ushort(2) : ushort(1);

    size_t[][ulong] lpsByCore;
    foreach (i, ref p; procs)
    {
        auto ck = (cast(ulong) p.group << 32)
            | (cast(ulong) p.packageIndex << 16)
            | p.coreIndex;
        lpsByCore[ck] ~= i;
    }

    PhysicalCore[] cores;
    foreach (ck, idxs; lpsByCore)
    {
        if (idxs.length == 0) continue;
        idxs.sort!((a, b) => procs[a].lpIndex < procs[b].lpIndex);
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

    ushort maxLlc = llcCount;
    foreach (ref p; procs)
        if (p.llcIndex + 1 > maxLlc)
            maxLlc = cast(ushort)(p.llcIndex + 1);

    LlcDomain[] domains;
    domains.length = maxLlc;
    foreach (i; 0 .. maxLlc)
        domains[i].llcIndex = cast(ushort) i;
    bool[ulong] coreOnDomain;
    foreach (ref p; procs)
    {
        if (p.llcIndex >= domains.length) continue;
        auto d = &domains[p.llcIndex];
        d.lpIndices ~= p.lpIndex;
        if (p.l3.sizeBytes) d.l3SizeBytes = p.l3.sizeBytes;
        if (p.l3.lineSize) d.lineSize = p.l3.lineSize;
        auto ck = (cast(ulong) p.group << 32)
            | (cast(ulong) p.packageIndex << 16)
            | p.coreIndex;
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

    ushort pCores, eCores;
    foreach (ref c; cores)
    {
        if (c.efficiencyClass == maxClass) pCores++;
        else eCores++;
    }

    TopologySnapshot snap;
    snap.os = os;
    snap.cacheLineSize = cacheLineSize ? cacheLineSize : 64;
    snap.llcCount = maxLlc;
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

/// Cached snapshot if discover() has run; null otherwise.
/// Thread-safe read after a completed `discover()`.
const(TopologySnapshot)* cachedSnapshot() @nogc nothrow
{
    return gTopology;
}
