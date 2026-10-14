module threadpool.topology;

/// One logical processor: Windows processor group plus group-relative LP
/// number. Linux uses group 0 and the system CPU number. Text form `g:lp`.
struct ProcessorId
{
    ushort group;
    ushort lp;

    /// The identity of a discovered processor.
    static ProcessorId of(ref const LogicalProcessor p) @nogc nothrow pure
    {
        return ProcessorId(p.group, p.lpIndex);
    }

    /// `(group, lp)` order.
    int opCmp(ProcessorId rhs) const @nogc nothrow pure
    {
        if (group != rhs.group) return group < rhs.group ? -1 : 1;
        if (lp != rhs.lp) return lp < rhs.lp ? -1 : 1;
        return 0;
    }

    /// Writes `g:lp`.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        char[16] buf;
        sink(formatUint(buf[], group));
        sink(":");
        sink(formatUint(buf[], lp));
    }
}

/// Malformed CPU-list text. `offset` is the start of the offending item.
class CpuListException : Exception
{
    size_t offset;

    this(string msg, size_t offset, string file = __FILE__, size_t line = __LINE__) pure
    {
        super(msg, file, line);
        this.offset = offset;
    }
}

/// Parse a CPU list. Items are comma-separated; surrounding whitespace is
/// ignored:
///     "lp" | "a-b"          -> group 0 (the Linux form, e.g. "0-7,16-23")
///     "g:lp" | "g:a-b"      -> Windows processor group g
/// Order is preserved, including duplicates: the result is an ordered request,
/// not a set. "" -> []. Throws `CpuListException` (with the offending item and
/// its offset) on syntax errors, a > b, or values above ushort.max. Pure: no
/// topology lookup, so unknown CPUs are reported later by their consumer, not
/// here.
ProcessorId[] parseCpuList(const(char)[] spec) pure
{
    ProcessorId[] r;
    size_t pos;
    if (isBlank(spec)) return r;
    while (true)
    {
        size_t end = pos;
        while (end < spec.length && spec[end] != ',') end++;
        size_t a = pos, b = end;
        while (a < b && isSpace(spec[a])) a++;
        while (b > a && isSpace(spec[b - 1])) b--;
        auto item = spec[a .. b];

        void fail(string why)
        {
            throw new CpuListException("threadpool: bad CPU list item '"
                ~ item.idup ~ "' at offset " ~ offsetText(a) ~ ": " ~ why, a);
        }

        if (item.length == 0) fail("empty item");
        ushort group;
        auto range = item;
        foreach (i, ch; item)
            if (ch == ':')
            {
                if (!parseU16(item[0 .. i], group)) fail("bad group");
                range = item[i + 1 .. $];
                break;
            }
        ushort lo, hi;
        size_t dash = range.length;
        foreach (i, ch; range)
            if (ch == '-') { dash = i; break; }
        if (!parseU16(range[0 .. dash], lo)) fail("bad processor number");
        hi = lo;
        if (dash < range.length && !parseU16(range[dash + 1 .. $], hi))
            fail("bad range end");
        if (lo > hi) fail("range start exceeds end");
        foreach (v; lo .. hi + 1)
            r ~= ProcessorId(group, cast(ushort) v);

        if (end >= spec.length) break;
        pos = end + 1;
    }
    return r;
}

/// Inverse of `parseCpuList` for reports: group-0 runs print as "a-b", other
/// groups as "g:a-b". `parseCpuList(formatCpuList(x)) == x` for sorted,
/// duplicate-free x.
string formatCpuList(const(ProcessorId)[] ids) pure
{
    string r;
    char[16] buf;
    size_t i;
    while (i < ids.length)
    {
        size_t j = i;
        while (j + 1 < ids.length && ids[j + 1].group == ids[i].group
            && ids[j + 1].lp == ids[j].lp + 1)
            j++;
        if (r.length) r ~= ",";
        if (ids[i].group != 0)
            r ~= formatUint(buf[], ids[i].group) ~ ":";
        r ~= formatUint(buf[], ids[i].lp);
        if (j > i)
            r ~= "-" ~ formatUint(buf[], ids[j].lp);
        i = j + 1;
    }
    return r;
}

private bool isSpace(char c) @nogc nothrow pure
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

private bool isBlank(const(char)[] s) @nogc nothrow pure
{
    foreach (c; s)
        if (!isSpace(c)) return false;
    return true;
}

private bool parseU16(const(char)[] s, out ushort v) @nogc nothrow pure
{
    if (s.length == 0) return false;
    uint acc;
    foreach (c; s)
    {
        if (c < '0' || c > '9') return false;
        acc = acc * 10 + (c - '0');
        if (acc > ushort.max) return false;
    }
    v = cast(ushort) acc;
    return true;
}

private string formatUint(char[] buf, ulong v) pure
{
    size_t n = buf.length;
    do
    {
        buf[--n] = cast(char)('0' + v % 10);
        v /= 10;
    }
    while (v != 0);
    return buf[n .. $].idup;
}

private string offsetText(size_t v) pure
{
    char[24] buf;
    return formatUint(buf[], v);
}

/// How a physical group (LLC domain) was formed. Only `l3` on a chiplet part
/// means a CCD; the fallbacks are shards, not caches.
enum GroupingSource : ubyte
{
    l3,                /// shared L3 (Linux sysfs, Windows RelationCache)
    l2Fallback,        /// Linux: no L3 reported, grouped by shared L2
    singletonFallback, /// Linux: no shared cache reported, one LP per group
    cpuSetLlcIndex,    /// Windows: no L3 record, CPU-set LastLevelCacheIndex
}

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
    /// Owning processor's cache domain, or `noLlcIndex` when ungrouped.
    ushort    llcIndex;
}

/// A discovered processor has no cache-domain membership.
enum noLlcIndex = ushort.max;

struct LogicalProcessor
{
    ushort group;
    ushort lpIndex;
    uint   cpuSetId;
    ushort coreIndex;
    /// Process-wide cache domain. Linux offline CPUs remain in `processors`
    /// but are omitted from `llcDomains` and use `noLlcIndex`.
    ushort llcIndex;
    /// OS-relative cache index, or `noLlcIndex` for Linux offline CPUs.
    ushort llcIndexInGroup;
    ushort numaIndex;
    ushort packageIndex;
    ushort moduleIndex;
    ubyte  efficiencyClass;
    bool   smtSibling;
    /// Windows CPU-set parking at discovery. Informational only; Linux never
    /// sets it (offline CPUs use `offlineAtDiscovery`).
    bool   parkedAtDiscovery;
    CacheInfo l1d, l1i, l2, l3;
    /// Outside the process OS scope at discovery.
    bool   affinityExcluded;
    /// Linux: offline in sysfs at discovery. Windows: always false.
    bool   offlineAtDiscovery;
}

/// One physical core. `TopologySnapshot.cores` is sorted by `processors[0]`.
struct PhysicalCore
{
    ushort coreIndex;
    /// Cache domain of an online sibling, or `noLlcIndex` if all are offline.
    ushort llcIndex;
    ushort moduleIndex;
    ubyte  efficiencyClass;
    bool   smt;
    ProcessorId[] processors;
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
    ProcessorId[] processors;
    ushort[] pCoreIndices;
    ushort[] eCoreIndices;
    GroupingSource source;
}

struct NumaNode
{
    ushort numaIndex;
    ProcessorId[] processors;
}

struct Package
{
    ushort packageIndex;
    ProcessorId[] processors;
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
    /// Snapshot-wide grouping source: the weakest source of any domain
    /// (`l3` < `l2Fallback` < `singletonFallback` < `cpuSetLlcIndex`).
    GroupingSource groupingSource;

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

    const(LogicalProcessor)* byId(ProcessorId id) const @nogc nothrow
    {
        return byLp(id.group, id.lp);
    }

    /// Inside the frozen OS scope and (Linux) online at discovery.
    bool isEligible(ref const LogicalProcessor p) const @nogc nothrow
    {
        return !p.affinityExcluded && !p.offlineAtDiscovery;
    }

    /// Every processor of a physical group, sorted by `(group, lp)`.
    const(ProcessorId)[] processorsInLlc(ushort llcIndex) const @nogc nothrow
    {
        auto d = domain(llcIndex);
        return d is null ? null : d.processors;
    }

    /// Eligible processors of a physical group, sorted by `(group, lp)`.
    ProcessorId[] eligibleProcessorsInLlc(ushort llcIndex) const
    {
        ProcessorId[] r;
        foreach (id; processorsInLlc(llcIndex))
        {
            auto p = byId(id);
            if (p !is null && isEligible(*p))
                r ~= id;
        }
        return r;
    }

    /// Physical groups with at least one eligible processor.
    ushort eligibleLlcCount() const @nogc nothrow
    {
        ushort n;
        foreach (ref d; llcDomains)
            foreach (id; d.processors)
            {
                auto p = byId(id);
                if (p !is null && isEligible(*p)) { n++; break; }
            }
        return n;
    }

    /// Processors whose dense table class (`tableClassIndex`) is `tableClass`.
    /// A homogeneous machine has the single class `classE` (0).
    ProcessorId[] processorsOfClass(ubyte tableClass) const
    {
        ProcessorId[] r;
        foreach (ref p; processors)
            if (tableClassIndex(p.efficiencyClass, maxEfficiencyClass, classCount) == tableClass)
                r ~= ProcessorId.of(p);
        return r;
    }

    /// Distinct process-wide llcIndex values whose LPs sit on this NUMA node.
    ushort[] llcsInNuma(ushort numaIndex) const
    {
        ushort[] r;
        foreach (ref p; processors)
        {
            if (p.llcIndex == noLlcIndex) continue;
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
        if (llcIndex == noLlcIndex) return false;
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
    if (dest.length == 0 || homeLlc == noLlcIndex) return 0;
    dest[0] = homeLlc;
    ushort n = 1;
    foreach (ref p; snap.processors)
    {
        if (p.llcIndex == noLlcIndex) continue;
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
    ushort cacheLineSize,
    const(GroupingSource)[] sourceOfLlc)
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
        // An offline primary can precede an online sibling on this core.
        foreach (i; idxs)
            if (procs[i].llcIndex != noLlcIndex)
            {
                c.llcIndex = procs[i].llcIndex;
                c.moduleIndex = procs[i].moduleIndex;
                break;
            }
        c.efficiencyClass = p0.efficiencyClass;
        c.smt = idxs.length > 1;
        foreach (i; idxs)
            c.processors ~= ProcessorId.of(procs[i]);
        cores ~= c;
    }
    cores.sort!((a, b) => a.processors[0] < b.processors[0]);

    ushort maxLlc = llcCount;
    foreach (ref p; procs)
        if (p.llcIndex != noLlcIndex && p.llcIndex + 1 > maxLlc)
            maxLlc = cast(ushort)(p.llcIndex + 1);

    LlcDomain[] domains;
    domains.length = maxLlc;
    GroupingSource weakest = GroupingSource.l3;
    foreach (i; 0 .. maxLlc)
    {
        domains[i].llcIndex = cast(ushort) i;
        domains[i].source = i < sourceOfLlc.length
            ? sourceOfLlc[i] : GroupingSource.singletonFallback;
        if (domains[i].source > weakest)
            weakest = domains[i].source;
    }
    bool[ulong] coreOnDomain;
    foreach (ref p; procs)
    {
        if (p.llcIndex >= domains.length) continue;
        auto d = &domains[p.llcIndex];
        d.processors ~= ProcessorId.of(p);
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
        d.processors.sort();
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
        numa[numaIdx[p.numaIndex]].processors ~= ProcessorId.of(p);
    }
    foreach (ref n; numa)
        n.processors.sort();

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
        packages[pkgIdx[p.packageIndex]].processors ~= ProcessorId.of(p);
    }
    foreach (ref pkg; packages)
        pkg.processors.sort();

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
    snap.groupingSource = weakest;
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

/// Allowed-processor key: `(group << 16) | lpIndex`. Linux uses group 0.
ulong affinityKey(ushort group, ushort lpIndex) @nogc nothrow
{
    return (cast(ulong) group << 16) | lpIndex;
}

/// Mark processors outside process affinity. An empty `allowedKeys` imposes
/// no restriction. Excluded processors stay in the snapshot.
void intersectAffinity(ref LogicalProcessor[] procs, const(ulong)[] allowedKeys)
    @nogc nothrow
{
    if (allowedKeys.length == 0) return;
    foreach (ref p; procs)
    {
        auto key = affinityKey(p.group, p.lpIndex);
        bool allowed;
        foreach (ok; allowedKeys)
            if (ok == key)
            {
                allowed = true;
                break;
            }
        if (!allowed)
            p.affinityExcluded = true;
    }
}

unittest
{
    // C1: syntax, order, duplicates and errors.
    alias P = ProcessorId;
    assert(parseCpuList("0-3,8") == [P(0, 0), P(0, 1), P(0, 2), P(0, 3), P(0, 8)]);
    assert(parseCpuList("1:0-1, 0:5") == [P(1, 0), P(1, 1), P(0, 5)]);
    assert(parseCpuList("3,1,3") == [P(0, 3), P(0, 1), P(0, 3)]);
    assert(parseCpuList("").length == 0);
    assert(parseCpuList(" ").length == 0);
    assert(parseCpuList("65535") == [P(0, 65535)]);

    size_t offsetOf(string spec)
    {
        try
            cast(void) parseCpuList(spec);
        catch (CpuListException e)
            return e.offset;
        assert(0, "expected CpuListException for '" ~ spec ~ "'");
    }

    assert(offsetOf("3-1") == 0);
    assert(offsetOf("a") == 0);
    assert(offsetOf("1:") == 0);
    assert(offsetOf("0-") == 0);
    assert(offsetOf("65536") == 0);
    assert(offsetOf("1:2:3") == 0);
    assert(offsetOf("0, 3-1") == 3);
    assert(offsetOf("0,1,,2") == 4);
    assert(offsetOf("0-7,16-23, 1:x") == 11);
}

unittest
{
    // C2: format/parse round trip for sorted, duplicate-free sets.
    alias P = ProcessorId;
    auto g0 = parseCpuList("0-7,16-23");
    assert(formatCpuList(g0) == "0-7,16-23");
    assert(parseCpuList(formatCpuList(g0)) == g0);

    auto mixed = [P(0, 0), P(0, 2), P(0, 3), P(1, 0), P(1, 1), P(1, 5), P(2, 7)];
    assert(formatCpuList(mixed) == "0,2-3,1:0-1,1:5,2:7");
    assert(parseCpuList(formatCpuList(mixed)) == mixed);
    assert(formatCpuList(null) == "");

    import std.format : format;
    assert(format("%s", P(1, 12)) == "1:12");
    assert(P(0, 9) < P(1, 0) && P(1, 0) < P(1, 1));
}

unittest
{
    // C3: cores are sorted by their first processor, whatever the input order.
    alias P = ProcessorId;
    LogicalProcessor[] procs;
    foreach_reverse (i; 0 .. 32)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort)(i % 16);
        p.llcIndex = cast(ushort)(i % 16 / 8);
        p.smtSibling = i >= 16;
        procs ~= p;
    }
    auto snap = assembleSnapshot("synthetic", procs, null, null, 2, 64,
        [GroupingSource.l3, GroupingSource.l3]);
    assert(snap.cores.length == 16);
    foreach (i, ref c; snap.cores)
    {
        assert(c.coreIndex == i);
        assert(c.processors == [P(0, cast(ushort) i), P(0, cast(ushort)(i + 16))]);
    }
}
