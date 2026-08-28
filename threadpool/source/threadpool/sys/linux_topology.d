module threadpool.sys.linux_topology;

version (linux):

import std.algorithm : sort;
import std.conv : to;
import std.exception : enforce;
import std.file : dirEntries, exists, isDir, readText, SpanMode;
import std.path : baseName, buildPath;
import std.string : split, strip;

import threadpool.topology;

/// Linux cpulist: "0-11", "0,6", "0-3,8-11". Empty / missing → no CPUs.
ushort[] parseLinuxCpuList(const(char)[] s)
{
    ushort[] r;
    if (s.length == 0) return r;
    foreach (part; s.split(','))
    {
        auto t = part.strip;
        if (t.length == 0) continue;
        auto dash = indexOfDash(t);
        if (dash < 0)
        {
            r ~= toU16(t);
            continue;
        }
        auto a = toU32(t[0 .. dash]);
        auto b = toU32(t[dash + 1 .. $]);
        if (a > b)
        {
            auto tmp = a;
            a = b;
            b = tmp;
        }
        foreach (v; a .. b + 1)
        {
            if (v > ushort.max) break;
            r ~= cast(ushort) v;
        }
    }
    return r;
}

/// sysfs cache `size`: "32K", "16384K", "16M", or a bare byte count.
uint parseLinuxCacheSize(const(char)[] s)
{
    s = s.strip;
    if (s.length == 0) return 0;
    ulong mul = 1;
    auto end = s.length;
    auto last = s[$-1];
    if (last == 'K' || last == 'k')
    {
        mul = 1024;
        end--;
    }
    else if (last == 'M' || last == 'm')
    {
        mul = 1024 * 1024;
        end--;
    }
    else if (last == 'G' || last == 'g')
    {
        mul = 1024UL * 1024 * 1024;
        end--;
    }
    auto n = toU64(s[0 .. end].strip);
    auto bytes = n * mul;
    if (bytes > uint.max) return uint.max;
    return cast(uint) bytes;
}

/// Parse a sysfs-shaped tree. `cpuRoot` contains `cpuN/`, `present`, `online`.
/// `nodeRoot` is `.../node` (`nodeN/cpulist`); optional.
/// `cpuCoreList` / `cpuAtomList` are Intel hybrid cpulist files (optional).
TopologySnapshot parseSysfsTopology(
    string cpuRoot,
    string nodeRoot = null,
    string cpuCoreList = null,
    string cpuAtomList = null)
{
    auto cpus = enumerateCpus(cpuRoot);
    enforce(cpus.length > 0, "threadpool: no CPUs found under " ~ cpuRoot);

    ushort[ushort] numaOf;
    fillNuma(nodeRoot, cpuRoot, cpus, numaOf);

    bool[ushort] listedP;
    bool[ushort] listedE;
    if (cpuCoreList.length)
        foreach (c; parseLinuxCpuList(readTrim(cpuCoreList)))
            listedP[c] = true;
    if (cpuAtomList.length)
        foreach (c; parseLinuxCpuList(readTrim(cpuAtomList)))
            listedE[c] = true;
    const useHybridLists = listedP.length > 0 || listedE.length > 0;

    SysfsCpu[] raws;
    raws.length = cpus.length;
    ushort maxLine;

    foreach (i, cpu; cpus)
    {
        auto dir = buildPath(cpuRoot, "cpu" ~ to!string(cpu));
        SysfsCpu r;
        r.cpu = cpu;
        r.online = cpuIsOnline(cpuRoot, cpu);
        r.core = parseU16(readTrim(buildPath(dir, "topology", "core_id")), cpu);
        r.pkg = parseU16(readTrim(buildPath(dir, "topology", "physical_package_id")), 0);
        r.siblings = parseLinuxCpuList(readTrim(buildPath(dir, "topology", "thread_siblings_list")));
        if (r.siblings.length == 0)
            r.siblings = [cpu];
        if (auto cap = readTrim(buildPath(dir, "cpu_capacity")))
        {
            r.hasCapacity = true;
            r.capacity = parseU32(cap);
        }
        readCaches(dir, r);
        if (r.maxLine > maxLine) maxLine = r.maxLine;
        raws[i] = r;
    }

    uint maxCap;
    bool mixedCap;
    foreach (ref r; raws)
        if (r.hasCapacity && r.capacity > maxCap)
            maxCap = r.capacity;
    if (maxCap > 0)
        foreach (ref r; raws)
            if (r.hasCapacity && r.capacity < maxCap)
                mixedCap = true;

    LogicalProcessor[] procs;
    procs.length = raws.length;
    size_t[ushort] idxOf;
    foreach (i, ref r; raws)
    {
        LogicalProcessor p;
        p.group = 0;
        p.lpIndex = r.cpu;
        p.cpuSetId = 0;
        p.coreIndex = r.core;
        p.packageIndex = r.pkg;
        p.numaIndex = numaOf.get(r.cpu, cast(ushort) 0);
        p.moduleIndex = ushort.max;
        p.parkedAtDiscovery = !r.online;
        p.l1d = r.l1d;
        p.l1i = r.l1i;
        p.l2 = r.l2;
        p.l3 = r.l3;
        p.efficiencyClass = classOf(r.cpu, r.hasCapacity, r.capacity, maxCap,
            mixedCap, useHybridLists, listedP, listedE);
        procs[i] = p;
        idxOf[r.cpu] = i;
    }

    // SMT: lowest lpIndex in thread_siblings is the primary; the rest are siblings.
    bool[ushort] smtMarked;
    foreach (ref r; raws)
    {
        ushort[] live;
        foreach (s; r.siblings)
            if (s in idxOf)
                live ~= s;
        live.sort();
        if (live.length <= 1) continue;
        foreach (n, s; live)
        {
            if (s in smtMarked) continue;
            smtMarked[s] = true;
            procs[idxOf[s]].smtSibling = n != 0;
        }
    }

    // Process-wide llcIndex: one dense id per distinct L3 sharing set.
    // Fall back to L2 share, then a singleton, so every LP gets a shard.
    ushort nextLlc;
    ushort[string] llcOfSet;
    foreach (ref r; raws)
    {
        auto share = r.hasL3 ? r.l3Share : (r.hasL2 ? r.l2Share : [r.cpu]);
        share = uniqueSorted(share);
        auto k = setKey(share);
        if (k !in llcOfSet)
            llcOfSet[k] = nextLlc++;
        auto lp = &procs[idxOf[r.cpu]];
        lp.llcIndex = llcOfSet[k];
        lp.llcIndexInGroup = r.hasL3 ? r.l3Id : lp.llcIndex;
        lp.l1d.llcIndex = lp.llcIndex;
        lp.l1i.llcIndex = lp.llcIndex;
        lp.l2.llcIndex = lp.llcIndex;
        lp.l3.llcIndex = lp.llcIndex;
    }

    ushort nextMod;
    ushort[string] modOfSet;
    L2Cluster[] clusters;
    foreach (ref r; raws)
    {
        if (!r.hasL2) continue;
        auto share = uniqueSorted(r.l2Share);
        if (share.length == 0) continue;
        auto k = setKey(share);
        if (k in modOfSet)
        {
            procs[idxOf[r.cpu]].moduleIndex = modOfSet[k];
            continue;
        }
        auto mid = nextMod++;
        modOfSet[k] = mid;
        L2Cluster c;
        c.moduleIndex = mid;
        c.l2SizeBytes = r.l2.sizeBytes;
        foreach (s; share)
        {
            if (s !in idxOf) continue;
            procs[idxOf[s]].moduleIndex = mid;
            c.lpIndices ~= s;
            c.llcIndex = procs[idxOf[s]].llcIndex;
        }
        c.lpIndices.sort();
        clusters ~= c;
    }

    ProcGroup pg;
    pg.group = 0;
    ushort maxCpu;
    ushort active;
    ulong mask;
    foreach (ref p; procs)
    {
        if (p.lpIndex > maxCpu) maxCpu = p.lpIndex;
        if (p.parkedAtDiscovery) continue;
        active++;
        if (p.lpIndex < 64)
            mask |= 1UL << p.lpIndex;
    }
    pg.maxProcessors = cast(ushort)(maxCpu + 1);
    pg.activeProcessors = active;
    pg.activeMask = mask;

    return assembleSnapshot("linux", procs, clusters, [pg], nextLlc, maxLine);
}

TopologySnapshot discoverLinux()
{
    return parseSysfsTopology(
        "/sys/devices/system/cpu",
        "/sys/devices/system/node",
        "/sys/devices/cpu_core/cpus",
        "/sys/devices/cpu_atom/cpus");
}

private int indexOfDash(const(char)[] s)
{
    foreach (i, ch; s)
        if (ch == '-')
            return cast(int) i;
    return -1;
}

private ushort toU16(const(char)[] s)
{
    return to!ushort(s.strip);
}

private uint toU32(const(char)[] s)
{
    return to!uint(s.strip);
}

private ulong toU64(const(char)[] s)
{
    return to!ulong(s.strip);
}

private string readTrim(string path)
{
    if (path.length == 0 || !exists(path)) return null;
    try
        return readText(path).strip;
    catch (Exception)
        return null;
}

private ushort parseU16(string s, ushort def)
{
    if (s.length == 0) return def;
    try
        return to!ushort(s);
    catch (Exception)
        return def;
}

private uint parseU32(string s)
{
    if (s.length == 0) return 0;
    try
        return to!uint(s);
    catch (Exception)
        return 0;
}

private ushort[] enumerateCpus(string cpuRoot)
{
    if (auto s = readTrim(buildPath(cpuRoot, "present")))
    {
        auto cpus = uniqueSorted(parseLinuxCpuList(s));
        if (cpus.length) return cpus;
    }
    if (auto s = readTrim(buildPath(cpuRoot, "possible")))
    {
        auto cpus = uniqueSorted(parseLinuxCpuList(s));
        if (cpus.length) return cpus;
    }
    if (auto s = readTrim(buildPath(cpuRoot, "online")))
    {
        auto cpus = uniqueSorted(parseLinuxCpuList(s));
        if (cpus.length) return cpus;
    }

    ushort[] cpus;
    if (!exists(cpuRoot) || !isDir(cpuRoot)) return cpus;
    foreach (e; dirEntries(cpuRoot, SpanMode.shallow))
    {
        if (!e.isDir) continue;
        auto name = baseName(e.name);
        if (name.length < 4 || name[0 .. 3] != "cpu") continue;
        auto rest = name[3 .. $];
        if (rest.length == 0) continue;
        bool digits = true;
        foreach (ch; rest)
            if (ch < '0' || ch > '9') { digits = false; break; }
        if (!digits) continue;
        try
            cpus ~= to!ushort(rest);
        catch (Exception) {}
    }
    return uniqueSorted(cpus);
}

/// cpu0 usually has no `online` file and is always online.
private bool cpuIsOnline(string cpuRoot, ushort cpu)
{
    auto p = buildPath(cpuRoot, "cpu" ~ to!string(cpu), "online");
    if (!exists(p)) return true;
    auto s = readTrim(p);
    return s != "0";
}

/// Prefer nodeN/cpulist. `cpuN/numa_node` is often missing; fall back to a
/// `cpuN/nodeM` symlink, then node 0.
private void fillNuma(string nodeRoot, string cpuRoot, ushort[] cpus,
                      ref ushort[ushort] numaOf)
{
    if (nodeRoot.length && exists(nodeRoot))
    {
        ushort[] nodes;
        if (auto s = readTrim(buildPath(nodeRoot, "online")))
            nodes = parseLinuxCpuList(s);
        if (nodes.length == 0 && exists(nodeRoot) && isDir(nodeRoot))
        {
            foreach (e; dirEntries(nodeRoot, SpanMode.shallow))
            {
                if (!e.isDir) continue;
                auto name = baseName(e.name);
                if (name.length < 5 || name[0 .. 4] != "node") continue;
                auto rest = name[4 .. $];
                bool digits = true;
                foreach (ch; rest)
                    if (ch < '0' || ch > '9') { digits = false; break; }
                if (digits)
                {
                    try
                        nodes ~= to!ushort(rest);
                    catch (Exception) {}
                }
            }
        }
        foreach (n; nodes)
        {
            auto list = readTrim(buildPath(nodeRoot, "node" ~ to!string(n), "cpulist"));
            foreach (c; parseLinuxCpuList(list))
                numaOf[c] = n;
        }
        if (numaOf.length) return;
    }

    foreach (c; cpus)
    {
        auto dir = buildPath(cpuRoot, "cpu" ~ to!string(c));
        if (auto s = readTrim(buildPath(dir, "numa_node")))
        {
            if (s.length && s[0] != '-')
            {
                try
                {
                    numaOf[c] = to!ushort(s);
                    continue;
                }
                catch (Exception) {}
            }
        }
        if (!exists(dir) || !isDir(dir)) continue;
        try
        {
            foreach (e; dirEntries(dir, SpanMode.shallow))
            {
                auto name = baseName(e.name);
                if (name.length < 5 || name[0 .. 4] != "node") continue;
                auto rest = name[4 .. $];
                bool digits = true;
                foreach (ch; rest)
                    if (ch < '0' || ch > '9') { digits = false; break; }
                if (!digits) continue;
                try
                    numaOf[c] = to!ushort(rest);
                catch (Exception) {}
                break;
            }
        }
        catch (Exception) {}
    }
}

private void readCaches(string cpuDir, ref SysfsCpu r)
{
    auto cacheDir = buildPath(cpuDir, "cache");
    if (!exists(cacheDir) || !isDir(cacheDir)) return;
    foreach (e; dirEntries(cacheDir, SpanMode.shallow))
    {
        if (!e.isDir) continue;
        auto name = baseName(e.name);
        if (name.length < 6 || name[0 .. 5] != "index") continue;

        auto level = parseU16(readTrim(buildPath(e.name, "level")), 0);
        auto typ = readTrim(buildPath(e.name, "type"));
        auto size = parseLinuxCacheSize(readTrim(buildPath(e.name, "size")));
        auto line = parseU16(readTrim(buildPath(e.name, "coherency_line_size")), 0);
        auto ways = parseU16(readTrim(buildPath(e.name, "ways_of_associativity")), 0);
        auto share = parseLinuxCpuList(readTrim(buildPath(e.name, "shared_cpu_list")));
        auto id = parseU16(readTrim(buildPath(e.name, "id")), 0);
        if (line > r.maxLine) r.maxLine = line;
        if (share.length == 0)
            share = [r.cpu];

        CacheKind kind = CacheKind.other;
        if (level == 1 && typ == "Data")
            kind = CacheKind.l1d;
        else if (level == 1 && typ == "Instruction")
            kind = CacheKind.l1i;
        else if (level == 1 && typ == "Unified")
            kind = CacheKind.l1d;
        else if (level == 2)
            kind = CacheKind.l2;
        else if (level == 3)
            kind = CacheKind.l3;

        CacheInfo info;
        info.kind = kind;
        info.level = cast(ubyte) level;
        info.lineSize = line;
        info.sizeBytes = size;
        info.associativity = ways;

        final switch (kind)
        {
        case CacheKind.l1d: r.l1d = info; break;
        case CacheKind.l1i: r.l1i = info; break;
        case CacheKind.l2:
            r.l2 = info;
            r.l2Share = share;
            r.hasL2 = true;
            break;
        case CacheKind.l3:
            r.l3 = info;
            r.l3Share = share;
            r.l3Id = id;
            r.hasL3 = true;
            break;
        case CacheKind.other:
            break;
        }
    }
}

private struct SysfsCpu
{
    ushort cpu;
    ushort core;
    ushort pkg;
    ushort[] siblings;
    uint capacity;
    bool hasCapacity;
    bool online;
    CacheInfo l1d, l1i, l2, l3;
    ushort[] l2Share;
    ushort[] l3Share;
    ushort l3Id;
    bool hasL2;
    bool hasL3;
    ushort maxLine;
}

private ubyte classOf(
    ushort cpu, bool hasCapacity, uint capacity, uint maxCap, bool mixedCap,
    bool useHybridLists, const bool[ushort] listedP, const bool[ushort] listedE)
{
    if (useHybridLists)
    {
        if (cpu in listedP) return 1;
        if (cpu in listedE) return 0;

        // Kernels normally expose both cpu_core and cpu_atom. If only one is
        // present, treat the unlisted CPUs as the other class rather than
        // incorrectly defaulting every unlisted CPU to E.
        if (listedP.length > 0 && listedE.length == 0)
            return 0;
        if (listedP.length == 0 && listedE.length > 0)
            return 1;
    }
    if (mixedCap && hasCapacity)
        return capacity >= maxCap ? 1 : 0;
    return 0;
}

private ushort[] uniqueSorted(ushort[] xs)
{
    if (xs.length == 0) return xs;
    xs.sort();
    size_t w = 1;
    foreach (i; 1 .. xs.length)
        if (xs[i] != xs[w - 1])
            xs[w++] = xs[i];
    xs.length = w;
    return xs;
}

private string setKey(const(ushort)[] cpus)
{
    import std.array : appender;
    import std.format : formattedWrite;

    auto ap = appender!string;
    foreach (i, c; cpus)
    {
        if (i) ap.put(',');
        formattedWrite(ap, "%s", c);
    }
    return ap.data;
}
