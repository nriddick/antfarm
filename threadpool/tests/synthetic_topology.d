module synthetic_topology;

// Synthetic topology builders shared by the pure resolution and host-plan
// tests; no OS calls, any host.

import threadpool.topology;

/// One logical processor; `smt` marks a non-primary sibling.
LogicalProcessor lp(ushort group, ushort lpIndex, ushort core, ushort llc,
    bool smt = false, ubyte eff = 0)
{
    LogicalProcessor p;
    p.group = group;
    p.lpIndex = lpIndex;
    p.coreIndex = core;
    p.llcIndex = llc;
    p.smtSibling = smt;
    p.efficiencyClass = eff;
    return p;
}

TopologySnapshot assemble(LogicalProcessor[] procs, ushort llcCount)
{
    auto sources = new GroupingSource[](llcCount);
    return assembleSnapshot("synthetic", procs, null, null, llcCount, 64, sources);
}

/// 5900XT shape: 32 LPs, core = lp % 16, LLC = core / 8, SMT sibling N+16.
TopologySnapshot f1(bool delegate(ushort lp) excluded = null,
    bool delegate(ushort lp) offline = null)
{
    LogicalProcessor[] procs;
    foreach (i; 0 .. 32)
    {
        auto core = cast(ushort)(i % 16);
        auto p = lp(0, cast(ushort) i, core, cast(ushort)(core / 8), i >= 16);
        if (excluded !is null) p.affinityExcluded = excluded(p.lpIndex);
        if (offline !is null) p.offlineAtDiscovery = offline(p.lpIndex);
        procs ~= p;
    }
    return assemble(procs, 2);
}

/// Two Windows processor groups with the same LP numbers, one LLC each.
TopologySnapshot twoGroups()
{
    LogicalProcessor[] procs;
    foreach (g; 0 .. 2)
        foreach (i; 0 .. 4)
            procs ~= lp(cast(ushort) g, cast(ushort) i, cast(ushort) i, cast(ushort) g);
    return assemble(procs, 2);
}
