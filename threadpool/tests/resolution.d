module resolution_tests;

// Pure resolution tests on synthetic snapshots; no OS calls, any host.

import std.algorithm : count, canFind;
import std.array : array;
import std.string : lineSplitter;

import threadpool.os_scope : OsScope, OsScopeSource;
import threadpool.pool : PoolOptions;
import threadpool.resolution;
import threadpool.topology;
import synthetic_topology;

/// Hybrid: 2 SMT P-cores (LPs 0-3) and 4 E-cores (LPs 4-7), one LLC.
private TopologySnapshot hybrid()
{
    LogicalProcessor[] procs;
    foreach (i; 0 .. 4)
        procs ~= lp(0, cast(ushort) i, cast(ushort)(i / 2), 0, i % 2 == 1, 1);
    foreach (i; 4 .. 8)
        procs ~= lp(0, cast(ushort) i, cast(ushort)(i - 2), 0, false, 0);
    return assemble(procs, 1);
}

/// Resolve with an unrestricted scope: exclusion comes from snapshot flags.
private PoolResolution resolveOf(ref const TopologySnapshot snap, PoolOptions opt)
{
    OsScope unrestricted;
    return resolvePool(snap, unrestricted, opt);
}

/// The pre-resolution `selectLps` (onlyProcessors branch; onlyLps empty), kept
/// as the R7 oracle.
private ProcessorId[] oracleSelect(ref const TopologySnapshot snap, ref const PoolOptions opt,
    const(ProcessorId)[] onlyProcessors)
{
    ProcessorId[] selected;
    foreach (ref lp; snap.processors)
    {
        if (lp.affinityExcluded) continue;
        if (onlyProcessors.length)
        {
            bool wanted;
            foreach (v; onlyProcessors)
                if (v.group == lp.group && v.lp == lp.lpIndex) { wanted = true; break; }
            if (!wanted) continue;
        }
        auto tc = tableClassIndex(lp.efficiencyClass, snap.maxEfficiencyClass, snap.classCount);
        if (snap.classCount <= 1)
        {
            if (!opt.enablePCores) continue;
        }
        else
        {
            if (tc == classP && !opt.enablePCores) continue;
            if (tc == classE && !opt.enableECores) continue;
        }
        if (opt.skipSmtSiblings && lp.smtSibling) continue;
        if (lp.offlineAtDiscovery) continue;
        selected ~= ProcessorId.of(lp);
    }
    return selected;
}

unittest
{
    // R1: an unknown id is reported and adds nothing.
    auto snap = f1();
    PoolOptions opt;
    opt.processors = parseCpuList("999,0");
    auto res = resolveOf(snap, opt);
    assert(res.requests.length == 2);
    assert(res.requests[0].unknown && !res.requests[0].accepted);
    assert(res.requests[1].known && res.requests[1].accepted);
    assert(res.workers == [ProcessorId(0, 0)]);
    assert(res.unknownRequests.array.length == 1);

    bool threw;
    try
        res.enforceRequested();
    catch (ResolutionException e)
    {
        threw = true;
        assert(e.msg.canFind("0:999 unknown"));
        assert(e.resolution is res);
    }
    assert(threw);
}

unittest
{
    // R2: a requested CPU outside the OS scope is osExcluded, not notRequested.
    auto snap = f1((ushort lp) => lp == 8);
    PoolOptions opt;
    opt.processors = parseCpuList("8,0");
    auto res = resolveOf(snap, opt);
    assert(res.requests[0].known && !res.requests[0].accepted);
    assert(res[ProcessorId(0, 8)].reasons & Reject.osExcluded);
    assert(!(res[ProcessorId(0, 8)].reasons & Reject.notRequested));
    assert(res[ProcessorId(0, 1)].reasons == Reject.notRequested);
    assert(res.workers == [ProcessorId(0, 0)]);

    bool threw;
    try
        res.enforceRequested();
    catch (ResolutionException e)
    {
        threw = true;
        assert(e.msg.canFind("0:8 rejected (osExcluded)"));
    }
    assert(threw);
}

unittest
{
    // R3: duplicates collapse to one worker and are flagged.
    auto snap = f1();
    PoolOptions opt;
    opt.processors = parseCpuList("0,0");
    auto res = resolveOf(snap, opt);
    assert(!res.requests[0].duplicate && res.requests[1].duplicate);
    assert(res.requests[1].known && res.requests[1].accepted);
    assert(res.workers == [ProcessorId(0, 0)]);
    bool threw;
    try
        res.enforceRequested();
    catch (ResolutionException e)
        threw = e.msg.canFind("duplicate");
    assert(threw);
}

unittest
{
    // R4: group-qualified requests never alias the same LP in another group.
    auto snap = twoGroups();
    PoolOptions opt;
    opt.processors = [ProcessorId(0, 0)];
    auto res = resolveOf(snap, opt);
    assert(res.workers == [ProcessorId(0, 0)]);
    assert(res[ProcessorId(1, 0)].reasons == Reject.notRequested);

    opt.processors = parseCpuList("1:0");
    res = resolveOf(snap, opt);
    assert(res.workers == [ProcessorId(1, 0)]);
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(res.workersIn(1) == [ProcessorId(1, 0)]);
    assert(res.workersIn(0).length == 0);
}

unittest
{
    // R5: every applicable reason is recorded.
    auto snap = f1((ushort lp) => lp >= 8);
    PoolOptions opt;
    opt.skipSmtSiblings = true;
    auto res = resolveOf(snap, opt);
    assert(res[ProcessorId(0, 16)].reasons == (Reject.osExcluded | Reject.smtSecondary));
    assert(res[ProcessorId(0, 0)].accepted);

    PoolOptions noP;
    noP.enablePCores = false;
    auto homogeneous = f1();
    res = resolveOf(homogeneous, noP);
    assert(res.workers.length == 0);
    foreach (ref c; res.cpus)
        assert(c.reasons == Reject.classDisabled);

    auto h = hybrid();
    PoolOptions noE;
    noE.enableECores = false;
    res = resolveOf(h, noE);
    assert(res.workers == parseCpuList("0-3"));
    assert(res[ProcessorId(0, 4)].reasons == Reject.classDisabled);
    assert(res[ProcessorId(0, 4)].tableClass == classE);
}

unittest
{
    // R6: group disposition on two-LLC hardware with LLC1 excluded.
    auto snap = f1((ushort lp) => (lp % 16) >= 8);
    PoolOptions opt;
    auto launch = OsScope(OsScopeSource.processLaunch, parseCpuList("0-7,16-23"));
    auto res = resolvePool(snap, launch, opt);
    assert(res.physicalGroupCount == 2);
    assert(res.acceptedGroupCount == 1);
    assert(res.groups[0].physical == 16 && res.groups[0].eligible == 16
        && res.groups[0].accepted == 16 && res.groups[0].common == Reject.none);
    assert(res.groups[1].physical == 16 && res.groups[1].eligible == 0
        && res.groups[1].accepted == 0);
    assert(res.groups[1].common == Reject.osExcluded);
    ushort[] accepted;
    foreach (g; res.acceptedGroups) accepted ~= g.llcIndex;
    assert(accepted == [ushort(0)]);
    assert(res.rejectedGroups.array.length == 1);
    assert(res.workersIn(0) == parseCpuList("0-7,16-23"));
    assert(res.rejectedCpus(Reject.osExcluded).array.length == 16);
    assert(res.acceptedCpus.array.length == 16);
    assert(!res.started);
    assert(res.osScope.source == OsScopeSource.processLaunch);
    assert(res.osScope.allowed == parseCpuList("0-7,16-23"));

    // The scope alone excludes, even when the snapshot carries no flags.
    auto unflagged = f1();
    auto byScope = resolvePool(unflagged, launch, opt);
    assert(byScope.workers == res.workers);
    assert(byScope.groups[1].common == Reject.osExcluded);
}

unittest
{
    // R7: selection equals the pre-resolution rule for every non-onlyLps option.
    auto snaps = [
        f1(),
        f1((ushort lp) => (lp % 16) >= 8),
        twoGroups(),
        f1(null, (ushort lp) => lp == 5 || lp == 21),
        hybrid(),
    ];
    const(ProcessorId)[][] lists = [
        null,
        parseCpuList("0,1,8,16"),
        parseCpuList("1:0,0:0,1:3"),
        parseCpuList("999,5,5,21"),
    ];
    size_t cases;
    foreach (ref snap; snaps)
        foreach (list; lists)
            foreach (skip; [false, true])
                foreach (p; [false, true])
                    foreach (e; [false, true])
                    {
                        PoolOptions opt;
                        opt.skipSmtSiblings = skip;
                        opt.enablePCores = p;
                        opt.enableECores = e;
                        opt.processors = list;
                        auto res = resolveOf(snap, opt);
                        assert(res.workers == oracleSelect(snap, opt, list));
                        foreach (ref c; res.cpus)
                            assert(c.accepted == (c.workerIndex != uint.max));
                        cases++;
                    }
    assert(cases == 5 * 4 * 8);
}

unittest
{
    // R8: the report prints every group and every CPU.
    auto snap = f1((ushort lp) => (lp % 16) >= 8);
    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.processors = parseCpuList("0-3,3,999");
    auto res = resolveOf(snap, opt);
    import std.conv : to;
    auto text = res.to!string;
    auto lines = text.lineSplitter.array;
    size_t cpuRows, groupRows;
    foreach (l; lines)
    {
        if (l.length > 4 && l[0 .. 4] == "  0:") cpuRows++;
        if (l.length > 3 && (l[0 .. 3] == "  0" || l[0 .. 3] == "  1") && l[3] == ' ')
            groupRows++;
    }
    assert(cpuRows == 32, text);
    assert(groupRows == 2, text);
    assert(text.canFind("requests: 0-3,3,999"));
    assert(text.canFind("0:999: unknown"));
    assert(text.canFind("0:3: duplicate"));
    assert(text.canFind("osExcluded,notRequested,smtSecondary"));
    assert(res.summary.canFind("4 accepted"));
    assert(res.summary.count('\n') == 0);
}
