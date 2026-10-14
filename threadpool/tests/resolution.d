module threadpool.tests.resolution;

// Pure resolution tests on synthetic snapshots; no OS calls, any host.
// Inside package threadpool so the package-only narrowedTo is reachable.

import std.algorithm : count, canFind;
import std.array : array;
import std.string : lineSplitter;

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

/// Resolve with the fixture's unrestricted scope: exclusion comes from snapshot flags.
private PoolResolution resolveOf(ref const TopologySnapshot snap, PoolOptions opt)
{
    return resolvePool(snap, opt);
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
    snap.osScopeSource = OsScopeSource.processLaunch;
    snap.osScopeAllowed = parseCpuList("0-7,16-23");
    PoolOptions opt;
    auto res = resolvePool(snap, opt);
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
    assert(res.topology.osScopeSource == OsScopeSource.processLaunch);
    assert(res.topology.osScopeAllowed == parseCpuList("0-7,16-23"));
    assert(res.summary.canFind("os scope processLaunch {0-7,16-23}"), res.summary);

    // The scope alone excludes, even when the snapshot carries no flags.
    auto unflagged = f1();
    unflagged.osScopeSource = OsScopeSource.processLaunch;
    unflagged.osScopeAllowed = parseCpuList("0-7,16-23");
    auto byScope = resolvePool(unflagged, opt);
    assert(byScope.workers == res.workers);
    assert(byScope.groups[1].common == Reject.osExcluded);
    assert(byScope.groups[1].eligible == 0);
    assert(unflagged.inOsScope(ProcessorId(0, 0)) && !unflagged.inOsScope(ProcessorId(0, 8)));
    assert(unflagged.inOsScope(ProcessorId(0, 23)) && !unflagged.inOsScope(ProcessorId(0, 999)));
    assert(!unflagged.isEligible(*unflagged.byId(ProcessorId(0, 8))));
    assert(unflagged.eligibleProcessorsInLlc(1).length == 0 && unflagged.eligibleLlcCount == 1);
    assert(f1().inOsScope(ProcessorId(0, 999)), "unrestricted admits unknown ids");
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

unittest
{
    // R9: narrowing keeps one disposition per CPU, request and group;
    // notSelected is exclusive and separates policy from filters.
    auto snap = f1((ushort lp) => lp == 3);
    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.processors = parseCpuList("0-3,9,999");
    auto full = resolvePool(snap, opt);
    assert(full.workers == parseCpuList("0-2,9"));
    foreach (ref c; full.cpus)
        assert(!(c.reasons & Reject.notSelected), "resolvePool never sets notSelected");
    foreach (ref g; full.groups)
        assert(g.candidates == g.accepted);

    auto narrow = full.narrowedTo(parseCpuList("2,0"));
    assert(narrow.workers == parseCpuList("0,2"), "snapshot order, not chosen order");
    assert(narrow[ProcessorId(0, 0)].workerIndex == 0 && narrow[ProcessorId(0, 2)].workerIndex == 1);
    assert(narrow[ProcessorId(0, 1)].reasons == Reject.notSelected);
    assert(narrow[ProcessorId(0, 9)].reasons == Reject.notSelected);
    assert(narrow[ProcessorId(0, 1)].workerIndex == uint.max);
    assert(narrow[ProcessorId(0, 3)].reasons == full[ProcessorId(0, 3)].reasons, "filters kept");
    assert(narrow.cpus.length == full.cpus.length);
    foreach (ref c; narrow.cpus)
    {
        assert(c.reasons == Reject.notSelected || !(c.reasons & Reject.notSelected));
        assert(((c.reasons & ~Reject.notSelected) == Reject.none) == full.isAccepted(c.id));
    }
    assert(narrow.rejectedCpus(Reject.notSelected).array.length == 2);
    assert(narrow.rejectedCpus(cast(Reject)(Reject.all & ~Reject.notSelected)).array.length
        == full.rejectedCpus.array.length);

    // Requests stay the user's filter list.
    assert(narrow.requests.length == full.requests.length);
    assert(narrow.requests[0].accepted && !narrow.requests[1].accepted);
    assert(narrow.requests[4].known && !narrow.requests[4].accepted);
    assert(narrow.requests[5].unknown);
    bool threw;
    try
        narrow.enforceRequested();
    catch (ResolutionException e)
        threw = e.msg.canFind("0:1 rejected (notSelected)") && e.msg.canFind("0:9 rejected (notSelected)");
    assert(threw);

    // Group 1 passed the filters but supplies no worker.
    assert(narrow.groups[0].candidates == 3 && narrow.groups[0].accepted == 2);
    assert(narrow.groups[1].candidates == 1 && narrow.groups[1].accepted == 0);
    assert(narrow.groups[1].common == Reject.none, "mixed notSelected and filter reasons");
    assert(narrow.acceptedGroupCount == 1 && narrow.workersIn(1).length == 0);
    assert(reasonsText(Reject.notSelected) == "notSelected");
    import std.conv : to;
    assert(narrow.to!string.canFind("candidates"));

    // Nothing chosen: every filter-passing CPU is notSelected.
    auto none = full.narrowedTo(null);
    assert(none.workers.length == 0);
    assert(none.rejectedCpus(Reject.notSelected).array.length == full.workers.length);
    assert(none.groups[1].common == Reject.none && none.groups[1].candidates == 1);

    // A group whose every CPU is a candidate but none chosen.
    auto homogeneous = f1();
    auto plain = resolvePool(homogeneous, PoolOptions.init);
    auto home = plain.narrowedTo(plain.workersIn(0));
    assert(home.groups[1].candidates == 16 && home.groups[1].common == Reject.notSelected);
}
