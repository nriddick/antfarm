module threadpool.tests.resolution;

// Pure resolution tests on synthetic snapshots; no OS calls, any host.
// Inside package threadpool so the package-only narrowedTo is reachable.

import std.algorithm : count, canFind;
import std.array : array;
import std.string : lineSplitter;

import threadpool.pin : WorkerPlacement;
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

/// The accepted CPUs (every set's processors) in snapshot order.
private ProcessorId[] acceptedOf(const PoolResolution res)
{
    ProcessorId[] ids;
    foreach (ref c; res.cpus)
        if (c.accepted) ids ~= c.id;
    return ids;
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
    assert(acceptedOf(res) == [ProcessorId(0, 0)] && res.workerCount == 1);
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
    assert(acceptedOf(res) == [ProcessorId(0, 0)] && res.workerCount == 1);

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
    // R3: duplicates collapse to one CPU and are flagged.
    auto snap = f1();
    PoolOptions opt;
    opt.processors = parseCpuList("0,0");
    auto res = resolveOf(snap, opt);
    assert(!res.requests[0].duplicate && res.requests[1].duplicate);
    assert(res.requests[1].known && res.requests[1].accepted);
    assert(acceptedOf(res) == [ProcessorId(0, 0)] && res.workerCount == 1);
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
    assert(acceptedOf(res) == [ProcessorId(0, 0)]);
    assert(res[ProcessorId(1, 0)].reasons == Reject.notRequested);

    opt.processors = parseCpuList("1:0");
    res = resolveOf(snap, opt);
    assert(acceptedOf(res) == [ProcessorId(1, 0)]);
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);
    assert(res.processorsIn(1) == [ProcessorId(1, 0)] && res.workerCountIn(1) == 1);
    assert(res.processorsIn(0).length == 0 && res.workerCountIn(0) == 0);
    assert(res.setOf(0) is null && res.sets.length == 1 && res.sets[0].llcIndex == 1);
    assert(res.workers.length == 1 && res.workers[0].llcIndex == 1 && res.workers[0].ordinal == 0);
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
    assert(acceptedOf(res) == parseCpuList("0-3"));
    assert(res.workerCount == 3, "a set of four leaves one CPU unstaffed");
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
    assert(res.groups[0].workers == 14 && res.workerCount == 14, "sixteen CPUs leave two unstaffed");
    assert(res.sets.length == 1 && res.sets[0].unstaffed == 2 && res.sets[0].firstWorker == 0);
    assert(res.groups[1].physical == 16 && res.groups[1].eligible == 0
        && res.groups[1].accepted == 0);
    assert(res.groups[1].common == Reject.osExcluded);
    ushort[] accepted;
    foreach (g; res.acceptedGroups) accepted ~= g.llcIndex;
    assert(accepted == [ushort(0)]);
    assert(res.rejectedGroups.array.length == 1);
    assert(res.processorsIn(0) == parseCpuList("0-7,16-23"));
    assert(res.placement == WorkerPlacement.free, "one reachable group: unconfined");
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
    assert(byScope.sameSelection(res) && acceptedOf(byScope) == acceptedOf(res));
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
                        assert(acceptedOf(res) == oracleSelect(snap, opt, list));
                        uint expected;
                        foreach (ref set; res.sets)
                        {
                            assert(set.workers == autoWorkersOf(set.processors.length));
                            assert(set.firstWorker == expected);
                            expected += set.workers;
                        }
                        assert(res.workerCount == expected);
                        foreach (ref c; res.cpus)
                            assert(c.workerIndex == uint.max, "no CPU has a worker of its own unless pinned");
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
    assert(acceptedOf(full) == parseCpuList("0-2,9"));
    assert(full.workerCountIn(0) == 2 && full.workerCountIn(1) == 1 && full.workerCount == 3);
    foreach (ref c; full.cpus)
        assert(!(c.reasons & Reject.notSelected), "resolvePool never sets notSelected");
    foreach (ref g; full.groups)
        assert(g.candidates == g.accepted);

    // Group 0 alone, with CPU 1 taken out of its set.
    auto narrow = full.narrowedTo([GroupTake(0, 5)], [ProcessorId(0, 1)]);
    assert(narrow.processorsIn(0) == parseCpuList("0,2"));
    assert(narrow.workerCount == 2, "a count is clamped to its set");
    assert(narrow[ProcessorId(0, 1)].reasons == Reject.notSelected);
    assert(narrow[ProcessorId(0, 9)].reasons == Reject.notSelected);
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
    assert(narrow.groups[0].candidates == 3 && narrow.groups[0].accepted == 2
        && narrow.groups[0].workers == 2);
    assert(narrow.groups[1].candidates == 1 && narrow.groups[1].accepted == 0
        && narrow.groups[1].workers == 0);
    assert(narrow.groups[1].common == Reject.none, "mixed notSelected and filter reasons");
    assert(narrow.acceptedGroupCount == 1 && narrow.workerCountIn(1) == 0 && narrow.setOf(1) is null);

    // A smaller count keeps the whole set: the workers are fewer, the mask is not.
    auto fewer = full.narrowedTo([GroupTake(0, 1), GroupTake(1, 0)]);
    assert(fewer.processorsIn(0) == parseCpuList("0-2") && fewer.workerCount == 1);
    assert(fewer.sets[0].unstaffed == 2);
    assert(fewer[ProcessorId(0, 9)].reasons == Reject.notSelected, "a take of zero leaves the group out");
    assert(reasonsText(Reject.notSelected) == "notSelected");
    import std.conv : to;
    assert(narrow.to!string.canFind("candidates"));

    // Nothing chosen: every filter-passing CPU is notSelected.
    auto none = full.narrowedTo(null);
    assert(none.workers.length == 0 && none.sets.length == 0);
    assert(none.rejectedCpus(Reject.notSelected).array.length == acceptedOf(full).length);
    assert(none.groups[1].common == Reject.none && none.groups[1].candidates == 1);

    // A group whose every CPU is a candidate but none chosen.
    auto homogeneous = f1();
    auto plain = resolvePool(homogeneous, PoolOptions.init);
    auto home = plain.narrowedTo([GroupTake(0, plain.workerCountIn(0))]);
    assert(home.groups[1].candidates == 16 && home.groups[1].common == Reject.notSelected);
    assert(home.sameSelection(plain.narrowedTo([GroupTake(0, 14)])) && !home.sameSelection(plain));
}

unittest
{
    // An explicit count that stops short of a group leaves it out.
    {
        auto two = f1();
        PoolOptions exact;
        exact.skipSmtSiblings = true;
        exact.workers = 8;
        auto r = resolvePool(two, exact);
        assert(r.workerCountIn(0) == 8 && r.workerCountIn(1) == 0 && r.sets.length == 1);
        assert(r[ProcessorId(0, 8)].reasons == Reject.notSelected);
        exact.workers = 11;
        r = resolvePool(two, exact);
        assert(r.workerCountIn(0) == 8 && r.workerCountIn(1) == 3 && r.processorsIn(1) == parseCpuList("8-15"));
    }

    // R10: the automatic count. None unstaffed of one or two CPUs, otherwise
    // one in eight rounded down and at least one.
    static immutable size_t[] sizes = [1, 2, 3, 8, 15, 16, 23, 24, 31, 32, 64];
    static immutable uint[] unstaffed = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4, 8];
    foreach (i, n; sizes)
    {
        assert(autoUnstaffed(n) == unstaffed[i]);
        assert(autoWorkersOf(n) == n - unstaffed[i]);
    }
    assert(autoWorkersOf(0) == 0);

    // The count is taken from the accepted CPUs: eight primaries leave one.
    auto snap = f1();
    PoolOptions opt;
    opt.skipSmtSiblings = true;
    auto res = resolvePool(snap, opt);
    assert(res.workerCountIn(0) == 7 && res.workerCountIn(1) == 7);
    assert(res.processorsIn(1) == parseCpuList("8-15"));
    assert(res.workers[7].llcIndex == 1 && res.workers[7].ordinal == 0 && !res.workers[7].pinned);
}

unittest
{
    // R11: placement. `automatic` is unconfined when the process reaches one
    // group and a mask per group otherwise; an explicit choice is kept.
    auto two = f1();
    auto one = f1((ushort lp) => (lp % 16) >= 8);
    PoolOptions opt;
    assert(resolvePool(two, opt).placement == WorkerPlacement.group);
    assert(resolvePool(one, opt).placement == WorkerPlacement.free);
    auto h = hybrid();
    assert(resolvePool(h, opt).placement == WorkerPlacement.free);
    // The filters do not make a group unreachable: the OS scope does.
    opt.processors = parseCpuList("0-3");
    assert(resolvePool(two, opt).placement == WorkerPlacement.group);
    auto scoped = f1();
    scoped.osScopeSource = OsScopeSource.processLaunch;
    scoped.osScopeAllowed = parseCpuList("0-7,16-23");
    assert(resolvePool(scoped, PoolOptions.init).placement == WorkerPlacement.free);

    foreach (p; [WorkerPlacement.free, WorkerPlacement.group, WorkerPlacement.pin])
    {
        PoolOptions forced;
        forced.placement = p;
        assert(resolvePool(one, forced).placement == p && effectivePlacement(one, forced) == p);
    }

    // Under `pin` each worker has one CPU, the first of its set.
    PoolOptions pinned;
    pinned.placement = WorkerPlacement.pin;
    pinned.skipSmtSiblings = true;
    auto res = resolvePool(two, pinned);
    assert(res.workerCount == 14);
    foreach (i, ref w; res.workers)
    {
        assert(w.pinned && w.processor == res.setOf(w.llcIndex).processors[w.ordinal]);
        assert(res[w.processor].workerIndex == i);
    }
    assert(res[ProcessorId(0, 7)].accepted && res[ProcessorId(0, 7)].workerIndex == uint.max,
        "the set's last CPU is accepted and has no worker");

    // Outcomes are recorded per worker.
    import threadpool.pin : PinOutcome;
    auto outcomes = new PinOutcome[](res.workerCount);
    outcomes[3] = PinOutcome.osRejected;
    auto started = res.withStart(outcomes);
    assert(!started.started && started.workers[3].start == StartState.placeFailed
        && started.workers[0].start == StartState.placed);
    assert(started.groups[0].started == 6 && started.groups[1].started == 7);
    assert(started.placeFailures().canFind("3 on 0:3") && started.placeFailures().canFind("osRejected"));
    outcomes[3] = PinOutcome.pinned;
    assert(res.withStart(outcomes).started);
}
