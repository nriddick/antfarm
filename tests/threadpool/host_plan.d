module threadpool.tests.host_plan;

// Pure FarmHost placement tests on synthetic snapshots; no OS calls, any
// host. Inside package threadpool so host-only facts (caller CPUs,
// participants, notes) are reachable through the package-only planHost.

import std.algorithm : all, any, canFind;
import std.array : array;

import threadpool.host;
import threadpool.hybrid : WorkerSelf;
import threadpool.pool : PoolOptions;
import threadpool.resolution : PoolResolution, Reject;
import threadpool.topology;
import synthetic_topology;

private HostPlan planOf(ref const TopologySnapshot snap, FarmHostOptions options)
{
    return planHost(snap, options);
}

/// The resolve exception's problems, or null when resolving succeeded.
private const(string)[] problemsOf(ref const TopologySnapshot snap, FarmHostOptions options)
{
    try
        cast(void) FarmHost.resolve(snap, options);
    catch (FarmHostResolveException e)
        return e.problems;
    return null;
}

private ushort llcOf(ref const TopologySnapshot snap, ProcessorId id)
{
    return snap.byId(id).llcIndex;
}

unittest
{
    auto snap = f1();
    FarmHostOptions options;
    options.workers = 0;
    options.maxEmitRounds = 0;
    auto problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("maxEmitRounds must be positive"));
    options.maxEmitRounds = 1;
    assert(problemsOf(snap, options).length == 0);
}

unittest
{
    // 1: automatic picks the group with more accepted CPUs; ties go to the lowest llc.
    auto tied = f1();
    FarmHostOptions options;
    options.workers = 2;
    auto plan = planOf(tied, options);
    assert(plan.farmGroup == 0 && plan.groups == [ushort(0)] && !plan.crossGroup);

    auto lopsided = f1((ushort lp) => lp == 0 || lp == 1);
    plan = planOf(lopsided, options);
    assert(plan.farmGroup == 1 && plan.groups == [ushort(1)]);
    assert(plan.resolution.workers.all!(id => llcOf(lopsided, id) == 1));
}

unittest
{
    // 2: only(1) uses group 1 alone; only on a rejected group throws with its reason.
    auto snap = f1();
    FarmHostOptions options;
    options.groups = HostGroups.only(1);
    auto plan = planOf(snap, options);
    assert(plan.farmGroup == 1 && plan.groups == [ushort(1)]);
    assert(plan.resolution.workers == parseCpuList("8-14"));
    assert(plan.callerCpus == parseCpuList("15,24-31"));

    auto noGroup1 = f1((ushort lp) => (lp % 16) >= 8);
    auto problems = problemsOf(noGroup1, options);
    assert(problems.length == 1 && problems[0].canFind("group 1 has no accepted CPU")
        && problems[0].canFind("osExcluded"), problems[0]);
}

unittest
{
    // 3: span([1,0]): home group 1, crossGroup, group-0 workers are foreign;
    // the worker (start) order is snapshot order, not the preference order.
    auto snap = f1();
    FarmHostOptions options;
    options.groups = HostGroups.span([1, 0]);
    auto plan = planOf(snap, options);
    assert(plan.farmGroup == 1 && plan.groups == [ushort(1), ushort(0)]);
    assert(plan.crossGroup);
    assert(plan.nativeConsumers == 7 && plan.foreignConsumers == 8);
    assert(plan.resolution.workers == parseCpuList("0-14"));
    assert(plan.callerCpus == parseCpuList("15,24-31"));

    // Explicit n fills the home group first.
    options.workers = 10;
    plan = planOf(snap, options);
    assert(plan.resolution.workers == parseCpuList("0-1,8-15"));
    assert(plan.nativeConsumers == 8 && plan.foreignConsumers == 2);
    assert(plan.callerCpus == parseCpuList("24-31") && !plan.callerSharesWorkerCpu);

    // Windows: a span may not cross processor groups yet.
    auto groups = twoGroups();
    options.workers = autoWorkers;
    options.groups = HostGroups.span([0, 1]);
    auto problems = problemsOf(groups, options);
    assert(problems.length == 1 && problems[0].canFind("processor groups"), problems[0]);

    options.groups = HostGroups.span([0, 0]);
    problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("listed twice"));
}

unittest
{
    // 4: autoWorkers with confine on f1: the home group's last accepted CPU
    // is reserved, and callerCpus include the SMT siblings.
    auto snap = f1();
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(plan.workers == 7);
    assert(plan.resolution.workers == parseCpuList("0-6"));
    assert(plan.caller == CallerPlacement.confine);
    assert(plan.callerCpus == parseCpuList("7,16-23"));
    assert(!plan.callerSharesWorkerCpu);
    assert(plan.nativeConsumers == 7 && plan.externalConsumers == 0);
    assert(plan.expectedConsumers == 7 && plan.maxSmall == 1 && plan.maxBulk == 0);

    FarmHostOptions pin;
    pin.caller = CallerPlacement.pin;
    plan = planOf(snap, pin);
    assert(plan.callerCpus == [ProcessorId(0, 7)]);

    FarmHostOptions unchanged;
    unchanged.caller = CallerPlacement.unchanged;
    plan = planOf(snap, unchanged);
    assert(plan.workers == 8 && plan.callerCpus.length == 0);
    assert(plan.notes.any!(n => n.canFind("caller placement unchanged")));
}

unittest
{
    // 5: n equal to every eligible home CPU: the caller shares, with a note,
    // and the confine mask is the whole group.
    auto snap = f1();
    FarmHostOptions options;
    options.pool.skipSmtSiblings = false;
    options.workers = 16;
    auto plan = planOf(snap, options);
    assert(plan.workers == 16 && plan.callerSharesWorkerCpu);
    assert(plan.notes.any!(n => n.canFind("caller shares worker CPUs")));
    assert(plan.callerCpus == parseCpuList("0-7,16-23"));

    options.caller = CallerPlacement.pin;
    plan = planOf(snap, options);
    assert(plan.callerSharesWorkerCpu && plan.callerCpus == [ProcessorId(0, 23)]);
}

unittest
{
    // 6: more workers than the group has is a plan error that names span;
    // the host never spills into another group.
    auto snap = f1();
    FarmHostOptions options;
    options.workers = 9;
    auto problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("span"), problems[0]);
}

unittest
{
    // 7: more than 128 consumers throws; extraConsumers pushes it over.
    auto snap = f1();
    FarmHostOptions options;
    options.workers = 0;
    options.farm.extraConsumers = 127;
    assert(planOf(snap, options).expectedConsumers == 128);
    options.farm.extraConsumers = 128;
    auto problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("128"), problems[0]);
}

unittest
{
    // 8: any hook field set is rejected.
    static bool body_(WorkerSelf*) nothrow @nogc { return false; }
    static void poolStopped() nothrow {}
    auto snap = f1();
    FarmHostOptions options;
    options.pool.workerBody = &body_;
    auto problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("workerBody"));
    options.pool.workerBody = null;
    options.pool.managedWorker.poolStopped = &poolStopped;
    problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("managedWorker"));
}

unittest
{
    // 9: bad sizing is a plan error, not an abort.
    auto snap = f1();
    FarmHostOptions options;
    options.farm.k = 3;
    auto problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("K must be a power of 2"), problems[0]);
}

unittest
{
    // 10: workers 0: the caller consumes, placement unchanged, no selection.
    auto snap = f1();
    FarmHostOptions options;
    options.workers = 0;
    options.farm.extraSmallProducers = 2;
    auto plan = planOf(snap, options);
    assert(plan.workers == 0 && plan.resolution.workers.length == 0);
    assert(plan.callerConsumes && plan.caller == CallerPlacement.unchanged);
    assert(plan.callerCpus.length == 0 && plan.groups.length == 0);
    assert(plan.expectedConsumers == 1 && plan.maxSmall == 3);
    assert(plan.externalConsumers == 1 && plan.maxSmall == 3);
}

unittest
{
    // 11: a launch scope restricted to group 0 notes the unused group.
    auto snap = f1();
    auto launch = f1();
    launch.osScopeSource = OsScopeSource.processLaunch;
    launch.osScopeAllowed = parseCpuList("0-7,16-23");
    FarmHostOptions options;
    options.workers = 4;
    auto plan = planOf(launch, options);
    assert(plan.farmGroup == 0);
    assert(plan.notes.any!(n => n.canFind("group 1 not used") && n.canFind("osExcluded")));
    auto text = plan.summary();
    assert(text.canFind("4 workers") && text.canFind("confine"), text);

    // An accepted group left unused is noted too.
    plan = planOf(snap, options);
    assert(plan.notes.any!(n => n.canFind("group 1 not used: 8 accepted CPUs not selected")));
}

unittest
{
    // 12: skipSmtSiblings defaults on.
    assert(FarmHostOptions.init.pool.skipSmtSiblings);
    auto snap = f1();
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(plan.resolution.workers.all!(id => !snap.byId(id).smtSibling));
    assert(plan.candidates[ProcessorId(0, 16)].reasons == Reject.smtSecondary);
    assert(plan.resolution[ProcessorId(0, 16)].reasons == Reject.smtSecondary);
}

unittest
{
    // 13: FarmHost.resolve is the host's selection: workers accepted, every
    // other filter-passing CPU notSelected, filters and user requests kept.
    auto snap = f1();
    auto res = FarmHost.resolve(snap, FarmHostOptions.init);
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(res.workers == plan.resolution.workers && res.workers == parseCpuList("0-6"));
    assert(res[ProcessorId(0, 7)].reasons == Reject.notSelected, "caller reservation");
    assert(res[ProcessorId(0, 8)].reasons == Reject.notSelected, "group not chosen");
    assert(res[ProcessorId(0, 16)].reasons == Reject.smtSecondary, "filter reason kept");
    assert(res.requests.length == 0);
    assert(res.groups[0].candidates == 8 && res.groups[0].accepted == 7);
    assert(res.groups[1].candidates == 8 && res.groups[1].accepted == 0);
    assert(res.groups[1].common == Reject.none, "notSelected primaries, smtSecondary siblings");
    assert(res.acceptedGroupCount == 1 && !res.started);
    foreach (ref c; res.cpus)
        assert(((c.reasons & ~Reject.notSelected) == Reject.none) == plan.candidates.isAccepted(c.id));

    // A user CPU list stays the request list; chosen-but-unrequested is impossible.
    FarmHostOptions listed;
    listed.workers = 2;
    listed.pool.processors = parseCpuList("8-11");
    res = FarmHost.resolve(snap, listed);
    assert(res.workers == parseCpuList("8-9"));
    assert(res.requests.length == 4 && res.requests[2].known && !res.requests[2].accepted);
    assert(res[ProcessorId(0, 10)].reasons == Reject.notSelected);
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);

    // workers == 0: never null, no workers, every candidate notSelected.
    FarmHostOptions none;
    none.workers = 0;
    res = FarmHost.resolve(snap, none);
    assert(res !is null && res.workers.length == 0);
    assert(res.rejectedCpus(Reject.notSelected).array.length == 16);
    assert(planOf(snap, none).summary().canFind("no workers"));
}
