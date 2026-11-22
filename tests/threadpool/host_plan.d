module threadpool.tests.host_plan;

// Pure FarmHost placement tests on synthetic snapshots; no OS calls, any
// host. Inside package threadpool so host-only facts (caller CPUs,
// participants, notes) are reachable through the package-only planHost.

import std.algorithm : all, any, canFind;
import std.array : array;

import threadpool.host;
import threadpool.hybrid : WorkerSelf;
import threadpool.pin : WorkerPlacement;
import threadpool.pool : PoolOptions;
import threadpool.resolution : PoolResolution, Reject, autoUnstaffed;
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

/// One physical group of `n` cores without SMT.
private TopologySnapshot oneGroup(size_t n)
{
    LogicalProcessor[] procs;
    foreach (i; 0 .. n)
        procs ~= lp(0, cast(ushort) i, cast(ushort) i, 0);
    return assemble(procs, 1);
}

/// f1 with physical group 1 outside the OS scope: one reachable group.
private TopologySnapshot f1Group0()
{
    return f1((ushort lp) => (lp % 16) >= 8);
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
    assert(plan.resolution.workers.all!(w => w.llcIndex == 1));
    assert(plan.resolution.processorsIn(1) == parseCpuList("8-15") && plan.workers == 2,
        "an explicit count keeps the whole set: fewer workers, the same CPUs");
}

unittest
{
    // 2: only(1) uses group 1 alone; only on a rejected group throws with its reason.
    auto snap = f1();
    FarmHostOptions options;
    options.groups = HostGroups.only(1);
    auto plan = planOf(snap, options);
    assert(plan.farmGroup == 1 && plan.groups == [ushort(1)]);
    assert(plan.resolution.processorsIn(1) == parseCpuList("8-15"));
    assert(plan.workers == 7, "eight accepted CPUs leave one unstaffed");
    assert(plan.callerCpus == parseCpuList("8-15,24-31"), "the confined caller has the whole group");
    assert(!plan.callerSharesWorkerCpu);

    auto noGroup1 = f1((ushort lp) => (lp % 16) >= 8);
    auto problems = problemsOf(noGroup1, options);
    assert(problems.length == 1 && problems[0].canFind("group 1 has no accepted CPU")
        && problems[0].canFind("osExcluded"), problems[0]);
}

unittest
{
    // 3: span([1,0]): home group 1, crossGroup, group-0 workers are foreign.
    auto snap = f1();
    FarmHostOptions options;
    options.groups = HostGroups.span([1, 0]);
    auto plan = planOf(snap, options);
    assert(plan.farmGroup == 1 && plan.groups == [ushort(1), ushort(0)]);
    assert(plan.crossGroup);
    assert(plan.nativeConsumers == 7 && plan.foreignConsumers == 7, "each set leaves one CPU");
    assert(plan.resolution.processorsIn(0) == parseCpuList("0-7"));
    assert(plan.resolution.processorsIn(1) == parseCpuList("8-15"));
    assert(plan.callerCpus == parseCpuList("8-15,24-31"));
    // Workers are numbered set by set, in group order.
    assert(plan.resolution.workers[0].llcIndex == 0 && plan.resolution.workers[7].llcIndex == 1);

    // Explicit n fills the home group first, and may leave it no unstaffed CPU.
    options.workers = 10;
    plan = planOf(snap, options);
    assert(plan.resolution.workerCountIn(1) == 8 && plan.resolution.workerCountIn(0) == 2);
    assert(plan.resolution.processorsIn(0) == parseCpuList("0-7"));
    assert(plan.nativeConsumers == 8 && plan.foreignConsumers == 2);
    assert(plan.callerCpus == parseCpuList("8-15,24-31") && !plan.callerSharesWorkerCpu,
        "the SMT siblings are still free");
    assert(plan.notes.any!(n => n.canFind("group 1 has no unstaffed processor")));
    assert(!plan.notes.any!(n => n.canFind("group 0 has no unstaffed processor")));

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
    // 4: autoWorkers on f1: one CPU of the home set has no worker, and the
    // confined caller has the whole group, SMT siblings included.
    auto snap = f1();
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(plan.workers == 7);
    assert(plan.resolution.processorsIn(0) == parseCpuList("0-7"));
    assert(plan.resolution.placement == WorkerPlacement.group, "two reachable groups: masks");
    assert(plan.caller == CallerPlacement.confine);
    assert(plan.callerCpus == parseCpuList("0-7,16-23"));
    assert(!plan.callerSharesWorkerCpu);
    assert(plan.nativeConsumers == 7 && plan.externalConsumers == 0);
    assert(plan.expectedConsumers == 7 && plan.maxSmall == 1 && plan.maxBulk == 0);

    // A pinned caller takes the set's last CPU; the count is of what is left.
    FarmHostOptions pin;
    pin.caller = CallerPlacement.pin;
    plan = planOf(snap, pin);
    assert(plan.callerCpus == [ProcessorId(0, 7)]);
    assert(plan.resolution.processorsIn(0) == parseCpuList("0-6") && plan.workers == 6);
    assert(plan.resolution[ProcessorId(0, 7)].reasons == Reject.notSelected);
    assert(!plan.callerSharesWorkerCpu);

    // The count does not depend on the caller's placement.
    FarmHostOptions unchanged;
    unchanged.caller = CallerPlacement.unchanged;
    plan = planOf(snap, unchanged);
    assert(plan.workers == 7 && plan.callerCpus.length == 0);
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
    assert(plan.notes.any!(n => n.canFind("group 0 has no unstaffed processor: 16 workers on 16 CPUs")));
    assert(plan.callerCpus == parseCpuList("0-7,16-23"));

    // A pinned caller cannot take a CPU the count needs: it shares the last.
    options.caller = CallerPlacement.pin;
    plan = planOf(snap, options);
    assert(plan.workers == 16 && plan.callerSharesWorkerCpu && plan.callerCpus == [ProcessorId(0, 23)]);
    assert(plan.resolution[ProcessorId(0, 23)].accepted);

    // One fewer leaves room: the caller's CPU comes out of the set.
    options.workers = 15;
    plan = planOf(snap, options);
    assert(plan.workers == 15 && !plan.callerSharesWorkerCpu && plan.callerCpus == [ProcessorId(0, 23)]);
    assert(plan.resolution[ProcessorId(0, 23)].reasons == Reject.notSelected);
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
    // The count is the host's to set.
    options.pool.managedWorker.poolStopped = null;
    options.pool.workers = 3;
    problems = problemsOf(snap, options);
    assert(problems.length == 1 && problems[0].canFind("pool.workers"));
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
    assert(text.canFind("4 of {0-7}") && text.canFind("unconfined"), text);

    // An accepted group left unused is noted too.
    plan = planOf(snap, options);
    assert(plan.notes.any!(n => n.canFind("group 1 not used: 8 accepted CPUs not selected")));
    assert(plan.summary().canFind("masked to their group"), plan.summary());
}

unittest
{
    // 12: skipSmtSiblings defaults on.
    assert(FarmHostOptions.init.pool.skipSmtSiblings);
    auto snap = f1();
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(plan.resolution.acceptedCpus.all!(c => !c.smtSibling));
    assert(plan.candidates[ProcessorId(0, 16)].reasons == Reject.smtSecondary);
    assert(plan.resolution[ProcessorId(0, 16)].reasons == Reject.smtSecondary);
}

unittest
{
    // 13: FarmHost.resolve is the host's selection: the chosen sets accepted,
    // every other filter-passing CPU notSelected, filters and user requests kept.
    auto snap = f1();
    auto res = FarmHost.resolve(snap, FarmHostOptions.init);
    auto plan = planOf(snap, FarmHostOptions.init);
    assert(res.sameSelection(plan.resolution));
    assert(res.processorsIn(0) == parseCpuList("0-7") && res.workerCount == 7);
    assert(res[ProcessorId(0, 7)].accepted, "the unstaffed CPU stays in the set");
    assert(res[ProcessorId(0, 8)].reasons == Reject.notSelected, "group not chosen");
    assert(res[ProcessorId(0, 16)].reasons == Reject.smtSecondary, "filter reason kept");
    assert(res.requests.length == 0);
    assert(res.groups[0].candidates == 8 && res.groups[0].accepted == 8 && res.groups[0].workers == 7);
    assert(res.groups[1].candidates == 8 && res.groups[1].accepted == 0 && res.groups[1].workers == 0);
    assert(res.groups[1].common == Reject.none, "notSelected primaries, smtSecondary siblings");
    assert(res.acceptedGroupCount == 1 && !res.started);
    foreach (ref c; res.cpus)
        assert(((c.reasons & ~Reject.notSelected) == Reject.none) == plan.candidates.isAccepted(c.id));

    // A user CPU list stays the request list, and is the set.
    FarmHostOptions listed;
    listed.workers = 2;
    listed.pool.processors = parseCpuList("8-11");
    res = FarmHost.resolve(snap, listed);
    assert(res.processorsIn(1) == parseCpuList("8-11") && res.workerCount == 2);
    assert(res.requests.length == 4 && res.requests.all!(r => r.known && r.accepted));
    assert(res[ProcessorId(0, 0)].reasons == Reject.notRequested);

    // workers == 0: never null, no workers, every candidate notSelected.
    FarmHostOptions none;
    none.workers = 0;
    res = FarmHost.resolve(snap, none);
    assert(res !is null && res.workers.length == 0 && res.sets.length == 0);
    assert(res.rejectedCpus(Reject.notSelected).array.length == 16);
    assert(planOf(snap, none).summary().canFind("no workers"));
}

unittest
{
    // 14: the automatic count by set size, whatever the caller's placement.
    static immutable size_t[] sizes = [1, 2, 3, 15, 16, 23, 24];
    static immutable uint[] workers = [1, 2, 2, 14, 14, 21, 21];
    foreach (i, n; sizes)
        foreach (caller; [CallerPlacement.unchanged, CallerPlacement.confine])
        {
            auto snap = oneGroup(n);
            FarmHostOptions options;
            options.caller = caller;
            auto plan = planOf(snap, options);
            assert(plan.workers == workers[i] && plan.workers == n - autoUnstaffed(n));
            assert(plan.resolution.processorsIn(0).length == n);
            assert(plan.resolution.placement == WorkerPlacement.free);
            // In a set of one or two the confined caller has no CPU of its own.
            assert(plan.callerSharesWorkerCpu == (caller == CallerPlacement.confine && n <= 2));
        }
}

unittest
{
    // 15: placement. One reachable group runs unconfined, and the filters
    // then set the count only; several get a mask each unless told otherwise.
    auto one = f1Group0();
    auto plan = planOf(one, FarmHostOptions.init);
    assert(plan.resolution.placement == WorkerPlacement.free && plan.workers == 7);
    assert(plan.summary().canFind("unconfined"), plan.summary());
    assert(plan.notes.any!(n => n.canFind("the processor filters set the worker count only")));
    assert(!plan.notes.any!(n => n.canFind("not confined to their groups")));

    FarmHostOptions open;
    open.pool.skipSmtSiblings = false;
    plan = planOf(one, open);
    assert(plan.workers == 14 && !plan.notes.any!(n => n.canFind("worker count only")),
        "every eligible CPU is in the set: nothing to note");

    auto two = f1();
    FarmHostOptions free;
    free.pool.placement = WorkerPlacement.free;
    free.groups = HostGroups.all();
    plan = planOf(two, free);
    assert(plan.resolution.placement == WorkerPlacement.free && plan.workers == 14);
    assert(plan.notes.any!(n => n.canFind("workers are not confined to their groups")));

    FarmHostOptions pinned;
    pinned.pool.placement = WorkerPlacement.pin;
    plan = planOf(two, pinned);
    assert(plan.summary().canFind("pinned"), plan.summary());
    assert(plan.resolution.workers.all!(w => w.pinned) && plan.workers == 7);
    assert(plan.resolution.workers[6].processor == ProcessorId(0, 6));
}

unittest
{
    // 16: a pinned caller under unconfined workers keeps no CPU to itself.
    auto one = f1Group0();
    FarmHostOptions options;
    options.caller = CallerPlacement.pin;
    auto plan = planOf(one, options);
    assert(plan.callerCpus == [ProcessorId(0, 7)] && plan.callerSharesWorkerCpu);
    assert(plan.resolution[ProcessorId(0, 7)].accepted && plan.workers == 7);
    assert(plan.notes.any!(n => n.canFind("which unconfined workers may also use")));

    // A masked set of one CPU: the caller takes a CPU outside the set if
    // the group has one, else it shares.
    auto two = f1();
    FarmHostOptions single;
    single.caller = CallerPlacement.pin;
    single.pool.processors = parseCpuList("3");
    plan = planOf(two, single);
    assert(plan.workers == 1 && plan.callerCpus == [ProcessorId(0, 23)] && !plan.callerSharesWorkerCpu);
}
