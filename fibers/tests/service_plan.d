module service_plan_tests;

// Pure Fiber service plan tests (P1-P5) on synthetic resolutions; no hardware
// or pool is needed. smoke.d runs them, and its live tests reuse `synthetic`.

import antfarm;
import antfarm_fibers;
import core.stdc.stdio : printf;
import std.algorithm : any, canFind;
import threadpool.os_scope : OsScope;
import threadpool.pool : PoolOptions;
import threadpool.resolution : PoolResolution, resolvePool;
import threadpool.topology : GroupingSource, LogicalProcessor,
    TopologySnapshot, assembleSnapshot;

/// One physical group per entry with `accepted[g]` accepted CPUs. A group
/// with none still exists physically, as one OS-excluded CPU.
PoolResolution synthetic(const(uint)[] accepted)
{
    LogicalProcessor[] processors;
    ushort lp;
    foreach (g, count; accepted)
        foreach (_; 0 .. (count == 0 ? 1 : count))
        {
            LogicalProcessor p;
            p.lpIndex = p.coreIndex = lp++;
            p.llcIndex = cast(ushort) g;
            p.affinityExcluded = count == 0;
            processors ~= p;
        }
    auto snapshot = new TopologySnapshot;
    *snapshot = assembleSnapshot("synthetic", processors, null, null,
        cast(ushort) accepted.length, 64, new GroupingSource[accepted.length]);
    OsScope unrestricted;
    return resolvePool(*snapshot, unrestricted, PoolOptions.init);
}

/// A small Farm sized for `views` consumers and `slots` small producers.
AntFarm* farmFor(uint views, uint slots)
{
    return AntFarm.create(2, 8, views == 0 ? 1 : views, 0, 0,
        slots == 0 ? 1 : slots, 4096, DEFAULT_SMALL_TABLE_THRESHOLD, false);
}

/// A Farm sized from the plan's requirement for `group`.
AntFarm* farmFor(FiberServicePlan plan, ushort group)
{
    auto r = plan.requirement(group);
    return farmFor(r.consumerViews, r.smallProducerSlots);
}

private bool reports(const(string)[] problems, string fragment)
{
    return problems.any!(p => p.canFind(fragment));
}

// P1: a shared domain over two staffed groups gets no edges by default, and
// each lane needs exactly its native workers.
void defaultPlanIsNativeOnly()
{
    auto res = synthetic([3, 5]);
    auto plan = new FiberServicePlan(res);
    auto domain = new FiberDomain;
    AntFarm*[] farms;
    scope (exit) foreach (farm; farms) farm.destroy();
    foreach (ushort g; 0 .. 2)
    {
        farms ~= farmFor(plan, g);
        plan.bind(g, new FiberLane(domain, farms[g]));
    }
    assert(plan.edges.length == 0);
    assert(plan.problems.length == 0);
    foreach (ushort g; 0 .. 2)
    {
        auto r = plan.requirement(g);
        assert(plan.service(g) == LaneService.native);
        assert(r.consumerViews == res.workersIn(g).length);
        assert(r.smallProducerSlots == res.workersIn(g).length);
        assert(r.foreignConsumers == 0 && r.foreignProducers == 0);
    }
    printf("P1 default plan is native-only: no edges, %u and %u native views\n",
        plan.requirement(0).consumerViews, plan.requirement(1).consumerViews);
}

// P2: a disabled group's lane is closed after a plan-only install.
void disabledGroupIsClosed()
{
    auto plan = new FiberServicePlan(synthetic([2, 2]));
    auto farm0 = farmFor(2, 2);
    scope (exit) farm0.destroy();
    auto farm1 = farmFor(2, 2);
    scope (exit) farm1.destroy();
    auto domain = new FiberDomain;
    auto lanes = [new FiberLane(domain, farm0), new FiberLane(domain, farm1)];
    plan.bind(0, lanes[0]).bind(1, lanes[1]).disable(1);
    assert(plan.service(1) == LaneService.disabled);
    assert(plan.requirement(1).consumerViews == 0);
    installFiberPlan(plan, null);
    scope (exit) uninstallFiberPlan();
    assert(lanes[0].state == FiberLaneState.pending);
    assert(lanes[1].group == 1 && lanes[1].service == LaneService.disabled);
    assert(lanes[1].state == FiberLaneState.closed);
    FiberLaneUnavailable refused;
    try lanes[1].spawn({});
    catch (FiberLaneUnavailable e) refused = e;
    assert(refused !is null && refused.state == FiberLaneState.closed
        && refused.service == LaneService.disabled && refused.group == 1);
    assert(domain.active == 0);
    printf("P2 disabled group: lane 1 closed, spawn refused\n");
}

// P3: every problem is reported together.
void invalidPlanReportsEveryProblem()
{
    // Groups 0, 1, 4, 5 are staffed; 2 and 3 are not.
    auto plan = new FiberServicePlan(synthetic([2, 2, 0, 0, 2, 2, 2]));
    auto shared_ = new FiberDomain;
    AntFarm*[] farms;
    scope (exit) foreach (farm; farms) farm.destroy();
    FiberLane lane(FiberDomain domain, uint slots)
    {
        farms ~= farmFor(8, slots);
        return new FiberLane(domain, farms[$ - 1]);
    }
    auto lane0 = lane(shared_, 8);
    plan.bind(0, lane0)
        .bind(1, lane(shared_, 1))            // maxSmall below its 2 workers
        .bind(2, lane0)                       // a lane bound twice
        .bind(3, lane(shared_, 8))
        .bind(4, lane(new FiberDomain, 8))
        .bind(6, lane(shared_, 8))
        .disable(6)
        // group 5: accepted workers, no lane
        .rescue(1, 0)                         // rescue of a staffed group
        .rescue(3, 2)                         // rescue by an unstaffed group
        .assist(4, 0)                         // across domains
        .assist(3, 1)                         // assist of an unstaffed group
        .assist(4, 0)                         // duplicate
        .assist(0, 6)                         // served by a disabled group
        .assist(6, 0)                         // into a disabled group
        .rescue(9, 0);                        // out of range
    auto problems = plan.problems();
    assert(reports(problems, "group 1's Farm has maxSmall 1"));
    assert(reports(problems, "one lane is bound to groups 0 and 2"));
    assert(reports(problems, "group 5 has 2 accepted workers and no bound lane"));
    assert(reports(problems, "rescue 0->1 targets staffed group 1 (use assist)"));
    assert(reports(problems, "rescue 2->3 is served by group 2, which has no accepted workers"));
    assert(reports(problems, "assist 0->4 spans two Fiber domains"));
    assert(reports(problems, "assist 1->3 targets group 3, which has no accepted workers (use rescue)"));
    assert(reports(problems, "assist 0->4 duplicates assist 0->4"));
    assert(reports(problems, "assist 6->0 is served by disabled group 6"));
    assert(reports(problems, "assist 0->6 serves disabled group 6"));
    assert(reports(problems, "rescue(9): group out of range"));
    FiberPlanException thrown;
    try plan.validate();
    catch (FiberPlanException e) thrown = e;
    assert(thrown !is null && thrown.problems == problems);

    // An edge into a group without a lane, a disabled staffed group without
    // its lane, and a self-edge.
    auto plan2 = new FiberServicePlan(synthetic([2, 0, 2]));
    auto farm = farmFor(8, 8);
    scope (exit) farm.destroy();
    plan2.bind(0, new FiberLane(farm)).disable(2).rescue(1, 0).assist(0, 0);
    auto problems2 = plan2.problems();
    assert(reports(problems2, "rescue 0->1 serves group 1, which has no bound lane"));
    assert(reports(problems2, "group 2 is disabled but has 2 accepted workers and no bound lane"));
    assert(reports(problems2, "assist 0->0 is a self-edge"));
    printf("P3 invalid plans: %u and %u problems reported together\n",
        cast(uint) problems.length, cast(uint) problems2.length);
}

// P4: a rescued group needs one view and one slot per worker of its rescuer.
void rescueSizing()
{
    auto res = synthetic([8, 0]);
    auto plan = new FiberServicePlan(res).rescue(1, 0);
    auto r = plan.requirement(1);
    assert(r.nativeConsumers == 0 && r.foreignConsumers == 8);
    assert(r.consumerViews == 8 && r.smallProducerSlots == 8);
    assert(plan.requirement(0).consumerViews == 8);
    auto farm0 = farmFor(plan, 0);
    scope (exit) farm0.destroy();
    auto farm1 = farmFor(plan, 1);
    scope (exit) farm1.destroy();
    auto domain = new FiberDomain;
    plan.bind(0, new FiberLane(domain, farm0)).bind(1, new FiberLane(domain, farm1));
    assert(plan.service(1) == LaneService.rescued);
    plan.validate();
    auto text = new char[0];
    plan.toString((const(char)[] s) { text ~= s; });
    assert(text.canFind("rescue<-0") && text.canFind("rescue->1"));
    printf("P4 rescue sizing: group 1 needs %u foreign views and %u slots\n",
        r.foreignConsumers, r.smallProducerSlots);
}

// P5: assistRing is the explicit spelling of the former automatic ring.
void assistRingEdges()
{
    auto plan = new FiberServicePlan(synthetic([4, 6])).assistRing([0, 1]);
    assert(plan.edges == [ServiceEdge(0, 1, ServiceKind.assist),
        ServiceEdge(1, 0, ServiceKind.assist)]);
    assert(plan.requirement(0).foreignConsumers == 6);
    assert(plan.requirement(1).foreignConsumers == 4);
    assert(plan.requirement(0).consumerViews == 10);
    assert(plan.requirement(1).smallProducerSlots == 10);
    auto domain = new FiberDomain;
    auto farm0 = farmFor(plan, 0);
    scope (exit) farm0.destroy();
    auto farm1 = farmFor(plan, 1);
    scope (exit) farm1.destroy();
    plan.bind(0, new FiberLane(domain, farm0)).bind(1, new FiberLane(domain, farm1));
    plan.validate();

    assert(new FiberServicePlan(synthetic([4, 6])).assistRing([0]).edges.length == 0);
    // A private-domain lane in the ring is reported, not silently skipped.
    auto split = new FiberServicePlan(synthetic([4, 6])).assistRing([0, 1]);
    split.bind(0, new FiberLane(farm0)).bind(1, new FiberLane(farm1));
    assert(reports(split.problems, "spans two Fiber domains"));
    printf("P5 assistRing: edges 0->1 and 1->0, foreign views %u and %u\n",
        plan.requirement(0).foreignConsumers, plan.requirement(1).foreignConsumers);
}

void servicePlanTests()
{
    defaultPlanIsNativeOnly();
    disabledGroupIsClosed();
    invalidPlanReportsEveryProblem();
    rescueSizing();
    assistRingEdges();
}
