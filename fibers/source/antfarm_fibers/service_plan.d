/++ Fiber service plans: which workers serve which lane, decided before start.

A plan binds one `FiberLane` to each physical group of a pool resolution and
derives every lane's service and Farm sizing from it. Service is native-only
by default: a lane is consumed by the accepted workers of its own group.
Cross-group consumers exist only through explicit edges, `rescue` (the served
group has no accepted workers) or `assist` (opt-in, experimental, for staffed
groups). Nothing generates edges implicitly. `installFiberPlan` validates and
installs a plan.
+/
module antfarm_fibers.service_plan;

import antfarm : MAX_CONSUMERS_LIMIT;
import antfarm_fibers.scheduler : FiberDomain;
import antfarm_fibers.threadpool_driver : FiberLane;
import threadpool.resolution : PoolResolution;

/// Planned service for a lane's physical group, decided before start from the
/// pool resolution.
enum LaneService : ubyte
{
    native,     /// group has accepted workers; served only by them
    rescued,    /// group has no accepted workers; served by explicit rescue edges
    unstaffed,  /// group has no accepted workers and no rescue: admission closed
    disabled,   /// caller disabled it: admission closed (its native workers idle)
    absent,     /// no lane bound (legal only for groups without accepted workers)
}

/// Why a service edge exists.
enum ServiceKind : ubyte
{
    rescue, /// serves a group with no accepted workers
    assist, /// experimental, opt-in: serves a staffed group beside its native workers
}

/// Every responder worker of `server` keeps a persistent consumer view and a
/// small producer token on the Farm of `served`'s lane. Edges are one hop and
/// same-domain only.
struct ServiceEdge
{
    ushort server;    /// physical group whose responders serve
    ushort served;    /// physical group whose lane is served
    ServiceKind kind; /// rescue or assist
}

/// Farm sizing for one lane, derived from the plan; usable before the Farm
/// exists. Native counts are the group's accepted workers (each holds one home
/// view and one small token). Foreign counts sum every worker of each serving
/// group: responders are chosen at start, so this is an upper bound. External
/// counts are what the caller declared. Spawns from other threads go through
/// the lane accumulator and need no producer slot.
struct FarmRequirement
{
    uint nativeConsumers;   /// accepted workers of the lane's own group
    uint foreignConsumers;  /// workers of groups serving the lane by an edge
    uint externalConsumers; /// declared non-pool consumers
    uint nativeProducers;   /// accepted workers of the lane's own group
    uint foreignProducers;  /// workers of groups serving the lane by an edge
    uint externalProducers; /// declared non-pool small producers

    /// Total consumer views: `AntFarm.create`'s `expectedConsumers`.
    @property uint consumerViews() const @nogc nothrow pure
    {
        return nativeConsumers + foreignConsumers + externalConsumers;
    }

    /// Total small producer tokens: `AntFarm.create`'s `maxSmall`.
    @property uint smallProducerSlots() const @nogc nothrow pure
    {
        return nativeProducers + foreignProducers + externalProducers;
    }
}

/// An invalid plan, listing every problem found.
class FiberPlanException : Exception
{
    string[] problems; /// each problem, one line

    ///
    this(string[] problems, string file = __FILE__, size_t line = __LINE__)
    {
        import std.array : join;

        this.problems = problems;
        super("antfarm_fibers: invalid Fiber service plan:\n  "
            ~ problems.join("\n  "), file, line);
    }
}

/// Which lane serves each physical group of one pool resolution, and which
/// explicit edges add foreign consumers. Declarations are setup-only and
/// chain; mistakes are collected and reported together by `problems`.
final class FiberServicePlan
{
    private const(PoolResolution) res;
    private FiberLane[] lanes;        // index == physical group
    private bool[] disabledGroups;
    private ServiceEdge[] edgeList;
    private uint[] externalConsumers;
    private uint[] externalProducers;
    private string[] declarationProblems;

    /// A plan over `resolution`'s physical groups, normally `pool.resolve()`.
    this(const PoolResolution resolution)
    {
        if (resolution is null)
            throw new Exception("antfarm_fibers: a Fiber service plan needs a pool resolution");
        res = resolution;
        immutable n = resolution.physicalGroupCount;
        lanes = new FiberLane[n];
        disabledGroups = new bool[n];
        externalConsumers = new uint[n];
        externalProducers = new uint[n];
    }

    /// Bind `lane` to physical group `group`. A lane serves one group, and a
    /// group has one lane.
    FiberServicePlan bind(ushort group, FiberLane lane)
    {
        if (!declared(group, "bind")) return this;
        if (lane is null)
        {
            note("bind(%s): null lane", group);
            return this;
        }
        if (lanes[group] is lane) return this;
        if (lanes[group] !is null)
        {
            note("group %s has two lanes", group);
            return this;
        }
        foreach (g, other; lanes)
            if (other is lane)
            {
                note("one lane is bound to groups %s and %s", g, group);
                return this;
            }
        lanes[group] = lane;
        return this;
    }

    /// Close admission on `group`. Its native workers idle; a group with
    /// accepted workers still binds its lane.
    FiberServicePlan disable(ushort group)
    {
        if (declared(group, "disable"))
            disabledGroups[group] = true;
        return this;
    }

    /// Serve `unstaffedGroup`, which has no accepted workers, by the
    /// responders of `byGroup`.
    FiberServicePlan rescue(ushort unstaffedGroup, ushort byGroup)
    {
        return addEdge(byGroup, unstaffedGroup, ServiceKind.rescue);
    }

    /// Experimental, opt-in: the responders of `byGroup` also serve the
    /// staffed `staffedGroup`, beside its native workers.
    FiberServicePlan assist(ushort staffedGroup, ushort byGroup)
    {
        return addEdge(byGroup, staffedGroup, ServiceKind.assist);
    }

    /// Experimental, opt-in: `groups[i]` assists `groups[i + 1]`, wrapping.
    /// The explicit spelling of the former automatic shared-domain ring; one
    /// group adds nothing.
    FiberServicePlan assistRing(ushort[] groups)
    {
        if (groups.length < 2) return this;
        foreach (i, server; groups)
            addEdge(server, groups[(i + 1) % groups.length], ServiceKind.assist);
        return this;
    }

    /// Declare non-pool participants on `group`'s Farm, replacing any earlier
    /// declaration for that group.
    FiberServicePlan external(ushort group, uint consumers, uint producers)
    {
        if (declared(group, "external"))
        {
            externalConsumers[group] = consumers;
            externalProducers[group] = producers;
        }
        return this;
    }

    /// The resolution the plan was built from.
    const(PoolResolution) resolution() const { return res; }

    /// Planned service of physical group `group`.
    LaneService service(ushort group) const
    {
        checkGroup(group);
        if (disabledGroups[group]) return LaneService.disabled;
        if (lanes[group] is null) return LaneService.absent;
        if (staffed(group)) return LaneService.native;
        foreach (ref e; edgeList)
            if (e.served == group && e.kind == ServiceKind.rescue)
                return LaneService.rescued;
        return LaneService.unstaffed;
    }

    /// Farm sizing for `group`'s lane.
    FarmRequirement requirement(ushort group) const
    {
        checkGroup(group);
        FarmRequirement r;
        if (!disabledGroups[group])
            r.nativeConsumers = r.nativeProducers = workers(group);
        foreach (ref e; edgeList)
            if (e.served == group)
            {
                r.foreignConsumers += workers(e.server);
                r.foreignProducers += workers(e.server);
            }
        r.externalConsumers = externalConsumers[group];
        r.externalProducers = externalProducers[group];
        return r;
    }

    /// Every declared edge, in declaration order.
    const(ServiceEdge)[] edges() const { return edgeList; }

    /// Everything wrong with the plan; empty when it is valid.
    string[] problems() const
    {
        import std.format : format;

        string[] p = declarationProblems.dup;
        foreach (ushort g; 0 .. cast(ushort) lanes.length)
            if (staffed(g) && lanes[g] is null)
                p ~= disabledGroups[g]
                    ? format("group %s is disabled but has %s accepted workers and no bound lane;"
                        ~ " a disabled group still binds its lane", g, workers(g))
                    : format("group %s has %s accepted workers and no bound lane", g, workers(g));
        foreach (i, ref e; edgeList)
        {
            immutable name = edgeName(e);
            if (e.server == e.served)
                p ~= format("%s is a self-edge", name);
            foreach (ref earlier; edgeList[0 .. i])
                if (earlier.server == e.server && earlier.served == e.served)
                {
                    p ~= format("%s duplicates %s", name, edgeName(earlier));
                    break;
                }
            if (!staffed(e.server))
                p ~= format("%s is served by group %s, which has no accepted workers",
                    name, e.server);
            if (disabledGroups[e.server])
                p ~= format("%s is served by disabled group %s", name, e.server);
            if (e.kind == ServiceKind.rescue && staffed(e.served))
                p ~= format("%s targets staffed group %s (use assist)", name, e.served);
            if (e.kind == ServiceKind.assist && !staffed(e.served))
                p ~= format("%s targets group %s, which has no accepted workers (use rescue)",
                    name, e.served);
            if (disabledGroups[e.served])
                p ~= format("%s serves disabled group %s", name, e.served);
            if (lanes[e.served] is null)
                p ~= format("%s serves group %s, which has no bound lane", name, e.served);
            else if (lanes[e.server] !is null
                && lanes[e.server].domain !is lanes[e.served].domain)
                p ~= format("%s spans two Fiber domains", name);
        }
        foreach (ushort g; 0 .. cast(ushort) lanes.length)
        {
            // Read-only use of the lane's atomic counters.
            auto lane = cast(FiberLane) lanes[g];
            if (lane is null) continue;
            auto r = requirement(g);
            if (r.consumerViews > MAX_CONSUMERS_LIMIT)
                p ~= format("group %s needs %s consumer views, above the Farm limit %s",
                    g, r.consumerViews, MAX_CONSUMERS_LIMIT);
            if (lane.farm.maxSmall < r.smallProducerSlots)
                p ~= format("group %s's Farm has maxSmall %s, below the %s small producer"
                    ~ " slots it needs", g, lane.farm.maxSmall, r.smallProducerSlots);
            immutable planned = service(g);
            if (!isServed(planned) && (lane.ready != 0 || lane.published != 0))
                p ~= format("group %s's lane is %s but already has queued work", g, planned);
            if (lane.group != ushort.max && isServed(lane.service) && !isServed(planned)
                && lane.domain.active != 0)
                p ~= format("group %s's lane was served (%s, group %s) and its domain has"
                    ~ " %s active tasks; this plan leaves it %s",
                    g, lane.service, lane.group, lane.domain.active, planned);
        }
        return p;
    }

    /// Throw `FiberPlanException` listing every problem, if any.
    void validate() const
    {
        auto p = problems();
        if (p.length)
            throw new FiberPlanException(p);
    }

    /// One row per physical group: lane, service, native workers, edges in and
    /// out, requirement, the bound Farm's sizing and the domain.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        import std.format : format, formattedWrite;

        auto w = (const(char)[] s) => sink(s);
        formattedWrite(w, "fiber service plan: %s physical groups, %s accepted, %s edges\n",
            lanes.length, res.acceptedGroupCount, edgeList.length);
        formattedWrite(w, "  %-5s  %-6s  %-9s  %-6s  %-16s  %-16s  %-5s  %-5s  %-10s  %-10s  %s\n",
            "group", "lane", "service", "native", "in", "out", "views", "slots",
            "farm-views", "farm-slots", "domain");
        const(FiberDomain)[] domains;
        foreach (ushort g; 0 .. cast(ushort) lanes.length)
        {
            string edgesIn, edgesOut;
            foreach (ref e; edgeList)
            {
                if (e.served == g)
                    edgesIn ~= format("%s%s<-%s", edgesIn.length ? "," : "", e.kind, e.server);
                if (e.server == g)
                    edgesOut ~= format("%s%s->%s", edgesOut.length ? "," : "", e.kind, e.served);
            }
            auto r = requirement(g);
            auto lane = lanes[g];
            string farmViews = "-", farmSlots = "-", domain = "-";
            if (lane !is null)
            {
                farmViews = format("%s", lane.farm.expectedConsumers);
                farmSlots = format("%s", lane.farm.maxSmall);
                size_t d = domains.length;
                foreach (i, known; domains)
                    if (known is lane.domain) d = i;
                if (d == domains.length) domains ~= lane.domain;
                domain = format("d%s", d);
            }
            formattedWrite(w, "  %-5s  %-6s  %-9s  %-6s  %-16s  %-16s  %-5s  %-5s  %-10s  %-10s  %s\n",
                g, lane is null ? "absent" : "bound", service(g), workers(g),
                edgesIn.length ? edgesIn : "-", edgesOut.length ? edgesOut : "-",
                r.consumerViews, r.smallProducerSlots, farmViews, farmSlots, domain);
        }
    }

    /// The lane bound to `group`, or null.
    package FiberLane laneOf(ushort group) { return lanes[group]; }

    private bool staffed(ushort group) const
    {
        return res.groups[group].isAccepted;
    }

    private uint workers(ushort group) const
    {
        return cast(uint) res.workersIn(group).length;
    }

    private bool declared(ushort group, string what)
    {
        if (group < lanes.length) return true;
        note("%s(%s): group out of range (%s physical groups)", what, group, lanes.length);
        return false;
    }

    private FiberServicePlan addEdge(ushort server, ushort served, ServiceKind kind)
    {
        import std.conv : to;

        immutable inRange = declared(server, kind.to!string)
            & declared(served, kind.to!string);
        if (inRange)
            edgeList ~= ServiceEdge(server, served, kind);
        return this;
    }

    private void note(Args...)(string fmt, Args args)
    {
        import std.format : format;
        declarationProblems ~= format(fmt, args);
    }

    private void checkGroup(ushort group) const
    {
        import std.format : format;
        if (group >= lanes.length)
            throw new Exception(format("antfarm_fibers: group %s out of range (%s physical groups)",
                group, lanes.length));
    }

    private static string edgeName(ref const ServiceEdge e)
    {
        import std.format : format;
        return format("%s %s->%s", e.kind, e.server, e.served);
    }
}

/// Native and rescued lanes are served; the others refuse admission.
package bool isServed(LaneService service) @nogc nothrow pure
{
    return service == LaneService.native || service == LaneService.rescued;
}
