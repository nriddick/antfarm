module placement;

import threadpool.topology : GroupingSource, LogicalProcessor, ProcessorId, TopologySnapshot,
    formatCpuList;
public import threadpool.pin : PinOutcome, pinProcessor;
import std.algorithm.searching : canFind;
import std.algorithm.sorting : sort;
import std.algorithm.iteration : uniq;
import std.array : array;
import std.conv : to;
import std.json : JSONValue;
import std.exception : enforce;
import core.stdc.stdio : printf;

/// One placed thread: the CPU to pin plus its physical group and core.
struct PlacedCpu
{
    ProcessorId id;
    ushort llc;
    ushort core;
}

struct Placement
{
    PlacedCpu[] consumers;
    PlacedCpu producer, mailbox;
    bool reused;
    /// The consumers, producer and mailbox span more than one physical group.
    bool crossesLlc;
}

// One thread per physical core first, faster classes first within each pass.
// Only after every core has a representative do we use SMT siblings.
// `llc >= 0` restricts the candidates to that physical group; the default
// ordering is unchanged so historical results stay comparable.
Placement choosePlacement(ref const TopologySnapshot topology, uint count, int llc = -1)
{
    auto processors = topology.processors.dup;
    sort!((a, b) => a.efficiencyClass != b.efficiencyClass
        ? a.efficiencyClass > b.efficiencyClass
        : (a.group != b.group ? a.group < b.group : a.lpIndex < b.lpIndex))(processors);
    PlacedCpu[] order;
    foreach (siblings; [false, true])
        foreach (ref p; processors)
        {
            if (!topology.isEligible(p)) continue;
            if (llc >= 0 && p.llcIndex != llc) continue;
            if (p.smtSibling == siblings)
                order ~= PlacedCpu(ProcessorId.of(p), p.llcIndex, p.coreIndex);
        }
    enforce(order.length != 0, "no logical processors discovered");
    Placement result;
    result.consumers = new PlacedCpu[count];
    foreach (i; 0 .. count) result.consumers[i] = order[i % order.length];
    result.producer = order[count % order.length];
    result.mailbox = order[(cast(size_t) count + 1) % order.length];
    result.reused = cast(size_t) count + 2 > order.length;
    foreach (c; result.consumers ~ [result.mailbox])
        if (c.llc != result.producer.llc)
            result.crossesLlc = true;
    return result;
}

void printPlacement(ref const Placement p)
{
    printf("placement nc=%u consumers(group:LP@llc)=", cast(uint) p.consumers.length);
    foreach (i, cpu; p.consumers)
        printf("%s%u:%u@llc%u", i ? ",".ptr : "".ptr, cast(uint) cpu.id.group,
            cast(uint) cpu.id.lp, cast(uint) cpu.llc);
    printf(" producer=%u:%u@llc%u mailbox=%u:%u@llc%u%s crossesLlc=%s\n",
        cast(uint) p.producer.id.group, cast(uint) p.producer.id.lp, cast(uint) p.producer.llc,
        cast(uint) p.mailbox.id.group, cast(uint) p.mailbox.id.lp, cast(uint) p.mailbox.llc,
        p.reused ? " (LP reuse)".ptr : "".ptr, p.crossesLlc ? "yes".ptr : "no".ptr);
}

/// Role counts per location for `planRoles`. `homeLlc < 0` picks the group
/// with the most eligible primaries. A non-empty `cpus` restricts every planned
/// CPU, the controller included.
struct RoleRequest
{
    uint homeP, homeC, homeExtraP, homeExtraC, offP, offC, smtP, smtC;
    int homeLlc = -1;
    const(ProcessorId)[] cpus;
}

struct PlacedThread
{
    string role; /// "producer" | "consumer" | "controller"
    string loc;  /// "home" | "home-extra" | "off" | "smt" | "controller"
    uint idx;    /// index within the role
    PlacedCpu cpu;
    bool smt;
}

struct RolePlan
{
    bool ok;
    /// "single-llc", "grouping-not-l3", "no-remote-primaries",
    /// "home-llc-not-eligible", "insufficient-primaries",
    /// "insufficient-siblings", "cpus-not-eligible: <list>"
    string reason;
    bool singleLlc, ccdLabelled, crossesLlc, controllerSharesCore;
    ushort home;
    int remote = -1;
    PlacedThread[] threads;
    PlacedThread controller;
}

/// Eligible SMT primaries of physical group `llc` in `(group, lp)` order,
/// restricted to `cpus` when it is non-empty.
ProcessorId[] eligiblePrimaries(ref const TopologySnapshot t, ushort llc,
    const(ProcessorId)[] cpus = null)
{
    ProcessorId[] r;
    foreach (id; t.eligibleProcessorsInLlc(llc))
        if (!t.byId(id).smtSibling && (cpus.length == 0 || cpus.canFind(id)))
            r ~= id;
    return r;
}

/// Eligible SMT siblings on `id`'s core, restricted to `cpus` when it is
/// non-empty. `t.cores` is sorted and each core lists its processors in LP
/// order, so the result order is stable.
ProcessorId[] eligibleSiblings(ref const TopologySnapshot t, ProcessorId id,
    const(ProcessorId)[] cpus = null)
{
    ProcessorId[] r;
    foreach (ref c; t.cores)
    {
        if (!c.processors.canFind(id)) continue;
        foreach (o; c.processors)
        {
            auto p = t.byId(o);
            if (o != id && p.smtSibling && t.isEligible(*p)
                && (cpus.length == 0 || cpus.canFind(o)))
                r ~= o;
        }
        break;
    }
    return r;
}

// Home and off-CCD roles on eligible SMT primaries of two physical groups,
// never reusing a CPU and never re-placing a role that does not fit: such a
// request comes back `!ok` with a reason. Unlike `choosePlacement`, off
// producers fill the remote group from its highest primary down.
RolePlan planRoles(ref const TopologySnapshot t, in RoleRequest r)
{
    RolePlan plan;
    plan.singleLlc = t.eligibleLlcCount() < 2;
    plan.ccdLabelled = t.groupingSource == GroupingSource.l3;

    RolePlan skip(string reason)
    {
        plan.ok = false;
        plan.reason = reason;
        plan.threads = null;
        return plan;
    }

    ProcessorId[] bad;
    foreach (id; r.cpus)
    {
        auto p = t.byId(id);
        if (p is null || !t.isEligible(*p))
            bad ~= id;
    }
    if (bad.length)
        return skip("cpus-not-eligible: " ~ formatCpuList(bad.sort().uniq.array));

    ProcessorId[][] prim;
    prim.length = t.llcCount;
    foreach (ushort l; 0 .. t.llcCount)
        prim[l] = eligiblePrimaries(t, l, r.cpus);

    immutable wantOff = r.offP + r.offC > 0;
    if (wantOff && plan.singleLlc)
        return skip("single-llc");
    if (wantOff && !plan.ccdLabelled)
        return skip("grouping-not-l3");

    if (r.homeLlc >= 0)
    {
        if (r.homeLlc >= t.llcCount || prim[r.homeLlc].length == 0)
            return skip(plan.singleLlc ? "single-llc" : "home-llc-not-eligible");
        plan.home = cast(ushort) r.homeLlc;
    }
    else
    {
        foreach (ushort l; 0 .. t.llcCount)
            if (prim[l].length > prim[plan.home].length)
                plan.home = l;
        if (prim.length == 0 || prim[plan.home].length == 0)
            return skip("insufficient-primaries");
    }
    foreach (ushort l; 0 .. t.llcCount)
        if (l != plan.home && prim[l].length
            && (plan.remote < 0 || prim[l].length > prim[plan.remote].length))
            plan.remote = l;
    if (wantOff && plan.remote < 0)
        return skip("no-remote-primaries");

    bool[ProcessorId] used;
    PlacedCpu at(ProcessorId id)
    {
        auto p = t.byId(id);
        return PlacedCpu(id, p.llcIndex, p.coreIndex);
    }
    uint[string] nextIdx;
    void add(string role, string loc, ProcessorId id)
    {
        used[id] = true;
        plan.threads ~= PlacedThread(role, loc, nextIdx.get(role, 0), at(id), t.byId(id).smtSibling);
        nextIdx[role] = nextIdx.get(role, 0) + 1;
    }

    auto home = prim[plan.home];
    if (r.homeP + r.homeC + r.homeExtraC + r.homeExtraP > home.length)
        return skip("insufficient-primaries");
    size_t h;
    foreach (i; 0 .. r.homeP) add("producer", "home", home[h++]);
    foreach (i; 0 .. r.homeC) add("consumer", "home", home[h++]);
    foreach (i; 0 .. r.homeExtraC) add("consumer", "home-extra", home[h++]);
    foreach (i; 0 .. r.homeExtraP) add("producer", "home-extra", home[h++]);
    auto homeProducers = home[0 .. r.homeP];
    auto homeConsumers = home[r.homeP .. r.homeP + r.homeC];

    auto remote = plan.remote >= 0 ? prim[plan.remote] : null;
    if (r.offC + r.offP > remote.length)
        return skip("insufficient-primaries");
    foreach (i; 0 .. r.offC) add("consumer", "off", remote[i]);
    foreach (i; 0 .. r.offP) add("producer", "off", remote[$ - 1 - i]);

    bool sib(ProcessorId id, out ProcessorId s)
    {
        foreach (o; eligibleSiblings(t, id, r.cpus))
            if (o !in used)
            {
                s = o;
                return true;
            }
        return false;
    }
    if (r.smtC > homeConsumers.length || r.smtP > homeProducers.length)
        return skip("insufficient-siblings");
    foreach (i; 0 .. r.smtC)
    {
        ProcessorId s;
        if (!sib(homeConsumers[i], s)) return skip("insufficient-siblings");
        add("consumer", "smt", s);
    }
    foreach (i; 0 .. r.smtP)
    {
        ProcessorId s;
        if (!sib(homeProducers[i], s)) return skip("insufficient-siblings");
        add("producer", "smt", s);
    }

    bool placed;
    foreach (pool; [home, remote])
    {
        foreach_reverse (id; pool)
            if (id !in used)
            {
                plan.controller = PlacedThread("controller", "controller", 0, at(id), false);
                placed = true;
                break;
            }
        if (placed) break;
    }
    if (!placed && homeConsumers.length)
    {
        ProcessorId s;
        if (sib(homeConsumers[$ - 1], s))
        {
            plan.controller = PlacedThread("controller", "controller", 0, at(s), true);
            plan.controllerSharesCore = placed = true;
        }
    }
    if (!placed)
        return skip("insufficient-primaries");

    foreach (th; plan.threads ~ plan.controller)
        if (th.cpu.llc != plan.home)
            plan.crossesLlc = true;
    plan.ok = true;
    return plan;
}

/// Text form `g:lp`.
string cpuText(ProcessorId id)
{
    return id.group.to!string ~ ":" ~ id.lp.to!string;
}

/// The topology object shared by the `c2c` and `offccd` JSON reports.
/// `liveMask` must be read before the calling thread pins itself.
JSONValue topologyJson(ref const TopologySnapshot t, ref const RolePlan plan,
    const(ProcessorId)[] liveMask)
{
    import threadpool.os_scope : OsScopeSource, osScope;

    auto scope_ = osScope();
    JSONValue j;
    j["os"] = t.os;
    j["llc_count"] = t.llcCount;
    j["eligible_llc_count"] = t.eligibleLlcCount();
    j["grouping_source"] = t.groupingSource.to!string;
    j["ccd_labelled"] = t.groupingSource == GroupingSource.l3;
    j["os_scope"] = JSONValue(["source": JSONValue(scope_.source.to!string),
        "allowed": JSONValue(formatCpuList(scope_.allowed))]);
    j["live_os_mask"] = formatCpuList(liveMask);
    j["scope_drift"] = scope_.source != OsScopeSource.unrestricted
        && liveMask != scope_.allowed;
    JSONValue[] llcs;
    foreach (ushort l; 0 .. t.llcCount)
    {
        auto eligible = t.eligibleProcessorsInLlc(l);
        auto prim = eligiblePrimaries(t, l);
        JSONValue d;
        d["llc"] = l;
        d["source"] = t.llcDomains[l].source.to!string;
        d["l3_bytes"] = t.llcDomains[l].l3SizeBytes;
        d["processors"] = formatCpuList(t.processorsInLlc(l));
        d["eligible"] = formatCpuList(eligible);
        d["eligible_primaries"] = prim.length;
        d["eligible_siblings"] = eligible.length - prim.length;
        llcs ~= d;
    }
    j["llcs"] = llcs;
    j["home_llc"] = plan.home;
    j["remote_llc"] = plan.remote >= 0 ? JSONValue(plan.remote) : JSONValue(null);
    j["single_llc"] = plan.singleLlc;
    j["crosses_llc"] = plan.crossesLlc;
    return j;
}

/// One planned thread as JSON (drivers add pin and observation fields).
JSONValue threadJson(ref const PlacedThread th)
{
    JSONValue j;
    j["role"] = th.role;
    j["loc"] = th.loc;
    j["idx"] = th.idx;
    j["cpu"] = cpuText(th.cpu.id);
    j["group"] = th.cpu.id.group;
    j["lp"] = th.cpu.id.lp;
    j["core"] = th.cpu.core;
    j["llc"] = th.cpu.llc;
    j["smt"] = th.smt;
    return j;
}

/// The plan as JSON: status, per-role CPU lists and the threads.
JSONValue planJson(ref const RolePlan plan)
{
    JSONValue j;
    j["ok"] = plan.ok;
    j["reason"] = plan.reason.length ? JSONValue(plan.reason) : JSONValue(null);
    j["home_llc"] = plan.home;
    j["remote_llc"] = plan.remote >= 0 ? JSONValue(plan.remote) : JSONValue(null);
    j["single_llc"] = plan.singleLlc;
    j["ccd_labelled"] = plan.ccdLabelled;
    j["crosses_llc"] = plan.crossesLlc;
    j["controller_shares_core"] = plan.controllerSharesCore;
    ProcessorId[][string] byKey;
    JSONValue[] threads;
    foreach (th; plan.threads)
    {
        byKey[th.loc ~ "-" ~ th.role] ~= th.cpu.id;
        threads ~= threadJson(th);
    }
    JSONValue lists = JSONValue(string[string].init);
    foreach (k, ids; byKey)
        lists[k] = formatCpuList(ids.sort().array);
    j["cpus"] = lists;
    j["threads"] = threads;
    j["controller"] = plan.ok ? threadJson(plan.controller) : JSONValue(null);
    return j;
}

struct PinVerdict
{
    bool ok;
    string reason; /// "pin-<PinOutcome>" | "pin-mismatch"
}

/// Pure: per-thread pin outcome and observed CPUs -> run status. Any outcome
/// but `pinned` wins over a mismatch, so a refused pin is reported as such.
PinVerdict pinVerdict(const PinOutcome[] outcomes, const ProcessorId[] planned,
    const ProcessorId[] observedStart, const ProcessorId[] observedEnd)
{
    foreach (o; outcomes)
        if (o != PinOutcome.pinned)
            return PinVerdict(false, "pin-" ~ o.to!string);
    foreach (i, id; planned)
        if (observedStart[i] != id || observedEnd[i] != id)
            return PinVerdict(false, "pin-mismatch");
    return PinVerdict(true, null);
}

unittest
{
    TopologySnapshot topology;
    // Two fast SMT cores and a slow core in another processor group.
    foreach (i; 0 .. 4)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort)(i / 2);
        p.efficiencyClass = 1;
        p.smtSibling = (i % 2) != 0;
        topology.processors ~= p;
    }
    LogicalProcessor slow;
    slow.group = 1;
    topology.processors ~= slow;
    auto p = choosePlacement(topology, 2);
    assert(p.consumers[0].id == ProcessorId(0, 0) && p.consumers[1].id == ProcessorId(0, 2));
    assert(p.consumers[1].core == 1);
    assert(p.producer.id == ProcessorId(1, 0));
    assert(p.mailbox.id == ProcessorId(0, 1));
    assert(!p.reused);
    assert(!p.crossesLlc);
    assert(choosePlacement(topology, 5).reused);
}

unittest
{
    // Two LLCs (5900XT-like, scaled down): LPs 0-3 on LLC 0 and 4-7 on LLC 1,
    // SMT sibling N+8. The default order crosses LLCs once LLC 0's cores are
    // used; the llc filter keeps every role inside one group.
    TopologySnapshot topology;
    foreach (i; 0 .. 16)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort)(i % 8);
        p.llcIndex = cast(ushort)((i % 8) / 4);
        p.smtSibling = i >= 8;
        topology.processors ~= p;
    }
    auto wide = choosePlacement(topology, 4);
    assert(wide.consumers[3].id == ProcessorId(0, 3) && wide.consumers[3].llc == 0);
    assert(wide.producer.id == ProcessorId(0, 4) && wide.producer.llc == 1);
    assert(wide.crossesLlc);

    auto one = choosePlacement(topology, 4, 1);
    foreach (c; one.consumers ~ [one.producer, one.mailbox])
        assert(c.llc == 1);
    assert(one.consumers[0].id == ProcessorId(0, 4));
    assert(one.producer.id == ProcessorId(0, 12), "SMT siblings follow the group's cores");
    assert(!one.crossesLlc);
}

version (unittest)
{
    import threadpool.topology : assembleSnapshot, parseCpuList;

    /// Ryzen shape: `cores` cores, 2 LPs each, core = lp % cores, LLC =
    /// core / 8, SMT sibling N+cores. `fake5900` is the 16-core 5900XT.
    TopologySnapshot fakeRyzen(uint cores, bool delegate(ushort lp) excluded = null,
        bool delegate(ushort lp) offline = null, GroupingSource source = GroupingSource.l3)
    {
        LogicalProcessor[] procs;
        foreach (i; 0 .. 2 * cores)
        {
            LogicalProcessor p;
            p.lpIndex = cast(ushort) i;
            p.coreIndex = cast(ushort)(i % cores);
            p.llcIndex = cast(ushort)(i % cores / 8);
            p.smtSibling = i >= cores;
            if (excluded !is null) p.affinityExcluded = excluded(p.lpIndex);
            if (offline !is null) p.offlineAtDiscovery = offline(p.lpIndex);
            procs ~= p;
        }
        immutable llcs = (cores + 7) / 8;
        auto sources = new GroupingSource[](llcs);
        sources[] = source;
        return assembleSnapshot("synthetic", procs, null, null, cast(ushort) llcs, 64, sources);
    }

    TopologySnapshot fake5900(bool delegate(ushort lp) excluded = null,
        bool delegate(ushort lp) offline = null, GroupingSource source = GroupingSource.l3)
    {
        return fakeRyzen(16, excluded, offline, source);
    }

    ProcessorId[] idsOf(const PlacedThread[] threads, string loc)
    {
        ProcessorId[] r;
        foreach (th; threads)
            if (th.loc == loc)
                r ~= th.cpu.id;
        return r;
    }

    void assertNoReuse(ref const RolePlan p)
    {
        bool[ProcessorId] cpus;
        bool[ushort] cores;
        foreach (th; p.threads ~ p.controller)
        {
            assert(th.cpu.id !in cpus);
            cpus[th.cpu.id] = true;
            if (!th.smt && !p.controllerSharesCore)
            {
                assert(th.cpu.core !in cores);
                cores[th.cpu.core] = true;
            }
        }
    }
}

unittest
{
    // (a) Full 5900XT: home 0..6, controller 0:7, off consumers 8..15.
    auto t = fake5900();
    auto p = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 8));
    assert(p.ok && p.ccdLabelled && !p.singleLlc);
    assert(p.home == 0 && p.remote == 1);
    assert(p.threads.length == 15);
    assert(idsOf(p.threads, "home") == parseCpuList("0-6"));
    assert(idsOf(p.threads, "off") == parseCpuList("8-15"));
    assert(p.controller.cpu.id == ProcessorId(0, 7) && !p.controllerSharesCore);
    foreach (th; p.threads)
        if (th.loc == "off")
            assert(th.cpu.llc == 1 && th.role == "consumer");
    assert(p.threads[0].role == "producer" && p.threads[1].idx == 1);
    assert(p.threads[2].role == "consumer" && p.threads[2].idx == 0);
    assert(p.crossesLlc);
    assertNoReuse(p);

    auto homeOnly = planRoles(t, RoleRequest(2, 5));
    assert(homeOnly.ok && !homeOnly.crossesLlc);
    assert(homeOnly.controller.cpu.id == ProcessorId(0, 7));

    // Off producers fill the remote group from the top.
    auto offP = planRoles(t, RoleRequest(2, 5, 0, 0, 2, 1));
    assert(offP.ok);
    assert(idsOf(offP.threads, "off") == [ProcessorId(0, 8), ProcessorId(0, 15), ProcessorId(0, 14)]);
}

unittest
{
    // (b) LLC1 outside the OS scope, (c) LLC1 offline: both single-LLC, and
    // no planned CPU is ineligible.
    bool llc1(ushort lp) { return lp % 16 >= 8; }
    foreach (t; [fake5900(&llc1), fake5900(null, &llc1)])
    {
        assert(t.eligibleLlcCount() == 1 && t.llcCount == 2);
        auto off = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 1));
        assert(!off.ok && off.reason == "single-llc" && off.singleLlc);
        auto mirror = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 0, 0, 0, 1));
        assert(!mirror.ok && mirror.reason == "single-llc");
        auto p = planRoles(t, RoleRequest(2, 5, 0, 1));
        assert(p.ok && p.singleLlc && p.remote == -1);
        assert(p.controllerSharesCore, "no spare primary: controller on a sibling");
        foreach (th; p.threads ~ p.controller)
            assert(t.isEligible(*t.byId(th.cpu.id)));
        assertNoReuse(p);
    }
}

unittest
{
    // (d) An 8-primary single-LLC snapshot cannot hold 2P+5C plus 2 extra.
    auto t = fakeRyzen(8);
    assert(t.llcCount == 1);
    auto p = planRoles(t, RoleRequest(2, 5, 0, 2));
    assert(!p.ok && p.reason == "insufficient-primaries");
    assert(planRoles(t, RoleRequest(2, 5, 0, 1)).ok);
}

unittest
{
    // (e) Mirror: home on LLC1, off-threads on LLC0.
    auto t = fake5900();
    auto p = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 8, 0, 0, 1));
    assert(p.ok && p.home == 1 && p.remote == 0);
    assert(idsOf(p.threads, "home") == parseCpuList("8-14"));
    assert(idsOf(p.threads, "off") == parseCpuList("0-7"));
    assert(p.controller.cpu.id == ProcessorId(0, 15));
    assert(p.crossesLlc);
}

unittest
{
    // (f) SMT roles use the N+16 siblings of the home consumers, then producers.
    auto t = fake5900();
    auto p = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 0, 1, 3));
    assert(p.ok);
    assert(idsOf(p.threads, "smt") == parseCpuList("18-20,16"));
    foreach (th; p.threads)
        assert(th.smt == (th.loc == "smt"));
    assert(!p.crossesLlc);
    assertNoReuse(p);

    bool sibs(ushort lp) { return lp >= 18 && lp <= 22; }
    auto noSibs = fake5900(&sibs);
    auto none = planRoles(noSibs, RoleRequest(2, 5, 0, 0, 0, 0, 0, 1));
    assert(!none.ok && none.reason == "insufficient-siblings");
}

unittest
{
    // (g) Sibling-only remote: LLC1 has eligible CPUs but no primaries.
    bool prim1(ushort lp) { return lp >= 8 && lp <= 15; }
    auto t = fake5900(&prim1);
    assert(t.eligibleLlcCount() == 2);
    auto p = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 1));
    assert(!p.ok && p.reason == "no-remote-primaries");
    assert(planRoles(t, RoleRequest(2, 5)).ok);
}

unittest
{
    // (h) Fallback grouping is not a CCD: off-threads refused, home-only runs.
    auto t = fake5900(null, null, GroupingSource.l2Fallback);
    auto p = planRoles(t, RoleRequest(2, 5, 0, 0, 0, 1));
    assert(!p.ok && p.reason == "grouping-not-l3");
    auto h = planRoles(t, RoleRequest(2, 5));
    assert(h.ok && !h.ccdLabelled);
}

unittest
{
    // (i) --cpus restricts every planned CPU, the controller included.
    auto t = fake5900();
    auto p = planRoles(t, RoleRequest(2, 2, 0, 0, 0, 1, 0, 0, -1, parseCpuList("0-4,8")));
    assert(p.ok);
    ProcessorId[] got;
    foreach (th; p.threads ~ p.controller)
        got ~= th.cpu.id;
    assert(got.sort().array == parseCpuList("0-4,8"));
    assert(p.controller.cpu.id == ProcessorId(0, 4));

    auto full = planRoles(t, RoleRequest(2, 2, 0, 0, 0, 1, 0, 0, -1, parseCpuList("0-3,8")));
    assert(!full.ok && full.reason == "insufficient-primaries", "no room for the controller");

    bool ex(ushort lp) { return lp == 9 || lp == 12; }
    auto exT = fake5900(&ex);
    auto bad = planRoles(exT, RoleRequest(1, 0, 0, 0, 0, 0, 0, 0, -1,
        parseCpuList("12,0,9,99")));
    assert(!bad.ok && bad.reason == "cpus-not-eligible: 9,12,99");
}

unittest
{
    // (j) pinVerdict.
    alias P = ProcessorId;
    auto planned = [P(0, 1), P(0, 2)];
    with (PinOutcome)
    {
        assert(pinVerdict([pinned, pinned], planned, planned, planned).ok);
        auto refused = pinVerdict([pinned, outsideOsScope], planned, planned, planned);
        assert(!refused.ok && refused.reason == "pin-outsideOsScope");
        auto moved = pinVerdict([pinned, pinned], planned, planned, [P(0, 1), P(0, 3)]);
        assert(!moved.ok && moved.reason == "pin-mismatch");
    }
}
