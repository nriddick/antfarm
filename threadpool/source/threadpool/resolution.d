module threadpool.resolution;

/**
 * Pool resolution: which known CPUs form each group's set, how many workers
 * the set gets, and why every other CPU, request and physical group is left
 * out.
 *
 * `resolvePool` is a pure function of (frozen snapshot, options), so
 * `CacheAwarePool.resolve()` previews exactly what `start()` will use. The OS
 * scope comes from the snapshot (`osScopeSource`, `osScopeAllowed`).
 */

import std.algorithm.iteration : filter;

import threadpool.pin : PinOutcome, WorkerPlacement;
import threadpool.pool : PoolOptions, autoWorkers;
import threadpool.topology;

/// Why a known processor is not a worker. Several reasons may apply.
/// `none` = accepted. May gain members: do not `final switch` on it, and
/// use `all` rather than a literal mask.
enum Reject : ushort
{
    none          = 0,
    osExcluded    = 1 << 0,  /// outside the OS scope
    offline       = 1 << 1,  /// Linux offline at discovery
    notRequested  = 1 << 2,  /// `PoolOptions.processors` is non-empty and does not name this CPU
    classDisabled = 1 << 3,  /// `enablePCores` / `enableECores`
    smtSecondary  = 1 << 4,  /// `skipSmtSiblings` and this is not its core's fixed primary
    /// Passed every filter, but a placement policy left it out: a host's
    /// group choice, a processor taken for its caller, or an explicit worker
    /// count that did not reach its group. Never combined with another
    /// reason.
    notSelected   = 1 << 5,
    all           = 0x3F,    /// every reason
}

/// Start progress of one worker.
enum StartState : ubyte
{
    notStarted,  /// the pool has not started
    placed,      /// the worker applied its placement and runs
    placeFailed, /// the OS refused the placement; see `WorkerDisposition.outcome`
}

/// Fate of one known CPU.
struct CpuDisposition
{
    ProcessorId id;          /// the CPU
    ushort llcIndex;         /// physical group, or `noLlcIndex` for an ungrouped CPU
    ushort coreIndex;        /// physical core
    ushort numaIndex;        /// NUMA node
    ubyte  tableClass;       /// dense P/E class of the CPU; informational
    bool   smtSibling;       /// not its core's primary
    bool   osParked;         /// Windows CPU-set parking: informational only
    Reject reasons;          /// every applicable reason, not just the first
    /// Under `WorkerPlacement.pin`, the worker pinned to this CPU (an index
    /// into `PoolResolution.workers`); `uint.max` otherwise.
    uint   workerIndex = uint.max;

    /// No reason applies: the CPU is in its group's set.
    @property bool accepted() const @nogc nothrow pure { return reasons == Reject.none; }
}

/// One entry per `PoolOptions.processors` element, in request order.
struct RequestDisposition
{
    ProcessorId id;        /// the requested CPU
    bool known;            /// names a CPU in the snapshot
    bool accepted;         /// that CPU was accepted (implies known)
    bool duplicate;        /// repeats an earlier entry; never adds a processor

    /// Names no CPU in the snapshot.
    @property bool unknown() const @nogc nothrow pure { return !known; }
}

/// Counts for one physical group.
struct GroupDisposition
{
    ushort llcIndex;        /// physical group
    GroupingSource source;  /// how the group was formed
    uint physical;          /// known CPUs
    uint eligible;          /// in OS scope and online
    /// Passed every filter: `accepted` plus CPUs rejected only as
    /// `notSelected`. Equals `accepted` for a plain `resolvePool` result.
    uint candidates;
    uint accepted;          /// CPUs in the group's set
    uint workers;           /// workers of the set
    uint started;           /// workers placed at start
    /// Reasons shared by every member when `accepted == 0` (e.g. osExcluded).
    Reject common;

    /// The group has a set, and so at least one worker.
    @property bool isAccepted() const @nogc nothrow pure { return accepted != 0; }
}

/// A set: the accepted processors of one physical group, and how many
/// workers run on them. Workers of a set are interchangeable.
struct SetDisposition
{
    ushort llcIndex;                 /// physical group
    const(ProcessorId)[] processors; /// accepted CPUs, snapshot order
    uint workers;                    /// workers of the set
    uint firstWorker;                /// index of its first worker in `PoolResolution.workers`

    /// Accepted CPUs beyond the worker count.
    @property uint unstaffed() const @nogc nothrow pure
    {
        return processors.length > workers ? cast(uint)(processors.length - workers) : 0;
    }
}

/// One worker.
struct WorkerDisposition
{
    ushort llcIndex;       /// its physical group
    uint ordinal;          /// its number within the group's set
    /// Under `WorkerPlacement.pin`: the one CPU it runs on. `pinned` is false
    /// and `processor` meaningless under `free` and `group`.
    bool pinned;
    ProcessorId processor; /// ditto
    StartState start;      /// start progress
    PinOutcome outcome;    /// meaningful when `start == placeFailed`
}

/// CPUs of a set of `n` that the automatic worker count leaves without a
/// worker: none of one or two, otherwise one in eight and at least one.
uint autoUnstaffed(size_t n) @nogc nothrow pure @safe
{
    if (n <= 2) return 0;
    immutable eighth = n / 8;
    return eighth > 1 ? cast(uint) eighth : 1;
}

/// Workers the automatic count gives a set of `n` accepted CPUs.
uint autoWorkersOf(size_t n) @nogc nothrow pure @safe
{
    return cast(uint)(n - autoUnstaffed(n));
}

/// A group's share when a host narrows a resolution (`narrowedTo`).
package struct GroupTake
{
    ushort llc;   /// physical group
    uint workers; /// workers of its set; 0 leaves the group out
}

/// A selection report. Immutable after construction; safe to share.
final class PoolResolution
{
    /// The snapshot resolved against; its `osScopeSource` and
    /// `osScopeAllowed` are the OS scope used.
    const(TopologySnapshot)* topology;
    /// What a worker is told about where it may run: `free`, `group` or
    /// `pin`, never `automatic`.
    WorkerPlacement placement;
    const(CpuDisposition)[]     cpus;      /// snapshot order: every known CPU
    const(RequestDisposition)[] requests;  /// request order: every requested id
    const(GroupDisposition)[]   groups;    /// index == llcIndex: every physical group
    const(SetDisposition)[]     sets;      /// groups with workers, in llcIndex order
    const(WorkerDisposition)[]  workers;   /// index == workerIndex, set by set

    private bool startComplete;

    private this(const(TopologySnapshot)* topology, WorkerPlacement placement,
        const(CpuDisposition)[] cpus, const(RequestDisposition)[] requests,
        const(SetDisposition)[] sets, const(WorkerDisposition)[] workers, bool startComplete)
    {
        this.topology = topology;
        this.placement = placement;
        this.cpus = cpus;
        this.requests = requests;
        this.sets = sets;
        this.workers = workers;
        this.startComplete = startComplete;

        size_t n = topology.llcDomains.length;
        if (topology.llcCount > n) n = topology.llcCount;
        foreach (ref c; cpus)
            if (c.llcIndex != noLlcIndex && c.llcIndex + 1 > n) n = c.llcIndex + 1;
        auto g = new GroupDisposition[](n);
        foreach (i, ref gd; g)
        {
            gd.llcIndex = cast(ushort) i;
            if (auto d = topology.domain(cast(ushort) i))
                gd.source = d.source;
            gd.common = Reject.all;
        }
        foreach (ref c; cpus)
        {
            if (c.llcIndex == noLlcIndex) continue;
            auto gd = &g[c.llcIndex];
            gd.physical++;
            if (!(c.reasons & (Reject.osExcluded | Reject.offline)))
                gd.eligible++;
            if ((c.reasons & ~Reject.notSelected) == Reject.none)
                gd.candidates++;
            if (c.accepted)
                gd.accepted++;
            gd.common &= c.reasons;
        }
        foreach (ref gd; g)
            if (gd.accepted != 0 || gd.physical == 0)
                gd.common = Reject.none;
        foreach (ref s; sets)
            g[s.llcIndex].workers = s.workers;
        foreach (ref w; workers)
            if (w.start == StartState.placed)
                g[w.llcIndex].started++;
        this.groups = g;
    }

    /// Accepted CPUs (those in a set), in snapshot order.
    auto acceptedCpus() const
    {
        return cpus.filter!(c => c.accepted);
    }

    /// CPUs rejected for at least one reason in `anyOf`.
    auto rejectedCpus(Reject anyOf = Reject.all) const
    {
        return cpus.filter!(c => (c.reasons & anyOf) != 0);
    }

    /// Physical groups with a set.
    auto acceptedGroups() const
    {
        return groups.filter!(g => g.isAccepted);
    }

    /// Physical groups without one.
    auto rejectedGroups() const
    {
        return groups.filter!(g => !g.isAccepted);
    }

    /// Requests naming no known CPU.
    auto unknownRequests() const
    {
        return requests.filter!(r => r.unknown);
    }

    /// Workers in total.
    @property uint workerCount() const @nogc nothrow
    {
        return cast(uint) workers.length;
    }

    /// Workers of physical group `llc`; 0 for a group without a set.
    uint workerCountIn(ushort llc) const @nogc nothrow
    {
        return llc < groups.length ? groups[llc].workers : 0;
    }

    /// The set of physical group `llc`, or null when it has none.
    const(SetDisposition)* setOf(ushort llc) const @nogc nothrow
    {
        foreach (ref s; sets)
            if (s.llcIndex == llc) return &s;
        return null;
    }

    /// The accepted CPUs of physical group `llc`: where its workers may run
    /// under `group`, and what its worker count was taken from.
    const(ProcessorId)[] processorsIn(ushort llc) const @nogc nothrow
    {
        auto s = setOf(llc);
        return s is null ? null : s.processors;
    }

    /// The disposition of a known CPU. Throws on an unknown id.
    ref const(CpuDisposition) opIndex(ProcessorId id) const
    {
        auto c = findCpu(id);
        if (c is null)
            throw new ResolutionException("threadpool: unknown processor " ~ idText(id), this);
        return *c;
    }

    /// `id` names a known, accepted CPU.
    bool isAccepted(ProcessorId id) const
    {
        auto c = findCpu(id);
        return c !is null && c.accepted;
    }

    /// Physical group count (`TopologySnapshot.llcCount`).
    @property ushort physicalGroupCount() const @nogc nothrow
    {
        return cast(ushort) groups.length;
    }

    /// Physical groups with a set.
    @property ushort acceptedGroupCount() const @nogc nothrow
    {
        ushort n;
        foreach (ref g; groups)
            if (g.isAccepted) n++;
        return n;
    }

    /// Every worker placed and the pool started.
    @property bool started() const @nogc nothrow
    {
        return startComplete;
    }

    /// Opt-in contract check; does not change filter semantics. Throws
    /// `ResolutionException` naming each unknown, rejected (with reasons) or
    /// duplicate request.
    void enforceRequested() const
    {
        string problems;
        foreach (ref r; requests)
        {
            string why;
            if (r.duplicate)
                why = "duplicate";
            else if (r.unknown)
                why = "unknown";
            else if (!r.accepted)
                why = "rejected (" ~ reasonsText(findCpu(r.id).reasons) ~ ")";
            if (why.length)
                problems ~= (problems.length ? "; " : "") ~ idText(r.id) ~ " " ~ why;
        }
        if (problems.length)
            throw new ResolutionException(
                "threadpool: requested processors not all accepted: " ~ problems, this);
    }

    /// Full report: header, group table, sets, request list and every CPU.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        import std.format : formattedWrite;

        auto w = (const(char)[] s) => sink(s);
        uint eligible, accepted;
        foreach (ref c; cpus)
        {
            if (!(c.reasons & (Reject.osExcluded | Reject.offline)))
                eligible++;
            if (c.accepted) accepted++;
        }
        formattedWrite(w, "threadpool resolution: os scope %s {%s}; %s known, %s eligible, %s accepted\n",
            topology.osScopeSource, scopeText(),
            cpus.length, eligible, accepted);
        formattedWrite(w, "  physical groups %s (source %s), accepted groups %s, workers %s, placement %s%s\n",
            physicalGroupCount, topology.groupingSource, acceptedGroupCount,
            workers.length, placement, startComplete ? ", started" : "");
        formattedWrite(w, "  %-5s  %-17s  %-8s  %-8s  %-10s  %-8s  %-7s  %-7s  %s\n",
            "group", "source", "physical", "eligible", "candidates", "accepted", "workers",
            "started", "common-reject");
        foreach (ref g; groups)
            formattedWrite(w, "  %-5s  %-17s  %-8s  %-8s  %-10s  %-8s  %-7s  %-7s  %s\n",
                g.llcIndex, g.source, g.physical, g.eligible, g.candidates, g.accepted,
                g.workers, g.started, reasonsText(g.common));
        foreach (ref s; sets)
            formattedWrite(w, "  set %s: %s worker%s on {%s}, %s unstaffed\n", s.llcIndex, s.workers,
                s.workers == 1 ? "" : "s", formatCpuList(s.processors), s.unstaffed);
        foreach (i, ref wd; workers)
            if (wd.start == StartState.placeFailed)
                formattedWrite(w, "  worker %s (group %s) placeFailed: %s\n", i, wd.llcIndex,
                    pinText(wd.outcome));
        if (requests.length == 0)
            sink("  requests: (none: all CPUs considered)\n");
        else
        {
            ProcessorId[] ids;
            foreach (ref r; requests) ids ~= r.id;
            formattedWrite(w, "  requests: %s\n", formatCpuList(ids));
            foreach (i, ref r; requests)
            {
                if (r.duplicate)
                    formattedWrite(w, "    request %s %s: duplicate\n", i, idText(r.id));
                else if (r.unknown)
                    formattedWrite(w, "    request %s %s: unknown\n", i, idText(r.id));
                else if (!r.accepted)
                    formattedWrite(w, "    request %s %s: rejected %s\n", i, idText(r.id),
                        reasonsText(findCpu(r.id).reasons));
            }
        }
        formattedWrite(w, "  %-7s  %-3s  %-4s  %-3s  %-5s  %-8s  %-40s  %s\n",
            "cpu", "llc", "core", "smt", "class", "status", "reasons", "worker");
        foreach (c; sortedCpus())
        {
            string reasons = reasonsText(c.reasons);
            if (c.osParked)
                reasons ~= " (parked)";
            formattedWrite(w, "  %-7s  %-3s  %-4s  %-3s  %-5s  %-8s  %-40s  %s\n",
                idText(c.id), c.llcIndex, c.coreIndex, c.smtSibling ? "yes" : "no",
                c.tableClass, c.accepted ? "accepted" : "rejected", reasons,
                c.workerIndex == uint.max ? "-" : idText(c.workerIndex));
        }
    }

    /// One-line summary.
    string summary() const
    {
        import std.format : format;

        uint eligible, accepted;
        foreach (ref c; cpus)
        {
            if (!(c.reasons & (Reject.osExcluded | Reject.offline)))
                eligible++;
            if (c.accepted) accepted++;
        }
        uint unknown, duplicate;
        foreach (ref r; requests)
        {
            if (r.duplicate) duplicate++;
            else if (r.unknown) unknown++;
        }
        auto s = format("os scope %s {%s}; %s known, %s eligible, %s accepted; %s worker%s (%s); "
            ~ "groups %s physical, %s accepted",
            topology.osScopeSource, scopeText(),
            cpus.length, eligible, accepted, workers.length, workers.length == 1 ? "" : "s",
            placement, physicalGroupCount, acceptedGroupCount);
        if (requests.length)
            s ~= format("; %s requested (%s unknown, %s duplicate)", requests.length, unknown, duplicate);
        return s;
    }

    /// The same placement, the same sets and the same worker count in each.
    bool sameSelection(const PoolResolution other) const
    {
        if (other is null || placement != other.placement || sets.length != other.sets.length)
            return false;
        foreach (i, ref s; sets)
            if (s.llcIndex != other.sets[i].llcIndex || s.workers != other.sets[i].workers
                || s.processors != other.sets[i].processors)
                return false;
        return true;
    }

    /// A copy that records each worker's placement outcome. `outcomes[i]`
    /// belongs to `workers[i]`.
    package PoolResolution withStart(const(PinOutcome)[] outcomes) const
    {
        auto w = workers.dup;
        bool all = outcomes.length == workers.length;
        foreach (i, ref d; w)
        {
            if (i >= outcomes.length) continue;
            d.outcome = outcomes[i];
            d.start = outcomes[i] == PinOutcome.pinned ? StartState.placed : StartState.placeFailed;
            if (outcomes[i] != PinOutcome.pinned) all = false;
        }
        return new PoolResolution(topology, placement, cpus, requests, sets, w, all);
    }

    /// The same snapshot, filters, requests and placement with only the
    /// groups of `takes` staffed, each with the worker count given (clamped
    /// to its set). Every accepted CPU of another group, and every CPU in
    /// `removed`, becomes `Reject.notSelected`.
    package PoolResolution narrowedTo(const(GroupTake)[] takes,
        const(ProcessorId)[] removed = null) const
    {
        auto c = cpus.dup;
        foreach (ref d; c)
        {
            if (!d.accepted) continue;
            bool keep;
            foreach (ref t; takes)
                if (t.llc == d.llcIndex && t.workers != 0) keep = true;
            if (!keep || contains(removed, d.id))
                d.reasons = Reject.notSelected;
            d.workerIndex = uint.max;
        }
        auto r = requests.dup;
        foreach (ref q; r)
            if (q.accepted)
                foreach (ref d; c)
                    if (d.id == q.id) q.accepted = d.accepted;
        uint[] counts = new uint[](groupSlots(c));
        bool[] taken = new bool[](counts.length);
        foreach (ref t; takes)
            if (t.llc < counts.length) { counts[t.llc] = t.workers; taken[t.llc] = true; }
        return build(topology, placement, c, r, (ushort llc, size_t n) =>
            taken[llc] ? (counts[llc] < n ? counts[llc] : cast(uint) n) : 0);
    }

    /// `"worker i (outcome)"` for every worker whose placement failed.
    package string placeFailures() const
    {
        string r;
        foreach (i, ref w; workers)
            if (w.start == StartState.placeFailed)
                r ~= (r.length ? ", " : "") ~ idText(i) ~ (w.pinned ? " on " ~ idText(w.processor) : "")
                    ~ " in group " ~ idText(w.llcIndex) ~ " (" ~ pinText(w.outcome) ~ ")";
        return r;
    }

    private string scopeText() const
    {
        return topology.osScopeSource == OsScopeSource.unrestricted
            ? "all" : formatCpuList(topology.osScopeAllowed);
    }

    private const(CpuDisposition)* findCpu(ProcessorId id) const @nogc nothrow
    {
        foreach (ref c; cpus)
            if (c.id == id)
                return &c;
        return null;
    }

    private const(CpuDisposition)[] sortedCpus() const
    {
        import std.algorithm.sorting : sort;
        auto r = cpus.dup;
        r.sort!((a, b) => a.id < b.id);
        return r;
    }

    private static size_t groupSlots(const(CpuDisposition)[] cpus) @nogc nothrow
    {
        size_t n;
        foreach (ref c; cpus)
            if (c.llcIndex != noLlcIndex && c.llcIndex + 1 > n) n = c.llcIndex + 1;
        return n;
    }

    /// Sets and workers from the accepted CPUs of `cpus`. `staff(llc, n)` is
    /// the worker count of group `llc`'s set of `n` CPUs; 0 gives no set.
    /// Under `pin` the workers take the set's first CPUs.
    private static PoolResolution build(const(TopologySnapshot)* topology, WorkerPlacement placement,
        CpuDisposition[] cpus, const(RequestDisposition)[] requests,
        scope uint delegate(ushort llc, size_t n) staff)
    {
        auto byGroup = new ProcessorId[][](groupSlots(cpus));
        foreach (ref c; cpus)
            if (c.accepted && c.llcIndex != noLlcIndex)
                byGroup[c.llcIndex] ~= c.id;
        SetDisposition[] sets;
        WorkerDisposition[] workers;
        foreach (llc, ids; byGroup)
        {
            if (ids.length == 0) continue;
            immutable count = staff(cast(ushort) llc, ids.length);
            if (count == 0)
            {
                // A set without workers is no set: its CPUs were passed over.
                foreach (ref c; cpus)
                    if (c.accepted && c.llcIndex == llc) c.reasons = Reject.notSelected;
                continue;
            }
            SetDisposition s;
            s.llcIndex = cast(ushort) llc;
            s.processors = ids;
            s.workers = count;
            s.firstWorker = cast(uint) workers.length;
            foreach (k; 0 .. count)
            {
                WorkerDisposition w;
                w.llcIndex = cast(ushort) llc;
                w.ordinal = k;
                if (placement == WorkerPlacement.pin)
                {
                    w.pinned = true;
                    w.processor = ids[k];
                    foreach (ref c; cpus)
                        if (c.id == ids[k]) c.workerIndex = cast(uint) workers.length;
                }
                workers ~= w;
            }
            sets ~= s;
        }
        return new PoolResolution(topology, placement, cpus, requests, sets, workers, false);
    }
}

/// A resolution contract failed.
class ResolutionException : Exception
{
    const PoolResolution resolution; /// the failing report

    this(string msg, const PoolResolution resolution,
        string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
        this.resolution = resolution;
    }
}

/// The placement `options` resolves to on `snap`: `automatic` is `free` when
/// the process can reach only one physical group, `group` otherwise.
WorkerPlacement effectivePlacement(ref const TopologySnapshot snap, in PoolOptions options)
{
    if (options.placement != WorkerPlacement.automatic)
        return options.placement;
    bool[] seen;
    uint reachable;
    foreach (ref p; snap.processors)
    {
        if (p.llcIndex == noLlcIndex || p.affinityExcluded || p.offlineAtDiscovery
            || !snap.inOsScope(ProcessorId.of(p)))
            continue;
        if (p.llcIndex >= seen.length) seen.length = p.llcIndex + 1;
        if (!seen[p.llcIndex]) { seen[p.llcIndex] = true; reachable++; }
    }
    return reachable <= 1 ? WorkerPlacement.free : WorkerPlacement.group;
}

/// Resolve `options` against `snap`. Pure: no OS calls. A CPU is OS-excluded
/// when the snapshot marks it `affinityExcluded` or is outside its OS scope
/// (`snap.inOsScope`), so a hand-built snapshot may set either. Every group
/// with an accepted CPU gets a set with the automatic worker count
/// (`autoWorkersOf`), or `options.workers` handed out in group order when
/// that is set; a group the count does not reach gets no set, and its CPUs
/// are `Reject.notSelected`.
PoolResolution resolvePool(ref const TopologySnapshot snap, in PoolOptions options)
{
    auto cpus = new CpuDisposition[](snap.processors.length);
    foreach (i, ref p; snap.processors)
    {
        auto id = ProcessorId.of(p);
        auto tc = tableClassIndex(p.efficiencyClass, snap.maxEfficiencyClass, snap.classCount);
        auto d = &cpus[i];
        d.id = id;
        d.llcIndex = p.llcIndex;
        d.coreIndex = p.coreIndex;
        d.numaIndex = p.numaIndex;
        d.tableClass = tc;
        d.smtSibling = p.smtSibling;
        d.osParked = p.parkedAtDiscovery;

        Reject r;
        if (p.affinityExcluded || !snap.inOsScope(id))
            r |= Reject.osExcluded;
        if (p.offlineAtDiscovery)
            r |= Reject.offline;
        if (options.processors.length && !contains(options.processors, id))
            r |= Reject.notRequested;
        if (snap.classCount <= 1)
        {
            if (!options.enablePCores) r |= Reject.classDisabled;
        }
        else
        {
            if (tc == classP && !options.enablePCores) r |= Reject.classDisabled;
            if (tc == classE && !options.enableECores) r |= Reject.classDisabled;
        }
        if (options.skipSmtSiblings && p.smtSibling)
            r |= Reject.smtSecondary;
        d.reasons = r;
    }

    auto requests = new RequestDisposition[](options.processors.length);
    foreach (i, id; options.processors)
    {
        requests[i].id = id;
        requests[i].duplicate = contains(options.processors[0 .. i], id);
        foreach (ref c; cpus)
            if (c.id == id)
            {
                requests[i].known = true;
                requests[i].accepted = c.accepted;
                break;
            }
    }

    // The resolution outlives its caller's snapshot, so it owns a heap copy.
    // A bare `[snap].ptr` may be a stack temporary under optimization: the
    // ASan fork-join lane read a dead frame through `topology`.
    const(TopologySnapshot)[] owned = [snap];
    size_t left = options.workers;
    return PoolResolution.build(owned.ptr, effectivePlacement(snap, options), cpus, requests,
        (ushort, size_t n) {
            if (options.workers == autoWorkers) return autoWorkersOf(n);
            immutable take = left < n ? left : n;
            left -= take;
            return cast(uint) take;
        });
}

/// `"osExcluded,smtSecondary"`, or `"-"` for none.
string reasonsText(Reject r) pure
{
    if (r == Reject.none) return "-";
    string s;
    static foreach (name; ["osExcluded", "offline", "notRequested", "classDisabled", "smtSecondary",
        "notSelected"])
        if (r & __traits(getMember, Reject, name))
            s ~= (s.length ? "," : "") ~ name;
    return s;
}

private string pinText(PinOutcome o)
{
    import std.conv : to;
    return o.to!string;
}

private string idText(T)(T v)
{
    import std.format : format;
    return format("%s", v);
}

private bool contains(const(ProcessorId)[] ids, ProcessorId id) @nogc nothrow pure
{
    foreach (v; ids)
        if (v == id) return true;
    return false;
}
