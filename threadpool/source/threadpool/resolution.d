module threadpool.resolution;

/**
 * Pool resolution: which known CPU becomes a worker, and why every other CPU,
 * request and physical group does not.
 *
 * `resolvePool` is a pure function of (frozen snapshot, options), so
 * `CacheAwarePool.resolve()` previews exactly what `start()` will use. The OS
 * scope comes from the snapshot (`osScopeSource`, `osScopeAllowed`).
 */

import std.algorithm.iteration : filter;

import threadpool.pin : PinOutcome;
import threadpool.pool : PoolOptions;
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
    /// Passed every filter, but a placement policy above the pool (FarmHost
    /// group choice, worker count, caller reservation) left it out. Never
    /// combined with another reason; never set by `resolvePool`.
    notSelected   = 1 << 5,
    all           = 0x3F,    /// every reason
}

/// Pool start progress for one CPU.
enum StartState : ubyte
{
    notStarted, /// not a worker, or the pool has not started
    pinned,     /// the worker pinned to this CPU
    pinFailed,  /// the worker could not pin; see `CpuDisposition.pin`
}

/// Fate of one known CPU.
struct CpuDisposition
{
    ProcessorId id;          /// the CPU
    ushort llcIndex;         /// physical group, or `noLlcIndex` for an ungrouped CPU
    ushort coreIndex;        /// physical core
    ushort numaIndex;        /// NUMA node
    ubyte  tableClass;       /// dense P/E table class
    bool   smtSibling;       /// not its core's primary
    bool   osParked;         /// Windows CPU-set parking: informational only
    Reject reasons;          /// every applicable reason, not just the first
    uint   workerIndex = uint.max; /// index into `PoolResolution.workers`
    StartState start;        /// start progress
    PinOutcome pin;          /// meaningful when `start == pinFailed`

    /// No reason applies.
    @property bool accepted() const @nogc nothrow pure { return reasons == Reject.none; }
}

/// One entry per `PoolOptions.processors` element, in request order.
struct RequestDisposition
{
    ProcessorId id;        /// the requested CPU
    bool known;            /// names a CPU in the snapshot
    bool accepted;         /// that CPU was accepted (implies known)
    bool duplicate;        /// repeats an earlier entry; never adds a worker

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
    uint accepted;          /// workers
    uint started;           /// workers pinned at start
    /// Reasons shared by every member when `accepted == 0` (e.g. osExcluded).
    Reject common;

    /// The group has at least one worker.
    @property bool isAccepted() const @nogc nothrow pure { return accepted != 0; }
}

/// A selection report. Immutable after construction; safe to share.
final class PoolResolution
{
    /// The snapshot resolved against; its `osScopeSource` and
    /// `osScopeAllowed` are the OS scope used.
    const(TopologySnapshot)* topology;
    const(CpuDisposition)[]     cpus;      /// snapshot order: every known CPU
    const(RequestDisposition)[] requests;  /// request order: every requested id
    const(GroupDisposition)[]   groups;    /// index == llcIndex: every physical group
    const(ProcessorId)[]        workers;   /// index == workerIndex

    private const(ProcessorId)[][] workersByGroup;
    private bool startComplete;

    private this(const(TopologySnapshot)* topology,
        const(CpuDisposition)[] cpus, const(RequestDisposition)[] requests,
        const(ProcessorId)[] workers, bool startComplete)
    {
        this.topology = topology;
        this.cpus = cpus;
        this.requests = requests;
        this.workers = workers;
        this.startComplete = startComplete;

        size_t n = topology.llcDomains.length;
        if (topology.llcCount > n) n = topology.llcCount;
        foreach (ref c; cpus)
            if (c.llcIndex != noLlcIndex && c.llcIndex + 1 > n) n = c.llcIndex + 1;
        auto g = new GroupDisposition[](n);
        auto byGroup = new ProcessorId[][](n);
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
            if (c.start == StartState.pinned)
                gd.started++;
            gd.common &= c.reasons;
        }
        foreach (ref gd; g)
            if (gd.accepted != 0 || gd.physical == 0)
                gd.common = Reject.none;
        foreach (w; workers)
            if (auto c = findCpu(w))
                byGroup[c.llcIndex] ~= w;
        this.groups = g;
        this.workersByGroup = cast(const(ProcessorId)[][]) byGroup;
    }

    /// Accepted CPUs (workers), in snapshot order.
    auto acceptedCpus() const
    {
        return cpus.filter!(c => c.accepted);
    }

    /// CPUs rejected for at least one reason in `anyOf`.
    auto rejectedCpus(Reject anyOf = Reject.all) const
    {
        return cpus.filter!(c => (c.reasons & anyOf) != 0);
    }

    /// Physical groups with at least one worker.
    auto acceptedGroups() const
    {
        return groups.filter!(g => g.isAccepted);
    }

    /// Physical groups without workers.
    auto rejectedGroups() const
    {
        return groups.filter!(g => !g.isAccepted);
    }

    /// Requests naming no known CPU.
    auto unknownRequests() const
    {
        return requests.filter!(r => r.unknown);
    }

    /// Workers assigned to physical group `llc`, in worker order.
    const(ProcessorId)[] workersIn(ushort llc) const
    {
        return llc < workersByGroup.length ? workersByGroup[llc] : null;
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

    /// Physical groups with at least one worker.
    @property ushort acceptedGroupCount() const @nogc nothrow
    {
        ushort n;
        foreach (ref g; groups)
            if (g.isAccepted) n++;
        return n;
    }

    /// Every worker pinned and the pool started.
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

    /// Full report: header, group table, request list and every CPU.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        import std.format : formattedWrite;

        auto w = (const(char)[] s) => sink(s);
        uint eligible;
        foreach (ref c; cpus)
            if (!(c.reasons & (Reject.osExcluded | Reject.offline)))
                eligible++;
        formattedWrite(w, "threadpool resolution: os scope %s {%s}; %s known, %s eligible, %s accepted\n",
            topology.osScopeSource, scopeText(),
            cpus.length, eligible, workers.length);
        formattedWrite(w, "  physical groups %s (source %s), accepted groups %s, workers %s%s\n",
            physicalGroupCount, topology.groupingSource, acceptedGroupCount,
            workers.length, startComplete ? ", started" : "");
        formattedWrite(w, "  %-5s  %-17s  %-8s  %-8s  %-10s  %-8s  %-7s  %s\n",
            "group", "source", "physical", "eligible", "candidates", "accepted", "started",
            "common-reject");
        foreach (ref g; groups)
            formattedWrite(w, "  %-5s  %-17s  %-8s  %-8s  %-10s  %-8s  %-7s  %s\n",
                g.llcIndex, g.source, g.physical, g.eligible, g.candidates, g.accepted,
                g.started, reasonsText(g.common));
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
            string status = c.accepted ? "accepted" : "rejected";
            if (c.start == StartState.pinned) status = "pinned";
            else if (c.start == StartState.pinFailed) status = "pinFailed";
            string reasons = reasonsText(c.reasons);
            if (c.start == StartState.pinFailed)
                reasons = pinText(c.pin);
            if (c.osParked)
                reasons ~= " (parked)";
            formattedWrite(w, "  %-7s  %-3s  %-4s  %-3s  %-5s  %-8s  %-40s  %s\n",
                idText(c.id), c.llcIndex, c.coreIndex, c.smtSibling ? "yes" : "no",
                c.tableClass, status, reasons,
                c.workerIndex == uint.max ? "-" : idText(c.workerIndex));
        }
    }

    /// One-line summary.
    string summary() const
    {
        import std.format : format;

        uint eligible;
        foreach (ref c; cpus)
            if (!(c.reasons & (Reject.osExcluded | Reject.offline)))
                eligible++;
        uint unknown, duplicate;
        foreach (ref r; requests)
        {
            if (r.duplicate) duplicate++;
            else if (r.unknown) unknown++;
        }
        auto s = format("os scope %s {%s}; %s known, %s eligible, %s accepted; groups %s physical, %s accepted",
            topology.osScopeSource, scopeText(),
            cpus.length, eligible, workers.length, physicalGroupCount, acceptedGroupCount);
        if (requests.length)
            s ~= format("; %s requested (%s unknown, %s duplicate)", requests.length, unknown, duplicate);
        return s;
    }

    /// Same workers in the same order.
    bool sameSelection(const PoolResolution other) const
    {
        return other !is null && workers == other.workers;
    }

    /// A copy that records each worker's pin outcome. `outcomes[i]` belongs to
    /// `workers[i]`.
    package PoolResolution withStart(const(PinOutcome)[] outcomes) const
    {
        auto c = cpus.dup;
        bool all = outcomes.length == workers.length;
        foreach (ref d; c)
        {
            if (d.workerIndex == uint.max || d.workerIndex >= outcomes.length) continue;
            auto o = outcomes[d.workerIndex];
            d.pin = o;
            d.start = o == PinOutcome.pinned ? StartState.pinned : StartState.pinFailed;
            if (o != PinOutcome.pinned) all = false;
        }
        return new PoolResolution(topology, c, requests, workers, all);
    }

    /// The same snapshot, filters and requests with only `chosen` as
    /// workers: every other accepted CPU becomes `Reject.notSelected`.
    /// Workers keep snapshot (start) order and are renumbered. Ids in
    /// `chosen` that this resolution did not accept are ignored.
    package PoolResolution narrowedTo(const(ProcessorId)[] chosen) const
    {
        auto c = cpus.dup;
        ProcessorId[] kept;
        foreach (ref d; c)
        {
            if (!d.accepted) continue;
            if (contains(chosen, d.id))
            {
                d.workerIndex = cast(uint) kept.length;
                kept ~= d.id;
            }
            else
            {
                d.reasons = Reject.notSelected;
                d.workerIndex = uint.max;
            }
        }
        auto r = requests.dup;
        foreach (ref q; r)
            if (q.accepted)
                q.accepted = contains(kept, q.id);
        return new PoolResolution(topology, c, r, kept, false);
    }

    /// `"id (outcome)"` for every worker whose pin failed.
    package string pinFailures() const
    {
        string r;
        foreach (ref c; cpus)
            if (c.start == StartState.pinFailed)
                r ~= (r.length ? ", " : "") ~ idText(c.id) ~ " (" ~ pinText(c.pin) ~ ")";
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

/// Resolve `options` against `snap`. Pure: no OS calls. A CPU is OS-excluded
/// when the snapshot marks it `affinityExcluded` or is outside its OS scope
/// (`snap.inOsScope`), so a hand-built snapshot may set either.
PoolResolution resolvePool(ref const TopologySnapshot snap, in PoolOptions options)
{
    auto cpus = new CpuDisposition[](snap.processors.length);
    ProcessorId[] workers;
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
        if (r == Reject.none)
        {
            d.workerIndex = cast(uint) workers.length;
            workers ~= id;
        }
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
    return new PoolResolution(owned.ptr, cpus, requests, workers, false);
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
