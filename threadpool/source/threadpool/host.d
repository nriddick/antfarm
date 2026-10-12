/++
 + FarmHost: one Farm, the calling thread's producer token and N pinned
 + `CacheAwarePool` workers that consume it, placed on one physical group
 + (LLC) by default.
 +
 + Placement is decided up front by a `FarmHostPlan`: `FarmHost.preview`
 + shows exactly what the constructor will do, and every compromise is a
 + plan note rather than a silent choice. Spanning groups is an explicit
 + opt-in (`HostGroups.span`), labelled `crossGroup` in the plan.
 +
 + This module is not re-exported by `import threadpool;`; it depends on the
 + `antfarm` package. Import it as `import threadpool.host;`.
 +/
module threadpool.host;

import core.time : Duration, msecs;

import antfarm : AntFarm, FarmCreateError, FarmCreateFailure,
    DEFAULT_SMALL_TABLE_THRESHOLD, DEFAULT_HUGE_PAGES, MAX_CONSUMERS_LIMIT;
import threadpool.os_scope : OsScope, OsScopeSource, osScope;
import threadpool.pool : CacheAwarePool, PoolOptions;
import threadpool.resolution : PoolResolution, Reject, reasonsText, resolvePool;
import threadpool.topology : ProcessorId, TopologySnapshot, formatCpuList;

/// `FarmHostOptions.workers` sentinel: every CPU the plan can staff in the
/// selected group(s), after the caller reservation.
enum uint autoWorkers = uint.max;

/// What an idle pool worker does after its final empty check.
/// Non-exhaustive: do not `final switch` on it.
enum WorkerIdle : ubyte
{
    /// Park. Host publication wrappers and `wake()` wake workers at once;
    /// raw-token publications are picked up within `idleTimeout`.
    park,
    /// Busy-poll with `pause` (Director `spin`). No wake cost on submit;
    /// every worker burns its CPU for the host's lifetime.
    spin,
}

/// Where the calling (control) thread may run while the host has workers.
/// Non-exhaustive: do not `final switch` on it.
enum CallerPlacement : ubyte
{
    /// Restrict the caller's affinity to the home group's online, in-scope
    /// CPUs that no worker owns (SMT siblings of workers included). When
    /// every such CPU is a worker, the whole home group, with
    /// `FarmHostPlan.callerSharesWorkerCpu` set. Restored by close.
    confine,
    /// Pin the caller to one CPU: the reserved one, else a free home-group
    /// CPU, else the last home worker CPU (shared). Restored by close.
    pin,
    /// Leave the caller's affinity alone. The plan reports the caller as
    /// an external participant that may run in another group.
    unchanged,
}

/// Which physical groups (llcIndex) supply workers. `.init` is `automatic`.
struct HostGroups
{
    /// The accepted group with the most accepted CPUs; ties go to the lowest index.
    static HostGroups automatic() pure nothrow @safe
    {
        return HostGroups.init;
    }

    /// Exactly this group. Plan error if it has no accepted CPU.
    static HostGroups only(ushort llc) pure nothrow @safe
    {
        HostGroups g;
        g.kind = Kind.only;
        g.llcs = [llc];
        return g;
    }

    /// One Farm consumed from several groups: deliberately cross-group.
    /// `llcs[0]` is the home group (caller placement, `farmGroup`). Every
    /// worker from another group is a foreign consumer in the plan.
    static HostGroups span(const(ushort)[] llcs) pure @safe
    {
        HostGroups g;
        g.kind = Kind.span;
        g.llcs = llcs.idup;
        return g;
    }

    private enum Kind : ubyte { automatic, only, span }
    private Kind kind;
    private const(ushort)[] llcs;
}

/// Farm geometry. Participant counts are computed by the plan; the fields
/// here only add to them.
struct FarmSizing
{
    uint  ringMiB  = 8;                              /// as AntFarm.create
    uint  k        = 8;                              /// as AntFarm.create
    ulong quotaSmall = 4096;                         /// as AntFarm.create
    uint  extraSmallProducers = 0;                   /// tokens from registerProducer(Tier.small)
    uint  bulkProducers = 0;                         /// maxBulk; 0 disables the bulk tier
    ulong quotaBulk = 0;                             /// as AntFarm.create
    /// Views subscribed outside the host. They must unsubscribe before
    /// `close()`/`abandon()`; the host cannot see them.
    uint  extraConsumers = 0;
    uint  smallThreshold = DEFAULT_SMALL_TABLE_THRESHOLD; /// as AntFarm.create
    bool  hugePages = DEFAULT_HUGE_PAGES;            /// environment-overridable as today
}

/// `PoolOptions.init` with `skipSmtSiblings = true`: one worker per core,
/// siblings left to the confined caller and the OS.
enum PoolOptions defaultHostPoolOptions = () {
    PoolOptions o;
    o.skipSmtSiblings = true;
    return o;
}();

/// Everything the host plan reads. `.init` is a sensible default.
struct FarmHostOptions
{
    /// Pool workers. 0 = no pool: the calling thread is the only consumer.
    uint workers = autoWorkers;
    /// Which group(s) supply workers. Ignored when `workers == 0`.
    HostGroups groups;
    /// Effective only with workers; `unchanged` when `workers == 0`.
    CallerPlacement caller = CallerPlacement.confine;
    /// The caller also owns a ConsumerView and consumes once per `step()`.
    /// Forced true when `workers == 0`.
    bool callerConsumes = false;
    /// Idle behavior of pool workers.
    WorkerIdle idle = WorkerIdle.park;
    /// Park bound for `WorkerIdle.park` (Director `sleep`). `Duration.max`
    /// parks until woken (Director `wait`); then raw-token publications need `wake()`.
    Duration idleTimeout = 1.msecs;
    /// Opt-in deadline applied afresh to every `waitUntil` call.
    /// `Duration.max` (default) = no deadline.
    Duration waitTimeout = Duration.max;
    /// CPU filters and QoS: processors, skipSmtSiblings, enable{P,E}Cores,
    /// spin iterations. The hook fields must be empty; the host owns them.
    PoolOptions pool = defaultHostPoolOptions;
    /// Farm geometry and extra participants.
    FarmSizing farm;
}

/// Participant counts on the host Farm, relative to its home group.
struct HostParticipants
{
    uint nativeConsumers;   /// pool workers assigned to the home group
    uint foreignConsumers;  /// pool workers assigned to another group (span only)
    uint externalConsumers; /// caller view + FarmSizing.extraConsumers
    uint smallProducers;    /// control token + extraSmallProducers
    uint bulkProducers;     /// FarmSizing.bulkProducers
}

/// What a `FarmHost` will do (or did). Immutable after construction; safe
/// to share, like `PoolResolution`.
final class FarmHostPlan
{
    const FarmHostOptions options;        /// as given
    const PoolResolution  candidates;     /// user filters over every group
    /// The exact pool start set, in start (snapshot) order; null when workers == 0.
    const PoolResolution  workerSelection;
    /// The Farm's declared home group; 0 and meaningless when workers == 0.
    ushort                farmGroup;
    const(ushort)[]       groups;         /// groups supplying workers, home first
    bool                  crossGroup;     /// foreignConsumers != 0
    uint                  workers;        /// resolved count
    CallerPlacement       caller;         /// effective (unchanged when workers == 0)
    const(ProcessorId)[]  callerCpus;     /// confine/pin set; empty when unchanged
    bool                  callerSharesWorkerCpu; /// no free home-group CPU was left
    bool                  callerConsumes; /// the caller owns a ConsumerView
    HostParticipants      participants;   /// consumers and producers on the Farm
    uint                  expectedConsumers; /// passed to AntFarm.create
    uint                  maxSmall;       /// passed to AntFarm.create
    uint                  maxBulk;        /// passed to AntFarm.create
    const(string)[]       notes;          /// e.g. "group 1 not used: osExcluded"

    private this(const FarmHostOptions options, const PoolResolution candidates,
        const PoolResolution workerSelection)
    {
        this.options = options;
        this.candidates = candidates;
        this.workerSelection = workerSelection;
    }

    /// One line: workers and where, caller placement, participants, sizing.
    string summary() const
    {
        import std.format : format;

        string s = workers == 0
            ? "FarmHost: no workers, the caller consumes"
            : format("FarmHost: %s worker%s on group%s %s {%s}%s, idle %s",
                workers, workers == 1 ? "" : "s", groups.length == 1 ? "" : "s",
                groupsText(groups), formatCpuList(workerSelection.workers),
                crossGroup ? " (crossGroup)" : "", idleText());
        if (workers != 0)
            s ~= format("; caller %s%s%s", caller,
                callerCpus.length ? " {" ~ formatCpuList(callerCpus) ~ "}" : "",
                callerSharesWorkerCpu ? " (shares worker CPUs)" : "");
        s ~= format("; consumers %s native + %s foreign + %s external, producers %s small + %s bulk",
            participants.nativeConsumers, participants.foreignConsumers,
            participants.externalConsumers, participants.smallProducers,
            participants.bulkProducers);
        s ~= format("; Farm %s MiB, k %s", options.farm.ringMiB, options.farm.k);
        return s;
    }

    /// Summary, every note, then the candidate resolution summary.
    void toString(scope void delegate(const(char)[]) sink) const
    {
        sink(summary());
        sink("\n");
        foreach (note; notes)
        {
            sink("  note: ");
            sink(note);
            sink("\n");
        }
        sink("  candidates: ");
        sink(candidates.summary());
        sink("\n");
    }

    private string idleText() const
    {
        import std.format : format;

        if (options.idle == WorkerIdle.spin) return "spin";
        if (options.idleTimeout == Duration.max) return "park (until woken)";
        return format("park (%s)", options.idleTimeout);
    }
}

/// A host operation failed. The message carries the plan summary when one
/// exists.
class FarmHostException : Exception
{
    /// `next` is the underlying exception, if any.
    this(string msg, Throwable next = null, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, file, line, next);
    }
}

/// The plan cannot be satisfied; nothing was created.
class FarmHostPlanException : FarmHostException
{
    const(string)[] problems; /// every problem found, in check order

    /// Message: "FarmHost plan rejected: " and the problems.
    this(const(string)[] problems, string file = __FILE__, size_t line = __LINE__)
        @safe pure
    {
        import std.array : join;

        super("FarmHost plan rejected: " ~ problems.join("; "), null, file, line);
        this.problems = problems;
    }
}

/// A `waitUntil` deadline expired. The host is still open with consumers
/// running: escalate (cancel) and wait again, or `abandon()`.
class FarmHostTimeout : FarmHostException
{
    Duration waited; /// the deadline that expired

    /// `waited` is the timeout that applied.
    this(string msg, Duration waited, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, null, file, line);
        this.waited = waited;
    }
}

/// Pool workers failed (a hook threw); the consumers are gone.
class FarmHostFailed : FarmHostException
{
    Throwable[] workerFailures; /// as `CacheAwarePool.workerFailures`

    /// `next` is the first worker failure, if any.
    this(string msg, Throwable[] workerFailures, string file = __FILE__, size_t line = __LINE__)
        @safe pure nothrow
    {
        super(msg, workerFailures.length ? workerFailures[0] : null, file, line);
        this.workerFailures = workerFailures;
    }
}

/// One Farm, its control token and its consumers. See the module header.
final class FarmHost
{
    /// What the constructor will do with `options`: discovers topology and
    /// the frozen OS scope (a threadpool setup operation), then plans as the
    /// overload below. Throws `FarmHostPlanException`.
    static FarmHostPlan preview(const FarmHostOptions options = FarmHostOptions.init)
    {
        const snap = CacheAwarePool.topology();
        const scope_ = osScope();
        return preview(snap, scope_, options);
    }

    /// The plan for an explicit snapshot and OS scope: deterministic, no OS
    /// queries (the test seam, like `resolvePool`). Throws
    /// `FarmHostPlanException` listing every problem found.
    static FarmHostPlan preview(ref const TopologySnapshot snap, ref const OsScope scope_,
        const FarmHostOptions options)
    {
        return planFor(snap, scope_, options);
    }
}

private FarmHostPlan planFor(ref const TopologySnapshot snap, ref const OsScope scope_,
    const FarmHostOptions options)
{
    import std.algorithm.searching : canFind;
    import std.format : format;

    string[] problems;
    string[] notes;

    // Hooks belong to the host.
    const hooks = options.pool;
    if (hooks.workerBody !is null) problems ~= "pool.workerBody must be null (the host owns worker hooks)";
    if (hooks.workerStop !is null) problems ~= "pool.workerStop must be null (the host owns worker hooks)";
    if (hooks.managedWorker.start !is null || hooks.managedWorker.pump !is null
        || hooks.managedWorker.stop !is null || hooks.managedWorker.poolStopped !is null)
        problems ~= "pool.managedWorker must be empty (the host owns worker hooks)";
    if (options.idle == WorkerIdle.park && options.idleTimeout <= Duration.zero)
        problems ~= "idleTimeout must be positive (Duration.max parks until woken)";

    const candidates = resolvePool(snap, scope_, options.pool);
    immutable callerConsumes = options.callerConsumes || options.workers == 0;
    auto caller = CallerPlacement.unchanged;
    const(ProcessorId)[] callerCpus;
    bool callerShares;
    ushort farmGroup;
    ProcessorId[] chosen;     // home-first preference order
    ushort[] groups;
    if (options.workers != 0)
    {
        groups = selectGroups(candidates, options.groups, problems);
        if (groups.length == 0)
            throw new FarmHostPlanException(problems);
        immutable home = groups[0];
        farmGroup = home;
        const homeAccepted = candidates.workersIn(home);
        immutable reserve = options.caller != CallerPlacement.unchanged;

        size_t available;
        foreach (g; groups)
            available += candidates.workersIn(g).length;
        size_t n;
        if (options.workers == autoWorkers)
            n = reserve && homeAccepted.length >= 2 ? available - 1 : available;
        else if (options.workers > available)
            problems ~= format("%s workers requested but group%s %s ha%s %s accepted CPU%s; "
                ~ "request fewer workers or opt into HostGroups.span to add a group (cross-group)",
                options.workers, groups.length == 1 ? "" : "s", groupsText(groups),
                groups.length == 1 ? "s" : "ve", available, available == 1 ? "" : "s");
        else
            n = options.workers;

        // Home group first; with autoWorkers and a reservation, the home
        // group's last accepted CPU stays free for the caller.
        immutable homeTake = options.workers == autoWorkers && n < available
            ? homeAccepted.length - 1
            : (n < homeAccepted.length ? n : homeAccepted.length);
        chosen ~= homeAccepted[0 .. homeTake];
        foreach (g; groups[1 .. $])
            foreach (id; candidates.workersIn(g))
                if (chosen.length < n) chosen ~= id;

        if (groups.length > 1)
        {
            ushort processorGroup = chosen.length ? chosen[0].group : 0;
            foreach (id; chosen)
                if (id.group != processorGroup)
                {
                    problems ~= "HostGroups.span across Windows processor groups is not supported yet";
                    break;
                }
        }

        if (reserve && n != 0)
        {
            // Home-group CPUs in the frozen OS scope and online, minus workers.
            ProcessorId[] homeEligible, free;
            foreach (ref c; candidates.cpus)
                if (c.llcIndex == home && !(c.reasons & (Reject.osExcluded | Reject.offline)))
                {
                    homeEligible ~= c.id;
                    if (!chosen.canFind(c.id)) free ~= c.id;
                }
            caller = options.caller;
            callerShares = free.length == 0;
            if (callerShares)
                notes ~= format("caller shares worker CPUs: every CPU of group %s is a worker", home);
            if (options.caller == CallerPlacement.confine)
                callerCpus = callerShares ? homeEligible : free;
            else if (homeTake < homeAccepted.length)
                callerCpus = [homeAccepted[$ - 1]]; // the reserved CPU
            else
                callerCpus = [free.length ? free[$ - 1] : homeAccepted[$ - 1]];
        }
        else if (n != 0)
            notes ~= format("caller placement unchanged: the control thread is an external "
                ~ "participant and may run outside group %s", home);

        // Every physical group not supplying workers, with its reason.
        foreach (ref gd; candidates.groups)
        {
            if (groups.canFind(gd.llcIndex) || gd.physical == 0) continue;
            notes ~= gd.isAccepted
                ? format("group %s not used: %s accepted CPU%s not selected", gd.llcIndex,
                    gd.accepted, gd.accepted == 1 ? "" : "s")
                : format("group %s not used: %s", gd.llcIndex, groupWhy(candidates, gd.llcIndex));
        }
    }
    else if (options.groups.kind != HostGroups.Kind.automatic)
        notes ~= "groups ignored: workers == 0 starts no pool";

    if (problems.length)
        throw new FarmHostPlanException(problems);

    const(PoolResolution) selection = chosen.length ? reresolve(snap, scope_, options.pool, chosen, problems) : null;
    if (problems.length)
        throw new FarmHostPlanException(problems);

    auto result = new FarmHostPlan(options, candidates, selection);
    result.farmGroup = farmGroup;
    result.groups = groups;
    result.workers = cast(uint) chosen.length;
    result.caller = caller;
    result.callerCpus = callerCpus;
    result.callerSharesWorkerCpu = callerShares;
    result.callerConsumes = callerConsumes;

    HostParticipants p;
    if (selection !is null)
        foreach (id; selection.workers)
        {
            if (candidates[id].llcIndex == farmGroup) p.nativeConsumers++;
            else p.foreignConsumers++;
        }
    immutable external = cast(ulong)(callerConsumes ? 1 : 0) + options.farm.extraConsumers;
    immutable consumers = external + chosen.length;
    immutable smallProducers = 1UL + options.farm.extraSmallProducers;
    p.externalConsumers = cast(uint) external;
    p.smallProducers = cast(uint) smallProducers;
    p.bulkProducers = options.farm.bulkProducers;
    result.participants = p;
    result.crossGroup = p.foreignConsumers != 0;

    if (consumers < 1 || consumers > MAX_CONSUMERS_LIMIT)
        problems ~= format("%s consumers (%s workers + %s external) exceed the Farm's %s-view limit",
            consumers, chosen.length, external, MAX_CONSUMERS_LIMIT);
    if (smallProducers > uint.max)
        problems ~= "extraSmallProducers overflows the small producer count";
    if (problems.length)
        throw new FarmHostPlanException(problems);
    result.expectedConsumers = cast(uint) consumers;
    result.maxSmall = cast(uint) smallProducers;
    result.maxBulk = options.farm.bulkProducers;

    const f = options.farm;
    immutable geometry = AntFarm.checkGeometry(f.ringMiB, f.k, result.expectedConsumers,
        result.maxBulk, f.quotaBulk, result.maxSmall, f.quotaSmall);
    if (geometry.kind != FarmCreateFailure.none)
        throw new FarmHostPlanException(["Farm sizing: " ~ geometry.detail]);
    result.notes = notes;
    return result;
}

/// The groups supplying workers, home first; empty plus problems when none.
private ushort[] selectGroups(const PoolResolution candidates, const HostGroups wanted,
    ref string[] problems)
{
    import std.format : format;

    final switch (wanted.kind)
    {
        case HostGroups.Kind.automatic:
            ushort best;
            uint most;
            foreach (ref gd; candidates.groups)
                if (gd.accepted > most)
                {
                    most = gd.accepted;
                    best = gd.llcIndex;
                }
            if (most == 0)
            {
                problems ~= "no group has an accepted CPU: " ~ candidates.summary();
                return null;
            }
            return [best];
        case HostGroups.Kind.only:
        case HostGroups.Kind.span:
            if (wanted.llcs.length == 0)
            {
                problems ~= "HostGroups.span needs at least one group";
                return null;
            }
            ushort[] result;
            foreach (i, g; wanted.llcs)
            {
                if (g >= candidates.groups.length)
                    problems ~= format("group %s does not exist (%s physical groups)",
                        g, candidates.groups.length);
                else if (!candidates.groups[g].isAccepted)
                    problems ~= format("group %s has no accepted CPU: %s", g, groupWhy(candidates, g));
                else
                {
                    bool duplicate;
                    foreach (earlier; wanted.llcs[0 .. i])
                        if (earlier == g) duplicate = true;
                    if (duplicate)
                        problems ~= format("group %s is listed twice", g);
                    else
                        result ~= g;
                }
            }
            return result.length == wanted.llcs.length ? result : null;
    }
}

/// Why group `g` has no worker: its common reasons, or every reason seen
/// when its CPUs disagree.
private string groupWhy(const PoolResolution candidates, ushort g)
{
    const gd = candidates.groups[g];
    if (gd.common != Reject.none)
        return reasonsText(gd.common);
    Reject any;
    foreach (ref c; candidates.cpus)
        if (c.llcIndex == g)
            any |= c.reasons;
    return "mixed " ~ reasonsText(any);
}

/// Resolve `chosen` as the exact worker set; its order is the start order.
private const(PoolResolution) reresolve(ref const TopologySnapshot snap, ref const OsScope scope_,
    const PoolOptions pool, const(ProcessorId)[] chosen, ref string[] problems)
{
    import std.algorithm.sorting : sort;

    PoolOptions exact = pool;
    exact.processors = chosen.dup;
    auto selection = resolvePool(snap, scope_, exact);
    auto want = chosen.dup;
    auto got = selection.workers.dup;
    want.sort();
    got.sort();
    if (want != got)
        problems ~= "internal: the final resolution selected {" ~ formatCpuList(selection.workers)
            ~ "} instead of {" ~ formatCpuList(chosen) ~ "}";
    return selection;
}

private string groupsText(const(ushort)[] groups)
{
    import std.conv : to;

    string s;
    foreach (i, g; groups)
        s ~= (i ? "," : "") ~ g.to!string;
    return s;
}
