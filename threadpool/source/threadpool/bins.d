module threadpool.bins;

/**
 * Physical-group-indexed registry of caller-owned C's.
 *
 * The table is keyed by the physical `llcIndex`, never compacted: build it
 * with `makeBins` from a `PoolResolution` and install it with that
 * resolution, which validates the length and every accepted group's slot.
 *
 * Setup (`install`, `uninstall`, `setLabel`) is **not** thread-safe. Call it
 * from `shared static this()` or one thread before `CacheAwarePool.start`.
 * Lookups (`home`, `search`, `at`, `homeFor`) are thread-safe after that
 * setup has completed, until a matching `uninstall` / re-`install`.
 *
 * Do not race lookups with `install` / `setLabel` / `uninstall`.
 */

import std.traits : isIntegral, isSomeString;
import threadpool.hybrid : WorkerSelf, currentWorker;
import threadpool.resolution : GroupDisposition, PoolResolution;

/// How a registered set of C is laid out against the CPU map.
private enum BinAxis : ubyte
{
    llc,          // bins[llcIndex] — P and E on that LLC share one C
    llcAndClass,  // bins[llcIndex * classCount + classIndex]
}

private struct BinLabel
{
    enum Tag : ubyte { none, num, str }
    Tag    tag;
    long   num;
    string str;

    void set(L)(L v)
    {
        static if (isSomeString!L)
        {
            tag = Tag.str;
            str = v.idup;
            num = 0;
        }
        else static if (isIntegral!L)
        {
            tag = Tag.num;
            num = v;
            str = null;
        }
        else
            static assert(0, "label must be a string or integer");
    }

    bool matches(L)(L v) const
    {
        static if (isSomeString!L)
            return tag == Tag.str && str == v;
        else static if (isIntegral!L)
            return tag == Tag.num && num == v;
        else
            static assert(0, "label must be a string or integer");
    }
}

private struct Registry(C)
{
    static __gshared C[]       bins;
    static __gshared BinLabel[] labels;
    static __gshared ushort    llcCount;
    static __gshared ushort    classCount;
    static __gshared BinAxis   axis;
    /// Per physical group: accepted in the validating resolution.
    static __gshared bool[]    staffedMask;
}

private size_t slotIndex(C)(ushort llc, ubyte cls) @nogc nothrow
{
    auto classN = Registry!C.classCount;
    if (Registry!C.axis == BinAxis.llc || classN <= 1)
        return llc;
    return cast(size_t) llc * classN + cls;
}

private bool inTable(C)(size_t idx) @nogc nothrow
{
    return Registry!C.bins.length > 0 && idx < Registry!C.bins.length;
}

/// Build a physical-group-indexed array: `make` runs once per physical group,
/// in `llcIndex` order, and sees that group's disposition. It may return
/// `C.init` / null for groups without accepted workers.
C[] makeBins(C)(const PoolResolution plan, scope C delegate(ref const GroupDisposition g) make)
{
    auto bins = new C[](plan.physicalGroupCount);
    foreach (i, ref g; plan.groups)
        bins[i] = make(g);
    return bins;
}

/// One C per physical group: `bins.length` must equal
/// `plan.physicalGroupCount`. For class or pointer C, every accepted group's
/// slot must be non-null. Not thread-safe. Setup only.
void install(C)(C[] bins, const PoolResolution plan)
{
    installChecked!C(bins, plan, BinAxis.llc, 1);
}

/// One C per (physical group × dense class): `bins.length` must equal
/// `plan.physicalGroupCount * classCount`, indexed `llc * classCount + class`.
/// For class or pointer C, every slot an accepted worker resolves to must be
/// non-null. Not thread-safe. Setup only.
void install(C)(C[] bins, const PoolResolution plan, ushort classCount)
{
    installChecked!C(bins, plan, BinAxis.llcAndClass, classCount);
}

private void installChecked(C)(C[] bins, const PoolResolution plan, BinAxis axis,
    ushort classCount)
{
    import std.conv : to;

    if (plan is null)
        throw new Exception("threadpool: install requires a PoolResolution");
    immutable groups = plan.physicalGroupCount;
    immutable classN = axis == BinAxis.llc || classCount == 0 ? ushort(1) : classCount;
    immutable want = cast(size_t) groups * classN;
    if (groups == 0 || bins.length != want)
        throw new Exception("threadpool: install!" ~ C.stringof ~ " needs " ~ want.to!string
            ~ " slots (" ~ groups.to!string ~ " physical groups x " ~ classN.to!string
            ~ " classes), got " ~ bins.length.to!string
            ~ "; build the table with makeBins from the same resolution");
    static if (is(C == class) || is(C == interface) || is(C == T*, T))
    {
        string missing;
        auto reported = new bool[](bins.length);
        foreach (ref c; plan.cpus)
        {
            if (!c.accepted) continue;
            immutable idx = classN <= 1 ? c.llcIndex : cast(size_t) c.llcIndex * classN + c.tableClass;
            if (bins[idx] is null && !reported[idx])
            {
                reported[idx] = true;
                missing ~= (missing.length ? ", " : "") ~ (classN <= 1 ? c.llcIndex.to!string
                    : c.llcIndex.to!string ~ "/class " ~ c.tableClass.to!string);
            }
        }
        if (missing.length)
            throw new Exception("threadpool: install!" ~ C.stringof
                ~ " has null slots for accepted groups: " ~ missing);
    }
    auto staffed = new bool[](groups);
    foreach (ref g; plan.groups)
        staffed[g.llcIndex] = g.isAccepted;

    uninstall!C();
    Registry!C.bins = bins;
    Registry!C.labels = new BinLabel[](bins.length);
    Registry!C.llcCount = groups;
    Registry!C.classCount = classN;
    Registry!C.axis = axis;
    Registry!C.staffedMask = staffed;
}

/// True when the installed table's group `llc` was accepted in the
/// validating resolution. Thread-safe after `install!C`.
bool binStaffed(C)(ushort llc) @nogc nothrow
{
    return llc < Registry!C.staffedMask.length && Registry!C.staffedMask[llc];
}

/// Not thread-safe. Setup / teardown only. Do not call while workers use `home` / `search`.
void uninstall(C)()
{
    Registry!C.bins = null;
    Registry!C.labels = null;
    Registry!C.llcCount = 0;
    Registry!C.classCount = 0;
    Registry!C.axis = BinAxis.llc;
    Registry!C.staffedMask = null;
}

/// Pointer form of `ref C home(C)()` — D refs cannot be null.
/// Thread-safe after `install!C` and until `uninstall!C`. Null if no current worker.
C* home(C)() @nogc nothrow
{
    auto w = currentWorker();
    if (w is null) return null;
    return homeFor!C(w);
}

/// Thread-safe after `install!C`. Null if `w` is null or nothing installed.
C* homeFor(C)(WorkerSelf* w) @nogc nothrow
{
    if (w is null || Registry!C.bins.length == 0) return null;
    auto idx = slotIndex!C(w.llcIndex, w.classIndex);
    if (!inTable!C(idx)) return null;
    return &Registry!C.bins[idx];
}

/// Pointer form of `ref C search(C, L)(L label)`.
/// From a pool worker: first NUMA-local match. Otherwise: first match in the set.
/// Thread-safe after `install!C` / `setLabel` and until those run again.
C* search(C, L)(L label) @nogc nothrow
{
    auto w = currentWorker();
    if (w !is null)
        return searchNuma!C(w, label);
    foreach (i; 0 .. Registry!C.bins.length)
    {
        if (Registry!C.labels[i].matches(label))
            return &Registry!C.bins[i];
    }
    return null;
}

private C* searchNuma(C, L)(WorkerSelf* w, L label) @nogc nothrow
{
    if (w is null || Registry!C.bins.length == 0) return null;
    foreach (i; 0 .. w.numaLlcCount)
    {
        auto idx = slotIndex!C(w.numaLlcs[i], w.classIndex);
        if (inTable!C(idx) && Registry!C.labels[idx].matches(label))
            return &Registry!C.bins[idx];
    }
    return null;
}

/// Not thread-safe. Setup only. Do not call concurrently with `search`.
bool setLabel(C, L)(C* c, L label)
{
    if (c is null || Registry!C.bins.length == 0) return false;
    const idx = c - Registry!C.bins.ptr;
    if (idx < 0 || idx >= cast(ptrdiff_t) Registry!C.bins.length) return false;
    Registry!C.labels[idx].set(label);
    return true;
}

/// Not thread-safe. Setup only. Do not call concurrently with `search`.
bool setLabel(C, L)(ushort llc, L label, ubyte classIndex = 0)
{
    if (Registry!C.bins.length == 0) return false;
    auto idx = slotIndex!C(llc, classIndex);
    if (!inTable!C(idx)) return false;
    Registry!C.labels[idx].set(label);
    return true;
}

/// Direct table access. Null if uninstalled or OOB. Not NUMA-filtered.
/// Thread-safe after `install!C` (SDL / producer threads use this).
C* at(C)(ushort llc, ubyte classIndex = 0) @nogc nothrow
{
    if (Registry!C.bins.length == 0) return null;
    auto idx = slotIndex!C(llc, classIndex);
    if (!inTable!C(idx)) return null;
    return &Registry!C.bins[idx];
}

version (unittest)
{
    import threadpool.pool : PoolOptions;
    import threadpool.resolution : resolvePool;
    import threadpool.os_scope : OsScope;
    import threadpool.topology;

    /// Two physical groups of two CPUs each; `excludeGroup1` puts group 1
    /// outside the OS scope. `hybrid` makes CPU 1 of each group an E-core.
    private const(PoolResolution) twoGroupPlan(bool excludeGroup1 = false, bool hybrid = false)
    {
        LogicalProcessor[] procs;
        foreach (i; 0 .. 4)
        {
            LogicalProcessor p;
            p.lpIndex = cast(ushort) i;
            p.coreIndex = cast(ushort) i;
            p.llcIndex = cast(ushort)(i / 2);
            p.efficiencyClass = hybrid && i % 2 == 1 ? 0 : 1;
            p.affinityExcluded = excludeGroup1 && i >= 2;
            procs ~= p;
        }
        auto snap = assembleSnapshot("synthetic", procs, null, null, 2, 64,
            [GroupingSource.l3, GroupingSource.l3]);
        OsScope unrestricted;
        return resolvePool(snap, unrestricted, PoolOptions.init);
    }
}

unittest
{
    import threadpool.hybrid : setCurrentWorker;

    struct C1 { int a; }
    struct C2 { int b; }

    auto plan = twoGroupPlan();
    auto n1 = new C1[](2);
    auto n2 = new C2[](2);
    n1[0].a = 10;
    n1[1].a = 11;
    n2[0].b = 20;
    n2[1].b = 21;
    install(n1, plan);
    install(n2, plan);
    scope (exit)
    {
        uninstall!C1();
        uninstall!C2();
    }

    assert(home!C1() is null);
    assert(home!C2() is null);

    WorkerSelf w0;
    w0.llcIndex = 0;
    w0.numaIndex = 0;
    w0.classIndex = 0;
    w0.numaLlcCount = 2;
    w0.numaLlcs[0] = 0;
    w0.numaLlcs[1] = 1;

    WorkerSelf w1 = w0;
    w1.llcIndex = 1;
    w1.numaLlcs[0] = 1;
    w1.numaLlcs[1] = 0;
    w1.numaLlcCount = 2;

    setCurrentWorker(&w0);
    assert(home!C1() !is null && home!C1().a == 10);
    assert(home!C2() !is null && home!C2().b == 20);
    setCurrentWorker(&w1);
    assert(home!C1().a == 11);
    assert(home!C2().b == 21);
    setCurrentWorker(null);
    assert(home!C1() is null);

    assert(setLabel!C1(at!C1(0), "alpha"));
    assert(setLabel!C1(cast(ushort) 1, 7));
    assert(setLabel!C2(at!C2(0), "beta"));

    setCurrentWorker(&w0);
    assert(search!(C1, string)("alpha") is at!C1(0));
    assert(search!(C1, int)(7) is at!C1(1));
    assert(search!(C2, string)("beta") is at!C2(0));
    assert(search!(C1, string)("nope") is null);

    WorkerSelf only0 = w0;
    only0.numaLlcCount = 1;
    only0.numaLlcs[0] = 0;
    setCurrentWorker(&only0);
    assert(search!(C1, string)("alpha") is at!C1(0));
    assert(search!(C1, int)(7) is null, "label on LLC 1 is outside this worker's NUMA view");

    setCurrentWorker(null);
    assert(search!(C1, int)(7) is at!C1(1), "no worker: search scans the whole set");
}

unittest
{
    // B1: a compacted table (accepted groups only) is rejected.
    static final class Bin { int g; this(int g) { this.g = g; } }
    auto plan = twoGroupPlan(true);
    assert(plan.physicalGroupCount == 2 && plan.acceptedGroupCount == 1);
    bool threw;
    try
        install([new Bin(0)], plan);
    catch (Exception e)
        threw = true;
    assert(threw, "a compacted table must not install");
    assert(at!Bin(0) is null);

    // B2: null is fine for a rejected group, not for an accepted one.
    install([new Bin(0), null], plan);
    scope (exit) uninstall!Bin();
    assert(binStaffed!Bin(0) && !binStaffed!Bin(1));
    assert(!binStaffed!Bin(7));
    threw = false;
    try
        install([null, new Bin(1)], plan);
    catch (Exception e)
    {
        threw = true;
        import std.algorithm.searching : canFind;
        assert(e.msg.canFind("accepted groups: 0"));
    }
    assert(threw, "a null slot for an accepted group must not install");

    // LLC x class: every slot an accepted worker resolves to must exist.
    auto hybrid = twoGroupPlan(false, true);
    assert(hybrid.topology.classCount == 2);
    threw = false;
    try
        install([new Bin(0), new Bin(1), null, new Bin(3)], hybrid, 2);
    catch (Exception e)
        threw = true;
    assert(threw);
    install([new Bin(0), new Bin(1), new Bin(2), new Bin(3)], hybrid, 2);
    assert(at!Bin(1, 1).g == 3);
}

unittest
{
    // B3: makeBins calls `make` once per physical group with its disposition.
    import threadpool.resolution : Reject;
    auto plan = twoGroupPlan(true);
    ushort[] seen;
    auto bins = makeBins!int(plan, (ref const GroupDisposition g) {
        seen ~= g.llcIndex;
        if (g.llcIndex == 1)
            assert(!g.isAccepted && g.common == Reject.osExcluded && g.physical == 2);
        else
            assert(g.isAccepted && g.accepted == 2);
        return g.isAccepted ? 100 + g.llcIndex : -1;
    });
    assert(seen == [ushort(0), 1]);
    assert(bins == [100, -1]);
}

unittest
{
    // B4: the table is keyed by physical group, so a worker in group 1 finds
    // slot 1 even when group 0 has no workers.
    import threadpool.hybrid : setCurrentWorker;
    static final class Bin { int g; this(int g) { this.g = g; } }
    LogicalProcessor[] procs;
    foreach (i; 0 .. 4)
    {
        LogicalProcessor p;
        p.lpIndex = cast(ushort) i;
        p.coreIndex = cast(ushort) i;
        p.llcIndex = cast(ushort)(i / 2);
        p.affinityExcluded = i < 2;
        procs ~= p;
    }
    auto snap = assembleSnapshot("synthetic", procs, null, null, 2, 64,
        [GroupingSource.l3, GroupingSource.l3]);
    OsScope unrestricted;
    auto plan = resolvePool(snap, unrestricted, PoolOptions.init);
    install(makeBins!Bin(plan, (ref const GroupDisposition g) =>
        g.isAccepted ? new Bin(g.llcIndex) : null), plan);
    scope (exit) uninstall!Bin();

    WorkerSelf w;
    w.llcIndex = 1;
    setCurrentWorker(&w);
    scope (exit) setCurrentWorker(null);
    assert(home!Bin() !is null && (*home!Bin()).g == 1);
}
