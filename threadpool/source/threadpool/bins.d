module threadpool.bins;

/**
 * LLC-indexed registry of caller-owned C's.
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

/// How a registered set of C is laid out against the CPU map.
enum BinAxis : ubyte
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

/// One C per LLC. `bins.length` should be the process-wide llcCount.
/// Not thread-safe. Setup only (e.g. `shared static this`).
void install(C)(C[] bins)
{
    install!C(bins, BinAxis.llc, cast(ushort) bins.length, 1);
}

/// One C per (LLC × class). `bins.length` must be llcCount * classCount.
/// Not thread-safe. Setup only.
void install(C)(C[] bins, ushort llcCount, ushort classCount)
{
    install!C(bins, BinAxis.llcAndClass, llcCount, classCount);
}

/// Not thread-safe. Setup only.
void install(C)(C[] bins, BinAxis axis, ushort llcCount, ushort classCount)
{
    if (bins.length == 0)
        throw new Exception("threadpool: install requires a non-empty C[]");
    if (axis == BinAxis.llcAndClass)
    {
        if (llcCount == 0 || classCount == 0
            || bins.length != cast(size_t) llcCount * classCount)
            throw new Exception("threadpool: llcAndClass install size must be llcCount*classCount");
    }
    uninstall!C();
    Registry!C.bins = bins;
    Registry!C.labels = new BinLabel[](bins.length);
    Registry!C.llcCount = llcCount ? llcCount : cast(ushort) bins.length;
    Registry!C.classCount = classCount ? classCount : 1;
    Registry!C.axis = axis;
}

/// Not thread-safe. Setup / teardown only. Do not call while workers use `home` / `search`.
void uninstall(C)()
{
    Registry!C.bins = null;
    Registry!C.labels = null;
    Registry!C.llcCount = 0;
    Registry!C.classCount = 0;
    Registry!C.axis = BinAxis.llc;
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

unittest
{
    import threadpool.hybrid : setCurrentWorker;

    struct C1 { int a; }
    struct C2 { int b; }

    auto n1 = new C1[](2);
    auto n2 = new C2[](2);
    n1[0].a = 10;
    n1[1].a = 11;
    n2[0].b = 20;
    n2[1].b = 21;
    install(n1);
    install(n2);
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
