module threadpool.exchange;

import std.traits : isIntegral, isSomeString;
import threadpool.hybrid : WorkerSelf, currentWorker;

/**
 * Hypothetical cross-NUMA exchange.
 *
 * LLC `install` is intra-node: `home!C()` / `search` never leave the worker's
 * NUMA. Exchange is a second table of C's, one per NUMA node, meant as
 * ingress endpoints for traffic that *does* leave the node.
 *
 * Layout (v1 stub): `bins[numaIndex]`. You allocate `t.numaNodes.length`
 * objects elsewhere (ideally node-local for that index), then
 * `installExchange(bins)`.
 *
 * Worker queries:
 *   exchangeHome!C()     — C this node *receives* on (drain)
 *   exchangeTo!C(remote) — C you *publish* into to reach `remote`
 *                          (null if remote == home NUMA; use `home!C` for that)
 *   foreach n in 0 .. exchangeCount!C(): exchangeTo!C(n)
 *
 * Not implemented: pairwise matrix `bins[src * n + dst]`, byte transfer,
 * worker-loop draining of exchange C's. Same C type can be LLC-installed
 * and exchange-installed independently (separate registries).
 *
 * Thread-safety: `installExchange` / `uninstallExchange` / `setExchangeLabel`
 * / `installExchangePairs` are setup-only (not thread-safe). Call from
 * `shared static this()` or one thread before the pool starts. Lookups
 * (`exchangeHome`, `exchangeTo`, `exchangeCount`, `atExchange`,
 * `exchangeSearch`) are thread-safe after that until uninstall/re-install.
 */

private struct XLabel
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

private struct XReg(C)
{
    static __gshared C[]      bins;
    static __gshared XLabel[] labels;
}

/// `bins[i]` is the ingress C for NUMA node `i`. Length is the NUMA count.
/// Not thread-safe. Setup only (e.g. `shared static this`).
void installExchange(C)(C[] bins)
{
    if (bins.length == 0)
        throw new Exception("threadpool: installExchange requires one C per NUMA node");
    uninstallExchange!C();
    XReg!C.bins = bins;
    XReg!C.labels = new XLabel[](bins.length);
}

/// Not thread-safe. Setup / teardown only.
void uninstallExchange(C)()
{
    XReg!C.bins = null;
    XReg!C.labels = null;
}

/// Thread-safe after `installExchange!C`.
ushort exchangeCount(C)() @nogc nothrow
{
    return cast(ushort) XReg!C.bins.length;
}

/// Ingress C for this worker's NUMA node. Null if no worker or nothing installed.
/// Thread-safe after `installExchange!C`.
C* exchangeHome(C)() @nogc nothrow
{
    auto w = currentWorker();
    if (w is null) return null;
    return atExchange!C(w.numaIndex);
}

/// Egress C toward `remoteNuma`. Null if that is this worker's own node (use
/// `home!C()` for intra-NUMA) or the index is out of range.
/// Thread-safe after `installExchange!C`.
C* exchangeTo(C)(ushort remoteNuma) @nogc nothrow
{
    auto w = currentWorker();
    if (w is null) return null;
    if (remoteNuma == w.numaIndex) return null;
    return atExchange!C(remoteNuma);
}

/// Unfiltered table slot. Null if OOB or uninstalled.
/// Thread-safe after `installExchange!C` (producer threads use this).
C* atExchange(C)(ushort numaIndex) @nogc nothrow
{
    if (numaIndex >= XReg!C.bins.length) return null;
    return &XReg!C.bins[numaIndex];
}

/// Not thread-safe. Setup only. Do not call concurrently with `exchangeSearch`.
bool setExchangeLabel(C, L)(C* c, L label)
{
    if (c is null || XReg!C.bins.length == 0) return false;
    const idx = c - XReg!C.bins.ptr;
    if (idx < 0 || idx >= cast(ptrdiff_t) XReg!C.bins.length) return false;
    XReg!C.labels[idx].set(label);
    return true;
}

/// Not thread-safe. Setup only. Do not call concurrently with `exchangeSearch`.
bool setExchangeLabel(C, L)(ushort numaIndex, L label)
{
    if (numaIndex >= XReg!C.bins.length) return false;
    XReg!C.labels[numaIndex].set(label);
    return true;
}

/// Label lookup on the exchange table. From a worker, skips the home NUMA
/// (those C's are not "cross" from here). Off a worker, first match.
/// Thread-safe after `installExchange` / `setExchangeLabel`.
C* exchangeSearch(C, L)(L label) @nogc nothrow
{
    auto w = currentWorker();
    foreach (i; 0 .. XReg!C.bins.length)
    {
        if (w !is null && i == w.numaIndex)
            continue;
        if (XReg!C.labels[i].matches(label))
            return &XReg!C.bins[i];
    }
    return null;
}

/// Pairwise `bins[srcNuma * n + dstNuma]` — not installed in this stub.
/// Not thread-safe (would be setup-only).
void installExchangePairs(C)(C[] bins, ushort numaCount)
{
    throw new Error("threadpool: pairwise numaExchange is not implemented");
}

unittest
{
    import threadpool.hybrid : setCurrentWorker;

    struct X { int id; }

    auto xs = new X[](2);
    xs[0].id = 100;
    xs[1].id = 101;
    installExchange(xs);
    scope (exit) uninstallExchange!X();
    setExchangeLabel!X(cast(ushort) 0, "node0");
    setExchangeLabel!X(cast(ushort) 1, "node1");

    assert(exchangeCount!X() == 2);
    assert(exchangeHome!X() is null);
    assert(exchangeTo!X(1) is null);

    WorkerSelf w0;
    w0.numaIndex = 0;
    w0.llcIndex = 0;
    w0.numaLlcCount = 1;
    w0.numaLlcs[0] = 0;

    WorkerSelf w1 = w0;
    w1.numaIndex = 1;
    w1.llcIndex = 1;
    w1.numaLlcs[0] = 1;

    setCurrentWorker(&w0);
    assert(exchangeHome!X() !is null && exchangeHome!X().id == 100);
    assert(exchangeTo!X(0) is null, "local NUMA is not an exchange target");
    assert(exchangeTo!X(1) !is null && exchangeTo!X(1).id == 101);
    assert(exchangeSearch!(X, string)("node1") is atExchange!X(1));
    assert(exchangeSearch!(X, string)("node0") is null, "home ingress is not a remote search hit");

    setCurrentWorker(&w1);
    assert(exchangeHome!X().id == 101);
    assert(exchangeTo!X(0).id == 100);
    assert(exchangeTo!X(1) is null);
    assert(exchangeSearch!(X, string)("node0") is atExchange!X(0));

    setCurrentWorker(null);
    assert(exchangeSearch!(X, string)("node0") is atExchange!X(0));
}
