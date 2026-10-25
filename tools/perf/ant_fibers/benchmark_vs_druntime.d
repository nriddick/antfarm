/++
 + Single-thread comparison: DRuntime `Fiber` vs ant_fibers vs a bare
 + Ant Farm payload. The DRuntime side uses the usual programmer tricks:
 + reserve the Fiber array, `Fiber.reset` instead of reallocating, a single
 + recycled Fiber when N concurrent stacks are not required, a two-pass
 + call for a known one-yield body, and `GC.disable` on loops that do not
 + allocate.
 +
 + After the ST table, the same binary sweeps Ant Farm + threadpool worker
 + sets on this machine (SMT on/off, 1..N LPs, flush/avgCost) and reports
 + warm fiber drain and bare-payload drain against that ST baseline. The
 + worker sets come from one LLC group (the group of the first eligible CPU),
 + because the single lane binds to one group: on multi-LLC hosts they are
 + capped to one LLC's CPUs.
 +/
module benchmark_vs_druntime;

import antfarm;
import ant_fibers;
import core.atomic;
import core.memory : GC;
import core.thread : Fiber, Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.stdio : stdout, writefln, writeln;
import threadpool;

shared long ran;
shared long mtHits;

void workFn() { atomicFetchAdd(ran, 1L); }

void yieldFn()
{
    atomicFetchAdd(ran, 1L);
    Fiber.yield();
    atomicFetchAdd(ran, 1L);
}

void farmYieldFn()
{
    atomicFetchAdd(ran, 1L);
    FiberDomain.yieldReady();
    atomicFetchAdd(ran, 1L);
}

extern(C) void payloadCallback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    atomicFetchAdd(ran, 1L);
    return;
}

extern(C) void mtPayloadCallback(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    atomicFetchAdd(mtHits, 1L);
    return;
}

struct Topo
{
    string name;
    bool skipSmt;
    size_t workers; // 0 = all LPs after SMT filter
    size_t flushBatch = 256;
    uint avgCost = avgCostForChunk(32);
}

/// Up to `workers` (0 = all) eligible CPUs of ONE LLC group, the group of the
/// first eligible CPU. A Fiber plan binds the single lane to one group, so on
/// multi-LLC hosts these runs are capped to that group's CPUs.
ProcessorId[] pickLps(bool skipSmt, size_t workers)
{
    auto snap = CacheAwarePool.topology();
    ProcessorId[] lps;
    foreach (id; snap.eligibleProcessorsInLlc(firstEligibleGroup()))
    {
        if (skipSmt && snap.byId(id).smtSibling) continue;
        lps ~= id;
        if (workers != 0 && lps.length == workers) break;
    }
    return lps;
}

/// The LLC group of the first eligible CPU: the one group these runs use.
ushort firstEligibleGroup()
{
    auto snap = CacheAwarePool.topology();
    foreach (ref lp; snap.processors)
        if (snap.isEligible(lp)) return lp.llcIndex;
    throw new Exception("no eligible logical processors");
}

void waitUntil(bool delegate() done, string what)
{
    immutable deadline = MonoTime.currTime + 30.seconds;
    while (!done())
    {
        if (MonoTime.currTime > deadline)
            throw new Exception("timeout waiting for " ~ what);
        Thread.yield();
    }
}

/// One FiberLane on this host's LLC map, `lps.length` pinned workers.
/// Returns warm fiber drain /s and payload drain /s.
Rate[2] runMtTopo(size_t n, Topo topo, ProcessorId[] lps)
{
    enum ln = 1 << 22;
    enum k = 8;
    enum segCap = ln / k;
    immutable slots = cast(uint)(lps.length + 1); // workers + control payload producer
    immutable quotaHeadroom = (k - 1) * segCap / slots;
    immutable quotaSmall = quotaHeadroom > 16_384 ? 16_384 : quotaHeadroom;
    auto farm = AntFarm.create(ringMiBFromUlongs(ln), k, cast(uint) lps.length, 0, 0, slots, quotaSmall);
    scope (exit) farm.destroy();
    auto lane = new FiberLane(farm);
    lane.flushBatch = topo.flushBatch;
    lane.avgCost = topo.avgCost;

    PoolOptions options;
    options.skipSmtSiblings = topo.skipSmt;
    options.processors = lps;
    options.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(options);
    immutable group = CacheAwarePool.topology().byId(lps[0]).llcIndex;
    installFiberPlan(new FiberServicePlan(pool.resolve()).bind(group, lane), pool);
    scope (exit) uninstallFiberPlan();
    pool.start();
    scope (exit) pool.shutdown();
    writefln("    group %s: %s", group, pool.resolution.summary());
    lane.domain.reserve(n);

    void burst()
    {
        foreach (_; 0 .. n)
            lane.domain.spawn({ atomicFetchAdd(mtHits, 1L); });
        pool.wakeAll();
    }

    atomicStore(mtHits, 0);
    burst();
    waitUntil(() => lane.drained, topo.name ~ " cold drain");
    lane.domain.retire();

    atomicStore(mtHits, 0);
    burst();
    auto t0 = MonoTime.currTime;
    waitUntil(() => lane.drained, topo.name ~ " warm drain");
    immutable fiberSec = (MonoTime.currTime - t0).total!"nsecs" / 1e9;
    assert(atomicLoad(mtHits) == n);
    lane.domain.retire();

    atomicStore(mtHits, 0);
    Callback header;

    header = &mtPayloadCallback;
    ulong word;
    PayloadEntry[256] chunk;
    foreach (i; 0 .. chunk.length)
        chunk[i] = PayloadEntry(header, (&word)[0 .. 1]);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    pool.wakeAll();
    t0 = MonoTime.currTime;
    size_t written;
    while (written < n || atomicLoad(mtHits) < n)
    {
        if (written < n)
        {
            immutable take = n - written;
            immutable batch = take < chunk.length ? take : chunk.length;
            immutable w = farm.write(chunk[0 .. batch], token, topo.avgCost);
            if (w == 0)
            {
                pool.wakeAll();
                Thread.yield();
            }
            else written += w;
        }
        else
        {
            pool.wakeAll();
            Thread.yield();
        }
    }
    immutable payloadSec = (MonoTime.currTime - t0).total!"nsecs" / 1e9;
    return [
        Rate(topo.name ~ " fiber", fiberSec, n / fiberSec),
        Rate(topo.name ~ " payload", payloadSec, n / payloadSec),
    ];
}

struct Rate
{
    string name;
    double seconds;
    double perSec;
}

void report(Rate r)
{
    writefln("  %-36s %9.4f s  %10.2f /s", r.name, r.seconds, r.perSec);
}

void main(string[] args)
{
    immutable n = args.length > 1 ? args[1].to!size_t : 100_000;
    immutable flushMax = args.length > 2 ? args[2].to!size_t : 256;

    auto farm = AntFarm.create(8, 8, 1, 0, 0, 1, 16_384);
    scope (exit) farm.destroy();
    auto domain = new FiberDomain(farm);
    domain.reserve(n);
    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);
    ConsumerView consumer;
    auto subscribed = subscribeOrThrow(consumer, farm);
    scope (exit) consumer.unsubscribe();

    writefln("n=%s flush=%s hugePages=%s  (ST)",
             n, flushMax, farm.usedLargePages);
    writeln();
    stdout.flush();

    Rate[] rows;

    void note(string name, size_t count, MonoTime t0)
    {
        immutable seconds = (MonoTime.currTime - t0).total!"nsecs" / 1e9;
        rows ~= Rate(name, seconds, count / seconds);
    }

    void farmComplete(void delegate() dg, string createName, string runName,
        size_t expected)
    {
        atomicStore(ran, 0);
        auto t0 = MonoTime.currTime;
        foreach (_; 0 .. n) domain.spawn(dg);
        note(createName, n, t0);
        t0 = MonoTime.currTime;
        drainUntilEmpty(domain, token, consumer, flushMax, avgCostForChunk(32));
        note(runName, n, t0);
        assert(atomicLoad(ran) == expected);
        domain.retire();
    }

    farmComplete({ workFn(); }, "antfarm fiber create", "antfarm fiber run", n);
    farmComplete({ workFn(); }, "antfarm fiber create (warm)",
        "antfarm fiber run (warm)", n);
    farmComplete({ farmYieldFn(); }, "antfarm fiber 1-yield create",
        "antfarm fiber 1-yield run", n * 2);

    {
        atomicStore(ran, 0);
        Callback header;

        header = &payloadCallback;
        ulong word;
        PayloadEntry[256] chunk;
        foreach (i; 0 .. chunk.length)
            chunk[i] = PayloadEntry(header, (&word)[0 .. 1]);

        auto t0 = MonoTime.currTime;
        size_t written;
        size_t visits;
        while (written < n || atomicLoad(ran) < n)
        {
            if (written < n)
            {
                immutable take = n - written;
                immutable batch = take < chunk.length ? take : chunk.length;
                written += farm.write(chunk[0 .. batch], token, avgCostForChunk(32));
            }
            consumer.consumeNext();
            if (++visits == 4_000_000)
                throw new Exception("payload drain stalled");
        }
        note("antfarm payload run", n, t0);
        assert(atomicLoad(ran) == n);
    }

    {
        atomicStore(ran, 0);
        Fiber[] fibers;
        auto t0 = MonoTime.currTime;
        foreach (_; 0 .. n) fibers ~= new Fiber(&workFn);
        note("druntime naive create", n, t0);
        t0 = MonoTime.currTime;
        GC.disable();
        foreach (f; fibers) f.call();
        GC.enable();
        note("druntime naive run", n, t0);
        assert(atomicLoad(ran) == n);
    }

    Fiber[] pool;
    pool.reserve(n);
    {
        atomicStore(ran, 0);
        auto t0 = MonoTime.currTime;
        foreach (_; 0 .. n) pool ~= new Fiber(&workFn);
        note("druntime reserve+create", n, t0);
        t0 = MonoTime.currTime;
        GC.disable();
        foreach (f; pool) f.call();
        GC.enable();
        note("druntime reserved run", n, t0);
        assert(atomicLoad(ran) == n);
    }
    {
        atomicStore(ran, 0);
        auto t0 = MonoTime.currTime;
        GC.disable();
        foreach (f; pool) f.reset(&workFn);
        GC.enable();
        note("druntime reset (warm)", n, t0);
        t0 = MonoTime.currTime;
        GC.disable();
        foreach (f; pool) f.call();
        GC.enable();
        note("druntime reset+run (warm)", n, t0);
        assert(atomicLoad(ran) == n);
    }
    {
        atomicStore(ran, 0);
        auto fiber = new Fiber(&workFn);
        auto t0 = MonoTime.currTime;
        GC.disable();
        foreach (i; 0 .. n)
        {
            fiber.call();
            if (i + 1 != n) fiber.reset(&workFn);
        }
        GC.enable();
        note("druntime 1x Fiber reset+run", n, t0);
        assert(atomicLoad(ran) == n);
    }
    {
        atomicStore(ran, 0);
        Fiber[] ypool;
        ypool.reserve(n);
        foreach (_; 0 .. n) ypool ~= new Fiber(&yieldFn);
        auto t0 = MonoTime.currTime;
        foreach (f; ypool) f.call();
        foreach (f; ypool) f.call();
        note("druntime 1-yield two-pass run", n, t0);
        assert(atomicLoad(ran) == n * 2);
    }

    writeln("ST rate");
    foreach (r; rows) report(r);
    stdout.flush();

    pool = null;
    GC.collect();
    GC.minimize();

    auto snap = CacheAwarePool.topology();
    writeln();
    writefln("topology: %s LLC(s), %s LP(s), %s P-cores, hugePages=%s",
             snap.llcCount, snap.logicalProcessorCount, snap.pCoreCount,
             farm.usedLargePages);
    writefln("MT worker sets: LLC group %s only (one lane, one group)",
             firstEligibleGroup());

    Topo[] topos = [
        Topo("1-worker", false, 1),
        Topo("2-workers", false, 2),
        Topo("4-workers", false, 4),
        Topo("6-phys", true, 6),
        Topo("6-phys flush32/2", true, 6, 32, avgCostForChunk(8)),
        Topo("12-smt", false, 12),
        Topo("12-smt flush32/2", false, 12, 32, avgCostForChunk(8)),
    ];

    Rate[] mtRows;
    Rate bestFiber, bestPayload;
    bestFiber.perSec = -1;
    bestPayload.perSec = -1;
    foreach (topo; topos)
    {
        auto lps = pickLps(topo.skipSmt, topo.workers);
        if (lps.length == 0) continue;
        topo.workers = lps.length;
        writefln("  running %s  lps=%s  flush=%s avgCost=%s",
                 topo.name, lps.length, topo.flushBatch, topo.avgCost);
        stdout.flush();
        Rate[2] pair;
        try
            pair = runMtTopo(n, topo, lps);
        catch (Throwable failure)
        {
            writefln("  FAILED %s: %s", topo.name, failure.msg);
            stdout.flush();
            GC.collect();
            continue;
        }
        mtRows ~= pair[0];
        mtRows ~= pair[1];
        if (pair[0].perSec > bestFiber.perSec) bestFiber = pair[0];
        if (pair[1].perSec > bestPayload.perSec) bestPayload = pair[1];
        GC.collect();
        GC.minimize();
        stdout.flush();
    }

    writeln();
    writeln("MT rate");
    foreach (r; mtRows) report(r);
    writeln();
    writefln("best MT fiber   %s  %.2f /s", bestFiber.name, bestFiber.perSec);
    writefln("best MT payload %s  %.2f /s", bestPayload.name, bestPayload.perSec);
}
