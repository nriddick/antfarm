/++
 + Sanity-check: the same one-shot Ant Farm payload through
 +   1. farm ST (producer thread also consumes)
 +   2. farm MT spin (pinned OS threads, tight consumeNext)
 +   3. threadpool managed pump that only consumeNext (spin vs idle)
 +   4. FiberLane managed pump (timers, coverage, flush)
 +
 + (2) is Ant Farm-specific throughput. (3) isolates the pool. (4) is the
 + fiber embedding. Page backing follows Ant Farm's default or the
 + ANTFARM_HUGE_PAGES override.
 +/
module benchmark_farm_embed;

import antfarm;
import antfarm_fibers;
import core.atomic;
import core.thread : Thread;
import core.time : MonoTime, seconds;
import std.conv : to;
import std.stdio : stdout, writefln, writeln;
import threadpool;
import threadpool.pin;

shared long gHits;
shared int gStop;

long payloadCb(PayloadHeader*, PayloadBody, ulong) nothrow @nogc @system
{
    atomicFetchAdd(gHits, 1L);
    return 1;
}

__gshared AntFarm* gFarm;
__gshared bool gPoolSpin;

final class PoolCons
{
    ConsumerView view;
    bool subscribed;
}

ushort[] pickLps(bool skipSmt, size_t workers)
{
    auto snap = CacheAwarePool.topology();
    ushort[] lps;
    foreach (ref lp; snap.processors)
    {
        version (linux)
            if (lp.parkedAtDiscovery) continue;
        if (skipSmt && lp.smtSibling) continue;
        lps ~= lp.lpIndex;
        if (workers != 0 && lps.length == workers) break;
    }
    return lps;
}

LogicalProcessor[] lpsToProcs(ushort[] lps)
{
    auto snap = CacheAwarePool.topology();
    LogicalProcessor[] outp;
    foreach (id; lps)
        foreach (ref lp; snap.processors)
            if (lp.lpIndex == id)
            {
                outp ~= lp;
                break;
            }
    return outp;
}

AntFarm* makeFarm(uint consumers)
{
    enum ln = 1 << 22;
    enum k = 8;
    enum segCap = ln / k;
    immutable slots = consumers + 1;
    immutable quotaHeadroom = (k - 1) * segCap / slots;
    immutable quotaSmall = quotaHeadroom > 16_384 ? 16_384 : quotaHeadroom;
    return AntFarm.create(ln, k, consumers, 0, 0, slots, quotaSmall);
}

void fillChunk(ref PayloadHeader header, ref ulong word, PayloadEntry[] chunk)
{
    header.maxCs = 1;
    header.done = 1;
    header.plen = 1;
    header.call = &payloadCb;
    foreach (i; 0 .. chunk.length)
        chunk[i] = PayloadEntry(&header, (&word)[0 .. 1]);
}

double produceWait(AntFarm* farm, size_t n, uint avgCost, CacheAwarePool pool)
{
    PayloadHeader header;
    ulong word;
    PayloadEntry[256] chunk;
    fillChunk(header, word, chunk[]);
    auto tok = farm.registerProducer(Tier.small);
    if (pool !is null) pool.wakeAll();
    auto t0 = MonoTime.currTime;
    size_t written;
    while (written < n || atomicLoad(gHits) < n)
    {
        if (written < n)
        {
            immutable take = n - written;
            immutable batch = take < chunk.length ? take : chunk.length;
            immutable w = farm.write(chunk[0 .. batch], tok, avgCost);
            if (w == 0)
            {
                if (pool !is null) pool.wakeAll();
                Thread.yield();
            }
            else
                written += w;
        }
        else
        {
            if (pool !is null) pool.wakeAll();
            Thread.yield();
        }
    }
    immutable seconds = (MonoTime.currTime - t0).total!"nsecs" / 1e9;
    farm.unregisterProducer(tok);
    return seconds;
}

double runSt(size_t n, uint avgCost)
{
    auto farm = makeFarm(1);
    scope (exit) farm.destroy();
    ConsumerView v;
    immutable sub = subscribeOrThrow(v, farm);
    scope (exit) v.unsubscribe();
    PayloadHeader header;
    ulong word;
    PayloadEntry[256] chunk;
    fillChunk(header, word, chunk[]);
    auto tok = farm.registerProducer(Tier.small);
    if (!tok.valid)
        throw new Exception("ST producer register failed");
    scope (exit) farm.unregisterProducer(tok);
    atomicStore(gHits, 0);
    auto t0 = MonoTime.currTime;
    size_t written;
    size_t visits;
    while (written < n || atomicLoad(gHits) < n)
    {
        if (written < n)
        {
            immutable take = n - written;
            immutable batch = take < chunk.length ? take : chunk.length;
            immutable w = farm.write(chunk[0 .. batch], tok, avgCost);
            if (w == 0) Thread.yield();
            else written += w;
        }
        v.consumeNext();
        if (++visits == 20_000_000)
            throw new Exception("ST payload stalled written="
                ~ written.to!string ~ " hits=" ~ atomicLoad(gHits).to!string);
    }
    return (MonoTime.currTime - t0).total!"nsecs" / 1e9;
}

final class SpinWorker : Thread
{
    AntFarm* farm;
    LogicalProcessor lp;

    this(AntFarm* farm, LogicalProcessor lp)
    {
        this.farm = farm;
        this.lp = lp;
        super(&entry);
    }

    void entry()
    {
        PinTarget t;
        t.cpuSetId = lp.cpuSetId;
        t.group = lp.group;
        t.lpIndex = lp.lpIndex;
        pinToLogicalProcessor(t);
        ConsumerView v;
        subscribeOrThrow(v, farm);
        while (atomicLoad(gStop) == 0)
            cast(void) v.consumeNext();
        v.unsubscribe();
    }
}

double runFarmSpin(size_t n, uint avgCost, ushort[] lps)
{
    auto procs = lpsToProcs(lps);
    auto farm = makeFarm(cast(uint) procs.length);
    scope (exit) farm.destroy();
    atomicStore(gStop, 0);
    atomicStore(gHits, 0);
    Thread[] th;
    foreach (ref lp; procs)
    {
        auto w = new SpinWorker(farm, lp);
        th ~= w;
        w.start();
    }
    waitCf(farm, procs.length);
    immutable seconds = produceWait(farm, n, avgCost, null);
    atomicStore(gStop, 1);
    foreach (t; th) t.join();
    return seconds;
}

void waitCf(AntFarm* farm, size_t n)
{
    auto deadline = MonoTime.currTime + 10.seconds;
    while (atomicLoad(farm.Cf) < cast(long) n)
    {
        if (MonoTime.currTime > deadline)
            throw new Exception("consumers did not subscribe");
        Thread.yield();
    }
}

void poolStart(WorkerSelf* w)
{
    auto c = new PoolCons;
    subscribeOrThrow(c.view, gFarm);
    c.subscribed = true;
    w.context = cast(void*) c;
}

ManagedPumpResult poolPump(WorkerSelf* w)
{
    auto c = cast(PoolCons) w.context;
    immutable did = c.view.consumeNext();
    if (gPoolSpin) return ManagedPumpResult.again;
    return did ? ManagedPumpResult.again : ManagedPumpResult.idle;
}

void poolStop(WorkerSelf* w)
{
    auto c = cast(PoolCons) w.context;
    if (c !is null && c.subscribed)
    {
        c.view.unsubscribe();
        c.subscribed = false;
    }
    w.context = null;
}

double runPool(size_t n, uint avgCost, ushort[] lps, bool spin)
{
    auto farm = makeFarm(cast(uint) lps.length);
    scope (exit) farm.destroy();
    gFarm = farm;
    gPoolSpin = spin;
    atomicStore(gHits, 0);
    PoolOptions opt;
    opt.onlyLps = lps;
    opt.managedWorker = ManagedWorkerHooks(&poolStart, &poolPump, &poolStop);
    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown();
    waitCf(farm, lps.length);
    return produceWait(farm, n, avgCost, pool);
}

double runFiberPump(size_t n, uint avgCost, ushort[] lps)
{
    auto farm = makeFarm(cast(uint) lps.length);
    scope (exit) farm.destroy();
    auto lane = new FiberLane(farm);
    lane.flushBatch = 256;
    lane.avgCost = avgCost;
    PoolOptions opt;
    opt.onlyLps = lps;
    opt.managedWorker = fiberWorkerHooks();
    auto pool = new CacheAwarePool(opt);
    installFiberLanes([lane], pool);
    scope (exit) uninstallFiberLanes();
    pool.start();
    scope (exit) pool.shutdown();
    atomicStore(gHits, 0);
    return produceWait(farm, n, avgCost, pool);
}

void report(string name, size_t n, double seconds)
{
    writefln("  %-28s %8.4f s  %10.2f /s", name, seconds, n / seconds);
}

void main(string[] args)
{
    immutable n = args.length > 1 ? args[1].to!size_t : 500_000;
    immutable avgCost = args.length > 2 ? args[2].to!uint : 0;
    auto snap = CacheAwarePool.topology();
    writefln("n=%s avgCost=%s  llc=%s lps=%s pcores=%s",
             n, avgCost, snap.llcCount, snap.logicalProcessorCount, snap.pCoreCount);
    writeln();

    writeln("antfarm ST (produce+consume)");
    stdout.flush();
    atomicStore(gHits, 0);
    auto st = runSt(n, avgCost);
    report("farm ST", n, st);
    stdout.flush();

    struct Scene { string name; bool skipSmt; size_t workers; }
    Scene[] scenes = [
        Scene("6-phys", true, 6),
        Scene("12-smt", false, 12),
    ];
    foreach (scene; scenes)
    {
        auto lps = pickLps(scene.skipSmt, scene.workers);
        if (lps.length == 0) continue;
        writefln("\n%s  workers=%s", scene.name, lps.length);
        stdout.flush();
        auto spin = runFarmSpin(n, avgCost, lps);
        report("farm spin consumeNext", n, spin);
        stdout.flush();
        auto pspin = runPool(n, avgCost, lps, true);
        report("pool pump spin", n, pspin);
        stdout.flush();
        auto pidle = runPool(n, avgCost, lps, false);
        report("pool pump idle+wake", n, pidle);
        stdout.flush();
        auto fiber = runFiberPump(n, avgCost, lps);
        report("fiber managed pump", n, fiber);
        stdout.flush();
    }
}
