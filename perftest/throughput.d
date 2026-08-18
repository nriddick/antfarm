/++
 + Synthetic throughput sweep for a 16 MiB Ant Farm.
 +
 +   make -C perftest run
 +
 + Default mode searches topology, then payload/table shape, and prints a
 + ranked table. `--once` runs a single configuration (see README).
 +/
module throughput;

import antfarm;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, atoi, abort, strtoull;

__gshared shared(long) g_calls;
__gshared shared(int) g_stop;
__gshared bool g_globalCount;   // --global-count: old one-global-atomic callback

// Per-worker batched counter.  The old single-global atomic increment made
// every Callback contend on one cache line (8 workers on one shared
// counter), so the sweep measured atomic-contention throughput rather than
// Ant Farm overhead.  Each worker accumulates locally and periodically
// flushes to the shared counter; the stop condition is still exact because
// consumers flush on idle and before unsubscribe.
private uint g_localCalls;
private enum uint LOCAL_FLUSH = 1024;

long benchCbLocal(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    if (++g_localCalls >= LOCAL_FLUSH)
    {
        atomicFetchAdd(g_calls, cast(long) g_localCalls);
        g_localCalls = 0;
    }
    return 1;
}

long benchCbGlobal(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    atomicFetchAdd(g_calls, 1L);
    return 1;
}

void flushLocalCalls() nothrow @nogc @system
{
    if (g_localCalls != 0)
    {
        atomicFetchAdd(g_calls, cast(long) g_localCalls);
        g_localCalls = 0;
    }
}

struct Cfg
{
    ulong ln = 1UL << 21;
    uint k = 8;
    uint nc = 4;
    uint nb = 1;
    uint ns = 0;
    uint body = 16;
    uint batch = 80;
    ulong n = 16_000_000;
    ulong qb = 0;      // 0 → segCap
    ulong qs = 4096;
    uint avgCost = 1;  // chunk hint: MAX_CHUNK >> avgCost
    uint small = 64;   // small-table threshold; 0 = auto clamp(sq*chunk,16,256)
    uint repeats = 3;
    bool huge;   // --huge: MADV_HUGEPAGE on the magic-buffer mapping
    bool nSet;
}

struct Trial
{
    Cfg cfg;
    double secs = 0;
    double mpps = 0;     // million payloads / s
    double mibs = 0;     // MiB/s of payload bodies
    ulong wt;
    uint stalls;
    bool ok;
    const(char)[] err;
}

struct ProdCtx
{
    AntFarm* f;
    PayloadEntry* pool;
    size_t poolN;
    size_t count;          // payloads this producer must commit
    Tier tier;
    uint batch;
    uint avgCost;          // chunk hint for write()
    shared uint* stalls;
    shared int* timedOut;
    MonoTime deadline;
}

struct ConsCtx
{
    AntFarm* f;
    long expected;
    MonoTime deadline;
    shared int* timedOut;
}

final class ProdJob
{
    ProdCtx* c;
    this(ProdCtx* c) { this.c = c; }
    void run() { producerMain(c); }
}

final class ConsJob
{
    ConsCtx* c;
    this(ConsCtx* c) { this.c = c; }
    void run() { consumerMain(c); }
}

void producerMain(ProdCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid)
    {
        fprintf(stderr, "producer overregistration\n");
        abort();
    }
    ulong done;
    uint localStalls;
    while (done < c.count && atomicLoad(g_stop) == 0)
    {
        if (MonoTime.currTime > c.deadline)
        {
            atomicStore(*c.timedOut, 1);
            break;
        }
        immutable remain = c.count - done;
        immutable take = remain < c.batch ? remain : c.batch;
        immutable span = take < c.poolN ? take : c.poolN;
        immutable wrote = c.f.write(c.pool[0 .. span], tok, c.avgCost);
        done += wrote;
        if (wrote == 0)
        {
            ++localStalls;
            Thread.yield();
        }
    }
    atomicOp!"+="(*c.stalls, localStalls);
    c.f.unregisterProducer(tok);
}

void consumerMain(ConsCtx* c)
{
    ConsumerView v;
    if (v.subscribe(c.f) < 0)
    {
        fprintf(stderr, "subscribe failed\n");
        abort();
    }
    for (;;)
    {
        if (!v.consumeNext())
        {
            flushLocalCalls();
            if (atomicLoad!(MemoryOrder.acq)(g_calls) >= c.expected)
                break;
            if (MonoTime.currTime > c.deadline)
            {
                atomicStore(*c.timedOut, 1);
                break;
            }
            Thread.yield();
        }
    }
    flushLocalCalls();
    v.unsubscribe();
}

/// Singleton table size for one ST payload; used to skip unsizable configs.
ulong singletonSize(uint body, uint sq)
{
    // THEAD 8 + 1 index + 7 pad + 1 progress + 7 pad + 8*sq Tcount + payload + 7 end
    return 8 + 1 + 7 + 1 + 7 + 8UL * sq + 17 + body + 7;
}

bool cfgFits(ref const Cfg c)
{
    if (c.nb + c.ns == 0) return false;
    if (c.nc == 0 || c.nc > MAX_CONSUMERS_LIMIT) return false;
    immutable segCap = c.ln / c.k;
    immutable qb = c.qb == 0 ? segCap : c.qb;
    immutable exmax = cast(ulong) c.nb * qb + cast(ulong) c.ns * c.qs;
    if (exmax > (c.k - 1) * segCap) return false;
    immutable sq = sqcsOf(c.nc);
    immutable one = singletonSize(c.body, sq);
    if (c.nb && one > qb) return false;
    if (c.ns && one > c.qs) return false;
    return true;
}

Trial runOnce(Cfg c)
{
    Trial t;
    t.cfg = c;
    if (!cfgFits(c))
    {
        t.err = "skip (quota/Exmax)";
        return t;
    }

    immutable segCap = c.ln / c.k;
    // create() defaults quotaBulk==0 to segCap only when the bulk tier is
    // in use (nb > 0), so an unused bulk tier cannot inject a segCap quota
    // into the Exmax check; pass the configured value straight through.
    immutable qb = c.qb;
    auto f = AntFarm.create(c.ln, c.k, c.nc, c.nb, qb, c.ns, c.qs, c.small, c.huge);

    immutable poolN = c.batch < 256 ? 256 : c.batch;
    auto headers = cast(PayloadHeader*) malloc(PayloadHeader.sizeof);
    auto body = cast(ulong*) malloc(c.body * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(poolN * PayloadEntry.sizeof);
    if (headers is null || body is null || entries is null)
    {
        t.err = "alloc";
        if (headers) free(headers);
        if (body) free(body);
        if (entries) free(entries);
        f.destroy();
        return t;
    }
    *headers = PayloadHeader.init;
    headers.maxCs = 1;
    headers.done = 1;
    headers.call = g_globalCount ? &benchCbGlobal : &benchCbLocal;
    foreach (i; 0 .. c.body)
        body[i] = i;
    foreach (i; 0 .. poolN)
    {
        entries[i].header = headers;
        entries[i].body = body[0 .. c.body];
    }

    immutable np = c.nb + c.ns;
    auto pctx = cast(ProdCtx*) malloc(np * ProdCtx.sizeof);
    auto cctx = cast(ConsCtx*) malloc(c.nc * ConsCtx.sizeof);
    Thread[] producers = new Thread[np];
    Thread[] consumers = new Thread[c.nc];

    shared uint stalls;
    shared int timedOut;
    atomicStore(g_calls, 0L);
    atomicStore(g_stop, 0);
    atomicStore(stalls, 0);
    atomicStore(timedOut, 0);

    immutable expected = cast(long) c.n;
    auto deadline = MonoTime.currTime + 90.seconds;

    // Split the payload count across producers (bulk first, then small).
    // write() copies into the farm, so every producer reuses the same pool.
    size_t cursor;
    foreach (i; 0 .. np)
    {
        immutable remain = cast(size_t) c.n - cursor;
        immutable share = remain / (np - i);
        pctx[i] = ProdCtx(f, entries, poolN, share,
                          i < c.nb ? Tier.bulk : Tier.small,
                          c.batch, c.avgCost, &stalls, &timedOut, deadline);
        cursor += share;
    }
    foreach (i; 0 .. c.nc)
        cctx[i] = ConsCtx(f, expected, deadline, &timedOut);

    GC.collect();
    GC.minimize();
    GC.disable();
    immutable t0 = MonoTime.currTime;
    foreach (i; 0 .. c.nc)
    {
        auto job = new ConsJob(&cctx[i]);
        consumers[i] = new Thread(&job.run);
        consumers[i].start();
    }
    foreach (i; 0 .. np)
    {
        auto job = new ProdJob(&pctx[i]);
        producers[i] = new Thread(&job.run);
        producers[i].start();
    }
    foreach (p; producers)
        p.join();
    foreach (cons; consumers)
        cons.join();
    immutable t1 = MonoTime.currTime;
    GC.enable();

    immutable secs = (t1 - t0).total!"nsecs" / 1e9;
    immutable calls = atomicLoad(g_calls);
    t.secs = secs;
    t.wt = atomicLoad(f.Wt);
    t.stalls = atomicLoad(stalls);

    free(pctx);
    free(cctx);
    free(headers);
    free(body);
    free(entries);
    f.destroy();

    if (atomicLoad(timedOut) || calls != expected)
    {
        t.err = atomicLoad(timedOut) ? "TIMEOUT" : "lost work";
        return t;
    }
    t.ok = true;
    t.mpps = (c.n / secs) / 1e6;
    t.mibs = (cast(double) c.n * c.body * 8.0 / secs) / (1024.0 * 1024.0);
    return t;
}

Trial runMedian(Cfg c)
{
    Trial[8] rs;
    uint n;
    foreach (r; 0 .. c.repeats)
    {
        if (n >= rs.length) break;
        rs[n++] = runOnce(c);
        if (!rs[n - 1].ok && rs[n - 1].err != "skip (quota/Exmax)")
            return rs[n - 1];
        if (!rs[n - 1].ok)
            return rs[n - 1];
    }
    // median by mpps
    foreach (i; 0 .. n)
        foreach (j; i + 1 .. n)
            if (rs[j].mpps < rs[i].mpps)
            {
                auto tmp = rs[i];
                rs[i] = rs[j];
                rs[j] = tmp;
            }
    return rs[n / 2];
}

void printHeader()
{
    printf("%-3s %3s %3s %3s %5s %5s %10s  %7s %8s %8s %8s  %s\n",
           "K".ptr, "nc".ptr, "nb".ptr, "ns".ptr, "body".ptr, "bat".ptr,
           "N".ptr, "sec".ptr, "Mpps".ptr, "MiB/s".ptr, "stalls".ptr, "note".ptr);
}

void printRow(ref const Trial t)
{
    auto c = t.cfg;
    if (!t.ok)
    {
        printf(" %-2u %3u %3u %3u %5u %5u %10llu  %7s %8s %8s %8s  %.*s\n",
               c.k, c.nc, c.nb, c.ns, c.body, c.batch, cast(ulong) c.n,
               "-".ptr, "-".ptr, "-".ptr, "-".ptr,
               cast(int) t.err.length, t.err.ptr);
        return;
    }
    printf(" %-2u %3u %3u %3u %5u %5u %10llu  %7.3f %8.3f %8.1f %8u\n",
           c.k, c.nc, c.nb, c.ns, c.body, c.batch, cast(ulong) c.n,
           t.secs, t.mpps, t.mibs, t.stalls);
}

Cfg parseArgs(string[] args, Cfg base)
{
    Cfg c = base;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (a == "--once" || a == "--sweep")
            continue;
        if (a == "--global-count")
        {
            g_globalCount = true;
            continue;
        }
        if (a == "--huge")
        {
            c.huge = true;
            continue;
        }
        if (i + 1 >= args.length)
            break;
        auto v = args[++i];
        if (a == "--ln") c.ln = strtoull(v.ptr, null, 0);
        else if (a == "--k") c.k = cast(uint) atoi(v.ptr);
        else if (a == "--nc") c.nc = cast(uint) atoi(v.ptr);
        else if (a == "--nb") c.nb = cast(uint) atoi(v.ptr);
        else if (a == "--ns") c.ns = cast(uint) atoi(v.ptr);
        else if (a == "--body") c.body = cast(uint) atoi(v.ptr);
        else if (a == "--batch") c.batch = cast(uint) atoi(v.ptr);
        else if (a == "--n") { c.n = strtoull(v.ptr, null, 0); c.nSet = true; }
        else if (a == "--qb") c.qb = strtoull(v.ptr, null, 0);
        else if (a == "--qs") c.qs = strtoull(v.ptr, null, 0);
        else if (a == "--ac") c.avgCost = cast(uint) atoi(v.ptr);
        else if (a == "--small") c.small = cast(uint) atoi(v.ptr);
        else if (a == "--repeats") c.repeats = cast(uint) atoi(v.ptr);
        else
            --i;
    }
    return c;
}

void scaleN(ref Cfg c)
{
    if (c.nSet) return;
    // Keep large-body runs from becoming a memcpy bench that takes minutes,
    // and give tiny bodies enough iterations that startup is not the signal.
    if (c.body >= 1024) c.n = 800_000;
    else if (c.body >= 256) c.n = 2_000_000;
    else if (c.body >= 64) c.n = 6_000_000;
    else c.n = 16_000_000;
}

void banner(ref const Cfg c)
{
    immutable bytes = c.ln * 8.0 / (1024.0 * 1024.0);
    printf("Ant Farm throughput  Ln=%llu (%.1f MiB)  repeats=%u  callback=%s  huge=%s\n",
           cast(ulong) c.ln, bytes, c.repeats,
           g_globalCount ? "global-atomic".ptr : "per-worker-batched".ptr,
           c.huge ? "yes".ptr : "no".ptr);
    fflush(stdout);
}

int runSweep(Cfg base)
{
    banner(base);
    printf("\n== phase 1: topology (body=%u batch=%u) ==\n", base.body, base.batch);
    printHeader();

    enum uint[] ks = [4, 8];
    enum uint[] ncs = [1, 2, 4, 8];
    struct Mix { uint nb, ns; }
    Mix[4] mixes = [Mix(1, 0), Mix(2, 0), Mix(1, 4), Mix(0, 4)];

    Trial[] results;
    Trial best;
    best.mpps = -1;

    foreach (k; ks)
        foreach (nc; ncs)
            foreach (mix; mixes)
            {
                Cfg c = base;
                c.k = k;
                c.nc = nc;
                c.nb = mix.nb;
                c.ns = mix.ns;
                scaleN(c);
                auto t = runMedian(c);
                printRow(t);
                fflush(stdout);
                if (t.ok)
                {
                    results ~= t;
                    if (t.mpps > best.mpps)
                        best = t;
                }
            }

    if (best.mpps < 0)
    {
        printf("phase 1 produced no successful runs\n");
        return 1;
    }

    printf("\nphase 1 winner: K=%u nc=%u nb=%u ns=%u  %.3f Mpps  %.1f MiB/s\n",
           best.cfg.k, best.cfg.nc, best.cfg.nb, best.cfg.ns,
           best.mpps, best.mibs);

    printf("\n== phase 2: body x batch on the winning topology ==\n");
    printHeader();

    enum uint[] bodies = [2, 16, 64, 256, 1024];
    enum uint[] batches = [1, 8, 16, 32, 63, 64, 80, 128, 256];

    Trial best2 = best;
    Trial bestBytes = best;

    foreach (body; bodies)
        foreach (batch; batches)
        {
            Cfg c = best.cfg;
            c.body = body;
            c.batch = batch;
            c.n = base.n;
            c.repeats = base.repeats;
            scaleN(c);
            auto t = runMedian(c);
            printRow(t);
            fflush(stdout);
            if (t.ok)
            {
                results ~= t;
                if (t.mpps > best2.mpps)
                    best2 = t;
                if (t.mibs > bestBytes.mibs)
                    bestBytes = t;
            }
        }

    printf("\n== ranked (successful runs, by Mpps) ==\n");
    printHeader();
    foreach (i; 0 .. results.length)
        foreach (j; i + 1 .. results.length)
            if (results[j].mpps > results[i].mpps)
            {
                auto tmp = results[i];
                results[i] = results[j];
                results[j] = tmp;
            }
    immutable show = results.length < 15 ? results.length : 15;
    foreach (i; 0 .. show)
        printRow(results[i]);

    printf("\nbest payloads/s : K=%u nc=%u nb=%u ns=%u body=%u batch=%u  %.3f Mpps  %.1f MiB/s\n",
           best2.cfg.k, best2.cfg.nc, best2.cfg.nb, best2.cfg.ns,
           best2.cfg.body, best2.cfg.batch, best2.mpps, best2.mibs);
    printf("best payload MiB/s: K=%u nc=%u nb=%u ns=%u body=%u batch=%u  %.3f Mpps  %.1f MiB/s\n",
           bestBytes.cfg.k, bestBytes.cfg.nc, bestBytes.cfg.nb, bestBytes.cfg.ns,
           bestBytes.cfg.body, bestBytes.cfg.batch, bestBytes.mpps, bestBytes.mibs);
    printf("phase-1 topology  : K=%u nc=%u nb=%u ns=%u  (held fixed in phase 2)\n",
           best.cfg.k, best.cfg.nc, best.cfg.nb, best.cfg.ns);
    fflush(stdout);
    return 0;
}

void main(string[] args)
{
    bool once;
    foreach (a; args[1 .. $])
        if (a == "--once")
            once = true;

    Cfg base;
    base = parseArgs(args, base);
    scaleN(base);

    if (once)
    {
        banner(base);
        printHeader();
        auto t = runMedian(base);
        printRow(t);
        if (!t.ok)
        {
            fprintf(stderr, "run failed: %.*s\n",
                    cast(int) t.err.length, t.err.ptr);
            abort();
        }
        return;
    }
    auto rc = runSweep(base);
    if (rc != 0)
        abort();
}
