/++
 + Dual-registered producer/consumer throughput test for Ant Farm.
 +
 + Each thread registers a producer ticket and subscribes a ConsumerView,
 + then alternates between producing and consuming.  This exercises the
 + mixed-role topology the sweep cannot: small producers that are also
 + consumers, interleaving small writes with consumeNext calls.
 +
 +   make -C perftest dual
 +   ./perftest/dual
 +
 + Default: 4 dual threads, all small tier, batch=1 write followed by one
 + consumeNext call, 16-ulong bodies.
 +/
module dual;

import antfarm;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, atoi, abort, strtoull;

__gshared shared(long) g_calls;
__gshared shared(int) g_stop;

// Per-worker batched counter, same idea as throughput.d: a single global
// atomic would measure callback contention instead of Ant Farm overhead.
private uint g_localCalls;
private enum uint LOCAL_FLUSH = 1024;

long benchCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    if (++g_localCalls >= LOCAL_FLUSH)
    {
        atomicFetchAdd(g_calls, cast(long) g_localCalls);
        g_localCalls = 0;
    }
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
    uint k = 4;
    uint nd = 4;            // dual-registered threads
    uint body = 16;
    uint batch = 1;         // payloads per produce phase (small write size)
    uint consume = 1;       // consumeNext calls per produce phase; 0 = drain until empty
    uint avgCost = 1;
    uint small = 64;
    ulong n = 4_000_000;
    uint repeats = 3;
    bool nSet;
    bool bulk;              // use bulk tier instead of small tier
}

struct Trial
{
    Cfg cfg;
    double secs;
    double mpps;
    double mibs;
    ulong produced;
    ulong consumed;
    uint stalls;
    bool ok;
    const(char)[] err;
}

struct DualCtx
{
    AntFarm* f;
    PayloadEntry* pool;
    size_t poolN;
    size_t produce;         // this thread's production share
    uint batch;
    uint consume;
    uint avgCost;
    Tier tier;
    long expected;
    MonoTime deadline;
    shared int* timedOut;
    size_t produced;
    uint stalls;
}

final class DualJob
{
    DualCtx* c;
    this(DualCtx* c) { this.c = c; }
    void run() { dualMain(c); }
}

void dualMain(DualCtx* c)
{
    auto tok = c.f.registerProducer(c.tier);
    if (!tok.valid)
    {
        fprintf(stderr, "dual producer overregistration\n");
        abort();
    }

    ConsumerView v;
    if (v.subscribe(c.f) < 0)
    {
        fprintf(stderr, "dual subscribe failed\n");
        abort();
    }

    ulong produced;
    uint localStalls;

    while (atomicLoad(g_stop) == 0 && MonoTime.currTime <= c.deadline)
    {
        // Produce phase: one small (or bulk) write.
        if (produced < c.produce)
        {
            immutable remain = c.produce - produced;
            immutable take = remain < c.batch ? remain : c.batch;
            immutable span = take < c.poolN ? take : c.poolN;
            immutable wrote = c.f.write(c.pool[0 .. span], tok, c.avgCost);
            produced += wrote;
            if (wrote == 0)
                ++localStalls;
        }

        // Consume phase: interleave consumeNext calls in the same thread.
        if (c.consume == 0)
        {
            while (v.consumeNext()) {}
        }
        else
        {
            foreach (i; 0 .. c.consume)
                if (!v.consumeNext())
                    break;
        }

        flushLocalCalls();
        if (atomicLoad!(MemoryOrder.acq)(g_calls) >= c.expected)
            break;
    }

    // Final drain: finish any work produced but not yet consumed.
    while (atomicLoad(g_stop) == 0 && MonoTime.currTime <= c.deadline)
    {
        if (!v.consumeNext())
        {
            flushLocalCalls();
            if (atomicLoad!(MemoryOrder.acq)(g_calls) >= c.expected)
                break;
            Thread.yield();
        }
        else
        {
            flushLocalCalls();
            if (atomicLoad!(MemoryOrder.acq)(g_calls) >= c.expected)
                break;
        }
    }

    flushLocalCalls();
    if (MonoTime.currTime > c.deadline)
    {
        atomicStore(g_stop, 1);
        atomicStore(*c.timedOut, 1);
    }

    c.produced = produced;
    c.stalls = localStalls;

    v.unsubscribe();
    c.f.unregisterProducer(tok);
}

Trial runDual(Cfg c)
{
    Trial t;
    t.cfg = c;

    if (c.nd == 0 || c.nd > MAX_CONSUMERS_LIMIT)
    {
        t.err = "bad nd";
        return t;
    }
    if (c.batch == 0)
    {
        t.err = "bad batch";
        return t;
    }
    if (c.body == 0)
    {
        t.err = "bad body";
        return t;
    }

    immutable segCap = c.ln / c.k;
    immutable maxBulk = c.bulk ? c.nd : 0;
    immutable maxSmall = c.bulk ? 0 : c.nd;
    // Bulk dual threads share the K-1 segment budget; small dual threads use
    // the normal small quota.  create() itself fatals on invalid Exmax, so
    // mirror the same arithmetic here to fail with a row instead of aborting.
    immutable qb = c.bulk ? ((c.k - 1) * segCap) / c.nd : 0;
    immutable qs = 4096;
    immutable exmax = cast(ulong) maxBulk * qb + cast(ulong) maxSmall * qs;
    if (exmax > (c.k - 1) * segCap)
    {
        t.err = "Exmax exceeds K-1 segments";
        return t;
    }

    auto f = AntFarm.create(c.ln, c.k, c.nd, maxBulk, qb, maxSmall, qs, c.small);

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
    headers.call = &benchCb;
    foreach (i; 0 .. c.body)
        body[i] = i;
    foreach (i; 0 .. poolN)
    {
        entries[i].header = headers;
        entries[i].body = body[0 .. c.body];
    }

    auto ctxs = cast(DualCtx*) malloc(c.nd * DualCtx.sizeof);
    if (ctxs is null)
    {
        t.err = "ctx alloc";
        free(headers);
        free(body);
        free(entries);
        f.destroy();
        return t;
    }

    Thread[] threads = new Thread[c.nd];
    shared int timedOut;
    atomicStore(g_calls, 0L);
    atomicStore(g_stop, 0);
    atomicStore(timedOut, 0);

    immutable expected = cast(long) c.n;
    auto deadline = MonoTime.currTime + 90.seconds;

    size_t cursor;
    foreach (i; 0 .. c.nd)
    {
        immutable remain = cast(size_t) c.n - cursor;
        immutable share = remain / (c.nd - i);
        ctxs[i] = DualCtx(f, entries, poolN, share, c.batch, c.consume,
                          c.avgCost, c.bulk ? Tier.bulk : Tier.small,
                          expected, deadline, &timedOut, 0, 0);
        cursor += share;
    }

    GC.collect();
    GC.minimize();
    GC.disable();
    immutable t0 = MonoTime.currTime;
    foreach (i; 0 .. c.nd)
    {
        auto job = new DualJob(&ctxs[i]);
        threads[i] = new Thread(&job.run);
        threads[i].start();
    }
    foreach (th; threads)
        th.join();
    immutable t1 = MonoTime.currTime;
    GC.enable();

    immutable secs = (t1 - t0).total!"nsecs" / 1e9;
    t.secs = secs;
    t.consumed = atomicLoad(g_calls);

    ulong produced;
    uint stalls;
    foreach (i; 0 .. c.nd)
    {
        produced += ctxs[i].produced;
        stalls += ctxs[i].stalls;
    }
    t.produced = produced;
    t.stalls = stalls;

    free(ctxs);
    free(headers);
    free(body);
    free(entries);
    f.destroy();

    if (atomicLoad(timedOut))
    {
        t.err = "TIMEOUT";
        return t;
    }
    if (produced != c.n || t.consumed != c.n)
    {
        t.err = "lost work";
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
        if (n >= rs.length)
            break;
        rs[n++] = runDual(c);
        if (!rs[n - 1].ok)
            return rs[n - 1];
    }
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
    printf("%-3s %3s %3s %5s %5s %5s %10s  %7s %8s %8s %8s  %s\n",
           "K".ptr, "nd".ptr, "tier".ptr, "body".ptr, "bat".ptr, "cons".ptr,
           "N".ptr, "sec".ptr, "Mpps".ptr, "MiB/s".ptr, "stalls".ptr, "note".ptr);
}

void printRow(ref const Trial t)
{
    auto c = t.cfg;
    if (!t.ok)
    {
        printf(" %-2u %3u %3s %5u %5u %5u %10llu  %7s %8s %8s %8s  %.*s\n",
               c.k, c.nd, c.bulk ? "bulk".ptr : "small".ptr, c.body, c.batch, c.consume,
               cast(ulong) c.n, "-".ptr, "-".ptr, "-".ptr, "-".ptr,
               cast(int) t.err.length, t.err.ptr);
        return;
    }
    printf(" %-2u %3u %3s %5u %5u %5u %10llu  %7.3f %8.3f %8.1f %8u\n",
           c.k, c.nd, c.bulk ? "bulk".ptr : "small".ptr, c.body, c.batch, c.consume,
           cast(ulong) c.n, t.secs, t.mpps, t.mibs, t.stalls);
}

Cfg parseArgs(string[] args, Cfg base)
{
    Cfg c = base;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (i + 1 >= args.length)
            break;
        auto v = args[++i];
        if (a == "--ln") c.ln = strtoull(v.ptr, null, 0);
        else if (a == "--k") c.k = cast(uint) atoi(v.ptr);
        else if (a == "--nd") c.nd = cast(uint) atoi(v.ptr);
        else if (a == "--body") c.body = cast(uint) atoi(v.ptr);
        else if (a == "--batch") c.batch = cast(uint) atoi(v.ptr);
        else if (a == "--consume") c.consume = cast(uint) atoi(v.ptr);
        else if (a == "--n") { c.n = strtoull(v.ptr, null, 0); c.nSet = true; }
        else if (a == "--ac") c.avgCost = cast(uint) atoi(v.ptr);
        else if (a == "--small") c.small = cast(uint) atoi(v.ptr);
        else if (a == "--tier") c.bulk = (v == "bulk");
        else if (a == "--repeats") c.repeats = cast(uint) atoi(v.ptr);
        else
            --i;
    }
    return c;
}

void scaleN(ref Cfg c)
{
    if (c.nSet)
        return;
    // Small alternating writes are the expensive case; keep runtime bounded.
    if (c.body >= 1024) c.n = 400_000;
    else if (c.body >= 256) c.n = 1_000_000;
    else if (c.body >= 64) c.n = 2_000_000;
    else c.n = 4_000_000;
}

void banner(ref const Cfg c)
{
    immutable bytes = c.ln * 8.0 / (1024.0 * 1024.0);
    printf("Ant Farm dual-role throughput  Ln=%llu (%.1f MiB)  repeats=%u  tier=%s\n",
           cast(ulong) c.ln, bytes, c.repeats, c.bulk ? "bulk".ptr : "small".ptr);
    fflush(stdout);
}

void main(string[] args)
{
    Cfg base;
    base = parseArgs(args, base);
    scaleN(base);
    banner(base);
    printHeader();
    auto t = runMedian(base);
    printRow(t);
    if (!t.ok)
    {
        fprintf(stderr, "dual run failed: %.*s (produced=%llu consumed=%llu)\n",
                cast(int) t.err.length, t.err.ptr,
                cast(ulong) t.produced, cast(ulong) t.consumed);
        abort();
    }
}
