/++
 + Dual-registered producer/consumer throughput test for Ant Farm.
 +
 + Each thread registers a producer ticket and subscribes a ConsumerView,
 + then alternates between producing and consuming.  This exercises the
 + mixed-role topology the sweep cannot: small producers that are also
 + consumers, interleaving small writes with consumeNext calls.
 +
 +   make -C tools/perf dual
 +   ./tools/perf/dual
 +
 + Default: 4 dual threads, all small tier, batch=1 write followed by one
 + consumeNext call, 16-ulong bodies.
 + `--grid N` sweeps the dual-thread count from 1 through N while holding
 + the remaining configuration fixed.
 + `--constant-header` uses the common-callback write overload with the same
 + bodies, batch cadence, and callback.
 + `--constant-callandlength` additionally supplies the uniform body length,
 + selecting arithmetic sizing without inspecting body fronts.
 + `--full-header-size` sizes the quota-fit check with the pre-compact
 + 16-word Callback, for runs against runtimes before SPEC 2f.
 +/
module dual;

import antfarm;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, atoi, abort, strtoull;
import core.stdc.math : sqrt;

__gshared shared(long) g_calls;
__gshared shared(int) g_stop;
__gshared shared(int) g_ready;
__gshared bool g_fullHeaderSize; // --full-header-size: pre-compact singleton sizing

enum WriteMode : ubyte
{
    payloadEntry,
    constantHeader,
    constantCallAndLength,
}

// Per-worker batched counter, same idea as throughput.d: a single global
// atomic would measure callback contention instead of Ant Farm overhead.
// Function-local `static` is TLS in D; one helper so increment and flush
// share the same slot.
private enum uint LOCAL_FLUSH = 1024;

uint* workerCalls() nothrow @nogc @system
{
    static uint n;
    return &n;
}

extern(C) void benchCb(const(ulong)* bodyPtr, size_t bodyWords) nothrow @nogc @system
{
    auto b = bodyPtr[0 .. bodyWords];
    auto p = workerCalls();
    if (++(*p) >= LOCAL_FLUSH)
    {
        atomicFetchAdd(g_calls, cast(long) *p);
        *p = 0;
    }
    return;
}

void flushLocalCalls() nothrow @nogc @system
{
    auto p = workerCalls();
    if (*p != 0)
    {
        atomicFetchAdd(g_calls, cast(long) *p);
        *p = 0;
    }
}

ulong singletonSize(uint body, uint sq)
{
    // THEAD + one index + pads/progress + Tcount + header/body + end pad.
    // The bench's payloads are all ST, so the table is compact (SPEC 2f):
    // one Call word per record. --full-header-size restores the 16-word
    // legacy payload header for runs against pre-compact runtimes.
    return (g_fullHeaderSize ? 8 : 5) + 1 + 7 + 1 + 7 + 8UL * sq + (g_fullHeaderSize ? 16 : 1) + body + 7;
}

struct Cfg
{
    ulong ln = 1UL << 21;
    uint k = 8;
    uint nd = 4;            // dual-registered threads
    uint body = 16;
    uint batch = 1;         // payloads per produce phase (small write size)
    uint consume = 1;       // consumeNext calls per produce phase; 0 = drain until empty
    uint avgCost = DEFAULT_AVG_COST;
    uint small = 64;
    ulong n = 4_000_000;
    uint repeats = 3;
    uint grid;              // --grid N: sweep nd from 1 through N
    bool nSet;
    bool bulk;              // use bulk tier instead of small tier
    bool huge;              // --huge: MADV_HUGEPAGE on the magic-buffer mapping
    WriteMode writeMode;
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
    Callback* commonCallback;
    PayloadBody* bodies;
    size_t poolN;
    size_t produce;         // this thread's production share
    uint batch;
    uint consume;
    uint avgCost;
    uint bodyWords;
    WriteMode writeMode;
    Tier tier;
    long expected;
    MonoTime deadline;
    shared int* timedOut;
    uint peers;             // dual-thread count; subscribe barrier
    size_t produced;
    uint stalls;
}

// Module-level (not function-nested) so `_outer` cannot dangle. Threads
// take `&jobs[i].run` from a stable array — a loop-local delegate would
// alias every worker to the last DualCtx (see tests/farm/torture_tests.d).
final class DualJob
{
    DualCtx* c;
    this(DualCtx* c) { this.c = c; }
    void run() { dualMain(c); }
}

void dualMain(DualCtx* c)
{
    final switch (c.writeMode)
    {
    case WriteMode.payloadEntry:
        return dualMainImpl!(WriteMode.payloadEntry)(c);
    case WriteMode.constantHeader:
        return dualMainImpl!(WriteMode.constantHeader)(c);
    case WriteMode.constantCallAndLength:
        return dualMainImpl!(WriteMode.constantCallAndLength)(c);
    }
}

private void dualMainImpl(WriteMode writeMode)(DualCtx* c)
{
    *workerCalls() = 0;
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

    atomicFetchAdd(g_ready, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_ready) < cast(int) c.peers
           && atomicLoad!(MemoryOrder.acq)(g_stop) == 0)
    {
        if (MonoTime.currTime > c.deadline)
        {
            atomicStore(g_stop, 1);
            atomicStore(*c.timedOut, 1);
            break;
        }
        Thread.yield();
    }

    ulong produced;
    uint localStalls;

    while (atomicLoad!(MemoryOrder.acq)(g_stop) == 0 && MonoTime.currTime <= c.deadline)
    {
        // Produce phase: one small (or bulk) write.
        if (produced < c.produce)
        {
            immutable remain = c.produce - produced;
            immutable take = remain < c.batch ? remain : c.batch;
            immutable span = take < c.poolN ? take : c.poolN;
            static if (writeMode == WriteMode.constantHeader)
                immutable wrote = c.f.write(*c.commonCallback,
                    c.bodies[0 .. span], tok, c.avgCost);
            else static if (writeMode == WriteMode.constantCallAndLength)
                immutable wrote = c.f.write(*c.commonCallback,
                    c.bodies[0 .. span], c.bodyWords, tok, c.avgCost);
            else
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
    immutable quota = c.bulk ? qb : qs;
    if (singletonSize(c.body, sqcsOf(c.nd)) > quota)
    {
        t.err = "payload exceeds tier quota";
        return t;
    }

    auto f = AntFarm.create(ringMiBFromUlongs(c.ln), c.k, c.nd, maxBulk, qb, maxSmall, qs, c.small, c.huge);

    immutable poolN = c.batch < 256 ? 256 : c.batch;
    auto callbacks = cast(Callback*) malloc(Callback.sizeof);
    auto body = cast(ulong*) malloc(c.body * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(poolN * PayloadEntry.sizeof);
    auto bodies = cast(PayloadBody*) malloc(poolN * PayloadBody.sizeof);
    if (callbacks is null || body is null || entries is null || bodies is null)
    {
        t.err = "alloc";
        if (callbacks) free(callbacks);
        if (body) free(body);
        if (entries) free(entries);
        if (bodies) free(bodies);
        f.destroy();
        return t;
    }

    *callbacks = Callback.init;

    callbacks[0] = &benchCb;
    foreach (i; 0 .. c.body)
        body[i] = i;
    foreach (i; 0 .. poolN)
    {
        entries[i].call = callbacks[0];
        entries[i].body = body[0 .. c.body];
        bodies[i] = body[0 .. c.body];
    }

    auto ctxs = cast(DualCtx*) malloc(c.nd * DualCtx.sizeof);
    if (ctxs is null)
    {
        t.err = "ctx alloc";
        free(callbacks);
        free(body);
        free(entries);
        free(bodies);
        f.destroy();
        return t;
    }

    Thread[] threads = new Thread[c.nd];
    auto jobs = new DualJob[](c.nd);
    shared int timedOut;
    atomicStore(g_calls, 0L);
    atomicStore(g_stop, 0);
    atomicStore(g_ready, 0);
    atomicStore(timedOut, 0);

    immutable expected = cast(long) c.n;
    auto deadline = MonoTime.currTime + 90.seconds;

    size_t cursor;
    foreach (i; 0 .. c.nd)
    {
        immutable remain = cast(size_t) c.n - cursor;
        immutable share = remain / (c.nd - i);
        ctxs[i] = DualCtx(f, entries, callbacks, bodies, poolN, share,
                          c.batch, c.consume, c.avgCost, c.body, c.writeMode,
                          c.bulk ? Tier.bulk : Tier.small,
                          expected, deadline, &timedOut, c.nd, 0, 0);
        cursor += share;
    }

    GC.collect();
    GC.minimize();
    GC.disable();
    immutable t0 = MonoTime.currTime;
    foreach (i; 0 .. c.nd)
    {
        jobs[i] = new DualJob(&ctxs[i]);
        threads[i] = new Thread(&jobs[i].run);
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
    free(callbacks);
    free(body);
    free(entries);
    free(bodies);
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
    if (c.repeats == 0 || c.repeats > rs.length)
    {
        rs[0].cfg = c;
        rs[0].err = "repeats must be 1..8";
        return rs[0];
    }
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
    import std.string : toStringz;

    Cfg c = base;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (a == "--huge")
        {
            c.huge = true;
            continue;
        }
        if (a == "--constant-header")
        {
            c.writeMode = WriteMode.constantHeader;
            continue;
        }
        if (a == "--constant-callandlength" ||
            a == "--constant-call-and-length")
        {
            c.writeMode = WriteMode.constantCallAndLength;
            continue;
        }
        if (a == "--full-header-size")
        {
            g_fullHeaderSize = true;
            continue;
        }
        if (i + 1 >= args.length)
            break;
        auto v = args[++i];
        auto numeric = toStringz(v);
        if (a == "--ln") c.ln = strtoull(numeric, null, 0);
        else if (a == "--k") c.k = cast(uint) atoi(numeric);
        else if (a == "--nd") c.nd = cast(uint) atoi(numeric);
        else if (a == "--body") c.body = cast(uint) atoi(numeric);
        else if (a == "--batch") c.batch = cast(uint) atoi(numeric);
        else if (a == "--consume") c.consume = cast(uint) atoi(numeric);
        else if (a == "--n") { c.n = strtoull(numeric, null, 0); c.nSet = true; }
        else if (a == "--ac") c.avgCost = cast(uint) atoi(numeric);
        else if (a == "--small") c.small = cast(uint) atoi(numeric);
        else if (a == "--tier") c.bulk = (v == "bulk");
        else if (a == "--repeats") c.repeats = cast(uint) atoi(numeric);
        else if (a == "--grid") c.grid = cast(uint) atoi(numeric);
        else
            --i;
    }
    return c;
}

unittest
{
    // Non-NUL-terminated slices must not consume the following digit.
    auto raw = "67 35 17 100001 0x10f".idup;
    auto c = parseArgs(["dual", "--nd", raw[0 .. 1],
        "--ac", raw[3 .. 4], "--repeats", raw[6 .. 7],
        "--n", raw[9 .. 14], "--ln", raw[16 .. 20]], Cfg.init);
    assert(c.nd == 6 && c.avgCost == 3 && c.repeats == 1);
    assert(c.n == 10_000 && c.nSet && c.ln == 16);
    auto bulk = "bulk-suffix".idup;
    c = parseArgs(["dual", "--tier", bulk[0 .. 4]], c);
    assert(c.bulk);
    auto small = "small-suffix".idup;
    c = parseArgs(["dual", "--tier", small[0 .. 5]], c);
    assert(!c.bulk);
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
    printf("Ant Farm dual-role throughput  Ln=%llu (%.1f MiB)  repeats=%u  tier=%s  write=%s  huge=%s\n",
           cast(ulong) c.ln, bytes, c.repeats, c.bulk ? "bulk".ptr : "small".ptr,
           c.writeMode == WriteMode.payloadEntry ? "payload-entry".ptr :
               (c.writeMode == WriteMode.constantHeader ?
                    "constant-header".ptr : "constant-callandlength".ptr),
           c.huge ? "yes".ptr : "no".ptr);
    fflush(stdout);
}

int runGrid(Cfg base, uint gridN)
{
    if (gridN == 0 || gridN > MAX_CONSUMERS_LIMIT)
    {
        fprintf(stderr, "grid must be in 1..%u\n", MAX_CONSUMERS_LIMIT);
        return 1;
    }

    banner(base);
    printf("\n== dual-thread grid nd 1..%u (body=%u batch=%u consume=%u tier=%s) ==\n",
           gridN, base.body, base.batch, base.consume,
           base.bulk ? "bulk".ptr : "small".ptr);
    printHeader();

    Trial[] results;
    Trial best;
    best.mpps = -1;

    foreach (nd; 1 .. gridN + 1)
    {
        Cfg c = base;
        c.nd = nd;
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

    if (results.length == 0)
    {
        printf("grid produced no successful runs\n");
        return 1;
    }

    double sum = 0;
    double sumSq = 0;
    foreach (r; results)
    {
        sum += r.mpps;
        sumSq += r.mpps * r.mpps;
    }
    immutable n = results.length;
    immutable aggMean = sum / n;
    double aggStddev = 0;
    if (n > 1)
    {
        immutable variance = (sumSq - sum * sum / n) / (n - 1);
        aggStddev = variance > 0 ? sqrt(variance) : 0;
    }

    double[] vals;
    foreach (r; results)
        vals ~= r.mpps;
    foreach (i; 0 .. n)
        foreach (j; i + 1 .. n)
            if (vals[j] < vals[i])
            {
                auto tmp = vals[i];
                vals[i] = vals[j];
                vals[j] = tmp;
            }
    immutable aggMedian = vals[n / 2];

    printf("\nbest dual-thread grid: nd=%u  %.3f Mpps  %.1f MiB/s\n",
           best.cfg.nd, best.mpps, best.mibs);
    printf("grid aggregate: %llu topologies  mean=%.3f Mpps  stddev=%.3f Mpps  median=%.3f Mpps\n",
           cast(ulong) n, aggMean, aggStddev, aggMedian);
    fflush(stdout);
    return 0;
}

void main(string[] args)
{
    Cfg base;
    base = parseArgs(args, base);
    scaleN(base);

    if (base.grid > 0)
    {
        auto rc = runGrid(base, base.grid);
        if (rc != 0)
            abort();
        return;
    }

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
