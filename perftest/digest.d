/++
 + Consume-side digest bench: does a linear table chunk beat a scattered walk?
 +
 +   make -C perftest digest && ./perftest/digest
 +
 + Phase 0/1 of the linear-chunk plan. Publish is untimed. Arms differ only
 + in how workers walk an already-published table.
 +/
module digest;

import antfarm;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, abort, atoi, strtoull;
import core.stdc.string : memcpy;

private enum ulong THEAD = 8;
private enum ulong PHLEN = PayloadHeader.sizeof / 8;

enum Arm : int { linear16, linear1, shuffled16, copyout16 }
enum Mix : int { touch, chase, alt }

immutable string[4] armName = ["linear16", "linear1", "shuffled16", "copyout16"];
immutable string[3] mixName = ["touch", "chase", "alt"];

struct Cfg
{
    ulong ln = 1UL << 21;
    uint k = 4;
    uint nc = 8;
    uint nb = 1; // untimed publish; 1×segCap Exmax still refreshes with the dummy pin
    uint body = 16;
    uint tlen = 256;     // payloads per table; >= 64 so shards are live
    ulong jobs = 40_000; // publish-then-drain must fit in one lap (no consumer progress)
    uint repeats = 3;
    Mix mix = Mix.touch;
    bool allArms = true;
    Arm oneArm = Arm.linear16;
}

__gshared shared(long) g_calls;
__gshared shared(ulong) g_sink;
__gshared ulong* g_world;
__gshared size_t g_worldN;
__gshared shared(int) g_go; // 0 = wait, 1 = consume, 2 = abort

long touchCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    ulong s = iter;
    foreach (w; b)
        s ^= w;
    atomicOp!"+="(g_sink, s);
    atomicFetchAdd(g_calls, 1L);
    return 1;
}

long touchCbB(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    ulong s = iter + 1;
    foreach (w; b)
        s += w;
    atomicOp!"+="(g_sink, s);
    atomicFetchAdd(g_calls, 1L);
    return 1;
}

long chaseCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    ulong s = iter;
    foreach (w; b)
        s ^= w;
    if (g_worldN)
    {
        immutable idx = (b.length ? b[0] : 0) % g_worldN;
        s += g_world[idx];
    }
    atomicOp!"+="(g_sink, s);
    atomicFetchAdd(g_calls, 1L);
    return 1;
}

Callback mixCall(Mix m, size_t i)
{
    if (m == Mix.chase) return &chaseCb;
    if (m == Mix.alt) return (i & 1) ? &touchCbB : &touchCb;
    return &touchCb;
}

void shuffleTindex(ulong* w, uint tlen, uint seed)
{
    uint x = seed ? seed : 1;
    foreach_reverse (i; 1 .. tlen)
    {
        x = x * 1664525u + 1013904223u;
        immutable j = x % (i + 1);
        immutable tmp = w[THEAD + i];
        w[THEAD + i] = w[THEAD + j];
        w[THEAD + j] = tmp;
    }
}

void enterInPlace(shared(ulong)* bp, ulong absIdx) nothrow @nogc @system
{
    auto head = cast(PayloadHeader*)(bp + absIdx);
    immutable c = atomicFetchAdd(head.pcount, 1UL << 32);
    if ((c >> 32) >= head.maxCs)
        return;
    immutable d = atomicFetchAdd(head.pcount, 1UL << 16);
    immutable called = (d >> 16) & 0xFFFF;
    if (called >= head.done)
        return;
    auto body_ = (cast(const(ulong)*)(cast(ulong*) bp + absIdx + PHLEN))[0 .. head.plen];
    head.call(head, body_, called);
    atomicFetchAdd(head.pcount, 1UL);
}

void enterCopyOut(shared(ulong)* bp, ulong absIdx, ulong* tmp, size_t tmpWords)
    nothrow @nogc @system
{
    auto head = cast(PayloadHeader*)(bp + absIdx);
    immutable c = atomicFetchAdd(head.pcount, 1UL << 32);
    if ((c >> 32) >= head.maxCs)
        return;
    immutable d = atomicFetchAdd(head.pcount, 1UL << 16);
    immutable called = (d >> 16) & 0xFFFF;
    if (called >= head.done)
        return;
    immutable need = PHLEN + head.plen;
    if (need > tmpWords)
        fatal("copy-out buffer too small");
    memcpy(tmp, cast(void*)(cast(ulong*) bp + absIdx), need * ulong.sizeof);
    auto h2 = cast(PayloadHeader*) tmp;
    auto body_ = (cast(const(ulong)*)(tmp + PHLEN))[0 .. h2.plen];
    h2.call(h2, body_, called);
    atomicFetchAdd(head.pcount, 1UL);
}

/// Primary-path shard walk with a chosen claim chunk. Completer sweeps
/// other shards so the table always drains. No MT / adopt / feedback.
uint walkShard(shared(ulong)* bp, ulong tseqIdx, ulong tindexOff, ulong tcountOff,
               ulong progOff, uint tlen, uint sq, uint shi, uint chunk,
               bool checkFirst, bool copyOut, ulong* tmp, size_t tmpWords)
    nothrow @nogc @system
{
    uint shstart, shlen, shbase;
    if (tlen < SMALL_TABLE_THRESHOLD)
    {
        if (shi != 0) return 0;
        shstart = 0;
        shlen = tlen;
        shbase = tlen;
    }
    else
    {
        shbase = tlen / sq;
        immutable shrm = tlen % sq;
        shlen = shi < shrm ? shbase + 1 : shbase;
        shstart = shi * shbase + (shi < shrm ? shi : shrm);
    }
    if (shlen == 0)
        return 0;
    immutable shiter = (shlen + chunk - 1) / chunk;
    auto shc = &bp[tcountOff + shi * 8];
    if (checkFirst)
    {
        immutable claims = cast(uint)(atomicLoad!(MemoryOrder.raw)(*shc) >> 32);
        if (claims >= shiter)
            return 0;
    }
    for (;;)
    {
        immutable rawc = atomicFetchAdd(*shc, 1UL << 32);
        immutable x = cast(uint)(rawc >> 32);
        if (x >= shiter)
            return 0;
        immutable runstart = shstart + x * chunk;
        immutable runlen = chunk < shlen - x * chunk ? chunk : shlen - x * chunk;
        foreach (i; 0 .. runlen)
        {
            immutable poff = atomicLoad!(MemoryOrder.raw)(bp[tindexOff + runstart + i]);
            if (copyOut)
                enterCopyOut(bp, tseqIdx + poff, tmp, tmpWords);
            else
                enterInPlace(bp, tseqIdx + poff);
        }
        immutable y = atomicFetchAdd(*shc, cast(ulong) runlen);
        if ((y & 0xFFFF_FFFFUL) == shlen - runlen)
        {
            atomicFetchAdd(bp[progOff], cast(ulong) shlen);
            return 1;
        }
    }
}

bool digestTable(ConsumerView* v, uint chunk, bool copyOut, ulong* tmp, size_t tmpWords)
    nothrow @nogc @system
{
    auto f = v.F;
    auto bp = f.buf;
    immutable idx = v.nextSeq & f.Lmask;
    if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(v.nextSeq))
        return false;
    immutable tnext = atomicLoad!(MemoryOrder.raw)(bp[idx + 1]);
    immutable w2 = atomicLoad!(MemoryOrder.raw)(bp[idx + 2]);
    immutable tlen = cast(uint) w2;
    immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(bp[idx + 4]);
    immutable tindexOff = idx + THEAD;
    immutable tmt = cast(uint)(w2 >> 32);
    immutable progOff = tindexOff + tlen + tmt + 7;
    immutable tcountOff = progOff + 8;
    if (tlen > 0 && sq > 0)
    {
        immutable myShi = cast(uint)((v.IDc + v.nextSeq) % sq);
        immutable sweeper = (walkShard(bp, idx, tindexOff, tcountOff, progOff,
                                       tlen, sq, myShi, chunk, false, copyOut,
                                       tmp, tmpWords) & 1) != 0;
        if (sweeper)
        {
            immutable nshards = tlen < SMALL_TABLE_THRESHOLD ? 1 : sq;
            foreach (s; 0 .. nshards)
                if (s != myShi)
                    walkShard(bp, idx, tindexOff, tcountOff, progOff,
                              tlen, sq, s, chunk, true, copyOut, tmp, tmpWords);
        }
    }
    v.nextSeq = tnext;
    return true;
}

struct ConsCtx
{
    AntFarm* f;
    Arm arm;
    long expected;
    MonoTime deadline;
}

void consumerMain(ConsCtx* c)
{
    ConsumerView v;
    if (v.subscribe(c.f) < 0)
        fatal("subscribe failed");
    while (atomicLoad(g_go) == 0)
        Thread.yield();
    if (atomicLoad(g_go) == 2)
    {
        v.unsubscribe();
        return;
    }

    immutable chunk = c.arm == Arm.linear1 ? 1u : 16u;
    immutable copyOut = c.arm == Arm.copyout16;
    ulong* tmp;
    size_t tmpWords;
    if (copyOut)
    {
        tmpWords = PHLEN + 4096;
        tmp = cast(ulong*) malloc(tmpWords * ulong.sizeof);
        if (tmp is null)
            fatal("copy-out alloc");
    }

    // All arms share digestTable so layout/chunk/copy-out are the only
    // variables. consumeNext is a separate production-path check if needed.
    while (atomicLoad!(MemoryOrder.acq)(g_calls) < c.expected)
    {
        if (!digestTable(&v, chunk, copyOut, tmp, tmpWords))
        {
            if (MonoTime.currTime > c.deadline)
                break;
            Thread.yield();
        }
    }
    if (tmp !is null)
        free(tmp);
    v.unsubscribe();
}

struct Trial
{
    Arm arm;
    Mix mix;
    uint body;
    double nsJob = 0;
    double jobsPerSec = 0;
    long calls;
    ulong jobs;
    bool ok;
    const(char)[] err;
}

Trial runOnce(Cfg cfg, Arm arm)
{
    Trial t;
    t.arm = arm;
    t.mix = cfg.mix;
    t.body = cfg.body;

    immutable nTables = (cfg.jobs + cfg.tlen - 1) / cfg.tlen;
    immutable total = nTables * cfg.tlen;

    auto f = AntFarm.create(cfg.ln, cfg.k, cfg.nc, cfg.nb, 0, 0, 4096);
    auto headers = cast(PayloadHeader*) malloc(cfg.tlen * PayloadHeader.sizeof);
    auto body = cast(ulong*) malloc(cfg.body * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(cfg.tlen * PayloadEntry.sizeof);
    if (headers is null || body is null || entries is null)
    {
        t.err = "alloc";
        if (headers) free(headers);
        if (body) free(body);
        if (entries) free(entries);
        f.destroy();
        return t;
    }
    foreach (i; 0 .. cfg.body)
        body[i] = 0x9E37_79B9_7F4A_7C15UL ^ (cast(ulong) i * 0xBF58_476D_1CE4_E5B9UL);
    foreach (i; 0 .. cfg.tlen)
    {
        headers[i] = PayloadHeader.init;
        headers[i].maxCs = 1;
        headers[i].done = 1;
        headers[i].call = mixCall(cfg.mix, i);
        entries[i].header = &headers[i];
        entries[i].body = body[0 .. cfg.body];
    }

    Token[8] toks;
    if (cfg.nb > toks.length)
        fatal("too many bulk producers");
    foreach (i; 0 .. cfg.nb)
    {
        toks[i] = f.registerProducer(Tier.bulk);
        if (!toks[i].valid)
            fatal("register");
    }

    ulong[] exi = new ulong[cfg.nb];
    ulong published;
    uint stalls;
    auto deadlinePub = MonoTime.currTime + 30.seconds;
    uint seq;
    while (published < total)
    {
        if (MonoTime.currTime > deadlinePub)
        {
            t.err = "publish timeout";
            break;
        }
        immutable pi = seq % cfg.nb;
        immutable remain = total - published;
        immutable want = remain < cfg.tlen ? cast(uint) remain : cfg.tlen;
        immutable before = atomicLoad(f.Wt);
        immutable n = f.write(entries[0 .. want], exi[pi], toks[pi]);
        if (n == 0)
        {
            ++stalls;
            Thread.yield();
            continue;
        }
        if (arm == Arm.shuffled16)
        {
            auto w = cast(ulong*) f.buf + (before & f.Lmask);
            shuffleTindex(w, cast(uint) n, cast(uint) before ^ (seq * 0x9E37_79B9));
        }
        published += n;
        ++seq;
    }

    foreach (i; 0 .. cfg.nb)
        f.unregisterProducer(toks[i]);

    if (t.err.length)
    {
        free(headers);
        free(body);
        free(entries);
        f.destroy();
        return t;
    }

    atomicStore(g_calls, 0L);
    atomicStore(g_go, 0);
    auto cctx = cast(ConsCtx*) malloc(cfg.nc * ConsCtx.sizeof);
    Thread[] consumers = new Thread[cfg.nc];
    auto drainDeadline = MonoTime.currTime + 60.seconds;
    foreach (i; 0 .. cfg.nc)
        cctx[i] = ConsCtx(f, arm, cast(long) published, drainDeadline);

    final class ConsJob
    {
        ConsCtx* c;
        this(ConsCtx* c) { this.c = c; }
        void run() { consumerMain(c); }
    }

    foreach (i; 0 .. cfg.nc)
    {
        auto job = new ConsJob(&cctx[i]);
        consumers[i] = new Thread(&job.run);
        consumers[i].start();
    }
    // Let them subscribe and park on the empty nextSeq before we time.
    Thread.sleep(20.msecs);

    GC.collect();
    GC.disable();
    immutable t0 = MonoTime.currTime;
    atomicStore(g_go, 1);
    foreach (th; consumers)
        th.join();
    immutable t1 = MonoTime.currTime;
    GC.enable();

    immutable secs = (t1 - t0).total!"nsecs" / 1e9;
    immutable calls = atomicLoad(g_calls);
    t.calls = calls;
    t.jobs = published;
    if (published == 0)
        t.err = "nothing published";
    else if (calls != cast(long) published)
        t.err = "lost work";
    else
    {
        t.ok = true;
        t.nsJob = secs * 1e9 / published;
        t.jobsPerSec = published / secs;
    }

    free(cctx);
    free(headers);
    free(body);
    free(entries);
    f.destroy();
    return t;
}

Trial runMedian(Cfg cfg, Arm arm)
{
    Trial[8] rs;
    uint n;
    foreach (_; 0 .. cfg.repeats)
    {
        rs[n++] = runOnce(cfg, arm);
        if (!rs[n - 1].ok)
            return rs[n - 1];
    }
    foreach (i; 0 .. n)
        foreach (j; i + 1 .. n)
            if (rs[j].nsJob < rs[i].nsJob)
            {
                auto tmp = rs[i];
                rs[i] = rs[j];
                rs[j] = tmp;
            }
    return rs[n / 2];
}

void printHeader()
{
    printf("%-12s %-6s %5s %12s %10s %10s  %s\n",
           "arm".ptr, "mix".ptr, "body".ptr, "jobs".ptr, "ns/job".ptr,
           "Mjob/s".ptr, "note".ptr);
}

void printRow(ref const Trial t, ulong jobs)
{
    if (!t.ok)
    {
        printf("%-12s %-6s %5u %12llu %10s %10s  %.*s\n",
               armName[t.arm].ptr, mixName[t.mix].ptr, t.body, jobs,
               "-".ptr, "-".ptr,
               cast(int) t.err.length, t.err.ptr);
        return;
    }
    printf("%-12s %-6s %5u %12llu %10.2f %10.3f\n",
           armName[t.arm].ptr, mixName[t.mix].ptr, t.body, jobs,
           t.nsJob, t.jobsPerSec / 1e6);
}

Cfg parse(string[] args)
{
    Cfg c;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (a == "--touch") { c.mix = Mix.touch; continue; }
        if (a == "--chase") { c.mix = Mix.chase; continue; }
        if (a == "--alt") { c.mix = Mix.alt; continue; }
        if (a == "--arm" && i + 1 < args.length)
        {
            auto v = args[++i];
            c.allArms = false;
            if (v == "linear16") c.oneArm = Arm.linear16;
            else if (v == "linear1") c.oneArm = Arm.linear1;
            else if (v == "shuffled16") c.oneArm = Arm.shuffled16;
            else if (v == "copyout16") c.oneArm = Arm.copyout16;
            continue;
        }
        if (i + 1 >= args.length) break;
        auto v = args[++i];
        if (a == "--body") c.body = cast(uint) atoi(v.ptr);
        else if (a == "--jobs") c.jobs = strtoull(v.ptr, null, 0);
        else if (a == "--tlen") c.tlen = cast(uint) atoi(v.ptr);
        else if (a == "--ln") c.ln = strtoull(v.ptr, null, 0);
        else if (a == "--nc") c.nc = cast(uint) atoi(v.ptr);
        else if (a == "--repeats") c.repeats = cast(uint) atoi(v.ptr);
        else --i;
    }
    // One unconsumed lap: pin sits on the subscribe/dummy segment.
    immutable seg = c.ln / c.k;
    immutable budget = (c.k - 1) * seg * 8 / 10;
    immutable per = 54 + (17UL + c.body + 1) * c.tlen;
    immutable cap = (budget / per) * c.tlen;
    if (c.jobs > cap)
        c.jobs = cap;
    return c;
}

void main(string[] args)
{
    auto cfg = parse(args);
    if (cfg.tlen < SMALL_TABLE_THRESHOLD)
    {
        fprintf(stderr, "tlen must be >= %u (sharded tables)\n", SMALL_TABLE_THRESHOLD);
        abort();
    }

    g_worldN = (32UL << 20) / ulong.sizeof; // 32 MiB cold world
    g_world = cast(ulong*) malloc(g_worldN * ulong.sizeof);
    if (g_world is null)
        fatal("world alloc");
    foreach (i; 0 .. g_worldN)
        g_world[i] = i * 0x9E37_79B9_7F4A_7C15UL;

    printf("digest  Ln=%llu (%.1f MiB)  nc=%u  tlen=%u  jobs~%llu  mix=%s  repeats=%u\n",
           cast(ulong) cfg.ln, cfg.ln * 8.0 / (1024.0 * 1024.0),
           cfg.nc, cfg.tlen, cast(ulong) cfg.jobs,
           mixName[cfg.mix].ptr, cfg.repeats);
    printHeader();
    fflush(stdout);

    Arm[] arms;
    if (cfg.allArms)
        arms = [Arm.linear16, Arm.linear1, Arm.shuffled16, Arm.copyout16];
    else
        arms = [cfg.oneArm];

    Trial base;
    foreach (arm; arms)
    {
        auto t = runMedian(cfg, arm);
        printRow(t, t.jobs ? t.jobs : cfg.jobs);
        fflush(stdout);
        if (arm == Arm.linear16)
            base = t;
        else if (t.ok && base.ok && base.nsJob > 0)
            printf("           vs linear16: %.2fx\n", t.nsJob / base.nsJob);
    }
    free(g_world);
}
