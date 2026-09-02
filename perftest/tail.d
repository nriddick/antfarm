/++
 + Tail latency of a mid-tick 1-payload write against production consumeNext.
 +
 +   make -C perftest tail && ./perftest/tail
 +   ldc2 -O2 -release tail.d ../antfarm.d -of=tail.exe
 +
 + t0 is taken just before write() of the sentinel; Call records now-t0.
 + Background jobs spin; the sentinel only timestamps.
 +/
module tail;

import antfarm;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, abort, atoi, strtoull;

version (Windows)
{
    import core.sys.windows.winbase : GetCurrentThread, GetSystemInfo, SetThreadAffinityMask, SYSTEM_INFO;
}
else version (Posix)
{
    import core.sys.posix.unistd : sysconf, _SC_NPROCESSORS_ONLN;
    version (linux)
    {
        extern (C) int sched_setaffinity(int pid, size_t cpusetsize, const(void)* mask) nothrow @nogc;
    }
}

enum Scene : int { idle, mid, burst, near, mbox }

immutable string[5] sceneName = ["idle", "mid-drain", "burst", "near-full", "mailbox"];

struct Cfg
{
    ulong ln = 1UL << 21;
    uint k = 8;
    uint nc = 6;
    uint body = 16;
    uint tlen = 256;
    ulong spinNs = 1000;
    uint samples = 10_000;
    uint warmup = 200;
    uint burstN = 32;
    uint repeats = 3;
    bool pin = true;
    bool runIdle = true;
    bool runMid = true;
    bool runBurst = true;
    bool runNear = true;
    bool runOversub = true;
    bool runAttr = false;
    bool runMailbox = false;
    uint avgCost = 1;  // chunk hint for dump writes
    uint small = 64;   // small-table threshold; 0 = auto
    bool huge;         // --huge: MADV_HUGEPAGE on the magic-buffer mapping
}

__gshared shared(long) g_bg;
__gshared shared(long) g_sent;
__gshared shared(int) g_go;   // 0 = park, 1 = consume, 2 = stop
__gshared shared(int) g_ready;
__gshared shared(int) g_pinOk = 1;

__gshared long* g_lat;
__gshared long* g_wlat;
__gshared long* g_first;
__gshared long* g_last;
__gshared shared(size_t) g_nlat;
__gshared shared(size_t) g_nwlat;
__gshared shared(size_t) g_nfirst;
__gshared shared(size_t) g_nlast;
__gshared size_t g_cap;

__gshared ulong g_spinNs;
__gshared uint g_burstN;
__gshared uint g_avgCost = 1;  // chunk hint for dump/sentinel writes
__gshared uint g_small = 64;   // small-table threshold; 0 = auto
__gshared shared(long) g_mbox; // 0 = empty; ticks of a mailbox sentinel

long nowTicks() nothrow @nogc @system
{
    return MonoTime.currTime.ticks;
}

long ticksToNs(long dt) nothrow @nogc @system
{
    if (dt < 0) return 0;
    immutable tps = MonoTime.ticksPerSecond;
    return dt / tps * 1_000_000_000L + (dt % tps) * 1_000_000_000L / tps;
}

void rec(shared(size_t)* n, long* buf, long ns) nothrow @nogc @system
{
    immutable i = atomicFetchAdd(*n, 1);
    if (i < g_cap)
        buf[i] = ns;
}

size_t histCount(ref shared(size_t) n) nothrow @nogc @system
{
    immutable v = atomicLoad(n);
    return v < g_cap ? v : g_cap;
}

void spinFor(ulong ns) nothrow @nogc @system
{
    if (ns == 0) return;
    immutable end = MonoTime.currTime + dur!"nsecs"(ns);
    while (MonoTime.currTime < end) {}
}

long bgCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    spinFor(g_spinNs);
    atomicFetchAdd(g_bg, 1L);
    return 1;
}

long nopCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    return 1;
}

long sentCb(PayloadHeader* h, PayloadBody b, ulong iter) nothrow @nogc @system
{
    immutable dt = ticksToNs(nowTicks() - cast(long)(b.length ? b[0] : 0));
    rec(&g_nlat, g_lat, dt);
    if (b.length > 1)
    {
        if (b[1] == 0)
            rec(&g_nfirst, g_first, dt);
        if (g_burstN != 0 && b[1] == g_burstN - 1)
            rec(&g_nlast, g_last, dt);
    }
    atomicFetchAdd(g_sent, 1L);
    return 1;
}

uint onlineCpus() nothrow @nogc @system
{
    version (Windows)
    {
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        return si.dwNumberOfProcessors;
    }
    else version (Posix)
    {
        immutable n = sysconf(_SC_NPROCESSORS_ONLN);
        return n > 0 ? cast(uint) n : 0;
    }
    else
        return 0;
}

bool pinTo(uint cpu) nothrow @nogc @system
{
    version (Windows)
    {
        // SetThreadAffinityMask is one 64-bit group-0 mask. Fail rather than
        // wrapping the shift (D masks shift counts), which would pin the wrong LP.
        if (cpu >= size_t.sizeof * 8)
            return false;
        return SetThreadAffinityMask(GetCurrentThread(), cast(size_t)(1UL << cpu)) != 0;
    }
    else version (linux)
    {
        ulong[16] mask;
        if (cpu >= mask.length * 64)
            return false;
        mask[cpu / 64] = 1UL << (cpu % 64);
        return sched_setaffinity(0, mask.sizeof, mask.ptr) == 0;
    }
    else
        return false;
}

void resetHist()
{
    atomicStore(g_nlat, 0);
    atomicStore(g_nwlat, 0);
    atomicStore(g_nfirst, 0);
    atomicStore(g_nlast, 0);
    atomicStore(g_bg, 0L);
    atomicStore(g_sent, 0L);
    atomicStore(g_mbox, 0L);
}

struct Pct
{
    size_t n;
    long p50, p99, p999, mx;
}

void insertionSort(long[] a)
{
    foreach (i; 1 .. a.length)
    {
        immutable x = a[i];
        sizediff_t j = cast(sizediff_t) i - 1;
        while (j >= 0 && a[j] > x)
        {
            a[j + 1] = a[j];
            --j;
        }
        a[j + 1] = x;
    }
}

Pct pctOf(long* buf, size_t n)
{
    Pct p;
    p.n = n;
    if (n == 0)
        return p;
    insertionSort(buf[0 .. n]);
    p.p50 = buf[n / 2];
    p.p99 = buf[n ? (n - 1) * 99 / 100 : 0];
    p.p999 = buf[n ? (n - 1) * 999 / 1000 : 0];
    p.mx = buf[n - 1];
    return p;
}

struct Row
{
    Scene scene;
    uint nc;
    uint tlen;
    ulong spinNs;
    Pct lat;
    Pct wlat;
    Pct first;
    Pct last;
    long miss;
    long zeros;
    long parked; // near-full: payloads before first 0
    bool ok;
    const(char)[] err;
}

struct ConsCtx
{
    AntFarm* f;
    uint cpu;
    bool doPin;
}

// Module-level (not nested in startFarm): a function-nested class keeps
// `_outer` on the creating stack frame, and startFarm returns while the
// workers are still running.  Jobs live in FarmSet.jobs; threads take
// `&jobs[i].run` so a loop-local cannot alias every worker to the last
// context (see review_torture/torture_tests.d).
final class ConsJob
{
    ConsCtx c;
    this(ConsCtx c) { this.c = c; }
    void run() { consumerMain(&c); }
}

final class MboxJob
{
    uint cpu;
    bool doPin;
    this(uint cpu, bool doPin) { this.cpu = cpu; this.doPin = doPin; }
    void run() { mailboxMain(cpu, doPin); }
}

void consumerMain(ConsCtx* c)
{
    if (c.doPin && !pinTo(c.cpu))
        atomicStore(g_pinOk, 0);
    ConsumerView v;
    if (v.subscribe(c.f) < 0)
        fatal("subscribe failed");
    atomicFetchAdd(g_ready, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 0) {}
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 1)
        v.consumeNext();
    v.unsubscribe();
}

struct FarmSet
{
    AntFarm* f;
    Token bulk;
    Token small;
    PayloadHeader* bgH;
    PayloadHeader* sentH;
    PayloadEntry* dump;
    ulong* bgBody;
    ulong* sentBody;
    Thread[] consumers;
    ConsJob[] jobs;
    uint nc;
}

void waitReady(uint nc, MonoTime deadline)
{
    while (atomicLoad(g_ready) < cast(int) nc)
    {
        if (MonoTime.currTime > deadline)
            fatal("subscribe timeout");
        Thread.yield();
    }
}

bool waitUntil(ref shared(long) c, long want, MonoTime deadline)
{
    while (atomicLoad(c) < want)
    {
        if (MonoTime.currTime > deadline)
            return false;
    }
    return true;
}

FarmSet startFarm(Cfg cfg, uint nc)
{
    FarmSet s;
    s.nc = nc;
    s.f = AntFarm.create(cfg.ln, cfg.k, nc, 1, 0, 1, 4096, cfg.small, cfg.huge);
    s.bulk = s.f.registerProducer(Tier.bulk);
    s.small = s.f.registerProducer(Tier.small);
    if (!s.bulk.valid || !s.small.valid)
        fatal("register");
    s.bgH = cast(PayloadHeader*) malloc(PayloadHeader.sizeof);
    s.sentH = cast(PayloadHeader*) malloc(PayloadHeader.sizeof);
    // Cover the largest mid-drain cell (8192) even when Cfg.tlen is smaller.
    immutable dumpCap = cfg.tlen > 8192 ? cfg.tlen : 8192u;
    s.dump = cast(PayloadEntry*) malloc(dumpCap * PayloadEntry.sizeof);
    s.bgBody = cast(ulong*) malloc(cfg.body * ulong.sizeof);
    s.sentBody = cast(ulong*) malloc(cfg.body * ulong.sizeof);
    if (!s.bgH || !s.sentH || !s.dump || !s.bgBody || !s.sentBody)
        fatal("alloc");
    foreach (i; 0 .. cfg.body)
    {
        s.bgBody[i] = 0xC0FFEEUL + i;
        s.sentBody[i] = 0;
    }
    *s.bgH = PayloadHeader.init;
    s.bgH.maxCs = 1;
    s.bgH.done = 1;
    s.bgH.call = &bgCb;
    *s.sentH = PayloadHeader.init;
    s.sentH.maxCs = 1;
    s.sentH.done = 1;
    s.sentH.call = &sentCb;
    foreach (i; 0 .. dumpCap)
    {
        s.dump[i].header = s.bgH;
        s.dump[i].body = s.bgBody[0 .. cfg.body];
    }
    atomicStore!(MemoryOrder.rel)(g_go, 0);
    atomicStore(g_ready, 0);
    atomicStore(g_pinOk, 1);
    s.jobs = new ConsJob[](nc);
    s.consumers = new Thread[nc];

    foreach (i; 0 .. nc)
    {
        s.jobs[i] = new ConsJob(ConsCtx(s.f, i, cfg.pin));
        s.consumers[i] = new Thread(&s.jobs[i].run);
        s.consumers[i].start();
    }
    waitReady(nc, MonoTime.currTime + 5.seconds);
    if (cfg.pin)
    {
        immutable ncpu = onlineCpus();
        immutable pc = nc < ncpu ? nc : (ncpu ? ncpu - 1 : 0);
        if (!pinTo(pc))
            atomicStore(g_pinOk, 0);
    }
    return s;
}

void stopFarm(ref FarmSet s)
{
    atomicStore!(MemoryOrder.rel)(g_go, 2);
    foreach (th; s.consumers)
        th.join();
    s.f.unregisterProducer(s.bulk);
    s.f.unregisterProducer(s.small);
    s.f.destroy();
    free(s.bgH);
    free(s.sentH);
    free(s.dump);
    free(s.bgBody);
    free(s.sentBody);
    s.f = null;
}

ulong writeDump(ref FarmSet s, uint tlen)
{
    return s.f.write(s.dump[0 .. tlen], s.bulk, g_avgCost);
}

ulong writeSent(ref FarmSet s, long t0ticks, ulong slot)
{
    s.sentBody[0] = cast(ulong) t0ticks;
    if (s.dump[0].body.length > 1)
        s.sentBody[1] = slot;
    PayloadEntry e;
    e.header = s.sentH;
    e.body = s.sentBody[0 .. s.dump[0].body.length];
    return s.f.write((&e)[0 .. 1], s.small, g_avgCost);
}

Row runIdle(Cfg cfg, uint nc)
{
    Row r;
    r.scene = Scene.idle;
    r.nc = nc;
    r.tlen = 0;
    r.spinNs = 0;
    g_spinNs = 0;
    g_burstN = 0;
    resetHist();
    auto s = startFarm(cfg, nc);
    GC.collect();
    GC.disable();
    atomicStore!(MemoryOrder.rel)(g_go, 1);

    immutable want = cfg.warmup + cfg.samples;
    long seen;
    auto deadline = MonoTime.currTime + 30.seconds;
    while (seen < want)
    {
        if (MonoTime.currTime > deadline)
        {
            r.err = "idle timeout";
            break;
        }
        immutable wt0 = nowTicks();
        immutable t0 = nowTicks();
        immutable n = writeSent(s, t0, 0);
        rec(&g_nwlat, g_wlat, ticksToNs(nowTicks() - wt0));
        if (n == 0)
        {
            ++r.zeros;
            continue;
        }
        if (n != 1)
            fatal("idle short write");
        ++seen;
        if (!waitUntil(g_sent, seen, MonoTime.currTime + 2.seconds))
        {
            r.err = "idle sentinel stuck";
            break;
        }
    }
    GC.enable();
    stopFarm(s);
    if (r.err.length)
        return r;
    // drop warmup from the front of the recorded arrays
    immutable got = histCount(g_nlat);
    size_t use = got;
    long* lat = g_lat;
    if (got > cfg.warmup)
    {
        lat = g_lat + cfg.warmup;
        use = got - cfg.warmup;
    }
    r.lat = pctOf(lat, use);
    r.wlat = pctOf(g_wlat, histCount(g_nwlat));
    r.ok = r.lat.n > 0;
    if (!r.ok)
        r.err = "no samples";
    return r;
}

Row runMid(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    Row r;
    r.scene = Scene.mid;
    r.nc = nc;
    r.tlen = tlen;
    r.spinNs = spinNs;
    g_spinNs = spinNs;
    g_burstN = 0;
    resetHist();
    auto s = startFarm(cfg, nc);
    GC.collect();
    GC.disable();
    atomicStore!(MemoryOrder.rel)(g_go, 1);

    immutable trigger = tlen <= 16 ? 1u : (tlen / 4 < 16 ? 16u : tlen / 4);
    immutable want = cfg.warmup + cfg.samples;
    long sentSeen;
    auto deadline = MonoTime.currTime + 120.seconds;
    while (sentSeen < want)
    {
        if (MonoTime.currTime > deadline)
        {
            r.err = "mid timeout";
            break;
        }
        immutable bg0 = atomicLoad(g_bg);
        immutable dumped = writeDump(s, tlen);
        if (dumped == 0)
        {
            ++r.zeros;
            Thread.yield();
            continue;
        }
        if (dumped != tlen)
        {
            // partial dump: still usable if > trigger
            if (dumped < trigger)
            {
                ++r.zeros;
                continue;
            }
        }
        auto win = MonoTime.currTime + 5.seconds;
        while (atomicLoad(g_bg) - bg0 < trigger)
        {
            if (MonoTime.currTime > win)
                break;
        }
        immutable progressed = atomicLoad(g_bg) - bg0;
        if (progressed >= dumped)
        {
            ++r.miss;
            // still publish a sentinel so the ring stays well-formed, untimed
            writeSent(s, nowTicks(), 0);
            waitUntil(g_sent, sentSeen + r.miss, MonoTime.currTime + 2.seconds);
            waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds);
            continue;
        }
        immutable wt0 = nowTicks();
        immutable t0 = nowTicks();
        immutable n = writeSent(s, t0, 0);
        rec(&g_nwlat, g_wlat, ticksToNs(nowTicks() - wt0));
        if (n == 0)
        {
            ++r.zeros;
            waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds);
            continue;
        }
        if (n != 1)
            fatal("mid short write");
        ++sentSeen;
        if (!waitUntil(g_sent, sentSeen + r.miss, MonoTime.currTime + 2.seconds))
        {
            r.err = "mid sentinel stuck";
            break;
        }
        if (!waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds))
        {
            r.err = "mid dump stuck";
            break;
        }
    }
    GC.enable();
    stopFarm(s);
    if (r.err.length)
        return r;
    immutable got = histCount(g_nlat);
    size_t use = got;
    long* lat = g_lat;
    if (got > cfg.warmup)
    {
        lat = g_lat + cfg.warmup;
        use = got - cfg.warmup;
    }
    r.lat = pctOf(lat, use);
    r.wlat = pctOf(g_wlat, histCount(g_nwlat));
    r.ok = r.lat.n > 0;
    if (!r.ok)
        r.err = r.miss ? "all misses" : "no samples";
    return r;
}

void mailboxMain(uint cpu, bool doPin)
{
    if (doPin && !pinTo(cpu))
        atomicStore(g_pinOk, 0);
    atomicFetchAdd(g_ready, 1);
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 0) {}
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 1)
    {
        immutable t0 = atomicLoad!(MemoryOrder.acq)(g_mbox);
        if (t0 != 0)
        {
            rec(&g_nlat, g_lat, ticksToNs(nowTicks() - t0));
            atomicStore!(MemoryOrder.rel)(g_mbox, 0L);
            atomicFetchAdd(g_sent, 1L);
        }
    }
}

Row runMailbox(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    Row r;
    r.scene = Scene.mbox;
    r.nc = nc;
    r.tlen = tlen;
    r.spinNs = spinNs;
    g_spinNs = spinNs;
    g_burstN = 0;
    resetHist();
    // Farm workers drain the dump; a dedicated poller is the OOB channel.
    auto s = startFarm(cfg, nc);
    immutable ncpu = onlineCpus();
    immutable mcpu = (nc + 1 < ncpu) ? nc + 1 : (ncpu ? ncpu - 1 : 0);
    auto mj = new MboxJob(mcpu, cfg.pin);
    auto mth = new Thread(&mj.run);
    mth.start();
    waitReady(nc + 1, MonoTime.currTime + 5.seconds);

    GC.collect();
    GC.disable();
    atomicStore!(MemoryOrder.rel)(g_go, 1);

    immutable trigger = tlen <= 16 ? 1u : (tlen / 4 < 16 ? 16u : tlen / 4);
    immutable want = cfg.warmup + cfg.samples;
    long sentSeen;
    auto deadline = MonoTime.currTime + 120.seconds;
    while (sentSeen < want)
    {
        if (MonoTime.currTime > deadline)
        {
            r.err = "mbox timeout";
            break;
        }
        immutable bg0 = atomicLoad(g_bg);
        immutable dumped = writeDump(s, tlen);
        if (dumped == 0)
        {
            ++r.zeros;
            Thread.yield();
            continue;
        }
        auto win = MonoTime.currTime + 5.seconds;
        while (atomicLoad(g_bg) - bg0 < trigger)
        {
            if (MonoTime.currTime > win)
                break;
        }
        if (atomicLoad(g_bg) - bg0 >= dumped)
        {
            ++r.miss;
            atomicStore!(MemoryOrder.rel)(g_mbox, nowTicks());
            waitUntil(g_sent, sentSeen + r.miss, MonoTime.currTime + 2.seconds);
            waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds);
            continue;
        }
        immutable t0 = nowTicks();
        atomicStore!(MemoryOrder.rel)(g_mbox, t0 == 0 ? 1 : t0);
        ++sentSeen;
        if (!waitUntil(g_sent, sentSeen + r.miss, MonoTime.currTime + 2.seconds))
        {
            r.err = "mbox sentinel stuck";
            break;
        }
        if (!waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds))
        {
            r.err = "mbox dump stuck";
            break;
        }
    }
    GC.enable();
    atomicStore!(MemoryOrder.rel)(g_go, 2);
    mth.join();
    stopFarm(s);
    if (r.err.length)
        return r;
    immutable got = histCount(g_nlat);
    size_t use = got;
    long* lat = g_lat;
    if (got > cfg.warmup)
    {
        lat = g_lat + cfg.warmup;
        use = got - cfg.warmup;
    }
    r.lat = pctOf(lat, use);
    r.ok = r.lat.n > 0;
    if (!r.ok)
        r.err = "no samples";
    // reuse note via scene: printRow doesn't know mailbox. Tag tlen and
    // leave scene as mid-drain; caller prints a prefix.
    return r;
}

Row medMailbox(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    auto rs = new Row[cfg.repeats];
    foreach (i; 0 .. cfg.repeats)
        rs[i] = runMailbox(cfg, nc, tlen, spinNs);
    return pickMedianP99(rs);
}

Row runBurst(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    Row r;
    r.scene = Scene.burst;
    r.nc = nc;
    r.tlen = tlen;
    r.spinNs = spinNs;
    g_spinNs = spinNs;
    g_burstN = cfg.burstN;
    resetHist();
    auto s = startFarm(cfg, nc);
    GC.collect();
    GC.disable();
    atomicStore!(MemoryOrder.rel)(g_go, 1);

    immutable trigger = tlen <= 16 ? 1u : 16u;
    immutable bursts = (cfg.warmup + cfg.samples + cfg.burstN - 1) / cfg.burstN;
    long sentSeen;
    long burstDone;
    auto deadline = MonoTime.currTime + 120.seconds;
    while (burstDone < bursts)
    {
        if (MonoTime.currTime > deadline)
        {
            r.err = "burst timeout";
            break;
        }
        immutable bg0 = atomicLoad(g_bg);
        immutable dumped = writeDump(s, tlen);
        if (dumped == 0)
        {
            ++r.zeros;
            Thread.yield();
            continue;
        }
        auto win = MonoTime.currTime + 5.seconds;
        while (atomicLoad(g_bg) - bg0 < trigger)
        {
            if (MonoTime.currTime > win)
                break;
        }
        if (atomicLoad(g_bg) - bg0 >= dumped)
        {
            ++r.miss;
            foreach (k; 0 .. cfg.burstN)
                writeSent(s, nowTicks(), k);
            sentSeen += cfg.burstN;
            waitUntil(g_sent, sentSeen, MonoTime.currTime + 2.seconds);
            waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds);
            continue;
        }
        foreach (k; 0 .. cfg.burstN)
        {
            immutable t0 = nowTicks();
            immutable n = writeSent(s, t0, k);
            if (n == 0)
            {
                ++r.zeros;
                break;
            }
            ++sentSeen;
        }
        ++burstDone;
        if (!waitUntil(g_sent, sentSeen, MonoTime.currTime + 2.seconds))
        {
            r.err = "burst sentinel stuck";
            break;
        }
        if (!waitUntil(g_bg, bg0 + dumped, MonoTime.currTime + 10.seconds))
        {
            r.err = "burst dump stuck";
            break;
        }
    }
    GC.enable();
    stopFarm(s);
    if (r.err.length)
        return r;
    r.lat = pctOf(g_lat, histCount(g_nlat));
    r.first = pctOf(g_first, histCount(g_nfirst));
    r.last = pctOf(g_last, histCount(g_nlast));
    r.ok = r.lat.n > 0;
    if (!r.ok)
        r.err = "no samples";
    return r;
}

struct SprayCtx
{
    FarmSet* s;
    uint tlen;
}

final class SprayJob
{
    SprayCtx* c;
    this(SprayCtx* c) { this.c = c; }
    void run() { sprayMain(c); }
}

void sprayMain(SprayCtx* c)
{
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 0) {}
    while (atomicLoad!(MemoryOrder.acq)(g_go) == 1)
    {
        if (writeDump(*c.s, c.tlen) == 0)
            Thread.yield();
    }
}

Row runNear(Cfg cfg, uint nc)
{
    Row r;
    r.scene = Scene.near;
    r.nc = nc;
    r.tlen = cfg.tlen;
    r.spinNs = 0;
    g_spinNs = 0;
    g_burstN = 0;
    resetHist();
    auto s = startFarm(cfg, nc);

    // Parked fill: consumers subscribed but not consuming. Probe with a
    // no-op header so a successful probe is not a timed sentinel.
    auto probeH = *s.sentH;
    probeH.call = &nopCb;
    PayloadEntry probe;
    probe.header = &probeH;
    probe.body = s.sentBody[0 .. s.dump[0].body.length];
    long parked;
    for (;;)
    {
        immutable n = writeDump(s, cfg.tlen);
        if (n == 0)
            break;
        parked += n;
        if (s.f.write((&probe)[0 .. 1], s.small, g_avgCost) == 0)
        {
            r.parked = parked;
            break;
        }
        ++parked;
    }
    if (r.parked == 0)
        r.parked = parked;
    resetHist();

    GC.collect();
    GC.disable();
    atomicStore!(MemoryOrder.rel)(g_go, 1);

    auto spray = SprayCtx(&s, cfg.tlen);
    auto sj = new SprayJob(&spray);
    auto sprayTh = new Thread(&sj.run);
    sprayTh.start();

    immutable want = cfg.samples > 1000 ? 1000u : cfg.samples;
    auto deadline = MonoTime.currTime + 60.seconds;
    long got;
    while (got < want)
    {
        if (MonoTime.currTime > deadline)
        {
            r.err = "near timeout";
            break;
        }
        immutable t0 = nowTicks();
        ulong n;
        for (;;)
        {
            n = writeSent(s, t0, 0);
            if (n != 0)
                break;
            ++r.zeros;
            if (MonoTime.currTime > deadline)
                break;
        }
        rec(&g_nwlat, g_wlat, ticksToNs(nowTicks() - t0));
        if (n == 0)
            break;
        if (n != 1)
            fatal("near short write");
        ++got;
        if (!waitUntil(g_sent, got, MonoTime.currTime + 2.seconds))
        {
            r.err = "near sentinel stuck";
            break;
        }
    }
    atomicStore!(MemoryOrder.rel)(g_go, 2);
    sprayTh.join();
    // stopFarm also sets g_go=2 and joins consumers
    GC.enable();
    // consumers still running; stopFarm joins them
    atomicStore!(MemoryOrder.rel)(g_go, 2);
    stopFarm(s);
    if (r.err.length)
        return r;
    r.lat = pctOf(g_lat, histCount(g_nlat));
    r.wlat = pctOf(g_wlat, histCount(g_nwlat));
    r.ok = true; // zeros-only is still a result
    return r;
}

void fmtNs(char* buf, size_t n, long ns)
{
    if (ns >= 1_000_000)
        snprintf(buf, n, "%.2fms", ns / 1e6);
    else if (ns >= 1000)
        snprintf(buf, n, "%.1fus", ns / 1e3);
    else
        snprintf(buf, n, "%lldns", cast(long) ns);
}

void printHeader()
{
    printf("%-10s %3s %5s %6s %6s %8s %8s %8s %8s %6s %6s  %s\n",
           "scene".ptr, "nc".ptr, "tlen".ptr, "spin".ptr, "n".ptr,
           "p50".ptr, "p99".ptr, "p99.9".ptr, "max".ptr, "miss".ptr, "zero".ptr,
           "note".ptr);
}

void printRow(ref const Row r)
{
    char[8][4] b;
    fmtNs(b[0].ptr, b[0].length, r.lat.p50);
    fmtNs(b[1].ptr, b[1].length, r.lat.p99);
    fmtNs(b[2].ptr, b[2].length, r.lat.p999);
    fmtNs(b[3].ptr, b[3].length, r.lat.mx);
    char[16] spin;
    if (r.scene == Scene.idle)
        snprintf(spin.ptr, spin.length, "-");
    else if (r.spinNs >= 1000)
        snprintf(spin.ptr, spin.length, "%lluus", cast(ulong)(r.spinNs / 1000));
    else
        snprintf(spin.ptr, spin.length, "%lluns", cast(ulong) r.spinNs);

    if (!r.ok)
    {
        printf("%-10s %3u %5u %6s %6s %8s %8s %8s %8s %6lld %6lld  %.*s\n",
               sceneName[r.scene].ptr, r.nc, r.tlen, spin.ptr,
               "-".ptr, "-".ptr, "-".ptr, "-".ptr, "-".ptr,
               cast(long) r.miss, cast(long) r.zeros, cast(int) r.err.length, r.err.ptr);
        return;
    }
    char[64] note;
    note[0] = 0;
    if (r.scene == Scene.burst)
    {
        char[8] f50, l50, f99, l99;
        fmtNs(f50.ptr, f50.length, r.first.p50);
        fmtNs(l50.ptr, l50.length, r.last.p50);
        fmtNs(f99.ptr, f99.length, r.first.p99);
        fmtNs(l99.ptr, l99.length, r.last.p99);
        snprintf(note.ptr, note.length, "first %s/%s  last %s/%s",
                 f50.ptr, f99.ptr, l50.ptr, l99.ptr);
    }
    else if (r.scene == Scene.near)
    {
        char[8] w50, w99;
        fmtNs(w50.ptr, w50.length, r.wlat.p50);
        fmtNs(w99.ptr, w99.length, r.wlat.p99);
        snprintf(note.ptr, note.length, "admit %s/%s  parked=%lld",
                 w50.ptr, w99.ptr, cast(long) r.parked);
    }
    printf("%-10s %3u %5u %6s %6zu %8s %8s %8s %8s %6lld %6lld  %s\n",
           sceneName[r.scene].ptr, r.nc, r.tlen, spin.ptr,
           r.lat.n, b[0].ptr, b[1].ptr, b[2].ptr, b[3].ptr,
           cast(long) r.miss, cast(long) r.zeros, note.ptr);
}

Cfg parse(string[] args)
{
    Cfg c;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (a == "--no-pin") { c.pin = false; continue; }
        if (a == "--huge") { c.huge = true; continue; }
        if (a == "--idle-only")
        {
            c.runMid = c.runBurst = c.runNear = c.runOversub = false;
            continue;
        }
        if (a == "--mid-only")
        {
            c.runIdle = c.runBurst = c.runNear = c.runOversub = false;
            continue;
        }
        if (a == "--attr")
        {
            c.runIdle = c.runMid = c.runBurst = c.runNear = c.runOversub = false;
            c.runAttr = true;
            continue;
        }
        if (a == "--mailbox")
        {
            c.runIdle = c.runMid = c.runBurst = c.runNear = c.runOversub = false;
            c.runMailbox = true;
            continue;
        }
        if (i + 1 >= args.length) break;
        auto v = args[++i];
        if (a == "--nc") c.nc = cast(uint) atoi(v.ptr);
        else if (a == "--ac") c.avgCost = cast(uint) atoi(v.ptr);
        else if (a == "--small") c.small = cast(uint) atoi(v.ptr);
        else if (a == "--tlen") c.tlen = cast(uint) atoi(v.ptr);
        else if (a == "--spin") c.spinNs = strtoull(v.ptr, null, 0);
        else if (a == "--samples") c.samples = cast(uint) atoi(v.ptr);
        else if (a == "--ln") c.ln = strtoull(v.ptr, null, 0);
        else if (a == "--repeats") c.repeats = cast(uint) atoi(v.ptr);
        else --i;
    }
    return c;
}

Row pickMedianP99(Row[] rs)
{
    if (rs.length == 0)
        return Row.init;
    foreach (ref t; rs)
        if (!t.ok)
            return t;
    foreach (i; 0 .. rs.length)
        foreach (j; i + 1 .. rs.length)
            if (rs[j].lat.p99 < rs[i].lat.p99)
            {
                auto tmp = rs[i];
                rs[i] = rs[j];
                rs[j] = tmp;
            }
    return rs[rs.length / 2];
}

Row medIdle(Cfg cfg, uint nc)
{
    auto rs = new Row[cfg.repeats];
    foreach (i; 0 .. cfg.repeats)
        rs[i] = runIdle(cfg, nc);
    return pickMedianP99(rs);
}

Row medMid(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    auto rs = new Row[cfg.repeats];
    foreach (i; 0 .. cfg.repeats)
        rs[i] = runMid(cfg, nc, tlen, spinNs);
    return pickMedianP99(rs);
}

Row medBurst(Cfg cfg, uint nc, uint tlen, ulong spinNs)
{
    auto rs = new Row[cfg.repeats];
    foreach (i; 0 .. cfg.repeats)
        rs[i] = runBurst(cfg, nc, tlen, spinNs);
    return pickMedianP99(rs);
}

void main(string[] args)
{
    auto cfg = parse(args);
    g_avgCost = cfg.avgCost;
    g_small = cfg.small;
    g_cap = cfg.samples + cfg.warmup + 4096;
    if (cfg.burstN * ((cfg.samples + cfg.warmup) / cfg.burstN + 4) > g_cap)
        g_cap = cfg.burstN * ((cfg.samples + cfg.warmup) / cfg.burstN + 8);
    g_lat = cast(long*) malloc(g_cap * long.sizeof);
    g_wlat = cast(long*) malloc(g_cap * long.sizeof);
    g_first = cast(long*) malloc(g_cap * long.sizeof);
    g_last = cast(long*) malloc(g_cap * long.sizeof);
    if (!g_lat || !g_wlat || !g_first || !g_last)
        fatal("hist alloc");

    immutable ncpu = cast(int) onlineCpus();
    printf("tail  Ln=%llu (%.1f MiB)  nc=%u  samples=%u  warmup=%u  cpus=%d  pin=%s  repeats=%u  huge=%s\n",
           cast(ulong) cfg.ln, cfg.ln * 8.0 / (1024.0 * 1024.0),
           cfg.nc, cfg.samples, cfg.warmup, ncpu,
           cfg.pin ? "yes".ptr : "no".ptr, cfg.repeats, cfg.huge ? "yes".ptr : "no".ptr);
    printf("metric: ticks before write() of 1-payload sentinel → first insn of its Call\n");
    printf("100us is fine at 60Hz; 1ms is a real slice; p99 that grows with tlen is shard coupling\n");
    printHeader();
    fflush(stdout);

    void emit(Row r)
    {
        printRow(r);
        fflush(stdout);
        if (cfg.pin && atomicLoad(g_pinOk) == 0)
            printf("           note: pin failed; numbers may be scheduler tails\n");
    }

    if (cfg.runIdle)
        emit(medIdle(cfg, cfg.nc));

    if (cfg.runMid)
    {
        emit(medMid(cfg, cfg.nc, 256, 0));
        emit(medMid(cfg, cfg.nc, 256, 1000));
        emit(medMid(cfg, cfg.nc, 256, 10_000));
        emit(medMid(cfg, cfg.nc, 2048, 1000));
        emit(medMid(cfg, cfg.nc, 8192, 1000));
        emit(medMid(cfg, cfg.nc, 32, 1000));
        emit(medMailbox(cfg, cfg.nc, 256, 1000));
        emit(medMailbox(cfg, cfg.nc, 8192, 1000));
    }

    if (cfg.runBurst)
        emit(medBurst(cfg, cfg.nc, 256, 1000));

    if (cfg.runNear)
        emit(runNear(cfg, cfg.nc));

    if (cfg.runAttr)
    {
        printf("attribution (Farm table path and OOB mailbox):\n");
        emit(medMid(cfg, cfg.nc, 32, 1000));
        emit(medMid(cfg, cfg.nc, 256, 1000));
        emit(medMailbox(cfg, cfg.nc, 256, 1000));
        emit(medMailbox(cfg, cfg.nc, 8192, 1000));
    }

    if (cfg.runMailbox)
        emit(medMailbox(cfg, cfg.nc, cfg.tlen, cfg.spinNs ? cfg.spinNs : 1000));

    if (cfg.runOversub && cfg.nc != 8)
    {
        printf("oversub check (nc=8 on this host):\n");
        emit(medIdle(cfg, 8));
        emit(medMid(cfg, 8, 256, 1000));
    }

    free(g_lat);
    free(g_wlat);
    free(g_first);
    free(g_last);
}
