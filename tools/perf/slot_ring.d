/++
 + The shipped slot ring (`antfarm.ring.SlotRing`) against singleton Farm
 + tables, with a 32-byte-slot Vyukov ring as a density reference.
 +
 + Ported from the 2026-10-30 slot-ring study (branch `exp/slot-ring`),
 + without the Element Farm prototype. `--impl ring` is the shipped
 + `SlotRing!RingElement`: 64-byte slots holding the 56-byte element
 + `ant_uv` puts on its request ring (tag, generation, pointer, four inline
 + words), so its rows measure the real request layout. `--impl vyukov` is
 + the study's local Vyukov ring with 32-byte slots (two per line) and a
 + three-word element; `--impl farm` writes one uniform-fixed Farm table per
 + element with `--qs` words of small quota (dual.d's 4096 by default).
 +
 +   make -C tools/perf slot_ring
 +   ./slot_ring --impl ring --mode mpsc --threads 4
 +   ./slot_ring --impl farm --mode latency --cores 0,8
 +
 + Modes: `same` (one thread enqueues one and drains), `dual N` (N threads
 + each enqueue one then dequeue one), `spmc N` (one producer, N consumers),
 + `mpsc N` (N producers, one consumer: the shape of a loop's request ring),
 + `latency` (one-way enqueue-to-dequeue delay between two pinned cores:
 + p50/p90/p99/max over N samples). Threads pin to `--cores` in order; the
 + default is the 5900XT's sixteen physical cores, CCD0 first.
 +
 + `--rings P` splits the `--cap` slot budget over P rings. Each thread gets
 + a preferred ring by contiguous block of its role's threads, so with
 + cores pinned CCD0-first the rings split by CCD; consumers poll their
 + preferred ring first and then the others in order (`foreign` counts
 + elements taken elsewhere); producers write to their own preferred ring
 + (`--policy own`), spread elements round-robin over all rings
 + (`--policy rr`), or over the rings their own CCD's threads prefer
 + (`--policy local`, `--ccd-cores` per CCD, 8 here), under which consumers
 + also fall back only within their CCD's rings. In latency mode
 + `--target-ring r` sends every element to ring r while the consumer
 + prefers ring 0.
 +/
module slot_ring;

import antfarm;
import antfarm.ring : RingElement, RingPush, SlotRing;
import core.atomic;
import core.memory : GC;
import core.thread;
import core.time;
import core.stdc.stdio;
import core.stdc.stdlib : malloc, free, atoi, abort, strtoull, qsort;
import core.sys.posix.stdlib : posix_memalign;
import core.sys.linux.sched : cpu_set_t, CPU_SET, sched_setaffinity;
import ldc.intrinsics : llvm_readcyclecounter;

struct Elem { ulong tag; ulong ptr; ulong gen; }

enum Impl : ubyte { vyukov, ring, farm }
enum Mode : ubyte { same, dual, spmc, mpsc, latency }
/// Where a producer puts an element: its preferred ring, round-robin over
/// every ring, or round-robin over the rings its own CCD's threads prefer.
enum Policy : ubyte { own, rr, local }

// ==== Vyukov bounded MPMC ====
struct Vyukov(bool padded)
{
    static if (padded)
        align(64) struct Slot { shared ulong seq; Elem e; ubyte[32] pad; }
    else
        struct Slot { shared ulong seq; Elem e; }
    static assert(Slot.sizeof == (padded ? 64 : 32));

    // Cursors are padded by distance, 64 bytes of nothing on either side,
    // so each one's line holds only padding whatever the struct's
    // alignment (a stack local is 8-aligned). The slot array itself is
    // line-aligned by posix_memalign.
    ubyte[64] pad0;
    shared ulong enqPos;
    ubyte[64] pad1;
    shared ulong deqPos;
    ubyte[64] pad2;
    Slot* slots;
    size_t mask;

    bool init(size_t cap) nothrow @nogc @system
    {
        if (cap < 2 || (cap & (cap - 1))) return false;
        void* p;
        if (posix_memalign(&p, 64, cap * Slot.sizeof) != 0) return false;
        slots = cast(Slot*) p;
        mask = cap - 1;
        foreach (i; 0 .. cap)
        {
            atomicStore!(MemoryOrder.raw)(slots[i].seq, cast(ulong) i);
            slots[i].e = Elem.init;
        }
        atomicStore!(MemoryOrder.raw)(enqPos, 0UL);
        atomicStore!(MemoryOrder.raw)(deqPos, 0UL);
        return true;
    }

    void destroy() nothrow @nogc @system { free(slots); slots = null; }

    bool tryEnqueue(ref const Elem e) nothrow @nogc @system
    {
        auto pos = atomicLoad!(MemoryOrder.raw)(enqPos);
        Slot* s;
        for (;;)
        {
            s = &slots[pos & mask];
            immutable seq = atomicLoad!(MemoryOrder.acq)(s.seq);
            immutable diff = cast(long)(seq - pos);
            if (diff == 0)
            {
                if (cas!(MemoryOrder.raw, MemoryOrder.raw)(&enqPos, pos, pos + 1)) break;
                pos = atomicLoad!(MemoryOrder.raw)(enqPos);
            }
            else if (diff < 0)
                return false;
            else
                pos = atomicLoad!(MemoryOrder.raw)(enqPos);
        }
        s.e = e;
        atomicStore!(MemoryOrder.rel)(s.seq, pos + 1);
        return true;
    }

    bool tryDequeue(ref Elem e) nothrow @nogc @system
    {
        auto pos = atomicLoad!(MemoryOrder.raw)(deqPos);
        Slot* s;
        for (;;)
        {
            s = &slots[pos & mask];
            immutable seq = atomicLoad!(MemoryOrder.acq)(s.seq);
            immutable diff = cast(long)(seq - (pos + 1));
            if (diff == 0)
            {
                if (cas!(MemoryOrder.raw, MemoryOrder.raw)(&deqPos, pos, pos + 1)) break;
                pos = atomicLoad!(MemoryOrder.raw)(deqPos);
            }
            else if (diff < 0)
                return false;
            else
                pos = atomicLoad!(MemoryOrder.raw)(deqPos);
        }
        e = s.e;
        atomicStore!(MemoryOrder.rel)(s.seq, pos + mask + 1);
        return true;
    }
}

// A set of rings, each line-aligned, sharing one slot budget.
struct Rings(bool padded)
{
    Vyukov!padded* r;
    uint n;

    bool init(uint count, size_t capPer) nothrow @nogc @system
    {
        void* p;
        if (posix_memalign(&p, 64, count * Vyukov!padded.sizeof) != 0) return false;
        r = cast(Vyukov!padded*) p;
        n = count;
        (cast(ubyte*) p)[0 .. count * Vyukov!padded.sizeof] = 0;
        foreach (i; 0 .. count)
            if (!r[i].init(capPer)) return false;
        return true;
    }

    void destroy() nothrow @nogc @system
    {
        foreach (i; 0 .. n) r[i].destroy();
        free(r); r = null; n = 0;
    }
}

// The shipped ring, one per `--rings` member, with the default RingElement:
// the request layout ant_uv puts on its ring.
struct ShippedRings
{
    SlotRing!RingElement*[MAXR] r;
    uint n;

    bool init(uint count, size_t capPer) nothrow @nogc @system
    {
        n = count;
        foreach (i; 0 .. count)
        {
            r[i] = SlotRing!RingElement.create(capPer);
            if (r[i] is null) return false;
        }
        return true;
    }

    void destroy() nothrow @nogc @system
    {
        foreach (i; 0 .. n)
            if (r[i] !is null) r[i].destroy();
        n = 0;
    }

    bool tryEnqueue(uint ring, ref const Elem e) nothrow @nogc @system
    {
        RingElement x;
        x.tag = e.tag;
        x.pointer = e.ptr;
        x.generation = e.gen;
        return r[ring].tryPush(x) == RingPush.pushed;
    }

    bool tryDequeue(uint ring, ref Elem e) nothrow @nogc @system
    {
        RingElement x;
        if (!r[ring].tryPop(x)) return false;
        e = Elem(x.tag, x.pointer, x.generation);
        return true;
    }
}

// ==== shared state ====
__gshared shared(long) g_consumed;
__gshared shared(int) g_stop;
__gshared shared(int) g_ready;
__gshared shared(ulong) g_ack;
__gshared ulong* g_samples;
__gshared ulong g_sampleCount;

// Per-thread tally, flushed every LOCAL_FLUSH so a global atomic is not
// what gets measured (as in dual.d). The tag switch stands in for the
// consumer dispatching an activation by element kind.
enum uint LOCAL_FLUSH = 1024;
struct Tally { uint n; ulong fibers, actors, other; }
Tally* tally() nothrow @nogc @system { static Tally t; return &t; }

void work(ref const Elem e) nothrow @nogc @system
{
    auto t = tally();
    switch (e.tag)
    {
    case 1: ++t.fibers; break;
    case 2: ++t.actors; break;
    default: ++t.other; break;
    }
    if (++t.n >= LOCAL_FLUSH) flushTally();
}

void flushTally() nothrow @nogc @system
{
    auto t = tally();
    if (t.n != 0)
    {
        atomicFetchAdd(g_consumed, cast(long) t.n);
        t.n = 0;
    }
}

extern(C) void elemCb(const(ulong)* b, size_t n) nothrow @nogc @system
{
    Elem e = Elem(b[0], b[1], b[2]);
    work(e);
}

void recordLatency(ref const Elem e) nothrow @nogc @system
{
    immutable now = llvm_readcyclecounter();
    if (e.gen < g_sampleCount) g_samples[e.gen] = now - e.ptr;
    atomicStore!(MemoryOrder.rel)(g_ack, e.gen + 1);
}

extern(C) void latCb(const(ulong)* b, size_t n) nothrow @nogc @system
{
    Elem e = Elem(b[0], b[1], b[2]);
    recordLatency(e);
}

bool pinTo(uint cpu) nothrow @nogc @system
{
    cpu_set_t set;
    set.__bits[] = 0;
    CPU_SET(cpu, &set);
    return sched_setaffinity(0, set.sizeof, &set) == 0;
}

// ==== configuration ====
struct Cfg
{
    Impl impl = Impl.vyukov;
    Mode mode = Mode.same;
    uint threads = 4;        // dual: N dual threads; spmc/mpsc: N consumers/producers
    ulong n = 4_000_000;
    ulong cap = 65536;       // Vyukov slots in total, split over --rings
    uint rings = 1;
    Policy policy = Policy.own;
    uint ccdCores = 8;       // cores per CCD on this host, for --policy local
    uint targetRing;         // latency mode: ring every element goes to
    ulong qs = 4096;         // Farm small quota words
    uint repeats = 3;
    uint[32] cores;
    uint ncores;
    bool nSet;
}

struct Trial
{
    double secs;
    double mops;
    ulong stalls;
    ulong foreign;
    double p50, p90, p99, max;
    bool ok;
    const(char)[] err;
}

enum Role : ubyte { producer, consumer, dual, latProducer, latConsumer }

struct Ctx
{
    Cfg* cfg;
    Role role;
    uint cpu;
    uint peers;
    ulong share;             // elements this thread produces
    ulong produced;
    ulong stalls;
    MonoTime deadline;
    shared int* timedOut;
    uint pref;               // preferred ring
    uint localLo, localHi;   // rings preferred by same-role threads on this CCD
    ulong rrCounter;
    ulong foreign;           // elements dequeued from a non-preferred ring
    // rings
    Rings!false* r32;
    ShippedRings* ring;
    AntFarm* farm;
}

enum uint MAXR = 32;

uint ringFor(ref Ctx c) nothrow @nogc @system
{
    if (c.cfg.mode == Mode.latency) return c.cfg.targetRing;
    final switch (c.cfg.policy)
    {
    case Policy.own: return c.pref;
    case Policy.rr: return cast(uint)(c.rrCounter++ % c.cfg.rings);
    case Policy.local: return c.localLo + cast(uint)(c.rrCounter++ % (c.localHi - c.localLo + 1));
    }
}

final class Job
{
    Ctx* c;
    this(Ctx* c) { this.c = c; }
    void run() { threadMain(c); }
}

// ---- generic enqueue/dequeue over the three implementations
struct FarmEnds
{
    Token tok;
    ConsumerView view;
    bool hasFarmTok, hasFarmView;
    ulong[3] words;
    PayloadBody[1] bodies;
}

bool enqueue(Impl impl, ref Ctx c, ref FarmEnds fe, ref const Elem e, Callback cb) nothrow @nogc @system
{
    final switch (impl)
    {
    case Impl.vyukov: return c.r32.r[ringFor(c)].tryEnqueue(e);
    case Impl.ring: return c.ring.tryEnqueue(ringFor(c), e);
    case Impl.farm:
        fe.words[0] = e.tag; fe.words[1] = e.ptr; fe.words[2] = e.gen;
        return c.farm.write(cb, fe.bodies[], 3, fe.tok, DEFAULT_AVG_COST) == 1;
    }
}

/// One dequeue attempt; `true` when an element was taken (and worked).
bool dequeue(Impl impl, ref Ctx c, ref FarmEnds fe, bool latency) nothrow @nogc @system
{
    Elem e;
    // Look-first: the preferred ring, then the others in order. Under the
    // local policy the scan stays within this CCD's rings.
    immutable bool localOnly = c.cfg.policy == Policy.local && c.cfg.mode != Mode.latency;
    immutable uint lo = localOnly ? c.localLo : 0;
    immutable uint n = localOnly ? c.localHi - c.localLo + 1 : c.cfg.rings;
    immutable uint start = c.pref - lo;
    final switch (impl)
    {
    case Impl.vyukov:
        {
            uint k;
            for (; k < n; ++k)
                if (c.r32.r[lo + (start + k) % n].tryDequeue(e)) break;
            if (k == n) return false;
            if (k != 0) ++c.foreign;
        }
        break;
    case Impl.ring:
        {
            uint k;
            for (; k < n; ++k)
                if (c.ring.tryDequeue(lo + (start + k) % n, e)) break;
            if (k == n) return false;
            if (k != 0) ++c.foreign;
        }
        break;
    case Impl.farm:
        return fe.view.consumeNext();
    }
    if (latency) recordLatency(e); else work(e);
    return true;
}

void threadMain(Ctx* c)
{
    auto cfg = c.cfg;
    immutable impl = cfg.impl;
    tally().n = 0;
    if (!pinTo(c.cpu)) { fprintf(stderr, "pin to cpu %u failed\n", c.cpu); abort(); }

    FarmEnds fe;
    immutable produces = c.role == Role.producer || c.role == Role.dual || c.role == Role.latProducer;
    immutable consumes = c.role == Role.consumer || c.role == Role.dual || c.role == Role.latConsumer;
    immutable latency = c.role == Role.latProducer || c.role == Role.latConsumer;
    immutable cb = latency ? &latCb : &elemCb;
    if (impl == Impl.farm)
    {
        fe.bodies[0] = fe.words[];
        if (produces)
        {
            fe.tok = c.farm.registerProducer(Tier.small);
            if (!fe.tok.valid) { fprintf(stderr, "producer registration failed\n"); abort(); }
            fe.hasFarmTok = true;
        }
        if (consumes)
        {
            if (fe.view.subscribe(c.farm) < 0) { fprintf(stderr, "subscribe failed\n"); abort(); }
            fe.hasFarmView = true;
        }
    }

    immutable expected = cast(long) cfg.n;
    ulong produced;
    ulong stalls;
    Elem e = Elem(1, 0, 0);
    uint spins;

    final switch (c.role)
    {
    case Role.producer:
    case Role.dual:
        // Produce one (dual: then consume one) until the share is written,
        // then drain until every element is consumed.
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0)
        {
            if (produced < c.share)
            {
                e.tag = 1 + (produced & 1); e.ptr = produced; e.gen = produced;
                if (enqueue(impl, *c, fe, e, cb)) ++produced; else ++stalls;
            }
            if (c.role == Role.dual)
                dequeue(impl, *c, fe, false);
            if (produced >= c.share)
            {
                if (c.role == Role.producer) break;
                flushTally();
                if (atomicLoad!(MemoryOrder.acq)(g_consumed) >= expected) break;
            }
            if ((++spins & 1023) == 0 && MonoTime.currTime > c.deadline)
            { atomicStore(g_stop, 1); atomicStore(*c.timedOut, 1); }
        }
        if (c.role == Role.dual)
            goto case Role.consumer;
        break;
    case Role.consumer:
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0)
        {
            if (!dequeue(impl, *c, fe, false))
            {
                flushTally();
                if (atomicLoad!(MemoryOrder.acq)(g_consumed) >= expected) break;
                if ((++spins & 1023) == 0 && MonoTime.currTime > c.deadline)
                { atomicStore(g_stop, 1); atomicStore(*c.timedOut, 1); }
            }
        }
        break;
    case Role.latProducer:
        foreach (i; 0 .. cfg.n)
        {
            e.tag = 3; e.gen = i;
            e.ptr = llvm_readcyclecounter();
            while (!enqueue(impl, *c, fe, e, cb)) { ++stalls; }
            while (atomicLoad!(MemoryOrder.acq)(g_ack) != i + 1)
            {
                if ((++spins & 65535) == 0 && MonoTime.currTime > c.deadline)
                { atomicStore(g_stop, 1); atomicStore(*c.timedOut, 1); break; }
            }
            if (atomicLoad!(MemoryOrder.raw)(g_stop) != 0) break;
            produced = i + 1;
        }
        break;
    case Role.latConsumer:
        while (atomicLoad!(MemoryOrder.raw)(g_stop) == 0 && atomicLoad!(MemoryOrder.acq)(g_ack) < cfg.n)
        {
            dequeue(impl, *c, fe, true);
            if ((++spins & 65535) == 0 && MonoTime.currTime > c.deadline)
            { atomicStore(g_stop, 1); atomicStore(*c.timedOut, 1); }
        }
        break;
    }
    flushTally();
    c.produced = produced;
    c.stalls = stalls;

    if (impl == Impl.farm)
    {
        if (fe.hasFarmView) fe.view.unsubscribe();
        if (fe.hasFarmTok) c.farm.unregisterProducer(fe.tok);
    }
}

/// Whether a producing thread may write ring `rr` under its policy.
bool producesOn(ref Ctx c, uint rr) nothrow @nogc @system
{
    if (c.cfg.mode == Mode.latency) return rr == c.cfg.targetRing;
    final switch (c.cfg.policy)
    {
    case Policy.own: return rr == c.pref;
    case Policy.rr: return true;
    case Policy.local: return rr >= c.localLo && rr <= c.localHi;
    }
}

/// Whether a consuming thread polls ring `rr` under its policy.
bool consumesOn(ref Ctx c, uint rr) nothrow @nogc @system
{
    if (c.cfg.policy == Policy.local && c.cfg.mode != Mode.latency)
        return rr >= c.localLo && rr <= c.localHi;
    return true;
}

Trial runOnce(ref Cfg cfg)
{
    Trial t;
    uint nthreads;
    final switch (cfg.mode)
    {
    case Mode.same: nthreads = 1; break;
    case Mode.dual: nthreads = cfg.threads; break;
    case Mode.spmc: nthreads = cfg.threads + 1; break;
    case Mode.mpsc: nthreads = cfg.threads + 1; break;
    case Mode.latency: nthreads = 2; break;
    }
    if (nthreads == 0 || nthreads > cfg.ncores) { t.err = "not enough cores for the thread count"; return t; }

    uint producers, consumers;
    final switch (cfg.mode)
    {
    case Mode.same: producers = 1; consumers = 1; break;
    case Mode.dual: producers = cfg.threads; consumers = cfg.threads; break;
    case Mode.spmc: producers = 1; consumers = cfg.threads; break;
    case Mode.mpsc: producers = cfg.threads; consumers = 1; break;
    case Mode.latency: producers = 1; consumers = 1; break;
    }

    if (cfg.rings == 0 || cfg.cap < cfg.rings) { t.err = "bad rings"; return t; }
    immutable capPer = cast(size_t)(cfg.cap / cfg.rings);
    if (cfg.targetRing >= cfg.rings) { t.err = "target ring out of range"; return t; }
    Rings!false r32;
    ShippedRings ring;
    AntFarm* farm;
    if (cfg.rings > MAXR) { t.err = "too many rings"; return t; }
    final switch (cfg.impl)
    {
    case Impl.vyukov: if (!r32.init(cfg.rings, capPer)) { t.err = "ring alloc (capacity per ring must be a power of two)"; return t; } break;
    case Impl.ring: if (!ring.init(cfg.rings, capPer)) { ring.destroy(); t.err = "ring alloc (capacity per ring must be a power of two, 2 to 2^20)"; return t; } break;
    case Impl.farm:
        farm = AntFarm.create(16, 8, consumers, 0, 0, producers, cfg.qs, 64, false);
        break;
    }

    atomicStore(g_consumed, 0L);
    atomicStore(g_stop, 0);
    atomicStore(g_ready, 0);
    atomicStore(g_ack, 0UL);
    shared int timedOut;
    if (cfg.mode == Mode.latency)
    {
        g_sampleCount = cfg.n;
        g_samples = cast(ulong*) malloc(cfg.n * ulong.sizeof);
        foreach (i; 0 .. cfg.n) g_samples[i] = 0;
    }

    auto ctxs = cast(Ctx*) malloc(nthreads * Ctx.sizeof);
    auto deadline = MonoTime.currTime + 60.seconds;
    size_t cursor;
    uint pi;
    foreach (i; 0 .. nthreads)
    {
        Ctx c;
        c.cfg = &cfg;
        c.cpu = cfg.cores[i];
        c.peers = nthreads;
        c.deadline = deadline;
        c.timedOut = &timedOut;
        c.r32 = &r32; c.ring = &ring; c.farm = farm;
        final switch (cfg.mode)
        {
        case Mode.same:
            // One thread: enqueue one, drain. Modelled as dual with one member.
            c.role = Role.dual; break;
        case Mode.dual: c.role = Role.dual; break;
        case Mode.spmc: c.role = i == 0 ? Role.producer : Role.consumer; break;
        case Mode.mpsc: c.role = i == 0 ? Role.consumer : Role.producer; break;
        case Mode.latency: c.role = i == 0 ? Role.latConsumer : Role.latProducer; break;
        }
        // Preferred ring: contiguous blocks of the role's threads, so the
        // pinned core order (CCD0 first) splits the rings by CCD.
        uint idx, roleCount;
        final switch (c.role)
        {
        case Role.dual: idx = i; roleCount = nthreads; break;
        case Role.producer: idx = pi; roleCount = producers; break;
        case Role.consumer: idx = cfg.mode == Mode.spmc ? i - 1 : 0; roleCount = consumers; break;
        case Role.latProducer: idx = 0; roleCount = 1; break;
        case Role.latConsumer: idx = 0; roleCount = 1; break;
        }
        c.pref = cast(uint)((cast(ulong) idx * cfg.rings) / roleCount);
        if (c.role == Role.producer || c.role == Role.dual)
        {
            immutable remain = cast(size_t) cfg.n - cursor;
            immutable share = remain / (producers - pi);
            c.share = share; cursor += share; ++pi;
        }
        else if (c.role == Role.latProducer)
            c.share = cfg.n;
        ctxs[i] = c;
    }
    // Local ring range: the preferred rings of same-role threads whose core
    // is on the same CCD (dual threads count as producers).
    foreach (i; 0 .. nthreads)
    {
        immutable ccd = ctxs[i].cpu / cfg.ccdCores;
        immutable isProd = ctxs[i].role == Role.producer || ctxs[i].role == Role.dual;
        uint lo = uint.max, hi = 0;
        foreach (j; 0 .. nthreads)
        {
            immutable jProd = ctxs[j].role == Role.producer || ctxs[j].role == Role.dual;
            if (jProd != isProd || ctxs[j].cpu / cfg.ccdCores != ccd) continue;
            if (ctxs[j].pref < lo) lo = ctxs[j].pref;
            if (ctxs[j].pref > hi) hi = ctxs[j].pref;
        }
        if (lo == uint.max) { lo = 0; hi = cfg.rings - 1; }
        ctxs[i].localLo = lo; ctxs[i].localHi = hi;
    }

    Thread[] threads = new Thread[nthreads];
    auto jobs = new Job[](nthreads);
    GC.collect(); GC.minimize(); GC.disable();
    immutable t0 = MonoTime.currTime;
    foreach (i; 0 .. nthreads)
    {
        jobs[i] = new Job(&ctxs[i]);
        threads[i] = new Thread(&jobs[i].run);
        threads[i].start();
    }
    foreach (th; threads) th.join();
    immutable t1 = MonoTime.currTime;
    GC.enable();

    t.secs = (t1 - t0).total!"nsecs" / 1e9;
    ulong produced, stalls, foreign;
    foreach (i; 0 .. nthreads) { produced += ctxs[i].produced; stalls += ctxs[i].stalls; foreign += ctxs[i].foreign; }
    immutable consumed = atomicLoad(g_consumed);
    free(ctxs);
    final switch (cfg.impl)
    {
    case Impl.vyukov: r32.destroy(); break;
    case Impl.ring: ring.destroy(); break;
    case Impl.farm: farm.destroy(); break;
    }
    t.foreign = foreign;

    if (atomicLoad(timedOut)) { t.err = "TIMEOUT"; return t; }
    if (cfg.mode == Mode.latency)
    {
        if (produced != cfg.n) { t.err = "latency samples short"; return t; }
        // Convert TSC ticks to ns with a short calibration.
        immutable c0 = llvm_readcyclecounter();
        immutable m0 = MonoTime.currTime;
        while ((MonoTime.currTime - m0) < 200.msecs) {}
        immutable c1 = llvm_readcyclecounter();
        immutable m1 = MonoTime.currTime;
        immutable ticksPerNs = cast(double)(c1 - c0) / (m1 - m0).total!"nsecs";
        qsort(g_samples, cfg.n, ulong.sizeof, &cmpUlong);
        double at(double q) { return g_samples[cast(size_t)(q * (cfg.n - 1))] / ticksPerNs; }
        t.p50 = at(0.50); t.p90 = at(0.90); t.p99 = at(0.99); t.max = g_samples[cfg.n - 1] / ticksPerNs;
        free(g_samples); g_samples = null;
    }
    else if (produced != cfg.n || consumed != cast(long) cfg.n)
    {
        t.err = "lost work";
        return t;
    }
    t.stalls = stalls;
    t.mops = (cfg.n / t.secs) / 1e6;
    t.ok = true;
    return t;
}

extern(C) int cmpUlong(scope const void* a, scope const void* b) nothrow @nogc
{
    immutable x = *cast(const ulong*) a, y = *cast(const ulong*) b;
    return x < y ? -1 : (x > y ? 1 : 0);
}

Trial runMedian(ref Cfg cfg)
{
    Trial[8] rs;
    uint n;
    foreach (r; 0 .. cfg.repeats)
    {
        rs[n++] = runOnce(cfg);
        if (!rs[n - 1].ok) return rs[n - 1];
    }
    // Median by throughput, or by p50 for latency.
    foreach (i; 0 .. n)
        foreach (j; i + 1 .. n)
        {
            immutable ki = cfg.mode == Mode.latency ? rs[i].p50 : rs[i].mops;
            immutable kj = cfg.mode == Mode.latency ? rs[j].p50 : rs[j].mops;
            if (kj < ki) { auto tmp = rs[i]; rs[i] = rs[j]; rs[j] = tmp; }
        }
    return rs[n / 2];
}

const(char)* implName(Impl i) { final switch (i) { case Impl.vyukov: return "vyukov32"; case Impl.ring: return "ring"; case Impl.farm: return "farm"; } }
const(char)* modeName(Mode m) { final switch (m) { case Mode.same: return "same"; case Mode.dual: return "dual"; case Mode.spmc: return "spmc"; case Mode.mpsc: return "mpsc"; case Mode.latency: return "latency"; } }

void printHeader()
{
    printf("%-9s %-8s %3s %5s %-4s %7s %10s %7s %9s %8s %10s %10s  %8s %8s %8s %9s\n",
        "impl".ptr, "mode".ptr, "thr".ptr, "rings".ptr, "pol".ptr, "cap/qs".ptr, "N".ptr, "sec".ptr, "Mops".ptr, "ns/op".ptr,
        "stalls".ptr, "foreign".ptr, "p50".ptr, "p90".ptr, "p99".ptr, "max".ptr);
}

uint threadCount(ref const Cfg c)
{
    final switch (c.mode)
    {
    case Mode.same: return 1;
    case Mode.dual: return c.threads;
    case Mode.spmc: return c.threads + 1;
    case Mode.mpsc: return c.threads + 1;
    case Mode.latency: return 2;
    }
}

void printRow(ref Cfg c, ref Trial t)
{
    immutable sz = c.impl == Impl.farm ? c.qs : c.cap;
    immutable thr = threadCount(c);
    immutable rings = c.impl == Impl.farm ? 1 : c.rings;
    const(char)* pol = c.impl == Impl.farm ? "-".ptr
        : c.mode == Mode.latency ? "tgt".ptr
        : c.policy == Policy.rr ? "rr".ptr : c.policy == Policy.local ? "loc".ptr : "own".ptr;
    if (!t.ok)
    {
        printf("%-9s %-8s %3u %5u %-4s %7llu %10llu  %.*s\n", implName(c.impl), modeName(c.mode), thr, rings, pol, sz, c.n,
            cast(int) t.err.length, t.err.ptr);
        return;
    }
    if (c.mode == Mode.latency)
        printf("%-9s %-8s %3u %5u %-4s %7llu %10llu %7.3f %9s %8s %10llu %10u  %8.1f %8.1f %8.1f %9.1f\n",
            implName(c.impl), modeName(c.mode), thr, rings, pol, sz, c.n, t.secs, "-".ptr, "-".ptr, t.stalls,
            c.targetRing, t.p50, t.p90, t.p99, t.max);
    else
        printf("%-9s %-8s %3u %5u %-4s %7llu %10llu %7.3f %9.3f %8.1f %10llu %10llu\n",
            implName(c.impl), modeName(c.mode), thr, rings, pol, sz, c.n, t.secs, t.mops, 1e3 / t.mops, t.stalls, t.foreign);
}

Cfg parseArgs(string[] args)
{
    import std.string : toStringz;
    import std.algorithm : splitter;
    import std.conv : to;

    Cfg c;
    foreach (i; 0 .. 16) c.cores[i] = cast(uint) i;
    c.ncores = 16;
    for (size_t i = 1; i < args.length; ++i)
    {
        auto a = args[i];
        if (i + 1 >= args.length) break;
        auto v = args[++i];
        auto numeric = toStringz(v);
        if (a == "--impl")
            c.impl = v == "vyukov" ? Impl.vyukov : v == "ring" ? Impl.ring : Impl.farm;
        else if (a == "--mode")
            c.mode = v == "same" ? Mode.same : v == "dual" ? Mode.dual : v == "spmc" ? Mode.spmc
                : v == "mpsc" ? Mode.mpsc : Mode.latency;
        else if (a == "--threads") c.threads = cast(uint) atoi(numeric);
        else if (a == "--n") { c.n = strtoull(numeric, null, 0); c.nSet = true; }
        else if (a == "--cap") c.cap = strtoull(numeric, null, 0);
        else if (a == "--rings") c.rings = cast(uint) atoi(numeric);
        else if (a == "--policy") c.policy = v == "rr" ? Policy.rr : v == "local" ? Policy.local : Policy.own;
        else if (a == "--ccd-cores") c.ccdCores = cast(uint) atoi(numeric);
        else if (a == "--target-ring") c.targetRing = cast(uint) atoi(numeric);
        else if (a == "--qs") c.qs = strtoull(numeric, null, 0);
        else if (a == "--repeats") c.repeats = cast(uint) atoi(numeric);
        else if (a == "--cores")
        {
            c.ncores = 0;
            foreach (part; v.splitter(','))
                if (c.ncores < c.cores.length) c.cores[c.ncores++] = part.to!uint;
        }
        else --i;
    }
    if (c.repeats == 0 || c.repeats > 8) c.repeats = 3;
    if (c.mode == Mode.latency && !c.nSet) c.n = 200_000;
    return c;
}

void printLayout()
{
    Vyukov!false v;
    printf("vyukov32: slot %zu bytes, enqPos line offset %zu, deqPos line offset %zu, cursor distance %zu\n",
        Vyukov!false.Slot.sizeof, cast(size_t) &v.enqPos & 63, cast(size_t) &v.deqPos & 63,
        cast(size_t)(&v.deqPos) - cast(size_t)(&v.enqPos));
    printf("ring: SlotRing!RingElement, %zu-byte element in a 64-byte slot; its cursor padding is "
        ~ "checked by antfarm/ring.d's unittest\n", RingElement.sizeof);
}

int main(string[] args)
{
    if (args.length == 2 && args[1] == "--layout") { printLayout(); return 0; }
    auto c = parseArgs(args);
    printHeader();
    auto t = runMedian(c);
    printRow(c, t);
    return t.ok ? 0 : 1;
}
