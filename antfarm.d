/++
 + Ant Farm: an M:N concurrent queue with superlative scaling.
 +
 + Implementation of the spec in this directory ("Ant Farm: An M:N Concurrent
 + Queue with Superlative Scaling"). A circular buffer using the "magic
 + buffer" memory mapping, K segments with root/leaf reference tallies,
 + quota-bounded producers and sharded consumers. No CAS operations are used;
 + all synchronization is fetch-add / fetch-sub plus acquire/release
 + load/store.
 +
 + Errors are fatal (process abort) rather than exceptional, per spec.
 + Interfaces are @nogc nothrow @system.
 +
 + Port note: the magic buffer is implemented for Posix via shm_open +
 + mmap MAP_FIXED of the same file twice (Linux: could equally use
 + memfd_create). Windows is not wired up in this port.
 +/
module antfarm;

import core.atomic;
// The stdc fetch-and/fetch-or aliases are not emitted under DMD
// (version(DigitalMars) blocks them off); fall back to core.atomic there.
version (DigitalMars) {}
else import core.stdc.stdatomic : atomic_fetch_and_explicit, atomic_fetch_or_explicit, memory_order;
import core.stdc.stdio : fprintf, stderr, snprintf;
import core.stdc.stdlib : abort, aligned_alloc, free;

version (Posix)
{
    import core.sys.posix.sys.mman;
    import core.sys.posix.sys.stat;
    import core.sys.posix.sys.types;
    import core.sys.posix.fcntl;
    import core.sys.posix.unistd;
}
else
    static assert(false, "antfarm: magic buffer mapping only implemented for Posix in this port");

/// Spec 2a/5a: maximum number of simultaneous subscribed consumers.
enum MAX_CONSUMERS_LIMIT = 128;
/// Spec 2a: preallocated leaf tallies per segment: ceiling square root of 128.
enum MAX_LEAVES = 12;
/// Spec 5e-d.
enum SMALL_TABLE_THRESHOLD = 64;
/// Spec 5e-g.
enum CLAIM_CHUNK = 16;
/// Spec 5e-g.
enum BIG_CHUNK = 64;
/// Maximum supported number of segments K (spec suggests 4 or 8).
enum KMAX = 16;

/// Spec 5a: a subscriber reference lives in the most significant half of Rt.
enum ulong SUB = 1UL << 32;
/// Spec 5a-c: Sub0 is a special bit at 2^16 in the least significant half.
enum ulong SUB0 = 1UL << 16;
/// Mask for Rt' (least significant half of Rt).
enum ulong LOWMASK = 0xFFFF_FFFFUL;
/// Mask for the active-consumer count bits within Rt' (below the Sub0 bit).
enum ulong COUNTMASK = SUB0 - 1;

private enum ulong SENTINEL_XOR = 0x9E37_79B9_7F4A_7C15UL;

/// Sentinel stored (release) as the first word of a table; computed from the
/// 64-bit sequence pointing at the start of the table (spec 5c/Thead).
ulong sentinelOf(ulong seq) pure nothrow @nogc @safe { return seq ^ SENTINEL_XOR; }

/// Atomically set bits (used for the Sub0 deposit): idempotent and
/// independent of the consumer count in the low half of Rt.
private void rtSetBits(ref shared ulong v, ulong bits) nothrow @nogc @system
{
    version (DigitalMars)
        atomicOp!"|="(v, bits);
    else
        atomic_fetch_or_explicit(&v, bits, memory_order.memory_order_seq_cst);
}

/// Atomically clear bits (used to clear Sub0): cannot corrupt the consumer
/// count even if the bit is already clear.
private void rtClearBits(ref shared ulong v, ulong bits) nothrow @nogc @system
{
    version (DigitalMars)
        atomicOp!"&="(v, ~bits);
    else
        atomic_fetch_and_explicit(&v, ~bits, memory_order.memory_order_seq_cst);
}

/// Spec 2a: square root with a minimum value of 1 for Cs <= 4, capped at the
/// preallocated leaf count. ceil_sqrt(128) == 12.
uint sqcsOf(ulong cs) pure nothrow @nogc @safe
{
    if (cs <= 4) return 1;
    ulong r = 2;
    while (r * r < cs) ++r;
    return r > MAX_LEAVES ? MAX_LEAVES : cast(uint) r;
}

/// Fatal error: print and abort. Spec 1: "most errors are fatal".
void fatal(const(char)[] msg) nothrow @nogc @system
{
    fprintf(stderr, "antfarm fatal: %.*s\n", cast(int) msg.length, msg.ptr);
    abort();
}

/// Spec 4a: type-erased const parameters for Call's work.
alias PayloadBody = const(ulong)[];

/// Spec 5f: decodes Pbody and executes an iteration of work.
alias Callback = long function(PayloadHeader* head, PayloadBody body, ulong iteration) nothrow @nogc @system;

/// Spec 5f: payload header, 16 ulongs (128 bytes) as laid out in the buffer.
struct PayloadHeader
{
    uint maxCs;             /// maximum consumers per payload; 1 = single threaded
    uint done;              /// iterations to complete; 1 = single threaded
    ulong plen;             /// payload body length in ulongs
    Callback call;          /// work callback
    ulong[6] filler;
    shared ulong pcount;    /// 32 MSB claims | 16 bits calls | 16 LSB completions
    ulong[7] filler2;
}
static assert(PayloadHeader.sizeof == 136);
/// PayloadHeader length in ulongs.
private enum ulong PHEAD_LEN = PayloadHeader.sizeof / 8;

/// Spec 4a.
struct PayloadEntry
{
    PayloadHeader* header;
    PayloadBody body;
}

/// Spec 3b: producer tiers.
enum Tier : ubyte { small, bulk }

/// Per-segment statistics (spec 2b). Padded to a cache line.
struct SegStats
{
    shared long es;     /// epoch currently occupying this segment; -1 = never written
    shared ulong seqt;  /// sequence of the first valid payload table for that epoch
    shared ulong cs;    /// snapshot of the number of consumers
    shared ulong sqcs;  /// sqcsOf(cs)
    shared ulong sd;    /// consumed-size accumulator: sum of completed table sizes
    ulong[3] pad;
}
static assert(SegStats.sizeof == 64);

/// Table layout offsets (in ulongs, relative to the table's starting
/// sequence), spec 4a:
///   [0..8)                 Thead: 0 Tsent, 1 Tnext, 2 Tmt<<32|Tlen, 3 Cs, 4 SqCs, 5..7 spare
///   [8 .. 8+Tlen+Tmt)      Tindex (total index first, MT index second)
///   7 ulongs padding
///   Tprogress (1 ulong)
///   7 ulongs padding
///   Tcount: SqCs counters, each followed by 7 ulongs padding
///   Payloads (each: 16-ulong header + Plen-ulong body)
///   7 ulongs padding
private enum ulong THEAD_LEN = 8;
private enum ulong PROG_PAD = 7;
private enum ulong END_PAD = 7;

struct AntFarm
{
    // ---- immutable configuration (spec 1, 3) ----
    ulong Ln;           /// buffer length in ulongs (power of 2)
    ulong Lmask;        /// Ln - 1
    uint K;             /// number of segments (power of 2)
    ulong kMask;        /// K - 1
    uint segShift;      /// log2(Ln / K)
    ulong segCap;       /// Ln / K segment capacity in ulongs
    uint expectedConsumers;
    uint maxBulk;
    ulong quotaBulk;    /// Exi for bulk producers
    uint maxSmall;
    ulong quotaSmall;   /// Exi for small producers
    ulong exmax;        /// Exmax: sum of all producer quotas (spec 3a)

    // ---- magic buffer ----
    shared(ulong)* buf; /// 2*Ln ulongs mapped; second half mirrors the first
    ulong bufBytes;     /// Ln * 8
    int shmFd;

    // ---- farm-level mutable metadata (spec 2c), one cache line each ----
    align(64) shared ulong Wt;      /// write tail sequence
    align(64) shared long Eg;       /// current global epoch
    align(64) shared long Cf;       /// current number of subscribed consumers
    align(64) shared ulong Reqs;    /// monotonically increasing subscription counter
    align(64) shared long Prbulk;   /// current number of bulk producers
    align(64) shared long Prsm;     /// current number of small producers

    // ---- segment metadata (spec 2a/2b) ----
    align(64) shared ulong[8][KMAX] Rt;              /// root tallies, one per segment
    align(64) shared long[8][KMAX * MAX_LEAVES] Lt;  /// leaf tallies
    align(64) SegStats[KMAX] stats;                  /// segment statistics

    // ------------------------------------------------------------------
    // Construction / destruction
    // ------------------------------------------------------------------

    static AntFarm* create(ulong ln = 1 << 20, uint k = 8, uint expectedConsumers = 4,
                           uint maxBulk = 2, ulong quotaBulk = 0,
                           uint maxSmall = 16, ulong quotaSmall = 4096) nothrow @nogc @system
    {
        if (k < 2 || k > KMAX || (k & (k - 1)) != 0) fatal("K must be a power of 2 in [2, KMAX]");
        if (ln < (1 << 14) || (ln & (ln - 1)) != 0) fatal("Ln must be a power of 2 >= 2^14");
        immutable segCap = ln / k;
        if (segCap < 2048) fatal("segment capacity too small");
        if (expectedConsumers == 0) fatal("expected consumers must be > 0");
        if (quotaBulk == 0) quotaBulk = segCap;
        if (quotaSmall == 0) fatal("small quota must be > 0");
        if (maxBulk + maxSmall == 0) fatal("no producer capacity configured");
        immutable exmax = cast(ulong) maxBulk * quotaBulk + cast(ulong) maxSmall * quotaSmall;
        if (quotaBulk > exmax || quotaSmall > exmax) fatal("quota exceeds Exmax");
        if (exmax > (k - 1) * segCap) fatal("Exmax exceeds K-1 segments' capacities");

        // Magic buffer: reserve 2*Ln, map the same file into both halves.
        immutable bytes = segCap * k * 8;
        static __gshared int shmCounter;
        char[64] name;
        snprintf(name.ptr, name.length, "/antfarm-%d-%d", cast(int) getpid(),
                 atomicFetchAdd(shmCounter, 1));
        int fd = shm_open(name.ptr, O_RDWR | O_CREAT | O_EXCL, 384); // 0600
        if (fd < 0) fatal("shm_open failed");
        shm_unlink(name.ptr);
        if (ftruncate(fd, cast(off_t) bytes) != 0) fatal("ftruncate failed");
        void* p = mmap(null, 2 * bytes, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
        if (p == MAP_FAILED) fatal("mmap reserve failed");
        if (mmap(p, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED)
            fatal("mmap first half failed");
        if (mmap(cast(char*) p + bytes, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0)
                == MAP_FAILED)
            fatal("mmap second half failed");

        auto mem = aligned_alloc(64, (AntFarm.sizeof + 63) & ~cast(size_t) 63);
        if (mem is null) fatal("alloc failed");
        auto f = cast(AntFarm*) mem;
        *f = AntFarm.init;
        f.Ln = ln;
        f.Lmask = ln - 1;
        f.K = k;
        f.kMask = k - 1;
        uint sh = 0;
        while ((1UL << sh) < segCap) ++sh;
        f.segShift = sh;
        f.segCap = segCap;
        f.expectedConsumers = expectedConsumers;
        f.maxBulk = maxBulk;
        f.quotaBulk = quotaBulk;
        f.maxSmall = maxSmall;
        f.quotaSmall = quotaSmall;
        f.exmax = exmax;
        f.buf = cast(shared(ulong)*) p;
        f.bufBytes = bytes;
        f.shmFd = fd;

        foreach (i; 0 .. KMAX)
            atomicStore!(MemoryOrder.raw)(f.stats[i].es, -1L);
        // Spec 2d: epoch 0's Rt is constructed with a dummy Sub0 value,
        // blocking producers from overwriting Seq 0 until consumers digest it.
        atomicStore!(MemoryOrder.raw)(f.Rt[0][0], SUB0);
        atomicStore!(MemoryOrder.raw)(f.stats[0].es, 0L);
        atomicStore!(MemoryOrder.raw)(f.stats[0].seqt, 0UL);
        atomicStore!(MemoryOrder.raw)(f.stats[0].cs, cast(ulong) expectedConsumers);
        atomicStore!(MemoryOrder.raw)(f.stats[0].sqcs, cast(ulong) sqcsOf(expectedConsumers));
        return f;
    }

    void destroy() nothrow @nogc @system
    {
        if (buf !is null)
            munmap(cast(void*) buf, 2 * bufBytes);
        if (shmFd >= 0)
            close(shmFd);
        free(cast(void*) &this);
    }

    // ------------------------------------------------------------------
    // Registration (spec 5a-a, 5b-a, 3b)
    // ------------------------------------------------------------------

    /// fetch-inc Cf; on success fetch-inc Reqs and return it (the IDc).
    /// Returns a negative value if oversubscribed.
    long add_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchAdd(Cf, 1);
        if (prev >= MAX_CONSUMERS_LIMIT)
        {
            atomicFetchSub(Cf, 1);
            return -1;
        }
        return cast(long) atomicFetchAdd(Reqs, 1);
    }

    /// Returns the previous Cf; when it returns 1 the caller is the last
    /// unsubscriber (spec 5b-a).
    long sub_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchSub(Cf, 1);
        if (prev <= 0) fatal("Cf underflow");
        return prev;
    }

    /// Spec 3b: returns negative if producers overregister.
    long registerProducer(Tier t) nothrow @nogc @system
    {
        if (t == Tier.bulk)
        {
            immutable p = atomicFetchAdd(Prbulk, 1);
            if (p >= maxBulk) { atomicFetchSub(Prbulk, 1); return -1; }
            return p;
        }
        else
        {
            immutable p = atomicFetchAdd(Prsm, 1);
            if (p >= maxSmall) { atomicFetchSub(Prsm, 1); return -1; }
            return p;
        }
    }

    void unregisterProducer(Tier t) nothrow @nogc @system
    {
        if (t == Tier.bulk)
        {
            if (atomicFetchSub(Prbulk, 1) <= 0) fatal("Prbulk underflow");
        }
        else
        {
            if (atomicFetchSub(Prsm, 1) <= 0) fatal("Prsm underflow");
        }
    }

    // ------------------------------------------------------------------
    // Production (spec 3, 4)
    // ------------------------------------------------------------------

    /// Spec 3a: reanchor on Wt and sweep contiguously-forward zero Rts.
    /// Quota is renewed only if at least Exmax of free space is verified.
    private bool refreshQuota(ref ulong exi, ulong quota) nothrow @nogc @system
    {
        immutable anchor = atomicOp!"+="(Wt, 0UL);
        immutable ea = anchor >> segShift;
        ulong freeSp = 0;
        foreach (j; 1 .. K)
        {
            immutable ki = (ea + j) & kMask;
            if (atomicLoad!(MemoryOrder.acq)(Rt[ki][0]) == 0)
                freeSp += segCap;
            else
                break;
            if (freeSp >= exmax)
                break;
        }
        if (freeSp >= exmax)
        {
            exi = quota;
            return true;
        }
        return false;
    }

    /// Spec 4. Writes as many of `payloads` as fit the caller's quota `exi`;
    /// returns the number written. Returns 0 (full) when backpressure is too
    /// high; callers retry or wait on consumers.
    ulong write(scope PayloadEntry[] payloads, ref ulong exi, Tier tier = Tier.small)
        nothrow @nogc @system
    {
        if (payloads.length == 0) return 0;
        immutable quota = tier == Tier.bulk ? quotaBulk : quotaSmall;
        immutable csl = atomicLoad!(MemoryOrder.acq)(Cf);
        immutable cs = csl > 0 ? cast(uint) csl : 1;
        immutable sq = sqcsOf(cs);

        // Fit as many payloads as the quota allows (spec 4a truncation).
        size_t n;
        uint m;
        ulong psum;
        for (;;)
        {
            n = 0; m = 0; psum = 0;
            foreach (i, ref pe; payloads)
            {
                if (pe.header is null || pe.header.call is null) fatal("bad payload header");
                if (pe.header.maxCs == 0) fatal("payload MaxCs == 0");
                if (pe.header.done == 0 || pe.header.done > 0xFFFF) fatal("payload Done out of range");
                if (pe.header.done > 1 && pe.header.maxCs < 2)
                    fatal("multithreaded payload requires MaxCs >= 2");
                immutable psz = PHEAD_LEN + pe.body.length;
                immutable cm = m + (pe.header.maxCs > 1 ? 1 : 0);
                immutable cand = THEAD_LEN + (i + 1 + cm) + PROG_PAD + 1 + 7 + 8 * sq + psum + psz + END_PAD;
                if (cand > exi) break;
                n = i + 1;
                m = cast(uint) cm;
                psum += psz;
            }
            if (n > 0) break;
            if (!refreshQuota(exi, quota)) return 0;
        }

        immutable size = THEAD_LEN + (n + m) + PROG_PAD + 1 + 7 + 8 * sq + psum + END_PAD;

        // Reserve space on the write tail (spec 3a, 4b).
        immutable wret = atomicOp!"+="(Wt, size) - size;
        immutable wtprime = wret + size;
        exi -= size;

        // Opportunistic quota renewal (spec 3a/4b): if the distance from
        // Wt' to the end of its segment is at least Exmax, all outstanding
        // blind writes provably land within the current write segment, so
        // the quota may be renewed without touching Rt. (Inert when
        // Exmax > segCap; renewal then always goes through the sweep.)
        immutable seqb = ((wtprime >> segShift) + 1) << segShift;
        if (seqb - wtprime >= exmax)
            exi = quota;

        // Segment/epoch transitions (spec 4b): initialize metadata for each
        // crossed segment, release-storing Es last; advance Eg by the delta.
        immutable eold = wret >> segShift;
        immutable enew = wtprime >> segShift;
        foreach (e; eold + 1 .. enew + 1)
        {
            immutable ki = e & kMask;
            atomicStore!(MemoryOrder.raw)(stats[ki].seqt, wtprime);
            atomicStore!(MemoryOrder.raw)(stats[ki].cs, cast(ulong) cs);
            atomicStore!(MemoryOrder.raw)(stats[ki].sqcs, cast(ulong) sq);
            atomicStore!(MemoryOrder.raw)(stats[ki].sd, 0UL); // fresh epoch: nothing consumed
            atomicStore!(MemoryOrder.rel)(stats[ki].es, cast(long) e);
        }
        if (enew > eold)
            atomicOp!"+="(Eg, cast(long)(enew - eold));

        // Write the table (spec 4a/4b). The magic buffer makes the linear
        // address range contiguous even across the Ln wrap point.
        auto w = cast(ulong*) buf + (wret & Lmask);
        immutable progOff = THEAD_LEN + n + m + PROG_PAD;
        immutable tcountOff = progOff + 8;
        immutable payOff = tcountOff + 8 * sq;

        w[1] = wtprime;                          // Tnext
        w[2] = (cast(ulong) m << 32) | cast(uint) n;  // Tmt | Tlen
        w[3] = cs;
        w[4] = sq;
        w[5] = size; // table size, accounted into the segment's Sd on completion
        w[6] = 0; w[7] = 0;

        // Tindex: total index first, MT index second.
        ulong po = payOff;
        foreach (i; 0 .. n)
        {
            w[THEAD_LEN + i] = po;
            po += PHEAD_LEN + payloads[i].body.length;
        }
        po = payOff;
        size_t mi = THEAD_LEN + n;
        foreach (i; 0 .. n)
        {
            immutable o = po;
            po += PHEAD_LEN + payloads[i].body.length;
            if (payloads[i].header.maxCs > 1)
                w[mi++] = o;
        }

        w[progOff] = 0;                          // Tprogress
        w[tcountOff .. tcountOff + 8 * sq] = 0;  // Tcount shards

        // Payloads.
        po = payOff;
        foreach (i; 0 .. n)
        {
            auto src = &payloads[i];
            auto h = cast(PayloadHeader*)(w + po);
            h.maxCs = src.header.maxCs;
            h.done = src.header.done;
            h.plen = src.body.length;
            h.call = src.header.call;
            h.filler[] = 0;
            h.filler2[] = 0;
            atomicStore!(MemoryOrder.raw)(h.pcount, 0UL);
            (w + po + PHEAD_LEN)[0 .. src.body.length] = src.body[];
            po += PHEAD_LEN + src.body.length;
        }
        w[size - END_PAD .. size] = 0;

        // The sentinel is store-released as the last value written, so that
        // consumers validating the expected sequence also validate the table
        // contents (spec 4b).
        atomicStore!(MemoryOrder.rel)(buf[wret & Lmask], sentinelOf(wret));
        return n;
    }
}

/// Spec 5a: a POD struct exclusively owned by one consumer at a time.
///
/// Generation 2 reference model: the consumer holds a *position* reference
/// on the segment of the last table it validated, plus a local array of
/// *trailing* references on segments it has passed through but not yet
/// confirmed complete. Confirmation (5j): the next segment has been
/// initialized for epoch Ei+1 (so no more tables can start in Ki) and
/// Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1], where Sd accumulates completed table
/// sizes. The target is exact because tables are contiguous: the sizes of
/// all tables starting in Ki sum to precisely Seqt[Ki+1] - Seqt[Ki], so a
/// large table completing ahead of a small one cannot force the crossing.
///
/// Invariants:
///  - Every consumer holds a reference on every segment it has entered but
///    not yet confirmed complete. Hence an incomplete segment is protected
///    from producer overwrite by every passerby still subscribed.
///  - The last unsubscriber deposits Sub0 at its earliest unconfirmed held
///    segment, preserving the backlog for the next subscriber.
///  - If producers stall on a held trailing reference, consumers catch up
///    to Wt, go idle, and re-walk the oldest held segment claiming
///    leftovers until it confirms: blocking creates the idleness that
///    resolves it.
struct ConsumerView
{
    AntFarm* F;
    long IDc = -1;      /// taken from F's Reqs during subscription
    ulong nextSeq;      /// sequence of the next table to read
    uint curKi;         /// position reference: segment of last validated table
    ulong curEi;        /// ... its epoch
    uint curLti;        /// ... leaf tally index actually used
    bool hasRef;

    /// Trailing references: segments entered but not yet confirmed complete.
    /// Pushed in increasing epoch order; bounded by K because a held
    /// trailing reference blocks producers from lapping it.
    uint[KMAX] trailKi;
    ulong[KMAX] trailEi;
    uint[KMAX] trailLti;
    uint trailN;

    /// Carried sweeper role: set when this consumer completed a shard of the
    /// previous table. Small tables concentrate all claims on shard 0
    /// (5e-d), so under churn they may have no native consumer at all; a
    /// carried sweeper sweeps a small next table regardless of its own
    /// shard assignment.
    bool sweeperNext;

    /// Spec 5a. Returns the (non-negative) starting epoch on success, or a
    /// negative value on failure.
    long subscribe(AntFarm* f) nothrow @nogc @system
    {
        if (f is null || hasRef) return -1;
        immutable idc = f.add_consumer();
        if (idc < 0) return idc;

        // 5a-b: walk backwards from Eg through the segments to the earliest
        // one whose Rt' is nonzero (the "pulse"). The pulse invariant
        // guarantees one exists; the only way to observe none is a race with
        // a not-last unsubscribe, in which case we stand at the frontier.
        immutable eg = atomicLoad!(MemoryOrder.acq)(f.Eg);
        long bestE = long.max;
        long bestSeg = -1;
        foreach (ulong d; 0 .. f.K)
        {
            if (d > eg) break;
            immutable e = eg - d;
            immutable ki = cast(uint)(e & f.kMask);
            immutable rt = atomicLoad!(MemoryOrder.acq)(f.Rt[ki][0]);
            if ((rt & LOWMASK) != 0)
            {
                immutable es = atomicLoad!(MemoryOrder.raw)(f.stats[ki].es);
                if (es >= 0 && es < bestE)
                {
                    bestE = es;
                    bestSeg = ki;
                }
            }
        }
        immutable seg = bestSeg >= 0 ? cast(uint) bestSeg : cast(uint)(eg & f.kMask);

        // 5a-c: deposit a Sub in the most significant half. The held Sub
        // blocks producers from lapping this segment while we establish the
        // real consumer reference, so there is no zero-gap window regardless
        // of what the low half did in the interim.
        immutable old = atomicFetchAdd(f.Rt[seg][0], SUB);
        immutable low = old & LOWMASK;
        immutable high = old >> 32;
        // 5a-c: first subscriber designation.
        immutable clearer = low >= SUB0 && high == 0;

        // 5a-e: normal leaf tally increment with root propagation.
        immutable es = atomicLoad!(MemoryOrder.acq)(f.stats[seg].es);
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[seg].sqcs);
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[seg].seqt);
        immutable lti = cast(uint)(idc % sq);
        immutable lo = atomicFetchAdd(f.Lt[seg * MAX_LEAVES + lti][0], 1);
        if (lo == 0)
            atomicFetchAdd(f.Rt[seg][0], 1);

        // 5a-f: the designated first subscriber clears Sub0. Ordering
        // (establish leaf, then clear Sub0, then remove Sub) keeps Rt
        // continuously nonzero and makes double-clearing impossible; the
        // clear is a bit-and so it cannot corrupt the consumer count.
        if (clearer)
            rtClearBits(f.Rt[seg][0], SUB0);
        // 5a-g: remove our Sub.
        atomicFetchSub(f.Rt[seg][0], SUB);

        F = f;
        IDc = idc;
        nextSeq = seqt;
        curKi = seg;
        curEi = cast(ulong) es;
        curLti = lti;
        trailN = 0;
        sweeperNext = false;
        hasRef = true;
        return es;
    }

    /// Spec 5b. References on confirmed-complete segments are dropped first;
    /// the last unsubscriber plants Sub0 at its earliest unconfirmed held
    /// segment, preserving the backlog for the next subscriber.
    void unsubscribe() nothrow @nogc @system
    {
        if (!hasRef) return;
        auto f = F;
        tryReleaseTrailing();
        immutable ski = trailN ? trailKi[0] : curKi;
        // 5b-a: the last unsubscriber deposits Sub0 before decrementing,
        // preserving the always-a-live-pulse invariant. Deposit is a bit-or:
        // idempotent and independent of the consumer count.
        if (f.sub_consumer() == 1)
            rtSetBits(f.Rt[ski][0], SUB0);
        // 5b-b: normal decrement process for every held reference.
        decRef(curKi, curLti);
        foreach (i; 0 .. trailN)
            decRef(trailKi[i], trailLti[i]);
        trailN = 0;
        hasRef = false;
        IDc = -1;
    }

    /// Consume one table if available. Returns false when no valid table is
    /// published at the current position yet; on that idle path the consumer
    /// first mops up its oldest unconfirmed segment.
    bool consumeNext() nothrow @nogc @system
    {
        if (!hasRef) return false;
        auto f = F;
        auto bp = f.buf;
        immutable idx = nextSeq & f.Lmask;
        // 5c: load-acquire the sentinel and validate location and value.
        if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(nextSeq))
        {
            sweepOldestTrailing();
            return false;
        }

        // Breaching a segment boundary: take a reference on the newer
        // segment; the old position reference becomes a trailing one and is
        // released only when its segment is confirmed complete.
        immutable ei = nextSeq >> f.segShift;
        immutable ki = cast(uint)(ei & f.kMask);
        if (ki != curKi)
            moveRef(ki, ei);

        immutable tnext = atomicLoad!(MemoryOrder.raw)(bp[idx + 1]);
        immutable w2 = atomicLoad!(MemoryOrder.raw)(bp[idx + 2]);
        immutable tlen = cast(uint) w2;
        immutable tmt = cast(uint)(w2 >> 32);
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(bp[idx + 4]);
        immutable tindexOff = idx + THEAD_LEN;
        immutable progOff = tindexOff + tlen + tmt + PROG_PAD;
        immutable tcountOff = progOff + 8;

        if (tlen > 0)
        {
            immutable myShi = cast(uint)((cast(ulong) IDc + nextSeq) % sq);
            bool sweeper;
            if (atomicLoad!(MemoryOrder.raw)(bp[progOff]) != tlen)
            {
                // 5e: primary path. The consumer completing a shard gains
                // the sweeper role: one linear pass over the other shards,
                // then the MT items. It takes all shards' completers being
                // gone for work to starve, in which case no progress is
                // possible anyway.
                sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                        progOff, tlen, sq, myShi, false, false) & 1) != 0;
                // Small tables are claimed wholesale by shard 0 (5e-d), so
                // they starve whenever no active consumer maps there. A
                // consumer carrying the sweeper role from the previous
                // table sweeps a small table regardless of its own shard.
                if (!sweeper && sweeperNext && tlen < SMALL_TABLE_THRESHOLD && myShi != 0)
                    sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                            progOff, tlen, sq, 0, true, false) & 1) != 0;
                if (sweeper)
                {
                    immutable nshards = tlen < SMALL_TABLE_THRESHOLD ? 1 : sq;
                    bool adopted;
                    foreach (s; 0 .. nshards)
                        if (s != myShi)
                        {
                            immutable r = processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                                       progOff, tlen, sq, s, true, !adopted);
                            if (r & 2) adopted = true;
                        }
                    tertiaryMt(bp, idx, tindexOff, tlen, tmt);
                }
                else
                {
                    // 5g: secondary round-robin share of the MT payloads.
                    secondaryMt(bp, idx, tindexOff, tlen, tmt, sq);
                }
            }
            else
            {
                // Complete table: cheap pre-checked MT pass only (MT
                // payloads can outlive their shards' completion).
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
            sweeperNext = sweeper;
        }

        nextSeq = tnext;
        tryReleaseTrailing();
        return true;
    }

private:
    /// Decrement a held leaf tally with root propagation (spec 2a). Edge
    /// transition (dec returns 1) repeats on Rt; underflows are fatal.
    void decRef(uint ki, uint lti) nothrow @nogc @system
    {
        auto f = F;
        immutable lo = atomicFetchSub(f.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo <= 0) fatal("leaf tally underflow");
        if (lo == 1)
        {
            immutable ro = atomicFetchSub(f.Rt[ki][0], 1);
            if ((ro & COUNTMASK) == 0) fatal("root tally underflow");
        }
    }

    /// Take the position reference on segment ki and demote the old one to
    /// a trailing reference. Increment-ahead (spec 2a) leaves no zero-gap.
    void moveRef(uint ki, ulong ei) nothrow @nogc @system
    {
        auto f = F;
        atomicLoad!(MemoryOrder.acq)(f.stats[ki].es); // pair with producer's release
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[ki].sqcs);
        immutable lti = cast(uint)(IDc % sq);
        immutable lo = atomicFetchAdd(f.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo == 0)
            atomicFetchAdd(f.Rt[ki][0], 1);
        if (trailN == KMAX) fatal("trailing reference overflow");
        trailKi[trailN] = curKi;
        trailEi[trailN] = curEi;
        trailLti[trailN] = curLti;
        ++trailN;
        curKi = ki;
        curEi = ei;
        curLti = lti;
    }

    /// Confirmation predicate: segment Ki at epoch Ei is complete when the
    /// next segment has been initialized for Ei+1 -- so no more tables can
    /// start in Ki -- and the consumed-size accumulator covers the whole
    /// span up to the next segment's first table. The target is exact:
    /// tables are contiguous, so the sizes of all tables starting in Ki sum
    /// to Seqt[Ki+1] - Seqt[Ki], and Sd accrues each table's size exactly
    /// once, only on completion -- a large table completing ahead of a
    /// small one cannot force the crossing early. A segment completely
    /// spanned by one table gets Seqt at or past Seqt[Ki+1] and confirms
    /// trivially. While we hold Ki's reference, the producer cannot lap
    /// past it, so stats[Ki+1] stays at epoch Ei+1 once initialized.
    bool confirmed(uint ki, ulong ei) nothrow @nogc @system
    {
        auto f = F;
        immutable ei1 = ei + 1;
        immutable ki1 = cast(uint)(ei1 & f.kMask);
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki1].es) != cast(long) ei1)
            return false;
        immutable seqtNext = atomicLoad!(MemoryOrder.raw)(f.stats[ki1].seqt);
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
        immutable sd = atomicLoad!(MemoryOrder.raw)(f.stats[ki].sd);
        return seqt + sd >= seqtNext;
    }

    /// Release trailing references on segments confirmed complete.
    void tryReleaseTrailing() nothrow @nogc @system
    {
        uint w = 0;
        foreach (i; 0 .. trailN)
        {
            if (confirmed(trailKi[i], trailEi[i]))
                decRef(trailKi[i], trailLti[i]);
            else
            {
                trailKi[w] = trailKi[i];
                trailEi[w] = trailEi[i];
                trailLti[w] = trailLti[i];
                ++w;
            }
        }
        trailN = w;
    }

    /// Idle path: re-walk the oldest unconfirmed held segment claiming any
    /// leftover work, so the segment confirms and its reference can be
    /// released. The data is guaranteed present and intact: we hold a
    /// reference on the segment, so producers cannot have overwritten it.
    void sweepOldestTrailing() nothrow @nogc @system
    {
        if (trailN == 0) return;
        auto f = F;
        auto bp = f.buf;
        immutable ki = trailKi[0];
        immutable boundary = (trailEi[0] + 1) << f.segShift;
        if (confirmed(ki, trailEi[0]))
        {
            tryReleaseTrailing();
            return;
        }
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
        ulong seq = seqt;
        while (seq < boundary)
        {
            immutable idx = seq & f.Lmask;
            if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(seq))
                fatal("held segment's table was overwritten");
            immutable tnext = atomicLoad!(MemoryOrder.raw)(bp[idx + 1]);
            immutable w2 = atomicLoad!(MemoryOrder.raw)(bp[idx + 2]);
            immutable tlen = cast(uint) w2;
            immutable tmt = cast(uint)(w2 >> 32);
            immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(bp[idx + 4]);
            immutable tindexOff = idx + THEAD_LEN;
            immutable progOff = tindexOff + tlen + tmt + PROG_PAD;
            immutable tcountOff = progOff + 8;
            if (tlen > 0)
            {
                if (atomicLoad!(MemoryOrder.raw)(bp[progOff]) != tlen)
                {
                    immutable nshards = tlen < SMALL_TABLE_THRESHOLD ? 1 : sq;
                    foreach (s; 0 .. nshards)
                        processShard(bp, seq, idx, tindexOff, tcountOff,
                                     progOff, tlen, sq, s, true, false);
                }
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
            seq = tnext;
        }
        tryReleaseTrailing();
    }

    /// Migrate the position leaf tally to a new leaf index (spec 2a
    /// ordering: inc ahead, then dec).
    void migratePositionLeaf(uint nl) nothrow @nogc @system
    {
        if (nl == curLti) return;
        immutable lo = atomicFetchAdd(F.Lt[curKi * MAX_LEAVES + nl][0], 1);
        if (lo == 0)
            atomicFetchAdd(F.Rt[curKi][0], 1);
        decRef(curKi, curLti);
        curLti = nl;
    }

    /// Spec 5h: when the number of consumers passing through an exhausted
    /// shard significantly exceeds SqCs, nudge IDc by +1 and migrate the
    /// position leaf tally, landing in a different bucket on the next table.
    void feedback(ulong z, uint sq) nothrow @nogc @system
    {
        if (sq == 0) return;
        if (z > 2UL * sq && atomicLoad!(MemoryOrder.raw)(F.Cf) > sq)
        {
            IDc += 1;
            immutable sq2 = cast(uint) atomicLoad!(MemoryOrder.raw)(F.stats[curKi].sqcs);
            migratePositionLeaf(cast(uint)(IDc % sq2));
        }
    }

    /// Undersaturation repair, complementing 5h: a sweeper finding a
    /// foreign shard with Z <= 1 (essentially no native traffic) nudges its
    /// IDc by the round-robin distance into that shard, patching the hole a
    /// pre-empted or unsubscribed native left behind. If a laggard native
    /// later wakes and finds the shard overbalanced, the 5h oversaturation
    /// nudge moves it out again. Self-limiting: each entering sweeper's
    /// failed exit-claim raises the shard's Z, so the signal disappears
    /// after a couple of adopters.
    void adopt(uint shi, uint sq, ulong tseq) nothrow @nogc @system
    {
        immutable myShi = cast(uint)((cast(ulong) IDc + tseq) % sq);
        immutable delta = (shi + sq - myShi) % sq;
        if (delta == 0) return;
        IDc += delta;
        immutable sq2 = cast(uint) atomicLoad!(MemoryOrder.raw)(F.stats[curKi].sqcs);
        migratePositionLeaf(cast(uint)(IDc % sq2));
    }

    /// Spec 5f: enter a payload. Claims bounded by MaxCs; calls bounded by
    /// Done. With loopAll, keep executing iterations until the calls are
    /// exhausted (secondary/tertiary paths); otherwise execute at most one.
    void enterPayload(shared(ulong)* bp, ulong absIdx, bool loopAll) nothrow @nogc @system
    {
        auto head = cast(PayloadHeader*)(bp + absIdx);
        immutable c = atomicFetchAdd(head.pcount, 1UL << 32);
        if ((c >> 32) >= head.maxCs)
            return; // overallocated
        do
        {
            immutable d = atomicFetchAdd(head.pcount, 1UL << 16);
            immutable called = (d >> 16) & 0xFFFF;
            if (called >= head.done)
                break;
            auto body_ = (cast(const(ulong)*)(cast(ulong*) bp + absIdx + PHEAD_LEN))[0 .. head.plen];
            head.call(head, body_, called);
            atomicFetchAdd(head.pcount, 1UL);
        }
        while (loopAll);
    }

    /// Spec 5e: work claims on one shard, mapping it to a linear slice of
    /// Tindex claimed by chunks on the shard's Tcount counter. With
    /// checkFirst (sweeps of foreign shards and re-walks), a plain load
    /// short-circuits fully-claimed shards without an RMW.
    ///
    /// Returns a bitmask: bit 0 set when this consumer added the final
    /// completion sum for the shard (spec 5e-k): it adds Shlen to
    /// Tprogress, accounts the table's size into the segment's Sd if
    /// the table thereby completed, and gains the sweeper role. Bit 1 set
    /// when, sweeping a foreign shard, it found the shard starved
    /// (Z <= 1) and adopted it (only when allowAdopt).
    uint processShard(shared(ulong)* bp, ulong tseq, ulong tseqIdx, ulong tindexOff,
                      ulong tcountOff, ulong progOff, uint tlen, uint sq, uint shi,
                      bool checkFirst, bool allowAdopt) nothrow @nogc @system
    {
        uint shstart, shlen, shbase;
        // 5e-d: small tables are claimed wholesale by shard 0 only.
        if (tlen < SMALL_TABLE_THRESHOLD)
        {
            if (shi != 0) return 0;
            shstart = 0;
            shlen = tlen;
            shbase = tlen;
        }
        else
        {
            // 5e-e/f.
            shbase = tlen / sq;
            immutable shrm = tlen % sq;
            shlen = shi < shrm ? shbase + 1 : shbase;
            shstart = shi * shbase + (shi < shrm ? shi : shrm);
        }
        if (shlen == 0)
            return 0;
        // 5e-g/h.
        immutable chunk = shbase >= BIG_CHUNK * 16 ? BIG_CHUNK : CLAIM_CHUNK;
        immutable shiter = (shlen + chunk - 1) / chunk;
        auto shc = &bp[tcountOff + shi * 8];
        if (checkFirst)
        {
            immutable claims = cast(uint)(atomicLoad!(MemoryOrder.raw)(*shc) >> 32);
            if (claims >= shiter)
            {
                // Shard already exhausted; a starved one (Z <= 1: at most
                // one prior exhausted-visitor) may be adopted. Small tables
                // are excluded: their lone shard always reads starved.
                if (allowAdopt && tlen >= SMALL_TABLE_THRESHOLD && claims - shiter <= 1)
                {
                    adopt(shi, sq, tseq);
                    return 2;
                }
                return 0;
            }
        }
        for (;;)
        {
            // 5e-i.
            immutable x = cast(uint)(atomicFetchAdd(*shc, 1UL << 32) >> 32);
            if (x >= shiter)
            {
                immutable z = x - shiter;
                if (!checkFirst)
                    feedback(z, sq); // 5h: Z = X - Shiter
                else if (allowAdopt && tlen >= SMALL_TABLE_THRESHOLD && z <= 1)
                {
                    adopt(shi, sq, tseq);
                    return 2;
                }
                return 0;
            }
            // 5e-j.
            immutable runstart = shstart + x * chunk;
            immutable runlen = chunk < shlen - x * chunk ? chunk : shlen - x * chunk;
            foreach (i; 0 .. runlen)
            {
                immutable poff = atomicLoad!(MemoryOrder.raw)(bp[tindexOff + runstart + i]);
                enterPayload(bp, tseqIdx + poff, false);
            }
            // 5e-k: the consumer adding the final completion sum for the
            // shard increments Tprogress by the shard length, whichever
            // consumer (owner, sweeper, or re-walker) that happens to be.
            immutable y = atomicFetchAdd(*shc, cast(ulong) runlen);
            if ((y & 0xFFFF_FFFFUL) == shlen - runlen)
            {
                immutable tp = atomicFetchAdd(bp[progOff], cast(ulong) shlen);
                if (tp + shlen == tlen)
                {
                    // Table complete: account its size into its starting
                    // segment's consumed-size accumulator. We provably
                    // still hold a reference on that segment (it was
                    // unconfirmed until now), so no producer can be
                    // re-zeroing the accumulator.
                    immutable tsize = atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 5]);
                    immutable tki = cast(uint)((tseq >> F.segShift) & F.kMask);
                    atomicFetchAdd(F.stats[tki].sd, tsize);
                }
                return 1;
            }
        }
    }

    /// Spec 5g: secondary work claims. Round-robin assignment of the
    /// multithreaded payloads by shard index, iterating each payload's
    /// internal counter.
    void secondaryMt(shared(ulong)* bp, ulong tseqIdx, ulong tindexOff,
                     uint tlen, uint tmt, uint sq) nothrow @nogc @system
    {
        if (tmt == 0) return;
        immutable shi = cast(uint)((cast(ulong) IDc + nextSeq) % sq);
        immutable mtBase = tindexOff + tlen;
        for (ulong j = shi; j < tmt; j += sq)
        {
            immutable poff = atomicLoad!(MemoryOrder.raw)(bp[mtBase + j]);
            enterPayload(bp, tseqIdx + poff, true);
        }
    }

    /// Tertiary MT sweep: enter any MT payload that still has iterations
    /// and a free claim slot, so MT payloads complete even if their
    /// designated consumers unsubscribed or were pre-empted. A primary
    /// visit burns exactly one claim per payload, and write() enforces
    /// MaxCs >= 2 on MT payloads, so a sweeper always finds a slot.
    void tertiaryMt(shared(ulong)* bp, ulong tseqIdx, ulong tindexOff,
                    uint tlen, uint tmt) nothrow @nogc @system
    {
        if (tmt == 0) return;
        immutable mtBase = tindexOff + tlen;
        foreach (j; 0 .. tmt)
        {
            immutable poff = atomicLoad!(MemoryOrder.raw)(bp[mtBase + j]);
            auto head = cast(PayloadHeader*)(cast(ulong*) bp + tseqIdx + poff);
            immutable pc = atomicLoad!(MemoryOrder.raw)(head.pcount);
            if (((pc >> 16) & 0xFFFF) >= head.done) continue;
            if ((pc >> 32) >= head.maxCs) continue;
            enterPayload(bp, tseqIdx + poff, true);
        }
    }
}
