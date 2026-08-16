/++
 + Ant Farm: an M:N concurrent queue with superlative scaling.
 +
 + Implementation of the spec in this directory ("Ant Farm: An M:N Concurrent
 + Queue with Superlative Scaling"). A circular buffer using the "magic
 + buffer" memory mapping, K segments with root/leaf reference tallies,
 + quota-bounded producers and sharded consumers. No CAS on hot paths;
 + all hot-path synchronization is fetch-add / fetch-sub plus acquire/release
 + load/store. Registration claims a ticket slot with a CAS (cold path).
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
import core.stdc.stdio : fprintf, stderr, snprintf;
import core.stdc.stdlib : abort, aligned_alloc, free;
import core.stdc.string : memset;

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

/// Spec 3b: producer ticket slot stride in ulongs. Each slot's hash sits on
/// its own 64-byte cache line: tickets are shared mutable state under the
/// registration CAS, and the buffer cannot align them beyond 8 bytes, so
/// the stride (not the natural ulong size) must carry the isolation.
private enum size_t PROD_SLOT_STRIDE = 8;

/// Spec 5a: a subscriber reference lives in the most significant half of Rt.
enum ulong SUB = 1UL << 32;
/// Spec 2a/5b: Sub0 is a counted pin in units of 2^16, not a bit.
enum ulong SUB0 = 1UL << 16;
/// Mask for Rt' (least significant half of Rt).
enum ulong LOWMASK = 0xFFFF_FFFFUL;
/// Mask for the active-consumer count bits within Rt' (below the Sub0 bit).
enum ulong COUNTMASK = SUB0 - 1;
/// Spec 4a: Done and MaxCs are capped so packed 16-bit fields cannot wrap
/// through write().
enum uint MAX_PAYLOAD_ITERS = 512;

private enum ulong SENTINEL_XOR = 0x9E37_79B9_7F4A_7C15UL;

/// Sentinel stored (release) as the first word of a table; computed from the
/// 64-bit sequence pointing at the start of the table (spec 5c/Thead).
ulong sentinelOf(ulong seq) pure nothrow @nogc @safe { return seq ^ SENTINEL_XOR; }

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

/// Spec 3b/4a: registration ticket hashed from (slot, Reqs_p). Hash 0 is invalid.
struct Token
{
    Tier tier;
    uint slot;
    ulong hash;

    bool valid() const pure nothrow @nogc @safe { return hash != 0; }
}

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

/// Overflow-checked addition used by write() size math (spec 4a).
private ulong addChecked(ulong a, ulong b, const(char)[] msg) nothrow @nogc @system
{
    if (b > ulong.max - a) fatal(msg);
    return a + b;
}

/// Table size in ulongs: Thead + index + pads + Tprogress + Tcount + payloads + end pad.
private ulong tableSizeChecked(ulong n, ulong m, ulong sq, ulong psum) nothrow @nogc @system
{
    ulong s = THEAD_LEN;
    s = addChecked(s, n, "table size overflow");
    s = addChecked(s, m, "table size overflow");
    s = addChecked(s, PROG_PAD, "table size overflow");
    s = addChecked(s, 1, "table size overflow");
    s = addChecked(s, 7, "table size overflow");
    if (sq > ulong.max / 8) fatal("table size overflow");
    s = addChecked(s, 8 * sq, "table size overflow");
    s = addChecked(s, psum, "table size overflow");
    s = addChecked(s, END_PAD, "table size overflow");
    return s;
}

/// Hash of (slot, Reqs_p). Never zero: 0 is the invalid-slot sentinel.
private ulong mixToken(uint slot, ulong reqs) pure nothrow @nogc @safe
{
    ulong x = reqs ^ (0x9E3779B97F4A7C15UL + (cast(ulong) slot << 1));
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9UL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBUL;
    x ^= x >> 31;
    return x == 0 ? 1 : x;
}

/// Slot i's hash word within a producer ticket array. PROD_SLOT_STRIDE
/// ulongs (64 bytes) per slot, so a CAS on one hash never touches another
/// slot's cache line.
private shared(ulong)* prodSlot(shared(ulong)* hashes, size_t i) nothrow @nogc @system
{
    return hashes + i * PROD_SLOT_STRIDE;
}

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
    int shmFd = -1;

    // ---- farm-level mutable metadata (spec 2c), one cache line each ----
    align(64) shared ulong Wt;      /// write tail sequence
    align(64) shared long Eg;       /// current global epoch
    align(64) shared long Cf;       /// current number of subscribed consumers
    align(64) shared ulong Reqs_c;  /// consumer subscription counter; source of IDc
    align(64) shared ulong Reqs_p;  /// producer registration counter; Token hash input
    align(64) shared long Prbulk;   /// current number of bulk producers
    align(64) shared long Prsm;     /// current number of small producers

    // ---- producer tickets (spec 3b); 0 = free slot ----
    shared(ulong)* prodHashBulk;
    shared(ulong)* prodHashSmall;

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
        if (maxBulk > 0)
        {
            // 64-aligned so slot 0's line cannot bleed into an unrelated
            // heap chunk; zeroed because aligned_alloc does not zero.
            f.prodHashBulk = cast(shared(ulong)*)
                aligned_alloc(64, maxBulk * PROD_SLOT_STRIDE * 8);
            if (f.prodHashBulk is null) fatal("alloc producer tickets failed");
            memset(cast(void*) f.prodHashBulk, 0, maxBulk * PROD_SLOT_STRIDE * 8);
        }
        if (maxSmall > 0)
        {
            f.prodHashSmall = cast(shared(ulong)*)
                aligned_alloc(64, maxSmall * PROD_SLOT_STRIDE * 8);
            if (f.prodHashSmall is null) fatal("alloc producer tickets failed");
            memset(cast(void*) f.prodHashSmall, 0, maxSmall * PROD_SLOT_STRIDE * 8);
        }

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
        if (atomicLoad!(MemoryOrder.raw)(Cf) != 0)
            fatal("destroy with live consumers");
        foreach (i; 0 .. maxBulk)
            if (prodHashBulk !is null && atomicLoad!(MemoryOrder.raw)(*prodSlot(prodHashBulk, i)) != 0)
                fatal("destroy with live bulk ticket");
        foreach (i; 0 .. maxSmall)
            if (prodHashSmall !is null && atomicLoad!(MemoryOrder.raw)(*prodSlot(prodHashSmall, i)) != 0)
                fatal("destroy with live small ticket");
        if (prodHashBulk !is null)
            free(cast(void*) prodHashBulk);
        if (prodHashSmall !is null)
            free(cast(void*) prodHashSmall);
        if (buf !is null)
            munmap(cast(void*) buf, 2 * bufBytes);
        if (shmFd >= 0)
            close(shmFd);
        shmFd = -1;
        free(cast(void*) &this);
    }

    // ------------------------------------------------------------------
    // Registration (spec 5a-a, 5b-a, 3b)
    // ------------------------------------------------------------------

    /// fetch-inc Cf; on success fetch-inc Reqs_c and return it (the IDc).
    /// Returns a negative value if oversubscribed.
    long add_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchAdd(Cf, 1);
        if (prev >= MAX_CONSUMERS_LIMIT)
        {
            atomicFetchSub(Cf, 1);
            return -1;
        }
        return cast(long) atomicFetchAdd(Reqs_c, 1);
    }

    /// Returns the previous Cf; when it returns 1 the caller is the last
    /// unsubscriber (spec 5b-a).
    long sub_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchSub(Cf, 1);
        if (prev <= 0) fatal("Cf underflow");
        return prev;
    }

    /// Spec 3b: allocate a ticketed slot. An invalid Token means the tier is full.
    Token registerProducer(Tier t) nothrow @nogc @system
    {
        immutable cap = t == Tier.bulk ? maxBulk : maxSmall;
        auto pr = t == Tier.bulk ? &Prbulk : &Prsm;
        auto hashes = t == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (cap == 0 || hashes is null)
            return Token.init;
        immutable p = atomicFetchAdd(*pr, 1);
        if (p >= cap)
        {
            atomicFetchSub(*pr, 1);
            return Token.init;
        }
        immutable reqs = atomicFetchAdd(Reqs_p, 1);
        foreach (i; 0 .. cap)
        {
            immutable h = mixToken(cast(uint) i, reqs);
            if (cas(prodSlot(hashes, i), 0UL, h))
                return Token(t, cast(uint) i, h);
        }
        fatal("producer slot missing");
        return Token.init;
    }

    /// Spec 3b: a Token that does not match the live slot is fatal.
    void unregisterProducer(Token tok) nothrow @nogc @system
    {
        if (!tok.valid) fatal("unregister invalid token");
        immutable cap = tok.tier == Tier.bulk ? maxBulk : maxSmall;
        auto hashes = tok.tier == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (hashes is null || tok.slot >= cap) fatal("unregister bad slot");
        if (atomicLoad!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot)) != tok.hash)
            fatal("unregister token mismatch");
        atomicStore!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot), 0UL);
        if (tok.tier == Tier.bulk)
        {
            if (atomicFetchSub(Prbulk, 1) <= 0) fatal("Prbulk underflow");
        }
        else
        {
            if (atomicFetchSub(Prsm, 1) <= 0) fatal("Prsm underflow");
        }
    }

    /// Spec 4a: write() fatals unless tok matches a live slot of its tier.
    private void requireToken(Token tok) nothrow @nogc @system
    {
        if (!tok.valid) fatal("write requires a producer token");
        immutable cap = tok.tier == Tier.bulk ? maxBulk : maxSmall;
        auto hashes = tok.tier == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (hashes is null || tok.slot >= cap) fatal("write bad token slot");
        if (atomicLoad!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot)) != tok.hash)
            fatal("write token mismatch");
    }

    /// Bits 16..31 of Rt': Sub0 units. At rest 0 or 1; transiently more.
    private enum ulong SUB0MASK = LOWMASK ^ COUNTMASK;

    /// Plant one Sub0 on an incomplete epoch that currently has no consumer
    /// count and no pulse. Does not retract: a racing 0→1 clears Sub0.
    void plantIfUnprotected(ulong e) nothrow @nogc @system
    {
        immutable ki = cast(uint)(e & kMask);
        if (atomicLoad!(MemoryOrder.acq)(stats[ki].es) != cast(long) e)
            return;
        immutable ei1 = e + 1;
        immutable ki1 = cast(uint)(ei1 & kMask);
        if (atomicLoad!(MemoryOrder.raw)(stats[ki1].es) != cast(long) ei1)
            return;
        immutable seqt = atomicLoad!(MemoryOrder.raw)(stats[ki].seqt);
        immutable seqtN = atomicLoad!(MemoryOrder.raw)(stats[ki1].seqt);
        immutable sd = atomicLoad!(MemoryOrder.raw)(stats[ki].sd);
        if (seqt >= seqtN || seqt + sd >= seqtN)
            return;
        // CAS the plant so a racing 0->1 subscriber cannot be straddled by
        // a bare fetch_add(SUB0).
        for (;;)
        {
            immutable rt = atomicLoad!(MemoryOrder.acq)(Rt[ki][0]);
            if ((rt & COUNTMASK) != 0 || (rt & SUB0MASK) != 0)
                return;
            if (cas(&Rt[ki][0], rt, rt + SUB0))
                return;
        }
    }

    /// After unsubscribe, cover any incomplete epoch still sitting at Rt==0.
    void plantUnprotectedIncomplete() nothrow @nogc @system
    {
        foreach (i; 0 .. K)
        {
            immutable es = atomicLoad!(MemoryOrder.raw)(stats[i].es);
            if (es >= 0)
                plantIfUnprotected(cast(ulong) es);
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
    /// high; callers retry or wait on consumers. `tok` is required.
    ulong write(scope PayloadEntry[] payloads, ref ulong exi, Token tok)
        nothrow @nogc @system
    {
        requireToken(tok);
        if (payloads.length == 0) return 0;
        immutable quota = tok.tier == Tier.bulk ? quotaBulk : quotaSmall;
        immutable maxExi = quotaBulk > quotaSmall ? quotaBulk : quotaSmall;
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
                if (pe.header.maxCs == 0 || pe.header.maxCs > MAX_PAYLOAD_ITERS)
                    fatal("payload MaxCs out of range");
                if (pe.header.done == 0 || pe.header.done > MAX_PAYLOAD_ITERS)
                    fatal("payload Done out of range");
                if (pe.header.done > 1 && pe.header.maxCs < 2)
                    fatal("multithreaded payload requires MaxCs >= 2");
                if (pe.body.length > ulong.max - PHEAD_LEN)
                    fatal("payload size overflow");
                immutable psz = PHEAD_LEN + pe.body.length;
                immutable oneMt = pe.header.maxCs > 1 ? 1UL : 0UL;
                immutable singleton = tableSizeChecked(1, oneMt, sq, psz);
                if (singleton > maxExi)
                    fatal("payload larger than any producer Exi");
                immutable cm = m + (pe.header.maxCs > 1 ? 1 : 0);
                immutable cand = tableSizeChecked(i + 1, cm, sq, addChecked(psum, psz, "payload sum overflow"));
                if (cand > exi) break;
                n = i + 1;
                m = cast(uint) cm;
                psum = addChecked(psum, psz, "payload sum overflow");
            }
            if (n > 0) break;
            if (!refreshQuota(exi, quota)) return 0;
        }

        immutable size = tableSizeChecked(n, m, sq, psum);

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

        // A write that crossed past Ki made Ki unable to accept more tables.
        // If no consumer ever entered it, last-releaser never runs and Rt
        // stays 0 — plant Sub0 so the next subscribe walk can find it.
        foreach (e; eold + 1 .. enew + 1)
            plantIfUnprotected(e - 1);

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
            po = addChecked(po, PHEAD_LEN + payloads[i].body.length, "Tindex overflow");
        }
        po = payOff;
        size_t mi = THEAD_LEN + n;
        foreach (i; 0 .. n)
        {
            immutable o = po;
            po = addChecked(po, PHEAD_LEN + payloads[i].body.length, "Tindex overflow");
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
/// The consumer holds a *position* reference on the segment of the last
/// table it validated (or, when idle at Wt, the initialized frontier of
/// nextSeq), plus a local array of *trailing* references on segments it
/// has passed through but not yet confirmed complete. Confirmation (5j):
/// the next segment has been initialized for epoch Ei+1 and
/// Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1].
///
/// Invariants:
///  - Every subscribed consumer always holds at least one pin; an idle
///    consumer migrates that pin onto nextSeq's initialized segment.
///  - The last *releaser* of an unconfirmed segment plants Sub0 via add /
///    maybe-sub. First consumer of a segment (count 0→1) subtracts it.
///  - Farm-empty with everything confirmed still plants exactly one pulse,
///    using the same last-releaser helper on the confirmed position.
struct ConsumerView
{
    @disable this(this);

    AntFarm* F;
    ulong IDc;          /// taken from F's Reqs_c during subscription
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

    /// Bench-only (perftest/tail). 0 = production: drain the shard, use
    /// CLAIM_CHUNK / BIG_CHUNK. Nonzero max-runs ends the visit after that
    /// many claimed runs and still advances nextSeq (leftovers: idle re-walk).
    /// Nonzero chunk replaces the spec chunk size.
    uint benchMaxRuns;
    uint benchChunk;

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
        // real consumer reference. Sub does not plant or clear Sub0.
        atomicFetchAdd(f.Rt[seg][0], SUB);

        // 5a-d: attach in place. Fail closed if the frontier was never written.
        immutable es = atomicLoad!(MemoryOrder.acq)(f.stats[seg].es);
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[seg].sqcs);
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[seg].seqt);
        if (es < 0 || sq == 0)
        {
            atomicFetchSub(f.Rt[seg][0], SUB);
            f.sub_consumer();
            return -1;
        }
        F = f;
        IDc = cast(ulong) idc;
        immutable lti = cast(uint)(IDc % sq);
        incLeaf(seg, lti);
        // 5a-g: remove our Sub. The 0→1 on Rt already retracted Sub0 if present.
        atomicFetchSub(f.Rt[seg][0], SUB);
        nextSeq = seqt;
        curKi = seg;
        curEi = cast(ulong) es;
        curLti = lti;
        trailN = 0;
        sweeperNext = false;
        hasRef = true;
        return es;
    }

    /// Spec 5b. Confirmed trails drop with a plain dec. Unconfirmed refs,
    /// and a last-of-farm confirmed position, use the last-releaser helper
    /// (add Sub0, dec the count, retract if this thread was not last).
    void unsubscribe() nothrow @nogc @system
    {
        if (!hasRef) return;
        auto f = F;
        tryReleaseTrailing();
        immutable lastOfFarm = f.sub_consumer() == 1;
        immutable posPulse = lastOfFarm || !confirmed(curKi, curEi);
        decRef(curKi, curLti, posPulse);
        foreach (i; 0 .. trailN)
            decRef(trailKi[i], trailLti[i], true);
        trailN = 0;
        hasRef = false;
        IDc = 0;
        f.plantUnprotectedIncomplete();
        F = null;
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
            migrateToFrontier();
            sweepOldestTrailing();
            sweepCurrentPosition();
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
                // Bench yielders (benchMaxRuns != 0) also try shard 0 on a
                // small next table, so a mid-tick sentinel is visible after
                // one run instead of waiting for a native Shi==0 visitor.
                if (!sweeper && benchMaxRuns != 0 && tlen < SMALL_TABLE_THRESHOLD && myShi != 0)
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
                sweeperNext = sweeper;
            }
            else
            {
                // Complete table: cheap pre-checked MT pass only (MT
                // payloads can outlive their shards' completion). Do not
                // touch sweeperNext here: a carried sweeper role must
                // survive across already-complete tables (H11).
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
        }

        nextSeq = tnext;
        migrateToFrontier();
        tryReleaseTrailing();
        return true;
    }

private:
    /// Spec 2a: take a count. Only the thread that observes 0→1 retracts Sub0.
    void takeRootCount(uint ki) nothrow @nogc @system
    {
        immutable old = atomicFetchAdd(F.Rt[ki][0], 1);
        immutable extra = old & AntFarm.SUB0MASK;
        if ((old & COUNTMASK) == 0 && extra != 0)
            atomicFetchSub(F.Rt[ki][0], extra);
    }

    /// Spec 2a/5b last-releaser: plant Sub0, dec the count, retract if not
    /// last. The add/dec/retract sequence is a single CAS loop: the old
    /// fetch-add/sub sequence could retract a SUB0 that a racing first
    /// subscriber had already cleared, underflowing the low half and
    /// borrowing from the SUB field.
    void releaseRootLeavePulse(uint ki) nothrow @nogc @system
    {
        for (;;)
        {
            immutable old = atomicLoad!(MemoryOrder.raw)(F.Rt[ki][0]);
            immutable c = old & COUNTMASK;
            if (c == 0) fatal("root tally underflow");
            ulong nv;
            if (c == 1)
            {
                // Last root: leave one pulse. Preserve any pulse already
                // present; add one only if none exists.
                nv = old & ~COUNTMASK;
                if ((old & AntFarm.SUB0MASK) == 0)
                    nv += SUB0;
            }
            else
            {
                // Not last: plain decrement, SUB0 field unchanged.
                nv = old - 1;
            }
            if (cas(&F.Rt[ki][0], old, nv))
                return;
        }
    }

    /// Increment a leaf; on 0→1 propagate to Rt and retract Sub0 if present.
    void incLeaf(uint ki, uint lti) nothrow @nogc @system
    {
        immutable lo = atomicFetchAdd(F.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo == 0)
            takeRootCount(ki);
    }

    /// Decrement a held leaf tally with root propagation (spec 2a). Edge
    /// transition (dec returns 1) repeats on Rt; underflows are fatal.
    /// `leavePulse` uses the last-releaser helper so an unconfirmed (or
    /// last-of-farm confirmed) drop never becomes visible as Rt == 0.
    void decRef(uint ki, uint lti, bool leavePulse = false) nothrow @nogc @system
    {
        auto f = F;
        immutable lo = atomicFetchSub(f.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo <= 0) fatal("leaf tally underflow");
        if (lo == 1)
        {
            if (leavePulse)
                releaseRootLeavePulse(ki);
            else
            {
                immutable ro = atomicFetchSub(f.Rt[ki][0], 1);
                if ((ro & COUNTMASK) == 0) fatal("root tally underflow");
            }
        }
    }

    /// Take the position reference on segment ki and demote the old one to
    /// a trailing reference. Increment-ahead (spec 2a) leaves no zero-gap.
    void moveRef(uint ki, ulong ei) nothrow @nogc @system
    {
        auto f = F;
        atomicLoad!(MemoryOrder.acq)(f.stats[ki].es); // pair with producer's release
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[ki].sqcs);
        if (sq == 0) fatal("moveRef into uninitialized segment");
        immutable lti = cast(uint)(IDc % sq);
        incLeaf(ki, lti);
        if (trailN == KMAX) fatal("trailing reference overflow");
        trailKi[trailN] = curKi;
        trailEi[trailN] = curEi;
        trailLti[trailN] = curLti;
        ++trailN;
        curKi = ki;
        curEi = ei;
        curLti = lti;
    }

    /// Spec 5c: if the position is confirmed and nextSeq names an initialized
    /// epoch, move the pin onto that frontier. Always ≥1 pin.
    void migrateToFrontier() nothrow @nogc @system
    {
        auto f = F;
        immutable ei = nextSeq >> f.segShift;
        immutable ki = cast(uint)(ei & f.kMask);
        if (ki == curKi && ei == curEi)
            return;
        if (!confirmed(curKi, curEi))
            return;
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki].es) != cast(long) ei)
            return;
        moveRef(ki, ei);
        tryReleaseTrailing();
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

    /// Idle path: re-walk the current position segment from its first table
    /// up to nextSeq, claiming any work that earlier hot-path visits left
    /// behind (for example a small table skipped by a consumer whose shard
    /// was not 0 and which had no carried-sweeper role). The segment is
    /// protected by the position reference, so the data is present.
    void sweepCurrentPosition() nothrow @nogc @system
    {
        auto f = F;
        auto bp = f.buf;
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[curKi].seqt);
        immutable end = nextSeq;
        if (seqt >= end)
            return;
        ulong seq = seqt;
        while (seq < end)
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
        incLeaf(curKi, nl);
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
        if ((c >> 32) == 0xFFFF_FFFFUL) fatal("Pcount claims wrap");
        if ((c >> 32) >= head.maxCs)
            return; // overallocated
        do
        {
            immutable d = atomicFetchAdd(head.pcount, 1UL << 16);
            immutable called = (d >> 16) & 0xFFFF;
            if (called == 0xFFFF) fatal("Pcount calls wrap");
            if (called >= head.done)
                break;
            auto body_ = (cast(const(ulong)*)(cast(ulong*) bp + absIdx + PHEAD_LEN))[0 .. head.plen];
            head.call(head, body_, called);
            immutable oldc = atomicFetchAdd(head.pcount, 1UL);
            if ((oldc & 0xFFFF) == 0xFFFF) fatal("Pcount comps wrap");
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
        // 5e-g/h. benchChunk / benchMaxRuns are 0 in production.
        immutable specChunk = shbase >= BIG_CHUNK * 16 ? BIG_CHUNK : CLAIM_CHUNK;
        immutable chunk = benchChunk != 0 ? benchChunk : specChunk;
        immutable shiter = (shlen + chunk - 1) / chunk;
        uint runsDone;
        uint ownClaims = 0;
        bool firstClaimant = false;
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
            immutable rawc = atomicFetchAdd(*shc, 1UL << 32);
            if ((rawc >> 32) == 0xFFFF_FFFFUL) fatal("Tcount claims wrap");
            immutable x = cast(uint)(rawc >> 32);
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
            ++ownClaims;
            if (x == 0)
                firstClaimant = true;
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
            if ((y & 0xFFFF_FFFFUL) > 0xFFFF_FFFFUL - runlen) fatal("Tcount comps wrap");
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
            ++runsDone;
            if (benchMaxRuns != 0 && runsDone >= benchMaxRuns)
                return 0;
            // Mid-tick latency relief (see perftest/POSTMORTEM.md): the
            // first claimant of a shard (the consumer that drew X == 0)
            // can detect that the shard is shared by observing more claim
            // strides than its own (claimsNow > ownClaims). After finishing
            // a claim-chunk + consume-chunk iteration, probe whether Tnext
            // is already published; if it is, stop working this shard so
            // consumeNext can advance to the live table (for example a
            // mid-tick 1-payload write) instead of draining the whole shard
            // first. Only the first claimant may break, so at most one
            // consumer per shard can leave early, and only when the shard
            // is shared. The consumers that claimed the other strides will
            // finish the shard, so unsubscription after the break cannot
            // starve it; OS pre-emption can delay completion but the idle
            // re-walk remains the backstop.
            if (!checkFirst && firstClaimant)
            {
                immutable claimsNow = atomicLoad!(MemoryOrder.raw)(*shc) >> 32;
                if (claimsNow > ownClaims)
                {
                    immutable tnext = atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 1]);
                    if (atomicLoad!(MemoryOrder.acq)(bp[tnext & F.Lmask]) == sentinelOf(tnext))
                        return 0;
                }
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
