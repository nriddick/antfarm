/++
 + Ant Farm: an M:N concurrent queue with superlative scaling.
 +
 + Responsibility: the fixed-memory ring, producer registration and quota,
 + work tables, consumer claims and segment reclamation. It imports only
 + `antfarm_allocation`; `antfarm_templates` and the actors build on it.
 + Canonical docs: SPEC.md is the algorithm and memory-order contract,
 + ARCHITECTURE.md is the layer and sizing guide, and docs/README.md is the
 + documentation map.
 + Section map: platform/mapping helpers, payload types and ranges, the
 + AntFarm struct (construction, registration, production), then
 + ConsumerView (claims, dispatch, reclamation helpers).
 + The AntfarmWriteAuditHooks scheduling points are production-elided:
 + ordinary builds contain neither the hook nor its call sites.
 +
 + Implementation of SPEC.md. A circular buffer using the "magic buffer"
 + memory mapping, K segments with root/leaf reference tallies, quota-bounded
 + producers and sharded consumers. No CAS on hot paths; hot-path
 + synchronization is fetch-add / fetch-sub plus acquire/release load/store.
 + Cold-path CAS: ticket claim, last-releaser pulse, plantIfUnprotected
 + plant/retract (SPEC.md 4d, 6c, 6e). Retry loops reload fresh state;
 + their retries are bounded by the low-half/confirmation state settling,
 + not by back-and-forth noise. Strong CAS (spec 1).
 + Producer registration/deregistration uses a Farm-local blocking mutex;
 + publishing and consuming never acquire it.
 +
 + Errors are fatal (process abort) rather than exceptional, per spec.
 + Interfaces are @nogc nothrow @system.
 +
 + Port note: the magic buffer is a dual virtual map of one physical
 + region. Posix: shm_open + mmap MAP_FIXED twice (Linux may madvise
 + MADV_HUGEPAGE). Windows 10 1803+: VirtualAlloc2 placeholders +
 + MapViewOfFile3; hugePages uses SEC_LARGE_PAGES (SeLockMemoryPrivilege).
 +/
module antfarm;

import core.atomic;
import core.stdc.stdio : fprintf, stderr, snprintf;
import core.stdc.stdlib : abort, free, getenv;
import core.stdc.string : memset;
import antfarm_allocation : allocateAligned64, freeAligned64;
import std.range.primitives : ElementType, empty, front, hasLength,
    isForwardRange, isInputRange, popFront, popFrontN, save;
import std.traits : Unqual;

version (Windows)
{
    import core.sys.windows.winbase;
    import core.sys.windows.windef;
    import core.sys.windows.winnt;
    pragma(lib, "advapi32");

    private extern (Windows) void AcquireSRWLockExclusive(void**) nothrow @nogc;
    private extern (Windows) void ReleaseSRWLockExclusive(void**) nothrow @nogc;
}
else version (Posix)
{
    import core.sys.posix.sys.mman;
    import core.sys.posix.sys.stat;
    import core.sys.posix.sys.types;
    import core.sys.posix.fcntl;
    import core.sys.posix.unistd;
    import core.sys.posix.pthread : pthread_mutex_t, pthread_mutex_init,
        pthread_mutex_destroy, pthread_mutex_lock, pthread_mutex_unlock;
    version (linux)
        import core.sys.linux.sys.mman : madvise, MADV_HUGEPAGE;
}
else
    static assert(false, "antfarm: magic buffer mapping only implemented for Windows and Posix");

version (Windows)
{
    private enum MEM_RESERVE_PLACEHOLDER = 0x00040000;
    private enum MEM_REPLACE_PLACEHOLDER = 0x00004000;
    private enum MEM_PRESERVE_PLACEHOLDER = 0x00000002;
    private enum MEM_LARGE_PAGES_FLAG = 0x20000000;
    private enum FILE_MAP_LARGE_PAGES_FLAG = 0x20000000;
    private enum SEC_LARGE_PAGES_FLAG = 0x80000000;
    private enum MemExtendedParameterAddressRequirements = 1;

    private struct MEM_ADDRESS_REQUIREMENTS
    {
        PVOID  LowestStartingAddress;
        PVOID  HighestEndingAddress;
        size_t Alignment;
    }

    private struct MEM_EXTENDED_PARAMETER
    {
        ulong typeAndReserved;
        union
        {
            ulong  ULong64;
            void*  Pointer;
            size_t Size;
            HANDLE Handle;
            DWORD  ULong;
        }
    }

    private alias FnVirtualAlloc2 = extern (Windows) PVOID function(
        HANDLE, PVOID, size_t, ULONG, ULONG, MEM_EXTENDED_PARAMETER*, ULONG) @nogc nothrow;
    private alias FnMapViewOfFile3 = extern (Windows) PVOID function(
        HANDLE, HANDLE, PVOID, ulong, size_t, ULONG, ULONG, MEM_EXTENDED_PARAMETER*, ULONG) @nogc nothrow;
    private alias FnGetLargePageMinimum = extern (Windows) size_t function() @nogc nothrow;

    private __gshared FnVirtualAlloc2 pVirtualAlloc2;
    private __gshared FnMapViewOfFile3 pMapViewOfFile3;
    private __gshared FnGetLargePageMinimum pGetLargePageMinimum;
    private __gshared bool winApisResolved;
    private __gshared void* winMapApiLock; // SRWLOCK_INIT

    private T winLoad(T)(const(char)* dll, const(char)* name) nothrow @nogc @system
    {
        auto h = GetModuleHandleA(dll);
        if (h is null)
            h = LoadLibraryA(dll);
        if (h is null) return null;
        return cast(T) GetProcAddress(h, name);
    }

    private void resolveWinMapApis() nothrow @nogc @system
    {
        AcquireSRWLockExclusive(&winMapApiLock);
        scope (exit) ReleaseSRWLockExclusive(&winMapApiLock);
        if (winApisResolved) return;
        pVirtualAlloc2 = winLoad!FnVirtualAlloc2("kernelbase.dll", "VirtualAlloc2");
        if (pVirtualAlloc2 is null)
            pVirtualAlloc2 = winLoad!FnVirtualAlloc2("kernel32.dll", "VirtualAlloc2");
        pMapViewOfFile3 = winLoad!FnMapViewOfFile3("kernelbase.dll", "MapViewOfFile3");
        if (pMapViewOfFile3 is null)
            pMapViewOfFile3 = winLoad!FnMapViewOfFile3("kernel32.dll", "MapViewOfFile3");
        pGetLargePageMinimum = winLoad!FnGetLargePageMinimum("kernel32.dll", "GetLargePageMinimum");
        winApisResolved = true;
    }

    private bool enableLockPages() nothrow @nogc @system
    {
        HANDLE tok;
        if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &tok))
            return false;
        scope (exit) CloseHandle(tok);
        TOKEN_PRIVILEGES tp;
        tp.PrivilegeCount = 1;
        if (!LookupPrivilegeValueW(null, SE_LOCK_MEMORY_NAME.ptr, &tp._Privileges.Luid))
            return false;
        tp._Privileges.Attributes = SE_PRIVILEGE_ENABLED;
        SetLastError(0);
        if (!AdjustTokenPrivileges(tok, FALSE, &tp, 0, null, null))
            return false;
        return GetLastError() == 0;
    }

    private void fatalWin(const(char)[] msg) nothrow @nogc @system
    {
        char[160] buf = void;
        auto n = snprintf(buf.ptr, buf.length, "%.*s (GetLastError=%u)",
            cast(int) msg.length, msg.ptr, GetLastError());
        if (n < 0) n = 0;
        if (n > buf.length) n = buf.length;
        fatal(buf[0 .. n]);
    }

    /// This SKU rejects MapViewOfFile3(MEM_REPLACE_PLACEHOLDER | MEM_LARGE_PAGES)
    /// (ERROR_INVALID_PARAMETER). Dual MapViewOfFileEx with FILE_MAP_LARGE_PAGES works.
    private void* mapMagicBufferWindowsLarge(size_t bytes) nothrow @nogc @system
    {
        if (!enableLockPages())
            fatal("antfarm: SeLockMemoryPrivilege not held. Grant Lock Pages in Memory to this user, log off, and retry (see README)");
        if (pGetLargePageMinimum is null)
            fatal("antfarm: GetLargePageMinimum unavailable");
        auto lp = pGetLargePageMinimum();
        if (lp == 0 || (bytes % lp) != 0)
            fatal("antfarm: ring size is not a multiple of the large-page minimum");

        auto section = CreateFileMappingW(INVALID_HANDLE_VALUE, null,
            PAGE_READWRITE | SEC_COMMIT | SEC_LARGE_PAGES_FLAG,
            cast(DWORD)(bytes >> 32), cast(DWORD) bytes, null);
        if (section is null)
            fatalWin("antfarm: CreateFileMapping SEC_LARGE_PAGES failed");

        foreach (attempt; 0 .. 16)
        {
            MEM_ADDRESS_REQUIREMENTS req;
            req.Alignment = lp;
            MEM_EXTENDED_PARAMETER ext;
            ext.typeAndReserved = MemExtendedParameterAddressRequirements;
            ext.Pointer = &req;
            auto hole = pVirtualAlloc2(null, null, 2 * bytes,
                MEM_RESERVE, PAGE_NOACCESS, &ext, 1);
            char* base;
            if (hole !is null)
            {
                base = cast(char*) hole;
                VirtualFree(hole, 0, MEM_RELEASE);
            }
            else
            {
                hole = VirtualAlloc(null, 2 * bytes + lp, MEM_RESERVE, PAGE_NOACCESS);
                if (hole is null)
                    continue;
                base = cast(char*)((cast(size_t) hole + lp - 1) & ~(lp - 1));
                VirtualFree(hole, 0, MEM_RELEASE);
            }
            auto v1 = MapViewOfFileEx(section, FILE_MAP_ALL_ACCESS | FILE_MAP_LARGE_PAGES_FLAG,
                0, 0, bytes, base);
            if (v1 is null)
                continue;
            auto v2 = MapViewOfFileEx(section, FILE_MAP_ALL_ACCESS | FILE_MAP_LARGE_PAGES_FLAG,
                0, 0, bytes, base + bytes);
            if (v2 is null)
            {
                UnmapViewOfFile(v1);
                continue;
            }
            CloseHandle(section);
            return v1;
        }
        CloseHandle(section);
        fatalWin("antfarm: MapViewOfFileEx large-page dual map failed");
        return null;
    }
}

// ==== Section: platform helpers and the magic-buffer mapping ====
/// Farm-local blocking mutex for producer slot lifetime transitions only.
/// Publication and consumption never acquire this lock.
private struct ProducerMutex
{
    version (Windows)
    {
        // An SRW lock is pointer-sized and initialized to zero (SRWLOCK_INIT).
        private void* state;
        void initialize() nothrow @nogc @system {}
        void destroy() nothrow @nogc @system {}
        void lock() nothrow @nogc @system { AcquireSRWLockExclusive(&state); }
        void unlock() nothrow @nogc @system { ReleaseSRWLockExclusive(&state); }
    }
    else version (Posix)
    {
        private pthread_mutex_t state;
        void initialize() nothrow @nogc @system
        {
            if (pthread_mutex_init(&state, null) != 0)
                fatal("producer mutex initialization failed");
        }
        void destroy() nothrow @nogc @system
        {
            if (pthread_mutex_destroy(&state) != 0)
                fatal("producer mutex destruction failed");
        }
        void lock() nothrow @nogc @system
        {
            if (pthread_mutex_lock(&state) != 0)
                fatal("producer mutex lock failed");
        }
        void unlock() nothrow @nogc @system
        {
            if (pthread_mutex_unlock(&state) != 0)
                fatal("producer mutex unlock failed");
        }
    }
}

private void* afAlignedAlloc(size_t bytes) nothrow @nogc @system
{
    return allocateAligned64(bytes);
}

private void afAlignedFree(void* p) nothrow @nogc @system
{
    freeAligned64(p);
}

/// `ANTFARM_HUGE_PAGES=0` forces 4K; `=1` forces huge pages. Unset or empty
/// keeps `requested`. Any other value is fatal so a typo cannot silently
/// select the wrong backing.
private bool wantHugePages(bool requested) nothrow @nogc @system
{
    auto e = getenv("ANTFARM_HUGE_PAGES");
    if (e is null || e[0] == 0) return requested;
    if (e[1] == 0)
    {
        if (e[0] == '0') return false;
        if (e[0] == '1') return true;
    }
    fatal("ANTFARM_HUGE_PAGES must be 0 or 1");
    return requested;
}

/// Dual-map `bytes` so `[0, bytes)` aliases `[bytes, 2*bytes)`.
private void* mapMagicBuffer(size_t bytes, bool hugePages) nothrow @nogc @system
{
    version (Windows)
    {
        resolveWinMapApis();
        if (pVirtualAlloc2 is null || pMapViewOfFile3 is null)
            fatal("antfarm: VirtualAlloc2/MapViewOfFile3 unavailable (need Windows 10 1803+)");

        if (hugePages)
            return mapMagicBufferWindowsLarge(bytes);

        auto p = pVirtualAlloc2(null, null, 2 * bytes,
            MEM_RESERVE | MEM_RESERVE_PLACEHOLDER, PAGE_NOACCESS, null, 0);
        if (p is null)
            fatalWin("antfarm: VirtualAlloc2 placeholder reserve failed");
        if (!VirtualFree(p, bytes, MEM_RELEASE | MEM_PRESERVE_PLACEHOLDER))
            fatalWin("antfarm: VirtualFree placeholder split failed");

        auto section = CreateFileMappingW(INVALID_HANDLE_VALUE, null, PAGE_READWRITE,
            cast(DWORD)(bytes >> 32), cast(DWORD) bytes, null);
        if (section is null)
            fatalWin("antfarm: CreateFileMapping failed");

        auto v1 = pMapViewOfFile3(section, null, p, 0, bytes,
            MEM_REPLACE_PLACEHOLDER, PAGE_READWRITE, null, 0);
        auto v2 = pMapViewOfFile3(section, null, cast(char*) p + bytes, 0, bytes,
            MEM_REPLACE_PLACEHOLDER, PAGE_READWRITE, null, 0);
        CloseHandle(section);
        if (v1 is null || v2 is null)
        {
            if (v1 !is null) UnmapViewOfFile(v1);
            if (v2 !is null) UnmapViewOfFile(v2);
            fatalWin("antfarm: MapViewOfFile3 dual map failed");
        }
        return v1;
    }
    else version (Posix)
    {
        static __gshared int shmCounter;
        char[64] name;
        snprintf(name.ptr, name.length, "/antfarm-%d-%d", cast(int) getpid(),
                 atomicFetchAdd!(MemoryOrder.raw)(shmCounter, 1));
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
        close(fd);

        if (hugePages)
        {
            version (linux)
            {
                if (madvise(p, bytes, MADV_HUGEPAGE) != 0)
                    fatal("madvise first half huge pages failed");
                if (madvise(cast(char*) p + bytes, bytes, MADV_HUGEPAGE) != 0)
                    fatal("madvise second half huge pages failed");
            }
            else
                fatal("huge pages only supported on Linux");
        }
        return p;
    }
}

private void unmapMagicBuffer(void* p, size_t bytes) nothrow @nogc @system
{
    if (p is null) return;
    version (Windows)
    {
        UnmapViewOfFile(p);
        UnmapViewOfFile(cast(char*) p + bytes);
    }
    else version (Posix)
        munmap(p, 2 * bytes);
}

/// Spec 4c: maximum number of simultaneous subscribed consumers.
enum MAX_CONSUMERS_LIMIT = 128;
/// Negative `ConsumerView.subscribe` results (spec 4c, 6b). Every failure
/// has already unwound its temporary pins; the view stays unsubscribed.
/// The Farm already has MAX_CONSUMERS_LIMIT subscribed views. Retry only
/// after another view unsubscribes.
enum long SUBSCRIBE_FULL = -1;
/// The frontier was uninitialized or moved during the attach walk. This is
/// transient; retry the same call.
enum long SUBSCRIBE_RETRY = -2;
/// Caller error: a null Farm, or a view that is already subscribed.
enum long SUBSCRIBE_INVALID = -3;
/// Internal: spec 2b — preallocated leaf tallies per segment: ceiling square root of 128.
enum MAX_LEAVES = 12;
/// Spec 7a default small-table threshold. 0 at construction selects the
/// auto rule so each shard gets at least one chunk.
enum DEFAULT_SMALL_TABLE_THRESHOLD = 64;
/// Default dual-ring backing. Huge pages are an explicit workload-tuned opt-in.
enum bool DEFAULT_HUGE_PAGES = false;
/// Spec 7a: chunk ceiling for the avgCost hint. write() publishes AvgCost
/// in Thead; consumers compute chunk = MAX_CHUNK >> AvgCost. Default
/// AvgCost = 3 -> chunk 16, a latency-conserving default between [1, 128].
enum uint MAX_CHUNK = 128;
/// log2(MAX_CHUNK); avgCost outside 0 .. MAX_AVG_COST is a caller error.
enum uint MAX_AVG_COST = 7;
/// Default `avgCost` of every public write overload: chunk 16, the latency-
/// conserving middle of the range. Cheap uniform work opts into classes 0 .. 2
/// (chunks 128, 64, 32); expensive work uses higher classes.
enum uint DEFAULT_AVG_COST = 3;

/// The `avgCost` class whose claims hold `chunk` entries (`MAX_CHUNK >>
/// class == chunk`). Internal callers name the chunk they were tuned for so
/// their claim splits do not move when `MAX_CHUNK` does.
///
/// A `chunk` that is not a power of two rounds down to the next smaller power
/// of two (24 gives the class of 16). A `chunk` of 0 or 1 gives
/// `MAX_AVG_COST` and a `chunk` of `MAX_CHUNK` or more gives 0. Usable at
/// compile time, so `enum cls = avgCostForChunk(32);` is a constant.
uint avgCostForChunk(uint chunk) pure nothrow @nogc @safe
{
    uint cost;
    while (cost < MAX_AVG_COST && (MAX_CHUNK >> cost) > chunk) ++cost;
    return cost;
}
/// Sentinel work-cost hint for callers that derive an effective class from
/// the table they are about to publish instead of naming one. The private
/// root pump uses it as its `avgCost` default; see `rootCostHintFor`.
enum uint rootCostHintAuto = uint.max;
/// Ceiling on the claims a derived-hint wholesale table asks for. It bounds
/// the per-table claim overhead on large hosts; 8 covers the consumer counts
/// where splitting small tables measurably pays (see perftest results
/// perf-g4).
enum uint rootCostHintMaxSplits = 8;
/// Largest chunk `rootCostHintFor` hands out. It is the cheapest class the
/// derived hint ever names: the hint was tuned against chunk 32 (perf-g4), so
/// it does not follow `MAX_CHUNK` upward.
enum uint rootCostHintMaxChunk = 32;

/++
 + Effective work-cost class for a default-hint table of `count` entries on
 + a farm whose configured small-table threshold is `smallThreshold`.
 + Consumers size a table's claims as `chunk = MAX_CHUNK >> avgCost` (spec
 + 7a), and a table below the small threshold is claimed wholesale by
 + shard 0 (spec 7a), so a cheap class (`avgCost = 0`, chunk 128) can run
 + every callback of a table of up to 128 entries inside one consumer claim,
 + serializing an expensive table (for example parents that each allocate a
 + child wave) on one worker.
 +
 + The derived hint never names a chunk above `rootCostHintMaxChunk` (32,
 + class 2). A table at least `smallThreshold` entries wide is sharded across
 + every shard and already spans multiple workers, so it keeps that class;
 + only a wholesale (single-shard) table is reclassed. For such a table this
 + returns the cheapest class whose claims still number at least
 + `min(count, clamp(consumers, 1, rootCostHintMaxSplits))`, so a small table
 + can occupy up to ~consumer-count workers, and the class of chunk 32 is kept
 + whenever it already provides that many claims. `consumers` is the farm's
 + live subscribed-view count (`Cf`), which can include the emitter's own
 + view; it may change between tables, and each table's class follows it
 + independently. A farm using the auto threshold rule
 + (`smallThreshold == 0`) is classified against DEFAULT_SMALL_TABLE_THRESHOLD,
 + which may differ from the auto rule's effective threshold; the result is
 + still a valid class.
 + Returns `avgCostForChunk(rootCostHintMaxChunk) .. MAX_AVG_COST`.
 +/
uint rootCostHintFor(size_t count, ulong consumers, uint smallThreshold)
    pure nothrow @nogc @safe
{
    enum uint cheapest = avgCostForChunk(rootCostHintMaxChunk);
    if (count == 0) return cheapest;
    // Sharded tables are already split across every shard (spec 7a), so
    // only wholesale tables can collapse onto fewer claims than consumers.
    if (count >= (smallThreshold != 0 ? smallThreshold : DEFAULT_SMALL_TABLE_THRESHOLD))
        return cheapest;
    immutable splits = consumers < 1 ? cast(ulong) 1
        : consumers > rootCostHintMaxSplits ? cast(ulong) rootCostHintMaxSplits
        : consumers;
    immutable target = cast(size_t) (count < splits ? count : splits);
    // Cheapest class whose ceil(count / chunk) claims reach the target.
    uint hint = cheapest;
    while (hint < MAX_AVG_COST
        && (count + (MAX_CHUNK >> hint) - 1) / (MAX_CHUNK >> hint) < target)
        ++hint;
    return hint;
}
/// Smallest ring accepted by `AntFarm.create`, in MiB (2^18 ulongs).
enum uint MIN_RING_MIB = 2;
/// Largest ring accepted by `AntFarm.create`, in MiB (64 GiB). Far beyond any
/// characterized size; it mainly rejects a length accidentally given in ulongs.
enum uint MAX_RING_MIB = 1 << 16;
/// log2 of the ulongs in one MiB: `Ln = ringMiB << ULONGS_PER_MIB_SHIFT`.
enum uint ULONGS_PER_MIB_SHIFT = 17;
/// Convert a ring length in ulongs to `AntFarm.create`'s `ringMiB`
/// argument. Fatal unless the length is a whole number of MiB, so a
/// migration from the former ulong parameter cannot silently truncate.
uint ringMiBFromUlongs(ulong ringUlongs) nothrow @nogc @system
{
    immutable mask = (1UL << ULONGS_PER_MIB_SHIFT) - 1;
    if ((ringUlongs & mask) != 0 || (ringUlongs >> ULONGS_PER_MIB_SHIFT) > uint.max)
        fatal("ring length in ulongs is not a whole number of MiB");
    return cast(uint)(ringUlongs >> ULONGS_PER_MIB_SHIFT);
}
/// Maximum supported number of segments K. Default create()/perftest K is 8.
enum KMAX = 16;

/// Spec 4d: producer ticket slot stride in ulongs. Each slot's hash sits on
/// its own 64-byte cache line: tickets are shared mutable state under the
/// registration CAS, and the buffer cannot align them beyond 8 bytes, so
/// the stride (not the natural ulong size) must carry the isolation.
private enum size_t PROD_SLOT_STRIDE = 8;

/// Internal: spec 2b/6b — a subscriber reference lives in the most significant half of Rt.
enum ulong SUB = 1UL << 32;
/// Internal: spec 2b/6c — Sub0 is a counted pin in units of 2^16, not a bit.
enum ulong SUB0 = 1UL << 16;
/// Internal: mask for Rtlow (least significant half of Rt).
enum ulong LOWMASK = 0xFFFF_FFFFUL;
/// Internal: mask for the active-consumer count bits within Rtlow (below the Sub0 bit).
enum ulong COUNTMASK = SUB0 - 1;
/// Spec 2d: Done and MaxCs are capped so packed 16-bit fields cannot wrap
/// through write().
enum uint MAX_PAYLOAD_ITERS = 512;

// Test-only scheduling points, elided in production builds. Tests may pause a
// producer here without changing quota or tally state; ordinary builds contain
// neither the hook nor its call sites.
version (AntfarmWriteAuditHooks)
{
    enum WriteAuditPhase { anchorProbed, quotaSwept, tailReserved, subscriberPinned, tablePublished, subscriberProbed, plantChecked, plantPublished }
    __gshared void function(AntFarm*, WriteAuditPhase, ulong, ulong)
        nothrow @nogc @system writeAuditHook;
}

private enum ulong SENTINEL_XOR = 0x9E37_79B9_7F4A_7C15UL;

/// Internal: sentinel stored (release) as the first word of a table; computed
/// from the 64-bit sequence pointing at the start of the table (spec
/// 5c/Thead).
ulong sentinelOf(ulong seq) pure nothrow @nogc @safe { return seq ^ SENTINEL_XOR; }

/// Internal: spec 2b — square root with a minimum value of 1 for Cs <= 2,
/// capped at the preallocated leaf count. ceil_sqrt(128) == 12.
uint sqcsOf(ulong cs) pure nothrow @nogc @safe
{
    if (cs <= 2) return 1;
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

/// Ring words are atomic objects. Consumers load-raw after an acquire of
/// Tsent; producers must store-raw (not plain) so wrap reuse is atomic vs
/// atomic, not a plain store racing an atomic load of the same physical word.
private void storeRaw(ref shared ulong slot, ulong v) nothrow @nogc @system
{
    atomicStore!(MemoryOrder.raw)(slot, v);
}

private void storeRawRange(shared(ulong)* p, size_t n, ulong v = 0) nothrow @nogc @system
{
    foreach (i; 0 .. n)
        atomicStore!(MemoryOrder.raw)(p[i], v);
}

private ulong loadRaw(ref shared ulong slot) nothrow @nogc @system
{
    return atomicLoad!(MemoryOrder.raw)(slot);
}

// Every leaf decrement is acq_rel: the last-on-leaf acquires preceding
// readers before releasing Rt. All Rt updates are RMWs, so a zero-root
// acquire sweep joins their release sequences; the acq_rel Wt reservation
// forwards that history to later producers spending retained quota. LLVM
// TSan models these actual atomics. Manual pin-drop/sweep annotations would
// bypass this chain and can hide a missing leaf acquire (leaf_reuse.d).
// The TSan version therefore selects runtime defaults only.
version (TSan)
{
    /// Default for a `-d-version=TSan` build. `TSAN_OPTIONS` still overrides.
    export extern (C) const(char)* __tsan_default_options() nothrow @nogc @system
    {
        return "history_size=7 halt_on_error=1";
    }
}

/// Packed-field wrap guards (Tcount/Pcount claims, calls, completions).
/// Provably unreachable under the 512 caps, the claim-per-payload rule and
/// MAX_CONSUMERS_LIMIT; they stay on as the corruption tripwire.

/// Compile-time switch for trailing-reference check frequency.
/// Default: eager release check after every table (baseline).
/// -d-version=SdCheckEveryN: check every SD_CHECK_EVERY_N table advances.
version (SdCheckEveryN)
    enum uint SD_CHECK_EVERY_N = 64;
else
    enum uint SD_CHECK_EVERY_N = 1;

// ==== Section: payload types and ranges ====
/// Spec 2d: read-only type-erased transport words for Call's work. Constness
/// protects the ring body, not an object reached through an encoded handle.
alias PayloadBody = const(ulong)[];

/// Spec 2d, 7c: decodes Pbody and executes an iteration of work.
///
/// `head` is valid only for the duration of the call and must not be modified
/// or retained. For a payload in a table of single-threaded payloads it points
/// to a consumer-local copy shared by the run's callbacks (SPEC.md section 2f,
/// compact ST tables), not to a ring header; multi-threaded payloads and mixed
/// tables point into the ring.
alias Callback = long function(PayloadHeader* head, PayloadBody body, ulong iteration) nothrow @nogc @system;

/// Low-level notification attached to one physical payload table. The Farm
/// invokes `call(context)` exactly once, on the consumer that advances that
/// table's primary Tprogress to Tlen. The pointed-to hook and its context
/// must remain stable until invocation. The Farm snapshots both fields before
/// calling and never accesses the hook/context afterward, so a notification
/// may release its own storage or transfer it as its final access. Otherwise
/// the caller must keep it stable until the notification returns.
alias TableCompletionCallback = void function(void* context)
    nothrow @nogc @system;

/// Optional exactly-once notification attached to one physical payload
/// table by writeTracked. The Farm snapshots both fields before invoking
/// `call(context)`, exactly once, on the consumer that advances that table's
/// primary Tprogress to Tlen. The pointed-to storage must remain stable
/// until invocation; see TableCompletionCallback for the full contract.
struct TableCompletionHook
{
    /// Opaque argument passed back to `call`.
    void* context;
    /// Invoked exactly once, on the consumer that completes the table.
    TableCompletionCallback call;

    /// True when `call` is set, i.e. the hook participates in a write.
    @property bool valid() const pure nothrow @nogc @safe
    {
        return call !is null;
    }
}

/// Spec 2d: payload header, 16 ulongs (128 bytes) as laid out in the buffer.
/// This is the public API type and the mixed/multithreaded ring layout: a
/// table containing any multithreaded payload stores this header per
/// payload. A compact all-single-threaded table stores only the Call word
/// per record (SPEC.md section 2f; the callback then receives a
/// consumer-local copy, not a ring header — see Callback). Producers set
/// maxCs, done and call; plen is overwritten with the body length and
/// pcount is runtime-owned.
/// Call sits immediately after Pcount: it is dereferenced only after a valid
/// claim is acquired (by the consumer that just wrote Pcount), so it can
/// share Pcount's cache line without adding cross-thread traffic; with the
/// 6 filler words after Call, Pcount's line is fully covered in every phase.
struct PayloadHeader
{
    uint maxCs;             /// maximum consumers per payload; 1 = single threaded
    uint done;              /// iterations to complete; 1 = single threaded
    ulong plen;             /// payload body length in ulongs
    ulong[6] filler;        /// layout filler covering Pcount's cache line
    shared ulong pcount;    /// 32 MSB claims | 16 bits calls | 16 LSB completions
    Callback call;          /// work callback — claim-gated, shares Pcount's line
    ulong[6] filler2;       /// layout filler closing the 128-byte header
}
static assert(PayloadHeader.sizeof == 128);
static assert(PayloadHeader.maxCs.offsetof == 0);
static assert(PayloadHeader.done.offsetof == 4);
static assert(PayloadHeader.plen.offsetof == 8);
static assert(PayloadHeader.filler.offsetof == 16);
static assert(PayloadHeader.pcount.offsetof == 64);
static assert(PayloadHeader.call.offsetof == 72);
static assert(PayloadHeader.filler2.offsetof == 80);
/// PayloadHeader length in ulongs.
private enum ulong PHEAD_LEN = PayloadHeader.sizeof / 8;

/// Compact ST table (spec 2f): a table with Tmt == 0 stores each
/// payload as [Call][Pbody...] with no other per-payload word. Tindex still
/// points at each record; Plen is the distance to the next Tindex entry
/// minus the Call word, and the last record ends at Tsize - END_PAD. The
/// reader hands the callback a consumer-local PayloadHeader carrying the
/// same values the ring header showed on the ST fast path.
private enum ulong PCOMPACT_LEN = 1;
static assert(Callback.sizeof == ulong.sizeof);
static assert(PCOMPACT_LEN < PHEAD_LEN);

/// Spec 2d: one ready-to-publish job: the header to copy into the table
/// (or a common header, for broadcast sources) and the body to append.
struct PayloadEntry
{
    /// Configuration of this payload; the Farm copies it into the table.
    PayloadHeader* header;
    /// Type-erased body words; copied after the header.
    PayloadBody body;
}

private void copyPayloadHeaderConfig(ref PayloadHeader dst,
        const PayloadHeader* src) nothrow @nogc @system
{
    dst = PayloadHeader.init;
    dst.maxCs = src.maxCs;
    dst.done = src.done;
    dst.plen = src.plen;
    dst.call = src.call;
}

private enum bool hasCommonPayloadHeader(R) =
    __traits(hasMember, R, "commonPayloadHeader");

private enum bool hasFixedPayloadLength(R) =
    __traits(hasMember, R, "fixedPayloadLength");

/// A lazy zip of independent header and body input ranges.  This is useful
/// when a job source naturally keeps scheduling metadata separate from its
/// argument storage.  The range ends when either input ends.
struct PayloadPairRange(HR, BR)
{
    /// Internal: the header source this adapter zips.
    HR headers;
    /// Internal: the body source this adapter zips.
    BR bodies;
    private PayloadHeader headerScratch;

    /// True when either source is exhausted.
    @property bool empty() nothrow @nogc @system
    {
        return headers.empty || bodies.empty;
    }

    /// The next PayloadEntry; header values are copied into scratch storage
    /// when the source yields values rather than pointers.
    @property PayloadEntry front() nothrow @nogc @system
    {
        PayloadHeader* hp;
        static if (is(ElementType!HR == PayloadHeader*))
            hp = headers.front;
        else
        {
            static if (is(ElementType!HR : const(PayloadHeader)*))
                copyPayloadHeaderConfig(headerScratch, headers.front);
            else
            {
                auto h = headers.front;
                copyPayloadHeaderConfig(headerScratch, &h);
            }
            hp = &headerScratch;
        }
        return PayloadEntry(hp, cast(PayloadBody) bodies.front);
    }

    /// Advance both sources by one element.
    void popFront() nothrow @nogc @system
    {
        headers.popFront();
        bodies.popFront();
    }

    /// Checkpoint both sources when they are forward ranges.
    static if (isForwardRange!HR && isForwardRange!BR)
    {
        @property typeof(this) save() nothrow @nogc @system
        {
            typeof(this) result = this;
            result.headers = headers.save;
            result.bodies = bodies.save;
            return result;
        }
    }

    static if (hasLength!HR && hasLength!BR)
    {
        /// The shorter of the two source lengths.
        @property size_t length() nothrow @nogc @system
        {
            immutable hn = cast(size_t) headers.length;
            immutable bn = cast(size_t) bodies.length;
            return hn < bn ? hn : bn;
        }
    }

    /// Pop up to `n` entries from both sources; returns how many were popped.
    size_t popFrontN(size_t n) nothrow @nogc @system
    {
        static if (hasLength!HR && hasLength!BR)
        {
            immutable count = n < length ? n : length;
            headers.popFrontN(count);
            bodies.popFrontN(count);
            return count;
        }
        else
        {
            size_t popped;
            while (popped < n && !empty)
            {
                popFront();
                ++popped;
            }
            return popped;
        }
    }
}

/// Adapt separate header and body input ranges to the ordinary payload-entry
/// source accepted by `write`. Header elements may be values or pointers;
/// body elements must be implicitly convertible to `const(ulong)[]`.
auto pairPayloads(HR, BR)(HR headers, BR bodies) nothrow @nogc @system
    if (isInputRange!HR && isInputRange!BR &&
        (is(Unqual!(ElementType!HR) == PayloadHeader) ||
         is(ElementType!HR : const(PayloadHeader)*)) &&
        is(ElementType!BR : PayloadBody))
{
    return PayloadPairRange!(HR, BR)(headers, bodies);
}

/// A lazy broadcast of one payload header over a body input range. The
/// header is copied into the adapter, so its source need only outlive this
/// call. Unlike pairing with a one-element header range, this adapter ends
/// only when the body range ends.
struct PayloadBroadcastRange(BR, bool fixedLength = false)
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    private BR bodies;
    private PayloadHeader hdr;
    static if (fixedLength)
        private ulong bodyWords;

    private this(const ref PayloadHeader header, BR source,
            ulong fixedBodyWords = 0)
            nothrow @nogc @system
    {
        bodies = source;
        copyPayloadHeaderConfig(hdr, &header);
        static if (fixedLength)
            bodyWords = fixedBodyWords;
    }

    /// Uniform-layout protocol consumed by AntFarm.writeImpl.
    @property PayloadHeader* commonPayloadHeader() nothrow @nogc @system
    {
        return &hdr;
    }

    /// True when the body source is exhausted.
    @property bool empty() nothrow @nogc @system { return bodies.empty; }

    /// The next entry: the common header paired with the next body.
    @property PayloadEntry front() nothrow @nogc @system
    {
        auto body = cast(PayloadBody) bodies.front;
        static if (fixedLength)
            if (body.length != bodyWords)
                fatal("fixed payload body length mismatch");
        return PayloadEntry(&hdr, body);
    }

    static if (fixedLength)
    {
        /// The uniform body length supplied at construction.
        @property ulong fixedPayloadLength() const pure nothrow @nogc @safe
        {
            return bodyWords;
        }
    }

    /// Advance the body source by one element.
    void popFront() nothrow @nogc @system { bodies.popFront(); }

    /// Checkpoint the body source when it is a forward range.
    static if (isForwardRange!BR)
    {
        @property typeof(this) save() nothrow @nogc @system
        {
            typeof(this) result = this;
            result.bodies = bodies.save;
            return result;
        }
    }

    static if (hasLength!BR)
    {
        /// The body source's remaining length.
        @property size_t length() nothrow @nogc @system
        {
            return cast(size_t) bodies.length;
        }
    }

    /// Pop up to `n` entries; returns how many were popped.
    size_t popFrontN(size_t n) nothrow @nogc @system
    {
        return bodies.popFrontN(n);
    }

    // Optional transactional-source protocol. The Farm has computed the
    // exact prefix that fits but has not reserved ring space yet. A source
    // may use this point to acquire external resources for only that prefix;
    // false abandons the write without making a table visible.
    static if (__traits(hasMember, BR, "prepareTableFrontN"))
    {
        /// Transactional-source hook: called with the exact table prefix the
        /// Farm will accept; false abandons this write attempt.
        bool prepareTableFrontN(size_t n) nothrow @nogc @system
        {
            return bodies.prepareTableFrontN(n);
        }
    }
}

/// Broadcast one common payload header over every body in `bodies`.
auto broadcastPayloads(BR)(const ref PayloadHeader header, BR bodies)
        nothrow @nogc @system
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    return PayloadBroadcastRange!BR(header, bodies);
}

/// Broadcast one common header over uniformly sized bodies. `bodyWords` is a
/// caller contract: every body must have exactly that many ulongs. This lets
/// `write` size the table without evaluating each body's `front`.
auto broadcastPayloads(BR)(const ref PayloadHeader header, BR bodies,
        ulong bodyWords) nothrow @nogc @system
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    return PayloadBroadcastRange!(BR, true)(header, bodies, bodyWords);
}

/// Spec 4d: producer tiers.
enum Tier : ubyte
{
    /// Short-burst producers; the default tier with the small-table quota.
    small,
    /// Large-table producers with the segment-scale bulk quota.
    bulk,
}

/// Spec 4d: registration ticket carrying the producer's private quota.
/// The hash validates the slot; `farmId` binds the ticket to the Farm that
/// issued it (a process-unique, never-reused identity), so a token presented
/// to any other Farm is fatal at write and unregister; `quotaLeft` is the
/// caller-held mirror of the farm's authoritative ledger (prodSlotQuota).
/// The private fields are deliberately hidden: the caller passes the Token
/// by reference and never reads or writes them;
/// write() validates the mirror against the ledger on every call.
///
/// Tickets start with no quota. Every grant follows an acquire probe of Wt
/// and a successful forward-segment sweep; writes only spend that grant.
///
/// Tokens are single-owner: copying is a *transfer*. The copy constructor
/// copies tier/slot/farmId/quotaLeft, release-stores the valid hash
/// *last* (so a reader that acquire-loads a valid hash is guaranteed to
/// see the other fields), then clears the source's hash so at most one
/// live token exists per slot and a stale copy fails requireToken.
/// Assignment takes the argument by value, which routes through the same
/// transfer copy. Assigning over a still-valid token is fatal: it would lose
/// the only handle to a registered slot. Unregister the old token first.
struct Token
{
    /// The issuing tier; selects the quota validated on every write.
    Tier tier;
    /// Dense slot index within that tier's ticket array.
    uint slot;
    /// Nonzero while the token is valid; opaque, validated against the slot.
    ulong hash;
    /// Issuing Farm's identity (AntFarm.farmId). Binds the ticket to one
    /// Farm for that Farm's lifetime; a mismatch is fatal at every entry
    /// point that consumes a token.
    private ulong farmId;
    private ulong quotaLeft;

    /// True while this token is registered with its Farm.
    bool valid() const pure nothrow @nogc @safe { return hash != 0; }

    private this(Tier t, uint s, ulong h, ulong fid, ulong grant) pure nothrow @nogc @safe
    {
        tier = t;
        slot = s;
        hash = h;
        farmId = fid;
        quotaLeft = grant;
    }

    private void invalidate() nothrow @nogc @system
    {
        hash = 0;
        farmId = 0;
        quotaLeft = 0;
    }

    /// Transfer-only copy: the source is consumed. The destination's valid
    /// hash is release-stored last so a reader that acquire-loads the hash
    /// also sees tier/slot/farmId/quotaLeft; the source hash is then cleared.
    this(ref Token src) nothrow @nogc @system
    {
        tier = src.tier;
        slot = src.slot;
        farmId = src.farmId;
        quotaLeft = src.quotaLeft;
        atomicStore!(MemoryOrder.rel)(*cast(shared ulong*) &hash, src.hash);
        atomicStore!(MemoryOrder.raw)(*cast(shared ulong*) &src.hash, 0UL);
    }

    /// Transfer-only assignment: the by-value argument was already moved
    /// into this temporary by the copy constructor above. The destination
    /// must be invalid (default or unregistered).
    ref Token opAssign(Token src) nothrow @nogc @system
    {
        if (valid)
            fatal("overwrite live producer Token; unregister it first");
        tier = src.tier;
        slot = src.slot;
        farmId = src.farmId;
        quotaLeft = src.quotaLeft;
        atomicStore!(MemoryOrder.raw)(*cast(shared ulong*) &hash, src.hash);
        return this;
    }
}

/// Internal: per-segment statistics (spec 2c). Padded to a cache line.
struct SegStats
{
    /// Internal: epoch currently occupying this segment; -1 = never written.
    shared long es;
    /// Internal: sequence of the first valid payload table for that epoch.
    shared ulong seqt;
    /// Internal: snapshot of the number of consumers.
    shared ulong cs;
    /// Internal: sqcsOf(cs).
    shared ulong sqcs;
    /// Internal: consumed-size accumulator: sum of completed table sizes.
    shared ulong sd;
    /// Internal: cache-line padding.
    ulong[3] pad;
}
static assert(SegStats.sizeof == 64);

/// Table layout offsets (in ulongs, relative to the table's starting
/// sequence), spec 2e:
///   [0..8)                 Thead: 0 Tsent, 1 Tnext, 2 Tmt<<32|Tlen,
///                          3 Cs, 4 SqCs, 5 Tsize, 6 AvgCost,
///                          7 optional TableCompletionHook pointer
///   [8 .. 8+Tlen+Tmt)      Tindex (total index first, MT index second)
///   7 ulongs padding
///   Tprogress (1 ulong)
///   7 ulongs padding
///   Tcount: SqCs counters, each followed by 7 ulongs padding
///   Payloads (each: 16-ulong header + Plen-ulong body; when Tmt == 0,
///             the compact record: Call + Plen-ulong body, 2f)
///   7 ulongs padding
private enum ulong THEAD_LEN = 8;
private enum ulong PROG_PAD = 7;
private enum ulong END_PAD = 7;

/// Overflow-checked addition used by write() size math (spec 5b).
private ulong addChecked(ulong a, ulong b, const(char)[] msg) nothrow @nogc @system
{
    if (b > ulong.max - a) fatal(msg);
    return a + b;
}

private ulong mulChecked(ulong a, ulong b, const(char)[] msg) nothrow @nogc @system
{
    if (a != 0 && b > ulong.max / a) fatal(msg);
    return a * b;
}

private void validatePayloadHeader(const PayloadHeader* header)
        nothrow @nogc @system
{
    if (header is null || header.call is null) fatal("bad payload header");
    if (header.maxCs == 0 || header.maxCs > MAX_PAYLOAD_ITERS)
        fatal("payload MaxCs out of range");
    if (header.done == 0 || header.done > MAX_PAYLOAD_ITERS)
        fatal("payload Done out of range");
    if (header.done > 1 && header.maxCs < 2)
        fatal("multithreaded payload requires MaxCs >= 2");
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

/// Slot i's authoritative remaining quota, one ulong past the hash (same
/// cache line). The farm owns this value; the Token's private quota is only
/// a caller-held mirror. write() validates the mirror against the ledger and
/// syncs the ledger on every reservation and refresh, so a forged token
/// cannot mint blind quota past Exmax.
private shared(ulong)* prodSlotQuota(shared(ulong)* hashes, size_t i) nothrow @nogc @system
{
    return hashes + i * PROD_SLOT_STRIDE + 1;
}

/// Process-global Farm issuance counter. Every created Farm takes one
/// unique, nonzero identity that is never reused for the process lifetime,
/// so a Token carries deterministic provenance: a ticket issued by one Farm
/// (or by a since-destroyed Farm) can never validate against another.
private shared ulong g_farmIssuance;

// ==== Section: AntFarm (construction, registration, production) ====
/// One fixed-capacity Farm: the dual-mapped ring plus its quota, tally and
/// statistics state. Create it with create() and release it with destroy();
/// a Farm is never copied or moved. The public fields are the immutable
/// configuration published at create time and the live shared counters named
/// in SPEC.md; callers read them but never write them.
/// See_Also:
///     SPEC.md (the algorithm and layout), ARCHITECTURE.md (sizing one
///     Farm), docs/reference/actor-lifecycle.md (tokens and provenance)
struct AntFarm
{
    // ---- immutable configuration (spec 1, 4a) ----
    ulong Ln;           /// buffer length in ulongs (power of 2)
    ulong Lmask;        /// Ln - 1
    uint K;             /// number of segments (power of 2)
    ulong kMask;        /// K - 1
    uint segShift;      /// log2(Ln / K)
    ulong segCap;       /// Ln / K segment capacity in ulongs
    uint expectedConsumers; /// configuration hint for epoch 0's Cs/SqCs
    uint maxBulk;           /// configured bulk producer slots
    ulong quotaBulk;    /// Exi for bulk producers
    uint maxSmall;           /// configured small producer slots
    ulong quotaSmall;   /// Exi for small producers
    ulong exmax;        /// Exmax: sum of all producer quotas (spec 4a)
    /// Small-table threshold (spec 7a): 0 selects the auto rule
    /// clamp(SqCs * Chunk, 16, 256); otherwise a fixed override.
    uint smallThreshold;
    /// Process-unique Farm identity, assigned once in create() from
    /// g_farmIssuance and never changed. Producer Tokens carry a copy;
    /// requireToken and unregisterProducer reject foreign tickets fatally.
    ulong farmId;

    // ---- magic buffer ----
    shared(ulong)* buf; /// 2*Ln ulongs mapped; second half mirrors the first
    ulong bufBytes;     /// Ln * 8
    /// Windows SEC_LARGE_PAGES was mapped, or Linux MADV_HUGEPAGE advice was
    /// applied. Linux promotion remains kernel-controlled.
    bool usedLargePages;

    // ---- farm-level mutable metadata (spec 2a), one cache line each ----
    align(64) shared ulong Wt;      /// write tail sequence
    align(64) shared long Cf;       /// current number of subscribed consumers
    align(64) shared ulong Reqs_c;  /// consumer subscription counter; source of IDc
    align(64) shared ulong Reqs_p;  /// producer registration counter; Token hash input
    align(64) shared long Prbulk;   /// current number of bulk producers
    align(64) shared long Prsm;     /// current number of small producers

    // ---- producer tickets (spec 4d); 0 = free slot ----
    /// Internal: bulk ticket array: slot hashes and quotas.
    shared(ulong)* prodHashBulk;
    /// Internal: small ticket array: slot hashes and quotas.
    shared(ulong)* prodHashSmall;
    private ProducerMutex producerMutex;

    // ---- segment metadata (spec 2b/2c) ----
    /// Internal: root tallies, one per segment.
    align(64) shared ulong[8][KMAX] Rt;
    /// Internal: leaf tallies.
    align(64) shared long[8][KMAX * MAX_LEAVES] Lt;
    /// Internal: segment statistics.
    align(64) SegStats[KMAX] stats;

    // ------------------------------------------------------------------
    // Construction / destruction
    // ------------------------------------------------------------------

    /// `ringMiB` is the ring size in MiB: a power of two, at least
    /// MIN_RING_MIB (2) and at most MAX_RING_MIB. The Farm's internal length
    /// `Ln` is that size in ulongs (`ringMiB << 17`); quotas stay in ulongs.
    /// Ordinary 4 KiB backing is the default. Pass `hugePages=true` or set
    /// `ANTFARM_HUGE_PAGES=1` to opt into the platform huge-page path.
    /// Distinct Farms may be created concurrently. Windows mapping API
    /// initialization is process-locked; the mapping/allocation work is local.
    static AntFarm* create(uint ringMiB = 8, uint k = 8, uint expectedConsumers = 4,
                           uint maxBulk = 2, ulong quotaBulk = 0,
                           uint maxSmall = 16, ulong quotaSmall = 4096,
                           uint smallThreshold = DEFAULT_SMALL_TABLE_THRESHOLD,
                           bool hugePages = DEFAULT_HUGE_PAGES) nothrow @nogc @system
    {
        // Construction constraints (spec 1, 4a):
        //  - K is the useful power-of-two range [2, KMAX=16].
        //  - ringMiB >= 2 (Ln >= 2^18 ulongs); smaller rings were never
        //    studied and per-table header overhead would dominate. The upper
        //    bound mainly catches a legacy caller passing a length in ulongs.
        //  - segCap = Ln/K has a floor so header/pad space never dominates a
        //    segment's capacity (subsumed by Ln >= 2^18 with K <= 16).
        //  - Exmax <= (K-1)*segCap so a full quota excursion is strictly
        //    less than a lap (spec 3 safety argument).
        //  - quotaRole > 0 iff maxRole > 0: an enabled role must declare a
        //    positive quota; a disabled role's quota is normalized to 0 and
        //    takes no part in Exmax.
        if (k < 2 || k > KMAX || (k & (k - 1)) != 0) fatal("K must be a power of 2 in [2, KMAX]");
        if (ringMiB < MIN_RING_MIB || (ringMiB & (ringMiB - 1)) != 0)
            fatal("ringMiB must be a power of 2 >= 2 (the ring size is in MiB, not ulongs)");
        if (ringMiB > MAX_RING_MIB)
            fatal("ringMiB exceeds MAX_RING_MIB (the ring size is in MiB, not ulongs)");
        immutable ln = cast(ulong) ringMiB << ULONGS_PER_MIB_SHIFT;
        immutable segCap = ln / k;
        if (segCap < 2048) fatal("segment capacity too small");
        if (expectedConsumers == 0) fatal("expected consumers must be > 0");
        // Quota-role rule (spec 4d): normalize disabled roles, validate
        // enabled ones. Bulk auto-defaults to segCap when enabled with an
        // unspecified quota (legacy convenience); small fatals on 0.
        if (maxBulk == 0)
            quotaBulk = 0;
        else if (quotaBulk == 0)
            quotaBulk = segCap;
        if (maxSmall == 0)
            quotaSmall = 0;
        else if (quotaSmall == 0)
            fatal("small quota must be > 0");
        if (maxBulk + maxSmall == 0) fatal("no producer capacity configured");
        immutable exmax = cast(ulong) maxBulk * quotaBulk + cast(ulong) maxSmall * quotaSmall;
        if (maxBulk > 0 && quotaBulk > exmax) fatal("quota exceeds Exmax");
        if (maxSmall > 0 && quotaSmall > exmax) fatal("quota exceeds Exmax");
        if (exmax > (k - 1) * segCap) fatal("Exmax exceeds K-1 segments' capacities");

        immutable bytes = segCap * k * 8;
        hugePages = wantHugePages(hugePages);
        auto p = mapMagicBuffer(bytes, hugePages);

        auto mem = afAlignedAlloc(AntFarm.sizeof);
        if (mem is null) fatal("alloc failed");
        auto f = cast(AntFarm*) mem;
        *f = AntFarm.init;
        f.producerMutex.initialize();
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
        f.smallThreshold = smallThreshold;
        f.farmId = atomicFetchAdd!(MemoryOrder.raw)(g_farmIssuance, 1UL) + 1;
        f.buf = cast(shared(ulong)*) p;
        f.bufBytes = bytes;
        f.usedLargePages = hugePages;
        if (maxBulk > 0)
        {
            // 64-aligned so slot 0's line cannot bleed into an unrelated
            // heap chunk; zeroed because aligned_alloc does not zero.
            f.prodHashBulk = cast(shared(ulong)*)
                afAlignedAlloc(maxBulk * PROD_SLOT_STRIDE * 8);
            if (f.prodHashBulk is null) fatal("alloc producer tickets failed");
            memset(cast(void*) f.prodHashBulk, 0, maxBulk * PROD_SLOT_STRIDE * 8);
        }
        if (maxSmall > 0)
        {
            f.prodHashSmall = cast(shared(ulong)*)
                afAlignedAlloc(maxSmall * PROD_SLOT_STRIDE * 8);
            if (f.prodHashSmall is null) fatal("alloc producer tickets failed");
            memset(cast(void*) f.prodHashSmall, 0, maxSmall * PROD_SLOT_STRIDE * 8);
        }

        foreach (i; 0 .. KMAX)
            atomicStore!(MemoryOrder.raw)(f.stats[i].es, -1L);
        // Spec 4b: epoch 0's Rt is constructed with a dummy Sub0 value,
        // blocking producers from overwriting Seq 0 until consumers digest it.
        atomicStore!(MemoryOrder.raw)(f.Rt[0][0], SUB0);
        atomicStore!(MemoryOrder.raw)(f.stats[0].es, 0L);
        atomicStore!(MemoryOrder.raw)(f.stats[0].seqt, 0UL);
        atomicStore!(MemoryOrder.raw)(f.stats[0].cs, cast(ulong) expectedConsumers);
        atomicStore!(MemoryOrder.raw)(f.stats[0].sqcs, cast(ulong) sqcsOf(expectedConsumers));
        return f;
    }

    /// Release the Farm. Fatal unless every consumer has unsubscribed and
    /// every producer token has been unregistered; the caller must join all
    /// threads that touched the Farm first. Unmaps the ring, frees the
    /// ticket arrays and the Farm storage itself.
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
            afAlignedFree(cast(void*) prodHashBulk);
        if (prodHashSmall !is null)
            afAlignedFree(cast(void*) prodHashSmall);
        unmapMagicBuffer(cast(void*) buf, bufBytes);
        producerMutex.destroy();
        afAlignedFree(cast(void*) &this);
    }

    // ------------------------------------------------------------------
    // Registration (spec 4c, 4d, 6b, 6e)
    // ------------------------------------------------------------------

    /// Internal: subscribe half used by ConsumerView.subscribe. fetch-inc Cf;
    /// on success fetch-inc Reqs_c and return it (the IDc).
    /// Returns SUBSCRIBE_FULL if oversubscribed.
    long add_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchAdd!(MemoryOrder.raw)(Cf, 1);
        if (prev >= MAX_CONSUMERS_LIMIT)
        {
            atomicFetchSub!(MemoryOrder.raw)(Cf, 1);
            return SUBSCRIBE_FULL;
        }
        return cast(long) atomicFetchAdd!(MemoryOrder.raw)(Reqs_c, 1);
    }

    /// Internal: unsubscribe half used by ConsumerView.unsubscribe.
    /// Returns the previous Cf; when it returns 1 the caller is the last
    /// unsubscriber (spec 6e).
    long sub_consumer() nothrow @nogc @system
    {
        immutable prev = atomicFetchSub!(MemoryOrder.raw)(Cf, 1);
        if (prev <= 0) fatal("Cf underflow");
        return prev;
    }

    /// Spec 4d: allocate a ticketed slot. An invalid Token means the tier is full.
    Token registerProducer(Tier t) nothrow @nogc @system
    {
        producerMutex.lock();
        scope (exit) producerMutex.unlock();
        immutable cap = t == Tier.bulk ? maxBulk : maxSmall;
        auto pr = t == Tier.bulk ? &Prbulk : &Prsm;
        auto hashes = t == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (cap == 0 || hashes is null)
            return Token.init;
        immutable p = atomicFetchAdd!(MemoryOrder.raw)(*pr, 1);
        if (p >= cap)
        {
            atomicFetchSub!(MemoryOrder.raw)(*pr, 1);
            return Token.init;
        }
        immutable reqs = atomicFetchAdd!(MemoryOrder.raw)(Reqs_p, 1);
        foreach (i; 0 .. cap)
        {
            immutable h = mixToken(cast(uint) i, reqs);
            if (cas!(MemoryOrder.raw, MemoryOrder.raw)(prodSlot(hashes, i), 0UL, h))
            {
                auto tok = Token(t, cast(uint) i, h, farmId, 0);
                atomicStore!(MemoryOrder.raw)(*prodSlotQuota(hashes, i), 0UL);
                return tok;
            }
        }
        fatal("producer slot missing");
        return Token.init;
    }

    /// Spec 4d: a Token that does not match the live slot is fatal.
    void unregisterProducer(ref Token tok) nothrow @nogc @system
    {
        producerMutex.lock();
        scope (exit) producerMutex.unlock();
        if (!tok.valid) fatal("unregister invalid token");
        if (tok.farmId != farmId) fatal("unregister token from another farm");
        immutable cap = tok.tier == Tier.bulk ? maxBulk : maxSmall;
        auto hashes = tok.tier == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (hashes is null || tok.slot >= cap) fatal("unregister bad slot");
        if (atomicLoad!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot)) != tok.hash)
            fatal("unregister token mismatch");
        atomicStore!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot), 0UL);
        atomicStore!(MemoryOrder.raw)(*prodSlotQuota(hashes, tok.slot), 0UL);
        if (tok.tier == Tier.bulk)
        {
            if (atomicFetchSub!(MemoryOrder.raw)(Prbulk, 1) <= 0) fatal("Prbulk underflow");
        }
        else
        {
            if (atomicFetchSub!(MemoryOrder.raw)(Prsm, 1) <= 0) fatal("Prsm underflow");
        }
        // Zero the token so a second unregister is a clean no-op failure,
        // and any write with a stale ticket is rejected by requireToken.
        tok.invalidate();
    }

    /// Spec 4d: write() fatals unless tok matches a live slot of its tier
    /// and its quota mirror does not exceed the farm's authoritative ledger.
    private void requireToken(ref Token tok) nothrow @nogc @system
    {
        if (!tok.valid) fatal("write requires a producer token");
        if (tok.farmId != farmId) fatal("write token from another farm");
        immutable cap = tok.tier == Tier.bulk ? maxBulk : maxSmall;
        auto hashes = tok.tier == Tier.bulk ? prodHashBulk : prodHashSmall;
        if (hashes is null || tok.slot >= cap) fatal("write bad token slot");
        if (atomicLoad!(MemoryOrder.raw)(*prodSlot(hashes, tok.slot)) != tok.hash)
            fatal("write token mismatch");
        immutable ledger = atomicLoad!(MemoryOrder.raw)(*prodSlotQuota(hashes, tok.slot));
        if (tok.quotaLeft > ledger)
            fatal("write quota exceeds farm ledger (forged token)");
    }

    /// Push the token's quota mirror into the authoritative ledger. Called
    /// after every quota mutation in write() (swept grant and reservation)
    /// so the next call's requireToken stays valid.
    private void syncQuota(ref Token tok) nothrow @nogc @system
    {
        auto hashes = tok.tier == Tier.bulk ? prodHashBulk : prodHashSmall;
        atomicStore!(MemoryOrder.raw)(*prodSlotQuota(hashes, tok.slot), tok.quotaLeft);
    }

    /// Bits 16..31 of Rtlow: Sub0 units. At rest 0 or 1; transiently more.
    private enum ulong SUB0MASK = LOWMASK ^ COUNTMASK;

    /// Plant one Sub0 on an incomplete epoch with count 0 and no pulse
    /// (spec 6c). Retain the defensive post-CAS completeness recheck and
    /// masked retract, although producer ownership rules out confirmation
    /// of its start epoch before this call returns.
    /// Producer-only: the caller's unpublished table prevents e from
    /// confirming/recycling before this plant. Never call from an unpinned
    /// scan after unsubscribe; a zero->count->zero ABA can resurrect a slot.
    private void plantIfUnprotected(ulong e) nothrow @nogc @system
    {
        immutable ki = cast(uint)(e & kMask);
        if (atomicLoad!(MemoryOrder.acq)(stats[ki].es) != cast(long) e)
            return;
        immutable ei1 = e + 1;
        immutable ki1 = cast(uint)(ei1 & kMask);
        // Acquire pairs with the producer's release-store of Es[ki1], so a
        // matching epoch also orders the Seqt[ki1] read below (valid Es
        // implies valid header metadata, as with the table sentinel).
        if (atomicLoad!(MemoryOrder.acq)(stats[ki1].es) != cast(long) ei1)
            return;
        immutable seqt = atomicLoad!(MemoryOrder.raw)(stats[ki].seqt);
        immutable seqtN = atomicLoad!(MemoryOrder.raw)(stats[ki1].seqt);
        immutable sd = atomicLoad!(MemoryOrder.raw)(stats[ki].sd);
        if (seqt >= seqtN || seqt + sd >= seqtN)
            return;
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(&this, WriteAuditPhase.plantChecked, e, 0);
        // Spec 6c: CAS the plant so a racing 0->1 cannot be straddled by a
        // bare fetch_add(SUB0). Count or pulse appearing bails — the race
        // invalidated the plant. Not an open loop; strong CAS (spec 1).
        for (;;)
        {
            immutable rt = atomicLoad!(MemoryOrder.acq)(Rt[ki][0]);
            if ((rt & COUNTMASK) != 0 || (rt & SUB0MASK) != 0)
                return;
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&Rt[ki][0], rt, rt + SUB0))
            {
                version (AntfarmWriteAuditHooks)
                    if (writeAuditHook !is null)
                        writeAuditHook(&this, WriteAuditPhase.plantPublished, e, 0);
                // Spec 6c defensive post-CAS retract. Producer ownership
                // prevents confirmation during a supported plant; this check
                // alone would not make an unpinned caller safe.
                // Retract exactly the unit we planted. The full-word CAS is
                // masked to the low half: a concurrent subscriber's SUB
                // pin/unpin changes the high half and would make a one-shot
                // exact-value CAS fail while our SUB0 is still present.
                // The retry is bounded by the low-half state -- it is either
                // exactly SUB0 (we remove it) or a racing 0->1 has already
                // changed it (and that same edge clears the pulse).
                immutable sd2 = atomicLoad!(MemoryOrder.raw)(stats[ki].sd);
                immutable seqt2 = atomicLoad!(MemoryOrder.raw)(stats[ki].seqt);
                immutable seqtN2 = atomicLoad!(MemoryOrder.raw)(stats[ki1].seqt);
                if (seqt2 + sd2 >= seqtN2)
                {
                    for (;;)
                    {
                        immutable cur = atomicLoad!(MemoryOrder.acq)(Rt[ki][0]);
                        if ((cur & LOWMASK) != SUB0)
                            return;
                        if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&Rt[ki][0], cur, cur - SUB0))
                            return;
                    }
                }
                return;
            }
        }
    }

    // ------------------------------------------------------------------
    // Production (spec 5)
    // ------------------------------------------------------------------

    /// Spec 5a: sweep contiguously-forward zero Rts from `anchor`.
    /// `anchor` is a fresh acquire-load of Wt for this grant. Reservations of
    /// Wt are acq_rel, so the load synchronizes with a published tail, and a
    /// later reservation spending this grant forwards the sweep's acquired
    /// reclamation history to every producer that reserves after it.
    private bool refreshQuota(ref ulong exi, ulong quota, ulong anchor) nothrow @nogc @system
    {
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(&this, WriteAuditPhase.anchorProbed, anchor, quota);
        immutable ea = anchor >> segShift;
        ulong freeSp = 0;
        foreach (j; 1 .. K)
        {
            immutable ki = (ea + j) & kMask;
            if (atomicLoad!(MemoryOrder.acq)(Rt[ki][0]) == 0)
            {
                freeSp += segCap;
            }
            else
                break;
            if (freeSp >= exmax)
                break;
        }
        if (freeSp >= exmax)
        {
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(&this, WriteAuditPhase.quotaSwept, anchor, quota);
            exi = quota;
            return true;
        }
        return false;
    }

    /// Diagnostic tripwire, not a reservation protocol. Wt has already
    /// advanced, but no metadata or body from this reservation is written.
    /// Check the exact metadata-transition range, including a tail landing
    /// exactly on a boundary. Epoch zero is not fresh (it starts with Sub0).
    /// Do not use wtprime-1 here: an exact landing initializes the next
    /// segment's metadata even though its body storage is not written yet.
    /// A concurrent subscription can add a provisional SUB after the sweep.
    /// Ignore that high half, but both active roots and Sub0 remain fatal.
    private void checkFreshWriteSegments(ulong wret, ulong wtprime)
        nothrow @nogc @system
    {
        foreach (e; (wret >> segShift) + 1 .. (wtprime >> segShift) + 1)
        {
            immutable ki = e & kMask;
            immutable rt = atomicLoad!(MemoryOrder.acq)(Rt[ki][0]);
            immutable low = rt & LOWMASK;
            if (low == 0) continue;
            immutable roots = low & COUNTMASK;
            immutable pulses = (low & SUB0MASK) / SUB0;
            immutable kind = roots != 0
                ? (pulses != 0 ? "roots+Sub0" : "roots") : "Sub0";
            fprintf(stderr,
                "allWriteCheck: protected fresh segment kind=%s wret=%llu wtprime=%llu size=%llu e=%llu ki=%llu Rt=%llx roots=%llu Sub0=%llu Sub=%llu Wt=%llu Exmax=%llu K=%u\n",
                kind.ptr, wret, wtprime, wtprime - wret, e, ki, rt,
                roots, pulses, rt / SUB,
                atomicLoad!(MemoryOrder.raw)(Wt), exmax, K);
            // This is a best-effort concurrent snapshot, not an atomic one.
            foreach (uint s; 0 .. K)
            {
                fprintf(stderr, "  ki=%u rt=%llx es=%lld seqt=%llu sd=%llu leaves=",
                    s, atomicLoad!(MemoryOrder.raw)(Rt[s][0]),
                    atomicLoad!(MemoryOrder.raw)(stats[s].es),
                    atomicLoad!(MemoryOrder.raw)(stats[s].seqt),
                    atomicLoad!(MemoryOrder.raw)(stats[s].sd));
                foreach (uint l; 0 .. MAX_LEAVES)
                    fprintf(stderr, "%s%lld", (l == 0 ? "" : ",").ptr,
                        atomicLoad!(MemoryOrder.raw)(Lt[s * MAX_LEAVES + l][0]));
                fprintf(stderr, "\n");
            }
            if (roots != 0 && pulses != 0)
                fatal("allWriteCheck: fresh segment has live roots and Sub0 pulses");
            if (roots != 0)
                fatal("allWriteCheck: fresh segment has live roots");
            fatal("allWriteCheck: fresh segment has Sub0 pulses");
        }
    }

    /// Spec 5b. Writes as many of `payloads` as fit the caller's remaining
    /// quota (carried inside `tok`); returns the number written. Returns 0
    /// (full) when backpressure is too high; callers retry or wait on
    /// consumers. `tok` is required, and is passed by reference so the farm
    /// can maintain the quota state; the caller must never inspect or copy
    /// the token's private quota. `avgCost` is the producer's declared Call
    /// cost class, a log2 shift in 0 .. MAX_AVG_COST (spec 5b / 7a).
    ulong write(scope PayloadEntry[] payloads, ref Token tok,
                uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
    {
        return writeImpl(payloads, tok, avgCost, null);
    }

    /// ditto: forward range overload. `write` checkpoints the source for its
    /// sizing and emission passes and never advances the caller's checkpoint.
    ulong write(R)(scope R payloads, ref Token tok,
                   uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
        if (isForwardRange!R && is(ElementType!R == PayloadEntry))
    {
        return writeImpl(payloads, tok, avgCost, null);
    }

    /// Write jobs from independent header and body input ranges.  Pairing is
    /// positional and stops at the shorter range.  This overload is a lazy
    /// shim: it does not allocate or materialize `PayloadEntry` pairs.
    ulong write(HR, BR)(scope HR headers, scope BR bodies, ref Token tok,
                        uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
        if (isForwardRange!HR && isForwardRange!BR &&
            (is(Unqual!(ElementType!HR) == PayloadHeader) ||
             is(ElementType!HR : const(PayloadHeader)*)) &&
            is(ElementType!BR : PayloadBody))
    {
        return writeImpl(pairPayloads(headers, bodies), tok, avgCost, null);
    }

    /// Broadcast one common header over a forward range of bodies. The
    /// common metadata is validated once per call rather than once per body.
    ulong write(BR)(const ref PayloadHeader header, scope BR bodies,
                    ref Token tok, uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
        if (isForwardRange!BR && is(ElementType!BR : PayloadBody))
    {
        return writeImpl(broadcastPayloads(header, bodies), tok, avgCost,
            null);
    }

    /// Broadcast a common header over bodies whose uniform length is supplied
    /// by contract. This selects arithmetic sizing even when the body range's
    /// element type is a runtime slice.
    ulong write(BR)(const ref PayloadHeader header, scope BR bodies,
                    ulong bodyWords, ref Token tok,
                    uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
        if (isForwardRange!BR && is(ElementType!BR : PayloadBody))
    {
        return writeImpl(broadcastPayloads(header, bodies, bodyWords), tok,
            avgCost, null);
    }

    /// Broadcast a uniform single-shot payload range into one physical table
    /// and notify `completion` when all callbacks in that table have returned.
    /// This is intentionally narrower than `write`: aggregate users such as
    /// actor waves must count the table before calling, because consumers may
    /// complete it before this function returns.
    ulong writeTracked(BR)(const ref PayloadHeader header, scope BR bodies,
                    ulong bodyWords, TableCompletionHook* completion,
                    ref Token tok, uint avgCost = DEFAULT_AVG_COST) nothrow @nogc @system
        if (isForwardRange!BR && is(ElementType!BR : PayloadBody))
    {
        if (completion is null || !completion.valid)
            fatal("tracked table requires a completion hook");
        if (header.maxCs != 1 || header.done != 1)
            fatal("tracked table requires single-shot payloads");
        return writeImpl(broadcastPayloads(header, bodies, bodyWords), tok,
            avgCost, completion);
    }

    private ulong writeImpl(R)(scope R payloads, ref Token tok,
                              uint avgCost,
                              TableCompletionHook* completion)
        nothrow @nogc @system
    {
        requireToken(tok);
        if (payloads.empty) return 0;
        if (avgCost > MAX_AVG_COST) fatal("avgCost out of range");
        immutable quota = tok.tier == Tier.bulk ? quotaBulk : quotaSmall;
        immutable csl = atomicLoad!(MemoryOrder.raw)(Cf);
        immutable cs = csl > 0 ? cast(uint) csl : 1;
        immutable sq = sqcsOf(cs);

        // Fit as many payloads as the quota and the table payload cap
        // allow (spec 5b truncation, 2e).
        // Base size of every table: Thead + progress pads + Tcount + end pad.
        immutable base = THEAD_LEN + PROG_PAD + 1 + 7 + 8UL * sq + END_PAD;
        // Spec 2e: a committed table carries fewer than 2^31 payloads.
        immutable size_t maxTablePayloads = 2_147_483_647; // 2^31 - 1
        size_t n;
        uint m;
        ulong psum;

        static if (hasCommonPayloadHeader!R)
        {
            auto commonHeader = payloads.commonPayloadHeader;
            validatePayloadHeader(commonHeader);
        }

        for (;;)
        {
            n = 0; m = 0; psum = 0;
            static if (hasCommonPayloadHeader!R && hasFixedPayloadLength!R)
            {
                immutable bodyLen = cast(ulong) payloads.fixedPayloadLength;
                if (bodyLen > ulong.max - PHEAD_LEN)
                    fatal("payload size overflow");
                immutable oneMt = commonHeader.maxCs > 1 ? 1UL : 0UL;
                // Spec 2f: a uniform ST range always forms compact tables.
                immutable psz = (oneMt != 0 ? PHEAD_LEN : PCOMPACT_LEN) + bodyLen;
                immutable singletonBase = base + 1UL + oneMt;
                if (psz > ulong.max - singletonBase)
                    fatal("table size overflow");
                if (singletonBase + psz > quota)
                    fatal("payload larger than this producer tier's Exi");

                immutable unit = addChecked(psz, 1UL + oneMt,
                    "table size overflow");
                static if (hasLength!R)
                {
                    immutable available = cast(size_t) payloads.length;
                    immutable maxFit = tok.quotaLeft > base
                        ? (tok.quotaLeft - base) / unit : 0;
                    n = available;
                    if (cast(ulong) n > maxFit)
                        n = cast(size_t) maxFit;
                    if (n > maxTablePayloads) n = maxTablePayloads;
                    m = oneMt != 0 ? cast(uint) n : 0;
                    psum = mulChecked(cast(ulong) n, psz,
                        "payload sum overflow");
                }
                else
                {
                    auto scan = payloads.save;
                    while (!scan.empty)
                    {
                        if (n >= maxTablePayloads) break;
                        immutable nextN = cast(ulong) n + 1;
                        immutable cand = addChecked(base,
                            mulChecked(nextN, unit, "table size overflow"),
                            "table size overflow");
                        if (cand > tok.quotaLeft) break;
                        ++n;
                        scan.popFront();
                    }
                    if (n > maxTablePayloads) fatal("too many payloads in table");
                    m = oneMt != 0 ? cast(uint) n : 0;
                    psum = mulChecked(cast(ulong) n, psz,
                        "payload sum overflow");
                }
            }
            else
            {
                size_t i = 0;
                // Spec 2f: the prefix is compact while it has no MT payload,
                // so track both payload sums and size by the prefix's format.
                ulong psumFull, psumCompact;
                auto scan = payloads.save;
                foreach (ref pe; scan)
                {
                    if (i >= maxTablePayloads) break;
                    static if (hasCommonPayloadHeader!R)
                        auto hp = commonHeader;
                    else
                    {
                        auto hp = pe.header;
                        validatePayloadHeader(hp);
                    }
                    if (pe.body.length > ulong.max - PHEAD_LEN)
                        fatal("payload size overflow");
                    immutable psz = PHEAD_LEN + pe.body.length;
                    immutable cpsz = PCOMPACT_LEN + pe.body.length;
                    immutable oneMt = hp.maxCs > 1 ? 1UL : 0UL;
                    // A singleton that exceeds this tier can never be
                    // published by this producer, so it is a contract error.
                    // An ST singleton is a compact table (spec 2f).
                    immutable singletonBase = base + 1UL + oneMt;
                    immutable singletonPsz = oneMt != 0 ? psz : cpsz;
                    if (singletonPsz > ulong.max - singletonBase)
                        fatal("table size overflow");
                    if (singletonBase + singletonPsz > quota)
                        fatal("payload larger than this producer tier's Exi");
                    immutable cm = m + (hp.maxCs > 1 ? 1 : 0);
                    immutable psumFullNext = addChecked(psumFull, psz,
                        "payload sum overflow");
                    immutable psumCompactNext = psumCompact + cpsz; // <= psumFullNext
                    immutable psumNext = cm == 0 ? psumCompactNext : psumFullNext;
                    immutable fixed = base + (cast(ulong) i + 1) + cm;
                    if (fixed > ulong.max - psumNext)
                        fatal("table size overflow");
                    immutable cand = fixed + psumNext;
                    if (cand > tok.quotaLeft) break;
                    n = i + 1;
                    m = cast(uint) cm;
                    psum = psumNext;
                    psumFull = psumFullNext;
                    psumCompact = psumCompactNext;
                    ++i;
                }
            }
            if (n > 0) break;
            if (!refreshQuota(tok.quotaLeft, quota,
                    atomicLoad!(MemoryOrder.acq)(Wt))) return 0;
            syncQuota(tok); // ledger follows a successful refresh
        }

        immutable size = tableSizeChecked(n, m, sq, psum);

        static if (__traits(hasMember, R, "prepareTableFrontN"))
            if (!payloads.prepareTableFrontN(n)) return 0;

        // Reserve space on the write tail (spec 5a, 5c, 9). acq_rel, not
        // release: a producer spending retained quota without a fresh sweep
        // must acquire the reclamation history (consumer pin drops -> Rt
        // release -> zero-Rt sweep acquire) that other producers imported
        // before advancing Wt past the words it is about to store.
        immutable wret = atomicFetchAdd!(MemoryOrder.acq_rel)(Wt, size);
        immutable wtprime = wret + size;
        immutable eold = wret >> segShift;
        immutable enew = wtprime >> segShift;
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(&this, WriteAuditPhase.tailReserved, wret, wtprime);
        if (eold != enew)
            checkFreshWriteSegments(wret, wtprime);
        tok.quotaLeft -= size;
        syncQuota(tok);

        // Segment/epoch transitions (spec 2c): initialize metadata for each
        // crossed segment, release-storing Es last.
        foreach (e; eold + 1 .. enew + 1)
        {
            immutable ki = e & kMask;
            atomicStore!(MemoryOrder.raw)(stats[ki].seqt, wtprime);
            atomicStore!(MemoryOrder.raw)(stats[ki].cs, cast(ulong) cs);
            atomicStore!(MemoryOrder.raw)(stats[ki].sqcs, cast(ulong) sq);
            atomicStore!(MemoryOrder.raw)(stats[ki].sd, 0UL); // fresh epoch: nothing consumed
            atomicStore!(MemoryOrder.rel)(stats[ki].es, cast(long) e);
        }

        // Spec 5c/6c: a write that crossed past Ki made Ki unable to accept
        // more tables. If no consumer ever entered it, last-releaser never
        // runs and Rt stays 0 — plant Sub0 so the next subscribe walk can
        // find it.
        foreach (e; eold + 1 .. enew + 1)
            plantIfUnprotected(e - 1);

        // Write the table (spec 5c/2e). The magic buffer makes the linear
        // address range contiguous even across the Ln wrap point. Ring words
        // are stored raw: consumers load-raw after acquiring Tsent, and a
        // later lap reuses the same physical words.
        auto w = buf + (wret & Lmask);
        immutable progOff = THEAD_LEN + n + m + PROG_PAD;
        immutable tcountOff = progOff + 8;
        immutable payOff = tcountOff + 8 * sq;

        storeRaw(w[1], wtprime);                          // Tnext
        storeRaw(w[2], (cast(ulong) m << 32) | cast(uint) n);  // Tmt | Tlen
        storeRaw(w[3], cs);
        storeRaw(w[4], sq);
        storeRaw(w[5], size); // table size, accounted into the segment's Sd on completion
        storeRaw(w[6], avgCost); // chunk hint: chunk = MAX_CHUNK >> avgCost (spec 7a)
        storeRaw(w[7], cast(ulong) cast(void*) completion);

        storeRaw(w[progOff], 0);                          // Tprogress
        storeRawRange(w + tcountOff, 8 * sq);             // Tcount shards

        // Emit both index sections and payload storage in one checkpointed
        // traversal. Header fields are ring words, not a PayloadHeader
        // overlay: overlay field writes would be plain stores.
        ulong po = payOff;
        size_t mi = THEAD_LEN + n;
        size_t i = 0;
        auto emit = payloads.save;
        // Spec 2f: an all-ST table (Tmt == 0) stores [Call][Pbody] records.
        if (m == 0)
        {
            while (i < n)
            {
                auto pe = emit.front;
                static if (hasCommonPayloadHeader!R)
                    auto hp = commonHeader;
                else
                    auto hp = pe.header;

                storeRaw(w[THEAD_LEN + i], po);
                auto ph = w + po;
                storeRaw(ph[0], cast(ulong) cast(void*) hp.call);
                foreach (k; 0 .. pe.body.length)
                    storeRaw(ph[PCOMPACT_LEN + k], pe.body[k]);
                po += PCOMPACT_LEN + pe.body.length;
                ++i;
                emit.popFront();
            }
        }
        while (i < n)
        {
            auto pe = emit.front;
            static if (hasCommonPayloadHeader!R)
                auto hp = commonHeader;
            else
                auto hp = pe.header;

            storeRaw(w[THEAD_LEN + i], po);
            if (hp.maxCs > 1)
                storeRaw(w[mi++], po);

            auto ph = w + po;
            storeRaw(ph[0], cast(ulong) hp.maxCs | (cast(ulong) hp.done << 32));
            storeRaw(ph[1], pe.body.length);
            storeRawRange(ph + 2, 6);
            storeRaw(ph[8], 0UL); // pcount
            storeRaw(ph[9], cast(ulong) cast(void*) hp.call);
            storeRawRange(ph + 10, 6);
            foreach (k; 0 .. pe.body.length)
                storeRaw(ph[PHEAD_LEN + k], pe.body[k]);
            po += PHEAD_LEN + pe.body.length;
            ++i;
            emit.popFront();
        }
        // The compact reader derives the last Plen from this end point.
        assert(po == size - END_PAD);
        storeRawRange(w + (size - END_PAD), END_PAD);

        // The sentinel is store-released as the last value written, so that
        // consumers validating the expected sequence also validate the table
        // contents (spec 5c).
        atomicStore!(MemoryOrder.rel)(buf[wret & Lmask], sentinelOf(wret));
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(&this, WriteAuditPhase.tablePublished, wret, wtprime);
        return n;
    }
}

// ==== Section: ConsumerView (claims, dispatch, reclamation) ====
/// Unique, non-copyable cursor exclusively owned by one consumer at a time.
/// See docs/guide/quickstart.md for who pumps and consumes, and
/// docs/reference/actor-lifecycle.md for the completion contract.
///
/// Held pins are a contiguous epoch range [oldestEi, newestEi] (equal when
/// freshly subscribed). newestEi is the position: the segment of the last
/// validated table, or, when idle at Wt, the initialized frontier of
/// nextSeq. oldestEi advances as confirmed prefix segments are released
/// (8). ltiRing[ki] is the leaf actually incremented at that physical
/// slot; IDc nudges do not rewrite it. Confirmation (8): Es[Ki]==Ei,
/// the next segment is initialized for Ei+1, and Seqt[Ki]+Sd[Ki] >= Seqt[Ki+1].
///
/// Invariants:
///  - Every subscribed consumer always holds at least one pin; an idle
///    consumer advances newestEi onto nextSeq's initialized segment.
///  - The last *releaser* of an unconfirmed segment plants Sub0 via the
///    last-releaser CAS (2b/6c). First consumer of a segment (count 0→1)
///    subtracts it.
///  - Farm-empty with everything confirmed still plants exactly one pulse,
///    using the same last-releaser helper on the confirmed position.
struct ConsumerView
{
    @disable this(this);

    /// Internal: the subscribed Farm; null while unsubscribed.
    AntFarm* F;
    /// Internal: from F.Reqs_c; targeting hint, not a leaf retarget (4c/6a; nudges 7d/7e)
    ulong IDc;
    /// Internal: sequence of the next table to read.
    ulong nextSeq;
    /// Internal: first still-held epoch.
    ulong oldestEi;
    /// Internal: position epoch.
    ulong newestEi;
    /// Internal: Lti actually incremented at physical ki.
    uint[KMAX] ltiRing;
    /// Internal: true between successful subscribe and unsubscribe.
    bool hasRef;

    /// Internal: carried sweeper role: set when this consumer completed a
    /// shard of the previous table. Small tables concentrate all claims on
    /// shard 0 (7a), so under churn they may have no native consumer at all;
    /// a carried sweeper sweeps a small next table regardless of its own
    /// shard assignment.
    bool sweeperNext;

    /// Internal: idle-path re-walk cursor (spec 8): sequence up to which the
    /// current position segment has been drained. A park resumes from here —
    /// O(new tables) per miss rather than O(tables in the segment). Reset
    /// when the position moves to a new segment.
    ulong sweepSeq;

    /// Internal: oldest-prefix release counter. With -d-version=SdCheckEveryN,
    /// tryReleaseOldest() runs every SD_CHECK_EVERY_N table advances
    /// instead of after every table. Default N = 1 (eager).
    uint sdCheckCounter;

    /// Internal: physical slot of the current position epoch.
    uint posKi() nothrow @nogc @system
    {
        return cast(uint)(newestEi & F.kMask);
    }

    /// Internal: leaf tally this view incremented for epoch `ei`.
    uint ltiOf(ulong ei) nothrow @nogc @system
    {
        return ltiRing[cast(uint)(ei & F.kMask)];
    }

    /// Spec 6b. Returns the (non-negative) starting epoch on success, or
    /// SUBSCRIBE_FULL, SUBSCRIBE_RETRY, or SUBSCRIBE_INVALID on failure.
    long subscribe(AntFarm* f) nothrow @nogc @system
    {
        if (f is null || hasRef) return SUBSCRIBE_INVALID;
        immutable idc = f.add_consumer();
        if (idc < 0) return idc;

        // Spec 6b: derive the walk range from the write tail (the only
        // unpinned read), then walk backwards pinning each slot with Sub
        // *before* inspecting Rtlow/Es. d=0 is the heuristic pin on the
        // frontier. bestSeg is the earliest Es among Rtlow != 0; else the
        // frontier.
        immutable wt0 = atomicLoad!(MemoryOrder.acq)(f.Wt);
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(f, WriteAuditPhase.subscriberProbed, wt0, 0);
        immutable eg = wt0 >> f.segShift;
        uint[KMAX] pinned;
        uint nPinned = 0;
        long bestE = long.max;
        long bestSeg = -1;
        foreach (ulong d; 0 .. f.K)
        {
            if (d > eg) break;
            immutable ki = cast(uint)((eg - d) & f.kMask);
            atomicFetchAdd!(MemoryOrder.rel)(f.Rt[ki][0], SUB);
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberPinned, ki, eg);
            pinned[nPinned++] = ki;
            immutable rt = atomicLoad!(MemoryOrder.acq)(f.Rt[ki][0]);
            if ((rt & LOWMASK) != 0)
            {
                immutable esi = atomicLoad!(MemoryOrder.acq)(f.stats[ki].es);
                if (esi >= 0 && esi < bestE)
                {
                    bestE = esi;
                    bestSeg = ki;
                }
            }
        }
        immutable seg = bestSeg >= 0 ? cast(uint) bestSeg : cast(uint)(eg & f.kMask);

        // Spec 6b: attach in place. Fail closed if the frontier was never written.
        immutable es = atomicLoad!(MemoryOrder.acq)(f.stats[seg].es);
        immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[seg].sqcs);
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[seg].seqt);
        if (es < 0 || sq == 0)
        {
            foreach (i; 0 .. nPinned)
                atomicFetchSub!(MemoryOrder.rel)(f.Rt[pinned[i]][0], SUB);
            f.sub_consumer();
            return SUBSCRIBE_RETRY;
        }

        // Re-read the write tail now that every slot is pinned. The first
        // read established the walk range; this one validates that the tail
        // has not already reserved past the selected epoch. The two reads
        // are also a cheap invalidation check: a later read cannot see an
        // older tail than an earlier read of the same atomic.
        immutable wt1 = atomicLoad!(MemoryOrder.acq)(f.Wt);
        immutable esU = cast(ulong) es;
        immutable seqtEpoch = seqt >> f.segShift;
        // With no live candidate the old frontier may already have been
        // reclaimed and swept into retained quota, even before Wt reserves
        // its next lap. Only attach to a still-current fallback frontier.
        if ((bestSeg < 0 && esU != (wt1 >> f.segShift)) ||
            seqtEpoch < esU || seqtEpoch > esU + f.K - 1 ||
            (wt1 >> f.segShift) >= esU + f.K)
        {
            foreach (i; 0 .. nPinned)
                atomicFetchSub!(MemoryOrder.rel)(f.Rt[pinned[i]][0], SUB);
            f.sub_consumer();
            return SUBSCRIBE_RETRY;
        }
        F = f;
        IDc = cast(ulong) idc;
        immutable lti = cast(uint)(IDc % sq);
        incLeaf(seg, lti);
        // Spec 6b: unwind every Sub. The 0→1 on Rt already retracted Sub0 if present.
        foreach (i; 0 .. nPinned)
            atomicFetchSub!(MemoryOrder.rel)(f.Rt[pinned[i]][0], SUB);
        nextSeq = seqt;
        oldestEi = newestEi = cast(ulong) es;
        ltiRing[seg] = lti;
        sweeperNext = false;
        sweepSeq = 0;
        sdCheckCounter = 0;
        hasRef = true;
        return es;
    }

    /// Spec 6e. Confirmed prefix drops with a plain dec. Confirmed
    /// interiors do too; unconfirmed interiors use the last-releaser
    /// helper. The position uses the helper if unconfirmed or
    /// last-of-farm; a non-last confirmed position drops plain.
    void unsubscribe() nothrow @nogc @system
    {
        if (!hasRef) return;
        auto f = F;
        tryReleaseOldest();
        immutable lastOfFarm = f.sub_consumer() == 1;
        foreach (e; oldestEi .. newestEi)
        {
            immutable ki = cast(uint)(e & f.kMask);
            // Interiors are never the empty-farm pulse: a confirmed
            // interior must be allowed to reach Rt == 0, and an
            // unconfirmed one keeps a pulse only while it is still
            // unconfirmed (retractIfConfirmed closes the race).
            immutable leavePulse = !confirmed(ki, e);
            decRef(ki, ltiOf(e), leavePulse, e, true);
        }
        immutable posPulse = lastOfFarm || !confirmed(posKi(), newestEi);
        decRef(posKi(), ltiOf(newestEi), posPulse, newestEi, !lastOfFarm);
        hasRef = false;
        IDc = 0;
        // Every incomplete held epoch was protected by its last drop above.
        // An unpinned scan here could resurrect a reclaimed, quota-certified
        // slot with a stale Sub0 plant before its post-CAS retract.
        F = null;
    }

    /// Consume one table if available. Returns false when no valid table is
    /// published at nextSeq yet; on that idle path the consumer migrates a
    /// confirmed position onto the frontier (6d), then re-walks the oldest
    /// unconfirmed epoch and the current position segment (8).
    bool consumeNext() nothrow @nogc @system
    {
        if (!hasRef) return false;
        auto f = F;
        auto bp = f.buf;
        immutable idx = nextSeq & f.Lmask;
        // Spec 6d: load-acquire the sentinel and validate location and value.
        if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(nextSeq))
        {
            migrateToFrontier();
            sweepOldestTrailing();
            sweepCurrentPosition();
            return false;
        }

        // Spec 6d: crossing a segment boundary advances newestEi; the old
        // position stays held until the confirmed prefix reaches it (8).
        immutable ei = nextSeq >> f.segShift;
        immutable ki = cast(uint)(ei & f.kMask);
        if (ei != newestEi)
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
                // Spec 7a: primary path. Completing a shard gains the
                // sweeper role (7e).
                sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                        progOff, tlen, tmt == 0, sq, myShi, false, false) & 1) != 0;
                // Spec 7e carried role: small tables concentrate on shard 0
                // (7a); a carried sweeper sweeps them regardless of Shi.
                if (!sweeper && sweeperNext && tlen < smallThresh(sq, bp, idx) && myShi != 0)
                    sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                            progOff, tlen, tmt == 0, sq, 0, true, false) & 1) != 0;
                if (sweeper)
                {
                    immutable nshards = tlen < smallThresh(sq, bp, idx) ? 1 : sq;
                    bool adopted;
                    foreach (s; 0 .. nshards)
                        if (s != myShi)
                        {
                            immutable r = processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                                       progOff, tlen, tmt == 0, sq, s, true, !adopted);
                            if (r & 2) adopted = true;
                        }
                    tertiaryMt(bp, idx, tindexOff, tlen, tmt);
                }
                else
                {
                    // Spec 7c: secondary round-robin share of the MT payloads.
                    secondaryMt(bp, idx, tindexOff, tlen, tmt, sq);
                }
                sweeperNext = sweeper;
            }
            else
            {
                // Complete table: cheap pre-checked MT pass only (MT
                // payloads can outlive their shards' completion). Do not
                // touch sweeperNext: a carried sweeper role must survive
                // across already-complete tables (spec 7e).
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
        }

        nextSeq = tnext;
        migrateToFrontier();
        if (++sdCheckCounter >= SD_CHECK_EVERY_N)
        {
            sdCheckCounter = 0;
            tryReleaseOldest();
        }
        return true;
    }

private:
    /// Spec 7a: chunk for this table from its published AvgCost. Cheap
    /// classes get big chunks (claim amortization), expensive classes get
    /// small chunks (short visits, less leftover tail). Clamped so a
    /// corrupted header can never produce a zero chunk. All consumers read
    /// the same header word, so Tcount shiter arithmetic cannot disagree.
    uint chunkOf(shared(ulong)* bp, ulong tseqIdx) nothrow @nogc @system
    {
        immutable ac = cast(uint) atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 6]);
        return MAX_CHUNK >> (ac > MAX_AVG_COST ? MAX_AVG_COST : ac);
    }

    /// Spec 7a: small-table threshold for this table. A nonzero farm
    /// field is the fixed override (default 64); 0 selects the auto rule
    /// clamp(SqCs * Chunk, 16, 256), so a table shards iff it can give
    /// every shard at least one full chunk.
    uint smallThresh(uint sq, shared(ulong)* bp, ulong tseqIdx) nothrow @nogc @system
    {
        if (F.smallThreshold != 0) return F.smallThreshold;
        immutable t = sq * chunkOf(bp, tseqIdx);
        return t < 16 ? 16 : (t > 256 ? 256 : t);
    }

    /// Spec 6c: take a count. Only the thread that observes 0→1 retracts Sub0.
    void takeRootCount(uint ki) nothrow @nogc @system
    {
        immutable old = atomicFetchAdd!(MemoryOrder.rel)(F.Rt[ki][0], 1);
        immutable extra = old & AntFarm.SUB0MASK;
        if ((old & COUNTMASK) == 0 && extra != 0)
            atomicFetchSub!(MemoryOrder.rel)(F.Rt[ki][0], extra);
    }

    /// Spec 6c/6e last-releaser. Plant-then-dec-then-retract is not atomic
    /// (a racing 0→1 can clear Sub0 before the retract, underflowing
    /// Rtlow). One CAS: if count is 1, clear COUNTMASK and leave one Sub0
    /// (add a unit only if none is present); if > 1, old - 1 with Sub0
    /// unchanged. A failed CAS retries against the new word, not the same
    /// snapshot (spec 1, strong CAS).
    ///
    /// When `retractIfConfirmed` is set, the caller wants a pulse only
    /// while the segment is still unconfirmed. After the last-root CAS
    /// succeeds, the segment is re-checked; if it has become confirmed in
    /// the meantime, the pulse we just left is retracted with the same
    /// low-half masked CAS as plantIfUnprotected. A racing 0->1 changes
    /// the low half away from Sub0, so the retry loop is bounded by that
    /// completion state and cannot underflow Rtlow.
    void releaseRootLeavePulse(uint ki, ulong ei = ulong.max,
                               bool retractIfConfirmed = false) nothrow @nogc @system
    {
        for (;;)
        {
            immutable old = atomicLoad!(MemoryOrder.acq)(F.Rt[ki][0]);
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
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&F.Rt[ki][0], old, nv))
            {
                if (c == 1 && retractIfConfirmed && confirmed(ki, ei))
                {
                    for (;;)
                    {
                        immutable cur = atomicLoad!(MemoryOrder.acq)(F.Rt[ki][0]);
                        if ((cur & COUNTMASK) != 0 || (cur & AntFarm.SUB0MASK) == 0)
                            return; // a racing 0->1 took the count and cleared the pulse
                        if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&F.Rt[ki][0], cur, cur - SUB0))
                            return;
                    }
                }
                return;
            }
        }
    }

    /// Increment a leaf; on 0→1 propagate to Rt and retract Sub0 if present.
    void incLeaf(uint ki, uint lti) nothrow @nogc @system
    {
        immutable lo = atomicFetchAdd!(MemoryOrder.acq_rel)(F.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo == 0)
            takeRootCount(ki);
    }

    /// Decrement a held leaf tally with root propagation (spec 2b). Edge
    /// transition (dec returns 1) repeats on Rt; underflows are fatal.
    /// `leavePulse` uses the last-releaser helper so an unconfirmed (or
    /// last-of-farm confirmed) drop never becomes visible as Rt == 0.
    /// `ei` is the epoch being released; `retractIfConfirmed` asks the
    /// last-releaser helper to take the pulse back if the segment turns
    /// out to be confirmed after the CAS (closing the check/release race).
    void decRef(uint ki, uint lti, bool leavePulse = false,
                ulong ei = ulong.max, bool retractIfConfirmed = false) nothrow @nogc @system
    {
        auto f = F;
        immutable lo = atomicFetchSub!(MemoryOrder.acq_rel)(f.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo <= 0) fatal("leaf tally underflow");
        if (lo == 1)
        {
            if (leavePulse)
                releaseRootLeavePulse(ki, ei, retractIfConfirmed);
            else
            {
                immutable ro = atomicFetchSub!(MemoryOrder.rel)(f.Rt[ki][0], 1);
                if ((ro & COUNTMASK) == 0) fatal("root tally underflow");
            }
        }
    }

    /// Advance newestEi onto ei, pinning every epoch in (newestEi, ei]
    /// so the held range stays contiguous. Increment-ahead (spec 2b)
    /// leaves no zero-gap. IDc % SqCs is the leaf for these new pins only.
    void moveRef(uint ki, ulong ei) nothrow @nogc @system
    {
        auto f = F;
        if (ei < newestEi)
            fatal("moveRef backward");
        if (ei == newestEi)
            return;
        if (ei - oldestEi + 1 > f.K)
        {
            fprintf(stderr,
                "held epoch range exceeds K  oldest=%llu newest=%llu ei=%llu nextSeq=%llu Wt=%llu K=%u\n",
                oldestEi, newestEi, ei, nextSeq,
                atomicLoad!(MemoryOrder.raw)(f.Wt), f.K);
            foreach (uint ski; 0 .. f.K)
            {
                fprintf(stderr, "  ki=%u rt=%llx es=%lld seqt=%llu sd=%llu lt0=%lld lt1=%lld\n",
                    ski,
                    atomicLoad!(MemoryOrder.raw)(f.Rt[ski][0]),
                    atomicLoad!(MemoryOrder.raw)(f.stats[ski].es),
                    atomicLoad!(MemoryOrder.raw)(f.stats[ski].seqt),
                    atomicLoad!(MemoryOrder.raw)(f.stats[ski].sd),
                    atomicLoad!(MemoryOrder.raw)(f.Lt[ski * MAX_LEAVES + 0][0]),
                    atomicLoad!(MemoryOrder.raw)(f.Lt[ski * MAX_LEAVES + 1][0]));
            }
            fatal("held epoch range exceeds K");
        }
        foreach (e; newestEi + 1 .. ei + 1)
        {
            immutable eki = cast(uint)(e & f.kMask);
            if (atomicLoad!(MemoryOrder.acq)(f.stats[eki].es) != cast(long) e)
                fatal("moveRef Es mismatch");
            immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[eki].sqcs);
            if (sq == 0) fatal("moveRef into uninitialized segment");
            immutable lti = cast(uint)(IDc % sq);
            incLeaf(eki, lti);
            ltiRing[eki] = lti;
        }
        if (ki != cast(uint)(ei & f.kMask))
            fatal("moveRef ki/ei disagree");
        newestEi = ei;
        sweepSeq = 0; // new position segment: re-walk from its first table
    }

    /// Spec 6d: if the position is confirmed and nextSeq names an initialized
    /// later epoch, advance newestEi onto that frontier. Always ≥1 pin.
    void migrateToFrontier() nothrow @nogc @system
    {
        auto f = F;
        immutable ei = nextSeq >> f.segShift;
        immutable ki = cast(uint)(ei & f.kMask);
        if (ei == newestEi)
            return;
        if (ei < newestEi)
            return;
        if (!confirmed(posKi(), newestEi))
            return;
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki].es) != cast(long) ei)
            return;
        moveRef(ki, ei);
        tryReleaseOldest();
    }

    /// Spec 6c/8 confirmation: Es[Ki]==Ei, Es[Ki+1]==Ei+1, and
    /// Seqt[Ki]+Sd[Ki] >= Seqt[Ki+1]. The Es match is required so a wrap
    /// that overwrote the slot is not "confirmed."
    bool confirmed(uint ki, ulong ei) nothrow @nogc @system
    {
        auto f = F;
        immutable ei1 = ei + 1;
        immutable ki1 = cast(uint)(ei1 & f.kMask);
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki].es) != cast(long) ei)
            return false;
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki1].es) != cast(long) ei1)
            return false;
        immutable seqtNext = atomicLoad!(MemoryOrder.raw)(f.stats[ki1].seqt);
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
        immutable sd = atomicLoad!(MemoryOrder.raw)(f.stats[ki].sd);
        return seqt + sd >= seqtNext;
    }

    /// Release confirmed prefix epochs from the old end only. Confirmed
    /// interiors stay pinned until they become oldest.
    void tryReleaseOldest() nothrow @nogc @system
    {
        auto f = F;
        while (oldestEi < newestEi)
        {
            immutable ki = cast(uint)(oldestEi & f.kMask);
            if (!confirmed(ki, oldestEi))
                break;
            decRef(ki, ltiOf(oldestEi));
            ++oldestEi;
        }
    }

    /// Idle path: re-walk the oldest unconfirmed held segment claiming any
    /// leftover work, so the segment confirms and its reference can be
    /// released. The data is guaranteed present and intact: we hold a
    /// reference on the segment, so producers cannot have overwritten it.
    void sweepOldestTrailing() nothrow @nogc @system
    {
        if (oldestEi == newestEi) return;
        auto f = F;
        auto bp = f.buf;
        immutable ki = cast(uint)(oldestEi & f.kMask);
        immutable boundary = (oldestEi + 1) << f.segShift;
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki].es) != cast(long) oldestEi)
            return;
        if (confirmed(ki, oldestEi))
        {
            tryReleaseOldest();
            return;
        }
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
        ulong seq = seqt;
        while (seq < boundary)
        {
            immutable idx = seq & f.Lmask;
            if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(seq))
                fatal("held segment's table was overwritten sweepoldest");
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
                    immutable nshards = tlen < smallThresh(sq, bp, idx) ? 1 : sq;
                    foreach (s; 0 .. nshards)
                        processShard(bp, seq, idx, tindexOff, tcountOff,
                                     progOff, tlen, tmt == 0, sq, s, true, false);
                }
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
            seq = tnext;
        }
        tryReleaseOldest();
    }

    /// Idle path: re-walk the current position segment from the last drained
    /// position up to nextSeq, claiming any work that earlier hot-path
    /// visits left behind (for example a small table skipped by a consumer
    /// whose shard was not 0 and which had no carried-sweeper role). The
    /// segment is protected by the position reference, so the data is
    /// present. Tables before the sweepSeq cursor are confirmed complete,
    /// so each park resumes where the previous pass stopped: the walk stays
    /// O(new tables) instead of O(tables in the segment) per miss.
    void sweepCurrentPosition() nothrow @nogc @system
    {
        auto f = F;
        auto bp = f.buf;
        immutable ki = posKi();
        if (atomicLoad!(MemoryOrder.acq)(f.stats[ki].es) != cast(long) newestEi)
            return;
        immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[ki].seqt);
        immutable end = nextSeq;
        if (seqt >= end)
            return;
        ulong seq = seqt > sweepSeq ? seqt : sweepSeq;
        while (seq < end)
        {
            immutable idx = seq & f.Lmask;
            if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(seq))
                fatal("held segment's table was overwritten sweepcurrent");
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
                    immutable nshards = tlen < smallThresh(sq, bp, idx) ? 1 : sq;
                    foreach (s; 0 .. nshards)
                        processShard(bp, seq, idx, tindexOff, tcountOff,
                                     progOff, tlen, tmt == 0, sq, s, true, false);
                }
                tertiaryMt(bp, idx, tindexOff, tlen, tmt);
            }
            seq = tnext;
        }
        sweepSeq = end;
        tryReleaseOldest();
    }

    /// Spec 7d: when the number of consumers passing through an exhausted
    /// shard significantly exceeds SqCs, nudge IDc by +1 so the next
    /// segment pin (and the next table's shard) land in a different bucket.
    /// Already-incremented leaves are not moved.
    void feedback(ulong z, uint sq) nothrow @nogc @system
    {
        if (sq == 0) return;
        if (z > 2UL * sq && atomicLoad!(MemoryOrder.raw)(F.Cf) > sq)
            IDc += 1;
    }

    /// Undersaturation repair, complementing 7d: a sweeper finding a
    /// foreign shard with Z <= 1 (essentially no native traffic) nudges its
    /// IDc by the round-robin distance into that shard, patching the hole a
    /// pre-empted or unsubscribed native left behind. If a laggard native
    /// later wakes and finds the shard overbalanced, the 7d oversaturation
    /// nudge moves it out again. Self-limiting: each entering sweeper's
    /// failed exit-claim raises the shard's Z, so the signal disappears
    /// after a couple of adopters. Already-incremented leaves stay put;
    /// the new IDc is the next pin's leaf target.
    void adopt(uint shi, uint sq, ulong tseq) nothrow @nogc @system
    {
        immutable myShi = cast(uint)((cast(ulong) IDc + tseq) % sq);
        immutable delta = (shi + sq - myShi) % sq;
        if (delta == 0) return;
        IDc += delta;
    }

    /// Spec 7c: enter a payload. Claims bounded by MaxCs; calls bounded by
    /// Done. With loopAll, keep executing iterations until the calls are
    /// exhausted (secondary/tertiary paths); otherwise execute at most one.
    void enterPayload(shared(ulong)* bp, ulong absIdx, bool loopAll) nothrow @nogc @system
    {
        pragma(inline, true);
        auto head = cast(PayloadHeader*)(bp + absIdx);
        // Header fields other than Pcount are ring words. Load them raw so
        // wrap reuse is atomic vs the producer's store-raw, matching Thead.
        immutable w0 = loadRaw(bp[absIdx]);
        immutable maxCs = cast(uint) w0;
        immutable done = cast(uint)(w0 >> 32);
        immutable plen = loadRaw(bp[absIdx + 1]);
        auto fn = cast(Callback) loadRaw(bp[absIdx + 9]);

        // Spec 7c: ST single-shot (MaxCs=1, Done=1). The shard Tcount claim
        // already assigned this index to one consumer; sweeper/re-walk use
        // the same counter, MT paths never walk ST payloads. Record that
        // unique claim without a second RMW. Keep the callback-visible
        // claims field at one and calls/completions at zero, as before.
        if (maxCs == 1 && done == 1 && !loopAll)
        {
            atomicStore!(MemoryOrder.raw)(head.pcount, 1UL << 32);
            // Table completion is Tcount/Tprogress; just do the Call.
            auto body_ = (cast(const(ulong)*)(bp + absIdx + PHEAD_LEN))[0 .. plen];
            fn(head, body_, 0);
            return;
        }

        immutable c = atomicFetchAdd!(MemoryOrder.raw)(head.pcount, 1UL << 32);
        if ((c >> 32) == 0xFFFF_FFFFUL) fatal("Pcount claims wrap");
        if ((c >> 32) >= maxCs)
            return; // overallocated
        do
        {
            immutable d = atomicFetchAdd!(MemoryOrder.raw)(head.pcount, 1UL << 16);
            immutable called = (d >> 16) & 0xFFFF;
            if (called == 0xFFFF) fatal("Pcount calls wrap");
            if (called >= done)
                break;
            auto body_ = (cast(const(ulong)*)(bp + absIdx + PHEAD_LEN))[0 .. plen];
            fn(head, body_, called);
            immutable oldc = atomicFetchAdd!(MemoryOrder.raw)(head.pcount, 1UL);
            if ((oldc & 0xFFFF) == 0xFFFF) fatal("Pcount comps wrap");
        }
        while (loopAll);
    }

    /// Spec 2f: execute one claimed Tindex run of a compact (Tmt == 0)
    /// table. Each record is [Call][Pbody]; Plen is the gap to the next
    /// Tindex entry minus the Call word, and the table's last record ends
    /// at Tsize - END_PAD. The run's claim makes this consumer the unique
    /// visitor of every payload in it, so the callback-visible header is a
    /// stack copy holding the ST fast path's values: MaxCs = Done = 1,
    /// claims 1, calls/completions 0, zero filler. It is shared by the run's
    /// payloads: callbacks must not modify it or retain it past return.
    void runCompact(shared(ulong)* bp, ulong tseqIdx, ulong tindexOff,
                    uint tlen, uint runstart, uint runlen) nothrow @nogc @system
    {
        pragma(inline, true);
        PayloadHeader head;
        head.maxCs = 1;
        head.done = 1;
        atomicStore!(MemoryOrder.raw)(head.pcount, 1UL << 32);
        immutable runend = runstart + runlen;
        immutable endOff = runend < tlen
            ? loadRaw(bp[tindexOff + runend])
            : loadRaw(bp[tseqIdx + 5]) - END_PAD;
        ulong poff = loadRaw(bp[tindexOff + runstart]);
        foreach (j; runstart + 1 .. runend + 1)
        {
            immutable next = j < runend ? loadRaw(bp[tindexOff + j]) : endOff;
            immutable absIdx = tseqIdx + poff;
            head.plen = next - poff - PCOMPACT_LEN;
            head.call = cast(Callback) loadRaw(bp[absIdx]);
            auto body_ = (cast(const(ulong)*)(bp + absIdx + PCOMPACT_LEN))[0 .. head.plen];
            head.call(&head, body_, 0);
            poff = next;
        }
    }

    /// Spec 7a: work claims on one shard, mapping it to a linear slice of
    /// Tindex claimed by chunks on the shard's Tcount counter. With
    /// checkFirst (sweeps of foreign shards and re-walks), a plain load
    /// short-circuits fully-claimed shards without an RMW.
    ///
    /// Returns a bitmask: bit 0 set when this consumer added the final
    /// completion sum for the shard (spec 7a): it adds Shlen to
    /// Tprogress, accounts the table's size into the segment's Sd if
    /// the table thereby completed, and gains the sweeper role. Bit 1 set
    /// when, sweeping a foreign shard, it found the shard starved
    /// (Z <= 1) and adopted it (only when allowAdopt).
    uint processShard(shared(ulong)* bp, ulong tseq, ulong tseqIdx, ulong tindexOff,
                      ulong tcountOff, ulong progOff, uint tlen, bool compact, uint sq,
                      uint shi, bool checkFirst, bool allowAdopt) nothrow @nogc @system
    {
        // Spec 7a: threshold and chunk come from the table's own
        // header (farm override or auto rule; AvgCost), so every consumer
        // agrees on sharding and shiter for this table.
        immutable thresh = smallThresh(sq, bp, tseqIdx);
        uint shstart, shlen, shbase;
        // Spec 7a: small tables are claimed wholesale by shard 0 only.
        if (tlen < thresh)
        {
            if (shi != 0) return 0;
            shstart = 0;
            shlen = tlen;
            shbase = tlen;
        }
        else
        {
            // Spec 7a.
            shbase = tlen / sq;
            immutable shrm = tlen % sq;
            shlen = shi < shrm ? shbase + 1 : shbase;
            shstart = shi * shbase + (shi < shrm ? shi : shrm);
        }
        if (shlen == 0)
            return 0;
        // Spec 7a. The table's published AvgCost determines chunk size.
        immutable chunk = chunkOf(bp, tseqIdx);
        immutable shiter = (shlen + chunk - 1) / chunk;
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
                if (allowAdopt && tlen >= thresh && claims - shiter <= 1)
                {
                    adopt(shi, sq, tseq);
                    return 2;
                }
                return 0;
            }
        }
        for (;;)
        {
            // Spec 7a.
            immutable rawc = atomicFetchAdd!(MemoryOrder.raw)(*shc, 1UL << 32);
            if ((rawc >> 32) == 0xFFFF_FFFFUL) fatal("Tcount claims wrap");
            immutable x = cast(uint)(rawc >> 32);
            if (x >= shiter)
            {
                immutable z = x - shiter;
                if (!checkFirst)
                    feedback(z, sq); // spec 7d: Z = X - Shiter
                else if (allowAdopt && tlen >= thresh && z <= 1)
                {
                    adopt(shi, sq, tseq);
                    return 2;
                }
                return 0;
            }
            // Spec 7a.
            ++ownClaims;
            if (x == 0)
                firstClaimant = true;
            immutable runstart = shstart + x * chunk;
            immutable runlen = chunk < shlen - x * chunk ? chunk : shlen - x * chunk;
            if (compact)
                runCompact(bp, tseqIdx, tindexOff, tlen, runstart, runlen);
            else
                foreach (i; 0 .. runlen)
                {
                    immutable poff = atomicLoad!(MemoryOrder.raw)(bp[tindexOff + runstart + i]);
                    enterPayload(bp, tseqIdx + poff, false);
                }
            // Spec 7a-e: the consumer adding the final completion sum for
            // the shard increments Tprogress by the shard length, whichever
            // consumer (owner, sweeper, or re-walker) that happens to be.
            // Join every chunk's primary-run callback writes before the final
            // chunk forwards this shard's completion into Tprogress. A relaxed
            // completion sum would join only the shard finisher's writes.
            // Every callback is included only for a tracked ST single-shot
            // table.
            immutable y = atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, cast(ulong) runlen);
            if ((y & 0xFFFF_FFFFUL) > 0xFFFF_FFFFUL - runlen) fatal("Tcount comps wrap");
            if ((y & 0xFFFF_FFFFUL) == shlen - runlen)
            {
                // Tprogress is also a completion join: each shard publishes
                // its primary-run callback writes, and the last RMW acquires
                // the release chain before notifying an aggregate table
                // observer. Secondary and tertiary MT callbacks are not
                // joined here.
                immutable tp = atomicFetchAdd!(MemoryOrder.acq_rel)(
                    bp[progOff], cast(ulong) shlen);
                if (tp + shlen == tlen)
                {
                    // Spec 8: the finisher accounts Tsize into the starting
                    // segment's Sd. We still hold that segment (it was
                    // unconfirmed until now), so no producer is re-zeroing.
                    // The next lap's Sd reset is ordered after this add by
                    // our later pin drop -> zero-Rt sweep -> acq_rel Wt
                    // reservation chain (spec 9).
                    immutable tsize = atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 5]);
                    immutable tki = cast(uint)((tseq >> F.segShift) & F.kMask);
                    atomicFetchAdd!(MemoryOrder.raw)(F.stats[tki].sd, tsize);
                    immutable hookWord = atomicLoad!(MemoryOrder.raw)(
                        bp[tseqIdx + 7]);
                    if (hookWord != 0)
                    {
                        auto hook = cast(TableCompletionHook*) cast(void*)
                            hookWord;
                        // Copy both fields before calling: completion may
                        // release the last external owner of the hook.
                        immutable notify = hook.call;
                        auto context = hook.context;
                        if (notify is null)
                            fatal("table completion hook lost callback");
                        notify(context);
                    }
                }
                return 1;
            }
            // Spec 7b: first-claimant mid-tick yield. After a run that
            // did not complete the shard, the X==0 claimant may leave if
            // the shard is shared (claimsNow > ownClaims) and Tnext's
            // sentinel is already live. Sweeper/re-walk (checkFirst) never
            // yields. The other claimants finish the shard.
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

    /// Spec 7c: secondary work claims. Round-robin assignment of the
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
