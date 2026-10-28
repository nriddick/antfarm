/++
 + Ant Farm: an M:N concurrent queue with superlative scaling.
 +
 + Responsibility: the fixed-memory ring, producer registration and quota,
 + work tables, consumer claims and segment reclamation. It imports only
 + `antfarm.allocation`; `antfarm.templates` and the actors build on it.
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
 + Cold-path CAS: ticket claim and the per-epoch Live discharge (SPEC.md 4d,
 + 6c). Retry loops reload fresh state; a discharge CAS retries only while
 + another root field changes under it and stops once Live reads clear.
 + Strong CAS (spec 1).
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
import core.checkedint : addu, mulu;
import core.stdc.errno : errno;
import core.stdc.stdio : fprintf, stderr, snprintf;
import core.stdc.stdlib : abort, free, getenv;
import core.stdc.string : memset;
import antfarm.allocation : allocateAligned64, freeAligned64;
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
        // Use a literal: druntime's SE_LOCK_MEMORY_NAME is exported data,
        // which would introduce a runtime dependency in a BetterC native link.
        if (!LookupPrivilegeValueW(null, "SeLockMemoryPrivilege"w.ptr, &tp._Privileges.Luid))
            return false;
        tp._Privileges.Attributes = SE_PRIVILEGE_ENABLED;
        SetLastError(0);
        if (!AdjustTokenPrivileges(tok, FALSE, &tp, 0, null, null))
            return false;
        return GetLastError() == 0;
    }

    /// Record a mapping failure with the current GetLastError; `create`
    /// appends it to the fatal message as "(GetLastError=N)".
    private void* mapFailedWin(ref FarmCreateError error, string detail) nothrow @nogc @system
    {
        error = FarmCreateError(FarmCreateFailure.mapping, cast(int) GetLastError(), detail);
        error.winLastError = true;
        return null;
    }

    /// This SKU rejects MapViewOfFile3(MEM_REPLACE_PLACEHOLDER | MEM_LARGE_PAGES)
    /// (ERROR_INVALID_PARAMETER). Dual MapViewOfFileEx with FILE_MAP_LARGE_PAGES works.
    private void* mapMagicBufferWindowsLarge(size_t bytes, ref FarmCreateError error) nothrow @nogc @system
    {
        if (!enableLockPages())
            return mapFailed(error, "antfarm: SeLockMemoryPrivilege not held. Grant Lock Pages in Memory to this user, log off, and retry (see README)");
        if (pGetLargePageMinimum is null)
            return mapFailed(error, "antfarm: GetLargePageMinimum unavailable");
        auto lp = pGetLargePageMinimum();
        if (lp == 0 || (bytes % lp) != 0)
            return mapFailed(error, "antfarm: ring size is not a multiple of the large-page minimum");

        auto section = CreateFileMappingW(INVALID_HANDLE_VALUE, null,
            PAGE_READWRITE | SEC_COMMIT | SEC_LARGE_PAGES_FLAG,
            cast(DWORD)(bytes >> 32), cast(DWORD) bytes, null);
        if (section is null)
            return mapFailedWin(error, "antfarm: CreateFileMapping SEC_LARGE_PAGES failed");

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
        // Capture GetLastError before CloseHandle, which may overwrite it.
        auto failed = mapFailedWin(error, "antfarm: MapViewOfFileEx large-page dual map failed");
        CloseHandle(section);
        return failed;
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
        int initialize() nothrow @nogc @system { return 0; }
        void destroy() nothrow @nogc @system {}
        void lock() nothrow @nogc @system { AcquireSRWLockExclusive(&state); }
        void unlock() nothrow @nogc @system { ReleaseSRWLockExclusive(&state); }
    }
    else version (Posix)
    {
        private pthread_mutex_t state;
        /// 0, or the pthread error code.
        int initialize() nothrow @nogc @system
        {
            return pthread_mutex_init(&state, null);
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
/// keeps `requested`. Any other value clears `valid` so a typo cannot
/// silently select the wrong backing (`create` makes that fatal).
private bool wantHugePages(bool requested, out bool valid) nothrow @nogc @system
{
    valid = true;
    auto e = getenv("ANTFARM_HUGE_PAGES");
    if (e is null || e[0] == 0) return requested;
    if (e[1] == 0)
    {
        if (e[0] == '0') return false;
        if (e[0] == '1') return true;
    }
    valid = false;
    return requested;
}

private void* mapFailed(ref FarmCreateError error, string detail, int osError = 0)
    nothrow @nogc @safe
{
    error = FarmCreateError(FarmCreateFailure.mapping, osError, detail);
    return null;
}

/// Dual-map `bytes` so `[0, bytes)` aliases `[bytes, 2*bytes)`. Null plus
/// `error` on failure, with every partial reservation, view and handle released.
private void* mapMagicBuffer(size_t bytes, bool hugePages, ref FarmCreateError error)
    nothrow @nogc @system
{
    version (Windows)
    {
        resolveWinMapApis();
        if (pVirtualAlloc2 is null || pMapViewOfFile3 is null)
            return mapFailed(error, "antfarm: VirtualAlloc2/MapViewOfFile3 unavailable (need Windows 10 1803+)");

        if (hugePages)
            return mapMagicBufferWindowsLarge(bytes, error);

        auto p = pVirtualAlloc2(null, null, 2 * bytes,
            MEM_RESERVE | MEM_RESERVE_PLACEHOLDER, PAGE_NOACCESS, null, 0);
        if (p is null)
            return mapFailedWin(error, "antfarm: VirtualAlloc2 placeholder reserve failed");
        if (!VirtualFree(p, bytes, MEM_RELEASE | MEM_PRESERVE_PLACEHOLDER))
        {
            mapFailedWin(error, "antfarm: VirtualFree placeholder split failed");
            VirtualFree(p, 0, MEM_RELEASE);
            return null;
        }
        // Two placeholders now: [p, p+bytes) and [p+bytes, p+2*bytes).
        auto section = CreateFileMappingW(INVALID_HANDLE_VALUE, null, PAGE_READWRITE,
            cast(DWORD)(bytes >> 32), cast(DWORD) bytes, null);
        if (section is null)
        {
            mapFailedWin(error, "antfarm: CreateFileMapping failed");
            VirtualFree(p, 0, MEM_RELEASE);
            VirtualFree(cast(char*) p + bytes, 0, MEM_RELEASE);
            return null;
        }

        auto v1 = pMapViewOfFile3(section, null, p, 0, bytes,
            MEM_REPLACE_PLACEHOLDER, PAGE_READWRITE, null, 0);
        if (v1 is null)
        {
            // Capture before the second map or CloseHandle overwrites it.
            mapFailedWin(error, "antfarm: MapViewOfFile3 dual map failed");
            CloseHandle(section);
            VirtualFree(p, 0, MEM_RELEASE);
            VirtualFree(cast(char*) p + bytes, 0, MEM_RELEASE);
            return null;
        }
        auto v2 = pMapViewOfFile3(section, null, cast(char*) p + bytes, 0, bytes,
            MEM_REPLACE_PLACEHOLDER, PAGE_READWRITE, null, 0);
        if (v2 is null)
        {
            mapFailedWin(error, "antfarm: MapViewOfFile3 dual map failed");
            CloseHandle(section);
            UnmapViewOfFile(v1);
            VirtualFree(cast(char*) p + bytes, 0, MEM_RELEASE);
            return null;
        }
        CloseHandle(section);
        return v1;
    }
    else version (Posix)
    {
        static __gshared int shmCounter;
        char[64] name;
        snprintf(name.ptr, name.length, "/antfarm-%d-%d", cast(int) getpid(),
                 atomicFetchAdd!(MemoryOrder.raw)(shmCounter, 1));
        int fd = shm_open(name.ptr, O_RDWR | O_CREAT | O_EXCL, 384); // 0600
        if (fd < 0) return mapFailed(error, "shm_open failed", errno);
        shm_unlink(name.ptr);
        void* posixFailed(void* reserved, string detail)
        {
            immutable osError = errno;
            if (reserved !is null) munmap(reserved, 2 * bytes);
            if (fd >= 0) close(fd);
            return mapFailed(error, detail, osError);
        }
        if (ftruncate(fd, cast(off_t) bytes) != 0) return posixFailed(null, "ftruncate failed");
        void* p = mmap(null, 2 * bytes, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
        if (p == MAP_FAILED) return posixFailed(null, "mmap reserve failed");
        if (mmap(p, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED)
            return posixFailed(p, "mmap first half failed");
        if (mmap(cast(char*) p + bytes, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0)
                == MAP_FAILED)
            return posixFailed(p, "mmap second half failed");
        close(fd);
        fd = -1;

        if (hugePages)
        {
            version (linux)
            {
                if (madvise(p, bytes, MADV_HUGEPAGE) != 0)
                    return posixFailed(p, "madvise first half huge pages failed");
                if (madvise(cast(char*) p + bytes, bytes, MADV_HUGEPAGE) != 0)
                    return posixFailed(p, "madvise second half huge pages failed");
            }
            else
                return posixFailed(p, "huge pages only supported on Linux");
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
/// `subscribeOnce` only: the write tail kept moving past every candidate for
/// the attempt's bounded rounds. Transient and recoverable; `subscribe`
/// retries it internally.
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
/// Ceiling on the claims a derived-hint table asks for. It bounds the
/// per-table claim overhead on large hosts; 8 covers the consumer counts
/// where splitting small tables measurably pays (see tools/perf results
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
 + class 2). It returns the cheapest class whose claims still number at
 + least `min(count, clamp(consumers, 1, rootCostHintMaxSplits))`, so a table
 + can occupy up to ~consumer-count workers, and the class of chunk 32 is kept
 + whenever it already provides that many claims. A wholesale table's claims
 + are `ceil(count / chunk)`. A table at least `smallThreshold` entries wide
 + is sharded, and its claims are counted per shard over `sqcsOf(consumers)`
 + shards. Without that count a table just above the threshold would get one
 + claim per shard, fewer than a table just below it. Wide tables already
 + reach the target at chunk 32 and keep it. `consumers` is the farm's live
 + subscribed-view count (`Cf`), which can include the emitter's own view; it
 + may change between tables, and each table's class follows it
 + independently. The shard count is an estimate: a table's `SqCs` follows
 + the live `Cf` at its own write, not its segment's consumer count. A farm
 + using the auto threshold
 + rule (`smallThreshold == 0`) is classified against
 + DEFAULT_SMALL_TABLE_THRESHOLD, which may differ from the auto rule's
 + effective threshold; the result is still a valid class.
 + Returns `avgCostForChunk(rootCostHintMaxChunk) .. MAX_AVG_COST`.
 +/
uint rootCostHintFor(size_t count, ulong consumers, uint smallThreshold)
    pure nothrow @nogc @safe
{
    enum uint cheapest = avgCostForChunk(rootCostHintMaxChunk);
    if (count == 0) return cheapest;
    // Spec 7a: a wholesale table is one shard; a sharded one splits its
    // entries over SqCs shards, the first count % SqCs one entry longer.
    immutable size_t shards =
        count >= (smallThreshold != 0 ? smallThreshold : DEFAULT_SMALL_TABLE_THRESHOLD)
        ? sqcsOf(consumers) : 1;
    immutable splits = consumers < 1 ? cast(ulong) 1
        : consumers > rootCostHintMaxSplits ? cast(ulong) rootCostHintMaxSplits
        : consumers;
    immutable target = cast(size_t) (count < splits ? count : splits);
    immutable shortLen = count / shards, longShards = count % shards;
    // Cheapest class whose claims over every shard reach the target.
    uint hint = cheapest;
    for (;; ++hint)
    {
        immutable chunk = cast(size_t) (MAX_CHUNK >> hint);
        immutable claims = longShards * ((shortLen + chunk) / chunk)
            + (shards - longShards) * ((shortLen + chunk - 1) / chunk);
        if (hint == MAX_AVG_COST || claims >= target) return hint;
    }
}
/// Smallest ring accepted by `AntFarm.create`, in MiB (2^17 ulongs). A ring's
/// cache footprint is its live window, not its length (the ring-size study in
/// docs/performance), and a 1 MiB ring with K = 8 still leaves 16 Ki-word
/// segments; the quota rule `Exmax <= (K-1)*segCap` is what bounds how many
/// producers such a ring can plan.
enum uint MIN_RING_MIB = 1;
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
/// Maximum supported number of segments K. Default create()/tools/perf K is 8.
enum KMAX = 16;

/// Spec 4d: producer ticket slot stride in ulongs. Each slot's hash sits on
/// its own 64-byte cache line: tickets are shared mutable state under the
/// registration CAS, and the buffer cannot align them beyond 8 bytes, so
/// the stride (not the natural ulong size) must carry the isolation.
private enum size_t PROD_SLOT_STRIDE = 8;

/// Internal: spec 2b/6b — one subscriber's provisional Sub pin, counted in the
/// most significant half of Rt.
enum ulong SUB = 1UL << 32;
/// Internal: spec 2b/6c — Live, one bit: the epoch in this slot was initialized
/// and has not been discharged after confirmation. Bits 17..31 are zero.
enum ulong LIVE = 1UL << 16;
/// Internal: mask for Rtlow (least significant half of Rt): Live and the count.
enum ulong LOWMASK = 0xFFFF_FFFFUL;
/// Internal: mask for the active-consumer count bits within Rtlow (below Live).
enum ulong COUNTMASK = LIVE - 1;
// Test-only scheduling points, elided in production builds. Tests may pause a
// producer or subscriber here without changing quota or tally state; ordinary
// builds contain neither the hook nor its call sites.
//   liveRaised(e)              producer: Live raised on epoch e, Es[e] not yet published
//   subscriberCounted          subscriber: admitted and counted, before any probe
//   subscriberProbed(wt, round) subscriber: tail sampled for this round
//   subscriberPinned(ki, eg)   subscriber: Sub added on slot ki
//   subscriberRead(ki, x)      subscriber: about to read slot ki as epoch x
//   subscriberReadMid(ki, x)   subscriber: between that read's Es and Rt loads
//   subscriberValidating(ki, x) subscriber: candidate x re-read Live under its
//                              Sub, before the validation loads
//   liveClearing(ki, rt)       consumer: discharge loaded rt (Live set) on slot
//                              ki, before its CAS
version (AntfarmWriteAuditHooks)
{
    enum WriteAuditPhase { anchorProbed, quotaSwept, tailReserved, subscriberPinned,
        tablePublished, subscriberProbed, liveRaised, subscriberCounted,
        subscriberRead, subscriberReadMid, subscriberValidating, liveClearing }
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

/// Optional process-wide observer of a fatal contract violation. It must not
/// throw, longjmp, or re-enter the Farm. Returning is followed by abort().
alias FatalHandler = extern(C) void function(const(char)* message, size_t bytes,
    void* context) nothrow @nogc @system;
private __gshared FatalHandler fatalHandler;
private __gshared void* fatalContext;
private bool reportingFatal;

/// Install before starting Farm users; replacement requires process-wide Farm
/// quiescence. Null restores the default diagnostic and abort policy.
void setFatalHandler(FatalHandler handler, void* context = null) nothrow @nogc @system
{
    fatalContext = context;
    fatalHandler = handler;
}

/// Fatal error: notify the installed observer, print, and abort (SPEC 10).
void fatal(const(char)[] msg) nothrow @nogc @system
{
    if (!reportingFatal && fatalHandler !is null)
    {
        reportingFatal = true;
        fatalHandler(msg.ptr, msg.length, fatalContext);
    }
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

/// Spec 2d, 7c: executes one payload. The body is borrowed until return.
/// C linkage and explicit pointer/count let C callbacks enter the ring directly.
extern(C) alias Callback = void function(const(ulong)* body, size_t bodyWords)
    nothrow @nogc @system;

// A blocking adapter must not re-enter a view whose walk is in a callback.
// This TLS guard covers claimed runs and completion hooks in every build.
private bool farmCallbackActive;

package bool inFarmCallback() nothrow @nogc @safe
{
    return farmCallbackActive;
}

private struct FarmCallbackScope
{
    bool previous;
    this(bool active) nothrow @nogc @safe
    {
        previous = farmCallbackActive;
        farmCallbackActive = active;
    }
    ~this() nothrow @nogc @safe { farmCallbackActive = previous; }
}

/// Low-level notification attached to one physical payload table. The Farm
/// invokes `call(context)` exactly once, on the consumer that advances that
/// table's Tprogress to Tlen. The pointed-to hook and its context
/// must remain stable until invocation. The Farm snapshots both fields before
/// calling and never accesses the hook/context afterward, so a notification
/// may release its own storage or transfer it as its final access. Otherwise
/// the caller must keep it stable until the notification returns.
extern(C) alias TableCompletionCallback = void function(void* context)
    nothrow @nogc @system;

/// Optional exactly-once notification attached to one physical payload
/// table by write. The Farm snapshots both fields before invoking
/// `call(context)`, exactly once, on the consumer that advances that table's
/// Tprogress to Tlen. The pointed-to storage must remain stable
/// until invocation; see TableCompletionCallback for the full contract.
/// `write` takes the hook as `const(TableCompletionHook)*`: the const covers
/// the hook record only. `context` is passed to `call` as the mutable
/// `void*` it was stored as, despite D's transitive const, matching a C
/// caller's `const af_completion_hook *`.
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

/// Compact tables (spec 2f) store [Call][Pbody...] with one index per record.
/// The last record ends at Tnext - Tseq - END_PAD; lengths are derived.
private enum ulong PCOMPACT_LEN = 1;
static assert(Callback.sizeof == ulong.sizeof);
static assert(TableCompletionHook.sizeof == 16);
static assert(TableCompletionHook.context.offsetof == 0);
static assert(TableCompletionHook.call.offsetof == 8);

/// Uniform tables (spec 2g) share Call in extension word 5. Kind 1 has
/// indexed bare bodies; kind 2 also stores Tblen >= 1 in word 6 and omits
/// indexes. Thead word 2 packs Tlen, kind and cost (spec 2e).
private enum TableKind : uint
{
    perPayload = 0,
    uniformCall = 1,
    uniformCallAndLength = 2,
}
/// Thead extension words for uniformCall / uniformCallAndLength tables.
private enum ulong UNIFORM_EXT = 1;
private enum ulong UNIFORM_FIXED_EXT = 2;

/// How a table's records are stored, decoded from the Thead kind.
private enum TableLayout : uint
{
    compact,      /// [Call][Pbody] per payload, 2f
    uniform,      /// [Pbody] per payload, indexed, 2g
    uniformFixed, /// [Pbody] per payload, index-free, 2g
}

private TableLayout tableLayoutOf(ulong word) pure nothrow @nogc @safe
{
    immutable kind = cast(ubyte)(word >> 32);
    assert(kind <= TableKind.uniformCallAndLength);
    if (kind == TableKind.uniformCallAndLength) return TableLayout.uniformFixed;
    if (kind == TableKind.uniformCall) return TableLayout.uniform;
    return TableLayout.compact;
}

private TableKind tableKindOf(TableLayout layout) pure nothrow @nogc @safe
{
    return layout == TableLayout.uniform ? TableKind.uniformCall
        : layout == TableLayout.uniformFixed ? TableKind.uniformCallAndLength
        : TableKind.perPayload;
}

/// Thead extension words of `layout`.
private ulong extWordsOf(TableLayout layout) pure nothrow @nogc @safe
{
    return layout == TableLayout.uniform ? UNIFORM_EXT
        : layout == TableLayout.uniformFixed ? UNIFORM_FIXED_EXT : 0;
}

/// Tindex words of a table of `n` payloads.
private ulong indexWordsOf(TableLayout layout, ulong n)
        pure nothrow @nogc @safe
{
    final switch (layout)
    {
    case TableLayout.compact: return n;
    case TableLayout.uniform: return n;
    case TableLayout.uniformFixed: return 0;
    }
}

/// Per-record words besides the body in `layout`.
private ulong recordWordsOf(TableLayout layout) pure nothrow @nogc @safe
{
    return layout == TableLayout.compact ? PCOMPACT_LEN : 0;
}

/// Spec 2d: one ready-to-publish callback and its borrowed body words.
struct PayloadEntry
{
    Callback call;    /// Work callback, copied into the table.
    PayloadBody body; /// Words copied into the ring before write returns.
}

private enum bool hasCommonPayloadCallback(R) =
    __traits(hasMember, R, "commonPayloadCallback");

private enum bool hasFixedPayloadLength(R) =
    __traits(hasMember, R, "fixedPayloadLength");

/// A lazy zip of independent callback and body input ranges.  This is useful
/// when a job source naturally keeps scheduling metadata separate from its
/// argument storage.  The range ends when either input ends.
struct PayloadPairRange(CR, BR)
{
    /// Internal: the callback source this adapter zips.
    CR callbacks;
    /// Internal: the body source this adapter zips.
    BR bodies;

    /// True when either source is exhausted.
    @property bool empty() nothrow @nogc @system
    {
        return callbacks.empty || bodies.empty;
    }

    /// The next callback paired with its body.
    @property PayloadEntry front() nothrow @nogc @system
    {
        return PayloadEntry(callbacks.front, cast(PayloadBody) bodies.front);
    }

    /// Advance both sources by one element.
    void popFront() nothrow @nogc @system
    {
        callbacks.popFront();
        bodies.popFront();
    }

    /// Checkpoint both sources when they are forward ranges.
    static if (isForwardRange!CR && isForwardRange!BR)
    {
        @property typeof(this) save() nothrow @nogc @system
        {
            typeof(this) result = this;
            result.callbacks = callbacks.save;
            result.bodies = bodies.save;
            return result;
        }
    }

    static if (hasLength!CR && hasLength!BR)
    {
        /// The shorter of the two source lengths.
        @property size_t length() nothrow @nogc @system
        {
            immutable hn = cast(size_t) callbacks.length;
            immutable bn = cast(size_t) bodies.length;
            return hn < bn ? hn : bn;
        }
    }

    /// Pop up to `n` entries from both sources; returns how many were popped.
    size_t popFrontN(size_t n) nothrow @nogc @system
    {
        static if (hasLength!CR && hasLength!BR)
        {
            immutable count = n < length ? n : length;
            callbacks.popFrontN(count);
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

/// Adapt separate callback and body input ranges to the ordinary payload-entry
/// source accepted by `write`. Callback elements are function pointers;
/// body elements must be implicitly convertible to `const(ulong)[]`.
auto pairPayloads(CR, BR)(CR callbacks, BR bodies) nothrow @nogc @system
    if (isInputRange!CR && isInputRange!BR &&
        is(ElementType!CR : Callback) &&
        is(ElementType!BR : PayloadBody))
{
    return PayloadPairRange!(CR, BR)(callbacks, bodies);
}

/// A lazy broadcast of one callback over a body input range. The
/// callback is copied into the adapter. Unlike pairing with a one-element callback range, this adapter ends
/// only when the body range ends.
struct PayloadBroadcastRange(BR, bool fixedLength = false)
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    private BR bodies;
    private Callback call;
    static if (fixedLength)
        private ulong bodyWords;

    private this(Callback callback, BR source,
            ulong fixedBodyWords = 0)
            nothrow @nogc @system
    {
        bodies = source;
        call = callback;
        static if (fixedLength)
            bodyWords = fixedBodyWords;
    }

    /// Uniform-layout protocol consumed by AntFarm.writeImpl.
    @property Callback commonPayloadCallback() nothrow @nogc @system { return call; }

    /// True when the body source is exhausted.
    @property bool empty() nothrow @nogc @system { return bodies.empty; }

    /// The next entry: the common callback paired with the next body.
    @property PayloadEntry front() nothrow @nogc @system
    {
        auto body = cast(PayloadBody) bodies.front;
        static if (fixedLength)
            if (body.length != bodyWords)
                fatal("fixed payload body length mismatch");
        return PayloadEntry(call, body);
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

/// Broadcast one common callback over every body in `bodies`.
auto broadcastPayloads(BR)(Callback callback, BR bodies)
        nothrow @nogc @system
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    return PayloadBroadcastRange!BR(callback, bodies);
}

/// Broadcast one common callback over uniformly sized bodies. `bodyWords` is a
/// caller contract: every body must have exactly that many ulongs. This lets
/// `write` size the table without evaluating each body's `front`.
auto broadcastPayloads(BR)(Callback callback, BR bodies,
        ulong bodyWords) nothrow @nogc @system
    if (isInputRange!BR && is(ElementType!BR : PayloadBody))
{
    return PayloadBroadcastRange!(BR, true)(callback, bodies, bodyWords);
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
/// Tokens are single-owner and move-only: copying and lvalue assignment do
/// not compile, because a copy would silently leave two tokens for one
/// registered slot. Transfer a token explicitly with `move` (std.algorithm
/// or core.lifetime), or lend it with a `ref` parameter. `opAssign` accepts
/// only an rvalue (`token = move(other)`), and assigning over a still-valid
/// token is fatal: it would lose the only handle to a registered slot.
/// Unregister the old token first. A token handed to another thread must
/// travel through a synchronization mechanism; the farm's own checks do
/// not order the receiving thread's first reads of the transferred fields.
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

    /// The issuing Farm's `farmId`; 0 for a token that was never registered.
    @property ulong issuer() const pure nothrow @nogc @safe { return farmId; }

    /// True while this token is registered with its Farm.
    bool valid() const pure nothrow @nogc @safe { return hash != 0; }

    /// Copying is disabled: transfer ownership with an explicit `move`.
    @disable this(ref typeof(this));

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

    /// Move-assignment from an explicit `move` source (an rvalue). The
    /// destination must be invalid (default or unregistered).
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
    /// Internal: shard/leaf fan-out snapshot, sqcsOf(live consumers).
    shared ulong sqcs;
    /// Internal: consumed-size accumulator: sum of completed table sizes.
    shared ulong sd;
    /// Internal: cache-line padding.
    ulong[4] pad;
}
static assert(SegStats.sizeof == 64);

/// Table layout offsets (in ulongs, relative to the table's starting
/// sequence), spec 2e:
///   [0..5)                 Thead: 0 Tsent, 1 Tnext, 2 packed Tlen/kind/cost,
///                          3 SqCs, 4 optional TableCompletionHook pointer
///   uniform tables (2g):   Tcall, then Tblen for the index-free kind
///   [.. +Tlen)             Tindex (one offset per payload;
///                          absent in index-free uniform tables)
///   7 ulongs padding
///   Tprogress (1 ulong)
///   7 ulongs padding
///   Tcount: SqCs counters, each followed by 7 ulongs padding
///   Payloads (compact Call + Plen-ulong body, 2f;
///             uniform bare Plen-ulong body, 2g)
///   7 ulongs padding
private enum ulong THEAD_LEN = 5;
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

private void validateCallback(Callback call) nothrow @nogc @system
{
    if (call is null) fatal("null payload callback");
}

/// Shared singleton cost for canFit and every write sizing path.
private bool singletonCost(ulong base, ulong blen, out ulong cost)
        pure nothrow @nogc @safe
{
    pragma(inline, true);
    immutable overhead = base + 2UL;
    if (blen > ulong.max - overhead) return false;
    cost = overhead + blen;
    return true;
}

/// A singleton exceeding this tier can never be published by this producer.
private void checkPayloadFits(ulong base, ulong quota, ulong blen)
        nothrow @nogc @system
{
    pragma(inline, true);
    ulong cost;
    if (!singletonCost(base, blen, cost)) fatal("table size overflow");
    if (cost > quota) fatal("payload larger than this producer tier's Exi");
}

/// Base size of every table in ulongs: Thead + progress pads + Tcount + end
/// pad. `sq` is a uint, so this cannot overflow.
private ulong tableBaseWords(uint sq) pure nothrow @nogc @safe
{
    return THEAD_LEN + PROG_PAD + 1 + 7 + 8UL * sq + END_PAD;
}

/// Table size in ulongs: Thead + extension and index words (`headWords`) +
/// pads + Tprogress + Tcount + payloads + end pad.
private ulong tableSizeChecked(ulong headWords, ulong sq, ulong psum) nothrow @nogc @system
{
    ulong s = THEAD_LEN;
    s = addChecked(s, headWords, "table size overflow");
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

/// Why `AntFarm.tryCreate` (or `AntFarm.checkGeometry`) failed.
/// Non-exhaustive: do not `final switch` on it.
enum FarmCreateFailure : ubyte
{
    none,             /// no failure
    geometry,         /// ring size, K, consumer count or producer quotas rejected
    hugePagesSetting, /// `ANTFARM_HUGE_PAGES` is set to something other than 0 or 1
    mutex,            /// the producer mutex could not be initialized
    mapping,          /// the dual-mapped ring could not be created
    allocation,       /// Farm or producer-ticket storage could not be allocated
}

/// The failure `AntFarm.tryCreate` reports and `AntFarm.checkGeometry`
/// returns. `FarmCreateError.init` (kind `none`) means success.
struct FarmCreateError
{
    FarmCreateFailure kind; /// what failed
    int osError;            /// errno / GetLastError, 0 when not an OS failure
    string detail;          /// static text; the message `create` passes to fatal
    private bool winLastError; // create appends "(GetLastError=osError)"
}

/// Normalized geometry shared by checkGeometry and tryCreate.
private struct FarmGeometry
{
    ulong ln, segCap, quotaBulk, quotaSmall, exmax;
}

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
    /// Every failure `tryCreate` reports is fatal here, with the same message.
    static AntFarm* create(uint ringMiB = 8, uint k = 8, uint expectedConsumers = 4,
                           uint maxBulk = 2, ulong quotaBulk = 0,
                           uint maxSmall = 16, ulong quotaSmall = 4096,
                           uint smallThreshold = DEFAULT_SMALL_TABLE_THRESHOLD,
                           bool hugePages = DEFAULT_HUGE_PAGES) nothrow @nogc @system
    {
        FarmCreateError error;
        auto f = tryCreate(ringMiB, k, expectedConsumers, maxBulk, quotaBulk,
            maxSmall, quotaSmall, smallThreshold, hugePages, &error);
        if (f is null)
        {
            version (Windows)
            {
                if (error.winLastError)
                {
                    char[160] buf = void;
                    auto n = snprintf(buf.ptr, buf.length, "%.*s (GetLastError=%u)",
                        cast(int) error.detail.length, error.detail.ptr, cast(uint) error.osError);
                    if (n < 0) n = 0;
                    if (n > buf.length) n = buf.length;
                    fatal(buf[0 .. n]);
                }
            }
            fatal(error.detail);
        }
        return f;
    }

    /// `create` without aborting: returns null and fills `*error` (when
    /// non-null) on any failure, with every partial mapping, mutex and
    /// allocation released. Arguments are exactly `create`'s. Failure kinds
    /// are geometry (see `checkGeometry`), an invalid `ANTFARM_HUGE_PAGES`,
    /// producer mutex initialization, ring mapping and allocation.
    static AntFarm* tryCreate(uint ringMiB = 8, uint k = 8, uint expectedConsumers = 4,
                              uint maxBulk = 2, ulong quotaBulk = 0,
                              uint maxSmall = 16, ulong quotaSmall = 4096,
                              uint smallThreshold = DEFAULT_SMALL_TABLE_THRESHOLD,
                              bool hugePages = DEFAULT_HUGE_PAGES,
                              FarmCreateError* error = null) nothrow @nogc @system
    {
        FarmCreateError local;
        auto err = error is null ? &local : error;
        FarmGeometry g;
        *err = normalizeGeometry(ringMiB, k, expectedConsumers, maxBulk, quotaBulk,
            maxSmall, quotaSmall, g);
        if (err.kind != FarmCreateFailure.none) return null;

        bool hugeSettingValid;
        hugePages = wantHugePages(hugePages, hugeSettingValid);
        if (!hugeSettingValid)
        {
            *err = FarmCreateError(FarmCreateFailure.hugePagesSetting, 0,
                "ANTFARM_HUGE_PAGES must be 0 or 1");
            return null;
        }
        immutable bytes = g.segCap * k * 8;
        auto p = mapMagicBuffer(bytes, hugePages, *err);
        if (p is null) return null;

        auto mem = afAlignedAlloc(AntFarm.sizeof);
        if (mem is null)
        {
            unmapMagicBuffer(p, bytes);
            *err = FarmCreateError(FarmCreateFailure.allocation, 0, "alloc failed");
            return null;
        }
        auto f = cast(AntFarm*) mem;
        *f = AntFarm.init;
        if (immutable mutexError = f.producerMutex.initialize())
        {
            afAlignedFree(mem);
            unmapMagicBuffer(p, bytes);
            *err = FarmCreateError(FarmCreateFailure.mutex, mutexError,
                "producer mutex initialization failed");
            return null;
        }
        // From here f.releaseStorage() unwinds everything acquired so far.
        f.buf = cast(shared(ulong)*) p;
        f.bufBytes = bytes;
        f.usedLargePages = hugePages;
        f.Ln = g.ln;
        f.Lmask = g.ln - 1;
        f.K = k;
        f.kMask = k - 1;
        uint sh = 0;
        while ((1UL << sh) < g.segCap) ++sh;
        f.segShift = sh;
        f.segCap = g.segCap;
        f.expectedConsumers = expectedConsumers;
        f.maxBulk = maxBulk;
        f.quotaBulk = g.quotaBulk;
        f.maxSmall = maxSmall;
        f.quotaSmall = g.quotaSmall;
        f.exmax = g.exmax;
        f.smallThreshold = smallThreshold;
        f.farmId = atomicFetchAdd!(MemoryOrder.raw)(g_farmIssuance, 1UL) + 1;
        if (maxBulk > 0)
        {
            // 64-aligned so slot 0's line cannot bleed into an unrelated
            // heap chunk; zeroed because aligned_alloc does not zero.
            f.prodHashBulk = cast(shared(ulong)*)
                afAlignedAlloc(maxBulk * PROD_SLOT_STRIDE * 8);
            if (f.prodHashBulk is null)
            {
                f.releaseStorage();
                *err = FarmCreateError(FarmCreateFailure.allocation, 0,
                    "alloc producer tickets failed");
                return null;
            }
            memset(cast(void*) f.prodHashBulk, 0, maxBulk * PROD_SLOT_STRIDE * 8);
        }
        if (maxSmall > 0)
        {
            f.prodHashSmall = cast(shared(ulong)*)
                afAlignedAlloc(maxSmall * PROD_SLOT_STRIDE * 8);
            if (f.prodHashSmall is null)
            {
                f.releaseStorage();
                *err = FarmCreateError(FarmCreateFailure.allocation, 0,
                    "alloc producer tickets failed");
                return null;
            }
            memset(cast(void*) f.prodHashSmall, 0, maxSmall * PROD_SLOT_STRIDE * 8);
        }

        foreach (i; 0 .. KMAX)
            atomicStore!(MemoryOrder.raw)(f.stats[i].es, -1L);
        // Spec 4b: epoch 0 starts initialized and Live, so no producer can lap
        // Seq 0 until a consumer confirms epoch 0 and discharges it.
        atomicStore!(MemoryOrder.raw)(f.Rt[0][0], LIVE);
        atomicStore!(MemoryOrder.raw)(f.stats[0].es, 0L);
        atomicStore!(MemoryOrder.raw)(f.stats[0].seqt, 0UL);
        atomicStore!(MemoryOrder.raw)(f.stats[0].sqcs, cast(ulong) sqcsOf(expectedConsumers));
        return f;
    }

    /// Validate geometry exactly as `create`/`tryCreate` do, with no side
    /// effects. `kind == FarmCreateFailure.none` means the arguments are
    /// accepted; otherwise `detail` is the message `create` would abort with.
    /// Quota sums and products are overflow-checked.
    static FarmCreateError checkGeometry(uint ringMiB, uint k, uint expectedConsumers,
        uint maxBulk, ulong quotaBulk, uint maxSmall, ulong quotaSmall)
        pure nothrow @nogc @safe
    {
        FarmGeometry g;
        return normalizeGeometry(ringMiB, k, expectedConsumers, maxBulk, quotaBulk,
            maxSmall, quotaSmall, g);
    }

    private static FarmCreateError normalizeGeometry(uint ringMiB, uint k,
        uint expectedConsumers, uint maxBulk, ulong quotaBulk, uint maxSmall,
        ulong quotaSmall, out FarmGeometry g) pure nothrow @nogc @safe
    {
        static FarmCreateError reject(string detail) pure nothrow @nogc @safe
        {
            return FarmCreateError(FarmCreateFailure.geometry, 0, detail);
        }
        // Construction constraints (spec 1, 4a):
        //  - K is the useful power-of-two range [2, KMAX=16].
        //  - ringMiB >= MIN_RING_MIB (1 MiB, Ln >= 2^17 ulongs). The upper
        //    bound mainly catches a length mistakenly given in ulongs.
        //  - segCap = Ln/K has a floor so header/pad space never dominates a
        //    segment's capacity (subsumed by Ln >= 2^17 with K <= 16).
        //  - Exmax <= (K-1)*segCap so a full quota excursion is strictly
        //    less than a lap (spec 3 safety argument).
        //  - quotaRole > 0 iff maxRole > 0: an enabled role must declare a
        //    positive quota; a disabled role's quota is normalized to 0 and
        //    takes no part in Exmax.
        if (k < 2 || k > KMAX || (k & (k - 1)) != 0) return reject("K must be a power of 2 in [2, KMAX]");
        if (ringMiB < MIN_RING_MIB || (ringMiB & (ringMiB - 1)) != 0)
            return reject("ringMiB must be a power of 2 >= 1 (the ring size is in MiB, not ulongs)");
        if (ringMiB > MAX_RING_MIB)
            return reject("ringMiB exceeds MAX_RING_MIB (the ring size is in MiB, not ulongs)");
        immutable ln = cast(ulong) ringMiB << ULONGS_PER_MIB_SHIFT;
        immutable segCap = ln / k;
        if (segCap < 2048) return reject("segment capacity too small");
        if (expectedConsumers == 0) return reject("expected consumers must be > 0");
        // Quota-role rule (spec 4a): normalize disabled roles, validate
        // enabled ones. Bulk auto-defaults to segCap when enabled with an
        // unspecified quota; small rejects 0.
        if (maxBulk == 0)
            quotaBulk = 0;
        else if (quotaBulk == 0)
            quotaBulk = segCap;
        if (maxSmall == 0)
            quotaSmall = 0;
        else if (quotaSmall == 0)
            return reject("small quota must be > 0");
        if (cast(ulong) maxBulk + maxSmall == 0) return reject("no producer capacity configured");
        bool overflow;
        immutable exmax = addu(mulu(cast(ulong) maxBulk, quotaBulk, overflow),
            mulu(cast(ulong) maxSmall, quotaSmall, overflow), overflow);
        // An overflowing sum certainly exceeds (K-1)*segCap.
        if (overflow) return reject("Exmax exceeds K-1 segments' capacities");
        if (maxBulk > 0 && quotaBulk > exmax) return reject("quota exceeds Exmax");
        if (maxSmall > 0 && quotaSmall > exmax) return reject("quota exceeds Exmax");
        if (exmax > (k - 1) * segCap) return reject("Exmax exceeds K-1 segments' capacities");
        g = FarmGeometry(ln, segCap, quotaBulk, quotaSmall, exmax);
        return FarmCreateError.init;
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
        releaseStorage();
    }

    /// Free the ticket arrays, unmap the ring, destroy the mutex and free
    /// the Farm itself. Shared by destroy and tryCreate's unwinding.
    private void releaseStorage() nothrow @nogc @system
    {
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

    /// Internal: unsubscribe half used by ConsumerView.unsubscribe and failed
    /// subscription attempts. Returns the previous Cf. Cf is a capacity and
    /// sizing count only: no lifetime decision depends on being last (6e).
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
                atomicStore!(MemoryOrder.raw)(*prodSlotQuota(hashes, i), 0UL);
                return Token(t, cast(uint) i, h, farmId, 0);
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
    /// exactly on a boundary. Epoch zero is not fresh (it starts Live).
    /// Do not use wtprime-1 here: an exact landing initializes the next
    /// segment's metadata even though its body storage is not written yet.
    /// A concurrent subscription can add a provisional SUB after the sweep.
    /// Ignore that high half; an active root, or the previous epoch's Live
    /// not yet discharged, remains fatal. This runs before the reservation
    /// raises its own Live on the segment (spec 5c).
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
            immutable live = (low & LIVE) != 0;
            immutable kind = roots != 0
                ? (live ? "roots+Live" : "roots") : (live ? "Live" : "reserved bits");
            fprintf(stderr,
                "allWriteCheck: protected fresh segment kind=%s wret=%llu wtprime=%llu size=%llu e=%llu ki=%llu Rt=%llx roots=%llu Live=%u Sub=%llu Wt=%llu Exmax=%llu K=%u\n",
                kind.ptr, wret, wtprime, wtprime - wret, e, ki, rt,
                roots, live ? 1u : 0u, rt / SUB,
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
            if (roots != 0 && live)
                fatal("allWriteCheck: fresh segment has live roots and is still Live");
            if (roots != 0)
                fatal("allWriteCheck: fresh segment has live roots");
            if (live)
                fatal("allWriteCheck: fresh segment is still Live");
            fatal("allWriteCheck: fresh segment has reserved root bits set");
        }
    }

    /// Whether one payload can fit a singleton table in `tier`, with
    /// `bodyWords` measured in ulongs (8 bytes). Returns false for null
    /// callbacks, size overflow, or a singleton exceeding that tier's quota;
    /// this query never fatals. Uses the compact/uniform singleton cost.
    ///
    /// This is a snapshot of the live consumer count and its SqCs, just as
    /// in write(). Quotas are fixed, but the answer is not monotone: consumer
    /// growth can turn true into false, and departures can turn false into
    /// true. It reserves no quota and does not predict backpressure or how
    /// many payloads a multi-payload write will accept. A later write still
    /// validates independently and fatals on an invalid or oversized payload.
    bool canFit(Callback callback, ulong bodyWords, Tier tier)
            const nothrow @nogc @system
    {
        if (callback is null) return false;
        immutable quota = tier == Tier.bulk ? quotaBulk : quotaSmall;
        immutable csl = atomicLoad!(MemoryOrder.raw)(Cf);
        immutable cs = csl > 0 ? cast(uint) csl : 1;
        immutable base = tableBaseWords(sqcsOf(cs));
        ulong cost;
        return singletonCost(base, bodyWords, cost) && cost <= quota;
    }

    /// Spec 5b. Writes as many of `payloads` as fit the caller's remaining
    /// quota (carried inside `tok`); returns the number written. Returns 0
    /// (full) when backpressure is too high; callers retry or wait on
    /// consumers. `tok` is required, and is passed by reference so the farm
    /// can maintain the quota state; the caller must never inspect or copy
    /// the token's private quota. `avgCost` is the producer's declared Call
    /// cost class, a log2 shift in 0 .. MAX_AVG_COST (spec 5b / 7a).
    ///
    /// An optional completion hook joins every callback in this accepted
    /// physical table, including mixed callbacks. Zero acceptance invokes no
    /// hook. Count a table before publishing: the hook can run before write
    /// returns. Keep its storage stable as required by TableCompletionHook.
    ///
    /// The table layout, and so how many payloads fit the quota, follows the
    /// accepted prefix's contents in every overload: an
    /// prefix sharing one Call stores it once and drops the per-payload Call
    /// word, and one sharing a nonzero body length as well drops Tindex
    /// (spec 2g); other prefixes are compact (2f).
    ulong write(scope PayloadEntry[] payloads, ref Token tok,
                uint avgCost = DEFAULT_AVG_COST,
                const(TableCompletionHook)* completion = null) nothrow @nogc @system
    {
        return writeImpl(payloads, tok, avgCost, completion);
    }

    /// ditto: forward range overload. `write` checkpoints the source for its
    /// sizing and emission passes and never advances the caller's checkpoint.
    ulong write(R)(scope R payloads, ref Token tok,
                   uint avgCost = DEFAULT_AVG_COST,
                   const(TableCompletionHook)* completion = null) nothrow @nogc @system
        if (isForwardRange!R && is(ElementType!R == PayloadEntry))
    {
        return writeImpl(payloads, tok, avgCost, completion);
    }

    /// Write jobs from independent callback and body input ranges.  Pairing is
    /// positional and stops at the shorter range.  This overload is a lazy
    /// shim: it does not allocate or materialize `PayloadEntry` pairs.
    ulong write(CR, BR)(scope CR callbacks, scope BR bodies, ref Token tok,
                        uint avgCost = DEFAULT_AVG_COST,
                        const(TableCompletionHook)* completion = null) nothrow @nogc @system
        if (isForwardRange!CR && isForwardRange!BR &&
            is(ElementType!CR : Callback) &&
            is(ElementType!BR : PayloadBody))
    {
        return writeImpl(pairPayloads(callbacks, bodies), tok, avgCost, completion);
    }

    /// Broadcast one common callback over a forward range of bodies. The
    /// common callback is validated once per call rather than once per body.
    ulong write(BR)(Callback callback, scope BR bodies,
                    ref Token tok, uint avgCost = DEFAULT_AVG_COST,
                    const(TableCompletionHook)* completion = null) nothrow @nogc @system
        if (isForwardRange!BR && is(ElementType!BR : PayloadBody))
    {
        return writeImpl(broadcastPayloads(callback, bodies), tok, avgCost,
            completion);
    }

    /// Broadcast a common callback over bodies whose uniform length is supplied
    /// by contract. This selects arithmetic sizing even when the body range's
    /// element type is a runtime slice.
    ulong write(BR)(Callback callback, scope BR bodies,
                    ulong bodyWords, ref Token tok,
                    uint avgCost = DEFAULT_AVG_COST,
                    const(TableCompletionHook)* completion = null) nothrow @nogc @system
        if (isForwardRange!BR && is(ElementType!BR : PayloadBody))
    {
        return writeImpl(broadcastPayloads(callback, bodies, bodyWords), tok,
            avgCost, completion);
    }

    private ulong writeImpl(R)(scope R payloads, ref Token tok,
                              uint avgCost,
                              const(TableCompletionHook)* completion)
        nothrow @nogc @system
    {
        requireToken(tok);
        if (completion !is null && !completion.valid)
            fatal("table completion hook requires a callback");
        if (payloads.empty) return 0;
        if (avgCost > MAX_AVG_COST) fatal("avgCost out of range");
        immutable quota = tok.tier == Tier.bulk ? quotaBulk : quotaSmall;
        immutable csl = atomicLoad!(MemoryOrder.raw)(Cf);
        immutable cs = csl > 0 ? cast(uint) csl : 1;
        immutable sq = sqcsOf(cs);

        // Fit as many payloads as the quota and the table payload cap
        // allow (spec 5b truncation, 2e).
        immutable base = tableBaseWords(sq);
        // Spec 2e: a committed table carries fewer than 2^31 payloads.
        immutable size_t maxTablePayloads = 2_147_483_647; // 2^31 - 1
        size_t n;
        ulong psum;
        // Spec 2f/2g: the accepted prefix's layout and, for the uniform
        // layouts, its shared Call and (index-free) body length.
        TableLayout layout;
        Callback tcall;
        ulong tblen;

        static if (hasCommonPayloadCallback!R)
        {
            auto commonCall = payloads.commonPayloadCallback;
            validateCallback(commonCall);
        }

        for (;;)
        {
            n = 0; psum = 0;
            static if (hasCommonPayloadCallback!R && hasFixedPayloadLength!R)
            {
                immutable bodyLen = cast(ulong) payloads.fixedPayloadLength;
                checkPayloadFits(base, quota, bodyLen);
                // Spec 2g: a uniform range shares its Call by
                // construction, and a nonzero uniform length drops Tindex.
                layout = bodyLen != 0 ? TableLayout.uniformFixed : TableLayout.uniform;
                tcall = commonCall;
                tblen = bodyLen;
                immutable head = base + extWordsOf(layout);
                // Per payload: index words, record words and the body.
                // Never zero: index-free tables have nonempty bodies.
                immutable rec = addChecked(recordWordsOf(layout), bodyLen,
                    "table size overflow");
                immutable unit = addChecked(indexWordsOf(layout, 1), rec,
                    "table size overflow");
                static if (hasLength!R)
                {
                    immutable available = cast(size_t) payloads.length;
                    immutable maxFit = tok.quotaLeft > head
                        ? (tok.quotaLeft - head) / unit : 0;
                    n = available;
                    if (cast(ulong) n > maxFit)
                        n = cast(size_t) maxFit;
                    if (n > maxTablePayloads) n = maxTablePayloads;
                    psum = mulChecked(cast(ulong) n, rec,
                        "payload sum overflow");
                }
                else
                {
                    auto scan = payloads.save;
                    while (!scan.empty)
                    {
                        if (n >= maxTablePayloads) break;
                        immutable nextN = cast(ulong) n + 1;
                        immutable cand = addChecked(head,
                            mulChecked(nextN, unit, "table size overflow"),
                            "table size overflow");
                        if (cand > tok.quotaLeft) break;
                        ++n;
                        scan.popFront();
                    }
                    if (n > maxTablePayloads) fatal("too many payloads in table");
                    psum = mulChecked(cast(ulong) n, rec,
                        "payload sum overflow");
                }
            }
            else
            {
                // Phased scan. A prefix can only lose uniformity, so each
                // layout gets a tight loop with its overhead hoisted; the
                // degrading entry is admitted only if the whole prefix still
                // fits under the next layout (otherwise keep the accepted one).
                ulong bodySum;
                bool stop;
                auto scan = payloads.save;
                {
                    auto pe = scan.front;
                    static if (hasCommonPayloadCallback!R)
                        tcall = commonCall;
                    else
                    {
                        tcall = pe.call;
                        validateCallback(tcall);
                    }
                    tblen = cast(ulong) pe.body.length;
                    checkPayloadFits(base, quota, tblen);
                    layout = tblen != 0 ? TableLayout.uniformFixed : TableLayout.uniform;
                }
                if (layout == TableLayout.uniformFixed)
                {
                    immutable head = base + UNIFORM_FIXED_EXT;
                    while (!scan.empty && n < maxTablePayloads)
                    {
                        auto pe = scan.front;
                        static if (!hasCommonPayloadCallback!R)
                            if (pe.call !is tcall) break;
                        immutable blen = cast(ulong) pe.body.length;
                        if (blen != tblen) break;
                        immutable nextBody = addChecked(bodySum, blen, "payload sum overflow");
                        if (addChecked(head, nextBody, "table size overflow") > tok.quotaLeft)
                        { stop = true; break; }
                        ++n;
                        bodySum = nextBody;
                        scan.popFront();
                    }
                    psum = bodySum;
                }
                bool degraded;
                if (!stop)
                {
                    immutable head = base + UNIFORM_EXT;
                    while (!scan.empty && n < maxTablePayloads)
                    {
                        auto pe = scan.front;
                        static if (!hasCommonPayloadCallback!R)
                        {
                            if (pe.call is null) validateCallback(pe.call);
                            if (pe.call !is tcall) { degraded = true; break; }
                        }
                        immutable blen = cast(ulong) pe.body.length;
                        checkPayloadFits(base, quota, blen);
                        immutable nextN = cast(ulong) n + 1;
                        immutable nextBody = addChecked(bodySum, blen, "payload sum overflow");
                        if (addChecked(head + nextN, nextBody, "table size overflow") > tok.quotaLeft)
                        { stop = true; break; }
                        ++n;
                        bodySum = nextBody;
                        psum = nextBody;
                        layout = TableLayout.uniform;
                        scan.popFront();
                    }
                }
                static if (!hasCommonPayloadCallback!R)
                    if (!stop && degraded)
                    {
                        // Include callback words for the accepted uniform prefix.
                        ulong records = PCOMPACT_LEN * cast(ulong) n + bodySum;
                        while (!scan.empty && n < maxTablePayloads)
                        {
                            auto pe = scan.front;
                            validateCallback(pe.call);
                            immutable blen = cast(ulong) pe.body.length;
                            checkPayloadFits(base, quota, blen);
                            immutable nextN = cast(ulong) n + 1;
                            immutable nextRecords = addChecked(records,
                                addChecked(PCOMPACT_LEN, blen, "payload sum overflow"),
                                "payload sum overflow");
                            if (addChecked(base + nextN, nextRecords, "table size overflow") > tok.quotaLeft)
                                break;
                            ++n;
                            records = nextRecords;
                            psum = nextRecords;
                            layout = TableLayout.compact;
                            scan.popFront();
                        }
                    }
                if (n == 0) layout = TableLayout.compact;
            }
            if (n > 0) break;
            if (!refreshQuota(tok.quotaLeft, quota,
                    atomicLoad!(MemoryOrder.acq)(Wt))) return 0;
            syncQuota(tok); // ledger follows a successful refresh
        }

        immutable size = tableSizeChecked(
            extWordsOf(layout) + indexWordsOf(layout, n), sq, psum);

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

        // Segment/epoch transitions (spec 2c, 5c, 6c): initialize metadata for
        // each crossed segment and raise its Live, release-storing Es last.
        // Live protects the epoch from here until a holder confirms and
        // discharges it, so no later step needs to manufacture protection.
        // The raise is a release RMW: a subscriber that acquires this Live
        // under a stale Es also acquires this reservation, and its tail
        // re-read then rejects the old epoch (6b). The fresh-segment check
        // above already saw a zero low half; a raise that finds otherwise
        // means a reference or Live arrived on a slot that was never covered.
        foreach (e; eold + 1 .. enew + 1)
        {
            immutable ki = e & kMask;
            atomicStore!(MemoryOrder.raw)(stats[ki].seqt, wtprime);
            atomicStore!(MemoryOrder.raw)(stats[ki].sqcs, cast(ulong) sq);
            atomicStore!(MemoryOrder.raw)(stats[ki].sd, 0UL); // fresh epoch: nothing consumed
            immutable before = atomicFetchAdd!(MemoryOrder.rel)(Rt[ki][0], LIVE);
            if ((before & LOWMASK) != 0)
                fatal("Live raised on a segment that still carries protection");
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(&this, WriteAuditPhase.liveRaised, e, 0);
            atomicStore!(MemoryOrder.rel)(stats[ki].es, cast(long) e);
        }

        // Write the table (spec 5c/2e). The magic buffer makes the linear
        // address range contiguous even across the Ln wrap point. Ring words
        // are stored raw: consumers load-raw after acquiring Tsent, and a
        // later lap reuses the same physical words.
        auto w = buf + (wret & Lmask);
        immutable tindexOff = THEAD_LEN + extWordsOf(layout);
        immutable progOff = tindexOff + indexWordsOf(layout, n) + PROG_PAD;
        immutable tcountOff = progOff + 8;
        immutable payOff = tcountOff + 8 * sq;

        storeRaw(w[1], wtprime);                          // Tnext
        // Low 32 bits: Tlen; next bytes: kind and AvgCost; top 16 reserved.
        storeRaw(w[2], cast(uint) n | (cast(ulong) tableKindOf(layout) << 32)
            | (cast(ulong) avgCost << 40));
        storeRaw(w[3], sq);
        storeRaw(w[4], cast(ulong) cast(const(void)*) completion);
        if (layout == TableLayout.uniform || layout == TableLayout.uniformFixed)
            storeRaw(w[THEAD_LEN], cast(ulong) cast(void*) tcall); // Tcall
        if (layout == TableLayout.uniformFixed)
            storeRaw(w[THEAD_LEN + 1], tblen);           // Tblen

        storeRaw(w[progOff], 0);                          // Tprogress
        storeRawRange(w + tcountOff, 8 * sq);             // Tcount shards

        // Emit both index sections and payload storage in one checkpointed
        // traversal. Every ring word is written through raw atomic stores.
        ulong po = payOff;
        size_t i = 0;
        auto emit = payloads.save;
        if (layout == TableLayout.uniformFixed)
        {
            // Spec 2g: bare bodies at payOff + i * Tblen, no Tindex.
            while (i < n)
            {
                auto pe = emit.front;
                foreach (k; 0 .. pe.body.length)
                    storeRaw(w[po + k], pe.body[k]);
                po += pe.body.length;
                ++i;
                emit.popFront();
            }
        }
        else if (layout == TableLayout.uniform)
        {
            // Spec 2g: bare bodies, Tindex holds each body's offset.
            while (i < n)
            {
                auto pe = emit.front;
                storeRaw(w[tindexOff + i], po);
                foreach (k; 0 .. pe.body.length)
                    storeRaw(w[po + k], pe.body[k]);
                po += pe.body.length;
                ++i;
                emit.popFront();
            }
        }
        else if (layout == TableLayout.compact)
        {
            // Spec 2f: a compact table stores [Call][Pbody] records.
            while (i < n)
            {
                auto pe = emit.front;
                static if (hasCommonPayloadCallback!R)
                    auto hp = commonCall;
                else
                    auto hp = pe.call;

                storeRaw(w[tindexOff + i], po);
                auto ph = w + po;
                storeRaw(ph[0], cast(ulong) cast(void*) hp);
                foreach (k; 0 .. pe.body.length)
                    storeRaw(ph[PCOMPACT_LEN + k], pe.body[k]);
                po += PCOMPACT_LEN + pe.body.length;
                ++i;
                emit.popFront();
            }
        }
        // Compact and uniform readers derive the last Plen from this end point.
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
///  - Live (2b/6c) is raised by the initializing producer and cleared only
///    by a holder that observes confirmation, before it releases that
///    epoch. Releasing an unconfirmed epoch leaves its Live in place, so
///    abandoned work stays protected and discoverable.
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

    /// Internal: oldest-epoch re-walk cursor (spec 8): sequence up to which
    /// every table of the oldest held segment was observed complete
    /// (Tprogress == Tlen). Reset when oldestEi advances.
    ulong oldSweepSeq;

    version (AntfarmSweepStats)
    {
        /// Test instrumentation: tables visited by the oldest-epoch
        /// re-walk (sweepOldestTrailing), cumulative.
        ulong oldSweepTables;
    }

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

    /// Subscribe this view to `f`. Returns the non-negative starting epoch,
    /// or SUBSCRIBE_FULL (the Farm already has MAX_CONSUMERS_LIMIT views) or
    /// SUBSCRIBE_INVALID (null Farm, or this view is already subscribed).
    /// The transient outcome that `subscribeOnce` reports as SUBSCRIBE_RETRY
    /// (spec 6b) is retried here immediately.
    long subscribe(AntFarm* f) nothrow @nogc @system
    {
        long result;
        do result = subscribeOnce(f);
        while (result == SUBSCRIBE_RETRY);
        return result;
    }

    /// Spec 6b, one attempt. Returns the (non-negative) starting epoch on
    /// success, or SUBSCRIBE_FULL, SUBSCRIBE_RETRY (the tail kept moving past
    /// every candidate for this attempt's bounded rounds; every pin was
    /// unwound and the registration undone, call again), or
    /// SUBSCRIBE_INVALID. `subscribe` is this with the retry folded in.
    long subscribeOnce(AntFarm* f) nothrow @nogc @system
    {
        if (f is null || hasRef) return SUBSCRIBE_INVALID;
        immutable idc = f.add_consumer();
        if (idc < 0) return idc;
        version (AntfarmWriteAuditHooks)
            if (writeAuditHook !is null)
                writeAuditHook(f, WriteAuditPhase.subscriberCounted, 0, 0);

        // Physical slots this attempt holds a Sub on (K <= KMAX = 16).
        uint held;
        void pin(uint ki, ulong eg) nothrow @nogc @system
        {
            if ((held >> ki) & 1) return;
            atomicFetchAdd!(MemoryOrder.rel)(f.Rt[ki][0], SUB);
            held |= 1u << ki;
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberPinned, ki, eg);
        }
        void unpin(uint ki) nothrow @nogc @system
        {
            atomicFetchSub!(MemoryOrder.rel)(f.Rt[ki][0], SUB);
            held &= ~(1u << ki);
        }
        void unpinAll() nothrow @nogc @system
        {
            foreach (uint ki; 0 .. f.K)
                if ((held >> ki) & 1)
                    unpin(ki);
        }
        // Whether the slot of window epoch x names x and is still Live. Es
        // is loaded first: a matching Es was release-stored after that
        // epoch's Live raise, so the Rt load then observes the raise, and a
        // clear Live means discharged rather than "not yet raised". An older
        // Es is an initialization still in flight; a newer one is a slot
        // already reused. Both loads are acquire (spec 6b, 9). Discovery does
        // not rely on the order; it keeps every classification exact.
        bool liveAt(ulong x) nothrow @nogc @system
        {
            immutable ki = cast(uint)(x & f.kMask);
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberRead, ki, x);
            immutable es = atomicLoad!(MemoryOrder.acq)(f.stats[ki].es);
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberReadMid, ki, x);
            immutable rt = atomicLoad!(MemoryOrder.acq)(f.Rt[ki][0]);
            return es == cast(long) x && (rt & LIVE) != 0;
        }

        // Spec 6b: rounds. Each probes the tail, holds a Sub on the newest
        // slot, and scans the probed window oldest-first with acquire loads
        // only; the first Live slot naming its window epoch is the earliest
        // Live epoch the round saw. That candidate is pinned and re-read
        // before it is validated. A round fails only when the tail moved
        // past what it read; the newest slot's Sub keeps the tail within one
        // lap of the first probe, so the bound below is a budget, not a
        // proof of success: exhausting it returns the recoverable RETRY.
        foreach (ulong round; 0 .. f.K + 2)
        {
            immutable wt = atomicLoad!(MemoryOrder.acq)(f.Wt);
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberProbed, wt, round);
            immutable eg = wt >> f.segShift;
            immutable ulong lo = eg + 1 > f.K ? eg + 1 - f.K : 0;
            pin(cast(uint)(eg & f.kMask), eg);
            long cand = -1;
            bool candFresh;
            foreach (ulong x; lo .. eg + 1)
            {
                if (!liveAt(x)) continue;
                immutable ki = cast(uint)(x & f.kMask);
                candFresh = ((held >> ki) & 1) == 0;
                pin(ki, eg);
                if (liveAt(x))
                {
                    cand = cast(long) x;
                    break;
                }
                // Discharged or reused between the hint and the pin.
                if (candFresh) unpin(ki);
            }
            if (cand < 0) continue;

            // Validate under the candidate's Sub. Es must still name the
            // candidate: a new lap's Live can be acquired before its Es is
            // published, and that acquire imports the new lap's reservation,
            // so the tail re-read below rejects the old epoch. Seqt must lie
            // in [Es, Es + K - 1] and the tail must not have reserved the
            // candidate's next lap.
            immutable seg = cast(uint)(cast(ulong) cand & f.kMask);
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(f, WriteAuditPhase.subscriberValidating, seg, cast(ulong) cand);
            immutable es = atomicLoad!(MemoryOrder.acq)(f.stats[seg].es);
            immutable sq = cast(uint) atomicLoad!(MemoryOrder.raw)(f.stats[seg].sqcs);
            immutable seqt = atomicLoad!(MemoryOrder.raw)(f.stats[seg].seqt);
            immutable wt1 = atomicLoad!(MemoryOrder.acq)(f.Wt);
            immutable esU = cast(ulong) es;
            immutable seqtEpoch = seqt >> f.segShift;
            if (es != cand || sq == 0 ||
                seqtEpoch < esU || seqtEpoch > esU + f.K - 1 ||
                (wt1 >> f.segShift) >= esU + f.K)
            {
                if (candFresh) unpin(seg);
                continue;
            }
            // Seqt beyond the candidate's epoch names the end of the write
            // that crossed into it, so the first table lies in a later epoch
            // and the first advance pins every epoch up to it (6d). Those
            // epochs' Es were stored by that producer after Es[cand], and no
            // edge acquired so far orders them: the discharges and reuses
            // this scan passed carry only their own successor's Es, and the
            // tail probe carries only reservations. Acquire the last of them
            // here; its release-store follows every earlier one, so the
            // attach imports them all. A mismatch is that initialization
            // still in flight (the tail bound above rules out reuse), and
            // the round ends.
            if (seqtEpoch > esU &&
                atomicLoad!(MemoryOrder.acq)(f.stats[cast(uint)(seqtEpoch & f.kMask)].es)
                    != cast(long) seqtEpoch)
            {
                if (candFresh) unpin(seg);
                continue;
            }
            F = f;
            IDc = cast(ulong) idc;
            immutable lti = cast(uint)(IDc % sq);
            incLeaf(seg, lti);
            // Spec 6b: the reference now protects the epoch; drop every Sub.
            unpinAll();
            nextSeq = seqt;
            oldestEi = newestEi = esU;
            ltiRing[seg] = lti;
            sweeperNext = false;
            sweepSeq = 0;
            oldSweepSeq = 0;
            sdCheckCounter = 0;
            hasRef = true;
            return es;
        }
        unpinAll();
        f.sub_consumer();
        return SUBSCRIBE_RETRY;
    }

    /// Spec 6e. Release the confirmed prefix (discharging it), then every
    /// remaining held epoch: a confirmed one is discharged before its
    /// reference drops, and an unconfirmed one keeps its Live, which
    /// protects the abandoned work until a later holder confirms it. No
    /// case depends on whether this view was the Farm's last.
    void unsubscribe() nothrow @nogc @system
    {
        if (!hasRef) return;
        auto f = F;
        tryReleaseOldest();
        f.sub_consumer();
        foreach (e; oldestEi .. newestEi + 1)
        {
            immutable ki = cast(uint)(e & f.kMask);
            if (confirmed(ki, e))
                clearLive(ki);
            decRef(ki, ltiOf(e));
        }
        hasRef = false;
        IDc = 0;
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

        immutable table = decodeTable(bp, idx);
        immutable tnext = table.next;
        immutable tlen = table.length;
        immutable sq = table.shards;
        immutable layout = table.layout;
        immutable tindexOff = table.index;
        immutable progOff = table.progress;
        immutable tcountOff = table.counts;

        // Skipping an already-complete table preserves the carried sweeper role.
        if (tlen > 0)
        {
            immutable myShi = cast(uint)((cast(ulong) IDc + nextSeq) % sq);
            bool sweeper;
            if (atomicLoad!(MemoryOrder.raw)(bp[progOff]) != tlen)
            {
                // Spec 7a: assigned-shard path. Completing a shard gains the
                // sweeper role (7e).
                sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                        progOff, tlen, layout, sq, myShi, false, false) & 1) != 0;
                // Spec 7e carried role: small tables concentrate on shard 0
                // (7a); a carried sweeper sweeps them regardless of Shi.
                if (!sweeper && sweeperNext && tlen < smallThresh(sq, bp, idx) && myShi != 0)
                    sweeper = (processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                            progOff, tlen, layout, sq, 0, true, false) & 1) != 0;
                if (sweeper)
                {
                    immutable nshards = tlen < smallThresh(sq, bp, idx) ? 1 : sq;
                    bool adopted;
                    foreach (s; 0 .. nshards)
                        if (s != myShi)
                        {
                            immutable r = processShard(bp, nextSeq, idx, tindexOff, tcountOff,
                                                       progOff, tlen, layout, sq, s, true, !adopted);
                            if (r & 2) adopted = true;
                        }
                }
                sweeperNext = sweeper;
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
    struct TableView
    {
        ulong next;
        uint length, shards;
        TableLayout layout;
        ulong index, progress, counts;
    }

    // Caller has acquired this table's sentinel and holds its epoch pin.
    static TableView decodeTable(shared(ulong)* bp, ulong idx) nothrow @nogc @system
    {
        pragma(inline, true);
        TableView table;
        table.next = loadRaw(bp[idx + 1]);
        immutable word = loadRaw(bp[idx + 2]);
        table.length = cast(uint) word;
        table.shards = cast(uint) loadRaw(bp[idx + 3]);
        table.layout = tableLayoutOf(word);
        table.index = idx + THEAD_LEN + extWordsOf(table.layout);
        table.progress = table.index + indexWordsOf(table.layout, table.length) + PROG_PAD;
        table.counts = table.progress + 8;
        return table;
    }

    /// Spec 7a: chunk for this table from its published AvgCost. Cheap
    /// classes get big chunks (claim amortization), expensive classes get
    /// small chunks (short visits, less leftover tail). Clamped so a
    /// corrupted header can never produce a zero chunk. All consumers read
    /// the same header word, so Tcount shiter arithmetic cannot disagree.
    uint chunkOf(shared(ulong)* bp, ulong tseqIdx) nothrow @nogc @system
    {
        immutable ac = cast(ubyte)(atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 2]) >> 40);
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

    /// Spec 6c: propagate a leaf's 0→1 edge to the root. Nothing else in the
    /// root changes: Live belongs to the epoch, not to its first reference.
    void takeRootCount(uint ki) nothrow @nogc @system
    {
        atomicFetchAdd!(MemoryOrder.rel)(F.Rt[ki][0], 1);
    }

    /// Spec 6c: discharge a confirmed epoch this view still holds. The caller
    /// observed confirmation, including the acquire of the next epoch's Es,
    /// and keeps its own reference until after this returns, so the slot
    /// cannot be reused under it. The release CAS forwards that history to
    /// any sweep that later acquires the root (spec 3, I3). Load first: a
    /// passer that finds Live already clear does not write the root line.
    /// Idempotent across confirmers; the CAS retries only while another root
    /// field changes under it.
    void clearLive(uint ki) nothrow @nogc @system
    {
        for (;;)
        {
            immutable old = atomicLoad!(MemoryOrder.acq)(F.Rt[ki][0]);
            if ((old & LIVE) == 0)
                return;
            version (AntfarmWriteAuditHooks)
                if (writeAuditHook !is null)
                    writeAuditHook(F, WriteAuditPhase.liveClearing, ki, old);
            if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&F.Rt[ki][0], old, old - LIVE))
                return;
        }
    }

    /// Increment a leaf; on 0→1 propagate to Rt.
    void incLeaf(uint ki, uint lti) nothrow @nogc @system
    {
        immutable lo = atomicFetchAdd!(MemoryOrder.acq_rel)(F.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo == 0)
            takeRootCount(ki);
    }

    /// Decrement a held leaf tally with root propagation (spec 2b). Edge
    /// transition (dec returns 1) repeats on Rt; underflows are fatal. The
    /// root decrement never touches Live; discharge is clearLive's job.
    void decRef(uint ki, uint lti) nothrow @nogc @system
    {
        auto f = F;
        immutable lo = atomicFetchSub!(MemoryOrder.acq_rel)(f.Lt[ki * MAX_LEAVES + lti][0], 1);
        if (lo <= 0) fatal("leaf tally underflow");
        if (lo == 1)
        {
            immutable ro = atomicFetchSub!(MemoryOrder.rel)(f.Rt[ki][0], 1);
            if ((ro & COUNTMASK) == 0) fatal("root tally underflow");
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

    /// Release confirmed prefix epochs from the old end only, discharging
    /// each before its reference drops (6c). Confirmed interiors stay pinned
    /// until they become oldest.
    void tryReleaseOldest() nothrow @nogc @system
    {
        auto f = F;
        while (oldestEi < newestEi)
        {
            immutable ki = cast(uint)(oldestEi & f.kMask);
            if (!confirmed(ki, oldestEi))
                break;
            clearLive(ki);
            decRef(ki, ltiOf(oldestEi));
            ++oldestEi;
            oldSweepSeq = 0; // new oldest segment: re-walk from its Seqt
        }
    }

    /// Idle path: re-walk the oldest unconfirmed held segment claiming any
    /// leftover work, so the segment confirms and its reference can be
    /// released. The data is guaranteed present and intact: we hold a
    /// reference on the segment, so producers cannot have overwritten it.
    /// The walk resumes at the oldSweepSeq cursor, which advances only over
    /// the leading run of tables observed complete. A table another
    /// consumer is still finishing (for example a callback parked in
    /// its callback) stops the cursor, so a stalled oldest epoch costs each
    /// park O(tables from the first incomplete one) rather than
    /// O(tables in the segment).
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
        rewalk(seqt, boundary, oldSweepSeq, true);
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
        rewalk(seqt, end, sweepSeq, false);
        tryReleaseOldest();
    }

    // Both walks claim every leftover run. The trailing cursor advances only
    // over the prefix observed complete before this pass; current-position
    // visits may pass in-flight runs because their owners must finish them.
    // Neither policy releases a pin: confirmation still controls that.
    void rewalk(ulong from, ulong to, ref ulong cursor, bool trailing)
        nothrow @nogc @system
    {
        auto bp = F.buf;
        ulong seq = from > cursor ? from : cursor;
        bool drained = true;
        while (seq < to)
        {
            version (AntfarmSweepStats) if (trailing) ++oldSweepTables;
            immutable idx = seq & F.Lmask;
            if (atomicLoad!(MemoryOrder.acq)(bp[idx]) != sentinelOf(seq))
                fatal("held segment's table was overwritten during rewalk");
            immutable table = decodeTable(bp, idx);
            // Raw skip checks are cursor hints, never application joins. Our
            // pin and monotonic Tprogress keep previously completed tables so.
            if (table.length > 0 && loadRaw(bp[table.progress]) != table.length)
            {
                drained = false;
                immutable nshards = table.length < smallThresh(table.shards, bp, idx)
                    ? 1 : table.shards;
                foreach (shard; 0 .. nshards)
                    processShard(bp, seq, idx, table.index, table.counts,
                        table.progress, table.length, table.layout, table.shards,
                        shard, true, false);
            }
            seq = table.next;
            if (!trailing || drained) cursor = seq;
        }
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

    /// Spec 2f/2g: execute one claimed run. Indexed layouts derive lengths
    /// from adjacent offsets, ending at Tnext - Tseq - END_PAD. Index-free
    /// bodies use Tblen. The claim gives every payload exactly one visitor.
    void runCompact(TableLayout layout)(shared(ulong)* bp, ulong tseq, ulong tseqIdx,
                    ulong tindexOff, ulong payOff, uint tlen, uint runstart,
                    uint runlen) nothrow @nogc @system
    {
        pragma(inline, true);
        auto callbackScope = FarmCallbackScope(true);
        static if (layout == TableLayout.uniformFixed)
        {
            immutable fn = cast(Callback) loadRaw(bp[tseqIdx + THEAD_LEN]);
            immutable blen = loadRaw(bp[tseqIdx + THEAD_LEN + 1]);
            auto p = cast(const(ulong)*)(bp + payOff + runstart * blen);
            foreach (j; 0 .. runlen)
            {
                fn(p, cast(size_t) blen);
                p += blen;
            }
        }
        else
        {
            enum ulong recLen = layout == TableLayout.compact ? PCOMPACT_LEN : 0;
            static if (layout == TableLayout.uniform)
                immutable fn = cast(Callback) loadRaw(bp[tseqIdx + THEAD_LEN]);
            immutable runend = runstart + runlen;
            immutable endOff = runend < tlen
                ? loadRaw(bp[tindexOff + runend])
                : loadRaw(bp[tseqIdx + 1]) - tseq - END_PAD;
            ulong poff = loadRaw(bp[tindexOff + runstart]);
            foreach (j; runstart + 1 .. runend + 1)
            {
                immutable next = j < runend ? loadRaw(bp[tindexOff + j]) : endOff;
                immutable absIdx = tseqIdx + poff;
                immutable blen = next - poff - recLen;
                static if (layout == TableLayout.compact)
                    immutable fn = cast(Callback) loadRaw(bp[absIdx]);
                fn(cast(const(ulong)*)(bp + absIdx + recLen), cast(size_t) blen);
                poff = next;
            }
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
                      ulong tcountOff, ulong progOff, uint tlen, TableLayout layout, uint sq,
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
            // One layout branch per claimed run, not per payload.
            if (layout == TableLayout.uniformFixed)
                runCompact!(TableLayout.uniformFixed)(bp, tseq, tseqIdx, tindexOff,
                    tcountOff + 8UL * sq, tlen, runstart, runlen);
            else if (layout == TableLayout.uniform)
                runCompact!(TableLayout.uniform)(bp, tseq, tseqIdx, tindexOff, 0,
                    tlen, runstart, runlen);
            else if (layout == TableLayout.compact)
                runCompact!(TableLayout.compact)(bp, tseq, tseqIdx, tindexOff, 0,
                    tlen, runstart, runlen);
            // Spec 7a-e: the consumer adding the final completion sum for
            // the shard increments Tprogress by the shard length, whichever
            // consumer (owner, sweeper, or re-walker) that happens to be.
            // Join every chunk's run callback writes before the final
            // chunk forwards this shard's completion into Tprogress. A relaxed
            // completion sum would join only the shard finisher's writes.
            // Every payload callback contributes through its claimed run.
            immutable y = atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, cast(ulong) runlen);
            if ((y & 0xFFFF_FFFFUL) > 0xFFFF_FFFFUL - runlen) fatal("Tcount comps wrap");
            if ((y & 0xFFFF_FFFFUL) == shlen - runlen)
            {
                // Tprogress is also a completion join: each shard publishes
                // its run callback writes, and the last RMW acquires
                // the release chain before notifying an aggregate table
                // observer.
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
                    immutable tsize = atomicLoad!(MemoryOrder.raw)(bp[tseqIdx + 1]) - tseq;
                    immutable tki = cast(uint)((tseq >> F.segShift) & F.kMask);
                    atomicFetchAdd!(MemoryOrder.raw)(F.stats[tki].sd, tsize);
                    immutable hookWord = atomicLoad!(MemoryOrder.raw)(
                        bp[tseqIdx + 4]);
                    if (hookWord != 0)
                    {
                        auto hook = cast(const(TableCompletionHook)*) cast(const(void)*)
                            hookWord;
                        // Copy both fields before calling: completion may
                        // release the last external owner of the hook.
                        immutable notify = hook.call;
                        auto context = cast(void*) hook.context;
                        if (notify is null)
                            fatal("table completion hook lost callback");
                        auto callbackScope = FarmCallbackScope(true);
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
}
