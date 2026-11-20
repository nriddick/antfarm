/++
 + The secure mimalloc instance (antfarm.secure_allocation), linked alone or
 + beside the fast instance in one D image:
 +
 + - both report the pinned version, and the secure one is a CHECKED build:
 +   a free of memory it does not own returns without touching it (an
 +   unchecked build follows the pointer into metadata that is not there);
 + - the D garbage collector collects with the secure instance live (no guard
 +   page in the static data it scans), and on Linux and Windows the secure
 +   instance's sealed process pages are read-only;
 + - eight threads allocate from both instances, check that ownership never
 +   overlaps, free each other's blocks and collect;
 + - 64 short-lived threads use the secure instance, and its thread count
 +   returns to where it was (thread exit reaches the renamed instance; on
 +   Windows that needs its TLS callbacks, which the build keeps);
 + - the module's own unittest.
 +
 + Built without AntfarmMimallocSecure it checks that every function falls
 + back to the C runtime.
 +/
module secure_test;

import antfarm.secure_allocation;
import core.atomic;
import core.memory : GC;
import core.stdc.stdio : fflush, printf, stdout;
import core.stdc.stdlib : exit, free, malloc;
import core.stdc.string : memset;
import core.thread : Thread;

enum int pinnedMimallocVersion = 30_700;

__gshared int failures;

void check(bool ok, const(char)* what)
{
    printf("%s %s\n", ok ? "PASS".ptr : "FAIL".ptr, what);
    if (!ok) ++failures;
}

version (AntfarmMimallocSecure)
{
    extern (C) nothrow @nogc @system
    {
        void afsec_mi_free(void* memory);
        void* afsec_mi_malloc_aligned(size_t size, size_t alignment);
        bool afsec_mi_is_in_heap_region(const(void)* memory);
        char* afsec_mi_stats_get_json(size_t size, char* buffer);
    }

    // threads.total and threads.current from the secure instance's statistics.
    long[2] secureThreads()
    {
        import std.conv : to;
        import std.regex : matchFirst, regex;
        import std.string : fromStringz;
        auto json = afsec_mi_stats_get_json(0, null);
        scope (exit) secureFree(json);
        auto m = matchFirst(json.fromStringz,
            regex(`"threads": \{ "total": (\d+), "peak": \d+, "current": (\d+)`));
        if (m.empty) return [-1, -1];
        return [m[1].to!long, m[2].to!long];
    }
}

version (AntfarmMimallocV3)
{
    extern (C) nothrow @nogc @system
    {
        int mi_version();
        void* mi_malloc_aligned(size_t size, size_t alignment);
        void mi_free(void* memory);
        bool mi_is_in_heap_region(const(void)* memory);
    }
}

__gshared ubyte[][] keep; // the optimizer must not drop the GC allocations

void collectWithGc()
{
    keep ~= new ubyte[](4096);
    GC.collect();
}

version (linux) version (AntfarmMimallocSecure)
{
    extern (C) extern __gshared ubyte afsec__mi_process;

    // The permissions of the mapping holding `address`, e.g. "r--p".
    string permissions(size_t address)
    {
        import std.stdio : File;
        import std.conv : to;
        import std.algorithm : findSplit;
        foreach (line; File("/proc/self/maps").byLineCopy)
        {
            auto range = line.findSplit(" ");
            auto bounds = range[0].findSplit("-");
            immutable lo = bounds[0].to!size_t(16), hi = bounds[2].to!size_t(16);
            if (address >= lo && address < hi) return range[2][0 .. 4];
        }
        return "none";
    }
}

version (Windows) version (AntfarmMimallocSecure)
{
    extern (C) extern __gshared ubyte afsec__mi_process;

    // The page protection of the page holding `address` (PAGE_READONLY, ...).
    uint protection(size_t address)
    {
        import core.sys.windows.winbase : VirtualQuery;
        import core.sys.windows.winnt : MEMORY_BASIC_INFORMATION;
        MEMORY_BASIC_INFORMATION info;
        if (VirtualQuery(cast(void*) address, &info, info.sizeof) == 0) return 0;
        return info.Protect;
    }
}

enum T = 8, N = 20_000;

version (AntfarmMimallocV3) version (AntfarmMimallocSecure)
{
    shared void*[T * 2] mailbox; // [i] a fast block, [T + i] a secure one
    shared long bad;

    void mixedWorker(size_t me)
    {
        immutable next = (me + 1) % T;
        uint r = cast(uint)(me * 2654435761 + 7);
        foreach (i; 0 .. N)
        {
            r = r * 1664525 + 1013904223;
            immutable size = 16 + (r >> 16) % 4080;
            immutable alignment = size_t(8) << ((r >> 8) % 4);
            auto f = mi_malloc_aligned(size, alignment);
            auto s = afsec_mi_malloc_aligned(size, alignment);
            if (f is null || s is null) { atomicOp!"+="(bad, 1); continue; }
            if (!mi_is_in_heap_region(f) || afsec_mi_is_in_heap_region(f)
                    || !afsec_mi_is_in_heap_region(s) || mi_is_in_heap_region(s))
                atomicOp!"+="(bad, 1);
            memset(f, 0xA5, size);
            memset(s, 0x5A, size);
            auto oldFast = atomicExchange(&mailbox[next], cast(shared void*) f);
            if (oldFast) mi_free(cast(void*) oldFast);
            auto oldSecure = atomicExchange(&mailbox[T + next], cast(shared void*) s);
            if (oldSecure) afsec_mi_free(cast(void*) oldSecure);
            if (i % 4000 == 0) collectWithGc();
        }
    }
}

void main()
{
    version (AntfarmMimallocSecure)
    {
        check(secureAllocatorLinked() && secureAllocatorName() == "mimalloc-secure",
            "the secure instance is linked");
        check(secureMimallocVersion() == pinnedMimallocVersion, "secure instance mi_version() == 30700");
        version (AntfarmMimallocV3)
            check(mi_version() == pinnedMimallocVersion, "fast instance mi_version() == 30700");

        // CHECKED: a free of memory the instance does not own is ignored,
        // not followed into foreign metadata. The block stays the C
        // runtime's, which frees it afterwards.
        auto libc = cast(ubyte*) malloc(64);
        memset(libc, 0x11, 64);
        afsec_mi_free(libc);
        bool intact = true;
        foreach (b; libc[0 .. 64]) intact &= b == 0x11;
        free(libc);
        check(intact, "the secure instance ignores a free of C runtime memory");
        // The module hands such memory to the C runtime instead.
        immutable foreign = secureForeignFrees();
        secureFree(malloc(64));
        check(secureForeignFrees() == foreign + 1, "secureFree returns C runtime memory to the C runtime");

        auto held = secureMalloc(100);
        collectWithGc();
        check(true, "the D GC collects with the secure instance live");
        version (linux)
        {
            immutable base = cast(size_t) &afsec__mi_process;
            check(permissions(base) == "r--p" && permissions(base + 4096) == "r--p",
                "the secure instance's sealed process pages are read-only");
            check(permissions(base + 8192) == "rw-p", "no guard page in its static process data");
        }
        version (Windows)
        {
            import core.sys.windows.winnt : PAGE_READONLY, PAGE_READWRITE, PAGE_WRITECOPY;
            // MSVC cannot page-align _mi_process (MI_SECURE_SEALED_ALIGNED is 0), so
            // its front area has a third page of slack and the two sealed pages
            // start at the first page boundary after its first byte
            // (mi_process_get_sealed_once in mimalloc/heap/process.h).
            immutable sealed = (cast(size_t) &afsec__mi_process + 4096) & ~size_t(4095);
            printf("secure process data protection: %#x %#x %#x
",
                protection(sealed), protection(sealed + 4096), protection(sealed + 8192));
            check(protection(sealed) == PAGE_READONLY && protection(sealed + 4096) == PAGE_READONLY,
                "the secure instance's sealed process pages are read-only");
            immutable rest = protection(sealed + 8192); // copy-on-write until the image page is first written
            check(rest == PAGE_READWRITE || rest == PAGE_WRITECOPY, "no guard page in its static process data");
        }
        secureFree(held);

        version (AntfarmMimallocV3)
        {
            import std.algorithm : map;
            import std.array : array;
            import std.range : iota;
            foreach (round; 0 .. 4)
            {
                auto threads = iota(T).map!(i => new Thread(() => mixedWorker(i))).array;
                foreach (t; threads) t.start();
                foreach (t; threads) t.join();
            }
            foreach (i; 0 .. T)
            {
                if (auto p = atomicExchange(&mailbox[i], null)) mi_free(cast(void*) p);
                if (auto p = atomicExchange(&mailbox[T + i], null)) afsec_mi_free(cast(void*) p);
            }
            check(atomicLoad(bad) == 0, "two instances: 8 threads x 4 rounds, ownership disjoint, cross-thread frees");
        }

        // Thread exit reaches the renamed instance.
        immutable before = secureThreads();
        foreach (batch; 0 .. 8)
        {
            Thread[8] ts;
            foreach (ref t; ts)
                t = new Thread({
                    void*[16] blocks;
                    foreach (ref b; blocks) b = secureAllocateAligned64(256);
                    foreach (b; blocks) secureFreeAligned64(b);
                }).start();
            foreach (t; ts) t.join();
        }
        immutable after = secureThreads();
        printf("secure instance threads: total %lld -> %lld, current %lld -> %lld\n",
            before[0], after[0], before[1], after[1]);
        check(before[0] >= 1 && after[0] >= before[0] + 64, "64 short-lived threads registered with the secure instance");
        check(after[1] <= before[1], "each of them was released at thread exit");
    }
    else
    {
        check(!secureAllocatorLinked() && secureAllocatorName() == "c-runtime", "without the option: C runtime");
        check(secureMimallocVersion() == 0 && !secureOwns(&failures), "without the option: nothing is owned");
        auto p = secureAllocateAligned64(100);
        check(p !is null && (cast(size_t) p & 63) == 0, "without the option: aligned allocation");
        secureFreeAligned64(p);
        secureFree(malloc(64));
        check(secureForeignFrees() == 0, "without the option: no foreign accounting");
    }

    if (failures)
    {
        printf("%d FAILED\n", failures);
        fflush(stdout);
        exit(1);
    }
    printf("ALL SECURE ALLOCATOR TESTS PASSED (%.*s)\n",
        cast(int) secureAllocatorName().length, secureAllocatorName().ptr);
}
