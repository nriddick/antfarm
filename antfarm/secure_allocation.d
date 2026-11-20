/++
 + Allocation for memory that holds bytes a peer controls: network receive
 + windows, send buffers, the socket handles around them, and libuv's own
 + allocations.
 +
 + The build option `AntfarmMimallocSecure` routes these to a second
 + mimalloc instance that `scripts/build-mimalloc-secure.py` builds from the
 + pinned source with `MI_SECURE=CHECKED`, `MI_SECURE_NO_STATIC_GUARD` (the
 + D garbage collector scans static data) and every symbol renamed with the
 + prefix `afsec_`. That instance validates every pointer passed to free,
 + encodes and randomizes its free lists, delays reuse of freed blocks and
 + fences its metadata with guard pages. It has its own heaps, so a write
 + past the end of a network buffer lands among other network buffers, not
 + in actor state or in the fast instance's free lists. It links beside the
 + `AntfarmMimallocV3` archive, or alone.
 +
 + Without the option every function here is the C runtime's, which is what
 + these call sites used before; the fast mimalloc instance is never used
 + for them.
 +
 + Pairing: memory from `secureAllocateAligned64` goes back through
 + `secureFreeAligned64`; memory from `secureMalloc`, `secureCalloc` and
 + `secureRealloc` through `secureFree` or `secureRealloc`. With the option
 + on, a pointer the secure instance does not own (memory a library
 + allocated before ant_uv installed the allocator, or a pairing mistake) is
 + handed to the C runtime instead and counted by `secureForeignFrees`.
 +
 + The functions are never inlined, so the decision is made once, where this
 + module is compiled, even when a dependent package is built without the
 + version.
 +/
module antfarm.secure_allocation;

import antfarm.allocation : allocateAligned64, freeAligned64;
import core.stdc.stdlib : calloc, free, malloc, realloc;

version (AntfarmMimallocSecure)
{
    import core.atomic : atomicFetchAdd, atomicLoad, MemoryOrder;

    // The renamed instance's public C ABI: only the calls used here.
    private extern (C) nothrow @nogc @system
    {
        void* afsec_mi_malloc(size_t size);
        void* afsec_mi_calloc(size_t count, size_t size);
        void* afsec_mi_realloc(void* memory, size_t size);
        void* afsec_mi_malloc_aligned(size_t size, size_t alignment);
        void afsec_mi_free(void* memory);
        bool afsec_mi_is_in_heap_region(const(void)* memory);
        int afsec_mi_version();
    }

    private shared ulong foreignFrees;

    private void countForeign() nothrow @nogc @trusted
    {
        atomicFetchAdd!(MemoryOrder.raw)(foreignFrees, 1);
    }
}

/// True when this build routes network memory to the secure instance.
pragma(inline, false)
bool secureAllocatorLinked() pure nothrow @nogc @safe
{
    version (AntfarmMimallocSecure)
        return true;
    else
        return false;
}

/// "mimalloc-secure" or "c-runtime", for reports.
pragma(inline, false)
string secureAllocatorName() pure nothrow @nogc @safe
{
    version (AntfarmMimallocSecure)
        return "mimalloc-secure";
    else
        return "c-runtime";
}

/// The secure instance's `mi_version()`, or 0 without it.
pragma(inline, false)
int secureMimallocVersion() nothrow @nogc @trusted
{
    version (AntfarmMimallocSecure)
        return afsec_mi_version();
    else
        return 0;
}

/// True when `memory` lies in the secure instance's heap. Always false
/// without the option. For tests and diagnostics.
pragma(inline, false)
bool secureOwns(const(void)* memory) nothrow @nogc @trusted
{
    version (AntfarmMimallocSecure)
        return memory !is null && afsec_mi_is_in_heap_region(memory);
    else
        return false;
}

/// Frees and reallocations of memory the secure instance did not own,
/// handed to the C runtime instead. Always 0 without the option.
pragma(inline, false)
ulong secureForeignFrees() nothrow @nogc @trusted
{
    version (AntfarmMimallocSecure)
        return atomicLoad(foreignFrees);
    else
        return 0;
}

/// `bytes` rounded up to a multiple of 64, aligned to 64, or null. The
/// contract of `antfarm.allocation.allocateAligned64`.
pragma(inline, false)
void* secureAllocateAligned64(size_t bytes) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
    {
        if (bytes > size_t.max - 63) return null;
        return afsec_mi_malloc_aligned((bytes + 63) & ~cast(size_t) 63, 64);
    }
    else
        return allocateAligned64(bytes);
}

/// Free memory from `secureAllocateAligned64`. Null is ignored.
pragma(inline, false)
void secureFreeAligned64(void* memory) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
    {
        if (memory is null) return;
        if (afsec_mi_is_in_heap_region(memory))
            return afsec_mi_free(memory);
        countForeign();
        freeAligned64(memory);
    }
    else
        freeAligned64(memory);
}

/// C `malloc` on the secure instance.
pragma(inline, false)
void* secureMalloc(size_t bytes) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
        return afsec_mi_malloc(bytes);
    else
        return malloc(bytes);
}

/// C `calloc` on the secure instance.
pragma(inline, false)
void* secureCalloc(size_t count, size_t bytes) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
        return afsec_mi_calloc(count, bytes);
    else
        return calloc(count, bytes);
}

/// C `realloc` on the secure instance. Memory it does not own is
/// reallocated by the C runtime and counted.
pragma(inline, false)
void* secureRealloc(void* memory, size_t bytes) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
    {
        if (memory is null || afsec_mi_is_in_heap_region(memory))
            return afsec_mi_realloc(memory, bytes);
        countForeign();
        return realloc(memory, bytes);
    }
    else
        return realloc(memory, bytes);
}

/// C `free` on the secure instance. Memory it does not own is freed by the
/// C runtime and counted. Null is ignored.
pragma(inline, false)
void secureFree(void* memory) nothrow @nogc @system
{
    version (AntfarmMimallocSecure)
    {
        if (memory is null) return;
        if (afsec_mi_is_in_heap_region(memory))
            return afsec_mi_free(memory);
        countForeign();
        free(memory);
    }
    else
        free(memory);
}

@system unittest
{
    foreach (n; [size_t(1), 63, 64, 65, 4096, 1 << 20])
    {
        auto p = secureAllocateAligned64(n);
        assert(p !is null && (cast(size_t) p & 63) == 0);
        (cast(ubyte*) p)[0 .. n] = 0xA5;
        assert(secureOwns(p) == secureAllocatorLinked());
        secureFreeAligned64(p);
    }
    assert(secureAllocateAligned64(size_t.max - 62) is null);
    auto c = cast(ubyte*) secureCalloc(16, 4);
    assert(c !is null);
    foreach (b; c[0 .. 64]) assert(b == 0);
    c = cast(ubyte*) secureRealloc(c, 4096);
    assert(c !is null && secureOwns(c) == secureAllocatorLinked());
    secureFree(c);
    secureFree(null);
    secureFreeAligned64(null);
    immutable before = secureForeignFrees();
    auto libc = malloc(32);
    secureFree(libc); // the C runtime's memory goes back to the C runtime
    assert(secureForeignFrees() == before + (secureAllocatorLinked() ? 1 : 0));
}
