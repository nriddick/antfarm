module antfarm_allocation;

import core.stdc.stdlib : free, malloc;

version (CRuntime_Microsoft) {}
else
    import core.stdc.stdlib : aligned_alloc;

/// Allocate `bytes` on a 64-byte boundary. The matching free function must be
/// used because the Microsoft C runtime path retains the original allocation
/// immediately before the aligned address.
/// Returns null if rounding or allocator overhead would overflow size_t.
void* allocateAligned64(size_t bytes) nothrow @nogc @system
{
    if (bytes > size_t.max - 63) return null;
    immutable n = (bytes + 63) & ~cast(size_t) 63;
    version (CRuntime_Microsoft)
    {
        // Avoid _aligned_malloc dllimport differences between D toolchains.
        if (n > size_t.max - 64 - (void*).sizeof) return null;
        auto raw = malloc(n + 64 + (void*).sizeof);
        if (raw is null) return null;
        auto aligned = (cast(size_t) raw + (void*).sizeof + 63) & ~cast(size_t) 63;
        (cast(void**) aligned)[-1] = raw;
        return cast(void*) aligned;
    }
    else
        return aligned_alloc(64, n);
}

unittest
{
    // Exercise both the rounding boundary and Microsoft's separate overhead.
    foreach (n; [size_t.max, size_t.max - 62, size_t.max - 63])
        assert(allocateAligned64(n) is null);
    version (CRuntime_Microsoft)
        assert(allocateAligned64(size_t.max - 127) is null);
    foreach (n; [size_t(1), 63, 64, 65, 4096])
    {
        auto p = allocateAligned64(n);
        assert(p !is null && (cast(size_t) p & 63) == 0);
        (cast(ubyte*) p)[0 .. n] = 0xA5;
        freeAligned64(p);
    }
}

/// Free memory returned by `allocateAligned64`.
void freeAligned64(void* memory) nothrow @nogc @system
{
    if (memory is null) return;
    version (CRuntime_Microsoft)
        free((cast(void**) memory)[-1]);
    else
        free(memory);
}
