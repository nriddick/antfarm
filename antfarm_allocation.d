module antfarm_allocation;

import core.stdc.stdlib : free, malloc;

version (CRuntime_Microsoft) {}
else
    import core.stdc.stdlib : aligned_alloc;

/// Allocate `bytes` on a 64-byte boundary. The matching free function must be
/// used because the Microsoft C runtime path retains the original allocation
/// immediately before the aligned address.
void* allocateAligned64(size_t bytes) nothrow @nogc @system
{
    immutable n = (bytes + 63) & ~cast(size_t) 63;
    version (CRuntime_Microsoft)
    {
        // Avoid _aligned_malloc dllimport differences between D toolchains.
        auto raw = malloc(n + 64 + (void*).sizeof);
        if (raw is null) return null;
        auto aligned = (cast(size_t) raw + (void*).sizeof + 63) & ~cast(size_t) 63;
        (cast(void**) aligned)[-1] = raw;
        return cast(void*) aligned;
    }
    else
        return aligned_alloc(64, n);
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
