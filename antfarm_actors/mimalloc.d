/++
 + Optional mimalloc v3 allocation policy for `antfarm_actors.actor`.
 +
 + Compile this module with `AntfarmMimallocV3` and link the mimalloc C
 + library. The default Ant Farm build has no mimalloc dependency.
 +/
module antfarm_actors.mimalloc;

version (AntfarmMimallocV3)
{
    public import antfarm_actors.actor : ActorAllocator;

    // Keep the binding deliberately narrow. These are public C ABI functions
    // in mimalloc v3; no mimalloc headers or allocator-private types enter the
    // actor API.
    extern (C) void* mi_malloc_aligned(size_t size, size_t alignment)
        nothrow @nogc @system;
    extern (C) void mi_free_size_aligned(void* memory, size_t size,
        size_t alignment) nothrow @nogc @system;

    /// Process-wide mimalloc v3 policy. ActorAllocator already supplies exact
    /// allocation sizes and alignments, allowing v3's sized-free fast path.
    ActorAllocator mimallocV3ActorAllocator()
        pure nothrow @nogc @safe
    {
        return ActorAllocator(null, &mimallocAllocate,
            &mimallocDeallocate);
    }

    private void* mimallocAllocate(void*, size_t bytes, size_t alignment)
        nothrow @nogc @system
    {
        return mi_malloc_aligned(bytes, alignment);
    }

    private void mimallocDeallocate(void*, void* memory, size_t bytes,
            size_t alignment) nothrow @nogc @system
    {
        mi_free_size_aligned(memory, bytes, alignment);
    }
}
