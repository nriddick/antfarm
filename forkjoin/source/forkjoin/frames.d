// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick.
/// Frame pool: small, short-lived task frames allocated and freed without
/// the process allocator on the fork-join hot path.
///
/// A fork allocates its children's frames and the join frees them, usually
/// on the same thread, sometimes on the thief that completed the last
/// child. General allocators charge for the cross-thread case and, at a
/// hundred million forks a second, for the fast path too; fib(39) at 32
/// threads moved by 30% between allocators with the same dispatcher.
///
/// The pool is headerless: every block lives in a 64 KiB chunk aligned to
/// its size, whose first cache line names the owning slot and the block
/// size, so `freeFrame` finds both by masking the pointer. Blocks come in
/// 16-byte classes up to `frameMax`; a 112-byte pair of fib frames is 112
/// bytes here, not 128. Each thread that allocates owns a `slot` with one
/// LIFO free list and one bump cursor per class. A free from the owning
/// thread is a push onto that list; a free from any other thread is one
/// CAS onto the slot's remote list for the class, which the owner takes
/// over wholesale only when its own list and cursor are empty, so remote
/// frees never slow the owner's fast path. Larger requests get a chunk of
/// their own and go back to the process allocator on free.
///
/// Slots outlive threads: `releaseFrames` (which the dispatcher calls when
/// a participant leaves) parks the slot with its chunks and lists, and the
/// next thread to allocate adopts it, so start/stop cycles do not grow the
/// footprint and a block freed to a parked slot waits there for the
/// adopter. Chunks are never returned to the process allocator.
module forkjoin.frames;

import core.atomic;
import core.stdc.stdlib : calloc, free, malloc, realloc;
import core.sys.posix.stdlib : posix_memalign;

/// Chunk size and alignment; the pool works in these units.
enum size_t frameChunkBytes = 64 * 1024;
/// Block sizes are multiples of this, and blocks are aligned to it.
enum size_t frameGranule = 16;
/// Largest pooled request; above it a request gets its own chunk.
enum size_t frameMax = 1024;

private enum size_t classCount = frameMax / frameGranule;
private enum size_t headerBytes = 64;
private enum uint flagLarge = 1;

private struct ChunkHeader
{
    Slot* slot;        // owning slot; null for a large chunk
    uint blockBytes;   // 0 for a large chunk
    uint flags;
}
static assert(ChunkHeader.sizeof <= headerBytes);

private struct Slot
{
    void*[classCount] free;     // owner-only LIFO free lists, linked through the blocks
    ubyte*[classCount] bump;    // owner-only bump cursor into the class's current chunk
    ubyte*[classCount] bumpEnd;
    align(64) shared(void*)[classCount] remote;  // cross-thread frees, pushed by CAS
    align(64) shared bool taken;
    uint chunks;                // chunks carved for this slot, for `frameStats`
}

private __gshared Slot*[] g_slots;      // every slot ever created; grows under g_lock
private __gshared size_t g_slotCount;
private shared bool g_lock;
private shared uint g_chunks;           // pooled chunks across all slots
private shared uint g_large;            // live large chunks
private Slot* t_slot;

/// Counts for tests and footprint checks.
struct FrameStats
{
    size_t slots;   /// slots created so far, parked or owned
    size_t chunks;  /// pooled chunks carved so far, never returned
    size_t large;   /// large chunks currently live
}

/// Returns the pool's counts.
FrameStats frameStats() nothrow @nogc @system
{
    lock();
    scope (exit) unlock();
    return FrameStats(g_slotCount, atomicLoad!(MemoryOrder.raw)(g_chunks), atomicLoad!(MemoryOrder.raw)(g_large));
}

/// Allocates `bytes` of uninitialised memory aligned to `frameGranule`.
/// Pooled up to `frameMax`, from this thread's slot; larger requests get a
/// chunk of their own. Returns null only when the process allocator does.
/// The fast path, a pop from a thread-local list, inlines into the caller;
/// everything else is out of line so the caller carries no spills for it.
pragma(inline, true)
void* allocFrame(size_t bytes) nothrow @nogc @system
{
    if (bytes > frameMax) return allocLarge(bytes);
    immutable cls = bytes == 0 ? 0 : (bytes - 1) / frameGranule;
    auto s = t_slot;
    if (s !is null)
    {
        auto p = s.free[cls];
        if (p !is null)
        {
            s.free[cls] = *cast(void**) p;
            return p;
        }
    }
    return allocSlow(cls);
}

/// Typed `allocFrame`: room for `n` uninitialised `T`s.
T* allocFrames(T)(size_t n) nothrow @nogc @system
{
    return cast(T*) allocFrame(n * T.sizeof);
}

/// Frees a block from `allocFrame`, from any thread. From the owning thread
/// this is a push onto a thread-local list, inlined into the caller; from
/// another thread it is one CAS onto the owner's remote list.
pragma(inline, true)
void freeFrame(void* p) nothrow @nogc @system
{
    if (p is null) return;
    auto h = cast(ChunkHeader*) (cast(size_t) p & ~(frameChunkBytes - 1));
    auto s = h.slot;
    if (s is t_slot && s !is null)
    {
        immutable cls = h.blockBytes / frameGranule - 1;
        *cast(void**) p = s.free[cls];
        s.free[cls] = p;
        return;
    }
    freeSlow(h, p);
}

/// A large chunk, or a block owned by another slot.
pragma(inline, false)
private void freeSlow(ChunkHeader* h, void* p) nothrow @nogc @system
{
    if (h.flags & flagLarge)
    {
        atomicOp!"-="(g_large, 1);
        free(h);
        return;
    }
    auto s = h.slot;
    immutable cls = h.blockBytes / frameGranule - 1;
    auto head = atomicLoad!(MemoryOrder.raw)(s.remote[cls]);
    for (;;)
    {
        *cast(void**) p = cast(void*) head;
        if (cas!(MemoryOrder.acq_rel, MemoryOrder.raw)(&s.remote[cls], head, cast(shared(void*)) p)) return;
        head = atomicLoad!(MemoryOrder.raw)(s.remote[cls]);
    }
}

/// Parks this thread's slot for the next thread that allocates. Blocks
/// from it stay valid and may still be freed from any thread. The
/// dispatcher calls this when a participant leaves; other threads that used
/// `allocFrame` may call it before they exit.
void releaseFrames() nothrow @nogc @system
{
    auto s = t_slot;
    if (s is null) return;
    t_slot = null;
    atomicStore!(MemoryOrder.rel)(s.taken, false);
}

// ---------------------------------------------------------------------------

private void lock() nothrow @nogc @system
{
    while (!cas(&g_lock, false, true)) {}
}

private void unlock() nothrow @nogc @system
{
    atomicStore!(MemoryOrder.rel)(g_lock, false);
}

/// Adopts a parked slot or creates one.
private Slot* attach() nothrow @nogc @system
{
    lock();
    scope (exit) unlock();
    foreach (s; g_slots[0 .. g_slotCount])
        if (!atomicLoad!(MemoryOrder.acq)(s.taken))
        {
            atomicStore!(MemoryOrder.raw)(s.taken, true);
            t_slot = s;
            return s;
        }
    auto s = cast(Slot*) calloc(1, Slot.sizeof);
    if (s is null) assert(0, "forkjoin.frames: out of memory");
    atomicStore!(MemoryOrder.raw)(s.taken, true);
    if (g_slotCount == g_slots.length)
    {
        immutable cap = g_slots.length ? g_slots.length * 2 : 64;
        auto grown = cast(Slot**) realloc(g_slots.ptr, cap * (Slot*).sizeof);
        if (grown is null) assert(0, "forkjoin.frames: out of memory");
        g_slots = grown[0 .. cap];
    }
    g_slots[g_slotCount++] = s;
    t_slot = s;
    return s;
}

/// No slot yet or an empty free list: attach, then the bump cursor, then
/// the remote list, then a chunk.
pragma(inline, false)
private void* allocSlow(size_t cls) nothrow @nogc @system
{
    auto s = t_slot;
    if (s is null) s = attach();
    immutable size = (cls + 1) * frameGranule;
    auto p = s.bump[cls];
    if (p !is null && p + size <= s.bumpEnd[cls])
    {
        s.bump[cls] = p + size;
        return p;
    }
    if (atomicLoad!(MemoryOrder.raw)(s.remote[cls]) !is null)
    {
        auto r = cast(void*) atomicExchange(&s.remote[cls], cast(shared(void*)) null);
        if (r !is null)
        {
            s.free[cls] = *cast(void**) r;
            return r;
        }
    }
    void* chunk;
    if (posix_memalign(&chunk, frameChunkBytes, frameChunkBytes) != 0) return null;
    auto h = cast(ChunkHeader*) chunk;
    h.slot = s;
    h.blockBytes = cast(uint) size;
    h.flags = 0;
    s.chunks++;
    atomicOp!"+="(g_chunks, 1);
    auto first = cast(ubyte*) chunk + headerBytes;
    s.bump[cls] = first + size;
    s.bumpEnd[cls] = cast(ubyte*) chunk + frameChunkBytes;
    return first;
}

pragma(inline, false)
private void* allocLarge(size_t bytes) nothrow @nogc @system
{
    immutable total = (headerBytes + bytes + frameChunkBytes - 1) & ~(frameChunkBytes - 1);
    void* chunk;
    if (posix_memalign(&chunk, frameChunkBytes, total) != 0) return null;
    auto h = cast(ChunkHeader*) chunk;
    h.slot = null;
    h.blockBytes = 0;
    h.flags = flagLarge;
    atomicOp!"+="(g_large, 1);
    return cast(ubyte*) chunk + headerBytes;
}

unittest
{
    // Same-thread reuse is LIFO within a class; sizes round up to 16.
    auto a = allocFrame(100);
    auto b = allocFrame(112);
    assert((cast(size_t) a & 15) == 0 && (cast(size_t) b & 15) == 0);
    freeFrame(b);
    freeFrame(a);
    assert(allocFrame(97) is a);
    assert(allocFrame(1) !is b); // class 0, not the 112-byte class
    auto big = allocFrame(frameMax + 1);
    assert(big !is null && (cast(size_t) big & 15) == 0);
    freeFrame(big);
}
