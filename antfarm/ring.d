/++
 + A bounded ring of fixed 64-byte slots for one-element handoff: Dmitry
 + Vyukov's bounded multi-producer, multi-consumer queue, with a close bit
 + folded into the enqueue cursor.
 +
 + Provisional (2.2.x). The interface ships for `ant_uv` and the slot-ring
 + harness and may change before it is declared stable; see
 + docs/design/event-loop.md, section 3.2.
 +
 + Contract:
 +
 + - **FIFO by reservation.** A push reserves a position with one CAS on the
 +   enqueue cursor, then stores the element and publishes the slot's sequence
 +   word. A pop takes positions in order. `tryPop` returning false therefore
 +   does not mean the ring is empty for good: an earlier producer may have
 +   reserved and not yet published, and a stalled producer blocks every
 +   consumer behind its position.
 + - **Drained is exact only after `close`.** `close` sets the closed bit in
 +   the enqueue cursor, so every later push fails with `RingPush.closed`
 +   while every push that reserved before it still publishes. `drained` is
 +   true once every position reserved before the close has been claimed by a
 +   consumer; it never reports a stalled reservation as drained.
 + - **Full keeps ownership with the caller.** `RingPush.full` and
 +   `RingPush.closed` leave the element with the caller; a thread whose own
 +   progress drains the ring must not spin on it.
 + - **Ownership moves at publication.** Once pushed, the element belongs to
 +   whichever consumer pops it. A popped element is a copy; nothing may retain
 +   a pointer into a slot.
 + - **The ring never wakes anyone.** A producer wakes the consumer itself.
 + - **Destruction needs quiescence.** No thread may be inside `tryPush` or
 +   `tryPop`, and none may call them afterwards.
 +
 + Elements are plain data of at most 56 bytes, so a slot (an 8-byte sequence
 + word and the element) is one cache line. Ring memory is not scanned by the
 + garbage collector: an element must not hold the only reference to GC
 + memory. No allocation after `create`, no locks, `@nogc nothrow`, BetterC.
 +/
module antfarm.ring;

import antfarm.allocation : allocateAligned64, freeAligned64;
import core.atomic;
import core.stdc.string : memset;

/// Result of `SlotRing.tryPush`.
enum RingPush : ubyte
{
    /// Published: the element now belongs to whichever consumer pops it.
    pushed,
    /// Every slot holds an unconsumed element; the caller keeps the element.
    full,
    /// `close` ran before this push could reserve; the caller keeps it.
    closed,
}

/// The default element: three words an embedding interprets and four inline
/// words, 56 bytes. The ring never reads any of them.
struct RingElement
{
    /// What the element is, in the embedding's own numbering.
    ulong tag;
    /// A generation or ticket that lets the consumer reject stale elements.
    ulong generation;
    /// Typically the address of caller-owned storage.
    ulong pointer;
    /// Inline words.
    ulong[4] payload;
}

static assert(RingElement.sizeof == 56);

/// The largest element a slot holds: one 64-byte line less the sequence word.
enum size_t RING_ELEMENT_MAX = 56;

/// Smallest capacity `SlotRing.create` accepts.
enum size_t RING_MIN_CAPACITY = 2;

/// Largest capacity `SlotRing.create` accepts (64 MiB of slots).
enum size_t RING_MAX_CAPACITY = size_t(1) << 20;

version (AntfarmRingTestHooks)
{
    /// Test seam: runs in `tryPush` after the CAS reserved `position` and
    /// before the element is stored, so a test can stall a producer there.
    __gshared void function(const(void)* ring, ulong position) nothrow @nogc
        ringReservedHook;
}

/// A bounded ring of `T` elements in 64-byte slots. Allocate it with
/// `create` and release it with `destroy`; it is used through the pointer
/// and cannot be copied.
struct SlotRing(T = RingElement)
{
    static assert(T.sizeof <= RING_ELEMENT_MAX,
        "SlotRing elements are at most 56 bytes: a slot is one cache line");
    static assert(T.alignof <= 8, "SlotRing elements need at most 8-byte alignment");
    static assert(__traits(isPOD, T), "SlotRing elements must be plain data");

    private enum ulong closedBit = 1UL << 63;

    /// One cache line: the slot's sequence word and its element.
    private static align(64) struct Slot
    {
        shared ulong sequence;
        T value;
    }

    static assert(Slot.sizeof == 64 && Slot.alignof == 64);

    // The cursors are padded by distance: 64 bytes of nothing on either side
    // of each, so each one's line holds only padding whatever the struct's
    // alignment. The read-only fields come last, on their own line.
    private ubyte[64] padBefore_;
    private shared ulong enqueue_; // next reservation, plus closedBit
    private ubyte[64] padBetween_;
    private shared ulong dequeue_; // next claim
    private ubyte[64] padAfter_;
    private Slot* slots_;
    private size_t mask_;

    @disable this(this);

    /// True for a power of two in `RING_MIN_CAPACITY .. RING_MAX_CAPACITY`.
    static bool validCapacity(size_t capacity) pure nothrow @nogc @safe
    {
        return capacity >= RING_MIN_CAPACITY && capacity <= RING_MAX_CAPACITY
            && (capacity & (capacity - 1)) == 0;
    }

    /// A ring of `capacity` slots, or null when `validCapacity` rejects the
    /// capacity or the allocation fails. Owns two 64-byte-aligned blocks.
    static SlotRing* create(size_t capacity) nothrow @nogc @system
    {
        if (!validCapacity(capacity)) return null;
        auto ring = cast(SlotRing*) allocateAligned64(SlotRing.sizeof);
        if (ring is null) return null;
        auto slots = cast(Slot*) allocateAligned64(capacity * Slot.sizeof);
        if (slots is null)
        {
            freeAligned64(ring);
            return null;
        }
        memset(ring, 0, SlotRing.sizeof);
        memset(slots, 0, capacity * Slot.sizeof);
        foreach (i; 0 .. capacity)
            atomicStore!(MemoryOrder.raw)(slots[i].sequence, cast(ulong) i);
        ring.slots_ = slots;
        ring.mask_ = capacity - 1;
        atomicStore!(MemoryOrder.rel)(ring.enqueue_, 0UL);
        atomicStore!(MemoryOrder.rel)(ring.dequeue_, 0UL);
        return ring;
    }

    /// Release the ring. Requires quiescence: no thread inside `tryPush` or
    /// `tryPop`, and none calls anything on the ring afterwards. Elements
    /// still in it are discarded without being read.
    void destroy() nothrow @nogc @system
    {
        auto slots = slots_;
        slots_ = null;
        freeAligned64(slots);
        freeAligned64(&this);
    }

    /// Slots in the ring.
    @property size_t capacity() const pure nothrow @nogc @safe
    {
        return mask_ + 1;
    }

    /// Publish `value`, or return `full` or `closed` and leave it with the
    /// caller. Any thread. Lock-free: a failed CAS means another producer
    /// reserved, and the retry reloads the cursor.
    RingPush tryPush(ref const T value) nothrow @nogc @system
    {
        return pushImpl(value);
    }

    /// ditto
    RingPush tryPush(T value) nothrow @nogc @system
    {
        return pushImpl(value);
    }

    private RingPush pushImpl(ref const T value) nothrow @nogc @system
    {
        auto word = atomicLoad!(MemoryOrder.raw)(enqueue_);
        Slot* slot;
        for (;;)
        {
            if ((word & closedBit) != 0) return RingPush.closed;
            slot = &slots_[word & mask_];
            immutable sequence = atomicLoad!(MemoryOrder.acq)(slot.sequence);
            immutable difference = cast(long)(sequence - word);
            if (difference == 0)
            {
                // The CAS compares the whole word, closed bit included, so a
                // concurrent close makes it fail and the reload sees closed.
                if (cas!(MemoryOrder.raw, MemoryOrder.raw)(&enqueue_, word, word + 1))
                    break;
                word = atomicLoad!(MemoryOrder.raw)(enqueue_);
            }
            else if (difference < 0)
                return RingPush.full; // the slot still holds the previous lap
            else
                word = atomicLoad!(MemoryOrder.raw)(enqueue_);
        }
        version (AntfarmRingTestHooks)
            if (ringReservedHook !is null)
                ringReservedHook(&this, word);
        slot.value = value;
        atomicStore!(MemoryOrder.rel)(slot.sequence, word + 1);
        return RingPush.pushed;
    }

    /// Copy out the oldest published element and free its slot. False when
    /// the next position is empty or reserved but not yet published, which
    /// is not the same as drained (see `drained`). Any thread.
    bool tryPop(ref T value) nothrow @nogc @system
    {
        auto position = atomicLoad!(MemoryOrder.raw)(dequeue_);
        Slot* slot;
        for (;;)
        {
            slot = &slots_[position & mask_];
            immutable sequence = atomicLoad!(MemoryOrder.acq)(slot.sequence);
            immutable difference = cast(long)(sequence - (position + 1));
            if (difference == 0)
            {
                if (cas!(MemoryOrder.raw, MemoryOrder.raw)(&dequeue_, position, position + 1))
                    break;
                position = atomicLoad!(MemoryOrder.raw)(dequeue_);
            }
            else if (difference < 0)
                return false;
            else
                position = atomicLoad!(MemoryOrder.raw)(dequeue_);
        }
        value = slot.value;
        atomicStore!(MemoryOrder.rel)(slot.sequence, position + mask_ + 1);
        return true;
    }

    /// Refuse every later push with `RingPush.closed`. Pushes that reserved
    /// before the close still publish. Idempotent; any thread.
    void close() nothrow @nogc @system
    {
        atomicOp!"|="(enqueue_, closedBit);
    }

    /// True once `close` ran.
    @property bool closed() const nothrow @nogc @system
    {
        return (atomicLoad!(MemoryOrder.acq)(enqueue_) & closedBit) != 0;
    }

    /// True once `close` ran and every position reserved before it has been
    /// claimed by a consumer. With several consumers, a consumer may still be
    /// copying its last element out; nothing remains to be popped.
    @property bool drained() const nothrow @nogc @system
    {
        immutable word = atomicLoad!(MemoryOrder.acq)(enqueue_);
        if ((word & closedBit) == 0) return false;
        return atomicLoad!(MemoryOrder.acq)(dequeue_) == (word & ~closedBit);
    }

    /// Reserved minus claimed positions: elements published or about to be.
    /// A snapshot of two cursors, exact only when no thread is pushing or
    /// popping.
    @property size_t length() const nothrow @nogc @system
    {
        immutable claimed = atomicLoad!(MemoryOrder.acq)(dequeue_);
        immutable reserved = atomicLoad!(MemoryOrder.acq)(enqueue_) & ~closedBit;
        return cast(size_t)(reserved - claimed);
    }
}

unittest
{
    alias Ring = SlotRing!RingElement;
    assert(Ring.create(0) is null && Ring.create(1) is null && Ring.create(3) is null);
    assert(Ring.create(RING_MAX_CAPACITY * 2) is null);

    auto ring = Ring.create(4);
    assert(ring !is null && ring.capacity == 4);
    assert((cast(size_t) ring & 63) == 0);
    assert(Ring.enqueue_.offsetof / 64 != Ring.dequeue_.offsetof / 64);
    assert(Ring.slots_.offsetof / 64 != Ring.dequeue_.offsetof / 64);

    RingElement e, got;
    assert(!ring.tryPop(got) && ring.length == 0 && !ring.closed && !ring.drained);
    // Several laps: FIFO, full at capacity, and the full push keeps nothing.
    ulong next, expected;
    foreach (lap; 0 .. 5)
    {
        foreach (i; 0 .. 4)
        {
            e.tag = next++;
            assert(ring.tryPush(e) == RingPush.pushed);
        }
        e.tag = 999;
        assert(ring.tryPush(e) == RingPush.full && ring.length == 4);
        foreach (i; 0 .. 4)
        {
            assert(ring.tryPop(got) && got.tag == expected++);
        }
        assert(!ring.tryPop(got) && ring.length == 0);
    }
    // Close: later pushes fail, the published ones still drain, and drained
    // turns true exactly when the last one is claimed.
    e.tag = 7;
    assert(ring.tryPush(e) == RingPush.pushed);
    e.tag = 8;
    assert(ring.tryPush(e) == RingPush.pushed);
    ring.close();
    ring.close();
    assert(ring.closed && !ring.drained && ring.tryPush(e) == RingPush.closed);
    assert(ring.tryPop(got) && got.tag == 7 && !ring.drained);
    assert(ring.tryPop(got) && got.tag == 8 && ring.drained);
    assert(!ring.tryPop(got));
    ring.destroy();

    // Smaller elements still take a full line each.
    static struct Pair { uint a; uint b; }
    auto small = SlotRing!Pair.create(2);
    assert(small.tryPush(Pair(1, 2)) == RingPush.pushed);
    Pair p;
    assert(small.tryPop(p) && p.a == 1 && p.b == 2);
    small.destroy();
}
