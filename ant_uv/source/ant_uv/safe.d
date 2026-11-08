/++
 + The `@safe` surface of `ant_uv`: the shapes through which code that must
 + not read a stranger's bytes through a raw pointer uses the loop.
 +
 + - `UvInbox` owns the ring UDP sockets deliver to and hands datagrams out
 +   as `UvReceived`: the header by value, the data as a scoped slice, the
 +   buffer released once, when the value is destroyed, released or handed
 +   to the next `tryPop`.
 + - `UvOutbox` owns the requests and bytes of in-flight sends. `send`
 +   copies a packet in and submits; a slot is reused only after the loop
 +   published it.
 + - `UvLoop.submitAndBlock` and the `ref` form of `submitAndWait`
 +   (`ant_uv.fibers`) take a request by reference and return only after
 +   publication, so a request on the caller's stack is sound.
 +
 + What these wrappers rely on is the loop's own contract, written down in
 + docs/design/event-loop.md section 9.12: a datagram's header is complete
 + before its element is pushed, and the element repeats the length; a
 + window outlives every datagram held from it; release from any thread is
 + one compare-and-swap; publication is the only way a request comes back.
 + Nothing but the loop pushes to an inbox's ring, which is why
 + `SlotRing.tryPush` stays `@system`. Setup and teardown stay `@system`
 + too: `UvLoop.create` and `close`, and `UvInbox.destroy`.
 +/
module ant_uv.safe;

import ant_uv.binding : UV_ECANCELED;
import ant_uv.loop;
import antfarm : fatal;
import antfarm.allocation : allocateAligned64, freeAligned64;
import antfarm.ring : RingElement, SlotRing;
import core.atomic;
import core.stdc.stdlib : calloc, free;
import core.stdc.string : memcpy;
import core.thread : Thread;
import core.time : Duration, MonoTime, seconds, usecs;

/// A received datagram as `@safe` code holds it: the header copied out, the
/// data as a slice that lives no longer than this value, and the buffer
/// returned to its socket's window exactly once, when the value is
/// destroyed, released, or handed to the next `UvInbox.tryPop`. It cannot
/// be copied; move it (`core.lifetime.move`) to keep a datagram past the
/// scope that popped it, and release it from any thread.
struct UvReceived
{
    /// The loop's clock when libuv delivered it (`UvLoop.nanos`).
    long arrivalNanos;
    /// `UvDatagramFlag` bits.
    uint flags;
    /// The socket id it arrived on.
    uint socket;
    /// The element tag that names that socket on the inbox's ring.
    ulong tag;
    /// The sender.
    UvAddress peer;
    private UvDatagram* datagram_;
    private uint length_;

    @disable this(this);

    /// True while this value holds a datagram.
    @property bool held() const pure nothrow @nogc @safe
    {
        return datagram_ !is null;
    }

    /// The data, empty once released. The method is `return`: the slice
    /// counts as a reference into this value, so under DIP1000 it cannot
    /// outlive the value in `@safe` code, though it points into the window.
    const(ubyte)[] bytes() const return nothrow @nogc @trusted
    {
        if (datagram_ is null) return null;
        return (cast(const(ubyte)*) datagram_)[UvDatagram.sizeof .. UvDatagram.sizeof + length_];
    }

    /// Return the buffer to its window now, from any thread. Idempotent;
    /// the destructor calls it.
    void release() nothrow @nogc @trusted
    {
        auto d = datagram_;
        if (d is null) return;
        datagram_ = null;
        length_ = 0;
        d.release();
    }

    /// Releases the buffer if it is still held.
    ~this() nothrow @nogc @trusted
    {
        release();
    }
}

/// The ring one or more UDP sockets deliver to, owned, with the sockets'
/// tags registered so that `tryPop` knows which elements are datagrams.
/// Register every socket through `options` before opening it, from the
/// thread that sets up; then pop from one thread or several. Nothing else
/// may push to the ring: a request that names it as `completions` is
/// discarded by tag and counted in `unexpected`.
final class UvInbox
{
    private SlotRing!RingElement* ring_;
    private Entry[maxEntries] entries_;
    private uint count_;
    private shared ulong popped_;
    private shared ulong unexpected_;

    private static struct Entry
    {
        ulong tag;
        uint window;
    }

    /// Sockets one inbox serves, the loop's own limit.
    enum uint maxEntries = 256;

    /// A ring of `capacity` slots, a power of two between
    /// `RING_MIN_CAPACITY` and `RING_MAX_CAPACITY`. Throws when the capacity
    /// is invalid or the allocation fails.
    this(size_t capacity) @trusted
    {
        ring_ = SlotRing!RingElement.create(capacity);
        if (ring_ is null)
            throw new Exception("ant_uv: UvInbox: the ring could not be made "
                ~ "(a power of two from 2 to 2^20 slots, and memory for it)");
    }

    /// Options for a socket that delivers here: `completions` is this ring,
    /// `tag` names the socket on it, `window` and `maxDatagram` size its
    /// buffers. Set `bind` and the rest on the result, then open it. The
    /// tag is registered, replacing an earlier registration of the same
    /// tag. Throws when the windows registered would exceed the ring's
    /// capacity, which `udpOpen` would refuse with `UV_ENOBUFS`, or when
    /// `maxEntries` tags are registered already.
    UvUdpOptions options(ulong tag, uint window = 64, uint maxDatagram = 1472) @safe
    {
        if (window == 0 || maxDatagram == 0)
            throw new Exception("ant_uv: UvInbox.options: window and maxDatagram must be nonzero");
        size_t total = window;
        uint slot = count_;
        foreach (i; 0 .. count_)
        {
            if (entries_[i].tag == tag)
                slot = i;
            else
                total += entries_[i].window;
        }
        if (total > capacity)
            throw new Exception("ant_uv: UvInbox.options: the windows registered exceed the ring's capacity");
        if (slot == count_)
        {
            if (count_ == maxEntries)
                throw new Exception("ant_uv: UvInbox.options: every tag slot is taken");
            count_++;
        }
        entries_[slot] = Entry(tag, window);
        UvUdpOptions o;
        o.completions = ring_;
        o.tag = tag;
        o.window = window;
        o.maxDatagram = maxDatagram;
        return o;
    }

    /// Pop the next datagram into `into`, releasing whatever it held first.
    /// False when no datagram is on the ring. Any thread; several threads
    /// may pop at once, each receiving distinct datagrams.
    bool tryPop(ref UvReceived into) nothrow @nogc @trusted
    {
        into.release();
        RingElement e;
        while (ring_.tryPop(e))
        {
            if (!registered(e.tag))
            {
                atomicOp!"+="(unexpected_, 1);
                continue;
            }
            auto d = cast(UvDatagram*) cast(size_t) e.pointer;
            // The loop completed the header before it pushed the element
            // and repeated the length in the payload: a header that disagrees
            // was overwritten by something holding the raw pointer, and no
            // slice over it is sound.
            if (d is null || d.length != cast(uint) e.payload[0])
            {
                fatal("ant_uv: a datagram's header disagrees with its ring element; "
                    ~ "something overwrote it through the raw pointer");
                assert(0);
            }
            into.arrivalNanos = d.arrivalNanos;
            into.flags = d.flags;
            into.socket = d.socket;
            into.tag = e.tag;
            into.peer = d.peer;
            into.datagram_ = d;
            into.length_ = d.length;
            atomicOp!"+="(popped_, 1);
            return true;
        }
        return false;
    }

    /// Slots in the ring.
    @property size_t capacity() const nothrow @nogc @safe
    {
        return ring_.capacity;
    }

    /// Datagrams handed out so far.
    @property ulong popped() const nothrow @nogc @safe
    {
        return atomicLoad(popped_);
    }

    /// Elements discarded because their tag was not registered: a request
    /// named this ring as its `completions`, or a socket was opened on it
    /// without `options`.
    @property ulong unexpected() const nothrow @nogc @safe
    {
        return atomicLoad(unexpected_);
    }

    /// The ring itself, for `@system` code that opens a socket without
    /// `options` (register its tag first) or inspects it. Only the loop may
    /// push to it.
    @property SlotRing!RingElement* ring() nothrow @nogc @system
    {
        return ring_;
    }

    /// Release the ring. `@system`: every socket that delivered here must
    /// have published its close, and no thread may be popping.
    void destroy() nothrow @nogc @system
    {
        if (ring_ is null) return;
        ring_.destroy();
        ring_ = null;
    }

    private bool registered(ulong tag) const nothrow @nogc @safe
    {
        foreach (i; 0 .. count_)
            if (entries_[i].tag == tag) return true;
        return false;
    }
}

/// Sends from `@safe` code: `slots` requests and their byte buffers, owned
/// by the outbox at stable addresses, each reused only after the loop
/// published it. `send` copies a packet in and submits; `stampAt` asks the
/// loop to write its transmit time into the copy. One thread sends at a
/// time. A send's result is read when its slot is reused or by `settle`,
/// into `sent`, `failed` and `lastError`.
final class UvOutbox
{
    private UvLoop loop_;
    private UvRequest* slots_;  // `count_` records from the C heap
    private ubyte* bytes_;      // `count_` buffers of `stride_` bytes, 64-aligned
    private uint count_;
    private uint capacity_;
    private size_t stride_;
    private uint next_;         // the slot the next send takes: the oldest
    private ulong accepted_;
    private ulong sent_;
    private ulong failed_;
    private long lastError_;
    private bool open_;

    /// `slots` requests of `capacity` bytes each. Throws when either is zero
    /// or the memory could not be had.
    this(UvLoop loop, uint slots, uint capacity) @trusted
    {
        if (loop is null)
            throw new Exception("ant_uv: UvOutbox needs a loop");
        if (slots == 0 || capacity == 0)
            throw new Exception("ant_uv: UvOutbox: slots and capacity must be nonzero");
        loop_ = loop;
        count_ = slots;
        capacity_ = capacity;
        stride_ = (cast(size_t) capacity + 63) & ~cast(size_t) 63;
        slots_ = cast(UvRequest*) calloc(slots, UvRequest.sizeof);
        bytes_ = cast(ubyte*) allocateAligned64(stride_ * slots);
        if (slots_ is null || bytes_ is null)
        {
            free(slots_);
            freeAligned64(bytes_);
            slots_ = null;
            bytes_ = null;
            throw new Exception("ant_uv: UvOutbox: out of memory");
        }
        foreach (i; 0 .. slots)
            slots_[i] = UvRequest.init;
        open_ = true;
    }

    /// Copy `packet` into the oldest slot and submit it as a send from
    /// `socket` to `peer`, or to the connected peer when `peer` is unset,
    /// with the loop's transmit stamp written at `stampAt` unless that is
    /// `uint.max`. The loop's answer: `full` when the oldest slot is still
    /// in flight or the request ring is full, `invalid` when the packet
    /// exceeds `capacity` or the stamp does not fit or the outbox is
    /// closed, `closed` once the loop is closing.
    UvSubmit send(int socket, scope const(ubyte)[] packet, UvAddress peer = UvAddress.init,
        uint stampAt = uint.max) nothrow @nogc @trusted
    {
        if (!open_ || packet.length > capacity_) return UvSubmit.invalid;
        auto slot = &slots_[next_];
        if (slot.state == UvRequestState.queued) return UvSubmit.full;
        account(slot);
        auto bytes = bytes_ + cast(size_t) next_ * stride_;
        if (packet.length != 0) memcpy(bytes, packet.ptr, packet.length);
        *slot = UvRequest.udpSend(socket, bytes[0 .. packet.length], peer);
        slot.stampAt = stampAt;
        immutable r = loop_.submit(slot);
        if (r == UvSubmit.accepted)
        {
            accepted_++;
            next_ = next_ + 1 == count_ ? 0 : next_ + 1;
        }
        return r;
    }

    /// Read the result of every published send not yet read, into `sent`,
    /// `failed` and `lastError`. Returns how many sends are still in flight.
    uint settle() nothrow @nogc @trusted
    {
        uint pending;
        foreach (i; 0 .. count_)
        {
            auto slot = &slots_[i];
            if (slot.state == UvRequestState.queued)
                pending++;
            else
                account(slot);
        }
        return pending;
    }

    /// Request slots.
    @property uint slots() const nothrow @nogc @safe { return count_; }
    /// Bytes a packet may have.
    @property uint capacity() const nothrow @nogc @safe { return capacity_; }
    /// Sends the loop accepted.
    @property ulong accepted() const nothrow @nogc @safe { return accepted_; }
    /// Sends published with a non-negative result, as read so far.
    @property ulong sent() const nothrow @nogc @safe { return sent_; }
    /// Sends published with an error or abandoned, as read so far.
    @property ulong failed() const nothrow @nogc @safe { return failed_; }
    /// The libuv code of the latest failure read (`UV_ECANCELED` for an
    /// abandoned send); 0 when none failed.
    @property long lastError() const nothrow @nogc @safe { return lastError_; }

    /// Wait until no send is in flight, then free the slots and the bytes;
    /// later sends are `invalid`. Call it after the sockets' or the loop's
    /// close, which publish every send. Throws when sends are still in
    /// flight after `timeout`, leaving the memory for a later call.
    void close(Duration timeout = 5.seconds) @trusted
    {
        if (slots_ is null) return;
        open_ = false;
        immutable deadline = MonoTime.currTime + timeout;
        while (settle() != 0)
        {
            if (MonoTime.currTime >= deadline)
                throw new Exception("ant_uv: UvOutbox.close: sends still in flight");
            Thread.sleep(50.usecs);
        }
        free(slots_);
        slots_ = null;
        freeAligned64(bytes_);
        bytes_ = null;
    }

    /// Memory left by a `close` that never ran is freed when nothing is in
    /// flight, and otherwise left to the loop, which may still write into it.
    ~this() nothrow @nogc @trusted
    {
        if (slots_ is null) return;
        foreach (i; 0 .. count_)
            if (slots_[i].state == UvRequestState.queued) return;
        free(slots_);
        freeAligned64(bytes_);
    }

    private void account(UvRequest* slot) nothrow @nogc @system
    {
        if (slot.user != 0 || slot.op != UvOp.udpSend) return;
        immutable s = slot.state;
        if (s != UvRequestState.done && s != UvRequestState.abandoned) return;
        slot.user = 1;
        if (s == UvRequestState.done && slot.result >= 0)
        {
            sent_++;
            return;
        }
        failed_++;
        lastError_ = s == UvRequestState.done ? slot.result : UV_ECANCELED;
    }
}
