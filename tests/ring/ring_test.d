/++
 + Multi-threaded tests of `antfarm.ring.SlotRing`: per-producer FIFO with
 + one consumer, exactly-once delivery with several consumers, close under
 + contention with an exact drained test, a producer stalled between its
 + reservation and its publication, and full-ring backpressure on a tiny
 + ring. Build with `-version=AntfarmRingTestHooks` (the Makefile does).
 +/
module ring_test;

version (TSan)
    extern(C) __gshared string[] rt_options = ["gcopt=parallel:0"];

import antfarm.ring;
import core.atomic;
import core.stdc.stdio : printf, fflush, stdout;
import core.stdc.stdlib : abort;
import core.thread : Thread;
import core.time : MonoTime, msecs, seconds;

version (AntfarmRingTestHooks) {}
else static assert(false, "build ring_test with -version=AntfarmRingTestHooks");

alias Ring = SlotRing!RingElement;

void check(bool condition, string what, int line = __LINE__) nothrow @nogc
{
    if (!condition)
    {
        printf("FAIL line %d: %.*s\n", line, cast(int) what.length, what.ptr);
        fflush(stdout);
        abort();
    }
}

void pass(string name)
{
    printf("PASS %.*s\n", cast(int) name.length, name.ptr);
    fflush(stdout);
}

void relax() nothrow @nogc
{
    core.atomic.pause();
}

Thread spawn(void delegate() body)
{
    auto t = new Thread(body);
    t.start();
    return t;
}

// A delegate per thread, built in its own frame: closures created in one
// loop body share that frame (see AF-005 in the omme friction log).
void delegate() producerBody(Ring* ring, uint id, ulong count, shared(ulong)* fullCount)
{
    return () {
        RingElement e;
        e.tag = id;
        ulong full;
        foreach (seq; 0 .. count)
        {
            e.generation = seq;
            e.payload[0] = id * 0x1_0000_0000UL + seq;
            for (;;)
            {
                immutable r = ring.tryPush(e);
                if (r == RingPush.pushed) break;
                check(r == RingPush.full, "push refused other than full");
                ++full;
                relax();
            }
        }
        atomicFetchAdd(*fullCount, full);
    };
}

/// Several producers, one consumer: every producer's elements arrive in its
/// own order, none lost or duplicated.
void mpscFifo()
{
    enum producers = 4;
    enum ulong perProducer = 200_000;
    auto ring = Ring.create(1024);
    shared ulong full;
    Thread[producers] threads;
    foreach (i; 0 .. producers)
        threads[i] = spawn(producerBody(ring, cast(uint) i, perProducer, &full));
    ulong[producers] nextSeq;
    ulong received;
    RingElement e;
    while (received < producers * perProducer)
    {
        if (!ring.tryPop(e)) { relax(); continue; }
        check(e.tag < producers, "unknown producer");
        check(e.generation == nextSeq[e.tag], "per-producer FIFO broken");
        check(e.payload[0] == e.tag * 0x1_0000_0000UL + e.generation, "payload torn");
        ++nextSeq[e.tag];
        ++received;
    }
    foreach (t; threads) t.join();
    check(!ring.tryPop(e) && ring.length == 0, "ring not empty after the count");
    ring.destroy();
    printf("  %llu elements, %llu full refusals\n", received, atomicLoad(full));
    pass("mpsc FIFO per producer");
}

/// Several producers and consumers: every element is delivered exactly once.
void mpmcExactlyOnce()
{
    enum producers = 4, consumers = 4;
    enum ulong perProducer = 100_000;
    enum ulong total = producers * perProducer;
    auto ring = Ring.create(256);
    auto seen = new shared(ubyte)[total];
    shared ulong received, full;
    Thread[producers + consumers] threads;
    foreach (i; 0 .. producers)
        threads[i] = spawn(producerBody(ring, cast(uint) i, perProducer, &full));
    foreach (i; 0 .. consumers)
        threads[producers + i] = spawn(() {
            RingElement e;
            while (atomicLoad(received) < total)
            {
                if (!ring.tryPop(e)) { relax(); continue; }
                immutable index = e.tag * perProducer + e.generation;
                check(index < total, "index out of range");
                check(atomicOp!"+="(seen[index], cast(ubyte) 1) == 1, "duplicate delivery");
                atomicFetchAdd(received, 1UL);
            }
        });
    foreach (t; threads) t.join();
    foreach (i; 0 .. total)
        check(atomicLoad(seen[i]) == 1, "element lost");
    ring.destroy();
    pass("mpmc exactly once");
}

/// Producers push until refused by close; the consumer stops only on the
/// exact drained test. Everything pushed is popped, once, in per-producer
/// order, and nothing is popped after drained.
void closeUnderContention()
{
    enum producers = 4;
    auto ring = Ring.create(64);
    shared ulong[producers] pushed;
    shared int go;
    Thread[producers] threads;
    static void delegate() closer(Ring* ring, uint id, shared(ulong)* count, shared(int)* go)
    {
        return () {
            while (atomicLoad(*go) == 0) relax();
            RingElement e;
            e.tag = id;
            ulong seq;
            for (;;)
            {
                e.generation = seq;
                immutable r = ring.tryPush(e);
                if (r == RingPush.closed) break;
                if (r == RingPush.pushed) ++seq;
                else relax();
            }
            atomicStore(*count, seq);
        };
    }
    foreach (i; 0 .. producers)
        threads[i] = spawn(closer(ring, cast(uint) i, &pushed[i], &go));
    auto closeAt = MonoTime.currTime + 20.msecs;
    atomicStore(go, 1);
    ulong[producers] nextSeq;
    ulong popped;
    bool closed;
    RingElement e;
    for (;;)
    {
        if (!closed && MonoTime.currTime >= closeAt)
        {
            ring.close();
            closed = true;
        }
        if (ring.tryPop(e))
        {
            check(e.generation == nextSeq[e.tag], "per-producer FIFO broken");
            ++nextSeq[e.tag];
            ++popped;
            continue;
        }
        if (closed && ring.drained) break;
        relax();
    }
    foreach (t; threads) t.join();
    ulong total;
    foreach (i; 0 .. producers)
    {
        check(nextSeq[i] == atomicLoad(pushed[i]), "pushed and popped counts differ");
        total += atomicLoad(pushed[i]);
    }
    check(popped == total && !ring.tryPop(e) && ring.drained, "pop after drained");
    check(ring.tryPush(e) == RingPush.closed, "push after close accepted");
    ring.destroy();
    printf("  %llu elements before the close\n", total);
    pass("close under contention, exact drained");
}

shared int stallArmed, stallReached, stallRelease;

void stallHook(const(void)*, ulong position) nothrow @nogc
{
    if (position != 0 || atomicLoad(stallArmed) == 0) return;
    atomicStore(stallReached, 1);
    while (atomicLoad(stallRelease) == 0) relax();
}

/// A producer stalled after reserving position 0 hides the published
/// element behind it from every consumer, and keeps the ring from draining.
void stalledProducer()
{
    auto ring = Ring.create(8);
    ringReservedHook = &stallHook;
    scope (exit) ringReservedHook = null;
    atomicStore(stallArmed, 1);
    auto stalled = spawn(() {
        RingElement e;
        e.tag = 1;
        check(ring.tryPush(e) == RingPush.pushed, "stalled push failed");
    });
    while (atomicLoad(stallReached) == 0) relax();
    atomicStore(stallArmed, 0);
    RingElement e, got;
    e.tag = 2;
    check(ring.tryPush(e) == RingPush.pushed, "second push failed");
    check(!ring.tryPop(got), "popped past an unpublished reservation");
    check(ring.length == 2, "length counts reservations");
    ring.close();
    check(ring.closed && !ring.drained, "drained with a reservation outstanding");
    check(ring.tryPush(e) == RingPush.closed, "push after close accepted");
    atomicStore(stallRelease, 1);
    stalled.join();
    check(ring.tryPop(got) && got.tag == 1, "stalled element out of order");
    check(!ring.drained, "drained with one element left");
    check(ring.tryPop(got) && got.tag == 2, "second element lost");
    check(ring.drained && !ring.tryPop(got), "not drained at the end");
    ring.destroy();
    pass("stalled producer blocks consumers and drained");
}

/// A two-slot ring under four producers and a slow consumer: refusals are
/// `full`, the caller keeps the element, and every element arrives once.
void tinyRingBackpressure()
{
    enum producers = 4;
    enum ulong perProducer = 20_000;
    auto ring = Ring.create(2);
    shared ulong full;
    Thread[producers] threads;
    foreach (i; 0 .. producers)
        threads[i] = spawn(producerBody(ring, cast(uint) i, perProducer, &full));
    ulong[producers] nextSeq;
    ulong received;
    RingElement e;
    while (received < producers * perProducer)
    {
        if (!ring.tryPop(e)) { relax(); continue; }
        check(e.generation == nextSeq[e.tag], "per-producer FIFO broken");
        ++nextSeq[e.tag];
        ++received;
        if ((received & 255) == 0) Thread.yield();
    }
    foreach (t; threads) t.join();
    check(atomicLoad(full) > 0, "a two-slot ring never refused");
    ring.destroy();
    printf("  %llu full refusals\n", atomicLoad(full));
    pass("tiny ring backpressure");
}

int main()
{
    immutable started = MonoTime.currTime;
    mpscFifo();
    mpmcExactlyOnce();
    foreach (round; 0 .. 5)
        closeUnderContention();
    stalledProducer();
    tinyRingBackpressure();
    printf("ALL RING TESTS PASSED (%lld ms)\n",
        (MonoTime.currTime - started).total!"msecs");
    return 0;
}
