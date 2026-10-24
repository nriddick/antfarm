// SPDX-License-Identifier: MIT
/// Exactly-once test for forkjoin.deque: one owner pushes and pops, several
/// thieves steal, every item is observed exactly once, and the deque never
/// loses or duplicates an item through full, wraparound and last-item races.
/// Capacity 64 on purpose: a full deque and a wraparound every few pushes.
///
/// Also the ThreadSanitizer lane for the deque:
///   ldc2 -g -O1 -fsanitize=thread -i -I. -Ithreadpool/source -Iforkjoin/source \
///        -d-version=ForkjoinDequeTestMain tests/forkjoin/deque_test.d -of=.build/deque_tsan
module deque_test;

import forkjoin.deque;
import forkjoin.node;
import core.atomic;
import core.thread;
import core.stdc.stdio : printf;

private enum items = 2_000_000;
private enum thieves = 6;
private enum capacity = 64;

private __gshared Deque* deque;
private __gshared Node[] nodes;
private __gshared shared(uint)[] seen;
private shared bool done;
private shared ulong stolen;
private shared bool failed;

private void mark(size_t idx, const(char)* who)
{
    if (atomicOp!"+="(seen[idx], 1) != 1)
    {
        printf("deque_test: duplicate %zu (%s)\n", idx, who);
        atomicStore(failed, true);
    }
}

private void thief()
{
    ulong mine;
    while (!atomicLoad!(MemoryOrder.acq)(done))
    {
        auto n = deque.steal();
        if (n is null) continue;
        mark(cast(size_t)(n - nodes.ptr), "thief");
        mine++;
    }
    for (;;)
    {
        auto n = deque.steal();
        if (n is null) break;
        mark(cast(size_t)(n - nodes.ptr), "thief tail");
        mine++;
    }
    atomicOp!"+="(stolen, mine);
}

/// Returns true on success.
bool runDequeTest()
{
    deque = Deque.create(capacity);
    scope (exit) deque.destroy();
    nodes = new Node[items];
    seen = new shared(uint)[items];
    atomicStore(done, false);
    atomicStore(stolen, 0UL);
    atomicStore(failed, false);
    Thread[] ts;
    foreach (i; 0 .. thieves)
    {
        auto t = new Thread(&thief);
        t.start();
        ts ~= t;
    }
    size_t next;
    ulong popped, refused;
    uint rng = 12345;
    while (next < items)
    {
        rng = rng * 1103515245 + 12345;
        auto burst = (rng >> 16) % 9 + 1; // 1..9 children, skynet-like
        if (next + burst > items) burst = cast(uint)(items - next);
        Node*[9] batch;
        foreach (i; 0 .. burst) batch[i] = &nodes[next + i];
        long depth;
        auto n = deque.pushAll(batch[0 .. burst], depth);
        refused += burst - n;
        foreach (i; n .. burst) // full: consumed directly, as the dispatcher spills
            mark(next + i, "refused");
        next += burst;
        rng = rng * 1103515245 + 12345;
        auto pops = (rng >> 16) % 10;
        foreach (_; 0 .. pops)
        {
            auto x = deque.pop();
            if (x is null) break;
            mark(cast(size_t)(x - nodes.ptr), "owner");
            popped++;
        }
    }
    for (;;)
    {
        auto x = deque.pop();
        if (x is null) break;
        mark(cast(size_t)(x - nodes.ptr), "owner tail");
        popped++;
    }
    atomicStore(done, true);
    foreach (t; ts) t.join();
    size_t missing;
    foreach (i; 0 .. items) if (atomicLoad(seen[i]) != 1) missing++;
    printf("deque_test: items %d popped %llu stolen %llu refused %llu missing %zu\n",
        items, popped, atomicLoad(stolen), refused, missing);
    return missing == 0 && !atomicLoad(failed) && popped + atomicLoad(stolen) + refused == items;
}

version (ForkjoinDequeTestMain)
{
    void main()
    {
        import core.stdc.stdlib : exit;
        if (!runDequeTest()) { printf("FAILED\n"); exit(1); }
        printf("passed\n");
    }
}
