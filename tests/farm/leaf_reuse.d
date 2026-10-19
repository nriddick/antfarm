module leaf_reuse;
import antfarm;
import core.atomic;
import core.thread : Thread;
import core.stdc.stdlib : malloc, free;
import core.stdc.stdio : puts;
__gshared AntFarm* f;
__gshared shared(int) cReady, qReady, published, readDone, cDrop, cDone, qDrop, qDone, finish;
__gshared shared(ulong) checksum;
void waitFlag(ref shared int flag) nothrow @nogc @system
{
    while (atomicLoad!(MemoryOrder.raw)(flag) == 0) Thread.yield();
}
void require(bool b, string s) nothrow @nogc @system { if (!b) fatal(s); }
long cb(PayloadHeader*, PayloadBody body) nothrow @nogc @system
{
    ulong sum;
    foreach (x; body) sum += x; // Plain ring reads must reach the later writer.
    atomicStore!(MemoryOrder.raw)(checksum, sum);
    return 1;
}
void reader()
{
    ConsumerView c;
    require(c.subscribe(f) == 0, "reader subscribe");
    atomicStore!(MemoryOrder.raw)(cReady, 1);
    waitFlag(published);
    require(c.consumeNext(), "reader consumes table");
    atomicStore!(MemoryOrder.raw)(readDone, 1);
    waitFlag(cDrop);
    c.unsubscribe(); // non-edge: Q still holds this leaf
    atomicStore!(MemoryOrder.raw)(cDone, 1);
    waitFlag(finish);
}
void lastLeaf()
{
    ConsumerView q;
    require(q.subscribe(f) == 0, "last leaf subscribe");
    atomicStore!(MemoryOrder.raw)(qReady, 1);
    waitFlag(qDrop);
    q.unsubscribe(); // last on leaf; must acquire C's release before Rt release
    atomicStore!(MemoryOrder.raw)(qDone, 1);
    waitFlag(finish);
}
ulong writeWords(ref Token tok, ulong words)
{
    auto sq = sqcsOf(atomicLoad!(MemoryOrder.raw)(f.Cf));
    // Compact ST singleton (spec 2f): 40 + 8*(sq-1) words.
    auto n = cast(size_t)(words - (40 + 8 * (sq-1)));
    auto body = cast(ulong*)malloc(n * 8);
    scope(exit) free(body);
    require(body !is null, "leaf body allocation");
    body[0..n] = 17;
    PayloadHeader h;
    h.call = &cb;
    PayloadEntry[1] entries = [PayloadEntry(&h,body[0..n])];
    return f.write(entries[],tok);
}
void main()
{
    f = AntFarm.create(2, 4, 1, 1, 3 * (1 << 16), 0, 0);
    auto tok = f.registerProducer(Tier.bulk);
    auto c = new Thread(&reader); c.start(); waitFlag(cReady);
    auto q = new Thread(&lastLeaf); q.start(); waitFlag(qReady);
    ConsumerView r;
    require(r.subscribe(f) == 0, "frontier keeper subscribe");
    require(writeWords(tok, 1000) == 1, "first leaf table");
    atomicStore!(MemoryOrder.raw)(published, 1);
    waitFlag(readDone);
    require(writeWords(tok, f.segCap + 128 - 1000) == 1, "crossing leaf table");
    // R migrates BEFORE C drops its leaf, so R's own drop and subsequent
    // root increments cannot accidentally forward C's callback history.
    require(r.consumeNext(), "keeper skips completed table");
    require(r.consumeNext() && r.oldestEi == 1, "keeper moves to epoch one");
    atomicStore!(MemoryOrder.raw)(cDrop, 1); waitFlag(cDone);
    atomicStore!(MemoryOrder.raw)(qDrop, 1); waitFlag(qDone);
    require(atomicLoad!(MemoryOrder.raw)(f.Rt[0][0]) == 0, "old root is free");
    // The new grant sweeps slot zero; these two writes reuse its first
    // 128 words, including words plainly read by C. No join or flag acquires
    // may carry those reads to the producer before this write.
    require(writeWords(tok, 3 * f.segCap - 64) == 1, "reuse leaf reader words");
    // Start within the first virtual mapping: TSan does not know that the
    // magic buffer's second mapping aliases the first physical pages.
    require(writeWords(tok, 64) == 1, "reuse at primary virtual address");
    atomicStore!(MemoryOrder.raw)(finish, 1);
    c.join(); q.join();
    while (r.consumeNext()) {}
    r.unsubscribe(); f.unregisterProducer(tok); f.destroy();
    puts("PASS leaf reuse: non-edge reader joins last leaf drop before sweep");
}
