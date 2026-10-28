// Subscribe/unsubscribe cost (SPEC 6b, 6e): ns per subscribe, three consumeNext
// calls and unsubscribe
// on an idle Farm (no producer) and under production (two producers, two
// steady consumers, 1 MiB ring, K = 8, the T29 geometry). Prints one line per
// scene: "scene=<idle|busy> cycles=<n> ns_per_cycle=<x> retries=<r>".
module subscribe_cost;
import antfarm;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, free;

__gshared shared(int) g_stop;
__gshared shared(long) g_calls;

extern(C) void cb(const(ulong)*, size_t) nothrow @nogc @system { atomicFetchAdd(g_calls, 1L); }

void producer(AntFarm* f, Tier tier, uint batch)
{
    auto tok = f.registerProducer(tier);
    if (!tok.valid) assert(0, "producer");
    auto bodies = cast(ulong*) malloc(16 * batch * ulong.sizeof);
    auto entries = cast(PayloadEntry*) malloc(batch * PayloadEntry.sizeof);
    foreach (i; 0 .. batch) entries[i] = PayloadEntry(&cb, bodies[16 * i .. 16 * i + 16]);
    while (atomicLoad(g_stop) == 0)
    {
        immutable n = f.write(entries[0 .. batch], tok);
        if (n == 0) Thread.yield();
    }
    f.unregisterProducer(tok);
    free(bodies); free(entries);
}

void steady(AntFarm* f)
{
    ConsumerView v;
    if (v.subscribe(f) < 0) assert(0, "steady");
    while (atomicLoad(g_stop) == 0)
        if (!v.consumeNext()) Thread.yield();
    while (v.consumeNext()) {}
    v.unsubscribe();
}

void measure(AntFarm* f, const(char)* scene, uint cycles)
{
    long retries;
    // warm-up
    foreach (_; 0 .. 2000) { ConsumerView v; long r; do r = v.subscribeOnce(f); while (r == SUBSCRIBE_RETRY); assert(r >= 0); v.consumeNext(); v.unsubscribe(); }
    immutable t0 = MonoTime.currTime;
    foreach (_; 0 .. cycles)
    {
        ConsumerView v;
        long r;
        do { r = v.subscribeOnce(f); if (r == SUBSCRIBE_RETRY) ++retries; } while (r == SUBSCRIBE_RETRY);
        assert(r >= 0);
        foreach (__; 0 .. 3) v.consumeNext();
        v.unsubscribe();
    }
    immutable ns = (MonoTime.currTime - t0).total!"nsecs";
    printf("scene=%s cycles=%u ns_per_cycle=%.1f retries=%lld\n", scene, cycles, cast(double) ns / cycles, retries);
}

void main()
{
    {
        auto f = AntFarm.create(1, 8, 32, 1, 8192, 1, 4096);
        measure(f, "idle", 200_000);
        f.destroy();
    }
    {
        auto f = AntFarm.create(1, 8, 32, 1, 8192, 1, 4096);
        atomicStore(g_stop, 0);
        auto s1 = new Thread({ steady(f); }); auto s2 = new Thread({ steady(f); });
        auto p1 = new Thread({ producer(f, Tier.bulk, 16); }); auto p2 = new Thread({ producer(f, Tier.small, 4); });
        s1.start(); s2.start(); p1.start(); p2.start();
        Thread.sleep(200.msecs);
        measure(f, "busy", 200_000);
        atomicStore(g_stop, 1);
        p1.join(); p2.join(); s1.join(); s2.join();
        f.destroy();
    }
}
