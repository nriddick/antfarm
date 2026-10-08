/++
 + Farm writer microbenchmark for the compact ST prototype (SPEC 2f).
 +
 + Built against either revision's antfarm.d (see writer-bench.sh). One thread
 + owns a small-tier producer and a ConsumerView. Each sample times one
 + fixed-length broadcast write (`write(header, bodies, bodyWords)`) or
 + `writeTracked` (the private/wave entry point) of B ST entries with the
 + cycle counter, then drains untimed. Timing starts after the ring has been
 + lapped twice (all pages faulted). It reports, per (API, body words, B):
 +   - entries admitted by one write from a fresh 16384-word quota (one table),
 +   - ring words per entry (Wt advance / admitted),
 +   - aggregate writer ticks (rdtsc) per entry over all timed writes,
 +     including quota sweeps and leftover-quota partial tables,
 +   - median ticks per entry over full-size (== admitted) tables.
 +/
module writer_bench;

import antfarm;
import core.atomic;
import core.stdc.stdio;
import core.stdc.stdlib : atoi, qsort;
import ldc.intrinsics : llvm_readcyclecounter;

long nopCb(PayloadHeader*, PayloadBody, ulong) nothrow @nogc @system { return 0; }

__gshared ulong g_hookCalls;
void hookCb(void*) nothrow @nogc @system { ++g_hookCalls; }

extern (C) int cmpU(scope const void* a, scope const void* b) nothrow @nogc
{
    immutable x = *cast(const ulong*) a, y = *cast(const ulong*) b;
    return x < y ? -1 : (x > y ? 1 : 0);
}

enum QUOTA = 16384;
enum MAXB = 8192;
__gshared ulong[MAXB * 4] g_words;
__gshared PayloadBody[MAXB] g_bodies;

void run(bool tracked, uint bodyWords, size_t batch, uint reps)
{
    auto f = AntFarm.create(8, 8, 1, 0, 0, 1, QUOTA);
    scope (exit) f.destroy();
    ConsumerView v;
    if (v.subscribe(f) < 0) fatal("subscribe");
    auto tok = f.registerProducer(Tier.small);
    PayloadHeader h;
    h.maxCs = 1; h.done = 1; h.plen = bodyWords; h.call = &nopCb;
    TableCompletionHook hook = TableCompletionHook(null, &hookCb);
    foreach (i; 0 .. batch)
        g_bodies[i] = g_words[i * bodyWords .. (i + 1) * bodyWords];

    ulong doWrite()
    {
        return tracked
            ? f.writeTracked(h, g_bodies[0 .. batch], bodyWords, &hook, tok)
            : f.write(h, g_bodies[0 .. batch], bodyWords, tok);
    }

    // Admission: a fresh token's first write sweeps and is granted a full
    // quota, so it shows how many entries one table admits.
    immutable wt0 = atomicLoad(f.Wt);
    immutable admitted = doWrite();
    immutable words = atomicLoad(f.Wt) - wt0;
    while (v.consumeNext()) {}

    // Timed writes. Leftover quota yields smaller tables and an empty
    // balance triggers a sweep, exactly as in a real pump loop: the
    // aggregate counts both, the median counts only full-size tables.
    // Warm up until the ring has been lapped twice, so every timed write
    // stores into already-faulted pages regardless of table size.
    while (atomicLoad(f.Wt) < 2 * f.Ln)
    {
        if (doWrite() == 0) fatal("warm-up write admitted nothing");
        while (v.consumeNext()) {}
    }
    static ulong[4096] samples;
    uint ns;
    ulong totalTicks, totalEntries;
    foreach (r; 0 .. reps + 50)
    {
        immutable t0 = llvm_readcyclecounter();
        immutable n = doWrite();
        immutable t1 = llvm_readcyclecounter();
        if (n == 0) fatal("write admitted nothing");
        while (v.consumeNext()) {}
        if (r < 50) continue;
        totalTicks += t1 - t0;
        totalEntries += n;
        if (n == admitted && ns < samples.length)
            samples[ns++] = t1 - t0;
    }
    if (ns == 0) fatal("no full-size samples");
    qsort(samples.ptr, ns, ulong.sizeof, &cmpU);
    immutable med = samples[ns / 2];
    printf("%s body=%u batch=%zu admitted=%llu table_words=%llu words_per_entry=%.2f agg_ticks_per_entry=%.2f full_tables=%u median_ticks_per_entry=%.2f p10=%.2f p90=%.2f\n",
        tracked ? "writeTracked".ptr : "write".ptr, bodyWords, batch,
        admitted, words, cast(double) words / admitted,
        cast(double) totalTicks / totalEntries, ns,
        cast(double) med / admitted,
        cast(double) samples[ns / 10] / admitted,
        cast(double) samples[ns * 9 / 10] / admitted);
    v.unsubscribe();
    f.unregisterProducer(tok);
}

void main(string[] args)
{
    immutable reps = args.length > 1 ? cast(uint) atoi(args[1].ptr) : 2000;
    foreach (tracked; [true, false])
        foreach (bw; [1u, 2u, 3u])
            foreach (b; [1UL, 16, 256, 1024, 8192])
                run(tracked, bw, cast(size_t) b, reps);
}
