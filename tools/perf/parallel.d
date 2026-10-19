/++ M1: grained parallel ranges, standalone or composed with other payloads.
 + Build with `make -C tools/perf parallel`. See README.md for the grid.
 +/
module parallel_bench;

import antfarm.parallel;
import core.atomic;
import core.thread;
import core.time : MonoTime;
import std.getopt : getopt;
import std.range : chain, iota;
import std.stdio : writefln, writeln;

__gshared ulong[] output;
__gshared uint itemCost;

ulong calculate(size_t i) nothrow @nogc @system
{
    ulong x = i + 1;
    foreach (_; 0 .. itemCost)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    return x;
}

void work(size_t i) nothrow @nogc @system { output[i] = calculate(i); }

void subscribe(ref ConsumerView view, AntFarm* farm)
{
    for (;;)
    {
        immutable result = view.subscribe(farm);
        if (result >= 0) return;
        if (result != SUBSCRIBE_RETRY) fatal("benchmark subscription failed");
        Thread.yield();
    }
}

void emit(R)(AntFarm* farm, ref Token token, ref ConsumerView view, R entries, uint costClass)
{
    while (!entries.empty)
    {
        import std.range.primitives : popFrontN;
        immutable accepted = farm.write(entries, token, costClass);
        entries.popFrontN(cast(size_t) accepted);
        if (accepted == 0 && !view.consumeNext()) Thread.yield();
    }
}

void main(string[] args)
{
    size_t n = 100_000, grain = 64, embedded;
    uint workers = 4, rounds = 10, cost = 16, costClass = DEFAULT_AVG_COST;
    getopt(args, "n", &n, "grain", &grain, "workers", &workers,
        "rounds", &rounds, "cost", &cost, "cost-class", &costClass,
        "embedded", &embedded);
    if (workers < 1 || workers > MAX_CONSUMERS_LIMIT || rounds == 0 || rounds == uint.max ||
        grain == 0 || costClass > MAX_AVG_COST ||
        embedded > size_t.max - n)
        fatal("invalid parallel benchmark options");

    output = new ulong[n + embedded];
    itemCost = cost;
    auto farm = AntFarm.create(8, 8, workers, 1, 16384, 0, 0);
    scope(exit) farm.destroy();
    auto token = farm.registerProducer(Tier.bulk);
    scope(exit) farm.unregisterProducer(token);
    ConsumerView view;
    subscribe(view, farm);
    scope(exit) view.unsubscribe();
    shared uint stopped, generation, ready;
    auto acknowledged = new shared(uint)[workers - 1];
    Thread[] threads;
    foreach (id; 0 .. workers - 1)
    {
        auto thread = new Thread((uint index) {
            return {
                ConsumerView local;
                subscribe(local, farm);
                atomicFetchAdd!(MemoryOrder.rel)(ready, 1U);
                while (!atomicLoad!(MemoryOrder.acq)(stopped))
                {
                    immutable round = atomicLoad!(MemoryOrder.acq)(generation);
                    if (!local.consumeNext())
                    {
                        atomicStore!(MemoryOrder.rel)(acknowledged[index], round);
                        Thread.yield();
                    }
                }
                local.unsubscribe();
            };
        }(id));
        threads ~= thread;
        thread.start();
    }
    scope(exit)
    {
        atomicStore!(MemoryOrder.rel)(stopped, 1U);
        foreach (thread; threads) thread.join();
    }
    while (atomicLoad!(MemoryOrder.acq)(ready) != workers - 1) Thread.yield();

    ulong elapsed, ringWords;
    foreach (round; 0 .. rounds + 1) // one warm-up, then measured rounds
    {
        output[] = 0;
        immutable beforeWords = atomicLoad!(MemoryOrder.raw)(farm.Wt);
        immutable start = MonoTime.currTime;
        {
            auto entries = parallelRange!work(iota(n), grain);
            if (embedded != 0)
                emit(farm, token, view, chain(entries,
                    parallelRange!work(iota(n, n + embedded), grain)), costClass);
            else emit(farm, token, view, entries, costClass);
        }
        // Raw composed ranges have no aggregate hook. A round completes only
        // after EVERY consumer drains after the publication generation. This
        // joins the outputs without adding an atomic to each measured item.
        atomicStore!(MemoryOrder.rel)(generation, round + 1);
        for (;;)
        {
            view.consumeNext();
            bool done = true;
            foreach (ref ack; acknowledged)
                done = done && atomicLoad!(MemoryOrder.acq)(ack) == round + 1;
            if (done) break;
            Thread.yield();
        }
        while (view.consumeNext()) {}
        immutable duration = MonoTime.currTime - start;
        if (round != 0)
        {
            elapsed += duration.total!"nsecs";
            ringWords += atomicLoad!(MemoryOrder.raw)(farm.Wt) - beforeWords;
        }
        foreach (i, value; output)
            if (value != calculate(i)) fatal("parallel benchmark output mismatch");
    }
    writeln("workers,n,grain,cost,cost_class,embedded,rounds,ns_per_round,ring_words_per_round");
    writefln("%s,%s,%s,%s,%s,%s,%s,%.1f,%.1f", workers, n, grain,
        cost, costClass, embedded, rounds,
        cast(double) elapsed / rounds, cast(double) ringWords / rounds);
}
