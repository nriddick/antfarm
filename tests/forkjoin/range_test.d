// SPDX-License-Identifier: MIT
/// forkJoinFor and forkJoinReduce: every element visited once across lengths
/// and grains, a borrowed-frame body writing a caller's array, a floating
/// point reduction that is bit-identical between one participant and many,
/// a loop started from a thread that is not a participant, a loop nested
/// inside a node, and a detached root drained by stop.
module range_test;

import forkjoin;
import threadpool;
import core.atomic;
import core.thread;
import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, free;
import std.range : iota;

private __gshared uint[] hits;

private void visit(size_t i) nothrow @nogc @system
{
    atomicOp!"+="(*cast(shared uint*) &hits[i], 1u);
}

private double term(size_t i) nothrow @nogc @safe
{
    return 1.0 / (i + 1);
}

private double add(double a, double b) nothrow @nogc @safe { return a + b; }

private bool checkHits(size_t n, const(char)* label)
{
    foreach (i, h; hits[0 .. n])
        if (h != 1)
        {
            printf("range_test: %s: element %zu visited %u times\n", label, i, h);
            return false;
        }
    return true;
}

private double reduceOn(const(ProcessorId)[] cpus, size_t n, size_t grain)
{
    Dispatcher d;
    d.start(cpus);
    scope (exit) d.stop();
    return forkJoinReduce!(term, add)(iota(n), 0.0, grain);
}

private __gshared double g_foreignSum;
private __gshared bool g_foreignOk;

private void foreignLoop()
{
    hits = new uint[10_007];
    forkJoinFor!visit(iota(hits.length), 16);
    g_foreignOk = checkHits(hits.length, "foreign thread");
    g_foreignSum = forkJoinReduce!(term, add)(iota(size_t(5000)), 0.0, 32);
}

// A node whose run starts a nested blocking loop.
private struct Nest
{
    Node node;
    size_t n;
    bool ok;
}

private Node* nestRun(Node* node) nothrow @nogc @system
{
    auto s = cast(Nest*) node;
    forkJoinFor!visit(iota(s.n), 8);
    s.ok = true;
    return complete(node);
}

// A detached root: its join records completion.
private struct Detached
{
    Node node;
    Detached* kids;
    shared(uint)* flag;
}

private Node* detachedRun(Node* node) nothrow @nogc @system
{
    auto d = cast(Detached*) node;
    if (d.kids is null)
    {
        auto kids = cast(Detached*) malloc(2 * Detached.sizeof);
        foreach (i; 0 .. 2)
        {
            kids[i] = Detached.init;
            kids[i].kids = cast(Detached*) 1; // leaf marker
            kids[i].node.run = &detachedRun;
        }
        d.kids = kids;
        d.node.join = &detachedJoin;
        Node*[2] nodes = [&kids[0].node, &kids[1].node];
        return fork(node, nodes[]);
    }
    return complete(node);
}

private Node* detachedJoin(Node* node) nothrow @nogc @system
{
    auto d = cast(Detached*) node;
    free(d.kids);
    atomicStore(*d.flag, 1u);
    return null;
}

private __gshared Detached g_detached;
private shared uint g_detachedDone;

private void detachedSubmitter()
{
    g_detached = Detached.init;
    g_detached.flag = &g_detachedDone;
    g_detached.node.run = &detachedRun;
    submitDetached(&g_detached.node);
}

/// Returns true on success.
bool runRangeTest()
{
    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    foreach (ref p; topo.processors)
        if (topo.isEligible(p) && cpus.length < 8) cpus ~= ProcessorId.of(p);
    if (cpus.length == 0) { printf("range_test: no eligible processors\n"); return false; }
    bool ok = true;

    {
        Dispatcher d;
        d.start(cpus);
        foreach (n; [size_t(0), 1, 7, 513, 1031, 50_003])
            foreach (grain; [size_t(1), 64, 1024])
            {
                hits = new uint[n];
                forkJoinFor!visit(iota(n), grain);
                ok &= checkHits(n, "forkJoinFor");
            }
        // Borrowed frame: write the caller's array through a delegate.
        uint[1031] output;
        forkJoinEach(iota(output.length), 13, (size_t i) nothrow @nogc @system { output[i] = cast(uint) i + 1; });
        foreach (i, v; output) if (v != i + 1) { printf("range_test: delegate body wrote %u at %zu\n", v, i); ok = false; break; }
        // Nested loop inside a node.
        hits = new uint[777];
        Nest nest; nest.n = 777; nest.node.run = &nestRun;
        runRoot(&nest.node);
        ok &= nest.ok && checkHits(777, "nested loop");
        // Foreign thread: submit path.
        auto t = new Thread(&foreignLoop);
        t.start(); t.join();
        ok &= g_foreignOk;
        // Detached root drained by stop.
        atomicStore(g_detachedDone, 0u);
        auto s = new Thread(&detachedSubmitter);
        s.start(); s.join();
        d.stop();
        if (atomicLoad(g_detachedDone) != 1) { printf("range_test: stop did not drain the detached root\n"); ok = false; }
    }

    // Deterministic reduction: one participant against many, several grains.
    foreach (grain; [size_t(1), 7, 100])
    {
        auto one = reduceOn(cpus[0 .. 1], 100_003, grain);
        auto many = reduceOn(cpus, 100_003, grain);
        if (one != many)
        {
            printf("range_test: reduce differs across participant counts at grain %zu: %.17g vs %.17g\n", grain, one, many);
            ok = false;
        }
        double seq = 0.0;
        foreach (i; 0 .. 100_003) seq += term(i);
        if ((one - seq) / seq > 1e-12 || (seq - one) / seq > 1e-12)
        {
            printf("range_test: reduce far from the sequential sum: %.17g vs %.17g\n", one, seq);
            ok = false;
        }
        if (g_foreignSum == 0.0) ok = false;
    }
    // Accumulator types: byte, double and a struct all compile and fold.
    {
        Dispatcher d;
        d.start(cpus[0 .. 2]);
        auto bytes = forkJoinReduce!((size_t i) => cast(ubyte)(i & 1), (ubyte a, ubyte b) => cast(ubyte)(a + b))(iota(size_t(100)), ubyte(0), 7);
        if (bytes != 50) { printf("range_test: ubyte reduce %u\n", bytes); ok = false; }
        static struct MinMax { size_t lo = size_t.max; size_t hi; }
        auto mm = forkJoinReduce!((size_t i) => MinMax(i, i), (MinMax a, MinMax b) => MinMax(a.lo < b.lo ? a.lo : b.lo, a.hi > b.hi ? a.hi : b.hi))(iota(size_t(5), size_t(905)), MinMax.init, 16);
        if (mm.lo != 5 || mm.hi != 904) { printf("range_test: struct reduce %zu..%zu\n", mm.lo, mm.hi); ok = false; }
        d.stop();
    }
    printf("range_test: %s\n", ok ? "ok".ptr : "FAILED".ptr);
    return ok;
}
