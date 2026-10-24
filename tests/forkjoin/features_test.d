// SPDX-License-Identifier: MIT
/// 2.0.1 features: forkJoinMap writes every element once; forkJoinScan
/// matches a sequential prefix fold, is bit-identical between one
/// participant and many, handles empty and single-element inputs and every
/// grain; forkJoinInvoke runs function pointers and borrowing delegates from
/// a participant and from a foreign thread; TaskGroup collects a batch of
/// foreign roots, is reusable, and lets stop drain cleanly; the option
/// presets start, and printCounters prints.
module features_test;

import forkjoin;
import threadpool;
import core.atomic;
import core.thread;
import core.stdc.stdio : printf;
import std.range : iota;

private double half(size_t i) nothrow @nogc @safe { return i * 0.5; }
private double term(size_t i) nothrow @nogc @safe { return 1.0 / (i + 1); }
private double add(double a, double b) nothrow @nogc @safe { return a + b; }
private ulong square(size_t i) nothrow @nogc @safe { return cast(ulong) i * i; }
private ulong addU(ulong a, ulong b) nothrow @nogc @safe { return a + b; }

private shared uint g_flagA;
private void setA() nothrow @nogc { atomicOp!"+="(g_flagA, 1u); }

private struct Count
{
    Node node;
    shared(uint)* target;
}

private Node* countRun(Node* node) nothrow @nogc @system
{
    auto c = cast(Count*) node;
    atomicOp!"+="(*c.target, 1u);
    return complete(node);
}

private bool scanMatches(const(double)[] got, size_t n, size_t grain, const(char)* label)
{
    // A tree fold associates differently from a sequential one, so doubles
    // agree to rounding, not bit for bit; the one-versus-many check below is exact.
    import std.math : abs;
    double running = 1.0;
    foreach (i; 0 .. n)
    {
        running += term(i);
        if (abs(got[i] - running) > 1e-12 * running)
        {
            printf("features_test: %s: scan n=%zu grain=%zu differs at %zu (%.17g vs %.17g)\n",
                label, n, grain, i, got[i], running);
            return false;
        }
    }
    return true;
}

private double[] scanOn(const(ProcessorId)[] cpus, size_t n, size_t grain, out double last)
{
    Dispatcher d;
    d.start(cpus);
    scope (exit) d.stop();
    auto out_ = new double[n];
    last = forkJoinScan!(term, add)(iota(n), out_, 1.0, grain);
    return out_;
}

private __gshared bool g_foreignInvokeOk;
private __gshared uint g_groupCount;

bool runFeaturesTest()
{
    bool ok = true;
    void fail(string what) { printf("features_test: FAILED: %.*s\n", cast(int) what.length, what.ptr); ok = false; }

    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    foreach (ref p; topo.processors) if (topo.isEligible(p)) cpus ~= ProcessorId.of(p);
    if (cpus.length > 8) cpus = cpus[0 .. 8];
    if (cpus.length < 2) { printf("features_test: NOTICE fewer than two eligible processors\n"); return true; }

    // 1. Map: every element written once, several lengths and grains.
    {
        Dispatcher d;
        d.start(cpus);
        foreach (n; [size_t(0), 1, 7, 1000, 10_007])
            foreach (grain; [size_t(1), 3, 64])
            {
                auto out_ = new double[n];
                out_[] = -1.0;
                forkJoinMap!half(iota(n), out_, grain);
                foreach (i; 0 .. n)
                    if (out_[i] != i * 0.5) { fail("map value"); break; }
            }
        d.stop();
    }

    // 2. Scan: sequential prefix, bit-identical across participant counts.
    foreach (n; [size_t(0), 1, 2, 7, 1000, 10_007])
        foreach (grain; [size_t(1), 3, 64, 100_000])
        {
            double lastOne, lastAll;
            auto one = scanOn(cpus[0 .. 1], n, grain, lastOne);
            auto all = scanOn(cpus, n, grain, lastAll);
            if (!scanMatches(all, n, grain, "many")) ok = false;
            if (one != all) fail("scan differs between participant counts");
            immutable expectedLast = n == 0 ? 1.0 : all[n - 1];
            if (lastAll != expectedLast || lastOne != lastAll) fail("scan return value");
        }
    {
        // An integer scan with a seed, grain larger than the input.
        Dispatcher d;
        d.start(cpus);
        auto out_ = new ulong[5];
        immutable last = forkJoinScan!(square, addU)(iota(size_t(5)), out_, 100UL, 8);
        if (out_ != [100UL, 101, 105, 114, 130] || last != 130) fail("integer scan");
        d.stop();
    }

    // 3. Invoke: function pointer, borrowing delegate, lambda; participant and foreign thread.
    {
        Dispatcher d;
        d.start(cpus);
        atomicStore(g_flagA, 0u);
        uint local;
        shared uint lambdaHits;
        forkJoinInvoke(&setA, () nothrow @nogc { local += 10; }, () nothrow @nogc { atomicOp!"+="(lambdaHits, 1u); });
        if (atomicLoad(g_flagA) != 1 || local != 10 || atomicLoad(lambdaHits) != 1) fail("invoke on participant");
        forkJoinInvoke(() nothrow @nogc { local += 1; });
        if (local != 11) fail("invoke with one callable");
        g_foreignInvokeOk = false;
        auto t = new Thread({
            uint a, b;
            forkJoinInvoke(() nothrow @nogc { a = 1; }, () nothrow @nogc { b = 2; }, &setA);
            g_foreignInvokeOk = a == 1 && b == 2;
        });
        t.start();
        t.join();
        if (!g_foreignInvokeOk || atomicLoad(g_flagA) != 2) fail("invoke from a foreign thread");
        d.stop();
    }

    // 4. TaskGroup: a batch from a foreign thread, reused, then a clean stop.
    {
        Dispatcher d;
        d.start(cpus);
        shared uint count;
        auto t = new Thread({
            TaskGroup g;
            auto roots = new Count[300];
            foreach (ref r; roots) { r.node.run = &countRun; r.target = &count; g.add(&r.node); }
            g.wait();
            immutable first = atomicLoad(count);
            g.wait(); // nothing added: returns at once
            foreach (ref r; roots[0 .. 50]) { r.node = Node.init; r.node.run = &countRun; g.add(&r.node); }
            g.wait();
            g_groupCount = first == 300 && atomicLoad(count) == 350 ? 350 : atomicLoad(count);
        });
        t.start();
        t.join();
        if (g_groupCount != 350) fail("task group count");
        d.stop(); // would hang if the group left a live root behind
    }

    // 5. Presets and the counters printer.
    {
        Dispatcher d;
        d.start(cpus, DispatcherOptions.perGroup());
        forkJoinFor!((size_t) {})(iota(size_t(100)), 10);
        d.stop();
        d.start(cpus, DispatcherOptions.oneFarm());
        forkJoinFor!((size_t) {})(iota(size_t(100)), 10);
        d.stop();
        auto s = counters();
        printf("features_test: counters follow\n");
        printCounters(s);
    }

    printf("features_test: %s\n", ok ? "ok".ptr : "FAILED".ptr);
    return ok;
}
