// SPDX-License-Identifier: MIT
/// Completion signalling and the Farm paths: roots submitted concurrently by
/// several foreign threads complete with their results visible after
/// `waitRoot`; detached roots are all joined by `stop`; a node that arrives
/// through the Farm (a submitted root, a spilled child) may start a nested
/// loop inside the Farm callback; and with one Farm per group, roots
/// submitted while every worker is parked still complete, since the wake
/// targets the Farm's group. A watchdog turns a hang into a failure.
module completion_test;

import forkjoin;
import threadpool;
import core.atomic;
import core.thread;
import core.time;
import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, free, exit;
import std.range : iota;

private __gshared shared(uint)[] hits;

private void visit(size_t i) nothrow @nogc @system
{
    atomicOp!"+="(hits[i], 1u);
}

private bool hitsOnce(size_t n, const(char)* label)
{
    foreach (i; 0 .. n)
        if (atomicLoad(hits[i]) != 1)
        {
            printf("completion_test: %s: element %zu hit %u times\n", label, i, atomicLoad(hits[i]));
            return false;
        }
    return true;
}

// A root whose run starts a loop over [lo, lo + n): when the root arrives
// through the Farm, the loop starts inside a Farm callback.
private struct LoopRoot
{
    Node node;
    size_t lo, n;
    ulong sum;
}

private Node* loopRun(Node* node) nothrow @nogc @system
{
    auto r = cast(LoopRoot*) node;
    forkJoinFor!visit(iota(r.lo, r.lo + r.n), 7);
    r.sum = forkJoinReduce!((size_t x) => cast(ulong) x, (ulong a, ulong b) => a + b)(iota(r.lo, r.lo + r.n), 0UL, 5);
    return complete(node);
}

// A wide root whose children (spilled into the Farm with a small deque) each
// run a nested loop.
private struct Wide
{
    Node node;
    Wide* kids;
    size_t width;
    ulong total;
}

private Node* wideLeaf(Node* node) nothrow @nogc @system
{
    auto w = cast(Wide*) node;
    w.total = forkJoinReduce!((size_t x) => cast(ulong) x, (ulong a, ulong b) => a + b)(iota(size_t(0), size_t(40)), 0UL, 4);
    return complete(node);
}

private Node* wideRun(Node* node) nothrow @nogc @system
{
    auto w = cast(Wide*) node;
    w.kids = cast(Wide*) malloc(w.width * Wide.sizeof);
    auto nodes = cast(Node**) malloc(w.width * (Node*).sizeof);
    foreach (i; 0 .. w.width)
    {
        w.kids[i] = Wide.init;
        w.kids[i].node.run = &wideLeaf;
        nodes[i] = &w.kids[i].node;
    }
    w.node.join = &wideJoin;
    auto next = fork(node, nodes[0 .. w.width]);
    free(nodes);
    return next;
}

private Node* wideJoin(Node* node) nothrow @nogc @system
{
    auto w = cast(Wide*) node;
    ulong t;
    foreach (i; 0 .. w.width) t += w.kids[i].total;
    w.total = t;
    free(w.kids);
    return null;
}

// 70 children with a deque of 64 from a root that arrived through the Farm:
// the surplus must run inline, since the nested wait cannot poll the view.
private struct Seventy
{
    Node node;
    Node[70] kids;
    shared uint ran;
}

private Node* seventyLeaf(Node* node) nothrow @nogc @system
{
    forkJoinFor!visitNothing(iota(size_t(8)), 1);
    atomicOp!"+="((cast(Seventy*) node.parent).ran, 1u);
    return complete(node);
}

private void visitNothing(size_t) nothrow @nogc @system {}

private Node* seventyJoin(Node*) nothrow @nogc @system { return null; }

private Node* seventyRun(Node* node) nothrow @nogc @system
{
    auto s = cast(Seventy*) node;
    s.node.join = &seventyJoin;
    Node*[70] kids;
    foreach (i, ref k; s.kids) { k.run = &seventyLeaf; kids[i] = &k; }
    return fork(node, kids[]);
}

// A leaf root for the sustained stream.
private Node* leafRun(Node* node) nothrow @nogc @system { return complete(node); }

private __gshared int g_failures;
private shared uint g_watchdogArmed;

private void watchdog()
{
    version (TSan) auto deadline = MonoTime.currTime + 900.seconds;
    else auto deadline = MonoTime.currTime + 120.seconds;
    while (atomicLoad(g_watchdogArmed) && MonoTime.currTime < deadline)
        Thread.sleep(50.msecs);
    if (atomicLoad(g_watchdogArmed))
    {
        printf("completion_test: FAILED: timed out after 120 s (a submitted root never completed)\n");
        exit(1);
    }
}

private Thread spawnSubmitter(size_t base)
{
    // A parameter, not the loop variable: each closure gets its own base.
    return new Thread({ submitterLoops(base); });
}

private void submitterLoops(size_t base)
{
    foreach (round; 0 .. 20)
    {
        LoopRoot root;
        root.lo = base + round * 100;
        root.n = 100;
        root.node.run = &loopRun;
        RootCompletion c;
        submitRoot(&root.node, &c);
        waitRoot(&c);
        ulong expected = 0;
        foreach (i; root.lo .. root.lo + root.n) expected += i;
        if (root.sum != expected)
        {
            printf("completion_test: submitter %zu round %d: sum %llu, expected %llu\n", base, round, root.sum, expected);
            atomicOp!"+="(*cast(shared int*) &g_failures, 1);
        }
    }
}

private void submitterParked()
{
    foreach (round; 0 .. 30)
    {
        LoopRoot root;
        root.lo = 0;
        root.n = 50;
        root.node.run = &loopRun;
        RootCompletion c;
        submitRoot(&root.node, &c);
        waitRoot(&c);
        if (root.sum != 1225) atomicOp!"+="(*cast(shared int*) &g_failures, 1);
        Thread.sleep(8.msecs); // every worker parks again before the next root
    }
}

/// Returns true on success.
bool runCompletionTest()
{
    auto topo = CacheAwarePool.topology();
    ProcessorId[] cpus;
    ushort firstLlc = ushort.max;
    size_t groups;
    foreach (ref p; topo.processors)
    {
        if (!topo.isEligible(p)) continue;
        if (firstLlc == ushort.max) firstLlc = p.llcIndex;
        if (p.llcIndex == firstLlc && cpus.length < 4) cpus ~= ProcessorId.of(p);
        else if (p.llcIndex != firstLlc && cpus.length < 8) cpus ~= ProcessorId.of(p);
    }
    foreach (ref p; topo.processors) if (topo.isEligible(p) && p.llcIndex != firstLlc) { groups = 2; break; }
    if (cpus.length < 2) { printf("completion_test: NOTICE fewer than two eligible processors\n"); return true; }
    g_failures = 0;
    atomicStore(g_watchdogArmed, 1u);
    auto dog = new Thread(&watchdog);
    dog.start();
    bool ok = true;

    // 1. Three foreign submitters at once, nested loops inside Farm callbacks.
    printf("completion_test: case 1 (concurrent submitters, nested loops in callbacks, %zu cpus, farmPerGroup %d)\n", cpus.length, groups == 2);
    {
        DispatcherOptions o;
        o.farmPerGroup = groups == 2;
        Dispatcher d;
        d.start(cpus, o);
        hits = new shared(uint)[6000];
        Thread[] ts;
        foreach (k; 0 .. 3)
        {
            auto t = spawnSubmitter(k * 2000);
            t.start();
            ts ~= t;
        }
        foreach (t; ts) t.join();
        printf("completion_test: case 1 submitters joined\n");
        ok &= hitsOnce(6000, "concurrent submitters");
        // Spilled children (deque capacity 64, 300-wide fork) each start a
        // nested reduction inside the Farm callback that runs them.
        d.stop();
    }
    printf("completion_test: case 1b (spilled children start nested reductions)\n");
    {
        DispatcherOptions o;
        o.dequeCapacity = 64;
        Dispatcher d;
        d.start(cpus, o);
        discardCounters();
        Wide w; w.width = 300; w.node.run = &wideRun;
        runRoot(&w.node);
        d.stop();
        if (w.total != 300 * 780) { printf("completion_test: spilled nested loops total %llu\n", w.total); ok = false; }
        if (counters().spilledChildren == 0) { printf("completion_test: NOTICE nothing spilled in the nested-spill case\n"); }
    }

    // 2. Parked workers with one Farm per group: submitted roots must wake a
    //    worker of the Farm's group, not just any worker.
    printf("completion_test: case 2 (per-group Farms, parked workers)\n");
    if (groups == 2)
    {
        DispatcherOptions o;
        o.farmPerGroup = true;
        Dispatcher d;
        d.start(cpus, o);
        discardCounters();
        Thread.sleep(40.msecs); // everyone parks
        auto t = new Thread(&submitterParked);
        t.start(); t.join();
        d.stop();
        auto s = counters();
        printf("completion_test: per-group Farms, parked workers: parks %llu, wakes %llu\n", s.parks, s.wakes);
    }
    else
        printf("completion_test: NOTICE one LLC group: per-group parked-wake case not exercised\n");

    // 3. One worker, deque of 64, a foreign root with 70 children each
    //    starting a nested loop: the surplus runs inline (reviewer case).
    printf("completion_test: case 3 (nested loops with forced overflow, one worker)\n");
    {
        DispatcherOptions o;
        o.dequeCapacity = 64;
        o.pollEvery = 1;
        Dispatcher d;
        d.start(cpus[0 .. 2], o);
        auto t = new Thread({
            Seventy s;
            s.node.run = &seventyRun;
            RootCompletion c;
            submitRoot(&s.node, &c);
            waitRoot(&c);
            if (atomicLoad(s.ran) != 70) { printf("completion_test: case 3 ran %u of 70\n", atomicLoad(s.ran)); atomicOp!"+="(*cast(shared int*) &g_failures, 1); }
        });
        t.start(); t.join();
        d.stop();
    }

    // 4. A sustained stream of roots from a foreign thread while the caller
    //    sleeps: 30,000 one-entry tables lap a 2 MiB ring several times
    //    (reviewer case: an idle caller view pinned reclamation).
    printf("completion_test: case 4 (sustained foreign stream, idle caller, 2 MiB ring)\n");
    {
        DispatcherOptions o;
        o.ringMiB = 2;
        Dispatcher d;
        d.start(cpus, o);
        shared uint finished;
        // Several laps of a 2 MiB ring; fewer under the sanitizer's slowdown.
        version (TSan) enum streamRoots = 4000;
        else enum streamRoots = 30_000;
        auto t = new Thread({
            foreach (i; 0 .. streamRoots)
            {
                Node root;
                root.run = &leafRun;
                RootCompletion c;
                submitRoot(&root, &c);
                waitRoot(&c);
                atomicOp!"+="(finished, 1u);
            }
        });
        t.start(); t.join();
        d.stop();
        if (atomicLoad(finished) != streamRoots) { printf("completion_test: stream finished %u\n", atomicLoad(finished)); ok = false; }
    }

    // 5. Asymmetric selection: the caller alone in its group, one worker in
    //    another group (reviewer case: no Farm for the caller's group).
    if (groups == 2)
    {
        printf("completion_test: case 5 (caller alone in its group)\n");
        ProcessorId other = cpus[0];
        foreach (c; cpus) if (topo.byId(c).llcIndex != firstLlc) { other = c; break; }
        Dispatcher d;
        d.start([cpus[0], other]);
        hits = new shared(uint)[1000];
        forkJoinFor!visit(iota(size_t(1000)), 10);
        ok &= hitsOnce(1000, "caller-only group");
        d.stop();
    }

    // 6. A failed start rolls back so a later start succeeds.
    printf("completion_test: case 6 (failed start, then a valid one)\n");
    {
        DispatcherOptions bad;
        bad.ringMiB = 2;
        bad.smallQuota = 30_000; // 9 producers times 30,000 words do not fit 7/8 of a 2 MiB ring
        Dispatcher d;
        bool threw;
        try d.start(cpus, bad);
        catch (Exception e) threw = true;
        if (!threw) { printf("completion_test: the oversized quota did not fail\n"); ok = false; if (d.host !is null) d.stop(); }
        else
        {
            try { d.start(cpus[0 .. 2]); d.stop(); }
            catch (Exception e) { printf("completion_test: retry after a failed start: %.*s\n", cast(int) e.msg.length, e.msg.ptr); ok = false; }
        }
    }

    atomicStore(g_watchdogArmed, 0u);
    dog.join();   // joined, so the runtime's exit path does not race its teardown
    if (g_failures) { printf("completion_test: %d wrong results\n", g_failures); ok = false; }
    printf("completion_test: %s\n", ok ? "ok".ptr : "FAILED".ptr);
    return ok;
}
