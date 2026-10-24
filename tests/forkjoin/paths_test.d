// SPDX-License-Identifier: MIT (see forkjoin/LICENSE)
/// Dispatcher paths the admission test does not reach: one participant (no
/// pool, children run inline), deque overflow spilled into the Farm and run
/// inline when the Farm is bypassed, a join that forks a second phase,
/// participants on two LLC groups and SMT siblings with uniform and biased
/// victim selection, and the spin idle policy. Results are checked; the
/// counters are printed so coverage of a path is visible.
module paths_test;

import forkjoin;
import threadpool;
import core.atomic;
import core.stdc.stdio : printf;
import core.stdc.stdlib : malloc, free;

// A flat fan-out: the root forks `width` leaves, each adding its index.
private struct Flat
{
    Node node;
    Flat* kids;
    uint width;
    ulong sum;
    ulong value;
}

private Node* flatLeaf(Node* node) nothrow @nogc @system
{
    auto f = cast(Flat*) node;
    f.sum = f.value;
    return complete(node);
}

private Node* flatRoot(Node* node) nothrow @nogc @system
{
    auto f = cast(Flat*) node;
    auto kids = cast(Flat*) malloc(f.width * Flat.sizeof);
    auto nodes = cast(Node**) malloc(f.width * (Node*).sizeof);
    foreach (i; 0 .. f.width)
    {
        kids[i] = Flat.init;
        kids[i].value = i;
        kids[i].node.run = &flatLeaf;
        nodes[i] = &kids[i].node;
    }
    f.kids = kids;
    f.node.join = &flatJoin;
    auto next = fork(node, nodes[0 .. f.width]);
    free(nodes);
    return next;
}

private Node* flatJoin(Node* node) nothrow @nogc @system
{
    auto f = cast(Flat*) node;
    ulong s;
    foreach (i; 0 .. f.width) s += f.kids[i].sum;
    f.sum = s;
    free(f.kids);
    return null;
}

// Two phases: the join of phase 0 forks phase 1 and returns its inline child.
private struct Two
{
    Node node;
    Two* kids;
    int phase;
    int depth;
    ulong count;
}

private Node* twoRun(Node* node) nothrow @nogc @system
{
    auto t = cast(Two*) node;
    if (t.depth == 0)
    {
        t.count = 1;
        return complete(node);
    }
    t.kids = cast(Two*) malloc(2 * Two.sizeof);
    t.phase = 0;
    t.node.join = &twoJoin;
    return twoFork(t);
}

private Node* twoFork(Two* t) nothrow @nogc @system
{
    foreach (i; 0 .. 2)
    {
        t.kids[i] = Two.init;
        t.kids[i].depth = t.depth - 1;
        t.kids[i].node.run = &twoRun;
    }
    Node*[2] nodes = [&t.kids[0].node, &t.kids[1].node];
    return fork(&t.node, nodes[]);
}

private Node* twoJoin(Node* node) nothrow @nogc @system
{
    auto t = cast(Two*) node;
    t.count += t.kids[0].count + t.kids[1].count;
    if (t.phase == 0)
    {
        t.phase = 1;
        return twoFork(t); // second phase: the join forks again
    }
    free(t.kids);
    return null;
}

private bool runFlat(uint width, const(char)* label)
{
    Flat root;
    root.width = width;
    root.node.run = &flatRoot;
    runRoot(&root.node);
    ulong expected = cast(ulong) width * (width - 1) / 2;
    if (root.sum != expected)
    {
        printf("paths_test: %s flat(%u) = %llu, expected %llu\n", label, width, root.sum, expected);
        return false;
    }
    return true;
}

private bool runTwo(int depth, const(char)* label)
{
    Two root;
    root.depth = depth;
    root.node.run = &twoRun;
    runRoot(&root.node);
    // Each internal node runs two phases of two children; leaves count 1.
    // count(d) = 1 + 2*count(d-1) per phase, two phases: 2*(2*count(d-1)) + leaf 0 ... computed directly:
    ulong expected = 1;
    foreach (_; 0 .. depth) expected = 4 * expected;
    if (root.count != expected)
    {
        printf("paths_test: %s two(%d) = %llu, expected %llu\n", label, depth, root.count, expected);
        return false;
    }
    return true;
}

private void report(const(char)* label)
{
    auto s = counters();
    printf("paths_test: %s: steals %llu, cross %llu, sibling %llu, spilled %llu, inline %llu, solo %llu, farm hits %llu, parks %llu, wakes %llu\n",
        label, s.stealSuccesses, s.crossSuccesses, s.siblingSuccesses, s.spilledChildren,
        s.inlineChildren, s.soloChildren, s.farmHits, s.parks, s.wakes);
}

/// Returns true on success.
bool runPathsTest()
{
    auto topo = CacheAwarePool.topology();
    ProcessorId[] eligible;
    foreach (ref p; topo.processors)
        if (topo.isEligible(p)) eligible ~= ProcessorId.of(p);
    if (eligible.length == 0) { printf("paths_test: no eligible processors\n"); return false; }
    bool ok = true;

    // 1. One participant: no pool, children run inline, and the sentinel
    //    farm exists for the spill path.
    {
        Dispatcher d;
        d.start(eligible[0 .. 1]);
        discardCounters();
        ok &= runFlat(100, "solo");
        ok &= runTwo(6, "solo");
        d.stop();
        report("one participant");
        if (counters().soloChildren == 0) { printf("paths_test: solo path not taken\n"); ok = false; }
    }

    // 2. Overflow: capacity 64, a 300-wide fork spills to the Farm.
    if (eligible.length >= 2)
    {
        DispatcherOptions o;
        o.dequeCapacity = 64;
        o.park = false; // the spin policy path
        Dispatcher d;
        d.start(eligible[0 .. eligible.length < 4 ? eligible.length : 4], o);
        discardCounters();
        ok &= runFlat(300, "spill");
        ok &= runTwo(8, "spill");
        d.stop();
        report("spill to Farm, spin policy");
        if (counters().spilledChildren == 0) { printf("paths_test: spill path not taken\n"); ok = false; }
    }

    // 3. Overflow with the Farm bypassed: children run inline.
    if (eligible.length >= 2)
    {
        DispatcherOptions o;
        o.dequeCapacity = 64;
        o.spillInline = true;
        Dispatcher d;
        d.start(eligible[0 .. 2], o);
        discardCounters();
        ok &= runFlat(300, "inline");
        d.stop();
        report("spill inline");
        if (counters().inlineChildren == 0) { printf("paths_test: inline path not taken\n"); ok = false; }
    }

    // 4. Two LLC groups and SMT siblings when the host has them, uniform
    //    victims, one Farm per group, and the hot-stats build if present.
    {
        ProcessorId[] cpus;
        ushort firstLlc = topo.byId(eligible[0]).llcIndex;
        // Up to four processors from the first group, then up to four from
        // another group; prefer whole cores so siblings are present.
        foreach (id; eligible) if (topo.byId(id).llcIndex == firstLlc && cpus.length < 4) cpus ~= id;
        size_t n1 = cpus.length;
        foreach (id; eligible) if (topo.byId(id).llcIndex != firstLlc && cpus.length < n1 + 4) cpus ~= id;
        // Add siblings of the chosen cores when they exist.
        foreach (id; eligible)
        {
            if (cpus.length >= 12) break;
            auto core = topo.byId(id).coreIndex;
            bool have, sameCore;
            foreach (c; cpus) { if (c == id) have = true; else if (topo.byId(c).coreIndex == core) sameCore = true; }
            if (!have && sameCore) cpus ~= id;
        }
        if (cpus.length >= 2)
        {
            DispatcherOptions o;
            o.crossDiv = 0;
            o.farmPerGroup = true;
            Dispatcher d;
            d.start(cpus, o);
            discardCounters();
            ok &= runTwo(10, "groups");
            ok &= runFlat(64, "groups");
            d.stop();
            report("two groups, siblings, uniform victims");
        }
        if (cpus.length >= 2)
        {
            DispatcherOptions o;
            o.crossDiv = 2;
            Dispatcher d;
            d.start(cpus, o);
            discardCounters();
            ok &= runTwo(10, "biased");
            d.stop();
            report("two groups, biased victims");
        }
    }
    return ok;
}
