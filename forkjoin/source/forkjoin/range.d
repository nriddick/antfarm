// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// Loops and reductions over finite random-access ranges as fork-join trees.
///
/// `forkJoinFor` and `forkJoinReduce` split a range in halves down to `grain`
/// elements, run the leaves on the dispatcher and join back up. The split
/// points depend only on the length and the grain, never on how many
/// participants there are or who stole what, so `forkJoinReduce` is
/// bit-identical across participant counts: the leaf folds run left to right
/// and the tree combines in a fixed shape. Uneven iterations balance through
/// stealing. The caller blocks until the join; a participant helps while it
/// waits, any other thread submits the root and waits on its completion.
/// Frames are heap cohorts of two, freed in the join.
module forkjoin.range;

import antfarm : fatal;
import forkjoin.node;
import forkjoin.dispatcher;
import core.stdc.stdlib : malloc, free;
import std.range.primitives : ElementType, hasLength, hasSlicing, isRandomAccessRange;

private enum suitableRange(R) = isRandomAccessRange!R && hasLength!R && hasSlicing!R;

private struct Frame(S, T)
{
    Node node;
    S slice;
    T acc;
    void* pool;
}

/// Runs `fn(element)` once for every element of `source`, in parallel, and
/// returns when every call has returned. `fn` is `nothrow @nogc`. `grain`
/// is the largest number of elements one leaf runs sequentially.
void forkJoinFor(alias fn, R)(R source, size_t grain = 1) nothrow @nogc @system
    if (suitableRange!R)
{
    static void leaf(S)(ref S slice, void*) nothrow @nogc @system
    {
        foreach (i; 0 .. slice.length) fn(slice[i]);
    }
    runTree!(typeof(source[0 .. 0]), ubyte, leaf, null)(source, grain, null, ubyte.init);
}

/// The same loop with a body that may borrow the caller's frame. The delegate
/// takes one element, is `nothrow @nogc`, and must not `break`, return
/// non-locally or `goto` out of the body; iterations run concurrently and
/// must keep their accesses disjoint or synchronized.
void forkJoinEach(R, D)(R source, size_t grain, scope D body) nothrow @nogc @system
    if (suitableRange!R && is(D == delegate))
{
    import std.traits : FunctionAttribute, functionAttributes, ReturnType;
    static assert(is(typeof(body(source[0]))), "forkJoinEach body must accept one element");
    static assert((functionAttributes!D & FunctionAttribute.nothrow_) && (functionAttributes!D & FunctionAttribute.nogc),
        "forkJoinEach body must be nothrow @nogc");
    static assert(is(ReturnType!D == void), "forkJoinEach body returns void: break and return are not available");
    static void leaf(S)(ref S slice, void* ctx) nothrow @nogc @system
    {
        auto b = *cast(D*) ctx;
        foreach (i; 0 .. slice.length) b(slice[i]);
    }
    runTree!(typeof(source[0 .. 0]), ubyte, leaf, null)(source, grain, cast(void*) &body, ubyte.init);
}

/// Folds `fn(element)` over `source` with `combine`, starting every leaf from
/// `seed`: `combine(combine(seed, fn(a)), fn(b))` within a leaf, then
/// `combine(left, right)` up the tree. `combine` must be associative for the
/// result to equal a sequential fold; it need not be commutative. The split
/// points are a function of length and grain only, so the result is
/// bit-identical for any participant count.
T forkJoinReduce(alias fn, alias combine, R, T)(R source, T seed, size_t grain = 1)
    nothrow @nogc @system if (suitableRange!R)
{
    static void leaf(S)(ref S slice, void* ctx, ref T acc) nothrow @nogc @system
    {
        acc = *cast(T*) ctx;
        foreach (i; 0 .. slice.length) acc = combine(acc, fn(slice[i]));
    }
    static T join(ref T a, ref T b) nothrow @nogc @system { return combine(a, b); }
    return runTree!(typeof(source[0 .. 0]), T, leaf, join)(source, grain, cast(void*) &seed, seed);
}

// ---------------------------------------------------------------------------

private struct Pool(S, T, alias leaf, alias join)
{
    void* ctx;
    size_t grain;

    alias F = Frame!(S, T);

    static Node* run(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        auto p = cast(Pool*) f.pool;
        immutable n = f.slice.length;
        if (n <= p.grain)
        {
            static if (is(T == ubyte)) leaf(f.slice, p.ctx);
            else leaf(f.slice, p.ctx, f.acc);
            return complete(node);
        }
        auto kids = cast(F*) malloc(2 * F.sizeof);
        if (kids is null) fatal("forkjoin: frame allocation failed");
        immutable mid = n / 2;
        kids[0] = F.init;
        kids[0].slice = f.slice[0 .. mid];
        kids[0].pool = p;
        kids[0].node.run = &run;
        kids[1] = F.init;
        kids[1].slice = f.slice[mid .. n];
        kids[1].pool = p;
        kids[1].node.run = &run;
        f.node.join = &joinKids;
        // The kids pointer travels in the parent's own slice slot: a parent
        // that forked no longer needs its slice.
        *cast(F**) &f.slice = kids;
        Node*[2] nodes = [&kids[0].node, &kids[1].node];
        return fork(node, nodes[]);
    }

    static Node* joinKids(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        auto kids = *cast(F**) &f.slice;
        static if (!is(T == ubyte)) f.acc = join(kids[0].acc, kids[1].acc);
        free(kids);
        return null;
    }
}

private T runTree(S, T, alias leaf, alias join, R)(R source, size_t grain, void* ctx, T seed)
    nothrow @nogc @system
{
    if (grain == 0) fatal("forkjoin: grain must be positive");
    static assert(S.sizeof >= (void*).sizeof, "slice type must hold a pointer");
    Pool!(S, T, leaf, join) pool;
    pool.ctx = ctx;
    pool.grain = grain;
    Frame!(S, T) root;
    root.slice = source[0 .. source.length];
    root.acc = seed;
    root.pool = &pool;
    root.node.run = &pool.run;
    if (isParticipant())
        runRoot(&root.node);
    else
    {
        RootCompletion c;
        submitRoot(&root.node, &c);
        waitRoot(&c);
    }
    return root.acc;
}
