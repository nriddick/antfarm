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
/// Frames are cohorts of two from the frame pool, freed in the join.
/// `forkJoinMap` writes a transform into an output slice over the same tree,
/// and `forkJoinScan` is an inclusive prefix fold in two passes over one
/// retained tree.
module forkjoin.range;

import antfarm : fatal;
import forkjoin.node;
import forkjoin.dispatcher;
import forkjoin.frames : allocFrames, freeFrame;
import std.range.primitives : ElementType, hasLength, hasSlicing, isRandomAccessRange;

private enum suitableRange(R) = isRandomAccessRange!R && hasLength!R && hasSlicing!R;

/// Accumulator type of the loop paths, which fold nothing.
private struct NoAcc {}

/// A source slice paired with an output slice of the same length, so a tree
/// over one splits the other identically.
private struct Paired(S, O)
{
    S src;
    O dst;
    size_t length() { return src.length; }
    Paired opSlice(size_t a, size_t b) { return Paired(src[a .. b], dst[a .. b]); }
}

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
    runTree!(typeof(source[0 .. 0]), NoAcc, leaf, null)(source, grain, null, NoAcc.init);
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
    runTree!(typeof(source[0 .. 0]), NoAcc, leaf, null)(source, grain, cast(void*) &body, NoAcc.init);
}

/// Writes `output[i] = fn(source[i])` for every element, in parallel, and
/// returns when every write is done. `output` is a random-access range of
/// the same length whose elements are assignable (a slice); each leaf
/// writes its own elements and nothing else, so no synchronization is
/// needed. `fn` is `nothrow @nogc`.
void forkJoinMap(alias fn, R, O)(R source, O output, size_t grain = 1) nothrow @nogc @system
    if (suitableRange!R && suitableRange!O)
{
    if (output.length != source.length) fatal("forkJoinMap: output length differs from source length");
    static void leaf(S)(ref S slice, void*) nothrow @nogc @system
    {
        foreach (i; 0 .. slice.length) slice.dst[i] = fn(slice.src[i]);
    }
    alias P = Paired!(typeof(source[0 .. 0]), typeof(output[0 .. 0]));
    runTree!(P, NoAcc, leaf, null)(P(source[0 .. source.length], output[0 .. output.length]), grain, null, NoAcc.init);
}

/// Inclusive prefix fold: `output[i]` becomes
/// `combine(... combine(combine(seed, fn(source[0])), fn(source[1])) ..., fn(source[i]))`
/// and the last output value is returned (`seed` for an empty source).
/// `combine` must be associative; the split points depend on length and
/// grain only, so the result is bit-identical for any participant count,
/// though a floating-point result may differ in the last bits from a
/// sequential left fold, as any tree fold does. Two passes
/// over one tree: the first writes `fn(element)` into `output` and folds
/// each leaf, the second folds each element's prefix in place. `fn` runs
/// once per element; `output`'s elements hold `T`.
T forkJoinScan(alias fn, alias combine, R, O, T)(R source, O output, T seed, size_t grain = 1)
    nothrow @nogc @system if (suitableRange!R && suitableRange!O)
{
    if (grain == 0) fatal("forkjoin: grain must be positive");
    if (output.length != source.length) fatal("forkJoinScan: output length differs from source length");
    if (source.length == 0) return seed;
    alias P = Paired!(typeof(source[0 .. 0]), typeof(output[0 .. 0]));
    ScanPool!(P, T, fn, combine) pool;
    pool.grain = grain;
    ScanFrame!(P, T) root;
    root.slice = P(source[0 .. source.length], output[0 .. output.length]);
    root.pool = &pool;
    root.node.run = &pool.run1;
    runRootAny(&root.node);
    root.node = Node.init;
    root.node.run = &pool.run2;
    root.prefix = seed;
    runRootAny(&root.node);
    return output[source.length - 1];
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
            static if (is(T == NoAcc)) leaf(f.slice, p.ctx);
            else leaf(f.slice, p.ctx, f.acc);
            return complete(node);
        }
        auto kids = allocFrames!F(2);
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
        static if (!is(T == NoAcc)) f.acc = join(kids[0].acc, kids[1].acc);
        freeFrame(kids);
        return null;
    }
}

private struct ScanFrame(P, T)
{
    Node node;
    P slice;
    T acc;      // fold of this subtree's fn values (pass 1)
    T prefix;   // fold of everything before this subtree, seed included (pass 2)
    void* pool;
    ScanFrame* kids;  // retained between the passes; null on a leaf
}

private struct ScanPool(P, T, alias fn, alias combine)
{
    size_t grain;
    alias F = ScanFrame!(P, T);

    /// Pass 1: write fn values, fold each leaf, combine up the tree.
    static Node* run1(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        auto p = cast(ScanPool*) f.pool;
        immutable n = f.slice.length;
        if (n <= p.grain)
        {
            f.slice.dst[0] = fn(f.slice.src[0]);
            f.acc = f.slice.dst[0];
            foreach (i; 1 .. n)
            {
                f.slice.dst[i] = fn(f.slice.src[i]);
                f.acc = combine(f.acc, f.slice.dst[i]);
            }
            return complete(node);
        }
        auto kids = allocFrames!F(2);
        if (kids is null) fatal("forkjoin: frame allocation failed");
        immutable mid = n / 2;
        kids[0] = F.init;
        kids[0].slice = f.slice[0 .. mid];
        kids[0].pool = p;
        kids[0].node.run = &run1;
        kids[1] = F.init;
        kids[1].slice = f.slice[mid .. n];
        kids[1].pool = p;
        kids[1].node.run = &run1;
        f.kids = kids;
        f.node.join = &join1;
        Node*[2] nodes = [&kids[0].node, &kids[1].node];
        return fork(node, nodes[]);
    }

    static Node* join1(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        f.acc = combine(f.kids[0].acc, f.kids[1].acc);
        return null;
    }

    /// Pass 2: hand each child its prefix, fold leaves in place, free frames.
    static Node* run2(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        auto kids = f.kids;
        if (kids is null)
        {
            auto running = f.prefix;
            foreach (i; 0 .. f.slice.length)
            {
                running = combine(running, f.slice.dst[i]);
                f.slice.dst[i] = running;
            }
            return complete(node);
        }
        kids[0].prefix = f.prefix;
        kids[1].prefix = combine(f.prefix, kids[0].acc);
        kids[0].node = Node.init;
        kids[0].node.run = &run2;
        kids[1].node = Node.init;
        kids[1].node.run = &run2;
        f.node.join = &join2;
        Node*[2] nodes = [&kids[0].node, &kids[1].node];
        return fork(node, nodes[]);
    }

    static Node* join2(Node* node) nothrow @nogc @system
    {
        auto f = cast(F*) node;
        freeFrame(f.kids);
        f.kids = null;
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
    runRootAny(&root.node);
    return root.acc;
}
