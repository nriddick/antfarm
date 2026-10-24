// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick.
/// `forkJoinInvoke`: a fixed set of callables run as the children of one
/// node and joined, the `invoke` of citor and TBB. The frames live on the
/// caller's stack, since the caller blocks until the join.
module forkjoin.invoke;

import forkjoin.node;
import forkjoin.dispatcher : fork, runRootAny;
import std.meta : staticMap;

private struct Call(F)
{
    Node node;
    F fn;

    static Node* run(Node* node) nothrow @nogc @system
    {
        auto c = cast(Call*) node;
        c.fn();
        return complete(node);
    }
}

private struct InvokeRoot
{
    Node node;
    Node*[] kids;

    static Node* run(Node* node) nothrow @nogc @system
    {
        auto r = cast(InvokeRoot*) node;
        r.node.join = &join;
        return fork(node, r.kids);
    }

    static Node* join(Node*) nothrow @nogc @system { return null; }
}

/// Runs every callable once, in parallel, and returns when all have
/// returned. Each takes no arguments and is `nothrow @nogc`; delegates may
/// borrow the caller's frame. From a participant the caller runs one of
/// them and helps with the rest; from any other thread the set is
/// submitted as one root.
void forkJoinInvoke(F...)(F fns) nothrow @nogc @system if (F.length >= 1)
{
    staticMap!(Call, F) calls;
    Node*[F.length] nodes;
    static foreach (i; 0 .. F.length)
    {{
        static assert(is(typeof(() nothrow @nogc { fns[i](); })),
            "forkJoinInvoke: every callable takes no arguments and is nothrow @nogc");
        calls[i].node = Node.init;
        calls[i].fn = fns[i];
        calls[i].node.run = &Call!(F[i]).run;
        nodes[i] = &calls[i].node;
    }}
    InvokeRoot root;
    root.kids = nodes[];
    root.node.run = &InvokeRoot.run;
    runRootAny(&root.node);
}
