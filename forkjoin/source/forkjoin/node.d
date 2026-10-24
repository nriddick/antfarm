// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// The work item and the trampoline.
///
/// Every task is a heap frame that begins with a `Node`. `run` executes the
/// node and returns the node this thread should run next: the child it
/// chose to run inline after a fork, or a continuation produced by
/// completing a parent. `join` runs once every child has finished and
/// returns null when the node is complete, or the node to run next when it
/// forked again. The worker loop runs `while (n) n = n.run(n)`, so stack
/// depth does not grow with tree depth.
module forkjoin.node;

import core.atomic;

/// The header every frame begins with.
struct Node
{
    /// The frame whose `pending` this one decrements on completion; null for a root.
    Node* parent;
    /// Children not yet finished; the last one to finish runs `join`.
    shared uint pending;
    /// Distance from the root, set by `fork`.
    uint depth;
    /// Runs the node; returns the node to run next on this thread or null.
    Node* function(Node*) nothrow @nogc @system run;
    /// Runs once `pending` reaches zero; returns null when complete, or a node to run.
    Node* function(Node*) nothrow @nogc @system join;
}

/// Roots that have not completed; `Dispatcher.stop` drains it.
package(forkjoin) shared uint g_rootsLive;

/// Runs `node` and every continuation it returns.
void runChain(Node* node) nothrow @nogc @system
{
    while (node !is null)
        node = node.run(node);
}

/// Marks `node` finished: decrements its parent's `pending`, runs the join
/// of every parent this completes, and returns a continuation to run next
/// (a parent that forked again) or null.
Node* complete(Node* node) nothrow @nogc @system
{
    for (;;)
    {
        auto p = node.parent;
        if (p is null)
        {
            atomicOp!"-="(g_rootsLive, 1);
            return null;
        }
        if (atomicOp!"-="(p.pending, 1) != 0)
            return null;
        auto next = p.join(p);
        if (next !is null)
            return next;
        node = p;
    }
}
