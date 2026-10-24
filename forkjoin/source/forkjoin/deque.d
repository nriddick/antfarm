// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// Bounded Chase-Lev deque of `Node*` after Lê, Pop, Cohen and Nardelli,
/// "Correct and Efficient Work-Stealing for Weak Memory Models" (PPoPP
/// 2013), figure 1, with fixed capacity: a full deque refuses the push and
/// the caller spills to the Farm. One owner pushes and pops at the bottom;
/// any number of thieves steal from the top.
///
/// The owner's pop needs a full barrier between its `bottom` store and its
/// `top` load. By default that is a seq_cst exchange on `bottom`, which on
/// x86-64 is a locked `xchg` and cheaper than `mfence`; build with
/// `-d-version=ForkjoinFence` for the paper's explicit fence.
module forkjoin.deque;

import core.atomic;
import core.stdc.stdlib : aligned_alloc, free;
import core.stdc.string : memset;
import forkjoin.node : Node;

struct Deque
{
    align(64) shared long top;
    align(64) shared long bottom;
    align(64) shared(ulong)* slots;
    size_t mask;
    long capacity;

    /// `capacity` rounds up to a power of two, at least 64.
    static Deque* create(size_t capacity) nothrow @nogc @system
    {
        size_t cap = 64;
        while (cap < capacity) cap <<= 1;
        auto d = cast(Deque*) aligned_alloc(64, Deque.sizeof);
        memset(d, 0, Deque.sizeof);
        d.slots = cast(shared(ulong)*) aligned_alloc(64, cap * ulong.sizeof);
        d.mask = cap - 1;
        d.capacity = cast(long) cap;
        return d;
    }

    void destroy() nothrow @nogc @system
    {
        free(cast(void*) slots);
        free(&this);
    }

    /// Owner only. Pushes a prefix of `xs`, all published by one release
    /// store, and returns how many fit. `depthAfter` receives the owner's
    /// view of the deque's size after the push.
    size_t pushAll(Node*[] xs, out long depthAfter) nothrow @nogc @system
    {
        auto b = atomicLoad!(MemoryOrder.raw)(bottom);
        auto t = atomicLoad!(MemoryOrder.acq)(top);
        auto room = capacity - (b - t);
        if (room <= 0)
        {
            depthAfter = b - t;
            return 0;
        }
        size_t n = xs.length < cast(size_t) room ? xs.length : cast(size_t) room;
        foreach (i; 0 .. n)
            atomicStore!(MemoryOrder.raw)(slots[(b + i) & mask], cast(ulong) xs[i]);
        atomicStore!(MemoryOrder.rel)(bottom, b + n);
        depthAfter = b - t + n;
        return n;
    }

    /// Owner only.
    Node* pop() nothrow @nogc @system
    {
        auto b = atomicLoad!(MemoryOrder.raw)(bottom) - 1;
        version (ForkjoinFence)
        {
            atomicStore!(MemoryOrder.raw)(bottom, b);
            atomicFence!(MemoryOrder.seq)();
        }
        else
        {
            atomicExchange!(MemoryOrder.seq)(&bottom, b);
        }
        auto t = atomicLoad!(MemoryOrder.raw)(top);
        if (t > b)
        {
            atomicStore!(MemoryOrder.raw)(bottom, b + 1);
            return null;
        }
        auto x = cast(Node*) atomicLoad!(MemoryOrder.raw)(slots[b & mask]);
        if (t != b)
            return x;
        // Last item: settle the race with any thief on `top`.
        bool won = cas(&top, t, t + 1);
        atomicStore!(MemoryOrder.raw)(bottom, b + 1);
        return won ? x : null;
    }

    /// Any thread. Null means empty or lost a race; the caller moves on.
    Node* steal() nothrow @nogc @system
    {
        auto t = atomicLoad!(MemoryOrder.acq)(top);
        atomicFence!(MemoryOrder.seq)();
        auto b = atomicLoad!(MemoryOrder.acq)(bottom);
        if (t >= b) return null;
        auto x = cast(Node*) atomicLoad!(MemoryOrder.raw)(slots[t & mask]);
        if (!cas(&top, t, t + 1))
            return null;
        return x;
    }
}
