// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Nathan Riddick. Design derived from citor by Vitalii
// Shutov (MIT); see the repository LICENSE.
/// Fork-join dispatcher for Ant Farm.
///
/// One bounded Chase-Lev deque per participant holds forked children; idle
/// participants steal from the top of another deque, same LLC group first.
/// The Farm is the admission channel for roots from other threads and the
/// overflow channel for a full deque. `forkjoin.frames` allocates the
/// frames themselves off the process allocator's hot path. Every task is a heap frame beginning
/// with a `Node`; `run` and `join` return continuations and the worker loop
/// is a trampoline, so the join never blocks a thread and stack depth does
/// not grow with tree depth. Design and measurements: docs/design/forkjoin.md.
module forkjoin;

public import forkjoin.node;
public import forkjoin.deque;
public import forkjoin.dispatcher;
public import forkjoin.range;
public import forkjoin.frames;
