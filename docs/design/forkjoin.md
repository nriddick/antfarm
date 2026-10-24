# Fork-join dispatcher design

`forkjoin` is the work-stealing complement to the Farm the
[roadmap](../../ROADMAP.md#work-stealing-deques-for-worker-pool-dispatch)
proposed. This page owns its contract and the reasoning; the user-facing
summary is [forkjoin/README.md](../../forkjoin/README.md). The measurements
were taken in [antfarm-fwbench](https://github.com/nriddick/antfarm-fwbench),
whose `docs/forkjoin-synthesis-plan.md` records every phase, including the
negative results.

The package is MIT licensed, unlike the rest of Ant Farm (BSL-1.0), because
its design derives from citor by Vitalii Shutov (MIT); the acknowledgement
and the list of what was taken are in [forkjoin/LICENSE](../../forkjoin/LICENSE).

## Why both a deque and the Farm

The Farm publishes tables cheaply and lets any consumer claim chunks without
CAS, but a recursive fork-join tree does not fit it: every fork was a `write`
whose table header and claim words 31 other consumers could contend on, the
ring is FIFO-ish so the tree expanded breadth-first and filled memory with
cold frames, and when the ring refused, the forking thread ran the subtree
inline where nobody could help. The fib(39) payload port on a 5900XT reached
9.7x on 32 threads against citor's 17x, for that last reason.

A Chase-Lev deque per participant gives the opposite properties for forked
children: the owner pushes with one release store and pops LIFO (the newest,
smallest subtree, cache-warm), thieves take FIFO from the top (the oldest,
largest subtree, one steal moves a whole subtree), and live frames stay
near participants times depth times fanout. The Farm keeps what the deque
cannot do: admission from any thread, bulk tables, and a bounded overflow
when a deque is full. One frame pointer is the unit in both.

## Contract

- **Frame.** Every task begins with a `Node{parent, pending, depth, run,
  join}`. `run` returns the next node for this thread or null; `join` runs
  once `pending` reaches zero and returns null (complete) or a node to run
  (it forked again). Both are `nothrow @nogc @system` and must return.
- **Fork.** `fork(parent, kids)` sets each child's `parent` and `depth`,
  stores `pending = kids.length` (raw: the deque's release store or the Farm
  write publishes it), pushes `kids[0 .. $-1]` with one release store and
  returns `kids[$-1]`. Overflow: the refused suffix goes to the home Farm as
  one uniform one-word table; what the Farm refuses runs inline, to
  completion, on the forking thread. With one participant the deferred
  children run inline directly (no thief can exist).
- **Completion.** `complete(node)`: `atomicOp!"-="` on `parent.pending`
  (seq_cst; acq_rel would do). The decrementer that reaches zero runs
  `join`. Climbing continues through `join == null` returns. The root's
  parent is null; its completion decrements `g_rootsLive`. Exactly-once
  execution follows from the deque's exactly-once pop/steal and the Farm's
  exactly-once claim.
- **Deque.** Lê, Pop, Cohen and Nardelli (PPoPP 2013) figure 1 with fixed
  capacity. `pushAll`: relaxed load of `bottom`, acquire load of `top`,
  relaxed slot stores, one release store of `bottom`. `pop`: seq_cst
  exchange on `bottom` (a full barrier on x86-64, replacing the paper's
  `mfence`; `-version=ForkjoinFence` restores the fence form), relaxed load
  of `top`, seq_cst CAS on `top` for the last item only. `steal`: acquire
  `top`, seq_cst fence, acquire `bottom`, slot load, seq_cst CAS on `top`.
  A full deque never overwrites a slot a thief may still read: the owner
  refuses when `bottom - top >= capacity` against a `top` it may only have
  under-read. Pointers are word loads, so a stale slot read whose CAS fails
  is discarded, never torn.
- **Scheduling order.** Pop own deque; every `pollEvery` pops and whenever
  the deque is empty, `consumeNext` once on the home view; then steal: hot
  victim, SMT sibling, one random victim (same group with probability
  `1 - 1/crossDiv`); then pause. The Farm poll is load-bearing: a subscribed
  view that never advances pins ring reclamation for every writer.
- **Parking.** Through the threadpool's protocol only. After `spinSteps`
  consecutive failed steps the body returns `false`; the pool arms the
  worker's wait word and calls the body again; the recheck sets the worker's
  bit in `g_parkedBits` and returns `false`; the pool parks it. A waker
  clears a set bit with CAS and calls `wakeWorker`, which exchanges the armed
  word: a worker armed but not yet asleep returns from `parkWorker` at once,
  so no wake is lost. Wakers: a push that made an empty deque non-empty, a
  spill, a root submission. Each wakes one; a woken worker that forks wakes
  the next.
- **Admission.** The caller is participant 0 and runs `runRoot`, which runs
  the root's chain then steps until `g_rootsLive == 0`. Other threads call
  `submitRoot`: the root's parent is a sentinel `Node` whose `join` sets an
  `Event`; the root goes into the first Farm as a one-entry table through a
  token shared under a spinlock; one parked worker is woken. `waitRoot`
  blocks on the `Event`.
- **Shutdown.** `Dispatcher.stop` shuts the pool down (every worker runs its
  stop hook: flush counters, unsubscribe, unregister), then the caller
  leaves, the registry is uninstalled, and deques and Farms are destroyed.

## What was measured and what was not

On the 5900XT (two CCDs of eight cores, SMT), against the C++ runtimes in
the same session, 32 threads: fib(39) 103.6 ms against citor's 107.5 and
libfork's 139.3; skynet 47.1 against citor's 53.9; nqueens 126.5 against
libfork's 140.8; matmul 60.1 against libfork's 59.9. First at every thread
count from 2 up on fib, skynet and nqueens; RSS 22 MB on fib where the
payload port needed 55.

Findings that shaped the defaults:

- Topology-aware victim selection (same-LLC bias, sibling first, cross-group
  ratio) measured as a no-op on this host for these benchmarks: cross-group
  steals are a few hundred per hundred million nodes. It is kept because it
  is what a per-group Farm host needs and because hosting.md's measurements
  say it matters once steals carry real data across groups.
- citor's owner-local join fast path does not transfer: the join migrates to
  the thread that finishes the last child, so `pending` must stay atomic.
- The pop barrier is inherent to Chase-Lev at every deque size; libfork and
  citor pay it too. The payload port's inline path was the only thing that
  avoided it, at the price of no redistribution.
- A per-participant frame allocator (size classes, local free lists, remote
  free stack) was slower than mimalloc by 8% and raised RSS; frames stay on
  `malloc`.
- Code layout moved fib by 4 to 10% at equal instruction counts: the worker
  body's burst loop must stay minimal (no per-step stack store), `step` stays
  out of line, and 64-byte function alignment is worth keeping.

Not done: a ThreadSanitizer lane for the dispatcher (the deque has one);
one-way remote tables into another group's Farm (nothing spills in these
benchmarks); a submit token per submitting thread (one shared token under a
spinlock suits occasional roots, not a stream); Windows validation of the
wake path (`threadpool.sys.win_wait` is imported but has not run); and the
nqueens cohort frames on the heap (38 MB against 19 for stack-framed
runtimes).
