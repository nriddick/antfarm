# Fork-join dispatcher design

`forkjoin` is the work-stealing complement to the Farm the
[roadmap](../../ROADMAP.md#work-stealing-deques-for-worker-pool-dispatch)
proposed. This page owns its contract and the reasoning; the user-facing
summary is [forkjoin/README.md](../../forkjoin/README.md). The measurements
were taken in [antfarm-fwbench](https://github.com/nriddick/antfarm-fwbench),
whose `docs/forkjoin-synthesis-plan.md` records every phase, including the
negative results.

The package's design derives from citor by Vitalii Shutov (MIT); the
acknowledgement and the list of what was taken are in the repository
[LICENSE](../../LICENSE), which is MIT for all of Ant Farm.

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
  completion, on the forking thread. It also runs inline inside a Farm
  callback, and on a thread whose home Farm no participant consumes: the
  caller alone in its group under `farmPerGroup` without `callerConsumes`,
  whose group Farm the host creates but nobody polls. With one participant
  the deferred children run inline directly (no thief can exist).
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
  worker's wait word and its bit in the pool's armed bitmap and calls the
  body again; the recheck returns `false`; the pool parks it. A waker
  (`wakeOneArmed`) clears a set bit with CAS and calls `wakeWorker`, which
  exchanges the armed word: a worker armed but not yet asleep returns from
  `parkWorker` at once, so no wake is lost. Wakers: a push that made an empty deque non-empty, a
  spill, a root submission. Each wakes one; a woken worker that forks wakes
  the next. The sleeper's arming is locked RMWs, then its recheck reads the
  work; the waker's publication is a release store (a plain `mov` on
  x86-64), then its bitmap read. That is the store-buffering shape, which
  TSO permits to miss on both sides, so the spill and root-submission
  wakers issue a seq_cst fence between the two: those works may have no
  other awake reader (a non-consuming caller spilling, a foreign
  submitter), and a missed wake there is a hang. The push waker does not
  fence; a missed wake costs parallelism only, since the owner pops its own
  work.
- **Admission.** The caller is participant 0 and runs `runRoot`, which runs
  the root's chain then steps until the root's own sentinel completes, so a
  nested `runRoot` waits only for its own tree. Other threads call
  `submitRoot`: the root's parent is a sentinel `Node` whose `join` sets an
  `Event`; the root goes into the first Farm as a one-entry table through a
  token shared under a spinlock; one parked worker is woken. `waitRoot`
  blocks on the `Event`.
- **Loops.** `forkjoin.range` splits a range in halves down to the grain;
  frames are heap cohorts of two freed in the join, so live frames are
  participants times depth. Split points are a function of length and grain,
  which makes `forkJoinReduce` deterministic across participant counts.
  Both run on the caller when it is a participant (`runRoot`, which waits on
  the root's own sentinel, so a nested loop waits only for its own tree) and
  through `submitRoot`/`waitRoot` otherwise. `submitDetached` is `submitRoot`
  without a completion: the root's parent is null, so its `join` runs only
  after children (as for any node), and a root that forks nothing must
  finish in its `run`; `stop` drains `g_rootsLive` before shutdown.
- **Host.** The dispatcher runs on `threadpool.groups.GroupHost`, which owns
  the pool, the caller's pin (`callerProcessor`), one Farm per group
  (`farmPerGroup`) or one shared Farm, the `GroupSlot` registry and the
  per-thread tokens. The dispatcher builds its participant tables from
  `GroupHost.resolve` before constructing the host, since workers run their
  first body call inside the constructor.
- **Shutdown.** `Dispatcher.stop` drains outstanding roots, then closes the host, which shuts the pool down (every worker runs its
  stop hook: flush counters, unsubscribe, unregister), then the caller
  leaves, the registry is uninstalled, and deques and Farms are destroyed.
  It returns at once when no start succeeded (never started, a failed
  start, a second stop), and is fatal off the thread that started it.
- **Start.** `start()` takes every eligible processor up to
  `maxParticipants` (128) in topology order, with one Farm per group;
  `start(opt)` takes the same processors and `opt` unchanged, so
  `DispatcherOptions.oneFarm()` shares one Farm. An unknown or ineligible
  processor and `bodySteps == 0` are rejected with an exception. SMT
  siblings are matched on `(group, package, core)`, as the topology keys
  cores.
- **Map and scan.** `forkJoinMap` runs the loop tree over a source slice
  paired with an output slice that splits identically. `forkJoinScan` runs
  two passes over one retained tree: pass one writes `fn(element)` into the
  output and folds each leaf and each inner node (`acc`); pass two hands each
  child its prefix (`prefix` for the left child, `combine(prefix, left.acc)`
  for the right) and the leaves fold their prefix in place, so `fn` runs once
  per element and the output is its own scratch. Frames are kept between the
  passes and freed in pass two's join. The split is the reduction's, so the
  result is bit-identical across participant counts; like any tree fold it
  may differ in the last bits from a sequential left fold.
- **Invoke.** `forkJoinInvoke(fns...)` builds one frame per callable on the
  caller's stack (the caller blocks until the join, so the frames outlive
  the children) and forks them from one root; delegates may borrow the
  caller's frame.
- **Task group.** `TaskGroup` is a sentinel node whose `pending` holds one
  count for the group itself plus one per root added; `add` publishes the
  root through the submit Farm, `wait` releases the group's count and
  blocks on an Event when roots are still running, or, when the release
  brings the count to zero, decrements the live-root count itself since no
  join ran. One live root is counted per armed group, so `Dispatcher.stop`
  drains a waited group cleanly; an unwaited group would hold `stop`.
- **Any thread.** `runRootAny` is the one entry the loops, the scan and
  `forkJoinInvoke` share: a participant runs the root and helps, any other
  thread submits it and waits.

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
- A first per-participant frame allocator (64-byte classes, a 16-byte block
  header, 256 KiB chunks) was slower than mimalloc by 8% and raised RSS; the
  2.0.1 pool (`forkjoin.frames`) is headerless with 16-byte classes and
  64 KiB chunks and its fast paths inline into the caller. It beats
  `malloc` under every allocator (below).
- Code layout moved fib by 4 to 10% at equal instruction counts: the worker
  body's burst loop must stay minimal (no per-step stack store), `step` stays
  out of line, and 64-byte function alignment is worth keeping.
- The benchmark harness links each runtime against the first of tcmalloc,
  mimalloc and jemalloc it finds. With `malloc` frames the dispatcher's fib
  depended on that choice: 106 ms with mimalloc, 117 with glibc, 135 with
  tcmalloc at 32 threads, since fib frees each two-child frame on whichever
  thread runs the join and tcmalloc charges for the cross-thread case. That
  is what the frame pool removes.

## The frame pool

`forkjoin.frames` allocates task frames off the process allocator. Blocks
live in 64 KiB chunks aligned to their size; the chunk's first cache line
names the owning slot and the block size, so `freeFrame` finds both by
masking the pointer and no block carries a header. Sizes are 16-byte classes
up to `frameMax` (1 KiB): a fib pair is 112 bytes, not 128. A slot has one
LIFO free list and one bump cursor per class, used only by its owning
thread; the fast paths (pop, push) are `pragma(inline, true)` and the
caller carries no spills for the slow ones. A free from another thread is
one CAS onto the slot's remote list for that class; the owner exchanges the
whole list into its free list only when the list and the bump cursor are
empty, so remote frees never touch the owner's fast path and the Treiber
stack has no pop, hence no ABA. Slots outlive threads: `releaseFrames`,
which `leaveThread` calls, parks the slot, and the next thread to allocate
adopts it with its chunks, lists and pending remote frees, and allocates
from the adopted list first, so dispatcher restarts do not grow the
footprint. Chunks are never returned to the process allocator; requests
above `frameMax` get a chunk of their own and are. Chunks come from
`posix_memalign`, or on Windows from `VirtualAlloc`, whose 64 KiB allocation
granularity is the chunk alignment; slots and deques from the core's
64-byte aligned allocator.

Measured on the 5900XT at 32 threads against the same binaries with `malloc`
frames, same session: fib(39) 102 ms under tcmalloc, glibc and mimalloc alike
(against 135, 117 and 106); skynet 47.6 against 49.3; nqueens 125.7 against
146 with peak memory 16 MB against 39; matmul unchanged. At one thread fib
executes 7% fewer instructions and runs within 2% of the mimalloc build.

Not done: one-way remote tables into another group's Farm (nothing spills in
these benchmarks); a submit token per submitting thread (one shared token
under a spinlock suits occasional roots, not a stream); Windows validation
of the wake path (`threadpool.sys.win_wait` is imported but has not run;
the package type-checks for x86_64-windows-msvc and `check-windows.py` has
a `forkjoin` lane, which has not run yet).
