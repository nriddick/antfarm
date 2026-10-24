# Ant Farm fork-join

`forkjoin` runs recursive divide-and-conquer work on the Ant Farm threadpool:
one bounded work-stealing deque per participant for forked children, the
Farm as the admission and overflow channel, and a continuation-passing join
that never blocks a thread. It is the dispatch layer the
[roadmap](../ROADMAP.md#work-stealing-deques-for-worker-pool-dispatch)
proposed, built against the fork-join benchmarks of
[tzcnt/runtime-benchmarks](https://github.com/tzcnt/runtime-benchmarks) and
measured in [antfarm-fwbench](https://github.com/nriddick/antfarm-fwbench).
The design and the measurements are in
[docs/design/forkjoin.md](../docs/design/forkjoin.md).

| Work shape | Use |
| --- | --- |
| Recursive fork-join, divide and conquer, irregular trees | `forkjoin` |
| A tree with typed result ports, cancellation, deadlines, non-POD state | a [private actor](../docs/guide/private-actors.md) hierarchy |
| Independent elements of a range | a [parallel loop](../docs/guide/parallel-loops.md) or raw payload tables |

## The model

Every task is a heap frame that begins with a `Node`:

```d
struct Node
{
    Node* parent;
    shared uint pending;   // children not yet finished
    uint depth;
    Node* function(Node*) nothrow @nogc @system run;   // returns the node to run next, or null
    Node* function(Node*) nothrow @nogc @system join;  // returns null when complete, or a node to run next
}
```

`fork(parent, kids)` pushes all children but the last onto the calling
participant's deque and returns the last for the caller to run; `complete(node)`
decrements the parent's `pending` and, on reaching zero, runs its `join`. The
worker loop runs `while (n) n = n.run(n)`, so recursion depth is heap frames,
not stack. The last child to finish runs the parent's join on whatever thread
it is on. `join` may fork again and return the child to run next (matmul's
second phase).

Frames come from the dispatcher's frame pool: `allocFrames!Fib(2)` (or
`allocFrame(bytes)`) in `run`, `freeFrame` in `join`. The pool is headerless
and thread-local: a free from the owning thread is a push onto a per-size
list, a free from the thief that finished the last child is one CAS onto the
owner's remote list, and the owner takes that list over only when its own is
empty. It takes the process allocator out of the hot path: fib(39) at 32
threads runs in the same 102 ms under tcmalloc, glibc or mimalloc, where
`malloc` frames ranged from 106 to 135 ms, and nqueens' peak memory falls from
39 MB to 16 MB. Requests above `frameMax` (1 KiB) get a chunk of their own.

Idle participants pop their own deque (LIFO: the newest, smallest subtree),
then poll the Farm, then steal from the top of another deque (FIFO: the
oldest, largest subtree): the last successful victim first, then the SMT
sibling, then one random same-LLC victim seven times in eight. A full deque
spills its surplus into the Farm as one table; a refusing Farm runs the
children inline. With one participant children run inline directly.

Idle workers park on the pool's futex after a spin budget and are woken by a
push onto an empty deque, a spill, or a root submission; a woken worker that
forks wakes the next one. Threads that are not participants submit roots
with `submitRoot` and block in `waitRoot`.

## Loops and reductions

`forkjoin.range` turns a finite random-access range into a fork-join tree:
halves down to `grain` elements, leaves on the deques, a join that never
blocks a worker. `forkJoinFor!fn` calls a function per element,
`forkJoinEach` a delegate that may borrow the caller's frame,
`forkJoinMap!fn` writes `fn(element)` into an output slice,
`forkJoinReduce!(fn, combine)` folds, and `forkJoinScan!(fn, combine)` is
an inclusive prefix fold into an output slice (two passes over one tree,
`fn` once per element).

```d
forkJoinFor!update(iota(n), 64);
forkJoinEach(iota(output.length), 64, (size_t i) nothrow @nogc @system { output[i] = f(i); });
forkJoinMap!f(iota(n), output, 64);
auto sum = forkJoinReduce!(term, add)(iota(n), 0.0, 64);
auto last = forkJoinScan!(term, add)(iota(n), prefix, 0.0, 64);   // prefix[i] = seed + term(0) + ... + term(i)
```

`forkJoinInvoke` runs a fixed set of callables as the children of one node
and returns when all have returned; the frames live on the caller's stack.
A `TaskGroup` lets a thread that is not a participant submit a batch of
roots and wait for them together, and may be reused.

```d
forkJoinInvoke(&buildIndex, () nothrow @nogc { total = sum(parts); });
TaskGroup g;
foreach (ref job; jobs) g.add(&job.node);
g.wait();
```

`DispatcherOptions.oneFarm()` and `DispatcherOptions.perGroup()` are the
two documented shapes; `printCounters(counters())` prints a run's counters.

The split points depend only on the length and the grain, so
`forkJoinReduce` is bit-identical across participant counts: leaf folds run
left to right and the tree combines in a fixed shape. `combine` must be
associative for the result to equal a sequential fold; it need not be
commutative. A participant helps while it waits; any other thread submits
the root and waits on its completion; a loop may start inside a node or a
Farm callback. `submitDetached(root)` publishes a root without waiting
(its `join` is the completion callback); `Dispatcher.stop` drains
outstanding roots first.

## Run it

```text
dub run :forkjoin -c fib          # fib(30) on the first LLC group's processors
dub test :forkjoin -- --DRT-testmode=run-main   # the suite: deque, admission, paths, ranges, completion, frames
```

[examples/forkjoin_fib.d](examples/forkjoin_fib.d) is the complete program.
Build with `-d-version=ForkjoinHotStats` (LDC) or `-version=ForkjoinHotStats`
(DMD) to count every push and pop; those counters cost 1% on fib and 6% on
skynet at two threads, so they are off by default.

## Acknowledgement

Ant Farm is MIT licensed ([LICENSE](../LICENSE)). This package's design
derives from [citor](https://github.com/Lallapallooza/citor)
by Vitalii Shutov (MIT): per-worker Chase-Lev deques with the last child run
inline, the hot-victim hint, same-cluster stealing with a cross-cluster escape
probe, the single-participant inline path and the chain wake. The deque
follows Lê, Pop, Cohen and Nardelli (PPoPP 2013). What is Ant Farm's own is
the continuation-passing join and trampoline, the Farm as admission and
overflow channel, and the parked bitmap over the threadpool's wait protocol.

## Rules

- A frame must be fully initialized before `fork` publishes it, and after
  `fork` returns the caller must not touch the parent except through the
  returned child: a thief may already have completed it.
- `run` and `join` are `nothrow @nogc @system` and must return; they are
  Farm callbacks when a frame arrives through the Farm. Never call a blocking
  parallel loop or `consumeNext` from them.
- Frames belong to the application. `malloc` through mimalloc measured
  faster than a per-participant frame allocator; free a cohort in the join.
- One `Dispatcher` per process, like one `CacheAwarePool`. The calling thread
  is participant 0 and must call `runRoot` itself; other threads use
  `submitRoot`.
- `DispatcherOptions.pollEvery` is load-bearing: a busy participant's Farm
  poll keeps its view advancing reclamation pins, not only fairness.
