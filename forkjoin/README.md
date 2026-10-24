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

## Run it

```text
dub run :forkjoin -c fib          # fib(30) on the first LLC group's processors
dub test :forkjoin                # deque exactly-once test and the admission test
```

[examples/forkjoin_fib.d](examples/forkjoin_fib.d) is the complete program.
Build with `-d-version=ForkjoinHotStats` (LDC) or `-version=ForkjoinHotStats`
(DMD) to count every push and pop; those counters cost 1% on fib and 6% on
skynet at two threads, so they are off by default.

## License and acknowledgement

This package is MIT licensed ([LICENSE](LICENSE)); the rest of Ant Farm is
BSL-1.0. Its design derives from [citor](https://github.com/Lallapallooza/citor)
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
