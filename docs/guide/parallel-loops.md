# Parallel loops

Import `antfarm.parallel` for finite, sliceable random-access ranges such as
`iota`. Functions must accept one element by value and be `nothrow @nogc`;
their return values are ignored. These are countable Farm payloads, with no
actor identity or Fiber suspension.

## Blocking loops

Use a registered producer token and a subscribed, exclusively owned view:

```d
// fragment: view is subscribed to the Farm that issued token
import antfarm.parallel;
import std.range : iota;

parallelForWait!update(iota(n), token, view, Options(grain: 64));

// The delegate borrows the calling frame until every body returns.
foreach (i; parallel(view, token, iota(output.length), Options(grain: 64)))
    output[i] = transform(i);
```

Both forms publish with retry and help-drain `view` until completion. They
allocate no region storage: it lives on the calling stack. The delegate must
also be `nothrow @nogc`. Its captures and mutable range data must be disjoint
between iterations or synchronized by the application. Do not use `break`,
nonlocal `return`, or `goto` out of the body; a nonzero delegate result is
fatal, including in release builds. `continue` is allowed.

**Never call a blocking parallel loop inside a Farm callback or table
completion hook.** Re-entering the running view corrupts its walk; waiting
without draining can deadlock all workers. Assertion builds track this with
a thread-local flag and fatal before publishing, including for empty loops.
Release builds retain the caller contract without the flag's overhead. Nested
work uses asynchronous publication and a hook, or a private actor tree with
its own structured join protocol.

The view must belong to the token's Farm. Other consumers can help, but the
caller can complete the region alone. When using parked workers, the host
still owns waking them; these adapters do not integrate with `FarmHost`'s
attachment or wake policy automatically.

## Asynchronous submission

```d
// fragment: done is a TableCompletionHook; stepHost drives/wakes consumers
auto handle = parallelFor!update(farm, iota(n), token,
    Options(grain: 64), &done);
while (!handle.submitted)
{
    stepHost();
    handle.publish(token);
}
// Continue other work; done runs once after every callback returns.
// handle.complete acquires both the callback writes and done's return.
```

`parallelFor` uses one `malloc` allocation independent of element count. It
publishes until admitted or until a write returns zero. `publish` retries
the remaining suffix with the supplied token, without waiting for space or
re-entering a consumer. Partial admission never repeats accepted elements.

The move-only `ParallelHandle!R` is owned by the producer thread. Keep it
until `submitted`, or transfer it with an explicit move; destroying a handle
with unsubmitted work is fatal. Once submitted, dropping the handle is safe:
outstanding execution retains the allocation through the hook's return.
The hook is copied at construction, so its pointer need not stay alive, but
its context must. It may run on a consumer or on the publishing thread, even
before `parallelFor`/`publish` returns. Empty input completes immediately and
still invokes the hook exactly once.

Asynchronous range descriptors must be POD with alignment at most 8 and no
unshared mutable aliases. The function signature follows `antfarm.templates`'s
same argument rules. Immutable/shared references carry no ownership: retain
their storage, output storage, and the Farm through completion. A callback
starting nested asynchronous work must hand any incomplete submission to a
later nonblocking pump; it must not spin waiting for ring space.

## Ranges for caller-owned batches

```d
// fragment: callbacks and referenced storage remain alive through execution
auto st = parallelRange!update(iota(n));          // one entry per element
auto chunks = parallelRange!update(iota(n), 64);  // one entry per grain
auto mt = parallelSlices!update(farm, iota(n));   // snapshot live SqCs
auto explicitMt = parallelSlices!update(iota(n), 4);

auto accepted = farm.write(chunks, token, avgCostForChunk(8));
chunks.popFrontN(cast(size_t) accepted); // advance only the admitted prefix
```

The raw adapters are forward ranges of `PayloadEntry`. They compose with
other entries, for example through `std.range.chain`, into the caller's own
`write`. Their header/body buffers only need to outlive that write; referred
data must outlive execution. A raw range supplies no aggregate completion
notification. Supply an application join when mixing it into a batch;
`Tprogress` is not an MT join.

| Form | Shape and footprint |
| --- | --- |
| `parallelRange!fn(range)` | Existing argument packing, one ST entry per element. A one-word argument costs one ring word per entry in a uniform kind-2 table. |
| `parallelRange!fn(range, grain)` | Copies one subrange descriptor per ST entry. A normal integral `iota` descriptor occupies two words. The runtime grain, stride, and short tail are represented by the subrange itself. |
| `parallelSlices!fn(farm, range, slices)` | Balanced contiguous slices, each with `Done = min(512, slice.length)` and `MaxCs = 512`. Each callback handles a balanced block within the slice. Uses full headers and both indices; entry count is independent of N. |
| Convenience ST (`parallelFor`, wait, foreach) | Two words per grain: a stable region pointer and the starting index. |
| Convenience MT | Three body words per slice plus full headers/indices, with one completion RMW per block. |

Table headers, padding and shard counters add fixed overhead. The grain form
does not promise a universal one-word record: a generic runtime grain needs
tail/stride metadata or a stable context. No hidden global context or per-item
heap records are used to manufacture that footprint.

`Options.grain` is positive and applies to ST. `costClass` maps directly to
Farm `avgCost` (0..7), whose claim chunking applies on top of the grain.
`ParallelForm.automatic` chooses ST for standalone convenience calls. Batch
composition is explicit through `parallelSlices`; an arbitrary caller's
`write` cannot be inferred by the convenience API.

MT `slices = 0` snapshots `sqcsOf(max(1, farm.Cf))`. Explicit counts are 1..512,
clamped to element count to avoid empty payloads. Consumer changes and partial
table admission can change effective width after that snapshot. One MT entry
has roughly C/SqCs normal secondary entrants (about sqrt(C)), plus its primary
visitor and sweepers. The default slices give every shard an MT position when
they fit in one table and consumer count stays stable. `slices = 1` is an
explicit comparison option, not the standalone default.

## Completion and lifetime proof

Every region starts with a producer credit. ST reserves an additional credit
before each `writeTracked`; a zero write rolls it back. The existing acq_rel
shard and `Tprogress` chains join all callbacks in an admitted ST table. Its
hook then drops one region credit with acq_rel ordering.

MT reserves all block credits before any publication. Every issued block
executes its elements and drops one credit with acq_rel ordering as its final
region access. The core's relaxed `Pcount.completions` and primary table
accounting are not used as a join.

The producer advances the accepted suffix and relinquishes its credit only
after all input is submitted. This prevents early callback completion from
freeing or notifying a region while publication still accesses it. The final
acq_rel decrement imports every preceding contributor's writes, invokes the
copied hook once, then release-stores `finished`. An acquire observer joins
both the work and the hook. For stack regions that store is the final region
access; heap regions also release their execution reference, independently of
the handle's reference. Core callback epilogues touch only the pinned ring.

The focused checks are `tests/farm/run_parallel.py`; the performance grid is
[M1 in the benchmark guide](../../tools/perf/README.md#parallel-loop-m1).
