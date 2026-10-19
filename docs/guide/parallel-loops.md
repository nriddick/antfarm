# Parallel loops

Import `antfarm.parallel` for finite, sliceable random-access ranges such as
`iota`. Functions must accept one element by value and be `nothrow @nogc`;
their return values are ignored. These are single-shot Farm payloads, with no
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
without draining can deadlock all workers. Every build tracks this with
a thread-local flag and fatals before publishing, including for empty loops. Nested
work uses asynchronous publication and a hook, or a private actor tree with
its own structured join protocol.

The view must belong to the token's Farm. Other consumers can help, but the
caller helps complete the region. Parked consumers can hold reclamation pins,
so supply `Options.wake` and `wakeContext` to notify them after every accepted
write and on idle polls. The callback/context must outlive the region.
For a `FarmHost`, use `host.parallelFor!update(source, options)` on its control
thread (requires caller consumption), or pass `host.parallelOptions(options)`
to a low-level adapter. These loops do not pump actor attachments.

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
import antfarm.templates : payloadRange;
auto entries = payloadRange!update(iota(n));       // one entry per element
auto grains = payloadRange!update(iota(n), 64);    // one entry per grain
// parallelRange!update(range, grain) uses the same grain adapter.

auto accepted = farm.write(grains, token, avgCostForChunk(8));
grains.popFrontN(cast(size_t) accepted); // accepted grains, not source elements
```

These forward ranges compose with other entries, for example through
`std.range.chain`, into a caller-owned `write`. Their temporary body
buffers need only outlive that write; referenced data must outlive execution.
A raw range supplies no aggregate completion notification. Supply an
application join when mixing it into a batch.

Each raw range can attach a table hook directly:

```d
// fragment: done and its context stay stable through notification
// Reserve an aggregate table credit before this call if publishing a phase.
auto accepted = farm.write(grains, token, avgCostForChunk(8), &done);
grains.popFrontN(cast(size_t) accepted);
// Roll back the credit if accepted == 0; no hook runs for that call.
```

The finisher joins all callback writes in the accepted table before invoking
the hook. This also works for mixed-callback entries. Partial admission joins
only its accepted prefix; a phase spanning several tables needs aggregate
credits (as used by `parallelFor`). Notification may run before `write`
returns. A table hook does not imply that its own execution has returned or
that the Farm is globally drained.

| Form | Shape and footprint |
| --- | --- |
| `payloadRange!fn(range)` / `parallelRange!fn(range)` | Arguments packed at publication, one entry per element. A one-word argument costs one ring word per entry in a uniform kind-2 table. |
| `payloadRange!fn(range, grain)` / `parallelRange!fn(range, grain)` | One copied subrange descriptor per entry, read during execution. A normal integral iota descriptor occupies two words. The final grain handles the short tail. |
| `parallelFor`, wait, foreach | Two words per grain: a stable region pointer and starting index. |

The grain overload requires a finite, sliceable random-access range whose
descriptor is POD, aligned to at most eight bytes, without unshared mutable
aliases. This is stricter than packing individual values at publication.
`payloadRange` supports scalar arguments, tuple expansion and zero-argument
functions; `parallelRange` and region APIs take one element per function call.
`length`, `popFrontN` and accepted write counts are in grains. Empty input
publishes no payloads. Grain zero is fatal, including in release builds;
a grain larger than the input produces one short payload.

Table headers, padding and shard counters add fixed overhead. Descriptor size
depends on the range type; there is no per-grain allocation or hidden context.
Omitting the grain argument retains minimal per-element argument packing.

`Options.grain` is positive. `costClass` maps directly to Farm `avgCost`
(0..7), whose run claim chunking applies on top of the grain. For example,
grain 64 with a claim chunk of eight can assign 512 source elements in one
run. Tune both against workload cost and the desired distribution across
workers. Separate grains run concurrently. A useful starting point is enough elements
for a callback to take a few microseconds, then measure. Select `costClass`
for that per-grain cost: one region publishes one class.

## Completion and lifetime proof

Every region starts with a producer credit. It reserves an additional credit
before each hooked `write`; a zero write rolls it back. The existing acq_rel
shard and `Tprogress` chains join all callbacks in an admitted table. Its
hook then drops one region credit with acq_rel ordering.

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
