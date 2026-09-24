# Ant Farm: unordered work, actors, and D Fibers

Ant Farm is a D runtime for distributing work among producers and consumers
without requiring FIFO execution. It began as a fixed-memory payload ring and
now includes topology-aware workers, actors with explicit state lifetime,
phase-oriented actor waves, and suspendable tasks built on DRuntime Fibers.
Actor and Fiber activations use the Farm transport; applications can use only
the layers they need.

The strongest measured improvements have come from batching, avoiding repeated
backlog scans, and matching allocation and worker placement to the workload.
Scaling depends on the workload: some payload configurations benefit from
more producers and consumers, while tiny actor lifetimes and wave
operations often peak with fewer workers. Managed Fibers provide ordinary D
control flow at a measurable cost over bare payloads and direct Fiber calls.

| Layer or work representation | What it supplies |
| --- | --- |
| Bare payload | A short `nothrow @nogc` callback, with optional independent iterations |
| Autonomous actor | Stable identity, owned POD state, coalesced wakes, an intrusive inbox, and explicit retirement |
| Actor wave | One phase operation across a set of actors, with aggregate completion before a dependent phase |
| Managed Fiber | D exceptions and cleanup, suspension, signals, timers, joins, cooperative cancellation, and stack reuse |
| Threadpool | Persistent pinned workers, topology lookup, and application-controlled idle/wake policy |

The measurements below retain the earlier same-host queue comparisons and add
the later Fiber, wave, and lifecycle characterization. The raw transport logs
identify both an Intel i7-12700H and a Ryzen 5 5500; each series is identified
where used. Historical revisions, page modes, timing boundaries, and worker
placements matter. A payload/s, an actor invocation/s, and a complete actor
lifetime/s count different amounts of work. The throughput and lifecycle
figures also predate the later quota-protection and worker-wake audit fixes;
they are historical evidence for their named configurations, not fresh
measurements of the combined implementation.

## How the transport works

A Farm owns a fixed-size ring, virtually mapped twice so a table can cross the
physical end without a split copy. Producers register single-owner tokens in
bulk or small tiers and receive quotas measured in 64-bit words. Their combined
quotas bound simultaneous reservation excursion. Consumers hold references to
segments as they progress; completed, unreferenced segments can be reused.
An incomplete segment retains a protection mark even after its last active
consumer leaves. The producer therefore cannot mistake temporary inactivity
for permission to overwrite unfinished work.

Producer registration and deregistration now use a Farm-local mutex; publishing
and consuming do not acquire it. Tokens start with zero quota. Each grant
requires an acquire probe of the write tail and a forward-segment sweep; writes
spend the finite grant without automatically replenishing it. This closes an
exact-boundary runoff defect in the earlier quota-renewal policy. Every build
also checks newly entered segments for active roots or incomplete-work pulses
before changing their metadata or payloads. That check is a diagnostic tripwire,
not an atomic reservation protocol; the sweep remains the admission mechanism.

A publication consists of a table header, indexes, padded shard counters, and
payloads. Each payload has a 128-byte header and a body of packed words.
`MaxCs = 1, Done = 1` describes a single-shot callback; multiconsumer payloads
expose numbered iterations with admission and completion accounting, bounded
by the implementation's 512 caps. Tables may contain different callbacks and
body lengths. Ring transport itself requires no per-payload heap allocation.

Consumers claim chunks from approximately square-root-many shards rather than
arbitrating on one global per-job sequence. `avgCost` selects a chunk size from
32 down to one. A first claimant can move ahead when later tables are available;
completers sweep unfinished shards and multiconsumer work. Idle re-walks retain
a cursor so they visit newly encountered tables instead of repeatedly scanning
the whole segment.

Synchronization is still present. Publication reserves ring space, chunk claims
update shared counters, and segment reuse has CAS retry paths. Actor ownership
adds its own atomic transitions and short gates. The current single-shot
payload path reuses the shard's unique ownership and records its payload claim
with an atomic store, eliminating a redundant fetch-add. The project does not
claim that the whole stack is contention-free or wait-free.

The transport API is `@nogc nothrow @system`. Callbacks receive read-only ring
body words; manually encoded identities can still refer to mutable external
objects with a separate ownership contract. Generated shims reject unshared
mutable aliases. Common-header and fixed-width write forms avoid repeated
validation or sizing passes, and generic sources must be forward ranges so
sizing and emission can use independent checkpoints.

`write()` returns the accepted prefix; the caller advances its source and
retries the remainder. Zero means backpressure. A false `consumeNext()` can
mean a publication hole rather than global emptiness. Neither call supplies a
FIFO completion guarantee. The detailed contract is in [SPEC.md](SPEC.md).

## Payload throughput and the existing queue comparisons

The earlier Intel i7-12700H grid used a 16 MiB Farm, eight segments, one-word
bodies, and batches of 256. Small-producer and consumer counts each ranged from
one through fourteen, giving 196 configurations:

| Configuration or summary | Million payloads/s |
| --- | ---: |
| 4 producers / 4 consumers | 289 |
| 6 producers / 6 consumers | 316 |
| 8 producers / 8 consumers | 341 |
| Best: 9 producers / 13 consumers | 397 |
| Median / mean across the grid | 282 / 255 |
| One producer, varying consumers | About 66–101 |
| Single-item publication, grid median / peak | 10.26 / 21.51 |

The grid median is a summary across different topologies, not a repeated-run
confidence estimate. It shows a substantial batching benefit and several
configurations that use additional producers and consumers effectively. It
does not establish monotonic scaling or a preferred worker count for other
workloads. The raw runs are in [throughput.txt](throughput.txt).

The existing same-host `moodycamel::ConcurrentQueue` measurements used
`uint64_t` items and a bounded-style matrix:

| Queue benchmark mode | Typical or peak result |
| --- | --- |
| No tokens, bulk | About 110–180 M/s in the middle of the matrix; 187 M/s peak at 1P→1C |
| No tokens, bulk, 6×6 | 115 M/s |
| With tokens, bulk | About 1.07 billion/s at 8–10 paired threads |
| With tokens, bulk, 6×6 | 807 M/s |

Ant Farm's 316 M payloads/s at 6×6 lies between those two measured queue modes.
The comparison is useful context for transport overhead, but the operations
are different: an Ant Farm payload carries callback metadata, sharding, and
iteration/completion semantics. The token and no-token results also show how
strongly the queue benchmark depends on its usage mode. They do not support a
blanket claim that either implementation is faster for arbitrary applications.

## Uniform publication and workers that also produce

The later Ryzen 5 5500 logs include common-header/fixed-width publication,
which sizes a homogeneous batch arithmetically instead of inspecting every
body before emission. These grids used a 16 MiB Farm, one-word bodies,
`avgCost=0`, huge pages requested, and three reported repeats per configuration.
Producer and consumer counts each ranged from one through ten:

| Publication path, batch 256 | Mean across the grid, M payloads/s | Peak, M payloads/s |
| --- | ---: | ---: |
| General `PayloadEntry` path | 209.262 | 333.877 at 6 producers / 5 consumers |
| Common header and fixed body width | 234.413 | 340.263 at 3 producers / 4 consumers |

The specialized grid has a higher mean, while the peaks are closer and occur
at different topologies. These logged sweeps characterize the two paths;
they do not establish a uniform percentage improvement. With the fixed-width
path, batch 63 peaked at 320.810 M/s, batch 32 at 266.312 M/s, and batch one
at 20.031 M/s.

Workers can hold both a producer token and a consumer view. A worker that
creates more work can publish it, consume, and retry on backpressure without
routing through a dedicated producer thread. The logged dual-role sweep used
one to eight such threads, fixed-width one-word payloads, and batches of 256.
It reached 305.064 M payloads/s at six threads and 276.505 M/s at eight.
Changing batch size to 32 reduced the peak to 162.357 M/s; single-item writes
peaked at 12.579 M/s with one thread. This exercises the shared producer/worker
role and shows its dependence on batching. The grids are retained in
[throughput.txt](throughput.txt).

## What overtaking does for latency

The earlier six-consumer Intel comparison measured a one-job sentinel arriving
behind an existing dump, with 1 µs of simulated work per callback. Ant Farm's
`tail` benchmark measures from immediately before `write()` to the first
instruction of the sentinel callback, including admission retries. The
same-host queue runs used sentinel round-trip timing after a pre-placed chunk.
The historical Intel harness assigned consumers to LPs 0–5: three physical
P-cores with SMT on that machine. It also admitted missed-window samples into
its histograms. The current harness discovers physical cores, prints placement,
and excludes those missed attempts. The numbers below are retained historical
measurements; they have not been regenerated with that corrected harness.
See [the harness notes](perftest/README.md#tail-latency-harness).

| Existing dump | Ant Farm p50 / p99 | moodycamel p50 / p99 | TBB p50 / p99 |
| --- | ---: | ---: | ---: |
| Idle | 0.50 / 3.3 µs | 0.70 / 1.10 µs | 0.70 / 1.10 µs |
| 256 jobs | 5.0 / 21.4 µs | 48 / 56 µs | 47 / 61 µs |
| 2,048 jobs | 25.2 / 27.9 µs | 378 / 419 µs | 379 / 416 µs |
| 8,192 jobs | 13.2 / 27.6 µs | 1.52 / 1.57 ms | 1.52 / 1.60 ms |

The recorded Boost.Lockfree result at 8,192 jobs was 1.61 ms p50. In these
occupied cases, Ant Farm's ability to overtake unfinished tables kept sentinel
p99 around 21–28 µs while the queue workloads drained their backlog first.
Idle p50 favored Ant Farm, but idle p99 favored the queues.

That is evidence for useful mid-drain overtaking, not a latency bound.
At 8,192 jobs, the same Ant Farm run recorded 129.3 µs p99.9 and a 1.23 ms
maximum. Twelve consumers raised p99 to 308.2 µs; one consumer gave roughly
6 ms p50. The near-full case also reached 1.23 ms p99. A finite ring can stop
admitting work, and additional consumers can make the tail worse.

Chunk sizing changes the result materially. On the Ryzen host, five pinned
consumers with huge pages and an 8,192-job dump gave 24.8 µs p99 at chunk 16,
15.8 µs at chunk 8, and 7.9 µs at chunk 4. Those samples support tuning the cost
hint to the actual callbacks; they do not show that smaller chunks are free.
The complete distributions and conditions are in [latency.txt](latency.txt).

## Persistent workers and ordinary D control flow

The threadpool discovers cores, SMT siblings, LLCs, NUMA nodes, and available
processor efficiency classes. It pins persistent workers and locates
application-owned state by topology. Worker bodies choose what to pump. A
single Director owner controls spin, wait, sleep, and cadence policies;
ordinary producers can notify workers through `wakeAll()`. The audited wait
protocol arms a worker's wait slot, rechecks work and stop state, and only then
permits parking. Notification exchanges the slot back to active, including
when the worker was already active; managed workers use the recheck's deadline.
Forced wait-race tests cover publication, stop, deadline, and exception edges.

Managed Fibers use these workers while retaining DRuntime's Fiber backend.
A runnable activation becomes one serial Farm payload with a shared callback
header and one-word body. The scheduler supplies generation tracking, waits,
timers, joins, cancellation, completion records, GC rooting, and recycling.
D exceptions and `scope(exit)` remain available. Cancellation is cooperative;
user code that never reaches a scheduler boundary can delay shutdown. Fibers
may resume on another eligible worker, so TLS-derived state cannot be retained
across suspension as though it were Fiber-local.

Earlier Ryzen characterization illustrates the cost of those facilities:

| Same-thread path | Representative rate |
| --- | ---: |
| DRuntime warm reset and run | About 7.3 M/s |
| One DRuntime Fiber repeatedly reset and run | About 27 M/s |
| Managed Fiber warm run | About 4.1 M/s |
| Managed Fiber with one yield | About 1.8 M Fibers/s |
| Bare payload run | About 55 M/s |
| Cold Fiber creation, either Fiber path | About 150–175 k/s |

These are different execution patterns, particularly the single recycled
Fiber versus many live stacks. They establish why short, non-suspending work
belongs on the payload path, while Fibers are useful for code that benefits
from suspension and ordinary D semantics. Cold stack creation is expensive;
recycling removes much of that cost. Reserving task metadata does not pre-map
DRuntime stacks.

A separate two-million-payload embedding test on six physical Ryzen cores
measured 54.5 M/s with direct pinned consumers, 54.9 M/s through the spinning
threadpool pump, and 51.0 M/s through managed Fiber worker hooks. Parking and
waking the bare pool gave 47.2 M/s. Worker integration therefore had modest
cost in that configuration, but the idle policy was consequential. These are
throughput results, not parked-worker wake-latency guarantees.

After warm-up, ready/completion publication, established waits, timer expiry,
and stack reuse avoid allocation. Cold tasks, growing containers, new wait
keys, returned result arrays, exceptions, and user code can still allocate.
The broader same-thread, worker-count, embedding, and allocation evidence is
in [fibers/PERFORMANCE.md](fibers/PERFORMANCE.md).

## Actors, inboxes, and dependent waves

An actor runtime owns a fixed-capacity array of stable control slots and
separately allocated POD state. A move-only `ActorOwner` controls retirement and
reclamation; a copyable generation-tagged handle supplies identity without
keeping that state alive. A callback receives an exclusive scoped borrow.
Wakes coalesce, and the intrusive inbox admits messages through a per-generation
submission gate. Retirement closes admission, lets accepted work finish, and
permits reclamation only after the remaining uses have drained. Inbox nodes
are explicitly completed before reuse. Actor callbacks cannot suspend; Fibers
can coordinate the waits around them.

Actors use non-GC storage. The stable slots remain until runtime destruction;
state allocations have shorter lifetimes. The API remains `@system`, and raw
pointer escapes can violate its ownership rules. Non-POD destruction, automatic
GC retention of referenced objects, and region-wide retirement are not supplied
by the current actor implementation.

A wave reserves a phase operation across a set of actors, publishes the accepted
prefix into one or more Farm tables, and seals one aggregate completion.
Membership rejects duplicate or overlapping unfinished waves. Successful
completion is the visibility boundary for a dependent phase. Actors within a
wave remain parallel: an application needing cross-actor reads can maintain
private mutable state and double-buffered public projections, selecting the
new public generation only after completion.

A Fiber can orchestrate those phases through counted generation triggers.
Each completed table advances wave progress. When the sealed wave finishes,
its hook advances a generation trigger and publishes a preallocated deferred
notification; managed worker code performs the wake after payload dispatch.
This replaced completion polling while keeping the completion callback
`nothrow @nogc`. Publication backpressure can still require cooperative yields.

The type-erased actor interface also permits an engine-owned registry of module
generations. Tests exercise an aggregate unload fence, including callbacks,
senders, and message/resource destruction that can outlive actor reclamation.
That is a tested ownership model, not an implemented dynamic loader or automatic
hot-reload system. The actor API is still an evaluation surface; its remaining
contracts are tracked in [ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md).

## What made actor waves faster

The early wave path repeatedly reserved the entire remaining actor slice and
cancelled the part the Farm could not accept. Splitting a large wave into small
tables therefore repeated work over a shrinking tail. Reserving only the exact
accepted prefix removed that quadratic behavior. Additional changes eliminated
ownership operations already supplied by the wave reservation.

On the Ryzen 5 5500, six physical workers, ordinary pages, LDC release builds,
32,768 actors per set, 200 measured generations, and three-run medians,
dispatch cost changed as follows:

| Wave implementation | One orchestration Fiber, ns/actor | Two independent orchestration Fibers, ns/actor |
| --- | ---: | ---: |
| Initial path | 286.50 | 211.63 |
| Remove redundant running-state RMW | 268.98 | 193.78 |
| Reserve only the accepted prefix | 40.99 | 36.94 |
| Remove redundant per-member submission pins | 39.51 | 36.38 |

Those are 7.25× and 5.82× improvements for this dispatch workload. The large
change came from avoiding repeated reservation/cancellation, not from changing
Fiber implementation. Other tested changes, including packed wave status and
a removed admission load, did not survive repeated sampling and were reverted.

Later trigger-based measurements reached about 36.25 and 29.22 ns/actor in
the one- and two-Fiber workflows. Ring footprint mattered too: a separate
three-run comparison reduced two-Fiber cost from 36.54 to 29.90 ns/actor by
using an 8 MiB rather than 16 MiB Farm. These are separate experiments;
their gains should not be multiplied together.

Worker scaling also had a clear limit. In one ordered prefix sweep, two
orchestration Fibers reached 30.96 M actor invocations/s at four physical
workers, 30.87 M/s at six, and 23.37 M/s with all twelve SMT workers. One
orchestration Fiber peaked near 24.56 M/s at two workers. The callbacks only
performed identity projections through actor cache lines; substantial behavior
could move that peak. The full protocols are in
[the wave characterization](fibers/PERFORMANCE.md#actor-wave-optimization-checkpoint).

## Removing repeated bookkeeping scans

The next optimization pass targeted actor creation, autonomous ready queues,
and Fiber lifecycle reporting. All three had costs that increased with backlog
size even when each callback did almost nothing:

- Actor creation restarted a vacant-slot search for each new actor. A gated
  intrusive free list now supplies constant-size slot operations, with allocator
  calls outside the gate.
- Each bounded actor flush detached the whole ready backlog and requeued its
  unused suffix. The runtime now retains that suffix and removes only the next
  batch, reducing a complete drain from approximately `O(N²/B)` to `O(N)`
  queue work for `N` actors and batch size `B`.
- Fiber lifecycle delivery recounted the retained suffix on every bounded
  drain. Its count now travels with the suffix, including handler-failure
  retries. Total traversal is linear; the initial detached-list reversal still
  visits the whole batch once.

Selected Ryzen/LDC `-O2 -release`, ordinary-page measurements, using matched
benchmark sources and three-run medians:

| Isolated operation | Before | After | Ratio |
| --- | ---: | ---: | ---: |
| Ready dispatch, 16,384 actors, batch 32 | 1,873.62 ns/actor | 23.50 ns/actor | 79.73× |
| Initial creation, 32,768 actors | 581.081 ms | 2.489 ms | 233.46× |
| Lifecycle handler, 16,384 records, batch 32 | 20.200 ms | 1.047 ms | 19.29× |
| Lifecycle array drain, same count/batch | 20.332 ms | 1.452 ms | 14.00× |

These ratios isolate the old scans. They are not application-wide speedups,
and the free-list gate does not guarantee contention-free concurrent creation.
Revision pairs, timing exclusions, and reproducing commands are recorded in
[perftest/README.md](perftest/README.md).

The unchanged synthetic workloads provide a useful check on that distinction.
Comparing `8e988f6` with `e6ce4d1`, payload throughput stayed around 83 M/s with
one consumer and 87–88 M/s with six; those payload executables were byte-identical.
Steady actor-wave medians moved from 26.190 to 26.828 M/s for one orchestration
Fiber and 31.977 to 32.170 M/s for two, with overlapping sample ranges.
Whole wave-benchmark process time fell from 4.742 to 2.355 seconds, consistent
with faster actor setup outside the steady dispatch timing.

There was also a regression: same-thread warm Fiber execution fell from 4.555
to 4.296 M/s. Seven additional pinned pairs reproduced about a 5% decline,
while warm creation and combined creation/execution improved. Its cause remains
unresolved. Twelve-worker warm spawn-through-completion changed from 1.730 to
1.759 M Fibers/s; higher drain-only figures exclude concurrent spawning time
and should not be presented as full lifecycle rates. See
[THROUGHPUT.md](perftest/THROUGHPUT.md) for the complete comparison.

## Complete actor lifetimes: the current optimization target

The sustained churn benchmark repeatedly creates a cohort, dispatches each
actor once, waits for completion, retires it, and reclaims its state. Every
cycle checks the exact callback count and generation, plus zero live, ready,
or stale actors. Farm storage, control slots, metadata arrays, and consumer
threads survive across cycles; their setup and shutdown are excluded. The
callback updates a private completion line. The benchmark isolates cohort
lifetimes with minimal work; it does not model substantial actor behavior or
concurrent creators and reclaimers.

This is a stricter timing boundary than dispatch-only throughput. In the first
matched comparison, using the C-runtime allocator, batch 256, one thread, and
three-run medians:

| Cohort and mode | Original `8e988f6`, M lifetimes/s | Optimized `e6ce4d1`, M lifetimes/s |
| --- | ---: | ---: |
| 4,096 autonomous actors | 0.777 | 9.018 |
| 4,096 actors in a wave | 0.811 | 9.809 |
| 16,384 autonomous actors | 0.115 | 8.003 |
| 16,384 actors in a wave | 0.117 | 8.670 |

The large-cohort gains mainly reflect eliminating the repeated slot search.
With those scans gone, allocation and smaller synchronization costs become
visible. The subsequent retirement change joins the closed submission gate,
rechecks the lifecycle, and uses a release store when the retiring owner is the
unique remaining writer. Matching mimalloc runs improved same-thread complete
lifetimes by 3.47% for autonomous actors and 4.39% for waves. Reusing the Farm's
single-shot ownership then improved wave lifetimes by another measured 3.24%
in its own matched comparison; the autonomous change was smaller and sample
ranges overlapped.

The same review fixed a wave-membership race: the old wave must finish clearing
its intrusive link before releasing the membership pin, because the actor can
immediately enter another wave or become reclaimable. A forced-interleaving
regression test exercises that reuse. This is a correctness fix, not an assigned
throughput gain.

## Mimalloc, batching, and placement

Actor state in the churn benchmark is allocated and freed on every cycle.
It is not repeatedly allocated with D `new`; D allocation supplies persistent
benchmark setup. The existing C-runtime default uses 64-byte-aligned, rounded
allocations. The optional pinned mimalloc v3.5.0 adapter uses the requested
size and alignment, with matching sized/aligned frees. The benchmark state is
16 bytes with 8-byte alignment. Mimalloc is linked with process-wide allocator
override disabled.

The later `cf768cf` measurements used the Ryzen 5 5500, LDC 1.43.0 / LLVM
22.1.8, `-O2 -release`, ordinary pages, 16,384 actors, batch 256, five warm-ups,
and medians of five runs of at least two measured seconds per configuration:

| Execution | Mode | C runtime, M lifetimes/s | Mimalloc, M lifetimes/s |
| --- | --- | ---: | ---: |
| Controller also consumes, CPU 2 | Autonomous | 8.426 | 18.247 |
| Controller also consumes, CPU 2 | Wave | 9.180 | 22.346 |
| Controller CPU 6; consumers 0–5 | Autonomous | 5.959 | 14.992 |
| Controller CPU 6; consumers 0–5 | Wave | 6.013 | 15.450 |

These compare the project's two allocation policies. They do not establish a
universal advantage over ordinary `malloc(16)`, because the default C policy
also requests stronger alignment. An earlier matching-64-byte mimalloc control
still improved lifetime throughput by 2.13× for autonomous actors and 2.34× for
waves. All creation and freeing in these timing runs occur on the controller;
foreign-thread reclamation is tested separately for correctness.

Offering an entire wave cohort at once raised same-thread mimalloc throughput
from 22.346 to 22.713 M lifetimes/s. A separate three-run placement measurement,
with controller CPU 6 and one consumer on CPU 1, a different physical core, reached
20.643 M autonomous lifetimes/s and 25.192 M wave lifetimes/s. Autonomous
publication used batch 256; wave publication offered all 16,384 actors.

Six continuously polling consumers did less well for these tiny callbacks.
The six-consumer placement also shares the controller's physical core with
one consumer, so worker count and topology are coupled in that comparison.
Creation and reclamation remain serial. This is why the earlier payload grid's
scaling result should not become a general recommendation to add workers.

The retained benchmark supports the C-runtime baseline, mimalloc, and a
matching-alignment mimalloc diagnostic. Experimental arena, pool, and other
malloc policies were removed. Historical measurements precede that harness
cleanup; they are reference results, not fresh timings of the cleaned binary.
Long-running memory retention, fragmentation, and application workloads remain
separate questions. See [MIMALLOC.md](perftest/MIMALLOC.md) and
[LIFECYCLE.md](perftest/LIFECYCLE.md).

## Memory and topology are workload choices

Ordinary 4 KiB Farm backing is the default. Huge pages remain an explicit option
because long payload walks can benefit while Fiber scheduling does not
necessarily do so. In the verified Ryzen page comparison, three-run medians
for 500,000 Fibers gave:

| Fiber drain | 4 KiB pages | Huge pages |
| --- | ---: | ---: |
| Single-thread recycled | 4.39 M/s | 4.37 M/s |
| Twelve-worker first drain | 8.85 M/s | 7.66 M/s |
| Twelve-worker recycled | 8.30 M/s | 8.10 M/s |

Live mapping inspection confirmed actual Linux huge-page promotion in that
experiment. Requesting `MADV_HUGEPAGE` alone is not proof of promotion, and the
Farm's page choice does not change Fiber stack mappings. Windows uses the
large-page mapping path and requires the corresponding privilege.

The pool and Fiber domain model support LLC-local lanes and covering workers,
and topology parsing has captured and synthetic tests. Current Ryzen
characterization has one LLC. Multi-LLC/NUMA throughput and worker-group
failover still need hardware validation. There is also no ring size that makes
a bounded Farm unconditionally immune to backpressure: cache footprint and
producer slack must be measured together.

## Correctness evidence and remaining scope

Before merging the host-audit changes, the September 24 integration checks
passed root unit and integration tests, actor torture, Farm torture, and Fiber
unit/smoke suites with LDC and DMD.
Actor and Farm ThreadSanitizer lanes, real mimalloc full-debug checks, LDC
release Fiber stress, and 42 churn smoke/argument checks also passed. The root
and Fiber test executables now request `testmode=run-main`, so their selected
main suites also execute after imported module unittests. Explicit smoke
commands are listed in the [Fiber test instructions](fibers/README.md#build-and-test).

After merging the host-audit changes, Linux checks passed root, actor, Farm,
Fiber, and threadpool suites under both compilers, including the armed-wait
race tests. Actor and Farm TSan, mimalloc debug, LDC release Fiber stress,
fresh-segment protection tests, 800 concurrent Farm creations, the bounded
quota model with its expected negative control, and 12 actor/wave lifecycle
smoke runs also passed. These checks validate the integration; the short
smoke runs do not replace the throughput measurements above.

Coverage includes ring reuse and partial publication, exact ST/MT execution,
concurrent creation and reclamation, late accepted sends during retirement,
ready-backlog handoff under backpressure, immediate wave-member reuse, dependent
phase visibility, lifecycle handler retries, and migration/cancellation/GC
retention for Fibers. The mimalloc suite additionally frees 8,192 actor states
from threads other than their allocation threads.

Migrating DRuntime Fibers have a known sanitizer/runtime boundary, so Farm and
actor TSan success is not evidence that stack-switching Fiber execution is
TSan-verified. Earlier Windows x64 DMD/LDC integration and stress runs are
recorded in [ROADMAP.md](ROADMAP.md); they should not be mistaken for a Windows
rerun of every subsequent optimization. The separate Windows host audit in
`f63b6ab` recorded one broader Fiber stress stall despite passing isolated
reruns; that observation remains unresolved.

The quota audit adds exact-boundary and spanning-table protection tests,
forced probe/sweep/reservation delays, provisional subscription races, and
negative controls which deliberately plant live roots or pulses. A bounded
sequentially consistent model explores quota interleavings but does not model
the full weak-memory implementation. The unconditional tripwire was retained
after four-pair Windows release measurements across ten raw/dual throughput
cases showed median changes from -1.57% to +2.08%, with no repeated busy-workload
p99 regression in six focused latency pairs. Those results measure the
tripwire's cost, not the combined optimization branch's throughput. Details
are in [the consumer-protection audit](review_torture/README.md#consumer-protection-audit-allwritecheck).

The supported use case is bounded, unordered work with explicit completion and
lifetime ownership. An engine can mix short payloads, persistent actors,
phase-oriented waves, and Fibers that coordinate waits on the same workers.
Ordered streams, unbounded backlogs, hard latency guarantees, and arbitrary
preemption require additional contracts. Non-POD actor lifetime, adopted state,
retirement groups, integrated actor wake targeting, and multi-LLC validation
remain open work. DRuntime Fibers remain the implemented control-flow model.

For onboarding, use [README.md](README.md) and the compiled examples. For the
layer relationships and shutdown order, use [ARCHITECTURE.md](ARCHITECTURE.md).
The detailed evidence lives in [the performance benchmarks](perftest/README.md),
[the Fiber report](fibers/PERFORMANCE.md),
[the actor ordering contract](actors/ACTOR_MEMORYORDER.md), and the
[Farm](review_torture/README.md) and [actor](actor_torture/README.md) torture suites.
