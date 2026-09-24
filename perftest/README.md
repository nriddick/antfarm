# Performance characterization

For the original checkout versus optimized branch on the existing payload,
Fiber, and actor-wave benchmarks, see [THROUGHPUT.md](THROUGHPUT.md).

## Sustained actor and wave churn

`actor_churn.d` repeatedly creates a full cohort of actors, dispatches each
exactly once, requests retirement, and reclaims every actor before the next
cycle. Actor state is allocated and passed back to its allocator on every
cycle. The Farm, runtime, owner/handle arrays, completion counters, wave
descriptor, and consumer threads persist across cycles, so stable actor slots and wave generations
are reused. There are no Fibers in this benchmark: it isolates the actor
and wave lifecycle rather than Fiber orchestration.

```sh
make -C perftest actor_churn
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn actor 4096 3 0 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn wave 4096 3 0 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn actor 4096 3 6 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn wave 4096 3 6 256 5
```

Arguments are mode (`actor` or `wave`), actors per cohort, minimum measured
seconds, background consumers, publication batch, warm-up cycles,
and optional allocator (`crt`, `mimalloc`, or `mimalloc64`). Autonomous
publication batches are 1–256; wave publication accepts any positive batch,
with the Farm splitting larger offered slices into physical tables.
An optional final comma-separated CPU list pins the controller followed by
each consumer; omit it or pass `-` to leave placement to the OS.
These examples also show the defaults except that `actor`/`wave` is required.
Zero background consumers means the controlling thread also consumes;
otherwise there is one controlling producer plus the requested number of
native consumer threads, continuously polling. No threads are
created or joined inside the timed loop.

The primary rate, `Mactor_cycles/s`, counts **complete actor lifetimes** in
millions per second, including creation, dispatch, completion waiting,
retirement, allocator reclamation, and verification. In wave mode,
`cycles/s` also gives completed cohort waves per second. Every cycle ends with an exact callback
count/generation check and zero live, ready, or stale actors. The benchmark
also reports aggregate creation, dispatch, and retirement/reclamation/check
times for the measured cycles. Warm-up, runtime setup, and final thread
shutdown are excluded; the final measured cycle always completes, even if
the requested duration has elapsed.

Each actor has 16 bytes of state on x86-64. Its callback increments a private
completion counter and release-publishes the cycle number on a separate
64-byte completion line. There is no contended global callback counter.
Autonomous mode wakes the whole cohort, flushes batches, and waits for all
callbacks. Wave mode starts one wave per cohort, publishes batches, seals
the wave, and waits for aggregate completion and membership release. Both
then retire and reclaim all actors. This is a minimal-work, cohort-based
allocation/reuse test; it does not model staggered lifetimes, simultaneous
creators/reclaimers, or substantial application work.

The Farm uses an 8 MiB ring, eight segments, one small producer, a
16,384-word quota, and `avgCost=0`. Builds default to LDC `-O2 -release`.
Correctness checks use `enforce` and remain enabled in release builds.

The normal build uses the C-runtime allocator: on Linux, each actor creation
calls `aligned_alloc(64, roundedSize)` and reclamation calls `free`. D `new`
is used only for persistent setup outside the timed loop. The optional
`actor_churn_mimalloc` target links the pinned real mimalloc v3.5.0 library
and defaults to the existing `actors.mimalloc` adapter. It can also select
`crt` at runtime for a comparison within the same executable. See
[MIMALLOC.md](MIMALLOC.md) for commands, alignment controls, and results.
For the subsequent runtime improvements, larger wave offers, and consumer
placement measurements, see [LIFECYCLE.md](LIFECYCLE.md).

### Sustained comparison

Measured on 2026-09-24 on the Ryzen 5 5500 host described below, with LDC
1.43.0 / LLVM 22.1.8, `-O2 -release`, and `ANTFARM_HUGE_PAGES=0`. The same
`actor_churn.d` source was compiled against original checkout `8e988f6` and
optimized runtime `e6ce4d1`. Each value is the median of three serial runs
per revision, alternating revision order. Every run uses five warm-up
cycles, at least three measured seconds, and publication batch 256.

| Actors/cycle | Background consumers | Mode | Original million lifetimes/s | Optimized million lifetimes/s | Speedup |
| ---: | ---: | --- | ---: | ---: | ---: |
| 4,096 | 0 | Autonomous | 0.777 | 9.018 | 11.60× |
| 4,096 | 0 | Wave | 0.811 | 9.809 | 12.10× |
| 4,096 | 6 | Autonomous | 0.611 | 6.366 | 10.43× |
| 4,096 | 6 | Wave | 0.626 | 7.931 | 12.68× |
| 16,384 | 0 | Autonomous | 0.115 | 8.003 | 69.69× |
| 16,384 | 0 | Wave | 0.117 | 8.670 | 73.86× |
| 16,384 | 6 | Autonomous | 0.108 | 7.942 | 73.85× |
| 16,384 | 6 | Wave | 0.109 | 8.243 | 75.75× |

At 16,384 actors on one thread, autonomous-mode phase costs were:

| Phase | Original ns/actor | Optimized ns/actor |
| --- | ---: | ---: |
| Creation | 8,408.86 | 61.94 |
| Wake, publish, execute, await completion | 265.23 | 33.16 |
| Retire, reclaim, verify | 33.69 | 29.63 |

Creation no longer repeats the quadratic slot search on every refill.
The autonomous dispatch phase also benefits from bounded ready snapshots.
Wave mode bypasses that ready backlog for its work; its primary improvement
here is actor creation. These are gains across repeated reuse, rather than
just first-time initialization.

The tiny callback and single creating/reclaiming thread do not benefit from
adding six consumers. Those un-pinned runs were also noisier: optimized
wave throughput at 16,384 actors ranged from 5.974 to 8.317 million lifetimes/s
across the three samples, versus 8.645–8.695 on one thread. The median is not
a latency guarantee or a claim about more substantial parallel actor work.

All 48 timed comparison runs passed the per-cycle checks. Additional smoke
runs passed with LDC debug, LDC release, and DMD release builds, including
single-actor batches, non-divisible 257-actor cohorts, and six consumers.

## Actor ready backlog

`actor_ready.d` measures autonomous actor dispatch from a prequeued burst.
One thread flushes batches and consumes the resulting Farm tables. Every
callback increments its own actor's counter; the program checks exact counts
and an empty ready queue after every round. Actor construction, waking,
counter verification, and reclamation are outside the timed region.

```sh
make -C perftest actor_ready
ANTFARM_HUGE_PAGES=0 ./perftest/actor_ready 4096 200 32
ANTFARM_HUGE_PAGES=0 ./perftest/actor_ready 16384 100 32
```

Arguments are actor count, measured rounds, and flush batch (1–256). Five
warm-up rounds precede measurement. The Farm uses an 8 MiB ring, eight
segments, one consumer, one small producer with an 8,192-word quota, and
`avgCost=0`. The default build is LDC `-O2 -release`.

## Bounded snapshot comparison

Measured on 2026-09-24 on a Ryzen 5 5500, Linux x86-64, LDC 1.43.0 / LLVM
22.1.8, with `ANTFARM_HUGE_PAGES=0`. Each entry is the median of three runs,
alternating the old and new binaries. Both binaries use this benchmark source;
the baseline actor implementation is commit `8e988f6`. Batch size is 32.

| Actors | Measured rounds | Before ns/actor | After ns/actor | Speedup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 200,000 | 170.69 | 151.24 | 1.13× |
| 32 | 20,000 | 27.86 | 27.25 | 1.02× |
| 256 | 4,000 | 48.80 | 25.35 | 1.93× |
| 4,096 | 200 | 485.91 | 27.79 | 17.49× |
| 16,384 | 100 | 1,873.62 | 23.50 | 79.73× |

Previously, every flush detached the whole ready list, selected its batch,
then traversed and individually requeued the remaining actors. Draining `N`
actors in batches of `B` incurred approximately `O(N²/B)` queue work. The
bounded snapshot keeps the untouched chain in the runtime, protected by a
short acquire/release drain gate, and removes at most `B` nodes per call.
A complete burst now requires `O(N)` queue work. A contending flusher returns
zero and retries; the gate is released before Farm publication. Selected
actors rejected by Farm backpressure are still requeued.

These are synthetic burst-drain measurements on one thread, not concurrent
worker scaling or application throughput guarantees. Small bursts include
proportionally more timing and Farm-table overhead. Compare the same actor
count, round count, batch, compiler flags, and page mode on both revisions.

The actor torture suite checks the corresponding ownership behavior under a
forced drain handoff, concurrent wakes, partial publication, repeated full-Farm
retries, four flushers, and two consumers. See
[actor_torture/README.md](../actor_torture/README.md) and the
[ready-queue ordering contract](../actors/ACTOR_MEMORYORDER.md#ready-queue-ownership).

## Actor creation and Fiber lifecycle drains

`scaling.d` isolates two bookkeeping costs without changing the DRuntime Fiber
backend or its exception, suspension, and cleanup behavior:

```sh
make -C perftest scaling
ANTFARM_HUGE_PAGES=0 ./perftest/scaling create 32768
ANTFARM_HUGE_PAGES=0 ./perftest/scaling lifecycle 16384 32 handle
ANTFARM_HUGE_PAGES=0 ./perftest/scaling lifecycle 16384 32 take
```

The creation mode times actor slot reservation and C-runtime state allocation
for an initially empty runtime. Farm/runtime setup, owner-array allocation,
and retirement/reclamation are outside the timed interval. Lifecycle modes
precreate Fibers with 4 KiB stacks, then time only draining their admitted
records. `handle` invokes a counting delegate; `take` also allocates the arrays
returned by `takeLifecycleEvents`. Fiber creation, execution, terminal-record
draining, and recycling are outside that interval.

The following results were collected on the same host and compiler described
above, with ordinary pages and LDC `-O2 -release`. Both binaries use the same
`scaling.d` source; the baseline is `1615d31`. Values are median milliseconds
from three alternating runs of each binary. Lifecycle batch size is 32.

| Operation | Count | Before ms | After ms | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Actor creation | 8,192 | 25.677 | 0.599 | 42.87× |
| Actor creation | 16,384 | 140.206 | 1.202 | 116.64× |
| Actor creation | 32,768 | 581.081 | 2.489 | 233.46× |
| Lifecycle handler | 4,096 | 1.116 | 0.251 | 4.45× |
| Lifecycle handler | 8,192 | 4.907 | 0.505 | 9.72× |
| Lifecycle handler | 16,384 | 20.200 | 1.047 | 19.29× |
| Lifecycle array drain | 16,384 | 20.332 | 1.452 | 14.00× |

Actor creation previously restarted its vacant-slot search at zero, making
initial population quadratic. Slots now come from an intrusive free list:
slot bookkeeping is constant work per actor, with a short gate protecting
concurrent list access. Allocator calls run outside that gate. Slot size stays
128 bytes; the link occupies former padding. Allocation failures and foreign-
thread reclamation return slots through the same list.

Lifecycle drains previously walked the entire remaining list to obtain its
length on every batch. The queue already counted its nodes while reversing
the detached stack; that count now travels with the retained suffix. Draining
`N` events in batches of `B` changes from `O(N²/B)` traversal to `O(N)` total
traversal. The first detach still reverses the whole batch once. Handler
failure retries retain the remaining count and assigned sequence numbers.

Focused `perf` samples before the changes attributed 99.65% of user cycles to
actor creation's search path at 32,768 actors, and 93.22% to lifecycle handling
at 16,384 events with batch size 1. These probes characterize those operations,
not overall application performance or concurrent allocator scalability.
