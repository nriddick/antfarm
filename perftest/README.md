# Performance characterization

For the original checkout versus optimized branch on the existing payload,
Fiber, and actor-wave benchmarks, see [THROUGHPUT.md](THROUGHPUT.md).

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
