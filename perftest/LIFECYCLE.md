# Improving complete actor lifetime throughput

The primary measure is sustained create → dispatch → retire → reclaim
throughput. Dispatch-only rates and phase timings explain a result, but do
not replace the complete-lifetime rate. Runtime algorithm changes are
measured first with unchanged workload and allocator settings; caller-side
allocation and batching policies are then measured separately. DRuntime
Fibers remain the project's Fiber execution model.

## Runtime change: finish quiescent idle retirement without a CAS

Starting from `deb987e`, `perf` attributed roughly 13% of autonomous churn's
user cycles and 15% of wave churn's cycles to the retirement-request path.
This is with mimalloc, 16,384 actors, batch 256, and one thread doing both
publication and consumption. Publication, dispatch, creation, and reclamation
accounted for most of the rest; there was no further large repeated scan in
this profile.

The retirement algorithm now joins the closed submission gate and rechecks
the actor lifecycle. When the gate has zero accepted senders and the lifecycle
is still idle with `RETIRE` set, the unique owner can release-store `RETIRED`.
At that point accepted senders have finished signalling, there is no running
callback, and ordinary wakes/wave reservations reject the closing actor.
An additional compare/exchange contributes no ownership or ordering.

The post-join lifecycle check is required. A sender can publish and queue
work after retirement's first idle observation and before releasing the last
submission reservation. A deterministic test forces that interleaving and
checks that the message still executes before reclamation. Other retirement
paths retain their RMW/drain protocol. See the
[ordering argument](../actors/ACTOR_MEMORYORDER.md#retirement-and-reclamation).

## Caller choices

The benchmark now supports a `pool` allocator: one preallocated slab holds
exactly one cohort of fixed-size actor states. Its free list is owned by the
controlling thread, which already performs every allocation and reclamation.
Runtime and slot allocations use the ordinary C policy. The pool is an
example of using `ActorAllocator`; it is not a production allocator or a
thread-safe policy for arbitrary callers.

Each pooled cycle still creates fresh actor generations, copies initial
state, executes the callback, retires and reclaims every actor, returns each
state block to the free list, and verifies complete return of the pool. It
does not return individual blocks to the system allocator. The backing slab
is allocated before warm-up and released after runtime teardown. Pool
capacity exhaustion returns allocation failure; there is no hidden fallback
for actor state. The normal C/mimalloc cases retain per-actor allocator calls.

Wave mode also accepts publication batches larger than 256. Passing the
whole cohort lets the existing Farm API choose physical table sizes. The
autonomous runtime's public flush limit remains 256. Neither change skips
actor lifetime operations or correctness checks.

```sh
make -C perftest actor_churn_mimalloc
export ANTFARM_HUGE_PAGES=0
./perftest/actor_churn_mimalloc actor 16384 2 0 256 5 mimalloc
./perftest/actor_churn_mimalloc actor 16384 2 0 256 5 pool
./perftest/actor_churn_mimalloc wave 16384 2 0 256 5 mimalloc
./perftest/actor_churn_mimalloc wave 16384 2 0 16384 5 mimalloc
./perftest/actor_churn_mimalloc wave 16384 2 0 16384 5 pool
```

`pool` also works in the plain `actor_churn` build without mimalloc. Replace
the consumer argument `0` with `6` to add six un-pinned polling consumers;
the controlling thread still owns creation, reclamation, and pool access.

An optional last argument explicitly pins threads. For example, on the
measurement host, CPU 2 runs the single-thread case; a six-consumer run uses
controller CPU 6 and consumer CPUs 0–5:

```sh
./perftest/actor_churn_mimalloc actor 16384 2 0 256 5 mimalloc 2
./perftest/actor_churn_mimalloc wave 16384 2 6 16384 5 mimalloc 6,0,1,2,3,4,5
```

These CPU IDs are host-specific. The six-consumer placement occupies all six
physical cores, with the controller sharing core 0's SMT pair with consumer
CPU 0. Pinning happens before allocation, warm-up, and measurement. An invalid
or unavailable placement fails instead of silently running un-pinned.

## Results

Measured 2026-09-24 on the Ryzen 5 5500 with LDC 1.43.0 / LLVM 22.1.8,
`-O2 -release`, ordinary pages, 16,384 actors, five warm-up cycles, and at
least two measured seconds per run. The same final benchmark source is built
against `deb987e` and the runtime change. The table uses three runs per
configuration in reversed/rotated order, with the CPU placements above.
All numbers are medians in **millions of complete actor lifetimes/second**.

Runtime change alone, preserving allocator and publication batch 256:

| Execution | Mode | Allocator | Before | After | Change |
| --- | --- | --- | ---: | ---: | ---: |
| Same thread | Autonomous | C runtime | 8.236 | 8.355 | +1.45% |
| Same thread | Wave | C runtime | 8.879 | 9.034 | +1.74% |
| Same thread | Autonomous | mimalloc | 17.437 | 18.042 | +3.47% |
| Same thread | Wave | mimalloc | 20.741 | 21.652 | +4.39% |
| Six consumers | Autonomous | C runtime | 5.924 | 5.919 | -0.08% |
| Six consumers | Wave | C runtime | 6.016 | 6.023 | +0.11% |
| Six consumers | Autonomous | mimalloc | 14.721 | 14.850 | +0.88% |
| Six consumers | Wave | mimalloc | 15.145 | 15.383 | +1.57% |

With mimalloc on one thread, the retirement/reclamation/check phase falls
from 16.59 to 14.89 ns/actor for autonomous dispatch and from 16.56 to
14.81 ns/actor for waves. Other phases stay close. The C allocator dominates
more of the total cycle, so this runtime saving has less effect there. The
six-consumer C results are effectively flat.

Caller policies on the new runtime:

| Execution | Mode | mimalloc, batch 256 | Pool, batch 256 | mimalloc, whole-wave offer | Pool, whole-wave offer |
| --- | --- | ---: | ---: | ---: | ---: |
| Same thread | Autonomous | 18.042 | 18.737 | — | — |
| Same thread | Wave | 21.652 | 22.028 | 22.069 | 23.008 |
| Six consumers | Autonomous | 14.850 | 15.536 | — | — |
| Six consumers | Wave | 15.383 | 16.341 | 15.927 | 16.815 |

For waves, the combined runtime/pool/batching result is about 11% above
the starting mimalloc/batch-256 rate in both placements. Pooling adds modest
benefit beyond mimalloc and requires known capacity plus allocator ownership;
it is an optional caller tradeoff. Larger wave offers work through the
existing publication API and need no custom allocator. No Fiber-model change
is involved.

The initial un-pinned matrix was too variable to interpret small differences
in the multithreaded cases: before/after mimalloc wave samples spanned
15.099–19.488 and 15.436–20.400 million lifetimes/s, respectively. The fixed
placement rerun narrowed those ranges to 15.132–15.181 and 15.374–15.411.
The pinned values describe this specific placement, including the controller
sharing an SMT core; they are not a claim about optimal worker topology.

Validation: root tests and actor torture tests pass with LDC and DMD; actor
ThreadSanitizer and the real mimalloc `MI_DEBUG=FULL` lane pass. The 72 pinned
timed comparisons all pass exact per-cycle execution/reclamation checks.
Pool/large-wave/affinity smoke checks also pass in LDC release and DMD debug
builds. Invalid CPU-list length and unavailable CPUs fail explicitly.
