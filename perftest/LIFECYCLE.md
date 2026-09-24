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

## Follow-up: use the table's unique single-shot claim

The next pass starts at `0425226`. Fresh profiles still put publication,
dispatch, creation, and reclamation at the top; no new repeated backlog scan
appears. With mimalloc and same-thread consumption, Farm publication accounts
for about 13–15% of sampled cycles and consumer shard processing for 6–8%.

A single-shot payload already belongs to exactly one table shard and one
successfully claimed run. Primary consumers, sweepers, and re-walkers all
claim runs through the same counter. The MT index excludes these payloads.
Their additional Pcount fetch-add therefore performs no arbitration. Replace
it with a raw atomic store recording the unique claim. Callbacks still observe
one claim, zero calls/completions, and iteration zero. Ring layout, table
completion, and all MT admission counters retain their existing contracts;
`MaxCs > 1, Done == 1` still takes the MT path. The ordering argument is in
[SPEC.md, section 7c](../SPEC.md#7c-secondary-work-claims).

This is a constant-factor improvement to the existing O(N) dispatch path.
It needs no allocator change, application API change, or Fiber replacement.

The same review found a wave-membership race, fixed separately in `8c36506`.
Clearing the intrusive link after releasing the lifetime pin could overwrite
the next wave's newly installed link. A deterministic test pauses immediately
after unpinning, reuses the actor as another wave's head, then resumes the
original wave. The old ordering loses a member and fails retirement; clearing
the link before unpinning passes. This is a correctness fix, not a claimed
throughput improvement.

The new Farm regression test mixes single-shot ST, single-shot MT, and
multi-iteration MT payloads across ring laps, two producers, eight consumers,
and chunk sizes 32 and one. It checks exact per-payload execution, no execution
on resubscription, and the callback-visible ST claim/iteration values.
Root and actor suites pass with LDC and DMD; the Farm `torture_tests` suite passes
with LDC, DMD, and ThreadSanitizer. Actor ThreadSanitizer, real mimalloc
`MI_DEBUG=FULL`, Fiber unit tests with both compilers, and the LDC release Fiber
stress run also pass.

### Follow-up measurements

The same unchanged churn harness compares `0425226` with this pass, using
five runs per configuration, two measured seconds per run, 16,384 actors,
batch 256, and the pinned placements from the earlier comparison. Builds and
correctness tests finish before timing starts. Medians below are millions of
complete lifetimes/second; all 120 timed runs pass the lifecycle checks.

| Execution | Mode | Allocator | Before | After | Change |
| --- | --- | --- | ---: | ---: | ---: |
| Same thread | Autonomous | C runtime | 8.364 | 8.426 | +0.74% |
| Same thread | Wave | C runtime | 9.049 | 9.180 | +1.45% |
| Same thread | Autonomous | mimalloc | 18.098 | 18.247 | +0.82% |
| Same thread | Wave | mimalloc | 21.646 | 22.346 | +3.24% |
| Six consumers | Autonomous | C runtime | 5.896 | 5.959 | +1.07% |
| Six consumers | Wave | C runtime | 5.991 | 6.013 | +0.38% |
| Six consumers | Autonomous | mimalloc | 14.822 | 14.992 | +1.15% |
| Six consumers | Wave | mimalloc | 15.319 | 15.450 | +0.86% |

Same-thread mimalloc wave samples are 21.411–21.733 before and
22.271–22.415 after. Dispatch falls from 20.61 to 18.87 ns/actor; creation and
retirement/reclamation remain close. Autonomous dispatch falls from 29.54
to 28.86 ns/actor, but its full-lifetime gain is smaller and sample ranges
overlap. Six-consumer mimalloc ranges separate by a small amount; both C
allocator ranges overlap. Treat the latter as effectively flat, rather than
evidence of a dependable one-percent improvement.

Caller policies on this pass, with the same placements:

| Execution | Mode | mimalloc, batch 256 | Pool, batch 256 | mimalloc, whole-wave offer | Pool, whole-wave offer |
| --- | --- | ---: | ---: | ---: | ---: |
| Same thread | Autonomous | 18.247 | 18.948 | — | — |
| Same thread | Wave | 22.346 | 22.784 | 22.713 | 23.338 |
| Six consumers | Autonomous | 14.992 | 15.483 | — | — |
| Six consumers | Wave | 15.450 | 16.289 | 15.940 | 16.616 |

Pooling and larger wave offers remain useful, modest caller choices. These
are new measurements, not a sum of percentage gains from earlier runs.

### Caller placement and consumer count

A separate three-run sweep keeps the controller on logical CPU 6. Consumers
first occupy separate physical cores (CPUs 1–5); the sixth consumer adds CPU
0, the controller's SMT sibling. Thus five versus six consumers directly
tests adding a poller on the controller's core. Zero consumers means the
controller also dispatches. These runs use the pool, autonomous batch 256,
and whole-wave offers of 16,384; all 24 runs pass lifecycle checks.

| Separate consumers | CPU list | Autonomous M lifetimes/s | Wave M lifetimes/s |
| ---: | --- | ---: | ---: |
| 0 | `6` | 18.864 | 23.059 |
| 1 | `6,1` | 21.786 | 26.276 |
| 5 | `6,1,2,3,4,5` | 19.970 | 21.616 |
| 6 | `6,1,2,3,4,5,0` | 15.469 | 16.728 |

For this trivial-callback workload, one separate consumer is best among
these placements: roughly 15% above same-thread execution for autonomous
actors and 14% for waves. The controller does every creation and reclamation,
so giving it its own physical core matters. Removing the sibling poller
improves five-consumer throughput by about 29% over six consumers. Additional
consumers do not automatically help a lifecycle-heavy application with tiny
callbacks; applications doing substantial callback work should measure their
own worker counts and placements.

```sh
./perftest/actor_churn_mimalloc actor 16384 2 1 256 5 pool 6,1
./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 pool 6,1
```

The one-consumer placement also helps without a custom pool. Three additional
runs with native mimalloc give **20.643 M autonomous lifetimes/s** (batch 256)
and **25.192 M wave lifetimes/s** (whole-wave offer), with all six runs passing
the lifecycle checks. The corresponding pooled values above add about 5.5%
and 4.3%. Replace `pool` with `mimalloc` in the commands to reproduce this
simpler caller configuration.
