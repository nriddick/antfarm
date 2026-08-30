# Fiber performance

This document records the characterization evidence and tuning guidance for
`antfarm-fibers`. Results are measurements on particular hosts, not API or
throughput guarantees. See [README.md](README.md) for usage and
[DESIGN.md](DESIGN.md) for scheduler invariants.

## What to compare

Ant Farm payloads and managed Fibers solve different problems:

- A payload is the fast path for short, non-suspending `nothrow @nogc` work.
- A managed Fiber pays for ordinary D control flow, suspension, waits, timers,
  joins, cancellation, completion, and stack reuse.
- A same-thread `Fiber.call` loop remains the right baseline for an application
  which only needs coroutines on one OS thread.

The useful system comparison is whether Fibers and payloads can share pinned,
topology-aware workers without blocking them, not whether an empty managed
Fiber can beat a direct `Fiber.call`.

## Characterization host

The results below were collected on a Ryzen 5 5500 with 12 logical processors,
six physical cores, and one LLC, using LDC release builds. Ant Farm huge pages
were enabled and the host used `THP=always`.

This CPU has been more pessimistic about concurrent Farm scaling than the
Intel i7-12700H host used for root Ant Farm profiling. In particular, its
SMT-12 tiny-payload results should not be treated as the transport ceiling.
Multi-LLC hardware results are not yet available.

## Huge-page comparison

`ANTFARM_HUGE_PAGES` changes the Ant Farm ring which backs runnable Fiber
activations; it does not change DRuntime Fiber stack mappings.

Reprofiled 2026-08-30 on the same Ryzen host with LDC 1.42 DUB release builds.
Each row below is the median of three alternating runs of 500,000 Fibers with
`flushBatch=256` and `avgCost=0`. The 4K run forced
`ANTFARM_HUGE_PAGES=0`; the huge-page run forced `=1`.

Live `/proc/<pid>/smaps` inspection verified the distinction after touching
the ring. With huge pages requested, the 8 MiB shared Farm mapping reported
8 MiB `ShmemPmdMapped`, `THPeligible: 1`, and the `hg` VM flag. With the
override disabled it reported zero `ShmemPmdMapped` and was not THP-eligible.
Thus this comparison measured promoted shmem pages, not merely successful
`MADV_HUGEPAGE` advice.

| Fiber backing profile | 4K pages | Huge pages | Difference |
| --- | ---: | ---: | ---: |
| Single-thread first drain | 4.21M/s | 4.29M/s | +2.0% |
| Single-thread recycled drain | 4.39M/s | 4.37M/s | -0.3% |
| 12-worker first drain | 8.85M/s | 7.66M/s | -13.5% |
| 12-worker recycled drain | 8.30M/s | 8.10M/s | -2.4% |

Run each command repeatedly and alternate the page mode:

```text
ANTFARM_HUGE_PAGES=0 dub run -c benchmark --build=release --compiler=ldc2 -- 500000 256 0
ANTFARM_HUGE_PAGES=1 dub run -c benchmark --build=release --compiler=ldc2 -- 500000 256 0
ANTFARM_HUGE_PAGES=0 dub run -c benchmark_mt --build=release --compiler=ldc2 -- 500000
ANTFARM_HUGE_PAGES=1 dub run -c benchmark_mt --build=release --compiler=ldc2 -- 500000
```

Five-pair runs at 200,000 Fibers showed the same boundary: the first concurrent
drain was about 22% slower with huge pages, while the recycled drain was within
2%. At 100,000, the 10–20 ms samples were too short to separate promotion work
from the next generation; the huge-page penalty appeared to spill into the
recycled measurement. The 500,000-Fiber table is the more stable comparison.

For a Fiber-only lane on this host, huge pages are not a steady-state
throughput optimization. They may modestly help a single sequential drain but
can penalize the first concurrent drain. Mixed Fiber/payload deployments must
measure the payload side separately: bare Farm table walks can still benefit
materially from huge pages even when `Fiber.call` and scheduler handoff are the
Fiber bottleneck.

## Scheduler rates

Representative warm and cold results for 200,000 tasks:

| Phase | Representative rate |
| --- | ---: |
| Cold create, single thread | ~147–153k Fibers/s |
| Warm create, single thread | ~3.2–3.6M Fibers/s |
| Warm run, single thread | ~3.4–4.2M resumptions/s |
| Warm run, multiple workers | ~6.9–8.1M resumptions/s |
| Warm create, multiple producers | ~2.0–2.3M Fibers/s |

Cold creation is dominated by DRuntime Fiber creation and stack mapping.
Changing Farm table size, `avgCost`, huge-page policy, or using a 4 KiB stack
did not materially change it. `FiberDomain.reserve` can avoid roots-array
growth for a known burst, but it does not pre-map Fiber stacks.

The common ready path uses one shared resume header and fixed one-word bodies.
It therefore selects Ant Farm's common-header/fixed-length write overload and
avoids per-body sizing. Empty-body Fiber throughput is nevertheless normally
limited by Fiber and scheduler handoff cost rather than Farm table sizing.

## Same-thread comparison

The `benchmark_vs_druntime` baseline reserves the DRuntime arrays, uses
`Fiber.reset`, recycles one Fiber where N stacks are unnecessary, and disables
the GC only around non-allocating direct call loops.

| Path | Representative rate |
| --- | ---: |
| DRuntime reserved run | ~7.0M/s |
| DRuntime reset and run, warm | ~7.3M/s |
| One DRuntime Fiber repeatedly reset and run | ~27M/s |
| DRuntime one-yield, two-pass | ~3.0M Fibers/s |
| Ant Farm Fiber run, warm | ~4.1M/s |
| Ant Farm Fiber one-yield run | ~1.8M Fibers/s |
| Ant Farm payload run | ~55M/s |
| Cold create, either Fiber path | ~150–175k/s |

On one thread, transporting an empty Fiber through the ready MPSC, Farm, and
`Fiber.call` costs roughly half the rate of a tuned direct call loop. A tiny
payload is an order of magnitude faster than either Fiber path. Work which
never suspends should normally remain a payload.

## Worker topology sweep

The following run used 100,000 warm empty Fibers and control-thread-produced
payloads. The Fiber-oriented configuration was `flushBatch=256, avgCost=0`;
the alternate payload configuration was `flushBatch=32, avgCost=2`.

| Topology | Fiber rate | Payload rate |
| --- | ---: | ---: |
| 1 worker | 3.5M/s | 42M/s |
| 2 workers | 3.6M/s | 31M/s |
| 4 workers | 5.4M/s | 32M/s |
| 6 physical cores | 6.7M/s | 29M/s |
| 6 physical, 32 / 2 | 5.4M/s | **53M/s** |
| 12 SMT | **8.3M/s** | 32M/s |
| 12 SMT, 32 / 2 | 5.1M/s | 51M/s |

On this host, all 12 logical processors and large tables won for empty Fibers.
Six physical consumers and smaller chunks won for tiny payloads produced by a
control thread. Extra workers provided more `Fiber.call` sites but contended on
the serial payload ring.

These measurements motivate the two starting presets exposed by the Fiber
integration:

- `fiberThroughput`: all logical processors, `flushBatch=256`, `avgCost=0`;
- `payloadThroughput`: skip SMT siblings, `flushBatch=32`, `avgCost=2`.

They are starting points, not portable policy. A mixed application should
benchmark both with its actual body and wake pattern.

## Embedding overhead

`benchmark_farm_embed` compares the same two-million one-shot payload workload
through direct Farm consumers, the bare threadpool pump, and Fiber managed
worker hooks.

| Embedding | 6 physical cores | 12 SMT |
| --- | ---: | ---: |
| Farm, single producer/consumer thread | 53.8M/s | — |
| Farm, pinned spinning consumers | 54.5M/s | 32.9M/s |
| Threadpool pump, spinning | 54.9M/s | 30.2M/s |
| Threadpool pump, idle and wake | 47.2M/s | 38.9M/s |
| Fiber managed pump | 51.0M/s | 41.3M/s |

On six physical cores, the bare pool pump matched pinned Farm consumers and the
Fiber managed pump was within roughly six percent. The threadpool machinery was
not the limiting component. Parking cost throughput on the physical-core case
but helped the oversubscribed SMT case by removing idle contenders.

## Tuning guidance

- Benchmark useful work. Empty bodies isolate scheduler overhead but do not
  predict a body that sleeps, waits, allocates, or performs substantial work.
- Keep bare payloads as the default for short serial compute. Use Fibers for
  work that actually needs suspension or structured D control flow.
- `flushBatch` and `avgCost` size a Farm table. They are not a resume quantum.
  Production pumps use `consumeNext` and finish or yield the claimed table.
- Lower `flushBatch` when a responder must return to timer polling more often.
- Size Farm consumer and producer capacity for native workers, covering
  sweepers, and every control producer that can hold a registration.
- Call `FiberDomain.reserve(n)` before a known live burst to avoid roots-array
  reallocations.
- Treat huge pages as a payload-oriented opt-in and benchmark Fiber-only lanes
  both ways. On this host they did not improve recycled Fiber drain and slowed
  the first concurrent drain. Set `ANTFARM_HUGE_PAGES=0` for an explicit
  4K-backed profile and `=1` for a huge-page profile.
- Do not wrap scheduler entry or Farm consumption in `GC.disable`. First Fiber
  entry, stack registration, and user callbacks may allocate.
- Profile multi-producer cold admission before optimizing it. Cold stack
  mapping currently occurs while domain admission is serialized and may
  convoy concurrent spawners.

## Allocation profile

After warm-up, ready and completion MPSC publication, stack-snapshot flush,
fixed-width Farm publication, ready republishing, established waits, timer
expiry, lifecycle record reuse, and `Fiber.reset` task reuse do not allocate.

Allocation remains expected for cold Fiber/stack creation, roots or timer heap
growth, first insertion of a signal key, lifecycle setup, returned completion
and lifecycle arrays, shutdown snapshots, exception creation, and user code.
The recycle pool grows toward the concurrent live-Fiber high-water mark rather
than the historical spawn count.

## Reproducing the benchmarks

Run from `fibers/` with an optimized LDC build:

```text
dub run -c benchmark --build=release --compiler=ldc2 -- 200000 256 0
dub run -c benchmark_mt --build=release --compiler=ldc2 -- 100000
dub run -c benchmark_vs_druntime --build=release --compiler=ldc2 -- 100000
dub run -c benchmark_farm_embed --build=release --compiler=ldc2 -- 2000000
```

- `benchmark` characterizes cold/warm creation and single-thread drain.
- `benchmark_mt` measures concurrent Fiber drain.
- `benchmark_vs_druntime` compares DRuntime, managed Fibers, bare payloads, and
  worker topologies.
- `benchmark_farm_embed` isolates direct Farm, threadpool, and Fiber-pump
  embedding cost on the same payload.

Record compiler version, build mode, CPU topology, huge-page setting, task
count, warm/cold state, and batch parameters with any published result.
