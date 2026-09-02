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
- `flushBatch` and `avgCost` size a Farm table. Production pumps use
  `consumeNext` and finish or yield the claimed table; use shorter tables when
  a scheduler visit must be shorter.
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
dub run -c benchmark_actor_wave --build=release --compiler=ldc2 -- 32768 200 6 20 0 8
dub run -c benchmark_actor_wave --build=release --compiler=ldc2 -- 32768 200 0 20 0 8 --smt --sweep
```

- `benchmark` characterizes cold/warm creation and single-thread drain.
- `benchmark_mt` measures concurrent Fiber drain.
- `benchmark_vs_druntime` compares DRuntime, managed Fibers, bare payloads, and
  worker topologies.
- `benchmark_farm_embed` isolates direct Farm, threadpool, and Fiber-pump
  embedding cost on the same payload.
- `benchmark_actor_wave` compares one Fiber producing two actor waves against
  two Fibers producing one independent wave each. Both cases commit one shared
  top-down generation only after both sets finish. Each actor contains one
  cache-padded private word and two cache-padded public words; its operation is
  only an identity projection through those lines. Arguments are actors per
  set, measured generations, maximum selected workers (`0` means all), warm-up
  generations, `avgCost`, and Farm ring size in MiB (default 8, power of two).
  `--smt` includes sibling logical processors after selecting one worker per
  physical core. `--sweep` recreates the Farm and worker pool for every prefix
  from one worker through that selected set, then prints a compact Mactor/s
  summary. The positional worker maximum caps the sweep; zero uses every
  available physical worker, or every logical processor with `--smt`.

### Actor-wave optimization checkpoint

The first guided pass used a Ryzen 5 5500, six non-SMT workers, ordinary 4 KiB
pages, LDC release mode, 32,768 actors per set, 200 measured generations, and
the median of three runs. Costs are nanoseconds per actor dispatch:

| implementation | one Fiber | two Fibers |
|---|---:|---:|
| initial wave path | 286.50 | 211.63 |
| omit redundant wave `scheduled -> running` RMW | 268.98 | 193.78 |
| reserve only the exact Farm-accepted prefix | 40.99 | 36.94 |
| remove redundant per-member submission pins | 39.51 | 36.38 |

The accepted changes reduce median cost by 7.25x and 5.82x respectively.
Exact-prefix reservation is the decisive change: the old path reserved the
entire remaining slice and cancelled its unwritten tail for every physical
table, creating a shrinking-tail quadratic scan. Packing wave generation and
status into one atomic word and removing a duplicate admission load were also
measured. Neither improvement survived repeated sampling, so both were
reverted rather than paying semantic or maintenance costs without evidence.

Replacing completion and two-producer rendezvous polling with counted Fiber
generation triggers was measured separately with the 8 MiB ring. At 500
generations after 20 warm-ups, median cost was 36.34 ns/actor for one Fiber and
29.27 ns/actor for two. Retaining scheduler-created wait keys made the final
medians 36.25 and 29.22 ns/actor respectively, within ordinary run variance,
while removing repeated wait-bucket allocation. Only Farm publication
backpressure now uses cooperative
yields; completion parks on the scheduler wait path. Typical measured yields
fell from hundreds per generation to roughly 4--12, depending on run timing.
The completion callback itself remains `nothrow @nogc`: it advances a counter
and enqueues one preallocated deferred node, which a managed worker converts
to a normal cancellable FiberEvent wake after payload dispatch.

### Actor-wave worker scaling

A full worker-prefix sweep on 2026-09-02 used the same Ryzen 5 5500, an LDC
release build, ordinary 4 KiB pages, the 8 MiB Farm ring, 32,768 actors per
set, 200 measured generations after 20 warm-ups, and `avgCost=0`. The first
six workers select one logical processor from each physical core; workers
7--12 add those cores' SMT siblings. This was one ordered characterization
sweep rather than a median of independent runs:

| Workers | SMT siblings | One Fiber Mactor/s | Two Fibers Mactor/s | Two / one |
|---:|---:|---:|---:|---:|
| 1 | 0 | 16.49 | 16.22 | 0.984x |
| 2 | 0 | **24.56** | 23.73 | 0.966x |
| 3 | 0 | 24.44 | 28.50 | 1.166x |
| 4 | 0 | 24.55 | **30.96** | 1.261x |
| 5 | 0 | 23.84 | 30.84 | 1.294x |
| 6 | 0 | 23.30 | 30.87 | 1.325x |
| 7 | 1 | 22.34 | 28.04 | 1.255x |
| 8 | 2 | 22.08 | 27.35 | 1.238x |
| 9 | 3 | 21.27 | 27.18 | 1.278x |
| 10 | 4 | 20.88 | 26.50 | 1.269x |
| 11 | 5 | 20.54 | 26.68 | 1.299x |
| 12 | 6 | 17.37 | 23.37 | 1.345x |

One orchestration Fiber gets nearly all of its scaling from the second
physical worker: throughput rises 49% from one to two workers, remains around
24.5 Mactor/s through four, and then declines. Two independent producer
Fibers are slightly slower at one and two workers, where their rendezvous and
publication overhead have no spare execution capacity. They overtake the
single producer at three workers, peak at four, and hold approximately 30.8
Mactor/s through all six physical cores. At six workers the two-Fiber workflow
is 1.33x as fast as one Fiber, but neither workflow scales linearly with core
count.

Adding SMT siblings is uniformly detrimental to absolute throughput in this
sweep. From six physical workers to all twelve logical workers, the one-Fiber
case falls 25% and the two-Fiber case falls 24%. The apparent two/one ratio at
twelve therefore reflects the single-producer path degrading more, not an SMT
throughput win. Full SMT also raised publication-backpressure churn sharply in
this run, to 116 cooperative yields per generation for one Fiber and 151 for
two, compared with 8 and 10 respectively at six physical workers.

This benchmark performs only the identity projection through each actor's
private and double-buffered public cache lines. Its useful conclusion is that
the wave transport has enough independent work to benefit from a few physical
workers and from two producers, while its empty behavior becomes limited by
Farm publication, cache traffic, and orchestration before all physical cores
are saturated. Actor behaviors with more computation may move that knee and
should repeat the sweep rather than adopting four workers as policy.

### Actor-wave Farm ring size

This host has one 16 MiB L3. After installing counted Fiber triggers, a
follow-up comparison used the same six physical workers, 32,768 actors per
set, 500 measured generations, and 20 warm-up generations. Each value is the
median of three runs with ordinary 4 KiB pages:

| Farm ring | one Fiber | two Fibers |
|---|---:|---:|
| 16 MiB | 39.75 ns/actor | 36.54 ns/actor |
| 8 MiB | 36.82 ns/actor | 29.90 ns/actor |

The 8 MiB ring improves the one-Fiber case by 7.4% and the two-independent-
Fiber case by 18.2%, despite incurring a few publication-backpressure yields
where 16 MiB incurred almost none. Cache footprint outweighs extra producer
slack for this workload, so 8 MiB is the characterization-host default. This
is not evidence for changing the general Farm default: the actor state alone
is 12 MiB, and the result may depend on this LLC, concurrent producer pattern,
table geometry, and state footprint.

Record compiler version, build mode, CPU topology, huge-page setting, task
count, warm/cold state, and batch parameters with any published result.
