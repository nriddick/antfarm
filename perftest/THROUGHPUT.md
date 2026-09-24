# Existing synthetic throughput comparison

Measured on 2026-09-24: original checkout `8e988f6` versus optimized runtime
`e6ce4d1` on branch `codex/optimize-batched-dispatch`. Host: Ryzen 5 5500,
six physical cores / twelve logical processors, one LLC; Linux x86-64;
LDC 1.43.0 / LLVM 22.1.8. Both revisions used `-O2 -release` and
`ANTFARM_HUGE_PAGES=0`. Benchmark sources were unchanged between revisions. These measurements
predate the retirement and single-shot payload changes recorded in
[LIFECYCLE.md](LIFECYCLE.md).

Each number below is the median of five runs per revision. Runs were serial,
alternating old/new order between pairs. Small changes with overlapping
sample ranges are not evidence of a reliable improvement.

| Workload | Original million/s | Optimized million/s | Change |
| --- | ---: | ---: | ---: |
| Payloads, one consumer | 83.383 | 82.867 | -0.6% |
| Payloads, six consumers | 87.211 | 87.800 | +0.7% |
| Fibers, single thread, warm creation | 3.087 | 3.440 | +11.4% |
| Fibers, single thread, warm execution | 4.555 | 4.296 | -5.7% |
| Fibers, single thread, warm creation + execution | 1.840 | 1.919 | +4.3% |
| Fibers, twelve workers, warm spawn through completion | 1.730 | 1.759 | +1.7% |
| Actor waves, six workers, one orchestration Fiber | 26.190 | 26.828 | +2.4% |
| Actor waves, six workers, two orchestration Fibers | 31.977 | 32.170 | +0.6% |

Units are completed payloads, Fibers, or actor invocations per second,
according to the row. These operations have different costs and should not
be compared with one another as interchangeable units.

The payload executables were byte-identical (SHA-256
`2b31f11f52b6d2a5276176198a47e8754356cce510d05c184efa759ef4c2bf30`),
so their differences are measurement variation. The core payload path was
unchanged between these two revisions.

The single-thread Fiber execution decline reproduced in seven additional
alternating pairs pinned to CPU 2: warm execution was 4.482 → 4.260 million/s
(-5.0%). Original samples ranged from 4.462 to 4.558; optimized samples ranged
from 4.186 to 4.316. Warm creation remained faster, with a wider spread:
2.657 → 3.205 million/s medians (+20.6%), and combined warm throughput was
1.668 → 1.836 million/s (+10.1%). This confirms an execution regression in
this synthetic workload; its cause has not been established. It should not
be hidden by the combined result. Cold creation stayed near 0.156 million
Fibers/s on one thread and 0.155 million/s in the multithreaded test.

The multithreaded benchmark's workers can execute during spawning. Its
reported drain-only warm rate was 7.727 → 7.831 million Fibers/s (+1.3%),
but that divides the full task count by only the post-spawn interval. The
table instead divides task count by spawn time plus drain time. Neither
combined Fiber metric includes the preceding completion release operation.

Steady actor-wave sample ranges overlap: one orchestration Fiber was
26.053–26.789 → 25.067–27.476 million actor invocations/s; two orchestration
Fibers were 31.328–32.208 → 31.526–32.631. Whole actor-wave process time,
including setup, warmup, both measured variants, and cleanup, fell from
4.742 to 2.355 seconds (50.3% less time). That is consistent with eliminating
the quadratic actor-slot creation scan. Construction is outside the timed
wave phases, so this gain does not appear in their steady throughput rates.

These ordinary throughput tests do not exercise autonomous actor-ready
backlog draining or Fiber lifecycle reporting. Their results are distinct
from the large scaling improvements measured in [README.md](README.md).
DRuntime Fiber execution and the public Fiber model remain in use.

## Reproduction

Build each revision separately, using the same toolchain:

```sh
ldc2 -O2 -release perftest/throughput.d antfarm.d antfarm_allocation.d -of=payload
ldc2 -O2 -release -i -I. -Ifibers/source -Ithreadpool/source fibers/benchmarks/benchmark.d -of=fiber_st
ldc2 -O2 -release -i -I. -Ifibers/source -Ithreadpool/source fibers/benchmarks/benchmark_mt.d -of=fiber_mt
ldc2 -O2 -release -i -I. -Ifibers/source -Ithreadpool/source fibers/benchmarks/actor_wave.d -of=actor_wave
```

Run the following on each revision, alternating revision order:

```sh
export ANTFARM_HUGE_PAGES=0
./payload --once --nb 0 --ns 1 --body 1 --batch 256 --ac 0 --qs 16384 --n 16000000 --repeats 1 --nc 1
./payload --once --nb 0 --ns 1 --body 1 --batch 256 --ac 0 --qs 16384 --n 16000000 --repeats 1 --nc 6
./fiber_st 200000 256 0
./fiber_mt 200000
./actor_wave 32768 500 6 20 0 8
```

Payload tests use one producer, one-word bodies, batch 256, a 16 MiB ring,
and per-worker callback counters; those threads were unpinned. Single-thread
Fibers use an 8 MiB ring, batch 256, no yields, and default stacks. The
additional single-thread check uses `taskset -c 2 ./fiber_st 200000 256 0`.
Multithreaded Fibers use twelve pinned workers and a 32 MiB ring. Actor waves
use two sets of 32,768 actors, 500 measured generations after 20 warmups,
six pinned physical-core workers, `avgCost=0`, and an 8 MiB ring.
