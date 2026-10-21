# Ant Farm performance

Measured on October 20, 2026, on an AMD Ryzen 9 5900XT (16 cores, 32 threads,
two 8-core CCDs with one L3 each) at runtime revision `d00b37c` on
`1.9.0-progress`, with LDC 1.43.0 `-O2 -release` and mimalloc 3.5.3. Every
figure is the **median ± sample standard deviation of five independent
process samples**; the tail tables aggregate five processes of 10,000
observations each. The i7-12700H tables that this page carried are retired:
the Windows host rebuilds them with the same driver when the branch
transfers, with the shorter thread lists noted under [Method](#method). The
previous page is archived in
[docs/performance/archive-2026-10-20.md](docs/performance/archive-2026-10-20.md).

## Headline throughput

Four workloads at 2 to 40 threads. Payloads are one-word, uniform-callback
tables of 1,700 entries claimed 128 at a time, with about one small producer
per two consumers, unpinned. Private actors are child lifecycles: each parent
refills one slab cohort eight times (`tree-refill-slab`), so spawn, dispatch,
retirement and refill all count and nothing allocates. Public actors are wave
lifecycles (`wave`): create, publish through a sealed `ActorWave`, retire and
reclaim 16,384 actors per cycle. Fibers are the warm burst of
`benchmark_mt`: 200,000 fibers spawned from the recycling pool and drained,
with that many pinned workers. For the actor and Fiber columns, threads are the
controller plus `threads - 1` pinned consumers or workers, filling one CCD's
cores, then its SMT siblings, then the other CCD; 40 threads oversubscribes the
32 logical processors and runs unpinned, and the Fiber pool cannot oversubscribe.

| Threads | Payloads, Mpps | Private actors, Mcycles/s | Public actors, Mcycles/s | Warm Fibers, Mfibers/s |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 687.8 ± 68.0 | 168.1 ± 0.2 | 29.64 ± 0.04 | 2.69 ± 0.01 |
| 4 | 1,070.1 ± 63.4 | 316.0 ± 0.3 | 26.17 ± 0.03 | 2.63 ± 0.05 |
| 8 | 2,045.8 ± 148.3 | 457.2 ± 0.7 | 24.33 ± 0.06 | 2.43 ± 0.05 |
| 16 | 2,309.2 ± 101.9 | 516.5 ± 0.7 | 16.52 ± 0.08 | 1.88 ± 0.01 |
| 20 | 2,298.7 ± 174.1 | 206.5 ± 0.6 | 10.97 ± 0.03 | 1.97 ± 0.02 |
| 24 | 2,487.1 ± 192.9 | 181.6 ± 6.5 | 9.41 ± 0.04 | 2.03 ± 0.01 |
| 32 | 2,415.8 ± 73.0 | 158.5 ± 4.0 | 8.36 ± 0.06 | 2.00 ± 0.02 |
| 40 | 2,179.0 ± 136.1 | 48.2 ± 7.3 | 3.94 ± 0.82 | — |

Payload throughput peaks at 2.49 billion per second with 24 threads and
stays above two billion from 8 threads up. Private child lifecycles peak at
517 million per second with 16 threads, when one CCD is fully occupied; the
first consumer on the second CCD costs more than it adds, and the rate settles
near 160 to 210 million per second until oversubscription. Public waves and
Fibers are bounded by their single control thread: wave admission runs at
about 30 million lifecycles per second with one consumer and falls as
consumers contend for the same publish path, and a Fiber burst is spawned by
one thread at 1.9 to 2.7 million per second however many workers drain it.
The thread sweeps of the root lifecycle, the public actor lifecycle and the
Fiber drain phase are in the [comprehensive array](#thread-sweeps).

## Tail latency

The latency of one payload: from just before its `write()` to the first
instruction of its callback, observed by the library's own consumer loop
(pinned threads calling `consumeNext`) with no out-of-band signalling.
Threads are `threads - 1` consumers plus the producer. **Idle** writes the
sentinel into an otherwise empty Farm. **Mid-drain** writes it behind a table
of 256 one-microsecond jobs once a quarter of them have run. **Burst** writes
32 sentinels back to back behind the same backlog. Each process records 10,000
observations per scene after 200 warm-ups and reports its own quantiles; the
table shows the median ± standard deviation of those process quantiles.

| Threads | Scene | p50 µs | p99 µs | p99.9 µs | Processes |
| ---: | --- | ---: | ---: | ---: | ---: |
| 2 | idle | 0.14 ± 0.00 | 1.80 ± 0.04 | 3.20 ± 0.05 | 5 / 5 |
| 2 | mid-drain, 256 × 1 µs | 204.80 ± 1.39 | 208.20 ± 1.31 | 213.50 ± 1.58 | 5 / 5 |
| 2 | burst, 256 × 1 µs | 250.30 ± 2.33 | 255.80 ± 2.19 | 264.30 ± 4.59 | 5 / 5 |
| 6 | idle | 0.23 ± 0.00 | 2.00 ± 0.08 | 4.30 ± 1.69 | 5 / 5 |
| 6 | mid-drain, 256 × 1 µs | 3.20 ± 0.04 | 20.40 ± 0.09 | 22.70 ± 0.23 | 5 / 5 |
| 6 | burst, 256 × 1 µs | 26.80 ± 0.58 | 30.50 ± 0.24 | 32.80 ± 0.65 | 5 / 5 |
| 12 | idle | 0.31 ± 0.00 | 2.60 ± 0.05 | 6.70 ± 0.57 | 5 / 5 |
| 12 | mid-drain, 256 × 1 µs | 10.50 ± 0.00 | 12.70 ± 0.65 | 14.40 ± 0.11 | 5 / 5 |
| 12 | burst, 256 × 1 µs | 10.90 ± 0.05 | 15.40 ± 0.11 | 18.40 ± 0.34 | 5 / 5 |
| 20 | idle | 0.60 ± 0.01 | 2.80 ± 0.18 | 11.70 ± 1.80 | 5 / 5 |
| 20 | mid-drain, 256 × 1 µs | 0.42 ± 0.00 | 1.50 ± 0.28 | 2.60 ± 0.47 | 5 / 5 |
| 20 | burst, 256 × 1 µs | 1.40 ± 0.05 | 6.50 ± 0.68 | 9.50 ± 4.04 | 5 / 5 |
| 32 | idle | 0.73 ± 0.01 | 3.60 ± 0.27 | 18.30 ± 0.67 | 5 / 5 |
| 32 | mid-drain, 256 × 1 µs | 0.67 ± 0.02 | 1.30 ± 0.07 | 3.30 ± 0.19 | 5 / 5 |
| 32 | burst, 256 × 1 µs | 0.88 ± 0.06 | 5.50 ± 0.67 | 9.60 ± 0.81 | 5 / 5 |

An idle sentinel reaches a consumer in 0.14 to 0.73 µs at the median and
under 4 µs at p99 at every thread count. Behind a backlog, one consumer must
first finish the 256 queued microsecond jobs ahead of the sentinel, hence the
205 µs at 2 threads. With 5 to 11 consumers it waits for one consumer's
current 16-entry chunk, 3 to 10 µs at the median, and from 20 threads it is
picked up as fast as an idle one. The full scene matrix, including 2,048 and 8,192-entry
backlogs, zero and 10 µs jobs and the near-full ring, is in the
[comprehensive array](#full-tail-latency-matrix).

## Method

| Property | This run |
| --- | --- |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads, two CCDs (LLC0 logical 0-7 and 16-23, LLC1 8-15 and 24-31), 62 GiB |
| OS | Linux 7.2.9 (CachyOS), `amd-pstate-epp` with the `performance` governor, ASLR on |
| Compiler | LDC 1.43.0 (LLVM 22.1.8), `-O2 -release`; Fibers: DUB release; C driver `cc -O2` |
| Allocator | mimalloc 3.5.3 (`tests/actors/.build/mimalloc-v3.5.3/libmimalloc.a`) unless the case says CRT or counted |
| Pages | ordinary 4 KiB pages (`ANTFARM_HUGE_PAGES=0`) |
| Samples | 5 independent processes per cell; tail: 5 processes × 10,000 observations per scene |
| Aggregation | median ± sample standard deviation of valid samples; tail: of per-process quantiles, never pooled |
| Validity | a nonzero exit, timeout or missing primary metric fails a sample and the cell; every cell here has 5 / 5 |
| Background load | none filtered; load averages are retained in the raw captures |
| Placement | pinned cells fill one last-level-cache group at a time, its cores before its SMT siblings; payload and dual cells are unpinned |
| Actor workload | 16,384 actors, 3 s per sample, batch 256, 5 warm-up cohorts, quota 16,384 words, unless named otherwise |

These are descriptive measurements, not a regression gate. Code layout alone
moves zero-consumer cells by several percent on identical semantics; compare
revisions with the [performance report](tools/perf/README.md#performance-report),
which pairs builds and reports spread. Synthetic callbacks are deliberately
small, so adding threads can expose coordination cost rather than add
throughput; these sweeps do not predict application speedups.

The [machine-readable results](docs/performance/5900xt-2026-10-20.json) keep
every sample, command, environment and host identity. Reproduce with the
current harness and the pinned allocator built:

```sh
make -C tools/perf SHELL=/usr/bin/bash all actor_churn_mimalloc
python3 -B tools/perf/run_performance_md.py --out .build/performance-refresh \
  --runs 5 --six-consumer-runs 5 --fiber-tasks 200000 \
  --sections host,headline,tail,farm_consumers,farm_chunk_batch,farm_best,dual,public,private,private_tuning,allocators,pinning,fibers,scaling \
  --markdown .build/performance-refresh/tables.md
```

On the 12700H add `--threads 2,4,8,16,20,24 --tail-threads 2,6,12,20`. Use a
fresh output directory for a new revision or rebuilt binaries: resume checks
command, environment and sample count, not runtime identity. The
[mode index](tools/perf/README.md) defines every workload.

## Comprehensive array

### Thread sweeps

The headline's companion columns: root lifecycles (`private-slab`, the
emitter spawning and collecting slab-stored roots), public actor lifecycles
(`actor`: create, wake, execute, await, retire, reclaim) and the drain phase of
the warm Fiber burst (fibers executed per second once spawned). Roots and
actors are bounded by their emitter; the drain scales with workers.

| Threads | Private roots, Mcycles/s | Public actor lifecycles, Mcycles/s | Warm Fiber drain, Mfibers/s |
| ---: | ---: | ---: | ---: |
| 2 | 68.6 ± 0.2 | 23.16 ± 0.06 | 4.94 ± 0.05 |
| 4 | 96.7 ± 0.4 | 23.54 ± 0.01 | 7.56 ± 0.27 |
| 8 | 122.7 ± 1.2 | 23.01 ± 0.03 | 11.14 ± 0.07 |
| 16 | 68.4 ± 1.5 | 16.35 ± 0.02 | 8.49 ± 0.07 |
| 20 | 50.8 ± 0.1 | 12.02 ± 0.02 | 10.26 ± 0.15 |
| 24 | 49.6 ± 0.3 | 10.75 ± 0.03 | 12.99 ± 0.11 |
| 32 | 52.5 ± 0.2 | 9.53 ± 0.02 | 12.69 ± 0.15 |
| 40 | 29.0 ± 2.6 | 6.30 ± 0.64 | — |

### Farm core

Rates are millions of completed payloads per second (Mpps). The callback uses
per-worker batched accounting and the ring is 16 MiB. **Throughput vs
consumers:** one bulk producer, 16-word bodies, batch 80, `nc` consumers.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `nc1` | 179.808 ± 1.525 | 5 / 5 |
| `nc2` | 130.995 ± 26.519 | 5 / 5 |
| `nc4` | 127.417 ± 21.891 | 5 / 5 |
| `nc6` | 110.869 ± 4.106 | 5 / 5 |
| `nc11` | 99.353 ± 6.583 | 5 / 5 |

**Chunk and batch:** four consumers, one bulk producer, 16-word bodies.
`ac0..3` select chunks 128, 64, 32 and 16; `b` is the write batch.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `ac0_b64` | 118.887 ± 17.274 | 5 / 5 |
| `ac0_b128` | 137.735 ± 26.302 | 5 / 5 |
| `ac0_b256` | 169.647 ± 16.818 | 5 / 5 |
| `ac0_b512` | 197.504 ± 11.159 | 5 / 5 |
| `ac0_b1024` | 200.827 ± 5.993 | 5 / 5 |
| `ac0_b2048` | 200.391 ± 1.112 | 5 / 5 |
| `ac1_b64` | 148.004 ± 21.896 | 5 / 5 |
| `ac1_b128` | 141.364 ± 20.153 | 5 / 5 |
| `ac1_b256` | 188.644 ± 11.688 | 5 / 5 |
| `ac1_b512` | 185.808 ± 2.187 | 5 / 5 |
| `ac1_b1024` | 199.751 ± 2.746 | 5 / 5 |
| `ac1_b2048` | 199.776 ± 2.075 | 5 / 5 |
| `ac2_b64` | 119.430 ± 26.916 | 5 / 5 |
| `ac2_b128` | 181.393 ± 24.120 | 5 / 5 |
| `ac2_b256` | 189.751 ± 12.696 | 5 / 5 |
| `ac2_b512` | 199.625 ± 8.806 | 5 / 5 |
| `ac2_b1024` | 203.580 ± 6.387 | 5 / 5 |
| `ac2_b2048` | 204.785 ± 5.052 | 5 / 5 |
| `ac3_b64` | 123.113 ± 23.716 | 5 / 5 |
| `ac3_b128` | 137.842 ± 27.996 | 5 / 5 |
| `ac3_b256` | 174.487 ± 13.808 | 5 / 5 |
| `ac3_b512` | 190.889 ± 5.980 | 5 / 5 |
| `ac3_b1024` | 196.900 ± 6.661 | 5 / 5 |
| `ac3_b2048` | 205.027 ± 4.580 | 5 / 5 |

**Fixed-width configurations:** `headline` is four small producers, eight
consumers, one-word bodies, batch 1700, chunk 128 and two billion payloads;
`headline_default_chunk` changes the chunk to 16; `single_producer` uses one
small producer and eleven consumers; `default_shape` is four consumers, a bulk
producer, 16-word bodies, batch 80 and 16 million payloads.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `headline` | 2,272.981 ± 64.710 | 5 / 5 |
| `headline_default_chunk` | 1,714.224 ± 100.329 | 5 / 5 |
| `single_producer` | 887.955 ± 6.858 | 5 / 5 |
| `default_shape` | 130.056 ± 25.365 | 5 / 5 |

**Dual-role:** `nd` threads each produce and consume (body 16, batch 256, one
consume attempt per turn, cost class 3 unless the suffix says otherwise).
Six dual-role threads are six workers in total, not six dedicated consumers.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `nd1` | 131.915 ± 4.032 | 5 / 5 |
| `nd2` | 221.850 ± 23.271 | 5 / 5 |
| `nd4` | 244.388 ± 59.622 | 5 / 5 |
| `nd6` | 294.458 ± 60.301 | 5 / 5 |
| `nd8` | 327.842 ± 43.859 | 5 / 5 |
| `nd11` | 201.077 ± 108.636 | 5 / 5 |
| `nd6_b256_ac0` | 336.327 ± 38.587 | 5 / 5 |
| `nd6_b2048_ac0` | 288.721 ± 118.165 | 5 / 5 |
| `nd6_b256_ac3` | 243.680 ± 94.709 | 5 / 5 |

### Public actors

Lifecycle modes report millions of complete lifecycles per second; `actor`,
`inbox` and `wave` at zero and six consumers with the default public chunk (8)
and with chunk 32 (`ANTFARM_PUBLIC_HINT=2`), then the `actor` consumer curve.
`actor_ready` is different: autonomous dispatch of a prequeued burst, in
millions of activations per second, with construction and reclamation outside
the timed region.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `actor_c0_default` | 22.702 ± 0.153 | 5 / 5 |
| `actor_c0_chunk32` | 23.026 ± 0.081 | 5 / 5 |
| `actor_c6_default` | 22.887 ± 0.082 | 5 / 5 |
| `actor_c6_chunk32` | 22.868 ± 0.061 | 5 / 5 |
| `wave_c0_default` | 27.702 ± 0.046 | 5 / 5 |
| `wave_c0_chunk32` | 27.922 ± 0.034 | 5 / 5 |
| `wave_c6_default` | 24.596 ± 0.061 | 5 / 5 |
| `wave_c6_chunk32` | 24.329 ± 0.042 | 5 / 5 |
| `inbox_c0_default` | 18.208 ± 0.035 | 5 / 5 |
| `inbox_c0_chunk32` | 18.377 ± 0.075 | 5 / 5 |
| `inbox_c6_default` | 18.350 ± 0.029 | 5 / 5 |
| `inbox_c6_chunk32` | 18.367 ± 0.028 | 5 / 5 |
| `actor_curve_c1` | 23.248 ± 0.057 | 5 / 5 |
| `actor_curve_c2` | 23.401 ± 0.023 | 5 / 5 |
| `actor_curve_c4` | 23.585 ± 0.059 | 5 / 5 |
| `actor_curve_c6` | 22.788 ± 0.056 | 5 / 5 |
| `actor_curve_c11` | 15.482 ± 0.046 | 5 / 5 |
| `actor_ready_default` | 76.230 ± 0.678 | 5 / 5 |
| `actor_ready_chunk32` | 78.030 ± 0.716 | 5 / 5 |

### Private actors

Lifecycle modes report millions of complete lifecycles per second. Subset
modes report millions of selected callbacks per second, a different
denominator. The [mode definitions](tools/perf/README.md#actor_churn)
distinguish tracked roots, consumer-side free, child trees, continuations,
refill, cancellation and subset waves; `-slab` modes use cohort slabs, and
`private-slab` / `private-remote-slab` use root slabs.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `private_c0` | 90.102 ± 0.141 | 5 / 5 |
| `private_c6` | 92.238 ± 0.443 | 5 / 5 |
| `private-remote_c0` | 91.611 ± 0.318 | 5 / 5 |
| `private-remote_c6` | 16.839 ± 0.010 | 5 / 5 |
| `private-tracked_c0` | 23.351 ± 0.055 | 5 / 5 |
| `private-tracked_c6` | 40.196 ± 0.155 | 5 / 5 |
| `private-slab_c0` | 122.474 ± 0.334 | 5 / 5 |
| `private-slab_c6` | 119.220 ± 0.311 | 5 / 5 |
| `private-remote-slab_c0` | 120.764 ± 1.273 | 5 / 5 |
| `private-remote-slab_c6` | 116.341 ± 0.227 | 5 / 5 |
| `tree_c0` | 95.562 ± 2.501 | 5 / 5 |
| `tree_c6` | 81.208 ± 0.235 | 5 / 5 |
| `tree-wait_c0` | 95.881 ± 2.229 | 5 / 5 |
| `tree-wait_c6` | 81.101 ± 0.206 | 5 / 5 |
| `tree-cont_c0` | 37.146 ± 0.443 | 5 / 5 |
| `tree-cont_c6` | 41.876 ± 0.059 | 5 / 5 |
| `tree-slab_c0` | 136.830 ± 0.666 | 5 / 5 |
| `tree-slab_c6` | 321.221 ± 3.218 | 5 / 5 |
| `tree-wait-slab_c0` | 136.081 ± 0.888 | 5 / 5 |
| `tree-wait-slab_c6` | 311.942 ± 1.176 | 5 / 5 |
| `tree-cont-slab_c0` | 42.646 ± 0.056 | 5 / 5 |
| `tree-cont-slab_c6` | 42.826 ± 0.053 | 5 / 5 |
| `tree-refill_c0` | 108.009 ± 3.604 | 5 / 5 |
| `tree-refill_c6` | 83.796 ± 0.326 | 5 / 5 |
| `tree-refill-slab_c0` | 165.971 ± 0.818 | 5 / 5 |
| `tree-refill-slab_c6` | 427.924 ± 0.880 | 5 / 5 |
| `tree-pipelined_c0` | 39.797 ± 0.077 | 5 / 5 |
| `tree-pipelined_c6` | 49.612 ± 0.095 | 5 / 5 |
| `tree-cancel_c0` | 63.348 ± 0.290 | 5 / 5 |
| `tree-cancel_c6` | 77.487 ± 0.285 | 5 / 5 |
| `dtor_c0` | 92.352 ± 0.300 | 5 / 5 |
| `dtor_c6` | 96.387 ± 0.296 | 5 / 5 |
| `subset1_c0` | 188.042 ± 0.438 | 5 / 5 |
| `subset1_c6` | 82.868 ± 0.088 | 5 / 5 |
| `subset10_c0` | 259.190 ± 0.737 | 5 / 5 |
| `subset10_c6` | 299.547 ± 0.459 | 5 / 5 |
| `subset100_c0` | 272.498 ± 6.531 | 5 / 5 |
| `subset100_c6` | 411.112 ± 0.569 | 5 / 5 |
| `subset-whole_c0` | 456.665 ± 5.503 | 5 / 5 |
| `subset-whole_c6` | 1,147.984 ± 1.880 | 5 / 5 |
| `subset-whole-unreserved_c0` | 455.757 ± 8.586 | 5 / 5 |
| `subset-whole-unreserved_c6` | 1,147.789 ± 3.073 | 5 / 5 |

#### Cohort slab vs per-object

Compare each tree or refill mode with its `-slab` partner at the same consumer
count above, and the two root modes with their `-slab` partners. Slab
reservation happens at creation and member storage is reused; these are
complete workload rates, not allocator-call timings. At six consumers the
child tree gains four times (81 to 321) and the refilled tree five times (84
to 428); consumer-freed roots gain seven times (17 to 116). Cancellation's
dispatched fraction is retained in the machine-readable counters.

#### Batch limit and work-cost hints

All cells use six consumers. `b` changes the batch limit; `auto`, `h2` and
`h0` select automatic root classification, chunk 32 and chunk 128. The `child`
cases change the child-wave hint instead. Choose defaults against the relevant
workload rather than the largest rate across unlike cases.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `tree_b256_auto` | 80.881 ± 0.249 | 5 / 5 |
| `tree_b256_h2` | 65.320 ± 0.110 | 5 / 5 |
| `tree_b256_h0` | 66.170 ± 0.140 | 5 / 5 |
| `tree_b1024_auto` | 95.530 ± 0.145 | 5 / 5 |
| `tree_b1024_h2` | 18.600 ± 0.191 | 5 / 5 |
| `tree_b1024_h0` | 18.504 ± 0.197 | 5 / 5 |
| `private_b256_auto` | 91.893 ± 0.195 | 5 / 5 |
| `private_b256_h2` | 91.897 ± 0.137 | 5 / 5 |
| `private_b256_h0` | 92.842 ± 0.246 | 5 / 5 |
| `private_b1024_auto` | 89.430 ± 0.857 | 5 / 5 |
| `private_b1024_h2` | 89.722 ± 0.423 | 5 / 5 |
| `private_b1024_h0` | 92.444 ± 0.199 | 5 / 5 |
| `private_b256` | 92.042 ± 0.403 | 5 / 5 |
| `private_b1024` | 89.839 ± 2.297 | 5 / 5 |
| `private_b2048` | 84.768 ± 0.571 | 5 / 5 |
| `tree_child_default` | 80.885 ± 0.116 | 5 / 5 |
| `tree_child_h0` | 80.864 ± 0.246 | 5 / 5 |
| `tree-slab_child_default` | 315.515 ± 3.272 | 5 / 5 |
| `tree-slab_child_h0` | 319.512 ± 2.636 | 5 / 5 |

### Allocators

Six consumers. CRT/mimalloc comparisons include the complete lifecycle.
`private_emitter` frees on the emitter; `private-remote` frees on consumers.
The `count` cases enable allocation instrumentation and are diagnostics, not
uninstrumented throughput; exact cross-thread counts are in the data file.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `actor_mimalloc` | 22.766 ± 0.047 | 5 / 5 |
| `actor_crt` | 9.413 ± 0.024 | 5 / 5 |
| `wave_mimalloc` | 24.593 ± 0.056 | 5 / 5 |
| `wave_crt` | 9.873 ± 0.111 | 5 / 5 |
| `tree_mimalloc` | 80.977 ± 0.228 | 5 / 5 |
| `tree_crt` | 18.368 ± 0.174 | 5 / 5 |
| `private-remote_mimalloc` | 16.782 ± 0.029 | 5 / 5 |
| `private-remote_crt` | 5.962 ± 0.099 | 5 / 5 |
| `private_emitter` | 92.009 ± 0.315 | 5 / 5 |
| `private_count` | 10.588 ± 0.016 | 5 / 5 |
| `private_emitter_count` | 60.956 ± 0.171 | 5 / 5 |

### Page backing and pinning

All results use ordinary pages. Pinned cells use controller 0 and consumers
1-6; unpinned cells leave placement to the OS. Pinning changes the median
little here and the spread a lot: the unpinned standard deviations are 40 to
100 times the pinned ones.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `actor_c6_pinned` | 22.811 ± 0.043 | 5 / 5 |
| `actor_c6_unpinned` | 22.619 ± 4.565 | 5 / 5 |
| `tree_c6_pinned` | 80.857 ± 0.407 | 5 / 5 |
| `tree_c6_unpinned` | 79.747 ± 17.702 | 5 / 5 |

### Fibers

`benchmark_st` is single-thread drain throughput in fibers per second (200,000
fibers, 256-entry batches, two generations). `benchmark_mt` is the cold full
burst, creation plus drain, with the pool's default of one worker per core on
every group; the warm burst and drain at each worker count are in the
[headline](#headline-throughput) and the [thread sweeps](#thread-sweeps). The
[historical Fiber characterization](docs/performance/archive-2026-10-20.md#fibers)
retains the same-thread, worker-topology, embedding, allocation and
assist-ring experiments.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `benchmark_st` | 5,226,749 ± 35,819 | 5 / 5 |
| `benchmark_mt` | 187,868 ± 1,884 | 5 / 5 |

### Scaling microbenchmarks

Elapsed milliseconds; lower is better. 16,384 creations, and lifecycle work
with batch 32 and handles.

| Case | Median ± stddev | Samples |
| --- | ---: | ---: |
| `create_16384` | 1.127 ± 0.077 | 5 / 5 |
| `lifecycle_16384_b32` | 1.016 ± 0.080 | 5 / 5 |

### Full tail-latency matrix

Every scene the tail harness runs, at each thread count: idle; mid-drain
behind tables of 32, 256, 2,048 and 8,192 entries with 0, 1 or 10 µs of work
per job; burst (32 sentinels back to back); and near-full, where the ring is
filled while consumers are parked and the sentinel is written against
backpressure. The near-full p99.9 of about 1.5 ms is the wait for ring space,
not consumer latency.

| Threads | Scene | Table | Work | p50 µs | p99 µs | p99.9 µs | Max µs | Processes |
| ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 2 | burst | 256 | 1us | 250.30 ± 2.33 | 255.80 ± 2.19 | 264.30 ± 4.59 | 265.40 ± 4.58 | 5 / 5 |
| 2 | idle | 0 | - | 0.14 ± 0.00 | 1.80 ± 0.04 | 3.20 ± 0.05 | 6.30 ± 5.15 | 5 / 5 |
| 2 | mid-drain | 32 | 1us | 16.80 ± 0.16 | 19.20 ± 0.15 | 20.40 ± 0.28 | 32.70 ± 4.80 | 5 / 5 |
| 2 | mid-drain | 256 | 0ns | 2.50 ± 0.00 | 2.90 ± 0.00 | 5.50 ± 0.63 | 8.50 ± 4.07 | 5 / 5 |
| 2 | mid-drain | 256 | 10us | 1,930.00 ± 0.00 | 1,940.00 ± 0.00 | 1,950.00 ± 0.00 | 1,970.00 ± 66.56 | 5 / 5 |
| 2 | mid-drain | 256 | 1us | 204.80 ± 1.39 | 208.20 ± 1.31 | 213.50 ± 1.58 | 240.50 ± 15.78 | 5 / 5 |
| 2 | mid-drain | 2048 | 1us | 1,630.00 ± 13.42 | 1,650.00 ± 5.48 | 1,650.00 ± 5.48 | 1,670.00 ± 4.47 | 5 / 5 |
| 2 | mid-drain | 8192 | 1us | 6,450.00 ± 52.15 | 6,560.00 ± 10.00 | 6,570.00 ± 8.37 | 6,580.00 ± 21.91 | 5 / 5 |
| 2 | near-full | 256 | 0ns | 0.14 ± 0.01 | 1.70 ± 0.24 | 1,440.00 ± 144.84 | 1,920.00 ± 317.69 | 5 / 5 |
| 6 | burst | 256 | 1us | 26.80 ± 0.58 | 30.50 ± 0.24 | 32.80 ± 0.65 | 34.10 ± 2.32 | 5 / 5 |
| 6 | idle | 0 | - | 0.23 ± 0.00 | 2.00 ± 0.08 | 4.30 ± 1.69 | 6.60 ± 41.77 | 5 / 5 |
| 6 | mid-drain | 32 | 1us | 0.20 ± 0.00 | 1.70 ± 0.04 | 3.20 ± 0.07 | 6.30 ± 2.49 | 5 / 5 |
| 6 | mid-drain | 256 | 0ns | 0.70 ± 0.02 | 2.60 ± 0.09 | 3.20 ± 0.09 | 5.40 ± 0.37 | 5 / 5 |
| 6 | mid-drain | 256 | 10us | 30.20 ± 0.00 | 191.50 ± 0.04 | 352.30 ± 70.55 | 352.70 ± 0.81 | 5 / 5 |
| 6 | mid-drain | 256 | 1us | 3.20 ± 0.04 | 20.40 ± 0.09 | 22.70 ± 0.23 | 36.90 ± 0.85 | 5 / 5 |
| 6 | mid-drain | 2048 | 1us | 10.60 ± 0.43 | 266.70 ± 1.32 | 269.00 ± 0.40 | 270.60 ± 1.11 | 5 / 5 |
| 6 | mid-drain | 8192 | 1us | 8.30 ± 0.93 | 1,030.00 ± 5.48 | 1,040.00 ± 4.47 | 1,040.00 ± 17.89 | 5 / 5 |
| 6 | near-full | 256 | 0ns | 0.24 ± 0.01 | 0.33 ± 0.02 | 1,570.00 ± 87.64 | 1,610.00 ± 21.68 | 5 / 5 |
| 12 | burst | 256 | 1us | 10.90 ± 0.05 | 15.40 ± 0.11 | 18.40 ± 0.34 | 27.80 ± 1.72 | 5 / 5 |
| 12 | idle | 0 | - | 0.31 ± 0.00 | 2.60 ± 0.05 | 6.70 ± 0.57 | 10.80 ± 33.78 | 5 / 5 |
| 12 | mid-drain | 32 | 1us | 0.32 ± 0.00 | 2.20 ± 0.00 | 4.30 ± 0.11 | 9.40 ± 128.56 | 5 / 5 |
| 12 | mid-drain | 256 | 0ns | 1.60 ± 0.00 | 2.10 ± 0.00 | 4.50 ± 0.43 | 5.80 ± 0.26 | 5 / 5 |
| 12 | mid-drain | 256 | 10us | 100.60 ± 0.00 | 103.70 ± 0.08 | 104.70 ± 0.11 | 105.40 ± 0.11 | 5 / 5 |
| 12 | mid-drain | 256 | 1us | 10.50 ± 0.00 | 12.70 ± 0.65 | 14.40 ± 0.11 | 15.30 ± 124.87 | 5 / 5 |
| 12 | mid-drain | 2048 | 1us | 1.10 ± 0.05 | 120.30 ± 0.10 | 123.30 ± 0.43 | 124.70 ± 0.42 | 5 / 5 |
| 12 | mid-drain | 8192 | 1us | 5.70 ± 0.05 | 465.10 ± 0.85 | 469.90 ± 0.40 | 471.30 ± 0.70 | 5 / 5 |
| 12 | near-full | 256 | 0ns | 0.29 ± 0.01 | 2.30 ± 0.09 | 1,390.00 ± 577.50 | 1,670.00 ± 301.41 | 5 / 5 |
| 20 | burst | 256 | 1us | 1.40 ± 0.05 | 6.50 ± 0.68 | 9.50 ± 4.04 | 23.10 ± 13.65 | 5 / 5 |
| 20 | idle | 0 | - | 0.60 ± 0.01 | 2.80 ± 0.18 | 11.70 ± 1.80 | 53.80 ± 28.24 | 5 / 5 |
| 20 | mid-drain | 32 | 1us | 0.41 ± 0.01 | 2.40 ± 0.04 | 4.20 ± 0.17 | 9.70 ± 33.04 | 5 / 5 |
| 20 | mid-drain | 256 | 0ns | 0.62 ± 0.01 | 1.60 ± 0.05 | 3.20 ± 0.13 | 5.40 ± 128.87 | 5 / 5 |
| 20 | mid-drain | 256 | 10us | 0.44 ± 0.01 | 1.10 ± 0.28 | 2.60 ± 0.11 | 4.90 ± 1.33 | 5 / 5 |
| 20 | mid-drain | 256 | 1us | 0.42 ± 0.00 | 1.50 ± 0.28 | 2.60 ± 0.47 | 5.10 ± 1.03 | 5 / 5 |
| 20 | mid-drain | 2048 | 1us | 5.00 ± 0.04 | 79.00 ± 0.16 | 81.30 ± 0.17 | 83.00 ± 3.23 | 5 / 5 |
| 20 | mid-drain | 8192 | 1us | 4.40 ± 0.05 | 298.10 ± 0.44 | 300.30 ± 2.02 | 313.50 ± 26.15 | 5 / 5 |
| 20 | near-full | 256 | 0ns | 0.61 ± 0.02 | 2.10 ± 766.27 | 1,610.00 ± 19.49 | 1,610.00 ± 0.00 | 5 / 5 |
| 32 | burst | 256 | 1us | 0.88 ± 0.06 | 5.50 ± 0.67 | 9.60 ± 0.81 | 13.40 ± 13.10 | 5 / 5 |
| 32 | idle | 0 | - | 0.73 ± 0.01 | 3.60 ± 0.27 | 18.30 ± 0.67 | 69.80 ± 43.35 | 5 / 5 |
| 32 | mid-drain | 32 | 1us | 0.73 ± 0.01 | 3.00 ± 0.05 | 5.00 ± 0.24 | 9.00 ± 47.63 | 5 / 5 |
| 32 | mid-drain | 256 | 0ns | 0.67 ± 0.01 | 1.50 ± 0.13 | 3.20 ± 0.42 | 5.40 ± 7.92 | 5 / 5 |
| 32 | mid-drain | 256 | 10us | 0.59 ± 0.01 | 1.20 ± 0.00 | 3.40 ± 0.27 | 5.50 ± 0.38 | 5 / 5 |
| 32 | mid-drain | 256 | 1us | 0.67 ± 0.02 | 1.30 ± 0.07 | 3.30 ± 0.19 | 5.80 ± 4.68 | 5 / 5 |
| 32 | mid-drain | 2048 | 1us | 16.50 ± 0.08 | 19.30 ± 0.22 | 30.30 ± 13.65 | 53.70 ± 1.80 | 5 / 5 |
| 32 | mid-drain | 8192 | 1us | 15.20 ± 0.08 | 200.20 ± 0.54 | 212.00 ± 10.20 | 236.20 ± 9.40 | 5 / 5 |
| 32 | near-full | 256 | 0ns | 0.70 ± 0.01 | 1.30 ± 1.03 | 1,610.00 ± 101.00 | 1,620.00 ± 348.24 | 5 / 5 |

## Follow-up

- Windows host: rebuild the 12700H tables with this driver and the shorter
  thread lists.
- Public actors and private roots are emitter-bound from about 4 and 8
  threads: the single flush or pump thread saturates. Batching more work per
  publication, or a second emitter, is the lever; the free-index-stack
  redesign for public reservation stays deferred.
- The first consumer on the second CCD costs private child throughput more
  than it adds; keep a workload inside one CCD, or give each CCD its own Farm.
