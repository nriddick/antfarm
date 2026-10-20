# Ant Farm performance

Fresh Intel i7-12700H baseline measured on October 20, 2026, at runtime revision `2cf59da` on `1.9.0-progress`. These measurements replace the old 12700H tables. **Ryzen 9 5900XT measurements are pending**; no cross-host comparison is implied.

**Follow-up qualification:** the ~503 Mpps headline below did not reproduce later with the **same saved executable**: it then delivered 2,183 Mpps under WSL2. Matched 1.8.3/current comparisons also reached 2,245/2,291 Mpps on native Windows and 2,123/2,186 Mpps under WSL2. The earlier session therefore cannot establish a slowdown caused by MT removal, nor a general WSL penalty. Its environmental slowdown remains unexplained. The original observations are retained below; see the [matched revision checks and exact-binary control](docs/performance/mt-excision-check-2026-10-20.md) before interpreting them as a representative baseline.

The [previous performance page](docs/performance/archive-2026-10-20.md) preserves the old Ryzen 5500 and native-Windows 12700H measurements, the detailed Fiber experiments, and the intervening 1.9 optimization reports with their original limitations. Those results are historical evidence, not rows in this baseline.

This is the baseline **before the proposed free-index-stack redesign**. Public actor reservation still walks at most 64 free-list links per gate hold. No runtime implementation changed for this refresh.

## Method

| Property | This run |
| --- | --- |
| CPU / environment | Intel i7-12700H; WSL2 Linux 6.6.87.2; 20 allowed logical CPUs |
| Compiler | LDC 1.42.0, LLVM 21.1.8 |
| Build modes | Standalone D tools and C ABI D code: `-O2 -release`; Fibers: DUB release; C driver: `cc -O2` |
| Windows host power plan | Balanced; guest governor unavailable; battery/thermal state not recorded |
| Actor allocator | mimalloc 3.5.3 unless the case says CRT or counted diagnostic |
| Page policy | `ANTFARM_HUGE_PAGES=0`; ordinary-page baseline |
| Samples | 5 per cell; 30 for six-consumer actor and throughput cells; tail retains 5 processes × 10,000 observations per scene |
| Aggregation | Median [minimum, maximum] of successful independent process samples |
| Background-load policy | No CPU/load filtering; load averages retained in raw logs |
| Validity | Nonzero exit, timeout or missing primary metric fails a sample; tail-scene completeness is reported separately |
| Actor workload | 16,384 actors, 3 seconds per sample, batch 256, 5 cohort warmups unless named otherwise |

Actor runs with workers pin the controller to logical CPU 0 and consumers to 1 through N; serial cases are unpinned. Throughput and dual-role runs are unpinned. Tail placement comes from the topology visible inside WSL and is printed in each raw capture. WSL exposes a virtualized topology: these CPU lists must not be described as distinct physical P cores. Six dual-role threads are six total workers, not six dedicated consumers.

Ranges describe observed variation, not confidence intervals. This is descriptive baseline characterization, not a paired regression gate, a C/D equivalence test, or a controlled comparison with the archived Windows results. Use the [committed-ref optimization gate](tools/perf/README.md#optimization-gate) for changes.
The callbacks are synthetic and deliberately small. Adding consumers can expose coordination cost rather than increase throughput; these worker sweeps do not predict application speedups.

The [machine-readable results](docs/performance/12700h-2026-10-20.json) retain individual sample values, exact commands, overrides, parsed counters and host identity. Full raw output, host metadata and local evidence are under `.build/performance-12700h-2026-10-20/`.

Reproduce with the current harness and the pinned allocator built:

```sh
make -C tools/perf SHELL=/usr/bin/bash
make -C tools/perf SHELL=/usr/bin/bash actor_churn_mimalloc
python3 tools/perf/run_performance_md.py \
  --out .build/performance-12700h-refresh --runs 5 --six-consumer-runs 30 \
  --sections host,farm_consumers,farm_chunk_batch,farm_best,dual,public,private,private_tuning,allocators,pinning,fibers,scaling
python3 tools/perf/run_performance_md.py \
  --out .build/performance-12700h-refresh --sections tail --runs 5
python3 tools/perf/run_actor_cabi.py \
  --output .build/performance-12700h-cabi-refresh --repeats 5 --seconds 0.4
```

Use a fresh output directory for a new revision or rebuilt binaries: resume checks command, environment and sample count, not runtime identity. Huge-page, off-LLC and expanded 12/18/20-thread sweeps are outside this run. The harness [mode index](tools/perf/README.md) defines the workloads.

## Farm core

Rates are millions of completed payloads per second (Mpps), not application work items unless one payload represents one item. The callback uses per-worker batched accounting. The ring is 16 MiB. These are current ST tables; the retired MT path is only in the archive.

**Writer follow-up:** `651030b` restores the layout-specific sizing scans for
inferred writes, including `write(callback, bodies)` and C's `af_write_bodies`.
The [matched writer measurements](docs/performance/writer-scan-2026-10-20.md)
recover the inferred-path loss against 1.8.3; the explicit-length overload was
already specialized. The baseline rows below remain observations at `2cf59da`,
before this restoration.

### Throughput vs consumers

One bulk producer, 16-word bodies, batch 80. `nc` counts consumers.

| Case | Mpps, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `nc1` | 144.598 [143.076, 146.020] | 5 / 5 |
| `nc2` | 134.729 [125.011, 137.126] | 5 / 5 |
| `nc4` | 94.374 [90.332, 107.015] | 5 / 5 |
| `nc6` | 46.998 [30.401, 94.491] | 30 / 30 |
| `nc11` | 25.265 [23.527, 27.908] | 5 / 5 |

### Chunk and batch

Four consumers, one bulk producer, 16-word bodies. `ac0..3` select chunks 128, 64, 32 and 16; `b` is the write batch.

| Case | Mpps, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `ac0_b64` | 41.342 [34.713, 42.337] | 5 / 5 |
| `ac0_b128` | 57.773 [52.274, 61.760] | 5 / 5 |
| `ac0_b256` | 50.323 [48.556, 56.788] | 5 / 5 |
| `ac0_b512` | 53.556 [48.364, 55.591] | 5 / 5 |
| `ac0_b1024` | 55.960 [51.937, 58.979] | 5 / 5 |
| `ac0_b2048` | 44.427 [43.859, 55.716] | 5 / 5 |
| `ac1_b64` | 38.343 [34.720, 46.245] | 5 / 5 |
| `ac1_b128` | 50.667 [44.212, 55.540] | 5 / 5 |
| `ac1_b256` | 48.303 [43.236, 54.085] | 5 / 5 |
| `ac1_b512` | 49.413 [42.039, 52.199] | 5 / 5 |
| `ac1_b1024` | 50.315 [49.324, 53.587] | 5 / 5 |
| `ac1_b2048` | 54.402 [50.391, 57.207] | 5 / 5 |
| `ac2_b64` | 44.708 [41.292, 48.100] | 5 / 5 |
| `ac2_b128` | 56.018 [52.351, 57.059] | 5 / 5 |
| `ac2_b256` | 60.046 [52.200, 64.016] | 5 / 5 |
| `ac2_b512` | 54.957 [51.226, 61.557] | 5 / 5 |
| `ac2_b1024` | 57.738 [52.434, 60.277] | 5 / 5 |
| `ac2_b2048` | 56.481 [52.972, 62.070] | 5 / 5 |
| `ac3_b64` | 44.733 [36.945, 45.953] | 5 / 5 |
| `ac3_b128` | 55.968 [53.617, 57.498] | 5 / 5 |
| `ac3_b256` | 48.255 [46.776, 56.886] | 5 / 5 |
| `ac3_b512` | 52.858 [48.467, 56.543] | 5 / 5 |
| `ac3_b1024` | 53.700 [48.851, 54.167] | 5 / 5 |
| `ac3_b2048` | 51.348 [48.965, 57.175] | 5 / 5 |

### Fixed-width configurations

`headline` is the historical case name, not a promised rate: four small producers, eight consumers, one-word bodies, batch 1700, chunk 128 and two billion payloads. `headline_default_chunk` changes the chunk to 16; `single_producer` uses one small producer and eleven consumers. `default_shape` uses four consumers, a bulk producer, 16-word bodies, batch 80 and 16 million payloads.

| Case | Mpps, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `headline` | 503.212 [485.906, 520.568] | 5 / 5 |
| `headline_default_chunk` | 405.682 [358.254, 419.167] | 5 / 5 |
| `single_producer` | 165.916 [161.517, 171.950] | 5 / 5 |
| `default_shape` | 42.339 [33.410, 44.655] | 5 / 5 |

### Dual-role

`nd` counts total producer/consumer workers. Defaults: body 16, batch 256, one consume attempt per turn and cost class 3. Explicit suffixes override batch and class.

| Case | Mpps, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `nd1` | 39.681 [33.209, 40.898] | 5 / 5 |
| `nd2` | 72.828 [68.517, 77.115] | 5 / 5 |
| `nd4` | 116.578 [112.731, 119.197] | 5 / 5 |
| `nd6` | 115.540 [108.777, 123.447] | 5 / 5 |
| `nd8` | 108.370 [96.634, 115.885] | 5 / 5 |
| `nd11` | 112.918 [106.315, 118.090] | 5 / 5 |
| `nd6_b256_ac0` | 122.730 [112.786, 134.172] | 5 / 5 |
| `nd6_b2048_ac0` | 151.558 [144.741, 157.807] | 5 / 5 |
| `nd6_b256_ac3` | 132.717 [132.627, 136.918] | 5 / 5 |

### Tail latency

Each cell uses five processes and requests 10,000 sentinel samples per scene in each process. Figures below are medians of each process's latency quantile, **not pooled quantiles**. Units are nanoseconds. Raw captures retain missed backlog windows and zero counts.

| Cell / scene | Table length | Work spin | Complete / attempted processes | p50 ns | p99 ns | p99.9 ns | Median process max ns |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| `mid_default` / mailbox | 256 | 1us | 0 / 5 **incomplete** | — | — | — | — |
| `mid_default` / mailbox | 8192 | 1us | 5 / 5 | 433 | 15,100 | 86,000 | 213,300 |
| `mid_default` / mid-drain | 32 | 1us | 5 / 5 | 672 | 3,700 | 7,400 | 79,200 |
| `mid_default` / mid-drain | 256 | 0ns | 5 / 5 | 3,100 | 8,100 | 10,600 | 53,300 |
| `mid_default` / mid-drain | 256 | 10us | 5 / 5 | 111,800 | 351,900 | 436,100 | 489,500 |
| `mid_default` / mid-drain | 256 | 1us | 5 / 5 | 8,500 | 30,000 | 46,400 | 79,000 |
| `mid_default` / mid-drain | 2048 | 1us | 5 / 5 | 12,200 | 274,000 | 328,200 | 425,700 |
| `mid_default` / mid-drain | 8192 | 1us | 5 / 5 | 11,200 | 992,800 | 1,060,000 | 1,230,000 |
| `idle_default` / idle | 0 | - | 5 / 5 | 504 | 7,600 | 43,300 | 181,800 |

A scene with fewer than five complete processes is **incomplete**; any displayed quantiles use only the available observations and do not establish a complete baseline. A mailbox timeout can occur inside a process whose primary mid-drain measurement succeeds. No latency scene is silently promoted to complete based on that primary metric.

## Public actors

Rates are million complete actor lifecycles/s, including creation, dispatch, retirement and reclamation. `actor`, `wave` and `inbox` change dispatch. `c` counts consumers; `chunk32` overrides the public hint to class 2. Ready-only rows below exclude lifecycle work.

| Case | Mcycles/s, median [min, max] | Samples |
| --- | ---: | ---: |
| `actor_c0_default` | 10.970 [8.246, 11.516] | 5 |
| `actor_c0_chunk32` | 8.683 [8.467, 8.852] | 5 |
| `actor_c6_default` | 5.621 [4.714, 6.412] | 30 |
| `actor_c6_chunk32` | 5.834 [5.299, 6.032] | 30 |
| `wave_c0_default` | 10.420 [7.648, 10.840] | 5 |
| `wave_c0_chunk32` | 10.611 [9.985, 10.757] | 5 |
| `wave_c6_default` | 6.307 [5.478, 6.925] | 30 |
| `wave_c6_chunk32` | 6.161 [5.716, 6.374] | 30 |
| `inbox_c0_default` | 6.391 [5.636, 6.432] | 5 |
| `inbox_c0_chunk32` | 6.197 [6.094, 6.336] | 5 |
| `inbox_c6_default` | 4.285 [3.837, 4.415] | 30 |
| `inbox_c6_chunk32` | 4.348 [3.964, 4.445] | 30 |
| `actor_curve_c1` | 7.903 [7.396, 7.968] | 5 |
| `actor_curve_c2` | 7.568 [7.404, 7.657] | 5 |
| `actor_curve_c4` | 6.401 [6.189, 6.861] | 5 |
| `actor_curve_c6` | 5.741 [5.342, 5.871] | 30 |
| `actor_curve_c11` | 4.572 [4.478, 4.694] | 5 |

### C ABI lifecycle

Matched C/D create-copy → idle retire → reclaim microbenchmark. Each actor copies 64 or 4096 bytes into its final state, with 64-byte alignment and cohorts of 64. One or four creator threads use local reclamation or a separate reclaimer. Five alternating-order timing repeats target 0.4 seconds per mode; separate counted runs check allocations. Timing excludes setup and includes barrier release and final joins. This measures lifecycle overhead, not callback/wave throughput or a regression gate.

All 48 separate counted runs confirmed one final-state allocation and one free per actor, with exactly the requested state bytes allocated. Timing runs disable these counters.

| State bytes | Creators | Reclaim | Allocator | D scalar ns/actor | C scalar ns/actor | C batch ns/actor |
| ---: | ---: | --- | --- | ---: | ---: | ---: |
| 64 | 1 | local | CRT | 125.217 [121.529, 130.632] | 140.606 [136.840, 145.804] | 115.354 [113.925, 116.226] |
| 64 | 1 | local | mimalloc | 60.613 [58.815, 63.888] | 76.056 [74.787, 78.628] | 50.982 [49.522, 53.859] |
| 64 | 1 | remote | CRT | 187.033 [181.292, 191.127] | 218.063 [196.478, 222.234] | 213.994 [200.620, 217.393] |
| 64 | 1 | remote | mimalloc | 100.993 [96.602, 112.636] | 121.339 [117.269, 133.177] | 112.871 [108.772, 116.056] |
| 64 | 4 | local | CRT | 441.489 [401.545, 468.155] | 444.763 [413.992, 524.459] | 209.130 [183.842, 210.090] |
| 64 | 4 | local | mimalloc | 467.327 [414.037, 475.239] | 443.007 [431.099, 453.908] | 198.518 [190.640, 211.670] |
| 64 | 4 | remote | CRT | 654.923 [552.071, 674.576] | 654.671 [563.394, 738.468] | 233.040 [212.997, 258.752] |
| 64 | 4 | remote | mimalloc | 576.621 [536.915, 587.045] | 608.300 [539.992, 647.174] | 257.167 [230.670, 323.355] |
| 4096 | 1 | local | CRT | 244.563 [216.749, 342.471] | 257.262 [235.642, 322.055] | 227.531 [202.741, 286.294] |
| 4096 | 1 | local | mimalloc | 181.317 [174.299, 192.224] | 203.836 [197.632, 208.744] | 175.709 [170.862, 190.361] |
| 4096 | 1 | remote | CRT | 1,085.138 [1,014.487, 1,163.752] | 1,102.571 [1,083.850, 1,118.227] | 1,098.647 [1,073.166, 1,162.017] |
| 4096 | 1 | remote | mimalloc | 268.700 [250.253, 309.929] | 272.975 [265.786, 302.774] | 282.990 [277.702, 308.749] |
| 4096 | 4 | local | CRT | 521.513 [425.630, 552.846] | 513.551 [460.345, 538.479] | 170.512 [130.336, 203.952] |
| 4096 | 4 | local | mimalloc | 606.195 [559.305, 653.043] | 630.371 [583.194, 660.941] | 251.102 [235.344, 264.546] |
| 4096 | 4 | remote | CRT | 843.351 [791.366, 975.544] | 882.170 [793.472, 1,006.393] | 856.840 [728.214, 886.474] |
| 4096 | 4 | remote | mimalloc | 650.509 [591.372, 829.675] | 638.207 [589.712, 742.763] | 289.324 [270.434, 344.032] |

### Prequeued ready dispatch

These are D ready-dispatch measurements, separate from the C/D lifecycle comparison.

| Prequeued ready dispatch | Million actors/s, median [min, max] | Samples |
| --- | ---: | ---: |
| `actor_ready_default` | 22.550 [16.300, 25.260] | 5 |
| `actor_ready_chunk32` | 29.480 [28.040, 29.980] | 5 |

## Private actors

Lifecycle modes report Mcycles/s. Subset modes report million selected callbacks/s, which is a different denominator. The [mode definitions](tools/perf/README.md#actor_churn) distinguish tracked roots, consumer-side free, child trees, continuations, refill, cancellation and subset waves.

| Case | Unit | Median [min, max] | Samples |
| --- | --- | ---: | ---: |
| `private_c0` | Mcycles/s | 39.470 [37.871, 40.895] | 5 |
| `private_c6` | Mcycles/s | 28.223 [26.300, 29.987] | 30 |
| `private-remote_c0` | Mcycles/s | 42.988 [38.469, 43.809] | 5 |
| `private-remote_c6` | Mcycles/s | 8.361 [8.001, 9.282] | 30 |
| `private-tracked_c0` | Mcycles/s | 13.902 [12.591, 14.110] | 5 |
| `private-tracked_c6` | Mcycles/s | 15.069 [13.691, 16.560] | 30 |
| `tree_c0` | Mcycles/s | 52.949 [43.542, 54.818] | 5 |
| `tree_c6` | Mcycles/s | 22.727 [19.641, 25.687] | 30 |
| `tree-wait_c0` | Mcycles/s | 52.099 [47.124, 53.207] | 5 |
| `tree-wait_c6` | Mcycles/s | 22.693 [21.132, 25.383] | 30 |
| `tree-cont_c0` | Mcycles/s | 18.671 [16.280, 19.151] | 5 |
| `tree-cont_c6` | Mcycles/s | 10.022 [8.909, 11.506] | 30 |
| `tree-slab_c0` | Mcycles/s | 66.470 [56.894, 70.136] | 5 |
| `tree-slab_c6` | Mcycles/s | 56.156 [50.719, 62.747] | 30 |
| `tree-wait-slab_c0` | Mcycles/s | 60.307 [56.522, 64.615] | 5 |
| `tree-wait-slab_c6` | Mcycles/s | 55.419 [51.297, 65.530] | 30 |
| `tree-cont-slab_c0` | Mcycles/s | 19.927 [17.660, 20.486] | 5 |
| `tree-cont-slab_c6` | Mcycles/s | 11.474 [8.794, 12.418] | 30 |
| `tree-refill_c0` | Mcycles/s | 58.992 [52.879, 61.426] | 5 |
| `tree-refill_c6` | Mcycles/s | 26.404 [20.078, 29.218] | 30 |
| `tree-refill-slab_c0` | Mcycles/s | 73.674 [67.082, 79.890] | 5 |
| `tree-refill-slab_c6` | Mcycles/s | 81.918 [73.394, 85.714] | 30 |
| `tree-pipelined_c0` | Mcycles/s | 19.359 [17.617, 19.764] | 5 |
| `tree-pipelined_c6` | Mcycles/s | 11.913 [10.793, 13.692] | 30 |
| `tree-cancel_c0` | Mcycles/s | 38.649 [32.955, 39.881] | 5 |
| `tree-cancel_c6` | Mcycles/s | 22.472 [20.205, 24.712] | 30 |
| `dtor_c0` | Mcycles/s | 38.597 [33.922, 39.815] | 5 |
| `dtor_c6` | Mcycles/s | 28.440 [24.892, 30.977] | 30 |
| `subset1_c0` | Mselected callbacks/s | 132.524 [109.421, 133.387] | 5 |
| `subset1_c6` | Mselected callbacks/s | 26.650 [24.263, 29.670] | 30 |
| `subset10_c0` | Mselected callbacks/s | 201.006 [175.224, 226.919] | 5 |
| `subset10_c6` | Mselected callbacks/s | 87.982 [80.124, 100.361] | 30 |
| `subset100_c0` | Mselected callbacks/s | 275.919 [245.528, 285.320] | 5 |
| `subset100_c6` | Mselected callbacks/s | 148.744 [126.080, 173.244] | 30 |
| `subset-whole_c0` | Mselected callbacks/s | 469.086 [416.418, 477.439] | 5 |
| `subset-whole_c6` | Mselected callbacks/s | 261.365 [239.767, 294.249] | 30 |
| `subset-whole-unreserved_c0` | Mselected callbacks/s | 434.307 [384.761, 453.093] | 5 |
| `subset-whole-unreserved_c6` | Mselected callbacks/s | 261.167 [236.269, 285.115] | 30 |

### Cohort slab vs per-object

Compare each tree/refill mode with its `-slab` partner at the same consumer count above. Slab reservation occurs at cohort creation and member storage is reused; these are complete workload rates, not isolated allocator-call timings. Cancellation's dispatched fraction is retained in the machine-readable counters.

### Batch limit and work-cost hints

All cells below have six consumers and 30 samples. `b` changes the batch limit; `auto`, `h2` and `h0` select automatic root classification, chunk 32 and chunk 128. The `child` cases change the child-wave hint instead. Defaults should be chosen against the relevant workload rather than the largest rate across unlike cases.

| Case | Mcycles/s, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `tree_b256_auto` | 22.904 [18.977, 24.218] | 30 / 30 |
| `tree_b256_h2` | 19.958 [16.065, 20.923] | 30 / 30 |
| `tree_b256_h0` | 19.776 [16.529, 20.312] | 30 / 30 |
| `tree_b1024_auto` | 28.722 [26.258, 29.460] | 30 / 30 |
| `tree_b1024_h2` | 10.106 [8.946, 10.624] | 30 / 30 |
| `tree_b1024_h0` | 9.968 [9.151, 10.380] | 30 / 30 |
| `private_b256_auto` | 28.414 [25.663, 29.408] | 30 / 30 |
| `private_b256_h2` | 28.013 [26.310, 29.162] | 30 / 30 |
| `private_b256_h0` | 29.362 [27.184, 30.032] | 30 / 30 |
| `private_b1024_auto` | 26.031 [23.230, 27.193] | 30 / 30 |
| `private_b1024_h2` | 26.052 [24.335, 27.241] | 30 / 30 |
| `private_b1024_h0` | 25.422 [23.499, 26.440] | 30 / 30 |
| `private_b256` | 28.347 [25.427, 29.071] | 30 / 30 |
| `private_b1024` | 25.995 [23.986, 26.839] | 30 / 30 |
| `private_b2048` | 20.502 [18.925, 21.281] | 30 / 30 |
| `tree_child_default` | 22.843 [21.007, 23.723] | 30 / 30 |
| `tree_child_h0` | 22.865 [20.793, 23.706] | 30 / 30 |
| `tree-slab_child_default` | 56.010 [52.491, 57.461] | 30 / 30 |
| `tree-slab_child_h0` | 55.794 [50.454, 58.425] | 30 / 30 |

## Allocators

Six consumers, 30 samples per case. CRT/mimalloc comparisons include the complete lifecycle. `private_emitter` frees on the emitter; `private-remote` frees on consumers. The `count` cases enable allocation instrumentation and are diagnostics, not uninstrumented allocator throughput. Exact cross-thread counts remain in the data file.

| Case | Mcycles/s, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `actor_mimalloc` | 5.734 [5.437, 5.895] | 30 / 30 |
| `actor_crt` | 3.091 [2.913, 3.256] | 30 / 30 |
| `wave_mimalloc` | 6.309 [5.764, 6.533] | 30 / 30 |
| `wave_crt` | 3.270 [3.089, 3.388] | 30 / 30 |
| `tree_mimalloc` | 22.678 [20.818, 23.826] | 30 / 30 |
| `tree_crt` | 7.638 [6.841, 7.844] | 30 / 30 |
| `private-remote_mimalloc` | 8.821 [8.111, 9.169] | 30 / 30 |
| `private-remote_crt` | 3.952 [2.979, 4.231] | 30 / 30 |
| `private_emitter` | 27.913 [25.790, 28.866] | 30 / 30 |
| `private_count` | 5.003 [4.384, 5.170] | 30 / 30 |
| `private_emitter_count` | 17.936 [16.656, 19.000] | 30 / 30 |

## Page backing and pinning

All results use ordinary pages. Pinned actor/tree cells use controller 0 and consumers 1–6; unpinned cells leave placement to the OS. No claim about huge-page benefit is made.

| Case | Mcycles/s, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `actor_c6_pinned` | 5.714 [5.319, 5.878] | 30 / 30 |
| `actor_c6_unpinned` | 5.690 [5.234, 5.878] | 30 / 30 |
| `tree_c6_pinned` | 22.870 [20.782, 23.287] | 30 / 30 |
| `tree_c6_unpinned` | 22.860 [20.642, 23.694] | 30 / 30 |

## Fibers

Single-thread cells measure drain throughput. Multithread cells use the full creation-plus-drain burst because workers can execute during creation. The machine-readable field statistics also retain warm-pool and creation rates.
The single-thread harness's existing rate-only total lines exposed a report-parser compatibility bug. The same five successful raw captures were reparsed after the fix; their timings were neither rerun nor discarded. Counted create/run phases supply the auxiliary full-burst totals. Malformed multithread totals remain errors.

| Benchmark / phase | Fibers/s, median [min, max] | Samples |
| --- | ---: | ---: |
| `benchmark_st` / cold creation | 132,650.470 [124,742.220, 135,647.130] | 5 |
| `benchmark_st` / cold drain | 3,577,651.130 [3,127,013.010, 3,829,099.630] | 5 |
| `benchmark_st` / cold full burst | 127,907.906 [119,956.935, 130,881.316] | 5 |
| `benchmark_st` / warm drain | 3,371,527.120 [2,927,494.740, 3,670,526.220] | 5 |
| `benchmark_st` / warm full burst | 1,087,133.772 [899,458.526, 1,199,407.493] | 5 |
| `benchmark_mt` / cold full burst | 122,435.242 [120,495.458, 125,179.116] | 5 |
| `benchmark_mt` / warm full burst | 2,656,169.485 [2,442,264.859, 2,702,775.751] | 5 |

The [historical Fiber characterization](docs/performance/archive-2026-10-20.md#fibers) retains the detailed same-thread, worker-topology, embedding, allocation and assist-ring experiments. Those experiments were not repeated as part of these two baseline cells.

## Scaling microbenchmarks

Elapsed milliseconds; lower is better. These separately measure 16,384 creations and lifecycle work with batch 32 and handles.

| Case | ms, median [min, max] | Valid / requested samples |
| --- | ---: | ---: |
| `create_16384` | 1.609 [1.598, 1.699] | 5 / 5 |
| `lifecycle_16384_b32` | 1.139 [1.079, 1.548] | 5 / 5 |

## Follow-up

- Ryzen 9 5900XT: pending a separate run with its own compiler, topology, affinity and page metadata.
- Free-index stack: this baseline still uses the linked free-slot structure. Evaluate that proposal with matched large-batch contention and latency measurements, including rollback and scalar controls.
- Historical inconclusive optimization gates remain inconclusive; an absolute-rate refresh does not reclassify them.
- The 256-entry mailbox latency scene timed out in all five processes. It has no complete latency baseline on this run.
