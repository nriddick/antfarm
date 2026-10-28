# Live lifecycle A/B against the pre-Live commit — 28 October 2026

Whether replacing the `Sub0` pulse with the per-epoch `Live` bit (SPEC 3, 6b,
6c, 6e) changed the Farm's throughput, tail latency or the systems built on
it. Base is `c2431a5`, the `2.1.1-progress` tip before the change; candidate
is `7e861a2`, the `review/live-lifecycle-audit` tip (the Live implementation
`8275359`, its audit, the two sibling review branches and the documentation
pass). Between the two commits the only runtime source that differs is
`antfarm/package.d`; every benchmark, Fiber, threadpool and fork-join source
is byte-identical, so each cell times the same benchmark against two Farms.

Ryzen 9 5900XT (two CCDs, 32 MiB L3 each), Linux 7.2.9, `performance`
governor, LDC 1.43.0 `-O2 -release`, ordinary pages, mimalloc 3.5.3 for the
actor cells. The host was otherwise idle (load average below 0.1 at the
start; the load recorded before each sample is the benchmark's own). Both
revisions were built from private `git archive` copies; every cell ran its
samples as matched base/candidate pairs in alternating order, and the change
reported is the median of the per-pair ratios with the smallest and largest
pair. The [machine-readable results](live-lifecycle-ab-2026-10-28.json) keep
every sample.

## Farm, Fibers, parallel loops and public dispatch

Seven pairs per cell, the PERFORMANCE.md commands. Payload cells are
unpinned; `fiber_mt` pins its workers; `parallel` is the loop-table
benchmark at eight workers, 100,000 elements of cost 16, twenty rounds.

| Section | Cell | Base `c2431a5` | Candidate `7e861a2` | Paired change, median [min, max] |
|---|---|---:|---:|---:|
| headline | `payload_t2`, Mpps | 733.0 ± 7.0 | 739.1 ± 35.1 | +2.2% [−6.5%, +4.8%] |
| headline | `payload_t4`, Mpps | 950.5 ± 49.5 | 956.4 ± 43.8 | +0.4% [−9.4%, +9.6%] |
| headline | `payload_t8`, Mpps | 1,863 ± 118 | 1,619 ± 134 | −3.3% [−13.8%, +15.1%] |
| headline | `payload_t16`, Mpps | 2,368 ± 105 | 2,381 ± 107 | −0.9% [−11.4%, +8.7%] |
| headline | `payload_t24`, Mpps | 2,436 ± 107 | 2,508 ± 95 | +0.8% [−5.3%, +8.2%] |
| headline | `payload_t32`, Mpps | 2,454 ± 132 | 2,464 ± 55 | −3.7% [−8.9%, +3.9%] |
| farm_best | `single_producer`, Mpps | 830.2 ± 5.0 | 813.6 ± 4.5 | −2.3% [−3.0%, −1.0%] |
| farm_best | `default_shape`, Mpps | 131.3 ± 20.9 | 130.8 ± 24.8 | +0.7% [−5.0%, +34.2%] |
| farm_consumers | `nc1`, Mpps | 180.6 ± 24.2 | 181.8 ± 24.6 | +1.1% [−29.2%, +35.2%] |
| farm_consumers | `nc4`, Mpps | 133.8 ± 25.5 | 171.2 ± 24.0 | −1.8% [−24.0%, +40.3%] |
| farm_consumers | `nc11`, Mpps | 98.8 ± 1.2 | 99.3 ± 12.7 | +0.5% [−8.4%, +26.0%] |
| dual | `nd2`, Mpps | 174.1 ± 39.5 | 201.9 ± 22.5 | +2.3% [−12.3%, +62.9%] |
| dual | `nd6`, Mpps | 367.6 ± 96.1 | 381.1 ± 95.2 | +4.6% [−36.2%, +76.4%] |
| dual | `nd11`, Mpps | 245.6 ± 110 | 272.7 ± 75.7 | −10.3% [−39.3%, +72.2%] |
| public | `actor_ready_default`, Mactor/s | 77.6 ± 4.3 | 77.1 ± 7.1 | −0.6% [−26.0%, +18.5%] |
| parallel | `parallel_g64_w8`, ns/round (lower is better) | 172,350 ± 3,803 | 173,095 ± 8,158 | +0.4% [−5.7%, +10.9%] |
| parallel | `parallel_g1_w8`, ns/round (lower is better) | 350,990 ± 14,187 | 347,080 ± 14,316 | −1.7% [−8.5%, +7.6%] |
| fibers | `fiber_st` drain, fibers/s | 4,277,270 ± 18,788 | 4,202,025 ± 118,043 | −1.4% [−2.5%, +4.9%] |
| fibers | `fiber_mt_w8` cold burst, fibers/s | 185,839 ± 1,746 | 185,917 ± 1,078 | −0.7% [−1.4%, +2.0%] |
| fibers | `fiber_mt_w8` warm burst, Mfibers/s | 2.61 ± 0.04 | 2.66 ± 0.05 | +2.0% |
| fibers | `fiber_mt_w16` cold burst, fibers/s | 187,152 ± 1,444 | 186,529 ± 980 | −0.1% [−2.0%, +1.0%] |
| fibers | `fiber_mt_w16` warm burst, Mfibers/s | 2.01 ± 0.05 | 2.05 ± 0.06 | +1.8% |

Every paired median lies inside its own pair spread, and the spreads are the
ones PERFORMANCE.md records for these cells (the `nc`, `nd` and
`default_shape` cells are bimodal on this host whichever Farm runs them).
The one cell whose pairs all fell on one side, `single_producer` at −2.3%,
is code layout, not the protocol:

| `single_producer` run | Base | Candidate | Paired change |
|---|---:|---:|---:|
| first run, 7 pairs | 830.2 ± 5.0 | 813.6 ± 4.5 | −2.3% [−3.0%, −1.0%] |
| repeated, 7 pairs | 821.7 ± 9.5 | 817.9 ± 3.5 | −0.4% [−3.6%, +0.1%] |
| both sides rebuilt with 64-byte function alignment, 7 pairs | 867.8 ± 5.3 | 903.3 ± 8.3 | +4.0% [+2.2%, +6.3%] |

The repeat did not reproduce the first run, and aligning every function
(`--align-all-functions=6`, the layout control the fork-join harness uses)
moved both sides up and the candidate ahead. Whole-program layout shifts of
this size on identical semantics are documented in the
[performance report method](../../tools/perf/README.md#performance-report).

## Tail latency

Three pairs per thread count at 2, 6 and 32 threads, eight at 12 (the first
three flagged a p99 move; five more were added). Per-process quantiles,
median over the samples, in microseconds.

| Threads | Scene | p50 base / cand | p99 base / cand | p99.9 base / cand |
| ---: | --- | ---: | ---: | ---: |
| 2 | idle | 0.14 / 0.14 | 1.80 / 1.80 | 3.40 / 3.40 |
| 2 | mid-drain, 256 × 1 µs | 201.6 / 200.9 | 204.8 / 204.5 | 210.7 / 209.1 |
| 2 | burst, 256 × 1 µs | 253.5 / 250.4 | 258.3 / 255.4 | 259.8 / 259.0 |
| 6 | idle | 0.23 / 0.23 | 2.10 / 2.10 | 5.30 / 4.90 |
| 6 | mid-drain, 256 × 1 µs | 3.20 / 3.20 | 20.5 / 20.5 | 22.8 / 22.8 |
| 6 | burst, 256 × 1 µs | 27.1 / 26.9 | 30.6 / 30.6 | 32.1 / 32.4 |
| 12 | idle | 0.31 / 0.32 | 2.70 / 2.80 | 7.20 / 7.20 |
| 12 | mid-drain, 256 × 1 µs | 10.5 / 10.5 | 11.0 / 12.9 (first 3 pairs); 12.1 / 12.2 (5 more) | 13.9 / 14.1 |
| 12 | burst, 256 × 1 µs | 10.5 / 10.6 | 15.4 / 15.4 | 17.9 / 17.9 |
| 32 | idle | 0.74 / 0.73 | 3.80 / 3.80 | 19.1 / 18.9 |
| 32 | mid-drain, 256 × 1 µs | 0.68 / 0.68 | 1.30 / 1.30 | 3.50 / 3.30 |
| 32 | burst, 256 × 1 µs | 0.82 / 0.84 | 5.70 / 5.70 | 10.1 / 10.6 |

The 12-thread mid-drain p99 is bimodal on this host (11.0 or about 12.9 µs
per process; the October 20 record has 11.5 to 13.2); its first three pairs
happened to split 11.0 against 12.9, and the five further pairs gave
−3.9% [−12.1%, +10.9%]. Nothing else moved by more than one quantile step.

## Actors and private actors

`tools/perf/perf_gate.py`, which builds both revisions itself in detached
worktrees and pins the controller to CPU 0 and the consumers to cores 1 to 7
of the same CCD. Five pairs per cell, `actor_churn` at 16,384 actors, three
seconds per sample, batch 256, quota 16,384 words, mimalloc. Millions of
complete lifecycles per second; the gate flags a cell whose paired median
falls below −1.5% or whose pairs span more than 6%.

| Mode | Consumers | Base | Candidate | Paired change [min, max] | Gate |
|---|---:|---:|---:|---:|---|
| `actor` | 0 | 23.08 | 23.42 | +1.5% [+0.6%, +1.8%] | pass |
| `actor` | 6 | 23.06 | 23.16 | +0.4% [+0.2%, +0.8%] | pass |
| `actor` | 7 | 22.78 | 22.87 | +0.4% [+0.3%, +1.1%] | pass |
| `wave` | 0 | 27.98 | 28.29 | +1.1% [+0.8%, +1.1%] | pass |
| `wave` | 6 | 24.58 | 24.89 | +1.3% [+1.2%, +1.6%] | pass |
| `wave` | 7 | 24.47 | 24.49 | +0.1% [−0.2%, +0.6%] | pass |
| `private-slab` | 0 | 122.96 | 120.56 | −2.0% [−2.5%, −1.4%] | pass, flagged |
| `private-slab` | 6 | 121.24 | 119.47 | −2.0% [−2.3%, +1.2%] | pass, flagged |
| `private-slab` | 7 | 122.18 | 121.18 | −0.6% [−1.9%, +0.6%] | pass |
| `tree-refill-slab` | 0 | 168.03 | 168.09 | +0.0% [−0.7%, +0.8%] | pass |
| `tree-refill-slab` | 6 | 423.07 | 427.30 | +1.0% [+0.9%, +1.4%] | pass |
| `tree-refill-slab` | 7 | 455.56 | 458.69 | +0.7% [+0.6%, +1.2%] | pass |
| `tree-cont` | 0 | 37.28 | 37.50 | +0.6% [−0.4%, +2.5%] | pass |
| `tree-cont` | 6 | 41.73 | 41.62 | −0.3% [−1.2%, +0.0%] | pass |
| `tree-cont` | 7 | 42.97 | 42.77 | −0.5% [−0.6%, +0.3%] | pass |
| `private-remote` | 0 | 92.21 | 92.51 | +0.3% [+0.2%, +1.6%] | pass |
| `private-remote` | 6 | 16.17 | 16.74 | +3.5% [+3.0%, +3.6%] | pass |
| `private-remote` | 7 | 15.55 | 16.16 | +3.9% [+3.6%, +4.3%] | pass |

The two flagged cells were rerun with the gate's `--boundary` setting,
twenty pairs each:

| Mode | Consumers | Base | Candidate | Paired change [min, max], 20 pairs | Gate |
|---|---:|---:|---:|---:|---|
| `private-slab` | 0 | 123.12 | 120.70 | −1.9% [−2.6%, −1.1%] | pass, flagged |
| `private-slab` | 6 | 121.63 | 120.93 | −0.3% [−1.0%, +0.3%] | pass |

The six-consumer cell is flat and the seven-consumer cell above is −0.6%,
so the zero-consumer −1.9% is the serial cell's layout swing, which the
gate's method says to read that way unless a six-consumer cell moves with
it; the method's own identical-binary measurements put that swing at 5 to
12% on serial cells. The private root lifecycle never subscribes or
unsubscribes inside the timed loop; the `Live` change touches it only
through the discharge CAS, once per epoch.

## Subscribe and unsubscribe

The Live change rewrote the subscribe scan and the departure path, which no
PERFORMANCE.md cell times on its own, so one microbenchmark was added for
this comparison: a thread repeating subscribe, three `consumeNext` calls and
unsubscribe on a 1 MiB, K = 8 Farm, once idle (no producer) and once beside
two producers writing continuously and two steady consumers draining (the
T29 geometry); 200,000 cycles after 2,000 warm-up cycles, seven pairs.

The microbenchmark is kept as `tools/perf/subscribe_cost.d`
(`make -C tools/perf subscribe_cost`).

| Scene | Base, ns per cycle | Candidate, ns per cycle | Paired change [min, max] |
|---|---:|---:|---:|
| idle Farm | 25.8 ± 1.0 | 26.3 ± 1.5 | +2.3% [−4.8%, +21.9%] |
| under production | 368.8 ± 118.7 | 148.3 ± 63.5 | −48.0% [−67.1%, −3.4%] |

On an idle Farm the two protocols cost the same: the window holds one
`Live` slot and both pin it. Under production every one of the seven pairs
favoured the candidate, by half at the median: the old walk added a `Sub`
to every slot of its window, eight contended RMWs on a K = 8 Farm plus the
pulse retraction, where the new scan pins only the newest slot and the
candidate it validates (SPEC 6b), the two pins the design record measured
as the common case. The cell is noisy because the producers move the window
under the subscriber (base 137 to 417 ns, candidate 110 to 295), and no
cycle reported `RETRY` on either side.

## Against the October 20 record

Where the same commands ran on October 20 at `d00b37c`, both sides of this
comparison land where that page says for the headline payload sweep, the
Farm consumer and dual cells, `actor_ready`, the actor gate's modes and the
cold Fiber burst, and the warm Fiber burst is higher on both sides (2.61
and 2.66 Mfibers/s at 8 workers against 2.43; 2.01 and 2.05 at 16 against
1.88). Two cells sit below that record on *both* sides, so the drift predates
the Live change and is not attributable to it: the single-thread Fiber drain
(4.28 and 4.20 Mfibers/s against 5.23, about 19% lower) and
`single_producer` (830 and 814 Mpps against 888, 6 to 8% lower). Neither was
investigated here.

## Conclusion

The Live lifecycle is performance-neutral on every existing cell and
cheaper on the path it rewrote: no Farm, Fiber, loop-table, `actor_ready`
or actor-gate cell moved outside its paired spread, the one consistent
single-run difference reversed under a layout control, the gate's
remaining flagged cell is the serial one with its parallel partners flat,
and a subscribe cycle under production costs about half of what it did.
What this comparison
does not cover: subscribe latency under `RETRY` pressure (no cell reaches
`RETRY`; the torture herds report zero), Windows, and the fork-join harness,
whose dispatcher runs deque-only by default and never enters the changed
paths.
