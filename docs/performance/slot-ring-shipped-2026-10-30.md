# Shipped slot ring with the request layout — 30 October 2026

The 2026-10-30 slot-ring study (`docs/performance/slot-ring-2026-10-30.md`
on branch `exp/slot-ring`) measured Vyukov rings with a three-word element.
The ring shipped as `antfarm.ring.SlotRing` carries `ant_uv`'s request
element instead: `RingElement`, 56 bytes (tag, generation, pointer, four
inline words), one per 64-byte slot. This record re-runs the study's
single-ended, many-producer and latency shapes on the shipped ring, beside
the study's 32-byte-slot Vyukov ring and singleton Farm tables, through the
same harness ported to the shipped ring (`tools/perf/slot_ring.d`).

Ryzen 9 5900XT (two CCDs of eight cores, 32 MiB L3 each), Linux 7.3, LDC
1.43.0 `-O2 -release`, Ant Farm `2.2.0-progress`. Load average 0.3 at the
start. Medians of three runs of four million elements (200,000 one-way
samples for latency); threads pinned to physical cores CCD0 first. The
study's run-to-run spread on contended shapes was about ±15%.

## Results

Nanoseconds per element; latency is one-way p50 / p99 in nanoseconds.

| Shape | `ring` (shipped, 56 B in 64 B) | `vyukov32` (24 B in 32 B) | Farm |
| --- | ---: | ---: | ---: |
| One thread, enqueue one, dequeue one | 11.2 | 8.7 | 44.3 |
| One producer, one consumer | 10.1 | 7.5–7.7 | 46.6–46.8 |
| Two producers, one consumer | 44.5 | 54.2 | 37.6 |
| Four producers, one consumer | 63.2 | 80.6 | 22.0 |
| Seven producers, one consumer | 81.1 | 109.7 | 23.2 |
| Latency, cores 0 and 1 (one CCD) | 40 / 70 | 50 / 100 | 150 / 1,560 |
| Latency, cores 0 and 8 (across CCDs) | 190 / 520 | 190 / 650 | 400 / 2,220 |

What it shows:

- **Single-ended.** The shipped ring costs about 2.5 ns more than the
  32-byte reference. That is the price of a line per slot and a 56-byte
  copy, and it is still four times under a one-element Farm table.
- **Many producers, one consumer.** This is the request ring's own shape,
  and the shipped ring degrades less than the reference: 22% better at
  four producers and 26% at seven, because a line per slot removes the
  neighbour-slot sharing the study found. The Farm still wins from four
  producers on, three to four times, as the study predicted for any shared
  end.
- **Latency.** Delivery is 40 ns on one CCD and 190 ns across CCDs; the
  cross-CCD figure is an inter-CCD line transfer per step.

For a loop's request ring these numbers are far below the cost of the file
operations it carries. The ring is sized for occupancy (1024 slots by
default), not throughput.

## Raw rows

```text
impl      mode     thr rings pol   cap/qs          N     sec      Mops    ns/op     stalls    foreign       p50      p90      p99       max
ring      same       1     1 own    65536    4000000   0.045    89.092     11.2          0          0
ring      spmc       2     1 own    65536    4000000   0.040    99.361     10.1          0          0
ring      mpsc       2     1 own    65536    4000000   0.040    98.823     10.1          0          0
ring      mpsc       3     1 own    65536    4000000   0.178    22.453     44.5          0          0
ring      mpsc       5     1 own    65536    4000000   0.253    15.813     63.2          0          0
ring      mpsc       8     1 own    65536    4000000   0.324    12.335     81.1          0          0
ring      latency    2     1 tgt    65536     200000   0.020         -        -          0          0      40.0     60.0     70.0    3699.9
ring      latency    2     1 tgt    65536     200000   0.080         -        -          0          0     190.0    200.0    520.0   16029.8
vyukov32  same       1     1 own    65536    4000000   0.035   114.586      8.7          0          0
vyukov32  spmc       2     1 own    65536    4000000   0.030   134.020      7.5          0          0
vyukov32  mpsc       2     1 own    65536    4000000   0.031   130.611      7.7          0          0
vyukov32  mpsc       3     1 own    65536    4000000   0.217    18.437     54.2          0          0
vyukov32  mpsc       5     1 own    65536    4000000   0.323    12.400     80.6          0          0
vyukov32  mpsc       8     1 own    65536    4000000   0.439     9.116    109.7          0          0
vyukov32  latency    2     1 tgt    65536     200000   0.024         -        -          0          0      50.0     60.0    100.0 3228384.6
vyukov32  latency    2     1 tgt    65536     200000   0.082         -        -          0          0     190.0    200.0    650.0   96178.6
farm      same       1     1 -       4096    4000000   0.177    22.594     44.3          0          0
farm      spmc       2     1 -       4096    4000000   0.187    21.371     46.8          0          0
farm      mpsc       2     1 -       4096    4000000   0.186    21.466     46.6          0          0
farm      mpsc       3     1 -       4096    4000000   0.150    26.619     37.6          0          0
farm      mpsc       5     1 -       4096    4000000   0.088    45.363     22.0    4958369          0
farm      mpsc       8     1 -       4096    4000000   0.093    43.038     23.2   12484320          0
farm      latency    2     1 -       4096     200000   0.047         -        -          0          0     150.0    170.0   1560.0   13209.8
farm      latency    2     1 -       4096     200000   0.134         -        -          0          0     400.0    410.0   2220.0   14049.8
```

The `thr` column counts every thread, so `mpsc` with N producers prints N + 1.
Commands, from `tools/perf` after `make slot_ring`:

```sh
for impl in ring vyukov farm; do
  ./slot_ring --impl $impl --mode same
  ./slot_ring --impl $impl --mode spmc --threads 1
  for p in 1 2 4 7; do ./slot_ring --impl $impl --mode mpsc --threads $p; done
  ./slot_ring --impl $impl --mode latency --cores 0,1
  ./slot_ring --impl $impl --mode latency --cores 0,8
done
```
