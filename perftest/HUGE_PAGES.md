# Huge pages (THP) A/B

Date: local run on the same Ryzen 5 5500 host (6c/12t, 16 MiB L3).
Method: `--huge` calls `madvise(MADV_HUGEPAGE)` on both halves of the
magic-buffer alias; `ShmemPmdMapped` confirmed 16 MiB of THP on the farm.

All runs are per-worker batched callback, `ldc2 -O2 -release`,
`--repeats 5` (except the large-ring table uses `--repeats 3`).

## 16 MiB throughput (`--ln $((1<<21))`)

| Config | 4K pages (Mpps) | `--huge` (Mpps) | note |
|---|---:|---:|---|
| `K=4 nc=4 nb=1 ns=4 body=2 batch=256` | 186.2 / 180.5 | 246.4 / 243.1 | +30–36% |
| `K=4 nc=4 nb=1 ns=4 body=16 batch=80` | 126.7 / 126.6 | 165.2 / 164.2 | +30% |
| `K=4 nc=4 nb=1 ns=4 body=1024 batch=80` | 7.13 / 6.45 | 9.71 / 9.75 | +36–51% |
| `K=8 nc=8 nb=2 ns=0 body=16 batch=256` | 78.4 / 77.5 | 89.4 / 88.7 | +14% |

Each cell is the median of a 5-repeat run; two independent rounds are shown
for the 16 MiB A/B.

## Ring-size scaling (`K=4 nc=4 nb=1 ns=4 body=16 batch=80`, `--n 32000000`)

| Ln | MiB | 4K pages (Mpps) | `--huge` (Mpps) | Delta |
|---|---:|---:|---:|---:|
| `1<<22` | 32 | 47.5 | 48.0 | +1% |
| `1<<23` | 64 | 38.3 | 38.7 | +1% |
| `1<<24` | 128 | 35.5 | 34.7 | −2% |
| `1<<25` | 256 | 34.1 | 34.7 | +2% |

The huge-page win is concentrated in the L3-resident 8–16 MiB ring.

## Body-bytes large-ring check (`body=1024 batch=80`, `--n 3200000`)

| Ln | MiB | 4K pages (Mpps) | `--huge` (Mpps) | Delta |
|---|---:|---:|---:|---:|
| `1<<22` | 32 | 2.44 | 2.54 | +4% |
| `1<<23` | 64 | 1.88 | 1.96 | +4% |
| `1<<24` | 128 | 1.74 | 1.81 | +4% |

## Shape across Farm topologies

One-repeat topology grid at 16 MiB (`K` 4/8, `nc` 1/2/4/8, mixes
`nb/ns` 1/0, 2/0, 1/4, 0/4) shows huge pages do **not** flatten the Mpps
curve; they make it more peaked.

| Body | Metric | 4K pages | `--huge` |
|-----:|--------|---:|---:|
| 16 | mean Mpps | 69.7 | 83.6 |
| 16 | coefficient of variation | 0.32 | 0.39 |
| 16 | max/min | 3.20 | 3.53 |
| 1024 | mean Mpps | 4.02 | 5.46 |
| 1024 | coefficient of variation | 0.39 | 0.47 |
| 1024 | max/min | 3.33 | 4.37 |

The speedup also correlates with baseline speed: for `body=1024`, the
bottom quartile of baseline topologies gained ~20% with `--huge`, while the
top quartile gained ~45%. So the existing sweet spots benefit the most.

## Digest sanity check

The execute-only digest bench was not the source of the win: `linear16`
at 16 MiB was ~31.2 ns/job (4K) vs ~31.4 ns/job (`--huge`), and at 128 MiB
~29.7 vs ~30.2 ns/job. The throughput gain is in the full publish/consume
path on the L3-resident ring.

## Commands

```
make -C perftest throughput
./perftest/throughput --once --huge --k 4 --nc 4 --nb 1 --ns 4 --body 16 --batch 80 --n 64000000 --repeats 5
./perftest/digest --huge --ln 16777216 --arm linear16 --jobs 100000 --repeats 3
./perftest/dual --huge --n 100000 --repeats 1
./perftest/tail --huge --idle-only --samples 100 --repeats 1
```
