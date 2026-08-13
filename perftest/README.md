# Ant Farm throughput

Synthetic benchmark for a **16 MiB** farm (`Ln = 2^21` ulongs). Verdict on
what held and what did not: `POSTMORTEM.md`. The work callback in the
sweep is a single atomic increment so those numbers are queue overhead,
not payload compute.

```
make -C perftest run          # two-phase sweep (default)
make -C perftest run-once     # one config (see flags below)
```

Release build (`ldc2 -O2 -release`). `fatal()` still prints and aborts.

## Default sweep

1. **Topology** — `K`, consumer count, producer mix — at a sharded table
   (`batch=80`) and a 16-ulong body.
2. **Shape** — payload body and write batch on the winning topology,
   including both sides of `SMALL_TABLE_THRESHOLD` (64).

Each config is run 3 times; the table reports the median payloads/s.
Configs that cannot fit one payload in the active quota are skipped.
A trial that fails to drain in 90s is recorded as `TIMEOUT` and does
not abort the sweep.

## First 16 MiB sweep (Ryzen 5 5500, 6c/12t, `ldc2 -O2 -release`)

Cheap ST callback, 16 MiB farm sits in L3.

| Goal | Config | Result |
|------|--------|--------|
| Payloads/s | `K=4` `nc=8` `nb=2` `body=16` `batch=80` | **61 Mpps** / 7.4 GiB/s body |
| Body bytes/s | same topology, `body=1024` `batch=63` | 4.6 Mpps / **35 GiB/s** body |

The winning region is wide: 8 consumers, 2 bulk producers (or 4 small), `batch ≥ 32`. `K=4` and `K=8` are close; `K=4` won this host. `batch=1` is ~4–5× slower. Two consumers was the *worst* consumer count (shard `sqcs=1` like one consumer, plus extra coherence).

## Digest bench (linear chunk vs scatter)

`make -C perftest digest-run` — execute-only window, `Call` reads the body.

| Arm | Meaning |
|-----|---------|
| `linear16` | claim 16, walk payloads in table order |
| `linear1` | same layout, claim 1 (amortization control) |
| `shuffled16` | claim 16, `Tindex` permuted (locality control) |
| `copyout16` | claim 16, memcpy then `Call` (by-value dequeue) |

On this 6c/12t box the **claim** dividend is real (`linear1` is ~1.5–1.6× ns/job). **Layout** is not: shuffled ≈ linear even at 128 MiB. Copy-out of 264 B is free. 8 KiB bodies are bandwidth and wash all three effects out.

```
./digest                  # touch, body=16
./digest --chase          # plus one load from a 32 MiB out-of-farm array
./digest --alt            # two alternating Call pointers
./digest --body 1024
./digest --ln $((1<<24))  # 128 MiB farm
```

## Tail bench (mid-tick publish → first Call)

`make -C perftest tail-run` — production `consumeNext`, not the digest walker.

Metric: ticks just before `write()` of a 1-payload small-tier sentinel → first instruction of that payload’s `Call`. Background dump jobs spin; the sentinel only timestamps. Default `nc=6` pinned to physical cores 0–5 (this host is 6c/12t). Median of 3 runs, 10k samples after 200 warmup.

| Scene | What it asks |
|-------|----------------|
| `idle` | workers spinning on an empty farm |
| `mid-drain` | one large table in flight; sentinel published after ~tlen/4 background Calls |
| `burst` | 32 one-payload tables during one dump (first vs last) |
| `near-full` | parked fill until small `write` returns 0, then spray + admit |
| `mailbox` | same mid-drain, sentinel on a dedicated atomic poller (the OOB channel) |

`yield16` / `claim1` are attribution, not production: five workers end the visit after one run, one stock worker drains leftovers (mixed chunk sizes on one `Tcount` deadlock). Yielders also try shard 0 on a following small table so the sentinel is visible.

```
./tail                  # full phase 0–1 + mailbox
./tail --idle-only
./tail --attr           # yield16, claim1, mailbox at 256 and 8192
./tail --no-pin
```

### First tail suite (same host)

| Scene | Arm | tlen | spin | p50 | p99 | note |
|-------|-----|------|------|-----|-----|------|
| idle | stock | — | — | 350 ns | 5.5 µs | floor; unpinned max was 1.2 ms |
| mid-drain | stock | 256 | 0 | 1.5 µs | 3.6 µs | empty Call hides coupling |
| mid-drain | stock | 256 | 1 µs | 11 µs | **32 µs** | ~2 chunks left |
| mid-drain | stock | 256 | 10 µs | 101 µs | 267 µs | scales with job time |
| mid-drain | stock | 2048 | 1 µs | 237 µs | 281 µs | |
| mid-drain | stock | 8192 | 1 µs | 1.08 ms | **1.82 ms** | grows with dump size |
| mid-drain | yield16 | 256 | 1 µs | 5.2 µs | 5.4 µs | visit ends after 16 |
| mid-drain | claim1 | 256 | 1 µs | 340 ns | 431 ns | visit ends after 1 |
| mid-drain | stock | 32 | 1 µs | 331 ns | 2.7 µs | small dump: skippers hit the sentinel |
| burst | stock | 256 | 1 µs | 20 µs | 55 µs | last faster than first (not shard-0 pileup) |
| near-full | stock | 256 | 0 | 351 ns | 7.9 µs | p99.9 1.5 ms; **zeros=0** under spray; parked wall at 30768 jobs |
| mailbox | — | 256 | 1 µs | 90 ns | 130 ns | independent of dump size |
| mailbox | — | 8192 | 1 µs | 91 ns | 131 ns | |

`nc=8` oversub (pinned): idle p99 7.1 µs, mid 256/1 µs p99 32.5 µs — same story, not a scheduler lie.

**Stock tail tracks remaining shard work**, which grows with the tick dump. At a 256-job dump and 1 µs jobs, p99 is 32 µs (fine at 60 Hz). At 8192 it is 1.8 ms (a real slice). A dedicated mailbox is ~130 ns either way — that is the OOB channel the design refused. `write()==0` exists when consumers are parked (~30k jobs at this topology) but did not show up under concurrent spray.

## Single run

```
./throughput --once --k 8 --nc 4 --nb 1 --ns 0 --body 16 --batch 80
```

| Flag | Meaning | Default |
|------|---------|---------|
| `--ln` | buffer length in ulongs (power of 2) | `1<<21` (16 MiB) |
| `--k` | segment count | 8 |
| `--nc` | consumers | 4 |
| `--nb` | bulk producers | 1 |
| `--ns` | small producers | 0 |
| `--body` | payload body length in ulongs | 16 |
| `--batch` | max payloads per `write()` | 80 |
| `--n` | total payloads | scaled by body (16e6 at 16 ulongs) |
| `--qb` | bulk quota (0 = segment capacity) | 0 |
| `--qs` | small quota | 4096 |
| `--repeats` | timed repeats | 3 |
