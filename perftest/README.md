# Ant Farm throughput

Synthetic benchmark for a **16 MiB** farm (`Ln = 2^21` ulongs). Verdict on
what held and what did not: `POSTMORTEM.md`. The default work callback is a
**per-worker batched counter** (local increment, one shared atomic flush per
1024 calls). Earlier sweeps used `--global-count`, a single shared atomic
increment; that made all eight workers contend on one cache line and the
number measured atomic-contention throughput rather than Ant Farm overhead.
`--global-count` is kept for A/B comparisons.

```
make -C perftest run          # two-phase sweep (default)
make -C perftest run-once     # one config (see flags below)
make -C perftest dual-run     # dual-registered producer/consumer threads
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

## Latest 16 MiB sweep (Ryzen 5 5500, 6c/12t, `ldc2 -O2 -release`)

Per-worker batched callback, 16 MiB farm sits in L3. With the old
`--global-count` callback the same K=8/nc=8/nb=2/body=16/batch=256 point is
~60.7 Mpps (the old callback is atomic-contention-bound, so this barely
moves); the batched-callback numbers below are the queue-overhead view.

| Goal | Config | Result |
|------|--------|--------|
| Payloads/s | `K=4` `nc=4` `nb=1` `ns=4` `body=2` `batch=256` | **168 Mpps** / 2.6 GiB/s body |
| 16-ulong body | `K=4` `nc=4` `nb=1` `ns=4` `body=16` `batch=80` | **120 Mpps** / 14.7 GiB/s body |
| Body bytes/s | `K=4` `nc=4` `nb=1` `ns=4` `body=1024` `batch=80` | 6.0 Mpps / **47.2 GiB/s** body |

With the batched callback the topology ranking changes: a mix of **1 bulk +
4 small producers** beats 2 bulk producers at `nc=4` and `nc=8` (the old
global-atomic callback masked producer-side work). `batch=1` is still the
worst shape. The full ranked table is `last_sweep.txt`.

The old global-atomic point `K=8 nc=8 nb=2 body=16 batch=256` remains a
robust production config: **~61 Mpps** global-atomic / **~77 Mpps**
batched on this host.
## Dual-role bench (small producers that are also consumers)

`make -C perftest dual-run` — every thread registers a producer ticket
(small tier by default) **and** subscribes a `ConsumerView`, then alternates
between one `write()` and a consume phase. `--consume 0` means “drain until
empty”; `--consume N` means exactly N `consumeNext()` calls per produce
phase. This is the test for the interleaved small-push shape the main sweep
cannot express.

Latest run (`last_dual.txt`, 16 MiB, body=16, 3-run medians):

| K | nd | tier | batch | consume | Mpps | MiB/s | stalls |
|---:|---:|:---|---:|---:|---:|---:|---:|
| 4 | 1 | small | 1 | drain | 10.9 | 1,329 | 0 |
| 4 | 1 | small | 80 | drain | 38.6 | 4,707 | 0 |
| 4 | 2 | small | 80 | drain | 64.1 | 7,830 | 0 |
| 4 | 4 | small | 1 | strict alt (1) | 6.0 | 738 | 11.9M |
| 4 | 4 | small | 1 | drain | 6.7 | 819 | 0 |
| 4 | 4 | small | 63 | strict alt (1) | 69.6 | 8,496 | 355k |
| 4 | 4 | small | 80 | drain | 87.5 | 10,676 | 0 |
| 4 | 8 | small | 80 | drain | **114.2** | **13,938** | 62k |

## ZERO_ST_RMW experimental ST path

`--d-version=ZERO_ST_RMW` replaces the ST fast path's one remaining Pcount
claims RMW with a regular non-atomic increment, relying entirely on the
shard `Tcount` chunk claim for exclusivity. Build it with:

```
ldc2 -O2 -release --d-version=ZERO_ST_RMW throughput.d ../antfarm.d -of=throughput
```

Measured at `K=8 nc=8 nb=2 body=16 batch=256`:

| callback | default RMW gate | ZERO_ST_RMW |
|---|---:|---:|
| per-worker batched | ~76–78 Mpps | ~76–77 Mpps |
| global-atomic | ~60–61 Mpps | ~63 Mpps |

The gain is real only under the legacy global-atomic callback; with the
batched callback the ST RMW is not the bottleneck. **Unease:** a
per-element search path outside the chunk digest would create duplicate ST
entry, and a consumer that crashes mid-chunk is already illegal, so the
chunk claim is the only guard. Treat this as an experiment, not a default.

Key trend: small **strictly alternating** writes (`batch=1`) are dominated
by small-table write cost and fill stalls; `batch >= 63` and draining after
each write recovers high throughput, and more dual threads keep helping on
this host (8 dual threads is the best measured). `--tier bulk` switches all
dual threads to bulk tickets if you want the other extreme.


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

### Latest tail suite (same host, claim-gated ST fast path)

| Scene | Arm | tlen | spin | p50 | p99 | p99.9 | note |
|-------|-----|------|------|-----|-----|-------|------|
| idle | stock | — | — | 370 ns | 3.3 µs | 8.4 µs | floor |
| mid-drain | stock | 256 | 0 | 651 ns | 2.6 µs | 3.7 µs | empty Call hides coupling |
| mid-drain | stock | 256 | 1 µs | 5.3 µs | 22.5 µs | 26.3 µs | ~2 chunks left |
| mid-drain | stock | 256 | 10 µs | 50.5 µs | 212.2 µs | 322.9 µs | scales with job time |
| mid-drain | stock | 2048 | 1 µs | 12.1 µs | 31.5 µs | 318.6 µs* | p99.9 noisy |
| mid-drain | stock | 8192 | 1 µs | 14.2 µs | 32.2 µs | 1.31 ms* | p50/p99 flat |
| mid-drain | yield16 | 256 | 1 µs | 5.2 µs | 5.5 µs | 17.0 µs | visit ends after 16 |
| mid-drain | claim1 | 256 | 1 µs | 331 ns | 531 ns | 1.1 µs | visit ends after 1 |
| mid-drain | stock | 32 | 1 µs | 330 ns | 2.7 µs | 9.8 µs | small dump |
| burst | stock | 256 | 1 µs | 14.1 µs | 32.6 µs | 35.9 µs | first 13.9/31.3, last 14.9/32.9 |
| near-full | stock | 256 | 0 | 341 ns | 1.62 ms | 2.19 ms | admit 91ns/2.8µs; parked=31696 |
| mailbox | — | 256 | 1 µs | 90 ns | 150 ns | 131.4 µs | independent of dump size |
| mailbox | — | 8192 | 1 µs | 100 ns | 210 ns | 79.0 µs | |

*p99.9 is noisy run-to-run; p50/p99 are the stable tail signal.

`nc=8` oversub (pinned): idle p50 371 ns / p99 3.4 µs, mid 256/1 µs p50
8.4 µs / p99 25.5 µs — same story, not a scheduler lie.

**Stock tail tracks remaining shard work**, which grows with the tick dump. At a 256-job dump and 1 µs jobs, p99 is 22.4 µs (fine at 60 Hz). At 8192 it is 29.3 µs, still flat because of first-claimant yield. A dedicated mailbox is ~130 ns either way — the OOB channel the design refused. `write()==0` exists when consumers are parked (~31.7k jobs at this topology) but did not show up under concurrent spray.

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
| `--ac` | avgCost chunk hint (0..5) | 1 |
| `--small` | small-table threshold (0 = auto) | 64 |
| `--global-count` | old one-global-atomic callback | off |
| `--repeats` | timed repeats | 3 |
