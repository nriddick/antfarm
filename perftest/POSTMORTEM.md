# Ant Farm postmortem — revision 5 (2026-08-16)

Host: Ryzen 5 5500 (6c/12t), 16 MB L3. Farm default 16 MiB (`Ln = 2^21`
ulongs), `K = 8`, release `ldc2 -O2 -release`. Drivers in this directory;
raw tables in `last_sweep.txt`, `last_digest.txt`, `last_tail.txt`.
`fatal()` still prints and aborts under `-release`.

Code state: spec2 revision 5 — per-tier unsizable-table fatal, ticketed
write, avgCost chunk hint (`MAX_CHUNK = 32`, default 1), small-table
threshold as a farm field (default 64), 128-byte PayloadHeader, 64-byte
ticket slots, first-claimant mid-tick yield (5e-m), and the idle re-walk
cursor (this session: restores the idle floor and collapses the dump-size
tail coupling). Nothing here changes production defaults; `benchMaxRuns` /
`benchChunk` / `--ac` / `--small` are bench-only knobs, zero on the shipped
path.

This note is the current-state brief for a top-level game job system that
dumps work every tick and must also accept a wait-free mid-tick `write()`
on the same ring: no mutex, no pinned SPSC, no out-of-band channel.

---

## 1. Recommended configurations (start here)

One sensible starting point and the variations, on this host:

| Goal | Config | Expected (16 MiB, EPP=performance) |
|------|--------|-------------------------------------|
| Max payloads/s | `K=8 nc=8 nb=2 ns=0 body=2 batch=256` | **59.5 Mpps** |
| Max payloads/s, realistic body | `K=8 nc=8 nb=2 ns=0 body=16 batch=256` | ~58–61 Mpps |
| Max body bandwidth | `body=1024 batch=63..80` | ~3.2–3.5 Mpps / **26 GiB/s** |
| Zero-cost mid-tick tail | `nc=6..8`, 1-payload small-tier writes | idle p50 ~0.37 µs, mid-drain p99 ~30 µs |
| Never-full ring (headroom > throughput) | `Ln=32..256 MiB`, same topology | stalls = 0 but **−25..−38%** throughput |

Guiding rules, in order of how much they matter:

1. **Consumers ≥ 8, producers 2 bulk (or 4 small).** `nc=8 nb=2` is the
   robust winner; `K=4` and `K=8` are tied within ~1%. `nc=2` is the worst
   count (shard `sqcs=1` like one consumer plus an extra coherence
   participant). `nb=1` is a publish-side serialization tax that *grows*
   with buffer size (24.8 Mpps at 128 MiB vs 40.2 for nb=2).
2. **Batch ≥ 128; 256–512 is better.** The batch curve climbs to the quota
   ceiling: 63 → 128 → 256 → 512 gives ~59.7 → 60.4 → 61.7 → 62.1 Mpps
   (K=4, body=16). `batch=1` is 4–5× slower. The caller's quota should
   bound the table size.
3. **`avgCost` matches chunk to Call cost.** Default 1 (chunk 16). Cheap
   Calls (a few ns) → 0 (chunk 32, max amortization). Expensive Calls
   (body-touching, cold state) → 2–3 (chunk 8–4): no throughput loss for
   chunks 8–32, and p99.9 at 8192-job dumps trims 246 → 130 µs at chunk 8.
   Chunk 1 (`avgCost=5`) costs −10–12% throughput: never batch Calls of a
   class that small.
4. **Keep the buffer at 8–16 MiB on this host** (≈ L3). Larger rings buy a
   never-full guarantee at a 25% cliff (32 MiB) declining to −38% at
   256 MiB — memory subsystem, not farm mechanics (see §2.2). The TLB half
   of that decline is a huge-pages lever, not a code change.
5. **Quota defaults are fine.** Explicit multi-MB quotas inside a big
   segment (activating opportunistic renewal) measure identical to the
   sweep-based renewal — the Rt sweep is cold and cheap.
6. **Body 2–16 for payload rate; 1024 for bandwidth.** The throughput
   winner is `body=2 batch=63..256`; bandwidth saturates around `body=1024
   batch=63..80`.

---

## 2. Detailed performance results

### 2.1 Throughput (publish + empty Call, 16 MiB)

Phase-1 topology sweep (`body=16 batch=80`, median of 3), current code:

| K | nc | nb | ns | Mpps | note |
|---|----|----|----|------|------|
| 8 | 8 | 2 | 0 | **56.0–57.1** | winner (run-to-run band) |
| 4 | 8 | 2 | 0 | 55.5–56.8 | tied within noise |
| 8 | 8 | 0 | 4 | 51.6 | small-tier mix |
| 4 | 8 | 0 | 4 | 51.1 | |
| 8 | 4 | 2 | 0 | 51.4 | half consumers |
| 4 | 1 | 1 | 0 | 40.6–42.8 | single consumer |

Shape phase on the winner: best payloads/s `body=2 batch=256` → **59.5
Mpps**; best bandwidth `body=1024 batch=80` → **26.2 GiB/s**. The batch
curve (body=16, K=4, EPP=performance): 63 → 128 → 256 → 512 = 59.7 → 60.4
→ 61.7 → 62.1 Mpps — still climbing at 512, plateauing at the quota.

**avgCost** (K=8 nc=8 nb=2 body=16 batch=128): chunk 8/16/32 equivalent
(57.7–60.4, ±noise); chunk 1 → 52.4–52.8 (−10–12%). The digest arms agree
(body=16: 48.5 → 31.5 → 31.2 → 31.0 ns/job at chunk 1/16/32/64).

**Small-table threshold** (farm field, `--small {32,48,64,128,192,256,0}`)
at batch 63/64: no ordering outside the ±2% run-noise band. The 5e-m yield,
carried sweeper and idle re-walks already absorb whatever it controls; the
default stays 64.

**Wrap-fatals gating A/B**: verify-on 59.64 vs verify-off 59.31 Mpps
(median of 3, alternating) — ~0%, so the packed-field wrap fatals stay in
the release build as the corruption tripwire (`-d-version=noverify` is the
opt-out).

**Clock-independence**: with EPP=performance (~4.1 GHz) the winner moves
<1% vs powersave (~3.5 GHz). The winning topologies are coherence/atomic-
bound, not clock-bound; the old record's higher absolute numbers were an
earlier revision's behavior, not the clock.

### 2.2 Buffer size (2–256 MiB)

Full curve (K=8 nc=8 nb=2 body=16 batch=128): 2–16 MiB flat at ~58–60
Mpps; **cliff at 32 MiB (−25% → 43.3)**; then a gradual decline to 36.1
Mpps at 256 MiB. Three quota regimes (default, qb=1 MiB, qb=2 MiB) are
indistinguishable — the Exmax-within-segment hypothesis is falsified.
Stalls (`write()==0`) vanish at ≥ 32 MiB: the one operational win of a big
ring, at a real throughput cost. K=4 tracks K=8 exactly.

The cliff is L3 fit (16 MiB farm ≈ 16 MiB L3); the 32→256 decline is 4 KiB
TLB pressure over the doubled magic mapping (2×Ln virtual). Consumer-side
execute cost is size-invariant (digest 30.3 → 31.8 ns/job, 16 → 256 MiB)
and shuffled ≈ linear even at 256 MiB (31.3 vs 32.4 ns/job) — the walk is
not the bottleneck; the ring's contended mutable lines are.

### 2.3 Digest (execute-only window)

`linear16` vs `linear1` at body=16: **1.50–1.57×** — the claim dividend is
real and is the consume-side product line (one `fetch_add` per sixteen
`Call`s). Layout is not: `shuffled16 ≈ linear16` (0.94–1.03×) and
`copyout16` is free (0.99–1.00×), at 16 MiB and at 256 MiB. A 32 MiB cold
chase adds ~16 ns/job to every arm equally; 8 KiB bodies are bandwidth and
wash all three effects out. Chunk arms: the dividend saturates at 8–16 for
cheap bodies; 64/128 add ≤ 1%.

### 2.4 Tail (mid-tick publish → first Call, pinned nc=6, samples=10000)

Current code, after the idle re-walk cursor fix (this session):

| Scene | arm | tlen | spin | p50 | p99 | p99.9 | max | note |
|-------|-----|------|------|-----|-----|-------|-----|------|
| idle | stock | — | — | **371 ns** | 3.3 µs | 7.3 µs | 11.8 µs | floor restored |
| mid-drain | stock | 256 | 1 µs | 5.3 µs | 22.5 µs | 28.3 µs | 338 µs | |
| mid-drain | stock | 2048 | 1 µs | 12.0 µs | 29.8 µs | 167 µs | 410 µs | |
| mid-drain | stock | 8192 | 1 µs | **14.0 µs** | **30.0 µs** | 57.2 µs | 1.35 ms | dump coupling gone |
| mid-drain | stock | 256 | 10 µs | 50.5 µs | 212 µs | 253 µs | 358 µs | scales with job time |
| mid-drain | yield16 | 256 | 1 µs | 5.2 µs | 5.5 µs | 13 µs | 23 µs | attribution |
| mid-drain | claim1 | 256 | 1 µs | 321 ns | 430 ns | 1.0 µs | 7.4 µs | attribution |
| mid-drain | stock | 32 | 1 µs | 331 ns | 2.6 µs | 4.9 µs | 12 µs | small dump: fast dispatch |
| burst | stock | 256 | 1 µs | 14.2 µs | 32.9 µs | 37 µs | 51 µs | 32 sentinels, first ≈ last |
| near-full | stock | 256 | 0 | 341 ns | 5.6 µs | 1.6 ms | 1.6 ms | parked=31696, admit 100 ns |
| mailbox | stock | 8192 | 1 µs | 110 ns | 240 µs* | 368 µs | 1.31 ms | *p99 polluted by host load |

The story since the first suite:

- **The p99-that-grows-with-dump-size weakness is resolved.** First suite:
  1.8 ms p99 at 8192 jobs. Revision 4's first-claimant yield (5e-m) fixed
  the p50 (45 µs → and now 14 µs); this session's idle re-walk cursor fixed
  the parked-worker path that still inflated it (117 µs → 30 µs p99). The
  p99 is now **flat across dump size** (22.5 / 29.8 / 30.0 µs at 256/2048/
  8192) — no longer leftover-shard-work growth. The remaining max (~1.3 ms)
  is a rare scheduler outlier (p99.9 ≤ 57 µs).
- **The idle floor is back.** The T18 `sweepCurrentPosition` re-walk was
  re-scanning the whole current segment on every park — O(tables) per park,
  which grows with every mid-tick write — costing the idle p50 350 ns →
  3.4–4.4 µs and the 0ns-arm a transient `write()==0` (~7%). The per-view
  `sweepSeq` cursor (tables before it are confirmed complete) makes parks
  O(1): idle p50 371 ns, p99 3.3 µs (better than the pre-T18 5.5 µs), and
  the empty-Call arm's backpressure disappeared (zeros 186–751 → 0).
- **avgCost trims the extreme tail**: 8192/1 µs p99.9 246 µs at chunk 16 →
  130 µs at chunk 8 (chunk 1: 125 µs). Producers declaring expensive Calls
  get the trim for free.
- **The OOB channel still wins by construction**: dedicated mailbox poller
  p50 90–110 ns independent of dump size. The farm's mid-tick story is now
  "tens of microseconds" instead of "milliseconds", but a mailbox is still
  the only path that removes the ring entirely.
- **`write()==0` is rare while anyone is draining** and vanishes on big
  rings; parked consumers hit a real wall (~31.7k jobs at this topology)
  — the ring will not pretend to be unbounded.

### 2.5 Quotas, tiers, and renewal

- **Per-tier unsizable fatal** (revision 5): a payload whose minimal table
  exceeds the caller's tier quota aborts instead of returning `write()==0`
  forever — the escape hatch for dynamic payload sizes elsewhere in the
  program. `write()==0` strictly means "farm full".
- **Opportunistic renewal ≈ sweep-based renewal**: activating the
  `seqb - wtprime >= exmax` fast path with explicit multi-MB quotas changes
  nothing measured; the Rt sweep is cold (~once per 240 writes) and cheap
  (≤ 7 acquire loads).
- **Ticket slots are 64-byte strided** and the PayloadHeader is 128 bytes
  (Call adjacent to Pcount, claim-gated line sharing): registration CAS and
  the pcount line are isolated; no measured hot-path cost or change.

---

## 3. Settled readings

- Quote **payloads/s** for "can a tick dump enter the ring." Quote
  **ns/job of a body-touching `Call`** for consume. Quote **p99
  publish → `Call`** for mid-tick. Do not mix the three.
- The consume-side product line is **one claim, sixteen `Call`s** — not
  "the bytes are adjacent" (shuffle ≈ linear at every size).
- The mid-tick product line is now **the first claimant's run + a poll**:
  p99 ~30 µs at 1 µs jobs regardless of dump size; the mailbox remains the
  only dump-size-independent path (90–110 ns).
- Configure **nc=8, nb=2, K=4/8, batch ≥ 128, 8–16 MiB**, and declare
  `avgCost` per Call family. Everything else measured is a no-op or worse.
- Keep `fatal()` in release: a wrapped Tcount / unsizable payload / bad
  token is not a throughput event.
- Tail p99+ numbers should be read with the host-load caveat: this session
  ran with a desktop browser active (mailbox p99 131 ns → 240 µs across
  runs); p50 and the farm scenes are robust.

## 4. Not measured / open

- Makespan of a tick (first dump sentinel → last dump `Call`); dump size
  would dominate it even with a mailbox.
- Sleeping workers + futex wake; the farm's story is a spinning job system.
- Mid-tick while a table spans a segment (`plantIfUnprotected` /
  `migrateToFrontier` on the path).
- Huge pages for the magic-buffer shm mapping (the 32→256 MiB TLB decline).
- Real game `Call`s (world mutation, waits, fan-out); digest says the farm
  walk is a rounding error next to real work.
- A second farm, steal deque, or moodycamel as a *dump* path.
- The `sweepSeq` cursor's interaction with `sweeperNext` chaining across
  many small tables (exercised, green, but not separately benchmarked).

---

## Drivers

```
make -C perftest run          # 16 MiB topology + shape sweep
make -C perftest digest-run
make -C perftest tail-run
./throughput --once --ln $((128<<17)) --k 8 --nc 8 --nb 2 --body 16 --batch 128   # big-ring probe
```

How to run and the flag tables live in `README.md`. This file is the
verdict, not the lab notebook; calibration detail and the buffer-size study
live in `SPECULATIVE_OPT_2026-08-16_1.md` §7/§9.
