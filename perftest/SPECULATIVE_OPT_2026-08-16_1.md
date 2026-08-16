# Speculative optimizations for a release build

Status: **proposals only — nothing here is implemented.** Reviewed against
`antfarm.d` at HEAD `969a9e9` (rev4: first-claimant mid-tick yield) and the
16 MiB sweeps recorded in `last_sweep.txt` / `sweep_current2.txt` /
`sweep_preyield.txt` / `sweep_t18prev.txt`.

Host: Ryzen 5 5500 (6c/12t), 16 MiB L3, 16 MiB farm (`Ln = 2^21`).
One environmental note from the re-sweep: with `powerprofilesctl set
performance` (EPP=performance, ~4.1 GHz under load) the winner was unchanged
(~56.8 Mpps vs ~56.7–57.1 under powersave/EPP=power) — the winning
topologies are coherence/atomic-bound, not clock-bound. The old record's
higher absolute numbers were an earlier revision's behavior, not the clock.

Guiding rule for every item: a release build may trade provably-dead checks
and redundant work, but it may not change observable behavior (fatal on
spec-violating input stays fatal; hot-path fetch-add-only stays
fetch-add-only; no CAS is introduced).

---

## 0. Summary

| # | Change | Kind | Status (2026-08-16) | Result |
|---|--------|------|---------------------|--------|
| 1 | Per-tier unsizable-table fatal in `write()` | correctness | **implemented** | `write()==0` strictly "farm full"; T17 extended; T13 covers nb==0 |
| 2 | Gate packed-field wrap fatals | perf (consumer) | **evaluated, not adopted** | A/B ~0% (verify 59.64 vs 59.31 Mpps) — below the 1% bar; checks stay on, opt-out only |
| 3 | `avgCost` chunk hint (MAX_CHUNK=32, default 1) | perf strategy | **implemented** | chunk 1 costs −10–12% throughput; chunks 8–32 equivalent; tail p99.9 8192: 246→130 µs at chunk 8 |
| 4 | Small-table threshold farm field + auto rule | dispatch policy | **implemented** (default 64) | no measurable effect in throughput or tail sweeps; default unchanged |
| 5 | Plain adds in Tindex build (hoist evaluated, not applied) | perf (write) | **implemented** | no hoist (would fatal on truncated-batch tails); plain adds safe |
| 6 | `PayloadHeader` 17 → 16 words, `call` next to `pcount` | layout | **implemented** | 128 B; baseline + T01–T18 green |

---

## 1. Per-tier unsizable-table fatal (correctness — do first)

**Problem.** `write()` validates each payload's *minimal table* only against
`maxExi = max(quotaBulk, quotaSmall)` (antfarm.d:514):

```d
immutable singleton = tableSizeChecked(1, oneMt, sq, psz);
if (singleton > maxExi)
    fatal("payload larger than any producer Exi");
```

A payload whose singleton lands in `(quotaSmall, quotaBulk]` clears this
check, then the fit loop breaks at `i == 0` with `n == 0`; `refreshQuota`
refills to exactly `quota` (the caller's tier quota) and the candidate still
does not fit, so `write()` returns 0 — **forever**. The caller spins on
permanent "full" for a payload no producer of its tier can ever publish.
Dynamic payload sizes elsewhere in the program are exactly what trips this.

The harness already models the intended rule and works around the code gap
(throughput.d:185):

```d
immutable qb = c.nb == 0 ? c.qs : (c.qb == 0 ? 0 : c.qb);   // workaround
```

**Fix** (one line, antfarm.d:514):

```d
if (singleton > quota)
    fatal("payload larger than this producer tier's Exi");
```

`quota` (antfarm.d:487) is the caller's tier quota and `quota <= maxExi`, so
the new check is strictly stronger and subsumes the old one. After this,
`write() == 0` provably means the farm is genuinely full, which is the
contract spec 4a promises ("never that a single payload is unsizable").

**Tests.** T17 (`review_torture/torture_tests.d:773`, `expectAbort` + fork)
keeps passing — its "unsizable" case (2048-ulong body vs quotaSmall=512)
trips both old and new checks. Add a T17 extension: farm with
`quotaBulk > quotaSmall`, push a payload sized in `(quotaSmall, quotaBulk]`
from a *small* tier producer, assert SIGABRT (and from a bulk producer that
it *writes*). Then delete the throughput.d:185 workaround and rely on
`cfgFits` alone.

---

## 2. Dispensible asserts: packed-field wrap fatals (consumer hot path)

The wrap checks in the consume path guard states that are **provably
unreachable** under the spec's own caps:

| Check | Location | Guarded state | Why unreachable |
|-------|----------|---------------|-----------------|
| `Pcount` claims-wrap | antfarm.d:1145 | 2^32 claim increments on one payload | claims issued ≤ visitors × entry multiplicity ≲ a few hundred (MAX_CONSUMERS_LIMIT=128, ~4 entry paths/consumer) |
| `Pcount` calls-wrap | antfarm.d:1150 | 65536 call increments on one payload | calls ≤ claims + Done ≤ ~1024 |
| `Pcount` comps-wrap | antfarm.d:1157 | 65536 completions on one payload | comps ≤ Done ≤ 512 |
| `Tcount` claims-wrap | antfarm.d:1225 | 2^32 claim strides on one shard | claims ≲ shiter + visitors |
| `Tcount` comps-wrap | antfarm.d:1254 | 2^32 completions on one shard | comps == shlen ≤ tlen (quota-bounded) |

These are tripwires for corruption/bugs, not legitimate traffic. Cost: the
two per-iteration checks in `enterPayload` (calls, comps) run on every
payload iteration — ~2 branches on already-loaded registers, roughly 1–3%
of consumer-side payload cost. The per-chunk ones in `processShard` are
nearly free (one branch per claim RMW).

**Verdict — evaluated, NOT adopted (2026-08-16).** The A/B (K=8 nc=8 nb=2
body=16 batch=128, 3 alternating runs each, median of 3): verify-on 59.64
Mpps vs verify-off 59.31 Mpps. The gating saves ~0% — below the 1% adoption
bar, and the branches are free on already-loaded registers. The checks stay
in the release build as the corruption tripwire; the implementation remains
in `antfarm.d` as `VERIFY_WRAPS` (constant-folded, default true) with
`-d-version=noverify` as the opt-out for anyone who wants to re-measure on
a different workload.

**Do not remove:** `requireToken` (spec 3b, one load per `write()`), the
write-side header validations (spec 4a — item 1 argues those fatals are the
*right* tool for dynamic sizes), and the `claims >= maxCs` overallocation
gate in `enterPayload` (real correctness).

---

## 3. Chunk sizing: producer-published chunk hint

**Problem with the current constants.** `CLAIM_CHUNK = 16` and
`BIG_CHUNK = 64` (antfarm.d:42–44) with the switch
`shbase >= BIG_CHUNK * 16 ? BIG_CHUNK : CLAIM_CHUNK` (antfarm.d:1198) were
guessed, and the evidence says they are in the right family but cannot be
tuned where it matters:

- Digest: claim-16 beats claim-1 by 1.5–1.6× at body=16; washes out at
  body ≥ 1024. Claim amortization is real and saturates early.
- Throughput batch curve plateaus at batch ≥ 32; batch=128 is now the best
  (58.5–59.5 Mpps vs 56.7–57.1 at 80). Amortization still pays at the top.
- The `BIG_CHUNK*16 = 1024` threshold means chunk 64 only applies at
  `tlen ≳ 3·1024` (sq=3). The sweep's best tables (tlen 63–128) never see
  it — BIG_CHUNK is effectively dead today.

**The real tension.** p99 tail is leftover shard work and scales with chunk
size; throughput wants big chunks (fewer `Tcount` RMWs). Cheap Calls and
expensive Calls want opposite chunk sizes, and one global rule cannot serve
both. Body size is a poor proxy for Call cost — the digest bench's "cold
chase" arm showed a 16-ulong body can be arbitrarily expensive — so the
signal should come from the producer, who wrote the Call.

**Proposal: a producer-declared cost class, `avgCost`.** `write()` gains an
optional trailing parameter `uint avgCost = 1`, a log2 cost class in
`0 .. log2(MAX_CHUNK)`. The producer knows its Call's cost profile; it
declares how much the chunk should shrink from the ceiling:

```d
// producer side (write()): the default is the middle of the range.
//   0 = Calls are very cheap  -> chunk shifts by 0 (ceiling, max amortization)
//   n = Calls are more costly -> chunk shifts down by n (short visits, less tail)
// validation: avgCost > log2(MAX_CHUNK) is a caller error -> fatal
w[6] = avgCost;                 // Thead spare word (antfarm.d:577, was 0)

// consumer side (processShard):
immutable specChunk = MAX_CHUNK >> avgCost;   // read from bp[idx + 6]
```

Concrete setting that matches "default is in the middle": `MAX_CHUNK = 32`,
`avgCost ∈ [0, 5]`, default `avgCost = 1` → chunk **16**, the current
`CLAIM_CHUNK` and the midpoint of [1, 32]. Cheap-call producers pass 0
(chunk 32), mid-cost keep the default (16), expensive pass 2–3 (8–4), and a
Call that should never batch passes 5 (1). The bench override
(`benchChunk != 0`) keeps precedence over the header value so the existing
yield16/claim1 tail attribution still works.

Safety: the postmortem's "mixed chunk sizes on one Tcount deadlock" required
consumers disagreeing on the *same* table's `shiter`. A header-published
`avgCost` is read identically by everyone, so agreement holds by
construction. The claim/completion arithmetic is per-table and unchanged.
The 5e-m yield interacts as intended: expensive Calls get small chunks, so
the first claimant finishes a run sooner and the leftover (and p99) shrinks.

**Implemented (2026-08-16).** `write(..., avgCost = 1)` validates the range,
publishes `w[6] = avgCost`; consumers compute `chunk = MAX_CHUNK >> avgCost`
(clamped). Calibration results:

- **Throughput** (K=8 nc=8 nb=2): chunk 1 (avgCost=5) is a clear regression
  (−10 to −12%: 52.4–54.9 vs 57.7–60.4 Mpps), confirming claim amortization
  matters; chunks 8/16/32 (avgCost 2/1/0) are equivalent within ±4% noise.
  The digest arms agree: at body=16 the dividend saturates at chunk 8–16
  (31.5→31.2→31.0 ns/job at 16/32/64; chunk 1 is 48.5), so MAX_CHUNK=32
  captures the ceiling — no need to raise it to 64.
- **Tail** (mid-drain 8192/1 µs, stock): p50/p99 are flat across avgCost
  (≈50 µs / ≈115 µs — the 5e-m yield already owns the tail); p99.9 trims
  with smaller chunks (chunk 16: 246 µs → chunk 8: 130 µs → chunk 1:
  125 µs). So expensive-Call producers get a real extreme-tail win at no
  measured cost elsewhere.
- Default `avgCost = 1` (chunk 16) stays; producers with cheap Calls may
  pass 0 (chunk 32) for the small amortization edge at no tail risk.

Notes / open items:
- Ceiling: `MAX_CHUNK = 32` confirmed by the digest arms (64/128 add ≤ 1%).
- The `shbase >= 1024` switch is gone: chunk is a pure function of the
  producer's hint, simpler to reason about.
- Truncation is unaffected: `write()` may commit `n < len`, but the hint
  still describes the written table.
- Item 4's auto rule depends on the per-table chunk; the farm-field
  override is orthogonal (see §4).

---

## 4. `SMALL_TABLE_THRESHOLD`: scale with the shard count

`SMALL_TABLE_THRESHOLD = 64` (antfarm.d:40) was guessed. The data shows no
throughput cliff at the crossing (batch=63 vs 64 at body=16: 55.1 vs 55.9
this session; reversed in the old record) — it is a dispatch-policy knob,
not a throughput one.

The fixed 64 ignores the shard count: at `sq=12` a tlen=63 table
concentrates on shard 0 while 11/12 of the consumers skip-and-advance (the
mechanism that gives tlen=32 a 2.7 µs mid-tick p99), but starvation exposure
grows whenever no shard-0-capable consumer arrives. The backstops (5e-m
yield, carried sweeper, T18 idle re-walks) make a higher threshold safe.

**Implemented (2026-08-16): farm field + auto rule, default stays 64.**
`create(..., smallThreshold = 64)`; 0 selects the auto rule
`clamp(sq * chunk, 16, 256)` computed per table from the same header words
every consumer reads. `--small` sweeps the field in the benches.

Calibration verdict — **no measurable effect; the default is unchanged.**
- Throughput: batch=63 and batch=64 × small ∈ {32, 48, 64, 128, 192, 256,
  0} all land in 57.1–60.4 Mpps with no ordering — the band is run noise
  (identical configs in adjacent phases differed by ±2%).
- Tail: mid-drain 256/2048/8192 × small ∈ {32, 64, 128, 256} are flat
  (8192 p99 ≈ 118–124 µs across all values).

The 5e-m yield + carried sweeper + T18 re-walks already absorb whatever the
threshold controls, so 64 (pre-revision behavior, zero change for existing
configs) is kept; the auto rule remains available for farms with very
different shard counts.

---

## 5. Cheap release wins, zero correctness risk

- **Hoist write-side validation out of the retry loop — evaluated, NOT
  applied.** The fit loop (antfarm.d:500–522) re-validates on every
  `refreshQuota` retry, but retries are rare (only when nothing fits and a
  refresh succeeds), so the win is negligible. Hoisting to the top of the
  call would change behavior: the loop validates payloads lazily as they
  are accepted, so a truncated batch never touches entries past the
  truncation point; an up-front pass would fatal on uninitialized tails of
  partially-populated arrays. Lazy validation is the spec contract
  ("validates every payload header before accepting it").
- **Plain adds in the Tindex build — implemented (2026-08-16).**
  `addChecked` in the Tindex pass was redundant with the fit loop's
  overflow-checked `psum`: `po` is bounded by the table size (≤ quota ≤
  exmax ≪ ulong.max), so the plain adds cannot wrap. Zero behavior change.
- **`moveRef`'s extra acquire** of `stats[ki].es` (antfarm.d:931) is
  belt-and-braces — the sentinel acquire already orders the segment stats.
  Keep unless measured; it runs once per table transition.

---

## 6. Explicitly not touching

- **`fatal()` in release.** Project philosophy ("most errors are fatal",
  spec 1), the harness README's verdict, and item 1's whole point. A wrapped
  Tcount / unsizable payload / bad token is not a throughput event.
- **The 5e-m yield rule.** It is the p50 fix, and the "shared shard only"
  gate is what keeps it correct (at most one early-leaver per shard, shard
  cannot strand, idle re-walk is the backstop). The A/B shows it is ~free
  for throughput at the optimum (56.7–57.1 current vs 56.4 pre-yield).
- **Registration/ticketing, quota arithmetic, segment confirmation.**
  Cold-path or correctness-critical; no wins there.

---

## 7. Calibration experiments — DONE (2026-08-16)

Raw data: `calib_sweep.txt`, `calib_throughput.txt`, `calib_item2.txt`.

1. **Chunk × body** (isolates claim amortization): digest `--chunk {1, 4,
   8, 16, 32, 64, 128}` × body {2, 16, 64, 256}. Amortization saturates at
   chunk 8–16 for cheap bodies (body=16: 48.5 → 31.5 → 31.2 → 31.0 ns/job
   at 1/16/32/64; 64/128 add ≤ 1%). At body=256 the effect washes out
   (bandwidth-bound, noisy). **MAX_CHUNK = 32 confirmed.**
2. **Batch tail** (table-size amortization): batch {63, 80, 128, 160, 256,
   512} on both K=8 and K=4 at body=16. The curve keeps climbing to ~256–512
   (K=4: 59.7 → 60.4 → 60.9 → 61.7 → 62.1 Mpps at 63/128/160/256/512),
   plateauing at the quota ceiling. **"Plateau at 32" is wrong — batches ≥
   128, larger still pays.** README guidance updated.
3. **avgCost** (validates item 3): implemented + swept {0, 1, 2, 3, 5}.
   Chunk 1 costs −10–12% throughput; chunks 8–32 equivalent; tail p99.9 at
   8192 trims 246 → 130 µs going 16 → 8. **Default 1 kept.**
4. **Threshold** (validates item 4): farm field + `--small` sweep {32, 48,
   64, 128, 192, 256, 0} on throughput (batch 63/64) and tail (256/2048/
   8192): no measurable effect anywhere. **Default 64 kept (zero behavior
   change); auto rule available.**
5. **Wrap-fatals gating** (validates item 2): A/B verify-on vs off at the
   winner: 59.64 vs 59.31 Mpps — ~0%, below the 1% bar. **Not adopted;
   checks stay on, `-d-version=noverify` opt-out left in place.**

Order executed: item 1 (correctness) → 5 (restructuring) → 6 (header) →
3/4 (implemented behind defaults) → calibration 1–4 → 2 last. All changes
green on baseline + T01–T18 (LDC and DMD variants).

---

## 8. Buffer layout & alignment audit (2026-08-16, compiled probe)

Audited against the same HEAD as the rest of this file. The rule: within
the magic buffer everything is only 8-byte aligned, so every *concurrent-
mutable* word must be isolated on its own 64-byte cache line by 56 bytes of
padding (7 ulongs). Metadata arrays live in the 64-aligned `AntFarm` and
must be 64-aligned there.

### Verified — metadata arrays are properly 64-aligned

Probe (compiled against `antfarm.d`):

| Field | offset | mod 64 |
|-------|--------|--------|
| `AntFarm` (aligned_alloc(64) base, alignof 64, sizeof 14912) | — | 0 |
| `Rt` | 576 | 0 |
| `Lt` | 1600 | 0 |
| `stats` | 13888 | 0 |
| `Wt` / `Eg` / `Cf` | 128 / 192 / 256 | 0 |

Each `Rt[ki]` and `Lt` row is 8 ulongs = 64 bytes (56 dead bytes after the
used word), so the root/leaf tallies are line-isolated. `SegStats` is
padded to 64 (static assert) with `sd` (the only consumer-mutated word)
inside the same line as producer-written `es/seqt/cs/sqcs` — safe: consumers
touch `sd` only after validating a sentinel of that epoch, which
happens-after the producer's writes.

### Verified — every concurrent-mutable word in a table has its 56-byte guard

| Mutable spot | Guard | Check |
|--------------|-------|-------|
| `Tprogress` | 7 pad words before (PROG_PAD) + 7 after | ✓ both sides |
| `Tcount[shi]` | 7 pad words after each (64 B/shard slot) | ✓ |
| `PayloadHeader.pcount` | 7 filler2 words after (56 B); before-side is read-only header filler | ✓ |

The buffer-relative phase is arbitrary — probe with `n=80` puts the first
pcount at mod64 55/63/7/15 for sq=1..4, and consecutive pcounts are ≥ 136 B
apart — so the after-guard is what isolates the line, and it is present in
every phase. No fix needed.

### One proposed change: `PayloadHeader` 17 → 16 words — IMPLEMENTED

The pcount guard can drop one word (filler2 7 → 6) **only if the exposed
word is claim-gated** — data read exclusively by the consumer that just
wrote pcount (it already owns the line, so no extra coherence traffic).
`call` qualifies: `enterPayload` dereferences it only after the
`claims >= maxCs` gate. `maxCs`/`done` do *not* qualify — `tertiaryMt`
pre-checks them without claiming, so a claim-less reader would pull the
line. Implemented layout (2026-08-16):

```d
struct PayloadHeader
{
    uint maxCs, done;
    ulong plen;
    ulong[6] filler;        // read-only
    shared ulong pcount;    // offset 64 (was 72)
    Callback call;          // offset 72 — claim-gated, inside pcount's line
    ulong[6] filler2;       // 48 B; pcount line fully covered (8 + 48 = 56)
}
static assert(PayloadHeader.sizeof == 128);   // was 136; PHEAD_LEN 16
```

Verified by probe: `PH2.sizeof == 128`, pcount@64, call@72, guard after
pcount = 56 B — in the worst line phase pcount's line is exactly
`pcount + call + filler2`, never reaching the next header or the body.
All code uses `PHEAD_LEN`/`PayloadHeader.sizeof` symbolically, so the
change is mechanical; spec 4a's "17 ulongs = 136 bytes" note must become
16 ulongs = 128 bytes.

### Producer ticket arrays — 64-byte-strided (implemented 2026-08-16)

`prodHashBulk`/`prodHashSmall` were 8-byte-spaced `calloc` slots. The
tickets are shared mutable state under the registration CAS, so adjacent
hashes shared cache lines. Fixed in `antfarm.d`:

- Each array is `aligned_alloc(64, cap * PROD_SLOT_STRIDE * 8)`, zeroed
  with `memset` (aligned_alloc does not zero). The 64-aligned base also
  keeps slot 0's line from bleeding into an unrelated heap chunk.
- `PROD_SLOT_STRIDE = 8` ulongs per slot; every access goes through
  `prodSlot(hashes, i) = hashes + i * PROD_SLOT_STRIDE` (registration scan
  + CAS, unregister check/clear, `requireToken`).
- Runtime probe: both bases ≡ 0 mod 64, slot words 64 bytes apart and
  line-aligned.

Registration remains a cold CAS path; the padding removes the shared-line
bounce at no hot-path cost. Baseline + T01–T18 green; TSAN's only report is
the pre-existing harness race in T06 (`StormCtx.cycles`, closures capture
the loop-local by reference) — noted in `review_torture/README.md`.

---

## 9. Buffer-size study (2026-08-16)

Motivation: does a larger ring pay — either because 16 MiB ≈ L3 is a cliff,
or because a multi-MB Exmax fits inside a larger buffer's segment span and
activates opportunistic quota renewal (`seqb - wtprime >= exmax` in 3a/4b)?
Raw data: `calib_buffersize.txt`, `calib_buffersize2.txt`.

Method: `throughput --once`, K=8 nc=8 nb=2 ns=0, body=16, batch=128,
repeats=3, EPP=performance, Ln ∈ {2, 4, 8, 16, 32, 64, 128, 256} MiB
(`--ln MiB<<17`), across three quota regimes:

| Ln | default (qb=segCap, renewal inert) | qb=1 MiB (active ≥8 MiB) | qb=2 MiB (active ≥16 MiB) |
|----|----|----|----|
| 2 MiB | 59.1 | — | — |
| 4 MiB | 58.3 | 56.0 | — |
| 8 MiB | 59.8 | 59.5 | 59.1 |
| 16 MiB | 57.9 | 58.6 | 57.3 |
| **32 MiB** | **43.3** | **43.5** | **44.3** |
| 64 MiB | 41.2 | 41.8 | 41.3 |
| 128 MiB | 39.3 | 39.3 | 39.8 |
| 256 MiB | 36.1 | 37.0 | 36.1 |

K=4 tracks K=8 exactly at 8/32/128/256 MiB.

### Findings

1. **Sweet spot is 8–16 MiB (~58–60 Mpps).** 16 MiB equals the 16 MiB L3;
   2–4 MiB are equally good. Nothing is gained beyond it.
2. **The cliff at 32 MiB (−25%) and the continued decline to 36 Mpps at
   256 MiB are memory-subsystem effects, not the Exmax mechanics.** The
   renewal hypothesis is falsified: activating opportunistic renewal
   (qb=1/2 MiB so exmax ≤ segCap for Ln ≥ 32 MiB) changes nothing vs the
   sweep-based renewal — the Rt sweep is cold (~once per 240 writes) and
   cheap (≤ 7 acquire loads). The decline tracks the contended mutable
   lines (Tcount/Pcount/Rt) and payload streams falling out of L3, then
   4 KiB TLB pressure growing with the doubled magic mapping.
3. **Stalls vanish at ≥ 32 MiB** (0 vs ~150–270k at ≤ 16 MiB): the one
   operational win of a big ring — producers never see `write()==0` — at
   the cost of 25–40% throughput.
4. **Publish is the sensitive side at scale**: nb=1 at 128 MiB collapses to
   24.8 Mpps (vs 40.2 for nb=2); ns=4 lands at 36.4.
5. **Consumer-side execute cost is size-invariant** (digest linear16
   body=16: 30.3 → 31.8 ns/job from 16 → 256 MiB) and **shuffled ≈ linear
   even at 256 MiB** (31.3 vs 32.4 ns/job) — the earlier "layout does not
   matter at scale" finding holds; the walk is not the bottleneck, the
   ring's contended lines are.

### Implication

Keep 16 MiB for throughput on this host. A larger ring buys a never-full
guarantee at a real cliff; the TLB half of the 32→256 decline is a
kernel-side lever (huge pages for the shm mapping), not a farm change.

---

## 10. Audit provenance

Probe: `/tmp/layout_probe.d` (offsetof/alignof/sizeof + table-offset
arithmetic compiled against `antfarm.d`; output captured 2026-08-16).
Sweep records this session: `last_sweep.txt`, `sweep_current2.txt`,
`sweep_preyield.txt`, `sweep_t18prev.txt`, `calib_sweep.txt`,
`calib_throughput.txt`, `calib_item2.txt`, `calib_buffersize.txt`,
`calib_buffersize2.txt`; environmental note at the top of this file.
