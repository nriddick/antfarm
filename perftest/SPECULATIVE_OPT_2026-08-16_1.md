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

| # | Change | Kind | Expected effect | Risk |
|---|--------|------|-----------------|------|
| 1 | Per-tier unsizable-table fatal in `write()` | correctness | `write()==0` becomes strictly "farm full" | none (spec-aligned) |
| 2 | Gate packed-field wrap fatals behind `version(verify)` | perf (consumer hot path) | ~1–3% consumer side | tripwire lost in release |
| 3 | Producer-published `avgCost` in Thead: `chunk = MAX_CHUNK >> avgCost` | perf strategy | chunk adapts to declared Call cost; tail vs throughput trade made per-table by the producer | none if header-published (agreement by construction) |
| 4 | `SMALL_TABLE_THRESHOLD` scaled by shard count | dispatch policy | more tables on the fast path; starvation risk bounded | low (yield + carried sweeper + T18 re-walks backstop) |
| 5 | Hoist write-side validation out of the retry loop; plain adds in Tindex build | perf (write side) | small; pure restructuring | none |
| 6 | `PayloadHeader` 17 → 16 words: move `call` next to `pcount`, filler2 7 → 6 words | layout | header 136 → 128 B (two exact lines); pcount line stays isolated | none — only if `call` stays claim-gated (see §8) |

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

**Proposal.** Gate them behind `version(verify)` — torture/TSAN builds keep
them, the perf release omits them — with an invariant comment at each site
stating the bound (Done/MaxCs ≤ 512, claim-per-payload, consumer limit).
Alternative: keep them, since they are ~1% and are the last line against
memory corruption. Decide after the chunk experiments (item 3) move more
cost than this.

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

Notes / open items (calibration, item 7):
- Ceiling: `MAX_CHUNK = 32` keeps the default at 16 with `avgCost = 1`. If
  the digest chunk arms show 32/64 still paying for very cheap Calls, raise
  the ceiling to 64 and default to `avgCost = 2` (still chunk 16).
- The `shbase >= 1024` switch disappears: chunk becomes a pure function of
  the producer's hint, which is also simpler to reason about.
- Truncation is unaffected: `write()` may commit `n < len`, but the hint
  still describes the written table.
- Item 4's `tlen >= sq * chunk` small-table rule then depends on the
  per-table chunk — a cheap table (chunk 32) shards at `tlen >= 3·32` (sq=3),
  an expensive one (chunk 8) at `tlen >= 24`. Consistent: the decision is
  per-table, made by consumers from the same header word.

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

**Proposal.** Shard iff `tlen >= sq * chunk`, else shard-0 wholesale
(small-table path). Concretely `threshold = clamp(sq * chunk, 16, 256)`:
48 for sq=3 (more tables on the fast path), 192 for sq=12 (avoids
micro-shards of ~1 chunk). Make it a farm field (read once per table, not an
enum) so the tail bench can sweep it.

Measure with the tail scenes (mid-drain 256/2048/8192), not the throughput
sweep: the threshold's value is dispatch latency, which the throughput bench
does not see.

---

## 5. Cheap release wins, zero correctness risk

- **Hoist write-side validation out of the retry loop.** The fit loop
  (antfarm.d:500–522) re-validates the whole batch (null headers, MaxCs/Done
  ranges, overflow, singleton size) on every `refreshQuota` retry. Validate
  once before the loop; the loop then only does candidate sizing. Pure
  restructuring; matters in stall-heavy mixes (nb=1 topologies show tens of
  thousands of stalls).
- **Plain adds in the Tindex build.** `addChecked` at antfarm.d:584/590 is
  redundant with the fit loop's overflow-checked `psum`: the running offset
  `po` is `payOff + Σpsz`, bounded by the table size which already passed
  checked arithmetic. Safe post-validation.
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

## 7. Calibration experiments before committing constants

1. **Chunk × body** (isolates claim amortization): extend `digest` with
   chunk arms {1, 4, 8, 16, 32, 64, 128} × body {2, 16, 64, 256}. Find
   where the amortization dividend saturates; this sets the cheap-Call chunk.
2. **Batch tail** (isolates table-size amortization at the top): throughput
   phase 2 on the winner with batch {63, 80, 128, 160, 256}; 128 won this
   session, confirm it is not noise.
3. **avgCost** (validates item 3): implement the optional
   `write(..., avgCost)` parameter + Thead word behind a flag, then sweep
   `avgCost ∈ {0, 1, 2, 3, 5}` on the throughput bench (body=16, cheap
   `Call`) — expect class 0 to win there — and on the tail scenes
   (mid-drain 256/2048/8192 with a body-touching `Call`), where the
   expensive classes must shorten p99 without moving Mpps. The digest chunk
   arms {1, 4, 8, 16, 32, 64} decide the `MAX_CHUNK` ceiling (32 vs 64).
4. **Threshold** (validates item 4): make it a farm field, sweep {32, 48,
   64, 128, 192, 256} on the tail scenes and the batch=63/64 pair on
   throughput.
5. **Wrap-fatals gating** (validates item 2): build with and without
   `version(verify)`; measure consumer-side Mpps delta. Adopt only if > 1%.

Order: item 1 first (one line, correctness), then 5 (pure restructuring),
then the calibration runs (7) before committing 3/4, and 2 last since it
trades a tripwire for < 3%.

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

### One proposed change: `PayloadHeader` 17 → 16 words

The pcount guard can drop one word (filler2 7 → 6) **only if the exposed
word is claim-gated** — data read exclusively by the consumer that just
wrote pcount (it already owns the line, so no extra coherence traffic).
`call` qualifies: `enterPayload` dereferences it only after the
`claims >= maxCs` gate. `maxCs`/`done` do *not* qualify — `tertiaryMt`
pre-checks them without claiming, so a claim-less reader would pull the
line. Proposed layout:

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

## 9. Audit provenance

Probe: `/tmp/layout_probe.d` (offsetof/alignof/sizeof + table-offset
arithmetic compiled against `antfarm.d`; output captured 2026-08-16).
Sweep records this session: `last_sweep.txt`, `sweep_current2.txt`,
`sweep_preyield.txt`, `sweep_t18prev.txt`; environmental note at the top
of this file.
