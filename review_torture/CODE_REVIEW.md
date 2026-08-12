# Antfarm Code Review (gen6 / spec2)

Reviewed: `antfarm.d` against living `spec2`, with baseline `antfarm_test.d` and the new
`review_torture/` suite. Mentorship tone: the architecture is serious and mostly
coherent; the remaining defects are not cosmetic.

## Summary

The design is unusually careful for a lock-free M:N work queue: no CAS on hot
paths, magic-buffer publication, Sub0 pulse, trailing references + Sd completion,
sweeper/tertiary MT drain, and carried sweeper for small tables. Baseline tests
pass on LDC/DMD, release, and ThreadSanitizer.

That is not a free pass. Torture testing confirmed **five real defect classes**:
T01/T02 packed Pcount carry + over-exec, T16 position-ref stall, T17 size-wrap
smash, **T18 pure-churn orphan / lossless loss**. Existing tests never stressed
Exmax≫segCap idle consumers, Done near the 16-bit packed-field width,
adversarial `PayloadBody.length`, or churn **without** a steady consumer.

**Verdict:** do not ship as a general-purpose queue until **T18 (lossless under
pure churn)**, T16, T17, and the Pcount packing story are fixed. Within the
geometries already covered by `antfarm_test.d` (always ≥1 steady consumer,
trusted body lengths, modest Exmax), behavior is solid.

## Evidence runs

| Suite | Toolchain | Result |
|-------|-----------|--------|
| `antfarm_test.d` | ldc2 -O0, dmd -g, ldc2 -O3 -release ×3 | ALL PASSED |
| `antfarm_test.d` | ldc2 -fsanitize=thread | ALL PASSED |
| `review_torture` | ldc2 -O1 | correctness T03–T15 OK; defects T01/T02/T16 confirmed; exit path reports 4 defect lines |

Commands:

```
make -C review_torture run
# or
ldc2 -g -O1 review_torture/torture_common.d review_torture/torture_tests.d antfarm.d \
  -of=review_torture/torture_tests && ./review_torture/torture_tests
```

Exit codes: `0` all green; `2` correctness green + defects confirmed; abort on correctness fail.
Latest log: `review_torture/last_run.log`.

---

## Critical Issues (Must Fix)

### C1. Idle consumers pin a completed position segment → producer deadlock under large Exmax

**Status:** CONFIRMED (T16)  
**Where:** `ConsumerView.consumeNext` / `moveRef` / `tryReleaseTrailing`  
  (`antfarm.d` ~636–716, 734–796)  
**Spec:** 5j retained-reference invariant + 3a quota renewal

**Mechanism**

1. Consumer validates tables through a segment and advances `nextSeq` to `Tnext`.
2. When the next table is not yet published (`nextSeq == Wt`), `consumeNext`
   returns false. `moveRef` only runs after a successful sentinel validation, so
   the consumer **never migrates** onto the frontier segment that only exists as
   a `Tnext`/`Seqt` target.
3. The old segment may already be fully complete (`seqt + sd >= seqtNext` and
   next Es initialized), but it is held as the **position** reference, not a
   trailing one. `tryReleaseTrailing` does not touch position refs.
4. Producers with `Exmax` spanning many segments need contiguous zero-Rt free
   space to renew quota. A single held completed segment in the sweep window
   starves renewal forever while consumers spin idle at Wt.

**Observed (T16)**

```
written=36/120 total=36 next=29562 trailN=0 holdKi=4
ki=4 rt=1 es=4 seqt=19708 sd=9854   # held, complete
ki=5..7 seqt=29562 sd=0             # frontier, no consumer ref migration
```

Consumers sit at `nextSeq == Wt` with `trailN==0` and a live count bit on a
finished segment. Producers stall; progress is zero.

**Why baseline tests missed it**

`antfarm_test` geometries keep Exmax small enough relative to free segments, or
end by unsubscribing (dropping the pin). T16 uses `Exmax = 20000`, `segCap = 4096`.

**Fix direction**

On the idle path (sentinel miss at `nextSeq`), if the position segment is
`confirmed(curKi, curEi)` and `nextSeq` points at/into a newer epoch, either:

- migrate the position ref forward to the segment of `nextSeq` (inc-ahead, then
  dec old), or
- demote the completed position to a trailing slot and immediately try-release it.

Also consider: when `nextSeq >= Wt` and position is confirmed complete, drop to a
“frontier hold” policy that does not pin a finished segment (Sub0 already covers
empty-consumer cases).

**Regression:** keep T16 red until fixed; then it should go green and exit 0.

---

### C2. Packed `Pcount` fields carry on overflow (claims/calls/comps)

**Status:** CONFIRMED (T01 unit + T02 live)  
**Where:** `enterPayload` (`antfarm.d` 892–909), layout comment at 118  
**Spec:** 5f

**Layout**

```
Pcount: 32 MSB claims | 16 bits calls | 16 LSB completions
```

Updates are independent `fetch_add` on field strides (`1<<32`, `1<<16`, `1`).
There is no saturation and no CAS-loop on the field.

**Failure**

- At `calls == 0xFFFF`, another `fetch_add(1<<16)` increments **claims**.
- At `comps == 0xFFFF`, another `fetch_add(1)` increments **calls**.

T01 demonstrates both with direct atomics. T02 publishes `Done=0xFFFF`,
`MaxCs=32`, 16 consumers: observed **calls=1_048_545** vs done=65535
(over-execution by ~16×).

**Impact**

- Spec allows `Done` up to `2^16-1`. Near that limit, extra failed claim/call
  attempts wrap the field and re-open work → duplicate side effects.
- Even with modest `Done`, a pathological flood of oversubscribed entrants can
  walk the 16-bit call counter up to wrap if they keep taking claim slots and
  failing the done check (less likely if MaxCs is small and primary burns one
  claim, but tertiary/secondary loops still add).

**Fix direction**

Prefer separate atomics (`shared uint claims`, `shared ushort calls`,
`shared ushort comps`) or a single CAS loop that rejects increments that would
carry. At minimum, validate/document a safe operating envelope
(`Done + worst_case_failed_exits < 2^16`) and fatal on MaxCs*pathological bound —
but that is a band-aid; packing without saturation is the bug.

**Note:** `write()` validates `Done ∈ 1..0xFFFF` but does **not** cap MaxCs
against anything that would bound failed exits.

---

### C3. `write()` table-size arithmetic wraps → under-reserve / memory smash

**Status:** CONFIRMED (T17, SIGILL)  
**Where:** `antfarm.d` 386–403, copy at 477  
**Spec:** 4a size/truncation must reflect real payload bytes

**Mechanism**

```d
immutable psz = PHEAD_LEN + pe.body.length;  // wraps if body.length huge
immutable cand = THEAD_LEN + ... + psum + psz + END_PAD;
if (cand > exi) break;  // wrapped cand can be tiny and "fit"
...
immutable size = ...;   // also wrapped
atomicOp!"+="(Wt, size);
// then memcpy body of enormous length → smash
```

With `body.length = size_t.max - 16`, `cand` wraps into a value ≤ small quota.
T17 forks a child into `write()`; child dies **signal 4 (SIGILL)**.

**Fix:** Reject on overflow before accepting a payload:
`if (psz < pe.body.length || cand < psz || psum + psz < psum) fatal(...)`,
and/or hard-cap `body.length` so `size ≤ min(exi, remaining policy bound)`.

**Regression:** T17 must go from defect → clean reject (fatal or n==0, no signal).

---

### C4. Pure subscription churn orphans incomplete early segments (lossless violation)

**Status:** CONFIRMED (T18, 3/3 trials; also 5/5 external probe)  
**Where:** `unsubscribe` / Sub0 plant (`antfarm.d` 613–630), attach-in-place subscribe  
**Spec:** 5b last-unsub Sub0 at earliest unconfirmed held segment; 5j “incomplete
segment protected by every passerby still subscribed”; §1 safety paragraph

**Mechanism**

1. Churners subscribe, consume a short burst, unsubscribe — **no steady consumer**.
2. Non-last unsubscribers drop **all** held refs, including trails on incomplete
   early segments.
3. The eventual last unsubscriber may only still hold a **later** segment, so
   Sub0 is planted there.
4. Early segment remains with `sd < seqtNext - seqt` and `Rt_low == 0`
   (unprotected incomplete work).
5. A late subscriber attaches at the pulse `Seqt` and **never walks back** into
   the orphan. Farm metadata looks clean (`Cf==0`, one pulse, zero count bits)
   while payloads sit at 0 calls.

**Observed (T18)**

```
trial 0: pre=192 post=640 exp=1200 orphans=1 zeros=280 pulses=1
trial 1: pre=192 post=632 exp=1200 orphans=1 zeros=284 pulses=1
trial 2: pre=192 post=589 exp=1200 orphans=1 zeros=311 pulses=1
```

**Why baseline missed it:** `testChurn` / `testSmallTableChurn` always keep
`nsteady >= 1`.

**Safety impact:** `Rt==0` on incomplete segment lets producers treat it as free
on the next lap and overwrite unexecuted work — direct violation of the §1
safety argument.

**Fix direction (spec and/or impl):**

- Last unsubscriber must plant Sub0 at the **globally earliest incomplete**
  segment, not merely its own earliest held ref — requires a farm-visible
  incomplete frontier, or
- Non-last unsub must not drop trails on unconfirmed segments (transfer /
  donate to remaining consumers), or
- Subscribe walk must scan for incomplete `seqt+sd < seqtNext` even when Rt is 0
  (weaker; still leaves unprotected overwrite window).

**Regression:** T18 must go green (exact Done, orphans==0 every trial).

---

## High / Design Weaknesses

### H0. `write()` does not enforce `registerProducer` (Exmax bypass)

**Status:** CONFIRMED (low-level probe)  
**Where:** register at 303–317 vs `write` at 363+  
**Spec 3b:** actual simultaneous producers must not exceed tier maxima

Unregistered threads can call `write` successfully. N > maxBulk+maxSmall writers
break the Exmax safety argument and can stomp consumer-protected data.

**Fix:** Require a producer ticket inside `write`, or only expose write via a
handle obtained from `registerProducer`.

### H1. Multi-small-producer near-completion stalls (related to C1)

Attempts to run 6–8 simultaneous small producers with tiny tables repeatedly
deadlocked or timed out a few payloads short of exact completion (e.g. 2736/2750,
3584/3600). Not isolated as cleanly as T16, but the same family: held refs +
quota renewal + leftover claims when producers stop making progress.

Treat as **blocker for “many small producers” workloads** until C1 is fixed and
a multi-small green test exists.

### H2. Position reference never participates in confirmation release

Spec 5j talks about trailing refs; the implementation’s position ref is sticky
until the next validated table. That is a silent hole in the “every entered
segment is released when confirmed complete” story when the consumer is caught
up.

### H3. `destroy()` is completely unchecked

`AntFarm.destroy` munmaps and frees with no assertion that `Cf==0`, producers
are unregistered, or no `ConsumerView` still holds refs. Concurrent destroy is
instant UB. Document as a hard precondition or assert in debug builds.

### H4. `refreshQuota` only counts full free segments from `ea+1`

Correct per spec 3a, but combined with C1 it means a single pin kills the entire
renewal sweep. No partial-segment free accounting, no “current write segment
remainder” path when Exmax > segCap (spec already notes opportunistic renewal is
inert then).

### H5. Root tally RMW memory orders

Leaf/root `fetch_add`/`fetch_sub` use default `core.atomic` orders (typically
seq_cst on LDC). Spec 2a asks for release on root mods and acquire on bare
reads. Functionally strong enough (TSAN clean on baseline), but not the minimal
model the spec describes; worth aligning intentionally.

### H6. Segment init races under dual bulk (latent)

On segment cross, producers store `seqt/cs/sqcs/sd` with `raw`, then `es` with
`rel`. Two producers crossing different new segments is fine; two producers both
initializing the **same** new epoch is prevented by Wt’s linear reservation.
However `seqt` is set to **this write’s** `wtprime`, which is correct only as
“first table of the new segment starts at the first byte of the first write that
entered the segment.” Contiguous tables guarantee that. OK if that invariant
holds; fragile if a future change allows holes.

### H7. `PayloadHeader` is 136 bytes / 17 ulongs

Matches spec2’s revision note. Static assert present. Good.

### H8. Windows / non-Posix

Hard `static assert`. Fine for this port; document.

---

### H9. `subscribe` frontier fallback can `% 0` (SIGFPE)

Never-written segments keep `sqcs = 0`. No-pulse race standing at an untouched
frontier does `idc % sq` → SIGFPE (confirmed by low-level probe). Fail closed
when `es < 0 || sq == 0`.

### H10. `Reqs → long` false failure leaks `Cf`

When `Reqs ≥ 1<<63`, `cast(long)` looks negative; subscribe bails after `Cf++`
without always unwinding (confirmed probe). Keep IDc as ulong / unwind Cf on
every failure path.

### H11. Complete-table path clears carried `sweeperNext`

`sweeperNext = sweeper` runs even when only the complete-table branch ran
(`sweeper` stays false), dropping gen5 carry across already-done tables. Idle
re-walk still drains; hot path regresses under small-table + complete skips.

### H12. `ConsumerView` is copyable → double-unsubscribe

POD copy + dual `unsubscribe` double-decs tallies / Cf. `@disable this(this)`.

### H13. `destroy()` treats `shmFd == 0` as live

`AntFarm.init.shmFd == 0`; `destroy` would `close(0)`. Initialize `shmFd = -1`.

---

### H14. `Tcount` claim half is 32-bit packed (same class as Pcount)

**Status:** CONFIRMED class / impractical today  
**Where:** `processShard` claim RMW `antfarm.d` ~968  
Exhausted visitors still `fetch_add(1<<32)`. After ~2^32 post-exhaust claims the
high half wraps below `shiter` and can re-admit primary work / corrupt completion
accounting. Same design smell as C2; needs saturating CAS or a separate counter.

---

## Spec Conformance Notes

| Area | Match? | Notes |
|------|--------|-------|
| Magic buffer + Wt reservation | Yes | shm_open + double MAP_FIXED |
| Exmax ≤ (K-1)·segCap | Yes | create() checks |
| Sub0 bit OR/AND | Yes | rtSetBits/rtClearBits |
| Attach-in-place subscribe | Yes | matches spec2, not old walk-forward |
| Trailing refs + Sd | Yes | gen6 Seqt[Ki+1] target |
| Sweeper + tertiary MT | Yes | |
| Carried sweeper small tables | Yes | |
| Done>1 ⇒ MaxCs≥2 | Yes | write() fatal |
| No CAS hot path | Mostly | Sub0 cold path uses fetch_or/and |
| Idle re-walk of oldest trailing | Yes | does not fix position-pin (C1) |

---

## What Is Strong

- Publication protocol (sentinel release last) is textbook and pairs with acquire
  loads on consume.
- Lossless backlog via Sub0 at earliest unconfirmed segment is well thought out;
  testBacklog / T05 cover the handoff.
- Sharded Tcount + bounded Tprogress mutations scale better than a single counter.
- Leaf caching per held reference avoids SqCs epoch skew corruption.
- Fatal-on-invariant is the right posture for this class of structure.
- Existing tests already cover Sub0 lifecycle, wraparound stall-before-lap,
  churn, spanned tables, small-table churn.

---

## Suggestions (Nice to Have)

1. **API surface:** expose read-only `Wt`, `Eg`, `Cf`, and a debug dump of
   Rt/Es/Seqt/Sd — torture diagnostics had to reach into fields.
2. **Quota API:** allow producers to observe “stalled on pin” vs “true full” to
   avoid blind yield loops.
3. **Callback contract:** document that callbacks must be `@nogc nothrow` and
   must not call back into the same farm (re-entrancy untested).
4. **MaxCs upper bound:** even after packing fix, cap MaxCs to something sane
   (e.g. MAX_CONSUMERS_LIMIT).
5. **Fuzz:** structured fuzzer over (K, Ln, quotas, N producers/consumers,
   payload size mix, Done/MaxCs) with exact call accounting and a global
   deadlock watchdog.

---

## Torture Suite Map

| ID | Role |
|----|------|
| T01 | Packed Pcount carry (unit) — **defect** |
| T02 | Done=0xFFFF over-execution — **defect** |
| T03 | Concurrent bulk+small exact calls |
| T04 | Many consumers, two producers |
| T05 | Zero-consumer gap / Sub0 handoff |
| T06 | Subscription storm |
| T07 | Small-table churn |
| T08 | Segment-spanning tables |
| T09 | Slow MT callbacks, per-payload MaxCs |
| T10 | Registration/subscription caps |
| T11 | Late subscriber multi-lap |
| T12 | No re-execution on resubscribe |
| T13 | create() geometry |
| T14 | K=4 and K=16 |
| T15 | Wave consumers + mixed tiers |
| T16 | Position-ref stall / large Exmax — **defect** |
| T17 | write() size arithmetic wrap / smash — **defect** |
| T18 | Pure churn orphan incomplete segments — **defect** |

---

## Recommended Fix Order

1. **C4 / T18** — lossless under pure churn (correctness of the safety argument).
2. **C3 / T17** — overflow-checked size math (memory safety, trivial).
3. **C1 / T16** — position ref release or migrate on idle.
4. **C2 / T01–T02** — unpack or CAS-saturate Pcount (and apply the same discipline to Tcount claims).
5. **H0** — enforce producer registration in `write`.
6. Add multi-small-producer green test once C1 is dead.
7. Harden `destroy()` preconditions (`shmFd = -1`, Cf==0 assert).
8. `subscribe` fail-closed on `sqcs==0` / never-written frontier; keep `IDc` unsigned.
9. Only then chase performance (order relaxation, chunk sizes).

---

## Questions for the author — answered

1. **Q:** Is a consumer that has caught Wt *required* to keep a live segment
   pin, or is "no pin while idle at frontier with confirmed backlog empty"
   acceptable under the safety paragraph in §1?
   **A:** Always at least one pin. Consumers holding the last segment is the
   mechanism. The idle consumer keeps its position reference on the frontier
   segment; the T16 fix migrates/releases the position reference once its
   segment is confirmed complete so the pin tracks the frontier instead of
   parking on a stale completed segment.
2. **Q:** Is `Done=0xFFFF` a real intended operating point, or was the field
   width only a packing convenience with a much smaller practical Done?
   **A:** Much lower. `Done` and `MaxCs` cap at 512; `write()` fatals on
   violation. Overflow beyond field capacity across `Tcount`/`Pcount` is a
   fatal assert, not a wrap.
3. **Q:** Should producers be allowed to share a consumer role on stall (spec
   4a hint) as the supported escape hatch for C1, or is that considered
   out-of-band?
   **A:** Yes — producers can absolutely share as consumers. Supported, not
   out-of-band.
4. **Q (new):** Producer registration / Exmax enforcement design?
   **A:** Sentinel ticket slots. `registerProducer` allocates a slot and
   returns its sentinel; `write()` takes a ticket parameter that must match a
   registered producer slot, so unregistered writers cannot bypass Exmax (H0).

Full policy notes: `README.md` → Design decisions.

---

*Review produced against tree at gen6 (`76de85c`). Artifacts in `review_torture/`.*

