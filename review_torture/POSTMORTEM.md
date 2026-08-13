# Ant Farm postmortem — gen6 review → next revision

Reviewed tree: gen6 (`76de85c`), `antfarm.d` against living `spec2`.
Torture suite: `review_torture/` (T01–T18). Findings: `CODE_REVIEW.md`.
Author policy (2026-08-11): `README.md` → Design decisions, with item 5
corrected (payload length, not table length).

This note records the defects, the directions settled after review, and the
alternatives rejected. It is the brief for the next farm revision. Nothing
here is implemented yet.

Baseline `antfarm_test.d` passed on LDC/DMD, release, and LDC TSan. That
geometry always keeps ≥1 steady consumer, trusted body lengths, and modest
Exmax. Torture confirmed four real defect classes plus H0 (unregistered
`write`). Verdict from the review stands: do not ship as a general-purpose
queue until C4, C3, C1, and C2 are closed.

---

## Policy already decided (author)

1. **Always at least one pin.** An idle consumer that has caught `Wt` keeps
   its position reference on the frontier segment and migrates it forward
   as the frontier advances. Never drop to zero pins; never park the pin on
   a stale completed segment.
2. **`Done` and `MaxCs` cap at 512.** `write()` fatals on violation.
   Packed-field overflow on `Pcount` / `Tcount` is a fatal assert, not a
   wrap.
3. **Producers may consume.** A stalled producer may subscribe a
   `ConsumerView` and drain before retrying `write`. Supported escape
   hatch, not out-of-band.
4. **Producer tickets.** `registerProducer` increments a dedicated
   `Reqs_p` (not the consumer `Reqs_c`), allocates a slot in a pre-sized
   bulk/small array, and returns `Token { slot, hash }` of `(slot, Reqs_p)`.
   Slots start invalid; `unregisterProducer(Token)` writes invalid.
   `write()` takes a required `Token` and fatals if the hash does not match
   the slot. Unregistered writers cannot bypass Exmax (H0). Consumer `IDc`
   stays a dense sequence from `Reqs_c` so producer registration cannot
   fragment shard assignment.
5. **Pathological payload length is a caller error.** A payload that cannot
   fit in any producer’s maximum `Exi` (a singleton table containing only
   that payload, including real table overhead, vs `max(quotaBulk,
   quotaSmall)`) is `fatal`, not `write() == 0`. Multi-payload truncation
   to the caller’s current quota stays legal (spec 4a).

---

## C4 / T18 — incomplete segments must never show `Rt == 0`

**Status:** CONFIRMED. Highest priority. Falsifies the §1 safety argument.

**Mechanism.** Churners subscribe, consume a short burst, unsubscribe.
There is no steady consumer. Non-last unsubscribers drop every held ref,
including trails on still-incomplete early segments. The eventual last
unsubscriber may only hold a later segment, so Sub0 is planted there.
Early segments sit at `sd < Seqt[Ki+1] - Seqt[Ki]` with `Rt_low == 0`.
A late subscriber attaches at the pulse and never walks back. Producers
may treat those segments as free on the next lap and overwrite unexecuted
work.

The 5j claim “protected by every passerby still subscribed; last
unsubscriber plants Sub0 at its earliest unconfirmed held ref” assumes
those two places are the same. T18 shows they are not.

**Settled direction: last-releaser, not last-unsubscriber.** Sub0 means
“this incomplete segment has no live consumer,” not only “the farm is
empty.” Currency is `±SUB0` (`fetch_add` / `fetch_sub` of `1<<16`). No
OR/AND on this field.

### Sub0 history (why OR/AND exists, and why it must go)

Early spec (Documents `spec2`, construction `spec` 5b-a): last unsub
**adds** `SUB0`, first sub **subtracts** `SUB0` when the `SUB` deposit
sees `Rt' >= SUB0 && high == 0`. Pairing is exact when Sub0 is only the
farm-empty pulse on one segment.

`Cf` is not a lock. Last-unsub (`Cf` 1→0) and first-sub (`Cf` 0→1)
overlap. If the first-sub’s designation runs *before* the plant, it sees
count-only `Rt'` (never `>= SUB0`), is not the clearer, and the later
add leaves Sub0 under a live consumer. The next farm-empty **adds
again** → `2*SUB0`. First-sub subtracts once; the leftover grows.

Spec2 / the implementation switched deposit to OR and clear to AND
(“Sub0 is a bit, not a count”) so a second plant is idempotent. That
fixes one pulse on one segment. It does not compose with a counted
last-releaser chain: AND is not a matching debit for an add, and a
first-sub AND-clear racing a releaser’s retract underflows into the
count bits. Construction `spec` never finished the conversion (5a-c/f
say OR/AND, 5b-a still says “adds”).

Return to the Documents-generation inc/sub, extended to every last
release of an unconfirmed segment. The retract-if-not-last rule is what
makes add/sub safe without OR/AND.

### Unified protocol

At rest each segment has 0 or 1 × `SUB0`. Transiently up to one unit per
concurrent last-leaf (still well below `SUB` at 128).

**Release that must leave a pulse iff the count actually hit zero** —
unconfirmed always; confirmed only when `Cf` just went 1→0 (empty-farm
pulse). Only when the leaf dec will propagate to Rt (`lo == 1`):

```
fetch_add(Rt, SUB0);
old = fetch_sub(Rt, 1);
if ((old & COUNTMASK) == 0) fatal("root tally underflow");
if ((old & COUNTMASK) > 1)  fetch_sub(Rt, SUB0);
```

Confirmed, farm not empty: plain `decRef`, no Sub0.

**Acquire (subscribe / `moveRef` leaf 0→1).** This *is* original 5a-f,
keyed off the Rt edge instead of the `SUB` snapshot:

```
old = fetch_add(Rt, 1);
if ((old & COUNTMASK) == 0 && (old & SUB0) != 0)
    fetch_sub(Rt, SUB0);
```

Only one thread observes count `0→1`. That is the unique subtractor for
that segment. Drop `high == 0 && Rt' >= SUB0` and drop `rtSetBits` /
`rtClearBits` on Sub0. Construction’s dummy `SUB0` clears on the same
edge.

Subscribe still deposits `SUB` (high half) for the establish-leaf
window. That is a different field; it does not clear Sub0.

**Do not extra-plant** `fetch_add(SUB0)` and then run the release helper
on the same segment — that is two plants. Last-unsub’s empty-farm plant
is only the confirmed-position case, and it is this same helper, not a
bare add. A concurrent first-sub that already incremented the count
makes `(old & COUNTMASK) > 1` and the plant retracts.

Always mask with `COUNTMASK`. Raw `old > 1` is wrong: the `SUB0` just
added, or a concurrent `SUB`, makes a true last-count drop look like
“not last.”

### Rejected for C4

- Plant Sub0 only at `Cf == 1` on the last unsub’s own earliest held
  ref — that is the current hole.
- Subscribe-time scan for `seqt+sd < seqtNext` even when `Rt == 0` —
  leaves an overwrite window.
- Donate trails to a remaining consumer — needs a consumer registry the
  farm does not have.
- Farm-level `MinIncomplete` epoch — `K` is tiny; producers already
  sweep per-segment `Rt`.
- Keep OR/AND for last-unsub / first-sub beside add/sub for releasers —
  accounting poison.
- Keep first-sub subtract *and* the `0→1` retract — double-sub.
- Touch Sub0 on confirmed release when the farm is not empty — leftover
  pins after drain (unless `0→1` retracts, in which case last-of-farm
  still needs the helper to leave a pulse).

**Regression.** T18 green: `orphans == 0`, exact `Done`, every trial,
`nsteady == 0`. Existing `testBacklog` / T05 / T07 must still pass.
Older tests that assert `pulses == 1` after churn are wrong under the
new rule: every abandoned unconfirmed segment may sit at `0|SUB0`. That
is the fix, not a regression. Farm-empty with everything confirmed still
has exactly one pulse. T18 / late-subscribe should check orphans and
call counts, not pulse cardinality while churn is in flight.

---

## C3 / T17 — overflow-checked size math + unsizable payloads

**Status:** CONFIRMED (child dies SIGILL).

**Mechanism.** `psz = PHEAD_LEN + pe.body.length` and `cand = … + psum +
psz + END_PAD` wrap. A wrapped `cand` looks like it fits a small `exi`,
then the body copy of `size_t.max - 16` smashes.

**Settled direction.** Two checks, not one.

1. **Arithmetic.** Reject before any compare to quota:
   `body.length > size_t.max - PHEAD_LEN`, `psum + psz` overflow, `cand`
   overflow, final `size` overflow, Tindex `po +=` overflow. Wrapped
   `cand` must never be compared to `exi`.
2. **Unsizeable payload (policy 5, corrected).** If a singleton table
   containing only this payload cannot fit the farm’s largest `Exi`
   (include `THEAD_LEN`, index slots, pads, `8 * sqcs`, `END_PAD`),
   `fatal`. That is a machine pushing a payload no producer can ever
   write. It is not “abort if this commit’s table exceeds bulk quota.”

**Regression.** T17 child must not die on a signal. `fatal` (abort,
distinctive message) is the intended reject. After the fix, a clean
`_exit(0)` is a harness miss. Point T17 at both wrap (`body.length =
size_t.max - 16`) and “fits in `size_t` but not in max `Exi`.”

Ride the same `write()` change set as tickets (H0) and the 512 caps (C2).

---

## C1 / T16 — keep the pin; migrate it to the frontier

**Status:** CONFIRMED. Producer stall under `Exmax ≫ segCap` with idle
consumers at `Wt`.

**Mechanism.** `moveRef` runs only after a successful sentinel validate.
On the idle path the position ref never migrates, so consumers sit on a
confirmed-complete start segment of a spanning table while `nextSeq`
points at a later initialized epoch. `refreshQuota` counts full free
segments from `ea+1`; one stale pin in the sweep window kills renewal.

T16 observation: `holdKi=4` complete, `next=29562` in epoch 7, `ki=5..7`
already initialized with `seqt=29562`.

**Settled direction.** On sentinel miss (and after a consume that set
`nextSeq` into a new epoch): if `confirmed(curKi)` and `Es[ki] == ei`
for `nextSeq`’s `(ki, ei)`, `moveRef` then `tryReleaseTrailing`.
Increment-ahead; the old position becomes a trail and releases if
confirmed. Always ≥1 pin; the live pin sits on the frontier.

`nextSeq` is always `Tnext` of a published table, which is that write’s
`wtprime`. Crossing writes already release-store `Es` through that
epoch (spec 4b). There is no uninitialized target on the idle path.
The case you cannot migrate into (`Ki+1` with `Es < 0`) only exists
when no write has crossed, which means `nextSeq` is still inside `Ki`
— the pin is on the write segment, which `refreshQuota` does not sweep.

Producers-as-consumers (policy 3) remains the stall escape hatch when
refresh fails for real backpressure, not the T16 fix.

**Rejected for C1.** Restricting Exmax to half the buffer.

- “Exmax is never observed” is opportunistic renewal (`Seqb - Wt' >=
  Exmax`), which is identically false when `Exmax > segCap`. Half the
  buffer does not revive it. The bound that does is `Exmax ≤ segCap`,
  a separate product decision.
- Half is also the wrong sweep bound for “tolerate a stale pin on a
  spanning table’s start.” Worst-case one reservation of size Exmax
  from the pin needs `Exmax ≤ floor((K-1)/2) · segCap` (3 segments at
  `K=8`, 1 at `K=4`). Half still loses the exact-boundary case.
- That cut only *tolerates* a pin left on a completed start. It does
  not restore “the idle pin tracks the frontier,” and it does nothing
  for C4.

**Regression.** T16: producer finishes 120/120. Add a multi-small-producer
green test once T16 is dead (H1). After this, a confirmed position
participates in release by becoming a trail (H2).

---

## C2 / T01–T02 — cap at 512; fatal instead of wrap

**Status:** CONFIRMED. `fetch_add(1<<16)` at `calls == 0xFFFF` carries
into claims; `fetch_add(1)` at `comps == 0xFFFF` carries into calls.
T02 (`Done=0xFFFF`, 16 consumers) over-executes by ~16×.

**Settled direction.** Policy 2, keep the no-CAS hot path.

- `write()`: `done ∈ 1..512`, `maxCs ∈ 1..512`, existing `done > 1 ⇒
  maxCs ≥ 2`.
- `enterPayload` / `processShard`: after each packed `fetch_add`, if
  the field just incremented is `0` (wrapped), `fatal`. Best-effort —
  the wrap has already landed — but under the cap 16-bit call/comp
  carry is unreachable through `write()` (`Done + MaxCs ≪ 65535`).
- Same post-increment fatal on **Tcount** claims (H14). 2^32 exhausted
  visitors on one shard is the pathological-accumulation case.

**Rejected for C2.** CAS-saturating loops in `enterPayload` (violates
no-CAS hot path). Unpacking `Pcount` into filler words is a valid later
alternative (still `fetch_add`, no carry) but is not required if the
512 cap is real policy.

**Tests.** Rewrite T01 to assert `write()` fatals on `Done`/`MaxCs > 512`,
and optionally that a packed increment at field max aborts. Rewrite T02
to `Done = 512` and demand exact call counts. Do not leave T01 asserting
“no carry at `0xFFFF`” against production `fetch_add`.

---

## H0 — ticketed `write()`

Unregistered threads can call `write` and break the Exmax argument.
Policy 4. Same `write()` pass as C3/C2 so the API break happens once.

---

## Adjacent hardenings (same functions, after the criticals)

| Id | Direction |
|----|-----------|
| H1 | Multi-small-producer green test once C1 is dead. |
| H2 | Closed by C1 migrate (confirmed position becomes a trail). |
| H3 / H13 | `shmFd = -1` at init; `destroy` asserts `Cf == 0` and no live tickets. |
| H9 | `subscribe` fail-closed on `es < 0 \|\| sqcs == 0`. |
| H10 | Keep `IDc` as ulong; unwind `Cf` on every subscribe failure path. |
| H12 | `@disable this(this)` on `ConsumerView`. |
| H5 / H11 | Leave until criticals are green (order relaxation, carried-sweeper skip). |

---

## Spec deltas

| Area | Change |
|------|--------|
| 5b / 5j | Last **releaser** of an unconfirmed segment plants Sub0 via add / maybe-sub. First consumer of a segment (`0→1`) subtracts it. No AND/OR. Last farm unsub still guarantees a pulse when everything held is confirmed, using the same helper. |
| 5c / idle | Position ref migrates to the initialized segment of `nextSeq` once the old position is confirmed. Always ≥1 pin. |
| 4a | `Done`, `MaxCs` ≤ 512; packed-field overflow is fatal. |
| 4a | Payload that cannot fit a singleton table in max `Exi` is fatal. Overflow-checked size math. |
| 3b / 4a | `write` requires a registration `Token` hashed from `Reqs_p`. Consumer IDs come only from `Reqs_c`. |
| 4a | Producers may subscribe a `ConsumerView` and drain on stall (normative). |

---

## Implementation order

1. **C4** — Sub0 add/sub last-releaser + `0→1` retract; T18 green.
2. **C3 + payload policy + Token + 512 caps** — one `write()` change set;
   T17 clean fatal; H0 closed; retarget T01/T02.
3. **C1** — idle/frontier `moveRef`; T16 green; then multi-small test.
4. Cheap hardenings in the same functions (H9, H10, H3/H13, H12).
5. Performance (H5, H11) last.

---

## Torture map after the fix

| ID | After |
|----|--------|
| T01 | Retarget: `write()` rejects `Done`/`MaxCs > 512`; optional wrap-abort probe. |
| T02 | Retarget: `Done = 512`, exact calls. |
| T03–T15 | Stay green. |
| T16 | Defect → producer completes. |
| T17 | Defect → `fatal`, no signal. |
| T18 | Defect → `orphans == 0`, exact `Done`, every trial. Allow `pulses >= 1` during churn. |
