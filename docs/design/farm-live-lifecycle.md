# Farm Live lifecycle: design record

The normative rules are in SPEC 3 (I3), 6b, 6c and 6e. This page records
why the per-epoch `Live` bit replaced the counted `Sub0` pulse in 2.1.1
development, which parts of the proposal that introduced it were dropped,
how the subscribe scan was chosen, and what the audit of the implementation
found. Dates are 2026-10-26 to 2026-10-28. The deterministic schedules named
here are in `tests/farm/subscription_live.d` unless stated otherwise.

## 1. What `Sub0` did, and the schedules that broke it

`Sub0` was a counted unit in bits 31..16 of `Rt`. Three actors deposited it:
the last releaser of an incomplete epoch (an atomic last-root CAS), a
crossing producer's `plantIfUnprotected` on the epoch it had just left, and
the last departing consumer on its position. An epoch's first reference
removed it. One field therefore carried protection of abandoned work,
coverage of a producer's window before its plant, discovery by the next
subscriber, and the empty-Farm anchor, and every one of those depended on
consumer positions or on being last.

Write-audit-hook schedules reproduced four consequences on `c2431a5` (and
on `f593920`, the scan branch of the time). They were not found by argument;
each earlier proposal's own argument had missed them.

| Schedule | Interleaving | Consequence on `Sub0` |
| --- | --- | --- |
| A, C | L, the only consumer, is parked on confirmed epoch 0; P's crossing write from 1 into 2 is paused before its plant; S subscribes and L leaves as soon as S is counted | L's non-last departure dropped the pin that covered P's unplanted window; Q then entered the next lap of slot 1 over P's unpublished table (I3 broken) |
| D1 | Lg, the only consumer, idles on confirmed 0 while P's crossing into 3 is paused; Lg moves 0 to 1 during S's backward walk | the walk missed the mover; a producer lapped work no member would reach |
| D2 | as D1, but Lg leaves inside S's walk | Lg saw S already counted, took no last-of-Farm action, and left the ring unpinned |
| count-after-walk variants | D2 on patched prototypes that counted the subscriber after its walk | every departing member planted a late pulse behind the others, stalling producers |

## 2. The proposal, and what survived review

The proposal (`proposal-fmembers-xfer-live`, 2026-10-28) replaced `Sub0`
with four things: `Fmembers`, a packed membership word; `Xfer`, a handoff
object the last departing member published and the next first subscriber
consumed, with its `Xsegment`, an `Xgen` generation to veto stale handoffs,
a handoff mutex, irreversible early promotion, a first-subscriber wait and a
watchdog; a higher admission ceiling; and `Live`, a bit raised when a
producer initializes an epoch and cleared by a holder that observed the
epoch confirmed.

The review (sections 13 and 14 of that proposal) found that `Live` alone
carries the invariant the others were built to supply: confirming epoch `e`
needs `Es[e+1]`, which is published only after `Live(e+1)` is raised, so the
earliest unconfirmed epoch is always initialized and `Live`, on an empty Farm
too. Epoch 0 is the base case. With that invariant the last departure has
nothing to protect and the next subscriber has nothing to be handed; `Xfer`
and everything attached to it were dropped. The bounded handoff model's
absence of `Xgen` mismatches had a structural explanation under the
proposal's own rules (one unresolved publication at a time), so the `Xgen`
veto had been answering the wrong question. Packed membership and the
higher ceiling became separate policy choices and were not adopted; `Cf`
stays a plain capacity and sizing count (SPEC 2a).

Two scratch models from that work, `anchor-protocol-model.py` and
`xfer-handoff-model.py`, were not carried into the repository: they model
the dropped handoff, not the adopted protocol.

## 3. Choosing the subscribe scan

A `Live`-only prototype of about 70 lines passed A to E, the ordering
regressions, and 17 torture tests, and then failed three further schedules
found while designing the scan. Each is now a regression:

| Schedule | What it showed on the prototype | Rule it produced (SPEC 6b) |
| --- | --- | --- |
| P | S samples `Wt` in epoch 0 and is paused before its first `Sub`; producers reach epoch 4 and reuse slot 0; S took `Live` epoch 4 in its one-slot window (3 of 9 callbacks ran) | a candidate counts only as the logical epoch its window position implies: `Es == x` |
| F | the probe goes stale by two epochs under out-of-order initialization; S attached at 5 with 15 of 18 tables run | the same, plus a pinned re-read before validation |
| G | a new lap's `Live` is raised before its `Es` is published; S acquired it beside the old `Es` | the `Live` raise is a release RMW after the reservation, so the validation's `Wt` re-load names the next lap and rejects the candidate |

Scan order. A newest-first walk that stops at the first discharged slot
needs three extra rules: departure may discharge only the confirmed prefix
(H), a re-read upward from the stop point after the walk (E), and `Es`
before `Rt` in each slot check (I). It also adds a global invariant, that no
discharge may leave older `Live` work behind, and one wrong attach strands
that work for every later walk. An oldest-first walk needs none of these:
each read either passes a confirmed epoch or is the first read at or above
the earliest unconfirmed one, which is `Live`. Oldest-first was adopted.

Pins. Unpinning slots during the scan let producers outrun it: one stress
run returned 11,551 consecutive `RETRY`s. Three remedies were measured on the
same harness (the numbers are from those sessions and were not re-taken
here):

| Variant | Rule | Result |
| --- | --- | --- |
| vL4 | pin all K slots first, then re-probe and re-read inside the attempt | 0 `RETRY` in about 13.8 M attaches; a slow subscriber freezes quota renewal while it attaches |
| vL5 | vL4 with pins placed newest-first | 0 `RETRY`; throughput with injected delays back near the baseline |
| vN | pin only the newest slot and the chosen candidate; read the rest with acquire loads | about 2 pins per subscribe in every regime; the best herd attach latency; up to K−1 extra rounds (about 15 at K = 16) |

Only the newest (tail) slot's pin is never stale, and it alone bounds the
tail to less than one lap during the attempt; pinning the oldest slots first
was the worst case for producer throughput under slow scans. vN is what SPEC
6b specifies. Its review recorded the obligations that remain open (section
6): the discovery induction is argued, not machine-checked; `held` pins
accumulate across rounds up to K, so "two pins" is the common case rather
than the maximum (the slow-scan log averaged 2.58 at K = 16); and the
`K + 2` round budget is not proven sufficient, which is why `RETRY` must
stay recoverable.

## 4. The implementation

Commit `8275359` made the change: `antfarm/package.d`, SPEC sections 1 to 6,
8 to 11, the Farm tests, and the new `subscription_live.d` with schedules A
to I and P. The full `scripts/check.sh` matrix passed on it (every TSan lane,
actors, private actors, threadpool, fibers, forkjoin, C ABI, examples).
Performance was not measured, and no 16 to 32-thread subscribe herd was run
on that code.

## 5. Audit of the implementation (this branch)

Three sessions reviewed `8275359` in parallel on sibling branches and were
merged here: this audit (`review/live-lifecycle-audit`), a second review
(`review/farm-live-lifecycle`), and a test-invariants pass
(`test/farm-live-invariants`). The audit re-derived I1 to I5 and 6b to 6e
against the code and added schedules for the interleavings the first set
did not cover. Every schedule is built with write-audit hooks that compile
out of ordinary builds; two hooks were added for it, `subscriberValidating`
and `liveClearing`.

**One hardening.** A fresh subscriber attaching at epoch `c` validates `Es[c]`
and takes `nextSeq = Seqt[c]`. When the write that crossed into `c` spanned
further, `Seqt[c]` lies in a later epoch `s`, and the subscriber's first
advance pins `c+1 .. s` after validating the *next* producer's table (6d).
That producer's reservation does not order the crossing producer's later
`Es[c+1 .. s]` stores, and neither does anything the scan acquired: a
discharge it passed carries only the discharger's acquire of its own
successor's `Es`, a reuse carries the same through the sweep, and the tail
probe carries reservations. Under sequential consistency the gap is
unreachable, because `c`'s predecessors can only be discharged after the
crossing table completed, which is after those stores; under the C++ model
there was no happens-before edge, and `moveRef` would fatal on the mismatch.
Validation now acquire-loads `Es` of `s` when `s > c` and ends the round on a
mismatch (SPEC 6b-c, 9). The `Wt` bound already excludes reuse of those
slots, so a mismatch can only be an initialization still in flight. The
second review found the same gap independently and wrote the same fix; a
follow-up review of that branch checked it with an exhaustive RC11 litmus of
the path, now kept as `tests/farm/rc11_spanned_attach.py` and re-run for
this record: on `8275359` the first `moveRef` reads stale `Es` or `SqCs` in
eight consistent executions, all of them RC11-only, and in none with the
acquire; with the acquire the only non-OK outcomes are rejections, and every
rejection is RC11-only, so the check never rejects a candidate under
sequential consistency. The model's last variant lets a later lap overwrite
`Es` without modelling the quota sweep, so its two SC outcomes there are
executions the subscriber's `Sub` and reference forbid. The second review
also added `farm_ordering.d`'s
`spannedAttach`, which reaches the path deterministically: one consumer
finishes the spanning table on an idle re-walk, another discharges its start
epoch without ever reading the sentinel, and the subscriber attaches at the
interior before consuming the next table. It is path coverage, not a
discriminator; `LN` below is the discriminator.

**Schedules added.** All run under LDC and DMD in `check-farm-ordering`.

| Schedule | Interleaving | Oracle |
| --- | --- | --- |
| J1 | C drains and discharges epoch 1 at S's pinned re-read of its slot | S drops that `Sub` and attaches at 2; slot 1 reads zero |
| J2 | C discharges 1 at S's validation, after the re-read saw `Live` | S attaches at the discharged epoch under its own `Sub`; its reference lands on protected space |
| J3 | C is paused inside its discharge, between the load and the CAS; S subscribes completely, landing on a second leaf | the CAS retries exactly once against the new count and clears `Live` under S's reference |
| Q | the same pause; a `Sub` lands on the slot and stays | the CAS retries against the `Sub` and clears `Live` with it intact |
| K | S2 subscribes completely while S1 has just pinned the same candidate | both attach at 1; every `Sub` is gone; the root count equals the occupied leaves |
| X | the last table of epoch 0 ends exactly at the boundary, initializing epoch 1 with `Seqt == S` and no table | S attaches at 1 with `nextSeq == S`, with C present and after C has left; the next table starts at the boundary |
| L | a bulk table spans 0 to 3; while C is paused discharging interior epoch 1, S attaches there with its first table in epoch 3 | S attaches at 1 in one round; its first advance pins 2 and 3 and releases the prefix |
| LN | L with `Es[3]` hidden during S's first validation (instrumentation negative control) | the round is rejected; the next round attaches at 1 |
| R (second review) | after each tail probe, C drains one whole epoch, so the frontier `Sub`s accumulate until they refuse the next grant; K = 2, 4, 8, 16 | S attaches at the Live frontier within K rounds and drops every accumulated pin; the schedule never needs rounds K+1 or K+2, so it is not evidence about the budget |

The test-invariants pass made the harness stricter around all of these:
D, E, F and G now fail if they miss their requested interleaving (only I
keeps its documented after-attach path), every schedule ends by requiring
a detached Farm (no member, reference, pin or leaf), and its own schedule
J, a discharge between the hint and the pinned re-read, is folded into J1
as the check that the rejected candidate's `Sub` is already gone when the
next slot is read while the tail's `Sub` stays.

**Ablations.** Each targeted defect was reintroduced on a private copy and
the schedules re-run:

| Variant | A to I, P | J1, J2, K | J3, Q | L | LN |
| --- | --- | --- | --- | --- | --- |
| `8275359` | pass | pass | pass | pass | not applicable: no successor acquire, no hooks |
| this branch | pass | pass | pass | pass | pass |
| successor acquire removed | pass | pass | pass | pass | fail: attaches with one probe |
| discharge CAS single-shot | pass | pass | fail: `Live` left set | pass | pass |

The `Sub0`-era and prototype results in sections 1 and 3 come from the
design sessions and were not re-run on this branch.

**Observations without a code change.**

- A round can also end without attaching when the frontier's `Es` is not yet
  visible (its producer is between the `Live` raise and the `Es` store) and
  every older epoch is discharged. The next round re-probes; the budget and
  the recoverable `RETRY` cover it. It is not a safety case.
- Attaching at a confirmed-but-undischarged epoch is legal: the subscriber
  holds a reference, skips the complete tables, and discharges the epoch
  when it releases it. J2 and J3 show the reference landing either side of
  the discharge.
- The position epoch is discharged only by `unsubscribe` or by a later
  holder, never by `tryReleaseOldest`, which stops before `newestEi`. A
  consumer idle on a confirmed position migrates on its next sentinel miss
  (6d), so no confirmed epoch stays `Live` while a holder keeps calling
  `consumeNext`.

## 6. Open obligations and what was not run

- The weak-memory form of the 6b discovery induction and the I3 induction
  are argued in SPEC 6b and 3, not machine-checked.
- The `K + 2` round budget is a budget. `RETRY` stays recoverable and no
  watchdog may treat a run of retries as corruption.
- Performance has not been measured on the shipped code: neither the hot
  path (discharge is one acquire load per confirmed epoch per passer and one
  CAS per epoch) nor subscribe latency with instrumentation outside the
  timed path.
- Two herd tests now exist. T28 (test-invariants pass) runs barrier-gated
  cohorts of 16 and 32 members at K = 2 and K = 16 on a 1 MiB ring: every
  other round starts with producers already refused by a backlog no member
  has seen, the cohort that joined must drain it with no steady helper and
  no rescue subscriber, and each departure must leave an exactly drained
  Farm, over at least three laps. T29 (this audit) lets 24 herd arrivals
  and departures overlap two producers and two steady consumers at the
  same ring size over about eight laps: roughly 40,000 to 55,000 attaches
  per trial under LDC and 20,000 under TSan, no `RETRY`, every payload
  exactly once, a drained Farm afterwards. Neither is evidence about the
  round budget: a round fails only when the tail outruns an attempt, and
  these producers cannot move a window's width in the time one attempt
  takes.
- The schedules run under LDC, DMD and TSan (`run-farm-tsan` builds them;
  `run-subscription-live-tsan` runs them alone); the condensed torture
  suite, T19, the sanity entry point and the unit suite passed under LDC,
  DMD and TSan on the merged branch. The rest of `scripts/check.sh`
  (actors, private actors, threadpool, fibers, forkjoin, C ABI, examples)
  was not re-run here; `antfarm/package.d` changed only on the subscribe
  path and in hook sites that compile out.
