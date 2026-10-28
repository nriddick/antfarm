# Proposal, integrated: validate under pins, then commit membership; probes are bounded by admission slots and never plant

Status: revision 3, 26 October 2026. Integrates the
[validate-before-Cf proposal](proposal-subscribe-validate-before-cf.md) and
its tested [prototype patch](subscribe-validate-before-cf.patch) (base
`fix/scan-2.1.1`, `f593920`) with the findings of revisions 1 and 2 of this
document. The patch is the implementation of record; this document records
why, what it must also say, and what is still open. Nothing is applied to a
checkout.

**The A1 residual, in brief.** As recorded in the review that followed
`47b980c`: "a subscriber that counts itself in `Cf` and then retries can
still briefly leave the Farm without a pulse. The window is narrower than
before and `subscribe`'s retry loop shrinks it, but I didn't close it." Two
sharpenings. The window opens on every attempt, not only on retries: it is
the interval between the subscriber's `Cf` increment and its first pin, and
a leaving last consumer decides "not last" inside it even when the attempt
goes on to succeed; the wrap that the subscriber's bound check then turns
into `RETRY` is the symptom, and each retry reopens the interval. And
brevity is not protection: I3's cover exists against a producer's own brief
window between reservation and plant, so two brief windows racing is the S3
failure mode, which T27 reached in 16% of trials. A direct `subscribeOnce`
caller that stops on `RETRY` leaves the unwind's empty, pulse-less state for
as long as it likes.

**History of this document.** Revision 1 moved the frontier pin ahead of the
count and made a failed attempt plant the empty-Farm pulse on the slot it
had pinned. Two reproduced failures killed it: a pin blocks only the sweeps
that come after it and revokes no grant already issued, so the plant landed
on retained quota (fatal "fresh segment has Sub0"); and `add_consumer`'s
increment-then-roll-back at capacity let 128 leavers all read "not last".
Revision 2 moved the count after validation so that probes never plant, but
left probes unbounded, which loses the `K × 128` bound on `Sub` units the
spec states in 6b-d. The validate-before-Cf proposal has the same ownership
boundary as revision 2 and restores the bound with a second counter. This
revision adopts it.

## 1. The design, as the patch implements it

Two counters with distinct responsibilities:

- `consumerSlots`, the capacity bound. A probe claims one slot with an
  acquire-release CAS before its first pin, only while the observed count
  is below `MAX_CONSUMERS_LIMIT`; observing the limit returns
  `SUBSCRIBE_FULL` without touching any counter or root. The slot is held
  through the walk, through membership, and through departure: a failed
  probe releases it after dropping its last `Sub`; an unsubscriber releases
  it after its final root release, inside the mutex.
- `Cf`, committed membership. A subscriber increments it (release) only
  after every 6b validation has passed under held pins, and attaches
  immediately after, with the candidate's `Sub` still held. No `RETRY` or
  `FULL` path follows the increment. Only serialized unsubscription
  decrements it, with acquire-release.

Invariant: `0 ≤ Cf ≤ consumerSlots ≤ 128`; each root carries at most 128
`Sub` units.

The subscription order is: reserve, pin from the frontier backwards, walk,
validate (`Es`, `SqCs`, the sequence-epoch range, the wrap bound, the
fallback's current-frontier equality), commit `Cf`, attach, drop the `Sub`s.
`RETRY` drops the `Sub`s and the slot; `FULL` has deposited nothing.

## 2. The invariant to add to SPEC 6c

> **Membership invariant.** `Cf` counts attached consumers and validated
> subscribers one `incLeaf` from attaching under held pins, never probes. A
> failed or rejected attempt was never counted, so it is never last of the
> Farm and never plants. The empty-Farm pulse is planted only by the
> last-of-Farm unsubscriber (6e), on a position that was pinned continuously
> since it was the frontier; probes never plant, because a `Sub` blocks the
> sweeps that come after it and is no claim against a grant issued before
> it. The pulse invariant holds at quiescence: during an unsubscription `Cf`
> is already zero while the departing view still holds the reference it will
> replace with the pulse. An empty, fully confirmed Farm then has exactly
> one pulse; abandoned incomplete epochs may carry more, as 6e requires.

## 3. What the patch's SPEC text should also say

The patch rewrites 2a (the two counters), 4c (admission), 6b-a through 6b-e
and the failure paragraph, adds a "Membership handoff" paragraph to 6b, the
acquire-release `Cf` decrement and slot release to 6e, three rows to section
9, and the capacity and destroy rows of section 10. Three additions from
this document's analysis belong beside them:

1. **Why one frontier `Sub` covers I3 for the walk's duration** (6b-b or
   I3). A sweep anchored at the frontier examines the K−1 following slots
   and reaches the crossed-out-of segment last. The probe's d = 0 pin is in
   that range, or is the crossed-out segment itself, whenever the frontier
   has moved since the pin, so no sweep taken after the pin can certify the
   pre-plant window. Grants issued before the pin spend only slots that were
   free at their sweep, when the consumer that later leaves was still
   present and covering I3. `checkFreshWriteSegments` already ignores the
   high half for exactly this case.
2. **No pulse is left behind an attach point** (6b "Membership handoff").
   If the last consumer leaves after a probe's validation and before its
   commit, it plants on its position. That position held a count during the
   probe's walk, so it was a candidate: the probe attaches there and its
   0→1 clears the pulse, or attaches at an earlier candidate and clears the
   pulse when its advance enters that segment. A pulse planted this way is
   always at or ahead of the attach point, never behind it, so it cannot
   become a confirmed non-empty-Farm pulse.
3. **The capacity bound's new meaning** (4c). `SUBSCRIBE_FULL` can now
   occur with `Cf` below the limit, because probes mid-walk and views
   mid-departure hold slots. It is transient in both cases; a caller that
   treats `FULL` as permanent should know that.

## 4. Safety, in the order a reviewer will ask

1. **W1 closed.** A leaver's "not last" is decided against committed
   members only, each holding a root count or one `incLeaf` from one under
   held pins; a plain release under `Cf > 1` leaves a root nonzero.
2. **W2 gone.** A failed attempt was never counted; the pulse is the
   leavers' responsibility alone, discharged by 6e on a continuously pinned
   position. No plant can land on certified space because probes never
   plant.
3. **The retained-grant schedule.** The probe pins after the grant, fails
   validation, drops its `Sub`s and its slot. The departing consumer is last
   by a count that excludes the probe and plants on its own position.
   Spending the grant sees only a high half on the certified slot.
4. **The capacity schedule.** A rejected attempt claims nothing, so the 128
   leavers decide against a count that was never touched; the last plants.
5. **Leaver last while a probe is validated or committed.** Point 2 of
   section 3, both branches, plus: a leaver whose decrement follows the
   commit is not last, and the probe's held `Sub` plus imminent count keep
   the ring blocked until the attach.
6. **The `Sub` bound.** Each probe holds one slot before its first pin and
   at most one `Sub` per physical slot, so `K × 128` units per root still
   holds; without the admission counter it would not.
7. **Memory order.** Candidate `Sub` RMW (release) → `Cf` increment
   (release) → leaver's `Cf` decrement (acquire-release) → leaver's root
   release → producer's acquire of that root. Admission slot reuse: final
   `Sub` or root drop → slot release (release) → next claimant's CAS
   (acquire-release). `Cf` was relaxed; it must not be.
8. **Producer snapshots.** `Cs` and the first-claimant yield heuristic read
   `Cf`, which now excludes probes. These size leaf distribution and a
   heuristic; 2b states that an inaccurate `Cs` degrades balance, not
   safety.

## 5. Tests

The patch's `tests/farm/subscription_admission.d` runs seven deterministic
schedules through the write-audit hooks (new phases `subscriberFull`,
`subscriberValidated`, `subscriberCommitted`):

| Schedule | Covers |
| --- | --- |
| `retainedGrant` (one probe, and two nested) | revision 1's failure 1: certification before the first pin, `RETRY`, the certified slot stays zero, the grant remains usable |
| `fullWithDepartures` | revision 1's failure 2: rejection paused while all 128 leave; the last leaver plants |
| `handoff` at `subscriberPinned`, `subscriberValidated`, `subscriberCommitted` | the last consumer leaving before the first pin, after validation, and after commit; the surviving protection refuses a producer write that would sweep through it; subscription completes |
| `pendingCapacity` | 128 uncommitted probes holding `Sub`s; the 129th is `FULL`; the admitted probes attach and leave |

Reported passing on the scratch implementation: `check-farm-ordering` with
LDC 1.43.0 and DMD 2.113.0, the Farm `baseline` suite, T18, T20, T23 and
T27 (2,000 trials, `lost=0 extra=0`) in ordinary and TSan builds, and the
seven schedules under TSan. The hooks are synchronous, so TSan there checks
the instrumented execution of those schedules, not an independent
weak-memory argument.

Two additions this document asks for:

- A racing form of `handoff` in T27's style, 2,000 fresh Farms, one idle
  last consumer leaving while a subscriber runs unscripted, checking `Cf`,
  the slot count, exactly one pulse, and that no `quotaSwept` certified a
  slot the subscriber then attached to. The deterministic schedules prove
  the logic; the racing form is what found S3.
- The full `make check` (both compilers, TSan and ASan lanes), the
  performance comparison, and the Windows matrix before merge, as the
  patch's own status notes.

## 6. Costs

The patch adds one cache-line-aligned counter (64 bytes per Farm), one
successful admission CAS and one release per accepted attempt, and keeps
the existing `Cf` increment and decrement; producer and consumer data paths
gain nothing. Subscription cost has not been measured for it; the change is
one CAS, which cannot be material.

The unsubscribe mutex stays, and this document's measurement is the
reason: 126 views leaving at a barrier on a 32-CPU machine while a producer
writes take 243 µs end-to-end against a 230 µs baseline for the same barrier
with no unsubscribe (the lock's share is about 14 µs); 30 leavers, with no
oversubscription, 47 µs against 17 µs, about 1 µs per leaver of handoff;
the critical section is 40 ns per view (126 views released by one thread:
4.5 µs). Producers, live consumers and subscribers never take it. A
lock-free unsubscription would save at most those microseconds per herd
event and would make the `Cf`-last leaver wait on other threads' releases,
which is blocking under another name. Declined.

## 7. Open points, all minor

- **Slot across retries.** `subscribeOnce` reserves and releases a slot per
  attempt, so `subscribe`'s `RETRY` loop pays two extra atomics per retry
  and can flap `FULL` under a herd. Reserving once in `subscribe` and
  passing the held slot down would remove both; not required for
  correctness.
- **`FULL` while a herd departs.** Retiring views hold slots until their
  final root release, so a subscriber racing a departing herd can be
  rejected for a few microseconds. Document it; do not retry inside the
  library.
- **Destroy strictness.** `destroy` is now fatal with any outstanding slot,
  including a stalled probe, not only with live consumers. Correct for an
  API that requires exclusive lifetime, and worth one sentence in the
  destroy row.
- **`tryReleaseOldest` outside the lock.** Still optional; a view holding
  its position count cannot create the hole by releasing its confirmed
  prefix.
