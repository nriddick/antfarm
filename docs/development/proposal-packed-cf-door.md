# Proposal, examined: subscribers in flight in `Cf`'s high half, the leaver's door, and how far the principle reaches

Status: examination, 26 October 2026. Base for the code references is
`fix/scan-2.1.1` at `f593920` (the unsubscribe mutex of `47b980c` is in
force there; `2.1.1-progress` does not carry it). Nothing is applied to a
checkout. The companion documents are the
[validate-before-Cf proposal](proposal-subscribe-validate-before-cf.md),
whose "unresolved departure race" section is the defect this proposal is
aimed at, the [pin-before-Cf document](proposal-subscribe-pin-before-cf.md),
revision 3, whose findings this one supersedes where they differ, and the
parallel session's
[packed membership examination](packed-membership-and-zeroth-consumer.md),
which frames the same proposal as an ownership-transfer question and asks
for an executable model before any mechanism is chosen; section 9 answers
its decisions within this model's scope.

**The proposal, as stated.** Pack `Cf`: the low 32 bits count consumers
(`Cflow`), the high 32 bits count subscribers in progress (`Cfhigh`),
incremented at the start of subscription. An unsubscriber unconditionally
adds a `Sub0` unit to its oldest root at the start of its unwind, then
decrements `Cflow`. If subscribers are in flight and it was the last
consumer out, it pauses, holding the door open until they establish. If it
was not the last out, it revokes its unit. A subscriber increments
`Cfhigh`, unwinds if `Cflow + Cfhigh` exceeds the cap, otherwise finds the
earliest `Sub0` and attaches, retrying through the usual consistency
checks; once attached it increments `Cflow`, and the held unsubscriber may
then drop.

**Verdict in four lines.** The in-flight half and the hold are right, and
they are the part of the proposal that closes both open defects, A1 and
the late pulse. They are not sufficient: the hold decides on a snapshot,
and a subscriber admitted after the snapshot can still join the leaver's
count and advance past it before the leaver drops; one more bit, a
`closing` flag that pauses admission across the last leaver's anchor drop,
closes that window. The door unit is redundant: the leaver's own count
already blocks every sweep and every certification while it holds, and the
last-releaser CAS already turns that count into the pulse atomically.
Leaver-versus-leaver stays the mutex's job; the lock-free form exists,
needs a third counter and a demotion rule, and buys the 14 µs the herd
measurement already put a number on.

Every claim below was checked by exhaustive interleaving over a small
model (section 3); each failure cites the model's shortest trace, each
success the scenarios and state counts. The model abstracts table
accounting, so it says nothing about I3's incomplete-epoch pulses, the
producer's crossing plant, or the subscriber's wrap bounds. Those are
unchanged by the proposal, which is itself a finding (section 6).

## 1. What the proposal is for

Three things are on the table, and it is worth separating them because the
proposal addresses them unequally.

1. **A1.** A subscriber counted in `Cf` before it holds any pin lets a
   leaving last consumer decide "not last" and drop its roots plainly. For
   the interval between that drop and the subscriber's first `Sub`, no root
   in the Farm is nonzero. The validate-before-Cf patch closes this by
   counting membership only after validation.
2. **The late pulse.** The last leaver decides at its `Cf` decrement and
   plants at its position drop. A subscriber that attaches between the two
   joins the leaver's count without a 0→1 edge, advances, and releases the
   root; the leaver's plant then lands on a confirmed root behind the only
   member. Nothing will clear it, a sweep through it is blocked, and the
   Farm's next quota renewal fails until another subscription happens to
   land there. The other session found this in both the baseline and the
   patch and left it open; the mutex does not touch it because the
   subscriber takes no mutex.
3. **Herd serialization.** `47b980c` serializes unsubscription so that two
   leavers cannot both read "not last" and empty the Farm (S3). The cost is
   known: 126 views leaving at a barrier on 32 CPUs, 243 µs end-to-end
   against a 230 µs baseline for the same barrier with no unsubscribe; the
   critical section is 40 ns per view.

The proposal is aimed at 2 and 3, and inherits a solution to 1 from the
ordering "attach, then increment `Cflow`".

## 2. Two readings of the text, and the one this document takes

"Adds a `Sub0` to its oldest root" and "then the previous un-subber can
drop" admit two protocols.

- **Reading A, the door beside the count.** The leaver keeps its counts
  while it holds the door; the unit is a marker on a root that already
  reads nonzero. The subscriber lands at the earliest nonzero root as it
  does today, which is the leaver's count with the unit beside it. "Drop"
  means release the counts.
- **Reading B, the door instead of the count.** The leaver converts its
  count into the unit at once and waits with no count held; the unit is
  what blocks the ring. Every subscriber's attach at it is then a 0→1 edge,
  which clears it, so there is no joining and no late pulse by
  construction. "Drop" means nothing remains to drop; the leaver only has
  to revoke a unit that nobody cleared.

Reading B is the more radical and the more tempting, because the 0→1 edge
does the handoff with no wait at all. It fails on a fungible-unit ABA:
units are counted, not owned. A leaver that is not last must remove the
unit it left, and it can only remove "one unit if the count is zero and a
unit is present". The sequence subscriber-attaches (clears the unit),
subscriber-unsubscribes-at-the-same-root (leaves its own unit through the
last-releaser CAS), first-leaver-revokes removes the second leaver's
anchor, after which the second leaver's `Cflow` decrement finds itself
last with no pulse behind it. A per-root owner word would fix it and
cannot be made atomic with the root. Reading B also still needs the
closing bit of section 4.2 for the leaver's own `Cflow` observation. This
document therefore takes reading A and treats the unit as what it is
there: a marker.

On "oldest root": after `tryReleaseOldest`, a view's oldest held epoch is
either unconfirmed, in which case its release leaves an I3 pulse by the
existing helper whatever the Farm's membership, or it is the position. The
empty-Farm anchor belongs on the position, as today (6e). Subscribers find
the earliest `Rtlow != 0` either way, so the choice does not affect where
they land; it affects only which root carries the marker, and the marker
turns out not to matter.

## 3. The model

`anchor-protocol-model.py`, beside this document, explores every
interleaving of a fixed scenario breadth-first and reports the first
violation with a shortest trace. State: the packed consumer word (members,
probes, retirees, closing), K = 4 roots as (count, units, Sub pins), a
write frontier F that a producer advances when the next root reads zero,
and a lock. Threads: members that advance toward F (take the next root,
clear units on a 0→1, drop the old root) or leave; subscribers that admit,
pin every root up to F, take the earliest nonzero root as candidate, may
retry once, attach in place, commit, unpin. Taking a count is two RMWs as
in `takeRootCount`. Every root below F is confirmed; root F is the
unconfirmed frontier, and a drop to zero there leaves one unit (the I3
rule, kept so that an idle leaver at the frontier behaves as today). Not
modelled: wrap and the 6b-c bounds, interior held epochs, table
accounting, the producer's crossing plant.

Violations in every state: a tally underflow, every root zero (the ring
unprotected), a closer whose root count is not exactly one, deadlock. At
quiescence (no thread mid-protocol, no lock held, not closing): counters
match the live views; an empty Farm has a pulse and no count; a non-empty
Farm has no pulse behind every member and no pulse joined to a count,
because nobody will clear either.

Variants:

| Variant | Admission counts | Leaver | Lock |
| --- | --- | --- | --- |
| `current` | members, at admission | decide at decrement, anchor at drop | yes |
| `validate` | slot at admission, members after validation | same | yes |
| `door` | probes; members after attach | door unit, decrement, hold for probes, bare revoke if not last | no |
| `door+close` | same | plus `closing` around the anchor drop | no |
| `close+lock` | same | decrement, close without holding, anchor | yes |
| `hold+close+lock` | same | decrement, hold for probes, close, anchor | yes |
| `hold+retire` | same | retire (members−1, retirees+1), hold, anchor; no closing | no |
| `hold+close+retire` | same | retire, hold, demote on `closing`, close, wait to be the only retiree, anchor | no |
| `door+hold+close+retire` | same | the previous with the door kept | no |

Scenarios: one member at root 0 with the frontier at 1 (the late-pulse
setup); two members on root 0; members on 0 and 1; one member and two
subscribers; an empty Farm with its anchor and two subscribers; and three
larger runs with two subscribers, two producer steps and two advances.

| Variant | 1 member | 2 on root 0 | 0 and 1 | 1 member, 2 subs | empty, 2 subs | large (3 runs) |
| --- | --- | --- | --- | --- | --- | --- |
| `current` | A1 | A1 | A1 | A1 | A1 | |
| `validate` | late pulse | late pulse | late pulse | late pulse | late pulse | |
| `door` | joined pulse | joined pulse | joined pulse | joined pulse | joined pulse | |
| `door+close` | ok | joined pulse | joined pulse | joined pulse | ok | |
| `close+lock` | closer not alone | | | | | |
| `hold+close+lock` | ok | ok | ok | ok | ok | ok, 0.29 M / 0.74 M / 0.16 M states |
| `hold+retire` | deadlock | ring unprotected | ring unprotected | stranded pulse | stranded pulse | |
| `hold+close+retire` | ok | ok | ok | ok | ok | ok, 0.40 M / 1.01 M / 0.17 M states |
| `door+hold+close+retire` | ok | ok | ok | ok | ok | ok, 0.82 M / 2.04 M / 0.26 M states |

The first three rows reproduce what is already known: the model finds A1
in five steps, the other session's late-pulse schedule in thirteen, and
confirms the proposal's post-observation window. That is the calibration;
the remaining rows are the findings.

## 4. Findings

### 4.1 The in-flight half and the hold are necessary

`close+lock`, which closes without waiting for probes, fails in eight
steps: the probe admitted before the close attaches under the leaver's
count, and the anchor drop finds two counts on its root. The hold is what
makes the leaver's decision wait for the one party that can still change
it. Counting probes where the leaver can see them in the same word as the
member count is what makes the hold decidable from one load. The other
session's `consumerSlots` is the same count in a separate word, and its
document notes that two loads of two words are not a snapshot; packing
them is the fix for that too.

### 4.2 The hold alone leaves the post-observation window

`door`, the proposal as stated, in the one-member scenario:

```text
 1. M0: unsubscribe begins
 2. M0: door: Sub0 unit on 0 (count held)
 3. M0: members-1 (last)
 4. M0: hold: nobody in flight; anchor
 5. S1: admit (probes+1)
 6. S1: pin 0..1, candidate 0 (earliest nonzero)
 7. S1: attach: take count on 0
 8. M0: anchor drop on 0 (count->1, units 1)
 9. M0: done
10. S1: commit (members+1, probes-1)
11. S1: unpin; now a member at 0
end: roots=[(1, 1, 0), ...]  a pulse joined to the only member's count
```

The leaver observed "nobody in flight" at step 4 and acted on it at step
8; the subscriber arrived in between and joined. The joined pulse is the
late pulse one step earlier: when S1 advances and drops root 0, the unit
stays behind it. The fix is to make the observation and the drop one
critical section against admission: the leaver's decision is a CAS from
"no members, no probes, not closing" to `closing`; a subscriber whose
admission RMW returns `closing` waits until it clears, which happens right
after the anchor drop. `door+close` passes the one-member scenarios. The
subscriber's wait is one CAS and one RMW of another thread, and it occurs
only when the Farm is emptying; the leaver's wait is a probe's walk. Both
are bounded by another thread's progress, which is the same class of
blocking as the mutex and much shorter.

### 4.3 The door unit is redundant

Under reading A the leaver holds its counts while it waits, so the root
already reads nonzero to every sweep and every subscriber. What the unit
adds is visible to nothing; what it costs is two more RMWs and a rule.
Keeping it is harmless only under a discipline the model enforces: the
unit is removed in the same CAS as the count drop or while the count is
still held, never after. A bare `fetch_sub(Sub0)` after the count has
gone is the race 6c already documents (a 0→1 clears the unit first and
the retract borrows from the `Sub` field); the model's bare revoke is safe
only because its count is held. `door+hold+close+retire` passes every
scenario that `hold+close+retire` passes, with more states and no
different outcome. Recommendation: do not plant it. The principle the
proposal reaches for, "the leaver becomes the 0th consumer", is already
how the last-releaser CAS works; what was missing was the ordering of
that CAS against admission, not an earlier plant.

### 4.4 Leaver versus leaver

`door+close`, two members on root 0, one subscriber:

```text
 1. M0: unsubscribe begins
 2. M0: door: Sub0 unit on 0 (count held)
 3. M0: members-1 (not last)
 4. M0: revoke door unit on 0
 5. M1: unsubscribe begins
 6. M1: door: Sub0 unit on 0 (count held)
 7. M1: members-1 (last)
 8. M1: hold: nobody in flight; close (CAS closing=1)
 9. M1: anchor drop on 0 (count->1, units 1)
10. M1: unclose
11. M1: done
12. S2: admit (probes+1)
13. S2: pin 0..1, candidate 0 (earliest nonzero)
14. S2: attach: take count on 0
15. M0: plain drop on 0 (count->1, units 1)
16. M0: done
17. S2: commit (members+1, probes-1)
18. S2: unpin; now a member at 0
end: roots=[(1, 1, 0), ...]
```

M0 decided "not last" at step 3 and was still holding its count at step
15. The closer M1 could not know that; its anchor drop left the unit
beside M0's count, M0's plain drop left it beside S2's. The mutex excludes
this by construction: M0's whole unsubscribe completes before M1's begins,
so when M1 decides last, no other leaver holds anything.
`hold+close+lock` passes every scenario.

The lock-free form is possible and the model has it: a leaver retires
with one RMW (members−1, retirees+1); if that RMW emptied the members, it
holds until no probe is in flight; if meanwhile a member commits or
another retiree has set `closing`, it is demoted and drops plainly; else
it closes and waits until it is the only retiree, anchors, uncloses and
unretires in one RMW. Two of those rules were found by the model. Without
the demotion rule, two "last" retirees (members went 1→0, a subscriber
committed, 1→0 again) wait for each other forever. Without the wait to be
the only retiree, the closer anchors over a count that an earlier retiree
is still about to drop, and the drop leaves zero (`hold+retire`: "ring
unprotected" in seven steps). `hold+close+retire` passes every scenario
including the million-state run.

Costs and benefit. A third counter field, two more RMWs per departure, two
more wait conditions, and one more thing the SPEC has to prove; against a
herd departure cost that the measurement put at about 1 µs per leaver of
handoff and 14 µs of lock share for 126 views, with producers, consumers
and subscribers never taking the lock. The mutex also keeps the lock-free
subscriber side simple: with it, the closer is alone among leavers by
construction, and the `closing` bit only has to exclude subscribers.
Recommendation: keep the mutex; record the lock-free form as an option
with its two rules, so that the next person does not rediscover them.

### 4.5 Where the anchor lands

`unsubscribe` does not migrate a confirmed position onto the frontier
before anchoring; it anchors wherever the view stood. A view that leaves
without having run its idle path can therefore anchor on a confirmed root
behind the tail, where a sweep reaching it fails until the next
subscription lands there and clears it on its 0→1. That is today's
behaviour and the proposal does not change it. It is a quota cost, not a
hole, and it is distinct from the late pulse, which strands a pulse behind
a member. If it is worth fixing, the fix is one `migrateToFrontier()`
before the decision, under the mutex, and it is independent of everything
above.

## 5. The protocol this document recommends

Packed `Cf`, one 64-bit word: bits 0..15 members, bits 16..31 probes, bit
63 `closing`. (Sixteen bits each; the cap is 128. The lock-free form would
use bits 32..47 for retirees.) All RMWs on it acquire-release; today's
relaxed increments are not enough, because the leaver's "not last" now
orders the subscriber's attach before the leaver's drop, and the closer's
CAS orders the probe's pins.

Subscriber, one attempt (`subscribeOnce`):

1. `old = fetch_add(Cf, PROBE)`. If `members(old) + probes(old) + 1 >
   MAX`, `fetch_sub(Cf, PROBE)` and return `FULL`: nothing was pinned.
2. If `closing(old)`, spin until the bit clears. The closer is one CAS and
   one RMW away from clearing it.
3. Walk, pin before reading, pick the earliest `Rtlow != 0`, validate the
   6b-c bounds, exactly as today. On any failure drop the `Sub`s, then
   `fetch_sub(Cf, PROBE)`, return `RETRY`.
4. `incLeaf` on the candidate: the real pin. Then one RMW: `Cf += 1 -
   PROBE`. Then drop the `Sub`s.

Leaver (`unsubscribe`), under the mutex as today:

1. `tryReleaseOldest`; release the interiors as today.
2. `old = fetch_sub(Cf, 1)`; `last = members(old) == 1`.
3. If not last: drop the position as today for a non-last view (plain if
   confirmed, the helper if not). Done.
4. If last: loop on a load of `Cf`. If `members > 0`, a probe committed;
   go to 3. If `probes > 0`, wait. Else CAS `Cf` from exactly
   (members 0, probes 0, clear) to `closing`; on failure reload.
5. Anchor: the last-releaser helper on the position, as today for the
   last-of-Farm view. Its count is exactly one (no member, no probe past
   admission, no other leaver under the mutex); the SPEC can assert it.
6. `fetch_and(Cf, ~CLOSING)`. Unlock.

Two properties of the waits. The leaver waits in step 4 holding the
mutex, which no subscriber ever takes, so it holds nothing the probe it
waits for needs. And the mutex bounds uncounted retirement to one view:
between step 2 and the unlock, exactly one view holds leaves and roots
while belonging to neither count, so at most 129 views hold leaves at any
instant and no uncounted view holds a `Sub`. The capacity bound on `Sub`
units per root is unchanged at 128.

Producers: unchanged. `Cs` snapshots and the first-claimant heuristic read
`members(Cf)`. `destroy` is fatal unless `Cf == 0`, which now also means
no probe in flight.

What this leaves of 6c: everything. Deposition sites stay three; the
state-machine table gains no row; the clear sites are unchanged; the race
proof is unchanged. What it changes in 6c is one sentence: the
last-of-Farm row's precondition becomes "under `closing`, no probe in
flight".

The relation to the validate-before-Cf patch: its `consumerSlots` is the
probe count, moved into the same word; its order "commit, then attach"
becomes "attach, then commit", which removes the one place where its
proof needed the held `Sub` to stand in for a count; its unsubscribe-side
slot release goes away because a retiring view is excluded from new
admissions by the mutex, not by a slot; its seven schedules remain valid
tests, and the late-pulse schedule its document describes becomes the
eighth, now expected to pass.

## 6. How far it goes: the surface

The principle reaches exactly the membership boundary and no further.
The producer protocol, I1 through I5, table accounting, the walk and its
bounds, `plantIfUnprotected`, `takeRootCount`, the last-releaser helper
and its confirmation recheck, and the C ABI are untouched. Section 11's
framing of `Sub0` as the member that never leaves stays true; the
proposal's "0th consumer" is that member, and the change is when the last
leaver is allowed to become it.

SPEC sections that change:

| Section | Change |
| --- | --- |
| 2a | `Cf` is a packed word: members, probes, `closing`. |
| 2b | No change to the `Rt` layout. The "transiently more than one unit" sentence stays; without the door the bound on units does not move. |
| 3, I3 | Last sentence: "the last-of-farm release retains a subscribe anchor, decided under `closing` with no subscriber in flight". |
| 4c | Rewrite: admission is one RMW on the packed word; `FULL` counts probes; a probe is not a member. |
| 6b-a | Admission step as in section 5; the `closing` wait. |
| 6b-e, 6b-f | Attach, then commit; the commit RMW; failure paths decrement probes, never members. |
| 6b, failure paragraph | "unwind `Cf`" becomes "release the probe". |
| 6c | One precondition on the last-of-Farm row; one sentence under "last-releaser" naming `closing`. |
| 6e | Rewrite of b and c's last bullet: the hold, the close, the assert, the unclose. The mutex paragraph stays. |
| 8 | No change; the retained-reference paragraph's "planting only at `Cf == 1` is not enough" remains true. |
| 9 | `Cf` RMWs become acquire-release; two rows: probe admission → closer's CAS; subscriber's commit → leaver's load. |
| 10 | `FULL` row: transient while probes are in flight; `destroy` row: or a subscriber in flight. |
| 11 | A history note: the door, why it was not needed, and the two rules of the lock-free form. |

Code (`antfarm/package.d`):

| Site | Change |
| --- | --- |
| `Cf` field (1359) and its doc; the comment at 556 | packed word; `members()` accessor |
| `add_consumer` / `sub_consumer` (1643, 1657) | replaced by admit / commit / release-probe / retire helpers |
| `destroy` (1611) | `Cf != 0` already covers probes once packed |
| `csl` snapshots (1920, 2012) and the first-claimant heuristic (2956) | read `members(Cf)` |
| `subscribeOnce` (2415) | admission moved to one RMW with the `closing` wait; `sub_consumer` on the two failure paths becomes release-probe; commit after `incLeaf` |
| `unsubscribe` (2516) | the hold loop, the CAS to `closing`, the assert on the position's count, the unclose |
| `WriteAuditPhase` | three phases for the schedules: after admission, after validation, after commit; one in the leaver's hold |
| tests reading `f.Cf` directly | `antfarm_test.d` (8 sites), `farm_ordering.d:96`, `write_protection.d:63`, `leaf_reuse.d:48`, `torture_tests.d:1445,1449`: use the accessor |
| C ABI | nothing reads `Cf` |

About 150 lines of code; the SPEC edits are larger than the code.

## 7. Tests

Each failing model trace is a deterministic schedule in the style of the
other session's `subscription_admission.d`, driven by the audit hooks:

- A1 (`current` trace): a probe admitted before its first pin while the
  last consumer leaves; expect the leaver to hold, the probe to attach,
  the leaver to drop plainly, and one count in the Farm.
- The late pulse (`validate` trace, the other session's schedule): expect
  the leaver to hold at step 4, the subscriber to attach and advance, the
  leaver to see a member and drop plainly; no pulse behind the member.
- The post-observation window (`door` trace): a probe admitted after the
  leaver's CAS to `closing`; expect the probe to wait, the anchor to land
  with count exactly one, the probe to clear it on its 0→1.
- The `FULL` path at the cap with a departing herd: probes count toward
  the cap; a rejected attempt touched no root.
- The seven schedules of the validate-before-Cf patch, re-expressed over
  the new phases.

A racing form in T27's style: 2,000 fresh Farms, one idle last consumer
leaving while a subscriber runs unscripted, checking `Cf == 1`, exactly
one count, no pulse, and under TSan. T27 itself must still pass, which the
mutex guarantees.

And the model as a regression: `python3 anchor-protocol-model.py
hold+close+lock 0 --F 1 --subs 2` and the other four scenarios run in
seconds; the matrix in section 3 is the expected output. If the SPEC's
protocol is changed again, change the model first.

## 8. Open points

- **Spin or yield in the two waits.** The leaver's wait is a probe's walk
  (hundreds of nanoseconds, or a preemption); the probe's wait is two RMWs.
  Spin with `pause`, and yield after a bound, for both.
- **Retry loop and the probe count.** `subscribe` re-admits on every
  `RETRY`, so a probe that keeps retrying flaps the probe count and can
  make the leaver's CAS lose repeatedly. Each retry has a window where
  probes read zero; the CAS wins in it or the probe eventually commits.
  Admitting once per `subscribe` call and holding the probe across retries
  removes the flap; not required.
- **`FULL` during a herd.** Unchanged from the other design: probes mid-walk
  count toward the cap. Retiring views no longer do, which is a small
  relaxation; the leaves have no cap, so nothing is sized by it.
- **The frontier migration before the anchor** (4.5), if the quota cost of
  an anchor behind the tail is worth one call.
- **The lock-free leaver form**, if a herd of departures ever shows up in a
  profile; its two rules are in 4.4 and in the model.
- **Callers that unsubscribe during shutdown.** The hold makes a last
  departure wait for a probe on another thread. Host worker teardown and
  Fiber consumer shutdown unsubscribe views while other workers may still
  be subscribing; neither path can have a probe waiting on the departing
  thread, but that should be checked in the code, not asserted here.

## 9. Relation to the companion examination

The companion document asks for an executable model before a `closing`
bit, a descriptor, or a mutex protocol is chosen, and lists seven
decisions a replacement SPEC must make. This document is that model for the
membership boundary, and its answers are:

1. **Transferable references or a persistent owner.** Neither is needed.
   The 0th consumer stays what section 11 of the SPEC says it is: the unit
   the last-releaser CAS leaves in place of the last count. The proposal's
   value is in ordering that CAS against admission, not in a new kind of
   reference.
2. **Which event creates, accepts and retires the offer.** There is no
   offer. The last leaver keeps its real count until nobody is in flight,
   decides under `closing`, and converts the count in one CAS; a later
   subscriber clears it on its 0→1 as today. The authority question the
   companion raises ("A must lose the authority to revoke or recreate it")
   is answered by never creating a second thing to revoke.
3. **Can an attaching consumer advance before departure cleanup completes.**
   Yes, and it is safe, because a consumer that attached before the close
   made the leaver "not last" (its commit is in the leaver's hold loop),
   and one that attaches after the close finds a root with no count.
   Section 4.2's trace is the schedule where neither holds without the
   bit.
4. **Ordering of arrivals after the last-leaver snapshot.** The snapshot
   is the `closing` CAS; an arrival after it waits for the unclose. The
   companion's alternatives (acknowledged claims, a helpable transfer
   record, serializing commit and departure) all do more than this needs.
5. **Charging probes and retiring owners.** Probes are charged to the cap
   in the packed word; a retiring owner is bounded to one by the mutex
   (section 5). Pausing a probe pauses the one leaver that is last;
   pausing a leaver pauses probes for two RMWs.
6. **Which guards survive for incomplete work.** All of them, unchanged:
   the I3 helper on unconfirmed epochs, the producer's crossing plant, the
   confirmation recheck. The model does not exercise them because the
   protocol does not touch them; the companion's broader model list is
   the right next step if the root protocol is ever changed.
7. **Progress if the awaited thread stops.** The leaver's wait is on a
   probe's walk and ends when the probe commits or withdraws; a probe that
   stops after `RETRY` without re-admitting has already withdrawn. A probe
   preempted mid-walk delays one departure for the preemption. The
   companion is right that no microsecond bound follows from uncontended
   measurements; the bound is "one walk of one other thread".

Of the companion's required schedules, the model covers the last leaver
paused before and after observing pending subscribers, the new consumer
attaching and advancing before the leaver finishes, every pending
subscriber withdrawing, two departures on one root and on different roots,
competing claimants, a healthy Farm with counts and no pulse, and
retirement after leaving the count. It does not cover stale tail probes
against certified quota, producer preemption after reservation, unconfirmed
interiors, shared-leaf attachment with a pending root increment, or slot
reuse and ABA across laps. The first and last of those are the 6b-c bounds
and I5, which the protocol leaves as they are; the shared-leaf case is
excluded during `closing` because no probe is past admission then, and
outside it a pending root increment behaves as a join, which the model
does cover at the root.

The companion's three scope levels place this protocol at level 1 plus
the hold and the bit, with the finding that level 1 alone does not close
the late pulse (`close+lock` and `hold+retire` in the matrix) and that
levels 2 and 3 are not required to close it.
