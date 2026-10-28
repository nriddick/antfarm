# Packed membership and explicit ownership of the 0th consumer

Status: design examination, 26 October 2026. The implementation baseline is
`fix/scan-2.1.1` at `f593920`; source links identify the relevant repository
files, whose checkout revision may differ. This document examines a replacement
membership and Sub0 protocol. It does not amend the canonical
[SPEC](../../SPEC.md) or select a finished implementation.

The subsequent [Fmembers/Xfer/Live proposal](proposal-fmembers-xfer-live.md)
records the 28 October design: separate root fields, Xsegment and Xgen,
early membership promotion, and a mutex restricted to first/last transitions.
It supersedes this examination's candidate mechanisms where specified and
retains explicit proof obligations before a canonical SPEC revision.

This is a revision of open membership and reclamation. Attachment, departure,
abandoned-work protection, and pulse removal currently depend on the same
Sub0 transitions. A coherent replacement could preserve the ring geometry,
table accounting, and much of the producer machinery, but it must reestablish
why a zero root encountered by a qualifying sweep permits reuse. No complete
replacement protocol has yet been implemented or proved.

## 1. The proposal and the change in approach

The starting proposal packs two quantities into `Cf`:

```text
bits 31..0:  C, consumers
bits 63..32: P, subscribers in progress
```

A subscriber announces itself in `P`, withdraws if admission exceeds the
consumer limit, finds a suitable anchor, and establishes a reference before
becoming a consumer. An unsubscriber first places a Sub0 on its oldest root,
then decrements `C`. A non-last leaver revokes its offer; a last leaver with
pending subscribers holds protection until a subscriber establishes itself.

This develops Sub0's original interpretation as the **0th consumer**: a
reference that preserves ownership while ordinary consumers arrive and leave.
The essential question is who owns that reference at each stage, and which
event transfers or retires it.

The earlier [validate-before-Cf proposal](proposal-subscribe-validate-before-cf.md)
changed membership timing while retaining the old pulse rules. It closed two
reproduced subscription failures, but retained a departure race. Moving the
plant earlier while retaining the existing clear rules also reproduced that
race. These are failures of those particular combinations, not a disproof of
an explicit-transfer protocol. A replacement attachment rule must be assessed
together with its replacement departure and withdrawal rules. The early-plant
experiment replayed root operations in the existing implementation; it did
not implement packed counts or a complete replacement ownership protocol.

## 2. What Sub0 currently does

The existing representation combines several obligations in the same field:

- **Bootstrap and empty-Farm anchoring.** Construction supplies a reference
  before any real consumer exists. The last departure retains an anchor even
  if all work has completed.
- **Abandoned-work protection.** An incomplete epoch remains protected after
  its last visiting consumer leaves, including when other consumers remain
  elsewhere in the Farm.
- **Producer coverage.** A crossing producer protects work no consumer has
  entered, before publishing its own table.
- **Subscriber discovery.** Low-half counts and pulses identify candidate
  epochs from which a subscriber can begin draining work.

These obligations are related but are not interchangeable. A temporary
departure offer can be withdrawn after a handoff; a guard on abandoned work
cannot disappear merely because some unrelated consumer joined elsewhere.

The present root clear rule belongs to the tree protocol: `takeRootCount`
removes observed Sub0 units only on a root count 0→1. This is not synonymous
with a consumer attaching. Several consumers can share one leaf; joining an
occupied leaf may not modify `Rt` at all. Even a propagated root increment
may observe a nonzero count and therefore clear no pulse.

Conversely, `releaseRootLeavePulse` decides locally whether the departing
root count is final, while `Cf` decides membership globally. Those decisions
are separate atomic events. Packing global counters does not combine them
with the leaf/root transitions.

The implementation sites are in [antfarm/package.d](../../antfarm/package.d):
`tryCreate`, `subscribeOnce`, `unsubscribe`, `takeRootCount`,
`releaseRootLeavePulse`, and `plantIfUnprotected`. Their current contracts
are specified in SPEC 4b, 6b, 6c, and 6e.

## 3. What the packed word provides

Packing gives an atomic snapshot of admission and membership, and permits an
atomic conversion between them. The natural successful conversion is:

```text
(C, P) -> (C + 1, P - 1)
```

This conversion should follow establishment of the protected reference and
any ownership claim required by the new protocol. Its publication then has
a precise meaning for a waiting leaver. Separate increments and decrements
would expose an intermediate double count or missing announcement.

The definition of `P` must cover the whole interval for which another actor
may rely on the announcement. On failure, drop provisional resources before
withdrawing the announcement if a leaver uses that withdrawal as permission
to finish. A direct `subscribeOnce` caller may stop after `RETRY`; the design
cannot depend on that caller trying again.

Admission needs an explicit choice. Increment-then-check permits rejected
announcements to appear transiently above the limit. That can be supported
only if capacity, waiting, withdrawal, and overflow rules account for those
announcements. A conditional CAS admits only below the limit and makes the
bound simpler, at the cost of a retry loop under contention. Neither choice
settles root ownership.

All arithmetic must operate on the intended fields. A whole-word decrement
with `C == 0` borrows from `P`; testing whether the old packed word was
positive would not detect this error. Successful conversion requires `P > 0`
and room in `C`. Masks, signedness, overflow checks, and any flag bits need
named accessors and explicit preconditions.

The word does **not** identify a segment, validate a generation, revoke a
producer's retained quota, or record which pulse a subscriber accepted.
Global `P` also does not replace the per-root high-half `Sub` pins. Those pins
protect the metadata walk; removing them would need a separate lifetime proof.

### Departing owners need accounting too

If `C` is decremented before roots are released, a departing view still owns
resources although it belongs to neither `C` nor `P`. Call the number of such
owners `R` for reasoning purposes; this does not prescribe a third field.

`C + P <= MAX_CONSUMERS_LIMIT` alone does not bound `R`, total retained views,
or the number of departure offers. The existing unsubscribe mutex could
bound simultaneous retirement to one if retained through final cleanup.
Removing it requires another bound or an ownership transfer that frees the
departing view from those obligations. Keeping capacity charged through
retirement is another possible policy.

This matters if every leaver adds a Sub0 unit. The old 16-bit pulse field's
bound cannot simply be inherited from the old membership limit. Bounds must
include pending and retiring actors. Similarly, packed `Cf == 0` is not by
itself proof that destruction is safe while a retiring owner still exists;
exclusive, externally synchronized Farm lifetime remains required.

## 4. Define the ownership object before its encoding

There are two materially different interpretations of the 0th consumer.

**A transferable reference on an epoch.** Construction, abandonment, and
departure create references with stated obligations. A subscriber can claim
one, establish real coverage, and discharge the corresponding synthetic
reference. Several epochs can carry such references simultaneously. This is
closest to the existing per-root representation, but changes its lifecycle.

**A persistent virtual consumer with a position or held range.** The 0th
consumer continuously owns the earliest relevant position and moves or
transfers its range as work completes. This could support pulse-only
discovery even while real consumers are active. It also needs rules for
moving that position, retaining incomplete interiors, accepting producer
work, and handing it to real consumers. That is additional algorithmic state,
even if encoded without a separate object.

Both interpretations merit examination. A single oldest reference may protect
later epochs against a contiguous sweep, but that geometric observation is
not yet a complete replacement for per-epoch abandoned-work guards. The
design must preserve discovery and coverage when the next consumer advances,
leaves early, or never reaches an interior epoch.

For the first model, give each synthetic reference a logical identity:
physical slot, epoch, purpose, and current disposition authority. These can
be proof variables rather than stored tags. Anonymous counted units may
still work if aggregate accounting proves that a unit cannot be both claimed
and revoked. A count alone is not that proof.

An ownership lifecycle to specify is:

```text
held by a real consumer
  -> offered for transfer while valid protection is retained
  -> accepted by an attaching consumer
  -> discharged exactly once

an unclaimed offer
  -> withdrawn while other valid coverage remains
  or retained for the empty Farm / outstanding work
```

These are semantic transitions, not a proposed sequence of atomic instructions.
The design must say when each transition takes effect, who may complete it,
and what survives if that actor pauses.

### Attachment must finish the ownership transfer

If B accepts A's offered 0th reference, A must lose the authority to revoke
or recreate it. B must establish coverage before the offer ceases to protect
the epoch. Before B advances beyond the epoch, the transfer must be settled
or there must be a helper that can settle it without creating a stale pulse.

This need not require root count 0→1. Indeed, an explicit transfer is valuable
precisely because B can join a root that A still holds. A new rule that clears
Sub0 on attachment must be reconciled with the existing automatic clear,
producer plants, and departure revocations; independently keeping all of
those clear paths would risk double subtraction.

All fallible validation should precede acceptance unless rollback of an
accepted offer is itself specified. A subscriber cannot consume an offer,
return `RETRY`, and leave the previous owner assuming protection transferred.

## 5. The schedules the new principle must explain

### A late arrival after the last leaver observes no subscribers

A plants an offer, decrements `C` to zero, observes `P == 0`, and pauses.
B announces, attaches, and advances. A then finishes its departure.

With the current clear rule, B can join a nonzero root without clearing the
offer, and A's eventual release leaves a pulse behind B. This was reproduced
with an early plant as well as with the original late plant. A replacement
that settles the explicit transfer before B advances can address this
schedule; the hybrid result does not rule that out.

The required ordering is that B either participates in an unfinished
handoff or arrives after a completed transition with a stable anchor.
Neither participant may act on contradictory ownership decisions.

A `CLOSING` state in the packed word is one implementation candidate, not an
established requirement. Other candidates include explicit claims with
acknowledgement, a helpable transfer record, or serialization of commit and
departure. Each must identify its state transitions across `Cf`, `Lt`, and
`Rt`; an atomic update to `Cf` cannot silently make all three atomic.

### Several departures and several claimants

Two leavers may offer on the same root, or on different epochs. Two
subscribers may select the same earliest offer. The model must decide which
reference each action discharges, or prove an equivalent aggregate balance.
It must also specify how unused offers on confirmed segments disappear after
the last departing thread returns.

Unconditionally adding a counted unit and later unconditionally subtracting
one is insufficient if another participant can consume those units between
the two operations. Retaining a real reference may prevent root reuse, but
does not by itself establish exclusive authority over the Sub0 field.

### A probe starts after quota was certified

A stale `Wt` read can identify a slot that a producer has already swept into
retained quota. Adding a provisional Sub afterward can block future sweeps;
it cannot invalidate the existing grant. An offer must originate from valid
ownership, and attachment must validate its epoch and history. A rejected
probe cannot repair its failure by publishing ownership on that slot.

### Pending subscribers all withdraw

The last leaver cannot wait only for `C > 0`. Every pending subscriber might
reject, fail validation, or stop after returning `RETRY`. The `P == 0` case
must finalize retained empty-Farm protection, with late arrivals ordered
against that finalization. Polling the two conditions is not itself the
ownership handshake.

### A nonempty Farm has no pulse

In the current design, all live references can be ordinary counts and there
may be no Sub0 anywhere. Pulse-only discovery therefore requires a new
always-discoverable synthetic owner, or a way to request an offer from a live
owner. Otherwise retain live-count candidates alongside synthetic ones.
This choice determines whether ordinary active-consumer progress also needs
new responsibilities.

## 6. How far the implementation and SPEC changes reach

### Membership, construction, and destruction

SPEC 2a and 4c need the packed layout, admission policy, successful conversion,
withdrawal semantics, and treatment of retirement. Construction in 4b must
create the initial ownership object with the same claim rules used later.
Destruction in section 10 must account for pending and retiring operations.

`add_consumer` and `sub_consumer` no longer describe the whole lifecycle.
Helpers should expose semantic operations such as admission, commit, failed
withdrawal, and departure completion. The API can retain `subscribe`,
`subscribeOnce`, and `unsubscribe`, but its progress and failure contracts
may change.

### Subscription, tree references, and consumer movement

SPEC 2b and 6b–6e need a coordinated rewrite: candidate selection, pinning,
validation, ownership acceptance, publication of membership, and removal of
temporary protection. `incLeaf`, `takeRootCount`, `decRef`, and
`releaseRootLeavePulse` must agree on which operations can discharge an offer.

The leaf/root tree has partial propagation states. One thread may have raised
a leaf from zero while its root increment is still pending; another thread
can join that leaf. A model that equates an attached view with a separately
visible root increment would miss this case. The existing provisional pins
and leaf acquire/release chain cannot be discarded merely because `Cf` is
now packed.

Choosing the oldest held epoch also affects `tryReleaseOldest`, `moveRef`,
`migrateToFrontier`, and idle re-walks. A leaver cannot offer on its oldest
epoch and then release that epoch through ordinary prefix cleanup without
accounting for the offer's ownership. Likewise, a subscriber must not advance
past an unresolved obligation that can later leave protection behind it.

### Abandonment, producer publication, and reuse

SPEC 3, 5, and 8 require renewed arguments even if their code largely survives.
The current producer-only `plantIfUnprotected` owns an unpublished table;
that ownership prevents its start epoch from being confirmed during the
plant. A replacement must explain whether this creates the same kind of
0th reference as departure does, or a distinct guard with different clear
conditions.

The period between tail reservation and guard publication remains central.
`refreshQuota` observes roots, not global admission. If it keeps accepting
zero roots without rereading completion, the new protocol must preserve the
coverage that makes those observations safe, including grants already held
by other producers.

The fresh-segment tripwire currently ignores provisional Subs and rejects
real counts or Sub0 on newly reused segments. Reassess that interpretation
against the new ownership categories; do not weaken the diagnostic merely
because a new protocol triggers it.

### Every reader of the old scalar count

Readers must request either committed consumers, pending subscribers, or
total admitted capacity. Reading the raw packed word as a scalar is wrong.
The audit includes:

- `writeImpl` and `canFit`, which derive `Cs`, `SqCs`, and table overhead;
- the shard-overload check in `checkFirst`;
- [private actor emission](../../antfarm/actors/private_actor.d), which feeds
  the count into `rootCostHintFor`;
- [the live hybrid benchmark](../../threadpool/benchmarks/live_hybrid.d) and
  tests that wait for a target consumer count;
- destruction checks, diagnostics, and count assertions.

Leaf distribution tolerates an approximate consumer snapshot, but that does
not license treating announcement bits as consumers. Use explicit decoded
accessors rather than relying on an incidental narrowing cast.

### Callers and progress guarantees

Waiting inside unsubscribe introduces dependence on another thread completing
subscription. An arbitrary scheduling pause can therefore delay departure;
it is not valid to infer a microsecond bound from uncontended measurements.
Evaluate [host worker teardown](../../threadpool/source/threadpool/host.d)
and [Fiber consumers](../../ant_fibers/source/ant_fibers/scheduler.d), including
shutdown and execution-resource dependencies.

A leaver must not hold a lock needed by a subscriber it is waiting for. If
helpers are permitted, their transfer records must remain valid after the
original thread pauses or returns. `FULL`, `RETRY`, retry loops, and any new
wait need stated behavior; `subscribeOnce` in particular must not acquire an
unstated obligation to call again.

## 7. What can plausibly survive

The target is to preserve quota geometry and I1's less-than-one-lap bound,
the magic buffer, table encodings, callback execution and completion joins,
`Sd` confirmation arithmetic, and the producer reservation chain. None
depends inherently on a particular interpretation of the 0th consumer.

Preservation is conditional on retaining their lifetime premises. For
example, the completion arithmetic may be identical while the proof that a
finisher still owns its epoch needs new ownership cases. The producer's
contiguous sweep may be identical while I3's explanation is substantially
rewritten.

There are three useful scope levels:

1. **Packed admission only.** Replace the separate admission/member counters
   while retaining the existing root protocol. This is a small representation
   change and does not resolve the reproduced late-departure pulse race.
2. **Explicit transfer of per-epoch synthetic references.** Rewrite the
   membership and pulse lifecycle, retain local provisional pins and live
   candidate discovery, and reestablish producer reuse. This is the first
   substantial replacement to model.
3. **A persistent virtual 0th consumer.** Give it position/range responsibilities
   and potentially use pulse-only discovery. This reaches normal movement,
   abandoned-work recovery, and possibly producer/consumer coordination.
   Its state and progress costs need to be justified against level 2.

The packed layout does not force a choice between levels 2 and 3. Nor does
it establish that the unsubscribe mutex can be removed. Those decisions
should follow the ownership and progress argument.

## 8. Invariants and memory-order obligations

The replacement contract should distinguish the following properties:

- **Lifetime safety:** every data access is covered by valid epoch ownership;
  no legal producer grant can authorize overwriting that live data.
- **Coverage during transfer:** releasing one representation of ownership
  cannot expose a reclaimable gap before its replacement is established.
- **Exactly one disposition:** an offer cannot be both accepted and revoked,
  and a completed transfer cannot be recreated by a delayed former owner.
- **Discoverability:** the earliest outstanding work remains reachable to a
  new consumer through the selected candidate mechanism.
- **Resource bounds:** admitted probes, attached views, retiring owners, and
  synthetic references fit their respective counter and storage limits.
- **Reclamation progress:** a confirmed reference does not remain behind all
  consumers with no participant obliged to revisit or retire it.

The familiar quiescent single-pulse condition is a representation-specific
test of the current empty-Farm design. A replacement should either preserve
it or state and test an equivalent bounded, reachable empty anchor. It
cannot dismiss the observed two-pulse case merely by allowing more pulses:
that case also demonstrated blocked producer renewal and no cleanup by the
remaining consumer's forward progress.

Do not require every isolated zero root to mean completion. The current
frontier and a producer's publication window are the reason the proof is
about qualifying reuse sweeps and coverage, not isolated root values.

Specify synchronization after the logical transitions are settled. Admission
must be visible to any departure that relies on it; publication of successful
membership must carry the attachment and ownership-claim history; failed
withdrawal must order the cleanup it promises. Final ownership release must
still carry consumer accesses through the leaf/root chain, sweep acquisition,
and later producer reservations before reuse.

A packed RMW can provide an edge between arrivals and departures. It does
not automatically publish the contents or lifetime of a referenced epoch.
Keep the `Es`/sentinel publication edges and root/leaf release-sequence
arguments from SPEC 9 unless a replacement explicitly supplies them.

## 9. Model and validation work

Start with an executable logical model before selecting a closing bit, a
descriptor, or a mutex protocol. Record `C`, `P`, pending/retiring actors,
slot epochs, provisional pins, leaf/root propagation, synthetic-reference
authority, completion, producer grants, reservations, and unpublished work.
Some of this is proof state rather than proposed runtime storage.

Use a tiny ring, a reduced admission limit, two producers, and enough actors
to exercise simultaneous departures, competing claimants, and rejection.
Permit multiple laps. Split operations at their actual atomic boundaries,
including the leaf increment before root propagation and the producer's
reservation before metadata, guard, and table publication. Start with
sequentially consistent interleavings; then supply the separate weak-memory
argument. A finite model search is evidence within its bounds, not a universal
proof.

Required schedules include:

- fresh construction, empty writes, and repeated empty-to-active transitions;
- a stale tail probe after quota certification, including retained grants;
- producer preemption immediately after reservation;
- the last leaver paused before and after observing pending subscribers;
- a new consumer attaching and advancing before that leaver finishes;
- all pending subscribers rejecting or returning `RETRY` without retrying;
- two offers on one epoch, offers on different epochs, and competing claims;
- a healthy nonempty Farm with ordinary counts but no pulses;
- unconfirmed interiors, spanning tables, and a non-last consumer abandoning
  work that other consumers have not entered;
- a subscriber accepted through a shared leaf while another root increment
  is still pending;
- confirmation and physical-slot reuse racing offer cleanup, including ABA;
- capacity saturation while departures retain roots after leaving `C`;
- stalled participants and any waits or help paths needed for progress.

Assertions should track logical epochs and live grants. A quota certificate
for an older generation of the same physical slot is not automatically a
violation, and a provisional Sub on previously certified space is allowed by
the current retry protocol. A consumer or persistent guard accepted onto
unsafe certified space is the relevant failure.

Translate discovered schedules into controlled D tests, then add real
arrival/departure races and the relevant sanitizer lanes. The existing
[ordering schedules](../../tests/farm/farm_ordering.d),
[write-protection tests](../../tests/farm/write_protection.d),
[leaf-reuse test](../../tests/farm/leaf_reuse.d), and
[torture suite](../../tests/farm/torture_tests.d) remain useful regression
sources. Preserve their safety and work-accounting oracles; assertions tied
to an old pulse count need an explicit new equivalent if the representation
changes. Synchronous hooks must not force nested execution through a lock
that the proposed protocol intentionally serializes.

Measure both ordinary lifecycle cost and contention: repeated subscribe/leave,
simultaneous arrivals and departures, rejection and retry, worker teardown,
and producer throughput while churn occurs. Packing can save a counter but
can also concentrate writes on one cache line. Waiting and helping have
scheduling costs beyond the number of atomic instructions.

## 10. Decisions needed before a replacement SPEC

The next design iteration should resolve these questions explicitly:

1. Is the 0th consumer a set of transferable epoch references or a persistent
   owner with a position/range?
2. Which event creates an offer, which event accepts it, and when does the
   former owner irrevocably lose disposition authority?
3. Can an attaching consumer advance before departure cleanup completes?
   If so, what makes later cleanup incapable of stranding protection?
4. How are arrivals after a last-leaver snapshot ordered with finalization?
5. How are unsuccessful probes and retiring owners charged to capacity and
   bounded, including when they pause?
6. Which guards survive for incomplete work, and how are they discovered
   when no ordinary consumer owns that work?
7. What progress guarantee applies if the thread awaited by a leaver stops
   running, and what synchronization does any helper require?

The deliverable from that iteration is a complete transition contract with
an ownership argument, bounded-model evidence, and an impact list for SPEC
2–10, including section 7's count-dependent heuristics, and the lineage
discussion in 11. Implementation should follow that
contract as one coordinated lifecycle revision. The prior patch and its
passing tests are useful controls, not validation of this new principle.
