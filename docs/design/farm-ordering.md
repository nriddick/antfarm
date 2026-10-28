# Farm ordering audit: S1, S2, Live follow-ups, and sanitizer evidence

Baseline: local `cf2aa91` (the acq_rel Wt reservation fix). These follow-ups
change ownership/validation, not memory-order strengths.

Status: S1 and S2 audit the Sub0-era protocol. The Live lifecycle (SPEC 3
I3, 6b, 6c) has since removed the frontier fallback, the producer plant and
every pulse: each epoch is `Live` from initialization until a confirming
holder discharges it, and a subscriber attaches only at an epoch it saw
`Live` under its own `Sub`. The two sections stay as the record of why those
rules existed. The regression schedules, sanitizer evidence and fast-path
audit below are current.

## Subscription (S1, Sub0 era)

The original mixed `Es`/`SqCs` theory is not a separate ordering bug.
Any initialized `SqCs` selects a preallocated leaf, and the view remembers
the leaf it actually incremented. A newer-lap `Seqt` paired with old `Es`
is rejected by the existing sequence-epoch bound; acquiring new `Es`
orders its sibling metadata. Main-walk candidates remain protected by a
root/pulse history, once the stale unsubscribe plant below is removed.

There was nevertheless a reachable defect in the fallback. With K=4,
segCap=65536 and Exmax=196608, a subscriber reads Wt=0 and pauses before
its first SUB. Another view drains epoch 0 and advances to epoch 1. A
producer refreshes quota anchored in epoch 1, certifying slots 2,3,0,
but does not reserve yet. The subscriber's original walk visits only slot
0, sees zero low root, falls back to old epoch 0 and passes Wt1 < epoch 4.
Spending the retained grant now encounters the subscriber's live root.

Require `Es == Wt1 >> segShift` only when no live candidate was found.
The confirming root release imports the reservation crossing out of the
old epoch; the subscriber acquires that history through its root RMW/read.
Trivially complete spanning interiors import the crossing through Es.
SPEC 6b records the case split and the reliance on the existing I1/I3
reclamation invariants. Older live candidates must remain admissible:
rejecting them by a blanket maximum-grant bound can prevent backlog drain.

## Plant ownership (S2, Sub0 era)

The producer invocation is safe: it plants before publishing its own table,
whose size is necessary to confirm its starting epoch. The epoch cannot
complete or lap during that plant; crossed interior epochs have equal
Seqt/SeqtNext and return without planting. An entering consumer that leaves
this incomplete epoch replaces its last count with a pulse atomically.

The post-unsubscribe scan lacked that ownership. It could check an incomplete
epoch, pause while the last consumer completed it and a producer swept it,
then plant on the reclaimed root. A subscriber could consume this transient
pulse before the post-CAS retract and thereby pin already-certified space.
No full lap or weak-memory observation is needed.

Remove the redundant scan. Every held incomplete epoch is already covered
by the last-releaser CAS; never-entered epochs are covered by the crossing
producer. Keep the producer helper private and preserve its defensive
post-CAS check. Moving metadata reads inside its CAS loop would not make
an unpinned scan safe: zero/count/zero ABA remains possible.

## Live follow-up: spanning-interior initialization visibility

A subscriber can attach at an interior epoch of a spanning table with
`nextSeq` in a later epoch. Its discovery history includes that interior's
`Es`, but not necessarily the spanning table's sentinel. Another consumer
can finish the table and a third can confirm and discharge its start epoch
without ever acquiring that sentinel. The C++ memory model then permits the
subscriber's first hot-path `moveRef` to see the later epoch's `Es` as
uninitialized, even though sequentially consistent execution and the
multi-copy-atomic x86/ARM hardware used for testing cannot produce that
outcome.

Subscription validation now acquires `Es` at `Seqt`'s epoch when that differs
from the attach epoch and requires it to match. A spanning producer publishes
crossed epochs in order, so this one acquire supplies every intermediate
initialization before the subscriber's first advance. The candidate, Seqt and
Wt bounds remain; the Wt lap check is defense in depth because the new lap's
Seqt store already precedes its Live raise.

`farm_ordering.d`'s `spannedAttach` schedule reaches this exact path: one
consumer finishes the spanning table on an idle re-walk, another discharges
its start without reading the sentinel, and the subscriber attaches at the
interior before consuming the next table. It runs under both LDC and DMD.
This is deterministic path coverage, not a claim that the unmodified code
fails on x86; the discriminating evidence is the memory-model analysis, not
this hardware execution.

## Regressions and mutation

`make -C tests/farm check-farm-ordering` builds and runs the controlled schedules with
LDC and DMD. They use production-elided scheduling hooks to execute other
actors' operations during an exact pause; no sleeps or random interleavings
are needed. On cf2aa91 both original `farm_ordering.d` schedules aborted with
only hooks added, when the always-on fresh-segment guard saw a live root as
Wt moved 65536 -> 262144.

Both schedules now state the Live-era property of the same interleaving.
S1's subscriber probes Wt == 0 and pins slot 0 after a producer has already
been granted it. It must attach at Live epoch one, not at the reclaimed
epoch, and spending the grant must pass the fresh-segment guard. S2's
departure leaves an incomplete epoch `Live` and creates no protection, and a
subscriber arriving after reclamation never selects the reclaimed epoch.
Both check exact callback accounting and quiescent protection of incomplete
epochs.

`subscription_live.d` holds the schedules found while designing the Live
scan (SPEC 11). Each payload carries an id that must execute exactly once,
and progress is judged by the views that already exist, before any new
subscription. Built against the rejected newest-down scan's ablations, those
oracles strand work and stall the producer, so the schedules are not vacuous.

`leaf_reuse.d` tests the memory-order chain separately. C reads one table,
R consumes a distinct crossing table and migrates before C drops, then C
drops a shared leaf non-edge and Q drops last. The producer sweeps and
reuses C's words before joining either worker. Scheduling flags are relaxed
so they supply no alternate happens-before edge. The final conflicting
write uses the first virtual mapping: TSan cannot identify the magic
buffer's two mappings as aliases of one physical region.

## TSan configuration

Previously Farm and example builds omitted `-d-version=TSan`; actor,
private-actor, review and allocator torture builds included it. The version
enabled manual pin-drop release and sweep acquire annotations in Farm.
LLVM 22.1.8 already models acq_rel leaf RMW joins and root release sequences.
The old comment denying that chain was incorrect.

All these lanes now use `-fsanitize=thread -d-version=TSan`. The D version
selects runtime defaults only (`history_size=7`, `halt_on_error=1`); Farm's
manual synchronization annotations have been removed. The shell runners
set matching defaults. The full Farm runner also executes the leaf test.

Controlled evidence with LDC 1.43.0 / LLVM 22.1.8:

| Leaf decrement | Manual annotations | Result |
|---|---|---|
| acq_rel (real code) | absent | 20/20 clean |
| release only (scratch mutation) | absent | 6/6 TSan races, exit 66 |
| release only (same mutation) | present | 20/20 clean: real defect masked |

Thus absent hooks do not explain away the earlier Wt race or create a
leaf-chain false positive on this runtime. Present hooks can hide a broken
real edge. These controls do not prove that every possible race is observable
by TSan, particularly cross-alias accesses or atomic-only lifecycle errors.

## Remaining fast-path audit

- `write`, retained quota, partial admission and failed
  transactional prepare converge on the existing acq_rel Wt reservation.
  No sibling release-only reservation remains.
- Root updates remain release RMWs: they forward the leaf acq_rel join;
  they need not acquire other roots because a zero-root acquire observes
  the all-RMW release sequences from each root contributor.
- The `Live` raise is a release RMW sequenced after the producer's Wt
  reservation and before its release-store of Es. The `Live` discharge is a
  release CAS after confirmation's acquire of Es[e+1], by a holder that
  keeps its reference until the CAS lands. A sweep that passes a
  discharged slot therefore acquires the next epoch's initialization
  (SPEC 3, 9).
- SUB increments are followed by an acquire re-read of the candidate and
  its metadata, and the Wt lap bound. Their release ordering is not a
  producer-to-producer reclamation gap, and a Sub never revokes quota a
  producer was already granted.
- Raw Sd is accounting under a pin; the pin-drop/sweep/Wt chain orders it
  before next-lap reset. Raw claim counters reserve disjoint work and do
  not publish completion. Shc and Tprogress completion RMWs remain acq_rel.
- Raw ticket/ledger/count operations are mutex-protected, single-owner, or
  advisory scheduling/accounting state. Raw immutable table fields follow
  acquire Tsent; segment sibling reads follow acquire Es under ownership.
- Every payload callback contributes through its shard completion join.
  Table hooks run after that join and retain their view pin through return.

No additional missing-acquire fast-path defect remains open after the
Seqt-epoch validation above. The I3 induction and oldest-first discovery
argument still have the separate weak-memory proof obligations recorded in
SPEC 11. Cancellation
must not abandon a reserved table or callback; shutdown still drains/joins
views before unregistering producers and destroying the Farm. Neither fix
adds blocking or retries beyond the existing subscription RETRY behavior.

Consultation rejected two proposals after checking full histories: moving
plant checks inside CAS does not cure ABA, and an abstract store-buffering
counterexample omitted the confirming release's acquisition of Es[e+1].
No speculative seq_cst strengthening was made. This is a source-level proof
using the quota and Live invariants, not a model checker proof of the
entire Farm or cross-architecture execution evidence.
