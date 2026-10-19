# Farm ordering audit: S1, S2, and sanitizer evidence

Baseline: local `cf2aa91` (the acq_rel Wt reservation fix). These follow-ups
change ownership/validation, not memory-order strengths.

## Subscription (S1)

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

## Plant ownership (S2)

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

## Regressions and mutation

`make -C tests/farm check-farm-ordering` builds and runs the controlled schedules with
LDC and DMD. `farm_ordering.d` uses production-elided scheduling hooks to
execute other actors' operations during an exact pause. No sleeps or random
interleavings are needed. Both schedules abort on cf2aa91 with only hooks
added: the always-on fresh-segment guard sees a live root when Wt moves
65536 -> 262144. Each fix passes; individually restoring its old behavior
reproduces the abort. The tests also check balanced retries, resubscription,
exact callback accounting and quiescent protection of incomplete epochs.

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
- SUB increments are followed by acquire root/metadata validation. Their
  release ordering is not a producer-to-producer reclamation gap.
- Raw Sd is accounting under a pin; the pin-drop/sweep/Wt chain orders it
  before next-lap reset. Raw claim counters reserve disjoint work and do
  not publish completion. Shc and Tprogress completion RMWs remain acq_rel.
- Raw ticket/ledger/count operations are mutex-protected, single-owner, or
  advisory scheduling/accounting state. Raw immutable table fields follow
  acquire Tsent; segment sibling reads follow acquire Es under ownership.
- Every payload callback contributes through its shard completion join.
  Table hooks run after that join and retain their view pin through return.

No additional missing-acquire fast-path defect was confirmed. Cancellation
must not abandon a reserved table or callback; shutdown still drains/joins
views before unregistering producers and destroying the Farm. Neither fix
adds blocking or retries beyond the existing subscription RETRY behavior.

Consultation rejected two proposals after checking full histories: moving
plant checks inside CAS does not cure ABA, and an abstract store-buffering
counterexample omitted the confirming release's acquisition of Es[e+1].
No speculative seq_cst strengthening was made. This is a source-level proof
using the existing quota/pulse invariants, not a model checker proof of the
entire Farm or cross-architecture execution evidence.
