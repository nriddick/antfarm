# Proposed SPEC revision: Fmembers, Xfer, Live, and Xgen

Status: proposed lifecycle revision, 28 October 2026. This is a draft for
review, not an amendment to the canonical [SPEC](../../SPEC.md) or an
implemented protocol. It develops the
[packed membership examination](packed-membership-and-zeroth-consumer.md)
into concrete fields and transitions. The source audit used checkout
`c2431a5`; the earlier unsubscribe-mutex experiments used `f593920`.

The proposal separates provisional attachment, empty-Farm handoff, unfinished
epoch protection, and ordinary references. It also removes ordinary
unsubscriptions from the Farm mutex. The exact ordinary-attachment search,
the generation proof, and some completion/progress obligations below still
need validation before these rules can replace SPEC 2–10.

The earlier [validate-before-Cf patch](proposal-subscribe-validate-before-cf.md)
and [door examination](proposal-packed-cf-door.md) retain old Sub0 rules.
Their results are useful controls; they do not validate this replacement.

Review updates, 28 October: a Live-only alternative offers a substantial
reduction of this machinery. [Section 13](#13-live-only-review) records two
stale-window counterexamples against its first prototype and a tested
candidate-validation change. The subsequent hint-based prototype addresses
that issue; [section 14](#14-newest-pin-and-unpinned-hints-review) records its
remaining proof and measurement concerns. Sections 1–12 preserve the proposal
that was reviewed; they should not be read as a settled requirement to retain
Xfer.

## 1. Intended operating profile and scope

A Farm is intended to serve a consumer cohort within one L3 group, commonly
16 or 32 threads returning from a sleep cycle. Foreign producers can publish
appropriately sized batches. Avoid a CAS admission loop and avoid serializing
an entire unsubscribe herd merely to enforce a precise capacity boundary.

The proposed admission policy uses 128 as the initial consumer-count gate
and 256 as the inclusive aggregate admission ceiling. A pre-increment total
less than 256 admits one more; a post-increment total greater than 256 is
rejected. This resolves the otherwise ambiguous phrase "sum < 256."

The upper ceiling provides surge headroom, not a supported performance
target. Full hardware concurrency at 256 operators remains unvalidated.
Counter, indexing, and lifetime correctness must still cover accepted states.
`SqCs` remains clamped to `MAX_LEAVES = 12`, as `sqcsOf` already does in
[antfarm/package.d](../../antfarm/package.d).

This draft preserves ring geometry, producer quota arithmetic, table formats,
callback completion joins, and the existing work scheduler unless a specific
replacement is stated. Their lifetime dependencies must be re-proved against
the new root protocol. No performance improvement has been measured for this
implementation, because it has not been written.

## 2. Farm fields: proposed replacement for SPEC 2a

Rename `Cf` to `Fmembers`, an unsigned 64-bit atomic word:

| Bits | Field | Unit | Meaning |
| --- | --- | --- | --- |
| 63–32 | `Subs` | `2^32` | announced attempts not yet promoted or rejected |
| 31–0 | `Consumers` | 1 | committed membership, including attachment still in progress |

All mutations operate on the full atomic word. Do not mix separate 32-bit
atomic accesses with 64-bit updates. Decode both fields before adding them
for admission, and perform that sum in a sufficiently wide type.

Define `SubsUnit = 1UL << 32`. Promotion is one RMW:

```text
(Consumers, Subs) -> (Consumers + 1, Subs - 1)
equivalently: fetch_sub(Fmembers, SubsUnit - 1)
```

The caller owns a Subs contribution, so `Subs > 0`; room in the Consumers
field is a representation requirement. Promotion preserves the aggregate.
Its returned old Consumers value elects the handoff subscriber when zero.
No independently loaded Subs or Consumers value elects that role.

Add these Farm fields:

- `Xsegment`: physical segment containing the current Xfer, or **99** when
  unset. Every post-publication access is under the Xfer mutex. A valid value
  is less than `K`; 99 must never be used as an array index. Assert that the
  supported segment range excludes 99.
- `Xgen`: an atomic, unsigned 64-bit, monotonically increasing generation
  counter. Increment
  it once when publishing an Xfer and once when consuming an Xfer. It counts
  serialized handoff-state changes, not completed handoff pairs. It must not
  wrap during a Farm's lifetime.
- An Xfer mutex covering Xsegment, Xfer publication/consumption, and the
  corresponding Xgen increment. Ordinary consumer work and ordinary
  unsubscriptions do not take this mutex.

At critical-section boundaries, Xsegment is 99 with no Xfer anywhere, or
names the sole Xfer. All participants read the current Xsegment after locking;
none may clear a location cached before acquiring the mutex. A physical
index suffices while the Xfer protects that slot against reuse. Expected-epoch
validation of segment metadata is still required when establishing a view.

Xgen must be atomic because a last unsubscriber samples it outside the mutex.
Xsegment need not be atomic if every access follows its mutex rule.

## 3. Root fields: proposed replacement for SPEC 2b and 6c

Use zero-based bit numbering:

| Bits | Field | Unit | Meaning |
| --- | --- | --- | --- |
| 63–40 | `Transients` | `2^40` | provisional subscriber pins on this physical slot |
| 39–31 | reserved | — | zero |
| 30 | `Xfer` | `2^30` | the mutex-controlled handoff anchor |
| 29–21 | reserved | — | zero |
| 20 | `Live` | `2^20` | the epoch has not been discharged by confirmation |
| 19–0 | `References` | 1 | propagated leaf references |

```text
TransientUnit = 1UL << 40
XferMask     = 1UL << 30
LiveMask     = 1UL << 20
ReferenceMask = LiveMask - 1
```

References is an ordinary zero-based counter. There is **no bias or sentinel
reference**. This keeps `Rt == 0` as the producer sweep's empty-root predicate.
The briefly considered biased encoding is withdrawn.

A Reference still represents a propagated occupied leaf, not an individual
consumer. Leaf increments/decrements propagate only on their zero edges.
Joining an occupied leaf can leave Rt unchanged. In-flight edge propagation
also needs the existing protection and ordering argument; it cannot be
replaced with an assumption that every completed leaf increment immediately
establishes a new root count.

For a decrement, inspect the previous masked References field:

```text
old = fetch_sub(Rt, 1)
if (old & ReferenceMask) == 0: fatal reference underflow
```

This is a fail-fast check, not recovery from an invalid decrement. Live, Xfer,
and Transients cannot mask the underflow. Legal execution must never borrow
or carry between fields. References has a maximum encoded value of
`2^20 - 1`; Transients has a maximum of `2^24 - 1`. Reserved bits remain zero.

Each subscriber removes exactly the Transient units it deposited. Xfer and
Live are bits, changed by atomic masked operations preserving all other
fields. They are never decremented as anonymous counted units. The Xfer mutex
does not exclude concurrent Reference, Live, or Transient operations on Rt.

Taking the first Reference no longer clears any other field. Releasing the
last Reference no longer creates a pulse. Remove the corresponding Sub0
clear, substitute, and retract rules together; mixing them with this layout
would be incorrect.

## 4. Construction and Live lifecycle

Construct all roots and leaves as zero, then initialize epoch 0's metadata
and set its root to `LiveMask | XferMask`. Set:

```text
Fmembers = 0
Xsegment = 0
Xgen = 0
```

These initial values are published with the Farm. Initial Xfer construction
does not count as a runtime Xgen increment. Consuming it advances Xgen to 1.

An initializing producer raises Live for every newly initialized epoch before
release-publishing that epoch's `Es`. Its valid reservation supplies the
initialization authority. Preserve any provisional Transients already present
on the physical slot; do not replace the whole root with a raw store.

Live means the epoch is still open or has not yet been confirmed complete.
It does not mean merely that some currently published callback is unfinished.
It remains set when consumers abandon incomplete work, including when the
last visitor was not the last consumer of the Farm. Unsubscription therefore
does not need to manufacture incomplete-work protection.

A consumer may clear Live only while protected against reuse and after the
existing confirmation predicate holds:

```text
Es[slot] == expectedEpoch
Es[nextSlot] == expectedEpoch + 1
Seqt[slot] + Sd[slot] >= Seqt[nextSlot]
```

Use an idempotent atomic bit clear so multiple confirming consumers cannot
double-subtract. Each clearer must retain its own valid protection through
the operation; a delayed unpinned clearer could otherwise clear another
generation's Live bit. Clearing Live does not release References or Xfer.
Completion hooks still retain the finisher's ordinary reference until return.

The implementation must include confirmation checks on movement, prefix
release, idle scans, and departure, as appropriate. A segment wholly crossed
by a spanning table may have no table starts and no local table finisher.
Its trivially satisfied confirmation must still discharge Live. "Only the
last table finisher clears Live" is therefore insufficient.

This replaces the producer's conditional `plantIfUnprotected` obligation
with initialization-time Live. The reservation-to-initialization interval
still needs the quota/coverage proof: raising a bit later cannot revoke a
previously granted quota. Preserve producer publication ordering and the
`Wt` reservation chain.

## 5. Admission and promotion: proposed replacement for SPEC 4c

Invalid arguments or an already subscribed view are rejected before admission.
For a valid attempt:

1. Load Fmembers and decode Consumers. If Consumers is at least 128, return
   FULL without mutation. This is the initial policy gate, not a later commit
   condition.
2. Fetch-add one Subs unit. Use that RMW's returned packed snapshot to compute
   the aggregate immediately after this attempt's increment.
3. If the resulting aggregate exceeds 256, subtract this attempt's Subs unit
   and return FULL. Do not increment Consumers or touch Xfer.
4. Otherwise admission is durable. Promote by the single packed RMW described
   in section 2. A returned Consumers value of zero selects the Xfer path;
   all other returned values select ordinary attachment.

The initial policy check can be passed by a herd while Consumers is below
128. Those entrants may finish even if Consumers subsequently reaches 128
or more. Do not repeat that gate after admission.

Rejected announcements can temporarily inflate Subs above the aggregate
ceiling and cause conservative FULL results for other attempts. The ceiling
bounds accepted outstanding memberships; it does not bound the instantaneous
raw sum including rejected attempts that have not yet unwound. Field-width
bounds must include those attempts. No admission CAS loop is required.

After durable admission, subscribe must finish attachment, retrying transient
validation failures internally. After promotion, no recoverable failure may
undo Consumers. An admitted membership contribution persists until that
view completes subscription and later unsubscribes normally.

`subscribeOnce` needs a revised public contract: it cannot preserve the old
"return RETRY and unwind everything" behavior after this early promotion.
Choose an infallible admitted attach phase, or a separate resumable operation
that retains membership and its obligation. Do not leave the old rollback
path in place. This API choice is not yet selected.

## 6. Subscription: proposed replacement for SPEC 6b

### First-consumer handoff

The subscriber whose promotion observes Consumers 0→1 already owns a positive
Consumers contribution before trying the Xfer mutex. It cannot unsubscribe
until subscription finishes. Ordinary consumers' departures cannot exhaust
this outstanding contribution while it waits.

Under the mutex:

1. Read the current Xsegment.
2. If it is valid, verify the corresponding Xfer and acquire-validate that
   protected epoch's metadata. Establish the view using the normal leaf and
   root procedure, caching the actual leaf index.
3. Once replacement coverage is established, clear Xfer atomically, set
   Xsegment to 99, increment Xgen, and release the mutex. Initialize/publish
   the local view completely before returning success or permitting advance.

The direct target must be a valid starting point for all outstanding work;
the earliest-Live coverage obligation in section 9 still applies. A shared
leaf attachment must be audited even when it performs no root increment.

If Xsegment is 99, the departing owner may not yet have entered its critical
section. **Release the mutex before backing off or waiting**, then reacquire
and inspect the current state again. Holding it while waiting would prevent
the required publisher from making progress.

The proposed profile includes a sanity/retry budget followed by a fatal
diagnostic if Xfer never appears. Its value remains to be selected. This is
an operational progress watchdog, not a proof that a finite number of failed
polls implies corruption: a valid departing thread can be descheduled.
Any production timeout/fatal policy must make that scheduling consequence
explicit. A failed wait does not withdraw Consumers and return FULL/RETRY.

### Ordinary attachment

All other promoted subscribers search for the earliest relevant Live epoch,
depositing a Transient before inspecting each candidate slot's metadata.
They retain the required provisional protection until attachment has valid
replacement coverage, then remove exactly their own Transients.

The proposed direction is a bounded breadth-first candidate pass: inspect
the eligible physical slots, rank valid Live candidates by logical epoch,
then attach after consistency checks. "Earliest" means earliest `Es`, not
lowest physical index or first slot encountered. The exact scan order,
frontier pin, retry schedule, and candidate validation remain design work.

The existing subscribe walk supplies obligations, not a ready-made proof:

- Pin before metadata access. A Transient protects future qualifying sweeps;
  it does not cancel a producer's retained grant.
- Acquire-validate `Es` and its published metadata, including nonzero,
  bounded SqCs. Preserve the Seqt/epoch and Wt/lap consistency checks unless
  a replacement proves the same properties.
- Associate the observed Live bit with the validated generation. A Live bit
  alone is not an epoch tag, and it may clear during the scan.
- Account for initialization in flight, a Live candidate disappearing, and
  a temporarily unavailable candidate. Retry with the membership contribution
  retained; do not publish a synthetic guard on a failed candidate.
- Complete normal leaf/root establishment before removing the provisional
  coverage on which it relies.

The old frontier fallback cannot simply be copied after substituting Live
for `Rtlow`. Its eligibility and retained-quota proof must be restated for
the new candidate rule. Ordinary successful attachment takes no Xfer mutex.

## 7. Unsubscription: proposed replacement for SPEC 6e

Each view has a single owner; unsubscribe cannot race that view's unfinished
subscription or its own concurrent use. Confirm/release any eligible prefix
while retaining at least one valid root for the possible handoff.

Decrement Consumers with a full-word RMW. Check the returned masked Consumers
field for underflow. If the old value exceeds 1, this is an ordinary departure:
release the remaining references and return without taking the Xfer mutex.
Live continues to protect incomplete epochs.

If the old value is 1:

1. Retain valid reference protection. Sample Xgen atomically **after** the
   decrement and before acquiring the mutex; call the sample `g`.
2. Acquire the Xfer mutex and read the current Xgen.
3. If the generation differs from `g`, perform no Xfer mutation. The proof
   obligation is that the departure's handoff was already covered by the
   intervening transition, not merely that unrelated activity occurred.
4. If the generation is unchanged and Xsegment is already valid, preserve
   that Xfer. Coalescing makes no new Xgen increment.
5. If the generation is unchanged and Xsegment is 99, install Xfer on a
   valid retained segment, record its physical index, and increment Xgen.
   A candidate placement is the oldest still-held epoch after prefix cleanup;
   the coverage obligation in section 9 must establish its suitability.
6. Release the mutex, then finish ordinary reference cleanup. Never recreate
   Xfer as a side effect of those later root decrements.

Do **not** suppress publication merely because Consumers is now positive.
A first subscriber raises Consumers before taking the mutex and may be
waiting specifically for this publication. Xgen replaces that count recheck.

An unsubscribe-only herd has one Consumers 1→0 observer, hence one Xfer-mutex
entrant. Other departures can finish concurrently. Retiring views still
own resources after leaving Consumers; this affects representation bounds
and destruction even though it does not require a new admission field.

## 8. Generation ordering and required schedules

### Positive Consumers does not mean that handoff finished

```text
L: Consumers 1 -> 0; sample Xgen = g; retain its root
S: promote Consumers 0 -> 1; must acquire Xfer
L: acquire mutex; Consumers is positive, Xgen is still g
```

L must publish Xfer if none exists. Skipping because of the positive count
leaves S waiting for a publisher that has returned. S's early increment
records an obligation to attach; it does not certify attachment.

### A late zero observation must not recreate a consumed handoff

An old departure may enter the mutex after handoff state has advanced. The
generation check is intended to invalidate its old publication obligation,
while Xsegment coalesces requests arriving before an outstanding Xfer is
consumed. Both rules need the same definition of which transition covers a
departure. Xsegment alone loses that history when it returns to 99.

### The snapshot belongs after the membership decrement

Moving the Xgen sample before the decrement is not an equivalent optimization:
a subscriber can finish an earlier handoff between that sample and this
departure's eventual Consumers 1→0. Comparing the old sample would then skip
a newly necessary publication.

For the post-decrement sample, the memory-order proof must ensure it imports
the preceding cohort's completed handoff. A first subscriber advances Xgen
before it may later release its own Consumers contribution. The release/
acquire chain through Fmembers decrements can carry that history to the last
leaver and its subsequent Xgen load. A raw, unconnected Xgen observation is
not a substitute for this argument.

The generation mismatch branch still needs a complete coverage proof with
the final attachment and retirement protocol. In the bounded model below,
it is never reached: the durable first-subscriber contribution prevents the
next zero transition until that subscriber has consumed an Xfer. The model
therefore provides no evidence for accepting arbitrary stale tickets merely
because their generation differs.

## 9. Coverage and progress obligations before adoption

The following are requirements on the completed design, not claims already
proved by adding fields:

- **Earliest work remains reachable.** Ordinary Live discovery and direct
  Xsegment attachment must agree. Either prove that the chosen Xfer anchor
  cannot strand earlier Live work outside the new view's forward range, or
  add a protected search/coverage step. Xfer's existence proves protection
  of its own slot, not automatically the correct traversal starting point.
- **Every Live bit has a discharge path.** Include trivially complete crossed
  epochs and epochs whose accounting finishes before a later observer checks
  closure. No consumer may permanently leave confirmed Live behind all views
  with no actor obliged to revisit it.
- **Initialization and retained grants remain sound.** Live publication follows
  a valid reservation and precedes Es publication. A stale Transient may land
  on already certified space; only validation can authorize attachment.
- **Tree propagation retains coverage.** Exercise occupied-leaf joins while
  a zero-edge root update is pending, and delayed root decrements followed by
  new leaf occupancy. Preserve the release chains and increment-ahead rule.
- **Retiring resources are bounded.** Consumers no longer counts a departing
  view's retained references. Delayed edge propagation can also leave extra
  root contributions. Neither the 256 admission ceiling nor 12 physical leaves
  alone bounds every transient root value. Establish bounds for outstanding
  retirements and operations, as well as admitted probes, before assigning
  field-width safety claims.
- **Destruction is externally exclusive.** Fmembers zero alone does not show
  that last-leaver cleanup, producers, or outstanding operations have returned.

## 10. Memory-order draft and producer compatibility

A conservative starting profile uses acquire-release RMWs for Fmembers
announcement, rollback of Subs, promotion, and departure; acquire loads for
admission observations and Xgen samples; and release increments of Xgen under
the mutex. The mutex provides acquire/release synchronization between its
owners. Any weakening requires a stated happens-before argument.

Keep acq_rel leaf RMWs and the root release sequence carrying consumer accesses
to producer acquire sweeps. Use atomic root-bit operations with the ordering
needed to publish their protection and completion history. Preserve the Es
and table-sentinel publication edges, completion joins, and the acq_rel Wt
reservation chain that carries prior reclamation to producers spending
retained quota. The handoff mutex does not synchronize ordinary producers or
consumers that never acquire it.

The producer's qualifying empty-root predicate remains `Rt == 0`. Any Live,
Xfer, Reference, or Transient blocks a new sweep through that root. Existing
quota cannot be retroactively canceled by a later Transient.

Revise the fresh-segment diagnostic to distinguish provisional Transients
from forbidden protection belonging to an older generation. Perform the
check before raising this reservation's new Live bit. Do not weaken it to
ignore old Live, Xfer, or References merely because the new protocol changes
the layout. Keep the timing and generation scope of the diagnostic explicit.

Every former Cf reader must decode Consumers. Passing the whole Fmembers
word to SqCs sizing, feedback, or a consumer-count hint would treat Subs as
billions of consumers. Audit producer sizing, `canFit`, `rootCostHintFor`,
actor call sites, benchmarks, diagnostics, destruction, and tests.

## 11. Evidence and validation plan

The companion [handoff model](xfer-handoff-model.py) explores three actors,
each subscribing and unsubscribing twice, with announcement, promotion,
mutex acquisition, generation sampling, waiting, and departure as separate
steps. It begins with construction's Xfer. Attachment and each locked
Xfer/Xgen mutation are abstract steps. Waiting releases the mutex.

```sh
python3 docs/development/xfer-handoff-model.py consumers
python3 docs/development/xfer-handoff-model.py xgen
```

The `consumers` variant intentionally retains the positive-Consumers
publication veto and is expected to find a blocked handoff. The `xgen`
variant follows the proposed generation check and is expected to complete
exploration. Each command exits nonzero if that expected outcome changes.

Observed results for this draft:

| Variant | Result |
| --- | --- |
| Consumers recheck | blocked first subscriber found after discovering 19,396 states |
| Xgen check | 28,693 states explored; no blocked terminal state |
| Xgen mismatch branch | never exercised in that exploration |

This is a small sequentially consistent membership model. It does not model
roots, table accounting, producers, retained grants, capacity rejection,
hardware reordering, or the proposed finite retry watchdog. It is not a proof
of the replacement Farm algorithm. Add those mechanisms and their actual
atomic boundaries before drawing broader conclusions.

Required tests for the completed implementation include:

- construction, a first subscriber beating its publisher to the mutex, and
  the positive-Consumers schedule above;
- delayed generation sampling, delayed lock acquisition, repeated zero
  transitions, and publication/consumption before an older operation resumes;
- 127/128 admission gating and 255/256/257 aggregate boundaries, including
  rejected Subs that remain temporarily announced;
- a pending herd admitted below 128 and promoted above it without rollback;
- stale tail probes and retained grants, metadata initialization in flight,
  earliest-Live selection, and logical-epoch wrap over physical slots;
- non-last departure abandoning incomplete work, trivially complete interiors,
  Live confirmation races, shared-leaf attachment, and retiring-root bounds;
- producer reuse after final releases, callback exactly-once accounting,
  completion-hook lifetime, and balanced teardown.

The existing [ordering schedules](../../tests/farm/farm_ordering.d),
[write-protection tests](../../tests/farm/write_protection.d),
[leaf-reuse test](../../tests/farm/leaf_reuse.d), and
[torture tests](../../tests/farm/torture_tests.d) provide regression oracles.
Rewrite Sub0-specific assertions around equivalent protection and progress
requirements. Do not discard an oracle simply because its old representation
has disappeared. Synchronous hooks must not recurse through a held mutex.

Population and boundary checks through 256 views can run on smaller machines.
Oversubscribed OS threads and controlled interleavings supplement them, but
they do not validate 256 operators executing concurrently on hardware thread
contexts. Keep that distinction explicit. Measure the intended 16–32 consumer
cadence, unsubscribe herds, retries, and producer throughput under churn when
an implementation exists.

## 12. SPEC and implementation change map

| Area | Coordinated revision |
| --- | --- |
| SPEC 1, 3 | replace Sub0-based coverage and progress arguments with Live/Xfer obligations |
| SPEC 2a, 4c | Fmembers names, field decoding, admission and irreversible promotion |
| SPEC 2b, 6c | root layout, plain Reference edges, separate Live and Xfer transitions |
| SPEC 4b | epoch-0 Live/Xfer, Xsegment 0, Xgen 0 |
| SPEC 5, 8 | producer Live initialization, confirmation clear paths, reuse diagnostics |
| SPEC 6b, 6d | first handoff, ordinary Live search, retained ranges and idle progress |
| SPEC 6e | ordinary departures without the mutex; generation-qualified last departures |
| SPEC 7, 9 | count-dependent heuristics and explicit synchronization chains |
| SPEC 10, 11 | subscribeOnce contract, support limits, diagnostics, and revised rationale |

In source, this touches `tryCreate`, `add_consumer`/`sub_consumer`,
`subscribe`/`subscribeOnce`, `unsubscribe`, `incLeaf`, `takeRootCount`,
`decRef`, `releaseRootLeavePulse`, `plantIfUnprotected`, `confirmed`,
prefix/idle helpers, producer initialization and sweep checks, and every
consumer-count reader. The old pulse helpers should be replaced as one
lifecycle change rather than retained alongside the new bit operations.

Before promoting this draft to the canonical SPEC, settle the ordinary scan
and earliest-work proof, generation-mismatch coverage, complete Live-clear
responsibility, resource bounds including retirement, the admitted API retry
contract, and the watchdog's operational policy. Then validate the complete
logical protocol, its memory ordering, and the implementation together.

## 13. Live-only review

The review's central simplification is credible: Live persists from epoch
initialization through confirmation, independently of subscribers arriving
or consumers departing. This removes the reason to manufacture protection
at the last departure. With a sound attachment scan, Xfer need not supply
either protection or discovery. The corresponding Xsegment, mutex, Xgen,
irreversible early promotion, first-subscriber wait, and watchdog can then
be removed. Packed membership and a higher admission ceiling become separate
policy choices, rather than prerequisites of this lifetime protocol.

That is a direction supported by the review, not a finding that its supplied
prototype is already correct. Two additional deterministic schedules below
strand work on that prototype, including with its oldest-first scan. Both
are discovery/progress failures: Live preserves the skipped work, and a new
subscriber can recover it. Neither demonstrates a need to restore Xfer.

### Protection argument

The useful replacement invariant is that the earliest unconfirmed epoch is
initialized and Live, including on an empty Farm. Confirming epoch `e`
requires publication of `Es[e+1]`; publication of that successor follows
raising its Live. Thus removing Live from `e` requires an initialized
successor. Epoch 0 supplies the base case. Live cannot be removed from the
earliest unconfirmed epoch merely because consumers leave.

The review also supplies the missing direction for section 4's
initialization-gap argument:

```text
raise Live(e+1)
  -> release Es(e+1)
  -> confirming holder's acquire Es(e+1)
  -> release clear Live(e)
  -> acquire zero Rt(e), directly or through the Wt reservation chain
  -> subsequent sweep read of Rt(e+1)
```

The sweep must acquire the predecessor's reclamation history even when the
target slot is first in that sweep; SPEC 9's reservation chain supplies that
case. Induction over slot generations is essential: an arbitrary zero from
construction is not evidence of discharge for a later lap. Subject to that
induction and the retained-quota bound, a sweep cannot certify the next lap
of an epoch whose Live has not yet been raised. This argument depends on
producer sweep geometry and publication ordering, but not on a last-member
decision or on a consumer's relative position.

Live does not replace Transients or ordinary References. A subscriber still
needs pin-before-read, metadata and lap validation, and protected leaf
attachment. A clearer still needs its own reference until after clearing
Live, including around delayed completion hooks.

### Counterexample: the initial scan window can expire

The prototype derives an ascending logical window from the initial `Wt`
sample, but accepts a candidate whose epoch lies beyond that window. Its
final `Wt < Es + K` check only rejects a candidate that is too old; it does
not reject a candidate that is too new for the scan order.

First reproduction, with `K = 4`:

1. S reads `Wt` in epoch 0, then pauses before its first Transient. Its
   computed scan has one slot, physical slot 0.
2. C attaches, completes epoch 0, and moves to epoch 1. Producers advance
   into epoch 4, reusing slot 0. Epochs 1–3 retain Live and unfinished work.
3. C leaves. S resumes, reads Live and `Es = 4` at slot 0, and attaches there.
   The existing metadata/lap checks accept this.
4. Only 3 of the 9 published callbacks have run, all from C's earlier work,
   and producers stall. A fresh subscriber attaches at epoch 1 and
   completes all 9.

This is not confined to the shortened construction-time scan. A second
reproduction scans all four slots, combining a stale initial tail with the
reviewer's out-of-order initialization schedule:

1. S samples `Wt` in epoch 3: its scan order is physical slots `0, 1, 2, 3`.
   C holds epoch 3, and epochs 0–2 have been discharged.
2. While S is paused, P reserves across 3 into 4 but pauses before
   initializing 4. Q reserves later, writes tables starting in epoch 4,
   and initializes epoch 5 first.
3. S reads slot 0 before Live(4) exists, then slot 1 with Live(5).
4. Before S reads slot 3, P finishes initialization/publication, C executes
   P's table, clears Live(3), moves to 4, and leaves. Live(4) remains.
5. S finds no other candidate and attaches at epoch 5. Only 15 of 18 tables
   execute, and the producer stalls with S idle at the tail. A fresh
   subscriber attaches at epoch 4 and completes all 18.

Oldest-first in an expired logical window is not oldest-first among the
epochs now occupying those physical slots. The review's fixed-window model
does not model this initial-tail delay or rotation.

### Candidate-window validation

A scratch correction adds one bound to the prototype's final validation:

```text
eg = initial_Wt >> segShift
if candidate_Es > eg:
    unwind Transients and membership; return RETRY
```

Keep the existing metadata and final-tail checks. Together they constrain
accepted candidates to the original logical window: final Wt cannot go
backwards from the initial sample, so `final_Wt_epoch < Es + K` supplies the
lower bound, and `Es <= eg` supplies the upper bound. The construction-time
lower bound is epoch 0. No new CAS or mutex is needed.

Both reproduced failures return RETRY with this guard and then attach before
the backlog on a fresh attempt. This fixes those schedules; it is not by
itself an exhaustive proof of the scan.

A proof must cover the entire accepted scan, including weak-memory
observations. One promising induction starts at the window's oldest epoch:
construction publishes Live(0), or the initial Wt acquire imports the
reclamation history needed to enter its sampled frontier. Skipping a cleared
Live must import the successor's initialization before the next root read.
An accepted candidate must belong to the logical window that this induction
traverses. Explicitly discharge the mixed-generation cases and the possibility
that a candidate confirms after it was observed but while its Transient is
still held. Do not substitute "an acquire read sees the latest value" for
these ordering arguments.

The review's memory-order requirements are useful here: release RMW for Live
raise, release RMW for Live clear after confirmation's acquire, acquire root
reads during scanning/sweeping, and all post-construction root writes as RMWs.
In particular, acquiring a new lap's Live must import its reservation so the
final Wt check rejects an old Es observed before new Es publication. A relaxed
Live raise would not supply that edge merely because Es is published later.

### Handoff-state reduction

The absence of Xgen-mismatch and existing-Xfer branches in the bounded model
has a structural explanation under its durable-membership rules. A designated
first subscriber keeps a Consumers contribution until it has consumed Xfer.
Until then, ordinary arrivals and departures cannot produce another 1-to-0
transition. Its preceding last departure must publish before that first
subscriber can finish attaching and eventually leave. By induction there is
at most one unresolved last-departure publication obligation, and publication
and consumption alternate. Delaying cleanup after publication does not leave
an unresolved publication obligation.

Within those assumptions, checking positive Consumers was the wrong veto;
Xgen was not needed to replace it. This explanation does not cover adding
cancellation or rollback of the designated subscriber's contribution. The
Live-only alternative removes this whole election, so ordinary recoverable
subscribeOnce retries can instead retain their existing meaning.

### Evidence and limits of this follow-up

Inspected the supplied prototype at
`/tmp/antfarm-scan-fmx-8f973896/vL/antfarm/package.d` and its
`work/live-prototype.diff`. Additional scratch artifacts are in
`/tmp/antfarm-live-review-Y66cZG/`:

- `stale_probe.d`: delayed epoch-0 tail probe;
- `stale_rotation.d`: full-width stale-window variant of the review's schedule
  E, using trigger 2;
- `guarded/antfarm/package.d`: the supplied prototype plus the candidate upper
  bound, used only for this experiment.

| Check | Supplied oldest-first prototype | With candidate upper bound |
| --- | --- | --- |
| Delayed epoch-0 probe, LDC and DMD | attaches at 4; 3/9 callbacks, rescue at 1 | RETRY, then 62/62 callbacks during continued producer/consumer rounds |
| Full-width stale window, LDC and DMD | attaches at 5; 15/18 tables, rescue at 4 | RETRY, attaches at 4, then 66/66 tables |
| Original schedule E, triggers 0–3, LDC | supplied review reported pass | all four pass |

Re-ran the review's SC scan model: newest-first with out-of-order
initialization strands work after exploring 1,333 states; oldest-first
explores 769 states without a failing terminal state. The ordered-initialization
variants explore 877 and 661 states respectively without failure. These are
fixed-scenario results, not ring-wrap or weak-memory coverage.

Re-ran the instrumented three-actor, two-round Xfer model: 28,693 states,
zero generation mismatches, zero coalesces, and 1,704 first-subscriber wait
transitions. The review's larger four-actor run and its broad regression suite
were not repeated. No TSan, full unit suite, hardware-concurrency validation,
or performance measurements were added here. Canonical SPEC and production
source remain unchanged.

## 14. Newest pin and unpinned hints review

This follow-up examines `vN/antfarm/package.d` under
`/tmp/antfarm-scan-fmx-8f973896/`, SHA-256
`4f3a6aacc6d3bd82d11d6d796d15dbe50c808a1faac089cb946886818451f526`.
It pins the sampled newest slot, searches for a candidate with unpinned
loads, and pins and rechecks that candidate before attachment. Internal
rounds retain every pin already acquired.

The section 13 failure is addressed: candidate selection uses an explicit
logical epoch `x` within the current round's window, `classify(x)` requires
matching Es, and final validation requires `Es == candidate`. A newer lap
cannot silently become an accepted candidate in an expired window.

The direction remains attractive, especially oldest-first hints. No new
concrete protocol failure was found in the focused checks below. Remaining
concerns fall into proof obligations and limitations of the evidence.

### Discovery and lifetime ordering

"Plain loads" must mean **atomic acquire loads without pin RMWs** here, as
the Es-first implementation actually uses. It must not become ordinary
unsynchronized loads or relaxed loads by an editing shorthand.

The candidate's own pin, Live recheck, Es identity, and final lap validation
address lifetime protection. They do not alone establish that no unfinished
epoch was skipped. The proof must cover each classification used to continue
or stop the scan:

- Matching Es followed by a clear Live imports initialization and then must
  identify a later discharge, rather than a pre-initialization zero.
- An older Es is an initialization gap, not evidence that the expected epoch
  has no work. Explain why continuing past it cannot accept above the earliest
  unfinished epoch; the cross-slot initialization/discharge induction matters.
- A newer Es must carry the reclamation history that justifies passing the
  old epoch and rejecting mixed-generation candidates.
- A candidate that confirms between the hint and the pin/recheck must either
  be rejected or remain safely covered through successful attachment.

For oldest-first, the initial Wt window and the forward succession of acquired
discharges offer the same induction route described in section 13. Spell it
out for unpinned observations and actual atomic orders. SC/x86 stress is not
that proof. Keep the direct candidate-Live release/acquire edge that imports
the initializing reservation when Es still names the previous lap.

### Newest-down adds a global discharge invariant

Stopping at the first discharged epoch requires that discharge cannot leave
older Live work behind. Clearing only a consumer's confirmed prefix is a
local rule; prove that successful attachments and all subsequent clear paths
preserve the required global rule. Include prefix release, departure, empty
interiors of spanning tables, out-of-order initialization, and concurrent
consumers with different held ranges. Initialization gaps mean that literal
contiguity of all Live bits is not itself the invariant.

The proposed extra pass below an attached candidate is not yet a recovery
protocol. Detecting an older Live and returning RETRY can cause every later
backward attempt to select above the same hole and reject again. Recovery
needs a validated path to the older work, such as an oldest-first fallback.
Keep any such check before exposing the view or running callbacks. If it
rejects, release tentative references without using a departure path that
can clear Live above the unresolved older work.

Oldest-first hints avoid the backward stop rule's extra invariant and recovery
dependency. There is not yet sufficient performance evidence to justify
choosing the more complicated rule.

### Two pins and bounded retries are not established guarantees

The code's `held` bitmap survives internal rounds. Failed candidate pins and
pins of successive sampled frontiers accumulate until success or RETRY, up
to K physical slots. Two pins describes a common successful case, not the
algorithm's maximum.

The supplied slow-scan log already reports higher averages at K=16: 2.58 for
oldest-first hints, 5.23 for newest-down hints, and 3.27 for newest-down with
the oldest-slot peek. Those numbers are in `work/out_M_stress.txt`.

A pin at a sampled tail can be stale before it lands. The proof of any round
bound must cover that interval, retained quota, pins accumulated on prior
rounds, and repeated classifications. The implementation currently attempts
`K + 2` rounds and then returns RETRY. Keep that recoverable outcome; observing
zero RETRY in a finite workload does not establish guaranteed success or
justify a fatal watchdog.

The diagnostic maximum is also a load/check/store of `liveMaxRounds`, not
an atomic maximum: a delayed smaller update can overwrite a larger one.
Use per-thread maxima or a correct aggregation before treating that number
as the largest round count observed by the workload.

### Measurement and completion-oracle limitations

Each measured pin executes `atomicFetchAdd(livePins, 1)` on a shared global
counter, in addition to the real Rt update. This creates extra contention
proportional to pin count, directly in the path whose cost is being compared.
Remeasure latency with instrumentation disabled; use per-thread counters
outside the timed path for separate counts. The current timings cannot
isolate the production cost of pinning. The supplied harness also does not
pin its consumer cohort to a single L3 group.

Both `herd_wrap.d` and `stress_wrap6.d` create a fresh final subscriber and
drain before comparing `g_calls` with `g_written`. That subscriber can rescue
work an earlier attachment stranded, as section 13 demonstrated. The stress
harness additionally has two steady consumers that can drain work missed by
churners. These are useful throughput workloads, but their final success does
not prove that each attachment preserves progress without a new subscriber.

The aggregate equality also does not establish per-payload exactly-once:
one missing execution and one duplicate can cancel. Strengthen correctness
oracles with individual payload identities and a progress phase using an
already-attached survivor, checking for stalled outstanding work before any
fresh subscription. Existing deterministic schedules do check for a stall
before their rescue attempt; retain that distinction.

### Focused checks in this follow-up

Copied the exact source above into `/tmp/antfarm-live-hint-review-TSNHnj/` and
built with LDC `-g -O1`. Both configurations enabled
`AntfarmWriteAuditHooks`, `LiveReadEsFirst`, and `LivePrefixDischarge`:

- Oldest-first hints: `LiveScanHint`.
- Newest-down hints with peek: `LiveScanBwd`, `LiveBwdHint`, `LiveScanPeek`.

Both pass the earlier delayed epoch-0 probe, schedules F and G from
`subpin_wrap6.d`, H, I, and all four triggers of E from `subpin_strand6.d`:
18 focused runs in total. Source copies, binaries, and run logs are in that
scratch directory. No new failing schedule was found; this does not close
the ordering or global-discharge proofs. No full unit suite, TSan lane, new
performance measurement, or 16–32-thread herd validation was performed in
this follow-up. Canonical SPEC and production source remain unchanged.
