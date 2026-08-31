# Suggested edits for `SPEC.md`

This is an editorial companion to the living Farm specification. It does not
change the contract by itself. The suggestions below were checked against the
current `antfarm.d`; they are intended to make a later rewrite easier without
losing the concurrency arguments that accumulated while bugs were being fixed.

The most useful overall change would be to separate four kinds of statement
that are currently interleaved:

1. normative safety and API requirements;
2. the current scheduling policy;
3. implementation constants and tuning heuristics; and
4. history or rationale for rejected designs.

The first two belong in the main specification. The third can live in an
"implementation profile" section, and the fourth in short rationale notes or a
separate design-history appendix. That split would remove much of the apparent
redundancy without discarding why the current protocol exists.

## Highest-priority semantic edits

These are more than copy edits. They clarify or correct the current contract.

### 1. Give "completion" and "reclamation" separate names

`SPEC.md` presently uses *complete* for several different states. Define the
states once and use the qualified terms throughout:

| Proposed term | Exact condition | What it does and does not establish |
| --- | --- | --- |
| shard primary-complete | the low half of that shard's `Tcount` has accumulated `Shlen` | Every primary Tindex run in the work shard returned and was accounted. |
| table primary-complete, or table accounted | `Tprogress == Tlen` | Every work shard is primary-complete; the table size may be added to `Sd`. This is not a join on every concurrent MT callback. |
| MT calls issued | `pcount.calls >= done` | Every MT iteration has been claimed for execution. Some callbacks can still be in flight. |
| MT callbacks returned | `pcount.completions == done` | Every issued MT callback has returned. The ST fast path deliberately does not maintain these two fields. |
| segment confirmed | matching `Es` values and `Seqt + Sd >= SeqtNext` | No more primary table accounting is missing from the segment. Confirmation alone does not make the bytes reusable. |
| slot admitted by a quota sweep | a producer observes `Rt == 0` for that following slot while scanning contiguously from its tail anchor | No consumer, subscriber, or Sub0 pin blocks the sweep there. In this reuse context, the pulse and contiguous-sweep invariants make zero sufficient; the producer does not recheck `Sd`. A current write-frontier epoch may be incomplete with `Rt == 0` because it is not a following slot being reclaimed, so `Rt == 0` is not globally synonymous with confirmation. |

This vocabulary resolves the apparent contradiction around MT payload lifetime.
It also makes the `Sd` proof easier to state: `Sd` accounts primary-complete
tables, while `Rt` prevents reuse until all synchronous callback users have
left.

### 2. Replace "MT payloads can outlive their shards' completion"

The current phrase occurs in the primary-path skip, the sweeper section, and the
implementation comments. It sounds as though a payload or its body is detached
from the ring shard and allowed to survive reclamation. That is not the design.

The user's intuition is correct for a primary visit: a primary claimant does not
add its run length to `Tcount` until its own `enterPayload(..., false)` calls
have returned. However, that does not join MT work entered concurrently through
the secondary or tertiary paths. A more exact sequence is:

```text
consumer A (primary owner)              consumer B (secondary/tertiary)
-----------------------------------     -----------------------------------
validate Tsent and retain a Farm pin    validate Tsent and retain a Farm pin
claim the payload's Tindex run          finish/exhaust another primary shard
enter one MT iteration                  take a Pcount admission on this payload
callback returns                        execute one or more MT iterations
increment Pcount.completions                         ... callback in flight ...
add run length to Tcount
possibly make Tprogress reach Tlen
possibly add Tsize to Sd
                                        callback returns
                                        increment Pcount.completions
                                        only later move/unsubscribe and drop pin
producer may reuse only after the relevant Rt is observed as zero
```

Both consumers execute the callback synchronously while their `ConsumerView`
still protects the table. A table that spans physical segments is also safe: a
pin on its starting epoch protects the table's forward extent under the
strictly-less-than-one-lap quota invariant. The ring body therefore never
"lives beyond" its protection.

Suggested replacement text for the three existing occurrences:

> `Tprogress == Tlen` means all primary Tindex runs are accounted. An MT
> callback entered concurrently by the secondary or tertiary path may still be
> in flight, or an MT iteration may still need a draining entrance. Every such
> callback runs synchronously under its caller's retained Farm pin, so table
> accounting can precede callback return without permitting ring reuse. The
> complete-table MT pass attempts still-unissued work when an admission appears
> available; it is not a storage lifetime mechanism.

The terse fast-path wording can then say "skip primary claiming and run the
pre-checked MT drain pass" without making a lifetime claim.

### 3. Correct the `subscribe()` success range

Section 5a currently says success returns a *positive* epoch. Epoch 0 is a valid
success and the tests require `subscribe(f) == 0` on a fresh Farm. Change this
to:

> Returns the non-negative starting epoch on success, or a negative value on
> failure.

### 4. Make the MT classifications match the accepted headers

The current prose tends to equate all of these:

- single-threaded;
- `done == 1`;
- `maxCs == 1`; and
- absence from the MT index.

The implementation has three distinct cases:

- the ST fast path is exactly `maxCs == 1 && done == 1`;
- a payload is copied into the MT index when `maxCs > 1`; and
- `done > 1` requires `maxCs >= 2`.

`maxCs > 1 && done == 1` is currently accepted and MT-indexed, even though only
one call can win. Either document that as legal or reject it in
`validatePayloadHeader`; do not imply that the implementation already rejects
it.

### 5. Weaken the claim-slot statement to the raced guarantee

Sections 4a and 5i say that, because the primary visit burns one claim and MT
payloads have `maxCs >= 2`, "a sweeper always finds a claim slot." A particular
sweeper is not guaranteed to win that slot: a secondary visitor or another
sweeper may have taken it, and the primary visit itself may arrive after a
draining visitor.

The useful guarantee is:

> A multi-iteration payload has at least two admission slots and exactly one
> primary Tindex attempt. Secondary and tertiary attempts use the draining loop.
> If the primary attempt wins one slot, a draining attempt can win another; if
> draining attempts arrive first, one of them already owns the unfinished
> iterations and the later primary attempt may be rejected. A particular
> sweeper may lose the race because another draining entrant already owns or
> exhausted the remaining work.

This preserves the liveness argument without promising that every sweeper can
enter.

### 6. Describe the auto small-table threshold as a bounded heuristic

Section 5e-d says the auto rule
`clamp(SqCs * Chunk, 16, 256)` means a table shards *iff* every shard can receive
one full chunk. The clamps make that equivalence false at both ends. For example,
when `SqCs * Chunk > 256`, a 256-entry table can shard without providing a full
chunk to every shard.

Suggested wording:

> Zero selects the bounded heuristic `clamp(SqCs * Chunk, 16, 256)`. Below the
> threshold all work is assigned to shard 0; at or above it the table uses all
> `SqCs` work shards. Away from the clamp limits, the heuristic begins sharding
> when there is roughly one full chunk per shard.

If the exact iff property is desired instead, remove the clamps in code rather
than retaining the present prose.

### 7. Remove the stale role-parenthetical in 5e-m

The final sentence after the first-claimant yield says the role invariant allows
"a shard completer (or a claimant observing the concurrency signal)" to advance
the sweeper role. In the current production path, observing the shared-shard
signal is what permits the first claimant to yield; it does not grant the
sweeper role. `sweeperNext` is advanced by a shard-completing result. Delete the
parenthetical unless another current path is meant.

### 8. Resolve the hard-invariant/eventual-cleanup tension around Sub0

The overview and most of 2a state a hard invariant: confirmed segments do not
retain Sub0. The x86-specific paragraph later calls a never-entered case
"residual" and "self-healing on the next 0->1." Those describe different
contracts: immediate absence versus an eventual capacity repair.

Do not paper this over during a prose pass. Choose and state one of:

1. the post-CAS recheck is sufficient on every supported target, so a confirmed
   non-empty-farm segment cannot retain Sub0; or
2. a confirmed segment may transiently retain Sub0 until the next 0->1 edge, in
   which case the overview and reclamation invariant must admit that temporary
   capacity loss.

The present regression tests express the first intent. If it remains the
contract, move the actual memory-order proof into the memory-order section and
remove the suggestion that the residual case is accepted behavior.

## Proposed document order

The current order makes the reader learn fixes before learning the objects they
fix. In particular, the overview introduces Sub0, confirmation, quota, and
retained epochs before their definitions; 2a mixes field layout with most of the
unsubscribe algorithm; the table layout precedes the payload layout; and the
consumption sequence skips section 5d.

A cleaner order would be:

1. **Model, scope, and terminology**
   - unordered M:N work distribution;
   - magic-ring geometry (`Ln`, `K`, `Seq`, `Ei`, `Ki`);
   - producer, consumer, physical segment, logical epoch, work shard, and leaf;
   - the qualified completion states above.
2. **Data layout**
   - Farm-level fields;
   - per-segment root/leaf tallies and statistics;
   - table header, indexes, progress counters, and padding;
   - payload header/body and packed `pcount`.
3. **Safety invariants**
   - quota is strictly less than one lap;
   - consumer pins form a contiguous epoch range and move increment-ahead;
   - incomplete unowned segments retain a pulse;
   - raw ring reads occur only after publication and while pinned;
   - producer reuse requires a contiguous sweep observing `Rt == 0`.
4. **Construction and registration**
   - validated geometry and implementation limits;
   - epoch-0 pulse;
   - consumer capacity and producer tickets.
5. **Producer protocol**
   - quota acquisition/renewal;
   - sizing and reservation;
   - crossed-segment initialization;
   - payload/table emission;
   - sentinel publication and return semantics.
6. **Consumer pin lifecycle**
   - subscribe and pin-before-read;
   - orientation and frontier migration;
   - confirmed-prefix release;
   - unsubscribe and Sub0 deposition.
7. **Work scheduling**
   - primary Tindex chunks;
   - secondary MT assignment;
   - tertiary sweep and carried role;
   - oversaturation/undersaturation feedback as heuristics.
8. **Table accounting and segment reclamation**
   - `Tcount`, `Tprogress`, and `Sd` transitions;
   - the distinction between accounting and MT callback return;
   - confirmation predicate;
   - idle re-walk and liveness;
   - why `Rt == 0`, not `Sd`, is the reuse gate.
9. **Memory-order contract**
   - one table of atomic operations and orders;
   - publication and reclamation handoff edges;
   - raw atomic ring-word rule;
   - platform assumptions, if any.
10. **API, errors, and implementation profile**
    - success/backpressure/failure table;
    - fatal caller-contract violations;
    - current constants, defaults, and tunables.
11. **Rationale/history appendix** (optional)
    - rejected `Sbal` design;
    - why strong CAS is currently used;
    - why tickets are cache-line-strided;
    - characterized rather than required values.

This order also provides a natural 5d-equivalent: table/payload interpretation
is defined before either producer emission or consumer claiming. If retaining
the current broad structure, at minimum rename 5c to 5c, insert **5d. Table and
payload interpretation**, and move the current Thead/Payload layout there.

## Redundancy consolidation map

For each concept below, keep one full explanation at the proposed canonical
location. Elsewhere use one sentence plus a section link.

| Concept repeated today | Canonical home | Places to reduce to a cross-reference |
| --- | --- | --- |
| Less-than-one-lap quota safety | Safety invariants, with equations in producer quota | Overview safety paragraph; 3a opening; create constraints; fullness escape-hatch prose. |
| Root/leaf tally propagation | Segment layout plus consumer pin lifecycle | Overview memory-order paragraph; several unsubscribe bullets; retained-reference proof. |
| Sub0 deposition and retraction | Consumer pin lifecycle, one state machine | Overview; most of 2a; epoch 0; subscribe attach; unsubscribe; 5j retained-reference paragraph. |
| `IDc` changes do not retarget existing leaves | Consumer pin lifecycle | 2a cache note; ConsumerView field list; orientation; 5h; 5i. |
| `Es` and `Tsent` release-last publication | Memory-order section | Overview order paragraph; segment statistics; producer crossing; table write; consumer orientation. |
| Farm counter purposes (`Cf`, `Reqs_c`, `Reqs_p`) | Farm-level layout | Producer registration's repeated explanation of why request counters are separate. |
| Producer ticket authenticity and quota ledger | Registration/API contract | Producer tiers and the long `Token` argument description. Keep transfer semantics once. |
| Table anatomy | Data layout | 4a table table and 5c Thead field list. |
| Payload anatomy and header limits | Data layout/API contract | 4a validation and 5f's repeated definitions. |
| Primary visit consumes one Pcount claim | Work scheduling, directly before drain guarantee | 4a validation; note after 5e-m; sweeper bullets and complete-table paragraph. |
| `Tprogress` finisher adds `Tsize` to `Sd` | Accounting/reclamation | Table-layout row; primary step k; 5j opening. |
| Idle re-walk as liveness backstop | Accounting/reclamation | Mid-tick yield; carried sweeper; orientation; final deadlock paragraph. |
| Producer may subscribe and drain on backpressure | API/backpressure | 3a sweep failure; end of 4a; final sentence of 5j. |
| Counter wrap is fatal | Error-contract table, with field widths | Table-layout row, payload-layout row, and primary claim steps. Local steps only need "apply the packed-counter guard." |

The Sub0 material is the largest win. Its current explanation has five layers:
the metaphor, the safety paragraph, the bit layout, the last-releaser race, and
the subscribe/unsubscribe consequences. Retain all five ideas, but express the
mechanism once as a transition table:

| Event | Preconditions | `Rt` transition | Purpose |
| --- | --- | --- | --- |
| first real root | count 0, optional Sub0 | increment count and remove the observed Sub0 units | replace the pulse with ownership |
| non-final root release | count greater than 1 | decrement count | ordinary release |
| final root, incomplete segment | count 1 | atomically replace count with one Sub0 | preserve backlog discoverability and prevent reuse |
| final root, confirmed non-empty-farm segment | count 1 | decrement to zero | admit producer reclamation |
| final root of an empty Farm | count 1 | atomically replace count with one Sub0 | retain a subscribe anchor |
| producer/unsubscribe orphan scan | incomplete, count 0, no pulse | CAS in one Sub0, then recheck confirmation | cover a segment no consumer entered or retained |
| racing confirmation after orphan plant | the planted low half is still exactly one Sub0 | CAS out that unit | avoid a confirmed-segment capacity leak |

After this table, the race proof can be one focused paragraph. There is no need
to restate the complete lifecycle in subscribe, unsubscribe, and 5j.

## Section-by-section edit suggestions

The anchors below quote phrases rather than line numbers so they survive a
manual rewrite.

### Title, preface, and section 1

- Keep the farmer story as intuition, but stop it before implementation terms
  such as *confirmed-complete* and Sub0 need forward explanation. Follow it with
  a small glossary.
- Replace "written as the design rather than as a pile of revisions." It is a
  claim about editorial history, not the algorithm, and invites contradiction
  as the document evolves.
- Split the long "Pains are taken" paragraph into: synchronization policy,
  runtime/lifetime policy, and error policy.
- "The CAS sites ... are not open retry loops" conflicts with the actual
  `for (;;)` revalidation loops. The intended property is more useful: they do
  not retry the same expected snapshot indefinitely. Say that every failure
  reloads and revalidates the reason for the operation.
- Move the strong-versus-weak CAS discussion to rationale. The normative line is
  simply that the current implementation uses a strong CAS.
- Move the D GC alignment observation to the implementation profile or README.
  The core contract is 64-byte-aligned allocation and explicit lifetime.
- Move the atomic-order catalogue to the dedicated memory-order section. Keep
  only the publication/reclamation principle in the early invariant list.
- Break the one-paragraph safety proof into named invariants. It currently
  repeats most of 2a, 3a, 5b, and 5j before those mechanisms are introduced.

### Section 2a: tallies

- Keep the `Rt` bitfield table and leaf/root aggregation here.
- Reserve *leaf* for `Lt` fan-out and *work shard* for a Tindex slice. The text
  currently uses *shard* for both, which makes "MT payloads outlive their
  shards" especially confusing.
- State the cached-leaf rule once: decrement the exact `Lti` used by the
  corresponding increment, irrespective of later `IDc` feedback.
- Move "Taking a count," "Last-releaser," and the Sub0 lifecycle into the
  consumer lifecycle/reclamation protocol.
- Replace "transiently up to one unit per concurrent last-leaf" with a direct
  bound or omit it. The safety-relevant facts are that Sub0 is counted, does not
  overlap the count/Sub fields, and may temporarily contain more than one unit.
- Keep underflow detection in the error contract; here it is enough to identify
  the bit ranges and signed leaf counters.
- Put the x86 paragraph in the memory-order section and resolve the immediate
  versus eventual cleanup issue described above.

### Sections 2b and 2c: statistics and Farm fields

- Convert both lists into one field table with columns for owner/writer,
  publication edge, and reader. That makes the memory model more visible than
  prose such as "miscellaneous immutable stats as are convenient."
- Define `Es` as a logical epoch/generation tag and `Ki` as a physical slot.
  Avoid referring to both as merely "the segment."
- State the `Es` publication rule once: raw-initialize sibling fields, then
  release-store matching `Es`; readers acquire `Es` before raw-loading them.
- Keep `Sd`'s meaning as *accounted primary-complete table bytes*, avoiding the
  less precise "consumed-size" until MT completion terminology is fixed.
- Define `Reqs_c` and `Reqs_p` once here. The cache-distribution rationale for
  keeping them separate can be one note, not repeated in producer registration.

### Section 2d: epoch 0

- Retain this as construction behavior, after the Sub0 transition table is
  available.
- Replace "more on subscription later" with a direct cross-reference.
- State plainly that Sub0 blocks reuse/lapping, not writes beginning in epoch 0.
  The last two existing sentences make this point but can be condensed.

### Section 3a: quota arithmetic

- Distinguish the tier grant, the token's `quotaLeft`, a reservation size, and
  `Exmax`. The current `Exi` shifts between "single commit limit" and "running
  quota," making the proof harder to follow.
- Make the construction relationship exact:
  `Exmax = maxBulk*quotaBulk + maxSmall*quotaSmall` and
  `Exmax <= (K-1)*segCap`.
- Change "should account for" to the actual enforced requirement.
- State the two quota provenance states once: sweep-verified quota may cross a
  boundary; opportunistically renewed quota must probe and cannot cross without
  a successful sweep.
- The sentence saying linearization on the Wt modification chain is "probably
  unnecessary" is an open design thought inside normative text. Either assert
  the acquire/release protocol and its proof, or move the uncertainty to an
  open-questions/rationale note.
- Merge the two identical "producer may subscribe, drain, and retry" passages
  into the backpressure contract.
- Clarify "If a producer leaves it can pass its quota along" as Token ownership
  transfer. It should not sound like a fresh producer can inherit undocumented
  quota outside the checked move operation.

### Section 3b: producer tiers

- Keep the simultaneous-producer cap as a safety requirement.
- Move performance advice (4 or 8 segments, useful batches, write-tail
  contention) to an implementation/tuning subsection.
- Keep construction constraints in one table. Tag each as either an algorithmic
  invariant (`Exmax` below a lap) or current implementation limit (`KMAX`,
  `Ln >= 2^18`, `segCap >= 2048`).
- Keep registration slot ownership and stale-token rejection normative.
- Move 64-byte ticket stride and the exact hash recipe to implementation detail
  unless external compatibility depends on them.
- Describe token transfer once. The exact copy-constructor store order belongs
  in the memory-order implementation table, not in both registration and the
  `write()` argument list.

### Section 4a: API, validation, and layout

- Move payload layout before table layout, since table sizing refers to payload
  header/body lengths.
- Replace "`PayloadHeader* header`. More on that later" with a link to an
  already-defined structure.
- Keep the D distinction between `const(ulong)[]` and `const ulong[]` as an API
  language note, not part of the concurrency proof.
- Keep one canonical table-layout table and delete the later Thead list in 5c.
- Add explicit definitions for offsets versus absolute sequences. In the table,
  Tindex entries are payload offsets relative to the table start; `Tnext` is an
  absolute sequence.
- Clarify that `Tmt` is populated when `maxCs > 1`, matching emission.
- Put all caller validation into a compact contract table: null header/callback,
  header bounds, overflow, tier-unsized singleton, bad `avgCost`, and invalid or
  forged token.
- Retain the per-tier singleton-size rule. It is subtle but normative; trim the
  narrative example after the rule is clear.
- Keep the partial-write and non-advancing-range contracts. The convenience
  overload inventory can move to API documentation because all forms emit the
  same table.

### Section 4b: reservation and publication

- Present this as an ordered list. The order is the contract:
  reserve `Wt`; initialize every crossed epoch; plant protection for crossed-out
  incomplete epochs; emit table words atomically raw; release-store `Tsent`
  last.
- Explain once why crossed segments receive the reservation's final `Wt'` as
  `Seqt`.
- Cross-reference the common release-last publication rule instead of restating
  it in 2b, 4b, and 5c.
- Separate logical publication from opportunistic quota renewal; their current
  adjacency makes `Wt'` do too many explanatory jobs at once.

### Section 5a: ConsumerView and subscription

- Correct positive success to non-negative success.
- Keep the unique-ownership/POD constraint and the contiguous
  `[oldestEi,newestEi]` pin range together.
- Define `newestEi` as position and `oldestEi` as reclamation frontier once.
- Turn subscribe into a numbered protocol with an explicit unwind rule shared
  by every failure path.
- Retain "pin before metadata read" prominently; it is the core wrap-safety
  rule.
- Move the transient confirmed-unsubscribe exception out of the main walk and
  into a race note after the basic candidate-selection rule.
- Keep the two generation bounds checked after the second `Wt` read, but label
  the units: compare epoch numbers after shifting `Seqt` and `Wt` by
  `segShift`.
- Condense the `Sub` arithmetic. The key contract is that it is a temporary
  high-half pin held across candidate validation and leaf establishment, and it
  never manipulates Sub0.
- State failure reasons in the API/error table as well as their local unwind
  points: capacity, mixed/wrapped snapshot, or uninitialized frontier.

### Sections 5b and 5c: release and orientation

- Put all root-release cases in the single Sub0 transition table rather than
  repeating the last-releaser proof in each bullet.
- Keep the important ordering: release confirmed old-prefix pins, decrement
  `Cf`, then release remaining pins with the correct last-of-Farm policy.
- Preserve the distinction between "last pin on this segment" and "last
  subscriber in the Farm." Much of the original bug history came from
  conflating them.
- Replace the long final pulse summary with two postconditions:
  abandoned incomplete work remains protected/discoverable; an empty fully
  accounted Farm retains one subscribe anchor.
- Split orientation into ordinary table advance and idle frontier migration.
  The latter is a liveness/reclamation mechanism, not basic parsing.
- Move Thead's field list to the layout section.
- Keep the spanning-table explanation, but tie it to the qualified states:
  position may move to an initialized frontier only after the old position is
  confirmed; the old pin is released only when it reaches the confirmed prefix.

### Section 5e: primary claims

- Restore continuous numbering, either by renumbering 5e as 5d or inserting a
  real 5d for table/payload interpretation.
- Define `Tseq`, `Shi`, shard geometry, threshold, and chunk before the numbered
  algorithm. Several current steps are definitions rather than actions.
- Change "all entries are complete" at `Tprogress == Tlen` to "all primary
  Tindex runs are accounted."
- Use an explicit low-half comparison for completion:
  `low32(Y) == Shlen - runLength`. The current `Y << 32` expression is an
  implementation trick that obscures which packed field is tested.
- Explain `X` and `Z` where they are first used rather than saying `X` will be
  useful later.
- Move first-claimant mid-tick yield into a named optional scheduling subsection.
  Its proof is worth retaining, but it interrupts the baseline claim loop.
- Keep the priority rule that shard completion beats yield. Delete the stale
  role parenthetical noted above.
- State once that each Tindex entry produces exactly one primary Pcount claim,
  after defining the secondary/tertiary distinction.

### Sections 5f and 5g: payload entry and secondary MT work

- Define `pcount` as three independent monotonic packed fields: admissions,
  issued calls, and returned calls. Avoid using *completions* without saying
  whether it means `pcount.completions`, Tcount run accounting, Tprogress, or Sd.
- Add the callback ordering that is currently implicit:
  claim admission; claim an iteration; call synchronously; increment returned
  calls; only then can that primary claimant account its run.
- Explain the ST optimization after the general path and label its exact
  predicate. Its calls/returned fields remain zero by design.
- State that callback return values are presently ignored by Farm accounting,
  if that remains the contract.
- Expand the secondary path just enough to be testable: work shard `Shi` visits
  MT-index positions `Shi, Shi+SqCs, ...`, and an admitted visitor loops over
  calls until no unissued iteration remains.
- Treat "normally about `SqCs` consumers enter" as performance intuition, not a
  guarantee. `maxCs`, races, and repeated visitors to the same shard determine
  actual admissions.

### Sections 5h and 5i: feedback and sweeping

- Mark both ID-feedback rules as scheduling heuristics. They affect future work
  shard/leaf selection but are not required for memory safety.
- Rewrite "the iterations are completed, then each Ci increments Shc one more
  time." `Z` counts exhausted-claim observations, not callback completion.
- Be explicit that `Z <= 1` is evidence used by a heuristic, not proof that a
  native consumer no longer exists.
- Keep foreign-shard sweep as the liveness policy; use "attempt to claim and
  process" because another sweeper can race.
- Apply the revised claim-slot guarantee. A specific sweeper need not find the
  second admission if another draining entrant already owns it.
- Replace both remaining MT-outlives-shard sentences with the precise
  accounting-versus-pin explanation above.
- Keep carried sweep across small and already-accounted tables. This is current
  liveness behavior, but isolate it from the baseline tertiary algorithm so its
  state transition is easy to see.

### Section 5j: accounting, confirmation, and reference retention

- Make this the canonical definition of the completion/reclamation states.
- Change "consumed-size accumulator" to "accounted-table-size accumulator" or
  explicitly define *consumed* as primary accounting, not all MT callbacks
  returned.
- State that the finisher can add `Tsize` while another MT callback is in flight;
  this is safe because the callback's own `ConsumerView` pin still prevents
  `Rt == 0` and reuse.
- Keep the three-term confirmation predicate together and distinguish logical
  `Ei` from physical `Ki` in every clause.
- Keep the contiguous-table-size proof. Move the parenthetical history of the
  rejected signed `Sbal` design to a rationale appendix.
- Retain the critical rule that a non-last Farm unsubscriber can nevertheless
  be the last releaser of an early segment and must leave Sub0 there. State it
  once after the transition table.
- Merge the two position/frontier paragraphs with 5c's idle migration.
- Keep the cursor-based idle re-walk as the final liveness backstop, but remove
  its repeated appearances from earlier scheduling sections.
- End with the reuse condition: confirmation permits pins to be released;
  producer reuse occurs only after the contiguous acquire sweep observes zero
  roots.

## Memory-order presentation

The current "Atomic memory orders, in one place" paragraph is useful but too
dense and not actually the only place the orders are described. Replace it with
a table and then let operational sections refer back to named handoff edges.

Suggested table shape:

| Object/transition | Publishing operation | Observing operation | Ordered data |
| --- | --- | --- | --- |
| table publication | release-store `Tsent` last | acquire-load and validate `Tsent` | raw-atomic Thead remainder, indexes, counters, payload header/body |
| segment metadata publication | release-store `Es` last | acquire-load and validate expected `Es` | raw-atomic `Seqt`, `Cs`, `SqCs`, initial `Sd` |
| consumer leaves a leaf | acq_rel leaf decrement | acq_rel edge decrement and release root update | earlier ring reads join the leaf release sequence |
| producer reuses a segment | release root transition toward zero | acquire-load `Rt == 0` in quota sweep | prior consumers' ring accesses happen before reuse |
| write-tail reservation | release RMW on `Wt` | acquire probe used by quota logic/subscribe | published tail position and bounded sweep anchor |
| temporary subscriber pin | release add/sub of `Sub` plus acquire metadata/root validation | producer acquire of nonzero `Rt` | pinned wrap window cannot be lapped while attaching |

Then state separately that all ring words are atomic objects even when accessed
with raw order. The release sentinel publishes their initialized values; using
raw atomic stores/loads also prevents a later physical-word reuse from becoming
a plain-versus-atomic data race.

Avoid a blanket "other orders are raw" if a later implementation adds an edge.
It is safer for the table to enumerate every object whose ordering is part of
the proof and for local code comments to cite the corresponding edge.

Platform-specific reasoning should be labeled as such. If x86 TSO is a support
requirement, say so in scope. If the algorithm is intended to be portable to
weaker architectures, the normative proof cannot end with an x86-only
observation.

## Formatting and terminology pass

These changes are mechanical after the semantic rewrite:

- Fix the missing 5d and regenerate the table of contents after renumbering.
- Use *physical segment/slot* for `Ki`, *logical epoch* for `Ei`, *work shard*
  for a Tindex slice, and *leaf* for an `Lt` counter.
- Use lowercase common nouns and backticks for exact identifiers. Avoid prose
  capitalization such as "Payloads," "Called," and "Call" when it does not
  denote `PayloadEntry`, `pcount.calls`, or the callback field.
- Choose *single-threaded* and *multithreaded* consistently.
- Choose one requirements vocabulary: **must** for safety/API requirements,
  **may** for allowed behavior, and "current implementation" for tunables.
- Replace "more on that later" with direct section links.
- Replace first-person design asides ("I have considered...") with rationale
  notes or remove them.
- Prefer explicit packed-field helpers such as `claims32(x)`, `calls16(x)`, and
  `returned16(x)` in prose over shifts that make the reader reconstruct the
  layout.
- Use `2^16`/`2^32` consistently, either as code or superscript, rather than
  mixing prose and HTML notation.
- Reflow all list items consistently. The `Payloads` argument and convenience
  range paragraphs are hard-wrapped while most surrounding bullets are not.
- Use one notation for operation names. The implementation spells them
  `atomicFetchAdd`/`atomicFetchSub`; the spec can use conceptual
  `fetch_add`/`fetch_sub`, but should state that convention once.
- Put formulas on their own lines when they are part of a proof. The quota and
  shard geometry paragraphs currently bury several independent equations.
- Avoid qualitative absolutes where the behavior is heuristic: "normally,"
  "about," "essentially," and "probably" should either be quantified or moved
  to tuning/rationale.

## Suggested conformance/error table

One compact table would replace many scattered "fatal" repetitions while
keeping the unusual process-abort policy visible:

| Situation | Result |
| --- | --- |
| producer tier has no registration slot | invalid Token; recoverable |
| consumer capacity exceeded | negative `subscribe()` result; recoverable |
| subscribe sees an uninitialized or generation-inconsistent frontier | negative result after unwinding temporary pins; recoverable |
| `write()` has no safe quota at present | returns 0; backpressure |
| only a prefix fits current quota | returns the prefix count; caller advances its own range |
| bad/stale/forged Token | fatal |
| invalid payload header, callback, iteration bounds, or `avgCost` | fatal |
| payload cannot fit a singleton table in the caller's tier | fatal |
| size arithmetic or packed counter wraps | fatal |
| tally underflow or invalid ownership transition | fatal |
| destroy with live consumers or producer tickets | fatal |

The operational section should still mention a fatal guard at the point where
it matters to a proof, but it need not repeat field widths and all consequences.

## Material to move out of the normative path

These details are worth preserving, just not inline with the core algorithm:

- the abandoned `Sbal` design and its producer-producer initialization race;
- the question of whether a weak CAS could replace the current strong CAS;
- historical motivation for the 2 MiB minimum ring and 2048-word segment floor;
- "probably 4 or 8" and other characterized geometry advice;
- cache-line placement rationale for producer slots and `call` beside `pcount`;
- the D GC's alignment behavior at the time the text was written;
- performance impressions about batch size;
- skip-list alternatives considered for table orientation.

Keep a short reason next to any rule whose removal would endanger correctness.
Move only the history, alternatives, and tuning observations.

## A practical folding sequence

To minimize accidental semantic changes while editing by hand:

1. Add the glossary and qualified completion states.
2. Apply the eight semantic corrections above without moving sections.
3. Build the one canonical layout section by copying, then delete the duplicate
   field lists only after checking every field survived.
4. Build the Sub0 transition table, then reduce the repeated lifecycle prose to
   cross-references.
5. Move producer, consumer, scheduling, and reclamation sections into lifecycle
   order.
6. Replace the memory-order paragraph with the edge table and verify every
   acquire/release in `antfarm.d` has a named reason.
7. Move history and heuristics to their appendices.
8. Renumber headings, regenerate the contents, and do the terminology/style
   pass last.
9. Finally compare the rewritten rules with the core tests: fresh epoch-0
   subscription, multi-lap wrap, spanning tables, pure subscription churn,
   confirmed-segment pulse cleanup, late attach under wrap, slow MT bounds,
   per-tier unsized payload rejection, and forged-token rejection.

The main rule for the rewrite is: `Sd` says how far primary table accounting has
advanced; `Rt` says whether ring memory is still protected. Keeping those two
facts separate prevents both the MT wording problem and most of the repeated
reclamation explanation.
