# Ant Farm: An M:N Concurrent Queue with Superlative Scaling

Living spec, matching the implementation in this directory. The farmer metaphor and the field names are the original; the structure is the current one — held-epoch pins, last-releaser Sub0, ticketed write, pin-before-read subscribe — written as the design rather than as a pile of revisions.

- [1. General Idea](#1-general-idea)
- [2. Fields](#2-fields)
  - [2a. Segment Tallies](#2a-metadata-fields-segment-tallies)
  - [2b. Segment Statistics](#2b-metadata-fields-segment-statistics)
  - [2c. Farm-level Metadata](#2c-farm-level-metadata-fields)
  - [2d. Epoch 0](#2d-epoch-0)
- [3. Production](#3-production)
  - [3a. Arithmetic and Safety](#3a-production-arithmetic-and-safety)
  - [3b. Producer Tiers](#3b-producer-tiers-and-notes-on-contention)
- [4. write() Details](#4-write-details)
  - [4a. Call and Return](#4a-call-and-return)
  - [4b. After fetch_add()](#4b-after-fetch_add-to-the-write-tail)
- [5. Consumption](#5-consumption)
  - [5a. ConsumerView](#5a-consumerview-overview-creation-and-subscription)
  - [5b. Unsubscription](#5b-unsubscription)
  - [5c. Orientation](#5c-consumer-orientation)
  - [5e. Primary Work Claims](#5e-consumption-strategy---primary-work-claims)
  - [5f. Payload Mechanisms](#5f-payload-mechanisms)
  - [5g. Secondary Work Claims](#5g-secondary-work-claims)
  - [5h. Oversaturation Feedback](#5h-feedback-potential-oversaturation)
  - [5i. The Sweeper Role](#5i-the-sweeper-role-tertiary-behavior)
  - [5j. Segment Completion](#5j-segment-completion-tracking-and-reference-retention)

---

## 1. General Idea

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. The last ant to leave a *confirmed-complete* area changes the tally (a reference counter) to zero. The last ant to leave an *incomplete* area instead leaves a Sub0 mark, so the farmer still sees a nonzero tally and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

The implementation of this idea: a circular buffer using the "magic buffer" memory mapping such that the second half of the buffer wraps the first half. The buffer has a power-of-two length `Ln` in sizes to fit the mapping requirements of Linux and Windows and uses 64-bit `ulong` as its base unit. The buffer has a power-of-two number `K` (probably 4 or 8) of segments at regular intervals, each segment representing a successive epoch `E` of data. A sequence number `Seq` is the element number of the buffer as it wraps around; `Seq & (Ln-1)` gives the array index for that sequence. `Seq * K / Ln` is the epoch for that Seq. `Ei & (K-1)` is the segment index `Ki` for that `Ei` and span of Seqs.

Pains are taken to *eliminate* CAS operations from the hot paths of the module. Hot-path synchronization is fetch-add / fetch-sub plus acquire/release load/store. The CAS sites are cold: producer-ticket claim, the last-releaser pulse on unsubscribe, and `plantIfUnprotected`'s plant / retract. They are not open retry loops. A failed CAS means some other thread changed the word, and that change often invalidates the reason to plant — count or pulse already present, so the loop bails or takes a different action against the new snapshot. The last-releaser still has to land its decrement, but each retry is against a freshly observed `Rt`, not a spin on the same snapshot. We use a *strong* CAS (D's `cas` does not fail spuriously). Whether a weak CAS would suffice is not settled: a spurious fail would only extra-retry on the looping sites, and the one-shot retract already treats failure as harmless because a later 0→1 clears the pulse. The reference implementation is in D and the Farm should be allocated into 64-byte-aligned memory with manual lifetime; at the time of writing D's garbage collector only guarantees alignment to 16 bytes. Another caveat: this system is designed such that most errors are fatal with very few exceptions. Just about every interface to the Farm is `@nogc nothrow @system` code.

Atomic memory orders, in one place: root-tally RMWs are release, root-tally loads are acquire; write tail RMWs are release and producers' probes are acquire. Leaf-tally RMWs are acq_rel: a non-edge consumer never touches `Rt`, so the last-on-leaf's `Rt` release has to carry that consumer's ring reads through the leaf's release sequence. `Tprogress` RMWs are acq_rel so its final transition can join callback writes before an optional table-completion notification. `Es` and `Tsent` follow a store-released-last pattern against acquire-loads. A matching acquire of `Es` (or of the table sentinel) therefore also orders the subsequent raw header reads. Other orders are raw. Table contents other than `Tsent` (Thead words 1–7, Tindex, Tcount, Phead, Pbody) are stored raw so they are the same atomic objects consumers load-raw; wrap reuse of a physical word is atomic vs atomic, not a plain store racing an atomic load.

Safety argument in one paragraph, since later sections rely on it: producers only ever write within quota verified by sweeps of contiguously-forward zero Rts, and total maximum quota `Exmax` is bounded by K-1 segments' capacities, strictly less than a lap. A consumer holding any tally therefore protects not just that segment but everything ahead of it within one lap: any sweep far enough ahead to threaten the consumer's unread data must pass contiguously through the held segment and break. Conversely, a producer cannot jump a lap on blind quota alone, so the protection cannot be outflanked. An incomplete segment is never visible as `Rt == 0`: the last releaser of an unconfirmed segment leaves a Sub0 pin (5b), so a producer cannot treat abandoned incomplete work as free space. `plantIfUnprotected`'s post-CAS retract keeps a confirmed segment from carrying Sub0, so the "incomplete segments are never `Rt == 0`" invariant is not paid for with the mirror-image leak of "confirmed segments are `Rt != 0`". Idle consumers that have caught `Wt` keep a live pin on the frontier segment (5c), so the system never drops to zero pins while anyone is still subscribed. Consumers retain references until segments are *confirmed complete* (5j). Consumers' unread data is always protected, and the system never overwrites unexecuted work.

---

## 2. Fields

### 2a. Metadata Fields: Segment Tallies

There are two preallocated K-length circular buffers of segment metadata: tallies (reference counters) and snapshots of general stats. The tallies are modified by initializing producers, consumers, and subscribers. There is a root tally `Rt` and array of leaf tallies `Lt` for each segment `Ki`. `Rt` tracks active consumers and prospective subscribers; more on that later. Let `Cs` be the segment's number of consumers snapshot, `SqCs` that value's square root, and `IDc` be a consumer `Ci`'s ID value. The square root function used throughout the module returns a minimum value of 1 for `Cs <= 2`. Its purpose is to balance consumers across shards and this prevents divide-by-zero on future operations. As consumers move through payload tables, they increment a reference on the next table's segment then decrement their current one.

`IDc % SqCs` gives the `Lti` for that `Ci` and `Ki` *at the moment of increment*. If the *actual* number of consumers majorly differs from `Cs`, or the span of IDs is fragmented and/or bunched up, performance may degrade but safety is inviolate. Consumers have a general strategy for mitigating unbalanced shards; more on that later. Enough Lts are preallocated for an enum `MAX_CONSUMERS_LIMIT = 128` — the ceiling square root of which is 12 (`MAX_LEAVES`).

`Ci` claims a reference to `Ki` by fetch-incrementing its `Lti`, and relinquishes by fetch-dec. Leaf RMWs are acq_rel. When the operation is an edge transition to/from zero — the inc() returns 0 or the dec() returns 1 — `Ci` repeats that operation on `Rt` with release semantics. A consumer that is not last on its leaf never writes `Rt`; acq_rel on the leaf puts that consumer's ring reads in a release sequence the last-on-leaf's `Rt` release (and a later producer acquire of `Rt == 0`) can carry. Underflows below 0 are fatal errors in any context and all fields subject to decs are signed integer types. `Rt` is the exception: it is an unsigned bitfield whose underflow is detected on the count bits via `COUNTMASK` rather than by sign, so a live Sub0 can never mask a genuine count underflow.

Important invariant: consumers *must* always increment a tally ahead before decrementing their current one. This ordering is necessary to leave no gaps where a producer might spuriously observe an `Rt` with a zero value. Producers load-acquire `Rt` and shall regard segments with nonzero Rts as contributing no available space. Distributing consumers across the leaf tallies reduces contention and producers need only one shared signal per segment to evaluate write limits.

`Rt` is a bitfield:

| Bits | Field | Unit | Role |
| ---: | --- | ---: | --- |
| 63..32 | `Sub` | 2<sup>32</sup> | prospective subscribers (5a) |
| 31..16 | `Sub0` | 2<sup>16</sup> | pulse pin on incomplete / empty-farm space |
| 15..0 | count | 1 | active consumers; `COUNTMASK = Sub0 - 1` |

Let `Rtlow` be the least significant half (`LOWMASK`). `COUNTMASK` is the count bits below Sub0. Underflow checks on `Rt` test only `COUNTMASK`.

Sub0 is a counted pin in units of 2<sup>16</sup>, not a bit. At rest each segment has 0 or 1 × Sub0; transiently up to one unit per concurrent last-leaf (still well below `Sub` at `MAX_CONSUMERS_LIMIT`). OR and AND are not used on this field: they cannot share accounting with the counted last-releaser chain (5b).

Because a segment's `SqCs` snapshot changes across epochs, a `Ci` must cache the `Lti` used at each increment and decrement that same leaf. The ConsumerView keeps one `Lti` per held physical slot (`ltiRing`, 5a). `IDc` may later be nudged (5h, 5i); already-incremented leaves are not moved.

**Taking a count** (leaf 0→1, subscribe or `moveRef`): `old = fetch_add(Rt, 1)`; if `(old & COUNTMASK) == 0` and `(old & Sub0MASK) != 0`, `fetch_sub(Rt, extra)` where `extra = old & Sub0MASK`, the Sub0 units the thread observed (normally one). Only one thread observes the count 0→1, so that thread is the unique subtractor for that segment. Construction's dummy Sub0 (2d) clears on this same edge.

**Last-releaser** (5b): only when the leaf dec will propagate to `Rt` (the dec returned 1). The naive sequence — plant Sub0, decrement the count, retract the plant if another consumer arrived — is not atomic. A racing first subscriber can clear the precautionary Sub0 after the count hits zero and before the delayed retract, whereupon that retract underflows `Rtlow` and borrows from the Sub field.

So the "leave a pulse only if this decrement actually hits zero" decision is one CAS on `Rt`. Load `old`; if `(old & COUNTMASK) == 0` the count underflowed (fatal); if the count is 1, the new value clears the count bits and leaves one Sub0 (adding a unit only if none is present); if > 1, the new value is `old - 1` with Sub0 unchanged. CAS that in. A failed CAS means another thread changed `Rt` — often the reason to plant is already gone — so reload and retry against the new word, not spin on the same snapshot (see 1 on strong CAS).

**Sub0 lifecycle.** Deposition happens at exactly three sites, all CAS-gated or cold:

1. Construction places the dummy pulse on `Rt[0]` (2d).
2. `plantIfUnprotected(e)` CAS-plants one unit on an incomplete epoch (`Es[e] == e`, `Es[e+1] == e+1`, `Seqt + Sd < Seqt[e+1]`, and `Rt` with count 0 and no pulse). Invoked by `write()` for every segment its reservation crosses out of, and by `unsubscribe()` via `plantUnprotectedIncomplete()` for every initialized segment left with `Rt == 0`.
3. `releaseRootLeavePulse`, the last-releaser helper: clears the count bits and leaves one unit (only if none is present) when the final root of an unconfirmed — or last-of-farm confirmed — segment is released.

Clear happens at two sites. The primary clear is the 0→1 retract in `takeRootCount`, where the unique thread whose fetch_add observed count 0 subtracts the Sub0 units it saw (normally one). The secondary clear is the retract inside `plantIfUnprotected`: the plant's completeness check read Seqt/Sd/SeqtNext once, and a finisher could have completed the segment between that read and the plant CAS, leaving Sub0 on a confirmed segment (5b-a violation, capacity leak until a new subscriber's 0→1 retracts it). The plant therefore re-reads the completeness values after its CAS succeeds and, if the segment is now confirmed, retracts exactly the unit it planted. The retract is a low-half masked CAS retry loop: it reloads `Rt`, checks that the low half is still exactly the `Sub0` that was planted, and then CASes `cur → cur - Sub0` against the freshly observed full word. A concurrent subscriber's `Sub` pin/unpin only changes the high half, so it can make a one-shot exact-value CAS fail while the planted `Sub0` is still present; the masked loop retries against the new high half and succeeds. The loop is bounded by the low-half state — it either removes the exact `Sub0`, or a racing 0→1 has already changed the low half and that same edge cleared the pulse, so the loop bails. The segment is confirmed at the time of retraction, so it needs no pulse; an incomplete segment is never retracted here.

On x86 TSO the re-read is airtight for the entered-and-released path: the last releaser's plain dec on a confirmed segment is a release RMW on `Rt` and the plant's `Rt` load is acquire, so observing count 0 synchronizes with that release and orders the finisher's earlier Sd add; a never-entered segment pulsed correctly and completed later is the residual case (self-healing on the next 0→1).

The two deposition sides are symmetric: plants are CAS-gated and only fire while the count is zero, so a plant cannot straddle a racing first subscriber (the CAS fails and retries against the new word, or bails if the count or pulse is now present — the race that invalidated the plant); clears are keyed to the 0→1 edge or to the plant's own post-CAS re-check, and only the thread that observed the edge subtracts, so a clear cannot underflow `Rtlow` or borrow into the Sub field. Producers see the signal in `refreshQuota`'s `Rt == 0` check — a lone pulse reads as occupied — and subscribers see it in the subscribe walk's `Rtlow != 0` test, so an abandoned incomplete segment is both protected from overwrite and discoverable for reclamation.

### 2b. Metadata Fields: Segment Statistics

The second set of segment metadata has these values:

- `Es` — the epoch for that segment.
- `Seqt` — the sequence that points to the first valid payload table for that segment.
- `Cs` — a snapshot of the number of consumers.
- `SqCs` — from section 2a, and other derived statistics as are convenient.
- `Sd` — the consumed-size accumulator: the sum of the sizes of all completed tables starting in this segment. See 5j.

When a producer `Pi`'s space reservation transitions across one or more segments, `Pi` initializes these values such that a load-acquired `Es` matching a `Ci`'s expected value verifies the others. This is the same publication pattern as the table sentinel (4b): `Es` is store-released last, after Seqt/Cs/SqCs/Sd are stored raw, so an acquire-load of `Es` that matches the expected epoch also orders the subsequent raw header-metadata reads. A valid `Es` therefore implies valid header metadata for that epoch, exactly as a valid table sentinel implies valid table contents. `Sd` is zeroed at the same time, before the release-store of `Es`. (Only consumers holding a reference ever add to `Sd`, and only after validating a sentinel released after initialization, so the zeroing can race nothing.)

### 2c. Farm-level Metadata Fields

There are these shared mutable fields:

- `Cf` — the current number of subscribed consumers.
- `Reqs_c` — a monotonically increasing counter of consumer subscription requests. `IDc` is taken from this counter alone, so producer registration cannot fragment consumer shard assignment.
- `Reqs_p` — a monotonically increasing counter of successful producer registrations (3b). Separate from `Reqs_c` so Token hashes do not walk the consumer ID space.
- `Prbulk` — the current number of bulk producers; more on producer tiers later.
- `Prsm` — the current number of small producers.
- `Wt` — the write tail sequence.

Plus the producer ticket slot arrays of 3b, and miscellaneous immutable stats and derived stats as are convenient.

### 2d. Epoch 0

At Farm creation we prevent a state where all segments have no references and producers might happily loop through the buffer with no one to block them. There are also zero known subscribers and no leaves or shards in which to distribute them. Thus we construct epoch 0's `Rt` with a dummy Sub0 value, blocking producers from overwriting Seq 0 until consumers digest it. More on subscription later; here it suffices to say the first subscription should succeed — on a fresh farm the construction guarantees it — and a failure (oversubscription or an uninitialized frontier) returns a negative value per 5a, it is not itself fatal. The first subscriber's leaf 0→1 on `Rt` (2a) subtracts that dummy Sub0. The Farm's constructor also takes an expected number of initial consumers > 0 to ballpark their distribution. Note that Sub0 does not block producers from writing to epoch 0. The segment where a write tail resides is not breached if there are consumers ahead of it, and epoch 0 begins with a full segment length ahead of the write tail and consumers bounded behind it.

---

## 3. Production

### 3a. Production Arithmetic and Safety

Consider `Exmax`, the maximum simultaneous excursion of the Farm's maximum set of producers. If `Exi` is a producer `Pi`'s single commit limit, `Exmax` is the sum of all Exis. We structure `Exi` as a running *quota* of ulongs all producers abide such that any `Pi` knows it can blindly write that much space before probing its actual position or checking for consumer references. Or otherwise stated, each `Pi` regards `Exmax` as a runoff zone it must observe before renewing its quota of `Exi` ulongs. `Exmax` should account for a maximum of K-1 segments' capacities.

When can `Exi` refresh? A prime opportunity: when a `Pi` fetch_add-releases `W` space on `Wt`, it necessarily receives a return value `Wret` where `Wret + W` is a momentary `Wt'` — the next write tail — with staleness bounded by `Exmax - Exi`. The beginning of the next segment `Seqb` from `Wt'` is given by simple arithmetic of segment capacity × next segment epoch, or `((Wt' * K / Ln) + 1) * (Ln / K)`. If `Seqb - Wt' >= Exmax`, `Pi` can opportunistically renew its quota: every outstanding blind write then lands within the segment already being written.

Leftover quota from an earlier renewal is not itself an Rt check. The Token records whether the current leftover was filled by a live `refreshQuota` sweep or by opportunistic in-segment renewal (`quotaSwept`). Sweep-verified leftover may cross segment boundaries; that sweep already performed a complete check against following segments. Opportunistic leftover must probe the current write tail: those producers can bump past a segment boundary, see remaining space `>= Exmax`, and renew again. Therefore only sweep-checked quotas may blindly produce or breach segments; others must probe `Wt` and either observe `Exmax` space still inside the current segment or conduct a full sweep. If remaining space is `< Exmax`, concurrent leftovers could sum past the boundary, so the write sweeps (and is then tagged swept); if the sweep fails it returns 0 (full) rather than crossing on leftover. Construction's initial grant is not a sweep.

The probe is an acquire-load of `Wt`, not a dummy `fetch_add(Wt, 0)` inserted into the modification order. With the leftover rules above, linearizing on the edit chain is probably unnecessary: opportunistic leftover cannot cross unless remaining space is `< Exmax`, and a sweep from a slightly stale anchor still looks at the same live segments. Reservations of `Wt` are release RMWs, so the acquire-load synchronizes with a published tail. What remains is bounded timeliness of those releases to later acquires: a stale load could point the leftover gate at the wrong remaining space. Release-to-acquire on `Wt` is how that bound is kept.

If a producer leaves it can pass its quota along, otherwise a new producer assumes an empty quota and uses the following mechanism to refresh.

If a `Pi` has depleted its swept quota, probed its `Wt` position, and needs to check following segments to make `Exmax` space, then it scans the K-1 segments ahead of the probe for contiguous 0 Rts, adding their full capacities until it has found `>= Exmax` space or encounters a live segment and breaks. The same acquire-load is the sweep's anchor; a second probe is not taken. If sweeping fails then backpressure is simply too high; full for `Pi`'s purposes. A stalled producer may subscribe a ConsumerView, drain, and retry `write()` — that is the supported escape hatch, not an out-of-band trick. The sweep cannot land on spuriously orphaned islands of space because consumers maintain continuous reference spans without gaps.

### 3b. Producer Tiers and Notes on Contention

The Farm uses two tiers of producer to cap the max excursion, so one or a few enumerated batch writers can commit large chunks and the thread pool can occasionally write a couple kilobytes at a time. The single write tail is a linear point of contention and it's on a performant developer's shoulders to control the frequency of adds to it. (Though testing has revealed batch sizes reasonably above 1 to be the most crucial aspect.) Important invariant: the number of *actual* simultaneous producers *must* not exceed the maximums to prevent stomping consumers.

Construction constraints (`create`): `K` is enforced as the useful power-of-two range `[2, KMAX]` where `KMAX = 16` (section 1 suggests 4 or 8). `Ln` must be a power of two `>= 2^18` (2 MiB in the ulong base unit); smaller rings were never studied, and a `segCap` floor (currently 2048, subsumed by `Ln >= 2^18` with `K <= 16`) keeps per-table header/pad space from dominating a segment's capacity — both bounds are documented for later investigation rather than tuned. `Exmax <= (K-1)*segCap` is enforced so a full quota excursion is strictly less than a lap (3a). Quota roles follow the rule `quotaRole > 0` iff `maxRole > 0`: an enabled tier must declare a positive quota (bulk auto-defaults to `segCap` when unspecified, legacy convenience; small fatals on 0), and a disabled tier's quota is normalized to 0 and takes no part in `Exmax`.

Registration is how that invariant is enforced. The Farm preallocates two slot arrays, one of length `maxBulk` and one of length `maxSmall`, each entry holding a hash (and an authoritative quota ledger) and initialized to an invalid value. Producer ticket slots are 64-byte strided: tickets are shared mutable state under the registration CAS, and the buffer cannot align them beyond 8 bytes, so the stride (not the natural ulong size) must carry the isolation. `registerProducer(tier)` fetch-increments the matching Pr counter (backing out and returning an invalid Token if the tier is full), fetch-increments `Reqs_p`, writes a hash of (slot index, `Reqs_p`) into a free slot, initializes the slot's quota ledger to the tier quota, and returns a Token `{ slot, hash, private quota }`. `unregisterProducer(ref Token)` writes invalid into that slot, clears the ledger, zeroes the token, and decrements the tier counter; a Token that does not match the slot is fatal. `write()` takes the Token by reference and fatals if the hash does not match the live slot or if the token's private quota mirror exceeds the farm's ledger for that slot. Unregistered threads therefore cannot call `write()` and cannot bypass `Exmax`, and a registered caller cannot forge a fresh quota past its grant by mutating the token. `Reqs_p` is not `Reqs_c`: a unified counter would advance `IDc` on every producer registration and immediately fragment consumers across shards.

---

## 4. write() Details

### 4a. Call and Return

Arguments:

- `Payloads` — a slice or forward range of `PayloadEntry` structs. `write()`
  saves independent checkpoints for sizing and emission and does not advance
  the caller's range.
- `Token` — the caller's registration ticket (3b). Required; a mismatch is fatal. The Token is passed by reference; it carries the caller's remaining quota as a private field, and the Farm mirrors it in a per-slot ledger so a forged ticket cannot exceed the granted excursion. Tokens are single-owner and transfer-only: copying consumes the source (the copy constructor release-stores the valid hash last, then clears the source's hash), so at most one live token exists per slot and a stale copy fails `requireToken`. `quotaSwept` (3a) travels with the leftover.
- `AvgCost` — optional, default 1. The producer's declared Call cost class: a log2 shift in `0 .. log2(MAX_CHUNK)` published in Thead. 0 means Calls are very cheap (chunk stays at `MAX_CHUNK`, maximum claim amortization); larger values shrink the chunk (short visits, less tail). `write()` fatals on `AvgCost` out of range.

Returns: the number of Payloads written.

Convenience sources preserve the same table contract: separate header/body
forward ranges are paired positionally; one common header may be broadcast
over a body range; and the common-header overload may additionally receive a
uniform body length in ulongs. The last form treats that length as a caller
contract and uses it for arithmetic sizing before emission.

`PayloadEntry`:

- `PayloadHeader* header`. More on that later.
- `PayloadBody body`; where `alias PayloadBody = const(ulong)[]`; type-erased data. (A mutable slice of const ulongs; `const ulong[]` would const the slice itself and make `PayloadEntry` unassignable in D.)

Producers commit a table `Ti` of Payloads up to `Exi` in total size, with sections for:

| Offset (ulongs from table start) | Contents |
| --- | --- |
| `[0 .. 8)` | Thead. Word 0 is `Tsent` (store-released last). 1 `Tnext`. 2 `Tmt<<32 \| Tlen`. 3 `Cs`. 4 `SqCs`. 5 `Tsize`. 6 `AvgCost`. 7 an optional stable `TableCompletionHook*` (zero for ordinary writes). |
| `[8 .. 8+Tlen+Tmt)` | Tindex: dense index of where each Payload sits. Total index first, MT index second. `Tmt` is the number of multithreaded payloads actually written; `Tlen` the total. |
| + 7 | padding |
| + 1 | `Tprogress`. When a `Ci` identifies itself adding the final completion sum for its shard, it increments `Tprogress` by the shard length with acquire/release ordering. |
| + 7 | padding |
| then | `Tcount`: combination claims/completions counters. Consumers fetch_add the 32 most significant bits as claims and 32 LSB as completions. There are `SqCs` such counters, each followed by 7 ulongs of padding. After any packed increment, if the field just incremented has wrapped to 0 the process fatals rather than continue with a corrupted counter. Under the 512 caps below, 32-bit claim wrap requires pathological visitor accumulation on one shard. |
| then | the Payloads laid sequentially. |
| last 7 | padding at the end of the table. |

`write()` validates every payload header before accepting it. `Done` and `MaxCs` are in 1..512. `MaxCs >= 1`, and `Done > 1` requires `MaxCs >= 2` — a multithreaded payload must admit at least two entrants, because a primary visit burns exactly one claim per payload (5e) and a later sweeper must always find a free claim slot to drain it (5i). Violations are fatal.

Size arithmetic is overflow-checked before any compare to quota. If `body.length` would overflow when added to the payload-header length, or the running payload sum, candidate table size, or final table size would wrap, that is fatal. A wrapped candidate must never be compared to `Exi`.

A payload that cannot fit in the *caller tier* maximum `Exi` is a caller error, not backpressure. If a singleton table containing only that payload — Thead, index slots, pads, Tcount words for the current `SqCs`, the payload, and the end pad — exceeds the caller's tier quota, `write()` fatals. The check is by caller tier, not `max(quotaBulk, quotaSmall)`, because a payload that only the other tier could ever publish would remain in a small producer's input range on future calls. Thus it's a contract failure that must be wrangled upstream, and `write() == 0` strictly means the farm is full for `Pi`'s purposes. Multi-payload truncation to the caller's current `Exi` stays legal: if `Ti`'s size exceeds a depleted `Exi`, `Pi` attempts to refresh it, then keeps however many Payloads fit.

At this level producers do nothing else to resolve fullness, they merely return commit lengths dwindling to 0. A stalled producer may subscribe a ConsumerView, drain, and retry — that is the supported escape hatch (3a).

`writeTracked` is the narrow completion-notification variant used by actor
waves. It accepts only a uniform single-threaded, single-shot header
(`MaxCs=1`, `Done=1`) and stores a caller-owned `TableCompletionHook*` in
Thead word 7. The hook is invoked exactly once by the `Tprogress == Tlen`
finisher, after every Call in that table has returned. Its storage and context
must remain stable until invocation returns. An aggregate producer must count
the table before calling `writeTracked`, because consumers may finish it before
the write call returns; a zero write admits no table and requires rollback.

### 4b. After fetch_add() to the Write Tail

`Pi` renews `Exi` opportunistically with `Wt'` and the segment boundary as detailed before. If `Wt'` is observed to cross segment/epoch boundaries, they are proactively marked incomplete with Sub0 and their metadata are initialized. All this comes before releasing the table's sentinel value; more on that later. `Wt'`, or the sequence *after* the current `write()`, becomes those segments' `Seqt`, the first valid table for that segment. Otherwise `Seqt` would almost certainly point to a previous segment and backtracking is ill-advised. This way consumers and especially subscribers can seek forward using `Seqt` for orientation. Segment initialization zeroes `Sd` (2b). A `Tnext` equal to this `Wt'` therefore always names an initialized epoch.

`Pi` then writes the table parts described in 4a into the reserved space. Thead's first value is a sentinel computed from `Wret`, but it is store-released as the *last* value written such that consumers validating the expected sentinel by its matching sequence also validate the following contents.

---

## 5. Consumption

### 5a. ConsumerView Overview, Creation and Subscription

A `Ci`'s ConsumerView is a POD struct to be exclusively owned by one `Ci` at a time with these fields:

- `F` — reference to the target Ant Farm.
- `IDc` — taken from `F`'s `Reqs_c` during subscription. `IDc` is a future targeting hint: 5h / `adopt` may nudge it, but already-incremented leaves are not moved.
- Held pins: a contiguous epoch range `[oldestEi, newestEi]`, equal when freshly subscribed. `newestEi` is the position (last validated table, or idle at `nextSeq`'s initialized frontier). `oldestEi` advances as confirmed prefix segments are released (5j). Span `newestEi - oldestEi + 1` is at most `K`. Physical `Ki = Ei & (K-1)`.
- `ltiRing[K]` — the leaf tally index actually incremented at each physical slot. Written only on `incLeaf` for that slot; never rewritten by `IDc` bumps.
- A carried-sweeper flag (5i).

Then derived and tracked stats as are convenient.

ConsumerView is initialized with `subscribe(AntFarm F)`; when it's called:

Returns: a signed long, positive epoch if it succeeds or a negative value if it fails.

**a.** The subscriber `Si` calls `F.add_consumer()` which fetch_incs `Cf`. If a slot is available, it fetch-incs `Reqs_c` and returns that result as `IDc`. If not, `F` is oversubscribed and `Si` fetch-decs `Cf` and returns a negative value. Every subsequent failure path also unwinds `Cf`.

**b.** Acquire-load `F.Wt` — the only unpinned read — and use `Wt >> segShift` as the walk range. Walk backwards through the corresponding segments (at most `K`, stopping at epoch 0). On each step, *before* inspecting that slot's metadata, fetch_add-release a `Sub` onto its `Rt`. The `d=0` step is the heuristic pin on the frontier slot `(Wt >> segShift) & (K-1)`: a held Sub is visible to producers as `Rt != 0`, so it blocks a full lap through the current view and segments ahead of it (3a contiguous-forward sweep). Older slots sit first in that sweep, so each older step pins before reading. After the pin, acquire-load `Rtlow` (least significant half of `Rt`) and acquire-load `Es`, and rank by earliest `Es` among `Rtlow != 0`. The pulse invariant (5b) guarantees a candidate exists; the one exception is a transient race with a *not-last* unsubscribe of a *confirmed* pin, in which case `Si` stands at the frontier. Several segments may hold Sub0 at once (one per abandoned unconfirmed segment); the walk picks the earliest, which is the frontier of unconsumed work.

Groping an unpinned slot's `Rtlow`/`Es` is incorrect: those reads are not protected against wrap, so the snapshot can mix generations and `Si` can attach with no valid table while still holding a pin.

After the attach candidate is read, and while all Sub pins are still held, `Si` re-reads `F.Wt` and validates two bounds. `Seqt >> segShift` must lie in `[Es, Es + K - 1]`; this rejects a mixed snapshot where `Es` is still the previous lap's value while `Seqt` has already been advanced. `Wt >> segShift` must be `< Es + K`; this rejects a slot whose next lap was already reserved before the pin. If either bound fails, `Si` unwinds every Sub and `Cf` and returns a negative value.

**c.** `Sub` is 2<sup>32</sup> such that it tracks subscribers in the most significant half, apart from normal consumers incrementing in `Rtlow`. The walk of 5a-b already holds one Sub on every inspected slot, including the attach candidate. The number of unique subscribers (and thus Sub units per segment) is bounded by `MAX_CONSUMERS_LIMIT = 128`; one `Si` pins each physical segment at most once, so `K × 128` is well under 2<sup>32</sup>. Sub is only the establish-leaf window; it does not plant or clear Sub0.

**d.** `Si` attaches *in place*, regardless of what `Rtlow` did in the interim. The held Subs are all the same to a producer (`Rt != 0`), so they block any lap of the pinned window while `Si` establishes a real reference. If an in-flight write had already reserved past the selected slot before the pin, the `Wt` re-read bound of 5a-b fails and `Si` fails closed; it does not attach to a slot whose next lap is already reserved. If `Es < 0` or `SqCs == 0` the frontier was never written; fail closed (unwind `Cf` and every deposited Sub) rather than divide by a zero snapshot. Otherwise `Si` performs the normal process of leaf tally increment and root propagation, caching the `Lti` used. The 0→1 on `Rt` subtracts a live Sub0 if present (2a). `Si` is fully established as a consumer `Ci`.

**e.** `Si` fetch_sub-releases every Sub deposited in 5a-b, including the attach slot. The 0→1 of 5a-d already retracted Sub0 if present.

### 5b. Unsubscription

Inactive consumers block reclamation unless they `unsubscribe()` from `F`.

**a.** `Ci` first releases the confirmed prefix from `oldestEi` toward `newestEi` (5j). Only the old end moves; confirmed interiors stay pinned until they become oldest. Confirmed release is the plain leaf-root propagation of 2a. A confirmed segment that is not the empty-farm pulse must be allowed to reach `Rt == 0` so producers can reclaim it; the `plantIfUnprotected` post-CAS retract (2a) enforces this on the other side by removing a pulse a finisher completed out from under.

**b.** `Ci` calls `F.sub_consumer()`. The return value 1 means `Ci` is the last unsubscriber of the farm (`Cf` went 1→0). That flag is used only in step c; it is not itself a Sub0 plant.

**c.** Then `Ci` releases every remaining epoch in `[oldestEi, newestEi]` — confirmed interiors with the plain release of 2a, unconfirmed interiors with the last-releaser helper, then the position (`newestEi`) as follows.

- Unconfirmed interior or unconfirmed non-last position: the last-releaser CAS helper of 2a. The helper re-checks confirmation after its CAS; if the segment was completed concurrently, it retracts the pulse it just left with the same low-half masked CAS as `plantIfUnprotected`. An incomplete segment therefore never becomes visible as `Rt == 0`, even when the releaser is not last of the farm. This is what closes pure-churn orphans: a non-last unsubscriber who is the last pin on an early incomplete segment leaves Sub0 there, and a later subscriber's walk (5a-b) attaches at that earliest pulse.
- Confirmed interior or confirmed non-last position: plain release, no Sub0.
- Confirmed position, and `Ci` is last of the farm: the same CAS helper, not a bare add. This is the empty-farm pulse. The CAS makes the "plant only if the count actually hit zero" decision atomic with the count decrement, so a concurrent first subscriber either sees the pulse and clears it on its 0→1, or increments first and causes the helper's CAS to fail; it can no longer pull the pulse out from under a stale retract.

Do not extra-plant `fetch_add(Sub0)` and then run the helper on the same segment — that is two plants. If the position is unconfirmed, the helper already leaves the pulse.

With `Rt-0` constructed as Sub0, last-releasers leaving Sub0 on every abandoned unconfirmed segment, and last-of-farm leaving Sub0 on a confirmed frontier, there is always a live "pulse" somewhere in the buffer for the `Si` in 5a-b to walk back and find a segment untrodden by producers. Several pulses may exist at once: that is required, not a leak. Farm-empty with everything confirmed still has exactly one.

### 5c. Consumer Orientation

A `Ci` often needs to seek data, for instance if it's newly subscribed. Segment metadata provides a reasonable starting point. Consumers load-acquire the segment epoch and return if it is not the expected value (the caller retries or treats the frontier as not yet written). From there it has a starting Seq to a table, where the same applies to the sentinel value at that Seq.

Seek costs per entry are primarily amortized by publishing large tables. I have considered building in skip lists or other structures dynamically updated by consumers but am sticking with what's essentially a linked list of one table pointing to the next. Within tables the `SqCs+1` counters — `SqCs` shard counters for contention plus `Tprogress` for completion — keep a small target for both.

As consumers move to new tables, they check transitions across segment boundaries. `Ci` incs a leaf tally at every epoch from `newestEi+1` through the newer table's epoch when it *validates* that table's sentinel — a valid sentinel implies the producer has initialized those segments' stats, so the increment always reads a live `SqCs`. `newestEi` advances to that epoch; `oldestEi` is unchanged. The old position stays held until the confirmed prefix reaches it (5j). `IDc % SqCs` at the increment is the leaf recorded in `ltiRing`; later `IDc` nudges do not retarget it.

Idle frontier migration. `moveRef` on the hot path still runs only after a successful sentinel validate. That is not enough: a spanning table can start in `Ki` and end at a `Wt'` several epochs later, leaving `Ci`'s position on confirmed-complete `newestEi` while `nextSeq` names a later initialized segment whose table is not yet published. `refreshQuota` counts full free segments from the write epoch + 1; a stale pin in that sweep window starves renewal even though the consumer is idle at `Wt`. Therefore, on a sentinel miss at `nextSeq` (and after a consume that advanced `nextSeq` into a new epoch), if the current position is confirmed complete and `Es[ki]` matches `ei` for `nextSeq`'s `(ki, ei)`, `Ci` advances `newestEi` onto that segment and then releases any confirmed prefix. Increment-ahead, always at least one pin, pin tracks the frontier.

`nextSeq` is always `Tnext` of a published table, which is that write's `Wt'`. Crossing writes already release-store `Es` through that epoch (4b), so the segment of `nextSeq` is already initialized. The case one cannot migrate into — `Ki+1` with `Es` unset — only exists when no write has crossed, which means `nextSeq` is still inside `Ki` and the pin is on the write segment, which the quota sweep does not examine. Restricting `Exmax` (for example to half the buffer) does not replace this migration: opportunistic renewal stays inert whenever `Exmax` exceeds a segment, and a smaller `Exmax` only *tolerates* a pin left on a completed start.

Thead's contents are essential for orientation:

- `Tsent` — sentinel computed from the 64-bit sequence pointing at the start of the table. Consumers load-acquire and validate the location and value.
- `Tnext` — the Seq of the next table.
- `Tmt` — uint, the number of multi-threaded payloads.
- `Tlen` — uint, the total number of payloads.
- `Tsize` — the table's total size in ulongs, accounted into `Sd` on completion (5j).
- `AvgCost` — the producer's declared cost class (4a / 5e-g).
- Derived stats the producer can compute as is convenient (`Cs` and `SqCs` used at write time).

### 5e. Consumption Strategy - Primary Work Claims

Finding a table, `Ci` can use a combination of header values and local stats to decide how to consume. The main pathway is this, mapping a shard of consumers to a linear slice of Tindex claimed by chunks on a sharded progress counter:

**a.** `Tprogress` gives an O(1) view of the table's overall completion; `Ci` checks if `Tprogress == Tlen` then all entries are complete and `Ci` skips. The skip covers primary work only; MT payloads can outlive their shards' completion, so a cheap pre-checked MT pass still runs (5i).

**b.** `Tseq` is the table's starting sequence.

**c.** `(IDc + Tseq) % SqCs` yields `Ci`'s shard index `Shi` and counter `Shc` exactly like its leaf index.

**d.** If `Tlen` < the table's small threshold then consumers with `Shi == 0` claim the entire table by chunks as below; the others do not. The threshold is a farm field; 0 selects the auto rule `clamp(SqCs * Chunk, 16, 256)`, so a table shards iff every shard can get at least one full chunk; the default fixed value is 64. All consumers derive the same threshold from the same header/farm values.

**e.** `Tlen / SqCs` and `Tlen % SqCs` yield a base length `Shbase` and `Shrm`; `Shlen = Shi < Shrm ? Shbase + 1 : Shbase`.

**f.** Starting index `Shstart = Shi * Shbase + (Shi < Shrm ? Shi : Shrm)`.

**g.** `Chunk = MAX_CHUNK >> AvgCost`, read from the table header word published by `write()`; `MAX_CHUNK = 32` so the default `AvgCost = 1` lands on 16, the midpoint of `[1, 32]`. Every consumer reads the same header word, so `Shiter` cannot disagree across visitors.

**h.** `Shiter = (Shlen >> log2(Chunk)) + ((Shlen & (Chunk - 1)) > 0 ? 1 : 0)` — `ceil(Shlen / Chunk)`, with explicit grouping since D's precedence would otherwise misparse the remainder test.

**i.** `Ci` gets claim `X = fetch_add(Shc, 2^32) >> 32`. If the claims half wrapped to 0, fatal (4a). If `X >= Shiter` the shard is already fully claimed and we move to step l. `X` is a useful feedback signal for `Ci` to remember for later.

**j.** If `X < Shiter` it maps to a run of Tindex of Chunk length at `Shstart + X * Chunk`. If `X == Shiter-1`, `Ci` truncates the run to the remainder (`Shlen & (Chunk-1)`) if it's nonzero.

**k.** When a `Ci` finishes a run do `Y = fetch_add(Shc, run_length)`; if `Y << 32 == (Shlen - run_length) << 32` then `Ci` completed the shard and adds `Shlen` to `Tprogress` with acquire/release ordering. This bounds the number of mutations to `Tprogress` to `SqCs` and makes the final RMW a join over preceding shard callback writes. The completer additionally gains the *sweeper role* for this table (5i), and if its `Tprogress` add lands exactly on `Tlen` it is the table's finisher and performs the Sd accounting (5j).

**l.** Then `Ci` returns to step i; or if `X >= Shiter` it enters the secondary pathway.

**m.** First-claimant mid-tick yield. The `Ci` that drew `X == 0` for a shard (the first claimant, at most one per shard) may, after finishing a run that did not complete the shard, read the claims half of `Shc`. If it is greater than the number of claims that `Ci` has personally made in this shard, the shard is shared: at least one other consumer has claimed a run here. The first claimant may then probe `Tnext`'s sentinel; if it is live, it may return from the primary pathway immediately instead of returning to step i, so `consumeNext` can advance to the already-published table (for example a mid-tick 1-payload write). The Tcount/`Tprogress` arithmetic is unchanged: the yield happens only after `Y = fetch_add(Shc, run_length)`, so the run is accounted, and the other claimant(s) will finish the shard. Unsubscription after the yield cannot starve the shard; OS pre-emption can only delay it, and the idle re-walk of 5j remains the backstop. Sweeper and re-walk calls (`checkFirst`) never yield early.

Priority of roles: if the same run that would qualify `Ci` for the yield is also the shard-completing run (the 5e-k completion sum lands on `Shlen`), the completer outcome takes precedence — `Ci` gains the sweeper role for this table and does not yield early, so the completer's foreign-shard sweep and MT pass are never lost to a mid-tick advance. The yield is only considered after a run that did not complete the shard. A yielded `Ci` also drops any carried sweeper role (5i): it begins the next table as a normal consumer without role baggage, which preserves the invariant that only a shard completer (or a claimant observing the concurrency signal) advances the sweeper role.

Note the primary path burns exactly one payload claim per payload per table (each payload index lives in exactly one shard's runs), which is what makes the sweeper's guaranteed claim slot in 5i work.

### 5f. Payload Mechanisms

A Payload's physical layout:

- `Phead` — `PayloadHeader`, 16 ulongs = 128 bytes:
  - `MaxCs` (uint) — maximum consumers per payload; 1 for single threaded, >1 for multithreaded. `write()` fatals on `MaxCs == 0` or `MaxCs > 512`.
  - `Done` (uint) — number of iterations to complete; 1 for single threaded, 2..512 granularity steps for multithreaded. `write()` fatals on `Done == 0` or `Done > 512`.
  - `Plen` — payload body length in ulongs.
  - 6 ulongs filler.
  - `Pcount` — 32 MSB claims like Tcount; next 16 MSB number of times Called; 16 LSB number of Calls completed. After any packed fetch_add, if the field just incremented has wrapped to 0 the process fatals. Under the 512 caps, worst-case call increments are about `Done + MaxCs`, far below 2<sup>16</sup>; 32-bit claim wrap still requires pathological visitor accumulation. The hot path stays fetch_add (no CAS saturating loop).
  - `Call` — callback function pointer which decodes Pbody and executes an iteration of work; `alias Callback = long function(PayloadHeader* head, PayloadBody body, ulong iteration)`. Call sits immediately after Pcount so the two share one cache line; safe because Call is dereferenced only by a consumer holding a valid claim — the same thread that just wrote Pcount — so no cross-thread coherence traffic is added. The 6 filler ulongs after Call complete the line in every buffer phase.
- `Pbody` — `PayloadBody` (`const(ulong)[]`); read-only type-erased transport
  words for Call's work. Constness prohibits mutation of the ring body itself;
  it does not imply transitive immutability of an object reached through an
  explicitly encoded handle. Generated shims currently impose the separate,
  stricter rule that packed parameters contain no unshared mutable aliases.

For a `Ci` entering a payload, consumption begins with `Ci` incrementing the claims. If it's below `MaxCs` then `Ci` increments calls which becomes a parameter to Call. Call performs an iteration if the parameter is below `Done` and increments completions. `MaxCs` prevents overallocation of Calling consumers.

For single-threaded single-shot payloads (`MaxCs=1`, `Done=1`) the shard counter already assigns the payload to exactly one consumer, so that consumer keeps only the Pcount claims fetch_add as an entry gate, executes the Call, and leaves the calls/completions fields untouched. Sweeper and idle re-walk paths use the same Tcount counter and therefore cannot claim the same run, and secondary/tertiary MT paths never walk ST payloads. Nothing reads the calls/completions fields for ST payloads, and table completion is tracked by Tcount/`Tprogress`, so those fields stay zero. Multi-threaded payloads keep the full packed fetch_add path.

### 5g. Secondary Work Claims

If `Tmt > 0` there are some payloads that are themselves multithreaded. In the primary pathway, `Ci`'s `Shi` maps it to a slice of the full payload set using the shard counters for iteration. Here `Shi` maps to a round-robin assignment of the multithreaded Payloads using a Payload's internal counter for iteration. The primary path fills in the completion status for the whole table. The secondary path adds work to high value parallel items as consumers run out of serial items.

This structures the consumer strategy such that exactly 1 consumer normally enters a single-threaded Payload, and ~`SqCs` consumers normally enter an MT payload. Tertiary behavior is the sweeper role in 5i.

### 5h. Feedback Potential (Oversaturation)

If `Ci` adds no work in either the primary or secondary path, it was entirely oversaturating the table. `Ci` can additionally track how many gross iterations it contributed between `Shlen` and the Done values of the MT payloads it saw. The value `X` from 5e-i has some useful properties.

Let `Z = X - Shiter`; the iterations are completed, then each `Ci` increments `Shc` one more time and sees the shard is fully claimed. `Z` then is how many consumers actually pass through a shard. If `Ci` observes `Z` significantly over `SqCs`, it can check the Farm level `Cf` and see if the shard is overloaded. `Ci` then nudges its `IDc` by +1. Already-incremented leaves are not moved. On the next table (and the next segment pin) `Ci` lands in a different bucket of consumers. This reduces an overallocated shard via a local feedback signal.

### 5i. The Sweeper Role (Tertiary Behavior)

The consumer whose 5e-k completion sum lands on `Shlen` — adding `Shlen` to `Tprogress` — gains the sweeper role for that table. This pulls one sweeper from each shard, so it takes all shards' completers being gone (hence no consumers and no progress anyway) for work to starve, without forcing much extra contention. The sweeper makes one linear pass:

- **Foreign shards:** for each shard other than its own, a plain load short-circuits fully-claimed shards (`claims >= Shiter`) without an RMW; otherwise it claims and processes runs exactly as in the primary path. The 5e-k completion rule fires for whichever consumer lands the final sum, owner or sweeper, so `Tprogress` mutations stay bounded by `SqCs`.
- **MT items:** a full pass over the MT index with plain-load pre-checks (skip if `calls >= Done` or `claims >= MaxCs`), entering and draining (looping calls until exhausted) anything left. Because a primary visit burns exactly one claim per payload and `write()` enforces `Done > 1 => MaxCs >= 2` (4a), a sweeper always finds a claim slot.

Sweeps of foreign shards do not recursively confer the role. A late visitor finding `Tprogress == Tlen` still runs the pre-checked MT pass, since MT payloads can outlive their shards' completion.

Undersaturation repair (complementing 5h). A sweeper reads the same `Z` value on foreign shards. Finding `Z <= 1` — essentially no native traffic — it nudges its `IDc` by the round-robin distance into that shard, `IDc += (Shi_foreign - Shi_own + SqCs) % SqCs`, patching the hole a pre-empted or unsubscribed native left. Already-incremented leaves stay on the ring; the new `IDc` is the next pin's leaf target. A native laggard waking to an overbalanced shard nudges itself out via 5h. The signal is self-limiting: each entering sweeper's failed exit-claim raises the shard's `Z`, so it disappears after a couple of adopters. A consumer adopts at most one shard per table. Small tables are excluded.

The carried role and small tables. Small tables concentrate all claims on shard 0 (5e-d), so under churn they may have no native consumer at all. A consumer that completed a shard of the previous table therefore carries the sweeper role one table forward: if the next table is small, it sweeps that table's single shard regardless of its own `Shi`, and if the sweep completes the shard the role chains forward again. The idle re-walk of 5j remains the backstop, but the hot path no longer stalls producers waiting for it. A complete table does not clear the carried role: its cheap pre-checked MT pass runs without touching `sweeperNext`, so a role carried across consecutive already-complete tables survives to the first table that still needs sweeping.

### 5j. Segment Completion Tracking and Reference Retention

Consumers retain references until completion is *confirmed*, using the consumed-size accumulator `Sd` (2b): the table's finisher (the `Ci` whose `Tprogress` add lands on `Tlen`) adds `Tsize` to `Sd` of the table's starting segment, then invokes a nonzero table-completion hook. `Sd` is zeroed by the producer when its reservation crosses into the segment for a new epoch, before the release-store of `Es`. The finisher provably still holds a reference on the segment (it was unconfirmed until this moment), so its accounting can never race a producer's re-zeroing.

A held epoch `Ei` (physical `Ki`) is released from the old end of the range when:

- `Es[Ki] == Ei` — the slot still names this epoch,
- `Es[Ki+1] == Ei + 1` — the next segment is initialized, so no more tables can start in `Ki`,
- `Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1]` — the span is fully consumed.

Only `oldestEi` advances; a confirmed interior stays pinned until it is oldest. Confirmation requires `Es[Ki] == Ei` as well as the next-segment test: a wrap that overwrote the slot is not "confirmed."

The target is exact: tables are contiguous, so the sizes of all tables starting in `Ki` sum to precisely `Seqt[Ki+1] - Seqt[Ki]`, and `Sd` accrues each table's size exactly once, only on completion. A large table completing ahead of a small one leaves the small one's size missing and cannot force the crossing. A segment completely spanned by one table gets a `Seqt` at or past `Seqt[Ki+1]` and confirms trivially. While any reference on `Ki` is held, no producer can lap past it, so the next segment's stats remain at epoch `Ei+1` once initialized. (An earlier draft used an arithmetic boundary plus a signed outstanding balance `Sbal` incremented by producers and decremented by finishers; that had a producer-producer race — one producer initializing the segment could zero the balance after another had already incremented it for a table starting there — and a stale-balance deadlock mode. Comparing against `Seqt[Ki+1]` removes the balance entirely.)

The retained-reference invariant: every consumer holds a reference on every segment it has entered but not yet confirmed complete. An incomplete segment is therefore protected by every passerby still subscribed. Protection does not end when those passerby unsubscribe. The last *releaser* of an unconfirmed segment — not only the last unsubscriber of the farm — leaves Sub0 via the last-releaser helper of 2a/5b. New subscribers land at the earliest `Rtlow > 0`, which is the earliest such pin, and drain the backlog exactly once. A non-last unsubscriber may drop its references; if it was the last pin on an early incomplete segment, Sub0 stays behind. Planting Sub0 only at `Cf == 1` on the last unsubscriber's own earliest held ref is not enough: that last unsubscriber may never have entered the orphan.

The position (`newestEi`) participates in the same confirmation story. After a spanning table, or on the idle path, a confirmed position is advanced onto the initialized segment of `nextSeq` (5c) and the old pin remains in the range until the confirmed prefix reaches it. An idle subscribed consumer is therefore always pinned on the frontier, never on a completed interior segment as its `newestEi`.

Consumers consult `Sd` only on position changes and on idleness, never per payload: after each table advance, at unsubscribe, and on the idle path below.

Deadlock freedom and the idle re-walk. If producers stall on a held trailing reference, consumers catch up to `Wt` and their next-table sentinels stop validating. On that idle path a consumer first advances a confirmed position onto `nextSeq`'s segment (5c), then re-walks its oldest unconfirmed epoch from `Seqt` if `Es` still names that epoch, claiming any leftover runs in every shard and draining MT items (the data is guaranteed present and intact while `Es` matches: the consumer's own held reference forbids overwrite), then releases the confirmed prefix. It also re-walks its current position segment: a per-consumer cursor (`sweepSeq`) remembers the sequence up to which that segment has been drained, so each park resumes from the cursor instead of rescanning the whole segment — O(new tables) per miss rather than O(tables in the segment). The cursor resets whenever the position moves to a new segment (`moveRef`). Blocking creates the idleness that resolves it. If refresh still fails, a producer may itself subscribe and drain (3a).
