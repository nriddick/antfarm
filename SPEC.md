# Ant Farm: An M:N Concurrent Queue with Superlative Scaling

Living spec, matching the implementation in this directory. The farmer metaphor and the field names are the original; the structure is the current one — held-epoch pins, last-releaser Sub0, ticketed write, pin-before-read subscribe — written as the design, with the concurrency arguments that accumulated during bug-fixing kept beside the rules they justify.

- [1. Model, Scope, and Terminology](#1-model-scope-and-terminology)
- [2. Data Layout](#2-data-layout)
  - [2a. Farm-level Fields](#2a-farm-level-fields)
  - [2b. Segment Tallies](#2b-segment-tallies)
  - [2c. Segment Statistics](#2c-segment-statistics)
  - [2d. Payload Layout](#2d-payload-layout)
  - [2e. Table Layout](#2e-table-layout)
  - [2f. Compact ST Tables](#2f-compact-st-tables)
  - [2g. Uniform-Call ST Tables](#2g-uniform-call-st-tables)
- [3. Safety Invariants](#3-safety-invariants)
- [4. Construction and Registration](#4-construction-and-registration)
  - [4a. Geometry and Quota Constraints](#4a-geometry-and-quota-constraints)
  - [4b. Epoch 0](#4b-epoch-0)
  - [4c. Consumer Capacity](#4c-consumer-capacity)
  - [4d. Producer Tiers and Tickets](#4d-producer-tiers-and-tickets)
- [5. Producer Protocol](#5-producer-protocol)
  - [5a. Quota Arithmetic and Sweeping](#5a-quota-arithmetic-and-sweeping)
  - [5b. write() Call and Return](#5b-write-call-and-return)
  - [5c. Reservation and Publication](#5c-reservation-and-publication)
- [6. Consumer Pin Lifecycle](#6-consumer-pin-lifecycle)
  - [6a. ConsumerView](#6a-consumerview)
  - [6b. Subscription](#6b-subscription)
  - [6c. The Sub0 State Machine](#6c-the-sub0-state-machine)
  - [6d. Orientation and Frontier Migration](#6d-orientation-and-frontier-migration)
  - [6e. Unsubscription](#6e-unsubscription)
- [7. Work Scheduling](#7-work-scheduling)
  - [7a. Primary Work Claims](#7a-primary-work-claims)
  - [7b. First-Claimant Mid-tick Yield](#7b-first-claimant-mid-tick-yield)
  - [7c. Secondary Work Claims](#7c-secondary-work-claims)
  - [7d. Oversaturation Feedback](#7d-oversaturation-feedback)
  - [7e. The Sweeper Role](#7e-the-sweeper-role)
- [8. Table Accounting and Segment Reclamation](#8-table-accounting-and-segment-reclamation)
- [9. Memory-Order Contract](#9-memory-order-contract)
- [10. API, Errors, and Implementation Profile](#10-api-errors-and-implementation-profile)
- [11. Rationale and History Appendix](#11-rationale-and-history-appendix)

---

## 1. Model, Scope, and Terminology

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. The last ant to leave a *confirmed-complete* area changes the tally (a reference counter) to zero. The last ant to leave an *incomplete* area instead leaves a Sub0 mark, so the farmer still sees a nonzero tally and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

The implementation of this idea: a circular buffer using the "magic buffer" memory mapping such that the second half of the buffer wraps the first half. The buffer has a power-of-two length `Ln` in sizes to fit the mapping requirements of Linux and Windows and uses 64-bit `ulong` as its base unit. The buffer has a power-of-two number `K` (probably 4 or 8) of segments at regular intervals, each segment representing a successive epoch `E` of data. A sequence number `Seq` is the element number of the buffer as it wraps around; `Seq & (Ln-1)` gives the array index for that sequence. `Seq * K / Ln` is the epoch for that Seq. `Ei & (K-1)` is the segment index `Ki` for that `Ei` and span of Seqs.

### Terminology

A few words are used in different enough ways that they need exact names here. Requirements vocabulary: **must** marks safety/API requirements, **may** marks allowed behavior, and "current implementation" marks tunables.

- **Physical segment** or **slot** — the array index `Ki = Ei & (K-1)`, the home of one generation of segment metadata.
- **Logical epoch** — `Ei`, the generation number stored in and expected from `Es[Ki]`.
- **Leaf** — one of the `Lt` fan-out counters for a physical segment. Do not confuse with a *work shard*.
- **Work shard** — a slice of a table's Tindex, `Shi`, claimed through one `Tcount` counter.
- **Producer** — a registered `write()` caller. **Consumer** — a subscribed `ConsumerView` owner.
- **Reservation** — the span of sequences a producer has claimed from `Wt`. **Quota** — the producer's permission to reserve without first sweeping for consumer pins.
- **`segCap`** — `Ln / K`, the capacity of one segment in ulongs. **`segShift`** — `log2(segCap)`, the shift that maps a sequence to its epoch. **`Lmask`** — `Ln - 1`, the index mask.

The qualified completion states:

| Proposed term | Exact condition | What it does and does not establish |
| --- | --- | --- |
| shard primary-complete | the low half of that shard's `Tcount` has accumulated `Shlen` | Every primary Tindex run in the work shard returned and was accounted. |
| table primary-complete, or table accounted | `Tprogress == Tlen` | Every work shard is primary-complete; the table size may be added to `Sd`. This is not a join on every concurrent MT callback. |
| MT calls issued | `pcount.calls >= done` | Every MT iteration has been claimed for execution. Some callbacks can still be in flight. |
| MT callbacks returned | `pcount.completions == done` | Every issued MT callback has returned. The ST fast path deliberately does not maintain these two fields. |
| segment confirmed | matching `Es` values and `Seqt + Sd >= SeqtNext` | No more primary table accounting is missing from the segment. Confirmation alone does not make the bytes reusable. |
| slot admitted by a quota sweep | a producer observes `Rt == 0` for that following slot while scanning contiguously from its tail anchor | No consumer, subscriber, or Sub0 pin blocks the sweep there. In this reuse context, the pulse and contiguous-sweep invariants make zero sufficient; the producer does not recheck `Sd`. A current write-frontier epoch may be incomplete with `Rt == 0` because it is not a following slot being reclaimed, so `Rt == 0` is not globally synonymous with confirmation. |

### Synchronization, runtime, and error policy

Pains are taken to *eliminate* CAS operations from the hot paths of the module. Hot-path synchronization is fetch-add / fetch-sub plus acquire/release load/store. The CAS sites are cold: producer-ticket claim, the last-releaser pulse on unsubscribe, and `plantIfUnprotected`'s plant / retract. They do not retry the same expected snapshot indefinitely; every failure reloads and revalidates the reason for the operation, and often the reload invalidates the reason to plant — count or pulse already present — so the loop bails or takes a different action against the new snapshot. The current implementation uses a *strong* CAS (D's `cas` does not fail spuriously). Whether a weak CAS would suffice is a rationale question, not a normative requirement; see 11.

The reference implementation is in D and the Farm must be allocated into 64-byte-aligned memory with manual lifetime; at the time of writing D's garbage collector only guarantees alignment to 16 bytes (10). Just about every interface to the Farm is `@nogc nothrow @system` code, and this system is designed such that most errors are fatal with very few exceptions. The recoverable exceptions are registration-full, oversubscription, an uninitialized or generation-inconsistent subscribe frontier, and backpressure.

Throughout, I use conceptual `fetch_add`/`fetch_sub` for the implementation's `atomicFetchAdd`/`atomicFetchSub`.

---

## 2. Data Layout

### 2a. Farm-level Fields

The shared mutable Farm fields:

- `Cf` — the current number of subscribed consumers.
- `Reqs_c` — a monotonically increasing counter of consumer subscription requests. `IDc` is taken from this counter alone, so producer registration cannot fragment consumer shard assignment.
- `Reqs_p` — a monotonically increasing counter of successful producer registrations (4d). It is separate from `Reqs_c` so Token hashes do not walk the consumer ID space.
- `FarmId` — a process-unique identity taken from a global issuance counter at Farm creation and never reused for the process lifetime. Every Token carries a copy, binding it to its issuing Farm (4d).
- `Prbulk` — the current number of bulk producers.
- `Prsm` — the current number of small producers.
- `Wt` — the write tail sequence.

Plus the producer ticket slot arrays of 4d, and miscellaneous immutable stats and derived stats as are convenient.

### 2b. Segment Tallies

There are two preallocated K-length circular buffers of segment metadata: tallies (reference counters) and snapshots of general stats. The tallies are modified by initializing producers, consumers, and subscribers. There is a root tally `Rt` and an array of leaf tallies `Lt` for each segment `Ki`. `Rt` tracks active consumers and prospective subscribers (6b). Let `Cs` be the segment's number-of-consumers snapshot, `SqCs` that value's square root, and `IDc` be a consumer `Ci`'s ID value. The square root function used throughout the module returns a minimum value of 1 for `Cs <= 2`; it balances consumers across leaves and prevents divide-by-zero.

`IDc % SqCs` gives the `Lti` for that `Ci` and `Ki` *at the moment of increment*. If the *actual* number of consumers differs greatly from `Cs`, or the span of IDs is fragmented and/or bunched up, performance may degrade but safety is inviolate. Enough Lts are preallocated for an enum `MAX_CONSUMERS_LIMIT = 128` — the ceiling square root of which is 12 (`MAX_LEAVES`).

`Ci` claims a reference to `Ki` by fetch-incrementing its `Lti`, and relinquishes by fetch-dec. Leaf RMWs are acq_rel. When the operation is an edge transition to/from zero — the inc() returns 0 or the dec() returns 1 — `Ci` repeats that operation on `Rt` with release semantics. A consumer that is not last on its leaf never writes `Rt`; acq_rel on the leaf puts that consumer's ring reads in a release sequence the last-on-leaf's `Rt` release (and a later producer acquire of `Rt == 0`) can carry.

Consumers **must** always increment a tally ahead before decrementing their current one. This ordering leaves no gap where a producer might spuriously observe an `Rt` with a zero value. Producers load-acquire `Rt` and regard segments with nonzero Rts as contributing no available space.

`Rt` is a bitfield:

| Bits | Field | Unit | Role |
| ---: | --- | ---: | --- |
| 63..32 | `Sub` | `2^32` | prospective subscribers (6b) |
| 31..16 | `Sub0` | `2^16` | pulse pin on incomplete / empty-farm space |
| 15..0 | count | 1 | active consumers; `COUNTMASK = Sub0 - 1` |

Let `Rtlow` be the least significant half (`LOWMASK`). `COUNTMASK` is the count bits below Sub0. Underflow checks on `Rt` test only `COUNTMASK`.

Sub0 is a counted pin in units of `2^16`, not a bit. At rest each segment has 0 or 1 × Sub0; transiently more than one unit may be present while several last-leaf releases race, still bounded well below `Sub` at `MAX_CONSUMERS_LIMIT`. The count field and Sub0 field never overlap, and `Rtlow` underflow tests are built around that. OR and AND are not used on this field: they cannot share accounting with the counted last-releaser chain (6c).

Because a segment's `SqCs` snapshot changes across epochs, a `Ci` must cache the `Lti` used at each increment and decrement that same leaf. The ConsumerView keeps one `Lti` per held physical slot (`ltiRing`, 6a). `IDc` may later be nudged (7d, 7e); already-incremented leaves are not moved.

All leaf counters subject to dec are signed integer types, and underflow below 0 is fatal in any context. The root tally is unsigned; its underflow is detected on the count bits via `COUNTMASK`, so a live Sub0 can never mask a genuine count underflow.

The full root/leaf propagation protocol — taking a count, the last-releaser CAS, and the Sub0 lifecycle — is specified once in 6c.

### 2c. Segment Statistics

The second set of segment metadata:

- `Es` — the epoch for that segment.
- `Seqt` — the sequence that points to the first valid payload table for that segment.
- `Cs` — a snapshot of the number of consumers.
- `SqCs` — from 2b, and other derived statistics as are convenient.
- `Sd` — the accounted-table-size accumulator: the sum of the sizes of all primary-complete tables starting in this segment. See 8.

When a producer `Pi`'s space reservation transitions across one or more segments, `Pi` initializes these values such that a load-acquired `Es` matching a `Ci`'s expected value verifies the others. This is the same publication pattern as the table sentinel (5c, 9): `Es` is store-released last, after Seqt/Cs/SqCs/Sd are stored raw, so an acquire-load of `Es` that matches the expected epoch also orders the subsequent raw header-metadata reads. A valid `Es` therefore implies valid header metadata for that epoch, exactly as a valid table sentinel implies valid table contents. `Sd` is zeroed at the same time, before the release-store of `Es`. Only consumers holding a reference ever add to `Sd`, and only after validating a sentinel released after initialization, so the zeroing can race nothing.

### 2d. Payload Layout

A Payload's physical layout:

- `Phead` — `PayloadHeader`, 16 ulongs = 128 bytes:
  - `MaxCs` (uint) — maximum consumers per payload; 1 for single threaded, >1 for multithreaded. `write()` fatals on `MaxCs == 0` or `MaxCs > 512`.
  - `Done` (uint) — number of iterations to complete; 1 for single threaded, 2..512 granularity steps for multithreaded. `write()` fatals on `Done == 0` or `Done > 512`.
  - `Plen` — payload body length in ulongs.
  - 6 ulongs filler.
  - `Pcount` — 32 MSB claims like Tcount; next 16 MSB are `calls` (iterations issued); 16 LSB are `completions` (iterations returned). After any packed fetch_add, if the field just incremented has wrapped to 0 the process fatals. Under the 512 caps, worst-case call increments are about `Done + MaxCs`, far below `2^16`; 32-bit claim wrap still requires pathological visitor accumulation. MT admission uses fetch_add (no CAS saturating loop); the unique ST visitor records its claim with a raw store (7c).
  - `Call` — callback function pointer which decodes Pbody and executes an iteration of work; `alias Callback = long function(PayloadHeader* head, PayloadBody body, ulong iteration)`. `Call` sits immediately after `Pcount` so the two share one cache line; safe because `Call` is dereferenced only by a consumer holding a valid claim — the same thread that just wrote `Pcount` — so no cross-thread coherence traffic is added. The 6 filler ulongs after `Call` complete the line in every buffer phase.
- `Pbody` — `PayloadBody` (`const(ulong)[]`); read-only type-erased transport words for Call's work. Constness prohibits mutation of the ring body itself; it does not imply transitive immutability of an object reached through an explicitly encoded handle. Generated shims currently impose the separate, stricter rule that packed parameters contain no unshared mutable aliases.

In prose I use packed-field helpers `claims32(x)`, `calls16(x)`, and `returned16(x)` rather than repeating the shifts.

`write()` validates every payload header before accepting it. `Done` and `MaxCs` are in 1..512. `MaxCs >= 1`, and `Done > 1` requires `MaxCs >= 2` — a multithreaded payload must admit at least two entrants, because a primary visit burns exactly one claim per payload (7a) and draining entrants may already own the other admission (7e). `MaxCs > 1 && Done == 1` is legal and MT-indexed even though only one call can win; the implementation accepts it.

### 2e. Table Layout

Producers commit a table `Ti` of Payloads up to `Exi` in total size, with sections for:

| Offset (ulongs from table start) | Contents |
| --- | --- |
| `[0 .. 8)` | Thead. Word 0 is `Tsent` (store-released last). 1 `Tnext`. 2 `Tmt<<32 \| Tlen`. 3 `Cs`. 4 `SqCs`. 5 `Tsize`. 6 `Tkind<<32 \| AvgCost` (`Tkind` is 0 except for uniform tables, 2g). 7 an optional stable `TableCompletionHook*` (zero for ordinary writes). |
| `[8 .. 8+Tlen+Tmt)` | Tindex: dense index of where each Payload sits. Total index first, MT index second. `Tmt` is the number of multithreaded payloads actually written; `Tlen` the total. Uniform tables (2g) insert their Thead extension words before Tindex, and the index-free kind has no Tindex. |
| + 7 | padding |
| + 1 | `Tprogress`. When a `Ci` identifies itself adding the final completion sum for its shard, it increments `Tprogress` by the shard length with acquire/release ordering. |
| + 7 | padding |
| then | `Tcount`: combination claims/completions counters. Consumers fetch_add the 32 most significant bits as claims and 32 LSB as completions. There are `SqCs` such counters, each followed by 7 ulongs of padding. After any packed increment, if the field just incremented has wrapped to 0 the process fatals rather than continue with a corrupted counter. A committed table carries fewer than 2^31 payloads, so successful primary claims on one shard stay below 2^31 even in the `AvgCost` scenario that shrinks `Chunk` to a single payload per claim. Wrapping the 32-bit claims half therefore still requires pathological visitor accumulation on one shard; in the largest tables the table itself is pathologically large as well. |
| then | the Payloads laid sequentially (compact `[Call][Pbody]` records when `Tmt == 0`, 2f; bare `[Pbody]` records in uniform tables, 2g). |
| last 7 | padding at the end of the table. |

Tindex entries are payload offsets relative to the table start; `Tnext` and `Tsent` are absolute sequences.

`Tlen` is always below 2^31: a table inserts fewer than 2^31 payloads.

`Tmt` is populated when `MaxCs > 1`, matching the MT index; single-threaded payloads appear only in the total index.

### 2f. Compact ST Tables

A table whose committed payloads are all single-threaded (`Tmt == 0`) and whose `Tkind` is 0 stores each payload as a compact record instead of `Phead` + `Pbody` (an all-ST table whose accepted prefix, within one write call, shares one `Call` is a uniform table instead, 2g):

| Offset (ulongs from the Tindex entry) | Contents |
| --- | --- |
| `0` | `Call` |
| `[1 .. 1+Plen)` | `Pbody` |

Nothing else of `Phead` is stored. Thead, Tindex, Tprogress, Tcount and the end pad are unchanged, and Tindex still holds each record's offset. `Plen` is not stored; the reader derives it from the existing index. Records are emitted contiguously in Tindex order, so `Plen_i = Tindex[i+1] - Tindex[i] - 1`, and the last record ends at `Tsize - 7` (the end pad), so `Plen_{Tlen-1} = Tsize - 7 - Tindex[Tlen-1] - 1`. A one-word body therefore costs 3 ring words per entry (index, Call, body) instead of 18.

**Why nothing else is needed.** `Tmt == 0` means every payload has `MaxCs == 1`, so `Done == 1` (2d) and every payload takes the ST single-shot path (7c). On that path the only mutable per-payload word is `Pcount`, and it is read by nothing but the running callback. Completion is table-level (Tcount, `Tprogress`, `Sd`, the hook). The Tcount claim makes the run's consumer the unique visitor of every payload in the run. No path takes over a claimed run, and secondary/tertiary visitors walk only the empty MT index. Callback return values are ignored, so there is no iterating payload.

**Writer.** The writer selects the format once per table from the accepted prefix's `m` (and, for 2g, its Calls and lengths). Sizing uses `1 + Plen` per payload while the prefix is compact and `16 + Plen` once it contains an MT payload (2g gives the uniform sizes), so a heterogeneous range keeps its greedy accepted prefix. Records are raw-stored like every other ring word, and `Tsent` is still release-stored last. Quota, wrap, `prepareTableFrontN`, `writeTracked` rollback and publication are unchanged (5b, 5c).

**Singleton contract check (5b.3).** An ST payload's singleton table is always compact-sized: it is written as a uniform table (2g), which costs exactly what the compact singleton costs, so its contract check uses the compact size, `base + 2 + Plen`. MT payloads keep the full-header size. Compared with the full layout, an ST payload may be up to 15 words larger before `write()` fatals. Nothing previously legal becomes fatal, and `write() == 0` still means full. In a heterogeneous range, an ST payload that fits only as a compact singleton ends the accepted prefix after a preceding MT payload, because adding it would need the full format. After a quota refresh if needed, the next call starts its prefix with that payload as an all-ST (uniform or compact) table, possibly followed by more payloads.

**Reader.** Every consumer path (primary, carried sweeper, sweeper, oldest-trailing and current-position re-walks) reads `Tmt` from Thead word 2 and `Tkind` from word 6, and selects the compact interpretation once per claimed run, not per payload. The callback still receives a `PayloadHeader*`. For a compact record it points to a consumer-local stack copy holding the values the ST fast path exposed through the ring header: `MaxCs = Done = 1`, `Plen`, `Call`, claims 1, calls/completions 0. The invariant fields, including zero filler, are filled once per run, and `Plen` and `Call` once per payload. The run's payloads share the copy, so callbacks must not modify it, and they must not retain the pointer past return (a ring header pointer used to stay valid until the table was recycled). Tables with `Tmt != 0` keep the full layout and paths unchanged.

**Known cost.** Compact tables admit several times more entries per quota, which shifts quota admission geometry and with it allocator cross-thread-free contention. At six consumers the private-remote churn cell moves against the full layout; that is an accepted trade-off, and the measured compact-path rates and allocator geometry are recorded in [PERFORMANCE.md](PERFORMANCE.md#allocators).

### 2g. Uniform-Call ST Tables

An all-ST table (`Tmt == 0`) whose committed payloads all carry the same `Call` stores that `Call` once, in a Thead extension, and each payload as its bare `Pbody`. `Tkind`, the high half of Thead word 6, names the kind. The low half stays `AvgCost`, so the chunk rule (7a) reads it unchanged.

| `Tkind` | Name | Extension (from word 8) | Tindex | Records |
| --- | --- | --- | --- | --- |
| 0 | per-payload `Call` | none | 2e / 2f | 2d `Phead` + `Pbody`, or 2f `[Call][Pbody]` |
| 1 | uniform `Call` | `Tcall` | `Tlen` body offsets | `[Pbody]` |
| 2 | uniform `Call` and length | `Tcall`, `Tblen >= 1` | none | `[Pbody]`, each `Tblen` words |

A nonzero `Tkind` implies `Tmt == 0`; the MT readers (7c) rely on it, because they locate the MT index at `Tindex + Tlen`.

Tprogress, Tcount and the end pad follow the extension and Tindex exactly as in 2e. In kind 1, `Plen_i = Tindex[i+1] - Tindex[i]`, and the last record ends at `Tsize - 7`. In kind 2, body `i` starts at the payload area start plus `i * Tblen`, and that start is the Tcount start plus `8 * SqCs`. A one-word body costs 2 ring words per entry in kind 1 (index and body) and 1 in kind 2, against 3 compact (2f) and 18 full (2d). Kind 0 tables are byte-identical to 2e/2f.

**Detection.** The writer sizes every accepted prefix by the layout it would emit. Layouts only degrade as the prefix grows (kind 2 → kind 1 → compact → full), and each step costs more words, so the greedy accepted prefix rule of 2f still holds. A prefix of a uniform range is uniform. When the next payload would degrade the layout and no longer fits, the table keeps the layout it was sized with. Detection is per write call:

- The `PayloadEntry` and pair forms compare each `Call` with the first.
- The forms without a supplied body length compare each body length with the first. A zero first length keeps kind 1, so no table is index-free over empty bodies.
- The common-header broadcast forms and `writeTracked` share their `Call` by construction.
- The common-header form with a supplied body length (including `writeTracked`) is kind 2 by arithmetic, or kind 1 for length 0. Sizing never inspects a body front; emission fatals on a body of another length, as before.

**Singleton contract (5b.3).** An ST singleton is a kind 2 table, or kind 1 when its body is empty. Either way it costs `base + 2 + Plen`, exactly the compact singleton, so the contract check and the set of legal payloads are unchanged.

**Reader.** Every consumer path decodes `Tkind` once per table, alongside `Tmt`, and selects the record interpretation once per claimed run. The callback receives the same consumer-local scratch header as 2f: `MaxCs = Done = 1`, `Plen`, `Call = Tcall`, claims 1, calls/completions 0. `Call` is set once per run, and so is `Plen` in kind 2. The same lifetime rules apply: callbacks must not modify the header or retain it past return.

**Why it is safe.** The Tcall, Tblen and kind words are written with relaxed stores before the `Tsent` release, like every other Thead word, and read with relaxed loads after the `Tsent` acquire (I4). Quota, wrap, `prepareTableFrontN`, `writeTracked` rollback and publication are unchanged (5b, 5c). Claims, completion and the hook are table-level as in 2f, so nothing per-payload is mutable.

---

## 3. Safety Invariants

These are the load-bearing rules. Later sections describe mechanisms; this section is the short form of why they hold.

**I1 — Quota is strictly less than one lap.** `Exmax = maxBulk*quotaBulk + maxSmall*quotaSmall`, and `Exmax <= (K-1)*segCap` is enforced at construction. Producers only ever write within quota verified by sweeps of contiguously-forward zero Rts, and total maximum quota is bounded by K-1 segments' capacities, strictly less than a lap.

**I2 — Consumer pins form a contiguous epoch range and move increment-ahead.** A consumer holding any tally protects not just that segment but everything ahead of it within one lap: any sweep far enough ahead to threaten the consumer's unread data must pass contiguously through the held segment and break. Conversely, a producer cannot jump a lap on blind quota alone, so the protection cannot be outflanked. Consumers always increment a tally ahead before decrementing their current one, so the protected span has no zero gap.

**I3 — Incomplete unowned epochs remain protected from reuse.** The last
releaser of an unconfirmed epoch replaces its last count with Sub0 atomically
(6c), so abandoned work has no zero gap. A crossing producer covers a
never-entered epoch before publishing its own table. A current frontier or
the producer's pre-plant window can have `Rt == 0`; this is not permission
to reclaim it. A qualifying contiguous sweep is still blocked by an earlier
held pin or the empty-farm pulse under I1/I2. Thus the invariant concerns
admission for reuse, not every isolated zero-root observation. Confirmation
rechecks on last-releaser pulses avoid retaining a confirmed non-empty-farm
segment. Idle consumers keep a live frontier pin (6d), and the last-of-farm
release retains a subscribe anchor.

**I4 — Raw ring reads occur only after publication and while pinned.** The ring is read without acquire only after an acquire-validated `Es` or `Tsent` publishes the initialized values, and only while the reader holds a tally on the relevant segment (9).

**I5 — Producer reuse requires a contiguous sweep observing `Rt == 0`.** Confirmation says table accounting is complete; it does not itself grant reuse. A producer admits a slot for reuse only when `refreshQuota`'s contiguous forward sweep observes `Rt == 0` for that slot. The sweep's anchor and contiguity, plus I2 and I3, make zero sufficient there.

Safety argument in one paragraph: producers only ever write within quota verified by sweeps of contiguously-forward zero Rts, and total maximum quota `Exmax` is bounded by K-1 segments' capacities, strictly less than a lap. A consumer holding any tally therefore protects not just that segment but everything ahead of it within one lap. Incomplete epochs remain protected against qualifying reuse sweeps, and confirmed non-empty-farm epochs do not retain Sub0. Idle consumers keep a live pin on the frontier segment, so the system never drops to zero pins while anyone is still subscribed. Consumers retain references until segments are confirmed (8). Consumers' unread data is always protected, and the system never overwrites unexecuted work.

---

## 4. Construction and Registration

### 4a. Geometry and Quota Constraints

`create` takes the ring size as `ringMiB` and derives `Ln = ringMiB << 17` ulongs. It enforces:

| Constraint | Current value | Kind |
| --- | --- | --- |
| `K` in `[2, KMAX]` | `KMAX = 16` (section 1 suggests 4 or 8) | implementation limit, power-of-two |
| `ringMiB` power of two in `[2, MAX_RING_MIB]` | `Ln >= 2^18` ulongs (2 MiB); `MAX_RING_MIB = 65536` (64 GiB) | implementation limit; smaller rings were never studied, and the upper bound rejects a length mistakenly given in ulongs |
| `segCap` floor | 2048, subsumed by `Ln >= 2^18` with `K <= 16` | keeps per-table header/pad space from dominating a segment |
| `Exmax <= (K-1)*segCap` | enforced | algorithmic invariant (I1) |
| quota role | `quotaRole > 0` iff `maxRole > 0` | algorithmic invariant |

An enabled tier must declare a positive quota. Bulk auto-defaults to `segCap` when unspecified (legacy convenience); small fatals on 0. A disabled tier's quota is normalized to 0 and takes no part in `Exmax`.

`Exmax = maxBulk*quotaBulk + maxSmall*quotaSmall`, the maximum simultaneous excursion of the Farm's maximum set of producers.

### 4b. Epoch 0

At Farm creation we prevent a state where all segments have no references and producers might happily loop through the buffer with no one to block them. There are also zero known subscribers and no leaves or shards in which to distribute them. Thus we construct epoch 0's `Rt` with a dummy Sub0 value, blocking producers from overwriting Seq 0 until consumers digest it. The first subscriber's leaf 0→1 on `Rt` (6c) subtracts that dummy Sub0. On a fresh farm the first subscription should succeed — the construction guarantees it — and a failure (oversubscription or an uninitialized frontier) returns a negative code per 6b; it is not itself fatal.

Note that Sub0 does not block producers from writing to epoch 0. The segment where a write tail resides is not breached if there are consumers ahead of it, and epoch 0 begins with a full segment length ahead of the write tail and consumers bounded behind it.

### 4c. Consumer Capacity

A subscriber `Si` calls `F.add_consumer()`, which fetch_incs `Cf`. If a slot is available, it fetch-incs `Reqs_c` and returns that result as `IDc`. If not, `F` is oversubscribed and `Si` fetch-decs `Cf` and returns `SUBSCRIBE_FULL`. Every subsequent subscription failure path also unwinds `Cf` (6b).

### 4d. Producer Tiers and Tickets

The Farm uses two tiers of producer to cap the max excursion, so one or a few enumerated batch writers can commit large chunks and the thread pool can occasionally write a couple kilobytes at a time. The single write tail is a linear point of contention and it's on a performant developer's shoulders to control the frequency of adds to it. (Though testing has revealed batch sizes reasonably above 1 to be the most crucial aspect.) Important invariant: the number of *actual* simultaneous producers **must** not exceed the maximums to prevent stomping consumers.

Registration is how that invariant is enforced. The Farm preallocates two 64-byte-strided slot arrays, one for each producer tier. A Farm-local blocking mutex serializes complete registration and deregistration transactions, including the hash, authoritative quota ledger, and tier counter. Publishing and consuming do not acquire it. This prevents reuse of a released hash before the departing producer finishes clearing its ledger.

`registerProducer(tier)` checks the tier capacity, assigns a hash derived from the slot and `Reqs_p`, stamps the token with the Farm's `FarmId`, initializes the quota ledger to zero, and returns a valid Token with zero remaining quota. Its first nonempty write must obtain a checked grant through the same forward sweep as every later renewal. `unregisterProducer(ref Token)` validates the token's Farm provenance and live slot, clears its hash and ledger, invalidates the token, and releases the tier capacity while holding the mutex.

`write()` takes the Token by reference and fatals if the token's `FarmId` is not the receiving Farm's, if the hash does not match the live slot, or if the token's private quota mirror exceeds the farm's ledger for that slot. Unregistered threads therefore cannot call `write()` and cannot bypass `Exmax`, a registered caller cannot forge a fresh quota past its grant by mutating the token, and a token registered on one Farm — including a since-destroyed Farm whose identity is never reissued — is fatal on every other Farm. The slot hash alone does not prove ownership: two Farms with identical registration histories derive identical hashes, so provenance is a separate deterministic field, not another probabilistic check.

Tokens are single-owner and transfer-only: copying consumes the source (the copy constructor release-stores the valid hash last, then clears the source's hash), so at most one live token exists per slot and a stale copy fails `requireToken`. When a producer leaves, it can pass its quota along only by transferring the Token; there is no undocumented inherited quota.

---

## 5. Producer Protocol

### 5a. Quota Arithmetic and Sweeping

Let `Exi` be a producer `Pi`'s running quota of ulongs: the amount it may blindly reserve before probing its actual position or checking for consumer references. Let `quotaLeft` be the token's current remainder of that grant, and let a reservation be the specific span a `write()` claim covers. `Exmax` is the sum of all tier grants (4a); each `Pi` regards `Exmax` as a runoff zone it must observe before renewing `Exi`.

Every quota grant requires a fresh acquire-load of `Wt` followed by a forward-segment sweep from that anchor. This applies to the first grant after registration as well as later renewals. A successful reservation only subtracts its table size from the remaining grant. There is no automatic renewal based on the reservation's returned tail or the amount of space remaining in its segment.

Once a sweep grants quota, the producer may spend that finite balance across table writes and segment boundaries. If no next payload fits the balance, `write()` probes `Wt` and attempts a new sweep; a failed sweep returns 0 without granting additional space. There is no separate unchecked-leftover state or `quotaSwept` flag. Reservations of `Wt` are acq_rel RMWs. Release pairs with the acquire probe used for each grant. Acquire is the producer-to-producer reclamation handoff: a producer may spend a retained balance long after its sweep, while other producers sweep and lap the ring. Its reservation must acquire the history those producers imported (consumer pin drops → `Rt` release → zero-`Rt` sweep acquire) before it stores into the words it reserved (9).

If a `Pi` has depleted its swept quota, probed its `Wt` position, and needs to check following segments to make `Exmax` space, then it scans the K-1 segments ahead of the probe for contiguous 0 Rts, adding their full capacities until it has found `>= Exmax` space or encounters a live segment and breaks. The same acquire-load is the sweep's anchor; a second probe is not taken. If sweeping fails then backpressure is simply too high; full for `Pi`'s purposes. A stalled producer may subscribe a ConsumerView, drain, and retry `write()` — that is the supported escape hatch, not an out-of-band trick. The sweep cannot land on spuriously orphaned islands of space because consumers maintain continuous reference spans without gaps.

### 5b. write() Call and Return

Arguments:

- `Payloads` — a slice or forward range of `PayloadEntry` structs. `write()` saves independent checkpoints for sizing and emission and does not advance the caller's range.
- `Token` — the caller's registration ticket (4d). Required; a mismatch is fatal. The Token is passed by reference; it carries the issuing Farm's `FarmId` and the caller's remaining quota as private fields, and the Farm mirrors the quota in a per-slot ledger so a forged ticket cannot exceed the granted excursion.
- `AvgCost` — optional, default 3 (`DEFAULT_AVG_COST`, chunk 16). The producer's declared `Call` cost class: a log2 shift in `0 .. log2(MAX_CHUNK)` (`0 .. 7`) published in Thead. 0 means `Call`s are very cheap (chunk stays at `MAX_CHUNK`, maximum claim amortization); larger values shrink the chunk (short visits, less tail). Classes 0 through 2 (chunks 128, 64, 32) are for very cheap uniform work; the default suits ordinary work; higher classes suit expensive work. `write()` fatals on `AvgCost` out of range.

Returns: the number of Payloads written.

Convenience sources preserve the same table contract: separate header/body forward ranges are paired positionally; one common header may be broadcast over a body range; and the common-header overload may additionally receive a uniform body length in ulongs. The last form treats that length as a caller contract and uses it for arithmetic sizing before emission.

`PayloadEntry`:

- `PayloadHeader* header` (2d).
- `PayloadBody body`; where `alias PayloadBody = const(ulong)[]`; type-erased data. (A mutable slice of const ulongs; `const ulong[]` would const the slice itself and make `PayloadEntry` unassignable in D.)

The payload/table sizing contract, in order:

1. `write()` validates every payload header before accepting it. `Done` and `MaxCs` are in 1..512. `MaxCs >= 1`, and `Done > 1` requires `MaxCs >= 2` (2d). Violations are fatal.
2. Size arithmetic is overflow-checked before any compare to quota. If `body.length` would overflow when added to the payload-header length, or the running payload sum, candidate table size, or final table size would wrap, that is fatal. A wrapped candidate must never be compared to `Exi`.
3. A payload that cannot fit in the *caller tier* maximum `Exi` is a caller error, not backpressure. If a singleton table containing only that payload — Thead, index slots, pads, Tcount words for the current `SqCs`, the payload, and the end pad — exceeds the caller's tier quota, `write()` fatals. An ST payload's singleton costs what a compact table (2f) costs; it is written as a uniform table (2g). The check is by caller tier, not `max(quotaBulk, quotaSmall)`, because a payload that only the other tier could ever publish would remain in a small producer's input range on future calls. Thus it's a contract failure that must be wrangled upstream, and `write() == 0` strictly means the farm is full for `Pi`'s purposes.
4. Multi-payload truncation to the caller's current `Exi` stays legal: if `Ti`'s size exceeds a depleted `Exi`, `Pi` attempts to refresh it, then keeps however many Payloads fit. The committed table also carries fewer than 2^31 payloads (2e); the sizing scan breaks at that bound exactly as it breaks on quota, so a longer input range is truncated, not fatal.

At this level producers do nothing else to resolve fullness; they merely return commit lengths dwindling to 0. A stalled producer may subscribe a ConsumerView, drain, and retry (5a).

`writeTracked` is the narrow completion-notification variant used by actor waves and [private actor hierarchies](docs/guide/private-actors.md). It accepts only a uniform single-threaded, single-shot header (`MaxCs=1`, `Done=1`) and stores a caller-owned `TableCompletionHook*` in Thead word 7. The hook is invoked exactly once by the `Tprogress == Tlen` finisher, after every `Call` in that table has returned. The Farm snapshots the function and context before calling and never dereferences either afterward. Hook/context storage must remain stable until invocation; the notification may reclaim or transfer it as its **final access**. Otherwise it must remain stable until invocation returns. An aggregate producer must count the table before calling `writeTracked`, because consumers may finish it before the write call returns; a zero write admits no table and requires rollback.

### 5c. Reservation and Publication

The ordered contract of a write:

1. Reserve `Wt` with an acq_rel RMW; the returned `Wret` plus the reserved size gives `Wt'`, the sequence after this write. The acquire half imports other producers' reclamation history before any ring store (5a, 9).
2. Deduct the reserved size from the checked quota grant; do not renew it.
3. For every segment/epoch boundary the reservation crosses, initialize that segment's metadata (2c) and, for segments crossed *out of* that are incomplete, plant Sub0 protection via `plantIfUnprotected` (6c).
4. Write the table parts described in 2d, or 2f/2g for Tmt == 0, into the reserved space.
5. Release-store `Tsent` last; it is computed from `Wret`, and consumers validating the expected sentinel by its matching sequence also validate the following contents.

All this comes before releasing the table's sentinel value. `Wt'`, or the sequence *after* the current `write()`, becomes those segments' `Seqt`, the first valid table for that segment. Otherwise `Seqt` would almost certainly point to a previous segment and backtracking is ill-advised. This way consumers and especially subscribers can seek forward using `Seqt` for orientation. Segment initialization zeroes `Sd` (2c). A `Tnext` equal to this `Wt'` therefore always names an initialized epoch.

---

## 6. Consumer Pin Lifecycle

### 6a. ConsumerView

A `Ci`'s ConsumerView is exclusively owned by one `Ci` at a time. Ordinary copying is disabled; it is a unique cursor, not a POD value that may be duplicated. Fields:

- `F` — reference to the target Ant Farm.
- `IDc` — taken from `F`'s `Reqs_c` during subscription. `IDc` is a future targeting hint: 7d / `adopt` may nudge it, but already-incremented leaves are not moved.
- Held pins: a contiguous epoch range `[oldestEi, newestEi]`, equal when freshly subscribed. `newestEi` is the position (last validated table, or idle at `nextSeq`'s initialized frontier). `oldestEi` advances as confirmed prefix segments are released (8). Span `newestEi - oldestEi + 1` is at most `K`. Physical `Ki = Ei & (K-1)`.
- `ltiRing[K]` — the leaf tally index actually incremented at each physical slot. Written only on `incLeaf` for that slot; never rewritten by `IDc` bumps.
- A carried-sweeper flag (7e).

Then derived and tracked stats as are convenient.

### 6b. Subscription

ConsumerView is initialized with `subscribe(AntFarm* F)`.

**Returns:** the non-negative starting epoch on success. Failures are negative: `SUBSCRIBE_FULL` (-1, capacity), `SUBSCRIBE_RETRY` (-2, transient frontier inconsistency), or `SUBSCRIBE_INVALID` (-3, null Farm or already subscribed view). Epoch 0 is a valid success.

The subscription protocol:

**a.** The subscriber `Si` calls `F.add_consumer()` (4c). If `Cf` is full, `F` is oversubscribed and `Si` returns `SUBSCRIBE_FULL`. Every subsequent failure path unwinds `Cf` too.

**b.** Acquire-load `F.Wt` — the only unpinned read — and use `Wt >> segShift` as the walk range. Walk backwards through the corresponding segments (at most `K`, stopping at epoch 0). On each step, *before* inspecting that slot's metadata, fetch_add-release a `Sub` onto its `Rt`. The `d=0` step is the heuristic pin on the frontier slot `(Wt >> segShift) & (K-1)`: a held Sub is visible to producers as `Rt != 0`, so it blocks a full lap through the current view and segments ahead of it (5a contiguous-forward sweep). Older slots sit first in that sweep, so each older step pins before reading. After the pin, acquire-load `Rtlow` and acquire-load `Es`, and rank by earliest `Es` among `Rtlow != 0`. The pulse invariant (6c) guarantees a candidate exists. Several segments may hold Sub0 at once (one per abandoned unconfirmed segment); the walk picks the earliest, which is the frontier of unconsumed work.

Groping an unpinned slot's `Rtlow`/`Es` is incorrect: those reads are not protected against wrap, so the snapshot can mix generations and `Si` can attach with no valid table while still holding a pin.

Race note: the one case where the walk finds no `Rtlow != 0` candidate is a transient race with a *not-last* unsubscribe of a *confirmed* pin, in which case `Si` stands at the frontier. The second `Wt` read and the bounds below still validate that frontier before attach.

**c.** After the attach candidate is read, and while all Sub pins are still held, `Si` re-reads `F.Wt` and validates the following bounds, all compared as epoch numbers after shifting by `segShift`. `Seqt >> segShift` must lie in `[Es, Es + K - 1]`; this rejects a mixed snapshot where `Es` is still the previous lap's value while `Seqt` has already been advanced. `Wt >> segShift` must be `< Es + K`; this rejects a slot whose next lap was already reserved before the pin. When no live candidate was found (`bestSeg < 0`), the fallback must also satisfy `Es == Wt >> segShift`. A delayed initial tail probe can name an already reclaimed slot; another producer may have swept that slot into retained quota without reserving its next lap yet. The fallback therefore must still be the current frontier. This extra bound applies only to the fallback: an older live candidate remains protected and must stay available to drain a backed-up Farm. If any bound fails, `Si` unwinds every Sub and `Cf` and returns `SUBSCRIBE_RETRY`.

**Fallback ordering proof.** The acquire root read after the subscriber's
`Sub` RMW imports earlier root releases: all post-construction root writes
are RMWs. If epoch `e` was entered and then reclaimed, the release that
made its low half zero follows `confirmed(e)`, whose acquire of `Es[e+1]`
imports the reservation crossing out of `e`. Thus the second tail read
cannot still name `e`. A preceding producer plant also releases that
crossing reservation through the root chain. If `e` was never entered and
is a trivially complete interior of a spanning table, acquiring `Es[e]`
itself imports a reservation ending beyond `e`. The remaining accepted
case is a current, initialized frontier: its reuse still requires completion
and the contiguous-sweep/pulse invariant I3. A stale tail load alone cannot
turn a reclaimed epoch into an accepted fallback. This argument uses the
actual reclamation history, not a presumed globally fresh acquire load.

**d.** `Sub` is `2^32` such that it tracks subscribers in the most significant half, apart from normal consumers incrementing in `Rtlow`. The walk of 6b-b already holds one Sub on every inspected slot, including the attach candidate. The number of unique subscribers (and thus Sub units per segment) is bounded by `MAX_CONSUMERS_LIMIT = 128`; one `Si` pins each physical segment at most once, so `K × 128` is well under `2^32`. Sub is only the establish-leaf window; it does not plant or clear Sub0.

**e.** `Si` attaches *in place*, regardless of what `Rtlow` did in the interim. The held Subs are all the same to a producer (`Rt != 0`), so they block any lap of the pinned window while `Si` establishes a real reference. If an in-flight write had already reserved past the selected slot before the pin, the `Wt` re-read bound of 6b-c fails and `Si` fails closed; it does not attach to a slot whose next lap is already reserved. If `Es < 0` or `SqCs == 0` the frontier was never written; fail closed (unwind `Cf` and every deposited Sub) rather than divide by a zero snapshot. Otherwise `Si` performs the normal leaf tally increment and root propagation, caching the `Lti` used. The 0→1 on `Rt` subtracts a live Sub0 if present (6c). `Si` is fully established as a consumer `Ci`.

**f.** `Si` fetch_sub-releases every Sub deposited in 6b-b, including the attach slot. The 0→1 of 6b-e already retracted Sub0 if present.

Failure reasons — capacity, mixed/wrapped snapshot, or uninitialized frontier — all unwind `Cf` and every deposited Sub before returning `SUBSCRIBE_FULL` or `SUBSCRIBE_RETRY`.

### 6c. The Sub0 State Machine

This is the root-tally lifecycle in one place. Keep the bit layout from 2b next to it; the transitions are:

| Event | Preconditions | `Rt` transition | Purpose |
| --- | --- | --- | --- |
| first real root | count 0, optional Sub0 | increment count and remove the observed Sub0 units | replace the pulse with ownership |
| non-final root release | count greater than 1 | decrement count | ordinary release |
| final root, incomplete segment | count 1 | atomically replace count with one Sub0 | preserve backlog discoverability and prevent reuse |
| final root, confirmed non-empty-farm segment | count 1 | decrement to zero | admit producer reclamation |
| final root of an empty Farm | count 1 | atomically replace count with one Sub0 | retain a subscribe anchor |
| producer crossing before table publication | incomplete, count 0, no pulse | CAS in one Sub0, then recheck confirmation | cover a segment no consumer entered or retained |
| confirmation recheck after last-releaser pulse | count remains zero and a pulse is present | CAS out that unit | avoid a confirmed-segment capacity leak |

**Taking a count** (leaf 0→1, subscribe or `moveRef`): `old = fetch_add(Rt, 1)`; if `(old & COUNTMASK) == 0` and `(old & Sub0MASK) != 0`, `fetch_sub(Rt, extra)` where `extra = old & Sub0MASK`, the Sub0 units the thread observed (normally one). Only one thread observes the count 0→1, so that thread is the unique subtractor for that segment. Construction's dummy Sub0 (4b) clears on this same edge.

**Last-releaser** (6e): only when the leaf dec will propagate to `Rt` (the dec returned 1). The naive sequence — plant Sub0, decrement the count, retract the plant if another consumer arrived — is not atomic. A racing first subscriber can clear the precautionary Sub0 after the count hits zero and before the delayed retract, whereupon that retract underflows `Rtlow` and borrows from the Sub field.

So the "leave a pulse only if this decrement actually hits zero" decision is one CAS on `Rt`. Load `old`; if `(old & COUNTMASK) == 0` the count underflowed (fatal); if the count is 1, the new value clears the count bits and leaves one Sub0 (adding a unit only if none is present); if > 1, the new value is `old - 1` with Sub0 unchanged. CAS that in. A failed CAS means another thread changed `Rt` — often the reason to plant is already gone — so reload and retry against the new word, not spin on the same snapshot (section 1).

**Deposition sites.** Deposition happens at exactly three sites, all CAS-gated or cold:

1. Construction places the dummy pulse on `Rt[0]` (4b).
2. `plantIfUnprotected(e)` CAS-plants one unit on an incomplete epoch (`Es[e] == e`, `Es[e+1] == e+1`, `Seqt + Sd < Seqt[e+1]`, and `Rt` with count 0 and no pulse). Invoked only by `write()` for every segment its reservation crosses out of (5c), before publishing its table.
3. `releaseRootLeavePulse`, the last-releaser helper: clears the count bits and leaves one unit (only if none is present) when the final root of an unconfirmed — or last-of-farm confirmed — segment is released.

**Plant ownership.** Only the crossing producer may call the plant helper.
Its own table starts in `eold` and is unpublished until after the plant;
`Sd[eold]` therefore cannot confirm that epoch meanwhile. Interior crossed
epochs have equal `Seqt`/`SeqtNext` and need no pulse. Any consumer that
enters and leaves the incomplete start epoch replaces its last count with
Sub0 atomically, so the root cannot ABA back to zero under this producer.
Unsubscription already applies that last-releaser protocol to every held
incomplete epoch; no unpinned follow-up scan is needed. Such a scan could
read incompleteness, pause through completion and quota certification,
then CAS a stale Sub0 onto zero. Even immediate post-CAS retraction is too
late: a subscriber can convert the transient pulse into a pin on certified
space. Moving the reads nearer the CAS cannot eliminate a zero/count/zero
ABA. Removing the scan also removes this invalid source of live candidates.

**Clear sites.** The primary clear is the 0→1 retract in `takeRootCount`, where the unique thread whose fetch_add observed count 0 subtracts the Sub0 units it saw (normally one). A last-releaser helper may replace its last count with a pulse just as accounting confirms the epoch; it re-checks confirmation and retracts that pulse when requested. `plantIfUnprotected` also retains its defensive post-CAS completeness check and masked retract, although its producer's unpublished table prevents confirmation during supported use. Neither check authorizes a plant after ownership has been relinquished. The retract is a low-half masked CAS retry loop: it reloads `Rt`, checks that the low half is still exactly the `Sub0` that was planted, and then CASes `cur → cur - Sub0` against the freshly observed full word. A concurrent subscriber's `Sub` pin/unpin only changes the high half, so it can make a one-shot exact-value CAS fail while the planted `Sub0` is still present; the masked loop retries against the new high half and succeeds. The loop is bounded by the low-half state — it either removes the exact `Sub0`, or a racing 0→1 has already changed the low half and that same edge cleared the pulse, so the loop bails.

**Race proof, once.** The two deposition sides are symmetric. Plants are CAS-gated and only fire while the count is zero, so a plant cannot straddle a racing first subscriber: the CAS fails and retries against the new word, or bails if the count or pulse is now present — the race that invalidated the plant. Clears are keyed to the 0→1 edge or to the plant's own post-CAS re-check, and only the thread that observed the edge subtracts, so a clear cannot underflow `Rtlow` or borrow into the Sub field.

The contract is the hard invariant: **a confirmed non-empty-farm segment cannot retain Sub0.** A segment that ever carried Sub0 and later becomes confirmed must have been entered by a consumer: either a table starts there and the finisher held that starting segment, or the segment was already incomplete and pulsed, and the only way to confirm it later is for a table starting there to complete — which again means a finisher held it. A segment that confirms trivially because a spanning table made `Seqt + Sd >= SeqtNext` without any local accounting was never incomplete at the moment `plantIfUnprotected` inspected it, so it never received a plant pulse in the first place. Therefore every path to confirmation either passes through a first real root, whose 0→1 edge clears any pulse, or races the last-releaser helper's confirmation recheck. That helper's acquire/release root CAS joins preceding root releases, and its expected-epoch acquires validate the completion values before retraction (9). There is no accepted residual case.

Producers see the signal in `refreshQuota`'s `Rt == 0` check — a lone pulse reads as occupied — and subscribers see it in the subscribe walk's `Rtlow != 0` test, so an abandoned incomplete segment is both protected from overwrite and discoverable for reclamation.

### 6d. Orientation and Frontier Migration

A `Ci` often needs to seek data, for instance if it's newly subscribed. Segment metadata provides a reasonable starting point. Consumers load-acquire the segment epoch and return if it is not the expected value (the caller retries or treats the frontier as not yet written). From there it has a starting Seq to a table, where the same applies to the sentinel value at that Seq.

Seek costs per entry are primarily amortized by publishing large tables. Within tables the `SqCs+1` counters — `SqCs` shard counters for contention plus `Tprogress` for completion — keep a small target for both. (I considered building in skip lists or other structures dynamically updated by consumers, but the design sticks with what's essentially a linked list of one table pointing to the next; see 11.)

**Ordinary table advance.** As consumers move to new tables, they check transitions across segment boundaries. `Ci` incs a leaf tally at every epoch from `newestEi+1` through the newer table's epoch when it *validates* that table's sentinel — a valid sentinel implies the producer has initialized those segments' stats, so the increment always reads a live `SqCs`. `newestEi` advances to that epoch; `oldestEi` is unchanged. The old position stays held until the confirmed prefix reaches it (8). `IDc % SqCs` at the increment is the leaf recorded in `ltiRing`; later `IDc` nudges do not retarget it.

**Idle frontier migration.** `moveRef` on the hot path still runs only after a successful sentinel validate. That is not enough: a spanning table can start in `Ki` and end at a `Wt'` several epochs later, leaving `Ci`'s position on confirmed-complete `newestEi` while `nextSeq` names a later initialized segment whose table is not yet published. `refreshQuota` counts full free segments from the write epoch + 1; a stale pin in that sweep window starves renewal even though the consumer is idle at `Wt`. Therefore, on a sentinel miss at `nextSeq` (and after a consume that advanced `nextSeq` into a new epoch), if the current position is confirmed complete and `Es[ki]` matches `ei` for `nextSeq`'s `(ki, ei)`, `Ci` advances `newestEi` onto that segment and then releases any confirmed prefix. Increment-ahead, always at least one pin, pin tracks the frontier.

`nextSeq` is always `Tnext` of a published table, which is that write's `Wt'`. Crossing writes already release-store `Es` through that epoch (5c), so the segment of `nextSeq` is already initialized. The case one cannot migrate into — `Ki+1` with `Es` unset — only exists when no write has crossed, which means `nextSeq` is still inside `Ki` and the pin is on the write segment, which the quota sweep does not examine. Restricting `Exmax` (for example to half the buffer) does not replace this migration: a smaller `Exmax` only *tolerates* a pin left on a completed start.

Thead's contents were laid out in 2d; orientation uses `Tsent`, `Tnext`, `Tmt`, `Tlen`, `Tsize`, `AvgCost`, and the derived stats the producer can compute as is convenient (`Cs` and `SqCs` used at write time).

### 6e. Unsubscription

Inactive consumers block reclamation unless they `unsubscribe()` from `F`.

**a.** `Ci` first releases the confirmed prefix from `oldestEi` toward `newestEi` (8). Only the old end moves; confirmed interiors stay pinned until they become oldest. Confirmed release is the plain leaf-root propagation of 2b. A confirmed segment that is not the empty-farm pulse must be allowed to reach `Rt == 0` so producers can reclaim it; the last-releaser helper's confirmation recheck (6c) removes a transient pulse when completion races that release.

**b.** `Ci` calls `F.sub_consumer()`. The return value 1 means `Ci` is the last unsubscriber of the farm (`Cf` went 1→0). That flag is used only in step c; it is not itself a Sub0 plant.

**c.** Then `Ci` releases every remaining epoch in `[oldestEi, newestEi]` — confirmed interiors with the plain release of 2b, unconfirmed interiors with the last-releaser helper, then the position (`newestEi`) as follows.

- Unconfirmed interior or unconfirmed non-last position: the last-releaser CAS helper of 6c. The helper re-checks confirmation after its CAS; if the segment was completed concurrently, it retracts the pulse it just left with the same low-half masked CAS as `plantIfUnprotected`. An incomplete segment therefore never becomes visible as `Rt == 0`, even when the releaser is not last of the farm. This is what closes pure-churn orphans: a non-last unsubscriber who is the last pin on an early incomplete segment leaves Sub0 there, and a later subscriber's walk (6b-b) attaches at that earliest pulse.
- Confirmed interior or confirmed non-last position: plain release, no Sub0.
- Confirmed position, and `Ci` is last of the farm: the same CAS helper, not a bare add. This is the empty-farm pulse. The CAS makes the "plant only if the count actually hit zero" decision atomic with the count decrement, so a concurrent first subscriber either sees the pulse and clears it on its 0→1, or increments first and causes the helper's CAS to fail; it can no longer pull the pulse out from under a stale retract.

Do not extra-plant `fetch_add(Sub0)` and then run the helper on the same segment — that is two plants. If the position is unconfirmed, the helper already leaves the pulse.

Postconditions: with `Rt[0]` constructed as Sub0, last-releasers leaving Sub0 on every abandoned unconfirmed segment, and last-of-farm leaving Sub0 on a confirmed frontier, there is always a live "pulse" somewhere in the buffer for the `Si` in 6b-b to walk back and find a segment untrodden by producers. Several pulses may exist at once: that is required, not a leak. Farm-empty with everything confirmed still has exactly one.

---

## 7. Work Scheduling

### 7a. Primary Work Claims

Finding a table, `Ci` can use a combination of header values and local stats to decide how to consume. The main pathway maps a shard of consumers to a linear slice of Tindex claimed by chunks on a sharded progress counter.

Definitions first:

- `Tseq` is the table's starting sequence.
- `(IDc + Tseq) % SqCs` yields `Ci`'s shard index `Shi` and counter `Shc`, exactly like its leaf index.
- `Shbase = Tlen / SqCs`, `Shrm = Tlen % SqCs`, `Shlen = Shi < Shrm ? Shbase + 1 : Shbase`.
- `Shstart = Shi * Shbase + (Shi < Shrm ? Shi : Shrm)`.
- `Chunk = MAX_CHUNK >> AvgCost`, read from the table header word published by `write()`; `MAX_CHUNK = 128` so the default `AvgCost = 3` lands on 16, a latency-conserving default inside `[1, 128]`; `AvgCost` 0 through 2 give 128, 64 and 32. Every consumer reads the same header word, so `Shiter` cannot disagree across visitors.
- `Shiter = ceil(Shlen / Chunk)`.
- `X = fetch_add(Shc, 2^32) >> 32`; if the claims half wrapped to 0, fatal (10). `X` is the claimant's run index, or a signal that the shard is fully claimed.

The baseline loop:

**a.** `Tprogress` gives an O(1) view of the table's overall accounting; `Ci` checks if `Tprogress == Tlen` and, if so, skips. The skip covers primary work only: `Tprogress == Tlen` means all primary Tindex runs are accounted, not that every MT callback has returned, so a cheap pre-checked MT drain pass still runs (7e).

**b.** If `Tlen` is below the table's small threshold, consumers with `Shi == 0` claim the entire table by chunks as below; the others do not. The threshold is a farm field; 0 selects the bounded heuristic `clamp(SqCs * Chunk, 16, 256)`. Below the threshold all work is assigned to shard 0; at or above it the table uses all `SqCs` work shards. Away from the clamp limits, the heuristic begins sharding when there is roughly one full chunk per shard. The default fixed value is 64. All consumers derive the same threshold from the same header/farm values.

**c.** `Ci` gets claim `X = fetch_add(Shc, 2^32) >> 32`. If `X >= Shiter` the shard is already fully claimed and we move to the secondary pathway (7c). `X` is also the feedback signal used by 7d.

**d.** If `X < Shiter` it maps to a run of Tindex of Chunk length at `Shstart + X * Chunk`. If `X == Shiter-1`, `Ci` truncates the run to the remainder (`Shlen & (Chunk-1)`) if it's nonzero.

**e.** When a `Ci` finishes a run do `Y = fetch_add(Shc, run_length)` with acquire/release ordering; if the low half of `Y` equals `Shlen - run_length`, then `Ci` completed the shard and adds `Shlen` to `Tprogress` with acquire/release ordering. The completion RMW joins preceding primary-run callback writes within the shard; the `Tprogress` RMW joins those shards. That is a join of all callbacks only for tracked ST single-shot tables (`MaxCs=1`, `Done=1`), whose callbacks all run on the primary path. Secondary and tertiary MT callbacks do not contribute an `Shc` completion and are not joined (7c, 7e). A relaxed shard completion sum would not establish the within-shard join. This bounds the number of mutations to `Tprogress` to `SqCs`. The completer additionally gains the *sweeper role* for this table (7e), and if its `Tprogress` add lands exactly on `Tlen` it is the table's finisher and performs the Sd accounting (8).

**f.** Then `Ci` returns to step c; or if `X >= Shiter` it enters the secondary pathway.

Each Tindex entry produces exactly one primary Pcount claim: each payload index lives in exactly one shard's runs. ST records this unique claim with a raw store; MT uses fetch_add to arbitrate against secondary/tertiary visitors. That is the fact the secondary/tertiary admission argument relies on (7c, 7e).

### 7b. First-Claimant Mid-tick Yield

The `Ci` that drew `X == 0` for a shard (the first claimant, at most one per shard) may, after finishing a run that did not complete the shard, read the claims half of `Shc`. If it is greater than the number of claims that `Ci` has personally made in this shard, the shard is shared: at least one other consumer has claimed a run here. The first claimant may then probe `Tnext`'s sentinel; if it is live, it may return from the primary pathway immediately instead of returning to step c, so `consumeNext` can advance to the already-published table (for example a mid-tick 1-payload write). The Tcount/`Tprogress` arithmetic is unchanged: the yield happens only after `Y = fetch_add(Shc, run_length)`, so the run is accounted, and the other claimant(s) will finish the shard. Unsubscription after the yield cannot starve the shard; OS pre-emption can only delay it, and the idle re-walk of 8 remains the backstop. Sweeper and re-walk calls (`checkFirst`) never yield early.

Priority of roles: if the same run that would qualify `Ci` for the yield is also the shard-completing run (the 7a-e completion sum lands on `Shlen`), the completer outcome takes precedence — `Ci` gains the sweeper role for this table and does not yield early, so the completer's foreign-shard sweep and MT pass are never lost to a mid-tick advance. The yield is only considered after a run that did not complete the shard. A yielded `Ci` also drops any carried sweeper role (7e): it begins the next table as a normal consumer without role baggage. Only a shard-completing result advances the sweeper role; observing the shared-shard signal permits the yield and does not grant the role.

### 7c. Secondary Work Claims

If `Tmt > 0` there are some payloads that are themselves multithreaded. In the primary pathway, `Ci`'s `Shi` maps it to a slice of the full payload set using the shard counters for iteration. Here `Shi` maps to a round-robin assignment of the multithreaded Payloads using a Payload's internal counter for iteration: work shard `Shi` visits MT-index positions `Shi, Shi+SqCs, ...`. An admitted visitor loops over calls until no unissued iteration remains.

The general MT entry order is the callback ordering that makes the accounting proof work:

1. claim admission (`pcount.claims < MaxCs`);
2. claim an iteration (`pcount.calls < Done`);
3. call synchronously;
4. increment returned calls (`pcount.completions`);
5. only then may that consumer account its primary run or table.

Callback return values are presently ignored by Farm accounting.

The ST fast path is exactly `MaxCs == 1 && Done == 1` on a primary visit. The shard counter already assigns the payload to exactly one consumer: shard slices and their claimed runs are disjoint, and sweepers/re-walkers claim runs through the same counters. ST payloads never appear in the MT index, so secondary/tertiary visitors cannot enter them. The unique consumer therefore raw-stores `1UL << 32` to Pcount and executes `Call` without another admission RMW. This preserves the callback-visible claims value of one and calls/completions values of zero. In a compact (2f) or uniform (2g) table there is no ring `Phead`; the callback receives a consumer-local header with the same values. The store remains atomic because later ring laps reuse the same words. Table completion is tracked by Tcount/`Tprogress`; no ST completion depends on Pcount. Multi-threaded payloads, including `MaxCs > 1 && Done == 1`, keep the full packed fetch_add path.

This structures the consumer strategy such that exactly 1 consumer normally enters a single-threaded Payload, and ~`SqCs` consumers normally enter an MT payload. "Normally" is performance intuition, not a guarantee: `MaxCs`, races, and repeated visitors to the same shard determine actual admissions. Tertiary behavior is the sweeper role in 7e.

### 7d. Oversaturation Feedback

If `Ci` adds no work in either the primary or secondary path, it was entirely oversaturating the table. The value `X` from 7a-c has useful properties.

Let `Z = X - Shiter`. After the primary shard's iterations are accounted, each `Ci` that fails to claim a run increments `Shc` one more time and observes the shard fully claimed; `Z` counts those exhausted-claim observations, not callback completions. If `Ci` observes `Z` significantly over `SqCs`, it can check the Farm level `Cf` and see if the shard is overloaded. `Ci` then nudges its `IDc` by +1. Already-incremented leaves are not moved. On the next table (and the next segment pin) `Ci` lands in a different bucket of consumers. This reduces an overallocated shard via a local feedback signal.

This is a scheduling heuristic: it affects future shard/leaf selection but is not required for memory safety.

### 7e. The Sweeper Role

The consumer whose 7a-e completion sum lands on `Shlen` — adding `Shlen` to `Tprogress` — gains the sweeper role for that table. This pulls one sweeper from each shard, so it takes all shards' completers being gone (hence no consumers and no progress anyway) for work to starve, without forcing much extra contention. The sweeper makes one linear pass:

- **Foreign shards:** for each shard other than its own, a plain load short-circuits fully-claimed shards (`claims >= Shiter`) without an RMW; otherwise it attempts to claim and process runs exactly as in the primary path. The 7a-e completion rule fires for whichever consumer lands the final sum, owner or sweeper, so `Tprogress` mutations stay bounded by `SqCs`.
- **MT items:** a full pass over the MT index with plain-load pre-checks (skip if `calls >= Done` or `claims >= MaxCs`), entering and draining (looping calls until exhausted) anything left.

A multi-iteration payload has at least two admission slots and exactly one primary Tindex attempt. Secondary and tertiary attempts use the draining loop. If the primary attempt wins one slot, a draining attempt can win another; if draining attempts arrive first, one of them already owns the unfinished iterations and the later primary attempt may be rejected. A particular sweeper may lose the race because another draining entrant already owns or exhausted the remaining work. Liveness is preserved without promising that every sweeper can enter.

Sweeps of foreign shards do not recursively confer the role. A late visitor finding `Tprogress == Tlen` still runs the pre-checked MT pass: an MT callback entered concurrently by the secondary or tertiary path may still be in flight, or an MT iteration may still need a draining entrance. Every such callback runs synchronously under its caller's retained Farm pin, so table accounting can precede callback return without permitting ring reuse. The complete-table MT pass attempts still-unissued work when an admission appears available; it is not a storage lifetime mechanism.

**Undersaturation repair** (complementing 7d). A sweeper reads the same `Z` value on foreign shards. Finding `Z <= 1` — evidence, not proof, that no native consumer exists there — it nudges its `IDc` by the round-robin distance into that shard, `IDc += (Shi_foreign - Shi_own + SqCs) % SqCs`, patching the hole a pre-empted or unsubscribed native left. Already-incremented leaves stay on the ring; the new `IDc` is the next pin's leaf target. A native laggard waking to an overbalanced shard nudges itself out via 7d. The signal is self-limiting: each entering sweeper's failed exit-claim raises the shard's `Z`, so it disappears after a couple of adopters. A consumer adopts at most one shard per table. Small tables are excluded.

**Carried role and small tables.** Small tables concentrate all claims on shard 0 (7a-b), so under churn they may have no native consumer at all. A consumer that completed a shard of the previous table therefore carries the sweeper role one table forward: if the next table is small, it sweeps that table's single shard regardless of its own `Shi`, and if the sweep completes the shard the role chains forward again. The idle re-walk of 8 remains the backstop, but the hot path no longer stalls producers waiting for it. A complete table does not clear the carried role: its cheap pre-checked MT pass runs without touching `sweeperNext`, so a role carried across consecutive already-complete tables survives to the first table that still needs sweeping.

---

## 8. Table Accounting and Segment Reclamation

This is the canonical home for the completion/reclamation states defined in 1. Two facts drive the whole story: `Sd` says how far primary table accounting has advanced; `Rt` says whether ring memory is still protected. Keeping those separate prevents both the MT-wording confusion and most of the repeated reclamation explanation.

`Tcount` accumulates primary runs per shard. `Tprogress` is the table's primary accounting: when it reaches `Tlen`, every Tindex run has been claimed, executed, and accounted. That is *not* a join on every concurrent MT callback; an MT callback entered by the secondary or tertiary path may still be in flight (7e). The table's finisher — the `Ci` whose `Tprogress` add lands on `Tlen` — adds `Tsize` to `Sd` of the table's starting segment.

A nonzero table-completion hook is a separate case from that MT discussion. `writeTracked` is the only writer of the hook, and it accepts only tracked ST single-shot tables (`MaxCs=1`, `Done=1`); ordinary `write` supplies no hook. For those tables every callback is a primary run, so the finisher's `Tprogress` transition has already joined all of them before the hook runs. The hook is not invoked while another MT callback is in flight: that combination is rejected. On an ordinary hook-less table, an MT callback still in flight remains safe for ring reuse because that callback's own `ConsumerView` pin still prevents `Rt == 0`.

`Sd` is zeroed by the producer when its reservation crosses into the segment for a new epoch, before the release-store of `Es` (2c). The finisher provably still holds a reference on the segment (it was unconfirmed until this moment), so its accounting can never race a producer's re-zeroing.

A held epoch `Ei` (physical `Ki`) is released from the old end of the range when:

- `Es[Ki] == Ei` — the slot still names this epoch,
- `Es[Ki+1] == Ei + 1` — the next segment is initialized, so no more tables can start in `Ki`,
- `Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1]` — the span is fully accounted.

Only `oldestEi` advances; a confirmed interior stays pinned until it is oldest. Confirmation requires `Es[Ki] == Ei` as well as the next-segment test: a wrap that overwrote the slot is not "confirmed."

The target is exact: tables are contiguous, so the sizes of all tables starting in `Ki` sum to precisely `Seqt[Ki+1] - Seqt[Ki]`, and `Sd` accrues each table's size exactly once, only on completion. A large table completing ahead of a small one leaves the small one's size missing and cannot force the crossing. A segment completely spanned by one table gets a `Seqt` at or past `Seqt[Ki+1]` and confirms trivially. While any reference on `Ki` is held, no producer can lap past it, so the next segment's stats remain at epoch `Ei+1` once initialized. (The rejected signed-balance `Sbal` design that this replaces lives in 11.)

**Retained-reference invariant.** Every consumer holds a reference on every segment it has entered but not yet confirmed complete. An incomplete segment is therefore protected by every passerby still subscribed. Protection does not end when those passerby unsubscribe. The last *releaser* of an unconfirmed segment — not only the last unsubscriber of the farm — leaves Sub0 via the last-releaser helper of 6c/6e. New subscribers land at the earliest `Rtlow > 0`, which is the earliest such pin, and drain the backlog exactly once. A non-last unsubscriber may drop its references; if it was the last pin on an early incomplete segment, Sub0 stays behind. Planting Sub0 only at `Cf == 1` on the last unsubscriber's own earliest held ref is not enough: that last unsubscriber may never have entered the orphan.

**Position and frontier.** The position (`newestEi`) participates in the same confirmation story. After a spanning table, or on the idle path, a confirmed position is advanced onto the initialized segment of `nextSeq` (6d) and the old pin remains in the range until the confirmed prefix reaches it. An idle subscribed consumer is therefore always pinned on the frontier, never on a completed interior segment as its `newestEi`.

Consumers consult `Sd` only on position changes and on idleness, never per payload: after each table advance, at unsubscribe, and on the idle path below.

**Deadlock freedom and the idle re-walk.** If producers stall on a held trailing reference, consumers catch up to `Wt` and their next-table sentinels stop validating. On that idle path a consumer first advances a confirmed position onto `nextSeq`'s segment (6d), then re-walks its oldest unconfirmed epoch from `Seqt` if `Es` still names that epoch, claiming any leftover runs in every shard and draining MT items (the data is guaranteed present and intact while `Es` matches: the consumer's own held reference forbids overwrite), then releases the confirmed prefix. It also re-walks its current position segment: a per-consumer cursor (`sweepSeq`) remembers the sequence up to which that segment has been drained, so each park resumes from the cursor instead of rescanning the whole segment — O(new tables) per miss rather than O(tables in the segment). The cursor resets whenever the position moves to a new segment (`moveRef`). Blocking creates the idleness that resolves it. If refresh still fails, a producer may itself subscribe and drain (5a).

**Reuse condition.** Confirmation permits pins to be released; producer reuse occurs only after the contiguous acquire sweep observes zero roots (I5). `Sd` is the accounting gate; `Rt` is the memory-protection gate.

---

## 9. Memory-Order Contract

Atomic memory orders, in one place:

| Object/transition | Publishing operation | Observing operation | Ordered data |
| --- | --- | --- | --- |
| root-tally RMWs | release `fetch_add`/`fetch_sub` on `Rt` | acquire loads of `Rt` (producers, subscribers, last-releaser CAS) | prior ring reads by the counted consumer join the tally's release sequence |
| write-tail reservation | acq_rel RMW on `Wt` | acquire probe used by quota logic and subscription; the acquire half of every later reservation | published tail position and bounded sweep anchor; each reserver's acquired reclamation history, handed to producers that later spend retained quota without sweeping |
| leaf-tally RMWs | acq_rel on `Lt` | acq_rel edge decrement and release root update | a non-edge consumer's ring reads join the leaf release sequence carried by the last-on-leaf's `Rt` release |
| table publication | release-store `Tsent` last | acquire-load and validate `Tsent` | raw-atomic Thead remainder, indexes, counters, payload header/body, compact `[Call][Pbody]` records (2f) or uniform `Tcall`/`Tblen` and `[Pbody]` records (2g) |
| segment metadata publication | release-store `Es` last | acquire-load and validate expected `Es` | raw-atomic `Seqt`, `Cs`, `SqCs`, initial `Sd` |
| table accounting join | acq_rel completion RMWs on `Shc`, then acq_rel RMW on `Tprogress` | acquire observation of `Tprogress == Tlen` | primary-run callback writes join within each shard, then across shards, before optional table-completion notification; all callbacks only for tracked ST single-shot tables |
| producer reuses a segment | release root transition toward zero | acquire-load `Rt == 0` in quota sweep; then, for any other producer reusing the swept space, the acq_rel `Wt` reservation chain | prior consumers' ring accesses happen before reuse, including reuse by a producer spending quota granted before those accesses |
| temporary subscriber pin | release add/sub of `Sub` plus acquire metadata/root validation | producer acquire of nonzero `Rt` | pinned wrap window cannot be lapped while attaching |

The operational sections cite these edges by name rather than restating the orders.

**Why the reservation chain suffices.** Every `Wt` change is an RMW, so a producer P's acq_rel reservation acquires every earlier reservation and, through each one, the sweep that preceded it in its thread. Let `A*` be the largest anchor of a grant P has acquired this way, including its own. P's span ends at most `Exmax` past `A*` (I1). One producer holds at most one unspent grant at a time, and `refreshQuota` replaces the balance. The grant at `A*` swept `Exmax` of free space after `A*`'s segment. That segment itself was certified by an earlier sweep, whose crossing reservation the `A*` probe acquired. Induction gives consumer accesses → leaf/`Rt` release → zero-`Rt` sweep acquire → reservation chain → P's ring and `stats` stores. P's per-lap `stats` stores come after its reservation, so this chain also orders a previous epoch's `Sd` add (made while the finisher still holds its pin) before P zeroes `Sd`. A separate acquire load of `Wt` before the RMW would not suffice: the tail can move between that load and the RMW that decides the write address.

The runtime accesses ring words atomically even with raw order, and the
release sentinel publishes their initialized values. Callbacks may read
payload bodies plainly, so atomic stores alone do not make wrap reuse safe:
the leaf/root/sweep/reservation happens-before chain must order every such
read before the next producer's stores. Raw runtime loads also remain
subject to publication and lifetime validation.

The raw ring-read rule: acquire-validate `Es` or `Tsent` first, then raw-load the published sibling fields, and only while holding the tally that protects that segment.

**Pulse ownership and the recheck.** The last-releaser helper changes count
to Sub0 atomically, so no producer can certify a zero gap before its
confirmation recheck. Its acquire/release root CAS and expected-epoch
acquires import accounting history before retraction. A producer's separate
plant is protected by its unpublished table (6c). An unpinned scan would
have neither guarantee: a transient stale pulse can be consumed by a
subscriber before a post-CAS recheck removes it. The ordering argument is
not specific to x86 TSO.

**Sanitizer evidence.** TSan builds use the same real atomic edges as ordinary
builds. `version(TSan)` selects runtime options, not synchronization hooks.
LLVM 22.1.8 carries non-edge leaf releases through the last-on-leaf acquire
and root release. The controlled `review_torture/leaf_reuse.d` mutation
exposes a missing leaf acquire only when the old manual root annotations
are absent; see [the ordering audit](review_torture/FARM_ORDERING.md).


---

## 10. API, Errors, and Implementation Profile

### Conformance and error table

| Situation | Result |
| --- | --- |
| producer tier has no registration slot | invalid Token; recoverable |
| consumer capacity exceeded | `subscribe()` returns `SUBSCRIBE_FULL` (-1); recoverable after another view unsubscribes |
| subscribe sees an uninitialized or generation-inconsistent frontier | `SUBSCRIBE_RETRY` (-2) after unwinding temporary pins; retry immediately |
| subscribe with a null Farm or an already subscribed view | `SUBSCRIBE_INVALID` (-3); caller error |
| assigning over a still-valid Token | fatal |
| `write()` has no safe quota at present | returns 0; backpressure |
| only a prefix fits current quota | returns the prefix count; caller advances its own range |
| bad/stale/forged Token, or a Token presented to a Farm other than its issuer | fatal |
| invalid payload header, callback, iteration bounds, or `avgCost` | fatal |
| payload cannot fit a singleton table in the caller's tier | fatal |
| size arithmetic or packed counter wraps | fatal |
| tally underflow or invalid ownership transition | fatal |
| destroy with live consumers or producer tickets | fatal |

Operational sections still mention a fatal guard at the point where it matters to a proof; this table is the compact form of the policy.

### Current implementation profile

- `MAX_CONSUMERS_LIMIT = 128`, `MAX_LEAVES = 12`.
- `KMAX = 16`; `ringMiB` must be a power of two `>= 2` (so `Ln >= 2^18` ulongs) and `<= MAX_RING_MIB`; `segCap` floor 2048. These last two are characterized rather than tuned (11).
- `MAX_CHUNK = 128`; default `AvgCost = 3` (chunk 16); small-table threshold default 64, auto rule `clamp(SqCs * Chunk, 16, 256)`.
- Payload `Done` and `MaxCs` bounds: 1..512. `AvgCost` in `0 .. log2(MAX_CHUNK)` (`0 .. 7`).
- Bulk quota auto-defaults to `segCap`; small quota fatals on 0.
- Producer ticket slots are 64-byte strided.
- The Farm must be allocated into 64-byte-aligned memory with manual lifetime. At the time of writing, D's garbage collector only guarantees 16-byte alignment, so the allocator is explicit.
- Just about every interface is `@nogc nothrow @system`.
- The implementation spells the atomic helpers `atomicFetchAdd`/`atomicFetchSub`; this spec uses conceptual `fetch_add`/`fetch_sub`.

---

## 11. Rationale and History Appendix

These details are worth preserving, but they are history, alternatives, or tuning observations rather than normative rules.

**Rejected `Sbal` design.** An earlier draft computed the confirmation boundary arithmetically and kept a signed outstanding balance `Sbal`, incremented by producers for tables starting in a segment and decremented by finishers. It had a producer-producer race — one producer initializing the segment could zero the balance after another had already incremented it for a table starting there — and a stale-balance deadlock mode. Comparing `Seqt[Ki] + Sd[Ki]` against `Seqt[Ki+1]` removes the balance entirely.

**Strong versus weak CAS.** The current implementation uses a strong CAS (D's `cas` does not fail spuriously). A weak CAS would extra-retry on the looping sites, and the one-shot retract already treats failure as harmless because a later 0→1 clears the pulse; whether weak CAS would suffice is an open question, not a contract point.

**Ring geometry.** "Probably 4 or 8" segments is a characterization, not a requirement. The 2 MiB minimum (`Ln >= 2^18`) and the 2048-word segment floor are documented for later investigation: smaller rings were never studied, and the floor keeps per-table header/pad space from dominating a segment's capacity.

**Batching and contention.** The single write tail is a linear contention point; testing has revealed batch sizes reasonably above 1 to be the most crucial aspect, more than the exact segment count.

**Cache-line placement.** Producer ticket slots are 64-byte strided because tickets are shared mutable state under the registration CAS and the buffer cannot align them beyond 8 bytes; the stride, not the natural ulong size, carries the isolation. `Call` sits immediately after `Pcount` so the two share one cache line, safe because `Call` is dereferenced only by a consumer holding a valid claim — the same thread that just wrote `Pcount`.

**Skip lists considered.** I have considered building skip lists or other structures dynamically updated by consumers for orientation, and am sticking with what's essentially a linked list of one table pointing to the next; large table publication amortizes the seek.

**D GC alignment.** At the time of writing, D's garbage collector only guarantees alignment to 16 bytes, which is why the Farm's 64-byte-aligned allocation is manual.
