# Ant Farm: An M:N Concurrent Ring with Superlative Scaling

Living spec, matching the implementation in this directory. The farmer metaphor and the field names are the original; the structure is the current one — held-epoch pins, initialization-time Live, ticketed write, pin-before-read subscribe — written as the design, with the concurrency arguments that accumulated during bug-fixing kept beside the rules they justify.

- [1. Model, Scope, and Terminology](#1-model-scope-and-terminology)
- [2. Data Layout](#2-data-layout)
  - [2a. Farm-level Fields](#2a-farm-level-fields)
  - [2b. Segment Tallies](#2b-segment-tallies)
  - [2c. Segment Statistics](#2c-segment-statistics)
  - [2d. Payload Layout](#2d-payload-layout)
  - [2e. Table Layout](#2e-table-layout)
  - [2f. Compact Tables](#2f-compact-tables)
  - [2g. Uniform-Call Tables](#2g-uniform-call-tables)
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
  - [6c. The Live Lifecycle](#6c-the-live-lifecycle)
  - [6d. Orientation and Frontier Migration](#6d-orientation-and-frontier-migration)
  - [6e. Unsubscription](#6e-unsubscription)
- [7. Work Scheduling](#7-work-scheduling)
  - [7a. Work Claims](#7a-work-claims)
  - [7b. First-Claimant Mid-tick Yield](#7b-first-claimant-mid-tick-yield)
  - [7c. Payload Execution](#7c-payload-execution)
  - [7d. Oversaturation Feedback](#7d-oversaturation-feedback)
  - [7e. The Sweeper Role](#7e-the-sweeper-role)
- [8. Table Accounting and Segment Reclamation](#8-table-accounting-and-segment-reclamation)
- [9. Memory-Order Contract](#9-memory-order-contract)
- [10. API, Errors, and Implementation Profile](#10-api-errors-and-implementation-profile)
- [11. Rationale and History Appendix](#11-rationale-and-history-appendix)

---

## 1. Model, Scope, and Terminology

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. Whenever the farmer lays down a new stretch of path he also plants a flag beside it, and only an ant that has seen the stretch cleared takes the flag down. The last ant to leave an area changes the tally (a reference counter) to zero; an area left half-done keeps its flag, so the farmer still sees a nonzero signal and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

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
| table complete | the finisher's acq_rel `Tprogress` increment reaches `Tlen` | Every payload callback has returned and the finisher has joined their writes. The finisher can account `Tsize` into `Sd`; a hook may still be running under its view pin. |
| segment confirmed | matching `Es` values and `Seqt + Sd >= SeqtNext` | No more table accounting is missing from the segment. Confirmation alone does not make the bytes reusable. |
| segment discharged | a consumer holding the segment observed it confirmed and cleared its Live bit (6c) | The epoch no longer blocks reuse by itself; references and subscriber Subs still do. A discharged epoch is confirmed; a confirmed epoch may stay Live until its next holder releases it. |
| slot admitted by a quota sweep | a producer observes `Rt == 0` for that following slot while scanning contiguously from its tail anchor | No consumer reference, subscriber Sub, or Live bit blocks the sweep there. Because every undischarged epoch is Live (I3), zero is sufficient in this reuse context; the producer does not recheck `Sd`. The slot of an epoch whose initialization is still in flight also reads `Rt == 0` until its producer raises Live, but no qualifying sweep can reach it (I3), so `Rt == 0` is not globally synonymous with discharge. |

### Synchronization, runtime, and error policy

Pains are taken to *eliminate* CAS operations from the hot paths of the module. Hot-path synchronization is fetch-add / fetch-sub plus acquire/release load/store. The CAS sites are cold: producer-ticket claim and the per-epoch Live discharge (6c). They do not retry the same expected snapshot indefinitely; every failure reloads and revalidates the reason for the operation, and a discharge stops as soon as the reload shows Live already clear. The current implementation uses a *strong* CAS (D's `cas` does not fail spuriously). Whether a weak CAS would suffice is a rationale question, not a normative requirement; see 11.

The reference implementation is in D and the Farm must be allocated into 64-byte-aligned memory with manual lifetime; at the time of writing D's garbage collector only guarantees alignment to 16 bytes (10). Just about every interface to the Farm is `@nogc nothrow @system` code, and this system is designed such that most errors are fatal with very few exceptions. The recoverable exceptions are registration-full, oversubscription, a subscribe attempt whose bounded rounds the write tail outran (6b), and backpressure.

Throughout, I use conceptual `fetch_add`/`fetch_sub` for the implementation's `atomicFetchAdd`/`atomicFetchSub`.

---

## 2. Data Layout

### 2a. Farm-level Fields

The shared mutable Farm fields:

- `Cf` — the current number of subscribed consumers, including subscriptions still attaching. It bounds capacity (4c) and sizes new tables' `SqCs`; no lifetime or protection decision depends on its value, in particular not on a departure being last (6e).
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

`Ci` claims a reference to `Ki` by fetch-incrementing its `Lti`, and relinquishes by fetch-dec. Leaf RMWs are acq_rel. When the operation is an edge transition to/from zero — the inc() returns 0 or the dec() returns 1 — `Ci` repeats that operation on `Rt` with release semantics. This is indirect reference counting with a fixed fan-out and open membership; see the lineage note in 11. A consumer that is not last on its leaf never writes `Rt`; acq_rel on the leaf puts that consumer's ring reads in a release sequence the last-on-leaf's `Rt` release (and a later producer acquire of `Rt == 0`) can carry.

Consumers **must** always increment a tally ahead before decrementing their current one. This ordering leaves no gap where a producer might spuriously observe an `Rt` with a zero value. Producers load-acquire `Rt` and regard segments with nonzero Rts as contributing no available space.

`Rt` is a bitfield:

| Bits | Field | Unit | Role |
| ---: | --- | ---: | --- |
| 63..32 | `Sub` | `2^32` | provisional subscriber pins (6b) |
| 31..17 | reserved | — | zero |
| 16 | `Live` | bit | the epoch in this slot is initialized and not yet discharged (6c) |
| 15..0 | count | 1 | propagated leaf references; `COUNTMASK = Live - 1` |

Let `Rtlow` be the least significant half (`LOWMASK`): `Live` and the count. Underflow checks on `Rt` test only `COUNTMASK`.

`Live` is one bit, not a counted unit. It is raised once per epoch by the producer whose reservation initializes that epoch, and cleared once by a consumer holding the epoch after it observes confirmation (6c). No reference transition touches it: taking a root's first count and releasing its last one change only the count bits, and a `Sub` add or drop changes only the high half. Every post-construction write to `Rt` is an RMW (`fetch_add`, `fetch_sub`, or CAS), never a plain store, so each release on `Rt` heads a release sequence that later RMWs extend (9). No legal operation borrows or carries between fields.

Because a segment's `SqCs` snapshot changes across epochs, a `Ci` must cache the `Lti` used at each increment and decrement that same leaf. The ConsumerView keeps one `Lti` per held physical slot (`ltiRing`, 6a). `IDc` may later be nudged (7d, 7e); already-incremented leaves are not moved.

All leaf counters subject to dec are signed integer types, and underflow below 0 is fatal in any context. The root tally is unsigned; its underflow is detected on the count bits via `COUNTMASK`, so neither `Live` nor `Sub` can mask a genuine count underflow.

Root propagation is the plain edge rule above. The `Live` lifecycle is specified once in 6c.

### 2c. Segment Statistics

The second set of segment metadata:

- `Es` — the epoch for that segment.
- `Seqt` — the sequence that points to the first valid payload table for that segment.
- `SqCs` — from 2b, and other derived statistics as are convenient.
- `Sd` — the accounted-table-size accumulator: the sum of the sizes of all complete tables starting in this segment. See 8.

When a producer `Pi`'s space reservation transitions across one or more segments, `Pi` initializes these values such that a load-acquired `Es` matching a `Ci`'s expected value verifies the others. This is the same publication pattern as the table sentinel (5c, 9): `Es` is store-released last, after Seqt/SqCs/Sd are stored raw, so an acquire-load of `Es` that matches the expected epoch also orders the subsequent raw header-metadata reads. A valid `Es` therefore implies valid header metadata for that epoch, exactly as a valid table sentinel implies valid table contents. `Sd` is zeroed at the same time, before the release-store of `Es`. Only consumers holding a reference ever add to `Sd`, and only after validating a sentinel released after initialization, so the zeroing can race nothing.

### 2d. Payload Layout

Every payload is single-shot: exactly one consumer executes its callback once.
Independent payloads can run concurrently. A grain adapter groups several
source elements into that one callback; the Farm still counts one payload.

A producer supplies a non-null `Callback` and a `PayloadBody` slice. The
callback type is `extern(C) void function(const(ulong)* body, size_t bodyWords)
nothrow @nogc @system`. The pointer and length borrow read-only transport
words from the ring until return; they do not grant ownership of objects
reached through encoded handles. Generated D adapters reconstruct a slice
locally and additionally reject unshared mutable aliases in packed arguments.

### 2e. Table Layout

Producers commit a table `Ti` of payloads up to `Exi` in total size:

| Offset (ulongs from table start) | Contents |
| --- | --- |
| `[0 .. 5)` | Thead: 0 `Tsent` (released last), 1 `Tnext`, 2 packed length/kind/cost, 3 `SqCs`, 4 optional stable `TableCompletionHook*`. |
| next | Uniform extension words, if any (2g). |
| next | `Tlen` index words holding record offsets, absent for index-free uniform tables. |
| + 7 | padding |
| + 1 | `Tprogress`; each shard completer adds its shard length with acquire/release ordering. |
| + 7 | padding |
| next | `SqCs` Tcount counters, each followed by 7 ulongs of padding. |
| next | Compact `[Call][Pbody]` records (2f), or bare bodies (2g). |
| last 7 | end padding |

Word 2 stores `Tlen` in bits 0–31, `Tkind` in 32–39, and `AvgCost` in
40–47. Bits 48–63 are reserved and must be zero. `Tsize = Tnext - Tseq` is
derived, never stored. The base cost is `27 + 8 * SqCs` words before extensions,
indexes and records.

Tindex offsets are relative to the table start; `Tnext` and `Tsent` are
absolute sequences. `Tlen < 2^31`. Tcount packs claims in its upper 32 bits
and completed payloads in its lower 32 bits. A packed increment that wraps
the incremented field fatals. Successful run claims stay below `2^31`;
claim wrap requires pathological accumulation of unsuccessful visitors.

### 2f. Compact Tables

A kind-0 table stores each payload as `[Call][Pbody]`. Tindex holds each
record's offset. Body length is the distance to the next offset minus one;
the last record ends at `Tsize - 7`. A one-word body costs three words per
entry: index, callback and body.

The writer chooses the format from the accepted prefix's callbacks and body
lengths (2g). A compact record costs `1 + Plen`, plus its index word.
Records use raw atomic stores, and `Tsent` is release-stored last. Quota,
wrap, `prepareTableFrontN`, transactional preparation and publication obey 5b/5c.

A singleton costs `base + 2 + Plen` in every applicable layout. The contract
check uses that cost: a payload too large for its tier fatals, while a legal
payload that cannot fit the remaining grant returns backpressure (5b.3).

The reader selects the layout once per claimed run and passes each callback
its body pointer and length. Shard claims give each payload exactly one visitor.
All completion is table-level through Tcount and Tprogress (7a, 8).

### 2g. Uniform-Call Tables

A table whose committed payloads all carry the same `Call` stores that `Call` once, in a Thead extension, and each payload as its bare `Pbody`. `Tkind`, bits 32–39 of Thead word 2, names the kind; bits 40–47 carry `AvgCost` for the chunk rule (7a).

| `Tkind` | Name | Extension (from word 5) | Tindex | Records |
| --- | --- | --- | --- | --- |
| 0 | per-payload `Call` | none | `Tlen` record offsets | `[Call][Pbody]` (2f) |
| 1 | uniform `Call` | `Tcall` | `Tlen` body offsets | `[Pbody]` |
| 2 | uniform `Call` and length | `Tcall`, `Tblen >= 1` | none | `[Pbody]`, each `Tblen` words |

Tprogress, Tcount and the end pad follow the extension and Tindex exactly as in 2e. In kind 1, `Plen_i = Tindex[i+1] - Tindex[i]`, and the last record ends at `Tsize - 7`. In kind 2, body `i` starts at the payload area start plus `i * Tblen`, and that start is the Tcount start plus `8 * SqCs`. A one-word body costs 2 ring words per entry in kind 1 (index and body) and 1 in kind 2, against 3 compact (2f).

**Detection.** The writer sizes every accepted prefix by the layout it would emit. Layouts only degrade as the prefix grows (kind 2 → kind 1 → compact), and each step costs more words, so the greedy accepted prefix rule of 2f still holds. A prefix of a uniform range is uniform. When the next payload would degrade the layout and no longer fits, the table keeps the layout it was sized with. Detection is per write call:

- The `PayloadEntry` and pair forms compare each `Call` with the first.
- The forms without a supplied body length compare each body length with the first. A zero first length keeps kind 1, so no table is index-free over empty bodies.
- The common-callback broadcast forms share their `Call` by construction.
- The common-callback form with a supplied body length is kind 2 by arithmetic, or kind 1 for length 0. Sizing never inspects a body front; emission fatals on a body of another length, as before.

Without a supplied length, the writer uses separate sizing loops for the
fixed-length uniform prefix, indexed uniform prefix and mixed-callback prefix.
The first callback and body length are validated once. Further fixed-prefix
entries with that same callback and length inherit those checks; a changed
callback or length enters the next phase's validation. A layout transition
resumes at that same payload and includes the larger representation's overhead
for the entire candidate prefix. Compact sizing maintains a running sum of body
and callback words. Only an accepted entry changes the committed layout; zero
admission resets it to compact. A common-callback source needs only the first
two loops. This is a sizing specialization, not an inferred promise that later
bodies have the first body's length.

**Singleton contract (5b.3).** A singleton is a kind 2 table, or kind 1 when its body is empty. Either way it costs `base + 2 + Plen`, exactly the compact singleton, so the contract check and the set of legal payloads are unchanged.

**Reader.** One decoder derives the table offsets after sentinel validation.
Every consumer path selects the record interpretation once per claimed run.
The callback receives the borrowed body pointer and its computed length.

**Why it is safe.** The Tcall, Tblen and kind words are written with relaxed stores before the `Tsent` release, like every other Thead word, and read with relaxed loads after the `Tsent` acquire (I4). Quota, wrap, `prepareTableFrontN`, transactional preparation and publication are unchanged (5b, 5c). Claims, completion and the hook are table-level as in 2f, so nothing per-payload is mutable.

---

## 3. Safety Invariants

These are the load-bearing rules. Later sections describe mechanisms; this section is the short form of why they hold.

**I1 — Quota is strictly less than one lap.** `Exmax = maxBulk*quotaBulk + maxSmall*quotaSmall`, and `Exmax <= (K-1)*segCap` is enforced at construction. Producers only ever write within quota verified by sweeps of contiguously-forward zero Rts, and total maximum quota is bounded by K-1 segments' capacities, strictly less than a lap.

**I2 — Consumer pins form a contiguous epoch range and move increment-ahead.** A consumer holding any tally protects not just that segment but everything ahead of it within one lap: any sweep far enough ahead to threaten the consumer's unread data must pass contiguously through the held segment and break. Conversely, a producer cannot jump a lap on blind quota alone, so the protection cannot be outflanked. Consumers always increment a tally ahead before decrementing their current one, so the protected span has no zero gap.

**I3 — Every epoch is protected from its initialization until it is discharged.** The producer whose reservation crosses into epoch `e` raises `Live` on `e`'s slot before it release-stores `Es[e]` (5c), and `Live` is cleared only by a consumer that holds `e` and has observed it confirmed (6c). A slot whose `Live` is set never reads `Rt == 0`, so no sweep admits it while its epoch is undischarged, whoever is or is not subscribed. Unfinished work needs no other protection; departures, including the last one, create none (6e).

The only zero-root interval of an epoch before its discharge lies between its producer's reservation and that producer's `Live` raise. A qualifying sweep still cannot admit the slot for its next lap. A sweep that reaches `e`'s slot as `e + K` either passed `e - 1`'s slot earlier in the same scan or, when `e`'s slot is its first, acquired through the `Wt` reservation chain (9) the sweep that admitted `e - 1`'s slot for its own next lap. Either way it acquired a zero root for `e - 1`, which by induction can only follow `e - 1`'s discharge:

```text
raise Live(e)                         the producer of e, before Es[e]
  -> release-store Es[e]
  -> acquire Es[e]                    the holder confirming e - 1 (8)
  -> release CAS clears Live(e - 1)   that holder's discharge (6c)
  -> acquire a zero Rt for e - 1      the sweep, directly or via Wt (9)
  -> that sweep's read of e's slot sees Live(e) or a later value
```

Epoch 0, which the Farm publishes Live (4b), is the base case. Hence a sweep admits only slots whose current epoch has been confirmed and discharged, and I1 with the retained-quota chain (9) extends this to every producer spending a grant. The induction is over slot generations: a zero from an earlier lap is not evidence about a later epoch.

A consequence used by subscription (6b): the earliest unconfirmed epoch is always initialized and Live, even on a Farm with no consumers, because confirming its predecessor required its `Es`, which is published after its `Live`.

**I4 — Raw ring reads occur only after publication and while pinned.** The ring is read without acquire only after an acquire-validated `Es` or `Tsent` publishes the initialized values, and only while the reader holds a tally on the relevant segment (9).

**I5 — Producer reuse requires a contiguous sweep observing `Rt == 0`.** Confirmation says table accounting is complete; it does not itself grant reuse. A producer admits a slot for reuse only when `refreshQuota`'s contiguous forward sweep observes `Rt == 0` for that slot. The sweep's anchor and contiguity, plus I2 and I3, make zero sufficient there.

Safety argument in one paragraph: producers only ever write within quota verified by sweeps of contiguously-forward zero Rts, and total maximum quota `Exmax` is bounded by K-1 segments' capacities, strictly less than a lap. Every epoch is Live from its initialization until a consumer that confirmed it discharges it, so unfinished work is never admitted for reuse, whether or not anyone is subscribed (I3). A consumer holding any tally also protects everything ahead of it within one lap (I2), which covers the data it is still reading; consumers retain references until segments are confirmed (8), and idle consumers keep a pin on the frontier segment (6d). Consumers' unread data is always protected, and the system never overwrites unexecuted work.

---

## 4. Construction and Registration

### 4a. Geometry and Quota Constraints

`create` takes the ring size as `ringMiB` and derives `Ln = ringMiB << 17` ulongs. It enforces:

| Constraint | Current value | Kind |
| --- | --- | --- |
| `K` in `[2, KMAX]` | `KMAX = 16` (section 1 suggests 4 or 8) | implementation limit, power-of-two |
| `ringMiB` power of two in `[1, MAX_RING_MIB]` | `Ln >= 2^17` ulongs (1 MiB; 2 MiB before 2.1.1); `MAX_RING_MIB = 65536` (64 GiB) | implementation limit; the 2.1.1 ring-size study found a ring's cache footprint is its live window, not its length; the upper bound rejects a length mistakenly given in ulongs |
| `segCap` floor | 2048, subsumed by `Ln >= 2^17` with `K <= 16` | keeps per-table header/pad space from dominating a segment |
| `Exmax <= (K-1)*segCap` | enforced | algorithmic invariant (I1) |
| quota role | `quotaRole > 0` iff `maxRole > 0` | algorithmic invariant |

An enabled tier must declare a positive quota. Bulk auto-defaults to `segCap` when unspecified (legacy convenience); small fatals on 0. A disabled tier's quota is normalized to 0 and takes no part in `Exmax`.

`Exmax = maxBulk*quotaBulk + maxSmall*quotaSmall`, the maximum simultaneous excursion of the Farm's maximum set of producers.

### 4b. Epoch 0

At Farm creation epoch 0 is initialized exactly as a producer would initialize an epoch: `Es[0] = 0`, `Seqt[0] = 0`, `SqCs` from the expected consumer count, and its root constructed as `Live`. There are zero known subscribers and no leaves or shards in which to distribute them, and none are needed: `Live` keeps producers from lapping Seq 0 until a consumer confirms epoch 0 and discharges it (6c), and the first subscriber finds epoch 0 by the ordinary scan (6b). Construction publishes these values with the Farm, which is the base case of I3. On a fresh farm the first subscription should succeed; a failure (oversubscription, or RETRY under a racing producer) returns a negative code per 6b and is not itself fatal.

Note that `Live` does not block producers from writing into epoch 0. The segment where the write tail resides is never examined by its own producer's sweep, and epoch 0 begins with a full segment length ahead of the write tail and consumers bounded behind it.

### 4c. Consumer Capacity

A subscriber `Si` calls `F.add_consumer()`, which fetch_incs `Cf`. If a slot is available, it fetch-incs `Reqs_c` and returns that result as `IDc`. If not, `F` is oversubscribed and `Si` fetch-decs `Cf` and returns `SUBSCRIBE_FULL`. Every subsequent subscription failure path also unwinds `Cf` (6b). A rejected or failed attempt can make `Cf` overcount briefly; that can only make another attempt conservatively `SUBSCRIBE_FULL` or size a table's `SqCs` high, because nothing derives protection or lifetime from `Cf` (2a, 6e).

### 4d. Producer Tiers and Tickets

The Farm uses two tiers of producer to cap the max excursion, so one or a few enumerated batch writers can commit large chunks and the thread pool can occasionally write a couple kilobytes at a time. The single write tail is a linear point of contention and it's on a performant developer's shoulders to control the frequency of adds to it. (Though testing has revealed batch sizes reasonably above 1 to be the most crucial aspect.) Important invariant: the number of *actual* simultaneous producers **must** not exceed the maximums to prevent stomping consumers.

Registration is how that invariant is enforced. The Farm preallocates two 64-byte-strided slot arrays, one for each producer tier. A Farm-local blocking mutex serializes complete registration and deregistration transactions, including the hash, authoritative quota ledger, and tier counter. Publishing and consuming do not acquire it. This prevents reuse of a released hash before the departing producer finishes clearing its ledger.

`registerProducer(tier)` checks the tier capacity, assigns a hash derived from the slot and `Reqs_p`, stamps the token with the Farm's `FarmId`, initializes the quota ledger to zero, and returns a valid Token with zero remaining quota. Its first nonempty write must obtain a checked grant through the same forward sweep as every later renewal. `unregisterProducer(ref Token)` validates the token's Farm provenance and live slot, clears its hash and ledger, invalidates the token, and releases the tier capacity while holding the mutex.

`write()` takes the Token by reference and fatals if the token's `FarmId` is not the receiving Farm's, if the hash does not match the live slot, or if the token's private quota mirror exceeds the farm's ledger for that slot. Unregistered threads therefore cannot call `write()` and cannot bypass `Exmax`, a registered caller cannot forge a fresh quota past its grant by mutating the token, and a token registered on one Farm — including a since-destroyed Farm whose identity is never reissued — is fatal on every other Farm. The slot hash alone does not prove ownership: two Farms with identical registration histories derive identical hashes, so provenance is a separate deterministic field, not another probabilistic check.

Tokens are single-owner and move-only: copying does not compile (the transfer-on-copy constructor is disabled with `@disable this(ref typeof(this))`), so at most one live token exists per slot and a stale copy fails `requireToken`. When a producer leaves, it can pass its quota along only by transferring the Token with an explicit `move`; there is no undocumented inherited quota.

---

## 5. Producer Protocol

### 5a. Quota Arithmetic and Sweeping

Let `Exi` be a producer `Pi`'s running quota of ulongs: the amount it may blindly reserve before probing its actual position or checking for consumer references. Let `quotaLeft` be the token's current remainder of that grant, and let a reservation be the specific span a `write()` claim covers. `Exmax` is the sum of all tier grants (4a); each `Pi` regards `Exmax` as a runoff zone it must observe before renewing `Exi`.

Every quota grant requires a fresh acquire-load of `Wt` followed by a forward-segment sweep from that anchor. This applies to the first grant after registration as well as later renewals. A successful reservation only subtracts its table size from the remaining grant. There is no automatic renewal based on the reservation's returned tail or the amount of space remaining in its segment.

Once a sweep grants quota, the producer may spend that finite balance across table writes and segment boundaries. If no next payload fits the balance, `write()` probes `Wt` and attempts a new sweep; a failed sweep returns 0 without granting additional space. There is no separate unchecked-leftover state or `quotaSwept` flag. Reservations of `Wt` are acq_rel RMWs. Release pairs with the acquire probe used for each grant. Acquire is the producer-to-producer reclamation handoff: a producer may spend a retained balance long after its sweep, while other producers sweep and lap the ring. Its reservation must acquire the history those producers imported (consumer pin drops → `Rt` release → zero-`Rt` sweep acquire) before it stores into the words it reserved (9).

If a `Pi` has depleted its swept quota, probed its `Wt` position, and needs to check following segments to make `Exmax` space, then it scans the K-1 segments ahead of the probe for contiguous 0 Rts, adding their full capacities until it has found `>= Exmax` space or encounters a live segment and breaks. The same acquire-load is the sweep's anchor; a second probe is not taken. If sweeping fails then backpressure is simply too high; full for `Pi`'s purposes. A stalled producer may subscribe a ConsumerView, drain, and retry `write()` — that is the supported escape hatch, not an out-of-band trick. The sweep cannot land on spuriously orphaned islands of space: consumers maintain continuous reference spans without gaps, and every undischarged epoch is Live (I3).

### 5b. write() Call and Return

Arguments:

- `Payloads` — a slice or forward range of `PayloadEntry` structs. `write()` saves independent checkpoints for sizing and emission and does not advance the caller's range.
- `Token` — the caller's registration ticket (4d). Required; a mismatch is fatal. The Token is passed by reference; it carries the issuing Farm's `FarmId` and the caller's remaining quota as private fields, and the Farm mirrors the quota in a per-slot ledger so a forged ticket cannot exceed the granted excursion.
- `AvgCost` — optional, default 3 (`DEFAULT_AVG_COST`, chunk 16). The producer's declared `Call` cost class: a log2 shift in `0 .. log2(MAX_CHUNK)` (`0 .. 7`) published in Thead. 0 means `Call`s are very cheap (chunk stays at `MAX_CHUNK`, maximum claim amortization); larger values shrink the chunk (short visits, less tail). Classes 0 through 2 (chunks 128, 64, 32) are for very cheap uniform work; the default suits ordinary work; higher classes suit expensive work. `write()` fatals on `AvgCost` out of range.

Returns: the number of Payloads written.

Convenience sources preserve the same table contract: separate callback/body forward ranges are paired positionally; one common callback may be broadcast over a body range; and the common-callback overload may additionally receive a uniform body length in ulongs. The last form treats that length as a caller contract and uses it for arithmetic sizing before emission.

`PayloadEntry`:

- `Callback call` (2d).
- `PayloadBody body`; where `alias PayloadBody = const(ulong)[]`; type-erased data. (A mutable slice of const ulongs; `const ulong[]` would const the slice itself and make `PayloadEntry` unassignable in D.)

The payload/table sizing contract, in order:

1. `write()` validates every payload callback before accepting it. The callback must be non-null (2d). Violations are fatal.
2. Size arithmetic is overflow-checked before any compare to quota. If singleton overhead plus body length, the running payload sum, candidate table size, or final table size would wrap, that is fatal. A wrapped candidate must never be compared to `Exi`.
3. A payload that cannot fit in the *caller tier* maximum `Exi` is a caller error, not backpressure. If a singleton table containing only that payload — Thead, index slots, pads, Tcount words for the current `SqCs`, the payload, and the end pad — exceeds the caller's tier quota, `write()` fatals. A payload's singleton costs what a compact table (2f) costs; it is written as a uniform table (2g). The check is by caller tier, not `max(quotaBulk, quotaSmall)`, because a payload that only the other tier could ever publish would remain in a small producer's input range on future calls. Thus it's a contract failure that must be wrangled upstream, and `write() == 0` strictly means the farm is full for `Pi`'s purposes.
   Producers can pre-check with `AntFarm.canFit(Callback call, ulong bodyWords, Tier tier) const nothrow @nogc`, where `bodyWords` is in ulongs. It returns false for a null callback, overflowing size, or singleton larger than the selected tier's quota, using the same singleton cost and live-consumer `SqCs` snapshot as `write()`. It reserves nothing and does not predict backpressure or multi-payload admission. The answer is not monotone: consumer growth can make true become false, and departures can make false become true. `write()` still validates independently and fatals on invalid or oversized payloads, including after a previously true pre-check if `SqCs` has grown.
4. Multi-payload truncation to the caller's current `Exi` stays legal: if `Ti`'s size exceeds a depleted `Exi`, `Pi` attempts to refresh it, then keeps however many Payloads fit. The committed table also carries fewer than 2^31 payloads (2e); the sizing scan breaks at that bound exactly as it breaks on quota, so a longer input range is truncated, not fatal.

At this level producers do nothing else to resolve fullness; they merely return commit lengths dwindling to 0. A stalled producer may subscribe a ConsumerView, drain, and retry (5a).

Every `write` overload accepts an optional trailing
`TableCompletionHook* completion = null`, after `avgCost`. This applies to
compact mixed-callback tables as well as both uniform layouts. A non-null hook
must have a callback, including for empty input. Its pointer is stored in
Thead word 4; a null pointer disables notification.

The `Tprogress == Tlen` finisher invokes the hook exactly once after every
payload callback in that accepted table has returned, using the existing
acquire/release join. There is no additional per-payload completion operation.
The Farm snapshots the hook's function and context and never accesses them
after invocation. Their storage must stay stable until invocation; a hook may
release or transfer it as its final access, otherwise retain it through return.

The hook covers one accepted physical table, not an unadmitted suffix. A zero
write, empty input or failed preparation invokes no hook. Aggregate producers
reserve a table credit before calling and roll it back on zero acceptance:
a consumer may complete a table before the publishing call returns.
Actor waves and [private actors](docs/guide/private-actors.md) use the same
optional hook on `write`.

### 5c. Reservation and Publication

The ordered contract of a write:

1. Reserve `Wt` with an acq_rel RMW; the returned `Wret` plus the reserved size gives `Wt'`, the sequence after this write. The acquire half imports other producers' reclamation history before any ring store (5a, 9).
2. Deduct the reserved size from the checked quota grant; do not renew it.
3. If the reservation crosses into new epochs, run the fresh-segment diagnostic: each such slot's `Rtlow` must be zero (a provisional `Sub` is allowed), else fatal. Then, for each new epoch in order, initialize its metadata (2c), raise its `Live` with a release RMW, and release-store its `Es` last. A `Live` raise that finds the low half nonzero is fatal: no qualifying sweep could have admitted such a slot. From here until a holder confirms and discharges it, `Live` protects the epoch (I3, 6c); the write needs no later protection step.
4. Write the table parts described in 2e/2f/2g, into the reserved space.
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

ConsumerView is initialized with `subscribe(AntFarm* F)`, which runs the one-attempt protocol below (`subscribeOnce`) until it does not report a transient outcome.

**Returns:** the non-negative starting epoch on success. Failures are negative: `SUBSCRIBE_FULL` (-1, capacity) or `SUBSCRIBE_INVALID` (-3, null Farm or already subscribed view). `subscribeOnce` additionally returns `SUBSCRIBE_RETRY` (-2: the write tail outran the attempt's bounded rounds; every `Sub` and `Cf` was unwound) instead of retrying. Epoch 0 is a valid success.

An attempt finds the earliest Live epoch it can see, protects it with a `Sub`, validates it, and attaches there.

**a. Admission.** The subscriber `Si` calls `F.add_consumer()` (4c). If `Cf` is full, `Si` returns `SUBSCRIBE_FULL`. Every later failure path unwinds `Cf` too.

**b. Rounds.** The attempt runs at most `K + 2` rounds. Each round:

1. acquire-loads `Wt`; `eg = Wt >> segShift` is the round's frontier and the logical epochs `[lo, eg]`, `lo = max(0, eg - K + 1)`, are its window;
2. adds a `Sub` (`fetch_add`, release) to the frontier's slot, unless this attempt already holds one there;
3. reads the window in increasing epoch order as *hints*: for epoch `x`, acquire-load `Es` of `x`'s slot, then acquire-load its `Rt`, with no RMW. The slot qualifies for `x` only if `Es == x` and `Live` is set;
4. at the first qualifying `x`, adds a `Sub` to its slot and reads it again the same way. If it no longer qualifies (discharged or reused between the two reads), the round drops that `Sub`, unless the attempt already held it, and continues with `x + 1`;
5. validates the re-read candidate (c) and attaches (e), or ends the round when validation fails.

A round ends without attaching only when the write tail moved past what the round read. After `K + 2` such rounds `Si` drops every `Sub` it holds, unwinds `Cf`, and returns `SUBSCRIBE_RETRY`. The bound is a budget, not a proven sufficient count: `RETRY` must stay recoverable, and no watchdog may treat a long run of retries as corruption. An attempt holds at most one `Sub` per physical slot, so at most `K`; frontier `Sub`s accumulate across its rounds and are dropped together at attachment or `RETRY`.

**The frontier `Sub`.** At a current probe the frontier epoch cannot be confirmed, because its successor is not initialized, so it is `Live`, and by I3 no sweep can already have admitted its slot for the next lap. While held, that `Sub` keeps the write tail from lapping the slot, so the window can move at most one lap during the attempt. A `Sub` on an older, already-discharged slot has no such guarantee: quota granted before the `Sub` landed ignores it (5a). Protection of the attach target therefore never rests on extra pins; it rests on `Live` observed under the target's own `Sub` (e). With a stale probe the frontier `Sub` may land on a slot the tail has already passed; that round fails validation or finds nothing, and the next round pins the current frontier.

**`Es` before `Rt`.** A producer raises `Live` before it release-stores `Es` (5c). Loading `Es` first means a matching `Es` imports that raise, so the following `Rt` load observes `Live` unless the epoch has since been discharged: "`Es` names `x` but `Live` is clear" means discharged, never "not yet raised". An older `Es` is an initialization still in flight; a newer one means the slot was already reused for a later lap. The oldest-first scan does not depend on this order for discovery: an epoch whose initialization lands between the two loads cannot be the earliest unconfirmed one, because its predecessor could not yet be confirmed, and the discovery argument below never needs it. The order makes each classification exact, which a scan that stops at a discharged slot would require; loading `Rt` first could make an initializing epoch look discharged.

**Discovery.** Let `B(t)` be the earliest unconfirmed epoch at time `t`. `B` only increases, and `B(t)` is always initialized and `Live` (I3). At the probe `B >= lo`, since `B`'s slot cannot be lapped while it is `Live`. Reading in increasing epoch order, each read of `x` either finds `x < B(t_x)`, a confirmed epoch the scan may pass whatever it reads, or is the first read with `x >= B(t_x)`. That `x` equals `B(t_x)`: for `x = lo` because `B >= lo`, otherwise because the previous read had `x - 1 < B(t_{x-1}) <= B(t_x)`. `B(t_x)` is initialized and `Live` at `t_x`, so it qualifies. A pinned re-read that no longer qualifies means `x` has since been confirmed, and the induction continues from `x + 1`. The round's first qualifying slot is therefore at or before the earliest unconfirmed epoch at some instant of the round, and because `B` only advances, attaching there skips no unfinished work. If the whole window was confirmed during the round, the round finds nothing and the next one re-probes.

Under weak memory the same induction is carried by happens-before rather than by instants. The probe's acquire of `Wt` imports, through the reservation chain (9), the discharge of the epoch whose slot the frontier reused, and so `lo`'s initialization (for `eg < K - 1`, `lo = 0` and epoch 0 is published with the Farm). Passing `x` because its slot reads discharged imports, through the discharging holder's release CAS, that holder's acquire of `Es[x + 1]`; passing it because the slot was reused imports, through the reusing producer's `Live` raise and reservation, the same discharge. Either way `x + 1`'s initialization is visible before the next read. This chain is argued here, not machine-checked; see the open obligations in 11.

**c. Validation.** Under the candidate's `Sub`, `Si` re-loads its `Es` (acquire), `SqCs` and `Seqt` (raw, ordered by that acquire), then `Wt` (acquire). It accepts only if `Es` still equals the candidate epoch `c`, `SqCs != 0`, `Seqt >> segShift` lies in `[c, c + K - 1]`, and `Wt >> segShift < c + K`. The `Es` identity rejects a slot reused after the re-read, and the `Seqt` range rejects a snapshot that mixes generations. The `Wt` bound rejects the one way a matching `Es` can come with another epoch's `Live`: a new lap's `Live`, raised before its `Es` is published, acquired beside the old lap's `Es`. That raise is a release RMW sequenced after its producer's `Wt` reservation, so acquiring it imports the reservation, and the `Wt` re-load names the candidate's next lap. A failed validation drops the candidate's `Sub`, unless the attempt already held it, and ends the round.

**d. `Sub`.** `Sub` is `2^32` such that it tracks subscribers in the most significant half, apart from consumers' counts and `Live`. An attempt holds at most one `Sub` per physical slot, and concurrent attempts are bounded by admission (4c), so `K × MAX_CONSUMERS_LIMIT` is far below `2^32`. A `Sub` only stops sweeps that read the slot after it lands; it does not revoke quota already granted (5a), and it never raises or clears `Live`.

Hint reads take no `Sub`; they only choose where to pin. Attachment never rests on a hint: the candidate is re-read under its own `Sub` and validated before any leaf increment. That is why unpinned reads, which could mix generations, are acceptable here and nowhere else.

**e. Attachment.** While its `Sub` on the candidate is held, `Si` performs the normal leaf tally increment and root propagation (2b), caching the `Lti` used, and takes the candidate epoch as its position with `nextSeq = Seqt`. The candidate cannot be reclaimed meanwhile. `Live` for one epoch is raised once and never again, and `Si` observed it under its `Sub`, with the `Wt` bound excluding a newer lap's `Live`. So no sweep before the `Sub` landed can have admitted the slot, since that would have required this `Live` clear, and none after can while the `Sub`, and then `Si`'s reference, is held, even if the epoch is confirmed and discharged in between. Epochs after the candidate within the lap are covered the same way, because any sweep reaching them first passes the candidate's slot (I2). `Si` is now a consumer `Ci`.

**f.** `Si` fetch_sub-releases every `Sub` the attempt holds, including the attach slot.

Failure reasons, capacity or a tail that outran the attempt's rounds, unwind `Cf` and every `Sub` before returning `SUBSCRIBE_FULL` or `SUBSCRIBE_RETRY`.

### 6c. The Live Lifecycle

`Live` is one bit per root (2b) and belongs to the epoch currently in that slot. This is its whole lifecycle:

| Event | Actor and preconditions | `Rt` transition | Purpose |
| --- | --- | --- | --- |
| construction | `tryCreate`, for epoch 0 | `Rt[0] = Live` | epoch 0 starts initialized and protected (4b) |
| raise | the producer whose reservation crosses into epoch `e`, after the fresh-segment diagnostic and before `Es[e]` (5c) | release `fetch_add(Live)` on a zero low half; fatal otherwise | protect `e` from initialization on (I3) |
| discharge | a consumer that holds `e` and observed it confirmed (8), before releasing its reference | load; if `Live` is set, release CAS clearing it | allow reuse once no reference or `Sub` remains |

Nothing else writes `Live`. Taking a root's first count and releasing its last change only the count bits, and `Sub`s never touch it.

**Discharge rule.** A consumer clears `Live` on epoch `e` only while it holds a reference on `e`, and only after observing confirmation: `Es[e] == e` and `Es[e+1] == e+1` by acquire loads, and `Seqt[e] + Sd[e] >= Seqt[e+1]`. Its reference keeps the slot from reuse until the clear lands, so the clear cannot hit a later generation's `Live`; an unpinned or delayed clearer could. Clearing is idempotent across confirmers. Each loads first and returns when `Live` already reads clear, so passers that find it discharged do not write the root line, and the CAS retries only while another field (a count or a `Sub`) changes under it. The release CAS forwards the confirmer's acquire of `Es[e+1]` to every later acquire of the root; that edge is the inductive step of I3 and of subscription's discovery argument (6b).

Two release paths discharge: the confirmed-prefix release (`tryReleaseOldest`, run after movement, on the idle re-walks of 8, and first in departure), and departure itself for any remaining held epoch that is confirmed (6e). Releasing an unconfirmed epoch leaves `Live` in place.

**Every `Live` bit has a discharge path.** A consumer enters every epoch it passes: movement increments each epoch through the new position (6d), including interiors of spanning tables, which confirm trivially, so a passer releases and discharges them. An epoch abandoned while unconfirmed keeps `Live` with no holder. It is at or after the earliest unconfirmed epoch, so the next subscription attaches at or before it (6b), and that consumer's movement discharges it once its work is done. An epoch that confirms only after its last holder has left, such as an interior a subscriber attached to before its successor existed, likewise keeps `Live` until the next consumer passes it. Until then it blocks reuse of its slot as an unfinished epoch would, and the next subscriber's scan reaches it because it is `Live`.

**No departure creates protection.** The last consumer to leave simply releases its references. Its unfinished epochs keep the `Live` they have had since initialization, its confirmed ones are discharged, and the frontier epoch stays `Live` because it cannot be confirmed. A Farm with no consumers therefore keeps producers from lapping its earliest `Live` epoch, and the next subscriber's scan finds that epoch (I3, 6b). There is no empty-Farm anchor and no last-of-Farm case.

Producers see the signal in `refreshQuota`'s `Rt == 0` check, where `Live` alone reads as occupied, and subscribers see it in the scan's `Live` test, so an abandoned incomplete epoch is both protected from overwrite and discoverable for reclamation.

**The hard invariant.** A discharged epoch is confirmed. The converse need not hold at every instant: a confirmed epoch keeps `Live` until its next holder releases it.

### 6d. Orientation and Frontier Migration

A `Ci` often needs to seek data, for instance if it's newly subscribed. Segment metadata provides a reasonable starting point. Consumers load-acquire the segment epoch and return if it is not the expected value (the caller retries or treats the frontier as not yet written). From there it has a starting Seq to a table, where the same applies to the sentinel value at that Seq.

Seek costs per entry are primarily amortized by publishing large tables. Within tables the `SqCs+1` counters — `SqCs` shard counters for contention plus `Tprogress` for completion — keep a small target for both. (I considered building in skip lists or other structures dynamically updated by consumers, but the design sticks with what's essentially a linked list of one table pointing to the next; see 11.)

**Ordinary table advance.** As consumers move to new tables, they check transitions across segment boundaries. `Ci` incs a leaf tally at every epoch from `newestEi+1` through the newer table's epoch when it *validates* that table's sentinel — a valid sentinel implies the producer has initialized those segments' stats, so the increment always reads a live `SqCs`. `newestEi` advances to that epoch; `oldestEi` is unchanged. The old position stays held until the confirmed prefix reaches it (8). `IDc % SqCs` at the increment is the leaf recorded in `ltiRing`; later `IDc` nudges do not retarget it.

**Idle frontier migration.** `moveRef` on the hot path still runs only after a successful sentinel validate. That is not enough: a spanning table can start in `Ki` and end at a `Wt'` several epochs later, leaving `Ci`'s position on confirmed-complete `newestEi` while `nextSeq` names a later initialized segment whose table is not yet published. `refreshQuota` counts full free segments from the write epoch + 1; a stale pin in that sweep window starves renewal even though the consumer is idle at `Wt`. Therefore, on a sentinel miss at `nextSeq` (and after a consume that advanced `nextSeq` into a new epoch), if the current position is confirmed complete and `Es[ki]` matches `ei` for `nextSeq`'s `(ki, ei)`, `Ci` advances `newestEi` onto that segment and then releases any confirmed prefix. Increment-ahead, always at least one pin, pin tracks the frontier.

`nextSeq` is always `Tnext` of a published table, which is that write's `Wt'`. Crossing writes already release-store `Es` through that epoch (5c), so the segment of `nextSeq` is already initialized. The case one cannot migrate into — `Ki+1` with `Es` unset — only exists when no write has crossed, which means `nextSeq` is still inside `Ki` and the pin is on the write segment, which the quota sweep does not examine. Restricting `Exmax` (for example to half the buffer) does not replace this migration: a smaller `Exmax` only *tolerates* a pin left on a completed start.

Thead is defined in 2e. Orientation uses `Tsent`, `Tnext`, `Tlen`, `Tkind`,
`AvgCost`, and `SqCs`; size is derived from `Tnext - Tseq`.

### 6e. Unsubscription

Inactive consumers block reclamation unless they `unsubscribe()` from `F`.

**a.** `Ci` first releases the confirmed prefix from `oldestEi` toward `newestEi` (8), discharging each epoch (6c) before its reference drops. Only the old end moves; confirmed interiors stay pinned until they become oldest.

**b.** `Ci` calls `F.sub_consumer()`. `Cf` is a capacity and sizing count (2a); the returned value is used only to detect underflow.

**c.** `Ci` releases every remaining epoch in `[oldestEi, newestEi]`, oldest first. A confirmed epoch is discharged before its reference drops; an unconfirmed one keeps its `Live` and is released with the plain edge rule of 2b. There is no last-of-Farm case and no protection to manufacture: unfinished epochs have been `Live` since initialization (I3), and the next subscriber finds the earliest of them by the ordinary scan (6b). A non-last unsubscriber that was the only holder of an early incomplete epoch leaves that epoch exactly as protected and discoverable as the last one would.

Postconditions: every unconfirmed epoch `Ci` held is still `Live`, and every confirmed epoch it released is discharged. A Farm whose consumers drained every published table and then left keeps exactly one `Live` epoch, its frontier: each passer discharged the epochs behind it.

---

## 7. Work Scheduling

### 7a. Work Claims

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

**a.** `Tprogress` gives an O(1) view of the table's overall accounting; `Ci` checks if `Tprogress == Tlen` and, if so, skips. Every payload callback has returned; the consumer can advance without another payload pass.

**b.** If `Tlen` is below the table's small threshold, consumers with `Shi == 0` claim the entire table by chunks as below; the others do not. The threshold is a farm field; 0 selects the bounded heuristic `clamp(SqCs * Chunk, 16, 256)`. Below the threshold all work is assigned to shard 0; at or above it the table uses all `SqCs` work shards. Away from the clamp limits, the heuristic begins sharding when there is roughly one full chunk per shard. The default fixed value is 64. All consumers derive the same threshold from the same header/farm values.

**c.** `Ci` gets claim `X = fetch_add(Shc, 2^32) >> 32`. If `X >= Shiter` the shard is already fully claimed and we finish this shard. `X` is also the feedback signal used by 7d.

**d.** If `X < Shiter` it maps to a run of Tindex of Chunk length at `Shstart + X * Chunk`. If `X == Shiter-1`, `Ci` truncates the run to the remainder (`Shlen & (Chunk-1)`) if it's nonzero.

**e.** When a `Ci` finishes a run do `Y = fetch_add(Shc, run_length)` with acquire/release ordering; if the low half of `Y` equals `Shlen - run_length`, then `Ci` completed the shard and adds `Shlen` to `Tprogress` with acquire/release ordering. The completion RMW joins preceding run callback writes within the shard; the `Tprogress` RMW joins those shards. Every payload callback contributes through its claimed run, including runs claimed by sweepers and re-walkers. A relaxed shard completion sum would not establish the within-shard join. This bounds the number of mutations to `Tprogress` to `SqCs`. The completer additionally gains the *sweeper role* for this table (7e), and if its `Tprogress` add lands exactly on `Tlen` it is the table's finisher and performs the Sd accounting (8).

**f.** Then `Ci` returns to step c; or if `X >= Shiter` it finishes this shard.

Each payload belongs to exactly one disjoint shard run. Every visitor uses those same run counters; no visitor takes over an already claimed run.

### 7b. First-Claimant Mid-tick Yield

The `Ci` that drew `X == 0` for a shard (the first claimant, at most one per shard) may, after finishing a run that did not complete the shard, read the claims half of `Shc`. If it is greater than the number of claims that `Ci` has personally made in this shard, the shard is shared: at least one other consumer has claimed a run here. The first claimant may then probe `Tnext`'s sentinel; if it is live, it may return from its assigned shard immediately instead of returning to step c, so `consumeNext` can advance to the already-published table (for example a mid-tick 1-payload write). The Tcount/`Tprogress` arithmetic is unchanged: the yield happens only after `Y = fetch_add(Shc, run_length)`, so the run is accounted, and the other claimant(s) will finish the shard. Unsubscription after the yield cannot starve the shard; OS pre-emption can only delay it, and the idle re-walk of 8 remains the backstop. Sweeper and re-walk calls (`checkFirst`) never yield early.

Priority of roles: if the same run that would qualify `Ci` for the yield is also the shard-completing run (the 7a-e completion sum lands on `Shlen`), the completer outcome takes precedence — `Ci` gains the sweeper role for this table and does not yield early, so the completer's foreign-shard sweep is never lost to a mid-tick advance. The yield is only considered after a run that did not complete the shard. A yielded `Ci` also drops any carried sweeper role (7e): it begins the next table as a normal consumer without role baggage. Only a shard-completing result advances the sweeper role; observing the shared-shard signal permits the yield and does not grant the role.

### 7c. Payload Execution

Consumer callbacks and table-completion hooks run inside an active view walk.
That walk is not re-entrant: nothing inside a callback may call
`consumeNext` on the running view or wait for ring space. A thread-local
callback flag records the walk for diagnostics. Nested work forks onto the
fork-join dispatcher or publishes asynchronously with a hook
([parallel loops](docs/guide/parallel-loops.md)).

The consumer executes every payload in its claimed run once, synchronously,
then accounts the run through 7a-e. It borrows the ring body as described
in 2f/2g. A grain callback may process multiple source elements sequentially;
its entire execution precedes the run completion RMW. Other consumers claim
other runs and can execute their callbacks concurrently.

### 7d. Oversaturation Feedback

If `Ci` adds no work in its assigned shard, it was entirely oversaturating the table. The value `X` from 7a-c has useful properties.

Let `Z = X - Shiter`. After the assigned shard's iterations are accounted, each `Ci` that fails to claim a run increments `Shc` one more time and observes the shard fully claimed; `Z` counts those exhausted-claim observations, not callback completions. If `Ci` observes `Z` significantly over `SqCs`, it can check the Farm level `Cf` and see if the shard is overloaded. `Ci` then nudges its `IDc` by +1. Already-incremented leaves are not moved. On the next table (and the next segment pin) `Ci` lands in a different bucket of consumers. This reduces an overallocated shard via a local feedback signal.

This is a scheduling heuristic: it affects future shard/leaf selection but is not required for memory safety.

**Undersaturation repair** (complementing 7d). A sweeper reads the same `Z` value on foreign shards. Finding `Z <= 1` — evidence, not proof, that no native consumer exists there — it nudges its `IDc` by the round-robin distance into that shard, `IDc += (Shi_foreign - Shi_own + SqCs) % SqCs`, patching the hole a pre-empted or unsubscribed native left. Already-incremented leaves stay on the ring; the new `IDc` is the next pin's leaf target. A native laggard waking to an overbalanced shard nudges itself out via 7d. The signal is self-limiting: each entering sweeper's failed exit-claim raises the shard's `Z`, so it disappears after a couple of adopters. A consumer adopts at most one shard per table. Small tables are excluded.

### 7e. The Sweeper Role

The shard finisher claims foreign shards' leftovers and carries that role
one table forward for small tables. This is a scheduling heuristic; safety
does not depend on it, and the idle re-walk in section 8 provides liveness.

The consumer whose 7a-e completion sum lands on `Shlen` — adding `Shlen` to `Tprogress` — gains the sweeper role for that table. This pulls one sweeper from each shard, so it takes all shards' completers being gone (hence no consumers and no progress anyway) for work to starve, without forcing much extra contention. The sweeper makes one linear pass:

- **Foreign shards:** for each shard other than its own, a plain load short-circuits fully-claimed shards (`claims >= Shiter`) without an RMW; otherwise it attempts to claim and process runs exactly as in the assigned-shard path. The 7a-e completion rule fires for whichever consumer lands the final sum, owner or sweeper, so `Tprogress` mutations stay bounded by `SqCs`.

Sweeps of foreign shards do not recursively confer the role. A late visitor
finding `Tprogress == Tlen` skips payload execution. The finisher may still
be running a completion hook, protected by its own retained view pin.

**Carried role and small tables.** Small tables concentrate all claims on shard 0 (7a-b), so under churn they may have no native consumer at all. A consumer that completed a shard of the previous table therefore carries the sweeper role one table forward: if the next table is small, it sweeps that table's single shard regardless of its own `Shi`, and if the sweep completes the shard the role chains forward again. The idle re-walk of 8 remains the backstop, but the hot path no longer stalls producers waiting for it. A complete table does not clear the carried role: skipping it leaves `sweeperNext` untouched, so a role carried across consecutive already-complete tables survives to the first table that still needs sweeping.

---

## 8. Table Accounting and Segment Reclamation

This is the canonical home for the completion/reclamation states defined in 1. Two facts drive the whole story: `Sd` says how far table accounting has advanced; `Rt` says whether ring memory is still protected. Completion permits confirmation; only the pin-drop and acquire-sweep protocol permits storage reuse.

`Tcount` accumulates runs per shard. `Tprogress` is the table's accounting: when it reaches `Tlen`, every Tindex run has been claimed, executed, and accounted. The acquire/release shard and table chains join all payload callback writes. The table's finisher — the `Ci` whose `Tprogress` add lands on `Tlen` — adds `Tsize` to `Sd` of the table's starting segment.

Every `write` overload can attach a table-completion hook; null disables it. The finisher has joined every payload before invoking it.
Tprogress and Sd can reach their accounting targets before the hook returns;
the finisher retains its view pin throughout the hook. Thus neither shared
accounting nor a false `consumeNext()` result is an application-wide drain
signal. The caller must honor the hook/context lifetime contract.

`Sd` is zeroed by the producer when its reservation crosses into the segment for a new epoch, before the release-store of `Es` (2c). The finisher provably still holds a reference on the segment (it was unconfirmed until this moment), so its accounting can never race a producer's re-zeroing.

A held epoch `Ei` (physical `Ki`) is released from the old end of the range when:

- `Es[Ki] == Ei` — the slot still names this epoch,
- `Es[Ki+1] == Ei + 1` — the next segment is initialized, so no more tables can start in `Ki`,
- `Seqt[Ki] + Sd[Ki] >= Seqt[Ki+1]` — the span is fully accounted.

Only `oldestEi` advances; a confirmed interior stays pinned until it is oldest. Confirmation requires `Es[Ki] == Ei` as well as the next-segment test: a wrap that overwrote the slot is not "confirmed."

The target is exact: tables are contiguous, so the sizes of all tables starting in `Ki` sum to precisely `Seqt[Ki+1] - Seqt[Ki]`, and `Sd` accrues each table's size exactly once, only on completion. A large table completing ahead of a small one leaves the small one's size missing and cannot force the crossing. A segment completely spanned by one table gets a `Seqt` at or past `Seqt[Ki+1]` and confirms trivially. While any reference on `Ki` is held, no producer can lap past it, so the next segment's stats remain at epoch `Ei+1` once initialized. (The rejected signed-balance `Sbal` design that this replaces lives in 11.)

**Retained-reference invariant.** Every consumer holds a reference on every segment it has entered but not yet confirmed complete. An incomplete segment is therefore protected by every passerby still subscribed. Protection does not end when those passerby unsubscribe: an unconfirmed segment keeps the `Live` it has had since initialization, whoever released it last and whether or not that releaser was the Farm's last consumer (6c, 6e). New subscribers attach at or before the earliest `Live` epoch their scan can see (6b) and drain the backlog exactly once.

**Position and frontier.** The position (`newestEi`) participates in the same confirmation story. After a spanning table, or on the idle path, a confirmed position is advanced onto the initialized segment of `nextSeq` (6d) and the old pin remains in the range until the confirmed prefix reaches it. An idle subscribed consumer is therefore always pinned on the frontier, never on a completed interior segment as its `newestEi`.

Consumers consult `Sd` only on position changes and on idleness, never per payload: after each table advance, at unsubscribe, and on the idle path below.

**Deadlock freedom and the idle re-walk.** If producers stall on a held trailing reference, consumers catch up to `Wt` and their next-table sentinels stop validating. On that idle path a consumer first advances a confirmed position onto `nextSeq`'s segment (6d), then re-walks its oldest unconfirmed epoch if `Es` still names that epoch, claiming any leftover runs in every shard (the data is guaranteed present and intact while `Es` matches: the consumer's own held reference forbids overwrite), then releases the confirmed prefix. It also re-walks its current position segment. Each re-walk resumes from a per-consumer cursor instead of rescanning the whole segment. The position cursor (`sweepSeq`) remembers the sequence up to which the position segment has been drained, so a park costs O(new tables) rather than O(tables in the segment); it resets whenever the position moves to a new segment (`moveRef`). The oldest-epoch cursor (`oldSweepSeq`) starts at `Seqt` and advances only over the leading run of tables this consumer observed with `Tprogress == Tlen`; the first incomplete table stops cursor advancement for that pass, while the walk continues through later tables to claim their leftovers. A stalled oldest epoch therefore costs each park O(tables from the first incomplete one). It resets whenever the oldest held epoch advances. Neither cursor affects confirmation or which pins are held. Blocking creates the idleness that resolves it. If refresh still fails, a producer may itself subscribe and drain (5a).

**Reuse condition.** Confirmation permits pins to be released and `Live` to be discharged (6c); producer reuse occurs only after the contiguous acquire sweep observes zero roots (I5). `Sd` is the accounting gate; `Rt` is the memory-protection gate.

---

## 9. Memory-Order Contract

Atomic memory orders, in one place:

| Object/transition | Publishing operation | Observing operation | Ordered data |
| --- | --- | --- | --- |
| root-tally RMWs | release `fetch_add`/`fetch_sub` on `Rt` | acquire loads of `Rt` (producers, subscribers, discharge CAS) | prior ring reads by the counted consumer join the tally's release sequence |
| `Live` raise | release `fetch_add(Live)` on `Rt`, after the producer's `Wt` reservation and before its `Es` release-store | acquire `Es` (confirmers, subscriber reads); acquire `Rt` (sweeps, subscriber reads) | the epoch's protection (I3); a subscriber that acquires a new lap's `Live` beside an old `Es` imports that lap's reservation, so its `Wt` re-load rejects the old epoch (6b-c) |
| `Live` discharge | release CAS clearing `Live`, after confirmation's acquire of `Es[e+1]`, while the clearer holds its reference | acquire `Rt` by sweeps and subscriber reads, directly or through the release sequence of later `Rt` RMWs | the successor's `Live` raise and `Es` before any read that passes the discharged slot (I3, 6b discovery) |
| write-tail reservation | acq_rel RMW on `Wt` | acquire probe used by quota logic and subscription; the acquire half of every later reservation | published tail position and bounded sweep anchor; each reserver's acquired reclamation history, handed to producers that later spend retained quota without sweeping |
| leaf-tally RMWs | acq_rel on `Lt` | acq_rel edge decrement and release root update | a non-edge consumer's ring reads join the leaf release sequence carried by the last-on-leaf's `Rt` release |
| table publication | release-store `Tsent` last | acquire-load and validate `Tsent` | raw-atomic Thead remainder, indexes, counters, payload metadata/body, compact `[Call][Pbody]` records (2f) or uniform `Tcall`/`Tblen` and `[Pbody]` records (2g) |
| segment metadata publication | release-store `Es` last | acquire-load and validate expected `Es` | raw-atomic `Seqt`, `SqCs`, initial `Sd` |
| table accounting join | acq_rel completion RMWs on `Shc`, then acq_rel RMW on `Tprogress` | the finisher's acq_rel `Tprogress` RMW lands on `Tlen` (raw skip checks order nothing) | run callback writes join within each shard, then across shards, before optional table-completion notification; every payload callback contributes |
| producer reuses a segment | release root transition toward zero | acquire-load `Rt == 0` in quota sweep; then, for any other producer reusing the swept space, the acq_rel `Wt` reservation chain | prior consumers' ring accesses happen before reuse, including reuse by a producer spending quota granted before those accesses |
| temporary subscriber pin | release add/sub of `Sub` plus acquire metadata/root validation | producer acquire of nonzero `Rt` | a sweep that reads the slot after the `Sub` cannot admit it while the subscriber attaches; quota granted earlier is not revoked (5a) |
| subscribe hint reads | none (no RMW) | acquire `Es`, then acquire `Rt`, per window slot | candidate choice only; attachment rests on the re-read under `Sub` and on validation (6b) |

The operational sections cite these edges by name rather than restating the orders.

**Why the reservation chain suffices.** Every `Wt` change is an RMW, so a producer P's acq_rel reservation acquires every earlier reservation and, through each one, the sweep that preceded it in its thread. Let `A*` be the largest anchor of a grant P has acquired this way, including its own. P's span ends at most `Exmax` past `A*` (I1). One producer holds at most one unspent grant at a time, and `refreshQuota` replaces the balance. The grant at `A*` swept `Exmax` of free space after `A*`'s segment. That segment itself was certified by an earlier sweep, whose crossing reservation the `A*` probe acquired. Induction gives consumer accesses → leaf/`Rt` release → zero-`Rt` sweep acquire → reservation chain → P's ring and `stats` stores. P's per-lap `stats` stores come after its reservation, so this chain also orders a previous epoch's `Sd` add (made while the finisher still holds its pin) before P zeroes `Sd`. A separate acquire load of `Wt` before the RMW would not suffice: the tail can move between that load and the RMW that decides the write address.

The runtime accesses ring words atomically even with raw order, and the
release sentinel publishes their initialized values. Callbacks may read
payload bodies plainly, so atomic stores alone do not make wrap reuse safe:
the leaf/root/sweep/reservation happens-before chain must order every such
read before the next producer's stores. Raw runtime loads also remain
subject to publication and lifetime validation.

The raw ring-read rule: acquire-validate `Es` or `Tsent` first, then raw-load the published sibling fields, and only while holding the tally that protects that segment.

**`Live` ownership.** Only the producer that reserved an epoch raises its
`Live`, and only a holder that observed the epoch confirmed clears it. The
raiser's valid reservation and the clearer's held reference keep each
operation on the generation it was decided for. A raise by anyone else, or a
clear without a held reference, would break I3's induction; departures and
subscription attempts do neither. The ordering argument is not specific to
x86 TSO.

**Sanitizer evidence.** TSan builds use the same real atomic edges as ordinary
builds. `version(TSan)` selects runtime options, not synchronization hooks.
LLVM 22.1.8 carries non-edge leaf releases through the last-on-leaf acquire
and root release. The controlled `tests/farm/leaf_reuse.d` mutation
exposes a missing leaf acquire only when the old manual root annotations
are absent; see [the ordering audit](docs/design/farm-ordering.md).


---

## 10. API, Errors, and Implementation Profile

### Conformance and error table

| Situation | Result |
| --- | --- |
| producer tier has no registration slot | invalid Token; recoverable |
| consumer capacity exceeded | `subscribe()` returns `SUBSCRIBE_FULL` (-1); recoverable after another view unsubscribes |
| the write tail outruns a subscribe attempt's bounded rounds (6b) | `subscribe` unwinds its `Sub`s and `Cf` and retries immediately; `subscribeOnce` returns `SUBSCRIBE_RETRY` (-2) instead |
| subscribe with a null Farm or an already subscribed view | `SUBSCRIBE_INVALID` (-3); caller error |
| assigning over a still-valid Token | fatal |
| `write()` has no safe quota at present | returns 0; backpressure |
| only a prefix fits current quota | returns the prefix count; caller advances its own range |
| bad/stale/forged Token, or a Token presented to a Farm other than its issuer | fatal |
| null payload callback, out-of-range `avgCost`, or a non-null completion hook with a null callback | fatal |
| payload cannot fit a singleton table in the caller's tier | fatal |
| size arithmetic or packed counter wraps | fatal |
| tally underflow or invalid ownership transition, including a `Live` raise on a slot whose low half is not zero | fatal |
| destroy with live consumers or producer tickets | fatal |

A process-wide `setFatalHandler` observer (C: `af_set_fatal_handler`) may log
or dump before the default diagnostic and abort. Install or replace it only
while Farm users are quiescent. Its borrowed message is valid only during the
call; it must not throw, longjmp, or re-enter the Farm. Returning still aborts.
The core and optional C runtime distribution share this same fatal policy.

Operational sections still mention a fatal guard at the point where it matters to a proof; this table is the compact form of the policy.

### Current implementation profile

- `MAX_CONSUMERS_LIMIT = 128`, `MAX_LEAVES = 12`.
- `KMAX = 16`; `ringMiB` must be a power of two `>= 2` (so `Ln >= 2^18` ulongs) and `<= MAX_RING_MIB`; `segCap` floor 2048. These last two are characterized rather than tuned (11).
- `MAX_CHUNK = 128`; default `AvgCost = 3` (chunk 16); small-table threshold default 64, auto rule `clamp(SqCs * Chunk, 16, 256)`.
- Every payload executes once. `AvgCost` in `0 .. log2(MAX_CHUNK)` (`0 .. 7`).
- Bulk quota auto-defaults to `segCap`; small quota fatals on 0.
- Producer ticket slots are 64-byte strided.
- The Farm must be allocated into 64-byte-aligned memory with manual lifetime. At the time of writing, D's garbage collector only guarantees 16-byte alignment, so the allocator is explicit.
- Just about every interface is `@nogc nothrow @system`.
- The implementation spells the atomic helpers `atomicFetchAdd`/`atomicFetchSub`; this spec uses conceptual `fetch_add`/`fetch_sub`.

---

## 11. Rationale and History Appendix

### MT payloads removed (1.9)

The removal and its breaking changes are recorded in
[docs/performance/archive-2026-10-20.md](docs/performance/archive-2026-10-20.md#mt-payloads-removed-19);
every payload runs once and every table finisher joins all callbacks.

These details are worth preserving, but they are history, alternatives, or tuning observations rather than normative rules.

**Lineage of the tallies.** The leaf/root tally is a refinement of indirect reference counting (Piquer, "Indirect reference counting: a distributed garbage collection algorithm," PARLE 1991, LNCS 505). In Piquer's scheme a reference holder that hands out copies counts them locally and becomes their parent; a holder sends a single decrement to its parent only when its own count reaches zero; and the owner's count reaches zero only when the whole tree has collapsed. The parent's count includes a child before the child exists, so no decrement can ever precede the increment it cancels. The Farm keeps that invariant and that upward-on-zero-only rule. `Lt` are the intermediate counters, `Rt` is the owner, and a consumer's root add is ordered before any matching root subtract by the acq_rel release sequence through its leaf: a leaf returns to zero only after every consumer that incremented it has decremented it, including the one whose 0→1 took the root count, and its root add precedes its own leaf decrement in program order.

Four things are specialized or added. The tree is a fixed two-level fan-out per segment, `SqCs` leaves chosen by `IDc % SqCs` and cached in `ltiRing`, in place of Piquer's unbounded copy-history tree; the fan-out is sized from the live consumer count so contention per counter and counters per segment are both about `√Cs`. Shared-memory RMWs replace messages. There is a concurrent reader: producers acquire-load `Rt` in a contiguous forward sweep (5a), where Piquer's owner only waits for zero. And membership is open. Piquer's zero is final because a reference can only be obtained from an existing holder; a `ConsumerView` subscribes from outside the tree. `Sub` (6b) makes the subscriber a provisional member of the root before it holds any leaf, and `Live` (6c) stands in for Piquer's owner: a per-epoch member that the reserving producer enrolls at initialization and that only a holder who confirmed the epoch retires. An abandoned incomplete segment therefore stays unreclaimable and discoverable without any departure having to create protection; open membership needs no counterpart to the closed-membership original's final zero beyond that.

SNZI (Ellen, Lev, Luchangco, Moir, PODC 2007) is the shared-memory member of this family and the Farm’s closest relative: a fixed tree of counters, arrivals and departures at any node, propagation only on zero/nonzero edges, and a concurrent Query. SNZI buys linearizability, a Query correct at every instant, with an intermediate ½ state, helping, and version numbers on CAS-updated nodes. The Farm gives that up. Its hot path is fetch-add only: the Farm's CAS sites are cold (section 1), and the discharge CAS runs about once per epoch, not once per operation (6c). It needs only the weaker guarantee that a sweep never certifies protected space. The increment-ahead rule (I2) and the subscriber’s Sub keep any leaf-1/root-0 window behind a pin that already stops the sweep. The subscribe scan tolerates a moving tail by validating under its `Sub` and retrying (6b-c). The release sequence rules out underflow. What the Farm adds lies above the counter: a per-epoch ring of indicators swept contiguously, fan-out resized each epoch, the `Sub` field that makes membership provisional, and the `Live` bit that keeps abandoned work protected and discoverable. Dijkstra and Scholten’s termination detection (1980) is an early form of the upward-on-zero tree.

**Sub0 replaced by `Live` (2.1.1 development).** Earlier versions kept a counted `Sub0` pulse in bits 31..16 of `Rt`. The last releaser of an incomplete epoch, a crossing producer's `plantIfUnprotected`, and the last departing consumer each deposited one, and an epoch's first reference removed it. One field carried protection of abandoned work, coverage of a producer's pre-plant window, discovery, and an empty-Farm anchor, and each depended on consumer positions or membership. Deterministic schedules reproduced the consequences: a non-last departure could strip the pin that covered a producer's pre-plant window and let another producer lap unpublished work; the backward subscribe walk could miss a consumer moving forward; a last consumer leaving during a walk could see the subscriber already counted and leave the ring unpinned, and counting the subscriber after its walk instead left a late last-departure pulse behind every member, stalling producers. `Live` is raised at initialization and cleared only by a confirming holder, so protection no longer depends on who is subscribed, every last-of-Farm decision disappears, and subscription only has to find the earliest `Live` epoch. The schedules, including the scan-order and stale-window cases found while designing 6b, are in `tests/farm/subscription_live.d`. The design record is the Fmembers/Xfer/Live proposal and its review (sections 13 and 14), which also explains why the Xfer handoff and handoff mutex it first proposed are unnecessary once `Live` exists.

**Open obligations.** These are recorded rather than proved:

- The discovery argument of 6b is exact for sequentially consistent interleavings. Its weak-memory form, the happens-before chain sketched there, has not been machine-checked under the D/C++ memory model, nor has the I3 induction of section 3.
- The subscribe round budget (`K + 2`) is not proven sufficient. `RETRY` stays recoverable; no watchdog may treat a run of retries as corruption.
- One attempt's `Sub`s accumulate across its rounds, up to `K`. Measurements of the common case should be taken with instrumentation outside the timed path.

**Provenance.** The tally design predates the lineage note. It was worked out from the LMAX Disruptor articles, a magic-buffer write-up, and C++ memory-order references, without knowledge of Piquer’s indirect reference counting or SNZI. Both were found afterward through a literature review, which prompted the comparison above. This is recorded as history, not as a claim of priority over the earlier work.

**Rejected `Sbal` design.** An earlier draft computed the confirmation boundary arithmetically and kept a signed outstanding balance `Sbal`, incremented by producers for tables starting in a segment and decremented by finishers. It had a producer-producer race — one producer initializing the segment could zero the balance after another had already incremented it for a table starting there — and a stale-balance deadlock mode. Comparing `Seqt[Ki] + Sd[Ki]` against `Seqt[Ki+1]` removes the balance entirely.

**Strong versus weak CAS.** The current implementation uses a strong CAS (D's `cas` does not fail spuriously). A weak CAS would only add retries to the discharge loop, which reloads and stops once `Live` reads clear, and to ticket claims; whether weak CAS would suffice is an open question, not a contract point.

**Ring geometry.** "Probably 4 or 8" segments is a characterization, not a requirement. The 2 MiB minimum (`Ln >= 2^18`) and the 2048-word segment floor are documented for later investigation: smaller rings were never studied, and the floor keeps per-table header/pad space from dominating a segment's capacity.

**Batching and contention.** The single write tail is a linear contention point; testing has revealed batch sizes reasonably above 1 to be the most crucial aspect, more than the exact segment count.

**Cache-line placement.** Producer ticket slots are 64-byte strided because tickets are shared mutable state under the registration CAS and the buffer cannot align them beyond 8 bytes; the stride, not the natural ulong size, carries the isolation. Payload metadata is consumer-local; ring records contain no mutable payload counter.

**Skip lists considered.** I have considered building skip lists or other structures dynamically updated by consumers for orientation, and am sticking with what's essentially a linked list of one table pointing to the next; large table publication amortizes the seek.

**D GC alignment.** At the time of writing, D's garbage collector only guarantees alignment to 16 bytes, which is why the Farm's 64-byte-aligned allocation is manual.
