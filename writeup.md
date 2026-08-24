# Ant Farm

Ant Farm is a concurrent M:N mixed-type jobs distributor on a fixed-length ring buffer. The ring is divided into discrete segments. Subscribed consumers gain references as they move across it and release them as work is completed; segments with zero references can be reclaimed. Producers hold a size quota in 64-bit ulongs and publish sets of jobs in tables — one job or arbitrarily many, within quota. The sum of all quotas is the maximum simultaneous excursion *Exmax*. When a producer needs to refresh, it checks the current and following segments for at least Exmax of free space.

Jobs dispatch to exactly one consumer (serial) or to many (parallel). A simple iteration counter gives a parallel job its fraction of granular completion. The tables carry cache-padded shard counters so a group of about √consumers claim jobs in discrete chunks; chunk size is the producer’s declared average cost of a Call. Some consumers ensure a table finishes, some look ahead, some rebalance shards, some pile onto parallel items. Hot-path work has no contended compare-and-swap: producers and consumers `fetch_add`. The CAS sites are cold — producer-ticket claim, last-releaser pulse, plant and retract of an incomplete-segment mark — and they are not open retry loops.

The end result is high throughput with synthetic figures comfortably between moodycamel’s no-token and token-pinned numbers, tail latency that generally crushes FIFO designs, and a concurrency shape that wants more workers, not fewer.

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. The last ant to leave a *confirmed-complete* area changes the tally (a reference counter) to zero. The last ant to leave an *incomplete* area instead leaves a pulse mark, so the farmer still sees a nonzero tally and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

A job is a callback and a set of read-only parameters. The callback decodes the payload body, takes a pointer to the payload header, and an iteration index for parallel work. Templates generate the callbacks and payload descriptions. The implementation is D: `@nogc nothrow @system`, a magic-buffer dual mapping of one physical ring, preallocated metadata, fatal on invariant violation. There is no per-job allocation. There is no FIFO.

Figures below are a 16 MiB farm on an Intel 12700H (6P+8E, 20 threads) unless noted. They move with boost; treat them as a recent run, not a plateau.

---

## What you actually publish

Producers register for a *bulk* or *small* tier and receive a single-owner ticket. A `write()` publishes a table: header, index, sharded claim counters, then the payloads laid out in the ring. A payload is a 128-byte header plus a type-erased body. `MaxCs = 1` is a single-threaded job; `MaxCs`/`Done` up to 512 is a mini-parallel task that several consumers may enter.

Consumers subscribe, pin a contiguous range of epochs, and `consumeNext()` through live tables. They claim a run of a shard with one `fetch_add` — commonly one claim per 16 Calls — execute, and move on. The first claimant of a shared shard may yield when the next table is already published, so a mid-tick one-job write does not wait behind an 8192-job dump. Completers sweep foreign shards and leftover parallel work. Idle consumers re-walk only the new tables, not the whole segment.

The only producer–consumer coupling is the per-segment tally. Producers never name a consumer. Consumers never wait on a producer beyond the table sentinel.

---

## Throughput, between two moodys

Small-producer grid, `K=8`, `body=1`, `batch=256`, every `ns`/`nc` pair from 1 through 14 (196 topologies):

| Shape | Result |
| --- | --- |
| Best | **397 M payloads/s** (`nc=13`, `ns=9`) |
| Grid median / mean | **282 / 255 M payloads/s** |
| Matched 4×4 / 6×6 / 8×8 | 289 / 316 / 341 M payloads/s |
| One producer, any `nc` | ~70–100 M payloads/s |
| Same grid, `batch=1` | ~10–15 M payloads/s |

Batching is still the first-order effect. One producer is a cap no matter how many consumers you add. Adding producers *and* consumers together keeps paying well past the six performance cores.

moodycamel::ConcurrentQueue on the same host (bounded-style matrix, `uint64_t` items):

| Mode | Typical / peak |
| --- | --- |
| No tokens, bulk | ~110–180 M/s in the middle of the matrix; **187 M/s** peak (1P→1C) |
| No tokens, 6×6 | **115 M/s** |
| With tokens, bulk | hundreds of M/s; **~1.07 billion/s** at 8–10 paired threads |
| With tokens, 6×6 | **807 M/s** |

Tokens pin pipelines: a consumer tends to drain the producer it is paired with. Ant Farm is a distributor, not a set of SPSC pipes, and each payload still carries a 128-byte header. Even so, **397 M (peak) and ~316 M at 6×6 sit comfortably between no-token bulk and token-pinned bulk** — above the unpinned transport, below the paired-token peak — while doing the other job (claim, shard, mix serial and parallel, let anyone publish).

A 6-core Ryzen 5 5500 showed the same shape at a lower ceiling (best mixed ~168 M, huge pages ~243–246 M at `body=2`).

---

## Tail, which is the product

Publish-to-first-Call on six pinned consumers, 1 µs of simulated work per Call. Ant Farm numbers are `tail`; the others are `queuebenches` on the same machine, 1 producer / 6 consumers, same 1 µs spin, sentinel round-trip after a pre-placed chunk.

| Scene (dump size) | Ant Farm p50 / p99 | moodycamel p50 / p99 | TBB p50 / p99 |
| --- | ---: | ---: | ---: |
| Idle | **500 ns / 3.3 µs** | 0.70 µs / 1.10 µs | 0.70 µs / 1.10 µs |
| Mid-drain 256 | **5.0 µs / 21.4 µs** | 48 µs / 56 µs | 47 µs / 61 µs |
| Mid-drain 2048 | **25.2 µs / 27.9 µs** | 378 µs / 419 µs | 379 µs / 416 µs |
| Mid-drain 8192 | **13.2 µs / 27.6 µs** | **1.52 ms / 1.57 ms** | 1.52 ms / 1.60 ms |

boost::lockfree is the same FIFO drain (1.61 ms p50 at 8192). **Ant Farm p99 does not grow with dump size.** A one-job write that arrives while an 8192-job table is draining still reaches a worker in tens of microseconds. The queues grow linearly with the backlog, as a FIFO must.

Idle, the queues are a little quicker. Occupied, Ant Farm is a different machine: it will overtake. One consumer is not that machine — `nc=1` mid-drain 8192 is ~6 ms p50, FIFO-shaped. Twelve consumers on this 6P+8E part keep p50 in the single-digit microseconds but p99 at 8192 stretches to ~300 µs; the product number is the six performance cores.

`write() == 0` is rare while anyone is draining. When consumers are parked the ring fills to a hard wall and stops. It will not pretend to be unbounded.

---

## A shape that wants more concurrency

The design bets are visible and they all point the same way.

Consumers are sharded by √Cs, not lined up on one sequence. Leaf tallies split the segment signal the same way. A claim is a chunk, not an item. Completers sweep holes left by a pre-empted or unsubscribed peer; oversaturated visitors nudge themselves into another bucket on the next table. Small tables, which would otherwise starve if nobody mapped to shard 0, carry a sweeper role forward. None of that is a single contended CAS.

On this host the small-producer grid keeps climbing as `ns` and `nc` rise together: ~186 M at 2×2, ~289 at 4×4, ~316 at 6×6, ~341 at 8×8, **397 M at 13 consumers / 9 producers**. No-token moodycamel bulk, over the same range, sits around 110 M and does not get a second wind. The intent is not “it is fast at nc=4.” The intent is that adding workers should keep paying, because the hot path never funnels through one word. NUMA is still unmeasured.

Work can originate from anywhere in the pool and dispatch at the same time.

Any registered producer may `write()` — the main thread dumping a frame, a worker that just discovered more work, a mid-tick “do this now” from some other subsystem. A stalled producer may itself subscribe, drain, and retry; that is the supported escape hatch. A table is a simultaneous dispatch: every live consumer may claim into it at once, serial jobs going to one claimant each, parallel jobs admitting up to `MaxCs`. The next table can be published before the current one has drained, and the first claimant will step over to it. There is no “the producer” and “the workers” as a pipeline. There is a ring, tickets, and a swarm.

That is the difference from a token-pinned concurrent queue (fast, but the work stays in lanes) and from a Disruptor worker pool (distributed, but one CAS per job on one sequence). Ant Farm is for the case where the next job may be born on any thread, must reach some thread quickly, and may be one Call or a thousand.

---

## Flexible on purpose

Construction picks a ring size, a segment count `K` (4 or 8 are tied), expected consumers, and two producer tiers with their quotas. An unused tier takes no part in Exmax. `avgCost` on each write sets chunk size (`MAX_CHUNK >> avgCost`): cheap Calls take chunk 32, expensive Calls take 8 or 4, and the tail of a large dump shrinks without a throughput tax. A farm-level small-table threshold decides when a table is claimed wholesale by shard 0. Huge pages are a switch. Consumers subscribe and unsubscribe. Tickets transfer; they do not copy.

What you put in a table is mixed by design: a bulk dump of single-threaded jobs, a handful of multithreaded ones, a later one-payload write from a worker. The same consume path covers all of it. You do not configure a separate “urgent queue.”

Recommended production-ish shape from the 12700H sweeps: `K = 8`, ring in L3, `batch` 256, several small producers with a matching consumer count, consumers pinned to the performance cores. Declare `avgCost` per Call family. Leave `fatal()` on — the wrap checks measured ~free. Do not run `batch=1` and expect the Mpps story.

---

## Where it belongs

It fits a **game frame tick**, or anything with the same silhouette:

1. Something dumps a large table of work.
2. A pool of spinning workers claims chunks and runs them.
3. Mid-tick, any thread may publish an urgent job and have it reach a worker in tens of microseconds while the dump is still draining.
4. The working set is bounded; the ring should sit in cache.
5. Completion order is not FIFO, and that is acceptable — accumulating work, tiles, islands, callbacks — not a strict pipeline.

It is the wrong tool for ordered streams, for 1P→1W peak transport, for sleeping workers (futex wake is unmeasured), and for an elastic unbounded backlog. You can buy a never-full ring at 32 MiB and up; you pay for it in throughput unless NUMA is addressed, and it has not been.

---

The living spec is `SPEC.md`. Tests are `antfarm_test.d` and `review_torture/`. Throughput and tail live under `perftest/`; raw grids in `throughput.txt` and `latency.txt`.
