# Ant Farm: a fixed-memory concurrent job distributor

## 1. The picture

Ant Farm is best understood as a **fixed-size, M:N job-distribution ring**, not a queue. The name comes from the spec's metaphor: a farmer (producer) walks a circular path placing work; ants (consumers) pick it up and haul it away. The farmer cannot see the ants and does not know how many there are, so the ants maintain signs at regular intervals (segment reference tallies). A nonzero sign means "ants are still working here, do not walk over this ground." The producer only has to watch one signal per segment; it never coordinates with any individual consumer.

Concretely:

- **Fixed memory footprint.** The farm is a power-of-two-length ring of `ulong`s using a "magic buffer" double-mapping, preallocated at creation. Segment metadata, leaf tallies, and producer-ticket arrays are all preallocated. `write()` and `consumeNext()` are `@nogc nothrow`; no per-job allocation exists.
- **Wait-free-style publishing.** The hot paths are deliberately CAS-free. A producer reserves space with a single `fetch_add` on the write tail, writes a table, and releases a sentinel. A consumer claims work with `fetch_add` on a sharded counter — the common case is **one claim per 16 `Call`s** (`MAX_CHUNK` is 32, default chunk 16).
- **Variable payloads.** Each payload is a 128-byte `PayloadHeader` plus a type-erased `const(ulong)[]` body. Payloads may be single-threaded (`MaxCs = 1`) or multithreaded (`Done`/`MaxCs` up to 512), so a payload can describe one job or a mini-parallel task.
- **Deliberately relaxed ordering.** There is no FIFO guarantee. Within a published table, consumers shard the index by `(IDc + Tseq) % SqCs` and claim chunks of the shard. Work is executed in claim order *within* a shard, not globally. A mid-tick one-payload write can overtake an in-flight dump table.
- **Failure is fatal.** Invariant violations (counter wraps, bad tokens, unsizable payloads) abort the process; this is the spec's corruption tripwire, and the perf work found the checks cost ~0%.

So the right mental model is: **producers publish tables of mixed-size jobs into a bounded ring; consumers independently claim runs of jobs, sharded to avoid contention; the only producer↔consumer coupling is per-segment reference tallies.**

## 2. Where it is strong

### 2.1 Throughput (16 MiB farm, Ryzen 5 5500, `ldc2 -O2 -release`)

The production-shaped topology is **`nc=8`, `nb=2`, `K=8`, 16 MiB ring, batch ≥ 128**:

| Shape | Result |
|---|---:|
| `body=16` (128 B payload), `batch=80` | **59.2 M payloads/s** (~7.1 GiB/s of body) |
| `body=2`, `batch=256` | **61.5 M payloads/s** |
| `body=16`, `batch=512` | ~61.2–61.6 M payloads/s (K=8) |
| `body=1024` (8 KiB payload), `batch=80` | ~4.3 M payloads/s, **~32.8 GiB/s of body** |

Important nuances from `construction/perftest`:

- **Batching matters more than anything.** `batch=1` is 4–5× slower. The batch curve keeps climbing to ~256–512.
- **One consumer is the weakest count** now that the `SqCs=1` floor stops at `Cs <= 2`: two consumers still share one shard but add a worker, and beat one consumer (~44 M vs ~38–41 M payloads/s at `nb=2`). Eight consumers with two bulk producers remains the robust winner.
- **`nc=8 nb=2` beats `nb=1` and `ns=4`.** A single bulk producer is a publish-side serialization tax that grows with ring size (24.8 Mpps at 128 MiB vs 40.2 for two bulk producers).
- **8–16 MiB is the sweet spot.** It matches the 16 MiB L3. A 32 MiB ring drops ~25% throughput; 256 MiB drops ~38%. The one operational win of a big ring is that `write()==0` disappears (producers are never full).
- **Execute-side cost is ~31.2 ns/job** in the digest bench (linear claim 16, body-touching `Call`). Claim amortization is real: claim-1 costs ~46.7 ns/job, a **~1.5× claim dividend**. Shuffled vs linear layout is ~free (30.3 vs 31.2 ns/job).

### 2.2 Tail latency — the real product

From `construction/perftest/last_tail.txt` (pinned `nc=6`, publish → first `Call`, 1 µs simulated Call spin):

| Scene | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| idle | 380 ns | 3.2 µs | 7.7 µs |
| mid-drain, dump 256 | 5.2 µs | 22.5 µs | 25.3 µs |
| mid-drain, dump 2048 | 12.0 µs | 29.9 µs | 273 µs |
| mid-drain, dump 8192 | **14.3 µs** | **30.2 µs** | 186 µs |
| small dump (32) | 311 ns | 2.7 µs | 5.0 µs |

The headline: **p99 no longer grows with dump size.** A mid-tick write arriving while an 8192-job table is being drained reaches its first worker in ~14 µs p50 / ~30 µs p99, versus ~1.8 ms in earlier revisions. The mechanisms responsible:

- **First-claimant mid-tick yield (5e-m):** the first worker to claim a shard may, if it sees the shard is shared and the next table's sentinel is already live, finish its current run and return immediately — so `consumeNext` can advance to the newly published mid-tick job instead of grinding through the rest of the shard.
- **Idle re-walk cursor (5j):** parked consumers no longer rescan the whole current segment; the idle floor stayed at ~371 ns p50.
- **`avgCost` chunking:** declaring expensive `Call`s shrinks the chunk (e.g. chunk 8), trimming the extreme tail (8192/1 µs p99.9: 246 µs at chunk 16 → 130 µs at chunk 8) at no throughput cost.

`write()==0` is rare while anyone is draining; when consumers are parked, the ring fills to a hard wall (~31.7k jobs at that topology) and stops — it refuses to pretend to be unbounded.

### 2.3 Scaling expectations

The current data tops out at `nc=8` on a 6c/12t host. This revision lowered the `SqCs=1` floor from `Cs <= 4` to `Cs <= 2`, so 3–4 consumers now shard across 2 leaf tallies and 2 claim counters; paired A/B on this host shows that lifts `nc=3`/`nc=4` slightly and leaves `nc=8` unchanged. The design's scaling bets are visible: per-consumer shard counters, leaf tallies distributed by `sqrt(Cs)`, sweeper-based load balancing, and claim granularity of 16 mean **more consumers should scale better than Disruptor WorkerPool does**. That remains to be proven on more cores, and NUMA is explicitly uninvestigated.

## 3. How it compares

### 3.1 Disruptor WorkerPool — clobbered where it should compete

From `disruptortest/BENCHMARK_SUMMARY.md`, same host, C++/GCC `-O2`:

| Config | Disruptor WorkerPool | Ant Farm |
|---|---:|---:|
| 8 workers, 2 producers, 16 MiB ring | 9.7–10.3 M events/s | **56.0–57.1 M payloads/s** (~5.5–5.8×) |
| 1 worker, 1 producer, ring 8192 | **126–145 M events/s** | 40.6–42.8 M payloads/s |
| tail, 8W, dump 8192, empty handler | ~61 µs p50 / ~190 µs p99 | ~14 µs p50 / ~30 µs p99 (nc=6) |

The structural reason: Disruptor's `WorkProcessor` makes **one CAS per event** on a single shared `m_workSequence`; Ant Farm amortizes **one `fetch_add` per chunk of 16 calls** across per-shard counters. Disruptor's batched path is fast (115–786 M ops/s) but it is a broadcast `BatchEventProcessor`, not a work-distributing pool, so it is not the right comparison.

### 3.2 moodycamel::ConcurrentQueue — more raw transport, worse distributor tail

From `moodytest/SUMMARY.md`:

- **Raw item throughput favors moodycamel.** Native bounded 16 MiB with `try_enqueue_bulk(32)`: ~120–743 M items/s depending on tokens and topology; 1p/1c no-token ~212 M items/s. At 8 KiB payloads it reaches ~45.8 GiB/s (tokens), well above Ant Farm's ~26 GiB/s body at 8 KiB. Part of this is the comparison itself: moodycamel's 16 B item is just the item, while an Ant Farm payload carries a 128-byte header plus table index/padding overhead.
- **Occupied tail favors Ant Farm.** Moodycamel no-token mid-drain p50 is FIFO-drain-shaped: 46 µs at dump 256, 370 µs at 2048, **1485 µs at 8192** (1 µs spin, 6 consumers). Tokens flatten the large-dump tail to ~143–287 µs. Ant Farm is ~5.3/12/14 µs p50 and ~30 µs p99 across the same dump sizes.
- **Idle latency is close.** Moodycamel idle p50 is ~0.23–0.26 µs; Ant Farm is ~0.37 µs. Moodycamel wins the empty ring by a small margin.

The summary line: moodycamel is a **strong bounded-ring transport**; Ant Farm is a **work distributor**. Ant Farm's relaxed ordering and first-claimant yield are exactly what keep an urgent mid-tick job from waiting behind the FIFO drain; moodycamel's FIFO order is exactly what makes its occupied tail grow with the backlog.

## 4. Where one might deploy it

Ant Farm fits systems that look like a **game frame tick**:

1. **Main thread / job system** dumps a large table of per-frame jobs each tick.
2. **A pool of spinning workers** (typically ≥ 8 on bigger cores) claims chunks and executes them.
3. **Mid-tick, another thread must publish an urgent job** with wait-free-style admission and have it reach a worker in tens of microseconds, even while the frame dump is still draining.
4. **The working set is bounded** and the ring should sit in L3 (8–16 MiB on the tested host).
5. **Ordering is either commutative or explicitly managed** — completion order is not FIFO, and that is usually fine for accumulating work (transforms, physics islands, particle updates, asset callbacks) but wrong for strict pipelines or message ordering.

Concrete examples:

- A game engine's per-tick job system: `write()` for the frame dump in bulk, small-tier 1-payload writes for mid-tick "please do this now" tasks.
- A task distributor for embarrassingly parallel or accumulating work over fixed-memory payload buffers (e.g., batch processing of blocks, AO/lighting tiles, animation evaluation).
- Any environment where the alternatives are a Disruptor WorkerPool (single CAS per event) or a concurrent queue with FIFO-occupied latency.

Where **not** to deploy it:

- **Strict FIFO / order-sensitive streams.** Ant Farm disclaims FIFO by design.
- **Single-consumer throughput.** Disruptor's 1P→1W path (and simple SPSC rings) beat it.
- **Blocking/sleeping workers.** The current story is spinning consumers; futex wake is unmeasured.
- **Unbounded or elastic backlog.** The ring is fixed; `write()==0` is the backpressure and a hard fill wall is the design.
- **Very large never-full rings.** You can buy zero stalls at ≥ 32 MiB, but it costs 25–38% throughput on this host unless huge pages/NUMA are addressed.

## 5. Recommended configuration (from the perf work)

- **`K = 4` or `8`** — tied within noise.
- **`nc = 8`, `nb = 2`** (or `ns = 4` small producers) — the robust winner.
- **`batch ≥ 128`, preferably 256–512**; let the producer's quota bound the table size.
- **Ring `Ln` = 8–16 MiB** on a 16 MiB L3 part; larger only if a never-full guarantee is worth the throughput cliff.
- **Declare `avgCost` per Call family**: cheap calls → `0` (chunk 32, max amortization); expensive calls → `2–3` (chunk 8–4) to trim tail; avoid `avgCost=5` (chunk 1, −10–12% throughput).
- **Pin consumers** to physical cores; keep `fatal()` in release — the wrap checks are ~free.

## 6. What is still open

The repo's own "not measured" list is the honest caveat set: makespan of a full tick, sleeping workers/futex wake, mid-tick writes that span a segment boundary, huge pages for the magic-buffer mapping, real game `Call` bodies, and — most relevant to the scaling claim — **tests beyond 8 consumers and NUMA behavior**. The architecture gives good reasons to expect Ant Farm to keep scaling as a task distributor, but that is still a hypothesis pending hardware.

---

**Artifacts:** `construction/spec2` (living spec), `construction/perftest/{README,POSTMORTEM,SPECULATIVE_OPT_2026-08-16_1,last_sweep,last_digest,last_tail}.txt`, `moodytest/SUMMARY.md`, and `disruptortest/BENCHMARK_SUMMARY.md`.
