# Ant Farm: a fixed-memory concurrent job distributor

## 1. The picture

Ant Farm is best understood as a **fixed-size, M:N job-distribution ring**, not a queue. The name comes from the spec's metaphor: a farmer (producer) walks a circular path placing work; ants (consumers) pick it up and haul it away. The farmer cannot see the ants and does not know how many there are, so the ants maintain signs at regular intervals (segment reference tallies). A nonzero sign means "ants are still working here, do not walk over this ground." The producer only has to watch one signal per segment; it never coordinates with any individual consumer.

Concretely:

- **Fixed memory footprint.** The farm is a power-of-two-length ring of `ulong`s using a "magic buffer" double-mapping, preallocated at creation. Segment metadata, leaf tallies, and producer-ticket arrays are all preallocated. `write()` and `consumeNext()` are `@nogc nothrow`; no per-job allocation exists.
- **Wait-free-style publishing.** The hot paths are deliberately CAS-free. A producer reserves space with a single `fetch_add` on the write tail, writes a table, and releases a sentinel. A consumer claims work with `fetch_add` on a sharded counter — the common case is **one claim per 16 `Call`s** (`MAX_CHUNK` is 32, default chunk 16).
- **Variable payloads.** Each payload is a 128-byte `PayloadHeader` plus a type-erased `const(ulong)[]` body. Payloads may be single-threaded (`MaxCs = 1`) or multithreaded (`Done`/`MaxCs` up to 512), so a payload can describe one job or a mini-parallel task.
- **Deliberately relaxed ordering.** There is no FIFO guarantee. Within a published table, consumers shard the index by `(IDc + Tseq) % SqCs` and claim chunks of the shard. Work is executed in claim order *within* a shard, not globally. A mid-tick one-payload write can overtake an in-flight dump table.
- **Failure is fatal.** Invariant violations (counter wraps, bad tokens, unsizable payloads) abort the process; this is the spec's corruption tripwire, and the perf work found the checks cost ~0%.
- **Producer quotas live in the ticket.** Each producer registers for a tier and receives a single-owner `Token` carrying a private quota mirror, validated against a farm-side per-slot ledger on every `write()`. Tokens transfer (copy consumes the source); a forged or copied ticket fatals on the ledger mismatch, so a registered caller cannot mint blind quota past Exmax. Callers pass the same `Token` by reference for the producer's lifetime.

### 1.1 Build variants

- **`ZERO_ST_RMW`** (`-d-version=ZERO_ST_RMW`): the single-threaded single-shot (ST) fast path in `enterPayload` drops the Pcount claims RMW entirely and uses a plain increment, relying solely on the shard Tcount chunk claim for exclusivity. Measured ~4% faster with the legacy global-count callback, neutral with the default per-worker-batched callback. It makes duplicate ST entry a real risk if a per-element search path is ever added outside the chunk digest; keep the default build for production.
- **`noverify`** (`-d-version=noverify`): disables the packed-counter wrap tripwires (`VERIFY_WRAPS`). They are provably unreachable under the 512 caps and cost ~0%; keep them on in release.

So the right mental model is: **producers publish tables of mixed-size jobs into a bounded ring; consumers independently claim runs of jobs, sharded to avoid contention; the only producer↔consumer coupling is per-segment reference tallies.**

## 2. Where it is strong

### 2.1 Throughput (16 MiB farm, Ryzen 5 5500, `ldc2 -O2 -release`)

The sweep callback is now a **per-worker batched counter** by default
(`--global-count` restores the old one-global-atomic callback). The old
global-atomic point is included below for continuity; the batched numbers
are the queue-overhead view.

**Best batched-callback shape found by the latest sweep:** `K=4`, `nc=4`,
`nb=1`, `ns=4` — **168 M payloads/s** at `body=2`, `batch=256`, and
**120 M payloads/s** at `body=16`, `batch=80`; best body bytes/s is
**~47.2 GiB/s** at `body=1024`, `batch=80`.

These numbers move around with CPU frequency/boost; treat them as a fresh
run rather than a new plateau.

Important nuances from `construction/perftest`:

- **Batching matters more than anything.** `batch=1` is 4–5× slower. The batch curve keeps climbing to ~256–512.
- With the batched callback the producer mix ranking changes: **1 bulk + 4 small producers** beats 2 bulk producers at `nc=4` and `nc=8`; the old global-atomic callback masked producer-side work.
- **8–16 MiB is the sweet spot.** It matches the 16 MiB L3. A 32 MiB ring drops ~25% throughput; 256 MiB drops ~38%. The one operational win of a big ring is that `write()==0` disappears (producers are never full).
- **Huge pages are a real win at the L3-sized ring, and they make the sweet spot more peaked rather than flattening it.** `--huge` on the 16 MiB farm measured ~180–186→~243–246 Mpps (`body=2`), ~126–127→~164–165 Mpps (`body=16`), and ~6.4–7.1→~9.7 Mpps (`body=1024`); at 32 MiB and larger the effect is only ~0–5%.
- **Execute-side cost is ~30.9 ns/job** in the digest bench (linear claim 16, body-touching `Call`). Claim amortization is real: claim-1 costs ~49.2 ns/job, a **~1.6× claim dividend**. Shuffled vs linear layout is ~free (31.1 vs 30.9 ns/job).

### 2.2 Tail latency — the real product

From `construction/perftest/last_tail.txt` (pinned `nc=6`, publish → first `Call`, 1 µs simulated Call spin):

| Scene | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| idle | 370 ns | 3.3 µs | 8.4 µs |
| mid-drain, dump 256 | 5.3 µs | 22.5 µs | 26.3 µs |
| mid-drain, dump 2048 | 12.1 µs | 31.5 µs | 318.6 µs* |
| mid-drain, dump 8192 | **14.2 µs** | **32.2 µs** | 1.31 ms* |
| small dump (32) | 330 ns | 2.7 µs | 9.8 µs |

*p99.9 is noisy run-to-run; p50/p99 are the stable tail signal.

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
| 8 workers, 2 producers, 16 MiB ring | 9.7–10.3 M events/s | **~61 M payloads/s** global-atomic / ~77 M batched (~6× / ~7.6×) |
| 1 worker, 1 producer, ring 8192 | **126–145 M events/s** | 40.6–42.8 M payloads/s |
| tail, 8W, dump 8192, empty handler | ~61 µs p50 / ~190 µs p99 | ~14 µs p50 / ~30 µs p99 (nc=6) |

The structural reason: Disruptor's `WorkProcessor` makes **one CAS per event** on a single shared `m_workSequence`; Ant Farm amortizes **one `fetch_add` per chunk of 16 calls** across per-shard counters. Disruptor's batched path is fast (115–786 M ops/s) but it is a broadcast `BatchEventProcessor`, not a work-distributing pool, so it is not the right comparison.

### 3.2 moodycamel::ConcurrentQueue — more raw transport, worse distributor tail

From `moodytest/SUMMARY.md`:

- **Raw item throughput favors moodycamel.** Native bounded 16 MiB with `try_enqueue_bulk(32)`: ~120–743 M items/s depending on tokens and topology. A fresh local probe of no-consumer-tokens configurations (16-byte elements, 16 MiB subqueue cap) ran 120–190 M items/s. With consumers tokens this number can be several times higher, but this also creates pinned pipelines as opposed to the work-distributing Ant Farm model. At 8 KiB payloads it reaches ~45.8 GiB/s (tokens), about equal to Ant Farm's ~47GiB/s at 8 KiB. Part of this is the comparison itself: moodycamel's 16 B item is just the item, while an Ant Farm payload carries a 128-byte header plus table index/padding overhead.
- **Occupied tail favors Ant Farm.** Moodycamel no-token mid-drain p50 is FIFO-drain-shaped: 46 µs at dump 256, 370 µs at 2048, **1485 µs at 8192** (1 µs spin simulating per-item work, 6 consumers). Tokens flatten the large-dump tail to ~143–287 µs. Ant Farm is ~5.3/12/14 µs p50 and ~30 µs p99 across the same dump sizes.
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
- **Very large never-full rings.** You can buy zero stalls at ≥ 32 MiB, but it costs 25–38% throughput on this host unless NUMA is addressed (huge pages alone do not close the gap beyond L3).

## 5. Recommended configuration (from the perf work)

- **`K = 4` or `8`** — tied within noise.
- **`nc = 8`, `nb = 2`** (or `ns = 4` small producers) — the robust production shape. The latest batched-callback sweep favors **`nc=4`, `nb=1`, `ns=4`** for raw payloads/s.
- **`batch ≥ 128`, preferably 256–512**; let the producer's quota bound the table size.
- **Ring `Ln` = 8–16 MiB** on a 16 MiB L3 part; larger only if a never-full guarantee is worth the throughput cliff.
- **Declare `avgCost` per Call family**: cheap calls → `0` (chunk 32, max amortization); expensive calls → `2–3` (chunk 8–4) to trim tail; avoid `avgCost=5` (chunk 1, −10–12% throughput).
- **Enable huge pages for the magic buffer on Linux** (`AntFarm.create(..., true)`, perftest `--huge`, or `ANTFARM_HUGE_PAGES=1`): measured +14–50% on the 16 MiB ring, negligible beyond L3.
- **Pin consumers** to physical cores; keep `fatal()` in release — the wrap checks are ~free.

### 5.1 Huge pages: which Ant Farm configurations likely benefit

From the current measurements, the huge-page win is concentrated in:

- **8–16 MiB rings on hosts with a similar L3 size.** This is the main
  sweet spot; 32 MiB and larger rings showed only ~0–5% movement.
- **Multi-producer / multi-consumer topologies that are already fast.**
  Huge pages amplify the best shapes (`K=4` or `8`, `nc=2–8`, with a
  small-producer mix) more than they rescue slow ones.
- **Both small and large payload bodies at the L3-sized ring.** `body=2`,
  `body=16`, and `body=1024` all improved; the benefit is not body-size
  specific.
- **Hosts that support THP/hugetlb and have memory to spare.** The current
  implementation uses `madvise(MADV_HUGEPAGE)` on the Linux magic-buffer
  mapping, so `THP=never` or a missing hugetlb reservation means no win.

Configurations less likely to benefit:

- **Rings >32 MiB** — outside L3, DRAM bandwidth dominates and the huge-page
  gain is small or within noise.
- **Rings below 2 MiB** — too small to map a 2 MiB page usefully.
- **Memory-tight deployments** — 2 MiB pages add allocation/fragmentation
  pressure without enough TLB win.

## 6. What is still open

The repo's own "not measured" list is the honest caveat set: makespan of a full tick, sleeping workers/futex wake, real game `Call` bodies, and — most relevant to the scaling claim — **tests beyond 8 consumers and NUMA behavior**. Huge pages for the magic-buffer mapping are now covered in `perftest/README.md` (strong at the L3-sized ring, small beyond it). The architecture gives good reasons to expect Ant Farm to keep scaling as a task distributor, but that is still a hypothesis pending hardware.

---

**Artifacts:** `construction/spec2` (living spec), `construction/perftest/README.md`, `construction/perftest/HUGE_PAGES.md`, `construction/perftest/{POSTMORTEM,SPECULATIVE_OPT_2026-08-16_1,last_sweep,last_digest,last_tail}.txt`, `moodytest/SUMMARY.md`, and `disruptortest/BENCHMARK_SUMMARY.md`.
