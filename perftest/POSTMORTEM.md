# Ant Farm postmortem — first performance suite

Host: Ryzen 5 5500 (6c/12t), 16 MB L3. Farm default 16 MiB (`Ln = 2^21`
ulongs), `K = 4`, release `ldc2 -O2 -release`. Drivers in this directory;
raw tables in `last_sweep.txt`, `last_digest.txt`, `last_tail.txt`.
`fatal()` still prints and aborts under `-release`.

This note is the brief after three benches — publish throughput, consume-side
digest, mid-tick tail — against the product the farm was built for: a
top-level game job system that dumps work every tick and must also accept a
wait-free mid-tick `write()` on the same ring. No mutex, no pinned SPSC, no
out-of-band channel. Success is not Mpps. It is whether publish + drain can
be nearly transparent at frame scale, and whether the linear table chunk
pays for itself once `Call` actually runs.

Nothing here changes production defaults. `ConsumerView.benchMaxRuns` /
`benchChunk` are bench-only and zero on the shipped path.

---

## What was bet

1. **Publish + claim can be cheap enough to hide.** A tick dump of thousands
   of packed `Call`s, plus a 1-payload mid-tick `write()`, should not be a
   scheduling event.
2. **A claimed run of consecutive `Tindex` is cheaper to execute** than a
   pointer MPMC / steal deque / copy-out, because the payloads sit in
   address order in the magic buffer.
3. **Mid-tick dispatch on the same ring stays a rounding error** next to a
   16 ms frame, even while workers are inside the dump. `write() == 0` is
   the remaining backpressure, not a lock.

(1) held. (2) held only as claim amortization, not as locality. (3) held
for a few-hundred-job dump and failed as dump size grew: tail is leftover
shard work, which scales with the table.

---

## Strengths

**Wait-free publish is fast enough to be invisible.** Empty-callback
throughput on the winning topology (`K=4`, 8 consumers, 2 bulk, `body=16`,
`batch=80`) is **61 Mpps / 7.4 GiB/s** body. Same topology, `body=1024`,
`batch=63`: **4.6 Mpps / 35 GiB/s**. The winning region is wide (`batch ≥
32`, `K=4` and `K=8` close). `batch=1` is 4.5× slower at `body=16` (13.4
Mpps) and 1.5× slower at `body=1024` (3.1 Mpps) — publish-side chunking
already shows the same amortization the consume side later isolated.

A 10k-job tick dump at those rates is tens of microseconds of pipe, not
milliseconds. For the stated purpose the farm *can* be nearly transparent
as a conveyor.

**Hot path is `fetch_add`, and it shows.** No CAS on `write()` or on the
primary claim. Ticket CAS is registration only. That is why a 1-payload
`write()` itself is tens to hundreds of nanoseconds, and why idle
publish → first `Call` sits at **p50 350 ns / p99 5.5 µs** with six pinned
spinners. The algorithm matches the product constraint: mid-tick publish
does not take a lock and does not bounce a CAS.

**Claim amortization is real.** Digest (execute window only, `Call` reads
the body, all arms share the same walker): `linear16` is **1.5–1.6×**
cheaper than `linear1` at `body=16` (~31 ns/job vs ~48 ns/job). The
dividend survives a cold chase into a 32 MiB out-of-farm array and two
alternating `Call`s. It vanishes at `body=1024` (~275 ns/job), where
bandwidth dominates. This is the consume-side twin of `batch=1` vs
`batch=80`. Sell **one `fetch_add` per sixteen `Call`s**, not adjacency.

**Small-after-small dispatch is better than the shard-0 story suggests.**
A 32-job dump (`Tlen < 64`) plus a 1-payload sentinel: p99 **2.7 µs** at
1 µs background jobs, *below* idle p99. Workers who do not map to shard 0
skip the dump and walk into the sentinel. Skip-and-advance, which is a
hazard for the skipped table, is a shortcut for whatever was published
after it. A burst of 32 sentinels during a 256-job dump does not pile up
on shard 0 either (last p99 38 µs, first 67 µs — remaining dump work, not
serialization).

**`write() == 0` is rare while anyone is draining.** Parked consumers: the
wall is real (~30k jobs at this topology before a small `write` returns
0). Concurrent spray + six workers: 1000 mid-tick admits saw **zero**
zeros; admit p99 7.9 µs, p99.9 1.5 ms. The live hazard is *admitted but
late*, not *rejected*. The subscribe-and-drain hatch was not needed on
the digest topology.

**Pinning is enough to keep the scheduler out of p99.** Unpinned idle max
was 1.2 ms. Pinned `nc=6` on physical 0–5: idle max 24 µs. `nc=8` oversub
on the same 6c/12t box does not invent a new tail (mid-drain 256 / 1 µs
p99 still 32 µs). Measure tail pinned or do not quote it.

---

## Weaknesses

**Locality of the linear chunk is not a dividend on this machine.**
Shuffled `Tindex` (same claim of 16, payloads not adjacent) matches
linear at 16 MiB and at 128 MiB (`1.00–1.02×`). Copy-out of a 264 B
payload is free. A 32 MiB cold chase adds ~16 ns to every arm equally.
I-cache (one `Call` vs two) does not move the number. The 16 MiB farm
fits in L3; even a farm that does not (128 MiB) did not resurrect
scatter as a cost for xor-body `Call`s. Phase 2 (pointer pool /
moodycamel execute compare) was skipped because there was no consume-side
layout gap to sell.

**Mid-tick tail is leftover shard work, and it grows with the dump.**
`consumeNext` is one table. `processShard` does not yield after
`CLAIM_CHUNK`. It claims until that consumer is done with its shard
(`sqcsOf(6) = 3` slices). A 1-payload table at `Tnext` is invisible until
that visit returns, and then only to `Shi == 0` or a carried sweeper.

Same 1 µs background `Call`, stock path:

| Dump | Shard | p50 | p99 |
|------|-------|-----|-----|
| 256 | ~85 | 11 µs | 32 µs |
| 2048 | ~682 | 237 µs | 281 µs |
| 8192 | ~2730 | 1.08 ms | **1.82 ms** |

p99 / job-time is ~30 leftover jobs at 256 (a couple of chunks, after the
`T/4` trigger) and ~1800 jobs at 8192 (a shard still incomplete when the
sentinel lands). Empty `Call`s hide this (256 / 0 ns looks like idle).
32 µs is fine at 60 Hz. 1.8 ms is a slice of the frame. Throughput bought
a long visit; tail is that visit.

**A shorter visit cuts the tail, and is not production.** Bench yield-16
(end the visit after one run of 16; one stock worker drains leftovers):
256 / 1 µs p99 **5.4 µs**. Claim-1: **431 ns**. Both are contaminated by
the mid-drain trigger — by `T/4` those workers are already polling
`Tnext` — but the order is the mechanism. They are not a free switch:

- Mixed chunk sizes on one `Tcount` deadlock (`shiter` disagrees).
- If every worker advances after one run, leftovers sit on the *current*
  segment. Idle re-walk (`sweepOldestTrailing`) only covers *trailing*
  segments. Same-segment leftovers stall. The 5j backstop does not save a
  yield-and-advance policy without a drainer or a position re-walk.
- Yielders who are not `Shi == 0` skip the small sentinel unless the
  bench forces a shard-0 try. Production relies on a native or a carried
  sweeper, and the carried sweeper is often the worker who just finished
  a shard — the delayed one.

**Small mid-tick tables are not “first free worker.”** `Tlen < 64` is
shard 0 wholesale. Everyone else advances past. The job is not lost, but
dispatch is “first worker who has left the current table *and* may take
shard 0.” That is the same rule that makes a small *previous* table a
fast path for the next one.

**The OOB channel the design refused wins this tail by construction.**
Dedicated atomic poller, dump still on the farm: p99 **130 ns at both 256
and 8192**. Dump size drops out because no `processShard` stands between
publish and `Call`. A mailbox checked only *after* stock `consumeNext`
would reproduce the farm tail. The thing that mitigates shard coupling is
looking between jobs (or looking at nothing else), not a different MPMC.
A mutex+futex wake would also bypass the shard and typically add 10–50 µs
of wakeup — better than 1.8 ms, worse than a spinning poller, and a lock
on the mid-tick path.

**Occupancy is exact and unforgiving when nobody drains.** `Exmax` plus
dummy `Sub0` is why digest had to publish with `nb=1` to fit a lap, and
why a parked fill hits `write() == 0`. The ring will not pretend to be
unbounded. That is a strength as backpressure and a weakness versus a
pointer MPMC that always admits.

**Two consumers is the worst consumer count.** `sqcsOf(2) = 1` (same as
one consumer) plus an extra coherence participant. Topology sweep: `nc=2`
lost to both `nc=1` and `nc=4`. Not a tail finding; do not ship a “two
worker” config thinking it is the small-N optimum.

---

## What each bench isolated

| Bench | Window | Question | Answer |
|-------|--------|----------|--------|
| `throughput` | publish + empty `Call` | Can the pipe be cheap? | Yes. 61 Mpps / 35 GiB/s. |
| `digest` | execute only, body touched | Does linear layout beat scatter? | No. Claim-16 beats claim-1. Shuffle ≈ linear. |
| `tail` | `write()` of one job → its `Call` | Does mid-tick stay cheap on the same ring? | At hundreds of jobs, yes. At thousands of 1 µs jobs, no. |

`throughput` used an increment callback and cannot speak to digest or
tail. `digest` used a custom walker so layout/chunk/copy-out were the
only variables; it cannot speak to `consumeNext` visit length. `tail`
uses production `consumeNext` (stock) and must not be read as another
Mpps number.

Moodycamel was not linked. Digest killed the layout story before a
named competitor could explain it. Tail’s competitor is a mailbox, not
another item-rate queue: the failure mode is visit length, not CAS retry
on enqueue.

---

## Settled reading

- Quote **payloads/s** for “can a tick dump enter the ring.” Quote
  **ns/job of a body-touching `Call`** for consume. Quote **p99
  publish → `Call`** for mid-tick. Do not mix the three.
- The consume-side product line is **one claim, sixteen `Call`s.** It is
  not “the bytes are adjacent.”
- Mid-tick on the stock path is **first worker to finish a shard** (and
  pass the small-table gate). If tick dumps stay a few hundred 1 µs-class
  jobs, that is tens of microseconds and the ring stays the only channel.
  If dumps grow to thousands, refusing an OOB path is a latency choice
  (~1.8 ms vs ~130 ns on this box), not a free invariant.
- Do not flip production to yield-per-chunk or claim-1 to buy tail.
  Drain and `Tcount` arithmetic assume one `shiter` and a visit that
  finishes the shard (or a sweeper). Changing visit length is a spec
  revision, not a flag.
- Keep `fatal()` in release. A wrapped `Tcount` / unsizable payload /
  bad token is not a throughput event.

---

## Not measured

- Makespan of a tick (first dump sentinel → last dump `Call`). Different
  number; dump size would dominate it even with a mailbox.
- Sleeping workers + futex wake. The farm’s story is a spinning job
  system; wakeup tail is a different product.
- Mid-tick while a table spans a segment (`plantIfUnprotected` /
  `migrateToFrontier` on the path). Optional in the tail plan; p99.9 of
  stock mid-drain did not require it to explain the shape.
- Real game `Call`s (world mutation, waits, fan-out). Spin-1 µs is a
  stand-in. If real jobs immediately chase cold world state, digest
  already says the farm walk is a rounding error and tail is still the
  visit.
- A second farm, steal deque, or moodycamel as a *dump* path. Not needed
  to explain these results.

---

## Drivers

```
make -C perftest run          # 16 MiB topology + shape sweep
make -C perftest digest-run
make -C perftest tail-run
```

How to run and the result tables live in `README.md`. This file is the
verdict, not the lab notebook.
