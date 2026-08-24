# Ant Farm

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. The last ant to leave a *confirmed-complete* area changes the tally (a reference counter) to zero. The last ant to leave an *incomplete* area instead leaves a pulse mark, so the farmer still sees a nonzero tally and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

It is a fixed-memory M:N job distributor, not a queue. Producers publish tables of mixed serial and parallel jobs onto a ring; spinning consumers claim chunks independently. There is no reliable FIFO. `write()` and `consumeNext()` are `@nogc nothrow`. Invariant violations abort. Throughput figures with working range of topologies (~200-390M items/s on Intel 12700H) sit comfortably between moodycamel no-tokens (~100-180M items/s) and with-tokens (~400M-1000M items/s), with the overall shape favoring *more* concurrency compared to moody's no-tokens topologies.

---

## Bring a farm up

```d
import antfarm_templates; // also imports antfarm
import core.thread;

long job(ulong x) nothrow @nogc @system { /* ... */ return 1; }

void main()
{
    auto f = AntFarm.create(); // 8 MiB ring, K=8, 4 expected consumers
    scope (exit) f.destroy();

    auto tok = f.registerProducer(Tier.small);
    ConsumerView v;
    if (v.subscribe(f) < 0) return;

    PayloadHeader hdr;
    ulong[256] bodies;
    PayloadEntry[256] table;
    foreach (i; 0 .. table.length)
        table[i] = payloadEntry!job(&hdr, bodies[i .. i + 1], i);

    size_t off;
    while (off < table.length)
    {
        immutable n = f.write(table[off .. $], tok); // avgCost defaults to 1 (chunk 16)
        if (n == 0) { while (!v.consumeNext()) Thread.yield(); continue; }
        off += n;
    }
    while (v.consumeNext()) {}

    v.unsubscribe();
    f.unregisterProducer(tok);
}
```

`create(ln, k, expectedConsumers, maxBulk, quotaBulk, maxSmall, quotaSmall, smallThreshold, hugePages)`:

- `ln` — ring length in ulongs, power of two, ≥ 2¹⁸ (2 MiB of ulongs). Default `1 << 20` is 8 MiB.
- `k` — segments, 4 or 8.
- An unused producer tier: pass `maxRole = 0`. Bulk quota `0` with `maxBulk > 0` auto-fills to one segment.
- `hugePages` defaults on. Windows needs **Lock Pages in Memory** (`grant_lock_pages.exe`, then log off/on) or `create()` fatals; `ANTFARM_HUGE_PAGES=0` forces 4K.

Keep the `Token` by reference for the producer’s life; copying transfers it. `write() == 0` means full — drain and retry, or subscribe yourself and drain. Destroy only with no live consumers or tickets.

```text
dmd -g antfarm.d antfarm_templates.d antfarm_test.d "-ofantfarm_test.exe"
dmd -g grant_lock_pages.d "-ofgrant_lock_pages.exe"
```

---

## Topology and batches

On a 16 MiB ring (L3-sized), the 12700H sweeps were unambiguous:

- **Batch 256** (or at least something significantly > 1). `batch=1` is an order of magnitude slower. Let the producer’s quota bound the table; don’t trickle one payload at a time.
- **Several small producers with a matching consumer count.** One producer caps out no matter how many consumers you add. Adding both together keeps paying (a 6×6 is already in the 300 M payloads/s range; the grid peak was 13 consumers / 9 producers).
- **`K = 8` or `4`**, ring sized for L3. Bigger rings buy fewer `write()==0` stalls and lose throughput.
- **`avgCost`** per Call family: cheap → `0` (chunk 32), expensive → `2`–`3` (chunk 8–4). Default `1` is chunk 16.

A bulk-tier producer can provide greater amortization but writing all that space blocks consumers before the table is done; many relatively modest tables work better. Workers may hold a publish ticket and publish mid-tick — that is a normal use, not a hack.

---

## What it is for

A **frame tick**: dump a large table, spin a pool, and let any thread publish an urgent job that must reach a worker in tens of microseconds while the dump is still draining. Mixed serial and parallel jobs in the same table. Completion order is not FIFO — accumulating work (tiles, islands, transforms, callbacks), not a pipeline.

Wrong tool for ordered streams, for 1P→1W peak transport, or for an unbounded backlog.

---

## Tokenized moodycamel

moodycamel::ConcurrentQueue with producer and consumer tokens is the faster *transport* when you can live with its shape:

- Tokens turn the queue into near-SPSC lanes. Paired threads on this host reach **hundreds of millions to ~1 B items/s** bulk; Ant Farm sits between that and the no-token numbers, as a distributor, with a 128-byte payload header on top.
- **FIFO.** Occupied tail grows with the backlog (milliseconds behind an 8192-item dump). Ant Farm will overtake a mid-tick job; tokenized moodycamel will not, unless you add another queue.
- Work stays in the lane it was enqueued on. That is the win if a producer is feeding a dedicated consumer. It is the loss if the next job may be born on any worker and must be claimed by whoever is free.

Use tokens when the graph is paired pipes and order matters. Use Ant Farm when the graph is a swarm, jobs appear from anywhere, and a dump plus a mid-tick “do this now” share one ring.

---

`SPEC.md` is the living spec. `writeup.md` has the 12700H numbers. Tests: `antfarm_test.d`, `review_torture/`. Benches: `perftest/`.
