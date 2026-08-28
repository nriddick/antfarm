# Ant Farm 1.5

Picture a farmer walking along a path placing down objects which a swarm of ants are picking up and taking away. He can't see the ants. He doesn't know how many there are. How can he avoid stepping on the ants? The answer: the ants maintain signage at regular intervals tallying how many ants are in the area. If it's higher than zero, the farmer waits. The last ant to leave a *confirmed-complete* area changes the tally (a reference counter) to zero. The last ant to leave an *incomplete* area instead leaves a pulse mark, so the farmer still sees a nonzero tally and will not step there. Thus the farmer only needs to watch for one signal to change, and doesn't need to try and communicate directly with any ants. And the ants have a panoply of strategies to break down and haul away their work pieces. Really the Ant Farm is a combination of known techniques and a disregard of FIFO guarantees; towards objectives of minimal synchronization upkeep, enhanced cache performance, and flexible role switching and load balancing.

It is a fixed-memory M:N job distributor, not a queue. Producers publish tables of mixed serial and parallel jobs onto a ring; spinning consumers claim chunks independently. There is no reliable FIFO. `write()` and `consumeNext()` are `@nogc nothrow`. Invariant violations abort. Throughput figures with working range of topologies (~200-390M items/s on Intel 12700H) sit comfortably between moodycamel no-tokens (~100-180M items/s) and with-tokens (~400M-1000M items/s), with the overall shape favoring *more* concurrency compared to moody's no-tokens topologies.

Version 1.5 is the authoritative home of three cooperating D components:

- `antfarm` is the fixed-memory M:N payload distributor at the repository
  root;
- `threadpool` maps persistent workers to cache topology and gives Director
  policy explicit control over cadence, affinity, and wake-up; and
- `antfarm_fibers` carries freely migrating druntime Fibers through the same
  Farm lanes, so waiting userland and short payload work do not require
  competing worker pools.

The components remain usable independently. The root DUB package advertises
`threadpool/` and `fibers/` as subpackages; the Fiber package depends on the
other two by local path. Start with [`threadpool/README.md`](threadpool/README.md)
for topology and worker ownership, and [`fibers/README.md`](fibers/README.md)
for the colocated scheduler. The combined usage report and immediate work are
[`fibers/POSTMORTEM.md`](fibers/POSTMORTEM.md) and
[`fibers/ROADMAP_pruned.md`](fibers/ROADMAP_pruned.md).

---

## Bring a farm up

```d
import antfarm_templates; // also imports antfarm
import core.atomic;
import core.thread;
import std.range : iota;
import std.stdio;
import threadpool;

enum ulong nTotal = 50_000; // values 1..nTotal: enough to lap the ring

__gshared shared(long) g_sum;   // accumulated by sumJob across pool workers
__gshared shared(ulong) g_done; // payloads executed so far

long sumJob(ulong x) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.rel)(g_sum, cast(long) x);
    atomicFetchAdd!(MemoryOrder.rel)(g_done, 1UL);
    return 1;
}

struct FarmBin
{
    AntFarm* farm;
}

// Pool worker: subscribe to the shared farm and claim chunks until done.
bool pump(WorkerSelf* w) nothrow @nogc @system
{
    static ConsumerView v;
    static bool subscribed;

    if (atomicLoad!(MemoryOrder.acq)(g_done) >= nTotal)
    {
        if (subscribed)
        {
            v.unsubscribe();
            subscribed = false;
        }
        return false; // idle; director policy applies
    }

    auto slot = home!FarmBin();
    if (slot is null)
        return false;
    if (!subscribed)
        subscribed = v.subscribe(slot.farm) >= 0;
    if (!subscribed)
        return false;

    return v.consumeNext();
}

void main()
{
    auto topo = CacheAwarePool.topology();

    PoolOptions opt;
    opt.skipSmtSiblings = true;
    opt.workerBody = &pump;

    uint ncons;
    foreach (ref p; topo.processors)
    {
        if (opt.skipSmtSiblings && p.smtSibling) continue;
        ++ncons;
    }

    auto f = AntFarm.create(1UL << 18, 8, ncons, 0, 0, 1, 4096);
    scope (exit) f.destroy();

    // One shared farm; every worker finds it through its home-LLC bin.
    auto bins = new FarmBin[](topo.llcCount);
    foreach (ref b; bins)
        b.farm = f;
    install(bins);
    scope (exit) uninstall!FarmBin();

    auto pool = new CacheAwarePool(opt);
    pool.start();
    scope (exit) pool.shutdown(true);
    pool.director().spin(); // workers hot-pump until the drain completes

    // Producer (this thread). payloadRange!sumJob pairs the callback with
    // the iota of 1..nTotal: each element becomes one payload whose
    // type-erased callback executes sumJob with that value packed as the
    // 1-ulong body. write() advances its own copy of the range, so we pop
    // what landed and retry when it returns 0 (ring full).
    auto tok = f.registerProducer(Tier.small);
    auto payloads = payloadRange!sumJob(iota(1, nTotal + 1));
    for (ulong left = nTotal;;)
    {
        immutable w = f.write(payloads, tok); // avgCost 1 (chunk 16)
        if (w == 0)
            Thread.yield(); // ring full: workers drain, then retry
        else
        {
            foreach (_; 0 .. w)
                payloads.popFront();
            left -= w;
            if (left == 0) break;
        }
    }
    f.unregisterProducer(tok);

    while (atomicLoad!(MemoryOrder.acq)(g_done) < nTotal)
        Thread.sleep(msecs(1));

    immutable expected = nTotal * (nTotal + 1) / 2; // exact sum
    assert(atomicLoad!(MemoryOrder.acq)(g_sum) == cast(long) expected);
    writeln("summed 1..", nTotal, " to ", atomicLoad!(MemoryOrder.acq)(g_sum));
}
```

The consumer pool is `CacheAwarePool` from the `threadpool` subpackage: one
pinned worker per LP (SMT siblings skipped here), each locating the farm
through its home-LLC bin and claiming chunks in its pump. `main` produces.

`create(ln, k, expectedConsumers, maxBulk, quotaBulk, maxSmall, quotaSmall, smallThreshold, hugePages)`:

- `ln` — ring length in ulongs, power of two, ≥ 2¹⁸ (2 MiB of ulongs). Default `1 << 20` is 8 MiB.
- `k` — segments, 4 or 8.
- An unused producer tier: pass `maxRole = 0`. Bulk quota `0` with `maxBulk > 0` auto-fills to one segment.
- `hugePages` defaults on. Windows needs **Lock Pages in Memory** (`grant_lock_pages.exe`, then log off/on) or `create()` fatals; `ANTFARM_HUGE_PAGES=0` forces 4K.

Keep the `Token` by reference for the producer’s life; copying transfers it. `write() == 0` means full — drain and retry, or subscribe yourself and drain. Destroy only with no live consumers or tickets.

Job metadata and bodies need not originate as collocated `PayloadEntry`
objects. `write(headers, bodies, token, avgCost)` lazily pairs independent
input ranges (header values or pointers, and `const(ulong)[]` bodies), stopping
at the shorter range; `pairPayloads(headers, bodies)` exposes the same shim
for composition with other range layers. `payloadRange!fn(argRange)` goes
further: it takes the callback function and an input range of its packed
arguments (a `Tuple` per element for multi-parameter `fn`, the value itself
for a single parameter) and yields the payload-entry range the example feeds
to `write()` above. All of these advance only when their consumer pops them:
`write()` returns how many payloads landed, and the producer pops that many
before the next call.

`ConsumerView.consumeQuantum()` is the bounded scheduler-oriented counterpart
to `consumeNext()`: it claims at most one primary chunk from the next table and
advances. Other consumers, or the normal idle re-walk, finish remaining chunks.

```text
dmd -g antfarm.d antfarm_templates.d antfarm_test.d "-ofantfarm_test.exe"
dmd -g -i iota_sum.d antfarm.d antfarm_templates.d -Ithreadpool/source "-ofiota_sum.exe"
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

1.5.0: make Ant Farm, the cache-aware threadpool, and the freely migrating
Fiber scheduler one authoritative project. The lower layers retain their
allocation-free contracts; the managed worker lane provides the GC-enabled
bridge used by Fibers.

1.0.1: TSan hygiene. Ring words are raw atomics, leaf RMWs are acq_rel, and `make -C review_torture run-tsan` defaults to `history_size=7`. A TSan report is a defect.

Licensed under the Boost Software License 1.0 (`LICENSE`).
