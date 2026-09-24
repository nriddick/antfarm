# Actor ready-backlog benchmark

`actor_ready.d` measures autonomous actor dispatch from a prequeued burst.
One thread flushes batches and consumes the resulting Farm tables. Every
callback increments its own actor's counter; the program checks exact counts
and an empty ready queue after every round. Actor construction, waking,
counter verification, and reclamation are outside the timed region.

```sh
make -C perftest actor_ready
ANTFARM_HUGE_PAGES=0 ./perftest/actor_ready 4096 200 32
ANTFARM_HUGE_PAGES=0 ./perftest/actor_ready 16384 100 32
```

Arguments are actor count, measured rounds, and flush batch (1–256). Five
warm-up rounds precede measurement. The Farm uses an 8 MiB ring, eight
segments, one consumer, one small producer with an 8,192-word quota, and
`avgCost=0`. The default build is LDC `-O2 -release`.

## Bounded snapshot comparison

Measured on 2026-09-24 on a Ryzen 5 5500, Linux x86-64, LDC 1.43.0 / LLVM
22.1.8, with `ANTFARM_HUGE_PAGES=0`. Each entry is the median of three runs,
alternating the old and new binaries. Both binaries use this benchmark source;
the baseline actor implementation is commit `8e988f6`. Batch size is 32.

| Actors | Measured rounds | Before ns/actor | After ns/actor | Speedup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 200,000 | 170.69 | 151.24 | 1.13× |
| 32 | 20,000 | 27.86 | 27.25 | 1.02× |
| 256 | 4,000 | 48.80 | 25.35 | 1.93× |
| 4,096 | 200 | 485.91 | 27.79 | 17.49× |
| 16,384 | 100 | 1,873.62 | 23.50 | 79.73× |

Previously, every flush detached the whole ready list, selected its batch,
then traversed and individually requeued the remaining actors. Draining `N`
actors in batches of `B` incurred approximately `O(N²/B)` queue work. The
bounded snapshot keeps the untouched chain in the runtime, protected by a
short acquire/release drain gate, and removes at most `B` nodes per call.
A complete burst now requires `O(N)` queue work. A contending flusher returns
zero and retries; the gate is released before Farm publication. Selected
actors rejected by Farm backpressure are still requeued.

These are synthetic burst-drain measurements on one thread, not concurrent
worker scaling or application throughput guarantees. Small bursts include
proportionally more timing and Farm-table overhead. Compare the same actor
count, round count, batch, compiler flags, and page mode on both revisions.

The actor torture suite checks the corresponding ownership behavior under a
forced drain handoff, concurrent wakes, partial publication, repeated full-Farm
retries, four flushers, and two consumers. See
[actor_torture/README.md](../actor_torture/README.md) and the
[ready-queue ordering contract](../actors/ACTOR_MEMORYORDER.md#ready-queue-ownership).
