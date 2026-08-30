# Ant Farm 1.6.1

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish
tables of work into a shared ring; any subscribed consumer may claim them.
It is designed for unordered compute where work can appear on any thread and
the next available worker should take it.

This repository contains three cooperating packages:

| Package | Start here when you need |
| --- | --- |
| `antfarm` | allocation-free `@nogc nothrow` payload distribution |
| `threadpool` | persistent workers pinned and grouped by cache topology |
| `antfarm-fibers` | waiting, yielding, and cancellation on those workers |

The packages can be used separately. For the complete relationship and sizing
rules, see [ARCHITECTURE.md](ARCHITECTURE.md). Current release work is tracked
in [ROADMAP.md](ROADMAP.md).

## Build and test

DMD 2.112+ and LDC 1.42+ are supported on Windows x64 and Linux x86-64.

```text
dub build
ANTFARM_HUGE_PAGES=0 dub test --compiler=dmd
ANTFARM_HUGE_PAGES=0 dub test --compiler=ldc2
```

Ordinary 4 KiB backing is the default. The environment override makes that
choice explicit for tests even if the calling shell is configured otherwise.
The experimental actor payload has a separate deterministic-interleaving and
sustained-contention suite:

```text
make -C actor_torture run
make -C actor_torture run-dmd
make -C actor_torture run-tsan
```

Its optional mimalloc v3 adapter is documented in
[ACTOR_ROADMAP.md](ACTOR_ROADMAP.md); ordinary builds retain the C-runtime
default and do not link mimalloc. The real allocator lane uses the pinned
submodule:

```text
git submodule update --init --recursive
make -C actor_torture run-mimalloc
make -C actor_torture run-mimalloc-debug
```

## First payload

Application functions can be adapted into Ant Farm payloads with
`payloadRange`. The callback must be `nothrow @nogc`; its arguments are packed
into the ring and reconstructed on a consumer.

```d
import antfarm_templates;
import core.atomic : MemoryOrder, atomicFetchAdd;
import std.range : iota;

__gshared shared(ulong) squareSum;

void addSquare(ulong value) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(squareSum, value * value);
}

auto jobs = payloadRange!addSquare(iota(1UL, 257UL));
auto token = farm.registerProducer(Tier.small);
while (!jobs.empty)
{
    immutable written = farm.write(jobs, token);
    if (written != 0)
        jobs.popFrontN(written);
    else
        drainOrYield();
}
farm.unregisterProducer(token);
```

`write()` does not advance the caller's range. Pop exactly the number it
returns, then retry. A generic source passed to `write()` must be a forward
range because the Farm checkpoints it for sizing and emission.

For a complete program using a cache-aware worker pool, build and run
[examples/iota_sum.d](examples/iota_sum.d):

```text
dmd -g -i examples/iota_sum.d antfarm.d antfarm_templates.d \
    -Ithreadpool/source -of=iota_sum
ANTFARM_HUGE_PAGES=0 ./iota_sum
```

## Choosing a write path

- `write(PayloadEntry[], token)` accepts already assembled payloads.
- `write(headers, bodies, token)` lazily pairs independent forward ranges.
- `write(header, bodies, token)` broadcasts one common header.
- `write(header, bodies, bodyWords, token)` also promises a fixed body width,
  allowing arithmetic sizing without a body inspection pass.
- `payloadRange!fn(arguments)` generates a common callback and fixed packed
  width automatically.

The common-header and fixed-width forms are useful for homogeneous batches.
They retain the same partial-write contract as `PayloadEntry[]`.

The callback receives its copied body words read-only. That constness protects
the ring representation; it does not assert that an object named by a manually
encoded handle is transitively immutable. Generated shims apply a separate,
stricter policy: packed arguments may not contain unshared mutable aliases.
Immutable references and explicitly shared/thread-safe interfaces are allowed,
and any referenced storage must remain alive until execution.

## Memory backing

Farms use ordinary 4 KiB pages by default. Huge pages remain an exploitable
option for low-level payload workloads which move very large quantities of
work through the ring. They reduce translation overhead during long sequential
table walks, but are not a universal win and can add promotion or first-touch
costs to shorter and Fiber-backed workloads.

Opt in per Farm with the final construction argument or for a complete process
with the environment override:

```d
auto farm = AntFarm.create(1 << 22, 8, consumers,
                           0, 0, producers, quota,
                           DEFAULT_SMALL_TABLE_THRESHOLD, true);
```

```text
ANTFARM_HUGE_PAGES=1 ./payload_benchmark
```

On Linux this requests `MADV_HUGEPAGE`; the kernel still decides whether to
promote the shared mapping. On Windows it uses `SEC_LARGE_PAGES` and requires
the Lock Pages in Memory right; `grant_lock_pages.d` builds the privilege
helper. `farm.usedLargePages` reports that the requested platform path was
applied, not proof of Linux promotion. Benchmark the actual payload and inspect
the live mapping when page size is material to the result.

## Core lifecycle

1. Create an `AntFarm` with ring size, segment count, consumer capacity, and
   producer-tier limits.
2. Subscribe one persistent `ConsumerView` per active consumer. It is a unique
   cursor and must not be copied.
3. Register one producer `Token` per active producer. Copying transfers it.
4. Publish useful batches and pop the returned count from the source.
5. Consume with `consumeNext()` until application completion.
6. Unsubscribe consumers, unregister producers, then destroy the Farm.

`write() == 0` is backpressure, not failure. `consumeNext() == false` may be a
ring hole rather than global emptiness. Ant Farm does not promise FIFO order.

## Next layers

- [threadpool/README.md](threadpool/README.md) shows topology discovery,
  worker ownership, and Director wake/cadence policy.
- [fibers/README.md](fibers/README.md) shows when and how to run managed Fibers
  on the same workers.
- [SPEC.md](SPEC.md) is the core algorithm and memory-order contract.
- [review_torture/README.md](review_torture/README.md) describes the extended
  concurrency and ThreadSanitizer suite.

Ant Farm is licensed under the Boost Software License 1.0.
