# Ant Farm 1.7.0-rc.3

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish
tables of work into a shared ring; any subscribed consumer may claim them.
It is designed for unordered compute where work can appear on any thread and
the next available worker should take it.

This repository contains four cooperating packages:

| Package | Start here when you need |
| --- | --- |
| `antfarm` | allocation-free `@nogc nothrow` payload distribution |
| `threadpool` | persistent workers pinned and grouped by cache topology |
| `actors` | stable identities, owned state, inboxes, and phase-oriented waves |
| `fibers` | waiting, yielding, and cancellation on those workers |

Use `antfarm` alone, or add only the higher-level packages a workload needs.
For the complete relationship and sizing rules, see
[ARCHITECTURE.md](ARCHITECTURE.md). Current release work is tracked in
[ROADMAP.md](ROADMAP.md).

The [project writeup](writeup.md) connects the design, optimization history,
and measured payload, Fiber, actor-wave, and complete-lifecycle results.

## Build and test

DMD 2.112+ and LDC 1.42+ are supported on Windows x64 and Linux x86-64.

```text
dub build
ANTFARM_HUGE_PAGES=0 dub test --compiler=dmd
ANTFARM_HUGE_PAGES=0 dub test --compiler=ldc2
ANTFARM_HUGE_PAGES=0 dub run -c unittest --compiler=dmd
ANTFARM_HUGE_PAGES=0 dub run -c unittest --compiler=ldc2
```

Both commands run the root integration suite in `antfarm_test.d`. `dub test`
also compiles and runs module `unittest` blocks; `dub run -c unittest` runs
the suite alone. See [fibers/README.md](fibers/README.md#build-and-test)
for the separate Fiber smoke and stress suites.

Ordinary 4 KiB backing is the default. The environment override makes that
choice explicit for tests even if the calling shell is configured otherwise.
The actor runtime has a separate deterministic-interleaving and
sustained-contention suite:

```text
make -C actor_torture run
make -C actor_torture run-dmd
make -C actor_torture run-tsan
```

The [performance benchmarks](perftest/README.md) cover payload throughput,
actor dispatch, and Fiber lifecycle bookkeeping. The
[complete actor lifecycle benchmark](perftest/LIFECYCLE.md) measures repeated
creation, dispatch, retirement, and reclamation with the C-runtime and optional
mimalloc policies, including wave batching and consumer placement.

The actor runtime's optional mimalloc v3 adapter is documented in
[ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md); ordinary builds retain the C-runtime
default and do not link mimalloc. The real allocator lane uses the pinned
submodule:

```text
git submodule update --init --recursive
make -C actor_torture run-mimalloc
make -C actor_torture run-mimalloc-debug
```

Dub's `mimalloc-v3` configuration also builds and links this pinned release
archive; it does not require a system mimalloc installation.

## First payload

Application functions can be adapted into Ant Farm payloads with
`payloadRange`. The callback must be `nothrow @nogc`; its arguments are packed
into the ring and reconstructed on a consumer. This complete single-threaded
program produces and consumes on one thread:

```d
import antfarm_templates;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad;
import std.range : iota;

__gshared shared(ulong) squareSum;
__gshared shared(ulong) completed;

void addSquare(ulong value) nothrow @nogc @system
{
    atomicFetchAdd!(MemoryOrder.raw)(squareSum, value * value);
    atomicFetchAdd!(MemoryOrder.rel)(completed, 1UL);
}

void main()
{
    // One consumer, no bulk producers, one small producer with 4096 words of quota.
    auto farm = AntFarm.create(ln: 1UL << 18, k: 8, expectedConsumers: 1,
                               maxBulk: 0, maxSmall: 1, quotaSmall: 4096);
    scope (exit) farm.destroy();

    ConsumerView view;               // one persistent cursor per consuming thread
    long subscribed;
    do
        subscribed = view.subscribe(farm);
    while (subscribed == SUBSCRIBE_RETRY);
    if (subscribed < 0) assert(0, "Farm full or view already subscribed");
    scope (exit) view.unsubscribe();

    auto token = farm.registerProducer(Tier.small);
    scope (exit) farm.unregisterProducer(token);

    auto jobs = payloadRange!addSquare(iota(1UL, 257UL));
    while (!jobs.empty)
    {
        immutable written = farm.write(jobs, token);
        if (written != 0)
            jobs.popFrontN(written); // write() never advances the caller's range
        else
            view.consumeNext();      // backpressure: help drain, then retry
    }

    // The Farm has no "all done" signal; count completions yourself.
    while (atomicLoad!(MemoryOrder.acq)(completed) < 256)
        view.consumeNext();
}
```

Things the example relies on:

- `AntFarm.create` has nine parameters. Named arguments, as above, keep the
  call readable; omitted ones take their defaults. See
  [Sizing one Farm](ARCHITECTURE.md#sizing-one-farm).
- `write()` returns how many payloads it published. Pop exactly that many and
  retry; `0` is backpressure, not failure. A generic source passed to
  `write()` must be a forward range because the Farm checkpoints it for
  sizing and emission.
- `consumeNext() == false` means nothing is ready at this cursor right now. It
  may be a temporary hole, not global emptiness, and there is no FIFO
  guarantee. Completion is the application's to track, e.g. with a counter
  incremented by the callback.
- `subscribe()` returns the starting epoch (`>= 0`) or a negative code:
  `SUBSCRIBE_RETRY` is transient, so call again; `SUBSCRIBE_FULL` means the
  Farm already has `MAX_CONSUMERS_LIMIT` (128) views; `SUBSCRIBE_INVALID` is a
  null Farm or an already subscribed view.
- Teardown order is unsubscribe consumers, unregister producers, then destroy
  the Farm. The `scope (exit)` blocks above run in that order.

For the multi-threaded version, with one consumer per pinned worker, build and
run [examples/iota_sum.d](examples/iota_sum.d):

```text
dmd -g -i examples/iota_sum.d antfarm.d antfarm_templates.d \
    -Ithreadpool/source -of=iota_sum
ANTFARM_HUGE_PAGES=0 ./iota_sum
```

Each worker subscribes a thread-local `ConsumerView` in its `workerBody` and
unsubscribes it in the pool's `workerStop` hook, which runs on every worker
before `pool.shutdown()` returns. Only after that may the Farm be destroyed.

## First actor and wave

The `actors` package adds explicit state lifetime and exclusive callback-local
borrowing without changing the worker side: ordinary Farm consumers execute
actor activations and wave tables. An `ActorOwner` controls retirement and
reclamation, while its copyable `ActorHandle` is the identity used to wake,
send to, or phase-dispatch the actor. The examples below assume the `farm`,
`token`, and consuming `view` from [First payload](#first-payload);
`drainOrYield()` stands for `view.consumeNext()` or a pool worker's pump.

```d
import actors;

struct Body { ulong position; }

void onWake(scope ref ActorBorrow!Body actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++actor.value.position;
    // context.republish(); // request another autonomous activation
}

void movement(scope ref ActorBorrow!Body actor) nothrow @nogc @system
{
    actor.value.position += 4;
}

auto runtime = ActorRuntime.create(farm, 1024);
auto owner = runtime.createActor!(Body, onWake)(Body.init);
auto handle = owner.handle;

handle.wake();                 // coalescing autonomous activation
runtime.flush(token, 32);      // publish queued actors into the Farm
```

A wave dispatches one phase operation across a collection of idle actor
handles created by the same runtime. It may span several physical Farm tables,
but exposes one generation-tagged completion:

```d
ActorWave wave;
wave.begin(farm);

size_t offset;
while (offset < handles.length)
{
    immutable written = wave.publish!movement(handles[offset .. $], token);
    if (written == 0)
    {
        if (wave.handle.failed) break;
        drainOrYield();         // Farm backpressure
        continue;
    }
    offset += written;
}

auto completion = wave.seal();
while (!completion.finished)
    drainOrYield();
```

Actors in the same wave run in parallel and must not consume one another's
intermediate writes. Observing `completion.finished` with acquire semantics and
confirming `!completion.failed` is the boundary after which an orchestrator may
publish a dependent wave. For cross-actor reads, keep private mutable actor
state plus caller-owned public projections; write the inactive public buffer
during a phase and select it only after successful wave completion. The actor
package supplies the exclusive borrow and completion boundary, not a
prescribed projection layout.

An `ActorWaveTrigger` can park a Fiber until that boundary instead of polling;
see [fibers/README.md](fibers/README.md). Detailed lifetime and memory-order
contracts are in [ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md) and
[ACTOR_MEMORYORDER.md](actors/ACTOR_MEMORYORDER.md).

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
and any referenced storage must remain alive until execution. Each argument must
implicitly convert to its parameter type; the shim applies no cast, so a
narrowing conversion or a mutable pointer passed as `immutable` is a compile
error.

## Memory backing

Farms use ordinary 4 KiB pages by default. Huge pages remain an exploitable
option for low-level payload workloads which move very large quantities of
work through the ring. They reduce translation overhead during long sequential
table walks, but are not a universal win and can add promotion or first-touch
costs to shorter and Fiber-backed workloads.

Opt in per Farm with the final construction argument or for a complete process
with the environment override:

```d
auto farm = AntFarm.create(ln: 1UL << 22, k: 8, expectedConsumers: consumers,
                           maxBulk: 0, maxSmall: producers, quotaSmall: quota,
                           hugePages: true);
```

```text
ANTFARM_HUGE_PAGES=1 ./payload_benchmark
```

`ANTFARM_HUGE_PAGES` accepts `0` (force 4 KiB) or `1` (force huge pages); any
other non-empty value is a fatal error rather than being silently ignored.

On Linux this requests `MADV_HUGEPAGE`; the kernel still decides whether to
promote the shared mapping. On Windows it uses `SEC_LARGE_PAGES` and requires
the Lock Pages in Memory right; `grant_lock_pages.d` builds the privilege
helper. `farm.usedLargePages` reports that the requested platform path was
applied, not proof of Linux promotion. Benchmark the actual payload and inspect
the live mapping when page size is material to the result.

## Core lifecycle

1. Create an `AntFarm` with ring size, segment count, expected consumer
   count (a sharding hint; the hard cap is 128 views), and producer-tier
   limits.
2. Subscribe one persistent `ConsumerView` per active consumer. It is a unique
   cursor and must not be copied.
3. Register one producer `Token` per active producer. Copying transfers it;
   assigning over a still-registered token is fatal, so unregister first.
4. Publish useful batches and pop the returned count from the source.
5. Consume with `consumeNext()` until application completion.
6. Unsubscribe consumers, unregister producers, then destroy the Farm.

Producer registration and deregistration take a Farm-local mutex; publishing
and consuming never lock. Quota mechanics are specified in
[SPEC.md](SPEC.md#5-producer-protocol).

Distinct Farms may be created concurrently. Windows mapping API initialization
uses a process-wide lock; POSIX mapping names use an atomic counter. Teardown
still requires exclusive ownership and no live consumers or producer tickets.


## Next layers

- [threadpool/README.md](threadpool/README.md) shows topology discovery,
  worker ownership, and Director wake/cadence policy.
- [actors/ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md) shows autonomous actors,
  inbox ownership, retirement, and dependent phase waves.
- [fibers/README.md](fibers/README.md) shows when and how to run managed Fibers
  on the same workers.
- [SPEC.md](SPEC.md) is the core algorithm and memory-order contract.
- [review_torture/README.md](review_torture/README.md) describes the extended
  concurrency and ThreadSanitizer suite.

Ant Farm is licensed under the Boost Software License 1.0.
