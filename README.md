# Ant Farm

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish
tables of work into a shared ring; any subscribed consumer may claim them.
It is designed for unordered compute where work can appear on any thread and
the next available worker should take it.

**Status.** The current development line targets **1.8.0-rc.1**. It is
release-candidate software: the public and private actor models are supported
functionality, but source and ABI stability are not promised across release
candidates, and actor state and results have the limits stated in their
guides. The correctness gate is the local Linux `make check`; there is no CI.
Windows (x64) is an intended target whose validation follows the release
candidate. See [CHANGELOG.md](CHANGELOG.md)
for what the candidate contains and its compatibility notes.

## Quickstart (Linux)

You need DMD 2.112+ or LDC 1.42+. From a clone of this repository, build and run
a complete tracked program that distributes work to a public actor service:

```text
dmd -g -i -I. -Iexamples examples/public_service.d -of=/tmp/public_service
ANTFARM_HUGE_PAGES=0 /tmp/public_service 0
```

Expected output, with exit status 0:

```text
public service: 36 ordered replies, balance=234, workers=0, closing-callback-count=0
```

The `0` consumes on the calling thread; `/tmp/public_service 2` uses two worker
threads and prints `workers=2` with the same counts. LDC works with the same
command (`ldc2 -g -i -I. -Iexamples examples/public_service.d
-of=/tmp/public_service`). If the program prints nothing or asserts, the toolchain
is the first thing to check. [examples/README.md](examples/README.md) has the
other programs and the Windows commands.

## Where to go next

| I want to... | Start with |
| --- | --- |
| **Run something** | [First payload](#first-payload) (the smallest complete program), then [examples/README.md](examples/README.md) for six actor programs and [threadpool/README.md](threadpool/README.md) for pinned workers. |
| **Choose and use actors** | The [choice table](#choosing-the-work-representation) below, then the [public actor guide](actors/ACTOR_ROADMAP.md) or the [private actor guide](actors/PRIVATE_ACTORS.md), the shared [lifecycle contract](actors/LIFECYCLE.md), and the matching [example](examples/README.md). |
| **Contribute** | [CONTRIBUTING.md](CONTRIBUTING.md) for `make check` and the change checklist, [ARCHITECTURE.md](ARCHITECTURE.md) for the layers, and [SPEC.md](SPEC.md) for the Farm algorithm and memory-order contract. |

This repository contains four cooperating packages:

| Package | Start here when you need |
| --- | --- |
| `antfarm` | allocation-free `@nogc nothrow` payload distribution |
| `threadpool` | persistent workers pinned and grouped by cache topology |
| `actors` | public identities, inboxes and waves, or private parent-owned hierarchies |
| `fibers` | waiting, yielding, and cancellation on those workers |

Use `antfarm` alone, or add only the higher-level packages a workload needs.
For the complete relationship and sizing rules, see
[ARCHITECTURE.md](ARCHITECTURE.md). Planned work is in [ROADMAP.md](ROADMAP.md).

### Choosing the work representation

| Your work is... | Use | Example |
| --- | --- | --- |
| short, `nothrow @nogc`, no identity | a payload ([First payload](#first-payload)) | [iota_sum.d](examples/iota_sum.d) |
| a stable identity that outside code addresses: a service with an inbox, several event sources | a **public actor** (`ActorOwner` / `ActorHandle`) | [public_service.d](examples/public_service.d) |
| a phase run over an externally selected set of identities | a public **actor wave** | [public_wave.d](examples/public_wave.d) |
| one exclusive computation tree under a single conductor, with typed joined results or non-POD state | a **private actor** hierarchy | [private_tree.d](examples/private_tree.d) |
| per-request tracked roots with deadlines and cancellation | private actors with `tracking` | [request_deadline.d](examples/request_deadline.d) |
| ordinary D exceptions, allocation, timers, or joins while waiting | a managed **Fiber** | [fibers/README.md](fibers/README.md) |

A public endpoint may front a private tree; their lifetimes still drain
separately. The shared contracts are in [actors/LIFECYCLE.md](actors/LIFECYCLE.md),
and [ARCHITECTURE.md](ARCHITECTURE.md#choosing-the-work-representation) gives
the longer comparison.

## Build and test

```text
dub build
```

builds the library. The full local correctness gate is `make check`; what it
runs, its prerequisites, and the focused and optional lanes are in
[CONTRIBUTING.md](CONTRIBUTING.md). Ordinary 4 KiB backing is the default;
`ANTFARM_HUGE_PAGES=0` makes that choice explicit for a run. See
[Memory backing](#memory-backing). The optional mimalloc v3 actor allocator is
described in [ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md); ordinary builds
retain the C-runtime default and do not link it.

Performance measurement is a separate, quiet-host workflow described in
[perftest/README.md](perftest/README.md); it is not part of `make check`.

## First payload

Application functions can be adapted into Ant Farm payloads with
`payloadRange`. The callback must be `nothrow @nogc`; its arguments are packed
into the ring and reconstructed on a consumer. This complete single-threaded
program produces and consumes on one thread. `make check` extracts this block
and runs it with LDC and DMD in debug and release:

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
    auto farm = AntFarm.create(ringMiB: 2, k: 8, expectedConsumers: 1,
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

To run it, save the program as `/tmp/first_payload.d`, then from the
repository root:

```text
dmd -g -i -I. /tmp/first_payload.d -of=/tmp/first_payload
ANTFARM_HUGE_PAGES=0 /tmp/first_payload
```

It prints nothing and exits with status 0 once all 256 jobs have run.

Things the example relies on:

- `AntFarm.create` has nine parameters. Named arguments, as above, keep the
  call readable; omitted ones take their defaults. `ringMiB` is the ring size
  in MiB (a power of two, at least 2); quotas are in ulongs. See
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
- The `scope (exit)` blocks above unregister the producer, unsubscribe the
  same-thread consumer, then destroy the Farm. All producers and consumers
  must be quiescent before Farm destruction.

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

The [private actor model](actors/PRIVATE_ACTORS.md) adds self-owned
roots and exclusively parent-owned child cohorts. Blind parks emit dispatches;
state is visible only inside the actor's authorized callback. See its
ownership limits before choosing it over the public actor API. Its
[documentation programs](actors/PRIVATE_ACTORS.md#api) are extracted and run by `make check`, including same-thread consumption and
joined results. The onboarding program
[examples/private_tree.d](examples/private_tree.d) uses typed result ports, a
child continuation, a subset wave, and a destructor. Shared setup, drain, and
shutdown rules for both models are in
[actors/LIFECYCLE.md](actors/LIFECYCLE.md). The
[shutdown recipe](actors/LIFECYCLE.md#shutdown-recipe) covers the public
retire/escalation path and the private tracked-root `cancelAll` and
`takeOutcome` drain. Its compiled forms are
[examples/request_deadline.d](examples/request_deadline.d),
[examples/fail_fast_fold.d](examples/fail_fast_fold.d), and
[examples/fiber_cancel_at.d](examples/fiber_cancel_at.d); build commands are in
[examples/README.md](examples/README.md).

## First actor and wave

The `actors` package adds explicit state lifetime and exclusive callback-local
borrowing without changing the worker side: ordinary Farm consumers execute
actor activations and wave tables. An `ActorOwner` controls retirement and
reclamation, while its copyable `ActorHandle` is the identity used to wake,
send to, or phase-dispatch the actor. Contracts that apply to both models are
in [actors/LIFECYCLE.md](actors/LIFECYCLE.md). Start with the compiled programs
[examples/public_service.d](examples/public_service.d) and
[examples/public_wave.d](examples/public_wave.d); build commands are in
[examples/README.md](examples/README.md). The blocks below are fragments. They
assume the `farm`, `token`, and consuming `view` from [First payload](#first-payload);
`drainOrYield()` stands for `view.consumeNext()` or a pool worker's pump.

```d
// fragment: assumes farm, token, and view from First payload
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

A wave dispatches one phase operation across a caller-selected collection of
idle actor handles. Handles passed to one `publish` must match that operation's
state type, and every handle must have been created on the same Farm. They need
not come from the same `ActorRuntime`. The wave may span several physical Farm
tables, but exposes one generation-tagged completion:

```d
// fragment: assumes farm, token, handles, and drainOrYield
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
confirming `!completion.failed` joins the actors' writes and is the boundary
after which an orchestrator may publish a dependent wave. It does not join an
optional completion hook: do not reuse that wave storage or free the hook
context until the hook's last access.
[examples/public_wave.d](examples/public_wave.d) is the compiled form of that
rule. For cross-actor reads, keep private mutable actor
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

The `PayloadHeader*` a callback receives is valid only during the call and
must not be modified or retained: for tables of single-threaded payloads it
points to a temporary consumer-local copy, not to the ring.

The callback receives its body words read-only. That constness protects
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
// fragment: construction arguments only; consumers and producers are omitted
auto farm = AntFarm.create(ringMiB: 32, k: 8, expectedConsumers: consumers,
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
   A token is bound to the Farm that issued it: `write` and
   `unregisterProducer` through any other Farm are fatal. `flush` and `pump`
   catch a foreign token only when they publish something.
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
- [actors/LIFECYCLE.md](actors/LIFECYCLE.md) is the shared actor lifecycle
  and contract guide, including the
  [shutdown recipe](actors/LIFECYCLE.md#shutdown-recipe) for both models.
- [examples/README.md](examples/README.md) has the compiled public and private
  actor programs, including the outcomes, cancellation, and deadline programs.
- [actors/ACTOR_ROADMAP.md](actors/ACTOR_ROADMAP.md) shows autonomous actors,
  inbox ownership, retirement, outcomes, cancellation, and dependent phase
  waves.
- [fibers/README.md](fibers/README.md) shows when and how to run managed Fibers
  on the same workers.
- [SPEC.md](SPEC.md) is the core algorithm and memory-order contract.
- [review_torture/README.md](review_torture/README.md) describes the extended
  concurrency and ThreadSanitizer suite.

Ant Farm is licensed under the Boost Software License 1.0.
