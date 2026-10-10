# Ant Farm

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish tables
of work into a shared ring; any subscribed consumer may claim them. It is
designed for unordered compute where work can appear on any thread and the next
available worker should take it.

**Status.** The current release candidate is **1.8.0-rc.2**. It is
release-candidate software: the public and private actor models are supported
functionality, but source and ABI stability are not promised across release
candidates, and actor state and results have the limits stated in their guides.
The correctness gate is the local Linux `make check`; there is no CI. Windows
(x64) is an intended target whose validation follows the release candidate. See
[CHANGELOG.md](CHANGELOG.md) for what the candidate contains and its
compatibility notes.

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
-of=/tmp/public_service`). [docs/guide/quickstart.md](docs/guide/quickstart.md)
explains who publishes, who consumes, and what completion means.
[examples/README.md](examples/README.md) has the other programs and the Windows
commands.

## Where to go next

| I want to... | Start with |
| --- | --- |
| **Run something** | [docs/guide/quickstart.md](docs/guide/quickstart.md), then [examples/README.md](examples/README.md) for six actor programs and [threadpool/README.md](threadpool/README.md) for pinned workers. |
| **Choose and use actors** | The [choice table](#choosing-the-work-representation) below, then [docs/guide/choosing-work.md](docs/guide/choosing-work.md), the public actor guide [docs/guide/public-actors.md](docs/guide/public-actors.md) or the private actor guide [docs/guide/private-actors.md](docs/guide/private-actors.md), the shared [lifecycle contract](docs/reference/actor-lifecycle.md), and the matching [example](examples/README.md). |
| **Contribute** | [CONTRIBUTING.md](CONTRIBUTING.md) for `make check` and the change checklist, [docs/development/testing.md](docs/development/testing.md) for the lane matrix, [ARCHITECTURE.md](ARCHITECTURE.md) for the layers, and [SPEC.md](SPEC.md) for the Farm algorithm and memory-order contract. |
| **Measure performance** | [PERFORMANCE.md](PERFORMANCE.md) for the measured numbers, method, and tuning guide, then [perftest/README.md](perftest/README.md) for the harness and `make perf-gate`. |

The full documentation map and the canonical owner of each rule are in
[docs/README.md](docs/README.md).

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
separately. This table is a summary: the full work-representation table and the
public-vs-private comparison are in
[docs/guide/choosing-work.md](docs/guide/choosing-work.md); the layer-level
comparison is in
[ARCHITECTURE.md](ARCHITECTURE.md#choosing-the-work-representation).

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

To run it, save the program as `/tmp/first_payload.d`, then from the repository
root:

```text
dmd -g -i -I. /tmp/first_payload.d -of=/tmp/first_payload
ANTFARM_HUGE_PAGES=0 /tmp/first_payload
```

It prints nothing and exits with status 0 once all 256 jobs have run.

Things the example relies on:

- `AntFarm.create` has nine parameters. Named arguments, as above, keep the call
  readable; omitted ones take their defaults. `ringMiB` is the ring size in MiB
  (a power of two, at least 2); quotas are in ulongs. See
  [Sizing one Farm](ARCHITECTURE.md#sizing-one-farm).
- `write()` returns how many payloads it published. Pop exactly that many and
  retry; `0` is backpressure, not failure. A generic source passed to `write()`
  must be a forward range because the Farm checkpoints it for sizing and
  emission. The write forms are summarized in
  [SPEC.md](SPEC.md); [ARCHITECTURE.md](ARCHITECTURE.md) covers sizing.
- `consumeNext() == false` means nothing is ready at this cursor right now. It
  may be a temporary hole, not global emptiness, and there is no FIFO
  guarantee. Completion is the application's to track, e.g. with a counter
  incremented by the callback.
- `subscribe()` returns the starting epoch (`>= 0`) or a negative code:
  `SUBSCRIBE_RETRY` is transient, so call again; `SUBSCRIBE_FULL` means the Farm
  already has `MAX_CONSUMERS_LIMIT` (128) views; `SUBSCRIBE_INVALID` is a null
  Farm or an already subscribed view.
- The `scope (exit)` blocks above unregister the producer, unsubscribe the
  same-thread consumer, then destroy the Farm. All producers and consumers must
  be quiescent before Farm destruction.

For the multi-threaded version, with one consumer per pinned worker, build and
run [examples/iota_sum.d](examples/iota_sum.d):

```text
dmd -g -i examples/iota_sum.d antfarm.d antfarm_templates.d \
    -Ithreadpool/source -of=iota_sum
ANTFARM_HUGE_PAGES=0 ./iota_sum
```

The shared producer, consumer, completion, and shutdown rules for payloads and
both actor models are in
[docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md).

Ant Farm is licensed under the Boost Software License 1.0.
