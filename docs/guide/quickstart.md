# Quickstart

You need DMD 2.112+ or LDC 1.42+ and a clone of the repository.

## First program

Ten lines on every core: the fork-join dispatcher, a deterministic
reduction. `make -C examples check` extracts this block and runs it with
LDC and DMD in debug and release:

```d
/+ dub.sdl:
    dependency "antfarm:forkjoin" path="."
+/
import threadpool;
import forkjoin;
import std.range : iota;
import std.stdio : writeln;

void main()
{
    Dispatcher d;
    d.start();                 // the caller plus one pinned worker per core
    scope (exit) d.stop();
    writeln("sum of squares: ", forkJoinReduce!((ulong x) => x * x,
        (ulong a, ulong b) => a + b)(iota(1UL, 257UL), 0UL, 16));
}
```

Run it with `dub run --single first.d` from the repository root; it prints
`sum of squares: 5625216`. What it relies on:

- `Dispatcher.start()` with no arguments takes every eligible processor:
  the calling thread is participant 0 and each other processor gets a pinned
  pool worker, with one Farm per last-level-cache group underneath
  ([forkjoin/README.md](../../forkjoin/README.md)). `start(cpus, options)`
  chooses processors and tuning.
- `forkJoinReduce!(fn, combine)(range, seed, grain)` splits the range in
  halves down to `grain` elements; the split points depend only on length
  and grain, so the result is the same for any participant count. `fn` and
  `combine` are `nothrow @nogc` and, as here, may be lambdas.
- `stop()` drains anything still running, parks and joins the workers and
  destroys the Farms. Pool workers are non-daemon threads: a dispatcher that
  is never stopped keeps the process alive.

## What the dispatcher does for you: the raw ring, one thread

This complete program produces and consumes on one thread. Application functions become
payloads through `payloadRange`: the callback must be `nothrow @nogc`, and its
arguments are packed into the ring and reconstructed on a consumer. The
dispatcher above does exactly this for its spilled and submitted roots; its
deques carry everything else. `make -C examples check` runs this block too:

```d
/+ dub.sdl:
    dependency "antfarm" path="."
+/
import antfarm.templates;
import core.atomic : MemoryOrder, atomicFetchAdd, atomicLoad;
import std.range : iota;
import std.stdio : writeln;

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
    if (view.subscribe(farm) < 0) assert(0, "Farm full or view already subscribed");
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
    writeln("first payload: 256 squares summed to ",
        atomicLoad!(MemoryOrder.acq)(squareSum));
}
```

To run it, save the program as `first_payload.d` in the repository root. The
`dub.sdl` header names this clone as its dependency, so Dub builds it in
single-file mode; or call the compiler directly:

```text
dub run --single first_payload.d

dmd -g -i -I. first_payload.d -of=first_payload
./first_payload
```

Add `--compiler=ldc2` to the Dub command, or substitute `ldc2` for `dmd`, to
use LDC.

It exits with status 0 once all 256 jobs have run and prints:

```text
first payload: 256 squares summed to 5625216
```

Things the example relies on:

- `AntFarm.create` has nine parameters. Named arguments, as above, keep the call
  readable; omitted ones take their defaults. `ringMiB` is the ring size in MiB
  (a power of two, at least 2); quotas are in ulongs. See
  [Sizing one Farm](../../ARCHITECTURE.md#sizing-one-farm).
- `write()` returns how many payloads it published. Pop exactly that many and
  retry; `0` is backpressure, not failure. A generic source passed to `write()`
  must be a forward range because the Farm checkpoints it for sizing and
  emission. The write forms are summarized in
  [SPEC.md](../../SPEC.md); [ARCHITECTURE.md](../../ARCHITECTURE.md) covers sizing.
- `consumeNext() == false` means nothing is ready at this cursor right now. It
  may be a temporary hole, not global emptiness, and there is no FIFO
  guarantee. Completion is the application's to track, e.g. with a counter
  incremented by the callback.
- `subscribe()` returns the starting epoch (`>= 0`) or a negative code:
  `SUBSCRIBE_FULL` means the Farm already has `MAX_CONSUMERS_LIMIT` (128)
  views; `SUBSCRIBE_INVALID` is a null Farm or an already subscribed view.
  Neither is transient, so there is nothing to retry.
- The `scope (exit)` blocks above unregister the producer, unsubscribe the
  same-thread consumer, then destroy the Farm. All producers and consumers must
  be quiescent before Farm destruction.

For the multi-threaded version, with one consumer per pinned worker, build and
run [examples/iota_sum.d](../../examples/iota_sum.d):

```text
dmd -g -i examples/iota_sum.d antfarm/package.d antfarm/templates.d \
    -Ithreadpool/source -of=iota_sum
./iota_sum
```

The shared producer, consumer, completion, and shutdown rules for payloads and
both actor models are in
[docs/reference/actor-lifecycle.md](../reference/actor-lifecycle.md).

## What just happened

An `AntFarm` is a fixed-memory work distributor, not a thread pool. Three
things cooperate:

- A **producer** registers a `Token` and publishes payload tables into the
  ring. `write()` returns how many payloads it admitted; `0` is backpressure,
  so pop exactly that many from the source and retry.
- **Consumers** each hold one persistent `ConsumerView` and call
  `consumeNext()`. A `false` result means nothing is ready at this cursor
  right now; it is not global emptiness.
- **Actors** add execution state above the distributor. A public actor owns
  POD state and a caller-owned inbox; a private hierarchy owns a parent-owned
  cohort tree. Neither creates threads: the program supplies whatever pump and
  consumers it wants, here the host's on-thread consumer or pinned pool
  workers.

## Run the public service

The second program adds pinned workers and a public actor service on a
`FarmHost`:

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=/tmp/public_service
/tmp/public_service 0
```

Expected output, with exit status 0:

```text
public service: 36 ordered replies, balance=234, workers=0, closing-callback-count=0
```

The trailing `0` consumes on the calling thread; `/tmp/public_service 2` uses
two pinned pool workers in one LLC and prints `workers=2` with the same counts.
LDC works with the same command (`ldc2 -g -i -I. -Iexamples -Ithreadpool/source
examples/public_service.d -of=/tmp/public_service`). If the program prints
nothing or aborts, check the toolchain first.
[../../examples/README.md](../../examples/README.md) lists the other programs and
the Windows commands.

`main` built a `FarmHost` (`threadpool.host`): one Farm, the calling thread's
producer token, and, for a nonzero argument, pinned pool workers in one LLC.
It created the actor runtime through the host, waited for each reply with
`host.waitUntil`, and
`scope (exit) host.closeOrAbandon();` ran the shutdown order. If `close()`
throws because work is undrained, `closeOrAbandon()` stops the consumers before
the error propagates.

Whatever the shape, submitted work only makes progress while some thread
holds a producer `Token` and keeps calling `runtime.flush(token)` (or the
private actor `pump`): `wake` and `send` queue an actor; they do not execute
it. `host.waitUntil` is that loop for the host's token. The token is
move-only, so hand it to exactly one pumper with `move` or share it by `ref`.

Completion is the application's to observe. The Farm has no "all done" signal,
`consumeNext() == false` is not completion, and `pump() == 0` is not a drain.
The examples count their own callbacks or watch a platform-specific predicate.
The shared rules are in
[../reference/actor-lifecycle.md](../reference/actor-lifecycle.md).

Follow the [onboarding path](../README.md#run-something) for pinned workers,
representation choice, and sizing.
