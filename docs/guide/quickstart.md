# Quickstart

You need DMD 2.112+ or LDC 1.42+. From a clone of the repository, build and run
a complete tracked program that distributes work to a public actor service:

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=/tmp/public_service
ANTFARM_HUGE_PAGES=0 /tmp/public_service 0
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
  consumers it wants, here a `FarmHost` with the on-thread consumer or pinned
  pool workers.

Whatever the shape, submitted work only makes progress while some thread
holds a producer `Token` and keeps calling `runtime.flush(token)` (or the
private actor `pump`): `wake` and `send` queue an actor; they do not execute
it. The token is move-only, so hand it to exactly one pumper with `move` or
share it by `ref`.

Completion is the application's to observe. The Farm has no "all done" signal,
`consumeNext() == false` is not completion, and `pump() == 0` is not a drain.
The examples count their own callbacks or watch a platform-specific predicate.
The shared rules are in
[../reference/actor-lifecycle.md](../reference/actor-lifecycle.md).

## Where to go next

- Pick a representation with [choosing-work.md](choosing-work.md): payload,
  public actor, actor wave, private hierarchy, or Fiber.
- Read the public actor guide, [public-actors.md](public-actors.md), or the
  private actor guide, [private-actors.md](private-actors.md).
- The smallest complete payload program, with its compile-and-run command, is
  the [First payload](../../README.md#first-payload) section of the README.
