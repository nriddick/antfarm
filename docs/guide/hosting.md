# Run actors on a FarmHost

`FarmHost` (`import threadpool.host;`) owns what every actor program needs
around its actors:
- one Farm sized for its participants;
- the calling thread's producer `Token`;
- pinned pool workers that consume the Farm;
- the runtimes and parks it creates;
- a progress loop with an opt-in deadline;
- the documented shutdown order.

A `std.concurrency` port needs three lines of setup and a teardown guard.
The module lives in the threadpool package, which depends on `antfarm`.
`import threadpool;` does not re-export it.

## Setup

```d
import antfarm.actors;
import threadpool.host;
import std.stdio : writeln;

struct Counter { long total; }

void add(scope ref ActorBorrow!Counter actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    for (auto node = context.popInbox(); node !is null; node = context.popInbox())
    {
        actor.value.total += *node.payloadAs!long;
        node.complete();
    }
}

void main()
{
    auto host = new FarmHost(2);    // two pinned workers in one LLC
    scope (exit) host.closeOrAbandon();

    auto runtime = host.createRuntime(1);
    auto owner = runtime.createActor!(Counter, add)(Counter(0));
    long value = 5;
    ActorInboxNode node;
    node.initialize(&value, 0);
    owner.handle.send(&node);
    host.waitUntil(() => node.available); // "receive"
    host.retire(owner);                   // "join"
    writeln(host.summary());
}
```

Build it with `-Ithreadpool/source` beside `-I.`, as every program in
[examples/](../../examples/README.md) does.

| std.concurrency | FarmHost |
| --- | --- |
| `spawn` | `host.createRuntime` (or `createPark`), then `createActor` |
| `send` | `handle.send(&node)` with a caller-owned `ActorInboxNode` |
| `receive` | `host.waitUntil(() => node.available)` |
| join, thread exit | `host.retire(owners...)`, then `close()` |

Use `scope (exit) host.closeOrAbandon();`.
The exit guard always tries ordered teardown. If `close()` throws,
`closeOrAbandon()` stops the consumers with `abandon()` before the error
propagates. This also covers normal return with undrained work. Pool workers
are non-daemon threads, so a host with workers that is neither closed nor
abandoned hangs process exit.

## Placement

`FarmHost.resolve(options)` returns the worker selection that the
constructor will start, as a `PoolResolution`: the workers are accepted,
every other CPU that passed the `options.pool` filters is
`Reject.notSelected`, and each physical group reports its `candidates` beside
its `accepted` count. It changes nothing, but it does warm topology
discovery (which freezes the OS scope), including when it throws
`FarmHostResolveException`. Call it once before concurrent construction or
resolve calls: that first discovery is an unlocked setup operation. On Linux
a racing first call is benign; on Windows it can spuriously refuse the
caller with `outsideOsScope`.

A live host reports the same selection, now with every worker's pin outcome,
as `host.resolution` (the pool's own `pool.resolution`), and the caller's
confine or pin set as `host.callerCpus`. `host.summary()` prints workers,
caller placement, Farm participants and sizing in one line, and
`host.toString` adds every compromise note and the resolution summary.
The defaults are:

- **One physical group.** Workers come from the accepted LLC with the most
  accepted CPUs. The host never spills into another group: asking for more
  workers than the group has is a `FarmHostResolveException` that names `span`.
- **One worker per core.** `skipSmtSiblings` is on in
  `defaultHostPoolOptions`.
- **Caller confined** (`CallerPlacement.confine`). The constructing thread's
  affinity is restricted to the home group's CPUs that no worker owns, SMT
  siblings included. With `autoWorkers`, the group's last accepted CPU is
  left free for it. `close()` and `abandon()` restore the previous affinity.
  Threads the caller creates meanwhile inherit the confined mask.
  `CallerPlacement.pin` uses one CPU instead. `CallerPlacement.unchanged`
  leaves the caller alone and notes it as an external participant.
- **`workers == 0`** starts no pool. The calling thread is the only consumer.

`HostGroups.only(g)` pins the choice to one group.

## Several physical groups

A last-level-cache group (a CCD on this host) is the unit of structure. What
crossing one costs is not the payload bytes but every cache line that two
groups write in turn: the table header a producer writes and the first
claimer RFOs, the claim word that every chunk claim CASes, the completion
counters, and the ring lines themselves when consumers read them before the
producer's group has finished with them. On the Ryzen 9 5900XT a line moves
between groups in about 87 ns against 21 ns inside one, so a shared line that
ping-pongs costs four times what it costs locally, and a line that crosses
once costs one such transfer per 64 bytes.

Measured on October 21, 2026, with the off-CCD harness
(`tools/perf/offccd`, one small producer, five consumers, one-word payloads,
streaming for one second), in millions of payloads per second:

| Work per payload | Batch | All in one group | Producer in one group, consumers in the other | Consumers split 3 + 2 |
| --- | ---: | ---: | ---: | ---: |
| touch | 16 | 360 | 83 | 125 |
| touch | 64 | 425 | 213 | 187 |
| touch | 256 | 888 | 537 | 476 |
| touch | 1,024 | 987 | 708 | 694 |
| touch | 1,700 | 996 | 731 | 751 |
| 100 ns | 16 | 42.7 | 42.0 | 38.8 |
| 100 ns | 256 | 46.3 | 44.4 | 43.1 |
| 100 ns | 1,700 | 47.2 | 45.3 | 44.7 |
| 1 µs | 16 | 4.8 | 4.7 | 4.7 |
| 1 µs | 1,700 | 5.0 | 4.8 | 4.9 |

Two things follow. **Table size amortizes the control lines**: with trivial
payloads, going from 16 to 256 entries per table takes the one-way channel
from 23% to 60% of local, and beyond 1,024 entries it gains nothing more.
**Work per payload hides the data lines**: at 100 ns per payload the
cross-group penalty is 2 to 4% at every batch size, and at 1 µs it is within
the noise. A trivial callback (a few nanoseconds per word) is latency-bound
on the line transfers however the tables are shaped, which is also why
[the headline](../../PERFORMANCE.md#headline-throughput) drops from 517 to
207 million child lifecycles per second when the first consumer lands on the
second CCD.

The guidance:

1. **Every Farm's consumers live in one group.** Consumers in two groups
   contend on the same claim and completion lines; that is the cliff, and it
   is paid by the first few remote consumers. The host's default already does
   this. A hand-written pool does it with one Farm per `llcIndex` bin
   (`makeBins`, `at!T(llc)` in the [threadpool](../../threadpool/README.md#install-llc-local-state)).
2. **Publish across groups only when the tables are large and the work is
   real.** Keep cross-group tables at 256 entries or more, and expect to lose
   little only when each payload carries about 100 ns of work or more
   (roughly 10 ns per word moved). Below that, keep the work on the
   producer's own group; adding remote consumers to a stream of trivial
   payloads slows it.
3. **A Farm is a channel.** To use a second group, give it its own Farm with
   all of its consumers, and let the control thread publish into it one-way:
   locally into its home group's Farm, remotely into the other group's. For
   private actors that is one root park per Farm with its own token, pumped
   in turn; child cohorts inherit their root's Farm. For public actors it is
   one runtime per Farm; handles and waves are runtime-bound, so a service
   is partitioned by group. When one control thread saturates, run one per
   group: two Farm, park and control sets with the work split at the top and
   only the application's own data crossing.
4. **Tokens are cheap to hold; quota is budgeted at creation.** A `Token` is
   32 bytes and a 64-byte-strided ticket slot in its Farm. Registration takes
   the Farm's producer mutex and scans the tier's slots, microseconds, so
   registering for a burst and unregistering after is fine, but there is no
   reason not to hold one per publishing thread per Farm. What a token really
   costs is the tier's quota: `maxBulk * quotaBulk + maxSmall * quotaSmall`
   must fit `k - 1` segments, and the Farm reserves that headroom whether or
   not the slots are in use. A thread that publishes occasionally into
   several groups' Farms can share a few tokens through a mutex, as long as
   each token has one active user at a time; tokens are move-only and never
   used concurrently.
5. **Size a channel Farm for its traffic.** The ring floor is 2 MiB
   (`MIN_RING_MIB`), double-mapped, plus a 15 KiB Farm structure, and every
   page of the ring is resident after one lap, so a channel costs about
   2 MiB of memory however little it carries. Its cache footprint is only the
   live segment, not the ring. Give a remote channel a larger ring than a
   local one when its producer is busy, so reclamation sweeps, which cross
   the interconnect, happen less often; give it one or two small producer
   slots and the quota they need, not the defaults. A smaller ring floor
   would be a geometry study (the 2,048-word segment floor keeps table
   headers from dominating a segment), not a design change.

**When you know better.** `HostGroups.span([home, other...])` consumes one
Farm from several groups anyway; `host.summary()` labels it `crossGroup` and
counts the other groups' workers as foreign consumers. It is the
configuration the table above measures and advises against, kept for work
heavy enough per payload (about 1 µs) that the control lines do not matter.

`GroupHost` (`import threadpool.groups;`) packages this for programs with a
raw worker body: `HostGroups.all` and `farmPerGroup` give one Farm per
accepted group with that group's workers on it, `farmOf(llc)` and
`token(llc)` route a control thread's publications group by group, and
`close` tears everything down in order. The fork-join dispatcher runs on it
([design](../design/threadpool.md#group-host)). `FarmHost` still owns one
Farm on one group for actor programs; routing its runtimes and parks by
group is the remaining step.

## Progress and waking

Work advances only while the control thread (the one that constructed the
host) is inside `step` or `waitUntil`, or while a holder of an extra token from
`registerProducer` flushes. The host has no background emitter. Actor callbacks
and other threads may `send` and `wake`, but someone still has to flush or
pump.

`step()` does the following, in order:
1. Flushes every host runtime and pumps every host park and attachment, in
   rounds. It stops after a round publishes nothing, or after
   `FarmHostOptions.maxEmitRounds` rounds (default **16**, must be positive).
2. Wakes parked workers, even if nothing was published, to request consumption
   and reclamation progress.
3. Consumes once on the caller's view, or yields when there is no caller view.

`waitUntil(done)` runs `done()` before every step, so the predicate is the
place to drain `takeOutcome`. Neither call ever infers idle from a zero flush,
pump or `consumeNext`.

Each round visits every emitter in registration order (host-owned emitters,
then attachments), including emitters that returned zero in an earlier round.
This batches publications before a wake without draining one busy emitter
indefinitely. The bound counts rounds, not tables or elapsed time: a park may
publish several pending child tables per pump and a runtime flushes up to four
times per visit. Attachments must return promptly, including on backpressure,
and must report the number of items they actually published. An attachment may
be called several times in one step. Set `maxEmitRounds = 1` for the previous
single-round behavior or when the control thread must check outcomes and
deadlines more frequently. `flush`, `pump` and `publish` retain their individual
publication/wake behavior.

Idle workers park by default (`WorkerIdle.park`, `idleTimeout = 1.msecs`).
The host's own wrappers wake them as soon as they publish: `step`, `flush`,
`pump`, `publish!op` and park pumps, including child-only publications.
Publications through the raw token (`runtime.flush(host.token)`) skip the
wake and are picked up within `idleTimeout`, or call `host.wake()`.
`idleTimeout = Duration.max` parks until woken, so raw-token users must then
call `wake()`. `WorkerIdle.spin` never parks and burns every worker's CPU
for the host's life. `wake()` may be called from any thread while the host is
open. It may overlap `close()`, `abandon()` or a worker-failure shutdown:
teardown stores a non-open state and waits for in-flight wakes before
stopping the pool.

A worker can park while another worker still runs a callback in an epoch it
holds. Completing that callback does not itself wake the parked worker to
release its reference. A full ring can therefore refuse publication even
after all callbacks finish. Keep calling `step()` on publication backpressure:
each step requests another worker poll, including when no emitter publishes.
One wake or a fixed number of polls before the peer finishes is insufficient.
Raw `host.farm.write` loops must wake after accepted writes and call `step()`
before retrying refused writes. `step()` respects `callerConsumes = false`;
it does not run callbacks on that caller. This progress guarantee requires the
control thread to keep stepping and callbacks eventually to return.

## Deadlines and failures

`FarmHostOptions.waitTimeout` (default `Duration.max`: none) starts a fresh
deadline on each `waitUntil`. When it expires, `FarmHostTimeout` is thrown and
the host stays open with its consumers running. Escalate, for example with
`requestRetire(ActorRetireMode.cancel)` or `park.cancelAll()`, and wait again,
or `abandon()`. A worker failure, such as an `assert` in a callback in a debug
build, surfaces as `FarmHostFailed` from the next `step` or `waitUntil`. The
host is then `failed`, and only `close()` (which acts as `abandon()`) remains.
After `FarmHostFailed`, the caller stays confined until `close()` or
`abandon()` restores affinity.

`FarmHostFailed.workerFailures` contains owned diagnostic snapshots, so an
assertion's thread-local error object is never used after its worker exits.
The Error/Exception distinction is preserved; the original subclass, chain
and stack trace are not. See the [worker failure capture contract](../design/threadpool.md#managed-worker-hooks)
for message/file limits and `sourceType`/`truncated` fields.

## Teardown

`close()` first checks, with no side effects:
- every runtime is `quiescent`;
- every park has `live == 0`;
- every attachment is drained;
- no extra producer is outstanding.

If any check fails, it throws and the host stays usable: drain and call it
again. Otherwise it performs the canonical
[shutdown order](../reference/actor-lifecycle.md#shutdown-order), finishing
attachments and restoring caller affinity as part of teardown.

`abandon()` stops the consumers, restores affinity and leaks everything that
did not drain. After `abandon()` or a failed host, call `leak()` on any
`ActorOwner` you cannot reclaim. Otherwise an assertion build prints a
"dropped without reclaim" diagnostic when the owner goes out of scope,
including while `FarmHostFailed` unwinds.

Join every thread that may call `requestCancel` on a host park before
`close()`. The host cannot see those threads. Views subscribed outside the
host (`FarmSizing.extraConsumers`) must unsubscribe first as well.

While it has workers, a host owns the process's single `CacheAwarePool`. A
second host with workers, or a host started beside an application-started
pool, is rejected with a `FarmHostException`, and the running one is left
alone. Hosts with `workers == 0` start no pool and can coexist.

A caller-driven `FiberDomain` joins the same host with
`ant_fibers.host.attachFiberDomain(host, new FiberDomain(host.farm))`.
[examples/fiber_cancel_at.d](../../examples/fiber_cancel_at.d) shows it.

## Performance and tuning

### Cheap work with parked workers

A large private cohort inherits a batch limit of 256 unless its parent was
configured otherwise. Publishing a small table and waking every worker can
cost more than its callbacks, especially with many workers. The default
16-round step burst amortizes these notifications across several tables; it
still wakes on an empty round so a full ring can reclaim storage. Workers
retain the selected idle policy, including indefinite parks, and
`callerConsumes = false` still keeps callbacks off the control thread.

For a large, uniform child wave, also measure an explicit child batch limit
such as `context.createChildren!Cell(population, 4096)`. That reduces table
overhead as well as wake frequency without changing the root batch limit.
The Farm quota can split it into smaller accepted prefixes. Larger root
batches have a different trade-off: a slow root holds the other roots in its
physical table until it returns. See [batch and quota choices](sizing-and-allocation.md#configuring-batches-and-farm-quota).

Measure end-to-end completion and small-request latency, with the same work,
worker placement, idle policy and verification. Use release builds and serial
runs on a quiet host. `WorkerIdle.spin` can help sustained work on dedicated
cores, but burns CPU while idle; it is an explicit application choice. A shorter
finite park timeout is not a substitute for efficient publication batches.

For a slow run, log application checkpoints as well as the deadline. Steady
progress up to a watchdog is evidence to check the workload's normal completion
time; it is not proof of a deadlock. Confirm under a bounded, realistic deadline
with all correctness checks enabled. Do not retry timed-out correctness runs
until one passes, or silently change the work or idle policy.

### Host loop versus hand-written consumers

A host's consumers run on the threadpool's worker loop, not a loop you
wrote. On one measured host, that cost was invisible except where consumers
were the bottleneck and often found nothing.

The comparison ran [tools/perf/actor_churn](../../tools/perf/README.md#actor_churn)
on each workload twice: with `ANTFARM_CHURN_HOST=1`, where a `FarmHost` owns
the Farm, the control token and the pinned consumers, and without it, on the
benchmark's own hand-written consumers. The controller and every consumer CPU
sat in one LLC of a Ryzen 9 5900XT; each configuration was measured as twenty
paired runs and reported as throughput change (negative is slower than the
hand-written loop) with a bootstrap confidence interval. These numbers are
from one host, not a universal constant; re-measure on yours.

The differences for `actor` and `wave` at 2 and 6 workers, and for `private`
and `tree` at 2 workers, were within the comparison boundary or in
`FarmHost`'s favor. The fine-grained private modes at 6 workers were slower:

| Mode | Throughput change | Bootstrap CI |
| --- | --- | --- |
| `private` | -7.8% | [-8.1, -7.6] |
| `tree` | -7.1% | [-8.0, -5.9] |

The cause is the idle path. A hand-written consumer that finds nothing does
one `pause()` and retries. A pool worker that finds nothing arms its wait slot
with an atomic exchange (`prepareWorkerWait`), consumes a second time, and
only then applies the idle policy: under `WorkerIdle.spin` — the policy the
benchmark selects, matching the hand-written loop's always-polling behavior —
it spins `PoolOptions.pSpinIters` times (128 by default on performance-class
workers), each iteration a `pause` plus an `atomicFetchAdd` on a
process-wide spin counter, before the loop starts over. The exchange, the
second consume and the spin bookkeeping cost throughput only where consumers
are the bottleneck and miss often: many workers on private- and tree-style
fine-grained workloads.

Re-measured on October 24, 2026 on the 5900XT at the 2.0.0-dev tree, same
command (`private`, 4,096 actors, 6 consumers, batch 256, three rounds of
3 s): the host ran 14.5 to 14.8 million actor cycles per second against the
hand-written loop's 14.0 to 14.1 before the change, and 14.9 against 14.0
after the spin counters moved behind `version (ThreadpoolStats)`. The
-7.8% above did not reproduce on this tree; the host was 4% ahead both
times, and removing the per-pause atomics added about 1.5% to the host
path, within one round's spread. The paragraph above is kept as the
record of what was measured when; the cost it names is now compiled out by
default.

What to tune:

- **Fewer workers** when consumers are the bottleneck: fewer consumers polling
  the same Farm miss less often. `actor` and `wave` were unaffected at the
  same worker counts, so size by measurement, not by core count.
- **The idle policy**: `FarmHostOptions.idle` selects `WorkerIdle.park` (the
  default: after its final empty check the worker parks at once, for up to
  `FarmHostOptions.idleTimeout`, 1 ms, or until the host's publication
  wrappers wake it) or `WorkerIdle.spin`; the spin count is
  `FarmHostOptions.pool.pSpinIters` (and `eSpinIters` on efficiency-class
  workers). The measurement above used `spin` with the default counts; `park`
  avoids the spin burn at the cost of wake latency. The publication wrappers
  wake a parked worker at once, so their latency stays small; a raw-token
  publication that no wrapper announces waits up to `idleTimeout` for the
  worker to recheck. `park` was not measured here.
- **A hand-written consumer loop** on the raw Farm, for maximum throughput on
  consumer-bound fine-grained workloads, as the benchmark's non-host path
  does: subscribe a `ConsumerView`, then loop
  `if (!view.consumeNext()) pause();`. You then own the pinning, the producer
  token and the teardown that the host otherwise provides.

To measure on your own machine, run the same command with and without the
variable (`ANTFARM_CHURN_HOST=1` needs the small tier, `consumers > 0` and a
CPU list naming the controller followed by each consumer);
[tools/perf/README.md](../../tools/perf/README.md#actor_churn) has the full
argument list.

## Parallel regions

Blocking loops run on the fork-join dispatcher (`forkJoinFor`,
`forkJoinReduce` in [forkjoin](../../forkjoin/README.md#loops-and-reductions)),
whose join never blocks a worker. For an asynchronous Farm region on the
host's Farm, pass `host.parallelOptions(options)` to `parallelFor` so every
accepted write wakes parked workers, then drive it with `step()` until the
handle reports completion; keep the host alive through completion. See
[parallel loops](parallel-loops.md) for ranges and grain sizing.
