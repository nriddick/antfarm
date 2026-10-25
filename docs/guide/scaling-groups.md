# Scaling across groups

What a last-level-cache group costs to cross, the host loop measured against
a hand-written consumer loop, and the tuning for cheap work on parked
workers. Split out of [the hosting guide](hosting.md) so that running a host
and scaling one are separate reads. The host primitive for several groups
is `GroupHost` ([design](../design/threadpool.md#group-host)).

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
5. **Size a channel Farm for its traffic.** The ring floor is 1 MiB
   (`MIN_RING_MIB`), double-mapped, plus a 15 KiB Farm structure, and every
   page of the ring is resident after one lap, so a channel costs about
   1 MiB of memory however little it carries. Its cache footprint is only the
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

`GroupHost` (`import threadpool.groups;`) packages this: `HostGroups.all`
and `farmPerGroup` give one Farm per accepted group with that group's
workers on it, `farmOf(llc)` and `token(llc)` route a control thread's
publications group by group, and `close` tears everything down in order.
Its worker loop is composed from attachments: the fork-join dispatcher is a
raw `@nogc` one, and `ant_fibers.groups.FiberGroups` attaches a Fiber domain
with one lane per group Farm ([design](../design/threadpool.md#group-host)).
`FarmHost` with workers runs on it: one shared Farm by default, or with
`FarmHostOptions.farmPerGroup` one Farm per group, where every runtime and
park is created on a group (`createRuntime(capacity, llc)`,
`createPark!(...)(capacity, options, llc)`; the default is the caller's)
and runs on that group's workers only. The host emits each through its
Farm's control token, `tokenOf(llc)` publishes raw tables into a chosen
group, and `farm`, `token` and the caller's view stay the caller group's.
Which actors go to which group is the application's decision: keep a
runtime's actors and the data they share on one group, and split work
across groups by runtime rather than by actor.

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
on each workload twice: with `ANTFARM_CHURN_HOST=1`, where a `FarmHost` runs
the Farm's pinned consumers and holds the control token, and without it, on the
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

