# Performance characterization

For the original checkout versus optimized branch on the existing payload,
Fiber, and actor-wave benchmarks, see [THROUGHPUT.md](THROUGHPUT.md).
These historical measurements predate the mandatory quota sweeps and
unconditional fresh-segment protection checks merged from the host audit.
They are reference results for the revisions named in each report.

## Sustained actor and wave churn

`actor_churn.d` repeatedly creates a full cohort of actors, dispatches each
exactly once, requests retirement, and reclaims every actor before the next
cycle. Actor state is allocated and passed back to its allocator on every
cycle. The Farm, runtime, owner/handle arrays, completion counters, wave
descriptor, and consumer threads persist across cycles, so stable actor slots and wave generations
are reused. There are no Fibers in this benchmark: it isolates the actor
and wave lifecycle rather than Fiber orchestration.

```sh
make -C perftest actor_churn
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn actor 4096 3 0 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn wave 4096 3 0 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn actor 4096 3 6 256 5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn wave 4096 3 6 256 5
```

Arguments are mode (`actor`, `inbox`, or `wave`), actors per cohort, minimum measured
seconds, background consumers, publication batch, warm-up cycles,
and optional allocator (`crt`, `mimalloc`, or `mimalloc64`). Autonomous
publication batches are 1–256; wave publication accepts any positive batch,
with the Farm splitting larger offered slices into physical tables.
An optional final comma-separated CPU list pins the controller followed by
each consumer; omit it or pass `-` to leave placement to the OS.
These examples also show the defaults except that `actor`/`wave` is required.
Zero background consumers means the controlling thread also consumes;
otherwise there is one controlling producer plus the requested number of
native consumer threads, continuously polling. No threads are
created or joined inside the timed loop.

The primary rate, `Mactor_cycles/s`, counts **complete actor lifetimes** in
millions per second, including creation, dispatch, completion waiting,
retirement, allocator reclamation, and verification. In wave mode,
`cycles/s` also gives completed cohort waves per second. Every cycle ends with an exact callback
count/generation check and zero live, ready, or stale actors. The benchmark
also reports aggregate creation, dispatch, and retirement/reclamation/check
times for the measured cycles. Warm-up, runtime setup, and final thread
shutdown are excluded; the final measured cycle always completes, even if
the requested duration has elapsed.

Each actor has 16 bytes of state on x86-64. Its callback increments a private
completion counter and release-publishes the cycle number on a separate
64-byte completion line. There is no contended global callback counter.
Autonomous mode wakes the whole cohort, flushes batches, and waits for all
callbacks. Wave mode starts one wave per cohort, publishes batches, seals
the wave, and waits for aggregate completion and membership release. Both
then retire and reclaim all actors. Inbox mode sends one caller-owned node per
actor instead of waking it. The handler pops and completes the node; the sender
acquires `available` before reusing that same node in the next cycle. This is a minimal-work, cohort-based
allocation/reuse test; it does not model staggered lifetimes, simultaneous
creators/reclaimers, or substantial application work.

The Farm uses an 8 MiB ring, eight segments, one small producer, a
16,384-word quota, and `avgCost=0`. Builds default to LDC `-O2 -release`.
Correctness checks use `enforce` and remain enabled in release builds.

The normal build uses the C-runtime allocator: on Linux, each actor creation
calls `aligned_alloc(64, roundedSize)` and reclamation calls `free`. D `new`
is used only for persistent setup outside the timed loop. The optional
`actor_churn_mimalloc` target links the pinned real mimalloc v3.5.3 library
and defaults to the existing `actors.mimalloc` adapter. It can also select
`crt` at runtime for a comparison within the same executable. See
[MIMALLOC.md](MIMALLOC.md) for commands, alignment controls, and results.
For the subsequent runtime improvements, larger wave offers, and consumer
placement measurements, see [LIFECYCLE.md](LIFECYCLE.md).

### Private destructor churn

`actor_churn dtor` uses non-copyable private state whose destructor increments
its per-actor completion counter and publishes the cycle stamp. Its callback
returns `finish`; every cycle drains the park and verifies exactly one owning
destruction per actor. Moved-from `.init` destructors do nothing. It uses the
same arguments, allocator and default emitter byte-reclamation policy as
`private`:

```sh
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn_mimalloc dtor 4096 3 6 256 5 mimalloc 0,1,2,3,4,5,6
```

### Sustained comparison

Measured on 2026-09-24 on the Ryzen 5 5500 host described below, with LDC
1.43.0 / LLVM 22.1.8, `-O2 -release`, and `ANTFARM_HUGE_PAGES=0`. The same
`actor_churn.d` source was compiled against original checkout `8e988f6` and
optimized runtime `e6ce4d1`. Each value is the median of three serial runs
per revision, alternating revision order. Every run uses five warm-up
cycles, at least three measured seconds, and publication batch 256.

| Actors/cycle | Background consumers | Mode | Original million lifetimes/s | Optimized million lifetimes/s | Speedup |
| ---: | ---: | --- | ---: | ---: | ---: |
| 4,096 | 0 | Autonomous | 0.777 | 9.018 | 11.60× |
| 4,096 | 0 | Wave | 0.811 | 9.809 | 12.10× |
| 4,096 | 6 | Autonomous | 0.611 | 6.366 | 10.43× |
| 4,096 | 6 | Wave | 0.626 | 7.931 | 12.68× |
| 16,384 | 0 | Autonomous | 0.115 | 8.003 | 69.69× |
| 16,384 | 0 | Wave | 0.117 | 8.670 | 73.86× |
| 16,384 | 6 | Autonomous | 0.108 | 7.942 | 73.85× |
| 16,384 | 6 | Wave | 0.109 | 8.243 | 75.75× |

At 16,384 actors on one thread, autonomous-mode phase costs were:

| Phase | Original ns/actor | Optimized ns/actor |
| --- | ---: | ---: |
| Creation | 8,408.86 | 61.94 |
| Wake, publish, execute, await completion | 265.23 | 33.16 |
| Retire, reclaim, verify | 33.69 | 29.63 |

Creation no longer repeats the quadratic slot search on every refill.
The autonomous dispatch phase also benefits from bounded ready snapshots.
Wave mode bypasses that ready backlog for its work; its primary improvement
here is actor creation. These are gains across repeated reuse, rather than
just first-time initialization.

The tiny callback and single creating/reclaiming thread do not benefit from
adding six consumers. Those un-pinned runs were also noisier: optimized
wave throughput at 16,384 actors ranged from 5.974 to 8.317 million lifetimes/s
across the three samples, versus 8.645–8.695 on one thread. The median is not
a latency guarantee or a claim about more substantial parallel actor work.

All 48 timed comparison runs passed the per-cycle checks. Additional smoke
runs passed with LDC debug, LDC release, and DMD release builds, including
single-actor batches, non-divisible 257-actor cohorts, and six consumers.

## Actor ready backlog

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

## Actor creation and Fiber lifecycle drains

`scaling.d` isolates two bookkeeping costs without changing the DRuntime Fiber
backend or its exception, suspension, and cleanup behavior:

```sh
make -C perftest scaling
ANTFARM_HUGE_PAGES=0 ./perftest/scaling create 32768
ANTFARM_HUGE_PAGES=0 ./perftest/scaling lifecycle 16384 32 handle
ANTFARM_HUGE_PAGES=0 ./perftest/scaling lifecycle 16384 32 take
```

The creation mode times actor slot reservation and C-runtime state allocation
for an initially empty runtime. Farm/runtime setup, owner-array allocation,
and retirement/reclamation are outside the timed interval. Lifecycle modes
precreate Fibers with 4 KiB stacks, then time only draining their admitted
records. `handle` invokes a counting delegate; `take` also allocates the arrays
returned by `takeLifecycleEvents`. Fiber creation, execution, terminal-record
draining, and recycling are outside that interval.

The following results were collected on the same host and compiler described
above, with ordinary pages and LDC `-O2 -release`. Both binaries use the same
`scaling.d` source; the baseline is `1615d31`. Values are median milliseconds
from three alternating runs of each binary. Lifecycle batch size is 32.

| Operation | Count | Before ms | After ms | Speedup |
| --- | ---: | ---: | ---: | ---: |
| Actor creation | 8,192 | 25.677 | 0.599 | 42.87× |
| Actor creation | 16,384 | 140.206 | 1.202 | 116.64× |
| Actor creation | 32,768 | 581.081 | 2.489 | 233.46× |
| Lifecycle handler | 4,096 | 1.116 | 0.251 | 4.45× |
| Lifecycle handler | 8,192 | 4.907 | 0.505 | 9.72× |
| Lifecycle handler | 16,384 | 20.200 | 1.047 | 19.29× |
| Lifecycle array drain | 16,384 | 20.332 | 1.452 | 14.00× |

Actor creation previously restarted its vacant-slot search at zero, making
initial population quadratic. Slots now come from an intrusive free list:
slot bookkeeping is constant work per actor, with a short gate protecting
concurrent list access. Allocator calls run outside that gate. Slot size stays
128 bytes; the link occupies former padding. Allocation failures and foreign-
thread reclamation return slots through the same list.

Lifecycle drains previously walked the entire remaining list to obtain its
length on every batch. The queue already counted its nodes while reversing
the detached stack; that count now travels with the retained suffix. Draining
`N` events in batches of `B` changes from `O(N²/B)` traversal to `O(N)` total
traversal. The first detach still reverses the whole batch once. Handler
failure retries retain the remaining count and assigned sequence numbers.

Focused `perf` samples before the changes attributed 99.65% of user cycles to
actor creation's search path at 32,768 actors, and 93.22% to lifecycle handling
at 16,384 events with batch size 1. These probes characterize those operations,
not overall application performance or concurrent allocator scalability.

## Tail latency harness

Build with `make -C perftest`. For a standalone tail build from the
repository root (use `-of=tail` instead of `-of=tail.exe` outside Windows):

```text
ldc2 -O2 -release -i -Ithreadpool/source -Iperftest perftest/tail.d antfarm.d antfarm_allocation.d -of=tail.exe
```

`tail` discovers topology through the repository's threadpool module. It assigns
one logical processor per physical core first, preferring faster efficiency
classes, then uses SMT siblings. The producer and optional mailbox poller use
the next entries. It prints every group:LP assignment and warns when the
requested layout reuses logical processors. `--no-pin` leaves placement to the
OS. An eight-consumer run is not inherently oversubscribed on every host.

Mid-drain, mailbox and burst runs discard a missed backlog window before
publishing the timed event. Those attempts increment `miss`, but do not enter
the latency histogram or consume its warmup count. A workload which consistently
misses the window may time out instead of reporting idle samples as backlog
latency. This does not guarantee that the backlog remains nonempty throughout
the subsequent timestamp and publication operations.

Historical [`results/latency.txt`](results/latency.txt) results predate these placement and sample-accounting
fixes. They must not be relabeled as measurements from the corrected harness.
Raw throughput uses a synthetic callback with batched counting, not useful
payload processing. For Windows huge-page comparisons set `ANTFARM_HUGE_PAGES=1`
and pass `--huge` from an account with the existing large-page privilege.

## Private actor lifecycle experiment

`actor_churn private` measures fresh allocation, one private root dispatch,
terminal dispatch, and emitter-side free/recycling. `private-remote` instead
frees root state on consumers as a diagnostic allocation-locality control.
`actor_churn tree` creates private parents, each of which creates up to `batch`
children inside its own dispatch and authorizes their terminal wave. The
reported rate counts children; parent and cohort creation/free are included
in elapsed time and reported separately as extra parents per cycle. This
mode distributes creation across workers and changes allocation locality.

See [PRIVATE_LIFECYCLE.md](PRIVATE_LIFECYCLE.md) for paired measurements and
[the ownership contract](../actors/PRIVATE_ACTORS.md). All modes can run
in one binary with the same Farm and allocator. For example:

```sh
make -C perftest actor_churn_mimalloc
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn_mimalloc private 16384 2 5 256 5 mimalloc 0,1,2,3,4,5
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn_mimalloc tree 16384 2 5 256 5 mimalloc 0,1,2,3,4,5
python3 perftest/private_lifecycle_matrix.py --binary perftest/actor_churn_mimalloc --output /tmp/private-matrix
```

The recorded September 25 matrix used mimalloc 3.5.0. New builds and the
matrix script default to the current 3.5.3 pin; `--mimalloc-version 30500`
checks a deliberately linked historical build. The script assumes this
host's 0..11 CPU layout; adapt placement when changing hosts.

The private modes accept any positive batch size that fits allocation sizing.
Optional final arguments select the Farm producer tier (`small` or `bulk`) and
its quota in ulongs (default `small 16384`, preserving the historical setup).
A final `subset-percent` (subset modes only) or `root-hint` argument follows.
`root-hint` is `auto` (default) or a fixed class `0..5` published for every
root table. Every private-family mode prints
`root_hint=… root_cost_hint=… root_hint_table=… root_hint_claims=…` for its
nominal first root table, where `root_cost_hint` is the derived class under
`auto` or the pinned class otherwise.

### Batch size versus work-cost hints

The root batch limit and the root table's work-cost hint interact. Consumers
claim a table in runs of `MAX_CHUNK >> hint` entries (32 at class 0), and a
table below the small-table threshold is claimed wholesale by shard 0, so a
root table of at most 32 entries published at class 0 runs on **one** worker
no matter how many consumers exist. A large batch limit shrinks the number of
parents a tree mode needs (`parents = ceil(actors / batch)`), so batch 1024
with 16,384 actors publishes 16-entry root tables whose parent callbacks —
each allocating a 1,024-child wave — all landed on one worker: tree/6 fell to
about 18 Mleaf/s. `pump` therefore derives its default hint per table now
(`rootCostHintFor`, see
[PRIVATE_ACTORS.md](../actors/PRIVATE_ACTORS.md#work-cost-hints)): only
wholesale (sub-threshold) root tables are reclassed, so a 16-entry root table
at six consumers derives class 4 (runs of two), which restored about 83
Mleaf/s in the G4 gate (see `results/perf-g4-*`). Tables at least the small
table threshold wide keep class 0; reclassing those, too, measured +31% on
tree/6 at the default batch but also shifted tree-cancel's dispatched work
fraction, so it stays out of the default.

Guidance: with the default derived hint, choose the batch for table overhead
and latency as before; the derived hint removes the small-table single-claim
pathology automatically. Pass an explicit `root-hint` only to pin scheduling
behavior for comparability — `root-hint=0` reproduces the pre-G4 fixed class
0 baseline, and the gate accepts `--root-hint` to run fixed-hint cells.
Larger batches still concentrate parent allocation ownership on fewer
callbacks; if parent callbacks are expensive, prefer more parents (smaller
batch) over hint tuning.
`private_lifecycle_matrix.py --suite batch` sweeps root batch limit × quota ×
consumer count; `--suite child` checks inherited child limits. Both use five
warmup cycles, five repetitions and three-second measurement windows by default.

## Optimization PR performance gate

`perf_gate.py` is the standard gate for optimization PRs. The **−3% boundary
is restored now that the host is quiet**. Inconclusive is never pass. Run
correctness checks first, then run timing serially on a quiet host, with no
other heavy jobs. This Linux gate requires LDC, Python 3, and the real pinned
mimalloc 3.5.3 archive. It builds committed refs in temporary detached
worktrees and removes them after building.

```sh
make perf-gate BASE=BASE_SHA CANDIDATE=CANDIDATE_SHA \
  PERF_GATE_OUTPUT=perftest/results/optimization-001
```

The target is separate from `make check`. `PERF_GATE_ARGS` passes additional
options, including `--archive /path/libmimalloc.a`. The archive defaults to
`actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a` in the selected repository;
`--repo` defaults to this checkout. Output directories must be new.

Defaults cover every `actor_churn` mode at 0 and 6 consumers, with subset
selections of 1%, 10%, and 100%, 16,384 actors, batch/quota 256/16,384, five
warmups, three-second windows and five clean matched pairs per cell. Both
revisions must use identical benchmark source; use `--benchmark-ref REF` to
select a common source when needed. Unsupported modes are incomplete.

Near the boundary, collect at least 20 process pairs and a byte-identical
control. `--boundary` defaults to 20 pairs and rejects fewer; its default
attempt limit is twice the pair count. `--control` independently builds the
base twice, sets candidate to base, and requires identical binary hashes.
The Make target also accepts control mode without `CANDIDATE`:
`make perf-gate BASE=CANDIDATE_SHA PERF_GATE_ARGS=--control`.
Both options can be combined. `--root-hint N` passes a fixed actor_churn
root work-cost class to both builds (the default `auto` appends nothing and
uses each runtime's derived default). For example:

```sh
python3 perftest/perf_gate.py --base BASE_SHA --candidate CANDIDATE_SHA \
  --boundary --modes tree-wait --consumers 0 --output /tmp/boundary-001
python3 perftest/perf_gate.py --base CANDIDATE_SHA --control \
  --output /tmp/control-001
```

Exit codes are **0 pass, 1 regression, 2 inconclusive/error**. A known
regression takes precedence over incomplete cells. Decisions use clean
paired ratios and a descriptive bootstrap median interval. At the default
five pairs, its bounds effectively equal the minimum/maximum paired ratios,
so this acts as a conservative all-pairs threshold screen. In boundary mode
(>=20 pairs), the decision tests the paired-median interval: individual pairs
can fall below −3% while the cell still passes if the interval's lower bound
is above −3%. The same interval decision applies to other repeat counts.
Neither mode provides a calibrated 95% confidence guarantee.

The gate alternates accepted-pair order and discards a whole pair if either
run has outside-affinity mean CPU use above 10%, any outside CPU above 25%,
or system busy time minus benchmark CPU above 0.25 core. Initial load is
checked before builds. CPU placement, SMT sharing, frequency samples,
governor availability, source/binary/archive hashes and build commands are
recorded. `tree-cancel` additionally requires valid dispatched fractions and
no median fraction change beyond the default 0.01 absolute tolerance;
unequal cancellation work is inconclusive.

Keep `gate.json`, `summary.json`, `metadata.json`, build logs and raw
`runs.jsonl` evidence with the PR results. Quote `paired_median_delta` and
its spread, or the `quiet_*` fields: legacy mean/delta fields include
contaminated attempts and cannot establish a pass.

Gate decision and option tests run without builds or timing:
`python3 -B -m unittest perftest/test_perf_gate.py`.

### Known limitations

The [2026-10-06 identical controls](results/perf-g1-2026-10-06/control-analysis.md)
showed one-sided shifts even at 20 balanced pairs: dtor/0 −1.40% and
subset1/0 +0.85%, both inside the −3% boundary. CPU contamination rules also
retained large throughput outliers. A passing screen does not establish an
unbiased effect estimate; keep matched controls and inspect order and drift
before attributing small gains or losses to code.
