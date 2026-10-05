# Private actor lifecycle and batching measurements

## E2-1 private destructors — 2026-10-05

All eight POD comparisons pass the −3% median gate against local main
`3fd4182`. Worst regression: **−2.60%** (`private-remote`, six consumers).
Favourable changes above +3% are reported below; they are not failures.

LDC 1.43.0, `-O2 -release`, real mimalloc v3.5.3 from the main checkout's
prebuilt archive (read only). Ordinary 8 MiB Farm, small tier, quota 16,384
words, 4,096 actors, batch 256, five warmup cycles, three-second windows.
Five alternating base/candidate pairs per POD configuration, reversing order
on successive pairs; five candidate-only `dtor` runs. Controller CPU 0,
consumers 1..6 pinned. Tests and soak finished before measurements.
The base executable was compiled from a `git archive 3fd4182`; the candidate
includes the new `dtor` benchmark mode. All per-cycle checks passed.
`tree-results` is absent from this baseline benchmark, so it has no gate row.

| Mode | Consumers | Base median M/s | E2-1 median M/s | Change |
| --- | ---: | ---: | ---: | ---: |
| private | 0 | 36.375 | 38.247 | +5.15% |
| private-remote | 0 | 37.011 | 38.864 | +5.01% |
| tree | 0 | 53.901 | 53.812 | -0.17% |
| tree-wait | 0 | 54.496 | 53.994 | -0.92% |
| dtor | 0 | — | 38.144 | new mode |
| private | 6 | 26.353 | 26.471 | +0.45% |
| private-remote | 6 | 12.962 | 12.624 | -2.60% |
| tree | 6 | 19.790 | 19.588 | -1.02% |
| tree-wait | 6 | 19.535 | 19.396 | -0.71% |
| dtor | 6 | — | 26.903 | new mode |

The +5.15% / +5.01% single-consumer gains retain the same number of POD actor
allocations, callbacks and frees; the results do not establish a universal
speedup. `dtor` moves a non-copyable state and performs the completion counter
and cycle-stamp writes in `~T`, rather than the user callback. It measures
stored-state destruction with emitter byte reclamation; moved-from `.init`
destruction is a no-op. Its medians are 38.14 / 26.90 million lifetimes/s.

[Raw runs, JSON medians, builds and verification](results/private-destructors-2026-10-05/)
include compiler versions, binary/library hashes and six isolated mutation diffs.

## Phase E1 root-wait gate — 2026-10-04

Gate: no regression worse than −3% versus base median; favourable deviations
above +3% are reported explicitly with a work-equivalence check. The unused-feature
gate passes against **c5a174f**. The worst measured regression is **−1.61%**.
The common child Entry remains 40 bytes, with LDC/DMD size and offset assertions.

These runs use **CRT**, the approved fallback because the local mimalloc submodule
is unavailable. They are not directly comparable to the mimalloc tables below.
Ryzen 5 5500, LDC 1.43.0 / LLVM 22.1.8, `-O2 -release`, ordinary 8 MiB Farm,
16,384 counted actors, small-tier quota 16,384 words, five warmup cycles and
three-second windows. Each base/new configuration has five alternating pairs.
Controller CPU 0 and consumers 1..N are pinned; 0/6, 1/7, … are SMT siblings.
Private modes use the lifecycle profiles 0/1/5/11; public modes use the plan's
0/6-consumer gate. Tests and the TSan soak finished before measurement.
The base was built from a clean archive inside the implementation worktree.

| Mode | Batch | Consumers | Base median M/s | New median M/s | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| private | 256 | 0 | 8.745 | 8.691 | -0.62% |
| private | 256 | 1 | 8.172 | 8.224 | +0.65% |
| private | 256 | 5 | 7.495 | 7.652 | +2.10% |
| private | 256 | 11 | 6.532 | 6.644 | +1.71% |
| private-remote | 256 | 0 | 8.748 | 8.752 | +0.05% |
| private-remote | 256 | 1 | 8.379 | 8.271 | -1.28% |
| private-remote | 256 | 5 | 4.507 | 4.502 | -0.11% |
| private-remote | 256 | 11 | 3.352 | 3.361 | +0.26% |
| tree | 256 | 0 | 10.965 | 10.819 | -1.33% |
| tree | 256 | 1 | 9.871 | 10.733 | +8.73% † |
| tree | 256 | 5 | 10.507 | 10.713 | +1.95% |
| tree | 256 | 11 | 8.325 | 8.191 | -1.61% |
| actor | 256 | 0 | 7.356 | 7.788 | +5.88% † |
| actor | 256 | 6 | 5.559 | 5.674 | +2.07% |
| wave | 256 | 0 | 7.374 | 7.907 | +7.23% † |
| wave | 256 | 6 | 5.659 | 5.818 | +2.82% |
| wave | 16384 | 0 | 7.961 | 8.275 | +3.94% † |
| wave | 16384 | 6 | 5.964 | 6.133 | +2.84% |
| tree, repeat | 256 | 1 | 9.783 | 10.137 | +3.61% † |

† Outside the symmetric ±3% band, favourable; not a regression. These exceptions
are retained explicitly rather than described as within ±3%. Polling tree at one
consumer measured **+8.73%** initially and **+3.61%** in an additional five-pair
repeat. Both rounds are retained. The initial base
sample **7.79 M/s** is an outlier (the other four are 9.75–9.94 M/s), but its median
was calculated normally without discarding it. Public actor and wave controls
also show favourable deviations at zero consumers.

A work-equivalence check confirmed all **210** runs report
`verified_actor_cycles == (rounds + 5) * 16384`. Tree runs retain 64 parents and
one terminal operation per child; per-cycle callbacks remain exactly counted.
Substituting `false` into the polling-tree specialization reproduces the base
callback, and the measured public dispatch/retirement/checking body is unchanged.
No cause for the favourable shifts was established; the benchmark code/link
layout changed, but these measurements do not isolate its effect.

### The `tree-wait` mode

`actor_churn tree-wait` runs the same parent/child lifecycle as `tree`, using
`waitChildren` after launching a terminal child wave and `finish` on the single
resumption. It does not poll `finished`. Both modes share the callback template;
child state layout and counted work are unchanged. `private_lifecycle_matrix.py`
includes both in its lifecycle and child suites. For example:

```sh
make -C perftest actor_churn
ANTFARM_HUGE_PAGES=0 ./perftest/actor_churn tree-wait 16384 3 5 256 5 crt 0,1,2,3,4,5 small 16384
```

| Consumers | tree-wait median M/s | Min–max M/s |
| ---: | ---: | ---: |
| 0 | 11.038 | 10.802–11.218 |
| 1 | 10.420 | 9.986–10.998 |
| 5 | 11.106 | 10.989–11.144 |
| 11 | 8.451 | 8.176–8.589 |

These feature-enabled samples were taken after the paired gate and have no
pre-E1 `tree-wait` baseline; they are throughput measurements, not a paired
speedup claim. The deterministic tests establish the removed polling callbacks.

Evidence: [protocol and reproduction](results/private-root-wait-2026-10-04/README.md),
[metadata](results/private-root-wait-2026-10-04/metadata.json),
[all primary runs](results/private-root-wait-2026-10-04/runs.jsonl),
[medians and ranges](results/private-root-wait-2026-10-04/summary.json),
[tree repeat](results/private-root-wait-2026-10-04/tree-one-repeat/summary.json),
[gate assessment](results/private-root-wait-2026-10-04/assessment.json),
[work-equivalence checks](results/private-root-wait-2026-10-04/work-equivalence.json),
and [build/test/mutation evidence](results/private-root-wait-2026-10-04/validation.md).

## 1.7.1-rc.1 batch policy — 2026-10-04

These measurements precede the follow-up that clamps root entry storage to
capacity and propagates explicit child limits to descendants. Raw metadata
preserves the measured source; the follow-up was validated for correctness
without re-recording these performance series.

Keep **`defaultPrivateBatchLimit = 256`** and allocate **`ceil(capacity /
configured batchLimit)`** root table slots. The sweep below measured all seven
limits with all three quotas and all four consumer counts. With the default
4,096-word small quota, 256 has the highest median at every consumer count.
Larger configured limits repeatedly copy/requeue a larger unaccepted suffix
and provide fewer table slots while the Farm still admits about 225 entries.
There is no throughput reason in these measurements to add a separate
`maxTablesInFlight` parameter. A 16,384-root park configured at 256 has 64
table slots; configure smaller batches when more small tables must be in flight.

At a bulk quota of 131,072 words, the one-thread median is 36.51 M lifetimes/s
at 256 and 36.68 at 512: this workload has already reached a plateau there.
Other consumer counts have different optima and broad ranges; there is no
universal fastest batch. At eleven consumers, bulk/1024 reaches 23.67 M/s
versus 19.27 at bulk/256, so that configuration can justify a larger limit.
A larger admitted physical table also ties more roots to its slowest callback.
Choose the Farm tier/quota together with the batch limit and latency needs.

All new runs use the same Ryzen 5 5500 and mimalloc **3.5.0** commit
`18b08671c9302247bfb682286e6bf3cc1773f801` as the historical matrix below.
The rebuilt static archive has the same SHA-256 as that historical artifact.
LDC 1.43.0 uses `-O2 -release`; the debug reentrancy guard is absent.
Each configuration has five measured runs of at least three seconds, each
after five warmup cycles, pinned to CPU 0 for the emitter and CPUs 1..N for
consumers. The Farm is 8 MiB with ordinary pages; each cycle allocates,
dispatches and frees 16,384 counted actors. Configuration order reverses
between repetitions. The table shows medians in M complete lifetimes/s;
the linked summaries retain min–max ranges and raw runs retain every command.

| Consumers | Tier / quota words | 32 | 64 | 128 | 256 | 512 | 1024 | 4096 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | small / 4096 | 26.89 | 24.00 | 25.57 | 27.52 | 21.82 | 15.82 | 7.25 |
| 0 | small / 16384 | 27.48 | 30.47 | 27.08 | 26.25 | 28.91 | 33.49 | 19.76 |
| 0 | bulk / 131072 | 32.60 | 33.18 | 32.37 | 36.51 | 36.68 | 36.06 | 33.33 |
| 1 | small / 4096 | 26.60 | 25.66 | 27.12 | 28.23 | 22.50 | 16.66 | 7.59 |
| 1 | small / 16384 | 25.03 | 30.19 | 30.15 | 28.95 | 29.64 | 34.72 | 20.66 |
| 1 | bulk / 131072 | 26.74 | 28.98 | 31.40 | 34.55 | 35.40 | 35.18 | 35.92 |
| 5 | small / 4096 | 21.78 | 22.12 | 23.96 | 24.91 | 20.46 | 15.34 | 7.02 |
| 5 | small / 16384 | 34.58 | 24.65 | 26.19 | 25.99 | 26.32 | 26.59 | 16.95 |
| 5 | bulk / 131072 | 34.41 | 28.04 | 28.64 | 28.59 | 29.55 | 28.86 | 30.34 |
| 11 | small / 4096 | 16.22 | 15.21 | 17.02 | 22.08 | 17.58 | 14.52 | 7.56 |
| 11 | small / 16384 | 15.67 | 19.08 | 21.67 | 26.20 | 17.69 | 19.06 | 14.02 |
| 11 | bulk / 131072 | 20.74 | 18.45 | 20.84 | 19.27 | 22.11 | 23.67 | 22.63 |

**420 successful root-batch runs**: [metadata](results/private-batch-2026-10-04/metadata.json),
[raw runs](results/private-batch-2026-10-04/runs.jsonl), and
[summary with ranges](results/private-batch-2026-10-04/summary.json).

### Inherited child limit check

The tree mode changes cohort size with the inherited limit, while keeping
16,384 counted children. Thus parent count and allocation distribution also
change: this is a practical cohort check, not an isolated limit-only comparison.
At five consumers, 256 is substantially faster than 1024 or 4096 in both quota
configurations, supporting the inherited default. At zero consumers the larger
bulk cohorts can amortize more overhead. These observations do not remove the
slow-callback trade-off.

| Consumers | Tier / quota words | 256 | 1024 | 4096 |
| ---: | --- | ---: | ---: | ---: |
| 0 | small / 16384 | 56.57 | 54.60 | 60.76 |
| 0 | bulk / 131072 | 56.59 | 60.94 | 62.61 |
| 5 | small / 16384 | 45.56 | 17.46 | 22.29 |
| 5 | bulk / 131072 | 47.70 | 17.34 | 28.20 |

**60 successful child-cohort runs**: [metadata](results/private-child-batch-2026-10-04/metadata.json),
[raw runs](results/private-child-batch-2026-10-04/runs.jsonl), and
[summary with ranges](results/private-child-batch-2026-10-04/summary.json).

### Re-recorded lifecycle matrix

The historical configurations below were repeated with the same allocator
archive and pinning, now with three-second rather than two-second windows.

| Mode | 0 consumers | 1 consumer | 5 consumers | 11 consumers |
| --- | ---: | ---: | ---: | ---: |
| Public actor | 16.74 | 19.10 | 16.95 | 11.81 |
| Public wave, 256 | 21.14 | 22.59 | 18.42 | 13.30 |
| Public wave, whole cohort | 21.01 | 23.33 | 19.17 | 13.34 |
| Private root, emitter free | 27.90 | 28.52 | 25.30 | 29.10 |
| Private root, consumer free | 27.27 | 27.97 | 13.45 | 8.12 |
| Private parent + children | 54.96 | 58.54 | 45.36 | 25.79 |

**150 successful lifecycle runs**, including the C-runtime control at five
consumers: [metadata](results/private-lifecycle-2026-10-04/metadata.json),
[raw runs](results/private-lifecycle-2026-10-04/runs.jsonl), and
[summary with ranges](results/private-lifecycle-2026-10-04/summary.json).

Reproduce these suites with `private_lifecycle_matrix.py --suite batch`,
`--suite child`, and `--suite lifecycle`, respectively, each with
`--mimalloc-version 30500` and a binary linked to the historical archive.
The script defaults to five repetitions, five warmup cycles and three seconds.
The independent public-versus-729c961 gate is recorded separately; the
historical lifecycle comparison is not that gate.

### Public performance gate against 729c961

The final branch and baseline binaries use the same pinned mimalloc archive,
compiler flags, CPU affinity, five warmups and three-second measurement windows
described above. Each of six configurations has five runs per binary; paired
baseline/candidate order alternates between repetitions. The two initially
flagged zero-consumer configurations were repeated with ten alternating pairs
each. All 100 runs, including the initial flagged results, are retained.

| Mode | Consumers | Pairs | Baseline median M/s | Branch median M/s | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Actor | 0 | 5 | 17.301 | 16.720 | −3.36% |
| Wave, 256 | 0 | 5 | 20.921 | 20.839 | −0.39% |
| Wave, whole cohort | 0 | 5 | 20.990 | 19.816 | −5.59% |
| Actor | 6 | 5 | 14.127 | 14.121 | −0.04% |
| Wave, 256 | 6 | 5 | 14.926 | 14.987 | +0.41% |
| Wave, whole cohort | 6 | 5 | 15.475 | 15.575 | +0.65% |
| Actor, repeat | 0 | 10 | 16.696 | 16.545 | −0.90% |
| Wave, whole cohort, repeat | 0 | 10 | 20.922 | 21.026 | +0.50% |

The initial actor ranges were separated (17.032–17.514 versus 16.507–16.978),
while its repeat ranges overlap (12.602–17.450 versus 15.705–17.077).
Whole-cohort wave ranges overlap in both series; its repeat ranges are
17.564–21.428 versus 14.209–21.746. The larger repeat series put both flagged
medians within 3%, so the initial deltas are treated as measurement noise.
The gate passes on that basis, rather than discarding the original runs.

`antfarm.d`, `antfarm_allocation.d`, `antfarm_templates.d` and `actors/wave.d`
are byte-identical to 729c961. The only `actors/actor.d` difference adds three
enum members under `AntfarmActorTestHooks`; release builds omit them. Public
runtime logic is unchanged, although the private benchmark modes change the
binary and link layout.

Evidence: [initial metadata](results/public-gate-2026-10-04/metadata.json),
[raw runs](results/public-gate-2026-10-04/runs.jsonl),
[initial summary](results/public-gate-2026-10-04/summary.json),
[actor repeat](results/public-gate-2026-10-04/actor-zero-repeat/summary.json),
[whole-cohort repeat](results/public-gate-2026-10-04/whole-cohort-repeat/summary.json),
and [gate assessment](results/public-gate-2026-10-04/assessment.json).
Each repeat directory also contains its metadata and raw command/output log.

## Historical lifecycle experiment — 2026-09-25

The private ownership model improves this synthetic lifecycle workload when
paired with suitable reclamation locality. At six total threads, private
roots reach **22.2 M lifetimes/s**, versus **13.3 M** for public actors and
**14.7 M** for whole-cohort public waves. Private parents creating their own
child cohorts reach **46.0 M child lifetimes/s**. At twelve total threads,
private roots reach **26.3 M**, while the tree workload falls to **22.9 M**.
These are medians, not guarantees; the ranges below are part of the result.

### Measurement conditions

Same Ryzen 5 5500 host: six physical cores, twelve SMT threads. LDC 1.43.0,
DMD frontend 2.113.0, LLVM 22.1.8, `-O2 -release`, mimalloc **3.5.0** verified
by `mi_version() == 30500`, 16-byte counted actor state, **8 MiB** Farm with
ordinary pages, 16,384 counted actors per cycle. Each case has five runs of
at least two seconds after five warmup cycles. Mode order reverses on
alternating repetitions. Every cycle verifies callback counts and finishes
all retirement/free/recycling before another cycle begins.

The one-thread case creates, consumes, and reclaims on CPU 0. Other rows
use one emitter on CPU 0 plus respectively 1, 5, or 11 consumers on the next
CPUs. Thus **six threads means 1 + 5**, not six consumers plus a controller.
CPUs 0..5 occupy distinct physical cores; 0/6, 1/7, … are SMT siblings.

All controls use the same binary and the corrected Farm/public-wave completion
protocols. The experiment started from `1809f0b` and used its mimalloc 3.5.0
dependency. The current build pin is 3.5.3; the recorded numbers below remain
3.5.0 results.
Farm, root/public runtime metadata, result buffers, and threads persist;
counted actor state is freshly allocated and freed every cycle. Startup and
persistent-runtime teardown are outside the timer.

### Mimalloc results

Millions of complete counted actor lifetimes per second; five-run median.

| Mode | 1 thread | 2 threads | 6 threads | 12 threads |
| --- | ---: | ---: | ---: | ---: |
| Public actor | 17.93 | 18.70 | 13.34 | 10.71 |
| Public wave, 256 | 22.95 | 24.90 | 14.21 | 13.90 |
| Public wave, whole cohort | 23.59 | 17.25 | 14.67 | 12.60 |
| Private root, emitter free | 30.98 | 23.89 | 22.20 | 26.28 |
| Private root, consumer free | 30.32 | 27.66 | 13.44 | 6.09 |
| Private parent + child wave | 62.06 | 63.85 | 45.96 | 22.86 |

Full observed min–max ranges, in the same units:

| Mode | 1 thread | 2 threads | 6 threads | 12 threads |
| --- | ---: | ---: | ---: | ---: |
| Public actor | 12.75–18.39 | 15.11–20.62 | 13.27–19.10 | 9.06–13.67 |
| Public wave, 256 | 15.37–23.73 | 16.58–25.42 | 14.18–20.52 | 13.48–14.33 |
| Public wave, whole cohort | 15.58–23.86 | 16.94–25.48 | 14.58–20.92 | 11.59–13.69 |
| Private root, emitter free | 24.92–31.49 | 23.64–31.28 | 22.00–28.48 | 19.75–29.33 |
| Private root, consumer free | 24.83–31.39 | 23.18–30.26 | 13.36–13.87 | 5.68–6.44 |
| Private parent + child wave | 39.55–62.87 | 31.67–64.29 | 44.27–47.27 | 13.40–23.07 |

Several ranges are wide and overlap, especially at one or two threads. For
example, private roots do not beat the 256-member public wave median at two
threads. No universal scaling or latency claim follows from this matrix.

### What changes between modes

`private` has no public owner/handle, inbox submission gate, wake coalescing,
or per-actor admission/exit CAS. A FIFO park blindly emits a detached prefix;
physical-table completion returns it to the emitter. Terminal roots are freed
there, on the thread that allocated them. This is the default policy.

`private-remote` uses the same private protocol but frees terminal root state
on consumers. At six threads that reduces the median from 22.20 to 13.44 M/s;
at twelve, from 26.28 to 6.09 M/s. Free placement is therefore consequential
for this tiny-allocation workload. This comparison supports retaining local
root reclamation; it does not quantify individual mimalloc internals.

`tree` creates 64 additional private parent actors per cycle, each of which
creates 256 children inside its own dispatch and authorizes their terminal
wave. Child allocation occurs on workers; terminal child frees occur on their
executing consumers. All parent, child-state, and child-park metadata allocation,
dispatch, retirement, and frees are included. The headline counts only the
16,384 children, not the 64 additional parents or their polling activations.

This changes the work organization as well as the ownership protocol: creation
is distributed across workers, cohorts are smaller, and allocation locality
differs. It is not an isolated measurement of removing synchronization from
otherwise identical public actors. Its strongest six-thread result and weaker
twelve-thread result make it a useful candidate for real workload testing,
not a reason to choose the largest worker count.

### C-runtime control at six total threads

| Mode | Median M/s | Min–max M/s |
| --- | ---: | ---: |
| Public actor | 7.97 | 7.30–8.21 |
| Public wave, 256 | 8.27 | 8.03–8.49 |
| Public wave, whole cohort | 8.32 | 8.27–8.58 |
| Private root, emitter free | 8.17 | 8.11–8.48 |
| Private root, consumer free | 4.61 | 4.58–4.67 |
| Private parent + child wave | 10.14 | 9.59–10.19 |

The C-runtime root path is roughly level with public waves here. The large
mimalloc gains should not be generalized to every allocator. No arena or
additional allocator implementation was added.

### Correctness work and reproduction

The Farm needed an acquire/release completion RMW within each shard, before
joining shards through `Tprogress`. The focused two-consumer regression reports
a ThreadSanitizer race when only that RMW is reverted to relaxed ordering.
Public waves also needed a producer hold and final-access table credits:
progress equality could expose completion while another hook still used the
descriptor, or could be read before a later publication/seal. Deterministic
pause/reuse tests fail against the old equality implementation and pass after
the fix. Both fixes are included in every number above.

Private tests pass with LDC, DMD, and LDC ThreadSanitizer, including nested
waves, both root reclamation policies, immediate completion before publication
returns, FIFO progress, partial/zero admission, and concurrent creators. The
Farm suite and public actor torture suite pass; the public suite also passes
DMD and ThreadSanitizer. The completed replacement matrix contains 150
successful runs. An earlier exploratory matrix stopped on a public-wave
abort; its numbers are excluded. The lost stderr from that first abort does
not establish which specific race caused it.

```sh
make -C private_actor_tests run run-dmd run-tsan
make -C actor_torture run run-dmd run-tsan
DUB_HOME=/tmp/antfarm-dub ANTFARM_HUGE_PAGES=0 dub test --compiler=ldc2
# Initialize the committed mimalloc submodule before the regular build target.
make -C perftest actor_churn_mimalloc
python3 perftest/private_lifecycle_matrix.py \
  --binary perftest/actor_churn_mimalloc --output /tmp/private-lifecycle
```

These commands now build and check mimalloc 3.5.3. To repeat the historical
allocator comparison, link the 3.5.0 archive at commit
`18b08671c9302247bfb682286e6bf3cc1773f801` and pass
`--mimalloc-version 30500` to the matrix script. Record those new runs in a
separate output directory.

The recorded experiment used the already-built matching 3.5.0 static archive
from the original checkout, with `MI_OVERRIDE=OFF`, rather than reinitializing its
submodule. The compiler flags, binary/archive hashes, complete command lines,
and raw output are retained in
[metadata](results/private-lifecycle-2026-09-25/metadata.json),
[runs](results/private-lifecycle-2026-09-25/runs.jsonl), and
[summary](results/private-lifecycle-2026-09-25/summary.json).
The matrix script assumes this host's CPU layout and defaults to checking
version 30503. It records the expected version in metadata and the reported
version with each run; adapt placement deliberately when changing hosts.

See [PRIVATE_ACTORS.md](../actors/PRIVATE_ACTORS.md) for API usage and limits:
struct/scalar state, callback-scoped `@system` borrowing, whole and subset child waves, and
synchronous child callbacks. Resumable child continuations are future work.

## Typed result ports: unused-feature gate (2026-10-04)

Compared E2 ports slice 1 against local main `374909c`, retaining `R = void` in
all benchmark cohorts. CRT allocator was used because the mimalloc submodule
was unavailable. Both binaries used LDC 1.43.0 `-O2 -release`, huge pages off,
16384 actors, batch 256 and small quota 16384 words. Five alternating runs per
binary/configuration used five warmup cycles and three-second windows, pinned
to CPU 0 (emitter) and CPUs 1–6 (six consumers). No build or test workload ran
during measurement.

| Mode | 0 consumers base → ports, M/s | Change | 6 consumers base → ports, M/s | Change |
| --- | ---: | ---: | ---: | ---: |
| `actor` | 7.793 → 7.678 | -1.47% | 5.906 → 5.932 | +0.44% |
| `wave` | 8.233 → 8.436 | +2.47% | 6.078 → 6.012 | -1.08% |
| `private` | 8.727 → 8.931 | +2.34% | 6.512 → 6.492 | -0.31% |
| `private-remote` | 9.012 → 9.121 | +1.21% | 3.854 → 3.878 | +0.62% |
| `tree` | 11.133 → 10.980 | -1.38% | 9.736 → 9.844 | +1.11% |
| `tree-wait` | 10.784 → 10.801 | +0.16% | 10.339 → 10.205 | -1.30% |

All twelve medians pass the −3% regression limit; changes range from −1.47%
to +2.47%, with no gain above +3%. This is the unused-feature gate, not a
measurement of result-slot throughput. The optional `tree-results` mode was
not added; `tree` and `tree-wait` are unchanged.

[Metadata](results/private-result-ports-2026-10-04/metadata.json),
[raw runs](results/private-result-ports-2026-10-04/runs.jsonl),
[summary](results/private-result-ports-2026-10-04/summary.json), and
[validation/mutations](results/private-result-ports-2026-10-04/validation.md)
retain the evidence. Reproduce with an isolated baseline build:

```sh
python3 perftest/results/private-result-ports-2026-10-04/measure.py \
  BASELINE/perftest/actor_churn perftest/actor_churn
```

## E2-2 subset waves and unused-feature gate (2026-10-05)

Compared subset-wave code against local main `3441b8b`. Both binaries use LDC
1.43.0 `-O2 -release`, mimalloc 3.5.3 (prebuilt static archive read from the
main checkout), huge pages off, n=16384, batch 256, small quota 16384 words,
five warmup waves/cycles and three-second windows. Each configuration has
five alternating runs, pinned to CPU 0 for the controller and CPUs 1–6 for
six workers. Builds and tests were stopped during the 150 measured runs.
The baseline was built from an isolated git archive; the main checkout was
not modified. `tree-results` is absent, so it is not part of the gate.

The unused-feature gate compares ordinary create/dispatch/retire/reclaim
cycles. All ten medians pass the −3% threshold, ranging from −1.10% to
+1.51%; no gain exceeds +3%.

| Mode | 0 workers base → subsets, M lifetimes/s | Change | 6 workers base → subsets, M lifetimes/s | Change |
| --- | ---: | ---: | ---: | ---: |
| `private` | 27.039 → 27.256 | +0.80% | 20.755 → 20.883 | +0.62% |
| `private-remote` | 26.816 → 26.521 | -1.10% | 12.813 → 12.832 | +0.14% |
| `tree` | 53.729 → 53.836 | +0.20% | 43.460 → 43.791 | +0.76% |
| `tree-wait` | 53.141 → 53.405 | +0.50% | 42.733 → 43.379 | +1.51% |
| `dtor` | 26.751 → 26.607 | -0.54% | 20.848 → 21.008 | +0.77% |

The subset mode keeps one root and its cohort alive, excludes setup and
teardown from timing, and runs no-op member waves through `waitChildren`.
The list selects indices in reverse order. Percentages round down to 163,
1638 and 16384 participants. Two whole-wave comparators use the same model:
`subset-whole` reserves selection storage, while
`subset-whole-unreserved` does not. Rates include selection validation/copy,
publication, the Wave join and root resumption. They measure waves and
selected callbacks, not allocation or retirement churn.

| Mode / participants | 0 workers, waves/s | Selected callbacks, M/s | 6 workers, waves/s | Selected callbacks, M/s |
| --- | ---: | ---: | ---: | ---: |
| Subset 1% / 163 | 600,205 | 97.833 | 253,318 | 41.291 |
| Subset 10% / 1638 | 71,913 | 117.794 | 47,416 | 77.667 |
| Subset 100% / 16384 | 7,246 | 118.721 | 5,098 | 83.534 |
| Whole, reserved / 16384 | 9,256 | 151.651 | 5,927 | 97.110 |
| Whole, unreserved / 16384 | 9,069 | 148.581 | 5,910 | 96.823 |

Against the reserved whole wave, 1% subsets run 64.84× / 42.74× as many
waves per second with 0 / 6 workers; 10% subsets run 7.77× / 8.00×.
**Known limitation:** 100% subsets have 21.71% / 13.98% lower wave throughput
than reserved whole waves, missing design §8's expectation of being within
5%. This corresponds to 27.74% / 16.25% more time per wave. Use `start` when
every member is needed. The unused-feature gate passes; the full-selection
expectation was reported to the orchestrator, who approved recording it
without runtime tuning in this PR. Reserved whole waves measure +2.07% /
+0.30% throughput versus unreserved whole waves; these small differences do
not establish a benefit from storage reservation.

[Metadata](results/private-subsets-2026-10-05/metadata.json),
[raw runs](results/private-subsets-2026-10-05/runs.jsonl),
[summary](results/private-subsets-2026-10-05/summary.json),
[validation](results/private-subsets-2026-10-05/validation.md) and
[35 mutations](results/private-subsets-2026-10-05/mutations.md) retain the
evidence. Reproduce with the two matching binaries and prebuilt archive:

```sh
python3 perftest/results/private-subsets-2026-10-05/measure.py \
  BASELINE/perftest/actor_churn_mimalloc perftest/actor_churn_mimalloc \
  MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a
# Explicit subset and whole-wave comparisons (last argument is percentage):
ANTFARM_HUGE_PAGES=0 perftest/actor_churn_mimalloc subset \
  16384 3 6 256 5 mimalloc 0,1,2,3,4,5,6 small 16384 10
ANTFARM_HUGE_PAGES=0 perftest/actor_churn_mimalloc subset-whole \
  16384 3 6 256 5 mimalloc 0,1,2,3,4,5,6 small 16384
ANTFARM_HUGE_PAGES=0 perftest/actor_churn_mimalloc subset-whole-unreserved \
  16384 3 6 256 5 mimalloc 0,1,2,3,4,5,6 small 16384
```

## E2-3 behaviour-neutral plumbing gate (2026-10-05)

Compared the E2-3 plumbing refactor (child Service pointer in the cohort,
`rootService` accessor, `Emittable`, generic wave credit target,
`Service.childReturns`) against local main `370dfbc`. LDC 1.43.0
`-O2 -release`, mimalloc 3.5.3 from the main checkout's read-only prebuilt
archive, huge pages off, n=16384, batch 256, small tier, quota 16384 words,
five warmups, three-second windows. Profiles with 0 consumers (CPU 0) and 6
consumers (CPUs 0–6); base/candidate measurements interleaved within each
repeat with the order reversing on successive repeats. `tree-results` is
absent from actor_churn, so it has no gate row. Tests and soak finished
before measurements; the main checkout was not modified.

**Gate verdict: passed on byte-identical binaries.** The gate table below is
the clean 11-repeat alternating run (a second clean 7-repeat run agrees on
all rows, worst −2.16%). Its base and candidate executables —
`336c751d…` and `a52b2531…` — are bit-for-bit the executables rebuilt at the
final tip `cf09780` (sha256-verified; the tip adds only documentation and
commit messages over `cbd3093`, so no compiled source differs). All 14
mode-configs pass the −3% median threshold; the worst regression is −2.64%
(subset k=1%, 0 consumers) and the largest gain is +0.94%.

| Mode | 0 cons. base → E2-3 | Change | 6 cons. base → E2-3 | Change |
| --- | ---: | ---: | ---: | ---: |
| `private` | 27.642 → 27.548 | -0.34% | 20.953 → 20.946 | -0.03% |
| `private-remote` | 27.184 → 26.962 | -0.81% | 12.357 → 12.418 | +0.50% |
| `tree` | 54.917 → 54.472 | -0.81% | 39.030 → 39.396 | +0.94% |
| `tree-wait` | 54.445 → 54.186 | -0.48% | 38.915 → 39.222 | +0.79% |
| `dtor` | 27.078 → 27.042 | -0.13% | 21.289 → 21.430 | +0.66% |
| subset k=1% | 556,610 → 541,907 waves/s | -2.64% | 231,181 → 232,560 waves/s | +0.60% |
| subset k=100% | 6,725 → 6,645 waves/s | -1.19% | 4,546 → 4,501 waves/s | -1.00% |

The one clean fresh-at-tip profile (attempt 2, consumers=0, bases inside the
established ranges) also passes all seven mode-configs (worst −1.81%) and
reports two gains above +3% — `tree` +4.93% (55.341 → 58.068) and
`tree-wait` +3.15% — which are reported, not failures, and do not change the
verdict.

Three fresh 11-repeat attempts against the tip itself were invalidated by
erratic external host CPU load (loadavg spikes to ~7–15 from outside the
measurement container): base and candidate depressed together far outside
the clean runs' variance, with same-repeat interleaved pairs diverging
exactly where bursts land. Per the agreed rule a burst depressing both
versions alike invalidates an attempt rather than failing the candidate;
attempt 2's clean first profile passes all seven mode-configs (worst
−1.81%), and attempts were capped at three once the host stayed erratic.
A dedicated re-measurement at the tip can revisit this when the host is
quiet; it cannot change the binaries, which are already identical to the
gate's. The known startSubset-at-k=100% overhead versus whole waves
(21.71% / 13.98% in E2-2) is unchanged by the plumbing: no cheap
reduction appeared, and none was implemented.

[Protocol, file map and assessment](results/private-plumbing-2026-10-05/README.md),
[the 7-repeat run](results/private-plumbing-2026-10-05/runs-run1.jsonl),
[the 11-repeat summary](results/private-plumbing-2026-10-05/summary-cbd3093-11rep.txt),
[contaminated attempts](results/private-plumbing-2026-10-05/attempt1-summary.txt),
[mutation evidence](results/private-plumbing-2026-10-05/mutations.md),
[tip checks](results/private-plumbing-2026-10-05/check-tip.txt) and
[per-commit builds](results/private-plumbing-2026-10-05/commit-builds.txt)
retain the evidence. Reproduce with the two matching binaries and the
prebuilt archive (base from an isolated `git archive 370dfbc`):

```sh
make -C perftest -o MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a \
  actor_churn_mimalloc \
  MIMALLOC_LIBRARY=MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a
python3 perftest/results/private-plumbing-2026-10-05/measure.py \
  BASELINE/perftest/actor_churn_mimalloc perftest/actor_churn_mimalloc \
  MAIN/actor_torture/.build/mimalloc-v3.5.3/libmimalloc.a 11
```
