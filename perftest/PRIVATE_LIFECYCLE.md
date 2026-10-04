# Private actor lifecycle and batching measurements

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
POD state, callback-scoped `@system` borrowing, whole-cohort child waves, and
synchronous child callbacks. Resumable child continuations are future work.
