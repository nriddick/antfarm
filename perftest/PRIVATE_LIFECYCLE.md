# Private actor full-lifecycle experiment — 2026-09-25

The private ownership model improves this synthetic lifecycle workload when
paired with suitable reclamation locality. At six total threads, private
roots reach **22.2 M lifetimes/s**, versus **13.3 M** for public actors and
**14.7 M** for whole-cohort public waves. Private parents creating their own
child cohorts reach **46.0 M child lifetimes/s**. At twelve total threads,
private roots reach **26.3 M**, while the tree workload falls to **22.9 M**.
These are medians, not guarantees; the ranges below are part of the result.

## Measurement conditions

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
protocols. The committed base is `1809f0b`; this isolated experiment uses its
mimalloc 3.5.0 dependency, not the separate checkout's pending 3.5.3 upgrade.
Farm, root/public runtime metadata, result buffers, and threads persist;
counted actor state is freshly allocated and freed every cycle. Startup and
persistent-runtime teardown are outside the timer.

## Mimalloc results

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

## What changes between modes

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

## C-runtime control at six total threads

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

## Correctness work and reproduction

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

This worktree used the already-built matching 3.5.0 static archive from the
original checkout, with `MI_OVERRIDE=OFF`, rather than reinitializing its
submodule. The compiler flags, binary/archive hashes, complete command lines,
and raw output are retained in
[metadata](results/private-lifecycle-2026-09-25/metadata.json),
[runs](results/private-lifecycle-2026-09-25/runs.jsonl), and
[summary](results/private-lifecycle-2026-09-25/summary.json).
The matrix script assumes this host's CPU layout and checks version 30500;
adapt those deliberately when changing host or allocator version.

See [PRIVATE_ACTORS.md](../actors/PRIVATE_ACTORS.md) for API usage and limits:
POD state, callback-scoped `@system` borrowing, whole-cohort child waves, and
synchronous child callbacks. Resumable child continuations are future work.
