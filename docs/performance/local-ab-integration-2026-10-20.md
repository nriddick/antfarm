# Selective local_ab integration — October 20, 2026

Integrated onto `ff79278`, the fetched `origin/1.9.0-progress` head containing
the comparison report, current private C exports and public-actor changes:

- `2277679`: port of `edb665a`, specialized inferred table-admission scans.
  Resolved the conflict with `651030b` by replacing only the inferred-sizing
  block. The resulting `antfarm/package.d` is byte-identical to `edb665a`.
  Existing expanded Farm tests remain byte-identical to `ff79278`.
- `759bfa2c`: port of `4630c4b`, restoring the unpublished root suffix to the
  front of its ready FIFO, with its regression test and ordering documentation.
  This applies to the current typed/erased root implementation, preserving the
  newer actor features and C exports.

The five-word header, explicit-length sizing, publication/claim memory ordering,
C headers/export sources and public-actor implementation are unchanged. SPEC's
scan description now reflects validation hoisting and the compact running sum.

## Focused validation before transfer

Linux x86-64; LDC 1.43.0 / LLVM 22.1.8 and DMD 2.113.0;
`ANTFARM_HUGE_PAGES=0`. All nine recorded commands exited zero:

- Full expanded Farm suite: LDC `-O2 -release`, DMD `-O -release`.
- Private test builds: LDC debug/release and DMD debug/release.
- Full private suite: LDC debug and release, including FIFO, early completion,
  six-consumer reuse/backpressure, nested waves and concurrent creators.
- `root-admission-order`: DMD debug and release.
- Complete `scripts/check-c-abi.py`: LDC and DMD, including native C/C++,
  private actors/cohorts, release fatal contracts, publication checks and hosted
  runtime integration. Both audit 98 core and 14 runtime exports; core archive
  audits and whole-archive native links pass.

Exact commands, exit codes and logs are in `.build/local-ab-integration/`.
An external copy and transfer bundle are retained in the accompanying Savina
workspace at `results/unified/antfarm-integration/20261020/`.
These were focused final-tree integration checks, not a full `make check`,
TSan, Windows gate or performance comparison. The reported gains remain the
[prior comparison's measurements](local-ab-comparison-2026-10-20.md), not new
measurements of this integrated tree. No push or benchmark repin was performed.

## Full Linux integration gate

After transfer, `make check SHELL=/usr/bin/bash` passed on committed runtime
`38d80b958e2387e501845e945669ed1fd43b8be7`, with a clean tracked tree at start.
This was native Linux x86-64 on the Ryzen 9 5900XT, using LDC 1.43.0 /
LLVM 22.1.8 and DMD 2.113.0. It is not a native Windows or WSL comparison.

The full gate ended with `ALL CHECKS PASSED` after **398.906 seconds**. Coverage
includes the Farm, private/public actors, parallel and threadpool suites, fiber
smoke, examples and documentation programs, fatal contracts, C/C++ ABI under
both compilers, and the registered TSan lanes. The small affinity mask was
`0-3`; the one-LLC mask was `0-7,16-23`. Conservative accounting of complete
commands containing sanitizer work, including their builds and any mixed
non-sanitizer subcommands, consumed **219.525 seconds** of the collective
600-second TSan budget. A process-group watchdog enforced the budget.

Evidence is retained in `.build/integration-gates-38d80b9/`: `correctness.log`,
`correctness-result.json`, `correctness-metadata.json`, and
`correctness-stages.jsonl`. Later changes affect only the performance harness
and documentation; runtime sources remain the tested committed candidate.

## Committed-ref actor performance gate

**Performance clearance is not established.** The full default gate compared
`ff79278` with `38d80b9`, using `38d80b9` benchmark source on both sides. Of
34 cells, the initial five-pair screen returned **24 pass, 3 regression and
7 inconclusive**; `gate.json` records exit status 1. An inconclusive result
does not count as a pass.

Runs used ordinary pages, LDC `-O2 -release`, the pinned mimalloc 3.5.3 archive,
16,384 actors, batch 256, quota 16,384 and three seconds per process. The
controller used CPU 0; the six-consumer cells used CPUs 1-6, all distinct
physical cores sharing one LLC. Timing was serial. The normal background CPU
filter remained enabled, with rejected attempts retained. The complete screen
took 1,082.390 seconds.

Initial non-passing cells are below. Deltas are medians of matched candidate /
baseline ratios; the interval is the gate's descriptive bootstrap interval.
At five pairs this is a conservative screen, not a calibrated 95% guarantee.

| Mode | Consumers | Median delta | Interval | Decision |
| --- | ---: | ---: | --- | --- |
| tree | 0 | -4.39% | [-4.91%, -1.97%] | inconclusive |
| tree-wait | 0 | -3.19% | [-4.38%, +3.41%] | inconclusive |
| tree-cont | 0 | -1.72% | [-3.93%, -0.69%] | inconclusive |
| subset, 1% | 0 | -6.71% | [-7.18%, -6.22%] | regression |
| subset, 10% | 0 | -8.69% | [-9.42%, -7.28%] | regression |
| subset, 100% | 0 | -4.86% | [-5.22%, -4.45%] | regression |
| subset-whole | 0 | -7.90% | [-12.78%, +7.11%] | inconclusive |
| subset-whole-unreserved | 0 | -7.17% | [-12.24%, +1.80%] | inconclusive |
| private | 6 | -0.88% | [-43.60%, +0.89%] | inconclusive |
| dtor | 6 | -2.89% | [-3.63%, -2.45%] | inconclusive |

Serial private, private-remote and dtor improved by approximately 50-54% in
the same screen. Those improvements do not cancel regressions in other cells.
All original samples and decisions remain under `perf-gate/`; boundary
follow-ups and identical-binary controls are retained separately.

The boundary follow-ups returned:

| Mode | Consumers | Clean pairs | Median delta | Interval | Decision |
| --- | ---: | ---: | ---: | --- | --- |
| tree | 0 | 20 | -4.30% | [-5.15%, -2.92%] | inconclusive |
| tree-wait | 0 | 20 | -4.33% | [-5.56%, -3.31%] | regression |
| tree-cont | 0 | 20 | +0.51% | [-0.31%, +1.00%] | pass |
| private | 6 | 30 | +0.06% | [-0.54%, +0.49%] | pass |
| dtor | 6 | 30 | -1.26% | [-2.39%, -0.62%] | pass |

Both base/base controls passed and verified byte-identical binary hashes:
serial tree at 20 pairs returned -0.44% with interval [-1.59%, +0.50%];
six-consumer private at 30 pairs returned -0.61% with interval
[-1.75%, -0.00%] (rounded). Evidence is in `serial-control/` and
`parallel-control/`.

These are separate runs, not a pooled replacement for the initial screen.
Serial subset failures remain, tree-wait now fails the boundary follow-up,
and tree plus the two subset-whole cells remain inconclusive. No performance
waiver was applied.

For follow-ups and controls, `--initial-load-limit 8` admits stale load averages
left by preceding threaded measurements. Before each gate, a three-second idle
window had to measure at most 0.25 busy core across the host. All normal
per-sample CPU exclusions remained enabled. The initial full screen used the
default load limit of 1. Commands, idle-window measurements, exit codes and
durations are in `followup-results.json`.

### Code and counter attribution

The scanner-only commit `2277679` produced an actor benchmark binary
byte-identical to `ff79278` when built with the common benchmark source.
Across the integrated baseline/candidate actor binaries, 4,170 of 4,185 common
functions have identical normalized disassembly; the 15 changed functions
are `pumpRoots` instantiations. Normalization ignores relocated control targets,
RIP displacements and trailing alignment no-ops. All emitted `writeImpl`
functions are identical under that comparison. This benchmark uses the
explicit-length writer path; it does not measure the inferred scanner change.

Three additional rotating `perf stat` pairs used the exact gate binaries.
Serial tree instructions per callback stayed about 195.542 while cycles rose
from 49.84 to 51.59. For serial subset at 10%, instructions stayed about 45.754
while cycles rose from 18.26 to 19.97. Subset has only one root, so reordering
multiple roots cannot explain that cell. These observations are consistent
with code-layout sensitivity, but they do not isolate a cause or establish a
layout waiver. Evidence is in `assembly/`, `scanner-only-build/` and
`actor-counters/`.

## Tail-latency harness and requested configurations

The report driver's `tail` section now explicitly includes both **avgCost 3 /
chunk 16** and **avgCost 5 / chunk 4**, for mid-drain and idle scenes. Each
process requests 10,000 observations per scene and one internal repetition;
the comparison rotates five independent processes per revision/configuration/
scene group. Both revisions compile the same corrected benchmark source.

Two benchmark defects had to be corrected before using the measurements:

- Numeric arguments were passed from D string slices to `atoi`/`strtoull`
  without ensuring NUL termination. One diagnostic process requested
  `--repeats 1` but reported `repeats=17`. The parser now uses `toStringz`.
  A heap-backed sliced-string regression test passes under LDC and DMD;
  restoring the old parser makes that same test fail as expected.
- A quota-truncated dump shorter than the progress trigger must be drained
  before retrying. The mailbox scene previously waited for an unreachable
  trigger and timed out; mid-drain could carry unfinished callbacks into the
  next iteration's progress count. Both now finish the short dump before
  taking another progress baseline.

The raw header records effective avgCost, chunk and repetitions. A dedicated
`make -C tools/perf check-tail` target runs the benchmark's unit tests; use
`DC=dmd` for DMD. Runtime library sources are unchanged by these corrections.

Tail processes use ordinary pages, a 16 MiB ring, six consumers on physical
CPUs 0-5, producer CPU 6 and mailbox CPU 7, all sharing one LLC. Aggregation
takes the median of independent process quantiles, not quantiles pooled across
processes. These are descriptive latency measurements, with no tail regression
threshold or automatic background-load exclusion. Raw placement and load
information is retained. Earlier diagnostic/provisional runs remain under
`tail/`, `tail-corrected/` and `tail-final/`; they are not mixed with the final
corrected-source comparison under `tail-verified/`.

The final corrected-source matrix completed **40/40 processes and 180/180
scenes**, each with 10,000 accepted observations (1.8 million total). The
independent audit checked the full expected scene matrix, observation counts,
ordered finite quantiles, source and binary hashes, and equality between the
measured harness and working-tree source. All checks passed.

Representative medians of process quantiles, in microseconds; each arrow is
`ff79278` to `38d80b9`. Mid-drain here means 256 payloads and 1 microsecond of
background work per callback.

| Scene | avgCost / chunk | p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- |
| Mid-drain | 3 / 16 | 5.2 → 5.3 | 22.2 → 22.2 | 24.1 → 24.4 |
| Mid-drain | 5 / 4 | 1.1 → 1.1 | 5.4 → 5.4 | 9.5 → 9.5 |
| Idle | 3 / 16 | 0.25 → 0.25 | 2.1 → 2.0 | 4.8 → 4.5 |
| Idle | 5 / 4 | 0.24 → 0.25 | 2.0 → 2.0 | 5.0 → 4.7 |

The short-dump correction materially changes some earlier provisional tail
values; only `tail-verified/` supplies this table. `comparison.csv` contains
all 18 scene/configuration comparisons, including mailbox, larger backlogs,
other callback costs and maxima. `summary.json` retains process-quantile
ranges, and `runs.jsonl` plus individual text outputs retain each process.

Final focused validation passed `check-tail` under LDC and DMD, plus
`python3 -B -m unittest tools/perf/test_perf_gate.py
tools/perf/test_run_performance_md.py`: 158 tests, three platform skips.
Commands and exit codes are in `harness-final-results.json`; parser mutation
evidence is in `tail-parser-results.json` and `tail-parser-negative.log`.

Commit `3e0da41` also applies the same NUL-termination fix to `throughput.d`
and `dual.d`, with sliced-argument regression tests and a combined
`make -C tools/perf check-benchmarks` target. Dual's string-valued `--tier`
continues to compare the original D string. These drivers were not used for
the measurements above. The actor gate uses bounded D conversions and is
unaffected by the parser defect.

The combined benchmark tests passed under LDC and DMD after these additional
fixes. Restoring the old parser independently in throughput and dual makes
each new sliced-argument test fail. Commands, compiler logs and negative
controls are retained in `.build/benchmark-parser-fixes-20261020/`.

## Repository location

The original `/home/nathan-riddick/pi_ground/antfarm` checkout was writable in
this gate session. The transferred bundle and all 13 manifest hashes were
verified before importing the integrated history. Its `1.9.0-progress` branch
was advanced to `38d80b9` before the gate run. The prior `05afaaf` tip remains
on `local_ab` and on
`backup/1.9.0-progress-before-integration-20261020`. The newer remote base is
`ff79278`; the older whole `local_ab` branch was not merged into it.

The source integration clone and original transfer evidence remain in the
Savina workspace. No push or `PERFORMANCE.md` benchmark repin was performed.
The full local evidence directory is archived as
`.build/integration-gates-38d80b9.tar.gz`, with a SHA-256 sidecar and an internal
file manifest. Raw gate artifacts remain ignored rather than committed.
