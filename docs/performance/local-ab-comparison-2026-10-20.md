# Comparing local_ab with the writer restoration — October 20, 2026

This comparison fixes our writer candidate at `651030b` and the fetched
`local_ab` tip at `05afaaf`. The current main working branch is
`1.9.0-progress`; no runtime change or branch merge was made for this comparison.

## Scope and source differences

The remote branch contributes two runtime commits:

- `edb665a`: specialized admission scans with validation hoisted out of the
  fixed prefix loop and a running compact-record sum.
- `4630c4b`: restore unaccepted private roots at the front of the ready FIFO,
  retaining their existing links and repairing the head/tail in constant time.

Both writer implementations retain the five-word header, explicit-length
arithmetic sizing, layout degradation, preparation, and publication/join
ordering. The remote scanner checks the first fixed entry once; later entries
with the same non-null callback and body length inherit that validation. A
changed callback or length goes through the next phase's checks. The compact
phase accumulates body and callback words together.

`local_ab` diverged at `091a58f`, before our private C exports and recent public
actor changes. A whole-branch comparison therefore includes unrelated source
differences. The three actor variants below separate the queue change:

| Variant | Sources |
| --- | --- |
| Ours | Exact `651030b` |
| Remote | Exact `05afaaf` |
| Ours + FIFO | `651030b` with only the runtime edit from `4630c4b` |

The isolated FIFO edit was checked against the exact before/after source in
`4630c4b`. It lives only in the evidence snapshot. Typed and erased private
actors on our branch retain their current implementation. The benchmark
`actor_churn.d` is byte-identical on both refs.

## Validation

The remote runtime passed the **full Farm unit suite from our candidate** in
LDC and DMD release builds, including our additional inferred broadcast,
empty-body, quota-boundary and preparation-rejection cases.

The remote and isolated FIFO variants both passed the full LDC private suite,
including six-consumer reuse/backpressure, nested waves, immediate completion
and concurrent creators. The remote `root-admission-order` selector passed
on both variants in debug and release. Running the same selector against our
unchanged queue failed at its expected ordered-callback assertion: a successful
negative control, not an unexplained test failure.

This is focused comparison evidence. It does **not** claim a new repository-wide
`make check`, TSan, native Windows or C ABI validation for the remote/integrated
candidate. Our prior full gate belongs to `651030b`; adopting either remote
change requires applicable final-tree validation.

## Core comparison

Intel i7-12700H under WSL2, LDC 1.42.0 / LLVM 21.1.8, `-O2 -release`, ordinary
pages, 16 MiB ring, unpinned workers, five interleaved processes per variant/case,
no background-load filtering. The first six cells use the exact benchmark and
workloads from the [writer restoration](writer-scan-2026-10-20.md). The two indexed
cells use a separately built, identical probe on both refs, alternating 16- and
15-word bodies in the existing payload pool. Their byte-throughput field still
assumes 16 words and is not used; the completed-payload rate is valid.

All 80 core processes verified completed payload counts. Rates are median
**Mpps**; percentages divide the remote median by our median. These small
screens are descriptive, not confidence bounds or an application speedup claim.

| Shape | Ours | Remote | Change |
| --- | ---: | ---: | ---: |
| Inferred callback, 1 word, 1 consumer | 518.795 | 611.131 | +17.80% |
| Inferred callback, 1 word, 4 consumers | 446.376 | 542.683 | +21.58% |
| Inferred callback, 16 words, 4 consumers | 138.986 | 146.248 | +5.22% |
| PayloadEntry, 1 word, 4 consumers | 413.045 | 519.835 | +25.85% |
| Mixed callbacks, 1 word, 4 consumers | 315.778 | 328.885 | +4.15% |
| Explicit length, 4 producers / 8 consumers | 2,214.727 | 2,153.183 | -2.78% |
| Inferred callback, indexed 16/15 words, 4 consumers | 97.991 | 96.365 | -1.66% |
| PayloadEntry, indexed 16/15 words, 4 consumers | 93.465 | 92.263 | -1.29% |

The explicit-length code was unchanged by either scanner patch. Movement in
that cell must not be attributed to reduced scan work; executable layout and
host variation remain possible contributors. Effective clocks and physical
P/E-core residency were not recorded.

## Private-root comparison

Thirty matched rounds per consumer count, three variants per round, three
seconds per process, 16,384 roots, batch 256, five warmup cohorts, automatic
root hint, quota 16,384 words and the same mimalloc 3.5.3 archive. Six permutations
of variant order repeat evenly. Controller/consumer guest CPU IDs are
`0,2,4,6,8,10,12`; the monitor uses guest CPU 1. These are not verified physical
P-core assignments. CPU telemetry is recorded, and no sample is excluded for
background activity. Builds, correctness checks and timing are serialized.

All 180 actor processes passed the benchmark's validity checks. Rates below
are median **millions of lifecycles/sec**. Each percentage is the median of
30 within-round ratios against ours; intervals use the existing performance
harness's descriptive bootstrap, not a calibrated confidence guarantee.

| Consumers | Variant | Median rate | Paired change vs ours | Descriptive interval | −3% threshold screen |
| --- | --- | ---: | ---: | --- | --- |
| 0 | Ours | 52.653 | — | — | reference |
| 0 | Remote | 63.295 | +19.32% | [+17.47%, +21.50%] | pass |
| 0 | Ours + FIFO | 64.633 | +22.32% | [+19.30%, +24.53%] | pass |
| 6 | Ours | 65.537 | — | — | reference |
| 6 | Remote | 66.255 | +1.38% | [+0.13%, +1.84%] | pass |
| 6 | Ours + FIFO | 65.993 | +0.89% | [+0.04%, +1.59%] | pass |

These are matched snapshot measurements using the existing actor harness and
statistics, not a `make perf-gate` invocation: the isolated FIFO variant has no
commit. The direct remote-versus-ours result includes branch divergence; the
isolated FIFO comparison is the useful attribution control. No public-actor,
tail-latency or complete PERFORMANCE refresh is claimed.

## Integration recommendation

Port the two scoped runtime changes onto the current branch, retaining our
additional Farm tests and the remote FIFO regression test. The remote scanner
improves the fixed-prefix cases by about 5–26% and the measured compact case by
4.2%, with an observed 1.3–1.7% loss in the two indexed probes. Its unchanged
explicit-length control is 2.8% lower in this screen; that movement is not an
algorithmic scanner regression established by this experiment.

The isolated FIFO correction gives a 22.3% paired median serial gain and 0.9%
with six consumers. The serial descriptive interval is entirely positive
(19.3% to 24.5%). This recovers substantial throughput on our current actor
implementation while keeping five metadata words and adding no padding. The
six-consumer result supports retaining the change without claiming a large
parallel speedup. It does not directly remeasure the earlier 1.8.2 baseline.

Do not replace the current tree with the older remote branch: keep the private
C exports and recent public-actor changes. The source review and focused
correctness tests found no blocking defect in the two proposed edits; final
integration still needs the repository's applicable correctness/performance
gates, including coverage of the existing C ABI.

## Evidence

Local evidence is `.build/writer-ab/local-ab-05afaaf/`: exact refs, source and
binary hashes, compiler identity, common benchmark source, correctness logs,
all 260 throughput samples, actor CPU telemetry and summaries. The archive
`.build/local-ab-comparison-evidence.zip` and SHA256 sidecar also include the
drivers and this report. Benchmark binaries and runtime source snapshots are
included; intermediate objects and correctness executables are omitted.
