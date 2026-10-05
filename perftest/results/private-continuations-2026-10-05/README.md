# E2-4 private child continuations — 2026-10-05

Base: local main `632c7f6`. Implementation branch: `feature/private-continuations`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push or main-checkout modification is part of this work.

## Protocol and review

The runtime implements the task 86 §3 protocol and task 89 review corrections:
activation custody, lazy enclosing-wave H, final-owner child return publication,
emitter-private FIFO/P, W-credited resume tables, independent range cursor,
prepare-time prefix/tail transfer, and consumer-only deferred cleanup.
Entry remains 40 bytes; resume scheduling adds 48 bytes per cohort. Completion
allocates nothing. Ordinary operations add no RMW; the pump adds one raw hint.

[lifetime-review.md](lifetime-review.md) is the independent task 139 source review,
including explicit checks of all four hardest points and its limitations.
[self-review.md](self-review.md) records implementation review.

Orchestrator-approved clarifications:

- `rootService(entry, enclosing)` uses structural calling context to prove root
  identity; debug rejects child contexts. No pointer-bit tag or Entry growth.
- Keep `parentIsRoot_` in the typed cohort; resume storage lives in ChildBase.
- C-M13 checks the debug empty FIFO invariant. Correct head-based append hides
  an omitted tail clear from black-box behavior; the exact mutation remains.
- C-M10 checks debug write-once tags, appended to the existing result allocation
  (one byte per slot capacity, no extra allocation). A wait-time extra write is
  otherwise hidden by the final done write. Release storage/work is unchanged.
- C-M7 uses an erased callback only in its throwaway mutation patch. None remains
  in the runtime.
- The base contains no pipelined tree benchmark. Add a matched `tree-pipelined`
  mode beside `tree-cont`, both with fanout eight and identical verified work.

## Test evidence

[test-evidence/README.md](test-evidence/README.md) maps C1–C14 and compile/death
coverage to selectors. [mutation-summary.md](test-evidence/mutation-summary.md)
records all 18 caught mutations, with exact patches and logs beneath it.
LDC/DMD debug/release suites and deaths, plus LDC TSan, are recorded there.

The initial concurrent long soak stalled C4 at a test-only barrier. The exact
[diagnosis and fix](test-evidence/late-owner-investigation/README.md) include a
forced reproduction, full state and GDB stack: the test accidentally permitted
emitter-owned finish and then blocked that emitter waiting for its own outer
pump loop. The corrected forcing observes leaf arrival after pump return before
opening the gate, proving the producer seal is gone. It explicitly checks
consumer ownership. Runtime is unchanged. The affected four mutations were
re-run; C-M7/M7b were also caught using the dedicated emitter-seal selector.

`run_soaks.py` records the original serial >=601s full private TSan soak and
>=181s soak with 16 CPU stress processes. The verifier revision adds a >=301s
loaded soak (`--loaded-seconds 301`). Logs retain head/tail excerpts, elapsed
time and iteration counts. `verify_commits.py` verifies private LDC/DMD suites
from isolated commit archives and the evidence commit's exact source tree.

## Verifier revision

Task 147 approved the lifetime protocol. Task 148 found a C13 fixture
publication gap and missing rejection of bare nested/disposition step types.
The emitter now pumps until the competing member starts before waiting for
its return. `PrivateChildStep` rejects both ambiguous result forms, with
negative compile assertions and LDC/DMD rejection probes. The releaseRoot
comment identifies the callers that prove root context.

[Forced schedule evidence](test-evidence/force-cas-stall.md): three failures
before the fixture fix; three normal and 100 TSan invocations passed after it.
C-M12 was re-run and remains caught at the retry-completion barrier. The
[C3/C4/C5/C7/C9/C14 publication audit](test-evidence/barrier-audit.md) found no
additional emitter wait on an unpublished table. Refreshed gate results are
listed below. Long raw logs are replaced by summaries and head/tail excerpts.

## Performance protocol

`measure.py BASE CANDIDATE MIMALLOC_ARCHIVE 5` records five alternating runs,
three measured seconds and five warmup cycles per case, ordinary backing,
16,384 leaves/actors, batch 256, small-tier quota 16,384 words, mimalloc 3.5.3.
The baseline is an isolated git archive of 632c7f6. The prebuilt static mimalloc
archive is read-only in the main checkout. Both binaries use LDC `-O2 -release`.
CPU profiles: controller 0 with no remote consumers; controller 0 and six
consumers pinned 1–6. Measurements begin after correctness/stress work ends.

Unused modes: private, private-remote, tree, tree-wait, dtor, subset at 1% and
100%. Existing benchmark operations are unchanged. New tree-cont and
tree-pipelined use root → mids → result leaves, eight leaves per mid (short
last group allowed), exactly the same total leaf callbacks as existing tree.
Both verify each leaf's cycle/count and folded sums. Pipelined uses two middle
waves; continuations fold within one. Report existing tree/tree-wait alongside,
with their different root → plain leaves topology explicitly distinguished.

The user relaxed the -3% gate during erratic host load; these measurements are
provisional pending a dedicated round. Raw runs include load averages and CPU
busy fractions outside benchmark affinity. Runs with >10% average outside-CPU
activity are marked contaminated; large within-case spread is recorded too.
These indicators cannot detect every cause of interference. Only a clearly
reproduced >5% regression under quiet conditions triggers escalation.

Windows smoke remains external; production changes and positive tests retain
portable D runtime/atomic/allocator APIs. No version bump.

## Correctness gate results

- `make check`: ALL CHECKS PASSED; [head/tail excerpt](check-tip.txt).
- `make check-soak`, run twice after the C13 fix: 79 loops and 78 loops,
  each reporting 122 seconds; [first excerpt](check-soak.txt),
  [repeat excerpt](check-soak-repeat.txt), [durations/exits](revision-gates.json).
- Revised loaded private TSan soak: 65 full suite loops in
  303.236 seconds with 16 CPU stress processes, no failures or sanitizer reports;
  [head/tail excerpt](tsan-loaded-revision.txt), [counts/durations](soaks.json).
- Original serial full private TSan soak with no concurrent compilation: 387 loops in
  601.341 seconds; [head/tail excerpt](tsan-10min.txt).
- Original deliberate-load full private TSan soak: 38 loops in 181.247 seconds with
  16 CPU stress processes; [head/tail excerpt](tsan-loaded-3min.txt),
  [machine-readable durations](soaks.json).
- Four compiled documentation programs: LDC/DMD, debug/release, all passed;
  [output](doc-examples.txt).
- Benchmark builds: LDC/mimalloc and DMD/CRT passed. Both new modes passed
  verified smoke cases at 1/13/513/1025/16384 leaves, short groups, quota
  splitting and 0/6 consumers; [output](benchmark-smoke.txt).
- Rewritten runtime/docs commits and the evidence commit's exact source tree:
  all three passed LDC/DMD private suites, 102 PASS lines each;
  [archive/tree build summaries](commit-builds.txt).
- Revised tip benchmark builds: LDC/mimalloc and DMD/CRT, eight verified smoke
  cases across both modes, short groups and 0/6 consumers;
  [build/smoke output](revision-benchmark.txt).

Source/doc whitespace checks pass. Exact unified mutation patches retain their
blank context lines, which Git's generic whitespace check reports in the
stored artifacts; those lines are patch syntax, not source whitespace defects.

## E2-4 child continuations (2026-10-05)

Compared against local main `632c7f6`: 160 pinned mimalloc 3.5.3 runs, five alternating repeats per case/profile, three measured seconds and five warmups, 16,384 leaves/actors, batch 256 and quota 16,384. Controller CPU 0; remote consumers 1–6. 125/160 runs were flagged for more than 10% average CPU activity outside benchmark affinity. Large spreads are retained in the raw summary.

**The normal −3% gate is deferred under the user-approved relaxed gate.** Two medians miss −3%; the worst is −4.51%, with no unused-case median below −5%. These are provisional measurements on the noisy host, not a quiet performance acceptance claim. A dedicated round remains. Measure `subset 1%` (−4.51%) and `tree` (−3.52%) first in that quiet round.

| Unused mode | Change, 0 consumers | Change, 6 consumers |
| --- | ---: | ---: |
| `private` | +1.20% | -0.72% |
| `private-remote` | +1.12% | -1.44% |
| `tree` | -3.52% | -1.02% |
| `tree-wait` | -1.07% | -2.18% |
| `dtor` | +0.57% | -0.72% |
| `subset 1%` | -4.51% | -1.18% |
| `subset 100%` | -1.75% | +0.81% |

Both new modes use root → mids → result leaves, fanout eight, identical leaf callbacks and verified folded results. `tree-cont` folds in one root-started wave; `tree-pipelined` uses launch then fold waves. The comparator is new in E2-4 because no pipelined benchmark existed in the base. Existing `tree`/`tree-wait` remain root → plain leaves and are shown for context, with different topology/work above the leaves.

| Candidate mode | 0 consumers, Mleaf cycles/s | 6 consumers, Mleaf cycles/s |
| --- | ---: | ---: |
| `tree` | 56.018 | 45.474 |
| `tree-wait` | 54.393 | 44.254 |
| `tree-pipelined` | 22.957 | 29.352 |
| `tree-cont` | 22.653 | 29.947 |

Continuation versus pipelined: -1.32% at zero remote consumers; +2.03% at six. The differences are small relative to the host-load caveat.

Detailed rates/ranges: [summary.json](summary.json); [raw samples](runs.jsonl); [compiler, affinity and binary hashes](metadata.json).

## Final commit verification

Runtime/tests: `8afce872c266c872e537ae27c58cd58d7f985268`; API/order/docs example:
`1d3a71d1befa0b701603bee4baacc8d9160bff8a`; benchmark/evidence: this commit.
The runtime and docs revisions are verified from isolated archives. The tip's
exact source tree, including both benchmark modes, is built and tested before
committing; only evidence is recorded afterward. [commit-builds.txt](commit-builds.txt)
uses that label for the tip because recording its own SHA would change it.
This representation was explicitly approved by the orchestrator. The final
three SHAs are returned to the orchestrator for its report.

[validated-sources.json](validated-sources.json) identifies the exact build
inputs. Long soak/test logs are trimmed to head/tail summaries; their headers
retain complete-output hashes and original line counts. Mutation logs/outcomes
and performance tables/samples are retained.

The work remains on feature/private-continuations, without push or main
checkout modification. No version bump. Windows smoke and a dedicated quiet
performance round remain the explicitly external/deferred follow-ups.
