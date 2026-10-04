# Typed result ports (E2 slice 1) validation

Base: `374909c`; version unchanged at `1.7.1-rc.1`. Linux x86-64, LDC 1.43.0,
DMD 2.113.0. Windows execution remains the user's manual check.

## Mutation checks

All P-M1–P-M13 were applied by hand to `actors/private_actor.d`, checked with
the named selector, and restored before the next mutation. The structured
[mutation records](mutations/results.json) retain exact source substitutions,
builds, commands and concise observed diagnostics. Test comments name each
mutation and its observed failure. Existing E1 tests remain unchanged.

| Mutation | Selector | Observed failure |
| --- | --- | --- |
| P-M1 | `ports-basic` | Spawn alone produces no results / final results survive retirement / void clears. |
| P-M2 | `ports-basic` | Void wave clears results. |
| P-M3 | `ports-basic` | Empty start keeps results. |
| P-M4 | `ports-basic` | Allocation balance. |
| P-M5 | `ports-poll-visibility` | TSan data race in 5/5 runs after both idle acquires become raw. |
| P-M6 | `ports-wait-visibility` | TSan slot-write race in 4/4 runs after a post-enqueue reset. |
| P-M7 | `ports-late-tail` | TSan race before the hook, heap-use-after-free after the hook, each 4/4. |
| P-M8 | `ports-large-cohort` | Generation/index tag for cursor or table-position slot indexing. |
| P-M9 | `ports-large-cohort` | Generation/index tag after duplicated partial-prefix dispatch. |
| P-M10 | `ports-nested` | Retiring context rejected / L2 fold length. |
| P-M11 | `ports-alloc-fail` | Failed-create allocation balance. |
| P-M12 | `ports-death` | Active-read survivor rejected in all four compiler/debug-release binaries. |
| P-M13 | `ports-death` | Unauthorized-read survivor rejected in all four binaries. |

P-M5/P-M6/P-M7 are ordering defects detected by TSan. Plain builds pass them.
P-M6 did not also produce the design's predicted tag mismatch; the orchestrator
accepted TSan-only evidence. P-M7 uses a real `resultCount_ = count_` write;
a self-assignment is optimized away and is not a valid mutant. P-M1's literal
live-count accessor fails before the final-length assertion; an additional
retirement-clear variant catches that assertion. E1 collector and wave-wake
mutations also fail `ports-wait-visibility`.

T8 static assertions reject non-POD R, invalid operation returns, R-producing
operations on void cohorts, and exposing `results` on void cohorts. Additional
hand mutations of constraints and layout failed compilation. Void cohorts retain
the existing size; typed cohorts add two words. Entry remains 40 bytes, checked
in the core and through allocator request sizes.

The large-cohort tests cover 257, 513 and 1025 members, limits 1/64/256, zero
admission, partial-prefix retry and four waves. TSan omits 513/1025 at limit 1
for runtime, retaining limit 64 coverage. Only limit 256 forces a strict partial
prefix with the default small quota. The nested test runs on five workers and
covers pipelined reductions, retiring reads and a separate depth-64 hierarchy.

## Test-author verification

- `make check`: PASS, including LDC/DMD, TSan, debug/release death tests and docs.
- `make check-soak`: PASS, 193 loops in 121 seconds, zero failures.
- Ports selectors repeated 40 times on TSan and 30 each on LDC/DMD: PASS.
- A test-side counter race found during T1 verification was fixed with an atomic
  increment before the passing runs.

## Performance protocol

CRT allocator (mimalloc submodule unavailable), huge pages disabled, 16384
actors, batch 256, small quota 16384 words. Five warmup cycles and five
repetitions with three-second measurement windows. Baseline and candidate
alternate within every configuration, reversing order on odd repetitions.
Controller pinned to CPU 0; consumers pinned to CPUs 1–6 for the six-consumer
cases. The baseline is built from an isolated archive of `374909c`; the main
checkout is untouched. [Metadata](metadata.json), [runs](runs.jsonl) and
[summary](summary.json) record provenance and measurements.

The optional `tree-results` benchmark mode was omitted, as permitted by
`ports.md` and confirmed by the orchestrator. The gate measures unused
`R = void` in all private modes and public actor/wave controls.

Gate result: **PASS** in all 12 configurations (120 successful measurements).
Median changes range from −1.47% to +2.47%; no gain exceeds +3%.
See the table in [PRIVATE_LIFECYCLE.md](../../PRIVATE_LIFECYCLE.md).


## Final executor verification

| Command / snapshot | Result |
| --- | --- |
| Implementation-only snapshot: `make -C private_actor_tests private_actor_tests private_actor_tests_dmd SHELL=/usr/bin/bash` | PASS with LDC and DMD, using the base tests. |
| Final tree: `make check` | PASS: `ALL CHECKS PASSED`; LDC/DMD, Farm/public/private suites, TSan, both compiled docs, debug/release death tests and negative controls. |
| Final tree: `make check-soak` | PASS: 195 loops in 121 seconds, zero failures or TSan reports. |
| Evidence checks | PASS: all P-M1–P-M13 variants, 120 performance runs, every median ≥ −3%; reproduction script parses. |
| `git diff --check` | PASS. |

The implementation, tests and docs snapshots build with both compilers. The
final evidence commit changes benchmark artifacts and documentation only.
No version bump, push or main-checkout modification was made. The branch is
`feature/private-result-ports`; local main remains `374909c`.
