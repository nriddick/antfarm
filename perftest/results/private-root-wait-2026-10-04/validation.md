# Phase E1 validation — 2026-10-04

Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
Branch: `feature/private-root-wait`; base: `c5a174f`. No push or main-checkout edit.
Version remains `1.7.1-rc.1`.

## Build and correctness evidence

| Commit/change | LDC and DMD verification |
| --- | --- |
| `b0640a7`, packed Entry preparation | `dub build --compiler=ldc2` and `dub build --compiler=dmd` passed. |
| `dd9ae9c`, root joins and prototype cases | Full private suites passed under both compilers. |
| `cef86c9`, hardening cases | Full private suites passed in debug/release, both compilers; reviewed CAS/seal cleanup rebuilt and focused cases passed. |
| `dd2b300`, API and ordering docs | Both complete documentation programs compiled and ran under both compilers, debug/release. The conductor uses `waitChildren` and reads joined results without `finished`. |
| Tree-wait benchmark mode | LDC `-O2 -release` and DMD `-O -release` builds passed; count-verified `tree-wait` smoke runs passed at 0 and 2 consumers. |

`make check` ended **ALL CHECKS PASSED**. It covers Farm LDC/DMD tests;
Farm, actor, review and private TSan; private LDC/DMD tests; both complete doc
programs in debug/release with both compilers; death tests and negative controls
in debug/release with both compilers. The existing strict-finish-with-active-child
death remains fatal. `make check-soak` completed **262 loops in 121 seconds**, with
no failures or TSan race reports. The test subtask additionally ran full private
release suites and isolated E1 selectors in all four compiler/build combinations.

The prototype tests were adopted before runtime changes. The pre-fix LDC build
failed because the base lacks the new dispositions and E1 hooks; this is an API
compilation failure, not a pre-existing runtime bug reproduction. Runtime fault
detection was checked by the eight isolated mutations below.

`Entry.sizeof == 40`, `Entry.alignof == 8`, and all field offsets are static
asserts in `actors/private_actor.d`, compiled by both compilers. The root fields
occupy offsets 32 (disposition), 33 (join flag), 34 (cleanup flag), and 36 (uint
counter), overlaying the child enclosing pointer at 32. Child storage is unchanged.
Only the new 32-bit root retain is checked; existing ulong wave fetch-adds and
all their memory orders remain unchanged.

Detailed local logs remain under `.e1-evidence/`: `pre-fix.log`,
`layout-{ldc,dmd}.log`, `core-tests.log`, `tests-reviewed-build.log`,
`doc-examples.log`, `check.log`, `soak.log`, `perftest-*-build.log`, and the test
subtask's `logs/` directory. `validated-sources.json` records the checked source
hashes. These are worktree artifacts, not required source files.

## Limits and deviations

The original symmetric ±3% gate was escalated when polling tree at one consumer
measured +8.73%. The orchestrator clarified that favourable differences do not
fail the no-regression gate, required another five alternating pairs, and kept
negative deviations below −3% as a hard stop. The repeat was +3.61%; both rounds
and the 7.79 M/s initial base outlier are retained. All measured regressions are
within −3%. Count/source sanity checks found no skipped work across all 210 runs.

The approved packed-counter option was used instead of growing every Entry or
allocating separate root headers. The existing conductor example was converted
to a compiled wait example, retaining the doc extractor's two-program contract.
No new production API beyond the two dispositions was added. Test-only seams are
excluded unless `AntfarmActorTestHooks` is enabled. An unused test field and a
one-use polling helper were removed during self-review; the focused cases were
rebuilt and rerun afterward.

Windows execution and weak-memory hardware were not available. Windows remains
the user's manual lane; new thread/atomic code is portable and the exhaustion
subprocess follows the existing explicit Linux-only death-test skip. The local
mimalloc submodule is absent, so performance uses the approved CRT fallback.
The task parent directory is read-only, so temporary evidence is inside this
worktree. `/bin/sh` is absent; child makes use `SHELL=/usr/bin/bash`.

## Hand-run mutation results

Run by `.e1-evidence/run_mutations.py` against the restored worktree sources (date 2026-10-04).
For each mutation the script copies the sources to `.e1-evidence/mutations/<id>/src` (the worktree is never mutated),
builds and runs the UNMUTATED copy first (`baseline_*.log`, must exit 0), applies the single change (`mutation.diff`),
rebuilds and runs the focused selector (`mutant_*.log`).

- Build: `SHELL=/usr/bin/bash ldc2 -g -O1 [-release] -d-version=AntfarmWriteAuditHooks -d-version=AntfarmActorTestHooks -I. private_actor_tests/main.d antfarm.d antfarm_allocation.d actors/package.d actors/actor.d actors/wave.d actors/private_actor.d -of=<binary>`
- Run: `ANTFARM_HUGE_PAGES=0 timeout 60 ./<binary> <selector>`; a failed check calls `abort()` (SIGABRT, shell status 134; the Python harness reports -6 because `timeout` re-raises the signal).
- Compiler: LDC (debug for M1-M8; additionally `-release` for M8, the death test).
- M1-M3 are caught by several tests; the table records the named test failing in isolation via its own selector.

| ID | Test | Selector | Mutation | Expected failure | Actual |
| --- | --- | --- | --- | --- | --- |
| E1-M1 | `e1Basic` | `e1-basic` | Collector never releases root custody (`releaseRoot(entry)` in `collectCompletedImpl` deleted). | `FAIL E1 completion deadline:0` | debug: exit 134, `FAIL E1 completion deadline:0` |
| E1-M2 | `e1BeforeCustody` | `e1-before-custody` | Collector decrement that never enqueues on zero (`releaseRoot(entry)` replaced by a bare `atomicFetchSub` of `pending`). | `FAIL E1 before custody completion:0` | debug: exit 134, `FAIL E1 before custody completion:0` |
| E1-M3 | `e1AfterCustody` | `e1-after-custody` | Wave-finish decrement never enqueues on zero (`releaseRoot(parent)` in `PrivateChildPark.finish` replaced by a bare `atomicFetchSub`). | `FAIL E1 after custody completion:0` | debug: exit 134, `FAIL E1 after custody completion:0` |
| E1-M4 | `e1LateStrict` | `e1-late-strict` | Strict finish ignores outstanding join credits (`entry.hasJoinCredits ||` deleted from the collector branch), so the Entry is reclaimed under the paused notification tail. | `FAIL E1 strict Entry pinned past state cleanup:0` | debug: exit 134, `FAIL E1 strict Entry pinned past state cleanup:0` |
| E1-M5 | `e1JoinedAndMixed` | `e1-joined-mixed` | `waitChildren` with no active waves is treated as a leaf finish (`\|\| entry.disposition != finish` deleted), so no later callback occurs. | `FAIL E1 already joined wait exactly one later call:0` | debug: exit 134, `FAIL E1 already joined wait exactly one later call:0` |
| E1-M6 | `e1RootReadyCasRetry` | `e1-root-ready-cas-retry` | Failed root-ready publication CAS drops the root instead of reloading the head and retrying (reload replaced by `return`). | `FAIL E1 CAS exactly one retry:0` | debug: exit 134, `FAIL E1 CAS exactly one retry:0` |
| E1-M7 | `e1SealFinalOwner` | `e1-seal-final-owner` | Emitter seal in `PrivateChildPark.emit` is a bare `atomicFetchSub` of `wave.pending`, so the seal never finishes the wave or wakes the root. | `FAIL E1 seal emitter released the final root credit:0` | debug: exit 134, `FAIL E1 seal emitter released the final root credit:0` |
| E1-M8 | `e1CounterExhaustionDeath` | `e1-counter-exhaustion-death` | Overflow check deleted from `retainRootCredit`; counter wraps from uint.max to 0. | check at private_actor_tests/main.d:2334 (`antfarm fatal: private join credit overflow` absent); the mutant child dies with `antfarm fatal: private root credit underflow` | debug: exit 134, `FAIL private_actor_tests/main.d:2334`; release: exit 134, `FAIL private_actor_tests/main.d:2334` |

Logs: `.e1-evidence/mutations/<id>/{baseline,mutant}_{debug,release}.log`; diffs: `.e1-evidence/mutations/<id>/mutation.diff`.

Restored-tree verification logs are in `.e1-evidence/logs/` (`selectors.log`, `full-*.log`, `death.log`, `tsan-*.log`).
