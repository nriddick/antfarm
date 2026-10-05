# Forced CAS-stall schedule: before/after (revision verification)

This records the adoption of the task-148 verifier reproducer
`force_cas_stall.py` for the C13/C-M12 return-CAS barrier, after the two
parent revision fixes:

1. `actors/private_actor.d` — `PrivateChildStep(R)` now statically rejects
   `R == PrivateChildDisposition` and `R == PrivateChildStep!X`.
2. `private_actor_tests/main.d` — `contCasRetry` keeps `park.pump` running
   after A reaches its CAS until B has started (`secondStarted`) before it
   releases `secondGo` / awaits `secondReturned`.

Two negative compile assertions were added next to the existing
`PrivateChildStep!void.init` control (no other canonical test change):

```d
static assert(!__traits(compiles, PrivateChildStep!void.init));
static assert(!__traits(compiles, PrivateChildStep!(PrivateChildStep!int).init));
static assert(!__traits(compiles, PrivateChildStep!PrivateChildDisposition.init));
```

Both LDC 1.43.0 (DMD 2.113.0 front end) and DMD 2.113.0 compile the module,
which means both negations hold. The guard is the parent's runtime change;
these assertions are the regression controls for it. Independently,
external probes that instantiate each rejected type are rejected by both
compilers with the exact guard message (`result type is ambiguous with a
continuation step`); see `revision-compile-controls.log`.

## Forcing hook

`force_cas_stall.py` injects a write-audit hook that holds the emitter's
second table publication (A's initial member table) until producer A reaches its return CAS:

```d
if (phase != WriteAuditPhase.tablePublished || ++vCasPublications != 2) return;
awaitRaw(crControl.firstAtCas, 1u, "force A CAS before emitter pump returns");
```

It writes a temporary probe source `private_actor_tests/cas_stall_probe.d`,
builds it to a writable `--out` directory (default a fresh
`tempfile.mkdtemp`; task-37's parent directory is read-only), runs
`cont-return-cas-retry`, and removes the probe source in `finally`. The
canonical `private_actor_tests/main.d` is never written. Exit status is 0
only if every invocation exits 0, prints the forced marker, and (for TSan)
produces no sanitizer warning.

Run:

```sh
cd <worktree>
python3 perftest/results/private-continuations-2026-10-05/test-evidence/force_cas_stall.py \
    --out /tmp/force-cas-full --normal-runs 3 --tsan-runs 100
```

## Before (parent's original run, pre-fix test barrier)

Raw log preserved verbatim from `/tmp/e24-review-revision/force-cas-stall.log`
as `force-cas-stall-before.log`.

| Run | Forced state | Result |
|---|---|---|
| 1 | `firstAtCas=1 secondStarted=0 secondReturned=0 firstGo=0` | `FAIL cont first CAS release:0`, exit -6 |
| 2 | same | same, exit -6 |
| 3 | same | same, exit -6 |

## After (fixed tree, this revision)

`force-cas-stall.log`, `force-cas-stall-summary.json`.

Build: normal `ldc2 -g -O1 -d-version=AntfarmWriteAuditHooks
-d-version=AntfarmActorTestHooks` exit 0; TSan adds `-fsanitize=thread
-d-version=TSan`, exit 0.

| Set | Invocations | Passed | Forced marker present | Failures |
|---|---:|---:|---:|---:|
| normal | 3 | 3 | 3/3 | 0 |
| TSan | 100 | 100 | 100/100 | 0 |

The forced hook still fires with the pre-fix diagnostic state
(`firstAtCas=1 secondStarted=0 secondReturned=0 firstGo=0`), but the child
now completes with exit 0 because `contCasRetry` pumps until B starts.

## LDC/DMD/TSan private build and suite results

`revision-build.log` / `revision-suite.log` (date 2026-10-05).

Builds — all exit 0:

```sh
make -C private_actor_tests SHELL=/usr/bin/bash -B \
  private_actor_tests private_actor_tests_dmd \
  private_actor_tests_release private_actor_tests_dmd_release \
  private_actor_tests_tsan -j3
```

Suites and death harnesses — make exit 0, 200 `PASS`, 0 `FAIL`, 1 `SKIP`
(`dtor-gc`, unrelated TSan/GC limitation):

```sh
make -C private_actor_tests SHELL=/usr/bin/bash \
  run run-dmd run-tsan run-death run-death-dmd -j1
```

All three `PASS cont core, deterministic handoffs, results, admission, CAS
and depth64 both policies` lines are present (LDC debug, DMD debug, TSan).
Coverage matches the C13 row: `cont-return-cas-retry` (first member blocked
with stale expected head, second member publishes on another table, first
resumes and retries).

## Artifacts

| File | Contents |
|---|---|
| `force_cas_stall.py` | adopted forcing runner (3 normal + 100 TSan defaults) |
| `force-cas-stall.patch` | exact probe patch applied to `main.d` text |
| `force-cas-stall-before.log` | pre-fix run, verbatim from parent's `/tmp` log |
| `force-cas-stall.log` | post-fix log, head/tail + summary (no bulk raw log) |
| `force-cas-stall-summary.json` | post-fix build/run summary, all exit codes |
| `revision-build.log` | five binary builds (LDC/DMD debug+release, TSan) |
| `revision-suite.log` | private suites + four death harnesses |
| `revision-compile-controls.log` | external probes rejected by LDC and DMD |

Task-148 artifacts were read only and are unchanged.
