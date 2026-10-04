# Phase E1 validation — 2026-10-04

Base: `c5a174f`; version: `1.7.1-rc.1`. Validation ran on x86-64 Linux.

| Command | Result |
| --- | --- |
| `dub build --compiler=ldc2` | PASS |
| `dub build --compiler=dmd` | PASS |
| `make check` | PASS: Farm, actor and private tests; LDC/DMD; TSan; compiled documentation examples and death tests in debug/release. |
| `make check-soak` | PASS: 262 loops in 121 seconds, no failures or TSan race reports. |

The full private suites and isolated E1 selectors also passed with LDC and DMD
in debug and release. The new `tree-wait` benchmark built with LDC `-O2 -release`
and DMD `-O -release`; count-verified smoke runs passed at 0 and 2 consumers.
Windows and weak-memory hardware were not tested.

On 64-bit targets, `actors/private_actor.d` asserts `Entry.sizeof == 40`,
`Entry.alignof == 8`, and field offsets under both compilers. Root disposition,
join flag, cleanup flag and counter occupy offsets 32, 33, 34 and 36 respectively,
overlaying the child enclosing pointer at 32. The child Entry remains 40 bytes.

## Mutation checks

Each isolated, unmodified case passed before its mutation. All eight mutations
were detected. M1–M8 ran with LDC debug; M8 also ran with `-release`.

```sh
ldc2 -g -O1 -d-version=AntfarmWriteAuditHooks -d-version=AntfarmActorTestHooks \
  -I. private_actor_tests/main.d antfarm.d antfarm_allocation.d \
  actors/package.d actors/actor.d actors/wave.d actors/private_actor.d -of=mutation-test
ANTFARM_HUGE_PAGES=0 timeout 60 ./mutation-test <selector>
```

| ID | Test / selector | Mutation | Observed failure |
| --- | --- | --- | --- |
| M1 | `e1Basic` / `e1-basic` | Omit collector custody release. | Completion deadline. |
| M2 | `e1BeforeCustody` / `e1-before-custody` | Collector decrements but never enqueues on zero. | Before-custody completion deadline. |
| M3 | `e1AfterCustody` / `e1-after-custody` | Wave finish decrements but never enqueues on zero. | After-custody completion deadline. |
| M4 | `e1LateStrict` / `e1-late-strict` | Strict finish ignores outstanding join credits. | Entry not pinned past state cleanup. |
| M5 | `e1JoinedAndMixed` / `e1-joined-mixed` | Treat an empty wait as leaf finish. | Missing exactly-once later callback. |
| M6 | `e1RootReadyCasRetry` / `e1-root-ready-cas-retry` | Drop root after failed publication CAS. | Missing CAS retry. |
| M7 | `e1SealFinalOwner` / `e1-seal-final-owner` | Emitter seal never finishes the wave. | Final root credit not released. |
| M8 | `e1CounterExhaustionDeath` / `e1-counter-exhaustion-death` | Omit root-credit overflow check. | Expected overflow diagnostic absent in debug and release. |

Every mutated run aborted with SIGABRT (shell status 134, subprocess return code
−6). [Structured results](mutations/results.json) retain the exact source edits,
selectors, expected diagnostics and observed failures.
