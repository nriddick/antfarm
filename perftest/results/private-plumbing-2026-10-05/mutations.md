# E2-3 plumbing mutation evidence

Tests: `private_actor_tests/main.d`, selector `entry-word` (debug and
-release builds). Mutations applied to `actors/private_actor.d` in the
worktree, one at a time, restored with `git checkout --` after each
(working tree verified clean afterward; the committed runtime was never
mutated). The committed test comments document these as E3-M1..E3-M3.

| ID | Mutation (edit to actors/private_actor.d) | Build | Observed failure |
|---|---|---|---|
| E3-M1 | `PrivateContext.service`: `rootService(actor_)` for members too (checked read on a child) | LDC -g -O1 (asserts on) | fatal: `antfarm fatal: private entry: service read on a child`, exit 134 (`entry-word`) |
| E3-M1 | same | LDC -g -O1 -release | `FAIL entry word mid derives the root's service:0`, exit 134 |
| E3-M1-raw | `PrivateContext.service`: raw `actor_.service` for members too (raw child read, bypasses the accessor) | LDC -g -O1 | `FAIL entry word mid derives the root's service:0`, exit 134 |
| E3-M1-raw | same | LDC -g -O1 -release | `FAIL entry word mid derives the root's service:0`, exit 134 |
| E3-M2 | `PrivateChildPark.dispose`: `rootService(base.parent)` instead of `base.service` (nested cohort's parent is a member) | LDC -g -O1 (asserts on) | fatal: `antfarm fatal: private entry: service read on a child`, exit 134 (`entry-word`) |

The committed death test `entry-word-death` additionally proves, on every
`make check`, that a debug-build child `rootService` read is fatal with the
exact stderr text, and `death-negative-control` proves the harness rejects
survivors and wrong-text deaths.

## entry.service audit (against the Entry layout table)

All remaining reads of `Entry.service` at the E2-3 tip are root-only and go
through `rootService(entry)`: `PrivateContext.service` root branch,
`releaseRoot` (rootReturns), and `PrivateRootPark.runRoot` (batchLimit and
the consumer-side free; one read feeding both). The single write is the
root-only `PrivateRootPark.create` initialization. Child sites use
`ChildBase.service`. Recorded in the commit message of "Route Entry.service
reads through a checked rootService accessor" and in
actors/PRIVATE_ACTORS.md "Entry layout and Service access (internal)".
