# F1 private wave outcomes — 2026-10-06

Base: local main `20e6b6d`. Branch: `feature/private-outcomes`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push and no main-checkout modification.

## What landed

Wave failure fields, member `context.fail`, `PrivateWaveOutcome` /
`park.outcome`, start resets after active/retiring/empty rejection, and a
checked failure-count CAS. Root `fail` is fatal on an untracked park until F3.
`Wave.cancel` is stored and reset so the outcome snapshot has a stable layout;
in-tree cancellation is not implemented. Entry stays 40 bytes. On LP64, Wave is
80 bytes and a signal-only cohort is 368 bytes.

## Mutations (hand-run, then reverted)

| Test | Mutation | Result |
| --- | --- | --- |
| O1 | Code and Entry stores moved outside `if (old == 0)`; `privateFailPairSplit` left inside that branch | Re-run after the comment fix: `FAIL main.d:7201` (`app.code - 100 == app.position`). Dropping the gate around the pause as well hangs the second report in the hook and times out at `o1 second report` instead |
| O2 | `resetOutcome` removed from `start` | wave 2 still reports failures; `failures == 0` fails |
| O3 | Position taken as the spawn index | subset position 5 instead of 2 |
| O4 | `outcome` allowed while active | death message is no longer `private child outcome: wave active` |
| O5 | Plain increment instead of the raw CAS | TSan data race in `recordWaveFailure` |
| O6 | behavioural; no mutation | leaf code 42 reaches application storage |
| O7 | Tracking check removed | root `fail` exits 0 (`root fail survived`) |
| O8 | Wrapping `fetchAdd` | second report near `uint.max` exits 0 |
| O9 | `resetOutcome(count_)` moved above `start`'s empty check, after `startFinal` has zeroed `count_` | `outcomes-o9-empty` exits on `FAIL main.d:8035` (`check(app.preserved)`). The subset-empty and active-rejection placements still fail `emptyOk` / `finalOk` |

## Correctness

- `make check` at the tip: ALL CHECKS PASSED. That includes LDC and DMD private
  suites, TSan, doc examples, examples, and death tests in debug and release
  for both compilers. Commit `194b6a2` contains the runtime and tests unchanged
  by the docs commit, so those suite results apply to both commits.
- `make check-soak`: 85 TSan loops in 122 seconds, no failure.

## Performance

Five alternating mimalloc 3.5.3 runs, 3 measured seconds, 5 warmups, 16384
actors, batch 256, small quota 16384, ordinary pages. Controller pinned to CPU
0; the six-consumer profile pins consumers to CPUs 1–6. Baseline is an isolated
archive of `20e6b6d`. No run had outside-affinity busy fraction above 10%.

Worst quiet deltas are `tree-cont` at 0 consumers (−0.48%) and `tree-wait` at
6 consumers (−0.44%). Nothing approached −5%. See `summary.json`.
