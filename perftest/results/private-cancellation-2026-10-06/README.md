# F2 private in-tree cancellation — 2026-10-06

Base: local main `2b1eac7`. Branch: `feature/private-cancellation`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push and no main-checkout modification.

## What landed

`ChildBase.root`, the sticky root `subtreeCancel` byte at Entry offset 35
(static-asserted), `scopeCancelled`, `context.cancelled`, `cancelChildren`,
`cancelPeers` and `park.cancel`. The emitter checks the scope before each
initial table. It stores `dispatched_` before sealing a non-retiring wave
(R3); a `startFinal` wave retires the rest through cleanup tables that use a
local header copy, so resume tables still run the op (R5). `results` and every
debug unwritten-slot check use `min(resultCount_, dispatched_)`. Entry stays
40 bytes; on LP64 a signal-only cohort is 376 bytes (ChildBase 168). Root
`cancelled` is false until root tracking (F3).

## Process

1. **Spike** in this worktree: runtime first, then C1–C13 and the torture.
   The existing suite passed unchanged apart from the cohort size assertion.
2. **Independent lifetime review** of R1–R5 and R13 (plus byte 35 and general
   credit checks): [lifetime-review.md](lifetime-review.md). Verdict PASS, no
   code change. Its two comment items were applied and its missing
   ACTOR_MEMORYORDER section was written before commit.
3. **Mutations:** every C-test mutation was applied alone and caught; see
   [the test README](../../../private_actor_tests/README.md#in-tree-cancellation-f2),
   [mutate.py](mutate.py) and [mutations.json](mutations.json). C7 first failed
   on its outcome check (`dispatched` still 4 when the root read it), so the test
   was reordered to check the quarantine poison first. The final run of all
   fourteen mutations, against the committed tests, matches the README table;
   C7 fails `c7 freed memory untouched`.
4. **TSan soak:** see below.

## Test deviations from design §12.2

- C1 and C13 fail differently by build: the debug unwritten-slot check fires
  first (`private child result: unwritten slot`); release builds fail
  `c1 truncated outcome` / `c13 prefix survives retirement`.
- A stale `subtreeCancel` only costs the slow path, so it has no functional
  symptom. C5 therefore reads the byte through the test-only seam
  `privateTestSubtreeCancel`.
- The design names C8's and C9's mutations only by effect. The concrete ones
  are: C8, `releaseRoot(root)` in `cancelPeers` when the wave's parent is the
  root (early wake); C9, re-evaluating truncation at the normal seal. C9 needed
  an extra hook point, `privateEmitBeforeSeal`, before the normal seals.
- C11's failing parent is a mid-level member in a normal (non-retiring) wave:
  `createChildren` returns null in a retiring context.
- C1 gets its schedule from the lockstep consumer; the
  `privateEmitBeforeCancelCheck` hook is used by C7.

## Deferred to F3

- The `records` branch in `scopeCancelled`: a root context's `cancelled`
  returns false until root tracking exists.
- C10's root-outcome half: the tracked `failed`/`cancelled` root status after
  `cancelChildren` + `finishAfterChildren`. Root `fail` is still fatal on an
  untracked park.

## Correctness

- `make check` at the tree that became the three code/doc commits: ALL CHECKS
  PASSED ([check-tip.txt](check-tip.txt)). That covers dub LDC/DMD, Farm TSan,
  actor torture LDC/DMD/TSan, review torture, the private suite on LDC, DMD and
  TSan, the four compiled doc programs, the examples lane, and death tests in
  debug and release for both compilers.
- Commit `30daf78` holds the runtime and tests byte-identical to the tip; its
  archived tree was rebuilt and the full private suite passed on LDC and DMD.
  `188a701` changes docs only; `e6245c0` only `perftest/actor_churn.d`.
- `make check-soak`: 83 TSan loops in 122 seconds ([check-soak.txt](check-soak.txt)).
- 10-minute TSan soaks, run concurrently ([run_soaks.py](run_soaks.py),
  [soaks.json](soaks.json)): the full private TSan suite, 354 loops in 601.6 s,
  and `cancel-torture 300 <seed>` with a fresh seed per loop, 17,666 loops in
  601.0 s. No failures. Each torture loop builds about 6,000 actors with
  hundreds of cancelled and truncated joined waves.

## Performance

[measure.py](measure.py): five alternating mimalloc 3.5.3 runs, 3 measured
seconds, 5 warmups, 16384 actors, batch 256, small quota 16384, ordinary pages.
Controller pinned to CPU 0; the six-consumer profile pins consumers to CPUs
1–6. Base is an archive of `2b1eac7` built with the same flags. Runs with more
than 10% busy time outside the benchmark affinity are marked contaminated
(13 of 100 comparison runs).

| Mode | 0 consumers, quiet delta | 6 consumers, quiet delta |
| --- | --- | --- |
| private | −0.34% | +0.23% |
| private-remote | +1.25% | +0.15% |
| tree | +0.47% | −1.11% |
| tree-wait | +0.31% | +0.16% |
| tree-cont | −0.28% | −1.81% (n=3) |

The all-runs `tree`/`tree-wait` deltas at 0 consumers (about −3.3%) come from
two contaminated runs each. No quiet delta approaches −5%.

New `tree-cancel` baseline (candidate only): 52.65 Mactor_cycles/s with inline
consumption (dispatched fraction 0.617) and 56.65 with six consumers (0.923).
Compare future `tree-cancel` runs on the 0-consumer figure: with six
consumers the cancel usually lands after most tables are published, so about
92% of leaves run the operation and the mode measures little cancellation.
See [PRIVATE_LIFECYCLE.md](../../PRIVATE_LIFECYCLE.md#f2-tree-cancel-baseline).
