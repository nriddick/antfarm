# E2-2 subset-wave tests: mutation evidence

Tests: `private_actor_tests/main.d` (committed in the tests slice).
Runtime snapshot tested: `actors/private_actor.d` sha256 `a066c67d4f41667ec013ca4eb4bad8e484197ed5ad0e41c391b054832a5f0754`
(unchanged at the end of the run; the shared worktree runtime was never mutated).

## Method
Mutations were applied to a private copy of the tree at `/tmp/e2-2-mut` (script `/tmp/e2-2-mut/run_mut.py`,
full log `/tmp/e2-2-mut/all.log`), never to the shared worktree, so no handoff with the parent was needed.
For each mutation: restore the pristine runtime, apply one text edit (the script asserts the anchor exists),
rebuild the LDC debug binary (TSan binary for Q-M23), run the named selector(s), then restore the runtime.
Every mutation below was CAUGHT (selector aborts or exits nonzero); the unmutated runtime passes all of them.

| ID | Mutation (edit to actors/private_actor.d) | Selector | Observed failure |
|---|---|---|---|
| Q-M1 | subset runChild: `results_[entry - entries_]` instead of inverse position | subset-basic | FAIL subset basic sparse |
| Q-M2 | `resultCount_` stored before the empty-list return | subset-basic | FAIL subset basic whole results length |
| Q-M3 | `start` omits `isSubset_ = false` | subset-basic | FAIL subset void whole resets members |
| Q-M3b | `start` omits `wave.emit = &emit` | subset-basic | FAIL subset void whole ran everyone |
| Q-M4 | empty list sets `wave.active` before returning | subset-basic | FAIL subset basic fresh empty takes no credit |
| Q-M5 | copy `sel_[j] = i` removed | subset-basic | fatal: duplicate member (zeroed selection) |
| Q-M6 | startSubset omits `retire_ = false` | subset-basic | FAIL subset basic refill whole |
| Q-M7 | duplicate check without `sel_[p] == i` confirmation | subset-basic | false fatal: duplicate member |
| Q-M8a | active check moved below validation/copy loop | subset-active | fatal: duplicate member (list [2,2] while active) |
| Q-M8b | `isSubset_ = true` before active check | subset-active | FAIL subset active whole keeps no members |
| Q-M8c | `wave.emit = &emitSubset` before active check | subset-active | FAIL subset active whole wave results |
| Q-M8d | `k_` stored before active check | subset-active | FAIL subset active members unchanged |
| Q-M11 | emitSubset slices `entries_` instead of `sel_` | subset-large, subset-basic | FAIL subset large partial prefix is the listed prefix; FAIL subset void exactly the selected members ran |
| Q-M12 | emitSubset `cursor_ += n` instead of `written` | subset-large | FAIL subset large partial prefix is the listed prefix |
| Q-M13 | emitSubset without `if (written == 0) releaseWave` | subset-large | FAIL subset large zero admission credit released |
| Q-M14 | subset runChild slot `results_[cursor_ % capacity_]` | subset-large | FAIL subset large sparse |
| Q-M15 | startSubset omits upstream `pending` increment | subset-nested | FAIL subset nested leaves ran after the join; FAIL subset nested root resumed before the nested subset joined |
| Q-M16 | dispose omits the `sel_` free | subset-alloc-fail | FAIL subset alloc total allocation balance |
| Q-M17 | sel_ failure path omits freeing `results_` | subset-alloc-fail | FAIL subset alloc failed create balance |
| Q-M17b | sel_ failure path omits freeing `entries_` | subset-alloc-fail | FAIL subset alloc failed create balance |
| Q-M18 | extra (separate) reserve allocation | subset-alloc-fail | FAIL subset alloc reserved void cohort makes three allocations |
| Q-M19 | `capacity > uint.max` check removed | subset-alloc-fail | FAIL subset alloc uint.max capacity allocates nothing |
| Q-M20 | startSubset sets `retire_ = true` | subset-dtor | FAIL subset dtor wave destroys nobody |
| Q-M22 | dispose walks only `k_` entries after a subset | subset-dtor | FAIL subset dtor unselected member destroyed once |
| Q-M23a | finish reads `k_` after idle store, before hook (TSan binary) | subset-late-tail | TSan: data race (finish read vs startSubset write of k_) |
| Q-M23b | finish reads `sel_[0]` after hook (TSan binary) | subset-late-tail | TSan: heap-use-after-free in finish |
| Q-M24 | options overload ignores `options.batchLimit` | subset-limits | FAIL explicit child table exceeds limit 7 |
| Q-M25 | options overload does not inherit on batchLimit 0 | subset-limits | FAIL inherited subset table count |

Death modes (each of the 11 selectors run individually against each mutation; unmutated: all die with the exact text):

| ID | Mutation | Result |
|---|---|---|
| Q-M26 | duplicate check removed | subset-dup-adjacent / -gap die with "retire private actor with active child wave" (wrong text) |
| Q-M27 | range check `i >= capacity_` | subset-range dies with "retire private actor with active child wave" (wrong text) |
| Q-M28 | range check removed | subset-range: wrong text; subset-range-max: SIGSEGV |
| Q-M29 | too-many check removed | subset-too-many dies with "duplicate member" (wrong text) |
| Q-M30 | `sel_ is null` fatal removed | subset-unreserved, -options: SIGSEGV; subset-unreserved-empty: survives ("subset misuse: survived", rc 0) |
| Q-M31a | `authorized` removed from startSubset | subset-wrong-parent, subset-default-context survive (rc 0) |
| Q-M31b | `authorized` removed from subsetMembers | subset-members-wrong-parent survives (rc 0) |

The deathHarness (`subsetDeath`) fails on any of these (nonzero status + exact text + no survivor marker).

## Unmutated results
- `make SHELL=/usr/bin/bash run run-dmd run-tsan death` (private_actor_tests): rc 0, 188 PASS lines, no TSan reports.
  Death harness ran on LDC and DMD, debug and -release.
- Stress: each new selector 15x on LDC and DMD (0 failures); each 5x on TSan; `phase-subset` 40x on TSan, 60x on each
  of LDC and DMD (0 failures).
- Tests deliberately not covered (out of scope): continuations, subset retirement, startSubset on a retiring context.
- Deviation noted: `subset-active` runs single-threaded (no Workers): with a worker consumer the wave can be emitted and
  finish inside the starting callback, so "active" is only guaranteed with one emitter and no other consumer
  (same reason ports-read-active is single-threaded).

## Mutation count (correction)
Exact distinct mutations run: 35 (28 runtime/selector mutations + 7 death-mode mutations Q-M26..Q-M31b),
matching `run_mut.py` (35 `mut(...)` entries) and `all.log` (35 `=====` headers). An earlier report said "30 + 7 = 37";
that was a miscount. No rows are missing from the tables above.

Parent review replaced the sbSame helper with D built-in uint slice equality and
updated API comments and the cohort-size comment. Runtime behavior is unchanged
from the mutation snapshot; the final compiler matrix and soak cover the reviewed code.
