# F3 private root tracking, X1a and lazy deadlines — 2026-10-06

Base: local main `c8b4bf2`. Branch: `feature/private-root-tracking`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
No push and no main-checkout modification.

## What landed

`PrivateRootOptions.tracking` gives each root slot a 32-byte `RootRecord`,
parallel to the park's Entries (`Service.records`/`rootEntries`). Its control
word, `generation:48 | cause:8 | flags:8`, carries the R7/R8 protocol.
Foreign-thread `PrivateRootHandle.requestCancel` CASes only from a live word
of its own generation. The emitter's plain release stores happen only from
non-live words. The terminal collection CAS decides
`failed > cancelled > completed`. `takeOutcome` bumps the generation and is
fatal at 2^48−1.

Also landed:
- `cancelAll` and lazily observed `MonoTime` deadlines set the same bit.
  Only armed roots read the clock.
- Root `fail` records the first code.
- Terminal roots hold their slot in an emitter-local outcome FIFO (through
  `Entry.next`) until `takeOutcome`, so `live` falls there.
- `scopeCancelled` and a root context's `cancelled` read the record. These
  were the F2 leftovers.
- C10 gains its tracked half, and O6 ends in `takeOutcome`.

Entry stays 40 bytes and the cohort stays 376 bytes. The untracked root park
grows by 32 bytes: `Service.records`/`rootEntries` plus the outcome head and
tail.

## Process

1. **Implementation and tests** in this worktree: T1–T9, the C10 and O6
   extensions, the tracked torture, and death tests for the tracking-only API.
2. **Consultant review of design §5.3** against the implementation:
   [control-word-review.md](control-word-review.md). Verdict: PASS with
   changes, no protocol change. All changes were applied before commit:
   - debug assertions that the terminal CAS and the in-tree check see a live
     word, and that take sees a non-live one;
   - a CAS-based generation test seam;
   - doc statements of the canceller release guarantee and of the rule to
     join cancellers before `destroy`;
   - corrected untracked-cost wording (spawn also gains a branch; the park
     grows by 32 bytes);
   - an armed, unexpired deadline in the new benchmark.
3. **Mutations:** each T-mutation was applied alone to a copy of the sources
   and built with LDC in debug and `-release`. Every one is caught in both
   builds; see [the test README](../../../private_actor_tests/README.md#root-tracking-f3),
   [mutate.py](mutate.py) and [mutations.json](mutations.json). The final run
   is against the committed tests and runtime. T1b first tripped an earlier
   handle-inequality check, so T1 was reordered to reach the stale CAS (ABA)
   that the design names.
4. **Soak and perf:** see below. The first perf pass found a quiet
   untracked regression worse than −5%. It was escalated, bisected and fixed,
   and the fix is folded into the code commit.

## Test deviations from design §12.3

- T2 drives order 2 with a pause at `privateTakeBeforeVacantStore`. The
  canceller sees FINISHED and gets `finished`, then `staleHandle` after the
  take. This also covers the reviewer's suggested stale-handle test across
  that hook.
- T5's destroy death test is the `root-tracking-misuse:destroy-untaken` child.
- T7 runs inline (one thread), so the states at `cancelAll` are exact. It also
  checks that `cancelAll` skips held outcomes, records cause `deadline` for an
  expired armed root (T7d), counts only live unfinished roots (T7e), and
  enforces failed > cancelled and first-code-wins (T7b, T7c). T7a, "recipe
  without `takeOutcome`", mutates the test's drain loop; the run also checks
  directly that pumping alone leaves `live` at 1000.
- T9's seam seeds 2^48−2: one take succeeds, then the next generation's take
  is fatal, which pins the exact bound.
- `cancelAll` observes deadlines, as design §5.3 lists it as an observer. It
  reads the clock at most once per call, and counts deadline conversions in
  its return value.
- `requestCancel` on a default handle returns `staleHandle`, like
  `ActorHandle`. `index` on a default handle is fatal.

## Correctness

- **`make check`** at the final tip `fd4cfdf`: ALL CHECKS PASSED
  ([check-tip.txt](check-tip.txt)). It covers dub LDC/DMD, Farm TSan, actor
  torture LDC/DMD/TSan, review torture, the private suite on LDC, DMD and
  TSan, the five compiled doc programs (now including the tracked shutdown
  program), the examples lane, and death tests in debug and release on both
  compilers.
- **The code commit's own tree:** `43a9bfe` was archived and rebuilt, and the
  private suite passed on LDC and DMD, with doc examples and death tests
  (debug and release, both compilers). `7da4b63` changes only docs and the
  doc-example extractor; `fd4cfdf` changes only `perftest/actor_churn.d`.
- **`make check-soak`** at the final tip: 75 TSan loops in 121 seconds
  ([check-soak.txt](check-soak.txt)).
- **10-minute TSan soaks**, run concurrently ([run_soaks.py](run_soaks.py),
  [soaks.json](soaks.json)). The full private TSan suite ran 291 loops in
  602.0 s. `tracked-torture 300 <seed>`, with a fresh seed per loop, ran
  17,035 loops in 601.0 s. No failures. They ran on the pre-fix code
  (`soaks.json` records head `e6b12b9`). The perf fix changes no atomic and
  no ordering: it moves the tracked tails out of line and replaces the
  untracked `records` load in `scopeCancelled` with an immutable per-cohort
  flag set before the cohort's first publication. The orchestrator therefore
  waived a repeat; `check-soak` and the full TSan matrix were re-run on the
  fixed tip.

## Performance

[measure.py](measure.py) runs five alternating mimalloc 3.5.3 runs per cell:
3 measured seconds, 5 warmups, 16384 actors, batch 256, small quota 16384,
ordinary pages. The controller is pinned to CPU 0; the six-consumer profile
pins consumers to CPUs 1–6. Base is an archive of `c8b4bf2` built with the
same flags. A run is marked contaminated when the mean busy time outside the
benchmark affinity exceeds 10%.

**Pass 1** (`e6b12b9`): all 120 comparison runs were contaminated by an
external load on CPUs 6–7. It is discarded:
[perf-pass1.txt](perf-pass1.txt),
[runs-pass1-contaminated.jsonl](runs-pass1-contaminated.jsonl).

**Pass 2** (`e6b12b9`, before the fix): the quiet deltas were private
−6.4%/−7.8%, private-remote −6.3%/+3.9% and tree-cont −2.6%/−9.9% (0/6
consumers). See [perf-pass2-prefix.txt](perf-pass2-prefix.txt),
[runs-pass2-prefix.jsonl](runs-pass2-prefix.jsonl) and
[summary-pass2-prefix.json](summary-pass2-prefix.json). This was escalated.

**Diagnostics** ([diag.py](diag.py), [variants.py](variants.py), raw logs
`diag-*.json`) used 2–3 quiet alternating repetitions per cell:

- The new runtime built with the base `actor_churn.d` matched the candidate,
  so the harness change was not the cause.
- The base runtime with only the new, unused fields gave tree/6 +10.4%,
  private/6 +1.3% and tree-cont/6 −3.4%. Layout therefore explains the large
  positive tree deltas, but not the private and tree-cont losses.
- Forcing the `spawnRoot` helper inline, or moving `Service.records` to
  another cache line, changed nothing.
- Bisect, removing one hunk at a time ([diag-bisect.json](diag-bisect.json)):
  - without the spawn tracked block, private/0 recovered fully (−0.2%);
  - without the `scopeCancelled` records branch, tree-cont/6 recovered to
    −3.8%, the layout level;
  - the reclaim branch was minor.
- Fix 1 moved the tracked spawn tail, the terminal outcome path and
  `rootCancelledNow` into `pragma(inline, false)` functions behind one null
  branch. That recovered private (−3.2%/−2.9%) but not tree-cont/6 (−8.6%)
  ([diag-fix1.json](diag-fix1.json)).
- Fix 2 also replaced `base.service.records !is null` in `scopeCancelled`
  with an immutable `ChildBase.tracked` flag in existing padding. Service's
  two lines are written by consumers (`incoming`, `childReturns`,
  `rootReturns`), so the emitter's per-table load of `records` missed. The
  flag sits beside `resumeScheduled`, on a line `emit` already reads. With it,
  tree-cont/6 was +3.4% ([diag-fix2.json](diag-fix2.json); `diag-bins.json`
  lists the binaries of that run).

**Pass 3** (final tip, fix folded into `43a9bfe`): 0 of 120 comparison runs
contaminated. [perf-pass3.txt](perf-pass3.txt), [runs.jsonl](runs.jsonl),
[summary.json](summary.json), [metadata.json](metadata.json).

| Mode | 0 consumers, quiet delta | 6 consumers, quiet delta |
| --- | --- | --- |
| private | −4.17% | −2.08% |
| private-remote | −5.00% (−4.999%) | +8.75% |
| tree | −0.70% | +12.83% |
| tree-wait | +3.73% | +13.50% |
| tree-cont | +0.55% | +2.37% |
| tree-cancel | +0.34% | +14.99% |

No quiet delta is worse than −5%. private-remote at zero consumers sits on
the line. The large six-consumer gains are most likely layout effects, not a
property of F3. The field-only build reproduced the tree/6 gain (+10.4%); it
was not run for the other modes.

**New `private-tracked` baseline** (candidate only, U5 churn): 13.38
Mactor_cycles/s with inline consumption and 12.96 with six consumers. That is
−55% and −42% against candidate `private` (30.04 and 22.43). Every root
spawns with an armed deadline an hour away, reads the clock in its one
`cancelled` check, does four record stores, one terminal CAS and one take, and
is verified `completed`. See
[PRIVATE_LIFECYCLE.md](../../PRIVATE_LIFECYCLE.md#f3-private-tracked-baseline).
