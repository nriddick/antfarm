# G2 wave retirement policy results — 2026-10-06

Candidate `ed0c89d` passes the −3% screen after the 20-pair follow-ups. Whole
waves pick their retiring or non-retiring `runChild` instance when `start`
installs the header. Subsets keep the stored-policy load, because a specialized
subset callback lost 3.68% at subset100/6 over 20 pairs (gate exit 1). That
superseded port (`d30d286`, the audit prototype) is kept here as evidence.

Base: `7ee7916337c71285398cd4fb52a164793e6c0295`. Candidate: `ed0c89d`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`, branch
`perf/g2-wave-policy`. Nothing was pushed or written to the main checkout.

## Change

`start` installs `runChild!(operation, false, true)` when `retire` is true and
`runChild!(operation, false, false)` otherwise. Every initial, resume and
continuation-cleanup call of a whole wave therefore uses a compile-time
`retiring`. A debug check compares it with the stored `retire_` on every call
(`fatal("wave retirement policy invariant")`). `static assert(!(subset &&
retiringWave))` keeps the subset rule. `startSubset` still installs
`runChild!(operation, true)`, and that instance still reads `park.retire_`;
its code is byte-for-byte the same as base (138 bytes in the benchmark).
`retire_` still drives emit/truncation/finish. The skipped-retirement path
(local cleanup header plus `runSkippedRetire`) is unchanged. Credit operations,
atomic orders, Entry (40 B) and the public API are unchanged. The invariant
follows `/home/nathan-riddick/pi_ground/.cydo/tasks/235/prototype_proofs.md`:
`retire_` and `header_.call` are written only by the two starts, after the
`active != 0` rejection, and `active` stays nonzero until `finish`.

Code size, LDC `-O2 -release` benchmark (`size` text):

| Binary | text | runChild instances | runChild bytes |
| --- | ---: | ---: | ---: |
| base 7ee7916 | 1,255,461 | 8 | 2,233 |
| superseded d30d286 | 1,254,941 | 8 | 1,777 |
| candidate ed0c89d | 1,255,093 | 8 | 1,906 |

The benchmark still needs only one instance per call site, because `startFinal`
inlines a constant `retire`. Candidate instance sizes are: cancelLeaf 186→203,
foldLeaf 218→202, privateOperate 170→154, foldLaunch 223→72, foldContinue
766→734, foldValue 394→394, whole subsetOperate 138→9, selected subsetOperate
138→138. Call sites that pass a runtime `retire` now instantiate both whole
variants. In the private test binary (LDC `-g -O1 -release`), runChild instances
rise from 99 to 111 (+12), while their total size falls from 26,156 to 20,435
bytes. Binary text falls from 2,182,337 to 2,176,625.

## Run decisions

| Run | Pairs/cell | Cells | Pass | Inconclusive | Regression | Exit | Raw runs | Contaminated runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [full-5](full-5/gate.json) | 5 | 34 | 32 | 2 | 0 | 2 | 340 | 0 |
| [boundary-0](boundary-0/gate.json) | 20 | 3 | 3 | 0 | 0 | 0 | 120 | 0 |
| [boundary-6](boundary-6/gate.json) | 20 | 2 | 2 | 0 | 0 | 0 | 80 | 0 |
| [control-boundary-0](control-boundary-0/gate.json) | 20 | 3 | 3 | 0 | 0 | 0 | 120 | 0 |
| [control-boundary-6](control-boundary-6/gate.json) | 20 | 2 | 2 | 0 | 0 | 0 | 80 | 0 |
| [superseded full-5](superseded-d30d286-full-5/gate.json) | 5 | 34 | 32 | 2 | 0 | 2 | 340 | 0 |
| [superseded boundary-6](superseded-d30d286-boundary-6/gate.json) | 20 | 3 | 2 | 0 | 1 | 1 | 122 | 1 |

The full sweep had two inconclusive cells: private-tracked/0 and subset100/6.
Three more cells had a paired minimum within one point of −3%: tree-wait/0
(−2.45%), tree-pipelined/0 (−2.45%) and private-tracked/6 (−2.40%). All five,
which include the required subset100/6 and tree-wait/0, ran 20 pairs against
the candidate and 20 pairs in a byte-identical base/base control.

| Cell | Candidate 20-pair median | Interval | Control median | Control min…max |
| --- | ---: | --- | ---: | --- |
| tree-wait/0 | +0.40% | −0.14…+2.19% | −0.30% | −10.59…+12.42% |
| tree-pipelined/0 | +1.42% | +1.05…+1.71% | −0.19% | −5.42…+0.24% |
| private-tracked/0 | −1.27% | −1.51…−1.09% | +0.27% | −0.46…+1.66% |
| subset100/6 | +0.32% | −0.82…+1.28% | +0.79% | −8.59…+5.59% |
| private-tracked/6 | −0.38% | −0.51…−0.11% | +0.16% | −9.93…+0.94% |

Five-pair medians outside ±1%: subset-whole/0 +17.52%, unreserved whole/0
+16.26%, subset100/0 +3.31%, subset-whole/6 +1.71%, unreserved whole/6 +1.66%,
and subset10/0 −1.71% (paired minimum −1.84%, far from −3%). subset10/0 runs
only the selected-subset callback, whose code is byte-identical to base. Every
other full-5 cell is within ±1% by median. The superseded port also gained at
sparse and full subset/0 (+8.00%, +11.83%, +12.54%), which ed0c89d gives up to
keep subset100/6. Among the 20-pair cells, private-tracked/0 at −1.27% is the
largest median loss. It is outside this change's code path: tracked roots, no
child waves. Its control is +0.27%.

Every run waited for one-minute load ≤ 1 before admission
(`investigation/scripts/gate.sh`). The first superseded full-5 attempt was
rejected by the gate's own load check before any build and was deleted. The
superseded runs were renamed after completion, so their metadata output paths
still say `full-5`/`boundary-6`. Binaries were removed; `binary-sha256.txt`
records them, and the control binaries are identical
(`9017b3a0…`). [per-cell-deltas.md](per-cell-deltas.md) has every table and
[commands.json](commands.json) every command.

## Investigation

Profiling binaries: `churn-base` (7ee7916) and `churn-cand` (superseded
d30d286), built with the gate's command. Variant B, the final source, rebuilds
byte-identically to the candidate.

### subset100/6

- **Bottleneck.** The controller is the bottleneck. `perf record` (cycles:u,
  999 Hz, per thread, in `investigation/subset100-6/rec-*.threads.txt`) puts
  81–82% of controller cycles in the subset Farm writer
  (`writeImpl!SubsetBodies`) in both binaries. Clock reads in the wait loop
  take about 13%. Workers mostly poll (Consumer.run, processShard,
  consumeNext and sweepCurrentPosition total about 85%).
- **What the specialization changes.** It cut worker runChild time from
  9.9–10.4% to 4.1–4.2% of worker cycles. The freed time went to polling
  (processShard 19.6% → 24.3%).
- **Counters.** Aggregate `perf stat` over all threads
  (`investigation/subset100-6/stat-*.csv`) shows the candidate retiring about
  2% fewer instructions and about 25% fewer cache misses. These are mostly
  worker events.
- **Writer coherence is not the cause.** Sampling demand fills from another
  cache in the CCX (raw `r0243:u`, `*.r0243.txt`) gives the same controller
  total (≈9M) in both binaries and in both fast and slow runs. Writer
  coherence misses therefore do not explain the gap. The exact interference
  mechanism is still unidentified.
- **Throughput is bimodal.** Base runs swing between about 86.5 and 95.5 M/s
  depending on host state. The specialized subset callback reaches the
  92–95 mode much less often. In the superseded 20 pairs it ran 87.5–91.5
  while base ran 87.0–95.6.
- **Fix.** Specializing only whole waves (variant B) gave +0.09% against base
  over 10 pairs (`ab-subset100-6-candB.log`). Against the superseded port it
  gave +2.69% over 8 pairs, all positive (`ab-subset100-6-cand-vs-candB.log`).
  The gate then confirmed it at 20 pairs: +0.32%. Code placement was not used
  as a fix.
- **Earlier exploratory A/B.** `ab-subset100-6-r1.log` compared base 7ee7916
  with the superseded d30d286 port over 10 pairs: +0.63% paired median
  (−5.51…+5.56%). The superseded port's later 20-pair gate found −3.68%.

### tree-wait/0 outlier

- **Clock speed, not the binary.** tree-wait/0 is single-threaded. Rate swings
  track effective user clock (`cycles:u` ÷ user seconds, 4.07–4.22 GHz).
  `twprobe.txt` shows sys time stays at 0.01 s. One base run fell to
  60.79 Mleaf/s while having the lowest cycles/leaf (63.89), which means it
  ran at about 3.88 GHz. The byte-identical control at 20 pairs spans
  −10.59…+12.42%. The audit's −9.81% prototype pair is therefore within
  harness noise.
- **Small real per-cycle cost.** The port retires 212.57 instead of 215.57
  instructions/leaf at slightly lower IPC: +0.6% cycles/leaf over four
  paired `perf stat` runs (`tw-*.csv`). Profiles are otherwise identical
  (`twrec-*.threads.txt`). The extra cycles sit in mimalloc/adapter symbols.
  `treeActivate!true` moved from a 64-byte-aligned address to offset 48,
  which is a plausible placement cause.
- **Exploratory A/B.** `ab-treewait-0-r1.log` compared base with the
  superseded d30d286 port over 10 pairs: −1.86% paired median (−5.55…+5.61%).
- **Gate result.** Over 20 gate pairs, ed0c89d's tree-wait/0 median is
  +0.40%.

The `investigation/ab-*.log` runs are informal (`investigation/scripts/ab.py`).
They did not wait for load ≤ 1, and their logged one-minute load reached about
4.75. The load ≤ 1 admission rule applied only to the gate runs. The
conclusions rest on the 20-pair gate and control runs, not on these logs.

## Verification

All of these ran on ed0c89d:

- `make check` passed (`verification/check-ed0c89d.log`): Farm LDC/DMD, TSan,
  torture suites, private suite on LDC/DMD/TSan, documentation examples, and
  debug/release LDC/DMD death tests.
- `make check-soak` passed: 78 TSan loops in 121 s.
- `verification/step4-suites-ed0c89d.log` reruns the step-4 selectors on LDC
  and DMD, debug and `-release`, and all 56 runs passed. The selectors are
  `cont-final-retire`, `cont-results`, `cont`, `phase-dtor`, `dtor-child`,
  `dtor-results`, `cancel-c2`, `cancel-c3`, `cancel-c13`, `cancel`,
  `refill-after-final`, `phase-subset`, `subset-dtor` and `phase-d`.
- ed0c89d is the only runtime commit, and `make check` builds it with both
  LDC and DMD. The results commit changes data and documentation only.

`verification/mutate.py` applies each mutation alone to a scratch
`git archive` copy, builds it with LDC debug and `-release`, and runs the
selector. All 18 builds aborted (SIGABRT), and diffs are in
`verification/mutations/`.

| Mutation | Selector | Debug failure | Release failure |
| --- | --- | --- | --- |
| D-M4 retiring branch destroys twice | dtor-child | final destroyed exactly once before the join | same |
| D-M4b destroy after every op | dtor-child | non-final waves never destroy | same |
| D-M6 retire before writing result | dtor-results | slot written before ~T | same |
| C2a seal truncated startFinal like non-retiring | cancel-c2 | every member destroyed once | same |
| C2b retire skipped members in finish | cancel-c2 | destructors on consumers | same |
| C3 switch `park.header_.call` | cancel-c3 | drain timeout | same |
| keep count after terminal completion | refill-after-final | post-final empty start | same |
| W1 install opposite variant at start (new) | dtor-child | `wave retirement policy invariant` fatal | non-final waves never destroy |
| W2 retiring continuation completes as `done` (new) | cont-final-retire | main.d:6793 | same |
