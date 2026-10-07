# G0 gate and G1 constant deadline results — 2026-10-06

Candidate passes the −3% screen after the 20-pair follow-ups; control shows
one-sided subthreshold shifts at 20 pairs (dtor/0 −1.40%, subset1/0 +0.85%).
All eight candidate boundary cells and all seven control boundary cells pass.
Private/remote/dtor at zero consumers improve +5.19%, +4.57%, +5.68%.
The original full five-pair sweep and full five-pair control remain
inconclusive (exit 2), never relabeled as passing runs. The [control
analysis](control-analysis.md) explains order, drift and retained outliers.

Base: `71e914c78cf440028df99d9f46d8a761c8945fd2`. Candidate: `46827993463d7908e3cca27807bcc7298147cc5c`.
Worktree: `/home/nathan-riddick/pi_ground/.cydo/tasks/37/worktree`.
Branch: `perf/g0-g1`. Nothing was pushed or written to the main checkout.

Measurement metadata retains the original commit IDs. After review and
history rewrite, G0 is `e462b0d811fb19dc99b2dc7d661828d7851fb4d7` and G1 is
`299cf4b5ceb1e204df05ba2aa509095226f0dae6`; the runtime and benchmark sources
are unchanged, so the rewritten G1 can be used with the recorded build command.

G0 `ea2c402` adds the audited committed-ref gate, repository-relative archive
default/override, boundary/control flags, Make target, documentation and
58 focused tests. The current tip has 61 tests: three were added for the
mode-dependent decision wording. G1 `4682799` uses a private compile-time enum
equal to `MonoTime.max` in plain spawn and the other runtime deadline-sentinel
comparison (`trackRoot`), with a static equality assertion. The tracked
overload's `MonoTime = MonoTime.max` default argument is not folded and still
emits a `max()` call; only the `noDeadline` enum folds. The default argument
was left unchanged because it is off the measured plain-spawn path. Layout,
public API, allocator actions and synchronization are unchanged.

## Verification and measurement

Both implementation commits and the final results tree passed the complete [correctness matrix](verification/README.md),
including LDC/DMD builds and TSan. G1 also passed both compilers' private
release lifecycle suites. The results commit changes documentation/data only;
the verified runtime and build inputs remain identical to G1.

Timing ran serially without any concurrent heavy work from this task. Each
new run waited for one-minute load ≤1 before admission. The gate kept its
original contamination, work-equivalence and exit rules. All runs use the
same real mimalloc archive, LDC O2/release, identical benchmark source at
`71e914c`, ordinary pages, pinned 0/6-consumer placement, 16,384 actors, batch
256, quota 16,384 words, five warmup cycles and three-second windows.
CPU/core sharing, unavailable governors, frequency snapshots, binary/source/
archive hashes and exact build commands are in each run's metadata.

[commands.json](commands.json) lists every timing command. [per-cell-deltas.md](per-cell-deltas.md) gives a compact comparison. [summary.json](summary.json)
aggregates complete cell data and gate objects; [gate-summaries.json](gate-summaries.json)
preserves every run's gate JSON plus cell decision counts. Each directory
retains build logs, metadata, stdout/stderr, CPU/load/frequency samples in
`runs.jsonl`, and its original gate/summary/unavailable JSON. Binaries were
not committed; they can be rebuilt from the recorded revision and build
command in `metadata.json`. The recorded gate JSON retains its original
"all-pairs" wording; the harness was corrected afterward to distinguish the
five-pair all-pairs screen from the larger-sample paired-median interval screen.

## Run decisions

| Run | Pairs/cell | Cells | Pass | Inconclusive | Exit | Raw runs | Contaminated runs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [full-5](full-5/gate.json) | 5 | 34 | 29 | 5 | 2 | 350 | 5 |
| [boundary-0](boundary-0/gate.json) | 20 | 7 | 7 | 0 | 0 | 280 | 0 |
| [boundary-6](boundary-6/gate.json) | 20 | 1 | 1 | 0 | 0 | 40 | 0 |
| [control-5](control-5/gate.json) | 5 | 34 | 27 | 7 | 2 | 344 | 2 |
| [control-boundary-0](control-boundary-0/gate.json) | 20 | 5 | 5 | 0 | 0 | 200 | 0 |
| [control-boundary-6](control-boundary-6/gate.json) | 20 | 2 | 2 | 0 | 0 | 86 | 4 |

No run contains a regression or an unavailable mode. Contaminated counts
are runs, not pairs; a whole pair is excluded if either run is contaminated.
1,300 raw processes yielded 640 accepted pairs; 11 processes were flagged
contaminated, causing 10 whole pairs to be excluded.

## Per-cell deltas

Deltas below are **paired medians** from clean pairs. The interval is the
descriptive bootstrap median interval, not a calibrated 95% guarantee.
Original five-pair intervals can cross −3% even when their medians are
positive; every decision is retained explicitly.

### full-5

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 5 | +0.77% | +0.65…+1.07% | +0.65…+1.07% | pass |
| wave | 100 | 0 | 5 | +0.37% | -2.33…+0.53% | -2.33…+0.53% | pass |
| inbox | 100 | 0 | 5 | +1.17% | -2.02…+1.68% | -2.02…+1.68% | pass |
| private | 100 | 0 | 5 | +5.19% | +3.90…+5.93% | +3.90…+5.93% | pass |
| private-remote | 100 | 0 | 5 | +4.57% | +2.45…+14.37% | +2.45…+14.37% | pass |
| tree | 100 | 0 | 5 | -0.50% | -1.28…+4.37% | -1.28…+4.37% | pass |
| tree-wait | 100 | 0 | 5 | -1.34% | -3.34…+5.19% | -3.34…+5.19% | inconclusive |
| tree-cont | 100 | 0 | 5 | +0.31% | +0.24…+0.55% | +0.24…+0.55% | pass |
| tree-pipelined | 100 | 0 | 5 | +1.05% | -0.16…+1.52% | -0.16…+1.52% | pass |
| dtor | 100 | 0 | 5 | +5.68% | +4.44…+7.47% | +4.44…+7.47% | pass |
| subset | 1 | 0 | 5 | -2.68% | -3.63…-0.82% | -3.63…-0.82% | inconclusive |
| subset | 10 | 0 | 5 | -2.04% | -5.57…-0.34% | -5.57…-0.34% | inconclusive |
| subset | 100 | 0 | 5 | -0.32% | -5.46…+2.89% | -5.46…+2.89% | inconclusive |
| subset-whole | 100 | 0 | 5 | +0.30% | -0.59…+7.88% | -0.59…+7.88% | pass |
| subset-whole-unreserved | 100 | 0 | 5 | +0.04% | -2.37…+3.58% | -2.37…+3.58% | pass |
| tree-cancel | 100 | 0 | 5 | +0.06% | -0.98…+0.61% | -0.98…+0.61% | pass |
| private-tracked | 100 | 0 | 5 | +0.33% | -0.11…+0.73% | -0.11…+0.73% | pass |
| actor | 100 | 6 | 5 | +1.96% | +1.86…+2.05% | +1.86…+2.05% | pass |
| wave | 100 | 6 | 5 | +2.08% | +1.91…+3.40% | +1.91…+3.40% | pass |
| inbox | 100 | 6 | 5 | +2.77% | +2.48…+7.41% | +2.48…+7.41% | pass |
| private | 100 | 6 | 5 | +2.37% | +1.35…+2.79% | +1.35…+2.79% | pass |
| private-remote | 100 | 6 | 5 | +0.08% | -0.45…+1.08% | -0.45…+1.08% | pass |
| tree | 100 | 6 | 5 | +1.75% | +1.27…+1.95% | +1.27…+1.95% | pass |
| tree-wait | 100 | 6 | 5 | +1.37% | +1.20…+1.99% | +1.20…+1.99% | pass |
| tree-cont | 100 | 6 | 5 | -0.42% | -1.03…-0.12% | -1.03…-0.12% | pass |
| tree-pipelined | 100 | 6 | 5 | +0.98% | +0.51…+4.24% | +0.51…+4.24% | pass |
| dtor | 100 | 6 | 5 | +2.44% | +0.42…+3.46% | +0.42…+3.46% | pass |
| subset | 1 | 6 | 5 | -0.08% | -0.35…+1.22% | -0.35…+1.22% | pass |
| subset | 10 | 6 | 5 | -0.02% | -0.23…+1.61% | -0.23…+1.61% | pass |
| subset | 100 | 6 | 5 | +0.36% | -3.91…+1.51% | -3.91…+1.51% | inconclusive |
| subset-whole | 100 | 6 | 5 | +0.03% | -0.10…+0.03% | -0.10…+0.03% | pass |
| subset-whole-unreserved | 100 | 6 | 5 | +0.00% | -0.12…+0.57% | -0.12…+0.57% | pass |
| tree-cancel | 100 | 6 | 5 | -0.36% | -0.61…+0.65% | -0.61…+0.65% | pass |
| private-tracked | 100 | 6 | 5 | +1.82% | +1.31…+2.50% | +1.31…+2.50% | pass |

### boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| wave | 100 | 0 | 20 | +2.02% | +1.84…+2.19% | +1.43…+4.19% | pass |
| inbox | 100 | 0 | 20 | -1.21% | -1.41…-1.08% | -2.09…+1.17% | pass |
| tree-wait | 100 | 0 | 20 | +2.51% | +1.42…+3.80% | -4.40…+8.20% | pass |
| subset | 1 | 0 | 20 | +0.88% | +0.48…+1.12% | -4.45…+1.54% | pass |
| subset | 10 | 0 | 20 | +1.24% | +0.74…+1.53% | -4.96…+6.04% | pass |
| subset | 100 | 0 | 20 | +1.09% | +0.45…+1.47% | -4.82…+3.03% | pass |
| subset-whole-unreserved | 100 | 0 | 20 | -0.18% | -0.65…+0.78% | -4.97…+2.35% | pass |

### boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| subset | 100 | 6 | 20 | +1.12% | -0.18…+4.07% | -9.70…+7.59% | pass |

### control-5

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| actor | 100 | 0 | 5 | -0.31% | -0.66…+0.33% | -0.66…+0.33% | pass |
| wave | 100 | 0 | 5 | -0.06% | -1.99…+0.12% | -1.99…+0.12% | pass |
| inbox | 100 | 0 | 5 | +0.15% | -0.50…+0.40% | -0.50…+0.40% | pass |
| private | 100 | 0 | 5 | +0.15% | -0.45…+0.72% | -0.45…+0.72% | pass |
| private-remote | 100 | 0 | 5 | -0.14% | -1.15…+0.44% | -1.15…+0.44% | pass |
| tree | 100 | 0 | 5 | -2.73% | -7.55…-1.02% | -7.55…-1.02% | inconclusive |
| tree-wait | 100 | 0 | 5 | +0.55% | -3.24…+13.69% | -3.24…+13.69% | inconclusive |
| tree-cont | 100 | 0 | 5 | +1.10% | -0.62…+2.08% | -0.62…+2.08% | pass |
| tree-pipelined | 100 | 0 | 5 | +1.19% | -0.38…+3.17% | -0.38…+3.17% | pass |
| dtor | 100 | 0 | 5 | -0.07% | -5.37…+0.87% | -5.37…+0.87% | inconclusive |
| subset | 1 | 0 | 5 | +0.70% | -3.99…+4.77% | -3.99…+4.77% | inconclusive |
| subset | 10 | 0 | 5 | -0.90% | -4.96…+4.34% | -4.96…+4.34% | inconclusive |
| subset | 100 | 0 | 5 | -0.97% | -2.13…+0.89% | -2.13…+0.89% | pass |
| subset-whole | 100 | 0 | 5 | -0.92% | -2.11…-0.67% | -2.11…-0.67% | pass |
| subset-whole-unreserved | 100 | 0 | 5 | +0.80% | -0.60…+1.73% | -0.60…+1.73% | pass |
| tree-cancel | 100 | 0 | 5 | +0.24% | -0.44…+2.93% | -0.44…+2.93% | pass |
| private-tracked | 100 | 0 | 5 | +0.21% | +0.11…+0.62% | +0.11…+0.62% | pass |
| actor | 100 | 6 | 5 | +0.38% | -0.43…+1.07% | -0.43…+1.07% | pass |
| wave | 100 | 6 | 5 | +0.00% | -0.23…+0.47% | -0.23…+0.47% | pass |
| inbox | 100 | 6 | 5 | -0.55% | -1.46…-0.17% | -1.46…-0.17% | pass |
| private | 100 | 6 | 5 | +0.18% | -0.30…+0.83% | -0.30…+0.83% | pass |
| private-remote | 100 | 6 | 5 | +0.22% | +0.06…+0.41% | +0.06…+0.41% | pass |
| tree | 100 | 6 | 5 | -0.59% | -1.12…-0.41% | -1.12…-0.41% | pass |
| tree-wait | 100 | 6 | 5 | -0.10% | -1.28…+0.15% | -1.28…+0.15% | pass |
| tree-cont | 100 | 6 | 5 | -0.43% | -0.89…-0.04% | -0.89…-0.04% | pass |
| tree-pipelined | 100 | 6 | 5 | -0.13% | -3.32…+0.49% | -3.32…+0.49% | inconclusive |
| dtor | 100 | 6 | 5 | +0.01% | -0.42…+0.52% | -0.42…+0.52% | pass |
| subset | 1 | 6 | 5 | -0.13% | -0.22…+0.62% | -0.22…+0.62% | pass |
| subset | 10 | 6 | 5 | -0.16% | -0.25…+0.08% | -0.25…+0.08% | pass |
| subset | 100 | 6 | 5 | +0.04% | -6.14…+9.18% | -6.14…+9.18% | inconclusive |
| subset-whole | 100 | 6 | 5 | -0.11% | -0.16…-0.02% | -0.16…-0.02% | pass |
| subset-whole-unreserved | 100 | 6 | 5 | -0.09% | -2.12…+0.11% | -2.12…+0.11% | pass |
| tree-cancel | 100 | 6 | 5 | +0.02% | -0.28…+0.99% | -0.28…+0.99% | pass |
| private-tracked | 100 | 6 | 5 | -0.28% | -1.01…+0.77% | -1.01…+0.77% | pass |

### control-boundary-0

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree | 100 | 0 | 20 | +0.97% | -0.10…+2.99% | -4.20…+11.30% | pass |
| tree-wait | 100 | 0 | 20 | +0.64% | -0.67…+1.20% | -7.58…+7.96% | pass |
| dtor | 100 | 0 | 20 | -1.40% | -1.78…-1.01% | -2.63…+23.57% | pass |
| subset | 1 | 0 | 20 | +0.85% | +0.43…+1.35% | -3.50…+5.65% | pass |
| subset | 10 | 0 | 20 | +0.63% | -0.16…+1.03% | -3.57…+5.21% | pass |

### control-boundary-6

| Mode | Selection % | Consumers | Clean pairs | Paired median Δ | Interval | Paired min…max | Decision |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| tree-pipelined | 100 | 6 | 20 | -0.09% | -0.30…+0.03% | -3.03…+2.91% | pass |
| subset | 100 | 6 | 20 | +0.65% | -0.38…+1.36% | -8.51…+3.05% | pass |

## Iteration and deviations

The full five-pair sweep had five inconclusive cells. The parent confirmed
20-pair follow-ups on those cells plus three cells whose lower bounds came
within one percentage point of −3%: wave/0, inbox/0, tree-wait/0,
subset1/0, subset10/0, subset100/0, unreserved whole/0, subset100/6.
All passed. Sparse subset slowdowns from the initial sweep did not persist
at 20 pairs, so the parent's conditional runtime disassembly investigation
was not triggered. Binary hashes match across all rebuilds of each revision.

The prescribed full five-pair control had seven inconclusive cells. The
parent then directed exactly one 20-pair control follow-up on tree/0,
tree-wait/0, dtor/0, subset1/0, subset10/0, tree-pipelined/6 and subset100/6.
All passed the threshold. The parent also directed raw order/drift analysis
for one-sided 20-pair controls and a repository README limitation note;
those are included. No timing cells were rerun to chase a preferred result,
no thresholds were relaxed, and no runtime change followed G1.

The extra 20-pair controls and the resulting limitation documentation are
the only additions to the original plan, both explicitly directed by the
parent. Correctness verification required no fixes or investigation.
