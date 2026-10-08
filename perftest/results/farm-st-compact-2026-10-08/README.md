# Farm compact ST tables (prototype) and the Wt reservation fix (2026-10-08)

**Round 2 (below) explains private-remote/6 and settles the perftest follow-ups; whether the prototype lands depends on the user's decision about that cell.** cf2aa91 is now local main; the prototype commits sit on it:

| Commit | Content | State |
| --- | --- | --- |
| cf2aa91 | `antfarm: acquire other producers' reclamation in the Wt reservation`. Pre-existing bug fix: the `Wt` reservation goes from `rel` to `acq_rel`. Adds the `testStaleQuotaReuse` regression and SPEC/ACTOR_MEMORYORDER docs. | Standalone on 2f853e5 and cherry-pickable to main. Correctness and gate evidence below. **Ready for review.** |
| 5e22442 | `antfarm: prototype compact encoding for all-ST (Tmt == 0) tables`. Runtime, SPEC 2f, and geometry retunes of existing tests. | Prototype. One 6-consumer cell below the boundary (private-remote/6 −4.5%), explained in round 2 as allocator-contention geometry; needs a user decision. |
| 902549c | `tests: compact ST table coverage`. Eight Farm tests plus the T26 death test. | Goes with 5e22442. |
| a7ab59f | `perf: record compact ST prototype and Wt reservation fix gates` (round 1 evidence). | Results only. |
| 7fb4e17 | `perftest: retire digest.d; size compact singletons in throughput/dual` (round 2). | Perftest only; no runtime change. |
| (this) | `perf: private-remote/6 investigation; tree-cancel comparability` (round 2). | Results and docs only. |

**Revision names in the evidence below.** Timing, soaks and profiles ran on the pre-review SHAs 0469901, 13b1f85 and 82cbce0. The review cleanup produced the commits above:
- cf2aa91 has the same tree as 0469901; only the commit message changed.
- 5e22442 differs from 13b1f85 only in SPEC wording and two perftest comments.
- 902549c differs from 82cbce0 in those files plus antfarm_test.d (new multi-run test, quota-boundary checks, mutation labels).
- `antfarm.d` and `actors/` are byte-identical, so the timing applies unchanged. `make check` and a Farm TSan soak were rerun at 902549c (see correctness.txt).

## Summary

- **Format.** A table whose payloads are all single-threaded (`Tmt == 0`) stores each payload as `[Call][Pbody]`. Tindex, Thead and the counters are unchanged. Plen comes from consecutive Tindex entries; the last record ends at `Tsize − 7`. Callbacks receive a stack-local `PayloadHeader` with the same observable values as before. Tables with `Tmt != 0` are untouched. Details are in [SPEC 2f](../../../SPEC.md#2f-compact-st-tables-prototype) and the step-0 analysis below.
- **Ring words per entry**, one-word private body: **18.04 → 3.01** at a full table, 18.15 → 3.15 at batch 256. Two words (public actor): 19 → 4. Three words (wave): 20 → 5.
- **Writer cost per entry**, `writeTracked`, one-word body, single thread, TSC ticks: **12.1 → 4.8** at batch 256 (−60%) and 11.5 → 4.3 at full tables. Small tables gain less (batch 16: 18.0 → 11.2), and singletons are flat at 108.
- **Small-tier admission at quota 16384**, one table: one-word bodies **908 → 5448**, two words 860 → 4086, three words 817 → 3269.
- **Gate (i)**, compact 82cbce0 against base+fix 0469901, full actor_churn matrix at 0/6 consumers, 5 pairs: 32 of 34 cells pass, mostly with large gains:
  - 0 consumers: +7.9% (wave) to +76.3% (subset-whole-unreserved). private/0 is +30.3%, tree/0 +22.5%.
  - 6 consumers: tree/6 +2.1% and tree-wait/6 +0.6%, then up to +226% (subset-whole/6). private/6 is +30.9%, subset/100%/6 +127%.
  - **private-remote/6 regresses: −4.42%** at 5 pairs. The 20-pair boundary run confirms it at −4.49% (pairs −4.85..−4.10), against −0.08% for the byte-identical control. **Round 2:** about three quarters of it reproduces in the base build from quota headroom alone (mimalloc cross-thread-free contention that depends on quota geometry). A residual of about 1% is unexplained. See [Round 2](#round-2-private-remote6-tree-cancel-and-perftest-follow-ups).
  - **tree-cancel/6 is `inconclusive_work`** at −16.0%. The candidate dispatches 99.9% of the cancellable work before cancellation lands, against 94.8% on base, so per-round work is unequal. **Accepted by the user (round 2):** tree-cancel rates are not comparable across this change.
- **Gate (ii)**, fix only, 0469901 against 2f853e5, same matrix: no regression. Five cells were inconclusive on single outlier pairs. All five pass in 20-pair boundary runs (paired medians −1.46%..+0.24%), and the byte-identical controls pass too. The x86 cost of `acq_rel` is nil within noise, as expected: `lock xadd` is already a full barrier.
- **Correctness**, final HEAD 82cbce0:
  - `make check` and `make check-soak` pass.
  - Farm TSan soak: 1131 loops in 630 s. Private TSan soak: 182 loops in 630 s. Zero failures.
  - Fix commit alone: Farm TSan soak of 1468 loops in 630 s, zero failures.
  - All eight listed mutations (M1–M5, M6a, M6b, M7) were verified by hand.
  - Independent consultant reviews: compact layout ([task 278](/home/nathan-riddick/pi_ground/.cydo/tasks/278/output.md)) and sibling producer ordering ([task 282](/home/nathan-riddick/pi_ground/.cydo/tasks/282/output.md)). Neither found a defect beyond the Wt reservation.
  - See [correctness.txt](correctness.txt).

## Round 2: private-remote/6, tree-cancel and perftest follow-ups

The full evidence is in [private-remote-6/README.md](private-remote-6/README.md); the driver is [investigate.sh](private-remote-6/investigate.sh). The investigation used diagnostic builds of cf2aa91 and 5e22442 only, and changed no runtime code.

**private-remote/6: mostly allocator-contention geometry; a residual of about 1% is unexplained.**
- **The bottleneck.** Every root free in this cell is a cross-thread push onto the controller's mimalloc page `xthread_free` list. That push averages about 3 CAS attempts.
- **CAS attempts order the rates exactly**, independent of layout:

  | config | rate | CAS attempts per free |
  | --- | ---: | ---: |
  | compact at quota 2842 | 14.34 | 2.90 |
  | base at quota 16384 | 14.29 | 2.91 |
  | base at quota 94440 | 13.86 | 3.02 |
  | compact at quota 16384 | 13.61 | 3.07 |

- **Writer speed is not the cause.** Adding spin to the candidate writer, up to 5× base's whole writer cost, raises the rate steadily from 13.59 to 13.78 (+1.4%). That stays small against the 5.4% gap to base (14.37) in that run.
- **Contention collapse.** Adding a useless 40-tick spin to each consumer callback *raises* throughput by about 40% in both trees, as CAS attempts per free fall to 1.9.
- **Geometry is the lever.** At batch 256 both layouts publish the same ≤256-entry tables with the same 32-entry claims, so claim chunking and cost hints do not apply. What changes is quota admission:
  - 16,384 words hold 3.5 full-layout or 20.3 compact root tables.
  - That changes how often the writer meets partly reclaimed quota (at the gate quota, base quota-truncates 25% of its tables and compact 5%), and with it the free interleaving.
- **The landscape is non-monotone in quota.** Base spans 13.62–15.87 M/s across quotas (about ±8%). Compact spans only 13.66–14.87 (about ±4%) and never reaches base's best (15.87 at quota 9400).
- **Matched geometry, 10 rounds:**

  | comparison | paired median |
  | --- | ---: |
  | compact at full-layout tables per quota (2842) vs base | +1.65% |
  | compact vs base at compact tables per quota (94440) | −0.94% |
  | base given compact's headroom (94440 vs 16384) | −3.44% |

  From the 3-run quota landscape, compact against base at matched tables per quota:

  | tables per quota | ~2 | 3.0 | 3.5 | 4.0 | ~10 | 20.3 |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: |
  | compact vs base | −6.3% | −4.8% | +1.2% | +4.2% | −3.5% | −1.2% |

  The two paired points fall on the favourable side of that spread.
- **Reading.** About three quarters of the loss (−3.44% of −4.49%) reproduces in the base build from quota headroom alone. The mechanism is mimalloc cross-thread-free contention that depends on quota geometry. A residual of roughly 1% is unexplained and could be format-related.

- **Remedy.** The only Farm-side lever would be a quota or admission default change, which would tune this non-monotone landscape rather than remove a mechanism. It was not made. A real remedy belongs on the allocation side (root-state recycling or a slab).

**tree-cancel/6: accepted change in dispatched work.**
- Compact publication is cheap enough that the candidate dispatches 99.9% of the cancellable work before cancellation lands, against 94.8% on base.
- Each round therefore does more work, so tree-cancel rates are **not comparable across this change**.
- The gate's `inconclusive_work` decision on tree-cancel/6 is correct and is documented, not waived.
- Future gates whose base and candidate are both at or after 5e22442 compare normally.

**Perftest follow-ups (7fb4e17).**
- `perftest/digest.d` is retired. It only published ST payloads, which compact tables now cover, and it read ring headers that compact tables no longer have.
- `singletonSize()` in throughput and dual uses the compact one-word record. `--full-header-size` keeps the old sizing for runs against pre-2f runtimes.

## How the Wt bug was found

The first Farm TSan soak of the pre-fix candidate (a5e95ed: compact on top of 2f853e5) failed at loop 1121 of about 1120 clean runs ([log](original-farm-tsan-race-a5e95ed.log)). TSan flagged a writer's raw store of a new table's Thead word 2 racing an earlier lap's plain read of a compact callback body.

The orchestrator's investigation ([task 279](/home/nathan-riddick/pi_ground/.cydo/tasks/279/output.md)) traced it to a **pre-existing** bug, not to the compact format:
- The `Wt` reservation was release-only.
- A producer spending a retained quota balance without a fresh sweep never acquired the reclamation history other producers had imported: consumer pin drop → `Rt` release → zero-`Rt` sweep acquire.
- So its ring stores were unordered against consumers' plain reads on the previous lap.

The base reproducer raced in 6/6 runs at 2f853e5 and passed 20/20 with `acq_rel`. The orchestrator approved the fix as a separate bottom commit.

Its regression test `testStaleQuotaReuse` (Farm TSan lane):
- **At 0469901:** reports the race in 6/6 runs with the reservation reverted to `rel`.
- **In the compact tree:** uses MaxCs=2 payloads so its tables keep the full layout. With `rel` it races 3/3. An all-ST compact variant did not trigger the race with this schedule, so it was not kept; calibrating one is listed under open questions.

The sibling-path audit (task 282) found no other confirmed producer ordering defect. It proved `acq_rel` sufficient for both ring and `stats` stores, given I1. The argument is now in SPEC 9.

Two pre-existing items are **suspected, not confirmed, and not fixed**:
- **S1:** `subscribe` could pair an old `es`/`seqt` with a newer lap's `sqcs` and attach to a lapped epoch. The audit's proposed fix is acquire loads in subscribe plus release crossing stores.
- **S2:** the producer's unpinned `plantIfUnprotected(e−1)` has an ABA hazard. If slot `e−1` is lapped between reservation and plant, a stray Sub0 can land. The audit's proposed mitigation is a post-CAS `es` recheck.

Both are logic or liveness hazards on atomic objects, not data races. They need targeted tests before any change.

### TSan hook build discrepancy (deferred, not fixed)

`antfarm.d` defines manual TSan hooks under `version (TSan)`:
- `tsanRelease(Rt)` at every consumer pin drop (`decRef`).
- `tsanAcquire(Rt)` at each zero-`Rt` sweep step.
- A strong `__tsan_default_options`.

Which builds pass `-d-version=TSan`:

| Target | `-fsanitize=thread` | `-d-version=TSan` | Hooks |
| --- | --- | --- | --- |
| root `make farm-tsan` / `run-farm-tsan` (the **Farm TSan lane**, `antfarm_test_tsan`) | yes | **no** | compiled out |
| `actor_torture` `run-tsan`, mimalloc TSan | yes | yes | active |
| `review_torture` `run-tsan`, `t19_tsan` | yes | yes | active |
| `private_actor_tests` `private_actor_tests_tsan` (also check-soak) | yes | yes | active |

Consequences:

- **The Farm lane is the stricter one.** It sees only the program's real atomics. The task-279 investigation checked LLVM 22.1.8's TSan runtime, which models acq_rel RMWs as ReleaseAcquire, so the leaf → `Rt` chain is tracked without help. Missing hooks therefore do not cause false positives there. The Farm lane depends on `run-farm-tsan.sh` to set `halt_on_error`, because the strong default options are compiled out too.
- **The hooked lanes can mask races.** `__tsan_release`/`__tsan_acquire` on `Rt[ki][0]` merge every release ever made on that address into the acquiring sweep's clock, whether or not the observed `Rt == 0` actually read from it. The address is reused every lap. That adds happens-before edges the real program may lack.
  - It cannot create false positives.
  - It can hide a real reuse race. Enabling the hooks did **not** hide the stale-quota bug in the base reproducer, because the spending producer does no sweep.
  - The private, actor and review suites have less reuse-ordering power than the Farm lane.
- **The comment near antfarm.d:530 is questionable** (task 279 §6). It says TSan "does not carry a non-edge leaf acq_rel through the last-on-leaf's Rt release". LLVM's runtime does carry acq_rel RMWs. The hooks look unnecessary, and possibly harmful because they can mask races. Removing them, or building every lane the same way, is deferred to a separate change.

## Gate protocol

- Host: Ryzen 5 5500, LDC 1.43.0 `-O2 -release`, pinned mimalloc 3.5.3, ordinary pages.
- Workload: 16,384 actors, batch/quota 256/16,384, small tier, five warmups, 3-second windows. Controller plus six consumers on CPUs 0..6.
- All timing ran serially on a quiet host after the correctness runs. The driver waits for the 1-minute load average to fall below 0.8.
- `actor_churn.d` is identical in all three revisions. Every gate compiles 2f853e5's copy (`--benchmark-ref 2f853e5`).
- Driver: [run-gate.sh](run-gate.sh). Tables come from [summarize.py](summarize.py).

| Gate | Base | Candidate | Role |
| --- | --- | --- | --- |
| `gate-compact` (i) | 0469901 | 82cbce0 | compact gains/regressions, full matrix, 5 pairs |
| `compact-boundary-6`, `compact-control-6` | 0469901 | 82cbce0 / 0469901 | 20-pair confirmation of private-remote/6, byte-identical control |
| `gate-fix` (ii) | 2f853e5 | 0469901 | fix-only cost, full matrix, 5 pairs |
| `fix-boundary-{0,6}`, `fix-control-{0,6}` | 2f853e5 | 0469901 / 2f853e5 | 20-pair runs of the five inconclusive cells, byte-identical controls |

## Writer microbenchmark: ring words, writer ticks, admission

[writer_bench.d](writer_bench.d) via [writer-bench.sh](writer-bench.sh), base 0469901 against candidate 82cbce0. LDC `-O2 -release`, single thread pinned to CPU 2, ordinary pages, ring 8 MiB, K=8, small tier with quota 16384, 2000 timed writes, 3 alternating rounds; the table shows the median over rounds.

Each timed write is one broadcast `write` or `writeTracked` of B identical ST entries, followed by an untimed drain on the same thread. Timing starts after the ring has been lapped twice. An earlier run without that warm-up timed first-touch page faults for small compact tables and is not reported.
- "agg ticks/entry" counts every timed write, including quota sweeps and leftover-quota partial tables.
- "median" counts only full-size tables.
- Ticks are rdtsc ticks, not core cycles.
- Raw output: [writer-bench.txt](writer-bench.txt).

| API | body | batch | admitted base → cand | ring words/entry | agg ticks/entry | median ticks/entry (full tables) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| write | 1 | 1 | 1 → 1 | 56.00 → 41.00 | 117.8 → 117.6 | 108.0 → 108.0 |
| write | 1 | 16 | 16 → 16 | 20.38 → 5.38 | 19.5 → 10.9 | 18.0 → 11.2 |
| write | 1 | 256 | 256 → 256 | 18.15 → 3.15 | 12.5 → 4.9 | 12.1 → 4.8 |
| write | 1 | 1024 | 908 → 1024 | 18.04 → 3.04 | 11.8 → 4.5 | 11.5 → 4.4 |
| write | 1 | 8192 | 908 → 5448 | 18.04 → 3.01 | 11.7 → 4.4 | 11.5 → 4.3 |
| write | 2 | 1 | 1 → 1 | 57.00 → 42.00 | 118.6 → 116.2 | 108.0 → 108.0 |
| write | 2 | 16 | 16 → 16 | 21.38 → 6.38 | 19.1 → 12.6 | 18.0 → 11.2 |
| write | 2 | 256 | 256 → 256 | 19.15 → 4.15 | 13.2 → 6.5 | 12.8 → 6.5 |
| write | 2 | 1024 | 860 → 1024 | 19.04 → 4.04 | 12.5 → 6.3 | 12.3 → 6.1 |
| write | 2 | 8192 | 860 → 4086 | 19.04 → 4.01 | 12.5 → 6.1 | 12.3 → 6.0 |
| write | 3 | 1 | 1 → 1 | 58.00 → 43.00 | 118.3 → 115.8 | 108.0 → 108.0 |
| write | 3 | 16 | 16 → 16 | 22.38 → 7.38 | 20.5 → 13.4 | 18.0 → 13.5 |
| write | 3 | 256 | 256 → 256 | 20.15 → 5.15 | 13.9 → 7.5 | 13.4 → 7.3 |
| write | 3 | 1024 | 817 → 1024 | 20.05 → 5.04 | 13.3 → 7.2 | 13.1 → 7.0 |
| write | 3 | 8192 | 817 → 3269 | 20.05 → 5.01 | 13.2 → 7.0 | 13.0 → 6.9 |
| writeTracked | 1 | 1 | 1 → 1 | 56.00 → 41.00 | 127.8 → 119.9 | 108.0 → 108.0 |
| writeTracked | 1 | 16 | 16 → 16 | 20.38 → 5.38 | 18.5 → 11.0 | 18.0 → 11.2 |
| writeTracked | 1 | 256 | 256 → 256 | 18.15 → 3.15 | 12.6 → 4.9 | 12.1 → 4.8 |
| writeTracked | 1 | 1024 | 908 → 1024 | 18.04 → 3.04 | 11.7 → 4.5 | 11.5 → 4.4 |
| writeTracked | 1 | 8192 | 908 → 5448 | 18.04 → 3.01 | 11.7 → 4.4 | 11.5 → 4.3 |
| writeTracked | 2 | 1 | 1 → 1 | 57.00 → 42.00 | 121.6 → 119.2 | 108.0 → 108.0 |
| writeTracked | 2 | 16 | 16 → 16 | 21.38 → 6.38 | 19.3 → 12.5 | 18.0 → 13.5 |
| writeTracked | 2 | 256 | 256 → 256 | 19.15 → 4.15 | 13.3 → 6.7 | 12.8 → 6.5 |
| writeTracked | 2 | 1024 | 860 → 1024 | 19.04 → 4.04 | 12.6 → 6.2 | 12.4 → 6.1 |
| writeTracked | 2 | 8192 | 860 → 4086 | 19.04 → 4.01 | 12.5 → 6.1 | 12.4 → 6.0 |
| writeTracked | 3 | 1 | 1 → 1 | 58.00 → 43.00 | 122.4 → 115.7 | 108.0 → 108.0 |
| writeTracked | 3 | 16 | 16 → 16 | 22.38 → 7.38 | 19.5 → 13.4 | 18.0 → 13.5 |
| writeTracked | 3 | 256 | 256 → 256 | 20.15 → 5.15 | 14.1 → 7.5 | 13.4 → 7.3 |
| writeTracked | 3 | 1024 | 817 → 1024 | 20.05 → 5.04 | 13.3 → 7.1 | 13.1 → 7.0 |
| writeTracked | 3 | 8192 | 817 → 3269 | 20.05 → 5.01 | 13.2 → 7.0 | 13.0 → 6.9 |

**Admission at the small tier:**
- At the gate's batch/quota 256/16384, a private table is 4646 words now and 806 compact. Quota did not limit it before, and does not now.
- Batch 1024 was quota-truncated to 908 entries per table and is now admitted whole.
- **Private batch defaults may now be limited differently.** The quota no longer caps one-word tables below about 5448 entries, so `defaultPrivateBatchLimit = 256` and the parent pump's work-cost hint, not the Farm quota, now set the table geometry for any batch up to about 5.4k. This was not retuned here.

## Profiles

User-space `cycles:u` profiles of the gate-compact binaries, after all timing ([profile.sh](profile.sh); symbol tables are kept, records are not). Writer share is the samples in `AntFarm.writeImpl` instantiations, across all threads.

| Cell | base writer share | candidate writer share | base rate | candidate rate |
| --- | ---: | ---: | ---: | ---: |
| subset/100%/0 | 44.20% | 16.14% | 130.4 M selected callbacks/s | 213.9 |
| private/0 | 11.12% | 4.83% | 31.4 M actor cycles/s | 40.6 |
| private/6 | 2.60% | 1.13% | 22.7 | 29.9 |
| private-remote/6 | 1.45% | < 0.5% | 14.36 | 13.71 |

**private-remote/6 (round 1 view; explained in round 2).** In both binaries `mi_free_size_aligned` takes about 58% of samples, plus `_mi_page_free_collect` at about 5.5% and `mi_free_try_collect_mt` at about 1.9%. The Farm writer is negligible on both sides. The regression is real (20 pairs plus control) but sits in consumer-side remote-free and allocator behaviour, not in the format itself.
- One hypothesis is that a cheaper emitter changes the timing or batching of cross-thread frees, raising contention on mimalloc's page free lists.
- Another is layout: compact moves `processShard`/`runCompact` code.
- Round 2 tested both: neither is the cause. See [private-remote-6/README.md](private-remote-6/README.md).

## Gate tables

### gate-compact

exit=1 candidate_env=(none) base=0469901 candidate=82cbce0 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| actor/0 | pass | +8.94% | +8.34..+9.53% | 18.634 | 20.341 | 18052 | 18128 |
| wave/0 | pass | +7.89% | +7.10..+8.38% | 24.195 | 26.001 | 18116 | 18056 |
| inbox/0 | pass | +9.00% | +8.79..+9.20% | 15.137 | 16.508 | 20104 | 20108 |
| private/0 | pass | +30.34% | +29.01..+30.80% | 31.896 | 41.333 | 18104 | 18100 |
| private-remote/0 | pass | +30.57% | +29.24..+31.55% | 31.279 | 40.987 | 18072 | 18080 |
| tree/0 | pass | +22.48% | +14.90..+35.76% | 63.603 | 78.973 | 18112 | 18236 |
| tree-wait/0 | pass | +23.13% | +20.31..+23.46% | 65.241 | 80.536 | 18188 | 18120 |
| tree-cont/0 | pass | +14.09% | +13.68..+14.33% | 26.148 | 29.831 | 18116 | 18028 |
| tree-pipelined/0 | pass | +15.52% | +15.29..+16.05% | 27.567 | 31.912 | 18116 | 18152 |
| dtor/0 | pass | +29.99% | +29.12..+30.61% | 31.288 | 40.666 | 18108 | 18136 |
| subset/1%/0 | pass | +44.22% | +29.60..+44.85% | 105.757 | 152.276 | 18208 | 18096 |
| subset/10%/0 | pass | +62.57% | +60.96..+63.03% | 129.570 | 210.452 | 18080 | 18164 |
| subset/100%/0 | pass | +64.39% | +62.63..+64.97% | 130.858 | 215.867 | 18056 | 18168 |
| subset-whole/0 | pass | +70.69% | +68.68..+71.18% | 206.821 | 353.030 | 18168 | 18020 |
| subset-whole-unreserved/0 | pass | +76.27% | +39.10..+94.37% | 200.945 | 353.498 | 16012 | 16184 |
| tree-cancel/0 | pass | +19.50% | +2.96..+19.96% | 53.969 | 64.492 | 18148 | 18080 |
| private-tracked/0 | pass | +20.30% | +19.91..+22.07% | 13.539 | 16.361 | 20128 | 20176 |
| actor/6 | pass | +11.53% | +10.14..+12.59% | 15.067 | 16.771 | 18104 | 17980 |
| wave/6 | pass | +9.00% | +8.18..+9.27% | 16.022 | 17.479 | 18064 | 18100 |
| inbox/6 | pass | +11.13% | +11.06..+12.02% | 12.655 | 14.060 | 20172 | 20132 |
| private/6 | pass | +30.87% | +30.43..+31.42% | 22.836 | 29.935 | 18148 | 18020 |
| private-remote/6 | regression | -4.42% | -4.68..-4.31% | 14.280 | 13.638 | 18236 | 18356 |
| tree/6 | pass | +2.09% | +1.02..+2.48% | 54.536 | 55.641 | 22244 | 20296 |
| tree-wait/6 | pass | +0.55% | +0.27..+0.98% | 55.422 | 55.729 | 20288 | 20112 |
| tree-cont/6 | pass | +18.59% | +17.51..+21.67% | 31.289 | 37.062 | 22224 | 20264 |
| tree-pipelined/6 | pass | +32.07% | +30.70..+32.27% | 30.961 | 40.830 | 22136 | 20164 |
| dtor/6 | pass | +30.63% | +30.33..+31.32% | 22.937 | 29.980 | 18088 | 18144 |
| subset/1%/6 | pass | +47.55% | +47.25..+47.71% | 42.702 | 63.000 | 18244 | 18228 |
| subset/10%/6 | pass | +118.62% | +118.25..+119.16% | 80.047 | 174.998 | 18100 | 18132 |
| subset/100%/6 | pass | +127.42% | +126.44..+133.00% | 94.994 | 215.710 | 18312 | 18192 |
| subset-whole/6 | pass | +225.92% | +225.20..+226.13% | 102.933 | 335.367 | 18328 | 18280 |
| subset-whole-unreserved/6 | pass | +225.88% | +225.28..+227.19% | 102.760 | 335.300 | 16208 | 16144 |
| tree-cancel/6 | inconclusive_work | -16.02% | -16.52..-15.69% | 66.843 | 56.143 | 22420 | 20236 |
| private-tracked/6 | pass | +19.02% | +18.55..+19.96% | 13.157 | 15.634 | 20236 | 20096 |

### compact-boundary-6

exit=1 candidate_env=(none) base=0469901 candidate=82cbce0 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private-remote/6 | regression | -4.49% | -4.85..-4.10% | 14.355 | 13.712 | 18272.0 | 18228.0 |

### compact-control-6

exit=0 candidate_env=(none) base=0469901 candidate=0469901 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private-remote/6 | pass | -0.08% | -0.65..+0.20% | 14.350 | 14.343 | 18202.0 | 18248.0 |

### gate-fix

exit=2 candidate_env=(none) base=2f853e5 candidate=0469901 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| actor/0 | pass | -0.06% | -0.52..+0.13% | 18.639 | 18.625 | 18080 | 18152 |
| wave/0 | pass | -0.22% | -0.49..+0.86% | 24.022 | 24.024 | 18160 | 18164 |
| inbox/0 | pass | +0.54% | -0.08..+0.89% | 15.026 | 15.103 | 20216 | 20084 |
| private/0 | pass | +0.33% | -0.57..+1.40% | 31.694 | 31.876 | 18148 | 18116 |
| private-remote/0 | pass | -0.22% | -0.44..+0.96% | 31.157 | 31.199 | 18196 | 18096 |
| tree/0 | pass | +1.71% | +0.40..+11.72% | 63.905 | 65.011 | 18048 | 18060 |
| tree-wait/0 | inconclusive | +0.98% | -3.54..+2.67% | 64.169 | 64.189 | 18112 | 18152 |
| tree-cont/0 | pass | +0.01% | -0.64..+0.86% | 26.097 | 26.099 | 18176 | 18140 |
| tree-pipelined/0 | pass | +0.78% | -2.54..+1.33% | 27.612 | 27.710 | 18088 | 18212 |
| dtor/0 | pass | +0.78% | -2.15..+1.35% | 30.923 | 31.147 | 18096 | 18064 |
| subset/1%/0 | pass | +0.66% | -2.19..+1.71% | 105.235 | 105.869 | 18116 | 18100 |
| subset/10%/0 | pass | +0.29% | -0.60..+1.16% | 128.566 | 128.719 | 18068 | 18112 |
| subset/100%/0 | inconclusive | -0.09% | -3.42..+0.57% | 131.664 | 131.200 | 18076 | 18104 |
| subset-whole/0 | pass | +1.07% | -0.08..+1.55% | 209.228 | 210.828 | 18100 | 18116 |
| subset-whole-unreserved/0 | inconclusive | +1.38% | -11.01..+4.09% | 208.693 | 211.577 | 16032 | 16040 |
| tree-cancel/0 | pass | +0.15% | -1.95..+3.83% | 53.182 | 53.284 | 18144 | 18144 |
| private-tracked/0 | pass | +0.22% | +0.06..+0.95% | 13.489 | 13.540 | 20184 | 20108 |
| actor/6 | pass | +0.02% | -2.36..+0.82% | 15.083 | 15.074 | 18052 | 18160 |
| wave/6 | pass | +0.04% | -0.20..+0.19% | 15.987 | 16.008 | 18080 | 18100 |
| inbox/6 | pass | +0.44% | +0.10..+0.74% | 12.542 | 12.635 | 20236 | 20200 |
| private/6 | pass | +0.28% | -0.43..+0.74% | 22.718 | 22.839 | 18196 | 18244 |
| private-remote/6 | pass | -0.03% | -0.10..+0.04% | 14.268 | 14.269 | 18176 | 18192 |
| tree/6 | pass | -0.30% | -0.47..+1.27% | 54.435 | 54.498 | 22200 | 22220 |
| tree-wait/6 | pass | -0.40% | -0.68..+0.43% | 55.237 | 55.043 | 20272 | 20100 |
| tree-cont/6 | pass | -0.10% | -2.87..+0.36% | 31.416 | 31.363 | 22192 | 22332 |
| tree-pipelined/6 | pass | -0.04% | -0.20..+0.34% | 30.945 | 30.959 | 22268 | 22244 |
| dtor/6 | pass | +0.23% | +0.09..+0.28% | 22.832 | 22.863 | 18152 | 18244 |
| subset/1%/6 | pass | +0.02% | -0.10..+0.18% | 42.688 | 42.695 | 18184 | 18324 |
| subset/10%/6 | inconclusive | -0.05% | -5.77..+0.04% | 79.940 | 79.887 | 18132 | 18204 |
| subset/100%/6 | inconclusive | -0.32% | -6.76..-0.09% | 87.408 | 87.135 | 18200 | 18284 |
| subset-whole/6 | pass | +0.02% | -0.49..+0.04% | 102.901 | 102.874 | 18192 | 18216 |
| subset-whole-unreserved/6 | pass | +0.05% | -0.11..+1.03% | 102.893 | 102.908 | 16188 | 16060 |
| tree-cancel/6 | pass | +1.09% | +0.81..+1.54% | 66.178 | 66.953 | 20372 | 20248 |
| private-tracked/6 | pass | +0.22% | -0.58..+0.75% | 13.134 | 13.161 | 20296 | 20364 |

### fix-boundary-0

exit=0 candidate_env=(none) base=2f853e5 candidate=0469901 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree-wait/0 | pass | -1.46% | -5.48..+9.36% | 64.489 | 63.630 | 18070.0 | 18086.0 |
| subset/100%/0 | pass | -0.24% | -3.69..+2.48% | 133.093 | 132.736 | 18092.0 | 18072.0 |
| subset-whole-unreserved/0 | pass | -0.18% | -11.72..+12.74% | 207.357 | 206.294 | 16032.0 | 16068.0 |

### fix-boundary-6

exit=0 candidate_env=(none) base=2f853e5 candidate=0469901 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| subset/10%/6 | pass | -0.00% | -1.47..+4.05% | 80.361 | 80.382 | 18230.0 | 18162.0 |
| subset/100%/6 | pass | +0.24% | -3.91..+3.77% | 93.816 | 94.481 | 18270.0 | 18226.0 |

### fix-control-0

exit=0 candidate_env=(none) base=2f853e5 candidate=2f853e5 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree-wait/0 | pass | +0.33% | -11.98..+12.09% | 63.307 | 63.171 | 18082.0 | 18076.0 |
| subset/100%/0 | pass | -0.15% | -2.85..+4.56% | 132.926 | 133.017 | 18108.0 | 18056.0 |
| subset-whole-unreserved/0 | pass | -0.78% | -12.47..+13.00% | 208.283 | 207.912 | 16038.0 | 16028.0 |

### fix-control-6

exit=0 candidate_env=(none) base=2f853e5 candidate=2f853e5 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| subset/10%/6 | pass | +0.21% | -2.00..+5.43% | 80.747 | 81.413 | 18198.0 | 18222.0 |
| subset/100%/6 | pass | -0.03% | -4.24..+6.58% | 91.015 | 91.614 | 18168.0 | 18192.0 |

## Tests

New compact coverage (82cbce0); the mutation each one catches is in [correctness.txt](correctness.txt):

| Test | What it covers |
| --- | --- |
| `testCompactMixedBodies` | heterogeneous bodies, including zero-length ones, with exact `Wt` geometry |
| `testCompactMultiRunBodies` | 40 heterogeneous bodies in 10 claimed runs (chunk 4), last body zero-length |
| `testCompactWraparound` | 3 producers, 4 consumers (one late), several laps, under TSan |
| `testCompactPartialAdmission` | exact accepted prefix, `writeTracked` hook, transactional prepare, zero admission |
| `testCompactQuotaBoundaries` | exact-quota table, one word over, exact-quota singleton |
| `testCompactOldLayout` | `Tmt != 0` still uses ring PayloadHeaders |
| `testCompactCallbackHeader` | SPEC 7c values seen by the callback |
| `testCompactHeterogeneousRule` | C′: MT prefix, then a compact-only ST payload written alone |
| review_torture T26 | fork death tests for the compact singleton contract |

Existing tests retuned for compact geometry in 13b1f85, each keeping its intent:

| Test | Change |
| --- | --- |
| `testUniformRanges` | N and P 300 → 1500 |
| `boundary_runoff` | body 457 → 472; the negative control reads body at `poff+1` |
| `write_protection` | overhead 55 → 40 |
| T23 | N 40000 → 160000 |
| T17 | comment only |
| private_actor_tests partial-admission quotas | 1024 → 200 and 4096 → 713, so the 54/64 and 225/256 prefixes are kept |
| private_actor_tests ring fillers | 20000 → 100000 |
| `testStaleQuotaReuse` | payloads become MaxCs=2 |

`perftest/digest.d` was outside `make check`. It only produced ST payloads, so after 5e22442 every digest configuration fatalled at its compact-table guard. Round 2 retired it (7fb4e17).

## What is ready and what is not

**Ready:**
- 0469901, the Wt reservation fix, as a standalone commit for main. It has `make check`, a Farm TSan soak, the regression test, an independent sibling audit, and a nil-cost gate with all boundary cells passing.
- The compact format's correctness: `make check`, check-soak, Farm and private TSan soaks, mutations verified by hand, and a consultant review.

**Not ready:**
- The compact prototype as a landing candidate, pending the user's decision on private-remote/6: −4.49% over 20 pairs against a −3% boundary. Round 2 attributes about three quarters of it to allocator-contention geometry (a residual of about 1% is unexplained), and no principled Farm-side fix exists without a quota or admission default change.
- tree-cancel/6 stays `inconclusive_work`; the user accepted the dispatched-work change, so it is documented rather than gated.
- No Windows run was done. The format is ulong words in the same magic buffer, so no platform-specific code changed.

## Open questions

1. **private-remote/6 −4.5%.** Investigated in round 2: about three quarters of it is quota-geometry-dependent allocator contention that reproduces in the base build; a residual of about 1% is unexplained and could be format-related. Open for the user: waive the cell on the matched-geometry evidence, or pursue an allocation-side change. A quota or admission default change is the only Farm lever, and it is not recommended.
2. **tree-cancel/6.** Resolved: the user accepted the higher dispatched fraction (0.948 → 0.999). Rates are not comparable across this change (perftest/README.md).
3. **Private batch and work-cost defaults.** Quota no longer limits one-word tables below about 5.4k entries. Should `defaultPrivateBatchLimit` or root cost hints be revisited? Not retuned here.
4. **Callback-header contract.** For compact tables `head` is a stack copy shared by the run's payloads. It must not be modified or retained past return (SPEC 2f). No in-tree callback does either. Should SPEC 7c state this for every table kind?
5. **perftest/digest.d.** Resolved: retired in 7fb4e17. It only produced ST payloads, which the compact layout now covers.
6. **Compact-geometry stale-quota regression.** The all-ST variant did not trigger the release-only race with this schedule, so full-layout (MaxCs=2) coverage is used. A compact-specific trigger would need calibration.
7. **Suspected S1/S2 and the TSan hook/comment cleanup** (above). These are pre-existing and deferred.
8. **`singletonSize()` in perftest/dual.d and throughput.d.** Resolved in 7fb4e17: it sizes with the one-word compact record, which matches the runtime's check exactly. `--full-header-size` keeps the old sizing for runs against pre-2f runtimes.

## Step 0 analysis

Base: 2f853e5. Line refs are antfarm.d at that commit.

### 1. Per-payload header fields (PayloadHeader, 16 words = 128 B, antfarm.d:601)

| Word | Field | Writer | Reader on the ST single-shot path | Mutable? |
| ---: | --- | --- | --- | --- |
| 0 | `maxCs` (lo32), `done` (hi32) | producer raw store at write (1747) | `enterPayload` raw load (2389) to select the ST fast path; **the callback, via `head`** | no (written once per lap) |
| 1 | `plen` | producer raw (1748) | `enterPayload` raw load to slice the body (2392); the callback via `head` | no |
| 2..7 | filler | producer raw zero (1749) | nobody | no |
| 8 | `pcount` | producer raw zero (1750); **the ST consumer raw-stores `1<<32`** (2401) | the callback via `head` (review_torture `claimContractCb` asserts `pcount == 1<<32`, maxCs/done == 1, iteration 0); MT payloads only: `tertiaryMt` load and `enterPayload` RMWs | **yes**, the only mutable word |
| 9 | `call` | producer raw (1751) | `enterPayload` raw load (2393) | no |
| 10..15 | filler2 | producer raw zero | nobody | no |

Table-level words (Thead, Tindex, Tprogress, Tcount) are not per-payload and are unchanged.

Other per-payload layout readers outside the Farm core:
- `review_torture/boundary_runoff.d` hard-codes the ST singleton geometry (overhead 55 words; it fatals unless each table is exactly 512 words). Its compile-time-only `ObserveUncheckedOverwrite` negative control also reads the body at `poff+16`.
- `perftest/digest.d` reimplements a consumer over its own hand-shuffled tables and reads `PayloadHeader` in the ring.
- `perftest/dual.d` and `throughput.d` `singletonSize()` assume a 16-word header. They are used only to skip unsizable configs, so with compact they become conservative overestimates.

### 2. Can an ST payload's mutable state be observed by anyone but its runner?

No.

- **Iterating payloads.** There are none on the ST path. The Farm ignores callback return values (SPEC 7c, "Callback return values are presently ignored by Farm accounting"). The iteration count is `Done`, and `validatePayloadHeader` forces `Done == 1` whenever `MaxCs == 1` (`done > 1` requires `maxCs >= 2`). `MAX_PAYLOAD_ITERS` (512) only caps MaxCs/Done. So Tmt == 0 ⇔ every payload has MaxCs == 1 ⇔ every payload is ST single-shot.
- **Sweeper or re-walker taking over mid-payload.** This cannot happen. A Tcount claim hands out a whole run (chunk), and the claimant executes it synchronously to the end. Sweepers and re-walkers (`processShard` with `checkFirst`) claim new runs through the same counter. The first-claimant yield (5e-m) happens only between runs. No path takes over a claimed run or payload. `secondaryMt` and `tertiaryMt` walk only the MT index, which is empty when Tmt == 0.
- **Stale or diagnostic paths.** The audit hooks receive only (wret, wtprime). Nothing in antfarm.d reads an ST payload header except `enterPayload`. Out of tree, `boundary_runoff`'s negative control is compile-time-only and geometry-dependent (see 1).
- **Completion accounting.** Shc, Tprogress, Sd and the completion hook are all table-level: Tcount words, Tprogress, Thead[5] Tsize and Thead[7] hook. None reads Pcount. SPEC 7c: "no ST completion depends on Pcount".
- **The one observer is the callback itself.** The callback ABI passes `PayloadHeader* head` into the ring. SPEC 7c promises: "preserves the callback-visible claims value of one and calls/completions values of zero". review_torture's `claimContractCb` checks `head.maxCs`, `head.done` and `head.pcount`. All production callbacks (actors, private actors, waves, templates, fibers, threadpool) ignore `head`.

⇒ The state is visible only to the running consumer's own callback. A consumer-local scratch `PayloadHeader` (maxCs=1, done=1, plen, pcount=1<<32, call), refreshed per payload, preserves every observable value with no restriction. It lives on the stack rather than in the ConsumerView, because a callback may drain a different view on the same thread.

### 3. What does the reader need to locate body and length?

- **Body start:** the existing per-table Tindex already stores each payload offset `poff`. In the compact layout it points at the `call` word, and the body is at `poff + 1`.
- **Body length:** this is needed because `PayloadBody` is a slice and callbacks check `body.length` (templates fatal on short bodies; T21 checks the exact length). Payloads are emitted contiguously in Tindex order, so `plen_i = poff_{i+1} - poff_i - 1`, with `poff_Tlen = Tsize - END_PAD` from Thead[5], which is already published. This adds no word per payload, but it does rely on the existing per-table index. A shard run already loads every `poff_i`, so carrying the next entry costs about one extra index load per run, plus a Thead[5] load when the run ends at Tlen-1.
- The alternative is a per-payload size word, `[call][plen][body]`. It costs one more word per entry and needs no end-of-table rule.

### 4. Words per entry

The table base is `30 + 8*SqCs` words: 38 at SqCs=1 (0–2 consumers), 54 at SqCs=3 (6 consumers). It is unchanged.

| Payload kind | body | current (index + 16 hdr + body) | compact (index + call + body) |
| --- | ---: | ---: | ---: |
| private root/child activation (writeTracked, bodyWords=1) | 1 | 18 | 3 |
| public actor (`farm.write(actorHeader, …, 2)`) | 2 | 19 | 4 |
| actor wave (writeTracked, bodyWords=3) | 3 | 20 | 5 |
| generic N-word ST | N | N+17 | N+2 |
| any table with Tmt != 0 | — | unchanged | unchanged |

At actor_churn's small quota of 16384 words, a one-word private table currently admits about 907 entries and about 5443–5448 entries compact. A default-batch (256) private table is 4646–4662 words now and 806–822 compact, so quota does not limit default batches in either case. Batch 1024 is currently quota-truncated at 907; compact admits all 1024.

### Writer sizing for heterogeneous ranges

The table format depends on the accepted prefix's `m`. The scan keeps both running payload sums (full and compact) and uses `cm == 0 ? compact : full` for the candidate size. If adding an MT payload switches the table to full format and no longer fits, the scan breaks, so the accepted prefix stays compact. The prefix stays greedy, as now. `prepareTableFrontN(n)` and the `n` return semantics are untouched.

### Decisions (orchestrator, at the step-0 STOP points)

- **A: callback ABI.** Use a stack-local scratch `PayloadHeader`: maxCs = done = 1, pcount = 1<<32, zero filler, set once per claimed run; plen and call set per payload. The ABI and the SPEC 7c observable values are unchanged.
- **B: body location and length.** Use the index-difference rule over the existing Tindex, with no added per-payload word. The compact record is exactly `[Call][Pbody...]`.
- **C → C′: singleton contract check.** The first decision was to keep the full-header size. Implementing it showed that a single ST payload could no longer form an exact-quota table, which two existing tests (boundary_runoff, write_protection) rely on. Revised to C′: an ST payload's singleton check uses the compact size, and MT keeps the full size. The fatal set only shrinks, and `write() == 0` still means full.
- **Layout-dependent harnesses.** boundary_runoff and write_protection were retuned to the compact geometry, keeping their exact-geometry intent. perftest/digest.d (not in `make check`) shuffles Tindex in place and reads ring headers, so porting it is not small. It refused compact tables loudly until round 2 retired it (7fb4e17).
