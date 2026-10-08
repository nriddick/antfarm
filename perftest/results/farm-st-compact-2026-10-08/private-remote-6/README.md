# private-remote/6 investigation (round 2, 2026-10-08)

**Finding.** The cheaper writer does not cause the −4.5%. The cell's mechanism is contention on mimalloc's per-page cross-thread free list, and that contention depends on Farm quota geometry. Compact tables fit about 6× more 256-entry tables in the gate's 16,384-word small-tier quota (20.3 against 3.5).
- **About three quarters of the loss (−3.44% of −4.49%) reproduces in the base build** when it gets that quota headroom alone.
- **A residual of roughly 1% is unexplained.** It could be format-related.

Evidence:
- At matched tables per quota, compact is −6.3% to +4.2% against base. The two 10-round paired points are +1.65% and −0.94%.
- The base, given the candidate's admission headroom, loses 3.4% by itself.
- Making consumers *slower* raises throughput by up to 40% in both trees.

No Farm-side fix is principled:
- The only Farm lever is quota or admission geometry, which is a default/protocol change.
- The landscape is non-monotone, so any such setting tunes to this benchmark rather than fixing a mechanism.

Nothing was changed in the runtime.

## Method

[investigate.sh](investigate.sh) rebuilds everything from git and reproduces every table below. It is diagnostic only: serial, on a quiet host, and not gate timing.
- **base** is cf2aa91 and **cand** is 5e22442. `antfarm.d` and `actors/` are identical at HEAD.
- Both use 2f853e5's `actor_churn.d`, as the gate does.
- Gate geometry otherwise: 16,384 actors, batch 256, 6 consumers on CPUs 0..6, mimalloc 3.5.3, 3-second windows, 5 warmups.

Diagnostic builds (never timed for the gate):
- [mimalloc-diag.patch](mimalloc-diag.patch): per-thread counters in `mi_free_block_mt`, covering cross-thread frees, `xthread_free` CAS attempts and newly-owned pages. Built from the pinned 3.5.3 `static.c` with `cc -O3`; it is used for counting only.
- [diagpatch.py](diagpatch.py): per-consumer claims, entries and busy fraction, plus per-table emitter counters from the root park pump.
- [spinpatch.py](spinpatch.py): rdtsc spin controls. `ANTFARM_DIAG_WRITER_TICKS` spins per entry after a compact table is emitted. `ANTFARM_DIAG_CONSUMER_TICKS` spins after each ST callback in a claimed run. Both are in-tree diagnostics only.

Quota arithmetic:
- A 256-entry one-word private table is 4646 words in the full layout and 806 words compact.
- At quota 16,384 that is 3.53 full tables or 20.3 compact tables.
- The "matched" quotas give the other layout the same tables per quota: 16384 × 806/4646 = **2842** for compact, and 16384 × 4646/806 = **94440** for full.

## Results

### Matched geometry ([matched.txt](matched.txt), 10 alternating rounds)

| comparison | paired median | min..max |
| --- | ---: | ---: |
| cand@16384 vs base@16384 (the gate cell) | −4.27% | −4.51..−4.03% |
| base@94440 vs base@16384 (base given compact's headroom) | **−3.44%** | −3.72..−2.76% |
| cand@2842 vs base@16384 (compact at full-layout tables per quota) | **+1.65%** | +0.98..+1.99% |
| cand@16384 vs base@94440 (full layout at compact tables per quota) | −0.94% | −1.30..−0.66% |

About three quarters of the gate loss (−3.44% of −4.49%) reproduces in the base binary from the quota headroom alone. These two matched points sit on the favourable side of the wider matched spread below, so the residual of roughly 1% is not shown to be layout noise. This cell does have layout sensitivity: the inert-spin base build runs 14.47 against 14.33 for the plain base.

### Matched tables per quota from the landscape ([quota.txt](quota.txt), medians of 3)

| tables per quota | base quota | base rate | compact quota | compact rate | compact vs base |
| ---: | ---: | ---: | ---: | ---: | ---: |
| ~2 (2.0 / 2.1) | 9400 | 15.87 | 1700 | 14.87 | −6.3% |
| 3.0 | 14000 | 14.95 | 2418 | 14.24 | −4.8% |
| 3.5 | 16384 | 14.34 | 2842 | 14.51 | +1.2% |
| 4.0 | 18584 | 13.62 | 3224 | 14.20 | +4.2% |
| ~10 (10.1 / 10.0) | 47000 | 14.63 | 8100 | 14.12 | −3.5% |
| 20.3 | 94440 | 13.83 | 16384 | 13.66 | −1.2% |

At matched geometry the layouts differ by −6.3% to +4.2%, and compact is lower at four of six points. The two 10-round paired points above (+1.65% at 3.5, −0.94% at 20.3) fall on the favourable side of this spread. So matching geometry does not by itself make the layouts equal.

### Quota landscape ([quota.txt](quota.txt), 3 runs each, Mactor cycles/s)

| base quota (full tables) | rate | cand quota (compact tables) | rate |
| --- | ---: | --- | ---: |
| 9400 (2.0) | 15.87 | 1700 (2.1) | 14.87 |
| 14000 (3.0) | 14.95 | 2418 (3.0) | 14.24 |
| **16384 (3.5)** | **14.34** | 2842 (3.5) | 14.51 |
| 18584 (4.0) | 13.62 | 3224 (4.0) | 14.20 |
| 28000 (6.0) | 14.95 | 4100 (5.1) | 13.99 |
| 47000 (10.1) | 14.63 | 8100 (10.0) | 14.12 |
| 94440 (20.3) | 13.83 | 12000 (14.9) | 14.20 |
| | | **16384 (20.3)** | **13.66** |

Neither layout is monotone in quota. Base spans 13.62–15.87 (about ±8%), with its best at quota 9400. Compact spans only 13.66–14.87 (about ±4%) and never reaches base's best. The gate's quota lands base near the middle of its range and compact at its lowest point.

### Remote-free contention ([cas.txt](cas.txt), [stat.txt](stat.txt), 3 runs each)

| config | rate | xthread frees/actor | CAS attempts/free | newly-owned pages/actor | instr/actor | r0243/actor | r0860/actor |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| base@16384 | 14.29 | 1.000 | 2.91 | 0.044 | 820 | 9.56 | 2.33 |
| cand@16384 | 13.61 | 1.000 | 3.07 | 0.030 | 851 | 8.72 | 2.20 |
| cand@2842 | 14.34 | 1.000 | 2.90 | 0.047 | 836 | 8.90 | 2.17 |
| base@94440 | 13.86 | 1.000 | 3.02 | 0.038 | 831 | 9.33 | 2.36 |

- Every root free is a cross-thread `mi_free_block_mt` push onto the controller's page.
- CAS attempts per free (about 3, so two failed CASes per free) order the four configurations exactly by rate, independent of layout.
- Counters are user-space totals over all seven threads, which spin when idle, divided by verified actor cycles.
- Compact has fewer cache fills per actor (r0243 −9%), and its change-to-writable count (r0860) is not higher. So the extra cycles are not Farm ring traffic.
- Profiles from round 1 put about 58% of samples in `mi_free_size_aligned` on both sides.

### Table geometry, claims and per-consumer distribution ([diag.txt](diag.txt))

| config | rate (diag build) | entries/table | quota-short tables | tables/cycle | entries/claim | busy fraction |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| base@16384 | 14.62 | 222.5 | 24.8% | 73.6 | 27.0–27.2 | 0.789 |
| cand@16384 | 13.71 | 251.2 | 4.9% | 65.2 | 27.8–28.5 | 0.802 |
| cand@2842 | 14.52 | 217.5 | 24.7% | 75.3 | 26.2–26.7 | 0.789 |
| base@94440 | 14.14 | 245.9 | 4.7% | 66.6 | 28.0–28.5 | 0.799 |
| base@14000 | 15.09 | 256.0 | 0% | 64.0 | 28.1–28.6 | 0.836 |

- **Table size is not the lever.** At batch 256 both layouts publish at most 256 entries per table. Compact's larger admission (5448 one-word entries) never applies, because the batch caps the table first.
- What changes with quota is how often the writer meets a partly reclaimed quota. Those meetings produce quota-truncated short tables and a different interleaving of consumers over the allocation pages.
- At the gate quota, base truncates a quarter of its tables to about 121 entries, and compact truncates almost none. Compact at the matched quota reproduces base's mix and its rate.
- base@14000 (exactly three tables, no truncation) is fastest of all. So short tables are not the cause either. The rate depends on the full geometry, not on one statistic.
- Claim sizes are the same: 32-entry chunks over three shards, 26–28.6 entries per claim.
- The per-consumer split has the same skew in every configuration. The CPU 4 consumer takes about 2× the entries of the others, and CPU 6, the controller's SMT sibling, polls less.
- Busy fraction does not order the rates (base@14000 is both busiest and fastest).

### Writer slowdown control ([writer-slowdown.txt](writer-slowdown.txt), median of 3)

| build | writer spin ticks/entry | rate |
| --- | ---: | ---: |
| base | – | 14.37 |
| cand-spin | 0 | 13.59 |
| cand-spin | 8 (≈ base's extra writer cost, 12.1 − 4.8) | 13.65 |
| cand-spin | 16 | 13.69 |
| cand-spin | 32 | 13.73 |
| cand-spin | 64 (5× base's whole writer cost) | 13.78 |

Recovery rises steadily with the spin, from 13.59 to 13.78 (+1.4%), but stays small against the 5.4% gap to base (14.37) in this run. **Writer speed is not the cause.**

### Consumer delay control ([consumer-delay.txt](consumer-delay.txt), median of 2)

| consumer spin ticks/callback | base rate | cand rate | CAS attempts/free, base / cand |
| ---: | ---: | ---: | --- |
| 0 | 14.47 | 13.78 | 2.86 / 3.08 |
| 10 | 14.68 | 14.26 | 2.63 / 2.75 |
| 20 | 14.67 | 14.17 | |
| 40 | **20.32** | **19.95** | 1.86 / 1.88 |
| 80 | 17.88 | 18.03 | |

Adding useless work to every consumer callback *raises* throughput by up to 40% in both trees, as CAS attempts per free fall from about 3 to 1.9. This is the signature of contention collapse.

- The cell's throughput is set by how consumers' cross-thread frees collide on the controller's mimalloc page `xthread_free` words.
- It is not set by how much Farm work each layout does.
- Any change to consumer or emitter timing or geometry moves the cell unpredictably along this landscape.

## Conclusion and options (for the user)

- **What the data supports.**
  - About three quarters of the loss (−3.44% of −4.49%) reproduces in the base build from quota headroom alone.
  - Writer speed is not the cause.
  - The mechanism is mimalloc cross-thread-free contention that depends on quota geometry.
  - A residual of roughly 1% is unexplained and could be format-related.
  - No Farm-local fix such as claim chunking or cost hints applies: tables, claims and hints are identical at batch 256.
- **Every Farm lever is a default or protocol change.** Examples: charging compact tables a minimum per-entry quota, or lowering the small-tier quota. Given the non-monotone landscape, either would tune the benchmark rather than remove a mechanism. Not done, per the round-2 instructions.
- **Where a real fix would live.** It belongs on the allocation side, for example:
  - consumer-side state recycling;
  - a cohort/slab arena for root state, as RF2 did for children;
  - spreading root-state allocation across pages.

  These are outside this prototype.
- **Gate treatment.** Waiving private-remote/6 needs the user's decision. The evidence for geometry is the base-only −3.44% from headroom alone. The evidence against a full waiver is the unexplained ~1% residual and the matched-geometry spread (−6.3% to +4.2%; the paired points −0.94% and +1.65% are on its favourable side).
