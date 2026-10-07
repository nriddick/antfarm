# RF1: mimalloc option screen (2026-10-07)

Measurement only. **No runtime or default changes from this screen.** The
baseline side of every gate ran mimalloc's defaults; no option configuration
is relabelled as a baseline.

## Method

- Host: Ryzen 5 5500, LDC 1.43.0 `-O2 -release`, ordinary pages, pinned
  mimalloc v3.5.3 (`mimalloc_version=30503`), 16,384 actors, batch/quota
  256/16,384, five warmups, 3-second windows, CPUs 0..6 (controller plus six
  consumers on distinct cores), monitor on CPU 7.
- Every gate is `perf_gate.py --control` on runtime `c3e7521` with the common
  benchmark `8143463`. Both sides run the **same byte-identical binary**
  (sha256 `b8d69f17…`, identical across all eight gates). The only difference
  is the candidate side's environment (`--candidate-env`), which is recorded in
  each `metadata.json` and per run in `runs.jsonl` (`extra_env`).
- Options are set with `MIMALLOC_*` environment variables. mimalloc reads them
  when options are first used, before any theap caches them, so they are fixed
  from process start. Thread-pool membership uses the benchmark-only hook
  `ANTFARM_CHURN_MI_THREADPOOL=1`, which calls `mi_thread_set_in_threadpool()`
  first thing on the controller and on every consumer thread. In 3.5.3 that
  call initializes the thread's default theap before setting the flag, so the
  thread-pool quartering of `page_full_retain` at theap init does not apply.
  Only the reclaim-on-free decisions change: originating reclaim is capped by
  `page_cross_thread_max_reclaim`, and cross-thread reclaim is disabled.
- Each run prints the effective values (`mi_threadpool`,
  `mi_page_reclaim_on_free`, `mi_page_full_retain`, `mi_page_max_reclaim`,
  `mi_page_cross_thread_max_reclaim`) and `peak_rss_kib` (VmHWM). Every base
  run in every gate shows the defaults (0 / 0 / 2 / -1 / 32); every candidate
  run shows exactly its labelled change.
- `page_cross_thread_max_reclaim` is read only for cross-thread reclaim
  (`reclaim_on_free=1`) or by thread-pool threads. Alone it is inert by
  source, so it was screened together with `reclaim_on_free=1`, at -1
  (unlimited; default 32).
- Screen: tree, tree-wait, tree-cont, private-remote and private at 0 and 6
  consumers. The 0-consumer cells are the sanity set (one thread allocates and
  frees). Five clean pairs per cell, then 20-pair `--boundary` confirmations
  of the largest effects. Only 4 runs in total were contaminated; they were
  discarded and retried.
- Reproduce with [run-rf1.sh](run-rf1.sh); tables come from
  `summarize.py DIR...`. Console logs are `*.console.log`; the first attempt
  at the five option configs tripped the initial-load guard right after the
  control and wrote only failure stubs, which were deleted before the rerun
  (`driver.log`).

At five pairs a decision is an all-pairs screen. `inconclusive` in the
0-consumer cells of the control itself (tree/0 had one +22.85% pair) shows how
noisy those cells are at n=5. Peak RSS is VmHWM of the whole process
(8 MiB ring and consumer stacks included). It did not move materially in any
configuration: medians are within ±2 MiB, with tree-wait/6 flipping between
about 20.1 and 22.2 MiB in base and candidate alike.

## Results (candidate = option, base = defaults)

### control-baseline

exit=2 candidate_env=(none) base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | inconclusive | +1.09% | -3.37..+22.85% | 62.532 | 63.018 | 18120 | 18124 |
| tree-wait/0 | inconclusive | -1.09% | -3.71..+0.65% | 62.778 | 62.618 | 18136 | 18136 |
| tree-cont/0 | pass | -0.68% | -1.48..+1.05% | 25.904 | 25.933 | 18096 | 18076 |
| private-remote/0 | pass | +0.58% | -0.65..+4.27% | 31.204 | 31.377 | 18240 | 18092 |
| private/0 | pass | +0.10% | -1.12..+2.93% | 31.685 | 31.717 | 18040 | 18016 |
| tree/6 | pass | +0.39% | -1.16..+1.22% | 54.479 | 54.677 | 22156 | 22324 |
| tree-wait/6 | pass | +0.18% | -1.54..+0.47% | 55.259 | 55.228 | 20196 | 20184 |
| tree-cont/6 | pass | -0.25% | -1.03..+0.11% | 31.491 | 31.316 | 22176 | 22164 |
| private-remote/6 | pass | +0.03% | -0.14..+0.20% | 14.481 | 14.472 | 18248 | 18204 |
| private/6 | pass | -0.20% | -0.47..+0.17% | 22.743 | 22.685 | 18056 | 18196 |

### page_full_retain-neg1

exit=1 candidate_env=MIMALLOC_PAGE_FULL_RETAIN=-1 base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | regression | -11.14% | -12.60..-6.76% | 63.531 | 55.744 | 18208 | 18092 |
| tree-wait/0 | regression | -6.86% | -7.54..-4.87% | 62.637 | 58.337 | 18076 | 18080 |
| tree-cont/0 | pass | -0.42% | -1.08..+0.68% | 26.012 | 26.059 | 18176 | 18072 |
| private-remote/0 | pass | -1.13% | -2.37..+1.20% | 31.283 | 31.011 | 18172 | 18120 |
| private/0 | pass | -0.55% | -1.18..+1.00% | 31.391 | 31.292 | 18160 | 17980 |
| tree/6 | pass | +0.03% | -0.63..+0.91% | 54.871 | 54.893 | 22196 | 22200 |
| tree-wait/6 | pass | +0.25% | -0.69..+0.72% | 55.115 | 55.276 | 20168 | 22252 |
| tree-cont/6 | pass | +2.48% | +2.22..+3.11% | 31.262 | 32.096 | 22244 | 22108 |
| private-remote/6 | pass | +14.32% | +14.12..+14.40% | 14.446 | 16.508 | 18260 | 18196 |
| private/6 | pass | -0.19% | -0.51..+1.22% | 22.829 | 22.814 | 18128 | 18184 |

### page_reclaim_on_free-1

exit=0 candidate_env=MIMALLOC_PAGE_RECLAIM_ON_FREE=1 base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | pass | -0.02% | -1.86..+2.12% | 63.611 | 64.135 | 18024 | 18092 |
| tree-wait/0 | pass | +3.14% | -2.81..+6.54% | 62.556 | 63.216 | 18096 | 18168 |
| tree-cont/0 | pass | -0.25% | -0.68..+0.96% | 26.314 | 26.198 | 18128 | 18112 |
| private-remote/0 | pass | +0.21% | -0.61..+2.24% | 31.398 | 31.463 | 18120 | 18040 |
| private/0 | pass | -0.24% | -1.01..+0.34% | 31.947 | 31.945 | 18080 | 18168 |
| tree/6 | pass | +0.40% | -0.92..+1.46% | 54.373 | 54.591 | 22256 | 22272 |
| tree-wait/6 | pass | +0.43% | -0.15..+0.75% | 55.198 | 55.374 | 20164 | 22060 |
| tree-cont/6 | pass | -0.70% | -2.56..-0.38% | 31.412 | 31.159 | 22192 | 22232 |
| private-remote/6 | pass | -0.18% | -1.24..+0.08% | 14.496 | 14.469 | 18204 | 18200 |
| private/6 | pass | -0.30% | -2.35..+0.29% | 22.765 | 22.698 | 18172 | 18300 |

### page_reclaim_on_free-neg1

exit=1 candidate_env=MIMALLOC_PAGE_RECLAIM_ON_FREE=-1 base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | regression | -18.42% | -30.77..-11.02% | 62.319 | 50.031 | 18084 | 18100 |
| tree-wait/0 | regression | -15.82% | -19.21..-15.43% | 60.857 | 51.230 | 18092 | 18064 |
| tree-cont/0 | regression | -10.36% | -10.50..-10.03% | 26.045 | 23.329 | 18088 | 18008 |
| private-remote/0 | regression | -11.67% | -12.18..-11.28% | 31.142 | 27.514 | 18180 | 18128 |
| private/0 | regression | -12.48% | -12.98..-11.41% | 31.813 | 27.883 | 18132 | 18060 |
| tree/6 | pass | +0.16% | -0.60..+0.43% | 54.525 | 54.761 | 22152 | 22148 |
| tree-wait/6 | pass | -0.55% | -0.87..+0.61% | 55.408 | 55.346 | 20064 | 22088 |
| tree-cont/6 | pass | +0.19% | +0.07..+1.09% | 31.415 | 31.511 | 22284 | 20216 |
| private-remote/6 | pass | -0.16% | -0.45..+0.08% | 14.488 | 14.471 | 18124 | 18168 |
| private/6 | regression | -24.49% | -24.69..-23.56% | 22.839 | 17.315 | 18152 | 18188 |

### reclaim_on_free-1+cross_thread_max_reclaim-neg1

exit=2 candidate_env=MIMALLOC_PAGE_RECLAIM_ON_FREE=1 MIMALLOC_PAGE_CROSS_THREAD_MAX_RECLAIM=-1 base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | inconclusive | -0.09% | -3.47..+12.25% | 63.711 | 62.754 | 18132 | 18168 |
| tree-wait/0 | inconclusive | +0.50% | -4.11..+1.49% | 63.484 | 63.371 | 18072 | 18068 |
| tree-cont/0 | pass | -1.50% | -1.84..-0.76% | 26.198 | 25.805 | 18204 | 18016 |
| private-remote/0 | pass | +0.55% | -0.93..+0.85% | 31.226 | 31.434 | 18052 | 18148 |
| private/0 | pass | +0.25% | -0.32..+1.36% | 31.586 | 31.637 | 18040 | 18188 |
| tree/6 | pass | -0.56% | -1.24..+0.22% | 55.054 | 54.704 | 22220 | 22120 |
| tree-wait/6 | pass | -0.24% | -0.49..-0.08% | 55.353 | 55.136 | 20256 | 20184 |
| tree-cont/6 | pass | -0.02% | -0.29..+0.21% | 31.270 | 31.286 | 22176 | 22268 |
| private-remote/6 | pass | +0.10% | -0.12..+0.40% | 14.490 | 14.493 | 18168 | 18228 |
| private/6 | pass | -0.50% | -0.85..+0.27% | 22.753 | 22.770 | 18108 | 18196 |

### thread_in_threadpool

exit=1 candidate_env=ANTFARM_CHURN_MI_THREADPOOL=1 base=c3e7521 candidate=c3e7521 repeats=5

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | pass | +2.15% | -0.15..+8.08% | 62.566 | 63.815 | 18048 | 18060 |
| tree-wait/0 | inconclusive | -0.04% | -3.53..+5.54% | 62.224 | 63.842 | 18192 | 18132 |
| tree-cont/0 | pass | +0.03% | -0.39..+2.02% | 26.313 | 26.355 | 17968 | 18104 |
| private-remote/0 | pass | +0.06% | -0.84..+1.38% | 31.150 | 31.172 | 18032 | 18040 |
| private/0 | regression | -4.83% | -11.43..-3.98% | 31.713 | 30.049 | 18092 | 18044 |
| tree/6 | pass | +0.04% | -0.98..+0.71% | 54.688 | 54.710 | 22288 | 22216 |
| tree-wait/6 | pass | -1.04% | -1.22..-0.58% | 55.402 | 54.897 | 20152 | 20088 |
| tree-cont/6 | pass | -0.14% | -0.58..-0.02% | 31.397 | 31.346 | 22260 | 22236 |
| private-remote/6 | pass | -0.38% | -0.72..-0.28% | 14.487 | 14.412 | 18260 | 20220 |
| private/6 | pass | -0.06% | -2.21..+0.61% | 22.853 | 22.796 | 18204 | 18160 |

### boundary-page_full_retain-neg1-6

exit=0 candidate_env=MIMALLOC_PAGE_FULL_RETAIN=-1 base=c3e7521 candidate=c3e7521 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| private-remote/6 | pass | +13.86% | +13.19..+14.20% | 14.608 | 16.633 | 18200.0 | 18262.0 |
| tree-cont/6 | pass | +2.15% | -0.94..+6.04% | 31.352 | 32.015 | 22182.0 | 22198.0 |

### boundary-page_full_retain-neg1-0

exit=1 candidate_env=MIMALLOC_PAGE_FULL_RETAIN=-1 base=c3e7521 candidate=c3e7521 repeats=20

| cell | decision | paired median | min..max pair | base median | cand median | base RSS KiB | cand RSS KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tree/0 | regression | -10.82% | -15.64..-4.40% | 63.652 | 56.712 | 18046.0 | 18078.0 |
| tree-wait/0 | regression | -7.18% | -15.71..-1.69% | 62.770 | 58.538 | 18044.0 | 18020.0 |


## Findings

| Configuration | Effect | Verdict |
| --- | --- | --- |
| `page_full_retain=-1` (never abandon full pages) | private-remote/6 **+13.9%** (20 pairs, all +13.2..+14.2%), tree-cont/6 +2.2% (20 pairs); but tree/0 **−10.8%** and tree-wait/0 **−7.2%** (20 pairs, regression). Others flat. RSS unchanged. | Workload-specific trade-off; not a default. |
| `page_reclaim_on_free=1` | No cell moved beyond noise. | No benefit here. |
| `page_reclaim_on_free=-1` | private/6 **−24.5%**; every 0-consumer cell −10% to −18%. | Harmful; avoid. |
| `reclaim_on_free=1` + `page_cross_thread_max_reclaim=-1` | No cell moved beyond noise. | No benefit here. |
| `mi_thread_set_in_threadpool` on all threads | private/0 −4.8% (pairs −11.4..−4.0%), tree-wait/6 −1.0%; others flat. | No benefit; slight harm. |

Interpretation, as hypotheses rather than measured mechanisms:
`private-remote` allocates roots on the emitter and frees them on consumers.
Retaining full pages keeps them owned by the emitter instead of abandoning
them, which plausibly avoids abandon/reclaim traffic; the remote CAS remains.
Single-thread tree churn instead pays to keep more full pages in its queues.
Disabling reclaim-on-free entirely (-1) prevents even originating-theap
reclaim of the allocator thread's own abandoned pages. That hurts exactly the
workloads that allocate and free on one thread.

None of these options removes the per-block remote CAS that dominates tree/6
fills (`perf-g3-2026-10-07`); no tree/6 or tree-wait/6 cell improved. That
motivates RF2's storage change rather than allocator tuning. Application
guidance is in [MIMALLOC.md](../../MIMALLOC.md#option-screen-rf1).
