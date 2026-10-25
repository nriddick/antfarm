# Warm Fiber dispatch experiment, after the 2.1.0 Fiber rework — 25 October 2026

The same mechanism experiment as [the morning run](../coroutines-2026-10-25/README.md), repeated on `e44e0f9` after the `ant_fibers` rework (claim-word waits, retirement in place of `takeCompletions`/`release`, the record ring, `serve`). The raw modes are unchanged code and serve as the session's reference; the **FiberDomain** column is the production scheduler as it now stands. Read the morning README for the method and the mode definitions; only what changed is repeated here.

## Measured rates

Ryzen 9 5900XT, Linux x86-64, LDC 1.43.0, `-O3 -release -mcpu=native`. Five independently started processes per cell in a fixed shuffled order; median, with min–max in parentheses, in **millions of completed warm Fiber lifetimes per second**. 64 retained 64 KiB Fibers per dispatch thread, four warm executions per Fiber before timing, timed runs calibrated near 0.4 s, GC collection disabled with allocations counted.

### 0 yields per lifetime

| Threads | Direct reuse | Farm ring (chunk 8) | Deque LIFO | Deque + oldest/32 | FiberDomain 2.1.0 |
| --- | --- | --- | --- | --- | --- |
| 1 | 27.91 (27.79–28.20) | 21.21 (20.69–21.81) | 20.11 (18.98–20.24) | 19.83 (19.62–19.93) | 6.42 (6.37–6.45) |
| 2 | 37.32 (37.07–41.71) | 21.83 (10.74–22.10) | 34.59 (28.58–35.09) | 34.26 (29.72–34.60) | 4.83 (4.77–4.85) |
| 4 | 91.89 (88.44–96.88) | 32.13 (15.56–33.42) | 56.77 (50.00–57.48) | 55.73 (48.33–56.73) | 4.68 (4.66–4.73) |
| 8 | 183.24 (135.18–190.60) | 26.54 (25.51–27.43) | 68.98 (66.25–70.74) | 70.68 (68.86–73.20) | 4.71 (4.48–4.74) |
| 16 | 285.90 (208.16–314.37) | 18.22 (17.92–38.02) | 58.30 (56.70–58.53) | 59.17 (58.66–60.17) | 2.35 (2.32–2.36) |
| 32 | 317.28 (232.87–346.16) | 36.19 (35.74–36.53) | 119.29 (108.61–121.20) | 125.92 (111.96–126.42) | 1.65 (1.63–1.67) |

### 3 yields per lifetime

| Threads | Direct reuse | Farm ring (chunk 8) | Deque LIFO | Deque + oldest/32 | FiberDomain 2.1.0 |
| --- | --- | --- | --- | --- | --- |
| 1 | 6.36 (6.32–6.39) | 4.73 (4.65–4.77) | 4.61 (4.37–4.64) | 4.62 (4.59–4.66) | 2.86 (2.85–2.90) |
| 2 | 11.17 (11.10–11.34) | 5.41 (2.52–5.49) | 9.09 (8.93–9.18) | 8.94 (8.63–9.00) | 2.38 (2.38–2.39) |
| 4 | 23.03 (22.94–23.40) | 7.48 (3.57–7.79) | 15.87 (15.67–15.94) | 15.57 (15.44–15.58) | 2.52 (2.48–2.54) |
| 8 | 45.93 (45.63–46.50) | 6.39 (6.21–6.48) | 21.15 (20.85–21.58) | 21.47 (21.43–21.57) | 2.77 (2.72–2.77) |
| 16 | 73.27 (56.80–74.77) | 4.50 (4.42–4.70) | 16.19 (15.95–16.21) | 16.12 (15.98–16.20) | 1.35 (1.34–1.36) |
| 32 | 102.96 (99.67–104.13) | 7.32 (7.24–8.07) | 23.35 (23.21–23.49) | 23.78 (23.74–23.86) | 1.08 (1.07–1.08) |

Resumes/dispatches per second are the same as lifetimes at zero yields and four times them at three.

## The FiberDomain mode, before and after

The morning table and this one are different sessions: the unchanged Deque LIFO mode moved from 141.77 to 119.29 M/s at 32 threads between them, so the 32-thread cells are not comparable across sessions. For the production scheduler the comparison was therefore taken within one session, three repeats each, `domain` mode only: the pre-rework scheduler built from `769eb3c` in a worktree, and `e44e0f9`.

| Threads | 769eb3c, 0 yields | 2.1.0, 0 yields | 769eb3c, 3 yields | 2.1.0, 3 yields |
| --- | --- | --- | --- | --- |
| 1 | 6.25 | 6.42 (+3%) | 2.88 | 2.88 (0%) |
| 2 | 4.90 | 4.84 (−1%) | 2.40 | 2.39 (0%) |
| 4 | 4.81 | 4.72 (−2%) | 2.56 | 2.54 (−1%) |
| 8 | 4.96 | 4.76 (−4%) | 2.84 | 2.81 (−1%) |
| 16 | 2.50 | 2.38 (−5%) | 1.38 | 1.35 (−2%) |
| 32 | 1.82 | 1.64 (−10%) | 1.15 | 1.07 (−6%) |

GC allocation per lifetime went from 13.9–47.6 bytes to **zero** in every cell: `takeCompletions`' result array is gone, and so are the array copies on root release and timer removal.

The rework's first measurement, with lifecycle records emitted for every task into the record ring, ran 8–17% below `769eb3c` at every count (5.77 at one thread, 1.57 at 32). Two ring operations per lifetime on one shared tail line is that cost; a domain without an `onRecord` handler therefore emits no records, which is what this table measures. With a handler, that cost is what the handler buys.

What remains at 16–32 threads is the Director: one thread admits every task, flushes every activation and retires every completion while 31 workers consume, so the mode measures the admission and retirement path under contention rather than dispatch. The retirement path takes the admission, roots and pool mutexes once per batch of 64, as the old release path did per call; the difference is within a few percent of a Director-bound 1.6 M lifetimes/s and is the number to beat for the [replacement executor](../../design/coroutine-replacement.md), whose per-worker admission and completion would remove the Director from that path.

## Figures and data

[Zero-yield SVG](warm-0-yields.svg), [three-yield SVG](warm-3-yields.svg); regenerate with `python3 tools/coroutines/plot.py docs/performance/coroutines-2026-10-25-rework`. [metadata.json](metadata.json) has the compiler command, source and binary hashes (the tree is `e44e0f9` with the handler-gated record emission applied), CPU placement and host; [samples.jsonl](samples.jsonl) every timed sample, [calibration.json](calibration.json) the pilots, [summary.json](summary.json) medians and spreads. The same-session `769eb3c` comparison was run from a detached worktree with `--modes domain --repeats 3` and is not retained beyond this table.
