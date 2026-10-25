# Warm Fiber dispatch experiment — 25 October 2026

This is a mechanism experiment, not a production replacement or an I/O benchmark. The production `ant_fibers` implementation is unchanged. The [replacement proposal](../../design/coroutine-replacement.md) uses these results to choose the next prototype.

## Measured rates

Ryzen 9 5900XT, Linux x86-64, LDC 1.43.0 (DMD frontend 2.113.0, LLVM 22.1.8), `-O3 -release -mcpu=native`. Five independently started processes per cell, in a fixed shuffled order; median below, min–max in parentheses. Units are **millions of completed warm Fiber lifetimes per second**. A lifetime includes reset/reuse after completion; a dispatch is one `Fiber.call`, which switches in and back out.

64 retained 64 KiB Fibers per dispatch thread (64–2,048 Fibers). Thread count includes the calling thread; there is no extra polling thread. Four full executions warm each Fiber before timing. Setup, thread creation and stack allocation are excluded. Timed runs are calibrated near 0.4 s. GC collection is disabled, but allocations remain enabled and are counted. Each completion count and body checksum is checked after the run; the managed mode checks that its final task identities match the warmed pool.

CPUs 0–7 share the first LLC, 8–15 add the second LLC, and 16–31 add SMT siblings. Frequency boost is enabled. This is a workstation run, without frequency isolation; outlier ranges and non-monotonic scaling are retained.

### 0 yields per lifetime

| Threads | Direct reuse | Farm ring (chunk 8) | Deque LIFO | Deque + oldest/32 | Existing FiberDomain |
| --- | --- | --- | --- | --- | --- |
| 1 | 28.67 (27.68–28.76) | 22.08 (21.77–22.25) | 20.69 (19.31–20.79) | 20.47 (19.37–20.68) | 6.27 (6.18–6.33) |
| 2 | 39.30 (37.97–53.41) | 23.05 (22.64–23.12) | 36.38 (33.86–39.78) | 34.79 (33.57–37.07) | 4.86 (4.82–4.91) |
| 4 | 90.96 (89.56–96.31) | 33.97 (15.67–34.96) | 57.50 (55.30–59.44) | 55.50 (54.89–61.48) | 4.72 (4.66–4.76) |
| 8 | 187.79 (174.10–189.98) | 25.77 (24.86–26.93) | 75.08 (74.24–75.58) | 76.45 (74.46–76.97) | 4.90 (4.82–4.97) |
| 16 | 165.94 (160.67–269.02) | 18.05 (17.77–37.84) | 56.37 (56.05–56.67) | 55.99 (54.32–56.52) | 2.49 (2.43–2.50) |
| 32 | 330.56 (315.47–338.26) | 35.80 (34.90–37.28) | 141.77 (141.16–152.93) | 150.90 (141.80–153.41) | 1.85 (1.81–1.86) |

Resumes/dispatches per second are the same as completed lifetimes.

### 3 yields per lifetime

| Threads | Direct reuse | Farm ring (chunk 8) | Deque LIFO | Deque + oldest/32 | Existing FiberDomain |
| --- | --- | --- | --- | --- | --- |
| 1 | 6.47 (6.21–6.54) | 4.79 (4.75–4.83) | 4.64 (4.62–4.68) | 4.62 (4.53–4.67) | 2.86 (2.81–2.90) |
| 2 | 11.60 (11.37–11.92) | 5.46 (5.06–5.51) | 8.97 (8.04–9.04) | 8.79 (8.73–8.85) | 2.39 (2.38–2.40) |
| 4 | 23.40 (22.63–23.54) | 7.33 (3.96–7.54) | 14.95 (14.93–15.06) | 14.84 (14.65–14.93) | 2.54 (2.52–2.56) |
| 8 | 46.65 (46.60–47.12) | 6.29 (6.22–6.48) | 21.49 (21.10–21.51) | 21.63 (21.61–21.65) | 2.84 (2.81–2.84) |
| 16 | 66.52 (64.44–76.83) | 9.01 (4.63–9.16) | 16.17 (16.08–16.25) | 16.05 (15.95–16.20) | 1.38 (1.37–1.38) |
| 32 | 103.00 (99.82–104.61) | 8.25 (6.93–8.40) | 23.18 (23.05–23.24) | 23.73 (23.64–23.75) | 1.17 (1.15–1.18) |

Resumes/dispatches per second are **four times** these rates: three yields, then a terminal return.

## What the modes mean

- **Direct reuse:** each thread repeatedly calls and resets its own warmed Fibers without a concurrent dispatch structure. A yielded Fiber is immediately resumed. This is a reference ceiling, not an executor.
- **Farm ring:** the same ticket/Fiber body travels through one shared 32 MiB Farm. Every worker owns a producer token and consumer view. Returned tickets collect into a batch; publication occurs outside the Farm callback. Tables use `avgCostForChunk(8)`. Rejected prefixes remain owned in staging and retry. This is the raw queue baseline, not the existing FiberDomain.
- **Deque LIFO:** the existing bounded Chase–Lev `forkjoin.deque` stores stable ticket pointers. Each worker pushes/pops its own end; thieves probe other workers. A resumed Fiber is requeued only after `Fiber.call` returns. There is no Farm injection or spill; total ticket count bounds every deque and its staging storage.
- **Deque + oldest/32:** the same experiment takes the oldest local item every 32 dispatches to reduce LIFO monopolization. This tests throughput cost, not a proof of starvation freedom or tail latency.
- **Existing FiberDomain:** the public production API performs spawn, ready publication, ring dispatch, completion drain and release in batches of all warm slots, with timers/waits/cancellation unused and lifecycle logging disabled. Its body uses `FiberDomain.yieldReady`. The calling thread is also the admission/completion director. This includes bookkeeping absent from the raw modes; ratios against it are opportunity estimates, not a queue-only speedup.

## Observations

1. At one thread the ring wins against the deque: 22.08 vs 20.69 M completed lifetimes/s without yields; 4.79 vs 4.64 M with three yields. A deque has an owner-pop barrier even when stealing is unnecessary.
2. At 32 threads the deque wins against the shared ring: 141.77 vs 35.80 M lifetimes/s without yields (3.96×); 23.18 vs 8.25 M with three yields (2.81×). The oldest/32 policy measures 150.90 and 23.73 M respectively, with no apparent throughput penalty in this run. That does not establish a latency guarantee.
3. The mechanism is strongly associated with locality. At 32 threads the ring changes the executing thread on about 94.8% of successive ticket resumptions; the LIFO deque changes it on about 0.08%, and oldest/32 on about 0.004–0.005%. The benchmark counts that directly. Queue overhead and migration/cache effects are not independently isolated.
4. The current FiberDomain scales poorly for this deliberately tiny workload: 6.27 → 1.85 M lifetimes/s from 1 → 32 threads without yields; 2.86 → 1.17 M with three yields. Source inspection shows serialized admission/root/pool operations and completion-array allocation. This experiment does not provide a CPU profile that attributes a percentage to each one.
5. Raw call/reset/dispatch modes report **zero timed GC bytes** on every worker. Existing FiberDomain reports approximately 13.9–47.6 GC bytes per completed lifetime, varying with batch size; stack reuse therefore does not imply allocation-free lifecycle management. Its `takeCompletions` allocates a result array, and other control-path allocations may contribute.
6. Scaling is not monotonic at the LLC/SMT boundaries, and some 2/4/16-thread samples have wide ranges. Do not fit a linear scalability curve or choose a production topology from these cells alone.

## Limits and next measurements

No raw mode implements generation-safe handles, concurrent cancellation, event/timer registration, external MPSC admission, priority/affinity restrictions, shutdown racing admission, GC-on behavior, sleeping-worker wakeups, I/O, or exception cleanup tests. The raw deque is not a drop-in replacement for `FiberDomain`. Repeating a very small body favors keeping one warm stack on one worker; a production scheduler must also meet fairness and latency bounds.

The next comparable prototype must retain the FiberDomain lifecycle guarantees while changing admission, ready dispatch, completion and pool ownership together. Measure fixed total pool sizes as well as fixed slots/worker; same-LLC vs cross-LLC placement; one/many external producers; skewed bursts; warm idle-to-ready latency; allocations/RSS; and p50/p99 completion/wakeup latency. A custom context switch has not been benchmarked here, nor has any async-I/O throughput or latency.

The [interactive figure and rate table](explorer.html) is a self-contained HTML artifact. The static figures are available as [zero-yield SVG](warm-0-yields.svg) and [three-yield SVG](warm-3-yields.svg); regenerate them with `python3 tools/coroutines/plot.py docs/performance/coroutines-2026-10-25`.

## Reproduce and inspect

```sh
python3 tools/coroutines/run.py --output .build/coroutine-results
```

The [D experiment](../../../tools/coroutines/warm_fibers.d) and [runner](../../../tools/coroutines/run.py) retain the payload and scheduling policies. The exact compiler command, binary and source SHA-256 values, CPU placement and host details are in [metadata.json](metadata.json). [samples.jsonl](samples.jsonl) holds every timed sample, [calibration.json](calibration.json) the pilots, and [summary.json](summary.json) the medians/spreads. The parent commit is `b8a2fe8`; the metadata records the uncommitted experiment tree by source and binary hashes. Production Fiber and deque sources are unchanged from that parent.
