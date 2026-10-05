## E2-1 private destructors — 2026-10-05

All eight POD comparisons pass the −3% median gate against local main
`3fd4182`. Worst regression: **−2.60%** (`private-remote`, six consumers).
Favourable changes above +3% are reported below; they are not failures.

LDC 1.43.0, `-O2 -release`, real mimalloc v3.5.3 from the main checkout's
prebuilt archive (read only). Ordinary 8 MiB Farm, small tier, quota 16,384
words, 4,096 actors, batch 256, five warmup cycles, three-second windows.
Five alternating base/candidate pairs per POD configuration, reversing order
on successive pairs; five candidate-only `dtor` runs. Controller CPU 0,
consumers 1..6 pinned. Tests and soak finished before measurements.
The base executable was compiled from a `git archive 3fd4182`; the candidate
includes the new `dtor` benchmark mode. All per-cycle checks passed.
`tree-results` is absent from this baseline benchmark, so it has no gate row.

| Mode | Consumers | Base median M/s | E2-1 median M/s | Change |
| --- | ---: | ---: | ---: | ---: |
| private | 0 | 36.375 | 38.247 | +5.15% |
| private-remote | 0 | 37.011 | 38.864 | +5.01% |
| tree | 0 | 53.901 | 53.812 | -0.17% |
| tree-wait | 0 | 54.496 | 53.994 | -0.92% |
| dtor | 0 | — | 38.144 | new mode |
| private | 6 | 26.353 | 26.471 | +0.45% |
| private-remote | 6 | 12.962 | 12.624 | -2.60% |
| tree | 6 | 19.790 | 19.588 | -1.02% |
| tree-wait | 6 | 19.535 | 19.396 | -0.71% |
| dtor | 6 | — | 26.903 | new mode |

The +5.15% / +5.01% single-consumer gains retain the same number of POD actor
allocations, callbacks and frees; the results do not establish a universal
speedup. `dtor` moves a non-copyable state and performs the completion counter
and cycle-stamp writes in `~T`, rather than the user callback. It measures
stored-state destruction with emitter byte reclamation; moved-from `.init`
destruction is a no-op. Its medians are 38.14 / 26.90 million lifetimes/s.

[Raw runs, JSON medians, builds and verification](./)
include compiler versions, binary/library hashes and six isolated mutation diffs.
