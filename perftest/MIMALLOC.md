# Actor lifetime churn with mimalloc

For the later native-malloc, arena, jemalloc, tcmalloc, and oneTBB comparison,
see [ALLOCATORS.md](ALLOCATORS.md). It includes an ordinary `malloc(16)`
control as well as the 64-byte-aligned C policy measured here.

The sustained benchmark allocates and frees actor state on **every** cycle.
It does not use D `new` for those state allocations. D allocations create
the owner/handle/completion arrays and consumer machinery once, outside the
timed loop. Runtime storage and stable actor slots are also allocated once;
their allocation follows the selected actor policy.

On Linux the default policy calls C `aligned_alloc(64, roundedSize)` and
`free`. The optional policy imports `actors.mimalloc` and passes
`mimallocV3ActorAllocator()` to `ActorRuntime.create`. That adapter calls
`mi_malloc_aligned` and `mi_free_size_aligned` with the requested size and
alignment. This benchmark's state is 16 bytes with 8-byte alignment, so the
two policies make different allocation requests by default.

## Build and run

```sh
make -C perftest actor_churn_mimalloc
export ANTFARM_HUGE_PAGES=0
./perftest/actor_churn_mimalloc actor 16384 3 0 256 5 crt
./perftest/actor_churn_mimalloc actor 16384 3 0 256 5 mimalloc
./perftest/actor_churn_mimalloc actor 16384 3 0 256 5 mimalloc64
```

Use `wave` instead of `actor` for one wave per cohort, and `6` instead of
`0` for six background consumers. Zero means the producer also consumes.
The last argument selects the allocator. Omitting it selects mimalloc in
this optional build, or C runtime in the ordinary `actor_churn` build.
The ordinary build rejects the mimalloc choices.

`mimalloc64` is a benchmark-only diagnostic wrapper around the same adapter:
it rounds byte counts to multiples of 64 and requests 64-byte alignment,
matching the C policy. The matching sized free receives those same rounded
arguments. This controls for the smaller, denser state allocation available
with the ordinary adapter. It does not change the production allocator.

The target reuses the release static-library build in `actor_torture/.build`
and builds it if needed. The source is the pinned v3.5.0 submodule at
`18b08671c9302247bfb682286e6bf3cc1773f801`; `MI_OVERRIDE=OFF` leaves ordinary
process allocation and the D runtime unchanged. `make -C perftest clean`
removes the benchmark executable but leaves that shared library build intact.
For DMD, set `DC=dmd DFLAGS='-O -release'
MIMALLOC_VERSION=-version=AntfarmMimallocV3` on the make command.

## Measurement protocol

Measured on 2026-09-24 using the same Ryzen 5 5500 host, LDC 1.43.0 / LLVM 22.1.8,
`-O2 -release`, ordinary pages, and cohort workload as [README.md](README.md).
Each run has five warm-up cycles and at least three measured seconds;
publication batch is 256. Farm/runtime/metadata setup is excluded. Creation,
dispatch, waiting, retirement, reclamation, and per-cycle validation are
included. All actor creation and freeing happens on the controlling thread,
including in the six-consumer cases.

The same benchmark source is built against original checkout `8e988f6` and
optimized runtime `e6ce4d1`. Both executables link the same mimalloc library.
Each executable supports both `crt` and `mimalloc`, selected before timing;
the allocator comparisons therefore use identical executable code layouts.
Each configuration has three serial runs with reversed/rotated order across
repeats. Values are medians, not independent samples taken from one run.
The 64-byte control additionally runs both modes on the optimized runtime
at 16,384 actors with no background consumers.

## Results

All rates below are **millions of complete actor lifetimes per second**,
including allocation and freeing. The C-runtime results are fresh runs of
the mimalloc-enabled executable with `crt` selected, rather than reused
measurements from the earlier standalone C-runtime binary.

| Actors/cycle | Background consumers | Mode | Original C | Original mimalloc | Optimized C | Optimized mimalloc |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 4,096 | 0 | Autonomous | 0.791 | 0.902 | 8.949 | 18.121 |
| 4,096 | 0 | Wave | 0.807 | 0.952 | 9.792 | 21.655 |
| 4,096 | 6 | Autonomous | 0.637 | 0.863 | 6.062 | 17.739 |
| 4,096 | 6 | Wave | 0.629 | 0.906 | 6.188 | 13.760 |
| 16,384 | 0 | Autonomous | 0.112 | 0.117 | 7.970 | 17.423 |
| 16,384 | 0 | Wave | 0.115 | 0.120 | 8.577 | 20.788 |
| 16,384 | 6 | Autonomous | 0.104 | 0.111 | 5.769 | 14.743 |
| 16,384 | 6 | Wave | 0.113 | 0.120 | 5.836 | 15.106 |

On the optimized runtime, mimalloc improves the same-thread cohort results
by 2.03–2.42×. The six-consumer medians improve by 2.22–2.93×, but these
un-pinned runs have more variation: for example, mimalloc wave throughput
at 4,096 actors ranged from 13.383 to 17.521 million lifetimes/s. Absolute
six-consumer rates also differ from the earlier standalone C-runtime runs;
the comparisons here hold the executable fixed when switching allocators.

The actor slot-search and ready-backlog improvements remain valuable when
both revisions use mimalloc. At 16,384 actors on one thread, optimized
autonomous churn is 148.93× faster than the original runtime with mimalloc;
wave churn is 173.90× faster. Switching allocators alone does not remove
the original runtime's quadratic slot search on every refill.

## Matching allocation requests

Optimized runtime, 16,384 actors per cycle, controlling thread also consuming:

| Mode | C runtime | mimalloc, natural requests | mimalloc, C-matched 64-byte requests |
| --- | ---: | ---: | ---: |
| Autonomous, million lifetimes/s | 7.970 | 17.423 | 16.949 |
| Wave, million lifetimes/s | 8.577 | 20.788 | 20.101 |

Even with matching size/alignment requests, the observed speedups are
2.13× and 2.34× respectively. Smaller allocations contribute only a small
part of the gain in this workload.

Autonomous-mode phase medians at that same cohort size, in ns/actor:

| Phase | C runtime | mimalloc | mimalloc64 |
| --- | ---: | ---: | ---: |
| Creation | 63.29 | 10.77 | 11.84 |
| Wake, publish, execute, await completion | 32.53 | 29.75 | 30.38 |
| Retire, reclaim, verify | 29.82 | 16.42 | 16.78 |

Creation and reclamation account for most of the improvement. This measures
the existing global adapter with same-thread allocation/free and tiny POD
state, not explicit mimalloc heap ownership, cross-thread freeing throughput,
or resident-memory behavior over long runs.

All 102 measured runs passed exact per-cycle execution and reclamation
checks. Allocator-selection smoke tests also passed with LDC release, DMD
release, and LDC debug linked to the real `MI_DEBUG=FULL` mimalloc library,
including non-divisible cohorts, six consumers, and the 64-byte control.
The default production actor allocator is unchanged.
