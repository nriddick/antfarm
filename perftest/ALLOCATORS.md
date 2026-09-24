# Arena and malloc comparison

This comparison keeps the actor runtime at `cf768cf` and changes only the
benchmark allocation policy. It measures complete create → dispatch → retire
→ reclaim cycles, including validation and any arena reset. Each callback
does the same tiny amount of work as in [LIFECYCLE.md](LIFECYCLE.md).

## Policies

| Policy | State allocation and reclamation |
| --- | --- |
| `crt` | Existing C policy: `aligned_alloc(64, 64)` and `free` for the 16-byte state. |
| `malloc` | `malloc(16)` and `free`, using natural allocator alignment. Runtime and stable slots still use the aligned C policy. |
| `mimalloc` | Existing direct mimalloc adapter: 16 bytes, alignment 8, sized/aligned free. |
| `mimalloc64` | Same adapter with size and alignment both rounded to 64. |
| `pool` | Existing controller-owned, fixed-size free list; individual reclaim returns each state to the list. |
| `arena` | Controller-owned bump pointer; individual state frees do nothing, followed by one reset after the entire cohort is reclaimed. |

The arena owns exactly `actors * 16` bytes of state storage. Allocation fails
on exhaustion; it does not grow or silently fall back for actor state.
Runtime and stable slots use separate C allocations and survive resets.
Backing allocation occurs before warm-up, and backing release follows runtime
destruction. All actors still get new generations, initialized state, one
callback, retirement, and reclamation in every cycle. Arena reset is timed and
occurs only after every owner is invalidated and the runtime has zero live,
ready, or stale actors. It needs known cohort capacity and a shared reclamation
boundary; it cannot free an arbitrary subset for reuse like the pool.

On Linux, `LD_PRELOAD` lets jemalloc, gperftools tcmalloc, or oneTBB replace
ordinary C allocation in the same executable. Both `malloc` and `crt` are
tested for each replacement, separating allocator choice from the 64-byte
alignment/size policy. This is process-wide interposition, so it also affects
untimed setup allocations. Mimalloc uses the existing directly linked adapter
and is built with `MI_OVERRIDE=OFF`. No replacement allocator is installed as
a project dependency or made the production default.

The benchmark reports the libraries providing `malloc`, `free`, and
`aligned_alloc` after measurement. The sweep rejects a run if a requested
replacement did not provide all three functions. Installed versions on the
measurement host: glibc 2.44, jemalloc 5.4.0, gperftools 2.18.1 (the minimal
tcmalloc library), oneTBB 2023.1.0, and pinned mimalloc 3.5.0.

## Reproduction

```sh
make -C perftest actor_churn_mimalloc
export ANTFARM_HUGE_PAGES=0
./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 arena 6,1
./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 malloc 6,1
LD_PRELOAD=/usr/lib/libjemalloc.so.2 ./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 malloc 6,1
LD_PRELOAD=/usr/lib/libtcmalloc_minimal.so ./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 malloc 6,1
LD_PRELOAD=/usr/lib/libtbbmalloc_proxy.so ./perftest/actor_churn_mimalloc wave 16384 2 1 16384 5 malloc 6,1

python3 perftest/allocator_sweep.py --output /tmp/actor-allocators \
    --actors 16384 --seconds 2 --repeats 3 --cpus 6 --cpus 6,1
```

Library paths and CPU IDs are host-specific. The sweep discovers installed
replacement libraries and explicitly reports missing ones. It runs serially
with deterministic shuffled allocator order, removes inherited allocator
tuning variables and preloads in each child process, and uses ordinary pages.
It saves the configuration, commands, provider identities, raw output,
throughput samples, and median phase timings as JSON and text. The plain
`actor_churn` build supports `malloc`, `arena`, and preloaded replacements too;
the full sweep requires the mimalloc-enabled build.

The reported throughput is for a 16,384-actor cohort with 16-byte state. All
allocation/free calls are on the controller; callbacks may run on the
consumer. This does not measure allocator scalability under concurrent
allocation/free, cross-thread frees, variable sizes, fragmentation, or RSS
over heterogeneous long-lived application workloads.
The callback reads actor state and writes a separate padded completion line;
packing states at their natural alignment does not test the false sharing
that parallel mutation of adjacent actor states could introduce.

## Results

Measured 2026-09-24 on the Ryzen 5 5500 with LDC 1.43.0 / LLVM 22.1.8,
`-O2 -release`, ordinary pages, and the same executable for every policy.
Each entry is the median of three runs of at least two seconds, after five
warm-up cycles. Autonomous batches are 256; waves offer the entire cohort.
The same-thread case uses CPU 6. The separate-consumer case uses controller
CPU 6 and consumer CPU 1, on different physical cores.

All values below are **millions of complete actor lifetimes/second**.

| Policy | Same-thread autonomous | Same-thread wave | Separate-consumer autonomous | Separate-consumer wave |
| --- | ---: | ---: | ---: | ---: |
| Existing glibc, 64-byte request | 8.358 | 9.205 | 8.395 | 9.005 |
| glibc, native `malloc` | 13.290 | 15.409 | 14.198 | 16.041 |
| jemalloc, native `malloc` | 15.226 | 18.178 | 17.126 | 19.838 |
| gperftools tcmalloc, native `malloc` | 16.545 | 20.246 | 18.787 | 22.202 |
| oneTBB, native `malloc` | 15.801 | 19.050 | 17.817 | 20.919 |
| mimalloc adapter | 18.120 | 22.434 | 20.562 | 25.150 |
| Free-list pool | 18.814 | 23.098 | 21.758 | 26.266 |
| Bump arena | **18.981** | **23.470** | **21.998** | **26.764** |

The arena adds roughly 1% over the pool for autonomous actors and 1.6–1.9%
for waves. With a separate consumer it improves on mimalloc by 7.0% and 6.4%,
respectively. Separate-consumer arena samples span 21.963–22.050 M/s for
autonomous actors and 26.597–26.846 M/s for waves; pool ranges are
21.753–21.786 and 26.247–26.298 M/s. The small pool-to-arena gain is consistent
across these samples, but much smaller than the improvement from replacing
the original 64-byte-aligned C policy.

On one thread, arena retirement/reclamation/verification takes 13.81 ns/actor
in autonomous mode and 13.77 ns in wave mode, versus 14.36 and 14.38 ns for
the pool. Creation is about 9.6 ns for both. Most remaining lifecycle time is
runtime bookkeeping, dispatch, and verification. These phase costs include
object-layout/cache effects; they are not isolated allocator-call timings.

Among the tested general-purpose policies, the existing mimalloc adapter
is fastest here, followed by tcmalloc, oneTBB, jemalloc, and native glibc.
Native glibc alone improves 1.59–1.78× over the existing C policy across the
four cases. The previous C-versus-mimalloc results therefore include a large
size/alignment-policy difference, as well as differences between allocators.

The 64-byte controls request the same aligned 64-byte block from each
allocator, including mimalloc:

| Allocator, 64-byte request | Same-thread autonomous | Same-thread wave | Separate-consumer autonomous | Separate-consumer wave |
| --- | ---: | ---: | ---: | ---: |
| glibc | 8.358 | 9.205 | 8.395 | 9.005 |
| jemalloc | 13.441 | 15.932 | 14.701 | 17.004 |
| gperftools tcmalloc | 15.754 | 19.025 | 17.608 | 20.901 |
| oneTBB | 14.022 | 16.603 | 15.537 | 17.946 |
| mimalloc | 17.563 | 21.919 | 19.713 | 24.130 |

Mimalloc remains ahead with equal size/alignment requests. The general-purpose
interfaces differ: its adapter uses sized/aligned free, whereas the preloaded
alternatives use ordinary `free`. These results compare practical caller
policies; they do not establish a universal ranking of allocator algorithms.

All **144 timed runs** pass the exact execution, generation, and reclamation
checks, including provider-identity verification. A 48-run release smoke
sweep covers all policies at a non-divisible 257-actor cohort. Another 24
LDC/DMD debug runs cover arena/native malloc with 1, 257, and 4,096 actors and
zero, one, or six consumers. Arena unit tests pass under both compilers,
checking alignment, overflow/exhaustion, non-overlapping live allocations,
the reset boundary, and preservation of separately allocated stable metadata.
