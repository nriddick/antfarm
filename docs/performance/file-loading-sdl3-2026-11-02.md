# File loading: ant_uv against SDL3's AsyncIO — 2 November 2026

Cached file loading through `ant_uv` and through SDL 3.4.18's AsyncIO, from
one thread that plays a game's main thread. The comparison led to three
changes on `2.2.0-progress`:

- `readFile` became one job on libuv's pool, where it had been four chained
  requests;
- the allocator runs in that job, under the actor allocator contract;
- `UvLoopOptions.ioUring` turns on libuv's io_uring for the other file
  operations.

The tables give the code after the changes; a later section compares ant_uv
before and after them.

## Setup

- **Host:** an i7-12700H laptop (6 performance and 8 efficiency cores, 20
  logical processors, one LLC), on AC power with the Balanced plan.
- **Windows:** Windows 11 build 26200, native.
- **Linux:** Ubuntu 24.04 under WSL2 on the same laptop, kernel 6.6.87.2.
  This is not native Linux, and its system calls cost more.
- **Toolchain:** LDC 1.42.0 `-O2 -release`.
- **libuv 1.53.0:** the pinned DLL on Windows, built statically from the
  same archive on Linux.
- **mimalloc:** 3.5.3 from the `third_party` submodule.
- **SDL 3.4.18:**
  - On Windows, the official development package. It chose its IoRing
    backend.
  - On Linux, built from the release source with liburing 2.15. It chose
    io_uring when `liburing-ffi.so.2` was on the library path, and its
    generic thread pool otherwise.

[`tools/perf/file_loading`](../../tools/perf/file_loading.d) runs one
implementation per process:

- Every file is read once first, so the page cache holds it.
- Up to 64 requests are outstanding at once.
- One thread submits and polls, spinning between polls.
- Nothing allocates from the GC after start-up, and every process reported 0
  collections.
- Every file buffer is checked to come from the heap the configuration
  names.

[`file_loading_matrix.py`](../../tools/perf/file_loading_matrix.py) runs five
processes per configuration, interleaved, each with three measured rounds
after a warm-up round. Throughput is the median of the 15 rounds; latency is
the median across processes of each process's quantile.

The first version of the benchmark allocated from the GC. In some of its
processes, all 19 of druntime's parallel mark threads then spun at full CPU
after a collection. That saturated the machine and produced millisecond
latency tails, which looked like ant_uv's. The benchmark has no GC
allocation since; the spin itself is not explained here.

Configurations:

- **ant_uv** uses its defaults: an unpinned loop thread and 4 pool threads.
  `readFile` allocates from the C runtime, or, with mimalloc, through
  `mimallocV3ActorAllocator()`.
- **SDL** loads whole files with `SDL_LoadFileAsync` and reads from an open
  file with `SDL_ReadAsyncIO`. Its buffers come from its own allocator, or
  from mimalloc through `SDL_SetMemoryFunctions`.

## Throughput

GiB/s; the range is the lowest and highest of the 15 rounds.

| Configuration | 2,000 files of 16 KiB | 32 files of 8 MiB | 4,096 random 64 KiB reads of one open 256 MiB file |
| --- | ---: | ---: | ---: |
| **Windows** | | | |
| ant_uv | 1.32 (1.13–1.62) | 7.98 (7.21–8.45) | 2.76 (2.44–3.21) |
| ant_uv + mimalloc | 1.26 (1.14–1.59) | 12.80 (10.24–14.27) | 2.77 (1.94–3.19) |
| SDL, IoRing | 0.49 (0.45–0.51) | 2.75 (2.60–2.85) | 4.32 (3.80–4.57) |
| SDL, IoRing + mimalloc | 0.61 (0.51–0.67) | 4.62 (2.90–5.07) | 4.37 (3.70–4.58) |
| **Linux (WSL2)** | | | |
| ant_uv | 3.00 (2.33–3.55) | 11.51 (10.57–14.04) | 5.41 (4.56–6.75) |
| ant_uv + mimalloc | 3.47 (1.94–3.81) | 17.12 (12.77–20.65) | 5.12 (4.30–7.89) |
| ant_uv, `ioUring` | — | — | 10.82 (9.02–11.94) |
| SDL, io_uring | 3.47 (3.05–3.73) | 2.63 (1.85–2.76) | 10.36 (9.35–11.01) |
| SDL, io_uring + mimalloc | 3.53 (2.64–3.91) | 8.90 (4.41–9.51) | 10.10 (8.22–11.06) |
| SDL, thread pool | 0.55 (0.51–0.57) | 9.62 (7.91–10.37) | 5.58 (5.10–5.98) |

## Time on the calling thread

Microseconds per request, as submit plus completion handling (which
includes freeing the file). A game's main thread pays this.

| Configuration | 16 KiB file | 8 MiB file | 64 KiB read |
| --- | ---: | ---: | ---: |
| **Windows** | | | |
| ant_uv | 0.71 + 0.60 | 0.12 + 349 | 1.21 + 0.21 |
| ant_uv + mimalloc | 0.79 + 0.59 | 0.13 + 1.3 | 1.15 + 0.23 |
| SDL, IoRing | 25.9 + 5.2 | 2,604 + 249 | 13.9 + 0.09 |
| SDL, IoRing + mimalloc | 21.5 + 3.6 | 1,688 + 2.0 | 13.8 + 0.08 |
| **Linux (WSL2)** | | | |
| ant_uv | 0.34 + 0.24 | 0.30 + 105 | 0.75 + 0.18 |
| ant_uv + mimalloc | 0.26 + 0.11 | 0.29 + 1.8 | 0.78 + 0.18 |
| ant_uv, `ioUring` | — | — | 0.29 + 0.09 |
| SDL, io_uring | 3.6 + 0.5 | 2,750 + 204 | 5.7 + 0.01 |
| SDL, io_uring + mimalloc | 3.5 + 0.5 | 876 + 1.3 | 5.9 + 0.01 |
| SDL, thread pool | 21.4 + 6.2 | 211 + 323 | 0.15 + 0.54 |

## One file at a time

One 16 KiB whole-file load, from submit until the bytes are usable, in
microseconds at p50 / p99. "Idle" sleeps a millisecond before each load,
so every thread involved has gone to sleep.

| Configuration | Windows | Windows, idle | Linux | Linux, idle |
| --- | --- | --- | --- | --- |
| ant_uv | 43.7 / 111 | 322 / 809 | 40.2 / 95 | 129 / 431 |
| ant_uv + mimalloc | 43.0 / 102 | 332 / 810 | 40.4 / 93 | 133 / 424 |
| SDL, kernel ring | 21.0 / 62 | 167 / 533 | 3.6 / 6.5 | 16 / 78 |
| SDL, kernel ring + mimalloc | 20.3 / 59 | 172 / 523 | 3.5 / 7.0 | 16 / 74 |
| SDL, thread pool | — | — | 61.5 / 191 | 164 / 339 |

## What it shows

- **SDL's kernel-ring backends do the work on the calling thread.**
  - `SDL_LoadFileAsync` opens the file, takes its size and allocates on
    the calling thread.
  - For a cached file, IoRing and io_uring then copy the bytes inside the
    submit call. Those processes used exactly one core's worth of CPU.
  - So an 8 MiB load costs the main thread 0.9 to 2.8 ms, a 16 KiB load
    3.5 to 26 µs, and a 64 KiB read 6 to 14 µs.
  - The same placement makes their single-file round trip the fastest
    measured, and their reads from an open file fast.
- **ant_uv's submit is a ring push:** 0.1 to 1.2 µs whatever the size. The
  work runs on the loop and pool threads, which are placed off the workers.
- **The whole-file job closed most of the latency gap on Linux.**
  - Batched 16 KiB loads now match SDL's io_uring there.
  - Large files load fastest through ant_uv's pool, because four threads
    copy in parallel where the ring's one thread copies in turn.
- **Single-file latency still favours SDL's rings.**
  - On Windows, ant_uv's job still adds a wake of a pool thread and a wake
    of the loop thread to the system calls SDL makes inline: 44 against
    21 µs.
  - Under WSL the system calls themselves are cheap and the wakes are not:
    40 against 3.6 µs.
- **The allocator matters most for large files, on both sides.**
  - The system allocators return a large block to the OS when it is freed
    and fault its pages in again on the next allocation.
  - An 8 MiB free cost the freeing thread 100 to 350 µs, against 1 to 2 µs
    with mimalloc.
  - Through mimalloc, 8 MiB loads gained 49% (ant_uv, Linux) to 3.4 times
    (SDL, Linux).
- **`SDL_SetMemoryFunctions` with mimalloc was stable.** That was 20
  processes across both sessions and platforms, with every buffer verified
  to be mimalloc's. It must be called before SDL allocates anything, and it
  is process-wide.
- **libuv's io_uring doubled reads from an open file:** 5.4 to 10.8 GiB/s,
  level with SDL's io_uring at a fifteenth of SDL's cost on the calling
  thread (0.38 against 5.7 µs). It needs both the loop option and `UV_USE_IO_URING=1`; the
  variable alone did nothing in the first session.

## ant_uv before and after

The same benchmark on the commit before the changes, in an earlier session
the same day. SDL ran interleaved in both sessions, so it shows the
session-to-session drift. ant_uv's 64 KiB reads went through an unchanged
path and still moved 12% on Windows, which bounds the noise.

| ant_uv | Before (four requests) | After (one job) |
| --- | --- | --- |
| Windows, 16 KiB files | 1.37 GiB/s | 1.32 GiB/s |
| Windows, 8 MiB files | 7.21 GiB/s | 7.98 GiB/s |
| Windows, one 16 KiB file, p50 / p99 | 66.3 / 129 µs | 43.7 / 111 µs |
| Windows, the same after idle | 532 / 1,133 µs | 322 / 809 µs |
| Linux, 16 KiB files | 0.84 GiB/s | 3.00 GiB/s |
| Linux, 16 KiB files, mimalloc | 0.85 GiB/s | 3.47 GiB/s |
| Linux, 8 MiB files | 6.73 GiB/s | 11.51 GiB/s |
| Linux, one 16 KiB file, p50 / p99 | 132.8 / 265 µs | 40.2 / 95 µs |
| Linux, the same after idle | 242 / 642 µs | 129 / 431 µs |

Between the sessions, SDL's IoRing round trip stayed at 21 µs, and its idle
round trip moved from 210 to 167 µs.

An intermediate build also routed `readFile` through the ring when
`ioUring` was on, as four operations. Measured under WSL, it was slower than
the job everywhere:

| `readFile` through the ring | System malloc | mimalloc |
| --- | --- | --- |
| 16 KiB files | 2.75 GiB/s | 2.83 GiB/s |
| 8 MiB files | 2.33 GiB/s | 8.25 GiB/s |
| One 16 KiB file, p50 | 69.9 µs | 71.0 µs |

The kernel's one polling thread copied every file in turn, and each step
waited on the loop. So `readFile` stays a job with `ioUring` too.

## Not measured

- Cold files. With the page cache dropped, the kernel rings and the thread
  pools should differ most, and dropping it needs administrator or root.
- Native Linux, and the Ryzen 9 5900XT that the other records use.
- A loaded worker pool beside the loading.
- Pinned placements. The laptop's hybrid cores and power plan make every
  figure here a laptop figure.

## Reproduce

Build the pinned libuv (`scripts/build-libuv-windows.py` on Windows), SDL 3
and mimalloc (`make -C tests/actors .build/mimalloc-v3.5.3/libmimalloc.a`).
Then, from `tools/perf`:

```sh
make file_loading SDL3_LIBS="-L-L/path/to/sdl3/lib -L-lSDL3"
./file_loading --setup /tmp/files
python3 -B file_loading_matrix.py ./file_loading /tmp/files out.json 5 \
  "sdl3-crt=--impl sdl --alloc crt" "sdl3-mi=--impl sdl --alloc mi" \
  "ant_uv-crt=--impl uv --alloc crt" "ant_uv-mi=--impl uv --alloc mi" \
  "ant_uv-io_uring=--impl uv --alloc crt --io-uring"
```

On Linux, SDL takes io_uring only when `liburing-ffi.so.2` can be loaded:
add `ENV:LD_LIBRARY_PATH=...` to a configuration to choose. On Windows, link
`SDL3.lib`, `uv.lib` and mimalloc's static library with `advapi32.lib`,
`psapi.lib` and `bcrypt.lib`, and put `SDL3.dll` and `uv.dll` beside the
program.
