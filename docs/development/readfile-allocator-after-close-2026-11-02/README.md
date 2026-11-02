# readFile's allocator after close — 2 November 2026

`UvLoop.close` can return while a `readFile` job is still running on one of
libuv's auxiliary threads, and that job can still call the request's
allocator. This record says when, reproduces both ways it happens, and
measures what a stop check before the allocation would change. It was made
on `9731be1` and changes no code; [run.sh](run.sh) reproduces it.

## When it happens

Only when `close` abandons a `readFile` that allocates its own block, one
submitted with a null `buffer`.

The whole read is one job on libuv's pool, `runReadJob` in
[loop.d](../../../ant_uv/source/ant_uv/loop.d) (lines 1316–1371 at
`9731be1`). Its steps run in this order:

1. `open`, then a check of the request's stop flag;
2. `fstat` and the directory test;
3. the allocation;
4. a check of the stop flag before each read;
5. `close`.

`close` works in three phases:

1. It keeps serving for `closeGrace`.
2. It sets the stop flag of every running job and calls `uv_cancel`
   (`cancelSlot`), which cancels only work that no thread has started.
3. After `abandonAfter` it publishes every job still running as
   `abandoned` (`abandonOutstanding`). libuv still holds those jobs'
   memory, so `teardown` leaves the loop allocated, and the loop thread
   exits.

Nothing waits for an abandoned job after that. If instead every request
settles, the allocator has finished before `close` returns: libuv runs a
job's completion callback only after the job returns, and `close` waits for
those callbacks. So the close report shows the case: `abandoned` is above
zero and `loopClose` is `UV_EBUSY`.

There are two ways an allocator call outlives `close`.

**A call that starts after `close` returns.** Nothing checks the stop flag
between `fstat` and the allocation. A job inside `fstat` when the flag is
set allocates the file's size once `fstat` succeeds, and stops only at the
check before its first read. For that, `fstat` must block for longer than
`abandonAfter`, 5 s by default.

On a local file system `fstat` reads the inode that `open` loaded and does
not wait for the disk. Network and FUSE file systems usually answer an
`fstat` just after `open` from their attribute cache, so a stalled server
stalls `open` instead. The case needs a file
system that sends every `fstat` to its server, such as NFS mounted with
`noac` or a FUSE file system that keeps no attributes (as the stub below
does), and a server that stalls between the `open` and the `fstat`.

An `fstat` that fails allocates nothing. A job stuck in `open` meets the
check after `open` instead.

**A call still running when `close` returns.** The job was already inside
the allocator when `close` abandoned it. This needs no slow file system:

- **An allocator that blocks.** The contract lets `allocate` block, and
  [loading files](../../guide/loading-files.md#where-the-bytes-live)
  suggests a budgeted pool. Suppose the pool waits for room that only the
  main thread's `step` frees, and the main thread is the one blocked in
  `close`. Then `close` waits the whole `closeGrace` plus `abandonAfter`,
  10 s by default, and abandons the job. The allocation completes after
  `close` returns, once shutdown frees memory. A lock that the allocator
  takes and the closing thread holds across `close` has the same effect.
- **A short `abandonAfter`.** Zero is allowed. `close` then returns within
  tens of microseconds, before the first allocations on libuv's new
  auxiliary threads have finished. In the runs below `close` took 22 to
  236 µs, and the allocations it left running, timed one run at a time,
  had taken 37 to 59 µs for 16 KiB files and 158 to 347 µs for 8 MiB, with
  glibc.

## Method

- **Host:** the Ryzen 9 5900XT under CachyOS: Linux 7.3.1, LDC 1.43.0,
  libuv 1.53.0, glibc 2.44 and libfuse 3.18.3. The local files were on
  tmpfs.
- **Tree:** `ant_uv` at `9731be1`. The runs were made before a rebase,
  when that commit was `a3ffacc`, which the outputs name; the rebase
  brought in only documentation.
- **The check under test:** a copy of `loop.d` with this line added after
  the directory test, as the job already does before each read:

  ```d
  if (stopped()) return closeFile(UV_ECANCELED);
  ```

- **[slowfs.c](slowfs.c):** a FUSE file system with three 64 KiB files. It
  never caches attributes, so every `stat` reaches it. `slow-open.bin`
  stalls 1.5 s in `open`. `slow-fstat.bin` stalls 1.5 s in the first
  `getattr` after an `open`, which is the job's `fstat`.
- **[probe.d](probe.d):** loads through `ant_uv` with an allocator that
  records when each call enters and returns, and counts `deallocate`
  calls. It was built twice, against the tree and against the copy with
  the check.
  - `stall`: one `readFile`, closed 300 ms after it was submitted, with
    `closeGrace` 50 ms and `abandonAfter` 100 ms. With `blocking`, the
    allocator waits until `close` has returned.
  - `zero`: 64 `readFile`s of one local file, closed 0 to 1000 µs after
    the last submit, with `closeGrace` and `abandonAfter` zero; 30
    processes per delay.

The stall cases scale the defaults down: a 1.5 s stall against an
`abandonAfter` of 100 ms stands for a stall of more than 5 s against the
default.

## Results

The stall cases, three runs of each on each build:

| Case | `9731be1` | With the check |
| --- | --- | --- |
| `fstat` stalls 1.5 s | allocator entered 1048.5–1048.7 ms after `close` returned | not called |
| `open` stalls 1.5 s | not called | not called |
| Allocator blocks until `close` returns | entered at the submit, 451 ms before `close` returned; returned 0.2–1.0 ms after | the same; returned 0.1–0.6 ms after |

In every stall run:

- `close` returned 450.6–452.1 ms after the submit: 300 ms, then
  `closeGrace`, then `abandonAfter`;
- the report counted one abandoned request, with `loopClose` −16
  (`UV_EBUSY`);
- the request read `abandoned`, with the result `UV_ECANCELED`;
- `deallocate` was never called.

In the `fstat` case the stub's log shows `getattr` returning 1.5 s after the
submit, and the allocator was entered then. [stall.txt](stall.txt) has
every run with the stub's log lines under it.

The zero-`abandonAfter` sweep, 30 processes per row. Each cell gives the
allocator calls still running when `close` returned, of all the calls made:

| File | Delay | `9731be1` | With the check |
| --- | --- | --- | --- |
| 16 KiB | 0 µs | 97 of 103 | 56 of 65 |
| 16 KiB | 20 µs | 4 of 215 | 0 of 190 |
| 16 KiB | 50, 100, 300, 1000 µs | none | none |
| 8 MiB | 0 µs | 98 of 98 | 73 of 73 |
| 8 MiB | 20 µs | 105 of 120 | 111 of 120 |
| 8 MiB | 50 µs | 109 of 120 | 108 of 120 |
| 8 MiB | 100 µs | 93 of 120 | 83 of 120 |
| 8 MiB | 300 µs | 8 of 120 | 7 of 120 |
| 8 MiB | 1000 µs | 0 of 120 | 0 of 120 |

- No call started after `close` returned in any of the 720 runs. Without a
  stall, only the second way occurred.
- `close` took 22 to 236 µs.
- [spans.txt](spans.txt) times the calls one run at a time, at delay 0.
  Those still running when `close` returned had taken 37 to 59 µs for
  16 KiB and 158 to 347 µs for 8 MiB, and returned up to 30 and 320 µs
  after it.

[zero.txt](zero.txt) has every row with the abandoned and freed counts.

## What it costs

- **The block leaks.** An abandoned loop never runs the job's completion
  callback, so the block is never handed over or freed: `deallocate` was
  never called in the stall runs. A budgeted pool loses that budget for
  good.
- **Tearing the allocator down after `close` becomes a use-after-free.**
  That covers resetting an arena, `mi_heap_destroy`, freeing a pool's
  context, and exit-time destructors. The late call runs on a thread that
  druntime does not know.
- **The C runtime and `mimallocV3ActorAllocator` only leak.** Both pass a
  null context to process-wide functions.

The contract does not say this today. The module's ownership rules in
`loop.d` say an abandoned request's block is never freed.
`UvCloseReport.abandoned` says libuv may still use the buffers, and the
README's [Close](../../../ant_uv/README.md#close) section says to leak them.
None of them mentions the allocator.

## What a fix covers

- **The stop check before the allocation** removes the first way: with it,
  no stall run called the allocator. It also stops a job that reaches it
  between the cancel and `close` returning, but it cannot reach a call
  that started before the cancel. With it, calls were still running when
  `close` returned at delay 0 for 16 KiB and at every delay up to 300 µs
  for 8 MiB.
- **An ownership rule is needed either way.** Once `close` reports
  anything abandoned, every `readFile` allocator and its context must stay
  valid for the rest of the process. It belongs in `loop.d`'s ownership
  rules and in the README's
  [allocation list](../../../ant_uv/README.md#where-readfile-puts-a-file).
- **What an application can do today:**
  - check `report.abandoned`, and leave the allocator alone when it is not
    zero;
  - keep `abandonAfter` well above the allocator's slowest call;
  - not let `allocate` wait on the thread that calls `close`. A bounded
    wait that returns null fails the read with `UV_ENOMEM` instead.
- **A related window, found by reading the code and not reproduced.** A
  job that a thread dequeues just as `close` cancels it still calls `open`
  on the request's `path` before its first check. After an abandoned
  publication that `path` is the caller's again. A check before `open`
  would close it.

## Not covered

- **Windows.** The job is the same code there, but these runs were Linux
  only.
- **The default 5 s settings.** They change only how long a stall must
  last.
- **A real network file system.** FUSE stood in for one; NFS, SMB and 9p
  were not tried.
- **Other allocators.** The probe wraps glibc's `posix_memalign`. The
  zero-`abandonAfter` timings depend on the allocator, and on whether the
  thread has allocated before.
- **io_uring.** `readFile` stays a pool job with `ioUring` set, so the
  steps are the same; it was not run.

## Files

| File | What it is |
| --- | --- |
| [run.sh](run.sh) | builds the stub and both probes, then runs everything below; 211 s on the 5900XT |
| [slowfs.c](slowfs.c) | the FUSE file system |
| [probe.d](probe.d) | the probe |
| [stall.txt](stall.txt) | the stall runs, with the stub's log under each |
| [zero.txt](zero.txt) | the zero-`abandonAfter` sweep, one line per 30 runs |
| [spans.txt](spans.txt) | the calls still running when `close` returned, from six runs at delay 0 |
