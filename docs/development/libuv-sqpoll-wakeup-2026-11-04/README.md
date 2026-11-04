# A lost SQPOLL wakeup in libuv 1.53.0 — 4 November 2026

With `UV_LOOP_USE_IO_URING_SQPOLL`, libuv 1.53.0 can submit a file
operation that never completes. The cause is a missing fence in its submit
path. This record has the reproduction, the patch, and the issue draft
that became [libuv#5408](https://github.com/libuv/libuv/issues/5408) and
[pull request #5409](https://github.com/libuv/libuv/pull/5409); the
outcome section at the end says where the fix lives for this project.

## How it was found

The `ant_uv` suite's io_uring lane, which runs every loop with
`UvLoopOptions.ioUring`, hung in 3 of 250 runs on the 5900XT (CachyOS,
Linux 7.3.1, libuv 1.53.0 as packaged), at a different case each time, and
in 2 of 250 runs of the tree before the UDP sockets were merged, so the
cause was older than either change. Every hung process looked the same in
`/proc/<pid>/task/*/wchan`: the main thread sleeping in a polling wait, the
loop thread in `do_epoll_wait`, libuv's auxiliary threads idle, and the
kernel's polling thread `iou-sqp-<pid>` asleep in `io_sq_thread`. One
request had been submitted to the ring and never completed.

## The cause

The kernel's SQPOLL thread polls the submission queue, and sleeps after
`sq_thread_idle` milliseconds without work (libuv sets 10). Before it
sleeps it sets `IORING_SQ_NEED_WAKEUP` in the ring's flags, issues a full
fence, and re-reads the queue's tail; an entry it finds then keeps it
awake (`io_uring/sqpoll.c`, `io_sq_thread`: `atomic_or`,
`smp_mb__after_atomic`, then `io_sqring_entries`). A submitter must do the
mirror image: store the tail, fence, then read the flags, and call
`io_uring_enter` with `IORING_ENTER_SQ_WAKEUP` if the flag is set. Without
the fence between the store and the load, the submitter can read the flags
before its tail store is visible, see no flag, and skip the wakeup, while
the thread re-reads the old tail and sleeps: the entry sits unread until
another submission happens to wake the thread. x86 permits exactly this
reordering of a store with a later load. liburing's `sq_ring_needs_enter`
has `io_uring_smp_mb()`, a `seq_cst` fence, between the two, with the
comment "Ensure the kernel can see the store to the SQ tail before we read
the flags".

libuv's `uv__iou_submit` (`src/unix/linux.c`, lines 864–878 at v1.53.0)
stores the tail with `memory_order_release` and loads the flags with
`memory_order_acquire`. Release and acquire order a store before later
stores and a load before later loads; they do not order a store before a
later load. The `v1.x` branch has the same code on 4 November 2026, and
v1.53.0 (2026-09-24) is the latest release.

## The patch

[libuv-1.53.0-sqpoll-wakeup-fence.patch](libuv-1.53.0-sqpoll-wakeup-fence.patch)
adds one `atomic_thread_fence(memory_order_seq_cst)` between the store and
the load, the fence liburing uses.

## Evidence

**The suite, alternating builds.** libuv 1.53.0 built from its release
archive twice, as released and with the patch, and the `ant_uv` suite's
io_uring lane run 500 times against each, the two builds alternating so
both saw the same load: 4 hangs as released (at a different case each
time: after the lifecycle, the full-ring, the timers and the ring-at-create
cases), 0 with the fence. Every hang had the thread picture above. The
scripts for that run are not part of this record; its method is the one
`run.sh` uses, with the suite in place of the program.

**The reproduction.** [sqpoll_lost_wakeup.c](sqpoll_lost_wakeup.c) is a
libuv program that submits one `stat` through the ring, waits for it, then
sleeps a random 9 to 12 ms so the next submission lands around the moment
the thread decides to sleep (10 to 11 ms after its last work, its idle time
being counted in 1 ms jiffies). When a `stat` is 2 s overdue it submits a
second one: if both then complete at once, the first had been in the ring
unread until the second woke the thread, which the program reports as the
lost wakeup and exits 1. The window is narrow, so [run.sh](run.sh) runs
eight copies at once against each build. On the 5900XT, two of the eight
copies against the released build caught it, at iterations 1,868 and 6,891
of 20,000, each with the first `stat` completing 0.017 ms after the second
was submitted; the eight copies against the fenced build ran their 20,000
iterations each without one ([results.txt](results.txt)).

## Issue draft for libuv

Title: **io_uring: uv__iou_submit can miss the SQPOLL wakeup; a request
then never completes**

> With `UV_LOOP_USE_IO_URING_SQPOLL`, a file operation submitted through
> the ring can stay in the submission queue forever: the kernel's SQPOLL
> thread has gone to sleep and libuv did not wake it.
>
> `uv__iou_submit` (src/unix/linux.c, v1.53.0 lines 864–878) stores the
> tail with `memory_order_release` and then loads the ring's flags with
> `memory_order_acquire` to decide whether to call `io_uring_enter` with
> `IORING_ENTER_SQ_WAKEUP`. Nothing orders the tail store before the flags
> load. The kernel's SQPOLL thread (`io_uring/sqpoll.c`, `io_sq_thread`)
> sets `IORING_SQ_NEED_WAKEUP`, issues `smp_mb__after_atomic()`, and
> re-reads the tail before it sleeps; the submitter must mirror that with a
> full fence between its tail store and its flags load, which is what
> liburing does in `sq_ring_needs_enter` (`io_uring_smp_mb()`, "Ensure the
> kernel can see the store to the SQ tail before we read the flags"). On
> x86 a load may be satisfied before an earlier store leaves the store
> buffer, so libuv can read "no wakeup needed" while the kernel thread
> re-reads the old tail and sleeps with the new entry unread. Nothing
> completes it until another submission happens to see the flag.
>
> Observed on Linux 7.3.1 (x86-64, Ryzen 9 5900XT) with libuv 1.53.0: a
> test suite that runs every file operation through the ring hung in about
> 1 of 100 runs, each time with the `iou-sqp` thread asleep, the loop
> thread in `epoll_wait`, and one request outstanding. Built from the
> release archive with the one-line fence below, the same suite hung in 0
> of 500 runs against 4 of 500 for the unpatched build, the two alternating
> under the same load. The attached program, eight copies of it for
> 20,000 iterations each, caught it twice on the released build (the
> overdue `stat` completed 17 µs after a second submission woke the
> thread) and never on the patched one.
>
> Patch (against v1.53.0):
>
> ```diff
> --- a/src/unix/linux.c
> +++ b/src/unix/linux.c
> @@ -868,6 +868,12 @@
>                          *iou->sqtail + 1,
>                          memory_order_release);
>  
> +  /* The kernel's SQPOLL thread sets NEED_WAKEUP, then re-reads the tail
> +   * before it sleeps. The tail store must be visible before the flags load,
> +   * which needs a full fence; release/acquire does not order store->load.
> +   */
> +  atomic_thread_fence(memory_order_seq_cst);
> +
>    flags = atomic_load_explicit((_Atomic uint32_t*) iou->sqflags,
>                                 memory_order_acquire);
>  
> ```
>
> The attached program reproduces it: it submits one `stat` at a time,
> spaced at random 9 to 12 ms so submissions land around the thread's idle
> transition, and when a `stat` is 2 s overdue submits another; if both then
> complete at once, the first had been in the ring unread. Run several
> copies for a few minutes.

Attach `sqpoll_lost_wakeup.c` and the patch.

## Outcome

Filed on 4 November 2026 as
[libuv#5408](https://github.com/libuv/libuv/issues/5408), with
[pull request #5409](https://github.com/libuv/libuv/pull/5409) from the
project's fork, branch
[`linux-sqpoll-wakeup-fence`](https://github.com/nriddick/libuv/tree/linux-sqpoll-wakeup-fence),
one commit on libuv's `v1.x`. Its message notes that an earlier pull
request with the same fence, #4921, was closed in 2025 unmerged, over
whether older kernels' barriers make the case unsalvageable. Since that
day the project's pinned libuv (`scripts/build-libuv.py`) is that fork
commit, `v1.x` at 1.53.1-dev plus the fence, and the patch here is no
longer applied by the script; it stays as the record's artifact. A release
archive takes the pin's place once a release carries the fence.

## Files

| File | What it is |
| --- | --- |
| [sqpoll_lost_wakeup.c](sqpoll_lost_wakeup.c) | the reproduction, a libuv program |
| [libuv-1.53.0-sqpoll-wakeup-fence.patch](libuv-1.53.0-sqpoll-wakeup-fence.patch) | the fence as a patch against 1.53.0, kept as the record's artifact; the pinned fork carries it, so the build script no longer applies it |
| [run.sh](run.sh) | builds libuv as released and with the patch, runs eight copies of the reproduction against each |
| [results.txt](results.txt) | that run on the 5900XT |
