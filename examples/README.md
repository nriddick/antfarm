# Actor examples

Complete programs for the two actor models. Read these with
[the lifecycle guide](../docs/reference/actor-lifecycle.md), not the test sources.

| Program | What it shows |
| --- | --- |
| [public_service.d](public_service.d) | POD ledger, caller-owned inbox nodes, queued/coalesced acceptance versus rejection, a two-node drain, replies after `available`, node reuse, admission close, idle retirement, stale handles |
| [public_wave.d](public_wave.d) | Externally selected actors from two runtimes on one Farm, and the completion-hook tail after `finished` |
| [private_tree.d](private_tree.d) | Typed result ports, root `waitChildren`, a child continuation fold, a subset wave, non-POD state with a destructor, drain to `live == 0` |
| [request_deadline.d](request_deadline.d) | Tracked root per request, a per-request deadline, cancellation from another thread, and terminal `takeOutcome` statuses |
| [fail_fast_fold.d](fail_fast_fold.d) | `context.fail` + `cancelPeers` fail-fast, the wave aggregate after the join, and explicit root propagation |
| [fiber_cancel_at.d](fiber_cancel_at.d) | Example bridge from `FiberDomain.sleepUntil` to `PrivateRootHandle.requestCancel` (not library API) |

Every program runs on a [`FarmHost`](../threadpool/source/threadpool/host.d) (`threadpool.host`): one Farm, the caller's producer token, and pinned pool workers on one LLC, with the ordered teardown in `close()`. Each sets an explicit 30 s `waitTimeout` as example policy, so a stuck drain fails instead of hanging. `waitUntil` ends only on the program's predicate: it never treats `consumeNext() == false` or `pump() == 0` as idle.

`iota_sum.d` remains the threadpool payload example. It is not part of this actor lane.

## Build

From the repository root, with DMD or LDC. The same command works on Windows
x64 and Linux x86-64. Pass `0` to consume on the calling thread, or a worker
count N for N pinned pool workers in one LLC, with the calling thread confined
beside them. A machine with fewer than N accepted worker CPUs (one per
core by default: `skipSmtSiblings`) in one LLC rejects N with a
`FarmHostResolveException` that explains why. Exactly N accepted
CPUs is allowed. If no free CPU remains for the caller (including SMT siblings),
it shares worker CPUs and the host adds a note:

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=public_service
ANTFARM_HUGE_PAGES=0 ./public_service 0

ldc2 -g -i -I. -Iexamples -Ithreadpool/source examples/public_wave.d -of=public_wave
ANTFARM_HUGE_PAGES=0 ./public_wave 2

dmd -g -i -I. -Iexamples -Ithreadpool/source examples/private_tree.d -of=private_tree
ANTFARM_HUGE_PAGES=0 ./private_tree 0
```

The outcomes and cancellation programs use the same command:

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/request_deadline.d -of=request_deadline
ANTFARM_HUGE_PAGES=0 ./request_deadline 0

dmd -g -i -I. -Iexamples -Ithreadpool/source examples/fail_fast_fold.d -of=fail_fast_fold
ANTFARM_HUGE_PAGES=0 ./fail_fast_fold 0
```

The fiber `cancelAt` adapter links the separate fibers package, so add the
fibers import path too:

```text
ldc2 -g -i -I. -Iexamples -Ifibers/source -Ithreadpool/source \
    examples/fiber_cancel_at.d -of=fiber_cancel_at
ANTFARM_HUGE_PAGES=0 ./fiber_cancel_at 0
```

On Windows the linker writes `public_service.exe` (and the same for the other
programs). Ordinary pages are the default, so `ANTFARM_HUGE_PAGES` may be left
unset. In cmd.exe set it with `set ANTFARM_HUGE_PAGES=0` before the run.

`make -C examples check` is the Linux matrix: LDC and DMD, debug and release,
each run with 0 and 2 workers, plus an LDC ThreadSanitizer build of the same
threaded programs. Every program run has a 300 s timeout. `fiber_cancel_at` still runs under TSan with 0 workers only; Fiber migration
under TSan is covered by the dedicated Fiber TSan smoke suite instead (see
[fibers/DESIGN.md](../fibers/DESIGN.md#validation-boundary)). That target also extracts the complete First payload program
from [the quickstart guide](../docs/guide/quickstart.md#first-payload) and runs it with LDC and DMD in debug and release.
Other quickstart D blocks are fragments and are rejected by the extractor unless
they say so. The repository `make check` includes that target.

The same runner supports native Windows x64 without make or Bash:

```text
python -B examples/run_examples.py
python -B examples/run_examples.py --compiler dmd
python -B examples/run_examples.py --ldc-only
```

Windows runs debug/release with 0 and 2 workers, uses explicit `.exe` paths,
and omits TSan and the POSIX terminal front end. The complete native integration
matrix is documented in
[Testing](../docs/development/testing.md#windows-x64-integration).

## Conway's Game of Life

[life/backend.d](life/backend.d) demonstrates private-actor waves with one actor
per cell on a toroidal board: the eight neighbour positions wrap at both edges.
[life/headless.d](life/headless.d) checks exact blinkers, blocks, wrapped gliders,
a seeded board against a single-threaded reference, and cancellation/restart.
These tests run in the examples lane with 0 and 2 workers on LDC/DMD in debug
and release, and on LDC with TSan. The backend uses only D and stays portable.

The application owns two projection buffers. In generation g, all cell actors
read buffer g%2 and each writes only its own slot of buffer (g+1)%2. The root
calls `start!stepCell`, returns `waitChildren`, swaps buffers on resumption, and
starts the next wave. The ordering chain is callback writes → Shc/Tprogress
join → wave credits → root `waitChildren` resume → next start/publication →
next wave's reads. The [private ordering proof](../docs/design/private-actor-ordering.md#root-child-join-ordering)
explains those edges; no extra fences or runtime API are needed. The read buffer
must never be written during a wave, and both buffers must outlive the cohort.

`Life.step(n)` runs synchronously. `begin(n)` and `progress()` let the UI pump
asynchronously. Each request creates one W×H cohort and runs n waves; the
cohort is disposed when its tracked root finishes. The simulator reuses its
Farm, root park, and buffers across requests. `current` is a read-only view
available after drain; do not retain/read a view across another request, since
the double buffer is reused. UI edits and rendering occur only between drained
requests. The generation count and accumulated request timing expose gen/s and
cells/s (including cohort creation and collection, excluding UI idle time).
Clear/randomize reset the generation count and timing.

### Terminal app (POSIX only)

The [terminal front end](life/tui.d) links the vendored
[termbox2 v2.5.0](../third_party/termbox2/README.md) through a small C shim and
minimal D bindings. It requires a C compiler and a POSIX terminal; it is not
built on Windows. The examples lane builds it with both D compilers in debug
and release, but never runs it without a tty.

From the repository root:

```text
make -C examples life
ANTFARM_HUGE_PAGES=0 examples/.build/life 2

# Use DMD instead of LDC:
make -C examples life DC=dmd

# Portable headless tests (also usable on Windows):
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/life/headless.d -of=life_headless
ANTFARM_HUGE_PAGES=0 ./life_headless 0
ANTFARM_HUGE_PAGES=0 ./life_headless 2
```

The optional argument is the number of pinned pool workers (default 2), all in
one LLC with the UI thread confined beside them. Zero consumes on the UI thread.
`life_headless` defaults to 0, and its two simulators share one `FarmHost`. Live cells appear as `#`; the cursor marks the editing position. The
status shows generation, measured gen/s, cells/s, worker count, pause state and
target speed. Target speed is capped by actual simulation/rendering throughput.

| Key | Action |
| --- | --- |
| Space | Pause/resume |
| n | Pause and advance one generation |
| r | Randomize |
| c | Clear |
| g | Insert a glider with its top-left corner at the cursor (wraps) |
| Arrow keys | Move the cursor, wrapping at board edges |
| Enter | Toggle the cell at the cursor |
| + / - | Raise/lower target generations/s (1–240) |
| q / Ctrl-C | Cancel the current request, drain and quit |

The board uses the terminal dimensions at startup, with two rows reserved for
status/help. Resizing clips/reveals the existing board without changing its
simulation dimensions; restart to use a different board size. Edits received
while a wave runs are queued until drain. The UI requests one generation at a
time to keep input responsive; each request therefore pays cohort allocation
cost. Quit uses the tracked root's cooperative `requestCancel`; an incomplete
cancelled wave is discarded rather than promoted to the current projection.

Shutdown follows the [lifecycle recipe](../docs/reference/actor-lifecycle.md#shutdown-order):
`Life.close` cancels and drains (pump, run consumers, take outcomes); the shared
`FarmHost`'s `close` then joins the consumers, destroys the root parks,
unregisters the producer and destroys the Farm. The simulator keeps its buffers
alive throughout. The terminal is restored on normal exit and
D exceptions. Runtime `fatal` aborts and externally forced termination cannot
run D scope cleanup; if needed, recover the terminal with `stty sane` / `reset`.

Per-cell actors make runtime overhead dominate this workload. Tiling would be
the realistic design for throughput; this sample deliberately illustrates
per-cell actors and wave joins.

### Short headless timing sample

Local Linux x86-64 sample with LDC 1.43.0 (DMD 2.113.0 / LLVM 22.1.8),
`-O2 -release`, ordinary pages, seed 123456, 256×256 cells and 200 generations
per request. Nine fresh-process runs per worker count, interleaved 0/2/6 on an
unpinned, noisy host; focused checks were also compiling during measurement.
Timing includes cohort creation, the waves and terminal root collection, but
excludes Farm/worker startup and randomization. Standard deviation is the sample
standard deviation across those nine runs. These are observations, not a
performance gate or a comparison with a tiled implementation. They were taken
before the examples moved to `FarmHost`, with unpinned druntime workers, and
have not been re-measured with pinned workers in one LLC.

| Workers | Gen/s median | Gen/s min–max | Gen/s stddev | Cells/s median | Cells/s min–max | Cells/s stddev |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1,631.3 | 1,548.9–1,681.4 | 40.2 | 106.91 M | 101.51–110.19 M | 2.63 M |
| 2 | 2,707.4 | 2,588.0–2,841.3 | 75.2 | 177.43 M | 169.61–186.21 M | 4.93 M |
| 6 | 4,079.4 | 3,317.7–4,301.2 | 282.8 | 267.35 M | 217.43–281.88 M | 18.54 M |

Reproduce one run from the repository root:

```text
ldc2 -O2 -release -i -I. -Iexamples -Ithreadpool/source examples/life/headless.d -of=examples/.build/life_bench
ANTFARM_HUGE_PAGES=0 examples/.build/life_bench 2 --bench 256 256 200
```

The terminal app uses one generation per request, so its repeated cohort
allocation and rendering costs differ from these 200-generation batches.
