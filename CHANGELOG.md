# Changelog

Older release notes are in [docs/releases/](docs/releases/):
[1.8.0 and its release candidates](docs/releases/1.8.md) and
[1.7 and earlier](docs/releases/1.7-and-earlier.md).

## 2.0.2 development — C dispatcher and coroutine investigation

- Add `antfarm_forkjoin.h`: dispatcher configuration/lifecycle/counters,
  eligible CPU enumeration, caller-owned C nodes with C-linkage run/join
  adapters, fork/complete, nested synchronous roots, foreign submission,
  reusable completions and detached roots. Invalid callback completion/fork
  contracts have release-build fatal diagnostics.
- Add callback-based for/reduce with the D range tree's deterministic midpoint
  split, generic POD results, checked sizes and explicit empty-range behavior.
  Add `cabi/examples/forkjoin.c` and native C tests for recursive/nested trees,
  overflow, re-forking joins, backpressure streams, completion reuse, detached
  drain and invalid admission/teardown.
- Add `antfarm_groups.h`: worker resolution/start, raw C worker hooks, group
  Farms, cached per-thread tokens and wake. `GroupHost.stopWorkers` keeps
  borrowed resources alive between joining workers and final close. C close
  refuses outstanding views/tokens/actors/waves/private parks. Foreign token
  holders cannot detach until their cache is released.
- Keep all previous ABI 1 lock rows and the BetterC core unchanged. New
  services participate in explicit runtime-session lifetime accounting. DMD
  materializes Farm GC metadata in the runtime archive without a second core.
  The contract is [C dispatcher and GroupHost](docs/design/c-dispatcher.md).
- Leave production Fibers unchanged and outside the ABI. Add an isolated
  [warm reuse experiment](docs/performance/coroutines-2026-10-25/README.md)
  and [replacement proposal](docs/design/coroutine-replacement.md). At 32
  threads on the 5900XT, zero-yield raw deque reuse measured 141.77 M/s
  (141.16–152.93), shared-Farm dispatch 35.80 M/s (34.90–37.28), and existing
  FiberDomain lifecycle 1.85 M/s (1.81–1.86), five samples per cell. With
  three yields, raw deque measured 23.18 M completed lifetimes/s versus
  ring 8.25 M and existing lifecycle 1.17 M. These are deliberately different
  feature sets, not a production speedup claim. All per-thread-count results,
  allocations, migrations, compiler/host metadata and raw samples are retained.

Validation: `make check SHELL=/usr/bin/bash` passed on Linux x86-64
(7.2.9-2-cachyos), Ryzen 9 5900XT, LDC 1.43.0, DMD 2.113.0 and GCC 16.2.1:
C/C++ ABI/death tests, core/pool/Fiber/fork-join suites, the gate's TSan and
ASan lanes, actor/private-tree/order tests, documentation and examples.
All 324 original ABI-lock rows remain identical, with 84 additions. The new
GroupHost cleanup test waits for user-body readiness; an immediate-stop test
separately covers workers stopped before first entry. Thirty repetitions
passed with each compiler. The 300 benchmark samples checked completion
counts/checksums and retained the warmed Fiber pool. No native Windows run,
standard `perf-gate`, async-I/O benchmark or custom context-engine performance
is claimed. No push or release is made by this development increment.

## Unreleased

Fixes:

- Public actors: a retiring entry or exit read the inbox before the
  submission gate, so a sender reserved before the close could push, signal
  and release in between. With `PENDING` already set (any retirement drive,
  or a wake during the activation) that signal rewrote the same lifecycle
  value, the retirement CAS succeeded, and the actor reached `RETIRED` with
  an accepted node queued: the reply never completed and `reclaim` was
  fatal. Both loops now acquire the gate first; a closed gate with no
  reservation joins every accepted push, so the inbox read after it is
  final. The torture suite parks a retirement between the two reads in
  entry, exit and self-retirement schedules.
- Fibers: a signal detached its waiter chain and left each node's
  membership valid. A cancellation that lost to the signal could then unlink
  a detached waiter and decrement a bucket recreated for the same signal by a
  later waiter, stranding that waiter where neither a signal nor its own
  cancellation could reach it. `WaitSet.take` now clears every detached
  waiter's membership under the stripe lock.
- Fibers: timer expiry popped the heap entry, dropped the heap mutex, and
  only then claimed the task's current wait. A waker that won meanwhile could
  resume the task, which could park on its next wait (or be recycled) before
  the expiry claimed that one: an hour-long sleep after a timed join could
  return at once. Expiry now claims the wait under the heap mutex; every
  other waker already removes the entry before publishing.
  tests/ant_fibers/wait_races.d pauses in each window through unittest seams.
- Threadpool: `shutdown()` on a pool that was not the live one (rejected by
  `start`, never started, already shut down) stopped the live pool's workers
  and cleared their process-wide records and hooks without joining them, so
  their stop hooks were skipped and the live pool's own `shutdown` returned
  early. It now returns unless the receiver is the live pool.
- `GroupHost`: a second host, rejected for the pool claim, ran its failure
  cleanup anyway, releasing the open host's caller tokens and clearing its
  Farm list; the open host's `close` then met a live worker ticket and was
  fatal. The rejection now happens before the cleanup guard is installed,
  and a second host without workers (which takes no pool claim) is rejected
  too. tests/threadpool/host_live.d covers both with tokens registered.
- Windows topology: NUMA identity came from the CPU-set `NumaNodeIndex`,
  which is relative to the processor group, so on a host with more than one
  group two sockets that both report index 0 merged into one node and a
  worker's NUMA-local neighbourhood (and `search!T`) took in the other
  socket's LLCs. The parser now decodes `RelationNumaNode` records (multi-
  and single-mask layouts) and uses their `NodeNumber`; a processor no record
  covers keeps its local index, qualified by group when there are several.
  tests/threadpool/topology_parse.d adds a simulated two-socket host of 256
  processors in four groups, where every group-relative index aliases, under
  both record layouts and with no records.
- forkjoin: with `farmPerGroup` and the caller alone in its LLC group (any
  CPU mask that isolates the first eligible CPU, including the no-argument
  `start()` under `taskset`), a deque overflow on the caller spilled into its
  group Farm, which no participant consumes, and `runRoot` never returned.
  A thread whose home Farm nobody else consumes now runs its overflow
  inline (completion_test case 5b).
- forkjoin: the spill and root-submission wakes publish with a release store
  and then read the armed bitmap, which x86 may reorder against the
  sleeper's arm-and-recheck; both sides could miss and a foreign `submitRoot`
  (or a non-consuming caller's spill) could wait on parked workers forever.
  Both now fence before the read; the push wake, which only costs
  parallelism when it misses, does not.
- forkjoin.frames: a thread that adopted a parked slot skipped the slot's
  free list, so its first allocation of a class carved a new chunk while the
  list was full, or, with remote frees pending, replaced the list with them
  and lost every block on it. Adoption now allocates from the list first;
  a module unittest builds both slot states and requires no new chunk.
- `Dispatcher.start(opt)` forced `farmPerGroup`, so `start(oneFarm())` got
  per-group Farms; it now honours `opt`, and `start()` is `perGroup()`.
  `start()` takes at most `maxParticipants` processors instead of throwing
  on larger machines. `Dispatcher.stop()` returns at once without a
  successful start (it crashed before `start`, after a failed one, and on a
  second call). `start` rejects `bodySteps == 0` (workers never stepped)
  and an unknown processor (a null dereference before), sizes its group
  table by LLC count rather than `maxParticipants`, and matches SMT
  siblings on `(group, package, core)`. `fork` off a participant is a
  diagnostic in builds with assertions. `TaskGroup` cannot be copied.
- `counters()` reported zeros unless `discardCounters()` had been called,
  because counts from the initial epoch were dropped; they now accumulate
  from process start until the first `discardCounters()`.
- forkjoin builds for Windows x64: the deque and frame slots use the core's
  64-byte aligned allocator, chunks come from `VirtualAlloc` (whose 64 KiB
  granularity is the chunk alignment), and the admission test reads CPU
  time through `GetProcessTimes`. `check-windows.py` has a `forkjoin` lane.
- Docs: a detached root's `join` runs after its children like any node's,
  never for a root that forks nothing (the docs called it the completion
  callback); `runRoot` waits on its own sentinel, not on `g_rootsLive`.

- `forkjoin.frames`: a frame pool for task frames (`allocFrame`,
  `allocFrames!T`, `freeFrame`, `releaseFrames`, `frameStats`). Headerless
  64 KiB chunks whose first line names the owning slot and block size,
  16-byte classes up to 1 KiB, thread-local LIFO lists and bump cursors, one
  CAS per cross-thread free onto the owner's remote list, slots parked and
  adopted across participant threads. fib(39) at 32 threads runs in 102 ms
  under tcmalloc, glibc and mimalloc alike (previously 135, 117 and 106 with
  `malloc` frames); nqueens 125.7 ms against 146 with peak memory 16 MB
  against 39; skynet 3% faster; matmul unchanged. The fib example uses it.
  tests/forkjoin/frames_test.d covers classes, patterns, reuse without new
  chunks, large requests, cross-thread drains, restarts and parked slots.
- 2.0.1 features. `forkJoinMap!fn(source, output, grain)` writes a transform
  into an output slice over the loop tree. `forkJoinScan!(fn, combine)(source,
  output, seed, grain)` is an inclusive prefix fold in two passes over one
  retained tree, `fn` once per element, bit-identical across participant
  counts, returning the last output. `forkJoinInvoke(fns...)` runs a fixed
  set of `nothrow @nogc` callables as the children of one node with the
  frames on the caller's stack. `TaskGroup` (`add`, `wait`) collects a batch
  of roots from a thread that is not a participant, reusable after `wait`.
  `runRootAny` runs a root from a participant or any other thread.
  `DispatcherOptions.oneFarm()` and `.perGroup()` name the two documented
  shapes; `printCounters` prints a `Stats`. The loop frames now come from the
  frame pool. tests/forkjoin/features_test.d covers all of it.
- The gate's `dub test :forkjoin` lanes pass `--DRT-testmode=run-main`:
  druntime skips `main` after module unittests, so the suite driver had run
  only in the sanitizer lanes.

- PERFORMANCE.md's fork-join table is re-taken under the benchmark harness's
  own build rules: the harness's allocator (tcmalloc here, not Ant Farm's
  bundled mimalloc), no function-alignment flag, library-default dispatcher
  options, with the ports staged for the upstream suite. fib(39) now trails
  citor at 16 threads and above because its per-fork frame allocation is
  allocator-bound; skynet and nqueens still lead at every count.

## 2.0.0 — 2026-10-24

2.0.0 adds a work-stealing fork-join dispatcher beside the Farm, puts one
owner under the pool, removes the blocking parallel adapters in its favour,
and moves the whole project to the MIT license. On the tzcnt/runtime-benchmarks
programs the dispatcher ties or leads citor, libfork, TooManyCooks and TBB on
fib and skynet at every thread count and is within 5% on nqueens and matmul
(the table at the top of PERFORMANCE.md). C ABI 1 is unchanged.

Highlights:

- `forkjoin`: per-participant bounded Chase-Lev deques, a heap-frame
  continuation-passing `Node` with `fork`/`complete`, the Farm as the
  overflow and admission channel, parking on the pool's armed bitmap,
  `Dispatcher.start()` on every eligible processor or on a chosen set,
  `runRoot`, `submitRoot`/`waitRoot`, `submitDetached`, and the loops
  `forkJoinFor`, `forkJoinEach` and `forkJoinReduce` (deterministic split).
- `threadpool.groups.GroupHost`: one owner of the pool, caller placement,
  one Farm per LLC group or one shared Farm, and per-thread producer tokens
  for raw worker bodies; the dispatcher runs on it.
- The pool's `wakeOne`/`anyWorkerArmed`; `import threadpool;` re-exports
  the host and groups modules.
- Onboarding: the README opens with the three rules and a ten-line fork-join
  first program; the hosting guide is split into running a host and scaling
  across groups; a docs inventory check covers every public declaration.

Breaking changes:

- License: BSL-1.0 to MIT for the whole project; the fork-join design is
  derived from citor (Vitalii Shutov, MIT), acknowledged in `LICENSE`.
- Removed: `antfarm.parallel`'s `parallelForWait`, `parallel` and
  `ParallelLoop`; `FarmHost.parallelFor` (`parallelOptions` and `step()`
  stay for asynchronous regions); `threadpool.exchange`.
- The pool's spin and park counters compile only with
  `version (ThreadpoolStats)`; the Director's `cadence`, `nudge` and
  `drift` and the `Cadence` options only with `version (ThreadpoolCadence)`.
- `HostGroups.span` is no longer a placement default.

The entries below record the work in the order it landed, newest first.

- PERFORMANCE.md opens with the fork-join dispatcher against citor, libfork,
  TooManyCooks and TBB on the tzcnt/runtime-benchmarks programs at 1, 2, 8,
  16, 24 and 32 threads; ROADMAP gains the 2.0.1 (wrappers and small
  features) and 2.0.2 (the C ABI grows to the dispatcher) sections.

- Fork-join review fixes, from an independent read of the dispatcher.
  `complete` reads the grandparent before running a parent's join, since the
  join may free the parent or hand it to a waiter (a use after free under
  ThreadSanitizer and AddressSanitizer). A loop started inside a Farm
  callback no longer polls the view that is running it (SPEC 7c) and runs
  spilled children inline, so a child in the Farm cannot be left for a
  consumer that is itself waiting. `DispatcherOptions.callerConsumes` now
  defaults to false: a subscribed view that never polls pins reclamation,
  and a sustained stream of foreign roots stalled after about 24,000 with
  an idle consuming caller; a refused root write wakes a worker of the
  submit Farm's group. A caller whose LLC group has no worker gets its own
  group instead of failing with "participant without a group Farm".
  `wakeOneArmedIn` compared its CAS against a word it had already modified
  and could skip a parked worker. A failed `start` rolls back, so a valid
  start may follow it. `forkJoinReduce` accepts accumulators narrower than
  a word. New: tests/forkjoin/completion_test.d (concurrent foreign
  submitters with nested loops in callbacks, spilled children starting
  nested reductions, parked workers with one Farm per group, forced
  overflow with one worker, a 30,000-root stream through a 2 MiB ring, a
  caller alone in its group, a failed start then a valid one, all under a
  watchdog), accumulator-type reductions in range_test.d, and the fork-join
  suite runs under ThreadSanitizer and AddressSanitizer in `make check`
  (tests/forkjoin/tsan.supp suppresses druntime's suspend-handler report).

- Cleanup step 6, onboarding. The README opens with the three rules and the
  representation table with a one-sentence default above it; the D first
  program is ten lines on the fork-join dispatcher (`Dispatcher.start()`
  with no arguments takes every eligible processor), with the raw one-thread
  ring program as the quickstart's second step; `import threadpool;`
  re-exports `threadpool.host` and `threadpool.groups`; the README shows
  only `dub run --single` and leaves compiler lines to examples/README.md;
  hosting.md is split into running a host and
  [scaling across groups](docs/guide/scaling-groups.md); the docs map's
  seventh step is running the example for your shape, and the gate stays in
  CONTRIBUTING. `run_examples.py` extracts every complete quickstart
  program.

- Cleanup step 1, one owner of the pool: `threadpool.groups.GroupHost`
  resolves workers with `FarmHost`'s planner, creates one Farm per selected
  LLC group (`farmPerGroup`, `HostGroups.all`) or one shared Farm, pins the
  caller (`callerProcessor`) or confines it, starts the pool with a raw
  worker body, installs a `GroupSlot` registry (`home!GroupSlot`,
  `farmOf`), hands out one token per thread per Farm (`token`,
  `groupToken`, `releaseThreadTokens`) and tears down in order. The
  fork-join dispatcher runs on it and lost its own Farm creation, pinning
  and teardown. `FarmHost` keeps its one-Farm implementation and shares the
  planner and the pool claim; routing its runtimes and parks by group is the
  roadmap's next item. host-test gains a GroupHost case (two groups, one
  Farm each, tokens per thread, teardown, pool released for a FarmHost).

- Cleanup step 2, idle machinery. The pool keeps one bit per armed worker
  and offers `wakeOne` (clear a bit, wake that worker; a woken worker that
  finds more work wakes the next) and `anyWorkerArmed`; the fork-join
  dispatcher uses them instead of its own bitmap. The spin and park counters
  behind `PoolStats` are compiled only with `version (ThreadpoolStats)`: a
  process-wide atomic add per pause had measured 7.8% on consumer-bound
  workloads under `WorkerIdle.spin`; the re-measurement in hosting.md found
  the host 4% ahead of the hand-written loop on this tree before the change
  and 1.5% more after it. The Director's `cadence`, `nudge` and `drift`
  verbs and the `Cadence` options, which have no in-tree user, are compiled
  only with `version (ThreadpoolCadence)` together with their tests.

- Cleanup step 3: blocking loops run on the fork-join dispatcher.
  `forkjoin.range` adds `forkJoinFor`, `forkJoinEach` (a borrowing
  delegate) and `forkJoinReduce`, a reduction whose split points depend only on
  length and grain and is therefore bit-identical across participant
  counts. `antfarm.parallel` loses `parallelForWait`, `parallel` and
  `ParallelLoop`; `FarmHost.parallelFor` is removed (`parallelOptions` stays
  for asynchronous regions driven by `step()`). The dispatcher gains
  `submitDetached`, `isParticipant`, per-root completion in `runRoot` (a
  nested loop waits only for its own tree) and a draining `stop`. The
  parked-worker regression in host-test drives an asynchronous handle with
  `step()`; the parallel death probes for nested blocking waits and
  `break` are gone with the adapters.

- Cleanup for 2.0, step 5 and 7 of [the plan](docs/development/cleanup-plan-2.0.md):
  `threadpool.exchange` (a self-described stub) is removed; `HostGroups.span`
  moved out of the placement defaults into a "when you know better" note;
  the MT-payload removal history moved from SPEC section 11 to the
  performance archive, the lineage appendix stays; `VERSION` holds the
  version and README, ARCHITECTURE and ROADMAP no longer carry it in prose;
  the roadmap's 1.8.2 and 1.9.0-progress sections closed into this file;
  the measured cliff figure left using-it-well for PERFORMANCE; the quoted
  check-quick duration left CONTRIBUTING.

- New subpackage `forkjoin`: a fork-join dispatcher on the threadpool with
  one bounded Chase-Lev deque per participant, the Farm as admission and
  overflow channel, a continuation-passing join that never blocks, idle
  parking through the Director, and root submission from foreign threads.
  `dub test :forkjoin` runs the deque exactly-once test and the admission
  test; both are in `make check` and `make check-quick`. Design and
  measurements: [docs/design/forkjoin.md](docs/design/forkjoin.md); the
  choosing guide, README and ARCHITECTURE name the new representation, and
  the roadmap's 2.0 work-stealing item records what is done and open.

- Ant Farm is now MIT licensed as a whole (`LICENSE`), replacing the Boost
  Software License 1.0. The license file carries the acknowledgement of
  citor by Vitalii Shutov, from whose design the `forkjoin` subpackage
  derives; `dub.sdl` of every package and the C header say MIT.

## 1.9.1 — 2026-10-23

Measurement records carried over from the roadmap's "1.9.0 progress" section
when it closed in 2.0:

- The matched MT-excision checks ([mt-excision-check-2026-10-20](docs/performance/mt-excision-check-2026-10-20.md))
  kept the October 20 baseline: the apparent fourfold headline loss did not
  reproduce with the saved executable, and matched 1.8.3/current core probes
  retained fast explicit-length uniform and mixed-callback paths on Windows
  and WSL2. The [inferred-writer check](docs/performance/writer-scan-2026-10-20.md)
  found a separate sizing-scan regression and measured its restoration in
  `651030b`. The environmental cause of the earlier low rates is unresolved.
- The October 19 quieter-host retest measured -14.1% paired-median for
  `private` at zero consumers against 1.8.2 (`2182eb8`), LDC 1.42 `-O2
  -release`, i7-12700H under WSL2; the other two serial private cases were
  inconclusive, six-consumer samples hit persistent CPU-accounting
  exclusions, and the 392-case grain grid verified output throughout without
  establishing a general performance win.
- The C wave bridge's typed-D `wave` path at `d94acc1` against `be025b1`
  (LDC 1.42 `-O2 -release`, mimalloc 3.5.3, 16,384 actors, distinct-core
  pins, Linux/WSL): caller-only execution passed the -3% boundary with 20
  clean pairs (paired-median -0.63%, interval [-2.70%, +0.80%]), a
  byte-identical control also passed, six-consumer execution stayed
  inconclusive (12 clean pairs of 40), so the combined gate did not pass.
  Raw evidence was archived locally in `.build/cabi-waves-performance-evidence.zip`.

- README now provides explicit C/C++ and D entry paths and separates building
  the library from using the runtime-free native core. Added `cpp_threads.cpp`,
  a C++11 example using application-owned threads, exclusive consumer views,
  partial-write retries, explicit completion and joined teardown. The native
  ABI and example runners build and execute it with both supported D compilers.

- Removed the invalid residual Fiber drain throughput column from PERFORMANCE
  and corrected Fiber counts to workers plus a separate controller. Full warm
  burst measurements are unchanged. C ABI documentation now states ABI 1 shipped
  in 1.9.0. Both performance drivers terminate remaining POSIX process-group
  descendants even after the leader exits. Report resume now requires an
  immutable source/binary/configuration manifest, retains original metadata,
  and uses hashed Fiber executable snapshots. Focused tests cover TERM-resistant
  descendants and incompatible resume; no new performance measurements are added.

- Corrected the current release in ARCHITECTURE and ROADMAP to 1.9.0.
  Major performance comparisons use the 5900XT; removed the planned 12700H
  table refresh because the laptop is not expected to sustain stable clocks
  over long measurements. Existing measurements and historical records remain.

- The hosting guide gains "Several physical groups": why crossing a CCD
  costs what it does (shared control lines, then data lines), measured
  one-way and split consumption at 16 to 1,700 entries per table and 0 to
  1 µs of work per payload, and the rules that follow: consumers of one Farm
  in one group, cross-group publication only in tables of 256 entries or
  more carrying about 100 ns of work per payload, a Farm per group as a
  one-way channel with parks and runtimes per Farm, tokens held per
  publishing thread (quota is budgeted at creation), and channel Farms sized
  for their traffic. The usage rules, architecture page and roadmap (a
  per-group-Farm host) point at it. The usage rules page carries a
  "Several LLC groups" stub pointing at it.

- Threadpool idle timing no longer wraps past about 9.2 s. Tick and
  nanosecond conversions multiplied before dividing, so with POSIX's 1e9
  ticks per second a park deadline more than about 9.2 s away became a
  zero-length park (`Thread.yield()` in a loop) for `sleepUntil`, cadence and
  managed deadlines; a cadence period or spin lead of about 9.2 s or more
  failed as "shorter than one tick"; and `sleep` under a managed deadline
  wrapped past about 18.4 s, reachable from C through
  `af_host_options.idle_timeout_ns`. Windows' 1e7 QPC rate moved the limit to
  about 15 minutes. Conversions now scale whole seconds and the remainder
  separately and saturate; results below the old limits are unchanged. Unit
  tests cover 10 s, 30 s and one hour at both rates.

- `CacheAwarePool.start` stops and joins already spawned workers when any
  `Throwable` escapes setup, not only an `Exception`. An `Error` such as an
  out-of-memory failure previously left the pool started with live workers,
  and process exit hung. A failure while stopping those workers is chained
  onto the original instead of replacing it. A unit test injects an `Error`
  after spawn and disarms the injection on every exit path.

- `check-c-abi.py` audits declarations, not only exported names. Each
  compiler's `-HC` output for the C ABI modules is checked against every
  header prototype with `static_assert`, and every header struct's size,
  alignment and field offsets are compared with the D definitions: 113
  prototypes, 25 structs and 115 fields. Ten prototypes take a handle that
  `-HC` prints as a bare pointer; those parameters are checked only as
  pointers of the same constness. Before, a prototype mismatch that still
  linked passed, and the actor and private headers had no C-side layout
  asserts. The 1.9.0 headers pass unchanged; ABI 1 is unaffected.

- The ABI 1 freeze is a test. `cabi/abi1.lock` records every header
  prototype, the 71 numeric enum and macro constants, and the audited struct
  layouts. The declaration audit fails on any difference and reports each
  changed, removed or added row, so a header and D definition changed
  together no longer pass. `check-c-abi.py --write-abi-lock` regenerates the
  lock; within ABI 1 only additions are compatible. The audit and the lock
  check run in every C ABI lane except `--tsan`.

- The D `write` overloads take `const(TableCompletionHook)*`, so the C ABI
  passes `const af_completion_hook *` through without casting const away.
  Mutable pointers still convert. The hook's const covers the record only:
  `context` still reaches `call` as a mutable `void*`, as documented on
  `TableCompletionHook`.

- Documented that C++ wrapper operations other than `get()`, moves and
  destruction need a non-empty object, that an `af_druntime_init` failing
  after druntime initialized also ends the session, and that a `search`
  neighborhood holds at most 16 LLCs per NUMA node.

## 1.9.0 — 2026-10-20

See the [release overview and migration notes](docs/releases/1.9.md).

- Release-candidate benchmark checkup: the native Windows build now resolves
  library imports before the benchmark directory, preventing the benchmark
  `actor_wave.d` from shadowing the Fiber library module when building
  `scaling`. The C/D lifecycle benchmark initializes the versioned actor-kind
  options through `af_actor_kind_options_default`. Generated performance
  tables report the full warm Fiber burst, omit the misleading residual
  drain rate, and label Fiber workers separately from the controller.

- Pre-push performance report for `cb2fb26..fac31ea` on the 5900XT (34
  cells, five pairs, then 20 pairs on the ten flagged cells; 340 + 400
  samples, none excluded). Six-consumer cells: within ±1% except `private`
  +2.3%, `tree-cont` +2.5%, `tree-pipelined` +2.1%, `tree-cancel` +3.5%,
  `dtor` +4.1%, `subset` 10% −1.6% (stddev 0.5%) and `subset` 100% −2.9%
  (stddev 0.2%). Serial cells: +0.2% to +9% except `tree-cancel` −5.3%
  (stddev 0.8%). The two consistent negatives are the subset callback (three
  dependent loads, one of them a park field whose line moved when the root
  park gained its slab pointer) and the serial cancellation cell, which sits
  with the other zero-consumer layout swings; `dtor` and `subset-whole`
  serial cells span 24% to 78% from single outlier pairs. Evidence is under
  `.build/push-gate-2026-10-20/` (ignored).

- PERFORMANCE.md is rebuilt around a headline table (payload, private root,
  public actor and warm Fiber throughput at 2 to 40 threads), a tail-latency
  table at 2 to 32 threads, and the comprehensive array, all as median ± sample
  standard deviation from five samples on the Ryzen 9 5900XT. The 12700H
  tables are retired. The report driver gains the `headline` and thread-swept `tail`
  sections, `--threads`, `--tail-threads`, `--fiber-tasks` and `--markdown`,
  topology-ordered pin lists, and standard deviations in every summary. The
  tail benchmark measures only the library's consumer loop: the out-of-band
  mailbox scene, the attribution mode and the extra consumer-count check are
  removed. Placement, in the tail harness and the Fiber benchmark's new
  worker-count argument, fills one physical group (cores, then SMT siblings)
  before the next, which measured 69.6 versus 38.5 Mcycles/s for private
  roots at 16 threads against spreading cores across both CCDs.

- C ABI 1 checkup before freezing: `af_actor_kind_options` gains the
  `struct_size`/`abi_version` prefix, a zero `reserved` word and
  `af_actor_kind_options_default` (56 bytes); `af_private_child_options` gains
  a zero `reserved` word (96 bytes). The batch creation and reclaim contracts
  state observable guarantees (bounded chunks, whole prefix reserved before
  initializers, unused reservations and reclaimed capacity returned by the
  time the call returns) instead of the current free-list mechanics, so the
  deferred free-index-stack redesign needs no contract change. `antfarm.h`
  states the return conventions: yes/no answers are exactly 0 or 1, outcome
  enums never return an `af_status`, and wake/send treat a null handle as
  stale. Renames: `af_runtime_*` session calls are `af_druntime_*` (they
  initialize the D runtime, not an actor runtime), the context's
  self-retirement is `af_actor_retire_self`, private cost parameters are
  `avg_cost` everywhere, and `AF_PRIVATE_CHILD_COST` is
  `AF_PRIVATE_CHILD_DEFAULT_COST`.

- The pre-push obligations are one gate, stated once in CONTRIBUTING: `make
  check` on the final tree, a performance report when a measured path changed,
  and the Windows matrix before a release. The TSan time budget, the mutation
  catalog guidance and the duplicated gate text are gone. `perf_gate.py` is a
  report, not a verdict: it classifies each cell against the threshold, flags
  cells for a 20-pair rerun (not a pass, span above 6%, or median below
  −1.5%), reports paired min, max, span and stddev, and exits 0 when the
  report is complete or 2 when it is not. It no longer refuses to start on a
  stale load average, and background CPU off the pinned set is recorded as
  telemetry instead of excluding samples: on this host the old 0.25-core rule
  sat inside the idle baseline (median 0.20, p99 0.26 core, much of it the
  monitor's own polling). A sample is excluded only when another task used the
  benchmark's pinned CPUs or the run timed out. `--initial-load-limit` and
  `--no-background-cpu-filter` are removed; `--boundary` and `--control`
  remain as the rerun and host-spread options.

- `ConsumerView.subscribe` now retries the transient frontier race itself and
  returns only the epoch, `SUBSCRIBE_FULL` or `SUBSCRIBE_INVALID`; the former
  one-attempt call is `subscribeOnce`, which still reports `SUBSCRIBE_RETRY`.
  The C `af_subscribe` retries the same race internally and never returns
  `AF_RETRY`, which stays registered. `FarmHost`,
  `ant_fibers` and the examples drop their retry loops. `FarmHost.createPark`
  takes the root storage policy as a template argument. The onboarding
  commands no longer set `ANTFARM_HUGE_PAGES=0`: ordinary pages are the
  default, and the variable is documented once under memory backing. The
  quickstart program and every D example carry a `dub.sdl` header naming
  this repository, so `dub run --single <file>` builds them without the
  compiler flags; the direct compiler commands remain documented.

- Added opt-in root state slabs: `PrivateRootPark!(T, step, reclamation,
  PrivateStateStorage.cohortSlab)` reserves one zeroed slab of
  `capacity * T.sizeof` at create, under the new
  `PrivateRootOptions.slabByteLimit`, so root spawn never allocates and a
  terminal root clears its slot where the reclamation policy would have freed
  it. The C `af_private_root_options` gains `slab_byte_limit`, `storage` and a
  zero `reserved` word (96 bytes). On the 5900XT with mimalloc, `private-slab`
  measured about 34% more lifecycles per second serially and 27% more with six
  consumers than `private`; `private-remote-slab` (consumer-side release)
  about 36% serially and seven times with six consumers, because no root state
  is freed across threads. `actor_churn` has `private-slab` and
  `private-remote-slab` modes; `run_performance_md.py` includes them.

- Restored layout-specific writer sizing loops for inferred fixed-length,
  indexed uniform and mixed-callback tables. `write(callback, bodies)` again
  scans equal-length bodies without carrying the general layout transition
  state through every entry; explicit-length arithmetic sizing is unchanged.
  Partial admission retains the accepted prefix's layout and transactional
  preparation still runs before ring reservation.

- Idle public-actor retirement publishes with a release store after joining
  senders and proving exclusive lifecycle ownership; debug builds retain a CAS
  tripwire. Batch creation detaches at most 64 free slots per lock hold, allowing
  reclaimers to interleave before construction begins.

- Added runtime-free private actor C exports in `antfarm_private.h`: tracked
  roots, cancellation/deadlines and outcomes, parent-owned cohorts, aligned
  result ports, subset waves, continuations, and per-object or slab state.
  Copy/init forms construct in final storage; descriptors live per park, with
  no per-member prefix or wrapper. Existing private custody and join machinery
  drives the C callbacks. Private parks may use application threads or the
  optional host; the emitter pumps them explicitly and destroys them before
  normal host close.
  `antfarm_private.hpp` adds a move-only C++ `private_roots` owner that retains
  its Farm and destroys the park on scope exit.

- Removed MT payloads from the D core. All payloads are single-shot; independent
  entries still execute across many consumers. Removed the full ring-header
  layout, MT index, iteration/admission counters and secondary payload passes.
- Removed `PayloadHeader` and `initPayloadHeader`. `Callback` is now
  `extern(C) void function(const(ulong)*, size_t) nothrow @nogc @system`;
  `PayloadEntry` stores `call` and `body`, and broadcasts take a callback.
  Use `payloadEntry!fn(buffer, args...)` or `callbackFor!fn()` for D functions.
  Removed `maxCs`, `done`, `pcount`, `MAX_PAYLOAD_ITERS`, `withIteration`
  and `payloadEntryRuntime`.
- Added `payloadRange!fn(source, grain)`; `parallelRange` shares that adapter.
  Grain must be positive. The final grain handles the tail; lengths and write
  counts are measured in grains. Descriptors refer to data read at execution,
  so retain that data until completion. Omit grain for per-element packing.
- Removed `parallelSlices`, `ParallelForm` and `Options.form/slices`.
  `Options` contains `grain`, `costClass`, `wake`, and `wakeContext`. Port MT iteration work to
  independent entries or grains. See [parallel loops](docs/guide/parallel-loops.md).
- Every `write` overload accepts an optional completion hook after `avgCost`,
  including mixed-callback tables. The existing table completion transition
  joins all accepted payload callbacks. Removed `writeTracked`; pass the hook
  after the token and cost class to `write`. Partial writes notify only their
  accepted table, and zero writes do not notify. Completion callbacks also
  use C linkage; hook storage stays two pointers.
- Blocking loops wake peers after accepted writes and on idle polls. Use
  `FarmHost.parallelFor` with caller consumption or `host.parallelOptions`.
  Nested waits inside callbacks or hooks are fatal in release builds too.
- The table header is five words: sentinel, next sequence, packed length/kind/
  cost, shard count, hook. Size derives from the next sequence. Removed the
  unused consumer snapshot; unified sizing, decoding and idle re-walk loops.
- Added a quick-lane BetterC check that compiles the core, parallel layer and
  actors, then links and runs C callbacks and hooks from a native C main without
  druntime. Validated with LDC and DMD on Windows and Linux.
- Added [C ABI 1](docs/design/c-abi.md): a BetterC static core, opaque Farm,
  producer and view handles, all three write forms with hooks and cost classes,
  options/version checks, thread-local status diagnostics, and a fatal observer
  shared with D callers. Added a move-only C++11 ownership adapter.
- Added a separate optional druntime-inclusive static library for the non-Fiber
  FarmHost, explicit runtime initialization/thread attachment, wake/step and
  shutdown. It links the same core; Fiber exports remain deferred.
- Added runtime-free public actor C exports: copied/aligned state, native or
  custom allocation, owners/handles, wake/flush, reusable inbox messages, replies,
  retirement and reclamation. The optional host now drives and owns C actors,
  joining consumers before destroying them. GC rooting remains a normal-D
  adapter.
- Finalized the actor C API with runtime-owned kinds, direct copy/init
  construction, optional destructors, 16-byte value owners, caller-owned inbox
  storage and strided batch/wave operations. Retirement pins and exclusive
  reclaim claims make copied owners safe without changing D move-only ownership.
- Added runtime-free C actor waves with erased state, prefix publication,
  generation-tagged observations, phase completion and a callback-return join
  before reuse/destruction. Waves also run on the optional non-Fiber host.
- Host wake/state now accept unattached native threads. State queries return a
  status plus an output state; null arguments are recoverable. Ordered close
  refusals use `FarmHostNotDrained` and map to `AF_BUSY`.
- C++ wrappers now default to empty, with nonthrowing status factories and
  checked ownership allocation in exception-enabled and disabled builds.
  Consumer creation returns transient retry to the caller. Optional
  `farm::make`/`producer::make` conveniences throw only in enabled builds.
- C ABI checks execute under NDEBUG, audit all archive exports/dependencies,
  link every core member with a native C toolchain, and cover allocation
  rollback, native registration churn and unattached host producers.
- Native C/C++ checks cover both compilers, release failures, partial admission,
  concurrent joins, completion before write return, GC roots and parked hosts.
  Actor tests include native-thread creation/consumption and deterministic
  message release before reply wake. The core ABI runs in the quick gate; full gates also check the optional host.

## 1.8.3 development — included in 1.9.0

- Added `antfarm.parallel`: range adapters, asynchronous
  submission handles and completion hooks, blocking help-drain calls, and
  a scoped foreach adapter. Assertion builds reject blocking loops in Farm
  callbacks and table hooks. See [parallel loops](docs/guide/parallel-loops.md)
  for partial submission, lifetimes and joins, and the M1 benchmark for tuning.

- `FarmHost.step()` now batches up to 16 emission rounds before waking workers.
  This amortizes pool-wide wakes for cheap actor tables while retaining the
  wake needed for reclamation on backpressure. Each round visits every emitter;
  an empty round ends the burst early. `FarmHostOptions.maxEmitRounds = 1`
  restores single-round stepping. Attachments may now be called several times
  per step and must return promptly, reporting actual publications. See
  [host tuning](docs/guide/scaling-groups.md#cheap-work-with-parked-workers) for batch,
  idle-policy and deadline guidance.
- `FarmHost.step()` now wakes parked workers even when its emitters publish
  nothing. This releases completed epochs retained by sleeping consumers and
  prevents publication-backpressure loops from stalling with indefinite waits.
  Raw publication loops must step on refusal and wake after accepted writes.
  Parked-worker step loops may incur more wake overhead; spin mode is unchanged.

## 1.8.2

1.8.2 reorganizes the repository. Module, package, directory and make-target
names change with no compatibility aliases, shims or forwarding modules; see
the migration table below.

### Repository layout

- The root modules are one `antfarm` package: `antfarm/package.d` (still
  `import antfarm;`), `antfarm/templates.d`, `antfarm/allocation.d` and
  `antfarm/actors/`. The root dub package builds `sourcePaths "antfarm"` and
  writes its targets to `bin/`, since an executable named `antfarm` would
  collide with the source directory.
- The Fiber package has one name, `ant_fibers`: dub subpackage, directory and
  module root. Its diagnostics start with `ant_fibers:`.
- `FarmHost` stays in the threadpool package as `threadpool.host`, and
  `threadpool` still depends on `antfarm`.
- Every test suite lives under `tests/` (`farm`, `actors`, `private`,
  `threadpool`, `ant_fibers`). The package dub configurations
  reference `../tests/<package>/`, so `dub test :<package>` and
  `dub run :<package> -c <config>` work as before.
- Performance tools live under `tools/perf/`, including the Fiber benchmarks
  in `tools/perf/ant_fibers/` (still `dub run :ant_fibers -c benchmark*`).
  `tools/grant_lock_pages.d` is the Windows privilege helper.
- Design notes live under `docs/design/`; the Fiber performance notes are a
  section of [PERFORMANCE.md](PERFORMANCE.md#fibers).
- `make docs` keeps source paths; start at `.build/docs/antfarm/package.html`.

### Documentation and tooling

- [Using Ant Farm well](docs/guide/using-it-well.md) (new) collects the
  correctness and performance rules on one page, each checked against the
  code and linked to its owner.
- The documentation map follows a payload-first onboarding path. The README is
  shorter; First payload lives in the [quickstart](docs/guide/quickstart.md).
  ARCHITECTURE, the guides and the lifecycle reference no longer repeat each
  other's rules.
- `tools/perf/perf_gate.py` builds revisions from before and after the
  reorganization, and maps benchmark imports across a mixed pair.
  `--build-only` builds both sides without measuring.
- The `tests/farm` `baseline` target builds again.

### Migration table

| Old | New |
| --- | --- |
| `antfarm.d` | `antfarm/package.d` (module `antfarm` unchanged) |
| `antfarm_templates.d`, `import antfarm_templates;` | `antfarm/templates.d`, `import antfarm.templates;` |
| `antfarm_allocation.d`, `import antfarm_allocation;` | `antfarm/allocation.d`, `import antfarm.allocation;` |
| `actors/*.d`, `import actors;` | `antfarm/actors/*.d`, `import antfarm.actors;` |
| `actors.actor`, `actors.wave`, `actors.private_actor`, `actors.mimalloc` | `antfarm.actors.actor`, `antfarm.actors.wave`, `antfarm.actors.private_actor`, `antfarm.actors.mimalloc` |
| static-assert prefix `antfarm_templates:` | `antfarm.templates:` |
| root dub outputs `./antfarm`, `./libantfarm.a` | `bin/antfarm`, `bin/libantfarm.a` |
| dub `antfarm:antfarm-fibers` (`:antfarm-fibers`) | dub `antfarm:ant_fibers` (`:ant_fibers`) |
| `fibers/`, `-Ifibers/source` | `ant_fibers/`, `-Iant_fibers/source` |
| `import antfarm_fibers;`, `antfarm_fibers.*` | `import ant_fibers;`, `ant_fibers.*` |
| Fiber diagnostic prefix `antfarm_fibers:` | `ant_fibers:` |
| `import threadpool.host;` (`FarmHost`) | unchanged: `threadpool.host` |
| `review_torture/` and `antfarm_test.d` | `tests/farm/` |
| `make farm-tsan`, `make run-farm-tsan`, `make check-farm-ordering` | `make -C tests/farm farm-tsan`, `run-farm-tsan`, `check-farm-ordering` |
| `actor_torture/`, binaries `actor_torture*` | `tests/actors/`, binaries `actors_torture*` |
| `actor_torture/.build/` (mimalloc archives) | `tests/actors/.build/` |
| `actor_torture/run-mimalloc-windows.ps1` | `tests/actors/run-mimalloc-windows.ps1` |
| `private_actor_tests/`, binaries `private_actor_tests*` | `tests/private/`, binaries `private_tests*` |
| `threadpool/tests/` | `tests/threadpool/` |
| `fibers/tests/` | `tests/ant_fibers/` |
| `perftest/`, `make -C perftest` | `tools/perf/`, `make -C tools/perf` |
| `fibers/benchmarks/` | `tools/perf/ant_fibers/` |
| `grant_lock_pages.d` | `tools/grant_lock_pages.d` |
| `review_torture/FARM_ORDERING.md` | `docs/design/farm-ordering.md` |
| `threadpool/DESIGN.md` | `docs/design/threadpool.md` |
| `fibers/DESIGN.md` | `docs/design/ant_fibers.md` |
| `fibers/PERFORMANCE.md` | the Fibers section of `PERFORMANCE.md` |
| `make docs` entry `.build/docs/antfarm.html` | `.build/docs/antfarm/package.html` |
| CHANGELOG sections 1.8.0 and earlier | `docs/releases/1.8.md`, `docs/releases/1.7-and-earlier.md` |

## 1.8.1

1.8.1 intentionally breaks the threadpool CPU-query API, with no deprecation
aliases; see the migration table below.

### Fixes and additions

- `AntFarm.canFit(header, bodyWords, tier)` (new) reports whether one
  singleton payload fits a producer tier's quota at the live consumer count.
  It never fatals and reserves nothing; a later `write` still validates.
- `canFit` and `write` share one table base-size helper; `write` codegen is
  unchanged.
- An idle consumer held behind a stalled older epoch no longer re-walks every
  table of that epoch on each poll. The oldest-trailing sweep resumes at a
  per-consumer cursor (review torture T27: 764 tables per poll, now 764 then
  1).
- [PERFORMANCE.md](PERFORMANCE.md) records `checkFreshWriteSegments` as a
  deliberately kept tripwire cost.
- `accepted(ActorWakeResult)`, `accepted(ActorSendResult)` and
  `accepted(ActorRetireResult)` (new, UFCS) name the results that admitted a
  request.
- `ActorWaveHandle.interrupted` (new) reports a wake, send or retirement
  admitted across the wave owner's publication gap. Such a benign race no
  longer sets `failed` (see Compatibility notes).
- Assertion builds print `antfarm warning: ActorOwner dropped without
  reclaim` to stderr when a live `ActorOwner` leaves scope; release builds
  have no destructor. `ActorOwner.leak()` (new) is the deliberate opt-out.
- Actor slot creation and `FiberDomain.release` enforce load-bearing
  invariants with fatal diagnostics in debug and release builds. Redundant
  actor retirement and test-hook diagnostics stay assertion-build-only.
- Private child allocation rollback and callback completion share their
  cleanup paths; behaviour is unchanged.
- The four `PrivateContext.createChildren` overloads share one
  implementation; signatures, attributes and generated code are unchanged.
- `make -C actor_torture death` is a public child-process harness: rows
  expect an abort with its exact message, a warning, or silence, and cover
  the public actor and wave fatal sites. `make check` runs it.
- `FiberSemaphore.wait` returns a permit acquired during registration, and an
  auto-reset `FiberEvent.wait` restores a consumed set, when cancellation wins
  before the waiter resumes. Previously another waiter could be stranded.
- TSan builds keep a sanitizer context per fiber task, so a suspended fiber
  can migrate between threads without underflowing TSan's shadow stack.
- Managed worker pumps drain queued remote cancellation requests, serialized
  with admission and recycling; release and shutdown stay Director-only. The
  pump counts applied requests without allocating, and
  `drainCancellationRequests()` still returns per-request results.
- Offline Linux CPUs stay discoverable with `noLlcIndex` and no longer form
  their own fallback LLC domains.
- Threadpool shares topology scans, selection loops and teardown paths.
- Pool start on Windows throws `threadpool: WaitOnAddress unavailable (need
  Windows 10 1803+)` instead of parking workers through a null pointer.
- [threadpool/README.md](threadpool/README.md) notes that CFS bandwidth
  quotas are not detected; size quota-limited pools explicitly.
- `scripts/check.sh` no longer needs `/bin/sh`, derives its CPU-pinned lanes
  from the process affinity (`CHECK_SMALL_CPUS` / `CHECK_ONE_LLC_CPUS`
  override; empty counts as unset; `--print-cpus` shows the choice), and
  `make check-quick` runs an LDC-only subset.
- Test harness only: the `delayed-final-credit` and `ports-poll-visibility`
  workers in private_actor_tests unsubscribe before waiting. Their inactive
  consumer view retained a ring pin, a circular wait that could hang the
  test. The suite's progress deadlines are uniform 60 s hang timeouts.
- Test harness only: the `dtor-gc` collector pauses 1 ms between collections
  so actor callbacks can acquire druntime's GC root-range lock under load.

### Compatibility notes

- Threadpool no longer has the Windows event-wait fallback. `WaitOnAddress`
  is required, with Windows 10 1803+ as the supported floor.
- `ActorWaveHandle.failed` now means a structural admission rejection only.
  A wake, send or retirement admitted across the wave owner's publication gap
  used to set `failed`; it now sets `interrupted` and does not fail the
  operation.

### Windows integration

- A native Python integration runner covers packages, threadpool affinity/wait
  and FarmHost tests, Farm ordering, review/T19, actor suites and examples with
  LDC and DMD. It provides quick/focused selection and process-tree timeouts;
  Windows coverage stays separate from the Linux correctness gate.
- The caller confinement/restoration regression now runs on Windows, checking
  thread group affinity and the unchanged process mask.
- Windows wait tests force a missing `WaitOnAddress` and check the startup
  rejection in debug and release builds before running the normal wait races.
- The example runner uses explicit Windows executable paths, supports compiler
  selection and omits TSan and the POSIX terminal front end on Windows.

### Breaking changes: two threadpool CPU-query surfaces

Threadpool had four overlapping ways to ask which CPUs a program may use:
`TopologySnapshot`, `OsScope`, `PoolResolution` and `FarmHostPlan`. Two
remain. `TopologySnapshot` is the machine plus the frozen OS scope, and
`PoolResolution` is the per-option disposition of every CPU, request and
group. The retired symbols are removed outright, with no deprecation
aliases.

- The OS scope is part of the snapshot: `TopologySnapshot.osScopeSource`,
  `osScopeAllowed` (sorted, empty when unrestricted, may name CPUs the
  snapshot does not know) and `inOsScope(id)`. `discover()` fills them from
  the same frozen capture that sets `affinityExcluded`. `isEligible` also
  consults the scope list, which makes no difference for discovered
  snapshots.
- `resolvePool(snap, options)` drops the `OsScope` argument. A CPU is
  `osExcluded` when flagged or outside `snap`'s scope, so a hand-built
  snapshot remains the whole test seam.
- `Reject.notSelected` (new): the CPU passed every filter, but a placement
  policy above the pool (FarmHost group choice, worker count, caller
  reservation) left it out. It is never combined with another reason, and
  `resolvePool` never sets it. `Reject.all` is now `0x3F`; use it rather than
  a literal mask. `GroupDisposition.candidates` (new) counts filter-passing
  CPUs (accepted plus `notSelected`), and the report's group table prints it.
- `FarmHost.resolve(options)` / `FarmHost.resolve(snap, options)` replace
  `preview`. They return one `PoolResolution`: the workers are accepted,
  every other filter-passing CPU is `notSelected`, and the requests stay the
  user's `options.pool.processors`. The result is never null, including with
  `workers == 0`. The same problems throw, and a throwing call still warms
  discovery.
- A live `FarmHost` has `resolution` (the started selection, which is
  `host.pool.resolution`: the pool now starts from the host's selection),
  `callerCpus`, `summary()` and `toString()`. Caller CPUs and Farm sizing are
  only visible on a live host.
- `FarmHostPlanException` is renamed `FarmHostResolveException`. Its message
  prefix is now `FarmHost options rejected: `.

| Old | New |
| --- | --- |
| `struct OsScope`, `osScope()` | `discover()` (or `CacheAwarePool.topology()`), then `.osScopeSource` / `.osScopeAllowed` |
| `OsScope.source` / `OsScope.allowed` | `snap.osScopeSource` / `snap.osScopeAllowed` |
| `threadpool.os_scope.OsScopeSource` | `threadpool.topology.OsScopeSource` (also `import threadpool;`) |
| `inOsScope(id)` | `snap.inOsScope(id)` |
| `threadpool.os_scope.scopeOsToCallingThread` | `threadpool.topology.scopeOsToCallingThread` (also `import threadpool;`) |
| `threadpool.os_scope.liveOsMask` | `threadpool.pin.liveOsMask` (also `import threadpool;`) |
| `resolvePool(snap, scope_, options)` | `resolvePool(snap, options)`; fakes set `snap.osScopeSource` / `snap.osScopeAllowed` (`OsScope unrestricted;` just goes away) |
| `PoolResolution.osScope` | `res.topology.osScopeSource` / `res.topology.osScopeAllowed` |
| `FarmHost.preview(options)` | `FarmHost.resolve(options)` → `PoolResolution` |
| `FarmHost.preview(snap, scope_, options)` | `FarmHost.resolve(snap, options)`, with the scope on the snapshot |
| `host.plan` | `host.resolution`, `host.callerCpus`, `host.summary()`, `host.toString` |
| `FarmHostPlan.options` | the options you passed |
| `.candidates` | `resolution`: `candidates.isAccepted(id)` ⇔ `(resolution[id].reasons & ~Reject.notSelected) == Reject.none`; per group `groups[g].candidates` |
| `.workerSelection`, `.workerSelection.workers` | `resolution`, `resolution.workers` (same workers, same start order; non-workers carry their real reason or `notSelected` instead of `notRequested`) |
| `.workers` | `resolution.workers.length` |
| `.groups` | `resolution.acceptedGroups` |
| `.farmGroup` | `HostGroups.span(llcs)`: `llcs[0]`; otherwise the single group in `resolution.acceptedGroups` |
| `.crossGroup` | `resolution.acceptedGroupCount > 1` |
| `.caller` | `options.workers == 0 ? CallerPlacement.unchanged : options.caller` |
| `.callerCpus` | `host.callerCpus` (live host only) |
| `.callerSharesWorkerCpu` | `host.callerCpus` intersects `resolution.workers` |
| `.callerConsumes` | `options.callerConsumes \|\| options.workers == 0` |
| `.participants` (`HostParticipants`, removed) | printed by `host.summary()`: native = workers in the home group, foreign = the rest, external = `callerConsumes + farm.extraConsumers`, small = `1 + farm.extraSmallProducers`, bulk = `farm.bulkProducers` |
| `.expectedConsumers` / `.maxSmall` / `.maxBulk` | `workers + external` / `1 + extraSmallProducers` / `bulkProducers` |
| `.notes` | `host.toString`; as data, `GroupDisposition` (`common`, `candidates` vs `accepted`) and `host.callerCpus` |
| `.summary()` / `.toString` | `host.summary()` / `host.toString` (`FarmHost.resolve(o).summary()` before construction, worker part only) |
| `FarmHostPlanException` | `FarmHostResolveException` |
