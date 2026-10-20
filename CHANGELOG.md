# Changelog

Older release notes are in [docs/releases/](docs/releases/):
[1.8.1, 1.8.0 and its release candidates](docs/releases/1.8.md) and
[1.7 and earlier](docs/releases/1.7-and-earlier.md).

## 1.9.0 — unreleased

1.9.0 makes every payload single-shot, collapses the payload callback to a
C-linkage body-only function, joins tables through an optional hook on every
`write`, and adds `antfarm.parallel` and a C/C++ ABI. It also changes how
`FarmHost.step()` emits and wakes. It folds in the unreleased 1.8.3 progress
branch. As in 1.8.2, removed and renamed APIs have no compatibility aliases;
see the [migration table](#migration-from-182).

### Farm core

- **MT payloads are removed.** Every payload runs exactly once on one
  consumer; independent entries still execute across many consumers. The full
  in-ring payload header, MT index, iteration/admission counters and secondary
  payload passes are gone. Work that several consumers entered as one payload
  becomes several payloads or grains sharing an application counter
  ([SPEC 11](SPEC.md#mt-payloads-removed-19)).
- **Callbacks take only their body.** `Callback` is now
  `extern(C) void function(const(ulong)* body, size_t bodyWords) nothrow @nogc @system`.
  `PayloadHeader` and `initPayloadHeader` are removed; `PayloadEntry` holds
  `call` and `body`, and broadcast writes take a `Callback`. Build entries
  with `payloadEntry!fn(buffer, args...)` or `callbackFor!fn()`.
  `TableCompletionCallback` also uses C linkage; a hook is still two pointers.
- **Every `write` overload accepts a completion hook** after `avgCost`,
  including mixed-callback tables and zero-length bodies. The table finisher
  joins all accepted callbacks before notifying. A partial write notifies
  only its accepted table; a zero write notifies nothing. `writeTracked` is
  removed.
- **`payloadRange!fn(source, grain)`** publishes one entry per grain of a
  finite, sliceable random-access range. The final grain handles the tail,
  and lengths and write counts are in grains. Descriptors refer to data read
  at execution, so retain it until completion. Without `grain`, arguments are
  packed per element as before. A zero grain is fatal in every build.
- The table header is five words: sentinel, next sequence, packed
  length/kind/cost, shard count and hook. Size derives from the next
  sequence. The unused consumer snapshot is gone, and sizing, decoding and
  idle re-walk share one loop.

### Parallel loops (new)

- [`antfarm.parallel`](docs/guide/parallel-loops.md) adapts countable ranges
  into payloads:
  - `parallelRange!fn` for caller-owned batches: the one-element form of
    `payloadRange`, per element or per grain;
  - `parallelFor!fn` for asynchronous submission, with a move-only
    `ParallelHandle`, retryable `publish` and a completion hook;
  - `parallelForWait!fn` and `foreach` over `parallel(view, token, range)`
    for blocking regions that help-drain the caller's view and keep region
    storage on the stack.
- `Options` holds `grain`, `costClass`, `wake` and `wakeContext`. Blocking
  loops call `wake` after every accepted write and on idle polls, so parked
  consumers release reclamation pins.
- `FarmHost.parallelFor!fn(source, options)` runs a blocking region on the
  control thread with caller consumption. `host.parallelOptions(options)`
  supplies the host wake to the low-level adapters.
- A blocking loop inside a Farm callback or table completion hook is fatal
  in every build, including empty loops.
- 1.8.3-progress users: `parallelSlices`, `ParallelForm` and
  `Options.form`/`slices` did not survive the MT removal. Port MT slices to
  grains.

### FarmHost

- `step()` now batches up to `FarmHostOptions.maxEmitRounds` (default 16)
  emission rounds before waking workers. Each round visits every emitter, and
  a round with no publications ends the burst. This amortizes pool-wide wakes
  for cheap actor tables. **Attachments may be called several times per
  step**: return promptly, including on backpressure, and report actual
  publications. `maxEmitRounds = 1` restores single-round stepping. See
  [host tuning](docs/guide/hosting.md#cheap-work-with-parked-workers).
- `step()` wakes parked workers even when nothing was published. Sleeping
  consumers can pin completed epochs, so a refused publication needs their
  polls before a retry succeeds. Raw publication loops must step on refusal
  and wake after accepted writes. Parked-mode step loops may pay more wake
  overhead; spin mode is unchanged.
- Ordered `close()` refusals throw `FarmHostNotDrained`.

### Public actors

- `runtime.live` counts occupied slots: in-flight construction reservations
  as well as retired generations awaiting reclaim. `runtime.destroy` fatals
  while reservations are live.
- Lifecycle transitions and occupancy accounting are fused; see the
  [lifecycle measurements](PERFORMANCE.md#public-actor-lifecycle-optimization-190-progress).

### C and C++ ABI 1 (new)

[C ABI 1](docs/design/c-abi.md) targets 64-bit Windows and Linux. All exports
and callbacks use C linkage.

- **Core** (`antfarm.h`, `antfarm_core`): a BetterC static library with
  opaque Farm, producer and view handles, all three write forms with hooks and
  cost classes, options/version checks, thread-local status diagnostics, and a
  fatal observer shared with D callers. It links from a native C `main`
  without druntime.
- **Public actors and waves** (`antfarm_actors.h`, in the core):
  runtime-owned kinds, direct copy/init construction, optional destructors,
  native or custom allocation, 16-byte value owners, caller-owned inbox
  messages, replies, strided batch/wave operations, retirement and
  reclamation. Retirement pins and exclusive reclaim claims make copied owners
  safe without changing D's move-only ownership. Waves add erased state,
  prefix publication, generation-tagged observations, phase completion and a
  ready query that joins the completion callback before reuse or
  destruction. GC rooting stays a D-only adapter; private actor exports come
  later.
- **Optional runtime** (`antfarm_runtime.h`, `antfarm_runtime`): a separate
  druntime-inclusive static library with the non-Fiber `FarmHost`, explicit
  runtime initialization and thread attachment, wake/step and shutdown. It
  links the same core, drives and owns C actors and waves, and joins
  consumers before destroying them. Unattached native threads may call wake
  and state queries; state queries return a status plus an output state, and
  `FarmHostNotDrained` maps to `AF_BUSY`. Fibers are outside ABI 1.
- **C++** (`antfarm.hpp`): move-only C++11 ownership wrappers that default to
  empty, with nonthrowing status factories and checked ownership allocation
  in builds with and without exceptions. Consumer creation returns transient
  retry to the caller; `farm::make`/`producer::make` throw only when
  exceptions are enabled.
- Examples: [c_core.c](examples/c_core.c), [c_actors.c](examples/c_actors.c),
  [c_wave.c](examples/c_wave.c) and [c_host.c](examples/c_host.c).

### Validation and tooling

- The quick gate compiles the core, parallel layer and actors as BetterC,
  then links and runs C callbacks and hooks from a native C `main` without
  druntime, with LDC and DMD on Windows and Linux. Full gates also check the
  optional host.
- C ABI checks run under `NDEBUG`, audit every archive export and dependency,
  link every core member with a native C toolchain, and cover allocation
  rollback, native registration churn and unattached host producers. Native
  C/C++ checks cover both compilers, release failures, partial admission,
  concurrent joins, completion before write return, GC roots and parked
  hosts. Actor tests include native-thread creation/consumption and
  deterministic message release before reply wake.
- `make -C tests/farm check-parallel` covers ranges, joins and the callback
  guard; [M1](tools/perf/README.md#parallel-loop-m1) is the parallel-loop
  benchmark grid.

### Migration from 1.8.2

| 1.8.2 | 1.9.0 |
| --- | --- |
| `long function(PayloadHeader* head, PayloadBody body, ulong iteration)` | `extern(C) void function(const(ulong)* body, size_t bodyWords)`; the return value and iteration are gone |
| `PayloadHeader`, `initPayloadHeader`, `PayloadEntry(header, body)` | `PayloadEntry(call, body)`; `callbackFor!fn()` returns the generated callback |
| `payloadEntry!(fn, maxCs, done, withIteration)(&header, buf, args...)` | `payloadEntry!fn(buf, args...)` |
| `payloadEntryRuntime`, `withIteration`, `MAX_PAYLOAD_ITERS`, `maxCs`, `done`, `pcount` | removed: publish independent payloads or `payloadRange!fn(source, grain)` |
| `write(header, bodies, ...)`, `broadcastPayloads(header, bodies)` | `write(callback, bodies, ...)`, `broadcastPayloads(callback, bodies)` |
| `writeTracked(header, bodies, bodyWords, hook, token, avgCost)` | `write(callback, bodies, bodyWords, token, avgCost, hook)`; every overload takes the trailing hook |
| D-linkage `TableCompletionCallback` | `extern(C)`; same `void function(void* context)` shape |
| `HostAttachment.emit` called once per `step()` | up to `maxEmitRounds` times per step; set `maxEmitRounds = 1` for the old cadence |
| 1.8.3-progress `parallelSlices`, `ParallelForm`, `Options.form`/`slices` | ST grains through `Options.grain`, `parallelRange` or `payloadRange` |

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
