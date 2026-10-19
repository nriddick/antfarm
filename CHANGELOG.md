# Changelog

Older release notes are in [docs/releases/](docs/releases/):
[1.8.0 and its release candidates](docs/releases/1.8.md) and
[1.7 and earlier](docs/releases/1.7-and-earlier.md).

## 1.9.0 — unreleased

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
  adapter. Private actor C exports are a subsequent increment.
- Added runtime-free C actor waves with erased state, prefix publication,
  generation-tagged observations, phase completion and a callback-return join
  before reuse/destruction. Waves also run on the optional non-Fiber host.
- Native C/C++ checks cover both compilers, release failures, partial admission,
  concurrent joins, completion before write return, GC roots and parked hosts.
  Actor tests include native-thread creation/consumption and deterministic
  message release before reply wake. The core ABI runs in the quick gate; full gates also check the optional host.

## 1.8.3 — progress branch

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
  [host tuning](docs/guide/hosting.md#cheap-work-with-parked-workers) for batch,
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
