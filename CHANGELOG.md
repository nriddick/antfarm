# Changelog

## 1.8.0 (unreleased)

Open for the life of the long-lived `1.8.0-rc` branch; it becomes the 1.8.0
release entry at merge.

### Windows verification fixes

- Optional Win32 function pointers now retain Windows linkage. DMD x64 workers
  previously busy-looped instead of parking through `WaitOnAddress`. The LLC
  wake regression test now shuts down before reporting a failed assertion.
- The actor-wave benchmark isolates over-aligned actor initialization from
  its cleanup frame, avoiding a DMD 2.112 Win64 `Director` destruction crash
  without changing actor layout or the measured workload.
- The MT Fiber benchmark prints explicit cold and warm creation-plus-drain
  timings. The performance report accepts these even when a residual drain
  phase is not measurable, while retaining support for older numeric logs.
- The off-LLC sweep uses native executable paths for both execution and
  metadata hashing, fixing missing Windows `.exe` hashes.

### Threadpool CPU groups

- `ProcessorId(group, lp)` is the one CPU identity, now in
  `threadpool.topology` with `of`, `(group, lp)` ordering and the `g:lp` text
  form. `parseCpuList` reads `0-7,16-23` and `g:a-b` lists (ordered, duplicates
  kept, `CpuListException` with the offending offset); `formatCpuList` prints
  them back.
- Each `LlcDomain` records a `GroupingSource` (`l3`, `l2Fallback`,
  `singletonFallback`, `cpuSetLlcIndex`), and `TopologySnapshot.groupingSource`
  is the weakest one present, so fallback shards are not mistaken for CCDs.
- `LogicalProcessor.offlineAtDiscovery` reports Linux offline CPUs.
  `TopologySnapshot` gains `byId`, `isEligible`, `processorsInLlc`,
  `eligibleProcessorsInLlc`, `eligibleLlcCount` and `processorsOfClass`.
- `TopologySnapshot.cores` is sorted by each core's first `ProcessorId`.
  Before, its order depended on associative-array iteration.
- `threadpool.resolution`: `resolvePool` and `PoolResolution` report every
  known CPU (`Reject` reason union), every requested id (unknown, accepted,
  duplicate) and every physical group (physical/eligible/accepted/started
  counts). `CacheAwarePool.resolve()` previews start; `pool.resolution` keeps
  the start result with each worker's `StartState` and `PinOutcome`.
  `enforceRequested()` is the opt-in exact-request check.
- `threadpool.os_scope`: one frozen, process-level OS scope (`osScope`,
  `OsScopeSource`, `inOsScope`, `liveOsMask`, `scopeOsToCallingThread`).
- `pinProcessor(LogicalProcessor)` / `pinProcessor(ProcessorId)` return a
  `PinOutcome` (`pinned`, `unknownProcessor`, `outsideOsScope`, `osRejected`)
  and validate every pin against the frozen scope on both OSes, capturing the
  scope first when no discovery has run yet (Windows captures lazily).
- `perftest/actor_churn` parses its CPU argument with `parseCpuList`
  (`0,1-7`, `g:lp`) and reports the failing id and `PinOutcome`.
- `pool.wakeLlc(g)` wakes only the workers assigned to physical group `g`.
- `makeBins` builds a physical-group-indexed table from a resolution;
  `binStaffed!T(llc)` reports whether a group had accepted workers.
- `hello_antfarm` exercises one Farm per accepted group and reports skipped
  groups; `hello_topology` prints the OS scope, eligibility and the default
  resolutions; `perftest/placement` records each role's LLC and core, reports
  `crossesLlc` and accepts an optional LLC filter (default order unchanged).

### Fiber service plans

- `FiberLane` separates planned service (`lane.service`: `LaneService`
  `native`, `rescued`, `unstaffed`, `disabled`, `absent`) from runtime
  admission (`lane.state`: `FiberLaneState` `pending`, `open`, `closed`,
  `stopped`, `failed`), and reports `lane.group` and `lane.liveServers`.
  Refused spawns throw the typed `FiberLaneUnavailable` (state, service,
  group).
- Fixed: queued work on another lane of the same domain no longer fails the
  pool at startup. The startup check is lane-local (`lane.ready` or
  `lane.published` on a lane that did not open), not the domain-wide `active`
  count.
- Fixed: a lane no longer reports itself served after pool shutdown. When its
  last server stops it becomes `stopped`, and a later spawn throws instead of
  stranding an undrainable task. A served lane that never had a server (a
  shutdown right after `start`, a worker's start hook failing, or an
  uninstalled plan) also becomes `stopped` instead of staying `pending`.
- `ManagedWorkerHooks.poolStopped` (optional) runs once on the shutting-down
  thread after every worker has joined, even if none started. The fiber hooks
  use it to retire lanes that no server stopped.
- Fixed: a timer armed by a fiber running on a non-responder worker (a custom
  `responderSelector`) now wakes the domain's responders when it becomes the
  earliest deadline. Before, a parked responder could miss it until an
  unrelated wake. `FiberReadyLane` gains the `noteTimerArmed` hook; a
  responder arming its own timer adds no wake.
- `FiberServicePlan` (`antfarm_fibers.service_plan`) binds one lane per
  physical group of a `PoolResolution`, plans each group's `LaneService`, and
  derives each Farm's `FarmRequirement` (native, foreign and external consumer
  views and small producer slots) before the Farms exist. `rescue` serves a
  group with no accepted workers; `assist` and `assistRing` are experimental,
  opt-in edges for staffed groups; `disable` closes a group; `external`
  declares non-pool participants. `problems()` reports every mistake together,
  and `validate()` throws `FiberPlanException`. `installFiberPlan(plan, pool)`
  is the single, validated installer (`pool = null` for plan-only tests);
  `uninstallFiberPlan()` removes it.
- Shared Fiber domains no longer add cross-LLC consumers implicitly. The
  former automatic ring measured 42% (one lane loaded) and 73% (both loaded)
  slower than no edges on two LLCs. `benchmark_mt` takes `private`, `shared`
  or `assist` to reproduce that A/B.
- `FiberWakePolicy.servingGroups` (opt-in; `broadcast` stays the default)
  wakes only the groups that serve a lane: its own group and its rescue/assist
  server groups. A new earliest timer armed off-responder wakes the groups
  hosting the domain's native lanes. On two LLCs with one busy lane, the idle
  group's worker no longer pumps.
- The single-lane benchmarks (`benchmark_farm_embed`, `benchmark_vs_druntime`,
  `benchmark_actor_wave`) draw their workers from one LLC group and print it:
  on multi-LLC hosts they are capped to one LLC's CPUs.

### Performance tooling

- `perftest/offccd` and `perftest/c2c` measure off-CCD consumer/producer
  scaling of one Farm and core-to-core latency from the threadpool's eligible
  L3 groups; `placement.planRoles` plans them and `pinVerdict` turns pin
  outcomes into run status. `perftest/offccd_sweep.py` runs and summarizes the
  suites.

### Breaking changes

- X0: `Token` (`antfarm`), `ActorOwner!T` and `ActorErasedOwner` (`actors`),
  and `threadpool.Director` are now move-only. Their transfer-on-copy
  constructors (which emptied the source) are disabled with
  `@disable this(ref typeof(this))`, because Phobos algorithms that take
  elements by value silently invalidated owners (for example
  `owners.all!(o => o.retired)` emptied `owners[0]`). Copying an owner or a
  token, assigning an lvalue (`owner = other`), and passing one to a
  by-value parameter no longer compile. Transfer ownership with `move`
  (`std.algorithm` or `core.lifetime`), lend with a `ref` parameter, and
  iterate owner arrays with `foreach (ref o; owners)`. `opAssign` still
  accepts an rvalue (`owner = move(other)`) and still fatals when it would
  overwrite a live owner or token. Returning by value, array default
  initialization and `new ActorOwner!T[n]` are unchanged.

- X1: `ProcessorId` moved to `threadpool.topology`; `threadpool.pool` no longer
  declares it. Import it from `threadpool` or `threadpool.topology`.
- X2: `TopologySnapshot.lpsInLlc`, `pLps` and `eLps` are removed. Use
  `processorsInLlc(i)`, `eligibleProcessorsInLlc(i)` or
  `processorsOfClass(classP / classE)`.
- X3: `PhysicalCore`, `LlcDomain`, `NumaNode` and `Package` `lpIndices: ushort[]`
  became `processors: ProcessorId[]`, sorted by `(group, lp)`. Print them with
  `formatCpuList`.
- X4: `parkedAtDiscovery` now means Windows CPU-set parking only. Linux offline
  CPUs set `offlineAtDiscovery`, and OS-scope exclusion no longer sets the
  parked flag on either OS. Replace `version (linux) if (p.parkedAtDiscovery)
  continue;` with `if (!snapshot.isEligible(p)) continue;`.
- X5: `PoolOptions.onlyLps` is removed and `onlyProcessors` is renamed
  `processors`. Use `opt.processors = parseCpuList("0-7")` or a topology
  helper; ids are group-qualified, so an LP number never matches in another
  group. Duplicates collapse to one worker and are reported.
- X6: `PinScope` and `PoolOptions.pinScope` are removed; only
  `logicalProcessor` pinning ever existed. Delete the assignment.
- X7: `CacheAwarePool.llcCount` and `CacheAwarePool.llc(i)` are removed. Use
  `pool.resolution.physicalGroupCount` / `acceptedGroupCount` or
  `CacheAwarePool.topology().llcDomains[i]`.
- X8: start-failure messages changed text (types unchanged): an empty
  selection carries `summary()`, a pin failure names each `g:lp` and its
  `PinOutcome`.
- X9: the Linux OS scope is the launch mask captured before `main`, not the
  mask of whichever thread discovers first. A program that narrows `main`'s
  mask before discovery calls `scopeOsToCallingThread()` first or filters with
  `PoolOptions.processors`. Windows is unchanged.
- X10: `pinToLogicalProcessor` and `PinTarget` are removed. Use
  `pinProcessor(p)` for a `LogicalProcessor` or `pinProcessor(id)`, and test
  `== PinOutcome.pinned`. Pins outside the frozen scope now fail on both OSes
  (`outsideOsScope`): an explicit CPU list outside a launch `taskset` fails
  loudly instead of escaping it.
- X11: `bins.install(bins)` and `install(bins, llcCount, classCount)` are
  removed and `BinAxis` is no longer exported. Use
  `install(makeBins!C(res, ...), res)` (or `install(bins, res, classCount)`)
  with `res = pool.resolve()`; the table must have one slot per physical group
  and non-null slots for accepted groups.
- X12: spawning on a lane planned without service (no accepted workers and no
  rescue) throws `FiberLaneUnavailable` before the pool starts, and spawning
  after every server of a lane stopped throws too. Catch
  `FiberLaneUnavailable`, or plan a rescue.
- X13: the responder requirement is per domain, not per lane (a relaxation):
  a staffed lane whose own workers are all non-responders opens when another
  lane of its domain has a responder. Some configurations that failed now
  pass.
- X14: `installFiberLanes`, `uninstallFiberLanes`, `installRemoteCoverageRing`
  and `FiberLane.bindPool` are removed, and there is no automatic coverage
  ring. Build `new FiberServicePlan(pool.resolve())`, size Farms from
  `plan.requirement(g)`, `bind(g, lane)` each accepted group, then
  `installFiberPlan(plan, pool)` / `uninstallFiberPlan()`; re-install the plan
  to move it to another pool with the same selection. Use `assistRing(groups)`
  to reproduce the old ring and `rescue` for unstaffed groups. Single-lane
  benchmarks restrict their workers to one group.
- X15: `FiberCoverage`, `lane.coverage`, `cover`, `coveredLanes` and
  `coveringLanes` are removed. Use `lane.state`, `lane.service` and
  `lane.liveServers` for lane health and `plan.edges` for the service graph.
- X16: the `FiberBackend` alias is removed and the `FiberLane.backend` field
  is renamed `domain`; the former `domain` property merges into the field.
  Replace `FiberBackend` with `FiberDomain` and `lane.backend` with
  `lane.domain`.
- X16 follow-up: `FiberTask.backend` is now `FiberTask.domain`; the remaining
  locals, parameters and test fields holding a `FiberDomain` follow it, and
  `FiberJoinStatus.wrongBackend` becomes `wrongDomain`.
- X17: `assembleSnapshot` takes a new required last parameter,
  `const(GroupingSource)[] sourceOfLlc` (one entry per LLC); each `LlcDomain`
  records it and `TopologySnapshot.groupingSource` is the weakest. Callers
  that build snapshots by hand pass `GroupingSource.l3` per group.
- X18: `TimerSet.insert` returns `bool` (true when the new entry became the
  earliest deadline) instead of `void`; a responder arming an earlier timer
  uses it to wake the domain's other responders.

## 1.8.0-rc.2

Second release candidate. Its runtime is identical to 1.8.0-rc.1: no file in
the Farm, actor, threadpool or fiber packages changed. The rc.1 local Linux
validation therefore still applies. The examples lane, which builds and runs the
new sample's headless tests, passes at this revision. Windows validation still
follows the candidate.

- Add a Conway's Game of Life [sample](examples/README.md#conways-game-of-life)
  demonstrating one private actor per cell, joined waves and application-owned
  double-buffered projections. Exact headless tests run in the examples lane;
  the POSIX terminal front end uses vendored termbox2 v2.5.0.

## 1.8.0-rc.1

This is the release candidate for the 1.8.0 line. The version banners in
[README.md](README.md), [ARCHITECTURE.md](ARCHITECTURE.md) and
[ROADMAP.md](ROADMAP.md) name it; it is tagged `v1.8.0-rc.1` and was superseded
by 1.8.0-rc.2.

Validation scope: the correctness gate is the local Linux `make check` (see
[CONTRIBUTING.md](CONTRIBUTING.md)); there is no CI. The exact revision,
toolchains and lanes that passed are recorded with the release, not here.
Windows validation follows the candidate; a Windows pass is not claimed. Multi-LLC
validation and Fiber ThreadSanitizer migration remain limited as stated in
[ARCHITECTURE.md](ARCHITECTURE.md#one-llc-and-many-llcs) and
[fibers/DESIGN.md](fibers/DESIGN.md#validation-boundary).

### Farm correctness

- The `Wt` reservation is an acquire/release RMW, so a producer spending a
  retained quota balance acquires the reclamation history that other producers
  imported before it stores into the words it reserved.
- A reclaimed subscription fallback is rejected before quota reuse, and the
  unsafe post-unsubscribe plant was removed; the remaining plants stay inside
  producer publication ownership.
- Shard completion is an acquire/release RMW, so a table hook or wave observer
  acquires every primary-run callback's writes within the table (see
  [SPEC.md](SPEC.md#7a-primary-work-claims)). Secondary and tertiary
  multi-threaded callbacks are not part of that join.
- Reclamation is validated through real atomic edges under ThreadSanitizer:
  the manual acquire/release annotations were removed and the TSan lanes
  exercise the real chain, with deterministic S1/S2 schedules and a leaf-reuse
  control. See [review_torture/FARM_ORDERING.md](review_torture/FARM_ORDERING.md).

### Compact and uniform tables for single-threaded payloads

- A table whose payloads are all single-threaded is stored in a compact
  encoding without a per-payload ring header ([SPEC.md](SPEC.md#2f-compact-st-tables)).
  `PayloadHeader` is still 128 bytes, and mixed or multi-threaded tables keep
  the full layout.
- **Callback header lifetime.** For compact records the `PayloadHeader*` passed
  to a callback points to a temporary consumer-local copy. A callback must not
  modify it or retain the pointer after returning. Previously it pointed into
  the ring and stayed valid until the table was recycled.
- **Quota admission changed.** Far more single-threaded entries fit in a given
  quota than with the full layout, so tables, accepted prefixes and
  backpressure points shift. Do not carry entry counts from earlier sizing
  guidance; see [How many entries fit a quota](ARCHITECTURE.md#how-many-entries-fit-a-quota).
- Benchmark harness: `digest.d` was retired, throughput and dual sizing account
  for compact singleton tables, and the `tree-cancel` private benchmark is not
  comparable across this change. The change has one accepted regression in a
  six-consumer private cross-thread-free cell; it is an accepted trade-off, not
  a general slowdown.
- **Uniform-callback tables.** A single-threaded table whose accepted prefix
  shares one callback stores it once per table and each payload as its bare
  body; when the bodies also share one nonzero length the table drops its
  payload index too ([SPEC.md](SPEC.md#2g-uniform-call-st-tables)). A one-word
  body costs 2 or 1 ring words instead of 3. Every `write` overload detects
  this from the contents, and all actor tables use the index-free form. Tables
  with several callbacks, multi-threaded payloads or empty bodies keep their
  previous layout, and the callback header contract is the compact one above.
- **Quota admission changed again.** Uniform tables admit up to three times
  more entries per quota than compact ones, so actor and single-callback tables
  are larger and split at different points. Entry counts from earlier sizing
  guidance and benchmark tables measured on the compact layout do not carry
  over.

### Public actors (1.7.0-rc.5 prerequisites and later)

- `ActorInboxNode.completeAndWake(sender)`, `failAndWake(sender, code)` and
  `cancelAndWake(sender)` complete a request/reply interaction and then
  schedule the sender, so the sender actor observes the reply without a
  manual `sender.wake()` (a missing wake used to hang the sender silently).
  They accept `ActorHandle!U` for any `U` and `ActorErasedHandle`, and return
  the `ActorWakeResult`. The completion store happens before the wake.
- `ActorInboxNode.payloadAs!T` is a plain cast of the node payload to `T*`,
  replacing the hand-written `cast(Request*) node.payload` at every read.

- Public wave completion keeps its descriptor alive until each holder's last
  access: a producer hold and one credit per admitted table replace the old
  progress-equality fence. `finished` still does not join the optional
  completion-hook tail; wave storage and the hook context must outlive the
  hook's last access.
- Public node outcomes (`ActorNodeOutcome`, `node.fail`, `node.cancel`) and
  cooperative cancel-mode retirement (`requestRetire(ActorRetireMode.cancel)`,
  `context.cancelling`, `cancelInbox`). There is no guaranteed final callback.

### Private actors (1.7.1-rc.1 baseline and later)

A second actor model, not a drop-in replacement for public actors: self-owned
roots, exclusively parent-owned child cohorts, explicit emitter and consumer
roles, configurable root and child batches, and fatal ownership checks. See
[docs/guide/private-actors.md](docs/guide/private-actors.md).

- Root waits: `waitChildren` and `finishAfterChildren`.
- Typed POD result ports, joined results, and result folds.
- Moved, non-POD private state with consumer-only destruction, and a
  `gcRooted` allocator adapter.
- Opt-in subset waves and child continuations after descendant joins.
- Outcomes and cancellation: `context.fail`, wave outcomes, `park.cancel`,
  `cancelChildren`, `cancelPeers`, tracked roots with `PrivateRootHandle`,
  lazy deadlines, `cancelAll` and `takeOutcome`.
- Opt-in per-cohort state slab (`PrivateStateStorage.cohortSlab`).
- A per-wave child work-cost hint: `start`, `startFinal` and `startSubset` take
  an optional trailing `avgCost` class, so cheap leaf callbacks can claim larger
  runs (for example `avgCostForChunk(128)`) while the default stays chunk 32.
  A focused test pins the claim split.
- Compiled examples and a shared lifecycle and shutdown guide
  ([docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md), [examples/](examples/README.md)).

### Documentation

- The README is an onboarding page with a quickstart, a work-representation
  choice table and three reader journeys; [CONTRIBUTING.md](CONTRIBUTING.md)
  separates local iteration from pre-push hardening.
- `docs/` is split into guide, reference, design and development pages with one
  named owner per rule in [docs/README.md](docs/README.md). `make check-docs`
  validates tracked-Markdown links and heading anchors and checks that every
  public declaration is documented; `make docs` builds the Ddoc API reference.
- A forward-looking [ROADMAP.md](ROADMAP.md) and this changelog were added, and
  the API and design pages carry current SPEC references.

### Performance work

Measurements are host specific and are not promises. Methodology and the gate
are in [perftest/README.md](perftest/README.md).

- A committed-ref optimization gate (`make perf-gate`).
- A fast path for roots without deadlines, whole-wave callback policy selected
  once, derived work-cost hints for root tables, and targeted inlining of public
  retirement and reclamation.
- The derived root hint now also splits sharded root tables: a table just above
  the small-table threshold uses shorter claims until its shards provide about
  one claim per consumer (up to eight), instead of one claim per shard. Tables
  wide enough for 32-entry claims to reach that target are unchanged.
- A cache-line placement experiment was a negative result and shipped nothing.
  One hint change landed with a documented one-off layout waiver.
- An option screen for mimalloc page retention made no runtime change.

### Compatibility notes and limits

- `MAX_CHUNK` is 128 (was 32) and `avgCost` is `0 .. 7` (was `0 .. 5`). Every
  `write` overload defaults to `DEFAULT_AVG_COST` (3), so the default run stays
  16 entries, and cheap uniform work can opt into classes 0 through 2 (runs of
  128, 64, 32). An explicit class `k` now means runs of `128 >> k`, four times
  longer than before: source that named a class should add 2 (see
  `avgCostForChunk`). The library's own callers (actor flush and waves, private
  child waves, the derived root hint, the Fiber lanes) were moved so their runs
  are unchanged. Public actor flush and wave tables default to chunk 8, private
  child-wave tables default to chunk 32, and the child wave starts take the
  per-wave override above.
- The 0-consumer private `subset` cells (10%, 100% and
  `subset-whole-unreserved`) read 5–25% below the pre-RC commit d152004. This
  is an accepted code-layout trade-off, not a cost of the child-wave cost
  hint. Restoring the constant child class does not recover the subset cells,
  and an inert-code control moves them by as much. The 6-consumer subset cells
  are unchanged. Data: [PERFORMANCE.md](PERFORMANCE.md#private-actors).
- The actor APIs are supported release-candidate functionality. Source and ABI
  stability are not promised between release candidates.
- Public actor state and results are POD; private state may be non-POD.
- The API is manual-ownership `@system` code. Borrow, context and park types
  prevent some mistakes but do not prove uniqueness of raw pointers.
- Pumping is explicit. Completion is observed by the application, and
  `pump() == 0` is not a drain. A tracked root holds its slot until
  `takeOutcome`.
- Cancellation is cooperative. No final public callback is guaranteed.
- Deferred: typed root outputs, external wake of a waiting root, timers,
  per-member retirement, concurrent subsets, non-POD results, adopted actor
  storage, and engine wake integration (see [ROADMAP.md](ROADMAP.md)).

### Fixes

- Fiber responders now service persistent remote subscriptions at least once
  per 16×R local pumps for R covered lanes, rotating on success and failure.
  Busy local work or an earlier busy remote can no longer indefinitely pin a
  later remote Farm's epochs and stop its producers from admitting work.

- `--fake-topology` shares each L3 across every CPU instead of each SMT
  pair, so the generated tree describes one LLC on the 5500 (it previously
  described six). Siblings group by `(physical package, core id)`, CPU lists
  are emitted as ranges, and an optional L3 group list describes a
  multi-CCD layout, so a non-contiguous layout is described correctly
  ([perftest/run_performance_md.py](perftest/run_performance_md.py)).
- Threadpool topology discovery intersects the process affinity
  (`sched_getaffinity` on Linux, `GetProcessAffinityMask` on Windows), so a
  `taskset`- or cpuset-restricted process no longer plans placements on
  forbidden CPUs. Forbidden CPUs are parked, the same signal consumers
  already skip for offline CPUs
  ([threadpool/source/threadpool/topology.d](threadpool/source/threadpool/topology.d)).
- `private_lifecycle_matrix.py` includes the CPU list in its grouping key, so
  runs pinned to different CPU sets no longer merge into one summary
  ([perftest/private_lifecycle_matrix.py](perftest/private_lifecycle_matrix.py)).

## Development milestones

These are not published releases; no tags were created for them.

- **1.7.1-rc.1** (private actors, October 2026): the private baseline above and
  its first features.
- **1.7.0-rc.5** (completion joins, October 2026): the shard-completion and
  public wave-credit fixes above.

Earlier candidates (1.7.0-rc.1 to rc.4) and the 1.6 line are recorded here for
history. They were development milestones, not published releases.

### 1.7.0 candidates

- **1.7.0-rc.4 — usability and shutdown fixes:** `AntFarm.create`'s first
  parameter became `ringMiB` (a power of two in `[2, MAX_RING_MIB]`) instead of
  a ulong length; `ConsumerView.subscribe` failures became distinguishable
  (`SUBSCRIBE_FULL` -1, `SUBSCRIBE_RETRY` -2, `SUBSCRIBE_INVALID` -3);
  `PoolOptions.workerStop` runs once on each worker during shutdown so
  worker-owned consumers unsubscribe deterministically;
  `CacheAwarePool.shutdown(bool drain)` was deprecated; assigning over a
  still-registered producer `Token` became fatal; generated payload shims
  require implicit argument conversion; an `ANTFARM_HUGE_PAGES` value other
  than `0` or `1` became fatal; Fiber `unittest`/`stress` keep asserts in
  `--build=release`; and a Fiber ready-lane accounting race that the
  stripped asserts had hidden was fixed.
- **1.7.0-rc.3 — lifecycle performance:** consolidated the host-audit fixes
  (swept quota grants, serialized producer registration, armed waits,
  fresh-segment protection checks in every build) with actor lifecycle
  optimizations (bounded ready-queue snapshots, linear actor creation and
  backlog scans, reused table ownership for single-shot payload admission, and
  optional pinned mimalloc v3.5.3 linking).
- **1.7.0-rc.2 — specification:** [SPEC.md](SPEC.md) was rewritten as one
  synthesized living specification of the current algorithm, replacing the
  separate edit notes.
- **1.7.0-rc.1 — actor waves:** made `actors` a directly onboarded work
  representation alongside bare payloads and Fibers: explicit non-GC actor
  ownership, generation-tagged handles, exclusive callback-local borrowing,
  coalesced autonomous activation, intrusive inboxes, sealed multi-table
  waves, and Fiber generation triggers for dependent phases.

### 1.6 line

- **1.6.1 — Windows integration:** patched the Fiber cache-line allocator to
  use the same Microsoft CRT-safe 64-byte allocation path as the core Farm,
  avoiding a C11 `aligned_alloc` import unavailable in DMD's Microsoft runtime
  bindings. Verified on Windows x64 on 2026-08-30.
- **1.6.0 — objective and scope:** shipped the unified Ant Farm, cache-aware
  threadpool, and migrating Fiber scheduler as one repository. Required
  generic `write()` sources to be forward ranges; added common-header and
  fixed-body-length write forms; routed Fiber ready activations through the
  fixed one-word path; defaulted Farms to 4 KiB backing with huge pages as an
  explicit option; removed the experimental Fiber parallel-loop API with no
  replacement compatibility promise; made each README an onboarding document;
  and kept examples as real compile targets. Cross-package release gates ran
  on 2026-08-29 on the Linux/Ryzen 5 5500 host.
