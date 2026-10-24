# Cleanup plan for 2.0

Follows the [surface and onboarding review](surface-and-onboarding-review-2026-10-24.md)
of October 24, 2026. The review lists what overlaps and what costs a reader
time; this page orders the work, names the deliverable and the check for
each step, and says what each step removes. It is a plan, not a commitment:
every step that touches a measured path ends with `make perf-gate`, and the
maintainer decides on the numbers.

Guiding rule: one owner per capability. Where two subsystems own the same
thing (the pool, a computation tree, a parallel loop, idle policy), one
becomes the substrate and the other a client of it, or one goes.

## Order

The steps are ordered so that each removes a maintenance point the next
would otherwise have to carry.

### 1. One owner of the pool: the per-group host

The roadmap's "per-group-Farm host" becomes the substrate under `FarmHost`
and `forkjoin.Dispatcher`.

- Deliverable: `threadpool.host` grows a `GroupHost` (or `FarmHost` gains a
  groups mode): one Farm per accepted LLC group, a token per Farm per
  publishing thread, `at!T(llc)` routing of `publish` and `pump`, the
  caller's placement, the ordered teardown. It exposes the per-participant
  worker body hook the dispatcher needs and a parked bitmap with
  `wakeOne` beside `wakeAll` and `wakeLlc`.
- `Dispatcher.start` shrinks to: build the host, install `GroupBin`s from
  the host's groups, attach the body. `FarmHost` becomes the one-group
  configuration of the same thing.
- Removes: the third copy of topology selection, caller pinning, Farm
  creation and teardown in `forkjoin/dispatcher.d`; hosting.md's
  "hand-written shape" paragraphs and the fwbench `farms.d` pool.
- Check: `dub test :forkjoin`, the host-test lane, fwbench's four
  benchmarks within 2% of the 2.0.0-progress numbers, `make perf-gate` on
  the actor churn and private cells. (In 2.0.0-dev: `GroupHost` exists and
  the dispatcher runs on it; `FarmHost` shares its planner and pool claim
  but is not yet a client. That conversion moved to the roadmap's Next.)

### 2. Idle machinery

- Deliverable: the spin-count atomics (`gSpinsP`, `gSpinsE`) move behind a
  `version (ThreadpoolStats)`; `WorkerIdle.spin` and `Director.spin` then
  cost one `pause` per iteration. The parked bitmap and `wakeOne` from step 1
  are the documented way to wake one worker. `cadence`, `nudge` and `drift`
  either acquire an in-tree user (the Fiber timer wheel is the candidate) or
  move behind a version with their tests.
- Removes: the -7.8% hosting.md measures for spin against a hand-written
  loop, and three Director verbs nothing uses.
- Check: `tools/perf/actor_churn` with `ANTFARM_CHURN_HOST=1` against the
  hand-written loop; the gap closes or the reason is written down. (Done in
  2.0.0-dev: counters behind `ThreadpoolStats`, cadence verbs behind
  `ThreadpoolCadence`, `wakeOne` and `anyWorkerArmed` on the pool; the
  measurement is in hosting.md.)

### 3. Parallel loops over fork-join

- Deliverable: `forkjoin.range`: `forkJoinFor!fn(range, grain)` splitting
  halves down to the grain, and `forkJoinReduce` with a deterministic tree
  (fixed split points, so results are bit-identical across participant
  counts, as citor's `parallelReduce` promises). parallel-loops.md routes
  blocking loops to it; `antfarm.parallel` keeps the asynchronous hooked
  form and the grain adapter, which are Farm features the dispatcher has no
  equivalent for.
- Removes: `parallelForWait`, the `parallel` foreach, `FarmHost.parallelFor`
  and the "never call a blocking loop inside a callback" rule, since the
  fork-join join never blocks. `Options.wake` and `host.parallelOptions`
  stay: the asynchronous region still needs to wake parked workers after
  each accepted write. (Done in 2.0.0-dev.)
- Check: `tests/farm/run_parallel.py` ported to the new entry points; the
  M1 grid in tools/perf shows the balanced cells within noise and the
  skewed cells faster.

### 4. Private trees retreat to their niche

- Deliverable: fork-join frames gain cooperative cancellation (a flag on the
  root that `fork` checks before pushing; a cancelled subtree completes
  without running its children) and an optional deadline through the
  sentinel join. private-actors.md opens by sending bare trees to
  `forkjoin` and keeps typed result ports, non-POD state with destructors,
  tracked outcomes and externally owned storage as its reasons to exist.
- Removes: nothing in code yet; the decision whether `startSubset`,
  pipelined folds and the cohort slab stay is taken after a quarter of
  usage with the new default in place. The fwbench `antfarm_private`
  variant (fib(39) in minutes and gigabytes) is retired from the harness.
- Check: the private test suites unchanged; choosing-work.md's table has
  one row per shape with a default named above it.

### 5. Stubs and history

- Delete `threadpool.exchange` (self-described stub) and its README
  section. Move `HostGroups.span` from the placement defaults into a
  short "when you know better" note. In SPEC section 11, move only the
  "MT payloads removed (1.9)" history into docs/performance/archive; the
  rationale appendix stays, and in particular the lineage passage (Piquer's
  indirect reference counting, SNZI, Dijkstra and Scholten) with its
  provenance note is kept in the SPEC as the owner of why the tally is
  shaped as it is. Say in threadpool/README.md that the managed worker lane
  exists for `ant_fibers`.
- Check: `make check-docs`, the threadpool and fiber suites.

### 6. Onboarding

- README: open with the three rules and the representation table with a
  one-sentence default above it, then the D and native paths side by side.
- A ten-line first program on `forkjoin` or `FarmHost`; the raw ring
  program becomes step two, "what the host does for you".
- Re-export `threadpool.host` from `threadpool`.
- Every example a `dub run --single`; compiler-direct lines move to
  examples/README.md.
- Split hosting.md into "run a host" and "scaling across groups"; move
  numbers out of using-it-well into PERFORMANCE and link.
- Step 7 of the docs map becomes "run the example for your shape"; the gate
  stays in CONTRIBUTING.
- Check: `make check-docs`; a reader who follows the map reaches a running
  multi-threaded program in two steps.

### 7. Duplicated status

- One `VERSION` file read by the release script; README, ARCHITECTURE and
  ROADMAP stop carrying the number in prose. ROADMAP's "1.9.0 progress"
  section closes into CHANGELOG. The quoted check-quick duration and the
  commit hashes leave the guides; measured figures live in PERFORMANCE and
  the design pages only.
- Check: `grep -n '1\.9\.' README.md ARCHITECTURE.md ROADMAP.md` returns
  only links.

## What stays out of scope

The C ABI (ABI 1 is shipped and locked), the Farm core and SPEC sections 1
to 10, the actor ordering proofs, and the Windows matrix are not touched by
this plan except where step 1 moves host code they already call. Fork-join
has no C exports yet; adding them is a separate decision after step 1
settles the host surface.

## Sequence and gates

Steps 1 and 2 are one release unit: the pool substrate and its idle policy
change together, and fwbench's four benchmarks plus the actor churn cells
are the gate. Steps 3 and 4 follow and are gated on the parallel-loop grid
and the private suites. Steps 5 to 7 are documentation and deletion, gated
on `make check-docs` and `make check-quick`, and can proceed in parallel
with 3 and 4. Each step is its own commit with its CHANGELOG line; the
roadmap's 2.0 section links this page and marks steps done as they land.
