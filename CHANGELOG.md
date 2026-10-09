# Changelog

## Unreleased — targeting 1.8.0-rc.1

No `v1.8.0-rc.1` tag exists yet. This section describes the contents of the
development line that will become the release candidate; when it is cut, this
heading, the banners in [README.md](README.md) and
[ARCHITECTURE.md](ARCHITECTURE.md), and the tag are switched together.

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

### Compact tables for single-threaded payloads

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

### Public actors (1.7.0-rc.5 prerequisites and later)

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
- Compiled examples and a shared lifecycle and shutdown guide
  ([docs/reference/actor-lifecycle.md](docs/reference/actor-lifecycle.md), [examples/](examples/README.md)).

### Performance work

Measurements are host specific and are not promises. Methodology and the gate
are in [perftest/README.md](perftest/README.md).

- A committed-ref optimization gate (`make perf-gate`).
- A fast path for roots without deadlines, whole-wave callback policy selected
  once, derived work-cost hints for root tables, and targeted inlining of public
  retirement and reclamation.
- A cache-line placement experiment was a negative result and shipped nothing.
  One hint change landed with a documented one-off layout waiver.
- An option screen for mimalloc page retention made no runtime change.

### Compatibility notes and limits

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
  storage, and engine wake integration (see [ROADMAP.md](ROADMAP.md) and
  [docs/guide/public-actors.md](docs/guide/public-actors.md)).

## Development milestones

These are not published releases; no tags were created for them.

- **1.7.1-rc.1** (private actors, October 2026): the private baseline above and
  its first features.
- **1.7.0-rc.5** (completion joins, October 2026): the shard-completion and
  public wave-credit fixes above.

Earlier candidates (1.7.0-rc.1 to rc.4) and the 1.6 line are described in
[ROADMAP.md](ROADMAP.md).
