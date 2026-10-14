# Outcomes and cancellation

A private domain failure is a value, not `fatal`. `context.fail(code)` records
it and returns; the callback still returns its normal step. It is valid in
plain, result, continuation, and result-continuation operations. Call it at
most once per member per wave; extra calls are counted and not diagnosed. A
failed result member still writes the `R` it returns; the usual convention is
`R.init` or a partial value.

The enclosing wave aggregates reports. `park.outcome(context)` is the snapshot
after the join: participants, dispatched, the report count, the first failure's
code and participant position, and whether this wave's own scope was cancelled.
Before the first nonempty start every field is zero. `succeeded` means no
reports and a full dispatch. Per-member detail belongs in `R`. Which concurrent
report supplies the representative pair is unspecified; the code and position
always belong to the same report. A whole wave's position is the spawn index. A
subset's position is the selection index, valid until the next nonempty start.

Nothing fails a parent automatically. After the join the parent reads `outcome`
and chooses: recover, or propagate with its own `context.fail`. Propagation
toward the application is explicit at each level. A root's `fail` sets its
tracked outcome to `failed`; the first code wins across all of its activations.
It is fatal on an untracked park, where a root reports through application
storage instead. Wrong or default context is fatal for both `fail` and
`outcome`. `outcome` and `results` while a wave is active are fatal. `uint.max`
reports in one wave are fatal before the count wraps.

A rejected start preserves the last nonempty wave. `start`, `startFinal`, and
`startSubset` reset the aggregate only after the active, retiring, and empty
rejections, and before the enqueue. An empty start, a busy start, and a void
operation after a result wave therefore leave the previous snapshot in place
until a new nonempty wave is actually published. `cancelled` and a `dispatched`
prefix shorter than `participants` come from cancellation.

Partial construction needs no extra API. If `createChildren` returns null or
`spawn` does not return `spawned`, a member calls `fail`. If no wave of that
actor is active it returns `done`; otherwise it calls `cancelChildren()` and
returns `done`, which still drains the cancelled waves. Never-dispatched members
are destroyed once when the owning actor retires.

## Cancellation

Cancellation is cooperative and advisory. It never interrupts a callback and
never wakes a waiting root or suspended member early. It shortens the existing
joins: the emitter stops publishing a cancelled wave's remaining tables, and
running or resumed callbacks see `context.cancelled` and finish early. Every
wait still ends through the unchanged credit chain.

- `park.cancel(context)` cancels that cohort's active wave and returns true. It
  returns false, with no effect, when it observes the cohort idle. Only the
  park's parent may call it.
- `context.cancelChildren()` cancels the active wave of every cohort the actor
  owns. Idle cohorts are unaffected. Roots and members may call it.
- `context.cancelPeers()` cancels the member's enclosing wave: its siblings
  and, through them, their descendants. It is fatal on a root context.
- `context.cancelled` is true when a wave on the path from this activation to
  its root is cancelled, or when the tracked root itself is (a handle,
  `cancelAll`, or an expired deadline). It may become true at any time and never
  becomes false within an activation. A root context sees only root-level
  cancellation, so on an untracked park a root's `cancelled` is false.

What a cancelled wave does:

1. **Undispatched work is skipped at table granularity.** Before each initial
   table the emitter checks the wave's scope. Once it is cancelled, the
   published prefix is final. A non-retiring wave seals at once. Its remaining
   members do not run, and their state, results, and children are untouched, so
   they stay ordinary live members for a later wave or for disposal. A
   `startFinal` wave publishes the remaining members in cleanup tables. These
   retire each member on a consumer without calling the operation or writing a
   result, so `startFinal` still retires every member exactly once. Subsets
   never retire.
2. **Resumptions are always delivered.** A suspended continuation is resumed
   into its operation, sees `cancelled`, and should return `done` (possibly
   after `fail`). Skipping it would leave its phase in `T` inconsistent and its
   result unwritten.
3. **Roots are always dispatched.** A root is the outcome authority. To stop
   its tree it calls `cancelChildren()` and returns `finishAfterChildren`, or
   `finish` if nothing is active. `finish` with an active child wave stays
   fatal.
4. **The join is never cut short.** The parent owns the child state, so it must
   join before it may finish or dispose anyway.

`outcome.dispatched` is the prefix that ran the operation, and
`results(context)` returns only that prefix (`min` of the result count and
`dispatched`). A retiring wave's prefix survives `count_ = 0`.
`outcome.cancelled` means this wave's own scope was cancelled, by its parent or
a peer. It reports that a cancel was requested during the generation, not that
work was skipped. A cancel that lands just as the wave finishes reports
`cancelled` with a full `dispatched`. A wave skipped because an ancestor was
cancelled has `dispatched < participants` with `cancelled` false. The wave flag
is reset by the next nonempty start. A per-root summary byte is sticky until the
root slot is recycled.

Recipes:

- Fail-fast (one failure stops the siblings and their descendants):
  `c.fail(code); c.cancelPeers();` then return as usual. The resumed parent
  reads `outcome` and propagates with its own `fail`, `cancelPeers`, or
  `cancelChildren`.
- First-wins search: the finder calls `c.cancelPeers()`. A polling root calls
  `park.cancel(c)` and keeps returning `again` until `finished`.

**Termination is eventual, not bounded by depth.** A cancelled tree drains under
fair pumping and consumption. The drain still includes cleanup tables for
truncated `startFinal` waves (N members at batch limit b take about N/b pumps),
resume tables for members that were already suspended, Farm backpressure, root
collection waiting for a whole physical batch, and any running callback until it
next checks `cancelled` or returns. Shutdown deadlines must budget for that
volume. A callback that ignores `cancelled`, or a member that never returns
`done`, is not interrupted.

## Root tracking and deadlines

Root tracking is one opt-in switch, `PrivateRootOptions.tracking`, passed to
`PrivateRootPark.create(farm, capacity, options)`. It gives each root slot a
record, which enables:

- **Identity.** `spawn(initial, out PrivateRootHandle root, deadline =
  MonoTime.max)` returns a copyable, non-owning handle to that root generation.
  `root.index` is a dense slot index below `capacity`; a recycled slot keeps its
  index under a new generation, so compare handles with `==`. `spawn(initial)`
  on a tracked park still tracks the root, with no deadline, and discards the
  handle.
- **Cancellation from any thread.** `root.requestCancel()` is the one park
  operation another thread may call. It returns `requested`, or
  `alreadyRequested`, `finished` (the root already reached terminal collection),
  or `staleHandle` (its outcome was taken, or a default handle). It only sets
  the root's cancel bit: the root and its descendants observe it through
  `context.cancelled`, and the emitter stops publishing initial tables in that
  tree. Writes made before a successful request are visible to an activation
  that observes it. Join every thread that may call `requestCancel` before the
  park's `destroy`.
- **`cancelAll()`.** On the emitter, cancels every live, unfinished root and
  returns how many it cancelled. It costs O(capacity).
- **Lazy deadlines.** A deadline other than `MonoTime.max` arms the root. It is
  not a timer: the deadline becomes cancellation, with cause `deadline`, only
  when something observes it. Observers are the root or a descendant reading
  `context.cancelled`, the emitter before an initial child table in that tree,
  and `cancelAll`. A root that never checks and never emits after its deadline
  completes normally. Only armed, not-yet-cancelled roots read the clock.
- **Root `fail`.** `context.fail(code)` on a root makes its outcome `failed`.
- **Outcomes (X1a).** A terminal root keeps its slot, and its place in `live`,
  until `takeOutcome(out PrivateRootOutcome outcome)` pops it in collection
  order and recycles the slot. The outcome names the root's handle (already
  stale) and its `status`. For `cancelled` it also carries the `cause`
  (`requested` or `deadline`); for `failed`, the first `code`.

The status is decided once, when the emitter collects the terminal root:
`failed` if the root called `fail`, else `cancelled` if a cancel was requested
before that collection, else `completed`. A cancel that arrives after the root's
work finished but before collection still makes it `cancelled`; the application
asked for it. A later request returns `finished`.

`takeOutcome` does not collect; call `pump` or `collectCompleted` first. Writes
that the root's activations made to application storage are visible on the
emitter thread once `takeOutcome` returns that root; another thread needs
ordinary synchronization with the emitter. Delivery is pull-based, so the
application may spawn a follow-up root between takes.

Untaken outcomes are the backpressure. A park whose slots all hold outcomes
returns `capacityFull` from `spawn`, and `destroy` with an untaken outcome is
the usual `live != 0` fatal. **A tracked drain must pump and take outcomes**
until `live == 0`; pumping alone never gets there. A cancel-mode shutdown is
`cancelAll()`, then that same loop. Tracking-only calls (`spawn` with a handle,
`takeOutcome`, `cancelAll`, root `fail`) are fatal on an untracked park.

```d
import antfarm;
import antfarm.actors;
import core.time : MonoTime, seconds;
import std.stdio : writeln;

struct Request { uint id; uint polls; }
// Request 0 finishes, request 2 fails, the rest run until cancelled.
PrivateRootDisposition serve(scope ref PrivateBorrow!Request request,
    scope ref PrivateContext context) nothrow @nogc @system
{
    if (context.cancelled) return PrivateRootDisposition.finish;
    if (request.value.id == 2)
    {
        context.fail(404);
        return PrivateRootDisposition.finish;
    }
    if (request.value.id == 0 && ++request.value.polls == 3)
        return PrivateRootDisposition.finish;
    return PrivateRootDisposition.again;
}

void main()
{
    auto farm = AntFarm.create();
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid) fatal("producer registration failed");
    ConsumerView view;
    if (view.subscribe(farm) < 0) fatal("consumer subscription failed");
    PrivateRootOptions options;
    options.tracking = true;
    auto park = PrivateRootPark!(Request, serve).create(farm, 8, options);
    if (park is null) fatal("private park allocation failed");
    PrivateRootHandle[5] roots;
    foreach (uint id; 0 .. 5)
    {
        auto deadline = id == 3 ? MonoTime.currTime - 1.seconds : MonoTime.max;
        if (park.spawn(Request(id), roots[id], deadline) != PrivateSpawnResult.spawned)
            fatal("private spawn failed");
    }
    // Any thread may do this while the park exists.
    if (roots[1].requestCancel() != PrivateCancelResult.requested) fatal("cancel failed");
    foreach (_; 0 .. 8)
    {
        park.pump(token);
        while (view.consumeNext()) {}
    }
    // Shutdown: cancel the rest, then pump AND take outcomes until live == 0.
    park.cancelAll();
    PrivateRootOutcome[5] outcomes;
    while (park.live != 0)
    {
        park.pump(token);
        while (view.consumeNext()) {}
        PrivateRootOutcome outcome;
        while (park.takeOutcome(outcome))
            foreach (id, root; roots)
                if (root == outcome.root) outcomes[id] = outcome;
    }
    alias S = PrivateOutcomeStatus;
    alias C = PrivateCancelCause;
    if (outcomes[0].status != S.completed
        || outcomes[1].status != S.cancelled || outcomes[1].cause != C.requested
        || outcomes[2].status != S.failed || outcomes[2].code != 404
        || outcomes[3].status != S.cancelled || outcomes[3].cause != C.deadline
        || outcomes[4].status != S.cancelled || outcomes[4].cause != C.requested)
        fatal("unexpected root outcomes");
    writeln("completed, cancelled, failed 404, deadline, cancelled at shutdown");
    park.destroy();
    park = null;
    view.unsubscribe();
    farm.unregisterProducer(token);
    farm.destroy();
}
```

Cost: an untracked park pays one `records` null branch in `spawn`, terminal
collection, and each cancel check, and the park is 32 bytes larger. A tracked
park allocates 32 bytes per root slot and pays, per root, four record stores at
`spawn`, one CAS at terminal collection, and one store at `takeOutcome`. Each
cancel check and each initial child table in a tracked tree adds one acquire
load of the record, plus a clock read only for an armed root. After a root is
cancelled, those checks return at the record without walking the tree. `index`
is 32-bit, so a tracked park's capacity is at most `uint.max`. Typed root
outputs (X1b) are not provided: put them in application storage reached from `T`
and read them after `takeOutcome`.

## Progress and outcomes

| Operation | Result / contract |
| --- | --- |
| Root `spawn` | `PrivateSpawnResult.spawned`, `capacityFull`, or `allocationFailed`; moves non-POD state (copies POD bytes) and returns no handle. On a tracked park the root is tracked with no deadline. |
| Root `spawn(initial, out root, deadline)` | Tracked parks only (fatal otherwise). The same results; on `spawned`, `root` names this generation and `deadline` (default `MonoTime.max`, none) arms a lazy deadline. |
| `PrivateRootHandle.requestCancel()` | Any thread, while the park exists: `requested`, `alreadyRequested`, `finished` (already collected terminal), or `staleHandle` (outcome taken, or a default handle). |
| `takeOutcome(out outcome)` | Tracked parks only. Pops one collected terminal root in collection order and recycles its slot; false if none. `live` falls here. The root's application writes are visible on the emitter. |
| `cancelAll()` | Tracked parks only. Cancels every live unfinished root (cause `deadline` if its deadline passed, else `requested`); returns how many. O(capacity). |
| Child `spawn` | The same outcomes, plus `waveActive` or `retiring`. |
| `start` / `startFinal` / `startSubset` | `PrivateStartResult.started`, `empty`, `waveActive`, or `retiring`. Empty starts are already complete and take no credit. |
| `PrivateChildDisposition.done` / `PrivateChildStep!R.done(value)` | Finish this member's participation; outstanding descendants still hold the wave active. The result form writes its slot once. |
| `PrivateChildDisposition.waitChildren` / `PrivateChildStep!R.waitChildren()` | Join this member's descendants, then publish exactly one later activation of the same operation, even if nothing is outstanding. |
| `context.fail(code)` | Member: counts one report on the enclosing wave and, for the first report, records that code and participant position. Does not end the activation. Root: makes the tracked outcome `failed` with the first code; fatal on an untracked park. Wrong or default context is fatal. A count of `uint.max` is fatal. |
| `outcome(context)` | `PrivateWaveOutcome` of the last nonempty wave, acquired after the join; all zero before the first such wave. Fatal while active or with wrong/default authority. Valid until the next nonempty start. |
| `results(context)` | `const(R)[]` in participant order, acquired after the join; only the dispatched prefix of a cancelled wave. Fatal while active or with wrong/default authority. Available only for non-void `R`. |
| `park.cancel(context)` | True if the cohort's active wave was cancelled; false, with no effect, if it was observed idle. Wrong/default authority is fatal. |
| `context.cancelChildren()` / `context.cancelPeers()` | Cancel every active owned cohort / the member's enclosing wave. `cancelPeers` on a root is fatal; default context is fatal for both. |
| `context.cancelled` | Advisory: true once a wave between this activation and its root is cancelled, or the tracked root is (handle, `cancelAll`, observed deadline). Always false on an untracked root. Default context is fatal. |
| `subsetMembers(context)` | `const(uint)[]` in list order for the most recent nonempty subset; empty after a whole wave. Wrong/default authority is fatal. |
| `finished(context)` | True before the first start and after the current wave joins; a root notification tail may still be pending. Wrong-parent or default authority is fatal, including when the cohort is idle. |
| `collectCompleted()` / `poll()` | Acquires completed root tables and wake returns, reclaims terminal roots, and appends ready roots; never publishes work or runs consumers. |
| `pump(token)` | Collects completions and emits at most one root table and one initial table and one resume table per pending child wave; returns only the number of root activations published, including internal deferred cleanup. Zero does not mean idle. The emitter must keep pumping to resume waiting roots and children. |
| `live` / `capacity` | Emitter-local root counts; `live` includes waiting roots and terminal roots with outstanding notification tails. It falls only on collection after the join, or, on a tracked park, only at `takeOutcome`. Neither getter synchronizes another thread. |
| `destroy()` | Collects once, then requires no live roots (including untaken outcomes) or pending work. It does not drain or cancel waiting roots. Use `park.destroy(); park = null;`. |

The shared shutdown recipe for both actor models is in
[../reference/actor-lifecycle.md#shutdown-recipe](../reference/actor-lifecycle.md#shutdown-recipe).
Its compiled forms are
[../../examples/request_deadline.d](../../examples/request_deadline.d),
[../../examples/fail_fast_fold.d](../../examples/fail_fast_fold.d), and
[../../examples/fiber_cancel_at.d](../../examples/fiber_cancel_at.d).
