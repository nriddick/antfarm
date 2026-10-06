# Actor lifecycle and contracts

This is the shared contract for public actors (`ActorRuntime`, inboxes,
`ActorWave`) and private actors (`PrivateRootPark` and parent-owned cohorts).
The Farm distributes work. It does not create workers, and it does not decide
that an application is finished. Actors add execution and lifetime rules above
that distributor. Neither `ActorRuntime.create` nor `PrivateRootPark.create`
starts a thread.

Complete programs are in [examples/](../examples/README.md). Protocol detail
stays in [PRIVATE_ACTORS.md](PRIVATE_ACTORS.md),
[ACTOR_ROADMAP.md](ACTOR_ROADMAP.md), and
[ACTOR_MEMORYORDER.md](ACTOR_MEMORYORDER.md). Sizing numbers are in
[ARCHITECTURE.md](../ARCHITECTURE.md#sizing-one-farm).

## Farm setup and tier choice

`AntFarm.create` takes ring size in MiB, not ulongs. `ringMiB` is a power of
two, at least 2. Producer quotas are ulongs. `expectedConsumers` is a sharding
hint; the hard cap is 128 subscribed views. Invalid geometry or an impossible
quota budget is fatal at construction. Defaults are enough for a first
program. Tune only after the sizing model, not by guessing.

The budget is the sum of simultaneous grants, `maxBulk * quotaBulk +
maxSmall * quotaSmall`. It must fit in `k - 1` segments. A tier left at
`max* = 0` is disabled. One active producer needs one slot of the tier it
registers. Register a bulk token only when a small quota cannot admit the
batch you actually publish.

Private root batches and the Farm quota are different knobs. The park's
configured batch limit fixes how many root-table slots are reserved.
`pump(token, limit)` may use a smaller limit; that does not add slots. The
default small quota of 4096 ulongs admits roughly 225 one-word private
entries per table. A larger configured batch is split into accepted prefixes.
Child waves inherit the creating context's limit unless creation passes an
override.

## Tokens and Farm provenance

Register a token on the Farm that will publish the work. Keep it exclusive to
one active producer, pass it by reference, and unregister it. Copying
transfers ownership. Dropping a token does not unregister it. Assigning over
a still-registered token is fatal.

Each Farm has a process-unique `farmId` that is never reissued. The token
stores that id beside the slot hash. `Token.valid` is only "this copy still
holds a hash". It does not check which Farm issued it. Presenting a token to
any other Farm is fatal at `write` and `unregisterProducer`. `flush` and
`pump` catch a foreign token only when they publish something; an empty call
returns before `write`. The check is fatal in debug and release.

The slot hash is not provenance. Two Farms with the same registration history
derive the same hash. Ownership is the separate `farmId` field, not a
probabilistic check.

## Consumers and views

Subscribe one persistent `ConsumerView` per consuming thread. Ordinary copying
is disabled; it is a unique cursor, not a POD value to duplicate.
`subscribe(AntFarm*)` returns a non-negative starting epoch, or a negative
code: `SUBSCRIBE_RETRY` (call again), `SUBSCRIBE_FULL` (128 views), or
`SUBSCRIBE_INVALID` (null Farm or an already subscribed view).

`consumeNext() == false` means nothing is ready at this cursor right now. It
may be a hole. It is not global idle, not actor completion, and not a reason
to destroy a Farm. A producer thread may also own a view and consume on the
side that publishes. Worker-owned views unsubscribe on that worker, as
`examples/iota_sum.d` does in `workerStop`, before the pool join returns.

## Making work run

`wake`, `send`, and private `spawn` do not execute actors. Someone must
publish, and someone must consume.

| Call | Publishes | Does not |
| --- | --- | --- |
| `runtime.flush(token)` | up to a bounded ready snapshot (default 32, cap 256) as Farm tables | consume, retire, or validate the token when the snapshot is empty |
| `park.pump(token)` | completions collected, at most one root table, and one initial plus one resume table per pending child wave | consume. The return count is root activations only, including internal cleanup |
| `park.collectCompleted()` / `poll()` | nothing | run consumers or resume waiters. It only acquires completed tables and wakes |
| `view.consumeNext()` | nothing | prove that every park, runtime, or wave is done |

Keep the emitter pumping while private roots sit in `waitChildren`. A pump
that returns 0 can still have child work, waiting roots, or a notification
tail in flight. Stopping there hangs. Completion does not wake a sleeping
emitter; an empty wake list is not a sleep handshake. The programs in
`examples/` spin or yield under an application deadline. That deadline is not
a library facility.

Public `flush` returning 0 means the ready snapshot was empty, publication
hit Farm backpressure, or another flusher held the snapshot. Those cases are
not distinguished. Retry while work may still be admitted, and keep a consumer
running. Do not treat 0 as idle.

## Allocators, including `gcRooted`

The default `ActorAllocator.cRuntime()` uses the repository's 64-byte-aligned
C-runtime pair. It supports alignments through 64 bytes and returns null
beyond that. Public actor state is still POD: creation copies bytes and there
is no destructor. Private state may be a non-POD struct with a `nothrow @nogc`
destructor. The library moves it and destroys the stored instance once, on a
consumer (roots follow the park's reclamation policy, emitter by default).
Result type `R` stays POD.

POD does not mean pointer-free, and it does not mean the GC scans the
allocation. Actor storage is outside the ring and outside the GC unless you
opt in. `ActorAllocator.gcRooted(&inner)` registers each allocation with
`GC.addRange` and removes the range before free, after destruction. `inner`
and its context must outlive every allocation and the hierarchy. Descendants
inherit the root allocator. Registration takes the global GC lock; skip it
when state holds only manually managed resources.

Callback threads must be druntime-registered. Threadpool workers already are.
A raw native thread needs `thread_attachThis`. The emitter thread must be
registered too when state holds GC references: a root spawn argument sits on
its stack while it pumps, and default emitter reclamation frees those ranges
there.

The allocator must allow allocate on one thread and free on another. The
allocating thread is not necessarily the freeing thread. Non-thread-safe
hooks are not checked.

## Message, result, and hook storage

Public inbox nodes are caller-owned and address-stable from a successful
`send` until the actor calls `complete`, `fail(code)`, or `cancel`, or explicitly
takes over the storage. Each terminal call requires a node popped into
processing ownership and releases it to the sender. Read and write payload
fields before that call. After `available` acquire-observes completion for
this interaction, read `outcome` (`ActorNodeOutcome.completed`, `failed`, or
`cancelled`) and `failureCode` before reuse or transfer. Failure codes are
24-bit (0 through `0xFF_FFFF`); larger codes are fatal. `complete` and `cancel`
clear the code to zero, while `fail(0)` still reports failed. A reply read
before `available` is a race. A newly initialized node is available but has
not completed an interaction; `send` does not reset a previous outcome.
`initialize` is for fresh exclusive storage. Calling it on a live node
bypasses the send/complete state checks. One node belongs to one inbox.
`nodeBusy` means that node is unavailable, not that a mailbox is full. There
is no byte cap.

Private cohort results live in cohort storage, in participant order (spawn
order for a whole wave, list order for a subset). `results(context)` is valid
only inside the authorized callback, and only until the next nonempty
`start`, `startFinal`, or `startSubset`. Keeping the slice, or returning a
pointer into child state, violates the `@system` contract. Slots outlive
member retirement and die with the cohort. A result must not refer to a
resource that member's destructor releases unless the callback clears the
owning field first.

Root-to-application output is still an application buffer. Keep it alive until
the root has been collected (`live == 0`), not merely until the callback that
wrote it returned. `waitChildren` makes those external writes visible on the
resumption. It does not grant child-state access.

A wave completion hook runs after `finished` is published. Observing
`finished` does not join the hook. The hook context must outlive the hook's
last access. The hook must not destroy the `ActorWave` before it returns, and
an old hook must not touch a descriptor that has been `begin`ned again. After
the hook's last access, the orchestrator may free that context and may reuse
the wave. Reuse invalidates old handles: `finished` becomes false, not a
saved historical result. `examples/public_wave.d` is the compiled form of this
rule.

Private destructors must tolerate `.init`. A successful spawn destroys the
moved-from argument on the caller as well as the stored instance later. Do
not call park APIs, block, or read other actors from a destructor.

## What "done" means

These predicates answer different questions. None of them is "the Farm is
quiescent" or "the process may exit".

| Observation | It means | It does not mean |
| --- | --- | --- |
| Public `runtime.live == 0` | no live actor generation in that runtime | ready queue empty, consumers gone, wave hook returned |
| `owner.retired` | this generation closed and drained | reclaimed; `handle.valid` is still true on copies |
| `ActorHandle.valid` | the copy holds a non-null slot and a non-zero generation | the generation is still live. After reclaim, `wake`/`send` return `staleHandle` |
| `node.available` | for an accepted interaction, the actor released `complete`/`fail`/`cancel`; outcome, code and payload writes are visible and the node may be reused | the actor has retired, or the runtime is quiet |
| `ActorWaveHandle.finished` and `!failed` | this generation completed; actor writes in the wave are visible. `failed` is structural or control-path interference, not a domain error, and it does not roll back earlier tables | the completion hook has returned. After `begin`, the old handle's `finished` is false |
| Private `park.live == 0` | the emitter has collected every root, including waiters and notification tails, and on a tracked park every outcome was taken | consumers unsubscribed, token unregistered, or `pump() == 0` along the way |
| Private `takeOutcome` returned a root | that root is terminal with this status; its application writes are visible on the emitter thread; its handle is stale | other roots are done, or another thread may read those writes without synchronizing with the emitter |
| `requestCancel() == requested` | the root's outcome will be `cancelled` or `failed` | the root has observed it or stopped working |
| Public `requestRetire(ActorRetireMode.cancel) == requested` | admission is closed; every later handler activation of this generation sees `context.cancelling` | any node was cancelled, or that a handler will run again: an idle or empty actor can retire with no call |
| Private `finished(context)` | the current child wave is idle. True before the first start, and true while a root notification tail may still hold `live` | safe to destroy the park, or authority to read child state |
| `consumeNext() == false` | this cursor sees nothing ready | any actor, wave, or park is done |
| `pump() == 0` | that call published no root activation | no child work, no waiting root, idle |
| `flush() == 0` | that call published no ready actor | the service is finished |

User callback counters are not a destruction fence. A root can be logically
finished and still count in `live` until its notification tail is collected.
`spawn` can return `capacityFull` for that slot until then. On a tracked park
a terminal root also counts in `live` until its outcome is taken.

## Shutdown order

`destroy` checks preconditions and frees. It is not graceful shutdown. Keep
publication and consumption available until the logical drain is finished.
A consumer may still be inside the actor trampoline after reclaim or
`live == 0`, so join workers and unsubscribe every view before destroying a
runtime or park. Unregistering an already-quiescent producer and unsubscribing
an already-quiescent consumer may happen in either order, but both must finish
before the Farm is destroyed. The examples stop consumers, destroy the runtime
or park, unregister, then destroy the Farm.

1. Stop admitting new application work. For a public service, stop senders,
   then `requestRetire`. Retirement closes admission. The default drain lets
   handlers process accepted nodes; `requestRetire(ActorRetireMode.cancel)`
   makes `context.cancelling` true so handlers call `context.cancelInbox()`,
   and senders see `ActorNodeOutcome.cancelled`. A drain can be escalated
   later with a cancel-mode request on the same owner, for example after an
   application deadline. Requests on one owner must not overlap. For a
   private tree, stop spawning new roots.
2. Keep flushing or pumping, and keep consumers running, until the model-specific
   drain holds: public owners `retired` and reclaimed; waves `finished` and,
   if a hook was set, the hook's last access observed; private `live == 0`
   reached through `pump` or `collectCompleted`. A tracked park reaches it
   only if the loop also drains `takeOutcome`; to cancel instead of drain,
   call `cancelAll()` first, then run the same loop. Idle public retirement can
   complete without another handler call, in either mode. `context.closing`
   and `context.cancelling` are not guaranteed finalizers; release resources
   reachable from public state after `retired`. Do not destroy the runtime or park in this step.
3. Stop and join workers. Their unsubscribe must happen before the join
   returns. Unsubscribe any control-thread view the same way.
4. Destroy actor infrastructure only after that join. `runtime.destroy`
   fatals if actors are live or the ready queue is non-empty.
   `park.destroy()` fatals if roots are live, including untaken outcomes;
   then set the pointer null. Join every thread that may call
   `requestCancel` on its handles first.
   `destroy(park)` is D's pointer destruction and does not free the park.
5. Unregister producer tokens.
6. Destroy the Farm. Live consumers or live tickets are fatal.

A public endpoint may front a private hierarchy. Drain the private park to
`live == 0` and reclaim the public owner while consumers are still running,
then join those consumers before destroying either runtime or park. Neither
count covers the other.

The example host's deadline is application policy for a stuck drain. The
library will not cancel a callback that never returns, a root that always
returns `again`, or an inbox that is never popped.

## Ranked misuse hazards

Guards below are fatal in debug and release unless the row says otherwise.
"Silent" means the library does not diagnose it: the usual result is a hang,
a race, or a use of freed memory.

| Rank | Hazard | What is caught | What is silent |
| --- | --- | --- | --- |
| 1 | Lifetime and aliasing across callbacks, messages, outputs, and runtime storage. Escaped state or context pointers, freeing a node or hook context too early, dangling GC references, handles used after runtime or park destruction. | `complete` on a node the actor does not own. Generation checks reject stale submits while that runtime's slot slab is alive. | The compiler does not prove these contracts. `@system` code can stash a `scope` pointer; DIP1000 does not make that a compile error today. Generation checks do not cover a destroyed slot slab. `initialize` on a live node skips the send/complete checks. |
| 2 | Progress and shutdown. Stop the emitter too soon, block every consumer while waiting for child work, treat node completion or `live == 0` as infrastructure quiescence, or free hook context after `finished`. | Destroying a Farm with live consumers or tickets, a runtime with live or queued actors, or a park with live roots. | A lost pump or a missing consumer hangs. `pump() == 0`, `consumeNext() == false`, `finished`, and `closing` do not mean what a shutdown loop wants. |
| 3 | Token used on the wrong Farm. | Fatal at `write` and `unregisterProducer`. `flush` and `pump` catch it only when they publish something. A since-destroyed Farm's id is never reissued, so a leftover ticket cannot match a new Farm. | `Token.valid` does not report provenance. An empty `flush` or `pump` returns before `write`. The slot hash matches across Farms with the same registration history; only `farmId` distinguishes them. |
| 4 | Assuming thread, allocator, or state properties the API does not enforce. A non-thread-safe allocator, thread-affine data in state that migrates across consumers, concurrent use of one private park, re-entering a park from a callback. | Alignment above 64 on the default allocator fails the allocation (null), not a later checked fault. Unauthorized private contexts, active-child `finish`, bad batch overrides, and subset index errors are fatal. Park reentry is fatal only in assertion builds. | POD checks are not transitive ownership. The reentry guard is not a lock. Release builds do not diagnose park reentry. |
| 5 | Overload and nontermination left as application policy. Unbounded inbox growth, an endless `again` or empty wait, a callback that never returns, a child that never finishes. | Ring, runtime capacity, and private cohort capacity are finite. Exhaustion is a typed status or backpressure, not an automatic cancel. | No mailbox byte cap or public deadline. Private cancellation is cooperative: a callback that ignores `cancelled` runs to completion. Private deadlines are lazy, observed only by checks and emission, and need a tracked park, as do root handles and `cancelAll`. An actor that never pops its inbox can refuse retirement forever, even in cancel mode: the library never discards nodes. The example host deadline is not part of the library. |

Other mistakes that fail late or not at all: dropping an owner or token skips
cleanup; a type-erased create checks size and alignment, not the semantic
type; a saved private context has no generation, so comparing its parent
pointer does not prove the callback is still running; moving or destroying
active wave storage invalidates embedded self-pointers even though ordinary
copying is disabled.

## Choosing public or private actors

Use a public actor when outsiders hold a stable submission handle: a service
with an inbox, several event sources, or actors an external orchestrator
selects into an `ActorWave`. The owner retires and reclaims; handles do not
extend state lifetime. Waves are how external code names the set.

Use a private hierarchy when one conductor owns the tree. There is no public
handle, no inbox, and no way for a sibling or the emitter to read child state.
The parent authorizes whole-cohort or subset waves, children can fold
descendant results inside one enclosing wave, and the root joins with
`waitChildren` instead of polling. That is the cheaper path for an exclusive
computation tree.

Do not merge the two into one handle type. A public service can front a
private tree by writing an external result buffer or channel. Callers of the
service never receive identities for the internal actors.

| Need | Public | Private |
| --- | --- | --- |
| Who names the actor | external handle | no external identity; parent owns the cohort |
| How work is admitted | `wake` / `send`, or an external wave | `spawn`, then `start` / `startSubset` / `startFinal` |
| How the caller sees a result | caller-owned node or other external buffer, after an acquire | cohort `results(context)` after the join; root output still external, visible on the emitter after `takeOutcome` on a tracked park |
| Domain failure | `node.fail(code)` and `ActorNodeOutcome` after `available`; public wave failures remain structural | `context.fail(code)`, then `outcome` after the join. A tracked root's `fail` becomes `PrivateRootOutcome.failed`; untracked root `fail` is fatal |
| Cancellation | cooperative: `requestRetire(ActorRetireMode.cancel)`, or escalation from drain, closes admission and sets `context.cancelling`; the handler calls `cancelInbox()`. No library discard, no callback interruption, no final call guaranteed | cooperative and in-tree: `park.cancel`, `cancelChildren`, `cancelPeers`, observed through `context.cancelled`. Tracked parks add thread-safe `PrivateRootHandle.requestCancel`, `cancelAll` and lazy deadlines. Skips undispatched tables; never interrupts a callback or wakes a waiter early |
| How to wait | poll a wave handle, a node, or `retired` | `waitChildren` on a root; `PrivateChildStep!R` inside a child wave |
| Shutdown fence | retire, reclaim, empty ready queue | `live == 0` through pump or collect (and `takeOutcome` when tracked), then `park.destroy()` |

## Where to go next

- [examples/public_service.d](../examples/public_service.d) — inbox statuses, bounded drain, close, stale handle.
- [examples/public_wave.d](../examples/public_wave.d) — external selection and the hook tail.
- [examples/private_tree.d](../examples/private_tree.d) — ports, continuation fold, subset, destructor, `live == 0`.
- [examples/README.md](../examples/README.md) — Windows and Linux build commands. `make check` runs the Linux matrix.
