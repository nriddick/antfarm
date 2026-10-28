# Actor lifecycle and contracts

This is the shared contract for public actors (`ActorRuntime`, inboxes,
`ActorWave`) and private actors (`PrivateRootPark` and parent-owned cohorts).
The Farm distributes work. It does not create workers, and it does not decide
that an application is finished. Actors add execution and lifetime rules above
that distributor. Neither `ActorRuntime.create` nor `PrivateRootPark.create`
starts a thread.

Complete programs are under [../../examples/README.md](../../examples/README.md).
The public guide is [../guide/public-actors.md](../guide/public-actors.md); the
private guide is [../guide/private-actors.md](../guide/private-actors.md).
Proof detail is in the design pages
[../design/public-actor-ordering.md](../design/public-actor-ordering.md) and
[../design/private-actor-ordering.md](../design/private-actor-ordering.md).
Sizing numbers and allocator rules are in
[../guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md).

## Tokens and Farm provenance

Register a token on the Farm that will publish the work. Keep it exclusive to
one active producer, pass it by reference, and unregister it. Tokens are
move-only: copying does not compile; transfer one explicitly with `move`.
Dropping a token does not unregister it. Assigning over a
still-registered token is fatal.

Each Farm has a process-unique `farmId` that is never reissued. The token stores
that id beside the slot hash. `Token.valid` is only "this copy still holds a
hash". It does not check which Farm issued it. Presenting a token to any other
Farm is fatal at `write` and `unregisterProducer`. `flush` and `pump` catch a
foreign token only when they publish something; an empty call returns before
`write`. The check is fatal in debug and release.

The slot hash is not provenance. Two Farms with the same registration history
derive the same hash. Ownership is the separate `farmId` field, not a
probabilistic check; `Token.issuer` reads it, and `FarmHost.unregisterProducer`
uses it to return a token to the Farm that issued it.

A runtime or park is bound to one Farm at creation. On a `FarmHost` with
`farmPerGroup` that is the Farm of the group it was created on, and the host
flushes or pumps it with that Farm's control token (`tokenOf(llc)`); a
handle may be woken from any thread, and the activation still arrives
through its runtime's Farm.

## Consumers and views

Subscribe one persistent `ConsumerView` per consuming thread. Ordinary copying
is disabled; it is a unique cursor, not a POD value to duplicate.
`subscribe(AntFarm*)` returns a non-negative starting epoch, or a negative code:
`SUBSCRIBE_FULL` (128 views) or `SUBSCRIBE_INVALID` (null Farm or an already
subscribed view). It retries the transient `SUBSCRIBE_RETRY` outcome (the write tail outran an attempt's rounds) itself;
`subscribeOnce` is the single attempt that reports it as `SUBSCRIBE_RETRY`.

`consumeNext() == false` means nothing is ready at this cursor right now. It may
be a hole. It is not global idle, not actor completion, and not a reason to
destroy a Farm. A producer thread may also own a view and consume on the side
that publishes. Worker-owned views unsubscribe on that worker, as
`examples/iota_sum.d` does in `workerStop`, before the pool join returns.

## Making work run

`wake`, `send`, and private `spawn` do not execute actors. Someone must publish,
and someone must consume.

| Call | Publishes | Does not |
| --- | --- | --- |
| `runtime.flush(token)` | up to a bounded ready snapshot (default 32, cap 256) as Farm tables | consume, retire, or validate the token when the snapshot is empty |
| `park.pump(token)` | completions collected, at most one root table, and one initial plus one resume table per pending child wave | consume. The return count is root activations only, including internal cleanup |
| `park.collectCompleted()` / `poll()` | nothing | run consumers or resume waiters. It only acquires completed tables and wakes |
| `view.consumeNext()` | nothing | prove that every park, runtime, or wave is done |

Keep the emitter pumping while private roots sit in `waitChildren`. A pump that
returns 0 can still have child work, waiting roots, or a notification tail in
flight. Stopping there hangs. Completion does not wake a sleeping emitter; an
empty wake list is not a sleep handshake. The programs in `examples/` step a
`FarmHost` in `waitUntil` under an opt-in deadline. That deadline is not an
actor-library facility.

Public `flush` returning 0 means the ready snapshot was empty, publication hit
Farm backpressure, or another flusher held the snapshot. Those cases are not
distinguished. Retry while work may still be admitted, and keep a consumer
running. Do not treat 0 as idle.

## Message, result, and hook storage

Public inbox nodes are caller-owned and address-stable from a successful `send`
until the actor calls `complete`, `fail(code)`, or `cancel`, or explicitly takes
over the storage. Each terminal call requires a node popped into processing
ownership and releases it to the sender. Read and write payload fields before
that call. After `available` acquire-observes completion for this interaction,
read `outcome` (`ActorNodeOutcome.completed`, `failed`, or `cancelled`) and
`failureCode` before reuse or transfer. Failure codes are 24-bit (0 through
`0xFF_FFFF`); larger codes are fatal. `complete` and `cancel` clear the code to
zero, while `fail(0)` still reports failed. A reply read before `available` is a
race. A newly initialized node is available but has not completed an
interaction; `send` does not reset a previous outcome. `initialize` is for fresh
exclusive storage. Calling it on a live node bypasses the send/complete state
checks. One node belongs to one inbox. `nodeBusy` means that node is
unavailable, not that a mailbox is full. There is no byte cap.

Private cohort results live in cohort storage, in participant order (spawn order
for a whole wave, list order for a subset). `results(context)` is valid only
inside the authorized callback, and only until the next nonempty `start`,
`startFinal`, or `startSubset`. Keeping the slice, or returning a pointer into
child state, violates the `@system` contract. Slots outlive member retirement
and die with the cohort. A result must not refer to a resource that member's
destructor releases unless the callback clears the owning field first.

Root-to-application output is still an application buffer. Keep it alive until
the root has been collected (`live == 0`), not merely until the callback that
wrote it returned. `waitChildren` makes those external writes visible on the
resumption. It does not grant child-state access.

A wave completion hook runs after `finished` is published. Observing `finished`
does not join the hook. The hook context must outlive the hook's last access. The
hook must not destroy the `ActorWave` before it returns, and an old hook must not
touch a descriptor that has been `begin`ned again. After the hook's last access,
the orchestrator may free that context and may reuse the wave. Reuse invalidates
old handles: `finished` becomes false, not a saved historical result.
[../../examples/public_wave.d](../../examples/public_wave.d) is the compiled
form of this rule.

Private destructors must tolerate `.init`. A successful spawn destroys the
moved-from argument on the caller as well as the stored instance later. Do not
call park APIs, block, or read other actors from a destructor.

## What "done" means

These predicates answer different questions. None of them is "the Farm is
quiescent" or "the process may exit".

| Observation | It means | It does not mean |
| --- | --- | --- |
| Public `runtime.live == 0` | no occupied slots: construction reservations and unreclaimed generations both count | final free-slot gate released, ready queue empty, consumers gone, wave hook returned |
| `owner.retired` | this generation closed and drained | reclaimed; `handle.valid` is still true on copies |
| `ActorHandle.valid` | the copy holds a non-null slot and a non-zero generation | the generation is still live. After reclaim, `wake`/`send` return `staleHandle` |
| `node.available` | for an accepted interaction, the actor released `complete`/`fail`/`cancel`; outcome, code and payload writes are visible and the node may be reused | the actor has retired, or the runtime is quiet |
| `ActorWaveHandle.finished` and `!failed` | this generation completed; actor writes in the wave are visible. `failed` is structural admission rejection, not a domain error; `interrupted` separately reports observed control-path interference. Neither rolls back earlier tables | the completion hook or handed-off autonomous work has finished. After `begin`, the old handle's `finished` is false |
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
`spawn` can return `capacityFull` for that slot until then. On a tracked park a
terminal root also counts in `live` until its outcome is taken.

## Shutdown recipe

Shutdown is application-driven: close admission, finish the logical drain or
cancel, then quiesce and destroy. The table is the recipe in both models;
[Shutdown order](#shutdown-order) below explains the ordering constraints.

The compiled forms are
[../../examples/request_deadline.d](../../examples/request_deadline.d) (private
drain and `takeOutcome`) and
[../../examples/public_service.d](../../examples/public_service.d) (public
retire and reclaim).

| Step | Public actor service | Private root park |
| --- | --- | --- |
| 1. Close admission | Stop senders, then `owner.requestRetire()` (the default is drain). Late `send`s return `closed`; a request during a wave returns `waveOwned` and must be retried after the wave. | Stop `spawn`ing roots. |
| 2. Drain or cancel | Keep calling `runtime.flush` and running consumers until `owner.retired`, then `owner.reclaim()` (retry on `busy`). Handlers process accepted nodes. To escalate a drain, call `owner.requestRetire(ActorRetireMode.cancel)` on the same owner after an application deadline; every later activation of that generation sees `context.cancelling`, so the handler calls `context.cancelInbox()` and senders observe `ActorNodeOutcome.cancelled`. | Drain with `pump`/`collectCompleted` and consumer progress to `live == 0`. To cancel instead, call `cancelAll()` first (tracked parks), then run the same loop. A tracked park must also call `takeOutcome` every pass: a terminal root stays in `live` until its outcome is taken. |
| 3. Join workers | Join every worker and unsubscribe every view, so no consumer can still be in the actor trampoline. | Same, and also join every thread that may call `handle.requestCancel` on its handles. |
| 4. Destroy infrastructure | `runtime.destroy()` only after every owner is reclaimed (`runtime.live == 0`) and the consumer join. | `park.destroy()` only after `live == 0` and the canceller join; then `park = null`. |
| 5. Unregister and destroy the Farm | Unregister producer tokens, then `farm.destroy()`. | Same. |

There is no guaranteed final handler call in either retire mode. An idle or
empty actor retires without another activation, so release resources reachable
from public state after `retired`, not from a hoped-for last callback. On the
private side, a cancelled callback is only *asked* to stop: cancellation never
interrupts it.

## Shutdown order

`destroy` checks preconditions and frees. It is not graceful shutdown. Keep
publication and consumption available until the logical drain is finished. A
consumer may still be inside the actor trampoline after reclaim or `live == 0`,
so join workers and unsubscribe every view before destroying a runtime or park.
Unregistering an already-quiescent producer and unsubscribing an already-
quiescent consumer may happen in either order, but both must finish before the
Farm is destroyed. `FarmHost.close()` (`threadpool.host`), which the examples
use, performs this order: it stops consumers, destroys the runtimes and parks,
unregisters, then destroys the Farm.

1. Stop admitting new application work. For a public service, stop senders, then
   `requestRetire`. Retirement closes admission. The default drain lets handlers
   process accepted nodes; `requestRetire(ActorRetireMode.cancel)` makes
   `context.cancelling` true so handlers call `context.cancelInbox()`, and
   senders see `ActorNodeOutcome.cancelled`. A drain can be escalated later with
   a cancel-mode request on the same owner, for example after an application
   deadline. A drain request on an actor already cancelling returns `requested`
   and leaves it in cancel mode: drain never downgrades cancel. Requests through one D owner remain serialized. Distinct copied C owners may
   contend; a retirement-operation pin returns retryable busy, and exclusive
   reclamation prevents duplicate destruction. See [the C contract](../design/c-abi.md#value-owners-and-retirement-arbitration). For a private tree, stop spawning new roots.
2. Keep flushing or pumping, and keep consumers running, until the model-specific
   drain holds: public owners `retired` and reclaimed; waves `finished` and, if a
   hook was set, the hook's last access observed; private `live == 0` reached
   through `pump` or `collectCompleted`. A tracked park reaches it only if the
   loop also drains `takeOutcome`; to cancel instead of drain, call `cancelAll()`
   first, then run the same loop. Idle public retirement can complete without
   another handler call, in either mode. `context.closing` and
   `context.cancelling` are not guaranteed finalizers; release resources
   reachable from public state after `retired`. Do not destroy the runtime or
   park in this step.
3. Stop and join workers. Their unsubscribe must happen before the join returns.
   Unsubscribe any control-thread view the same way.
4. Destroy actor infrastructure only after that join. `runtime.destroy` fatals if
   actors or construction reservations are live, or the ready queue is non-empty. `park.destroy()` fatals if
   roots are live, including untaken outcomes; then set the pointer null. Join
   every thread that may call `requestCancel` on its handles first.
   `destroy(park)` is D's pointer destruction and does not free the park.
5. Unregister producer tokens.
6. Destroy the Farm. Live consumers or live tickets are fatal.

A public endpoint may front a private hierarchy. Drain the private park to
`live == 0` and reclaim the public owner while consumers are still running, then
join those consumers before destroying either runtime or park. Neither count
covers the other.

`FarmHost.waitUntil` takes an opt-in timeout; that deadline is application
policy for a stuck drain. The library will not cancel a callback that never
returns, a root that always returns `again`, or an inbox that is never popped.

### With Fibers on the host

For a full stack, seal admitted actor waves and let orchestrating Fibers observe
completion, including any completion hook's last access. Signal or
cooperatively cancel indefinite Fiber waiters, drain runnable work and
retire completion and lifecycle records (`domain.retire()`) before closing
the host; its `close()` stops the workers, runs the attachments' `uninstall`
and then destroys the Farms. The application must bound shutdown of user code that never reaches a
scheduler boundary.

## Ranked misuse hazards

Guards below are fatal in debug and release unless the row says otherwise.
"Silent" means the library does not diagnose it: the usual result is a hang, a
race, or a use of freed memory.

| Rank | Hazard | What is caught | What is silent |
| --- | --- | --- | --- |
| 1 | Lifetime and aliasing across callbacks, messages, outputs, and runtime storage. Escaped state or context pointers, freeing a node or hook context too early, dangling GC references, handles used after runtime or park destruction. | `complete` on a node the actor does not own. Generation checks reject stale submits while that runtime's slot slab is alive. | The compiler does not prove these contracts. `@system` code can stash a `scope` pointer; DIP1000 does not make that a compile error today. Generation checks do not cover a destroyed slot slab. `initialize` on a live node skips the send/complete checks. |
| 2 | Progress and shutdown. Stop the emitter too soon, block every consumer while waiting for child work, treat node completion or `live == 0` as infrastructure quiescence, or free hook context after `finished`. | Destroying a Farm with live consumers or tickets, a runtime with live or queued actors, or a park with live roots. | A lost pump or a missing consumer hangs. `pump() == 0`, `consumeNext() == false`, `finished`, and `closing` do not mean what a shutdown loop wants. |
| 3 | Token used on the wrong Farm. | Fatal at `write` and `unregisterProducer`. `flush` and `pump` catch it only when they publish something. A since-destroyed Farm's id is never reissued, so a leftover ticket cannot match a new Farm. | `Token.valid` does not report provenance. An empty `flush` or `pump` returns before `write`. The slot hash matches across Farms with the same registration history; only `farmId` distinguishes them. |
| 4 | Assuming thread, allocator, or state properties the API does not enforce. A non-thread-safe allocator, thread-affine data in state that migrates across consumers, concurrent use of one private park, re-entering a park from a callback. | Alignment above 64 on the default allocator fails the allocation (null), not a later checked fault. Unauthorized private contexts, active-child `finish`, bad batch overrides, and subset index errors are fatal. Park reentry is fatal only in assertion builds. | POD checks are not transitive ownership. The reentry guard is not a lock. Release builds do not diagnose park reentry. |
| 5 | Overload and nontermination left as application policy. Unbounded inbox growth, an endless `again` or empty wait, a callback that never returns, a child that never finishes. | Ring, runtime capacity, and private cohort capacity are finite. Exhaustion is a typed status or backpressure, not an automatic cancel. | No mailbox byte cap or public deadline. Private cancellation is cooperative: a callback that ignores `cancelled` runs to completion. Private deadlines are lazy, observed only by checks and emission, and need a tracked park, as do root handles and `cancelAll`. An actor that never pops its inbox can refuse retirement forever, even in cancel mode: the library never discards nodes. `FarmHost.waitUntil` takes an opt-in timeout; the actor library has none. |

Other mistakes that fail late or not at all: dropping a live `ActorOwner`
skips cleanup and leaks its slot; assertion builds print a "dropped without
reclaim" diagnostic on stderr (no abort, so it is safe while an exception
unwinds), release builds stay silent, and `leak()` is the deliberate opt-out
for abandoned or failed hosts. Dropping a token, or an `ActorErasedOwner`,
skips cleanup silently; a type-erased create checks size and alignment, not
the semantic type; a saved private context has no generation, so comparing its
parent pointer does not prove the callback is still running; moving or
destroying active wave storage invalidates embedded self-pointers even though
ordinary copying is disabled.

## Where to go next

- [../guide/public-actors.md](../guide/public-actors.md) and
  [../guide/private-actors.md](../guide/private-actors.md).
- [../guide/outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md)
  for the failure, deadline and cancellation recipes.
- [../guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md) for
  quotas, batches and allocators.
