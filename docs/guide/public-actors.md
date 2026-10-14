# Public actors

The `actors` package adds explicit state lifetime and exclusive
callback-local borrowing without changing the worker side: ordinary Farm
consumers execute actor activations and wave tables. The shared contracts are
in [../reference/actor-lifecycle.md](../reference/actor-lifecycle.md); this
page shows how to use the public API. Complete programs are under
[../../examples/](../../examples/README.md).

An `ActorOwner` controls retirement and reclamation, while its copyable
`ActorHandle` is the identity used to wake, send to, or phase-dispatch the
actor. Owners (and producer `Token`s) are move-only: copying one, assigning
an lvalue, or passing one to a by-value parameter does not compile, because
`owners.all!(o => o.retired)` taking elements by value would silently
invalidate `owners[0]`. Transfer ownership with `move` (std.algorithm or
core.lifetime), lend with a `ref` parameter, and iterate owner arrays with
`foreach (ref o; owners)`. State is a separate allocation, POD for public
actors, owned through an `ActorAllocator`.

```d
import actors;

struct Body { ulong position; }

void onWake(scope ref ActorBorrow!Body actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ++actor.value.position;
    // context.republish(); // request another autonomous activation
}

auto runtime = ActorRuntime.create(farm, 1024);
auto owner = runtime.createActor!(Body, onWake)(Body.init);
auto handle = owner.handle;

handle.wake();                 // coalescing autonomous activation
runtime.flush(token, 32);      // publish queued actors into the Farm
```

The handler must declare both `ActorBorrow!T` and `ActorContext` as
`scope ref`; actor creation rejects an unqualified handler. The borrow is valid
only for the dynamic extent of the callback. `wake`, `send`, and republish do
not execute the actor: someone must publish with `runtime.flush` and someone
must consume. `flush(token, limit)` publishes up to a bounded ready snapshot
(default 32, cap 256) as Farm tables; a `0` return is empty, backpressured, or
contended, and those are not distinguished.

Binding the borrowed state to a reference needs the call form:
`ref T s = actor.value;` does not compile, while `ref Body s =
actor.value();` and `auto p = &actor.value();` do (verified on DMD and
LDC). Reading and mutating through `actor.value.field` works as written
above. Actor state is copied into allocator-owned storage the garbage
collector does not scan: a POD state that contains GC slices or pointers
compiles, but those allocations are not kept alive by the actor. Keep them
reachable elsewhere for the actor's lifetime, or store non-GC memory
(malloc, arenas, the allocator adapters) in actor state.

The compiled form of this path is
[../../examples/public_service.d](../../examples/public_service.d):

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=/tmp/public_service
ANTFARM_HUGE_PAGES=0 /tmp/public_service 0
```

## Inbox

The built-in inbox is an accumulator-style interaction protocol. Any producer
may `initialize` freshly allocated storage and `send` the node; the actor is
the only consumer. A successful send owns the node until the actor calls
`complete`, `fail(code)`, or `cancel`, or consumes its storage. A wake is only
a coalescing notification, not one unit of work. Read the payload through
`node.payloadAs!Request` (a plain cast of `payload` to `Request*`) instead of
a hand-written cast.

For request/reply, `completeAndWake(sender)`, `failAndWake(sender, code)`
and `cancelAndWake(sender)` complete the node and then schedule the sender,
returning the `ActorWakeResult`. Without them the receiver had to call
`sender.wake()` by hand, and a missing wake hung the sender silently. The
sender handle travels in the payload (any `ActorHandle!U` or the erased
handle is accepted); the node does not store a reply target. The completion
store happens before the wake, so the woken sender observes `available` and
the recorded outcome; do not touch the node after the call.

Read and write payload fields before the terminal call. After `available`
acquire-observes completion for this interaction, read `outcome`
(`ActorNodeOutcome.completed`, `failed`, or `cancelled`) and `failureCode`
before reuse or transfer. Failure codes are 24-bit (0 through `0xFF_FFFF`);
larger codes are fatal. A reply read before `available` is a race.
[../../examples/public_service.d](../../examples/public_service.d) exercises
queued/coalesced acceptance, rejection, a two-node drain, replies after
`available`, node reuse, admission close, idle retirement, and stale handles.

`send` returns `nodeBusy` when that node is unavailable, not when the mailbox
is full; there is no byte cap. The intrusive node and payload stay
caller-allocated, so malloc, slab, pool, and arena storage are equally usable.

## Waves

An `ActorWave` dispatches one phase operation across a caller-selected
collection of idle actor handles. Handles passed to one `publish` must match
that operation's state type, and every handle must have been created on the same
Farm. They need not come from the same `ActorRuntime`. The wave may span several
physical Farm tables but exposes one generation-tagged completion.

```d
void movement(scope ref ActorBorrow!Body actor) nothrow @nogc @system
{
    actor.value.position += 4;
}

ActorWave wave;
wave.begin(farm);
size_t offset;
while (offset < handles.length)
{
    immutable written = wave.publish!movement(handles[offset .. $], token);
    if (written == 0)
    {
        if (wave.handle.failed) break; // stale, duplicate, active, wrong Farm
        drainOrYield();                // Farm backpressure
        continue;
    }
    offset += written;
}
auto completion = wave.seal();
while (!completion.finished)
    drainOrYield();
```

Each successful `publish` creates one physical table and may accept only a
prefix. The operation and any module code it reaches must remain resident until
the completion handle finishes. The wave operation receives only the exclusive
borrow, not `ActorContext`; republish, inbox draining, and self-retirement stay
in the autonomous actor mode.

Observing `completion.finished` with acquire semantics and confirming
`!completion.failed` joins the actors' writes and is the boundary after which an
orchestrator may publish a dependent wave. It does not join an optional
completion hook: do not reuse that wave storage or free the hook context until
the hook's last access, and an old hook must not touch a descriptor that has
been `begin`ned again. `failed` means structural admission rejection (stale,
non-idle, duplicate, or wrong-Farm handles), not a domain error; it does not
roll back earlier tables.

`completion.interrupted` is a separate sticky diagnostic: a wave operation
completed but observed a concurrent wake, send, or retirement request and handed
it to autonomous dispatch after its borrow ended. Such a race does not set
`failed`. Both flags can be set if another table's admission fails. `finished`
joins the wave operations, not the resulting autonomous work; a dependent wave
still requires idle members. Requests arriving after an operation's exit
transition need not set `interrupted`.

Actors in the same wave run in parallel and must not consume one another's
intermediate writes. For cross-actor reads, keep private mutable actor state
plus caller-owned public projections; write the inactive public buffer during a
phase and select it only after successful wave completion. The actor package
supplies the exclusive borrow and completion boundary, not a prescribed
projection layout. The projection model and its commit rules are in
[../design/public-actor-ordering.md](../design/public-actor-ordering.md).

An `ActorWaveTrigger` can park a Fiber until that boundary instead of polling;
see [../../fibers/README.md](../../fibers/README.md). The compiled form is
[../../examples/public_wave.d](../../examples/public_wave.d).

## Retirement and reclamation

Stop senders first, then `owner.requestRetire()` (the default is drain). Late
sends return `closed`, and a request during a wave returns `waveOwned` and must
be retried after the wave. To escalate a drain, call
`owner.requestRetire(ActorRetireMode.cancel)` on the same owner; admission
closes and every later activation of that generation sees
`context.cancelling`, so the handler calls `context.cancelInbox()` and senders
observe `ActorNodeOutcome.cancelled`. Cooperative retirement guarantees no
final handler call: an idle or empty actor can retire with no activation, so
release resources reachable from public state after `retired`, not from a
hoped-for last callback.

Keep flushing and running consumers until `owner.retired`, then
`owner.reclaim()` (retry on `busy`). Join every worker and unsubscribe every
view before `runtime.destroy()`, which fatals if actors are live or the ready
queue is non-empty. The full ordering is the shared
[shutdown recipe](../reference/actor-lifecycle.md#shutdown-recipe).

## Resident type-erased adapter

A resident engine can pass `runtime.erasedAdapter` across its
reloadable-module boundary. The adapter carries opaque runtime context and a
resident creation function pointer, so creation uses a non-template boundary
and returns ownership whose layout does not depend on the module's `State`:

```d
void moduleActor(scope ref ActorErasedBorrow actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ref State state = actor.value!State();
    // Mutate state and process interactions.
}

State initial;
ActorErasedAdapter adapter = runtime.erasedAdapter;
ActorErasedOwner owner = adapter.createActor(&initial,
    State.sizeof, State.alignof, &moduleActor);
ActorErasedHandle handle = owner.handle;
```

`ActorErasedBorrow.value!State()` checks the recorded size and alignment before
returning the callback-local reference. The erased creation call remains an
`@system` POD byte-copy contract: it cannot inspect a type that has already been
erased, run a destructor, or make GC references safe in unscanned memory. The
adapter is non-owning and must not outlive its runtime.

The dispatch pointer can still name reloadable module code. Type erasure makes
the resident registry and creation/reclamation path independent of `State`; it
does not itself authorize unloading that code. The engine must keep the module
generation resident until its erased owners have retired and been reclaimed. A
typed owner can transfer into the same registry representation with
`intoErased()`, while `ActorHandle!T.erased` makes a non-owning erased copy.

## Where to go next

- The exact authority, completion, storage and shutdown rules:
  [../reference/actor-lifecycle.md](../reference/actor-lifecycle.md).
- Why activation and wave completion order the way they do:
  [../design/public-actor-ordering.md](../design/public-actor-ordering.md).
- Allocator choice and the optional mimalloc adapter:
  [sizing-and-allocation.md](sizing-and-allocation.md).
- Runnable programs: [../../examples/README.md](../../examples/README.md).
