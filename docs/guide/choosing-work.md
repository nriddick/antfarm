# Choosing a work representation

Ant Farm has one distributor and four ways to put work on it. Start with the
narrowest representation that fits; each layer adds state lifetime and
scheduling rules above the payload path.

An input element does not automatically need an actor. Use actors when their
identity, state lifetime or tree joins serve the application; independent
stateless cells can be payload entries. If one actor per element is required,
keep that representation while tuning [child batches and host emission bursts](hosting.md#cheap-work-with-parked-workers).

| Your work is... | Use | Example |
| --- | --- | --- |
| short, `nothrow @nogc`, no identity | a bare **payload** | [iota_sum.d](../../examples/iota_sum.d) |
| a stable identity that outside code addresses: a service with an inbox, several event sources | a **public actor** (`ActorOwner` / `ActorHandle`) | [public_service.d](../../examples/public_service.d) |
| a phase run over an externally selected set of identities | a public **actor wave** | [public_wave.d](../../examples/public_wave.d) |
| one exclusive computation tree under a single conductor, with typed joined results or non-POD state | a **private actor** hierarchy | [private_tree.d](../../examples/private_tree.d) |
| per-request tracked roots with deadlines and cancellation | private actors with `tracking` | [request_deadline.d](../../examples/request_deadline.d) |
| ordinary D exceptions, allocation, timers, or joins while waiting | a managed **Fiber** | [ant_fibers/README.md](../../ant_fibers/README.md) |

A bare payload is the lowest-overhead path. A public actor adds explicit
non-GC state lifetime and a submit surface. A wave adds a bulk-synchronous
phase boundary. A private hierarchy removes public admission machinery and
gives one conductor the whole tree. A Fiber is the only representation that
may suspend mid-work; payload and actor callbacks must return.

A public endpoint may front a private tree: write an external result buffer or
channel at the boundary. Their lifetimes still drain separately, and callers of
the service never receive identities for the internal actors.

A payload runs once on one consumer; publish separate grains for independent iterations. Autonomous
actor handlers receive an exclusive callback-local borrow and may request
republishing through `ActorContext`; wave operations receive only the borrow,
and the same actor cannot belong to overlapping unfinished waves. Fibers add
`scope(exit)`, application signals, and cooperative cancellation; a Fiber that
never suspends is usually better represented as a payload.

For independent range work, [parallel loops](parallel-loops.md) provide
ST grains, asynchronous completion hooks and a blocking foreach
adapter. Raw ranges also compose into caller-owned payload batches. Use a
private tree when the computation needs structured nested joins.

The Farm rules are in [SPEC.md](../../SPEC.md).

## Public or private actors

Use a **public** actor when outsiders hold a stable submission handle: a
service with an inbox, several event sources, or actors an external
orchestrator selects into an `ActorWave`. The owner retires and reclaims;
handles do not extend state lifetime. Waves are how external code names the
set.

Use a **private** hierarchy when one conductor owns the tree. There is no
general submission handle, no inbox, no external identity for a child, and no
way for a sibling or the emitter to read child state. Tracked roots expose only
a cancellation/status handle. The parent authorizes whole-cohort or subset
waves, children can fold descendant results inside one enclosing wave, and the
root joins with `waitChildren` instead of polling. That is the cheaper path for
an exclusive computation tree.

Do not merge the two into one handle type.

| Need | Public | Private |
| --- | --- | --- |
| Who names the actor | external handle | no external identity; parent owns the cohort |
| How work is admitted | `wake` / `send`, or an external wave | `spawn`, then `start` / `startSubset` / `startFinal` |
| How the caller sees a result | caller-owned node or other external buffer, after an acquire | cohort `results(context)` after the join; root output still external, visible on the emitter after `takeOutcome` on a tracked park |
| Domain failure | `node.fail(code)` and `ActorNodeOutcome` after `available`; public wave failures remain structural | `context.fail(code)`, then `outcome` after the join. A tracked root's `fail` becomes `PrivateRootOutcome.failed`; untracked root `fail` is fatal |
| Cancellation | cooperative: `requestRetire(ActorRetireMode.cancel)`, or escalation from drain, closes admission and sets `context.cancelling`; the handler calls `cancelInbox()`. No library discard, no callback interruption, no final call guaranteed | cooperative and in-tree: `park.cancel`, `cancelChildren`, `cancelPeers`, observed through `context.cancelled`. Tracked parks add thread-safe `PrivateRootHandle.requestCancel`, `cancelAll` and lazy deadlines. Skips undispatched tables; never interrupts a callback or wakes a waiter early |
| How to wait | poll a wave handle, a node, or `retired` | `waitChildren` on a root; `PrivateChildStep!R` inside a child wave |
| Shutdown fence | retire, reclaim, empty ready queue | `live == 0` through pump or collect (and `takeOutcome` when tracked), then `park.destroy()` |

The shared authority, completion and shutdown rules for both models are in
[../reference/actor-lifecycle.md](../reference/actor-lifecycle.md). The
public guide is [public-actors.md](public-actors.md); the private guide is
[private-actors.md](private-actors.md).
