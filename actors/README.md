# actors

Public and private actor runtimes for Ant Farm.

```d
import actors;
```

re-exports three modules:

| Module | Contents |
| --- | --- |
| `actors.actor` | public `ActorRuntime`, `ActorOwner`, `ActorHandle`, `ActorBorrow`, `ActorContext`, `ActorInboxNode`, `ActorAllocator`, and result enums |
| `actors.wave` | public `ActorWave`, `ActorWaveHandle`, and completion hooks |
| `actors.private_actor` | `PrivateRootPark`, `PrivateChildPark`, `PrivateContext`, `PrivateBorrow`, options, dispositions and outcome enums |

`actors.mimalloc` is an optional adapter for the pinned mimalloc v3 allocator;
it is not part of the `actors` package re-export.

The two models:

- **Public actors** have stable externally held handles, an intrusive inbox, and
  externally selected phase waves.
- **Private actors** have no external identity: a single emitter drives a
  parent-owned cohort tree, with typed joined results and cooperative in-tree
  cancellation.

Start with the docs map and ownership index in
[../docs/README.md](../docs/README.md). The shared contracts are in
[../docs/reference/actor-lifecycle.md](../docs/reference/actor-lifecycle.md);
the guides are
[../docs/guide/public-actors.md](../docs/guide/public-actors.md) and
[../docs/guide/private-actors.md](../docs/guide/private-actors.md). The compact
API topic index is in
[../docs/reference/README.md](../docs/reference/README.md). Proofs are in
[../docs/design/public-actor-ordering.md](../docs/design/public-actor-ordering.md),
[../docs/design/private-actor-ordering.md](../docs/design/private-actor-ordering.md),
and [../docs/design/private-actor-layout.md](../docs/design/private-actor-layout.md).
