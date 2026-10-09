# Actor API reference index

The D source comments are authoritative for signatures, parameters, results,
calling thread, fatal conditions, memory visibility, and lifetimes. This page
is a topic index: it maps a topic to the source symbol that documents it and to
the guide or design page that explains it. It is not a second signature
catalog.

Generate local HTML reference from the repository root:

```sh
mkdir -p /tmp/antfarm-ddoc
dmd -D -Dd/tmp/antfarm-ddoc -o- -I. \
  antfarm.d antfarm_templates.d \
  actors/package.d actors/actor.d actors/wave.d actors/private_actor.d
```

Ddoc omits declarations that carry no doc comment, and exposes the package
re-exports through `actors/package.d`. The public surface is reached with
`import antfarm;`, `import antfarm_templates;`, or `import actors;`.

## Farm

| Topic | Symbol | Explanation |
| --- | --- | --- |
| Ring construction and tuning | `AntFarm.create`, `AntFarm` | [ARCHITECTURE.md](../../ARCHITECTURE.md#sizing-one-farm) |
| Consumer cursor | `ConsumerView` | [actor-lifecycle.md](actor-lifecycle.md#consumers-and-views) |
| Producer tickets | `Token`, `Tier` | [actor-lifecycle.md](actor-lifecycle.md#tokens-and-farm-provenance) |
| Payload adapters | `PayloadBody`, `Callback`, `PayloadHeader`, `PayloadEntry`, `pairPayloads`, `broadcastPayloads` | [SPEC.md](../../SPEC.md), [README First payload](../../README.md#first-payload) |
| Generated payload shims | `antfarm_templates` (`payloadRange`, `payloadEntry`, `argsConvert`) | [README Choosing a write path](../../README.md#choosing-a-write-path) |
| Huge pages | `AntFarm.usedLargePages`, `hugePages` | [README Memory backing](../../README.md#memory-backing) |

## Public actors

| Topic | Symbol | Explanation |
| --- | --- | --- |
| Runtime and slot capacity | `ActorRuntime` | [public-actors.md](../guide/public-actors.md) |
| Ownership and identity | `ActorOwner`, `ActorHandle` | [public-actors.md](../guide/public-actors.md) |
| Callback borrow and context | `ActorBorrow`, `ActorContext` | [design/public-actor-ordering.md](../design/public-actor-ordering.md) |
| Inbox nodes and outcomes | `ActorInboxNode`, `ActorNodeOutcome` | [public-actors.md](../guide/public-actors.md#inbox) |
| Wake/send/retire/reclaim results | `ActorWakeResult`, `ActorSendResult`, `ActorRetireResult`, `ActorRetireMode`, `ActorReclaimResult` | [actor-lifecycle.md](actor-lifecycle.md) |
| Type-erased resident adapter | `ActorErasedAdapter`, `ActorErasedOwner`, `ActorErasedHandle`, `ActorErasedBorrow`, `ActorErasedHandler` | [public-actors.md](../guide/public-actors.md#resident-type-erased-adapter) |
| Phase waves | `ActorWave`, `ActorWaveHandle`, `ActorWaveCompletionHook` | [public-actors.md](../guide/public-actors.md#waves) |

## Private actors

| Topic | Symbol | Explanation |
| --- | --- | --- |
| Root park and options | `PrivateRootPark`, `PrivateRootOptions`, `defaultPrivateBatchLimit` | [private-actors.md](../guide/private-actors.md) |
| Child cohort park | `PrivateChildPark`, `PrivateCohortOptions`, `PrivateStateStorage` | [private-results-and-waits.md](../guide/private-results-and-waits.md) |
| Callback borrow and context | `PrivateBorrow`, `PrivateContext` | [private-actors.md](../guide/private-actors.md) |
| Root disposition | `PrivateRootDisposition` | [private-actors.md](../guide/private-actors.md#the-cohort-api) |
| Child step and disposition | `PrivateChildStep`, `PrivateChildDisposition` | [private-results-and-waits.md](../guide/private-results-and-waits.md#resumable-child-folds) |
| Spawn/start results | `PrivateSpawnResult`, `PrivateStartResult` | [outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md#progress-and-outcomes) |
| Wave outcomes | `PrivateWaveOutcome`, `PrivateOutcomeStatus`, `PrivateCancelCause` | [guide/outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md) |
| Tracked roots | `PrivateRootHandle`, `PrivateRootOutcome`, `PrivateCancelResult` | [outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md#root-tracking-and-deadlines) |

## Allocation

| Topic | Symbol | Explanation |
| --- | --- | --- |
| Allocator interface | `ActorAllocator` | [guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md#allocators) |
| GC-rooted adapter | `ActorAllocator.gcRooted` | [guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md#gc-references) |
| Real mimalloc v3 adapter | `mimallocV3ActorAllocator` | [guide/sizing-and-allocation.md](../guide/sizing-and-allocation.md#the-mimalloc-v3-adapter) |

## Contracts

| Contract | Owner |
| --- | --- |
| Authority, completion, storage, drain and shutdown, for both models | [actor-lifecycle.md](actor-lifecycle.md) |
| Public activation and wave memory ordering | [design/public-actor-ordering.md](../design/public-actor-ordering.md) |
| Private join, outcome and cancellation ordering | [design/private-actor-ordering.md](../design/private-actor-ordering.md) |
| Private Entry/Service/cohort layout | [design/private-actor-layout.md](../design/private-actor-layout.md) |
