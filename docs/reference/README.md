# Actor API reference index

The D source comments are authoritative for signatures, parameters, results,
calling thread, fatal conditions, memory visibility, and lifetimes. This page
is a topic index: it maps a topic to the source symbols that document it and
to the guide page that explains it. It is not a second signature catalog.

Generate the local HTML reference from the repository root with:

```sh
make docs
```

which runs DMD's Ddoc generator into the ignored `.build/docs/` directory
(start with `.build/docs/antfarm/package.html`). The same works wherever DMD exists,
Windows included, with the raw command:

```sh
dmd -D -Dd.build\docs -op -o- -version=AntfarmMimallocV3 -I. -Ithreadpool\source antfarm\package.d antfarm\templates.d antfarm\parallel.d antfarm\actors\package.d antfarm\actors\actor.d antfarm\actors\wave.d antfarm\actors\private_actor.d antfarm\actors\mimalloc.d threadpool\source\threadpool\host.d
```

Ddoc omits declarations that carry no doc comment, so the generated pages
cover every public declaration of the runtime modules. `make check-docs`
verifies that: every symbol named in the Symbol column of the table on this
page exists in the DMD declaration inventory, and every public declaration
in those modules carries a non-empty Ddoc comment. The public surface is
reached with `import antfarm;`, `import antfarm.templates;`,
`import antfarm.parallel;`, `import antfarm.actors;`, or `import threadpool.host;`.

## Actor package imports

`import antfarm.actors;` re-exports three modules:

| Module | Contents |
| --- | --- |
| `antfarm.actors.actor` | public `ActorRuntime`, `ActorOwner`, `ActorHandle`, `ActorBorrow`, `ActorContext`, `ActorInboxNode`, `ActorAllocator`, and result enums |
| `antfarm.actors.wave` | public `ActorWave`, `ActorWaveHandle`, and completion hooks |
| `antfarm.actors.private_actor` | `PrivateRootPark`, `PrivateChildPark`, `PrivateContext`, `PrivateBorrow`, options, dispositions and outcome enums |

`antfarm.actors.mimalloc` is an optional adapter for the pinned mimalloc v3 allocator;
it is not part of the `antfarm.actors` package re-export. See
[sizing-and-allocation.md](../guide/sizing-and-allocation.md#the-mimalloc-v3-adapter)
for its setup and import.

## Farm

[Using Ant Farm well](../guide/using-it-well.md) collects practical rules and
links each to its owning contract or tuning page.

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Ring construction, tuning and destruction | `AntFarm.create`, `AntFarm.tryCreate`, `AntFarm.checkGeometry`, `FarmCreateError`, `FarmCreateFailure`, `AntFarm.destroy`, `MIN_RING_MIB`, `MAX_RING_MIB`, `ringMiBFromUlongs` | [ARCHITECTURE.md](../../ARCHITECTURE.md#sizing-one-farm) |
| Producer tickets and registration | `Token`, `Token.valid`, `Tier`, `AntFarm.registerProducer`, `AntFarm.unregisterProducer`, `SUBSCRIBE_FULL`, `SUBSCRIBE_RETRY`, `SUBSCRIBE_INVALID` | [actor-lifecycle.md](actor-lifecycle.md#tokens-and-farm-provenance) |
| Writing payload tables | `AntFarm.write`, `PayloadEntry` | [actor-lifecycle.md](actor-lifecycle.md) |
| Payload layout and callbacks | `PayloadBody`, `Callback`, `TableCompletionCallback`, `TableCompletionHook` | [SPEC.md](../../SPEC.md), [First payload](../guide/quickstart.md#first-payload) |
| Payload range adapters | `pairPayloads`, `broadcastPayloads`, `PayloadPairRange`, `PayloadBroadcastRange` | [SPEC.md](../../SPEC.md) |
| Fatal diagnostics | `FatalHandler`, `setFatalHandler`, `fatal` | [C ABI](../design/c-abi.md#handles-failures-and-lifecycle), [SPEC](../../SPEC.md#10-api-errors-and-implementation-profile) |
| Consumers and views | `ConsumerView`, `ConsumerView.subscribe`, `ConsumerView.unsubscribe`, `ConsumerView.consumeNext` | [actor-lifecycle.md](actor-lifecycle.md#consumers-and-views) |
| Work-cost hints | `rootCostHintAuto`, `rootCostHintFor`, `rootCostHintMaxSplits`, `rootCostHintMaxChunk`, `MAX_CHUNK`, `MAX_AVG_COST`, `DEFAULT_AVG_COST`, `avgCostForChunk` | [sizing-and-allocation.md](../guide/sizing-and-allocation.md) |
| Huge pages | `AntFarm.usedLargePages`, `DEFAULT_HUGE_PAGES`, `DEFAULT_SMALL_TABLE_THRESHOLD` | [sizing-and-allocation.md](../guide/sizing-and-allocation.md#memory-backing) |

## Parallel loops

Parallel loop adapters are a separate module:

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Parallel payload ranges and options | `parallelRange`, `ParallelRange`, `Options` | [Parallel loops](../guide/parallel-loops.md) |
| Asynchronous and blocking regions | `parallelFor`, `ParallelHandle`, `parallelForWait`, `parallel`, `ParallelLoop` | [Parallel loops](../guide/parallel-loops.md) |

## Public actors

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Runtime and slot capacity | `ActorRuntime.create`, `ActorRuntime.capacity`, `ActorRuntime.live`, `ActorRuntime.ready`, `ActorRuntime.quiescent`, `ActorRuntime.flush`, `ActorRuntime.destroy`, `ActorRuntime.erasedAdapter` | [public-actors.md](../guide/public-actors.md) |
| Ownership and identity | `ActorOwner`, `ActorOwner.handle`, `ActorOwner.intoErased`, `ActorOwner.leak`, `ActorHandle` | [public-actors.md](../guide/public-actors.md) |
| Callback borrow and context | `ActorBorrow`, `ActorContext`, `ActorContext.popInbox` | [design/public-actor-ordering.md](../design/public-actor-ordering.md) |
| Inbox nodes and outcomes | `ActorInboxNode`, `ActorInboxNode.initialize`, `ActorInboxNode.payloadAs`, `ActorInboxNode.complete`, `ActorInboxNode.fail`, `ActorInboxNode.cancel`, `ActorInboxNode.completeAndWake`, `ActorInboxNode.failAndWake`, `ActorInboxNode.cancelAndWake`, `ActorNodeOutcome` | [public-actors.md](../guide/public-actors.md#inbox) |
| Wake and send statuses | `ActorWakeResult`, `ActorSendResult` | [actor-lifecycle.md](actor-lifecycle.md) |
| Retirement and reclamation | `ActorRetireMode`, `ActorRetireResult`, `ActorOwner.requestRetire`, `ActorOwner.reclaim`, `ActorReclaimResult` | [actor-lifecycle.md](actor-lifecycle.md) |
| Type-erased resident adapter | `ActorErasedAdapter`, `ActorErasedOwner`, `ActorErasedHandle`, `ActorErasedBorrow`, `ActorErasedHandler` | [public-actors.md](../guide/public-actors.md#resident-type-erased-adapter) |

## Actor waves

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Wave lifecycle and publication | `ActorWave`, `ActorWave.begin`, `ActorWave.publish`, `ActorWave.seal` | [public-actors.md](../guide/public-actors.md#waves) |
| Observation and completion | `ActorWaveHandle`, `ActorWaveHandle.finished`, `ActorWaveCompletionHook` | [public-actors.md](../guide/public-actors.md#waves) |

## Private actors

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Root park and options | `PrivateRootPark`, `PrivateRootOptions`, `defaultPrivateBatchLimit`, `PrivateRootReclamation` | [private-actors.md](../guide/private-actors.md) |
| Root scheduling | `PrivateRootPark.spawn`, `PrivateRootPark.collectCompleted`, `PrivateRootPark.pump`, `PrivateRootPark.pumpCounted`, `PrivatePumpCount`, `PrivateRootPark.live`, `PrivateRootPark.destroy` | [private-actors.md](../guide/private-actors.md) |
| Root dispositions | `PrivateRootDisposition` | [private-actors.md](../guide/private-actors.md#the-cohort-api) |
| Child cohort park | `PrivateChildPark`, `PrivateCohortOptions`, `PrivateStateStorage` | [private-results-and-waits.md](../guide/private-results-and-waits.md) |
| Cohort waves and results | `PrivateChildPark.spawn`, `PrivateChildPark.start`, `PrivateChildPark.startSubset`, `PrivateChildPark.startFinal`, `PrivateChildPark.finished`, `PrivateChildPark.outcome`, `PrivateChildPark.results`, `PrivateChildPark.subsetMembers`, `PrivateWaveOutcome` | [private-results-and-waits.md](../guide/private-results-and-waits.md) |
| Child steps and dispositions | `PrivateChildStep`, `PrivateChildDisposition` | [private-results-and-waits.md](../guide/private-results-and-waits.md#resumable-child-folds) |
| Borrow and context | `PrivateBorrow`, `PrivateContext`, `PrivateContext.createChildren`, `PrivateContext.fail`, `PrivateContext.cancelled`, `PrivateContext.cancelChildren`, `PrivateContext.cancelPeers` | [private-actors.md](../guide/private-actors.md) |
| Spawn and start statuses | `PrivateSpawnResult`, `PrivateStartResult` | [outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md#progress-and-outcomes) |

## Tracked roots and cancellation

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Tracking options and outcomes | `PrivateRootOptions`, `PrivateRootOutcome`, `PrivateOutcomeStatus`, `PrivateRootPark.takeOutcome`, `PrivateRootPark.cancelAll` | [outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md#root-tracking-and-deadlines) |
| Handles and deadlines | `PrivateRootHandle`, `PrivateRootHandle.index`, `PrivateRootHandle.requestCancel`, `PrivateCancelResult`, `PrivateCancelCause` | [outcomes-and-cancellation.md](../guide/outcomes-and-cancellation.md#root-tracking-and-deadlines) |

## Allocation

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Allocator interface | `ActorAllocator`, `ActorAllocate`, `ActorDeallocate`, `ActorAllocator.cRuntime` | [sizing-and-allocation.md](../guide/sizing-and-allocation.md#allocators) |
| GC-rooted adapter | `ActorAllocator.gcRooted` | [sizing-and-allocation.md](../guide/sizing-and-allocation.md#gc-references) |
| Real mimalloc v3 adapter | `mimallocV3ActorAllocator` | [sizing-and-allocation.md](../guide/sizing-and-allocation.md#the-mimalloc-v3-adapter) |

## Generated payload shims

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Function adaptation | `payloadEntry`, `callbackFor`, `payloadRange`, `PayloadGrainRange`, `PayloadArgRange`, `packedLen`, `paramOffset`, `argsConvert` | [SPEC.md](../../SPEC.md), [First payload](../guide/quickstart.md#first-payload) |

## Farm host

| Topic | Symbols | Explanation |
| --- | --- | --- |
| Options and placement | `FarmHost`, `FarmHost.resolve`, `FarmHost.resolution`, `FarmHost.callerCpus`, `FarmHost.summary`, `FarmHost.toString`, `FarmHostOptions`, `HostGroups`, `CallerPlacement`, `WorkerIdle`, `FarmSizing`, `autoWorkers`, `defaultHostPoolOptions` | [hosting.md](../guide/hosting.md#placement) |
| Progress, waits and publication | `FarmHost.step`, `FarmHost.waitUntil`, `FarmHost.token`, `FarmHost.flush`, `FarmHost.pump`, `FarmHost.publish`, `FarmHost.wake` | [hosting.md](../guide/hosting.md#progress-and-waking) |
| Host-owned infrastructure | `FarmHost.createRuntime`, `FarmHost.createPark`, `FarmHost.attach`, `HostAttachment`, `FarmHost.registerProducer`, `FarmHost.unregisterProducer` | [hosting.md](../guide/hosting.md#setup) |
| Retirement, teardown and failures | `FarmHost.retire`, `FarmHost.close`, `FarmHost.closeOrAbandon`, `FarmHost.abandon`, `HostState`, `FarmHostException`, `FarmHostResolveException`, `FarmHostTimeout`, `FarmHostFailed` | [hosting.md](../guide/hosting.md#teardown) |

## Contracts

| Contract | Owner |
| --- | --- |
| Authority, completion, storage, drain and shutdown, for both models | [actor-lifecycle.md](actor-lifecycle.md) |
| Public activation and wave memory ordering | [design/public-actor-ordering.md](../design/public-actor-ordering.md) |
| Private join, outcome and cancellation ordering | [design/private-actor-ordering.md](../design/private-actor-ordering.md) |
| Private Entry/Service/cohort layout | [design/private-actor-layout.md](../design/private-actor-layout.md) |
