# Actor torture suite

This suite promotes the A2 actor inbox beyond ordinary contention coverage.
It compiles deterministic test hooks into `actors.actor` and forces senders
to stop at each ownership/publication boundary while activation and retirement
proceed on other threads.

Coverage includes:

- node claim before inbox publication;
- close after reservation, claim, publication, and activation signal;
- eight simultaneous pre-close reservations racing to claim one node;
- accepted-before-close drain and reservation quiescence;
- an engine-side module-generation fence model which closes admission and
  aggregates resident type-erased actor owners with old-generation code and
  ownership claims;
- independent unload stalls for a running actor callback, a module-owned
  destructor, a sender paused after its actor submission reservation has been
  released, and a popped-but-uncompleted inbox node;
- proof in the sender and popped-node cases that the actor can be reclaimed
  first while the remaining engine claim still forbids the simulated unload;
- wake and send in the wave lifecycle-CAS/owner-store gap, plus a sender
  admitted before the wave and signalled after owner publication: the wave
  completes with `interrupted && !failed`, and exactly one later activation
  honours the signal (and completes the sent node). Descriptor reuse clears
  outcomes and rejects old handles; duplicate admission still sets `failed`;
- a wave-reservation/retirement race proving that aggregate membership pins a
  retired actor and its runtime until the open wave is sealed;
- immediate reuse of a released wave member as another wave's head, proving
  that the old wave finishes its link writes before releasing the lifetime pin;
- a late accepted sender queuing work between retirement's initial idle
  observation and its submission-gate join, proving the lifecycle is rechecked
  before using the idle retirement store;
- F4b public cancel-mode retirement: a 32-node backlog cancelled in one
  `cancelling` activation with every sender reading `cancelled` after
  `available`, a drain-mode control on the identical backlog that completes
  every node and never reports `cancelling`, and a budgeted first activation
  whose carried remainder is cancelled by the next activation's
  `cancelInbox`;
- sequential drain-to-cancel escalation (`requested`), repeated cancel
  (`alreadyRetired`), drain-after-cancel keeping the cancel mode, and
  cancel-after-RETIRED, exercised against consumer entry (sender paused
  after its activation signal), consumer exit (activation stalled inside the
  handler while the owner escalates), and a drain request parked at
  `retirementIdleObserved` while a late sender's signal reschedules the
  actor; the same cancel and drain-to-cancel escalation through the
  type-erased `ActorErasedOwner`, whose handler must observe `cancelling`;
- cancel while SCHEDULED with a queued inbox (the entry word), cancel of an
  idle actor whose submission gate is held by a paused sender (retirement's
  idle reschedule and the empty-inbox consumer reschedule, both without a
  handler call), and a sender's signal landing on IDLE|RETIRE|CANCEL while
  the retirement thread is parked on its idle observation;
- cancel between a wave's lifecycle reservation and its membership
  publication, on both the admission path (the wave operation exits
  interrupted and hands the actor back with the mode preserved) and the
  rollback path (a non-idle second member fails the reservation and
  `cancelActorWavePayload` restores the mode), in both cases with a sender
  that passed the zero `waveOwnerWord` check observing `cancelled` rather
  than `completed`;
- an idle, empty actor cancelled directly retiring without any handler
  invocation, documenting that no final handler call is guaranteed;
- multi-actor ready-queue contention with six producers and six consumers;
- a paused ready-list detach proving concurrent flushers return promptly,
  wakes remain publishable, and the next drainer acquires the saved backlog;
- a 1,024-actor backlog with partial writes, repeated Farm backpressure,
  four concurrent flushers, two consumers, and exact self-republication counts;
- FIFO order per producer, exact-once delivery, payload checksums, bounded
  carry republishing, 32 rounds of generation reuse, and a disabled GC;
- balanced cold-path allocation and a flat warm-path allocation count;
- free-slot recovery after repeated state-allocation failures, exact capacity
  exhaustion, and eight-thread creation/reclamation through 64 generations;
- the optional mimalloc v3 adapter's exact pointer/size/alignment forwarding
  and 128-byte-aligned actor state, using a local ABI stub rather than
  requiring mimalloc for the core suite;
- a real static link against the pinned mimalloc v3.5.3 submodule, checked at
  runtime with `mi_version() == 30503`; and
- 8,192 actor-state frees from eight threads other than the allocation thread,
  plus foreign-thread frees of each runtime and stable-slot array. The same
  test runs against release, `MI_DEBUG=FULL`, and mimalloc-instrumented
  ThreadSanitizer builds.

Run with LDC, DMD, or LDC ThreadSanitizer:

```text
make -C actor_torture run
make -C actor_torture run-dmd
make -C actor_torture run-tsan
```

`make -C actor_torture death` runs the child-process diagnostics table
(`actor_torture death`) on LDC and DMD, debug and `-release`. Each row
re-executes `/proc/self/exe` as `child:<mode>` and checks the exit status and
stderr: fatal sites must abort with their message, debug diagnostics such as a
dropped live `ActorOwner` must print and exit 0, and silent rows must leave
stderr empty. Rows for `version(assert)`-only diagnostics print `SKIP` under
`-release`.

## Public fatal-site coverage

The existing child table contains 17 real abort rows, replacing the harness
sample, plus the owner diagnostic/silent controls. Each abort row requires a
nonzero exit and the full `antfarm fatal: <message>` substring. All abort rows
run in both debug and `-release`; only the live-owner drop warning skips in
`-release`. Successful rows print their mode for focused evidence.

Two production-elided hooks exercise the real generation increment and wrap
checks. Under `version(AntfarmActorTestHooks)` only,
`ActorRuntime.testPresetFreeSlotGeneration(ulong generation)` sets the previous
generation of the next vacant slot on a quiescent runtime, with no concurrent
operations, and `ActorWave.testPresetGeneration(ulong generation)` sets a
finished wave's generation, with no concurrent descriptor accesses or completion
callbacks. The tests preset one below the last representable generation, create
or begin that final valid generation, retire/reclaim or seal it, then attempt
the next generation. Neither hook changes production lifecycle logic.

The table inventories every `fatal()` call in `actors/actor.d` and
`actors/wave.d`. Line numbers describe this revision. "Unreachable" means no
feasible probe through the public API and existing synchronization hooks while
preserving internal storage and ownership invariants. The last column states
what extra fault injection or private entry point would be required; those
sites are not faked. Public misuse with a defined fatal check is covered,
including reinitializing a queued node to trigger the node-state check.
The two submission-underflow calls are listed separately.

| Site | Exact fatal message | Coverage / reason and required seam |
| --- | --- | --- |
| `actors/actor.d:384` | `complete actor inbox node not owned by actor` | Covered: `child:node-complete-unowned` |
| `actors/actor.d:395` | `fail actor inbox node not owned by actor` | Covered: `child:node-fail-unowned` |
| `actors/actor.d:397` | `actor inbox node failure code out of range` | Covered: `child:node-fail-oversize` |
| `actors/actor.d:408` | `cancel actor inbox node not owned by actor` | Covered: `child:node-cancel-unowned` |
| `actors/actor.d:570` | `type-erased actor borrow type mismatch` | Covered: `child:erased-borrow-mismatch` |
| `actors/actor.d:755` | `overwrite live ActorErasedOwner` | Covered: `child:erased-owner-overwrite-live` |
| `actors/actor.d:835` | `overwrite live ActorOwner` | Covered: `child:owner-overwrite-live` |
| `actors/actor.d:1162` | `actor generation wrap` | Covered: `child:actor-generation-wrap` |
| `actors/actor.d:1228` | `destroy actor runtime with live actors` | Covered: `child:runtime-destroy-live` |
| `actors/actor.d:1232` | `destroy actor runtime with queued actors` | Unreachable: Queued actors retain live slots, so public teardown hits the earlier live-actor check. Would need a ready-queue/count corruption seam. |
| `actors/actor.d:1396` | `running actor lost generation during self-retirement` | Unreachable: Running ownership pins the generation until callback exit. Would need lifecycle corruption during a callback. |
| `actors/actor.d:1481` | `reclaim actor with live inbox or submission` | Unreachable: RETIRED requires an empty inbox and closed, joined submission gate. Would need a retired-slot inbox/gate corruption seam. |
| `actors/actor.d:1724` | `actor inbox submission count wrap` | Unreachable: No feasible public probe can hold 2^63−1 simultaneous reservations; existing hooks only pause operations. Would need a submission-count preset seam. |
| `actors/actor.d:1741` | `actor inbox submission underflow` | Unreachable: Each successful reservation is released exactly once, including failed confirmation. Would need a gate-count corruption seam or private release entry point. |
| `actors/actor.d:1752` | `actor inbox submission underflow` | Unreachable: Each successful reservation is released exactly once, including failed confirmation. Would need a gate-count corruption seam or private release entry point. |
| `actors/actor.d:1806` | `accepted actor inbox send lost its generation` | Unreachable: The sender reservation pins the generation and prevents retirement until its final signal. Would need lifecycle corruption while reserved. |
| `actors/actor.d:1835` | `actor inbox node state corrupt` | Covered: `child:node-reinitialize-queued` |
| `actors/actor.d:1889` | `cancel actor wave payload lost membership` | Unreachable: Rollback receives the membership owner it just installed. Would need a membership corruption seam before rollback. |
| `actors/actor.d:1899` | `cancel actor wave payload lost reservation` | Unreachable: Rollback retains the scheduled generation; public wake/retire only add flags. Would need lifecycle corruption before rollback. |
| `actors/actor.d:1923` | `actor wave completion lost membership` | Unreachable: Aggregate membership remains pinned until its unique release. Would need a membership corruption seam before completion. |
| `actors/actor.d:1948` | `actor wave dispatch lost reservation` | Unreachable: The published payload owns a scheduled generation and membership pin. Would need lifecycle corruption before dispatch. |
| `actors/actor.d:1961` | `actor wave operation lost reservation` | Unreachable: The operation retains its scheduled generation; public wake/retire only add flags. Would need lifecycle corruption during the operation. |
| `actors/wave.d:198` | `actor wave requires a Farm` | Covered: `child:wave-null-farm` |
| `actors/wave.d:203` | `reuse unfinished actor wave` | Covered: `child:wave-reuse-unfinished` |
| `actors/wave.d:205` | `actor wave generation wrap` | Covered: `child:wave-generation-wrap` |
| `actors/wave.d:247` | `publish uninitialized actor wave` | Covered: `child:wave-publish-uninitialized` |
| `actors/wave.d:249` | `publish sealed actor wave` | Covered: `child:wave-publish-sealed` |
| `actors/wave.d:253` | `actor wave table count wrap` | Unreachable: No feasible public probe publishes 2^64 tables in one generation. Would need a published-table count preset seam. |
| `actors/wave.d:268` | `actor wave table count underflow` | Unreachable: A failed publish rolls back its own increment under the single-orchestrator contract. Would need counter corruption or unsupported overlapping descriptor writes. |
| `actors/wave.d:280` | `seal uninitialized actor wave` | Covered: `child:wave-seal-uninitialized` |
| `actors/wave.d:285` | `seal actor wave twice` | Covered: `child:wave-seal-twice` |
| `actors/wave.d:321` | `actor wave progress exceeds published tables` | Unreachable: Farm notifies each tracked table once after its publication count increment. Would need a duplicate notification or completion-counter corruption seam. |
| `actors/wave.d:332` | `actor wave credit underflow` | Unreachable: The producer and each tracked table release one owned credit. Would need a duplicate notification or credit-count corruption seam. |
| `actors/wave.d:337` | `finish unsealed actor wave` | Unreachable: The producer credit persists until seal; tables cannot release the final credit while unsealed. Would need credit/status corruption. |
| `actors/wave.d:402` | `actor wave preparation exceeds member range` | Unreachable: Farm computes a prefix bounded by the private range length. Would need a private range entry point accepting an invalid count. |
| `actors/wave.d:414` | `actor wave member list corrupt` | Unreachable: Single-orchestrator reservation pushes and rollback pops in reverse order. Would need intrusive-list corruption before rollback. |
| `actors/wave.d:439` | `actor wave payload length corrupt` | Unreachable: The private publisher emits exactly three words. Would need a private callback entry point or malformed ring body. |
| `actors/wave.d:444` | `actor wave payload identity corrupt` | Unreachable: Preparation rejects invalid handles and the publisher supplies its own nonnull wave address. Would need malformed ring identity words. |
| `actors/wave.d:453` | `actor wave completion lost owner` | Unreachable: begin installs a nonnull descriptor address as the tracked hook context. Would need a private completion entry point or hook-context corruption. |

## Plan scenarios that are not fatal sites

| Scenario | Actual behavior | Function / existing evidence |
| --- | --- | --- |
| Double reclaim | Second call returns `staleOwner` | `reclaimActor`; silent `child:owner-double-reclaim` control |
| Reclaim under live wave membership | Returns `busy`, even after retirement | `reclaimActor`; `testWaveMembershipPinsRetiredActor` |
| Overlapping retire requests on one owner | Unsupported: requests must be serialized; there is no fatal guard or guaranteed overlapping-call result | `requestActorRetire`; sequential escalation and repeated requests are covered by the cancel-mode tests |
| Duplicate wave membership | Admission rejects the duplicate and sets `failed` | `ActorWaveBodyRange.prepareTableFrontN` / `reserveActorWavePayload`; `testWaveSignalRace` duplicate control |
| Begin reuse while a completion hook runs | Allowed after `finished`; old callback context must remain alive and descriptor access needs separate synchronization | `ActorWave.begin` / `releaseCredit`; `testCompletionCallbackRunsAfterFinished` |
| Erased adapter misuse | Invalid creation metadata returns an invalid owner; requesting a mismatched borrow size/alignment is fatal | `ActorErasedAdapter.createActor` / `ActorErasedBorrow.value`; `child:erased-borrow-mismatch` |

For the wave lifecycle and outcome checks only, build with `make -C
actor_torture` and run `actor_torture/actor_torture --waves`. The individual
signal regressions accept `--wave-wake`, `--wave-send`, and `--wave-late-send`.
Each fails against the old dispatch-to-`failed` mapping after verifying the
operation and autonomous signal both ran, at "benign wave signal must not fail
the completed wave".

After initializing submodules, run the real mimalloc lanes with:

```text
make -C actor_torture run-mimalloc
make -C actor_torture run-mimalloc-dmd
make -C actor_torture run-mimalloc-debug
make -C actor_torture run-mimalloc-tsan
```

The real TSan lane requires Clang for mimalloc and LDC for the D test binary.

On Windows x64, with CMake, Visual Studio and the D compiler on PATH, run
the static allocator lanes from the repository root:

```powershell
./actor_torture/run-mimalloc-windows.ps1 -Compiler dmd -Configuration Debug
./actor_torture/run-mimalloc-windows.ps1 -Compiler ldc2 -Configuration Debug
./actor_torture/run-mimalloc-windows.ps1 -Compiler ldc2 -Configuration Release
```

`-CMake` accepts a full path to CMake when it is not on PATH. Debug uses
unoptimized allocator code and `MI_DEBUG=FULL`, with the static release CRT
(`CMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`, D `-mscrtlib=libcmt`).
Using CMake's default Debug CRT instead introduces an unresolved
`__imp__CrtDbgReport` dependency. The script builds separate Debug/Release
archives, links the real allocator, and runs the full actor suite including
cross-thread frees. It does not modify the pinned submodule.

The ordinary library build contains neither the deterministic hook branches
nor a mimalloc link dependency. The Makefile builds the pinned static source
with allocator override disabled. The root Dub configuration `mimalloc-v3`
builds and links that pinned archive for Linux x86-64 and Windows x64. The
pinned correctness lane links the same source directly.

The module-generation fence in this suite is deliberately a test-side engine
model. Its close/count gate is the unload authority, while its per-kind counts
are diagnostics and its actor registry is driven by one coordinator. It does
not add module hierarchy to `actors`, implement dynamic-library loading,
or define non-POD actor-state destruction. The "destructor" claimant models
module-owned resource or message destructor code admitted before close.
