# Private actor correctness tests

```sh
make -C tests/private run
make -C tests/private run-dmd
make -C tests/private run-tsan
make -C tests/private run-death
make -C tests/private run-death-dmd
```

Tests force ordinary pages and a 2 MiB Farm for ring reuse and backpressure;
performance measurements use 8 MiB. A same-thread publication audit hook
consumes each table after its sentinel release but before `write`
returns. Consumer reclamation frees terminal root state in that interval;
nested child waves also finish before their producer has sealed publication.
The hook compiles out of ordinary builds.

The suite checks bounded FIFO progress, exact callback counts, balanced frees,
allocation failures, accepted-prefix retry, zero admission, repeated roots,
parent authority, nonoverlapping child waves, three-level descendant joins,
terminal-wave restrictions, and four independent creator parks on one Farm.
Borrow/context/park ordinary copies and state-field access are compile-fail
assertions. They do not prove arbitrary `@system` code obeys the no-escape
contract; see [../../docs/guide/private-actors.md](../../docs/guide/private-actors.md).

`./tests/private/private_tests_tsan join-only` is the focused
same-shard memory-order regression. It forces two consumers into distinct
one-payload runs in one shard; both write plain values and the completion hook
reads them. The handshake before those writes is relaxed. There is no test
release after the writes that could hide a missing Farm join. Replacing the
completion `atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, runlen)` with `raw`
produces a ThreadSanitizer data race in `joined`; the corrected code passes.

The public `actors_torture` suite additionally pauses a wave table notification
after recording Wprogress. Both sealing and a second consumer completing
another table must leave the wave unfinished until the paused hook drops its
credit. Reuse begins immediately after `finished`, before joining the old
consumer. The former equality-based implementation deterministically fails
`progress equality must not release active table notification`.

## Fatal-death harness

`run-death` (LDC) and `run-death-dmd` (DMD) build both a debug and a
`-release` binary and run the `death` and `death-negative-control` CLI modes
on each, so the checks cannot be removed by `-release`'s disabled asserts.
`death` re-executes `/proc/self/exe` with the `fatal-sample:` child mode,
which calls `fatal("private death harness sample")`. It requires both a
nonzero death status (exit code or terminating signal) and the exact
`antfarm fatal: private death harness sample` text drained from a stderr pipe
kept separate from stdout. `death-negative-control` reuses the same predicate
and fails if it accepts either a child that exits zero or a nonzero death with
the wrong stderr text. Non-Linux hosts print an explicit skip; `/proc/self/exe`
re-exec is Linux-only. Add contract scenarios as child modes in `main` and
check them with `diedWithMessage` in `deathHarness`. `make death` runs both
compiler lanes.

## Hardening and mutation verification

The normal suite also covers 257/513-member child cohorts under forced zero
and partial admission, child object/entry/state allocation failures, mixed
continuing/terminal roots under both reclamation policies, refill after a final
wave, empty starts, and a depth-64 hierarchy. `phase-d` runs these cases alone.
Root limits 1/255/256/257/1024/4096, overflow checks, quota splitting and a
1,537-member child cohort are covered separately by the normal suite.
`huge-root-limit` verifies capacity-clamped storage with configured limits
`size_t.max` and `1UL << 32`, balanced allocation sizes, and a descriptor-array
overflow rejection before any allocation. The overflow capacity is
`size_t.max / Batch.sizeof + 1` with limit 1; the earlier entry-metadata bound
makes overflow of the clamped batch-pointer backing array unreachable.
`explicit-child-limit` counts eight tables for 50 children at limit 7.
`inherited-child-limit` counts ten grandchild tables at the middle cohort's
explicit limit 5, despite a root limit of 31. Both check every table's callback
count with the existing same-thread publication hook.

With `AntfarmActorTestHooks`, deterministic tests pause a final wave before idle
publication, force exactly one return-stack CAS retry, and complete a child
table while its emitter is still inside `write`. Parent observations
always use a live callback context. Raw control flags do not publish callback
results; result visibility must come from the library joins. Release builds
without the test version have identical private-module sizes at the rewritten
C7 and D commits: 2,846 bytes of text, 608 bytes of data and zero BSS. The
original pre-follow-up comparison also found identical release lifecycle
binaries: 1,126,813 bytes of text, with data/BSS unchanged.

Each mutation below was applied independently, rebuilt with LDC, and run once
on 2026-10-04. All seven terminated with SIGABRT and the listed failure. Each
rebuild contained only its named mutation; the unmodified source was
restored after the series. Mutation comments beside the tests identify the exact
statements in `antfarm/actors/private_actor.d`.

| Mutation | Focused mode | Observed failure |
| --- | --- | --- |
| Omit rejected child table credit release | `child-large-cohort` | `child zero admission credit released` |
| Advance child cursor by offered rather than accepted count | `child-large-cohort` | `cohort exactly once per generation` |
| Set accepted root count to zero | `mixed-table` | `mixed table one terminal collected` |
| Publish idle before the final-access pause | `delayed-final-credit` | `delayed idle: authorized parent must still see active` |
| Discard a batch after failed return CAS | `return-cas-retry` | `return CAS both tables collected timeout` |
| Initialize pending credits without the producer hold | `child-before-seal` | `early child completion must not publish idle before emitter seal` |
| Keep the member count after terminal completion | `refill-after-final` | `post-final empty start` |

The review follow-up also verified these two mutations independently with LDC
and `AntfarmWriteAuditHooks`, then restored the source. Both terminated with
SIGABRT and the expected named failure:

| Mutation | Focused mode | Observed failure |
| --- | --- | --- |
| Force `PrivateChildPark.create` to assign `batchLimit_ = 256` | `explicit-child-limit` | `explicit child table exceeds limit 7` |
| Restore root-only inheritance in no-argument `createChildren` | `inherited-child-limit` | `grandchild table exceeds immediate parent limit 5` |

`make check` includes LDC/DMD suites, TSan, both compiled documentation
programs, and debug/release death tests. `make check-soak` builds the private
TSan binary once and repeats it for at least two minutes, stopping on any
failure. TSan builds define `TSan` to shorten only the repeated stress rounds;
all deterministic cases still run in each iteration. Windows death-harness
execution is explicitly skipped; Windows smoke testing remains manual.

The restored-source run on 2026-10-04 passed `make check` and completed
263 TSan soak loops in 121 seconds with no failures.

## Root-wait and result-port selectors

Pass a selector to `private_tests`, `private_tests_dmd`, or
`private_tests_tsan`. The normal suite includes `phase-e1` and
`phase-ports` and `phase-dtor`; deterministic schedules require the test-hook build flags
supplied by the Makefile.

| Group | Selectors |
| --- | --- |
| E1 root waits | `phase-e1`, `e1-basic`, `e1-before-custody`, `e1-after-custody`, `e1-late-strict`, `e1-joined-mixed`, `e1-root-ready-cas-retry`, `e1-seal-final-owner` |
| E1 counter death | `e1-counter-exhaustion-death` runs the harness; `e1-counter-exhaustion` is its fatal child mode. |
| Result ports | `phase-ports`, `ports-basic`, `ports-poll-visibility`, `ports-wait-visibility`, `ports-late-tail`, `ports-large-cohort`, `ports-nested`, `ports-alloc-fail` |
| Result-port death | `ports-death` runs the harness; `ports-read-active`, `ports-wrong-parent-results`, and `ports-default-results` are its fatal child modes. |
| Destructors (E2-1) | `phase-dtor`, `dtor-root`, `dtor-child`, `dtor-order`, `dtor-failed-spawn`, `dtor-aligned`, `dtor-moved-from`, `dtor-recursive`, `dtor-results`, `dtor-gc` (skipped under TSan: druntime's GC suspend signals are not TSan-clean) |
| Destructor death | `dtor-death` runs the harness; `dtor-destroy-live`, `dtor-finish-active-child`, and `dtor-reenter-failed-spawn` (debug only: the reentrancy flag is an assert-mode field) are its fatal child modes (the first two may not run a destructor). |

The port tests cover typed-result lifetime, acquire visibility, notification
tails, admission retries, pipelined reductions and allocator failures. Compile
checks reject qualified/non-POD result types and invalid callback signatures.
`make death` runs the E1 and port death harnesses in both compiler lanes,
including debug and release. The destructor tests record every `~T` with its
thread: the stored instance is destroyed exactly once on a consumer (never the
emitter), caller-side values (failed-spawn arguments, moved-from sources) on
the calling thread. The `stateAllowed` compile checks invoke `spawn`/`createChildren`.

### E2-1 mutation evidence

Verified individually on LDC against the final E2-1 implementation. Each
mutated binary aborted with its named invariant; the unmodified tests passed.

| Mutation | Selector | Failure |
| --- | --- | --- |
| D-M1: cohort disposal omits `~T` | `dtor-order` | stored exactly once |
| D-M2: root destruction moves to emitter reclaim | `dtor-root` | stored exactly once / consumer placement |
| D-M3: destroy member before descendants | `dtor-order` | leaves before mid |
| D-M4: destroy terminal child twice | `dtor-child` | final destroyed exactly once before join |
| D-M5: remove GC range before `~T` | `dtor-gc` | referenced objects survive until destroyed |
| D-M6: write result slot after destruction | `dtor-results` | slot written before `~T` |
| D-M8move: failed root spawn guards with `move(initial)` | `dtor-aligned` | argument destroyed once / no double frees (DMD) |

D-M4 covers today's terminal child dispatch; deferred child retirement belongs
to E2-4. Extra test-author scratch checks caught missing root/member destruction,
root post-order reversal, destruction on nonfinal waves, copying instead of
moving, duplicate failed-argument destruction, live-park teardown destruction,
and destruction before the active-child fatal. Deleting the root failed-spawn
guarded destruction makes its debug death test survive. Restoring `T a;` in
the constraint probe rejects the disabled-default regression states.
`dtor-aligned` also holds on LDC with D-M8move applied, because LDC's
`core.lifetime.move` is correct for over-aligned structs; the mutation is
confirmed on DMD, whose move runs `~T` during the move and double-frees the
owning token.

Forced `GC.collect()` runs under LDC/DMD debug and release. Only that test
skips TSan: druntime's stop-the-world suspend-signal handler reports
"signal handler spoils errno". All other destructor cases run under TSan.

## In-tree cancellation (F2)

| Group | Selectors |
| --- | --- |
| Deterministic schedules | `cancel` (all), `cancel-c1` … `cancel-c13` |
| Torture | `cancel-torture [roots=3000] [seed=17]` |
| Death | `cancel-death` runs the harness; `cancel-peers-root`, `cancel-park-unauthorized`, `cancel-default-cancelled`, `cancel-default-children` and `cancel-default-peers` are its fatal child modes |

Most schedules use a lockstep consumer: a second thread consumes only while
the emitter waits in `lockstepDrain`. Publication, cancellation and completion
then interleave in one fixed order, and no callback runs on the emitter
thread, so destructor placement is checkable. C4 and C7 use free-running
workers; C7 and C9 also use the `privateEmitBeforeCancelCheck`,
`privateTruncateBeforeSeal`, `privateEmitBeforeSeal` and
`privateRootAfterIdleBeforeCredit` hook points. C7's quarantining allocator
poisons each freed block with an offset-dependent pattern and verifies it at
the end.

The torture builds random trees up to depth 4 on four consumers, with random
`start`/`startFinal`/`startSubset`, member `fail`, `cancelPeers`,
`cancelChildren`, polling-root `park.cancel` and `cancelChildren` +
`finishAfterChildren`. It checks result prefixes and slot values, failure
pairs, `cancelled` monotonicity, exactly-once destruction and allocation
balance, and requires that truncation and failure both occurred. The normal
suite runs 3000 roots (300 under TSan).

Each mutation below was applied alone to `antfarm/actors/private_actor.d`, rebuilt
with LDC and run on its selector on 2026-10-06; the source was restored
afterwards. Every run aborted (SIGABRT) with the listed failure. The script and
raw results were archived outside the repository; the deterministic schedules
below remain the in-tree control.

| Mutation | Selector | Observed failure |
| --- | --- | --- |
| C1a: drop `dispatched_ = cursor_` | `cancel-c1` | debug: `private child result: unwritten slot`; release: `c1 truncated outcome` |
| C1b: truncate without the seal | `cancel-c1` | `c1 drain` timeout |
| C2a: seal a truncated `startFinal` wave like a non-retiring one | `cancel-c2` | `c2 every member destroyed once` |
| C2b: seal at truncation and retire the skipped members in `finish` | `cancel-c2` | `c2 destructors on consumers` |
| C3: switch `park.header_.call` instead of a local cleanup header | `cancel-c3` | `c3 drain` timeout |
| C4: omit the `subtreeCancel` store | `cancel-c4` | `c4 grandchildren observe cancellation` |
| C5: omit the `subtreeCancel` reset at spawn | `cancel-c5` | `c5 recycled root starts clear` |
| C6: omit the `cancel` reset at start | `cancel-c6` | `c6 second wave runs fully` |
| C7: store `dispatched_` after the truncating seal | `cancel-c7` | `c7 freed memory untouched` |
| C8: early wake (`releaseRoot` in `cancelPeers`) | `cancel-c8` | `c8 drain` timeout (root reclaimed twice) |
| C9: re-check truncation at the seal | `cancel-c9` | `private wave credit underflow` |
| C12: subset counters from `count_` | `cancel-c12` | `c12 subset outcome` |
| C13: result prefix from `count_` | `cancel-c13` | debug: `private child result: unwritten slot`; release: `c13 prefix survives retirement` |

C10 and C11 are behavioural (`cancelChildren` + `finishAfterChildren`, and
partial construction). F3 runs C10 also on tracked parks, where the root
fails, is cancelled through its handle, or both, and checks the taken
`failed(31)`, `cancelled(requested)` and `failed` outcomes.

## Root tracking (F3)

| Group | Selectors |
| --- | --- |
| Deterministic schedules | `root-tracking` (all), `root-t1` … `root-t8`; C10's tracked half runs under `cancel-c10`; O6 under `outcomes` |
| Torture | `tracked-torture [roots=3000] [seed=23]` |
| Death | `root-tracking-death` runs the harness; `root-tracking-misuse:spawn-handle`, `:take`, `:cancel-all`, `:handle-index`, `:destroy-untaken` and `root-generation-wrap` are its fatal child modes |

T1 and T2 pause at `privateRootCancelBeforeCas`, `privateRootTerminalBeforeCas`
and `privateTakeBeforeVacantStore` while a foreign thread cancels; T4 counts
clock reads through the test-only `privateClockReads`; T9 seeds a root's
generation with `privateTestSetRootGeneration`. T6 runs under four free-running
consumers and is the TSan check of the X1a edge on both terminal paths and with
consumer reclamation. T7 is inline, so its 1000 roots are in exact states when
`cancelAll` runs.

The tracked torture reuses the F2 trees under tracked roots with random
deadlines (none, past, within 2 ms, an hour away), root `fail`, a foreign
thread cancelling random published handles (most of them stale by then) and
occasional `cancelAll`. The emitter pumps and takes outcomes. It checks one
outcome per root named by its handle, `failed` exactly for failing roots with
their code, a cause exactly for `cancelled`, `deadline` only for roots that had
one, that no root which observed `cancelled` and no handle that got
`requested` ends `completed`, exactly-once destruction, allocation balance and
`live` reaching zero. The normal suite runs 3000 roots (300 under TSan).

Each mutation below was applied alone to a copy of the sources, built with
LDC in debug and `-release`, and run on its selector on 2026-10-06. Both builds
aborted (SIGABRT) with the listed failure. T7a mutates the test's drain loop,
not the runtime. The script and raw results were archived outside the
repository; the deterministic schedules below remain the in-tree control.

| Mutation | Selector | Observed failure |
| --- | --- | --- |
| T1a: canceller load+store instead of CAS | `root-t1` | `t1 resumed canceller is stale` |
| T1b: `takeOutcome` does not bump the generation | `root-t1` | `t1 resumed canceller is stale` (ABA: the stale CAS succeeds) |
| T2: terminal transition as a plain store from the first load | `root-t2` | `t2 cancel before terminal CAS` |
| T3: drop the `records` branch from `scopeCancelled` | `root-t3` | `t3 first emit truncates` |
| T4: ignore the armed flag (always read the clock) | `root-t4` | `t4 no clock read without a deadline` |
| T5: recycle the slot at collection | `root-t5` | `t5 capacityFull while outcomes held` |
| T7a: drain without `takeOutcome` | `root-t7` | `t7 drained` timeout |
| T7b: `cancelled` outranks `failed` | `root-t7` | `t7 statuses` |
| T7c: a later root `fail` overwrites the code | `root-t7` | `t7 statuses` |
| T7d: `cancelAll` ignores expired deadlines | `root-t7` | `t7 statuses` |
| T7e: `cancelAll` also marks held outcomes | `root-t7` | `t7 cancelAll counts live unfinished roots` |
| T9: no generation wrap check | `root-tracking-death` | the `root-generation-wrap` child prints `root generation wrap survived` and exits 0, so the harness check fails |

T6 (visibility edge under TSan), T8 (handle identity) and the O6 and C10
extensions are behavioural.

## Cohort slab storage

Opt-in `PrivateStateStorage.cohortSlab` cohorts. `slab` runs the group; death
modes run from `deathHarness` (`make death`) as well as `slab-death`.
`slab-gc` is skipped under TSan, like `dtor-gc`. Test-only accessors
(`privateTestSlabPointer`, `privateTestSlabSlotZero`, `privateTestDirtySlabSlot`,
entry/result/selection pointers) live in `version (AntfarmActorTestHooks)` in
`antfarm/actors/private_actor.d`.

| Group | Selectors |
| --- | --- |
| Correctness | `slab`, `slab-retire`, `slab-retain`, `slab-refill`, `slab-continuation`, `slab-cancel`, `slab-idle`, `slab-create-fail`, `slab-gc`, `slab-reclaim`, `slab-results`, `slab-per-object` |
| Root slabs | `root-slab` runs `root-slab-lifecycle`, `root-slab-dtor`, `root-slab-tracked` and `root-slab-create-fail`: `PrivateStateStorage.cohortSlab` on `PrivateRootPark` under both reclamation policies (test accessors `privateTestRootSlabPointer`, `privateTestRootSlabSlotZero`, `privateTestRootNextSlot`, `privateTestDirtyRootSlabSlot`) |
| Death | `slab-death`; child modes `slab-unauthorized`, `slab-active-dispose`, `slab-dirty-slot` and `root-slab-dirty-slot` (debug death; `-release` survives because the uncleared-slot checks are `version(assert)`) |

Each mutation below was applied alone to `antfarm/actors/private_actor.d`, rebuilt with
LDC, and run on its selector; the source was restored afterwards. Every run
aborted (SIGABRT) with the listed failure. The release row used the `-release`
binary.

| Mutation | Selector | Observed failure |
| --- | --- | --- |
| Slab `retireMember` also `freeMemory`s the slot | `slab-retire` | `slab exact: mismatched triple` |
| `dispose` frees each slab slot | `slab-retire` | `slab exact: mismatched triple` |
| POD `dispose` frees `entry.state` | `slab-retain` | `slab exact: mismatched triple` |
| non-POD `dispose` skips retained members | `slab-retain` | `slab-retain disposed once` |
| Slab `spawn` allocates | `slab-refill` | `slab-refill no allocation after create` |
| `retireMember` drops the slot memset | `slab-refill` | `private child slab: spawn into an uncleared slot` |
| Cleanup calls `retireMember` with `perObject` | `slab-continuation` | `slab exact: mismatched triple` |
| Retire when a continuation returns `done`, before the join | `slab-continuation` | `retire private actor with active child wave` |
| `runSkippedRetire` uses `perObject` | `slab-cancel` | `slab exact: interior pointer` |
| Truncation seals a retiring wave | `slab-cancel` | `slab-cancel destroyed once before refill` |
| Slot memset removed from `retireMember` (not done before idle publication) | `slab-idle` | `slab-idle slots zero` |
| Slot memset moved above `destroyState` | `slab-idle` | `slab-idle field live in destructor` |
| Slab allocation failure frees only the park | `slab-create-fail` | `slab exact: unwind order` |
| Slab `createChildren` drops the `slabByteLimit` check | `slab-create-fail` | `slab-create-fail limit 0` |
| Slab `createChildren` drops the `capacity * T.sizeof` overflow check | `slab-create-fail` | `slab-create-fail overflow allocates nothing` |
| No post-destroy memset | `slab-gc` | `slab-gc retired slot released its reference` |
| Slab uses `allocate` instead of `allocateZero` | `slab-gc` | `slab-gc unspawned tail was zeroed` |
| `dispose` skips the slab free | `slab-reclaim` | `slab-reclaim allocation balance` |
| Retiring `finish` clears result storage | `slab-results` | `slab-results readable after retirement` |
| perObject `spawn` does not allocate | `slab-per-object` | `slab-per-object one state per spawn` |
| `spawn` drops `authorized` | `slab-death` | harness rejects a surviving unauthorized spawn |
| `disposeChildren` drops the active-wave fatal | `slab-death` | harness rejects a surviving active disposal |
| Uncleared-slot check deleted | `slab-death` (debug) | harness rejects a surviving dirty spawn |
| Uncleared-slot check not under `version(assert)` | `slab-death` (`-release`) | harness rejects a dying release child |
