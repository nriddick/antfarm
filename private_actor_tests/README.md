# Private actor correctness tests

```sh
make -C private_actor_tests run
make -C private_actor_tests run-dmd
make -C private_actor_tests run-tsan
make -C private_actor_tests run-death
make -C private_actor_tests run-death-dmd
```

Tests force ordinary pages and a 2 MiB Farm for ring reuse and backpressure;
performance measurements use 8 MiB. A same-thread publication audit hook
consumes each table after its sentinel release but before `writeTracked`
returns. Consumer reclamation frees terminal root state in that interval;
nested child waves also finish before their producer has sealed publication.
The hook compiles out of ordinary builds.

The suite checks bounded FIFO progress, exact callback counts, balanced frees,
allocation failures, accepted-prefix retry, zero admission, repeated roots,
parent authority, nonoverlapping child waves, three-level descendant joins,
terminal-wave restrictions, and four independent creator parks on one Farm.
Borrow/context/park ordinary copies and state-field access are compile-fail
assertions. They do not prove arbitrary `@system` code obeys the no-escape
contract; see [PRIVATE_ACTORS.md](../actors/PRIVATE_ACTORS.md).

`./private_actor_tests/private_actor_tests_tsan join-only` is the focused
same-shard memory-order regression. It forces two consumers into distinct
one-payload runs in one shard; both write plain values and the completion hook
reads them. The handshake before those writes is relaxed. There is no test
release after the writes that could hide a missing Farm join. Replacing the
completion `atomicFetchAdd!(MemoryOrder.acq_rel)(*shc, runlen)` with `raw`
produces a ThreadSanitizer data race in `joined`; the corrected code passes.

The public `actor_torture` suite additionally pauses a wave table notification
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
table while its emitter is still inside `writeTracked`. Parent observations
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
statements in `actors/private_actor.d`.

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
