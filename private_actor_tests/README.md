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
