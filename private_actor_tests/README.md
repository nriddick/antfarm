# Private actor correctness tests

```sh
make -C private_actor_tests run
make -C private_actor_tests run-dmd
make -C private_actor_tests run-tsan
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
