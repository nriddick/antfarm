# FarmHost

`farmhost` runs actors on pinned [threadpool](../threadpool/README.md) workers
without writing the worker hooks yourself. A `FarmHost` owns what every actor
program needs around its actors:
- one Farm sized for its participants;
- the calling thread's producer `Token`;
- pinned pool workers that consume the Farm, placed on one physical group
  (LLC) by default;
- the runtimes and parks it creates;
- a progress loop with an opt-in deadline;
- the documented shutdown order.

It is the dub subpackage `:farmhost` with the single module `farmhost`
(`import farmhost;`), and depends on `antfarm` and `threadpool`. `threadpool`
itself does not depend on Ant Farm. Supported targets are those of
`threadpool`: Windows x64 (Windows 10 1803+) and Linux x86-64 with DMD 2.112+
or LDC 1.42+.

## Use it

```d
// fragment: the host lifecycle around your actors
auto host = new FarmHost(2);    // two pinned workers in one LLC
scope (exit) host.closeOrAbandon();

auto runtime = host.createRuntime(1);
// create actors, send, then host.waitUntil(...) and host.retire(owners...)
```

Without dub, build with `-I. -Ithreadpool/source -Ifarmhost/source` and `-i`,
as every program in [examples/](../examples/README.md) does.

[Run actors on a FarmHost](../docs/guide/hosting.md) is the guide: a complete
program, placement, progress and waking, deadlines and failures, teardown and
tuning. Every public symbol is listed in the
[API reference index](../docs/reference/README.md).

## Commands

```text
dub test :farmhost --compiler=ldc2
dub run :farmhost -c host-test --compiler=ldc2
dub run :farmhost -c wakefail --compiler=ldc2
```

`dub test` runs the pure placement tests on synthetic snapshots, on any host.
`host-test` is the live regression: it needs two accepted worker CPUs in one
LLC (one per core by default), or three for a free caller CPU, and prints SKIP
and exits 0 otherwise; `make check` treats that SKIP as a failure unless
`ALLOW_HOST_SKIP=1`. `wakefail` races `wake()` against a worker-failure
shutdown. The sources live in [tests/farmhost/](../tests/farmhost/).
