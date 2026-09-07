# Performance harnesses

Build with `make -C perftest`. For a standalone Windows tail build from the
repository root:

```text
ldc2 -O2 -release -i -Ithreadpool/source -Iperftest perftest/tail.d antfarm.d antfarm_allocation.d -of=tail.exe
```

`tail` discovers topology through the repository's threadpool module. It assigns
one logical processor per physical core first, preferring faster efficiency
classes, then uses SMT siblings. The producer and optional mailbox poller use
the next entries. It prints every group:LP assignment and warns when the
requested layout reuses logical processors. `--no-pin` leaves placement to the
OS. An eight-consumer run is not inherently oversubscribed on every host.

Mid-drain, mailbox and burst runs discard a missed backlog window before
publishing the timed event. Those attempts increment `miss`, but do not enter
the latency histogram or consume its warmup count. A workload which consistently
misses the window may time out instead of reporting idle samples as backlog
latency. This does not guarantee that the backlog remains nonempty throughout
the subsequent timestamp and publication operations.

Historical `latency.txt` results predate these placement and sample-accounting
fixes. They must not be relabeled as measurements from the corrected harness.
Raw throughput uses a synthetic callback with batched counting, not useful
payload processing. For Windows huge-page comparisons set `ANTFARM_HUGE_PAGES=1`
and pass `--huge` from an account with the existing large-page privilege.
