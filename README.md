# Ant Farm

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish tables
of work into a shared ring; any subscribed consumer may claim them. It suits
unordered compute where the next available worker should take the work.
Use it alone, or add pinned workers, actors, and suspendable Fibers.

Start with [First payload](docs/guide/quickstart.md#first-payload) for a complete
single-threaded program, or [choose a work representation](docs/guide/choosing-work.md).
Before building on the Farm, read [Using Ant Farm well](docs/guide/using-it-well.md):
one page with the correctness and performance rules that matter most.
The current release is **1.8.1**; **1.8.2** (repository layout and documentation)
is in progress.
Public and private actors are supported, with the limits in their guides and
source/ABI stability subject to the [promotion gates](ROADMAP.md#actor-api-promotion-gates).
See [CHANGELOG.md](CHANGELOG.md) for release and compatibility notes.

## Quickstart (Linux)

You need DMD 2.112+ or LDC 1.42+. From a clone, run this public actor service:

```text
dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=/tmp/public_service
ANTFARM_HUGE_PAGES=0 /tmp/public_service 0
```

`ANTFARM_HUGE_PAGES=0` explicitly selects ordinary pages, which are also the
default; it overrides a Farm's huge-page request. The example commands in
the guides use it for reproducible setup.

Expected output, with exit status 0:

```text
public service: 36 ordered replies, balance=234, workers=0, closing-callback-count=0
```

The `0` consumes on the calling thread; `/tmp/public_service 2` uses two pinned
workers and prints `workers=2` with the same counts. Substitute `ldc2` for
`dmd` to use LDC. The [quickstart guide](docs/guide/quickstart.md) explains
publication, consumption, and completion and includes First payload as its
second program.

`write()` returns a published count; pop that prefix and retry. A false
`consumeNext()` is no completion signal: count your own completions. Contract
violations abort the process through `fatal()`; `canFit` and the `SUBSCRIBE_*`
codes provide recoverable checks on the core Farm path. The threadpool
`Director` supplies waiting. `FarmHost` needs `import threadpool.host;`;
`import threadpool;` does not re-export it.

## Choosing the work representation

| Your work is... | Use | Example |
| --- | --- | --- |
| short, `nothrow @nogc`, no identity | a payload | [iota_sum.d](examples/iota_sum.d) |
| a stable identity addressed by outside code, with an inbox | a public actor | [public_service.d](examples/public_service.d) |
| a phase over an externally chosen set of identities | an actor wave | [public_wave.d](examples/public_wave.d) |
| one conductor, a computation tree, and joined results | a private hierarchy | [private_tree.d](examples/private_tree.d) |
| tracked requests with deadlines and cancellation | private actors with tracking | [request_deadline.d](examples/request_deadline.d) |
| exceptions, allocation, or waits during work | a managed Fiber | [hello_fibers.d](ant_fibers/examples/hello_fibers.d) |

The [choice guide](docs/guide/choosing-work.md) owns the detailed comparison.
A public endpoint may front a private tree; their lifetimes drain separately.

## Read next

- [Documentation map](docs/README.md): the seven-step path from First payload
  to workers, representation choice, sizing, and local checks.
- [Examples](examples/README.md): complete actor programs and Windows commands;
  [threadpool](threadpool/README.md), [actors](antfarm/actors/README.md), and
  [Fibers](ant_fibers/README.md) are the package entries. The Fiber Dub package
  is `ant_fibers` (`dub test :ant_fibers`).
- [Using Ant Farm well](docs/guide/using-it-well.md): correctness and tuning rules,
  linked to their owning pages.
- [CONTRIBUTING.md](CONTRIBUTING.md): start with `make check-quick`; the full local
  Linux correctness gate is `make check`. There is no CI. Windows has a separate
  [native integration matrix](docs/development/testing.md#windows-x64-integration).

[ARCHITECTURE.md](ARCHITECTURE.md) describes the layers and sizing formulas;
[SPEC.md](SPEC.md) owns the Farm contract. [PERFORMANCE.md](PERFORMANCE.md)
contains measurements and methodology, and [ROADMAP.md](ROADMAP.md) tracks work.

Ant Farm is licensed under the Boost Software License 1.0.
