# Ant Farm

Ant Farm is a fixed-memory M:N job distributor for D. Producers publish tables
of work into a shared ring; any subscribed consumer may claim them. It suits
unordered compute where the next available worker should take the work.
Use it alone, or add pinned workers, actors, and suspendable Fibers.

The current release is **1.8.2**; see [CHANGELOG.md](CHANGELOG.md) for the
1.9 work in progress, including the [C/C++ ABI](docs/design/c-abi.md).
Ant Farm needs DMD 2.112+ or LDC 1.42+ on Linux x86-64 or Windows x64.

## First program

One thread, one file, one import. Save
[First payload](docs/guide/quickstart.md#first-payload) as `first_payload.d` in
the repository root and run it with Dub (its header names this clone as the
dependency) or with the compiler directly:

```text
dub run --single first_payload.d

dmd -g -i -I. first_payload.d -of=first_payload
./first_payload
```

It prints `first payload: 256 squares summed to 5625216`; the
[quickstart](docs/guide/quickstart.md) explains what it relies on.

Next, run a complete actor service on pinned workers. The argument `0`
consumes on the calling thread; `2` adds two pinned workers in one LLC.

```text
dub run --single examples/public_service.d -- 2

dmd -g -i -I. -Iexamples -Ithreadpool/source examples/public_service.d -of=public_service
./public_service 2
```

Every example carries the same kind of header. Add `--compiler=ldc2` to Dub,
or substitute `ldc2` for `dmd`, to use LDC. Then follow the
[documentation map](docs/README.md#run-something): seven short steps from this
program to workers, representation choice, sizing, and local checks.

## Three rules

- `write()` returns how many payloads it published. Pop exactly that many and
  retry; `0` is backpressure, not failure.
- `consumeNext() == false` means nothing is ready at this cursor now. It is
  not completion: count your own completions.
- Contract violations abort through `fatal()`. `canFit` and the subscribe
  codes are the recoverable checks on the core path.

[Using Ant Farm well](docs/guide/using-it-well.md) is the one page of
correctness and performance rules behind these, each linked to its owner.

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
Every actor program in [examples/](examples/README.md) runs on a `FarmHost`
(`import threadpool.host;`), which owns the Farm, the token, the pinned
workers and the shutdown order.

## Read next

- [Documentation map](docs/README.md): guides, reference, design pages and
  which page owns each rule.
- [ARCHITECTURE.md](ARCHITECTURE.md) describes the layers and sizing formulas;
  [SPEC.md](SPEC.md) owns the Farm contract; [PERFORMANCE.md](PERFORMANCE.md)
  has the measurements; [ROADMAP.md](ROADMAP.md) tracks work.
- [CONTRIBUTING.md](CONTRIBUTING.md): `make check-quick` for iteration, `make check`
  for the full local Linux gate. There is no CI; Windows has a separate
  [native integration matrix](docs/development/testing.md#windows-x64-integration).

Ant Farm is licensed under the Boost Software License 1.0.
