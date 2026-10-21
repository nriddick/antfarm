# Ant Farm

Ant Farm is a fixed-memory M:N job distributor and concurrency framework,
implemented in D, with a C ABI and C++ ownership adapters. Producers publish
tables of work into a shared ring; any subscribed consumer may claim them. It suits
unordered compute where the next available worker should take the work.
Use it alone, or add pinned workers, actors, and suspendable Fibers.

The current release is **1.9.0**; see the [release notes](docs/releases/1.9.md)
and [CHANGELOG.md](CHANGELOG.md), including the [C/C++ ABI](docs/design/c-abi.md).
Supported platforms are Linux x86-64 and Windows x64.

| Start here | Entry path |
| --- | --- |
| C or C++ | [Native core and application threads](#native-core-and-application-threads) |
| D | [D first program](#d-first-program) |

- **Building Ant Farm from source** requires DMD 2.112+ or LDC 1.42+.
- **Using the built core** requires an ordinary native C/C++ toolchain and
  platform libraries. The core and public/private actors have **no D runtime
  dependency** and can run on your application's threads. The optional
  non-Fiber `FarmHost` library includes druntime; Fibers remain a D API.

## Native core and application threads

Start with [c_core.c](examples/c_core.c) for one-thread publication and a
completion hook, or [cpp_threads.cpp](examples/cpp_threads.cpp) for two
`std::thread` workers consuming through the C ABI. The threaded example
subscribes one view per worker, retries partial writes, counts completed work,
and joins both threads before destroying the Farm. It needs no D runtime
initialization or foreign-thread attachment.

Build the core once, then compile and link your application with a native
toolchain. From the repository root on Linux:

```sh
python3 scripts/build-c-abi.py --compiler ldc2
c++ -std=c++11 -Iinclude examples/cpp_threads.cpp .build/c-abi-ldc2-posix/libantfarm_core.a -pthread -o .build/cpp-threads
.build/cpp-threads
```

It prints `native threads: 256 squares summed to 5559680`. The
[build guide](docs/design/c-abi.md#build-and-run) includes Windows commands
and the optional host library. [antfarm.h](include/antfarm.h) is the C interface;
[antfarm.hpp](include/antfarm.hpp) adds C++11 move-only ownership adapters.

## D first program

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

Every D example carries the same kind of header. Add `--compiler=ldc2` to Dub,
or substitute `ldc2` for `dmd`, to use LDC. Then follow the
[documentation map](docs/README.md#run-something): seven short steps from this
program to workers, representation choice, sizing, and local checks.

## Three rules

- `write()` / `af_write*()` returns how many payloads it published. Pop exactly that many and
  retry; `0` is backpressure, not failure.
- `consumeNext() == false` / `af_consume_next() == 0` means nothing is ready at this cursor now. It is
  not completion: count your own completions.
- Contract violations abort through `fatal()`. `canFit` and the subscribe
  codes are the recoverable checks on the core path.

[Using Ant Farm well](docs/guide/using-it-well.md) is the one page of
correctness and performance rules behind these, each linked to its owner.

## Choosing the work representation

| Your work is... | Use | Example |
| --- | --- | --- |
| short, nonthrowing work without identity | a payload | [c_core.c](examples/c_core.c), [iota_sum.d](examples/iota_sum.d) |
| a stable identity addressed by outside code, with an inbox | a public actor | [public_service.d](examples/public_service.d) |
| a phase over an externally chosen set of identities | an actor wave | [public_wave.d](examples/public_wave.d) |
| one conductor, a computation tree, and joined results | a private hierarchy | [private_tree.d](examples/private_tree.d) |
| tracked requests with deadlines and cancellation | private actors with tracking | [request_deadline.d](examples/request_deadline.d) |
| exceptions, allocation, or waits during work | a managed Fiber | [hello_fibers.d](ant_fibers/examples/hello_fibers.d) |

The [choice guide](docs/guide/choosing-work.md) owns the detailed comparison.
The D actor programs in [examples/](examples/README.md) run on a `FarmHost`
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
