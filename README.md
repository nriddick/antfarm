<p align="center">
  <img src="docs/assets/antfarm-icon.png" width="160" alt="An ant beside a wooden signpost carved with tally marks">
</p>

# Ant Farm

Ant Farm is a fixed-memory M:N job distributor and concurrency framework,
implemented in D, with a C ABI and C++ ownership adapters. Producers publish
tables of work into a shared ring; any subscribed consumer may claim them. It suits
unordered compute where the next available worker should take the work.
Use it alone, or add pinned workers, actors, and suspendable Fibers.

The released version is the one line in [VERSION](VERSION); what each
release changed is in [CHANGELOG.md](CHANGELOG.md), including the
[C/C++ ABI](docs/design/c-abi.md).
Supported platforms are Linux x86-64 and Windows x64.

## Three rules

- `write()` / `af_write*()` returns how many payloads it published. Pop
  exactly that many and retry; `0` is backpressure, not failure.
- `consumeNext() == false` / `af_consume_next() == 0` means nothing is ready
  at this cursor now. It is not completion: count your own completions.
- Contract violations abort through `fatal()`. `canFit` and the subscribe
  codes are the recoverable checks on the core path.

[Using Ant Farm well](docs/guide/using-it-well.md) is the one page of
correctness and performance rules behind these, each linked to its owner.

## Choosing the work representation

Independent elements go in payload tables; a recursive tree runs on the
fork-join dispatcher; a service with an inbox is a public actor; work that
waits, throws or allocates is a Fiber. The table carries the exceptions.

| Your work is... | Use | Example |
| --- | --- | --- |
| short, nonthrowing work without identity | a payload | [c_core.c](examples/c_core.c), [iota_sum.d](examples/iota_sum.d) |
| a stable identity addressed by outside code, with an inbox | a public actor | [public_service.d](examples/public_service.d) |
| a phase over an externally chosen set of identities | an actor wave | [public_wave.d](examples/public_wave.d) |
| one conductor, a computation tree, and joined results | a private hierarchy | [private_tree.d](examples/private_tree.d) |
| recursive fork-join: divide and conquer, irregular trees, no identity | the fork-join dispatcher | [forkjoin_fib.d](forkjoin/examples/forkjoin_fib.d) |
| tracked requests with deadlines and cancellation | private actors with tracking | [request_deadline.d](examples/request_deadline.d) |
| exceptions, allocation, or waits during work | a managed Fiber | [hello_fibers.d](ant_fibers/examples/hello_fibers.d) |

The [choice guide](docs/guide/choosing-work.md) owns the detailed comparison.
The fork-join dispatcher (`import forkjoin;`, [forkjoin/README.md](forkjoin/README.md))
runs on the threadpool with one work-stealing deque per participant and the
Farm as its admission and overflow channel.
The D actor programs in [examples/](examples/README.md) run on a `FarmHost`
(`import threadpool.host;`), which runs one Farm's pinned workers on the
`GroupHost` and owns the token, the actor runtimes and the shutdown order.

## Start here

| | Entry path |
| --- | --- |
| D | [D first program](#d-first-program) below: ten lines on every core |
| C or C++ | [Native core and application threads](#native-core-and-application-threads) |

Building Ant Farm from source requires DMD 2.112+ or LDC 1.42+. Using the
built core requires only a native C/C++ toolchain: the core and the
public/private actors have no D runtime dependency and run on your
application's threads. The optional `FarmHost` and `GroupHost` libraries
include druntime; Fibers remain a D API.

## D first program

Save this as `first.d` in the repository root and run it with
`dub run --single first.d` (add `--compiler=ldc2` for LDC):

```d
/+ dub.sdl:
    dependency "antfarm:forkjoin" path="."
+/
import threadpool;
import forkjoin;
import std.range : iota;
import std.stdio : writeln;

void main()
{
    Dispatcher d;
    d.start();                 // the caller plus a pinned worker per eligible CPU
    scope (exit) d.stop();
    writeln("sum of squares: ", forkJoinReduce!((ulong x) => x * x,
        (ulong a, ulong b) => a + b)(iota(1UL, 257UL), 0UL, 16));
}
```

It prints `sum of squares: 5625216`. The dispatcher split the range in
halves down to 16 elements, idle workers stole subtrees, and the reduction
is bit-identical for any worker count. The [quickstart](docs/guide/quickstart.md)
explains what it relies on and then shows the same sum on the raw ring,
one thread, which is what the dispatcher does underneath.

Next, run a complete actor service on pinned workers:
`dub run --single examples/public_service.d -- 2` (`0` consumes on the
calling thread; `2` adds two pinned workers in one LLC). Every D example
carries the same kind of header; the direct compiler commands are in
[examples/README.md](examples/README.md#build). Then follow the
[documentation map](docs/README.md#run-something).

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

## Read next

- [Documentation map](docs/README.md): guides, reference, design pages and
  which page owns each rule.
- [ARCHITECTURE.md](ARCHITECTURE.md) describes the layers and sizing formulas;
  [SPEC.md](SPEC.md) owns the Farm contract; [PERFORMANCE.md](PERFORMANCE.md)
  has the measurements; [ROADMAP.md](ROADMAP.md) tracks work.
- [CONTRIBUTING.md](CONTRIBUTING.md): `make check-quick` for iteration, `make check`
  for the full local Linux gate. There is no CI; Windows has a separate
  [native integration matrix](docs/development/testing.md#windows-x64-integration).

Ant Farm is licensed under the MIT License ([LICENSE](LICENSE)). The
`forkjoin` subpackage's design derives from citor by Vitalii Shutov (MIT);
the acknowledgement is in the license file.
