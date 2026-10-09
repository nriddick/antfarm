# Actor examples

Complete programs for the two actor models. Read these with
[the lifecycle guide](../docs/reference/actor-lifecycle.md), not the test sources.

| Program | What it shows |
| --- | --- |
| [public_service.d](public_service.d) | POD ledger, caller-owned inbox nodes, queued/coalesced acceptance versus rejection, a two-node drain, replies after `available`, node reuse, admission close, idle retirement, stale handles |
| [public_wave.d](public_wave.d) | Externally selected actors from two runtimes on one Farm, and the completion-hook tail after `finished` |
| [private_tree.d](private_tree.d) | Typed result ports, root `waitChildren`, a child continuation fold, a subset wave, non-POD state with a destructor, drain to `live == 0` |
| [request_deadline.d](request_deadline.d) | Tracked root per request, a per-request deadline, cancellation from another thread, and terminal `takeOutcome` statuses |
| [fail_fast_fold.d](fail_fast_fold.d) | `context.fail` + `cancelPeers` fail-fast, the wave aggregate after the join, and explicit root propagation |
| [fiber_cancel_at.d](fiber_cancel_at.d) | Example bridge from `FiberDomain.sleepUntil` to `PrivateRootHandle.requestCancel` (not library API) |

[example_host.d](example_host.d) is scaffolding: a same-thread consumer or a few druntime workers, plus a progress deadline. It is not a production scheduler. It does not treat `consumeNext() == false` or `pump() == 0` as idle.

`iota_sum.d` remains the threadpool payload example. It is not part of this actor lane.

## Build

From the repository root, with DMD or LDC. The same command works on Windows
x64 and Linux x86-64. Pass `0` to consume on the calling thread, or a worker
count to use that many druntime threads:

```text
dmd -g -i -I. -Iexamples examples/public_service.d -of=public_service
ANTFARM_HUGE_PAGES=0 ./public_service 0

ldc2 -g -i -I. -Iexamples examples/public_wave.d -of=public_wave
ANTFARM_HUGE_PAGES=0 ./public_wave 2

dmd -g -i -I. -Iexamples examples/private_tree.d -of=private_tree
ANTFARM_HUGE_PAGES=0 ./private_tree 0
```

The outcomes and cancellation programs use the same command:

```text
dmd -g -i -I. -Iexamples examples/request_deadline.d -of=request_deadline
ANTFARM_HUGE_PAGES=0 ./request_deadline 0

dmd -g -i -I. -Iexamples examples/fail_fast_fold.d -of=fail_fast_fold
ANTFARM_HUGE_PAGES=0 ./fail_fast_fold 0
```

The fiber `cancelAt` adapter links the separate fibers package, so add the
fibers and threadpool import paths:

```text
ldc2 -g -i -I. -Iexamples -Ifibers/source -Ithreadpool/source \
    examples/fiber_cancel_at.d -of=fiber_cancel_at
ANTFARM_HUGE_PAGES=0 ./fiber_cancel_at 0
```

On Windows the linker writes `public_service.exe` (and the same for the other
programs). Ordinary pages are the default, so `ANTFARM_HUGE_PAGES` may be left
unset. In cmd.exe set it with `set ANTFARM_HUGE_PAGES=0` before the run.

`make -C examples check` is the Linux matrix: LDC and DMD, debug and release,
each run with 0 and 2 workers, plus an LDC ThreadSanitizer build of the same
threaded programs. `fiber_cancel_at` runs under TSan with 0 workers only,
because TSan fails at the stack switch for migrating DRuntime Fibers; see
[fibers/DESIGN.md](../fibers/DESIGN.md#validation-boundary). That target also extracts the complete First payload program
from the repository README and runs it with LDC and DMD in debug and release.
Other README D blocks are fragments and are rejected by the extractor unless
they say so. The repository `make check` includes that target.
