# Actor examples

Complete programs for the two actor models. Read these instead of the
test sources.

| Program | What it shows |
| --- | --- |
| [public_service.d](public_service.d) | POD ledger, caller-owned inbox nodes, queued/coalesced acceptance versus rejection, a two-node drain, replies after `available`, node reuse, admission close, idle retirement, stale handles |
| [public_wave.d](public_wave.d) | Externally selected actors from two runtimes on one Farm, and the completion-hook tail after `finished` |
| [private_tree.d](private_tree.d) | Typed result ports, root `waitChildren`, a child continuation fold, a subset wave, non-POD state with a destructor, drain to `live == 0` |

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

On Windows the linker writes `public_service.exe` (and the same for the other
programs). Ordinary pages are the default, so `ANTFARM_HUGE_PAGES` may be left
unset. In cmd.exe set it with `set ANTFARM_HUGE_PAGES=0` before the run.

`make -C examples check` is the Linux matrix: LDC and DMD, debug and release,
each run with 0 and 2 workers, plus an LDC ThreadSanitizer build of the same
threaded programs. The repository `make check` includes that target.
