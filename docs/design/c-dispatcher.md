# C dispatcher and GroupHost (2.1.0 development)

[antfarm_forkjoin.h](../../include/antfarm_forkjoin.h) and
[antfarm_groups.h](../../include/antfarm_groups.h) extend ABI 1 through the
optional `antfarm_runtime` archive. The runtime-free core and every existing
ABI 1 layout, status value and prototype are unchanged. The roadmap's
`antfarm_*` candidate names use the established `af_*` prefix in the actual
headers. Fibers have no C exports.

Build with `python3 scripts/build-c-abi.py --runtime`. A C main still uses
`af_druntime_init`/`af_druntime_shutdown`; a foreign native thread attaches
before entering these services and detaches before exiting. Core-only Farm
publication remains runtime-free. There is one process-wide worker pool;
FarmHost, GroupHost and the dispatcher cannot each own workers concurrently.

## First program

[cabi/examples/forkjoin.c](../../cabi/examples/forkjoin.c) computes the sum of
`[0, 1_000_000)` through a C reduction callback and prints `499999500000`:

```sh
python3 scripts/build-c-abi.py --runtime
cc -std=c11 -Iinclude -c cabi/examples/forkjoin.c -o .build/c-forkjoin.o
ldc2 .build/c-forkjoin.o .build/c-abi-ldc2-posix/libantfarm_runtime.a \
  .build/c-abi-ldc2-posix/libantfarm_core.a -of=.build/c-forkjoin
.build/c-forkjoin
```

`af_processors` enumerates eligible CPUs as `(group, processor, llc)` rows.
Pass a prefix to `af_dispatcher_start` to select the participant count: the
first CPU is the pinned calling thread, the rest are workers. An absent list
selects up to 128 eligible CPUs. Default options use one shared Farm; set
`farm_per_group=1` for one per selected LLC. The C options map to the D
[dispatcher options](forkjoin.md), including deque size, polling cadence,
steal preference, parking and overflow behavior. Start validates boolean,
size/version and reserved fields, clears the output on failure and contains
D exceptions. The runtime never takes ownership of the supplied CPU array.

## Continuation trees and lifetime

C embeds `af_node` first in its own stable frame, initializes it with
`af_node_init`, then supplies `run` and (for a forking parent) `join` callbacks.
The 32-byte private prefix contains a D `Node`; the following callbacks have
C linkage and run through explicit adapters. No C callback is cast to a
D-linkage function pointer. The declaration audit checks the entire C-visible
layout. The prefix is not an invitation to edit D scheduling state.

A leaf's `run` returns `af_complete(node)`. A parent initializes its children
and returns `af_fork(parent, children, count)`. Once published, it must not
access the parent or its children until their authorized continuation. The
last child's completion calls the parent's `join`, which observes every
child's results through the D pending-counter acquire/release chain. A null
join return completes the parent automatically. A nonnull return can continue
another fork from that parent. Calling `af_complete(parent)` inside `join`
would count completion twice and is a release-build fatal violation.

The node itself, callback code and application context must stay live until
completion. The pointer array passed to `af_fork` only needs to survive the
call; the pointed-to frames need to survive execution. A terminal join can
free its frame as its final access. A leaf must keep its header alive through
`af_complete`; the common policy is for the parent's join to free its children.
The final completion can run before submission returns. C memory does not
become a GC root merely by containing a D object pointer.

Node initialization, nonempty fork spans, participant ownership, self-child
forks, double completion within a callback and completion from a join have
release-build diagnostics. Raw pointer use-after-free, duplicate child
pointers, concurrent node reuse and arbitrary forged pointers remain invalid
C memory accesses, not recoverable errors. Callbacks must not throw, longjmp
or stop/destroy the running dispatcher. A callback can run a nested synchronous
root or range: that nested wait helps only until its own sentinel completes.

`af_run_root` requires a participant. A foreign attached thread uses
`af_root_completion_create`, `af_submit_root`, `af_wait_root`, then reuses or
destroys its completion. The completion has one exclusive owner; a synchronized
handoff is permitted. A pending completion cannot be resubmitted or destroyed.
`af_submit_detached` uses application completion or the dispatcher's stop fence.
These adapters reject foreign admission with no worker or with
`caller_consumes=1`: an idle subscribed caller can pin ring reclamation. A
foreign submit retries on backpressure; it has no cancellation/timeout result.

Stop requires the original control thread and quiescent external submitters.
It drains all admitted roots, joins workers, and retains a stopped handle for
`af_dispatcher_counters`. Counter reads while running return `AF_BUSY` because
the underlying D counters are only complete after participants leave. The 17
64-bit fields follow D `Stats` in the same order; the three hot counters need
`ForkjoinHotStats`, and `submits` remains reserved. Destroy the stopped handle
and every completion before runtime shutdown. A completed but unwaited handle
still needs `af_wait_root` after stop, then destruction.

## Callback ranges

`af_forkjoin_for` invokes `(context, index)` exactly once per element.
`af_forkjoin_reduce` takes POD result width, seed, leaf and join callbacks.
Both synchronously join their tree; attached foreign threads use submission
internally, while participants help locally.

For `[begin,end)`, split at `begin+(end-begin)/2` until each leaf has at most
`grain` elements. The shape depends only on the range and grain, matching the
D reduction. A leaf callback folds its interval in increasing index order
starting from seed, and a join combines left then right into distinct storage.
The empty range copies seed without invoking either callback. Seed and final
output may alias. Associativity and an identity seed are needed to agree with
a serial fold; fixed shape alone guarantees reproducible ordering even for a
non-associative operation. The tests deliberately use such an operation to
check the exact split at 1, 2 and 4 participants.

Result buffers are byte-copyable, at most 16-byte aligned, and must be fully
written. No destructors or variable-length result objects are implied.
Recursive frames come from the existing D frame pool; the synchronous root
uses native allocation so a foreign thread does not acquire a frame-pool slot.
Root allocation failure returns `AF_ALLOCATION_FAILED`; recursive frame
exhaustion has the D range API's fatal policy. Range arithmetic and byte-size
overflow are rejected before allocation. Map/scan/invoke/task-group and frame
allocator convenience exports remain possible additions, outside this roadmap
increment.

## GroupHost without the dispatcher

`af_group_resolve` returns the worker CPU plan. `af_group_start` installs the
GroupHost registry and runs a C body/stop pair on its workers. Group selection
supports automatic, all and one explicit LLC; nested host options specify
worker count, caller placement, SMT filtering, idle policy and Farm sizing.
The callbacks receive host, worker index, home LLC and application context.
They may obtain their Farm and token even before start returns.

A worker owns its consumer view and returns 0 when idle, 1 on progress. Its
stop callback unsubscribes and destroys that view. Pool startup is not a
barrier for entry into every user body: an immediate stop can precede a worker's
loop entirely, in which case it acquired no user resources and need not call
the user stop hook. Synchronize consumer readiness before the first
publication; start returning alone is not that application barrier. The pool's arm/recheck/wake
protocol still owns parking. `af_group_wake` publishes no work: it fences
prior publication and wakes the selected group, or every group for
`UINT32_MAX`. Call it after accepted writes and on retry/idle polls. Quiesce
wake callers before stopping workers.

`af_group_farm` lazily creates one native borrowed wrapper per distinct Farm,
under a host lock. Shared-Farm groups get the same wrapper. `af_group_token`
caches one small-tier C token per thread per Farm; repeat lookup allocates
nothing. The C token cache uses the same GroupHost participant sizing but
ordinary core token accounting, so busy-close detection sees foreign holders.
It does not duplicate a D `groupToken` reservation. Worker caches release after
the stop hook; foreign holders explicitly call `af_group_release_tokens`
before detach; control-thread cached tokens release during close. Borrowed
tokens must not be transferred or directly unregistered. Independently
registered core tokens retain their ordinary owning contract.

Close has two phases because worker views and foreign resources cannot be
reclaimed together:

1. Establish application completion, quiesce submitters and wakers, then call
   `af_group_stop_workers`. It joins workers and runs their stop hooks, keeping
   the Farms and registry alive. This also has a D `GroupHost.stopWorkers`
   entry point; repeated calls are harmless.
2. Return external views/tokens and dispose actors, waves and private parks,
   then `af_group_close`. It returns `AF_BUSY` while those resources remain;
   success destroys the Farms, restores affinity and consumes the handle.
   A busy attempt already releases control-thread cached tokens.

There is no inference that an empty poll means application completion, and no
GroupHost abandonment export. Runtime shutdown counts these service handles
and completion handles, in addition to the earlier FarmHosts and attachments.
DMD's runtime archive explicitly materializes Farm GC pointer metadata needed
by GroupHost's arrays; the BetterC core is still compiled exactly once.

## Validation

The regular runtime C lane builds and runs the example,
[continuation tests](../../tests/cabi/forkjoin.c) and
[GroupHost tests](../../tests/cabi/groups.c), including release death cases.
The C++ declaration audit includes both headers, callback prototypes, sizes,
alignment, offsets and every new ABI lock row. All previous lock rows must
remain identical. `python3 scripts/check-c-abi.py --runtime-only` runs the
runtime tests; `--compiler dmd` selects DMD. These lanes are part of the full
Linux gate and the existing native Windows C ABI lane. Execution on Windows
is a separate validation obligation, not established by compiling on Linux.
