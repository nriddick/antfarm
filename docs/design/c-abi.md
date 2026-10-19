# C ABI 1

Status: implemented on `1.9.0-progress`, unreleased. The public transport is
defined by [antfarm.h](../../include/antfarm.h), public actors in
[antfarm_actors.h](../../include/antfarm_actors.h), the optional
[antfarm_runtime.h](../../include/antfarm_runtime.h), and the C++ ownership
adapter [antfarm.hpp](../../include/antfarm.hpp). Ring and ownership contracts
remain in [SPEC](../../SPEC.md). ABI 1 supports 64-bit Windows and Linux;
all exports and callbacks use C linkage.

## Distribution boundary

- **antfarm_core**: Farm, tokens, views, publication, table hooks, public actors,
  native actor allocation, actor waves, and fatal policy. Build with BetterC and link from a C main without druntime. Core-only
  threads need no D initialization or attachment. The callback guard uses
  native TLS in every build. libc, platform mapping APIs, and pthread on Linux
  are ordinary native dependencies.
- **antfarm_runtime**: optional druntime-inclusive non-Fiber worker pool and
  FarmHost. It depends on the same core instance: never embed a second copy
  of the Farm identity allocator or fatal-handler state. Runtime initialization
  and shutdown are explicit. Runtime-owned worker threads are attached;
  foreign threads entering runtime services must attach for their call lifetime.
- **Public actors**: implemented in the core with native C link/run evidence.
  The optional D GC-rooted allocator is excluded from BetterC builds.
  Waves are also runtime-free; private roots/children are the next increment.
- **Fibers**: outside ABI 1. A runtime-inclusive build need not expose Fibers.

The first runtime distribution includes the non-Fiber pool requested for C
hosts. A native pool implementation can follow independently; ABI 1 does not
promise a runtime-free FarmHost. Core users can provide their own threads now.

## Build and run

Build a static core archive with LDC (substitute `dmd` for DMD):

```text
python3 scripts/build-c-abi.py --compiler ldc2
```

The output is `.build/c-abi-ldc2-posix/libantfarm_core.a` on Linux or
`.build/c-abi-ldc2-nt/antfarm_core.lib` on Windows. `--output DIR` selects
another directory. The core contains only BetterC objects. A native C/C++
linker supplies libc and platform libraries, without a D runtime:

```text
cc -std=c11 -Iinclude examples/c_core.c .build/c-abi-ldc2-posix/libantfarm_core.a -pthread -o .build/c-core
ANTFARM_HUGE_PAGES=0 .build/c-core
```

In an x64 Visual Studio developer prompt:

```text
cl /nologo /std:c11 /MT /Iinclude examples/c_core.c .build/c-abi-ldc2-nt/antfarm_core.lib /Fe:.build/c-core.exe /link kernel32.lib advapi32.lib
```

The [core example](../../examples/c_core.c) publishes three C callbacks,
waits through a table hook, and tears down its handles. It prints `sum = 10`.
The C++11 wrapper supplies move-only `farm`, `producer`, and `consumer`;
producers and consumers retain the Farm through cold-path shared ownership.
Publication simply calls the C exports. Construction reports an
`antfarm::error` containing the status, including retryable subscription
failures. Destructors release handles; join work before teardown.

Add `--runtime` to build the separate runtime archive with the same source revision and D
compiler. Compile [c_host.c](../../examples/c_host.c) as C, then use that D
compiler's driver to link the runtime archive before the core archive. This
supplies matching druntime/Phobos libraries; neither core source nor core
archive is embedded in `antfarm_runtime`.

```text
python3 scripts/build-c-abi.py --compiler ldc2 --runtime
cc -std=c11 -Iinclude -c examples/c_host.c -o .build/c_host.o
ldc2 .build/c_host.o .build/c-abi-ldc2-posix/libantfarm_runtime.a .build/c-abi-ldc2-posix/libantfarm_core.a -L-lpthread -L-lrt -L-latomic -of=.build/c-host
ANTFARM_HUGE_PAGES=0 .build/c-host
```

On Windows, compile the C file with `cl /std:c11 /MT /c /Iinclude`; link its
object with `ldc2`, `antfarm_runtime.lib`, `antfarm_core.lib`, `kernel32.lib`
and `advapi32.lib`. The host example uses two workers and prints
`parallel sum = 523776`. All C examples are built by the ABI check runner.
Static distributions are supported in ABI 1; a DLL/shared-object loader
interface is not shipped. Link one core instance into the program.
Independently embedding core copies in multiple plugins is not supported.

## Representation and publication

D's `Callback` already has the exact pointer/count C signature; it returns
void and receives no payload header. Generated D helpers construct slices
locally. C callbacks enter the ring directly, with no D-to-C trampoline.
`TableCompletionCallback` also uses C linkage. Assert the hook's size (16),
context offset (0), and callback offset (8) in both the C header and D module.

Do not cast `af_entry[]` to `PayloadEntry[]`: D slices and the explicit C
pointer/count structs have different field order. A nonallocating forward
range maps each C entry to `PayloadEntry(call, body[0 .. body_words])`.
The common-body range similarly maps `af_body`; the fixed form advances a
contiguous block by `body_words`. That preserves the existing sizing and
emission passes without intermediate arrays.

All three forms carry `avg_cost` (0 through 7, default convention 3) and an
optional hook. Entries allow mixed callbacks; body arrays share one callback
with variable lengths; fixed publication supplies one common body width.
For zero-word bodies the fixed form permits a null body pointer and uses
`count` to publish empty callbacks. Validate every count/length multiplication
before pointer arithmetic. Null pointers are permitted only for zero-length
ranges. Body storage is copied before return; referenced objects must outlive
execution. The callback's ring pointer is borrowed only until callback return.

Return values count accepted payloads. Zero means no table was published and
no hook runs. A partial write covers only that prefix, and the caller retries
the suffix. The hook can run before the publication call returns. Reserve an
aggregate credit first, undo it on zero, and retain a producer credit until
all tables are submitted. A hook sees every callback's writes in its physical
table; raw reads of ring accounting do not establish that join.

The hook pointer/context must remain stable until notification. The Farm
snapshots both fields before calling; a hook may release or transfer its own
storage as its final access. Otherwise keep it alive through return. A table
hook does not prove that the Farm is drained or that another hook has returned.

Grains are the C caller's job: encode a subrange or start/count in the body and
loop inside the callback. Separate payloads may execute concurrently, while
one payload always executes exactly once on one consumer. There is no MT or
iteration-count interface. D's `writeTracked` has been retired; every D/C write
uses the same optional table hook.

## Handles, failures, and lifecycle

Handles hide noncopyable D values and permit internal representation changes.
Registration and view creation allocate handle storage; publication/consumption
remain allocation-free. Unregister destroys its token handle, and view destroy
requires an unsubscribed view. A token may transfer threads only after a
quiescent ownership handoff; a view has one exclusive owner and is never walked
concurrently or re-entered. The Farm outlives every registered token and view.

Subscribe-full, retry, and invalid map to distinct statuses. Creation uses
`tryCreate`: geometry/settings errors return `AF_INVALID`, allocation failure
returns `AF_ALLOCATION_FAILED`, and mapping/mutex failures return
`AF_SYSTEM_ERROR`. Capacity exhaustion
at producer registration is recoverable. Cross-Farm tokens, invalid
callbacks or cost classes, overflow, and impossible singleton payload sizes
retain the core's fatal contract; `af_can_fit` provides a nonfatal sizing query.
Released or forged handles and concurrent handle destruction are invalid C
memory accesses; the API cannot diagnose use-after-free. No D exception crosses C. Runtime wrappers catch exceptions and return a status
with a diagnostic from `af_last_error_message()` and `af_last_os_error()`.
Status-returning operations replace that thread's diagnostic; core queries and
hot publication/consumption do not. Messages are borrowed, NUL-terminated and
truncated to 511 bytes. Creation/registration clear valid output pointers on
failure. A null options pointer selects defaults.

Install the fatal handler before creating Farms or starting workers; replacing
it requires quiescence. A null handler restores the default. It receives a
borrowed message and may log or dump; it must not throw, longjmp, re-enter the
Farm, or return control to the failed operation. If it returns, the library
aborts. The setter configures the same fatal implementation used by the core.

`struct_size`, `abi_version`, and zero reserved fields make options validation
explicit. ABI 1 rejects unknown required layouts rather than guessing. Numeric
enum values and exported symbol names are defined by the ABI 1 headers. Keep
internal ring offsets private; the five-word Thead is not a C ABI promise.

## Runtime-free public actors

[antfarm_actors.h](../../include/antfarm_actors.h) exports public actors from
the same core archive. Include it from C or C++; no D runtime initialization
or native-thread attachment is needed. The [C actor example](../../examples/c_actors.c)
copies a counter state, sends three messages, drains retirement and reclaims the
owner. Build it with the same native command as `c_core.c`; it prints
`actor sum = 10`.

Create an `af_actor_runtime` over an owned Farm with a fixed slot capacity.
It starts no threads. Create actors with nonempty plain state, a power-of-two
alignment and a C handler. Creation copies the bytes; pointees and the handler's
`user` context remain application-owned. No destructor runs. The default
allocator supports alignment through 64; a custom `af_actor_allocator` supplies
C allocation/free callbacks and can support larger alignments. Its copied
context must outlive the runtime; callbacks must be thread-safe and may not
throw or longjmp. The allocator sees runtime and slot storage, and a state
allocation containing a private dispatch prefix before the aligned user bytes.
Free receives the exact allocated size/alignment. Wrapper handles and temporary
creation buffers use native malloc separately.

Creation returns `AF_INVALID` for invalid metadata, `AF_FULL` when all slots
are reserved, and `AF_ALLOCATION_FAILED` on allocation failure. Successful
creation returns an exclusive owner. Its 16-byte submission handle is copyable
and shareable, but has no authority to access state or reclaim it. A reclaimed
generation reports stale through wake/send until runtime destruction; all
handles expire when their runtime is destroyed. Forged pointers and handles
used after destruction are invalid C accesses.

Wake coalesces; flush publishes a bounded ready snapshot with the Farm's token
and cost class. Zero publication is not a completion signal. Within each
activation the C handler receives exclusive state and a callback-local context.
It may pop messages, republish, or request retirement. The adapter uses the
existing erased D dispatch with a small C callback bridge; raw Farm callbacks
still enter directly. No D actor state-machine or memory-order edge changes.

Messages are cold-allocated reusable nodes with a payload pointer and a tag.
An accepted send transfers ownership until completion, failure or cancellation.
Pop in producer linearization order; unpopped messages carry into the next
activation. A terminal operation is the final access to that message and its
payload: another thread may immediately reuse or free them after acquiring
availability. Read outcome/failure only after acquiring that interaction's
release; initial availability is not a reply. Failure codes are 24 bits.
The complete/fail/cancel-and-wake variants snapshot the target handle before
release, so the target may be stored in the released payload.

Request drain or cancel retirement, keep flushing/consuming until retired, then
reclaim the owner. A request result is not a retirement fence:
`AF_ACTOR_ALREADY_RETIRED` can mean cancellation was already requested, so
always query `af_actor_retired`. Successful reclaim consumes its C handle. There is no
guaranteed final handler invocation; release application resources after
retirement. Before destroying a standalone actor runtime, reclaim all owners
and join all consumers, senders and other runtime users. Retired actors and
zero live/ready counts do not prove that consumers have left the trampoline.
Destroy runtimes before their Farm. Runtime creation/destruction must be
outside callbacks. Detailed actor semantics remain in
[actor lifecycle](../reference/actor-lifecycle.md).

### GC adapter boundary

`ActorAllocator.gcRooted` remains available in normal D builds and is omitted
under BetterC, along with its GC helper functions. C actor state that only
refers to manually managed C/C++/Rust/Zig memory gains nothing from GC rooting.
The optional host uses druntime for its threads and GC-roots its own D host
object; that does not register C actor state as GC roots.

Mixed-language applications holding D GC-managed references in actor state
still need external D roots or a custom allocator backed by D root registration,
and appropriate druntime attachment for threads using those references.
The C ABI supplies no GC-rooted allocator export. Native allocations are the
default even when public actors run on the optional worker host.


## Runtime-free actor waves

Create an `af_actor_wave` bound to an owned or borrowed Farm, then begin a
generation with a C operation and its user context. Publish prefixes of C actor
handles with that Farm's token and seal exactly once. Actors must be idle and
may come from several runtimes on that Farm, with different state sizes and
alignments. The operation receives exclusive user state and its byte length;
it must interpret each layout correctly. It receives no actor context.

Publication allocates nothing. The bridge maps C handles through a range and
uses the existing wave reservation, rollback, membership and credit protocol.
The erased payload carries five words: slot, generation, wave, erased operation
and context. Typed D waves retain their three-word payload. No lifecycle atomic
ordering changes. The operation and user context remain live until finished.

A zero return without `failed` is empty input or backpressure. Consume work
(and step/wake a worker host) before retrying. A stale, active, duplicated or
wrong-Farm actor rejects the prospective physical table and sets `failed`.
Earlier accepted tables still execute: seal and drain them even on failure.
Membership persists through aggregate completion, so an actor cannot join an
overlapping unfinished wave even after its own table has completed.
`interrupted` separately reports observed control-path interference; completion
does not join any autonomous work handed off by that interference.

Copy `af_actor_wave_handle` values to observers. `finished` acquire-joins all
phase writes and membership release. It does not join the optional completion
callback. The C wrapper additionally supplies `ready`: the completion bridge
release-stores its final-access flag after the callback returns, and an acquire
observation joins that tail. Begin/reuse and destruction require readiness.
This is a stricter reuse rule than the underlying D descriptor permits, giving
C users an explicit lifetime fence. The copied hook can run inline in seal for
an empty or already-consumed wave. It must not wait for readiness or reuse or
destroy its own wave. Its context must survive readiness.

A begin invalidates older handles; their queries return false/zero rather than
historical results. A never-begun wave can be destroyed directly. Quiesce all
observers before destruction; afterward even stale handles are invalid memory
accesses. Destroy waves before their Farm and normal host close. Host close
returns `AF_BUSY` while a wave wrapper remains, even if ready. Explicit host
abandonment joins workers and retains unfinished wave storage along with the
ring and actor storage; all associated handles become invalid. Retained callback
code and contexts must remain resident with that abandoned work.

The [C wave example](../../examples/c_wave.c) runs two phases over 64 actors,
handles partial publication, joins a completion callback, and prints
`wave positions = 448, notifications = 2`. Build it with the core-only native
link command used for `c_core.c`. The same API works with externally supplied
native threads or actors created by `af_host_create_actors`; the optional host
is responsible for stepping/waking and joining its consumers.

## Runtime-inclusive pool and host

Call `af_runtime_init` on the thread that will control hosts. It takes one
balanced druntime initialization reference and warms topology discovery before
concurrent use. An embedding D program retains its own runtime reference.
Only this control thread creates hosts/host actor runtimes, steps, registers host producers, closes or
abandons hosts. These ownership checks remain active in release builds.

Foreign threads call `af_runtime_attach_thread` before runtime services and
`af_runtime_detach_thread` before exit. New attachments run D TLS constructors
and destructors; existing D thread attachments are borrowed and never detached.
Hosts are explicitly rooted while C owns their handles. The session cannot
shut down while a host or an ABI attachment remains. Init/shutdown are
serialized setup operations, cannot race other runtime services, and run on
the same thread. ABI 1 permits one session; restart after shutdown is rejected.

`af_host_options_default` chooses automatic workers in one LLC, one worker per
core, caller consumption, unchanged caller affinity, 1 ms parks, and one extra
small producer. Zero workers uses only the caller. The options expose geometry,
extra participants, idle policy and optional caller placement; the D pool's
single active worker-host limit still applies. Fiber APIs are absent.

`af_host_farm` returns a borrowed core handle. Never destroy it directly.
`af_host_register_producer` uses the extra slots, returning ordinary core
producer handles. Return them with `af_unregister_producer`; external views use
the ordinary core API too. Hosts refuse close while these participants or wave wrappers remain.
Core writes keep their allocation-free, direct-callback path.

Use `af_host_create_actors` for host-owned actor runtimes. `af_host_step`
flushes their ready queues and wakes consumers. Standalone runtime creation
rejects a borrowed host Farm; destroying a host-owned actor runtime directly
is fatal. Close returns `AF_BUSY` until every owner is reclaimed and the ready
queues are empty, then joins consumers before destroying actor storage and
the Farm. Core actor operations remain callable from unattached native threads;
only calls to runtime services require attachment.

Wake after every accepted table and on idle/retry polls: sleeping views can pin
completed epochs and prevent the producer reclaiming space. `af_host_step`
wakes workers and advances the caller view, reporting worker failures.
`af_host_wake` and `af_host_state` may be called by attached foreign threads
while the handle remains live. No external caller may race handle release.
A blocking or consumer-control operation inside a Farm callback/hook is fatal.

Close requires the application's completion signals for all raw C work; it
cannot infer whether arbitrary application jobs are drained. Return extra
tokens, unsubscribe external views, and quiesce external users first. Close
joins workers and consumes the handle only on `AF_OK`. On an error the handle
remains owned, so fix a busy precondition and retry; after a terminal cleanup
error a retry releases the terminal handle. `af_host_abandon` explicitly stops
and joins workers while retaining the ring and unfinished work, matching the
D host's emergency policy. This also leaks actor storage, outstanding owners
and allocator contexts; all associated C handles become invalid. Failed D
hosts close through that abandonment path.

## Validation and remaining work

```text
python3 scripts/check-c-abi.py --compiler ldc2
python3 scripts/check-c-abi.py --compiler dmd
```

`--core-only` checks native C/C++ linking without druntime, layouts, all write
forms, partial/zero admission, empty bodies, concurrent table joins, opaque
handle ownership, and release-build contract deaths. A separate audit build
uses the existing production-elided publication hook to force completion before
write returns. The core tests use native C threads specifically to verify
that no D attachment is needed. Public actor tests cover capacity reservations,
allocation failure/rollback, aligned copied state, coalescing/republishing, inbox
order/reuse/outcomes, retirement, stale handles, and creation and callback
execution on native threads. A production-elided actor completion seam frees
the message and its payload before the reply wake continues, testing the
adapter's final-access boundary for all three terminal outcomes.

Wave tests cover mixed layouts/runtimes, partial and zero admission, duplicate,
active, stale and foreign actors, rollback and persistent membership, native
parallel phases, generation invalidation, and completion before publish returns.
A paused hook proves that finished can precede ready, and release-build deaths
reject reuse/destruction during that tail. Ready joins the hook's ordinary writes.

The runtime checks cover caller-only and two indefinitely parked workers,
publication beyond ring capacity, busy shutdown, native thread attach/detach,
GC roots, explicit abandonment, foreign control rejection, and an already-initialized D embedding.
Public actors and waves are also driven by zero- and two-worker hosts, with GC collection,
busy close, joined destruction and explicit abandonment.
They require two accepted worker CPUs. `--runtime-only` selects this portion.
The runners bound subprocess lifetimes. Windows uses MSVC; Linux uses
`CC`/`CXX` (default `cc`/`c++`). Headers require C11 or C++11
(MSVC is checked in its C++14 mode).

The quick gate includes the core ABI alongside the existing BetterC module
checks. Full Linux and Windows integration also covers the optional runtime.
The full Linux gate additionally runs the native C/core, public actor and wave tests under TSan; use
`python3 scripts/check-c-abi.py --tsan` to select it directly.
Private roots/children, Fiber exports, shared-library packaging, and
Rust/Zig ownership wrappers remain outside the shipped surface. Public actors
can use application-supplied threads or the optional non-Fiber host.
