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
  foreign threads entering runtime services other than wake/state must attach
  for their call lifetime.
- **Public actors**: implemented in the core with native C link/run evidence.
  The optional D GC-rooted allocator is excluded from BetterC builds.
  Waves are also runtime-free. The actor API refresh below precedes private
  root/child exports.
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
The C++11 wrapper supplies move-only `farm`, `producer`, and `consumer`.
Default construction leaves each empty in both exception-enabled and
exception-disabled builds. Use `farm::create(out, options)`,
`producer::create(out, farm, tier)`, and `consumer::create(out, farm)`;
all are nonthrowing status-returning factories. Outputs must be empty
(else `AF_BUSY`) and remain unchanged on failure. Consumer creation tries
subscription exactly once: `AF_RETRY` leaves it empty for caller-directed retry.
There is no implicitly blocking or throwing consumer constructor.

Participants retain their Farm through an atomic ownership control block,
allocated once per Farm with checked nonthrowing allocation. Allocation failure
destroys the newly created Farm and returns `AF_ALLOCATION_FAILED`. Ownership
increments are relaxed; the final acquire/release decrement joins earlier
participant releases before Farm destruction. The wrappers themselves remain
exclusive: concurrent mutation/destruction of one wrapper is invalid.
Publication and polling simply call the C exports without reference-count work.
Destructors release handles; join work before teardown.

When exceptions are enabled, `farm::make(options)` and
`producer::make(farm, tier)` are optional throwing conveniences. Their
`antfarm::error` contains the status and a status-specific message.
The declarations remain present in exception-disabled builds to keep class
definitions identical across translation units; calling them in those builds
is a link error. Use the status factories there.
Factory status is authoritative: errors detected by the C++ wrapper itself
(nonempty output or ownership allocation failure) do not replace the C thread
diagnostic. This replaces ABI 1's earlier unreleased throwing constructors.

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

Initialize options with `af_options_default` or `af_host_options_default`.
For both structures, ABI 1 requires `struct_size == sizeof(the structure)`,
`abi_version == AF_ABI_VERSION`, and zero reserved fields. Smaller and larger
sizes return `AF_INVALID`; the implementation checks the size prefix before
reading the rest of the structure. The caller still supplies valid, suitably
aligned storage; the size field cannot validate an allocation's actual extent.

Keep this exact-size rule for ABI 1. Version identifies the contract; size
independently catches a mismatched binding/layout before a full structure read.
It does not currently promise append-only compatibility. A future extension
must specify supported prefix sizes and defaults field by field, or change ABI
version. Accepting arbitrary shorter sizes with a zero-filled tail would leave
partial fields and required geometry undefined by the interface.
Numeric enum values and exported symbol names are defined by the ABI 1 headers.
Keep internal ring offsets private; the five-word Thead is not a C ABI promise.

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
It may pop messages, republish, or request retirement. Contexts must not escape
their callback or thread. The active-context diagnostic is best-effort: a later
callback can reuse the same stack address, so a saved pointer is invalid even
when the check accepts it. The adapter uses the
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

Foreign threads call `af_runtime_attach_thread` before runtime services other than
wake/state and `af_runtime_detach_thread` before exit. New attachments run D TLS constructors
and destructors; existing D thread attachments are borrowed and never detached.
Hosts are explicitly rooted while C owns their handles. The session cannot
shut down while a host or an ABI attachment remains. Init/shutdown are
serialized setup operations, cannot race other runtime services, and run on
the same thread. ABI 1 permits one session; restart after shutdown is rejected.

On Linux, druntime reserves process-wide GC suspend/resume signal handlers.
The supported LDC 1.42 and DMD 2.112 runtimes default to `SIGRTMIN` and
`SIGRTMIN + 1`, not `SIGUSR1`/`SIGUSR2`; use the linked runtime's configured
signals if an embedding application overrides them. Do not replace their
handlers or block them on attached threads, including the host's workers.
Account for inherited masks before starting the runtime, workers or attaching
foreign threads. This requirement comes from druntime; the runtime-free core
installs no such handlers and requires no attachment.

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
runtime lifecycle/control services require attachment.

Wake after every accepted table and on idle/retry polls: sleeping views can pin
completed epochs and prevent the producer reclaiming space. `af_host_step`
wakes workers and advances the caller view, reporting worker failures.
`af_host_wake` and `af_host_state(host, &state)` may be called by any native
thread without attachment while the handle remains live. Both return
`AF_INVALID` for null arguments; the state output is unchanged on failure.
Wake is a no-op on a non-open host. State also reports terminal states while
the handle is still owned after a cleanup error. No external caller may race
handle release: join/quiesce wake and state callers before close or abandon.
The D host's in-flight wake interlock does not extend the C wrapper's lifetime.
A blocking or consumer-control operation inside a Farm callback/hook is fatal.

Close requires the application's completion signals for all raw C work; it
cannot infer whether arbitrary application jobs are drained. Return extra
tokens, unsubscribe external views, and quiesce external users first. Close
joins workers and consumes the handle only on `AF_OK`. On an error the handle
remains owned. A D `FarmHostNotDrained` refusal maps to `AF_BUSY`, including
attachment checks beyond the C prechecks. Fix the precondition and retry; after a terminal cleanup
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

## Planned actor API refresh

This section is a proposal, not the current exported contract. Implement it
after the host/C++ contract fixes above and before private-root/child exports.
ABI 1 is unreleased: update the header, implementation, examples and tests
together instead of maintaining two actor interfaces. Keep the core/optional
runtime distribution boundary and existing wave joins.

### Runtime-owned actor kinds

Register an immutable kind descriptor once per runtime: handler, handler user
pointer, state byte size and alignment. Per-instance data belongs in actor
state. A kind belongs to exactly one runtime and remains addressable until that
runtime is destroyed; registration is a quiescent setup operation initially.
Use the runtime's allocator for registry storage.

Validate descriptor metadata at registration: nonnull handler, nonzero state
size, power-of-two alignment and representable allocation arithmetic. Alignments
above 64 require a custom allocator. Record the validated descriptor unchanged;
creation checks kind/runtime identity and per-call inputs without repeating
descriptor validation. Live registration or kind unloading is outside this
increment; handler/destructor code and user data must outlive their kind's uses.

Replace the unused eight-byte `ActorSlot.inboxPadding` with an internal kind
pointer. Preserve the 128-byte slot size and 64-byte alignment, and initialize
the pointer before the generation's release transition to idle. Typed D actors
use null there. Autonomous C dispatch obtains the kind from its pinned slot
through an internal context accessor; C wave dispatch obtains the kind through
an internal accessor on its pinned borrow, and the byte count from that borrow.
The proposed wave callback is
`(void *wave_user, const af_actor_kind *, void *state, size_t bytes)`.
Kinds are stable identities for interpreting mixed-kind waves, including kinds
from different runtimes on the same Farm. Neither dispatch needs a prefix before
the user's state. Clear the field only under exclusive reclamation, before
returning the slot.

### Sixteen-byte value owners

Use distinct public owner and submission-handle types, each a slot pointer
plus generation. Owners live in caller storage and need no allocated wrapper.
Successful reclaim clears the supplied owner value; copied values remain
capabilities for their recorded generation until made stale. Runtime destruction
still requires external callers to be quiescent: a generation does not protect
a pointer to an already destroyed slot array.

The current D reclaim implementation assumes one exclusive owner. Its acquire
load of the retired generation is followed by plain state reads and a free;
there is no claim CAS. Generation checking alone does **not** prevent two copied
owners concurrently freeing the same generation. Add an exclusive
`retired -> reclaiming` CAS before reading/clearing reclaimable fields.
Only the winner runs destruction, deallocates state, updates live/owner counts
and returns the slot. Recheck the generation in the CAS, and preserve wave
membership and closed-submission requirements. Losing calls report busy or
stale without touching state. A vacant matching generation is already reclaimed,
rather than indefinitely busy.

Reserve lifecycle phase 6 for `reclaiming` (retired is 5; the phase mask remains
`0x7`, with phase 7 unused). Wake, send and retirement treat it as terminal and
must not access state, schedule work or reopen admission. A competing reclaim
reports busy until it observes a stale/vacant identity.

Retirement also assumes an exclusive owner today. The idle fast path in
`driveActorRetirement` rechecks a closed, empty submission gate, then blindly
stores retired. Replace that store with a CAS from the exact joined lifecycle
word to retired, reloading and rechecking on failure. A delayed retire caller
must never overwrite a reclaimed or reused generation.

The terminal CAS alone is insufficient: `requestActorRetire` can be delayed
after marking retirement but before its untagged `closeSubmissionGate` call.
Another copied owner could retire, reclaim and reuse that slot first; the old
caller could then close the new generation's gate. Its debug inbox reads and
other final slot accesses need protection too. Hold a generation-bound,
exclusive owner-operation pin across the whole retirement request, acquired
before marking/closing and released only after its final slot access. Use the
unused lifecycle flag `0x40`; preserve it through every intervening lifecycle
transition, including callback retirement. Contending retire calls report
retryable busy. Reclaim may claim retired only with that pin clear, using the
same lifecycle CAS to exclude a concurrent pin acquisition. It keeps the slot
in `reclaiming` through destruction, then returns it as vacant to the free list. Document the
acquire/release edges and audit every lifecycle rebuild before enabling copied
owners; a separate generation check followed by an untagged gate write is not
sufficient.

Keep the normal D owner move-only. Runtime-scoped owner operations take the C
runtime explicitly, so provenance and reservation accounting do not depend on
reading a kind pointer after another caller has reclaimed it. Recognize a
canonical empty owner as stale first. Before any slot dereference, validate
the integer address against the runtime's slot-array
bounds and require `(address - base) % sizeof(ActorSlot) == 0`; return
`AF_INVALID` for foreign, out-of-range or interior-slot addresses. Alignment
to 64 alone is insufficient for a 128-byte slot. The runtime pointer itself
must be valid and live. Wake/send handles remain runtime-free capabilities:
forged handles are invalid memory accesses, while genuine stale handles remain
safe until runtime destruction.
The value fields themselves still require caller synchronization: safe copied
values do not authorize simultaneous writes to the same output object.

### Copy and initializer creation

Provide scalar copy and initializer forms, each implemented on the same
construction primitive as the corresponding batch form. Allocate final state
through the runtime allocator, initialize the kind pointer and final state,
then release-publish idle. Remove the temporary state allocation, zero-and-copy
pass and owner wrapper. Copy creation accepts byte-copyable state only.

Initializer creation receives final aligned storage, a caller context and the
logical input element index. It returns a status; it must not throw or longjmp.
The slot is unavailable to wake/send until initialization succeeds and the owner
is returned. No public reserve/commit state escapes the create call.

Placement-new also needs a destruction contract. Add an optional kind destructor
called exactly once by the winning reclaimer after retirement and before freeing
storage. It executes on whichever thread wins reclaim, outside allocator/free-slot
gates, and must not throw or assume the creating thread. It must not call actor
operations on its own actor. An initializer that reports failure must clean up
any partial construction itself; the library frees that storage without calling the kind
destructor. A successfully initialized object receives the registered destructor
on normal reclaim. Emergency abandonment retains it under the existing policy.

### Strided spans and batch results

Represent actor sets by first-element pointer, byte stride and count. Provide
const handle/owner input spans, mutable owner output spans and strided result
spans. Starting at an entity's embedded owner/handle field with stride equal to
the entity size requires no packing buffer. Validate alignment, minimum stride,
count/stride/address overflow and output capacity before mutation; empty spans
accept null pointers. Preflight checks require every batch creation output to be
an empty owner value. Non-overlap of input/output/result element bytes is a caller contract,
not a general strided-overlap check performed by the API. Their bounding spans
may overlap when they address different fields in the same entity array.
An in-place reclaim span is one mutable owner span, not two overlapping spans.
Stride validation does not establish backing allocation bounds; storage extent
and lifetime remain the caller's responsibility.

Batch copy/init creation uses one kind and returns a successful prefix plus the
stop status (complete, capacity, allocation failure or initializer failure).
Returned owners remain live on a partial result; the suffix remains empty.
Provide a starting logical index for initializer callbacks so retries preserve
indices after advancing the spans.

Take the free-slot gate once to detach an available prefix into a private chain,
then release it before allocation or any initializer/destructor callback.
Initialize and publish outside the gate. On failure return the unused reserved
suffix as a chain under one gate acquisition. Count in-progress reservations
until they are committed or returned. Capacity admission and `AF_FULL` must
account for reserved slots; runtime/host teardown must refuse while any
reservation is in flight, even before its first owner is returned. This avoids
both per-element gate entry on successful admission and holding a global gate
across caller code.

Batch wake, retire and reclaim attempt every element and report per-element
results, so one stale or busy member does not hide later outcomes. Structural
span errors reject the call before any action. Reclaim clears successful or
confirmed-stale owner outputs, leaving busy owners available for retry. Scalar
and batch calls share the same state-machine operations.

Also return an aggregate success count: queued/coalesced for wake,
requested/already-retired for retirement, and reclaimed/confirmed-stale (owner
cleared) for reclaim. Busy and invalid outcomes do not count. Wake/retire success
still means request acceptance, not callback completion or proven retirement;
the aggregate is not a join. Each input occurrence gets its own result, including
duplicate capabilities; mutations to the same owner storage remain exclusive.

Wave publication consumes a strided span through a nonallocating range. Preserve
the existing accepted-prefix/one-physical-table contract, membership rollback,
seal/drain obligation and finished-versus-ready distinction. Callers advance the
span by the accepted count. Do not introduce an implicit wait or all-or-nothing
transaction around the batch. The nonallocating range is an implementation
detail; the public surface is a span, with no pull-iterator/callback-range API.

### Caller-owned inbox messages

Expose a 32-byte, eight-byte-aligned storage type matching `ActorInboxNode`,
with size/alignment/offset assertions on both sides. Keep synchronization fields
opaque; C/C++ code uses initialization, availability, outcome and payload/tag
accessors rather than directly manipulating their representation. Do not expose
C/C++ atomic types whose layout could vary between compilers.

Replace allocating create/destroy with initialization of caller-exclusive
storage. Messages may be embedded in entities, arenas or stack frames whose
lifetimes cover the entire interaction. Once submitted they cannot move, be
copied or be reinitialized until ownership returns. Release completion remains
the last access to the node and its payload; reply wake snapshots the target
before release, preserving the existing final-access guarantee.

### Implementation and validation sequence

1. Introduce the retirement-operation pin, exact-word retirement CAS and exclusive
   reclaim claim, keeping the typed D surface move-only. Deterministically test
   retire/retire, retire/reclaim and reclaim/reclaim with duplicate capabilities,
   including drain-to-cancel escalation, a pause before gate closing, a pause
   before the terminal CAS, slot reuse and wave membership. Audit pin preservation
   through callback completion and every lifecycle transition; test new-generation
   wake/send after each losing old-generation call.
2. Add kinds and direct copy/init construction; test registration validation,
   mixed-kind waves, exact allocation sizes, initializer rollback and exactly-once
   destruction with the custom allocator, including reclaim on another thread.
3. Replace C owner wrappers and message allocation together with their examples
   and host lifetime accounting. Test foreign runtimes, out-of-range pointers and
   the 64-aligned interior of a 128-byte slot before accepting owner provenance.
4. Add strided spans and batch admission/operations, then wave publication over
   spans. Test embedded entity fields, mixed results, duplicate inputs, partial
   capacity, in-flight reservation accounting, per-operation success counts,
   allocation failure at each element and invalid span arithmetic.
5. Compare C and D lifecycle workloads with the same states and allocator:
   small/large state, scalar/batch, local/remote reclamation and mimalloc.
   Include several creating threads. Reuse the creation review's scratch
   `bench_c.c`, `bench_d.d` and `bench_proto.c` if available, after checking that
   workloads and completion/lifetime rules match. Count allocations/copies as
   well as time. There must be no native temporary
   or owner allocation per actor, and no message allocation by the API.
   Private cohort/slab exports follow separately and must retain their
   per-cohort storage advantage.

Run the normal BetterC, native C/C++, DMD/LDC, Linux/Windows and focused TSan
checks for each changed protocol. Record performance limits under the existing
optimization policy; do not infer C throughput from the typed-D wave benchmark.
