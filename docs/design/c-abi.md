# C ABI 1

Status: ABI 1 shipped in Ant Farm 1.9.0. The public transport is
defined by [antfarm.h](../../include/antfarm.h), public actors in
[antfarm_actors.h](../../include/antfarm_actors.h), private actors in
[antfarm_private.h](../../include/antfarm_private.h), the optional
[antfarm_runtime.h](../../include/antfarm_runtime.h), and the C++ ownership
adapters [antfarm.hpp](../../include/antfarm.hpp) and
[antfarm_private.hpp](../../include/antfarm_private.hpp). Ring and ownership contracts
remain in [SPEC](../../SPEC.md). ABI 1 supports 64-bit Windows and Linux;
all exports and callbacks use C linkage.

## Distribution boundary

- **antfarm_core**: Farm, tokens, views, publication, table hooks, public actors,
  native actor allocation, actor waves, private roots/cohorts, and fatal policy. Build with BetterC and link from a C main without druntime. Core-only
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
  Waves and private roots/children are also runtime-free.
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
.build/c-core
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
(else `AF_BUSY`) and remain unchanged on failure. `af_subscribe` retries the
transient frontier race itself, so consumer creation returns only `AF_OK`,
`AF_FULL` or `AF_INVALID`; `AF_RETRY` stays registered but unused in ABI 1.
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

`antfarm_private.hpp` adds a move-only `private_roots` owner with the same
`create`/`make` rules. It retains its Farm like a producer and destroys the
park on scope exit, so drain roots and take tracked outcomes first, including
before unwinding. Its members forward to the park calls; callbacks and child
cohorts stay raw C.

Add `--runtime` to build the separate runtime archive with the same source revision and D
compiler. Compile [c_host.c](../../examples/c_host.c) as C, then use that D
compiler's driver to link the runtime archive before the core archive. This
supplies matching druntime/Phobos libraries; neither core source nor core
archive is embedded in `antfarm_runtime`.

```text
python3 scripts/build-c-abi.py --compiler ldc2 --runtime
cc -std=c11 -Iinclude -c examples/c_host.c -o .build/c_host.o
ldc2 .build/c_host.o .build/c-abi-ldc2-posix/libantfarm_runtime.a .build/c-abi-ldc2-posix/libantfarm_core.a -L-lpthread -L-lrt -L-latomic -of=.build/c-host
.build/c-host
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

`antfarm.h` registers every `af_status` value from all four headers and
reserves a range per header for future statuses, so extensions cannot collide.
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

Create an `af_actor_runtime` over an owned Farm with fixed capacity, then
register immutable kinds concurrently with actors or other registrations. A kind records handler/user, state size
and alignment, and an optional destructor. Copy or initializer creation writes
directly into allocator-owned final state and returns a 16-byte value owner.
No per-actor prefix, owner wrapper or temporary allocation remains. The default
allocator supports alignments through 64; custom allocators also supply runtime,
slot and kind storage, and can support larger alignment.

Owners and submission handles are distinct copyable capabilities. Only owners
authorize retirement/reclamation; copies arbitrate through a generation-bound
retirement pin and exclusive reclaim CAS. Mutations of one owner value require
caller synchronization. Stale capabilities remain safe only while their runtime
exists. See [the construction and ownership contract](#actor-construction-and-ownership)
for statuses, provenance, direct initialization, destructors and batch operations.

Wake coalesces; flush publishes a bounded ready snapshot with the Farm's token
and cost class. Zero publication is not a completion signal. Within each
activation the C handler receives exclusive state and a callback-local context.
It may pop messages, republish, or request retirement. Contexts must not escape
their callback or thread. The active-context diagnostic is best-effort: a later
callback can reuse the same stack address, so a saved pointer is invalid even
when the check accepts it. The adapter uses the
existing erased D dispatch with a small C callback bridge; raw Farm callbacks
still enter directly. The shared lifecycle arbitration also protects copied C owners.

Messages are caller-owned 32-byte nodes initialized with a payload pointer and tag.
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
always query `af_actor_retired`. Successful or confirmed-stale reclaim clears its supplied owner value. There is no
guaranteed final handler invocation; release application resources after
retirement, or use the registered destructor. Before destroying a standalone actor runtime, reclaim all actors
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
alignments. The operation receives the stable kind, exclusive user state and its byte length;
it must interpret each layout correctly. It receives no actor context.

Publication allocates nothing. The bridge maps C handles through a range and
uses the existing wave reservation, rollback, membership and credit protocol.
The erased payload carries five words: slot, generation, wave, erased operation
and context. Typed D waves retain their three-word payload. The reservation keeps the kind and state pinned. The operation and user context remain live until finished.

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

Call `af_druntime_init` on the thread that will control hosts. It takes one
balanced druntime initialization reference and warms topology discovery before
concurrent use. An embedding D program retains its own runtime reference.
Only this control thread creates hosts/host actor runtimes, steps, registers host producers, closes or
abandons hosts. These ownership checks remain active in release builds.

Foreign threads call `af_druntime_attach_thread` before runtime services other than
wake/state and `af_druntime_detach_thread` before exit. New attachments run D TLS constructors
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
Fiber exports, shared-library packaging, and
Rust/Zig ownership wrappers remain outside the shipped surface. Public actors
can use application-supplied threads or the optional non-Fiber host.

## Actor construction and ownership

ABI 1 shipped in 1.9.0. It replaces the earlier development-only allocating actor interface;
header, adapter, tests and examples use one contract. Private cohorts and slabs
use the separate header described below.

### Runtime-owned kinds and direct construction

`af_actor_kind_register` copies an immutable descriptor containing handler,
user pointer, state byte count, alignment and optional destructor. Registration
may overlap actors, construction and other registrations; each call has exclusive
output storage. Registrars must be joined before runtime destruction. A release
CAS publishes each fully initialized node onto an append-only list; teardown
acquires that list after external quiescence. Creation reads its supplied kind,
not the list. This removes the former `AF_BUSY` restriction without adding work
to the actor creation path.
Descriptors use the runtime allocator and remain stable until its destruction.
A nonnull handler, nonzero size, power-of-two alignment and representable
allocation arithmetic are required; alignment above 64 needs a custom allocator.

`ActorSlot.kind` occupies the former eight-byte padding word; slots remain
128 bytes aligned to 64. Typed D actors use null. A C autonomous callback obtains
the kind through its pinned context, and a wave through its pinned erased borrow.
No prefix precedes user state. Mixed-kind waves receive
`(wave_user, kind, state, bytes)`, including kinds from different runtimes on
one Farm.

`af_actor_create_copy` accepts byte-copyable state. `af_actor_create_init`
passes final aligned storage, caller context and logical index to a callback.
Both share construction machinery with their batch forms. There is no public
reserve/commit interval. On success the slot release-publishes idle after state
and kind initialization. An initializer returns `AF_OK` on success; any other
return becomes `AF_ACTOR_INITIALIZER_FAILED`. On failure it must clean up its
partial object; the library frees storage without invoking the kind destructor.

The optional destructor runs exactly once, on the winning reclaimer's thread,
after retirement and outside all allocator/free-slot gates. It must not throw,
longjmp or call actor operations on its own actor. Handler/destructor code and
user data outlive all uses of their kind. Emergency abandonment deliberately
retains allocations and does not run destruction.

### Value owners and retirement arbitration

Owners and handles are distinct 16-byte types: slot pointer plus generation.
An owner is caller storage, with canonical empty value `{0}`. Copies may retire
or reclaim concurrently; mutation of a particular owner object remains exclusive.
Successful or confirmed-stale reclaim clears that object. Runtime destruction
still requires all users, including stale-handle/owner callers, to be joined.

Before dereferencing an owner slot, a runtime-scoped operation checks its integer
address range and `(address - base) % sizeof(ActorSlot) == 0`. The 64-byte-aligned
interior of a 128-byte slot is invalid. Foreign/interior/out-of-range owners return
`AF_INVALID`; canonical empty owners are stale. Runtime and kind pointers must
themselves be valid and live. Submission handles remain runtime-free capabilities,
so fabricated handle pointers are invalid accesses.

The shared D implementation reserves lifecycle phase 6 for reclaiming and flag
0x40 for an exclusive retirement-operation pin. Lifecycle rebuilds preserve
the pin until the requesting call releases it. The request fuses its mark and pin
into one CAS before closing admission. Idle retirement's terminal release store
(checked by an exact-word CAS in debug builds) also releases the pin as its final
slot access; scheduled/running paths use a
separate final unpin. Contention
returns `AF_ACTOR_RETIRE_BUSY`. Reclaim requires retired, no operation pin and
no wave membership, then exclusively claims the generation before accessing or
freeing state. The complete proof is in
[public actor ordering](public-actor-ordering.md#retirement-and-reclamation).
Normal D owners remain move-only.

C retirement outcomes occupy values 16–20 and reclaim outcomes 32–34, disjoint
from `AF_INVALID`. Use symbolic constants. `af_actor_retired(runtime, owner, out)`
returns a validation status separately from its Boolean observation. Stale or
reclaiming identities report false. When copied owners may reclaim concurrently,
retry reclaim itself and handle busy/stale instead of waiting exclusively for
that Boolean. Request acceptance and batch success counts are not retirement joins.

### Strided sets and partial results

Handle/owner inputs, mutable owner outputs, state inputs and result outputs use
pointer/byte-stride/count spans. Empty spans accept null. Nonempty spans validate
alignment, minimum stride, multiplication/address overflow and output capacity
before mutation. Creation additionally preflights empty owner outputs. Storage
extent/lifetime and non-overlap of actual element bytes are caller obligations;
bounding spans may overlap for different fields of an entity array.

Batch copy/init returns `af_actor_batch_result {count, status}`: a live successful
prefix and `AF_OK`, `AF_FULL`, `AF_ALLOCATION_FAILED`, or
`AF_ACTOR_INITIALIZER_FAILED`. Invalid structure returns `AF_INVALID` with no
actions. The suffix remains empty; advance spans and the initializer's
`first_index` when retrying.

Slots are reserved in bounded chunks, so other creators and reclaimers may
run between chunks. The entire available prefix is reserved before allocations
or initializers run, which happen outside any gate. A failed creation returns
its unused reservation before the call returns. Reservations consume capacity
and prevent runtime/host destruction before an owner is returned.
`af_actor_runtime_live` counts occupied capacity, including the whole reserved
prefix, through successful construction and retirement until the slot is
returned. Failed construction returns the unused suffix and its count
together. Capacity that a reclaim call (scalar or batch) releases is available
to creators when that call returns. The chunk size, the free-slot structure
(today a linked list walked under one gate) and the counter's publication are
implementation details, not ABI promises; the [free-index stack
redesign](../../ROADMAP.md#next) may change them without changing this contract.
A zero count is not a runtime-lifetime fence: shutdown also checks the released
gate and empty ready queue, and requires external users to be joined.

Batch wake/retire/reclaim attempts every input occurrence and stores every result.
Its aggregate count includes queued/coalesced wakes, requested/already-retired
retirements, and reclaimed/confirmed-stale owners. Busy and invalid outcomes do
not count. Reclaim uses one mutable owner span and leaves busy owners for retry.
None of these totals is a join.

Wave publication uses a nonallocating strided range and retains its accepted
prefix/one-physical-table behavior, membership rollback, seal/drain obligation,
and finished-versus-ready distinction. The C wave example keeps initial state,
owners, handles and results inside entity structs.

### Caller-owned messages

`af_actor_message` exposes 32 opaque bytes aligned to eight, with C/C++ and D
layout assertions. `af_actor_message_init` initializes caller-exclusive storage;
there are no allocating message create/destroy exports. Messages can live in
entities, arenas or stack frames whose lifetimes cover the interaction.
Synchronization words remain opaque, with no dependency on C atomic layouts.

After successful send, do not move, copy or reinitialize the node until ownership
returns. Completion remains its final access; reply wake snapshots its target
before release. Access payload/tag/outcome only under the documented ownership
and acquire rules.

### Validation and measurement

The suites cover forced retirement/reclaim pauses, copied-owner races, slot reuse,
wave membership, mixed-kind identity, direct placement-new, destructor uniqueness,
foreign/interior owners, embedded spans, partial capacity, every allocation-failure
position, initializer rollback, malformed arithmetic and in-flight host admission.
The runtime-free archive audit and native C threads remain part of the normal
C ABI gate. C++ checks run with exceptions enabled and disabled.

Lifecycle timing compares the same state sizes and allocator across D scalar,
C scalar and C batch creation, local/remote reclaim and multiple creators.
Measurements and their limits belong in the performance report; correctness
results do not imply a throughput improvement. The
[initial refresh record](../performance/archive-2026-10-20.md#actor-c-abi-refresh-190-progress)
and [lifecycle optimization record](../performance/archive-2026-10-20.md#public-actor-lifecycle-optimization-190-progress)
include the C/D matrix, allocation counts, committed-reference gates and native
Windows comparisons, with their limits.

## Runtime-free private actors

[antfarm_private.h](../../include/antfarm_private.h) exports private root parks
and parent-owned child cohorts from the same BetterC core. The
[C example](../../examples/c_private.c) runs a slab-backed cohort, retires its
members, and reads their joined results; it prints `private result = 30`.
Use the native build command for `c_core.c` with this source instead.

A root park copies one state descriptor (byte size, alignment and optional
consumer-side destructor), root callback, and user pointer. Root spawn either
copies byte-copyable state or calls an initializer in final aligned storage.
The initializer's logical index is passed through unchanged. Failure leaves no
admitted actor and calls no destructor: the initializer must clean its own
partial object. There is no per-root prefix, owner wrapper, or temporary state
allocation. One C handle and the D park metadata are allocated per park.

Every park operation is single-emitter and non-reentrant, including observation.
The emitter must keep calling `af_private_roots_pump` while roots wait on child
work. It returns both root activations and total tables, so child-only or resume
publication can trigger worker notification. A zero count is not a join. Root
steps return again, finish, wait-children, or finish-after-children; the existing
[private custody protocol](private-actor-ordering.md) determines their scheduling
and destruction. The strict finish form rejects active descendant work.

With tracking enabled, spawn may return a 16-byte cancellation handle. Copies
may request cancellation from native threads without attachment; all cancellers
must be joined before park destruction. Handles grant no state access or wake
capability. Terminal roots retain capacity until the emitter takes their
outcomes. A successful take joins the root's application writes and invalidates
that generation's handles. Failure takes priority over cancellation. Deadlines
use the native monotonic ticks and frequency exported by this header; they are
lazy observations, not timers or wake sources. BetterC uses CLOCK_MONOTONIC on
Linux and QueryPerformanceCounter on Windows, without D module initialization.
Normal D builds retain MonoTime semantics.

Root parks choose state storage the same way: `af_private_root_options.storage`
is per-object (the default) or a cohort slab, with `slab_byte_limit` as the
required cap on `capacity * roundUp(state_bytes, state_alignment)`. A slab
root park reserves that slab once at creation, after its metadata, so
`af_private_root_spawn_copy` and `spawn_init` never allocate; the terminal
release clears the slot where the reclamation option would have freed it, on
the emitter at collection or on the consumer in the final activation, and
`af_private_roots_destroy` frees the slab once. An unset cap, an unknown
storage value, a nonzero `reserved` word or an overflowing product return
`AF_INVALID`; a slab exceeding its set cap, or any allocation failure, returns
`AF_ALLOCATION_FAILED` with every earlier allocation unwound.

Only a current private callback may create child cohorts. Each cohort copies
its state descriptor and result geometry, inherits the hierarchy's allocator,
and belongs to that parent until automatic post-order disposal. Parents cannot
borrow child state. Per-object storage allocates at spawn; cohort slabs reserve
`capacity * roundUp(state_bytes, state_alignment)` bytes at creation under an
explicit byte cap. The default cap of zero is unset and returns `AF_INVALID`;
exceeding a set cap returns `AF_ALLOCATION_FAILED`. Every slab child initializes directly in its slot. Failed
initialization clears the slot without admitting it. Terminal retirement calls
the destructor and clears the full stride; refill starts only after the retiring
wave joins. The slab is freed once at cohort disposal. No per-member allocation,
prefix, or wrapper is introduced on the slab path.

A child wave chooses its callback/user pointer, cost class, retirement flag, and
leaf or continuation mode. Leaf callbacks return done. Continuations may return
child-wait, then run again after their descendants join; terminal continuations
also support deferred consumer cleanup. Whole waves can retire members; subsets
cannot. Subsets copy unique spawn indices into eagerly reserved selection
storage and return results in selection order. Empty or rejected starts retain
the previous operation, results and outcome. Publication allocates nothing.

Result byte size zero selects a signal-only cohort. Otherwise the cohort
reserves aligned result storage once; callbacks receive their final slot and
must write a byte-copyable value before returning done. Result slots have no
library destructor and cannot contain pointers into retiring child state.
Joined results expose pointer/byte-stride/count, remain valid after terminal
member retirement, and expire at the next nonempty start or cohort disposal.
They may be read only within the authorized parent's callback. Cancellation
exposes the dispatched prefix; failure counts and the first failure's code and
participant position are available through the wave outcome. Cancellation,
subtree cancellation and peer cancellation use the existing credit protocol;
they do not cut a join short.

Private contexts and state/result pointers cannot escape a callback. The native
TLS context check diagnoses ordinary wrong-thread or expired-context use, but
stack-address reuse cannot make a retained context valid. Initializers run with
private context authority disabled. Allocator hooks and destructors must not
re-enter the hierarchy. The callback and destructor code, users, allocator
context and application-owned output storage must outlive their uses.

Both owned Farms and borrowed host Farms accept private parks. These parks are
application-owned: the optional host does not pump or destroy them. Pump with
an exclusive producer token and call host wake/step on publication and retry
polls. Drain the park (taking all tracked outcomes), join cancellers and park
users, destroy it, and return external tokens/views before normal host close.
An allocated private park makes host close return AF_BUSY, even when empty.
Emergency host abandonment invalidates associated handles and retains unfinished
hierarchies and their contexts, as it does public actors.

Validation covers native C threads, both root free policies, nested results and
continuations, subset order, truncation cleanup, signal-only cohorts, cleared
slab reuse, all creation-allocation failure positions, failed initializers,
stale cancellation handles, lazy deadlines and release-build contract deaths.
C++ checks construct noncopyable objects in final storage, including 128-byte
alignment, and drive the park through `private_roots`, with exceptions enabled
and disabled. The default C ABI lane runs
both release and debug private tests; its Linux TSan lane includes native C
private actors. Host tests use zero and two workers plus an unattached native
emitter. Shared-library packaging, GC allocator exports and Fibers remain outside
this ABI. These checks establish behavior, not a C/D throughput equivalence.
