# C ABI proposal

Status: proposal and implementation work on `1.9.0-progress`, branched from
`1.8.3-progress` at `207cd94`. Scope revised 2026-10-19.
The interfaces below are proposed, not shipped. This document owns the proposed
C boundary; the [specification](../../SPEC.md) and existing lifecycle documents
continue to own the underlying behavior.

## Recommendation

Ship two explicit distributions with the same core C API:

| Distribution | Contents | Runtime requirement |
| --- | --- | --- |
| `antfarm_core` | Farm, producer tokens, consumer views, single-shot ST payload writes and ST table completion | BetterC; C runtime and platform libraries only |
| `antfarm_runtime` | The same core API, plus the existing non-Fiber worker pool and `FarmHost` | druntime and the required Phobos dependencies; explicit initialization |

Use `antfarm.h` for the common API and `antfarm_runtime.h` for the optional
runtime APIs. Start with static libraries for Windows x64 and Linux x86-64.
The ABI version is independent of the Ant Farm release number.

Initially these are alternative complete link targets: an application links
one of them. The runtime distribution includes exactly one implementation of
the core. Do not link a BetterC core and a second normally compiled core into
the same process. That would duplicate Farm issuance, fatal policy and other
module state, and would complicate handle ownership. A separately loadable
runtime add-on sharing one core can follow a dedicated linkage proof; it is
not a prerequisite for this separation.

The non-Fiber pool belongs in the initial C ABI program of work, immediately
after the core. Actors follow with a runtime-free allocator path. Fibers remain
outside the first ABI. A pool that itself needs no druntime is a separate
extension, not an existing property of the non-Fiber pool.

### ST-only core

The first 1.9 milestone removes MT payloads from the shared core, including
the D API. Every payload executes once on one consumer; independent payloads
can execute concurrently across all workers. Parallel work is expressed as
independent entries or grains, with an explicit positive grain in the ST
table helpers. The existing compact and uniform layouts become the only
layouts. Full ring headers, the MT index and per-payload iteration counters
are removed rather than reserved in the C ABI.

This is a breaking D source and binary change. The native descriptor now
contains only body length and callback, and the callback has no iteration
argument. The C ABI will build on this simpler representation. Production
experience with ST batches motivates the scope; benchmark intended workloads
before making comparative performance claims.

## What the feasibility summary establishes, and what it misses

The central conclusion is sound: the Farm is close to a useful native library
boundary. `nothrow @nogc` helps, but does not by itself prove runtime independence.
[BetterC](https://dlang.org/spec/betterc.html) restricts runtime-dependent language
features; imported declarations can still introduce unresolved runtime symbols.
A real C link is the acceptance test.

Inspection and disposable Windows probes at the 1.8.3 base found:

| Claim | Evidence and consequence |
| --- | --- |
| Core and three actor modules compile with BetterC | Confirmed with LDC 1.42 and DMD 2.112 on Windows x64. This alone does not instantiate every template or prove a usable library. |
| The current core links without druntime everywhere | Not yet. The Windows huge-page path references `core.sys.windows.winnt.SE_LOCK_MEMORY_NAME`, a data symbol supplied by druntime. Use a module-local wide literal for the Windows privilege name. |
| The core needs a large rewrite | No evidence of that. After that one substitution in a scratch copy, a native MSVC C executable linked and ran core operations built by both D compilers, with no D libraries supplied. |
| All actor functionality is runtime-free | Too broad. `actor.obj` contains `gc_addRange` and `gc_removeRange` references from `ActorAllocator.gcRooted`. Exclude or split this adapter in the BetterC build; do not rely on dead stripping to hide it. |
| Concrete publication forms | Entry arrays and fixed-width broadcast each accept an optional completion hook. The hook covers one accepted physical table; the D `writeTracked` spelling no longer needs a separate C operation. |
| Non-Fiber means runtime-independent | False for this pool: it creates `core.thread.Thread` instances and uses classes, GC allocations and exceptions. It can still be exposed through an ordinary C ABI. |
| The new parallel API belongs automatically in the BetterC closure | Not established. Its blocking paths call `Thread.yield`; range templates and completion adapters need separate instantiation and linkage tests. |

The successful probe instantiated entry-array writes, fixed-length broadcast
writes, tracked ST writes and an MT payload, then consumed, unsubscribed,
unregistered and destroyed the Farm. Nine callback invocations produced the
expected sum and exactly one tracked notification. Both C-linked executables
reported success; dependency inspection showed Windows and Microsoft C runtime
DLLs only. Linker dead stripping was disabled. This was an assertions-enabled,
single-thread feasibility check using D-defined payload callbacks behind one
exported C function. It does **not** establish direct C callback compatibility,
actor linkage, release behavior, concurrency or Linux support. The scratch
substitution is not a production source change.
The probe's MT case records the 1.8.3 baseline. It must be replaced with ST
grain coverage when proving the 1.9 build.

Sources for these findings: [core](../../antfarm/package.d),
[actor allocator](../../antfarm/actors/actor.d),
[parallel adapters](../../antfarm/parallel.d),
[pool](../../threadpool/source/threadpool/pool.d),
[worker creation](../../threadpool/source/threadpool/worker.d), and
[host](../../threadpool/source/threadpool/host.d).

## Core boundary

### Callbacks and compatibility

Make the native callback type C-compatible at its definition, including its
arguments. The proposed C signature is:

```c
typedef int64_t (*af_callback)(const af_payload_header *header,
                              const uint64_t *body, size_t body_words);
typedef void (*af_completion_callback)(void *context);
```

The corresponding D aliases use `extern(C)`, `nothrow` and `@nogc`. C linkage
and explicit pointer/count arguments are necessary; D slices have no direct C
type equivalent. See [D's C interface rules](https://dlang.org/spec/interfaceToC.html).
Retain the existing 64-bit callback return type in this migration; it must not
be mistaken for a new cancellation or error protocol.
Change both payload and completion callback types together. Update generated
thunks in `templates`, `parallel` and the actor modules, handwritten callbacks,
examples and tests. A D thunk can reconstruct a slice without allocation.
Generated thunks should retain one direct call into the specialized user
function, while a C callback occupies the ring's Call field directly. Verify
multiple template instantiations link without C-symbol collisions.

This is a D source and binary compatibility change, not a free change for all
D callers. Carry it explicitly in the `1.9.0-progress` work and document the
callback migration before release; a branch name alone is not an ABI promise.
The ST-only milestone already removes MT execution and its iteration argument.
Keep the current D `create` overload for source compatibility: adding C options
does not require replacing the D constructor. Keep this work on
`1.9.0-progress`, separate from the ongoing `1.8.3-progress` branch.

Recommend one callback representation over maintaining both C and D callback
tags in every table. A temporary trampoline can prove interoperability, but
should not become the performance baseline or the permanent compatibility
mechanism. The claimed cost of an extra indirect call must be measured, not
inferred from the 50 ns workload figure.

### Header and data representation

For ABI 1, mirror the new 16-byte payload descriptor: a 64-bit body-word count
at offset 0 and the C callback pointer at offset 8. Assert size, alignment and
both offsets in C and D on supported 64-bit targets. Reject incompatible
packing. Producers supply the callback; the library supplies the copied body
length. There are no scheduling controls, runtime counters or reserved cache
lines in the descriptor.

The callback's entire header is read-only and borrowed only until return.
It always refers to consumer-local metadata, never a ring header. This
representation is proposed for the C boundary; the current native D callback
still takes a D body slice until the C-linkage milestone lands.

Bodies remain arrays of 64-bit words, with lengths in words, never bytes.
Keep pointer transport explicit via `uintptr_t` on the supported 64-bit targets.
An application context can be encoded in body words; no hidden allocation or
unrequested context field is needed in the ring header. The copied body does
not extend the lifetime of an object addressed by those words.

Define a C entry as a header pointer plus body pointer and word count. Implement
an allocation-free D forward-range adapter over this array; do not cast it to
`PayloadEntry[]` and assume D slice layout. Use another small range adapter for
contiguous fixed-length bodies. These adapters preserve common-header and
fixed-length information so the uniform ST fast paths still apply.

### Options, errors and handles

Provide opaque `af_farm`, `af_token` and `af_view` handles. Library allocation
and matching release happen during setup/teardown; publication and consumption
must not allocate wrapper objects. Caller-provided placement storage can be a
later API if measured setup costs warrant it.

Use options structures containing `struct_size`, fixed-width fields and
reserved zero fields. `af_options_init(options, sizeof *options)` writes only
the caller-provided extent. Creation accepts a defined minimum prefix, fills
missing optional fields with defaults, and rejects unsupported flags or
nonzero unknown extensions. Never return an options struct by value or require
callers to know nine positional arguments. Use C integer flags rather than
compiler-dependent enum storage or C++ `bool` fields.

Map `tryCreate` failures to fixed-width `af_status` codes with optional
caller-owned error output: geometry, environment setting, mutex, mapping and
allocation failures already have separate causes. Preserve the OS error code
and copy diagnostic text into a bounded buffer. Do not return D strings,
exceptions, GC pointers or a process-global last-error buffer.

Producer exhaustion and subscription retry/full/invalid remain recoverable.
Keep an explicit accepted-count result for writes: zero is backpressure, and a
positive result is the accepted prefix. `canFit` is a non-reserving snapshot,
not a guarantee that a later write will succeed or remain valid after consumer
growth. Existing invalid-payload, provenance and lifetime violations remain
fatal; wrapping them in C does not make them recoverable.

Proposed operation groups, rather than a target of forty functions:

| Group | Initial operations |
| --- | --- |
| Discovery | ABI version and feature bits; no runtime initialization required for these queries |
| Farm | options initialization, geometry check, create, destroy, can-fit |
| Producer | register, unregister/release; tier and Farm association retained internally |
| Publication | Entry array and common header over contiguous fixed-word bodies; optional completion hook on both |
| Consumer | allocate, subscribe, consume-next, unsubscribe, free |
| Diagnostics | configure fatal handler during exclusive setup |

Only complete accepted entries transfer to the ring. Descriptor and body input
storage need survive the write call, while callback code and pointees must
survive all accepted work. An optional table hook and context follow the existing
address-stability contract, and may be invoked before the write returns.
No table accepted means no notification, including an empty input. Retrying an
unpublished suffix requires a separate live hook or explicit aggregate
accounting; one hook must not be mutated or reclaimed while an earlier table can call it. An immutable hook/context pair may be shared by several tables if its callback safely counts each notification.

### Ownership, completion and fatal policy

One token has one active producer. Transfer between threads requires an
application synchronization handoff. A view has one consuming owner; do not
share it concurrently or reenter it from a callback. An opaque pointer is not a
reference-counted handle: copying its address does not create another owner.
Detectable misuse should fail consistently in debug and release; arbitrary
dangling pointers cannot be made safe by this ABI.

`consume-next == false` is a cursor observation, never a global completion or
drain test. Join batches through optional table notifications with explicit
aggregate accounting, or an application completion protocol with the required
release/acquire ordering. Every table finisher joins its payload callbacks;
raw progress is still not a global drain or a join on a running hook. Preserve the current shutdown sequence: establish
application completion, stop and join participating threads with their views
unsubscribed, unregister tokens, then destroy the Farm. See
[lifecycle contracts](../reference/actor-lifecycle.md).

Add an `extern(C)` fatal handler receiving context, message pointer and byte
length. Configure it before creating Farms or starting concurrent library use;
its context must outlive that use. It runs on the failing thread and is a
logging opportunity, followed unconditionally by `abort`. It must not throw,
longjmp, reenter Ant Farm or attempt recovery. Keep a fallback diagnostic.
Initially this covers explicit Ant Farm contract failures; compiler-generated
assertion/bounds failures can still go directly to the compiler's failure path.
Do not promise that one hook intercepts every process failure.

## Runtime-inclusive worker pool and host

Expose the existing pool in two useful forms through `antfarm_runtime.h`:

1. `af_pool`: the raw non-Fiber worker pool. Supply C work/stop callbacks with a
   context pointer and a borrowed worker-info structure, plus options for CPU
   selection, SMT filtering, QoS and idle behavior. Expose resolve/start,
   wake-all, stop/join and copied resolution/failure information. Initially
   expose a whole-pool idle policy instead of the complete Director/Selection
   interface, labels, bins and exchanges.
2. `af_host`: a Farm plus its workers and control-thread operations. Expose
   options, resolve/create, a borrowed Farm handle, extra producer registration,
   wake, step, wait with a C predicate and timeout, close, explicit abandon and
   final handle release. Actor attachment operations follow the actor ABI.

There is currently one live `CacheAwarePool` per process. An `af_host` with
workers consumes that slot; two pools, or a pool and a worker-backed host, must
return a clear busy status. Serialize first topology discovery in the wrapper.
Preserve control-thread ownership of host operations and pool setup/teardown.
For initial C defaults, leave caller affinity unchanged unless the caller opts
in; still expose the existing host placement choices explicitly.

The raw pool owns no job queue. Its shutdown stops and joins workers; it does
not drain application work. The work callback reports work/idle, and stop runs
on the worker so it can unsubscribe its view. A C callback adapter at this pool
pump boundary is acceptable initially and must be measured separately from
the direct per-payload callback path.

The host has no background emitter: `step`/`wait` or an extra producer must
publish. Keep parked workers' wake protocol intact. Writes through a borrowed
Farm need `af_host_wake` when appropriate; an empty `consume-next` does not
authorize sleep or shutdown. Do not give C callers ownership of the host's
control token or Farm. Extra producer handles must register/unregister through
host accounting even when they use common core write functions.

### Runtime lifecycle and cross-boundary failures

Provide `af_runtime_acquire`/`af_runtime_release` and explicit foreign-thread
attach/detach guards. Acquire on the application control thread before runtime
APIs; pair each successful acquisition with release after all hosts, pools,
attached foreign threads and runtime-owned handles are finished. The wrapper
must balance its own initialization references and refuse premature release.
It must not terminate a runtime still owned by the embedding application.
[D's runtime API](https://dlang.org/phobos/core_runtime.html) already provides
paired initialization/termination operations; hide their details behind the
C contract and pin the supported compiler/runtime combination.

Foreign threads entering runtime-bearing APIs must attach once for their
participating lifetime. Pool-created D threads are already registered. Track
whether the wrapper attached a thread so it never detaches someone else's
registration. Attachment must also handle applicable D TLS constructors and
destructors: raw `thread_attachThis` does not run those constructors, as the
[thread API](https://dlang.org/phobos/core_thread_osthread.html) documents.
Initially require attachment for all operational calls into the runtime
distribution, even calls using its common core API; any thread-safe exemption
needs an explicit audit. The BetterC distribution requires neither runtime
initialization nor thread registration.

Keep GC-managed host/pool objects rooted behind opaque handles; a raw pointer
stored only in C memory is insufficient. Setup may allocate and trigger GC.
Do not describe the runtime-inclusive library as GC-free merely because its
work callback is `@nogc`.

No exception may unwind into C. Map expected D exceptions to statuses such as
resolution failure, startup failure, timeout, busy, not drained and worker
failure, with copied diagnostics. Timeout leaves the host open. A refused close
can leave it open, while an error reported after teardown can leave it closed;
expose state so callers can distinguish these. Unexpected `Error`/`Throwable`
at the boundary is fatal, not an ordinary recoverable status. C++ callbacks
must not throw; internal catch blocks cannot make foreign unwinding supported.

Keep `abandon` explicit: the existing operation stops workers but deliberately
leaks the Farm and undrained owned state. It is not an alias for free, cancel or
successful drain. A C++ destructor must not conceal it behind normal teardown.

### If a runtime-free pool is desired later

Add a native-thread pool above `antfarm_core`, using platform thread creation,
explicit storage and status returns. Audit topology discovery, worker startup,
TLS, wake/park handshakes, stop/join and failure reporting; replacing
`new Thread` alone is insufficient. Reuse proven wait primitives where their
dependency closure allows it. Keep this as a separately estimated milestone.
The first runtime-free release can already use application-owned native threads.

## Actors and language bindings

Public actors come before private actors/waves. Build the C boundary around
existing erased handles/owners and their generation/lifecycle checks. State
needs size **and alignment**, execution/destruction callbacks, allocator
ownership and a borrow valid only during activation; `void *state, size_t size`
alone does not express these obligations. Explicitly adopt C linkage for every
cross-boundary allocator and actor callback. Keep GC-rooted allocation solely
in the runtime-inclusive distribution. Test actual creation, execution,
retirement and reclamation rather than just compiling modules.

A small move-only C++ wrapper is the first binding deliverable. It preserves
explicit completion and shutdown, pairs allocation/release and prevents copies
of tokens/views. Zig can consume the C header. A Rust wrapper follows after the
contracts stabilize: token transfer may support `Send`, but `!Sync` alone is
not a complete proof. Callback/context lifetimes, referenced data, Farm
ownership and host control-thread restrictions determine the safe traits.
Do not promise thread-safety from bindgen alone.

## Delivery and acceptance

| Milestone | Concrete work | Required evidence |
| --- | --- | --- |
| ST foundation | Remove MT payloads, simplify native headers/callbacks, add grain parameters to ST helpers and hooks on every write | Native D suites, exact execution, partial admission, tail and grain checks; preserve completion and reclamation ordering |
| 0: dependency boundary | Dedicated BetterC source list; fix Windows constant; isolate GC-rooted actor adapter; establish the two distribution builds | Native C compile/link/run without D libraries for core; imports/symbol audit; runtime build contains only one core |
| 1: core ABI | C callback migration, public header, options/errors, opaque handles, two write forms with optional hooks, fatal hook, C example | Direct C callbacks and completion hooks; all layout assertions; null-callback and sizing checks on every C write; ownership/backpressure/lifetime checks; ST and grain regression suites |
| 2: pool and host ABI | Runtime lifecycle, foreign-thread guards, rooted handles, C pool hooks and host control API, exception translation | C-owned control thread and extra producers; topology/start failure; parked wake races; worker failures; timeout; refused close; ordered stop/join; single-pool exclusion |
| 3: integration | C++ ownership helpers, build/install examples, usage and compatibility notes | C11 and C++ builds; both distributions consume the same common header; toolchain/runtime packaging documented |
| 4: actor ABI | Runtime-free public actors, then private actors and waves | Representative erased-state instantiations, allocator pairing, generation checks, retirement/reclamation, no GC references in core distribution |

MT payloads are removed before the C ABI; no MT milestone is planned.

Add a small native C link-and-run lane to `check-quick` when the first public
header lands, plus the equivalent Windows runner lane. Keep runtime tests
separate so a successful runtime build cannot mask a core dependency leak.
Use explicit source lists rather than compiling all of `antfarm/`, which now
also includes the parallel module and optional allocator integrations.

Before declaring ABI 1 stable:

- Test LDC and DMD, assertions-enabled and release, on Windows x64 and Linux
  x86-64. Compile C with the platform C toolchain, and link without implicit D
  libraries in the core lane. Inspect imports and unresolved symbols as well
  as running the executable; absence of a druntime DLL alone is insufficient.
- Exercise real C callbacks in compact ST and both uniform ST table layouts;
  zero-length bodies; partial publication; zero acceptance; producer and
  consumer exhaustion; foreign tokens; malformed options; allocation failure;
  early tracked completion; callback/header/body lifetime; and fatal paths in
  child processes. Check null callbacks and invalid sizing in every C write
  form, and false from the C can-fit query.
  Cover mixed callbacks, grain tails, partial admission and multi-consumer exact execution.
- Run native-thread producers/consumers with wrap/reclamation pressure, then
  targeted Linux TSan and the applicable repository correctness gates. Include
  a C host that invokes GC in the runtime build while its opaque handles remain
  live, to verify rooting and attachment.
- Compare D versus C entry points with identical ST data and callback work.
  Check tiny entries, grain sizes, fixed-length broadcast and pool-driven work
  across consumer counts. Measure worker utilization and throughput for the
  intended embarrassingly parallel workloads. Recheck the uniform-table
  optimization and measure wrapper/pump overhead;
  do not advertise a zero-cost ABI before this comparison.
- Test ABI major mismatch, options prefix compatibility, reserved-field
  rejection, all exported symbol names and caller packing checks. Document
  static CRT/runtime choices and forbid cross-library frees.

The original two-to-three-day core estimate is plausible for a prototype. It
does not cover the runtime pool/host lifecycle or the cross-platform acceptance
matrix. Plan and review by these milestones; estimate the pool and actor work
after their respective linkage and ownership probes. The first implementation
slice is the ST foundation. Next comes milestone 0 and a direct-C-callback
smoke test, before broad wrappers or language bindings freeze the interface.
