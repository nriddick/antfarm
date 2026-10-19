# C ABI 1

Status: implemented on `1.9.0-progress`, unreleased. The public transport is
defined by [antfarm.h](../../include/antfarm.h), the optional
[antfarm_runtime.h](../../include/antfarm_runtime.h), and the C++ ownership
adapter [antfarm.hpp](../../include/antfarm.hpp). Ring and ownership contracts
remain in [SPEC](../../SPEC.md). ABI 1 supports 64-bit Windows and Linux;
all exports and callbacks use C linkage.

## Distribution boundary

- **antfarm_core**: Farm, tokens, views, publication, table hooks, and fatal
  policy. Build with BetterC and link from a C main without druntime. Core-only
  threads need no D initialization or attachment. The callback guard uses
  native TLS in every build. libc, platform mapping APIs, and pthread on Linux
  are ordinary native dependencies.
- **antfarm_runtime**: optional druntime-inclusive non-Fiber worker pool and
  FarmHost. It depends on the same core instance: never embed a second copy
  of the Farm identity allocator or fatal-handler state. Runtime initialization
  and shutdown are explicit. Runtime-owned worker threads are attached;
  foreign threads entering runtime services must attach for their call lifetime.
- **Actors**: follow after core publication is stable. Compile-only BetterC
  evidence does not establish allocator independence. Split allocator selection
  and D-only typed/GC-rooted adapters before promising runtime-free actor exports.
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
`parallel sum = 523776`. Both examples are built by the ABI check runner.
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

## Runtime-inclusive pool and host

Call `af_runtime_init` on the thread that will control hosts. It takes one
balanced druntime initialization reference and warms topology discovery before
concurrent use. An embedding D program retains its own runtime reference.
Only this control thread creates, steps, registers host producers, closes or
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
single active worker-host limit still applies. Actor and Fiber APIs are absent.

`af_host_farm` returns a borrowed core handle. Never destroy it directly.
`af_host_register_producer` uses the extra slots, returning ordinary core
producer handles. Return them with `af_unregister_producer`; external views use
the ordinary core API too. Hosts refuse close while these participants remain.
Core writes keep their allocation-free, direct-callback path.

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
D host's emergency policy. Failed D hosts close through that abandonment path.

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
that no D attachment is needed.

The runtime checks cover caller-only and two indefinitely parked workers,
publication beyond ring capacity, busy shutdown, native thread attach/detach,
GC roots, explicit abandonment, foreign control rejection, and an already-initialized D embedding.
They require two accepted worker CPUs. `--runtime-only` selects this portion.
The runners bound subprocess lifetimes. Windows uses MSVC; Linux uses
`CC`/`CXX` (default `cc`/`c++`). Headers require C11 or C++11
(MSVC is checked in its C++14 mode).

The quick gate includes the core ABI alongside the existing BetterC module
checks. Full Linux and Windows integration also covers the optional runtime.
The full Linux gate additionally runs the native C/core test under TSan; use
`python3 scripts/check-c-abi.py --tsan` to select it directly.
No standalone C actor exports, Fiber exports, shared-library packaging, or
Rust/Zig ownership wrappers are promised. Split actor allocators/adapters before
adding actor exports; C hosts can already supply their own threads or use the
optional non-Fiber host above.
