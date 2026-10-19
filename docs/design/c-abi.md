# C ABI proposal for 1.9

Status: proposed exports; the callback types and single-shot D surface below
are implemented. No stable C library has been shipped yet. This document owns
the distribution boundary and proposed ABI 1 declarations. The ring and
ownership contracts remain in [SPEC](../../SPEC.md).

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

## Proposed core header

All functions and callbacks use C linkage. Types use fixed-width integers
except native pointers, lengths, and counts. ABI 1 initially supports 64-bit
Windows and Linux, matching the Farm's current implementation profile.
Declarations below describe the interface, not an installed header.

```c
#include <stddef.h>
#include <stdint.h>

typedef struct af_farm af_farm;
typedef struct af_token af_token;
typedef struct af_view af_view;

typedef void (*af_callback)(const uint64_t *body, size_t body_words);
typedef void (*af_completion_callback)(void *context);
typedef struct af_completion_hook {
    void *context;
    af_completion_callback call;
} af_completion_hook;

typedef struct af_entry {
    af_callback call;
    const uint64_t *body;
    size_t body_words;
} af_entry;
typedef struct af_body {
    const uint64_t *words;
    size_t count;
} af_body;

typedef uint32_t af_status;
enum { AF_OK, AF_INVALID, AF_FULL, AF_RETRY, AF_ALLOCATION_FAILED };
typedef uint32_t af_tier;
enum { AF_SMALL, AF_BULK };

typedef struct af_options {
    uint32_t struct_size;
    uint32_t abi_version;
    uint32_t ring_mib;
    uint32_t segments;
    uint32_t expected_consumers;
    uint32_t max_bulk_producers;
    uint64_t bulk_quota_words;
    uint32_t max_small_producers;
    uint32_t small_table_threshold;
    uint64_t small_quota_words;
    uint32_t huge_pages;
    uint32_t reserved;
} af_options;

uint32_t af_abi_version(void);
void af_options_default(af_options *out);
af_status af_create(const af_options *options, af_farm **out);
void af_destroy(af_farm *farm);
uint32_t af_can_fit(const af_farm *farm, af_callback call,
                    size_t body_words, af_tier tier);
af_status af_register_producer(af_farm *farm, af_tier tier, af_token **out);
void af_unregister_producer(af_farm *farm, af_token *token);

size_t af_write(af_farm *farm, const af_entry *entries, size_t count,
                af_token *token, uint32_t avg_cost,
                const af_completion_hook *completion);
size_t af_write_bodies(af_farm *farm, af_callback call,
                       const af_body *bodies, size_t count,
                       af_token *token, uint32_t avg_cost,
                       const af_completion_hook *completion);
size_t af_write_fixed(af_farm *farm, af_callback call,
                      const uint64_t *bodies, size_t body_words, size_t count,
                      af_token *token, uint32_t avg_cost,
                      const af_completion_hook *completion);

af_status af_view_create(af_view **out);
af_status af_subscribe(af_view *view, af_farm *farm);
uint32_t af_consume_next(af_view *view);
void af_unsubscribe(af_view *view);
void af_view_destroy(af_view *view);

typedef void (*af_fatal_handler)(const char *message, size_t message_bytes,
                                void *context);
void af_set_fatal_handler(af_fatal_handler handler, void *context);
```

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

Map subscribe-full, retry, and invalid to distinct status codes. Creation uses
`tryCreate`, preserving geometry and allocation errors. Capacity exhaustion
at producer registration is recoverable. Bad/stale/cross-Farm tokens, invalid
callbacks or cost classes, overflow, and impossible singleton payload sizes
retain the core's fatal contract; `af_can_fit` provides a nonfatal sizing query.
No D exception crosses C. Runtime wrappers catch exceptions and return a status
with a separately retrievable diagnostic.

Install the fatal handler before creating Farms or starting workers; replacing
it requires quiescence. A null handler restores the default. It receives a
borrowed message and may log or dump; it must not throw, longjmp, re-enter the
Farm, or return control to the failed operation. If it returns, the library
aborts. The setter configures the same fatal implementation used by the core.

`struct_size`, `abi_version`, and zero reserved fields make options validation
explicit. ABI 1 rejects unknown required layouts rather than guessing. Freeze
numeric enum values and exported symbol names with the first header. Keep
internal ring offsets private; the five-word Thead is not a C ABI promise.

## Runtime-inclusive pool and host

Expose explicit runtime init/shutdown and foreign-thread attach/detach entry
points. Track whether this library owns runtime initialization so an embedding
D application does not initialize or terminate it twice. Shutdown requires
all hosts closed, workers joined, and foreign attachments released.

Start the host surface with options/default/create/close, producer registration,
a caller progress step, wake, and failure/diagnostic queries. Creation reserves
all consumer and producer capacity before workers start. The creating thread
owns host control operations and its producer token; a dedicated core view
may be used by another attached owner only under the ordinary ownership rules.

Blocking C loops can publish C grains and use a caller-owned subscribed view,
with an optional wake callback/context. Mirror the D behavior: wake after every
accepted table and on idle polls, since sleeping views can pin completed epochs.
The D `FarmHost.parallelFor` convenience and `parallelOptions` already supply
that policy. Nested blocking waits inside Farm callbacks or table hooks are
fatal in every build. No Fiber scheduler is needed for this worker pool.

## Delivery and acceptance

1. Freeze the collapsed D surface, table layout, and wake/join behavior first.
2. Implement the core exports and header together, including the fatal hook,
   layout assertions, native C example, and opaque-handle lifetime checks.
3. Add a minimal C++ RAII wrapper; encode unique token/view ownership. Rust and
   Zig can bind the same header later without changing its transport contract.
4. Implement the optional runtime-inclusive non-Fiber pool/host exports.
5. Split actor allocators/adapters before adding actor exports. Keep Fibers
   outside ABI 1.

Acceptance requires native C linking without druntime on both supported
platforms, correct callback/hook layout and calling convention, partial/zero
admission, empty bodies, callback-before-return, release-build contract deaths,
and concurrent joins. Keep BetterC compilation in the quick lane so accidental
runtime dependencies fail early; compilation alone is not C link evidence.
Use the full correctness matrix and comparable performance measurements before
release. Previous 1.8.x feasibility probes are historical, not evidence for
this implementation.
