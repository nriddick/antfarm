/* Runtime-free public actors, C ABI 1. Link antfarm_core. */
#ifndef ANTFARM_ACTORS_H
#define ANTFARM_ACTORS_H
#include "antfarm.h"
#ifdef __cplusplus
#define AF_ACTOR_STATIC_ASSERT(c, m) static_assert(c, m)
#define AF_ACTOR_ALIGNOF(t) alignof(t)
extern "C" {
#else
#define AF_ACTOR_STATIC_ASSERT(c, m) _Static_assert(c, m)
#define AF_ACTOR_ALIGNOF(t) _Alignof(t)
#endif
typedef struct af_actor_runtime af_actor_runtime;
typedef struct af_actor_kind af_actor_kind;
typedef struct af_actor_context af_actor_context;
/* Distinct copyable identities. Fields are opaque. All copies expire when their
   runtime is destroyed; the runtime must outlive every caller and observer. */
typedef struct af_actor_handle { void *_slot; uint64_t _generation; } af_actor_handle;
typedef struct af_actor_owner { void *_slot; uint64_t _generation; } af_actor_owner;
/* Stable caller-owned storage. Never inspect the words, copy/move a submitted
   message, or reinitialize it before ownership returns. No C atomic layout is
   part of this ABI. Uninitialized storage may only be passed to message_init. */
typedef struct af_actor_message { uint64_t _opaque[4]; } af_actor_message;
typedef void (*af_actor_handler)(void *user, void *state, size_t state_bytes, af_actor_context *);
typedef void (*af_actor_destructor)(void *user, void *state, size_t state_bytes);
/* Return AF_OK on success, any other value on failure. Failed initialization
   cleans its own partial object; the library frees storage without destruction.
   index is the logical index, including first_index on batch retries. */
typedef af_status (*af_actor_initializer)(void *user, void *state, size_t state_bytes, size_t index);
typedef struct af_actor_kind_options {
    af_actor_handler handler;
    void *user;
    size_t state_bytes, state_alignment;
    af_actor_destructor destroy; /* optional */
} af_actor_kind_options;
typedef struct af_actor_allocator {
    void *context;
    void *(*allocate)(void *, size_t bytes, size_t alignment);
    void (*deallocate)(void *, void *, size_t bytes, size_t alignment);
} af_actor_allocator;

enum { AF_ACTOR_QUEUED = 0, AF_ACTOR_COALESCED = 1, AF_ACTOR_WAVE_OWNED = 2,
       AF_ACTOR_STALE_HANDLE = 3, AF_ACTOR_CLOSED = 4, AF_ACTOR_NODE_BUSY = 5 };
enum { AF_ACTOR_DRAIN = 0, AF_ACTOR_CANCEL = 1 };
/* Owner outcomes are disjoint from AF_INVALID, returned on bad provenance. */
enum { AF_ACTOR_RETIRE_REQUESTED = 16, AF_ACTOR_RETIRE_WAVE_OWNED = 17,
       AF_ACTOR_ALREADY_RETIRED = 18, AF_ACTOR_STALE_OWNER = 19, AF_ACTOR_RETIRE_BUSY = 20 };
enum { AF_ACTOR_RECLAIMED = 32, AF_ACTOR_RECLAIM_BUSY = 33, AF_ACTOR_RECLAIM_STALE = 34 };
enum { AF_ACTOR_COMPLETED = 0, AF_ACTOR_FAILED = 1, AF_ACTOR_CANCELLED = 2 };
enum { AF_ACTOR_INITIALIZER_FAILED = 8 }; /* af_status; registry in antfarm.h */
#define AF_ACTOR_DEFAULT_COST 4u /* claims of eight entries */

/* NULL allocator uses native aligned allocation (alignments through 64).
   Custom allocation receives runtime, slots, kinds and final state; frees repeat
   exact sizes/alignments. Functions must be thread-safe, return aligned memory
   or NULL, and never throw/longjmp. Context outlives runtime. No threads start.
   Create/destroy the runtime at quiescence, outside Farm callbacks. farm must be
   owned; for a host Farm use af_host_create_actors. Farm outlives runtime. */
af_status af_actor_runtime_create(af_farm *, size_t capacity, const af_actor_allocator *,
                                  af_actor_runtime **out);
/* Occupied slots: includes the entire reserved construction prefix, live and
   retired actors, and reclamation until the slot returns. Zero is not a join. */
size_t af_actor_runtime_live(const af_actor_runtime *);
size_t af_actor_runtime_ready(const af_actor_runtime *);
/* Zero is empty, backpressure or contention, not a join. maximum is capped at
   256. The token must belong to this runtime's Farm. */
size_t af_actor_runtime_flush(af_actor_runtime *, af_token *, size_t maximum, uint32_t avg_cost);
/* NULL is a no-op. Retire/reclaim every actor; join consumers, creators, senders
   and copied-owner callers first. Host-owned runtimes are destroyed by host. */
void af_actor_runtime_destroy(af_actor_runtime *);

/* Immutable runtime-owned kind; registration may overlap actors, construction
   and other registrations. Join registrars before runtime destruction. Each
   call needs exclusive output storage. Descriptor is copied. Nonnull
   handler, nonzero bytes, power-of-two alignment; >64 needs a custom allocator.
   Handler/user/destructor remain live through all actors and waves using it.
   Optional destructor runs exactly once on the winning reclaimer's thread,
   outside gates; no throw/longjmp or actor operations on its own actor.
   Abandonment deliberately retains state and does not run destructors. */
af_status af_actor_kind_register(af_actor_runtime *, const af_actor_kind_options *,
                                 const af_actor_kind **out);
/* Empty output required. Copy accepts byte-copyable state; init receives final
   aligned storage for placement-new or filling from component data. No state
   prefix, temporary buffer or owner allocation. AF_FULL includes reservations.
   Callbacks must not throw/longjmp; borrowed state/context expire on return.
   Pointees are not copied or retained by GC. Creation can run concurrently. */
af_status af_actor_create_copy(af_actor_runtime *, const af_actor_kind *, const void *initial_state,
                               af_actor_owner *out);
af_status af_actor_create_init(af_actor_runtime *, const af_actor_kind *, af_actor_initializer,
                               void *user, size_t index, af_actor_owner *out);
void af_actor_owner_handle(const af_actor_owner *, af_actor_handle *out);
uint32_t af_actor_wake(const af_actor_handle *);
uint32_t af_actor_send(const af_actor_handle *, af_actor_message *);
/* Copies may operate concurrently on the same generation, but writes to any
   one owner object need caller synchronization. Runtime-scoped operations reject
   foreign/interior slot addresses with AF_INVALID before dereferencing them.
   Runtime/kind pointers must themselves be valid and live. Canonical empty
   owner is stale. Handles remain runtime-free: forged handles are invalid access.
   RETIRE_BUSY is retryable contention with another retirement call.
   ALREADY_RETIRED can mean cancel already requested: it is not a join.
   Keep driving until retired reports true. Cancel is cooperative. */
uint32_t af_actor_request_retire(af_actor_runtime *, const af_actor_owner *, uint32_t mode);
/* True only for the matching retired phase; stale/reclaiming identities return
   false. If copies may reclaim concurrently, retry reclaim itself (BUSY/STALE)
   instead of waiting exclusively for this observation. */
af_status af_actor_retired(af_actor_runtime *, const af_actor_owner *, uint32_t *out);
/* RECLAIMED or RECLAIM_STALE clears this owner; BUSY leaves it for retry.
   Exclusive generation claim prevents duplicate destruction through copies. */
uint32_t af_actor_reclaim(af_actor_runtime *, af_actor_owner *);

/* Strides are bytes. Empty spans accept NULL. Nonempty spans require aligned
   first elements, stride >= element size, and nonoverflowing address arithmetic.
   State copy input has byte alignment. Result/output count is capacity.
   Storage extent/lifetime and non-overlap of element bytes are caller contracts;
   bounding spans may overlap for distinct fields in the same entity array. */
typedef struct af_actor_handle_span { const af_actor_handle *data; size_t stride, count; } af_actor_handle_span;
typedef struct af_actor_owner_span { const af_actor_owner *data; size_t stride, count; } af_actor_owner_span;
typedef struct af_actor_owner_output_span { af_actor_owner *data; size_t stride, count; } af_actor_owner_output_span;
typedef struct af_actor_state_span { const void *data; size_t stride, count; } af_actor_state_span;
typedef struct af_actor_result_span { uint32_t *data; size_t stride, count; } af_actor_result_span;
typedef struct af_actor_batch_result { size_t count; af_status status; } af_actor_batch_result;
/* Preflight all output owners empty. Successful prefix remains live; suffix
   stays empty. status = AF_OK, AF_FULL, AF_ALLOCATION_FAILED or
   AF_ACTOR_INITIALIZER_FAILED; structural errors return AF_INVALID, count=0.
   Reserve the prefix in chunks of at most 64 slots per gate hold, allowing
   other creators/reclaimers between chunks. Finish all reservations before
   callbacks/allocations, which run outside the gate.
   Init count comes from output; first_index preserves indices across retries. */
af_actor_batch_result af_actor_create_copy_batch(af_actor_runtime *, const af_actor_kind *,
                                                af_actor_state_span, af_actor_owner_output_span);
af_actor_batch_result af_actor_create_init_batch(af_actor_runtime *, const af_actor_kind *,
                                                af_actor_initializer, void *user, size_t first_index,
                                                af_actor_owner_output_span);
/* Structural errors reject before mutation. Otherwise attempt every occurrence
   and write its result, including duplicate identities. count totals accepted
   queued/coalesced wakes, requested/already-retired retirements, or cleared
   reclaimed/stale owners. Busy/invalid do not count. These totals are not joins.
   Reclaim is in place; every busy owner remains available for retry. */
af_actor_batch_result af_actor_wake_batch(af_actor_handle_span, af_actor_result_span);
af_actor_batch_result af_actor_request_retire_batch(af_actor_runtime *, af_actor_owner_span,
                                                   uint32_t mode, af_actor_result_span);
af_actor_batch_result af_actor_reclaim_batch(af_actor_runtime *, af_actor_owner_output_span,
                                            af_actor_result_span);

/* Context functions are valid only in this callback, on this thread. Do not
   retain the pointer. Diagnostic is best-effort: later stack-address reuse
   does not make an expired context valid. */
void af_actor_republish(af_actor_context *);
void af_actor_retire(af_actor_context *);
uint32_t af_actor_closing(const af_actor_context *);
uint32_t af_actor_cancelling(const af_actor_context *);
af_actor_message *af_actor_pop(af_actor_context *);
size_t af_actor_cancel_inbox(af_actor_context *);

/* Initialize caller-exclusive storage. A successful send transfers ownership
   until complete/fail/cancel releases it. Message and payload stay stable
   throughout. Newly initialized storage is available but has no reply. */
void af_actor_message_init(af_actor_message *, void *payload, uint64_t tag);
void af_actor_message_set(af_actor_message *, void *payload, uint64_t tag); /* exclusive, available */
void *af_actor_message_payload(const af_actor_message *);
uint64_t af_actor_message_tag(const af_actor_message *);
uint32_t af_actor_message_available(const af_actor_message *); /* acquire */
/* Read after acquiring availability. set/send preserve the previous outcome. */
uint32_t af_actor_message_outcome(const af_actor_message *);
uint32_t af_actor_message_failure(const af_actor_message *);
/* Processing owner only. Final message/payload access: another thread may
   immediately reuse or free them. Reply wake snapshots the target first. */
void af_actor_message_complete(af_actor_message *);
void af_actor_message_fail(af_actor_message *, uint32_t code); /* 24-bit code */
void af_actor_message_cancel(af_actor_message *);
uint32_t af_actor_message_complete_and_wake(af_actor_message *, const af_actor_handle *);
uint32_t af_actor_message_fail_and_wake(af_actor_message *, uint32_t code, const af_actor_handle *);
uint32_t af_actor_message_cancel_and_wake(af_actor_message *, const af_actor_handle *);

/* Stable, reusable phase over idle C actors, possibly from several runtimes on
   one Farm. One orchestrator owns create/begin/publish/seal/destroy; observation
   handles may be shared. State sizes may differ: the operation must interpret
   each actor's state correctly. It receives no inbox/retirement context.
   State pointers are borrowed only until return; operations in the same wave
   must not read one another's mutable state. No allocation during publication. */
typedef struct af_actor_wave af_actor_wave;
typedef struct af_actor_wave_handle {
    af_actor_wave *_wave;
    uint64_t _generation;
} af_actor_wave_handle;
typedef void (*af_actor_wave_operation)(void *user, const af_actor_kind *, void *state, size_t state_bytes);
af_status af_actor_wave_create(af_farm *, af_actor_wave **out);
/* Begin requires a new wave or previous ready generation. The optional hook is
   copied. operation/user remain live until finished; hook context until ready.
   The hook runs once after finished, possibly inline in seal. It must not throw,
   longjmp, destroy/reuse this wave, or wait for this wave's ready signal.
   begin/destroy must be outside Farm callbacks. */
void af_actor_wave_begin(af_actor_wave *, af_actor_wave_operation, void *user,
                         const af_completion_hook *);
void af_actor_wave_observe(af_actor_wave *, af_actor_wave_handle *out);
/* At most one physical table; returns its accepted prefix, which can be partial.
   Advance by that prefix. Zero without failed means backpressure (or empty
   input); consume/wake workers and retry. Structural rejection sets failed:
   stale, active, duplicate, or foreign-Farm actor. Seal and drain any accepted
   tables even on failure. Earlier operations are never rolled back.
   Input handles are copied before return; their runtimes outlive the wave.
   token must belong to the wave's Farm; cost is 0..7 (default convention 4).
   Malformed span geometry is fatal before publication. */
size_t af_actor_wave_publish(af_actor_wave *, af_actor_handle_span,
                             af_token *, uint32_t avg_cost);
void af_actor_wave_seal(af_actor_wave *); /* exactly once per generation */
/* Zero-initialized/null handles return false/zero. Reuse invalidates old handles;
   after wave destruction all handles expire and must never be accessed. */
uint32_t af_actor_wave_valid(const af_actor_wave_handle *);
uint32_t af_actor_wave_finished(const af_actor_wave_handle *); /* acquire joins actor writes */
uint32_t af_actor_wave_failed(const af_actor_wave_handle *);
uint32_t af_actor_wave_interrupted(const af_actor_wave_handle *);
uint64_t af_actor_wave_progress(const af_actor_wave_handle *); /* diagnostic table count */
uint64_t af_actor_wave_length(const af_actor_wave_handle *);
/* Acquire joins the completion callback's return and final adapter access too.
   finished alone does not. Neither query joins handed-off autonomous work.
   interrupted is a sticky diagnostic of observed wake/send/retirement races. */
uint32_t af_actor_wave_ready(const af_actor_wave_handle *);
/* NULL is a no-op. Requires never begun or ready; quiesce observers first.
   Destroy waves before their Farm, actor runtimes, or normal host close.
   Host abandon invalidates/leaks remaining waves together with retained work. */
void af_actor_wave_destroy(af_actor_wave *);

/* Owner object mutations are exclusive; distinct copies may race safely.
   All handles expire at runtime destruction; forged or freed pointers are
   invalid C access. No D runtime or thread attachment is needed here. */
#ifdef __cplusplus
}
#endif
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_owner) == 16 && AF_ACTOR_ALIGNOF(af_actor_owner) == 8, "actor owner layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_handle) == 16 && AF_ACTOR_ALIGNOF(af_actor_handle) == 8, "actor handle layout");
AF_ACTOR_STATIC_ASSERT(offsetof(af_actor_owner, _generation) == 8 && offsetof(af_actor_handle, _generation) == 8, "identity offsets");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_message) == 32 && AF_ACTOR_ALIGNOF(af_actor_message) == 8 &&
                 offsetof(af_actor_message, _opaque) == 0, "inbox storage layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_kind_options) == 40 && offsetof(af_actor_kind_options, destroy) == 32, "kind layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_handle_span) == 24 && sizeof(af_actor_owner_span) == 24 &&
                 sizeof(af_actor_owner_output_span) == 24 && sizeof(af_actor_state_span) == 24 &&
                 sizeof(af_actor_result_span) == 24, "span layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_batch_result) == 16 && offsetof(af_actor_batch_result, status) == 8, "batch result layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_wave_handle) == 16 && AF_ACTOR_ALIGNOF(af_actor_wave_handle) == 8, "wave identity layout");
AF_ACTOR_STATIC_ASSERT(sizeof(af_actor_allocator) == 24, "actor allocator layout");
#undef AF_ACTOR_STATIC_ASSERT
#undef AF_ACTOR_ALIGNOF
#endif
