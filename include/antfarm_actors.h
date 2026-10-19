/* Runtime-free public actors, C ABI 1. Link antfarm_core. */
#ifndef ANTFARM_ACTORS_H
#define ANTFARM_ACTORS_H
#include "antfarm.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct af_actor_runtime af_actor_runtime;
typedef struct af_actor_owner af_actor_owner;
typedef struct af_actor_context af_actor_context;
typedef struct af_actor_message af_actor_message;
/* Copyable, non-owning identity. Do not inspect or modify its fields. A saved
   handle becomes stale on reclamation but is usable until runtime destruction. */
typedef struct af_actor_handle { void *_slot; uint64_t _generation; } af_actor_handle;
typedef void (*af_actor_handler)(void *user, void *state, size_t state_bytes, af_actor_context *);
typedef struct af_actor_allocator {
    void *context;
    void *(*allocate)(void *, size_t bytes, size_t alignment);
    void (*deallocate)(void *, void *, size_t bytes, size_t alignment);
} af_actor_allocator;

enum { AF_ACTOR_QUEUED = 0, AF_ACTOR_COALESCED = 1, AF_ACTOR_WAVE_OWNED = 2,
       AF_ACTOR_STALE_HANDLE = 3, AF_ACTOR_CLOSED = 4, AF_ACTOR_NODE_BUSY = 5 };
enum { AF_ACTOR_DRAIN = 0, AF_ACTOR_CANCEL = 1 };
enum { AF_ACTOR_RETIRE_REQUESTED = 0, AF_ACTOR_RETIRE_WAVE_OWNED = 1,
       AF_ACTOR_ALREADY_RETIRED = 2, AF_ACTOR_STALE_OWNER = 3 };
enum { AF_ACTOR_RECLAIMED = 0, AF_ACTOR_RECLAIM_BUSY = 1, AF_ACTOR_RECLAIM_STALE = 2 };
enum { AF_ACTOR_COMPLETED = 0, AF_ACTOR_FAILED = 1, AF_ACTOR_CANCELLED = 2 };
#define AF_ACTOR_DEFAULT_COST 4u /* claims of eight entries */

/* Allocator is copied; its context must outlive the runtime. NULL uses native
   aligned allocation (alignments through 64). Callbacks must be thread-safe,
   return suitably aligned memory or NULL, and never throw/longjmp.
   Create/destroy at quiescence. farm must be owned; for a host Farm use
   af_host_create_actors. The Farm outlives this runtime.
   Runtime creation starts no threads. Custom allocators also receive the
   runtime/slot storage and the state prefix; deallocation repeats the exact
   size/alignment from allocation. Runtime destruction must be outside callbacks. */
af_status af_actor_runtime_create(af_farm *, size_t capacity, const af_actor_allocator *,
                                  af_actor_runtime **out);
size_t af_actor_runtime_live(const af_actor_runtime *);
size_t af_actor_runtime_ready(const af_actor_runtime *);
/* Zero is empty, backpressure or contention, not a global completion signal.
   maximum is capped at 256. The token must belong to this runtime's Farm. */
size_t af_actor_runtime_flush(af_actor_runtime *, af_token *, size_t maximum, uint32_t avg_cost);
/* NULL is a no-op. Retire/reclaim all owners; join consumers and senders first.
   Forbidden for host-owned runtimes, which the host destroys after its join. */
void af_actor_runtime_destroy(af_actor_runtime *);

/* Copies nonempty plain state. Pointees are not copied or retained by any GC.
   State alignment must be a power of two supported by the allocator.
   The callback's state and context are borrowed only until it returns.
   Callbacks may execute on different consumers, exclusively for this actor.
   user and reachable state resources must survive retirement; there is no
   guaranteed final callback. AF_FULL means all actor slots are reserved.
   AF_INVALID covers invalid metadata (including default alignment above 64);
   allocation failure returns AF_ALLOCATION_FAILED. Different owners can be
   created/reclaimed concurrently; runtime destruction requires quiescence. */
af_status af_actor_create(af_actor_runtime *, const void *initial_state, size_t state_bytes,
                          size_t state_alignment, af_actor_handler, void *user, af_actor_owner **out);
void af_actor_owner_handle(af_actor_owner *, af_actor_handle *out);
uint32_t af_actor_wake(const af_actor_handle *);
uint32_t af_actor_send(const af_actor_handle *, af_actor_message *);
/* Request status does not prove retirement. ALREADY_RETIRED can mean cancel
   was already requested. Keep driving until af_actor_retired returns true.
   Cancel is cooperative; a later drain request never downgrades it. */
uint32_t af_actor_request_retire(af_actor_owner *, uint32_t mode);
uint32_t af_actor_retired(const af_actor_owner *);
/* AF_ACTOR_RECLAIMED consumes the owner handle; copied submission handles
   remain safe to query via wake/send until the runtime is destroyed. */
uint32_t af_actor_reclaim(af_actor_owner *);

/* Context functions are valid only in this callback, on this thread. Do not
   retain the context pointer. The diagnostic is best-effort: a later callback
   may reuse its stack address, so passing an expired context is invalid even
   if the check does not detect it. */
void af_actor_republish(af_actor_context *);
void af_actor_retire(af_actor_context *);
uint32_t af_actor_closing(const af_actor_context *);
uint32_t af_actor_cancelling(const af_actor_context *);
af_actor_message *af_actor_pop(af_actor_context *);
size_t af_actor_cancel_inbox(af_actor_context *);

/* Cold allocation; reuse messages between interactions. Successful send
   transfers ownership until complete/fail/cancel release it. Payload storage
   remains caller-owned and stable through that transfer. */
af_status af_actor_message_create(void *payload, uint64_t tag, af_actor_message **out);
void af_actor_message_destroy(af_actor_message *); /* NULL no-op; requires available */
void af_actor_message_set(af_actor_message *, void *payload, uint64_t tag); /* exclusive, available */
void *af_actor_message_payload(const af_actor_message *);
uint64_t af_actor_message_tag(const af_actor_message *);
uint32_t af_actor_message_available(const af_actor_message *); /* acquire */
/* Read after acquiring availability for the accepted interaction. A newly
   created message is available but has no reply. set/send preserve old outcome. */
uint32_t af_actor_message_outcome(const af_actor_message *);
uint32_t af_actor_message_failure(const af_actor_message *);
/* Processing owner only. These are the final access to the message/payload:
   another thread may immediately reuse or free them after the release. */
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
typedef void (*af_actor_wave_operation)(void *user, void *state, size_t state_bytes);
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
   token must belong to the wave's Farm; cost is 0..7 (default convention 4). */
size_t af_actor_wave_publish(af_actor_wave *, const af_actor_handle *, size_t count,
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

/* Owners are exclusive; synchronize handoff. Handles may be copied and shared.
   All handles expire at runtime destruction; forged or freed pointers are
   invalid C access. No D runtime or thread attachment is needed here. */
#ifdef __cplusplus
}
static_assert(sizeof(af_actor_handle) == 16 && alignof(af_actor_handle) == 8, "actor identity layout");
static_assert(sizeof(af_actor_wave_handle) == 16 && alignof(af_actor_wave_handle) == 8, "wave identity layout");
static_assert(sizeof(af_actor_allocator) == 24, "actor allocator layout");
#else
_Static_assert(sizeof(af_actor_handle) == 16 && _Alignof(af_actor_handle) == 8, "actor identity layout");
_Static_assert(sizeof(af_actor_wave_handle) == 16 && _Alignof(af_actor_wave_handle) == 8, "wave identity layout");
_Static_assert(sizeof(af_actor_allocator) == 24, "actor allocator layout");
#endif
#endif
