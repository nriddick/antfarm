/* Runtime-free private roots and child cohorts, C ABI 1. Link antfarm_core. */
#ifndef ANTFARM_PRIVATE_H
#define ANTFARM_PRIVATE_H
#include "antfarm_actors.h"
#ifdef __cplusplus
extern "C" {
#define AF_PRIVATE_ASSERT(c, m) static_assert(c, m)
#else
#define AF_PRIVATE_ASSERT(c, m) _Static_assert(c, m)
#endif

typedef struct af_private_roots af_private_roots;
typedef struct af_private_children af_private_children;
typedef struct af_private_context af_private_context;
/* Copyable cancellation identity, never a state borrow or scheduling handle.
   Fields are opaque. All copies expire at park destruction. */
typedef struct af_private_root_handle { void *_record; uint64_t _generation; } af_private_root_handle;
typedef struct af_private_root_outcome {
    af_private_root_handle root;
    uint32_t status, cause, code, reserved;
} af_private_root_outcome;
typedef struct af_private_wave_outcome {
    size_t participants, dispatched;
    uint32_t failures, first_failure_code;
    size_t first_failure_position;
    uint32_t cancelled, reserved;
} af_private_wave_outcome;
typedef struct af_private_pump_count { size_t roots, tables; } af_private_pump_count;
typedef struct af_private_results { const void *data; size_t stride, count; } af_private_results;

enum { AF_PRIVATE_AGAIN = 0, AF_PRIVATE_FINISH = 1,
       AF_PRIVATE_WAIT_CHILDREN = 2, AF_PRIVATE_FINISH_AFTER_CHILDREN = 3 };
enum { AF_PRIVATE_DONE = 0, AF_PRIVATE_CHILD_WAIT = 1 };
enum { AF_PRIVATE_PER_OBJECT = 0, AF_PRIVATE_COHORT_SLAB = 1 };
enum { AF_PRIVATE_FREE_ON_EMITTER = 0, AF_PRIVATE_FREE_ON_CONSUMER = 1 };
enum { AF_PRIVATE_RETIRING = 9, AF_PRIVATE_EMPTY = 10 }; /* af_status; registry in antfarm.h */
enum { AF_PRIVATE_COMPLETED = 0, AF_PRIVATE_FAILED = 1, AF_PRIVATE_CANCELLED = 2 };
enum { AF_PRIVATE_CAUSE_NONE = 0, AF_PRIVATE_CAUSE_REQUESTED = 1, AF_PRIVATE_CAUSE_DEADLINE = 2 };
enum { AF_PRIVATE_CANCEL_REQUESTED = 0, AF_PRIVATE_CANCEL_ALREADY = 1,
       AF_PRIVATE_CANCEL_FINISHED = 2, AF_PRIVATE_CANCEL_STALE = 3 };
#define AF_PRIVATE_AUTO_COST UINT32_MAX
#define AF_PRIVATE_CHILD_COST 2u /* claims of 32 */
#define AF_PRIVATE_NO_DEADLINE INT64_MAX

/* Callbacks never throw/longjmp or retain state/context/result pointers. Only
   the running actor may access its state. Parents read joined results, never
   child state. Callback code and user pointers outlive all their uses. */
typedef uint32_t (*af_private_root_step)(void *user, void *state, size_t bytes, af_private_context *);
/* A leaf returns DONE. With continuation=1 it may return CHILD_WAIT and is
   re-entered after descendants join. result is NULL for a signal-only cohort;
   otherwise write all result_bytes before DONE. Results are byte-copyable,
   have no library destructor, and must not refer to retiring child storage. */
typedef uint32_t (*af_private_child_step)(void *user, void *state, size_t bytes,
                                         void *result, size_t result_bytes, af_private_context *);
typedef struct af_private_state_options {
    size_t bytes, alignment;
    af_actor_destructor destroy;
    void *destroy_user;
} af_private_state_options;
typedef struct af_private_root_options {
    uint32_t struct_size, abi_version;
    size_t capacity, batch_limit;
    af_private_state_options state;
    af_private_root_step step;
    void *user;
    uint32_t tracking, reclamation;
    size_t slab_byte_limit; /* COHORT_SLAB: required cap on capacity * aligned state stride */
    uint32_t storage, reserved; /* PER_OBJECT or COHORT_SLAB; reserved must be 0 */
} af_private_root_options;
typedef struct af_private_child_options {
    uint32_t struct_size, abi_version;
    size_t capacity, batch_limit; /* 0 batch limit inherits parent */
    af_private_state_options state;
    size_t result_bytes, result_alignment; /* bytes=0: no storage, alignment must be 1 */
    size_t slab_byte_limit; /* COHORT_SLAB: required cap on capacity * aligned state stride */
    uint32_t storage, subsets;
} af_private_child_options;

/* Defaults leave state/step unset. Exact struct size/version required; Boolean
   fields must be 0/1. State bytes>0, alignments power-of-two. The native allocator
   supports through 64; custom allocators can support larger alignments.
   Options are copied. Metadata and descendants inherit the allocator and its
   context. All allocation callbacks must be thread-safe and correctly aligned.
   Root storage: PER_OBJECT allocates each root's state at spawn and frees it
   under the reclamation option. COHORT_SLAB reserves one zeroed slab of
   capacity * aligned stride at create, under slab_byte_limit, so spawn never
   allocates and the terminal release clears the slot (on the emitter at
   collection, or on the consumer in the final activation) instead of freeing
   it; the slab is freed once at destroy. A slab with slab_byte_limit 0 (the
   default) or an overflowing product: AF_INVALID; a slab exceeding its set
   cap, or any allocation failure: AF_ALLOCATION_FAILED. */
void af_private_root_options_default(af_private_root_options *);
void af_private_child_options_default(af_private_child_options *);
af_status af_private_roots_create(af_farm *, const af_private_root_options *,
                                   const af_actor_allocator *, af_private_roots **out);
/* All park operations are single-emitter and non-reentrant, including live and
   destroy. None may run in a Farm callback or allocator/initializer/destructor
   reentry. Only root_request_cancel may run on another native thread.
   An owned or borrowed host Farm is allowed; destroy park before Farm/host close.
   No worker threads are started. Each emitter exclusively owns its token. */
size_t af_private_roots_live(af_private_roots *);
void af_private_roots_collect(af_private_roots *);
/* limit=0 uses configured limit; otherwise 1..configured limit. cost is 0..7 or
   AUTO_COST. Returns publications, not completed work. tables counts child and
   resume publications too: use it to wake parked workers. Keep pumping and
   consuming during waits; zero is not a join. Never calls consumer callbacks. */
af_private_pump_count af_private_roots_pump(af_private_roots *, af_token *, size_t limit, uint32_t cost);
/* Copy is for byte-copyable state. Init writes directly to final aligned state
   (e.g. placement-new). Failure cleans its own partial object; no destructor is
   invoked, and no root is admitted. Neither form allocates a state prefix or
   temporary. Initializers may not call private context APIs.
   Return AF_OK/FULL/ALLOCATION_FAILED/ACTOR_INITIALIZER_FAILED/INVALID.
   handle may be NULL; non-NULL handle and deadlines require tracking. A valid
   handle output is cleared on failure. All tracked outcomes must be taken. */
af_status af_private_root_spawn_copy(af_private_roots *, const void *initial,
                                      int64_t deadline, af_private_root_handle *handle);
af_status af_private_root_spawn_init(af_private_roots *, af_actor_initializer, void *user,
                                      size_t index, int64_t deadline, af_private_root_handle *handle);
/* Collect/pump first. Successful take acquire-joins that root's application
   writes, recycles its slot and makes every copy of the returned handle stale.
   Tracked-only calls are fatal on an untracked park. Returns 1 when an outcome
   was taken, 0 (output cleared) when none is ready. */
uint32_t af_private_root_take_outcome(af_private_roots *, af_private_root_outcome *);
size_t af_private_roots_cancel_all(af_private_roots *);
uint32_t af_private_root_request_cancel(const af_private_root_handle *);
size_t af_private_root_index(const af_private_root_handle *);
/* Lazy monotonic deadlines use these ticks and frequency, not wall-clock or
   platform-independent units. Never subtract/add without checking overflow.
   No timer wakes a sleeping emitter. Cancellation does not interrupt callbacks. */
int64_t af_private_clock_now(void);
int64_t af_private_clock_frequency(void);
/* NULL no-op. Collects once, then fatally rejects live roots/untaken outcomes.
   Join cancellers and all park users first. Stored destructors run on consumers,
   after descendants; only root raw-byte free follows the reclamation option. */
void af_private_roots_destroy(af_private_roots *);

/* Child cohorts are owned/disposed by their parent. Never free them yourself or
   use them outside that parent's current callback. The context check is best-
   effort; stack-address reuse never makes an expired context legal.
   PER_OBJECT allocates each state at spawn. COHORT_SLAB allocates one aligned
   slab at create, then spawns without allocation; terminal waves destroy/clear
   slots and permit refill only after the wave join. No per-member wrappers.
   Structural option errors, including a slab with slab_byte_limit 0 (the
   default): AF_INVALID; retiring: AF_PRIVATE_RETIRING; a slab exceeding its
   set cap, or allocation failure: AF_ALLOCATION_FAILED. A valid output is cleared. */
af_status af_private_children_create(af_private_context *, const af_private_child_options *,
                                      af_private_children **out);
af_status af_private_child_spawn_copy(af_private_children *, af_private_context *, const void *initial);
af_status af_private_child_spawn_init(af_private_children *, af_private_context *,
                                       af_actor_initializer, void *user, size_t index);
/* Starts return AF_OK, PRIVATE_EMPTY, BUSY, PRIVATE_RETIRING or INVALID. Empty/rejected
   starts preserve previous results/outcome and operation. retire/continuation
   are Booleans, cost=0..7. Leaf terminal callbacks cannot create/start children;
   terminal continuations can. Keep operation/user alive through finished.
   Start allocates nothing. A subset copies unique spawn indices into storage
   reserved with subsets=1. Duplicate/out-of-range selections are fatal. */
af_status af_private_children_start(af_private_children *, af_private_context *,
                                     af_private_child_step, void *user, uint32_t retire,
                                     uint32_t continuation, uint32_t cost);
af_status af_private_children_start_subset(af_private_children *, af_private_context *,
                                            af_private_child_step, void *user,
                                            const uint32_t *members, size_t count,
                                            uint32_t continuation, uint32_t cost);
/* finished returns 1 when the current wave has joined (also before the first
   start), acquiring its writes. cancel returns 1 when it cancelled an active
   wave, 0 with no effect when the cohort is idle. */
uint32_t af_private_children_finished(af_private_children *, af_private_context *);
uint32_t af_private_children_cancel(af_private_children *, af_private_context *);
/* Outcome/results require a joined wave. Results have aligned byte stride and
   participant order: spawn order, or subset list order. Cancellation exposes
   only the dispatched prefix. Storage outlives terminal member retirement but
   expires at next nonempty start or parent disposal, and must not escape this
   parent callback. Signal-only results return {NULL,0,0}. */
void af_private_children_outcome(af_private_children *, af_private_context *, af_private_wave_outcome *);
af_private_results af_private_children_results(af_private_children *, af_private_context *);
const uint32_t *af_private_children_subset(af_private_children *, af_private_context *, size_t *count);
void af_private_fail(af_private_context *, uint32_t code); /* root requires tracking */
/* cancelled returns 1 once this actor's root or an enclosing wave is cancelled
   (handle, cancel_all, deadline, parent or peer); within one callback it never
   reverts to 0. */
uint32_t af_private_cancelled(af_private_context *);
void af_private_cancel_children(af_private_context *);
void af_private_cancel_peers(af_private_context *); /* child context only */

#ifdef __cplusplus
}
#endif
AF_PRIVATE_ASSERT(sizeof(af_private_root_handle) == 16, "private handle layout");
AF_PRIVATE_ASSERT(sizeof(af_private_root_outcome) == 32, "root outcome layout");
AF_PRIVATE_ASSERT(sizeof(af_private_wave_outcome) == 40, "wave outcome layout");
AF_PRIVATE_ASSERT(sizeof(af_private_root_options) == 96 && offsetof(af_private_root_options, step) == 56
                  && offsetof(af_private_root_options, slab_byte_limit) == 80
                  && offsetof(af_private_root_options, storage) == 88, "root options layout");
AF_PRIVATE_ASSERT(sizeof(af_private_child_options) == 88 && offsetof(af_private_child_options, storage) == 80, "child options layout");
AF_PRIVATE_ASSERT(sizeof(af_private_results) == 24 && sizeof(af_private_pump_count) == 16, "private spans layout");
#undef AF_PRIVATE_ASSERT
#endif
