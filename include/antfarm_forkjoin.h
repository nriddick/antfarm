/* Optional druntime-inclusive dispatcher. MIT License. C ABI 1 additions. */
#ifndef ANTFARM_FORKJOIN_H
#define ANTFARM_FORKJOIN_H
#include "antfarm_runtime.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct af_dispatcher af_dispatcher;
typedef struct af_root_completion af_root_completion;
typedef struct af_node af_node;
typedef af_node *(*af_node_callback)(af_node *);
/* Initialize with af_node_init. Stable caller-owned storage, never copy or
   mutate while admitted. private_words is scheduler-owned. Embed this header
   first in a larger C frame, or use context. run/join must not throw/longjmp.
   run must return exactly the value af_complete(self) or af_fork(self, ...)
   returned to it. join runs once after all children (a forking parent
   needs one); it returns NULL, which completes the parent, or exactly the
   value af_fork returned when it forked again. Any other return, or a run
   that neither completes nor forks, is fatal in every build. Do not call
   af_complete(self) from join. A leaf's join is never called. Range
   callbacks (each/leaf/join of af_forkjoin_for/reduce) run with no active
   node: they cannot complete or fork the node that started the range.
   A join may free its own frame as its final access. A leaf must retain its
   header through af_complete. A terminal join may free its frame; other
   frames stay live through their tree completion.
   Completion/reuse is exclusive: exactly one admission/completion per node. */
typedef struct af_node {
    uint64_t private_words[4];
    af_node_callback run, join;
    void *context;
} af_node;
typedef struct af_processor {
    uint16_t group, processor;
    uint16_t llc, reserved;
} af_processor;
/* Enumerate eligible processors in topology order. NULL/0 queries count.
   AF_FULL reports required count and writes no partial output. */
af_status af_processors(af_processor *out, size_t capacity, size_t *count);
typedef struct af_dispatcher_options {
    uint32_t struct_size, abi_version;
    uint32_t ring_mib, small_quota, deque_capacity, poll_every, avg_cost, cross_div;
    uint32_t sibling_first, body_steps, idle_max, park, spin_steps;
    uint32_t farm_per_group, caller_consumes, spill_inline;
    uint64_t reserved;
} af_dispatcher_options;
typedef struct af_dispatcher_stats {
    uint64_t pushed, popped, steal_attempts, steal_successes;
    uint64_t cross_attempts, cross_successes, sibling_successes;
    uint64_t spilled_tables, spilled_children, inline_children, solo_children;
    uint64_t farm_polls, farm_hits, deque_high_water, parks, wakes, submits;
} af_dispatcher_stats;
void af_dispatcher_options_default(af_dispatcher_options *out);
/* Runtime control thread. NULL options selects one shared Farm. NULL/0 CPUs
   selects up to 128 eligible processors; otherwise CPUs[0] pins the caller,
   the remainder are workers. group/processor are input; llc is informational.
   All options require exact size, ABI version and zero reserved fields.
   Only one running dispatcher/pool. Failures clear *out and undo startup. */
af_status af_dispatcher_start(const af_dispatcher_options *, const af_processor *, size_t count, af_dispatcher **out);
/* Quiesce external submitters first. Drains detached and submitted roots,
   joins workers, restores affinity. Idempotent; retains handle for counters. */
af_status af_dispatcher_stop(af_dispatcher *);
/* Available after stop only (AF_BUSY while running), copied per handle.
   pushed/popped/deque_high_water are zero without ForkjoinHotStats; submits
   is reserved as on the D side. */
af_status af_dispatcher_counters(const af_dispatcher *, af_dispatcher_stats *out);
/* Control thread, stopped handle only; NULL is a no-op. */
void af_dispatcher_destroy(af_dispatcher *);
void af_node_init(af_node *, af_node_callback run, af_node_callback join, void *context);
/* Participant callbacks only. children must be nonempty, distinct initialized
   nodes. fork transfers them and parent; do not touch parent after publishing.
   The returned node must be returned by the callback, or executed by the same
   trampoline. Invalid hot-path contracts are fatal in release builds too. */
af_node *af_fork(af_node *parent, af_node **children, size_t count);
af_node *af_complete(af_node *);
/* Participant only; nested roots wait on their own tree. */
af_status af_run_root(af_dispatcher *, af_node *);
/* Attached non-participant only, requires at least one worker and no caller
   Farm subscription (caller_consumes=0). Submission retries on backpressure.
   Completion is an exclusive, reusable handle: submit, wait, then reuse.
   Waiting establishes visibility of all tree results. Destroy only idle.
   Submitted frames may finish before submit returns. */
af_status af_root_completion_create(af_root_completion **out);
af_status af_submit_root(af_dispatcher *, af_node *, af_root_completion *);
af_status af_wait_root(af_root_completion *);
af_status af_root_completion_destroy(af_root_completion *);
af_status af_submit_detached(af_dispatcher *, af_node *);

typedef void (*af_for_callback)(void *context, size_t index);
typedef void (*af_reduce_leaf)(void *context, size_t begin, size_t end, const void *seed, void *out);
typedef void (*af_reduce_join)(void *context, const void *left, const void *right, void *out);
/* Synchronous, attached thread; participants help, foreign threads submit/wait.
   [begin,end) splits at begin+(end-begin)/2 until length <= positive grain.
   No callbacks for empty ranges. Reduction copies seed for an empty range;
   otherwise each leaf folds from seed in increasing index order (the leaf
   callback's obligation), then joins left/right in the fixed tree order.
   POD result storage has value_size bytes, alignment <= 16. Callbacks write
   every byte; join output never aliases inputs. seed and out may alias.
   Callbacks must not throw/longjmp, and context must support concurrent calls.
   Temporary frames use the D frame pool; allocation exhaustion is fatal.
   Result equivalence to a serial fold needs an associative operation and an
   identity seed. The tree shape is identical across participant counts. */
af_status af_forkjoin_for(af_dispatcher *, size_t begin, size_t end, size_t grain, af_for_callback, void *context);
af_status af_forkjoin_reduce(af_dispatcher *, size_t begin, size_t end, size_t grain, size_t value_size,
                            const void *seed, af_reduce_leaf, af_reduce_join, void *context, void *out);
#ifdef __cplusplus
}
static_assert(sizeof(af_node) == 56 && alignof(af_node) == 8, "node layout");
static_assert(sizeof(af_dispatcher_options) == 72, "dispatcher options layout");
#else
_Static_assert(sizeof(af_node) == 56 && _Alignof(af_node) == 8, "node layout");
_Static_assert(sizeof(af_dispatcher_options) == 72, "dispatcher options layout");
#endif
#endif
