/* Optional druntime-inclusive GroupHost. MIT License. */
#ifndef ANTFARM_GROUPS_H
#define ANTFARM_GROUPS_H
#include "antfarm_forkjoin.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct af_group_host af_group_host;
enum { AF_GROUP_AUTOMATIC = 0, AF_GROUP_ALL = 1, AF_GROUP_ONLY = 2 };
typedef struct af_group_options {
    uint32_t struct_size, abi_version;
    af_host_options host;
    uint32_t group_mode, group_index, farm_per_group, reserved;
} af_group_options;
/* A worker body owns its consumer view; return 0 when idle, 1 on progress.
   The pool arms, rechecks body, then parks. stop must unsubscribe/destroy the
   worker's views. Both callbacks must not throw/longjmp or retain callback
   arguments. stop runs on every started worker, including one stopped
   before its body ever ran, which acquired no user resources. host/context
   stay live through
   stop_workers. Workers can call
   farm/token during startup, before af_group_start returns. Start is not a
   user-body barrier: synchronize consumer readiness before first publication. */
typedef uint32_t (*af_group_body)(af_group_host *, uint32_t worker_index, uint32_t llc, void *context);
typedef void (*af_group_stop)(af_group_host *, uint32_t worker_index, uint32_t llc, void *context);
void af_group_options_default(af_group_options *out);
/* Resolves the worker CPU set without starting it. Query with NULL/0;
   AF_FULL writes required count and no partial output. Control thread only. */
af_status af_group_resolve(const af_group_options *, af_processor *out, size_t capacity, size_t *count);
af_status af_group_start(const af_group_options *, af_group_body, af_group_stop, void *context, af_group_host **out);
af_status af_group_caller_llc(af_group_host *, uint32_t *out);
/* Attached threads. Borrowed Farm, stable until close, never af_destroy it.
   Invalid/unselected groups return AF_INVALID and clear output. */
af_status af_group_farm(af_group_host *, uint32_t llc, af_farm **out);
/* Lazily register/cache one small token per calling thread per distinct Farm.
   Borrowed token: never af_unregister_producer it or transfer it to another
   thread. Cache hits allocate nothing. Foreign threads consume the configured
   extra_small_producers budget; release_tokens before detach or host close.
   Workers release automatically after stop callback; control thread at close.
   Separate af_register_producer tokens remain caller-owned. */
af_status af_group_token(af_group_host *, uint32_t llc, af_token **out);
af_status af_group_release_tokens(af_group_host *);
/* Any native thread, no attachment needed. UINT32_MAX wakes every worker,
   otherwise only the selected LLC. Wake after writes AND retry/idle polls;
   notification follows release-publication and is only a prompt to recheck.
   Quiesce wake callers before stop_workers or close. */
af_status af_group_wake(af_group_host *, uint32_t llc);
/* Control thread, after application work completes: joins workers and runs
   stop callbacks but keeps Farms/handles alive. Idempotent. */
af_status af_group_stop_workers(af_group_host *);
/* Requires stop_workers first. AF_BUSY retains handle until all external
   tokens/views, actors, waves and private parks are returned. Releases cached
   control-thread tokens, destroys Farms and restores caller affinity.
   AF_OK consumes handle. No automatic application-work drain or abandonment. */
af_status af_group_close(af_group_host *);
#ifdef __cplusplus
}
static_assert(sizeof(af_group_options) == 112, "group options layout");
#else
_Static_assert(sizeof(af_group_options) == 112, "group options layout");
#endif
#endif
