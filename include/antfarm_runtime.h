/* Optional druntime-inclusive non-Fiber FarmHost. Link with antfarm_core. */
#ifndef ANTFARM_RUNTIME_H
#define ANTFARM_RUNTIME_H
#include "antfarm_actors.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct af_host af_host;
#define AF_AUTO_WORKERS UINT32_MAX
#define AF_WAIT_FOREVER UINT64_MAX
enum { AF_CALLER_UNCHANGED = 0, AF_CALLER_CONFINE = 1, AF_CALLER_PIN = 2 };
enum { AF_IDLE_PARK = 0, AF_IDLE_SPIN = 1 };
enum { AF_HOST_OPEN = 0, AF_HOST_FAILED = 1, AF_HOST_CLOSED = 2,
       AF_HOST_ABANDONED = 3, AF_HOST_STOPPING = 4 };
/* Initialize with af_host_options_default. ABI 1 requires struct_size == sizeof
   (af_host_options), abi_version == AF_ABI_VERSION and reserved == 0. Truncated
   and larger option structures are rejected with AF_INVALID. */
typedef struct af_host_options {
    uint32_t struct_size, abi_version;
    uint32_t workers, caller_consumes, caller_placement, idle;
    uint64_t idle_timeout_ns;
    uint32_t ring_mib, segments, extra_small_producers, bulk_producers;
    uint64_t bulk_quota_words, small_quota_words;
    uint32_t extra_consumers, small_table_threshold, huge_pages, skip_smt_siblings;
    uint64_t reserved;
} af_host_options;

/* One explicit session per process. Call init/shutdown serially on the same
   control thread, with lifecycle operations in other D libraries quiescent.
   A balanced druntime reference preserves an embedding D application's own
   initialization. Shutdown returns AF_BUSY until hosts and attachments close.
   No runtime function may race shutdown. ABI 1 does not support restarting
   a session after shutdown. */
af_status af_runtime_init(void);
af_status af_runtime_shutdown(void);
/* Foreign threads must attach before runtime services other than wake/state,
   and detach before exit.
   Already attached D threads are borrowed, never detached by this library.
   These calls include TLS constructors/destructors only for new attachments.
   Core-only C threads need neither function.
   On Linux, druntime installs process-wide GC suspend/resume signal handlers.
   The supported LDC/DMD runtimes default to SIGRTMIN and SIGRTMIN+1; honor the
   linked runtime's configured signals. Do not replace those handlers or block
   those signals on attached threads, including pool workers. Account for
   inherited signal masks when starting the runtime, workers or attachments.
   The runtime-free core does not install these handlers. */
af_status af_runtime_attach_thread(void);
af_status af_runtime_detach_thread(void);

void af_host_options_default(af_host_options *out);
/* Control thread only, except wake and state. Defaults: automatic worker
   count on one LLC, one worker/core, caller consumption, unchanged caller
   affinity, 1 ms parks, one extra small producer slot. Zero workers is valid.
   A process can have only one host with workers (the D pool's existing limit).
   Extra producers/views use the same core handles as standalone Farms. */
af_status af_host_create(const af_host_options *, af_host **out);
af_farm *af_host_farm(af_host *); /* borrowed; never af_destroy this Farm */
af_status af_host_register_producer(af_host *, af_tier, af_token **out);
/* Host-owned public actors. step flushes their ready queues. Retire and
   reclaim every owner before close; the host joins consumers before destroying
   these runtimes. Never pass them to af_actor_runtime_destroy. */
af_status af_host_create_actors(af_host *, size_t capacity, const af_actor_allocator *,
                               af_actor_runtime **out);
af_status af_host_step(af_host *);
/* Any native thread; neither call requires attachment. The handle must remain
   live: quiesce external callers before close/abandon releases it. Wake is a
   no-op when not open. State queries also accept still-owned terminal hosts.
   NULL arguments return AF_INVALID; state output is unchanged on failure. */
af_status af_host_wake(af_host *);
af_status af_host_state(const af_host *, uint32_t *out_state);
/* Call wake after accepted raw writes AND on idle/retry polls. With indefinite
   parks, sleeping consumer pins can otherwise block producer reclamation.
   step performs that wake and caller progress, and reports worker failures.

   Close also returns AF_BUSY while host-owned actors remain live or queued,
   or any wave bound to the borrowed Farm remains allocated (even if ready).
   Destroy waves before close.
   Successful close invalidates their runtime and submission handles.
   Close requires application completion signals for every submitted job,
   returned tokens, unsubscribed external views, and quiescent external users.
   It joins workers; it does not infer that arbitrary C work has drained.
   AF_OK consumes the host handle. On any error it remains owned by the caller;
   repair AF_BUSY and retry. A terminal host can be closed again after a cleanup
   diagnostic. Failed D hosts follow FarmHost's abandonment policy. */
af_status af_host_close(af_host *);
/* Emergency shutdown: stop/join workers, intentionally retain the Farm's ring
   and unfinished work (including actors, waves and allocator contexts), and
   consume the host handle. All associated C handles become invalid. Return
   external tokens and views first; waves may remain and are deliberately
   leaked. Keep retained code/contexts resident. This is for unrecoverable failure. */
af_status af_host_abandon(af_host *);

/* Status diagnostics use af_last_error_message()/af_last_os_error(). No D
   exception crosses these exports. Callbacks still must not throw/longjmp. */
#ifdef __cplusplus
}
static_assert(sizeof(af_host_options) == 88 && alignof(af_host_options) == 8 &&
              offsetof(af_host_options, idle_timeout_ns) == 24, "host options layout");
#else
_Static_assert(sizeof(af_host_options) == 88 && _Alignof(af_host_options) == 8 &&
               offsetof(af_host_options, idle_timeout_ns) == 24, "host options layout");
#endif
#endif
