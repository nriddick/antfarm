/* Optional druntime-inclusive non-Fiber FarmHost. Link with antfarm_core. */
#ifndef ANTFARM_RUNTIME_H
#define ANTFARM_RUNTIME_H
#include "antfarm_private.h"
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
enum { AF_GROUP_AUTOMATIC = 0, AF_GROUP_ALL = 1, AF_GROUP_ONLY = 2 };
/* ABI 2 statuses of the runtime range. */
enum { AF_HOST_TIMED_OUT = 112, AF_HOST_WORKER_FAILED = 113 };
typedef struct af_processor {
    uint16_t group, processor;
    uint16_t llc, reserved;
} af_processor;
/* Initialize with af_host_options_default. Accepted by its own size and
   version: struct_size == sizeof (af_host_options) with abi_version
   AF_ABI_VERSION, or the ABI 1 image (88 bytes, the fields through reserved)
   with abi_version 1, the ABI 2 fields then taking their defaults. Reserved
   words are zero; any other size or version is AF_INVALID.
   ABI 2 fields: wait_timeout_ns bounds af_host_wait_until when that call is
   passed AF_WAIT_FOREVER (default AF_WAIT_FOREVER: no deadline);
   max_emit_rounds bounds the emission rounds of one step (16; positive);
   group_mode, group_index and farm_per_group select the host's LLC groups as
   the group host's options do (AF_GROUP_AUTOMATIC, 0, 0). */
typedef struct af_host_options {
    uint32_t struct_size, abi_version;
    uint32_t workers, caller_consumes, caller_placement, idle;
    uint64_t idle_timeout_ns;
    uint32_t ring_mib, segments, extra_small_producers, bulk_producers;
    uint64_t bulk_quota_words, small_quota_words;
    uint32_t extra_consumers, small_table_threshold, huge_pages, skip_smt_siblings;
    uint64_t reserved;
    uint64_t wait_timeout_ns;
    uint32_t max_emit_rounds, group_mode, group_index, farm_per_group;
    uint64_t reserved2;
} af_host_options;

/* The D runtime session (hence the prefix: unrelated to af_actor_runtime).
   One explicit session per process. Call init/shutdown serially on the same
   control thread, with lifecycle operations in other D libraries quiescent.
   A balanced druntime reference preserves an embedding D application's own
   initialization. Shutdown returns AF_BUSY until hosts, dispatcher/GroupHost handles, root
   completions and attachments close.
   No runtime function may race shutdown. ABI 1 does not support restarting
   a session after shutdown. A failed init that had already initialized
   druntime (topology discovery failing, for example) terminates it and ends
   the session the same way: later init calls return AF_INVALID. */
af_status af_druntime_init(void);
af_status af_druntime_shutdown(void);
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
af_status af_druntime_attach_thread(void);
af_status af_druntime_detach_thread(void);

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
   Destroy waves and private root parks before close. An allocated private park
   also returns AF_BUSY, even when empty. Pump private parks explicitly with
   their emitter token; step does not pump them.
   Successful close invalidates their runtime and submission handles.
   Close requires application completion signals for every submitted job,
   returned tokens, unsubscribed external views, and quiescent external users.
   It joins workers; it does not infer that arbitrary C work has drained.
   AF_OK consumes the host handle. On any error it remains owned by the caller;
   repair AF_BUSY and retry. A terminal host can be closed again after a cleanup
   diagnostic. Failed D hosts follow FarmHost's abandonment policy. */
af_status af_host_close(af_host *);
/* Emergency shutdown: stop/join workers, intentionally retain the Farm's ring
   and unfinished work (including public/private actors, waves and allocator contexts), and
   consume the host handle. All associated C handles become invalid. Return
   external tokens and views first; waves may remain and are deliberately
   leaked. Keep retained code/contexts resident. This is for unrecoverable failure. */
af_status af_host_abandon(af_host *);

/* ABI 2. One emitter or service the host drives from every step, on the
   control thread. emit runs with the host's control token once per emission
   round and returns the items it published (zero is not drained); the token
   is the host's: write with it, never unregister or keep it. drained is the
   close precondition (NULL: always drained). finish runs after consumers stop
   and before runtimes, parks and the Farm are destroyed. All three return
   promptly, never wait for a worker, never throw or longjmp, and make no
   control call (fatal, as in any runtime callback). context stays live until
   finish has run or the host is abandoned. The structure is copied; at least
   one callback is required. */
typedef struct af_attachment {
    uint32_t struct_size, abi_version;
    const char *name;                       /* for the host's reports; may be NULL */
    size_t (*emit)(void *context, af_token *);
    uint32_t (*drained)(void *context);
    void (*finish)(void *context);
    void *context;
    uint64_t reserved;
} af_attachment;
af_status af_host_attach(af_host *, const af_attachment *);
/* Transfers a park created on af_host_farm(host): step pumps it with the
   host's token and the automatic cost, close requires it empty (live roots
   block close; take tracked outcomes in a wait predicate, the host never
   does) and destroys it. Never pump or destroy it yourself afterwards. */
af_status af_host_attach_park(af_host *, af_private_roots *);
typedef uint32_t (*af_host_predicate)(void *context);
/* while (!done(context)) step, on the control thread. AF_OK once done returns
   nonzero; AF_HOST_TIMED_OUT when timeout_ns elapsed first (AF_WAIT_FOREVER
   selects the options' wait_timeout_ns), the host still open; AF_HOST_WORKER_
   FAILED when a pool worker failed, after which only close and abandon remain.
   done runs before every step and may take outcomes and poll; it is a runtime
   callback, so a control call inside it is fatal. */
af_status af_host_wait_until(af_host *, af_host_predicate, void *context, uint64_t timeout_ns);
/* The worker CPU set the options would select, without starting it. Query
   with NULL/0; AF_FULL writes the required count and no partial output.
   Control thread only. */
af_status af_host_resolve(const af_host_options *, af_processor *out, size_t capacity, size_t *count);
/* The host's one-line summary, NUL-terminated, into out; len receives its
   length without the terminator. AF_FULL when capacity < len + 1, writing
   nothing but len. Attached threads. */
af_status af_host_summary(const af_host *, char *out, size_t capacity, size_t *len);
/* The D garbage collector, for a host that embeds the runtime from C.
   Attached threads, outside Farm callbacks and runtime callbacks (fatal). A
   collection suspends attached threads, pool workers included, for its
   duration; unattached C threads keep running. */
af_status af_druntime_gc_collect(void);
af_status af_druntime_gc_minimize(void);
af_status af_druntime_gc_enable(void);
af_status af_druntime_gc_disable(void);

/* Status diagnostics use af_last_error_message()/af_last_os_error(). No D
   exception crosses these exports: a D Exception is AF_RUNTIME_ERROR with its
   message, a D Error aborts through the fatal handler. Callbacks still must
   not throw/longjmp. */
#ifdef __cplusplus
}
static_assert(sizeof(af_host_options) == 120 && alignof(af_host_options) == 8 &&
              offsetof(af_host_options, idle_timeout_ns) == 24 &&
              offsetof(af_host_options, wait_timeout_ns) == 88, "host options layout");
static_assert(sizeof(af_attachment) == 56 && offsetof(af_attachment, context) == 40, "attachment layout");
#else
_Static_assert(sizeof(af_host_options) == 120 && _Alignof(af_host_options) == 8 &&
               offsetof(af_host_options, idle_timeout_ns) == 24 &&
               offsetof(af_host_options, wait_timeout_ns) == 88, "host options layout");
_Static_assert(sizeof(af_attachment) == 56 && offsetof(af_attachment, context) == 40, "attachment layout");
#endif
#endif
