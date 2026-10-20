/* Ant Farm C ABI 1. BSL-1.0. Windows/Linux, 64-bit. */
#ifndef ANTFARM_H
#define ANTFARM_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#define AF_STATIC_ASSERT(c, m) static_assert(c, m)
#define AF_ALIGNOF(t) alignof(t)
#else
#define AF_STATIC_ASSERT(c, m) _Static_assert(c, m)
#define AF_ALIGNOF(t) _Alignof(t)
#endif

#define AF_ABI_VERSION 1u
#define AF_DEFAULT_COST 3u
#define AF_MAX_COST 7u
typedef struct af_farm af_farm;
typedef struct af_token af_token;
typedef struct af_view af_view;
typedef void (*af_callback)(const uint64_t *, size_t);
typedef void (*af_completion_callback)(void *);
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

/* af_status registry: every value in every ABI 1 header, never reused.
     0-7    antfarm.h          AF_OK .. AF_RUNTIME_ERROR
     8      antfarm_actors.h   AF_ACTOR_INITIALIZER_FAILED
     9-10   antfarm_private.h  AF_PRIVATE_RETIRING, AF_PRIVATE_EMPTY
   New statuses take the next free value of their header's range: antfarm.h
   64-79, antfarm_actors.h 80-95, antfarm_private.h 96-111, antfarm_runtime.h
   112-127. 11-63 stay unassigned: actor retire (16-20) and reclaim (32-34)
   outcomes share their return value with AF_INVALID. */
typedef uint32_t af_status;
enum {
    AF_OK = 0, AF_INVALID = 1, AF_FULL = 2, AF_RETRY = 3,
    AF_ALLOCATION_FAILED = 4, AF_SYSTEM_ERROR = 5,
    AF_BUSY = 6, AF_RUNTIME_ERROR = 7
};
typedef uint32_t af_tier;
enum { AF_SMALL = 0, AF_BULK = 1 };
/* Initialize with af_options_default. ABI 1 requires struct_size == sizeof
   (af_options), abi_version == AF_ABI_VERSION and reserved == 0. Size is a
   layout check, not support for truncated or future option structures. */
typedef struct af_options {
    uint32_t struct_size, abi_version;
    uint32_t ring_mib, segments, expected_consumers, max_bulk_producers;
    uint64_t bulk_quota_words;
    uint32_t max_small_producers, small_table_threshold;
    uint64_t small_quota_words;
    uint32_t huge_pages, reserved;
} af_options;

AF_STATIC_ASSERT(sizeof(void *) == 8, "Ant Farm ABI 1 requires 64-bit pointers");
AF_STATIC_ASSERT(sizeof(af_completion_hook) == 16, "completion hook size");
AF_STATIC_ASSERT(offsetof(af_completion_hook, context) == 0, "hook context offset");
AF_STATIC_ASSERT(offsetof(af_completion_hook, call) == 8, "hook callback offset");
AF_STATIC_ASSERT(sizeof(af_entry) == 24 && offsetof(af_entry, body_words) == 16, "entry layout");
AF_STATIC_ASSERT(sizeof(af_body) == 16 && offsetof(af_body, count) == 8, "body layout");
AF_STATIC_ASSERT(sizeof(af_options) == 56 && offsetof(af_options, small_quota_words) == 40, "options layout");

AF_STATIC_ASSERT(AF_ALIGNOF(af_options) == 8 && AF_ALIGNOF(af_entry) == 8 &&
                 AF_ALIGNOF(af_body) == 8 && AF_ALIGNOF(af_completion_hook) == 8, "natural ABI alignment");

uint32_t af_abi_version(void);
void af_options_default(af_options *out);
/* NULL options uses defaults. A valid out is cleared even on failure.
   Geometry and invalid settings return AF_INVALID; OS mapping/mutex failures
   return AF_SYSTEM_ERROR. ANTFARM_HUGE_PAGES overrides huge_pages. */
af_status af_create(const af_options *options, af_farm **out);
/* NULL is a no-op. All participants must be released and threads joined. */
void af_destroy(af_farm *farm);
uint32_t af_can_fit(const af_farm *farm, af_callback call, size_t body_words, af_tier tier);
af_status af_register_producer(af_farm *farm, af_tier tier, af_token **out);
/* Consumes token. Use only with its issuing Farm. */
void af_unregister_producer(af_farm *farm, af_token *token);

/* All writes are allocation-free and return the accepted prefix length.
   Zero publishes no table and invokes no hook. Retry the remaining suffix.
   Bodies are copied before return; pointed-to objects are NOT copied.
   Pointers may be NULL only for zero-length spans. Length arithmetic is checked.
   Cost must be 0..7. Invalid callbacks, spans, tokens or impossible singleton
   sizes are fatal. The token and input storage have one exclusive writer.

   The optional hook joins all callbacks in this physical table and CAN RUN
   BEFORE WRITE RETURNS. Reserve a completion credit before publication.
   Its storage/context must remain stable until notification. The hook may
   release its own storage as its final access; otherwise retain it through
   callback return. Hooks and callbacks must not throw or longjmp. */
size_t af_write(af_farm *, const af_entry *, size_t count, af_token *, uint32_t avg_cost,
                const af_completion_hook *);
size_t af_write_bodies(af_farm *, af_callback, const af_body *, size_t count,
                       af_token *, uint32_t avg_cost, const af_completion_hook *);
size_t af_write_fixed(af_farm *, af_callback, const uint64_t *bodies,
                      size_t body_words, size_t count, af_token *, uint32_t avg_cost,
                      const af_completion_hook *);

af_status af_view_create(af_view **out);
af_status af_subscribe(af_view *, af_farm *);
/* One owner per view. Walking or unsubscribing from a Farm callback/hook is
   forbidden. A false result is an idle poll, NOT a global completion fence. */
uint32_t af_consume_next(af_view *);
void af_unsubscribe(af_view *);
/* Requires an unsubscribed view. NULL is a no-op. */
void af_view_destroy(af_view *);

/* Status-returning calls replace this calling thread's diagnostic. Borrowed,
   NUL-terminated, truncated to 511 bytes; never free. Not changed by writes,
   polling, successful teardown, or these queries. OS error is errno/GetLastError
   (raw 32-bit value), or zero when not applicable. */
const char *af_last_error_message(void);
int32_t af_last_os_error(void);
typedef void (*af_fatal_handler)(const char *, size_t, void *);
/* Install at quiescence. Handler may log/dump; it must not throw, longjmp or
   re-enter Farm operations. If it returns, abort follows. NULL resets it. */
void af_set_fatal_handler(af_fatal_handler, void *context);

/* Handles are exclusive owning capabilities, not copyable values. A token
   can move to another thread only through a synchronized, quiescent handoff.
   Use-after-release, forged pointers and concurrent handle destruction are
   invalid C memory accesses, not recoverable errors. No druntime initialization
   or thread attachment is needed for any function in this header. */
#ifdef __cplusplus
}
#endif
#undef AF_ALIGNOF
#undef AF_STATIC_ASSERT
#endif
