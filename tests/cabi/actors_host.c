#include "antfarm_runtime.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
extern void antfarm_test_collect(void);
static test_atomic calls;
static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%u: %s\n", s, af_last_error_message()); abort(); }
}
static void tick(void *user, void *state, size_t bytes, af_actor_context *c) {
    assert(user == &calls && bytes == sizeof(unsigned));
    unsigned *count = (unsigned *)state; ++*count;
    if (*count == 1000) af_actor_retire(c); else af_actor_republish(c);
    test_add(&calls, 1);
}
static af_host *make_host(unsigned workers) {
    af_host_options o; af_host_options_default(&o); o.workers = workers;
    o.ring_mib = 2; o.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *h = NULL; check(af_host_create(&o, &h)); return h;
}
typedef struct creation { af_actor_runtime *runtime; af_actor_owner *owner; } creation;
TEST_THREAD(create_from_c) {
    /* Core actor operations do not call druntime, even on a host's runtime.
       This native thread deliberately never attaches. */
    creation *c = (creation *)arg; unsigned initial = 0;
    check(af_actor_create(c->runtime, &initial, sizeof initial, _Alignof(unsigned), tick, &calls, &c->owner));
    TEST_RETURN;
}
static void exercise(unsigned workers) {
    test_store(&calls, 0); af_host *h = make_host(workers);
    af_actor_runtime *r = (af_actor_runtime *)(uintptr_t)1;
    assert(af_actor_runtime_create(af_host_farm(h), 1, NULL, &r) == AF_INVALID && !r);
    assert(af_host_create_actors(h, 0, NULL, &r) == AF_INVALID && !r);
    check(af_host_create_actors(h, 1, NULL, &r));
    af_actor_runtime *empty = NULL; check(af_host_create_actors(h, 2, NULL, &empty));
    creation c = {r, NULL}; test_thread t = test_start(create_from_c, &c); test_join(t);
    assert(af_host_close(h) == AF_BUSY);
    antfarm_test_collect(); /* Also keeps the attachment delegates reachable. */
    af_actor_handle handle; af_actor_owner_handle(c.owner, &handle);
    assert(af_actor_wake(&handle) == AF_ACTOR_QUEUED);
    while (!af_actor_retired(c.owner)) { check(af_host_step(h)); test_yield(); }
    assert(test_load(&calls) == 1000 && af_host_close(h) == AF_BUSY);
    assert(af_actor_reclaim(c.owner) == AF_ACTOR_RECLAIMED);
    assert(af_actor_wake(&handle) == AF_ACTOR_STALE_HANDLE);
    check(af_host_close(h)); /* Consumers join before both actor runtimes die. */
    printf("C actor host passed: %u workers\n", workers);
}
static void fatal_notice(const char *s, size_t n, void *p) {
    (void)p; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    (void)argv; check(af_runtime_init());
    if (argc > 1) {
        af_set_fatal_handler(fatal_notice, NULL); af_host *h = make_host(0); af_actor_runtime *r = NULL;
        check(af_host_create_actors(h, 1, NULL, &r)); af_actor_runtime_destroy(r); return 1;
    }
    exercise(0); exercise(2);
    af_host *h = make_host(0); creation c = {NULL, NULL};
    check(af_host_create_actors(h, 1, NULL, &c.runtime));
    test_thread t = test_start(create_from_c, &c); test_join(t);
    af_actor_handle handle; af_actor_owner_handle(c.owner, &handle);
    assert(af_actor_wake(&handle) == AF_ACTOR_QUEUED);
    assert(af_host_close(h) == AF_BUSY);
    check(af_host_abandon(h)); /* Deliberately leaks queued actor and owner. */
    h = make_host(0); check(af_host_close(h)); /* Global host claim was released. */
    antfarm_test_collect(); check(af_runtime_shutdown());
    puts("C ABI actor host checks passed"); return 0;
}
