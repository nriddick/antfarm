#include "check.h"
#include "antfarm_runtime.h"
#include "threads.h"
#include "actor_helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
extern void antfarm_test_collect(void);
static test_atomic calls;
static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%u: %s\n", s, af_last_error_message()); abort(); }
}
static void tick(void *user, void *state, size_t bytes, af_actor_context *c) {
    CHECK(user == &calls && bytes == sizeof(unsigned));
    unsigned *count = (unsigned *)state; ++*count;
    if (*count == 1000) af_actor_retire_self(c); else af_actor_republish(c);
    test_add(&calls, 1);
}
static void phase(void *user, const af_actor_kind *kind, void *state, size_t bytes) {
    CHECK(kind != NULL);
    CHECK(user == &calls && bytes == sizeof(unsigned));
    ++*(unsigned *)state; test_add(&calls, 1);
}
static af_host *make_host(unsigned workers) {
    af_host_options o; af_host_options_default(&o); o.workers = workers;
    o.ring_mib = 2; o.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *h = NULL; check(af_host_create(&o, &h)); return h;
}
typedef struct creation { af_actor_runtime *runtime; af_actor_owner owner; const af_actor_kind *kind; } creation;
TEST_THREAD(create_from_c) {
    /* Core actor operations do not call druntime, even on a host's runtime.
       This native thread deliberately never attaches. */
    creation *c = (creation *)arg; unsigned initial = 0;
    check(af_actor_create_copy(c->runtime, c->kind, &initial, &c->owner));
    TEST_RETURN;
}
static void exercise(unsigned workers) {
    test_store(&calls, 0); af_host *h = make_host(workers);
    af_actor_runtime *r = (af_actor_runtime *)(uintptr_t)1;
    CHECK(af_actor_runtime_create(af_host_farm(h), 1, NULL, &r) == AF_INVALID && !r);
    CHECK(af_host_create_actors(h, 0, NULL, &r) == AF_INVALID && !r);
    check(af_host_create_actors(h, 1, NULL, &r));
    af_actor_runtime *empty = NULL; check(af_host_create_actors(h, 2, NULL, &empty));
    creation c = {0}; c.runtime = r; c.kind = test_kind(r, sizeof(unsigned), _Alignof(unsigned), tick, &calls); test_thread t = test_start(create_from_c, &c); test_join(t);
    CHECK(af_host_close(h) == AF_BUSY);
    antfarm_test_collect(); /* Also keeps the attachment delegates reachable. */
    af_actor_handle handle; af_actor_owner_handle(&c.owner, &handle);
    af_actor_wave *wave = NULL; check(af_actor_wave_create(af_host_farm(h), &wave));
    af_actor_wave_begin(wave, phase, &calls, NULL);
    af_actor_wave_handle completion; af_actor_wave_observe(wave, &completion);
    af_token *token = NULL; check(af_host_register_producer(h, AF_SMALL, &token));
    CHECK(af_actor_wave_publish(wave, (af_actor_handle_span){&handle, sizeof handle, 1}, token, 4) == 1);
    af_actor_wave_seal(wave);
    while (!af_actor_wave_ready(&completion)) { check(af_host_step(h)); test_yield(); }
    CHECK(!af_actor_wave_failed(&completion) && test_load(&calls) == 1);
    af_unregister_producer(af_host_farm(h), token);
    CHECK(af_actor_wake(&handle) == AF_ACTOR_QUEUED);
    while (!is_retired(c.runtime, &c.owner)) { check(af_host_step(h)); test_yield(); }
    CHECK(test_load(&calls) == 1000 && af_host_close(h) == AF_BUSY);
    CHECK(af_actor_reclaim(c.runtime, &c.owner) == AF_ACTOR_RECLAIMED);
    CHECK(af_actor_wake(&handle) == AF_ACTOR_STALE_HANDLE);
    CHECK(af_host_close(h) == AF_BUSY); /* A ready wave still owns its Farm binding. */
    af_actor_wave_destroy(wave);
    check(af_host_close(h)); /* Consumers join before both actor runtimes die. */
    printf("C actor host passed: %u workers\n", workers);
}

typedef struct blocked_creation { creation c; test_atomic entered, release; af_status result; } blocked_creation;
static af_status blocked_init(void *user, void *state, size_t bytes, size_t index) {
    blocked_creation *b = (blocked_creation *)user; (void)index; CHECK(bytes == sizeof(unsigned));
    *(unsigned *)state = 0; test_store(&b->entered, 1);
    while (!test_load(&b->release)) test_yield();
    return AF_OK;
}
TEST_THREAD(create_blocked) {
    blocked_creation *b = (blocked_creation *)arg;
    b->result = af_actor_create_init(b->c.runtime, b->c.kind, blocked_init, b, 0, &b->c.owner);
    TEST_RETURN;
}
static void reserved_host_close(void) {
    af_host *h = make_host(0); blocked_creation b = {0};
    check(af_host_create_actors(h, 1, NULL, &b.c.runtime));
    b.c.kind = test_kind(b.c.runtime, sizeof(unsigned), _Alignof(unsigned), tick, &calls);
    test_thread thread = test_start(create_blocked, &b);
    while (!test_load(&b.entered)) test_yield();
    CHECK(af_actor_runtime_live(b.c.runtime) == 1 && af_host_close(h) == AF_BUSY);
    test_store(&b.release, 1); test_join(thread); check(b.result);
    CHECK(af_actor_request_retire(b.c.runtime, &b.c.owner, AF_ACTOR_DRAIN) == AF_ACTOR_RETIRE_REQUESTED);
    CHECK(af_actor_reclaim(b.c.runtime, &b.c.owner) == AF_ACTOR_RECLAIMED);
    check(af_host_close(h));
}

static void fatal_notice(const char *s, size_t n, void *p) {
    (void)p; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    (void)argv; check(af_druntime_init());
    if (argc > 1) {
        af_set_fatal_handler(fatal_notice, NULL); af_host *h = make_host(0); af_actor_runtime *r = NULL;
        check(af_host_create_actors(h, 1, NULL, &r)); af_actor_runtime_destroy(r); return 1;
    }
    exercise(0); exercise(2); reserved_host_close();
    af_host *h = make_host(0); creation c = {0};
    check(af_host_create_actors(h, 1, NULL, &c.runtime));
    memset(&c.owner, 0, sizeof c.owner); c.kind = test_kind(c.runtime, sizeof(unsigned), _Alignof(unsigned), tick, &calls);
    test_thread t = test_start(create_from_c, &c); test_join(t);
    af_actor_handle handle; af_actor_owner_handle(&c.owner, &handle);
    CHECK(af_actor_wake(&handle) == AF_ACTOR_QUEUED);
    CHECK(af_host_close(h) == AF_BUSY);
    check(af_host_abandon(h)); /* Deliberately leaks queued actor and owner. */
    h = make_host(0); check(af_host_create_actors(h, 1, NULL, &c.runtime));
    memset(&c.owner, 0, sizeof c.owner); c.kind = test_kind(c.runtime, sizeof(unsigned), _Alignof(unsigned), tick, &calls);
    t = test_start(create_from_c, &c); test_join(t); af_actor_owner_handle(&c.owner, &handle);
    af_actor_wave *wave = NULL; check(af_actor_wave_create(af_host_farm(h), &wave));
    af_actor_wave_begin(wave, phase, &calls, NULL);
    af_token *token = NULL; check(af_host_register_producer(h, AF_SMALL, &token));
    CHECK(af_actor_wave_publish(wave, (af_actor_handle_span){&handle, sizeof handle, 1}, token, 4) == 1); af_actor_wave_seal(wave);
    af_unregister_producer(af_host_farm(h), token);
    CHECK(af_host_close(h) == AF_BUSY);
    check(af_host_abandon(h)); /* Unfinished wave storage/code/contexts are retained too. */
    h = make_host(0); check(af_host_close(h)); /* Global host claim was released. */
    antfarm_test_collect(); check(af_druntime_shutdown());
    puts("C ABI actor host checks passed"); return 0;
}
