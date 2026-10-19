#include "antfarm_runtime.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
extern void antfarm_test_collect(void);
static unsigned *results;
static test_atomic pending, foreign_ready, foreign_stop;
static af_host *live_host;
static unsigned abandoned_calls;
static void abandoned_job(const uint64_t *b, size_t n) { (void)b; (void)n; ++abandoned_calls; }
enum { ITEMS = 300000 };
static void job(const uint64_t *b, size_t n) {
    assert(n == 1 && b[0] < ITEMS); results[b[0]] = (unsigned)b[0] + 1;
}
static void table_done(void *p) { test_add((test_atomic *)p, UINT32_MAX); }
static void nested(const uint64_t *b, size_t n) { (void)b; (void)n; af_host_step(live_host); }
static void fatal_notice(const char *s, size_t n, void *p) {
    assert(p == &pending); fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
static void check(af_status status) {
    if (status != AF_OK) { fprintf(stderr, "status %u: %s\n", status, af_last_error_message()); abort(); }
}
static af_host *make_host(unsigned workers) {
    af_host_options o; af_host_options_default(&o);
    o.workers = workers; o.ring_mib = 2; o.extra_consumers = 1;
    o.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *h = NULL; check(af_host_create(&o, &h)); return h;
}
static void exercise(unsigned workers) {
    af_host *h = make_host(workers);
    assert(af_runtime_shutdown() == AF_BUSY);
    assert(af_host_state(h) == AF_HOST_OPEN);
    af_farm *f = af_host_farm(h); assert(f);
    af_view *v = NULL; check(af_view_create(&v)); check(af_subscribe(v, f));
    assert(af_host_close(h) == AF_BUSY); af_unsubscribe(v); af_view_destroy(v);
    af_token *t = NULL, *excess = NULL; check(af_host_register_producer(h, AF_SMALL, &t));
    assert(af_host_register_producer(h, AF_SMALL, &excess) == AF_FULL && excess == NULL);
    assert(af_host_close(h) == AF_BUSY);
    antfarm_test_collect(); /* the C handle must keep its D host reachable */
    uint64_t *jobs = (uint64_t *)malloc(ITEMS * sizeof *jobs); assert(jobs);
    results = (unsigned *)calloc(ITEMS, sizeof *results); assert(results);
    for (unsigned i = 0; i < ITEMS; ++i) jobs[i] = i;
    af_completion_hook hook = {&pending, table_done};
    test_store(&pending, 1); /* producer credit prevents early aggregate completion */
    size_t offset = 0;
    while (offset < ITEMS) {
        test_add(&pending, 1);
        size_t n = af_write_fixed(f, job, jobs + offset, 1, ITEMS - offset, t, 0, &hook);
        if (!n) test_add(&pending, UINT32_MAX);
        offset += n;
        check(af_host_wake(h));
        check(af_host_step(h)); /* wake idle pins even after zero acceptance */
    }
    test_add(&pending, UINT32_MAX);
    while (test_load(&pending)) check(af_host_step(h));
    for (unsigned i = 0; i < ITEMS; ++i) assert(results[i] == i + 1);
    af_unregister_producer(f, t); check(af_host_close(h));
    free(jobs); free(results); results = NULL;
    printf("C runtime host passed: %u workers, region larger than ring\n", workers);
}
TEST_THREAD(foreign) {
    af_host *h = (af_host *)arg;
    assert(af_host_wake(h) == AF_INVALID);
    check(af_runtime_attach_thread());
    assert(af_runtime_attach_thread() == AF_BUSY);
    assert(af_host_step(h) == AF_INVALID);
    check(af_host_wake(h));
    test_store(&foreign_ready, 1);
    while (!test_load(&foreign_stop)) test_yield();
    check(af_runtime_detach_thread());
    assert(af_runtime_detach_thread() == AF_INVALID);
    TEST_RETURN;
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    af_host *invalid = (af_host *)(uintptr_t)1;
    assert(af_host_create(NULL, &invalid) == AF_INVALID && invalid == NULL);
    assert(af_runtime_shutdown() == AF_INVALID);
    check(af_runtime_init()); assert(af_runtime_init() == AF_BUSY);
    check(af_runtime_attach_thread()); /* borrows the existing main-thread attachment */
    assert(af_runtime_shutdown() == AF_BUSY); check(af_runtime_detach_thread());
    if (argc > 1) {
        af_set_fatal_handler(fatal_notice, &pending);
        live_host = make_host(0); af_farm *f = af_host_farm(live_host);
        if (!strcmp(argv[1], "borrowed")) af_destroy(f);
        af_token *t = NULL; check(af_host_register_producer(live_host, AF_SMALL, &t));
        af_write_fixed(f, nested, NULL, 0, 1, t, 3, NULL); af_host_step(live_host);
        return 0;
    }
    af_host_options bad; af_host_options_default(&bad); bad.struct_size = 4;
    assert(af_host_create(&bad, &invalid) == AF_INVALID && invalid == NULL);
    af_host_options_default(&bad); bad.reserved = 1; assert(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.workers = 100000; assert(af_host_create(&bad, &invalid) == AF_INVALID);
    exercise(0); exercise(2);
    af_host *abandoned = make_host(0); af_token *abandoned_token = NULL;
    check(af_host_register_producer(abandoned, AF_SMALL, &abandoned_token));
    af_farm *abandoned_farm = af_host_farm(abandoned);
    assert(af_write_fixed(abandoned_farm, abandoned_job, NULL, 0, 1, abandoned_token, 3, NULL) == 1);
    assert(af_host_abandon(abandoned) == AF_BUSY);
    af_unregister_producer(abandoned_farm, abandoned_token);
    check(af_host_abandon(abandoned)); assert(abandoned_calls == 0);
    af_host *h = make_host(0); test_thread t = test_start(foreign, h);
    while (!test_load(&foreign_ready)) test_yield();
    antfarm_test_collect();
    check(af_host_close(h));
    assert(af_runtime_shutdown() == AF_BUSY); /* attachment alone keeps session alive */
    test_store(&foreign_stop, 1); test_join(t);
    check(af_runtime_shutdown());
    assert(af_runtime_shutdown() == AF_INVALID && af_runtime_init() == AF_INVALID);
    puts("C ABI runtime checks passed"); return 0;
}
