#include "check.h"
#include "antfarm_runtime.h"
#include "abi1_images.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
extern void antfarm_test_collect(void);
extern void antfarm_test_busy_attachment(af_host *, int *);
extern void antfarm_test_cleanup_failure(af_host *);
extern void antfarm_test_error_failure(af_host *);
static unsigned *results;
static test_atomic pending, foreign_ready, foreign_stop;
static af_host *live_host;
static unsigned abandoned_calls;
static void abandoned_job(const uint64_t *b, size_t n) { (void)b; (void)n; ++abandoned_calls; }
enum { ITEMS = 300000 };
static void job(const uint64_t *b, size_t n) {
    CHECK(n == 1 && b[0] < ITEMS); results[b[0]] = (unsigned)b[0] + 1;
}
static void table_done(void *p) { test_add((test_atomic *)p, UINT32_MAX); }
static void nested(const uint64_t *b, size_t n) { (void)b; (void)n; af_host_step(live_host); }
static void gc_in_callback(const uint64_t *b, size_t n) { (void)b; (void)n; af_druntime_gc_collect(); }
static size_t emit_control(void *context, af_token *token) { (void)token; af_host_step((af_host *)context); return 0; }
static void fatal_notice(const char *s, size_t n, void *p) {
    CHECK(p == &pending); fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
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
    CHECK(af_druntime_shutdown() == AF_BUSY);
    uint32_t state; check(af_host_state(h, &state)); CHECK(state == AF_HOST_OPEN);
    af_farm *f = af_host_farm(h); CHECK(f);
    af_view *v = NULL; check(af_view_create(&v)); check(af_subscribe(v, f));
    CHECK(af_host_close(h) == AF_BUSY); af_unsubscribe(v); af_view_destroy(v);
    af_token *t = NULL, *excess = NULL; check(af_host_register_producer(h, AF_SMALL, &t));
    CHECK(af_host_register_producer(h, AF_SMALL, &excess) == AF_FULL && excess == NULL);
    CHECK(af_host_close(h) == AF_BUSY);
    antfarm_test_collect(); /* the C handle must keep its D host reachable */
    uint64_t *jobs = (uint64_t *)malloc(ITEMS * sizeof *jobs); CHECK(jobs);
    results = (unsigned *)calloc(ITEMS, sizeof *results); CHECK(results);
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
    for (unsigned i = 0; i < ITEMS; ++i) CHECK(results[i] == i + 1);
    af_unregister_producer(f, t); check(af_host_close(h));
    free(jobs); free(results); results = NULL;
    printf("C runtime host passed: %u workers, region larger than ring\n", workers);
}
static test_atomic producer_blocked, release_jobs;
typedef struct { af_host *host; af_farm *farm; uint64_t *jobs; } producer_args;
static void gated_job(const uint64_t *b, size_t n) {
    while (!test_load(&release_jobs)) test_yield();
    job(b, n);
}
TEST_THREAD(native_producer) {
    producer_args *p = (producer_args *)arg;
    af_token *token = NULL; check(af_register_producer(p->farm, AF_SMALL, &token));
    uint32_t state; check(af_host_state(p->host, &state)); CHECK(state == AF_HOST_OPEN);
    af_completion_hook hook = {&pending, table_done};
    test_store(&pending, 1);
    size_t offset = 0;
    while (offset < ITEMS) {
        test_add(&pending, 1);
        size_t n = af_write_fixed(p->farm, gated_job, p->jobs + offset, 1,
                                 ITEMS - offset, token, 0, &hook);
        if (!n) { test_add(&pending, UINT32_MAX); test_store(&producer_blocked, 1); }
        offset += n;
        check(af_host_wake(p->host)); /* also wake after backpressure */
    }
    test_add(&pending, UINT32_MAX);
    while (test_load(&pending)) { check(af_host_wake(p->host)); test_yield(); }
    for (unsigned i = 0; i < ITEMS; ++i) CHECK(results[i] == i + 1);
    af_unregister_producer(p->farm, token);
    /* No attach call occurred anywhere on this native thread. */
    CHECK(af_druntime_detach_thread() == AF_INVALID);
    TEST_RETURN;
}
/* ABI 2: attachments, a timed wait, resolve, summary and GC control. */
static unsigned emits, finishes; static int attachment_drained; static af_farm *attached_farm;
static test_atomic attached_pending;
static void job_42(const uint64_t *b, size_t n) { CHECK(n == 1 && b[0] == 42); }
static size_t emit(void *context, af_token *token) {
    CHECK(context == &emits); ++emits;
    if (emits > 3) return 0;
    uint64_t word = 42; static af_completion_hook hook = {&attached_pending, table_done}; /* stable until notification */
    test_add(&attached_pending, 1);
    size_t n = af_write_fixed(attached_farm, job_42, &word, 1, 1, token, AF_COST_AUTO, &hook);
    if (!n) test_add(&attached_pending, UINT32_MAX);
    return n;
}
static uint32_t drained(void *context) { CHECK(context == &emits); return attachment_drained; }
static void finish(void *context) { CHECK(context == &emits); ++finishes; }
static uint32_t emitted_three(void *context) { (void)context; return emits >= 3; }
static uint32_t pending_done(void *context) { return test_load((test_atomic *)context) == 0; }
static uint32_t never(void *context) { (void)context; return 0; }
static void exercise_abi2(void) {
    af_host_options o; af_host_options_default(&o);
    o.workers = 2; o.ring_mib = 2; o.idle_timeout_ns = AF_WAIT_FOREVER;
    o.wait_timeout_ns = 20ull * 1000 * 1000;
    size_t count = 99; check(af_host_resolve(&o, NULL, 0, &count)); CHECK(count == 2);
    af_processor cpus[128]; check(af_host_resolve(&o, cpus, 128, &count)); CHECK(count == 2);
    CHECK(af_host_resolve(&o, cpus, 1, &count) == AF_FULL && count == 2);
    CHECK(af_host_resolve(&o, NULL, 1, &count) == AF_INVALID);
    CHECK(af_host_resolve(&o, cpus, 128, NULL) == AF_INVALID);
    af_host *h = NULL; check(af_host_create(&o, &h)); attached_farm = af_host_farm(h);
    char text[512]; size_t len = 0;
    check(af_host_summary(h, text, sizeof text, &len)); CHECK(len > 0 && strlen(text) == len);
    CHECK(af_host_summary(h, text, 1, &len) == AF_FULL && len > 0);
    CHECK(af_host_summary(h, NULL, 1, &len) == AF_INVALID);
    af_attachment a = { sizeof a, AF_ABI_VERSION, "C telemetry", emit, drained, finish, &emits, 0 };
    af_attachment bad = a; bad.struct_size = 8; CHECK(af_host_attach(h, &bad) == AF_INVALID);
    bad = a; bad.reserved = 1; CHECK(af_host_attach(h, &bad) == AF_INVALID);
    bad = a; bad.emit = NULL; bad.drained = NULL; bad.finish = NULL; CHECK(af_host_attach(h, &bad) == AF_INVALID);
    CHECK(af_host_attach(h, NULL) == AF_INVALID);
    check(af_host_attach(h, &a));
    test_store(&attached_pending, 0);
    check(af_host_wait_until(h, emitted_three, NULL, AF_WAIT_FOREVER));
    check(af_host_wait_until(h, pending_done, &attached_pending, AF_WAIT_FOREVER)); /* the three tables completed */
    CHECK(af_host_wait_until(h, never, NULL, AF_WAIT_FOREVER) == AF_HOST_TIMED_OUT); /* the options' 20 ms; host open */
    CHECK(strstr(af_last_error_message(), "not done"));
    CHECK(af_host_wait_until(h, never, NULL, 5ull * 1000 * 1000) == AF_HOST_TIMED_OUT);
    CHECK(af_host_wait_until(h, NULL, NULL, AF_WAIT_FOREVER) == AF_INVALID);
    uint32_t state; check(af_host_state(h, &state)); CHECK(state == AF_HOST_OPEN);
    CHECK(af_host_close(h) == AF_BUSY); /* the attachment is not drained */
    attachment_drained = 1;
    check(af_druntime_gc_disable()); check(af_druntime_gc_collect());
    check(af_druntime_gc_enable()); check(af_druntime_gc_minimize());
    check(af_host_close(h)); CHECK(finishes == 1 && emits >= 3);
    puts("C ABI 2 host passed: resolve, summary, attachment, wait_until, GC control");
}
static void exercise_foreign_producer(void) {
    af_host_options o; af_host_options_default(&o);
    o.workers = 2; o.caller_consumes = 0; o.ring_mib = 2;
    o.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *h = NULL; check(af_host_create(&o, &h));
    uint64_t *jobs = (uint64_t *)malloc(ITEMS * sizeof *jobs); CHECK(jobs);
    results = (unsigned *)calloc(ITEMS, sizeof *results); CHECK(results);
    for (unsigned i = 0; i < ITEMS; ++i) jobs[i] = i;
    producer_args args = {h, af_host_farm(h), jobs};
    test_thread thread = test_start(native_producer, &args);
    /* Hold callbacks until the ring fills, proving the zero-acceptance path. */
    while (!test_load(&producer_blocked)) test_yield();
    antfarm_test_collect();
    test_store(&release_jobs, 1);
    test_join(thread); check(af_host_close(h));
    free(jobs); free(results); results = NULL;
    puts("C unattached producer passed: parked workers, backpressure, state, join");
}
TEST_THREAD(foreign) {
    af_host *h = (af_host *)arg;
    check(af_host_wake(h));
    uint32_t state; check(af_host_state(h, &state)); CHECK(state == AF_HOST_OPEN);
    CHECK(af_druntime_gc_collect() == AF_INVALID); /* GC control needs attachment */
    check(af_druntime_attach_thread());
    CHECK(af_druntime_attach_thread() == AF_BUSY);
    CHECK(af_host_step(h) == AF_INVALID);
    check(af_druntime_gc_collect()); /* an attached foreign thread may collect */
    check(af_host_wake(h));
    test_store(&foreign_ready, 1);
    while (!test_load(&foreign_stop)) test_yield();
    check(af_druntime_detach_thread());
    CHECK(af_druntime_detach_thread() == AF_INVALID);
    TEST_RETURN;
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    af_host *invalid = (af_host *)(uintptr_t)1;
    CHECK(af_host_create(NULL, &invalid) == AF_INVALID && invalid == NULL);
    CHECK(af_druntime_shutdown() == AF_INVALID);
    check(af_druntime_init()); CHECK(af_druntime_init() == AF_BUSY);
    check(af_druntime_attach_thread()); /* borrows the existing main-thread attachment */
    CHECK(af_druntime_shutdown() == AF_BUSY); check(af_druntime_detach_thread());
    setvbuf(stdout, NULL, _IONBF, 0); /* progress survives a crash in the runner's capture */
    if (argc > 1) {
        af_set_fatal_handler(fatal_notice, &pending);
        live_host = make_host(0); af_farm *f = af_host_farm(live_host);
        if (!strcmp(argv[1], "error")) { /* a D Error in close aborts; it is never a status */
            antfarm_test_error_failure(live_host); af_host_close(live_host);
            fprintf(stderr, "death case returned: error\n"); return 0;
        }
        if (!strcmp(argv[1], "emit-control")) { /* a control call inside an attachment */
            af_attachment a = { sizeof a, AF_ABI_VERSION, "bad", emit_control, NULL, NULL, live_host, 0 };
            check(af_host_attach(live_host, &a)); af_host_step(live_host);
            fprintf(stderr, "death case returned: emit-control\n"); return 0;
        }
        if (!strcmp(argv[1], "borrowed")) af_destroy(f);
        af_token *t = NULL; check(af_host_register_producer(live_host, AF_SMALL, &t));
        if (!strcmp(argv[1], "gc-callback")) { /* GC control inside a Farm callback */
            af_write_fixed(f, gc_in_callback, NULL, 0, 1, t, 3, NULL); af_host_step(live_host);
            fprintf(stderr, "death case returned: gc-callback\n"); return 0;
        }
        af_write_fixed(f, nested, NULL, 0, 1, t, 3, NULL); af_host_step(live_host);
        return 0;
    }
    af_host_options bad; af_host_options_default(&bad); bad.struct_size = 4;
    CHECK(af_host_create(&bad, &invalid) == AF_INVALID && invalid == NULL);
    af_host_options_default(&bad); bad.reserved = 1; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.abi_version = 0; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af1_host_options image; af1_host_options_default(&image); image.workers = 0; /* the ABI 1 image, by its own size and version */
    af_host *previous = NULL; check(af_host_create((const af_host_options *)&image, &previous)); check(af_host_close(previous));
    af_host_options_default(&bad); bad.abi_version = AF_ABI_VERSION_MIN; /* a version-1 tag on the ABI 2 size is not an image */
    CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.max_emit_rounds = 0; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.reserved2 = 1; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.group_mode = 3; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.group_index = 1; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_host_options_default(&bad); bad.wait_timeout_ns = (uint64_t)INT64_MAX + 1; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    af_build_info info = { sizeof info, AF_ABI_VERSION, NULL, NULL, 0, 0 };
    check(af_build_info_get(&info)); CHECK(info.features & AF_FEATURE_RUNTIME);
    af_host_options_default(&bad); bad.workers = 100000; CHECK(af_host_create(&bad, &invalid) == AF_INVALID);
    uint32_t state = 99;
    CHECK(af_host_wake(NULL) == AF_INVALID);
    CHECK(af_host_state(NULL, &state) == AF_INVALID && state == 99);
    af_host *busy = make_host(0); int blocked = 1;
    CHECK(af_host_state(busy, NULL) == AF_INVALID);
    antfarm_test_busy_attachment(busy, &blocked);
    CHECK(af_host_close(busy) == AF_BUSY);
    check(af_host_state(busy, &state)); CHECK(state == AF_HOST_OPEN);
    blocked = 0; check(af_host_close(busy));
    af_host *terminal = make_host(0);
    antfarm_test_cleanup_failure(terminal);
    CHECK(af_host_close(terminal) == AF_RUNTIME_ERROR);
    check(af_host_state(terminal, &state)); CHECK(state == AF_HOST_CLOSED);
    check(af_host_wake(terminal)); check(af_host_close(terminal));
    exercise(0); exercise(2); exercise_foreign_producer(); exercise_abi2();
    af_host *abandoned = make_host(0); af_token *abandoned_token = NULL;
    check(af_host_register_producer(abandoned, AF_SMALL, &abandoned_token));
    af_farm *abandoned_farm = af_host_farm(abandoned);
    CHECK(af_write_fixed(abandoned_farm, abandoned_job, NULL, 0, 1, abandoned_token, 3, NULL) == 1);
    CHECK(af_host_abandon(abandoned) == AF_BUSY);
    af_unregister_producer(abandoned_farm, abandoned_token);
    check(af_host_abandon(abandoned)); CHECK(abandoned_calls == 0);
    af_host *h = make_host(0); test_thread t = test_start(foreign, h);
    while (!test_load(&foreign_ready)) test_yield();
    antfarm_test_collect();
    check(af_host_close(h));
    CHECK(af_druntime_shutdown() == AF_BUSY); /* attachment alone keeps session alive */
    test_store(&foreign_stop, 1); test_join(t);
    check(af_druntime_shutdown());
    CHECK(af_druntime_shutdown() == AF_INVALID && af_druntime_init() == AF_INVALID);
    puts("C ABI runtime checks passed"); return 0;
}
