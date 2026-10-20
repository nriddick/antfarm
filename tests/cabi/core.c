#include "check.h"
#include "antfarm.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint64_t total;
static unsigned calls, hooks;
static af_view *current_view;
static void add(const uint64_t *b, size_t n) { ++calls; for (size_t i = 0; i < n; ++i) total += b[i]; }
static void twice(const uint64_t *b, size_t n) { ++calls; for (size_t i = 0; i < n; ++i) total += 2 * b[i]; }
static void completed(void *p) { ++*(unsigned *)p; }
static void nested(const uint64_t *b, size_t n) { (void)b; (void)n; af_consume_next(current_view); }
static void nested_hook(void *p) { af_unsubscribe((af_view *)p); }
static void fatal_notice(const char *s, size_t n, void *p) {
    CHECK(p == &total);
    fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
static af_farm *make_farm(uint64_t quota) {
    af_options o; af_options_default(&o);
    o.ring_mib = 2; o.expected_consumers = 4; o.max_bulk_producers = 0;
    o.max_small_producers = 1; o.small_quota_words = quota;
    af_farm *f = NULL; CHECK(af_create(&o, &f) == AF_OK); return f;
}
static af_view *subscribe(af_farm *f) {
    af_view *v = NULL; CHECK(af_view_create(&v) == AF_OK);
    af_status s = af_subscribe(v, f);
    CHECK(s == AF_OK); return v;
}
static void release_view(af_view *v) { af_unsubscribe(v); af_view_destroy(v); }
static void check_options(void) {
    af_options o; af_options_default(&o);
    CHECK(af_abi_version() == AF_ABI_VERSION && o.struct_size == sizeof o);
    af_farm *f = (af_farm *)(uintptr_t)1;
    o.struct_size = 4; CHECK(af_create(&o, &f) == AF_INVALID && f == NULL);
    CHECK(strstr(af_last_error_message(), "size") && af_last_os_error() == 0);
    af_options_default(&o); ++o.abi_version; CHECK(af_create(&o, &f) == AF_INVALID);
    af_options_default(&o); o.reserved = 1; CHECK(af_create(&o, &f) == AF_INVALID);
    af_options_default(&o); o.ring_mib = 3; CHECK(af_create(&o, &f) == AF_INVALID);
    af_options_default(&o); o.huge_pages = 2; CHECK(af_create(&o, &f) == AF_INVALID);
    CHECK(af_create(NULL, NULL) == AF_INVALID);
    CHECK(af_create(NULL, &f) == AF_OK && af_last_error_message()[0] == 0); af_destroy(f);
    af_destroy(NULL); af_view_destroy(NULL);
}
static void serial(void) {
    af_farm *f = make_farm(64); af_token *t = NULL, *other = (af_token *)(uintptr_t)1;
    CHECK(af_register_producer(f, AF_SMALL, &t) == AF_OK);
    CHECK(af_register_producer(f, AF_SMALL, &other) == AF_FULL && other == NULL);
    CHECK(af_register_producer(f, AF_BULK, &other) == AF_FULL);
    CHECK(af_register_producer(f, 2, &other) == AF_INVALID);
    af_view *v = subscribe(f); CHECK(af_subscribe(v, f) == AF_INVALID);
    CHECK(af_can_fit(f, add, 1, AF_SMALL)); CHECK(!af_can_fit(f, add, SIZE_MAX, AF_SMALL));
    CHECK(!af_can_fit(f, NULL, 0, AF_SMALL) && !af_can_fit(f, add, 0, 2));
    uint64_t data[] = {2, 3, 5};
    af_entry entries[] = {{add, data, 3}, {twice, data, 1}, {add, NULL, 0}};
    af_completion_hook hook = {&hooks, completed};
    CHECK(af_write(f, entries, 3, t, 7, &hook) == 3);
    while (hooks < 1) af_consume_next(v);
    CHECK(calls == 3 && total == 14);
    af_body bodies[] = {{data, 3}, {data, 1}, {NULL, 0}};
    CHECK(af_write_bodies(f, add, bodies, 3, t, 7, &hook) == 3);
    while (hooks < 2) af_consume_next(v);
    CHECK(calls == 6 && total == 26);
    CHECK(af_write_fixed(f, add, NULL, 0, 3, t, 7, &hook) == 3);
    while (hooks < 3) af_consume_next(v);
    CHECK(calls == 9 && total == 26);
    CHECK(af_write(f, NULL, 0, t, 3, &hook) == 0);
    CHECK(af_write_bodies(f, add, NULL, 0, t, 3, &hook) == 0);
    CHECK(af_write_fixed(f, add, NULL, 0, 0, t, 3, &hook) == 0);
    CHECK(hooks == 3);
    uint64_t many[128]; for (size_t i = 0; i < 128; ++i) many[i] = 1;
    unsigned tables = hooks; size_t accepted = 0, n;
    while ((n = af_write_fixed(f, add, many, 1, 128, t, 7, &hook)) != 0) {
        CHECK(n < 128); accepted += n; ++tables; CHECK(tables < 100000);
    }
    CHECK(accepted > 128 && hooks == 3); /* zero admission invokes no hook */
    while (hooks < tables) af_consume_next(v);
    CHECK(total == 26 + accepted && calls == 9 + accepted);
    n = 0; for (unsigned i = 0; i < 32 && !n; ++i) {
        af_consume_next(v); n = af_write_fixed(f, add, data, 1, 1, t, 7, &hook);
    }
    CHECK(n == 1); while (hooks < tables + 1) af_consume_next(v);
    release_view(v); af_unregister_producer(f, t); af_destroy(f);
}

/* Every adapter must return an exact prefix that can be resubmitted at an
   offset; exercise both token tiers and nonuniform per-entry body lengths. */
static void partial_prefixes(void) {
    af_options o; af_options_default(&o);
    o.ring_mib = 2; o.max_small_producers = o.max_bulk_producers = 1;
    o.small_quota_words = o.bulk_quota_words = 64;
    af_farm *f = NULL; CHECK(af_create(&o, &f) == AF_OK);
    af_view *v = subscribe(f);
    uint64_t words[128][2]; af_entry entries[128]; af_body bodies[128];
    for (unsigned i = 0; i < 128; ++i) {
        words[i][0] = i + 1; words[i][1] = i + 2;
        entries[i].call = i % 2 ? twice : add;
        entries[i].body = words[i]; entries[i].body_words = i % 3;
        bodies[i].words = words[i]; bodies[i].count = i % 3;
    }
    af_completion_hook hook = {&hooks, completed};
    for (af_tier tier = AF_SMALL; tier <= AF_BULK; ++tier) {
        af_token *t = NULL; CHECK(af_register_producer(f, tier, &t) == AF_OK);
        for (unsigned form = 0; form < 3; ++form) {
            size_t offset = 0; uint64_t expected = total; unsigned before = calls;
            while (offset < 128) {
                unsigned target = hooks + 1;
                size_t n = form == 0 ? af_write(f, entries + offset, 128 - offset, t, 7, &hook) :
                    form == 1 ? af_write_bodies(f, add, bodies + offset, 128 - offset, t, 7, &hook) :
                    af_write_fixed(f, add, words[offset], 2, 128 - offset, t, 7, &hook);
                CHECK(n && n <= 128 - offset);
                if (!offset) CHECK(n < 128);
                for (size_t i = offset; i < offset + n; ++i) {
                    size_t width = form == 2 ? 2 : i % 3;
                    for (size_t j = 0; j < width; ++j)
                        expected += words[i][j] * (form == 0 && i % 2 ? 2 : 1);
                }
                offset += n;
                while (hooks < target) af_consume_next(v);
                CHECK(total == expected && calls == before + offset);
            }
        }
        af_unregister_producer(f, t);
    }
    release_view(v); af_destroy(f);
}

enum { JOBS = 1024, WORKERS = 3 };
static unsigned results[JOBS];
static test_atomic ready, stop, joined;
static af_farm *concurrent_farm;
static void job(const uint64_t *b, size_t n) { CHECK(n == 1 && b[0] < JOBS); results[b[0]] = (unsigned)b[0] + 1; }
static void join_jobs(void *p) {
    (void)p;
    for (unsigned i = 0; i < JOBS; ++i) CHECK(results[i] == i + 1);
    test_store(&joined, 1);
}
TEST_THREAD(worker) {
    af_view *v = subscribe((af_farm *)arg); test_add(&ready, 1);
    while (!test_load(&stop)) { if (!af_consume_next(v)) test_yield(); }
    release_view(v); TEST_RETURN;
}
#ifdef AF_TEST_AUDIT
extern void antfarm_test_after_publish(void (*fn)(void));
static void hold_publisher(void) { while (!test_load(&joined)) test_yield(); }
#endif
static void concurrent(void) {
    concurrent_farm = make_farm(16384);
    af_token *t = NULL; CHECK(af_register_producer(concurrent_farm, AF_SMALL, &t) == AF_OK);
    test_thread threads[WORKERS];
    for (unsigned i = 0; i < WORKERS; ++i) threads[i] = test_start(worker, concurrent_farm);
    while (test_load(&ready) != WORKERS) test_yield();
    uint64_t jobs[JOBS]; for (unsigned i = 0; i < JOBS; ++i) jobs[i] = i;
    af_completion_hook hook = {NULL, join_jobs};
#ifdef AF_TEST_AUDIT
    antfarm_test_after_publish(hold_publisher);
#endif
    CHECK(af_write_fixed(concurrent_farm, job, jobs, 1, JOBS, t, 0, &hook) == JOBS);
#ifdef AF_TEST_AUDIT
    CHECK(test_load(&joined) == 1); /* finisher ran before publication returned */
    antfarm_test_after_publish(NULL);
#endif
    while (!test_load(&joined)) test_yield();
    test_store(&stop, 1);
    for (unsigned i = 0; i < WORKERS; ++i) test_join(threads[i]);
    af_unregister_producer(concurrent_farm, t); af_destroy(concurrent_farm);
}
enum { REGISTER_THREADS = 4, REGISTER_ROUNDS = 1000 };
static test_atomic registration_start;
TEST_THREAD(register_churn) {
    af_farm *f = (af_farm *)arg;
    while (!test_load(&registration_start)) test_yield();
    for (unsigned i = 0; i < REGISTER_ROUNDS; ++i) {
        af_token *token = NULL;
        CHECK(af_register_producer(f, AF_SMALL, &token) == AF_OK && token);
        af_unregister_producer(f, token);
    }
    TEST_RETURN;
}
static void concurrent_registration(void) {
    af_options o; af_options_default(&o);
    o.ring_mib = 2; o.max_small_producers = REGISTER_THREADS;
    af_farm *f = NULL; CHECK(af_create(&o, &f) == AF_OK);
    test_thread threads[REGISTER_THREADS];
    for (unsigned i = 0; i < REGISTER_THREADS; ++i) threads[i] = test_start(register_churn, f);
    test_store(&registration_start, 1);
    for (unsigned i = 0; i < REGISTER_THREADS; ++i) test_join(threads[i]);
    /* No token/registration count leaked through contention. */
    af_token *tokens[REGISTER_THREADS];
    for (unsigned i = 0; i < REGISTER_THREADS; ++i)
        CHECK(af_register_producer(f, AF_SMALL, &tokens[i]) == AF_OK);
    af_token *excess = NULL; CHECK(af_register_producer(f, AF_SMALL, &excess) == AF_FULL);
    for (unsigned i = 0; i < REGISTER_THREADS; ++i) af_unregister_producer(f, tokens[i]);
    af_destroy(f);
}
static void death(const char *which) {
    af_set_fatal_handler(fatal_notice, &total);
    af_farm *f = make_farm(64); af_token *t = NULL;
    CHECK(af_register_producer(f, AF_SMALL, &t) == AF_OK);
    current_view = subscribe(f);
    uint64_t word = 1; af_completion_hook hook = {NULL, NULL};
    if (!strcmp(which, "callback")) af_write_fixed(f, NULL, NULL, 0, 1, t, 3, NULL);
    if (!strcmp(which, "cost")) af_write_fixed(f, add, NULL, 0, 0, t, 8, NULL);
    if (!strcmp(which, "span")) af_write_fixed(f, add, NULL, 1, 1, t, 3, NULL);
    if (!strcmp(which, "overflow")) af_write_fixed(f, add, &word, SIZE_MAX, 2, t, 3, NULL);
    if (!strcmp(which, "entry-overflow")) af_write(f, (const af_entry *)&word, SIZE_MAX, t, 3, NULL);
    if (!strcmp(which, "hook")) af_write_fixed(f, add, &word, 1, 1, t, 3, &hook);
    if (!strcmp(which, "foreign")) af_write_fixed(make_farm(64), add, &word, 1, 1, t, 3, NULL);
    if (!strcmp(which, "farm-live")) af_destroy(f);
    if (!strcmp(which, "token-live")) { release_view(current_view); af_destroy(f); }
    if (!strcmp(which, "view-live")) af_view_destroy(current_view);
    if (!strcmp(which, "nested")) { af_write_fixed(f, nested, NULL, 0, 1, t, 3, NULL); af_consume_next(current_view); }
    if (!strcmp(which, "nested-hook")) { hook.context = current_view; hook.call = nested_hook;
        af_write_fixed(f, add, NULL, 0, 1, t, 3, &hook); af_consume_next(current_view); }
    fprintf(stderr, "death case returned: %s\n", which); exit(0);
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    if (argc > 1) death(argv[1]);
    check_options(); serial(); partial_prefixes(); concurrent(); concurrent_registration();
    puts("C ABI core checks passed"); return 0;
}
