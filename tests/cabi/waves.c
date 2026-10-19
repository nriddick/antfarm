#include "check.h"
#include "antfarm_actors.h"
#include "threads.h"
#include "actor_helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%u: %s\n", s, af_last_error_message()); abort(); }
}
typedef struct fixture { af_farm *farm; af_token *token; af_view *view; af_actor_runtime *runtime; const af_actor_kind *kinds[2]; } fixture;
static af_view *subscribe(af_farm *f) {
    af_view *v = NULL; check(af_view_create(&v)); af_status s;
    do { s = af_subscribe(v, f); } while (s == AF_RETRY);
    check(s); return v;
}
static void register_kinds(fixture *x);
static fixture setup(size_t capacity) {
    fixture x = {0}; af_options o; af_options_default(&o);
    o.ring_mib = 2; o.max_small_producers = 1; o.max_bulk_producers = 0;
    o.small_quota_words = 64; o.expected_consumers = 4;
    check(af_create(&o, &x.farm)); check(af_register_producer(x.farm, AF_SMALL, &x.token));
    check(af_actor_runtime_create(x.farm, capacity, NULL, &x.runtime)); x.view = subscribe(x.farm);
    register_kinds(&x);
    return x;
}
static void release_view(af_view *v) { af_unsubscribe(v); af_view_destroy(v); }
static void teardown(fixture *x) {
    release_view(x->view); af_actor_runtime_destroy(x->runtime);
    af_unregister_producer(x->farm, x->token); af_destroy(x->farm);
}
static void retire(fixture *x, af_actor_owner *owner) {
    CHECK(af_actor_request_retire(x->runtime, owner, AF_ACTOR_DRAIN) == AF_ACTOR_RETIRE_REQUESTED);
    while (!is_retired(x->runtime, owner)) {
        af_actor_runtime_flush(x->runtime, x->token, 256, 4); af_consume_next(x->view);
    }
    CHECK(af_actor_reclaim(x->runtime, owner) == AF_ACTOR_RECLAIMED);
}
typedef struct state { unsigned *output; unsigned value, id; const af_actor_kind *kind; } state;
static void autonomous(void *u, void *memory, size_t bytes, af_actor_context *c) {
    (void)u; (void)bytes; (void)c; state *s = (state *)memory; *s->output = ++s->value;
}
static void register_kinds(fixture *x) {
    x->kinds[0] = test_kind(x->runtime, sizeof(state), _Alignof(state), autonomous, NULL);
    x->kinds[1] = test_kind(x->runtime, sizeof(state) + 16, 64, autonomous, NULL);
}
static af_actor_owner create(fixture *x, unsigned *output, unsigned id, af_actor_handle *h) {
    struct { state s; uint64_t padding[2]; } initial = {{output, 0, id, x->kinds[id % 2]}, {0, 0}};
    af_actor_owner o = {0};
    check(af_actor_create_copy(x->runtime, x->kinds[id % 2], &initial, &o));
    af_actor_owner_handle(&o, h); return o;
}
static void operation(void *u, const af_actor_kind *kind, void *memory, size_t bytes) {
    CHECK(kind != NULL);
    state *s = (state *)memory; unsigned delta = *(unsigned *)u; CHECK(kind == s->kind);
    CHECK(bytes == (s->id % 2 ? sizeof(state) + 16 : sizeof(state)));
    if (s->id % 2) CHECK((uintptr_t)memory % 64 == 0);
    s->value += delta; *s->output = s->value;
}
static af_actor_wave *begin(fixture *x, unsigned *delta, const af_completion_hook *hook, af_actor_wave_handle *h) {
    af_actor_wave *w = NULL; check(af_actor_wave_create(x->farm, &w));
    af_actor_wave_begin(w, operation, delta, hook); af_actor_wave_observe(w, h); return w;
}
static void finish(fixture *x, af_actor_wave *w, af_actor_wave_handle *h) {
    af_actor_wave_seal(w);
    while (!af_actor_wave_ready(h)) af_consume_next(x->view);
    CHECK(af_actor_wave_finished(h));
}
static void counted(void *u) { ++*(unsigned *)u; }
static test_atomic filler_calls;
static void filler(const uint64_t *body, size_t words) {
    (void)body; CHECK(words == 0); test_add(&filler_calls, 1);
}
static void empty(void) {
    fixture x = setup(1); unsigned delta = 1, hooks = 0; af_actor_wave_handle h;
    af_actor_wave *w = NULL; check(af_actor_wave_create(x.farm, &w));
    af_actor_wave_observe(w, &h); CHECK(!af_actor_wave_valid(&h)); af_actor_wave_destroy(w);
    af_completion_hook hook = {&hooks, counted}; w = begin(&x, &delta, &hook, &h);
    af_actor_wave_seal(w);
    CHECK(hooks == 1 && af_actor_wave_ready(&h) && af_actor_wave_finished(&h));
    CHECK(!af_actor_wave_length(&h) && !af_actor_wave_progress(&h));
    af_actor_wave_destroy(w); teardown(&x);
}
static void serial(void) {
    enum { N = 128 }; fixture x = setup(N);
    fixture second = x; check(af_actor_runtime_create(x.farm, N, NULL, &second.runtime)); register_kinds(&second);
    unsigned outputs[N] = {0}, delta = 3, notifications = 0;
    af_actor_owner owners[N]; af_actor_handle actors[N];
    for (unsigned i = 0; i < N; ++i) owners[i] = create(i % 2 ? &second : &x, outputs + i, i, actors + i);
    af_completion_hook hook = {&notifications, counted}; af_actor_wave_handle h;
    af_actor_wave *w = begin(&x, &delta, &hook, &h);
    /* Fill the ring with independent raw work. Failed admission must neither
       reserve an actor nor leave a table credit behind. */
    size_t filled = 0, n;
    while ((n = af_write_fixed(x.farm, filler, NULL, 0, 128, x.token, 4, NULL)) != 0) filled += n;
    CHECK(filled && !test_load(&filler_calls));
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){actors, sizeof(af_actor_handle), N}, x.token, 4) == 0);
    CHECK(!af_actor_wave_failed(&h) && !af_actor_wave_length(&h));
    while (test_load(&filler_calls) < filled) af_consume_next(x.view);
    size_t offset = 0;
    while (offset < N) {
        n = af_actor_wave_publish(w, (af_actor_handle_span){actors + offset, sizeof(af_actor_handle), N - offset}, x.token, 4);
        CHECK(!af_actor_wave_failed(&h));
        if (!offset && n) CHECK(n < N);
        offset += n; af_consume_next(x.view);
    }
    while (af_actor_wave_progress(&h) != af_actor_wave_length(&h)) af_consume_next(x.view);
    CHECK(!af_actor_wave_finished(&h) && !notifications); /* producer credit */
    CHECK(af_actor_wake(actors) == AF_ACTOR_WAVE_OWNED);
    CHECK(af_actor_request_retire(x.runtime, &owners[0], AF_ACTOR_CANCEL) == AF_ACTOR_RETIRE_WAVE_OWNED);
    af_actor_message storage, *m = &storage; af_actor_message_init(m, NULL, 0);
    CHECK(af_actor_send(actors, m) == AF_ACTOR_WAVE_OWNED && af_actor_message_available(m));
    finish(&x, w, &h);
    CHECK(notifications == 1 && !af_actor_wave_failed(&h) && !af_actor_wave_interrupted(&h));
    for (unsigned i = 0; i < N; ++i) CHECK(outputs[i] == 3);
    af_actor_wave_handle old = h; delta = 4; af_actor_wave_begin(w, operation, &delta, NULL);
    af_actor_wave_observe(w, &h);
    CHECK(!af_actor_wave_valid(&old) && !af_actor_wave_finished(&old) && !af_actor_wave_ready(&old));
    CHECK(!af_actor_wave_failed(&old) && !af_actor_wave_interrupted(&old));
    CHECK(!af_actor_wave_length(&old) && !af_actor_wave_progress(&old));
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){NULL, sizeof(af_actor_handle), 0}, x.token, 4) == 0);
    for (offset = 0; offset < N;) {
        n = af_actor_wave_publish(w, (af_actor_handle_span){actors + offset, sizeof(af_actor_handle), N - offset}, x.token, 7);
        CHECK(!af_actor_wave_failed(&h)); offset += n; af_consume_next(x.view);
    }
    finish(&x, w, &h);
    for (unsigned i = 0; i < N; ++i) CHECK(outputs[i] == 7);
    af_actor_wave_destroy(w);
    for (unsigned i = 0; i < N; ++i) retire(i % 2 ? &second : &x, &owners[i]);
    af_actor_runtime_destroy(second.runtime); teardown(&x);
}
static void rejections(void) {
    fixture x = setup(3), foreign = setup(1); unsigned output[4] = {0}, delta = 1;
    af_actor_handle a[4]; af_actor_owner o[4];
    for (unsigned i = 0; i < 3; ++i) o[i] = create(&x, output + i, i, a + i);
    o[3] = create(&foreign, output + 3, 3, a + 3);
    retire(&x, &o[2]); /* stale generation while its slot remains allocated */
    af_actor_wave_handle h; af_actor_wave *w = begin(&x, &delta, NULL, &h);
    for (unsigned which = 0; which < 4; ++which) {
        if (which) { af_actor_wave_begin(w, operation, &delta, NULL); af_actor_wave_observe(w, &h); }
        if (which == 3) CHECK(af_actor_wake(a + 1) == AF_ACTOR_QUEUED);
        static const unsigned rejected[] = {0, 2, 3, 1};
        af_actor_handle offered[] = {a[0], a[rejected[which]]};
        CHECK(af_actor_wave_publish(w, (af_actor_handle_span){offered, sizeof(af_actor_handle), 2}, x.token, 4) == 0);
        CHECK(af_actor_wave_failed(&h) && af_actor_wave_length(&h) == 0);
        finish(&x, w, &h);
        CHECK(output[0] == 0); /* all prospective reservations rolled back */
    }
    while (output[1] == 0) { af_actor_runtime_flush(x.runtime, x.token, 32, 4); af_consume_next(x.view); }
    af_actor_wave_begin(w, operation, &delta, NULL); af_actor_wave_observe(w, &h);
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){a, sizeof(af_actor_handle), 1}, x.token, 4) == 1);
    while (!af_actor_wave_progress(&h)) af_consume_next(x.view);
    CHECK(!af_actor_wave_finished(&h) && output[0] == 1);
    /* Membership persists even after this table finishes. An overlapping wave
       fails, and a duplicate later table cannot erase the accepted prefix. */
    af_actor_wave_handle other_h; af_actor_wave *other = begin(&x, &delta, NULL, &other_h);
    CHECK(af_actor_wave_publish(other, (af_actor_handle_span){a, sizeof(af_actor_handle), 1}, x.token, 4) == 0 && af_actor_wave_failed(&other_h));
    finish(&x, other, &other_h); af_actor_wave_destroy(other);
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){a, sizeof(af_actor_handle), 1}, x.token, 4) == 0 && af_actor_wave_failed(&h));
    finish(&x, w, &h); CHECK(output[0] == 1 && af_actor_wave_length(&h) == 1);
    af_actor_wave_destroy(w); retire(&x, &o[0]); retire(&x, &o[1]); retire(&foreign, &o[3]);
    teardown(&foreign); teardown(&x);
}
typedef struct worker { af_farm *farm; test_atomic started, stop; test_atomic *run; } worker;
TEST_THREAD(consume) {
    worker *w = (worker *)arg; af_view *v = subscribe(w->farm); test_store(&w->started, 1);
    while (w->run && !test_load(w->run)) test_yield();
    while (!test_load(&w->stop)) { af_consume_next(v); test_yield(); }
    release_view(v); TEST_RETURN;
}

static void concurrent(void) {
    enum { N = 128 }; fixture x = setup(N); unsigned output[N] = {0}, delta = 1;
    af_actor_owner owners[N]; af_actor_handle actors[N];
    for (unsigned i = 0; i < N; ++i) owners[i] = create(&x, output + i, i, actors + i);
    worker workers[3] = {0}; test_thread threads[3];
    for (unsigned i = 0; i < 3; ++i) { workers[i].farm = x.farm; threads[i] = test_start(consume, workers + i); }
    for (unsigned i = 0; i < 3; ++i) while (!test_load(&workers[i].started)) test_yield();
    af_actor_wave_handle h; af_actor_wave *w = begin(&x, &delta, NULL, &h);
    for (unsigned round = 1; round <= 100; ++round) {
        if (round > 1) { af_actor_wave_begin(w, operation, &delta, NULL); af_actor_wave_observe(w, &h); }
        for (size_t offset = 0; offset < N;) {
            offset += af_actor_wave_publish(w, (af_actor_handle_span){actors + offset, sizeof(af_actor_handle), N - offset}, x.token, 7);
            CHECK(!af_actor_wave_failed(&h)); af_consume_next(x.view);
        }
        finish(&x, w, &h);
        for (unsigned i = 0; i < N; ++i) CHECK(output[i] == round);
    }
    af_actor_wave_destroy(w);
    for (unsigned i = 0; i < 3; ++i) test_store(&workers[i].stop, 1);
    for (unsigned i = 0; i < 3; ++i) test_join(threads[i]);
    for (unsigned i = 0; i < N; ++i) retire(&x, &owners[i]);
    teardown(&x);
}
typedef struct tail { test_atomic entered, release; af_actor_wave_handle *handle; unsigned result; } tail;
static void paused_hook(void *u) {
    tail *t = (tail *)u;
    CHECK(af_actor_wave_finished(t->handle) && !af_actor_wave_ready(t->handle));
    test_store(&t->entered, 1);
    while (!test_load(&t->release)) test_yield();
    t->result = 17; /* readiness, not finished, must publish this write */
}
static void callback_tail(const char *death) {
    fixture x = setup(1); unsigned output = 0, delta = 9; af_actor_handle actor;
    af_actor_owner o = create(&x, &output, 1, &actor);
    af_actor_wave_handle h; tail t = {0}; t.handle = &h;
    af_completion_hook hook = {&t, paused_hook}; af_actor_wave *w = begin(&x, &delta, &hook, &h);
    test_atomic run = {0};
    worker worker_state = {0}; worker_state.farm = x.farm; worker_state.run = &run;
    test_thread thread = test_start(consume, &worker_state);
    while (!test_load(&worker_state.started)) test_yield();
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){&actor, sizeof(af_actor_handle), 1}, x.token, 4) == 1); af_actor_wave_seal(w);
    /* Force the worker to own completion. If it finished before seal, seal
       could invoke paused_hook inline and block its own releasing thread. */
    test_store(&run, 1);
    while (!test_load(&t.entered)) test_yield();
    CHECK(af_actor_wave_finished(&h) && output == 9 && !af_actor_wave_ready(&h));
    if (death && !strcmp(death, "tail-begin")) af_actor_wave_begin(w, operation, &delta, NULL);
    if (death && !strcmp(death, "tail-destroy")) af_actor_wave_destroy(w);
    test_store(&t.release, 1);
    while (!af_actor_wave_ready(&h)) test_yield();
    CHECK(t.result == 17);
    af_actor_wave_destroy(w); /* final adapter access has joined, before worker join */
    test_store(&worker_state.stop, 1); test_join(thread);
    retire(&x, &o); teardown(&x);
}
#ifdef AF_TEST_AUDIT
extern void antfarm_test_after_publish(void (*)(void));
static fixture *audit_fixture;
static af_actor_wave_handle *audit_handle;
static void consume_before_return(void) {
    while (af_actor_wave_progress(audit_handle) != 1) af_consume_next(audit_fixture->view);
    CHECK(!af_actor_wave_finished(audit_handle));
}
static void early_completion(void) {
    fixture x = setup(1); unsigned output = 0, delta = 5, hooks = 0; af_actor_handle actor;
    af_actor_owner o = create(&x, &output, 0, &actor);
    af_actor_wave_handle h; af_completion_hook hook = {&hooks, counted};
    af_actor_wave *w = begin(&x, &delta, &hook, &h); audit_fixture = &x; audit_handle = &h;
    antfarm_test_after_publish(consume_before_return);
    CHECK(af_actor_wave_publish(w, (af_actor_handle_span){&actor, sizeof(af_actor_handle), 1}, x.token, 4) == 1);
    antfarm_test_after_publish(NULL);
    CHECK(output == 5 && hooks == 0); finish(&x, w, &h); CHECK(hooks == 1);
    af_actor_wave_destroy(w); retire(&x, &o); teardown(&x);
}
#endif
static void fatal_notice(const char *s, size_t n, void *p) {
    (void)p; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
static void deaths(const char *name) {
    af_set_fatal_handler(fatal_notice, NULL);
    if (!strncmp(name, "tail-", 5)) { callback_tail(name); return; }
    fixture x = setup(1); unsigned output = 0, delta = 1; af_actor_handle a;
    create(&x, &output, 0, &a);
    af_actor_wave *w = NULL; check(af_actor_wave_create(x.farm, &w));
    if (!strcmp(name, "uninitialized")) af_actor_wave_publish(w, (af_actor_handle_span){&a, sizeof(af_actor_handle), 1}, x.token, 4);
    if (!strcmp(name, "operation")) af_actor_wave_begin(w, NULL, NULL, NULL);
    if (!strcmp(name, "hook")) { af_completion_hook bad = {0}; af_actor_wave_begin(w, operation, &delta, &bad); }
    af_actor_wave_begin(w, operation, &delta, NULL);
    if (!strcmp(name, "begin")) af_actor_wave_begin(w, operation, &delta, NULL);
    if (!strcmp(name, "destroy")) af_actor_wave_destroy(w);
    if (!strcmp(name, "span")) af_actor_wave_publish(w, (af_actor_handle_span){NULL, sizeof(af_actor_handle), 1}, x.token, 4);
    if (!strcmp(name, "overflow")) af_actor_wave_publish(w, (af_actor_handle_span){&a, sizeof(af_actor_handle), SIZE_MAX}, x.token, 4);
    if (!strcmp(name, "foreign")) { fixture y = setup(1); af_actor_wave_publish(w, (af_actor_handle_span){&a, sizeof(af_actor_handle), 1}, y.token, 4); }
    if (!strcmp(name, "cost")) af_actor_wave_publish(w, (af_actor_handle_span){&a, sizeof(af_actor_handle), 1}, x.token, 8);
    af_actor_wave_seal(w);
    if (!strcmp(name, "seal")) af_actor_wave_seal(w);
    if (!strcmp(name, "sealed")) af_actor_wave_publish(w, (af_actor_handle_span){&a, sizeof(af_actor_handle), 1}, x.token, 4);
    if (!strcmp(name, "farm")) {
        release_view(x.view); af_unregister_producer(x.farm, x.token);
        /* Separate empty Farm so the wave guard is the failing precondition. */
        af_farm *f = NULL; check(af_create(NULL, &f)); af_actor_wave *empty = NULL;
        check(af_actor_wave_create(f, &empty)); af_destroy(f);
    }
    abort();
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    if (argc > 1) { deaths(argv[1]); return 1; }
    af_actor_wave_handle zero = {0};
    CHECK(!af_actor_wave_valid(&zero) && !af_actor_wave_ready(NULL));
    af_actor_wave *invalid = (af_actor_wave *)(uintptr_t)1;
    CHECK(af_actor_wave_create(NULL, &invalid) == AF_INVALID && !invalid);
    af_actor_wave_destroy(NULL);
    empty(); serial(); rejections(); concurrent(); callback_tail(NULL);
#ifdef AF_TEST_AUDIT
    early_completion();
#endif
    puts("C ABI wave checks passed"); return 0;
}
