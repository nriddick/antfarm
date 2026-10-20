#include "check.h"
#include "antfarm_actors.h"
#include "threads.h"
#include "actor_helpers.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <malloc.h>
#endif
static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%u: %s\n", s, af_last_error_message()); abort(); }
}
static af_farm *make_farm(void) {
    af_options o; af_options_default(&o); o.ring_mib = 2; o.max_bulk_producers = 0;
    o.max_small_producers = 1; o.expected_consumers = 4; o.small_quota_words = 1024;
    af_farm *f = NULL; check(af_create(&o, &f)); return f;
}
static af_view *subscribe(af_farm *f) {
    af_view *v = NULL; check(af_view_create(&v)); af_status s;
    do { s = af_subscribe(v, f); } while (s == AF_RETRY);
    check(s); return v;
}
static void release_view(af_view *v) { af_unsubscribe(v); af_view_destroy(v); }
typedef struct fixture { af_farm *farm; af_actor_runtime *runtime; af_token *token; af_view *view; } fixture;
static fixture setup(size_t capacity, const af_actor_allocator *allocator) {
    fixture x = {0}; x.farm = make_farm();
    check(af_actor_runtime_create(x.farm, capacity, allocator, &x.runtime));
    check(af_register_producer(x.farm, AF_SMALL, &x.token)); x.view = subscribe(x.farm); return x;
}
static void step(fixture *x) {
    af_actor_runtime_flush(x->runtime, x->token, 256, AF_ACTOR_DEFAULT_COST);
    af_consume_next(x->view);
}
static void retire(fixture *x, af_actor_owner *o, unsigned mode) {
    unsigned r = af_actor_request_retire(x->runtime, o, mode);
    CHECK(r == AF_ACTOR_RETIRE_REQUESTED || r == AF_ACTOR_ALREADY_RETIRED);
    for (unsigned i = 0; !is_retired(x->runtime, o); ++i) { CHECK(i < 100000); step(x); }
    CHECK(af_actor_reclaim(x->runtime, o) == AF_ACTOR_RECLAIMED);
}
static void teardown(fixture *x) {
    CHECK(af_actor_runtime_live(x->runtime) == 0);
    release_view(x->view); af_actor_runtime_destroy(x->runtime);
    af_unregister_producer(x->farm, x->token); af_destroy(x->farm);
}
typedef struct state { unsigned calls, sum; } state;
typedef struct report { unsigned calls, sum, expected_tag, alignment, limit; af_actor_context *saved; } report;
static void handler(void *user, void *memory, size_t bytes, af_actor_context *c) {
    report *r = (report *)user; state *s = (state *)memory;
    CHECK(bytes == sizeof *s && (uintptr_t)memory % r->alignment == 0);
    r->saved = c; ++s->calls; r->calls = s->calls;
    if (af_actor_cancelling(c)) { CHECK(af_actor_closing(c)); af_actor_cancel_inbox(c); return; }
    af_actor_message *m;
    unsigned count = 0;
    while (count < r->limit && (m = af_actor_pop(c)) != NULL) {
        CHECK(af_actor_message_tag(m) == r->expected_tag++);
        unsigned *value = (unsigned *)af_actor_message_payload(m);
        s->sum += *value; *value += 10;
        if (count == 0) af_actor_message_complete(m);
        else if (count == 1) af_actor_message_fail(m, 0x123456);
        else af_actor_message_cancel(m);
        ++count;
    }
    r->sum = s->sum;
}
static void basic(void) {
    fixture x = setup(1, NULL); state initial = {7, 0}; report r = {0}; r.alignment = 64; r.limit = 3;
    af_actor_owner o = {0}, extra = {0};
    const af_actor_kind *kind = test_kind(x.runtime, sizeof initial, 64, handler, &r);
    check(af_actor_create_copy(x.runtime, kind, &initial, &o));
    CHECK(initial.calls == 7 && af_actor_runtime_live(x.runtime) == 1);
    CHECK(af_actor_create_copy(x.runtime, kind, &initial, &extra) == AF_FULL && !extra._slot);
    CHECK(af_actor_reclaim(x.runtime, &o) == AF_ACTOR_RECLAIM_BUSY);
    af_actor_handle h; af_actor_owner_handle(&o, &h);
    CHECK(af_actor_wake(&h) == AF_ACTOR_QUEUED);
    CHECK(af_actor_wake(&h) == AF_ACTOR_COALESCED && af_actor_runtime_ready(x.runtime) == 1);
    CHECK(af_actor_runtime_flush(x.runtime, x.token, 0, 4) == 0);
    while (r.calls != 8) step(&x);
    enum { N = 7 }; unsigned values[N]; af_actor_message storage[N], *messages[N];
    for (unsigned i = 0; i < N; ++i) {
        values[i] = i + 1; messages[i] = storage + i; af_actor_message_init(messages[i], values + i, i);
        CHECK(af_actor_message_available(messages[i]));
        unsigned sent = af_actor_send(&h, messages[i]); CHECK(sent <= AF_ACTOR_COALESCED);
    }
    CHECK(af_actor_send(&h, messages[0]) == AF_ACTOR_NODE_BUSY);
    while (!af_actor_message_available(messages[N - 1])) step(&x);
    CHECK(r.expected_tag == N && r.sum == 28 && initial.sum == 0);
    for (unsigned i = 0; i < N; ++i) {
        CHECK(values[i] == i + 11);
        CHECK(af_actor_message_outcome(messages[i]) == i % 3);
        CHECK(af_actor_message_failure(messages[i]) == (i % 3 == 1 ? 0x123456u : 0));
    }
    /* Reuse preserves the old reply until completion of this interaction. */
    af_actor_message_set(messages[1], &values[0], N);
    CHECK(af_actor_message_outcome(messages[1]) == AF_ACTOR_FAILED);
    CHECK(af_actor_send(&h, messages[1]) <= AF_ACTOR_COALESCED);
    while (!af_actor_message_available(messages[1])) step(&x);
    CHECK(af_actor_message_outcome(messages[1]) == AF_ACTOR_COMPLETED && r.sum == 39);
    /* Cancel an accepted interaction before flushing. */
    af_actor_message_set(messages[0], &values[0], N + 1);
    CHECK(af_actor_send(&h, messages[0]) <= AF_ACTOR_COALESCED);
    CHECK(af_actor_request_retire(x.runtime, &o, AF_ACTOR_CANCEL) == AF_ACTOR_RETIRE_REQUESTED);
    CHECK(af_actor_wake(&h) == AF_ACTOR_CLOSED);
    CHECK(af_actor_send(&h, messages[2]) == AF_ACTOR_CLOSED && af_actor_message_available(messages[2]));
    while (!is_retired(x.runtime, &o)) step(&x);
    CHECK(af_actor_message_available(messages[0]) && af_actor_message_outcome(messages[0]) == AF_ACTOR_CANCELLED);
    CHECK(af_actor_reclaim(x.runtime, &o) == AF_ACTOR_RECLAIMED);
    CHECK(af_actor_wake(&h) == AF_ACTOR_STALE_HANDLE);
    check(af_actor_create_copy(x.runtime, kind, &initial, &o));
    CHECK(af_actor_send(&h, messages[0]) == AF_ACTOR_STALE_HANDLE && af_actor_message_available(messages[0]));
    retire(&x, &o, AF_ACTOR_DRAIN);
    teardown(&x);
}
typedef struct allocator_log { size_t calls, frees, outstanding, fail_at; unsigned fail; } allocator_log;
static void *custom_alloc(void *ctx, size_t bytes, size_t alignment) {
    allocator_log *a = (allocator_log *)ctx; ++a->calls;
    if (a->fail || (a->fail_at && a->calls == a->fail_at)) return NULL;
    if (alignment < sizeof(void *)) alignment = sizeof(void *);
#ifdef _WIN32
    void *p = _aligned_malloc(bytes, alignment);
#else
    CHECK(bytes <= SIZE_MAX - alignment + 1);
    void *p = aligned_alloc(alignment, (bytes + alignment - 1) & ~(alignment - 1));
#endif
    if (p) ++a->outstanding;
    return p;
}
static void custom_free(void *ctx, void *p, size_t bytes, size_t alignment) {
    allocator_log *a = (allocator_log *)ctx; CHECK(p && bytes && alignment);
    CHECK((uintptr_t)p % alignment == 0 && a->outstanding); --a->outstanding; ++a->frees;
#ifdef _WIN32
    _aligned_free(p);
#else
    free(p);
#endif
}
static void invalid_and_allocation(void) {
    af_farm *f = make_farm(); af_actor_runtime *r = (af_actor_runtime *)(uintptr_t)1;
    CHECK(af_actor_runtime_create(f, 0, NULL, &r) == AF_INVALID && !r);
    CHECK(af_actor_runtime_create(f, SIZE_MAX, NULL, &r) == AF_INVALID && !r);
    af_actor_allocator bad = {0};
    CHECK(af_actor_runtime_create(f, 1, &bad, &r) == AF_INVALID);
    allocator_log log = {0}; log.fail = 1;
    af_actor_allocator a = {&log, custom_alloc, custom_free};
    CHECK(af_actor_runtime_create(f, 1, &a, &r) == AF_ALLOCATION_FAILED && !r && !log.outstanding);
    log.fail = 0; check(af_actor_runtime_create(f, 1, &a, &r));
    state s = {0}; report report_ = {0}; report_.alignment = 256;
    af_actor_owner o = {0};
    af_actor_kind_options options = {handler, &report_, 0, 8, NULL};
    const af_actor_kind *kind = NULL;
    CHECK(af_actor_kind_register(r, &options, &kind) == AF_INVALID && !kind);
    options.state_bytes = sizeof s; options.state_alignment = 3;
    CHECK(af_actor_kind_register(r, &options, &kind) == AF_INVALID);
    options.state_bytes = SIZE_MAX; options.state_alignment = 8;
    CHECK(af_actor_kind_register(r, &options, &kind) == AF_INVALID);
    options.state_bytes = sizeof s; options.handler = NULL;
    CHECK(af_actor_kind_register(r, &options, &kind) == AF_INVALID);
    kind = test_kind(r, sizeof s, 256, handler, &report_);
    log.fail = 1;
    CHECK(af_actor_create_copy(r, kind, &s, &o) == AF_ALLOCATION_FAILED && !o._slot);
    log.fail = 0; check(af_actor_create_copy(r, kind, &s, &o));
    fixture x = {f, r, NULL, subscribe(f)}; check(af_register_producer(f, AF_SMALL, &x.token));
    af_actor_handle h; af_actor_owner_handle(&o, &h); CHECK(af_actor_wake(&h) == AF_ACTOR_QUEUED);
    while (!report_.calls) step(&x);
    retire(&x, &o, AF_ACTOR_DRAIN); teardown(&x);
    CHECK(log.outstanding == 0 && log.calls == log.frees + 2);
    af_actor_runtime_destroy(NULL);
    CHECK(af_actor_wake(NULL) == AF_ACTOR_STALE_HANDLE);
}

/* Republish state remains exclusive even when activations migrate among three
   native consumers. No D runtime or foreign-thread registration is present. */
static test_atomic ready, stop, finished, entered;
static unsigned count_result;
static void repeating(void *user, void *memory, size_t n, af_actor_context *ctx) {
    (void)user; CHECK(n == sizeof(unsigned));
    CHECK(test_add(&entered, 1) == 0);
    unsigned *value = (unsigned *)memory;
    if (++*value < 2000) af_actor_republish(ctx);
    else { count_result = *value; af_actor_retire(ctx); test_store(&finished, 1); }
    CHECK(test_add(&entered, UINT32_MAX) == 1);
}
TEST_THREAD(consumer) {
    af_view *v = subscribe((af_farm *)arg); test_add(&ready, 1);
    while (!test_load(&stop)) { if (!af_consume_next(v)) test_yield(); }
    release_view(v); TEST_RETURN;
}
static void concurrent(void) {
    af_farm *f = make_farm(); af_actor_runtime *r = NULL; af_actor_owner o = {0}; af_token *t = NULL;
    check(af_actor_runtime_create(f, 1, NULL, &r)); check(af_register_producer(f, AF_SMALL, &t));
    unsigned initial = 0; const af_actor_kind *kind = test_kind(r, sizeof initial, _Alignof(unsigned), repeating, NULL);
    check(af_actor_create_copy(r, kind, &initial, &o));
    test_thread workers[3]; for (unsigned i = 0; i < 3; ++i) workers[i] = test_start(consumer, f);
    while (test_load(&ready) != 3) test_yield();
    af_actor_handle h; af_actor_owner_handle(&o, &h); CHECK(af_actor_wake(&h) == AF_ACTOR_QUEUED);
    while (!is_retired(r, &o)) { af_actor_runtime_flush(r, t, 32, 4); test_yield(); }
    CHECK(test_load(&finished) && count_result == 2000);
    CHECK(af_actor_reclaim(r, &o) == AF_ACTOR_RECLAIMED);
    test_store(&stop, 1); for (unsigned i = 0; i < 3; ++i) test_join(workers[i]);
    af_actor_runtime_destroy(r); af_unregister_producer(f, t); af_destroy(f);
}

typedef struct creator { af_actor_runtime *runtime; const af_actor_kind *kind; unsigned success; } creator;
static void unused(void *u, void *s, size_t n, af_actor_context *c) { (void)u; (void)s; (void)n; (void)c; }
TEST_THREAD(create_reclaim) {
    creator *c = (creator *)arg;
    for (unsigned i = 0; i < 200; ++i) {
        unsigned s = 0; af_actor_owner o = {0};
        af_status result = af_actor_create_copy(c->runtime, c->kind, &s, &o);
        if (result == AF_FULL) { test_yield(); continue; }
        check(result); ++c->success;
        CHECK(af_actor_request_retire(c->runtime, &o, AF_ACTOR_DRAIN) == AF_ACTOR_RETIRE_REQUESTED);
        CHECK(is_retired(c->runtime, &o) && af_actor_reclaim(c->runtime, &o) == AF_ACTOR_RECLAIMED);
    }
    TEST_RETURN;
}
static void concurrent_creation(void) {
    af_farm *f = make_farm(); af_actor_runtime *r = NULL; check(af_actor_runtime_create(f, 1, NULL, &r));
    const af_actor_kind *kind = test_kind(r, sizeof(unsigned), _Alignof(unsigned), unused, NULL);
    creator callers[3]; test_thread threads[3]; unsigned total = 0;
    for (unsigned i = 0; i < 3; ++i) {
        callers[i].runtime = r; callers[i].kind = kind; callers[i].success = 0;
        threads[i] = test_start(create_reclaim, callers + i);
    }
    for (unsigned i = 0; i < 3; ++i) { test_join(threads[i]); total += callers[i].success; }
    CHECK(total && !af_actor_runtime_live(r));
    af_actor_runtime_destroy(r); af_destroy(f);
}


typedef struct refresh_state { size_t index; void *address; } refresh_state;
typedef struct refresh_log { test_atomic destroyed; size_t fail_index, calls, indices[16]; } refresh_log;
static af_status init_state(void *user, void *memory, size_t bytes, size_t index) {
    refresh_log *log = (refresh_log *)user; CHECK(bytes == sizeof(refresh_state));
    CHECK(log->calls < 16); log->indices[log->calls++] = index;
    if (index == log->fail_index) return AF_SYSTEM_ERROR;
    refresh_state *s = (refresh_state *)memory; s->index = index; s->address = memory;
    return AF_OK;
}
static void destroy_state(void *user, void *memory, size_t bytes) {
    refresh_log *log = (refresh_log *)user; refresh_state *s = (refresh_state *)memory;
    CHECK(bytes == sizeof *s && (!s->address || s->address == memory));
    test_add(&log->destroyed, 1);
}
static void batch_contracts(void) {
    enum { N = 5 };
    typedef struct entity { uint64_t before; refresh_state initial; af_actor_owner owner;
        af_actor_handle handle; uint32_t result; uint64_t after; } entity;
    entity e[N] = {0}; allocator_log allocations = {0};
    af_actor_allocator allocator = {&allocations, custom_alloc, custom_free};
    fixture x = setup(N, &allocator); refresh_log log = {0}; log.fail_index = 102;
    af_actor_kind_options options = {unused, &log, sizeof(refresh_state), 64, destroy_state};
    const af_actor_kind *kind = NULL; check(af_actor_kind_register(x.runtime, &options, &kind));
    size_t baseline = allocations.calls;
    af_actor_owner_output_span owners = {&e[0].owner, sizeof(entity), N};
    af_actor_result_span results = {&e[0].result, sizeof(entity), N};
    /* Structural and output preflight must precede reservation/allocation. */
    af_actor_owner_output_span bad = owners; bad.stride = 8;
    CHECK(af_actor_create_init_batch(x.runtime, kind, init_state, &log, 100, bad).status == AF_INVALID);
    bad = owners; bad.count = SIZE_MAX;
    CHECK(af_actor_create_init_batch(x.runtime, kind, init_state, &log, 100, bad).status == AF_INVALID);
    CHECK(af_actor_create_init_batch(x.runtime, kind, init_state, &log, SIZE_MAX, owners).status == AF_INVALID);
    e[4].owner._generation = 1;
    CHECK(af_actor_create_init_batch(x.runtime, kind, init_state, &log, 100, owners).status == AF_INVALID);
    e[4].owner._generation = 0;
    CHECK(allocations.calls == baseline && !log.calls);
    af_actor_batch_result r = af_actor_create_init_batch(x.runtime, kind, init_state, &log, 100, owners);
    CHECK(r.count == 2 && r.status == AF_ACTOR_INITIALIZER_FAILED && af_actor_runtime_live(x.runtime) == 2);
    CHECK(log.calls == 3 && log.indices[0] == 100 && log.indices[2] == 102);
    CHECK(!test_load(&log.destroyed) && allocations.calls == baseline + 3);
    for (unsigned i = 2; i < N; ++i) CHECK(!e[i].owner._slot);
    /* Immutable kinds may be registered while actors remain live. */
    const af_actor_kind *rejected = NULL;
    CHECK(af_actor_kind_register(x.runtime, &options, &rejected) == AF_OK && rejected);
    log.fail_index = SIZE_MAX;
    r = af_actor_create_init_batch(x.runtime, kind, init_state, &log, 102,
        (af_actor_owner_output_span){&e[2].owner, sizeof(entity), 3});
    CHECK(r.count == 3 && r.status == AF_OK && log.indices[3] == 102 && log.indices[5] == 104);
    af_actor_owner extra = {0}; refresh_state initial = {7, NULL};
    CHECK(af_actor_create_copy(x.runtime, kind, &initial, &extra) == AF_FULL && !extra._slot);
    for (unsigned i = 0; i < N; ++i) {
        CHECK(!e[i].before && !e[i].after);
        af_actor_owner_handle(&e[i].owner, &e[i].handle);
    }
    af_actor_handle_span handles = {&e[0].handle, sizeof(entity), N};
    af_actor_result_span short_results = results; --short_results.count;
    CHECK(af_actor_wake_batch(handles, short_results).status == AF_INVALID && !af_actor_runtime_ready(x.runtime));
    r = af_actor_wake_batch(handles, results); CHECK(r.status == AF_OK && r.count == N);
    r = af_actor_request_retire_batch(x.runtime,
        (af_actor_owner_span){owners.data, owners.stride, N}, AF_ACTOR_DRAIN, results);
    CHECK(r.status == AF_OK && r.count == N);
    r = af_actor_reclaim_batch(x.runtime, owners, results); CHECK(!r.count && r.status == AF_OK);
    while (af_actor_runtime_ready(x.runtime)) step(&x);
    for (unsigned i = 0; i < N; ++i) while (!is_retired(x.runtime, &e[i].owner)) step(&x);
    r = af_actor_reclaim_batch(x.runtime, owners, results); CHECK(r.count == N && r.status == AF_OK);
    CHECK(test_load(&log.destroyed) == N && !af_actor_runtime_live(x.runtime));
    /* Allocation failure returns the entire unused reservation suffix. */
    allocations.fail = 1;
    r = af_actor_create_copy_batch(x.runtime, kind,
        (af_actor_state_span){&e[0].initial, sizeof(entity), N}, owners);
    CHECK(!r.count && r.status == AF_ALLOCATION_FAILED);
    allocations.fail = 0;
    af_actor_owner many[7] = {{0}};
    refresh_state states[7] = {{0}};
    r = af_actor_create_copy_batch(x.runtime, kind,
        (af_actor_state_span){states, sizeof states[0], 7},
        (af_actor_owner_output_span){many, sizeof many[0], 7});
    CHECK(r.count == N && r.status == AF_FULL && !many[5]._slot && !many[6]._slot);
    /* Foreign, out-of-range and half-slot addresses reject before dereference. */
    fixture foreign = setup(1, NULL);
    CHECK(af_actor_reclaim(foreign.runtime, &many[0]) == AF_INVALID);
    const af_actor_kind *foreign_kind = test_kind(foreign.runtime, sizeof initial, 8, unused, NULL);
    CHECK(af_actor_create_copy(x.runtime, foreign_kind, &initial, &extra) == AF_INVALID);
    af_actor_owner forged = many[0]; forged._slot = (char *)forged._slot + 64;
    CHECK(af_actor_request_retire(x.runtime, &forged, AF_ACTOR_DRAIN) == AF_INVALID);
    CHECK(af_actor_reclaim(x.runtime, &forged) == AF_INVALID);
    forged._slot = (void *)(uintptr_t)8;
    unsigned observed = 123;
    CHECK(af_actor_retired(x.runtime, &forged, &observed) == AF_INVALID && observed == 123);
    CHECK(af_actor_request_retire(x.runtime, &many[0], 2) == AF_INVALID);
    /* Every occurrence produces a result, including copies, stale and invalid. */
    af_actor_owner mixed[] = {many[0], many[0], {0}, forged, many[1]};
    uint32_t outcomes[5] = {0};
    r = af_actor_request_retire_batch(x.runtime,
        (af_actor_owner_span){mixed, sizeof mixed[0], 5}, AF_ACTOR_DRAIN,
        (af_actor_result_span){outcomes, sizeof outcomes[0], 5});
    CHECK(r.status == AF_OK && r.count == 3 && outcomes[2] == AF_ACTOR_STALE_OWNER && outcomes[3] == AF_INVALID);
    r = af_actor_reclaim_batch(x.runtime, (af_actor_owner_output_span){mixed, sizeof mixed[0], 5},
        (af_actor_result_span){outcomes, sizeof outcomes[0], 5});
    CHECK(r.status == AF_OK && r.count == 4 && outcomes[0] == AF_ACTOR_RECLAIMED &&
        outcomes[1] == AF_ACTOR_RECLAIM_STALE && outcomes[3] == AF_INVALID);
    for (unsigned i = 0; i < 2; ++i) CHECK(af_actor_reclaim(x.runtime, many + i) == AF_ACTOR_RECLAIM_STALE);
    for (unsigned i = 2; i < N; ++i) retire(&x, many + i, AF_ACTOR_DRAIN);
    CHECK(test_load(&log.destroyed) == 2 * N);
    r = af_actor_create_init_batch(x.runtime, kind, init_state, &log, 0,
        (af_actor_owner_output_span){NULL, 0, 0}); CHECK(r.status == AF_OK && !r.count);
    r = af_actor_wake_batch((af_actor_handle_span){NULL, 0, 0},
        (af_actor_result_span){NULL, 0, 0}); CHECK(r.status == AF_OK && !r.count);
    teardown(&foreign); teardown(&x); CHECK(!allocations.outstanding);
}

typedef struct copied_job { af_actor_runtime *runtime; af_actor_owner owner; test_atomic *start; unsigned result; } copied_job;
TEST_THREAD(copied_reclaim) {
    copied_job *j = (copied_job *)arg;
    while (!test_load(j->start)) test_yield();
    for (;;) {
        unsigned retired = af_actor_request_retire(j->runtime, &j->owner, AF_ACTOR_DRAIN);
        CHECK(retired == AF_ACTOR_RETIRE_REQUESTED || retired == AF_ACTOR_ALREADY_RETIRED ||
            retired == AF_ACTOR_STALE_OWNER || retired == AF_ACTOR_RETIRE_BUSY);
        j->result = af_actor_reclaim(j->runtime, &j->owner);
        if (j->result != AF_ACTOR_RECLAIM_BUSY) break;
        test_yield();
    }
    CHECK(j->result == AF_ACTOR_RECLAIMED || j->result == AF_ACTOR_RECLAIM_STALE);
    TEST_RETURN;
}
static void copied_owners(void) {
    fixture x = setup(1, NULL); refresh_log log = {0}; log.fail_index = SIZE_MAX;
    af_actor_kind_options options = {unused, &log, sizeof(refresh_state), 8, destroy_state};
    const af_actor_kind *kind = NULL; check(af_actor_kind_register(x.runtime, &options, &kind));
    for (unsigned round = 0; round < 50; ++round) {
        refresh_state initial = {round, NULL}; af_actor_owner owner = {0};
        check(af_actor_create_copy(x.runtime, kind, &initial, &owner));
        test_atomic start = {0}; copied_job jobs[4]; test_thread threads[4];
        for (unsigned i = 0; i < 4; ++i) {
            jobs[i].runtime = x.runtime; jobs[i].owner = owner; jobs[i].start = &start;
            threads[i] = test_start(copied_reclaim, jobs + i);
        }
        test_store(&start, 1); unsigned winners = 0;
        for (unsigned i = 0; i < 4; ++i) { test_join(threads[i]); winners += jobs[i].result == AF_ACTOR_RECLAIMED; }
        CHECK(winners == 1 && test_load(&log.destroyed) == round + 1);
        CHECK(af_actor_reclaim(x.runtime, &owner) == AF_ACTOR_RECLAIM_STALE && !owner._slot);
    }
    teardown(&x);
}

/* Native concurrent registration must retain every allocated descriptor, even
   with live actors and creators. Counts detect lost-list nodes at teardown. */
typedef struct kind_alloc_log { test_atomic allocations, frees; } kind_alloc_log;
static void *kind_alloc(void *ctx, size_t bytes, size_t alignment) {
    allocator_log local = {0};
    void *p = custom_alloc(&local, bytes, alignment);
    if (p) test_add(&((kind_alloc_log *)ctx)->allocations, 1);
    return p;
}
static void kind_free(void *ctx, void *p, size_t bytes, size_t alignment) {
    allocator_log local = {0}; local.outstanding = 1;
    custom_free(&local, p, bytes, alignment);
    test_add(&((kind_alloc_log *)ctx)->frees, 1);
}
enum { KIND_THREADS = 8, KINDS_PER_THREAD = 32 };
typedef struct kind_job {
    af_actor_runtime *runtime; test_atomic *start;
    const af_actor_kind *kinds[KINDS_PER_THREAD];
} kind_job;
TEST_THREAD(register_kinds) {
    kind_job *j = (kind_job *)arg;
    while (!test_load(j->start)) test_yield();
    for (unsigned i = 0; i < KINDS_PER_THREAD; ++i) {
        af_actor_kind_options options = {unused, NULL, sizeof(unsigned), 8, NULL};
        check(af_actor_kind_register(j->runtime, &options, j->kinds + i));
        af_actor_owner owner = {0}; unsigned initial = i;
        check(af_actor_create_copy(j->runtime, j->kinds[i], &initial, &owner));
        CHECK(af_actor_request_retire(j->runtime, &owner, AF_ACTOR_DRAIN) == AF_ACTOR_RETIRE_REQUESTED);
        CHECK(af_actor_reclaim(j->runtime, &owner) == AF_ACTOR_RECLAIMED);
    }
    TEST_RETURN;
}
static void concurrent_kinds(void) {
    kind_alloc_log log = {0}; af_actor_allocator allocator = {&log, kind_alloc, kind_free};
    fixture x = setup(KIND_THREADS + 1, &allocator);
    const af_actor_kind *first = test_kind(x.runtime, sizeof(unsigned), 8, unused, NULL);
    af_actor_owner live = {0}; unsigned initial = 0;
    check(af_actor_create_copy(x.runtime, first, &initial, &live));
    test_atomic start = {0}; kind_job jobs[KIND_THREADS] = {0}; test_thread threads[KIND_THREADS];
    for (unsigned i = 0; i < KIND_THREADS; ++i) {
        jobs[i].runtime = x.runtime; jobs[i].start = &start;
        threads[i] = test_start(register_kinds, jobs + i);
    }
    test_store(&start, 1);
    for (unsigned i = 0; i < KIND_THREADS; ++i) test_join(threads[i]);
    CHECK(af_actor_runtime_live(x.runtime) == 1);
    for (unsigned i = 0; i < KIND_THREADS * KINDS_PER_THREAD; ++i) {
        const af_actor_kind *a = jobs[i / KINDS_PER_THREAD].kinds[i % KINDS_PER_THREAD];
        CHECK(a && a != first);
        for (unsigned k = 0; k < i; ++k)
            CHECK(a != jobs[k / KINDS_PER_THREAD].kinds[k % KINDS_PER_THREAD]);
    }
    retire(&x, &live, AF_ACTOR_DRAIN);
    teardown(&x);
    CHECK(test_load(&log.allocations) == test_load(&log.frees));
}

typedef struct reservation_job {
    af_actor_runtime *runtime; const af_actor_kind *kind;
    test_atomic entered, release; af_actor_owner owners[2]; af_actor_batch_result result;
} reservation_job;
static af_status pause_init(void *user, void *memory, size_t bytes, size_t index) {
    reservation_job *j = (reservation_job *)user;
    CHECK(bytes == sizeof(unsigned)); *(unsigned *)memory = (unsigned)index;
    test_store(&j->entered, 1); while (!test_load(&j->release)) test_yield(); return AF_OK;
}
TEST_THREAD(reserve_batch) {
    reservation_job *j = (reservation_job *)arg;
    j->result = af_actor_create_init_batch(j->runtime, j->kind, pause_init, j, 42,
        (af_actor_owner_output_span){j->owners, sizeof j->owners[0], 2});
    TEST_RETURN;
}

static void allocation_prefixes(void) {
    enum { N = 4 }; allocator_log log = {0};
    af_actor_allocator allocator = {&log, custom_alloc, custom_free};
    fixture x = setup(N, &allocator);
    const af_actor_kind *kind = test_kind(x.runtime, sizeof(unsigned), 8, unused, NULL);
    unsigned states[N] = {0}, outcomes[N] = {0}; af_actor_owner owners[N] = {{0}};
    for (unsigned fail = 0; fail < N; ++fail) {
        log.fail_at = log.calls + fail + 1;
        af_actor_batch_result made = af_actor_create_copy_batch(x.runtime, kind,
            (af_actor_state_span){states, sizeof states[0], N},
            (af_actor_owner_output_span){owners, sizeof owners[0], N});
        CHECK(made.status == AF_ALLOCATION_FAILED && made.count == fail);
        for (unsigned i = fail; i < N; ++i) CHECK(!owners[i]._slot);
        log.fail_at = 0;
        af_actor_request_retire_batch(x.runtime, (af_actor_owner_span){owners, sizeof owners[0], N},
            AF_ACTOR_DRAIN, (af_actor_result_span){outcomes, sizeof outcomes[0], N});
        af_actor_batch_result freed = af_actor_reclaim_batch(x.runtime,
            (af_actor_owner_output_span){owners, sizeof owners[0], N},
            (af_actor_result_span){outcomes, sizeof outcomes[0], N});
        CHECK(freed.count == N && !af_actor_runtime_live(x.runtime));
    }
    teardown(&x); CHECK(!log.outstanding);
}

static void in_flight_reservation(void) {
    fixture x = setup(2, NULL); reservation_job job = {0};
    job.runtime = x.runtime; job.kind = test_kind(x.runtime, sizeof(unsigned), 8, unused, NULL);
    test_thread thread = test_start(reserve_batch, &job);
    while (!test_load(&job.entered)) test_yield();
    CHECK(af_actor_runtime_live(x.runtime) == 2);
    unsigned initial = 0; af_actor_owner extra = {0};
    CHECK(af_actor_create_copy(x.runtime, job.kind, &initial, &extra) == AF_FULL);
    af_actor_kind_options options = {unused, NULL, sizeof(unsigned), 8, NULL};
    const af_actor_kind *rejected = NULL;
    CHECK(af_actor_kind_register(x.runtime, &options, &rejected) == AF_OK && rejected);
    test_store(&job.release, 1); test_join(thread);
    CHECK(job.result.status == AF_OK && job.result.count == 2);
    for (unsigned i = 0; i < 2; ++i) retire(&x, job.owners + i, AF_ACTOR_DRAIN);
    teardown(&x);
}

#ifdef AF_TEST_ACTOR_AUDIT
extern void af_test_actor_after_completion(void (*fn)(void));
static af_actor_message *released_message;
static af_actor_handle *released_target;
static unsigned release_count, reply_kind;
static void released(void) {
    CHECK(af_actor_message_available(released_message));
    CHECK(af_actor_message_outcome(released_message) == reply_kind);
    memset(released_target, 0, sizeof *released_target);
    free(released_target); free(released_message); ++release_count;
}
static void reply(void *user, void *memory, size_t n, af_actor_context *ctx) {
    (void)user; (void)memory; (void)n;
    af_actor_message *m = af_actor_pop(ctx); CHECK(m);
    const af_actor_handle *h = (const af_actor_handle *)af_actor_message_payload(m);
    unsigned result = reply_kind == 0 ? af_actor_message_complete_and_wake(m, h) :
        reply_kind == 1 ? af_actor_message_fail_and_wake(m, 42, h) :
        af_actor_message_cancel_and_wake(m, h);
    CHECK(result == AF_ACTOR_QUEUED);
}
static void final_access(void) {
    fixture x = setup(2, NULL); state s = {0}; report r = {0}; r.alignment = 8;
    af_actor_owner receiver = {0}, sender = {0};
    const af_actor_kind *rk = test_kind(x.runtime, sizeof s, 8, reply, NULL);
    const af_actor_kind *sk = test_kind(x.runtime, sizeof s, 8, handler, &r);
    check(af_actor_create_copy(x.runtime, rk, &s, &receiver));
    check(af_actor_create_copy(x.runtime, sk, &s, &sender));
    af_actor_handle target; af_actor_owner_handle(&receiver, &target);
    af_test_actor_after_completion(released);
    for (reply_kind = 0; reply_kind < 3; ++reply_kind) {
        released_target = (af_actor_handle *)malloc(sizeof *released_target); CHECK(released_target);
        af_actor_owner_handle(&sender, released_target);
        released_message = (af_actor_message *)malloc(sizeof *released_message); CHECK(released_message);
        af_actor_message_init(released_message, released_target, 0);
        CHECK(af_actor_send(&target, released_message) == AF_ACTOR_QUEUED);
        while (r.calls != reply_kind + 1) step(&x);
        CHECK(release_count == reply_kind + 1);
    }
    af_test_actor_after_completion(NULL);
    retire(&x, &receiver, AF_ACTOR_DRAIN); retire(&x, &sender, AF_ACTOR_DRAIN); teardown(&x);
}
#endif
static void fatal_notice(const char *s, size_t n, void *p) {
    (void)p; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)n, s); fflush(stderr);
}
static void bad_failure(void *user, void *memory, size_t n, af_actor_context *ctx) {
    (void)user; (void)memory; (void)n; af_actor_message_fail(af_actor_pop(ctx), 0x1000000);
}
static void *misaligned(void *ctx, size_t bytes, size_t alignment) {
    (void)ctx; (void)alignment; return (char *)malloc(bytes + 1) + 1;
}
static void death(const char *name) {
    af_set_fatal_handler(fatal_notice, NULL);
    fixture x = setup(1, NULL); state s = {0}; report r = {0}; r.alignment = 8;
    af_actor_owner o = {0}; af_actor_handle h; af_actor_message storage, *m = &storage;
    const af_actor_kind *kind = test_kind(x.runtime, sizeof s, 8,
        !strcmp(name, "failure") ? bad_failure : handler, &r);
    check(af_actor_create_copy(x.runtime, kind, &s, &o));
    af_actor_owner_handle(&o, &h); af_actor_message_init(m, NULL, 0);
    if (!strcmp(name, "farm")) af_destroy(x.farm);
    if (!strcmp(name, "live")) af_actor_runtime_destroy(x.runtime);
    if (!strcmp(name, "foreign")) af_actor_runtime_flush(x.runtime, NULL, 0, 4);
    if (!strcmp(name, "cost")) af_actor_runtime_flush(x.runtime, x.token, 0, 8);
    if (!strcmp(name, "context")) {
        af_actor_wake(&h); while (!r.calls) step(&x); af_actor_republish(r.saved);
    }
    if (!strcmp(name, "complete")) af_actor_message_complete(m);
    if (!strcmp(name, "alignment")) {
        af_actor_allocator a = {NULL, misaligned, custom_free}; af_actor_runtime *bad = NULL;
        af_actor_runtime_create(x.farm, 1, &a, &bad);
    }
    af_actor_send(&h, m);
    if (!strcmp(name, "set")) af_actor_message_set(m, NULL, 0);
    if (!strcmp(name, "failure")) step(&x);
    abort(); /* Must not substitute for the fatal observer/diagnostic. */
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    if (argc > 1) { death(argv[1]); return 1; }
    invalid_and_allocation(); basic(); concurrent(); concurrent_creation();
    batch_contracts(); copied_owners(); concurrent_kinds(); in_flight_reservation(); allocation_prefixes();
#ifdef AF_TEST_ACTOR_AUDIT
    final_access();
#endif
    puts("C ABI public actor checks passed"); return 0;
}
