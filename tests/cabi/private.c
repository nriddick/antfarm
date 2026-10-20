#include "antfarm_private.h"
#include "threads.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct memory_header { void *raw; size_t bytes, alignment; } memory_header;
typedef struct memory_counts { test_atomic calls, frees, live, fail_at; } memory_counts;
static void *count_allocate(void *context, size_t bytes, size_t alignment) {
    memory_counts *m = (memory_counts *)context;
    unsigned n = test_add(&m->calls, 1) + 1;
    if (n == test_load(&m->fail_at)) return NULL;
    if (alignment < sizeof(void *)) alignment = sizeof(void *);
    void *raw = malloc(bytes + alignment - 1 + sizeof(memory_header));
    if (!raw) return NULL;
    uintptr_t address = ((uintptr_t)raw + sizeof(memory_header) + alignment - 1) & ~(uintptr_t)(alignment - 1);
    memory_header *h = (memory_header *)address - 1;
    h->raw = raw; h->bytes = bytes; h->alignment = alignment;
    test_add(&m->live, 1);
    return (void *)address;
}
static void count_free(void *context, void *memory, size_t bytes, size_t alignment) {
    memory_counts *m = (memory_counts *)context;
    memory_header *h = (memory_header *)memory - 1;
    if (alignment < sizeof(void *)) alignment = sizeof(void *);
    CHECK(h->bytes == bytes && h->alignment == alignment);
    test_add(&m->frees, 1); test_add(&m->live, (unsigned)-1);
    free(h->raw);
}
typedef struct fixture {
    af_farm *farm; af_token *token; af_view *view;
    memory_counts memory;
    test_atomic root_dtors, child_dtors, leaf_dtors, calls, stop;
    unsigned failure_position, mode, storage;
    uint64_t output, cancel_value;
    af_private_root_handle handle;
    af_private_context *saved_context;
    af_private_children *saved_children;
} fixture;
static void setup(fixture *f) {
    memset(f, 0, sizeof(*f));
    af_options o; af_options_default(&o); o.expected_consumers = 4;
    CHECK(af_create(&o, &f->farm) == AF_OK);
    CHECK(af_register_producer(f->farm, AF_SMALL, &f->token) == AF_OK);
    CHECK(af_view_create(&f->view) == AF_OK);
    while (af_subscribe(f->view, f->farm) == AF_RETRY) {}
}
static void teardown(fixture *f) {
    CHECK(test_load(&f->memory.live) == 0);
    af_unsubscribe(f->view); af_view_destroy(f->view);
    af_unregister_producer(f->farm, f->token); af_destroy(f->farm);
}
static af_actor_allocator allocator(fixture *f) {
    af_actor_allocator a = {&f->memory, count_allocate, count_free}; return a;
}
static void root_destroy(void *u, void *s, size_t bytes) {
    fixture *f = (fixture *)u; CHECK(s && bytes); test_add(&f->root_dtors, 1);
}
static void child_destroy(void *u, void *s, size_t bytes) {
    fixture *f = (fixture *)u; CHECK(s && bytes); test_add(&f->child_dtors, 1);
}
static void leaf_destroy(void *u, void *s, size_t bytes) {
    fixture *f = (fixture *)u; CHECK(s && bytes); test_add(&f->leaf_dtors, 1);
}
static af_private_roots *roots(fixture *f, size_t bytes, af_private_root_step step, unsigned tracking, unsigned reclaim) {
    af_private_root_options o; af_private_root_options_default(&o);
    o.capacity = 2; o.batch_limit = 1; o.state.bytes = bytes; o.state.alignment = 32;
    o.state.destroy = root_destroy; o.state.destroy_user = f;
    o.step = step; o.user = f; o.tracking = tracking; o.reclamation = reclaim;
    af_actor_allocator a = allocator(f); af_private_roots *p = NULL;
    CHECK(af_private_roots_create(f->farm, &o, &a, &p) == AF_OK); return p;
}
static void drive(fixture *f, af_private_roots *p) {
    for (unsigned i = 0; i < 1000000; ++i) {
        af_private_roots_pump(p, f->token, 0, AF_PRIVATE_AUTO_COST);
        while (af_consume_next(f->view)) {}
        af_private_roots_collect(p);
        if (!af_private_roots_live(p)) return;
        test_yield();
    }
    CHECK(!"private park failed to drain");
}
static af_private_root_outcome take(fixture *f, af_private_roots *p) {
    af_private_root_outcome result;
    for (unsigned i = 0; i < 1000000; ++i) {
        af_private_roots_pump(p, f->token, 0, AF_PRIVATE_AUTO_COST);
        while (af_consume_next(f->view)) {}
        af_private_roots_collect(p);
        if (af_private_root_take_outcome(p, &result)) return result;
        test_yield();
    }
    CHECK(!"tracked private root failed to drain"); memset(&result, 0, sizeof(result)); return result;
}
typedef struct child_state { uint64_t value; af_private_children *children; } child_state;
typedef struct root_state { unsigned stage; af_private_children *children; } root_state;
static uint32_t leaf(void *u, void *s, size_t bytes, void *r, size_t rb, af_private_context *c) {
    (void)u; CHECK(bytes >= sizeof(child_state)); CHECK(rb == 9 && r);
    child_state *v = (child_state *)s;
    uint64_t square = v->value * v->value;
    memcpy(r, &square, 8); ((unsigned char *)r)[8] = 0xA5;
    CHECK(((uintptr_t)r & 15) == 0);
    /* Terminal leaf contexts cannot create a new cohort. */
    af_private_child_options o; af_private_child_options_default(&o); o.state.bytes = 8;
    af_private_children *unused = (af_private_children *)(uintptr_t)1;
    if (v->value >= 10) CHECK(af_private_children_create(c, &o, &unused) == AF_PRIVATE_RETIRING && !unused);
    return AF_PRIVATE_DONE;
}
static uint64_t sum_results(af_private_children *p, af_private_context *c, size_t expected) {
    CHECK(af_private_children_finished(p, c));
    af_private_results results = af_private_children_results(p, c);
    CHECK(results.count == expected && results.stride == 16);
    uint64_t sum = 0;
    for (size_t i = 0; i < results.count; ++i) {
        const unsigned char *slot = (const unsigned char *)results.data + i * results.stride;
        uint64_t value; memcpy(&value, slot, 8); CHECK(slot[8] == 0xA5); sum += value;
    }
    return sum;
}
static af_private_children *children(fixture *f, af_private_context *c, size_t count, unsigned storage, unsigned subsets, unsigned is_leaf) {
    af_private_child_options o; af_private_child_options_default(&o);
    o.capacity = count; o.batch_limit = 2;
    o.state.bytes = sizeof(child_state) + 3; o.state.alignment = 32;
    o.state.destroy = is_leaf ? leaf_destroy : child_destroy; o.state.destroy_user = f;
    o.result_bytes = 9; o.result_alignment = 16;
    o.storage = storage; o.slab_byte_limit = count * 32; o.subsets = subsets;
    af_private_children *p = NULL;
    CHECK(af_private_children_create(c, &o, &p) == AF_OK); return p;
}
static af_status init_child(void *u, void *s, size_t bytes, size_t index) {
    fixture *f = (fixture *)u;
    if (f->storage == AF_PRIVATE_COHORT_SLAB) {
        const unsigned char *p = (const unsigned char *)s;
        for (size_t i = 0; i < bytes; ++i) CHECK(!p[i]);
    }
    CHECK(((uintptr_t)s & 31) == 0);
    memset(s, 0, bytes); ((child_state *)s)->value = index;
    return AF_OK;
}
static af_status fail_child(void *u, void *s, size_t bytes, size_t index) {
    (void)u; (void)index; memset(s, 0xFF, bytes); return AF_INVALID;
}
static uint32_t continuation(void *u, void *s, size_t bytes, void *r, size_t rb, af_private_context *c) {
    fixture *f = (fixture *)u; child_state *v = (child_state *)s; CHECK(bytes >= sizeof(*v));
    if (!v->children) {
        v->children = children(f, c, 2, AF_PRIVATE_PER_OBJECT, 0, 1);
        for (size_t i = 0; i < 2; ++i) {
            struct { child_state state; unsigned char tail[3]; } initial = {0};
            initial.state.value = v->value * 10 + i;
            CHECK(af_private_child_spawn_copy(v->children, c, &initial) == AF_OK);
        }
        CHECK(af_private_children_start(v->children, c, leaf, f, 1, 0, AF_PRIVATE_CHILD_COST) == AF_OK);
        return AF_PRIVATE_CHILD_WAIT;
    }
    uint64_t sum = sum_results(v->children, c, 2);
    CHECK(rb == 9); memcpy(r, &sum, 8); ((unsigned char *)r)[8] = 0xA5;
    return AF_PRIVATE_DONE;
}
static uint32_t tree(void *u, void *s, size_t bytes, af_private_context *c) {
    fixture *f = (fixture *)u; root_state *v = (root_state *)s; CHECK(bytes == sizeof(*v));
    test_add(&f->calls, 1);
    if (v->stage == 0) {
        v->children = children(f, c, 4, f->storage, 1, 0);
        unsigned allocations = test_load(&f->memory.calls);
        CHECK(af_private_child_spawn_init(v->children, c, fail_child, f, 0) == AF_ACTOR_INITIALIZER_FAILED);
        for (size_t i = 1; i <= 4; ++i) CHECK(af_private_child_spawn_init(v->children, c, init_child, f, i) == AF_OK);
        CHECK(af_private_child_spawn_init(v->children, c, init_child, f, 5) == AF_FULL);
        if (f->storage == AF_PRIVATE_COHORT_SLAB) CHECK(allocations == test_load(&f->memory.calls));
        uint32_t selected[2] = {3, 1};
        CHECK(af_private_children_start_subset(v->children, c, leaf, f, selected, 2, 0, 1) == AF_OK);
        CHECK(af_private_children_start(v->children, c, continuation, NULL, 1, 1, 2) == AF_BUSY);
        ++v->stage; return AF_PRIVATE_WAIT_CHILDREN;
    }
    if (v->stage == 1) {
        CHECK(sum_results(v->children, c, 2) == 20);
        size_t count; const uint32_t *selection = af_private_children_subset(v->children, c, &count);
        CHECK(count == 2 && selection[0] == 3 && selection[1] == 1);
        CHECK(af_private_children_start_subset(v->children, c, continuation, NULL, NULL, 0, 1, 2) == AF_PRIVATE_EMPTY);
        CHECK(sum_results(v->children, c, 2) == 20);
        CHECK(af_private_children_start(v->children, c, continuation, f, 1, 1, 2) == AF_OK);
        ++v->stage; return AF_PRIVATE_WAIT_CHILDREN;
    }
    CHECK(v->stage == 2);
    f->output = sum_results(v->children, c, 4); CHECK(f->output == 6204);
    CHECK(test_load(&f->child_dtors) == 4 && test_load(&f->leaf_dtors) == 8);
    CHECK(af_private_child_spawn_init(v->children, c, fail_child, f, 0) == AF_ACTOR_INITIALIZER_FAILED);
    CHECK(af_private_child_spawn_init(v->children, c, init_child, f, 10) == AF_OK);
    CHECK(af_private_children_start(v->children, c, leaf, f, 1, 0, 2) == AF_OK);
    return AF_PRIVATE_FINISH_AFTER_CHILDREN;
}
typedef struct worker { af_view *view; test_atomic *stop; } worker;
TEST_THREAD(consume) {
    worker *w = (worker *)arg;
    while (!test_load(w->stop)) if (!af_consume_next(w->view)) test_yield();
    af_unsubscribe(w->view); af_view_destroy(w->view); TEST_RETURN;
}
static void tree_test(unsigned storage, unsigned reclaim, unsigned workers) {
    fixture f; setup(&f); f.storage = storage;
    worker w[2]; test_thread threads[2];
    for (unsigned i = 0; i < workers; ++i) {
        CHECK(af_view_create(&w[i].view) == AF_OK);
        while (af_subscribe(w[i].view, f.farm) == AF_RETRY) {}
        w[i].stop = &f.stop; threads[i] = test_start(consume, &w[i]);
    }
    af_private_roots *p = roots(&f, sizeof(root_state), tree, 1, reclaim);
    root_state state = {0};
    CHECK(af_private_root_spawn_copy(p, &state, AF_PRIVATE_NO_DEADLINE, &f.handle) == AF_OK);
    af_private_root_outcome out = take(&f, p);
    CHECK(out.status == AF_PRIVATE_COMPLETED && f.output == 6204);
    CHECK(test_load(&f.calls) == 3 && test_load(&f.root_dtors) == 1 && test_load(&f.child_dtors) == 5);
    CHECK(!af_private_roots_live(p));
    CHECK(af_private_root_request_cancel(&f.handle) == AF_PRIVATE_CANCEL_STALE);
    af_private_roots_destroy(p);
    test_store(&f.stop, 1); for (unsigned i = 0; i < workers; ++i) test_join(threads[i]);
    teardown(&f);
}
static uint32_t cancel_leaf(void *u, void *s, size_t bytes, void *r, size_t rb, af_private_context *c) {
    fixture *f = (fixture *)u; child_state *v = (child_state *)s;
    CHECK(bytes >= sizeof(*v) && rb == 9); uint64_t n = v->value;
    memcpy(r, &n, 8); ((unsigned char *)r)[8] = 0xA5;
    test_add(&f->calls, 1);
    if (!n) { af_private_fail(c, 73); af_private_cancel_peers(c); }
    return AF_PRIVATE_DONE;
}
static uint32_t cancel_tree(void *u, void *s, size_t bytes, af_private_context *c) {
    fixture *f = (fixture *)u; root_state *v = (root_state *)s; (void)bytes;
    if (!v->children) {
        v->children = children(f, c, 7, f->storage, 0, 0);
        for (size_t i = 0; i < 7; ++i) CHECK(af_private_child_spawn_init(v->children, c, init_child, f, i) == AF_OK);
        CHECK(af_private_children_start(v->children, c, cancel_leaf, f, 1, 0, 2) == AF_OK);
        return AF_PRIVATE_WAIT_CHILDREN;
    }
    af_private_wave_outcome out; af_private_children_outcome(v->children, c, &out);
    CHECK(out.participants == 7 && out.dispatched == 2 && out.cancelled);
    CHECK(out.failures == 1 && out.first_failure_code == 73 && out.first_failure_position == 0);
    CHECK(af_private_children_results(v->children, c).count == 2);
    CHECK(test_load(&f->child_dtors) == 7);
    af_private_fail(c, 999); return AF_PRIVATE_FINISH;
}
static void truncated_test(unsigned storage) {
    fixture f; setup(&f); f.storage = storage;
    af_private_roots *p = roots(&f, sizeof(root_state), cancel_tree, 1, 0);
    root_state initial = {0}; CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    af_private_root_outcome out = take(&f, p); CHECK(out.status == AF_PRIVATE_FAILED && out.code == 999);
    CHECK(test_load(&f.calls) == 2); af_private_roots_destroy(p); teardown(&f);
}
static uint32_t simple(void *u, void *s, size_t bytes, af_private_context *c) {
    fixture *f = (fixture *)u; CHECK(bytes == sizeof(uint64_t));
    if (f->mode == 1) CHECK(af_private_cancelled(c));
    if (f->mode == 2) {
        test_store(&f->calls, 1);
        while (!af_private_cancelled(c)) test_yield();
        CHECK(f->cancel_value == 1234567);
    }
    *(uint64_t *)s += 1;
    f->output = *(uint64_t *)s;
    return f->mode == 3 && f->output < 3 ? AF_PRIVATE_AGAIN : AF_PRIVATE_FINISH;
}
TEST_THREAD(cancel_root) {
    fixture *f = (fixture *)arg; while (!test_load(&f->calls)) test_yield();
    f->cancel_value = 1234567;
    CHECK(af_private_root_request_cancel(&f->handle) == AF_PRIVATE_CANCEL_REQUESTED); TEST_RETURN;
}
static uint32_t signal_child(void *u, void *s, size_t bytes, void *result, size_t rb, af_private_context *ctx) {
    (void)u; (void)ctx; CHECK(bytes == 8 && !result && !rb); ++*(uint64_t *)s;
    return AF_PRIVATE_DONE;
}
static uint32_t signal_root(void *u, void *s, size_t bytes, af_private_context *ctx) {
    (void)u; (void)bytes; root_state *r = (root_state *)s;
    if (!r->children) {
        af_private_child_options o; af_private_child_options_default(&o); o.state.bytes = 8;
        o.storage = AF_PRIVATE_COHORT_SLAB; o.slab_byte_limit = o.capacity * 8;
        CHECK(af_private_children_create(ctx, &o, &r->children) == AF_OK);
        uint64_t initial = 2; CHECK(af_private_child_spawn_copy(r->children, ctx, &initial) == AF_OK);
        CHECK(af_private_children_start(r->children, ctx, signal_child, NULL, 1, 0, 2) == AF_OK);
        return AF_PRIVATE_WAIT_CHILDREN;
    }
    af_private_results results = af_private_children_results(r->children, ctx);
    CHECK(!results.data && !results.stride && !results.count);
    CHECK(!af_private_children_cancel(r->children, ctx)); af_private_cancel_children(ctx);
    return AF_PRIVATE_FINISH;
}
static void root_test(void) {
    fixture f; setup(&f); af_private_roots *p = roots(&f, 8, simple, 1, 1); uint64_t initial = 0;
    CHECK(af_private_clock_frequency() > 0); int64_t now = af_private_clock_now();
    f.mode = 1;
    CHECK(af_private_root_spawn_copy(p, &initial, now - 1, &f.handle) == AF_OK);
    size_t index = af_private_root_index(&f.handle);
    af_private_root_outcome out = take(&f, p);
    CHECK(out.status == AF_PRIVATE_CANCELLED && out.cause == AF_PRIVATE_CAUSE_DEADLINE);
    af_private_root_handle old = f.handle; f.mode = 2;
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, &f.handle) == AF_OK);
    CHECK(af_private_root_index(&f.handle) == index);
    CHECK(af_private_root_request_cancel(&old) == AF_PRIVATE_CANCEL_STALE);
    test_thread t = test_start(cancel_root, &f); out = take(&f, p); test_join(t);
    CHECK(out.status == AF_PRIVATE_CANCELLED && out.cause == AF_PRIVATE_CAUSE_REQUESTED);
    f.mode = 0;
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, &f.handle) == AF_OK);
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_FULL);
    CHECK(af_private_roots_cancel_all(p) == 2); (void)take(&f, p); (void)take(&f, p);
    af_private_roots_destroy(p);
    p = roots(&f, 8, simple, 0, 0); f.mode = 3;
    CHECK(af_private_root_spawn_copy(p, &initial, now, NULL) == AF_INVALID);
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    drive(&f, p); CHECK(f.output == 3); af_private_roots_destroy(p);
    p = roots(&f, sizeof(root_state), signal_root, 0, 0); root_state state = {0};
    CHECK(af_private_root_spawn_copy(p, &state, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    drive(&f, p); af_private_roots_destroy(p); teardown(&f);
}
static af_status fail_root(void *u, void *s, size_t bytes, size_t index) {
    (void)u; (void)index; memset(s, 3, bytes); return AF_INVALID;
}
static uint32_t allocation_child(void *u, void *s, size_t bytes, af_private_context *c) {
    fixture *f = (fixture *)u; (void)s; (void)bytes;
    af_private_child_options o; af_private_child_options_default(&o);
    o.state.bytes = 8; o.capacity = 4; o.result_bytes = 8; o.result_alignment = 8;
    o.storage = AF_PRIVATE_COHORT_SLAB; o.subsets = 1;
    /* An unset cap is a configuration error; only a set cap can be exceeded. */
    unsigned calls = test_load(&f->memory.calls); af_private_children *p = NULL;
    CHECK(af_private_children_create(c, &o, &p) == AF_INVALID && !p);
    o.slab_byte_limit = 31; CHECK(af_private_children_create(c, &o, &p) == AF_ALLOCATION_FAILED && !p);
    CHECK(test_load(&f->memory.calls) == calls); o.slab_byte_limit = 32;
    unsigned before = test_load(&f->memory.live);
    test_store(&f->memory.fail_at, test_load(&f->memory.calls) + f->failure_position);
    af_status status = af_private_children_create(c, &o, &p);
    if (f->failure_position <= 5) { CHECK(status == AF_ALLOCATION_FAILED && !p); CHECK(test_load(&f->memory.live) == before); }
    else CHECK(status == AF_OK && p);
    test_store(&f->memory.fail_at, 0);
    return AF_PRIVATE_FINISH;
}
static void allocation_test(void) {
    fixture f; setup(&f); af_actor_allocator a = allocator(&f);
    af_private_root_options o; af_private_root_options_default(&o);
    o.state.bytes = 8; o.step = simple; o.user = &f; o.tracking = 1;
    for (unsigned i = 1; i <= 7; ++i) {
        test_store(&f.memory.fail_at, test_load(&f.memory.calls) + i);
        af_private_roots *p = NULL; af_status status = af_private_roots_create(f.farm, &o, &a, &p);
        if (i <= 6) CHECK(status == AF_ALLOCATION_FAILED && !p);
        else { CHECK(status == AF_OK); af_private_roots_destroy(p); }
        CHECK(test_load(&f.memory.live) == 0);
    }
    test_store(&f.memory.fail_at, 0);
    af_private_roots *p = roots(&f, 8, simple, 1, 0);
    unsigned before = test_load(&f.memory.live);
    CHECK(af_private_root_spawn_init(p, fail_root, NULL, 12, AF_PRIVATE_NO_DEADLINE, &f.handle) == AF_ACTOR_INITIALIZER_FAILED);
    CHECK(!f.handle._record && !af_private_roots_live(p) && !test_load(&f.root_dtors));
    CHECK(test_load(&f.memory.live) == before);
    test_store(&f.memory.fail_at, test_load(&f.memory.calls) + 1);
    uint64_t initial = 0;
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, &f.handle) == AF_ALLOCATION_FAILED);
    test_store(&f.memory.fail_at, 0); af_private_roots_destroy(p);
    for (unsigned i = 1; i <= 6; ++i) {
        f.failure_position = i; p = roots(&f, 8, allocation_child, 0, 0);
        CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
        drive(&f, p); af_private_roots_destroy(p);
    }
    o.struct_size--; p = (af_private_roots *)(uintptr_t)1;
    CHECK(af_private_roots_create(f.farm, &o, &a, &p) == AF_INVALID && !p);
    o.struct_size++; o.capacity = SIZE_MAX;
    CHECK(af_private_roots_create(f.farm, &o, &a, &p) == AF_INVALID && !p);
    teardown(&f);
}
static void fatal_observer(const char *message, size_t bytes, void *context) {
    (void)context; fprintf(stderr, "C_FATAL_OBSERVER: %.*s\n", (int)bytes, message); fflush(stderr);
}
static const char *death_name;
static uint32_t bad_leaf(void *u, void *s, size_t bytes, void *r, size_t rb, af_private_context *c) {
    (void)u; (void)s; (void)bytes; (void)r; (void)rb; (void)c; return AF_PRIVATE_CHILD_WAIT;
}
static af_status bad_init(void *u, void *s, size_t bytes, size_t index) {
    fixture *f = (fixture *)u; (void)s; (void)bytes; (void)index;
    af_private_fail(f->saved_context, 1); return AF_OK;
}
static uint32_t bad_root(void *u, void *s, size_t bytes, af_private_context *c) {
    fixture *f = (fixture *)u; (void)s; (void)bytes; f->saved_context = c;
    if (!strcmp(death_name, "disposition")) return 99;
    if (!strcmp(death_name, "fail")) { af_private_fail(c, 1); return AF_PRIVATE_FINISH; }
    if (!strcmp(death_name, "context")) return AF_PRIVATE_FINISH;
    if (!strcmp(death_name, "authority") && f->saved_children) {
        (void)af_private_children_finished(f->saved_children, c); return AF_PRIVATE_AGAIN;
    }
    af_private_children *p = children(f, c, 2, AF_PRIVATE_COHORT_SLAB, 1, 0);
    f->saved_children = p;
    if (!strcmp(death_name, "authority")) return AF_PRIVATE_AGAIN;
    if (!strcmp(death_name, "init-context")) { (void)af_private_child_spawn_init(p, c, bad_init, f, 0); return AF_PRIVATE_FINISH; }
    CHECK(af_private_child_spawn_init(p, c, init_child, f, 1) == AF_OK);
    if (!strcmp(death_name, "subset")) {
        CHECK(af_private_child_spawn_init(p, c, init_child, f, 2) == AF_OK);
        uint32_t selection[2] = {0, 0};
        (void)af_private_children_start_subset(p, c, leaf, f, selection, 2, 0, 2);
    }
    CHECK(af_private_children_start(p, c, bad_leaf, f, 1, 0, 2) == AF_OK);
    if (!strcmp(death_name, "early")) (void)af_private_children_results(p, c);
    return AF_PRIVATE_WAIT_CHILDREN;
}
static void death_test(const char *name) {
    death_name = name; af_set_fatal_handler(fatal_observer, NULL);
    fixture f; setup(&f); f.storage = AF_PRIVATE_COHORT_SLAB;
    af_private_roots *p = roots(&f, 8, bad_root, 0, 0); uint64_t initial = 0;
    if (!strcmp(name, "farm")) af_destroy(f.farm);
    if (!strcmp(name, "cost")) (void)af_private_roots_pump(p, f.token, 0, 8);
    if (!strcmp(name, "outcome")) { af_private_root_outcome out; (void)af_private_root_take_outcome(p, &out); }
    CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    if (!strcmp(name, "live")) af_private_roots_destroy(p);
    if (!strcmp(name, "authority")) CHECK(af_private_root_spawn_copy(p, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    af_private_roots_pump(p, f.token, 0, AF_PRIVATE_AUTO_COST);
    while (af_consume_next(f.view)) {}
    if (!strcmp(name, "context")) (void)af_private_cancelled(f.saved_context);
    drive(&f, p); CHECK(!"expected private death");
}
int main(int argc, char **argv) {
#ifdef _WIN32
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif
    if (argc > 1) { death_test(argv[1]); return 1; }
    for (unsigned s = 0; s < 2; ++s) for (unsigned r = 0; r < 2; ++r) {
        tree_test(s, r, 0); tree_test(s, r, 2);
    }
    truncated_test(0); truncated_test(1); root_test(); allocation_test();
    puts("C private actors passed: nested results, continuations, slabs, cancellation, native threads, rollback");
    return 0;
}
