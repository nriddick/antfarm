#include "antfarm_private.h"
#include "check.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
struct Counters { unsigned roots, children, allocations; uint64_t total; };
struct alignas(128) Child {
    Child *self; uint64_t value; unsigned char padding[112];
    Child(uint64_t n) noexcept : self(this), value(n) {}
    Child(const Child &) = delete;
    ~Child() noexcept { CHECK(self == this); }
};
struct Root {
    Root *self; af_private_children *children;
    Root() noexcept : self(this), children(NULL) {}
    Root(const Root &) = delete;
    ~Root() noexcept { CHECK(self == this); }
};
static void *allocate(void *u, size_t n, size_t alignment) {
    Counters *c = static_cast<Counters *>(u); ++c->allocations;
    if (alignment < alignof(void *)) alignment = alignof(void *);
    void *raw = std::malloc(n + alignment - 1 + sizeof(void *));
    if (!raw) return NULL;
    uintptr_t p = (reinterpret_cast<uintptr_t>(raw) + sizeof(void *) + alignment - 1) & ~(alignment - 1);
    reinterpret_cast<void **>(p)[-1] = raw; return reinterpret_cast<void *>(p);
}
static void deallocate(void *, void *p, size_t, size_t) { std::free(static_cast<void **>(p)[-1]); }
static af_status root_init(void *, void *p, size_t n, size_t) { CHECK(n == sizeof(Root)); new(p) Root(); return AF_OK; }
static af_status child_init(void *, void *p, size_t n, size_t i) {
    CHECK(n == sizeof(Child) && reinterpret_cast<uintptr_t>(p) % alignof(Child) == 0);
    new(p) Child(i + 1); return AF_OK;
}
static void root_destroy(void *u, void *p, size_t) { static_cast<Root *>(p)->~Root(); ++static_cast<Counters *>(u)->roots; }
static void child_destroy(void *u, void *p, size_t) { static_cast<Child *>(p)->~Child(); ++static_cast<Counters *>(u)->children; }
static uint32_t child_step(void *, void *p, size_t, void *r, size_t n, af_private_context *) {
    Child *c = static_cast<Child *>(p); CHECK(c->self == c && n == 8);
    CHECK(reinterpret_cast<uintptr_t>(r) % 64 == 0);
    new(r) uint64_t(c->value * c->value); return AF_PRIVATE_DONE;
}
static uint32_t root_step(void *u, void *p, size_t, af_private_context *ctx) {
    Counters *c = static_cast<Counters *>(u); Root *r = static_cast<Root *>(p); CHECK(r->self == r);
    if (!r->children) {
        af_private_child_options o; af_private_child_options_default(&o);
        o.capacity = 4; o.state.bytes = sizeof(Child); o.state.alignment = alignof(Child);
        o.state.destroy = child_destroy; o.state.destroy_user = c;
        o.result_bytes = 8; o.result_alignment = 64;
        o.storage = AF_PRIVATE_COHORT_SLAB; o.slab_byte_limit = 4 * sizeof(Child);
        CHECK(af_private_children_create(ctx, &o, &r->children) == AF_OK);
        unsigned before = c->allocations;
        for (size_t i = 0; i < 4; ++i) CHECK(af_private_child_spawn_init(r->children, ctx, child_init, NULL, i) == AF_OK);
        CHECK(c->allocations == before);
        CHECK(af_private_children_start(r->children, ctx, child_step, NULL, 1, 0, 2) == AF_OK);
        return AF_PRIVATE_WAIT_CHILDREN;
    }
    af_private_results results = af_private_children_results(r->children, ctx);
    CHECK(results.count == 4 && results.stride == 64 && c->children == 4);
    for (size_t i = 0; i < results.count; ++i) {
        uint64_t value; std::memcpy(&value, static_cast<const unsigned char *>(results.data) + i * results.stride, 8);
        c->total += value;
    }
    return AF_PRIVATE_FINISH;
}
int main() {
    af_farm *farm = NULL; af_token *token = NULL; af_view *view = NULL;
    CHECK(af_create(NULL, &farm) == AF_OK); CHECK(af_register_producer(farm, AF_SMALL, &token) == AF_OK);
    CHECK(af_view_create(&view) == AF_OK); while (af_subscribe(view, farm) == AF_RETRY) {}
    Counters c = {}; af_actor_allocator a = {&c, allocate, deallocate};
    af_private_root_options o; af_private_root_options_default(&o);
    o.capacity = 1; o.state.bytes = sizeof(Root); o.state.alignment = alignof(Root);
    o.state.destroy = root_destroy; o.state.destroy_user = &c; o.step = root_step; o.user = &c;
    af_private_roots *park = NULL; CHECK(af_private_roots_create(farm, &o, &a, &park) == AF_OK);
    CHECK(af_private_root_spawn_init(park, root_init, NULL, 0, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    while (af_private_roots_live(park)) { af_private_roots_pump(park, token, 0, AF_PRIVATE_AUTO_COST); while (af_consume_next(view)) {} }
    CHECK(c.total == 30 && c.roots == 1 && c.children == 4);
    af_private_roots_destroy(park); af_unsubscribe(view); af_view_destroy(view);
    af_unregister_producer(farm, token); af_destroy(farm);
    std::puts("C++ private placement construction and aligned slab passed");
}
