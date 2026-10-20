/* A root joins a slab-backed cohort and reads results after member retirement.
   Link antfarm_core with a native C linker; no druntime or thread setup. */
#include "antfarm_private.h"
#include <stdio.h>
#include <stdlib.h>
static void require(int ok) { if (!ok) abort(); }
typedef struct conductor { af_private_children *children; uint64_t *total; } conductor;
static uint32_t square(void *user, void *state, size_t bytes, void *result, size_t result_bytes,
                       af_private_context *context) {
    (void)user; (void)context; require(bytes == 8 && result_bytes == 8);
    uint64_t n = *(uint64_t *)state; *(uint64_t *)result = n * n;
    return AF_PRIVATE_DONE;
}
static uint32_t conduct(void *user, void *state, size_t bytes, af_private_context *context) {
    (void)user; require(bytes == sizeof(conductor)); conductor *c = (conductor *)state;
    if (!c->children) {
        af_private_child_options options; af_private_child_options_default(&options);
        options.capacity = 4; options.state.bytes = 8; options.state.alignment = 8;
        options.result_bytes = 8; options.result_alignment = 8;
        options.storage = AF_PRIVATE_COHORT_SLAB; options.slab_byte_limit = 32;
        require(af_private_children_create(context, &options, &c->children) == AF_OK);
        for (uint64_t n = 1; n <= 4; ++n)
            require(af_private_child_spawn_copy(c->children, context, &n) == AF_OK);
        require(af_private_children_start(c->children, context, square, NULL, 1, 0,
                                          AF_PRIVATE_CHILD_COST) == AF_OK);
        return AF_PRIVATE_WAIT_CHILDREN;
    }
    af_private_results results = af_private_children_results(c->children, context);
    for (size_t i = 0; i < results.count; ++i)
        *c->total += *(const uint64_t *)((const unsigned char *)results.data + i * results.stride);
    return AF_PRIVATE_FINISH;
}
int main(void) {
    af_farm *farm = NULL; af_token *token = NULL; af_view *view = NULL;
    require(af_create(NULL, &farm) == AF_OK);
    require(af_register_producer(farm, AF_SMALL, &token) == AF_OK);
    require(af_view_create(&view) == AF_OK);
    af_status status; do { status = af_subscribe(view, farm); } while (status == AF_RETRY);
    require(status == AF_OK);
    af_private_root_options options; af_private_root_options_default(&options);
    options.capacity = 1; options.state.bytes = sizeof(conductor); options.step = conduct;
    af_private_roots *park = NULL;
    require(af_private_roots_create(farm, &options, NULL, &park) == AF_OK);
    uint64_t total = 0; conductor initial = {NULL, &total};
    require(af_private_root_spawn_copy(park, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK);
    while (af_private_roots_live(park)) {
        af_private_roots_pump(park, token, 0, AF_PRIVATE_AUTO_COST);
        while (af_consume_next(view)) {}
    }
    require(total == 30); printf("private result = %llu\n", (unsigned long long)total);
    af_private_roots_destroy(park);
    af_unsubscribe(view); af_view_destroy(view); af_unregister_producer(farm, token); af_destroy(farm);
    return 0;
}
