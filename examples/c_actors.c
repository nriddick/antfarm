/* A public actor service using only the runtime-free C library. */
#include "antfarm_actors.h"
#include <stdio.h>
#include <stdlib.h>

static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%s\n", af_last_error_message()); exit(1); }
}
typedef struct counter { unsigned sum; } counter;
static void add(void *user, void *state, size_t bytes, af_actor_context *ctx) {
    (void)bytes; counter *c = (counter *)state;
    af_actor_message *m;
    while ((m = af_actor_pop(ctx)) != NULL) {
        c->sum += *(const unsigned *)af_actor_message_payload(m);
        af_actor_message_complete(m); /* final access to this message/payload */
    }
    *(unsigned *)user = c->sum;
}
int main(void) {
    af_farm *farm = NULL; check(af_create(NULL, &farm));
    af_actor_runtime *runtime = NULL; check(af_actor_runtime_create(farm, 1, NULL, &runtime));
    af_token *token = NULL; check(af_register_producer(farm, AF_SMALL, &token));
    af_view *view = NULL; check(af_view_create(&view)); check(af_subscribe(view, farm));
    counter initial = {0}; unsigned sum = 0, values[] = {2, 3, 5};
    af_actor_owner owner = {0};
    af_actor_kind_options kind_options = {add, &sum, sizeof(counter), _Alignof(counter), NULL};
    const af_actor_kind *kind = NULL; check(af_actor_kind_register(runtime, &kind_options, &kind));
    check(af_actor_create_copy(runtime, kind, &initial, &owner));
    af_actor_handle handle; af_actor_owner_handle(&owner, &handle);
    af_actor_message messages[3];
    for (unsigned i = 0; i < 3; ++i) {
        af_actor_message_init(messages + i, values + i, i);
        if (af_actor_send(&handle, messages + i) > AF_ACTOR_COALESCED) return 1;
    }
    af_actor_request_retire(runtime, &owner, AF_ACTOR_DRAIN);
    unsigned retired = 0;
    for (;;) {
        check(af_actor_retired(runtime, &owner, &retired)); if (retired) break;
        af_actor_runtime_flush(runtime, token, 32, AF_ACTOR_DEFAULT_COST);
        af_consume_next(view);
    }
    if (af_actor_reclaim(runtime, &owner) != AF_ACTOR_RECLAIMED) return 1;
    /* No background consumers in this example. With threads, join them here. */
    af_unsubscribe(view); af_view_destroy(view);
    af_actor_runtime_destroy(runtime);
    af_unregister_producer(farm, token); af_destroy(farm);
    printf("actor sum = %u\n", sum);
    return sum == 10 ? 0 : 1;
}
