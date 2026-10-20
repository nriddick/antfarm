/* Two phases over copied actor state, using only the runtime-free C archive. */
#include "antfarm_actors.h"
#include <stdio.h>
#include <stdlib.h>

static void check(af_status s) {
    if (s != AF_OK) { fprintf(stderr, "%s\n", af_last_error_message()); exit(1); }
}
typedef struct body { unsigned position; unsigned *projection; } body;
static void idle(void *user, void *state, size_t bytes, af_actor_context *context) {
    (void)user; (void)state; (void)bytes; (void)context;
}
static void move_body(void *user, const af_actor_kind *kind, void *state, size_t bytes) {
    (void)kind; (void)bytes; body *b = (body *)state;
    b->position += *(const unsigned *)user; *b->projection = b->position;
}
static void notified(void *user) { ++*(unsigned *)user; }
int main(void) {
    enum { N = 64 }; unsigned positions[N] = {0}, notifications = 0;
    af_options options; af_options_default(&options); options.small_quota_words = 64;
    af_farm *farm = NULL; check(af_create(&options, &farm));
    af_actor_runtime *runtime = NULL; check(af_actor_runtime_create(farm, N, NULL, &runtime));
    af_token *token = NULL; check(af_register_producer(farm, AF_SMALL, &token));
    af_view *view = NULL; check(af_view_create(&view)); check(af_subscribe(view, farm));
    /* Owners and handles stay embedded in application entities. */
    struct entity { body initial; af_actor_owner owner; af_actor_handle handle; uint32_t result; };
    struct entity entities[N] = {0};
    af_actor_kind_options kind_options; af_actor_kind_options_default(&kind_options);
    kind_options.handler = idle; kind_options.user = NULL; kind_options.state_bytes = sizeof(body); kind_options.state_alignment = _Alignof(body); kind_options.destroy = NULL;
    const af_actor_kind *kind = NULL; check(af_actor_kind_register(runtime, &kind_options, &kind));
    for (unsigned i = 0; i < N; ++i) entities[i].initial.projection = positions + i;
    af_actor_owner_output_span owners = {&entities[0].owner, sizeof(struct entity), N};
    af_actor_batch_result created = af_actor_create_copy_batch(runtime, kind,
        (af_actor_state_span){&entities[0].initial, sizeof(struct entity), N}, owners);
    /* Handle a successful prefix even if a later allocation failed. */
    if (created.count != N) {
        for (size_t i = 0; i < created.count; ++i) {
            af_actor_request_retire(runtime, &entities[i].owner, AF_ACTOR_DRAIN);
            af_actor_reclaim(runtime, &entities[i].owner);
        }
        check(created.status);
    }
    for (unsigned i = 0; i < N; ++i)
        af_actor_owner_handle(&entities[i].owner, &entities[i].handle);
    af_actor_wave *wave = NULL; check(af_actor_wave_create(farm, &wave));
    af_completion_hook hook = {&notifications, notified};
    unsigned failed = 0;
    for (unsigned phase = 0; phase < 2; ++phase) {
        unsigned distance = phase ? 4 : 3;
        af_actor_wave_begin(wave, move_body, &distance, &hook);
        af_actor_wave_handle completion; af_actor_wave_observe(wave, &completion);
        size_t offset = 0;
        while (offset < N) {
            offset += af_actor_wave_publish(wave,
                (af_actor_handle_span){&entities[offset].handle, sizeof(struct entity), N - offset},
                token, AF_ACTOR_DEFAULT_COST);
            if (af_actor_wave_failed(&completion)) break;
            af_consume_next(view);
        }
        /* Seal and join even if admission rejected an actor after earlier tables. */
        af_actor_wave_seal(wave);
        while (!af_actor_wave_ready(&completion)) af_consume_next(view);
        if (af_actor_wave_failed(&completion)) { failed = 1; break; }
        /* ready also joins notified(), so both the projection and hook are safe. */
    }
    af_actor_wave_destroy(wave);
    unsigned sum = 0;
    for (unsigned i = 0; i < N; ++i) sum += positions[i];
    af_actor_result_span results = {&entities[0].result, sizeof(struct entity), N};
    af_actor_batch_result retired = af_actor_request_retire_batch(runtime,
        (af_actor_owner_span){&entities[0].owner, sizeof(struct entity), N}, AF_ACTOR_DRAIN, results);
    check(retired.status);
    /* These actors are idle after their joined waves, so retirement completes
       inline. General callers must pump and retry busy reclaim results. */
    af_actor_batch_result reclaimed = af_actor_reclaim_batch(runtime, owners, results);
    check(reclaimed.status); if (reclaimed.count != N) return 1;
    af_unsubscribe(view); af_view_destroy(view);
    af_actor_runtime_destroy(runtime); af_unregister_producer(farm, token); af_destroy(farm);
    printf("wave positions = %u, notifications = %u\n", sum, notifications);
    return failed || sum != 448 || notifications != 2;
}
