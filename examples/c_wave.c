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
static void move_body(void *user, void *state, size_t bytes) {
    (void)bytes; body *b = (body *)state;
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
    af_actor_owner *owners[N]; af_actor_handle actors[N];
    for (unsigned i = 0; i < N; ++i) {
        body initial = {0, positions + i};
        check(af_actor_create(runtime, &initial, sizeof initial, _Alignof(body), idle, NULL, owners + i));
        af_actor_owner_handle(owners[i], actors + i);
    }
    af_actor_wave *wave = NULL; check(af_actor_wave_create(farm, &wave));
    af_completion_hook hook = {&notifications, notified};
    unsigned failed = 0;
    for (unsigned phase = 0; phase < 2; ++phase) {
        unsigned distance = phase ? 4 : 3;
        af_actor_wave_begin(wave, move_body, &distance, &hook);
        af_actor_wave_handle completion; af_actor_wave_observe(wave, &completion);
        size_t offset = 0;
        while (offset < N) {
            offset += af_actor_wave_publish(wave, actors + offset, N - offset, token, AF_ACTOR_DEFAULT_COST);
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
    for (unsigned i = 0; i < N; ++i) {
        sum += positions[i]; af_actor_request_retire(owners[i], AF_ACTOR_DRAIN);
        while (!af_actor_retired(owners[i])) {
            af_actor_runtime_flush(runtime, token, 32, AF_ACTOR_DEFAULT_COST); af_consume_next(view);
        }
        if (af_actor_reclaim(owners[i]) != AF_ACTOR_RECLAIMED) return 1;
    }
    af_unsubscribe(view); af_view_destroy(view);
    af_actor_runtime_destroy(runtime); af_unregister_producer(farm, token); af_destroy(farm);
    printf("wave positions = %u, notifications = %u\n", sum, notifications);
    return failed || sum != 448 || notifications != 2;
}
