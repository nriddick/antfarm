#include "antfarm_private.h"
#include "antfarm_runtime.h"
#include "threads.h"
#include <stdio.h>
extern void antfarm_test_collect(void);
static uint32_t step(void *u, void *s, size_t bytes, af_private_context *ctx) {
    (void)ctx; CHECK(bytes == sizeof(unsigned)); unsigned *n = (unsigned *)s;
    if (++*n != 100) return AF_PRIVATE_AGAIN;
    test_add((test_atomic *)u, 1); return AF_PRIVATE_FINISH;
}
typedef struct emitter { af_private_roots *park; af_token *token; af_host *host; test_atomic done; } emitter;
TEST_THREAD(emit) {
    /* Core-only emitter deliberately does not attach to druntime. wake is the
       host's explicitly attachment-free entry point. */
    emitter *e = (emitter *)arg;
    while (af_private_roots_live(e->park)) {
        af_private_roots_pump(e->park, e->token, 0, AF_PRIVATE_AUTO_COST);
        CHECK(af_host_wake(e->host) == AF_OK); test_yield();
    }
    test_store(&e->done, 1); TEST_RETURN;
}
static void exercise(unsigned workers) {
    af_host_options hopt; af_host_options_default(&hopt);
    hopt.workers = workers; hopt.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *host = NULL; CHECK(af_host_create(&hopt, &host) == AF_OK);
    af_farm *farm = af_host_farm(host); test_atomic calls = {0};
    af_private_root_options o; af_private_root_options_default(&o);
    o.state.bytes = sizeof(unsigned); o.step = step; o.user = &calls;
    af_private_roots *park = NULL; CHECK(af_private_roots_create(farm, &o, NULL, &park) == AF_OK);
    CHECK(af_host_close(host) == AF_BUSY); /* Even an empty, allocated park pins the host Farm. */
    af_token *token = NULL; CHECK(af_host_register_producer(host, AF_SMALL, &token) == AF_OK);
    for (unsigned i = 0; i < 50; ++i) { unsigned initial = 0;
        CHECK(af_private_root_spawn_copy(park, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK); }
    emitter e = {park, token, host, {0}}; test_thread t = test_start(emit, &e);
    antfarm_test_collect();
    while (!test_load(&e.done)) { CHECK(af_host_step(host) == AF_OK); test_yield(); }
    test_join(t); CHECK(test_load(&calls) == 50);
    af_unregister_producer(farm, token); CHECK(af_host_close(host) == AF_BUSY);
    af_private_roots_destroy(park); CHECK(af_host_close(host) == AF_OK);
    printf("C private host passed: %u workers\n", workers);
}
/* ABI 2: the park transferred to the host, which pumps it from step and
   destroys it at close. */
static uint32_t park_empty(void *context) { return af_private_roots_live((af_private_roots *)context) == 0; }
static void exercise_attached(unsigned workers) {
    af_host_options hopt; af_host_options_default(&hopt);
    hopt.workers = workers; hopt.idle_timeout_ns = AF_WAIT_FOREVER;
    af_host *host = NULL; CHECK(af_host_create(&hopt, &host) == AF_OK);
    af_farm *farm = af_host_farm(host); test_atomic calls = {0};
    af_private_root_options o; af_private_root_options_default(&o);
    o.state.bytes = sizeof(unsigned); o.step = step; o.user = &calls;
    af_private_roots *park = NULL; CHECK(af_private_roots_create(farm, &o, NULL, &park) == AF_OK);
    af_farm *other = NULL; af_private_roots *foreign = NULL;
    CHECK(af_create(NULL, &other) == AF_OK && af_private_roots_create(other, &o, NULL, &foreign) == AF_OK);
    CHECK(af_host_attach_park(host, foreign) == AF_INVALID); /* not this host's Farm */
    CHECK(af_host_attach_park(host, NULL) == AF_INVALID);
    af_private_roots_destroy(foreign); af_destroy(other);
    CHECK(af_host_attach_park(host, park) == AF_OK);
    for (unsigned i = 0; i < 50; ++i) { unsigned initial = 0;
        CHECK(af_private_root_spawn_copy(park, &initial, AF_PRIVATE_NO_DEADLINE, NULL) == AF_OK); }
    CHECK(af_host_close(host) == AF_BUSY); /* live roots block close */
    CHECK(af_host_wait_until(host, park_empty, park, AF_WAIT_FOREVER) == AF_OK);
    CHECK(test_load(&calls) == 50);
    antfarm_test_collect();
    CHECK(af_host_close(host) == AF_OK); /* destroys the park */
    printf("C private host passed: %u workers, park transferred to the host\n", workers);
}
int main(void) {
    CHECK(af_druntime_init() == AF_OK); exercise(0); exercise(2);
    exercise_attached(0); exercise_attached(2);
    CHECK(af_druntime_shutdown() == AF_OK); return 0;
}
