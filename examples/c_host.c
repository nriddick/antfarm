/* Optional non-Fiber runtime. Compile this file with a C compiler; use the D
   compiler driver to link antfarm_runtime, antfarm_core and its runtime. */
#include "antfarm_runtime.h"
#include <stdio.h>
#include <stdlib.h>
#ifdef _WIN32
#include <windows.h>
static volatile LONG done;
static void complete(void *p) { InterlockedExchange((volatile LONG *)p, 1); }
static unsigned is_done(void) { return (unsigned)InterlockedCompareExchange(&done, 0, 0); }
#else
#include <stdatomic.h>
static atomic_uint done;
static void complete(void *p) { atomic_store((atomic_uint *)p, 1); }
static unsigned is_done(void) { return atomic_load(&done); }
#endif
static unsigned values[1024];
static void fill(const uint64_t *body, size_t n) {
    if (n != 2) abort();
    for (size_t i = (size_t)body[0]; i < (size_t)body[1]; ++i) values[i] = (unsigned)i;
}
static void check(af_status status) {
    if (status != AF_OK) { fprintf(stderr, "%s\n", af_last_error_message()); exit(1); }
}
int main(void) {
    check(af_runtime_init());
    af_host_options options; af_host_options_default(&options); options.workers = 2;
    af_host *host = NULL; check(af_host_create(&options, &host));
    af_farm *farm = af_host_farm(host); af_token *token = NULL;
    check(af_host_register_producer(host, AF_SMALL, &token));
    const uint64_t grains[][2] = {{0, 256}, {256, 512}, {512, 768}, {768, 1024}};
    const af_completion_hook hook = {(void *)&done, complete};
    if (af_write_fixed(farm, fill, &grains[0][0], 2, 4, token, AF_DEFAULT_COST, &hook) != 4) abort();
    check(af_host_wake(host));
    while (!is_done()) check(af_host_step(host));
    unsigned sum = 0; for (unsigned i = 0; i < 1024; ++i) sum += values[i];
    printf("parallel sum = %u\n", sum);
    af_unregister_producer(farm, token); check(af_host_close(host));
    check(af_runtime_shutdown());
    return sum == 523776 ? 0 : 1;
}
