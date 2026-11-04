/* A module that reaches Ant Farm only through the function table: it is
   built as a shared library with no unresolved symbol but libc's, which is
   what a hot-swapped module on Windows needs. The host loads it by path and
   calls antfarm_test_module with its table. */
#include "antfarm_api.h"
#ifdef _WIN32
#define MODULE_EXPORT __declspec(dllexport)
#else
#define MODULE_EXPORT
#endif
static uint64_t sum;
static unsigned done;
static void add(const uint64_t *b, size_t n) { for (size_t i = 0; i < n; ++i) sum += b[i]; }
static void completed(void *p) { ++*(unsigned *)p; }
MODULE_EXPORT uint32_t antfarm_test_module(const af_api *api) {
    if (api == NULL || api->abi_version != AF_ABI_VERSION || api->size < sizeof *api) return 1;
    if (api->core == NULL || api->core->size < sizeof *api->core) return 2;
    if (api->runtime == NULL || api->runtime->size < sizeof *api->runtime) return 3;
    af_host_options o; api->runtime->host_options_default(&o);
    o.workers = 0; o.ring_mib = 2;
    af_host *host = NULL;
    if (api->runtime->host_create(&o, &host) != AF_OK) return 4;
    af_farm *farm = api->runtime->host_farm(host);
    af_token *token = NULL;
    if (api->runtime->host_register_producer(host, AF_SMALL, &token) != AF_OK) return 5;
    uint64_t words[3] = {2, 3, 5};
    af_completion_hook hook = {&done, completed}; /* stable: this frame waits for it */
    sum = 0; done = 0;
    if (api->core->write_fixed(farm, add, words, 1, 3, token, AF_COST_AUTO, &hook) != 3) return 6;
    while (!done) if (api->runtime->host_step(host) != AF_OK) return 7;
    if (sum != 10) return 8;
    api->core->unregister_producer(farm, token);
    if (api->runtime->host_close(host) != AF_OK) return 9;
    return 0;
}
