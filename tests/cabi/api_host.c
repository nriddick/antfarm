#include "check.h"
#include "antfarm_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* The table in a runtime program: the runtime sections appear at init, and a
   module loaded by path runs through the table alone. */
typedef uint32_t (*module_entry)(const af_api *);
#ifdef _WIN32
#include <windows.h>
static void *load_module(const char *path) { void *m = LoadLibraryA(path); CHECK(m); return m; }
static module_entry find_entry(void *m) {
    FARPROC sym = GetProcAddress((HMODULE)m, "antfarm_test_module"); CHECK(sym);
    module_entry entry; memcpy(&entry, &sym, sizeof entry); return entry;
}
static void unload_module(void *m) { CHECK(FreeLibrary((HMODULE)m)); }
#else
#include <dlfcn.h>
static void *load_module(const char *path) {
    void *m = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!m) { fprintf(stderr, "dlopen: %s\n", dlerror()); abort(); }
    return m;
}
static module_entry find_entry(void *m) {
    void *sym = dlsym(m, "antfarm_test_module"); CHECK(sym);
    module_entry entry; memcpy(&entry, &sym, sizeof entry); return entry;
}
static void unload_module(void *m) { CHECK(dlclose(m) == 0); }
#endif
int main(int argc, char **argv) {
    CHECK(argc == 2);
    const af_api *api = af_api_get();
    CHECK(api && api->size == sizeof *api && api->abi_version == AF_ABI_VERSION);
    CHECK(api->core && api->actors && api->private_roots);
    CHECK(!api->runtime && !api->forkjoin && !api->groups && !api->fibers && !api->uv); /* before init */
    CHECK(af_druntime_init() == AF_OK);
    CHECK(api->runtime && api->runtime->size == sizeof *api->runtime);
    CHECK(api->forkjoin && api->forkjoin->size == sizeof *api->forkjoin);
    CHECK(api->groups && api->groups->size == sizeof *api->groups);
    CHECK(api->runtime->druntime_init == af_druntime_init && api->runtime->host_wait_until == af_host_wait_until);
    CHECK(api->forkjoin->processors == af_processors && api->groups->group_start == af_group_start);
    void *module = load_module(argv[1]);
    module_entry entry = find_entry(module);
    uint32_t result = entry(api);
    if (result) { fprintf(stderr, "module failed: %u (%s)\n", result, af_last_error_message()); abort(); }
    af_api withheld = *api; withheld.runtime = NULL; /* a resident may hand out less */
    CHECK(entry(&withheld) == 3);
    unload_module(module);
    CHECK(af_druntime_shutdown() == AF_OK);
    puts("C ABI 2 table passed: runtime sections after init, a module loaded by path that imports nothing");
    return 0;
}
