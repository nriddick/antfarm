#include "check.h"
#include "antfarm_api.h"
#include <stdio.h>
/* The table in a core-only program: the three core sections filled, the
   runtime sections absent, and calls through it. */
int main(void) {
    const af_api *api = af_api_get();
    CHECK(api && api == af_api_get() && api->size == sizeof *api);
    CHECK(api->abi_version == AF_ABI_VERSION && api->reserved == 0);
    CHECK(api->core && api->core->size == sizeof *api->core);
    CHECK(api->actors && api->actors->size == sizeof *api->actors);
    CHECK(api->private_roots && api->private_roots->size == sizeof *api->private_roots);
    CHECK(!api->runtime && !api->forkjoin && !api->groups && !api->fibers && !api->uv);
    for (unsigned i = 0; i < 6; ++i) CHECK(api->reserved_sections[i] == NULL);
    CHECK(api->core->abi_version == af_abi_version && api->core->abi_version() == AF_ABI_VERSION);
    CHECK(api->actors->actor_runtime_create == af_actor_runtime_create);
    CHECK(api->private_roots->private_clock_frequency == af_private_clock_frequency);
    af_options o; api->core->options_default(&o); CHECK(o.struct_size == sizeof o);
    af_farm *f = NULL; CHECK(api->core->create(&o, &f) == AF_OK && f);
    CHECK(api->core->used_large_pages(f) <= 1);
    api->core->destroy(f);
    CHECK(api->private_roots->private_clock_frequency() > 0);
    af_actor_kind_options k; api->actors->actor_kind_options_default(&k); CHECK(k.struct_size == sizeof k);
    puts("C ABI 2 table passed: core sections, calls through the table, runtime sections absent");
    return 0;
}
