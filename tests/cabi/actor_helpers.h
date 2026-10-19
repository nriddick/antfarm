#ifndef AF_ACTOR_TEST_HELPERS_H
#define AF_ACTOR_TEST_HELPERS_H
#include "antfarm_actors.h"
#include "check.h"
static const af_actor_kind *test_kind(af_actor_runtime *r, size_t bytes, size_t alignment,
                                     af_actor_handler handler, void *user) {
    af_actor_kind_options options = {handler, user, bytes, alignment, NULL};
    const af_actor_kind *kind = NULL;
    CHECK(af_actor_kind_register(r, &options, &kind) == AF_OK);
    return kind;
}
static unsigned is_retired(af_actor_runtime *r, const af_actor_owner *owner) {
    unsigned value = 0; CHECK(af_actor_retired(r, owner, &value) == AF_OK); return value;
}
#endif
