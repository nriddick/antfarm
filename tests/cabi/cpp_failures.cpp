// Fault-injected C boundary: each partial acquisition must be unwound exactly once.
#include "check.h"
#include "antfarm.hpp"
#include <initializer_list>
static bool fail_control, live_farm;
static unsigned destroyed, views, tokens, subscriptions;
static af_status view_result = AF_OK, subscribe_result = AF_OK, producer_result = AF_OK;
void *operator new(size_t bytes, const std::nothrow_t &) noexcept {
    if (fail_control) { fail_control = false; return nullptr; }
    return ::operator new(bytes);
}
struct af_farm {};
struct af_token {};
struct af_view {};
static af_farm native_farm;
static af_token native_token;
static af_view native_view;
extern "C" af_status af_create(const af_options *, af_farm **out) {
    CHECK(!live_farm); live_farm = true; *out = &native_farm; return AF_OK;
}
extern "C" void af_destroy(af_farm *p) {
    CHECK(p == &native_farm && live_farm && !views && !tokens);
    live_farm = false; ++destroyed;
}
extern "C" af_status af_register_producer(af_farm *p, af_tier, af_token **out) {
    CHECK(p == &native_farm && live_farm); *out = nullptr;
    if (producer_result != AF_OK) return producer_result;
    CHECK(!tokens); ++tokens; *out = &native_token; return AF_OK;
}
extern "C" void af_unregister_producer(af_farm *p, af_token *t) {
    CHECK(p == &native_farm && t == &native_token && live_farm && tokens == 1); --tokens;
}
extern "C" af_status af_view_create(af_view **out) {
    *out = nullptr;
    if (view_result != AF_OK) return view_result;
    CHECK(!views); ++views; *out = &native_view; return AF_OK;
}
extern "C" af_status af_subscribe(af_view *v, af_farm *p) {
    CHECK(v == &native_view && p == &native_farm && live_farm && views == 1);
    ++subscriptions; return subscribe_result;
}
extern "C" void af_unsubscribe(af_view *v) { CHECK(v == &native_view && views == 1 && live_farm); }
extern "C" void af_view_destroy(af_view *v) { CHECK(v == &native_view && views == 1); --views; }
int main() {
    antfarm::farm f;
    fail_control = true;
    CHECK(antfarm::farm::create(f) == AF_ALLOCATION_FAILED);
    CHECK(!f.get() && !live_farm && destroyed == 1);
    CHECK(antfarm::farm::create(f) == AF_OK);
    antfarm::consumer c;
    view_result = AF_ALLOCATION_FAILED;
    CHECK(antfarm::consumer::create(c, f) == AF_ALLOCATION_FAILED && !views && !c.get());
    view_result = AF_OK;
    for (af_status status : {AF_RETRY, AF_FULL, AF_INVALID}) {
        subscribe_result = status;
        unsigned before = subscriptions;
        CHECK(antfarm::consumer::create(c, f) == status && !views && !c.get());
        CHECK(subscriptions == before + 1); // exactly one attempt, never spins
    }
    subscribe_result = AF_OK;
    CHECK(antfarm::consumer::create(c, f) == AF_OK);
    antfarm::producer p;
    producer_result = AF_FULL;
    CHECK(antfarm::producer::create(p, f) == AF_FULL && !tokens && !p.get());
    producer_result = AF_OK;
    CHECK(antfarm::producer::create(p, f) == AF_OK);
    f = antfarm::farm();
    CHECK(live_farm); // both independent participants retain ownership
    p = antfarm::producer();
    CHECK(live_farm && !tokens);
    c = antfarm::consumer();
    CHECK(!live_farm && !views && destroyed == 2);
    puts("C++ retry and allocation rollback checks passed");
    return 0;
}
