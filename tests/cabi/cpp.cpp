#include "check.h"
#include "antfarm_actors.h"
#include "antfarm.hpp"
#include <type_traits>
#include <thread>
#include <memory>
#include <new>
#ifdef AF_TEST_TSAN
// LDC links the TSan archive without extracting its preinit-only object.
// Initialize before libstdc++'s constructors call intercepted pthread routines.
extern "C" void __tsan_init();
__attribute__((section(".preinit_array"), used))
static void (*const tsan_preinit)() = __tsan_init;
#endif
static unsigned calls;
static std::atomic<unsigned> start_threads(0), thread_calls(0);
extern "C" void count(const uint64_t *, size_t) noexcept { ++calls; }
extern "C" void thread_count(const uint64_t *, size_t) noexcept { thread_calls.fetch_add(1); }
static void publish(antfarm::producer p) {
    while (!start_threads.load()) std::this_thread::yield();
    CHECK(p.write_fixed(thread_count, nullptr, 0, 1) == 1);
}
static void consume(antfarm::consumer c) {
    while (!start_threads.load()) std::this_thread::yield();
    while (!thread_calls.load()) c.poll();
}
static_assert(!std::is_copy_constructible<antfarm::producer>::value, "unique token");
static_assert(!std::is_copy_constructible<antfarm::consumer>::value, "unique view");
static_assert(noexcept(antfarm::farm::create(std::declval<antfarm::farm &>())), "fallible creation does not throw");

struct alignas(64) placed_actor {
    std::unique_ptr<unsigned> value;
    size_t index;
    placed_actor *address;
    unsigned char padding[40]; // Fill the explicit 64-byte alignment without MSVC C4324.
    static std::atomic<unsigned> destroyed;
    explicit placed_actor(size_t i) noexcept : value(new unsigned(7)), index(i), address(this) {}
    ~placed_actor() noexcept { CHECK(address == this && *value == 7); ++destroyed; }
};
std::atomic<unsigned> placed_actor::destroyed(0);
extern "C" void placed_handler(void *, void *, size_t, af_actor_context *) noexcept {}
extern "C" af_status placed_init(void *, void *p, size_t bytes, size_t index) noexcept {
    CHECK(bytes == sizeof(placed_actor) && reinterpret_cast<uintptr_t>(p) % alignof(placed_actor) == 0);
    if (index == 2) return AF_INVALID; // No object constructed on failure.
    new (p) placed_actor(index);
    return AF_OK;
}
extern "C" void placed_destroy(void *, void *p, size_t bytes) noexcept {
    CHECK(bytes == sizeof(placed_actor)); static_cast<placed_actor *>(p)->~placed_actor();
}
static void placement_actors() {
    af_farm *f = nullptr; CHECK(af_create(nullptr, &f) == AF_OK);
    af_actor_runtime *r = nullptr; CHECK(af_actor_runtime_create(f, 4, nullptr, &r) == AF_OK);
    af_actor_kind_options options = {placed_handler, nullptr, sizeof(placed_actor), alignof(placed_actor), placed_destroy};
    const af_actor_kind *kind = nullptr; CHECK(af_actor_kind_register(r, &options, &kind) == AF_OK);
    af_actor_owner owners[4] = {};
    af_actor_owner_output_span output = {owners, sizeof owners[0], 4};
    af_actor_batch_result made = af_actor_create_init_batch(r, kind, placed_init, nullptr, 0, output);
    CHECK(made.count == 2 && made.status == AF_ACTOR_INITIALIZER_FAILED && !owners[2]._slot);
    CHECK(!placed_actor::destroyed.load());
    for (size_t i = 0; i < made.count; ++i) {
        // Copies race on native threads. Only one destructor may run.
        std::thread reclaimers[4];
        for (auto &thread : reclaimers) {
            af_actor_owner copy = owners[i];
            thread = std::thread([r, copy]() mutable {
                for (;;) {
                    af_actor_request_retire(r, &copy, AF_ACTOR_DRAIN);
                    unsigned result = af_actor_reclaim(r, &copy);
                    if (result == AF_ACTOR_RECLAIMED || result == AF_ACTOR_RECLAIM_STALE) break;
                    CHECK(result == AF_ACTOR_RECLAIM_BUSY); std::this_thread::yield();
                }
                CHECK(!copy._slot);
            });
        }
        for (auto &thread : reclaimers) thread.join();
        CHECK(af_actor_reclaim(r, owners + i) == AF_ACTOR_RECLAIM_STALE);
    }
    CHECK(placed_actor::destroyed.load() == 2);
    af_actor_runtime_destroy(r); af_destroy(f);
}

int main() {
    placement_actors();
    antfarm::farm f;
    CHECK(!f.get() && antfarm::farm::create(f) == AF_OK);
    CHECK(antfarm::farm::create(f) == AF_BUSY);
    antfarm::producer p, replacement;
    CHECK(antfarm::producer::create(p, f) == AF_OK);
    CHECK(antfarm::producer::create(replacement, f) == AF_OK);
    antfarm::consumer v, other;
    CHECK(antfarm::consumer::create(v, f) == AF_OK);
    CHECK(antfarm::consumer::create(other, f) == AF_OK);
    CHECK(antfarm::consumer::create(other, f) == AF_BUSY);
    antfarm::producer moved(std::move(p)); replacement = std::move(moved);
    antfarm::consumer cursor(std::move(v)); other = std::move(cursor);
    auto &self_f = f; f = std::move(self_f);
    auto &self_p = replacement; replacement = std::move(self_p);
    auto &self_v = other; other = std::move(self_v);
    CHECK(f.get() && replacement.get() && other.get());
    { antfarm::farm owner(std::move(f)); } // participants retain the Farm
    CHECK(!f.get() && !p.get() && !moved.get() && !v.get() && !cursor.get());
    CHECK(replacement.write_fixed(count, nullptr, 0, 3) == 3);
    while (calls != 3) other.poll();
    af_options bad = {};
    CHECK(antfarm::farm::create(f, &bad) == AF_INVALID && !f.get());
#if ANTFARM_CPP_EXCEPTIONS
    try { auto invalid = antfarm::farm::make(&bad); (void)invalid; CHECK(false); }
    catch (const antfarm::error &e) { CHECK(e.status == AF_INVALID); }
    auto good = antfarm::farm::make();
    auto producer = antfarm::producer::make(good);
    CHECK(good.get() && producer.get());
#endif
    antfarm::farm shared;
    antfarm::producer sender;
    antfarm::consumer receiver;
    CHECK(antfarm::farm::create(shared) == AF_OK);
    CHECK(antfarm::producer::create(sender, shared) == AF_OK);
    CHECK(antfarm::consumer::create(receiver, shared) == AF_OK);
    std::thread writer(publish, std::move(sender)), reader(consume, std::move(receiver));
    shared = antfarm::farm();
    start_threads.store(1);
    writer.join(); reader.join(); // last participant may destroy Farm on either thread
    CHECK(thread_calls.load() == 1);
    puts("C++ ownership checks passed");
    return 0;
}
