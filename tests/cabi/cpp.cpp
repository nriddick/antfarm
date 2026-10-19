#include "check.h"
#include "antfarm_actors.h"
#include "antfarm.hpp"
#include <type_traits>
#include <thread>
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
int main() {
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
