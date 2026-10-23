// C++11 application threads + the runtime-free C core; no FarmHost or druntime.
#include "antfarm.h"
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>

static const size_t count = 256;
static uint64_t squares[count];
static std::atomic<size_t> completed{0};

extern "C" void square(const uint64_t *body, size_t words) noexcept {
    if (words != 1 || body[0] >= count) std::abort();
    const size_t i = static_cast<size_t>(body[0]);
    squares[i] = i * i; // Each single-shot payload owns a distinct output slot.
    completed.fetch_add(1, std::memory_order_release);
}

static void check(af_status status) {
    if (status != AF_OK) {
        std::fprintf(stderr, "%s\n", af_last_error_message());
        std::abort();
    }
}

int main() {
    af_farm *farm = nullptr;
    af_token *token = nullptr;
    af_view *views[2] = {};
    check(af_create(nullptr, &farm));
    check(af_register_producer(farm, AF_SMALL, &token));
    // Subscribe before publishing. Each view has one exclusive owner at a time.
    for (auto &view : views) {
        check(af_view_create(&view));
        check(af_subscribe(view, farm));
    }
    std::atomic<bool> stop{false};
    std::thread workers[2];
    for (size_t i = 0; i < 2; ++i) {
        af_view *view = views[i];
        workers[i] = std::thread([view, &stop] {
            while (!stop.load(std::memory_order_acquire)) {
                if (!af_consume_next(view)) std::this_thread::yield();
            }
        });
    }
    uint64_t indices[count];
    for (size_t i = 0; i < count; ++i) indices[i] = i;
    for (size_t sent = 0; sent < count;) {
        const size_t accepted = af_write_fixed(farm, square, indices + sent, 1,
                                               count - sent, token, AF_DEFAULT_COST, nullptr);
        sent += accepted; // Retry only the unaccepted suffix; zero is backpressure.
        if (!accepted) std::this_thread::yield();
    }
    // An idle poll is not completion. Count work, then join callback returns.
    while (completed.load(std::memory_order_acquire) != count) std::this_thread::yield();
    stop.store(true, std::memory_order_release);
    for (auto &worker : workers) worker.join();
    uint64_t sum = 0;
    for (auto value : squares) sum += value;
    for (auto view : views) {
        af_unsubscribe(view);
        af_view_destroy(view);
    }
    af_unregister_producer(farm, token);
    af_destroy(farm);
    std::printf("native threads: 256 squares summed to %llu\n", static_cast<unsigned long long>(sum));
    return sum == 5559680 ? 0 : 1;
}
