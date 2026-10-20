/* C++11 ownership adapter for private root parks. Callbacks and child cohorts
   stay raw C; cohorts are owned by their parent actor, not by the caller. */
#ifndef ANTFARM_PRIVATE_HPP
#define ANTFARM_PRIVATE_HPP
#include "antfarm.hpp"
#include "antfarm_private.h"

namespace antfarm {
// Owns one root park and retains its Farm, so the park is destroyed first.
// Destruction is af_private_roots_destroy: drain live roots and take every
// tracked outcome before scope exit, unwinding included, or teardown is fatal.
// Same factory rules as antfarm.hpp: empty output, unchanged on failure.
class private_roots {
    detail::farm_ref farm_;
    af_private_roots *value_ = nullptr;
    void reset() noexcept { if (value_) af_private_roots_destroy(value_); value_ = nullptr; }
public:
    private_roots() noexcept = default;
    static af_status create(private_roots &out, const farm &f, const af_private_root_options &options,
                            const af_actor_allocator *allocator = nullptr) noexcept {
        if (out.value_) return AF_BUSY;
        af_private_roots *p = nullptr;
        const af_status s = af_private_roots_create(f.get(), &options, allocator, &p);
        if (s != AF_OK) return s;
        out.farm_ = f.value_; out.value_ = p;
        return AF_OK;
    }
    // Defined only with exceptions; calling make without them is a link error.
    // Keep the class identical across build modes; use create in either mode.
    static private_roots make(const farm &f, const af_private_root_options &options,
                              const af_actor_allocator *allocator = nullptr);
    ~private_roots() { reset(); }
    private_roots(const private_roots &) = delete;
    private_roots &operator=(const private_roots &) = delete;
    private_roots(private_roots &&p) noexcept : farm_(std::move(p.farm_)), value_(p.value_) { p.value_ = nullptr; }
    private_roots &operator=(private_roots &&p) noexcept {
        if (this != &p) { reset(); farm_ = std::move(p.farm_); value_ = p.value_; p.value_ = nullptr; }
        return *this;
    }
    af_private_roots *get() const noexcept { return value_; }
    size_t live() noexcept { return af_private_roots_live(value_); }
    void collect() noexcept { af_private_roots_collect(value_); }
    af_private_pump_count pump(af_token *token, size_t limit = 0,
                               uint32_t cost = AF_PRIVATE_AUTO_COST) noexcept {
        return af_private_roots_pump(value_, token, limit, cost);
    }
    af_status spawn_copy(const void *initial, int64_t deadline = AF_PRIVATE_NO_DEADLINE,
                         af_private_root_handle *handle = nullptr) noexcept {
        return af_private_root_spawn_copy(value_, initial, deadline, handle);
    }
    af_status spawn_init(af_actor_initializer init, void *user, size_t index,
                         int64_t deadline = AF_PRIVATE_NO_DEADLINE,
                         af_private_root_handle *handle = nullptr) noexcept {
        return af_private_root_spawn_init(value_, init, user, index, deadline, handle);
    }
    bool take_outcome(af_private_root_outcome &out) noexcept {
        return af_private_root_take_outcome(value_, &out) != 0;
    }
    size_t cancel_all() noexcept { return af_private_roots_cancel_all(value_); }
};
#if ANTFARM_CPP_EXCEPTIONS
inline private_roots private_roots::make(const farm &f, const af_private_root_options &options,
                                         const af_actor_allocator *allocator) {
    private_roots out; check(create(out, f, options, allocator)); return out;
}
#endif
} // namespace antfarm
#endif
