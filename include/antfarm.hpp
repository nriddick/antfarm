/* C++11 ownership adapter. No exceptions cross a Farm callback. */
#ifndef ANTFARM_HPP
#define ANTFARM_HPP
#include "antfarm.h"
#include <atomic>
#include <new>
#include <utility>
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS) || defined(_CPPUNWIND)
#define ANTFARM_CPP_EXCEPTIONS 1
#include <stdexcept>
#else
#define ANTFARM_CPP_EXCEPTIONS 0
#endif

namespace antfarm {
namespace detail {
struct farm_control {
    std::atomic<size_t> references;
    af_farm *value;
    explicit farm_control(af_farm *p) noexcept : references(1), value(p) {}
};
// Only ownership changes touch this counter; write/poll go straight to C.
class farm_ref {
    farm_control *p_ = nullptr;
public:
    farm_ref() noexcept = default;
    explicit farm_ref(farm_control *p) noexcept : p_(p) {}
    farm_ref(const farm_ref &r) noexcept : p_(r.p_) {
        if (p_) p_->references.fetch_add(1, std::memory_order_relaxed);
    }
    farm_ref(farm_ref &&r) noexcept : p_(r.p_) { r.p_ = nullptr; }
    farm_ref &operator=(farm_ref r) noexcept { std::swap(p_, r.p_); return *this; }
    ~farm_ref() {
        // The final owner acquires all earlier releases before destroying Farm.
        if (p_ && p_->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            af_destroy(p_->value); delete p_;
        }
    }
    af_farm *get() const noexcept { return p_ ? p_->value : nullptr; }
};
}

#if ANTFARM_CPP_EXCEPTIONS
class error : public std::runtime_error {
    static const char *message(af_status s) noexcept {
        switch (s) {
        case AF_INVALID: return "invalid Ant Farm arguments";
        case AF_FULL: return "Ant Farm capacity full";
        case AF_RETRY: return "Ant Farm subscription must be retried";
        case AF_ALLOCATION_FAILED: return "Ant Farm allocation failed";
        case AF_SYSTEM_ERROR: return "Ant Farm system error";
        case AF_BUSY: return "Ant Farm busy: output owns a handle, or the target is active";
        case AF_RUNTIME_ERROR: return "Ant Farm runtime error";
        default: return "Ant Farm status";
        }
    }
public:
    af_status status;
    explicit error(af_status s) : std::runtime_error(message(s)), status(s) {}
};
inline void check(af_status status) { if (status != AF_OK) throw error(status); }
#endif

class producer;
class consumer;
class private_roots; // antfarm_private.hpp
// Default constructors are empty in every build. Factories require empty output
// and leave it unchanged on failure. Status is authoritative: wrapper errors
// (busy output/control allocation) do not set the C thread diagnostic.
class farm {
    detail::farm_ref value_;
    friend class producer;
    friend class consumer;
    friend class private_roots;
public:
    farm() noexcept = default;
    static af_status create(farm &out, const af_options *options = nullptr) noexcept {
        if (out.get()) return AF_BUSY;
        af_farm *p = nullptr;
        const af_status s = af_create(options, &p);
        if (s != AF_OK) return s;
        auto control = new (std::nothrow) detail::farm_control(p);
        if (!control) { af_destroy(p); return AF_ALLOCATION_FAILED; }
        out.value_ = detail::farm_ref(control);
        return AF_OK;
    }
    // Defined only with exceptions; calling make without them is a link error.
    // Keep the class identical across build modes; use create in either mode.
    static farm make(const af_options *options = nullptr);
    farm(const farm &) = delete;
    farm &operator=(const farm &) = delete;
    farm(farm &&) noexcept = default;
    farm &operator=(farm &&) noexcept = default;
    af_farm *get() const noexcept { return value_.get(); }
};
class producer {
    detail::farm_ref farm_;
    af_token *value_ = nullptr;
    void reset() noexcept { if (value_) af_unregister_producer(farm_.get(), value_); value_ = nullptr; }
public:
    producer() noexcept = default;
    static af_status create(producer &out, const farm &f, af_tier tier = AF_SMALL) noexcept {
        if (out.value_) return AF_BUSY;
        af_token *p = nullptr;
        const af_status s = af_register_producer(f.get(), tier, &p);
        if (s != AF_OK) return s;
        out.farm_ = f.value_; out.value_ = p;
        return AF_OK;
    }
    // Defined only with exceptions; calling make without them is a link error.
    // Keep the class identical across build modes; use create in either mode.
    static producer make(const farm &f, af_tier tier = AF_SMALL);
    ~producer() { reset(); }
    producer(const producer &) = delete;
    producer &operator=(const producer &) = delete;
    producer(producer &&p) noexcept : farm_(std::move(p.farm_)), value_(p.value_) { p.value_ = nullptr; }
    producer &operator=(producer &&p) noexcept {
        if (this != &p) { reset(); farm_ = std::move(p.farm_); value_ = p.value_; p.value_ = nullptr; }
        return *this;
    }
    af_token *get() const noexcept { return value_; }
    size_t write(const af_entry *p, size_t n, uint32_t cost = AF_DEFAULT_COST,
                 const af_completion_hook *hook = nullptr) noexcept {
        return af_write(farm_.get(), p, n, value_, cost, hook);
    }
    size_t write(af_callback call, const af_body *p, size_t n, uint32_t cost = AF_DEFAULT_COST,
                 const af_completion_hook *hook = nullptr) noexcept {
        return af_write_bodies(farm_.get(), call, p, n, value_, cost, hook);
    }
    size_t write_fixed(af_callback call, const uint64_t *p, size_t width, size_t n,
                       uint32_t cost = AF_DEFAULT_COST, const af_completion_hook *hook = nullptr) noexcept {
        return af_write_fixed(farm_.get(), call, p, width, n, value_, cost, hook);
    }
};
class consumer {
    detail::farm_ref farm_;
    af_view *value_ = nullptr;
    void reset() noexcept {
        if (value_) { af_unsubscribe(value_); af_view_destroy(value_); value_ = nullptr; }
    }
public:
    consumer() noexcept = default;
    // af_subscribe retries the transient frontier race itself.
    static af_status create(consumer &out, const farm &f) noexcept {
        if (out.value_) return AF_BUSY;
        if (!f.get()) return AF_INVALID;
        af_view *p = nullptr;
        af_status s = af_view_create(&p);
        if (s != AF_OK) return s;
        s = af_subscribe(p, f.get());
        if (s != AF_OK) { af_view_destroy(p); return s; }
        out.farm_ = f.value_; out.value_ = p;
        return AF_OK;
    }
    ~consumer() { reset(); }
    consumer(const consumer &) = delete;
    consumer &operator=(const consumer &) = delete;
    consumer(consumer &&v) noexcept : farm_(std::move(v.farm_)), value_(v.value_) { v.value_ = nullptr; }
    consumer &operator=(consumer &&v) noexcept {
        if (this != &v) { reset(); farm_ = std::move(v.farm_); value_ = v.value_; v.value_ = nullptr; }
        return *this;
    }
    af_view *get() const noexcept { return value_; }
    bool poll() noexcept { return af_consume_next(value_) != 0; }
};
#if ANTFARM_CPP_EXCEPTIONS
inline farm farm::make(const af_options *options) {
    farm out; check(create(out, options)); return out;
}
inline producer producer::make(const farm &f, af_tier tier) {
    producer out; check(create(out, f, tier)); return out;
}
#endif
} // namespace antfarm
#endif
