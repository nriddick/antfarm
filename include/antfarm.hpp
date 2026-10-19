/* Minimal C++11 ownership adapter. No exceptions cross a Farm callback. */
#ifndef ANTFARM_HPP
#define ANTFARM_HPP
#include "antfarm.h"
#include <memory>
#include <stdexcept>
#include <utility>

namespace antfarm {
class error : public std::runtime_error {
public:
    af_status status;
    explicit error(af_status s) : std::runtime_error(af_last_error_message()), status(s) {}
};
inline void check(af_status status) { if (status != AF_OK) throw error(status); }
class producer;
class consumer;
class farm {
    std::shared_ptr<af_farm> value_;
    friend class producer;
    friend class consumer;
public:
    explicit farm(const af_options *options = nullptr) {
        af_farm *p = nullptr;
        check(af_create(options, &p));
        value_ = std::shared_ptr<af_farm>(p, af_destroy);
    }
    farm(const farm &) = delete;
    farm &operator=(const farm &) = delete;
    farm(farm &&) noexcept = default;
    farm &operator=(farm &&) noexcept = default;
    af_farm *get() const noexcept { return value_.get(); }
};
class producer {
    std::shared_ptr<af_farm> farm_;
    af_token *value_ = nullptr;
    void reset() noexcept { if (value_) af_unregister_producer(farm_.get(), value_); value_ = nullptr; }
public:
    explicit producer(const farm &f, af_tier tier = AF_SMALL) : farm_(f.value_) {
        check(af_register_producer(farm_.get(), tier, &value_));
    }
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
    std::shared_ptr<af_farm> farm_;
    af_view *value_ = nullptr;
    void reset() noexcept {
        if (value_) { af_unsubscribe(value_); af_view_destroy(value_); value_ = nullptr; }
    }
public:
    explicit consumer(const farm &f) : farm_(f.value_) {
        check(af_view_create(&value_));
        const af_status s = af_subscribe(value_, farm_.get());
        if (s != AF_OK) { af_view_destroy(value_); value_ = nullptr; check(s); }
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
} // namespace antfarm
#endif
