#include "antfarm.hpp"
#include <cassert>
#include <type_traits>
static unsigned calls;
extern "C" void count(const uint64_t *, size_t) noexcept { ++calls; }
static_assert(!std::is_copy_constructible<antfarm::producer>::value, "unique token");
static_assert(!std::is_copy_constructible<antfarm::consumer>::value, "unique view");
int main() {
    antfarm::farm f;
    antfarm::producer p(f), replacement(f);
    antfarm::consumer v(f), other(f);
    antfarm::producer moved(std::move(p)); replacement = std::move(moved);
    antfarm::consumer cursor(std::move(v)); other = std::move(cursor);
    { antfarm::farm owner(std::move(f)); } // participants retain the Farm
    assert(replacement.write_fixed(count, nullptr, 0, 3) == 3);
    while (calls != 3) other.poll();
    try { af_options bad = {}; antfarm::farm invalid(&bad); assert(false); }
    catch (const antfarm::error &e) { assert(e.status == AF_INVALID); }
}
