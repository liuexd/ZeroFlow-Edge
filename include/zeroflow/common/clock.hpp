#pragma once

#include <cstdint>
#include <ctime>

namespace zeroflow::common {

[[nodiscard]] inline uint64_t now_monotonic_ns() noexcept {
    struct timespec ts {};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

[[nodiscard]] inline uint64_t now_monotonic_us() noexcept {
    return now_monotonic_ns() / 1000ULL;
}

[[nodiscard]] inline uint64_t now_monotonic_raw_ns() noexcept {
    struct timespec ts {};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
}

}  // namespace zeroflow::common