#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace zeroflow::common {

namespace detail {
constexpr std::array<uint32_t, 256> generate_crc32_table() noexcept {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}
}  // namespace detail

[[nodiscard]] inline uint32_t compute_crc32(const uint8_t* data, std::size_t length) noexcept {
    static constexpr auto kTable = detail::generate_crc32_table();
    uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < length; ++i) {
        crc = kTable[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

}  // namespace zeroflow::common