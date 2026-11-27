#pragma once

#include <cstddef>
#include <cstdint>

namespace zeroflow::ipc {

// 高 4 位编码，最多支持 16 种像素或控制消息格式
enum class PixelFormat : uint8_t {
    kUnknown       = 0,
    kBgr24         = 1,
    kRgb24         = 2,
    kNv12          = 3,
    kYuyv          = 4,
    kTensorRaw     = 5,
    kDetectionMeta = 15,
};

constexpr uint16_t pack_height_and_fmt(uint16_t height, PixelFormat fmt) noexcept {
    return static_cast<uint16_t>((height & 0x0FFFu) |
                                 ((static_cast<uint16_t>(fmt) & 0x0Fu) << 12));
}

constexpr uint16_t unpack_height(uint16_t height_and_fmt) noexcept {
    return static_cast<uint16_t>(height_and_fmt & 0x0FFFu);
}

constexpr PixelFormat unpack_pixel_format(uint16_t height_and_fmt) noexcept {
    return static_cast<PixelFormat>((height_and_fmt >> 12) & 0x0Fu);
}

#pragma pack(push, 1)
struct SlotDescriptor {
    uint32_t slot_index{0};      // [0..3]   共享内存槽位下标 (0 ~ N-1)
    uint32_t generation{0};      // [4..7]   槽位代次校验码（防过期描述符误读）
    uint64_t frame_seq{0};       // [8..15]  全局单调递增帧序号
    uint64_t timestamp_us{0};    // [16..23] CLOCK_MONOTONIC 采集时间戳 (us)
    uint32_t payload_bytes{0};   // [24..27] 实际有效负载字节数 (如 1080p BGR24 = 6220800)
    uint16_t width{0};           // [28..29] 图像宽度
    uint16_t height_and_fmt{0};  // [30..31] 低 12 位高度 + 高 4 位 PixelFormat
};
#pragma pack(pop)

static_assert(sizeof(SlotDescriptor) == 32,
              "SlotDescriptor must be strictly 32 bytes for UDS signaling");

}  // namespace zeroflow::ipc