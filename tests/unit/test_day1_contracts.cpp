#include <ctime>
#include <gtest/gtest.h>

#include "zeroflow/ipc/shm_arena.hpp"
#include "zeroflow/ipc/slot_descriptor.hpp"

using namespace zeroflow::ipc;

TEST(Day1ContractsTest, SlotDescriptorPackingAndSize) {
    SlotDescriptor desc{};
    desc.slot_index = 7;
    desc.generation = 42;
    desc.frame_seq = 10001;
    desc.timestamp_us = 123456789ULL;
    desc.payload_bytes = 1920u * 1080u * 3u;
    desc.width = 1920;
    desc.height_and_fmt = pack_height_and_fmt(1080, PixelFormat::kBgr24);

    EXPECT_EQ(sizeof(desc), 32u);
    EXPECT_EQ(unpack_height(desc.height_and_fmt), 1080u);
    EXPECT_EQ(unpack_pixel_format(desc.height_and_fmt), PixelFormat::kBgr24);
}

TEST(Day1ContractsTest, CacheLineAlignmentAndLockFree) {
    SlotMeta meta{};
    EXPECT_EQ(sizeof(meta), 64u);
    EXPECT_EQ(alignof(SlotMeta), 64u);
    EXPECT_TRUE(meta.state.is_lock_free());
    EXPECT_TRUE(meta.ref_count.is_lock_free());
    EXPECT_TRUE(meta.lease_timestamp_ns.is_lock_free());
}

TEST(Day1ContractsTest, MonotonicRawClockWorksOnArm64) {
    struct timespec ts {};
    ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC_RAW, &ts), 0);
    EXPECT_GT(ts.tv_sec, 0);
}