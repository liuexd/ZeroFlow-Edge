#include <csignal>
#include <cstring>
#include <gtest/gtest.h>
#include <unistd.h>
#include <vector>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/ipc/shm_arena.hpp"

using namespace zeroflow::ipc;

namespace {
std::string make_unique_pool_name(const char* tag) {
    return std::string("zf_ut_") + tag + "_" + std::to_string(getpid());
}
}  // namespace

TEST(ShmAllocatorTest, OneWriterThreeReadersAndIdempotentRelease) {
    const std::string pool = make_unique_pool_name("fanout3");
    PosixShmArena creator(pool, ArenaRole::kCreator, 4, 4096);
    ASSERT_TRUE(creator.init());

    PosixShmArena producer(pool, ArenaRole::kProducer);
    PosixShmArena consumer0(pool, ArenaRole::kConsumer);
    PosixShmArena consumer1(pool, ArenaRole::kConsumer);
    PosixShmArena consumer2(pool, ArenaRole::kConsumer);
    ASSERT_TRUE(producer.init());
    ASSERT_TRUE(consumer0.init());
    ASSERT_TRUE(consumer1.init());
    ASSERT_TRUE(consumer2.init());

    auto wslot = producer.allocate_slot();
    ASSERT_TRUE(wslot.has_value());
    const uint32_t slot_idx = wslot->slot_index;

    const char msg[] = "ZeroFlow-ARM64-ZeroCopy-Frame";
    std::memcpy(wslot->data, msg, sizeof(msg));

    // 一写三读：mask = 0b0111 (sub_id = 0, 1, 2)
    auto desc = producer.commit_slot(slot_idx,
                                     sizeof(msg),
                                     0x07u,
                                     1,
                                     zeroflow::common::now_monotonic_us(),
                                     1920,
                                     1080,
                                     PixelFormat::kBgr24);
    ASSERT_TRUE(desc.has_value());
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].ref_count.load(), 3u);

    auto r0 = consumer0.acquire_slot(*desc, 0);
    auto r1 = consumer1.acquire_slot(*desc, 1);
    auto r2 = consumer2.acquire_slot(*desc, 2);
    ASSERT_TRUE(r0.has_value());
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());
    EXPECT_STREQ(reinterpret_cast<const char*>(r0->data()), msg);

    // 消费者 0 释放并故意重复裸调一次 release_slot，验证幂等性不误扣他人计数
    r0->reset();
    consumer0.release_slot(slot_idx, 0);
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].ref_count.load(), 2u);
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].state.load(),
              static_cast<uint32_t>(SlotState::kReady));

    // 消费者 1 释放后仍剩 1 个消费者
    r1->reset();
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].ref_count.load(), 1u);

    // 最后一个消费者 2 释放，计数归零，代次自增为 1，状态回到 kFree
    r2->reset();
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].ref_count.load(), 0u);
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].generation.load(), 1u);
    EXPECT_EQ(creator.slot_meta_array()[slot_idx].state.load(),
              static_cast<uint32_t>(SlotState::kFree));
}

TEST(ShmAllocatorTest, PoolExhaustionReturnsNulloptUntilReleased) {
    const std::string pool = make_unique_pool_name("exhaust");
    PosixShmArena creator(pool, ArenaRole::kCreator, 2, 4096);
    ASSERT_TRUE(creator.init());

    PosixShmArena producer(pool, ArenaRole::kProducer);
    PosixShmArena consumer(pool, ArenaRole::kConsumer);
    ASSERT_TRUE(producer.init());
    ASSERT_TRUE(consumer.init());

    auto s0 = producer.allocate_slot();
    auto s1 = producer.allocate_slot();
    ASSERT_TRUE(s0.has_value());
    ASSERT_TRUE(s1.has_value());

    // 第 3 次申请应立即返回 nullopt（非阻塞背压）
    EXPECT_FALSE(producer.allocate_slot().has_value());

    auto d0 = producer.commit_slot(
        s0->slot_index, 64, 0x01u, 1, 100, 64, 1, PixelFormat::kBgr24);
    ASSERT_TRUE(d0.has_value());

    {
        auto r0 = consumer.acquire_slot(*d0, 0);
        ASSERT_TRUE(r0.has_value());
    }  // 离开作用域 RAII 自动归还槽位

    // 槽位回收后可再次成功申请
    auto s2 = producer.allocate_slot();
    EXPECT_TRUE(s2.has_value());
}

TEST(ShmAllocatorTest, GenerationMismatchBlocksStaleDescriptor) {
    const std::string pool = make_unique_pool_name("aba");
    PosixShmArena creator(pool, ArenaRole::kCreator, 1, 4096);
    ASSERT_TRUE(creator.init());

    PosixShmArena producer(pool, ArenaRole::kProducer);
    PosixShmArena consumer(pool, ArenaRole::kConsumer);
    ASSERT_TRUE(producer.init());
    ASSERT_TRUE(consumer.init());

    // 第一轮写入并消费释放，槽位 generation 变为 1
    auto s1 = producer.allocate_slot();
    ASSERT_TRUE(s1.has_value());
    auto stale_desc = producer.commit_slot(
        s1->slot_index, 32, 0x01u, 1, 100, 32, 1, PixelFormat::kBgr24);
    ASSERT_TRUE(stale_desc.has_value());
    {
        auto r = consumer.acquire_slot(*stale_desc, 0);
        ASSERT_TRUE(r.has_value());
    }

    // 第二轮在同一槽位写入新帧 (generation == 1)
    auto s2 = producer.allocate_slot();
    ASSERT_TRUE(s2.has_value());
    auto fresh_desc = producer.commit_slot(
        s2->slot_index, 32, 0x01u, 2, 200, 32, 1, PixelFormat::kBgr24);
    ASSERT_TRUE(fresh_desc.has_value());
    EXPECT_EQ(fresh_desc->generation, 1u);

    // 尝试用第一轮过期描述符获取槽位，必须被 generation 校验拦截
    EXPECT_FALSE(consumer.acquire_slot(*stale_desc, 0).has_value());

    auto valid_r = consumer.acquire_slot(*fresh_desc, 0);
    EXPECT_TRUE(valid_r.has_value());
}

TEST(ShmAllocatorTest, ConsumerWriteTriggersHardwareSigsegv) {
    const std::string pool = make_unique_pool_name("ro_prot");
    PosixShmArena creator(pool, ArenaRole::kCreator, 2, 4096);
    ASSERT_TRUE(creator.init());

    PosixShmArena producer(pool, ArenaRole::kProducer);
    ASSERT_TRUE(producer.init());

    auto wslot = producer.allocate_slot();
    ASSERT_TRUE(wslot.has_value());
    wslot->data[0] = 0xAA;
    auto desc = producer.commit_slot(
        wslot->slot_index, 16, 0x01u, 1, 100, 16, 1, PixelFormat::kBgr24);
    ASSERT_TRUE(desc.has_value());

    // 死亡测试：验证 kConsumer 的数据区为硬件级 PROT_READ，强行写入必触发 SIGSEGV
    EXPECT_DEATH(
        {
            PosixShmArena bad_consumer(pool, ArenaRole::kConsumer);
            if (!bad_consumer.init()) {
                _exit(0);
            }
            auto rslot = bad_consumer.acquire_slot(*desc, 0);
            if (!rslot.has_value()) {
                _exit(0);
            }
            auto* mutable_ptr = const_cast<uint8_t*>(rslot->data());
            mutable_ptr[0] = 0x55;  // 触发 CPU MMU 段错误
            _exit(0);
        },
        "");
}