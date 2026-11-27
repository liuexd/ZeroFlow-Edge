#include <atomic>
#include <csignal>
#include <cstring>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/core/operator_node.hpp"
#include "zeroflow/ipc/shm_arena.hpp"
#include "zeroflow/ipc/uds_channel.hpp"

using namespace zeroflow::core;
using namespace zeroflow::ipc;

namespace {

class DummyProducerNode final : public OperatorNode {
public:
    explicit DummyProducerNode(NodeRuntimeConfig cfg) : OperatorNode(std::move(cfg)) {}

    uint64_t published_count() const noexcept { return published_; }

protected:
    void on_timer() override {
        auto slot = allocate_output_slot();
        if (!slot.has_value()) {
            return;
        }
        ++seq_;
        const uint64_t magic_payload = 0xCAFEBABE00000000ULL | seq_;
        std::memcpy(slot->data, &magic_payload, sizeof(magic_payload));

        if (publish_output("raw_frame",
                           *slot,
                           sizeof(magic_payload),
                           seq_,
                           zeroflow::common::now_monotonic_us(),
                           64,
                           1,
                           PixelFormat::kBgr24)) {
            ++published_;
        }
    }

private:
    uint64_t seq_{0};
    uint64_t published_{0};
};

class DummyConsumerNode final : public OperatorNode {
public:
    DummyConsumerNode(NodeRuntimeConfig cfg, uint64_t target_frames)
        : OperatorNode(std::move(cfg)), target_frames_(target_frames) {}

    uint64_t valid_frames() const noexcept { return valid_frames_; }

protected:
    void on_input(const std::string& local_port,
                  const SlotDescriptor& desc,
                  ReadOnlySlot slot) override {
        if (local_port != "in_frame" || slot.size() != sizeof(uint64_t)) {
            return;
        }
        uint64_t payload = 0;
        std::memcpy(&payload, slot.data(), sizeof(payload));
        if (payload == (0xCAFEBABE00000000ULL | desc.frame_seq)) {
            ++valid_frames_;
        }
        if (valid_frames_ >= target_frames_) {
            request_stop();
        }
    }

private:
    uint64_t target_frames_{0};
    uint64_t valid_frames_{0};
};

}  // namespace

TEST(UdsNodeTest, SeqpacketBoundaryAndFanout) {
    const std::string sock_path =
        make_port_socket_path("ut_" + std::to_string(getpid()), "pub", "out");

    UdsPublisherPort pub(sock_path);
    ASSERT_TRUE(pub.bind_and_listen());

    UdsSubscriberPort sub0(sock_path);
    UdsSubscriberPort sub1(sock_path);
    ASSERT_TRUE(sub0.connect_with_retry(500, 5));
    ASSERT_TRUE(sub1.connect_with_retry(500, 5));

    // 连续发送两条 32 字节报文，验证 SOCK_SEQPACKET 不会粘包合并
    SlotDescriptor d1{};
    d1.slot_index = 1;
    d1.frame_seq = 101;
    SlotDescriptor d2{};
    d2.slot_index = 2;
    d2.frame_seq = 102;

    EXPECT_EQ(pub.broadcast(d1), 2u);
    EXPECT_EQ(pub.broadcast(d2), 2u);
    EXPECT_EQ(pub.client_count(), 2u);

    SlotDescriptor rx1{};
    SlotDescriptor rx2{};
    ASSERT_EQ(sub0.recv_descriptor(rx1), RecvStatus::kOk);
    ASSERT_EQ(sub0.recv_descriptor(rx2), RecvStatus::kOk);
    EXPECT_EQ(rx1.frame_seq, 101u);
    EXPECT_EQ(rx2.frame_seq, 102u);
    EXPECT_EQ(sub0.recv_descriptor(rx1), RecvStatus::kWouldBlock);

    // 断开 sub1 后再发一帧，发布端应自动剔除失效连接
    sub1.close_fd();
    EXPECT_EQ(pub.broadcast(d1), 1u);
    EXPECT_EQ(pub.client_count(), 1u);
}

TEST(UdsNodeTest, MultiProcessOperatorNodeEndToEnd) {
    const std::string pid_tag = std::to_string(getpid());
    const std::string pool_name = "zf_day4_pool_" + pid_tag;
    const std::string sock_path = make_port_socket_path("day4_" + pid_tag, "cap", "raw_frame");

    PosixShmArena creator(pool_name, ArenaRole::kCreator, 8, 4096);
    ASSERT_TRUE(creator.init());

    // 1. 拉起独立生产者进程 (timerfd 5ms 周期发帧)
    const pid_t prod_pid = fork();
    ASSERT_GE(prod_pid, 0);
    if (prod_pid == 0) {
        int rc = 1;
        {
            NodeRuntimeConfig cfg;
            cfg.pipeline_name = "day4_" + pid_tag;
            cfg.pool_name = pool_name;
            cfg.node_id = "capture_node";
            cfg.tick_interval_ms = 5;
            cfg.outputs.push_back({"raw_frame", sock_path, 0x01u});

            DummyProducerNode node(std::move(cfg));
            rc = node.run();
        }
        _exit(rc);
    }

    // 2. 拉起独立消费者进程 (收满 10 帧后通过 request_stop 退出)
    const pid_t cons_pid = fork();
    ASSERT_GE(cons_pid, 0);
    if (cons_pid == 0) {
        int rc = 1;
        {
            NodeRuntimeConfig cfg;
            cfg.pipeline_name = "day4_" + pid_tag;
            cfg.pool_name = pool_name;
            cfg.node_id = "consumer_node";
            cfg.connect_timeout_ms = 2000;
            cfg.inputs.push_back({"in_frame", sock_path, 0});

            DummyConsumerNode node(std::move(cfg), 10);
            rc = node.run();
        }
        _exit(rc);
    }

    // 等待消费者收满 10 帧并正常退出
    int cons_status = 0;
    ASSERT_EQ(waitpid(cons_pid, &cons_status, 0), cons_pid);
    ASSERT_TRUE(WIFEXITED(cons_status));
    EXPECT_EQ(WEXITSTATUS(cons_status), 0);

    // 向生产者发送 SIGTERM 验证 signalfd 优雅停机
    ASSERT_EQ(kill(prod_pid, SIGTERM), 0);
    int prod_status = 0;
    ASSERT_EQ(waitpid(prod_pid, &prod_status, 0), prod_pid);
    ASSERT_TRUE(WIFEXITED(prod_status));
    EXPECT_EQ(WEXITSTATUS(prod_status), 0);

    // 核查共享内存池零槽位泄漏
    for (uint32_t i = 0; i < creator.slot_count(); ++i) {
        EXPECT_EQ(creator.slot_meta_array()[i].state.load(),
                  static_cast<uint32_t>(SlotState::kFree));
        EXPECT_EQ(creator.slot_meta_array()[i].ref_count.load(), 0u);
    }
}