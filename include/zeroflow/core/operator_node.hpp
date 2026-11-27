#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "zeroflow/ipc/shm_arena.hpp"
#include "zeroflow/ipc/uds_channel.hpp"

namespace zeroflow::core {

struct InputEndpointSpec {
    std::string local_port;
    std::string socket_path;
    uint32_t subscriber_id{0};
};

struct OutputEndpointSpec {
    std::string port_name;
    std::string socket_path;
    uint32_t subscriber_mask{0};
};

struct NodeRuntimeConfig {
    std::string pipeline_name;
    std::string pool_name;
    std::string node_id;
    uint32_t tick_interval_ms{0};  // >0 时启用 timerfd 周期触发 on_timer()
    uint32_t connect_timeout_ms{2000};
    std::vector<InputEndpointSpec> inputs;
    std::vector<OutputEndpointSpec> outputs;
};

class OperatorNode {
public:
    explicit OperatorNode(NodeRuntimeConfig config);
    virtual ~OperatorNode();

    OperatorNode(const OperatorNode&) = delete;
    OperatorNode& operator=(const OperatorNode&) = delete;

    // 启动算子主循环（阻塞直至收到 SIGINT/SIGTERM 或调用 request_stop()）
    [[nodiscard]] int run();

    void request_stop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
    }

protected:
    // 子类可重写的生命周期与事件回调
    [[nodiscard]] virtual bool on_init() { return true; }

    virtual void on_input(const std::string& /*local_port*/,
                          const ipc::SlotDescriptor& /*desc*/,
                          ipc::ReadOnlySlot /*slot*/) {}

    virtual void on_timer() {}

    virtual void on_shutdown() {}

    // 供子类调用的零拷贝生产辅助接口
    [[nodiscard]] std::optional<ipc::WritableSlot> allocate_output_slot() noexcept;

    [[nodiscard]] bool publish_output(const std::string& output_port,
                                      const ipc::WritableSlot& slot,
                                      uint32_t payload_bytes,
                                      uint64_t frame_seq,
                                      uint64_t timestamp_us,
                                      uint16_t width,
                                      uint16_t height,
                                      ipc::PixelFormat fmt) noexcept;

    [[nodiscard]] const NodeRuntimeConfig& config() const noexcept { return config_; }

private:
    struct InputBindingRuntime {
        InputEndpointSpec spec;
        std::unique_ptr<ipc::UdsSubscriberPort> sub_port;
    };

    struct OutputBindingRuntime {
        OutputEndpointSpec spec;
        std::unique_ptr<ipc::UdsPublisherPort> pub_port;
    };

    bool setup_signal_fd() noexcept;
    bool setup_timer_fd() noexcept;
    void cleanup_runtime() noexcept;

    NodeRuntimeConfig config_;
    std::atomic<bool> stop_requested_{false};

    std::unique_ptr<ipc::PosixShmArena> arena_;
    std::vector<InputBindingRuntime> input_runtimes_;
    std::unordered_map<int, std::size_t> fd_to_input_idx_;
    std::unordered_map<std::string, OutputBindingRuntime> output_runtimes_;

    int epoll_fd_{-1};
    int signal_fd_{-1};
    int timer_fd_{-1};
};

}  // namespace zeroflow::core