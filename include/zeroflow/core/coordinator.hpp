#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

#include "zeroflow/core/dag_validator.hpp"
#include "zeroflow/core/node_cli.hpp"
#include "zeroflow/core/pipeline_config.hpp"
#include "zeroflow/ipc/shm_arena.hpp"

namespace zeroflow::core {

struct CoordinatorOptions {
    uint32_t max_run_duration_ms{0};       // >0 时到达时长后自动触发优雅停机
    uint32_t shutdown_grace_ms{1500};      // SIGTERM 优雅(grace)退出等待上限，超时转 SIGKILL
    uint32_t socket_ready_timeout_ms{800}; // 等待上游 .sock 绑定的最长时间
};

//总控进程/协调器：
//它自己不亲自处理任何视频帧或业务数据，
//它的唯一使命就是拿着全局图纸，把共享内存建好、把所有算子子进程按正确顺序拉起来、盯住它们的死活，并在结束时把所有资源清理干净。
class Coordinator {
public:
    Coordinator(PipelineConfig config, CoordinatorOptions opts = {});
    ~Coordinator();

    Coordinator(const Coordinator&) = delete; //禁止拷贝构造也就是：Coordinator b;Coordinator a(b);
    Coordinator& operator=(const Coordinator&) = delete; //禁止拷贝赋值

    // 启动流水线并进入守护循环，返回 0 表示正常停机退出
    [[nodiscard]] int run();

    void request_stop() noexcept {
        stop_requested_.store(true, std::memory_order_release);
    }

    [[nodiscard]] std::vector<pid_t> active_child_pids() const;
    [[nodiscard]] uint64_t total_restarts() const noexcept { return total_restarts_; }
    [[nodiscard]] uint64_t total_reclaimed_slots() const noexcept { return total_reclaimed_slots_; }

private:
    struct ManagedNode {
        NodeSpec spec;
        std::vector<InputEndpointSpec> resolved_inputs;
        std::vector<OutputEndpointSpec> resolved_outputs;
        pid_t pid{-1};
        uint32_t restart_count{0};
    };

    bool prepare_managed_nodes(const DagValidationResult& dag_res);
    bool spawn_node(ManagedNode& node);
    void wait_for_node_sockets(const ManagedNode& node) const noexcept;
    void handle_sigchld();
    void reclaim_orphan_slots(pid_t dead_pid, const ManagedNode& node) noexcept;
    void shutdown_all_children();
    void cleanup_files_and_fds() noexcept;

    PipelineConfig config_;
    CoordinatorOptions opts_;
    std::atomic<bool> stop_requested_{false};

    std::unique_ptr<ipc::PosixShmArena> arena_;
    std::vector<ManagedNode> nodes_;  // 严格按 topo_order 排列
    std::unordered_map<pid_t, std::size_t> pid_to_node_idx_;

    int epoll_fd_{-1};
    int signal_fd_{-1};
    int duration_timer_fd_{-1};

    uint64_t total_restarts_{0};
    uint64_t total_reclaimed_slots_{0};
};

}  // namespace zeroflow::core