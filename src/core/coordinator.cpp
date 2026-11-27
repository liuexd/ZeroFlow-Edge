#include "zeroflow/core/coordinator.hpp"

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/ipc/uds_channel.hpp"

namespace zeroflow::core {

Coordinator::Coordinator(PipelineConfig config, CoordinatorOptions opts)
    : config_(std::move(config)), opts_(opts) {}

Coordinator::~Coordinator() {
    shutdown_all_children();
    cleanup_files_and_fds();
}

bool Coordinator::prepare_managed_nodes(const DagValidationResult& dag_res) {
    std::unordered_map<std::string, NodeSpec> spec_map;
    for (const auto& n : config_.nodes) {
        spec_map.emplace(n.id, n);
    }

    // "<node_id>/<port_name>" -> initial_subscriber_mask
    std::unordered_map<std::string, uint32_t> mask_map;
    for (const auto& pf : dag_res.port_fanouts) {
        mask_map.emplace(pf.node_id + "/" + pf.port_name, pf.initial_subscriber_mask);
    }

    nodes_.clear();
    nodes_.reserve(dag_res.topo_order.size());

    for (const auto& node_id : dag_res.topo_order) {
        ManagedNode mn;
        mn.spec = spec_map.at(node_id);

        for (const auto& in_bind : mn.spec.inputs) {
            InputEndpointSpec ep;
            ep.local_port = in_bind.local_port;
            ep.socket_path = ipc::make_port_socket_path(
                config_.pipeline_name, in_bind.upstream_node, in_bind.upstream_port);
            ep.subscriber_id = in_bind.subscriber_id;
            mn.resolved_inputs.push_back(std::move(ep));
        }

        for (const auto& out_port : mn.spec.outputs) {
            OutputEndpointSpec ep;
            ep.port_name = out_port;
            ep.socket_path =
                ipc::make_port_socket_path(config_.pipeline_name, mn.spec.id, out_port);
            ep.subscriber_mask = mask_map.at(mn.spec.id + "/" + out_port);
            mn.resolved_outputs.push_back(std::move(ep));
        }

        nodes_.push_back(std::move(mn));
    }

    return true;
}

void Coordinator::wait_for_node_sockets(const ManagedNode& node) const noexcept {
    if (node.resolved_outputs.empty()) {
        return;
    }

    const uint64_t deadline_ns =
        common::now_monotonic_ns() +
        static_cast<uint64_t>(opts_.socket_ready_timeout_ms) * 1000000ULL;

    while (common::now_monotonic_ns() < deadline_ns) {
        bool all_ready = true;
        for (const auto& out_ep : node.resolved_outputs) {
            if (access(out_ep.socket_path.c_str(), F_OK) != 0) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) {
            return;
        }
        usleep(5000);  // 5ms 轮询等待绑定完成
    }
}

bool Coordinator::spawn_node(ManagedNode& node) {
    // 先清理可能残留的同名旧 socket 文件，防止后续误判就绪
    for (const auto& out_ep : node.resolved_outputs) {
        unlink(out_ep.socket_path.c_str());
    }

    std::vector<std::string> arg_storage = build_node_exec_args(
        config_.pipeline_name,
        config_.shm_pool.pool_name,
        node.spec.executable,
        node.spec.id,
        node.spec.args,
        node.resolved_inputs,
        node.resolved_outputs,
        node.spec.params);

    std::vector<char*> argv;
    argv.reserve(arg_storage.size() + 1);
    for (auto& s : arg_storage) {
        argv.push_back(s.data());
    }
    argv.push_back(nullptr);

    const pid_t pid = fork();
    if (pid < 0) {
        std::perror("[Coordinator] fork failed");
        return false;
    }

    if (pid == 0) {
        // 子进程继承了父进程阻塞 SIGINT/SIGTERM/SIGCHLD 的信号掩码，exec 前必须清空恢复
        sigset_t empty_mask;
        sigemptyset(&empty_mask);
        pthread_sigmask(SIG_SETMASK, &empty_mask, nullptr);

        execvp(argv[0], argv.data());
        std::perror("[Coordinator] execvp failed");
        _exit(127);
    }

    node.pid = pid;
    std::printf("[Coordinator] Spawned node '%s' (PID=%d, exec=%s)\n",
                node.spec.id.c_str(), pid, node.spec.executable.c_str());
    std::fflush(stdout);

    wait_for_node_sockets(node);
    return true;
}

void Coordinator::reclaim_orphan_slots(pid_t dead_pid, const ManagedNode& node) noexcept {
    if (!arena_ || arena_->slot_meta_array() == nullptr) {
        return;
    }

    ipc::SlotMeta* metas = arena_->slot_meta_array();
    const uint32_t total_slots = arena_->slot_count();
    uint32_t reclaimed = 0;

    for (uint32_t i = 0; i < total_slots; ++i) {
        ipc::SlotMeta& meta = metas[i];
        const uint32_t st = meta.state.load(std::memory_order_acquire);

        // 1. 若崩溃进程作为生产者正处于 kWriting 写半帧状态，直接强制回收槽位
        if (st == static_cast<uint32_t>(ipc::SlotState::kWriting) &&
            meta.writer_pid.load(std::memory_order_acquire) == static_cast<int32_t>(dead_pid)) {
            meta.writer_pid.store(0, std::memory_order_relaxed);
            meta.lease_timestamp_ns.store(0, std::memory_order_relaxed);
            meta.generation.fetch_add(1, std::memory_order_acq_rel);
            meta.state.store(static_cast<uint32_t>(ipc::SlotState::kFree),
                             std::memory_order_release);
            ++reclaimed;
            continue;
        }

        // 2. 若崩溃进程作为订阅者持有 kReady 槽位，按其 subscriber_id 精准剥离计数，绝不误伤旁路消费者
        if (st == static_cast<uint32_t>(ipc::SlotState::kReady)) {
            for (const auto& in_ep : node.resolved_inputs) {
                const uint32_t bit = (1u << in_ep.subscriber_id);
                const uint32_t prev_mask =
                    meta.subscriber_mask.fetch_and(~bit, std::memory_order_acq_rel);
                if ((prev_mask & bit) != 0) {
                    const uint32_t prev_ref =
                        meta.ref_count.fetch_sub(1, std::memory_order_acq_rel);
                    if (prev_ref == 1) {
                        meta.state.store(static_cast<uint32_t>(ipc::SlotState::kReclaiming),
                                         std::memory_order_acq_rel);
                        meta.writer_pid.store(0, std::memory_order_relaxed);
                        meta.lease_timestamp_ns.store(0, std::memory_order_relaxed);
                        meta.generation.fetch_add(1, std::memory_order_acq_rel);
                        meta.state.store(static_cast<uint32_t>(ipc::SlotState::kFree),
                                         std::memory_order_release);
                    }
                    ++reclaimed;
                }
            }
        }
    }

    if (reclaimed > 0) {
        total_reclaimed_slots_ += reclaimed;
        std::printf("[Coordinator] Reclaimed %u orphan slot ref(s) from dead PID=%d ('%s')\n",
                    reclaimed, dead_pid, node.spec.id.c_str());
        std::fflush(stdout);
    }
}

void Coordinator::handle_sigchld() {
    while (true) {
        int status = 0;
        const pid_t dead_pid = waitpid(-1, &status, WNOHANG);
        if (dead_pid <= 0) {
            break;
        }

        const auto it = pid_to_node_idx_.find(dead_pid);
        if (it == pid_to_node_idx_.end()) {
            continue;
        }

        const std::size_t idx = it->second;
        pid_to_node_idx_.erase(it);
        ManagedNode& node = nodes_[idx];
        node.pid = -1;

        const bool normal_zero_exit = WIFEXITED(status) && (WEXITSTATUS(status) == 0);
        if (WIFSIGNALED(status)) {
            std::printf("[Coordinator] Node '%s' (PID=%d) killed by signal %d\n",
                        node.spec.id.c_str(), dead_pid, WTERMSIG(status));
        } else {
            std::printf("[Coordinator] Node '%s' (PID=%d) exited with code %d\n",
                        node.spec.id.c_str(), dead_pid, WEXITSTATUS(status));
        }
        std::fflush(stdout);

        // 无论是否重启，立即清扫该进程遗留的孤儿共享内存槽位
        reclaim_orphan_slots(dead_pid, node);

        if (stop_requested_.load(std::memory_order_acquire)) {
            continue;
        }

        bool should_restart = false;
        if (node.spec.restart_policy == RestartPolicy::kAlways) {
            should_restart = true;
        } else if (node.spec.restart_policy == RestartPolicy::kOnFailure && !normal_zero_exit) {
            should_restart = true;
        }

        if (should_restart) {
            usleep(50000);  // 50ms 退避防狂刷拉起
            if (spawn_node(node)) {
                pid_to_node_idx_[node.pid] = idx;
                node.restart_count += 1;
                total_restarts_ += 1;
            }
        }
    }
}

void Coordinator::shutdown_all_children() {
    if (nodes_.empty()) {
        return;
    }

    // 1. 按逆拓扑序向所有存活子进程发送 SIGTERM（先停下游消费端，再停上游生产端）
    for (auto it = nodes_.rbegin(); it != nodes_.rend(); ++it) {
        if (it->pid > 0) {
            kill(it->pid, SIGTERM);
        }
    }

    // 2. 在宽限期内非阻塞回收子进程
    const uint64_t deadline_ns =
        common::now_monotonic_ns() +
        static_cast<uint64_t>(opts_.shutdown_grace_ms) * 1000000ULL;

    while (!pid_to_node_idx_.empty() && common::now_monotonic_ns() < deadline_ns) {
        int status = 0;
        const pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid > 0) {
            const auto it = pid_to_node_idx_.find(pid);
            if (it != pid_to_node_idx_.end()) {
                nodes_[it->second].pid = -1;
                pid_to_node_idx_.erase(it);
            }
        } else {
            usleep(10000);
        }
    }

    // 3. 若仍有僵死不退出的进程，升级为 SIGKILL 强制回收，杜绝僵尸进程
    for (auto& node : nodes_) {
        if (node.pid > 0) {
            std::fprintf(stderr, "[Coordinator] Force killing stubborn node '%s' (PID=%d)\n",
                         node.spec.id.c_str(), node.pid);
            kill(node.pid, SIGKILL);
            int status = 0;
            waitpid(node.pid, &status, 0);
            pid_to_node_idx_.erase(node.pid);
            node.pid = -1;
        }
    }
}

void Coordinator::cleanup_files_and_fds() noexcept {
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (duration_timer_fd_ >= 0) {
        close(duration_timer_fd_);
        duration_timer_fd_ = -1;
    }
    if (signal_fd_ >= 0) {
        close(signal_fd_);
        signal_fd_ = -1;
    }

    for (const auto& node : nodes_) {
        for (const auto& out_ep : node.resolved_outputs) {
            unlink(out_ep.socket_path.c_str());
        }
    }

    // 析构 PosixShmArena (kCreator) 自动执行 shm_unlink 释放 /dev/shm 内存
    arena_.reset();
}

std::vector<pid_t> Coordinator::active_child_pids() const {
    std::vector<pid_t> pids;
    pids.reserve(pid_to_node_idx_.size());
    for (const auto& kv : pid_to_node_idx_) {
        pids.push_back(kv.first);
    }
    return pids;
}

int Coordinator::run() {
    // 1. 静态拓扑校验与启动顺序生成
    auto dag_res = validate_and_sort_dag(config_);
    if (!dag_res.ok) {
        std::fprintf(stderr, "[Coordinator] DAG validation failed: %s\n",
                     dag_res.error_msg.c_str());
        return 1;
    }

    if (!prepare_managed_nodes(dag_res)) {
        return 1;
    }

    // 2. 创建共享内存池 (Creator 角色)
    const std::size_t slot_bytes =
        static_cast<std::size_t>(config_.shm_pool.slot_size_mb) * 1024u * 1024u;
    arena_ = std::make_unique<ipc::PosixShmArena>(config_.shm_pool.pool_name,
                                                  ipc::ArenaRole::kCreator,
                                                  config_.shm_pool.slot_count,
                                                  slot_bytes);
    if (!arena_->init()) {
        std::fprintf(stderr, "[Coordinator] Failed to create SHM pool '%s'\n",
                     config_.shm_pool.pool_name.c_str());
        return 2;
    }

    // 3. 初始化 epoll 与 signalfd (统一接管 SIGINT / SIGTERM / SIGCHLD)
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGCHLD);
    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        cleanup_files_and_fds();
        return 3;
    }

    signal_fd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (signal_fd_ < 0 || epoll_fd_ < 0) {
        cleanup_files_and_fds();
        return 3;
    }

    struct epoll_event sig_ev {};
    sig_ev.events = EPOLLIN;
    sig_ev.data.fd = signal_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, signal_fd_, &sig_ev) != 0) {
        cleanup_files_and_fds();
        return 3;
    }

    if (opts_.max_run_duration_ms > 0) {
        duration_timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (duration_timer_fd_ < 0) {
            cleanup_files_and_fds();
            return 3;
        }
        struct itimerspec its {};
        its.it_value.tv_sec = static_cast<time_t>(opts_.max_run_duration_ms / 1000u);
        its.it_value.tv_nsec =
            static_cast<long>((opts_.max_run_duration_ms % 1000u) * 1000000L);
        if (timerfd_settime(duration_timer_fd_, 0, &its, nullptr) != 0) {
            cleanup_files_and_fds();
            return 3;
        }
        struct epoll_event tmr_ev {};
        tmr_ev.events = EPOLLIN;
        tmr_ev.data.fd = duration_timer_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, duration_timer_fd_, &tmr_ev) != 0) {
            cleanup_files_and_fds();
            return 3;
        }
    }

    // 4. 按拓扑顺序依次拉起所有算子进程
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        if (!spawn_node(nodes_[i])) {
            stop_requested_.store(true, std::memory_order_release);
            shutdown_all_children();
            cleanup_files_and_fds();
            return 4;
        }
        pid_to_node_idx_[nodes_[i].pid] = i;
    }

    std::printf("[Coordinator] Pipeline '%s' running with %zu worker processes.\n",
                config_.pipeline_name.c_str(), nodes_.size());
    std::fflush(stdout);

    // 5. 守护进程主事件循环
    constexpr int kMaxEvents = 8;
    struct epoll_event events[kMaxEvents];

    while (!stop_requested_.load(std::memory_order_acquire)) {
        // 若所有子进程均已退出且不再重启，自动结束守护循环
        if (pid_to_node_idx_.empty()) {
            break;
        }

        const int nfds = epoll_wait(epoll_fd_, events, kMaxEvents, 100);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            const int fd = events[i].data.fd;
            if (fd == signal_fd_) {
                struct signalfd_siginfo fdsi {};
                while (read(signal_fd_, &fdsi, sizeof(fdsi)) == sizeof(fdsi)) {
                    if (fdsi.ssi_signo == SIGCHLD) {
                        handle_sigchld();
                    } else if (fdsi.ssi_signo == SIGINT || fdsi.ssi_signo == SIGTERM) {
                        std::printf("\n[Coordinator] Received signal %u, shutting down pipeline...\n",
                                    fdsi.ssi_signo);
                        std::fflush(stdout);
                        stop_requested_.store(true, std::memory_order_release);
                    }
                }
            } else if (fd == duration_timer_fd_) {
                uint64_t exp = 0;
                while (read(duration_timer_fd_, &exp, sizeof(exp)) == sizeof(exp)) {
                }
                std::printf("[Coordinator] Max duration reached, initiating graceful shutdown...\n");
                std::fflush(stdout);
                stop_requested_.store(true, std::memory_order_release);
            }
        }
    }

    shutdown_all_children();
    cleanup_files_and_fds();
    std::printf("[Coordinator] Shutdown complete. All SHM and UDS resources cleaned.\n");
    std::fflush(stdout);
    return 0;
}

}  // namespace zeroflow::core