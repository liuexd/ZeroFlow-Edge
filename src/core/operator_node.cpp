#include "zeroflow/core/operator_node.hpp"

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>

namespace zeroflow::core {

OperatorNode::OperatorNode(NodeRuntimeConfig config)
    : config_(std::move(config)) {}

OperatorNode::~OperatorNode() {
    cleanup_runtime();
}

bool OperatorNode::setup_signal_fd() noexcept {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);

    if (pthread_sigmask(SIG_BLOCK, &mask, nullptr) != 0) {
        return false;
    }

    signal_fd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    return signal_fd_ >= 0;
}

bool OperatorNode::setup_timer_fd() noexcept {
    if (config_.tick_interval_ms == 0) {
        return true;
    }

    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ < 0) {
        return false;
    }

    struct itimerspec its {};
    const time_t sec = static_cast<time_t>(config_.tick_interval_ms / 1000u);
    const long nsec = static_cast<long>((config_.tick_interval_ms % 1000u) * 1000000L);
    its.it_interval.tv_sec = sec;
    its.it_interval.tv_nsec = nsec;
    its.it_value.tv_sec = sec;
    its.it_value.tv_nsec = nsec;

    if (timerfd_settime(timer_fd_, 0, &its, nullptr) != 0) {
        close(timer_fd_);
        timer_fd_ = -1;
        return false;
    }
    return true;
}

void OperatorNode::cleanup_runtime() noexcept {
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (timer_fd_ >= 0) {
        close(timer_fd_);
        timer_fd_ = -1;
    }
    if (signal_fd_ >= 0) {
        close(signal_fd_);
        signal_fd_ = -1;
    }
    fd_to_input_idx_.clear();
    input_runtimes_.clear();
    output_runtimes_.clear();
    arena_.reset();
}

std::optional<ipc::WritableSlot> OperatorNode::allocate_output_slot() noexcept {
    if (!arena_) {
        return std::nullopt;
    }
    return arena_->allocate_slot();
}

bool OperatorNode::publish_output(const std::string& output_port,
                                  const ipc::WritableSlot& slot,
                                  uint32_t payload_bytes,
                                  uint64_t frame_seq,
                                  uint64_t timestamp_us,
                                  uint16_t width,
                                  uint16_t height,
                                  ipc::PixelFormat fmt) noexcept {
    if (!arena_) {
        return false;
    }
    const auto it = output_runtimes_.find(output_port);
    if (it == output_runtimes_.end()) {
        return false;
    }

    auto desc = arena_->commit_slot(slot.slot_index,
                                    payload_bytes,
                                    it->second.spec.subscriber_mask,
                                    frame_seq,
                                    timestamp_us,
                                    width,
                                    height,
                                    fmt);
    if (!desc.has_value()) {
        return false;
    }

    it->second.pub_port->broadcast(*desc);
    return true;
}

int OperatorNode::run() {
    // 1. 挂载共享内存池：纯消费节点强制启用 PROT_READ 硬件隔离
    const ipc::ArenaRole role =
        config_.outputs.empty() ? ipc::ArenaRole::kConsumer : ipc::ArenaRole::kProducer;
    arena_ = std::make_unique<ipc::PosixShmArena>(config_.pool_name, role);
    if (!arena_->init()) {
        std::fprintf(stderr, "[%s] Failed to attach SHM pool '%s'\n",
                     config_.node_id.c_str(), config_.pool_name.c_str());
        return 1;
    }

    // 2. 创建 epoll 实例与统一信号/定时器 fd
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0 || !setup_signal_fd() || !setup_timer_fd()) {
        std::fprintf(stderr, "[%s] Failed to init epoll/signalfd/timerfd\n",
                     config_.node_id.c_str());
        cleanup_runtime();
        return 2;
    }

    auto add_epoll_fd = [this](int fd, uint32_t events) -> bool {
        struct epoll_event ev {};
        ev.events = events;
        ev.data.fd = fd;
        return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
    };

    if (!add_epoll_fd(signal_fd_, EPOLLIN)) {
        cleanup_runtime();
        return 2;
    }
    if (timer_fd_ >= 0 && !add_epoll_fd(timer_fd_, EPOLLIN)) {
        cleanup_runtime();
        return 2;
    }

    // 3. 绑定所有输出端口并监听下游订阅连接
    for (const auto& out_spec : config_.outputs) {
        OutputBindingRuntime rt;
        rt.spec = out_spec;
        rt.pub_port = std::make_unique<ipc::UdsPublisherPort>(out_spec.socket_path);
        if (!rt.pub_port->bind_and_listen()) {
            std::fprintf(stderr, "[%s] Failed to bind output socket '%s'\n",
                         config_.node_id.c_str(), out_spec.socket_path.c_str());
            cleanup_runtime();
            return 3;
        }
        if (!add_epoll_fd(rt.pub_port->listen_fd(), EPOLLIN)) {
            cleanup_runtime();
            return 3;
        }
        output_runtimes_.emplace(out_spec.port_name, std::move(rt));
    }

    // 4. 连接所有上游发布端并将数据 socket 注册入 epoll
    for (const auto& in_spec : config_.inputs) {
        InputBindingRuntime rt;
        rt.spec = in_spec;
        rt.sub_port = std::make_unique<ipc::UdsSubscriberPort>(in_spec.socket_path);
        if (!rt.sub_port->connect_with_retry(config_.connect_timeout_ms, 10)) {
            std::fprintf(stderr, "[%s] Timeout connecting upstream '%s'\n",
                         config_.node_id.c_str(), in_spec.socket_path.c_str());
            cleanup_runtime();
            return 4;
        }
        const int sfd = rt.sub_port->fd();
        if (!add_epoll_fd(sfd, EPOLLIN | EPOLLRDHUP)) {
            cleanup_runtime();
            return 4;
        }
        fd_to_input_idx_[sfd] = input_runtimes_.size();
        input_runtimes_.push_back(std::move(rt));
    }

    // 5. 执行业务子类初始化钩子
    if (!on_init()) {
        std::fprintf(stderr, "[%s] on_init() failed\n", config_.node_id.c_str());
        cleanup_runtime();
        return 5;
    }

    // 6. 主事件循环
    constexpr int kMaxEvents = 16;
    struct epoll_event events[kMaxEvents];

    while (!stop_requested_.load(std::memory_order_acquire)) {
        const int nfds = epoll_wait(epoll_fd_, events, kMaxEvents, 100);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            const int ready_fd = events[i].data.fd;

            if (ready_fd == signal_fd_) {
                struct signalfd_siginfo fdsi {};
                while (read(signal_fd_, &fdsi, sizeof(fdsi)) == sizeof(fdsi)) {
                }
                stop_requested_.store(true, std::memory_order_release);
                break;
            }

            if (ready_fd == timer_fd_) {
                uint64_t expirations = 0;
                while (read(timer_fd_, &expirations, sizeof(expirations)) == sizeof(expirations)) {
                }
                on_timer();
                continue;
            }

            const auto in_it = fd_to_input_idx_.find(ready_fd);
            if (in_it != fd_to_input_idx_.end()) {
                auto& in_rt = input_runtimes_[in_it->second];
                while (!stop_requested_.load(std::memory_order_acquire)) {
                    ipc::SlotDescriptor desc{};
                    const auto st = in_rt.sub_port->recv_descriptor(desc);
                    if (st == ipc::RecvStatus::kOk) {
                        auto slot = arena_->acquire_slot(desc, in_rt.spec.subscriber_id);
                        if (slot.has_value()) {
                            // 离开 on_input 作用域时 ReadOnlySlot 自动归还引用计数
                            on_input(in_rt.spec.local_port, desc, std::move(*slot));
                        }
                    } else if (st == ipc::RecvStatus::kWouldBlock) {
                        break;
                    } else {
                        // 上游关闭或异常，摘除该 fd 防止 epoll 水平触发死循环
                        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ready_fd, nullptr);
                        fd_to_input_idx_.erase(ready_fd);
                        in_rt.sub_port->close_fd();
                        break;
                    }
                }
                continue;
            }

            // 检查是否为输出端口有新下游接入
            for (auto& kv : output_runtimes_) {
                if (kv.second.pub_port->listen_fd() == ready_fd) {
                    kv.second.pub_port->accept_pending_clients();
                    break;
                }
            }
        }
    }

    on_shutdown();
    cleanup_runtime();
    return 0;
}

}  // namespace zeroflow::core