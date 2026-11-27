#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "zeroflow/ipc/slot_descriptor.hpp"

namespace zeroflow::ipc {

// 生成规范化的 Unix Domain Socket 路径: /tmp/zf_<pipeline>_<node>_<port>.sock
[[nodiscard]] std::string make_port_socket_path(const std::string& pipeline_name,
                                                const std::string& node_id,
                                                const std::string& port_name);

enum class RecvStatus {
    kOk,          // 成功读取一条完整的 32B SlotDescriptor
    kWouldBlock,  // 接收队列已排空 (EAGAIN / EWOULDBLOCK)
    kPeerClosed,  // 上游发布者关闭连接或进程退出
    kError,       // 系统调用异常
};

// 发布端端口：监听一个 .sock 路径，支持 1 对 N 扇出广播并自动剔除断开的下游
class UdsPublisherPort {
public:
    explicit UdsPublisherPort(std::string socket_path);
    ~UdsPublisherPort();

    UdsPublisherPort(const UdsPublisherPort&) = delete;
    UdsPublisherPort& operator=(const UdsPublisherPort&) = delete;

    [[nodiscard]] bool bind_and_listen() noexcept;
    void close_all() noexcept;

    // 排空所有待接入的下游订阅连接 (非阻塞 accept4)
    void accept_pending_clients() noexcept;

    // 向所有已连接订阅者广播 32B 描述符；返回成功送达的下游数量
    uint32_t broadcast(const SlotDescriptor& desc) noexcept;

    [[nodiscard]] int listen_fd() const noexcept { return listen_fd_; }
    [[nodiscard]] std::size_t client_count() const noexcept { return client_fds_.size(); }
    [[nodiscard]] const std::string& socket_path() const noexcept { return socket_path_; }

private:
    std::string socket_path_;
    int listen_fd_{-1};
    std::vector<int> client_fds_;
};

// 订阅端端口：连接上游发布的 .sock 路径，配合 epoll 接收 32B SlotDescriptor
class UdsSubscriberPort {
public:
    explicit UdsSubscriberPort(std::string socket_path);
    ~UdsSubscriberPort();

    UdsSubscriberPort(const UdsSubscriberPort&) = delete;
    UdsSubscriberPort& operator=(const UdsSubscriberPort&) = delete;

    // 带重试地连接上游服务端，成功后切换为非阻塞模式以接入 epoll
    [[nodiscard]] bool connect_with_retry(uint32_t timeout_ms = 2000,
                                          uint32_t retry_interval_ms = 10) noexcept;
    void close_fd() noexcept;

    [[nodiscard]] RecvStatus recv_descriptor(SlotDescriptor& out_desc) noexcept;

    [[nodiscard]] int fd() const noexcept { return sock_fd_; }
    [[nodiscard]] const std::string& socket_path() const noexcept { return socket_path_; }

private:
    std::string socket_path_;
    int sock_fd_{-1};
};

}  // namespace zeroflow::ipc