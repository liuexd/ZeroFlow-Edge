#include "zeroflow/ipc/uds_channel.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <utility>

namespace zeroflow::ipc {
namespace {

bool fill_sockaddr_un(const std::string& path, struct sockaddr_un& addr) noexcept {
    if (path.empty() || path.size() >= sizeof(addr.sun_path)) {
        return false;
    }
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
    return true;
}

bool set_nonblocking(int fd) noexcept {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

std::string make_port_socket_path(const std::string& pipeline_name,
                                  const std::string& node_id,
                                  const std::string& port_name) {
    return "/tmp/zf_" + pipeline_name + "_" + node_id + "_" + port_name + ".sock";
}

UdsPublisherPort::UdsPublisherPort(std::string socket_path)
    : socket_path_(std::move(socket_path)) {}

UdsPublisherPort::~UdsPublisherPort() {
    close_all();
}

bool UdsPublisherPort::bind_and_listen() noexcept {
    if (listen_fd_ >= 0) {
        return true;
    }

    struct sockaddr_un addr {};
    if (!fill_sockaddr_un(socket_path_, addr)) {
        return false;
    }

    const int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return false;
    }

    // 清理上次异常退出可能残留的同名 socket 文件
    unlink(socket_path_.c_str());

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return false;
    }

    if (listen(fd, 32) != 0) {
        close(fd);
        unlink(socket_path_.c_str());
        return false;
    }

    listen_fd_ = fd;
    return true;
}

void UdsPublisherPort::close_all() noexcept {
    for (int cfd : client_fds_) {
        if (cfd >= 0) {
            close(cfd);
        }
    }
    client_fds_.clear();

    if (listen_fd_ >= 0) {
        close(listen_fd_);
        listen_fd_ = -1;
        if (!socket_path_.empty()) {
            unlink(socket_path_.c_str());
        }
    }
}

void UdsPublisherPort::accept_pending_clients() noexcept {
    if (listen_fd_ < 0) {
        return;
    }

    while (true) {
        const int cfd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
            if (errno == EINTR) {
                continue;
            }
            // EAGAIN / EWOULDBLOCK 表示当前已无挂起连接
            break;
        }
        client_fds_.push_back(cfd);
    }
}

uint32_t UdsPublisherPort::broadcast(const SlotDescriptor& desc) noexcept {
    accept_pending_clients();

    uint32_t delivered = 0;
    std::size_t idx = 0;
    while (idx < client_fds_.size()) {
        const int cfd = client_fds_[idx];
        const ssize_t sent =
            send(cfd, &desc, sizeof(desc), MSG_NOSIGNAL | MSG_DONTWAIT);
        if (sent == static_cast<ssize_t>(sizeof(desc))) {
            ++delivered;
            ++idx;
        } else if (sent < 0 && errno == EINTR) {
            continue;
        } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // 单路下游发送缓冲区满，保留连接但跳过本帧
            ++idx;
        } else {
            // 下游已断开 (EPIPE / ECONNRESET)，安全关闭并从活跃列表剔除
            close(cfd);
            client_fds_[idx] = client_fds_.back();
            client_fds_.pop_back();
        }
    }
    return delivered;
}

UdsSubscriberPort::UdsSubscriberPort(std::string socket_path)
    : socket_path_(std::move(socket_path)) {}

UdsSubscriberPort::~UdsSubscriberPort() {
    close_fd();
}

bool UdsSubscriberPort::connect_with_retry(uint32_t timeout_ms,
                                           uint32_t retry_interval_ms) noexcept {
    if (sock_fd_ >= 0) {
        return true;
    }

    struct sockaddr_un addr {};
    if (!fill_sockaddr_un(socket_path_, addr)) {
        return false;
    }

    uint32_t waited_ms = 0;
    while (true) {
        const int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            return false;
        }

        if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            if (!set_nonblocking(fd)) {
                close(fd);
                return false;
            }
            sock_fd_ = fd;
            return true;
        }

        close(fd);
        if (waited_ms >= timeout_ms) {
            return false;
        }
        usleep(retry_interval_ms * 1000u);
        waited_ms += retry_interval_ms;
    }
}

void UdsSubscriberPort::close_fd() noexcept {
    if (sock_fd_ >= 0) {
        close(sock_fd_);
        sock_fd_ = -1;
    }
}

RecvStatus UdsSubscriberPort::recv_descriptor(SlotDescriptor& out_desc) noexcept {
    if (sock_fd_ < 0) {
        return RecvStatus::kError;
    }

    while (true) {
        const ssize_t n = recv(sock_fd_, &out_desc, sizeof(out_desc), MSG_DONTWAIT);
        if (n == static_cast<ssize_t>(sizeof(out_desc))) {
            return RecvStatus::kOk;
        }
        if (n == 0) {
            return RecvStatus::kPeerClosed;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return RecvStatus::kWouldBlock;
            }
            if (errno == ECONNRESET) {
                return RecvStatus::kPeerClosed;
            }
            return RecvStatus::kError;
        }
        // 非完整 32 字节报文视为协议异常
        return RecvStatus::kError;
    }
}

}  // namespace zeroflow::ipc