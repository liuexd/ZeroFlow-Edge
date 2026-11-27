#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/common/crc32.hpp"
#include "zeroflow/ipc/shm_arena.hpp"

using namespace zeroflow::ipc;

namespace {

struct BenchOptions {
    uint32_t readers{3};
    uint32_t payload_mb{6};
    uint32_t rate_hz{1000};
    uint32_t duration_sec{10};
};

BenchOptions parse_args(int argc, char* argv[]) {
    BenchOptions opts;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--readers" && i + 1 < argc) {
            opts.readers = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--payload-mb" && i + 1 < argc) {
            opts.payload_mb = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--rate-hz" && i + 1 < argc) {
            opts.rate_hz = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--duration-sec" && i + 1 < argc) {
            opts.duration_sec = static_cast<uint32_t>(std::stoul(argv[++i]));
        }
    }
    return opts;
}

bool read_exact(int fd, void* buf, std::size_t bytes) noexcept {
    auto* p = static_cast<uint8_t*>(buf);
    std::size_t off = 0;
    while (off < bytes) {
        const ssize_t n = read(fd, p + off, bytes - off);
        if (n <= 0) {
            return false;
        }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

bool write_exact(int fd, const void* buf, std::size_t bytes) noexcept {
    const auto* p = static_cast<const uint8_t*>(buf);
    std::size_t off = 0;
    while (off < bytes) {
        const ssize_t n = write(fd, p + off, bytes - off);
        if (n <= 0) {
            return false;
        }
        off += static_cast<std::size_t>(n);
    }
    return true;
}

int run_reader_process(const std::string& pool_name, uint32_t sub_id, int pipe_read_fd) {
    PosixShmArena consumer(pool_name, ArenaRole::kConsumer);
    if (!consumer.init()) {
        std::fprintf(stderr, "[Reader %u] Failed to init ShmArena\n", sub_id);
        return 1;
    }

    uint64_t frames_checked = 0;
    uint64_t crc_errors = 0;
    SlotDescriptor desc{};

    while (read_exact(pipe_read_fd, &desc, sizeof(desc))) {
        auto slot = consumer.acquire_slot(desc, sub_id);
        if (!slot.has_value()) {
            std::fprintf(stderr, "[Reader %u] Failed to acquire slot %u (gen=%u)\n",
                         sub_id, desc.slot_index, desc.generation);
            ++crc_errors;
            continue;
        }

        const std::size_t body_len = slot->size() - sizeof(uint32_t);
        uint32_t expected_crc = 0;
        std::memcpy(&expected_crc, slot->data() + body_len, sizeof(uint32_t));

        const uint32_t actual_crc = zeroflow::common::compute_crc32(slot->data(), body_len);
        if (actual_crc != expected_crc) {
            std::fprintf(stderr, "[Reader %u] CRC32 MISMATCH at frame %lu! expected=0x%08X actual=0x%08X\n",
                         sub_id, desc.frame_seq, expected_crc, actual_crc);
            ++crc_errors;
        }
        ++frames_checked;
    }

    close(pipe_read_fd);
    std::printf("[Reader %u] Finished: verified=%lu frames, crc_errors=%lu\n",
                sub_id, frames_checked, crc_errors);
    return (crc_errors == 0 && frames_checked > 0) ? 0 : 2;
}

int run_writer_process(const std::string& pool_name,
                       const BenchOptions& opts,
                       const std::vector<int>& pipe_write_fds) {
    PosixShmArena producer(pool_name, ArenaRole::kProducer);
    if (!producer.init()) {
        std::fprintf(stderr, "[Writer] Failed to init ShmArena\n");
        return 1;
    }

    const std::size_t payload_bytes = static_cast<std::size_t>(opts.payload_mb) * 1024u * 1024u;
    const std::size_t body_len = payload_bytes - sizeof(uint32_t);
    const uint32_t all_sub_mask = (1u << opts.readers) - 1u;
    const uint64_t interval_ns = 1000000000ULL / opts.rate_hz;
    const uint64_t total_target_frames = static_cast<uint64_t>(opts.rate_hz) * opts.duration_sec;

    // 预生成随机底板以降低写入端 PRNG 瓶颈，每帧覆盖动态序列头尾以防伪造一致
    std::vector<uint8_t> template_buf(body_len);
    for (std::size_t i = 0; i < body_len; ++i) {
        template_buf[i] = static_cast<uint8_t>((i * 131u + 17u) & 0xFFu);
    }

    uint64_t published = 0;
    uint64_t dropped_backpressure = 0;
    uint64_t next_deadline_ns = zeroflow::common::now_monotonic_ns();
    const uint64_t start_ns = next_deadline_ns;

    for (uint64_t seq = 1; seq <= total_target_frames; ++seq) {
        auto wslot = producer.allocate_slot();
        if (!wslot.has_value()) {
            ++dropped_backpressure;
        } else {
            std::memcpy(wslot->data, template_buf.data(), body_len);
            // 写入动态帧号印记，确保相邻复用同一槽位的帧内容完全不同
            std::memcpy(wslot->data, &seq, sizeof(seq));
            std::memcpy(wslot->data + body_len - sizeof(seq), &seq, sizeof(seq));

            const uint32_t crc = zeroflow::common::compute_crc32(wslot->data, body_len);
            std::memcpy(wslot->data + body_len, &crc, sizeof(uint32_t));

            auto desc = producer.commit_slot(wslot->slot_index,
                                             static_cast<uint32_t>(payload_bytes),
                                             all_sub_mask,
                                             seq,
                                             zeroflow::common::now_monotonic_us(),
                                             1920,
                                             1080,
                                             PixelFormat::kBgr24);
            if (desc.has_value()) {
                for (int fd : pipe_write_fds) {
                    if (!write_exact(fd, &(*desc), sizeof(*desc))) {
                        std::fprintf(stderr, "[Writer] Pipe broken\n");
                        break;
                    }
                }
                ++published;
            }
        }

        next_deadline_ns += interval_ns;
        struct timespec ts {};
        ts.tv_sec = static_cast<time_t>(next_deadline_ns / 1000000000ULL);
        ts.tv_nsec = static_cast<long>(next_deadline_ns % 1000000000ULL);
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr);
    }

    for (int fd : pipe_write_fds) {
        close(fd);
    }

    const double elapsed_sec =
        static_cast<double>(zeroflow::common::now_monotonic_ns() - start_ns) / 1e9;
    const double total_read_gbps =
        (static_cast<double>(published) * payload_bytes * opts.readers) / (elapsed_sec * 1e9);

    std::printf("[Writer] Done in %.2fs: published=%lu, pool_full_dropped=%lu, aggregate_read_bw=%.2f GB/s\n",
                elapsed_sec, published, dropped_backpressure, total_read_gbps);
    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    const BenchOptions opts = parse_args(argc, argv);
    if (opts.readers == 0 || opts.readers > 32) {
        std::cerr << "--readers must be within [1, 32]\n";
        return 1;
    }

    const std::string pool_name = "zf_stress_" + std::to_string(getpid());
    const std::size_t slot_cap = static_cast<std::size_t>(opts.payload_mb + 2) * 1024u * 1024u;

    PosixShmArena creator(pool_name, ArenaRole::kCreator, 16, slot_cap);
    if (!creator.init()) {
        std::cerr << "Failed to create PosixShmArena\n";
        return 1;
    }

    std::printf("=== Starting Multi-Process SHM Stress Benchmark ===\n");
    std::printf("Topology: 1 Writer -> %u Readers | Payload: %u MiB | Target Rate: %u Hz | Duration: %u s\n",
                opts.readers, opts.payload_mb, opts.rate_hz, opts.duration_sec);

    std::vector<int> read_fds(opts.readers, -1);
    std::vector<int> write_fds(opts.readers, -1);
    std::vector<pid_t> child_pids;

    for (uint32_t i = 0; i < opts.readers; ++i) {
        int fds[2];
        if (pipe(fds) != 0) {
            std::perror("pipe");
            return 1;
        }
        read_fds[i] = fds[0];
        write_fds[i] = fds[1];
    }

    // 1. 拉起 N 个独立只读消费进程
    for (uint32_t i = 0; i < opts.readers; ++i) {
        const pid_t pid = fork();
        if (pid == 0) {
            for (uint32_t j = 0; j < opts.readers; ++j) {
                close(write_fds[j]);
                if (j != i) close(read_fds[j]);
            }
            const int rc = run_reader_process(pool_name, i, read_fds[i]);
            _exit(rc);
        }
        child_pids.push_back(pid);
    }

    // 2. 拉起 1 个独立写进程
    const pid_t writer_pid = fork();
    if (writer_pid == 0) {
        for (uint32_t j = 0; j < opts.readers; ++j) {
            close(read_fds[j]);
        }
        const int rc = run_writer_process(pool_name, opts, write_fds);
        _exit(rc);
    }
    child_pids.push_back(writer_pid);

    // 父进程关闭所有管道端点
    for (uint32_t j = 0; j < opts.readers; ++j) {
        close(read_fds[j]);
        close(write_fds[j]);
    }

    bool all_ok = true;
    for (pid_t pid : child_pids) {
        int status = 0;
        waitpid(pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            all_ok = false;
        }
    }

    // 3. 巡检全部 16 个槽位是否 100% 回归 kFree 且 ref_count == 0
    uint32_t leaked_slots = 0;
    for (uint32_t i = 0; i < creator.slot_count(); ++i) {
        const auto& meta = creator.slot_meta_array()[i];
        if (meta.state.load() != static_cast<uint32_t>(SlotState::kFree) ||
            meta.ref_count.load() != 0) {
            ++leaked_slots;
            std::fprintf(stderr, "[LEAK] Slot %u state=%u ref_count=%u\n",
                         i, meta.state.load(), meta.ref_count.load());
        }
    }

    std::printf("=== Pool Audit: leaked_slots = %u / %u ===\n",
                leaked_slots, creator.slot_count());
    return (all_ok && leaked_slots == 0) ? 0 : 1;
}