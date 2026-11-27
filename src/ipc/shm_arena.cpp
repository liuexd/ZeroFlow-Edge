#include "zeroflow/ipc/shm_arena.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <new>
#include <utility>

#include "zeroflow/common/clock.hpp"

namespace zeroflow::ipc {
namespace {

std::string normalize_shm_name(const std::string& raw) {
    if (!raw.empty() && raw[0] == '/') {
        return raw;
    }
    return "/" + raw;
}

uint32_t count_set_bits(uint32_t mask) noexcept {
    return static_cast<uint32_t>(__builtin_popcount(mask));
}

}  // namespace

PosixShmArena::PosixShmArena(std::string pool_name,
                             ArenaRole role,
                             uint32_t slot_count,
                             std::size_t slot_capacity)
    : pool_name_(normalize_shm_name(pool_name)),
      role_(role),
      slot_count_(slot_count),
      slot_capacity_(slot_capacity) {}

PosixShmArena::~PosixShmArena() {
    destroy_backing_shm();
}

bool PosixShmArena::init() noexcept {
    if (header_ != nullptr) {
        return true;
    }
    if (slot_count_ == 0 || slot_count_ > 32 || slot_capacity_ == 0) {
        return false;
    }

    data_region_bytes_ = static_cast<std::size_t>(slot_count_) * slot_capacity_;
    const std::size_t total_bytes = kHeaderRegionBytes + data_region_bytes_;

    if (role_ == ArenaRole::kCreator) {
        // 清理可能残留的同名陈旧共享内存段
        shm_unlink(pool_name_.c_str());
        shm_fd_ = shm_open(pool_name_.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
        if (shm_fd_ < 0) {
            return false;
        }
        if (ftruncate(shm_fd_, static_cast<off_t>(total_bytes)) != 0) {
            close(shm_fd_);
            shm_fd_ = -1;
            shm_unlink(pool_name_.c_str());
            return false;
        }
    } else {
        // 即使 kConsumer 也需对 Header 区执行原子计数修改，因此 fd 以 O_RDWR 打开，
        // 随后在 mmap Data 区时通过 PROT_READ 实现硬件级只读隔离
        shm_fd_ = shm_open(pool_name_.c_str(), O_RDWR, 0666);
        if (shm_fd_ < 0) {
            return false;
        }
    }

    // 1. 映射 Header 元数据区 (4 KiB, RW)
    void* hdr_ptr = mmap(nullptr,
                         kHeaderRegionBytes,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED,
                         shm_fd_,
                         0);
    if (hdr_ptr == MAP_FAILED) {
        close(shm_fd_);
        shm_fd_ = -1;
        return false;
    }

    header_ = static_cast<ArenaHeader*>(hdr_ptr);
    slots_meta_ = reinterpret_cast<SlotMeta*>(
        static_cast<uint8_t*>(hdr_ptr) + sizeof(ArenaHeader));

    if (role_ == ArenaRole::kCreator) {
        std::memset(hdr_ptr, 0, kHeaderRegionBytes);
        new (header_) ArenaHeader{};
        header_->slot_count = slot_count_;
        header_->slot_capacity = slot_capacity_;
        header_->created_at_ns = common::now_monotonic_ns();

        for (uint32_t i = 0; i < slot_count_; ++i) {
            SlotMeta* meta = new (&slots_meta_[i]) SlotMeta{};
            meta->payload_capacity = slot_capacity_;
        }
    } else {
        if (header_->magic != kArenaMagic || header_->version != kArenaVersion) {
            munmap(hdr_ptr, kHeaderRegionBytes);
            header_ = nullptr;
            slots_meta_ = nullptr;
            close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
        slot_count_ = header_->slot_count;
        slot_capacity_ = static_cast<std::size_t>(header_->slot_capacity);
        data_region_bytes_ = static_cast<std::size_t>(slot_count_) * slot_capacity_;
    }

    // 2. 分段映射 Data Slots 负载区：订阅者强制 PROT_READ 只读权限
    const int data_prot =
        (role_ == ArenaRole::kConsumer) ? PROT_READ : (PROT_READ | PROT_WRITE);

    void* data_ptr = mmap(nullptr,
                          data_region_bytes_,
                          data_prot,
                          MAP_SHARED,
                          shm_fd_,
                          static_cast<off_t>(kHeaderRegionBytes));

    // POSIX mmap 成功后内核已持有引用，立即关闭 fd 防止描述符泄漏
    close(shm_fd_);
    shm_fd_ = -1;

    if (data_ptr == MAP_FAILED) {
        munmap(header_, kHeaderRegionBytes);
        header_ = nullptr;
        slots_meta_ = nullptr;
        return false;
    }

    data_base_ = static_cast<uint8_t*>(data_ptr);
    return true;
}

void PosixShmArena::destroy_backing_shm() noexcept {
    if (data_base_ != nullptr) {
        munmap(data_base_, data_region_bytes_);
        data_base_ = nullptr;
    }
    if (header_ != nullptr) {
        munmap(header_, kHeaderRegionBytes);
        header_ = nullptr;
        slots_meta_ = nullptr;
    }
    if (shm_fd_ >= 0) {
        close(shm_fd_);
        shm_fd_ = -1;
    }
    if (role_ == ArenaRole::kCreator && !pool_name_.empty()) {
        shm_unlink(pool_name_.c_str());
    }
}

std::optional<WritableSlot> PosixShmArena::allocate_slot() noexcept {
    if (role_ == ArenaRole::kConsumer || header_ == nullptr || data_base_ == nullptr) {
        return std::nullopt;
    }

    const uint32_t total = slot_count_;
    const int32_t self_pid = static_cast<int32_t>(getpid());

    for (uint32_t attempt = 0; attempt < total; ++attempt) {
        const uint32_t idx = (alloc_cursor_ + attempt) % total;
        SlotMeta& meta = slots_meta_[idx];

        uint32_t expected = static_cast<uint32_t>(SlotState::kFree);
        if (meta.state.compare_exchange_strong(expected,
                                               static_cast<uint32_t>(SlotState::kWriting),
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire)) {
            alloc_cursor_ = (idx + 1) % total;
            meta.writer_pid.store(self_pid, std::memory_order_relaxed);
            meta.lease_timestamp_ns.store(common::now_monotonic_ns(), std::memory_order_relaxed);

            WritableSlot slot;
            slot.slot_index = idx;
            slot.generation = meta.generation.load(std::memory_order_relaxed);
            slot.data = data_base_ + static_cast<std::size_t>(idx) * slot_capacity_;
            slot.capacity = slot_capacity_;
            return slot;
        }
    }

    // 池满，返回空交由上游执行 Drop-Newest 有界背压
    return std::nullopt;
}

std::optional<SlotDescriptor> PosixShmArena::commit_slot(uint32_t slot_index,
                                                         uint32_t payload_bytes,
                                                         uint32_t subscriber_mask,
                                                         uint64_t frame_seq,
                                                         uint64_t timestamp_us,
                                                         uint16_t width,
                                                         uint16_t height,
                                                         PixelFormat fmt) noexcept {
    if (role_ == ArenaRole::kConsumer || slot_index >= slot_count_ ||
        payload_bytes > slot_capacity_) {
        return std::nullopt;
    }

    SlotMeta& meta = slots_meta_[slot_index];
    if (meta.state.load(std::memory_order_acquire) !=
        static_cast<uint32_t>(SlotState::kWriting)) {
        return std::nullopt;
    }

    const uint32_t sub_count = count_set_bits(subscriber_mask);
    if (sub_count == 0) {
        // 无任何下游订阅者，直接回退至 kFree 避免泄漏
        meta.writer_pid.store(0, std::memory_order_relaxed);
        meta.lease_timestamp_ns.store(0, std::memory_order_relaxed);
        meta.state.store(static_cast<uint32_t>(SlotState::kFree), std::memory_order_release);
        return std::nullopt;
    }

    const uint32_t gen = meta.generation.load(std::memory_order_relaxed);
    meta.ref_count.store(sub_count, std::memory_order_relaxed);
    meta.subscriber_mask.store(subscriber_mask, std::memory_order_relaxed);
    meta.lease_timestamp_ns.store(common::now_monotonic_ns(), std::memory_order_relaxed);

    // Release 内存屏障：保证图像负载与计数元数据写入对所有下游 CPU 核心可见后才发布 READY
    meta.state.store(static_cast<uint32_t>(SlotState::kReady), std::memory_order_release);

    SlotDescriptor desc{};
    desc.slot_index = slot_index;
    desc.generation = gen;
    desc.frame_seq = frame_seq;
    desc.timestamp_us = timestamp_us;
    desc.payload_bytes = payload_bytes;
    desc.width = width;
    desc.height_and_fmt = pack_height_and_fmt(height, fmt);
    return desc;
}

std::optional<ReadOnlySlot> PosixShmArena::acquire_slot(const SlotDescriptor& desc,
                                                        uint32_t subscriber_id) noexcept {
    if (header_ == nullptr || data_base_ == nullptr ||
        subscriber_id >= kMaxSubscribersPerPort ||
        desc.slot_index >= slot_count_ ||
        desc.payload_bytes > slot_capacity_) {
        return std::nullopt;
    }

    SlotMeta& meta = slots_meta_[desc.slot_index];

    // Acquire 内存屏障：与发布端的 release 配对，观测最新图像数据与元数据
    if (meta.state.load(std::memory_order_acquire) !=
        static_cast<uint32_t>(SlotState::kReady)) {
        return std::nullopt;
    }

    // 校验代次码，防止延迟描述符引发 ABA 串帧
    if (meta.generation.load(std::memory_order_acquire) != desc.generation) {
        return std::nullopt;
    }

    const uint32_t bit = (1u << subscriber_id);
    if ((meta.subscriber_mask.load(std::memory_order_acquire) & bit) == 0) {
        return std::nullopt;
    }

    const uint8_t* ptr =
        data_base_ + static_cast<std::size_t>(desc.slot_index) * slot_capacity_;
    return ReadOnlySlot(this, desc.slot_index, subscriber_id, ptr, desc.payload_bytes);
}

void PosixShmArena::release_slot(uint32_t slot_index, uint32_t subscriber_id) noexcept {
    if (header_ == nullptr || slot_index >= slot_count_ ||
        subscriber_id >= kMaxSubscribersPerPort) {
        return;
    }

    SlotMeta& meta = slots_meta_[slot_index];
    const uint32_t bit = (1u << subscriber_id);

    // 幂等位清除：只有自身 bit 确实为 1 时才允许递减引用计数，防重复释放 Bug
    const uint32_t prev_mask =
        meta.subscriber_mask.fetch_and(~bit, std::memory_order_acq_rel);
    if ((prev_mask & bit) == 0) {
        return;
    }

    const uint32_t prev_ref = meta.ref_count.fetch_sub(1, std::memory_order_acq_rel);
    if (prev_ref == 1) {
        // 最后一个消费者归还，进入回收态并自增代次后释放回 kFree
        meta.state.store(static_cast<uint32_t>(SlotState::kReclaiming),
                         std::memory_order_acq_rel);
        meta.writer_pid.store(0, std::memory_order_relaxed);
        meta.lease_timestamp_ns.store(0, std::memory_order_relaxed);
        meta.generation.fetch_add(1, std::memory_order_acq_rel);
        meta.state.store(static_cast<uint32_t>(SlotState::kFree),
                         std::memory_order_release);
    }
}

}  // namespace zeroflow::ipc