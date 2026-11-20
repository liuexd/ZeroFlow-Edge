#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "zeroflow/ipc/slot_descriptor.hpp"

namespace zeroflow::ipc {

static_assert(std::atomic<uint32_t>::is_always_lock_free,
              "uint32_t atomic must be lock-free across processes");
static_assert(std::atomic<uint64_t>::is_always_lock_free,
              "uint64_t atomic must be lock-free across processes");

constexpr uint32_t kArenaMagic = 0x5A464C57u;  // "ZFLW"
constexpr uint32_t kArenaVersion = 1u;
constexpr std::size_t kHeaderRegionBytes = 4096u;  // 4 KiB 页对齐元数据区
constexpr uint32_t kMaxSubscribersPerPort = 32u;

enum class SlotState : uint32_t {
    kFree       = 0,  // 空闲可分配
    kWriting    = 1,  // 发布者已抢占，正在写入负载
    kReady      = 2,  // 写入完成并已 release 发布，消费者只读访问中
    kReclaiming = 3,  // 引用计数归零或孤儿回收过渡态
};

enum class ArenaRole {
    kCreator,   // Coordinator 守护进程：创建并初始化共享内存段
    kProducer,  // 发布者算子：Header 与 Data 区均 RW 映射
    kConsumer,  // 订阅者算子：Header 区 RW，Data 区强制 PROT_READ 只读映射
};

// 单槽位元数据：严格 64 字节独占一个 Cache Line，消除多核更新伪共享
struct alignas(64) SlotMeta {
    std::atomic<uint32_t> state{static_cast<uint32_t>(SlotState::kFree)};
    std::atomic<uint32_t> ref_count{0};
    std::atomic<uint32_t> subscriber_mask{0};
    std::atomic<uint32_t> generation{0};
    std::atomic<int32_t>  writer_pid{0};
    uint32_t              reserved_pad{0};
    std::atomic<uint64_t> lease_timestamp_ns{0};
    uint64_t              payload_capacity{0};
    uint64_t              reserved[3]{0, 0, 0};
};

static_assert(sizeof(SlotMeta) == 64, "SlotMeta must occupy exactly 64 bytes");
static_assert(alignof(SlotMeta) == 64, "SlotMeta must be 64-byte aligned");

// 共享内存池全局头部：同样 64 字节缓存行对齐
struct alignas(64) ArenaHeader {
    uint32_t magic{kArenaMagic};
    uint32_t version{kArenaVersion};
    uint32_t slot_count{16};
    uint32_t header_region_bytes{static_cast<uint32_t>(kHeaderRegionBytes)};
    uint64_t slot_capacity{8u * 1024u * 1024u};  // 默认单槽 8 MiB
    uint64_t created_at_ns{0};
    uint64_t reserved[4]{0, 0, 0, 0};
};

static_assert(sizeof(ArenaHeader) == 64, "ArenaHeader must occupy 64 bytes");
static_assert(sizeof(ArenaHeader) + 32 * sizeof(SlotMeta) <= kHeaderRegionBytes,
              "Header region must fit ArenaHeader and up to 32 SlotMeta entries");

class IMemoryArena;

// 发布端可写槽位视图
struct WritableSlot {
    uint32_t slot_index{0};
    uint32_t generation{0};
    uint8_t* data{nullptr};
    std::size_t capacity{0};
};

// 消费端只读槽位 RAII 守卫：析构或显式 reset() 时自动归还引用计数
class ReadOnlySlot {
public:
    ReadOnlySlot() = default;
    ReadOnlySlot(IMemoryArena* arena,
                 uint32_t slot_index,
                 uint32_t subscriber_id,
                 const uint8_t* data,
                 std::size_t size) noexcept
        : arena_(arena),
          slot_index_(slot_index),
          subscriber_id_(subscriber_id),
          data_(data),
          size_(size) {}

    ~ReadOnlySlot() noexcept { reset(); }

    ReadOnlySlot(const ReadOnlySlot&) = delete;
    ReadOnlySlot& operator=(const ReadOnlySlot&) = delete;

    ReadOnlySlot(ReadOnlySlot&& other) noexcept
        : arena_(other.arena_),
          slot_index_(other.slot_index_),
          subscriber_id_(other.subscriber_id_),
          data_(other.data_),
          size_(other.size_) {
        other.arena_ = nullptr;
        other.data_ = nullptr;
        other.size_ = 0;
    }

    ReadOnlySlot& operator=(ReadOnlySlot&& other) noexcept {
        if (this != &other) {
            reset();
            arena_ = other.arena_;
            slot_index_ = other.slot_index_;
            subscriber_id_ = other.subscriber_id_;
            data_ = other.data_;
            size_ = other.size_;
            other.arena_ = nullptr;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    void reset() noexcept;

    [[nodiscard]] bool valid() const noexcept { return arena_ != nullptr && data_ != nullptr; }
    [[nodiscard]] const uint8_t* data() const noexcept { return data_; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] uint32_t slot_index() const noexcept { return slot_index_; }

private:
    IMemoryArena* arena_{nullptr};
    uint32_t slot_index_{0};
    uint32_t subscriber_id_{0};
    const uint8_t* data_{nullptr};
    std::size_t size_{0};
};

// 内存池抽象接口（预留 RK3588 DmaBufHeapArena 硬件零拷贝平滑替换）
class IMemoryArena {
public:
    virtual ~IMemoryArena() = default;

    [[nodiscard]] virtual std::optional<WritableSlot> allocate_slot() noexcept = 0;

    [[nodiscard]] virtual std::optional<SlotDescriptor> commit_slot(
        uint32_t slot_index,
        uint32_t payload_bytes,
        uint32_t subscriber_mask,
        uint64_t frame_seq,
        uint64_t timestamp_us,
        uint16_t width,
        uint16_t height,
        PixelFormat fmt) noexcept = 0;

    [[nodiscard]] virtual std::optional<ReadOnlySlot> acquire_slot(
        const SlotDescriptor& desc,
        uint32_t subscriber_id) noexcept = 0;

    virtual void release_slot(uint32_t slot_index, uint32_t subscriber_id) noexcept = 0;

    [[nodiscard]] virtual uint32_t slot_count() const noexcept = 0;
    [[nodiscard]] virtual std::size_t slot_capacity() const noexcept = 0;
};

inline void ReadOnlySlot::reset() noexcept {
    if (arena_ != nullptr) {
        arena_->release_slot(slot_index_, subscriber_id_);
        arena_ = nullptr;
        data_ = nullptr;
        size_ = 0;
    }
}

// POSIX 共享内存池实现 (/dev/shm)
class PosixShmArena final : public IMemoryArena {
public:
    PosixShmArena(std::string pool_name,
                  ArenaRole role,
                  uint32_t slot_count = 16,
                  std::size_t slot_capacity = 8u * 1024u * 1024u);
    ~PosixShmArena() override;

    PosixShmArena(const PosixShmArena&) = delete;
    PosixShmArena& operator=(const PosixShmArena&) = delete;

    [[nodiscard]] bool init() noexcept;
    void destroy_backing_shm() noexcept;

    [[nodiscard]] std::optional<WritableSlot> allocate_slot() noexcept override;
    [[nodiscard]] std::optional<SlotDescriptor> commit_slot(
        uint32_t slot_index,
        uint32_t payload_bytes,
        uint32_t subscriber_mask,
        uint64_t frame_seq,
        uint64_t timestamp_us,
        uint16_t width,
        uint16_t height,
        PixelFormat fmt) noexcept override;
    [[nodiscard]] std::optional<ReadOnlySlot> acquire_slot(
        const SlotDescriptor& desc,
        uint32_t subscriber_id) noexcept override;
    void release_slot(uint32_t slot_index, uint32_t subscriber_id) noexcept override;

    [[nodiscard]] uint32_t slot_count() const noexcept override { return slot_count_; }
    [[nodiscard]] std::size_t slot_capacity() const noexcept override { return slot_capacity_; }

    // 供 Coordinator 巡检与孤儿槽位清扫只读访问元数据数组
    [[nodiscard]] SlotMeta* slot_meta_array() noexcept { return slots_meta_; }

private:
    std::string pool_name_;
    ArenaRole role_;
    uint32_t slot_count_;
    std::size_t slot_capacity_;

    int shm_fd_{-1};
    ArenaHeader* header_{nullptr};
    SlotMeta* slots_meta_{nullptr};
    uint8_t* data_base_{nullptr};
    std::size_t data_region_bytes_{0};
    uint32_t alloc_cursor_{0};
};

}  // namespace zeroflow::ipc