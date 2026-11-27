#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/common/crc32.hpp"
#include "zeroflow/core/node_cli.hpp"
#include "zeroflow/core/operator_node.hpp"

using namespace zeroflow::core;
using namespace zeroflow::ipc;

namespace {

class DummySourceNode final : public OperatorNode {
public:
    DummySourceNode(NodeRuntimeConfig cfg,
                    std::string output_port,
                    uint32_t payload_bytes)
        : OperatorNode(std::move(cfg)),
          output_port_(std::move(output_port)),
          payload_bytes_(payload_bytes) {}

protected:
    bool on_init() override {
        const std::size_t body_len = payload_bytes_ - sizeof(uint32_t);
        pattern_.resize(body_len);
        for (std::size_t i = 0; i < body_len; ++i) {
            pattern_[i] = static_cast<uint8_t>((i * 37u + 11u) & 0xFFu);
        }
        return true;
    }

    void on_timer() override {
        auto slot = allocate_output_slot();
        if (!slot.has_value()) {
            ++dropped_;
            return;
        }
        if (slot->capacity < payload_bytes_) {
            return;
        }

        ++seq_;
        const std::size_t body_len = payload_bytes_ - sizeof(uint32_t);
        std::memcpy(slot->data, pattern_.data(), body_len);
        std::memcpy(slot->data, &seq_, sizeof(seq_));

        const uint32_t crc = zeroflow::common::compute_crc32(slot->data, body_len);
        std::memcpy(slot->data + body_len, &crc, sizeof(uint32_t));

        if (publish_output(output_port_,
                           *slot,
                           payload_bytes_,
                           seq_,
                           zeroflow::common::now_monotonic_us(),
                           1920,
                           1080,
                           PixelFormat::kBgr24)) {
            ++published_;
        }
    }

    void on_shutdown() override {
        std::printf("[%s] Stopped: published=%lu, dropped=%lu\n",
                    config().node_id.c_str(), published_, dropped_);
        std::fflush(stdout);
    }

private:
    std::string output_port_;
    uint32_t payload_bytes_;
    std::vector<uint8_t> pattern_;
    uint64_t seq_{0};
    uint64_t published_{0};
    uint64_t dropped_{0};
};

}  // namespace

int main(int argc, char* argv[]) {
    auto cli = parse_node_cli_args(argc, argv);
    if (!cli.ok) {
        std::fprintf(stderr, "[zf_dummy_source] %s\n", cli.error_msg.c_str());
        return 1;
    }

    uint32_t fps = 30;
    if (auto it = cli.params.find("fps"); it != cli.params.end()) {
        fps = static_cast<uint32_t>(std::stoul(it->second));
    }
    uint32_t payload_bytes = 64 * 1024;  // 默认 64 KiB 测试帧
    if (auto it = cli.params.find("payload_bytes"); it != cli.params.end()) {
        payload_bytes = static_cast<uint32_t>(std::stoul(it->second));
    }

    cli.runtime.tick_interval_ms = (fps > 0) ? (1000u / fps) : 33u;
    const std::string out_port =
        cli.runtime.outputs.empty() ? "raw_frame" : cli.runtime.outputs[0].port_name;

    DummySourceNode node(std::move(cli.runtime), out_port, payload_bytes);
    return node.run();
}