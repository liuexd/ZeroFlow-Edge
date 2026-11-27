#include <cstdio>
#include <cstring>
#include <string>

#include "zeroflow/common/clock.hpp"
#include "zeroflow/common/crc32.hpp"
#include "zeroflow/core/node_cli.hpp"
#include "zeroflow/core/operator_node.hpp"

using namespace zeroflow::core;
using namespace zeroflow::ipc;

namespace {

class DummyTransformNode final : public OperatorNode {
public:
    DummyTransformNode(NodeRuntimeConfig cfg, std::string output_port)
        : OperatorNode(std::move(cfg)), output_port_(std::move(output_port)) {}

protected:
    void on_input(const std::string& /*local_port*/,
                  const SlotDescriptor& desc,
                  ReadOnlySlot slot) override {
        if (slot.size() <= sizeof(uint32_t)) {
            return;
        }
        const std::size_t body_len = slot.size() - sizeof(uint32_t);
        uint32_t expected_crc = 0;
        std::memcpy(&expected_crc, slot.data() + body_len, sizeof(uint32_t));
        if (zeroflow::common::compute_crc32(slot.data(), body_len) != expected_crc) {
            ++crc_errors_;
            return;
        }
        ++processed_;

        // 向旁路汇聚节点发送轻量推理结果报文
        auto out_slot = allocate_output_slot();
        if (!out_slot.has_value()) {
            return;
        }

        struct EventPayload {
            uint64_t source_seq;
            uint32_t det_count;
            uint32_t crc;
        } ev{desc.frame_seq, 2, 0};

        ev.crc = zeroflow::common::compute_crc32(
            reinterpret_cast<const uint8_t*>(&ev), sizeof(ev) - sizeof(uint32_t));
        std::memcpy(out_slot->data, &ev, sizeof(ev));

        (void)publish_output(output_port_,
                             *out_slot,
                             sizeof(ev),
                             desc.frame_seq,
                             zeroflow::common::now_monotonic_us(),
                             0,
                             0,
                             PixelFormat::kTensorRaw);
    }

    void on_shutdown() override {
        std::printf("[%s] Stopped: processed=%lu, crc_errors=%lu\n",
                    config().node_id.c_str(), processed_, crc_errors_);
        std::fflush(stdout);
    }

private:
    std::string output_port_;
    uint64_t processed_{0};
    uint64_t crc_errors_{0};
};

}  // namespace

int main(int argc, char* argv[]) {
    auto cli = parse_node_cli_args(argc, argv);
    if (!cli.ok) {
        std::fprintf(stderr, "[zf_dummy_transform] %s\n", cli.error_msg.c_str());
        return 1;
    }

    const std::string out_port =
        cli.runtime.outputs.empty() ? "det_event" : cli.runtime.outputs[0].port_name;
    DummyTransformNode node(std::move(cli.runtime), out_port);
    return node.run();
}