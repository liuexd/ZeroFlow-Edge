#include <cstdio>
#include <cstring>
#include <string>

#include "zeroflow/common/crc32.hpp"
#include "zeroflow/core/node_cli.hpp"
#include "zeroflow/core/operator_node.hpp"

using namespace zeroflow::core;
using namespace zeroflow::ipc;

namespace {

class DummySinkNode final : public OperatorNode {
public:
    explicit DummySinkNode(NodeRuntimeConfig cfg) : OperatorNode(std::move(cfg)) {}

protected:
    void on_input(const std::string& local_port,
                  const SlotDescriptor& /*desc*/,
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

        if (local_port == "in_frame") {
            ++frame_count_;
        } else {
            ++event_count_;
        }
    }

    void on_shutdown() override {
        std::printf("[%s] Stopped: frames=%lu, events=%lu, crc_errors=%lu\n",
                    config().node_id.c_str(), frame_count_, event_count_, crc_errors_);
        std::fflush(stdout);
    }

private:
    uint64_t frame_count_{0};
    uint64_t event_count_{0};
    uint64_t crc_errors_{0};
};

}  // namespace

int main(int argc, char* argv[]) {
    auto cli = parse_node_cli_args(argc, argv);
    if (!cli.ok) {
        std::fprintf(stderr, "[zf_dummy_sink] %s\n", cli.error_msg.c_str());
        return 1;
    }

    DummySinkNode node(std::move(cli.runtime));
    return node.run();
}