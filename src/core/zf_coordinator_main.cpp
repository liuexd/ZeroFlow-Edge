#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

#include "zeroflow/core/coordinator.hpp"
#include "zeroflow/core/pipeline_config.hpp"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <pipeline_config.yaml> [--duration-sec <N>]\n";
        return 1;
    }

    const std::string yaml_path = argv[1];
    zeroflow::core::CoordinatorOptions opts;

    for (int i = 2; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--duration-sec" && i + 1 < argc) {
            opts.max_run_duration_ms =
                static_cast<uint32_t>(std::stoul(argv[++i])) * 1000u;
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            return 1;
        }
    }

    auto parse_res = zeroflow::core::load_pipeline_config_from_file(yaml_path);
    if (!parse_res.ok) {
        std::cerr << "[CONFIG ERROR] " << parse_res.error_msg << "\n";
        return 2;
    }

    zeroflow::core::Coordinator coordinator(std::move(parse_res.config), opts);
    return coordinator.run();
}