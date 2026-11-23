#include <cstdio>
#include <iostream>

#include "zeroflow/core/dag_validator.hpp"
#include "zeroflow/core/pipeline_config.hpp"

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <pipeline_config.yaml>\n";
        return 1;
    }

    auto parse_res = zeroflow::core::load_pipeline_config_from_file(argv[1]);
    if (!parse_res.ok) {
        std::cerr << "[CONFIG ERROR] " << parse_res.error_msg << "\n";
        return 2;
    }

    auto dag_res = zeroflow::core::validate_and_sort_dag(parse_res.config);
    if (!dag_res.ok) {
        std::cerr << "[DAG ERROR] " << dag_res.error_msg << "\n";
        return 3;
    }

    const auto& shm = parse_res.config.shm_pool;
    const uint32_t total_mb = shm.slot_count * shm.slot_size_mb;

    std::cout << "=== ZeroFlow-Edge DAG Validation Passed ===\n";
    std::cout << "Pipeline : " << parse_res.config.pipeline_name << "\n";
    std::cout << "SHM Pool : /dev/shm/" << shm.pool_name << " (" << shm.slot_count
              << " slots x " << shm.slot_size_mb << " MiB = " << total_mb << " MiB)\n";

    std::cout << "\n[Topological Startup Order]\n";
    for (std::size_t i = 0; i < dag_res.topo_order.size(); ++i) {
        std::cout << "  " << (i + 1) << ". " << dag_res.topo_order[i] << "\n";
    }

    std::cout << "\n[Port Fan-out & Subscriber Bitmask]\n";
    for (const auto& pf : dag_res.port_fanouts) {
        std::printf("  - %s/%s : fanout=%u, mask=0x%08X",
                    pf.node_id.c_str(),
                    pf.port_name.c_str(),
                    pf.fanout_count,
                    pf.initial_subscriber_mask);
        if (!pf.subscribers.empty()) {
            std::cout << " -> [";
            for (std::size_t i = 0; i < pf.subscribers.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << pf.subscribers[i];
            }
            std::cout << "]";
        }
        std::cout << "\n";
    }

    return 0;
}