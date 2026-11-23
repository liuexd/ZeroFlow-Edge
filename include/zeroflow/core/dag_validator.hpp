#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "zeroflow/core/pipeline_config.hpp"

namespace zeroflow::core {

struct PortFanoutInfo {
    std::string node_id;
    std::string port_name;
    uint32_t fanout_count{0};
    uint32_t initial_subscriber_mask{0};
    std::vector<std::string> subscribers;  // 格式: "<downstream_node>:<local_port>"
};

struct DagValidationResult {
    bool ok{false};
    std::string error_msg;
    std::vector<std::string> topo_order;       // Kahn 算法生成的安全启动拓扑序
    std::vector<std::string> cycle_path;       // 若存在有向环，记录具体成环路径
    std::vector<PortFanoutInfo> port_fanouts;  // 各输出端口扇出度与位掩码预计算结果
};

// 校验节点唯一性、端口引用完整性、单端口扇出上限(<=32)，并生成拓扑启动序
[[nodiscard]] DagValidationResult validate_and_sort_dag(PipelineConfig& config);

}  // namespace zeroflow::core