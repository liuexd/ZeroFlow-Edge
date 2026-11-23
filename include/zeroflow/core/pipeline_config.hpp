#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace zeroflow::core {

enum class RestartPolicy {
    kAlways,
    kOnFailure,
    kNever,
};

struct ShmPoolSpec {
    std::string pool_name{"zeroflow_pool"};
    uint32_t slot_count{16};
    uint32_t slot_size_mb{8};
};

struct PortBinding {
    std::string local_port;
    std::string upstream_node;
    std::string upstream_port;
    uint32_t subscriber_id{0};  // 由 DAG 校验器自动分配 (0..31)
};

struct NodeSpec {
    std::string id;
    std::string executable;
    std::vector<std::string> args;
    RestartPolicy restart_policy{RestartPolicy::kOnFailure};
    std::vector<PortBinding> inputs;
    std::vector<std::string> outputs;
    std::unordered_map<std::string, std::string> params;
};

struct PipelineConfig {
    std::string pipeline_name{"default_pipeline"};
    ShmPoolSpec shm_pool;
    std::vector<NodeSpec> nodes;
};

struct ParseResult {
    bool ok{false};
    PipelineConfig config;
    std::string error_msg;
};

[[nodiscard]] ParseResult load_pipeline_config_from_file(const std::string& file_path);
[[nodiscard]] ParseResult load_pipeline_config_from_string(const std::string& yaml_content);

}  // namespace zeroflow::core