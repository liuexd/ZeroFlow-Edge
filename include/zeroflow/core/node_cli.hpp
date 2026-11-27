#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "zeroflow/core/operator_node.hpp"

namespace zeroflow::core {

struct ParsedNodeCli {
    bool ok{false};
    std::string error_msg;
    NodeRuntimeConfig runtime;
    std::unordered_map<std::string, std::string> params;
};

// 解析 Coordinator 注入的算子进程参数：
// --pipeline <name> --pool <pool> --node-id <id>
// --input <local_port>:<socket_path>:<sub_id>
// --output <port_name>:<socket_path>:<sub_mask_hex>
// --param <key>=<value>
[[nodiscard]] ParsedNodeCli parse_node_cli_args(int argc, char* argv[]);

// 将节点配置序列化为 execvp 参数列表（首个元素为 executable）
[[nodiscard]] std::vector<std::string> build_node_exec_args(
    const std::string& pipeline_name,
    const std::string& pool_name,
    const std::string& executable,
    const std::string& node_id,
    const std::vector<std::string>& extra_args,
    const std::vector<InputEndpointSpec>& inputs,
    const std::vector<OutputEndpointSpec>& outputs,
    const std::unordered_map<std::string, std::string>& params);

}  // namespace zeroflow::core