#include "zeroflow/core/node_cli.hpp"

#include <cstdio>
#include <cstdlib>

namespace zeroflow::core {
namespace {

bool parse_three_part_spec(const std::string& raw,
                           std::string& part1,
                           std::string& part2,
                           std::string& part3) {
    const std::size_t first = raw.find(':');
    if (first == std::string::npos || first == 0) {
        return false;
    }
    const std::size_t second = raw.find(':', first + 1);
    if (second == std::string::npos || second <= first + 1 || second + 1 >= raw.size()) {
        return false;
    }
    part1 = raw.substr(0, first);
    part2 = raw.substr(first + 1, second - first - 1);
    part3 = raw.substr(second + 1);
    return true;
}

}  // namespace

ParsedNodeCli parse_node_cli_args(int argc, char* argv[]) {
    ParsedNodeCli res;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next_val = [&](std::string& out) -> bool {
            if (i + 1 >= argc) {
                res.error_msg = "Missing value after option: " + arg;
                return false;
            }
            out = argv[++i];
            return true;
        };

        if (arg == "--pipeline") {
            if (!next_val(res.runtime.pipeline_name)) return res;
        } else if (arg == "--pool") {
            if (!next_val(res.runtime.pool_name)) return res;
        } else if (arg == "--node-id") {
            if (!next_val(res.runtime.node_id)) return res;
        } else if (arg == "--input") {
            std::string spec_str;
            if (!next_val(spec_str)) return res;
            std::string port, path, id_str;
            if (!parse_three_part_spec(spec_str, port, path, id_str)) {
                res.error_msg = "Invalid --input format: " + spec_str;
                return res;
            }
            InputEndpointSpec ep;
            ep.local_port = std::move(port);
            ep.socket_path = std::move(path);
            ep.subscriber_id = static_cast<uint32_t>(std::stoul(id_str));
            res.runtime.inputs.push_back(std::move(ep));
        } else if (arg == "--output") {
            std::string spec_str;
            if (!next_val(spec_str)) return res;
            std::string port, path, mask_str;
            if (!parse_three_part_spec(spec_str, port, path, mask_str)) {
                res.error_msg = "Invalid --output format: " + spec_str;
                return res;
            }
            OutputEndpointSpec ep;
            ep.port_name = std::move(port);
            ep.socket_path = std::move(path);
            ep.subscriber_mask = static_cast<uint32_t>(std::stoul(mask_str, nullptr, 16));
            res.runtime.outputs.push_back(std::move(ep));
        } else if (arg == "--param") {
            std::string kv;
            if (!next_val(kv)) return res;
            const std::size_t eq = kv.find('=');
            if (eq == std::string::npos || eq == 0) {
                res.error_msg = "Invalid --param format (expected key=value): " + kv;
                return res;
            }
            res.params[kv.substr(0, eq)] = kv.substr(eq + 1);
        } else {
            res.error_msg = "Unknown option: " + arg;
            return res;
        }
    }

    if (res.runtime.pool_name.empty() || res.runtime.node_id.empty()) {
        res.error_msg = "--pool and --node-id are required";
        return res;
    }

    res.ok = true;
    return res;
}

std::vector<std::string> build_node_exec_args(
    const std::string& pipeline_name,
    const std::string& pool_name,
    const std::string& executable,//可执行程序路径
    const std::string& node_id,
    const std::vector<std::string>& extra_args,
    const std::vector<InputEndpointSpec>& inputs,
    const std::vector<OutputEndpointSpec>& outputs,
    const std::unordered_map<std::string, std::string>& params) {
    std::vector<std::string> argv;
    argv.push_back(executable);
    argv.push_back("--pipeline");
    argv.push_back(pipeline_name);
    argv.push_back("--pool");
    argv.push_back(pool_name);
    argv.push_back("--node-id");
    argv.push_back(node_id);

    for (const auto& extra : extra_args) {
        argv.push_back(extra);
    }

    for (const auto& in_ep : inputs) {
        argv.push_back("--input");
        argv.push_back(in_ep.local_port + ":" + in_ep.socket_path + ":" +
                       std::to_string(in_ep.subscriber_id));
    }

    for (const auto& out_ep : outputs) {
        char mask_buf[16];
        std::snprintf(mask_buf, sizeof(mask_buf), "%x", out_ep.subscriber_mask);
        argv.push_back("--output");
        argv.push_back(out_ep.port_name + ":" + out_ep.socket_path + ":" + mask_buf);
    }

    for (const auto& kv : params) {
        argv.push_back("--param");
        argv.push_back(kv.first + "=" + kv.second);
    }

    return argv;
}

}  // namespace zeroflow::core