#include "zeroflow/core/pipeline_config.hpp"

#include <fstream>
#include <sstream>
#include <yaml-cpp/yaml.h>

#include "zeroflow/ipc/shm_arena.hpp"

namespace zeroflow::core {
namespace {

bool parse_restart_policy(const std::string& raw, RestartPolicy& out) {
    if (raw == "always") {
        out = RestartPolicy::kAlways;
        return true;
    }
    if (raw == "on-failure") {
        out = RestartPolicy::kOnFailure;
        return true;
    }
    if (raw == "never") {
        out = RestartPolicy::kNever;
        return true;
    }
    return false;
}

bool parse_binding_expr(const std::string& expr,
                        std::string& upstream_node,
                        std::string& upstream_port) {
    const std::size_t slash_pos = expr.find('/');
    if (slash_pos == std::string::npos || slash_pos == 0 || slash_pos + 1 >= expr.size()) {
        return false;
    }
    // 禁止出现多个 '/'
    if (expr.find('/', slash_pos + 1) != std::string::npos) {
        return false;
    }
    upstream_node = expr.substr(0, slash_pos);
    upstream_port = expr.substr(slash_pos + 1);
    return true;
}

ParseResult parse_yaml_root(const YAML::Node& root) {
    ParseResult res;
    if (!root || !root.IsMap()) {
        res.error_msg = "YAML root must be a valid mapping object";
        return res;
    }

    if (root["pipeline_name"]) {
        res.config.pipeline_name = root["pipeline_name"].as<std::string>();
    }

    if (const auto shm_node = root["shm_pool"]) {
        if (!shm_node.IsMap()) {
            res.error_msg = "'shm_pool' must be a mapping object";
            return res;
        }
        if (shm_node["pool_name"]) {
            res.config.shm_pool.pool_name = shm_node["pool_name"].as<std::string>();
        }
        if (shm_node["slot_count"]) {
            res.config.shm_pool.slot_count = shm_node["slot_count"].as<uint32_t>();
        }
        if (shm_node["slot_size_mb"]) {
            res.config.shm_pool.slot_size_mb = shm_node["slot_size_mb"].as<uint32_t>();
        }
    }

    if (res.config.shm_pool.pool_name.empty()) {
        res.error_msg = "shm_pool.pool_name cannot be empty";
        return res;
    }
    if (res.config.shm_pool.slot_count == 0 || res.config.shm_pool.slot_count > 32) {
        res.error_msg = "shm_pool.slot_count must be within [1, 32] to fit 4KiB ArenaHeader";
        return res;
    }
    if (res.config.shm_pool.slot_size_mb == 0 || res.config.shm_pool.slot_size_mb > 64) {
        res.error_msg = "shm_pool.slot_size_mb must be within [1, 64]";
        return res;
    }

    const auto nodes_seq = root["nodes"];
    if (!nodes_seq || !nodes_seq.IsSequence() || nodes_seq.size() == 0) {
        res.error_msg = "'nodes' must be a non-empty sequence";
        return res;
    }

    for (std::size_t i = 0; i < nodes_seq.size(); ++i) {
        const YAML::Node& n = nodes_seq[i];
        if (!n.IsMap() || !n["id"] || !n["executable"]) {
            res.error_msg = "Node at index " + std::to_string(i) +
                            " is missing required 'id' or 'executable' field";
            return res;
        }

        NodeSpec spec;
        spec.id = n["id"].as<std::string>();
        spec.executable = n["executable"].as<std::string>();
        if (spec.id.empty() || spec.executable.empty()) {
            res.error_msg = "Node 'id' and 'executable' cannot be empty";
            return res;
        }

        if (n["args"]) {
            if (!n["args"].IsSequence()) {
                res.error_msg = "Node '" + spec.id + "': 'args' must be a sequence";
                return res;
            }
            for (const auto& arg : n["args"]) {
                spec.args.push_back(arg.as<std::string>());
            }
        }

        if (n["restart_policy"]) {
            const std::string policy_str = n["restart_policy"].as<std::string>();
            if (!parse_restart_policy(policy_str, spec.restart_policy)) {
                res.error_msg = "Node '" + spec.id + "': invalid restart_policy '" + policy_str + "'";
                return res;
            }
        }

        if (n["inputs"]) {
            if (!n["inputs"].IsMap()) {
                res.error_msg = "Node '" + spec.id + "': 'inputs' must be a mapping";
                return res;
            }
            for (const auto& kv : n["inputs"]) {
                PortBinding binding;
                binding.local_port = kv.first.as<std::string>();
                const std::string target_expr = kv.second.as<std::string>();
                if (binding.local_port.empty() ||
                    !parse_binding_expr(target_expr, binding.upstream_node, binding.upstream_port)) {
                    res.error_msg = "Node '" + spec.id + "': invalid input binding '" +
                                    binding.local_port + ": " + target_expr +
                                    "' (expected '<upstream_node>/<output_port>')";
                    return res;
                }
                spec.inputs.push_back(std::move(binding));
            }
        }

        if (n["outputs"]) {
            if (!n["outputs"].IsSequence()) {
                res.error_msg = "Node '" + spec.id + "': 'outputs' must be a sequence";
                return res;
            }
            for (const auto& out_item : n["outputs"]) {
                const std::string out_name = out_item.as<std::string>();
                if (out_name.empty()) {
                    res.error_msg = "Node '" + spec.id + "': output port name cannot be empty";
                    return res;
                }
                spec.outputs.push_back(out_name);
            }
        }

        if (n["params"]) {
            if (!n["params"].IsMap()) {
                res.error_msg = "Node '" + spec.id + "': 'params' must be a mapping";
                return res;
            }
            for (const auto& kv : n["params"]) {
                spec.params[kv.first.as<std::string>()] = kv.second.as<std::string>();
            }
        }

        res.config.nodes.push_back(std::move(spec));
    }

    res.ok = true;
    return res;
}

}  // namespace

ParseResult load_pipeline_config_from_string(const std::string& yaml_content) {
    ParseResult res;
    try {
        YAML::Node root = YAML::Load(yaml_content);
        return parse_yaml_root(root);
    } catch (const YAML::Exception& e) {
        res.error_msg = std::string("YAML syntax error: ") + e.what();
        return res;
    } catch (const std::exception& e) {
        res.error_msg = std::string("Unexpected config parse error: ") + e.what();
        return res;
    }
}

ParseResult load_pipeline_config_from_file(const std::string& file_path) {
    ParseResult res;
    std::ifstream ifs(file_path);
    if (!ifs.is_open()) {
        res.error_msg = "Failed to open config file: " + file_path;
        return res;
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return load_pipeline_config_from_string(oss.str());
}

}  // namespace zeroflow::core