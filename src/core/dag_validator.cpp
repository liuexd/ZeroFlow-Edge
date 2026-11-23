#include "zeroflow/core/dag_validator.hpp"

#include <algorithm>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#include "zeroflow/ipc/shm_arena.hpp"

namespace zeroflow::core {
namespace {

// 0 = 未访问, 1 = 正在当前 DFS 递归栈中, 2 = 已完成搜索
bool find_cycle_dfs(std::size_t u,
                    const std::vector<std::vector<std::size_t>>& adj,
                    const std::vector<NodeSpec>& nodes,
                    std::vector<uint8_t>& color,
                    std::vector<std::string>& path_stack,
                    std::vector<std::string>& out_cycle) {
    color[u] = 1;
    path_stack.push_back(nodes[u].id);

    for (std::size_t v : adj[u]) {
        if (color[v] == 1) {
            auto it = std::find(path_stack.begin(), path_stack.end(), nodes[v].id);
            out_cycle.assign(it, path_stack.end());
            out_cycle.push_back(nodes[v].id);
            return true;
        }
        if (color[v] == 0) {
            if (find_cycle_dfs(v, adj, nodes, color, path_stack, out_cycle)) {
                return true;
            }
        }
    }

    path_stack.pop_back();
    color[u] = 2;
    return false;
}

}  // namespace

DagValidationResult validate_and_sort_dag(PipelineConfig& config) {
    DagValidationResult result;
    const std::size_t n = config.nodes.size();
    if (n == 0) {
        result.error_msg = "Pipeline contains no nodes";
        return result;
    }

    // 1. 节点 ID 唯一性校验与索引映射
    std::unordered_map<std::string, std::size_t> node_index_map;
    std::vector<std::unordered_set<std::string>> node_outputs(n);

    for (std::size_t i = 0; i < n; ++i) {
        const auto& node = config.nodes[i];
        if (!node_index_map.emplace(node.id, i).second) {
            result.error_msg = "Duplicate node ID detected: '" + node.id + "'";
            return result;
        }

        for (const auto& out_port : node.outputs) {
            if (!node_outputs[i].insert(out_port).second) {
                result.error_msg = "Duplicate output port '" + out_port +
                                   "' in node '" + node.id + "'";
                return result;
            }
        }

        std::unordered_set<std::string> seen_inputs;
        for (const auto& in_port : node.inputs) {
            if (!seen_inputs.insert(in_port.local_port).second) {
                result.error_msg = "Duplicate input port '" + in_port.local_port +
                                   "' in node '" + node.id + "'";
                return result;
            }
        }
    }

    // 2. 端口引用完整性校验、邻接表构建与订阅者位掩码预计算
    std::vector<std::vector<std::size_t>> adj(n);
    std::vector<std::unordered_set<std::size_t>> unique_edges(n);
    std::vector<uint32_t> in_degree(n, 0);

    // key: "<node_id>/<output_port>" -> PortFanoutInfo 索引
    std::unordered_map<std::string, std::size_t> fanout_lookup;

    for (std::size_t i = 0; i < n; ++i) {
        const auto& node = config.nodes[i];
        for (const auto& out_port : node.outputs) {
            const std::string key = node.id + "/" + out_port;
            PortFanoutInfo info;
            info.node_id = node.id;
            info.port_name = out_port;
            fanout_lookup[key] = result.port_fanouts.size();
            result.port_fanouts.push_back(std::move(info));
        }
    }

    for (std::size_t dst_idx = 0; dst_idx < n; ++dst_idx) {
        auto& dst_node = config.nodes[dst_idx];
        for (auto& binding : dst_node.inputs) {
            const auto up_it = node_index_map.find(binding.upstream_node);
            if (up_it == node_index_map.end()) {
                result.error_msg = "Dangling upstream node reference in node '" + dst_node.id +
                                   "' input '" + binding.local_port + "': node '" +
                                   binding.upstream_node + "' does not exist";
                return result;
            }

            const std::size_t src_idx = up_it->second;
            if (node_outputs[src_idx].find(binding.upstream_port) == node_outputs[src_idx].end()) {
                result.error_msg = "Dangling upstream port reference in node '" + dst_node.id +
                                   "' input '" + binding.local_port + "': port '" +
                                   binding.upstream_node + "/" + binding.upstream_port +
                                   "' is not declared";
                return result;
            }

            const std::string port_key = binding.upstream_node + "/" + binding.upstream_port;
            auto& fanout_info = result.port_fanouts[fanout_lookup.at(port_key)];
            if (fanout_info.fanout_count >= ipc::kMaxSubscribersPerPort) {
                result.error_msg = "Port '" + port_key + "' exceeds max fanout limit of " +
                                   std::to_string(ipc::kMaxSubscribersPerPort) + " subscribers";
                return result;
            }

            const uint32_t assigned_sub_id = fanout_info.fanout_count;
            binding.subscriber_id = assigned_sub_id;
            fanout_info.fanout_count += 1;
            fanout_info.initial_subscriber_mask |= (1u << assigned_sub_id);
            fanout_info.subscribers.push_back(dst_node.id + ":" + binding.local_port);

            if (unique_edges[src_idx].insert(dst_idx).second) {
                adj[src_idx].push_back(dst_idx);
                in_degree[dst_idx] += 1;
            }
        }
    }

    // 3. Kahn 算法拓扑排序（按节点声明顺序稳定出队）
    std::deque<std::size_t> ready_q;
    for (std::size_t i = 0; i < n; ++i) {
        if (in_degree[i] == 0) {
            ready_q.push_back(i);
        }
    }

    while (!ready_q.empty()) {
        const std::size_t u = ready_q.front();
        ready_q.pop_front();
        result.topo_order.push_back(config.nodes[u].id);

        for (std::size_t v : adj[u]) {
            in_degree[v] -= 1;
            if (in_degree[v] == 0) {
                ready_q.push_back(v);
            }
        }
    }

    // 4. 若拓扑序节点数小于总节点数，说明存在有向环，DFS 定位具体成环链路
    if (result.topo_order.size() < n) {
        std::vector<uint8_t> color(n, 0);
        std::vector<std::string> path_stack;
        for (std::size_t i = 0; i < n; ++i) {
            if (color[i] == 0 &&
                find_cycle_dfs(i, adj, config.nodes, color, path_stack, result.cycle_path)) {
                break;
            }
        }

        std::string chain;
        for (std::size_t i = 0; i < result.cycle_path.size(); ++i) {
            if (i > 0) {
                chain += " -> ";
            }
            chain += result.cycle_path[i];
        }
        result.error_msg = "DAG cycle detected: " + chain;
        result.topo_order.clear();
        return result;
    }

    result.ok = true;
    return result;
}

}  // namespace zeroflow::core