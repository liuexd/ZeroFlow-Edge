#include <gtest/gtest.h>

#include "zeroflow/core/dag_validator.hpp"
#include "zeroflow/core/pipeline_config.hpp"

using namespace zeroflow::core;

TEST(DagParserTest, ValidDiamondFanoutPipeline) {
    const std::string yaml = R"(
pipeline_name: diamond_demo
shm_pool:
  pool_name: test_pool
  slot_count: 16
  slot_size_mb: 8
nodes:
  - id: capture
    executable: ./cap
    outputs: [raw_frame]
  - id: infer
    executable: ./inf
    inputs:
      img: capture/raw_frame
    outputs: [det_meta]
  - id: recorder
    executable: ./rec
    inputs:
      img: capture/raw_frame
      meta: infer/det_meta
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok) << parsed.error_msg;

    auto dag = validate_and_sort_dag(parsed.config);
    ASSERT_TRUE(dag.ok) << dag.error_msg;
    ASSERT_EQ(dag.topo_order.size(), 3u);
    EXPECT_EQ(dag.topo_order[0], "capture");
    EXPECT_EQ(dag.topo_order[1], "infer");
    EXPECT_EQ(dag.topo_order[2], "recorder");

    // capture/raw_frame 扇出度为 2，mask 应为 0b11 (0x3)
    ASSERT_FALSE(dag.port_fanouts.empty());
    EXPECT_EQ(dag.port_fanouts[0].node_id, "capture");
    EXPECT_EQ(dag.port_fanouts[0].fanout_count, 2u);
    EXPECT_EQ(dag.port_fanouts[0].initial_subscriber_mask, 0x03u);
}

TEST(DagParserTest, ValidLinearPipeline) {
    const std::string yaml = R"(
nodes:
  - id: n1
    executable: ./n1
    outputs: [out]
  - id: n2
    executable: ./n2
    inputs: {in: n1/out}
    outputs: [out]
  - id: n3
    executable: ./n3
    inputs: {in: n2/out}
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    ASSERT_TRUE(dag.ok);
    EXPECT_EQ(dag.topo_order, (std::vector<std::string>{"n1", "n2", "n3"}));
}

TEST(DagParserTest, ValidMergePipeline) {
    const std::string yaml = R"(
nodes:
  - id: cam_a
    executable: ./cam
    outputs: [f]
  - id: cam_b
    executable: ./cam
    outputs: [f]
  - id: stitcher
    executable: ./stitch
    inputs:
      left: cam_a/f
      right: cam_b/f
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    ASSERT_TRUE(dag.ok);
    EXPECT_EQ(dag.topo_order.back(), "stitcher");
}

TEST(DagParserTest, RejectSelfLoop) {
    const std::string yaml = R"(
nodes:
  - id: self_node
    executable: ./self
    inputs: {in: self_node/out}
    outputs: [out]
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("self_node -> self_node"), std::string::npos);
}

TEST(DagParserTest, RejectTwoNodeCycle) {
    const std::string yaml = R"(
nodes:
  - id: node_a
    executable: ./a
    inputs: {in: node_b/out}
    outputs: [out]
  - id: node_b
    executable: ./b
    inputs: {in: node_a/out}
    outputs: [out]
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("DAG cycle detected"), std::string::npos);
}

TEST(DagParserTest, RejectThreeNodeCycleWithPath) {
    const std::string yaml = R"(
nodes:
  - id: src
    executable: ./src
    outputs: [out]
  - id: A
    executable: ./a
    inputs: {in1: src/out, in2: C/out}
    outputs: [out]
  - id: B
    executable: ./b
    inputs: {in: A/out}
    outputs: [out]
  - id: C
    executable: ./c
    inputs: {in: B/out}
    outputs: [out]
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("A -> B -> C -> A"), std::string::npos);
}

TEST(DagParserTest, RejectDuplicateNodeId) {
    const std::string yaml = R"(
nodes:
  - id: dup_node
    executable: ./a
  - id: dup_node
    executable: ./b
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("Duplicate node ID"), std::string::npos);
}

TEST(DagParserTest, RejectDanglingUpstreamNode) {
    const std::string yaml = R"(
nodes:
  - id: consumer
    executable: ./c
    inputs: {in: ghost_node/frame}
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("ghost_node"), std::string::npos);
}

TEST(DagParserTest, RejectDanglingUpstreamPort) {
    const std::string yaml = R"(
nodes:
  - id: producer
    executable: ./p
    outputs: [video_out]
  - id: consumer
    executable: ./c
    inputs: {in: producer/wrong_port_name}
)";
    auto parsed = load_pipeline_config_from_string(yaml);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("wrong_port_name"), std::string::npos);
}

TEST(DagParserTest, RejectFanoutOver32AndMalformedYaml) {
    // 1. 语法错误 YAML 不能抛异常崩溃
    auto bad_yaml = load_pipeline_config_from_string("nodes: [unclosed_bracket");
    EXPECT_FALSE(bad_yaml.ok);

    // 2. 扇出超过 32 个订阅者应被拦截
    std::string big_fanout = "nodes:\n  - id: pub\n    executable: ./p\n    outputs: [out]\n";
    for (int i = 0; i < 33; ++i) {
        big_fanout += "  - id: sub_" + std::to_string(i) +
                      "\n    executable: ./s\n    inputs: {in: pub/out}\n";
    }
    auto parsed = load_pipeline_config_from_string(big_fanout);
    ASSERT_TRUE(parsed.ok);
    auto dag = validate_and_sort_dag(parsed.config);
    EXPECT_FALSE(dag.ok);
    EXPECT_NE(dag.error_msg.find("max fanout limit"), std::string::npos);
}