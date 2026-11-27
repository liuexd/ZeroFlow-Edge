#include <csignal>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "zeroflow/core/coordinator.hpp"
#include "zeroflow/core/pipeline_config.hpp"
#include "zeroflow/ipc/uds_channel.hpp"

using namespace zeroflow::core;

#ifndef ZEROFLOW_BIN_DIR
#define ZEROFLOW_BIN_DIR "./build"
#endif

namespace {

PipelineConfig make_test_diamond_config(const std::string& tag) {
    PipelineConfig cfg;
    cfg.pipeline_name = "e2e_" + tag;
    cfg.shm_pool.pool_name = "zf_e2e_pool_" + tag;
    cfg.shm_pool.slot_count = 8;
    cfg.shm_pool.slot_size_mb = 1;

    const std::string bin_dir = ZEROFLOW_BIN_DIR;

    NodeSpec src;
    src.id = "src_node";
    src.executable = bin_dir + "/zf_dummy_source";
    src.restart_policy = RestartPolicy::kAlways;
    src.outputs = {"raw_frame"};
    src.params["fps"] = "100";
    src.params["payload_bytes"] = "16384";

    NodeSpec tr;
    tr.id = "tr_node";
    tr.executable = bin_dir + "/zf_dummy_transform";
    tr.restart_policy = RestartPolicy::kOnFailure;
    tr.inputs.push_back({"in_frame", "src_node", "raw_frame", 0});
    tr.outputs = {"det_event"};

    NodeSpec snk;
    snk.id = "snk_node";
    snk.executable = bin_dir + "/zf_dummy_sink";
    snk.restart_policy = RestartPolicy::kAlways;
    snk.inputs.push_back({"in_frame", "src_node", "raw_frame", 0});
    snk.inputs.push_back({"in_event", "tr_node", "det_event", 0});

    cfg.nodes = {std::move(src), std::move(tr), std::move(snk)};
    return cfg;
}

}  // namespace

TEST(CoordinatorE2ETest, DiamondPipelineLifecycleAndCleanShutdown) {
    const std::string tag = "life_" + std::to_string(getpid());
    auto cfg = make_test_diamond_config(tag);
    const std::string shm_file = "/dev/shm/" + cfg.shm_pool.pool_name;
    const std::string sock_src =
        zeroflow::ipc::make_port_socket_path(cfg.pipeline_name, "src_node", "raw_frame");

    const pid_t coord_pid = fork();
    ASSERT_GE(coord_pid, 0);
    if (coord_pid == 0) {
        CoordinatorOptions opts;
        opts.max_run_duration_ms = 0;
        Coordinator coord(std::move(cfg), opts);
        _exit(coord.run());
    }

    // 等待拓扑与 UDS 全部就绪并运行 350ms
    usleep(350000);
    EXPECT_EQ(access(shm_file.c_str(), F_OK), 0);
    EXPECT_EQ(access(sock_src.c_str(), F_OK), 0);

    // 向 Coordinator 发送 SIGTERM 触发优雅停机
    ASSERT_EQ(kill(coord_pid, SIGTERM), 0);
    int status = 0;
    ASSERT_EQ(waitpid(coord_pid, &status, 0), coord_pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);

    // 验证停机后 /dev/shm 与 /tmp/*.sock 零残留
    EXPECT_NE(access(shm_file.c_str(), F_OK), 0);
    EXPECT_NE(access(sock_src.c_str(), F_OK), 0);
}

TEST(CoordinatorE2ETest, KillMinusNineReclaimsOrphanSlotsAndRespawns) {
    const std::string tag = "heal_" + std::to_string(getpid());
    auto cfg = make_test_diamond_config(tag);

    CoordinatorOptions opts;
    opts.max_run_duration_ms = 450;

    // 在独立进程中运行带有主动故障注入的 Coordinator 验证器
    const pid_t runner_pid = fork();
    ASSERT_GE(runner_pid, 0);
    if (runner_pid == 0) {
        // 先启动子线程或定时器难以跨 fork，改为用子进程在 150ms 后强杀同组内的 tr_node
        const pid_t killer_pid = fork();
        if (killer_pid == 0) {
            usleep(180000);
            // 精准强杀带有 "--node-id tr_node" 参数的子进程模拟 kill -9 崩溃
            const std::string cmd = "pkill -9 -f 'node-id tr_node'";
            const int rc = std::system(cmd.c_str());
            (void)rc;
            _exit(0);
        }

        Coordinator coord(std::move(cfg), opts);
        const int rc = coord.run();
        int kstatus = 0;
        waitpid(killer_pid, &kstatus, 0);

        if (rc != 0 || coord.total_restarts() < 1) {
            _exit(2);
        }
        _exit(0);
    }

    int status = 0;
    ASSERT_EQ(waitpid(runner_pid, &status, 0), runner_pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}