#!/usr/bin/env bash
set -euo pipefail

echo "=== [1/4] 安装 C++17 工具链、配置库、多媒体与测试依赖 ==="
sudo apt update
sudo apt install -y \
    build-essential cmake ninja-build pkg-config git gdb \
    libyaml-cpp-dev libgtest-dev valgrind \
    libavcodec-dev libavformat-dev libavutil-dev libswscale-dev \
    ffmpeg v4l-utils

echo "=== [2/4] 校验编译器与 CMake 版本 ==="
g++ --version | head -n 1
cmake --version | head -n 1

echo "=== [3/4] 校验 /dev/shm 容量与 RK3588 硬件加速节点 ==="
df -h /dev/shm
ls -l /dev/dma_heap/ /dev/mpp_service /dev/dri/renderD129 2>/dev/null || true

echo "=== [4/4] 检查 Cortex-A76 大核编号与 perf 工具 ==="
lscpu -e=CPU,CORE,MAXMHZ
if ! command -v perf >/dev/null 2>&1; then
    FOUND_PERF=$(find /usr/lib/linux-tools* -name perf 2>/dev/null | head -n 1 || true)
    if [[ -n "${FOUND_PERF}" ]]; then
        echo "发现可用 perf: ${FOUND_PERF}，创建软链接至 /usr/local/bin/perf"
        sudo ln -sf "${FOUND_PERF}" /usr/local/bin/perf
    else
        echo "[WARN] 未找到预装 perf，如需硬件 PMU 采样可后续从对应内核源码 tools/perf 编译"
    fi
fi

PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
echo "当前 kernel.perf_event_paranoid = ${PARANOID} (压测前可执行 sudo sysctl -w kernel.perf_event_paranoid=-1)"
echo "=== Day 1 环境基线检查完成 ==="