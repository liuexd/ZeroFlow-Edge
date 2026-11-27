cmake_minimum_required(VERSION 3.20)
project(ZeroFlowEdge VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ZEROFLOW_BUILD_TESTS "Build unit tests and benchmark targets" ON)
option(ZEROFLOW_ENABLE_ASAN "Enable AddressSanitizer and UBSan" ON)

add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Werror=return-type
)

if(ZEROFLOW_ENABLE_ASAN AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "ZeroFlow-Edge: ASan + UBSan ENABLED")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
else()
    message(STATUS "ZeroFlow-Edge: Sanitizers DISABLED (Release / Benchmark mode)")
endif()

find_package(Threads REQUIRED)
find_package(yaml-cpp REQUIRED)

add_library(zeroflow_headers INTERFACE)
target_include_directories(zeroflow_headers INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(zeroflow_headers INTERFACE
    Threads::Threads
    rt
)

# Day 3 & Day 4 通信底座：POSIX 共享内存池 + UDS SOCK_SEQPACKET 信令通道
add_library(zeroflow_ipc STATIC
    src/ipc/shm_arena.cpp
    src/ipc/uds_channel.cpp
)
target_link_libraries(zeroflow_ipc PUBLIC
    zeroflow_headers
)

# Day 2, 4, 5 核心库：配置解析、DAG 校验、OperatorNode 与 Coordinator 守护框架
add_library(zeroflow_core STATIC
    src/core/pipeline_config.cpp
    src/core/dag_validator.cpp
    src/core/operator_node.cpp
    src/core/node_cli.cpp
    src/core/coordinator.cpp
)
target_link_libraries(zeroflow_core PUBLIC
    zeroflow_headers
    zeroflow_ipc
    yaml-cpp
)

# 控制面可执行文件
add_executable(zf_dag_check
    src/core/zf_dag_check_main.cpp
)
target_link_libraries(zf_dag_check PRIVATE
    zeroflow_core
)

add_executable(zf_coordinator
    src/core/zf_coordinator_main.cpp
)
target_link_libraries(zf_coordinator PRIVATE
    zeroflow_core
)

# Day 5 空壳验证算子进程
add_executable(zf_dummy_source
    src/nodes/zf_dummy_source_main.cpp
)
target_link_libraries(zf_dummy_source PRIVATE
    zeroflow_core
)

add_executable(zf_dummy_transform
    src/nodes/zf_dummy_transform_main.cpp
)
target_link_libraries(zf_dummy_transform PRIVATE
    zeroflow_core
)

add_executable(zf_dummy_sink
    src/nodes/zf_dummy_sink_main.cpp
)
target_link_libraries(zf_dummy_sink PRIVATE
    zeroflow_core
)

if(ZEROFLOW_BUILD_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)

    add_executable(test_day1_contracts
        tests/unit/test_day1_contracts.cpp
    )
    target_link_libraries(test_day1_contracts PRIVATE
        zeroflow_headers
        GTest::gtest_main
    )
    add_test(NAME Day1ContractsTest COMMAND test_day1_contracts)

    add_executable(test_dag_parser
        tests/unit/test_dag_parser.cpp
    )
    target_link_libraries(test_dag_parser PRIVATE
        zeroflow_core
        GTest::gtest_main
    )
    add_test(NAME DagParserTest COMMAND test_dag_parser)

    add_executable(test_shm_allocator
        tests/unit/test_shm_allocator.cpp
    )
    target_link_libraries(test_shm_allocator PRIVATE
        zeroflow_ipc
        GTest::gtest_main
    )
    add_test(NAME ShmAllocatorTest COMMAND test_shm_allocator)

    add_executable(test_uds_node
        tests/unit/test_uds_node.cpp
    )
    target_link_libraries(test_uds_node PRIVATE
        zeroflow_core
        GTest::gtest_main
    )
    add_test(NAME UdsNodeTest COMMAND test_uds_node)

    add_executable(test_coordinator_e2e
        tests/unit/test_coordinator_e2e.cpp
    )
    target_compile_definitions(test_coordinator_e2e PRIVATE
        ZEROFLOW_BIN_DIR="${CMAKE_BINARY_DIR}"
    )
    target_link_libraries(test_coordinator_e2e PRIVATE
        zeroflow_core
        GTest::gtest_main
    )
    add_test(NAME CoordinatorE2ETest COMMAND test_coordinator_e2e)

    add_executable(bench_shm_pool_stress
        tests/benchmark/bench_shm_pool_stress.cpp
    )
    target_link_libraries(bench_shm_pool_stress PRIVATE
        zeroflow_ipc
    )
endif()