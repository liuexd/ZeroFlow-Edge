cmake_minimum_required(VERSION 3.20)
project(ZeroFlowEdge VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ZEROFLOW_BUILD_TESTS "Build unit tests and benchmark targets" ON)
option(ZEROFLOW_ENABLE_ASAN "Enable AddressSanitizer and UBSan" ON)

# 全局严格编译告警
add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Werror=return-type
)

# 基准压测构建(-DCMAKE_BUILD_TYPE=Release)时应显式关闭 ASan
if(ZEROFLOW_ENABLE_ASAN AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "ZeroFlow-Edge: ASan + UBSan ENABLED")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
else()
    message(STATUS "ZeroFlow-Edge: Sanitizers DISABLED (Release / Benchmark mode)")
endif()

find_package(Threads REQUIRED)
find_package(yaml-cpp REQUIRED)

# 公共头文件接口目标（Day 1 契约头先行）
add_library(zeroflow_headers INTERFACE)
target_include_directories(zeroflow_headers INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(zeroflow_headers INTERFACE
    Threads::Threads
    rt
)

# Day 1 契约与 ARM64 硬件原子特性验证程序
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
endif()