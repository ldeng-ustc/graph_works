cmake_minimum_required(VERSION 3.16)
project(e12_livegraph LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(SCATTER_GRAPH_ROOT "" CACHE PATH "ScatterGraph checkout containing the E12 protocol")
if(NOT EXISTS "${SCATTER_GRAPH_ROOT}/include/sg/mixed_workload/workload.h")
  message(FATAL_ERROR "Set SCATTER_GRAPH_ROOT to the shared E12 checkout")
endif()
get_filename_component(BASELINES_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
set(LIVEGRAPH_ROOT "${BASELINES_ROOT}/LiveGraph")
list(APPEND CMAKE_MODULE_PATH "${LIVEGRAPH_ROOT}/cmake")
find_package(Threads REQUIRED)
find_package(OpenMP REQUIRED)
find_package(TBB REQUIRED)
add_library(e12_livegraph_native STATIC
  "${LIVEGRAPH_ROOT}/src/graph.cpp"
  "${LIVEGRAPH_ROOT}/src/transaction.cpp")
target_include_directories(e12_livegraph_native PUBLIC
  "${LIVEGRAPH_ROOT}" "${TBB_INCLUDE_DIRS}"
  "${SCATTER_GRAPH_ROOT}/include" "${BASELINES_ROOT}/oneway_basic_benchmarks")
target_link_libraries(e12_livegraph_native PUBLIC Threads::Threads OpenMP::OpenMP_CXX ${TBB_LIBRARIES})
# Match the native build: its Bloom filter needs AVX2 and its paired counters
# need cmpxchg16b. The build and execution machine are the same NVMe host.
target_compile_options(e12_livegraph_native PUBLIC -march=native PRIVATE -O3)
add_executable(livegraph_mixed_bench main.cpp)
target_link_libraries(livegraph_mixed_bench PRIVATE e12_livegraph_native)
add_executable(livegraph_mixed_test test.cpp)
target_link_libraries(livegraph_mixed_test PRIVATE e12_livegraph_native)
enable_testing()
# The outer caller must hold the shared device lock. Tests require an explicit
# new runtime root, so they cannot silently create graphs in a source/build tree.
