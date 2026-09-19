add_executable(bench_run_algo bench_run_algo.cpp)
target_link_libraries(bench_run_algo lsmgraph)

add_executable(lsmgraph_bin32_bench lsmgraph_bin32_bench.cpp)
target_link_libraries(lsmgraph_bin32_bench lsmgraph)

add_executable(lsmgraph_oneway_basic_benchmarks oneway_basic_benchmarks.cpp)
target_link_libraries(lsmgraph_oneway_basic_benchmarks lsmgraph)

add_executable(text2bin text2bin.cpp)
add_executable(read_file read_file.cpp)
target_link_libraries(read_file ${LSMG_SPDLOG_TARGET} ${LSMG_FMT_TARGET})

# 顶点稀疏性 bench：内存生成边集 + 单线程 ingest，禁 flush。仅在带
# LSMG_VSPARSITY_NO_FLUSH 宏的独立 build 里构建，默认 build 不受影响。
set(VSPARSITY_DIR /home/denglong.1997/scatter_graph/related_works/vertex_sparsity)
set(GENERATOR_DIR /home/denglong.1997/scatter_graph/generator)
if(EXISTS ${VSPARSITY_DIR}/lsmgraph_vsparsity.cc)
  add_executable(lsmgraph_vsparsity ${VSPARSITY_DIR}/lsmgraph_vsparsity.cc)
  target_include_directories(lsmgraph_vsparsity PRIVATE ${VSPARSITY_DIR} ${GENERATOR_DIR})
  target_link_libraries(lsmgraph_vsparsity lsmgraph ${VSPARSITY_DIR}/libkrongen.a)
endif()
