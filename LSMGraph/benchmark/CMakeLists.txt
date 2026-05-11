add_executable(bench_run_algo bench_run_algo.cpp)
target_link_libraries(bench_run_algo lsmgraph)

add_executable(lsmgraph_bin32_bench lsmgraph_bin32_bench.cpp)
target_link_libraries(lsmgraph_bin32_bench lsmgraph)

add_executable(text2bin text2bin.cpp)
add_executable(read_file read_file.cpp)
target_link_libraries(read_file ${LSMG_SPDLOG_TARGET} ${LSMG_FMT_TARGET})
