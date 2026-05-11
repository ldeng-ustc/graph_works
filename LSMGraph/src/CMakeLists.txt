add_subdirectory(buffer)
add_subdirectory(storage)
add_subdirectory(cache)
add_subdirectory(compaction)
add_subdirectory(index)
add_subdirectory(version)

add_library(lsmgraph STATIC lsmgraph.cpp common/flags.cpp)

set(LSMG_LIBS
  lsmgraph_cache
  lsmgraph_buffer
  lsmgraph_storage_disk
  lsmgraph_storage_page
  lsmgraph_compaction
  lsmgraph_index
  lsmgraph_version
)

set(LSMG_THIRDPARTY_LIBS
  ${LSMG_FMT_TARGET}
  ${LSMG_SPDLOG_TARGET}
  ${LSMG_TBB_TARGET}
  ${LSMG_GFLAGS_TARGET}
  # omp
  # tcmalloc
)


target_link_libraries(
  lsmgraph
  ${LSMG_LIBS}
  ${LSMG_THIRDPARTY_LIBS})

target_include_directories(
  lsmgraph PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
