set(CORE_TOOLS
  sst_dump.cc
  ldb.cc)
if(WITH_CORE_TOOLS)
  foreach(src ${CORE_TOOLS})
    get_filename_component(exename ${src} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX}
      ${src})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
    list(APPEND core_tool_deps ${exename})
  endforeach()
endif()

if(WITH_ASTER_BENCH)
  add_executable(aster_bin32_bench${ARTIFACT_SUFFIX}
    aster_bin32_bench.cc)
  target_link_libraries(aster_bin32_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})

  find_package(OpenMP QUIET)
  if(OpenMP_CXX_FOUND AND TARGET aster_bin32_bench${ARTIFACT_SUFFIX})
    target_link_libraries(aster_bin32_bench${ARTIFACT_SUFFIX} OpenMP::OpenMP_CXX)
  endif()

  list(APPEND tool_deps aster_bin32_bench)
endif()

if(WITH_TOOLS)
  set(TOOLS
    db_sanity_test.cc
    write_stress.cc
    db_repl_stress.cc
    graph_benchmark.cc
    dump/rocksdb_dump.cc
    dump/rocksdb_undump.cc)
  foreach(src ${TOOLS})
    get_filename_component(exename ${src} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX}
      ${src})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
    list(APPEND tool_deps ${exename})
  endforeach()

  add_custom_target(ldb_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/ldb_tests.py
    DEPENDS ldb)
endif()
