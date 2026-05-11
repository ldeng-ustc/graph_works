cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# for cmake debug
# set(CMAKE_FIND_DEBUG_MODE OFF)

project(LSMGraph
  DESCRIPTION "A High-Performance Dynamic Graph Storage System with Multi-level CSR"
  LANGUAGES C CXX
)

# for search
if(CMAKE_EXPORT_COMPILE_COMMANDS)
  set(CMAKE_CXX_STANDARD_INCLUDE_DIRECTORIES
    ${CMAKE_CXX_IMPLICIT_INCLUDE_DIRECTORIES})
endif()

set(CMAKE_BUILD_TYPE "Release")
# set(CMAKE_BUILD_TYPE "Debug")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to `Release` as none was specified.")
  set(CMAKE_BUILD_TYPE "Release")
endif()

# run cmake in build dir
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)

if(EXISTS "${PATH_TO_CMAKELISTS_TXT}")
  message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\"")
endif()

# sanitizer
if(NOT DEFINED LSMG_SANITIZER)
  set(LSMG_SANITIZER address)
endif()

message("** Build mode: ${CMAKE_BUILD_TYPE}")
# message("${LSMG_SANITIZER} sanitizer will be enabled in debug mode.")

# Compiler flags.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -g -fno-omit-frame-pointer -fno-optimize-sibling-calls")
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Werror -U_FORTIFY_SOURCE")
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused-parameter -Wno-attributes")
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive -fopenmp")

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

# Output directory.
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

include(FetchContent)

set(LSMG_FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/_deps")
set(FETCHCONTENT_BASE_DIR "${LSMG_FETCHCONTENT_BASE_DIR}" CACHE PATH "Base directory for fetched third-party dependencies" FORCE)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "Avoid updating already fetched dependencies" FORCE)

find_package(fmt REQUIRED)
find_package(TBB CONFIG QUIET)
if(NOT TBB_FOUND)
  find_package(TBB REQUIRED MODULE)
endif()
# find_package(OpenMP REQUIRED)
find_package(gflags REQUIRED)

option(LSMG_FETCH_SPDLOG "Fetch pinned spdlog into the build tree" ON)
set(LSMG_SPDLOG_GIT_TAG "v1.12.0" CACHE STRING "Pinned spdlog version")

if(LSMG_FETCH_SPDLOG)
  set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "Use external fmt with spdlog" FORCE)
  set(SPDLOG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_EXAMPLE_HO OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_TESTS_HO OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
  set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

  set(LSMG_SPDLOG_SOURCE_DIR "${FETCHCONTENT_BASE_DIR}/spdlog-src")
  set(LSMG_SPDLOG_BINARY_DIR "${FETCHCONTENT_BASE_DIR}/spdlog-build")
  set(LSMG_SPDLOG_INCLUDE_DIR "${LSMG_SPDLOG_SOURCE_DIR}/include")
  if(EXISTS "${LSMG_SPDLOG_SOURCE_DIR}/include/spdlog/spdlog.h")
    message(STATUS "Reusing fetched spdlog from ${LSMG_SPDLOG_SOURCE_DIR}")
    add_subdirectory("${LSMG_SPDLOG_SOURCE_DIR}" "${LSMG_SPDLOG_BINARY_DIR}" EXCLUDE_FROM_ALL)
  else()
    message(STATUS "Fetching spdlog ${LSMG_SPDLOG_GIT_TAG} into ${LSMG_SPDLOG_SOURCE_DIR}")
    FetchContent_Declare(
      spdlog
      GIT_REPOSITORY https://github.com/gabime/spdlog.git
      GIT_TAG ${LSMG_SPDLOG_GIT_TAG}
      GIT_SHALLOW TRUE
      SOURCE_DIR "${LSMG_SPDLOG_SOURCE_DIR}"
      BINARY_DIR "${LSMG_SPDLOG_BINARY_DIR}"
    )
    FetchContent_Populate(spdlog)
    add_subdirectory("${spdlog_SOURCE_DIR}" "${spdlog_BINARY_DIR}" EXCLUDE_FROM_ALL)
  endif()
else()
  find_package(spdlog REQUIRED)
endif()

if(TARGET fmt::fmt)
  set(LSMG_FMT_TARGET fmt::fmt)
elseif(TARGET fmt)
  set(LSMG_FMT_TARGET fmt)
else()
  message(FATAL_ERROR "fmt target not found")
endif()

if(TARGET spdlog::spdlog)
  set(LSMG_SPDLOG_TARGET spdlog::spdlog)
elseif(TARGET spdlog)
  set(LSMG_SPDLOG_TARGET spdlog)
else()
  message(FATAL_ERROR "spdlog target not found")
endif()

if(NOT LSMG_SPDLOG_INCLUDE_DIR)
  get_target_property(LSMG_SPDLOG_INCLUDE_DIRS ${LSMG_SPDLOG_TARGET} INTERFACE_INCLUDE_DIRECTORIES)
  if(LSMG_SPDLOG_INCLUDE_DIRS)
    list(GET LSMG_SPDLOG_INCLUDE_DIRS 0 LSMG_SPDLOG_INCLUDE_DIR)
  endif()
endif()

if(TARGET TBB::tbb)
  set(LSMG_TBB_TARGET TBB::tbb)
elseif(TBB_LIBRARIES)
  add_library(lsmg_tbb_vendor INTERFACE)
  target_link_libraries(lsmg_tbb_vendor INTERFACE ${TBB_LIBRARIES})
  if(TBB_INCLUDE_DIRS)
    target_include_directories(lsmg_tbb_vendor INTERFACE ${TBB_INCLUDE_DIRS})
  endif()
  set(LSMG_TBB_TARGET lsmg_tbb_vendor)
elseif(TARGET tbb)
  set(LSMG_TBB_TARGET tbb)
else()
  message(FATAL_ERROR "TBB target not found")
endif()

if(TARGET gflags::gflags)
  set(LSMG_GFLAGS_TARGET gflags::gflags)
elseif(gflags_LIBRARIES)
  add_library(lsmg_gflags_vendor INTERFACE)
  target_link_libraries(lsmg_gflags_vendor INTERFACE ${gflags_LIBRARIES})
  if(gflags_INCLUDE_DIRS)
    target_include_directories(lsmg_gflags_vendor INTERFACE ${gflags_INCLUDE_DIRS})
  endif()
  set(LSMG_GFLAGS_TARGET lsmg_gflags_vendor)
elseif(TARGET gflags)
  set(LSMG_GFLAGS_TARGET gflags)
else()
  message(FATAL_ERROR "gflags target not found")
endif()

if(TBB_FOUND)
  message("tbb include dir: ${TBB_INCLUDE_DIR}")
endif()

# for nix 
# if (OpenMP_FOUND AND NOT OpenMP_CXX_INCLUDE_DIRS)
#   set(OpenMP_CXX_INCLUDE_DIRS $ENV{OMP_INCLUDE_PATH})
#   set(OpenMP_LIB_DIRS $ENV{OMP_LIB_PATH})
#   message("omp include dirs: ${OpenMP_CXX_INCLUDE_DIRS}")
#   message("omp lib dirs: ${OpenMP_LIB_DIRS}")
# endif()

# spdlog use external fmt library
add_definitions(-DSPDLOG_FMT_EXTERNAL)

# Includes.
set(LSMG_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src)
set(LSMG_APP_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/app)
set(LSMG_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)

include_directories(${LSMG_SRC_INCLUDE_DIR} ${LSMG_APP_INCLUDE_DIR} ${LSMG_TEST_INCLUDE_DIR})
if(LSMG_SPDLOG_INCLUDE_DIR)
  include_directories(${LSMG_SPDLOG_INCLUDE_DIR})
endif()

# link_directories(${OpenMP_LIB_DIRS})

add_subdirectory(src)
add_subdirectory(benchmark)
