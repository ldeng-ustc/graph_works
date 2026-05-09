cmake_minimum_required(VERSION 3.14)

if (POLICY CMP0141)
  cmake_policy(SET CMP0141 NEW)
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<IF:$<AND:$<C_COMPILER_ID:MSVC>,$<CXX_COMPILER_ID:MSVC>>,$<$<CONFIG:Debug,RelWithDebInfo>:EditAndContinue>,$<$<CONFIG:Debug,RelWithDebInfo>:ProgramDatabase>>")
endif()

SET(CMAKE_C_COMPILER "gcc-12")
SET(CMAKE_CXX_COMPILER "g++-12")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

project ("BACH_demo" LANGUAGES CXX)

include(FetchContent)
find_package(OpenMP REQUIRED)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

include_directories(${PROJECT_SOURCE_DIR})

# Keep third-party sources in the build tree and avoid update checks on
# subsequent re-configures. Delete the corresponding directories under
# `${CMAKE_BINARY_DIR}/_deps` when you want to refresh the pinned versions.
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)

set(DYNAMICBITSET_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(DYNAMICBITSET_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(DYNAMICBITSET_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(DYNAMICBITSET_FORMAT_TARGET OFF CACHE BOOL "" FORCE)
set(DYNAMICBITSET_HEADERS_TARGET_IDE OFF CACHE BOOL "" FORCE)
set(DYNAMICBITSET_INSTALL OFF CACHE BOOL "" FORCE)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_BROKEN_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_HANGING_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_SLOW_TESTS OFF CACHE BOOL "" FORCE)

# Force folly's `find_package(Boost ... MODULE)` calls to use CMake's
# traditional FindBoost module against the system Boost installation
# instead of picking up an unrelated BoostConfig.cmake from /usr/local.
unset(Boost_DIR CACHE)
unset(Boost_ROOT CACHE)
set(Boost_NO_BOOST_CMAKE ON CACHE BOOL "" FORCE)
set(Boost_NO_SYSTEM_PATHS OFF CACHE BOOL "" FORCE)
set(BOOST_ROOT /usr CACHE PATH "" FORCE)
set(BOOST_INCLUDEDIR /usr/include CACHE PATH "" FORCE)
set(BOOST_LIBRARYDIR /usr/lib/x86_64-linux-gnu CACHE PATH "" FORCE)

FetchContent_Declare(
  dynamic_bitset
  GIT_REPOSITORY https://github.com/pinam45/dynamic_bitset.git
  GIT_TAG v1.3.3
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  folly
  GIT_REPOSITORY https://github.com/facebook/folly.git
  GIT_TAG v2024.10.28.00
  GIT_SHALLOW TRUE
)

FetchContent_MakeAvailable(dynamic_bitset folly)

file(GLOB_RECURSE srcs CONFIGURE_DEPENDS src/*.cpp)

add_library(bach SHARED bind/BACH.cpp ${srcs})
target_include_directories(bach PUBLIC include)
target_link_libraries(bach PUBLIC sul::dynamic_bitset folly)
target_compile_features(bach PUBLIC cxx_std_20)

add_executable(bach_bin32_bench bach_bin32_bench.cpp)
target_include_directories(bach_bin32_bench PRIVATE include)
target_compile_features(bach_bin32_bench PRIVATE cxx_std_20)
target_link_libraries(bach_bin32_bench PRIVATE bach OpenMP::OpenMP_CXX)

set(DEBUG OFF)

set(CMAKE_CXX_FLAGS "-O3 ${CMAKE_CXX_FLAGS}")

if(DEBUG)
	set(CMAKE_CXX_FLAGS " -Wall ${CMAKE_CXX_FLAGS}")
	target_link_options(bach PRIVATE -lprofiler -ltcmalloc)
endif()
