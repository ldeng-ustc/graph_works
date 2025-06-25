# CMakeLists.txt for LiveGraph examples

# Find all example source files
file(GLOB EXAMPLES "*.cpp")

find_package(OpenMP REQUIRED)

# For each example source file
foreach(example_source ${EXAMPLES})
    # Get the filename without extension
    get_filename_component(example_name ${example_source} NAME_WE)
    
    # Add an executable target
    add_executable(${example_name} ${example_source})
    
    # Link against the main LiveGraph library
    target_link_libraries(${example_name} PRIVATE livegraph OpenMP::OpenMP_CXX)

    message(STATUS "C++ compiler ID: ${CMAKE_CXX_COMPILER_ID}")       # 如 GNU, Clang, MSVC
    message(STATUS "C++ compiler version: ${CMAKE_CXX_COMPILER_VERSION}")  # 版本号
    message(STATUS "C++ compiler path: ${CMAKE_CXX_COMPILER}")        # 编译器可执行文件路径

    # C++ standard settings
    set_target_properties(${example_name} PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS YES
    )

endforeach()