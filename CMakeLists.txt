# Set the minimum required version of CMake and project details
cmake_minimum_required(VERSION 3.22)
project(ColumnarDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# When using Homebrew LLVM on macOS, the compiler uses its own (newer) libc++
# headers but the linker defaults to the SDK's older libc++, producing
# undefined symbols like std::__1::__hash_memory. Point the linker at
# Homebrew LLVM's libc++ to keep header/runtime ABI in sync.
if (APPLE AND CMAKE_CXX_COMPILER MATCHES "/opt/homebrew/.*/llvm/.*/clang\\+\\+$")
  get_filename_component(_llvm_bin_dir "${CMAKE_CXX_COMPILER}" DIRECTORY)
  get_filename_component(_llvm_prefix "${_llvm_bin_dir}" DIRECTORY)
  set(_llvm_cxx_libdir "${_llvm_prefix}/lib/c++")
  if (EXISTS "${_llvm_cxx_libdir}/libc++.dylib")
    add_link_options("-L${_llvm_cxx_libdir}" "-Wl,-rpath,${_llvm_cxx_libdir}")
  endif()
endif()

# Reuse fetched dependencies across builds by storing them in the source tree
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/.deps")

# --- Warnings ---
if (MSVC)
  add_compile_options(/permissive- /W4)
else()
  add_compile_options(-Wall -Wextra -Wshadow -Wpedantic)
endif()

# --- Dependencies ---
include(FetchContent)

# Hyrise SQL Parser, wrapped in a helper for cleanliness
include(cmake/FetchHyriseSQL.cmake)

# A central INTERFACE library to bundle all common dependencies.
# Any internal target can link against this to get access to 3rd-party libs and includes.
add_library(columnar_db_deps INTERFACE)

# Add our project's own include directory to the interface
target_include_directories(columnar_db_deps INTERFACE
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
)

# Link all common third-party dependencies to our interface target
target_link_libraries(columnar_db_deps INTERFACE
  hsql # Our custom target for the Hyrise SQL Parser
)

# --- Subdirectories ---
# Add the source directory which contains all our libraries and the executable
add_subdirectory(src)

# Enable testing at the top level so `ctest` from the build root finds
# tests registered by gtest_discover_tests in subdirectories.
enable_testing()
add_subdirectory(tests)