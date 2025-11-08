# Set the minimum required version of CMake and project details
cmake_minimum_required(VERSION 3.22)
project(ColumnarDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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