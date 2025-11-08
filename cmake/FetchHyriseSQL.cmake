# FetchHyriseSQL.cmake — wraps Hyrise SQL parser as target: hsql
# Options you can override BEFORE include():
#   - HSQL_GIT_TAG: commit/tag to pin (default: a known-good commit)
#   - HSQL_LOCAL: path to local checkout to avoid network
#   - FETCHCONTENT_FULLY_DISCONNECTED: honor global offline builds

if (TARGET hsql)
  return()  # already set up
endif()

include(FetchContent)

# default pin: replace with your preferred commit/tag later
if (NOT DEFINED HSQL_GIT_TAG)
  set(HSQL_GIT_TAG main) # example commit
endif()

# Local override wins (put a copy at third_party/hyrise-sql-parser to go fully vendored)
if (NOT DEFINED HSQL_LOCAL)
  set(HSQL_LOCAL "${CMAKE_SOURCE_DIR}/third_party/hyrise-sql-parser")
endif()

if (EXISTS "${HSQL_LOCAL}/src/SQLParser.h")
  set(hsql_src_SOURCE_DIR "${HSQL_LOCAL}")
else()
  FetchContent_Declare(hsql_src
    GIT_REPOSITORY https://github.com/hyrise/sql-parser.git
    GIT_TAG        ${HSQL_GIT_TAG}
    GIT_SHALLOW    TRUE
    UPDATE_DISCONNECTED TRUE
  )
  FetchContent_MakeAvailable(hsql_src)
endif()

# Collect pregenerated sources
file(GLOB_RECURSE HSQL_SOURCES
  "${hsql_src_SOURCE_DIR}/src/*.cpp"
)

add_library(hsql STATIC ${HSQL_SOURCES})
target_include_directories(hsql PUBLIC "${hsql_src_SOURCE_DIR}/src")

# Mark as system to quiet external warnings (optional)
if (COMMAND target_system_include_directories)
  target_system_include_directories(hsql PUBLIC "${hsql_src_SOURCE_DIR}/src")
endif()
