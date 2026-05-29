# pesdb

Learning project: building a columnar SQL store from scratch to understand database internals.

## Prerequisites

- CMake 3.22+
- C++20 compatible compiler (GCC 10+, Clang 12+, or MSVC 2019+)
- Git (for fetching dependencies)

## Quick Start

```bash
# Create build directory
mkdir -p build
cd build

# Configure
cmake ..

# Build
cmake --build .

# Run tests
ctest --output-on-failure
```

## Build Options

```bash
# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release build
cmake -DCMAKE_BUILD_TYPE=Release ..

# Specify compiler
cmake -DCMAKE_CXX_COMPILER=clang++ ..
```

## Running Tests

```bash
# All tests
cd build
ctest --output-on-failure

# Specific test
./tests/unit/storage_tests

# Verbose output
ctest -V
```

## Project Structure

```
src/
  storage/         -> Storage layer implementation
  main/            -> CLI executable
  common/          -> Common utilities
include/
  storage/         -> Storage headers
  common/          -> Common headers
tests/
  unit/            -> Google Test unit tests
doc/
  design/          -> Module design docs (.adoc)
  learnings/       -> Deep dives on topics
```

## Dependencies

Auto-fetched via CMake FetchContent:
- Hyrise SQL Parser -> SQL parsing
- Google Test -> Unit testing

Dependencies cached in `.deps/` to speed up rebuilds.

## Development Workflow

1. Make code changes in `src/` or `include/`
2. Rebuild: `cmake --build build`
3. Run tests: `cd build && ctest --output-on-failure`
4. Verify your changes work

## Common Issues

**Build fails with "C++20 required"**
-> Update your compiler or specify a newer one with `-DCMAKE_CXX_COMPILER`

**Tests fail after code changes**
-> Rebuild first: `cmake --build build`

**Clean build needed**
-> `rm -rf build && mkdir build && cd build && cmake ..`

## Current Status

Active modules:
- Storage layer (page management, disk I/O, WAL support)

Planned modules (not yet implemented):
- Recovery (WAL replay, checkpointing)
- Concurrency (transactions, MVCC)
- Execution (query operators)
- Query engine

## Learning Resources

Check `doc/design/` for detailed design documents on implemented modules.
Check `doc/learnings/` for deep dives on specific topics.
