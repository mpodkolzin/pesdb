# PesDB Project Structure

## Directory Layout

```
pesdb/
├── CMakeLists.txt                    # Top-level build configuration
├── .gitignore                        # Ignore build/, .deps/, *.db files
├── README.md                         # Project overview
│
├── cmake/                            # CMake helper modules
│   └── FetchHyriseSQL.cmake          # SQL parser dependency
│
├── include/columnar_db/              # Public headers (mirrored with src/)
│   ├── common/                       # Common types, config, utilities
│   │   ├── config.h                  # PAGE_SIZE, constants
│   │   └── types.h                   # page_id_t, DataType enum, etc.
│   │
│   ├── storage/                      # Storage layer (Phase 1-2)
│   │   ├── disk_manager.h            # Raw file I/O
│   │   ├── page.h                    # In-memory page representation
│   │   ├── buffer_pool_manager.h     # Caching layer with LRU
│   │   ├── column_segment.h          # Single column storage (Phase 2)
│   │   ├── column_table.h            # Multi-column table (Phase 2)
│   │   ├── schema.h                  # Table schema metadata
│   │   └── catalog.h                 # Database catalog (Phase 3)
│   │
│   ├── recovery/                     # WAL & Recovery (Phase 4-5)
│   │   ├── log_record.h              # Log record types
│   │   ├── log_manager.h             # WAL manager
│   │   └── recovery_manager.h        # ARIES recovery
│   │
│   ├── concurrency/                  # MVCC & Transactions (Phase 5-6)
│   │   ├── transaction.h             # Transaction abstraction
│   │   ├── transaction_manager.h     # Transaction lifecycle
│   │   ├── timestamp_manager.h       # Logical clocks
│   │   └── garbage_collector.h       # MVCC cleanup
│   │
│   ├── execution/                    # Query execution (Phase 7-8)
│   │   ├── expression.h              # Expression AST
│   │   ├── executor.h                # Base executor class
│   │   └── executors/                # Concrete operators
│   │       ├── seq_scan_executor.h
│   │       ├── filter_executor.h
│   │       ├── projection_executor.h
│   │       ├── hash_aggregate_executor.h
│   │       └── hash_join_executor.h
│   │
│   └── engine/                       # Top-level engine
│       └── query_executor.h          # SQL statement dispatcher
│
├── src/                              # Implementation files (mirrors include/)
│   ├── common/
│   │   └── CMakeLists.txt
│   │
│   ├── storage/
│   │   ├── CMakeLists.txt
│   │   ├── disk_manager.cpp
│   │   ├── page.cpp
│   │   ├── buffer_pool_manager.cpp
│   │   ├── column_segment.cpp
│   │   ├── column_table.cpp
│   │   ├── schema.cpp
│   │   └── catalog.cpp
│   │
│   ├── recovery/
│   │   ├── CMakeLists.txt
│   │   ├── log_record.cpp
│   │   ├── log_manager.cpp
│   │   └── recovery_manager.cpp
│   │
│   ├── concurrency/
│   │   ├── CMakeLists.txt
│   │   ├── transaction.cpp
│   │   ├── transaction_manager.cpp
│   │   ├── timestamp_manager.cpp
│   │   └── garbage_collector.cpp
│   │
│   ├── execution/
│   │   ├── CMakeLists.txt
│   │   ├── expression.cpp
│   │   └── executors/
│   │       ├── seq_scan_executor.cpp
│   │       ├── filter_executor.cpp
│   │       ├── projection_executor.cpp
│   │       ├── hash_aggregate_executor.cpp
│   │       └── hash_join_executor.cpp
│   │
│   ├── engine/
│   │   ├── CMakeLists.txt
│   │   └── query_executor.cpp
│   │
│   ├── main/
│   │   ├── CMakeLists.txt
│   │   └── main.cpp               # CLI executable
│   │
│   └── CMakeLists.txt              # Top-level src/ CMake
│
├── tests/                          # Unit & integration tests
│   ├── CMakeLists.txt              # Google Test setup
│   ├── unit/                       # Component tests
│   │   ├── storage/
│   │   │   ├── disk_manager_test.cpp
│   │   │   ├── page_test.cpp
│   │   │   └── buffer_pool_manager_test.cpp
│   │   ├── recovery/
│   │   │   └── log_manager_test.cpp
│   │   └── execution/
│   │       └── expression_test.cpp
│   │
│   └── integration/                # End-to-end SQL tests
│       └── sql_test.cpp
│
└── doc/                            # Documentation
    ├── design_exploration.md       # ✅ Complete roadmap
    ├── PROJECT_STRUCTURE.md        # This file
    │
    ├── design/                     # Design docs per component
    │   ├── storage/
    │   │   ├── disk-manager.md
    │   │   ├── buffer-pool.md
    │   │   └── columnar-storage.md
    │   ├── recovery/
    │   │   └── wal.md
    │   └── concurrency/
    │       └── mvcc.md
    │
    └── learnings/                  # Your personal notes
        ├── page-based-storage.md
        ├── wal-protocol.md
        └── mvcc-in-columnar.md
```

---

## Build Artifacts (Gitignored)

```
pesdb/
├── build/                          # CMake build directory
├── .deps/                          # FetchContent cached dependencies
├── .cache/                         # clangd/ccls LSP cache
├── *.db                            # Database files from testing
└── *.wal                           # WAL files from testing
```

---

## CMake Structure

### Top-level `CMakeLists.txt`
- Set C++20 standard
- Configure compiler warnings
- Fetch external dependencies (HyriseSQL, GoogleTest)
- Create `columnar_db_deps` INTERFACE library
- Add subdirectories

### `src/CMakeLists.txt`
- Add all module subdirectories
- Each module has its own `CMakeLists.txt`

### Module `CMakeLists.txt` Pattern
```cmake
# Example: src/storage/CMakeLists.txt
add_library(columnar_db_storage
    disk_manager.cpp
    page.cpp
    buffer_pool_manager.cpp
)

target_link_libraries(columnar_db_storage
    PUBLIC columnar_db_deps
)
```

### Main Executable
```cmake
# src/main/CMakeLists.txt
add_executable(pesdb main.cpp)
target_link_libraries(pesdb
    columnar_db_storage
    columnar_db_engine
)
```

### Tests
```cmake
# tests/CMakeLists.txt
enable_testing()
add_subdirectory(unit)
add_subdirectory(integration)
```

---

## Development Workflow

### Phase-by-Phase Implementation

1. **Phase 1** (Weeks 1-2): Implement `storage/` (disk_manager, page, buffer_pool_manager)
2. **Phase 2** (Weeks 3-4): Add `storage/` columnar (column_segment, column_table)
3. **Phase 3** (Week 5): Add `storage/catalog` + `engine/query_executor`
4. **Phase 4-5** (Weeks 6-9): Implement `recovery/` (log_manager, recovery_manager)
5. **Phase 6** (Weeks 10-12): Implement `concurrency/` (transaction_manager, MVCC)
6. **Phase 7-8** (Weeks 13-16): Implement `execution/` (operators, vectorization)

### Test-Driven Development
- Write unit test FIRST (TDD)
- Implement component
- Test passes → move to next component
- Integration tests after phase completion

---

## Naming Conventions

### Files
- **Headers**: `snake_case.h` (e.g., `disk_manager.h`)
- **Implementation**: `snake_case.cpp` (e.g., `disk_manager.cpp`)
- **Tests**: `component_test.cpp` (e.g., `disk_manager_test.cpp`)

### Code
- **Classes**: `PascalCase` (e.g., `DiskManager`, `BufferPoolManager`)
- **Functions**: `PascalCase` (e.g., `ReadPage()`, `AllocatePage()`)
- **Variables**: `snake_case_` with trailing underscore for members (e.g., `page_id_`, `file_name_`)
- **Constants**: `UPPER_CASE` (e.g., `PAGE_SIZE`, `INVALID_PAGE_ID`)
- **Namespace**: `db` (everything in `namespace db { }`)

### Types
- **Type aliases**: `snake_case_t` (e.g., `page_id_t`, `frame_id_t`, `txn_id_t`)

---

## Include Conventions

### Internal includes (our code)
```cpp
#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/common/config.h"
```

### External dependencies
```cpp
#include <vector>
#include <string>
#include "SQLParser.h"  // HyriseSQL
```

### Include order
1. Corresponding header (if .cpp file)
2. Project headers
3. Third-party headers
4. Standard library headers

---

## Key Design Principles

1. **Separation of Concerns**: Each module has clear responsibility
2. **Dependency Direction**: Lower layers don't depend on higher layers
   - `storage/` doesn't know about `execution/`
   - `recovery/` depends on `storage/`
3. **Interface Libraries**: Use CMake INTERFACE targets for shared dependencies
4. **Testability**: Every component has unit tests
5. **Documentation**: Design doc before implementation, learnings doc after

---

## Next Steps

1. **Clean slate**: Delete old code or move to `old/` directory
2. **Create structure**: Set up directories and CMake files
3. **Start Phase 1**: Implement Disk Manager with tests
