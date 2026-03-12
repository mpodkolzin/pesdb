# Storage library - foundational storage components
# Phase 1: DiskManager, Page, BufferPoolManager
# Phase 2: ColumnSegment, ColumnTable

add_library(columnar_db_storage
    disk_manager.cpp
    # page.cpp              # Phase 1, Step 2 (header-only)
    buffer_pool_manager.cpp  # Phase 1, Step 3
    # column_segment.cpp    # Phase 2
    # column_table.cpp      # Phase 2
)

# Link against common dependencies (includes, third-party libs)
target_link_libraries(columnar_db_storage
    PUBLIC columnar_db_deps
)

# Make headers available to other targets
target_include_directories(columnar_db_storage
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
)
