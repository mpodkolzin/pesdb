add_library(storage STATIC
  buffer_pool_manager.cpp
  disk_manager.cpp
  table.cpp
  catalog.cpp
)

# Publicly link against our dependency bundle
target_link_libraries(storage PUBLIC columnar_db_deps)