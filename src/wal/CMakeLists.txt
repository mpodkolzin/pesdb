add_library(wal STATIC
  log_manager.cpp
  log_record.cpp
)

# Publicly link against our dependency bundle
target_link_libraries(wal PUBLIC columnar_db_deps)