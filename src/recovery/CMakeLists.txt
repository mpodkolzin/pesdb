add_library(recovery STATIC
  recovery_manager.cpp
)

target_link_libraries(recovery
  PUBLIC
    engine
    wal
    columnar_db_deps
)
