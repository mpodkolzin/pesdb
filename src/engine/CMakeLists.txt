add_library(engine STATIC
  database.cpp
)

target_link_libraries(engine
  PUBLIC
    wal
    columnar_db_deps
)
