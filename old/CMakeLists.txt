add_executable(columnar_db
  main.cpp
)

# The executable needs the engine to run queries
target_link_libraries(columnar_db PRIVATE engine storage wal)