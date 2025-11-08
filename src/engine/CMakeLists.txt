add_library(engine STATIC
  query_executor.cpp
)

target_link_libraries(engine PUBLIC
  storage         # Our engine needs to know about storage
  columnar_db_deps # And it needs the common dependencies
)