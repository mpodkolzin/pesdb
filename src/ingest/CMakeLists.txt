# Ingest library - CSV loading and data ingestion

add_library(columnar_db_ingest
    schema.cpp
)

target_include_directories(columnar_db_ingest
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(columnar_db_ingest
    PUBLIC
        # No dependencies yet (just uses common types)
)
