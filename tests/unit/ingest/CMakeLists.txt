# Ingest unit tests

add_executable(ingest_tests
    schema_test.cpp
)

target_link_libraries(ingest_tests
    PRIVATE
        columnar_db_ingest
        GTest::gtest_main
)

# Register with CTest
include(GoogleTest)
gtest_discover_tests(ingest_tests)
