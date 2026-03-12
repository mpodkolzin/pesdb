# Storage unit tests

add_executable(storage_tests
    disk_manager_test.cpp
    page_test.cpp
    buffer_pool_manager_test.cpp
)

target_link_libraries(storage_tests
    PRIVATE
        columnar_db_storage
        GTest::gtest_main
)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(storage_tests)
