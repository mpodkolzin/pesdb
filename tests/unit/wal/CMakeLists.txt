# WAL unit tests

add_executable(wal_tests
    log_record_test.cpp
    log_manager_test.cpp
)

target_link_libraries(wal_tests
    PRIVATE
        wal
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(wal_tests)
