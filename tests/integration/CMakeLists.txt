add_executable(integration_tests
  wal_recovery_test.cpp
)

target_link_libraries(integration_tests
  PRIVATE
    recovery   # transitively pulls engine + wal
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(integration_tests)
