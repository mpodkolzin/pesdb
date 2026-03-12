#include "columnar_db/storage/page.h"

#include <gtest/gtest.h>

#include <cstring>
#include <thread>
#include <vector>

namespace db {

// ============================================================================
// Test Fixture
// ============================================================================

/**
 * PageTest fixture provides a fresh Page for each test.
 *
 * SetUp/TearDown not strictly needed (Page cleans up automatically),
 * but good practice for test organization.
 */
class PageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Each test gets a fresh page
    // Page constructor zero-initializes data
  }

  void TearDown() override {
    // No cleanup needed (RAII handles it)
  }
};

// ============================================================================
// Test 1: Basic Construction and Initialization
// ============================================================================

TEST_F(PageTest, Construction) {
  Page page;

  // Page should start with invalid page_id
  EXPECT_EQ(page.page_id(), INVALID_PAGE_ID);

  // Data should be zero-initialized
  const char* data = page.data();
  for (size_t i = 0; i < PAGE_SIZE; ++i) {
    EXPECT_EQ(data[i], 0) << "Byte at offset " << i << " is not zero";
  }
}

// ============================================================================
// Test 2: Data Access
// ============================================================================

TEST_F(PageTest, DataAccess) {
  Page page;

  // Get mutable pointer
  char* data = page.data();
  ASSERT_NE(data, nullptr);

  // Write some data
  data[0] = 'A';
  data[100] = 'B';
  data[PAGE_SIZE - 1] = 'Z';

  // Read back and verify
  EXPECT_EQ(data[0], 'A');
  EXPECT_EQ(data[100], 'B');
  EXPECT_EQ(data[PAGE_SIZE - 1], 'Z');
}

TEST_F(PageTest, ConstDataAccess) {
  Page page;

  // Write some data
  char* data = page.data();
  data[0] = 'X';

  // Read via const pointer
  const Page& const_page = page;
  const char* const_data = const_page.data();
  EXPECT_EQ(const_data[0], 'X');
}

// ============================================================================
// Test 3: Page ID
// ============================================================================

TEST_F(PageTest, PageIdInitiallyInvalid) {
  Page page;
  EXPECT_EQ(page.page_id(), INVALID_PAGE_ID);
}

// Note: page_id_ is private and set by BufferPoolManager via friend access.
// We can't test setting it directly here (that's intentional - encapsulation!).
// BufferPoolManager tests will verify page_id management.

// ============================================================================
// Test 4: Manual Locking API - Basic Usage
// ============================================================================

TEST_F(PageTest, ReadLatch) {
  Page page;

  // Acquire and release read lock
  page.r_latch();
  // In a real scenario, would read data here
  page.r_unlatch();

  // If we got here without deadlock, locking works
  SUCCEED();
}

TEST_F(PageTest, WriteLatch) {
  Page page;

  // Acquire and release write lock
  page.w_latch();
  // In a real scenario, would modify data here
  page.w_unlatch();

  // If we got here without deadlock, locking works
  SUCCEED();
}

TEST_F(PageTest, ReadWriteCycle) {
  Page page;

  // Read, release, write, release
  page.r_latch();
  page.r_unlatch();

  page.w_latch();
  page.w_unlatch();

  // Multiple cycles
  page.r_latch();
  page.r_unlatch();

  page.w_latch();
  page.w_unlatch();

  SUCCEED();
}

// ============================================================================
// Test 5: RAII Lock Guards - Basic Usage
// ============================================================================

TEST_F(PageTest, ReadGuardBasic) {
  Page page;

  {
    Page::ReadGuard guard(&page);
    // Lock held here
    // Read data
  }  // Lock automatically released

  // Can acquire lock again (proves previous lock was released)
  {
    Page::ReadGuard guard(&page);
    SUCCEED();
  }
}

TEST_F(PageTest, WriteGuardBasic) {
  Page page;

  {
    Page::WriteGuard guard(&page);
    // Lock held here
    // Modify data
  }  // Lock automatically released

  // Can acquire lock again (proves previous lock was released)
  {
    Page::WriteGuard guard(&page);
    SUCCEED();
  }
}

TEST_F(PageTest, ReadGuardExceptionSafety) {
  Page page;

  // Even if exception is thrown, lock should be released
  try {
    Page::ReadGuard guard(&page);
    throw std::runtime_error("Test exception");
  } catch (const std::runtime_error&) {
    // Exception caught
  }

  // Can acquire lock (proves previous lock was released despite exception)
  Page::ReadGuard guard(&page);
  SUCCEED();
}

TEST_F(PageTest, WriteGuardExceptionSafety) {
  Page page;

  // Even if exception is thrown, lock should be released
  try {
    Page::WriteGuard guard(&page);
    throw std::runtime_error("Test exception");
  } catch (const std::runtime_error&) {
    // Exception caught
  }

  // Can acquire lock (proves previous lock was released despite exception)
  Page::WriteGuard guard(&page);
  SUCCEED();
}

// ============================================================================
// Test 6: Multi-threaded Locking - Multiple Readers
// ============================================================================

TEST_F(PageTest, MultipleReadersSimultaneous) {
  Page page;

  // Write some data first
  page.data()[0] = 42;

  constexpr int NUM_READERS = 10;
  std::vector<std::thread> readers;

  // Launch multiple reader threads
  for (int i = 0; i < NUM_READERS; ++i) {
    readers.emplace_back([&page]() {
      Page::ReadGuard guard(&page);
      // All readers should be able to read simultaneously
      char value = page.data()[0];
      EXPECT_EQ(value, 42);
    });
  }

  // Wait for all readers to complete
  for (auto& t : readers) {
    t.join();
  }

  // If we got here without deadlock, multiple readers worked!
  SUCCEED();
}

// ============================================================================
// Test 7: Multi-threaded Locking - Writer Exclusion
// ============================================================================

TEST_F(PageTest, WriterBlocksReaders) {
  Page page;

  std::atomic<bool> writer_holding_lock{false};
  std::atomic<bool> reader_acquired_lock{false};

  // Start writer thread (holds lock for a bit)
  std::thread writer([&page, &writer_holding_lock]() {
    Page::WriteGuard guard(&page);
    writer_holding_lock = true;
    // Hold lock for 100ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  // Wait for writer to acquire lock
  while (!writer_holding_lock) {
    std::this_thread::yield();
  }

  // Try to acquire read lock (should block until writer releases)
  std::thread reader([&page, &reader_acquired_lock]() {
    Page::ReadGuard guard(&page);
    reader_acquired_lock = true;
  });

  // Give reader a chance to try acquiring lock
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Reader should still be blocked
  EXPECT_FALSE(reader_acquired_lock);

  // Wait for writer to finish
  writer.join();

  // Now reader should be able to acquire lock
  reader.join();
  EXPECT_TRUE(reader_acquired_lock);
}

TEST_F(PageTest, WriterExclusive) {
  Page page;

  std::atomic<int> concurrent_writers{0};
  std::atomic<int> max_concurrent_writers{0};

  constexpr int NUM_WRITERS = 5;
  std::vector<std::thread> writers;

  // Launch multiple writer threads
  for (int i = 0; i < NUM_WRITERS; ++i) {
    writers.emplace_back([&page, &concurrent_writers, &max_concurrent_writers]() {
      Page::WriteGuard guard(&page);

      // Increment concurrent writer count
      int current = ++concurrent_writers;

      // Track maximum
      int expected = max_concurrent_writers.load();
      while (current > expected &&
             !max_concurrent_writers.compare_exchange_weak(expected, current)) {
        // Keep trying to update max
      }

      // Hold lock briefly
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // Decrement
      --concurrent_writers;
    });
  }

  // Wait for all writers
  for (auto& t : writers) {
    t.join();
  }

  // Only one writer should have held lock at a time
  EXPECT_EQ(max_concurrent_writers, 1);
}

// ============================================================================
// Test 8: Data Integrity Under Concurrent Access
// ============================================================================

TEST_F(PageTest, ReadersSeeSameData) {
  Page page;

  // Initialize data with pattern
  char* data = page.data();
  for (size_t i = 0; i < PAGE_SIZE; ++i) {
    data[i] = static_cast<char>(i % 256);
  }

  constexpr int NUM_READERS = 20;
  std::vector<std::thread> readers;
  std::atomic<int> failures{0};

  // Launch readers that verify the pattern
  for (int i = 0; i < NUM_READERS; ++i) {
    readers.emplace_back([&page, &failures]() {
      Page::ReadGuard guard(&page);
      const char* data = page.data();

      // Verify pattern
      for (size_t i = 0; i < PAGE_SIZE; ++i) {
        if (data[i] != static_cast<char>(i % 256)) {
          ++failures;
          break;
        }
      }
    });
  }

  for (auto& t : readers) {
    t.join();
  }

  EXPECT_EQ(failures, 0);
}

TEST_F(PageTest, WriterModifiesDataSafely) {
  Page page;

  constexpr int NUM_WRITERS = 10;
  std::vector<std::thread> writers;

  // Each writer increments first byte
  for (int i = 0; i < NUM_WRITERS; ++i) {
    writers.emplace_back([&page]() {
      Page::WriteGuard guard(&page);
      page.data()[0]++;
    });
  }

  for (auto& t : writers) {
    t.join();
  }

  // First byte should be incremented NUM_WRITERS times
  EXPECT_EQ(static_cast<unsigned char>(page.data()[0]), NUM_WRITERS);
}

// ============================================================================
// Test 9: Memory Layout and Size
// ============================================================================

TEST_F(PageTest, PageSize) {
  Page page;

  // Page should contain at least PAGE_SIZE bytes of data
  // (actual sizeof(Page) will be larger due to metadata)
  EXPECT_GE(sizeof(Page), PAGE_SIZE);

  // Verify we can access all PAGE_SIZE bytes
  char* data = page.data();
  data[PAGE_SIZE - 1] = 'X';  // Should not crash
  EXPECT_EQ(data[PAGE_SIZE - 1], 'X');
}

// ============================================================================
// Test 10: Copy/Move Deleted
// ============================================================================

// Note: These are compile-time checks, so we can't really "test" them at runtime.
// If the code compiles, the deleted functions are correctly deleted.
// Uncommenting these should cause compile errors:

// TEST_F(PageTest, CannotCopy) {
//   Page page1;
//   Page page2 = page1;  // Should not compile
// }

// TEST_F(PageTest, CannotMove) {
//   Page page1;
//   Page page2 = std::move(page1);  // Should not compile
// }

}  // namespace db
