// Unit tests for DiskManager
// Tests fundamental disk I/O operations: page allocation, read/write, persistence

#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/common/config.h"
#include "columnar_db/common/types.h"

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>

namespace db {

// Test fixture - provides clean test database for each test
class DiskManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create unique test DB file for each test
    test_db_file_ = "test_disk_manager.db";

    // Remove any leftover test file from previous runs
    if (std::filesystem::exists(test_db_file_)) {
      std::filesystem::remove(test_db_file_);
    }
  }

  void TearDown() override {
    // Clean up test database after each test
    if (std::filesystem::exists(test_db_file_)) {
      std::filesystem::remove(test_db_file_);
    }
  }

  std::string test_db_file_;
};

// Test 1: CreateNewDatabase
// Learning: DiskManager creates new file if it doesn't exist
TEST_F(DiskManagerTest, CreateNewDatabase) {
  // Create a new database - file should be created
  {
    DiskManager disk_manager(test_db_file_);

    // Verify the file was created
    EXPECT_TRUE(std::filesystem::exists(test_db_file_));

    // New database should have 0 pages initially
    EXPECT_EQ(disk_manager.GetNumPages(), 0);
  }
  // DiskManager destructor closes file (RAII)
}

// Test 2: AllocateSinglePage
// Learning: AllocatePage() returns sequential page IDs starting from 0
TEST_F(DiskManagerTest, AllocateSinglePage) {
  DiskManager disk_manager(test_db_file_);

  // First allocated page should be page 0
  page_id_t page_id = disk_manager.AllocatePage();
  EXPECT_EQ(page_id, 0);
  EXPECT_EQ(disk_manager.GetNumPages(), 1);

  // Verify the page is zero-initialized (eager allocation)
  char buffer[PAGE_SIZE];
  disk_manager.ReadPage(page_id, buffer);

  // All bytes should be zero
  for (size_t i = 0; i < PAGE_SIZE; ++i) {
    EXPECT_EQ(buffer[i], 0) << "Byte at offset " << i << " is not zero";
  }
}

// Test 3: WriteAndReadPage
// Learning: Write/Read roundtrip should preserve data exactly
TEST_F(DiskManagerTest, WriteAndReadPage) {
  DiskManager disk_manager(test_db_file_);

  page_id_t page_id = disk_manager.AllocatePage();

  // Create test data with a recognizable pattern
  char write_buffer[PAGE_SIZE];
  for (size_t i = 0; i < PAGE_SIZE; ++i) {
    write_buffer[i] = static_cast<char>(i % 256);  // Pattern: 0, 1, 2, ..., 255, 0, 1, ...
  }

  // Write the pattern to disk
  disk_manager.WritePage(page_id, write_buffer);

  // Read it back
  char read_buffer[PAGE_SIZE];
  disk_manager.ReadPage(page_id, read_buffer);

  // Verify data matches exactly
  EXPECT_EQ(std::memcmp(write_buffer, read_buffer, PAGE_SIZE), 0)
      << "Read data doesn't match written data";
}

// Test 4: MultiplePages
// Learning: Each page is independent, can hold different data
TEST_F(DiskManagerTest, MultiplePages) {
  DiskManager disk_manager(test_db_file_);

  const int num_pages = 10;
  page_id_t page_ids[num_pages];

  // Allocate and write unique data to each page
  for (int i = 0; i < num_pages; ++i) {
    page_ids[i] = disk_manager.AllocatePage();
    EXPECT_EQ(page_ids[i], i) << "Page IDs should be sequential";

    // Each page gets a unique pattern based on page number
    char buffer[PAGE_SIZE];
    std::memset(buffer, i, PAGE_SIZE);  // Fill with page number
    disk_manager.WritePage(page_ids[i], buffer);
  }

  EXPECT_EQ(disk_manager.GetNumPages(), num_pages);

  // Read back and verify each page has its unique pattern
  for (int i = 0; i < num_pages; ++i) {
    char buffer[PAGE_SIZE];
    disk_manager.ReadPage(page_ids[i], buffer);

    // Verify all bytes match the expected pattern
    for (size_t j = 0; j < PAGE_SIZE; ++j) {
      EXPECT_EQ(static_cast<unsigned char>(buffer[j]), i)
          << "Page " << i << " byte " << j << " has wrong value";
    }
  }
}

// Test 5: ReopenDatabase
// Learning: Data persists across DiskManager instances (durability)
TEST_F(DiskManagerTest, ReopenDatabase) {
  const char test_pattern = 'X';
  page_id_t page_id;

  // Phase 1: Create database, write data, close
  {
    DiskManager disk_manager(test_db_file_);
    page_id = disk_manager.AllocatePage();

    char write_buffer[PAGE_SIZE];
    std::memset(write_buffer, test_pattern, PAGE_SIZE);
    disk_manager.WritePage(page_id, write_buffer);

    EXPECT_EQ(disk_manager.GetNumPages(), 1);
  }
  // DiskManager destructor closes file

  // Phase 2: Reopen database, verify data persisted
  {
    DiskManager disk_manager(test_db_file_);

    // Existing database should remember it has 1 page
    EXPECT_EQ(disk_manager.GetNumPages(), 1);

    // Read the page and verify data
    char read_buffer[PAGE_SIZE];
    disk_manager.ReadPage(page_id, read_buffer);

    for (size_t i = 0; i < PAGE_SIZE; ++i) {
      EXPECT_EQ(read_buffer[i], test_pattern)
          << "Data not persisted at byte " << i;
    }
  }
}

// Test 6: ReadInvalidPage
// Learning: Reading beyond allocated pages throws exception
TEST_F(DiskManagerTest, ReadInvalidPage) {
  DiskManager disk_manager(test_db_file_);

  // Allocate page 0, so valid range is [0, 0]
  disk_manager.AllocatePage();

  char buffer[PAGE_SIZE];

  // Try to read page 1 (not allocated yet)
  EXPECT_THROW(disk_manager.ReadPage(1, buffer), std::out_of_range)
      << "Should throw when reading beyond allocated pages";

  // Try to read negative page ID
  EXPECT_THROW(disk_manager.ReadPage(-1, buffer), std::out_of_range)
      << "Should throw for negative page ID";
}

}  // namespace db
