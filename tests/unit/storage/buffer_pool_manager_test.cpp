// Unit tests for BufferPoolManager - Phase A (Simple Implementation)
// Tests buffer pool operations: fetch, pin/unpin, eviction, dirty page handling

#include "columnar_db/storage/buffer_pool_manager.h"
#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/common/config.h"
#include "columnar_db/common/types.h"

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <memory>

namespace db {

// Test fixture - provides clean buffer pool for each test
class BufferPoolManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create unique test DB file for each test
    test_db_file_ = "test_buffer_pool.db";

    // Remove any leftover test file from previous runs
    if (std::filesystem::exists(test_db_file_)) {
      std::filesystem::remove(test_db_file_);
    }

    // Create DiskManager
    disk_manager_ = std::make_unique<DiskManager>(test_db_file_);

    // Create BufferPoolManager with small pool size for easier testing
    buffer_pool_manager_ = std::make_unique<BufferPoolManager>(pool_size_, disk_manager_.get());
  }

  void TearDown() override {
    // Destroy BufferPoolManager first (flushes dirty pages)
    buffer_pool_manager_.reset();

    // Destroy DiskManager (closes file)
    disk_manager_.reset();

    // Clean up test database
    if (std::filesystem::exists(test_db_file_)) {
      std::filesystem::remove(test_db_file_);
    }
  }

  std::string test_db_file_;
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
  const size_t pool_size_ = 10;  // Small pool for testing
};

// ============================================================================
// Test 1: Basic FetchPage and UnpinPage
// Learning: Understand the fetch-pin-unpin lifecycle
// ============================================================================

TEST_F(BufferPoolManagerTest, FetchAndUnpin) {
  // Allocate a page on disk first
  page_id_t page0_id = disk_manager_->AllocatePage();

  // Fetch the page from buffer pool (cache miss - loads from disk)
  Page* page0 = buffer_pool_manager_->FetchPage(page0_id);
  ASSERT_NE(page0, nullptr);

  // Verify page metadata
  EXPECT_EQ(page0->page_id(), page0_id);
  EXPECT_EQ(page0->GetPinCount(), 1);  // Fetched page is pinned

  // Unpin the page
  EXPECT_TRUE(buffer_pool_manager_->UnpinPage(page0_id, false));
  EXPECT_EQ(page0->GetPinCount(), 0);  // Now unpinned (eligible for eviction)
}

// ============================================================================
// Test 2: Cache Hit
// Learning: Pages stay in pool after unpin, subsequent fetch is fast (no I/O)
// ============================================================================

TEST_F(BufferPoolManagerTest, CacheHit) {
  page_id_t page0_id = disk_manager_->AllocatePage();

  // First fetch (cache miss)
  Page* page0_first = buffer_pool_manager_->FetchPage(page0_id);
  ASSERT_NE(page0_first, nullptr);
  buffer_pool_manager_->UnpinPage(page0_id, false);

  // Second fetch (cache hit - page still in pool)
  Page* page0_second = buffer_pool_manager_->FetchPage(page0_id);
  ASSERT_NE(page0_second, nullptr);

  // Should be the SAME pointer (same frame)
  EXPECT_EQ(page0_first, page0_second);
  EXPECT_EQ(page0_second->GetPinCount(), 1);

  buffer_pool_manager_->UnpinPage(page0_id, false);
}

// ============================================================================
// Test 3: NewPage
// Learning: BufferPoolManager can allocate new pages
// ============================================================================

TEST_F(BufferPoolManagerTest, NewPage) {
  // Allocate new page through buffer pool
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);

  ASSERT_NE(new_page, nullptr);
  EXPECT_EQ(new_page->page_id(), new_page_id);
  EXPECT_EQ(new_page->GetPinCount(), 1);     // New page is pinned
  EXPECT_TRUE(new_page->IsDirty());          // New page is dirty

  buffer_pool_manager_->UnpinPage(new_page_id, false);
}

// ============================================================================
// Test 4: Multiple Pins (Reference Counting)
// Learning: Pin count tracks how many threads are using a page
// ============================================================================

TEST_F(BufferPoolManagerTest, MultiplePins) {
  page_id_t page0_id = disk_manager_->AllocatePage();

  // Fetch the same page multiple times
  Page* page1 = buffer_pool_manager_->FetchPage(page0_id);
  Page* page2 = buffer_pool_manager_->FetchPage(page0_id);
  Page* page3 = buffer_pool_manager_->FetchPage(page0_id);

  ASSERT_NE(page1, nullptr);
  EXPECT_EQ(page1, page2);  // All point to same page
  EXPECT_EQ(page2, page3);

  // Pin count should be 3
  EXPECT_EQ(page1->GetPinCount(), 3);

  // Unpin one by one
  buffer_pool_manager_->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 2);

  buffer_pool_manager_->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 1);

  buffer_pool_manager_->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 0);  // Now eligible for eviction
}

// ============================================================================
// Test 5: Write and Read Data
// Learning: Pages can store data and persist to disk
// ============================================================================

TEST_F(BufferPoolManagerTest, WriteAndReadData) {
  // Create new page
  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // Write data to the page
  const char* test_data = "Hello, Buffer Pool Manager!";
  strcpy(page->data(), test_data);

  // Unpin and mark as dirty (we modified it)
  buffer_pool_manager_->UnpinPage(page_id, true);

  // Flush to disk
  EXPECT_TRUE(buffer_pool_manager_->FlushPage(page_id));

  // Fetch again and verify data persisted
  page = buffer_pool_manager_->FetchPage(page_id);
  ASSERT_NE(page, nullptr);
  EXPECT_STREQ(page->data(), test_data);

  buffer_pool_manager_->UnpinPage(page_id, false);
}

// ============================================================================
// Test 6: Dirty Flag Handling
// Learning: Dirty flag is "sticky" - once dirty, stays dirty until flushed
// ============================================================================

TEST_F(BufferPoolManagerTest, DirtyFlag) {
  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // New page is dirty
  EXPECT_TRUE(page->IsDirty());

  // Unpin with is_dirty=false, but page should still be dirty
  buffer_pool_manager_->UnpinPage(page_id, false);

  // Fetch again - still dirty
  page = buffer_pool_manager_->FetchPage(page_id);
  EXPECT_TRUE(page->IsDirty());

  buffer_pool_manager_->UnpinPage(page_id, false);

  // Flush the page - now clean
  buffer_pool_manager_->FlushPage(page_id);

  page = buffer_pool_manager_->FetchPage(page_id);
  EXPECT_FALSE(page->IsDirty());  // Now clean after flush

  // Modify and mark dirty
  buffer_pool_manager_->UnpinPage(page_id, true);

  page = buffer_pool_manager_->FetchPage(page_id);
  EXPECT_TRUE(page->IsDirty());  // Dirty again

  buffer_pool_manager_->UnpinPage(page_id, false);
}

// ============================================================================
// Test 7: Simple LRU Eviction
// Learning: When pool is full, LRU evicts least recently used unpinned page
// ============================================================================

TEST_F(BufferPoolManagerTest, SimpleLRUEviction) {
  // Create buffer pool with size 3 for easier testing
  buffer_pool_manager_ = std::make_unique<BufferPoolManager>(3, disk_manager_.get());

  // Allocate 3 pages (fill the pool)
  page_id_t pid0, pid1, pid2;
  Page* page0 = buffer_pool_manager_->NewPage(&pid0);
  Page* page1 = buffer_pool_manager_->NewPage(&pid1);
  Page* page2 = buffer_pool_manager_->NewPage(&pid2);

  ASSERT_NE(page0, nullptr);
  ASSERT_NE(page1, nullptr);
  ASSERT_NE(page2, nullptr);

  // Write identifiable data to each page
  strcpy(page0->data(), "Page 0");
  strcpy(page1->data(), "Page 1");
  strcpy(page2->data(), "Page 2");

  // Unpin all (make eligible for eviction)
  buffer_pool_manager_->UnpinPage(pid0, true);
  buffer_pool_manager_->UnpinPage(pid1, true);
  buffer_pool_manager_->UnpinPage(pid2, true);

  // Access pid1 (moves it to front of LRU - most recently used)
  page1 = buffer_pool_manager_->FetchPage(pid1);
  ASSERT_NE(page1, nullptr);
  buffer_pool_manager_->UnpinPage(pid1, false);

  // LRU order now: pid1 (MRU), pid2, pid0 (LRU)

  // Allocate 4th page - should evict pid0 (LRU)
  page_id_t pid3;
  Page* page3 = buffer_pool_manager_->NewPage(&pid3);
  ASSERT_NE(page3, nullptr);
  strcpy(page3->data(), "Page 3");
  buffer_pool_manager_->UnpinPage(pid3, true);

  // Verify pid1 and pid2 are still in pool
  page1 = buffer_pool_manager_->FetchPage(pid1);
  ASSERT_NE(page1, nullptr);
  EXPECT_STREQ(page1->data(), "Page 1");
  buffer_pool_manager_->UnpinPage(pid1, false);

  page2 = buffer_pool_manager_->FetchPage(pid2);
  ASSERT_NE(page2, nullptr);
  EXPECT_STREQ(page2->data(), "Page 2");
  buffer_pool_manager_->UnpinPage(pid2, false);

  // Verify pid3 is in pool
  page3 = buffer_pool_manager_->FetchPage(pid3);
  ASSERT_NE(page3, nullptr);
  EXPECT_STREQ(page3->data(), "Page 3");
  buffer_pool_manager_->UnpinPage(pid3, false);

  // pid0 should have been evicted - but can re-fetch from disk
  page0 = buffer_pool_manager_->FetchPage(pid0);
  ASSERT_NE(page0, nullptr);  // Should succeed (loads from disk)
  EXPECT_STREQ(page0->data(), "Page 0");  // Data persisted
  buffer_pool_manager_->UnpinPage(pid0, false);
}

// ============================================================================
// Test 8: All Pages Pinned (Cannot Evict)
// Learning: When all pages pinned, new allocation fails (pool exhaustion)
// ============================================================================

TEST_F(BufferPoolManagerTest, AllPagesPinned) {
  // Create small buffer pool (size 2)
  buffer_pool_manager_ = std::make_unique<BufferPoolManager>(2, disk_manager_.get());

  // Allocate 2 pages and keep them pinned
  page_id_t pid0, pid1;
  Page* page0 = buffer_pool_manager_->NewPage(&pid0);
  Page* page1 = buffer_pool_manager_->NewPage(&pid1);

  ASSERT_NE(page0, nullptr);
  ASSERT_NE(page1, nullptr);

  // Both pages are pinned (pin_count = 1)

  // Try to allocate 3rd page - should fail (all pages pinned, can't evict)
  page_id_t pid2;
  Page* page2 = buffer_pool_manager_->NewPage(&pid2);
  EXPECT_EQ(page2, nullptr);  // Should return nullptr

  // Unpin one page
  buffer_pool_manager_->UnpinPage(pid0, false);

  // Now should succeed (can evict pid0)
  page2 = buffer_pool_manager_->NewPage(&pid2);
  EXPECT_NE(page2, nullptr);

  // Clean up
  buffer_pool_manager_->UnpinPage(pid1, false);
  buffer_pool_manager_->UnpinPage(pid2, false);
}

// ============================================================================
// Test 9: FlushAllPages
// Learning: FlushAllPages writes all dirty pages to disk
// ============================================================================

TEST_F(BufferPoolManagerTest, FlushAllPages) {
  // Create several dirty pages
  page_id_t pid0, pid1, pid2;
  Page* page0 = buffer_pool_manager_->NewPage(&pid0);
  Page* page1 = buffer_pool_manager_->NewPage(&pid1);
  Page* page2 = buffer_pool_manager_->NewPage(&pid2);

  ASSERT_NE(page0, nullptr);
  ASSERT_NE(page1, nullptr);
  ASSERT_NE(page2, nullptr);

  // Write data
  strcpy(page0->data(), "Data 0");
  strcpy(page1->data(), "Data 1");
  strcpy(page2->data(), "Data 2");

  // All are dirty
  EXPECT_TRUE(page0->IsDirty());
  EXPECT_TRUE(page1->IsDirty());
  EXPECT_TRUE(page2->IsDirty());

  buffer_pool_manager_->UnpinPage(pid0, true);
  buffer_pool_manager_->UnpinPage(pid1, true);
  buffer_pool_manager_->UnpinPage(pid2, true);

  // Flush all
  buffer_pool_manager_->FlushAllPages();

  // All should be clean now
  page0 = buffer_pool_manager_->FetchPage(pid0);
  page1 = buffer_pool_manager_->FetchPage(pid1);
  page2 = buffer_pool_manager_->FetchPage(pid2);

  EXPECT_FALSE(page0->IsDirty());
  EXPECT_FALSE(page1->IsDirty());
  EXPECT_FALSE(page2->IsDirty());

  buffer_pool_manager_->UnpinPage(pid0, false);
  buffer_pool_manager_->UnpinPage(pid1, false);
  buffer_pool_manager_->UnpinPage(pid2, false);
}

// ============================================================================
// Test 10: UnpinPage Error Cases
// Learning: UnpinPage returns false for invalid operations
// ============================================================================

TEST_F(BufferPoolManagerTest, UnpinPageErrors) {
  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // Unpin once - should succeed
  EXPECT_TRUE(buffer_pool_manager_->UnpinPage(page_id, false));

  // Unpin again - should fail (already at pin_count 0)
  EXPECT_FALSE(buffer_pool_manager_->UnpinPage(page_id, false));

  // Unpin non-existent page - should fail
  EXPECT_FALSE(buffer_pool_manager_->UnpinPage(999, false));
}

// ============================================================================
// Test 11: FlushPage Non-Existent
// Learning: FlushPage returns false for pages not in pool
// ============================================================================

TEST_F(BufferPoolManagerTest, FlushPageNonExistent) {
  // Try to flush page that's not in pool
  EXPECT_FALSE(buffer_pool_manager_->FlushPage(999));
}

// ============================================================================
// Test 12: Persistence After Destruction
// Learning: Destructor flushes dirty pages, data survives restart
// ============================================================================

TEST_F(BufferPoolManagerTest, PersistenceAfterDestruction) {
  page_id_t page_id;

  // Create page and write data
  {
    auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager_.get());
    Page* page = bpm->NewPage(&page_id);
    ASSERT_NE(page, nullptr);

    strcpy(page->data(), "Persistent Data");
    bpm->UnpinPage(page_id, true);

    // Destructor called here - flushes dirty pages
  }

  // Create new buffer pool (simulates restart)
  {
    auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager_.get());

    // Fetch page - should load from disk with persisted data
    Page* page = bpm->FetchPage(page_id);
    ASSERT_NE(page, nullptr);
    EXPECT_STREQ(page->data(), "Persistent Data");

    bpm->UnpinPage(page_id, false);
  }
}

}  // namespace db
