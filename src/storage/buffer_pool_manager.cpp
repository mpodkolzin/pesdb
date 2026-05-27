#include "columnar_db/storage/buffer_pool_manager.h"

#include <algorithm>
#include <stdexcept>

namespace db {

// ============================================================================
// Constructor: Initialize Buffer Pool
// ============================================================================

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size),
      disk_manager_(disk_manager),
      pages_(pool_size) {  // Allocate all frames upfront

  // Initially, all frames are free
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(static_cast<frame_id_t>(i));
  }

  // page_table_ and lru_list_ start empty (populated as pages loaded)
}

// ============================================================================
// Destructor: Flush All Dirty Pages
// ============================================================================

BufferPoolManager::~BufferPoolManager() {
  // Critical for durability! Write all dirty pages before shutdown
  FlushAllPages();
}

// ============================================================================
// FetchPage: Get Page from Pool or Disk
// ============================================================================

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // -----------------------------------------------------------------------
  // Case 1: Page already in pool (cache hit)
  // -----------------------------------------------------------------------

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // Page found! This is the fast path.
    frame_id_t frame_id = it->second;

    // Pin the page (increment reference count)
    // BufferPoolManager is a friend of Page, so can access pin_count_ directly
    pages_[frame_id].pin_count_++;

    // Move to front of LRU (most recently used)
    UpdateLRU(frame_id);

    return &pages_[frame_id];
  }

  // -----------------------------------------------------------------------
  // Case 2: Page not in pool (cache miss) - need a frame
  // -----------------------------------------------------------------------

  frame_id_t frame_id;

  // Try to get a free frame first (avoids eviction)
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // No free frames - must evict a victim page
    if (!FindVictimFrame(&frame_id)) {
      // All pages are pinned! Cannot evict anything.
      return nullptr;
    }
    // FindVictimFrame already removed victim from page_table_ and lru_list_
  }

  // -----------------------------------------------------------------------
  // We have a frame - load page from disk
  // -----------------------------------------------------------------------

  // Load page data from disk into the frame
  disk_manager_->ReadPage(page_id, pages_[frame_id].data());

  // Update page metadata (BufferPoolManager is friend, can access private members)
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;      // Newly fetched page is pinned
  pages_[frame_id].is_dirty_ = false;   // Fresh from disk (clean)

  // Update BufferPoolManager metadata
  page_table_[page_id] = frame_id;       // Register page in page table
  lru_list_.push_front(frame_id);        // Add to front (most recently used)

  return &pages_[frame_id];
}

// ============================================================================
// NewPage: Allocate New Page
// ============================================================================

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // -----------------------------------------------------------------------
  // Find a frame for the new page
  // -----------------------------------------------------------------------

  frame_id_t frame_id;

  // Try free list first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // No free frames - must evict
    if (!FindVictimFrame(&frame_id)) {
      // All pages are pinned! Cannot evict anything.
      return nullptr;
    }
  }

  // -----------------------------------------------------------------------
  // Allocate new page on disk
  // -----------------------------------------------------------------------

  // DiskManager::AllocatePage() returns the new page_id and initializes it
  page_id_t new_page_id = disk_manager_->AllocatePage();
  *page_id = new_page_id;  // Return to caller

  // -----------------------------------------------------------------------
  // Initialize page metadata
  // -----------------------------------------------------------------------

  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;      // New page is pinned (caller will use it)
  pages_[frame_id].is_dirty_ = true;    // New page is dirty (caller will write to it)
  pages_[frame_id].ResetMemory();       // Zero out the page data

  // Update BufferPoolManager metadata
  page_table_[new_page_id] = frame_id;
  lru_list_.push_front(frame_id);

  return &pages_[frame_id];
}

// ============================================================================
// UnpinPage: Release Page (Decrement Pin Count)
// ============================================================================

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);

  // Check if page is in the pool
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // Page not in pool
  }

  frame_id_t frame_id = it->second;

  // Check if already unpinned
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;  // Cannot unpin page with pin_count <= 0
  }

  // Decrement pin count
  pages_[frame_id].pin_count_--;

  // Update dirty flag (sticky: once dirty, stays dirty until flushed)
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  return true;
}

// ============================================================================
// FlushPage: Write Specific Page to Disk
// ============================================================================

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Check if page is in the pool
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // Page not in pool
  }

  frame_id_t frame_id = it->second;

  // Only write if dirty (optimization: avoid unnecessary I/O)
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data());
    pages_[frame_id].is_dirty_ = false;  // Now clean
  }

  return true;
}

// ============================================================================
// FlushAllPages: Write All Dirty Pages to Disk
// ============================================================================

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  // Iterate through all pages currently in the pool
  for (const auto& [page_id, frame_id] : page_table_) {
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(page_id, pages_[frame_id].data());
      pages_[frame_id].is_dirty_ = false;
    }
  }
}

// ============================================================================
// FindVictimFrame: LRU Eviction (Phase A - Simple)
// ============================================================================

bool BufferPoolManager::FindVictimFrame(frame_id_t* frame_id) {
  // Walk LRU list from back (least recently used) to front (most recently used)
  for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
    frame_id_t candidate = *it;

    // Can only evict unpinned pages
    if (pages_[candidate].GetPinCount() == 0) {
      *frame_id = candidate;

      // -----------------------------------------------------------------------
      // Write-before-evict: if the victim is dirty, persist it before the
      // frame is reused. The page's modifications live only in this frame's
      // memory; once the frame is overwritten by another page they are gone.
      // Skipping this would silently lose committed data -- it is the core
      // durability rule of a buffer pool.
      // -----------------------------------------------------------------------
      if (pages_[candidate].IsDirty()) {
        disk_manager_->WritePage(pages_[candidate].page_id(),
                                 pages_[candidate].data());
        pages_[candidate].is_dirty_ = false;
      }

      // Remove from metadata structures
      page_table_.erase(pages_[candidate].page_id());

      // Convert reverse_iterator to forward_iterator for erase
      // std::next(it).base() gives the forward iterator
      lru_list_.erase(std::next(it).base());

      return true;
    }
  }

  // All pages are pinned - cannot evict!
  return false;
}

// ============================================================================
// UpdateLRU: Move Frame to Front (Most Recently Used)
// ============================================================================

void BufferPoolManager::UpdateLRU(frame_id_t frame_id) {
  // Remove from current position in list (O(n) operation)
  lru_list_.remove(frame_id);

  // Add to front (most recently used)
  lru_list_.push_front(frame_id);
}

}  // namespace db
