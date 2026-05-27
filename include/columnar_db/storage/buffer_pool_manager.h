#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "columnar_db/common/config.h"
#include "columnar_db/common/types.h"
#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/storage/page.h"

namespace db {

/**
 * @class BufferPoolManager
 * @brief Memory cache layer for database pages (Phase A: Simple Implementation).
 *
 * The BufferPoolManager sits between DiskManager (slow I/O) and database
 * operations (fast memory access). It caches disk pages in memory and manages
 * eviction when the pool is full.
 *
 * **Key Concepts:**
 * - **Frames vs Pages**: Pool has fixed frames (e.g., 100), disk has unlimited pages
 * - **page_table**: Maps page_id → frame_id (which frame holds which page)
 * - **Pinning**: Reference counting prevents eviction while page in use
 * - **LRU Eviction**: Least Recently Used policy for choosing victim pages
 * - **Dirty Pages**: Modified pages must be written to disk before eviction
 *
 * **Current Characteristics (Simple):**
 * - Coarse-grained locking (hold latch for entire operation, including I/O)
 * - List-based LRU (simple move-to-front on access)
 * - Write-before-evict: dirty victims are flushed to disk before reuse
 * - Focus on correctness over performance
 *
 * **Thread Safety Model:**
 * - latch_ protects: page_table_, free_list_, lru_list_, pin_count_, is_dirty_
 * - Page latch protects: page data content (separate from BufferPool latch)
 *
 * **Future Work:**
 * - Write-ahead invariant: flush WAL up to page_lsn before evicting a page
 *   (depends on WAL Phase 2 introducing page_lsn). See
 *   doc/design/wal/log_manager.md § Deferred Work.
 * - Optimize concurrency (release latch during I/O)
 *
 * Example Usage:
 * @code
 *   auto disk_mgr = std::make_unique<DiskManager>("mydb.db");
 *   auto bpm = std::make_unique<BufferPoolManager>(100, disk_mgr.get());
 *
 *   // Fetch page (pins it)
 *   Page* page = bpm->FetchPage(42);
 *   if (page != nullptr) {
 *     // Use page...
 *     page->w_latch();
 *     memcpy(page->data(), new_data, 100);
 *     page->w_unlatch();
 *
 *     // Unpin when done (mark dirty if modified)
 *     bpm->UnpinPage(42, true);
 *   }
 *
 *   // Create new page
 *   page_id_t new_page_id;
 *   Page* new_page = bpm->NewPage(&new_page_id);
 *   // ... use new_page ...
 *   bpm->UnpinPage(new_page_id, true);
 *
 *   // Flush all dirty pages before shutdown (done automatically in destructor)
 *   bpm->FlushAllPages();
 * @endcode
 */
class BufferPoolManager {
 public:
  /**
   * Creates a buffer pool with fixed size.
   *
   * @param pool_size Number of pages in the buffer pool (e.g., 100)
   * @param disk_manager Pointer to disk manager for page I/O (must outlive BufferPoolManager)
   *
   * **Initializes:**
   * - pages_ vector with pool_size Page objects
   * - free_list_ with all frame IDs [0, 1, ..., pool_size-1]
   * - page_table_ and lru_list_ start empty
   */
  BufferPoolManager(size_t pool_size, DiskManager* disk_manager);

  /**
   * Destructor: Flushes all dirty pages to disk.
   *
   * **Critical for durability!** Without this, dirty pages would be lost on shutdown.
   * Calls FlushAllPages() to ensure all modifications are written to disk.
   */
  ~BufferPoolManager();

  // Delete copy/move constructors (manages complex state, shouldn't be copied)
  BufferPoolManager(const BufferPoolManager&) = delete;
  BufferPoolManager& operator=(const BufferPoolManager&) = delete;
  BufferPoolManager(BufferPoolManager&&) = delete;
  BufferPoolManager& operator=(BufferPoolManager&&) = delete;

  /**
   * Fetches a page from the buffer pool.
   *
   * **Cache hit:** If page already in pool, increment pin_count and return it.
   * **Cache miss:** If page not in pool:
   *   1. Find a free frame (or evict a victim page)
   *   2. Load page from disk
   *   3. Update metadata (pin_count = 1, is_dirty = false)
   *   4. Return page pointer
   *
   * **The returned page is PINNED (pin_count = 1).**
   * Caller must call UnpinPage() when done to allow eviction.
   *
   * @param page_id The page to fetch from disk/pool
   * @return Pointer to Page in buffer pool, or nullptr if all pages are pinned
   *
   * **Returns nullptr when:**
   * - All frames are occupied AND all pages are pinned (pool exhaustion)
   *
   * **Thread Safety:** Entire operation happens under latch_ (Phase A)
   */
  Page* FetchPage(page_id_t page_id);

  /**
   * Allocates a new page on disk and loads it into the buffer pool.
   *
   * **Steps:**
   * 1. Find a free frame (or evict a victim page)
   * 2. Call disk_manager->AllocatePage() to get new page_id
   * 3. Initialize page metadata (pin_count = 1, is_dirty = true, data zeroed)
   * 4. Return page pointer
   *
   * **The new page is PINNED and DIRTY.**
   * - Pinned: Caller is using it
   * - Dirty: Caller will write data to it
   *
   * @param[out] page_id Set to the newly allocated page_id
   * @return Pointer to new Page in buffer pool, or nullptr if all pages are pinned
   *
   * **Returns nullptr when:**
   * - All frames are occupied AND all pages are pinned (pool exhaustion)
   */
  Page* NewPage(page_id_t* page_id);

  /**
   * Unpins a page, making it eligible for eviction.
   *
   * **Decrements pin_count.** When pin_count reaches 0, page becomes an eviction candidate.
   *
   * @param page_id The page to unpin
   * @param is_dirty Whether the page was modified
   *   - If true, marks page as dirty (needs write-back before eviction)
   *   - If false, does not change dirty flag
   *   - **Dirty flag is sticky:** Once true, stays true until written to disk
   *
   * @return false if page not in pool or already at pin_count 0
   *
   * **Typical usage pattern:**
   * @code
   *   Page* page = bpm->FetchPage(42);  // pin_count = 1
   *   // ... read page ...
   *   bpm->UnpinPage(42, false);        // pin_count = 0, not modified
   *
   *   page = bpm->FetchPage(42);        // pin_count = 1 again
   *   // ... modify page ...
   *   bpm->UnpinPage(42, true);         // pin_count = 0, mark dirty
   * @endcode
   */
  bool UnpinPage(page_id_t page_id, bool is_dirty);

  /**
   * Flushes a specific page to disk (writes if dirty).
   *
   * **Does not unpin the page.** Page can still be in use (pinned).
   *
   * @param page_id The page to flush
   * @return false if page not in pool
   *
   * **Use cases:**
   * - Force write before checkpoint
   * - Explicit durability guarantee
   * - Testing/debugging
   */
  bool FlushPage(page_id_t page_id);

  /**
   * Flushes all dirty pages to disk.
   *
   * Iterates through page_table, writes all dirty pages.
   * Called automatically by destructor.
   *
   * **Use cases:**
   * - Shutdown/cleanup
   * - Checkpoint/snapshot
   * - Testing/debugging
   */
  void FlushAllPages();

 private:
  /**
   * Finds a victim frame to evict.
   *
   * **Eviction policy: LRU (Least Recently Used)**
   * - Walks lru_list_ from back (oldest) to front (newest)
   * - Finds first frame with pin_count == 0
   * - If the victim is dirty, flushes its page to disk before reuse
   *   (write-before-evict — without this, modifications would be lost when
   *   the frame is overwritten)
   * - Removes from page_table and lru_list_
   *
   * @param[out] frame_id Set to victim frame_id if found
   * @return false if all pages are pinned (can't evict)
   *
   * **Called by:** FetchPage() and NewPage() when free_list_ is empty
   */
  bool FindVictimFrame(frame_id_t* frame_id);

  /**
   * Updates LRU ordering when a page is accessed.
   *
   * Moves frame to front of lru_list_ (most recently used).
   *
   * **LRU Invariant:**
   * - lru_list_.front() = Most Recently Used (MRU)
   * - lru_list_.back() = Least Recently Used (LRU)
   *
   * @param frame_id The frame to move to front
   *
   * **Called by:** FetchPage() on cache hit
   */
  void UpdateLRU(frame_id_t frame_id);

  // ============================================================================
  // Configuration
  // ============================================================================

  const size_t pool_size_;           ///< Number of frames in pool (fixed at construction)
  DiskManager* const disk_manager_;  ///< For page I/O (not owned, must outlive BufferPool)

  // ============================================================================
  // Core Data Structures
  // ============================================================================

  /**
   * Array of page frames.
   *
   * - Size: pool_size_
   * - Index: frame_id (0 to pool_size_-1)
   * - Each element is a Page object (4KB data + metadata)
   *
   * **Example (pool_size = 3):**
   * pages_[0] = Page(page_id=5, pin_count=1, is_dirty=false)
   * pages_[1] = Page(page_id=10, pin_count=0, is_dirty=true)
   * pages_[2] = Page(page_id=INVALID, pin_count=0, is_dirty=false)  // Free frame
   */
  std::vector<Page> pages_;

  /**
   * Maps page_id to frame_id (which frame holds which page).
   *
   * - Key: page_id_t (disk page identifier)
   * - Value: frame_id_t (index into pages_ array)
   * - Only contains pages currently in buffer pool
   *
   * **Example:**
   * page_table_ = {5 → 0, 10 → 1}
   * Means: Page 5 is in frame 0, Page 10 is in frame 1
   *
   * **Used for:**
   * - FetchPage: Check if page already in pool (cache hit)
   * - UnpinPage/FlushPage: Find frame for given page_id
   */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /**
   * List of free frames (not currently holding a page).
   *
   * - Initially: [0, 1, 2, ..., pool_size_-1]
   * - As pages loaded: frames removed from free_list_
   * - When page evicted: frame added back to free_list_
   *
   * **Optimization:** Use free frames before evicting (avoids I/O for victim write-back)
   *
   * **Invariant:** frame is in free_list_ XOR page_table_ (not both)
   */
  std::list<frame_id_t> free_list_;

  /**
   * LRU list for eviction policy.
   *
   * - Front = Most Recently Used (MRU)
   * - Back = Least Recently Used (LRU)
   * - Contains only occupied frames (not in free_list_)
   *
   * **Example (most recent access → oldest):**
   * lru_list_ = [3, 1, 0, 2]
   *              ↑           ↑
   *            MRU         LRU
   *
   * **Operations:**
   * - Access frame: Move to front (UpdateLRU)
   * - Evict: Walk from back, find first unpinned frame
   */
  std::list<frame_id_t> lru_list_;

  // ============================================================================
  // Concurrency Control
  // ============================================================================

  /**
   * Mutex protecting all BufferPoolManager data structures.
   *
   * **Protects:**
   * - page_table_
   * - free_list_
   * - lru_list_
   * - Page metadata: pin_count_, is_dirty_, page_id_
   *
   * **Does NOT protect:**
   * - Page data content (protected by Page latch_)
   *
   * **Phase A approach:** Coarse-grained locking
   * - Hold latch for entire operation (including I/O)
   * - Simple to reason about, but not optimal for concurrency
   * - Will optimize in Phase C (release latch during I/O)
   */
  std::mutex latch_;
};

}  // namespace db
