#pragma once

#include <cstring>        // For std::memset
#include <shared_mutex>   // For std::shared_mutex (C++17)

#include "columnar_db/common/config.h"
#include "columnar_db/common/types.h"

namespace db {

// Forward declaration for friend class
class BufferPoolManager;

/**
 * @class Page
 * @brief In-memory representation of a single database page with metadata.
 *
 * The Page class wraps a 4KB buffer (PAGE_SIZE) with metadata for buffer pool
 * management. It tracks which disk page it represents, whether it's been
 * modified (dirty), and how many threads are using it (pin_count).
 *
 * **Memory Layout (cache-optimized):**
 * - data_[4096]       ← Offset 0 (HOT: accessed frequently)
 * - page_id_          ← Offset 4096 (COLD: metadata)
 * - pin_count_
 * - is_dirty_
 * - latch_
 *
 * **Thread Safety Model:**
 * - Page latch (latch_) protects: data_[4096] ONLY (page content)
 * - BufferPool latch protects: pin_count_, is_dirty_, page_id_ (metadata)
 *
 * **Managed by BufferPoolManager:**
 * The Page class is not meant to be used directly. It's managed exclusively
 * by BufferPoolManager, which handles allocation, eviction, and metadata updates.
 *
 * **Key Concepts:**
 * - Pin counting: Reference counting for eviction control
 * - Dirty flag: Tracks if page needs write-back to disk
 * - Readers-writer lock: Multiple readers OR one writer for page content
 *
 * Example usage (via BufferPoolManager):
 * @code
 *   Page* page = buffer_pool.FetchPage(5);  // Pin page
 *   {
 *     Page::ReadGuard guard(page);          // Lock for reading
 *     // Read from page->data()
 *   }  // Auto-unlock
 *   buffer_pool.UnpinPage(5, false);        // Unpin (not dirty)
 * @endcode
 */
class Page {
  // BufferPoolManager is the ONLY class that should modify private metadata
  friend class BufferPoolManager;

 public:
  /**
   * @brief Constructor: Initializes page with zeroed data.
   *
   * Page starts in an "empty" state:
   * - Data is zero-filled
   * - page_id = INVALID_PAGE_ID (not associated with disk page yet)
   * - pin_count = 0 (not in use)
   * - is_dirty = false (clean)
   */
  Page() {
    ResetMemory();
  }

  /**
   * @brief Destructor: Default (no special cleanup needed)
   *
   * std::shared_mutex handles its own cleanup
   * Data array is on stack, no manual deallocation
   */
  ~Page() = default;

  // ============================================================================
  // Delete copy/move operations
  // ============================================================================

  /**
   * Pages cannot be copied because:
   * 1. std::shared_mutex is not copyable (concurrency primitive)
   * 2. Each Page is uniquely managed by BufferPoolManager
   * 3. Copying 4KB buffer is expensive and rarely needed
   */
  Page(const Page&) = delete;
  Page& operator=(const Page&) = delete;

  /**
   * Pages cannot be moved because:
   * 1. BufferPoolManager stores Pages in a fixed array
   * 2. Moving would invalidate pointers held by users
   * 3. Simpler ownership model (Pages never leave the pool)
   */
  Page(Page&&) = delete;
  Page& operator=(Page&&) = delete;

  // ============================================================================
  // Data Access (Public API)
  // ============================================================================

  /**
   * @brief Returns pointer to raw page data.
   * @return Mutable pointer to 4KB data buffer
   *
   * **Important:** Caller must hold appropriate latch:
   * - r_latch() for reading
   * - w_latch() for writing
   *
   * **Example:**
   * @code
   *   page->w_latch();
   *   char* data = page->data();
   *   memcpy(data, new_data, 100);
   *   page->w_unlatch();
   * @endcode
   */
  char* data() { return data_; }

  /**
   * @brief Returns const pointer to raw page data.
   * @return Const pointer to 4KB data buffer
   *
   * Const version for read-only access.
   */
  const char* data() const { return data_; }

  /**
   * @brief Returns the page ID (which disk page this represents).
   * @return page_id_t (disk page identifier) or INVALID_PAGE_ID if unassigned
   *
   * **Note:** page_id is set by BufferPoolManager when page is fetched.
   * Users should not rely on page_id being stable across unpins/fetches.
   */
  page_id_t page_id() const { return page_id_; }

  /**
   * @brief Returns the current pin count.
   * @return Number of threads currently using this page
   *
   * **Used by BufferPoolManager** to determine if page can be evicted.
   * - pin_count > 0: Page is in use, cannot evict
   * - pin_count = 0: Page is free, can be evicted
   */
  int GetPinCount() const { return pin_count_; }

  /**
   * @brief Checks if page has been modified.
   * @return true if page modified since last write to disk
   *
   * **Used by BufferPoolManager** to determine if page needs write-back
   * before eviction.
   */
  bool IsDirty() const { return is_dirty_; }

  // ============================================================================
  // Locking API - Manual (Explicit Control)
  // ============================================================================

  /**
   * @brief Acquires a read (shared) lock on the page.
   *
   * **Readers-Writer Lock Semantics:**
   * - Multiple readers can hold the lock simultaneously
   * - Blocks if a writer holds the lock
   * - Prevents writers from acquiring lock while held
   *
   * **Must call r_unlatch() when done!**
   *
   * **Use for:** Reading page data
   *
   * **Example:**
   * @code
   *   page->r_latch();
   *   // Read from page->data()
   *   page->r_unlatch();  // Don't forget!
   * @endcode
   */
  void r_latch() { latch_.lock_shared(); }

  /**
   * @brief Releases a read (shared) lock on the page.
   *
   * Must be called after r_latch().
   * Allows waiting writers to acquire the lock.
   */
  void r_unlatch() { latch_.unlock_shared(); }

  /**
   * @brief Acquires a write (exclusive) lock on the page.
   *
   * **Readers-Writer Lock Semantics:**
   * - Only ONE writer can hold the lock
   * - Blocks if any readers or writers hold the lock
   * - Prevents readers and writers from acquiring lock while held
   *
   * **Must call w_unlatch() when done!**
   *
   * **Use for:** Modifying page data
   *
   * **Example:**
   * @code
   *   page->w_latch();
   *   // Modify page->data()
   *   page->w_unlatch();  // Don't forget!
   * @endcode
   */
  void w_latch() { latch_.lock(); }

  /**
   * @brief Releases a write (exclusive) lock on the page.
   *
   * Must be called after w_latch().
   * Allows waiting readers and writers to acquire the lock.
   */
  void w_unlatch() { latch_.unlock(); }

  // ============================================================================
  // RAII Lock Guards (Exception-Safe, Automatic Unlock)
  // ============================================================================

  /**
   * @class ReadGuard
   * @brief RAII wrapper for read (shared) lock on a Page.
   *
   * Acquires lock in constructor, releases in destructor.
   * Guarantees unlock even if exception is thrown.
   *
   * **Example:**
   * @code
   *   {
   *     Page::ReadGuard guard(page);
   *     // Read from page->data()
   *     if (error) {
   *       throw std::runtime_error("Error");
   *       // Destructor STILL runs, lock released!
   *     }
   *   }  // Lock automatically released here
   * @endcode
   */
  class ReadGuard {
   public:
    /**
     * @brief Constructs guard and acquires read lock.
     * @param page Page to lock (must not be nullptr)
     */
    explicit ReadGuard(Page* page) : page_(page) {
      page_->r_latch();
    }

    /**
     * @brief Destructor: automatically releases read lock.
     *
     * Called when guard goes out of scope or during stack unwinding
     * (exception handling). Guarantees lock is always released.
     */
    ~ReadGuard() {
      page_->r_unlatch();
    }

    // Delete copy/move: Each guard should manage one lock acquisition
    ReadGuard(const ReadGuard&) = delete;
    ReadGuard& operator=(const ReadGuard&) = delete;
    ReadGuard(ReadGuard&&) = delete;
    ReadGuard& operator=(ReadGuard&&) = delete;

   private:
    Page* page_;  ///< Page being guarded
  };

  /**
   * @class WriteGuard
   * @brief RAII wrapper for write (exclusive) lock on a Page.
   *
   * Acquires lock in constructor, releases in destructor.
   * Guarantees unlock even if exception is thrown.
   *
   * **Example:**
   * @code
   *   {
   *     Page::WriteGuard guard(page);
   *     // Modify page->data()
   *     if (error) {
   *       throw std::runtime_error("Error");
   *       // Destructor STILL runs, lock released!
   *     }
   *   }  // Lock automatically released here
   * @endcode
   */
  class WriteGuard {
   public:
    /**
     * @brief Constructs guard and acquires write lock.
     * @param page Page to lock (must not be nullptr)
     */
    explicit WriteGuard(Page* page) : page_(page) {
      page_->w_latch();
    }

    /**
     * @brief Destructor: automatically releases write lock.
     *
     * Called when guard goes out of scope or during stack unwinding
     * (exception handling). Guarantees lock is always released.
     */
    ~WriteGuard() {
      page_->w_unlatch();
    }

    // Delete copy/move: Each guard should manage one lock acquisition
    WriteGuard(const WriteGuard&) = delete;
    WriteGuard& operator=(const WriteGuard&) = delete;
    WriteGuard(WriteGuard&&) = delete;
    WriteGuard& operator=(WriteGuard&&) = delete;

   private:
    Page* page_;  ///< Page being guarded
  };

 private:
  // ============================================================================
  // Private Methods (Only BufferPoolManager can call)
  // ============================================================================

  /**
   * @brief Zeros out all page data.
   *
   * Used during initialization and when recycling a page.
   * Ensures no data leaks between different page uses.
   */
  void ResetMemory() {
    std::memset(data_, 0, PAGE_SIZE);
  }

  // ============================================================================
  // Private Data Members
  // ============================================================================

  /**
   * Raw page data (4KB buffer).
   *
   * **Placed FIRST for cache efficiency:**
   * When CPU fetches Page*, data is at offset 0, likely in same cache line.
   * Since data accesses are much more frequent than metadata accesses,
   * this improves cache hit rate.
   */
  char data_[PAGE_SIZE]{};

  /**
   * Page identifier (which disk page this represents).
   *
   * INVALID_PAGE_ID (-1) means page is not currently associated with a disk page.
   * Set by BufferPoolManager when page is fetched.
   */
  page_id_t page_id_{INVALID_PAGE_ID};

  /**
   * Pin count (reference counter).
   *
   * Tracks how many threads are currently using this page.
   * - pin_count > 0: Page is "pinned", cannot be evicted
   * - pin_count = 0: Page can be evicted by BufferPoolManager
   *
   * **Thread Safety:** Modified only by BufferPoolManager while holding
   * its own latch. Plain int (not atomic) is sufficient.
   */
  int pin_count_{0};

  /**
   * Dirty flag (has page been modified?).
   *
   * - is_dirty = false: Page matches disk, can be discarded without write-back
   * - is_dirty = true: Page modified, MUST write to disk before evicting
   *
   * **Thread Safety:** Modified only by BufferPoolManager while holding
   * its own latch. Plain bool (not atomic) is sufficient.
   *
   * **Performance:** Dirty tracking avoids unnecessary disk writes for
   * read-only pages (huge win for read-heavy workloads).
   */
  bool is_dirty_{false};

  /**
   * Readers-writer lock (protects page data ONLY).
   *
   * **Protects:** data_[4096] (page content)
   * **Does NOT protect:** page_id_, pin_count_, is_dirty_ (BufferPool latch protects these)
   *
   * **std::shared_mutex (C++17):**
   * - lock_shared() / unlock_shared(): Shared (read) lock
   * - lock() / unlock(): Exclusive (write) lock
   * - Multiple readers OR one writer (classic readers-writer pattern)
   *
   * **Why not mutex?**
   * - std::mutex: Only one thread at a time (even for reads)
   * - std::shared_mutex: Many readers simultaneously (better for read-heavy workloads)
   */
  std::shared_mutex latch_;
};

}  // namespace db
