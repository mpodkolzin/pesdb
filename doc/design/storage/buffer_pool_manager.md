# BufferPoolManager Design Document - Phase A

**Component:** Storage Foundation - Buffer Pool Manager (Phase A: Simple)
**Phase:** 1, Step 3 of 26
**Status:** Design Complete
**Estimated Time:** 2-3 days

---

## Overview

The **BufferPoolManager** (BPM) is the memory cache layer that sits between the DiskManager and database operations. It's responsible for keeping frequently-accessed pages in memory and managing the limited pool of memory frames.

**Core Responsibility**: Cache disk pages in memory, manage eviction, and coordinate reads/writes through the DiskManager.

```
Application Layer (SQL Executor, etc.)
       ↓
Buffer Pool Manager ← YOU ARE HERE
       ↓
Disk Manager (page-based I/O)
       ↓
Operating System (file I/O)
       ↓
Physical Disk
```

**Think of it as:** A cache for database pages, like how your browser caches web pages. When you need page 42, the BPM checks if it's already in memory (cache hit). If not, it fetches from disk (cache miss).

---

## Goals - Phase A

1. **Core Lifecycle**: Implement fetch-pin-unpin-flush operations correctly
2. **Simple LRU**: List-based eviction policy (move to front on access)
3. **Memory Management**: Fixed-size pool (e.g., 100 pages)
4. **Correctness First**: Coarse-grained locking, focus on getting it right
5. **Learning Focus**: Understand buffer pool fundamentals before optimizing

**This is Phase A of a 3-phase plan:**
- **Phase A (this doc)**: Simple BufferPool with coarse-grained locking
- **Phase B (future)**: Add proper eviction with write-before-evict
- **Phase C (future)**: Optimize concurrency (release latch during I/O)

---

## Non-Goals - Phase A

- ❌ Fine-grained concurrency (Phase C optimization)
- ❌ Multiple replacement policies (just LRU for now)
- ❌ Prefetching or sequential scan optimization
- ❌ Page compression or encryption
- ❌ Multiple buffer pools (one global pool)

---

## Key Concepts You'll Learn

### 1. Frames vs Pages

**Critical distinction:**
- **Frame**: A slot in your buffer pool (memory location). Fixed number (e.g., 100 frames).
- **Page**: Data from disk (can have unlimited pages on disk).
- **page_table**: Maps `page_id → frame_id` (tells you which frame holds which page).

**Example:**
```
Buffer Pool (3 frames):
pages_[0] = Page(page_id=5, data=..., pin_count=1)
pages_[1] = Page(page_id=10, data=..., pin_count=0)
pages_[2] = Page(page_id=20, data=..., pin_count=2)

page_table = {5 → 0, 10 → 1, 20 → 2}
```

**Learning Point**: Disk has unlimited pages, but memory has limited frames. The page_table bridges this gap.

### 2. Pinning: Reference Counting for Eviction Control

**Why pinning?** Imagine you're reading a page and the buffer pool evicts it mid-read. Disaster!

**Pin semantics:**
```cpp
Page* page = bpm->FetchPage(42);  // pin_count++ (now 1)
// Use page safely - it won't be evicted!
bpm->UnpinPage(42, false);        // pin_count-- (now 0)
// Now eligible for eviction
```

**Rules:**
- Pinned pages (`pin_count > 0`) CANNOT be evicted
- Only unpinned pages (`pin_count == 0`) are eviction candidates
- Multiple threads can pin the same page (pin_count can be > 1)

**Learning Point**: Pinning is like reference counting - prevents use-after-free for pages.

### 3. LRU Eviction Policy (Least Recently Used)

**The idea:** When memory is full, evict the page that hasn't been used in the longest time.

**Data structure:** Doubly-linked list
```
lru_list_: [3, 1, 0, 2]
           ↑           ↑
          MRU         LRU
     (most recent)  (least recent)
```

**Operations:**
- **Access page**: Move to front (becomes Most Recently Used)
- **Need to evict**: Pick from back (Least Recently Used) if pin_count == 0

**Example:**
```
Initial: lru_list_ = [1, 0, 2]

FetchPage(0):  → [0, 1, 2]  // Move 0 to front
FetchPage(5):  → [3, 0, 1, 2]  // New page, add frame 3 to front

Evict: Walk from back → frame 2 (if pin_count == 0)
```

**Learning Point**: LRU approximates "keep hot pages in memory" - simple and effective!

### 4. Dirty Pages & Write-Back

**Dirty page:** Modified in memory, out of sync with disk.

```cpp
Page* page = bpm->FetchPage(42);
page->WriteLatch();
memcpy(page->GetData(), new_data, 100);  // Modify page
page->WriteUnlatch();
bpm->UnpinPage(42, true);  // Mark as dirty ← IMPORTANT!
```

**Why it matters:**
- **Before evicting a dirty page, you MUST write it to disk!**
- Otherwise: data loss (modified data only in memory, then lost)
- Clean pages can be evicted immediately (disk copy is current)

**Learning Point**: Dirty flag is critical for durability. This is how databases ensure you don't lose data.

### 5. The Latch (Mutex)

**Two levels of locking in our design:**

```
BufferPoolManager:
  latch_  ← Protects: page_table_, free_list_, lru_list_, pin_count_

Page:
  latch_  ← Protects: data_[4096] (page content)
```

**Why separate?**
- **BufferPool latch**: Short critical sections (update metadata)
- **Page latch**: Longer holds (reading/writing page content)

**Phase A approach:** Hold BufferPool latch for entire operation (simple, but not optimal).

**Learning Point**: Locking granularity matters. We'll optimize in Phase C.

---

## Design Decisions

### Decision 1: Coarse-Grained Locking (Phase A)

**Approach:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);  // Hold for ENTIRE operation

  // All work happens here (including I/O!)
  // ...

  return page;
  // Lock released
}
```

**Rationale:**
- **Simple to reason about**: No race conditions, easy to verify correctness
- **Good starting point**: Get it working first, optimize later
- **Learning focus**: Understand buffer pool mechanics before concurrency

**Trade-off:**
- **Con**: I/O happens while holding lock (blocks other threads)
- **Con**: Not production-grade performance

**Decision:** Acceptable for Phase A. We'll optimize in Phase C by releasing latch before I/O.

### Decision 2: LRU Eviction with std::list

**Approach:**
```cpp
std::list<frame_id_t> lru_list_;  // Front = MRU, Back = LRU

void UpdateLRU(frame_id_t frame_id) {
  lru_list_.remove(frame_id);      // O(n)
  lru_list_.push_front(frame_id);  // O(1)
}
```

**Rationale:**
- **Industry standard**: PostgreSQL uses LRU (with optimizations), SQLite uses similar
- **Simple to implement**: Standard library list works great
- **Good for learning**: Clear mental model (front = hot, back = cold)

**Alternative considered: Clock algorithm**
- **Pros**: O(1) eviction, more efficient
- **Cons**: More complex, harder to understand
- **Decision**: Save Clock for optional future enhancement

**Trade-off:**
- **Con**: `remove()` is O(n) - could optimize with iterators in page_table
- **Decision**: Acceptable for learning, can optimize later

### Decision 3: Free List for Initial Frames

**Approach:**
```cpp
std::list<frame_id_t> free_list_;  // [0, 1, 2, ..., pool_size-1] initially

// In FetchPage:
if (!free_list_.empty()) {
  frame_id = free_list_.front();
  free_list_.pop_front();
} else {
  FindVictimFrame(&frame_id);  // Must evict
}
```

**Rationale:**
- **Avoids early evictions**: Use free frames first before evicting
- **Clear separation**: Free frames vs. occupied frames
- **Matches industry**: PostgreSQL has similar concept (free buffer headers)

**Alternative considered: Single LRU list (no free list)**
- **Cons**: Would need special handling for "never used" frames
- **Decision**: Free list is cleaner

### Decision 4: Page Table with unordered_map

**Approach:**
```cpp
std::unordered_map<page_id_t, frame_id_t> page_table_;
```

**Rationale:**
- **Fast lookups**: O(1) average case (critical for cache hit path)
- **Simple API**: `page_table_[page_id] = frame_id`
- **Standard library**: Well-tested, portable

**Alternative considered: Custom hash table**
- **Cons**: Over-engineering for learning project
- **Decision**: Use standard library

---

## API Design

### Class: `BufferPoolManager`

```cpp
namespace db {

class BufferPoolManager {
 public:
  /**
   * Creates a buffer pool with fixed size.
   *
   * @param pool_size Number of pages in the buffer pool (e.g., 100)
   * @param disk_manager Pointer to disk manager (for I/O)
   */
  BufferPoolManager(size_t pool_size, DiskManager* disk_manager);

  /**
   * Destructor: Flushes all dirty pages to disk.
   */
  ~BufferPoolManager();

  // Delete copy/move (manages complex state, shouldn't be copied)
  BufferPoolManager(const BufferPoolManager&) = delete;
  BufferPoolManager& operator=(const BufferPoolManager&) = delete;

  /**
   * Fetches a page from the buffer pool.
   * If page is in pool, increment pin_count and return it.
   * If page is not in pool, load from disk into a free frame (or evict).
   *
   * @param page_id The page to fetch
   * @return Pointer to Page, or nullptr if all pages are pinned
   */
  Page* FetchPage(page_id_t page_id);

  /**
   * Allocates a new page on disk and loads it into the buffer pool.
   * The new page is pinned and marked dirty.
   *
   * @param[out] page_id Set to the new page_id
   * @return Pointer to new Page, or nullptr if all pages are pinned
   */
  Page* NewPage(page_id_t* page_id);

  /**
   * Unpins a page, making it eligible for eviction.
   * Decrements pin_count. If is_dirty is true, marks page as dirty.
   *
   * @param page_id The page to unpin
   * @param is_dirty Whether the page was modified
   * @return false if page not in pool or already unpinned
   */
  bool UnpinPage(page_id_t page_id, bool is_dirty);

  /**
   * Flushes a specific page to disk (writes if dirty).
   * Does not unpin the page.
   *
   * @param page_id The page to flush
   * @return false if page not in pool
   */
  bool FlushPage(page_id_t page_id);

  /**
   * Flushes all dirty pages to disk.
   * Called by destructor to ensure durability.
   */
  void FlushAllPages();

 private:
  /**
   * Finds a victim frame to evict.
   * Walks LRU list from back, finds first unpinned page.
   * (Phase A: Does NOT write dirty page - caller must handle)
   *
   * @param[out] frame_id Set to victim frame_id
   * @return false if all pages are pinned (can't evict)
   */
  bool FindVictimFrame(frame_id_t* frame_id);

  /**
   * Updates LRU ordering when a page is accessed.
   * Moves frame to front of lru_list_ (most recently used).
   *
   * @param frame_id The frame to update
   */
  void UpdateLRU(frame_id_t frame_id);

  // Configuration
  const size_t pool_size_;           // Number of frames in pool
  DiskManager* const disk_manager_;  // For page I/O

  // Core data structures
  std::vector<Page> pages_;          // Array of page frames (size = pool_size_)
  std::unordered_map<page_id_t, frame_id_t> page_table_;  // page_id → frame_id
  std::list<frame_id_t> free_list_;  // Free frames (initially all frames)
  std::list<frame_id_t> lru_list_;   // LRU ordering (front=MRU, back=LRU)

  // Concurrency control
  std::mutex latch_;                 // Protects all data structures
};

}  // namespace db
```

---

## Implementation Details

### Constructor: Initialize Pool

```cpp
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size),
      disk_manager_(disk_manager),
      pages_(pool_size) {  // Allocate all frames

  // Initially, all frames are free
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(static_cast<frame_id_t>(i));
  }
}
```

**Learning Points:**
- `pages_(pool_size)` allocates vector of Page objects
- All frames start on free_list
- lru_list_ starts empty (populated as pages loaded)

### Destructor: Flush All

```cpp
BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();  // Ensure durability before shutdown
}
```

**Learning Point:** Critical for durability! Without this, dirty pages lost on shutdown.

### FetchPage: Core Operation

```cpp
Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Case 1: Page already in pool (cache hit)
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    pages_[frame_id].pin_count_++;  // Pin it (BufferPool modifies directly)
    UpdateLRU(frame_id);             // Move to front
    return &pages_[frame_id];
  }

  // Case 2: Page not in pool (cache miss) - need a frame
  frame_id_t frame_id;

  // Try free list first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // No free frames - must evict
    if (!FindVictimFrame(&frame_id)) {
      return nullptr;  // All pages pinned - can't evict!
    }
  }

  // Now we have a frame - load page from disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // Update page metadata (BufferPool has friend access)
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;     // Newly fetched page is pinned
  pages_[frame_id].is_dirty_ = false;  // Fresh from disk (clean)
  pages_[frame_id].ResetMemory();      // Clear old data first

  // Update BufferPool metadata
  page_table_[page_id] = frame_id;
  lru_list_.push_front(frame_id);  // Most recently used

  return &pages_[frame_id];
}
```

**Learning Points:**
1. **Cache hit path**: Very fast (just increment pin_count and update LRU)
2. **Cache miss path**: Must find frame, then load from disk
3. **Free list optimization**: Avoid eviction when possible
4. **Pin on fetch**: Caller gets pinned page (safe to use)
5. **Error handling**: Return nullptr if all pages pinned

**Flow diagram:**
```
FetchPage(42)
    |
    ├─ In page_table? ──YES──> Increment pin_count, UpdateLRU, return page
    |
    └─ NO (cache miss)
        |
        ├─ Free frames? ──YES──> Use free frame
        |
        └─ NO ──> FindVictimFrame
                   |
                   ├─ Found unpinned? ──YES──> Use victim frame
                   |
                   └─ NO ──> Return nullptr (all pinned!)
```

### NewPage: Allocate New Page

```cpp
Page* BufferPoolManager::NewPage(page_id_t* page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Need a frame for the new page
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!FindVictimFrame(&frame_id)) {
      return nullptr;  // All pages pinned
    }
  }

  // Allocate new page on disk (DiskManager assigns page_id)
  page_id_t new_page_id = disk_manager_->AllocatePage();
  *page_id = new_page_id;  // Return to caller

  // Initialize page metadata
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;     // New page is pinned
  pages_[frame_id].is_dirty_ = true;   // New page is dirty (not on disk yet with user data)
  pages_[frame_id].ResetMemory();      // Zero out the page

  // Update BufferPool metadata
  page_table_[new_page_id] = frame_id;
  lru_list_.push_front(frame_id);

  return &pages_[frame_id];
}
```

**Learning Points:**
- New pages are always **dirty** (caller will write data)
- New pages are always **pinned** (caller using them)
- `AllocatePage()` returns the new page_id
- Page is zero-initialized (clean slate)

### UnpinPage: Release Page

```cpp
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // Page not in pool
  }

  frame_id_t frame_id = it->second;

  if (pages_[frame_id].pin_count_ <= 0) {
    return false;  // Already unpinned (error)
  }

  // Decrement pin count
  pages_[frame_id].pin_count_--;

  // Update dirty flag (once dirty, stays dirty until flush)
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  return true;
}
```

**Learning Points:**
- **Dirty flag is sticky**: Once true, stays true until written to disk
- **Pin count can go to 0**: Now eligible for eviction
- **Error checking**: Can't unpin if not in pool or already at 0

**Common pattern:**
```cpp
Page* page = bpm->FetchPage(42);  // pin_count = 1
// Read page...
bpm->UnpinPage(42, false);        // pin_count = 0, not dirty

page = bpm->FetchPage(42);        // pin_count = 1
// Modify page...
bpm->UnpinPage(42, true);         // pin_count = 0, DIRTY
```

### FlushPage: Write to Disk

```cpp
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // Not in pool
  }

  frame_id_t frame_id = it->second;

  // Only write if dirty
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;  // Now clean
  }

  return true;
}
```

**Learning Point:** Check `is_dirty_` before writing (skip unnecessary I/O).

### FlushAllPages: Write All Dirty Pages

```cpp
void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (const auto& [page_id, frame_id] : page_table_) {
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
  }
}
```

**Learning Point:** Iterate page_table (only occupied frames) to find dirty pages.

### FindVictimFrame: Simple Eviction (Phase A)

```cpp
bool BufferPoolManager::FindVictimFrame(frame_id_t* frame_id) {
  // Walk LRU list from back (least recently used)
  for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
    frame_id_t candidate = *it;

    // Can only evict unpinned pages
    if (pages_[candidate].GetPinCount() == 0) {
      *frame_id = candidate;

      // Phase A: Simple version - just remove from metadata
      // (Caller will overwrite with new page data)
      page_table_.erase(pages_[candidate].GetPageId());
      lru_list_.erase(std::next(it).base());  // Convert reverse_iterator to forward

      // TODO Phase B: Write dirty page before evicting!
      // if (pages_[candidate].IsDirty()) {
      //   disk_manager_->WritePage(pages_[candidate].GetPageId(),
      //                            pages_[candidate].GetData());
      // }

      return true;
    }
  }

  return false;  // All pages pinned - can't evict!
}
```

**Learning Points:**
- Walk LRU from back (coldest pages first)
- Check `pin_count == 0` (only evict unpinned)
- Remove from page_table and lru_list
- **Phase A limitation**: Doesn't write dirty pages! (Will add in Phase B)

**Phase B improvement:**
We'll add write-before-evict to ensure durability:
```cpp
if (pages_[candidate].IsDirty()) {
  disk_manager_->WritePage(pages_[candidate].GetPageId(),
                          pages_[candidate].GetData());
  pages_[candidate].is_dirty_ = false;
}
```

### UpdateLRU: Move to Front

```cpp
void BufferPoolManager::UpdateLRU(frame_id_t frame_id) {
  lru_list_.remove(frame_id);      // Remove from current position (O(n))
  lru_list_.push_front(frame_id);  // Add to front (most recently used)
}
```

**Learning Point:** Simple but O(n). Could optimize by storing iterators in page_table.

---

## Data Structures State Machine

### Example: Buffer Pool with 3 Frames

**Initial state:**
```
pages_ = [Page(), Page(), Page()]  // All invalid (page_id = -1)
page_table_ = {}
free_list_ = [0, 1, 2]
lru_list_ = []
```

**After FetchPage(5):**
```
pages_[0] = Page(page_id=5, pin_count=1, is_dirty=false)
pages_[1] = Page()
pages_[2] = Page()

page_table_ = {5 → 0}
free_list_ = [1, 2]
lru_list_ = [0]
```

**After FetchPage(10):**
```
pages_[0] = Page(page_id=5, pin_count=1, is_dirty=false)
pages_[1] = Page(page_id=10, pin_count=1, is_dirty=false)
pages_[2] = Page()

page_table_ = {5 → 0, 10 → 1}
free_list_ = [2]
lru_list_ = [1, 0]  // 10 is MRU, 5 is LRU
```

**After UnpinPage(5, false):**
```
pages_[0].pin_count = 0  // Now eligible for eviction
(Everything else unchanged)
```

**After FetchPage(5) again (cache hit):**
```
pages_[0].pin_count = 1  // Pinned again
lru_list_ = [0, 1]  // 5 moved to front (MRU)
```

**After FetchPage(20) (must evict):**
```
FindVictimFrame walks lru_list from back:
  - Frame 1 (page 10): pin_count = 1 ❌ (pinned)
  - Frame 0 (page 5): pin_count = 0 ✅ (can evict)

Evict page 5 from frame 0, load page 20:
pages_[0] = Page(page_id=20, pin_count=1, is_dirty=false)

page_table_ = {10 → 1, 20 → 0}
lru_list_ = [0, 1]  // 20 is MRU
```

---

## Testing Strategy

### Unit Tests (Google Test)

**Test File:** `tests/unit/storage/buffer_pool_manager_test.cpp`

#### Test 1: Basic Fetch and Unpin

```cpp
TEST(BufferPoolManagerTest, FetchAndUnpin) {
  const std::string db_file = "test_bpm.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);

  // Allocate a page on disk
  page_id_t page0_id = disk_mgr->AllocatePage();

  // Create buffer pool (size 10)
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  // Fetch page (cache miss)
  Page* page0 = bpm->FetchPage(page0_id);
  ASSERT_NE(page0, nullptr);
  EXPECT_EQ(page0->GetPageId(), page0_id);
  EXPECT_EQ(page0->GetPinCount(), 1);

  // Unpin
  EXPECT_TRUE(bpm->UnpinPage(page0_id, false));
  EXPECT_EQ(page0->GetPinCount(), 0);

  std::filesystem::remove(db_file);
}
```

#### Test 2: Cache Hit

```cpp
TEST(BufferPoolManagerTest, CacheHit) {
  const std::string db_file = "test_cache.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);
  page_id_t page0_id = disk_mgr->AllocatePage();
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  // First fetch (cache miss)
  Page* page0_first = bpm->FetchPage(page0_id);
  ASSERT_NE(page0_first, nullptr);
  bpm->UnpinPage(page0_id, false);

  // Second fetch (cache hit - should be same pointer)
  Page* page0_second = bpm->FetchPage(page0_id);
  ASSERT_NE(page0_second, nullptr);
  EXPECT_EQ(page0_first, page0_second);  // Same frame!
  EXPECT_EQ(page0_second->GetPinCount(), 1);

  bpm->UnpinPage(page0_id, false);
  std::filesystem::remove(db_file);
}
```

#### Test 3: NewPage

```cpp
TEST(BufferPoolManagerTest, NewPage) {
  const std::string db_file = "test_new.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  // Allocate new page through BPM
  page_id_t new_page_id;
  Page* new_page = bpm->NewPage(&new_page_id);

  ASSERT_NE(new_page, nullptr);
  EXPECT_EQ(new_page->GetPageId(), new_page_id);
  EXPECT_EQ(new_page->GetPinCount(), 1);
  EXPECT_TRUE(new_page->IsDirty());  // New pages are dirty

  bpm->UnpinPage(new_page_id, false);
  std::filesystem::remove(db_file);
}
```

#### Test 4: Multiple Pins (Reference Counting)

```cpp
TEST(BufferPoolManagerTest, MultiplePins) {
  const std::string db_file = "test_pins.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);
  page_id_t page0_id = disk_mgr->AllocatePage();
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  // Fetch multiple times
  Page* page1 = bpm->FetchPage(page0_id);
  Page* page2 = bpm->FetchPage(page0_id);
  Page* page3 = bpm->FetchPage(page0_id);

  EXPECT_EQ(page1->GetPinCount(), 3);

  // Unpin one by one
  bpm->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 2);

  bpm->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 1);

  bpm->UnpinPage(page0_id, false);
  EXPECT_EQ(page1->GetPinCount(), 0);  // Now eligible for eviction

  std::filesystem::remove(db_file);
}
```

#### Test 5: Write and Read Data

```cpp
TEST(BufferPoolManagerTest, WriteAndReadData) {
  const std::string db_file = "test_data.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_mgr.get());

  // Create new page and write data
  page_id_t page_id;
  Page* page = bpm->NewPage(&page_id);
  ASSERT_NE(page, nullptr);

  // Write known pattern
  char* data = page->GetData();
  const char* test_string = "Hello, Buffer Pool!";
  strcpy(data, test_string);

  bpm->UnpinPage(page_id, true);  // Mark as dirty
  bpm->FlushPage(page_id);        // Force write to disk

  // Clear from buffer pool (evict)
  // ... (would need to fill pool and evict, or restart BPM)

  // Re-fetch and verify
  page = bpm->FetchPage(page_id);
  EXPECT_STREQ(page->GetData(), test_string);

  bpm->UnpinPage(page_id, false);
  std::filesystem::remove(db_file);
}
```

#### Test 6: LRU Eviction (Phase A - No Write-Before-Evict Yet)

```cpp
TEST(BufferPoolManagerTest, SimpleLRUEviction) {
  const std::string db_file = "test_lru.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);

  // Small buffer pool (3 pages)
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());

  // Create 3 pages (fill the pool)
  page_id_t pid0, pid1, pid2;
  Page* page0 = bpm->NewPage(&pid0);
  Page* page1 = bpm->NewPage(&pid1);
  Page* page2 = bpm->NewPage(&pid2);

  ASSERT_NE(page0, nullptr);
  ASSERT_NE(page1, nullptr);
  ASSERT_NE(page2, nullptr);

  // Unpin all (make eligible for eviction)
  bpm->UnpinPage(pid0, false);
  bpm->UnpinPage(pid1, false);
  bpm->UnpinPage(pid2, false);

  // Access pid1 (moves to front of LRU)
  bpm->FetchPage(pid1);
  bpm->UnpinPage(pid1, false);

  // Create 4th page - should evict LRU (pid0)
  page_id_t pid3;
  Page* page3 = bpm->NewPage(&pid3);
  ASSERT_NE(page3, nullptr);

  // Trying to fetch pid0 should work (re-load from disk)
  page0 = bpm->FetchPage(pid0);
  ASSERT_NE(page0, nullptr);  // Should succeed

  bpm->UnpinPage(pid0, false);
  bpm->UnpinPage(pid3, false);
  std::filesystem::remove(db_file);
}
```

#### Test 7: All Pages Pinned (Cannot Evict)

```cpp
TEST(BufferPoolManagerTest, AllPagesPinned) {
  const std::string db_file = "test_pinned.db";
  std::filesystem::remove(db_file);

  auto disk_mgr = std::make_unique<DiskManager>(db_file);

  // Small buffer pool (2 pages)
  auto bpm = std::make_unique<BufferPoolManager>(2, disk_mgr.get());

  // Create 2 pages and keep them pinned
  page_id_t pid0, pid1;
  Page* page0 = bpm->NewPage(&pid0);
  Page* page1 = bpm->NewPage(&pid1);

  ASSERT_NE(page0, nullptr);
  ASSERT_NE(page1, nullptr);

  // Try to create 3rd page - should fail (all pinned)
  page_id_t pid2;
  Page* page2 = bpm->NewPage(&pid2);
  EXPECT_EQ(page2, nullptr);  // Should return nullptr

  // Unpin one page
  bpm->UnpinPage(pid0, false);

  // Now should succeed
  page2 = bpm->NewPage(&pid2);
  EXPECT_NE(page2, nullptr);

  std::filesystem::remove(db_file);
}
```

### Manual Testing Ideas

**Test 1: Visualize Buffer Pool State**

Add debug print method:
```cpp
void BufferPoolManager::DebugPrint() {
  std::cout << "=== Buffer Pool State ===" << std::endl;
  std::cout << "Free frames: " << free_list_.size() << std::endl;
  std::cout << "Occupied frames: " << page_table_.size() << std::endl;

  std::cout << "LRU order (MRU → LRU): ";
  for (auto fid : lru_list_) {
    std::cout << fid << " ";
  }
  std::cout << std::endl;

  for (const auto& [page_id, frame_id] : page_table_) {
    std::cout << "  Page " << page_id << " → Frame " << frame_id
              << " (pin=" << pages_[frame_id].GetPinCount()
              << ", dirty=" << pages_[frame_id].IsDirty() << ")" << std::endl;
  }
}
```

**Test 2: Benchmark Cache Hit Ratio**

```cpp
// Fetch 1000 pages with some repeats
// Measure cache hits vs misses
```

---

## Implementation Plan

1. ✅ Design exploration complete (`doc/design_exploration.md`)
2. ✅ Write formal design doc (this file)
3. → Create `include/columnar_db/storage/buffer_pool_manager.h`
4. → Create `src/storage/buffer_pool_manager.cpp`
5. → Update `src/storage/CMakeLists.txt`
6. → Create `tests/unit/storage/buffer_pool_manager_test.cpp`
7. → Update `tests/unit/CMakeLists.txt`
8. 🔨 Build and test: Run all tests and verify correctness
9. 📝 Document learnings: Update with what we learned

---

## Success Criteria - Phase A

✅ All unit tests pass
✅ Can fetch pages (cache hit and miss)
✅ Can create new pages
✅ Pin counting works correctly (reference counting)
✅ LRU eviction works (evicts oldest unpinned page)
✅ Returns nullptr when all pages pinned
✅ Dirty flag tracking works
✅ FlushPage and FlushAllPages write to disk
✅ Code compiles with no warnings

**Known limitation (Phase A):**
- ⚠️ FindVictimFrame doesn't write dirty pages before evicting (will fix in Phase B)

---

## What You'll Learn - Phase A

After implementing this component, you will deeply understand:

### Database Concepts
1. **Buffer Pool Mechanics**: How databases cache pages in memory
2. **Pinning**: Reference counting to prevent use-after-free
3. **LRU Eviction**: Simple and effective page replacement policy
4. **Dirty Pages**: Tracking modifications for write-back
5. **Cache Hit/Miss**: Performance implications of buffer pool size

### C++ Concepts
1. **STL Containers**: vector, unordered_map, list usage
2. **Mutex & Lock Guards**: Basic thread safety
3. **Friend Classes**: BufferPool modifying Page internals
4. **Resource Management**: RAII for cleanup (destructor flush)
5. **Error Handling**: Returning nullptr on failure

### Systems Concepts
1. **Memory Hierarchy**: Bridging RAM and disk
2. **Page Table**: Virtual-to-physical mapping analog
3. **Reference Counting**: When resources are in use
4. **Eviction Policies**: Approximating optimal replacement

---

## Phase B Preview: Add Proper Eviction

**What we'll add:**
```cpp
bool FindVictimFrame(frame_id_t* frame_id) {
  for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
    frame_id_t candidate = *it;

    if (pages_[candidate].GetPinCount() == 0) {
      *frame_id = candidate;

      // ← ADD THIS: Write dirty page before evicting!
      if (pages_[candidate].IsDirty()) {
        disk_manager_->WritePage(
          pages_[candidate].GetPageId(),
          pages_[candidate].GetData()
        );
        pages_[candidate].is_dirty_ = false;
      }

      page_table_.erase(pages_[candidate].GetPageId());
      lru_list_.erase(std::next(it).base());
      return true;
    }
  }
  return false;
}
```

**Why important:** Without this, evicting dirty pages = data loss!

---

## Phase C Preview: Optimize Concurrency

**The problem with Phase A:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);  // Held for entire operation

  // ... metadata work ...
  disk_manager_->ReadPage(...);  // ← I/O while holding lock! (SLOW)
  // ... more metadata work ...

  return page;
  // Lock released here
}
```

**Impact:** I/O takes milliseconds. Other threads blocked entire time. Bad for concurrency!

**Phase C optimization:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);

  // ... find victim, copy dirty data to temp buffer ...

  lock.unlock();  // ← Release BEFORE I/O
  disk_manager_->ReadPage(...);  // I/O without holding lock
  lock.lock();  // Re-acquire after I/O

  // ... finish up ...
  return page;
}
```

**Why complex:**
- Between unlock/lock, other threads can modify state
- Need careful error handling and rollback

**Learning value:** How production databases achieve high concurrency.

---

## References

**Industry Implementations:**
- **PostgreSQL**: `src/backend/storage/buffer/bufmgr.c` - Production buffer manager
- **MySQL InnoDB**: `storage/innobase/buf/` - Buffer pool implementation
- **SQLite**: `src/pager.c` - Simpler pager (similar concept)
- **BusTub** (CMU 15-445): Educational buffer pool (good reference)

**Academic Papers:**
- "The Five-Minute Rule" (Gray & Graefe) - When to cache in memory vs. disk
- "LRU-K: An O(1) Algorithm for Buffer Management" - Advanced eviction

**Books:**
- "Database System Concepts" Ch. 10 - Buffer Management
- "Database Internals" Ch. 5 - Buffer Pool Management

**Your old code:**
- `old/src/storage/buffer_pool_manager.cpp` - Advanced reference with Phase C optimizations

---

**Ready to implement Phase A!** 🚀

Let's create the header and implementation files next.
