# Design Exploration Log

This document captures brainstorming sessions and design decisions made during the learning journey.

---

## Session 1: Page Class Design (2026-01-08)

### Problem Statement

Need to build an **in-memory Page representation** that wraps the raw `char[4096]` buffer with metadata for buffer pool management.

**Requirements:**
- Wrap page data with metadata (page_id, pin_count, dirty flag)
- Thread-safe access to page content (readers-writer lock)
- Reference counting for eviction control (pin_count)
- Track modifications (dirty flag for write-back)
- Managed exclusively by BufferPoolManager

**Learning Goals:**
- Memory layout and cache considerations
- Thread safety with std::shared_mutex
- Friend class pattern for encapsulation
- RAII for lock management
- Reference counting patterns
- Atomic operations vs. mutex protection

---

### Ideas Explored

#### Idea 1: Classic Buffer Pool Page (Industry Standard)

**Approach:**
```cpp
class Page {
  friend class BufferPoolManager;

public:
  char* data() { return data_; }
  page_id_t page_id() const { return page_id_; }

  void r_latch() { latch_.lock_shared(); }
  void r_unlatch() { latch_.unlock_shared(); }
  void w_latch() { latch_.lock(); }
  void w_unlatch() { latch_.unlock(); }

private:
  char data_[PAGE_SIZE]{};           // Data first (cache-friendly)
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  std::shared_mutex latch_;
};
```

**Learning Value:**
- **Friend class pattern**: BufferPool modifies private members, users can't
- **Readers-writer lock**: Multiple readers OR one writer for page content
- **Memory layout**: Data first for cache locality
- **Manual locking**: Explicit control but easy to forget unlock

**Matches:** PostgreSQL, BusTub, most production databases

---

#### Idea 2: Add RAII Lock Guards (Modern C++)

**Enhancement to Idea 1:**
```cpp
class Page {
  // ... same as Idea 1 ...

  class ReadGuard {
  public:
    explicit ReadGuard(Page* page) : page_(page) {
      page_->latch_.lock_shared();
    }
    ~ReadGuard() { page_->latch_.unlock_shared(); }
    ReadGuard(const ReadGuard&) = delete;
  private:
    Page* page_;
  };

  class WriteGuard {
  public:
    explicit WriteGuard(Page* page) : page_(page) {
      page_->latch_.lock();
    }
    ~WriteGuard() { page_->latch_.unlock(); }
    WriteGuard(const WriteGuard&) = delete;
  private:
    Page* page_;
  };
};

// Usage:
{
  Page::ReadGuard guard(page);  // Lock acquired
  // Access page->data()
}  // Lock automatically released!
```

**Learning Value:**
- **RAII pattern for locks**: Automatic unlock on scope exit
- **Exception safety**: Lock released even if exception thrown
- **Scope-based locking**: Clear lifetime of lock hold
- **Can't forget to unlock**: Compiler ensures cleanup

**Provides both APIs**: Manual (for BufferPool) and RAII (for safety)

---

#### Idea 3: Atomic Pin Count (Performance Optimization)

**Alternative:**
```cpp
class Page {
private:
  std::atomic<int> pin_count_{0};  // Lock-free increment/decrement
};
```

**Learning Value:**
- **Lock-free programming**: No mutex needed for pin_count
- **Atomic operations**: Thread-safe without locks
- **Performance**: Cheaper than mutex for reference counting

**Trade-offs:**
- More complex reasoning about thread safety
- BufferPool still needs latch for page_table operations
- Can add later as optimization

---

### Decision: Plain int pin_count + BufferPool Latch Protection

**Chosen Approach:** Idea 1 (Classic) + Idea 2 (RAII Guards)

**For pin_count, we chose: Plain `int` (not atomic)**

#### Rationale: Two-Level Locking Model

```
BufferPoolManager:
  Latch: latch_  ← Protects:
    • page_table_ (page_id → frame mapping)
    • free_list_
    • pin_count_   ← PROTECTED BY THIS LATCH
    • is_dirty_

Page:
  Latch: latch_  ← Protects:
    • data_[4096] ONLY (page content)
```

**How thread safety works:**
```cpp
class BufferPoolManager {
  Page* FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);  // Acquire BufferPool latch

    // Safe to modify pin_count - we hold the latch
    Page* page = find_in_page_table(page_id);
    page->pin_count_++;  // Protected by BufferPool latch

    return page;
    // Release latch
  }
};
```

**Why plain int is sufficient:**
- BufferPool always modifies pin_count while holding its own latch
- Page latch protects page DATA, not metadata
- This is the PostgreSQL/BusTub model (industry standard)
- Simpler to reason about: "BufferPool latch protects all metadata"

**Why NOT atomic:**
- Would allow lock-free pin_count increment
- But BufferPool still needs latch for page_table lookups
- Minimal performance benefit, added complexity
- Can optimize later after measuring if needed

#### Alternative Considered: Atomic Pin Count

**Could release BufferPool latch earlier:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  Page* page = find_in_page_table(page_id);
  lock.unlock();  // Release early

  page->pin_count_++;  // Atomic, no latch needed
  return page;
}
```

**Rejected for learning phase because:**
- More complex mental model (two different protection mechanisms)
- Harder to debug and test
- Can add as optimization exercise later
- Standard approach is simpler and proven

---

### Memory Layout Decision: Data First

```cpp
// Our layout:
class Page {
private:
  char data_[PAGE_SIZE]{};           // Offset 0 (HOT data)
  page_id_t page_id_;                // Offset 4096
  int pin_count_;                    // Offset 4100
  bool is_dirty_;                    // Offset 4104
  std::shared_mutex latch_;          // Offset 4108
};

Total size: ~4096 + 48 bytes
```

**Why data first:**
- `page->data()` access → offset 0 → CPU cache hit likely
- Data accesses are MUCH more frequent than metadata accesses
- Matches industry practice (cache-friendly layout)

**Alternative (metadata first) rejected:**
- Logical grouping but poor cache behavior
- Data at offset 48 → more cache misses

---

### Friend Class Pattern Decision

```cpp
friend class BufferPoolManager;
```

**Why friend:**
- BufferPool needs to modify private members (pin_count_, is_dirty_, page_id_)
- Users should NOT modify these (safety)
- Friend grants controlled access without public setters
- Documents intent: "Page is managed by BufferPoolManager"

**Alternative (public setters) rejected:**
```cpp
void set_pin_count(int count);  // BAD: Anyone can call!
```
- Breaks encapsulation
- No compile-time safety
- Easy to misuse from application code

---

### Lock API Decision: Manual + RAII (Both)

**Provide both locking APIs:**

**Manual (for BufferPool internal use):**
```cpp
page->r_latch();
// Access data
page->r_unlatch();
```

**RAII Guards (for safety/application use):**
```cpp
{
  Page::ReadGuard guard(page);
  // Access data
}  // Auto-unlock
```

**Why both:**
- BufferPool developers know what they're doing (manual is fine)
- Application code should use guards (safer, exception-safe)
- Guards teach RAII pattern
- Manual shows explicit control
- Can measure if guards have overhead (they shouldn't)

---

### What We'll Learn Building This

**C++ Concepts:**
1. **Friend classes**: Controlled access to private members
2. **std::shared_mutex**: Readers-writer lock implementation
3. **Memory layout**: Cache-friendly data structure design
4. **RAII for locks**: Scope-based resource management
5. **Deleted copy/move**: Preventing copies of non-copyable resources
6. **Fixed-size arrays**: Stack allocation vs. heap

**Database Concepts:**
1. **Pin counting**: Reference counting for buffer pool eviction
2. **Dirty flag**: Tracking modifications for write-back
3. **Page latch vs BufferPool latch**: Two-level locking model
4. **Readers-writer concurrency**: Multiple readers, single writer
5. **Buffer pool management**: How pages are cached in memory

**Systems Concepts:**
1. **Cache locality**: Why data layout matters for performance
2. **Lock granularity**: Page-level vs. pool-level locking
3. **Thread safety**: Mutex vs. atomic protection trade-offs

---

### Testing Strategy

**Unit tests to write:**
1. **Basic functionality**:
   - Create page, access data
   - Set/get page_id
   - Verify zero-initialization

2. **Locking**:
   - Acquire/release read latch
   - Acquire/release write latch
   - Test RAII guards (auto-unlock)
   - Multiple readers simultaneously (shared lock)
   - Writer blocks readers (exclusive lock)

3. **Thread safety** (multi-threaded tests):
   - Multiple threads reading same page
   - Writer waits for readers
   - No data races on pin_count (protected by BufferPool latch in real usage)

4. **Memory**:
   - Page size is correct (sizeof check)
   - Data is cache-aligned
   - Can't copy a Page (deleted copy constructor)

---

### Implementation Plan

**Phase 1: Basic Page (30 mins)**
1. Create `include/columnar_db/storage/page.h`
2. Implement core structure (data, page_id, pin_count, is_dirty)
3. Add friend class declaration
4. Implement basic accessors

**Phase 2: Locking (30 mins)**
5. Add std::shared_mutex member
6. Implement r_latch/r_unlatch, w_latch/w_unlatch
7. Add ReadGuard and WriteGuard nested classes

**Phase 3: Testing (1 hour)**
8. Create `tests/unit/storage/page_test.cpp`
9. Write basic functionality tests
10. Write locking tests
11. Write multi-threaded tests (optional, more for BufferPool)

**Phase 4: Documentation (30 mins)**
12. Add comprehensive doc comments
13. Update design_exploration.md with learnings
14. Prepare for BufferPoolManager integration

**Total estimated time: 2-3 hours**

---

### Next Steps

1. ✅ Design exploration complete (this document)
2. → **Write formal design document** (`doc/design/storage/page.md`)
   - Use Design Document Assistant mode
3. → Implement Page class
4. → Write tests
5. → Document learnings
6. → Ready for BufferPoolManager (next component)

---

### References

**Industry implementations:**
- **PostgreSQL**: `src/include/storage/buf.h` - Buffer descriptor (similar concept)
- **BusTub** (CMU 15-445): `src/include/storage/page/page.h` - Educational reference
- **DuckDB**: `src/storage/buffer/buffer_handle.hpp` - Modern C++ approach

**Concepts:**
- Readers-writer locks: Multiple readers OR one writer
- RAII: Resource Acquisition Is Initialization
- Friend classes: Controlled breaking of encapsulation
- Cache-friendly data layout: Hot data first

---

**Status:** Design complete, ready for formal design doc and implementation.

---

## Session 2: BufferPoolManager Design (2026-01-09)

### Problem Statement

Need to build a **BufferPoolManager** that acts as a memory cache between the disk manager (slow I/O) and database operations (fast memory access).

**Requirements:**
- Fixed-size pool of Page frames (e.g., 100 pages)
- Translate page_id to in-memory location (page table)
- Fetch pages from disk on cache miss
- Evict pages when pool is full (LRU policy)
- Pin pages in use (prevent eviction)
- Write dirty pages back to disk

**Learning Goals:**
- Understand buffer pool lifecycle (fetch-pin-unpin-flush)
- Learn LRU eviction policies
- Understand dirty page management and write-back
- Learn about concurrency in buffer pools
- Understand memory pressure handling

---

### Database Concepts Involved

#### 1. Frames vs Pages
- **Frame**: A slot in your buffer pool (memory location)
- **Page**: Data from disk that occupies a frame
- Fixed number of frames (e.g., 100), but unlimited pages on disk
- page_table maps: `page_id → frame_id`

#### 2. Pinning Semantics
- When someone uses a page, **pin it** (increment pin_count)
- Pinned pages CANNOT be evicted (someone is using them!)
- When done, **unpin it** (decrement pin_count)
- Only unpinned pages (pin_count = 0) are eviction candidates

#### 3. LRU Eviction (Least Recently Used)
- Track access order in a list
- Front = Most Recently Used (MRU)
- Back = Least Recently Used (LRU)
- When pool is full, evict from back (oldest unused page)

#### 4. Dirty Pages & Write-Back
- **Dirty**: Page modified in memory, out of sync with disk
- Before evicting a dirty page, MUST write to disk
- Otherwise data loss!
- Clean pages can be evicted immediately (disk copy is current)

#### 5. The Critical Latch Problem
- BufferPool needs a latch to protect page_table, free_list, etc.
- I/O is SLOW (milliseconds)
- Holding latch during I/O kills concurrency
- **Advanced technique**: Release latch before I/O, re-acquire after
- We'll start simple and optimize later!

---

### Ideas Explored

#### Idea 1: Minimal Buffer Pool (Simple Start - CHOSEN FOR PHASE A)

**What it teaches:** Core lifecycle without complexity

**Design:**
```cpp
class BufferPoolManager {
 public:
  BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
  ~BufferPoolManager();

  // Core API
  Page* FetchPage(page_id_t page_id);        // Get page (from pool or disk)
  Page* NewPage(page_id_t* page_id);         // Allocate new page
  bool UnpinPage(page_id_t page_id, bool is_dirty);  // Release page
  bool FlushPage(page_id_t page_id);         // Force write to disk
  void FlushAllPages();                      // Write all dirty pages

 private:
  // Data structures
  std::vector<Page> pages_;                  // Fixed array of frames
  std::unordered_map<page_id_t, frame_id_t> page_table_;  // page_id → frame
  std::list<frame_id_t> free_list_;          // Available frames
  std::list<frame_id_t> lru_list_;           // LRU ordering (front=MRU, back=LRU)
  std::mutex latch_;                          // Coarse-grained lock
  DiskManager* disk_manager_;                 // For I/O
};
```

**Key characteristics:**
- **Coarse-grained locking**: Hold latch for entire operation (including I/O)
- **Simple LRU**: List-based, move to front on access
- **No optimization**: Focus on correctness first
- **Free list**: Initially all frames are free

**Learning value:**
- Understand fetch-pin-unpin lifecycle
- See how page_table + LRU work together
- Learn why pinning prevents eviction
- Simple enough to reason about correctness

**Implementation approach:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // Case 1: Page already in pool (cache hit)
  if (page_table_.count(page_id)) {
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;  // Pin it
    UpdateLRU(frame_id);             // Move to front (MRU)
    return &pages_[frame_id];
  }

  // Case 2: Cache miss - need a frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // Use free frame
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // No free frames - must evict
    if (!FindVictimFrame(&frame_id)) {
      return nullptr;  // All pages pinned!
    }
  }

  // Load page from disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // Update metadata
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  page_table_[page_id] = frame_id;
  lru_list_.push_front(frame_id);

  return &pages_[frame_id];
}
```

---

#### Idea 2: Add Proper Eviction Logic (PHASE B)

**What it teaches:** How real databases handle memory pressure

**Addition to Idea 1:**
```cpp
bool FindVictimFrame(frame_id_t* frame_id) {
  // Walk LRU list from back (least recently used)
  for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
    frame_id_t candidate = *it;

    // Can only evict unpinned pages
    if (pages_[candidate].GetPinCount() == 0) {
      *frame_id = candidate;

      // CRITICAL: Write dirty page before evicting!
      if (pages_[candidate].IsDirty()) {
        disk_manager_->WritePage(
          pages_[candidate].GetPageId(),
          pages_[candidate].GetData()
        );
      }

      // Clean up metadata
      page_table_.erase(pages_[candidate].GetPageId());
      lru_list_.erase(std::next(it).base());
      return true;
    }
  }

  return false;  // All pages pinned - can't evict!
}

void UpdateLRU(frame_id_t frame_id) {
  // Move to front (most recently used)
  lru_list_.remove(frame_id);
  lru_list_.push_front(frame_id);
}
```

**Learning value:**
- Teaches "write-before-evict" pattern (critical for durability!)
- Understand what happens when all pages pinned (pool exhaustion)
- See how LRU policy works in practice
- Learn why dirty flag matters

**Key database concept - Write-Before-Evict:**
If you evict a dirty page without writing it, you **lose data**! This is a durability violation. Always check `is_dirty_` before evicting.

---

#### Idea 3: Advanced Concurrency Optimization (FUTURE - PHASE C)

**What it teaches:** Production-grade concurrency patterns

**Key optimization:** Release latch during I/O

**From your old implementation:**
```cpp
Page* FetchPage(page_id_t page_id) {
  std::unique_lock<std::mutex> lock(latch_);

  // ... find victim, prepare eviction ...

  // Copy dirty data to temp buffer WHILE holding latch
  std::vector<char> temp_data;
  if (victim_is_dirty) {
    temp_data.assign(page_data, page_data + PAGE_SIZE);
  }

  // Update new page metadata
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;

  // RELEASE LATCH before I/O!
  lock.unlock();

  // Perform I/O WITHOUT holding latch (allows other threads to progress)
  if (victim_is_dirty) {
    disk_manager_->WritePage(victim_page_id, temp_data.data());
  }
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // Re-acquire latch to finish
  lock.lock();
  // ... final updates ...

  return &pages_[frame_id];
}
```

**Why this is complex:**
- Between unlock/lock, other threads can modify buffer pool state
- Need to handle race conditions (what if another thread evicts our frame?)
- Must handle I/O errors and rollback metadata changes
- Requires careful ordering of operations

**Learning value:**
- Teaches why databases need fine-grained concurrency
- Understand the "latching while copying, then release" pattern
- Learn about atomicity and error recovery
- See how PostgreSQL achieves high concurrency

**Trade-offs:**
- **Pro**: Much better concurrency (I/O doesn't block other threads)
- **Pro**: More realistic to production systems
- **Con**: Significantly more complex to reason about
- **Con**: Harder to debug race conditions
- **Con**: Need careful error handling

**Decision:** Save for later! Start simple, optimize once basic version works.

---

### Incremental Implementation Plan

#### Phase A: Simple BufferPool (Start Here!)
**Goal:** Correctness over performance

**What to build:**
- Coarse-grained locking (hold latch entire operation)
- Simple LRU list
- Basic FetchPage/NewPage/UnpinPage/FlushPage
- No eviction complexity initially (can fail if pool full)

**What you'll learn:**
- Page lifecycle (fetch, pin, unpin, flush)
- How page_table works
- Pinning semantics
- LRU ordering basics

**Testing focus:**
- Single-threaded correctness
- Cache hit/miss behavior
- Basic pin/unpin operations

---

#### Phase B: Add Proper Eviction
**Goal:** Handle memory pressure correctly

**What to add:**
- FindVictimFrame with write-before-evict
- Handle all-pages-pinned case
- Proper dirty page management
- UpdateLRU on access

**What you'll learn:**
- Eviction policies in depth
- Durability guarantees (dirty pages)
- Pool exhaustion handling
- LRU effectiveness

**Testing focus:**
- Eviction behavior
- Dirty page write-back
- Pool full scenarios
- Pin count exhaustion

---

#### Phase C: Optimize Concurrency (Future)
**Goal:** Production-grade performance

**What to add:**
- Release latch before I/O
- Error handling and rollback
- Handle concurrent modifications
- Thread-safety stress tests

**What you'll learn:**
- Fine-grained concurrency
- Race condition prevention
- Error recovery patterns
- Performance optimization trade-offs

**Testing focus:**
- Multi-threaded stress tests
- Concurrent fetch/evict scenarios
- I/O error handling
- Performance benchmarks

---

### Data Structures Explained

#### 1. pages_ (std::vector<Page>)
- Fixed-size array of frames
- Size determined at construction (e.g., 100 pages)
- Each element is a Page object (4KB data + metadata)
- Index is frame_id (0 to pool_size-1)

#### 2. page_table_ (std::unordered_map<page_id_t, frame_id_t>)
- Maps disk page_id to in-memory frame_id
- Example: `{5 → 0, 10 → 1, 20 → 2}` means:
  - Page 5 is in frame 0
  - Page 10 is in frame 1
  - Page 20 is in frame 2
- Only contains pages currently in buffer pool

#### 3. free_list_ (std::list<frame_id_t>)
- List of frames that are not in use
- Initially: [0, 1, 2, ..., pool_size-1]
- As pages loaded, frames removed from free_list
- When page evicted, frame added back

#### 4. lru_list_ (std::list<frame_id_t>)
- Tracks access order for eviction
- Front = Most Recently Used (MRU)
- Back = Least Recently Used (LRU)
- On access: move frame to front
- On eviction: pick from back (if pin_count = 0)

**Example state:**
```
Pool size: 3 frames
Loaded: Pages 5, 10, 20

pages_: [Page(id=5), Page(id=10), Page(id=20)]
page_table_: {5→0, 10→1, 20→2}
free_list_: [] (empty, all frames in use)
lru_list_: [1, 0, 2] (Page 10 most recent, Page 20 least recent)

Access Page 5:
lru_list_: [0, 1, 2] (Page 5 now most recent)

Evict:
Walk lru_list_ from back: frame 2 (Page 20) if pin_count = 0
```

---

### Alternative Approaches Considered

#### Clock Algorithm (Alternative to LRU)
- More efficient eviction (O(1) instead of O(n))
- Uses circular buffer + reference bit
- PostgreSQL uses this

**Why not for Phase A:**
- More complex to understand
- LRU is simpler and teaches core concepts better
- Can add as optional exercise later

#### Multiple LRU Lists (K-LRU)
- Separate lists for different access patterns
- Example: separate hot/cold lists

**Why not:**
- Over-engineering for learning project
- LRU is standard starting point

---

### Success Criteria

**Phase A Complete When:**
- ✅ FetchPage works (cache hit and miss)
- ✅ NewPage allocates new pages
- ✅ UnpinPage decrements pin_count
- ✅ FlushPage writes to disk
- ✅ Tests pass (basic lifecycle)

**Phase B Complete When:**
- ✅ FindVictimFrame evicts correctly
- ✅ Dirty pages written before eviction
- ✅ Pool full handled gracefully
- ✅ LRU ordering correct
- ✅ Tests pass (eviction scenarios)

**Phase C Complete When:**
- ✅ Latch released during I/O
- ✅ Error handling robust
- ✅ Multi-threaded tests pass
- ✅ Performance improved vs. Phase B

---

### Next Steps

1. ✅ Design exploration complete (this document)
2. → **Write formal design document** for Phase A (`doc/design/storage/buffer_pool_manager.md`)
   - Use Design Document Assistant mode
3. → Implement Phase A (simple BufferPool)
4. → Write tests for Phase A
5. → Document learnings
6. → Move to Phase B (eviction)
7. → (Future) Phase C (concurrency optimization)

---

### References

**Industry implementations:**
- **PostgreSQL**: `src/backend/storage/buffer/bufmgr.c` - Production buffer manager
- **BusTub** (CMU 15-445): Educational buffer pool implementation
- **Your old code**: `old/src/storage/buffer_pool_manager.cpp` - Advanced reference

**Key concepts:**
- LRU eviction: Least Recently Used
- Pinning: Reference counting to prevent eviction
- Dirty pages: Modified in memory, need write-back
- Write-before-evict: Durability requirement
- Latch optimization: Release during I/O for concurrency

---

**Status:** Design exploration complete. Ready for Phase A formal design doc and implementation.
