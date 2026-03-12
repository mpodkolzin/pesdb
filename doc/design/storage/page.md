# Page Class Design

**Component:** Storage Foundation - In-Memory Page Representation
**Phase:** 1, Step 2 of 26
**Status:** Ready for Implementation
**Date:** 2026-01-08

---

## Overview

The **Page** class is an in-memory wrapper around a 4KB buffer that adds metadata for buffer pool management. It sits between the BufferPoolManager (which manages a pool of Pages) and the raw data on disk.

**Core Responsibility:** Hold page data in memory with metadata for caching, eviction, and concurrency control.

```
DiskManager (disk I/O)
       ↓
   Page (in-memory wrapper) ← YOU ARE HERE
       ↓
BufferPoolManager (caching - Step 3)
       ↓
Application
```

---

## What Problem Does This Solve?

**Without Page class:**
```cpp
char buffer[PAGE_SIZE];
disk_manager.ReadPage(5, buffer);
// Problem: No way to know:
// - Is this dirty? (needs write-back?)
// - Which page is this? (page_id?)
// - Can we evict it? (reference count?)
```

**With Page class:**
```cpp
Page* page = buffer_pool.FetchPage(5);
page->data();        // Access raw data
page->page_id();     // Know which page
// BufferPool knows: pin_count, is_dirty, can evict?
```

---

## Design Decisions

### Decision 1: Classic Buffer Pool Page (Industry Standard)

**What we're building:**
```cpp
class Page {
  friend class BufferPoolManager;  // BufferPool manages metadata

public:
  char* data();                    // Access page data
  page_id_t page_id() const;       // Which disk page

  // Manual locking API
  void r_latch();                  // Acquire read lock
  void r_unlatch();                // Release read lock
  void w_latch();                  // Acquire write lock
  void w_unlatch();                // Release write lock

  // RAII lock guards (safer)
  class ReadGuard { ... };
  class WriteGuard { ... };

private:
  char data_[PAGE_SIZE]{};         // Data FIRST (cache-friendly)
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;              // Reference count
  bool is_dirty_ = false;          // Modified?
  std::shared_mutex latch_;        // Readers-writer lock
};
```

**Why this design:**
- Matches PostgreSQL and BusTub (industry standard)
- Data first for cache locality (hot path optimization)
- Friend class pattern for encapsulation (BufferPool manages metadata)
- Both manual and RAII locking APIs (flexibility + safety)

---

### Decision 2: Plain int pin_count (Not Atomic)

**Thread safety model:**
```
BufferPoolManager:
  Latch: latch_  ← Protects:
    • page_table_ (page_id → frame)
    • pin_count_
    • is_dirty_

Page:
  Latch: latch_  ← Protects:
    • data_[4096] ONLY
```

**Why plain int:**
- BufferPool modifies pin_count while holding its own latch
- Two-level locking: BufferPool latch for metadata, Page latch for content
- Simpler mental model than atomic operations
- Can optimize to atomic later if profiling shows benefit

**Alternative considered: std::atomic<int> pin_count**
- Would allow lock-free increment/decrement
- But BufferPool still needs latch for page_table operations
- Added complexity for minimal gain at this stage
- See `doc/design_exploration.md` for full discussion

---

### Decision 3: Memory Layout - Data First

```cpp
Page object layout (~4144 bytes):
┌─────────────────────────┐  Offset 0
│ data_[4096]             │  ← HOT: Accessed frequently
├─────────────────────────┤  Offset 4096
│ page_id_: 4 bytes       │  ← COLD: Metadata
│ pin_count_: 4 bytes     │
│ is_dirty_: 1 byte       │
│ latch_: ~40 bytes       │
└─────────────────────────┘
```

**Why data first:**
- When you access `page->data()`, data is at offset 0
- CPU cache line fetch gets the actual data immediately
- Metadata is rarely accessed with data
- Matches industry practice

**What I learned:**
- Cache-friendly data structures put hot fields first
- Ordering matters for performance even with modern CPUs
- This is a common pattern in high-performance systems

---

### Decision 4: Friend Class Pattern

```cpp
friend class BufferPoolManager;
```

**Why friend:**
- BufferPool needs to modify `pin_count_`, `is_dirty_`, `page_id_`
- Application code should NOT modify these (safety)
- Friend provides controlled access without public setters
- Documents ownership: "Page is managed by BufferPoolManager"

**Alternative rejected: Public setters**
```cpp
void set_pin_count(int count);  // BAD: Anyone can call!
```
Would break encapsulation and allow misuse.

---

### Decision 5: Dual Locking API (Manual + RAII)

**Manual locking (for BufferPool internals):**
```cpp
page->r_latch();
// Read data
page->r_unlatch();
```

**RAII guards (for application/safety):**
```cpp
{
  Page::ReadGuard guard(page);  // Lock acquired
  // Read data
}  // Lock automatically released!
```

**Why both:**
- Manual gives explicit control (BufferPool knows what it's doing)
- RAII provides exception safety (can't forget to unlock)
- Guards teach the RAII pattern
- Zero overhead (inlined by compiler)

---

## Key Concepts Learned

### 1. Readers-Writer Lock (std::shared_mutex)

**Multiple readers OR one writer:**
```cpp
Thread 1: r_latch()  → Reading  ✓
Thread 2: r_latch()  → Reading  ✓ (allowed!)
Thread 3: w_latch()  → BLOCKS (waits)

// After threads 1&2 release:
Thread 3: w_latch()  → Writing  ✓ (exclusive)
Thread 4: r_latch()  → BLOCKS (waits for writer)
```

**Why this works:**
- Read operations don't modify data → safe to read simultaneously
- Write operations modify data → need exclusive access
- Classic concurrency pattern in databases

---

### 2. Pin Counting (Reference Counting for Pages)

**The eviction problem:**
```cpp
// Thread 1:
Page* page = buffer_pool.FetchPage(5);  // pin_count = 1
// Using page...

// Thread 2 (BufferPool trying to evict):
if (page->pin_count_ == 0) {
  evict(page);  // Safe, nobody using it
} else {
  // CAN'T evict! Thread 1 still using it
}

// Thread 1:
buffer_pool.UnpinPage(5);  // pin_count = 0
// Now can be evicted
```

**Pin/Unpin protocol:**
- `FetchPage()` → pin_count++
- `UnpinPage()` → pin_count--
- Only pages with pin_count == 0 can be evicted
- Like reference counting in smart pointers

---

### 3. Dirty Flag (Write-Back Tracking)

**Optimization: Only write back modified pages**
```cpp
Page* page = buffer_pool.FetchPage(5);
// page->is_dirty_ = false (clean, matches disk)

// Modify data:
memcpy(page->data(), new_data, 100);
buffer_pool.UnpinPage(5, true);  // Mark dirty!
// page->is_dirty_ = true

// Later, when evicting:
if (page->is_dirty_) {
  disk_manager.WritePage(page->page_id_, page->data());  // Write back
}
// If clean, just discard (no I/O needed!)
```

**Why this matters:**
- Writing to disk is SLOW (milliseconds)
- If page wasn't modified, skip the write
- Huge performance win for read-heavy workloads

---

### 4. Friend Classes (Controlled Encapsulation Breaking)

**The tension:**
- Want: BufferPool to modify `pin_count_`, `is_dirty_`
- Don't want: Application code to modify these

**Friend solves this:**
```cpp
class Page {
  friend class BufferPoolManager;  // Only BufferPool can touch privates

private:
  int pin_count_;  // Application can't access
};

// In BufferPoolManager:
void FetchPage(page_id_t page_id) {
  page->pin_count_++;  // ✓ Allowed (friend)
}

// In application:
page->pin_count_++;  // ✗ Compile error!
```

**What I learned:**
- Friend is like a controlled exception to encapsulation
- Use sparingly (only when clear ownership exists)
- Documents the relationship in code

---

### 5. RAII for Locks (Scope-Based Resource Management)

**The problem with manual locks:**
```cpp
page->r_latch();
if (error) {
  // Forgot to unlock! DEADLOCK!
  return;
}
process_data();
page->r_unlatch();
```

**RAII solution:**
```cpp
{
  Page::ReadGuard guard(page);  // Constructor locks
  if (error) {
    return;  // Destructor AUTOMATICALLY unlocks!
  }
  process_data();
}  // Destructor unlocks here too
```

**Why this works:**
- C++ guarantees destructor runs when leaving scope
- Even if exception is thrown!
- Can't forget to unlock (compiler enforces)

**What I learned:**
- RAII = Resource Acquisition Is Initialization
- Use constructor to acquire, destructor to release
- Makes exception safety "automatic"

---

## API Summary

```cpp
class Page {
  friend class BufferPoolManager;

public:
  // Constructor (zero-initializes data)
  Page();

  // Destructor
  ~Page() = default;

  // Delete copy/move (Page has mutex, not copyable)
  Page(const Page&) = delete;
  Page& operator=(const Page&) = delete;

  // Data access
  char* data() { return data_; }
  const char* data() const { return data_; }

  // Metadata access (read-only for users)
  page_id_t page_id() const { return page_id_; }

  // Manual locking API
  void r_latch();    // Acquire shared lock
  void r_unlatch();  // Release shared lock
  void w_latch();    // Acquire exclusive lock
  void w_unlatch();  // Release exclusive lock

  // RAII lock guards
  class ReadGuard {
  public:
    explicit ReadGuard(Page* page);
    ~ReadGuard();
    ReadGuard(const ReadGuard&) = delete;
  private:
    Page* page_;
  };

  class WriteGuard {
  public:
    explicit WriteGuard(Page* page);
    ~WriteGuard();
    WriteGuard(const WriteGuard&) = delete;
  private:
    Page* page_;
  };

private:
  // Helper: zero out data
  void reset_memory();

  // Data members (BufferPool can access via friend)
  char data_[PAGE_SIZE]{};           // Page content
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  std::shared_mutex latch_;
};
```

---

## Implementation Notes

**File locations:**
- Header: `include/columnar_db/storage/page.h`
- Tests: `tests/unit/storage/page_test.cpp`

**Implementation is header-only (simple enough):**
- Constructor calls `reset_memory()` to zero buffer
- Locking methods just forward to `std::shared_mutex`
- Guards are simple RAII wrappers

**Testing strategy:**
1. Basic: Create page, access data, verify zero-init
2. Locking: Test r_latch/r_unlatch, w_latch/w_unlatch
3. RAII: Test guards auto-unlock
4. Multi-threaded: Multiple readers, exclusive writer (optional for now)

---

## Trade-offs Made

| Decision | Pro | Con | Why Chosen |
|----------|-----|-----|------------|
| Plain int pin_count | Simpler model | Not lock-free | BufferPool latch already needed |
| Data first layout | Cache-friendly | Wastes padding | Performance critical |
| Friend class | Clear ownership | Breaks encapsulation | Controlled, well-documented |
| Both lock APIs | Flexibility | More code | Teaching value + safety |
| Manual implementation | Full control | More code | Learning experience |

---

## Open Questions

**For later exploration:**
1. Should pin_count be atomic? (Measure after BufferPool built)
2. Can we use `std::shared_lock` instead of manual lock/unlock? (Yes, can refactor)
3. How does memory alignment affect performance? (Profile later)
4. Should ReadGuard/WriteGuard be moveable? (Not needed, but could add)

---

## What's Next

After Page is complete:
→ **Phase 1, Step 3: BufferPoolManager**

The BufferPoolManager will:
- Maintain a pool of Page objects (array of Pages)
- Map page_id to frame_id (which slot in the array)
- Manage pin/unpin protocol
- Implement LRU eviction policy
- Coordinate with DiskManager for I/O

The Page class provides the foundation for all of that!

---

## References

**Industry implementations:**
- PostgreSQL: `src/include/storage/buf_internals.h` - BufferDesc
- BusTub (CMU 15-445): `src/include/storage/page/page.h`
- DuckDB: `src/storage/buffer/buffer_handle.hpp`

**Concepts:**
- Readers-writer locks: `std::shared_mutex` (C++17)
- RAII pattern: "Resource Acquisition Is Initialization"
- Cache-friendly data structures
- Friend classes for controlled access

---

**Status:** Design complete. Ready to implement! 🚀
