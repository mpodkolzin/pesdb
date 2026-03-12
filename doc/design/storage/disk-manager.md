# Disk Manager Design Document

**Component:** Storage Foundation - Disk Manager
**Phase:** 1, Step 1 of 26
**Status:** Implementation Ready
**Estimated Time:** 1-2 days

---

## Overview

The **Disk Manager** is the foundational storage layer that provides an abstraction over raw file I/O. It's the lowest layer in our database storage hierarchy.

**Core Responsibility**: Translate page IDs to file offsets and perform synchronous read/write operations.

```
Application Layer
       ↓
Buffer Pool Manager (caching - Step 3)
       ↓
Disk Manager ← YOU ARE HERE
       ↓
Operating System (file I/O)
       ↓
Physical Disk
```

---

## Goals

1. **Page Abstraction**: Provide fixed-size, page-based storage (4KB pages)
2. **Simple Allocation**: Support append-only page allocation (no free list yet)
3. **Direct I/O**: Synchronous reads and writes (no buffering at this layer)
4. **Learning Focus**: Clear, simple implementation that teaches fundamentals

---

## Non-Goals

- ❌ Buffering/caching (that's BufferPoolManager's job - Step 3)
- ❌ Free page management (start with append-only, add later if needed)
- ❌ Concurrent access control (single-threaded for now)
- ❌ I/O optimization (no async I/O, no O_DIRECT, keep it simple)
- ❌ Multiple database files (one file per database)

---

## Key Concepts You'll Learn

### 1. Page-Based Storage

**Why pages?**
- Operating systems work in pages (typically 4KB)
- Fixed-size units simplify addressing: `offset = page_id * PAGE_SIZE`
- Buffer pool can cache whole pages (no partial page reads)
- Standard abstraction in all modern databases

**Alternative (not used)**: Variable-sized records written directly to file
- **Problem**: Complex offset tracking, fragmentation, no clean caching unit

### 2. File I/O Basics (POSIX)

We'll use standard POSIX file I/O:
- `open()` - Open or create file
- `read()` - Read bytes from file
- `write()` - Write bytes to file
- `lseek()` - Position file pointer
- `close()` - Close file descriptor
- `fstat()` - Get file metadata (size)

**Why not C++ `std::fstream`?**
- POSIX gives more control (direct I/O, fsync, etc.)
- Matches what real databases use (PostgreSQL, MySQL)
- Learning systems programming fundamentals

### 3. Offset Calculation

The heart of page-based storage:
```
offset = page_id * PAGE_SIZE
```

Example:
- Page 0: offset = 0 * 4096 = 0
- Page 1: offset = 1 * 4096 = 4096
- Page 2: offset = 2 * 4096 = 8192

**This simple math is why page-based storage works!**

---

## Design Decisions

### Decision 1: Page Size = 4KB

**Rationale:**
- Matches typical OS page size (4096 bytes on Linux/macOS)
- Efficient I/O: OS reads in 4KB chunks anyway
- Standard in many databases (PostgreSQL, MySQL InnoDB)
- Good balance: not too small (overhead), not too large (wasted space for small tables)

**Alternative Considered:** 8KB pages
- **Pros**: More data per page (fewer pages total)
- **Cons**: More wasted space for small tables, not as common
- **Decision**: Stick with 4KB for learning, can change later

### Decision 2: File Layout - Simple Linear

The simplest possible layout:

```
Database File: mydb.db
+----------------+----------------+----------------+-------
| Page 0         | Page 1         | Page 2         | ...
| (4KB)          | (4KB)          | (4KB)          |
+----------------+----------------+----------------+-------
Offset: 0        4096            8192            12288
```

**Mapping**: `file_offset = page_id * 4096`

**Rationale:**
- Simplest possible design (no metadata header, no page directory)
- Direct mapping (no indirection)
- Easy to reason about for learning
- Can visualize with `hexdump -C mydb.db`

**Future Enhancement**: Could add file header with:
- Magic number (e.g., `0xDEADBEEF`) to identify file type
- Version number
- Page size (for flexibility)
- Metadata (creation time, etc.)

### Decision 3: Allocation Strategy - Append-Only

New pages are allocated sequentially at the end of the file.

```cpp
page_id_t AllocatePage() {
  return num_pages_++;  // Next page ID is current count
}
```

**Rationale:**
- Simplest allocation strategy
- No free page tracking needed
- Good starting point before introducing complexity

**Future Enhancement**: Free list for page recycling
- When table data is deleted, mark pages as free
- `AllocatePage()` can reuse free pages before extending file
- Reduces file size growth

### Decision 4: Use C++ `std::fstream` (Not POSIX)

**UPDATE**: For simplicity and portability, we'll use `std::fstream` instead of raw POSIX calls.

**Rationale:**
- Simpler for learning (less error handling)
- Cross-platform (works on Windows, Linux, macOS)
- Still teaches page-based I/O concepts
- Can switch to POSIX later if we need `fsync()` control

**Trade-off**: Less control over buffering and synchronization (but we don't need it yet)

---

## API Design

### Class: `DiskManager`

```cpp
namespace db {

class DiskManager {
public:
  /**
   * Opens or creates a database file.
   * If file doesn't exist, creates it. If exists, opens for read/write.
   *
   * @param db_file Path to database file (e.g., "mydb.db")
   */
  explicit DiskManager(const std::string& db_file);

  /**
   * Destructor: closes file handle
   */
  ~DiskManager();

  // Delete copy/move (owns file handle, shouldn't be copied)
  DiskManager(const DiskManager&) = delete;
  DiskManager& operator=(const DiskManager&) = delete;

  /**
   * Reads a page from disk into provided buffer.
   *
   * @param page_id The page to read (must be < num_pages_)
   * @param data Output buffer (must be at least PAGE_SIZE bytes)
   */
  void ReadPage(page_id_t page_id, char* data);

  /**
   * Writes a page from buffer to disk.
   *
   * @param page_id The page to write (can be existing or num_pages_)
   * @param data Input buffer (must be at least PAGE_SIZE bytes)
   */
  void WritePage(page_id_t page_id, const char* data);

  /**
   * Allocates a new page at end of file.
   * Returns the new page_id, increments num_pages_.
   * Does NOT initialize the page (caller must WritePage).
   *
   * @return page_id of the newly allocated page
   */
  page_id_t AllocatePage();

  /**
   * Returns total number of pages in the file.
   */
  size_t GetNumPages() const { return num_pages_; }

private:
  std::string file_name_;          // Path to database file
  std::fstream file_stream_;       // File handle
  size_t num_pages_;               // Total pages in file
};

}  // namespace db
```

---

## Implementation Details

### Constructor: Open or Create File

```cpp
DiskManager::DiskManager(const std::string& db_file)
    : file_name_(db_file), num_pages_(0) {

  // Try to open existing file
  file_stream_.open(file_name_,
                   std::ios::in | std::ios::out | std::ios::binary);

  if (!file_stream_.is_open()) {
    // File doesn't exist, create it
    file_stream_.open(file_name_,
                     std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);

    if (!file_stream_.is_open()) {
      throw std::runtime_error("Cannot create database file: " + file_name_);
    }
  }

  // Determine file size and calculate num_pages
  file_stream_.seekg(0, std::ios::end);
  size_t file_size = file_stream_.tellg();
  num_pages_ = file_size / PAGE_SIZE;
}
```

**Learning Points:**
- `std::ios::binary` - No text mode translation (we want raw bytes)
- `std::ios::trunc` - Truncate if exists (for creating new DB)
- `seekg(0, end)` + `tellg()` - Get file size
- Calculate `num_pages_` from file size

### Destructor: Close File

```cpp
DiskManager::~DiskManager() {
  if (file_stream_.is_open()) {
    file_stream_.close();
  }
}
```

**Learning Point:** RAII - file closed automatically when object destroyed

### ReadPage: Seek and Read

```cpp
void DiskManager::ReadPage(page_id_t page_id, char* data) {
  // Validate page_id
  if (page_id < 0 || static_cast<size_t>(page_id) >= num_pages_) {
    throw std::out_of_range("Invalid page_id: " + std::to_string(page_id));
  }

  // Calculate offset
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

  // Seek to position
  file_stream_.seekg(offset, std::ios::beg);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
  }

  // Read page
  file_stream_.read(data, PAGE_SIZE);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to read page " + std::to_string(page_id));
  }
}
```

**Learning Points:**
- Bounds checking (don't read beyond file)
- `seekg()` positions read pointer
- `read(buffer, size)` reads bytes
- Error handling with exceptions

### WritePage: Seek and Write

```cpp
void DiskManager::WritePage(page_id_t page_id, const char* data) {
  // Validate page_id (can write to existing page or extend file)
  if (page_id < 0 || static_cast<size_t>(page_id) > num_pages_) {
    throw std::out_of_range("Invalid page_id: " + std::to_string(page_id));
  }

  // Calculate offset
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

  // Seek to position
  file_stream_.seekp(offset, std::ios::beg);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
  }

  // Write page
  file_stream_.write(data, PAGE_SIZE);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to write page " + std::to_string(page_id));
  }

  // Flush to disk (ensure durability)
  file_stream_.flush();

  // Update num_pages if we extended the file
  if (static_cast<size_t>(page_id) >= num_pages_) {
    num_pages_ = page_id + 1;
  }
}
```

**Learning Points:**
- `seekp()` positions write pointer
- `write(buffer, size)` writes bytes
- `flush()` ensures data written to OS (not just buffered)
- Track `num_pages_` as file grows

### AllocatePage: Append New Page (Eager Allocation)

```cpp
page_id_t DiskManager::AllocatePage() {
  // New page ID is current count
  page_id_t new_page_id = static_cast<page_id_t>(num_pages_);

  // Eager allocation: zero-initialize page immediately
  char zeros[PAGE_SIZE];
  std::memset(zeros, 0, PAGE_SIZE);
  WritePage(new_page_id, zeros);  // This updates num_pages_

  return new_page_id;
}
```

**Learning Points:**
- Append-only: next page_id is always `num_pages_`
- **Eager allocation**: Page initialized with zeros immediately
- **Clear semantics**: Allocated page is always readable
- Matches PostgreSQL behavior (industry standard)

**Design Decision:** We chose eager over lazy allocation for learning clarity.
See `doc/design/storage/file-layout.md` for full rationale.

---

## Testing Strategy

### Unit Tests (Google Test)

**Test File:** `tests/unit/storage/disk_manager_test.cpp`

#### Test 1: Create New Database

```cpp
TEST(DiskManagerTest, CreateNewDatabase) {
  std::filesystem::remove("test_new.db");  // Ensure clean state

  auto dm = std::make_unique<DiskManager>("test_new.db");

  EXPECT_EQ(dm->GetNumPages(), 0);  // Empty database

  std::filesystem::remove("test_new.db");  // Cleanup
}
```

#### Test 2: Allocate Single Page

```cpp
TEST(DiskManagerTest, AllocateSinglePage) {
  std::filesystem::remove("test_alloc.db");

  auto dm = std::make_unique<DiskManager>("test_alloc.db");

  page_id_t page0 = dm->AllocatePage();
  page_id_t page1 = dm->AllocatePage();

  EXPECT_EQ(page0, 0);
  EXPECT_EQ(page1, 1);
  EXPECT_EQ(dm->GetNumPages(), 2);

  std::filesystem::remove("test_alloc.db");
}
```

#### Test 3: Write and Read Page

```cpp
TEST(DiskManagerTest, WriteAndReadPage) {
  std::filesystem::remove("test_rw.db");

  auto dm = std::make_unique<DiskManager>("test_rw.db");

  // Allocate page
  page_id_t pid = dm->AllocatePage();

  // Write known pattern
  char write_buf[PAGE_SIZE];
  std::memset(write_buf, 'A', PAGE_SIZE);
  dm->WritePage(pid, write_buf);

  // Read back
  char read_buf[PAGE_SIZE];
  std::memset(read_buf, 0, PAGE_SIZE);
  dm->ReadPage(pid, read_buf);

  // Verify
  EXPECT_EQ(std::memcmp(write_buf, read_buf, PAGE_SIZE), 0);

  std::filesystem::remove("test_rw.db");
}
```

#### Test 4: Multiple Pages with Different Data

```cpp
TEST(DiskManagerTest, MultiplePages) {
  std::filesystem::remove("test_multi.db");

  auto dm = std::make_unique<DiskManager>("test_multi.db");

  const int NUM_PAGES = 10;
  char write_bufs[NUM_PAGES][PAGE_SIZE];

  // Write pages with different patterns
  for (int i = 0; i < NUM_PAGES; i++) {
    page_id_t pid = dm->AllocatePage();
    std::memset(write_bufs[i], 'A' + i, PAGE_SIZE);
    dm->WritePage(pid, write_bufs[i]);
  }

  // Read back and verify
  for (int i = 0; i < NUM_PAGES; i++) {
    char read_buf[PAGE_SIZE];
    dm->ReadPage(i, read_buf);
    EXPECT_EQ(std::memcmp(write_bufs[i], read_buf, PAGE_SIZE), 0);
  }

  std::filesystem::remove("test_multi.db");
}
```

#### Test 5: Reopen Database (Persistence)

```cpp
TEST(DiskManagerTest, ReopenDatabase) {
  std::filesystem::remove("test_persist.db");

  // Create database, write data
  {
    auto dm = std::make_unique<DiskManager>("test_persist.db");
    page_id_t pid = dm->AllocatePage();

    char write_buf[PAGE_SIZE];
    std::memset(write_buf, 'Z', PAGE_SIZE);
    dm->WritePage(pid, write_buf);

    // DiskManager destroyed, file closed
  }

  // Reopen database
  {
    auto dm = std::make_unique<DiskManager>("test_persist.db");

    EXPECT_EQ(dm->GetNumPages(), 1);  // Page still there

    char read_buf[PAGE_SIZE];
    dm->ReadPage(0, read_buf);

    // Verify data persisted
    EXPECT_EQ(read_buf[0], 'Z');
    EXPECT_EQ(read_buf[PAGE_SIZE - 1], 'Z');
  }

  std::filesystem::remove("test_persist.db");
}
```

#### Test 6: Error Handling - Invalid Page ID

```cpp
TEST(DiskManagerTest, ReadInvalidPage) {
  std::filesystem::remove("test_error.db");

  auto dm = std::make_unique<DiskManager>("test_error.db");
  dm->AllocatePage();  // page_id 0 exists

  char buf[PAGE_SIZE];

  // Reading page that doesn't exist should throw
  EXPECT_THROW(dm->ReadPage(1, buf), std::out_of_range);
  EXPECT_THROW(dm->ReadPage(-1, buf), std::out_of_range);

  std::filesystem::remove("test_error.db");
}
```

### Manual Testing

**Inspect file with hexdump:**

```bash
# Create a database and write a page
./pesdb  # (your test program)

# Inspect the file
hexdump -C test.db | head -20

# Should see:
# 00000000  41 41 41 41 41 41 41 41  41 41 41 41 41 41 41 41  |AAAAAAAAAAAAAAAA|
# (repeated for 4096 bytes if you wrote 'A')
```

**Check file size:**

```bash
ls -lh test.db
# 1 page = 4096 bytes
# 10 pages = 40960 bytes
```

---

## Implementation Plan

1. ✅ Create `include/columnar_db/storage/disk_manager.h` - Class declaration
2. ✅ Create `src/storage/disk_manager.cpp` - Implementation
3. ✅ Create `src/storage/CMakeLists.txt` - Build configuration
4. ✅ Create `tests/unit/storage/disk_manager_test.cpp` - Unit tests
5. ✅ Update `tests/CMakeLists.txt` - Enable tests
6. 🔨 **Build and test**: `cmake --build build && ./build/tests/unit/storage_tests`
7. 📝 **Document learnings**: Write `doc/learnings/page-based-storage.md`

---

## Success Criteria

✅ All unit tests pass
✅ Can create new database file
✅ Can allocate and write 1000+ pages
✅ Can reopen database and read pages
✅ File size matches `num_pages * PAGE_SIZE`
✅ Code compiles with no warnings (`-Wall -Wextra`)

---

## What You'll Learn

After implementing this component, you will deeply understand:

1. **Page-Based Storage**: Why databases use fixed-size pages
2. **File I/O**: C++ file stream operations, seeking, reading, writing
3. **Offset Calculation**: The simple math that makes page abstraction work
4. **Resource Management**: RAII pattern for file handles
5. **Error Handling**: Validating inputs, throwing exceptions
6. **Testing**: Unit testing with Google Test, test-driven development

---

## Next Steps

After Disk Manager is complete and tested:

→ **Phase 1, Step 2**: Implement `Page` class (in-memory page representation)

The Page class will wrap raw `char data[PAGE_SIZE]` with metadata:
- `page_id_` - which page this represents
- `pin_count_` - reference counting (for buffer pool)
- `is_dirty_` - has this page been modified?
- `latch_` - mutex for concurrent access (later)

---

## References

- **PostgreSQL**: `src/backend/storage/smgr/md.c` (magnetic disk manager)
- **SQLite**: `src/pager.c` (page-based storage abstraction)
- **DuckDB**: `src/storage/storage_manager.cpp` (modern columnar storage)
- **Book**: "Database System Concepts" Ch. 10 - Storage and File Structure

---

**Ready to implement!** 🚀

Let's write `disk_manager.h` and `disk_manager.cpp` next.
