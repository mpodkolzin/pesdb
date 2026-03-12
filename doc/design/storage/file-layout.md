# Database File Layout - Complete Architecture

**Status:** Comprehensive Design Document
**Last Updated:** 2026-01-06
**Covers:** Phase 1-8 (Foundation to Columnar Optimizations)

---

## ⚠️ IMPORTANT: Clarification on "Metadata"

**We DO have metadata - Page 0 is the catalog!**

When you see phrases like "simple linear layout" or "no file header," this refers to the **physical layout**:
- ✅ Physical: `offset = page_id * 4096` (simple arithmetic)
- ✅ No header bytes BEFORE pages (file starts at offset 0 = Page 0)

**BUT** we DO have **logical metadata**:
- ✅ **Page 0 = Catalog** (stores table/column metadata)
- ✅ **Page 1+ = User data** (column data)

**Architectural decision:** Page 0 is **ALWAYS** reserved for catalog, even from Phase 1. This is our design from the start, not a future addition.

---

## Table of Contents

1. [Architectural Decisions](#architectural-decisions)
2. [Single File Organization](#single-file-organization)
3. [Dynamic Growth with Linked Lists](#dynamic-growth-with-linked-lists)
4. [Columnar Storage Layout](#columnar-storage-layout)
5. [Variable-Length Types (VARCHAR)](#variable-length-types-varchar)
6. [Page Allocation Strategy](#page-allocation-strategy)
7. [Catalog Structure (Page 0)](#catalog-structure-page-0)
8. [Visual Examples](#visual-examples)

---

## Architectural Decisions

### Decision 1: Single File vs Multiple Files ✅

**CHOSEN: Single File for Entire Database**

```
mydb.db (one file)
├─ Page 0: Catalog
├─ Pages 1+: All table/column data (interleaved)
└─ Managed by one DiskManager instance
```

**Rationale:**
- ✅ **Simplest for learning**: One file handle, one buffer pool
- ✅ **Standard pattern**: PostgreSQL, SQLite use single file
- ✅ **Easy to implement**: No multi-file coordination

**Alternatives considered:**
- One file per table (MySQL MyISAM) - more complex
- One file per column (ClickHouse) - extreme granularity, many file handles

**Trade-offs accepted:**
- ❌ File grows indefinitely (all data in one file)
- ❌ Can't easily drop table to reclaim space (need compaction)
- ✅ But: simpler mental model for learning!

---

### Decision 2: Page Linking Strategy ✅

**CHOSEN: Linked Lists (PostgreSQL-style)**

Each page contains `next_page_id` pointer to next page in chain.

**Rationale:**
- ✅ **Flexible**: Pages don't need to be contiguous
- ✅ **Dynamic growth**: Easy to extend column (append page, update last page's pointer)
- ✅ **No pre-allocation**: Allocate pages on-demand as data inserted

**Alternatives considered:**
- Contiguous ranges (DuckDB) - faster random access, but requires complex allocator
- Page directory (MySQL) - indirection layer, more complex

**Trade-offs accepted:**
- ❌ Random access requires list traversal (slow for row N)
- ✅ But: analytical queries scan sequentially anyway!

---

### Decision 3: Page Allocation Strategy ✅

**CHOSEN: Eager Allocation with Zero-Initialization**

`AllocatePage()` immediately writes zeros to disk.

**Rationale:**
- ✅ **Clear semantics**: Allocated page is always readable
- ✅ **No surprises**: `ReadPage()` never fails on allocated page
- ✅ **Matches PostgreSQL**: Industry standard behavior

**Alternatives considered:**
- Lazy allocation - faster, but allocated pages aren't readable
- Two counters (allocated vs on-disk) - correct but complex

**Trade-offs accepted:**
- ❌ Extra I/O (4KB write per allocation)
- ✅ But: clarity > performance for learning!

---

### Decision 4: Page 0 Reserved for Catalog ✅

**CHOSEN: Page 0 Stores Database Metadata**

All user data starts at Page 1+.

**Rationale:**
- ✅ **Standard practice**: PostgreSQL, MySQL reserve page 0
- ✅ **Bootstrap**: Known location to find all tables/columns
- ✅ **Metadata separation**: Clear distinction from data pages

---

## Single File Organization

### Physical Layout

```
Database File: mydb.db
┌──────────────────────────────────────────────┐
│ Page 0: CATALOG (4096 bytes)                 │
│   - Magic number, version                    │
│   - Table metadata                           │
│   - Column metadata with first_page pointers │
├──────────────────────────────────────────────┤
│ Page 1+: DATA PAGES (interleaved)            │
│   - Pages from different columns mixed       │
│   - Each column maintains linked list        │
│   - No pre-allocated ranges!                 │
└──────────────────────────────────────────────┘
```

### Key Properties

- **Single page ID space**: All pages share global address space (0, 1, 2, 3, ...)
- **Interleaved columns**: Page 1 might be "users.id", Page 2 might be "orders.amount"
- **Catalog tracking**: Page 0 stores which pages belong to which column
- **Linear addressing**: `file_offset = page_id * 4096` (never changes!)

### Page ID Formula

```
page_id     →  file_offset
─────────────────────────
   0        →      0
   1        →   4,096
   2        →   8,192
  100       → 409,600
1000        → 4,096,000
```

**Implementation** (`disk_manager.cpp:63`):
```cpp
size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
```

---

## Dynamic Growth with Linked Lists

### No Pre-Allocated Ranges!

**IMPORTANT:** Columns do NOT have fixed page ranges. Pages are allocated **on-demand** as data is inserted.

### Initial State: Empty Database

```
mydb.db (4096 bytes)
┌──────────────────────┐
│ Page 0: CATALOG      │
│   Table "users":     │
│     Column "id":     │
│       first_page = INVALID (-1)  ← No pages yet!
│     Column "name":   │
│       first_page = INVALID (-1)
│     Column "age":    │
│       first_page = INVALID (-1)
└──────────────────────┘
```

**File size:** 4096 bytes (only catalog)

### After Inserting 1000 Rows

```
mydb.db (28672 bytes = 7 pages)
┌──────────────────────┐
│ Page 0: CATALOG      │
│   Table "users":     │
│     Column "id":   first_page = 1
│     Column "name": first_page = 3
│     Column "age":  first_page = 5
├──────────────────────┤
│ Page 1: users.id     │  ← Allocated during INSERT
│   next_page = 2      │
│   count = 511        │
│   values[0..510]     │
├──────────────────────┤
│ Page 2: users.id     │
│   next_page = -1     │  ← Last page (for now)
│   count = 489        │
│   values[0..488]     │
├──────────────────────┤
│ Page 3: users.name   │  ← Different column interleaved!
│   next_page = 4      │
├──────────────────────┤
│ Page 4: users.name   │
│   next_page = -1     │
├──────────────────────┤
│ Page 5: users.age    │
│   next_page = 6      │
├──────────────────────┤
│ Page 6: users.age    │
│   next_page = -1     │
└──────────────────────┘
```

**Key observations:**
- ✅ Pages allocated as needed (not pre-allocated)
- ✅ Columns interleaved in file (Pages 1-2 for "id", 3-4 for "name", 5-6 for "age")
- ✅ Each column has linked list (next_page pointers)

### After Inserting 5000 MORE Rows

Columns **extend** by allocating new pages and linking them:

```
mydb.db (larger file)
┌──────────────────────┐
│ Page 0: CATALOG      │
│   (unchanged)        │
├──────────────────────┤
│ Pages 1-2: users.id  │  ← Old pages
│   (unchanged)        │
├──────────────────────┤
│ Pages 3-4: users.name│
├──────────────────────┤
│ Pages 5-6: users.age │
├──────────────────────┤
│ Page 7: users.id     │  ← NEW page extends "id" column
│   next_page = 8      │     (Page 2's next_page updated: 2 → 7)
├──────────────────────┤
│ Page 8: users.id     │
│   next_page = 9      │
├──────────────────────┤
│ Page 9: users.id     │
│   next_page = -1     │  ← New last page for "id"
├──────────────────────┤
│ Page 10: users.name  │  ← Extends "name" column
│   (Page 4 → 10)      │
├──────────────────────┤
│ Page 11: users.age   │  ← Extends "age" column
│   (Page 6 → 11)      │
└──────────────────────┘
```

**Growth algorithm** (pseudocode):
```
To append value to column:
  1. Traverse linked list to find last page (cache this!)
  2. If last page has space:
       Add value to page, increment count, mark dirty
  3. If last page full:
       Allocate new page from DiskManager
       Initialize new page (next=-1, count=0)
       Link old last page to new page (update next_page)
       Add value to new page
```

**No limits!** Columns can grow indefinitely (up to 2^31 pages = 8TB).

---

## Columnar Storage Layout

### Phase 1: Raw Pages (Current)

Pages are **unstructured bytes** - just for testing DiskManager.

```
┌──────────────────────────────────┐
│         Page N (4096 bytes)      │
│                                  │
│     [raw bytes, no structure]    │
│                                  │
└──────────────────────────────────┘
```

### Phase 2: Fixed-Width Columns (BIGINT)

Pages store **arrays of fixed-size values** with linked list header.

```cpp
struct ColumnDataPage {
  page_id_t next_page_id;  // -1 if last page, else next page in chain
  uint32_t value_count;     // Values stored in THIS page (0..511)
  int64_t values[511];      // Dense array of values
};
```

**Memory layout:**
```
┌──────────────────────────────────┐
│  ColumnDataPage (4096 bytes)     │
├──────────────────────────────────┤
│  Header (8 bytes):               │
│    next_page_id (4 bytes)        │
│    value_count  (4 bytes)        │
├──────────────────────────────────┤
│  Data Area (4088 bytes):         │
│    int64_t values[0]             │
│    int64_t values[1]             │
│    ...                           │
│    int64_t values[510]           │
│                                  │
│  Total values: 4088 / 8 = 511   │
└──────────────────────────────────┘
```

**Access pattern (column scan):**
```cpp
page_id_t current = column_metadata.first_page;
while (current != INVALID_PAGE_ID) {
  ReadPage(current, buffer);
  auto* page = reinterpret_cast<ColumnDataPage*>(buffer);

  // Process page->values[0 .. value_count-1]
  for (uint32_t i = 0; i < page->value_count; i++) {
    ProcessValue(page->values[i]);
  }

  current = page->next_page_id;  // Follow link
}
```

**Random access (row N):**
```cpp
// Find which page contains row N
uint32_t page_index = N / 511;          // Which page? (0-indexed)
uint32_t slot_index = N % 511;          // Which slot in page?

// Traverse linked list to page_index
page_id_t current = first_page;
for (uint32_t i = 0; i < page_index; i++) {
  ReadPage(current, buffer);
  current = reinterpret_cast<ColumnDataPage*>(buffer)->next_page_id;
}

// Read value from slot
ReadPage(current, buffer);
auto* page = reinterpret_cast<ColumnDataPage*>(buffer);
int64_t value = page->values[slot_index];
```

---

## Variable-Length Types (VARCHAR)

### The Challenge

Strings have **variable lengths** - can't use simple array:

```
Row 0: "Alice"         (5 bytes)
Row 1: "Bob"           (3 bytes)
Row 2: "Christopher"   (11 bytes)
Row 3: "Eve"           (3 bytes)
```

### Solution: Dictionary Encoding ✅ CHOSEN

**Most columnar databases (Parquet, ORC, ClickHouse, DuckDB) use this approach.**

#### Concept

1. **Build dictionary**: Map unique strings → integer codes
2. **Store codes**: Column pages store codes (like BIGINT!)
3. **Decode on read**: Lookup string in dictionary

#### Example

**Input data:**
```
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Alice'), ('Charlie'), ('Bob');
```

**Dictionary** (stored in column metadata):
```
┌───────┬──────────────┐
│ Code  │ String       │
├───────┼──────────────┤
│   0   │ "Alice"      │
│   1   │ "Bob"        │
│   2   │ "Charlie"    │
└───────┴──────────────┘
```

**Column data** (stored in pages):
```
┌──────────────────────┐
│ ColumnDataPage       │
│   next_page_id = -1  │
│   value_count = 5    │
│   codes[0..4]:       │
│     [0, 1, 0, 2, 1]  │  ← Just integers!
└──────────────────────┘
```

**Result:** VARCHAR column behaves exactly like BIGINT column (stores uint32_t codes)!

#### Storage Structure

```cpp
// Dictionary (stored in column metadata or dedicated pages)
struct StringDictionary {
  std::unordered_map<std::string, uint32_t> string_to_code;
  std::vector<std::string> code_to_string;
};

// Column pages (same structure as BIGINT!)
struct StringColumnPage {
  page_id_t next_page_id;
  uint32_t value_count;
  uint32_t codes[511];  // Dictionary codes, not actual strings
};
```

#### Encode/Decode

**Encode (INSERT):**
```cpp
uint32_t EncodeString(const std::string& str) {
  if (string_to_code.count(str)) {
    return string_to_code[str];  // Existing string
  } else {
    uint32_t code = code_to_string.size();
    code_to_string.push_back(str);
    string_to_code[str] = code;
    return code;  // New string
  }
}
```

**Decode (SELECT):**
```cpp
std::string DecodeString(uint32_t code) {
  return code_to_string[code];  // Simple lookup
}
```

#### Benefits

- ✅ **Compression**: Repeated strings stored once
  - Example: "USA" appears 1M times = 1M × 4 bytes (codes), not 1M × 3 bytes (strings + overhead)
- ✅ **Fixed-width storage**: VARCHAR column behaves like BIGINT
- ✅ **Fast equality**: Compare codes (`code1 == code2`), not strings (`strcmp`)
- ✅ **Industry standard**: Used in Parquet, ORC, ClickHouse

#### Limitations

- ❌ **Dictionary overhead**: Need to store dictionary (but usually small, e.g., 100K unique strings = ~few MB)
- ❌ **Cardinality**: uint32_t allows 4B unique strings (fine in practice, even for UUIDs)
- ✅ **String comparisons**: Can build sorted code mapping for efficient `<`, `>`, `LIKE` operations

#### Implementation Phases

**Phase 1-2:** Support only BIGINT (fixed-width)
**Phase 3:** Add dictionary-encoded VARCHAR
**Phase 8:** Optimize dictionary (sorted codes, compression)

---

## Page Allocation Strategy

### Global Page Allocator

```cpp
class DiskManager {
  page_id_t AllocatePage() {
    page_id_t new_id = static_cast<page_id_t>(num_pages_);

    // Write zeros immediately (eager allocation)
    char zeros[PAGE_SIZE];
    std::memset(zeros, 0, PAGE_SIZE);
    WritePage(new_id, zeros);  // Extends file, updates num_pages_

    return new_id;
  }
};
```

### Allocation Timeline

**Step 1:** Create empty database
```
File size: 4096 bytes (Page 0 only)
num_pages_ = 1
```

**Step 2:** Create table "users"
```
Catalog updated: Table "users" registered, no pages yet
File size: 4096 bytes (still just catalog)
```

**Step 3:** Insert first row into "users"
```
AllocatePage() → 1  (for column "id")
AllocatePage() → 2  (for column "name")
AllocatePage() → 3  (for column "age")
File size: 16384 bytes (4 pages)
```

**Step 4:** Create table "orders"
```
Catalog updated: Table "orders" registered
File size: 16384 bytes (no new pages yet)
```

**Step 5:** Insert first row into "orders"
```
AllocatePage() → 4  (for column "order_id")
AllocatePage() → 5  (for column "amount")
File size: 24576 bytes (6 pages)
```

**Key insight:** Pages allocated **lazily** as data inserted, **interleaved** across tables/columns.

---

## Catalog Structure (Page 0)

### Purpose

Page 0 stores **all database metadata** needed to bootstrap:
- Which tables exist
- Which columns belong to each table
- Where each column's data starts (first_page)

### Layout (Conceptual)

```cpp
struct DatabaseCatalog {
  // File header
  uint32_t magic_number;      // 0xDEADBEEF (identify file type)
  uint32_t version;           // 1 (schema version)
  uint32_t num_tables;        // Count of tables

  // Table metadata
  TableMetadata tables[MAX_TABLES];
};

struct TableMetadata {
  char name[64];              // "users", "orders"
  uint32_t num_columns;       // 3, 2
  ColumnMetadata columns[MAX_COLUMNS];
};

struct ColumnMetadata {
  char name[32];              // "id", "name", "age"
  DataType type;              // BIGINT, VARCHAR
  page_id_t first_page;       // -1 if no data, else first page in chain

  // For VARCHAR: dictionary metadata
  uint32_t dict_size;         // Number of unique strings
  // Dictionary stored separately (in-memory or dedicated pages)
};
```

### Example Catalog Contents

```
Page 0: Catalog (serialized binary)
┌────────────────────────────────┐
│ magic_number = 0xDEADBEEF      │
│ version = 1                    │
│ num_tables = 2                 │
├────────────────────────────────┤
│ Table[0]: "users"              │
│   num_columns = 3              │
│   Column[0]: "id"              │
│     type = BIGINT              │
│     first_page = 1             │
│   Column[1]: "name"            │
│     type = VARCHAR             │
│     first_page = 3             │
│     dict_size = 100            │
│   Column[2]: "age"             │
│     type = BIGINT              │
│     first_page = 5             │
├────────────────────────────────┤
│ Table[1]: "orders"             │
│   num_columns = 2              │
│   Column[0]: "order_id"        │
│     type = BIGINT              │
│     first_page = 7             │
│   Column[1]: "amount"          │
│     type = BIGINT              │
│     first_page = 9             │
└────────────────────────────────┘
```

### Bootstrap Process

```cpp
// On database open
DiskManager dm("mydb.db");

// Read catalog from Page 0
char catalog_buf[PAGE_SIZE];
dm.ReadPage(0, catalog_buf);
auto* catalog = reinterpret_cast<DatabaseCatalog*>(catalog_buf);

// Verify magic number
if (catalog->magic_number != 0xDEADBEEF) {
  throw std::runtime_error("Not a valid database file");
}

// Load tables into memory
for (uint32_t i = 0; i < catalog->num_tables; i++) {
  TableMetadata* table = &catalog->tables[i];
  // Register table in in-memory catalog
}
```

---

## Visual Examples

### Example 1: Empty Database

```
mydb.db (4096 bytes)
┌──────────────────────┐
│ Page 0: CATALOG      │
│   magic = 0xDEADBEEF │
│   version = 1        │
│   num_tables = 0     │
│   (rest zeros)       │
└──────────────────────┘
```

### Example 2: One Table, 10 Rows

```sql
CREATE TABLE users (id BIGINT, name VARCHAR, age BIGINT);
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), ...;
```

```
mydb.db (16384 bytes = 4 pages)
┌──────────────────────────────────┐
│ Page 0: CATALOG                  │
│   Table "users":                 │
│     id:   first_page=1           │
│     name: first_page=2           │
│     age:  first_page=3           │
│     Dictionary: {0:"Alice", 1:"Bob"}│
├──────────────────────────────────┤
│ Page 1: users.id (BIGINT)        │
│   next_page = -1                 │
│   count = 10                     │
│   values = [1, 2, 3, ..., 10]    │
├──────────────────────────────────┤
│ Page 2: users.name (VARCHAR)     │
│   next_page = -1                 │
│   count = 10                     │
│   codes = [0, 1, 0, ...]         │  ← Dictionary codes
├──────────────────────────────────┤
│ Page 3: users.age (BIGINT)       │
│   next_page = -1                 │
│   count = 10                     │
│   values = [30, 25, 35, ...]     │
└──────────────────────────────────┘
```

### Example 3: Two Tables, 10K Rows Each

```
mydb.db (~200 pages = 819KB)
┌──────────────────────────────────┐
│ Page 0: CATALOG                  │
│   Table "users": 3 cols          │
│   Table "orders": 2 cols         │
├──────────────────────────────────┤
│ Pages 1-20: users.id             │
│   (linked list: 1→2→...→20→-1)   │
├──────────────────────────────────┤
│ Pages 21-40: users.name          │
│   (linked list: 21→22→...→40→-1) │
├──────────────────────────────────┤
│ Pages 41-60: users.age           │
├──────────────────────────────────┤
│ Pages 61-80: orders.order_id     │
├──────────────────────────────────┤
│ Pages 81-100: orders.amount      │
└──────────────────────────────────┘
```

**Key observation:** Columns interleaved, but each maintains its own linked list.

---

## Summary: Architectural Choices

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **File organization** | Single file | Simplest for learning |
| **Page linking** | Linked lists | Flexible dynamic growth |
| **Allocation** | On-demand | No wasted pre-allocation |
| **Page initialization** | Eager (zeros) | Clear semantics |
| **Page 0** | Reserved for catalog | Standard practice |
| **VARCHAR storage** | Dictionary encoding | Industry standard, columnar-friendly |
| **Column growth** | Extend linked list | No fixed limits |

---

## Implementation Roadmap

### Phase 1: Disk Manager (Current)
- ✅ Page-based I/O
- ✅ Single file
- ✅ Linear addressing
- ⏳ Eager allocation (to implement)

### Phase 2: Columnar Storage
- Implement `ColumnDataPage` structure
- Linked list traversal
- Support BIGINT only

### Phase 3: Catalog & SQL
- Implement Page 0 catalog
- Serialize/deserialize metadata
- Support CREATE TABLE, INSERT, SELECT

### Phase 4-8: Advanced Features
- WAL, transactions, MVCC, query execution, dictionary encoding, vectorization

---

**Design complete!** Ready for implementation when you switch modes. 🚀

### Phase 3: Catalog (Future)

**Page 0 will be special** - reserved for catalog:

```
┌──────────────────────────────────┐
│  Page 0: Catalog (4096 bytes)    │
├──────────────────────────────────┤
│  Magic Number: 0xDEADBEEF        │
│  Version: 1                      │
│  Num Tables: N                   │
├──────────────────────────────────┤
│  Table 1: name, schema, first_pg │
│  Table 2: name, schema, first_pg │
│  ...                             │
└──────────────────────────────────┘
```

**For now (Phase 1):** Page 0 is just another page!

---

## File Size Examples

### Empty Database

```bash
$ ls -l mydb.db
0 bytes
```

**num_pages = 0**

### 1 Page Allocated and Written

```bash
$ ls -l mydb.db
4096 bytes
```

**num_pages = 1**

### 10 Pages

```bash
$ ls -l mydb.db
40960 bytes (40 KB)
```

**num_pages = 10**

### 1000 Pages

```bash
$ ls -l mydb.db
4096000 bytes (~4 MB)
```

**num_pages = 1000**

### Max Theoretical Size

With `page_id_t = int32_t`:
- Max page_id = 2,147,483,647 (2^31 - 1)
- Max file size = 2^31 × 4096 = **8 TB**

(In practice, you'd hit filesystem limits first)

---

## Inspecting the File (Manual Testing)

### Create a Test Database

```cpp
#include "columnar_db/storage/disk_manager.h"
#include <cstring>

int main() {
  db::DiskManager dm("test.db");

  // Allocate 3 pages
  auto p0 = dm.AllocatePage();  // page_id = 0
  auto p1 = dm.AllocatePage();  // page_id = 1
  auto p2 = dm.AllocatePage();  // page_id = 2

  // Write different patterns
  char buf[db::PAGE_SIZE];

  std::memset(buf, 'A', db::PAGE_SIZE);
  dm.WritePage(p0, buf);

  std::memset(buf, 'B', db::PAGE_SIZE);
  dm.WritePage(p1, buf);

  std::memset(buf, 'C', db::PAGE_SIZE);
  dm.WritePage(p2, buf);

  return 0;
}
```

### Inspect with hexdump

```bash
$ hexdump -C test.db | head -30
```

**Expected output:**

```
# Page 0 (offset 0x0000 - 0x0FFF): All 'A' (0x41)
00000000  41 41 41 41 41 41 41 41  41 41 41 41 41 41 41 41  |AAAAAAAAAAAAAAAA|
*
00001000

# Page 1 (offset 0x1000 - 0x1FFF): All 'B' (0x42)
00001000  42 42 42 42 42 42 42 42  42 42 42 42 42 42 42 42  |BBBBBBBBBBBBBBBB|
*
00002000

# Page 2 (offset 0x2000 - 0x2FFF): All 'C' (0x43)
00002000  43 43 43 43 43 43 43 43  43 43 43 43 43 43 43 43  |CCCCCCCCCCCCCCCC|
*
00003000
```

**Key observations:**
- `*` means "repeated line" (hexdump compresses identical lines)
- Offsets are in hex: 0x0000, 0x1000 (4096), 0x2000 (8192)
- Page boundaries are clearly visible

### Inspect File Size

```bash
$ ls -lh test.db
-rw-r--r--  1 user  staff   12K Jan  6 10:00 test.db
```

**12K = 3 pages × 4KB** ✅

### Verify with od (octal dump)

```bash
$ od -A x -t x1z -N 64 test.db
```

Shows first 64 bytes in hex + ASCII.

---

## File Growth Behavior

### Lazy Allocation (Current Implementation)

```cpp
page_id_t p0 = dm.AllocatePage();  // num_pages = 1, file size = 0
page_id_t p1 = dm.AllocatePage();  // num_pages = 2, file size = 0
// File is STILL EMPTY (0 bytes)

dm.WritePage(p0, data);  // NOW file grows to 4096 bytes
dm.WritePage(p1, data);  // NOW file grows to 8192 bytes
```

**Key insight:** File grows only when `WritePage()` is called!

### What Happens if You Read Before Write?

```cpp
dm.AllocatePage();  // page_id = 0, num_pages = 1
char buf[PAGE_SIZE];
dm.ReadPage(0, buf);  // THROWS! Page doesn't exist on disk yet
```

**Why?** Because `ReadPage()` checks:
```cpp
if (page_id >= num_pages_) { throw std::out_of_range(...); }
```

Wait, this is wrong! Let me check the logic...

Actually looking at the code:
- `AllocatePage()` increments `num_pages_`
- But doesn't write to disk
- `ReadPage()` checks `page_id < num_pages_`
- If file is empty (size = 0), but `num_pages_ = 1`, ReadPage will try to read... and fail!

**THIS IS A BUG IN OUR DESIGN!** 🐛

Let me fix this...

### Design Issue: Allocated vs. On-Disk Pages

There's a subtle difference:
- **Allocated pages**: `num_pages_` (logical count)
- **On-disk pages**: `file_size / PAGE_SIZE` (physical count)

Current implementation conflates these!

**Two options:**

#### Option A: Eager Allocation (PostgreSQL-style)
```cpp
page_id_t AllocatePage() {
  page_id_t pid = num_pages_++;

  // Zero-initialize page immediately
  char zeros[PAGE_SIZE] = {0};
  WritePage(pid, zeros);

  return pid;
}
```

**Pros:** No surprise errors, page always readable
**Cons:** Extra I/O, slower allocation

#### Option B: Two Counters (DuckDB-style)
```cpp
private:
  size_t num_allocated_;  // Logical pages (including not-yet-written)
  size_t num_on_disk_;    // Physical pages (actually in file)
```

**Pros:** Clear separation, lazy allocation works
**Cons:** More complex bookkeeping

#### Option C: Current (Lazy, Document Caveat)
Keep current design, but document:
- "Must call `WritePage()` before `ReadPage()` for newly allocated pages"
- Tests verify this behavior

**For learning:** Let's go with **Option A (Eager Allocation)** - simpler mental model!

---

## Recommended Fix

Update `AllocatePage()`:

```cpp
page_id_t DiskManager::AllocatePage() {
  page_id_t new_page_id = static_cast<page_id_t>(num_pages_);

  // Initialize page with zeros (eager allocation)
  char zeros[PAGE_SIZE];
  std::memset(zeros, 0, PAGE_SIZE);
  WritePage(new_page_id, zeros);

  return new_page_id;
}
```

Now:
- `AllocatePage()` returns a **readable** page
- File grows immediately
- `num_pages_` always matches physical file size

---

## Summary: Phase 1 File Layout

### Current State

✅ **Simple linear layout:** offset = page_id × 4096
✅ **No metadata:** Pure page storage
✅ **Append-only:** Pages allocated sequentially
⚠️ **Needs fix:** Eager allocation for consistency

### What's Missing (Future Phases)

- ❌ File header (magic number, version)
- ❌ Page 0 reserved for catalog
- ❌ Free page list (page recycling)
- ❌ Page checksums (detect corruption)
- ❌ Page-level metadata (LSN for WAL)

### What This Teaches

1. **Simplicity first:** Start with simplest design
2. **Incremental complexity:** Add features as needed
3. **Offset arithmetic:** The heart of page-based storage
4. **File inspection:** Understanding what's on disk
5. **Edge cases:** Allocated vs. on-disk pages

---

## Next Steps

1. **Fix `AllocatePage()`:** Implement eager allocation
2. **Update tests:** Verify new behavior
3. **Move to Phase 1, Step 2:** Implement `Page` class

---

**Question for you:** Should we go with **eager allocation (Option A)** or keep lazy and document the caveat?

For learning, I recommend eager - clearer semantics, no surprises!
