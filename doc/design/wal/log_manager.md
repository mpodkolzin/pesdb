# LogManager / WAL Design Document — Phase 1

**Component:** Recovery Foundation — Write-Ahead Log (Phase 1: Logical, Single-Threaded Recovery)
**Status:** ✅ **Phase 1 complete** — built, tested, wired into the default build. Not yet connected to a write path.
**This doc covers:** the Phase 1 design (still accurate as a historical record) and a **Deferred Work** punch list of everything *not* in Phase 1 so we can pick it up later. New work building on this should live in its own design doc (e.g. `wal_write_path_wiring.md`).

---

## Overview

The **LogManager** is the durability layer for changes that happen in memory. Before any modification reaches the on-disk pages (via the BufferPoolManager), a description of that modification is appended to the WAL file and `fsync`'d to disk. If the process crashes between the log write and the page write, recovery replays the log on startup and re-applies the change.

```
SQL Executor (INSERT / UPDATE / DELETE)
       │
       ├──► LogManager::AppendLogRecord  ───► fsync ───► WAL file (mydb.wal)
       │
       └──► BufferPoolManager (modifies in-memory page, marked dirty)
                         │
                         └──► DiskManager (write-back later, on flush/eviction)
```

The contract: **the log record for a change is durable on disk before the change is allowed to be observed as committed.** That is the "write-ahead" in WAL.

---

## Where We Are Now (Phase 1 Closeout)

Live under `src/wal/`, `include/columnar_db/wal/`, and `tests/unit/wal/`:

- **`LogRecord`** — logical, length-prefixed serialization. Carries one type today, `INSERT_TUPLE`, with a `std::string` table name and a `std::vector<int64_t>` tuple. `LogRecordType` underlying type is pinned to `uint8_t` so the on-disk byte width is stable across compilers. `Deserialize(buffer, out)` takes a pointer to a complete record (size header included) and parses linearly.
- **`LogManager`** — opens an `fstream` in `in | out | app | binary` (with a create-then-reopen fallback for first run). `AppendLogRecord` serializes into a temp buffer, `write()`s, and `flush()`es to the OS page cache. `ReadAllLogRecords` rewinds, frame-decodes records, and treats both clean EOF and any short read (torn header *or* torn body, *or* a `record_size < sizeof(uint32_t)` corrupt header) as end-of-log. `ClearLog` close→`trunc`→reopen-in-append. One `std::mutex latch_` across all three public methods. Copy/move all deleted.
- **Build** — `src/CMakeLists.txt` does `add_subdirectory(wal)`; the library is built by default.
- **Tests** — `tests/unit/wal/{log_record_test.cpp, log_manager_test.cpp}` cover round-trip (full / empty-tuple / empty-name / large-tuple), `GetSize()` accuracy, first-4-bytes-are-size, append+read-on-same-instance, durability across destructor + reopen, torn-body, torn-header, and `ClearLog`-then-append-still-works.

What's *not* in Phase 1 (intentional — see [Deferred Work](#deferred-work--future-phases)):

- No `fsync(2)` — durability is process-crash safe only, not kernel/power-crash safe.
- No LSN, no transaction records, no recovery driver.
- WAL is not connected to any write path — nothing currently calls `AppendLogRecord`.

---

## Goals — Phase 1

1. **Build integration** — make the `wal` library compile as part of the default build, alongside `columnar_db_storage`.
2. **Test coverage** — round-trip serialization, append+read-all, multiple records, partial/torn tail, clear-and-reuse.
3. **Tighten the existing implementation** where doing so doesn't change the API:
   - Make append durability explicit (`flush` → `fsync`-equivalent guarantee).
   - Document/handle the `std::ios::app` interaction with `seekp`/`seekg`.
   - Make `Deserialize`'s contract symmetric with `Serialize` (it currently relies on the reader pre-stripping the size header in a confusing way — see below).
4. **No new functionality** — don't add LSNs, txn IDs, or recovery driver yet. Those are Phase 2.

---

## Non-Goals — Phase 1

- ❌ LSNs and ordering across log records (Phase 2)
- ❌ Transaction IDs / BEGIN-COMMIT-ABORT records (Phase 2)
- ❌ ARIES-style page-LSN tracking on `Page` (Phase 3)
- ❌ Checkpoints / log truncation policy (Phase 3)
- ❌ Group commit, async log writer thread (Phase 4 / perf)
- ❌ A recovery driver that replays records into the catalog/heap. There is no catalog/heap in the new tree yet — adding one is a separate work stream.

---

## Key Concepts

### 1. Write-Ahead

The rule: "log first, page later." Concretely, before `BufferPoolManager` is allowed to acknowledge a write as durable, the corresponding `LogRecord` must already be on disk. In Phase 1 we enforce this by fsync-ing on every `AppendLogRecord` call, so by the time `Append` returns the record is durable.

This is conservative (one fsync per record = poor throughput) but correct, and it's the right starting point for a learning project.

### 2. Logical vs Physical Logging

Two main flavors:

- **Physical (page-level)**: log "page 42, byte offset 100, write these 8 bytes." Recovery is just re-applying byte writes. Simple to recover, but logs are large and tightly coupled to page format.
- **Logical (operation-level)**: log "INSERT (1, 25) into table 'users'." Recovery has to re-execute the operation. Logs are small, but recovery has to be idempotent and the executor must be available.

**We chose logical** for Phase 1. It's smaller, easier to read in a hex dump, and aligns with what a teaching project wants to show. The downside (idempotency, dependence on a working executor) lands later when we actually build a recovery driver.

### 3. Durability via fsync

Calling `wal_file_.flush()` on a `std::fstream` only pushes data from the C++ stream buffer into the OS page cache. To get a true durability guarantee, the OS has to be told to push that page cache to the physical device — `fsync(fd)` on POSIX, `FlushFileBuffers` on Windows.

`std::fstream` does not expose `fsync`. For Phase 1 we will document this gap explicitly (the current `flush()` is "best-effort" and crash-safety in the OS-crash sense is *not* guaranteed). A future change can switch the WAL to a raw file descriptor (`open` + `write` + `fsync`) when we need real durability.

### 4. Recovery Sketch (Phase 2 preview)

```
on startup:
  records = log_manager.ReadAllLogRecords()
  for r in records:
    catalog.Apply(r)   // idempotent re-execution
  log_manager.ClearLog()  // optional: truncate after successful recovery
```

This is what the API is *shaped for*; we are not building it yet.

---

## Design Decisions

### Decision 1: One Log File, Append-Only

`mydb.wal`, opened once at startup, written sequentially. No segmentation, no rotation, no per-table logs.

**Rationale:** simplest possible thing. PostgreSQL splits WAL into 16 MB segments for retention/archival; we don't need that until we have checkpoints (Phase 3).

**Trade-off:** the file grows unbounded until `ClearLog()` is called. That's acceptable for a learning project; it would not be acceptable in production.

### Decision 2: Length-Prefixed Variable-Size Records

Each record on disk:

```
[uint32 total_size][LogRecordType type]
[uint32 name_len][name_len bytes table_name]
[uint32 tuple_len][tuple_len * 8 bytes tuple_data]
```

The `total_size` header makes the read loop trivial: read 4 bytes, allocate that many, read the rest, deserialize. It also gives us a natural way to detect torn tail records: if we can read the size header but not the full body, we treat it as a crash truncation and stop.

**Rationale:** generic enough that adding new record types later is just a new `LogRecordType` value plus a new payload layout — no framing changes.

### Decision 3: Logical Records, Not Page-Level

See *Key Concepts §2*. We log "INSERT (1, 25) into users", not "page 42 byte 100 write 0x01000000…". When the catalog/heap exist, recovery will re-execute the insert through the same code path that handles a live `INSERT`.

**Trade-off:** we now owe ourselves idempotency once recovery exists. With logical records and no LSN, replaying twice would re-insert. Acceptable for Phase 1 (no recovery driver). Phase 2 introduces LSN + page-LSN comparison so we only re-apply records that haven't yet made it to the page.

### Decision 4: Coarse Mutex Around the File

`std::mutex latch_` held for the entire `Append` (including `flush`). Multi-threaded callers serialize.

**Rationale:** correct, easy to reason about, and matches the BufferPoolManager Phase A locking philosophy. Group commit / log buffer + background flusher is a Phase 4 perf concern.

### Decision 5: `std::fstream` (for now)

We use `std::fstream` because it's portable and the API is familiar from the rest of the storage code. We accept that this means:

- `flush()` only reaches the OS buffer, not the device (no `fsync`).
- The `in | out | app | binary` mode combination is unusual — `app` forces all writes to the end of file regardless of `seekp`. This matters when we want to read the existing log and then append: reads can be positional via `seekg`, but writes always go to EOF. For Phase 1 that's exactly what we want.

When durability becomes real (Phase 3+), we'll switch to `::open` / `::write` / `::fsync`.

---

## API

### `LogRecord` (`include/columnar_db/wal/log_record.h`)

```cpp
enum class LogRecordType {
  INVALID,
  INSERT_TUPLE
  // Phase 2: BEGIN_TXN, COMMIT_TXN, ABORT_TXN, UPDATE_TUPLE, DELETE_TUPLE, CREATE_TABLE
};

class LogRecord {
 public:
  LogRecord(LogRecordType type,
            std::string table_name,
            std::vector<int64_t> tuple);

  void Serialize(char* buffer) const;
  static uint32_t Deserialize(const char* buffer, LogRecord& out);
  uint32_t GetSize() const;

  LogRecordType GetType() const;
  const std::string& GetTableName() const;
  const std::vector<int64_t>& GetTuple() const;
};
```

**Contract change for Phase 1 cleanup:** `Deserialize(buffer, out)` will assume `buffer` points to a complete record beginning with its `[uint32 total_size]` header. The caller (`LogManager::ReadAllLogRecords`) supplies the full record buffer; today it copies the size header into the front of the body buffer and then reads the rest behind it, which works but is non-obvious. The fix is to read the size header, allocate `size` bytes, copy the header into the front, read `size - sizeof(uint32_t)` bytes for the body — same on-disk layout, but the data flow is explicit.

### `LogManager` (`include/columnar_db/wal/log_manager.h`)

```cpp
class LogManager {
 public:
  explicit LogManager(const std::string& wal_file);
  ~LogManager();

  // Append + fsync (best-effort in Phase 1, see Decision 5).
  void AppendLogRecord(const LogRecord& record);

  // Read every committed-on-disk record from the start of the file.
  // Stops cleanly on a torn tail (partial header or partial body).
  std::vector<LogRecord> ReadAllLogRecords();

  // Truncate the log to zero bytes. Used after a successful recovery.
  void ClearLog();
};
```

No public API changes are planned for Phase 1 — the work is entirely internal cleanup + tests + build wiring.

---

## Phase 1 Outcomes (Known Issues — Status)

The Phase 1 punch list, with what actually shipped:

1. ✅ **Build wiring.** `src/CMakeLists.txt:3` now does `add_subdirectory(wal)`; the `wal` library is part of the default build.
2. ✅ **`Deserialize` data flow.** `Deserialize(buffer, out)` takes a pointer to a complete record (size header included) and parses linearly. The caller-side copy-the-header-into-the-body trick is gone.
3. ✅ **`flush()` ≠ `fsync()`.** Documented in the `LogManager` class header (`include/columnar_db/wal/log_manager.h`) and inline in `AppendLogRecord` — explicitly states that we survive a process crash but not a kernel/power crash, and points to Phase 3 for the fix.
4. ✅ **`ReadAllLogRecords` + corrupt tail.** Loop comments explain torn-header and torn-body as end-of-log, plus a guard for `record_size < sizeof(uint32_t)` (corrupt header). Tests `TornBodyTreatedAsEof` and `TornHeaderTreatedAsEof` lock the behavior in.
5. ✅ **`LogRecordType` underlying type.** Pinned to `uint8_t` (`enum class LogRecordType : uint8_t`). On-disk type byte is stable.
6. ⚠️ **`main.cpp` is still broken** against the new tree (uses deleted `catalog.h`, `engine/query_executor.h`, `BUFFER_POOL_SIZE`). Not a WAL bug; tracked under [Deferred Work](#deferred-work--future-phases) as a prerequisite for the write-path wiring.

---

## Testing Strategy

All tests live under `tests/unit/wal/` and register a new `wal_tests` executable, mirroring `tests/unit/storage/`. Each test that touches the filesystem uses a unique temp filename and `std::filesystem::remove`s it before and after.

### `LogRecord` round-trip

```cpp
TEST(LogRecordTest, RoundTripPreservesAllFields) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "users", {1, 25});
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  LogRecord out(LogRecordType::INVALID, "", {});
  uint32_t consumed = LogRecord::Deserialize(buf.data(), out);

  EXPECT_EQ(consumed, r.GetSize());
  EXPECT_EQ(out.GetType(), LogRecordType::INSERT_TUPLE);
  EXPECT_EQ(out.GetTableName(), "users");
  EXPECT_EQ(out.GetTuple(), (std::vector<int64_t>{1, 25}));
}
```

Variants: empty tuple, empty table name, large tuple (100+ int64s), table name with non-ASCII bytes (binary-safe).

### `LogRecord::GetSize` matches actual serialized size

```cpp
TEST(LogRecordTest, GetSizeMatchesSerializedBytes) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "t", {7, 8, 9});
  std::vector<char> buf(r.GetSize() + 16, 0xAB);  // sentinel
  r.Serialize(buf.data());
  for (size_t i = r.GetSize(); i < buf.size(); ++i) {
    EXPECT_EQ(static_cast<unsigned char>(buf[i]), 0xAB)
        << "Serialize wrote past GetSize() at offset " << i;
  }
}
```

### `LogManager` append + read-all

```cpp
TEST(LogManagerTest, AppendThenReadAllReturnsRecordsInOrder) {
  const std::string wal = "test_append_read.wal";
  std::filesystem::remove(wal);

  {
    LogManager lm(wal);
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {1, 25}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {2, 30}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "orders", {99}));
  }

  LogManager lm2(wal);
  auto records = lm2.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 3u);
  EXPECT_EQ(records[0].GetTableName(), "users");
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{1, 25}));
  EXPECT_EQ(records[2].GetTableName(), "orders");
  EXPECT_EQ(records[2].GetTuple(), (std::vector<int64_t>{99}));

  std::filesystem::remove(wal);
}
```

### Durability across destructor / re-open

The above test already exercises this implicitly: records appended in the first `LogManager` instance must be visible to a fresh `LogManager` opened on the same file. This is what proves the `flush()` reaches the OS, modulo fsync caveats.

### Torn tail (corrupt last record)

```cpp
TEST(LogManagerTest, TornTailIsTreatedAsEndOfLog) {
  const std::string wal = "test_torn_tail.wal";
  std::filesystem::remove(wal);

  {
    LogManager lm(wal);
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {1, 25}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {2, 30}));
  }

  // Truncate the file partway through the second record's body.
  auto good_size = std::filesystem::file_size(wal);
  std::filesystem::resize_file(wal, good_size - 4);

  LogManager lm2(wal);
  auto records = lm2.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{1, 25}));

  std::filesystem::remove(wal);
}
```

Variant: truncate so even the size header of the last record is incomplete.

### `ClearLog` truncates and allows append to continue

```cpp
TEST(LogManagerTest, ClearLogTruncatesAndKeepsHandleUsable) {
  const std::string wal = "test_clear.wal";
  std::filesystem::remove(wal);

  LogManager lm(wal);
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {1}));
  EXPECT_GT(std::filesystem::file_size(wal), 0u);

  lm.ClearLog();
  EXPECT_EQ(std::filesystem::file_size(wal), 0u);

  // Append must still work after a clear.
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {2}));
  auto records = lm.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{2}));

  std::filesystem::remove(wal);
}
```

### Empty WAL file reads as zero records

```cpp
TEST(LogManagerTest, EmptyFileReadsZeroRecords) {
  const std::string wal = "test_empty.wal";
  std::filesystem::remove(wal);

  LogManager lm(wal);
  EXPECT_TRUE(lm.ReadAllLogRecords().empty());

  std::filesystem::remove(wal);
}
```

### Concurrency smoke test (optional but cheap)

Spawn N threads each appending M records, then verify `ReadAllLogRecords().size() == N*M`. Doesn't prove much beyond "the mutex isn't a no-op," but catches gross regressions.

---

## File / Build Layout Changes

```
include/columnar_db/wal/         # already exists
  log_record.h
  log_manager.h

src/wal/                          # already exists
  log_record.cpp
  log_manager.cpp
  CMakeLists.txt                  # already exists, defines `wal` library

src/CMakeLists.txt                # ADD: add_subdirectory(wal)

tests/unit/wal/                   # NEW
  CMakeLists.txt                  # NEW: defines `wal_tests` executable
  log_record_test.cpp             # NEW
  log_manager_test.cpp            # NEW

tests/unit/CMakeLists.txt         # ADD: add_subdirectory(wal)
```

`wal_tests` links against the `wal` library and `GTest::gtest_main`, mirroring `storage_tests`.

---

## Implementation Plan

1. ✅ Design exploration / context (`doc/design_exploration.md` Session 3).
2. ✅ Write formal design doc (this file).
3. ✅ Wire `wal` into the build (`src/CMakeLists.txt:3`).
4. ✅ Apply the Known Issues fixes (pinned `LogRecordType` underlying type, restructured `Deserialize`, documented `flush` vs `fsync` in the header, commented torn-tail behavior in `ReadAllLogRecords`).
5. ✅ Add `tests/unit/wal/` with `log_record_test.cpp` and `log_manager_test.cpp`, wired into `tests/unit/CMakeLists.txt`.
6. ✅ Build and run: storage + wal tests all pass.
7. ⚠️ Tracked separately: `src/main/main.cpp` is still broken (catalog/engine deps removed). Carried as a prerequisite under Deferred Work; will be resolved as part of write-path wiring.

---

## Success Criteria — Phase 1 (all met)

- ✅ `cmake --build` produces both `columnar_db_storage` and `wal` libraries.
- ✅ `ctest` (or running `storage_tests` and `wal_tests` directly) passes all tests.
- ✅ Round-trip serialization is verified for `INSERT_TUPLE` records of various shapes.
- ✅ A WAL file written by one `LogManager` instance is read back identically by a fresh instance.
- ✅ A truncated/torn tail does not crash recovery and yields only the records that were fully written.
- ✅ The header explicitly documents the `flush` vs `fsync` distinction (no false durability claims).
- ✅ `LogRecordType` has a fixed underlying type so the wire format is portable.

**Known limitation carried forward:**
- ⚠️ Durability is best-effort under `std::fstream` (no `fsync`). Acceptable for Phase 1; revisited under Deferred Work § Phase 3 when we need real crash safety against an OS-level crash.

---

## Deferred Work — Future Phases

Everything that is **not in Phase 1** but is needed before WAL becomes load-bearing. Items grouped by phase, ordered roughly by dependency. Each is a future work item — pick one off the list, write a small design doc for it, implement, tick the box here.

### Phase 2 — Wiring WAL to a Write Path

Goal: every mutation logs first, and replay-after-crash reproduces state. This is what we're brainstorming next.

- [ ] **Catalog port.** Move `old/include/catalog.h` + `old/src/storage/catalog.cpp` into `include/columnar_db/storage/` + `src/storage/`, adapted to the new tree. Without it, an `INSERT_TUPLE` replay has nowhere to land. Prerequisite for everything else in Phase 2.
- [ ] **Tuple/heap (or equivalent target).** Decide and build the smallest *thing* that an `INSERT_TUPLE` can be applied to: row-oriented heap page, or a stub columnar segment. Drives the shape of `INSERT_TUPLE` payloads.
- [ ] **`lsn_t` allocation in `LogManager`.** Make `AppendLogRecord` return the assigned LSN. `lsn_t` is already declared in `include/columnar_db/common/types.h:37` but unused.
- [ ] **`page_lsn` on `Page`.** Each page remembers the LSN of the last log record applied to it. Enables idempotent replay (skip records with `lsn ≤ page_lsn`).
- [ ] **Write-ahead invariant in `BufferPoolManager`.** Before flushing a dirty page, ensure `flushed_wal_lsn ≥ page_lsn`. Concretely: WAL must be flushed up to the page's LSN before the page is allowed to leave the buffer pool. This is the *actual* "write-ahead" guarantee.
- [ ] **Recovery driver.** New module `src/recovery/` (already commented out at `src/CMakeLists.txt:6`). First version: `Recover(LogManager&, Catalog&, BufferPoolManager&)` → `ReadAllLogRecords()` then apply each record (idempotent via page-LSN check) → `ClearLog()` after success. End-to-end test: append → destroy → recover into a fresh state → assert.
- [ ] **Wire `AppendLogRecord` into the write path.** Once a write path exists in the new tree (likely a re-ported `QueryExecutor` or its successor), every mutating op calls `AppendLogRecord` *before* mutating the buffer pool, and stamps the page's `page_lsn` after.
- [ ] **Fix `src/main/main.cpp`.** Currently broken (refers to deleted `catalog.h`, `engine/query_executor.h`, `BUFFER_POOL_SIZE`). Resolves itself once Catalog + a write path are back.

### Phase 3 — Transactions

Goal: replay can distinguish committed from uncommitted work. Requires Phase 2 done.

- [ ] **Transaction record types.** Add `BEGIN_TXN`, `COMMIT_TXN`, `ABORT_TXN`, plus `UPDATE_TUPLE`, `DELETE_TUPLE`, `CREATE_TABLE` to `LogRecordType`. Bumps the wire format — either version the file or refuse to read v1 logs (fine for a learning project).
- [ ] **`txn_id_t` on every data record.** Threaded through `LogRecord` payload.
- [ ] **Recovery: redo committed, skip uncommitted.** Two-pass recovery: first scan to collect the set of committed txn IDs (saw `COMMIT_TXN`), second pass redoes only those records.
- [ ] **(Optional) Undo for in-place updates.** Not needed yet — we have no in-place updates. Becomes relevant once `UPDATE_TUPLE` modifies a page in place.

### Phase 4 — Real Durability

Goal: survive a kernel/power crash, not just a process crash. Self-contained, no other layer involved.

- [ ] **Raw fd + `fsync(2)`.** Replace `std::fstream` with `::open` / `::write` / `::fsync` (and `FlushFileBuffers` on Windows if we ever care). Expose `Sync()` explicitly or fsync per append depending on the durability mode.
- [ ] **Document the durability contract.** "After `Append` returns, the record is on the device" — currently the header says the opposite, which is honest but should change once Phase 4 lands.
- [ ] **Test for real durability.** Kill -9 the process, restart, assert records are still readable. Power-loss tests are out of scope; OS-crash tests via `O_DIRECT` or process kill are achievable.

### Phase 5 — Performance / Production Concerns

Goal: stop being a learning prototype.

- [ ] **Log buffer + group commit.** In-memory ring buffer of pending records; a background flusher coalesces fsyncs. Requires a workload to measure against — premature otherwise.
- [ ] **Checkpoints + log truncation.** The WAL grows unbounded today. A checkpoint flushes all dirty pages and lets us truncate the log up to the checkpoint's LSN.
- [ ] **Log segmentation.** Split WAL into fixed-size segments (Postgres uses 16 MB) for retention/archival. Enabled by checkpoints.
- [ ] **Per-record CRC32 checksums.** Today a bit-flip mid-record deserializes into garbage silently. Add a CRC at the end of each record; on read, verify and treat mismatch as torn-tail-equivalent. Bumps the wire format.

### Phase 6 — Other Known Gaps

Smaller items that don't fit neatly into a phase:

- [ ] **Generalize tuple types.** Today `LogRecord` carries `std::vector<int64_t>` only — mirrors the BIGINT-only catalog. Generalize when the catalog grows real types (`DataType::DOUBLE`, `VARCHAR`, …).
- [ ] **Endian-explicit serialization.** Currently we `memcpy` integers, which means the WAL is host-endian. Fine for single-host use; would break if a WAL written on x86 were read on a big-endian box. Not a real-world problem for this project, but worth a comment in `log_record.cpp`.
- [ ] **Fine-grained locking / async writer.** One coarse `latch_` serializes Append, ReadAllLogRecords, and ClearLog. Fine until profiling shows it. Decoupling read/write contention or moving to an async writer is downstream of group commit (Phase 5).

---

## References

**Industry implementations:**
- **PostgreSQL**: `src/backend/access/transam/xlog.c` — production WAL.
- **SQLite**: `src/wal.c` — well-documented, single-file WAL.
- **BusTub** (CMU 15-445): educational `LogManager` and recovery manager.

**Papers:**
- Mohan et al., *ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging* (1992).

**In-tree references:**
- `doc/design/storage/buffer_pool_manager.md` — Phase A/B/C model that this doc mirrors.
- `old/src/storage/` — pre-rewrite reference for how WAL plugged into the executor previously.

---

**Phase 1 complete.** Next: brainstorm and design **Phase 2 — wiring WAL to the write path** (see [Deferred Work § Phase 2](#phase-2--wiring-wal-to-a-write-path)). New phase work should land in its own design doc rather than expanding this one further.
