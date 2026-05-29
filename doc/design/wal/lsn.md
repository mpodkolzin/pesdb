# LSN — Introducing Log Sequence Numbers

**Component:** WAL Phase 2, slice 1 of 3.
**Status:** ✅ Implemented (2026-05-28). All success criteria met; 75/75 tests pass.
**Predecessor:** `doc/design/wal/log_manager.md` (Phase 1). This doc covers one
bullet of that doc's [Deferred Work § Phase 2](log_manager.md#phase-2--wiring-wal-to-a-write-path):
> **`lsn_t` allocation in `LogManager`.** Make `AppendLogRecord` return the assigned LSN.

**Successors (not covered here):**
1. `page_lsn` on `Page` — stamp the highest LSN applied to each page.
2. Write-ahead invariant in `BufferPoolManager` — refuse to evict a dirty page until its `page_lsn` is on disk.

Both of those *consume* what this slice produces. We do this one first, in isolation, so the wire-format bump and startup-resume logic are settled before we touch buffer-pool semantics.

---

## Why we want LSNs at all

Up to now every log record has been anonymous: a serialized blob in a sequential file, identifiable only by its position when read top-to-bottom. That works for "replay the whole log into an empty database," which is what the Phase 1 recovery slice does. It breaks the moment we want any of:

- **Idempotent replay.** If the buffer pool has already applied records 1..50 to a page (because they were flushed to disk before the crash), we don't want to redo them on recovery. We need a way to say "this page already reflects everything up to LSN 50, skip records ≤ 50."
- **Write-ahead enforcement.** Before a dirty page leaves the buffer pool for disk, the WAL must be durable up to the LSN of the latest change that touched that page. That requires (a) every change to *have* a logical "time," and (b) the buffer pool to *know* that time per page.
- **Checkpointing.** Truncate the WAL up to LSN X once everything ≤ X is reflected in the data files. Without LSNs there's no "X" to talk about.

So LSNs are the substrate the next two slices rest on. This slice introduces the substrate and nothing else.

### Concept: LSN as logical clock

An LSN is a **monotonically increasing identifier for a log record.** Two key properties:

1. **Total order.** Given any two records, one happened before the other. The WAL is the totally-ordered history of all changes.
2. **Allocation order == disk order.** If record A's LSN < record B's LSN, then A's bytes appear *earlier in the file* than B's. This is the invariant we'll pay attention to in implementation (§ Threading).

LSNs are not transaction IDs. A single transaction will (in Phase 3) span many records, each with its own LSN. LSN orders *records*; txn ID groups records into transactions.

---

## Design decisions

### 1. Counter, not byte offset

PostgreSQL's LSN is the **byte position in the WAL stream**: `lsn_t` is literally "how many bytes into the log were we when this record started." That has two huge benefits:

- **O(1) seek-to-record.** Given an LSN, you know exactly where on disk to find the record.
- **"Flush up to LSN X" is trivially "fsync the file up to byte offset X."** Maps directly onto file-system primitives.

The cost: LSN values aren't consecutive (each record is a different size, so LSNs grow in irregular jumps), and the allocation scheme is tightly coupled to the on-disk file layout — checkpoints, segmentation, and torn writes all interact with it.

**We pick: simple counter.** `next_lsn_` starts at 1, `AppendLogRecord` allocates `next_lsn_++` under the mutex. LSNs are 1, 2, 3, … with no relationship to file offsets.

Trade-off accepted: when we eventually want "flush up to LSN X," we'll need an in-memory map from LSN → file offset, or a separate `flushed_lsn_` counter that the LogManager updates after each flush. Easy enough; that's a future slice.

Postgres's offset-based approach is worth understanding (we may revisit when we tackle checkpoints), but a counter is the right call for the learning curve right now: it lets us focus on what an LSN *means* without simultaneously absorbing the file-layout consequences.

### 2. `INVALID_LSN = 0`, real LSNs start at 1

Matches the project's existing sentinel pattern (`INVALID_PAGE_ID`). Concretely:

```cpp
// include/columnar_db/common/types.h
constexpr lsn_t INVALID_LSN = 0;
```

A `LogRecord` constructed in memory (not yet appended) has `lsn_ == INVALID_LSN`. After `AppendLogRecord` it has a real value. After `Deserialize` it has the value that was on disk. The sentinel lets future code assert "this record has been logged" without ambiguity.

### 3. Wire-format bump

Old layout (Phase 1):

```
[size:u32] [type:u8] [table_name_len:u32] [table_name] [tuple_len:u32] [tuple_data]
```

New layout (this slice):

```
[size:u32] [lsn:i64] [type:u8] [table_name_len:u32] [table_name] [tuple_len:u32] [tuple_data]
```

The LSN sits between the size header and the type. Placement is a matter of taste — putting it *first* (after `size`) means anything that scans the file for LSNs doesn't have to parse the rest of the record.

**Versioning:** none. We just break Phase 1 WAL files. No `magic` byte, no `version` field. If we ever want a versioned format, we'd add it at the **file** level (header at byte 0), not the record level — but that's another future slice and not relevant here.

### 4. Where the LSN lives in code

Add a member to `LogRecord`:

```cpp
class LogRecord {
public:
    // ... existing API ...
    lsn_t GetLSN() const { return lsn_; }

private:
    lsn_t lsn_{INVALID_LSN};
    // ... existing members ...
};
```

Plus a single mutator used only by `LogManager`:

```cpp
void SetLSN(lsn_t lsn) { lsn_ = lsn; }
```

Yes, `SetLSN` is a public mutator on what is otherwise a value type. We accept that for symmetry: `Serialize`/`Deserialize` both touch `lsn_` in the same way. The alternative (passing `lsn` as an explicit parameter to `Serialize` and keeping the LogRecord otherwise const) is *more* asymmetric, not less — the read side would still need to fill `lsn_` somehow.

### 5. `AppendLogRecord` signature

```cpp
// Allocates an LSN, stamps it into the record, serializes, writes, flushes.
// Returns the LSN that was assigned.
lsn_t AppendLogRecord(LogRecord record);
```

Two changes from today:

- **Return value** is now `lsn_t` instead of `void` — that's the whole point of this slice.
- **Argument is `LogRecord` by value** (not `const LogRecord&`). The function takes ownership so it can call `SetLSN` without forcing a `const_cast` or a defensive deep copy. Callers move the record in: `log_mgr.AppendLogRecord(std::move(rec))`.

Why by value, not non-const reference? Two reasons:

- **Move-friendly.** Callers usually construct a record and immediately log it. With value-taken-by-rvalue, no copy happens — the record's internals (`std::string`, `std::vector<int64_t>`) are stolen into the function's parameter.
- **API clarity.** A non-const reference `LogRecord&` implies "I will mutate your record and you'll keep using it." We don't want that contract — we want "you hand me this record and forget about it."

### 6. Startup: how `LogManager` resumes the LSN counter

When `LogManager` opens an existing non-empty WAL, `next_lsn_` cannot start at 1 — that would re-issue LSNs that already exist on disk. Two approaches:

**(a) Scan on construction** — In the constructor, after opening the file, walk it once and compute `next_lsn_ = max(lsn) + 1`. Empty file → `next_lsn_ = 1`.

**(b) Persist `next_lsn_` separately** — write it to a sidecar file on every shutdown, read it on startup. Cheaper at startup but adds a new on-disk artifact that has to stay consistent with the WAL.

**We pick (a).** Reasons:

- It's robust by construction — if the WAL is the only source of truth for "what records exist," then scanning the WAL is the *correct* way to know the next LSN. A sidecar file can get out of sync with the WAL (think: crash between WAL append and sidecar update).
- The cost (O(records in WAL)) is the same cost the recovery driver will pay anyway. In the eventual Phase 2 finished state, the recovery driver will scan the WAL once on startup and pass `max_lsn + 1` to `LogManager` — at that point we can swap the constructor scan for an `Init(starting_lsn)` API, but until the recovery driver exists, doing the scan inside `LogManager` is the simpler shape.
- Matches the "WAL is the single source of truth for recent history" mental model we want to build.

The implementation reuses `ReadAllLogRecords`'s parsing logic. We'll factor that path slightly so the constructor doesn't need to materialize every record into a `std::vector<LogRecord>` just to find the max LSN — see Implementation Sketch.

### 7. Threading: LSN allocation must happen under the same latch as the write

This is the invariant from § 1: **allocation order must equal disk-write order.** If two threads concurrently call `AppendLogRecord`, and we allocated LSNs *before* taking the latch, this sequence is legal:

```
T1: lsn = next_lsn_++    // T1 grabs LSN 5
T2: lsn = next_lsn_++    // T2 grabs LSN 6
T2: lock latch_, write record 6
T1: lock latch_, write record 5
```

Now record 5 is *after* record 6 in the file. Recovery sees them out of order. Bad.

Solution: allocate the LSN **inside the critical section,** after acquiring `latch_`. The existing `std::lock_guard<std::mutex>` at the top of `AppendLogRecord` already creates that section — we just put the allocation there:

```cpp
lsn_t LogManager::AppendLogRecord(LogRecord record) {
    std::lock_guard<std::mutex> lock(latch_);
    const lsn_t lsn = next_lsn_++;
    record.SetLSN(lsn);
    // ... serialize, write, flush ...
    return lsn;
}
```

This is a tiny piece of code but it's the **core WAL invariant in three lines.** Worth flagging in a comment.

### 8. `ClearLog` resets the counter

`ClearLog` truncates the WAL to zero bytes. After it returns, the next record should be LSN 1, not LSN-whatever-was-before. Reset `next_lsn_ = 1` under the latch.

(This is correct because `ClearLog` is what the recovery driver calls after a successful replay — at that point the data files reflect everything the WAL said, so there's nothing the old LSN values mean any more.)

---

## What this slice deliberately does NOT do

To keep the scope honest:

- ❌ `page_lsn` on `Page` — next slice.
- ❌ `flushed_lsn_` (highest LSN known durable) on `LogManager` — not needed until the buffer pool asks.
- ❌ "Flush up to LSN X" API — same reason.
- ❌ Any change to `BufferPoolManager`.
- ❌ Any change to `RecoveryManager` (it already replays the whole log; LSNs let it become smarter later, but it doesn't have to *yet*).
- ❌ Wire-format versioning. We just break v1 files.

---

## Implementation Sketch

### Files touched

**Headers:**
- `include/columnar_db/common/types.h` — add `INVALID_LSN`.
- `include/columnar_db/wal/log_record.h` — add `lsn_` member, `GetLSN`, `SetLSN`.
- `include/columnar_db/wal/log_manager.h` — change `AppendLogRecord` signature, add `next_lsn_` member, add private `RecoverNextLSN()` helper.

**Sources:**
- `src/wal/log_record.cpp` — bump `Serialize` / `Deserialize` / `GetSize` for the LSN field.
- `src/wal/log_manager.cpp` — allocate LSN under latch; scan the file in the constructor to set `next_lsn_`; reset on `ClearLog`.

**Tests:**
- `tests/unit/wal/log_record_test.cpp` — extend round-trip tests to assert LSN survives serialization.
- `tests/unit/wal/log_manager_test.cpp` — new cases: (i) appended record returns the LSN it got, (ii) LSNs are 1, 2, 3, …, (iii) reopened LogManager continues numbering, (iv) `ClearLog` resets to 1.

### Pseudocode for the constructor scan

We don't want to materialize every record into a vector just to find the max LSN. Factor `LogManager` so the file-frame-decoding loop can either build the vector (for `ReadAllLogRecords`) or just inspect each header (for the constructor scan). A small private helper:

```cpp
// Walks the file from byte 0, treating any torn record as end-of-log
// (same rules as ReadAllLogRecords). For each fully-written record,
// invokes `visit(record)`. Restores the put-pointer to end-of-file.
template <typename Visit>
void LogManager::ScanLogLocked(Visit visit);
```

Then both call sites are one-liners:

```cpp
// constructor
lsn_t max_lsn = 0;
ScanLogLocked([&](const LogRecord& r) { max_lsn = std::max(max_lsn, r.GetLSN()); });
next_lsn_ = max_lsn + 1;

// ReadAllLogRecords
std::vector<LogRecord> out;
ScanLogLocked([&](LogRecord r) { out.push_back(std::move(r)); });
return out;
```

(Template lets the visitor be inlined; could equally be `std::function` if we don't care about the indirection cost. For a learning project either is fine — I'll write the template form because it's also a good excuse to think about how templates interact with `.cpp` definitions.)

### Constructor mutex note

We do **not** need to take `latch_` in the constructor — no other thread can have a reference to the `LogManager` until the constructor returns. Skipping the lock there is correct, and worth a comment so a future reader doesn't "fix" it.

---

## Tricky C++ / system bits worth flagging while we're in here

These are the things to slow down on during implementation. None of them are exotic; all are easy to get subtly wrong.

### A. `std::lock_guard` vs `std::unique_lock` vs `std::scoped_lock`

`AppendLogRecord` uses `std::lock_guard<std::mutex>`. Quick taxonomy:

- **`std::lock_guard`** (C++11) — Acquires in constructor, releases in destructor. Cannot be unlocked early, cannot be moved. The minimal, zero-overhead choice when you want "hold this lock for the rest of the scope." We're using this; it's correct.
- **`std::unique_lock`** (C++11) — Same RAII shape but also supports deferred locking, manual unlock/relock, and is movable. Necessary if you want to use a condition variable, or you want to release the lock before the scope ends (e.g., to do non-critical work). Slightly heavier.
- **`std::scoped_lock`** (C++17) — Can lock *multiple* mutexes at once, deadlock-free (uses the lock-ordering algorithm internally). Use when you genuinely need to hold two mutexes; never needed for one.

For this slice we keep `lock_guard`. We don't condition-wait, don't unlock early, don't grab two mutexes.

### B. Pass-by-value with move semantics

The signature `AppendLogRecord(LogRecord record)` takes by value. The good pattern at the call site:

```cpp
LogRecord rec(LogRecordType::INSERT_TUPLE, "users", {1, 2, 3});
lsn_t lsn = log_mgr.AppendLogRecord(std::move(rec));
// `rec` is now in a valid-but-unspecified state; don't use it.
```

`std::move(rec)` casts to rvalue, the by-value parameter is move-constructed from it, the function mutates its local copy. Zero heap traffic for the string / vector — they're just pointer-swapped into `record`. This is the textbook reason to prefer pass-by-value + move over const-ref-then-copy when you know you'll need a copy anyway.

Common mistake: writing `const LogRecord& record` in the signature and then `LogRecord local = record;` inside. That forces a deep copy. The by-value signature lets the *caller* decide whether to move (cheap) or copy (correct but expensive); both are valid syntactically.

### C. Endianness and the wire format

We `memcpy` integers into the buffer, which means the WAL is **host-endian** — fine on one machine, wrong if you ever wrote on x86 and read on a big-endian box. We already accepted this for Phase 1 (`log_record.cpp` has no byte-order conversion). The new `lsn_t i64` field inherits the same property. No change needed — but worth knowing it's a latent issue we'd fix with `htobe64`/`be64toh` (or hand-rolled byte shuffles) if the project ever crossed architectures.

### D. The constructor-scan "no lock needed" subtlety

Inside `LogManager`'s constructor we touch `wal_file_` without holding `latch_`. That is **correct** — no other thread can have a reference to `*this` yet, because we haven't finished constructing it. The C++ memory model guarantees that other threads can only observe a fully constructed object *after* the constructor returns and a happens-before edge (e.g. publication via shared_ptr, mutex release) is established.

But: a future reader looking at the constructor will see "this touches mutable state without a lock" and may "fix" it by adding a `lock_guard`. That's harmless but unnecessary, and worth a one-liner comment to preempt.

### E. Symmetric Serialize / Deserialize

Once we add the LSN to the wire format, both functions must touch the field. The serialize side reads `record.lsn_`; the deserialize side writes into `out_record.lsn_`. The invariant to keep is:

```
Deserialize(Serialize(record)) == record   // including LSN
```

If we ever drift from this (e.g., serialize writes the field but deserialize forgets to read it), `ReadAllLogRecords` will start producing records whose `lsn_` is the default `INVALID_LSN`, and the constructor scan will conclude `next_lsn_ == 1` no matter how many records are on disk. The test that "reopened LogManager continues numbering" catches exactly this.

### F. The WAL invariant in one comment

Worth adding a comment at the LSN allocation site in `AppendLogRecord`:

```cpp
// LSN allocation MUST happen under latch_, in the same critical section as
// the file write. Otherwise two concurrent appends could allocate LSNs in
// one order and reach disk in the other — recovery would see records out
// of order. This is the core WAL ordering invariant.
```

This is one of those comments that earns its keep: removing the lock and allocating outside it is a perfectly natural "optimization" that breaks the world subtly. The comment exists to stop a future reader (including future-us) from doing that.

---

## Success Criteria

- ✅ `AppendLogRecord` returns the LSN it assigned.
- ✅ Successive appends produce LSNs 1, 2, 3, ….
- ✅ Closing and reopening a `LogManager` against an existing WAL file continues numbering (next LSN = max-on-disk + 1).
- ✅ `ClearLog` resets numbering to 1.
- ✅ `ReadAllLogRecords` returns records carrying their original LSNs.
- ✅ All existing tests still pass (after being updated to the new wire format).
- ✅ A torn tail in the WAL does not prevent the constructor from computing `next_lsn_` — same torn-tail handling as `ReadAllLogRecords`.

---

## What we'll have learned after this slice

- The role of LSN as the logical clock that ties every other piece of the recovery story together.
- The core WAL ordering invariant (allocation under the same lock as the write), and why violating it breaks correctness even though it "looks fine."
- A real use of pass-by-value + move semantics for an owning parameter.
- Why a small wire-format change (one `i64`) is a useful place to think about versioning, endianness, and round-trip symmetry, even when we choose to do none of those things now.
- The "scan-on-startup vs sidecar" question is the same question that comes up for every piece of database metadata; this is our first encounter with it.

---

## Implementation notes (post-build, 2026-05-28)

Two things worth recording against the design above:

- **`database.cpp` needed no change.** Its single call site already passes a
  *temporary* `LogRecord(...)`, which is a prvalue — it binds straight to the
  new by-value parameter and is constructed in place, no `std::move` required.
  The plan's "callers move the record in" matters for *named* lvalues; a
  temporary already moves. The discarded return value is fine — `Database`
  doesn't consume the LSN until the `page_lsn` slice.
- **`ScanLogLocked` template lives in the `.cpp`, not the header.** Legal only
  because both instantiations (constructor's max-LSN visitor, `ReadAllLogRecords`'s
  push-back visitor) are in `log_manager.cpp`. The visitor receives
  `visit(std::move(record))`; a `const LogRecord&` lambda binds the rvalue for
  read-only inspection, a by-value lambda move-constructs it for keeping.
- **Build gotcha (not LSN-specific):** a pre-existing `build/` dir pinned to
  Homebrew LLVM caused a `std::__1::__hash_memory` link error against gtest.
  `build.sh`'s `CC/CXX` defaults can't override a cached compiler — `rm -rf build`
  first to force a reconfigure with Apple clang.

## What's next

Once this slice lands:

1. **`page_lsn` on `Page`** — small slice. Add the field, default to `INVALID_LSN`, update under the page latch when a log record is applied to the page.
2. **Write-ahead invariant in `BufferPoolManager`** — the big one. Buffer pool needs a back-reference to the `LogManager` (or a new "flush WAL up to X" interface), and `FindVictimFrame` / `FlushPage` must call it before allowing a dirty page to leave memory. This will require us to add `flushed_lsn_` tracking inside `LogManager` and a `FlushUpTo(lsn_t)` method.

Those two together close out the original Phase 2 bullet "wiring WAL to the write path" — at which point we revisit `log_manager.md` and tick the boxes.
