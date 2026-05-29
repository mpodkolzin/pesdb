#pragma once

#include "columnar_db/wal/log_record.h"
#include <string>
#include <fstream>
#include <mutex>

namespace db {

/**
 * @class LogManager
 * @brief Append-only Write-Ahead Log over a single file.
 *
 * Phase 1: logical records, one fsync-equivalent flush per append, coarse mutex.
 *
 * **Durability caveat (Phase 1):**
 * AppendLogRecord calls std::fstream::flush(), which pushes data from the C++
 * stream buffer into the OS page cache. It does NOT call fsync(2), so a
 * kernel-level crash (power loss, OS panic) can lose recently-appended records
 * even though Append has returned. This is acceptable for a learning project;
 * Phase 3 will switch to a raw fd + fsync for real crash safety.
 *
 * **Thread safety:** `latch_` serializes Append, ReadAllLogRecords, and ClearLog.
 */
class LogManager {
public:
    explicit LogManager(const std::string& wal_file);
    ~LogManager();

    // Delete copy/move (owns a file handle).
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;
    LogManager(LogManager&&) = delete;
    LogManager& operator=(LogManager&&) = delete;

    // Allocates the next LSN, stamps it into `record`, serializes, appends to
    // the WAL file, and flushes the C++ stream buffer to the OS. Returns the
    // LSN that was assigned. See class doc for the durability caveat.
    //
    // Takes `record` by value: callers move a freshly-built record in
    // (`AppendLogRecord(std::move(rec))`) so the string/vector are stolen, not
    // copied, and the function can stamp the LSN without a const_cast.
    lsn_t AppendLogRecord(LogRecord record);

    // Reads every fully-written record from the start of the file.
    // A torn tail (partial size header or partial body, e.g. from a crash
    // mid-append) is treated as end-of-log: the partial record is silently
    // skipped and only the records before it are returned.
    std::vector<LogRecord> ReadAllLogRecords();

    // Truncates the WAL file to zero bytes. The handle remains usable for
    // subsequent appends. Typically called after a successful recovery replay.
    void ClearLog();

private:
    // Walks the file from byte 0, decoding each fully-written record (a torn
    // tail is treated as end-of-log, same rules as ReadAllLogRecords) and
    // invoking visit(LogRecord). Restores the put-pointer to end-of-file.
    // Caller must hold latch_ (hence the *Locked suffix).
    //
    // Defined in the .cpp: it's a template, but every instantiation lives in
    // log_manager.cpp, so the definition doesn't need to be in this header.
    template <typename Visit>
    void ScanLogLocked(Visit visit);

    std::string wal_file_name_;
    std::fstream wal_file_;
    std::mutex latch_;

    // Next LSN to hand out. Starts at 1 for a fresh log; the constructor scans
    // an existing WAL and sets this to max-LSN-on-disk + 1. Guarded by latch_.
    lsn_t next_lsn_{1};
};

} // namespace db