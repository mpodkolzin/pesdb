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

    // Serializes `record`, appends it to the WAL file, and flushes the C++
    // stream buffer to the OS. See class doc for the durability caveat.
    void AppendLogRecord(const LogRecord& record);

    // Reads every fully-written record from the start of the file.
    // A torn tail (partial size header or partial body, e.g. from a crash
    // mid-append) is treated as end-of-log: the partial record is silently
    // skipped and only the records before it are returned.
    std::vector<LogRecord> ReadAllLogRecords();

    // Truncates the WAL file to zero bytes. The handle remains usable for
    // subsequent appends. Typically called after a successful recovery replay.
    void ClearLog();

private:
    std::string wal_file_name_;
    std::fstream wal_file_;
    std::mutex latch_;
};

} // namespace db