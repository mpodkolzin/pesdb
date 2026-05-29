#include "columnar_db/wal/log_manager.h"
#include <algorithm> // For std::max
#include <iostream>
#include <vector>
#include <cstring> // For std::memcpy

namespace db {

// --- LogManager Implementation ---

LogManager::LogManager(const std::string& wal_file) : wal_file_name_(wal_file) {
    // Open in 'append' mode. 'binary' is critical.
    wal_file_.open(wal_file_name_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
    if (!wal_file_.is_open()) {
        // If it fails, try to create it
        wal_file_.open(wal_file_name_, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!wal_file_.is_open()) {
            throw std::runtime_error("Cannot open or create WAL file: " + wal_file_name_);
        }
        wal_file_.close();
        // Re-open in the correct mode
        wal_file_.open(wal_file_name_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);
    }

    // Resume the LSN counter past anything already on disk. We do NOT take
    // latch_ here: no other thread can hold a reference to *this until the
    // constructor returns, so the access is data-race-free by construction.
    // (Don't "fix" this by adding a lock_guard — it would be harmless but
    // pointless.)
    lsn_t max_lsn = INVALID_LSN;
    ScanLogLocked([&](const LogRecord& record) {
        max_lsn = std::max(max_lsn, record.GetLSN());
    });
    next_lsn_ = max_lsn + 1;
}

LogManager::~LogManager() {
    if (wal_file_.is_open()) {
        wal_file_.close();
    }
}

lsn_t LogManager::AppendLogRecord(LogRecord record) {
    std::lock_guard<std::mutex> lock(latch_);

    // LSN allocation MUST happen under latch_, in the same critical section as
    // the file write. Otherwise two concurrent appends could allocate LSNs in
    // one order and reach disk in the other — recovery would see records out
    // of order. This is the core WAL ordering invariant.
    const lsn_t lsn = next_lsn_++;
    record.SetLSN(lsn);

    uint32_t size = record.GetSize();
    std::vector<char> buffer(size);
    record.Serialize(buffer.data());

    // Write the data
    wal_file_.write(buffer.data(), size);
    if (wal_file_.fail()) {
        throw std::runtime_error("Failed to write to WAL file.");
    }

    // Push the C++ stream buffer into the OS page cache. Survives a process
    // crash; does NOT survive a kernel/power crash (no fsync). See class doc.
    wal_file_.flush();

    return lsn;
}

template <typename Visit>
void LogManager::ScanLogLocked(Visit visit) {
    // Reset error/eof flags from any prior read, rewind to start.
    wal_file_.clear();
    wal_file_.seekg(0);

    while (true) {
        // Step 1: read the [uint32 total_size] header.
        std::vector<char> buffer(sizeof(uint32_t));
        wal_file_.read(buffer.data(), sizeof(uint32_t));
        if (wal_file_.gcount() == 0) {
            break;  // Clean EOF: no more records.
        }
        if (wal_file_.gcount() != static_cast<std::streamsize>(sizeof(uint32_t))) {
            // Torn tail: partial size header. Treat as end-of-log.
            break;
        }

        uint32_t record_size;
        std::memcpy(&record_size, buffer.data(), sizeof(uint32_t));
        if (record_size < sizeof(uint32_t)) {
            // Corrupt size header (smaller than the header itself). Bail out.
            break;
        }

        // Step 2: grow the buffer to the full record size and read the body.
        buffer.resize(record_size);
        const std::streamsize body_bytes =
            static_cast<std::streamsize>(record_size - sizeof(uint32_t));
        wal_file_.read(buffer.data() + sizeof(uint32_t), body_bytes);
        if (wal_file_.gcount() != body_bytes) {
            // Torn tail: header was complete but body was truncated.
            // (e.g. crashed mid-append before flush hit the device)
            break;
        }

        LogRecord record(LogRecordType::INVALID, "", {});
        LogRecord::Deserialize(buffer.data(), record);
        visit(std::move(record));
    }

    // Restore append-at-end position for subsequent writes.
    wal_file_.clear();
    wal_file_.seekp(0, std::ios::end);
}

std::vector<LogRecord> LogManager::ReadAllLogRecords() {
    std::lock_guard<std::mutex> lock(latch_);
    std::vector<LogRecord> records;
    ScanLogLocked([&](LogRecord record) { records.push_back(std::move(record)); });
    return records;
}

void LogManager::ClearLog() {
    std::lock_guard<std::mutex> lock(latch_);
    
    // Truncate the file by closing and re-opening with 'trunc'
    wal_file_.close();
    wal_file_.open(wal_file_name_, std::ios::out | std::ios::binary | std::ios::trunc);
    wal_file_.close();
    
    // Re-open in the correct append mode
    wal_file_.open(wal_file_name_, std::ios::in | std::ios::out | std::ios::app | std::ios::binary);

    // The log is empty again, so numbering restarts at 1. (ClearLog runs after
    // a successful recovery replay: the data files already reflect everything
    // the old LSNs named, so those values carry no meaning any more.)
    next_lsn_ = 1;
}

} // namespace db