#include "columnar_db/wal/log_manager.h"
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
}

LogManager::~LogManager() {
    if (wal_file_.is_open()) {
        wal_file_.close();
    }
}

void LogManager::AppendLogRecord(const LogRecord& record) {
    std::lock_guard<std::mutex> lock(latch_);

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
}

std::vector<LogRecord> LogManager::ReadAllLogRecords() {
    std::lock_guard<std::mutex> lock(latch_);
    std::vector<LogRecord> records;

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
        records.push_back(std::move(record));
    }

    // Restore append-at-end position for subsequent writes.
    wal_file_.clear();
    wal_file_.seekp(0, std::ios::end);

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
}

} // namespace db