#pragma once

#include "columnar_db/common/types.h"
#include <vector>
#include <string>

namespace db {

// Underlying type pinned to uint8_t so the on-disk wire format is stable
// across compilers/platforms (default underlying type of `enum class` is
// implementation-defined width).
enum class LogRecordType : uint8_t {
    INVALID = 0,
    INSERT_TUPLE = 1
    // Phase 2 will add:
    // BEGIN_TXN, COMMIT_TXN, ABORT_TXN, UPDATE_TUPLE, DELETE_TUPLE, CREATE_TABLE
};

/**
 * @class LogRecord
 * @brief Represents a single operation written to the WAL.
 *
 * For simplicity, we are making this a "logical" log record.
 * It's not fixed-size; it's serialized to the WAL file.
 * We'll use a simple format:
 * [size_in_bytes] [LogRecordType] [table_name] [tuple_data]
 */
class LogRecord {
public:
    LogRecord(LogRecordType type, std::string table_name, std::vector<int64_t> tuple)
        : type_(type), table_name_(std::move(table_name)), tuple_(std::move(tuple)) {}

    // --- Serialization ---
    // Writes the record into `buffer`, which must be at least GetSize() bytes.
    // The first 4 bytes of the written data are the total record size.
    void Serialize(char* buffer) const;

    // Parses a record from `buffer`. The buffer must point to a complete
    // record beginning with its [uint32 total_size] header.
    // Returns the number of bytes consumed (== the size header value).
    static uint32_t Deserialize(const char* buffer, LogRecord& out_record);

    // Calculates the total size (including the size header)
    uint32_t GetSize() const;

    // --- Getters ---
    LogRecordType GetType() const { return type_; }
    const std::string& GetTableName() const { return table_name_; }
    const std::vector<int64_t>& GetTuple() const { return tuple_; }

private:
    LogRecordType type_{LogRecordType::INVALID};
    std::string table_name_;
    std::vector<int64_t> tuple_;
};

} // namespace db