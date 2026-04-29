#include "columnar_db/wal/log_record.h"
#include <vector>   // For std::vector
#include <cstring>  // For std::memcpy

namespace db {

uint32_t LogRecord::GetSize() const {
    // size_of(size) + size_of(type) + size_of(name_len) + name_len + size_of(tuple_len) + tuple_data
    return sizeof(uint32_t) +                                       // This record's total size
           sizeof(LogRecordType) +                                  // type_
           sizeof(uint32_t) + table_name_.length() +                // table_name_
           sizeof(uint32_t) + tuple_.size() * sizeof(int64_t);    // tuple_
}

void LogRecord::Serialize(char* buffer) const {
    char* p = buffer;

    // 1. Write total size
    uint32_t size = GetSize();
    std::memcpy(p, &size, sizeof(uint32_t));
    p += sizeof(uint32_t);

    // 2. Write type
    std::memcpy(p, &type_, sizeof(LogRecordType));
    p += sizeof(LogRecordType);

    // 3. Write table name (length-prefixed)
    uint32_t table_name_len = table_name_.length();
    std::memcpy(p, &table_name_len, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::memcpy(p, table_name_.c_str(), table_name_len);
    p += table_name_len;

    // 4. Write tuple (length-prefixed)
    uint32_t tuple_len = tuple_.size();
    std::memcpy(p, &tuple_len, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::memcpy(p, tuple_.data(), tuple_len * sizeof(int64_t));
}

uint32_t LogRecord::Deserialize(const char* buffer, LogRecord& out_record) {
    const char* p = buffer;

    // 1. Read total size
    uint32_t size;
    std::memcpy(&size, p, sizeof(uint32_t));
    p += sizeof(uint32_t);

    // 2. Read type
    LogRecordType type;
    std::memcpy(&type, p, sizeof(LogRecordType));
    p += sizeof(LogRecordType);

    // 3. Read table name
    uint32_t table_name_len;
    std::memcpy(&table_name_len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::string table_name(p, table_name_len);
    p += table_name_len;

    // 4. Read tuple
    uint32_t tuple_len;
    std::memcpy(&tuple_len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::vector<int64_t> tuple(tuple_len);
    std::memcpy(tuple.data(), p, tuple_len * sizeof(int64_t));
    
    out_record = LogRecord(type, table_name, tuple);
    return size;
}

} // namespace db