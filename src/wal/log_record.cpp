#include "columnar_db/wal/log_record.h"
#include <vector>   // For std::vector
#include <cstring>  // For std::memcpy

namespace db {

uint32_t LogRecord::GetSize() const {
    // size_of(size) + size_of(lsn) + size_of(type) + size_of(name_len) + name_len + size_of(tuple_len) + tuple_data
    return sizeof(uint32_t) +                                       // This record's total size
           sizeof(lsn_t) +                                          // lsn_
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

    // 2. Write LSN (immediately after size so a file scanner can read each
    //    record's LSN without parsing the rest of the record).
    std::memcpy(p, &lsn_, sizeof(lsn_t));
    p += sizeof(lsn_t);

    // 3. Write type
    std::memcpy(p, &type_, sizeof(LogRecordType));
    p += sizeof(LogRecordType);

    // 4. Write table name (length-prefixed)
    uint32_t table_name_len = table_name_.length();
    std::memcpy(p, &table_name_len, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::memcpy(p, table_name_.c_str(), table_name_len);
    p += table_name_len;

    // 5. Write tuple (length-prefixed)
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

    // 2. Read LSN
    lsn_t lsn;
    std::memcpy(&lsn, p, sizeof(lsn_t));
    p += sizeof(lsn_t);

    // 3. Read type
    LogRecordType type;
    std::memcpy(&type, p, sizeof(LogRecordType));
    p += sizeof(LogRecordType);

    // 4. Read table name
    uint32_t table_name_len;
    std::memcpy(&table_name_len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::string table_name(p, table_name_len);
    p += table_name_len;

    // 5. Read tuple
    uint32_t tuple_len;
    std::memcpy(&tuple_len, p, sizeof(uint32_t));
    p += sizeof(uint32_t);
    std::vector<int64_t> tuple(tuple_len);
    std::memcpy(tuple.data(), p, tuple_len * sizeof(int64_t));

    out_record = LogRecord(type, table_name, tuple);
    out_record.SetLSN(lsn);  // round-trip symmetry: Deserialize(Serialize(r)) == r
    return size;
}

} // namespace db