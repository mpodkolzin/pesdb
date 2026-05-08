#include "columnar_db/engine/database.h"

#include "columnar_db/wal/log_record.h"

#include <stdexcept>

namespace db {

Database::Database(const std::string& wal_file) : log_manager_(wal_file) {}

void Database::CreateTable(const std::string& table_name) {
  // operator[] inserts an empty vector if the key is missing; idempotent.
  tables_[table_name];
}

void Database::Insert(const std::string& table_name,
                      const std::vector<int64_t>& tuple) {
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    throw std::out_of_range("Database::Insert: unknown table '" +
                            table_name + "'");
  }

  // Append-before-mutate. If AppendLogRecord throws (write/flush failure),
  // the in-memory state stays consistent. If we crash between the log
  // append and the push_back below, the next startup will replay the
  // record and bring memory back in sync — that's the whole point of WAL.
  log_manager_.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE,
                                         table_name, tuple));
  it->second.push_back(tuple);
}

const std::vector<std::vector<int64_t>>& Database::Scan(
    const std::string& table_name) const {
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    throw std::out_of_range("Database::Scan: unknown table '" +
                            table_name + "'");
  }
  return it->second;
}

void Database::ClearAll() {
  for (auto& [_, rows] : tables_) {
    rows.clear();
  }
}

void Database::ApplyInsertFromLog(const std::string& table_name,
                                  const std::vector<int64_t>& tuple) {
  // Auto-create on demand. The log is authoritative during replay — no
  // CREATE_TABLE records exist yet (Phase 3), so we infer existence from
  // the first INSERT_TUPLE we see for a table.
  tables_[table_name].push_back(tuple);
}

}  // namespace db
