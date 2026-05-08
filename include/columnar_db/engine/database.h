#pragma once

#include "columnar_db/wal/log_manager.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace db {

class RecoveryManager;

/**
 * @class Database
 * @brief Slice 1 write API: in-memory tables backed by a WAL.
 *
 * `Insert` follows the append-before-mutate discipline: the log record is
 * appended (and flushed to the OS) before the in-memory state changes. If
 * the process crashes between the log write and the in-memory mutation the
 * record is still on disk, and `RecoveryManager::Recover` will re-apply it
 * on the next start.
 *
 * **Slice 1 caveats:**
 * - Storage is an `std::map`, not a buffer pool / page heap. This slice is
 *   about WAL semantics, not persistence — the only thing that survives a
 *   restart is the log. Real storage lands in Slice 2.
 * - `CreateTable` is *not* logged (no `CREATE_TABLE` record type yet). On
 *   restart, recovery auto-creates a table the first time it sees an
 *   `INSERT_TUPLE` referencing it.
 * - No `lsn_t`, no `page_lsn`, no write-ahead invariant in the buffer pool.
 *   All deferred to Slice 2 — see `doc/design/wal/log_manager.md` § Deferred
 *   Work.
 */
class Database {
 public:
  explicit Database(const std::string& wal_file);
  ~Database() = default;

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;
  Database(Database&&) = delete;
  Database& operator=(Database&&) = delete;

  // Creates an empty table. No-op if the table already exists.
  // Not logged in Slice 1 — see header note.
  void CreateTable(const std::string& table_name);

  // Appends an INSERT_TUPLE log record, flushes the WAL, then mutates the
  // in-memory table. Throws std::out_of_range if the table doesn't exist.
  void Insert(const std::string& table_name,
              const std::vector<int64_t>& tuple);

  // Returns all tuples for a table in insertion order.
  // Throws std::out_of_range if the table doesn't exist.
  const std::vector<std::vector<int64_t>>& Scan(
      const std::string& table_name) const;

 private:
  // RecoveryManager is the only caller allowed to mutate state without
  // going through the WAL.
  friend class RecoveryManager;

  // Wipes table contents (keeps table names with empty row sets).
  // Recovery-only.
  void ClearAll();

  // Applies an insert without logging. Auto-creates the table if missing.
  // Recovery-only.
  void ApplyInsertFromLog(const std::string& table_name,
                          const std::vector<int64_t>& tuple);

  LogManager& log_manager() { return log_manager_; }

  LogManager log_manager_;
  std::map<std::string, std::vector<std::vector<int64_t>>> tables_;
};

}  // namespace db
