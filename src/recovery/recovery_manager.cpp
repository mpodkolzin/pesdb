#include "columnar_db/recovery/recovery_manager.h"

#include "columnar_db/engine/database.h"
#include "columnar_db/wal/log_manager.h"
#include "columnar_db/wal/log_record.h"

#include <stdexcept>

namespace db {

void RecoveryManager::Recover(Database& db) {
  // 1. Wipe in-memory state. Recovery must be re-runnable, so we don't
  //    accumulate tuples on top of whatever happened to be in memory.
  db.ClearAll();

  // 2. Replay every fully-written log record.
  std::vector<LogRecord> records = db.log_manager().ReadAllLogRecords();
  for (const LogRecord& record : records) {
    switch (record.GetType()) {
      case LogRecordType::INSERT_TUPLE:
        db.ApplyInsertFromLog(record.GetTableName(), record.GetTuple());
        break;
      case LogRecordType::INVALID:
        // Phase 1 only writes INSERT_TUPLE. INVALID in the log means a
        // corrupt or unknown record type — treat as fatal so we don't
        // silently lose data.
        throw std::runtime_error(
            "RecoveryManager: encountered INVALID log record type");
    }
  }

  // 3. Drop the log. Replay was successful, so the records are now
  //    redundant with in-memory state. Slice 2's incremental recovery
  //    will revisit this — for now, brute force.
  db.log_manager().ClearLog();
}

}  // namespace db
