#pragma once

namespace db {

class Database;

/**
 * @class RecoveryManager
 * @brief Replays the WAL into a `Database` on startup.
 *
 * **Slice 1 strategy: replay-from-scratch.**
 * Wipes the database's in-memory state, replays every record in the log,
 * then truncates the log. Idempotent because the slate is wiped first —
 * no LSN comparison needed. Replacing this with selective LSN-based replay
 * is Slice 2's job, once `page_lsn` exists.
 *
 * **Contract:**
 * - Must be called *before* any writes on a freshly-constructed `Database`.
 * - On return, the database's in-memory state matches the union of all
 *   `INSERT_TUPLE` records that were durably appended to the WAL.
 * - The WAL file is truncated to zero bytes on success (so the next run
 *   starts from a clean log).
 */
class RecoveryManager {
 public:
  static void Recover(Database& db);
};

}  // namespace db
