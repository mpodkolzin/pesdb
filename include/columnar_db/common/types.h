#pragma once

#include <cstdint>

namespace db {

// ============================================================================
// Storage Types (Phase 1)
// ============================================================================

/**
 * page_id_t: Unique identifier for a page in the database file
 *
 * Range: [0, 2^31-1] allows ~2 billion pages
 * With 4KB pages: max database size = 8TB
 */
using page_id_t = int32_t;

/**
 * frame_id_t: Index into the buffer pool's frame array
 *
 * Range: [0, buffer_pool_size-1]
 * Each frame holds one cached page
 */
using frame_id_t = int32_t;

// ============================================================================
// Transaction Types (Phase 4-5 - Future)
// ============================================================================

/**
 * lsn_t: Log Sequence Number for WAL
 *
 * Monotonically increasing identifier for log records
 * Range: [0, 2^63-1] (signed for easier comparison)
 */
using lsn_t = int64_t;

/**
 * txn_id_t: Transaction identifier
 *
 * Unique identifier for each transaction
 */
using txn_id_t = int32_t;

/**
 * timestamp_t: Logical timestamp for MVCC
 *
 * Monotonically increasing timestamp for snapshot isolation
 */
using timestamp_t = uint64_t;

// ============================================================================
// Data Types (Phase 2-3 - Future)
// ============================================================================

/**
 * DataType: Supported column data types
 *
 * Phase 1-2: Only BIGINT (fixed-width, simple)
 * Phase 3+: Add VARCHAR, DOUBLE, etc.
 */
enum class DataType {
  INVALID = 0,
  BIGINT = 1,   // 8-byte signed integer
  // DOUBLE = 2,   // 8-byte floating point (future)
  // VARCHAR = 3,  // Variable-length string (future)
};

}  // namespace db
