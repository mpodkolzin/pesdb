#pragma once

#include <cstdint>
#include <cstddef>

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
 * INVALID_LSN: Sentinel for "this record has no LSN yet"
 *
 * A LogRecord constructed in memory carries INVALID_LSN until LogManager
 * stamps a real value during append. Real LSNs start at 1, mirroring the
 * INVALID_PAGE_ID sentinel pattern used elsewhere.
 */
constexpr lsn_t INVALID_LSN = 0;

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
// Data Types
// ============================================================================

/**
 * DataType: Supported column data types
 *
 * Underlying type is uint8_t for stable wire format (serialization)
 * Each type has a fixed encoding for on-disk storage
 */
enum class DataType : uint8_t {
  INVALID = 0,
  INT64   = 1,   // 8-byte signed integer
  FLOAT64 = 2,   // 8-byte IEEE 754 double precision
  BOOL    = 3,   // 1-byte boolean (0x00=false, 0x01=true)
  STRING  = 4    // Variable-length string (length-prefixed, no null terminator)
};

/**
 * Get fixed size for a type (0 for variable-size types like STRING)
 */
inline size_t GetTypeSize(DataType type) {
  switch (type) {
    case DataType::INT64:   return 8;
    case DataType::FLOAT64: return 8;
    case DataType::BOOL:    return 1;
    case DataType::STRING:  return 0;  // Variable size
    case DataType::INVALID: return 0;
  }
  return 0;
}

/**
 * Check if type is fixed-size
 */
inline bool IsFixedSize(DataType type) {
  return GetTypeSize(type) > 0;
}

}  // namespace db
