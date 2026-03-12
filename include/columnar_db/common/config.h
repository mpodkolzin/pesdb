#pragma once

#include <cstddef>
#include <cstdint>

namespace db {

// ============================================================================
// Page Configuration
// ============================================================================

/**
 * PAGE_SIZE: Fixed size of database pages (4KB)
 *
 * Why 4KB?
 * - Matches typical OS page size (efficient I/O, no partial page reads)
 * - Standard in many databases (PostgreSQL, MySQL InnoDB)
 * - Good balance: not too small (overhead), not too large (wasted space)
 *
 * Alternative: 8KB (more data per page, but more waste for small tables)
 */
constexpr size_t PAGE_SIZE = 4096;

/**
 * INVALID_PAGE_ID: Sentinel value for invalid/null page references
 */
constexpr int32_t INVALID_PAGE_ID = -1;

// ============================================================================
// Buffer Pool Configuration (Phase 1, Step 3)
// ============================================================================

/**
 * DEFAULT_BUFFER_POOL_SIZE: Number of pages to cache in memory
 *
 * For learning: small pool (128 pages = 512 KB) to observe eviction easily
 * For production: thousands of pages (e.g., 10,000 pages = 40 MB)
 */
constexpr size_t DEFAULT_BUFFER_POOL_SIZE = 128;

/**
 * INVALID_FRAME_ID: Sentinel value for invalid buffer pool frame
 */
constexpr int32_t INVALID_FRAME_ID = -1;

}  // namespace db