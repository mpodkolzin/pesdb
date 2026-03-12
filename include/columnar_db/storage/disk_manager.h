#pragma once

#include <fstream>
#include <string>

#include "columnar_db/common/config.h"
#include "columnar_db/common/types.h"

namespace db {

/**
 * @class DiskManager
 * @brief Provides page-based abstraction over raw file I/O.
 *
 * The DiskManager is the lowest layer in the storage hierarchy.
 * It translates page IDs to file offsets and performs synchronous reads/writes.
 *
 * Key Concepts:
 * - Fixed 4KB pages (PAGE_SIZE)
 * - Append-only allocation (no free list)
 * - Simple linear file layout: offset = page_id * PAGE_SIZE
 *
 * No buffering happens here - that's the BufferPoolManager's job (Step 3).
 *
 * Example Usage:
 *   auto dm = std::make_unique<DiskManager>("mydb.db");
 *   page_id_t pid = dm->AllocatePage();
 *   char buffer[PAGE_SIZE] = {...};
 *   dm->WritePage(pid, buffer);
 *   dm->ReadPage(pid, buffer);
 */
class DiskManager {
 public:
  /**
   * Opens or creates a database file.
   * If file doesn't exist, creates it. If exists, opens for read/write.
   *
   * @param db_file Path to database file (e.g., "mydb.db")
   * @throws std::runtime_error if file cannot be opened/created
   */
  explicit DiskManager(const std::string& db_file);

  /**
   * Destructor: closes file handle.
   * RAII ensures file is closed even if exception occurs.
   */
  ~DiskManager();

  // Delete copy/move constructors (owns file handle, shouldn't be copied)
  DiskManager(const DiskManager&) = delete;
  DiskManager& operator=(const DiskManager&) = delete;
  DiskManager(DiskManager&&) = delete;
  DiskManager& operator=(DiskManager&&) = delete;

  /**
   * Reads a page from disk into provided buffer.
   *
   * @param page_id The page to read (must be < num_pages_)
   * @param data Output buffer (must be at least PAGE_SIZE bytes)
   * @throws std::out_of_range if page_id is invalid
   * @throws std::runtime_error if read fails
   */
  void ReadPage(page_id_t page_id, char* data);

  /**
   * Writes a page from buffer to disk.
   *
   * @param page_id The page to write (can be existing page or extend file)
   * @param data Input buffer (must be at least PAGE_SIZE bytes)
   * @throws std::out_of_range if page_id is invalid
   * @throws std::runtime_error if write fails
   */
  void WritePage(page_id_t page_id, const char* data);

  /**
   * Allocates a new page at end of file.
   * Returns the new page_id, increments num_pages_.
   *
   * EAGER ALLOCATION: Page is zero-initialized immediately.
   * This ensures allocated pages are always readable.
   *
   * @return page_id of the newly allocated page
   */
  page_id_t AllocatePage();

  /**
   * Returns total number of pages in the file.
   * This equals the number of pages on disk.
   */
  size_t GetNumPages() const { return num_pages_; }

  /**
   * Returns the database file path.
   */
  const std::string& GetFileName() const { return file_name_; }

 private:
  std::string file_name_;     ///< Path to database file
  std::fstream file_stream_;  ///< File handle (C++ streams for simplicity)
  size_t num_pages_;          ///< Total pages in file
};

}  // namespace db
