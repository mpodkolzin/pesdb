#include "columnar_db/storage/disk_manager.h"

#include <cstring>
#include <stdexcept>

namespace db {

// ============================================================================
// Constructor: Open or Create Database File
// ============================================================================

DiskManager::DiskManager(const std::string& db_file)
    : file_name_(db_file), num_pages_(0) {

  // Try to open existing file
  file_stream_.open(file_name_,
                    std::ios::in | std::ios::out | std::ios::binary);

  if (!file_stream_.is_open()) {
    // File doesn't exist, create it
    file_stream_.open(file_name_,
                      std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);

    if (!file_stream_.is_open()) {
      throw std::runtime_error("Cannot create database file: " + file_name_);
    }
  }

  // Determine file size and calculate num_pages
  file_stream_.seekg(0, std::ios::end);
  std::streampos file_size = file_stream_.tellg();

  if (file_size < 0) {
    throw std::runtime_error("Failed to get file size: " + file_name_);
  }

  num_pages_ = static_cast<size_t>(file_size) / PAGE_SIZE;
}

// ============================================================================
// Destructor: Close File Handle
// ============================================================================

DiskManager::~DiskManager() {
  if (file_stream_.is_open()) {
    file_stream_.close();
  }
}

// ============================================================================
// ReadPage: Seek to Page and Read
// ============================================================================

void DiskManager::ReadPage(page_id_t page_id, char* data) {
  // Validate page_id (must be within existing pages)
  if (page_id < 0 || static_cast<size_t>(page_id) >= num_pages_) {
    throw std::out_of_range("ReadPage: Invalid page_id " +
                            std::to_string(page_id) +
                            " (num_pages=" + std::to_string(num_pages_) + ")");
  }

  // Calculate offset: THE KEY FORMULA!
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

  // Clear any error flags before seeking
  file_stream_.clear();

  // Seek to page position
  file_stream_.seekg(offset, std::ios::beg);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
  }

  // Read page data
  file_stream_.read(data, PAGE_SIZE);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to read page " + std::to_string(page_id));
  }

  // Verify we read full page
  std::streamsize bytes_read = file_stream_.gcount();
  if (bytes_read != PAGE_SIZE) {
    throw std::runtime_error("Partial read: expected " + std::to_string(PAGE_SIZE) +
                            " bytes, got " + std::to_string(bytes_read));
  }
}

// ============================================================================
// WritePage: Seek to Page and Write
// ============================================================================

void DiskManager::WritePage(page_id_t page_id, const char* data) {
  // Validate page_id (can write to existing page or extend file by one)
  if (page_id < 0 || static_cast<size_t>(page_id) > num_pages_) {
    throw std::out_of_range("WritePage: Invalid page_id " +
                            std::to_string(page_id) +
                            " (num_pages=" + std::to_string(num_pages_) + ")");
  }

  // Calculate offset
  size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;

  // Clear error flags
  file_stream_.clear();

  // Seek to page position
  file_stream_.seekp(offset, std::ios::beg);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to seek to page " + std::to_string(page_id));
  }

  // Write page data
  file_stream_.write(data, PAGE_SIZE);
  if (file_stream_.fail()) {
    throw std::runtime_error("Failed to write page " + std::to_string(page_id));
  }

  // Flush to ensure durability (data written to OS, not just buffered)
  file_stream_.flush();

  // Update num_pages if we extended the file
  if (static_cast<size_t>(page_id) >= num_pages_) {
    num_pages_ = page_id + 1;
  }
}

// ============================================================================
// AllocatePage: Reserve Next Page ID (EAGER ALLOCATION)
// ============================================================================

page_id_t DiskManager::AllocatePage() {
  // Next page ID is current count (append-only allocation)
  page_id_t new_page_id = static_cast<page_id_t>(num_pages_);

  // EAGER ALLOCATION: Zero-initialize page immediately
  // This ensures the allocated page is always readable
  char zeros[PAGE_SIZE];
  std::memset(zeros, 0, PAGE_SIZE);
  WritePage(new_page_id, zeros);  // This updates num_pages_

  return new_page_id;
}

}  // namespace db
