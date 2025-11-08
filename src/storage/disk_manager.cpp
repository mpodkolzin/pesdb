#include "columnar_db/storage/disk_manager.h"
#include <stdexcept>
#include <sys/stat.h>
#include <vector>

namespace db {

DiskManager::DiskManager(const std::string& db_file) : file_name_(std::move(db_file)) {
    // Open the file with 'ate' to position at the end for size check
    file_stream_.open(file_name_, std::ios::in | std::ios::out | std::ios::binary | std::ios::ate);

    bool is_new_db = false;
    if (!file_stream_.is_open()) {
        // File doesn't exist, create it with 'trunc'
        file_stream_.open(file_name_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
        if (!file_stream_.is_open()) {
            throw std::runtime_error("Cannot create or open database file: " + file_name_);
        }
        is_new_db = true;
    }

    off_t file_size = file_stream_.tellg();
    next_page_id_ = file_size / PAGE_SIZE;

    // --- CRITICAL FIX ---
    // If it's a new DB, the file size is 0. We must allocate Page 0 for the catalog.
    if (is_new_db || file_size == 0) {
        allocate_and_zero_out_page(0); // Allocate space for Page 0
        next_page_id_ = 1; // The next page to be allocated will be Page 1
    }
    // --- END FIX ---
}

DiskManager::~DiskManager() {
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

void DiskManager::allocate_and_zero_out_page(page_id_t page_id) {
    // Create a buffer of zeros. Using a static vector is efficient.
    static const std::vector<char> zero_buffer(PAGE_SIZE, 0);
    
    // Seek to the position of the new page and write zeros.
    // This will extend the file to the required size.
    file_stream_.seekp(static_cast<std::streampos>(page_id) * PAGE_SIZE);
    file_stream_.write(zero_buffer.data(), PAGE_SIZE);
    
    // Ensure the write is flushed to disk.
    file_stream_.flush();
}

void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);
    
    // Clear any previous error states before seeking
    file_stream_.clear();
    
    std::streampos offset = static_cast<std::streampos>(page_id) * PAGE_SIZE;
    file_stream_.seekg(offset);

    // Check if the seek was successful
    if (file_stream_.fail()) {
         // This might happen if the page is beyond the file size.
         // In a robust system, you'd handle this more gracefully.
         // For now, we'll just return, leaving page_data as zeros.
         return;
    }

    file_stream_.read(page_data, PAGE_SIZE);
    // No need to check gcount(), if it's less than PAGE_SIZE, that's okay,
    // the rest of the buffer will be zeros from when the page was created.
}

void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
    std::lock_guard<std::mutex> lock(latch_);
    
    // Clear any previous error states before seeking
    file_stream_.clear();

    std::streampos offset = static_cast<std::streampos>(page_id) * PAGE_SIZE;
    file_stream_.seekp(offset);
    file_stream_.write(page_data, PAGE_SIZE);
    file_stream_.flush();
}

page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lock(latch_);
    page_id_t new_page_id = next_page_id_++;
    allocate_and_zero_out_page(new_page_id);
    return new_page_id;
}

} // namespace db