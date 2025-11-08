#include "columnar_db/storage/buffer_pool_manager.h"
#include <stdexcept>

namespace db {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), pages_(pool_size) {
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.push_back(i);
    }
}

BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // 1. Search for page in the buffer pool page table.
    if (page_table_.count(page_id)) {
        frame_id_t frame_id = page_table_[page_id];
        pages_[frame_id].pin_count_++;
        update_replacer(frame_id);
        return &pages_[frame_id];
    }

    // 2. If not found, find a replacement frame (from free list or by evicting).
    frame_id_t frame_id;
    if (!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
    } else {
        if (!find_victim_frame(&frame_id)) {
            return nullptr; // No page can be evicted.
        }
    }

    // 3. Update page metadata and read data from disk.
    page_table_[page_id] = frame_id;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].reset_memory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].data());

    // 4. Add the new page to the replacer.
    replacer_.push_front(frame_id);

    return &pages_[frame_id];
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // 1. Find a replacement frame.
    frame_id_t frame_id;
    if (!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
    } else {
        if (!find_victim_frame(&frame_id)) {
            return nullptr;
        }
    }

    // 2. Allocate a new page ID on disk.
    *page_id = disk_manager_->AllocatePage();

    // 3. Set up the new page's metadata.
    page_table_[*page_id] = frame_id;
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = true; // New page is always dirty.
    pages_[frame_id].reset_memory();

    // 4. Add the new page to the replacer.
    replacer_.push_front(frame_id);

    return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);

    if (!page_table_.count(page_id)) {
        return false;
    }

    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ <= 0) {
        return false; // Cannot unpin a page with pin_count <= 0.
    }

    pages_[frame_id].pin_count_--;
    if (is_dirty) {
        pages_[frame_id].is_dirty_ = true;
    }
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    if (!page_table_.count(page_id)) {
        return false;
    }

    frame_id_t frame_id = page_table_[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].data());
    pages_[frame_id].is_dirty_ = false;
    return true;
}

void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> lock(latch_);
    for (auto const& [page_id, frame_id] : page_table_) {
        if (pages_[frame_id].is_dirty_) {
            disk_manager_->WritePage(page_id, pages_[frame_id].data());
            pages_[frame_id].is_dirty_ = false;
        }
    }
}

bool BufferPoolManager::find_victim_frame(frame_id_t* frame_id) {
    // Iterate from the back of the replacer (LRU end).
    for (auto it = replacer_.rbegin(); it != replacer_.rend(); ++it) {
        frame_id_t current_frame_id = *it;
        if (pages_[current_frame_id].pin_count_ == 0) {
            // Found a victim.
            *frame_id = current_frame_id;
            
            // If victim is dirty, write it back to disk.
            if (pages_[current_frame_id].is_dirty_) {
                disk_manager_->WritePage(pages_[current_frame_id].page_id(), pages_[current_frame_id].data());
            }

            // Remove from page table and replacer.
            page_table_.erase(pages_[current_frame_id].page_id());
            replacer_.erase(std::next(it).base()); // Erase using forward iterator
            
            return true;
        }
    }
    return false; // No victim found.
}

void BufferPoolManager::update_replacer(frame_id_t frame_id) {
    // Move the accessed frame to the front of the LRU list.
    replacer_.remove(frame_id);
    replacer_.push_front(frame_id);
}

} // namespace db