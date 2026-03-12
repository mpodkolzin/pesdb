#include "columnar_db/storage/buffer_pool_manager.h"
#include <stdexcept>
#include <vector> // Need this for the temp buffer

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
    // Use std::unique_lock to allow manually unlocking
    std::unique_lock<std::mutex> lock(latch_);

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

    // 3. We have a victim frame. Get its details *while holding the latch*.
    bool victim_is_dirty = pages_[frame_id].is_dirty_;
    page_id_t victim_page_id = pages_[frame_id].page_id();
    
    // Copy dirty data to a temp buffer *while holding the latch*.
    std::vector<char> temp_data;
    if (victim_is_dirty) {
        temp_data.assign(pages_[frame_id].data(), pages_[frame_id].data() + PAGE_SIZE);
    }

    // 4. Update page metadata for the NEW page.
    page_table_[page_id] = frame_id;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].reset_memory();
    replacer_.push_front(frame_id);

    // 5. RELEASE THE LATCH before doing any I/O.
    lock.unlock();

    // 6. Perform I/O *after* the latch is released.
    if (victim_is_dirty) {
        disk_manager_->WritePage(victim_page_id, temp_data.data());
    }
    if (!disk_manager_->ReadPage(page_id, pages_[frame_id].data())) {
        // I/O Error! The page is invalid (e.g., doesn't exist).
        // We must revert our changes and return nullptr.
        lock.lock(); // Re-acquire latch to revert state
        
        // Undo the changes we made in step 4
        page_table_.erase(page_id);
        replacer_.remove(frame_id); // It's at the front
        free_list_.push_front(frame_id); // Put the frame back on the free list
        
        // Reset the frame's metadata to be safe
        pages_[frame_id].page_id_ = INVALID_PAGE_ID;
        pages_[frame_id].pin_count_ = 0;
        
        lock.unlock();
        return nullptr; // <--- Signal failure to caller
    }

    return &pages_[frame_id];
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::unique_lock<std::mutex> lock(latch_);

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

    // 2. We have a victim frame. Get its details.
    bool victim_is_dirty = pages_[frame_id].is_dirty_;
    page_id_t victim_page_id = pages_[frame_id].page_id();
    std::vector<char> temp_data;
    if (victim_is_dirty) {
        temp_data.assign(pages_[frame_id].data(), pages_[frame_id].data() + PAGE_SIZE);
    }
    
    // 3. "Reserve" the frame by pinning it and resetting.
    // We can't add to page_table_ yet, as we don't know the new page_id.
    pages_[frame_id].pin_count_ = 1; 
    pages_[frame_id].reset_memory();
    
    // 4. RELEASE THE LATCH before doing I/O.
    lock.unlock();

    // 5. Perform I/O.
    if (victim_is_dirty) {
        disk_manager_->WritePage(victim_page_id, temp_data.data());
    }
    // This is also I/O:
    page_id_t new_page_id = disk_manager_->AllocatePage();
    *page_id = new_page_id;

    // 6. RE-ACQUIRE LATCH to update metadata safely.
    lock.lock();

    // 7. Update metadata for the new page.
    pages_[frame_id].page_id_ = new_page_id;
    pages_[frame_id].is_dirty_ = true; // New page is always dirty.
    // pin_count_ is already 1
    
    page_table_[new_page_id] = frame_id;
    replacer_.push_front(frame_id);

    return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    // ... (This function is unchanged)
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
    // ... (This function is unchanged, but note it also does I/O inside a lock!)
    // ... (This is OK for now, as it's not called during a fetch/new)
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
    // ... (This function is unchanged)
    std::lock_guard<std::mutex> lock(latch_);
    for (auto const& [page_id, frame_id] : page_table_) {
        if (pages_[frame_id].is_dirty_) {
            disk_manager_->WritePage(page_id, pages_[frame_id].data());
            pages_[frame_id].is_dirty_ = false;
        }
    }
}

bool BufferPoolManager::find_victim_frame(frame_id_t* frame_id) {
    // ... (Ensure the WritePage call we removed earlier is still gone)
    for (auto it = replacer_.rbegin(); it != replacer_.rend(); ++it) {
        frame_id_t current_frame_id = *it;
        if (pages_[current_frame_id].pin_count_ == 0) {
            // Found a victim.
            *frame_id = current_frame_id;
            
            // Remove from page table and replacer.
            page_table_.erase(pages_[current_frame_id].page_id());
            replacer_.erase(std::next(it).base()); // Erase using forward iterator
            
            return true;
        }
    }
    return false; // No victim found.
}

void BufferPoolManager::update_replacer(frame_id_t frame_id) {
    // ... (This function is unchanged)
    replacer_.remove(frame_id);
    replacer_.push_front(frame_id);
}

} // namespace db