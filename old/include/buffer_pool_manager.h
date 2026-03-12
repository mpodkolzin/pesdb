#pragma once

#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/storage/page.h"
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace db {


class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    BufferPoolManager(const BufferPoolManager &) = delete;
    BufferPoolManager &operator=(const BufferPoolManager &) = delete;

    // Fetches a page from the buffer pool, reading from disk if necessary.
    Page* FetchPage(page_id_t page_id);

    // Creates a new page in the buffer pool and allocates it on disk.
    Page* NewPage(page_id_t* page_id);

    // Unpins a page, making it a candidate for eviction.
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    // Flushes a specific page to disk, regardless of its pin count.
    bool FlushPage(page_id_t page_id);

    // Flushes all dirty pages to disk.
    void FlushAllPages();

private:
    // Tries to find a victim page to evict and returns its frame_id.
    bool find_victim_frame(frame_id_t* frame_id);
    
    // Updates the LRU replacer when a page is accessed.
    void update_replacer(frame_id_t frame_id);

    const size_t pool_size_;
    DiskManager* const disk_manager_;

    // The array of Page objects that make up the buffer pool frames.
    std::vector<Page> pages_;

    // Mapping from page_id to the frame_id where it is stored.
    std::unordered_map<page_id_t, frame_id_t> page_table_;

    // A list of frame_ids that are currently free.
    std::list<frame_id_t> free_list_;

    // A list of frame_ids of occupied pages, for the LRU replacer.
    // The front is the most recently used, the back is the least recently used.
    std::list<frame_id_t> replacer_;

    // A mutex to protect the internal data structures of the BPM.
    std::mutex latch_;
};

} // namespace db