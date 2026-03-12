#pragma once

#include "columnar_db/common/config.h"
#include <shared_mutex>
#include <cstring> // For std::memset

namespace db {

class BufferPoolManager; // Forward declaration

/**
 * @class Page
 * @brief Represents a single page in the buffer pool.
 *
 * The Page class is a container for a fixed-size block of memory (PAGE_SIZE)
 * that is read from or written to the disk. It also holds metadata about the
 * page's state, such as its ID, pin count, and dirty flag. It is managed
 * exclusively by the BufferPoolManager.
 */
class Page {
    // The BufferPoolManager needs to modify the private members of Page.
    friend class BufferPoolManager;

public:
    /**
     * @brief Default constructor. Initializes page memory to zeros.
     */
    Page() {
        reset_memory();
    }

    /**
     * @brief Default destructor.
     */
    ~Page() = default;

    // Delete copy constructor and copy assignment operator to prevent copying.
    Page(const Page &) = delete;
    Page &operator=(const Page &) = delete;

    /**
     * @return A pointer to the raw data block of the page.
     */
    char* data() { return data_; }

    /**
     * @return The unique identifier of this page.
     */
    page_id_t page_id() const { return page_id_; }

    /**
     * @brief Acquires a read (shared) lock on the page.
     * Prevents other threads from acquiring a write lock.
     */
    void r_latch() { latch_.lock_shared(); }

    /**
     * @brief Releases a read (shared) lock on the page.
     */
    void r_unlatch() { latch_.unlock_shared(); }

    /**
     * @brief Acquires a write (exclusive) lock on the page.
     * Prevents any other thread from acquiring a read or write lock.
     */
    void w_latch() { latch_.lock(); }

    /**
     * @brief Releases a write (exclusive) lock on the page.
     */
    void w_unlatch() { latch_.unlock(); }

private:
    /**
     * @brief Zeros out the page's data memory.
     */
    void reset_memory() {
        std::memset(data_, 0, PAGE_SIZE);
    }

    // The raw data of the page.
    char data_[PAGE_SIZE]{};

    // The page's unique identifier.
    page_id_t page_id_ = INVALID_PAGE_ID;

    // The number of threads that have "pinned" this page.
    // A page cannot be evicted if its pin count is greater than 0.
    int pin_count_ = 0;

    // True if the page has been modified since being read from disk.
    bool is_dirty_ = false;

    // A read-write latch to protect the page's contents from concurrent access.
    std::shared_mutex latch_;
};

} // namespace db