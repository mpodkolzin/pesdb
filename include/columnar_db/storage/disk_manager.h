#pragma once

#include "columnar_db/common/config.h"
#include <fstream>
#include <string>
#include <mutex>

namespace db {

class DiskManager {
public:
    explicit DiskManager(const std::string& db_file);
    ~DiskManager();

    void ReadPage(page_id_t page_id, char* page_data);
    void WritePage(page_id_t page_id, const char* page_data);
    page_id_t AllocatePage();

private:
    // Helper function to extend the file and zero out the new page.
    void allocate_and_zero_out_page(page_id_t page_id);

    std::string file_name_;
    std::fstream file_stream_;
    page_id_t next_page_id_ = 0;
    std::mutex latch_;
};

} // namespace db