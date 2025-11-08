#include "columnar_db/storage/table_heap.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace db {

TableHeap::TableHeap(std::string table_name, BufferPoolManager* bpm)
    : table_name_(std::move(table_name)), buffer_pool_manager_(bpm) {
    // In a real system, we'd load metadata like num_rows from a catalog file.
    // For now, we assume it's a new table or we will populate it manually.
}

std::string TableHeap::get_column_file_name(int col_idx) const {
    return table_name_ + ".col" + std::to_string(col_idx) + ".dat";
}

TableHeap::Iterator TableHeap::begin() {
    return Iterator(this, 0);
}

TableHeap::Iterator TableHeap::end() {
    return Iterator(this, num_rows_);
}

// --- Iterator Implementation ---

TableHeap::Iterator::Iterator(TableHeap* table, int64_t row_idx)
    : table_(table), current_row_idx_(row_idx) {}

std::pair<int64_t, int64_t> TableHeap::Iterator::operator*() {
    constexpr int64_t TUPLES_PER_PAGE = PAGE_SIZE / sizeof(int64_t);

    // Column 0
    page_id_t page_id0 = current_row_idx_ / TUPLES_PER_PAGE;
    int64_t offset0 = current_row_idx_ % TUPLES_PER_PAGE;
    Page* page0 = table_->buffer_pool_manager_->fetch_page(table_->get_column_file_name(0), page_id0);
    int64_t val1 = reinterpret_cast<int64_t*>(page0->get_data())[offset0];
    table_->buffer_pool_manager_->unpin_page(page_id0, false);

    // Column 1
    page_id_t page_id1 = current_row_idx_ / TUPLES_PER_PAGE;
    int64_t offset1 = current_row_idx_ % TUPLES_PER_PAGE;
    Page* page1 = table_->buffer_pool_manager_->fetch_page(table_->get_column_file_name(1), page_id1);
    int64_t val2 = reinterpret_cast<int64_t*>(page1->get_data())[offset1];
    table_->buffer_pool_manager_->unpin_page(page_id1, false);

    return {val1, val2};
}

TableHeap::Iterator& TableHeap::Iterator::operator++() {
    current_row_idx_++;
    return *this;
}

bool TableHeap::Iterator::operator!=(const Iterator& other) const {
    return current_row_idx_ != other.current_row_idx_;
}


void create_table_from_csv(const std::string& table_name, const std::string& csv_path, BufferPoolManager* bpm) {
    std::ifstream file(csv_path);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open CSV file: " + csv_path);
    }

    std::vector<std::vector<int64_t>> columns(2);
    
    // Skip header
    std::string line;
    std::getline(file, line);

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string cell;
        // id,name,age -> we read id and age
        std::getline(ss, cell, ',');
        columns[0].push_back(std::stoll(cell)); // id
        std::getline(ss, cell, ','); // skip name
        std::getline(ss, cell, ',');
        columns[1].push_back(std::stoll(cell)); // age
    }

    // Now write the columns to disk page by page
    constexpr int64_t TUPLES_PER_PAGE = PAGE_SIZE / sizeof(int64_t);
    for (int col_idx = 0; col_idx < 2; ++col_idx) {
        std::string file_name = table_name + ".col" + std::to_string(col_idx) + ".dat";
        for (size_t row = 0; row < columns[col_idx].size(); ++row) {
            page_id_t page_id = row / TUPLES_PER_PAGE;
            int64_t offset = row % TUPLES_PER_PAGE;

            Page* page = bpm->fetch_page(file_name, page_id);
            reinterpret_cast<int64_t*>(page->get_data())[offset] = columns[col_idx][row];
            bpm->unpin_page(page_id, true); // Mark as dirty
        }
    }
    
    std::cout << "Created table " << table_name << " with " << columns[0].size() << " rows." << std::endl;
}

} // namespace db