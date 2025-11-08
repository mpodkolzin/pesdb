#pragma once

#include "columnar_db/storage/buffer_pool_manager.h"
#include <string>
#include <vector>

namespace db {

class TableHeap {
public:
    TableHeap(std::string table_name, BufferPoolManager* bpm);

    // For simplicity, we'll store tuples of two int64_t values
    void insert_tuple(int64_t val1, int64_t val2);

    // An iterator to scan the table
    class Iterator {
    public:
        Iterator(TableHeap* table, int64_t row_idx);
        std::pair<int64_t, int64_t> operator*();
        Iterator& operator++();
        bool operator!=(const Iterator& other) const;
    private:
        TableHeap* table_;
        int64_t current_row_idx_;
    };

    Iterator begin();
    Iterator end();

    size_t get_num_rows() const { return num_rows_; }

private:
    friend class Iterator; // Allow iterator to access private members
    
    std::string table_name_;
    BufferPoolManager* buffer_pool_manager_;
    size_t num_rows_ = 0; // We'll store this in memory for now. A real DB would persist it.

    std::string get_column_file_name(int col_idx) const;
};

// Function to create a table from a CSV
void create_table_from_csv(const std::string& table_name, const std::string& csv_path, BufferPoolManager* bpm);

} // namespace db