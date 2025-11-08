#pragma once

#include "columnar_db/storage/buffer_pool_manager.h"
#include "columnar_db/storage/catalog.h"
#include <vector>

namespace db {

/**
 * @struct ColumnDataPage
 * @brief Represents the memory layout of a data page for a single column.
 *
 * This struct is memcpy'd to/from the raw data of a Page object.
 */
struct ColumnDataPage {
    // Header
    page_id_t next_page_id_{INVALID_PAGE_ID};
    uint32_t value_count_{0};

    // The rest of the page is data. We calculate how many values can fit.
    static constexpr uint32_t MAX_VALUES = (PAGE_SIZE - sizeof(page_id_t) - sizeof(uint32_t)) / sizeof(int64_t);

    // Data area
    int64_t values_[MAX_VALUES];
};

/**
 * @class Table
 * @brief Manages all data for a single table, providing insert and scan capabilities.
 */
class Table {
public:
    Table(const TableSchema* schema, BufferPoolManager* bpm);

    // Inserts a new tuple into the table. Returns true on success.
    bool InsertTuple(const std::vector<int64_t>& tuple);

    // Forward declaration of the iterator
    class Iterator;

    Iterator begin();
    Iterator end();

    uint64_t GetNumRows() const { return num_rows_; }

private:
    friend class Iterator; // Allow iterator to access private members

    const TableSchema* schema_;
    BufferPoolManager* bpm_;

    // Total number of rows in the table.
    uint64_t num_rows_ = 0;

    // A cache of the last page_id for each column's segment to avoid
    // traversing the linked list on every insert.
    std::vector<page_id_t> last_page_ids_;
};

/**
 * @class Table::Iterator
 * @brief An iterator for scanning tuples in the table.
 */
class Table::Iterator {
public:
    // Dereference operator to get the current tuple.
    std::vector<int64_t> operator*() const;

    // Prefix increment operator.
    Iterator& operator++();

    // Comparison operator.
    bool operator!=(const Iterator& other) const { return row_id_ != other.row_id_; }

private:
    friend class Table; // Allow Table to construct the iterator
    Iterator(Table* table, uint64_t row_id) : table_(table), row_id_(row_id) {}

    Table* table_;
    uint64_t row_id_;
};

} // namespace db