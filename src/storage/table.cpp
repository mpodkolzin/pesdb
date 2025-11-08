#include "columnar_db/storage/table.h"
#include <stdexcept>
#include <cassert>
#include <iostream>

namespace db {

Table::Table(const TableSchema* schema, BufferPoolManager* bpm) : schema_(schema), bpm_(bpm) {
    assert(schema != nullptr && "Table schema cannot be null.");
    if (schema->columns.empty()) {
        return;
    }

    // Initialize the last_page_ids cache and determine num_rows.
    // We assume all columns have the same number of rows.
    for (const auto& col : schema->columns) {
        last_page_ids_.push_back(col.first_page_id);
    }

    std::cout << "Table object created successfully." << std::endl;
    // Determine num_rows by scanning the first column until the end.
    // This is a one-time cost at initialization.
    page_id_t current_page_id = schema->columns[0].first_page_id;
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = bpm_->FetchPage(current_page_id);
        if (page == nullptr) throw std::runtime_error("Failed to fetch page to count rows.");
        page->r_latch();

        auto* data_page = reinterpret_cast<ColumnDataPage*>(page->data());
        num_rows_ += data_page->value_count_;
        last_page_ids_[0] = current_page_id; // Update last page for first col
        current_page_id = data_page->next_page_id_;

        page->r_unlatch();
        bpm_->UnpinPage(page->page_id(), false);
    }
    std::cout << "Number of rows: " << num_rows_ << std::endl;

    // Now, populate the last_page_ids for all other columns.
    for (size_t i = 1; i < schema->columns.size(); ++i) {
        current_page_id = schema->columns[i].first_page_id;
        while (current_page_id != INVALID_PAGE_ID) {
            Page* page = bpm_->FetchPage(current_page_id);
            page->r_latch();
            auto* data_page = reinterpret_cast<ColumnDataPage*>(page->data());
            last_page_ids_[i] = current_page_id;
            current_page_id = data_page->next_page_id_;
            page->r_unlatch();
            bpm_->UnpinPage(page->page_id(), false);
        }
    }
}

bool Table::InsertTuple(const std::vector<int64_t>& tuple) {
    if (tuple.size() != schema_->columns.size()) {
        return false; // Tuple doesn't match schema
    }

    for (size_t i = 0; i < schema_->columns.size(); ++i) {
        page_id_t current_pid = last_page_ids_[i];
        Page* page = bpm_->FetchPage(current_pid);
        page->w_latch();
        auto* data_page = reinterpret_cast<ColumnDataPage*>(page->data());

        if (data_page->value_count_ == ColumnDataPage::MAX_VALUES) {
            // The last page is full. Allocate a new one.
            page_id_t new_pid;
            Page* new_page_raw = bpm_->NewPage(&new_pid);
            if (new_page_raw == nullptr) {
                page->w_unlatch();
                bpm_->UnpinPage(current_pid, false);
                return false; // Buffer pool is full
            }

            // Link the old page to the new page
            data_page->next_page_id_ = new_pid;
            page->w_unlatch();
            bpm_->UnpinPage(current_pid, true); // Old page is now dirty

            // The new page is now our current page
            page = new_page_raw;
            page->w_latch();
            data_page = reinterpret_cast<ColumnDataPage*>(page->data());
            last_page_ids_[i] = new_pid;
        }

        // Insert the value into the page
        data_page->values_[data_page->value_count_] = tuple[i];
        data_page->value_count_++;

        page->w_unlatch();
        bpm_->UnpinPage(page->page_id(), true); // Page is dirty
    }

    num_rows_++;
    return true;
}

Table::Iterator Table::begin() {
    return Iterator(this, 0);
}

Table::Iterator Table::end() {
    return Iterator(this, num_rows_);
}

// --- Iterator Implementation ---

std::vector<int64_t> Table::Iterator::operator*() const {
    std::vector<int64_t> tuple;
    tuple.reserve(table_->schema_->columns.size());

    for (size_t i = 0; i < table_->schema_->columns.size(); ++i) {
        page_id_t current_pid = table_->schema_->columns[i].first_page_id;
        uint64_t remaining_rows = row_id_;

        // Follow the page chain to find the correct page
        while (true) {
            Page* page = table_->bpm_->FetchPage(current_pid);
            page->r_latch();
            auto* data_page = reinterpret_cast<ColumnDataPage*>(page->data());

            if (remaining_rows < data_page->value_count_) {
                // The value is on this page
                tuple.push_back(data_page->values_[remaining_rows]);
                page->r_unlatch();
                table_->bpm_->UnpinPage(current_pid, false);
                break;
            }

            remaining_rows -= data_page->value_count_;
            current_pid = data_page->next_page_id_;
            page->r_unlatch();
            table_->bpm_->UnpinPage(page->page_id(), false);
        }
    }
    return tuple;
}

Table::Iterator& Table::Iterator::operator++() {
    row_id_++;
    return *this;
}

} // namespace db