#include "columnar_db/storage/catalog.h"
#include "columnar_db/storage/page.h"
#include "columnar_db/storage/table.h"
#include <cstring>
#include <iostream>

namespace db {


constexpr page_id_t CATALOG_PAGE_ID = 0;
constexpr uint32_t DB_MAGIC_NUMBER = 0xDEADBEEF;

Catalog::Catalog(BufferPoolManager* buffer_pool_manager, bool is_new_db) : bpm_(buffer_pool_manager) {
    if (is_new_db) {
        std::cout << "Initializing new database file." << std::endl;
        PersistToDisk();
    } else {
        LoadFromDisk();
    }
}

void Catalog::LoadFromDisk() {
    Page* page = bpm_->FetchPage(CATALOG_PAGE_ID);
    if (page == nullptr) throw std::runtime_error("Failed to fetch catalog page.");
    page->r_latch();

    char* data = page->data();
    uint32_t magic_number;
    std::memcpy(&magic_number, data, sizeof(uint32_t));
    std::cout << "Magic number: " << magic_number << std::endl;
    std::cout << "DB_MAGIC_NUMBER: " << DB_MAGIC_NUMBER << std::endl;

    if (magic_number != DB_MAGIC_NUMBER) {
        page->r_unlatch();
        bpm_->UnpinPage(CATALOG_PAGE_ID, false);
        throw std::runtime_error("Database file is corrupted or not a valid DB file.");
    }

    int offset = sizeof(uint32_t);
    int table_count;
    std::memcpy(&table_count, data + offset, sizeof(int));
    offset += sizeof(int);

    for (int i = 0; i < table_count; ++i) {
        TableSchema schema;
        std::memcpy(schema.name, data + offset, sizeof(schema.name));
        offset += sizeof(schema.name);

        int col_count;
        std::memcpy(&col_count, data + offset, sizeof(int));
        offset += sizeof(int);

        for (int j = 0; j < col_count; ++j) {
            Column col;
            std::memcpy(&col, data + offset, sizeof(Column));
            offset += sizeof(Column);
            schema.columns.push_back(col);
        }
        schemas_[schema.name] = schema;
    }

    page->r_unlatch();
    bpm_->UnpinPage(CATALOG_PAGE_ID, false);
}

void Catalog::PersistToDisk() {
    Page* page = bpm_->FetchPage(CATALOG_PAGE_ID);
    if (page == nullptr) throw std::runtime_error("Failed to fetch catalog page for persisting.");
    page->w_latch();
    
    char* data = page->data();
    std::memset(data, 0, PAGE_SIZE);

    int offset = 0;
    std::memcpy(data + offset, &DB_MAGIC_NUMBER, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    std::cout << "Offset: " << data + offset  << std::endl;

    int table_count = schemas_.size();
    std::memcpy(data + offset, &table_count, sizeof(int));
    offset += sizeof(int);

    for (const auto& [name, schema] : schemas_) {
        std::memcpy(data + offset, schema.name, sizeof(schema.name));
        offset += sizeof(schema.name);

        int col_count = schema.columns.size();
        std::memcpy(data + offset, &col_count, sizeof(int));
        offset += sizeof(int);

        for (const auto& col : schema.columns) {
            std::memcpy(data + offset, &col, sizeof(Column));
            offset += sizeof(Column);
        }
    }

    page->w_unlatch();
    bpm_->UnpinPage(CATALOG_PAGE_ID, true);
    bpm_->FlushPage(CATALOG_PAGE_ID);
}


bool Catalog::CreateTable(TableSchema& schema) {
    if (schemas_.count(schema.name)) {
        return false;
    }

    for (auto& col : schema.columns) {
        page_id_t first_page_id;
        Page* first_page = bpm_->NewPage(&first_page_id);
        if (first_page == nullptr) {
            // Not enough space in the buffer pool.
            return false;
        }

        // --- THE FIX: Initialize the new page as an empty ColumnDataPage ---
        first_page->w_latch(); // Lock for writing
        auto* data_page = reinterpret_cast<ColumnDataPage*>(first_page->data());
        
        // This is the most important line: it marks the end of the segment.
        data_page->next_page_id_ = INVALID_PAGE_ID;
        data_page->value_count_ = 0;
        
        first_page->w_unlatch();
        // --- END FIX ---

        col.first_page_id = first_page_id;
        
        // The page is dirty because we initialized it, so the second param is true.
        bpm_->UnpinPage(first_page_id, true);
    }

    schemas_[schema.name] = schema;
    PersistToDisk();
    return true;
}

const TableSchema* Catalog::GetTableSchema(const std::string& table_name) {
    auto it = schemas_.find(table_name);
    if (it != schemas_.end()) {
        return &it->second;
    }
    return nullptr;
}

} 
