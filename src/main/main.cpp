#include "columnar_db/storage/buffer_pool_manager.h"
#include "columnar_db/storage/catalog.h"
#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/storage/table.h"

#include <iostream>
#include <memory>
#include <sys/stat.h>
#include <vector>

// ... print_schema function is unchanged ...
void print_schema(const db::TableSchema& schema) {
    std::cout << "  Table: " << schema.name << std::endl;
    for (const auto& col : schema.columns) {
        std::cout << "    - Column: " << col.name;
        std::cout << ", Type: ";
        switch (col.type) {
            case db::DataType::BIGINT:
                std::cout << "BIGINT";
                break;
            default:
                std::cout << "INVALID";
                break;
        }
        std::cout << ", First Page ID: " << col.first_page_id << std::endl;
    }
}


int main() {
    const std::string db_file = "mydb.db";
    const std::string table_name = "users";

    //std::remove(db_file.c_str());

    struct stat stat_buf;
    bool is_new_db = (stat(db_file.c_str(), &stat_buf) != 0 || stat_buf.st_size == 0);

    auto disk_manager = std::make_unique<db::DiskManager>(db_file);
    auto buffer_pool_manager = std::make_unique<db::BufferPoolManager>(db::BUFFER_POOL_SIZE, disk_manager.get());
    auto catalog = std::make_unique<db::Catalog>(buffer_pool_manager.get(), is_new_db);

    const db::TableSchema* schema = catalog->GetTableSchema(table_name);
    if (schema == nullptr) {
        std::cout << "Table '" << table_name << "' not found. Creating it..." << std::endl;
        db::TableSchema new_schema;
        strncpy(new_schema.name, table_name.c_str(), sizeof(new_schema.name) - 1);
        new_schema.name[sizeof(new_schema.name) - 1] = '\0';
        
        db::Column id_col;
        strncpy(id_col.name, "id", sizeof(id_col.name) - 1);
        id_col.name[sizeof(id_col.name) - 1] = '\0';
        id_col.type = db::DataType::BIGINT;
        
        db::Column age_col;
        strncpy(age_col.name, "age", sizeof(age_col.name) - 1);
        age_col.name[sizeof(age_col.name) - 1] = '\0';
        age_col.type = db::DataType::BIGINT;
        
        new_schema.columns.push_back(id_col);
        new_schema.columns.push_back(age_col);
        
        if (!catalog->CreateTable(new_schema)) {
            std::cerr << "Failed to create table '" << table_name << "'. Exiting." << std::endl;
            return 1;
        }
        std::cout << "Table '" << table_name << "' created successfully." << std::endl;
    }

    std::cout << "\n--- Interacting with Table Data ---" << std::endl;

    schema = catalog->GetTableSchema(table_name);
    std::cout << "Received schema: "<< std::endl;
    auto users_table = std::make_unique<db::Table>(schema, buffer_pool_manager.get());
    
    std::cout << "Inserting data..." << std::endl;
    users_table->InsertTuple({101, 30});
    users_table->InsertTuple({102, 25});
    users_table->InsertTuple({103, 42});

    std::cout << "Total rows in table: " << users_table->GetNumRows() << std::endl;
    std::cout << "\nScanning table:" << std::endl;
    std::cout << "id\tage" << std::endl;
    std::cout << "------" << std::endl;
    for (const auto& tuple : *users_table) {
        for (size_t i = 0; i < tuple.size(); ++i) {
            std::cout << tuple[i] << (i == tuple.size() - 1 ? "" : "\t");
        }
        std::cout << std::endl;
    }

    std::cout << "\n--- Shutting down ---" << std::endl;
    return 0;
}