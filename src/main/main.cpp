#include "columnar_db/storage/buffer_pool_manager.h"
#include "columnar_db/storage/catalog.h"
#include "columnar_db/storage/disk_manager.h"
#include "columnar_db/engine/query_executor.h" // Include the new executor
#include "SQLParser.h" // Include the Hyrise SQL Parser

#include <iostream>
#include <memory>
#include <string> // For std::string and std::getline
#include <sys/stat.h>

int main() {
    const std::string db_file = "mydb.db";
    const std::string table_name = "users";

    // --- 1. Database Setup ---
    struct stat stat_buf;
    bool is_new_db = (stat(db_file.c_str(), &stat_buf) != 0 || stat_buf.st_size == 0);

    auto disk_manager = std::make_unique<db::DiskManager>(db_file);
    auto buffer_pool_manager = std::make_unique<db::BufferPoolManager>(db::BUFFER_POOL_SIZE, disk_manager.get());
    auto catalog = std::make_unique<db::Catalog>(buffer_pool_manager.get(), is_new_db);

    // --- 2. Create 'users' table if it doesn't exist (for convenience) ---
    // This is the same logic as before, just to ensure we have a table to query.
    // In a real DB, you'd send "CREATE TABLE ..." via the REPL.
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

    // --- 3. Instantiate the Query Executor ---
    auto query_executor = std::make_unique<db::QueryExecutor>(catalog.get(), buffer_pool_manager.get());

    // --- 4. Start the Read-Evaluate-Print Loop (REPL) ---
    std::string query;
    std::cout << "Welcome to pesdb. Type 'quit' to exit." << std::endl;

    while (true) {
        std::cout << "db > ";
        std::getline(std::cin, query);

        if (query == "quit") {
            break;
        }

        if (query.empty()) {
            continue;
        }

        // Parse the query
        hsql::SQLParserResult result;
        hsql::SQLParser::parseSQLString(query, &result);

        if (result.isValid()) {
            // Execute the query
            query_executor->Execute(result.getStatement(0));
        } else {
            std::cerr << "Error: Invalid SQL query." << std::endl;
            std::cerr << "  " << result.errorMsg() << " (L:" << result.errorLine() << ", C:" << result.errorColumn() << ")" << std::endl;
        }
        std::cout << std::endl;
    }

    std::cout << "\n--- Shutting down ---" << std::endl;
    // The BufferPoolManager destructor will automatically flush all dirty pages.
    return 0;
}