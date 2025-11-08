#include "columnar_db/engine/query_executor.h"
#include "columnar_db/storage/table_heap.h"
#include "SQLParser.h"
#include <iostream>

namespace db {

// This now needs a reference to the table heap
void execute_query(const hsql::SQLStatement* statement, TableHeap& table) {
    if (statement->type() != hsql::kStmtSelect) {
        std::cout << "Sorry, I can only handle SELECT statements for now." << std::endl;
        return;
    }

    std::cout << "id\tage" << std::endl;
    std::cout << "----------" << std::endl;
    
    // Full table scan using the iterator
    int rows_scanned = 0;
    for (auto tuple : table) {
        // tuple is std::pair<int64_t, int64_t>
        std::cout << tuple.first << "\t" << tuple.second << std::endl;
        rows_scanned++;
    }
    std::cout << "--------------------" << std::endl;
    std::cout << "Scanned " << rows_scanned << " rows." << std::endl;
}

} // namespace db```
