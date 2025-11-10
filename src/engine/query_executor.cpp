#include "columnar_db/engine/query_executor.h"
#include "columnar_db/storage/table.h"
#include "SQLParser.h"
#include "sql/SelectStatement.h"
#include "sql/InsertStatement.h"
#include "sql/Expr.h"
#include <iostream>
#include <functional> // For std::function
#include "columnar_db/wal/log_manager.h"

namespace db {

QueryExecutor::QueryExecutor(Catalog* catalog, BufferPoolManager* bpm, LogManager* log_manager)
    : catalog_(catalog), bpm_(bpm), log_manager_(log_manager) {}

void QueryExecutor::Execute(const hsql::SQLStatement* statement) {
    switch (statement->type()) {
        case hsql::kStmtSelect:
            ExecuteSelect(statement);
            break;
        case hsql::kStmtInsert:
            ExecuteInsert(statement);
            break;
        default:
            std::cerr << "Error: Only SELECT and INSERT statements are supported." << std::endl;
            break;
    }
}

void QueryExecutor::ExecuteSelect(const hsql::SQLStatement* statement) {
    const auto* select_stmt = static_cast<const hsql::SelectStatement*>(statement);
    
    const char* table_name = select_stmt->fromTable->getName();
    if (table_name == nullptr) {
        std::cerr << "Error: SELECT must be from a table." << std::endl;
        return;
    }

    const TableSchema* schema = catalog_->GetTableSchema(table_name);
    if (schema == nullptr) {
        std::cerr << "Error: Table '" << table_name << "' not found." << std::endl;
        return;
    }

    // --- NEW PREDICATE LOGIC ---

    // A predicate is a function that takes a tuple and returns true if
    // it matches the WHERE clause, or false otherwise.
    // By default, it always returns true (matching all rows).
    std::function<bool(const std::vector<int64_t>&)> predicate = 
        // FIX for -Wunused-parameter: remove the variable name
        [](const std::vector<int64_t>&) { return true; };

    if (select_stmt->whereClause != nullptr) {
        // We have a WHERE clause. Let's try to parse it.
        // We only support "column_name = integer_value" for now.
        const hsql::Expr* where = select_stmt->whereClause;
        
        // FIX: Replaced 'expr1' with 'expr'
        if (where->type == hsql::kExprOperator && where->opType == hsql::kOpEquals &&
            where->expr->type == hsql::kExprColumnRef &&
            where->expr2->type == hsql::kExprLiteralInt) {
            
            // FIX: Replaced 'expr1' with 'expr'
            const char* col_name = where->expr->name;
            int64_t value = where->expr2->ival;

            // Find the column index from the schema
            int col_idx = -1;
            for (size_t i = 0; i < schema->columns.size(); ++i) {
                if (std::strcmp(schema->columns[i].name, col_name) == 0) {
                    col_idx = i;
                    break;
                }
            }

            if (col_idx == -1) {
                std::cerr << "Error: Column '" << col_name << "' not found in table '" << table_name << "'." << std::endl;
                return;
            }

            // Success! Update our predicate to perform the filter.
            predicate = [col_idx, value](const std::vector<int64_t>& tuple) {
                return tuple[col_idx] == value;
            };

        } else {
            std::cerr << "Error: Unsupported WHERE clause. Only 'column_name = integer_value' is supported." << std::endl;
            return;
        }
    }
    // --- END NEW PREDICATE LOGIC ---


    Table table(schema, bpm_);

    // Print headers
    for (const auto& col : schema->columns) {
        std::cout << col.name << "\t";
    }
    std::cout << std::endl;
    for (size_t i = 0; i < schema->columns.size(); ++i) {
        std::cout << "------\t";
    }
    std::cout << std::endl;

    // Iterate and print tuples
    int rows_scanned = 0;
    int rows_matched = 0;
    for (const auto& tuple : table) {
        rows_scanned++;
        // Apply the predicate filter
        if (predicate(tuple)) {
            for (size_t i = 0; i < tuple.size(); ++i) {
                std::cout << tuple[i] << (i == tuple.size() - 1 ? "" : "\t");
            }
            std::cout << std::endl;
            rows_matched++;
        }
    }

    std::cout << "--------------------" << std::endl;
    std::cout << "Matched " << rows_matched << " rows (scanned " << rows_scanned << " rows)." << std::endl;
}

// ... ExecuteInsert is unchanged ...
void QueryExecutor::ExecuteInsert(const hsql::SQLStatement* statement) {
    const auto* insert_stmt = static_cast<const hsql::InsertStatement*>(statement);

    // Get table name
    const char* table_name = insert_stmt->tableName;
    
    // Get table schema from catalog
    const TableSchema* schema = catalog_->GetTableSchema(table_name);
    if (schema == nullptr) {
        std::cerr << "Error: Table '" << table_name << "' not found." << std::endl;
        return;
    }

    // Create a Table instance
    Table table(schema, bpm_);

    // Validate value list
    if (insert_stmt->values == nullptr) {
        std::cerr << "Error: INSERT statement must have a VALUES clause." << std::endl;
        return;
    }
    
    if (insert_stmt->values->size() != schema->columns.size()) {
        std::cerr << "Error: Column count doesn't match value count." << std::endl;
        return;
    }
    
    // Parse values from the AST (Abstract Syntax Tree)
    std::vector<int64_t> tuple;
    for (const auto* expr : *insert_stmt->values) {
        if (expr->type != hsql::kExprLiteralInt) {
            std::cerr << "Error: Only integer literals are supported in INSERT statements." << std::endl;
            return;
        }
        tuple.push_back(expr->ival);
    }

    LogRecord log_record(LogRecordType::INSERT_TUPLE, table_name, tuple);
    try {
        log_manager_->AppendLogRecord(log_record);
    } catch (const std::exception& e) {
        std::cerr << "Error: Failed to append log record: " << e.what() << std::endl;
        return;
    }


    // Insert the tuple
    if (table.InsertTuple(tuple)) {
        std::cout << "Inserted 1 row." << std::endl;
    } else {
        std::cerr << "Error: Failed to insert tuple." << std::endl;
    }
}

} // namespace db