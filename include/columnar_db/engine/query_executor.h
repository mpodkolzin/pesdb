#pragma once

#include "columnar_db/storage/buffer_pool_manager.h"
#include "columnar_db/storage/catalog.h"

namespace hsql { struct SQLStatement; }

namespace db {

/**
 * @class QueryExecutor
 * @brief Executes parsed SQL statements against the persistent storage.
 */
class QueryExecutor {
public:
    /**
     * @brief Constructs a new QueryExecutor.
     * @param catalog The database catalog to find table schemas.
     * @param bpm The buffer pool manager to pass to Table objects.
     */
    QueryExecutor(Catalog* catalog, BufferPoolManager* bpm);

    /**
     * @brief Main entry point for executing a parsed statement.
     */
    void Execute(const hsql::SQLStatement* statement);

private:
    /**
     * @brief Executes a SELECT statement.
     */
    void ExecuteSelect(const hsql::SQLStatement* statement);

    /**
     * @brief Executes an INSERT statement.
     */
    void ExecuteInsert(const hsql::SQLStatement* statement);

    Catalog* catalog_;
    BufferPoolManager* bpm_;
};

} // namespace db