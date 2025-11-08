#pragma once

namespace hsql { class SQLStatement; }
namespace db { class InMemoryTable; }

namespace db {

// Executes parsed SQL statements against a given table.
class QueryExecutor {
public:
    // The executor is tied to a specific table.
    explicit QueryExecutor(InMemoryTable& table);

    // Main entry point for executing a statement.
    void execute(const hsql::SQLStatement* statement);

private:
    void execute_select(const hsql::SQLStatement* statement);

    // A reference to the table we are querying.
    InMemoryTable& _table;
};

} // namespace db