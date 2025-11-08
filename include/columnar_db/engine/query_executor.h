#pragma once

// Forward declare Hyrise types instead of including the full header
namespace hsql { class SQLStatement; }
namespace db { class InMemoryTable; }

namespace db {
// A very basic query executor
void execute_query(const hsql::SQLStatement* statement, InMemoryTable& table);
} // namespace db