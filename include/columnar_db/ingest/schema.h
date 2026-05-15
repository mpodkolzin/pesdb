#pragma once

#include "columnar_db/common/types.h"
#include <string>
#include <vector>

namespace db {

/**
 * @struct Column
 * @brief Represents a single column in a table schema
 *
 * A column has a name and a data type. Future extensions may include
 * constraints (NOT NULL, UNIQUE, etc.), default values, etc.
 */
struct Column {
    std::string name;
    DataType type;

    Column() : type(DataType::INVALID) {}
    Column(std::string n, DataType t) : name(std::move(n)), type(t) {}

    bool operator==(const Column& other) const {
        return name == other.name && type == other.type;
    }
};

/**
 * @class Schema
 * @brief Table schema - column names and types
 *
 * Represents the structure of a table. Can be:
 * - Parsed from CSV header line (e.g., "id:INT64,name:STRING")
 * - Serialized to bytes for storage on page 0
 * - Deserialized from bytes when reading page 0
 *
 * On-disk format (page 0):
 * [Magic: 4 bytes][Version: 4 bytes]
 * [Table Name Length: 4 bytes][Table Name: variable]
 * [Column Count: 4 bytes]
 * [Column 1: Name Length: 4 bytes][Name: variable][Type: 1 byte]
 * [Column 2: ...]
 * ...
 */
class Schema {
public:
    Schema() = default;
    Schema(std::string table_name, std::vector<Column> columns);

    // Parse schema from CSV header line
    // Format: "column_name:TYPE,column_name:TYPE,..."
    // Example: "id:INT64,name:STRING,score:FLOAT64"
    // Throws std::runtime_error on invalid format
    static Schema ParseCSVHeader(
        const std::string& table_name,
        const std::string& header_line
    );

    // Serialize schema to bytes (for writing to page 0)
    // Returns number of bytes written
    size_t Serialize(char* buffer) const;

    // Deserialize schema from bytes (reading from page 0)
    // Returns number of bytes consumed
    static size_t Deserialize(const char* buffer, Schema& out);

    // Calculate serialized size (to know how much buffer to allocate)
    size_t GetSerializedSize() const;

    // Accessors
    const std::string& GetTableName() const { return table_name_; }
    const std::vector<Column>& GetColumns() const { return columns_; }
    size_t GetColumnCount() const { return columns_.size(); }

    // Get column by index
    const Column& GetColumn(size_t idx) const { return columns_.at(idx); }

    // Find column index by name (returns -1 if not found)
    int FindColumn(const std::string& name) const;

    bool operator==(const Schema& other) const {
        return table_name_ == other.table_name_ &&
               columns_ == other.columns_;
    }

private:
    std::string table_name_;
    std::vector<Column> columns_;

    // Magic number to identify schema pages
    static constexpr uint32_t SCHEMA_MAGIC = 0x50455344;  // "PESD" in hex
    static constexpr uint32_t SCHEMA_VERSION = 1;

    // Helper: parse type name to DataType enum
    static DataType ParseTypeName(const std::string& type_name);

    // Helper: convert DataType enum to string
    static std::string TypeToString(DataType type);
};

}  // namespace db
