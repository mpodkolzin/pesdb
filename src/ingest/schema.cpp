#include "columnar_db/ingest/schema.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <sstream>
#include <stdexcept>

namespace db {

// ============================================================================
// Constructor
// ============================================================================

Schema::Schema(std::string table_name, std::vector<Column> columns)
    : table_name_(std::move(table_name)), columns_(std::move(columns)) {}

// ============================================================================
// Parse CSV Header
// ============================================================================

Schema Schema::ParseCSVHeader(
    const std::string& table_name,
    const std::string& header_line
) {
    std::vector<Column> columns;

    // Split header by comma
    std::stringstream ss(header_line);
    std::string column_spec;

    while (std::getline(ss, column_spec, ',')) {
        // Trim whitespace
        column_spec.erase(0, column_spec.find_first_not_of(" \t"));
        column_spec.erase(column_spec.find_last_not_of(" \t") + 1);

        if (column_spec.empty()) {
            throw std::runtime_error("Empty column specification in CSV header");
        }

        // Split by colon to get name:type
        size_t colon_pos = column_spec.find(':');
        if (colon_pos == std::string::npos) {
            throw std::runtime_error(
                "Invalid column specification '" + column_spec +
                "' - expected format 'name:TYPE'"
            );
        }

        std::string col_name = column_spec.substr(0, colon_pos);
        std::string type_str = column_spec.substr(colon_pos + 1);

        // Trim column name and type string
        col_name.erase(0, col_name.find_first_not_of(" \t"));
        col_name.erase(col_name.find_last_not_of(" \t") + 1);
        type_str.erase(0, type_str.find_first_not_of(" \t"));
        type_str.erase(type_str.find_last_not_of(" \t") + 1);

        if (col_name.empty()) {
            throw std::runtime_error("Empty column name in '" + column_spec + "'");
        }

        if (type_str.empty()) {
            throw std::runtime_error("Empty type in '" + column_spec + "'");
        }

        // Parse type
        DataType type = ParseTypeName(type_str);
        if (type == DataType::INVALID) {
            throw std::runtime_error(
                "Unknown type '" + type_str + "' in column '" + col_name + "'"
            );
        }

        columns.emplace_back(col_name, type);
    }

    if (columns.empty()) {
        throw std::runtime_error("CSV header has no columns");
    }

    return Schema(table_name, std::move(columns));
}

// ============================================================================
// Serialization
// ============================================================================

size_t Schema::GetSerializedSize() const {
    size_t size = 0;
    size += sizeof(uint32_t);  // Magic
    size += sizeof(uint32_t);  // Version
    size += sizeof(uint32_t);  // Table name length
    size += table_name_.size();
    size += sizeof(uint32_t);  // Column count

    for (const auto& col : columns_) {
        size += sizeof(uint32_t);  // Column name length
        size += col.name.size();
        size += sizeof(uint8_t);   // Type
    }

    return size;
}

size_t Schema::Serialize(char* buffer) const {
    char* ptr = buffer;

    // Write magic number
    uint32_t magic = SCHEMA_MAGIC;
    std::memcpy(ptr, &magic, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Write version
    uint32_t version = SCHEMA_VERSION;
    std::memcpy(ptr, &version, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Write table name
    uint32_t table_name_len = static_cast<uint32_t>(table_name_.size());
    std::memcpy(ptr, &table_name_len, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    std::memcpy(ptr, table_name_.data(), table_name_len);
    ptr += table_name_len;

    // Write column count
    uint32_t col_count = static_cast<uint32_t>(columns_.size());
    std::memcpy(ptr, &col_count, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Write each column
    for (const auto& col : columns_) {
        // Column name
        uint32_t col_name_len = static_cast<uint32_t>(col.name.size());
        std::memcpy(ptr, &col_name_len, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        std::memcpy(ptr, col.name.data(), col_name_len);
        ptr += col_name_len;

        // Column type
        uint8_t type_byte = static_cast<uint8_t>(col.type);
        std::memcpy(ptr, &type_byte, sizeof(uint8_t));
        ptr += sizeof(uint8_t);
    }

    return static_cast<size_t>(ptr - buffer);
}

size_t Schema::Deserialize(const char* buffer, Schema& out) {
    const char* ptr = buffer;

    // Read and verify magic number
    uint32_t magic;
    std::memcpy(&magic, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    if (magic != SCHEMA_MAGIC) {
        throw std::runtime_error(
            "Invalid schema magic number - not a valid schema page"
        );
    }

    // Read version (for future compatibility)
    uint32_t version;
    std::memcpy(&version, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    if (version != SCHEMA_VERSION) {
        throw std::runtime_error(
            "Unsupported schema version: " + std::to_string(version)
        );
    }

    // Read table name
    uint32_t table_name_len;
    std::memcpy(&table_name_len, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    std::string table_name(ptr, table_name_len);
    ptr += table_name_len;

    // Read column count
    uint32_t col_count;
    std::memcpy(&col_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // Read columns
    std::vector<Column> columns;
    columns.reserve(col_count);

    for (uint32_t i = 0; i < col_count; ++i) {
        // Column name
        uint32_t col_name_len;
        std::memcpy(&col_name_len, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        std::string col_name(ptr, col_name_len);
        ptr += col_name_len;

        // Column type
        uint8_t type_byte;
        std::memcpy(&type_byte, ptr, sizeof(uint8_t));
        ptr += sizeof(uint8_t);

        DataType type = static_cast<DataType>(type_byte);

        columns.emplace_back(col_name, type);
    }

    out = Schema(std::move(table_name), std::move(columns));

    return static_cast<size_t>(ptr - buffer);
}

// ============================================================================
// Utilities
// ============================================================================

int Schema::FindColumn(const std::string& name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].name == name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

DataType Schema::ParseTypeName(const std::string& type_name) {
    // Convert to uppercase for case-insensitive comparison
    std::string upper = type_name;
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return std::toupper(c); });

    if (upper == "INT64") return DataType::INT64;
    if (upper == "FLOAT64") return DataType::FLOAT64;
    if (upper == "BOOL" || upper == "BOOLEAN") return DataType::BOOL;
    if (upper == "STRING" || upper == "VARCHAR" || upper == "TEXT") return DataType::STRING;

    return DataType::INVALID;
}

std::string Schema::TypeToString(DataType type) {
    switch (type) {
        case DataType::INT64:   return "INT64";
        case DataType::FLOAT64: return "FLOAT64";
        case DataType::BOOL:    return "BOOL";
        case DataType::STRING:  return "STRING";
        case DataType::INVALID: return "INVALID";
    }
    return "UNKNOWN";
}

}  // namespace db
