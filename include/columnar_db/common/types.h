#pragma once

#include <string>
#include <variant>

// A simple type definition for our table cells
using TableCell = std::variant<long, std::string>;

namespace db {
// Enum for column data types
enum class DataType {
    INVALID,
    BIGINT, // For now, we'll only handle 8-byte integers
    // We will add VARCHAR later, as variable-length data is more complex
};
}
