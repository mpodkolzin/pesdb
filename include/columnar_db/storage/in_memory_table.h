#pragma once

#include <string>
#include <vector>
#include "columnar_db/common/types.h"

namespace db {

// A representation of an in-memory table
class InMemoryTable {
public:
    std::vector<std::string> column_names;
    std::vector<std::vector<TableCell>> data; // Data stored column by column

    void add_column(const std::string& name);
};

// Function to load data from a CSV file into our table
void load_csv(const std::string& filename, InMemoryTable& table);

} // namespace db