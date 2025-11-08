#include "columnar_db/storage/in_memory_table.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace db {

void InMemoryTable::add_column(const std::string& name) {
    column_names.push_back(name);
    data.emplace_back();
}

void load_csv(const std::string& filename, InMemoryTable& table) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        return;
    }

    std::string header_line;
    if (std::getline(file, header_line)) {
        std::stringstream ss(header_line);
        std::string column_name;
        while (std::getline(ss, column_name, ',')) {
            table.add_column(column_name);
        }
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string cell;
        int col_idx = 0;
        while (std::getline(ss, cell, ',')) {
            try {
                long val = std::stol(cell);
                table.data[col_idx].push_back(val);
            } catch (const std::invalid_argument&) {
                table.data[col_idx].push_back(cell);
            }
            col_idx++;
        }
    }
    std::cout << "Loaded " << (table.data.empty() ? 0 : table.data[0].size()) << " rows from " << filename << std::endl;
}

} // namespace db