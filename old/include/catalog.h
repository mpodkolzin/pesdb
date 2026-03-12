#pragma once

#include "columnar_db/common/types.h"
#include "columnar_db/common/config.h"
#include "columnar_db/storage/buffer_pool_manager.h"
#include <string>
#include <vector>
#include <map>

namespace db {

struct Column {
    char name[32];
    DataType type;
    page_id_t first_page_id;
};

struct TableSchema {
    char name[32];
    std::vector<Column> columns;
};

class Catalog {
public:
    explicit Catalog(BufferPoolManager* buffer_pool_manager, bool is_new_db);

    bool CreateTable(TableSchema& schema);
    const TableSchema* GetTableSchema(const std::string& table_name);

private:
    void LoadFromDisk();
    void PersistToDisk();

    BufferPoolManager* bpm_;
    std::map<std::string, TableSchema> schemas_;
};

} // namespace db