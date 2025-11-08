#pragma once

#include <cstdint>

namespace db {

using page_id_t = int32_t;
using frame_id_t = int32_t;

constexpr page_id_t INVALID_PAGE_ID = -1;
static constexpr int PAGE_SIZE = 4096; // 4KB pages
static constexpr int BUFFER_POOL_SIZE = 10; // A small pool of 10 pages for learning

} // namespace db