// Unit tests for LogRecord serialization.

#include "columnar_db/wal/log_record.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <vector>

namespace db {

TEST(LogRecordTest, RoundTripPreservesAllFields) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "users", {1, 25});
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  LogRecord out(LogRecordType::INVALID, "", {});
  uint32_t consumed = LogRecord::Deserialize(buf.data(), out);

  EXPECT_EQ(consumed, r.GetSize());
  EXPECT_EQ(out.GetType(), LogRecordType::INSERT_TUPLE);
  EXPECT_EQ(out.GetTableName(), "users");
  EXPECT_EQ(out.GetTuple(), (std::vector<int64_t>{1, 25}));
}

TEST(LogRecordTest, RoundTripEmptyTuple) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "t", {});
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  LogRecord out(LogRecordType::INVALID, "x", {7});
  LogRecord::Deserialize(buf.data(), out);

  EXPECT_EQ(out.GetTableName(), "t");
  EXPECT_TRUE(out.GetTuple().empty());
}

TEST(LogRecordTest, RoundTripEmptyTableName) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "", {42});
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  LogRecord out(LogRecordType::INVALID, "filled", {});
  LogRecord::Deserialize(buf.data(), out);

  EXPECT_TRUE(out.GetTableName().empty());
  EXPECT_EQ(out.GetTuple(), (std::vector<int64_t>{42}));
}

TEST(LogRecordTest, RoundTripLargeTuple) {
  std::vector<int64_t> big;
  big.reserve(256);
  for (int64_t i = 0; i < 256; ++i) {
    big.push_back(i * i - 17);
  }
  LogRecord r(LogRecordType::INSERT_TUPLE, "wide_table", big);
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  LogRecord out(LogRecordType::INVALID, "", {});
  LogRecord::Deserialize(buf.data(), out);

  EXPECT_EQ(out.GetTuple(), big);
}

TEST(LogRecordTest, GetSizeMatchesSerializedBytes) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "t", {7, 8, 9});
  // Allocate extra trailing bytes filled with a sentinel; Serialize must not
  // touch any byte past GetSize().
  const uint8_t kSentinel = 0xAB;
  std::vector<char> buf(r.GetSize() + 16, static_cast<char>(kSentinel));
  r.Serialize(buf.data());
  for (size_t i = r.GetSize(); i < buf.size(); ++i) {
    EXPECT_EQ(static_cast<unsigned char>(buf[i]), kSentinel)
        << "Serialize wrote past GetSize() at offset " << i;
  }
}

TEST(LogRecordTest, FirstFourBytesAreTotalSize) {
  LogRecord r(LogRecordType::INSERT_TUPLE, "abc", {1, 2, 3, 4});
  std::vector<char> buf(r.GetSize());
  r.Serialize(buf.data());

  uint32_t header_size;
  std::memcpy(&header_size, buf.data(), sizeof(uint32_t));
  EXPECT_EQ(header_size, r.GetSize());
}

TEST(LogRecordTest, LogRecordTypeIsSingleByte) {
  // Wire-format stability check: the underlying type must be uint8_t.
  EXPECT_EQ(sizeof(LogRecordType), 1u);
}

}  // namespace db
