// Unit tests for LogManager: append/read/clear and torn-tail recovery.

#include "columnar_db/wal/log_manager.h"
#include "columnar_db/wal/log_record.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace db {

class LogManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    wal_file_ = "test_" + std::string(
        ::testing::UnitTest::GetInstance()->current_test_info()->name()) + ".wal";
    Remove();
  }

  void TearDown() override {
    Remove();
  }

  void Remove() {
    if (std::filesystem::exists(wal_file_)) {
      std::filesystem::remove(wal_file_);
    }
  }

  std::string wal_file_;
};

TEST_F(LogManagerTest, EmptyFileReadsZeroRecords) {
  LogManager lm(wal_file_);
  EXPECT_TRUE(lm.ReadAllLogRecords().empty());
}

TEST_F(LogManagerTest, AppendThenReadAllReturnsRecordsInOrder) {
  {
    LogManager lm(wal_file_);
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users",  {1, 25}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users",  {2, 30}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "orders", {99}));
  }

  // Re-open: the records appended above must be visible to a fresh instance,
  // which exercises the durability-across-handle-close contract.
  LogManager lm2(wal_file_);
  auto records = lm2.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 3u);

  EXPECT_EQ(records[0].GetType(),      LogRecordType::INSERT_TUPLE);
  EXPECT_EQ(records[0].GetTableName(), "users");
  EXPECT_EQ(records[0].GetTuple(),     (std::vector<int64_t>{1, 25}));

  EXPECT_EQ(records[1].GetTableName(), "users");
  EXPECT_EQ(records[1].GetTuple(),     (std::vector<int64_t>{2, 30}));

  EXPECT_EQ(records[2].GetTableName(), "orders");
  EXPECT_EQ(records[2].GetTuple(),     (std::vector<int64_t>{99}));
}

TEST_F(LogManagerTest, ReadAllAfterAppendOnSameInstance) {
  LogManager lm(wal_file_);
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {1}));
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {2}));

  auto records = lm.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 2u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{1}));
  EXPECT_EQ(records[1].GetTuple(), (std::vector<int64_t>{2}));

  // Subsequent appends must still land at end-of-file (ReadAll restores
  // the put-position).
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {3}));
  auto records2 = lm.ReadAllLogRecords();
  ASSERT_EQ(records2.size(), 3u);
  EXPECT_EQ(records2[2].GetTuple(), (std::vector<int64_t>{3}));
}

TEST_F(LogManagerTest, TornTailBodyIsTreatedAsEndOfLog) {
  {
    LogManager lm(wal_file_);
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {1, 25}));
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {2, 30}));
  }

  // Truncate partway through the second record's body.
  auto good_size = std::filesystem::file_size(wal_file_);
  ASSERT_GT(good_size, 4u);
  std::filesystem::resize_file(wal_file_, good_size - 4);

  LogManager lm2(wal_file_);
  auto records = lm2.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{1, 25}));
}

TEST_F(LogManagerTest, TornTailHeaderIsTreatedAsEndOfLog) {
  {
    LogManager lm(wal_file_);
    lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "users", {1, 25}));
  }

  // Append 2 stray bytes — not even a full size header. Recovery should
  // skip them and return only the one fully-written record.
  {
    std::ofstream raw(wal_file_, std::ios::binary | std::ios::app);
    char garbage[2] = {0x12, 0x34};
    raw.write(garbage, sizeof(garbage));
  }

  LogManager lm2(wal_file_);
  auto records = lm2.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{1, 25}));
}

TEST_F(LogManagerTest, ClearLogTruncatesAndKeepsHandleUsable) {
  LogManager lm(wal_file_);
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {1}));
  EXPECT_GT(std::filesystem::file_size(wal_file_), 0u);

  lm.ClearLog();
  EXPECT_EQ(std::filesystem::file_size(wal_file_), 0u);
  EXPECT_TRUE(lm.ReadAllLogRecords().empty());

  // Append must still work after a clear.
  lm.AppendLogRecord(LogRecord(LogRecordType::INSERT_TUPLE, "t", {2}));
  auto records = lm.ReadAllLogRecords();
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records[0].GetTuple(), (std::vector<int64_t>{2}));
}

}  // namespace db
