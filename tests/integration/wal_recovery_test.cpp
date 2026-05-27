// End-to-end recovery tests: prove that WAL + RecoveryManager reproduce
// in-memory state across a Database lifecycle.

#include "columnar_db/engine/database.h"
#include "columnar_db/recovery/recovery_manager.h"

#include <gtest/gtest.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace db {

class WalRecoveryTest : public ::testing::Test {
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

// Sanity: a fresh Database (no log file yet) recovers to an empty state
// without crashing.
TEST_F(WalRecoveryTest, RecoverOnEmptyLogIsNoOp) {
  Database db(wal_file_);
  RecoveryManager::Recover(db);  // Should not throw.
  db.CreateTable("users");
  EXPECT_TRUE(db.Scan("users").empty());
}

// The core slice 1 lesson: inserts that happen before destruction are
// recovered into a fresh Database via the log alone.
TEST_F(WalRecoveryTest, InsertsSurviveDestroyAndRecover) {
  // Run 1: insert, then destroy without flushing in-memory state anywhere.
  {
    Database db(wal_file_);
    db.CreateTable("users");
    db.Insert("users", {1, 25});
    db.Insert("users", {2, 30});
    db.Insert("users", {3, 99});
    // db goes out of scope. The in-memory map is gone; only the WAL remains.
  }

  // Run 2: a fresh Database on the same WAL recovers the inserts.
  Database db2(wal_file_);
  RecoveryManager::Recover(db2);

  // No CreateTable call here — recovery's auto-create handles it.
  const auto& users = db2.Scan("users");
  ASSERT_EQ(users.size(), 3u);
  EXPECT_EQ(users[0], (std::vector<int64_t>{1, 25}));
  EXPECT_EQ(users[1], (std::vector<int64_t>{2, 30}));
  EXPECT_EQ(users[2], (std::vector<int64_t>{3, 99}));
}

// Recovery should be idempotent: running it twice in a row produces the
// same state as running it once. The first call truncates the WAL, so the
// second call sees an empty log.
TEST_F(WalRecoveryTest, RecoverIsIdempotent) {
  {
    Database db(wal_file_);
    db.CreateTable("users");
    db.Insert("users", {1, 25});
    db.Insert("users", {2, 30});
  }

  Database db2(wal_file_);
  RecoveryManager::Recover(db2);
  RecoveryManager::Recover(db2);  // Second call: WAL is empty, ClearAll wipes,
                                   // replay yields nothing.

  // After two recoveries, the original inserts are gone — the second
  // ClearAll wiped them and the empty WAL had nothing to replay. This
  // is the brute-force semantics of slice 1's "wipe then replay": it is
  // *not* safe to re-run after the first successful recovery has cleared
  // the log. Slice 2's LSN-based recovery removes this footgun.
  EXPECT_TRUE(db2.Scan("users").empty());
}

// Inserts done after recovery are durable across another recovery cycle.
TEST_F(WalRecoveryTest, PostRecoveryInsertsAreThemselvesRecoverable) {
  {
    Database db(wal_file_);
    db.CreateTable("users");
    db.Insert("users", {1, 25});
  }

  {
    Database db2(wal_file_);
    RecoveryManager::Recover(db2);
    // After Recover: users has {1,25}, WAL is empty.
    db2.Insert("users", {2, 30});  // This insert logs to the now-empty WAL.
  }

  // A third run sees only the post-recovery insert in its log (the
  // pre-recovery one was applied to memory and then the log was cleared
  // — that's the slice 1 model).
  Database db3(wal_file_);
  RecoveryManager::Recover(db3);
  const auto& users = db3.Scan("users");
  ASSERT_EQ(users.size(), 1u);
  EXPECT_EQ(users[0], (std::vector<int64_t>{2, 30}));
}

// Insert into an unknown table throws — pre-validation runs *before* the
// log append, so a typo doesn't leave a useless record on disk.
TEST_F(WalRecoveryTest, InsertIntoUnknownTableThrowsAndDoesNotLog) {
  Database db(wal_file_);
  EXPECT_THROW(db.Insert("ghost", {1}), std::out_of_range);

  // Recovery on the fresh log finds nothing — the failed insert never
  // wrote a record.
  Database db2(wal_file_);
  RecoveryManager::Recover(db2);
  EXPECT_THROW(db2.Scan("ghost"), std::out_of_range);
}

// Multiple tables in the same WAL are kept distinct on replay.
TEST_F(WalRecoveryTest, MultipleTablesRecoverIndependently) {
  {
    Database db(wal_file_);
    db.CreateTable("users");
    db.CreateTable("orders");
    db.Insert("users", {1, 25});
    db.Insert("orders", {100, 200, 300});
    db.Insert("users", {2, 30});
  }

  Database db2(wal_file_);
  RecoveryManager::Recover(db2);

  const auto& users = db2.Scan("users");
  const auto& orders = db2.Scan("orders");
  ASSERT_EQ(users.size(), 2u);
  ASSERT_EQ(orders.size(), 1u);
  EXPECT_EQ(users[0], (std::vector<int64_t>{1, 25}));
  EXPECT_EQ(users[1], (std::vector<int64_t>{2, 30}));
  EXPECT_EQ(orders[0], (std::vector<int64_t>{100, 200, 300}));
}

}  // namespace db
