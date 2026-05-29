// Unit tests for Schema - CSV header parsing and serialization

#include "columnar_db/ingest/schema.h"
#include "columnar_db/common/config.h"

#include <gtest/gtest.h>
#include <vector>

namespace db {

// ============================================================================
// Test 1: Parse Simple CSV Header
// ============================================================================

TEST(SchemaTest, ParseSimpleCSVHeader) {
  std::string header = "id:INT64,name:STRING,score:FLOAT64";
  Schema schema = Schema::ParseCSVHeader("users", header);

  EXPECT_EQ(schema.GetTableName(), "users");
  EXPECT_EQ(schema.GetColumnCount(), 3);

  EXPECT_EQ(schema.GetColumn(0).name, "id");
  EXPECT_EQ(schema.GetColumn(0).type, DataType::INT64);

  EXPECT_EQ(schema.GetColumn(1).name, "name");
  EXPECT_EQ(schema.GetColumn(1).type, DataType::STRING);

  EXPECT_EQ(schema.GetColumn(2).name, "score");
  EXPECT_EQ(schema.GetColumn(2).type, DataType::FLOAT64);
}

// ============================================================================
// Test 2: Parse Header with Whitespace
// ============================================================================

TEST(SchemaTest, ParseHeaderWithWhitespace) {
  std::string header = " id : INT64 , name : STRING , active : BOOL ";
  Schema schema = Schema::ParseCSVHeader("users", header);

  EXPECT_EQ(schema.GetColumnCount(), 3);
  EXPECT_EQ(schema.GetColumn(0).name, "id");
  EXPECT_EQ(schema.GetColumn(1).name, "name");
  EXPECT_EQ(schema.GetColumn(2).name, "active");
  EXPECT_EQ(schema.GetColumn(2).type, DataType::BOOL);
}

// ============================================================================
// Test 3: Parse Header - Case Insensitive Types
// ============================================================================

TEST(SchemaTest, ParseHeaderCaseInsensitiveTypes) {
  std::string header = "a:int64,b:FLOAT64,c:bool,d:String";
  Schema schema = Schema::ParseCSVHeader("test", header);

  EXPECT_EQ(schema.GetColumn(0).type, DataType::INT64);
  EXPECT_EQ(schema.GetColumn(1).type, DataType::FLOAT64);
  EXPECT_EQ(schema.GetColumn(2).type, DataType::BOOL);
  EXPECT_EQ(schema.GetColumn(3).type, DataType::STRING);
}

// ============================================================================
// Test 4: Parse Header - Type Aliases
// ============================================================================

TEST(SchemaTest, ParseHeaderTypeAliases) {
  // BOOLEAN -> BOOL, VARCHAR -> STRING, TEXT -> STRING
  std::string header = "a:BOOLEAN,b:VARCHAR,c:TEXT";
  Schema schema = Schema::ParseCSVHeader("test", header);

  EXPECT_EQ(schema.GetColumn(0).type, DataType::BOOL);
  EXPECT_EQ(schema.GetColumn(1).type, DataType::STRING);
  EXPECT_EQ(schema.GetColumn(2).type, DataType::STRING);
}

// ============================================================================
// Test 5: Parse Header - Invalid Type
// ============================================================================

TEST(SchemaTest, ParseHeaderInvalidType) {
  std::string header = "id:INT64,value:UNKNOWN_TYPE";

  EXPECT_THROW(
    { Schema::ParseCSVHeader("test", header); },
    std::runtime_error
  );
}

// ============================================================================
// Test 6: Parse Header - Missing Type
// ============================================================================

TEST(SchemaTest, ParseHeaderMissingType) {
  std::string header = "id:INT64,name";  // Missing :TYPE

  EXPECT_THROW(
    { Schema::ParseCSVHeader("test", header); },
    std::runtime_error
  );
}

// ============================================================================
// Test 7: Parse Header - Empty Column Name
// ============================================================================

TEST(SchemaTest, ParseHeaderEmptyColumnName) {
  std::string header = ":INT64,name:STRING";

  EXPECT_THROW(
    { Schema::ParseCSVHeader("test", header); },
    std::runtime_error
  );
}

// ============================================================================
// Test 8: Parse Header - Empty Header
// ============================================================================

TEST(SchemaTest, ParseHeaderEmpty) {
  std::string header = "";

  EXPECT_THROW(
    { Schema::ParseCSVHeader("test", header); },
    std::runtime_error
  );
}

// ============================================================================
// Test 9: Serialize and Deserialize Round-Trip
// ============================================================================

TEST(SchemaTest, SerializeDeserializeRoundTrip) {
  // Create schema
  std::vector<Column> columns = {
    {"id", DataType::INT64},
    {"name", DataType::STRING},
    {"score", DataType::FLOAT64},
    {"active", DataType::BOOL}
  };
  Schema original("users", columns);

  // Serialize
  size_t size = original.GetSerializedSize();
  std::vector<char> buffer(size);
  size_t written = original.Serialize(buffer.data());

  EXPECT_EQ(written, size);

  // Deserialize
  Schema deserialized;
  size_t consumed = Schema::Deserialize(buffer.data(), deserialized);

  EXPECT_EQ(consumed, written);
  EXPECT_EQ(deserialized, original);
}

// ============================================================================
// Test 10: Serialize - GetSerializedSize Matches Actual Size
// ============================================================================

TEST(SchemaTest, GetSerializedSizeMatchesActual) {
  std::vector<Column> columns = {
    {"x", DataType::INT64},
    {"y", DataType::FLOAT64},
    {"label", DataType::STRING}
  };
  Schema schema("points", columns);

  size_t expected_size = schema.GetSerializedSize();
  std::vector<char> buffer(expected_size + 16, 0xAB);  // Sentinel bytes

  size_t actual_size = schema.Serialize(buffer.data());

  EXPECT_EQ(actual_size, expected_size);

  // Verify we didn't write past GetSerializedSize()
  for (size_t i = expected_size; i < buffer.size(); ++i) {
    EXPECT_EQ(static_cast<unsigned char>(buffer[i]), 0xAB)
        << "Serialize wrote past GetSerializedSize() at offset " << i;
  }
}

// ============================================================================
// Test 11: Deserialize - Invalid Magic Number
// ============================================================================

TEST(SchemaTest, DeserializeInvalidMagic) {
  std::vector<char> buffer(100, 0x00);

  // Write incorrect magic number
  uint32_t bad_magic = 0xDEADBEEF;
  std::memcpy(buffer.data(), &bad_magic, sizeof(uint32_t));

  Schema schema;
  EXPECT_THROW(
    { Schema::Deserialize(buffer.data(), schema); },
    std::runtime_error
  );
}

// ============================================================================
// Test 12: Deserialize - Unsupported Version
// ============================================================================

TEST(SchemaTest, DeserializeUnsupportedVersion) {
  std::vector<char> buffer(100, 0x00);

  // Write correct magic
  uint32_t magic = 0x50455344;  // SCHEMA_MAGIC
  std::memcpy(buffer.data(), &magic, sizeof(uint32_t));

  // Write unsupported version
  uint32_t version = 999;
  std::memcpy(buffer.data() + 4, &version, sizeof(uint32_t));

  Schema schema;
  EXPECT_THROW(
    { Schema::Deserialize(buffer.data(), schema); },
    std::runtime_error
  );
}

// ============================================================================
// Test 13: FindColumn
// ============================================================================

TEST(SchemaTest, FindColumn) {
  std::vector<Column> columns = {
    {"id", DataType::INT64},
    {"name", DataType::STRING},
    {"score", DataType::FLOAT64}
  };
  Schema schema("test", columns);

  EXPECT_EQ(schema.FindColumn("id"), 0);
  EXPECT_EQ(schema.FindColumn("name"), 1);
  EXPECT_EQ(schema.FindColumn("score"), 2);
  EXPECT_EQ(schema.FindColumn("nonexistent"), -1);
}

// ============================================================================
// Test 14: Single Column Schema
// ============================================================================

TEST(SchemaTest, SingleColumnSchema) {
  std::string header = "value:INT64";
  Schema schema = Schema::ParseCSVHeader("single", header);

  EXPECT_EQ(schema.GetColumnCount(), 1);
  EXPECT_EQ(schema.GetColumn(0).name, "value");
  EXPECT_EQ(schema.GetColumn(0).type, DataType::INT64);
}

// ============================================================================
// Test 15: Many Columns Schema
// ============================================================================

TEST(SchemaTest, ManyColumnsSchema) {
  // Build header with 50 columns
  std::string header;
  for (int i = 0; i < 50; ++i) {
    if (i > 0) header += ",";
    header += "col" + std::to_string(i) + ":INT64";
  }

  Schema schema = Schema::ParseCSVHeader("wide", header);

  EXPECT_EQ(schema.GetColumnCount(), 50);
  EXPECT_EQ(schema.GetColumn(0).name, "col0");
  EXPECT_EQ(schema.GetColumn(49).name, "col49");

  // Round-trip serialization
  std::vector<char> buffer(schema.GetSerializedSize());
  schema.Serialize(buffer.data());

  Schema deserialized;
  Schema::Deserialize(buffer.data(), deserialized);

  EXPECT_EQ(deserialized, schema);
}

}  // namespace db
