//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_avro.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

enum class AvroType : uint8_t {
	NULL_,
	BOOLEAN,
	INT,
	LONG,
	FLOAT,
	DOUBLE,
	BYTES,
	STRING,
	RECORD,
	ENUM,
	ARRAY,
	MAP,
	FIXED,
	UNION
};

struct AvroSchemaNode;

struct AvroField {
	string name;
	shared_ptr<AvroSchemaNode> schema;
	//! Raw JSON text of the "default" attribute (empty if absent)
	string default_json;
	//! Raw JSON text of the "field-id" attribute (empty if absent)
	string field_id;
};

struct AvroSchemaNode {
	AvroType type = AvroType::NULL_;
	//! Full name for record/enum/fixed
	string name;
	//! Logical type annotation (informational only, e.g. "decimal", "date")
	string logical_type;
	//! RECORD
	vector<AvroField> fields;
	//! ENUM
	vector<string> symbols;
	//! UNION
	vector<shared_ptr<AvroSchemaNode>> branches;
	//! ARRAY items / MAP values
	shared_ptr<AvroSchemaNode> child;
	//! FIXED
	idx_t fixed_size = 0;
	//! Raw JSON text of "element-id" (ARRAY), "key-id" / "value-id" (MAP); empty if absent
	string element_id;
	string key_id;
	string value_id;

	//! Index of a record field by name, or DConstants::INVALID_INDEX
	idx_t FieldIndex(const string &field_name) const;
};

class AvroSchema {
public:
	AvroSchema() {
	}
	//! Parses an Avro schema from JSON text
	static AvroSchema Parse(const string &json);

	const string &Json() const {
		return json;
	}
	bool IsValid() const {
		return root != nullptr;
	}
	const shared_ptr<AvroSchemaNode> &Root() const {
		return root;
	}

private:
	string json;
	shared_ptr<AvroSchemaNode> root;
};

enum class AvroValueKind : uint8_t {
	NULL_,
	BOOL,
	INT,
	LONG,
	FLOAT,
	DOUBLE,
	BYTES,
	STRING,
	RECORD,
	ARRAY,
	MAP,
	ENUM,
	FIXED,
	UNION
};

//! Dynamically typed Avro value
class AvroValue {
public:
	AvroValue() : kind(AvroValueKind::NULL_) {
	}

	static AvroValue Null();
	static AvroValue Bool(bool value);
	static AvroValue Int(int32_t value);
	static AvroValue Long(int64_t value);
	static AvroValue Float(float value);
	static AvroValue Double(double value);
	static AvroValue Bytes(string value);
	static AvroValue String(string value);
	//! Fields in schema order
	static AvroValue Record(vector<AvroValue> fields);
	static AvroValue Array(vector<AvroValue> items);
	//! Keys and values must have the same length; order is preserved
	static AvroValue Map(vector<string> keys, vector<AvroValue> values);
	static AvroValue Enum(idx_t index, string symbol = string());
	static AvroValue Fixed(string value);
	static AvroValue Union(idx_t branch, AvroValue value);

public:
	AvroValueKind kind;
	//! BOOL, INT, LONG, ENUM index, UNION branch
	int64_t int_value = 0;
	//! FLOAT, DOUBLE
	double double_value = 0;
	//! BYTES, STRING, FIXED, ENUM symbol
	string str_value;
	//! RECORD fields, ARRAY items, MAP values, UNION (single child)
	vector<AvroValue> children;
	//! MAP keys
	vector<string> keys;
	//! Set on records produced by the reader; used by Get(name)
	shared_ptr<AvroSchemaNode> record_schema;

public:
	//! Unwraps unions (recursively)
	const AvroValue &Unwrap() const;
	//! True for NULL and for a union holding null
	bool IsNull() const;
	int64_t AsLong() const;
	bool AsBool() const;
	double AsDouble() const;
	//! STRING, BYTES, FIXED and ENUM (symbol) contents
	const string &AsString() const;
	//! Record field by name (requires record_schema); throws if not found
	const AvroValue &Get(const string &field_name) const;
	//! Record field by name using an explicit schema node
	const AvroValue &Get(const AvroSchemaNode &schema, const string &field_name) const;
	//! Record field by name, null value if missing or no schema
	const AvroValue &TryGet(const string &field_name) const;
	//! Map value by key, nullptr if missing
	const AvroValue *MapGet(const string &key) const;
	//! Renders as JSON (bytes/fixed as hex, unions unwrapped)
	string ToJSON() const;
};

struct AvroFileData {
	string schema_json;
	AvroSchema schema;
	//! Header metadata including avro.schema / avro.codec (raw bytes)
	map<string, string> metadata;
	vector<AvroValue> records;
};

class AvroReader {
public:
	//! Reads an Avro object container file. Supported codecs: null, deflate
	static AvroFileData Read(const string &bytes);
};

class AvroWriter {
public:
	//! Writes an Avro object container file. Supported codecs: null, deflate.
	//! schema_json is written verbatim as avro.schema; avro.schema/avro.codec in metadata are ignored.
	static string Write(const string &schema_json, const map<string, string> &metadata,
	                    const vector<AvroValue> &records, const string &codec = "null");
};

} // namespace duckdb
