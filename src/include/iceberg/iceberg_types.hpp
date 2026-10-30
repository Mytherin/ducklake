//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/json_document.hpp"
#include "rest/ducklake_rest_json.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

struct IcebergField;

//! An Iceberg type: a primitive (written as in the Iceberg specification, e.g. "long" or "decimal(10, 2)") or a
//! struct, list or map
struct IcebergType {
	enum class Kind : uint8_t { PRIMITIVE, STRUCT, LIST, MAP };

	Kind kind = Kind::PRIMITIVE;
	string primitive;
	//! The fields of a struct, the element of a list (one field), or the key and the value of a map (two fields)
	vector<IcebergField> fields;

	IcebergType Copy() const;
};

struct IcebergField {
	idx_t id = 0;
	string name;
	bool required = false;
	IcebergType type;
	string doc;
};

struct IcebergSchema {
	idx_t schema_id = 0;
	vector<IcebergField> fields;
	vector<idx_t> identifier_field_ids;

	//! The largest field id of the schema, including the nested fields
	idx_t MaxFieldId() const;
	const IcebergField *FindField(idx_t field_id) const;
	const IcebergField *FindTopLevelByName(const string &name) const;
	//! Whether the schemas have the same fields (ignoring the schema id)
	bool SameFields(const IcebergSchema &other) const;
};

class IcebergTypes {
public:
	//! The Iceberg field of a DuckLake column, throws if the type cannot be exposed through Iceberg
	static IcebergField FromColumn(const DuckLakeColumnInfo &column, bool top_level = true);
	static IcebergSchema FromColumns(const vector<DuckLakeColumnInfo> &columns, idx_t schema_id);
	//! The DuckDB type of an Iceberg type
	static LogicalType ToLogicalType(const IcebergType &type);
	//! The SQL type of an Iceberg type
	static string ToSQLType(const IcebergType &type);
	//! Whether the type contains a struct, list or map
	static bool IsNested(const IcebergType &type);

	static JSONMutableValue TypeToJson(JSONWriter &writer, const IcebergType &type);
	static JSONMutableValue FieldToJson(JSONWriter &writer, const IcebergField &field);
	static JSONMutableValue SchemaToJson(JSONWriter &writer, const IcebergSchema &schema);
	static IcebergType TypeFromJson(RESTJsonIn json);
	static IcebergField FieldFromJson(RESTJsonIn json);
	static IcebergSchema SchemaFromJson(RESTJsonIn json);
	static string SchemaToString(const IcebergSchema &schema);

	//! Parses decimal(P, S) and fixed[L]
	static bool ParseDecimal(const string &primitive, idx_t &precision, idx_t &scale);
	static bool ParseFixed(const string &primitive, idx_t &length);
	//! Normalizes the writing of a primitive (e.g. whitespace in decimals)
	static string NormalizePrimitive(const string &primitive);
};

} // namespace duckdb
