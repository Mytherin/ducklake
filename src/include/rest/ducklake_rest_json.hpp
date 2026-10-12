//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_json.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! A handle to a mutable JSON value that is owned by a JSONWriter
class RESTJsonOut {
public:
	RESTJsonOut(JSONWriter &writer, JSONMutableValue val) : writer(&writer), val(val) {
	}

	RESTJsonOut Obj(const string &key);
	RESTJsonOut Arr(const string &key);
	void SetStr(const string &key, const string &value);
	void SetUInt(const string &key, idx_t value);
	void SetInt(const string &key, int64_t value);
	void SetBool(const string &key, bool value);
	void SetDouble(const string &key, double value);
	void SetNull(const string &key);
	//! Writes null for an invalid index
	void SetOpt(const string &key, optional_idx value);
	void SetValue(const string &key, const Value &value);
	void SetRaw(const string &key, JSONMutableValue value);

	RESTJsonOut AppendObj();
	RESTJsonOut AppendArr();
	void AppendStr(const string &value);
	void AppendUInt(idx_t value);
	void AppendRaw(JSONMutableValue value);

	JSONWriter *writer;
	JSONMutableValue val;
};

//! Builds a JSON document with an object as root
class RESTJsonWriter {
public:
	RESTJsonWriter();

	RESTJsonOut Root() {
		return RESTJsonOut(writer, root);
	}
	void SetRoot(JSONMutableValue value) {
		root = value;
		writer.SetRoot(value);
	}
	string ToString() const {
		return writer.ToString();
	}

	JSONWriter writer;
	JSONMutableValue root;
};

//! A handle to an immutable JSON value that is owned by a JSONDocument
class RESTJsonIn {
public:
	RESTJsonIn() {
	}
	explicit RESTJsonIn(JSONValue val) : val(val) {
	}

	bool IsNull() const;
	bool IsObject() const;
	bool IsArray() const;
	bool IsString() const;
	bool Has(const string &key) const;
	//! Throws if the key is missing
	RESTJsonIn Get(const string &key) const;
	//! Returns a null value if the key is missing
	RESTJsonIn TryGet(const string &key) const;
	vector<RESTJsonIn> Items() const;
	vector<pair<string, RESTJsonIn>> Members() const;

	string Str() const;
	idx_t UInt() const;
	int64_t Int() const;
	double Double() const;
	bool Bool() const;

	string Str(const string &key) const;
	string Str(const string &key, const string &default_value) const;
	idx_t UInt(const string &key) const;
	idx_t UInt(const string &key, idx_t default_value) const;
	int64_t Int(const string &key) const;
	bool Bool(const string &key, bool default_value = false) const;
	optional_idx Opt(const string &key) const;
	Value GetValue(const string &key) const;

	JSONValue val;
};

//! Parses a JSON document
class RESTJsonReader {
public:
	explicit RESTJsonReader(const string &json);

	RESTJsonIn Root() const {
		return RESTJsonIn(doc->GetRoot());
	}

	unique_ptr<JSONDocument> doc;
};

struct RESTJson {
	//! Values without a known type: NULL as null, VARCHAR as string, otherwise {"type": .., "value": ..}
	static JSONMutableValue ValueToJson(JSONWriter &writer, const Value &value);
	static Value ValueFromJson(RESTJsonIn json);

	//! Logical types as structured JSON
	static JSONMutableValue TypeToJson(JSONWriter &writer, const LogicalType &type);
	static LogicalType TypeFromJson(RESTJsonIn json);

	//! Values of a known type: nested types as JSON objects / arrays, leaves as JSON scalars or strings
	static JSONMutableValue TypedValueToJson(JSONWriter &writer, const Value &value);
	static Value TypedValueFromJson(RESTJsonIn json, const LogicalType &type);
};

} // namespace duckdb
