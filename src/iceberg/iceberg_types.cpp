#include "iceberg/iceberg_types.hpp"

#include "common/ducklake_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergType IcebergType::Copy() const {
	IcebergType result;
	result.kind = kind;
	result.primitive = primitive;
	for (auto &field : fields) {
		IcebergField copy;
		copy.id = field.id;
		copy.name = field.name;
		copy.required = field.required;
		copy.type = field.type.Copy();
		copy.doc = field.doc;
		result.fields.push_back(std::move(copy));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
static idx_t MaxFieldIdOf(const IcebergField &field) {
	idx_t result = field.id;
	for (auto &child : field.type.fields) {
		result = MaxValue<idx_t>(result, MaxFieldIdOf(child));
	}
	return result;
}

idx_t IcebergSchema::MaxFieldId() const {
	idx_t result = 0;
	for (auto &field : fields) {
		result = MaxValue<idx_t>(result, MaxFieldIdOf(field));
	}
	return result;
}

static const IcebergField *FindFieldRecursive(const vector<IcebergField> &fields, idx_t field_id) {
	for (auto &field : fields) {
		if (field.id == field_id) {
			return &field;
		}
		auto nested = FindFieldRecursive(field.type.fields, field_id);
		if (nested) {
			return nested;
		}
	}
	return nullptr;
}

const IcebergField *IcebergSchema::FindField(idx_t field_id) const {
	return FindFieldRecursive(fields, field_id);
}

const IcebergField *IcebergSchema::FindTopLevelByName(const string &name) const {
	for (auto &field : fields) {
		if (StringUtil::CIEquals(field.name, name)) {
			return &field;
		}
	}
	return nullptr;
}

static bool SameType(const IcebergType &a, const IcebergType &b);

static bool SameFieldList(const vector<IcebergField> &a, const vector<IcebergField> &b) {
	if (a.size() != b.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.size(); i++) {
		if (a[i].id != b[i].id || a[i].name != b[i].name || a[i].required != b[i].required ||
		    !SameType(a[i].type, b[i].type)) {
			return false;
		}
	}
	return true;
}

static bool SameType(const IcebergType &a, const IcebergType &b) {
	if (a.kind != b.kind) {
		return false;
	}
	if (a.kind == IcebergType::Kind::PRIMITIVE) {
		return a.primitive == b.primitive;
	}
	return SameFieldList(a.fields, b.fields);
}

bool IcebergSchema::SameFields(const IcebergSchema &other) const {
	return SameFieldList(fields, other.fields);
}

//===--------------------------------------------------------------------===//
// DuckLake -> Iceberg
//===--------------------------------------------------------------------===//
static string PrimitiveFromLogicalType(const LogicalType &type, const string &column_name) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "boolean";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "int";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "long";
	case LogicalTypeId::FLOAT:
		return "float";
	case LogicalTypeId::DOUBLE:
		return "double";
	case LogicalTypeId::DECIMAL:
		return StringUtil::Format("decimal(%d, %d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return "timestamp";
	case LogicalTypeId::TIMESTAMP_NS:
		return "timestamp_ns";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalTypeId::VARCHAR:
		return "string";
	case LogicalTypeId::UUID:
		return "uuid";
	case LogicalTypeId::BLOB:
		return "binary";
	default:
		throw NotImplementedException("The type %s of column \"%s\" cannot be exposed through the Iceberg REST API",
		                              type.ToString(), column_name);
	}
}

IcebergField IcebergTypes::FromColumn(const DuckLakeColumnInfo &column, bool top_level) {
	IcebergField result;
	result.id = column.id.index;
	result.name = column.name;
	result.required = top_level && !column.nulls_allowed;
	auto type = DuckLakeTypes::FromString(column.type);
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		result.type.kind = IcebergType::Kind::STRUCT;
		for (auto &child : column.children) {
			result.type.fields.push_back(FromColumn(child, false));
		}
		break;
	case LogicalTypeId::LIST:
		result.type.kind = IcebergType::Kind::LIST;
		for (auto &child : column.children) {
			result.type.fields.push_back(FromColumn(child, false));
		}
		if (result.type.fields.size() != 1) {
			throw InvalidInputException("Lists must have a single child entry");
		}
		result.type.fields[0].name = "element";
		break;
	case LogicalTypeId::MAP:
		result.type.kind = IcebergType::Kind::MAP;
		for (auto &child : column.children) {
			result.type.fields.push_back(FromColumn(child, false));
		}
		if (result.type.fields.size() != 2) {
			throw InvalidInputException("Maps must have two child entries");
		}
		result.type.fields[0].name = "key";
		result.type.fields[0].required = true;
		result.type.fields[1].name = "value";
		break;
	default:
		result.type.kind = IcebergType::Kind::PRIMITIVE;
		result.type.primitive = PrimitiveFromLogicalType(type, column.name);
		break;
	}
	return result;
}

IcebergSchema IcebergTypes::FromColumns(const vector<DuckLakeColumnInfo> &columns, idx_t schema_id) {
	IcebergSchema result;
	result.schema_id = schema_id;
	for (auto &column : columns) {
		result.fields.push_back(FromColumn(column));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Iceberg -> DuckDB
//===--------------------------------------------------------------------===//
bool IcebergTypes::ParseDecimal(const string &primitive, idx_t &precision, idx_t &scale) {
	if (!StringUtil::StartsWith(primitive, "decimal(") || !StringUtil::EndsWith(primitive, ")")) {
		return false;
	}
	auto inner = primitive.substr(8, primitive.size() - 9);
	auto parts = StringUtil::Split(inner, ',');
	if (parts.size() != 2) {
		return false;
	}
	StringUtil::Trim(parts[0]);
	StringUtil::Trim(parts[1]);
	try {
		precision = std::stoull(parts[0]);
		scale = std::stoull(parts[1]);
	} catch (...) {
		return false;
	}
	return true;
}

bool IcebergTypes::ParseFixed(const string &primitive, idx_t &length) {
	if (!StringUtil::StartsWith(primitive, "fixed[") || !StringUtil::EndsWith(primitive, "]")) {
		return false;
	}
	try {
		length = std::stoull(primitive.substr(6, primitive.size() - 7));
	} catch (...) {
		return false;
	}
	return true;
}

string IcebergTypes::NormalizePrimitive(const string &primitive) {
	idx_t precision, scale;
	if (ParseDecimal(primitive, precision, scale)) {
		return StringUtil::Format("decimal(%d, %d)", precision, scale);
	}
	return primitive;
}

LogicalType IcebergTypes::ToLogicalType(const IcebergType &type) {
	switch (type.kind) {
	case IcebergType::Kind::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &field : type.fields) {
			children.emplace_back(field.name, ToLogicalType(field.type));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case IcebergType::Kind::LIST:
		return LogicalType::LIST(ToLogicalType(type.fields[0].type));
	case IcebergType::Kind::MAP:
		return LogicalType::MAP(ToLogicalType(type.fields[0].type), ToLogicalType(type.fields[1].type));
	default:
		break;
	}
	auto &name = type.primitive;
	idx_t precision, scale, length;
	if (name == "boolean") {
		return LogicalType::BOOLEAN;
	} else if (name == "int") {
		return LogicalType::INTEGER;
	} else if (name == "long") {
		return LogicalType::BIGINT;
	} else if (name == "float") {
		return LogicalType::FLOAT;
	} else if (name == "double") {
		return LogicalType::DOUBLE;
	} else if (name == "date") {
		return LogicalType::DATE;
	} else if (name == "time") {
		return LogicalType::TIME;
	} else if (name == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (name == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (name == "timestamp_ns") {
		return LogicalType::TIMESTAMP_NS;
	} else if (name == "string") {
		return LogicalType::VARCHAR;
	} else if (name == "uuid") {
		return LogicalType::UUID;
	} else if (name == "binary" || ParseFixed(name, length)) {
		return LogicalType::BLOB;
	} else if (ParseDecimal(name, precision, scale)) {
		return LogicalType::DECIMAL(NumericCast<uint8_t>(precision), NumericCast<uint8_t>(scale));
	}
	throw NotImplementedException("The Iceberg type \"%s\" is not supported by DuckLake", name);
}

string IcebergTypes::ToSQLType(const IcebergType &type) {
	return ToLogicalType(type).ToString();
}

bool IcebergTypes::IsNested(const IcebergType &type) {
	return type.kind != IcebergType::Kind::PRIMITIVE;
}

//===--------------------------------------------------------------------===//
// JSON
//===--------------------------------------------------------------------===//
JSONMutableValue IcebergTypes::FieldToJson(JSONWriter &writer, const IcebergField &field) {
	auto result = writer.CreateObject();
	result.Add("id", writer.CreateSignedInteger(NumericCast<int64_t>(field.id)));
	result.AddString("name", field.name);
	result.Add("required", writer.CreateBoolean(field.required));
	result.Add("type", TypeToJson(writer, field.type));
	if (!field.doc.empty()) {
		result.AddString("doc", field.doc);
	}
	return result;
}

JSONMutableValue IcebergTypes::TypeToJson(JSONWriter &writer, const IcebergType &type) {
	switch (type.kind) {
	case IcebergType::Kind::PRIMITIVE:
		return writer.CreateString(type.primitive);
	case IcebergType::Kind::STRUCT: {
		auto result = writer.CreateObject();
		result.AddString("type", "struct");
		auto fields = writer.CreateArray();
		for (auto &field : type.fields) {
			fields.Append(FieldToJson(writer, field));
		}
		result.Add("fields", fields);
		return result;
	}
	case IcebergType::Kind::LIST: {
		auto result = writer.CreateObject();
		auto &element = type.fields[0];
		result.AddString("type", "list");
		result.Add("element-id", writer.CreateSignedInteger(NumericCast<int64_t>(element.id)));
		result.Add("element-required", writer.CreateBoolean(element.required));
		result.Add("element", TypeToJson(writer, element.type));
		return result;
	}
	case IcebergType::Kind::MAP: {
		auto result = writer.CreateObject();
		auto &key = type.fields[0];
		auto &value = type.fields[1];
		result.AddString("type", "map");
		result.Add("key-id", writer.CreateSignedInteger(NumericCast<int64_t>(key.id)));
		result.Add("key", TypeToJson(writer, key.type));
		result.Add("value-id", writer.CreateSignedInteger(NumericCast<int64_t>(value.id)));
		result.Add("value-required", writer.CreateBoolean(value.required));
		result.Add("value", TypeToJson(writer, value.type));
		return result;
	}
	}
	throw InternalException("Unknown Iceberg type kind");
}

JSONMutableValue IcebergTypes::SchemaToJson(JSONWriter &writer, const IcebergSchema &schema) {
	auto result = writer.CreateObject();
	result.AddString("type", "struct");
	result.Add("schema-id", writer.CreateSignedInteger(NumericCast<int64_t>(schema.schema_id)));
	auto fields = writer.CreateArray();
	for (auto &field : schema.fields) {
		fields.Append(FieldToJson(writer, field));
	}
	result.Add("fields", fields);
	if (!schema.identifier_field_ids.empty()) {
		auto ids = writer.CreateArray();
		for (auto id : schema.identifier_field_ids) {
			ids.Append(writer.CreateSignedInteger(NumericCast<int64_t>(id)));
		}
		result.Add("identifier-field-ids", ids);
	}
	return result;
}

string IcebergTypes::SchemaToString(const IcebergSchema &schema) {
	RESTJsonWriter writer;
	writer.SetRoot(SchemaToJson(writer.writer, schema));
	return writer.ToString();
}

IcebergType IcebergTypes::TypeFromJson(RESTJsonIn json) {
	IcebergType result;
	if (json.IsString()) {
		result.kind = IcebergType::Kind::PRIMITIVE;
		result.primitive = NormalizePrimitive(json.Str());
		return result;
	}
	if (!json.IsObject()) {
		throw InvalidInputException("Invalid Iceberg type");
	}
	auto kind = json.Str("type");
	if (kind == "struct") {
		result.kind = IcebergType::Kind::STRUCT;
		for (auto &field : json.Get("fields").Items()) {
			result.fields.push_back(FieldFromJson(field));
		}
	} else if (kind == "list") {
		result.kind = IcebergType::Kind::LIST;
		IcebergField element;
		element.id = json.UInt("element-id");
		element.name = "element";
		element.required = json.Bool("element-required", true);
		element.type = TypeFromJson(json.Get("element"));
		result.fields.push_back(std::move(element));
	} else if (kind == "map") {
		result.kind = IcebergType::Kind::MAP;
		IcebergField key;
		key.id = json.UInt("key-id");
		key.name = "key";
		key.required = true;
		key.type = TypeFromJson(json.Get("key"));
		IcebergField value;
		value.id = json.UInt("value-id");
		value.name = "value";
		value.required = json.Bool("value-required", true);
		value.type = TypeFromJson(json.Get("value"));
		result.fields.push_back(std::move(key));
		result.fields.push_back(std::move(value));
	} else {
		throw InvalidInputException("Unknown Iceberg type \"%s\"", kind);
	}
	return result;
}

IcebergField IcebergTypes::FieldFromJson(RESTJsonIn json) {
	IcebergField result;
	result.id = json.UInt("id");
	result.name = json.Str("name");
	result.required = json.Bool("required", false);
	result.type = TypeFromJson(json.Get("type"));
	result.doc = json.Str("doc", "");
	return result;
}

IcebergSchema IcebergTypes::SchemaFromJson(RESTJsonIn json) {
	IcebergSchema result;
	result.schema_id = json.UInt("schema-id", 0);
	for (auto &field : json.Get("fields").Items()) {
		result.fields.push_back(FieldFromJson(field));
	}
	auto identifier_ids = json.TryGet("identifier-field-ids");
	if (identifier_ids.IsArray()) {
		for (auto &id : identifier_ids.Items()) {
			result.identifier_field_ids.push_back(id.UInt());
		}
	}
	return result;
}

} // namespace duckdb
