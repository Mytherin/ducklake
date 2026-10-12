#include "rest/ducklake_rest_json.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include <cmath>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
RESTJsonOut RESTJsonOut::Obj(const string &key) {
	auto result = writer->CreateObject();
	val.Add(key, result);
	return RESTJsonOut(*writer, result);
}

RESTJsonOut RESTJsonOut::Arr(const string &key) {
	auto result = writer->CreateArray();
	val.Add(key, result);
	return RESTJsonOut(*writer, result);
}

void RESTJsonOut::SetStr(const string &key, const string &value) {
	val.AddString(key, value);
}

void RESTJsonOut::SetUInt(const string &key, idx_t value) {
	val.Add(key, writer->CreateUnsignedInteger(value));
}

void RESTJsonOut::SetInt(const string &key, int64_t value) {
	val.Add(key, writer->CreateSignedInteger(value));
}

void RESTJsonOut::SetBool(const string &key, bool value) {
	val.Add(key, writer->CreateBoolean(value));
}

void RESTJsonOut::SetDouble(const string &key, double value) {
	val.Add(key, writer->CreateDouble(value));
}

void RESTJsonOut::SetNull(const string &key) {
	val.Add(key, writer->CreateNull());
}

void RESTJsonOut::SetOpt(const string &key, optional_idx value) {
	if (value.IsValid()) {
		SetUInt(key, value.GetIndex());
	} else {
		SetNull(key);
	}
}

void RESTJsonOut::SetValue(const string &key, const Value &value) {
	val.Add(key, RESTJson::ValueToJson(*writer, value));
}

void RESTJsonOut::SetRaw(const string &key, JSONMutableValue value) {
	val.Add(key, value);
}

RESTJsonOut RESTJsonOut::AppendObj() {
	auto result = writer->CreateObject();
	val.Append(result);
	return RESTJsonOut(*writer, result);
}

RESTJsonOut RESTJsonOut::AppendArr() {
	auto result = writer->CreateArray();
	val.Append(result);
	return RESTJsonOut(*writer, result);
}

void RESTJsonOut::AppendStr(const string &value) {
	val.AppendString(value);
}

void RESTJsonOut::AppendUInt(idx_t value) {
	val.Append(writer->CreateUnsignedInteger(value));
}

void RESTJsonOut::AppendRaw(JSONMutableValue value) {
	val.Append(value);
}

RESTJsonWriter::RESTJsonWriter() {
	root = writer.CreateObject();
	writer.SetRoot(root);
}

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
RESTJsonReader::RESTJsonReader(const string &json) {
	JSONParseError error;
	doc = JSONDocument::TryParse(json.c_str(), json.size(), error, JSONReadFlags::ALLOW_INVALID_UNICODE);
	if (!doc) {
		throw InvalidInputException("Failed to parse DuckLake REST JSON: %s at position %llu", error.message,
		                            error.position);
	}
}

bool RESTJsonIn::IsNull() const {
	return !val.IsValid() || val.IsNull();
}

bool RESTJsonIn::IsObject() const {
	return val.IsValid() && val.IsObject();
}

bool RESTJsonIn::IsArray() const {
	return val.IsValid() && val.IsArray();
}

bool RESTJsonIn::IsString() const {
	return val.IsValid() && val.IsString();
}

bool RESTJsonIn::Has(const string &key) const {
	return IsObject() && val.GetMember(key).IsValid();
}

RESTJsonIn RESTJsonIn::Get(const string &key) const {
	if (!IsObject()) {
		throw InvalidInputException("DuckLake REST: expected a JSON object when reading \"%s\"", key);
	}
	auto result = val.GetMember(key);
	if (!result.IsValid()) {
		throw InvalidInputException("DuckLake REST: missing required field \"%s\"", key);
	}
	return RESTJsonIn(result);
}

RESTJsonIn RESTJsonIn::TryGet(const string &key) const {
	if (!IsObject()) {
		return RESTJsonIn();
	}
	return RESTJsonIn(val.GetMember(key));
}

vector<RESTJsonIn> RESTJsonIn::Items() const {
	vector<RESTJsonIn> result;
	if (IsNull()) {
		return result;
	}
	if (!IsArray()) {
		throw InvalidInputException("DuckLake REST: expected a JSON array");
	}
	val.IterateArray([&](JSONValue entry) { result.emplace_back(entry); });
	return result;
}

vector<pair<string, RESTJsonIn>> RESTJsonIn::Members() const {
	vector<pair<string, RESTJsonIn>> result;
	if (IsNull()) {
		return result;
	}
	if (!IsObject()) {
		throw InvalidInputException("DuckLake REST: expected a JSON object");
	}
	val.IterateObject([&](const string &key, JSONValue entry) { result.emplace_back(key, RESTJsonIn(entry)); });
	return result;
}

string RESTJsonIn::Str() const {
	if (!IsString()) {
		throw InvalidInputException("DuckLake REST: expected a JSON string");
	}
	return val.GetString();
}

idx_t RESTJsonIn::UInt() const {
	if (!val.IsValid() || !val.IsInteger()) {
		throw InvalidInputException("DuckLake REST: expected a JSON integer");
	}
	if (val.GetType() == JSONValueType::SIGNED_INTEGER) {
		auto result = val.GetSignedInteger();
		if (result < 0) {
			throw InvalidInputException("DuckLake REST: expected a non-negative JSON integer");
		}
		return UnsafeNumericCast<idx_t>(result);
	}
	return val.GetUnsignedInteger();
}

int64_t RESTJsonIn::Int() const {
	if (!val.IsValid() || !val.IsInteger()) {
		throw InvalidInputException("DuckLake REST: expected a JSON integer");
	}
	if (val.GetType() == JSONValueType::UNSIGNED_INTEGER) {
		return NumericCast<int64_t>(val.GetUnsignedInteger());
	}
	return val.GetSignedInteger();
}

double RESTJsonIn::Double() const {
	if (!val.IsValid() || !val.IsNumber()) {
		throw InvalidInputException("DuckLake REST: expected a JSON number");
	}
	return val.GetNumber();
}

bool RESTJsonIn::Bool() const {
	if (!val.IsValid() || val.GetType() != JSONValueType::BOOLEAN) {
		throw InvalidInputException("DuckLake REST: expected a JSON boolean");
	}
	return val.GetBoolean();
}

string RESTJsonIn::Str(const string &key) const {
	return Get(key).Str();
}

string RESTJsonIn::Str(const string &key, const string &default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.Str();
}

idx_t RESTJsonIn::UInt(const string &key) const {
	return Get(key).UInt();
}

idx_t RESTJsonIn::UInt(const string &key, idx_t default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.UInt();
}

int64_t RESTJsonIn::Int(const string &key) const {
	return Get(key).Int();
}

bool RESTJsonIn::Bool(const string &key, bool default_value) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? default_value : entry.Bool();
}

optional_idx RESTJsonIn::Opt(const string &key) const {
	auto entry = TryGet(key);
	return entry.IsNull() ? optional_idx() : optional_idx(entry.UInt());
}

Value RESTJsonIn::GetValue(const string &key) const {
	return RESTJson::ValueFromJson(TryGet(key));
}

//===--------------------------------------------------------------------===//
// Values and types
//===--------------------------------------------------------------------===//
JSONMutableValue RESTJson::ValueToJson(JSONWriter &writer, const Value &value) {
	if (value.IsNull()) {
		return writer.CreateNull();
	}
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		return writer.CreateString(StringValue::Get(value));
	}
	auto result = writer.CreateObject();
	result.Add("type", TypeToJson(writer, value.type()));
	result.Add("value", TypedValueToJson(writer, value));
	return result;
}

Value RESTJson::ValueFromJson(RESTJsonIn json) {
	if (json.IsNull()) {
		return Value();
	}
	switch (json.val.GetType()) {
	case JSONValueType::STRING:
		return Value(json.Str());
	case JSONValueType::OBJECT:
		return TypedValueFromJson(json.Get("value"), TypeFromJson(json.Get("type")));
	case JSONValueType::BOOLEAN:
		return Value::BOOLEAN(json.Bool());
	case JSONValueType::SIGNED_INTEGER:
	case JSONValueType::UNSIGNED_INTEGER:
		return Value::BIGINT(json.Int());
	default:
		return Value::DOUBLE(json.Double());
	}
}

JSONMutableValue RESTJson::TypeToJson(JSONWriter &writer, const LogicalType &type) {
	auto result = writer.CreateObject();
	RESTJsonOut out(writer, result);
	out.SetStr("id", EnumUtil::ToString(type.id()));
	if (type.HasAlias()) {
		out.SetStr("alias", type.GetAlias());
	}
	switch (type.id()) {
	case LogicalTypeId::GEOMETRY:
		if (GeoType::HasCRS(type)) {
			out.SetStr("crs", GeoType::GetCRS(type).GetDefinition());
		}
		break;
	case LogicalTypeId::DECIMAL:
		out.SetUInt("width", DecimalType::GetWidth(type));
		out.SetUInt("scale", DecimalType::GetScale(type));
		break;
	case LogicalTypeId::LIST:
		out.SetRaw("child", TypeToJson(writer, ListType::GetChildType(type)));
		break;
	case LogicalTypeId::ARRAY:
		out.SetRaw("child", TypeToJson(writer, ArrayType::GetChildType(type)));
		out.SetUInt("size", ArrayType::GetSize(type));
		break;
	case LogicalTypeId::MAP:
		out.SetRaw("key", TypeToJson(writer, MapType::KeyType(type)));
		out.SetRaw("value", TypeToJson(writer, MapType::ValueType(type)));
		break;
	case LogicalTypeId::STRUCT: {
		auto children = out.Arr("children");
		for (auto &child : StructType::GetChildTypes(type)) {
			auto child_obj = children.AppendObj();
			child_obj.SetStr("name", child.first.GetIdentifierName());
			child_obj.SetRaw("type", TypeToJson(writer, child.second));
		}
		break;
	}
	default:
		break;
	}
	return result;
}

static LogicalType TypeFromJsonInternal(RESTJsonIn json) {
	auto id = EnumUtil::FromString<LogicalTypeId>(json.Str("id"));
	switch (id) {
	case LogicalTypeId::VARIANT:
		return LogicalType::VARIANT();
	case LogicalTypeId::GEOMETRY:
		return LogicalType::GEOMETRY(json.Str("crs", string()));
	case LogicalTypeId::DECIMAL:
		return LogicalType::DECIMAL(NumericCast<uint8_t>(json.UInt("width")), NumericCast<uint8_t>(json.UInt("scale")));
	case LogicalTypeId::LIST:
		return LogicalType::LIST(RESTJson::TypeFromJson(json.Get("child")));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(RESTJson::TypeFromJson(json.Get("child")), json.UInt("size"));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(RESTJson::TypeFromJson(json.Get("key")), RESTJson::TypeFromJson(json.Get("value")));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : json.Get("children").Items()) {
			children.emplace_back(Identifier(child.Str("name")), RESTJson::TypeFromJson(child.Get("type")));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	default:
		return LogicalType(id);
	}
}

LogicalType RESTJson::TypeFromJson(RESTJsonIn json) {
	auto result = TypeFromJsonInternal(json);
	auto alias = json.Str("alias", string());
	if (!alias.empty()) {
		result = result.WithAlias(std::move(alias));
	}
	return result;
}

JSONMutableValue RESTJson::TypedValueToJson(JSONWriter &writer, const Value &value) {
	if (value.IsNull()) {
		return writer.CreateNull();
	}
	auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return writer.CreateBoolean(BooleanValue::Get(value));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return writer.CreateSignedInteger(value.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return writer.CreateUnsignedInteger(value.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE: {
		auto dbl = value.GetValue<double>();
		if (std::isfinite(dbl)) {
			return writer.CreateDouble(dbl);
		}
		break;
	}
	case LogicalTypeId::VARCHAR:
		return writer.CreateString(StringValue::Get(value));
	case LogicalTypeId::INTERVAL: {
		auto interval = IntervalValue::Get(value);
		auto result = writer.CreateObject();
		result.Add("months", writer.CreateSignedInteger(interval.months));
		result.Add("days", writer.CreateSignedInteger(interval.days));
		result.Add("micros", writer.CreateSignedInteger(interval.micros));
		return result;
	}
	case LogicalTypeId::VARIANT: {
		// the value the variant holds, with its type
		Vector variant_vector(value, count_t(1));
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(variant_vector, format);
		UnifiedVariantVectorData variant_data(format);
		return ValueToJson(writer, VariantUtils::ConvertVariantToValue(variant_data, 0, 0));
	}
	case LogicalTypeId::STRUCT: {
		auto result = writer.CreateObject();
		auto &child_types = StructType::GetChildTypes(type);
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			result.Add(child_types[i].first.GetIdentifierName(), TypedValueToJson(writer, children[i]));
		}
		return result;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		auto result = writer.CreateArray();
		auto &children =
		    type.id() == LogicalTypeId::LIST ? ListValue::GetChildren(value) : ArrayValue::GetChildren(value);
		for (auto &child : children) {
			result.Append(TypedValueToJson(writer, child));
		}
		return result;
	}
	case LogicalTypeId::MAP: {
		auto result = writer.CreateArray();
		for (auto &entry : MapValue::GetChildren(value)) {
			auto &kv = StructValue::GetChildren(entry);
			auto obj = writer.CreateObject();
			obj.Add("key", TypedValueToJson(writer, kv[0]));
			obj.Add("value", TypedValueToJson(writer, kv[1]));
			result.Append(obj);
		}
		return result;
	}
	default:
		break;
	}
	return writer.CreateString(value.ToString());
}

//! Variants keep the types of the values they hold, also of nested values
static Value VariantFromValue(const Value &value) {
	if (value.IsNull()) {
		return Value(LogicalType::VARIANT());
	}
	switch (value.type().id()) {
	case LogicalTypeId::STRUCT: {
		child_list_t<Value> children;
		auto &values = StructValue::GetChildren(value);
		for (idx_t i = 0; i < values.size(); i++) {
			children.emplace_back(StructType::GetChildName(value.type(), i), VariantFromValue(values[i]));
		}
		return Value::STRUCT(std::move(children)).DefaultCastAs(LogicalType::VARIANT());
	}
	case LogicalTypeId::LIST: {
		vector<Value> children;
		for (auto &child : ListValue::GetChildren(value)) {
			children.push_back(VariantFromValue(child));
		}
		return Value::LIST(LogicalType::VARIANT(), std::move(children)).DefaultCastAs(LogicalType::VARIANT());
	}
	default:
		return value.DefaultCastAs(LogicalType::VARIANT());
	}
}

Value RESTJson::TypedValueFromJson(RESTJsonIn json, const LogicalType &type) {
	if (json.IsNull()) {
		return Value(type);
	}
	switch (type.id()) {
	case LogicalTypeId::INTERVAL: {
		if (!json.IsObject()) {
			break;
		}
		interval_t interval;
		interval.months = NumericCast<int32_t>(json.Int("months"));
		interval.days = NumericCast<int32_t>(json.Int("days"));
		interval.micros = json.Int("micros");
		return Value::INTERVAL(interval);
	}
	case LogicalTypeId::VARIANT:
		return VariantFromValue(ValueFromJson(json));
	case LogicalTypeId::STRUCT: {
		if (!json.IsObject()) {
			break;
		}
		vector<Value> children;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.push_back(TypedValueFromJson(json.TryGet(child.first.GetIdentifierName()), child.second));
		}
		return Value::STRUCT(type, std::move(children));
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		if (!json.IsArray()) {
			break;
		}
		auto is_list = type.id() == LogicalTypeId::LIST;
		auto &child_type = is_list ? ListType::GetChildType(type) : ArrayType::GetChildType(type);
		vector<Value> children;
		for (auto &child : json.Items()) {
			children.push_back(TypedValueFromJson(child, child_type));
		}
		return is_list ? Value::LIST(child_type, std::move(children)) : Value::ARRAY(child_type, std::move(children));
	}
	case LogicalTypeId::MAP: {
		if (!json.IsArray()) {
			break;
		}
		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		vector<Value> keys;
		vector<Value> values;
		for (auto &entry : json.Items()) {
			keys.push_back(TypedValueFromJson(entry.Get("key"), key_type));
			values.push_back(TypedValueFromJson(entry.TryGet("value"), value_type));
		}
		return Value::MAP(key_type, value_type, std::move(keys), std::move(values));
	}
	default:
		break;
	}
	Value result;
	switch (json.val.GetType()) {
	case JSONValueType::STRING:
		result = Value(json.Str());
		break;
	case JSONValueType::BOOLEAN:
		result = Value::BOOLEAN(json.Bool());
		break;
	case JSONValueType::UNSIGNED_INTEGER:
		result = Value::UBIGINT(json.val.GetUnsignedInteger());
		break;
	case JSONValueType::SIGNED_INTEGER:
		result = Value::BIGINT(json.val.GetSignedInteger());
		break;
	case JSONValueType::DOUBLE:
		result = Value::DOUBLE(json.val.GetDouble());
		break;
	default:
		throw InvalidInputException("DuckLake REST: cannot convert JSON value to type %s", type.ToString());
	}
	if (result.type() == type) {
		return result;
	}
	return result.DefaultCastAs(type);
}

} // namespace duckdb
