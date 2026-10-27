#include "iceberg/iceberg_avro.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "miniz.hpp"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <random>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Minimal JSON parser (keeps the source span of every value)
//===--------------------------------------------------------------------===//
namespace {

enum class JKind : uint8_t { NUL, BOOL, NUMBER, STRING, ARRAY, OBJECT };

struct JNode {
	JKind kind = JKind::NUL;
	//! Decoded string value (STRING)
	string str;
	vector<JNode> items;
	vector<pair<string, JNode>> members;
	idx_t begin = 0;
	idx_t end = 0;

	const JNode *Find(const string &key) const {
		for (auto &member : members) {
			if (member.first == key) {
				return &member.second;
			}
		}
		return nullptr;
	}
};

class JParser {
public:
	explicit JParser(const string &text_p) : text(text_p) {
	}

	JNode ParseDocument() {
		auto result = ParseValue(0);
		SkipWs();
		if (pos != text.size()) {
			Fail("trailing characters");
		}
		return result;
	}

	string Raw(const JNode &node) const {
		return text.substr(node.begin, node.end - node.begin);
	}

private:
	[[noreturn]] void Fail(const string &msg) const {
		throw InvalidInputException("Invalid Avro schema JSON at offset %llu: %s", static_cast<uint64_t>(pos), msg);
	}
	void SkipWs() {
		while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\n' || text[pos] == '\r')) {
			pos++;
		}
	}
	bool Consume(char c) {
		SkipWs();
		if (pos < text.size() && text[pos] == c) {
			pos++;
			return true;
		}
		return false;
	}
	void Expect(char c) {
		if (!Consume(c)) {
			Fail(string("expected '") + c + "'");
		}
	}
	void ExpectWord(const char *word) {
		auto len = strlen(word);
		if (text.compare(pos, len, word) != 0) {
			Fail("unexpected token");
		}
		pos += len;
	}
	static void AppendUtf8(string &out, uint32_t cp) {
		if (cp < 0x80) {
			out += static_cast<char>(cp);
		} else if (cp < 0x800) {
			out += static_cast<char>(0xC0 | (cp >> 6));
			out += static_cast<char>(0x80 | (cp & 0x3F));
		} else if (cp < 0x10000) {
			out += static_cast<char>(0xE0 | (cp >> 12));
			out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
			out += static_cast<char>(0x80 | (cp & 0x3F));
		} else {
			out += static_cast<char>(0xF0 | (cp >> 18));
			out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
			out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
			out += static_cast<char>(0x80 | (cp & 0x3F));
		}
	}
	uint32_t ParseHex4() {
		if (pos + 4 > text.size()) {
			Fail("truncated unicode escape");
		}
		uint32_t value = 0;
		for (idx_t i = 0; i < 4; i++) {
			char c = text[pos++];
			value <<= 4;
			if (c >= '0' && c <= '9') {
				value |= static_cast<uint32_t>(c - '0');
			} else if (c >= 'a' && c <= 'f') {
				value |= static_cast<uint32_t>(c - 'a' + 10);
			} else if (c >= 'A' && c <= 'F') {
				value |= static_cast<uint32_t>(c - 'A' + 10);
			} else {
				Fail("invalid unicode escape");
			}
		}
		return value;
	}
	string ParseString() {
		Expect('"');
		string out;
		while (true) {
			if (pos >= text.size()) {
				Fail("unterminated string");
			}
			char c = text[pos++];
			if (c == '"') {
				break;
			}
			if (c != '\\') {
				out += c;
				continue;
			}
			if (pos >= text.size()) {
				Fail("unterminated escape");
			}
			char e = text[pos++];
			switch (e) {
			case 'n':
				out += '\n';
				break;
			case 't':
				out += '\t';
				break;
			case 'r':
				out += '\r';
				break;
			case 'b':
				out += '\b';
				break;
			case 'f':
				out += '\f';
				break;
			case 'u': {
				uint32_t cp = ParseHex4();
				if (cp >= 0xD800 && cp < 0xDC00 && text.compare(pos, 2, "\\u") == 0) {
					pos += 2;
					uint32_t low = ParseHex4();
					cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
				}
				AppendUtf8(out, cp);
				break;
			}
			default:
				out += e;
				break;
			}
		}
		return out;
	}
	JNode ParseValue(idx_t depth) {
		if (depth > 256) {
			Fail("nesting too deep");
		}
		SkipWs();
		if (pos >= text.size()) {
			Fail("unexpected end of input");
		}
		JNode node;
		node.begin = pos;
		char c = text[pos];
		if (c == '{') {
			pos++;
			node.kind = JKind::OBJECT;
			if (!Consume('}')) {
				do {
					SkipWs();
					auto key = ParseString();
					Expect(':');
					node.members.emplace_back(std::move(key), ParseValue(depth + 1));
				} while (Consume(','));
				Expect('}');
			}
		} else if (c == '[') {
			pos++;
			node.kind = JKind::ARRAY;
			if (!Consume(']')) {
				do {
					node.items.push_back(ParseValue(depth + 1));
				} while (Consume(','));
				Expect(']');
			}
		} else if (c == '"') {
			node.kind = JKind::STRING;
			node.str = ParseString();
		} else if (c == 't') {
			ExpectWord("true");
			node.kind = JKind::BOOL;
		} else if (c == 'f') {
			ExpectWord("false");
			node.kind = JKind::BOOL;
		} else if (c == 'n') {
			ExpectWord("null");
			node.kind = JKind::NUL;
		} else if (c == '-' || (c >= '0' && c <= '9')) {
			node.kind = JKind::NUMBER;
			while (pos < text.size() && (text[pos] == '-' || text[pos] == '+' || text[pos] == '.' || text[pos] == 'e' ||
			                             text[pos] == 'E' || (text[pos] >= '0' && text[pos] <= '9'))) {
				pos++;
			}
		} else {
			Fail("unexpected character");
		}
		node.end = pos;
		return node;
	}

private:
	const string &text;
	idx_t pos = 0;
};

//===--------------------------------------------------------------------===//
// Schema parsing
//===--------------------------------------------------------------------===//
class SchemaParser {
public:
	explicit SchemaParser(const string &json) : parser(json) {
	}

	shared_ptr<AvroSchemaNode> Parse() {
		auto doc = parser.ParseDocument();
		return ParseType(doc, string());
	}

private:
	static bool PrimitiveType(const string &name, AvroType &type) {
		static const pair<const char *, AvroType> PRIMITIVES[] = {
		    {"null", AvroType::NULL_},  {"boolean", AvroType::BOOLEAN}, {"int", AvroType::INT},
		    {"long", AvroType::LONG},   {"float", AvroType::FLOAT},     {"double", AvroType::DOUBLE},
		    {"bytes", AvroType::BYTES}, {"string", AvroType::STRING}};
		for (auto &entry : PRIMITIVES) {
			if (name == entry.first) {
				type = entry.second;
				return true;
			}
		}
		return false;
	}

	static string Qualify(const string &name, const string &ns) {
		if (name.find('.') != string::npos || ns.empty()) {
			return name;
		}
		return ns + "." + name;
	}

	static string NamespaceOf(const string &full_name, const string &fallback) {
		auto dot = full_name.rfind('.');
		return dot == string::npos ? fallback : full_name.substr(0, dot);
	}

	string RawMember(const JNode &obj, const char *key) const {
		auto member = obj.Find(key);
		return member ? parser.Raw(*member) : string();
	}

	string StringMember(const JNode &obj, const char *key) const {
		auto member = obj.Find(key);
		if (!member) {
			return string();
		}
		if (member->kind != JKind::STRING) {
			throw InvalidInputException("Avro schema: attribute \"%s\" must be a string", key);
		}
		return member->str;
	}

	shared_ptr<AvroSchemaNode> ParseType(const JNode &node, const string &ns) {
		switch (node.kind) {
		case JKind::STRING:
			return ParseNamed(node.str, ns);
		case JKind::ARRAY: {
			auto result = make_shared_ptr<AvroSchemaNode>();
			result->type = AvroType::UNION;
			for (auto &item : node.items) {
				result->branches.push_back(ParseType(item, ns));
			}
			return result;
		}
		case JKind::OBJECT:
			return ParseObject(node, ns);
		default:
			throw InvalidInputException("Avro schema: invalid type declaration");
		}
	}

	shared_ptr<AvroSchemaNode> ParseNamed(const string &name, const string &ns) {
		AvroType type;
		if (PrimitiveType(name, type)) {
			auto result = make_shared_ptr<AvroSchemaNode>();
			result->type = type;
			return result;
		}
		auto it = named.find(Qualify(name, ns));
		if (it == named.end()) {
			it = named.find(name);
		}
		if (it == named.end()) {
			throw InvalidInputException("Avro schema: unknown type name \"%s\"", name);
		}
		return it->second;
	}

	shared_ptr<AvroSchemaNode> ParseObject(const JNode &obj, const string &ns) {
		auto type_node = obj.Find("type");
		if (!type_node) {
			throw InvalidInputException("Avro schema: object without \"type\"");
		}
		if (type_node->kind != JKind::STRING) {
			// {"type": {...}} or {"type": [...]}
			return ParseType(*type_node, ns);
		}
		auto &type_name = type_node->str;
		auto result = make_shared_ptr<AvroSchemaNode>();
		result->logical_type = StringMember(obj, "logicalType");
		AvroType primitive;
		if (PrimitiveType(type_name, primitive)) {
			result->type = primitive;
			return result;
		}
		if (type_name == "record" || type_name == "error") {
			result->type = AvroType::RECORD;
			auto name = StringMember(obj, "name");
			auto explicit_ns = obj.Find("namespace");
			string inner_ns = ns;
			if (name.find('.') != string::npos) {
				inner_ns = NamespaceOf(name, ns);
			} else if (explicit_ns && explicit_ns->kind == JKind::STRING) {
				inner_ns = explicit_ns->str;
			}
			result->name = Qualify(name, name.find('.') != string::npos ? string() : inner_ns);
			named[result->name] = result;
			auto fields = obj.Find("fields");
			if (!fields || fields->kind != JKind::ARRAY) {
				throw InvalidInputException("Avro schema: record \"%s\" has no fields array", name);
			}
			for (auto &field_node : fields->items) {
				if (field_node.kind != JKind::OBJECT) {
					throw InvalidInputException("Avro schema: record field must be an object");
				}
				AvroField field;
				field.name = StringMember(field_node, "name");
				auto field_type = field_node.Find("type");
				if (!field_type) {
					throw InvalidInputException("Avro schema: field \"%s\" has no type", field.name);
				}
				field.schema = ParseType(*field_type, inner_ns);
				field.default_json = RawMember(field_node, "default");
				field.field_id = RawMember(field_node, "field-id");
				result->fields.push_back(std::move(field));
			}
			return result;
		}
		if (type_name == "enum") {
			result->type = AvroType::ENUM;
			auto name = StringMember(obj, "name");
			auto explicit_ns = obj.Find("namespace");
			string inner_ns = explicit_ns && explicit_ns->kind == JKind::STRING ? explicit_ns->str : ns;
			result->name = Qualify(name, inner_ns);
			auto symbols = obj.Find("symbols");
			if (!symbols || symbols->kind != JKind::ARRAY) {
				throw InvalidInputException("Avro schema: enum \"%s\" has no symbols array", name);
			}
			for (auto &symbol : symbols->items) {
				result->symbols.push_back(symbol.str);
			}
			named[result->name] = result;
			return result;
		}
		if (type_name == "fixed") {
			result->type = AvroType::FIXED;
			auto name = StringMember(obj, "name");
			auto explicit_ns = obj.Find("namespace");
			string inner_ns = explicit_ns && explicit_ns->kind == JKind::STRING ? explicit_ns->str : ns;
			result->name = Qualify(name, inner_ns);
			auto size = RawMember(obj, "size");
			if (size.empty()) {
				throw InvalidInputException("Avro schema: fixed \"%s\" has no size", name);
			}
			result->fixed_size = NumericCast<idx_t>(std::stoll(size));
			named[result->name] = result;
			return result;
		}
		if (type_name == "array") {
			result->type = AvroType::ARRAY;
			auto items = obj.Find("items");
			if (!items) {
				throw InvalidInputException("Avro schema: array without items");
			}
			result->child = ParseType(*items, ns);
			result->element_id = RawMember(obj, "element-id");
			return result;
		}
		if (type_name == "map") {
			result->type = AvroType::MAP;
			auto values = obj.Find("values");
			if (!values) {
				throw InvalidInputException("Avro schema: map without values");
			}
			result->child = ParseType(*values, ns);
			result->key_id = RawMember(obj, "key-id");
			result->value_id = RawMember(obj, "value-id");
			return result;
		}
		return ParseNamed(type_name, ns);
	}

private:
	JParser parser;
	unordered_map<string, shared_ptr<AvroSchemaNode>> named;
};

} // namespace

idx_t AvroSchemaNode::FieldIndex(const string &field_name) const {
	for (idx_t i = 0; i < fields.size(); i++) {
		if (fields[i].name == field_name) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

AvroSchema AvroSchema::Parse(const string &json) {
	AvroSchema result;
	SchemaParser parser(json);
	result.root = parser.Parse();
	result.json = json;
	return result;
}

//===--------------------------------------------------------------------===//
// AvroValue
//===--------------------------------------------------------------------===//
AvroValue AvroValue::Null() {
	return AvroValue();
}

AvroValue AvroValue::Bool(bool value) {
	AvroValue result;
	result.kind = AvroValueKind::BOOL;
	result.int_value = value ? 1 : 0;
	return result;
}

AvroValue AvroValue::Int(int32_t value) {
	AvroValue result;
	result.kind = AvroValueKind::INT;
	result.int_value = value;
	return result;
}

AvroValue AvroValue::Long(int64_t value) {
	AvroValue result;
	result.kind = AvroValueKind::LONG;
	result.int_value = value;
	return result;
}

AvroValue AvroValue::Float(float value) {
	AvroValue result;
	result.kind = AvroValueKind::FLOAT;
	result.double_value = value;
	return result;
}

AvroValue AvroValue::Double(double value) {
	AvroValue result;
	result.kind = AvroValueKind::DOUBLE;
	result.double_value = value;
	return result;
}

AvroValue AvroValue::Bytes(string value) {
	AvroValue result;
	result.kind = AvroValueKind::BYTES;
	result.str_value = std::move(value);
	return result;
}

AvroValue AvroValue::String(string value) {
	AvroValue result;
	result.kind = AvroValueKind::STRING;
	result.str_value = std::move(value);
	return result;
}

AvroValue AvroValue::Record(vector<AvroValue> fields) {
	AvroValue result;
	result.kind = AvroValueKind::RECORD;
	result.children = std::move(fields);
	return result;
}

AvroValue AvroValue::Array(vector<AvroValue> items) {
	AvroValue result;
	result.kind = AvroValueKind::ARRAY;
	result.children = std::move(items);
	return result;
}

AvroValue AvroValue::Map(vector<string> keys, vector<AvroValue> values) {
	if (keys.size() != values.size()) {
		throw InternalException("AvroValue::Map: key/value count mismatch");
	}
	AvroValue result;
	result.kind = AvroValueKind::MAP;
	result.keys = std::move(keys);
	result.children = std::move(values);
	return result;
}

AvroValue AvroValue::Enum(idx_t index, string symbol) {
	AvroValue result;
	result.kind = AvroValueKind::ENUM;
	result.int_value = NumericCast<int64_t>(index);
	result.str_value = std::move(symbol);
	return result;
}

AvroValue AvroValue::Fixed(string value) {
	AvroValue result;
	result.kind = AvroValueKind::FIXED;
	result.str_value = std::move(value);
	return result;
}

AvroValue AvroValue::Union(idx_t branch, AvroValue value) {
	AvroValue result;
	result.kind = AvroValueKind::UNION;
	result.int_value = NumericCast<int64_t>(branch);
	result.children.push_back(std::move(value));
	return result;
}

const AvroValue &AvroValue::Unwrap() const {
	auto current = this;
	while (current->kind == AvroValueKind::UNION) {
		current = &current->children[0];
	}
	return *current;
}

bool AvroValue::IsNull() const {
	return Unwrap().kind == AvroValueKind::NULL_;
}

int64_t AvroValue::AsLong() const {
	auto &value = Unwrap();
	switch (value.kind) {
	case AvroValueKind::BOOL:
	case AvroValueKind::INT:
	case AvroValueKind::LONG:
	case AvroValueKind::ENUM:
		return value.int_value;
	default:
		throw InvalidInputException("Avro value is not an integer");
	}
}

bool AvroValue::AsBool() const {
	auto &value = Unwrap();
	if (value.kind != AvroValueKind::BOOL) {
		throw InvalidInputException("Avro value is not a boolean");
	}
	return value.int_value != 0;
}

double AvroValue::AsDouble() const {
	auto &value = Unwrap();
	if (value.kind == AvroValueKind::FLOAT || value.kind == AvroValueKind::DOUBLE) {
		return value.double_value;
	}
	if (value.kind == AvroValueKind::INT || value.kind == AvroValueKind::LONG) {
		return static_cast<double>(value.int_value);
	}
	throw InvalidInputException("Avro value is not a floating point number");
}

const string &AvroValue::AsString() const {
	auto &value = Unwrap();
	switch (value.kind) {
	case AvroValueKind::STRING:
	case AvroValueKind::BYTES:
	case AvroValueKind::FIXED:
	case AvroValueKind::ENUM:
		return value.str_value;
	default:
		throw InvalidInputException("Avro value is not a string");
	}
}

const AvroValue &AvroValue::Get(const AvroSchemaNode &schema, const string &field_name) const {
	auto &value = Unwrap();
	if (value.kind != AvroValueKind::RECORD || schema.type != AvroType::RECORD) {
		throw InvalidInputException("Avro value is not a record");
	}
	auto idx = schema.FieldIndex(field_name);
	if (idx == DConstants::INVALID_INDEX || idx >= value.children.size()) {
		throw InvalidInputException("Avro record has no field \"%s\"", field_name);
	}
	return value.children[idx];
}

const AvroValue &AvroValue::Get(const string &field_name) const {
	auto &value = Unwrap();
	if (!value.record_schema) {
		throw InvalidInputException("Avro record has no schema attached, cannot look up field \"%s\"", field_name);
	}
	return value.Get(*value.record_schema, field_name);
}

const AvroValue &AvroValue::TryGet(const string &field_name) const {
	static const AvroValue NULL_VALUE;
	auto &value = Unwrap();
	if (value.kind != AvroValueKind::RECORD || !value.record_schema) {
		return NULL_VALUE;
	}
	auto idx = value.record_schema->FieldIndex(field_name);
	if (idx == DConstants::INVALID_INDEX || idx >= value.children.size()) {
		return NULL_VALUE;
	}
	return value.children[idx];
}

const AvroValue *AvroValue::MapGet(const string &key) const {
	auto &value = Unwrap();
	if (value.kind != AvroValueKind::MAP) {
		throw InvalidInputException("Avro value is not a map");
	}
	for (idx_t i = 0; i < value.keys.size(); i++) {
		if (value.keys[i] == key) {
			return &value.children[i];
		}
	}
	return nullptr;
}

static void AppendJSONString(string &out, const string &value) {
	out += '"';
	for (char ch : value) {
		auto c = static_cast<unsigned char>(ch);
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out += ch;
			}
			break;
		}
	}
	out += '"';
}

static void AppendJSONDouble(string &out, double value, bool is_float) {
	if (std::isnan(value) || std::isinf(value)) {
		out += std::isnan(value) ? "\"NaN\"" : (value > 0 ? "\"Infinity\"" : "\"-Infinity\"");
		return;
	}
	char buf[40];
	snprintf(buf, sizeof(buf), is_float ? "%.9g" : "%.17g", value);
	out += buf;
}

static void RenderJSON(const AvroValue &input, string &out) {
	auto &value = input.Unwrap();
	switch (value.kind) {
	case AvroValueKind::NULL_:
		out += "null";
		break;
	case AvroValueKind::BOOL:
		out += value.int_value ? "true" : "false";
		break;
	case AvroValueKind::INT:
	case AvroValueKind::LONG:
		out += std::to_string(value.int_value);
		break;
	case AvroValueKind::FLOAT:
		AppendJSONDouble(out, value.double_value, true);
		break;
	case AvroValueKind::DOUBLE:
		AppendJSONDouble(out, value.double_value, false);
		break;
	case AvroValueKind::BYTES:
	case AvroValueKind::FIXED: {
		static const char HEX[] = "0123456789abcdef";
		string hex;
		for (char ch : value.str_value) {
			auto c = static_cast<unsigned char>(ch);
			hex += HEX[c >> 4];
			hex += HEX[c & 15];
		}
		AppendJSONString(out, hex);
		break;
	}
	case AvroValueKind::STRING:
		AppendJSONString(out, value.str_value);
		break;
	case AvroValueKind::ENUM:
		if (value.str_value.empty()) {
			out += std::to_string(value.int_value);
		} else {
			AppendJSONString(out, value.str_value);
		}
		break;
	case AvroValueKind::RECORD: {
		auto schema = value.record_schema.get();
		if (!schema) {
			out += '[';
			for (idx_t i = 0; i < value.children.size(); i++) {
				if (i > 0) {
					out += ',';
				}
				RenderJSON(value.children[i], out);
			}
			out += ']';
			break;
		}
		out += '{';
		for (idx_t i = 0; i < value.children.size(); i++) {
			if (i > 0) {
				out += ',';
			}
			AppendJSONString(out, schema->fields[i].name);
			out += ':';
			RenderJSON(value.children[i], out);
		}
		out += '}';
		break;
	}
	case AvroValueKind::ARRAY:
		out += '[';
		for (idx_t i = 0; i < value.children.size(); i++) {
			if (i > 0) {
				out += ',';
			}
			RenderJSON(value.children[i], out);
		}
		out += ']';
		break;
	case AvroValueKind::MAP:
		out += '{';
		for (idx_t i = 0; i < value.children.size(); i++) {
			if (i > 0) {
				out += ',';
			}
			AppendJSONString(out, value.keys[i]);
			out += ':';
			RenderJSON(value.children[i], out);
		}
		out += '}';
		break;
	case AvroValueKind::UNION:
		break;
	}
}

string AvroValue::ToJSON() const {
	string result;
	RenderJSON(*this, result);
	return result;
}

//===--------------------------------------------------------------------===//
// Compression
//===--------------------------------------------------------------------===//
static string RawInflate(const char *data, idx_t size) {
	duckdb_miniz::mz_stream stream;
	memset(&stream, 0, sizeof(stream));
	if (duckdb_miniz::mz_inflateInit2(&stream, -MZ_DEFAULT_WINDOW_BITS) != duckdb_miniz::MZ_OK) {
		throw InvalidInputException("Avro: failed to initialize inflate");
	}
	stream.next_in = reinterpret_cast<const unsigned char *>(data);
	stream.avail_in = static_cast<unsigned int>(size);
	string out;
	out.resize(MaxValue<idx_t>(size * 4, 1024));
	int ret;
	while (true) {
		auto produced = static_cast<idx_t>(stream.total_out);
		if (produced == out.size()) {
			out.resize(out.size() * 2);
		}
		stream.next_out = reinterpret_cast<unsigned char *>(&out[0]) + produced;
		stream.avail_out = static_cast<unsigned int>(out.size() - produced);
		ret = duckdb_miniz::mz_inflate(&stream, duckdb_miniz::MZ_SYNC_FLUSH);
		if (ret == duckdb_miniz::MZ_STREAM_END) {
			break;
		}
		if (ret == duckdb_miniz::MZ_BUF_ERROR && stream.avail_in == 0 && stream.avail_out != 0) {
			duckdb_miniz::mz_inflateEnd(&stream);
			throw InvalidInputException("Avro: truncated deflate block");
		}
		if (ret != duckdb_miniz::MZ_OK && ret != duckdb_miniz::MZ_BUF_ERROR) {
			duckdb_miniz::mz_inflateEnd(&stream);
			throw InvalidInputException("Avro: corrupt deflate block (miniz error %d)", ret);
		}
	}
	out.resize(static_cast<idx_t>(stream.total_out));
	duckdb_miniz::mz_inflateEnd(&stream);
	return out;
}

static string RawDeflate(const string &input) {
	duckdb_miniz::mz_stream stream;
	memset(&stream, 0, sizeof(stream));
	if (duckdb_miniz::mz_deflateInit2(&stream, 6, MZ_DEFLATED, -MZ_DEFAULT_WINDOW_BITS, 9,
	                                  duckdb_miniz::MZ_DEFAULT_STRATEGY) != duckdb_miniz::MZ_OK) {
		throw InternalException("Avro: failed to initialize deflate");
	}
	auto bound = duckdb_miniz::mz_deflateBound(&stream, static_cast<duckdb_miniz::mz_ulong>(input.size()));
	string out;
	out.resize(bound + 64);
	stream.next_in = reinterpret_cast<const unsigned char *>(input.data());
	stream.avail_in = static_cast<unsigned int>(input.size());
	stream.next_out = reinterpret_cast<unsigned char *>(&out[0]);
	stream.avail_out = static_cast<unsigned int>(out.size());
	auto ret = duckdb_miniz::mz_deflate(&stream, duckdb_miniz::MZ_FINISH);
	if (ret != duckdb_miniz::MZ_STREAM_END) {
		duckdb_miniz::mz_deflateEnd(&stream);
		throw InternalException("Avro: deflate failed (miniz error %d)", ret);
	}
	out.resize(static_cast<idx_t>(stream.total_out));
	duckdb_miniz::mz_deflateEnd(&stream);
	return out;
}

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
namespace {

constexpr idx_t AVRO_SYNC_SIZE = 16;
constexpr const char AVRO_MAGIC[4] = {'O', 'b', 'j', '\x01'};

class AvroCursor {
public:
	AvroCursor(const char *data_p, idx_t size_p) : data(data_p), size(size_p) {
	}

	idx_t Remaining() const {
		return size - pos;
	}
	bool AtEnd() const {
		return pos >= size;
	}
	uint8_t ReadByte() {
		if (pos >= size) {
			throw InvalidInputException("Avro: unexpected end of data");
		}
		return static_cast<uint8_t>(data[pos++]);
	}
	int64_t ReadLong() {
		uint64_t value = 0;
		uint32_t shift = 0;
		while (true) {
			auto b = ReadByte();
			value |= static_cast<uint64_t>(b & 0x7F) << shift;
			if (!(b & 0x80)) {
				break;
			}
			shift += 7;
			if (shift > 63) {
				throw InvalidInputException("Avro: varint too long");
			}
		}
		return static_cast<int64_t>(value >> 1) ^ -static_cast<int64_t>(value & 1);
	}
	const char *ReadRaw(idx_t len) {
		if (len > Remaining()) {
			throw InvalidInputException("Avro: unexpected end of data (need %llu bytes, have %llu)",
			                            static_cast<uint64_t>(len), static_cast<uint64_t>(Remaining()));
		}
		auto result = data + pos;
		pos += len;
		return result;
	}
	string ReadBytes() {
		auto len = ReadLong();
		if (len < 0) {
			throw InvalidInputException("Avro: negative length");
		}
		return string(ReadRaw(static_cast<idx_t>(len)), static_cast<idx_t>(len));
	}

private:
	const char *data;
	idx_t size;
	idx_t pos = 0;
};

//! Reads a block count; for negative counts also consumes the byte size
idx_t ReadBlockCount(AvroCursor &cursor) {
	auto count = cursor.ReadLong();
	if (count < 0) {
		if (count == NumericLimits<int64_t>::Minimum()) {
			throw InvalidInputException("Avro: invalid block count");
		}
		count = -count;
		cursor.ReadLong();
	}
	return static_cast<idx_t>(count);
}

AvroValue DecodeValue(AvroCursor &cursor, const shared_ptr<AvroSchemaNode> &schema_ptr) {
	auto &schema = *schema_ptr;
	switch (schema.type) {
	case AvroType::NULL_:
		return AvroValue::Null();
	case AvroType::BOOLEAN: {
		auto b = cursor.ReadByte();
		if (b > 1) {
			throw InvalidInputException("Avro: invalid boolean byte");
		}
		return AvroValue::Bool(b == 1);
	}
	case AvroType::INT: {
		auto v = cursor.ReadLong();
		if (v < NumericLimits<int32_t>::Minimum() || v > NumericLimits<int32_t>::Maximum()) {
			throw InvalidInputException("Avro: int out of range");
		}
		return AvroValue::Int(static_cast<int32_t>(v));
	}
	case AvroType::LONG:
		return AvroValue::Long(cursor.ReadLong());
	case AvroType::FLOAT: {
		float v;
		memcpy(&v, cursor.ReadRaw(sizeof(v)), sizeof(v));
		return AvroValue::Float(v);
	}
	case AvroType::DOUBLE: {
		double v;
		memcpy(&v, cursor.ReadRaw(sizeof(v)), sizeof(v));
		return AvroValue::Double(v);
	}
	case AvroType::BYTES:
		return AvroValue::Bytes(cursor.ReadBytes());
	case AvroType::STRING:
		return AvroValue::String(cursor.ReadBytes());
	case AvroType::RECORD: {
		vector<AvroValue> fields;
		fields.reserve(schema.fields.size());
		for (auto &field : schema.fields) {
			fields.push_back(DecodeValue(cursor, field.schema));
		}
		auto result = AvroValue::Record(std::move(fields));
		result.record_schema = schema_ptr;
		return result;
	}
	case AvroType::ENUM: {
		auto idx = cursor.ReadLong();
		if (idx < 0 || static_cast<idx_t>(idx) >= schema.symbols.size()) {
			throw InvalidInputException("Avro: enum index out of range");
		}
		return AvroValue::Enum(static_cast<idx_t>(idx), schema.symbols[static_cast<idx_t>(idx)]);
	}
	case AvroType::ARRAY: {
		vector<AvroValue> items;
		while (true) {
			auto count = ReadBlockCount(cursor);
			if (count == 0) {
				break;
			}
			if (schema.child->type != AvroType::NULL_ && count > cursor.Remaining()) {
				throw InvalidInputException("Avro: array block count exceeds remaining data");
			}
			for (idx_t i = 0; i < count; i++) {
				items.push_back(DecodeValue(cursor, schema.child));
			}
		}
		return AvroValue::Array(std::move(items));
	}
	case AvroType::MAP: {
		vector<string> keys;
		vector<AvroValue> values;
		while (true) {
			auto count = ReadBlockCount(cursor);
			if (count == 0) {
				break;
			}
			if (count > cursor.Remaining()) {
				throw InvalidInputException("Avro: map block count exceeds remaining data");
			}
			for (idx_t i = 0; i < count; i++) {
				keys.push_back(cursor.ReadBytes());
				values.push_back(DecodeValue(cursor, schema.child));
			}
		}
		return AvroValue::Map(std::move(keys), std::move(values));
	}
	case AvroType::FIXED:
		return AvroValue::Fixed(string(cursor.ReadRaw(schema.fixed_size), schema.fixed_size));
	case AvroType::UNION: {
		auto idx = cursor.ReadLong();
		if (idx < 0 || static_cast<idx_t>(idx) >= schema.branches.size()) {
			throw InvalidInputException("Avro: union branch index out of range");
		}
		return AvroValue::Union(static_cast<idx_t>(idx), DecodeValue(cursor, schema.branches[static_cast<idx_t>(idx)]));
	}
	}
	throw InternalException("Avro: unknown schema type");
}

} // namespace

AvroFileData AvroReader::Read(const string &bytes) {
	AvroCursor cursor(bytes.data(), bytes.size());
	if (bytes.size() < sizeof(AVRO_MAGIC) || memcmp(bytes.data(), AVRO_MAGIC, sizeof(AVRO_MAGIC)) != 0) {
		throw InvalidInputException("Not an Avro object container file (bad magic)");
	}
	cursor.ReadRaw(sizeof(AVRO_MAGIC));

	AvroFileData result;
	while (true) {
		auto count = ReadBlockCount(cursor);
		if (count == 0) {
			break;
		}
		for (idx_t i = 0; i < count; i++) {
			auto key = cursor.ReadBytes();
			result.metadata[key] = cursor.ReadBytes();
		}
	}
	string sync(cursor.ReadRaw(AVRO_SYNC_SIZE), AVRO_SYNC_SIZE);

	auto schema_entry = result.metadata.find("avro.schema");
	if (schema_entry == result.metadata.end()) {
		throw InvalidInputException("Avro file header has no avro.schema");
	}
	result.schema_json = schema_entry->second;
	result.schema = AvroSchema::Parse(result.schema_json);

	string codec = "null";
	auto codec_entry = result.metadata.find("avro.codec");
	if (codec_entry != result.metadata.end()) {
		codec = codec_entry->second;
	}
	if (codec != "null" && codec != "deflate") {
		throw NotImplementedException("Avro codec \"%s\" is not supported (only null and deflate)", codec);
	}

	while (!cursor.AtEnd()) {
		auto record_count = cursor.ReadLong();
		auto block_size = cursor.ReadLong();
		if (record_count < 0 || block_size < 0) {
			throw InvalidInputException("Avro: invalid data block header");
		}
		auto block = cursor.ReadRaw(static_cast<idx_t>(block_size));
		string inflated;
		const char *data = block;
		idx_t data_size = static_cast<idx_t>(block_size);
		if (codec == "deflate") {
			inflated = RawInflate(block, data_size);
			data = inflated.data();
			data_size = inflated.size();
		}
		if (memcmp(cursor.ReadRaw(AVRO_SYNC_SIZE), sync.data(), AVRO_SYNC_SIZE) != 0) {
			throw InvalidInputException("Avro: sync marker mismatch");
		}
		AvroCursor block_cursor(data, data_size);
		for (int64_t i = 0; i < record_count; i++) {
			result.records.push_back(DecodeValue(block_cursor, result.schema.Root()));
		}
		if (!block_cursor.AtEnd()) {
			throw InvalidInputException("Avro: trailing bytes in data block");
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
namespace {

void WriteLong(string &out, int64_t value) {
	auto v = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
	while (v & ~static_cast<uint64_t>(0x7F)) {
		out += static_cast<char>((v & 0x7F) | 0x80);
		v >>= 7;
	}
	out += static_cast<char>(v);
}

void WriteBytes(string &out, const string &value) {
	WriteLong(out, NumericCast<int64_t>(value.size()));
	out += value;
}

[[noreturn]] void KindMismatch(const AvroSchemaNode &schema, const AvroValue &value) {
	throw InternalException("Avro write: value kind %d does not match schema type %d%s", static_cast<int>(value.kind),
	                        static_cast<int>(schema.type), schema.name.empty() ? "" : " (" + schema.name + ")");
}

void EncodeValue(string &out, const AvroSchemaNode &schema, const AvroValue &value) {
	if (schema.type == AvroType::UNION) {
		if (value.kind != AvroValueKind::UNION) {
			throw InternalException("Avro write: expected union value for union schema");
		}
		auto idx = value.int_value;
		if (idx < 0 || static_cast<idx_t>(idx) >= schema.branches.size() || value.children.size() != 1) {
			throw InternalException("Avro write: invalid union branch %lld", static_cast<long long>(idx));
		}
		WriteLong(out, idx);
		EncodeValue(out, *schema.branches[static_cast<idx_t>(idx)], value.children[0]);
		return;
	}
	switch (schema.type) {
	case AvroType::NULL_:
		if (value.kind != AvroValueKind::NULL_) {
			KindMismatch(schema, value);
		}
		return;
	case AvroType::BOOLEAN:
		if (value.kind != AvroValueKind::BOOL) {
			KindMismatch(schema, value);
		}
		out += static_cast<char>(value.int_value ? 1 : 0);
		return;
	case AvroType::INT:
		if (value.kind != AvroValueKind::INT && value.kind != AvroValueKind::LONG) {
			KindMismatch(schema, value);
		}
		if (value.int_value < NumericLimits<int32_t>::Minimum() ||
		    value.int_value > NumericLimits<int32_t>::Maximum()) {
			throw InternalException("Avro write: value out of range for int");
		}
		WriteLong(out, value.int_value);
		return;
	case AvroType::LONG:
		if (value.kind != AvroValueKind::INT && value.kind != AvroValueKind::LONG) {
			KindMismatch(schema, value);
		}
		WriteLong(out, value.int_value);
		return;
	case AvroType::FLOAT: {
		if (value.kind != AvroValueKind::FLOAT && value.kind != AvroValueKind::DOUBLE) {
			KindMismatch(schema, value);
		}
		auto f = static_cast<float>(value.double_value);
		out.append(reinterpret_cast<const char *>(&f), sizeof(f));
		return;
	}
	case AvroType::DOUBLE: {
		if (value.kind != AvroValueKind::FLOAT && value.kind != AvroValueKind::DOUBLE) {
			KindMismatch(schema, value);
		}
		out.append(reinterpret_cast<const char *>(&value.double_value), sizeof(double));
		return;
	}
	case AvroType::BYTES:
		if (value.kind != AvroValueKind::BYTES && value.kind != AvroValueKind::FIXED) {
			KindMismatch(schema, value);
		}
		WriteBytes(out, value.str_value);
		return;
	case AvroType::STRING:
		if (value.kind != AvroValueKind::STRING) {
			KindMismatch(schema, value);
		}
		WriteBytes(out, value.str_value);
		return;
	case AvroType::FIXED:
		if (value.kind != AvroValueKind::FIXED && value.kind != AvroValueKind::BYTES) {
			KindMismatch(schema, value);
		}
		if (value.str_value.size() != schema.fixed_size) {
			throw InternalException("Avro write: fixed \"%s\" expects %llu bytes, got %llu", schema.name,
			                        static_cast<uint64_t>(schema.fixed_size),
			                        static_cast<uint64_t>(value.str_value.size()));
		}
		out += value.str_value;
		return;
	case AvroType::ENUM: {
		if (value.kind != AvroValueKind::ENUM) {
			KindMismatch(schema, value);
		}
		auto idx = value.int_value;
		if (!value.str_value.empty()) {
			idx = -1;
			for (idx_t i = 0; i < schema.symbols.size(); i++) {
				if (schema.symbols[i] == value.str_value) {
					idx = NumericCast<int64_t>(i);
					break;
				}
			}
		}
		if (idx < 0 || static_cast<idx_t>(idx) >= schema.symbols.size()) {
			throw InternalException("Avro write: invalid enum value for \"%s\"", schema.name);
		}
		WriteLong(out, idx);
		return;
	}
	case AvroType::RECORD:
		if (value.kind != AvroValueKind::RECORD) {
			KindMismatch(schema, value);
		}
		if (value.children.size() != schema.fields.size()) {
			throw InternalException("Avro write: record \"%s\" expects %llu fields, got %llu", schema.name,
			                        static_cast<uint64_t>(schema.fields.size()),
			                        static_cast<uint64_t>(value.children.size()));
		}
		for (idx_t i = 0; i < schema.fields.size(); i++) {
			EncodeValue(out, *schema.fields[i].schema, value.children[i]);
		}
		return;
	case AvroType::ARRAY:
		if (value.kind != AvroValueKind::ARRAY) {
			KindMismatch(schema, value);
		}
		if (!value.children.empty()) {
			WriteLong(out, NumericCast<int64_t>(value.children.size()));
			for (auto &item : value.children) {
				EncodeValue(out, *schema.child, item);
			}
		}
		WriteLong(out, 0);
		return;
	case AvroType::MAP:
		if (value.kind != AvroValueKind::MAP || value.keys.size() != value.children.size()) {
			KindMismatch(schema, value);
		}
		if (!value.children.empty()) {
			WriteLong(out, NumericCast<int64_t>(value.children.size()));
			for (idx_t i = 0; i < value.children.size(); i++) {
				WriteBytes(out, value.keys[i]);
				EncodeValue(out, *schema.child, value.children[i]);
			}
		}
		WriteLong(out, 0);
		return;
	case AvroType::UNION:
		break;
	}
}

constexpr idx_t AVRO_RECORDS_PER_BLOCK = 16384;

} // namespace

string AvroWriter::Write(const string &schema_json, const map<string, string> &metadata,
                         const vector<AvroValue> &records, const string &codec) {
	if (codec != "null" && codec != "deflate") {
		throw NotImplementedException("Avro codec \"%s\" is not supported for writing (only null and deflate)", codec);
	}
	auto schema = AvroSchema::Parse(schema_json);

	string sync(AVRO_SYNC_SIZE, '\0');
	std::random_device device;
	std::mt19937_64 engine((static_cast<uint64_t>(device()) << 32) ^ device());
	for (auto &c : sync) {
		c = static_cast<char>(engine() & 0xFF);
	}

	string out(AVRO_MAGIC, sizeof(AVRO_MAGIC));
	vector<pair<string, string>> header;
	header.emplace_back("avro.schema", schema_json);
	header.emplace_back("avro.codec", codec);
	for (auto &entry : metadata) {
		if (entry.first == "avro.schema" || entry.first == "avro.codec") {
			continue;
		}
		header.emplace_back(entry.first, entry.second);
	}
	WriteLong(out, NumericCast<int64_t>(header.size()));
	for (auto &entry : header) {
		WriteBytes(out, entry.first);
		WriteBytes(out, entry.second);
	}
	WriteLong(out, 0);
	out += sync;

	for (idx_t start = 0; start < records.size(); start += AVRO_RECORDS_PER_BLOCK) {
		auto end = MinValue<idx_t>(start + AVRO_RECORDS_PER_BLOCK, records.size());
		string block;
		for (idx_t i = start; i < end; i++) {
			EncodeValue(block, *schema.Root(), records[i]);
		}
		if (codec == "deflate") {
			block = RawDeflate(block);
		}
		WriteLong(out, NumericCast<int64_t>(end - start));
		WriteLong(out, NumericCast<int64_t>(block.size()));
		out += block;
		out += sync;
	}
	return out;
}

} // namespace duckdb
