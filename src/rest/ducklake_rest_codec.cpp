#include "rest/ducklake_rest_codec.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

RESTRows::RESTRows() {
}

RESTRows::RESTRows(ColumnDataCollection &rows_p) : rows(rows_p) {
}

RESTRows::RESTRows(unique_ptr<ColumnDataCollection> rows_p) : owned_rows(std::move(rows_p)), rows(owned_rows.get()) {
}

RESTRows::RESTRows(RESTRows &&other) noexcept : owned_rows(std::move(other.owned_rows)), rows(other.rows) {
	other.rows = nullptr;
}

RESTRows &RESTRows::operator=(RESTRows &&other) noexcept {
	owned_rows = std::move(other.owned_rows);
	rows = other.rows;
	other.rows = nullptr;
	return *this;
}

RESTRows::~RESTRows() {
}

//===--------------------------------------------------------------------===//
// Paths
//===--------------------------------------------------------------------===//
static string WithTrailingSeparator(string path) {
	if (!path.empty() && path.back() != '/') {
		path += '/';
	}
	return path;
}

RESTPathMapper::RESTPathMapper(string client_path_p, string server_path_p)
    : client_path(WithTrailingSeparator(std::move(client_path_p))),
      server_path(WithTrailingSeparator(std::move(server_path_p))) {
}

string RESTPathMapper::Map(const string &path, const string &from, const string &to) {
	if (StringUtil::StartsWith(path, from)) {
		return to + path.substr(from.size());
	}
	// the data path itself, without the trailing separator
	if (path.size() + 1 == from.size() && StringUtil::StartsWith(from, path)) {
		return to.substr(0, to.size() - 1);
	}
	return path;
}

string RESTPathMapper::ToServer(const string &path) const {
	return Map(path, client_path, server_path);
}

string RESTPathMapper::ToClient(const string &path) const {
	return Map(path, server_path, client_path);
}

static thread_local optional_ptr<const RESTPathMapper> current_path_mapper;

RESTPathScope::RESTPathScope(optional_ptr<const RESTPathMapper> mapper) : previous(current_path_mapper) {
	current_path_mapper = mapper;
}

RESTPathScope::~RESTPathScope() {
	current_path_mapper = previous;
}

optional_ptr<const RESTPathMapper> RESTPathScope::Current() {
	return current_path_mapper;
}

JSONMutableValue RESTPathCodec::Write(JSONWriter &writer, const string &value) {
	auto mapper = RESTPathScope::Current();
	return writer.CreateString(mapper ? mapper->ToServer(value) : value);
}

void RESTPathCodec::Read(RESTJsonIn in, string &result) {
	auto mapper = RESTPathScope::Current();
	result = in.Str();
	if (mapper) {
		result = mapper->ToClient(result);
	}
}

JSONMutableValue RESTTimestampCodec::Write(JSONWriter &writer, timestamp_tz_t value) {
	return writer.CreateString(Value::TIMESTAMPTZ(value).ToString());
}

JSONMutableValue RESTTimestampCodec::Write(JSONWriter &writer, const string &value) {
	return writer.CreateString(value);
}

void RESTTimestampCodec::Read(RESTJsonIn in, timestamp_tz_t &result) {
	result = Value(in.Str()).DefaultCastAs(LogicalType::TIMESTAMP_TZ).GetValue<timestamp_tz_t>();
}

void RESTTimestampCodec::Read(RESTJsonIn in, string &result) {
	// timestamps that are kept as strings end up in metadata queries
	result = in.Str();
	Value(result).DefaultCastAs(LogicalType::TIMESTAMP_TZ);
}

JSONMutableValue RESTRowsCodec::Write(JSONWriter &writer, const RESTRows &value) {
	auto result = writer.CreateArray();
	if (!value.rows) {
		return result;
	}
	for (auto &chunk : value.rows->Chunks()) {
		for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
			auto row = writer.CreateArray();
			for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
				row.Append(RESTJson::TypedValueToJson(writer, chunk.GetValue(col_idx, row_idx)));
			}
			result.Append(row);
		}
	}
	return result;
}

void RESTRowsCodec::Read(RESTJsonIn in, RESTRows &result, const vector<LogicalType> &types) {
	auto rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	ColumnDataAppendState append_state;
	rows->InitializeAppend(append_state);
	for (auto &row : in.Items()) {
		auto values = row.Items();
		if (values.size() != types.size()) {
			throw InvalidInputException("DuckLake REST: a row has %llu values but there are %llu columns",
			                            values.size(), types.size());
		}
		auto row_idx = chunk.size();
		for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
			chunk.SetValue(col_idx, row_idx, RESTJson::TypedValueFromJson(values[col_idx], types[col_idx]));
		}
		chunk.SetCardinality(row_idx + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			rows->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		rows->Append(append_state, chunk);
	}
	result = RESTRows(std::move(rows));
}

} // namespace duckdb
