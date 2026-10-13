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
