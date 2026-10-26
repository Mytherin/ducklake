#include "rest/ducklake_rest_arrow.hpp"

#include "rest/ducklake_rest_arrow_ipc.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! The type that sends a type as Arrow, invalid if the type cannot be sent
static LogicalType ToArrowType(const LogicalType &type) {
	if (type.HasAlias()) {
		return type.IsJSONType() ? LogicalType::VARCHAR : LogicalType::INVALID;
	}
	switch (type.id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME_TZ:
		return LogicalType::VARCHAR;
	case LogicalTypeId::LIST: {
		auto child = ToArrowType(ListType::GetChildType(type));
		return child.id() == LogicalTypeId::INVALID ? child : LogicalType::LIST(child);
	}
	case LogicalTypeId::MAP: {
		auto key = ToArrowType(MapType::KeyType(type));
		auto value = ToArrowType(MapType::ValueType(type));
		if (key.id() == LogicalTypeId::INVALID || value.id() == LogicalTypeId::INVALID) {
			return LogicalType::INVALID;
		}
		return LogicalType::MAP(key, value);
	}
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : StructType::GetChildTypes(type)) {
			auto child_type = ToArrowType(child.second);
			if (child_type.id() == LogicalTypeId::INVALID) {
				return child_type;
			}
			children.emplace_back(child.first, std::move(child_type));
		}
		return children.empty() ? LogicalType(LogicalTypeId::INVALID) : LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::SQLNULL:
		return type;
	default:
		return LogicalType::INVALID;
	}
}

bool RESTArrow::Supports(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (ToArrowType(type).id() == LogicalTypeId::INVALID) {
			return false;
		}
	}
	return true;
}

string RESTArrow::Write(ClientContext &context, const vector<string> &names, ColumnDataCollection &rows) {
	vector<LogicalType> arrow_types;
	for (auto &type : rows.Types()) {
		auto arrow_type = ToArrowType(type);
		if (arrow_type.id() == LogicalTypeId::INVALID) {
			throw NotImplementedException("Arrow cannot hold the type %s", type.ToString());
		}
		arrow_types.push_back(std::move(arrow_type));
	}
	ClientProperties options("UTC", ArrowOffsetSize::REGULAR, false, false, false, ArrowFormatVersion::V1_0, &context);
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, arrow_types, names, options);
	vector<ArrowArrayWrapper> wrappers;
	vector<ArrowArray> batches;
	DataChunk converted;
	converted.Initialize(Allocator::DefaultAllocator(), arrow_types);
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> no_extensions;
	for (auto &chunk : rows.Chunks()) {
		converted.Reset();
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			if (chunk.data[col_idx].GetType() == arrow_types[col_idx]) {
				converted.data[col_idx].Reference(chunk.data[col_idx]);
			} else {
				VectorOperations::Cast(context, chunk.data[col_idx], converted.data[col_idx], chunk.size());
			}
		}
		converted.SetChildCardinality(chunk.size());
		wrappers.emplace_back();
		ArrowConverter::ToArrowArray(converted, &wrappers.back().arrow_array, options, no_extensions);
		batches.push_back(wrappers.back().arrow_array);
	}
	return RESTArrowIPC::Write(schema.arrow_schema, batches);
}

unique_ptr<ColumnDataCollection> RESTArrow::Read(ClientContext &context, shared_ptr<const string> data, idx_t offset,
                                                 idx_t size, const vector<LogicalType> &types, vector<string> &names) {
	auto stream = RESTArrowIPC::Read(std::move(data), offset, size);
	ArrowTableSchema arrow_table;
	ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, stream->schema);
	vector<LogicalType> arrow_types;
	names.clear();
	for (idx_t col_idx = 0; col_idx < NumericCast<idx_t>(stream->schema.n_children); col_idx++) {
		arrow_types.push_back(arrow_table.GetColumns().at(col_idx)->GetDuckType());
		names.push_back(stream->schema.children[col_idx]->name);
	}
	if (!types.empty() && types.size() != arrow_types.size()) {
		throw InvalidInputException("The Arrow stream has %llu columns instead of %llu", arrow_types.size(),
		                            types.size());
	}
	auto &result_types = types.empty() ? arrow_types : types;
	auto result = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), result_types);
	DataChunk scanned;
	scanned.Initialize(Allocator::DefaultAllocator(), arrow_types);
	DataChunk converted;
	if (!types.empty()) {
		converted.Initialize(Allocator::DefaultAllocator(), types);
	}
	for (auto &batch : stream->batches) {
		auto wrapper = make_uniq<ArrowArrayWrapper>();
		wrapper->arrow_array = batch;
		batch.release = nullptr;
		ArrowScanLocalState state(std::move(wrapper), context);
		auto length = NumericCast<idx_t>(state.chunk->arrow_array.length);
		while (state.chunk_offset < length) {
			auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, length - state.chunk_offset);
			scanned.Reset();
			scanned.SetChildCardinality(count);
			ArrowTableFunction::ArrowToDuckDB(state, arrow_table.GetColumns(), scanned);
			state.chunk_offset += count;
			if (types.empty()) {
				result->Append(scanned);
				continue;
			}
			converted.Reset();
			for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
				if (scanned.data[col_idx].GetType() == types[col_idx]) {
					converted.data[col_idx].Reference(scanned.data[col_idx]);
				} else {
					VectorOperations::Cast(context, scanned.data[col_idx], converted.data[col_idx], count);
				}
			}
			converted.SetChildCardinality(count);
			result->Append(converted);
		}
	}
	return result;
}

} // namespace duckdb
