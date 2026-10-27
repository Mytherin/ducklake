#include "iceberg/iceberg_avro_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "iceberg/iceberg_avro.hpp"

namespace duckdb {

namespace {

struct AvroFunctionBindData : public TableFunctionData {
	string path;
};

struct AvroFunctionState : public GlobalTableFunctionState {
	bool loaded = false;
	idx_t offset = 0;
	vector<vector<Value>> rows;
};

string ReadWholeFile(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	string content;
	content.resize(size);
	idx_t done = 0;
	while (done < size) {
		auto read = handle->Read(&content[done], size - done);
		if (read <= 0) {
			throw IOException("Failed to read file \"%s\"", path);
		}
		done += NumericCast<idx_t>(read);
	}
	return content;
}

unique_ptr<FunctionData> AvroBindCommon(TableFunctionBindInput &input) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("Avro file path cannot be NULL");
	}
	auto result = make_uniq<AvroFunctionBindData>();
	result->path = StringValue::Get(input.inputs[0]);
	return std::move(result);
}

unique_ptr<FunctionData> AvroReadBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("record");
	return_types.emplace_back(LogicalType::VARCHAR);
	return AvroBindCommon(input);
}

unique_ptr<FunctionData> AvroSchemaBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("key");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);
	return AvroBindCommon(input);
}

unique_ptr<GlobalTableFunctionState> AvroInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AvroFunctionState>();
}

void EmitRows(AvroFunctionState &state, DataChunk &output) {
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		for (idx_t c = 0; c < row.size(); c++) {
			output.data[c].Append(row[c]);
		}
		count++;
	}
	output.SetChildCardinality(count);
}

void AvroReadExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<AvroFunctionBindData>();
	auto &state = data_p.global_state->Cast<AvroFunctionState>();
	if (!state.loaded) {
		state.loaded = true;
		auto file = AvroReader::Read(ReadWholeFile(context, bind_data.path));
		for (auto &record : file.records) {
			state.rows.push_back({Value(record.ToJSON())});
		}
	}
	EmitRows(state, output);
}

void AvroSchemaExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<AvroFunctionBindData>();
	auto &state = data_p.global_state->Cast<AvroFunctionState>();
	if (!state.loaded) {
		state.loaded = true;
		auto file = AvroReader::Read(ReadWholeFile(context, bind_data.path));
		for (auto &entry : file.metadata) {
			Value value;
			if (Value::StringIsValid(entry.second.c_str(), entry.second.size())) {
				value = Value(entry.second);
			} else {
				value = Value::BLOB_RAW(entry.second).DefaultCastAs(LogicalType::VARCHAR);
			}
			state.rows.push_back({Value(entry.first), std::move(value)});
		}
	}
	EmitRows(state, output);
}

} // namespace

DuckLakeAvroReadFunction::DuckLakeAvroReadFunction()
    : TableFunction("ducklake_avro_read", FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
                    AvroReadExecute, AvroReadBind, AvroInit) {
}

DuckLakeAvroSchemaFunction::DuckLakeAvroSchemaFunction()
    : TableFunction("ducklake_avro_schema", FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
                    AvroSchemaExecute, AvroSchemaBind, AvroInit) {
}

} // namespace duckdb
