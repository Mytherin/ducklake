#include "rest/ducklake_rest_server.hpp"
#include "rest/ducklake_rest_serialization.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
static DuckLakeTableEntry &GetTableEntry(DuckLakeRESTRequest &request, DuckLakeSnapshot snapshot, TableIndex table_id) {
	auto entry = request.catalog->GetEntryById(*request.transaction, snapshot, table_id);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("Table with id %llu does not exist at snapshot %llu", table_id.index,
		                       snapshot.snapshot_id);
	}
	return entry->Cast<DuckLakeTableEntry>();
}

static void WriteQueryResult(RESTJsonOut out, QueryResult &result) {
	auto columns = out.Arr("columns");
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto column = columns.AppendObj();
		column.SetStr("name", result.GetNames()[col_idx].GetIdentifierName());
		column.SetRaw("type", RESTJson::TypeToJson(*out.writer, result.GetTypes()[col_idx]));
	}
	auto rows = out.Arr("rows");
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			auto row = rows.AppendArr();
			for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
				row.AppendRaw(RESTJson::TypedValueToJson(*out.writer, chunk->GetValue(col_idx, row_idx)));
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Read Endpoints
//===--------------------------------------------------------------------===//
static void ConfigEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto metadata = request.GetMetadataManager().LoadDuckLake();
	for (auto &tag : metadata.tags) {
		if (tag.key == "data_path") {
			// clients get the resolved data path
			tag.value = request.catalog->DataPath();
		}
	}
	DuckLakeRESTSerializer::Write(output, metadata);
}

static void SnapshotDataEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto &metadata_manager = request.GetMetadataManager();
	DuckLakeSnapshot snapshot;
	auto at = input.TryGet("at");
	if (!at.IsNull()) {
		BoundAtClause at_clause(Identifier(at.Str("unit")), at.GetValue("value"));
		auto bound = input.Str("bound", "upper") == "lower" ? SnapshotBound::LOWER_BOUND : SnapshotBound::UPPER_BOUND;
		snapshot = *metadata_manager.GetSnapshot(at_clause, bound);
	} else {
		snapshot = request.GetSnapshot(input);
	}
	DuckLakeRESTSerializer::Write(output.Obj("snapshot"), snapshot);
	if (input.Bool("include_catalog", false)) {
		DuckLakeRESTSerializer::Write(output.Obj("catalog"), metadata_manager.GetCatalogForSnapshot(snapshot));
	}
	if (input.Bool("include_stats", false)) {
		auto stats = output.Arr("stats");
		for (auto &table_stats : metadata_manager.GetGlobalTableStats(snapshot)) {
			DuckLakeRESTSerializer::Write(stats.AppendObj(), table_stats);
		}
	}
}

static void SelectEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto &table = GetTableEntry(request, snapshot, TableIndex(input.UInt("table_id")));
	auto files = request.GetMetadataManager().GetFilesForTable(table, snapshot);
	auto files_out = output.Arr("files");
	for (auto &file : files) {
		DuckLakeRESTSerializer::Write(files_out.AppendObj(), file);
	}
}

static void ReadInlinedDataEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	vector<string> columns;
	for (auto &column : input.Get("columns").Items()) {
		columns.push_back(column.Str());
	}
	auto result = request.GetMetadataManager().ReadInlinedData(snapshot, input.Str("table_name"), columns);
	result->ThrowIfError("Failed to read inlined data: ");
	WriteQueryResult(output, *result);
}

static void TableSchemaVersionsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto &metadata_manager = request.GetMetadataManager();
	auto table_id = TableIndex(input.UInt("table_id"));
	output.SetUInt("begin_snapshot", metadata_manager.GetBeginSnapshotForTable(table_id));
	if (input.Has("snapshot_id")) {
		auto versions = metadata_manager.GetTableSchemaVersions(table_id, request.GetSnapshot(input));
		output.SetOpt("creation", versions.creation);
		output.SetOpt("last_change", versions.last_change);
	}
	auto schema_version = input.TryGet("schema_version");
	if (!schema_version.IsNull()) {
		output.SetUInt("schema_version_begin_snapshot",
		               metadata_manager.GetBeginSnapshotForSchemaVersion(table_id, schema_version.UInt()));
	}
}

static void RowCountEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto &metadata_manager = request.GetMetadataManager();
	auto snapshot = request.GetSnapshot(input);
	auto count = input.Str("count");
	if (count == "inlined") {
		output.SetUInt("row_count", metadata_manager.GetNetInlinedRowCount(input.Str("table_name"), snapshot));
		return;
	}
	auto table_id = TableIndex(input.UInt("table_id"));
	if (count == "data_files") {
		output.SetUInt("row_count", metadata_manager.GetNetDataFileRowCount(table_id, snapshot));
	} else if (count == "data_files_exact") {
		output.SetOpt("row_count", metadata_manager.GetNetDataFileRowCountForStats(table_id, snapshot));
	} else {
		throw InvalidInputException("Unsupported row count \"%s\"", count);
	}
}

static void InlinedTablesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto names = output.Arr("table_names");
	for (auto &name : request.GetMetadataManager().GetInlinedTableNames(TableIndex(input.UInt("table_id")))) {
		names.AppendStr(name);
	}
}

static void ListSnapshotsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshots = output.Arr("snapshots");
	for (auto &snapshot : request.GetMetadataManager().GetAllSnapshots()) {
		DuckLakeRESTSerializer::Write(snapshots.AppendObj(), snapshot);
	}
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//
//! Replays the operations of a client transaction in the transaction of the request
class DuckLakeRESTCommit {
public:
	explicit DuckLakeRESTCommit(DuckLakeRESTRequest &request)
	    : request(request), transaction(*request.transaction), catalog(*request.catalog),
	      context(*request.connection.context) {
	}

	void Run(RESTJsonIn input, RESTJsonOut output);

private:
	void ApplyRequest(const string &path, RESTJsonIn parameters);
	void CreateSchema(RESTJsonIn parameters);
	void CreateTable(RESTJsonIn parameters);
	void InsertRows(RESTJsonIn parameters);
	void InsertFiles(RESTJsonIn parameters);

	DuckLakeSchemaEntry &GetSchema(RESTJsonIn schema_path);
	//! Resolves an id of the request, negative ids refer to entries created by earlier requests
	TableIndex GetTableId(RESTJsonIn id);
	DuckLakeTableEntry &GetTable(TableIndex table_id);

private:
	DuckLakeRESTRequest &request;
	DuckLakeTransaction &transaction;
	DuckLakeCatalog &catalog;
	ClientContext &context;
	map<int64_t, TableIndex> placeholder_ids;
};

DuckLakeSchemaEntry &DuckLakeRESTCommit::GetSchema(RESTJsonIn schema_path) {
	vector<Identifier> path;
	for (auto &name : schema_path.Items()) {
		path.emplace_back(name.Str());
	}
	if (path.empty()) {
		path.emplace_back(*catalog.GetDefaultSchema());
	}
	auto schema = catalog.GetSchema(catalog.GetCatalogTransaction(context), path, OnEntryNotFound::THROW_EXCEPTION);
	return schema->Cast<DuckLakeSchemaEntry>();
}

TableIndex DuckLakeRESTCommit::GetTableId(RESTJsonIn id) {
	auto table_id = id.Int();
	if (table_id >= 0) {
		return TableIndex(NumericCast<idx_t>(table_id));
	}
	auto entry = placeholder_ids.find(table_id);
	if (entry == placeholder_ids.end()) {
		throw InvalidInputException("Placeholder id %lld does not refer to an entry created in this transaction",
		                            table_id);
	}
	return entry->second;
}

DuckLakeTableEntry &DuckLakeRESTCommit::GetTable(TableIndex table_id) {
	optional_ptr<CatalogEntry> entry;
	if (DuckLakeTransaction::IsTransactionLocal(table_id.index)) {
		entry = transaction.GetLocalEntryById(table_id);
	} else {
		entry = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
	}
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("Table with id %llu does not exist", table_id.index);
	}
	return entry->Cast<DuckLakeTableEntry>();
}

void DuckLakeRESTCommit::CreateSchema(RESTJsonIn parameters) {
	auto info = make_uniq<CreateSchemaInfo>();
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &name : parameters.Get("schema_path").Items()) {
		path.emplace_back(name.Str());
	}
	if (path.size() < 2) {
		throw InvalidInputException("create_schema requires a non-empty schema_path");
	}
	info->SetQualifiedName(QualifiedName(std::move(path), Identifier()));
	info->on_conflict =
	    parameters.Bool("if_not_exists") ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	catalog.CreateSchema(catalog.GetCatalogTransaction(context), *info);
}

void DuckLakeRESTCommit::CreateTable(RESTJsonIn parameters) {
	auto &schema = GetSchema(parameters.TryGet("schema_path"));
	auto info = make_uniq<CreateTableInfo>(schema, Identifier(parameters.Str("table_name")));
	for (auto &column_in : parameters.Get("columns").Items()) {
		auto type = TransformStringToLogicalType(column_in.Str("column_type"), context);
		ColumnDefinition column(Identifier(column_in.Str("column_name")), std::move(type));
		auto default_value = column_in.TryGet("default");
		if (!default_value.IsNull()) {
			column.SetDefaultValue(Parser::GetBuiltinParser().ParseSingleExpression(default_value.Str()));
		}
		auto comment = column_in.TryGet("comment");
		if (!comment.IsNull()) {
			column.SetComment(Value(comment.Str()));
		}
		auto not_null = column_in.Bool("not_null", false);
		auto logical_index = LogicalIndex(info->columns.LogicalColumnCount());
		info->columns.AddColumn(std::move(column));
		if (not_null) {
			info->constraints.push_back(make_uniq<NotNullConstraint>(logical_index));
		}
	}
	for (auto &expr : parameters.TryGet("partitioned_by").Items()) {
		info->partition_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr.Str()));
	}
	for (auto &expr : parameters.TryGet("sorted_by").Items()) {
		info->sort_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr.Str()));
	}
	for (auto &option : parameters.TryGet("options").Members()) {
		info->options[option.first] = Parser::GetBuiltinParser().ParseSingleExpression(option.second.Str());
	}
	auto comment = parameters.TryGet("comment");
	if (!comment.IsNull()) {
		info->comment = Value(comment.Str());
	}
	BoundCreateTableInfo bound_info(schema, std::move(info));
	auto uuid = parameters.Str("table_uuid", transaction.GenerateUUID());
	auto path = parameters.Str("path", string());
	if (path.empty()) {
		path = schema.GenerateTableDataPath(uuid, bound_info.Base().GetTableName().GetIdentifierName());
	}
	auto entry = schema.CreateTableExtended(catalog.GetCatalogTransaction(context), bound_info, std::move(uuid),
	                                        std::move(path));
	if (!entry) {
		throw CatalogException("Table \"%s\" already exists", parameters.Str("table_name"));
	}
	auto placeholder = parameters.TryGet("table_id");
	if (!placeholder.IsNull()) {
		placeholder_ids[placeholder.Int()] = entry->Cast<DuckLakeTableEntry>().GetTableId();
	}
}

void DuckLakeRESTCommit::InsertRows(RESTJsonIn parameters) {
	auto table_id = GetTableId(parameters.Get("table_id"));
	auto &table = GetTable(table_id);
	vector<LogicalType> types;
	for (auto &type : parameters.Get("column_types").Items()) {
		types.push_back(RESTJson::TypeFromJson(type));
	}
	auto new_data = make_uniq<DuckLakeInlinedData>();
	new_data->data = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	ColumnDataAppendState append_state;
	new_data->data->InitializeAppend(append_state);
	for (auto &row : parameters.Get("rows").Items()) {
		auto values = row.Items();
		if (values.size() != types.size()) {
			throw InvalidInputException("insert_rows: row has %llu values but %llu columns were given", values.size(),
			                            types.size());
		}
		auto row_idx = chunk.size();
		for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
			chunk.SetValue(col_idx, row_idx, RESTJson::TypedValueFromJson(values[col_idx], types[col_idx]));
		}
		chunk.SetCardinality(row_idx + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			new_data->data->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		new_data->data->Append(append_state, chunk);
	}
	for (auto &row_id : parameters.TryGet("row_ids").Items()) {
		new_data->row_ids.push_back(row_id.Int());
	}
	for (auto &stats_in : parameters.TryGet("column_stats").Items()) {
		new_data->column_stats.emplace(FieldIndex(stats_in.UInt("column_id")),
		                               DuckLakeRESTSerializer::ReadColumnStats(stats_in));
	}
	transaction.AppendInlinedData(table.GetTableId(), std::move(new_data));
}

void DuckLakeRESTCommit::InsertFiles(RESTJsonIn parameters) {
	auto table_id = GetTableId(parameters.Get("table_id"));
	auto &table = GetTable(table_id);
	vector<DuckLakeDataFile> files;
	for (auto &file : parameters.Get("files").Items()) {
		files.push_back(DuckLakeRESTSerializer::ReadDataFile(file));
	}
	transaction.AppendFiles(table.GetTableId(), std::move(files));
}

void DuckLakeRESTCommit::ApplyRequest(const string &path, RESTJsonIn parameters) {
	if (path == "create_schema") {
		CreateSchema(parameters);
	} else if (path == "create_table") {
		CreateTable(parameters);
	} else if (path == "insert_rows") {
		InsertRows(parameters);
	} else if (path == "insert_files") {
		InsertFiles(parameters);
	} else {
		throw NotImplementedException("Unsupported request \"%s\" in commit_transaction", path);
	}
}

void DuckLakeRESTCommit::Run(RESTJsonIn input, RESTJsonOut output) {
	auto snapshot_id = input.TryGet("snapshot_id");
	if (!snapshot_id.IsNull()) {
		// conflicts are detected against the snapshot the client transaction started from
		transaction.SetSnapshot(request.GetSnapshotById(snapshot_id.UInt()));
	}
	auto commit_info = input.TryGet("commit_info");
	if (!commit_info.IsNull()) {
		transaction.SetCommitMessage(DuckLakeRESTSerializer::ReadSnapshotCommit(commit_info));
	}
	transaction.SetRequiresNewInlinedTable(input.Bool("requires_new_inlined_table", false));
	for (auto &entry : input.Get("requests").Items()) {
		auto path = entry.Str("path");
		if (StringUtil::StartsWith(path, "/")) {
			path = path.substr(1);
		}
		ApplyRequest(path, entry.TryGet("parameters"));
	}
	auto transaction_ref = transaction.shared_from_this();
	request.connection.Commit();
	auto committed = transaction_ref->GetCommittedSnapshot();
	if (committed.snapshot_id == DConstants::INVALID_INDEX) {
		output.SetNull("snapshot_id");
		output.SetNull("schema_version");
	} else {
		output.SetUInt("snapshot_id", committed.snapshot_id);
		output.SetUInt("schema_version", committed.schema_version);
	}
}

static void CommitTransactionEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	DuckLakeRESTCommit commit(request);
	commit.Run(input, output);
}

const unordered_map<string, rest_endpoint_t> &DuckLakeRESTServer::GetEndpoints() {
	static const unordered_map<string, rest_endpoint_t> endpoints = {
	    {"config", ConfigEndpoint},
	    {"snapshot_data", SnapshotDataEndpoint},
	    {"select", SelectEndpoint},
	    {"read_inlined_data", ReadInlinedDataEndpoint},
	    {"table_schema_versions", TableSchemaVersionsEndpoint},
	    {"list_snapshots", ListSnapshotsEndpoint},
	    {"row_count", RowCountEndpoint},
	    {"inlined_tables", InlinedTablesEndpoint},
	    {"commit_transaction", CommitTransactionEndpoint}};
	return endpoints;
}

} // namespace duckdb
