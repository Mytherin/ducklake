#include "rest/ducklake_rest_server.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_filters.hpp"
#include "rest/ducklake_rest_serialization.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "common/ducklake_util.hpp"
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

//! Writes the columns and rows of a result, optionally with other names for the columns
static void WriteQueryResult(RESTJsonOut out, QueryResult &result, const vector<string> &names = {}) {
	auto columns = out.Arr("columns");
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		auto column = columns.AppendObj();
		column.SetStr("name", names.empty() ? result.GetNames()[col_idx].GetIdentifierName() : names[col_idx]);
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

//! The name of an inlined data or deletion table of a request, which ends up in metadata queries
static string GetInlinedTableName(RESTJsonIn input, const string &key = "table_name") {
	auto name = input.Str(key);
	bool valid = StringUtil::StartsWith(name, "ducklake_inlined_");
	for (auto c : name) {
		valid = valid && (StringUtil::CharacterIsAlphaNumeric(c) || c == '_');
	}
	if (!valid) {
		throw InvalidInputException("\"%s\" is not the name of an inlined data table", name);
	}
	return name;
}

static bool IsColumnName(const ParsedExpression &expression) {
	return expression.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
	       !expression.Cast<ColumnRefExpression>().IsQualified();
}

//! The columns to read of a request, which can only be column names, optionally cast to the type to read them as
static vector<string> GetColumnsToRead(RESTJsonIn input) {
	vector<string> columns;
	for (auto &column : input.Get("columns").Items()) {
		auto expressions = Parser::GetBuiltinParser().ParseExpressionList(column.Str());
		bool valid = expressions.size() == 1;
		if (valid && expressions[0]->GetExpressionClass() == ExpressionClass::CAST) {
			valid = IsColumnName(expressions[0]->Cast<CastExpression>().Child());
		} else if (valid) {
			valid = IsColumnName(*expressions[0]);
		}
		if (!valid) {
			throw InvalidInputException("\"%s\" is not a column name", column.Str());
		}
		columns.push_back(expressions[0]->ToString());
	}
	return columns;
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
	// clients that pass the versions they have loaded get the catalog and statistics if they changed
	auto cached_metadata_version = input.TryGet("cached_metadata_version");
	if (!cached_metadata_version.IsNull() && cached_metadata_version.UInt() != snapshot.schema_version) {
		DuckLakeRESTSerializer::Write(output.Obj("catalog"), metadata_manager.GetCatalogForSnapshot(snapshot));
	}
	auto cached_data_version = input.TryGet("cached_data_version");
	if (!cached_data_version.IsNull() && cached_data_version.UInt() != snapshot.next_file_id) {
		auto stats = output.Arr("stats");
		for (auto &table_stats : metadata_manager.GetGlobalTableStats(snapshot)) {
			DuckLakeRESTSerializer::Write(stats.AppendObj(), table_stats);
		}
	}
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
	auto &metadata_manager = request.GetMetadataManager();
	unique_ptr<FilterPushdownInfo> filters;
	auto filters_in = input.TryGet("filters");
	if (!filters_in.IsNull()) {
		filters = DuckLakeRESTFilters::Read(*request.connection.context, filters_in, table);
	}
	auto files_out = output.Arr("files");
	if (input.Bool("extended", false)) {
		for (auto &file : metadata_manager.GetExtendedFilesForTable(table, snapshot, filters.get())) {
			DuckLakeRESTSerializer::Write(files_out.AppendObj(), file);
		}
	} else {
		for (auto &file : metadata_manager.GetFilesForTable(table, snapshot, filters.get())) {
			DuckLakeRESTSerializer::Write(files_out.AppendObj(), file);
		}
	}
	if (!input.Bool("include_inlined_data", false)) {
		return;
	}
	// the rows of the table that are inlined in the catalog, with all columns of their inlined data tables
	auto col_names = metadata_manager.InlinedColNames();
	auto inlined_data = output.Arr("inlined_data");
	for (auto &inlined_table : table.GetInlinedDataTables(*request.transaction, snapshot)) {
		auto table_at_version = request.catalog->GetTableAtSchemaVersion(*request.transaction, table.GetTableId(),
		                                                                 inlined_table.schema_version);
		if (!table_at_version) {
			continue;
		}
		vector<string> names {col_names.row_id, col_names.begin_snapshot, col_names.end_snapshot};
		vector<string> columns;
		for (auto &name : names) {
			columns.push_back(SQLIdentifier::ToString(name));
		}
		for (auto &column : table_at_version->GetColumns().Physical()) {
			names.push_back(column.Name().GetIdentifierName());
			columns.push_back(
			    metadata_manager.CastColumnToTarget(SQLIdentifier::ToString(names.back()), column.Type()));
		}
		auto result = metadata_manager.ReadInlinedData(snapshot, inlined_table.table_name, columns);
		result->ThrowIfError("Failed to read inlined data: ");
		auto inlined_out = inlined_data.AppendObj();
		inlined_out.SetStr("table_name", inlined_table.table_name);
		inlined_out.SetUInt("schema_version", inlined_table.schema_version);
		WriteQueryResult(inlined_out, *result, names);
	}
}

//! The table at the schema version of one of its inlined data tables
static DuckLakeTableEntry &GetInlinedTableEntry(DuckLakeRESTRequest &request, DuckLakeSnapshot snapshot,
                                                TableIndex table_id, const string &inlined_table_name) {
	auto &table = GetTableEntry(request, snapshot, table_id);
	for (auto &inlined_table : table.GetInlinedDataTables(*request.transaction, snapshot)) {
		if (inlined_table.table_name != inlined_table_name) {
			continue;
		}
		auto result =
		    request.catalog->GetTableAtSchemaVersion(*request.transaction, table_id, inlined_table.schema_version);
		if (result) {
			return *result;
		}
	}
	throw CatalogException("Table %llu has no inlined data table \"%s\"", table_id.index, inlined_table_name);
}

//! Rows that the client transaction deleted are not flushed, the request transaction is rolled back afterwards
static void AddFlushDeletes(DuckLakeRESTRequest &request, RESTJsonIn input, DuckLakeTableEntry &table,
                            const string &inlined_table_name) {
	set<idx_t> row_ids;
	for (auto &row_id : input.TryGet("deleted_row_ids").Items()) {
		row_ids.insert(row_id.UInt());
	}
	request.transaction->AddNewInlinedDeletes(table.GetTableId(), inlined_table_name, std::move(row_ids));
}

//! The ORDER BY of a flush, parsed so that it can only contain ordering expressions
static string GetFlushOrder(RESTJsonIn input) {
	auto order_by = input.Str("order_by", string());
	if (order_by.empty()) {
		return order_by;
	}
	string result;
	for (auto &order : Parser::GetBuiltinParser().ParseOrderList(order_by)) {
		if (!result.empty()) {
			result += ", ";
		}
		result += order.ToString();
	}
	return result;
}

static void ReadInlinedDataEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto table_name = GetInlinedTableName(input);
	auto columns = GetColumnsToRead(input);
	auto &metadata_manager = request.GetMetadataManager();
	auto mode = input.Str("mode", "scan");
	unique_ptr<QueryResult> result;
	if (mode == "scan") {
		result = metadata_manager.ReadInlinedData(snapshot, table_name, columns);
	} else if (mode == "insertions" || mode == "deletions") {
		auto start_snapshot = request.GetSnapshot(input, "start_snapshot");
		if (mode == "insertions") {
			result = metadata_manager.ReadInlinedDataInsertions(start_snapshot, snapshot, table_name, columns);
		} else {
			result = metadata_manager.ReadInlinedDataDeletions(start_snapshot, snapshot, table_name, columns);
		}
	} else if (mode == "flush") {
		auto &table = GetInlinedTableEntry(request, snapshot, TableIndex(input.UInt("table_id")), table_name);
		AddFlushDeletes(request, input, table, table_name);
		result =
		    metadata_manager.ReadAllInlinedDataForFlush(snapshot, table_name, table, GetFlushOrder(input), columns);
	} else {
		throw InvalidInputException("Unsupported inlined data read mode \"%s\"", mode);
	}
	result->ThrowIfError("Failed to read inlined data: ");
	WriteQueryResult(output, *result);
	request.connection.Rollback();
}

static void DeletedInlinedRowsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto table_name = input.Str("table_name");
	auto &table = GetInlinedTableEntry(request, snapshot, TableIndex(input.UInt("table_id")), table_name);
	AddFlushDeletes(request, input, table, table_name);
	auto rows_out = output.Arr("rows");
	for (auto &row : request.GetMetadataManager().GetDeletedInlinedRowVersions(table, table_name, snapshot)) {
		auto row_out = rows_out.AppendObj();
		row_out.SetInt("row_id", row.first.first);
		row_out.SetInt("begin_snapshot", row.first.second);
		row_out.SetInt("end_snapshot", row.second);
	}
	request.connection.Rollback();
}

static void InlinedFileDeletionsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto &table = GetTableEntry(request, snapshot, TableIndex(input.UInt("table_id")));
	auto deletions_out = output.Arr("deletions");
	for (auto &deletion : request.GetMetadataManager().GetInlinedFileDeletionsForFlush(table, snapshot)) {
		auto deletion_out = deletions_out.AppendObj();
		deletion_out.SetUInt("data_file_id", deletion.file_id.index);
		deletion_out.SetStr("data_file_path", deletion.data_file_path);
		deletion_out.SetUInt("row_id", deletion.row_id);
		deletion_out.SetUInt("begin_snapshot", deletion.begin_snapshot);
		if (deletion.delete_file_id.IsValid()) {
			deletion_out.SetUInt("delete_file_id", deletion.delete_file_id.index);
			DuckLakeRESTSerializer::Write(deletion_out.Obj("delete_file"), deletion.delete_file);
			deletion_out.SetUInt("delete_file_begin_snapshot", deletion.delete_file_begin_snapshot);
		}
	}
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
		output.SetUInt("row_count", metadata_manager.GetNetInlinedRowCount(GetInlinedTableName(input), snapshot));
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

static void NextColumnIdEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	output.SetUInt("next_column_id", request.GetMetadataManager().GetNextColumnId(TableIndex(input.UInt("table_id"))));
}

static void ColumnCreatedWithTableEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	output.SetBool("created_with_table", request.GetMetadataManager().IsColumnCreatedWithTable(
	                                         input.Str("table_name"), input.Str("column_name")));
}

//! A timestamp of a request, which is validated before it ends up in a metadata query
static string GetTimestamp(RESTJsonIn input, const string &key) {
	auto timestamp = input.Str(key, string());
	if (!timestamp.empty()) {
		Value(timestamp).DefaultCastAs(LogicalType::TIMESTAMP_TZ);
	}
	return timestamp;
}

static DuckLakeSnapshotFilter GetSnapshotFilter(RESTJsonIn input) {
	DuckLakeSnapshotFilter filter;
	filter.exclude_latest = input.Bool("exclude_latest", false);
	filter.older_than = GetTimestamp(input, "older_than");
	auto snapshot_ids = input.TryGet("snapshot_ids");
	filter.has_snapshot_ids = !snapshot_ids.IsNull();
	for (auto &snapshot_id : snapshot_ids.Items()) {
		filter.snapshot_ids.push_back(snapshot_id.UInt());
	}
	return filter;
}

static void ListSnapshotsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshots = output.Arr("snapshots");
	for (auto &snapshot : request.GetMetadataManager().GetAllSnapshots(GetSnapshotFilter(input))) {
		DuckLakeRESTSerializer::Write(snapshots.AppendObj(), snapshot);
	}
}

static void ExpireSnapshotsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	DuckLakeSnapshotFilter filter;
	filter.has_snapshot_ids = true;
	for (auto &snapshot_id : input.Get("snapshot_ids").Items()) {
		filter.snapshot_ids.push_back(snapshot_id.UInt());
	}
	auto &metadata_manager = request.GetMetadataManager();
	auto snapshots = metadata_manager.GetAllSnapshots(filter);
	if (!snapshots.empty()) {
		metadata_manager.DeleteSnapshots(snapshots);
		for (auto &snapshot : snapshots) {
			request.catalog->InvalidateSchemaCache(snapshot.schema_version);
		}
	}
	output.SetUInt("expired_snapshots", snapshots.size());
}

static void FilesForCleanupEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto files = output.Arr("files");
	for (auto &file : request.GetMetadataManager().GetOldFilesForCleanup(GetTimestamp(input, "older_than"))) {
		auto file_out = files.AppendObj();
		file_out.SetUInt("data_file_id", file.id.index);
		file_out.SetStr("path", file.path);
		file_out.SetStr("schedule_start", Value::TIMESTAMPTZ(file.time).ToString());
	}
}

static void RemoveCleanedUpFilesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	vector<DuckLakeFileForCleanup> files;
	for (auto &file_id : input.Get("data_file_ids").Items()) {
		DuckLakeFileForCleanup file;
		file.id = DataFileIndex(file_id.UInt());
		files.push_back(std::move(file));
	}
	if (!files.empty()) {
		request.GetMetadataManager().RemoveFilesScheduledForCleanup(files);
	}
}

static void FilesForCompactionEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto &table = GetTableEntry(request, snapshot, TableIndex(input.UInt("table_id")));
	auto type = CompactionTypeFromString(input.Str("type"));
	DuckLakeMergeAdjacentOptions merge_options;
	merge_options.min_file_size = input.Opt("min_file_size");
	merge_options.max_file_size = input.Opt("max_file_size");
	merge_options.newer_than = input.GetValue("newer_than");
	merge_options.older_than = input.GetValue("older_than");
	DuckLakeFileSizeOptions options(merge_options, input.UInt("target_file_size"));
	auto delete_threshold = input.TryGet("delete_threshold");
	auto files = request.GetMetadataManager().GetFilesForCompaction(
	    table, type, delete_threshold.IsNull() ? 0 : delete_threshold.Double(), snapshot, options);
	auto files_out = output.Arr("files");
	for (auto &file : files) {
		DuckLakeRESTSerializer::Write(files_out.AppendObj(), file);
	}
}

static void TableSizesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto tables = output.Arr("tables");
	for (auto &table : request.GetMetadataManager().GetTableSizes(request.GetSnapshot(input))) {
		DuckLakeRESTSerializer::Write(tables.AppendObj(), table);
	}
}

static void ColumnMappingsEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto mappings = output.Arr("mappings");
	for (auto &mapping : request.GetMetadataManager().GetColumnMappings(input.Opt("start_from"))) {
		DuckLakeRESTSerializer::Write(mappings.AppendObj(), mapping);
	}
}

static void TableChangesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto snapshot = request.GetSnapshot(input);
	auto start_snapshot = request.GetSnapshot(input, "start_snapshot");
	auto &table = GetTableEntry(request, snapshot, TableIndex(input.UInt("table_id")));
	auto &metadata_manager = request.GetMetadataManager();
	auto files = output.Arr("files");
	if (input.Str("changes") == "deletions") {
		for (auto &entry : metadata_manager.GetTableDeletions(table, start_snapshot, snapshot)) {
			DuckLakeRESTSerializer::Write(files.AppendObj(), entry);
		}
		return;
	}
	for (auto &entry : metadata_manager.GetTableInsertions(table, start_snapshot, snapshot)) {
		DuckLakeRESTSerializer::Write(files.AppendObj(), entry);
	}
}

static void DuckLakeTablesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto names = output.Arr("table_names");
	for (auto &name : request.GetMetadataManager().GetMetadataTableNames()) {
		names.AppendStr(name);
	}
}

static void DuckLakeTableEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto result = request.GetMetadataManager().ReadMetadataTable(input.Str("table_name"));
	WriteQueryResult(output, *result);
}

static void KnownFilesEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	auto paths = output.Arr("paths");
	for (auto &path : request.GetMetadataManager().GetKnownFilePaths()) {
		paths.AppendStr(path);
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
	void CreateView(RESTJsonIn parameters);
	void CreateMacro(RESTJsonIn parameters);
	void Alter(RESTJsonIn parameters);
	void Drop(CatalogType type, RESTJsonIn parameters);
	void InsertRows(RESTJsonIn parameters);
	void InsertFiles(RESTJsonIn parameters);
	void DeleteData(RESTJsonIn parameters);
	void FlushInlinedData(RESTJsonIn parameters);
	void Compact(RESTJsonIn parameters);
	void CreateNameMap(RESTJsonIn parameters);
	void SetOption(RESTJsonIn parameters);
	void RemapNameMap(DuckLakeDataFile &file);
	//! Maps the id of a partitioning that the client set to the id of the partitioning of the table
	void RegisterPartition(DuckLakeTableEntry &table, RESTJsonIn parameters);
	//! Files written for a partitioning that the transaction set refer to the partitioning of the client
	void RemapPartition(DuckLakeTableEntry &table, DuckLakeDataFile &file);

	DuckLakeSchemaEntry &GetSchema(RESTJsonIn schema_path);
	//! The qualified name of an entry in a schema of the catalog
	QualifiedName GetEntryName(RESTJsonIn schema_path, const string &name);
	//! Resolves an id of the request, negative ids refer to entries created by earlier requests
	TableIndex GetTableId(RESTJsonIn id);
	DuckLakeTableEntry &GetTable(TableIndex table_id);

private:
	DuckLakeRESTRequest &request;
	DuckLakeTransaction &transaction;
	DuckLakeCatalog &catalog;
	ClientContext &context;
	map<int64_t, TableIndex> placeholder_ids;
	//! The schemas of the tables created by the transaction
	map<TableIndex, SchemaIndex> local_table_schemas;
	//! The tables created by the requests with their placeholder ids, in the order they were created
	vector<pair<RESTJsonIn, TableIndex>> created_tables;
	//! The name maps of the client transaction, by their id in the client transaction
	map<idx_t, MappingIndex> name_map_ids;
	//! The partitionings set by the client transaction, by their id in the client transaction
	map<idx_t, idx_t> partition_ids;
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

QualifiedName DuckLakeRESTCommit::GetEntryName(RESTJsonIn schema_path, const string &name) {
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &schema_name : schema_path.Items()) {
		path.emplace_back(schema_name.Str());
	}
	if (path.size() == 1) {
		path.emplace_back(*catalog.GetDefaultSchema());
	}
	return QualifiedName(std::move(path), Identifier(name));
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
	if (!DuckLakeTransaction::IsTransactionLocal(table_id.index)) {
		auto entry = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
		if (entry && entry->type == CatalogType::TABLE_ENTRY) {
			return entry->Cast<DuckLakeTableEntry>();
		}
		throw CatalogException("Table with id %llu does not exist", table_id.index);
	}
	// tables created by the transaction are not in the id map of their catalog set
	auto schema_id = local_table_schemas.find(table_id);
	if (schema_id != local_table_schemas.end()) {
		auto local_entries = transaction.GetTransactionLocalEntries(CatalogType::TABLE_ENTRY, schema_id->second);
		if (local_entries) {
			for (auto &local_entry : local_entries->GetEntries()) {
				auto &catalog_entry = *local_entry.second;
				if (catalog_entry.type == CatalogType::TABLE_ENTRY &&
				    catalog_entry.Cast<DuckLakeTableEntry>().GetTableId() == table_id) {
					return catalog_entry.Cast<DuckLakeTableEntry>();
				}
			}
		}
	}
	throw CatalogException("Table with id %llu does not exist", table_id.index);
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
		auto not_null = column_in.Bool("not_null", false);
		auto logical_index = LogicalIndex(info->columns.LogicalColumnCount());
		info->columns.AddColumn(DuckLakeRESTCatalogChanges::ReadColumn(context, column_in));
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
	auto table_id = entry->Cast<DuckLakeTableEntry>().GetTableId();
	local_table_schemas[table_id] = schema.GetSchemaId();
	RegisterPartition(entry->Cast<DuckLakeTableEntry>(), parameters);
	auto placeholder = parameters.TryGet("table_id");
	if (!placeholder.IsNull()) {
		placeholder_ids[placeholder.Int()] = table_id;
	}
	created_tables.emplace_back(placeholder, table_id);
}

void DuckLakeRESTCommit::CreateView(RESTJsonIn parameters) {
	auto &schema = GetSchema(parameters.TryGet("schema_path"));
	auto dialect = parameters.Str("dialect", "duckdb");
	if (dialect != "duckdb") {
		throw NotImplementedException("Unsupported view dialect \"%s\"", dialect);
	}
	auto info = make_uniq<CreateViewInfo>(schema, Identifier(parameters.Str("view_name")));
	auto parser = Parser::GetBuiltinParser();
	info->query = CreateViewInfo::ParseSelect(parser, parameters.Str("sql"));
	for (auto &alias : parameters.TryGet("column_aliases").Items()) {
		info->aliases.emplace_back(alias.Str());
	}
	auto comment = parameters.TryGet("comment");
	if (!comment.IsNull()) {
		info->comment = Value(comment.Str());
	}
	schema.CreateView(catalog.GetCatalogTransaction(context), *info);
}

void DuckLakeRESTCommit::CreateMacro(RESTJsonIn parameters) {
	auto &schema = GetSchema(parameters.TryGet("schema_path"));
	auto macro = DuckLakeRESTSerializer::ReadMacroInfo(parameters.Get("macro"));
	auto info = CreateMacroInfoFromDucklake(context, macro, schema.name.GetIdentifierName());
	schema.CreateFunction(catalog.GetCatalogTransaction(context), *info);
}

void DuckLakeRESTCommit::Alter(RESTJsonIn parameters) {
	AlterEntryData entry(GetEntryName(parameters.TryGet("schema_path"), parameters.Str("name")),
	                     OnEntryNotFound::THROW_EXCEPTION);
	auto info = DuckLakeRESTCatalogChanges::ReadAlter(context, parameters.Get("alter"), entry);
	catalog.Alter(context, *info);
	if (parameters.Has("partition_id")) {
		auto &schema = GetSchema(parameters.TryGet("schema_path"));
		auto table = schema.GetEntry(catalog.GetCatalogTransaction(context), CatalogType::TABLE_ENTRY,
		                             info->GetQualifiedName().Name());
		if (table) {
			RegisterPartition(table->Cast<DuckLakeTableEntry>(), parameters);
		}
	}
}

void DuckLakeRESTCommit::Drop(CatalogType type, RESTJsonIn parameters) {
	DropInfo info;
	info.type = type;
	if (type == CatalogType::SCHEMA_ENTRY) {
		vector<Identifier> path;
		path.push_back(catalog.GetName());
		for (auto &name : parameters.Get("schema_path").Items()) {
			path.emplace_back(name.Str());
		}
		info.SetQualifiedName(QualifiedName(std::move(path), Identifier()));
		catalog.DropEntry(context, info);
		return;
	}
	if (type == CatalogType::MACRO_ENTRY && parameters.Str("macro_type", "scalar") == "table") {
		info.type = CatalogType::TABLE_MACRO_ENTRY;
	}
	info.SetQualifiedName(GetEntryName(parameters.TryGet("schema_path"), parameters.Str("name")));
	catalog.DropEntry(context, info);
}

void DuckLakeRESTCommit::DeleteData(RESTJsonIn parameters) {
	auto table_id = GetTable(GetTableId(parameters.Get("table_id"))).GetTableId();
	vector<DuckLakeDeleteFile> delete_files;
	for (auto &file : parameters.TryGet("delete_files").Items()) {
		delete_files.push_back(DuckLakeRESTSerializer::ReadDeleteFile(file));
	}
	if (!delete_files.empty()) {
		transaction.AddDeletes(table_id, std::move(delete_files));
	}
	for (auto &deletes : parameters.TryGet("inlined_deletes").Items()) {
		set<idx_t> row_ids;
		for (auto &row_id : deletes.Get("row_ids").Items()) {
			row_ids.insert(row_id.UInt());
		}
		transaction.AddNewInlinedDeletes(table_id, GetInlinedTableName(deletes), std::move(row_ids));
	}
	for (auto &deletes : parameters.TryGet("file_row_deletes").Items()) {
		set<idx_t> row_ids;
		for (auto &row_id : deletes.Get("row_ids").Items()) {
			row_ids.insert(row_id.UInt());
		}
		transaction.AddNewInlinedFileDeletes(table_id, deletes.UInt("data_file_id"), std::move(row_ids));
	}
	for (auto &file : parameters.TryGet("dropped_files").Items()) {
		transaction.DropFile(table_id, DataFileIndex(file.UInt("data_file_id")), file.Str("path"),
		                     file.UInt("record_count"), file.UInt("file_size_bytes"));
	}
	if (parameters.Bool("delete_attempted", false)) {
		transaction.MarkDeleteAttempted(table_id);
	}
}

void DuckLakeRESTCommit::FlushInlinedData(RESTJsonIn parameters) {
	for (auto &inlined_table_in : parameters.TryGet("inlined_tables").Items()) {
		DuckLakeInlinedTableInfo inlined_table;
		inlined_table.table_name = GetInlinedTableName(inlined_table_in);
		inlined_table.schema_version = inlined_table_in.UInt("schema_version");
		transaction.MarkInlinedDataForDeletion(std::move(inlined_table), inlined_table_in.UInt("flush_snapshot_id"));
	}
	for (auto &file_deletions : parameters.TryGet("file_deletions").Items()) {
		auto table_id = GetTable(GetTableId(file_deletions.Get("table_id"))).GetTableId();
		transaction.MarkInlinedFileDeletionsFlushed(table_id, file_deletions.UInt("flush_snapshot_id"));
	}
}

void DuckLakeRESTCommit::CreateNameMap(RESTJsonIn parameters) {
	auto mapping = DuckLakeRESTSerializer::ReadColumnMappingInfo(parameters.Get("mapping"));
	auto client_id = mapping.mapping_id.index;
	mapping.table_id = GetTable(GetTableId(parameters.Get("table_id"))).GetTableId();
	name_map_ids[client_id] = transaction.AddNameMap(DuckLakeNameMap::FromColumnMapping(std::move(mapping)));
}

void DuckLakeRESTCommit::RegisterPartition(DuckLakeTableEntry &table, RESTJsonIn parameters) {
	auto client_partition_id = parameters.TryGet("partition_id");
	auto partition_data = table.GetPartitionData();
	if (client_partition_id.IsNull() || !partition_data) {
		return;
	}
	partition_ids[client_partition_id.UInt()] = partition_data->partition_id;
}

void DuckLakeRESTCommit::RemapPartition(DuckLakeTableEntry &table, DuckLakeDataFile &file) {
	if (!file.partition_id.IsValid() || !DuckLakeTransaction::IsTransactionLocal(file.partition_id.GetIndex())) {
		return;
	}
	auto entry = partition_ids.find(file.partition_id.GetIndex());
	if (entry == partition_ids.end()) {
		throw InvalidInputException("File \"%s\" refers to a partitioning that was not set in this transaction",
		                            file.file_name);
	}
	file.partition_id = entry->second;
}

void DuckLakeRESTCommit::SetOption(RESTJsonIn parameters) {
	auto option_in = parameters.Get("option");
	DuckLakeConfigOption option;
	option.option.key = option_in.Str("key");
	option.option.value = option_in.Str("value", string());
	auto schema_id = option_in.TryGet("schema_id");
	if (!schema_id.IsNull()) {
		option.schema_id = SchemaIndex(schema_id.UInt());
	}
	auto table_id = option_in.TryGet("table_id");
	if (!table_id.IsNull()) {
		option.table_id = GetTable(GetTableId(table_id)).GetTableId();
	}
	if (parameters.Bool("reset", false)) {
		transaction.ResetConfigOption(option);
	} else {
		transaction.SetConfigOption(option);
	}
}

void DuckLakeRESTCommit::RemapNameMap(DuckLakeDataFile &file) {
	if (!file.mapping_id.IsValid()) {
		return;
	}
	auto entry = name_map_ids.find(file.mapping_id.index);
	if (entry != name_map_ids.end()) {
		file.mapping_id = entry->second;
	} else if (DuckLakeTransaction::IsTransactionLocal(file.mapping_id.index)) {
		throw InvalidInputException("Name map %llu does not refer to a name map created in this transaction",
		                            file.mapping_id.index);
	}
}

void DuckLakeRESTCommit::Compact(RESTJsonIn parameters) {
	auto table_id = GetTable(GetTableId(parameters.Get("table_id"))).GetTableId();
	for (auto &compaction : parameters.Get("compactions").Items()) {
		auto entry = DuckLakeRESTSerializer::ReadCompactionEntry(compaction);
		for (auto &file : entry.written_files) {
			RemapNameMap(file);
		}
		transaction.AddCompaction(table_id, std::move(entry));
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
		RemapNameMap(files.back());
		RemapPartition(table, files.back());
	}
	transaction.AppendFiles(table.GetTableId(), std::move(files));
}

void DuckLakeRESTCommit::ApplyRequest(const string &path, RESTJsonIn parameters) {
	if (path == "create_schema") {
		CreateSchema(parameters);
	} else if (path == "create_table") {
		CreateTable(parameters);
	} else if (path == "create_view") {
		CreateView(parameters);
	} else if (path == "create_macro") {
		CreateMacro(parameters);
	} else if (path == "alter_table" || path == "alter_view") {
		Alter(parameters);
	} else if (path == "drop_schema") {
		Drop(CatalogType::SCHEMA_ENTRY, parameters);
	} else if (path == "drop_table") {
		Drop(CatalogType::TABLE_ENTRY, parameters);
	} else if (path == "drop_view") {
		Drop(CatalogType::VIEW_ENTRY, parameters);
	} else if (path == "drop_macro") {
		Drop(CatalogType::MACRO_ENTRY, parameters);
	} else if (path == "insert_rows") {
		InsertRows(parameters);
	} else if (path == "insert_files") {
		InsertFiles(parameters);
	} else if (path == "delete_data") {
		DeleteData(parameters);
	} else if (path == "flush_inlined_data") {
		FlushInlinedData(parameters);
	} else if (path == "compact") {
		Compact(parameters);
	} else if (path == "create_name_map") {
		CreateNameMap(parameters);
	} else if (path == "set_option") {
		SetOption(parameters);
	} else {
		throw NotImplementedException("Unsupported request \"%s\" in commit_transaction", path);
	}
}

void DuckLakeRESTCommit::Run(RESTJsonIn input, RESTJsonOut output) {
	if (input.Has("snapshot") || input.Has("snapshot_id")) {
		// conflicts are detected against the snapshot the client transaction started from
		transaction.SetSnapshot(request.GetSnapshot(input));
	}
	auto retry = input.TryGet("retry");
	if (!retry.IsNull()) {
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_max_retry_count",
		                                  Value::UBIGINT(retry.UInt("max_retry_count")));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_wait_ms",
		                                  Value::UBIGINT(retry.UInt("retry_wait_ms")));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_backoff",
		                                  Value::DOUBLE(retry.Get("retry_backoff").Double()));
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
	auto &committed_tables = transaction_ref->GetCommittedTableIds();
	auto table_ids = output.Arr("table_ids");
	for (auto &entry : created_tables) {
		auto committed_table = committed_tables.find(entry.second);
		if (committed_table == committed_tables.end()) {
			// the table was dropped again
			continue;
		}
		auto table_id = table_ids.AppendObj();
		if (entry.first.IsNull()) {
			table_id.SetNull("placeholder");
		} else {
			table_id.SetInt("placeholder", entry.first.Int());
		}
		table_id.SetUInt("table_id", committed_table->second.index);
	}
}

static void CommitTransactionEndpoint(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output) {
	DuckLakeRESTCommit commit(request);
	commit.Run(input, output);
}

bool DuckLakeRESTServer::IsCommitRequest(const string &path) {
	static const unordered_set<string> requests = {
	    "create_schema", "create_table",       "create_view", "create_macro",    "alter_table", "alter_view",
	    "drop_schema",   "drop_table",         "drop_view",   "drop_macro",      "insert_rows", "insert_files",
	    "delete_data",   "flush_inlined_data", "compact",     "create_name_map", "set_option"};
	return requests.find(path) != requests.end();
}

const unordered_map<string, rest_endpoint_t> &DuckLakeRESTServer::GetEndpoints() {
	static const unordered_map<string, rest_endpoint_t> endpoints = {
	    {"config", ConfigEndpoint},
	    {"snapshot_data", SnapshotDataEndpoint},
	    {"select", SelectEndpoint},
	    {"read_inlined_data", ReadInlinedDataEndpoint},
	    {"deleted_inlined_rows", DeletedInlinedRowsEndpoint},
	    {"inlined_file_deletions", InlinedFileDeletionsEndpoint},
	    {"table_schema_versions", TableSchemaVersionsEndpoint},
	    {"list_snapshots", ListSnapshotsEndpoint},
	    {"expire_snapshots", ExpireSnapshotsEndpoint},
	    {"files_for_cleanup", FilesForCleanupEndpoint},
	    {"remove_cleaned_up_files", RemoveCleanedUpFilesEndpoint},
	    {"known_files", KnownFilesEndpoint},
	    {"ducklake_tables", DuckLakeTablesEndpoint},
	    {"ducklake_table", DuckLakeTableEndpoint},
	    {"files_for_compaction", FilesForCompactionEndpoint},
	    {"table_sizes", TableSizesEndpoint},
	    {"column_mappings", ColumnMappingsEndpoint},
	    {"table_changes", TableChangesEndpoint},
	    {"row_count", RowCountEndpoint},
	    {"inlined_tables", InlinedTablesEndpoint},
	    {"next_column_id", NextColumnIdEndpoint},
	    {"column_created_with_table", ColumnCreatedWithTableEndpoint},
	    {"commit_transaction", CommitTransactionEndpoint}};
	return endpoints;
}

} // namespace duckdb
