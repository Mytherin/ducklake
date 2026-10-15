#include "rest/ducklake_rest_server.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_filters.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enum_util.hpp"
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

//! Reads the columns and rows of a result, optionally with other names for the columns
static void ReadQueryResult(RESTRowSet &rows, QueryResult &result, const vector<string> &names = {}) {
	auto &types = result.GetTypes();
	for (idx_t col_idx = 0; col_idx < result.ColumnCount(); col_idx++) {
		RESTResultColumn column;
		column.name = names.empty() ? result.GetNames()[col_idx].GetIdentifierName() : names[col_idx];
		column.type = types[col_idx];
		rows.columns.push_back(std::move(column));
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		collection->Append(*chunk);
	}
	rows.rows = RESTRows(std::move(collection));
}

//! The name of the inlined data table of a table at a schema version. Requests refer to inlined data by the table and
//! the schema version, the names are the ones of the catalog.
static string GetInlinedTableName(DuckLakeTransaction &transaction, const DuckLakeTableEntry &table,
                                  DuckLakeSnapshot snapshot, idx_t schema_version) {
	for (auto &inlined_table : table.GetInlinedDataTables(transaction, snapshot, false)) {
		if (inlined_table.schema_version == schema_version) {
			return inlined_table.table_name;
		}
	}
	throw CatalogException("Table %llu has no inlined data for schema version %llu", table.GetTableId().index,
	                       schema_version);
}

static bool IsColumnName(const ParsedExpression &expression) {
	return expression.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
	       !expression.Cast<ColumnRefExpression>().IsQualified();
}

//! The columns to read of a request, which can only be column names, optionally cast to the type to read them as
static vector<string> GetColumnsToRead(const vector<string> &columns_to_read) {
	vector<string> columns;
	for (auto &column : columns_to_read) {
		auto expressions = Parser::GetBuiltinParser().ParseExpressionList(column);
		bool valid = expressions.size() == 1;
		if (valid && expressions[0]->GetExpressionClass() == ExpressionClass::CAST) {
			valid = IsColumnName(expressions[0]->Cast<CastExpression>().Child());
		} else if (valid) {
			valid = IsColumnName(*expressions[0]);
		}
		if (!valid) {
			throw InvalidInputException("\"%s\" is not a column name", column);
		}
		columns.push_back(expressions[0]->ToString());
	}
	return columns;
}

//===--------------------------------------------------------------------===//
// Read Endpoints
//===--------------------------------------------------------------------===//
DuckLakeMetadata DuckLakeRESTEndpoints::Config(DuckLakeRESTRequest &request, const RESTConfigRequest &parameters) {
	auto metadata = request.GetMetadataManager().LoadDuckLake();
	for (auto &tag : metadata.tags) {
		if (tag.key == "data_path") {
			// clients get the resolved data path
			tag.value = request.catalog->DataPath();
		}
	}
	return metadata;
}

RESTSnapshotDataResponse DuckLakeRESTEndpoints::SnapshotData(DuckLakeRESTRequest &request,
                                                             const RESTSnapshotDataRequest &parameters) {
	auto &metadata_manager = request.GetMetadataManager();
	RESTSnapshotDataResponse result;
	if (parameters.at) {
		BoundAtClause at_clause(Identifier(parameters.at->unit), parameters.at->value);
		auto bound = parameters.bound == "lower" ? SnapshotBound::LOWER_BOUND : SnapshotBound::UPPER_BOUND;
		auto newest_snapshot_id = parameters.newest_snapshot_id.IsValid() ? parameters.newest_snapshot_id.GetIndex()
		                                                                  : DConstants::INVALID_INDEX;
		result.snapshot = *metadata_manager.GetSnapshot(at_clause, bound, newest_snapshot_id);
	} else {
		result.snapshot = request.GetSnapshot(parameters);
	}
	auto &snapshot = result.snapshot;
	// clients that pass the versions they have loaded get the catalog and statistics if they changed
	bool include_catalog =
	    parameters.include_catalog || (parameters.cached_metadata_version.IsValid() &&
	                                   parameters.cached_metadata_version.GetIndex() != snapshot.schema_version);
	bool include_stats =
	    parameters.include_stats || (parameters.cached_data_version.IsValid() &&
	                                 parameters.cached_data_version.GetIndex() != snapshot.next_file_id);
	if (include_catalog) {
		result.catalog = make_uniq<DuckLakeCatalogInfo>(metadata_manager.GetCatalogForSnapshot(snapshot));
	}
	if (include_stats) {
		result.stats = metadata_manager.GetGlobalTableStats(snapshot);
	}
	return result;
}

//! Reads the inlined rows of a table for every schema version that it has inlined rows for, with the row id, the begin
//! and end snapshot and all columns of the table at that schema version
static vector<RESTInlinedTableRows>
ReadInlinedTableRows(DuckLakeRESTRequest &request, DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                     const std::function<unique_ptr<QueryResult>(const string &, const vector<string> &)> &read_rows,
                     const RESTOptional<vector<idx_t>> &projection = RESTOptional<vector<idx_t>>()) {
	unordered_set<idx_t> projected_fields(projection.value.begin(), projection.value.end());
	auto &metadata_manager = request.GetMetadataManager();
	auto col_names = metadata_manager.InlinedColNames();
	vector<RESTInlinedTableRows> result;
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
			auto field_id = table_at_version->GetFieldId(column.Physical()).GetFieldIndex().index;
			if (projection.has_value && projected_fields.find(field_id) == projected_fields.end()) {
				continue;
			}
			names.push_back(column.Name().GetIdentifierName());
			columns.push_back(
			    metadata_manager.CastColumnToTarget(SQLIdentifier::ToString(names.back()), column.Type()));
		}
		auto query_result = read_rows(inlined_table.table_name, columns);
		query_result->ThrowIfError("Failed to read inlined data: ");
		RESTInlinedTableRows rows;
		rows.schema_version = inlined_table.schema_version;
		ReadQueryResult(rows, *query_result, names);
		result.push_back(std::move(rows));
	}
	return result;
}

RESTSelectResponse DuckLakeRESTEndpoints::Select(DuckLakeRESTRequest &request, const RESTSelectRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	auto &metadata_manager = request.GetMetadataManager();
	unique_ptr<FilterPushdownInfo> filters;
	if (parameters.filters) {
		filters = DuckLakeRESTFilters::Read(*request.connection.context, *parameters.filters, table);
	}
	RESTSelectResponse result;
	if (parameters.extended) {
		result.extended_files = metadata_manager.GetExtendedFilesForTable(table, snapshot, filters.get());
	} else {
		result.files = metadata_manager.GetFilesForTable(table, snapshot, filters.get());
	}
	if (!parameters.include_inlined_data) {
		return result;
	}
	result.inlined_data = ReadInlinedTableRows(
	    request, table, snapshot,
	    [&](const string &inlined_table_name, const vector<string> &columns) {
		    return metadata_manager.ReadInlinedData(snapshot, inlined_table_name, columns);
	    },
	    parameters.columns);
	return result;
}

//! The table at the schema version of its inlined data, and the name of the inlined data table
static DuckLakeTableEntry &GetInlinedTableEntry(DuckLakeRESTRequest &request, DuckLakeSnapshot snapshot,
                                                TableIndex table_id, idx_t schema_version, string &inlined_table_name) {
	auto &table = GetTableEntry(request, snapshot, table_id);
	inlined_table_name = GetInlinedTableName(*request.transaction, table, snapshot, schema_version);
	auto result = request.catalog->GetTableAtSchemaVersion(*request.transaction, table_id, schema_version);
	if (!result) {
		throw CatalogException("Table %llu has no schema version %llu", table_id.index, schema_version);
	}
	return *result;
}

//! Rows that the client transaction deleted are not flushed, the request transaction is rolled back afterwards
static void AddFlushDeletes(DuckLakeRESTRequest &request, const set<idx_t> &deleted_row_ids, DuckLakeTableEntry &table,
                            const string &inlined_table_name) {
	request.transaction->AddNewInlinedDeletes(table.GetTableId(), inlined_table_name, set<idx_t>(deleted_row_ids));
}

//! The ORDER BY of a flush, parsed so that it can only contain ordering expressions
static string GetFlushOrder(const string &order_by) {
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

RESTInlinedRows DuckLakeRESTEndpoints::InlinedDataForFlush(DuckLakeRESTRequest &request,
                                                           const RESTInlinedDataForFlushRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto columns = GetColumnsToRead(parameters.columns);
	string inlined_table_name;
	auto &table = GetInlinedTableEntry(request, snapshot, TableIndex(parameters.table_id), parameters.schema_version,
	                                   inlined_table_name);
	AddFlushDeletes(request, parameters.deleted_row_ids, table, inlined_table_name);
	auto query_result = request.GetMetadataManager().ReadAllInlinedDataForFlush(
	    snapshot, inlined_table_name, table, GetFlushOrder(parameters.order_by), columns);
	query_result->ThrowIfError("Failed to read inlined data: ");
	RESTInlinedRows result;
	ReadQueryResult(result, *query_result);
	request.connection.Rollback();
	return result;
}

RESTDeletedInlinedRowsResponse
DuckLakeRESTEndpoints::DeletedInlinedRows(DuckLakeRESTRequest &request,
                                          const RESTDeletedInlinedRowsRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	string inlined_table_name;
	auto &table = GetInlinedTableEntry(request, snapshot, TableIndex(parameters.table_id), parameters.schema_version,
	                                   inlined_table_name);
	AddFlushDeletes(request, parameters.deleted_row_ids, table, inlined_table_name);
	RESTDeletedInlinedRowsResponse result;
	for (auto &row : request.GetMetadataManager().GetDeletedInlinedRowVersions(table, inlined_table_name, snapshot)) {
		RESTDeletedRowVersion version;
		version.row_id = row.first.first;
		version.begin_snapshot = row.first.second;
		version.end_snapshot = row.second;
		result.rows.push_back(version);
	}
	request.connection.Rollback();
	return result;
}

RESTInlinedFileDeletionsResponse
DuckLakeRESTEndpoints::InlinedFileDeletions(DuckLakeRESTRequest &request,
                                            const RESTInlinedFileDeletionsRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	RESTInlinedFileDeletionsResponse result;
	result.deletions = request.GetMetadataManager().GetInlinedFileDeletionsForFlush(table, snapshot);
	return result;
}

RESTTableSchemaVersionsResponse
DuckLakeRESTEndpoints::TableSchemaVersions(DuckLakeRESTRequest &request,
                                           const RESTTableSchemaVersionsRequest &parameters) {
	auto &metadata_manager = request.GetMetadataManager();
	auto table_id = TableIndex(parameters.table_id);
	RESTTableSchemaVersionsResponse result;
	result.begin_snapshot = metadata_manager.GetBeginSnapshotForTable(table_id);
	if (!RESTIsNull(parameters.snapshot) || parameters.snapshot_id.IsValid()) {
		auto versions = metadata_manager.GetTableSchemaVersions(table_id, request.GetSnapshot(parameters));
		result.creation = versions.creation;
		result.last_change = versions.last_change;
	}
	if (parameters.schema_version.IsValid()) {
		result.schema_version_begin_snapshot =
		    metadata_manager.GetBeginSnapshotForSchemaVersion(table_id, parameters.schema_version.GetIndex());
	}
	return result;
}

RESTRowCountResponse DuckLakeRESTEndpoints::RowCount(DuckLakeRESTRequest &request,
                                                     const RESTRowCountRequest &parameters) {
	auto &metadata_manager = request.GetMetadataManager();
	auto snapshot = request.GetSnapshot(parameters);
	RESTRowCountResponse result;
	auto table_id = TableIndex(parameters.table_id);
	if (parameters.count == "inlined") {
		if (!parameters.schema_version.IsValid()) {
			throw InvalidInputException("row_count: counting inlined rows requires a schema_version");
		}
		auto table_name = GetInlinedTableName(*request.transaction, GetTableEntry(request, snapshot, table_id),
		                                      snapshot, parameters.schema_version.GetIndex());
		result.row_count = metadata_manager.GetNetInlinedRowCount(table_name, snapshot);
		return result;
	}
	if (parameters.count == "data_files") {
		result.row_count = metadata_manager.GetNetDataFileRowCount(table_id, snapshot);
	} else {
		D_ASSERT(parameters.count == "data_files_exact");
		result.row_count = metadata_manager.GetNetDataFileRowCountForStats(table_id, snapshot);
	}
	return result;
}

RESTInlinedTablesResponse DuckLakeRESTEndpoints::InlinedTables(DuckLakeRESTRequest &request,
                                                               const RESTInlinedTablesRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	RESTInlinedTablesResponse result;
	for (auto &inlined_table : table.GetInlinedDataTables(*request.transaction, snapshot, false)) {
		result.schema_versions.push_back(inlined_table.schema_version);
	}
	return result;
}

RESTNextColumnIdResponse DuckLakeRESTEndpoints::NextColumnId(DuckLakeRESTRequest &request,
                                                             const RESTNextColumnIdRequest &parameters) {
	RESTNextColumnIdResponse result;
	result.next_column_id = request.GetMetadataManager().GetNextColumnId(TableIndex(parameters.table_id));
	return result;
}

RESTListSnapshotsResponse DuckLakeRESTEndpoints::ListSnapshots(DuckLakeRESTRequest &request,
                                                               const RESTListSnapshotsRequest &parameters) {
	DuckLakeSnapshotFilter filter;
	filter.exclude_latest = parameters.exclude_latest;
	filter.older_than = parameters.older_than;
	filter.has_snapshot_ids = parameters.snapshot_ids.has_value;
	filter.snapshot_ids = parameters.snapshot_ids.value;
	RESTListSnapshotsResponse result;
	result.snapshots = request.GetMetadataManager().GetAllSnapshots(filter);
	return result;
}

RESTTableSizesResponse DuckLakeRESTEndpoints::TableSizes(DuckLakeRESTRequest &request,
                                                         const RESTTableSizesRequest &parameters) {
	RESTTableSizesResponse result;
	result.tables = request.GetMetadataManager().GetTableSizes(request.GetSnapshot(parameters));
	return result;
}

RESTTablesWithLiveDataFilesResponse
DuckLakeRESTEndpoints::TablesWithLiveDataFiles(DuckLakeRESTRequest &request,
                                               const RESTTablesWithLiveDataFilesRequest &parameters) {
	RESTTablesWithLiveDataFilesResponse result;
	for (auto table_id : request.GetMetadataManager().GetTablesWithLiveDataFiles()) {
		result.table_ids.push_back(table_id);
	}
	return result;
}

RESTColumnMappingsResponse DuckLakeRESTEndpoints::ColumnMappings(DuckLakeRESTRequest &request,
                                                                 const RESTColumnMappingsRequest &parameters) {
	RESTColumnMappingsResponse result;
	result.mappings = request.GetMetadataManager().GetColumnMappings(parameters.start_from);
	return result;
}

RESTTableChangesResponse DuckLakeRESTEndpoints::TableChanges(DuckLakeRESTRequest &request,
                                                             const RESTTableChangesRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto start_snapshot = request.GetSnapshot(parameters.start_snapshot, parameters.start_snapshot_id);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	auto &metadata_manager = request.GetMetadataManager();
	RESTTableChangesResponse result;
	bool deletions = parameters.changes == "deletions";
	if (deletions) {
		result.deletions = metadata_manager.GetTableDeletions(table, start_snapshot, snapshot);
	} else {
		result.insertions = metadata_manager.GetTableInsertions(table, start_snapshot, snapshot);
	}
	if (parameters.include_inlined_data) {
		result.inlined_data = ReadInlinedTableRows(
		    request, table, snapshot,
		    [&](const string &inlined_table_name, const vector<string> &columns) {
			    if (deletions) {
				    return metadata_manager.ReadInlinedDataDeletions(start_snapshot, snapshot, inlined_table_name,
				                                                     columns);
			    }
			    return metadata_manager.ReadInlinedDataInsertions(start_snapshot, snapshot, inlined_table_name,
			                                                      columns);
		    },
		    parameters.columns);
	}
	return result;
}

RESTDuckLakeTablesResponse DuckLakeRESTEndpoints::DuckLakeTables(DuckLakeRESTRequest &request,
                                                                 const RESTDuckLakeTablesRequest &parameters) {
	RESTDuckLakeTablesResponse result;
	result.table_names = request.GetMetadataManager().GetMetadataTableNames();
	return result;
}

RESTInlinedRows DuckLakeRESTEndpoints::DuckLakeTable(DuckLakeRESTRequest &request,
                                                     const RESTDuckLakeTableRequest &parameters) {
	auto query_result = request.GetMetadataManager().ReadMetadataTable(parameters.table_name);
	RESTInlinedRows result;
	ReadQueryResult(result, *query_result);
	return result;
}

//===--------------------------------------------------------------------===//
// Maintenance Endpoints
//===--------------------------------------------------------------------===//
RESTExpireSnapshotsResponse DuckLakeRESTEndpoints::ExpireSnapshots(DuckLakeRESTRequest &request,
                                                                   const RESTExpireSnapshotsRequest &parameters) {
	DuckLakeSnapshotFilter filter;
	filter.has_snapshot_ids = true;
	filter.snapshot_ids = parameters.snapshot_ids;
	auto &metadata_manager = request.GetMetadataManager();
	auto snapshots = metadata_manager.GetAllSnapshots(filter);
	if (!snapshots.empty()) {
		metadata_manager.DeleteSnapshots(snapshots);
		for (auto &snapshot : snapshots) {
			request.catalog->InvalidateSchemaCache(snapshot.schema_version);
		}
	}
	RESTExpireSnapshotsResponse result;
	result.expired_snapshots = snapshots.size();
	return result;
}

RESTFilesForCleanupResponse DuckLakeRESTEndpoints::FilesForCleanup(DuckLakeRESTRequest &request,
                                                                   const RESTFilesForCleanupRequest &parameters) {
	RESTFilesForCleanupResponse result;
	result.files = request.GetMetadataManager().GetOldFilesForCleanup(parameters.older_than);
	return result;
}

void DuckLakeRESTEndpoints::RemoveCleanedUpFiles(DuckLakeRESTRequest &request,
                                                 const RESTRemoveCleanedUpFilesRequest &parameters) {
	vector<DuckLakeFileForCleanup> files;
	for (auto &file_id : parameters.data_file_ids) {
		DuckLakeFileForCleanup file;
		file.id = DataFileIndex(file_id);
		files.push_back(std::move(file));
	}
	if (!files.empty()) {
		request.GetMetadataManager().RemoveFilesScheduledForCleanup(files);
	}
}

RESTFilesForCompactionResponse
DuckLakeRESTEndpoints::FilesForCompaction(DuckLakeRESTRequest &request,
                                          const RESTFilesForCompactionRequest &parameters) {
	auto snapshot = request.GetSnapshot(parameters);
	auto &table = GetTableEntry(request, snapshot, TableIndex(parameters.table_id));
	DuckLakeMergeAdjacentOptions merge_options;
	merge_options.min_file_size = parameters.min_file_size;
	merge_options.max_file_size = parameters.max_file_size;
	merge_options.newer_than = parameters.newer_than;
	merge_options.older_than = parameters.older_than;
	DuckLakeFileSizeOptions options(merge_options, parameters.target_file_size);
	auto delete_threshold = parameters.delete_threshold.has_value ? parameters.delete_threshold.value : 0;
	RESTFilesForCompactionResponse result;
	result.files =
	    request.GetMetadataManager().GetFilesForCompaction(table, parameters.type, delete_threshold, snapshot, options);
	return result;
}

RESTKnownFilesResponse DuckLakeRESTEndpoints::KnownFiles(DuckLakeRESTRequest &request,
                                                         const RESTKnownFilesRequest &parameters) {
	RESTKnownFilesResponse result;
	result.paths = request.GetMetadataManager().GetKnownFilePaths();
	return result;
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//
//! Replays the operations of a client transaction in the transaction of the request
class DuckLakeRESTCommit : public DuckLakeRESTCommitHandler {
public:
	explicit DuckLakeRESTCommit(DuckLakeRESTRequest &request)
	    : request(request), transaction(*request.transaction), catalog(*request.catalog),
	      context(*request.connection.context) {
	}

	RESTCommitTransactionResponse Run(const RESTCommitTransactionRequest &parameters);

protected:
	void CreateSchema(RESTCreateSchemaRequest &parameters) override;
	void CreateTable(RESTCreateTableRequest &parameters) override;
	void CreateView(RESTCreateViewRequest &parameters) override;
	void CreateMacro(RESTCreateMacroRequest &parameters) override;
	void AlterTable(RESTAlterTableRequest &parameters) override;
	void AlterView(RESTAlterViewRequest &parameters) override;
	void DropSchema(RESTDropSchemaRequest &parameters) override;
	void DropTable(RESTDropTableRequest &parameters) override;
	void DropView(RESTDropViewRequest &parameters) override;
	void DropMacro(RESTDropMacroRequest &parameters) override;
	void SetOption(RESTSetOptionRequest &parameters) override;
	void CreateNameMap(RESTCreateNameMapRequest &parameters) override;
	void InsertRows(RESTInsertRowsRequest &parameters) override;
	void InsertFiles(RESTInsertFilesRequest &parameters) override;
	void DeleteData(RESTDeleteDataRequest &parameters) override;
	void FlushInlinedData(RESTFlushInlinedDataRequest &parameters) override;
	void Compact(RESTCompactRequest &parameters) override;

private:
	void Alter(const vector<string> &schema_path, const string &name, const RESTAlteration &alter,
	           optional_idx partition_id);
	void Drop(CatalogType type, const vector<string> &schema_path, const string &name);
	void RemapNameMap(DuckLakeDataFile &file);
	//! Maps the id of a partitioning that the client set to the id of the partitioning of the table
	void RegisterPartition(DuckLakeTableEntry &table, optional_idx client_partition_id);
	//! Files written for a partitioning that the transaction set refer to the partitioning of the client
	void RemapPartition(DuckLakeTableEntry &table, DuckLakeDataFile &file);

	DuckLakeSchemaEntry &GetSchema(const vector<string> &schema_path);
	//! The qualified name of an entry in a schema of the catalog
	QualifiedName GetEntryName(const vector<string> &schema_path, const string &name);
	//! The qualified name of a schema
	QualifiedName GetSchemaName(const vector<string> &schema_path);
	//! Resolves an id of the request, negative ids refer to entries created by earlier requests
	TableIndex GetTableId(int64_t id);
	DuckLakeTableEntry &GetTable(TableIndex table_id);
	void AddEntryReference(optional_ptr<CatalogEntry> entry, optional_idx reference);

private:
	DuckLakeRESTRequest &request;
	DuckLakeTransaction &transaction;
	DuckLakeCatalog &catalog;
	ClientContext &context;
	map<int64_t, TableIndex> placeholder_ids;
	//! The schemas of the tables created by the transaction
	map<TableIndex, SchemaIndex> local_table_schemas;
	//! The tables created by the requests with their placeholder ids, in the order they were created
	vector<pair<RESTOptional<int64_t>, TableIndex>> created_tables;
	//! The name maps of the client transaction, by their id in the client transaction
	map<idx_t, MappingIndex> name_map_ids;
	//! The partitionings set by the client transaction, by their id in the client transaction
	map<idx_t, idx_t> partition_ids;
	//! The client references of the created entries, by the oid of the entry
	map<idx_t, idx_t> entry_references;
};

void DuckLakeRESTCommit::AddEntryReference(optional_ptr<CatalogEntry> entry, optional_idx reference) {
	if (entry && reference.IsValid()) {
		entry_references[entry->oid] = reference.GetIndex();
	}
}

DuckLakeSchemaEntry &DuckLakeRESTCommit::GetSchema(const vector<string> &schema_path) {
	vector<Identifier> path;
	for (auto &name : schema_path) {
		path.emplace_back(name);
	}
	if (path.empty()) {
		path.emplace_back(*catalog.GetDefaultSchema());
	}
	auto schema = catalog.GetSchema(catalog.GetCatalogTransaction(context), path, OnEntryNotFound::THROW_EXCEPTION);
	return schema->Cast<DuckLakeSchemaEntry>();
}

QualifiedName DuckLakeRESTCommit::GetEntryName(const vector<string> &schema_path, const string &name) {
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &schema_name : schema_path) {
		path.emplace_back(schema_name);
	}
	if (path.size() == 1) {
		path.emplace_back(*catalog.GetDefaultSchema());
	}
	return QualifiedName(std::move(path), Identifier(name));
}

QualifiedName DuckLakeRESTCommit::GetSchemaName(const vector<string> &schema_path) {
	vector<Identifier> path;
	path.push_back(catalog.GetName());
	for (auto &name : schema_path) {
		path.emplace_back(name);
	}
	return QualifiedName(std::move(path), Identifier());
}

TableIndex DuckLakeRESTCommit::GetTableId(int64_t table_id) {
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

void DuckLakeRESTCommit::CreateSchema(RESTCreateSchemaRequest &parameters) {
	if (parameters.schema_path.empty()) {
		throw InvalidInputException("create_schema requires a non-empty schema_path");
	}
	auto info = make_uniq<CreateSchemaInfo>();
	info->SetQualifiedName(GetSchemaName(parameters.schema_path));
	info->on_conflict =
	    parameters.if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	AddEntryReference(catalog.CreateSchema(catalog.GetCatalogTransaction(context), *info), parameters.entry_reference);
}

void DuckLakeRESTCommit::CreateTable(RESTCreateTableRequest &parameters) {
	auto &schema = GetSchema(parameters.schema_path);
	auto info = make_uniq<CreateTableInfo>(schema, Identifier(parameters.table_name));
	for (auto &column : parameters.columns) {
		auto logical_index = LogicalIndex(info->columns.LogicalColumnCount());
		info->columns.AddColumn(DuckLakeRESTCatalogChanges::ReadColumn(context, column));
		if (column.not_null) {
			info->constraints.push_back(make_uniq<NotNullConstraint>(logical_index));
		}
	}
	for (auto &expr : parameters.partitioned_by) {
		info->partition_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr));
	}
	for (auto &expr : parameters.sorted_by) {
		info->sort_keys.push_back(Parser::GetBuiltinParser().ParseSingleExpression(expr));
	}
	for (auto &option : parameters.options) {
		info->options[option.first] = Parser::GetBuiltinParser().ParseSingleExpression(option.second);
	}
	if (parameters.comment.has_value) {
		info->comment = Value(parameters.comment.value);
	}
	BoundCreateTableInfo bound_info(schema, std::move(info));
	auto uuid = parameters.table_uuid.empty() ? transaction.GenerateUUID() : parameters.table_uuid;
	auto path = parameters.path;
	if (path.empty()) {
		path = schema.GenerateTableDataPath(uuid, bound_info.Base().GetTableName().GetIdentifierName());
	}
	auto entry = schema.CreateTableExtended(catalog.GetCatalogTransaction(context), bound_info, std::move(uuid),
	                                        std::move(path));
	if (!entry) {
		throw CatalogException("Table \"%s\" already exists", parameters.table_name);
	}
	AddEntryReference(entry, parameters.entry_reference);
	auto &table = entry->Cast<DuckLakeTableEntry>();
	auto table_id = table.GetTableId();
	local_table_schemas[table_id] = schema.GetSchemaId();
	RegisterPartition(table, parameters.partition_id);
	if (parameters.table_id.has_value) {
		placeholder_ids[parameters.table_id.value] = table_id;
	}
	created_tables.emplace_back(parameters.table_id, table_id);
}

void DuckLakeRESTCommit::CreateView(RESTCreateViewRequest &parameters) {
	auto &schema = GetSchema(parameters.schema_path);
	if (parameters.dialect != "duckdb") {
		throw NotImplementedException("Unsupported view dialect \"%s\"", parameters.dialect);
	}
	auto info = make_uniq<CreateViewInfo>(schema, Identifier(parameters.view_name));
	auto parser = Parser::GetBuiltinParser();
	info->query = CreateViewInfo::ParseSelect(parser, parameters.sql);
	for (auto &alias : parameters.column_aliases) {
		info->aliases.emplace_back(alias);
	}
	if (parameters.comment.has_value) {
		info->comment = Value(parameters.comment.value);
	}
	for (auto &entry : parameters.column_comments) {
		info->column_comments_map[Identifier(entry.first)] = Value(entry.second);
	}
	AddEntryReference(schema.CreateView(catalog.GetCatalogTransaction(context), *info), parameters.entry_reference);
}

void DuckLakeRESTCommit::CreateMacro(RESTCreateMacroRequest &parameters) {
	auto &schema = GetSchema(parameters.schema_path);
	auto info = CreateMacroInfoFromDucklake(context, parameters.macro, schema.name.GetIdentifierName());
	AddEntryReference(schema.CreateFunction(catalog.GetCatalogTransaction(context), *info), parameters.entry_reference);
}

void DuckLakeRESTCommit::Alter(const vector<string> &schema_path, const string &name, const RESTAlteration &alter,
                               optional_idx partition_id) {
	AlterEntryData entry(GetEntryName(schema_path, name), OnEntryNotFound::THROW_EXCEPTION);
	auto info = DuckLakeRESTCatalogChanges::ReadAlter(context, alter, entry);
	catalog.Alter(context, *info);
	if (partition_id.IsValid()) {
		auto &schema = GetSchema(schema_path);
		auto table = schema.GetEntry(catalog.GetCatalogTransaction(context), CatalogType::TABLE_ENTRY,
		                             info->GetQualifiedName().Name());
		if (table) {
			RegisterPartition(table->Cast<DuckLakeTableEntry>(), partition_id);
		}
	}
}

void DuckLakeRESTCommit::AlterTable(RESTAlterTableRequest &parameters) {
	Alter(parameters.schema_path, parameters.name, *parameters.alter, parameters.partition_id);
}

void DuckLakeRESTCommit::AlterView(RESTAlterViewRequest &parameters) {
	Alter(parameters.schema_path, parameters.name, *parameters.alter, optional_idx());
}

void DuckLakeRESTCommit::Drop(CatalogType type, const vector<string> &schema_path, const string &name) {
	DropInfo info;
	info.type = type;
	info.SetQualifiedName(GetEntryName(schema_path, name));
	catalog.DropEntry(context, info);
}

void DuckLakeRESTCommit::DropSchema(RESTDropSchemaRequest &parameters) {
	DropInfo info;
	info.type = CatalogType::SCHEMA_ENTRY;
	info.SetQualifiedName(GetSchemaName(parameters.schema_path));
	catalog.DropEntry(context, info);
}

void DuckLakeRESTCommit::DropTable(RESTDropTableRequest &parameters) {
	Drop(CatalogType::TABLE_ENTRY, parameters.schema_path, parameters.name);
}

void DuckLakeRESTCommit::DropView(RESTDropViewRequest &parameters) {
	Drop(CatalogType::VIEW_ENTRY, parameters.schema_path, parameters.name);
}

void DuckLakeRESTCommit::DropMacro(RESTDropMacroRequest &parameters) {
	auto type = parameters.macro_type == "table" ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY;
	Drop(type, parameters.schema_path, parameters.name);
}

void DuckLakeRESTCommit::DeleteData(RESTDeleteDataRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	auto table_id = table.GetTableId();
	if (!parameters.delete_files.empty()) {
		transaction.AddDeletes(table_id, std::move(parameters.delete_files));
	}
	for (auto &deletes : parameters.inlined_deletes) {
		auto inlined_table_name =
		    GetInlinedTableName(transaction, table, transaction.GetSnapshot(), deletes.schema_version);
		transaction.AddNewInlinedDeletes(table_id, inlined_table_name, std::move(deletes.row_ids));
	}
	for (auto &deletes : parameters.file_row_deletes) {
		transaction.AddNewInlinedFileDeletes(table_id, deletes.data_file_id, std::move(deletes.row_ids));
	}
	for (auto &file : parameters.dropped_files) {
		transaction.DropFile(table_id, DataFileIndex(file.data_file_id), file.path, file.record_count,
		                     file.file_size_bytes);
	}
	if (parameters.delete_attempted) {
		transaction.MarkDeleteAttempted(table_id);
	}
}

void DuckLakeRESTCommit::FlushInlinedData(RESTFlushInlinedDataRequest &parameters) {
	for (auto &flushed_table : parameters.inlined_tables) {
		DuckLakeInlinedTableInfo inlined_table;
		auto &table = GetTable(GetTableId(flushed_table.table_id));
		inlined_table.table_name =
		    GetInlinedTableName(transaction, table, transaction.GetSnapshot(), flushed_table.schema_version);
		inlined_table.schema_version = flushed_table.schema_version;
		transaction.MarkInlinedDataForDeletion(std::move(inlined_table), flushed_table.flush_snapshot_id,
		                                       flushed_table.wrote_files);
	}
	for (auto &file_deletions : parameters.file_deletions) {
		auto table_id = GetTable(GetTableId(file_deletions.table_id)).GetTableId();
		transaction.MarkInlinedFileDeletionsFlushed(table_id, file_deletions.flush_snapshot_id);
	}
}

void DuckLakeRESTCommit::CreateNameMap(RESTCreateNameMapRequest &parameters) {
	auto &mapping = parameters.mapping;
	auto client_id = mapping.mapping_id.index;
	mapping.table_id = GetTable(GetTableId(parameters.table_id)).GetTableId();
	name_map_ids[client_id] = transaction.AddNameMap(DuckLakeNameMap::FromColumnMapping(std::move(mapping)));
}

void DuckLakeRESTCommit::RegisterPartition(DuckLakeTableEntry &table, optional_idx client_partition_id) {
	auto partition_data = table.GetPartitionData();
	if (!client_partition_id.IsValid() || !partition_data) {
		return;
	}
	partition_ids[client_partition_id.GetIndex()] = partition_data->partition_id;
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

void DuckLakeRESTCommit::SetOption(RESTSetOptionRequest &parameters) {
	auto &option_in = parameters.option;
	DuckLakeConfigOption option;
	option.option.key = option_in.key;
	option.option.value = option_in.value;
	if (option_in.schema_id.IsValid()) {
		option.schema_id = SchemaIndex(option_in.schema_id.GetIndex());
	}
	if (option_in.table_id.has_value) {
		option.table_id = GetTable(GetTableId(option_in.table_id.value)).GetTableId();
	}
	if (parameters.reset) {
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

void DuckLakeRESTCommit::Compact(RESTCompactRequest &parameters) {
	auto table_id = GetTable(GetTableId(parameters.table_id)).GetTableId();
	for (auto &compaction : parameters.compactions) {
		for (auto &file : compaction.written_files) {
			RemapNameMap(file);
		}
		transaction.AddCompaction(table_id, std::move(compaction));
	}
}

void DuckLakeRESTCommit::InsertRows(RESTInsertRowsRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	auto new_data = make_uniq<DuckLakeInlinedData>();
	if (parameters.rows.owned_rows) {
		new_data->data = std::move(parameters.rows.owned_rows);
	} else {
		new_data->data = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), parameters.column_types);
	}
	new_data->row_ids = std::move(parameters.row_ids);
	new_data->column_stats = std::move(parameters.column_stats);
	transaction.AppendInlinedData(table.GetTableId(), std::move(new_data));
}

void DuckLakeRESTCommit::InsertFiles(RESTInsertFilesRequest &parameters) {
	auto &table = GetTable(GetTableId(parameters.table_id));
	for (auto &file : parameters.files) {
		RemapNameMap(file);
		RemapPartition(table, file);
	}
	transaction.AppendFiles(table.GetTableId(), std::move(parameters.files));
}

RESTCommitTransactionResponse DuckLakeRESTCommit::Run(const RESTCommitTransactionRequest &parameters) {
	if (!RESTIsNull(parameters.snapshot) || parameters.snapshot_id.IsValid()) {
		// conflicts are detected against the snapshot the client transaction started from
		transaction.SetSnapshot(request.GetSnapshot(parameters));
	}
	if (parameters.retry) {
		auto &retry = *parameters.retry;
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_max_retry_count", Value::UBIGINT(retry.max_retry_count));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_wait_ms", Value::UBIGINT(retry.retry_wait_ms));
		DuckLakeUtil::SetExtensionSetting(context, "ducklake_retry_backoff", Value::DOUBLE(retry.retry_backoff));
	}
	if (parameters.commit_info) {
		transaction.SetCommitMessage(*parameters.commit_info);
	}
	for (auto &commit_request : parameters.requests) {
		Apply(*commit_request);
	}
	auto transaction_ref = transaction.shared_from_this();
	request.connection.Commit();
	RESTCommitTransactionResponse result;
	auto committed = transaction_ref->GetCommittedSnapshot();
	if (committed.snapshot_id != DConstants::INVALID_INDEX) {
		result.snapshot_id = committed.snapshot_id;
		result.schema_version = committed.schema_version;
	}
	auto &committed_tables = transaction_ref->GetCommittedTableIds();
	for (auto &entry : created_tables) {
		auto committed_table = committed_tables.find(entry.second);
		if (committed_table == committed_tables.end()) {
			// the table was dropped again
			continue;
		}
		RESTCreatedTable created_table;
		created_table.placeholder = entry.first;
		created_table.table_id = committed_table->second.index;
		result.table_ids.push_back(created_table);
	}
	for (auto &entry : transaction_ref->GetCommittedEntryOids()) {
		auto reference = entry_references.find(entry.second);
		if (reference == entry_references.end()) {
			continue;
		}
		RESTCreatedEntry created_entry;
		created_entry.entry_reference = reference->second;
		created_entry.type = EnumUtil::ToString(entry.first.first);
		created_entry.id = entry.first.second;
		result.entry_ids.push_back(std::move(created_entry));
	}
	for (auto &option : transaction_ref->GetCommittedTableOptions()) {
		RESTOptionSetting setting;
		setting.key = option.option.key;
		setting.value = option.option.value;
		setting.table_id = option.table_id.index;
		result.table_options.push_back(std::move(setting));
	}
	return result;
}

RESTCommitTransactionResponse DuckLakeRESTEndpoints::CommitTransaction(DuckLakeRESTRequest &request,
                                                                       const RESTCommitTransactionRequest &parameters) {
	DuckLakeRESTCommit commit(request);
	return commit.Run(parameters);
}

} // namespace duckdb
