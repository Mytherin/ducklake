#include "metadata_manager/rest_metadata_manager.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "common/ducklake_types.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_filters.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_transaction_state.hpp"
#include "storage/ducklake_view_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

RESTMetadataManager::RESTMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

RESTMetadataManager::~RESTMetadataManager() {
}

//! The core refers to the inlined data of a table at a schema version by the name that the inlined data table has in a
//! metadata database (users see it as the filename of the rows). The REST API only knows the table and the version.
static const char *INLINED_TABLE_PREFIX = "ducklake_inlined_data_";

static string InlinedTableName(TableIndex table_id, idx_t schema_version) {
	return DuckLakeMetadataManager::InlinedTableNameFor(table_id.index, schema_version);
}

struct RESTInlinedTableHandle {
	TableIndex table_id;
	idx_t schema_version;
};

static RESTInlinedTableHandle ParseInlinedTableName(const string &name) {
	if (!StringUtil::StartsWith(name, INLINED_TABLE_PREFIX)) {
		throw InternalException("\"%s\" is not the name of the inlined data of a REST catalog", name);
	}
	auto parts = StringUtil::Split(name.substr(strlen(INLINED_TABLE_PREFIX)), '_');
	if (parts.size() != 2) {
		throw InternalException("\"%s\" is not the name of the inlined data of a REST catalog", name);
	}
	return {TableIndex(std::stoull(parts[0])), std::stoull(parts[1])};
}

//! Entries created by the transaction are referred to by negative placeholder ids
static int64_t ToRequestId(idx_t id) {
	if (DuckLakeTransaction::IsTransactionLocal(id)) {
		return -NumericCast<int64_t>(id - DuckLakeConstants::TRANSACTION_LOCAL_ID_START + 1);
	}
	return NumericCast<int64_t>(id);
}

DuckLakeRESTClient &RESTMetadataManager::GetClient() {
	return transaction.GetCatalog().GetRESTClient();
}

bool RESTMetadataManager::HasPendingChanges() const {
	// flushes of empty inlined tables only drop the tables
	return !catalog_requests.empty() || !transaction.GetFlushedInlinedTables().empty() ||
	       !transaction.GetFlushedInlinedFileDeletions().empty();
}

//===--------------------------------------------------------------------===//
// Initialization
//===--------------------------------------------------------------------===//
unique_ptr<QueryResult> RESTMetadataManager::AttachMetadata(const string &attach_query) {
	throw InternalException("REST catalogs have no metadata database to attach");
}

bool RESTMetadataManager::MetadataExists() {
	return true;
}

void RESTMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	throw InvalidInputException("DuckLake REST catalogs are initialized by the server");
}

DuckLakeMetadata RESTMetadataManager::LoadDuckLake() {
	return GetClient().Config(RESTConfigRequest());
}

//===--------------------------------------------------------------------===//
// SQL
//===--------------------------------------------------------------------===//
static unique_ptr<QueryResult> ThrowSQLNotSupported(const string &query) {
	throw NotImplementedException("DuckLake REST catalogs do not support metadata queries in SQL: %s", query);
}

unique_ptr<QueryResult> RESTMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Execute(string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::ExecuteInTransaction(string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ThrowSQLNotSupported(query);
}

unique_ptr<QueryResult> RESTMetadataManager::Query(string &query) {
	return ThrowSQLNotSupported(query);
}

//===--------------------------------------------------------------------===//
// Reads
//===--------------------------------------------------------------------===//
RESTSnapshotDataResponse RESTMetadataManager::GetSnapshotData(optional_ptr<DuckLakeSnapshot> snapshot,
                                                              bool include_catalog, bool include_stats) {
	RESTSnapshotDataRequest request;
	if (snapshot) {
		request.snapshot = *snapshot;
	}
	request.include_catalog = include_catalog;
	request.include_stats = include_stats;
	return GetClient().SnapshotData(request);
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot() {
	// the catalog and statistics of a new snapshot come along, which saves a round-trip to load them
	auto &client = GetClient();
	RESTSnapshotDataRequest request;
	idx_t loaded_metadata_version = client.loaded_metadata_version;
	idx_t loaded_data_version = client.loaded_data_version;
	if (loaded_metadata_version == DConstants::INVALID_INDEX) {
		request.include_catalog = true;
	} else {
		request.cached_metadata_version = loaded_metadata_version;
	}
	if (loaded_data_version == DConstants::INVALID_INDEX) {
		request.include_stats = true;
	} else {
		request.cached_data_version = loaded_data_version;
	}
	auto response = client.SnapshotData(request);
	auto &snapshot = response.snapshot;
	lock_guard<mutex> guard(cache_lock);
	if (response.catalog) {
		prefetched_catalog = std::move(response.catalog);
		prefetched_metadata_version = snapshot.schema_version;
	}
	if (response.stats.has_value) {
		auto &snapshot_stats = table_stats[snapshot.snapshot_id];
		for (auto &table : response.stats.value) {
			snapshot_stats.push_back(std::move(table));
		}
		client.loaded_data_version = snapshot.next_file_id;
	}
	return make_uniq<DuckLakeSnapshot>(snapshot);
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound,
                                                              idx_t newest_snapshot_id) {
	auto value = at_clause.GetValue();
	if (at_clause.Unit() == "timestamp") {
		auto context = transaction.context.lock();
		value = value.CastAs(*context, LogicalType::TIMESTAMP_TZ);
	}
	RESTSnapshotDataRequest request;
	request.at = make_uniq<RESTAtClause>();
	request.at->unit = RESTAtUnitCodec::FromString(StringUtil::Lower(at_clause.Unit().GetIdentifierName()));
	request.at->value = value;
	request.bound = bound == SnapshotBound::LOWER_BOUND ? "lower" : "upper";
	request.newest_snapshot_id = newest_snapshot_id;
	auto response = GetClient().SnapshotData(request);
	return make_uniq<DuckLakeSnapshot>(response.snapshot);
}

DuckLakeCatalogInfo RESTMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &client = GetClient();
	{
		lock_guard<mutex> guard(cache_lock);
		if (prefetched_catalog && prefetched_metadata_version == snapshot.schema_version) {
			// the catalog only changes with the schema version
			auto result = std::move(*prefetched_catalog);
			prefetched_catalog.reset();
			client.loaded_metadata_version = snapshot.schema_version;
			return result;
		}
	}
	auto response = GetSnapshotData(snapshot, true, false);
	client.loaded_metadata_version = snapshot.schema_version;
	if (!response.catalog) {
		throw InvalidInputException("DuckLake REST: the server did not return the catalog of snapshot %llu",
		                            snapshot.snapshot_id);
	}
	return std::move(*response.catalog);
}

vector<DuckLakeGlobalStatsInfo> RESTMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot) {
	// table statistics are not versioned, a transaction keeps the statistics it read first
	{
		lock_guard<mutex> guard(cache_lock);
		auto entry = table_stats.find(snapshot.snapshot_id);
		if (entry != table_stats.end()) {
			return entry->second;
		}
	}
	auto response = GetSnapshotData(snapshot, false, true);
	auto &result = response.stats.value;
	lock_guard<mutex> guard(cache_lock);
	table_stats.emplace(snapshot.snapshot_id, result);
	GetClient().loaded_data_version = snapshot.next_file_id;
	return std::move(result);
}

map<TableIndex, idx_t> RESTMetadataManager::GetTableRecordCounts(DuckLakeSnapshot snapshot) {
	map<TableIndex, idx_t> result;
	for (auto &stats : GetGlobalTableStats(snapshot)) {
		if (!stats.record_count_unknown) {
			result.emplace(stats.table_id, stats.record_count);
		}
	}
	return result;
}

//! Sets the columns of a request that returns inlined rows, all columns if they are not set
template <class REQUEST>
static void SetProjection(REQUEST &request, const RESTInlinedProjection &projection) {
	if (projection.all) {
		return;
	}
	vector<idx_t> columns(projection.columns.begin(), projection.columns.end());
	std::sort(columns.begin(), columns.end());
	request.columns = RESTOptional<vector<idx_t>>(std::move(columns));
}

RESTMetadataManager::InlinedRowsPlan
RESTMetadataManager::PlanInlinedRows(const RESTInlinedRowsKey &key, optional_ptr<const vector<FieldIndex>> projection) {
	auto wanted = RESTInlinedProjection::From(projection);
	if (inlined_rows_cache.Covers(key, wanted)) {
		return {false, wanted};
	}
	if (inlined_rows_cache.HasResponse(key)) {
		// other scans of the table need other columns: get all of them, so that no scan has to ask again
		wanted = RESTInlinedProjection();
	}
	return {true, wanted};
}

vector<DuckLakeFileListEntry> RESTMetadataManager::GetFilesForTable(DuckLakeTableEntry &table,
                                                                    DuckLakeSnapshot snapshot,
                                                                    const FilterPushdownInfo *filter_info,
                                                                    bool include_local_changes,
                                                                    optional_ptr<const vector<FieldIndex>> projection) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	RESTSelectRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	// the inlined rows of an earlier scan at this snapshot can have the columns already
	RESTInlinedRowsKey inlined_key {RESTInlinedRowsKind::SCAN, 0, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	request.include_inlined_data = plan.fetch;
	SetProjection(request, plan.projection);
	if (filter_info) {
		request.filters = make_uniq<RESTScanFilters>(DuckLakeRESTFilters::Write(*filter_info, table));
	}
	auto response = GetClient().Select(request);
	// this transaction reads the inlined deletions it flushed from its delete files
	if (include_local_changes && transaction.InlinedFileDeletionsFlushed(table.GetTableId())) {
		for (auto &file : response.files) {
			file.inlined_file_deletions.clear();
		}
	}
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, response.inlined_data);
	}
	return std::move(response.files);
}

vector<DuckLakeFileListExtendedEntry>
RESTMetadataManager::GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                              const FilterPushdownInfo *filter_info,
                                              optional_ptr<const vector<FieldIndex>> projection) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	RESTSelectRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	request.extended = true;
	RESTInlinedRowsKey inlined_key {RESTInlinedRowsKind::SCAN, 0, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	request.include_inlined_data = plan.fetch;
	SetProjection(request, plan.projection);
	if (filter_info) {
		request.filters = make_uniq<RESTScanFilters>(DuckLakeRESTFilters::Write(*filter_info, table));
	}
	auto response = GetClient().Select(request);
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, response.inlined_data);
	}
	return std::move(response.extended_files);
}

vector<DuckLakeCompactionFileEntry>
RESTMetadataManager::GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type, double deletion_threshold,
                                           DuckLakeSnapshot snapshot, DuckLakeFileSizeOptions options) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	RESTFilesForCompactionRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	request.type = type;
	request.delete_threshold = deletion_threshold;
	request.min_file_size = options.min_file_size;
	request.max_file_size = options.max_file_size;
	request.newer_than = options.newer_than;
	request.older_than = options.older_than;
	request.target_file_size = options.target_file_size;
	return std::move(GetClient().FilesForCompaction(request).files);
}

idx_t RESTMetadataManager::GetNextColumnId(TableIndex table_id) {
	RESTNextColumnIdRequest request;
	request.table_id = table_id.index;
	return GetClient().NextColumnId(request).next_column_id;
}

unique_ptr<QueryResult> RESTMetadataManager::ToQueryResult(RESTInlinedRows rows) {
	auto context = transaction.context.lock();
	vector<Identifier> names;
	for (auto &column : rows.columns) {
		names.emplace_back(column.name);
	}
	return make_uniq<QueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), std::move(names),
	                              std::move(rows.rows.owned_rows), context->GetClientProperties());
}

shared_ptr<RESTInlinedRowsResponse> RESTMetadataManager::FetchInlinedRows(const RESTInlinedRowsKey &key,
                                                                          DuckLakeSnapshot start_snapshot,
                                                                          DuckLakeSnapshot snapshot) {
	vector<RESTInlinedTableRows> inlined_data;
	if (key.kind == RESTInlinedRowsKind::SCAN) {
		RESTSelectRequest request;
		request.snapshot = snapshot;
		request.table_id = key.table_id;
		request.include_inlined_data = true;
		inlined_data = std::move(GetClient().Select(request).inlined_data);
	} else {
		RESTTableChangesRequest request;
		request.table_id = key.table_id;
		request.start_snapshot = start_snapshot;
		request.snapshot = snapshot;
		request.changes = key.kind == RESTInlinedRowsKind::INSERTIONS ? "insertions" : "deletions";
		request.include_inlined_data = true;
		inlined_data = std::move(GetClient().TableChanges(request).inlined_data);
	}
	return inlined_rows_cache.Add(key, RESTInlinedProjection(), inlined_data);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadCachedInlinedRows(RESTInlinedRowsKind kind,
                                                                   DuckLakeSnapshot start_snapshot,
                                                                   DuckLakeSnapshot snapshot,
                                                                   const string &inlined_table_name,
                                                                   const vector<string> &columns_to_read) {
	auto handle = ParseInlinedTableName(inlined_table_name);
	idx_t start_snapshot_id = kind == RESTInlinedRowsKind::SCAN ? 0 : start_snapshot.snapshot_id;
	RESTInlinedRowsKey key {kind, start_snapshot_id, snapshot.snapshot_id, handle.table_id.index};
	auto rows_ptr = inlined_rows_cache.Find(key, handle.schema_version, columns_to_read);
	if (!rows_ptr) {
		// the rows usually came along with the file list of the scan. Otherwise (or if they came with other
		// columns) the server returns all columns, which are read from the response itself: the cache can have
		// changed in the meantime
		auto response = FetchInlinedRows(key, start_snapshot, snapshot);
		auto entry = response->rows.find(handle.schema_version);
		if (entry == response->rows.end()) {
			throw InvalidInputException("DuckLake REST: the server did not return the inlined rows of \"%s\"",
			                            inlined_table_name);
		}
		rows_ptr = entry->second;
	}
	// scans of the table can read the rows at the same time
	lock_guard<mutex> read_guard(rows_ptr->read_lock);
	auto &rows = rows_ptr->rows;
	// the columns to read are column names, optionally cast to the type to read them as
	auto context = transaction.context.lock();
	vector<idx_t> column_indexes;
	vector<LogicalType> types;
	for (auto &column : columns_to_read) {
		auto expressions = Parser::GetBuiltinParser().ParseExpressionList(column);
		if (expressions.size() != 1) {
			throw InvalidInputException("DuckLake REST: \"%s\" is not a column of the inlined rows", column);
		}
		optional_ptr<const ParsedExpression> column_ref = expressions[0].get();
		optional<LogicalType> cast_type;
		if (column_ref->GetExpressionClass() == ExpressionClass::CAST) {
			auto &cast = column_ref->Cast<CastExpression>();
			cast_type = TransformStringToLogicalType(cast.TargetType().ToString(), *context);
			column_ref = cast.Child();
		}
		if (column_ref->GetExpressionClass() != ExpressionClass::COLUMN_REF ||
		    column_ref->Cast<ColumnRefExpression>().IsQualified()) {
			throw InvalidInputException("DuckLake REST: \"%s\" is not a column of the inlined rows", column);
		}
		auto &name = column_ref->Cast<ColumnRefExpression>().GetColumnName();
		optional_idx column_index;
		for (idx_t i = 0; i < rows.columns.size(); i++) {
			if (Identifier(rows.columns[i].name) == name) {
				column_index = i;
			}
		}
		if (!column_index.IsValid()) {
			throw InvalidInputException("DuckLake REST: the inlined rows have no column \"%s\"", column);
		}
		column_indexes.push_back(column_index.GetIndex());
		types.push_back(cast_type ? *cast_type : rows.columns[column_index.GetIndex()].type);
	}
	RESTInlinedRows result;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		RESTResultColumn column;
		column.name = "col" + to_string(i + 1);
		column.type = types[i];
		result.columns.push_back(std::move(column));
	}
	auto projected_rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	ColumnDataAppendState append_state;
	projected_rows->InitializeAppend(append_state);
	DataChunk projected;
	projected.Initialize(Allocator::DefaultAllocator(), types);
	for (auto &chunk : rows.rows.rows->Chunks()) {
		projected.Reset();
		for (idx_t i = 0; i < column_indexes.size(); i++) {
			auto &source = chunk.data[column_indexes[i]];
			if (source.GetType() == types[i]) {
				projected.data[i].Reference(source);
			} else {
				VectorOperations::DefaultCast(source, projected.data[i], chunk.size());
			}
		}
		projected.SetCardinality(chunk.size());
		projected_rows->Append(append_state, projected);
	}
	result.rows = RESTRows(std::move(projected_rows));
	return ToQueryResult(std::move(result));
}

set<idx_t> RESTMetadataManager::GetTransactionDeletes(const DuckLakeTableEntry &table,
                                                      const string &inlined_table_name) {
	auto deletes = transaction.GetInlinedDeletes(table.GetTableId(), inlined_table_name);
	if (!deletes) {
		return set<idx_t>();
	}
	return deletes->rows;
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                             const string &inlined_table_name,
                                                             const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::SCAN, snapshot, snapshot, inlined_table_name, columns_to_read);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
                                                                       DuckLakeSnapshot end_snapshot,
                                                                       const string &inlined_table_name,
                                                                       const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::INSERTIONS, start_snapshot, end_snapshot, inlined_table_name,
	                             columns_to_read);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
                                                                      DuckLakeSnapshot end_snapshot,
                                                                      const string &inlined_table_name,
                                                                      const vector<string> &columns_to_read) {
	return ReadCachedInlinedRows(RESTInlinedRowsKind::DELETIONS, start_snapshot, end_snapshot, inlined_table_name,
	                             columns_to_read);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot,
                                                                        const string &inlined_table_name,
                                                                        const DuckLakeTableEntry &table,
                                                                        const string &sort_order_sql,
                                                                        const vector<string> &columns_to_read) {
	RESTInlinedDataForFlushRequest request;
	request.snapshot = snapshot;
	auto handle = ParseInlinedTableName(inlined_table_name);
	request.table_id = handle.table_id.index;
	request.schema_version = handle.schema_version;
	request.columns = columns_to_read;
	request.order_by = sort_order_sql;
	request.deleted_row_ids = GetTransactionDeletes(table, inlined_table_name);
	return ToQueryResult(GetClient().InlinedDataForFlush(request));
}

DuckLakeDeletedRowVersions RESTMetadataManager::GetDeletedInlinedRowVersions(DuckLakeTableEntry &table,
                                                                             const string &inlined_table_name,
                                                                             DuckLakeSnapshot snapshot) {
	RESTDeletedInlinedRowsRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	request.schema_version = ParseInlinedTableName(inlined_table_name).schema_version;
	request.deleted_row_ids = GetTransactionDeletes(table, inlined_table_name);
	DuckLakeDeletedRowVersions result;
	for (auto &row : GetClient().DeletedInlinedRows(request).rows) {
		result.emplace(make_pair(row.row_id, row.begin_snapshot), row.end_snapshot);
	}
	return result;
}

vector<DuckLakeInlinedFileDeletionEntry>
RESTMetadataManager::GetInlinedFileDeletionsForFlush(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	RESTInlinedFileDeletionsRequest request;
	request.snapshot = snapshot;
	request.table_id = table.GetTableId().index;
	return std::move(GetClient().InlinedFileDeletions(request).deletions);
}

idx_t RESTMetadataManager::GetBeginSnapshotForTable(TableIndex table_id) {
	RESTTableSchemaVersionsRequest request;
	request.table_id = table_id.index;
	return GetClient().TableSchemaVersions(request).begin_snapshot;
}

idx_t RESTMetadataManager::GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version) {
	auto &catalog = transaction.GetCatalog();
	auto cached_snapshot = catalog.TryGetSchemaVersionBeginSnapshot(table_id, schema_version);
	if (cached_snapshot.IsValid()) {
		return cached_snapshot.GetIndex();
	}
	RESTTableSchemaVersionsRequest request;
	request.table_id = table_id.index;
	request.schema_version = schema_version;
	auto response = GetClient().TableSchemaVersions(request);
	if (!response.schema_version_begin_snapshot.IsValid()) {
		throw InvalidInputException("DuckLake REST: the server did not return the snapshot of schema version %llu",
		                            schema_version);
	}
	auto begin_snapshot = response.schema_version_begin_snapshot.GetIndex();
	catalog.CacheSchemaVersionBeginSnapshot(table_id, schema_version, begin_snapshot);
	return begin_snapshot;
}

DuckLakeTableSchemaVersions RESTMetadataManager::GetTableSchemaVersions(TableIndex table_id,
                                                                        DuckLakeSnapshot snapshot) {
	RESTTableSchemaVersionsRequest request;
	request.table_id = table_id.index;
	request.snapshot = snapshot;
	auto response = GetClient().TableSchemaVersions(request);
	DuckLakeTableSchemaVersions result;
	result.creation = response.creation;
	result.last_change = response.last_change;
	return result;
}

optional_idx RESTMetadataManager::GetRowCount(DuckLakeSnapshot snapshot, const string &count, TableIndex table_id,
                                              optional_idx schema_version) {
	if (IsTransactionLocal(table_id)) {
		return 0;
	}
	RESTRowCountRequest request;
	request.snapshot = snapshot;
	request.count = count;
	request.table_id = table_id.index;
	request.schema_version = schema_version;
	return GetClient().RowCount(request).row_count;
}

idx_t RESTMetadataManager::GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot) {
	return GetRowCount(snapshot, "data_files", table_id).GetIndex();
}

optional_idx RESTMetadataManager::GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot) {
	return GetRowCount(snapshot, "data_files_exact", table_id);
}

idx_t RESTMetadataManager::GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot) {
	auto handle = ParseInlinedTableName(inlined_table_name);
	return GetRowCount(snapshot, "inlined", handle.table_id, handle.schema_version).GetIndex();
}

unordered_set<string> RESTMetadataManager::GetInlinedTableNames(TableIndex table_id) {
	if (IsTransactionLocal(table_id)) {
		return {};
	}
	RESTInlinedTablesRequest request;
	request.table_id = table_id.index;
	unordered_set<string> result;
	for (auto schema_version : GetClient().InlinedTables(request).schema_versions) {
		result.insert(InlinedTableName(table_id, schema_version));
	}
	return result;
}

vector<DuckLakeInlinedTableInfo> RESTMetadataManager::GetInlinedDataTables(const DuckLakeTableEntry &table,
                                                                           DuckLakeSnapshot snapshot) {
	auto table_id = table.GetTableId();
	if (IsTransactionLocal(table_id)) {
		return {};
	}
	vector<idx_t> schema_versions;
	// the scan of the table came with the schema versions it has inlined rows for
	bool cached = inlined_rows_cache.GetSchemaVersions(snapshot.snapshot_id, table_id.index, schema_versions);
	if (!cached) {
		RESTInlinedTablesRequest request;
		request.snapshot = snapshot;
		request.table_id = table_id.index;
		schema_versions = GetClient().InlinedTables(request).schema_versions;
	}
	vector<DuckLakeInlinedTableInfo> result;
	for (auto schema_version : schema_versions) {
		DuckLakeInlinedTableInfo inlined_table;
		inlined_table.table_name = InlinedTableName(table_id, schema_version);
		inlined_table.schema_version = schema_version;
		result.push_back(std::move(inlined_table));
	}
	return result;
}

vector<DuckLakeSnapshotInfo> RESTMetadataManager::GetAllSnapshots(const DuckLakeSnapshotFilter &filter) {
	RESTListSnapshotsRequest request;
	request.exclude_latest = filter.exclude_latest;
	request.older_than = filter.older_than;
	if (filter.has_snapshot_ids) {
		request.snapshot_ids = filter.snapshot_ids;
	}
	return std::move(GetClient().ListSnapshots(request).snapshots);
}

void RESTMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	RESTExpireSnapshotsRequest request;
	for (auto &snapshot : snapshots) {
		request.snapshot_ids.push_back(snapshot.id);
	}
	GetClient().ExpireSnapshots(request);
}

vector<DuckLakeFileForCleanup> RESTMetadataManager::GetOldFilesForCleanup(const string &older_than) {
	RESTFilesForCleanupRequest request;
	request.older_than = older_than;
	return std::move(GetClient().FilesForCleanup(request).files);
}

vector<string> RESTMetadataManager::GetMetadataTableNames() {
	return std::move(GetClient().DuckLakeTables(RESTDuckLakeTablesRequest()).table_names);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadMetadataTable(const string &table_name) {
	RESTDuckLakeTableRequest request;
	request.table_name = table_name;
	return ToQueryResult(GetClient().DuckLakeTable(request));
}

vector<string> RESTMetadataManager::GetKnownFilePaths() {
	return std::move(GetClient().KnownFiles(RESTKnownFilesRequest()).paths);
}

static RESTOptionSetting GetOptionSetting(const DuckLakeConfigOption &option) {
	RESTOptionSetting result;
	result.key = option.option.key;
	result.value = option.option.value;
	if (option.schema_id.IsValid()) {
		result.schema_id = option.schema_id.index;
	}
	if (option.table_id.IsValid()) {
		result.table_id = ToRequestId(option.table_id.index);
	}
	return result;
}

void RESTMetadataManager::SetConfigOption(const DuckLakeConfigOption &option) {
	auto &request = AddCommitRequest<RESTSetOptionRequest>();
	request.option = GetOptionSetting(option);
}

bool RESTMetadataManager::ResetConfigOption(const DuckLakeConfigOption &option) {
	auto &request = AddCommitRequest<RESTSetOptionRequest>();
	request.option = GetOptionSetting(option);
	request.reset = true;
	return true;
}

vector<DuckLakeTableSizeInfo> RESTMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	RESTTableSizesRequest request;
	request.snapshot = snapshot;
	return std::move(GetClient().TableSizes(request).tables);
}

unordered_set<idx_t> RESTMetadataManager::GetTablesWithLiveDataFiles() {
	auto response = GetClient().TablesWithLiveDataFiles(RESTTablesWithLiveDataFilesRequest());
	return unordered_set<idx_t>(response.table_ids.begin(), response.table_ids.end());
}

vector<DuckLakeColumnMappingInfo> RESTMetadataManager::GetColumnMappings(optional_idx start_from) {
	RESTColumnMappingsRequest request;
	request.start_from = start_from;
	return std::move(GetClient().ColumnMappings(request).mappings);
}

RESTTableChangesResponse RESTMetadataManager::GetTableChanges(DuckLakeTableEntry &table,
                                                              DuckLakeSnapshot start_snapshot,
                                                              DuckLakeSnapshot snapshot, const string &changes,
                                                              optional_ptr<const vector<FieldIndex>> projection) {
	RESTTableChangesRequest request;
	request.table_id = table.GetTableId().index;
	request.start_snapshot = start_snapshot;
	request.snapshot = snapshot;
	request.changes = changes;
	auto kind = changes == "insertions" ? RESTInlinedRowsKind::INSERTIONS : RESTInlinedRowsKind::DELETIONS;
	RESTInlinedRowsKey inlined_key {kind, start_snapshot.snapshot_id, snapshot.snapshot_id, table.GetTableId().index};
	auto plan = PlanInlinedRows(inlined_key, projection);
	request.include_inlined_data = plan.fetch;
	SetProjection(request, plan.projection);
	auto response = GetClient().TableChanges(request);
	if (plan.fetch) {
		inlined_rows_cache.Add(inlined_key, plan.projection, response.inlined_data);
	}
	return response;
}

vector<DuckLakeFileListEntry>
RESTMetadataManager::GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
                                        DuckLakeSnapshot snapshot, optional_ptr<const vector<FieldIndex>> projection) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	return std::move(GetTableChanges(table, start_snapshot, snapshot, "insertions", projection).insertions);
}

vector<DuckLakeDeleteScanEntry>
RESTMetadataManager::GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
                                       DuckLakeSnapshot snapshot, bool include_local_changes,
                                       optional_ptr<const vector<FieldIndex>> projection) {
	if (IsTransactionLocal(table.GetTableId())) {
		return {};
	}
	return std::move(GetTableChanges(table, start_snapshot, snapshot, "deletions", projection).deletions);
}

void RESTMetadataManager::RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) {
	RESTRemoveCleanedUpFilesRequest request;
	for (auto &file : cleaned_up_files) {
		request.data_file_ids.push_back(file.id.index);
	}
	GetClient().RemoveCleanedUpFiles(request);
}

//===--------------------------------------------------------------------===//
// Catalog Changes
//===--------------------------------------------------------------------===//
static vector<string> GetSchemaPath(DuckLakeSchemaEntry &schema) {
	vector<string> path;
	for (optional_ptr<DuckLakeSchemaEntry> entry = &schema; entry; entry = entry->ParentDuckLakeSchema()) {
		path.push_back(entry->name.GetIdentifierName());
	}
	std::reverse(path.begin(), path.end());
	return path;
}

void RESTMetadataManager::OnCreateSchema(DuckLakeSchemaEntry &schema) {
	auto &request = AddCommitRequest<RESTCreateSchemaRequest>();
	request.entry_reference = schema.oid;
	request.schema_path = GetSchemaPath(schema);
	request.schema_uuid = schema.GetSchemaUUID();
	request.path = schema.DataPath();
}

void RESTMetadataManager::OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info,
                                        const DuckLakeTableEntry &table) {
	auto &request = AddCommitRequest<RESTCreateTableRequest>();
	request.table_id = ToRequestId(table.GetTableId().index);
	request.entry_reference = table.oid;
	request.schema_path = GetSchemaPath(schema);
	request.table_name = info.GetTableName().GetIdentifierName();
	request.table_uuid = table.GetTableUUID();
	request.path = table.DataPath();
	auto version = transaction.GetCatalog().GetDuckLakeVersion();
	auto not_null_fields = table.GetNotNullFields();
	for (auto &column : table.GetColumns().Logical()) {
		// fail with the error of a commit to a metadata database, rather than on a type the server cannot parse
		DuckLakeTypes::ToString(column.GetType());
		DuckLakeTypes::CheckSupportedType(column.GetType(), version);
		request.columns.push_back(DuckLakeRESTCatalogChanges::WriteColumn(
		    column, not_null_fields.count(column.Name().GetIdentifierName()) > 0));
	}
	for (auto &key : info.partition_keys) {
		request.partitioned_by.push_back(key->ToString());
	}
	for (auto &key : info.sort_keys) {
		request.sorted_by.push_back(key->ToString());
	}
	for (auto &option : info.options) {
		request.options[option.first] = option.second->ToString();
	}
	if (!table.comment.IsNull()) {
		request.comment = table.comment.ToString();
	}
}

void RESTMetadataManager::OnCreateView(DuckLakeViewEntry &view) {
	auto &request = AddCommitRequest<RESTCreateViewRequest>();
	request.entry_reference = view.oid;
	request.schema_path = GetSchemaPath(view.ParentSchema().Cast<DuckLakeSchemaEntry>());
	request.view_name = view.name.GetIdentifierName();
	request.view_uuid = view.GetViewUUID();
	request.dialect = "duckdb";
	request.sql = view.GetQuerySQL();
	for (auto &alias : view.aliases) {
		request.column_aliases.push_back(alias.GetIdentifierName());
	}
	if (!view.comment.IsNull()) {
		request.comment = view.comment.ToString();
	}
	auto view_info = view.GetInfo();
	for (auto &entry : view_info->Cast<CreateViewInfo>().column_comments_map) {
		if (!entry.second.IsNull()) {
			request.column_comments[entry.first.GetIdentifierName()] = entry.second.ToString();
		}
	}
}

void RESTMetadataManager::OnCreateMacro(MacroCatalogEntry &macro) {
	auto &request = AddCommitRequest<RESTCreateMacroRequest>();
	request.entry_reference = macro.oid;
	request.schema_path = GetSchemaPath(macro.schema.Cast<DuckLakeSchemaEntry>());
	request.macro = DuckLakeTransactionState::GetMacroInfo(macro);
}

static bool AltersView(const AlterInfo &info) {
	switch (info.type) {
	case AlterType::ALTER_VIEW:
		return true;
	case AlterType::SET_COMMENT:
		return info.Cast<SetCommentInfo>().entry_catalog_type == CatalogType::VIEW_ENTRY;
	case AlterType::SET_COLUMN_COMMENT:
		return info.Cast<SetColumnCommentInfo>().catalog_entry_type == CatalogType::VIEW_ENTRY;
	default:
		return false;
	}
}

//! The type an ALTER TABLE gives a column, if any
static LogicalType GetAlteredType(const AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		return LogicalType::INVALID;
	}
	auto &alter = info.Cast<AlterTableInfo>();
	switch (alter.alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		return alter.Cast<AddColumnInfo>().new_column.GetType();
	case AlterTableType::ADD_FIELD:
		return alter.Cast<AddFieldInfo>().new_field.GetType();
	case AlterTableType::ALTER_COLUMN_TYPE:
		return alter.Cast<ChangeColumnTypeInfo>().target_type;
	default:
		return LogicalType::INVALID;
	}
}

void RESTMetadataManager::OnAlterEntry(DuckLakeSchemaEntry &schema, const AlterInfo &info) {
	auto altered_type = GetAlteredType(info);
	if (altered_type.id() != LogicalTypeId::INVALID) {
		DuckLakeTypes::ToString(altered_type);
		DuckLakeTypes::CheckSupportedType(altered_type, transaction.GetCatalog().GetDuckLakeVersion());
	}
	auto name = info.GetQualifiedName().Name().GetIdentifierName();
	if (AltersView(info)) {
		auto &request = AddCommitRequest<RESTAlterViewRequest>();
		request.schema_path = GetSchemaPath(schema);
		request.name = std::move(name);
		request.alter = DuckLakeRESTCatalogChanges::WriteAlter(info);
		return;
	}
	auto &request = AddCommitRequest<RESTAlterTableRequest>();
	request.schema_path = GetSchemaPath(schema);
	request.name = std::move(name);
	request.alter = DuckLakeRESTCatalogChanges::WriteAlter(info);
}

void RESTMetadataManager::OnDropEntry(CatalogEntry &entry) {
	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto &request = AddCommitRequest<RESTDropSchemaRequest>();
		request.schema_path = GetSchemaPath(entry.Cast<DuckLakeSchemaEntry>());
		return;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &request = AddCommitRequest<RESTDropTableRequest>();
		request.schema_path = GetSchemaPath(entry.ParentSchema().Cast<DuckLakeSchemaEntry>());
		request.name = entry.name.GetIdentifierName();
		return;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &request = AddCommitRequest<RESTDropViewRequest>();
		request.schema_path = GetSchemaPath(entry.ParentSchema().Cast<DuckLakeSchemaEntry>());
		request.name = entry.name.GetIdentifierName();
		return;
	}
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &request = AddCommitRequest<RESTDropMacroRequest>();
		request.schema_path = GetSchemaPath(entry.ParentSchema().Cast<DuckLakeSchemaEntry>());
		request.name = entry.name.GetIdentifierName();
		request.macro_type = entry.type == CatalogType::MACRO_ENTRY ? "scalar" : "table";
		return;
	}
	default:
		throw InternalException("Unsupported catalog entry type for DROP");
	}
}

void RESTMetadataManager::OnEntryChanged(CatalogEntry &entry) {
	if (catalog_requests.empty() || entry.type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &table = entry.Cast<DuckLakeTableEntry>();
	auto partition_data = table.GetPartitionData();
	if (!partition_data || !DuckLakeTransaction::IsTransactionLocal(partition_data->partition_id)) {
		return;
	}
	// files written for the new partitioning refer to it by this id
	auto &last_request = *catalog_requests.back();
	if (last_request.type == RESTCreateTableRequest::TYPE) {
		last_request.Cast<RESTCreateTableRequest>().partition_id = partition_data->partition_id;
	} else if (last_request.type == RESTAlterTableRequest::TYPE) {
		last_request.Cast<RESTAlterTableRequest>().partition_id = partition_data->partition_id;
	}
}

void RESTMetadataManager::OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path,
                                     idx_t row_count, idx_t file_size_bytes) {
	DuckLakeFileListEntry file;
	file.file_id = data_file_id;
	file.file.path = path;
	file.file.file_size_bytes = file_size_bytes;
	file.max_row_count = row_count;
	dropped_files[table_id].push_back(std::move(file));
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//
template <class T>
static T &AddRequest(vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto request = make_uniq<T>();
	auto &result = *request;
	requests.push_back(std::move(request));
	return result;
}

void RESTMetadataManager::AddDeletes(const TransactionChangeInformation &transaction_changes,
                                     vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	set<TableIndex> tables_with_deletes;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto &changes = entry.second;
		if (changes.new_delete_files.empty() && changes.new_inlined_data_deletes.empty() &&
		    !changes.new_inlined_file_deletes) {
			continue;
		}
		tables_with_deletes.insert(entry.first);
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(entry.first.index);
		for (auto &file_entry : changes.new_delete_files) {
			for (auto &delete_file : file_entry.second) {
				request.delete_files.push_back(delete_file);
			}
		}
		for (auto &inlined_entry : changes.new_inlined_data_deletes) {
			RESTInlinedRowDeletes deletes;
			deletes.schema_version = ParseInlinedTableName(inlined_entry.first).schema_version;
			deletes.row_ids = inlined_entry.second->rows;
			request.inlined_deletes.push_back(std::move(deletes));
		}
		if (changes.new_inlined_file_deletes) {
			for (auto &file_entry : changes.new_inlined_file_deletes->file_deletes) {
				RESTFileRowDeletes deletes;
				deletes.data_file_id = file_entry.first;
				deletes.row_ids = file_entry.second;
				request.file_row_deletes.push_back(std::move(deletes));
			}
		}
		request.dropped_files = GetDroppedFiles(entry.first);
	}
	for (auto &entry : dropped_files) {
		if (tables_with_deletes.find(entry.first) != tables_with_deletes.end() ||
		    dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(entry.first.index);
		request.dropped_files = GetDroppedFiles(entry.first);
	}
	for (auto &table_id : transaction.GetTablesDeleteAttempted()) {
		// deletes from tables created by the transaction cannot conflict with other transactions
		if (dropped_tables.find(table_id) != dropped_tables.end() || IsTransactionLocal(table_id)) {
			continue;
		}
		auto &request = AddRequest<RESTDeleteDataRequest>(requests);
		request.table_id = ToRequestId(table_id.index);
		request.delete_attempted = true;
	}
}

void RESTMetadataManager::AddInserts(const TransactionChangeInformation &transaction_changes,
                                     vector<unique_ptr<RESTCommitRequest>> &requests) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto table_id = ToRequestId(entry.first.index);
		auto &changes = entry.second;
		if (!changes.new_data_files.empty()) {
			auto &request = AddRequest<RESTInsertFilesRequest>(requests);
			request.table_id = table_id;
			request.files = changes.new_data_files;
		}
		if (changes.new_inlined_data && changes.new_inlined_data->data) {
			auto &inlined_data = *changes.new_inlined_data;
			auto &request = AddRequest<RESTInsertRowsRequest>(requests);
			request.table_id = table_id;
			request.column_types = inlined_data.data->Types();
			request.rows = RESTRows(*inlined_data.data);
			if (inlined_data.HasPreservedRowIds()) {
				request.row_ids = inlined_data.row_ids;
			}
			request.column_stats = inlined_data.column_stats;
		}
		if (!changes.compactions.empty()) {
			auto &request = AddRequest<RESTCompactRequest>(requests);
			request.table_id = table_id;
			request.compactions = changes.compactions;
		}
	}
}

void RESTMetadataManager::AddNameMaps(vector<unique_ptr<RESTCommitRequest>> &requests) {
	for (auto &entry : transaction.GetNewNameMaps().name_maps) {
		auto &name_map = *entry.second;
		auto &request = AddRequest<RESTCreateNameMapRequest>(requests);
		request.table_id = ToRequestId(name_map.table_id.index);
		request.mapping.table_id = name_map.table_id;
		request.mapping.mapping_id = name_map.id;
		request.mapping.map_type = "map_by_name";
		request.mapping.map_columns = name_map.FlattenColumns();
	}
}

vector<RESTDroppedFile> RESTMetadataManager::GetDroppedFiles(TableIndex table_id) {
	vector<RESTDroppedFile> result;
	auto entry = dropped_files.find(table_id);
	if (entry == dropped_files.end()) {
		return result;
	}
	for (auto &file : entry->second) {
		RESTDroppedFile dropped_file;
		dropped_file.data_file_id = file.file_id.index;
		dropped_file.path = file.file.path;
		dropped_file.record_count = file.max_row_count.GetIndex();
		dropped_file.file_size_bytes = file.file.file_size_bytes;
		result.push_back(std::move(dropped_file));
	}
	return result;
}

void RESTMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                 DuckLakeSnapshot transaction_snapshot,
                                                 const TransactionChangeInformation &transaction_changes,
                                                 const DuckLakeRetryConfig &retry_config) {
	RESTCommitTransactionRequest request;
	auto &requests = request.requests;
	// deletes go first, as the catalog changes of the transaction can depend on them (e.g. SET NOT NULL)
	AddDeletes(transaction_changes, requests);
	for (auto &catalog_request : catalog_requests) {
		requests.push_back(std::move(catalog_request));
	}
	catalog_requests.clear();
	// inserts go last, they are in the format of the tables at the end of the transaction
	AddNameMaps(requests);
	AddInserts(transaction_changes, requests);
	auto &flushed_tables = flush_transaction.GetFlushedInlinedTables();
	auto &flushed_file_deletions = flush_transaction.GetFlushedInlinedFileDeletions();
	if (!flushed_tables.empty() || !flushed_file_deletions.empty()) {
		auto &flush = AddRequest<RESTFlushInlinedDataRequest>(requests);
		for (auto &flushed_table : flushed_tables) {
			RESTFlushedInlinedTable table;
			table.table_id = ToRequestId(ParseInlinedTableName(flushed_table.inlined_table.table_name).table_id.index);
			table.schema_version = flushed_table.inlined_table.schema_version;
			table.flush_snapshot_id = flushed_table.flush_snapshot_id;
			table.wrote_files = flushed_table.wrote_files;
			flush.inlined_tables.push_back(std::move(table));
		}
		for (auto &entry : flushed_file_deletions) {
			RESTFlushedFileDeletions deletions;
			deletions.table_id = ToRequestId(entry.first.index);
			deletions.flush_snapshot_id = entry.second;
			flush.file_deletions.push_back(deletions);
		}
	}
	if (transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX) {
		request.snapshot = transaction_snapshot;
	}
	request.commit_info = make_uniq<DuckLakeSnapshotCommit>(flush_transaction.GetCommitInfo());
	request.retry = make_uniq<RESTRetryConfig>();
	request.retry->max_retry_count = retry_config.max_retry_count;
	request.retry->retry_wait_ms = retry_config.retry_wait_ms;
	request.retry->retry_backoff = retry_config.retry_backoff;
	auto response = GetClient().CommitTransaction(request);
	if (!response.snapshot_id.IsValid()) {
		// nothing was written that creates a snapshot (e.g. only options were set)
		flush_transaction.ApplyServerSideCommit(transaction_snapshot.schema_version);
		return;
	}
	auto &catalog = flush_transaction.GetCatalog();
	// the created entries keep their oid under the ids the commit gave them
	ducklake_entry_oids_t entry_oids;
	for (auto &entry : response.entry_ids) {
		auto type = EnumUtil::FromString<CatalogType>(entry.type);
		entry_oids.emplace(make_pair(type, entry.id), entry.entry_reference);
	}
	catalog.SetEntryOids(entry_oids);
	for (auto &setting : response.table_options) {
		DuckLakeConfigOption option;
		option.option.key = setting.key;
		option.option.value = setting.value;
		option.table_id = TableIndex(setting.table_id.value);
		catalog.SetConfigOption(option);
	}
	catalog.SetCommittedSnapshotId(response.snapshot_id.GetIndex());
	if (!response.schema_version.IsValid()) {
		throw InvalidInputException("DuckLake REST: the server did not return the schema version of the commit");
	}
	flush_transaction.ApplyServerSideCommit(response.schema_version.GetIndex());
}

} // namespace duckdb
