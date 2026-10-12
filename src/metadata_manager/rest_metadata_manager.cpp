#include "metadata_manager/rest_metadata_manager.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "rest/ducklake_rest_catalog_changes.hpp"
#include "rest/ducklake_rest_serialization.hpp"
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

RESTMetadataManager::RESTMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction), client(transaction.GetCatalog().GetRESTClient()),
      catalog_change_requests(catalog_requests.Root().Arr("requests")) {
}

RESTMetadataManager::~RESTMetadataManager() {
}

//! Entries created by the transaction are referred to by negative placeholder ids
static int64_t ToRequestId(idx_t id) {
	if (DuckLakeTransaction::IsTransactionLocal(id)) {
		return -NumericCast<int64_t>(id - DuckLakeConstants::TRANSACTION_LOCAL_ID_START + 1);
	}
	return NumericCast<int64_t>(id);
}

unique_ptr<RESTJsonReader> RESTMetadataManager::Post(const string &endpoint, const RESTJsonWriter &request) {
	return client.Post(endpoint, request);
}

RESTJsonOut RESTMetadataManager::AddCommitRequest(const string &path) {
	auto request = catalog_change_requests.AppendObj();
	request.SetStr("path", path);
	return request.Obj("parameters");
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
	RESTJsonWriter request;
	auto response = Post("config", request);
	return DuckLakeRESTSerializer::ReadMetadata(response->Root());
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
unique_ptr<RESTJsonReader> RESTMetadataManager::GetSnapshotData(optional_ptr<DuckLakeSnapshot> snapshot,
                                                                bool include_catalog, bool include_stats) {
	RESTJsonWriter request;
	auto root = request.Root();
	if (snapshot) {
		root.SetUInt("snapshot_id", snapshot->snapshot_id);
	}
	root.SetBool("include_catalog", include_catalog);
	root.SetBool("include_stats", include_stats);
	return Post("snapshot_data", request);
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot() {
	auto response = GetSnapshotData(nullptr, false, false);
	return make_uniq<DuckLakeSnapshot>(DuckLakeRESTSerializer::ReadSnapshot(response->Root().Get("snapshot")));
}

unique_ptr<DuckLakeSnapshot> RESTMetadataManager::GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound) {
	auto value = at_clause.GetValue();
	if (at_clause.Unit() == "timestamp") {
		auto context = transaction.context.lock();
		value = value.CastAs(*context, LogicalType::TIMESTAMP_TZ);
	}
	RESTJsonWriter request;
	auto at = request.Root().Obj("at");
	at.SetStr("unit", at_clause.Unit().GetIdentifierName());
	at.SetValue("value", value);
	request.Root().SetStr("bound", bound == SnapshotBound::LOWER_BOUND ? "lower" : "upper");
	auto response = Post("snapshot_data", request);
	return make_uniq<DuckLakeSnapshot>(DuckLakeRESTSerializer::ReadSnapshot(response->Root().Get("snapshot")));
}

DuckLakeCatalogInfo RESTMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto response = GetSnapshotData(snapshot, true, false);
	return DuckLakeRESTSerializer::ReadCatalogInfo(response->Root().Get("catalog"));
}

vector<DuckLakeGlobalStatsInfo> RESTMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot) {
	auto response = GetSnapshotData(snapshot, false, true);
	vector<DuckLakeGlobalStatsInfo> result;
	for (auto &stats : response->Root().Get("stats").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadGlobalStatsInfo(stats));
	}
	return result;
}

map<TableIndex, idx_t> RESTMetadataManager::GetTableRecordCounts(DuckLakeSnapshot snapshot) {
	RESTJsonWriter request;
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	request.Root().SetBool("include_record_counts", true);
	auto response = Post("snapshot_data", request);
	map<TableIndex, idx_t> result;
	for (auto &entry : response->Root().Get("record_counts").Items()) {
		result.emplace(TableIndex(entry.UInt("table_id")), entry.UInt("record_count"));
	}
	return result;
}

vector<DuckLakeFileListEntry> RESTMetadataManager::GetFilesForTable(DuckLakeTableEntry &table,
                                                                    DuckLakeSnapshot snapshot,
                                                                    const FilterPushdownInfo *filter_info) {
	vector<DuckLakeFileListEntry> result;
	if (table.IsTransactionLocal()) {
		return result;
	}
	RESTJsonWriter request;
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	request.Root().SetUInt("table_id", table.GetTableId().index);
	auto response = Post("select", request);
	for (auto &file : response->Root().Get("files").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadFileListEntry(file));
	}
	return result;
}

vector<DuckLakeFileListExtendedEntry>
RESTMetadataManager::GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                              const FilterPushdownInfo *filter_info) {
	vector<DuckLakeFileListExtendedEntry> result;
	if (table.IsTransactionLocal()) {
		return result;
	}
	RESTJsonWriter request;
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	request.Root().SetUInt("table_id", table.GetTableId().index);
	request.Root().SetBool("extended", true);
	auto response = Post("select", request);
	for (auto &file : response->Root().Get("files").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadFileListExtendedEntry(file));
	}
	return result;
}

vector<DuckLakeCompactionFileEntry>
RESTMetadataManager::GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type, double deletion_threshold,
                                           DuckLakeSnapshot snapshot, DuckLakeFileSizeOptions options) {
	vector<DuckLakeCompactionFileEntry> result;
	if (table.IsTransactionLocal()) {
		return result;
	}
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetUInt("table_id", table.GetTableId().index);
	root.SetStr("type", CompactionTypeToString(type));
	root.SetDouble("delete_threshold", deletion_threshold);
	root.SetOpt("min_file_size", options.min_file_size);
	root.SetOpt("max_file_size", options.max_file_size);
	root.SetValue("newer_than", options.newer_than);
	root.SetValue("older_than", options.older_than);
	root.SetUInt("target_file_size", options.target_file_size);
	auto response = Post("files_for_compaction", request);
	for (auto &file : response->Root().Get("files").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadCompactionFileEntry(file));
	}
	return result;
}

idx_t RESTMetadataManager::GetNextColumnId(TableIndex table_id) {
	RESTJsonWriter request;
	request.Root().SetUInt("table_id", table_id.index);
	auto response = Post("next_column_id", request);
	return response->Root().UInt("next_column_id");
}

bool RESTMetadataManager::IsColumnCreatedWithTable(const string &table_name, const string &column_name) {
	RESTJsonWriter request;
	request.Root().SetStr("table_name", table_name);
	request.Root().SetStr("column_name", column_name);
	auto response = Post("column_created_with_table", request);
	return response->Root().Get("created_with_table").Bool();
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedRows(RESTJsonWriter &request) {
	auto response = Post("read_inlined_data", request);
	auto response_root = response->Root();
	vector<Identifier> names;
	vector<LogicalType> types;
	for (auto &column : response_root.Get("columns").Items()) {
		names.emplace_back(column.Str("name"));
		types.push_back(RESTJson::TypeFromJson(column.Get("type")));
	}
	auto context = transaction.context.lock();
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	for (auto &row : response_root.Get("rows").Items()) {
		auto values = row.Items();
		auto row_idx = chunk.size();
		for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
			chunk.SetValue(col_idx, row_idx, RESTJson::TypedValueFromJson(values[col_idx], types[col_idx]));
		}
		chunk.SetCardinality(row_idx + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		collection->Append(append_state, chunk);
	}
	return make_uniq<QueryResult>(StatementType::SELECT_STATEMENT, StatementProperties(), std::move(names),
	                              std::move(collection), context->GetClientProperties());
}

static RESTJsonOut InlinedReadRequest(RESTJsonWriter &request, DuckLakeSnapshot snapshot, const string &mode,
                                      const string &inlined_table_name, const vector<string> &columns_to_read) {
	auto root = request.Root();
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetStr("mode", mode);
	root.SetStr("table_name", inlined_table_name);
	auto columns = root.Arr("columns");
	for (auto &column : columns_to_read) {
		columns.AppendStr(column);
	}
	return root;
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                             const string &inlined_table_name,
                                                             const vector<string> &columns_to_read) {
	RESTJsonWriter request;
	InlinedReadRequest(request, snapshot, "scan", inlined_table_name, columns_to_read);
	return ReadInlinedRows(request);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
                                                                       DuckLakeSnapshot end_snapshot,
                                                                       const string &inlined_table_name,
                                                                       const vector<string> &columns_to_read) {
	RESTJsonWriter request;
	auto root = InlinedReadRequest(request, end_snapshot, "insertions", inlined_table_name, columns_to_read);
	root.SetUInt("start_snapshot_id", start_snapshot.snapshot_id);
	return ReadInlinedRows(request);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
                                                                      DuckLakeSnapshot end_snapshot,
                                                                      const string &inlined_table_name,
                                                                      const vector<string> &columns_to_read) {
	RESTJsonWriter request;
	auto root = InlinedReadRequest(request, end_snapshot, "deletions", inlined_table_name, columns_to_read);
	root.SetUInt("start_snapshot_id", start_snapshot.snapshot_id);
	return ReadInlinedRows(request);
}

unique_ptr<QueryResult> RESTMetadataManager::ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot,
                                                                        const string &inlined_table_name,
                                                                        const DuckLakeTableEntry &table,
                                                                        const string &sort_order_sql,
                                                                        const vector<string> &columns_to_read) {
	RESTJsonWriter request;
	auto root = InlinedReadRequest(request, snapshot, "flush", inlined_table_name, columns_to_read);
	root.SetUInt("table_id", table.GetTableId().index);
	root.SetStr("order_by", sort_order_sql);
	return ReadInlinedRows(request);
}

vector<DuckLakeFlushedRowDeletion>
RESTMetadataManager::GetFlushedRowDeletions(DuckLakeTableEntry &table, const string &inlined_table_name,
                                            const string &sort_order_sql, const vector<Value> &partition_values,
                                            idx_t offset, idx_t count, DuckLakeSnapshot snapshot) {
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetUInt("table_id", table.GetTableId().index);
	root.SetStr("table_name", inlined_table_name);
	root.SetStr("order_by", sort_order_sql);
	auto values = root.Arr("partition_values");
	for (auto &value : partition_values) {
		values.AppendRaw(RESTJson::ValueToJson(*root.writer, value));
	}
	root.SetUInt("offset", offset);
	root.SetUInt("count", count);
	auto response = Post("flushed_row_deletions", request);
	vector<DuckLakeFlushedRowDeletion> result;
	for (auto &deletion : response->Root().Get("deletions").Items()) {
		result.push_back({deletion.UInt("position"), deletion.UInt("end_snapshot")});
	}
	return result;
}

vector<DuckLakeInlinedFileDeletionEntry>
RESTMetadataManager::GetInlinedFileDeletionsForFlush(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot) {
	RESTJsonWriter request;
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	request.Root().SetUInt("table_id", table.GetTableId().index);
	auto response = Post("inlined_file_deletions", request);
	vector<DuckLakeInlinedFileDeletionEntry> result;
	for (auto &deletion : response->Root().Get("deletions").Items()) {
		DuckLakeInlinedFileDeletionEntry entry;
		entry.file_id = DataFileIndex(deletion.UInt("data_file_id"));
		entry.data_file_path = deletion.Str("data_file_path");
		entry.row_id = deletion.UInt("row_id");
		entry.begin_snapshot = deletion.UInt("begin_snapshot");
		auto delete_file_id = deletion.TryGet("delete_file_id");
		if (!delete_file_id.IsNull()) {
			entry.delete_file_id = DataFileIndex(delete_file_id.UInt());
			entry.delete_file = DuckLakeRESTSerializer::ReadFileData(deletion.Get("delete_file"));
			entry.delete_file_begin_snapshot = deletion.UInt("delete_file_begin_snapshot");
		}
		result.push_back(std::move(entry));
	}
	return result;
}

idx_t RESTMetadataManager::GetBeginSnapshotForTable(TableIndex table_id) {
	RESTJsonWriter request;
	request.Root().SetUInt("table_id", table_id.index);
	auto response = Post("table_schema_versions", request);
	return response->Root().UInt("begin_snapshot");
}

idx_t RESTMetadataManager::GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version) {
	auto &catalog = transaction.GetCatalog();
	auto cached_snapshot = catalog.TryGetSchemaVersionBeginSnapshot(table_id, schema_version);
	if (cached_snapshot.IsValid()) {
		return cached_snapshot.GetIndex();
	}
	RESTJsonWriter request;
	request.Root().SetUInt("table_id", table_id.index);
	request.Root().SetUInt("schema_version", schema_version);
	auto response = Post("table_schema_versions", request);
	auto begin_snapshot = response->Root().UInt("schema_version_begin_snapshot");
	catalog.CacheSchemaVersionBeginSnapshot(table_id, schema_version, begin_snapshot);
	return begin_snapshot;
}

DuckLakeTableSchemaVersions RESTMetadataManager::GetTableSchemaVersions(TableIndex table_id,
                                                                        DuckLakeSnapshot snapshot) {
	RESTJsonWriter request;
	request.Root().SetUInt("table_id", table_id.index);
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	auto response = Post("table_schema_versions", request);
	DuckLakeTableSchemaVersions result;
	result.creation = response->Root().Opt("creation");
	result.last_change = response->Root().Opt("last_change");
	return result;
}

optional_idx RESTMetadataManager::GetRowCount(DuckLakeSnapshot snapshot, const string &count, TableIndex table_id,
                                              const string &table_name) {
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetStr("count", count);
	if (table_id.IsValid()) {
		root.SetUInt("table_id", table_id.index);
	}
	if (!table_name.empty()) {
		root.SetStr("table_name", table_name);
	}
	auto response = Post("row_count", request);
	return response->Root().Opt("row_count");
}

idx_t RESTMetadataManager::GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot) {
	return GetRowCount(snapshot, "data_files", table_id, string()).GetIndex();
}

optional_idx RESTMetadataManager::GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot) {
	return GetRowCount(snapshot, "data_files_exact", table_id, string());
}

idx_t RESTMetadataManager::GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot) {
	return GetRowCount(snapshot, "inlined", TableIndex(), inlined_table_name).GetIndex();
}

unordered_set<string> RESTMetadataManager::GetInlinedTableNames(TableIndex table_id) {
	RESTJsonWriter request;
	request.Root().SetUInt("table_id", table_id.index);
	auto response = Post("inlined_tables", request);
	unordered_set<string> result;
	for (auto &name : response->Root().Get("table_names").Items()) {
		result.insert(name.Str());
	}
	return result;
}

vector<DuckLakeSnapshotInfo> RESTMetadataManager::GetAllSnapshots(const DuckLakeSnapshotFilter &filter) {
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetBool("exclude_latest", filter.exclude_latest);
	if (!filter.older_than.empty()) {
		root.SetStr("older_than", filter.older_than);
	}
	if (filter.has_snapshot_ids) {
		auto snapshot_ids = root.Arr("snapshot_ids");
		for (auto snapshot_id : filter.snapshot_ids) {
			snapshot_ids.AppendUInt(snapshot_id);
		}
	}
	auto response = Post("list_snapshots", request);
	vector<DuckLakeSnapshotInfo> result;
	for (auto &snapshot : response->Root().Get("snapshots").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadSnapshotInfo(snapshot));
	}
	return result;
}

void RESTMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	RESTJsonWriter request;
	auto snapshot_ids = request.Root().Arr("snapshot_ids");
	for (auto &snapshot : snapshots) {
		snapshot_ids.AppendUInt(snapshot.id);
	}
	Post("expire_snapshots", request);
}

vector<DuckLakeFileForCleanup> RESTMetadataManager::GetOldFilesForCleanup(const string &older_than) {
	RESTJsonWriter request;
	if (!older_than.empty()) {
		request.Root().SetStr("older_than", older_than);
	}
	auto response = Post("files_for_cleanup", request);
	vector<DuckLakeFileForCleanup> result;
	for (auto &file : response->Root().Get("files").Items()) {
		DuckLakeFileForCleanup info;
		info.id = DataFileIndex(file.UInt("data_file_id"));
		info.path = file.Str("path");
		info.time =
		    Value(file.Str("schedule_start")).DefaultCastAs(LogicalType::TIMESTAMP_TZ).GetValue<timestamp_tz_t>();
		result.push_back(std::move(info));
	}
	return result;
}

vector<string> RESTMetadataManager::GetKnownFilePaths() {
	RESTJsonWriter request;
	auto response = Post("known_files", request);
	vector<string> result;
	for (auto &path : response->Root().Get("paths").Items()) {
		result.push_back(path.Str());
	}
	return result;
}

void RESTMetadataManager::SetConfigOption(const DuckLakeConfigOption &option) {
	RESTJsonWriter request;
	DuckLakeRESTSerializer::Write(request.Root().Obj("option"), option);
	Post("set_option", request);
}

bool RESTMetadataManager::ResetConfigOption(const DuckLakeConfigOption &option) {
	RESTJsonWriter request;
	DuckLakeRESTSerializer::Write(request.Root().Obj("option"), option);
	request.Root().SetBool("reset", true);
	auto response = Post("set_option", request);
	return response->Root().Bool("reset", false);
}

vector<DuckLakeTableSizeInfo> RESTMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	RESTJsonWriter request;
	request.Root().SetUInt("snapshot_id", snapshot.snapshot_id);
	auto response = Post("table_sizes", request);
	vector<DuckLakeTableSizeInfo> result;
	for (auto &table : response->Root().Get("tables").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadTableSizeInfo(table));
	}
	return result;
}

vector<DuckLakeColumnMappingInfo> RESTMetadataManager::GetColumnMappings(optional_idx start_from) {
	RESTJsonWriter request;
	request.Root().SetOpt("start_from", start_from);
	auto response = Post("column_mappings", request);
	vector<DuckLakeColumnMappingInfo> result;
	for (auto &mapping : response->Root().Get("mappings").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadColumnMappingInfo(mapping));
	}
	return result;
}

unique_ptr<RESTJsonReader> RESTMetadataManager::GetTableChanges(DuckLakeTableEntry &table,
                                                                DuckLakeSnapshot start_snapshot,
                                                                DuckLakeSnapshot snapshot, const string &changes) {
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetUInt("table_id", table.GetTableId().index);
	root.SetUInt("start_snapshot_id", start_snapshot.snapshot_id);
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetStr("changes", changes);
	return Post("table_changes", request);
}

vector<DuckLakeFileListEntry> RESTMetadataManager::GetTableInsertions(DuckLakeTableEntry &table,
                                                                      DuckLakeSnapshot start_snapshot,
                                                                      DuckLakeSnapshot snapshot) {
	vector<DuckLakeFileListEntry> result;
	if (table.IsTransactionLocal()) {
		return result;
	}
	auto response = GetTableChanges(table, start_snapshot, snapshot, "insertions");
	for (auto &file : response->Root().Get("files").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadFileListEntry(file));
	}
	return result;
}

vector<DuckLakeDeleteScanEntry> RESTMetadataManager::GetTableDeletions(DuckLakeTableEntry &table,
                                                                       DuckLakeSnapshot start_snapshot,
                                                                       DuckLakeSnapshot snapshot) {
	vector<DuckLakeDeleteScanEntry> result;
	if (table.IsTransactionLocal()) {
		return result;
	}
	auto response = GetTableChanges(table, start_snapshot, snapshot, "deletions");
	for (auto &file : response->Root().Get("files").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadDeleteScanEntry(file));
	}
	return result;
}

void RESTMetadataManager::RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) {
	RESTJsonWriter request;
	auto file_ids = request.Root().Arr("data_file_ids");
	for (auto &file : cleaned_up_files) {
		file_ids.AppendUInt(file.id.index);
	}
	Post("remove_cleaned_up_files", request);
}

//===--------------------------------------------------------------------===//
// Catalog Changes
//===--------------------------------------------------------------------===//
static void WriteSchemaPath(RESTJsonOut out, DuckLakeSchemaEntry &schema) {
	vector<string> path;
	for (optional_ptr<DuckLakeSchemaEntry> entry = &schema; entry; entry = entry->ParentDuckLakeSchema()) {
		path.push_back(entry->name.GetIdentifierName());
	}
	auto path_out = out.Arr("schema_path");
	for (idx_t i = path.size(); i > 0; i--) {
		path_out.AppendStr(path[i - 1]);
	}
}

void RESTMetadataManager::OnCreateSchema(DuckLakeSchemaEntry &schema) {
	auto parameters = AddCommitRequest("create_schema");
	WriteSchemaPath(parameters, schema);
	parameters.SetStr("schema_uuid", schema.GetSchemaUUID());
	parameters.SetStr("path", schema.DataPath());
}

void RESTMetadataManager::OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info, TableIndex table_id,
                                        const string &table_uuid, const string &table_data_path) {
	auto parameters = AddCommitRequest("create_table");
	parameters.SetInt("table_id", ToRequestId(table_id.index));
	WriteSchemaPath(parameters, schema);
	parameters.SetStr("table_name", info.GetTableName().GetIdentifierName());
	parameters.SetStr("table_uuid", table_uuid);
	parameters.SetStr("path", table_data_path);
	auto columns = parameters.Arr("columns");
	for (auto &column : info.columns.Logical()) {
		DuckLakeRESTCatalogChanges::WriteColumn(columns.AppendObj(), column,
		                                        info.FindNotNullConstraint(column.Logical()).IsValid());
	}
	auto partitioned_by = parameters.Arr("partitioned_by");
	for (auto &key : info.partition_keys) {
		partitioned_by.AppendStr(key->ToString());
	}
	auto sorted_by = parameters.Arr("sorted_by");
	for (auto &key : info.sort_keys) {
		sorted_by.AppendStr(key->ToString());
	}
	auto options = parameters.Obj("options");
	for (auto &option : info.options) {
		options.SetStr(option.first, option.second->ToString());
	}
	if (!info.comment.IsNull()) {
		parameters.SetStr("comment", info.comment.ToString());
	}
}

void RESTMetadataManager::OnCreateView(DuckLakeViewEntry &view) {
	auto parameters = AddCommitRequest("create_view");
	WriteSchemaPath(parameters, view.ParentSchema().Cast<DuckLakeSchemaEntry>());
	parameters.SetStr("view_name", view.name.GetIdentifierName());
	parameters.SetStr("view_uuid", view.GetViewUUID());
	parameters.SetStr("dialect", "duckdb");
	parameters.SetStr("sql", view.GetQuerySQL());
	auto aliases = parameters.Arr("column_aliases");
	for (auto &alias : view.aliases) {
		aliases.AppendStr(alias.GetIdentifierName());
	}
	if (!view.comment.IsNull()) {
		parameters.SetStr("comment", view.comment.ToString());
	}
}

void RESTMetadataManager::OnCreateMacro(MacroCatalogEntry &macro) {
	auto parameters = AddCommitRequest("create_macro");
	WriteSchemaPath(parameters, macro.schema.Cast<DuckLakeSchemaEntry>());
	DuckLakeRESTSerializer::Write(parameters.Obj("macro"), DuckLakeTransactionState::GetMacroInfo(macro));
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

void RESTMetadataManager::OnAlterEntry(DuckLakeSchemaEntry &schema, const AlterInfo &info) {
	auto parameters = AddCommitRequest(AltersView(info) ? "alter_view" : "alter_table");
	WriteSchemaPath(parameters, schema);
	parameters.SetStr("name", info.GetQualifiedName().Name().GetIdentifierName());
	DuckLakeRESTCatalogChanges::WriteAlter(parameters.Obj("alter"), info);
}

void RESTMetadataManager::OnDropEntry(CatalogEntry &entry) {
	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto parameters = AddCommitRequest("drop_schema");
		WriteSchemaPath(parameters, entry.Cast<DuckLakeSchemaEntry>());
		return;
	}
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		break;
	default:
		throw InternalException("Unsupported catalog entry type for DROP");
	}
	string path = entry.type == CatalogType::TABLE_ENTRY  ? "drop_table"
	              : entry.type == CatalogType::VIEW_ENTRY ? "drop_view"
	                                                      : "drop_macro";
	auto parameters = AddCommitRequest(path);
	WriteSchemaPath(parameters, entry.ParentSchema().Cast<DuckLakeSchemaEntry>());
	parameters.SetStr("name", entry.name.GetIdentifierName());
	if (entry.type == CatalogType::MACRO_ENTRY || entry.type == CatalogType::TABLE_MACRO_ENTRY) {
		parameters.SetStr("macro_type", entry.type == CatalogType::MACRO_ENTRY ? "scalar" : "table");
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
static void WriteInlinedRows(RESTJsonOut parameters, const DuckLakeInlinedData &inlined_data) {
	auto &data = *inlined_data.data;
	auto types = parameters.Arr("column_types");
	for (auto &type : data.Types()) {
		types.AppendRaw(RESTJson::TypeToJson(*parameters.writer, type));
	}
	auto rows = parameters.Arr("rows");
	for (auto &chunk : data.Chunks()) {
		for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
			auto row = rows.AppendArr();
			for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
				row.AppendRaw(RESTJson::TypedValueToJson(*parameters.writer, chunk.GetValue(col_idx, row_idx)));
			}
		}
	}
	if (inlined_data.HasPreservedRowIds()) {
		auto row_ids = parameters.Arr("row_ids");
		for (auto row_id : inlined_data.row_ids) {
			row_ids.AppendRaw(parameters.writer->CreateSignedInteger(row_id));
		}
	}
	auto stats = parameters.Arr("column_stats");
	for (auto &entry : inlined_data.column_stats) {
		auto stats_out = stats.AppendObj();
		stats_out.SetUInt("column_id", entry.first.index);
		DuckLakeRESTSerializer::Write(stats_out, entry.second);
	}
}

static void WriteRowIds(RESTJsonOut out, const string &key, const set<idx_t> &row_ids) {
	auto row_ids_out = out.Arr(key);
	for (auto row_id : row_ids) {
		row_ids_out.AppendUInt(row_id);
	}
}

static RESTJsonOut AddRequest(RESTJsonOut &requests, const string &path) {
	auto request = requests.AppendObj();
	request.SetStr("path", path);
	return request.Obj("parameters");
}

void RESTMetadataManager::AddDeletes(const TransactionChangeInformation &transaction_changes, RESTJsonOut requests) {
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
		auto parameters = AddRequest(requests, "delete_data");
		parameters.SetInt("table_id", ToRequestId(entry.first.index));
		auto delete_files = parameters.Arr("delete_files");
		for (auto &file_entry : changes.new_delete_files) {
			for (auto &delete_file : file_entry.second) {
				DuckLakeRESTSerializer::Write(delete_files.AppendObj(), delete_file);
			}
		}
		auto inlined_deletes = parameters.Arr("inlined_deletes");
		for (auto &inlined_entry : changes.new_inlined_data_deletes) {
			auto deletes = inlined_deletes.AppendObj();
			deletes.SetStr("table_name", inlined_entry.first);
			WriteRowIds(deletes, "row_ids", inlined_entry.second->rows);
		}
		auto file_row_deletes = parameters.Arr("file_row_deletes");
		if (changes.new_inlined_file_deletes) {
			for (auto &file_entry : changes.new_inlined_file_deletes->file_deletes) {
				auto deletes = file_row_deletes.AppendObj();
				deletes.SetUInt("data_file_id", file_entry.first);
				WriteRowIds(deletes, "row_ids", file_entry.second);
			}
		}
		WriteDroppedFiles(parameters, entry.first);
	}
	for (auto &entry : dropped_files) {
		if (tables_with_deletes.find(entry.first) != tables_with_deletes.end() ||
		    dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto parameters = AddRequest(requests, "delete_data");
		parameters.SetInt("table_id", ToRequestId(entry.first.index));
		WriteDroppedFiles(parameters, entry.first);
	}
	for (auto &table_id : transaction.GetTablesDeleteAttempted()) {
		if (dropped_tables.find(table_id) != dropped_tables.end()) {
			continue;
		}
		auto parameters = AddRequest(requests, "delete_data");
		parameters.SetInt("table_id", ToRequestId(table_id.index));
		parameters.SetBool("delete_attempted", true);
	}
}

void RESTMetadataManager::AddInserts(const TransactionChangeInformation &transaction_changes, RESTJsonOut requests) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto table_id = ToRequestId(entry.first.index);
		auto &changes = entry.second;
		if (!changes.new_data_files.empty()) {
			auto parameters = AddRequest(requests, "insert_files");
			parameters.SetInt("table_id", table_id);
			auto files = parameters.Arr("files");
			for (auto &file : changes.new_data_files) {
				DuckLakeRESTSerializer::Write(files.AppendObj(), file);
			}
		}
		if (changes.new_inlined_data && changes.new_inlined_data->data) {
			auto parameters = AddRequest(requests, "insert_rows");
			parameters.SetInt("table_id", table_id);
			WriteInlinedRows(parameters, *changes.new_inlined_data);
		}
		if (!changes.compactions.empty()) {
			auto parameters = AddRequest(requests, "compact");
			parameters.SetInt("table_id", table_id);
			auto compactions = parameters.Arr("compactions");
			for (auto &compaction : changes.compactions) {
				DuckLakeRESTSerializer::Write(compactions.AppendObj(), compaction);
			}
		}
	}
}

void RESTMetadataManager::AddNameMaps(RESTJsonOut requests) {
	for (auto &entry : transaction.GetNewNameMaps().name_maps) {
		auto &name_map = *entry.second;
		auto parameters = AddRequest(requests, "create_name_map");
		parameters.SetInt("table_id", ToRequestId(name_map.table_id.index));
		DuckLakeColumnMappingInfo mapping;
		mapping.table_id = name_map.table_id;
		mapping.mapping_id = name_map.id;
		mapping.map_type = "map_by_name";
		mapping.map_columns = name_map.FlattenColumns();
		DuckLakeRESTSerializer::Write(parameters.Obj("mapping"), mapping);
	}
}

void RESTMetadataManager::WriteDroppedFiles(RESTJsonOut parameters, TableIndex table_id) {
	auto files = parameters.Arr("dropped_files");
	auto entry = dropped_files.find(table_id);
	if (entry == dropped_files.end()) {
		return;
	}
	for (auto &file : entry->second) {
		auto file_out = files.AppendObj();
		file_out.SetUInt("data_file_id", file.file_id.index);
		file_out.SetStr("path", file.file.path);
		file_out.SetUInt("record_count", file.max_row_count.GetIndex());
		file_out.SetUInt("file_size_bytes", file.file.file_size_bytes);
	}
}

void RESTMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                 DuckLakeSnapshot transaction_snapshot,
                                                 const TransactionChangeInformation &transaction_changes,
                                                 const DuckLakeRetryConfig &retry_config) {
	RESTJsonWriter commit_request;
	auto root = commit_request.Root();
	auto requests = root.Arr("requests");
	// deletes go first, as the catalog changes of the transaction can depend on them (e.g. SET NOT NULL)
	AddDeletes(transaction_changes, requests);
	RESTJsonReader catalog_changes(catalog_requests.ToString());
	for (auto &request : catalog_changes.Root().Get("requests").Items()) {
		requests.AppendRaw(commit_request.writer.CreateCopy(request.val));
	}
	// inserts go last, they are in the format of the tables at the end of the transaction
	AddNameMaps(requests);
	AddInserts(transaction_changes, requests);
	auto &flushed_tables = flush_transaction.GetFlushedInlinedTables();
	auto &flushed_file_deletions = flush_transaction.GetFlushedInlinedFileDeletions();
	if (!flushed_tables.empty() || !flushed_file_deletions.empty()) {
		auto parameters = AddRequest(requests, "flush_inlined_data");
		auto inlined_tables = parameters.Arr("inlined_tables");
		for (auto &flushed_table : flushed_tables) {
			auto table_out = inlined_tables.AppendObj();
			table_out.SetStr("table_name", flushed_table.inlined_table.table_name);
			table_out.SetUInt("schema_version", flushed_table.inlined_table.schema_version);
			table_out.SetUInt("flush_snapshot_id", flushed_table.flush_snapshot_id);
		}
		auto file_deletions = parameters.Arr("file_deletions");
		for (auto &entry : flushed_file_deletions) {
			auto deletions_out = file_deletions.AppendObj();
			deletions_out.SetInt("table_id", ToRequestId(entry.first.index));
			deletions_out.SetUInt("flush_snapshot_id", entry.second);
		}
	}
	if (transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX) {
		root.SetUInt("snapshot_id", transaction_snapshot.snapshot_id);
	}
	DuckLakeRESTSerializer::Write(root.Obj("commit_info"), flush_transaction.GetCommitInfo());
	root.SetBool("requires_new_inlined_table", flush_transaction.GetRequiresNewInlinedTable());
	auto response = Post("commit_transaction", commit_request);
	auto response_root = response->Root();
	auto snapshot_id = response_root.TryGet("snapshot_id");
	if (snapshot_id.IsNull()) {
		flush_transaction.ApplyServerSideCommit(flush_transaction.GetCatalogVersion());
		return;
	}
	flush_transaction.GetCatalog().SetCommittedSnapshotId(snapshot_id.UInt());
	flush_transaction.ApplyServerSideCommit(response_root.UInt("schema_version"));
}

} // namespace duckdb
