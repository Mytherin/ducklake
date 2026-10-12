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
      commit_requests(commit_request.Root().Arr("requests")) {
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
	auto request = commit_requests.AppendObj();
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

unique_ptr<QueryResult> RESTMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                             const string &inlined_table_name,
                                                             const vector<string> &columns_to_read) {
	RESTJsonWriter request;
	auto root = request.Root();
	root.SetUInt("snapshot_id", snapshot.snapshot_id);
	root.SetStr("table_name", inlined_table_name);
	auto columns = root.Arr("columns");
	for (auto &column : columns_to_read) {
		columns.AppendStr(column);
	}
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

vector<DuckLakeSnapshotInfo> RESTMetadataManager::GetAllSnapshots(const string &filter) {
	if (!filter.empty()) {
		throw NotImplementedException("DuckLake REST catalogs do not support filtering snapshots in SQL: %s", filter);
	}
	RESTJsonWriter request;
	auto response = Post("list_snapshots", request);
	vector<DuckLakeSnapshotInfo> result;
	for (auto &snapshot : response->Root().Get("snapshots").Items()) {
		result.push_back(DuckLakeRESTSerializer::ReadSnapshotInfo(snapshot));
	}
	return result;
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

void RESTMetadataManager::AddDataChanges(const TransactionChangeInformation &transaction_changes) {
	auto &dropped_tables = transaction_changes.dropped_tables;
	set<TableIndex> tables_with_deletes;
	for (auto &entry : transaction.GetLocalChanges().Changes()) {
		if (dropped_tables.find(entry.first) != dropped_tables.end()) {
			continue;
		}
		auto table_id = ToRequestId(entry.first.index);
		auto &changes = entry.second;
		if (!changes.new_data_files.empty()) {
			auto parameters = AddCommitRequest("insert_files");
			parameters.SetInt("table_id", table_id);
			auto files = parameters.Arr("files");
			for (auto &file : changes.new_data_files) {
				DuckLakeRESTSerializer::Write(files.AppendObj(), file);
			}
		}
		if (changes.new_inlined_data && changes.new_inlined_data->data) {
			auto parameters = AddCommitRequest("insert_rows");
			parameters.SetInt("table_id", table_id);
			WriteInlinedRows(parameters, *changes.new_inlined_data);
		}
		if (!changes.compactions.empty()) {
			throw NotImplementedException("DuckLake REST catalogs do not support compaction yet");
		}
		if (changes.new_delete_files.empty() && changes.new_inlined_data_deletes.empty() &&
		    !changes.new_inlined_file_deletes) {
			continue;
		}
		tables_with_deletes.insert(entry.first);
		auto parameters = AddCommitRequest("delete_data");
		parameters.SetInt("table_id", table_id);
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
		auto parameters = AddCommitRequest("delete_data");
		parameters.SetInt("table_id", ToRequestId(entry.first.index));
		WriteDroppedFiles(parameters, entry.first);
	}
	for (auto &table_id : transaction.GetTablesDeleteAttempted()) {
		if (dropped_tables.find(table_id) != dropped_tables.end()) {
			continue;
		}
		auto parameters = AddCommitRequest("delete_data");
		parameters.SetInt("table_id", ToRequestId(table_id.index));
		parameters.SetBool("delete_attempted", true);
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
	if (!flush_transaction.GetFlushedInlinedTables().empty() ||
	    !flush_transaction.GetFlushedInlinedFileDeletions().empty() ||
	    !flush_transaction.GetNewNameMaps().name_maps.empty()) {
		throw NotImplementedException("DuckLake REST catalogs do not support flushing inlined data or name maps yet");
	}
	AddDataChanges(transaction_changes);
	auto root = commit_request.Root();
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
