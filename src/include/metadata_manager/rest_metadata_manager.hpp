//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/rest_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"
#include "rest/ducklake_rest_client.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
class DuckLakeTableEntry;
class DuckLakeSchemaEntry;
struct CreateTableInfo;
struct CreateSchemaInfo;

//! Communicates with a DuckLake REST server instead of a metadata database
class RESTMetadataManager : public DuckLakeMetadataManager {
public:
	explicit RESTMetadataManager(DuckLakeTransaction &transaction);
	~RESTMetadataManager() override;

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<RESTMetadataManager>(transaction);
	}

	bool IsRemote() const override {
		return true;
	}
	bool SupportsAppender() const override {
		return false;
	}
	bool CommitsEachStatement() const override {
		return true;
	}
	bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const override {
		return true;
	}
	bool HasPendingChanges() const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;

	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	bool MetadataExists() override;
	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;
	DuckLakeMetadata LoadDuckLake() override;

	using DuckLakeMetadataManager::Execute;
	using DuckLakeMetadataManager::Query;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Execute(string &query) override;
	unique_ptr<QueryResult> ExecuteInTransaction(string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;

	unique_ptr<DuckLakeSnapshot> GetSnapshot() override;
	unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound) override;
	DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot) override;
	map<TableIndex, idx_t> GetTableRecordCounts(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeFileListEntry> GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                               const FilterPushdownInfo *filter_info = nullptr,
	                                               bool include_local_changes = true) override;
	vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr) override;
	idx_t GetNextColumnId(TableIndex table_id) override;
	vector<DuckLakeCompactionFileEntry> GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type,
	                                                          double deletion_threshold, DuckLakeSnapshot snapshot,
	                                                          DuckLakeFileSizeOptions options) override;
	unique_ptr<QueryResult> ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
	                                                  const string &inlined_table_name,
	                                                  const vector<string> &columns_to_read) override;
	unique_ptr<QueryResult> ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
	                                                 const string &inlined_table_name,
	                                                 const vector<string> &columns_to_read) override;
	unique_ptr<QueryResult> ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                                   const DuckLakeTableEntry &table, const string &sort_order_sql,
	                                                   const vector<string> &columns_to_read) override;
	DuckLakeDeletedRowVersions GetDeletedInlinedRowVersions(DuckLakeTableEntry &table, const string &inlined_table_name,
	                                                        DuckLakeSnapshot snapshot) override;
	vector<DuckLakeInlinedFileDeletionEntry> GetInlinedFileDeletionsForFlush(DuckLakeTableEntry &table,
	                                                                         DuckLakeSnapshot snapshot) override;
	bool IsColumnCreatedWithTable(const string &table_name, const string &column_name) override;
	unique_ptr<QueryResult> ReadInlinedData(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                        const vector<string> &columns_to_read) override;
	idx_t GetBeginSnapshotForTable(TableIndex table_id) override;
	idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version) override;
	DuckLakeTableSchemaVersions GetTableSchemaVersions(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	vector<DuckLakeSnapshotInfo> GetAllSnapshots(const DuckLakeSnapshotFilter &filter = {}) override;
	void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) override;
	vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &older_than) override;
	vector<string> GetKnownFilePaths() override;
	vector<string> GetMetadataTableNames() override;
	unique_ptr<QueryResult> ReadMetadataTable(const string &table_name) override;
	void SetConfigOption(const DuckLakeConfigOption &option) override;
	bool ResetConfigOption(const DuckLakeConfigOption &option) override;
	vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from) override;
	vector<DuckLakeFileListEntry> GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                 DuckLakeSnapshot snapshot) override;
	vector<DuckLakeDeleteScanEntry> GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                  DuckLakeSnapshot snapshot,
	                                                  bool include_local_changes = true) override;
	void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) override;
	idx_t GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	optional_idx GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	idx_t GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot) override;
	unordered_set<string> GetInlinedTableNames(TableIndex table_id) override;

	void OnCreateSchema(DuckLakeSchemaEntry &schema) override;
	void OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info,
	                   const DuckLakeTableEntry &table) override;
	void OnCreateView(DuckLakeViewEntry &view) override;
	void OnCreateMacro(MacroCatalogEntry &macro) override;
	void OnAlterEntry(DuckLakeSchemaEntry &schema, const AlterInfo &info) override;
	void OnDropEntry(CatalogEntry &entry) override;
	void OnEntryChanged(CatalogEntry &entry) override;
	void OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path, idx_t row_count,
	                idx_t file_size_bytes) override;

private:
	DuckLakeRESTClient &GetClient();
	//! Adds a catalog change to the requests that are sent when the transaction commits
	template <class T>
	T &AddCommitRequest() {
		auto request = make_uniq<T>();
		auto &result = *request;
		catalog_requests.push_back(std::move(request));
		return result;
	}
	unique_ptr<QueryResult> ToQueryResult(RESTInlinedRows rows);
	//! Keeps the inlined rows that came along with a file list, until the scan reads them
	void CacheInlinedRows(DuckLakeSnapshot snapshot, vector<RESTInlinedTableRows> &inlined_data);
	unique_ptr<QueryResult> TryReadCachedInlinedRows(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                                 const vector<string> &columns_to_read);
	RESTSnapshotDataResponse GetSnapshotData(optional_ptr<DuckLakeSnapshot> snapshot, bool include_catalog,
	                                         bool include_stats);
	void AddDeletes(const TransactionChangeInformation &changes, vector<unique_ptr<RESTCommitRequest>> &requests);
	void AddInserts(const TransactionChangeInformation &changes, vector<unique_ptr<RESTCommitRequest>> &requests);
	void AddNameMaps(vector<unique_ptr<RESTCommitRequest>> &requests);
	//! The inlined rows the transaction deleted, which are not flushed
	set<idx_t> GetTransactionDeletes(const DuckLakeTableEntry &table, const string &inlined_table_name);
	RESTTableChangesResponse GetTableChanges(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                         DuckLakeSnapshot snapshot, const string &changes);
	vector<RESTDroppedFile> GetDroppedFiles(TableIndex table_id);
	optional_idx GetRowCount(DuckLakeSnapshot snapshot, const string &count, TableIndex table_id,
	                         const string &table_name);

private:
	//! The catalog changes of the transaction, in the order in which they were made
	vector<unique_ptr<RESTCommitRequest>> catalog_requests;
	mutex cache_lock;
	//! The catalog of a snapshot that the transaction loaded, which came along with the snapshot
	unique_ptr<DuckLakeCatalogInfo> prefetched_catalog;
	idx_t prefetched_metadata_version = DConstants::INVALID_INDEX;
	//! The inlined rows of file lists that were not read yet, by snapshot and inlined data table
	map<pair<idx_t, string>, RESTInlinedRows> inlined_rows;
	//! The table statistics read by the transaction, by snapshot
	unordered_map<idx_t, vector<DuckLakeGlobalStatsInfo>> table_stats;
	//! The data files dropped by the transaction, by table
	map<TableIndex, vector<DuckLakeFileListEntry>> dropped_files;
};

} // namespace duckdb
