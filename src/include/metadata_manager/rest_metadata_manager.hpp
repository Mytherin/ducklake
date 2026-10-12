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
	                                               const FilterPushdownInfo *filter_info = nullptr) override;
	vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr) override;
	idx_t GetNextColumnId(TableIndex table_id) override;
	bool IsColumnCreatedWithTable(const string &table_name, const string &column_name) override;
	unique_ptr<QueryResult> ReadInlinedData(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                        const vector<string> &columns_to_read) override;
	idx_t GetBeginSnapshotForTable(TableIndex table_id) override;
	idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version) override;
	DuckLakeTableSchemaVersions GetTableSchemaVersions(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	vector<DuckLakeSnapshotInfo> GetAllSnapshots(const string &filter = string()) override;
	idx_t GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	optional_idx GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot) override;
	idx_t GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot) override;
	unordered_set<string> GetInlinedTableNames(TableIndex table_id) override;

	void OnCreateSchema(DuckLakeSchemaEntry &schema) override;
	void OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info, TableIndex table_id,
	                   const string &table_uuid, const string &table_data_path) override;
	void OnCreateView(DuckLakeViewEntry &view) override;
	void OnCreateMacro(MacroCatalogEntry &macro) override;
	void OnAlterEntry(DuckLakeSchemaEntry &schema, const AlterInfo &info) override;
	void OnDropEntry(CatalogEntry &entry) override;
	void OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path, idx_t row_count,
	                idx_t file_size_bytes) override;

private:
	//! Sends a request to the server, the request is not part of the commit of the transaction
	unique_ptr<RESTJsonReader> Post(const string &endpoint, const RESTJsonWriter &request);
	//! Adds a request to the requests that are sent when the transaction commits
	RESTJsonOut AddCommitRequest(const string &path);
	unique_ptr<RESTJsonReader> GetSnapshotData(optional_ptr<DuckLakeSnapshot> snapshot, bool include_catalog,
	                                           bool include_stats);
	void AddDataChanges(const TransactionChangeInformation &changes);
	void WriteDroppedFiles(RESTJsonOut parameters, TableIndex table_id);
	optional_idx GetRowCount(DuckLakeSnapshot snapshot, const string &count, TableIndex table_id,
	                         const string &table_name);

private:
	DuckLakeRESTClient client;
	//! The requests that make up the commit of the transaction
	RESTJsonWriter commit_request;
	RESTJsonOut commit_requests;
	//! The data files dropped by the transaction, by table
	map<TableIndex, vector<DuckLakeFileListEntry>> dropped_files;
};

} // namespace duckdb
