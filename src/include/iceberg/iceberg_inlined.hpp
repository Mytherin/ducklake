//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_inlined.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"

namespace duckdb {

//! The stats of a column of a file
struct IcebergFileStats {
	idx_t column_id = 0;
	optional_idx column_size;
	optional_idx value_count;
	optional_idx null_count;
	string min_value;
	string max_value;
	bool has_min = false;
	bool has_max = false;
	bool contains_nan = false;
};

//! A file that was generated from data that is inlined in the catalog
struct IcebergInlinedFile {
	string path;
	idx_t record_count = 0;
	idx_t file_size = 0;
	vector<IcebergFileStats> stats;
};

//! A position delete file for the deletes of data files that are inlined in the catalog
struct IcebergInlinedDeleteFile {
	idx_t data_file_id = 0;
	string path;
	idx_t delete_count = 0;
	idx_t file_size = 0;
};

//! The data of a table that DuckLake keeps in its catalog, which is exposed to Iceberg as Parquet files
class IcebergInlinedExport {
public:
	static unique_ptr<IcebergInlinedExport> Create(IcebergRequest &request, const DuckLakeTableInfo &table);

	//! Whether rows of the table are inlined at the snapshot
	bool HasRows(idx_t snapshot_id) const;
	//! Whether file deletions of the table are inlined at the snapshot
	bool HasDeletes(idx_t snapshot_id) const;
	//! Writes the inlined rows that are visible at the snapshot to Parquet files
	vector<IcebergInlinedFile> Materialize(idx_t snapshot_id, const vector<DuckLakeColumnInfo> &columns,
	                                       const string &directory);
	//! Writes the inlined deletions that are visible at the snapshot to position delete files. The data files are
	//! given by id with their path and the first row id.
	vector<IcebergInlinedDeleteFile>
	MaterializeDeletes(idx_t snapshot_id, const map<idx_t, pair<string, idx_t>> &data_files, const string &directory);

private:
	struct InlinedTable {
		string name;
		idx_t schema_version;
		//! The snapshots in which the rows begin and end
		vector<pair<idx_t, optional_idx>> lifetimes;
	};

	IcebergInlinedExport(IcebergRequest &request, const DuckLakeTableInfo &table) : request(request), table(table) {
	}

private:
	IcebergRequest &request;
	const DuckLakeTableInfo &table;
	vector<InlinedTable> tables;
	//! The snapshots in which file deletions begin
	vector<idx_t> delete_snapshots;
	string delete_table;
};

} // namespace duckdb
