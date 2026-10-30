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

//! The rows of one table that DuckLake keeps in its catalog at a snapshot
struct IcebergInlinedRows {
	idx_t schema_version = 0;
	idx_t row_count = 0;
};

//! The deletes of the rows of a data file that DuckLake keeps in its catalog at a snapshot
struct IcebergInlinedDeletes {
	idx_t data_file_id = 0;
	idx_t delete_count = 0;
};

//! The data of a table that DuckLake keeps in its catalog, which is exposed to Iceberg as Parquet files
class IcebergInlinedExport {
public:
	static unique_ptr<IcebergInlinedExport> Create(IcebergRequest &request, const DuckLakeTableInfo &table);

	//! Whether rows of the table are inlined at the snapshot
	bool HasRows(idx_t snapshot_id) const;
	//! Whether file deletions of the table are inlined at the snapshot
	bool HasDeletes(idx_t snapshot_id) const;
	//! The inlined rows of the table that are visible at the snapshot
	vector<IcebergInlinedRows> RowsAt(idx_t snapshot_id) const;
	//! The inlined deletes of the data files at the snapshot
	vector<IcebergInlinedDeletes> DeletesAt(idx_t snapshot_id);
	//! The Parquet file with the inlined rows that are visible at the snapshot
	string GenerateRows(idx_t snapshot_id, idx_t schema_version, const vector<DuckLakeColumnInfo> &columns);
	//! The position delete file with the inlined deletes of a data file at the snapshot
	string GenerateDeletes(idx_t snapshot_id, idx_t data_file_id, const string &data_path, idx_t row_id_start);

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
