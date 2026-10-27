//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_table_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"
#include "iceberg/iceberg_types.hpp"

namespace duckdb {

//! The files that were generated for a snapshot of a table
struct IcebergExportedSnapshot {
	string manifest_list_path;
	string operation;
	map<string, string> summary;
};

//! Remembers the manifest lists that the server generated, which do not change once their snapshot is committed
class IcebergExportCache {
public:
	bool Get(const string &key, IcebergExportedSnapshot &result);
	void Put(const string &key, IcebergExportedSnapshot value);

private:
	mutex lock;
	map<string, IcebergExportedSnapshot> entries;
};

//! The Iceberg view of a table at the latest snapshot of a request
struct IcebergTableLoad {
	//! The table metadata JSON, and the location of the file that holds the same metadata
	string metadata_json;
	string metadata_location;
	//! The state that updates are validated against
	string table_uuid;
	optional_idx current_snapshot_id;
	idx_t current_schema_id = 0;
	idx_t last_column_id = 0;
	idx_t default_spec_id = 0;
	idx_t last_partition_id = 0;
	idx_t last_sequence_number = 0;
	vector<IcebergSchema> schemas;
};

//! Generates the Iceberg metadata of DuckLake tables: the table metadata JSON, and the Avro manifest lists and
//! manifests of the snapshots
class IcebergTableExporter {
public:
	explicit IcebergTableExporter(IcebergRequest &request);

	//! The metadata of a table. The files of the snapshots are written to the metadata path of the server.
	//! snapshots is "all" or "refs", as in the load table request.
	IcebergTableLoad Export(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table,
	                        const string &snapshots = "all");

	//! The state of a table that requirements of updates are checked against, no files are written
	IcebergTableLoad GetState(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table);

private:
	IcebergRequest &request;
};

//! The response body of load table / create table / update table requests
string IcebergLoadTableResponse(const IcebergTableLoad &load, bool include_config, const map<string, string> &config);

} // namespace duckdb
