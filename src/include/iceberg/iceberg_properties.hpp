//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"

namespace duckdb {

//! Properties of tables, views and namespaces that DuckLake has no equivalent for, which the server keeps in the
//! metadata catalog of the DuckLake
class IcebergProperties {
public:
	static map<string, string> GetTableProperties(IcebergRequest &request, const DuckLakeTableInfo &table);
	static void UpdateTableProperties(IcebergRequest &request, const DuckLakeTableInfo &table,
	                                  const map<string, string> &updates, const vector<string> &removals);
	static void SetInitialTableProperties(IcebergRequest &request, const vector<string> &ns, const string &name,
	                                      const map<string, string> &properties);
};

} // namespace duckdb
