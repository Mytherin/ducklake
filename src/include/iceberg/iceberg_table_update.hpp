//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_table_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"

namespace duckdb {

//! The endpoints that create and change tables
class IcebergTableEndpoints {
public:
	static void CreateTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void UpdateTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void RegisterTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void CommitTransaction(IcebergRequest &request, const IcebergHttpRequest &http,
	                              IcebergHttpResponse &response);
};

} // namespace duckdb
