//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_views.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"

namespace duckdb {

//! The endpoints of the views
class IcebergViewEndpoints {
public:
	//! The metadata of a view, as a client reads it from its metadata location
	static string GenerateMetadata(IcebergRequest &request, const DuckLakeSchemaInfo &schema,
	                               const DuckLakeViewInfo &view);
	static void ListViews(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void CreateView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void LoadView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void ReplaceView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void DropView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
};

} // namespace duckdb
