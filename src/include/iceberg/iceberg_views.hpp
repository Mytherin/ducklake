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
	static void ListViews(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void CreateView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void LoadView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void ReplaceView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
	static void DropView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response);
};

} // namespace duckdb
