//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_filters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_json.hpp"

namespace duckdb {
class ClientContext;
class DuckLakeTableEntry;
struct FilterPushdownInfo;

//! Conversion of the filters of a scan to and from SQL expressions over the columns of the table
struct DuckLakeRESTFilters {
	//! Filters that cannot be expressed in SQL are left out, they are only used to prune files
	static void Write(RESTJsonOut out, const FilterPushdownInfo &filters, const DuckLakeTableEntry &table);
	//! Expressions that cannot be used to prune files are left out
	static unique_ptr<FilterPushdownInfo> Read(ClientContext &context, RESTJsonIn in, const DuckLakeTableEntry &table);
};

} // namespace duckdb
