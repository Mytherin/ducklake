//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_catalog_changes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_json.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {
class ClientContext;
struct AlterInfo;
struct AlterEntryData;

//! Conversion of catalog changes (ALTER statements, column definitions) to and from their REST API representation
struct DuckLakeRESTCatalogChanges {
	static void WriteColumn(RESTJsonOut out, const ColumnDefinition &column, bool not_null);
	static ColumnDefinition ReadColumn(ClientContext &context, RESTJsonIn in);

	static void WriteAlter(RESTJsonOut out, const AlterInfo &info);
	static unique_ptr<AlterInfo> ReadAlter(ClientContext &context, RESTJsonIn in, const AlterEntryData &entry);
};

} // namespace duckdb
