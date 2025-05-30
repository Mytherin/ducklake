//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class DuckLakeTypes {
public:
	static LogicalType FromString(const string &str);
	static string ToString(const LogicalType &str);
	static void CheckSupportedType(const LogicalType &type);
};

} // namespace duckdb
