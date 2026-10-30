//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_avro_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! ducklake_avro_read(path) -> (record VARCHAR), one JSON text per record
class DuckLakeAvroReadFunction : public TableFunction {
public:
	DuckLakeAvroReadFunction();
};

//! ducklake_avro_schema(path) -> (key VARCHAR, value VARCHAR): schema JSON and header metadata
class DuckLakeAvroSchemaFunction : public TableFunction {
public:
	DuckLakeAvroSchemaFunction();
};

} // namespace duckdb
