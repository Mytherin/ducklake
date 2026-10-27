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

//! Properties of tables, views and namespaces, and references to snapshots, which DuckLake has no equivalent for. The
//! server keeps them in a table of the metadata catalog of the DuckLake.
class IcebergProperties {
public:
	static constexpr const char *TABLE = "table";
	static constexpr const char *NAMESPACE = "namespace";
	static constexpr const char *VIEW = "view";
	//! The references (branches and tags) of a table: the keys are the names of the references, the values JSON
	static constexpr const char *REFERENCE = "reference";

	//! The properties of an entity with the uuid, read in the transaction of the request
	static map<string, string> Get(IcebergRequest &request, const string &entity_type, const string &entity_uuid);
	//! Changes the properties of an entity, in a transaction of its own
	static void Update(IcebergRequest &request, const string &entity_type, const string &entity_uuid,
	                   const map<string, string> &updates, const vector<string> &removals);
};

} // namespace duckdb
