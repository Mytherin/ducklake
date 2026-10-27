#include "iceberg/iceberg_properties.hpp"

namespace duckdb {

map<string, string> IcebergProperties::GetTableProperties(IcebergRequest &request, const DuckLakeTableInfo &table) {
	return map<string, string>();
}

void IcebergProperties::UpdateTableProperties(IcebergRequest &request, const DuckLakeTableInfo &table,
                                              const map<string, string> &updates, const vector<string> &removals) {
}

void IcebergProperties::SetInitialTableProperties(IcebergRequest &request, const vector<string> &ns,
                                                  const string &name, const map<string, string> &properties) {
}

} // namespace duckdb
