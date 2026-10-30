#include "iceberg/iceberg_properties.hpp"

#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

static bool PropertyTableExists(IcebergRequest &request) {
	auto &catalog = *request.catalog;
	return request.transaction->GetConnection().TableInfo(Identifier(catalog.MetadataDatabaseName()),
	                                                      catalog.MetadataSchemaName(),
	                                                      Identifier("ducklake_iceberg_property")) != nullptr;
}

map<string, string> IcebergProperties::Get(IcebergRequest &request, const string &entity_type,
                                           const string &entity_uuid) {
	map<string, string> result;
	if (!PropertyTableExists(request)) {
		return result;
	}
	auto rows = request.MetadataQuery(StringUtil::Format(
	    "SELECT key, value FROM {METADATA_CATALOG}.ducklake_iceberg_property WHERE entity_type = %s AND entity_uuid = "
	    "%s",
	    SQLString::ToString(entity_type), SQLString::ToString(entity_uuid)));
	for (auto &row : *rows) {
		result[row.GetValue<string>(0)] = row.IsNull(1) ? string() : row.GetValue<string>(1);
	}
	return result;
}

void IcebergProperties::Update(IcebergRequest &request, const string &entity_type, const string &entity_uuid,
                               const map<string, string> &updates, const vector<string> &removals) {
	if (updates.empty() && removals.empty()) {
		return;
	}
	IcebergRequest writer(request.server, request.db, request.catalog_name, request.prefix);
	if (!PropertyTableExists(writer)) {
		writer.MetadataQuery("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_iceberg_property(entity_type "
		                     "VARCHAR, entity_uuid VARCHAR, key VARCHAR, value VARCHAR)");
	}
	vector<string> keys = removals;
	for (auto &update : updates) {
		keys.push_back(update.first);
	}
	vector<string> key_literals;
	for (auto &key : keys) {
		key_literals.push_back(SQLString::ToString(key));
	}
	writer.MetadataQuery(StringUtil::Format(
	    "DELETE FROM {METADATA_CATALOG}.ducklake_iceberg_property WHERE entity_type = %s AND entity_uuid = %s AND key "
	    "IN (%s)",
	    SQLString::ToString(entity_type), SQLString::ToString(entity_uuid), StringUtil::Join(key_literals, ", ")));
	for (auto &update : updates) {
		writer.MetadataQuery(
		    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_iceberg_property VALUES (%s, %s, %s, %s)",
		                       SQLString::ToString(entity_type), SQLString::ToString(entity_uuid),
		                       SQLString::ToString(update.first), SQLString::ToString(update.second)));
	}
	writer.Commit();
}

} // namespace duckdb
