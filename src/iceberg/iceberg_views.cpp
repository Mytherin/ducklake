#include "iceberg/iceberg_views.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg/iceberg_properties.hpp"
#include "iceberg/iceberg_types.hpp"
#include "iceberg/iceberg_util.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

namespace {

IcebergException NoSuchView(const vector<string> &ns, const string &name) {
	return IcebergException(404, "NoSuchViewException",
	                        StringUtil::Format("View does not exist: %s.%s", IcebergUtil::NamespaceToString(ns), name));
}

const DuckLakeSchemaInfo &GetSchemaOrThrow(IcebergRequest &request, const vector<string> &ns) {
	auto schema = request.FindSchema(ns);
	if (!schema) {
		throw IcebergException(404, "NoSuchNamespaceException",
		                       StringUtil::Format("Namespace does not exist: %s", IcebergUtil::NamespaceToString(ns)));
	}
	return *schema;
}

//! The Iceberg field of a column type, the ids are assigned in the order of the fields
IcebergField FieldFromLogicalType(const string &name, const LogicalType &type, idx_t &next_id) {
	IcebergField field;
	field.id = next_id++;
	field.name = name;
	field.required = false;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		field.type.kind = IcebergType::Kind::STRUCT;
		for (auto &child : StructType::GetChildTypes(type)) {
			field.type.fields.push_back(FieldFromLogicalType(child.first.GetIdentifierName(), child.second, next_id));
		}
		break;
	case LogicalTypeId::LIST:
		field.type.kind = IcebergType::Kind::LIST;
		field.type.fields.push_back(FieldFromLogicalType("element", ListType::GetChildType(type), next_id));
		break;
	case LogicalTypeId::MAP: {
		field.type.kind = IcebergType::Kind::MAP;
		field.type.fields.push_back(FieldFromLogicalType("key", MapType::KeyType(type), next_id));
		field.type.fields[0].required = true;
		field.type.fields.push_back(FieldFromLogicalType("value", MapType::ValueType(type), next_id));
		break;
	}
	default: {
		DuckLakeColumnInfo column;
		column.name = name;
		column.id = FieldIndex(field.id);
		column.nulls_allowed = true;
		column.type = type.ToString();
		// the DuckLake names of the types are lowercase variants of the SQL names, so convert through the names
		static const vector<pair<LogicalTypeId, string>> NAMES = {{LogicalTypeId::BOOLEAN, "boolean"},
		                                                          {LogicalTypeId::TINYINT, "int8"},
		                                                          {LogicalTypeId::SMALLINT, "int16"},
		                                                          {LogicalTypeId::INTEGER, "int32"},
		                                                          {LogicalTypeId::BIGINT, "int64"},
		                                                          {LogicalTypeId::UTINYINT, "uint8"},
		                                                          {LogicalTypeId::USMALLINT, "uint16"},
		                                                          {LogicalTypeId::UINTEGER, "uint32"},
		                                                          {LogicalTypeId::FLOAT, "float32"},
		                                                          {LogicalTypeId::DOUBLE, "float64"},
		                                                          {LogicalTypeId::DATE, "date"},
		                                                          {LogicalTypeId::TIME, "time"},
		                                                          {LogicalTypeId::TIMESTAMP, "timestamp"},
		                                                          {LogicalTypeId::TIMESTAMP_TZ, "timestamptz"},
		                                                          {LogicalTypeId::TIMESTAMP_NS, "timestamp_ns"},
		                                                          {LogicalTypeId::VARCHAR, "varchar"},
		                                                          {LogicalTypeId::UUID, "uuid"},
		                                                          {LogicalTypeId::BLOB, "blob"}};
		bool found = false;
		for (auto &entry : NAMES) {
			if (entry.first == type.id()) {
				column.type = entry.second;
				found = true;
			}
		}
		if (type.id() == LogicalTypeId::DECIMAL) {
			column.type =
			    StringUtil::Format("decimal(%d,%d)", DecimalType::GetWidth(type), DecimalType::GetScale(type));
			found = true;
		}
		if (!found) {
			throw NotImplementedException("The type %s of the column \"%s\" cannot be exposed through Iceberg",
			                              type.ToString(), name);
		}
		auto converted = IcebergTypes::FromColumn(column, false);
		converted.id = field.id;
		return converted;
	}
	}
	return field;
}

string ViewMetadata(IcebergRequest &request, const vector<string> &ns, const DuckLakeSchemaInfo &schema,
                    const DuckLakeViewInfo &view, string &metadata_location) {
	// the columns of the view
	auto result = request.Query("SELECT * FROM " + request.EntrySQL(ns, view.name) + " LIMIT 0");
	IcebergSchema iceberg_schema;
	iceberg_schema.schema_id = 0;
	idx_t next_id = 1;
	auto &names = result->GetNames();
	auto &types = result->GetTypes();
	for (idx_t i = 0; i < names.size(); i++) {
		iceberg_schema.fields.push_back(FieldFromLogicalType(names[i].GetIdentifierName(), types[i], next_id));
	}
	auto time_ms = IcebergUtil::NowMs();
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto metadata = w.CreateObject();
	metadata.AddString("view-uuid", view.uuid);
	metadata.Add("format-version", w.CreateSignedInteger(1));
	auto location = IcebergUtil::JoinPath(IcebergUtil::StripTrailingSlash(schema.path), "views/" + view.name);
	metadata.AddString("location", location);
	auto schemas = w.CreateArray();
	schemas.Append(IcebergTypes::SchemaToJson(w, iceberg_schema));
	metadata.Add("schemas", schemas);
	metadata.Add("current-version-id", w.CreateSignedInteger(1));
	auto versions = w.CreateArray();
	auto version = w.CreateObject();
	version.Add("version-id", w.CreateSignedInteger(1));
	version.Add("timestamp-ms", w.CreateSignedInteger(time_ms));
	version.Add("schema-id", w.CreateSignedInteger(0));
	auto summary = w.CreateObject();
	summary.AddString("engine-name", "ducklake");
	version.Add("summary", summary);
	auto representations = w.CreateArray();
	auto representation = w.CreateObject();
	representation.AddString("type", "sql");
	representation.AddString("sql", view.sql);
	representation.AddString("dialect", view.dialect.empty() ? "duckdb" : view.dialect);
	representations.Append(representation);
	version.Add("representations", representations);
	version.Add("default-namespace", IcebergUtil::NamespaceToJson(w, ns));
	versions.Append(version);
	metadata.Add("versions", versions);
	auto version_log = w.CreateArray();
	auto log_entry = w.CreateObject();
	log_entry.Add("timestamp-ms", w.CreateSignedInteger(time_ms));
	log_entry.Add("version-id", w.CreateSignedInteger(1));
	version_log.Append(log_entry);
	metadata.Add("version-log", version_log);
	metadata.Add("properties",
	             IcebergUtil::StringMap(w, IcebergProperties::Get(request, IcebergProperties::VIEW, view.uuid)));
	writer.SetRoot(metadata);
	auto text = writer.ToString();
	auto &server = request.server;
	metadata_location = IcebergUtil::JoinPath(
	    IcebergUtil::JoinPath(IcebergUtil::JoinPath(server.MetadataPath(), view.uuid), "metadata"),
	    StringUtil::Format("%016llx.metadata.json", std::hash<string>()(text)));
	if (!IcebergUtil::FileExists(*request.connection.context, metadata_location)) {
		IcebergUtil::WriteFile(*request.connection.context, metadata_location, text);
	}
	return text;
}

string ViewResponse(IcebergRequest &request, const vector<string> &ns, const DuckLakeSchemaInfo &schema,
                    const DuckLakeViewInfo &view) {
	string location;
	auto metadata = ViewMetadata(request, ns, schema, view, location);
	return "{\"metadata-location\": " + IcebergUtil::JsonString(location) + ", \"metadata\": " + metadata +
	       ", \"config\": {}}";
}

//! The SQL of the representation of a view version that DuckDB can run
string GetViewSQL(RESTJsonIn version, string &dialect) {
	string result;
	for (auto &representation : version.Get("representations").Items()) {
		if (representation.Str("type", "sql") != "sql") {
			continue;
		}
		auto candidate_dialect = representation.Str("dialect", "");
		if (result.empty() || StringUtil::CIEquals(candidate_dialect, "duckdb")) {
			result = representation.Str("sql");
			dialect = candidate_dialect;
		}
	}
	if (result.empty()) {
		throw IcebergException(400, "BadRequestException", "The view version has no SQL representation");
	}
	return result;
}

} // namespace

void IcebergViewEndpoints::ListViews(IcebergRequest &request, const IcebergHttpRequest &http,
                                     IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetSchemaOrThrow(request, ns);
	vector<string> names;
	for (auto &view : request.GetCatalogInfo().views) {
		if (view.schema_id == schema.id) {
			names.push_back(view.name);
		}
	}
	std::sort(names.begin(), names.end());
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	auto identifiers = w.CreateArray();
	for (auto &name : names) {
		auto identifier = w.CreateObject();
		identifier.Add("namespace", IcebergUtil::NamespaceToJson(w, ns));
		identifier.AddString("name", name);
		identifiers.Append(identifier);
	}
	root.Add("identifiers", identifiers);
	root.Add("next-page-token", w.CreateNull());
	writer.SetRoot(root);
	response.body = writer.ToString();
}

void IcebergViewEndpoints::LoadView(IcebergRequest &request, const IcebergHttpRequest &http,
                                    IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOrThrow(request, ns);
	auto view = request.FindView(schema, name);
	if (!view) {
		throw NoSuchView(ns, name);
	}
	if (http.method == "HEAD") {
		response.status = 204;
		return;
	}
	response.body = ViewResponse(request, ns, schema, *view);
}

void IcebergViewEndpoints::CreateView(IcebergRequest &request, const IcebergHttpRequest &http,
                                      IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetSchemaOrThrow(request, ns);
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto name = root.Str("name");
	if (request.FindView(schema, name) || request.FindTable(schema, name)) {
		throw IcebergException(409, "AlreadyExistsException",
		                       StringUtil::Format("View or table already exists: %s", name));
	}
	string dialect;
	auto sql = GetViewSQL(root.Get("view-version"), dialect);
	request.Query(StringUtil::Format("CREATE VIEW %s AS %s", request.EntrySQL(ns, name), sql));
	request.Commit();
	{
		IcebergRequest lookup(request.server, request.db, request.catalog_name, request.prefix);
		auto &lookup_schema = GetSchemaOrThrow(lookup, ns);
		auto view = lookup.FindView(lookup_schema, name);
		if (!view) {
			throw NoSuchView(ns, name);
		}
		IcebergProperties::Update(lookup, IcebergProperties::VIEW, view->uuid,
		                          IcebergUtil::ReadStringMap(root.TryGet("properties")), vector<string>());
		lookup.connection.Rollback();
	}
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	auto &fresh_schema = GetSchemaOrThrow(fresh, ns);
	auto view = fresh.FindView(fresh_schema, name);
	if (!view) {
		throw NoSuchView(ns, name);
	}
	response.body = ViewResponse(fresh, ns, fresh_schema, *view);
	fresh.connection.Rollback();
}

void IcebergViewEndpoints::ReplaceView(IcebergRequest &request, const IcebergHttpRequest &http,
                                       IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto name = http.path_values[1];
	auto &schema = GetSchemaOrThrow(request, ns);
	auto view = request.FindView(schema, name);
	if (!view) {
		throw NoSuchView(ns, name);
	}
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	for (auto &requirement :
	     root.TryGet("requirements").IsArray() ? root.Get("requirements").Items() : vector<RESTJsonIn>()) {
		if (requirement.Str("type") == "assert-view-uuid" &&
		    !StringUtil::CIEquals(requirement.Str("uuid"), view->uuid)) {
			throw IcebergException(409, "CommitFailedException", "Requirement failed: view UUID does not match");
		}
	}
	auto updates = root.TryGet("updates");
	map<string, string> property_updates;
	vector<string> property_removals;
	if (updates.IsArray()) {
		for (auto &update : updates.Items()) {
			auto action = update.Str("action");
			if (action == "set-properties") {
				for (auto &entry : IcebergUtil::ReadStringMap(update.Get("updates"))) {
					property_updates[entry.first] = entry.second;
				}
			} else if (action == "remove-properties") {
				for (auto &removal : update.Get("removals").Items()) {
					property_removals.push_back(removal.Str());
				}
			}
		}
		map<int64_t, RESTJsonIn> versions;
		optional_idx current_version;
		int64_t last_added = -1;
		int64_t current = -2;
		for (auto &update : updates.Items()) {
			auto action = update.Str("action");
			if (action == "add-view-version") {
				auto version = update.Get("view-version");
				last_added = version.Int("version-id");
				versions.emplace(last_added, version);
			} else if (action == "set-current-view-version") {
				current = update.Int("view-version-id");
				if (current == -1) {
					current = last_added;
				}
			}
		}
		if (current != -2) {
			auto entry = versions.find(current);
			if (entry != versions.end()) {
				string dialect;
				auto sql = GetViewSQL(entry->second, dialect);
				request.Query(StringUtil::Format("CREATE OR REPLACE VIEW %s AS %s", request.EntrySQL(ns, name), sql));
			}
		}
	}
	request.Commit();
	{
		IcebergRequest lookup(request.server, request.db, request.catalog_name, request.prefix);
		auto &lookup_schema = GetSchemaOrThrow(lookup, ns);
		auto updated = lookup.FindView(lookup_schema, name);
		if (!updated) {
			throw NoSuchView(ns, name);
		}
		IcebergProperties::Update(lookup, IcebergProperties::VIEW, updated->uuid, property_updates, property_removals);
		lookup.connection.Rollback();
	}
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	auto &fresh_schema = GetSchemaOrThrow(fresh, ns);
	auto updated = fresh.FindView(fresh_schema, name);
	if (!updated) {
		throw NoSuchView(ns, name);
	}
	response.body = ViewResponse(fresh, ns, fresh_schema, *updated);
	fresh.connection.Rollback();
}

void IcebergViewEndpoints::DropView(IcebergRequest &request, const IcebergHttpRequest &http,
                                    IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOrThrow(request, ns);
	if (!request.FindView(schema, name)) {
		throw NoSuchView(ns, name);
	}
	request.Query("DROP VIEW " + request.EntrySQL(ns, name));
	request.InvalidateCatalogInfo();
	response.status = 204;
}

} // namespace duckdb
