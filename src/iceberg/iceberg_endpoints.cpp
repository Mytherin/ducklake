#include "iceberg/iceberg_server.hpp"

#include "common/ducklake_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg/iceberg_properties.hpp"
#include "iceberg/iceberg_table_export.hpp"
#include "iceberg/iceberg_table_update.hpp"
#include "iceberg/iceberg_types.hpp"
#include "iceberg/iceberg_util.hpp"
#include "iceberg/iceberg_views.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
static IcebergException NoSuchNamespace(const vector<string> &ns) {
	return IcebergException(404, "NoSuchNamespaceException",
	                        StringUtil::Format("Namespace does not exist: %s", IcebergUtil::NamespaceToString(ns)));
}

static IcebergException NoSuchTable(const vector<string> &ns, const string &name) {
	return IcebergException(
	    404, "NoSuchTableException",
	    StringUtil::Format("Table does not exist: %s.%s", IcebergUtil::NamespaceToString(ns), name));
}

static const DuckLakeSchemaInfo &GetNamespaceOrThrow(IcebergRequest &request, const vector<string> &ns) {
	auto schema = request.FindSchema(ns);
	if (!schema) {
		throw NoSuchNamespace(ns);
	}
	return *schema;
}

//! The schema of a table: a table of a namespace that does not exist does not exist either
static const DuckLakeSchemaInfo &GetSchemaOfTable(IcebergRequest &request, const vector<string> &ns,
                                                  const string &name) {
	auto schema = request.FindSchema(ns);
	if (!schema) {
		throw NoSuchTable(ns, name);
	}
	return *schema;
}

static void SetJson(IcebergHttpResponse &response, RESTJsonWriter &writer, JSONMutableValue root) {
	writer.SetRoot(root);
	response.body = writer.ToString();
	response.content_type = "application/json";
}

static bool IsHead(const IcebergHttpRequest &http) {
	return http.method == "HEAD";
}

//===--------------------------------------------------------------------===//
// Config / OAuth
//===--------------------------------------------------------------------===//
static const vector<string> &SupportedEndpoints() {
	static const vector<string> endpoints = {"GET /v1/{prefix}/namespaces",
	                                         "POST /v1/{prefix}/namespaces",
	                                         "GET /v1/{prefix}/namespaces/{namespace}",
	                                         "HEAD /v1/{prefix}/namespaces/{namespace}",
	                                         "DELETE /v1/{prefix}/namespaces/{namespace}",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/properties",
	                                         "GET /v1/{prefix}/namespaces/{namespace}/tables",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/tables",
	                                         "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	                                         "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	                                         "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/register",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
	                                         "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
	                                         "POST /v1/{prefix}/tables/rename",
	                                         "POST /v1/{prefix}/transactions/commit",
	                                         "GET /v1/{prefix}/namespaces/{namespace}/views",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/views",
	                                         "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
	                                         "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
	                                         "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
	                                         "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
	                                         "POST /v1/{prefix}/views/rename"};
	return endpoints;
}

static void GetConfig(DuckLakeIcebergServer &server, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto prefix = server.ResolveWarehouse(http.GetQuery("warehouse"));
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	root.Add("defaults", IcebergUtil::StringMap(w, server.ClientDefaults()));
	auto overrides = server.ClientOverrides();
	if (overrides.find("prefix") == overrides.end()) {
		overrides["prefix"] = prefix;
	}
	root.Add("overrides", IcebergUtil::StringMap(w, overrides));
	auto endpoints = w.CreateArray();
	for (auto &endpoint : SupportedEndpoints()) {
		endpoints.AppendString(endpoint);
	}
	root.Add("endpoints", endpoints);
	SetJson(response, writer, root);
}

static void OAuthToken(DuckLakeIcebergServer &server, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	// the form body of a client credentials grant: the token of the server is the secret of the client
	if (!server.Token().empty()) {
		bool valid = false;
		for (auto &param : StringUtil::Split(http.body, '&')) {
			auto equals = param.find('=');
			if (equals == string::npos) {
				continue;
			}
			auto key = param.substr(0, equals);
			auto value = param.substr(equals + 1);
			if (key == "client_secret" && value == server.Token()) {
				valid = true;
			}
		}
		if (!valid) {
			throw IcebergException(401, "NotAuthorizedException", "Invalid client credentials");
		}
	}
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	root.AddString("access_token", server.Token().empty() ? "ducklake" : server.Token());
	root.AddString("token_type", "bearer");
	root.AddString("issued_token_type", "urn:ietf:params:oauth:token-type:access_token");
	root.Add("expires_in", w.CreateSignedInteger(3600 * 24 * 365));
	SetJson(response, writer, root);
}

//===--------------------------------------------------------------------===//
// Namespaces
//===--------------------------------------------------------------------===//
static void ListNamespaces(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	vector<string> parent;
	if (http.HasQuery("parent") && !http.GetQuery("parent").empty()) {
		parent = IcebergUtil::ParseNamespace(http.GetQuery("parent"));
		GetNamespaceOrThrow(request, parent);
	}
	vector<vector<string>> children;
	for (auto &schema : request.GetCatalogInfo().schemas) {
		auto ns = request.GetNamespace(schema);
		if (ns.size() != parent.size() + 1) {
			continue;
		}
		if (!std::equal(parent.begin(), parent.end(), ns.begin())) {
			continue;
		}
		children.push_back(std::move(ns));
	}
	std::sort(children.begin(), children.end());
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	auto namespaces = w.CreateArray();
	for (auto &ns : children) {
		namespaces.Append(IcebergUtil::NamespaceToJson(w, ns));
	}
	root.Add("namespaces", namespaces);
	root.Add("next-page-token", w.CreateNull());
	SetJson(response, writer, root);
}

static void NamespaceResponse(IcebergHttpResponse &response, const vector<string> &ns,
                              const map<string, string> &properties) {
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	root.Add("namespace", IcebergUtil::NamespaceToJson(w, ns));
	root.Add("properties", IcebergUtil::StringMap(w, properties));
	SetJson(response, writer, root);
}

static map<string, string> GetNamespaceProperties(IcebergRequest &request, const DuckLakeSchemaInfo &schema) {
	auto result = IcebergProperties::Get(request, IcebergProperties::NAMESPACE, schema.uuid);
	result["location"] = IcebergUtil::StripTrailingSlash(schema.path);
	return result;
}

static void CreateNamespace(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto ns = IcebergUtil::NamespaceFromJson(root.Get("namespace"));
	if (ns.empty()) {
		throw IcebergException(400, "BadRequestException", "The namespace cannot be empty");
	}
	if (request.FindSchema(ns)) {
		throw IcebergException(409, "AlreadyExistsException",
		                       StringUtil::Format("Namespace already exists: %s", IcebergUtil::NamespaceToString(ns)));
	}
	if (ns.size() > 1) {
		auto parent = ns;
		parent.pop_back();
		GetNamespaceOrThrow(request, parent);
	}
	request.Query("CREATE SCHEMA " + request.SchemaSQL(ns));
	request.Commit();
	{
		IcebergRequest lookup(request.server, request.db, request.catalog_name, request.prefix);
		auto created = lookup.FindSchema(ns);
		if (!created) {
			throw NoSuchNamespace(ns);
		}
		auto properties = IcebergUtil::ReadStringMap(root.TryGet("properties"));
		properties.erase("location");
		IcebergProperties::Update(lookup, IcebergProperties::NAMESPACE, created->uuid, properties, vector<string>());
		lookup.connection.Rollback();
	}
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	auto created = fresh.FindSchema(ns);
	if (!created) {
		throw NoSuchNamespace(ns);
	}
	NamespaceResponse(response, ns, GetNamespaceProperties(fresh, *created));
	fresh.connection.Rollback();
}

static void GetNamespace(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetNamespaceOrThrow(request, ns);
	if (IsHead(http)) {
		response.status = 204;
		return;
	}
	NamespaceResponse(response, ns, GetNamespaceProperties(request, schema));
}

static void DropNamespace(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetNamespaceOrThrow(request, ns);
	auto &info = request.GetCatalogInfo();
	bool empty = true;
	for (auto &table : info.tables) {
		empty = empty && table.schema_id != schema.id;
	}
	for (auto &view : info.views) {
		empty = empty && view.schema_id != schema.id;
	}
	for (auto &other : info.schemas) {
		empty = empty && !(other.parent_id.IsValid() && other.parent_id == schema.id);
	}
	if (!empty) {
		throw IcebergException(409, "NamespaceNotEmptyException",
		                       StringUtil::Format("Namespace %s is not empty", IcebergUtil::NamespaceToString(ns)));
	}
	auto uuid = schema.uuid;
	request.Query("DROP SCHEMA " + request.SchemaSQL(ns));
	request.Commit();
	// the properties of the namespace go with it
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	vector<string> keys;
	for (auto &property : IcebergProperties::Get(fresh, IcebergProperties::NAMESPACE, uuid)) {
		keys.push_back(property.first);
	}
	IcebergProperties::Update(fresh, IcebergProperties::NAMESPACE, uuid, map<string, string>(), keys);
	fresh.connection.Rollback();
	response.status = 204;
}

static void UpdateNamespaceProperties(IcebergRequest &request, const IcebergHttpRequest &http,
                                      IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetNamespaceOrThrow(request, ns);
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto current = GetNamespaceProperties(request, schema);
	auto updates = IcebergUtil::ReadStringMap(root.TryGet("updates"));
	if (updates.find("location") != updates.end() && updates["location"] != current["location"]) {
		throw IcebergException(400, "BadRequestException", "The location of a namespace cannot be changed");
	}
	updates.erase("location");
	vector<string> removals;
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto result = w.CreateObject();
	auto updated = w.CreateArray();
	for (auto &update : updates) {
		updated.AppendString(update.first);
	}
	auto removed = w.CreateArray();
	auto missing = w.CreateArray();
	auto removal_list = root.TryGet("removals");
	if (removal_list.IsArray()) {
		for (auto &removal : removal_list.Items()) {
			auto key = removal.Str();
			if (current.find(key) != current.end() && key != "location") {
				removed.AppendString(key);
				removals.push_back(key);
			} else {
				missing.AppendString(key);
			}
		}
	}
	IcebergProperties::Update(request, IcebergProperties::NAMESPACE, schema.uuid, updates, removals);
	result.Add("updated", updated);
	result.Add("removed", removed);
	result.Add("missing", missing);
	SetJson(response, writer, result);
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//
static void ListTables(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &schema = GetNamespaceOrThrow(request, ns);
	vector<string> names;
	for (auto &table : request.GetCatalogInfo().tables) {
		if (table.schema_id == schema.id) {
			names.push_back(table.name);
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
	SetJson(response, writer, root);
}

static void LoadTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOfTable(request, ns, name);
	auto table = request.FindTable(schema, name);
	if (!table) {
		throw NoSuchTable(ns, name);
	}
	if (IsHead(http)) {
		response.status = 204;
		return;
	}
	IcebergTableExporter exporter(request);
	auto load = exporter.Export(schema, *table, http.GetQuery("snapshots", "all"));
	response.body = IcebergLoadTableResponse(load, true, request.server.TableConfig());
}

static string PlanResponse(const string &plan_id, const string &plan) {
	// the plan is a JSON object with the tasks: the status and the id of the plan are added to it
	return "{\"status\": \"completed\", \"plan-id\": " + IcebergUtil::JsonString(plan_id) + ", " +
	       plan.substr(plan.find('{') + 1);
}

static void PlanTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOfTable(request, ns, name);
	auto table = request.FindTable(schema, name);
	if (!table) {
		throw NoSuchTable(ns, name);
	}
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	optional_idx snapshot_id;
	if (root.Has("snapshot-id") && !root.TryGet("snapshot-id").IsNull()) {
		snapshot_id = root.UInt("snapshot-id");
	}
	if (root.Has("start-snapshot-id") && !root.TryGet("start-snapshot-id").IsNull()) {
		throw IcebergException(501, "NotImplementedException", "Incremental scans are not supported");
	}
	IcebergTableExporter exporter(request);
	auto plan = exporter.PlanScan(schema, *table, snapshot_id);
	auto plan_id = IcebergUtil::RandomUUID();
	request.server.GetExportCache().PutPlan(plan_id, plan);
	response.body = PlanResponse(plan_id, plan);
}

static void FetchPlan(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	string plan;
	if (!request.server.GetExportCache().GetPlan(http.path_values[2], plan)) {
		throw IcebergException(404, "NoSuchPlanIdException",
		                       StringUtil::Format("The plan %s does not exist", http.path_values[2]));
	}
	response.body = PlanResponse(http.path_values[2], plan);
}

static void CancelPlan(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	request.server.GetExportCache().RemovePlan(http.path_values[2]);
	response.status = 204;
}

static void FetchScanTasks(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	// the plans of this server are complete and have no plan tasks that could be fetched
	throw IcebergException(404, "NoSuchPlanTaskException", "The plan task does not exist");
}

static void DropTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOfTable(request, ns, name);
	auto table = request.FindTable(schema, name);
	if (!table) {
		throw NoSuchTable(ns, name);
	}
	request.Query("DROP TABLE " + request.EntrySQL(ns, name));
	request.InvalidateCatalogInfo();
	response.status = 204;
}

static void RenameEntry(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response,
                        bool view) {
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto source = root.Get("source");
	auto destination = root.Get("destination");
	auto source_ns = IcebergUtil::NamespaceFromJson(source.Get("namespace"));
	auto source_name = source.Str("name");
	auto destination_ns = IcebergUtil::NamespaceFromJson(destination.Get("namespace"));
	auto destination_name = destination.Str("name");
	auto &source_schema = GetNamespaceOrThrow(request, source_ns);
	auto &destination_schema = GetNamespaceOrThrow(request, destination_ns);
	bool exists = view ? request.FindView(source_schema, source_name) != nullptr
	                   : request.FindTable(source_schema, source_name) != nullptr;
	if (!exists) {
		if (view) {
			throw IcebergException(404, "NoSuchViewException",
			                       StringUtil::Format("View does not exist: %s", source_name));
		}
		throw NoSuchTable(source_ns, source_name);
	}
	if (request.FindTable(destination_schema, destination_name) ||
	    request.FindView(destination_schema, destination_name)) {
		throw IcebergException(409, "AlreadyExistsException",
		                       StringUtil::Format("Table or view already exists: %s", destination_name));
	}
	if (source_schema.id != destination_schema.id) {
		throw IcebergException(501, "NotImplementedException",
		                       "DuckLake cannot move a table or view to another namespace");
	}
	request.Query(StringUtil::Format("ALTER %s %s RENAME TO %s", view ? "VIEW" : "TABLE",
	                                 request.EntrySQL(source_ns, source_name),
	                                 KeywordHelper::WriteQuoted(destination_name, '"')));
	request.InvalidateCatalogInfo();
	response.status = 204;
}

static void RenameTable(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	RenameEntry(request, http, response, false);
}

static void RenameView(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	RenameEntry(request, http, response, true);
}

static void ReportMetrics(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	response.status = 204;
}

static void GetCredentials(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto &name = http.path_values[1];
	auto &schema = GetSchemaOfTable(request, ns, name);
	auto table = request.FindTable(schema, name);
	if (!table) {
		throw NoSuchTable(ns, name);
	}
	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto root = w.CreateObject();
	auto credentials = w.CreateArray();
	auto &config = request.server.TableConfig();
	if (!config.empty()) {
		auto entry = w.CreateObject();
		entry.AddString("prefix", IcebergUtil::StripTrailingSlash(table->path));
		entry.Add("config", IcebergUtil::StringMap(w, config));
		credentials.Append(entry);
	}
	root.Add("storage-credentials", credentials);
	SetJson(response, writer, root);
}

//===--------------------------------------------------------------------===//
// Routes
//===--------------------------------------------------------------------===//
const vector<IcebergRoute> &IcebergEndpoints::GetRoutes() {
	static const vector<IcebergRoute> routes = [] {
		vector<IcebergRoute> result;
		auto global = [&](const string &method, const string &path, iceberg_global_handler_t handler) {
			IcebergRoute route;
			route.method = method;
			route.path = path;
			route.global_handler = std::move(handler);
			result.push_back(std::move(route));
		};
		auto add = [&](const string &method, const string &path, iceberg_handler_t handler) {
			IcebergRoute route;
			route.method = method;
			route.path = path;
			route.handler = std::move(handler);
			result.push_back(std::move(route));
		};
		global("GET", "/config", GetConfig);
		global("POST", "/oauth/tokens", OAuthToken);
		add("GET", "/namespaces", ListNamespaces);
		add("POST", "/namespaces", CreateNamespace);
		add("GET", "/namespaces/{namespace}", GetNamespace);
		add("DELETE", "/namespaces/{namespace}", DropNamespace);
		add("POST", "/namespaces/{namespace}/properties", UpdateNamespaceProperties);
		add("GET", "/namespaces/{namespace}/tables", ListTables);
		add("POST", "/namespaces/{namespace}/tables", IcebergTableEndpoints::CreateTable);
		add("GET", "/namespaces/{namespace}/tables/{table}", LoadTable);
		add("POST", "/namespaces/{namespace}/tables/{table}", IcebergTableEndpoints::UpdateTable);
		add("DELETE", "/namespaces/{namespace}/tables/{table}", DropTable);
		add("POST", "/namespaces/{namespace}/register", IcebergTableEndpoints::RegisterTable);
		add("POST", "/namespaces/{namespace}/tables/{table}/plan", PlanTable);
		add("GET", "/namespaces/{namespace}/tables/{table}/plan/{plan-id}", FetchPlan);
		add("DELETE", "/namespaces/{namespace}/tables/{table}/plan/{plan-id}", CancelPlan);
		add("POST", "/namespaces/{namespace}/tables/{table}/tasks", FetchScanTasks);
		add("POST", "/namespaces/{namespace}/tables/{table}/metrics", ReportMetrics);
		add("GET", "/namespaces/{namespace}/tables/{table}/credentials", GetCredentials);
		add("POST", "/tables/rename", RenameTable);
		add("POST", "/transactions/commit", IcebergTableEndpoints::CommitTransaction);
		add("GET", "/namespaces/{namespace}/views", IcebergViewEndpoints::ListViews);
		add("POST", "/namespaces/{namespace}/views", IcebergViewEndpoints::CreateView);
		add("GET", "/namespaces/{namespace}/views/{view}", IcebergViewEndpoints::LoadView);
		add("POST", "/namespaces/{namespace}/views/{view}", IcebergViewEndpoints::ReplaceView);
		add("DELETE", "/namespaces/{namespace}/views/{view}", IcebergViewEndpoints::DropView);
		add("POST", "/views/rename", RenameView);
		return result;
	}();
	return routes;
}

} // namespace duckdb
