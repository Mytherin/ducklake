#include "iceberg/iceberg_server.hpp"
#include "iceberg/iceberg_table_export.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "ducklake_extension.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "rest/ducklake_rest_json.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

#include "httplib.hpp"

namespace duckdb {

string IcebergHttpRequest::GetQuery(const string &name, const string &default_value) const {
	for (auto &entry : query) {
		if (entry.first == name) {
			return entry.second;
		}
	}
	return default_value;
}

bool IcebergHttpRequest::HasQuery(const string &name) const {
	for (auto &entry : query) {
		if (entry.first == name) {
			return true;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Request
//===--------------------------------------------------------------------===//
IcebergRequest::IcebergRequest(DuckLakeIcebergServer &server_p, DatabaseInstance &db_p, const string &catalog_name_p,
                               const string &prefix_p)
    : server(server_p), db(db_p), connection(db_p), prefix(prefix_p), catalog_name(catalog_name_p) {
	connection.BeginTransaction();
	auto &context = *connection.context;
	auto &entry = Catalog::GetCatalog(context, Identifier(catalog_name));
	if (entry.GetCatalogType() != "ducklake") {
		throw InvalidInputException("Catalog \"%s\" is not a DuckLake catalog", catalog_name);
	}
	catalog = &entry.Cast<DuckLakeCatalog>();
	transaction = &DuckLakeTransaction::Get(context, *catalog);
}

DuckLakeMetadataManager &IcebergRequest::GetMetadataManager() {
	return transaction->GetMetadataManager();
}

DuckLakeSnapshot IcebergRequest::GetSnapshot() {
	if (!snapshot) {
		snapshot = make_uniq<DuckLakeSnapshot>(transaction->GetSnapshot());
	}
	return *snapshot;
}

const DuckLakeCatalogInfo &IcebergRequest::GetCatalogInfo() {
	if (!catalog_info) {
		catalog_info = make_uniq<DuckLakeCatalogInfo>(GetMetadataManager().GetCatalogForSnapshot(GetSnapshot()));
	}
	return *catalog_info;
}

void IcebergRequest::InvalidateCatalogInfo() {
	catalog_info.reset();
}

unique_ptr<QueryResult> IcebergRequest::Query(const string &query) {
	auto result = connection.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw();
	}
	return std::move(result);
}

unique_ptr<QueryResult> IcebergRequest::MetadataQuery(string query) {
	auto result = GetMetadataManager().Query(GetSnapshot(), query);
	if (result->HasError()) {
		result->GetErrorObject().Throw();
	}
	return result;
}

void IcebergRequest::Commit() {
	connection.Commit();
}

optional_ptr<const DuckLakeSchemaInfo> IcebergRequest::GetSchemaById(SchemaIndex id) {
	for (auto &schema : GetCatalogInfo().schemas) {
		if (schema.id == id) {
			return schema;
		}
	}
	return nullptr;
}

vector<string> IcebergRequest::GetNamespace(const DuckLakeSchemaInfo &schema) {
	vector<string> result;
	optional_ptr<const DuckLakeSchemaInfo> current = schema;
	idx_t depth = 0;
	while (current && depth++ < 64) {
		result.insert(result.begin(), current->name);
		current = current->parent_id.IsValid() ? GetSchemaById(current->parent_id) : nullptr;
	}
	return result;
}

optional_ptr<const DuckLakeSchemaInfo> IcebergRequest::FindSchema(const vector<string> &ns) {
	if (ns.empty()) {
		return nullptr;
	}
	for (auto &schema : GetCatalogInfo().schemas) {
		if (GetNamespace(schema) == ns) {
			return schema;
		}
	}
	return nullptr;
}

optional_ptr<const DuckLakeTableInfo> IcebergRequest::FindTable(const DuckLakeSchemaInfo &schema, const string &name) {
	for (auto &table : GetCatalogInfo().tables) {
		if (table.schema_id == schema.id && table.name == name) {
			return table;
		}
	}
	return nullptr;
}

optional_ptr<const DuckLakeViewInfo> IcebergRequest::FindView(const DuckLakeSchemaInfo &schema, const string &name) {
	for (auto &view : GetCatalogInfo().views) {
		if (view.schema_id == schema.id && view.name == name) {
			return view;
		}
	}
	return nullptr;
}

string IcebergRequest::SchemaSQL(const vector<string> &ns) {
	string result = KeywordHelper::WriteOptionallyQuoted(catalog_name, '"', true);
	for (auto &part : ns) {
		result += "." + KeywordHelper::WriteQuoted(part, '"');
	}
	return result;
}

string IcebergRequest::EntrySQL(const vector<string> &ns, const string &name) {
	return SchemaSQL(ns) + "." + KeywordHelper::WriteQuoted(name, '"');
}

//===--------------------------------------------------------------------===//
// Server
//===--------------------------------------------------------------------===//
DuckLakeIcebergServer::DuckLakeIcebergServer() : export_cache(make_uniq<IcebergExportCache>()) {
}

IcebergExportCache &DuckLakeIcebergServer::GetExportCache() {
	return *export_cache;
}

DuckLakeIcebergServer::~DuckLakeIcebergServer() {
	Stop();
}

void DuckLakeIcebergServer::AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name) {
	lock_guard<mutex> guard(catalog_lock);
	catalogs[prefix] = ServedCatalog {prefix, std::move(db), std::move(catalog_name)};
}

bool DuckLakeIcebergServer::RemoveCatalog(const string &prefix) {
	lock_guard<mutex> guard(catalog_lock);
	return catalogs.erase(prefix) > 0;
}

void DuckLakeIcebergServer::SetClientProperties(map<string, string> defaults, map<string, string> overrides) {
	client_defaults = std::move(defaults);
	client_overrides = std::move(overrides);
}

vector<string> DuckLakeIcebergServer::GetPrefixes() {
	lock_guard<mutex> guard(catalog_lock);
	vector<string> result;
	for (auto &entry : catalogs) {
		result.push_back(entry.first);
	}
	return result;
}

string DuckLakeIcebergServer::ResolveWarehouse(const string &warehouse) {
	lock_guard<mutex> guard(catalog_lock);
	if (!warehouse.empty()) {
		auto entry = catalogs.find(warehouse);
		if (entry != catalogs.end()) {
			return entry->first;
		}
		for (auto &catalog : catalogs) {
			if (catalog.second.catalog_name == warehouse) {
				return catalog.first;
			}
		}
	}
	if (catalogs.size() == 1) {
		return catalogs.begin()->first;
	}
	if (catalogs.empty()) {
		throw IcebergException(404, "NoSuchWarehouseException", "The server serves no catalogs");
	}
	throw IcebergException(
	    404, "NoSuchWarehouseException",
	    StringUtil::Format("The server serves %llu catalogs: the warehouse has to be one of them", catalogs.size()));
}

DuckLakeIcebergServer::ServedCatalog DuckLakeIcebergServer::GetCatalog(optional_ptr<const string> prefix) {
	lock_guard<mutex> guard(catalog_lock);
	if (prefix) {
		auto entry = catalogs.find(*prefix);
		if (entry == catalogs.end()) {
			throw IcebergException(404, "NoSuchWarehouseException",
			                       StringUtil::Format("The server serves no catalog under the prefix \"%s\"", *prefix));
		}
		return entry->second;
	}
	if (catalogs.size() != 1) {
		throw IcebergException(404, "NoSuchWarehouseException",
		                       StringUtil::Format("The server serves %llu catalogs: the URLs need the prefix of one "
		                                          "of them (/v1/<prefix>/...)",
		                                          catalogs.size()));
	}
	return catalogs.begin()->second;
}

mutex &DuckLakeIcebergServer::TableLock(const string &table_uuid) {
	lock_guard<mutex> guard(table_lock_lock);
	auto &entry = table_locks[table_uuid];
	if (!entry) {
		entry = make_uniq<mutex>();
	}
	return *entry;
}

static string ErrorBody(const string &type, const string &message, int32_t status) {
	RESTJsonWriter writer;
	auto root = writer.writer.CreateObject();
	auto error = writer.writer.CreateObject();
	error.AddString("message", message);
	error.AddString("type", type);
	error.Add("code", writer.writer.CreateSignedInteger(status));
	root.Add("error", error);
	writer.SetRoot(root);
	return writer.ToString();
}

static void ErrorFromException(const ErrorData &error, int32_t &status, string &type) {
	auto &message = error.RawMessage();
	switch (error.Type()) {
	case ExceptionType::TRANSACTION:
		status = 409;
		type = "CommitFailedException";
		break;
	case ExceptionType::CATALOG:
		if (message.find("already exists") != string::npos) {
			status = 409;
			type = "AlreadyExistsException";
		} else {
			status = 404;
			type = "NoSuchTableException";
		}
		break;
	case ExceptionType::INVALID_INPUT:
	case ExceptionType::BINDER:
	case ExceptionType::PARSER:
	case ExceptionType::CONVERSION:
	case ExceptionType::CONSTRAINT:
	case ExceptionType::INVALID_CONFIGURATION:
		status = 400;
		type = "BadRequestException";
		break;
	case ExceptionType::NOT_IMPLEMENTED:
		status = 501;
		type = "NotImplementedException";
		break;
	case ExceptionType::PERMISSION:
		status = 403;
		type = "ForbiddenException";
		break;
	default:
		status = 500;
		type = "RuntimeException";
		break;
	}
}

int32_t DuckLakeIcebergServer::HandleRequest(const IcebergRoute &route, optional_ptr<const string> prefix,
                                             const IcebergHttpRequest &http, IcebergHttpResponse &response) {
	try {
		if (route.global_handler) {
			route.global_handler(*this, http, response);
			return response.status;
		}
		auto served = GetCatalog(prefix);
		IcebergRequest request(*this, *served.db, served.catalog_name, served.prefix);
		try {
			route.handler(request, http, response);
			if (request.connection.HasActiveTransaction()) {
				request.Commit();
			}
		} catch (...) {
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
		return response.status;
	} catch (IcebergException &ex) {
		response.body = ErrorBody(ex.type, ex.message, ex.status);
		response.content_type = "application/json";
		response.status = ex.status;
		return ex.status;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		int32_t status;
		string type;
		ErrorFromException(error, status, type);
		response.body = ErrorBody(type, error.RawMessage(), status);
		response.content_type = "application/json";
		response.status = status;
		return status;
	}
}

idx_t DuckLakeIcebergServer::Start(const string &host, idx_t port, idx_t thread_count) {
	server = make_uniq<duckdb_httplib::Server>();
	if (thread_count > 0) {
		server->new_task_queue = [thread_count]() {
			return new duckdb_httplib::ThreadPool(thread_count);
		};
	}
	for (auto &route : IcebergEndpoints::GetRoutes()) {
		for (bool with_prefix : {false, true}) {
			if (with_prefix && route.global_handler) {
				continue;
			}
			auto handler = [this, &route, with_prefix](const duckdb_httplib::Request &req,
			                                           duckdb_httplib::Response &res) {
				IcebergHttpResponse response;
				const bool open_endpoint = route.global_handler && route.path == "/oauth/tokens";
				if (!token.empty() && !open_endpoint && req.get_header_value("Authorization") != "Bearer " + token) {
					res.status = 401;
					res.set_content(ErrorBody("NotAuthorizedException",
					                          "Missing or invalid token for the Iceberg REST server", 401),
					                "application/json");
					return;
				}
				IcebergHttpRequest http;
				http.method = req.method;
				string prefix;
				idx_t first_value = 1;
				if (with_prefix) {
					prefix = req.matches.groups[1].str();
					first_value = 2;
				}
				for (idx_t i = first_value; i < req.matches.groups.size(); i++) {
					http.path_values.push_back(req.matches.groups[i].str());
				}
				for (auto &param : req.params) {
					http.query.emplace_back(param.first, param.second);
				}
				http.body = req.body;
				http.content_type = req.get_header_value("Content-Type");
				HandleRequest(route, with_prefix ? optional_ptr<const string>(&prefix) : nullptr, http, response);
				res.status = response.status;
				if (response.status == 204 || req.method == "HEAD") {
					return;
				}
				res.set_content(response.body, response.content_type);
			};
			// the path starts with the version, the prefix of the catalog follows it
			string pattern = "/v1" + (with_prefix ? string("/([^/]+)") : string());
			idx_t pos = 0;
			while (pos < route.path.size()) {
				auto start = route.path.find('{', pos);
				if (start == string::npos) {
					pattern += route.path.substr(pos);
					break;
				}
				pattern += route.path.substr(pos, start - pos);
				pattern += "([^/]+)";
				pos = route.path.find('}', start) + 1;
			}
			if (route.method == "GET") {
				server->Get(pattern, handler);
			} else if (route.method == "DELETE") {
				server->Delete(pattern, handler);
			} else {
				server->Post(pattern, handler);
			}
		}
	}
	server->set_error_handler([](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		if (res.body.empty() && res.status != 204) {
			res.set_content(ErrorBody("BadRequestException", "Unknown Iceberg REST endpoint", res.status),
			                "application/json");
		}
	});
	int32_t bound_port;
	if (port == 0) {
		bound_port = server->bind_to_any_port(host);
	} else {
		bound_port = server->bind_to_port(host, NumericCast<int32_t>(port)) ? NumericCast<int32_t>(port) : -1;
	}
	if (bound_port < 0) {
		throw IOException("Failed to start the Iceberg REST server on %s:%llu", host, port);
	}
	listen_uri = StringUtil::Format("http://%s:%d", host, bound_port);
	server_thread = std::thread([this]() { server->listen_after_bind(); });
	return NumericCast<idx_t>(bound_port);
}

void DuckLakeIcebergServer::Stop() {
	if (server) {
		server->stop();
	}
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

void DuckLakeIcebergServer::Wait() {
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

//===--------------------------------------------------------------------===//
// Registry
//===--------------------------------------------------------------------===//
DuckLakeIcebergServerRegistry &DuckLakeIcebergServerRegistry::Get() {
	// leaked on purpose - the servers have to outlive any static destruction order
	static auto registry = new DuckLakeIcebergServerRegistry();
	return *registry;
}

static string ServerAddress(const string &uri) {
	auto scheme_end = uri.find("://");
	auto path_start = uri.find('/', scheme_end == string::npos ? 0 : scheme_end + 3);
	return path_start == string::npos ? uri : uri.substr(0, path_start);
}

static string UriPrefix(const string &uri) {
	auto address = ServerAddress(uri);
	auto prefix = uri.substr(address.size());
	while (StringUtil::StartsWith(prefix, "/")) {
		prefix.erase(0, 1);
	}
	StringUtil::RTrim(prefix, "/");
	return prefix;
}

DuckLakeIcebergServer &DuckLakeIcebergServerRegistry::Start(unique_ptr<DuckLakeIcebergServer> server,
                                                            const string &host, idx_t port) {
	server->Start(host, port);
	auto listen_uri = server->ListenURI();
	lock_guard<mutex> guard(lock);
	auto &result = *server;
	servers[listen_uri] = std::move(server);
	return result;
}

bool DuckLakeIcebergServerRegistry::Stop(const string &uri) {
	auto prefix = UriPrefix(uri);
	if (!prefix.empty()) {
		auto server = GetServer(uri);
		return server && server->RemoveCatalog(prefix);
	}
	unique_ptr<DuckLakeIcebergServer> server;
	{
		lock_guard<mutex> guard(lock);
		auto entry = servers.find(ServerAddress(uri));
		if (entry == servers.end()) {
			return false;
		}
		server = std::move(entry->second);
		servers.erase(entry);
	}
	server->Stop();
	return true;
}

optional_ptr<DuckLakeIcebergServer> DuckLakeIcebergServerRegistry::GetServer(const string &uri) {
	lock_guard<mutex> guard(lock);
	auto entry = servers.find(ServerAddress(uri));
	if (entry == servers.end()) {
		return nullptr;
	}
	return entry->second.get();
}

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
static Value GetNamedParameter(TableFunctionBindInput &input, const string &name) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end()) {
		return Value();
	}
	return entry->second;
}

static map<string, string> ToStringMap(const Value &value) {
	map<string, string> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : ListValue::GetChildren(value)) {
		auto &children = StructValue::GetChildren(entry);
		result[StringValue::Get(children[0])] = children[1].IsNull() ? string() : StringValue::Get(children[1]);
	}
	return result;
}

static shared_ptr<DatabaseInstance> OpenIsolatedDuckLake(ClientContext &context, const string &path,
                                                         const string &attach_options, const string &catalog_name) {
	DBConfig config;
	config.SetOptionByName("threads",
	                       Value::BIGINT(NumericCast<int64_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())));
	DuckDB db(nullptr, &config);
	db.LoadStaticExtension<DucklakeExtension>();
	Connection con(db);
	auto query = StringUtil::Format("ATTACH %s AS %s", SQLString(path), SQLIdentifier(catalog_name));
	if (!attach_options.empty()) {
		query += " (" + attach_options + ")";
	}
	auto result = con.Query(query);
	result->ThrowIfError("Failed to attach the DuckLake served over the Iceberg REST API: ");
	return db.instance;
}

static unique_ptr<FunctionData> DuckLakeIcebergServeBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto target = input.inputs[0];
	if (target.IsNull()) {
		throw BinderException("ducklake_iceberg_serve: the catalog cannot be NULL");
	}
	auto target_str = StringValue::Get(target);
	auto host_val = GetNamedParameter(input, "host");
	auto port_val = GetNamedParameter(input, "port");
	auto options_val = GetNamedParameter(input, "attach_options");
	auto wait_val = GetNamedParameter(input, "wait");
	auto token_val = GetNamedParameter(input, "token");
	auto prefix_val = GetNamedParameter(input, "prefix");
	auto defaults_val = GetNamedParameter(input, "defaults");
	auto overrides_val = GetNamedParameter(input, "overrides");
	auto metadata_path_val = GetNamedParameter(input, "metadata_path");
	auto snapshot_limit_val = GetNamedParameter(input, "snapshot_limit");
	string host = host_val.IsNull() ? "localhost" : StringValue::Get(host_val);
	idx_t port = port_val.IsNull() ? 0 : port_val.GetValue<idx_t>();
	string attach_options = options_val.IsNull() ? string() : StringValue::Get(options_val);
	bool wait = !wait_val.IsNull() && BooleanValue::Get(wait_val);

	auto &registry = DuckLakeIcebergServerRegistry::Get();
	optional_ptr<DuckLakeIcebergServer> server;
	if (port != 0) {
		server = registry.GetServer(StringUtil::Format("http://%s:%llu", host, port));
	}
	bool isolated = StringUtil::StartsWith(target_str, "ducklake:");
	const string catalog_name = isolated ? "ducklake_iceberg" : target_str;
	string prefix = prefix_val.IsNull() ? catalog_name : StringValue::Get(prefix_val);
	if (server) {
		server->RemoveCatalog(prefix);
	}
	shared_ptr<DatabaseInstance> db;
	if (isolated) {
		db = OpenIsolatedDuckLake(context, target_str, attach_options, catalog_name);
	} else {
		auto &catalog = Catalog::GetCatalog(context, Identifier(target_str));
		if (catalog.GetCatalogType() != "ducklake") {
			throw BinderException("ducklake_iceberg_serve: catalog \"%s\" is not a DuckLake catalog", target_str);
		}
		db = context.db;
	}
	if (!server) {
		server = registry.Start(make_uniq<DuckLakeIcebergServer>(), host, port);
	}
	if (!token_val.IsNull()) {
		server->SetToken(StringValue::Get(token_val));
	}
	if (!metadata_path_val.IsNull()) {
		server->SetMetadataPath(StringValue::Get(metadata_path_val));
	}
	if (!snapshot_limit_val.IsNull()) {
		auto limit = snapshot_limit_val.GetValue<idx_t>();
		if (limit == 0) {
			throw BinderException("ducklake_iceberg_serve: snapshot_limit has to be larger than 0");
		}
		server->SetSnapshotLimit(limit);
	}
	if (!defaults_val.IsNull() || !overrides_val.IsNull()) {
		server->SetClientProperties(ToStringMap(defaults_val), ToStringMap(overrides_val));
	}
	server->AddCatalog(prefix, std::move(db), catalog_name);
	auto listen_uri = server->ListenURI();
	if (wait) {
		server->Wait();
	}

	names.emplace_back("listen_uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("warehouse");
	return_types.emplace_back(LogicalType::VARCHAR);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value(listen_uri), Value(prefix)});
	return std::move(result);
}

static unique_ptr<FunctionData> DuckLakeIcebergStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto uri = input.inputs[0];
	if (uri.IsNull()) {
		throw BinderException("ducklake_iceberg_stop: the listen URI cannot be NULL");
	}
	auto stopped = DuckLakeIcebergServerRegistry::Get().Stop(StringValue::Get(uri));
	names.emplace_back("stopped");
	return_types.emplace_back(LogicalType::BOOLEAN);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value::BOOLEAN(stopped)});
	return std::move(result);
}

static void DuckLakeIcebergExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

DuckLakeIcebergServeFunction::DuckLakeIcebergServeFunction()
    : TableFunction("ducklake_iceberg_serve", FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    DuckLakeIcebergExecute, DuckLakeIcebergServeBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("host", LogicalType::VARCHAR)
		    .Add("port", LogicalType::UBIGINT)
		    .Add("attach_options", LogicalType::VARCHAR)
		    .Add("wait", LogicalType::BOOLEAN)
		    .Add("token", LogicalType::VARCHAR)
		    .Add("prefix", LogicalType::VARCHAR)
		    .Add("metadata_path", LogicalType::VARCHAR)
		    .Add("snapshot_limit", LogicalType::UBIGINT)
		    .Add("defaults", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR))
		    .Add("overrides", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	});
}

DuckLakeIcebergStopFunction::DuckLakeIcebergStopFunction()
    : TableFunction("ducklake_iceberg_stop", FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR),
                    DuckLakeIcebergExecute, DuckLakeIcebergStopBind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
