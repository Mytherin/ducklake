#include "rest/ducklake_rest_server.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "ducklake_extension.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

#include "rest/ducklake_rest_client.hpp"

#include "httplib.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Request
//===--------------------------------------------------------------------===//
DuckLakeRESTRequest::DuckLakeRESTRequest(DatabaseInstance &db, const string &catalog_name) : connection(db) {
	connection.BeginTransaction();
	auto &context = *connection.context;
	auto &entry = Catalog::GetCatalog(context, Identifier(catalog_name));
	if (entry.GetCatalogType() != "ducklake") {
		throw InvalidInputException("Catalog \"%s\" is not a DuckLake catalog", catalog_name);
	}
	catalog = &entry.Cast<DuckLakeCatalog>();
	transaction = &DuckLakeTransaction::Get(context, *catalog);
}

DuckLakeMetadataManager &DuckLakeRESTRequest::GetMetadataManager() {
	return transaction->GetMetadataManager();
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshotById(idx_t snapshot_id) {
	BoundAtClause at_clause(Identifier("version"), Value::UBIGINT(snapshot_id));
	return transaction->GetSnapshot(at_clause);
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshot(const DuckLakeSnapshot &snapshot, optional_idx snapshot_id) {
	if (!RESTIsNull(snapshot)) {
		return snapshot;
	}
	if (!snapshot_id.IsValid()) {
		return transaction->GetSnapshot();
	}
	return GetSnapshotById(snapshot_id.GetIndex());
}

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshot(const RESTSnapshotReference &reference) {
	return GetSnapshot(reference.snapshot, reference.snapshot_id);
}

//===--------------------------------------------------------------------===//
// Server
//===--------------------------------------------------------------------===//
DuckLakeRESTServer::DuckLakeRESTServer() {
}

void DuckLakeRESTServer::AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name) {
	if (prefix.empty() || prefix.find('/') != string::npos) {
		throw InvalidInputException("The prefix of a catalog has to be a name without slashes, not \"%s\"", prefix);
	}
	lock_guard<mutex> guard(catalog_lock);
	catalogs[prefix] = ServedCatalog {std::move(db), std::move(catalog_name)};
}

bool DuckLakeRESTServer::RemoveCatalog(const string &prefix) {
	ServedCatalog removed;
	{
		lock_guard<mutex> guard(catalog_lock);
		auto entry = catalogs.find(prefix);
		if (entry == catalogs.end()) {
			return false;
		}
		removed = std::move(entry->second);
		catalogs.erase(entry);
	}
	return true;
}

DuckLakeRESTServer::ServedCatalog DuckLakeRESTServer::GetCatalog(optional_ptr<const string> prefix) {
	lock_guard<mutex> guard(catalog_lock);
	if (prefix) {
		auto entry = catalogs.find(*prefix);
		if (entry == catalogs.end()) {
			throw CatalogException("The server serves no catalog under the prefix \"%s\"", *prefix);
		}
		return entry->second;
	}
	if (catalogs.size() != 1) {
		throw CatalogException(
		    "The server serves %llu catalogs: the URLs need the prefix of one of them (/v1/<prefix>/...)",
		    catalogs.size());
	}
	return catalogs.begin()->second;
}

DuckLakeRESTServer::~DuckLakeRESTServer() {
	Stop();
}

static int32_t StatusCodeForError(ExceptionType type) {
	switch (type) {
	case ExceptionType::TRANSACTION:
		return 409;
	case ExceptionType::CATALOG:
		return 404;
	case ExceptionType::INVALID_INPUT:
	case ExceptionType::BINDER:
	case ExceptionType::PARSER:
	case ExceptionType::CONVERSION:
	case ExceptionType::CONSTRAINT:
	case ExceptionType::INVALID_CONFIGURATION:
		return 400;
	case ExceptionType::NOT_IMPLEMENTED:
		return 501;
	default:
		return 500;
	}
}

static string ErrorBody(ExceptionType type, const string &message, int32_t status) {
	RESTErrorResponse response;
	response.error.type = Exception::ExceptionTypeToString(type);
	response.error.message = message;
	response.error.code = status;
	return DuckLakeRESTSerializer::ToJSON(response);
}

int32_t DuckLakeRESTServer::HandleRequest(const RESTRoute &route, optional_ptr<const string> prefix,
                                          const vector<string> &path_values, const vector<pair<string, string>> &query,
                                          const string &body, string &response) {
	try {
		auto served = GetCatalog(prefix);
		auto &endpoints = DuckLakeRESTAPI::GetEndpoints();
		auto entry = endpoints.find(route.endpoint);
		if (entry == endpoints.end()) {
			throw InternalException("DuckLake REST: the endpoint \"%s\" has no handler", route.endpoint);
		}
		auto request_json = route.ServerRequest(path_values, query, body);
		DuckLakeRESTRequest request(*served.db, served.catalog_name);
		try {
			response = entry->second(request, request_json);
			if (request.connection.HasActiveTransaction()) {
				request.connection.Commit();
			}
		} catch (...) {
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
		return 200;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto status = StatusCodeForError(error.Type());
		response = ErrorBody(error.Type(), error.RawMessage(), status);
		return status;
	}
}

idx_t DuckLakeRESTServer::Start(const string &host, idx_t port, idx_t thread_count) {
	server = make_uniq<duckdb_httplib::Server>();
	if (thread_count > 0) {
		server->new_task_queue = [thread_count]() {
			return new duckdb_httplib::ThreadPool(thread_count);
		};
	}
	for (auto &route : DuckLakeRESTAPI::GetRoutes()) {
		for (bool with_prefix : {false, true}) {
			auto handler = [this, &route, with_prefix](const duckdb_httplib::Request &req,
			                                           duckdb_httplib::Response &res) {
				string response;
				if (!token.empty() && req.get_header_value("Authorization") != "Bearer " + token) {
					res.status = 401;
					res.set_content(ErrorBody(ExceptionType::PERMISSION,
					                          "Missing or invalid token for the DuckLake REST server", 401),
					                "application/json");
					return;
				}
				// the first group is the prefix of the catalog, if the URL has one
				idx_t first_value = with_prefix ? 2 : 1;
				string prefix;
				if (with_prefix) {
					prefix = req.matches.groups[1].str();
				}
				vector<string> path_values;
				for (idx_t i = first_value; i < req.matches.groups.size(); i++) {
					path_values.push_back(req.matches.groups[i].str());
				}
				vector<pair<string, string>> query;
				for (auto &param : req.params) {
					query.emplace_back(param.first, param.second);
				}
				res.status = HandleRequest(route, with_prefix ? optional_ptr<const string>(&prefix) : nullptr,
				                           path_values, query, req.body, response);
				res.set_content(response, "application/json");
			};
			auto pattern = route.PathPattern(with_prefix);
			if (route.method == "GET") {
				server->Get(pattern, handler);
			} else {
				server->Post(pattern, handler);
			}
		}
	}
	server->set_error_handler([](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		if (res.body.empty()) {
			res.set_content(ErrorBody(ExceptionType::INVALID_INPUT, "Unknown DuckLake REST endpoint", res.status),
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
		throw IOException("Failed to start DuckLake REST server on %s:%llu", host, port);
	}
	listen_uri = StringUtil::Format("http://%s:%d", host, bound_port);
	server_thread = std::thread([this]() { server->listen_after_bind(); });
	return NumericCast<idx_t>(bound_port);
}

void DuckLakeRESTServer::Stop() {
	if (server) {
		server->stop();
	}
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

void DuckLakeRESTServer::Wait() {
	if (server_thread.joinable()) {
		server_thread.join();
	}
}

//===--------------------------------------------------------------------===//
// Registry
//===--------------------------------------------------------------------===//
//! Attaches the DuckLake in a database of its own, so the served catalog is isolated from the caller
static shared_ptr<DatabaseInstance> OpenIsolatedDuckLake(ClientContext &context, const string &path,
                                                         const string &attach_options, const string &catalog_name) {
	// the database of the server uses as many threads as the database that starts it
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
	result->ThrowIfError("Failed to attach the DuckLake served over REST: ");
	return db.instance;
}

DuckLakeRESTServerRegistry &DuckLakeRESTServerRegistry::Get() {
	// leaked on purpose - the servers have to outlive any static destruction order
	static auto registry = new DuckLakeRESTServerRegistry();
	return *registry;
}

//! The address of a URI of a server or of one of its catalogs: the scheme, host and port
static string ServerAddress(const string &uri) {
	auto scheme_end = uri.find("://");
	auto path_start = uri.find('/', scheme_end == string::npos ? 0 : scheme_end + 3);
	return path_start == string::npos ? uri : uri.substr(0, path_start);
}

//! The prefix of the catalog in a URI, empty if it has none
static string UriPrefix(const string &uri) {
	auto address = ServerAddress(uri);
	auto prefix = uri.substr(address.size());
	while (StringUtil::StartsWith(prefix, "/")) {
		prefix.erase(0, 1);
	}
	StringUtil::RTrim(prefix, "/");
	return prefix;
}

DuckLakeRESTServer &DuckLakeRESTServerRegistry::Start(unique_ptr<DuckLakeRESTServer> server, const string &host,
                                                      idx_t port) {
	server->Start(host, port);
	auto listen_uri = server->ListenURI();
	lock_guard<mutex> guard(lock);
	auto &result = *server;
	servers[listen_uri] = std::move(server);
	return result;
}

bool DuckLakeRESTServerRegistry::Stop(const string &uri) {
	auto prefix = UriPrefix(uri);
	if (!prefix.empty()) {
		// a catalog of the server
		auto server = GetServer(uri);
		return server && server->RemoveCatalog(prefix);
	}
	unique_ptr<DuckLakeRESTServer> server;
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

optional_ptr<DuckLakeRESTServer> DuckLakeRESTServerRegistry::GetServer(const string &uri) {
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

static unique_ptr<FunctionData> DuckLakeRESTServeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto target = input.inputs[0];
	if (target.IsNull()) {
		throw BinderException("ducklake_rest_serve: the catalog cannot be NULL");
	}
	auto target_str = StringValue::Get(target);
	auto host_val = GetNamedParameter(input, "host");
	auto port_val = GetNamedParameter(input, "port");
	auto options_val = GetNamedParameter(input, "attach_options");
	auto wait_val = GetNamedParameter(input, "wait");
	auto token_val = GetNamedParameter(input, "token");
	string host = host_val.IsNull() ? "localhost" : StringValue::Get(host_val);
	idx_t port = port_val.IsNull() ? 0 : port_val.GetValue<idx_t>();
	string attach_options = options_val.IsNull() ? string() : StringValue::Get(options_val);
	bool wait = !wait_val.IsNull() && BooleanValue::Get(wait_val);

	auto prefix_val = GetNamedParameter(input, "prefix");

	auto &registry = DuckLakeRESTServerRegistry::Get();
	// a server that already listens on this address serves the catalog as well
	optional_ptr<DuckLakeRESTServer> server;
	if (port != 0) {
		server = registry.GetServer(StringUtil::Format("http://%s:%llu", host, port));
	}
	bool isolated = StringUtil::StartsWith(target_str, "ducklake:");
	const string catalog_name = isolated ? "ducklake_rest" : target_str;
	string prefix = prefix_val.IsNull() ? catalog_name : StringValue::Get(prefix_val);
	if (server) {
		// a catalog that is served under the prefix has to release its DuckLake first
		server->RemoveCatalog(prefix);
	}
	shared_ptr<DatabaseInstance> db;
	if (isolated) {
		db = OpenIsolatedDuckLake(context, target_str, attach_options, catalog_name);
	} else {
		auto &catalog = Catalog::GetCatalog(context, Identifier(target_str));
		if (catalog.GetCatalogType() != "ducklake") {
			throw BinderException("ducklake_rest_serve: catalog \"%s\" is not a DuckLake catalog", target_str);
		}
		db = context.db;
	}
	if (!server) {
		server = registry.Start(make_uniq<DuckLakeRESTServer>(), host, port);
	}
	if (!token_val.IsNull()) {
		server->SetToken(StringValue::Get(token_val));
	}
	server->AddCatalog(prefix, std::move(db), catalog_name);
	auto listen_uri = server->ListenURI() + "/" + prefix;
	if (wait) {
		server->Wait();
	}

	names.emplace_back("listen_uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value(listen_uri)});
	return std::move(result);
}

static unique_ptr<FunctionData> DuckLakeRESTStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto uri = input.inputs[0];
	if (uri.IsNull()) {
		throw BinderException("ducklake_rest_stop: the listen URI cannot be NULL");
	}
	auto stopped = DuckLakeRESTServerRegistry::Get().Stop(StringValue::Get(uri));
	names.emplace_back("stopped");
	return_types.emplace_back(LogicalType::BOOLEAN);
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value::BOOLEAN(stopped)});
	return std::move(result);
}

static unique_ptr<FunctionData> DuckLakeRESTRequestBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("ducklake_rest_request: the arguments cannot be NULL");
		}
	}
	auto token = GetNamedParameter(input, "token");
	DuckLakeRESTClient client(StringValue::Get(input.inputs[0]), token.IsNull() ? string() : StringValue::Get(token));
	string response;
	auto status = client.SendRaw(StringUtil::Upper(StringValue::Get(input.inputs[1])),
	                             StringValue::Get(input.inputs[2]), StringValue::Get(input.inputs[3]), response);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("response");
	return_types.emplace_back(LogicalType::JSON());
	auto result = make_uniq<MetadataBindData>();
	result->rows.push_back({Value::INTEGER(status), Value(response)});
	return std::move(result);
}

static void DuckLakeRESTExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<MetadataBindData>();
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	DuckLakeBaseMetadataFunction::ScanRows(data.rows, state.offset, output);
}

DuckLakeRESTServeFunction::DuckLakeRESTServeFunction()
    : TableFunction("ducklake_rest_serve", FunctionSignature().AddPositionalOnly("catalog", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTServeBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("host", LogicalType::VARCHAR)
		    .Add("port", LogicalType::UBIGINT)
		    .Add("attach_options", LogicalType::VARCHAR)
		    .Add("wait", LogicalType::BOOLEAN)
		    .Add("token", LogicalType::VARCHAR)
		    .Add("prefix", LogicalType::VARCHAR);
	});
}

DuckLakeRESTRequestFunction::DuckLakeRESTRequestFunction()
    : TableFunction("ducklake_rest_request",
                    FunctionSignature()
                        .AddPositionalOnly("listen_uri", LogicalType::VARCHAR)
                        .AddPositionalOnly("method", LogicalType::VARCHAR)
                        .AddPositionalOnly("path", LogicalType::VARCHAR)
                        .AddPositionalOnly("body", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTRequestBind, DuckLakeRunOnceState::Init) {
	GetSignature().WithTypedKwargs("options",
	                               [&](TypedKwargs &options) { options.Add("token", LogicalType::VARCHAR); });
}

DuckLakeRESTStopFunction::DuckLakeRESTStopFunction()
    : TableFunction("ducklake_rest_stop", FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTStopBind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
