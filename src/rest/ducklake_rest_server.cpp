#include "rest/ducklake_rest_server.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "ducklake_extension.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

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

DuckLakeSnapshot DuckLakeRESTRequest::GetSnapshot(RESTJsonIn request) {
	auto snapshot_id = request.TryGet("snapshot_id");
	if (snapshot_id.IsNull()) {
		return transaction->GetSnapshot();
	}
	return GetSnapshotById(snapshot_id.UInt());
}

//===--------------------------------------------------------------------===//
// Server
//===--------------------------------------------------------------------===//
DuckLakeRESTServer::DuckLakeRESTServer(shared_ptr<DatabaseInstance> db_p, string catalog_name_p,
                                       bool keep_database_alive)
    : db(db_p), catalog_name(std::move(catalog_name_p)) {
	if (keep_database_alive) {
		owned_db = std::move(db_p);
	}
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
	case ExceptionType::NOT_IMPLEMENTED:
	case ExceptionType::INVALID_CONFIGURATION:
		return 400;
	default:
		return 500;
	}
}

int32_t DuckLakeRESTServer::HandleRequest(const string &endpoint, const string &body, string &response) {
	try {
		auto &endpoints = GetEndpoints();
		auto entry = endpoints.find(endpoint);
		if (entry == endpoints.end()) {
			throw InvalidInputException("Unknown DuckLake REST endpoint \"%s\"", endpoint);
		}
		auto database = db.lock();
		if (!database) {
			throw ConnectionException("The database served by the DuckLake REST server was closed");
		}
		RESTJsonReader input(body.empty() ? "{}" : body);
		RESTJsonWriter output;
		DuckLakeRESTRequest request(*database, catalog_name);
		try {
			entry->second(request, input.Root(), output.Root());
			if (request.connection.HasActiveTransaction()) {
				request.connection.Commit();
			}
		} catch (...) {
			if (request.connection.HasActiveTransaction()) {
				request.connection.Rollback();
			}
			throw;
		}
		response = output.ToString();
		return 200;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		RESTJsonWriter output;
		auto error_out = output.Root().Obj("error");
		error_out.SetStr("type", Exception::ExceptionTypeToString(error.Type()));
		error_out.SetStr("message", error.RawMessage());
		response = output.ToString();
		return StatusCodeForError(error.Type());
	}
}

static const string ENDPOINT_PREFIX = "/v1/";

idx_t DuckLakeRESTServer::Start(const string &host, idx_t port, idx_t thread_count) {
	server = make_uniq<duckdb_httplib::Server>();
	if (thread_count > 0) {
		server->new_task_queue = [thread_count]() {
			return new duckdb_httplib::ThreadPool(thread_count);
		};
	}
	auto handler = [this](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		string response;
		res.status = HandleRequest(req.path.substr(ENDPOINT_PREFIX.size()), req.body, response);
		res.set_content(response, "application/json");
	};
	const string pattern = R"(/v1/([a-z_]+))";
	server->Get(pattern, handler);
	server->Post(pattern, handler);
	int bound_port;
	if (port == 0) {
		bound_port = server->bind_to_any_port(host);
	} else {
		bound_port = server->bind_to_port(host, NumericCast<int>(port)) ? NumericCast<int>(port) : -1;
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
static shared_ptr<DatabaseInstance> OpenIsolatedDuckLake(const string &path, const string &attach_options,
                                                         const string &catalog_name) {
	DuckDB db(nullptr);
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

string DuckLakeRESTServerRegistry::Start(unique_ptr<DuckLakeRESTServer> server, const string &host, idx_t port) {
	if (port != 0) {
		// a server that is already listening on this address is replaced
		Stop(StringUtil::Format("http://%s:%llu", host, port));
	}
	server->Start(host, port);
	auto listen_uri = server->ListenURI();
	lock_guard<mutex> guard(lock);
	servers[listen_uri] = std::move(server);
	return listen_uri;
}

bool DuckLakeRESTServerRegistry::Stop(const string &listen_uri) {
	unique_ptr<DuckLakeRESTServer> server;
	{
		lock_guard<mutex> guard(lock);
		auto entry = servers.find(listen_uri);
		if (entry == servers.end()) {
			return false;
		}
		server = std::move(entry->second);
		servers.erase(entry);
	}
	server->Stop();
	return true;
}

shared_ptr<DuckLakeRESTServer> DuckLakeRESTServerRegistry::GetLocalServer(const shared_ptr<DatabaseInstance> &db,
                                                                          const string &path,
                                                                          const string &attach_options) {
	static constexpr idx_t LOCAL_SERVER_THREADS = 4;
	lock_guard<mutex> guard(lock);
	auto key = to_string(reinterpret_cast<uintptr_t>(db.get())) + ":" + path;
	auto entry = local_servers.find(key);
	if (entry != local_servers.end()) {
		auto server = entry->second.lock();
		if (server) {
			return server;
		}
	}
	auto catalog_name = "__ducklake_rest_" + to_string(local_server_count++);
	Connection con(*db);
	auto query =
	    StringUtil::Format("ATTACH %s AS %s (HIDDEN true", SQLString("ducklake:" + path), SQLIdentifier(catalog_name));
	if (!attach_options.empty()) {
		query += ", " + attach_options;
	}
	query += ")";
	auto result = con.Query(query);
	result->ThrowIfError("Failed to attach the DuckLake served over REST: ");
	auto server = make_shared_ptr<DuckLakeRESTServer>(db, catalog_name, false);
	server->Start("localhost", 0, LOCAL_SERVER_THREADS);
	local_servers[key] = server;
	return server;
}

optional_ptr<DuckLakeRESTServer> DuckLakeRESTServerRegistry::GetServer(const string &listen_uri) {
	lock_guard<mutex> guard(lock);
	auto entry = servers.find(listen_uri);
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
	string host = host_val.IsNull() ? "localhost" : StringValue::Get(host_val);
	idx_t port = port_val.IsNull() ? 0 : port_val.GetValue<idx_t>();
	string attach_options = options_val.IsNull() ? string() : StringValue::Get(options_val);
	bool wait = !wait_val.IsNull() && BooleanValue::Get(wait_val);

	unique_ptr<DuckLakeRESTServer> server;
	if (StringUtil::StartsWith(target_str, "ducklake:")) {
		const string catalog_name = "ducklake_rest";
		auto db = OpenIsolatedDuckLake(target_str, attach_options, catalog_name);
		server = make_uniq<DuckLakeRESTServer>(std::move(db), catalog_name);
	} else {
		auto &catalog = Catalog::GetCatalog(context, Identifier(target_str));
		if (catalog.GetCatalogType() != "ducklake") {
			throw BinderException("ducklake_rest_serve: catalog \"%s\" is not a DuckLake catalog", target_str);
		}
		server = make_uniq<DuckLakeRESTServer>(context.db, target_str);
	}
	auto &registry = DuckLakeRESTServerRegistry::Get();
	auto listen_uri = registry.Start(std::move(server), host, port);
	if (wait) {
		auto running = registry.GetServer(listen_uri);
		if (running) {
			running->Wait();
		}
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
		    .Add("wait", LogicalType::BOOLEAN);
	});
}

DuckLakeRESTStopFunction::DuckLakeRESTStopFunction()
    : TableFunction("ducklake_rest_stop", FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR),
                    DuckLakeRESTExecute, DuckLakeRESTStopBind, DuckLakeRunOnceState::Init) {
}

} // namespace duckdb
