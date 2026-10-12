//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_server.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "rest/ducklake_rest_json.hpp"

#include <thread>

namespace duckdb_httplib {
class Server;
} // namespace duckdb_httplib

namespace duckdb {
class DuckLakeCatalog;
class DuckLakeTransaction;
class DuckLakeMetadataManager;
struct DuckLakeSnapshot;

//! The state of a single REST request, which runs in its own transaction on the served catalog
class DuckLakeRESTRequest {
public:
	DuckLakeRESTRequest(DatabaseInstance &db, const string &catalog_name);

	DuckLakeMetadataManager &GetMetadataManager();
	//! The snapshot of the "snapshot_id" field, or the latest snapshot if it is absent
	DuckLakeSnapshot GetSnapshot(RESTJsonIn request);
	DuckLakeSnapshot GetSnapshotById(idx_t snapshot_id);

	Connection connection;
	DuckLakeCatalog *catalog;
	DuckLakeTransaction *transaction;
};

typedef void (*rest_endpoint_t)(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output);

//! An HTTP server that exposes a DuckLake catalog through the DuckLake REST API
class DuckLakeRESTServer {
public:
	DuckLakeRESTServer(shared_ptr<DatabaseInstance> db, string catalog_name);
	~DuckLakeRESTServer();

	//! Binds the socket and starts serving in the background, returns the bound port
	idx_t Start(const string &host, idx_t port);
	void Stop();
	//! Blocks until the server is stopped
	void Wait();

	const string &ListenURI() const {
		return listen_uri;
	}

	//! Handles a request for an endpoint, returns the HTTP status code
	int32_t HandleRequest(const string &endpoint, const string &body, string &response);

	static const unordered_map<string, rest_endpoint_t> &GetEndpoints();

private:
	//! Kept alive for as long as the server runs
	shared_ptr<DatabaseInstance> db;
	string catalog_name;
	unique_ptr<duckdb_httplib::Server> server;
	std::thread server_thread;
	string listen_uri;
};

//! The REST servers running in this process, by listen URI
class DuckLakeRESTServerRegistry {
public:
	static DuckLakeRESTServerRegistry &Get();

	string Start(unique_ptr<DuckLakeRESTServer> server, const string &host, idx_t port);
	bool Stop(const string &listen_uri);
	optional_ptr<DuckLakeRESTServer> GetServer(const string &listen_uri);

private:
	mutex lock;
	unordered_map<string, unique_ptr<DuckLakeRESTServer>> servers;
};

struct DuckLakeRESTServeFunction : public TableFunction {
	DuckLakeRESTServeFunction();
};

struct DuckLakeRESTStopFunction : public TableFunction {
	DuckLakeRESTStopFunction();
};

} // namespace duckdb
