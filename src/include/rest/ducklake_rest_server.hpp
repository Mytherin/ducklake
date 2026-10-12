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
	//! The snapshot of a request: the "snapshot" object, the snapshot with the id "snapshot_id", or the latest
	//! snapshot if both are absent
	DuckLakeSnapshot GetSnapshot(RESTJsonIn request, const string &key = "snapshot");
	DuckLakeSnapshot GetSnapshotById(idx_t snapshot_id);

	Connection connection;
	DuckLakeCatalog *catalog;
	DuckLakeTransaction *transaction;
};

typedef void (*rest_endpoint_t)(DuckLakeRESTRequest &request, RESTJsonIn input, RESTJsonOut output);

//! An HTTP server that exposes a DuckLake catalog through the DuckLake REST API
class DuckLakeRESTServer {
public:
	//! The server keeps the database alive, unless it is a server of a catalog of the same database
	DuckLakeRESTServer(shared_ptr<DatabaseInstance> db, string catalog_name, bool keep_database_alive = true);
	~DuckLakeRESTServer();

	//! Binds the socket and starts serving in the background, returns the bound port
	idx_t Start(const string &host, idx_t port, idx_t thread_count = 0);
	//! Requests have to carry the token as a bearer token, if it is set
	void SetToken(string token_p) {
		token = std::move(token_p);
	}
	void Stop();
	//! Blocks until the server is stopped
	void Wait();

	const string &ListenURI() const {
		return listen_uri;
	}
	const string &CatalogName() const {
		return catalog_name;
	}

	//! Handles a request for an endpoint, returns the HTTP status code
	int32_t HandleRequest(const string &endpoint, const string &body, string &response);

	static const unordered_map<string, rest_endpoint_t> &GetEndpoints();

private:
	weak_ptr<DatabaseInstance> db;
	shared_ptr<DatabaseInstance> owned_db;
	string catalog_name;
	string token;
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
	//! A server on a free local port that serves the DuckLake at a path, attached in a database as a hidden catalog.
	//! The server is shared by all users of the DuckLake in the database, and stops when the last one is gone.
	shared_ptr<DuckLakeRESTServer> GetLocalServer(const shared_ptr<DatabaseInstance> &db, const string &path,
	                                              const string &attach_options);

private:
	mutex lock;
	unordered_map<string, unique_ptr<DuckLakeRESTServer>> servers;
	unordered_map<string, weak_ptr<DuckLakeRESTServer>> local_servers;
	idx_t local_server_count = 0;
};

struct DuckLakeRESTServeFunction : public TableFunction {
	DuckLakeRESTServeFunction();
};

struct DuckLakeRESTStopFunction : public TableFunction {
	DuckLakeRESTStopFunction();
};

} // namespace duckdb
