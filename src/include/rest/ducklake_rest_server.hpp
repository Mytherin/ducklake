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
#include "rest/ducklake_rest_api.hpp"

#include <thread>

namespace duckdb_httplib {
class Server;
} // namespace duckdb_httplib

namespace duckdb {
class DuckLakeCatalog;
class DuckLakeTransaction;
class DuckLakeMetadataManager;
class DuckLakeRESTServer;
struct DuckLakeSnapshot;

//! The state of a single REST request, which runs in its own transaction on the served catalog
class DuckLakeRESTRequest {
public:
	DuckLakeRESTRequest(DatabaseInstance &db, const string &catalog_name);

	DuckLakeMetadataManager &GetMetadataManager();
	//! The snapshot of a request: the snapshot, the snapshot with the id, or the latest snapshot if both are absent
	DuckLakeSnapshot GetSnapshot(const DuckLakeSnapshot &snapshot, optional_idx snapshot_id);
	DuckLakeSnapshot GetSnapshot(const RESTSnapshotReference &reference);
	DuckLakeSnapshot GetSnapshotById(idx_t snapshot_id);

	Connection connection;
	DuckLakeCatalog *catalog;
	DuckLakeTransaction *transaction;
	//! The server and the prefix of the catalog that the request is for
	optional_ptr<DuckLakeRESTServer> server;
	string prefix;
};

//! An HTTP server that exposes DuckLake catalogs through the DuckLake REST API. The URLs of a catalog have its prefix
//! after the version (`/v1/<prefix>/...`). The URLs without a prefix refer to the catalog that the server serves, if it
//! serves exactly one.
class DuckLakeRESTServer {
public:
	DuckLakeRESTServer();
	~DuckLakeRESTServer();

	//! Serves a catalog under the prefix, replacing the catalog that was served under it
	void AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name);
	//! Stops serving the catalog under the prefix, returns whether there was one
	bool RemoveCatalog(const string &prefix);
	//! The properties that clients get from the server, besides the prefix of their catalog
	void SetClientProperties(map<string, string> defaults, map<string, string> overrides);
	const map<string, string> &ClientDefaults() const {
		return client_defaults;
	}
	const map<string, string> &ClientOverrides() const {
		return client_overrides;
	}

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

	//! Handles a request for an endpoint of a catalog, returns the HTTP status code. The prefix is only set for URLs
	//! with a prefix
	int32_t HandleRequest(const RESTRoute &route, optional_ptr<const string> prefix, const vector<string> &path_values,
	                      const vector<pair<string, string>> &query, const string &body, string &response);

private:
	struct ServedCatalog {
		string prefix;
		shared_ptr<DatabaseInstance> db;
		string catalog_name;
	};
	//! The catalog of a request, throws if there is none
	ServedCatalog GetCatalog(optional_ptr<const string> prefix);

private:
	mutex catalog_lock;
	map<string, ServedCatalog> catalogs;
	map<string, string> client_defaults;
	map<string, string> client_overrides;
	string token;
	unique_ptr<duckdb_httplib::Server> server;
	std::thread server_thread;
	string listen_uri;
};

//! The REST servers running in this process, by listen URI
class DuckLakeRESTServerRegistry {
public:
	static DuckLakeRESTServerRegistry &Get();

	//! Starts a server, which serves no catalogs yet
	DuckLakeRESTServer &Start(unique_ptr<DuckLakeRESTServer> server, const string &host, idx_t port);
	//! Stops the server, or stops serving a catalog if the URI has a prefix
	bool Stop(const string &listen_uri);
	//! The server that listens on the address of a URI, which can have a prefix
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

//! Sends a request to a DuckLake REST server, e.g. to explore the API
struct DuckLakeRESTRequestFunction : public TableFunction {
	DuckLakeRESTRequestFunction();
};

} // namespace duckdb
