//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_server.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "storage/ducklake_metadata_info.hpp"

#include <functional>
#include <thread>

namespace duckdb_httplib {
class Server;
} // namespace duckdb_httplib

namespace duckdb {
class DuckLakeCatalog;
class DuckLakeTransaction;
class DuckLakeMetadataManager;
class DuckLakeIcebergServer;
class IcebergExportCache;
struct DuckLakeSnapshot;

//! An error that is reported to the client of the Iceberg REST API
class IcebergException : public std::exception {
public:
	IcebergException(int32_t status_p, string type_p, string message_p)
	    : status(status_p), type(std::move(type_p)), message(std::move(message_p)) {
	}
	const char *what() const noexcept override {
		return message.c_str();
	}

	int32_t status;
	string type;
	string message;
};

struct IcebergHttpRequest {
	string method;
	//! The values of the path parameters, in the order of the path
	vector<string> path_values;
	vector<pair<string, string>> query;
	string body;
	string content_type;
	//! The host header of the request
	string host;
	//! Whether the request has the token of the server
	bool authorized = true;

	string GetQuery(const string &name, const string &default_value = string()) const;
	bool HasQuery(const string &name) const;
};

struct IcebergHttpResponse {
	int32_t status = 200;
	string body;
	string content_type = "application/json";
};

//! The state of a single request: it runs in its own transaction on the served catalog
class IcebergRequest {
public:
	IcebergRequest(DuckLakeIcebergServer &server, DatabaseInstance &db, const string &catalog_name,
	               const string &prefix);

	DuckLakeMetadataManager &GetMetadataManager();
	//! The snapshot that the request reads
	DuckLakeSnapshot GetSnapshot();
	//! The catalog at the snapshot of the request
	const DuckLakeCatalogInfo &GetCatalogInfo();
	//! Runs a statement in the transaction of the request, throws if it fails
	unique_ptr<QueryResult> Query(const string &query);
	//! Runs a statement on the metadata catalog (with {METADATA_CATALOG} placeholders)
	unique_ptr<QueryResult> MetadataQuery(string query);
	void Commit();

	//! The namespace (schema path) of a schema
	vector<string> GetNamespace(const DuckLakeSchemaInfo &schema);
	//! The schema of a namespace, nullptr if there is none
	optional_ptr<const DuckLakeSchemaInfo> FindSchema(const vector<string> &ns);
	optional_ptr<const DuckLakeTableInfo> FindTable(const DuckLakeSchemaInfo &schema, const string &name);
	optional_ptr<const DuckLakeViewInfo> FindView(const DuckLakeSchemaInfo &schema, const string &name);
	optional_ptr<const DuckLakeSchemaInfo> GetSchemaById(SchemaIndex id);
	//! The qualified SQL name of the schema / table / view
	string SchemaSQL(const vector<string> &ns);
	string EntrySQL(const vector<string> &ns, const string &name);
	//! Makes the request read the catalog at another snapshot
	void UseSnapshot(idx_t snapshot_id);
	//! Forgets the catalog at the snapshot of the request, e.g. after the request changed it
	void InvalidateCatalogInfo();

	DuckLakeIcebergServer &server;
	DatabaseInstance &db;
	Connection connection;
	DuckLakeCatalog *catalog;
	DuckLakeTransaction *transaction;
	string prefix;
	string catalog_name;
	//! The address of the server as the client sees it, which the URLs of the generated files start with
	string base_uri;

private:
	unique_ptr<DuckLakeSnapshot> snapshot;
	unique_ptr<DuckLakeCatalogInfo> catalog_info;
};

//! A handler of an endpoint of the Iceberg REST API
typedef std::function<void(IcebergRequest &request, const IcebergHttpRequest &http, IcebergHttpResponse &response)>
    iceberg_handler_t;

//! A handler of an endpoint that is not for a catalog
typedef std::function<void(DuckLakeIcebergServer &server, const IcebergHttpRequest &http,
                           IcebergHttpResponse &response)>
    iceberg_global_handler_t;

struct IcebergRoute {
	string method;
	//! The path after the version and the prefix, with {name} for the parameters, e.g. /namespaces/{namespace}
	string path;
	//! The handler of an endpoint of a catalog
	iceberg_handler_t handler;
	//! The handler of an endpoint that is not for a catalog, which has no prefix
	iceberg_global_handler_t global_handler;
};

//! An HTTP server that exposes DuckLake catalogs through the Iceberg REST catalog API
class DuckLakeIcebergServer {
public:
	DuckLakeIcebergServer();
	~DuckLakeIcebergServer();

	//! Serves a catalog under the prefix, replacing the catalog that was served under it
	void AddCatalog(const string &prefix, shared_ptr<DatabaseInstance> db, string catalog_name);
	bool RemoveCatalog(const string &prefix);

	//! Binds the socket and starts serving in the background, returns the bound port
	idx_t Start(const string &host, idx_t port, idx_t thread_count = 0);
	void Stop();
	//! Blocks until the server is stopped
	void Wait();

	const string &ListenURI() const {
		return listen_uri;
	}
	void SetToken(string token_p) {
		token = std::move(token_p);
	}
	//! Whether the metadata files are served by the server (generated when they are requested) instead of written to
	//! the metadata path
	void SetVirtualFiles(bool enabled) {
		virtual_files = enabled;
	}
	bool VirtualFiles() const {
		return virtual_files;
	}
	//! The address of the server for clients, if it is not the address in the host header of their requests
	void SetPublicURI(string uri) {
		public_uri = std::move(uri);
	}
	const string &PublicURI() const {
		return public_uri;
	}
	const string &Token() const {
		return token;
	}
	//! A directory for the files that the server needs while it generates metadata files, which are not kept
	void SetMetadataPath(string path) {
		metadata_path = std::move(path);
	}
	const string &MetadataPath() const {
		return metadata_path;
	}
	//! The number of snapshots of the history of a table that are exposed
	void SetSnapshotLimit(idx_t limit) {
		snapshot_limit = limit;
	}
	idx_t SnapshotLimit() const {
		return snapshot_limit;
	}
	void SetClientProperties(map<string, string> defaults, map<string, string> overrides);
	//! The properties that clients get with every table, e.g. the configuration of the storage of the table
	void SetTableConfig(map<string, string> config) {
		table_config = std::move(config);
	}
	const map<string, string> &TableConfig() const {
		return table_config;
	}
	const map<string, string> &ClientDefaults() const {
		return client_defaults;
	}
	const map<string, string> &ClientOverrides() const {
		return client_overrides;
	}
	//! The prefixes of the catalogs that are served
	vector<string> GetPrefixes();
	//! The prefix of the catalog that a warehouse refers to, or of the only served catalog
	string ResolveWarehouse(const string &warehouse);

	struct ServedCatalog {
		string prefix;
		shared_ptr<DatabaseInstance> db;
		string catalog_name;
	};
	ServedCatalog GetCatalog(optional_ptr<const string> prefix);

	//! Handles a request, returns the HTTP status
	int32_t HandleRequest(const IcebergRoute &route, optional_ptr<const string> prefix, const IcebergHttpRequest &http,
	                      IcebergHttpResponse &response);

	//! A lock that serializes the commits of a table
	mutex &TableLock(const string &table_uuid);
	//! The manifests and manifest lists that were generated
	IcebergExportCache &GetExportCache();

private:
	mutex catalog_lock;
	map<string, ServedCatalog> catalogs;
	map<string, string> client_defaults;
	map<string, string> client_overrides;
	map<string, string> table_config;
	string token;
	string public_uri;
	bool virtual_files = false;
	string metadata_path;
	idx_t snapshot_limit = 100;
	unique_ptr<duckdb_httplib::Server> server;
	std::thread server_thread;
	string listen_uri;
	mutex table_lock_lock;
	map<string, unique_ptr<mutex>> table_locks;
	unique_ptr<IcebergExportCache> export_cache;
};

//! The Iceberg REST servers running in this process, by listen URI
class DuckLakeIcebergServerRegistry {
public:
	static DuckLakeIcebergServerRegistry &Get();

	DuckLakeIcebergServer &Start(unique_ptr<DuckLakeIcebergServer> server, const string &host, idx_t port);
	//! Stops the server, or stops serving a catalog if the URI has a prefix
	bool Stop(const string &listen_uri);
	optional_ptr<DuckLakeIcebergServer> GetServer(const string &listen_uri);

private:
	mutex lock;
	unordered_map<string, unique_ptr<DuckLakeIcebergServer>> servers;
};

struct DuckLakeIcebergServeFunction : public TableFunction {
	DuckLakeIcebergServeFunction();
};

struct DuckLakeIcebergStopFunction : public TableFunction {
	DuckLakeIcebergStopFunction();
};

//! The endpoints of the Iceberg REST catalog API
class IcebergEndpoints {
public:
	static const vector<IcebergRoute> &GetRoutes();
};

} // namespace duckdb
