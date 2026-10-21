//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_client.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "rest/ducklake_rest_api.hpp"

#include <chrono>

namespace duckdb_httplib {
class Client;
} // namespace duckdb_httplib

namespace duckdb {

//! Sends requests to a DuckLake REST server, the connections to the server are pooled
class DuckLakeRESTClient : public DuckLakeRESTClientAPI {
public:
	//! Requests carry the token as a bearer token, if it is set
	DuckLakeRESTClient(string base_url, string token);
	~DuckLakeRESTClient() override;

	string Call(const string &endpoint, const string &json, bool idempotent) override;

	//! The client has another view of the data path than the server: paths are mapped between the two
	void SetPathMapping(string client_path, string server_path);
	optional_ptr<const RESTPathMapper> GetPathMapper() const override {
		return path_mapper.get();
	}
	//! Sends an HTTP request, returns the status code and the body of the response
	int32_t SendRaw(const string &method, const string &target, const string &body, string &response);

	const string &BaseURL() const {
		return base_url;
	}

	//! The schema version and data version (next file id) of the latest catalog and statistics the client loaded
	atomic<idx_t> loaded_metadata_version {DConstants::INVALID_INDEX};
	atomic<idx_t> loaded_data_version {DConstants::INVALID_INDEX};

private:
	//! A connection that was used before and is kept for later requests
	struct IdleConnection {
		unique_ptr<duckdb_httplib::Client> connection;
		std::chrono::steady_clock::time_point idle_since;
	};

	//! Returns an idle connection, if there is one that the server has not closed yet, or a new connection
	unique_ptr<duckdb_httplib::Client> GetConnection(bool &reused);
	void ReturnConnection(unique_ptr<duckdb_httplib::Client> connection);
	//! Sends a request, returns the status code and the body of the response
	int32_t Send(const string &method, const string &target, const string &body, bool idempotent, string &response);

private:
	unique_ptr<RESTPathMapper> path_mapper;
	//! The URL of the server and the prefix of the catalog on the server, which is the path of the URL
	string base_url;
	string server_url;
	string prefix;
	string token;
	mutex lock;
	vector<IdleConnection> idle_connections;
};

} // namespace duckdb
