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
#include "rest/ducklake_rest_json.hpp"

namespace duckdb_httplib {
class Client;
} // namespace duckdb_httplib

namespace duckdb {

//! Sends requests to a DuckLake REST server, the connections to the server are pooled
class DuckLakeRESTClient {
public:
	//! Requests carry the token as a bearer token, if it is set
	DuckLakeRESTClient(string base_url, string token);
	~DuckLakeRESTClient();

	//! Sends the request to an endpoint, rethrows the error reported by the server
	unique_ptr<RESTJsonReader> Post(const string &endpoint, const RESTJsonWriter &request);
	//! Sends a request body to an endpoint, returns the status code and the body of the response
	int32_t PostRaw(const string &endpoint, const string &body, string &response);

	const string &BaseURL() const {
		return base_url;
	}

	//! The schema version and data version (next file id) of the latest catalog and statistics the client loaded
	atomic<idx_t> loaded_metadata_version {DConstants::INVALID_INDEX};
	atomic<idx_t> loaded_data_version {DConstants::INVALID_INDEX};

private:
	unique_ptr<duckdb_httplib::Client> GetConnection();
	void ReturnConnection(unique_ptr<duckdb_httplib::Client> connection);

private:
	string base_url;
	string token;
	mutex lock;
	vector<unique_ptr<duckdb_httplib::Client>> idle_connections;
};

} // namespace duckdb
