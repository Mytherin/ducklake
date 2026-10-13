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

	string Post(const string &endpoint, const string &body) override;

	//! The client has another view of the data path than the server: paths are mapped between the two
	void SetPathMapping(string client_path, string server_path);
	optional_ptr<const RESTPathMapper> GetPathMapper() const override {
		return path_mapper.get();
	}
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
	unique_ptr<RESTPathMapper> path_mapper;
	string base_url;
	string token;
	mutex lock;
	vector<unique_ptr<duckdb_httplib::Client>> idle_connections;
};

} // namespace duckdb
