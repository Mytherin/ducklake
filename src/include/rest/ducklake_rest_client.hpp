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

//! Sends requests to a DuckLake REST server
class DuckLakeRESTClient {
public:
	explicit DuckLakeRESTClient(string base_url);
	~DuckLakeRESTClient();

	//! Sends the request to an endpoint, rethrows the error reported by the server
	unique_ptr<RESTJsonReader> Post(const string &endpoint, const RESTJsonWriter &request);

	const string &BaseURL() const {
		return base_url;
	}

private:
	string base_url;
	mutex lock;
	unique_ptr<duckdb_httplib::Client> client;
};

} // namespace duckdb
