//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_client.hpp"

namespace duckdb {
class ClientContext;
class DuckLakeRESTServer;
struct DuckLakeOptions;

//! The connection of a DuckLake to the REST server that serves its metadata. For a local DuckLake
//! (ducklake:rest:<path>) the connection also holds the local server that serves it.
class DuckLakeRESTConnection {
public:
	DuckLakeRESTConnection(string url, string token, shared_ptr<DuckLakeRESTServer> local_server);
	~DuckLakeRESTConnection();

	//! Connects to the server of the options, and starts the local server for a local DuckLake
	static unique_ptr<DuckLakeRESTConnection> Open(ClientContext &context, const DuckLakeOptions &options);

	DuckLakeRESTClient &Client() {
		return client;
	}
	//! The last user of a local server detaches the DuckLake that it serves
	void Close(ClientContext &context);

private:
	DuckLakeRESTClient client;
	shared_ptr<DuckLakeRESTServer> local_server;
};

} // namespace duckdb
