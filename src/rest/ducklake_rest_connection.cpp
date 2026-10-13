#include "rest/ducklake_rest_connection.hpp"

#include "common/ducklake_options.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "rest/ducklake_rest_server.hpp"

namespace duckdb {

DuckLakeRESTConnection::DuckLakeRESTConnection(string url, string token, shared_ptr<DuckLakeRESTServer> local_server_p)
    : client(std::move(url), std::move(token)), local_server(std::move(local_server_p)) {
}

DuckLakeRESTConnection::~DuckLakeRESTConnection() {
}

unique_ptr<DuckLakeRESTConnection> DuckLakeRESTConnection::Open(ClientContext &context,
                                                                const DuckLakeOptions &options) {
	shared_ptr<DuckLakeRESTServer> local_server;
	string url = options.metadata_path;
	if (!options.rest_local_path.empty()) {
		local_server = DuckLakeRESTServerRegistry::Get().GetLocalServer(context.db, options.rest_local_path,
		                                                                options.rest_local_attach_options);
		url = local_server->ListenURI();
	} else if (StringUtil::StartsWith(url, "rest:")) {
		url = url.substr(5);
	}
	string token;
	auto entry = options.metadata_parameters.find("token");
	if (entry != options.metadata_parameters.end()) {
		token = entry->second.ToString();
	}
	return make_uniq<DuckLakeRESTConnection>(std::move(url), std::move(token), std::move(local_server));
}

void DuckLakeRESTConnection::Close(ClientContext &context) {
	auto server = std::move(local_server);
	if (!server || server.use_count() != 1) {
		return;
	}
	auto served_catalog = server->CatalogName();
	server.reset();
	DatabaseManager::Get(context).DetachDatabase(context, Identifier(served_catalog), OnEntryNotFound::RETURN_NULL);
}

} // namespace duckdb
