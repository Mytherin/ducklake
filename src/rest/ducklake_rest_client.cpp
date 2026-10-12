#include "rest/ducklake_rest_client.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"

#include "httplib.hpp"

namespace duckdb {

DuckLakeRESTClient::DuckLakeRESTClient(string base_url_p, string token_p)
    : base_url(std::move(base_url_p)), token(std::move(token_p)) {
	StringUtil::RTrim(base_url, "/");
}

DuckLakeRESTClient::~DuckLakeRESTClient() {
}

unique_ptr<duckdb_httplib::Client> DuckLakeRESTClient::GetConnection() {
	{
		lock_guard<mutex> guard(lock);
		if (!idle_connections.empty()) {
			auto connection = std::move(idle_connections.back());
			idle_connections.pop_back();
			return connection;
		}
	}
	auto connection = make_uniq<duckdb_httplib::Client>(base_url);
	connection->set_keep_alive(true);
	connection->set_read_timeout(300, 0);
	connection->set_write_timeout(300, 0);
	return connection;
}

void DuckLakeRESTClient::ReturnConnection(unique_ptr<duckdb_httplib::Client> connection) {
	lock_guard<mutex> guard(lock);
	idle_connections.push_back(std::move(connection));
}

int32_t DuckLakeRESTClient::PostRaw(const string &endpoint, const string &body, string &response) {
	auto connection = GetConnection();
	duckdb_httplib::Headers headers;
	if (!token.empty()) {
		headers.emplace("Authorization", "Bearer " + token);
	}
	auto result = connection->Post("/v1/" + endpoint, headers, body, "application/json");
	if (!result) {
		throw IOException("Failed to send DuckLake REST request \"%s\" to %s: %s", endpoint, base_url,
		                  duckdb_httplib::to_string(result.error()));
	}
	ReturnConnection(std::move(connection));
	response = result->body;
	return result->status;
}

unique_ptr<RESTJsonReader> DuckLakeRESTClient::Post(const string &endpoint, const RESTJsonWriter &request) {
	auto body = request.ToString();
	auto connection = GetConnection();
	duckdb_httplib::Headers headers;
	if (!token.empty()) {
		headers.emplace("Authorization", "Bearer " + token);
	}
	auto result = connection->Post("/v1/" + endpoint, headers, body, "application/json");
	if (!result) {
		throw IOException("Failed to send DuckLake REST request \"%s\" to %s: %s", endpoint, base_url,
		                  duckdb_httplib::to_string(result.error()));
	}
	ReturnConnection(std::move(connection));
	auto response = make_uniq<RESTJsonReader>(result->body);
	if (result->status != 200) {
		auto error = response->Root().TryGet("error");
		if (error.IsNull()) {
			throw IOException("DuckLake REST request \"%s\" failed with status %d", endpoint, result->status);
		}
		auto type = Exception::StringToExceptionType(error.Str("type", "Error"));
		ErrorData(type, error.Str("message", string())).Throw();
	}
	return response;
}

} // namespace duckdb
