#include "rest/ducklake_rest_client.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"

#include "httplib.hpp"

namespace duckdb {

DuckLakeRESTClient::DuckLakeRESTClient(string base_url_p) : base_url(std::move(base_url_p)) {
	StringUtil::RTrim(base_url, "/");
	client = make_uniq<duckdb_httplib::Client>(base_url);
	client->set_keep_alive(true);
	client->set_read_timeout(300, 0);
	client->set_write_timeout(300, 0);
}

DuckLakeRESTClient::~DuckLakeRESTClient() {
}

unique_ptr<RESTJsonReader> DuckLakeRESTClient::Post(const string &endpoint, const RESTJsonWriter &request) {
	auto body = request.ToString();
	lock_guard<mutex> guard(lock);
	auto result = client->Post("/v1/" + endpoint, body, "application/json");
	if (!result) {
		throw IOException("Failed to send DuckLake REST request \"%s\" to %s: %s", endpoint, base_url,
		                  duckdb_httplib::to_string(result.error()));
	}
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
