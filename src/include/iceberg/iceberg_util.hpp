//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/json_document.hpp"
#include "rest/ducklake_rest_json.hpp"
#include "iceberg/iceberg_server.hpp"

namespace duckdb {

class IcebergUtil {
public:
	//! The namespace of a path parameter or a JSON array
	static vector<string> ParseNamespace(const string &encoded);
	static vector<string> NamespaceFromJson(RESTJsonIn json);
	static JSONMutableValue NamespaceToJson(JSONWriter &writer, const vector<string> &ns);
	static string NamespaceToString(const vector<string> &ns);

	//! A JSON document of a request, throws a BadRequest if the body is not valid
	static unique_ptr<RESTJsonReader> ParseBody(const IcebergHttpRequest &http);

	//! Reads the whole content of a file / writes a file, creating its directories
	static string ReadFile(ClientContext &context, const string &path);
	static void WriteFile(ClientContext &context, const string &path, const string &content);
	static bool FileExists(ClientContext &context, const string &path);
	//! Writes a file that readers never see half written (a temporary file that is renamed)
	static void WriteFileAtomically(ClientContext &context, const string &path, const string &content);
	static string JoinPath(const string &a, const string &b);
	static string StripTrailingSlash(string path);

	//! Runs a COPY statement of the metadata catalog that writes a file ({FILE} is the path of the file in the
	//! statement) and returns the content of the file. The file is not kept.
	static string CopyToBytes(IcebergRequest &request, const string &copy_statement);

	//! The current time in milliseconds since the epoch
	static int64_t NowMs();
	static string RandomUUID();

	//! A JSON string literal
	static string JsonString(const string &value);
	static JSONMutableValue StringMap(JSONWriter &writer, const map<string, string> &values);
	static map<string, string> ReadStringMap(RESTJsonIn json);
};

} // namespace duckdb
