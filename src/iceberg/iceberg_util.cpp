#include "iceberg/iceberg_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>

namespace duckdb {

vector<string> IcebergUtil::ParseNamespace(const string &encoded) {
	// the levels of a multipart namespace are separated by the unit separator (0x1F)
	vector<string> result;
	string current;
	for (auto c : encoded) {
		if (c == '\x1F') {
			result.push_back(std::move(current));
			current.clear();
		} else {
			current += c;
		}
	}
	result.push_back(std::move(current));
	return result;
}

vector<string> IcebergUtil::NamespaceFromJson(RESTJsonIn json) {
	if (!json.IsArray()) {
		throw IcebergException(400, "BadRequestException", "The namespace has to be an array of strings");
	}
	vector<string> result;
	for (auto &item : json.Items()) {
		result.push_back(item.Str());
	}
	return result;
}

JSONMutableValue IcebergUtil::NamespaceToJson(JSONWriter &writer, const vector<string> &ns) {
	auto result = writer.CreateArray();
	for (auto &part : ns) {
		result.Append(writer.CreateString(part));
	}
	return result;
}

string IcebergUtil::NamespaceToString(const vector<string> &ns) {
	return StringUtil::Join(ns, ".");
}

unique_ptr<RESTJsonReader> IcebergUtil::ParseBody(const IcebergHttpRequest &http) {
	try {
		return make_uniq<RESTJsonReader>(http.body.empty() ? string("{}") : http.body);
	} catch (std::exception &ex) {
		throw IcebergException(400, "BadRequestException", StringUtil::Format("Invalid JSON request: %s", ex.what()));
	}
}

string IcebergUtil::ReadFile(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
	auto size = handle->GetFileSize();
	string result;
	result.resize(size);
	if (size > 0) {
		handle->Read(QueryContext(context), data_ptr_cast(&result[0]), size, 0);
	}
	return result;
}

void IcebergUtil::WriteFile(ClientContext &context, const string &path, const string &content) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto separator = path.find_last_of('/');
	if (separator != string::npos && separator > 0 && path.find("://") == string::npos) {
		fs.CreateDirectoriesRecursive(path.substr(0, separator));
	}
	auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
	if (!content.empty()) {
		handle->Write(QueryContext(context), const_cast<char *>(content.data()), content.size(), 0);
	}
	handle->Sync();
	handle->Close();
}

void IcebergUtil::WriteFileAtomically(ClientContext &context, const string &path, const string &content) {
	if (path.find("://") != string::npos) {
		// the objects of an object store appear when they are complete
		WriteFile(context, path, content);
		return;
	}
	auto temporary = path + ".tmp-" + RandomUUID();
	WriteFile(context, temporary, content);
	FileSystem::GetFileSystem(context).MoveFile(temporary, path);
}

bool IcebergUtil::FileExists(ClientContext &context, const string &path) {
	return FileSystem::GetFileSystem(context).FileExists(path);
}

string IcebergUtil::StripTrailingSlash(string path) {
	while (path.size() > 1 && path.back() == '/') {
		path.pop_back();
	}
	return path;
}

string IcebergUtil::JoinPath(const string &a, const string &b) {
	return StripTrailingSlash(a) + "/" + b;
}

string IcebergUtil::CopyToBytes(IcebergRequest &request, const string &copy_statement) {
	auto &context = *request.connection.context;
	auto &fs = FileSystem::GetFileSystem(context);
	auto directory = request.server.MetadataPath();
	fs.CreateDirectoriesRecursive(directory);
	auto path = JoinPath(directory, "generating-" + RandomUUID() + ".parquet");
	try {
		request.MetadataQuery(StringUtil::Replace(copy_statement, "{FILE}", SQLString::ToString(path)));
		auto result = ReadFile(context, path);
		fs.TryRemoveFile(path);
		return result;
	} catch (...) {
		fs.TryRemoveFile(path);
		throw;
	}
}

int64_t IcebergUtil::NowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

string IcebergUtil::RandomUUID() {
	return UUID::ToString(UUID::GenerateRandomUUID());
}

string IcebergUtil::JsonString(const string &value) {
	RESTJsonWriter writer;
	writer.SetRoot(writer.writer.CreateString(value));
	return writer.ToString();
}

JSONMutableValue IcebergUtil::StringMap(JSONWriter &writer, const map<string, string> &values) {
	auto result = writer.CreateObject();
	for (auto &entry : values) {
		result.AddString(entry.first, entry.second);
	}
	return result;
}

map<string, string> IcebergUtil::ReadStringMap(RESTJsonIn json) {
	map<string, string> result;
	if (!json.IsObject()) {
		return result;
	}
	for (auto &member : json.Members()) {
		result[member.first] = member.second.IsString() ? member.second.Str() : member.second.val.ToString();
	}
	return result;
}

} // namespace duckdb
