#include "iceberg/iceberg_virtual_files.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg/iceberg_table_export.hpp"
#include "iceberg/iceberg_util.hpp"
#include "iceberg/iceberg_views.hpp"
#include "mbedtls_wrapper.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

static string Hex(const string &bytes) {
	static constexpr const char *DIGITS = "0123456789abcdef";
	string result;
	for (auto c : bytes) {
		result += DIGITS[static_cast<uint8_t>(c) >> 4];
		result += DIGITS[static_cast<uint8_t>(c) & 15];
	}
	return result;
}

static string Sign(DuckLakeIcebergServer &server, const string &prefix, const string &entity, const string &name) {
	return Hex(
	    duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(server.Token() + '\0' + prefix + '/' + entity + '/' + name));
}

string IcebergVirtualFiles::Entity(const DuckLakeTableInfo &table) {
	return "t" + to_string(table.id.index);
}

string IcebergVirtualFiles::ViewEntity(const DuckLakeViewInfo &view) {
	return "v" + to_string(view.id.index);
}

static string FilePath(IcebergRequest &request, const string &entity, const string &name) {
	return IcebergUtil::JoinPath(
	    IcebergUtil::JoinPath(IcebergUtil::JoinPath(request.server.MetadataPath(), request.prefix), entity), name);
}

void IcebergVirtualFiles::Publish(IcebergRequest &request, const string &entity, const string &name,
                                  const string &content) {
	if (request.server.VirtualFiles()) {
		return;
	}
	auto path = FilePath(request, entity, name);
	auto &context = *request.connection.context;
	if (!IcebergUtil::FileExists(context, path)) {
		IcebergUtil::WriteFileAtomically(context, path, content);
	}
}

bool IcebergVirtualFiles::IsGenerated(IcebergRequest &request, const string &path) {
	if (IsVirtual(path)) {
		return true;
	}
	return !request.server.VirtualFiles() && StringUtil::StartsWith(path, request.server.MetadataPath());
}

string IcebergVirtualFiles::Url(IcebergRequest &request, const string &entity, const string &name) {
	if (!request.server.VirtualFiles()) {
		return FilePath(request, entity, name);
	}
	auto url = request.base_uri + "/v1/" + request.prefix + "/metadata/" + entity + "/" + name;
	auto &server = request.server;
	if (!server.Token().empty()) {
		// the clients read the files without the token
		url += "?sig=" + Sign(server, request.prefix, entity, name);
	}
	return url;
}

bool IcebergVirtualFiles::Parse(const string &path, string &prefix, string &entity, string &name) {
	auto marker = path.find("/v1/");
	if (marker == string::npos) {
		return false;
	}
	auto parts = StringUtil::Split(path.substr(marker + 4), '/');
	if (parts.size() != 4 || parts[1] != "metadata") {
		return false;
	}
	prefix = parts[0];
	entity = parts[2];
	name = parts[3];
	auto query = name.find('?');
	if (query != string::npos) {
		name = name.substr(0, query);
	}
	return true;
}

bool IcebergVirtualFiles::IsVirtual(const string &path) {
	string prefix, entity, name;
	return (StringUtil::StartsWith(path, "http://") || StringUtil::StartsWith(path, "https://")) &&
	       Parse(path, prefix, entity, name);
}

bool IcebergVirtualFiles::CheckSignature(DuckLakeIcebergServer &server, const string &prefix, const string &entity,
                                         const string &name, const string &signature) {
	return !signature.empty() && signature == Sign(server, prefix, entity, name);
}

//! The snapshot in the name of a file
static idx_t SnapshotOfName(const string &name) {
	auto parts = StringUtil::Split(name, '-');
	if (parts.size() < 2) {
		throw IcebergException(404, "NoSuchFileException", "The file does not exist");
	}
	// inlined-delete-<snapshot>-... has the snapshot after two words
	auto index = parts[0] == "inlined" && parts[1] == "delete" ? 2 : 1;
	try {
		return std::stoull(parts[index]);
	} catch (...) {
		throw IcebergException(404, "NoSuchFileException", "The file does not exist");
	}
}

string IcebergVirtualFiles::Generate(IcebergRequest &request, const string &entity, const string &name) {
	auto cache_key = request.prefix + "/" + entity + "/" + name;
	string cached;
	if (request.server.GetExportCache().GetBytes(cache_key, cached)) {
		return cached;
	}
	idx_t id = 0;
	try {
		id = std::stoull(entity.substr(1));
	} catch (...) {
		throw IcebergException(404, "NoSuchFileException", "The file does not exist");
	}
	string result;
	if (StringUtil::StartsWith(entity, "t")) {
		try {
			request.UseSnapshot(SnapshotOfName(name));
		} catch (InvalidInputException &) {
			// the snapshot does not exist (anymore)
			throw IcebergException(404, "NoSuchFileException", "The file does not exist: " + name);
		}
		auto &info = request.GetCatalogInfo();
		for (auto &table : info.tables) {
			if (table.id.index != id) {
				continue;
			}
			auto schema = request.GetSchemaById(table.schema_id);
			if (!schema) {
				break;
			}
			IcebergTableExporter exporter(request);
			result = exporter.GenerateFile(*schema, table, name);
			request.server.GetExportCache().PutBytes(cache_key, result);
			return result;
		}
	} else if (StringUtil::StartsWith(entity, "v")) {
		for (auto &view : request.GetCatalogInfo().views) {
			if (view.id.index != id) {
				continue;
			}
			auto schema = request.GetSchemaById(view.schema_id);
			if (!schema) {
				break;
			}
			return IcebergViewEndpoints::GenerateMetadata(request, *schema, view);
		}
	}
	throw IcebergException(404, "NoSuchFileException", "The file does not exist: " + name);
}

string IcebergVirtualFiles::Read(IcebergRequest &request, const string &path) {
	string prefix, entity, name;
	if ((StringUtil::StartsWith(path, "http://") || StringUtil::StartsWith(path, "https://")) &&
	    Parse(path, prefix, entity, name) && prefix == request.prefix) {
		// the file is generated in a transaction of its own: it reads the catalog at the snapshot of its name
		IcebergRequest generator(request.server, request.db, request.catalog_name, request.prefix);
		generator.base_uri = request.base_uri;
		auto result = Generate(generator, entity, name);
		generator.connection.Rollback();
		return result;
	}
	return IcebergUtil::ReadFile(*request.connection.context, path);
}

} // namespace duckdb
