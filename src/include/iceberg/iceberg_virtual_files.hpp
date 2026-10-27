//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg/iceberg_virtual_files.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "iceberg/iceberg_server.hpp"

namespace duckdb {

//! The files of the Iceberg metadata of DuckLake (table metadata, manifest lists, manifests, and the Parquet files with
//! inlined data and deletes) do not exist. Their URLs are addresses of the server, and the server generates a file
//! from the DuckLake catalog when it is requested. The name of a file has the information to generate it:
//!   <base>/v1/<prefix>/metadata/t<table id>/meta-<snapshot>.metadata.json
//!   <base>/v1/<prefix>/metadata/t<table id>/snap-<snapshot>-<previous snapshot | none>.avro
//!   <base>/v1/<prefix>/metadata/t<table id>/m-<snapshot>-<spec>-<m | d>.avro
//!   <base>/v1/<prefix>/metadata/t<table id>/delete-<snapshot>-<delete file id>.parquet
//!   <base>/v1/<prefix>/metadata/t<table id>/inlined-<snapshot>-<schema version>.parquet
//!   <base>/v1/<prefix>/metadata/t<table id>/inlined-delete-<snapshot>-<data file id>.parquet
//!   <base>/v1/<prefix>/metadata/v<view id>/view.metadata.json
class IcebergVirtualFiles {
public:
	//! The URL of a file of an entity ("t<table id>" or "v<view id>")
	static string Url(IcebergRequest &request, const string &entity, const string &name);
	//! A file that was generated is written to the metadata path of the server, unless the files are virtual
	static void Publish(IcebergRequest &request, const string &entity, const string &name, const string &content);
	//! Whether a path is a generated file: the URL of a generated file, or a file in the metadata path
	static bool IsGenerated(IcebergRequest &request, const string &path);
	//! Whether a path is the URL of a generated file of a server
	static bool IsVirtual(const string &path);
	//! Splits the URL of a generated file
	static bool Parse(const string &path, string &prefix, string &entity, string &name);
	//! Whether the signature of the URL is valid (URLs are signed if the server has a token)
	static bool CheckSignature(DuckLakeIcebergServer &server, const string &prefix, const string &entity,
	                           const string &name, const string &signature);
	//! The content of a generated file
	static string Generate(IcebergRequest &request, const string &entity, const string &name);
	//! The content of a file: generated if it is the URL of a generated file, otherwise read from the file system
	static string Read(IcebergRequest &request, const string &path);

	static string Entity(const DuckLakeTableInfo &table);
	static string ViewEntity(const DuckLakeViewInfo &view);
};

} // namespace duckdb
