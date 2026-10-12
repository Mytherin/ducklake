//===----------------------------------------------------------------------===//
//                         DuckDB
//
// rest/ducklake_rest_serialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "rest/ducklake_rest_json.hpp"
#include "common/ducklake_snapshot.hpp"
#include "common/ducklake_data_file.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

//! Conversion of the DuckLake metadata structures to and from their REST API JSON representation
struct DuckLakeRESTSerializer {
	static void Write(RESTJsonOut out, const DuckLakeSnapshot &snapshot);
	static DuckLakeSnapshot ReadSnapshot(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeMetadata &metadata);
	static DuckLakeMetadata ReadMetadata(RESTJsonIn in);

	static void WriteTags(RESTJsonOut out, const string &key, const vector<DuckLakeTag> &tags);
	static vector<DuckLakeTag> ReadTags(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeSchemaInfo &schema);
	static DuckLakeSchemaInfo ReadSchemaInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeColumnInfo &column);
	static DuckLakeColumnInfo ReadColumnInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeTableInfo &table);
	static DuckLakeTableInfo ReadTableInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeViewInfo &view);
	static DuckLakeViewInfo ReadViewInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeMacroInfo &macro);
	static DuckLakeMacroInfo ReadMacroInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakePartitionInfo &partition);
	static DuckLakePartitionInfo ReadPartitionInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeSortInfo &sort);
	static DuckLakeSortInfo ReadSortInfo(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeCatalogInfo &catalog);
	static DuckLakeCatalogInfo ReadCatalogInfo(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeGlobalStatsInfo &stats);
	static DuckLakeGlobalStatsInfo ReadGlobalStatsInfo(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeFileData &file);
	static DuckLakeFileData ReadFileData(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeFileListEntry &entry);
	static DuckLakeFileListEntry ReadFileListEntry(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeFileListExtendedEntry &entry);
	static DuckLakeFileListExtendedEntry ReadFileListExtendedEntry(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeColumnStats &stats);
	static DuckLakeColumnStats ReadColumnStats(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeDeleteFile &file);
	static DuckLakeDeleteFile ReadDeleteFile(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeDataFile &file);
	static DuckLakeDataFile ReadDataFile(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeCompactionFileEntry &entry);
	static DuckLakeCompactionFileEntry ReadCompactionFileEntry(RESTJsonIn in);
	static void Write(RESTJsonOut out, const DuckLakeCompactionEntry &entry);
	static DuckLakeCompactionEntry ReadCompactionEntry(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeSnapshotInfo &snapshot);
	static DuckLakeSnapshotInfo ReadSnapshotInfo(RESTJsonIn in);

	static void Write(RESTJsonOut out, const DuckLakeSnapshotCommit &commit);
	static DuckLakeSnapshotCommit ReadSnapshotCommit(RESTJsonIn in);
};

} // namespace duckdb
