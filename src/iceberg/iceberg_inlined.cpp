#include "iceberg/iceberg_inlined.hpp"

#include "common/ducklake_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg/iceberg_util.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

unique_ptr<IcebergInlinedExport> IcebergInlinedExport::Create(IcebergRequest &request, const DuckLakeTableInfo &table) {
	unique_ptr<IcebergInlinedExport> result(new IcebergInlinedExport(request, table));
	auto &mm = request.GetMetadataManager();
	auto col_names = mm.InlinedColNames();
	auto names = request.MetadataQuery(StringUtil::Format(
	    "SELECT table_name, schema_version FROM {METADATA_CATALOG}.ducklake_inlined_data_tables WHERE table_id = %d",
	    table.id.index));
	vector<pair<string, idx_t>> inlined_names;
	for (auto &row : *names) {
		inlined_names.emplace_back(row.GetValue<string>(0), row.GetValue<idx_t>(1));
	}
	for (auto &entry : inlined_names) {
		InlinedTable inlined_table;
		inlined_table.name = entry.first;
		inlined_table.schema_version = entry.second;
		auto rows = request.MetadataQuery(StringUtil::Format("SELECT %s, %s FROM {METADATA_CATALOG}.%s",
		                                                     col_names.begin_snapshot, col_names.end_snapshot,
		                                                     KeywordHelper::WriteOptionallyQuoted(entry.first)));
		for (auto &row : *rows) {
			optional_idx end;
			if (!row.IsNull(1)) {
				end = row.GetValue<idx_t>(1);
			}
			inlined_table.lifetimes.emplace_back(row.GetValue<idx_t>(0), end);
		}
		result->tables.push_back(std::move(inlined_table));
	}
	// the file deletions that are inlined
	result->delete_table = StringUtil::Format("ducklake_inlined_delete_%d", table.id.index);
	bool delete_table_exists = mm.InlinedDeletionTableExists(result->delete_table);
	if (delete_table_exists) {
		auto rows = request.MetadataQuery(
		    StringUtil::Format("SELECT begin_snapshot FROM {METADATA_CATALOG}.%s", result->delete_table));
		for (auto &row : *rows) {
			result->delete_snapshots.push_back(row.GetValue<idx_t>(0));
		}
	}
	return result;
}

bool IcebergInlinedExport::HasRows(idx_t snapshot_id) const {
	for (auto &inlined_table : tables) {
		for (auto &lifetime : inlined_table.lifetimes) {
			if (lifetime.first <= snapshot_id &&
			    (!lifetime.second.IsValid() || lifetime.second.GetIndex() > snapshot_id)) {
				return true;
			}
		}
	}
	return false;
}

bool IcebergInlinedExport::HasDeletes(idx_t snapshot_id) const {
	for (auto begin : delete_snapshots) {
		if (begin <= snapshot_id) {
			return true;
		}
	}
	return false;
}

//! The specification of the field ids of a Parquet file for a DuckDB COPY
static string FieldIdsOf(const vector<DuckLakeColumnInfo> &columns);

static string FieldIdOf(const DuckLakeColumnInfo &column) {
	if (column.children.empty()) {
		return to_string(column.id.index);
	}
	return "{'__duckdb_field_id': " + to_string(column.id.index) + ", " + FieldIdsOf(column.children) + "}";
}

static string FieldIdsOf(const vector<DuckLakeColumnInfo> &columns) {
	vector<string> parts;
	for (auto &column : columns) {
		parts.push_back(SQLString(column.name) + ": " + FieldIdOf(column));
	}
	return StringUtil::Join(parts, ", ");
}

vector<IcebergInlinedRows> IcebergInlinedExport::RowsAt(idx_t snapshot_id) const {
	vector<IcebergInlinedRows> result;
	for (auto &inlined_table : tables) {
		IcebergInlinedRows rows;
		rows.schema_version = inlined_table.schema_version;
		for (auto &lifetime : inlined_table.lifetimes) {
			if (lifetime.first <= snapshot_id &&
			    (!lifetime.second.IsValid() || lifetime.second.GetIndex() > snapshot_id)) {
				rows.row_count++;
			}
		}
		if (rows.row_count > 0) {
			result.push_back(rows);
		}
	}
	return result;
}

vector<IcebergInlinedDeletes> IcebergInlinedExport::DeletesAt(idx_t snapshot_id) {
	vector<IcebergInlinedDeletes> result;
	if (!HasDeletes(snapshot_id)) {
		return result;
	}
	auto rows = request.MetadataQuery(StringUtil::Format(
	    "SELECT file_id, COUNT(*) FROM {METADATA_CATALOG}.%s WHERE begin_snapshot <= %d GROUP BY file_id ORDER BY "
	    "file_id",
	    delete_table, snapshot_id));
	for (auto &row : *rows) {
		IcebergInlinedDeletes deletes;
		deletes.data_file_id = row.GetValue<idx_t>(0);
		deletes.delete_count = row.GetValue<idx_t>(1);
		result.push_back(deletes);
	}
	return result;
}

string IcebergInlinedExport::GenerateRows(idx_t snapshot_id, idx_t schema_version,
                                          const vector<DuckLakeColumnInfo> &columns) {
	auto &mm = request.GetMetadataManager();
	auto col_names = mm.InlinedColNames();
	for (auto &inlined_table : tables) {
		if (inlined_table.schema_version != schema_version) {
			continue;
		}
		// the columns of the inlined table by name
		auto description = request.MetadataQuery(StringUtil::Format(
		    "SELECT column_name FROM (DESCRIBE SELECT * FROM {METADATA_CATALOG}.%s)", inlined_table.name));
		case_insensitive_set_t inlined_columns;
		for (auto &row : *description) {
			inlined_columns.insert(row.GetValue<string>(0));
		}
		vector<string> select_list;
		for (auto &column : columns) {
			auto type = DuckLakeTypes::FromColumnInfo(column);
			auto quoted = KeywordHelper::WriteQuoted(column.name, '"');
			if (inlined_columns.find(column.name) != inlined_columns.end()) {
				select_list.push_back(StringUtil::Format("CAST(%s AS %s) AS %s", quoted, type.ToString(), quoted));
			} else {
				select_list.push_back(StringUtil::Format("CAST(NULL AS %s) AS %s", type.ToString(), quoted));
			}
		}
		return IcebergUtil::CopyToBytes(
		    request, StringUtil::Format("COPY (SELECT %s FROM {METADATA_CATALOG}.%s WHERE %d >= %s AND (%d < %s OR %s "
		                                "IS NULL) ORDER BY %s) TO {FILE} (FORMAT PARQUET, FIELD_IDS {%s})",
		                                StringUtil::Join(select_list, ", "), inlined_table.name, snapshot_id,
		                                col_names.begin_snapshot, snapshot_id, col_names.end_snapshot,
		                                col_names.end_snapshot, col_names.row_id, FieldIdsOf(columns)));
	}
	throw IcebergException(404, "NoSuchFileException", "The inlined data does not exist");
}

string IcebergInlinedExport::GenerateDeletes(idx_t snapshot_id, idx_t data_file_id, const string &data_path,
                                             idx_t row_id_start) {
	if (!HasDeletes(snapshot_id)) {
		throw IcebergException(404, "NoSuchFileException", "The inlined deletes do not exist");
	}
	return IcebergUtil::CopyToBytes(
	    request,
	    StringUtil::Format("COPY (SELECT %s AS file_path, row_id - %d AS pos FROM {METADATA_CATALOG}.%s WHERE "
	                       "file_id = %d AND begin_snapshot <= %d ORDER BY row_id) TO {FILE} (FORMAT PARQUET, "
	                       "FIELD_IDS {'file_path': 2147483546, 'pos': 2147483545})",
	                       SQLString::ToString(data_path), row_id_start, delete_table, data_file_id, snapshot_id));
}

} // namespace duckdb
