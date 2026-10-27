#include "iceberg/iceberg_table_update.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "iceberg/iceberg_avro.hpp"
#include "iceberg/iceberg_properties.hpp"
#include "iceberg/iceberg_table_export.hpp"
#include "iceberg/iceberg_types.hpp"
#include "iceberg/iceberg_util.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
IcebergException BadRequest(const string &message) {
	return IcebergException(400, "BadRequestException", message);
}

IcebergException CommitFailed(const string &message) {
	return IcebergException(409, "CommitFailedException", message);
}

IcebergException Unsupported(const string &message) {
	return IcebergException(501, "NotImplementedException", message);
}

string NormalizePath(string path) {
	if (StringUtil::StartsWith(path, "file://")) {
		path = path.substr(7);
	} else if (StringUtil::StartsWith(path, "file:")) {
		path = path.substr(5);
	}
	return path;
}

//! The location that clients can read a file from
string ReadablePath(const string &path) {
	return NormalizePath(path);
}

struct IcebergFileEntry {
	idx_t content = 0;
	string path;
	idx_t record_count = 0;
	idx_t file_size = 0;
	//! The values of the partition fields of the file
	vector<AvroValue> partition;
};

//! A field of the partitioning of a table
struct PartitionLayoutField {
	string column_name;
	string transform;
	LogicalType column_type;
};

//! The files of a snapshot, read from its manifest list and manifests
vector<IcebergFileEntry> ReadSnapshotFiles(ClientContext &context, const string &manifest_list_path) {
	vector<IcebergFileEntry> result;
	auto list = AvroReader::Read(IcebergUtil::ReadFile(context, ReadablePath(manifest_list_path)));
	for (auto &manifest : list.records) {
		auto manifest_path = manifest.Get("manifest_path").AsString();
		auto &content_value = manifest.TryGet("content");
		idx_t manifest_content = content_value.IsNull() ? 0 : NumericCast<idx_t>(content_value.AsLong());
		auto manifest_file = AvroReader::Read(IcebergUtil::ReadFile(context, ReadablePath(manifest_path)));
		for (auto &entry : manifest_file.records) {
			auto status = entry.Get("status").AsLong();
			if (status == 2) {
				continue;
			}
			auto &data_file = entry.Get("data_file");
			IcebergFileEntry file;
			auto &file_content = data_file.TryGet("content");
			file.content = file_content.IsNull() ? manifest_content : NumericCast<idx_t>(file_content.AsLong());
			file.path = data_file.Get("file_path").AsString();
			auto &format = data_file.Get("file_format").AsString();
			if (!StringUtil::CIEquals(format, "parquet")) {
				throw Unsupported(
				    StringUtil::Format("The file format %s of \"%s\" is not supported", format, file.path));
			}
			file.record_count = NumericCast<idx_t>(data_file.Get("record_count").AsLong());
			file.file_size = NumericCast<idx_t>(data_file.Get("file_size_in_bytes").AsLong());
			auto &partition = data_file.TryGet("partition").Unwrap();
			if (partition.kind == AvroValueKind::RECORD) {
				for (auto &field : partition.children) {
					file.partition.push_back(field.Unwrap());
				}
			}
			result.push_back(std::move(file));
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// The state of a table
//===--------------------------------------------------------------------===//
//! The schema argument of the table functions of DuckLake for a namespace: the levels are separated by dots, a dot in
//! a name is written twice
string SchemaFunctionArgument(const vector<string> &ns) {
	vector<string> levels;
	for (auto &level : ns) {
		levels.push_back(StringUtil::Replace(level, ".", ".."));
	}
	return StringUtil::Join(levels, ".");
}

//! The names of the keys of the partitions of DuckLake
vector<string> PartitionKeyNames(const vector<PartitionLayoutField> &layout) {
	vector<string> result;
	case_insensitive_set_t used_names;
	for (auto &field : layout) {
		auto prefix = field.transform == "identity" ? field.column_name : field.transform;
		string name = prefix;
		if (used_names.find(name) != used_names.end()) {
			string base_name = prefix + "_" + field.column_name;
			name = base_name;
			idx_t counter = 2;
			while (used_names.find(name) != used_names.end()) {
				name = base_name + "_" + to_string(counter++);
			}
		}
		used_names.insert(name);
		result.push_back(name);
	}
	return result;
}

//! The text of a value of a partition field of an Iceberg file, as DuckLake reads it from a partition key
bool PartitionValueText(const AvroValue &value, const PartitionLayoutField &field, string &result) {
	if (value.IsNull()) {
		return false;
	}
	if (field.transform != "identity") {
		result = to_string(value.AsLong());
		return true;
	}
	switch (field.column_type.id()) {
	case LogicalTypeId::DATE:
		result = Value::DATE(date_t(NumericCast<int32_t>(value.AsLong()))).ToString();
		return true;
	case LogicalTypeId::TIME:
		result = Value::TIME(dtime_t(value.AsLong())).ToString();
		return true;
	case LogicalTypeId::TIMESTAMP:
		result = Value::TIMESTAMP(timestamp_t(value.AsLong())).ToString();
		return true;
	case LogicalTypeId::TIMESTAMP_TZ:
		result = Value::TIMESTAMPTZ(timestamp_tz_t(value.AsLong())).ToString();
		return true;
	case LogicalTypeId::BOOLEAN:
		result = value.AsBool() ? "true" : "false";
		return true;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		result = Value::DOUBLE(value.AsDouble()).ToString();
		return true;
	case LogicalTypeId::UUID: {
		auto &bytes = value.AsString();
		if (bytes.size() != 16) {
			return false;
		}
		hugeint_t uuid;
		uuid.upper = 0;
		uuid.lower = 0;
		for (idx_t i = 0; i < 8; i++) {
			uuid.upper = (uuid.upper << 8) | static_cast<uint8_t>(bytes[i]);
			uuid.lower = (uuid.lower << 8) | static_cast<uint8_t>(bytes[8 + i]);
		}
		uuid.upper ^= static_cast<int64_t>(static_cast<uint64_t>(1) << 63);
		result = UUID::ToString(uuid);
		return true;
	}
	default:
		if (value.Unwrap().kind == AvroValueKind::STRING) {
			result = value.AsString();
		} else {
			result = to_string(value.AsLong());
		}
		return true;
	}
}

struct LiveDataFile {
	idx_t id;
	string path;
	idx_t record_count;
	idx_t file_size;
	optional_idx delete_file_id;
	string delete_path;
	string delete_format;
};

//! The properties and references that a request sets, which are written after the commit
struct PendingProperties {
	string table_uuid;
	map<string, string> properties;
	vector<string> property_removals;
	map<string, string> references;
	vector<string> reference_removals;
};

class TableUpdate {
public:
	TableUpdate(IcebergRequest &request_p, vector<string> ns_p, string name_p)
	    : request(request_p), context(*request_p.connection.context), ns(std::move(ns_p)), name(std::move(name_p)) {
	}

	//! Applies the requirements and updates of a request for a table
	void Apply(RESTJsonIn requirements, RESTJsonIn updates);
	//! Makes the table have the files of a snapshot
	void ApplyManifestList(const string &manifest_list_path);
	//! The expressions of the partitioning of the table that a spec describes
	static vector<string> PartitionExpressions(const IcebergSchema &schema, RESTJsonIn spec);

	PendingProperties &Pending() {
		return pending;
	}
	const vector<string> &Namespace() const {
		return ns;
	}
	const string &Name() const {
		return name;
	}

private:
	void Reload();
	void CheckRequirements(RESTJsonIn requirements);
	void ApplySchema(const IcebergSchema &target);
	void DiffFields(const vector<IcebergField> &current, const vector<IcebergField> &target, const string &parent_path);
	void ApplyPartitionSpec(const IcebergSchema &schema, RESTJsonIn spec);
	void ApplySnapshot(const string &manifest_list_path, const vector<PartitionLayoutField> &layout);
	vector<PartitionLayoutField> GetPartitionLayout();
	vector<LiveDataFile> GetLiveFiles();
	string ResolvePath(const string &path, bool relative) const;
	string ColumnPath(const string &parent, const string &child) const;

private:
	IcebergRequest &request;
	ClientContext &context;
	vector<string> ns;
	string name;
	optional_ptr<const DuckLakeSchemaInfo> schema;
	optional_ptr<const DuckLakeTableInfo> table;
	IcebergTableLoad state;
	PendingProperties pending;
};

string TableUpdate::ColumnPath(const string &parent, const string &child) const {
	auto quoted = KeywordHelper::WriteQuoted(child, '"');
	return parent.empty() ? quoted : parent + "." + quoted;
}

void TableUpdate::Reload() {
	request.InvalidateCatalogInfo();
	schema = request.FindSchema(ns);
	if (!schema) {
		throw IcebergException(404, "NoSuchNamespaceException",
		                       StringUtil::Format("Namespace does not exist: %s", IcebergUtil::NamespaceToString(ns)));
	}
	table = request.FindTable(*schema, name);
	if (!table) {
		throw IcebergException(
		    404, "NoSuchTableException",
		    StringUtil::Format("Table does not exist: %s.%s", IcebergUtil::NamespaceToString(ns), name));
	}
	IcebergTableExporter exporter(request);
	state = exporter.GetState(*schema, *table);
	pending.table_uuid = table->uuid;
}

void TableUpdate::CheckRequirements(RESTJsonIn requirements) {
	if (!requirements.IsArray()) {
		return;
	}
	for (auto &requirement : requirements.Items()) {
		auto type = requirement.Str("type");
		if (type == "assert-create") {
			if (state.current_snapshot_id.IsValid()) {
				throw CommitFailed("Table already exists");
			}
		} else if (type == "assert-table-uuid") {
			if (!StringUtil::CIEquals(requirement.Str("uuid"), state.table_uuid)) {
				throw CommitFailed(StringUtil::Format("Requirement failed: UUID does not match: expected %s but was %s",
				                                      requirement.Str("uuid"), state.table_uuid));
			}
		} else if (type == "assert-ref-snapshot-id") {
			auto ref = requirement.Str("ref");
			auto expected = requirement.TryGet("snapshot-id");
			optional_idx actual;
			if (ref == "main") {
				actual = state.current_snapshot_id;
			} else {
				auto references = IcebergProperties::Get(request, IcebergProperties::REFERENCE, state.table_uuid);
				auto entry = references.find(ref);
				if (entry != references.end()) {
					RESTJsonReader reader(entry->second);
					actual = reader.Root().UInt("snapshot-id");
				}
			}
			if (expected.IsNull()) {
				if (actual.IsValid()) {
					throw CommitFailed(
					    StringUtil::Format("Requirement failed: branch %s was created concurrently", ref));
				}
			} else if (!actual.IsValid() || actual.GetIndex() != expected.UInt()) {
				throw CommitFailed(StringUtil::Format("Requirement failed: branch %s has changed: expected id %llu",
				                                      ref, expected.UInt()));
			}
		} else if (type == "assert-last-assigned-field-id") {
			if (requirement.UInt("last-assigned-field-id") != state.last_column_id) {
				throw CommitFailed("Requirement failed: last assigned field id changed");
			}
		} else if (type == "assert-current-schema-id") {
			if (requirement.UInt("current-schema-id") != state.current_schema_id) {
				throw CommitFailed("Requirement failed: current schema changed");
			}
		} else if (type == "assert-last-assigned-partition-id") {
			if (requirement.UInt("last-assigned-partition-id") != state.last_partition_id) {
				throw CommitFailed("Requirement failed: last assigned partition id changed");
			}
		} else if (type == "assert-default-spec-id") {
			if (requirement.UInt("default-spec-id") != state.default_spec_id) {
				throw CommitFailed("Requirement failed: default partition spec changed");
			}
		} else if (type == "assert-default-sort-order-id") {
			if (requirement.UInt("default-sort-order-id") != 0) {
				throw CommitFailed("Requirement failed: default sort order changed");
			}
		} else {
			throw BadRequest(StringUtil::Format("Unknown requirement \"%s\"", type));
		}
	}
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
static const IcebergField *FindById(const vector<IcebergField> &fields, idx_t id) {
	for (auto &field : fields) {
		if (field.id == id) {
			return &field;
		}
	}
	return nullptr;
}

static bool IsStruct(const IcebergField &field) {
	return field.type.kind == IcebergType::Kind::STRUCT;
}

void TableUpdate::DiffFields(const vector<IcebergField> &current, const vector<IcebergField> &target,
                             const string &parent_path) {
	auto table_sql = request.EntrySQL(ns, name);
	// drop the fields that are gone
	for (auto &field : current) {
		if (!FindById(target, field.id)) {
			request.Query(
			    StringUtil::Format("ALTER TABLE %s DROP COLUMN %s", table_sql, ColumnPath(parent_path, field.name)));
		}
	}
	for (auto &field : target) {
		auto existing = FindById(current, field.id);
		if (!existing) {
			continue;
		}
		string path = ColumnPath(parent_path, existing->name);
		if (existing->name != field.name) {
			request.Query(StringUtil::Format("ALTER TABLE %s RENAME COLUMN %s TO %s", table_sql, path,
			                                 KeywordHelper::WriteQuoted(field.name, '"')));
			path = ColumnPath(parent_path, field.name);
		}
		if (IsStruct(*existing) && IsStruct(field)) {
			DiffFields(existing->type.fields, field.type.fields, path);
		} else if (existing->type.kind != field.type.kind) {
			throw Unsupported(StringUtil::Format("Changing the type of column \"%s\" is not supported", field.name));
		} else if (existing->type.kind == IcebergType::Kind::PRIMITIVE) {
			if (existing->type.primitive != field.type.primitive) {
				request.Query(StringUtil::Format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", table_sql, path,
				                                 IcebergTypes::ToSQLType(field.type)));
			}
		} else if (IcebergTypes::ToSQLType(existing->type) != IcebergTypes::ToSQLType(field.type)) {
			throw Unsupported(
			    StringUtil::Format("Changing the type of the nested column \"%s\" is not supported", field.name));
		}
		if (parent_path.empty() && existing->required != field.required) {
			request.Query(StringUtil::Format("ALTER TABLE %s ALTER COLUMN %s %s NOT NULL", table_sql, path,
			                                 field.required ? "SET" : "DROP"));
		}
	}
	// add the new fields
	for (auto &field : target) {
		if (FindById(current, field.id)) {
			continue;
		}
		request.Query(StringUtil::Format("ALTER TABLE %s ADD COLUMN %s %s", table_sql,
		                                 ColumnPath(parent_path, field.name), IcebergTypes::ToSQLType(field.type)));
	}
}

void TableUpdate::ApplySchema(const IcebergSchema &target) {
	// the schema that the table has now
	auto columns = request.GetMetadataManager().GetCatalogForSnapshot(request.GetSnapshot());
	IcebergSchema current;
	for (auto &candidate : columns.tables) {
		if (candidate.id == table->id) {
			current = IcebergTypes::FromColumns(candidate.columns, 0);
		}
	}
	if (current.SameFields(target)) {
		return;
	}
	DiffFields(current.fields, target.fields, string());
}

//===--------------------------------------------------------------------===//
// Partitioning
//===--------------------------------------------------------------------===//
vector<string> TableUpdate::PartitionExpressions(const IcebergSchema &schema_p, RESTJsonIn spec) {
	vector<string> expressions;
	for (auto &field : spec.Get("fields").Items()) {
		auto source_id = field.UInt("source-id");
		auto transform = field.Str("transform");
		auto source = schema_p.FindField(source_id);
		if (!source) {
			throw BadRequest(StringUtil::Format("The partition field refers to the unknown field %llu", source_id));
		}
		auto column = KeywordHelper::WriteQuoted(source->name, '"');
		if (transform == "identity") {
			expressions.push_back(column);
		} else if (transform == "year" || transform == "month" || transform == "day" || transform == "hour") {
			expressions.push_back("epoch_" + transform + "(" + column + ")");
		} else if (StringUtil::StartsWith(transform, "bucket[") && StringUtil::EndsWith(transform, "]")) {
			expressions.push_back(
			    StringUtil::Format("bucket(%s, %s)", transform.substr(7, transform.size() - 8), column));
		} else {
			throw Unsupported(StringUtil::Format("The partition transform \"%s\" is not supported", transform));
		}
	}
	return expressions;
}

void TableUpdate::ApplyPartitionSpec(const IcebergSchema &schema_p, RESTJsonIn spec) {
	auto expressions = PartitionExpressions(schema_p, spec);
	auto table_sql = request.EntrySQL(ns, name);
	// the partitioning that the table has now
	auto current = request.MetadataQuery(StringUtil::Format(
	    "SELECT c.column_id, c.transform FROM {METADATA_CATALOG}.ducklake_partition_info p JOIN "
	    "{METADATA_CATALOG}.ducklake_partition_column c ON p.partition_id = c.partition_id AND p.table_id = "
	    "c.table_id WHERE p.table_id = %d AND p.end_snapshot IS NULL ORDER BY c.partition_key_index",
	    table->id.index));
	vector<string> current_fields;
	for (auto &row : *current) {
		current_fields.push_back(StringUtil::Format("%d:%s", row.GetValue<idx_t>(0), row.GetValue<string>(1)));
	}
	vector<string> wanted_fields;
	for (auto &field : spec.Get("fields").Items()) {
		auto source = schema_p.FindField(field.UInt("source-id"));
		auto transform = field.Str("transform");
		string ducklake_transform = transform == "identity" ? "identity" : "epoch_" + transform;
		if (StringUtil::StartsWith(transform, "bucket[")) {
			ducklake_transform = "bucket(" + transform.substr(7, transform.size() - 8) + ")";
		}
		// the id of the column in the table, which is 0 for a column that the table does not have yet
		idx_t column_id = 0;
		for (auto &candidate : request.GetCatalogInfo().tables) {
			if (candidate.id == table->id) {
				for (auto &column : candidate.columns) {
					if (StringUtil::CIEquals(column.name, source->name)) {
						column_id = column.id.index;
					}
				}
			}
		}
		wanted_fields.push_back(StringUtil::Format("%d:%s", column_id, ducklake_transform));
	}
	if (current_fields == wanted_fields) {
		return;
	}
	if (expressions.empty()) {
		request.Query(StringUtil::Format("ALTER TABLE %s RESET PARTITIONED BY", table_sql));
	} else {
		request.Query(StringUtil::Format("ALTER TABLE %s SET PARTITIONED BY (%s)", table_sql,
		                                 StringUtil::Join(expressions, ", ")));
	}
}

//===--------------------------------------------------------------------===//
// Data
//===--------------------------------------------------------------------===//
string TableUpdate::ResolvePath(const string &path, bool relative) const {
	if (!relative) {
		return path;
	}
	return IcebergUtil::StripTrailingSlash(table->path) + "/" + path;
}

vector<LiveDataFile> TableUpdate::GetLiveFiles() {
	vector<LiveDataFile> result;
	map<idx_t, idx_t> index_by_id;
	auto files = request.MetadataQuery(StringUtil::Format(
	    "SELECT data_file_id, path, path_is_relative, record_count, file_size_bytes FROM "
	    "{METADATA_CATALOG}.ducklake_data_file WHERE table_id = %d AND end_snapshot IS NULL ORDER BY data_file_id",
	    table->id.index));
	for (auto &row : *files) {
		LiveDataFile file;
		file.id = row.GetValue<idx_t>(0);
		file.path = ResolvePath(row.GetValue<string>(1), row.IsNull(2) ? true : row.GetValue<bool>(2));
		file.record_count = row.GetValue<idx_t>(3);
		file.file_size = row.GetValue<idx_t>(4);
		index_by_id[file.id] = result.size();
		result.push_back(std::move(file));
	}
	auto deletes = request.MetadataQuery(
	    StringUtil::Format("SELECT delete_file_id, data_file_id, path, path_is_relative, format FROM "
	                       "{METADATA_CATALOG}.ducklake_delete_file WHERE table_id = %d AND end_snapshot IS NULL",
	                       table->id.index));
	for (auto &row : *deletes) {
		auto entry = index_by_id.find(row.GetValue<idx_t>(1));
		if (entry == index_by_id.end()) {
			continue;
		}
		auto &file = result[entry->second];
		file.delete_file_id = row.GetValue<idx_t>(0);
		file.delete_path = ResolvePath(row.GetValue<string>(2), row.IsNull(3) ? true : row.GetValue<bool>(3));
		file.delete_format = row.IsNull(4) ? "parquet" : row.GetValue<string>(4);
	}
	return result;
}

static set<idx_t> ReadPositions(IcebergRequest &request, const string &delete_path, const string &data_path,
                                bool filter_by_path) {
	set<idx_t> positions;
	string query = StringUtil::Format("SELECT pos FROM read_parquet(%s)", SQLString(delete_path));
	if (filter_by_path) {
		query += StringUtil::Format(" WHERE file_path = %s", SQLString(data_path));
	}
	auto result = request.Query(query);
	for (auto &row : *result) {
		positions.insert(row.GetValue<idx_t>(0));
	}
	return positions;
}

void TableUpdate::ApplySnapshot(const string &manifest_list_path, const vector<PartitionLayoutField> &layout) {
	auto files = ReadSnapshotFiles(context, manifest_list_path);
	auto live = GetLiveFiles();
	map<string, idx_t> live_by_path;
	for (idx_t i = 0; i < live.size(); i++) {
		live_by_path[NormalizePath(live[i].path)] = i;
	}
	set<string> wanted_data;
	vector<const IcebergFileEntry *> added_files;
	vector<IcebergFileEntry> delete_entries;
	for (auto &file : files) {
		if (file.content == 0) {
			auto path = NormalizePath(file.path);
			wanted_data.insert(path);
			if (live_by_path.find(path) == live_by_path.end()) {
				added_files.push_back(&file);
			}
		} else if (file.content == 1) {
			delete_entries.push_back(file);
		} else {
			throw Unsupported("Equality delete files are not supported");
		}
	}
	// the data files that are not part of the snapshot anymore
	for (auto &file : live) {
		if (wanted_data.find(NormalizePath(file.path)) == wanted_data.end()) {
			request.transaction->DropFile(table->id, DataFileIndex(file.id), file.path, file.record_count,
			                              file.file_size);
		}
	}
	// the deletes of the data files that stay
	map<string, set<idx_t>> wanted_deletes;
	map<string, vector<string>> delete_paths_of_file;
	for (auto &entry : delete_entries) {
		auto delete_path = ReadablePath(entry.path);
		auto rows =
		    request.Query(StringUtil::Format("SELECT file_path, pos FROM read_parquet(%s)", SQLString(delete_path)));
		for (auto &row : *rows) {
			auto data_path = NormalizePath(row.GetValue<string>(0));
			wanted_deletes[data_path].insert(row.GetValue<idx_t>(1));
		}
	}
	vector<DuckLakeDeleteFile> new_deletes;
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &deletes : wanted_deletes) {
		auto entry = live_by_path.find(deletes.first);
		if (entry == live_by_path.end()) {
			if (wanted_data.find(deletes.first) != wanted_data.end()) {
				throw Unsupported("Deletes of data files that are added in the same commit are not supported");
			}
			continue;
		}
		auto &file = live[entry->second];
		if (wanted_data.find(deletes.first) == wanted_data.end()) {
			continue;
		}
		set<idx_t> existing;
		if (file.delete_file_id.IsValid()) {
			if (file.delete_format != "parquet") {
				throw Unsupported("Data files with deletion vectors cannot be changed");
			}
			existing = ReadPositions(request, file.delete_path, file.path, false);
		}
		if (existing == deletes.second) {
			continue;
		}
		WriteDeleteFileInput input {context,
		                            *request.transaction,
		                            fs,
		                            IcebergUtil::StripTrailingSlash(table->path) + "/",
		                            string(),
		                            file.path,
		                            deletes.second,
		                            DeleteFileSource::REGULAR};
		auto delete_file = DuckLakeDeleteFileWriter::WriteDeleteFile(context, input);
		delete_file.data_file_id = DataFileIndex(file.id);
		if (file.delete_file_id.IsValid()) {
			delete_file.overwrites_existing_delete = true;
			delete_file.overwritten_delete_file.delete_file_id = DataFileIndex(file.delete_file_id.GetIndex());
			delete_file.overwritten_delete_file.path = file.delete_path;
		}
		new_deletes.push_back(std::move(delete_file));
	}
	// a data file that lost all its deletes
	for (auto &file : live) {
		if (!file.delete_file_id.IsValid() || wanted_data.find(NormalizePath(file.path)) == wanted_data.end()) {
			continue;
		}
		if (wanted_deletes.find(NormalizePath(file.path)) == wanted_deletes.end()) {
			throw Unsupported("Removing the deletes of a data file is not supported");
		}
	}
	if (!new_deletes.empty()) {
		request.transaction->AddDeletes(table->id, std::move(new_deletes));
	}
	// the files are added one by one when they have partition values
	auto key_names = PartitionKeyNames(layout);
	vector<string> unpartitioned_paths;
	for (auto file : added_files) {
		auto path = SQLString::ToString(NormalizePath(file->path));
		if (layout.empty()) {
			unpartitioned_paths.push_back(path);
			continue;
		}
		if (file->partition.size() != layout.size()) {
			throw BadRequest(StringUtil::Format("The partition of the file \"%s\" does not match the partitioning of "
			                                    "the table",
			                                    file->path));
		}
		vector<string> keys, texts;
		for (idx_t i = 0; i < layout.size(); i++) {
			string text;
			if (!PartitionValueText(file->partition[i], layout[i], text)) {
				continue;
			}
			keys.push_back(SQLString::ToString(key_names[i]));
			texts.push_back(SQLString::ToString(text));
		}
		request.Query(StringUtil::Format(
		    "CALL ducklake_add_data_files(%s, %s, %s, schema => %s, allow_missing => true, ignore_extra_columns => "
		    "true, hive_partitioning => false, partition_values => MAP([%s], [%s]))",
		    SQLString::ToString(request.catalog_name), SQLString::ToString(name), path,
		    SQLString::ToString(SchemaFunctionArgument(ns)), StringUtil::Join(keys, ", "),
		    StringUtil::Join(texts, ", ")));
	}
	if (!unpartitioned_paths.empty()) {
		request.Query(StringUtil::Format(
		    "CALL ducklake_add_data_files(%s, %s, [%s], schema => %s, allow_missing => true, ignore_extra_columns => "
		    "true)",
		    SQLString::ToString(request.catalog_name), SQLString::ToString(name),
		    StringUtil::Join(unpartitioned_paths, ", "), SQLString::ToString(SchemaFunctionArgument(ns))));
	}
}

//===--------------------------------------------------------------------===//
// Updates
//===--------------------------------------------------------------------===//
vector<PartitionLayoutField> TableUpdate::GetPartitionLayout() {
	vector<PartitionLayoutField> result;
	auto layout = request.MetadataQuery(StringUtil::Format(
	    "SELECT col.column_name, col.column_type, c.transform FROM {METADATA_CATALOG}.ducklake_partition_info p JOIN "
	    "{METADATA_CATALOG}.ducklake_partition_column c ON p.partition_id = c.partition_id AND p.table_id = "
	    "c.table_id JOIN {METADATA_CATALOG}.ducklake_column col ON col.table_id = c.table_id AND col.column_id = "
	    "c.column_id AND col.end_snapshot IS NULL WHERE p.table_id = %d AND p.end_snapshot IS NULL ORDER BY "
	    "c.partition_key_index",
	    table->id.index));
	for (auto &row : *layout) {
		PartitionLayoutField field;
		field.column_name = row.GetValue<string>(0);
		field.column_type = DuckLakeTypes::FromString(row.GetValue<string>(1));
		field.transform = row.GetValue<string>(2);
		result.push_back(std::move(field));
	}
	return result;
}

void TableUpdate::ApplyManifestList(const string &manifest_list_path) {
	Reload();
	ApplySnapshot(manifest_list_path, GetPartitionLayout());
}

void TableUpdate::Apply(RESTJsonIn requirements, RESTJsonIn updates) {
	Reload();
	CheckRequirements(requirements);
	if (!updates.IsArray()) {
		return;
	}
	vector<IcebergSchema> added_schemas;
	map<idx_t, IcebergSchema> known_schemas;
	for (auto &known : state.schemas) {
		known_schemas[known.schema_id] = known;
	}
	vector<RESTJsonIn> added_specs;
	optional_idx main_snapshot;
	map<idx_t, string> added_snapshot_lists;
	optional_idx last_added_snapshot;
	// the schema that is current at the end of the updates
	bool has_current_schema = false;
	IcebergSchema current_schema;
	bool has_default_spec = false;
	RESTJsonIn default_spec;
	for (auto &update : updates.Items()) {
		auto action = update.Str("action");
		if (action == "assign-uuid" || action == "upgrade-format-version" || action == "set-location" ||
		    action == "add-sort-order" || action == "set-default-sort-order" || action == "set-statistics" ||
		    action == "remove-statistics" || action == "set-partition-statistics" ||
		    action == "remove-partition-statistics" || action == "remove-snapshots" ||
		    action == "remove-partition-specs" || action == "remove-schemas" || action == "enable-row-lineage") {
			// no equivalent in DuckLake: the history of DuckLake is managed with its snapshot functions
			continue;
		} else if (action == "add-schema") {
			auto added = IcebergTypes::SchemaFromJson(update.Get("schema"));
			added_schemas.push_back(added);
		} else if (action == "set-current-schema") {
			auto schema_id = update.Int("schema-id");
			if (schema_id == -1) {
				if (added_schemas.empty()) {
					throw BadRequest("set-current-schema -1 without an added schema");
				}
				current_schema = added_schemas.back();
				has_current_schema = true;
			} else {
				bool found = false;
				for (auto &added : added_schemas) {
					if (NumericCast<int64_t>(added.schema_id) == schema_id) {
						current_schema = added;
						has_current_schema = true;
						found = true;
					}
				}
				if (!found) {
					auto known = known_schemas.find(NumericCast<idx_t>(schema_id));
					if (known == known_schemas.end()) {
						throw BadRequest(StringUtil::Format("Unknown schema id %lld", schema_id));
					}
					current_schema = known->second;
					has_current_schema = true;
				}
			}
		} else if (action == "add-spec") {
			added_specs.push_back(update.Get("spec"));
		} else if (action == "set-default-spec") {
			auto spec_id = update.Int("spec-id");
			if (spec_id == -1) {
				if (added_specs.empty()) {
					throw BadRequest("set-default-spec -1 without an added spec");
				}
				default_spec = added_specs.back();
				has_default_spec = true;
			} else {
				for (auto &added : added_specs) {
					if (NumericCast<int64_t>(added.UInt("spec-id")) == spec_id) {
						default_spec = added;
						has_default_spec = true;
					}
				}
			}
		} else if (action == "set-properties") {
			for (auto &entry : IcebergUtil::ReadStringMap(update.Get("updates"))) {
				pending.properties[entry.first] = entry.second;
			}
		} else if (action == "remove-properties") {
			for (auto &removal : update.Get("removals").Items()) {
				pending.property_removals.push_back(removal.Str());
			}
		} else if (action == "add-snapshot") {
			auto snapshot = update.Get("snapshot");
			auto snapshot_id = snapshot.UInt("snapshot-id");
			added_snapshot_lists[snapshot_id] = snapshot.Str("manifest-list");
			last_added_snapshot = snapshot_id;
		} else if (action == "remove-snapshot-ref") {
			auto ref_name = update.Str("ref-name");
			if (ref_name == "main") {
				throw BadRequest("The main branch cannot be removed");
			}
			pending.reference_removals.push_back(ref_name);
		} else if (action == "set-snapshot-ref") {
			auto ref_name = update.Str("ref-name");
			if (ref_name != "main") {
				// a reference to a snapshot of the table: new snapshots can only be added to the main branch
				RESTJsonWriter ref_writer;
				auto &w = ref_writer.writer;
				auto ref = w.CreateObject();
				ref.Add("snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(update.UInt("snapshot-id"))));
				ref.AddString("type", update.Str("type", "branch"));
				for (auto &key : {"min-snapshots-to-keep", "max-snapshot-age-ms", "max-ref-age-ms"}) {
					if (update.Has(key) && !update.TryGet(key).IsNull()) {
						ref.Add(key, w.CreateSignedInteger(update.Int(key)));
					}
				}
				ref_writer.SetRoot(ref);
				pending.references[ref_name] = ref_writer.ToString();
				continue;
			}
			main_snapshot = update.UInt("snapshot-id");
		} else {
			throw BadRequest(StringUtil::Format("Unknown update action \"%s\"", action));
		}
	}
	if (has_current_schema) {
		ApplySchema(current_schema);
	}
	if (has_default_spec) {
		ApplyPartitionSpec(has_current_schema ? current_schema : state.schemas.back(), default_spec);
	}
	if (!main_snapshot.IsValid() && last_added_snapshot.IsValid()) {
		main_snapshot = last_added_snapshot;
	}
	if (main_snapshot.IsValid()) {
		auto entry = added_snapshot_lists.find(main_snapshot.GetIndex());
		if (entry == added_snapshot_lists.end()) {
			if (state.current_snapshot_id.IsValid() &&
			    state.current_snapshot_id.GetIndex() == main_snapshot.GetIndex()) {
				// the branch stays where it is
			} else {
				throw Unsupported("Moving the main branch to an existing snapshot is not supported");
			}
		} else {
			vector<PartitionLayoutField> layout;
			if (has_default_spec) {
				auto &layout_schema = has_current_schema ? current_schema : state.schemas.back();
				for (auto &field : default_spec.Get("fields").Items()) {
					auto source = layout_schema.FindField(field.UInt("source-id"));
					auto transform = field.Str("transform");
					PartitionLayoutField layout_field;
					layout_field.column_name = source->name;
					layout_field.column_type = IcebergTypes::ToLogicalType(source->type);
					layout_field.transform = transform == "identity" ? "identity" : "epoch_" + transform;
					if (StringUtil::StartsWith(transform, "bucket[")) {
						layout_field.transform = "bucket";
					}
					layout.push_back(std::move(layout_field));
				}
			} else {
				layout = GetPartitionLayout();
			}
			ApplySnapshot(entry->second, layout);
		}
	}
}

//! Commits the transaction of the request and loads the table in a transaction of its own
string RespondWithTable(IcebergRequest &request, const vector<string> &ns, const string &name, bool include_config,
                        PendingProperties *pending = nullptr) {
	request.Commit();
	if (pending) {
		IcebergRequest lookup(request.server, request.db, request.catalog_name, request.prefix);
		auto schema = lookup.FindSchema(ns);
		auto table = schema ? lookup.FindTable(*schema, name) : nullptr;
		if (table) {
			IcebergProperties::Update(lookup, IcebergProperties::TABLE, table->uuid, pending->properties,
			                          pending->property_removals);
			IcebergProperties::Update(lookup, IcebergProperties::REFERENCE, table->uuid, pending->references,
			                          pending->reference_removals);
		}
		lookup.connection.Rollback();
	}
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	auto schema = fresh.FindSchema(ns);
	if (!schema) {
		throw IcebergException(404, "NoSuchNamespaceException", "The namespace does not exist");
	}
	auto table = fresh.FindTable(*schema, name);
	if (!table) {
		throw IcebergException(404, "NoSuchTableException", "The table does not exist");
	}
	if (include_config) {
		// object stores have no directories, but the clients of a local file system expect them
		auto &fs = FileSystem::GetFileSystem(*fresh.connection.context);
		auto location = IcebergUtil::StripTrailingSlash(table->path);
		if (location.find("://") == string::npos) {
			fs.CreateDirectoriesRecursive(location + "/metadata");
			fs.CreateDirectoriesRecursive(location + "/data");
		}
	}
	IcebergTableExporter exporter(fresh);
	auto load = exporter.Export(*schema, *table, "all");
	fresh.connection.Rollback();
	return IcebergLoadTableResponse(load, include_config, {});
}

void CreateTableFromSchema(IcebergRequest &request, const vector<string> &ns, const string &name,
                           const IcebergSchema &schema) {
	vector<string> columns;
	for (auto &field : schema.fields) {
		string column = KeywordHelper::WriteQuoted(field.name, '"') + " " + IcebergTypes::ToSQLType(field.type);
		if (field.required) {
			column += " NOT NULL";
		}
		columns.push_back(std::move(column));
	}
	request.Query(
	    StringUtil::Format("CREATE TABLE %s (%s)", request.EntrySQL(ns, name), StringUtil::Join(columns, ", ")));
	request.InvalidateCatalogInfo();
}

} // namespace

//===--------------------------------------------------------------------===//
// Endpoints
//===--------------------------------------------------------------------===//
void IcebergTableEndpoints::CreateTable(IcebergRequest &request, const IcebergHttpRequest &http,
                                        IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto name = root.Str("name");
	auto schema = request.FindSchema(ns);
	if (!schema) {
		throw IcebergException(404, "NoSuchNamespaceException",
		                       StringUtil::Format("Namespace does not exist: %s", IcebergUtil::NamespaceToString(ns)));
	}
	if (request.FindTable(*schema, name) || request.FindView(*schema, name)) {
		throw IcebergException(
		    409, "AlreadyExistsException",
		    StringUtil::Format("Table already exists: %s.%s", IcebergUtil::NamespaceToString(ns), name));
	}
	auto iceberg_schema = IcebergTypes::SchemaFromJson(root.Get("schema"));
	CreateTableFromSchema(request, ns, name, iceberg_schema);
	auto spec = root.TryGet("partition-spec");
	if (spec.IsObject() && spec.TryGet("fields").IsArray() && !spec.Get("fields").Items().empty()) {
		auto expressions = TableUpdate::PartitionExpressions(iceberg_schema, spec);
		request.Query(StringUtil::Format("ALTER TABLE %s SET PARTITIONED BY (%s)", request.EntrySQL(ns, name),
		                                 StringUtil::Join(expressions, ", ")));
	}
	PendingProperties pending;
	pending.properties = IcebergUtil::ReadStringMap(root.TryGet("properties"));
	response.body = RespondWithTable(request, ns, name, true, &pending);
}

void IcebergTableEndpoints::UpdateTable(IcebergRequest &request, const IcebergHttpRequest &http,
                                        IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto name = http.path_values[1];
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	lock_guard<mutex> guard(request.server.TableLock(IcebergUtil::NamespaceToString(ns) + "." + name));
	TableUpdate update(request, ns, name);
	update.Apply(root.TryGet("requirements"), root.TryGet("updates"));
	response.body = RespondWithTable(request, ns, name, false, &update.Pending());
}

void IcebergTableEndpoints::CommitTransaction(IcebergRequest &request, const IcebergHttpRequest &http,
                                              IcebergHttpResponse &response) {
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	vector<unique_ptr<TableUpdate>> updates;
	vector<RESTJsonIn> changes = root.Get("table-changes").Items();
	// the tables are locked in a fixed order
	map<string, unique_ptr<lock_guard<mutex>>> locks;
	for (auto &change : changes) {
		auto identifier = change.Get("identifier");
		auto ns = IcebergUtil::NamespaceFromJson(identifier.Get("namespace"));
		auto key = IcebergUtil::NamespaceToString(ns) + "." + identifier.Str("name");
		locks[key] = nullptr;
	}
	for (auto &lock : locks) {
		lock.second = make_uniq<lock_guard<mutex>>(request.server.TableLock(lock.first));
	}
	for (auto &change : changes) {
		auto identifier = change.Get("identifier");
		auto ns = IcebergUtil::NamespaceFromJson(identifier.Get("namespace"));
		auto update = make_uniq<TableUpdate>(request, ns, identifier.Str("name"));
		update->Apply(change.TryGet("requirements"), change.TryGet("updates"));
		updates.push_back(std::move(update));
	}
	request.Commit();
	for (auto &update : updates) {
		auto &pending = update->Pending();
		IcebergProperties::Update(request, IcebergProperties::TABLE, pending.table_uuid, pending.properties,
		                          pending.property_removals);
		IcebergProperties::Update(request, IcebergProperties::REFERENCE, pending.table_uuid, pending.references,
		                          pending.reference_removals);
	}
	response.status = 204;
}

void IcebergTableEndpoints::RegisterTable(IcebergRequest &request, const IcebergHttpRequest &http,
                                          IcebergHttpResponse &response) {
	auto ns = IcebergUtil::ParseNamespace(http.path_values[0]);
	auto body = IcebergUtil::ParseBody(http);
	auto root = body->Root();
	auto name = root.Str("name");
	auto metadata_location = root.Str("metadata-location");
	auto schema = request.FindSchema(ns);
	if (!schema) {
		throw IcebergException(404, "NoSuchNamespaceException",
		                       StringUtil::Format("Namespace does not exist: %s", IcebergUtil::NamespaceToString(ns)));
	}
	if (request.FindTable(*schema, name) || request.FindView(*schema, name)) {
		throw IcebergException(
		    409, "AlreadyExistsException",
		    StringUtil::Format("Table already exists: %s.%s", IcebergUtil::NamespaceToString(ns), name));
	}
	auto metadata_text = IcebergUtil::ReadFile(*request.connection.context, ReadablePath(metadata_location));
	RESTJsonReader metadata_reader(metadata_text);
	auto metadata = metadata_reader.Root();
	// the current schema and partition spec of the table
	IcebergSchema current_schema;
	bool found_schema = false;
	if (metadata.Has("schemas")) {
		auto current_id = metadata.UInt("current-schema-id", 0);
		for (auto &candidate : metadata.Get("schemas").Items()) {
			if (candidate.UInt("schema-id", 0) == current_id) {
				current_schema = IcebergTypes::SchemaFromJson(candidate);
				found_schema = true;
			}
		}
	} else if (metadata.Has("schema")) {
		current_schema = IcebergTypes::SchemaFromJson(metadata.Get("schema"));
		found_schema = true;
	}
	if (!found_schema) {
		throw BadRequest("The metadata of the table has no schema");
	}
	CreateTableFromSchema(request, ns, name, current_schema);
	if (metadata.Has("partition-specs")) {
		auto default_id = metadata.UInt("default-spec-id", 0);
		for (auto &spec : metadata.Get("partition-specs").Items()) {
			if (spec.UInt("spec-id", 0) == default_id && spec.Get("fields").IsArray() &&
			    !spec.Get("fields").Items().empty()) {
				auto expressions = TableUpdate::PartitionExpressions(current_schema, spec);
				request.Query(StringUtil::Format("ALTER TABLE %s SET PARTITIONED BY (%s)", request.EntrySQL(ns, name),
				                                 StringUtil::Join(expressions, ", ")));
			}
		}
	}
	string manifest_list;
	auto current_snapshot = metadata.TryGet("current-snapshot-id");
	if (!current_snapshot.IsNull() && current_snapshot.Int() >= 0) {
		for (auto &snapshot : metadata.Get("snapshots").Items()) {
			if (snapshot.Int("snapshot-id") == current_snapshot.Int()) {
				manifest_list = snapshot.Str("manifest-list");
			}
		}
	}
	request.Commit();
	IcebergRequest fresh(request.server, request.db, request.catalog_name, request.prefix);
	if (!manifest_list.empty()) {
		TableUpdate update(fresh, ns, name);
		update.ApplyManifestList(manifest_list);
	}
	response.body = RespondWithTable(fresh, ns, name, true);
}

} // namespace duckdb
