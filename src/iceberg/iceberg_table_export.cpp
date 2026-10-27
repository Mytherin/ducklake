#include "iceberg/iceberg_table_export.hpp"

#include "common/ducklake_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "iceberg/iceberg_avro.hpp"
#include "iceberg/iceberg_inlined.hpp"
#include "iceberg/iceberg_properties.hpp"
#include "iceberg/iceberg_util.hpp"
#include "iceberg/iceberg_virtual_files.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

bool IcebergExportCache::Get(const string &key, IcebergExportedSnapshot &result) {
	lock_guard<mutex> guard(lock);
	auto entry = entries.find(key);
	if (entry == entries.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

bool IcebergExportCache::GetBytes(const string &key, string &result) {
	lock_guard<mutex> guard(lock);
	auto entry = bytes.find(key);
	if (entry == bytes.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

void IcebergExportCache::PutBytes(const string &key, string content) {
	static constexpr idx_t MAX_BYTES = 256ULL * 1024 * 1024;
	lock_guard<mutex> guard(lock);
	if (content.size() > MAX_BYTES / 4 || bytes.find(key) != bytes.end()) {
		return;
	}
	bytes_size += content.size();
	bytes[key] = std::move(content);
	bytes_order.push_back(key);
	while (bytes_size > MAX_BYTES && !bytes_order.empty()) {
		auto oldest = bytes.find(bytes_order.front());
		if (oldest != bytes.end()) {
			bytes_size -= oldest->second.size();
			bytes.erase(oldest);
		}
		bytes_order.erase(bytes_order.begin());
	}
}

bool IcebergExportCache::GetPlan(const string &plan_id, string &result) {
	lock_guard<mutex> guard(lock);
	auto entry = plans.find(plan_id);
	if (entry == plans.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

void IcebergExportCache::PutPlan(const string &plan_id, string plan) {
	lock_guard<mutex> guard(lock);
	plans[plan_id] = std::move(plan);
	plan_order.push_back(plan_id);
	// the plans are kept for a while
	while (plan_order.size() > 100) {
		plans.erase(plan_order.front());
		plan_order.erase(plan_order.begin());
	}
}

void IcebergExportCache::RemovePlan(const string &plan_id) {
	lock_guard<mutex> guard(lock);
	plans.erase(plan_id);
}

void IcebergExportCache::Put(const string &key, IcebergExportedSnapshot value) {
	lock_guard<mutex> guard(lock);
	entries[key] = std::move(value);
}

namespace {

//===--------------------------------------------------------------------===//
// Rows of the metadata of a table
//===--------------------------------------------------------------------===//
struct ExportColumnRow {
	idx_t id;
	idx_t begin;
	optional_idx end;
	idx_t order;
	string name;
	string type;
	bool nulls_allowed;
	optional_idx parent;
};

struct ExportDataFile {
	idx_t id;
	idx_t begin;
	optional_idx end;
	string path;
	idx_t record_count;
	idx_t file_size;
	optional_idx partition_id;
	optional_idx partial_max;
	idx_t row_id_start = 0;
};

struct ExportDeleteFile {
	idx_t id;
	idx_t begin;
	optional_idx end;
	idx_t data_file_id;
	string path;
	string format;
	idx_t delete_count;
	idx_t file_size;
	optional_idx partial_max;
};

using ExportStats = IcebergFileStats;

struct ExportPartitionField {
	idx_t column_id;
	string transform;
	idx_t key_index;
};

struct ExportPartition {
	idx_t partition_id;
	idx_t begin;
	optional_idx end;
	vector<ExportPartitionField> fields;
};

//! A partition spec of Iceberg, which only has the transforms that DuckLake shares with Iceberg
struct ExportSpecField {
	idx_t source_id;
	string transform;
	idx_t field_id;
	string name;
	//! The index of the value in the partition values of a DuckLake file
	idx_t key_index;
};

struct ExportSortField {
	idx_t source_id;
	string name;
	bool descending;
	bool nulls_first;
};

struct ExportSpec {
	idx_t spec_id;
	vector<ExportSpecField> fields;
};

//! The id of the column with the path of a data file in position delete files
constexpr idx_t POSITION_DELETE_PATH_ID = 2147483546;

struct ManifestItem {
	idx_t begin = 0;
	string path;
	idx_t record_count = 0;
	idx_t file_size = 0;
	//! The data file whose partition values the entry has
	const ExportDataFile *partition_source = nullptr;
	const vector<ExportStats> *stats = nullptr;
	vector<ExportStats> owned_stats;

	const vector<ExportStats> *Stats() const {
		return stats ? stats : (owned_stats.empty() ? nullptr : &owned_stats);
	}
	//! The data file that a delete file refers to
	string referenced_path;
};

//! The files of a snapshot, by the spec of their partition
struct GatheredItems {
	vector<const ExportDataFile *> data_list;
	vector<const ExportDeleteFile *> delete_list;
	idx_t inlined_file_count = 0;
	idx_t inlined_rows = 0;
	idx_t inlined_delete_count = 0;
	map<idx_t, vector<ManifestItem>> data_items;
	map<idx_t, vector<ManifestItem>> delete_items;
	idx_t delete_rows = 0;
};

struct ManifestSummary {
	idx_t length = 0;
	idx_t min_sequence = 0;
	idx_t added_files = 0;
	idx_t existing_files = 0;
	idx_t added_rows = 0;
	idx_t existing_rows = 0;
};

struct ExportSnapshotRow {
	idx_t id;
	int64_t time_ms;
	idx_t schema_version;
};

optional_idx ReadOptIdx(const QueryResultRow &row, idx_t col) {
	if (row.IsNull(col)) {
		return optional_idx();
	}
	return optional_idx(row.GetValue<idx_t>(col));
}

string FormatHash(const string &content) {
	auto hash = std::hash<string>()(content);
	return StringUtil::Format("%016llx", hash);
}

//! The Iceberg transform of a DuckLake transform, empty if Iceberg has no equivalent
string IcebergTransform(const string &ducklake_transform) {
	if (ducklake_transform == "identity") {
		return "identity";
	}
	if (ducklake_transform == "epoch_year") {
		return "year";
	}
	if (ducklake_transform == "epoch_month") {
		return "month";
	}
	if (ducklake_transform == "epoch_day") {
		return "day";
	}
	if (ducklake_transform == "epoch_hour") {
		return "hour";
	}
	if (StringUtil::StartsWith(ducklake_transform, "bucket(") && StringUtil::EndsWith(ducklake_transform, ")")) {
		return "bucket[" + ducklake_transform.substr(7, ducklake_transform.size() - 8) + "]";
	}
	return string();
}

bool IsSupportedIdentityType(const IcebergType &type) {
	if (type.kind != IcebergType::Kind::PRIMITIVE) {
		return false;
	}
	static const char *SUPPORTED[] = {"boolean", "int",       "long",        "float",  "double", "date",
	                                  "time",    "timestamp", "timestamptz", "string", "uuid"};
	for (auto supported : SUPPORTED) {
		if (type.primitive == supported) {
			return true;
		}
	}
	return false;
}

string PartitionFieldName(const string &column_name, const string &transform) {
	if (transform == "identity") {
		return column_name;
	}
	if (StringUtil::StartsWith(transform, "bucket[")) {
		return column_name + "_bucket";
	}
	return column_name + "_" + transform;
}

//! Makes a name compatible with Avro like the Iceberg libraries do
string AvroCompatibleName(const string &name) {
	string result;
	for (idx_t i = 0; i < name.size(); i++) {
		auto c = name[i];
		bool valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (i > 0 && c >= '0' && c <= '9');
		if (valid) {
			result += c;
		} else if (i == 0 && c >= '0' && c <= '9') {
			result += "_" + string(1, c);
		} else {
			result += StringUtil::Format("_x%02X", static_cast<uint8_t>(c));
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Values
//===--------------------------------------------------------------------===//
void AppendLE(string &out, uint64_t value, idx_t bytes) {
	for (idx_t i = 0; i < bytes; i++) {
		out += static_cast<char>((value >> (8 * i)) & 0xFF);
	}
}

//! Serializes a value like the single-value serialization of the Iceberg specification
bool SerializeValue(const Value &value, const IcebergType &type, string &out) {
	if (value.IsNull() || type.kind != IcebergType::Kind::PRIMITIVE) {
		return false;
	}
	auto &name = type.primitive;
	idx_t precision, scale;
	if (name == "boolean") {
		out = string(1, BooleanValue::Get(value) ? '\1' : '\0');
	} else if (name == "int") {
		AppendLE(out, static_cast<uint64_t>(value.GetValue<int32_t>()), 4);
	} else if (name == "long") {
		AppendLE(out, static_cast<uint64_t>(value.GetValue<int64_t>()), 8);
	} else if (name == "float") {
		auto f = value.GetValue<float>();
		uint32_t bits;
		memcpy(&bits, &f, sizeof(bits));
		AppendLE(out, bits, 4);
	} else if (name == "double") {
		auto d = value.GetValue<double>();
		uint64_t bits;
		memcpy(&bits, &d, sizeof(bits));
		AppendLE(out, bits, 8);
	} else if (name == "date") {
		AppendLE(out, static_cast<uint64_t>(static_cast<int64_t>(value.GetValue<int32_t>())), 4);
	} else if (name == "time" || name == "timestamp" || name == "timestamptz" || name == "timestamp_ns") {
		AppendLE(out, static_cast<uint64_t>(value.GetValue<int64_t>()), 8);
	} else if (name == "string") {
		out = StringValue::Get(value);
	} else if (name == "uuid") {
		auto uuid = value.GetValue<hugeint_t>();
		// the UUIDs of DuckDB flip the first bit
		uint64_t upper = static_cast<uint64_t>(uuid.upper) ^ (static_cast<uint64_t>(1) << 63);
		for (int i = 7; i >= 0; i--) {
			out += static_cast<char>((upper >> (8 * i)) & 0xFF);
		}
		for (int i = 7; i >= 0; i--) {
			out += static_cast<char>((uuid.lower >> (8 * i)) & 0xFF);
		}
	} else if (IcebergTypes::ParseDecimal(name, precision, scale)) {
		hugeint_t unscaled;
		switch (value.type().InternalType()) {
		case PhysicalType::INT16:
			unscaled = hugeint_t(value.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			unscaled = hugeint_t(value.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			unscaled = hugeint_t(value.GetValueUnsafe<int64_t>());
			break;
		case PhysicalType::INT128:
			unscaled = value.GetValueUnsafe<hugeint_t>();
			break;
		default:
			return false;
		}
		// the minimal big-endian two's complement representation
		string bytes;
		for (int i = 15; i >= 0; i--) {
			uint64_t half = i >= 8 ? static_cast<uint64_t>(unscaled.upper) : unscaled.lower;
			bytes += static_cast<char>((half >> (8 * (i % 8))) & 0xFF);
		}
		idx_t start = 0;
		bool negative = unscaled.upper < 0;
		while (start + 1 < bytes.size()) {
			auto first = static_cast<uint8_t>(bytes[start]);
			auto second = static_cast<uint8_t>(bytes[start + 1]);
			if ((negative && first == 0xFF && (second & 0x80)) || (!negative && first == 0x00 && !(second & 0x80))) {
				start++;
			} else {
				break;
			}
		}
		out = bytes.substr(start);
	} else {
		return false;
	}
	return true;
}

bool TextToValue(const string &text, const IcebergType &type, Value &result) {
	if (type.kind != IcebergType::Kind::PRIMITIVE) {
		return false;
	}
	try {
		auto logical_type = IcebergTypes::ToLogicalType(type);
		if (logical_type.id() == LogicalTypeId::VARCHAR) {
			result = Value(text);
			return true;
		}
		Value varchar(text);
		auto casted = varchar.DefaultTryCastAs(logical_type);
		if (!casted) {
			return false;
		}
		result = *casted;
		return true;
	} catch (...) {
		return false;
	}
}

//! The JSON representation of a single value in the Iceberg REST API
JSONMutableValue TypedValueToJson(JSONWriter &w, const Value &value, const IcebergType &type) {
	if (value.IsNull() || type.kind != IcebergType::Kind::PRIMITIVE) {
		return w.CreateNull();
	}
	auto &name = type.primitive;
	idx_t precision, scale;
	if (name == "boolean") {
		return w.CreateBoolean(BooleanValue::Get(value));
	} else if (name == "int") {
		return w.CreateSignedInteger(value.GetValue<int32_t>());
	} else if (name == "long") {
		return w.CreateSignedInteger(value.GetValue<int64_t>());
	} else if (name == "float" || name == "double") {
		return w.CreateDouble(value.GetValue<double>());
	} else if (name == "timestamp") {
		return w.CreateString(StringUtil::Replace(value.ToString(), " ", "T"));
	} else if (name == "timestamptz") {
		return w.CreateString(
		    StringUtil::Replace(Value::TIMESTAMP(timestamp_t(value.GetValue<int64_t>())).ToString(), " ", "T") +
		    "+00:00");
	} else if (IcebergTypes::ParseDecimal(name, precision, scale) || name == "date" || name == "time" ||
	           name == "string" || name == "uuid") {
		return w.CreateString(value.ToString());
	}
	return w.CreateNull();
}

//===--------------------------------------------------------------------===//
// Avro schemas
//===--------------------------------------------------------------------===//
string AvroPrimitiveSchema(const IcebergType &type) {
	auto &name = type.primitive;
	if (name == "boolean" || name == "int" || name == "long" || name == "float" || name == "double") {
		return "\"" + name + "\"";
	}
	if (name == "date") {
		return "{\"type\": \"int\", \"logicalType\": \"date\"}";
	}
	if (name == "time") {
		return "{\"type\": \"long\", \"logicalType\": \"time-micros\"}";
	}
	if (name == "timestamp") {
		return "{\"type\": \"long\", \"logicalType\": \"timestamp-micros\", \"adjust-to-utc\": false}";
	}
	if (name == "timestamptz") {
		return "{\"type\": \"long\", \"logicalType\": \"timestamp-micros\", \"adjust-to-utc\": true}";
	}
	if (name == "string") {
		return "\"string\"";
	}
	if (name == "uuid") {
		return "{\"type\": \"fixed\", \"size\": 16, \"logicalType\": \"uuid\", \"name\": \"uuid_fixed\"}";
	}
	throw NotImplementedException("Partition values of type %s are not supported", name);
}

string TransformResultSchema(const ExportSpecField &field, const IcebergType &source_type) {
	if (field.transform == "identity") {
		return AvroPrimitiveSchema(source_type);
	}
	if (field.transform == "day") {
		return "{\"type\": \"int\", \"logicalType\": \"date\"}";
	}
	return "\"int\"";
}

string MapEntrySchema(idx_t field_id, idx_t key_id, idx_t value_id, const string &key_value_name,
                      const string &value_type) {
	return StringUtil::Format(
	    "{\"name\": \"%s\", \"field-id\": %d, \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": "
	    "\"record\", \"name\": \"k%d_v%d\", \"fields\": [{\"name\": \"key\", \"type\": \"int\", \"field-id\": %d}, "
	    "{\"name\": \"value\", \"type\": \"%s\", \"field-id\": %d}]}, \"logicalType\": \"map\"}], \"default\": null}",
	    key_value_name, field_id, key_id, value_id, key_id, value_type, value_id);
}

string ManifestSchema(const ExportSpec &spec, const IcebergSchema &schema) {
	string partition_fields;
	for (auto &field : spec.fields) {
		auto source = schema.FindField(field.source_id);
		if (!source) {
			throw InternalException("The source of a partition field is not in the schema");
		}
		if (!partition_fields.empty()) {
			partition_fields += ", ";
		}
		partition_fields += StringUtil::Format(
		    "{\"name\": \"%s\", \"field-id\": %d, \"type\": [\"null\", %s], \"default\": null}",
		    AvroCompatibleName(field.name), field.field_id, TransformResultSchema(field, source->type));
	}
	string result =
	    "{\"type\": \"record\", \"name\": \"manifest_entry\", \"fields\": ["
	    "{\"name\": \"status\", \"field-id\": 0, \"type\": \"int\"}, "
	    "{\"name\": \"snapshot_id\", \"field-id\": 1, \"type\": [\"null\", \"long\"], \"default\": null}, "
	    "{\"name\": \"sequence_number\", \"field-id\": 3, \"type\": [\"null\", \"long\"], \"default\": null}, "
	    "{\"name\": \"file_sequence_number\", \"field-id\": 4, \"type\": [\"null\", \"long\"], \"default\": null}, "
	    "{\"name\": \"data_file\", \"field-id\": 2, \"type\": {\"type\": \"record\", \"name\": \"r2\", \"fields\": ["
	    "{\"name\": \"content\", \"field-id\": 134, \"type\": \"int\"}, "
	    "{\"name\": \"file_path\", \"field-id\": 100, \"type\": \"string\"}, "
	    "{\"name\": \"file_format\", \"field-id\": 101, \"type\": \"string\"}, "
	    "{\"name\": \"partition\", \"field-id\": 102, \"type\": {\"type\": \"record\", \"name\": \"r102\", "
	    "\"fields\": [" +
	    partition_fields +
	    "]}}, "
	    "{\"name\": \"record_count\", \"field-id\": 103, \"type\": \"long\"}, "
	    "{\"name\": \"file_size_in_bytes\", \"field-id\": 104, \"type\": \"long\"}, " +
	    MapEntrySchema(108, 117, 118, "column_sizes", "long") + ", " +
	    MapEntrySchema(109, 119, 120, "value_counts", "long") + ", " +
	    MapEntrySchema(110, 121, 122, "null_value_counts", "long") + ", " +
	    MapEntrySchema(137, 138, 139, "nan_value_counts", "long") + ", " +
	    MapEntrySchema(125, 126, 127, "lower_bounds", "bytes") + ", " +
	    MapEntrySchema(128, 129, 130, "upper_bounds", "bytes") +
	    ", "
	    "{\"name\": \"key_metadata\", \"field-id\": 131, \"type\": [\"null\", \"bytes\"], \"default\": null}, "
	    "{\"name\": \"split_offsets\", \"field-id\": 132, \"type\": [\"null\", {\"type\": \"array\", \"element-id\": "
	    "133, \"items\": \"long\"}], \"default\": null}, "
	    "{\"name\": \"equality_ids\", \"field-id\": 135, \"type\": [\"null\", {\"type\": \"array\", \"element-id\": "
	    "136, \"items\": \"int\"}], \"default\": null}, "
	    "{\"name\": \"sort_order_id\", \"field-id\": 140, \"type\": [\"null\", \"int\"], \"default\": null}"
	    "]}}]}";
	return result;
}

const char *MANIFEST_LIST_SCHEMA =
    "{\"type\": \"record\", \"name\": \"manifest_file\", \"fields\": ["
    "{\"name\": \"manifest_path\", \"field-id\": 500, \"type\": \"string\"}, "
    "{\"name\": \"manifest_length\", \"field-id\": 501, \"type\": \"long\"}, "
    "{\"name\": \"partition_spec_id\", \"field-id\": 502, \"type\": \"int\"}, "
    "{\"name\": \"content\", \"field-id\": 517, \"type\": \"int\"}, "
    "{\"name\": \"sequence_number\", \"field-id\": 515, \"type\": \"long\"}, "
    "{\"name\": \"min_sequence_number\", \"field-id\": 516, \"type\": \"long\"}, "
    "{\"name\": \"added_snapshot_id\", \"field-id\": 503, \"type\": \"long\"}, "
    "{\"name\": \"added_files_count\", \"field-id\": 504, \"type\": \"int\"}, "
    "{\"name\": \"existing_files_count\", \"field-id\": 505, \"type\": \"int\"}, "
    "{\"name\": \"deleted_files_count\", \"field-id\": 506, \"type\": \"int\"}, "
    "{\"name\": \"added_rows_count\", \"field-id\": 512, \"type\": \"long\"}, "
    "{\"name\": \"existing_rows_count\", \"field-id\": 513, \"type\": \"long\"}, "
    "{\"name\": \"deleted_rows_count\", \"field-id\": 514, \"type\": \"long\"}, "
    "{\"name\": \"partitions\", \"field-id\": 507, \"type\": [\"null\", {\"type\": \"array\", \"element-id\": 508, "
    "\"items\": {\"type\": \"record\", \"name\": \"r508\", \"fields\": ["
    "{\"name\": \"contains_null\", \"field-id\": 509, \"type\": \"boolean\"}, "
    "{\"name\": \"contains_nan\", \"field-id\": 518, \"type\": [\"null\", \"boolean\"], \"default\": null}, "
    "{\"name\": \"lower_bound\", \"field-id\": 510, \"type\": [\"null\", \"bytes\"], \"default\": null}, "
    "{\"name\": \"upper_bound\", \"field-id\": 511, \"type\": [\"null\", \"bytes\"], \"default\": null}]}}], "
    "\"default\": null}, "
    "{\"name\": \"key_metadata\", \"field-id\": 519, \"type\": [\"null\", \"bytes\"], \"default\": null}]}";

AvroValue Optional(bool present, AvroValue value) {
	if (!present) {
		return AvroValue::Union(0, AvroValue::Null());
	}
	return AvroValue::Union(1, std::move(value));
}

AvroValue CountMap(const vector<pair<idx_t, int64_t>> &entries, bool bytes, const vector<string> &byte_values) {
	if (entries.empty()) {
		return AvroValue::Union(0, AvroValue::Null());
	}
	vector<AvroValue> items;
	for (idx_t i = 0; i < entries.size(); i++) {
		vector<AvroValue> fields;
		fields.push_back(AvroValue::Int(NumericCast<int32_t>(entries[i].first)));
		fields.push_back(bytes ? AvroValue::Bytes(byte_values[i]) : AvroValue::Long(entries[i].second));
		items.push_back(AvroValue::Record(std::move(fields)));
	}
	return AvroValue::Union(1, AvroValue::Array(std::move(items)));
}

} // namespace

//===--------------------------------------------------------------------===//
// The state of a table
//===--------------------------------------------------------------------===//
namespace {

class TableExport {
public:
	TableExport(IcebergRequest &request_p, const DuckLakeSchemaInfo &schema_p, const DuckLakeTableInfo &table_p)
	    : request(request_p), context(*request_p.connection.context), schema(schema_p), table(table_p),
	      table_id(table_p.id.index) {
	}

	IcebergTableLoad Run(const string &snapshots_mode);
	//! Only computes the state that requirements are checked against, without writing files
	bool state_only = false;
	//! Computes the scan of a snapshot, which is returned as the metadata JSON of the load
	bool plan_scan = false;
	optional_idx plan_snapshot;
	//! Generates a file of the table instead of its metadata
	string generate_name;
	string generated;

private:
	void LoadColumns();
	void LoadPartitions();
	void LoadSnapshots();
	void LoadFiles();
	void ChooseSnapshots();
	vector<DuckLakeColumnInfo> ColumnsAt(idx_t snapshot_id);
	const ExportSpec &SpecOfPartition(optional_idx partition_id);
	idx_t SchemaIdAt(idx_t snapshot_id);
	void LoadStatsAndPartitionValues(const vector<const ExportDataFile *> &files);
	IcebergExportedSnapshot ExportSnapshot(idx_t index);
	string WriteManifest(const vector<ManifestItem> &items, const ExportSpec &spec, idx_t snapshot_id,
	                     const IcebergSchema &snapshot_schema, bool deletes, ManifestSummary &summary);
	string GenerateDeleteFile(idx_t snapshot_id, idx_t delete_file_id);
	string GenerateParquet(const string &name);
	string GenerateManifest(idx_t snapshot_id, idx_t spec_id, bool deletes);
	string GenerateManifestList(idx_t snapshot_id, const string &previous);
	string Dispatch(const string &name);
	//! The URL of a generated file of the table
	string Url(const string &name) const;
	void GatherItems(idx_t snapshot_id, GatheredItems &out, bool materialize);
	string BuildPlan(optional_idx requested_snapshot);
	vector<const ExportDataFile *> VisibleDataFiles(idx_t snapshot_id);
	vector<const ExportDeleteFile *> VisibleDeleteFiles(idx_t snapshot_id);
	string FilePath(const string &path, bool relative_flag) const;

private:
	IcebergRequest &request;
	ClientContext &context;
	const DuckLakeSchemaInfo &schema;
	const DuckLakeTableInfo &table;
	idx_t table_id;
	idx_t table_begin = 0;
	//! The files that ended before this snapshot are not of interest
	idx_t file_cutoff = 0;
	string table_location;

	vector<ExportColumnRow> column_rows;
	vector<ExportPartition> partitions;
	vector<ExportSnapshotRow> all_snapshots;
	map<idx_t, string> snapshot_changes;
	vector<ExportDataFile> data_files;
	vector<ExportDeleteFile> delete_files;
	map<idx_t, bool> data_file_relative;
	map<idx_t, bool> delete_file_relative;
	//! The files of the table by id, and their stats and partition values
	map<idx_t, vector<ExportStats>> file_stats;
	map<idx_t, map<idx_t, string>> file_partition_values;
	set<idx_t> loaded_stats;

	vector<ExportSpec> specs;
	map<idx_t, idx_t> partition_to_spec;
	vector<IcebergSchema> schemas;
	//! The schema id of the DuckLake snapshots from which the schema changed
	vector<pair<idx_t, idx_t>> schema_starts;
	vector<idx_t> chosen;
	idx_t latest_snapshot = 0;
	int64_t latest_time_ms = 0;
	map<idx_t, bool> inlined_visible;
	unique_ptr<IcebergInlinedExport> inlined;
	map<string, idx_t> field_ids_by_key;
	//! The references (branches and tags) of the table by name, as JSON, and the snapshots they refer to
	map<string, string> references;
	set<idx_t> reference_snapshots;
};

string TableExport::FilePath(const string &path, bool relative) const {
	if (!relative) {
		return path;
	}
	return IcebergUtil::StripTrailingSlash(table.path) + "/" + path;
}

void TableExport::LoadColumns() {
	auto result = request.MetadataQuery(StringUtil::Format(
	    "SELECT column_id, begin_snapshot, end_snapshot, column_order, column_name, column_type, nulls_allowed, "
	    "parent_column FROM {METADATA_CATALOG}.ducklake_column WHERE table_id = %d ORDER BY column_order",
	    table_id));
	for (auto &row : *result) {
		ExportColumnRow column;
		column.id = row.GetValue<idx_t>(0);
		column.begin = row.GetValue<idx_t>(1);
		column.end = ReadOptIdx(row, 2);
		column.order = row.GetValue<idx_t>(3);
		column.name = row.GetValue<string>(4);
		column.type = row.GetValue<string>(5);
		column.nulls_allowed = row.IsNull(6) ? true : row.GetValue<bool>(6);
		column.parent = ReadOptIdx(row, 7);
		column_rows.push_back(std::move(column));
	}
}

static DuckLakeColumnInfo BuildColumn(const vector<const ExportColumnRow *> &visible, const ExportColumnRow &row) {
	DuckLakeColumnInfo info;
	info.id = FieldIndex(row.id);
	info.name = row.name;
	info.type = row.type;
	info.nulls_allowed = row.nulls_allowed;
	vector<const ExportColumnRow *> children;
	for (auto child : visible) {
		if (child->parent.IsValid() && child->parent.GetIndex() == row.id) {
			children.push_back(child);
		}
	}
	std::sort(children.begin(), children.end(),
	          [](const ExportColumnRow *a, const ExportColumnRow *b) { return a->order < b->order; });
	for (auto child : children) {
		info.children.push_back(BuildColumn(visible, *child));
	}
	return info;
}

vector<DuckLakeColumnInfo> TableExport::ColumnsAt(idx_t snapshot_id) {
	vector<const ExportColumnRow *> visible;
	for (auto &row : column_rows) {
		if (row.begin <= snapshot_id && (!row.end.IsValid() || row.end.GetIndex() > snapshot_id)) {
			visible.push_back(&row);
		}
	}
	vector<const ExportColumnRow *> roots;
	for (auto row : visible) {
		if (!row->parent.IsValid()) {
			roots.push_back(row);
		}
	}
	std::sort(roots.begin(), roots.end(),
	          [](const ExportColumnRow *a, const ExportColumnRow *b) { return a->order < b->order; });
	vector<DuckLakeColumnInfo> result;
	for (auto root : roots) {
		result.push_back(BuildColumn(visible, *root));
	}
	return result;
}

void TableExport::LoadPartitions() {
	auto result = request.MetadataQuery(StringUtil::Format(
	    "SELECT p.partition_id, p.begin_snapshot, p.end_snapshot, c.partition_key_index, c.column_id, c.transform "
	    "FROM {METADATA_CATALOG}.ducklake_partition_info p LEFT JOIN {METADATA_CATALOG}.ducklake_partition_column c "
	    "ON p.partition_id = c.partition_id AND p.table_id = c.table_id WHERE p.table_id = %d "
	    "ORDER BY p.partition_id, c.partition_key_index",
	    table_id));
	for (auto &row : *result) {
		auto partition_id = row.GetValue<idx_t>(0);
		if (partitions.empty() || partitions.back().partition_id != partition_id) {
			ExportPartition partition;
			partition.partition_id = partition_id;
			partition.begin = row.GetValue<idx_t>(1);
			partition.end = ReadOptIdx(row, 2);
			partitions.push_back(std::move(partition));
		}
		if (!row.IsNull(4)) {
			ExportPartitionField field;
			field.key_index = row.GetValue<idx_t>(3);
			field.column_id = row.GetValue<idx_t>(4);
			field.transform = row.GetValue<string>(5);
			partitions.back().fields.push_back(std::move(field));
		}
	}
}

void TableExport::LoadSnapshots() {
	auto result = request.MetadataQuery(StringUtil::Format(
	    "SELECT snapshot_id, CAST(epoch_ms(snapshot_time) AS BIGINT), schema_version FROM "
	    "{METADATA_CATALOG}.ducklake_snapshot WHERE snapshot_id >= %d AND snapshot_id <= {SNAPSHOT_ID} ORDER BY "
	    "snapshot_id",
	    table_begin));
	for (auto &row : *result) {
		ExportSnapshotRow snapshot;
		snapshot.id = row.GetValue<idx_t>(0);
		snapshot.time_ms = row.GetValue<int64_t>(1);
		snapshot.schema_version = row.GetValue<idx_t>(2);
		all_snapshots.push_back(snapshot);
	}
	auto changes = request.MetadataQuery(StringUtil::Format(
	    "SELECT snapshot_id, changes_made FROM {METADATA_CATALOG}.ducklake_snapshot_changes WHERE snapshot_id >= %d "
	    "AND snapshot_id <= {SNAPSHOT_ID}",
	    table_begin));
	for (auto &row : *changes) {
		if (!row.IsNull(1)) {
			snapshot_changes[row.GetValue<idx_t>(0)] = row.GetValue<string>(1);
		}
	}
}

void TableExport::LoadFiles() {
	auto files = request.MetadataQuery(StringUtil::Format(
	    "SELECT data_file_id, begin_snapshot, end_snapshot, path, path_is_relative, record_count, file_size_bytes, "
	    "partition_id, partial_max, encryption_key IS NOT NULL, row_id_start FROM "
	    "{METADATA_CATALOG}.ducklake_data_file WHERE "
	    "table_id = %d AND begin_snapshot <= {SNAPSHOT_ID} AND (end_snapshot IS NULL OR end_snapshot > %d) ORDER BY "
	    "begin_snapshot, file_order, data_file_id",
	    table_id, file_cutoff));
	for (auto &row : *files) {
		if (row.GetValue<bool>(9)) {
			throw NotImplementedException("The Iceberg REST API does not support encrypted DuckLake tables");
		}
		ExportDataFile file;
		file.id = row.GetValue<idx_t>(0);
		file.begin = row.GetValue<idx_t>(1);
		file.end = ReadOptIdx(row, 2);
		file.path = FilePath(row.GetValue<string>(3), row.IsNull(4) ? true : row.GetValue<bool>(4));
		file.record_count = row.GetValue<idx_t>(5);
		file.file_size = row.GetValue<idx_t>(6);
		file.partition_id = ReadOptIdx(row, 7);
		file.partial_max = ReadOptIdx(row, 8);
		file.row_id_start = row.IsNull(10) ? 0 : row.GetValue<idx_t>(10);
		data_files.push_back(std::move(file));
	}
	auto deletes = request.MetadataQuery(StringUtil::Format(
	    "SELECT delete_file_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, "
	    "delete_count, file_size_bytes, partial_max FROM {METADATA_CATALOG}.ducklake_delete_file WHERE table_id = %d "
	    "AND begin_snapshot <= {SNAPSHOT_ID} AND (end_snapshot IS NULL OR end_snapshot > %d) ORDER BY "
	    "begin_snapshot, delete_file_id",
	    table_id, file_cutoff));
	for (auto &row : *deletes) {
		ExportDeleteFile file;
		file.id = row.GetValue<idx_t>(0);
		file.begin = row.GetValue<idx_t>(1);
		file.end = ReadOptIdx(row, 2);
		file.data_file_id = row.GetValue<idx_t>(3);
		file.path = FilePath(row.GetValue<string>(4), row.IsNull(5) ? true : row.GetValue<bool>(5));
		file.format = row.IsNull(6) ? "parquet" : row.GetValue<string>(6);
		file.delete_count = row.GetValue<idx_t>(7);
		file.file_size = row.GetValue<idx_t>(8);
		file.partial_max = ReadOptIdx(row, 9);
		delete_files.push_back(std::move(file));
	}
}

bool IsDataEventToken(const string &token, idx_t table_id) {
	auto colon = token.find(':');
	if (colon == string::npos) {
		return false;
	}
	auto name = token.substr(0, colon);
	if (name == "altered_table" || name == "created_table" || name.find("dropped") != string::npos ||
	    name.find("created") != string::npos || name.find("altered") != string::npos) {
		return false;
	}
	auto value = token.substr(colon + 1);
	if (value.empty() || value.find_first_not_of("0123456789") != string::npos) {
		return false;
	}
	return std::stoull(value) == table_id;
}

void TableExport::ChooseSnapshots() {
	set<idx_t> candidates;
	for (auto &entry : snapshot_changes) {
		for (auto &token : StringUtil::Split(entry.second, ',')) {
			if (IsDataEventToken(token, table_id)) {
				candidates.insert(entry.first);
			}
		}
	}
	for (auto &file : data_files) {
		candidates.insert(file.begin);
		if (file.end.IsValid()) {
			candidates.insert(file.end.GetIndex());
		}
	}
	for (auto &file : delete_files) {
		candidates.insert(file.begin);
		if (file.end.IsValid()) {
			candidates.insert(file.end.GetIndex());
		}
	}
	for (auto snapshot : reference_snapshots) {
		candidates.insert(snapshot);
	}
	// snapshots that were expired are gone
	set<idx_t> existing_snapshots;
	for (auto &snapshot : all_snapshots) {
		existing_snapshots.insert(snapshot.id);
	}
	for (auto candidate : candidates) {
		if (candidate >= table_begin && candidate <= latest_snapshot &&
		    existing_snapshots.find(candidate) != existing_snapshots.end()) {
			chosen.push_back(candidate);
		}
	}
	if (chosen.empty() && !data_files.empty()) {
		// every snapshot that changed the files was expired: the latest snapshot has the files
		chosen.push_back(latest_snapshot);
	}
	// the snapshots in which the files are only partially visible cannot be expressed
	vector<idx_t> valid;
	for (auto snapshot : chosen) {
		bool partial = false;
		for (auto &file : data_files) {
			if (file.begin <= snapshot && (!file.end.IsValid() || file.end.GetIndex() > snapshot) &&
			    file.partial_max.IsValid() && file.partial_max.GetIndex() > snapshot) {
				partial = true;
			}
		}
		for (auto &file : delete_files) {
			if (file.begin <= snapshot && (!file.end.IsValid() || file.end.GetIndex() > snapshot) &&
			    file.partial_max.IsValid() && file.partial_max.GetIndex() > snapshot) {
				partial = true;
			}
		}
		if (!partial) {
			valid.push_back(snapshot);
		}
	}
	// the data that is inlined in the catalog is only exposed in the latest snapshot
	for (idx_t i = 0; i + 1 < valid.size();) {
		if (inlined->HasRows(valid[i]) || inlined->HasDeletes(valid[i])) {
			valid.erase(valid.begin() + NumericCast<int64_t>(i));
		} else {
			i++;
		}
	}
	chosen = std::move(valid);
	auto limit = request.server.SnapshotLimit();
	if (chosen.size() > limit) {
		// the snapshots of the references stay
		vector<idx_t> kept;
		idx_t first_recent = chosen.size() - limit;
		for (idx_t i = 0; i < chosen.size(); i++) {
			if (i >= first_recent || reference_snapshots.find(chosen[i]) != reference_snapshots.end()) {
				kept.push_back(chosen[i]);
			}
		}
		chosen = std::move(kept);
	}
	// the files of the snapshots before the cutoff were not loaded
	while (!chosen.empty() && chosen.front() < file_cutoff) {
		chosen.erase(chosen.begin());
	}
}

const ExportSpec &TableExport::SpecOfPartition(optional_idx partition_id) {
	if (!partition_id.IsValid()) {
		return specs[0];
	}
	auto entry = partition_to_spec.find(partition_id.GetIndex());
	if (entry == partition_to_spec.end()) {
		return specs[0];
	}
	return specs[entry->second];
}

idx_t TableExport::SchemaIdAt(idx_t snapshot_id) {
	idx_t result = 0;
	for (auto &start : schema_starts) {
		if (start.first <= snapshot_id) {
			result = start.second;
		}
	}
	return result;
}

IcebergTableLoad TableExport::Run(const string &snapshots_mode) {
	auto &server = request.server;
	latest_snapshot = request.GetSnapshot().snapshot_id;
	table_location = IcebergUtil::StripTrailingSlash(table.path);

	// when the table was created
	auto begin = request.MetadataQuery(StringUtil::Format(
	    "SELECT MIN(begin_snapshot) FROM {METADATA_CATALOG}.ducklake_table WHERE table_id = %d", table_id));
	for (auto &row : *begin) {
		table_begin = row.GetValue<idx_t>(0);
	}
	LoadColumns();
	LoadPartitions();
	LoadSnapshots();
	for (auto &reference : IcebergProperties::Get(request, IcebergProperties::REFERENCE, table.uuid)) {
		try {
			RESTJsonReader reader(reference.second);
			references[reference.first] = reference.second;
			reference_snapshots.insert(reader.Root().UInt("snapshot-id"));
		} catch (std::exception &) {
			// a reference that cannot be read is left out
		}
	}
	// only the files of the snapshots that are exposed are loaded
	file_cutoff = table_begin;
	vector<idx_t> data_snapshots;
	for (auto &entry : snapshot_changes) {
		for (auto &token : StringUtil::Split(entry.second, ',')) {
			if (IsDataEventToken(token, table_id)) {
				data_snapshots.push_back(entry.first);
				break;
			}
		}
	}
	std::sort(data_snapshots.begin(), data_snapshots.end());
	auto limit = request.server.SnapshotLimit();
	if (data_snapshots.size() > limit) {
		file_cutoff = MaxValue<idx_t>(file_cutoff, data_snapshots[data_snapshots.size() - limit]);
	}
	for (auto snapshot : reference_snapshots) {
		file_cutoff = MinValue<idx_t>(file_cutoff, snapshot);
	}
	if (!generate_name.empty()) {
		// only the files that are visible at the snapshot of the file are needed
		file_cutoff = latest_snapshot;
	}
	LoadFiles();
	for (auto &snapshot : all_snapshots) {
		if (snapshot.id == latest_snapshot) {
			latest_time_ms = snapshot.time_ms;
		}
	}
	inlined = IcebergInlinedExport::Create(request, table);
	ChooseSnapshots();

	// the schemas: a new one for every change of the columns
	set<idx_t> schema_snapshots = {table_begin};
	for (auto &row : column_rows) {
		schema_snapshots.insert(row.begin);
		if (row.end.IsValid()) {
			schema_snapshots.insert(row.end.GetIndex());
		}
	}
	vector<IcebergSchema> candidate_schemas;
	for (auto snapshot : schema_snapshots) {
		if (snapshot < table_begin || snapshot > latest_snapshot) {
			continue;
		}
		auto columns = ColumnsAt(snapshot);
		if (columns.empty()) {
			continue;
		}
		auto iceberg_schema = IcebergTypes::FromColumns(columns, schemas.size());
		if (!schemas.empty() && schemas.back().SameFields(iceberg_schema)) {
			continue;
		}
		schema_starts.emplace_back(snapshot, schemas.size());
		schemas.push_back(std::move(iceberg_schema));
	}
	if (schemas.empty()) {
		throw InvalidInputException("The table \"%s\" has no columns", table.name);
	}

	// the partition specs
	ExportSpec unpartitioned;
	unpartitioned.spec_id = 0;
	specs.push_back(std::move(unpartitioned));
	vector<pair<string, idx_t>> partition_field_ids;
	auto field_id_of = [&](idx_t column_id, const string &transform) {
		auto key = StringUtil::Format("%d:%s", column_id, transform);
		auto entry = field_ids_by_key.find(key);
		if (entry != field_ids_by_key.end()) {
			return entry->second;
		}
		auto field_id = 1000 + field_ids_by_key.size();
		field_ids_by_key[key] = field_id;
		return field_id;
	};
	auto &current_schema = schemas.back();
	for (auto &partition : partitions) {
		ExportSpec spec;
		for (auto &field : partition.fields) {
			auto transform = IcebergTransform(field.transform);
			auto source = current_schema.FindField(field.column_id);
			if (transform.empty() || !source) {
				continue;
			}
			if (transform == "identity" && !IsSupportedIdentityType(source->type)) {
				continue;
			}
			ExportSpecField spec_field;
			spec_field.source_id = field.column_id;
			spec_field.transform = transform;
			spec_field.field_id = field_id_of(field.column_id, transform);
			spec_field.name = PartitionFieldName(source->name, transform);
			spec_field.key_index = field.key_index;
			spec.fields.push_back(std::move(spec_field));
		}
		// the same fields as another spec share its id
		idx_t spec_id = specs.size();
		for (auto &existing : specs) {
			if (existing.fields.size() != spec.fields.size()) {
				continue;
			}
			bool same = true;
			for (idx_t i = 0; i < spec.fields.size(); i++) {
				same = same && existing.fields[i].field_id == spec.fields[i].field_id;
			}
			if (same) {
				spec_id = existing.spec_id;
				break;
			}
		}
		if (spec_id == specs.size()) {
			spec.spec_id = spec_id;
			specs.push_back(std::move(spec));
		}
		partition_to_spec[partition.partition_id] = spec_id;
	}
	// the sort order: the expressions that are columns
	vector<ExportSortField> sort_fields;
	{
		auto sort = request.MetadataQuery(StringUtil::Format(
		    "SELECT e.expression, e.sort_direction, e.null_order FROM {METADATA_CATALOG}.ducklake_sort_info s JOIN "
		    "{METADATA_CATALOG}.ducklake_sort_expression e ON s.sort_id = e.sort_id AND s.table_id = e.table_id WHERE "
		    "s.table_id = %d AND s.end_snapshot IS NULL ORDER BY e.sort_key_index",
		    table_id));
		bool representable = true;
		for (auto &row : *sort) {
			auto expression = row.GetValue<string>(0);
			if (expression.size() >= 2 && expression.front() == '"' && expression.back() == '"') {
				expression = StringUtil::Replace(expression.substr(1, expression.size() - 2), "\"\"", "\"");
			}
			auto source = current_schema.FindTopLevelByName(expression);
			if (!source || source->type.kind != IcebergType::Kind::PRIMITIVE) {
				representable = false;
				continue;
			}
			ExportSortField field;
			field.source_id = source->id;
			field.name = source->name;
			field.descending = StringUtil::CIEquals(row.GetValue<string>(1), "DESC");
			field.nulls_first = StringUtil::CIEquals(row.GetValue<string>(2), "NULLS_FIRST");
			sort_fields.push_back(std::move(field));
		}
		if (!representable) {
			sort_fields.clear();
		}
	}
	string sort_signature;
	for (auto &field : sort_fields) {
		sort_signature += StringUtil::Format("%s:%s:%s;", field.name, field.descending ? "desc" : "asc",
		                                     field.nulls_first ? "nulls-first" : "nulls-last");
	}
	idx_t default_sort_order_id = sort_fields.empty() ? 0 : 1;
	idx_t default_spec_id = 0;
	for (auto &partition : partitions) {
		if (!partition.end.IsValid()) {
			default_spec_id = partition_to_spec[partition.partition_id];
		}
	}

	if (!generate_name.empty()) {
		generated = Dispatch(generate_name);
		return IcebergTableLoad();
	}
	if (plan_scan) {
		IcebergTableLoad plan;
		plan.metadata_json = BuildPlan(plan_snapshot);
		return plan;
	}
	if (!state_only && table_location.find("://") == string::npos) {
		// object stores have no directories, but the clients of a local file system expect them
		auto &fs = FileSystem::GetFileSystem(context);
		fs.CreateDirectoriesRecursive(table_location + "/metadata");
		fs.CreateDirectoriesRecursive(table_location + "/data");
	}
	if (!state_only && table_location.find("://") == string::npos) {
		// object stores have no directories, but the clients of a local file system expect them
		auto &fs = FileSystem::GetFileSystem(context);
		fs.CreateDirectoriesRecursive(table_location + "/metadata");
		fs.CreateDirectoriesRecursive(table_location + "/data");
	}
	if (state_only) {
		IcebergTableLoad state;
		state.table_uuid = table.uuid;
		if (!chosen.empty()) {
			state.current_snapshot_id = chosen.back();
		}
		state.current_schema_id = schemas.back().schema_id;
		for (auto &row : column_rows) {
			state.last_column_id = MaxValue<idx_t>(state.last_column_id, row.id);
		}
		state.default_spec_id = default_spec_id;
		state.default_sort_order_id = default_sort_order_id;
		state.sort_signature = sort_signature;
		state.last_partition_id = field_ids_by_key.empty() ? 999 : 999 + field_ids_by_key.size();
		state.last_sequence_number = chosen.empty() ? 0 : chosen.back();
		state.schemas = schemas;
		return state;
	}

	// the snapshots
	vector<IcebergExportedSnapshot> exported;
	for (idx_t i = 0; i < chosen.size(); i++) {
		exported.push_back(ExportSnapshot(i));
	}
	if (snapshots_mode == "refs" && exported.size() > 1) {
		exported.erase(exported.begin(), exported.end() - 1);
		chosen.erase(chosen.begin(), chosen.end() - 1);
	}

	idx_t last_column_id = 0;
	for (auto &row : column_rows) {
		last_column_id = MaxValue<idx_t>(last_column_id, row.id);
	}
	idx_t last_partition_id = field_ids_by_key.empty() ? 999 : 999 + field_ids_by_key.size();
	idx_t last_sequence_number = chosen.empty() ? 0 : chosen.back();
	idx_t current_schema_id = schemas.back().schema_id;

	RESTJsonWriter writer;
	auto &w = writer.writer;
	auto metadata = w.CreateObject();
	metadata.Add("format-version", w.CreateSignedInteger(2));
	metadata.AddString("table-uuid", table.uuid);
	metadata.AddString("location", table_location);
	metadata.Add("last-sequence-number", w.CreateSignedInteger(NumericCast<int64_t>(last_sequence_number)));
	metadata.Add("last-updated-ms", w.CreateSignedInteger(latest_time_ms));
	metadata.Add("last-column-id", w.CreateSignedInteger(NumericCast<int64_t>(last_column_id)));
	auto schemas_json = w.CreateArray();
	for (auto &iceberg_schema : schemas) {
		schemas_json.Append(IcebergTypes::SchemaToJson(w, iceberg_schema));
	}
	metadata.Add("schemas", schemas_json);
	metadata.Add("current-schema-id", w.CreateSignedInteger(NumericCast<int64_t>(current_schema_id)));
	auto specs_json = w.CreateArray();
	for (auto &spec : specs) {
		auto spec_json = w.CreateObject();
		spec_json.Add("spec-id", w.CreateSignedInteger(NumericCast<int64_t>(spec.spec_id)));
		auto fields_json = w.CreateArray();
		for (auto &field : spec.fields) {
			auto field_json = w.CreateObject();
			field_json.AddString("name", field.name);
			field_json.AddString("transform", field.transform);
			field_json.Add("source-id", w.CreateSignedInteger(NumericCast<int64_t>(field.source_id)));
			field_json.Add("field-id", w.CreateSignedInteger(NumericCast<int64_t>(field.field_id)));
			fields_json.Append(field_json);
		}
		spec_json.Add("fields", fields_json);
		specs_json.Append(spec_json);
	}
	metadata.Add("partition-specs", specs_json);
	metadata.Add("default-spec-id", w.CreateSignedInteger(NumericCast<int64_t>(default_spec_id)));
	metadata.Add("last-partition-id", w.CreateSignedInteger(NumericCast<int64_t>(last_partition_id)));
	auto properties = IcebergProperties::Get(request, IcebergProperties::TABLE, table.uuid);
	metadata.Add("properties", IcebergUtil::StringMap(w, properties));
	if (chosen.empty()) {
		metadata.Add("current-snapshot-id", w.CreateSignedInteger(-1));
	} else {
		metadata.Add("current-snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(chosen.back())));
	}
	auto snapshots_json = w.CreateArray();
	auto snapshot_log = w.CreateArray();
	for (idx_t i = 0; i < chosen.size(); i++) {
		int64_t time_ms = 0;
		idx_t snapshot_schema_id = SchemaIdAt(chosen[i]);
		for (auto &snapshot : all_snapshots) {
			if (snapshot.id == chosen[i]) {
				time_ms = snapshot.time_ms;
			}
		}
		auto snapshot_json = w.CreateObject();
		snapshot_json.Add("snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(chosen[i])));
		if (i > 0) {
			snapshot_json.Add("parent-snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(chosen[i - 1])));
		}
		snapshot_json.Add("sequence-number", w.CreateSignedInteger(NumericCast<int64_t>(chosen[i])));
		snapshot_json.Add("timestamp-ms", w.CreateSignedInteger(time_ms));
		snapshot_json.AddString("manifest-list", exported[i].manifest_list_path);
		auto summary = w.CreateObject();
		summary.AddString("operation", exported[i].operation);
		for (auto &entry : exported[i].summary) {
			summary.AddString(entry.first, entry.second);
		}
		snapshot_json.Add("summary", summary);
		snapshot_json.Add("schema-id", w.CreateSignedInteger(NumericCast<int64_t>(snapshot_schema_id)));
		snapshots_json.Append(snapshot_json);
		auto log_entry = w.CreateObject();
		log_entry.Add("timestamp-ms", w.CreateSignedInteger(time_ms));
		log_entry.Add("snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(chosen[i])));
		snapshot_log.Append(log_entry);
	}
	metadata.Add("snapshots", snapshots_json);
	metadata.Add("snapshot-log", snapshot_log);
	metadata.Add("metadata-log", w.CreateArray());
	auto sort_orders = w.CreateArray();
	auto unsorted = w.CreateObject();
	unsorted.Add("order-id", w.CreateSignedInteger(0));
	unsorted.Add("fields", w.CreateArray());
	sort_orders.Append(unsorted);
	if (!sort_fields.empty()) {
		auto sort_order = w.CreateObject();
		sort_order.Add("order-id", w.CreateSignedInteger(1));
		auto fields_json = w.CreateArray();
		for (auto &field : sort_fields) {
			auto field_json = w.CreateObject();
			field_json.AddString("transform", "identity");
			field_json.Add("source-id", w.CreateSignedInteger(NumericCast<int64_t>(field.source_id)));
			field_json.AddString("direction", field.descending ? "desc" : "asc");
			field_json.AddString("null-order", field.nulls_first ? "nulls-first" : "nulls-last");
			fields_json.Append(field_json);
		}
		sort_order.Add("fields", fields_json);
		sort_orders.Append(sort_order);
	}
	metadata.Add("sort-orders", sort_orders);
	metadata.Add("default-sort-order-id", w.CreateSignedInteger(NumericCast<int64_t>(default_sort_order_id)));
	auto refs = w.CreateObject();
	if (!chosen.empty()) {
		auto main = w.CreateObject();
		main.Add("snapshot-id", w.CreateSignedInteger(NumericCast<int64_t>(chosen.back())));
		main.AddString("type", "branch");
		refs.Add("main", main);
	}
	for (auto &reference : references) {
		RESTJsonReader reader(reference.second);
		auto snapshot_id = reader.Root().UInt("snapshot-id");
		if (reference.first == "main" || std::find(chosen.begin(), chosen.end(), snapshot_id) == chosen.end()) {
			continue;
		}
		refs.Add(reference.first, w.CreateCopy(reader.Root().val));
	}
	metadata.Add("refs", refs);
	metadata.Add("statistics", w.CreateArray());
	metadata.Add("partition-statistics", w.CreateArray());
	writer.SetRoot(metadata);

	IcebergTableLoad load;
	load.metadata_json = writer.ToString();
	load.table_uuid = table.uuid;
	load.location = table_location;
	if (!chosen.empty()) {
		load.current_snapshot_id = chosen.back();
	}
	load.current_schema_id = current_schema_id;
	load.last_column_id = last_column_id;
	load.default_spec_id = default_spec_id;
	load.default_sort_order_id = default_sort_order_id;
	load.sort_signature = sort_signature;
	load.last_partition_id = last_partition_id;
	load.last_sequence_number = last_sequence_number;
	load.schemas = schemas;
	auto metadata_name = StringUtil::Format("meta-%d.metadata.json", latest_snapshot);
	load.metadata_location = Url(metadata_name);
	IcebergVirtualFiles::Publish(request, IcebergVirtualFiles::Entity(table), metadata_name, load.metadata_json);
	return load;
}

void TableExport::LoadStatsAndPartitionValues(const vector<const ExportDataFile *> &files) {
	vector<string> missing;
	for (auto file : files) {
		if (loaded_stats.find(file->id) == loaded_stats.end()) {
			missing.push_back(to_string(file->id));
			loaded_stats.insert(file->id);
		}
	}
	if (missing.empty()) {
		return;
	}
	auto id_list = StringUtil::Join(missing, ", ");
	auto stats = request.MetadataQuery(StringUtil::Format(
	    "SELECT data_file_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, "
	    "contains_nan FROM {METADATA_CATALOG}.ducklake_file_column_stats WHERE table_id = %d AND data_file_id IN (%s)",
	    table_id, id_list));
	for (auto &row : *stats) {
		ExportStats entry;
		entry.column_id = row.GetValue<idx_t>(1);
		entry.column_size = ReadOptIdx(row, 2);
		entry.value_count = ReadOptIdx(row, 3);
		entry.null_count = ReadOptIdx(row, 4);
		if (!row.IsNull(5)) {
			entry.min_value = row.GetValue<string>(5);
			entry.has_min = true;
		}
		if (!row.IsNull(6)) {
			entry.max_value = row.GetValue<string>(6);
			entry.has_max = true;
		}
		entry.contains_nan = !row.IsNull(7) && row.GetValue<bool>(7);
		file_stats[row.GetValue<idx_t>(0)].push_back(std::move(entry));
	}
	auto values = request.MetadataQuery(StringUtil::Format("SELECT data_file_id, partition_key_index, partition_value "
	                                                       "FROM {METADATA_CATALOG}.ducklake_file_partition_value "
	                                                       "WHERE table_id = %d AND data_file_id IN (%s)",
	                                                       table_id, id_list));
	for (auto &row : *values) {
		if (!row.IsNull(2)) {
			file_partition_values[row.GetValue<idx_t>(0)][row.GetValue<idx_t>(1)] = row.GetValue<string>(2);
		}
	}
}

string TableExport::WriteManifest(const vector<ManifestItem> &items, const ExportSpec &spec, idx_t snapshot_id,
                                  const IcebergSchema &snapshot_schema, bool deletes, ManifestSummary &summary) {
	vector<AvroValue> records;
	summary = ManifestSummary();
	summary.min_sequence = NumericLimits<idx_t>::Maximum();

	auto partition_value = [&](const ExportDataFile *file) {
		vector<AvroValue> fields;
		auto values = file ? file_partition_values.find(file->id) : file_partition_values.end();
		for (auto &spec_field : spec.fields) {
			auto source = snapshot_schema.FindField(spec_field.source_id);
			bool present = false;
			AvroValue value;
			if (values != file_partition_values.end() && source) {
				auto entry = values->second.find(spec_field.key_index);
				if (entry != values->second.end()) {
					present = true;
					if (spec_field.transform == "identity") {
						Value typed;
						if (!TextToValue(entry->second, source->type, typed)) {
							present = false;
						} else {
							auto &prim = source->type.primitive;
							if (prim == "boolean") {
								value = AvroValue::Bool(BooleanValue::Get(typed));
							} else if (prim == "int") {
								value = AvroValue::Int(typed.GetValue<int32_t>());
							} else if (prim == "long" || prim == "time" || prim == "timestamp" ||
							           prim == "timestamptz") {
								value = AvroValue::Long(typed.GetValue<int64_t>());
							} else if (prim == "float") {
								value = AvroValue::Float(typed.GetValue<float>());
							} else if (prim == "double") {
								value = AvroValue::Double(typed.GetValue<double>());
							} else if (prim == "date") {
								value = AvroValue::Int(typed.GetValue<int32_t>());
							} else if (prim == "string") {
								value = AvroValue::String(StringValue::Get(typed));
							} else {
								string bytes;
								if (SerializeValue(typed, source->type, bytes)) {
									value = AvroValue::Fixed(bytes);
								} else {
									present = false;
								}
							}
						}
					} else {
						try {
							value = AvroValue::Int(NumericCast<int32_t>(std::stoll(entry->second)));
						} catch (...) {
							present = false;
						}
					}
				}
			}
			fields.push_back(Optional(present, std::move(value)));
		}
		return AvroValue::Record(std::move(fields));
	};

	for (auto &item : items) {
		bool added = item.begin == snapshot_id;
		vector<AvroValue> entry;
		entry.push_back(AvroValue::Int(added ? 1 : 0));
		entry.push_back(Optional(true, AvroValue::Long(NumericCast<int64_t>(item.begin))));
		entry.push_back(Optional(true, AvroValue::Long(NumericCast<int64_t>(item.begin))));
		entry.push_back(Optional(true, AvroValue::Long(NumericCast<int64_t>(item.begin))));
		vector<AvroValue> data_file;
		data_file.push_back(AvroValue::Int(deletes ? 1 : 0));
		data_file.push_back(AvroValue::String(item.path));
		data_file.push_back(AvroValue::String("PARQUET"));
		data_file.push_back(partition_value(item.partition_source));
		data_file.push_back(AvroValue::Long(NumericCast<int64_t>(item.record_count)));
		data_file.push_back(AvroValue::Long(NumericCast<int64_t>(item.file_size)));
		vector<pair<idx_t, int64_t>> sizes, counts, nulls, nans, lowers, uppers;
		vector<string> lower_values, upper_values, no_values;
		if (deletes) {
			// the data file that the deletes refer to
			lowers.emplace_back(POSITION_DELETE_PATH_ID, 0);
			lower_values.push_back(item.referenced_path);
			uppers.emplace_back(POSITION_DELETE_PATH_ID, 0);
			upper_values.push_back(item.referenced_path);
		} else if (item.Stats()) {
			for (auto &stat : *item.Stats()) {
				auto field = snapshot_schema.FindField(stat.column_id);
				if (!field || field->type.kind != IcebergType::Kind::PRIMITIVE) {
					continue;
				}
				if (stat.column_size.IsValid()) {
					sizes.emplace_back(stat.column_id, NumericCast<int64_t>(stat.column_size.GetIndex()));
				}
				if (stat.value_count.IsValid()) {
					counts.emplace_back(stat.column_id, NumericCast<int64_t>(stat.value_count.GetIndex()));
				}
				if (stat.null_count.IsValid()) {
					nulls.emplace_back(stat.column_id, NumericCast<int64_t>(stat.null_count.GetIndex()));
				}
				if (stat.contains_nan) {
					continue;
				}
				Value typed;
				string bytes;
				if (stat.has_min && TextToValue(stat.min_value, field->type, typed) &&
				    SerializeValue(typed, field->type, bytes)) {
					lowers.emplace_back(stat.column_id, 0);
					lower_values.push_back(bytes);
				}
				bytes.clear();
				if (stat.has_max && TextToValue(stat.max_value, field->type, typed) &&
				    SerializeValue(typed, field->type, bytes)) {
					uppers.emplace_back(stat.column_id, 0);
					upper_values.push_back(bytes);
				}
			}
		}
		data_file.push_back(CountMap(sizes, false, no_values));
		data_file.push_back(CountMap(counts, false, no_values));
		data_file.push_back(CountMap(nulls, false, no_values));
		data_file.push_back(CountMap(nans, false, no_values));
		data_file.push_back(CountMap(lowers, true, lower_values));
		data_file.push_back(CountMap(uppers, true, upper_values));
		data_file.push_back(AvroValue::Union(0, AvroValue::Null()));
		data_file.push_back(AvroValue::Union(0, AvroValue::Null()));
		data_file.push_back(AvroValue::Union(0, AvroValue::Null()));
		data_file.push_back(AvroValue::Union(0, AvroValue::Null()));
		entry.push_back(AvroValue::Record(std::move(data_file)));
		records.push_back(AvroValue::Record(std::move(entry)));
		summary.min_sequence = MinValue<idx_t>(summary.min_sequence, item.begin);
		if (added) {
			summary.added_files++;
			summary.added_rows += item.record_count;
		} else {
			summary.existing_files++;
			summary.existing_rows += item.record_count;
		}
	}

	RESTJsonWriter spec_writer;
	auto &w = spec_writer.writer;
	auto spec_json = w.CreateArray();
	for (auto &field : spec.fields) {
		auto field_json = w.CreateObject();
		field_json.Add("source-id", w.CreateSignedInteger(NumericCast<int64_t>(field.source_id)));
		field_json.Add("field-id", w.CreateSignedInteger(NumericCast<int64_t>(field.field_id)));
		field_json.AddString("transform", field.transform);
		field_json.AddString("name", field.name);
		spec_json.Append(field_json);
	}
	spec_writer.SetRoot(spec_json);
	map<string, string> header;
	header["schema"] = IcebergTypes::SchemaToString(snapshot_schema);
	header["partition-spec"] = spec_writer.ToString();
	header["partition-spec-id"] = to_string(spec.spec_id);
	header["format-version"] = "2";
	header["content"] = deletes ? "deletes" : "data";
	auto bytes = AvroWriter::Write(ManifestSchema(spec, snapshot_schema), header, records, "null");
	summary.length = bytes.size();
	return bytes;
}

vector<const ExportDataFile *> TableExport::VisibleDataFiles(idx_t snapshot_id) {
	vector<const ExportDataFile *> result;
	for (auto &file : data_files) {
		if (file.begin <= snapshot_id && (!file.end.IsValid() || file.end.GetIndex() > snapshot_id)) {
			result.push_back(&file);
		}
	}
	return result;
}

vector<const ExportDeleteFile *> TableExport::VisibleDeleteFiles(idx_t snapshot_id) {
	vector<const ExportDeleteFile *> result;
	for (auto &file : delete_files) {
		if (file.begin <= snapshot_id && (!file.end.IsValid() || file.end.GetIndex() > snapshot_id)) {
			result.push_back(&file);
		}
	}
	return result;
}

void TableExport::GatherItems(idx_t snapshot_id, GatheredItems &out, bool materialize) {
	auto &data_list = out.data_list;
	auto &delete_list = out.delete_list;
	data_list = VisibleDataFiles(snapshot_id);
	delete_list = VisibleDeleteFiles(snapshot_id);
	LoadStatsAndPartitionValues(data_list);

	map<idx_t, const ExportDataFile *> data_by_id;
	for (auto file : data_list) {
		data_by_id[file->id] = file;
	}
	auto &data_items = out.data_items;
	auto &delete_items = out.delete_items;
	for (auto file : data_list) {
		ManifestItem item;
		item.begin = file->begin;
		item.path = file->path;
		item.record_count = file->record_count;
		item.file_size = file->file_size;
		item.partition_source = file;
		auto stats = file_stats.find(file->id);
		item.stats = stats == file_stats.end() ? nullptr : &stats->second;
		data_items[SpecOfPartition(file->partition_id).spec_id].push_back(std::move(item));
	}
	// the rows that DuckLake has in its catalog are a file of the snapshot
	for (auto &rows : inlined->RowsAt(snapshot_id)) {
		auto name = StringUtil::Format("inlined-%d-%d.parquet", snapshot_id, rows.schema_version);
		ManifestItem item;
		item.begin = snapshot_id;
		item.path = Url(name);
		item.record_count = rows.row_count;
		item.file_size = materialize ? GenerateParquet(name).size() : 0;
		data_items[0].push_back(std::move(item));
		out.inlined_file_count++;
		out.inlined_rows += rows.row_count;
	}
	for (auto &deletes : inlined->DeletesAt(snapshot_id)) {
		auto data_file = data_by_id.find(deletes.data_file_id);
		if (data_file == data_by_id.end()) {
			continue;
		}
		auto name = StringUtil::Format("inlined-delete-%d-%d.parquet", snapshot_id, deletes.data_file_id);
		ManifestItem item;
		item.begin = snapshot_id;
		item.path = Url(name);
		item.record_count = deletes.delete_count;
		item.file_size = materialize ? GenerateParquet(name).size() : 0;
		item.partition_source = data_file->second;
		item.referenced_path = data_file->second->path;
		out.delete_rows += deletes.delete_count;
		out.inlined_delete_count++;
		delete_items[SpecOfPartition(data_file->second->partition_id).spec_id].push_back(std::move(item));
	}
	// the delete files of DuckLake have other field ids than the position delete files of Iceberg: they are rewritten
	for (auto file : delete_list) {
		auto data_file = data_by_id.find(file->data_file_id);
		if (data_file == data_by_id.end()) {
			continue;
		}
		auto name = StringUtil::Format("delete-%d-%d.parquet", snapshot_id, file->id);
		ManifestItem item;
		item.begin = file->begin;
		item.path = Url(name);
		item.record_count = file->delete_count;
		item.file_size = materialize ? GenerateParquet(name).size() : 0;
		item.partition_source = data_file->second;
		item.referenced_path = data_file->second->path;
		out.delete_rows += file->delete_count;
		delete_items[SpecOfPartition(data_file->second->partition_id).spec_id].push_back(std::move(item));
	}
}

string TableExport::Url(const string &name) const {
	return IcebergVirtualFiles::Url(request, IcebergVirtualFiles::Entity(table), name);
}

//! The Parquet files that are generated: the rows and deletes that DuckLake has in its catalog, and the delete files
string TableExport::GenerateParquet(const string &name) {
	auto cache_key = request.prefix + "/" + IcebergVirtualFiles::Entity(table) + "/" + name;
	string cached;
	if (request.server.GetExportCache().GetBytes(cache_key, cached)) {
		return cached;
	}
	auto parts = StringUtil::Split(name.substr(0, name.size() - 8), '-');
	string result;
	try {
		if (parts[0] == "delete" && parts.size() == 3) {
			result = GenerateDeleteFile(std::stoull(parts[1]), std::stoull(parts[2]));
		} else if (parts[0] == "inlined" && parts.size() == 3) {
			result =
			    inlined->GenerateRows(std::stoull(parts[1]), std::stoull(parts[2]), ColumnsAt(std::stoull(parts[1])));
		} else if (parts[0] == "inlined" && parts[1] == "delete" && parts.size() == 4) {
			auto data_file_id = std::stoull(parts[3]);
			for (auto &file : data_files) {
				if (file.id == data_file_id) {
					result = inlined->GenerateDeletes(std::stoull(parts[2]), file.id, file.path, file.row_id_start);
				}
			}
		}
	} catch (std::invalid_argument &) {
		result.clear();
	}
	if (result.empty()) {
		throw IcebergException(404, "NoSuchFileException", "The file does not exist: " + name);
	}
	request.server.GetExportCache().PutBytes(cache_key, result);
	IcebergVirtualFiles::Publish(request, IcebergVirtualFiles::Entity(table), name, result);
	return result;
}

//! A delete file of DuckLake as the position delete file of Iceberg: with the ids and the path of the data file that
//! Iceberg readers expect
string TableExport::GenerateDeleteFile(idx_t snapshot_id, idx_t delete_file_id) {
	for (auto &file : delete_files) {
		if (file.id != delete_file_id) {
			continue;
		}
		string data_path;
		for (auto &data_file : data_files) {
			if (data_file.id == file.data_file_id) {
				data_path = data_file.path;
			}
		}
		string source;
		if (file.format == "puffin") {
			// a deletion vector: the positions are written as a position delete file
			DuckLakeFileData delete_file;
			delete_file.path = file.path;
			delete_file.format = DeleteFileFormat::PUFFIN;
			delete_file.file_size_bytes = file.file_size;
			auto positions = DuckLakeDeleteFilter::ScanDeletionVectorFile(context, delete_file).deleted_rows;
			std::sort(positions.begin(), positions.end());
			vector<string> literals;
			for (auto position : positions) {
				literals.push_back(to_string(position));
			}
			source = StringUtil::Format("(SELECT unnest([%s]::BIGINT[]) AS pos)", StringUtil::Join(literals, ", "));
		} else {
			source = StringUtil::Format("read_parquet(%s)", SQLString::ToString(file.path));
		}
		return IcebergUtil::CopyToBytes(
		    request, StringUtil::Format("COPY (SELECT %s AS file_path, pos FROM %s ORDER BY pos) TO {FILE} (FORMAT "
		                                "PARQUET, FIELD_IDS {'file_path': %d, 'pos': %d})",
		                                SQLString::ToString(data_path), source, POSITION_DELETE_PATH_ID,
		                                POSITION_DELETE_PATH_ID - 1));
	}
	return string();
}

string TableExport::GenerateManifest(idx_t snapshot_id, idx_t spec_id, bool deletes) {
	GatheredItems gathered;
	GatherItems(snapshot_id, gathered, true);
	auto &items = deletes ? gathered.delete_items : gathered.data_items;
	auto entry = items.find(spec_id);
	if (entry == items.end() || spec_id >= specs.size()) {
		throw IcebergException(404, "NoSuchFileException", "The manifest does not exist");
	}
	ManifestSummary summary;
	return WriteManifest(entry->second, specs[spec_id], snapshot_id, schemas[SchemaIdAt(snapshot_id)], deletes,
	                     summary);
}

string TableExport::GenerateManifestList(idx_t snapshot_id, const string &previous) {
	GatheredItems gathered;
	GatherItems(snapshot_id, gathered, true);
	auto &snapshot_schema = schemas[SchemaIdAt(snapshot_id)];
	vector<AvroValue> list_records;
	auto add_manifests = [&](map<idx_t, vector<ManifestItem>> &items, bool deletes) {
		for (auto &entry : items) {
			ManifestSummary summary;
			auto bytes =
			    WriteManifest(entry.second, specs[entry.first], snapshot_id, snapshot_schema, deletes, summary);
			auto name = StringUtil::Format("m-%d-%d-%s.avro", snapshot_id, entry.first, deletes ? "d" : "m");
			request.server.GetExportCache().PutBytes(
			    request.prefix + "/" + IcebergVirtualFiles::Entity(table) + "/" + name, bytes);
			IcebergVirtualFiles::Publish(request, IcebergVirtualFiles::Entity(table), name, bytes);
			vector<AvroValue> fields;
			fields.push_back(AvroValue::String(Url(name)));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(summary.length)));
			fields.push_back(AvroValue::Int(NumericCast<int32_t>(entry.first)));
			fields.push_back(AvroValue::Int(deletes ? 1 : 0));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(snapshot_id)));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(summary.min_sequence)));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(snapshot_id)));
			fields.push_back(AvroValue::Int(NumericCast<int32_t>(summary.added_files)));
			fields.push_back(AvroValue::Int(NumericCast<int32_t>(summary.existing_files)));
			fields.push_back(AvroValue::Int(0));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(summary.added_rows)));
			fields.push_back(AvroValue::Long(NumericCast<int64_t>(summary.existing_rows)));
			fields.push_back(AvroValue::Long(0));
			fields.push_back(AvroValue::Union(0, AvroValue::Null()));
			fields.push_back(AvroValue::Union(0, AvroValue::Null()));
			list_records.push_back(AvroValue::Record(std::move(fields)));
		}
	};
	add_manifests(gathered.data_items, false);
	add_manifests(gathered.delete_items, true);
	map<string, string> header;
	header["snapshot-id"] = to_string(snapshot_id);
	header["parent-snapshot-id"] = previous == "none" ? "null" : previous;
	header["sequence-number"] = to_string(snapshot_id);
	header["format-version"] = "2";
	auto bytes = AvroWriter::Write(MANIFEST_LIST_SCHEMA, header, list_records, "null");
	IcebergVirtualFiles::Publish(request, IcebergVirtualFiles::Entity(table),
	                             StringUtil::Format("snap-%d-%s.avro", snapshot_id, previous), bytes);
	return bytes;
}

//! Generates the file with a name (see IcebergVirtualFiles)
string TableExport::Dispatch(const string &name) {
	auto parts = StringUtil::Split(name.substr(0, name.rfind('.')), '-');
	try {
		if (StringUtil::EndsWith(name, ".parquet")) {
			return GenerateParquet(name);
		}
		if (parts[0] == "snap" && parts.size() == 3) {
			return GenerateManifestList(std::stoull(parts[1]), parts[2]);
		}
		if (parts[0] == "m" && parts.size() == 4) {
			return GenerateManifest(std::stoull(parts[1]), std::stoull(parts[2]), parts[3] == "d");
		}
	} catch (std::invalid_argument &) {
	}
	throw IcebergException(404, "NoSuchFileException", "The file does not exist: " + name);
}

string TableExport::BuildPlan(optional_idx requested_snapshot) {
	if (chosen.empty()) {
		return "{\"file-scan-tasks\": [], \"delete-files\": []}";
	}
	idx_t snapshot_id = chosen.back();
	if (requested_snapshot.IsValid()) {
		snapshot_id = requested_snapshot.GetIndex();
		if (std::find(chosen.begin(), chosen.end(), snapshot_id) == chosen.end()) {
			throw IcebergException(404, "NoSuchSnapshotException",
			                       StringUtil::Format("The snapshot %d does not exist", snapshot_id));
		}
	}
	GatheredItems gathered;
	GatherItems(snapshot_id, gathered, true);
	auto &snapshot_schema = schemas[SchemaIdAt(snapshot_id)];
	RESTJsonWriter writer;
	auto &w = writer.writer;

	auto counts_json = [&](const vector<pair<idx_t, JSONMutableValue>> &entries) {
		auto result = w.CreateObject();
		auto keys = w.CreateArray();
		auto values = w.CreateArray();
		for (auto &entry : entries) {
			keys.Append(w.CreateSignedInteger(NumericCast<int64_t>(entry.first)));
			values.Append(entry.second);
		}
		result.Add("keys", keys);
		result.Add("values", values);
		return result;
	};
	auto content_file = [&](const ManifestItem &item, idx_t spec_id, bool deletes) {
		auto &spec = specs[spec_id];
		auto file = w.CreateObject();
		file.Add("spec-id", w.CreateSignedInteger(NumericCast<int64_t>(spec_id)));
		file.AddString("content", deletes ? "position-deletes" : "data");
		file.AddString("file-path", item.path);
		file.AddString("file-format", "parquet");
		auto partition = w.CreateArray();
		auto values =
		    item.partition_source ? file_partition_values.find(item.partition_source->id) : file_partition_values.end();
		for (auto &spec_field : spec.fields) {
			auto source = snapshot_schema.FindField(spec_field.source_id);
			JSONMutableValue json = w.CreateNull();
			if (values != file_partition_values.end() && source) {
				auto entry = values->second.find(spec_field.key_index);
				if (entry != values->second.end()) {
					if (spec_field.transform == "identity") {
						Value typed;
						if (TextToValue(entry->second, source->type, typed)) {
							json = TypedValueToJson(w, typed, source->type);
						}
					} else {
						try {
							json = w.CreateSignedInteger(std::stoll(entry->second));
						} catch (...) {
						}
					}
				}
			}
			partition.Append(json);
		}
		file.Add("partition", partition);
		file.Add("file-size-in-bytes", w.CreateSignedInteger(NumericCast<int64_t>(item.file_size)));
		file.Add("record-count", w.CreateSignedInteger(NumericCast<int64_t>(item.record_count)));
		if (!deletes && item.Stats()) {
			vector<pair<idx_t, JSONMutableValue>> sizes, value_counts, null_counts, lowers, uppers;
			for (auto &stat : *item.Stats()) {
				auto field = snapshot_schema.FindField(stat.column_id);
				if (!field || field->type.kind != IcebergType::Kind::PRIMITIVE) {
					continue;
				}
				if (stat.column_size.IsValid()) {
					sizes.emplace_back(stat.column_id,
					                   w.CreateSignedInteger(NumericCast<int64_t>(stat.column_size.GetIndex())));
				}
				if (stat.value_count.IsValid()) {
					value_counts.emplace_back(stat.column_id,
					                          w.CreateSignedInteger(NumericCast<int64_t>(stat.value_count.GetIndex())));
				}
				if (stat.null_count.IsValid()) {
					null_counts.emplace_back(stat.column_id,
					                         w.CreateSignedInteger(NumericCast<int64_t>(stat.null_count.GetIndex())));
				}
				if (stat.contains_nan) {
					continue;
				}
				Value typed;
				if (stat.has_min && TextToValue(stat.min_value, field->type, typed)) {
					lowers.emplace_back(stat.column_id, TypedValueToJson(w, typed, field->type));
				}
				if (stat.has_max && TextToValue(stat.max_value, field->type, typed)) {
					uppers.emplace_back(stat.column_id, TypedValueToJson(w, typed, field->type));
				}
			}
			file.Add("column-sizes", counts_json(sizes));
			file.Add("value-counts", counts_json(value_counts));
			file.Add("null-value-counts", counts_json(null_counts));
			file.Add("lower-bounds", counts_json(lowers));
			file.Add("upper-bounds", counts_json(uppers));
		}
		if (!deletes) {
			file.Add("sort-order-id", w.CreateSignedInteger(0));
		}
		return file;
	};

	// the delete files, and the data files that they apply to
	auto delete_files = w.CreateArray();
	map<string, vector<idx_t>> delete_references;
	idx_t delete_index = 0;
	for (auto &entry : gathered.delete_items) {
		for (auto &item : entry.second) {
			delete_files.Append(content_file(item, entry.first, true));
			delete_references[item.referenced_path].push_back(delete_index++);
		}
	}
	auto tasks = w.CreateArray();
	for (auto &entry : gathered.data_items) {
		for (auto &item : entry.second) {
			auto task = w.CreateObject();
			task.Add("data-file", content_file(item, entry.first, false));
			auto references = delete_references.find(item.path);
			if (references != delete_references.end()) {
				auto reference_list = w.CreateArray();
				for (auto reference : references->second) {
					reference_list.Append(w.CreateSignedInteger(NumericCast<int64_t>(reference)));
				}
				task.Add("delete-file-references", reference_list);
			}
			auto residual = w.CreateBoolean(true);
			task.Add("residual-filter", residual);
			tasks.Append(task);
		}
	}
	auto root = w.CreateObject();
	root.Add("file-scan-tasks", tasks);
	root.Add("delete-files", delete_files);
	writer.SetRoot(root);
	return writer.ToString();
}

IcebergExportedSnapshot TableExport::ExportSnapshot(idx_t index) {
	auto snapshot_id = chosen[index];
	auto &server = request.server;
	string previous_text = index > 0 ? to_string(chosen[index - 1]) : "none";
	auto cache_key = StringUtil::Format("%s:%d:%d:%s", request.prefix, table_id, snapshot_id, previous_text);
	IcebergExportedSnapshot result;
	if (server.GetExportCache().Get(cache_key, result)) {
		return result;
	}
	result.manifest_list_path = Url(StringUtil::Format("snap-%d-%s.avro", snapshot_id, previous_text));
	if (!server.VirtualFiles()) {
		// the files are written now, virtual files are generated when a client asks for them
		GenerateManifestList(snapshot_id, previous_text);
	}
	GatheredItems gathered;
	GatherItems(snapshot_id, gathered, false);
	auto &data_list = gathered.data_list;
	auto &delete_list = gathered.delete_list;
	idx_t delete_rows = gathered.delete_rows;
	idx_t total_rows = 0, total_size = 0, data_file_count = 0;
	for (auto &entry : gathered.data_items) {
		for (auto &item : entry.second) {
			total_rows += item.record_count;
			total_size += item.file_size;
			data_file_count++;
		}
	}

	// the operation: what changed compared to the previous snapshot
	set<idx_t> previous_data, current_data, previous_deletes, current_deletes;
	if (index > 0) {
		for (auto file : VisibleDataFiles(chosen[index - 1])) {
			previous_data.insert(file->id);
		}
		for (auto file : VisibleDeleteFiles(chosen[index - 1])) {
			previous_deletes.insert(file->id);
		}
	}
	for (auto file : data_list) {
		current_data.insert(file->id);
	}
	for (auto file : delete_list) {
		current_deletes.insert(file->id);
	}
	idx_t added_data = 0, removed_data = 0, added_deletes = 0, removed_deletes = 0;
	idx_t added_data_rows = 0;
	for (auto file : data_list) {
		if (previous_data.find(file->id) == previous_data.end()) {
			added_data++;
			added_data_rows += file->record_count;
		}
	}
	for (auto id : previous_data) {
		removed_data += current_data.find(id) == current_data.end();
	}
	for (auto file : delete_list) {
		added_deletes += previous_deletes.find(file->id) == previous_deletes.end();
	}
	for (auto id : previous_deletes) {
		removed_deletes += current_deletes.find(id) == current_deletes.end();
	}
	added_deletes += gathered.inlined_delete_count;
	added_data += gathered.inlined_file_count;
	added_data_rows += gathered.inlined_rows;
	if (removed_data == 0 && removed_deletes == 0 && added_deletes == 0) {
		result.operation = "append";
	} else if (added_data == 0 && added_deletes == 0) {
		result.operation = "delete";
	} else {
		result.operation = "overwrite";
	}
	result.summary["added-data-files"] = to_string(added_data);
	result.summary["deleted-data-files"] = to_string(removed_data);
	result.summary["added-records"] = to_string(added_data_rows);
	result.summary["added-delete-files"] = to_string(added_deletes);
	result.summary["removed-delete-files"] = to_string(removed_deletes);
	result.summary["total-data-files"] = to_string(data_file_count);
	result.summary["total-delete-files"] = to_string(delete_list.size());
	result.summary["total-records"] = to_string(total_rows);
	result.summary["total-files-size"] = to_string(total_size);
	result.summary["total-position-deletes"] = to_string(delete_rows);
	result.summary["total-equality-deletes"] = "0";
	server.GetExportCache().Put(cache_key, result);
	return result;
}

} // namespace

IcebergTableExporter::IcebergTableExporter(IcebergRequest &request_p) : request(request_p) {
}

IcebergTableLoad IcebergTableExporter::Export(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table,
                                              const string &snapshots) {
	TableExport table_export(request, schema, table);
	return table_export.Run(snapshots);
}

string IcebergTableExporter::GenerateFile(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table,
                                          const string &name) {
	TableExport table_export(request, schema, table);
	if (StringUtil::StartsWith(name, "meta-")) {
		return table_export.Run("all").metadata_json;
	}
	table_export.generate_name = name;
	table_export.Run("all");
	return table_export.generated;
}

string IcebergTableExporter::PlanScan(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table,
                                      optional_idx snapshot_id) {
	TableExport table_export(request, schema, table);
	table_export.plan_scan = true;
	table_export.plan_snapshot = snapshot_id;
	return table_export.Run("all").metadata_json;
}

IcebergTableLoad IcebergTableExporter::GetState(const DuckLakeSchemaInfo &schema, const DuckLakeTableInfo &table) {
	TableExport table_export(request, schema, table);
	table_export.state_only = true;
	return table_export.Run("all");
}

string IcebergLoadTableResponse(const IcebergTableLoad &load, bool include_config, const map<string, string> &config) {
	string result = "{\"metadata-location\": " + IcebergUtil::JsonString(load.metadata_location) +
	                ", \"metadata\": " + load.metadata_json;
	if (include_config) {
		RESTJsonWriter writer;
		writer.SetRoot(IcebergUtil::StringMap(writer.writer, config));
		result += ", \"config\": " + writer.ToString();
		if (!config.empty()) {
			// the configuration is for the storage of the table
			RESTJsonWriter credentials_writer;
			auto &w = credentials_writer.writer;
			auto credentials = w.CreateArray();
			auto entry = w.CreateObject();
			entry.AddString("prefix", load.location);
			entry.Add("config", IcebergUtil::StringMap(w, config));
			credentials.Append(entry);
			credentials_writer.SetRoot(credentials);
			result += ", \"storage-credentials\": " + credentials_writer.ToString();
		}
	}
	return result + "}";
}

} // namespace duckdb
