#include "rest/ducklake_rest_serialization.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_extra_stats.hpp"

namespace duckdb {

template <class T>
static void SetIndex(RESTJsonOut &out, const string &key, const T &index) {
	if (index.IsValid()) {
		out.SetUInt(key, index.index);
	} else {
		out.SetNull(key);
	}
}

template <class T>
static T GetIndex(const RESTJsonIn &in, const string &key) {
	auto entry = in.TryGet(key);
	if (entry.IsNull()) {
		return T();
	}
	return T(entry.UInt());
}

//===--------------------------------------------------------------------===//
// Snapshot / Metadata
//===--------------------------------------------------------------------===//
void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeSnapshot &snapshot) {
	out.SetUInt("snapshot_id", snapshot.snapshot_id);
	out.SetUInt("schema_version", snapshot.schema_version);
	out.SetUInt("next_catalog_id", snapshot.next_catalog_id);
	out.SetUInt("next_file_id", snapshot.next_file_id);
}

DuckLakeSnapshot DuckLakeRESTSerializer::ReadSnapshot(RESTJsonIn in) {
	return DuckLakeSnapshot(in.UInt("snapshot_id"), in.UInt("schema_version"), in.UInt("next_catalog_id"),
	                        in.UInt("next_file_id"));
}

void DuckLakeRESTSerializer::WriteTags(RESTJsonOut out, const string &key, const vector<DuckLakeTag> &tags) {
	auto tag_out = out.Arr(key);
	for (auto &tag : tags) {
		auto entry = tag_out.AppendObj();
		entry.SetStr("key", tag.key);
		entry.SetStr("value", tag.value);
	}
}

vector<DuckLakeTag> DuckLakeRESTSerializer::ReadTags(RESTJsonIn in) {
	vector<DuckLakeTag> result;
	for (auto &entry : in.Items()) {
		DuckLakeTag tag;
		tag.key = entry.Str("key");
		tag.value = entry.Str("value", string());
		result.push_back(std::move(tag));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeMetadata &metadata) {
	WriteTags(out, "tags", metadata.tags);
	auto schema_settings = out.Arr("schema_settings");
	for (auto &setting : metadata.schema_settings) {
		auto entry = schema_settings.AppendObj();
		SetIndex(entry, "schema_id", setting.schema_id);
		entry.SetStr("key", setting.tag.key);
		entry.SetStr("value", setting.tag.value);
	}
	auto table_settings = out.Arr("table_settings");
	for (auto &setting : metadata.table_settings) {
		auto entry = table_settings.AppendObj();
		SetIndex(entry, "table_id", setting.table_id);
		entry.SetStr("key", setting.tag.key);
		entry.SetStr("value", setting.tag.value);
	}
}

DuckLakeMetadata DuckLakeRESTSerializer::ReadMetadata(RESTJsonIn in) {
	DuckLakeMetadata result;
	result.tags = ReadTags(in.TryGet("tags"));
	for (auto &entry : in.TryGet("schema_settings").Items()) {
		DuckLakeSchemaSetting setting;
		setting.schema_id = GetIndex<SchemaIndex>(entry, "schema_id");
		setting.tag.key = entry.Str("key");
		setting.tag.value = entry.Str("value", string());
		result.schema_settings.push_back(std::move(setting));
	}
	for (auto &entry : in.TryGet("table_settings").Items()) {
		DuckLakeTableSetting setting;
		setting.table_id = GetIndex<TableIndex>(entry, "table_id");
		setting.tag.key = entry.Str("key");
		setting.tag.value = entry.Str("value", string());
		result.table_settings.push_back(std::move(setting));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//
void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeSchemaInfo &schema) {
	SetIndex(out, "schema_id", schema.id);
	SetIndex(out, "parent_schema_id", schema.parent_id);
	out.SetStr("schema_uuid", schema.uuid);
	out.SetStr("schema_name", schema.name);
	out.SetStr("path", schema.path);
	WriteTags(out, "tags", schema.tags);
}

DuckLakeSchemaInfo DuckLakeRESTSerializer::ReadSchemaInfo(RESTJsonIn in) {
	DuckLakeSchemaInfo result;
	result.id = GetIndex<SchemaIndex>(in, "schema_id");
	result.parent_id = GetIndex<SchemaIndex>(in, "parent_schema_id");
	result.uuid = in.Str("schema_uuid", string());
	result.name = in.Str("schema_name");
	result.path = in.Str("path", string());
	result.tags = ReadTags(in.TryGet("tags"));
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeColumnInfo &column) {
	SetIndex(out, "column_id", column.id);
	out.SetStr("column_name", column.name);
	out.SetStr("column_type", column.type);
	out.SetValue("initial_default", column.initial_default);
	out.SetValue("default_value", column.default_value);
	out.SetStr("default_value_type", column.default_value_type);
	out.SetBool("nulls_allowed", column.nulls_allowed);
	WriteTags(out, "tags", column.tags);
	if (!column.children.empty()) {
		auto children = out.Arr("children");
		for (auto &child : column.children) {
			Write(children.AppendObj(), child);
		}
	}
}

DuckLakeColumnInfo DuckLakeRESTSerializer::ReadColumnInfo(RESTJsonIn in) {
	DuckLakeColumnInfo result;
	result.id = GetIndex<FieldIndex>(in, "column_id");
	result.name = in.Str("column_name");
	result.type = in.Str("column_type");
	result.initial_default = in.GetValue("initial_default");
	result.default_value = in.GetValue("default_value");
	result.default_value_type = in.Str("default_value_type", string());
	result.nulls_allowed = in.Bool("nulls_allowed", true);
	result.tags = ReadTags(in.TryGet("tags"));
	for (auto &child : in.TryGet("children").Items()) {
		result.children.push_back(ReadColumnInfo(child));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeTableInfo &table) {
	SetIndex(out, "table_id", table.id);
	SetIndex(out, "schema_id", table.schema_id);
	out.SetStr("table_uuid", table.uuid);
	out.SetStr("table_name", table.name);
	out.SetStr("path", table.path);
	auto columns = out.Arr("columns");
	for (auto &column : table.columns) {
		Write(columns.AppendObj(), column);
	}
	WriteTags(out, "tags", table.tags);
	auto inlined = out.Arr("inlined_data_tables");
	for (auto &inlined_table : table.inlined_data_tables) {
		auto entry = inlined.AppendObj();
		entry.SetStr("table_name", inlined_table.table_name);
		entry.SetUInt("schema_version", inlined_table.schema_version);
	}
}

DuckLakeTableInfo DuckLakeRESTSerializer::ReadTableInfo(RESTJsonIn in) {
	DuckLakeTableInfo result;
	result.id = GetIndex<TableIndex>(in, "table_id");
	result.schema_id = GetIndex<SchemaIndex>(in, "schema_id");
	result.uuid = in.Str("table_uuid", string());
	result.name = in.Str("table_name");
	result.path = in.Str("path", string());
	for (auto &column : in.TryGet("columns").Items()) {
		result.columns.push_back(ReadColumnInfo(column));
	}
	result.tags = ReadTags(in.TryGet("tags"));
	for (auto &entry : in.TryGet("inlined_data_tables").Items()) {
		DuckLakeInlinedTableInfo inlined_table;
		inlined_table.table_name = entry.Str("table_name");
		inlined_table.schema_version = entry.UInt("schema_version");
		result.inlined_data_tables.push_back(std::move(inlined_table));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeViewInfo &view) {
	SetIndex(out, "view_id", view.id);
	SetIndex(out, "schema_id", view.schema_id);
	out.SetStr("view_uuid", view.uuid);
	out.SetStr("view_name", view.name);
	out.SetStr("dialect", view.dialect);
	auto aliases = out.Arr("column_aliases");
	for (auto &alias : view.column_aliases) {
		aliases.AppendStr(alias);
	}
	out.SetStr("sql", view.sql);
	WriteTags(out, "tags", view.tags);
	auto column_tags = out.Arr("column_tags");
	for (auto &tag : view.column_tags) {
		auto entry = column_tags.AppendObj();
		entry.SetStr("column_name", tag.column_name);
		entry.SetStr("key", tag.key);
		entry.SetValue("value", tag.value);
	}
}

DuckLakeViewInfo DuckLakeRESTSerializer::ReadViewInfo(RESTJsonIn in) {
	DuckLakeViewInfo result;
	result.id = GetIndex<TableIndex>(in, "view_id");
	result.schema_id = GetIndex<SchemaIndex>(in, "schema_id");
	result.uuid = in.Str("view_uuid", string());
	result.name = in.Str("view_name");
	result.dialect = in.Str("dialect", "duckdb");
	for (auto &alias : in.TryGet("column_aliases").Items()) {
		result.column_aliases.push_back(alias.Str());
	}
	result.sql = in.Str("sql");
	result.tags = ReadTags(in.TryGet("tags"));
	for (auto &entry : in.TryGet("column_tags").Items()) {
		DuckLakeViewColumnTag tag;
		tag.column_name = entry.Str("column_name");
		tag.key = entry.Str("key");
		tag.value = entry.GetValue("value");
		result.column_tags.push_back(std::move(tag));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeMacroInfo &macro) {
	SetIndex(out, "schema_id", macro.schema_id);
	SetIndex(out, "macro_id", macro.macro_id);
	out.SetStr("macro_name", macro.macro_name);
	auto implementations = out.Arr("implementations");
	for (auto &impl : macro.implementations) {
		auto impl_out = implementations.AppendObj();
		impl_out.SetStr("dialect", impl.dialect);
		impl_out.SetStr("sql", impl.sql);
		impl_out.SetStr("type", impl.type);
		auto parameters = impl_out.Arr("parameters");
		for (auto &param : impl.parameters) {
			auto param_out = parameters.AppendObj();
			param_out.SetStr("parameter_name", param.parameter_name);
			param_out.SetStr("parameter_type", param.parameter_type);
			param_out.SetValue("default_value", param.default_value);
			param_out.SetStr("default_value_type", param.default_value_type);
		}
	}
}

DuckLakeMacroInfo DuckLakeRESTSerializer::ReadMacroInfo(RESTJsonIn in) {
	DuckLakeMacroInfo result;
	result.schema_id = GetIndex<SchemaIndex>(in, "schema_id");
	result.macro_id = GetIndex<MacroIndex>(in, "macro_id");
	result.macro_name = in.Str("macro_name");
	for (auto &impl_in : in.TryGet("implementations").Items()) {
		DuckLakeMacroImplementation impl;
		impl.dialect = impl_in.Str("dialect", "duckdb");
		impl.sql = impl_in.Str("sql");
		impl.type = impl_in.Str("type");
		for (auto &param_in : impl_in.TryGet("parameters").Items()) {
			DuckLakeMacroParameters param;
			param.parameter_name = param_in.Str("parameter_name");
			param.parameter_type = param_in.Str("parameter_type", string());
			param.default_value = param_in.GetValue("default_value");
			param.default_value_type = param_in.Str("default_value_type", string());
			impl.parameters.push_back(std::move(param));
		}
		result.implementations.push_back(std::move(impl));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakePartitionInfo &partition) {
	out.SetOpt("partition_id", partition.id);
	SetIndex(out, "table_id", partition.table_id);
	auto fields = out.Arr("fields");
	for (auto &field : partition.fields) {
		auto field_out = fields.AppendObj();
		field_out.SetUInt("partition_key_index", field.partition_key_index);
		SetIndex(field_out, "column_id", field.field_id);
		field_out.SetStr("transform", field.transform);
	}
}

DuckLakePartitionInfo DuckLakeRESTSerializer::ReadPartitionInfo(RESTJsonIn in) {
	DuckLakePartitionInfo result;
	result.id = in.Opt("partition_id");
	result.table_id = GetIndex<TableIndex>(in, "table_id");
	for (auto &field_in : in.TryGet("fields").Items()) {
		DuckLakePartitionFieldInfo field;
		field.partition_key_index = field_in.UInt("partition_key_index");
		field.field_id = GetIndex<FieldIndex>(field_in, "column_id");
		field.transform = field_in.Str("transform");
		result.fields.push_back(std::move(field));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeSortInfo &sort) {
	out.SetOpt("sort_id", sort.id);
	SetIndex(out, "table_id", sort.table_id);
	auto fields = out.Arr("fields");
	for (auto &field : sort.fields) {
		auto field_out = fields.AppendObj();
		field_out.SetUInt("sort_key_index", field.sort_key_index);
		field_out.SetStr("expression", field.expression);
		field_out.SetStr("dialect", field.dialect);
		field_out.SetStr("sort_direction", EnumUtil::ToString(field.sort_direction));
		field_out.SetStr("null_order", EnumUtil::ToString(field.null_order));
	}
}

DuckLakeSortInfo DuckLakeRESTSerializer::ReadSortInfo(RESTJsonIn in) {
	DuckLakeSortInfo result;
	result.id = in.Opt("sort_id");
	result.table_id = GetIndex<TableIndex>(in, "table_id");
	for (auto &field_in : in.TryGet("fields").Items()) {
		DuckLakeSortFieldInfo field;
		field.sort_key_index = field_in.UInt("sort_key_index");
		field.expression = field_in.Str("expression");
		field.dialect = field_in.Str("dialect", "duckdb");
		field.sort_direction = EnumUtil::FromString<OrderType>(field_in.Str("sort_direction"));
		field.null_order = EnumUtil::FromString<OrderByNullType>(field_in.Str("null_order"));
		result.fields.push_back(std::move(field));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeCatalogInfo &catalog) {
	auto schemas = out.Arr("schemas");
	for (auto &schema : catalog.schemas) {
		Write(schemas.AppendObj(), schema);
	}
	auto tables = out.Arr("tables");
	for (auto &table : catalog.tables) {
		Write(tables.AppendObj(), table);
	}
	auto views = out.Arr("views");
	for (auto &view : catalog.views) {
		Write(views.AppendObj(), view);
	}
	auto macros = out.Arr("macros");
	for (auto &macro : catalog.macros) {
		Write(macros.AppendObj(), macro);
	}
	auto partitions = out.Arr("partitions");
	for (auto &partition : catalog.partitions) {
		Write(partitions.AppendObj(), partition);
	}
	auto sorts = out.Arr("sorts");
	for (auto &sort : catalog.sorts) {
		Write(sorts.AppendObj(), sort);
	}
}

DuckLakeCatalogInfo DuckLakeRESTSerializer::ReadCatalogInfo(RESTJsonIn in) {
	DuckLakeCatalogInfo result;
	for (auto &entry : in.TryGet("schemas").Items()) {
		result.schemas.push_back(ReadSchemaInfo(entry));
	}
	for (auto &entry : in.TryGet("tables").Items()) {
		result.tables.push_back(ReadTableInfo(entry));
	}
	for (auto &entry : in.TryGet("views").Items()) {
		result.views.push_back(ReadViewInfo(entry));
	}
	for (auto &entry : in.TryGet("macros").Items()) {
		result.macros.push_back(ReadMacroInfo(entry));
	}
	for (auto &entry : in.TryGet("partitions").Items()) {
		result.partitions.push_back(ReadPartitionInfo(entry));
	}
	for (auto &entry : in.TryGet("sorts").Items()) {
		result.sorts.push_back(ReadSortInfo(entry));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Stats
//===--------------------------------------------------------------------===//
static void SetOptionalString(RESTJsonOut &out, const string &key, bool has_value, const string &value) {
	if (has_value) {
		out.SetStr(key, value);
	} else {
		out.SetNull(key);
	}
}

static void SetOptionalBool(RESTJsonOut &out, const string &key, bool has_value, bool value) {
	if (has_value) {
		out.SetBool(key, value);
	} else {
		out.SetNull(key);
	}
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeGlobalStatsInfo &stats) {
	SetIndex(out, "table_id", stats.table_id);
	out.SetBool("initialized", stats.initialized);
	if (stats.record_count_unknown) {
		out.SetNull("record_count");
	} else {
		out.SetUInt("record_count", stats.record_count);
	}
	out.SetUInt("next_row_id", stats.next_row_id);
	if (stats.missing_table_size) {
		out.SetNull("file_size_bytes");
	} else {
		out.SetUInt("file_size_bytes", stats.table_size_bytes);
	}
	auto columns = out.Arr("column_stats");
	for (auto &col : stats.column_stats) {
		auto col_out = columns.AppendObj();
		SetIndex(col_out, "column_id", col.column_id);
		SetOptionalBool(col_out, "contains_null", col.has_contains_null, col.contains_null);
		SetOptionalBool(col_out, "contains_nan", col.has_contains_nan, col.contains_nan);
		SetOptionalString(col_out, "min_value", col.has_min, col.min_val);
		SetOptionalString(col_out, "max_value", col.has_max, col.max_val);
		SetOptionalString(col_out, "extra_stats", col.has_extra_stats, col.extra_stats);
		col_out.SetBool("min_is_exact", col.min_is_exact);
		col_out.SetBool("max_is_exact", col.max_is_exact);
	}
}

DuckLakeGlobalStatsInfo DuckLakeRESTSerializer::ReadGlobalStatsInfo(RESTJsonIn in) {
	DuckLakeGlobalStatsInfo result;
	result.table_id = GetIndex<TableIndex>(in, "table_id");
	result.initialized = in.Bool("initialized", true);
	auto record_count = in.TryGet("record_count");
	result.record_count_unknown = record_count.IsNull();
	result.record_count = record_count.IsNull() ? 0 : record_count.UInt();
	result.next_row_id = in.UInt("next_row_id", 0);
	auto file_size = in.TryGet("file_size_bytes");
	result.missing_table_size = file_size.IsNull();
	result.table_size_bytes = file_size.IsNull() ? 0 : file_size.UInt();
	for (auto &col_in : in.TryGet("column_stats").Items()) {
		DuckLakeGlobalColumnStatsInfo col;
		col.column_id = GetIndex<FieldIndex>(col_in, "column_id");
		auto contains_null = col_in.TryGet("contains_null");
		col.has_contains_null = !contains_null.IsNull();
		col.contains_null = col.has_contains_null && contains_null.Bool();
		auto contains_nan = col_in.TryGet("contains_nan");
		col.has_contains_nan = !contains_nan.IsNull();
		col.contains_nan = col.has_contains_nan && contains_nan.Bool();
		auto min_val = col_in.TryGet("min_value");
		col.has_min = !min_val.IsNull();
		col.min_val = col.has_min ? min_val.Str() : string();
		auto max_val = col_in.TryGet("max_value");
		col.has_max = !max_val.IsNull();
		col.max_val = col.has_max ? max_val.Str() : string();
		auto extra_stats = col_in.TryGet("extra_stats");
		col.has_extra_stats = !extra_stats.IsNull();
		col.extra_stats = col.has_extra_stats ? extra_stats.Str() : string();
		col.min_is_exact = col_in.Bool("min_is_exact", false);
		col.max_is_exact = col_in.Bool("max_is_exact", false);
		result.column_stats.push_back(std::move(col));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeColumnStats &stats) {
	out.SetRaw("type", RESTJson::TypeToJson(*out.writer, stats.type));
	SetOptionalString(out, "min_value", stats.has_min, stats.min);
	SetOptionalString(out, "max_value", stats.has_max, stats.max);
	if (stats.has_null_count) {
		out.SetUInt("null_count", stats.null_count);
	} else {
		out.SetNull("null_count");
	}
	if (stats.has_num_values) {
		out.SetUInt("value_count", stats.num_values);
	} else {
		out.SetNull("value_count");
	}
	out.SetUInt("column_size_bytes", stats.column_size_bytes);
	SetOptionalBool(out, "contains_nan", stats.has_contains_nan, stats.contains_nan);
	out.SetBool("any_valid", stats.any_valid);
	out.SetBool("bounds_unknown", stats.bounds_unknown);
	out.SetBool("min_is_exact", stats.min_is_exact);
	out.SetBool("max_is_exact", stats.max_is_exact);
	string extra_stats;
	if (stats.extra_stats && stats.extra_stats->TrySerialize(extra_stats)) {
		out.SetStr("extra_stats", DuckLakeUtil::UnquoteSQLString(extra_stats));
	}
}

DuckLakeColumnStats DuckLakeRESTSerializer::ReadColumnStats(RESTJsonIn in) {
	DuckLakeColumnStats result(RESTJson::TypeFromJson(in.Get("type")));
	auto min_val = in.TryGet("min_value");
	result.has_min = !min_val.IsNull();
	result.min = result.has_min ? min_val.Str() : string();
	auto max_val = in.TryGet("max_value");
	result.has_max = !max_val.IsNull();
	result.max = result.has_max ? max_val.Str() : string();
	auto null_count = in.TryGet("null_count");
	result.has_null_count = !null_count.IsNull();
	result.null_count = result.has_null_count ? null_count.UInt() : 0;
	auto value_count = in.TryGet("value_count");
	result.has_num_values = !value_count.IsNull();
	result.num_values = result.has_num_values ? value_count.UInt() : 0;
	result.column_size_bytes = in.UInt("column_size_bytes", 0);
	auto contains_nan = in.TryGet("contains_nan");
	result.has_contains_nan = !contains_nan.IsNull();
	result.contains_nan = result.has_contains_nan && contains_nan.Bool();
	result.any_valid = in.Bool("any_valid", true);
	result.bounds_unknown = in.Bool("bounds_unknown", false);
	result.min_is_exact = in.Bool("min_is_exact", false);
	result.max_is_exact = in.Bool("max_is_exact", false);
	auto extra_stats = in.TryGet("extra_stats");
	if (!extra_stats.IsNull() && result.extra_stats) {
		result.extra_stats->Deserialize(extra_stats.Str());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Files
//===--------------------------------------------------------------------===//
void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeFileData &file) {
	out.SetStr("path", file.path);
	out.SetStr("encryption_key", file.encryption_key);
	out.SetUInt("file_size_bytes", file.file_size_bytes);
	out.SetOpt("footer_size", file.footer_size);
	out.SetStr("format", DeleteFileFormatToString(file.format));
}

DuckLakeFileData DuckLakeRESTSerializer::ReadFileData(RESTJsonIn in) {
	DuckLakeFileData result;
	result.path = in.Str("path", string());
	result.encryption_key = in.Str("encryption_key", string());
	result.file_size_bytes = in.UInt("file_size_bytes", 0);
	result.footer_size = in.Opt("footer_size");
	auto format = in.TryGet("format");
	if (!format.IsNull()) {
		result.format = DeleteFileFormatFromString(format.Str());
	}
	return result;
}

static const char *DataTypeToString(DuckLakeDataType type) {
	switch (type) {
	case DuckLakeDataType::DATA_FILE:
		return "data_file";
	case DuckLakeDataType::INLINED_DATA:
		return "inlined_data";
	case DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA:
		return "transaction_local_inlined_data";
	default:
		throw InternalException("Unsupported DuckLakeDataType");
	}
}

static DuckLakeDataType DataTypeFromString(const string &type) {
	if (type == "inlined_data") {
		return DuckLakeDataType::INLINED_DATA;
	}
	if (type == "transaction_local_inlined_data") {
		return DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
	}
	return DuckLakeDataType::DATA_FILE;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeFileListEntry &entry) {
	out.SetOpt("data_file_id", entry.data_file_id);
	SetIndex(out, "file_id", entry.file_id);
	Write(out.Obj("file"), entry.file);
	if (!entry.delete_file.path.empty()) {
		Write(out.Obj("delete_file"), entry.delete_file);
	}
	out.SetOpt("row_id_start", entry.row_id_start);
	out.SetOpt("snapshot_id", entry.snapshot_id);
	out.SetOpt("max_row_count", entry.max_row_count);
	out.SetOpt("snapshot_filter_max", entry.snapshot_filter_max);
	out.SetOpt("snapshot_filter_min", entry.snapshot_filter_min);
	SetIndex(out, "mapping_id", entry.mapping_id);
	out.SetStr("data_type", DataTypeToString(entry.data_type));
	auto deletions = out.Arr("inlined_file_deletions");
	for (auto &row : entry.inlined_file_deletions) {
		deletions.AppendUInt(row);
	}
	auto min_max = out.Arr("column_min_max");
	for (auto &col : entry.column_min_max) {
		auto col_out = min_max.AppendObj();
		col_out.SetUInt("column_id", col.first);
		SetOptionalString(col_out, "min_value", col.second.has_min, col.second.min);
		SetOptionalString(col_out, "max_value", col.second.has_max, col.second.max);
	}
}

DuckLakeFileListEntry DuckLakeRESTSerializer::ReadFileListEntry(RESTJsonIn in) {
	DuckLakeFileListEntry result;
	result.data_file_id = in.Opt("data_file_id");
	result.file_id = GetIndex<DataFileIndex>(in, "file_id");
	result.file = ReadFileData(in.Get("file"));
	auto delete_file = in.TryGet("delete_file");
	if (!delete_file.IsNull()) {
		result.delete_file = ReadFileData(delete_file);
	}
	result.row_id_start = in.Opt("row_id_start");
	result.snapshot_id = in.Opt("snapshot_id");
	result.max_row_count = in.Opt("max_row_count");
	result.snapshot_filter_max = in.Opt("snapshot_filter_max");
	result.snapshot_filter_min = in.Opt("snapshot_filter_min");
	result.mapping_id = GetIndex<MappingIndex>(in, "mapping_id");
	auto data_type = in.TryGet("data_type");
	if (!data_type.IsNull()) {
		result.data_type = DataTypeFromString(data_type.Str());
	}
	for (auto &row : in.TryGet("inlined_file_deletions").Items()) {
		result.inlined_file_deletions.insert(row.UInt());
	}
	for (auto &col_in : in.TryGet("column_min_max").Items()) {
		DuckLakeFileColumnStats stats;
		auto min_val = col_in.TryGet("min_value");
		stats.has_min = !min_val.IsNull();
		stats.min = stats.has_min ? min_val.Str() : string();
		auto max_val = col_in.TryGet("max_value");
		stats.has_max = !max_val.IsNull();
		stats.max = stats.has_max ? max_val.Str() : string();
		result.column_min_max[col_in.UInt("column_id")] = std::move(stats);
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeFileListExtendedEntry &entry) {
	SetIndex(out, "file_id", entry.file_id);
	SetIndex(out, "delete_file_id", entry.delete_file_id);
	Write(out.Obj("file"), entry.file);
	if (!entry.delete_file.path.empty()) {
		Write(out.Obj("delete_file"), entry.delete_file);
	}
	out.SetOpt("row_id_start", entry.row_id_start);
	out.SetOpt("snapshot_id", entry.snapshot_id);
	out.SetOpt("delete_file_begin_snapshot", entry.delete_file_begin_snapshot);
	out.SetUInt("record_count", entry.row_count);
	out.SetUInt("delete_count", entry.delete_count);
	SetIndex(out, "mapping_id", entry.mapping_id);
	out.SetStr("data_type", DataTypeToString(entry.data_type));
}

DuckLakeFileListExtendedEntry DuckLakeRESTSerializer::ReadFileListExtendedEntry(RESTJsonIn in) {
	DuckLakeFileListExtendedEntry result;
	result.file_id = GetIndex<DataFileIndex>(in, "file_id");
	result.delete_file_id = GetIndex<DataFileIndex>(in, "delete_file_id");
	result.file = ReadFileData(in.Get("file"));
	auto delete_file = in.TryGet("delete_file");
	if (!delete_file.IsNull()) {
		result.delete_file = ReadFileData(delete_file);
	}
	result.row_id_start = in.Opt("row_id_start");
	result.snapshot_id = in.Opt("snapshot_id");
	result.delete_file_begin_snapshot = in.Opt("delete_file_begin_snapshot");
	result.row_count = in.UInt("record_count");
	result.delete_count = in.UInt("delete_count", 0);
	result.mapping_id = GetIndex<MappingIndex>(in, "mapping_id");
	auto data_type = in.TryGet("data_type");
	if (!data_type.IsNull()) {
		result.data_type = DataTypeFromString(data_type.Str());
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeDeleteFile &file) {
	SetIndex(out, "data_file_id", file.data_file_id);
	out.SetStr("data_file_path", file.data_file_path);
	out.SetStr("path", file.file_name);
	out.SetStr("format", DeleteFileFormatToString(file.format));
	out.SetUInt("delete_count", file.delete_count);
	out.SetUInt("file_size_bytes", file.file_size_bytes);
	out.SetUInt("footer_size", file.footer_size);
	out.SetOpt("row_group_count", file.row_group_count);
	out.SetStr("encryption_key", file.encryption_key);
	out.SetBool("overwrites_existing_delete", file.overwrites_existing_delete);
	SetIndex(out, "overwritten_delete_file_id", file.overwritten_delete_file.delete_file_id);
	out.SetStr("overwritten_delete_file_path", file.overwritten_delete_file.path);
	out.SetOpt("begin_snapshot", file.begin_snapshot);
	out.SetOpt("max_snapshot", file.max_snapshot);
	out.SetStr("source", file.source == DeleteFileSource::FLUSH ? "flush" : "regular");
}

DuckLakeDeleteFile DuckLakeRESTSerializer::ReadDeleteFile(RESTJsonIn in) {
	DuckLakeDeleteFile result;
	result.data_file_id = GetIndex<DataFileIndex>(in, "data_file_id");
	result.data_file_path = in.Str("data_file_path", string());
	result.file_name = in.Str("path");
	result.format = DeleteFileFormatFromString(in.Str("format", "parquet"));
	result.delete_count = in.UInt("delete_count");
	result.file_size_bytes = in.UInt("file_size_bytes");
	result.footer_size = in.UInt("footer_size", 0);
	result.row_group_count = in.Opt("row_group_count");
	result.encryption_key = in.Str("encryption_key", string());
	result.overwrites_existing_delete = in.Bool("overwrites_existing_delete", false);
	result.overwritten_delete_file.delete_file_id = GetIndex<DataFileIndex>(in, "overwritten_delete_file_id");
	result.overwritten_delete_file.path = in.Str("overwritten_delete_file_path", string());
	result.begin_snapshot = in.Opt("begin_snapshot");
	result.max_snapshot = in.Opt("max_snapshot");
	result.source = in.Str("source", "regular") == "flush" ? DeleteFileSource::FLUSH : DeleteFileSource::REGULAR;
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeDataFile &file) {
	out.SetStr("path", file.file_name);
	out.SetUInt("record_count", file.row_count);
	out.SetUInt("file_size_bytes", file.file_size_bytes);
	out.SetOpt("footer_size", file.footer_size);
	out.SetOpt("row_group_count", file.row_group_count);
	out.SetOpt("partition_id", file.partition_id);
	out.SetStr("encryption_key", file.encryption_key);
	SetIndex(out, "mapping_id", file.mapping_id);
	out.SetOpt("begin_snapshot", file.begin_snapshot);
	out.SetOpt("max_partial_file_snapshot", file.max_partial_file_snapshot);
	out.SetOpt("flush_row_id_start", file.flush_row_id_start);
	out.SetBool("created_by_ducklake", file.created_by_ducklake);
	auto stats = out.Arr("column_stats");
	for (auto &entry : file.column_stats) {
		auto stats_out = stats.AppendObj();
		stats_out.SetUInt("column_id", entry.first.index);
		Write(stats_out, entry.second);
	}
	auto partition_values = out.Arr("partition_values");
	for (auto &entry : file.partition_values) {
		auto value_out = partition_values.AppendObj();
		value_out.SetUInt("partition_key_index", entry.partition_column_idx);
		value_out.SetValue("partition_value", entry.partition_value);
	}
	auto delete_files = out.Arr("delete_files");
	for (auto &delete_file : file.delete_files) {
		Write(delete_files.AppendObj(), delete_file);
	}
}

DuckLakeDataFile DuckLakeRESTSerializer::ReadDataFile(RESTJsonIn in) {
	DuckLakeDataFile result;
	result.file_name = in.Str("path");
	result.row_count = in.UInt("record_count");
	result.file_size_bytes = in.UInt("file_size_bytes");
	result.footer_size = in.Opt("footer_size");
	result.row_group_count = in.Opt("row_group_count");
	result.partition_id = in.Opt("partition_id");
	result.encryption_key = in.Str("encryption_key", string());
	result.mapping_id = GetIndex<MappingIndex>(in, "mapping_id");
	result.begin_snapshot = in.Opt("begin_snapshot");
	result.max_partial_file_snapshot = in.Opt("max_partial_file_snapshot");
	result.flush_row_id_start = in.Opt("flush_row_id_start");
	result.created_by_ducklake = in.Bool("created_by_ducklake", false);
	for (auto &stats_in : in.TryGet("column_stats").Items()) {
		result.column_stats.emplace(FieldIndex(stats_in.UInt("column_id")), ReadColumnStats(stats_in));
	}
	for (auto &value_in : in.TryGet("partition_values").Items()) {
		DuckLakeFilePartition partition;
		partition.partition_column_idx = value_in.UInt("partition_key_index");
		partition.partition_value = value_in.GetValue("partition_value");
		result.partition_values.push_back(std::move(partition));
	}
	for (auto &delete_in : in.TryGet("delete_files").Items()) {
		result.delete_files.push_back(ReadDeleteFile(delete_in));
	}
	return result;
}

static void WriteCompactionBaseFile(RESTJsonOut &out, const DuckLakeCompactionBaseFileData &file) {
	SetIndex(out, "file_id", file.id);
	DuckLakeRESTSerializer::Write(out.Obj("file"), file.data);
	out.SetUInt("record_count", file.row_count);
	out.SetUInt("begin_snapshot", file.begin_snapshot);
	out.SetOpt("end_snapshot", file.end_snapshot);
	out.SetOpt("max_row_count", file.max_row_count);
}

static void ReadCompactionBaseFile(const RESTJsonIn &in, DuckLakeCompactionBaseFileData &file) {
	file.id = GetIndex<DataFileIndex>(in, "file_id");
	file.data = DuckLakeRESTSerializer::ReadFileData(in.Get("file"));
	file.row_count = in.UInt("record_count");
	file.begin_snapshot = in.UInt("begin_snapshot");
	file.end_snapshot = in.Opt("end_snapshot");
	file.max_row_count = in.Opt("max_row_count");
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeCompactionFileEntry &entry) {
	auto file = out.Obj("data_file");
	WriteCompactionBaseFile(file, entry.file);
	file.SetOpt("row_id_start", entry.file.row_id_start);
	SetIndex(file, "mapping_id", entry.file.mapping_id);
	file.SetOpt("partition_id", entry.file.partition_id);
	auto partition_values = file.Arr("partition_values");
	for (auto &value : entry.file.partition_values) {
		partition_values.AppendRaw(RESTJson::ValueToJson(*out.writer, value));
	}
	auto delete_files = out.Arr("delete_files");
	for (auto &delete_file : entry.delete_files) {
		auto delete_out = delete_files.AppendObj();
		WriteCompactionBaseFile(delete_out, delete_file);
		SetIndex(delete_out, "delete_file_id", delete_file.delete_file_id);
		delete_out.SetOpt("max_snapshot", delete_file.max_snapshot);
	}
	out.SetOpt("max_partial_file_snapshot", entry.max_partial_file_snapshot);
	out.SetUInt("schema_version", entry.schema_version);
	out.SetOpt("partition_schema_version", entry.partition_schema_version);
	auto deletions = out.Arr("inlined_file_deletions");
	for (auto &row : entry.inlined_file_deletions) {
		deletions.AppendUInt(row);
	}
	out.SetBool("has_inlined_deletions", entry.has_inlined_deletions);
	out.SetDouble("delete_ratio", entry.delete_ratio);
}

DuckLakeCompactionFileEntry DuckLakeRESTSerializer::ReadCompactionFileEntry(RESTJsonIn in) {
	DuckLakeCompactionFileEntry result;
	auto file = in.Get("data_file");
	ReadCompactionBaseFile(file, result.file);
	result.file.row_id_start = file.Opt("row_id_start");
	result.file.mapping_id = GetIndex<MappingIndex>(file, "mapping_id");
	result.file.partition_id = file.Opt("partition_id");
	for (auto &value : file.TryGet("partition_values").Items()) {
		result.file.partition_values.push_back(RESTJson::ValueFromJson(value));
	}
	for (auto &delete_in : in.TryGet("delete_files").Items()) {
		DuckLakeCompactionDeleteFileData delete_file;
		ReadCompactionBaseFile(delete_in, delete_file);
		delete_file.delete_file_id = GetIndex<DataFileIndex>(delete_in, "delete_file_id");
		delete_file.max_snapshot = delete_in.Opt("max_snapshot");
		result.delete_files.push_back(std::move(delete_file));
	}
	result.max_partial_file_snapshot = in.Opt("max_partial_file_snapshot");
	result.schema_version = in.UInt("schema_version");
	result.partition_schema_version = in.Opt("partition_schema_version");
	for (auto &row : in.TryGet("inlined_file_deletions").Items()) {
		result.inlined_file_deletions.insert(row.UInt());
	}
	result.has_inlined_deletions = in.Bool("has_inlined_deletions", false);
	auto delete_ratio = in.TryGet("delete_ratio");
	result.delete_ratio = delete_ratio.IsNull() ? 0 : delete_ratio.Double();
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeCompactionEntry &entry) {
	out.SetStr("type", CompactionTypeToString(entry.type));
	out.SetOpt("row_id_start", entry.row_id_start);
	auto source_files = out.Arr("source_files");
	for (auto &file : entry.source_files) {
		Write(source_files.AppendObj(), file);
	}
	auto written_files = out.Arr("written_files");
	for (auto &file : entry.written_files) {
		Write(written_files.AppendObj(), file);
	}
}

DuckLakeCompactionEntry DuckLakeRESTSerializer::ReadCompactionEntry(RESTJsonIn in) {
	DuckLakeCompactionEntry result;
	result.type = CompactionTypeFromString(in.Str("type"));
	result.row_id_start = in.Opt("row_id_start");
	for (auto &file : in.TryGet("source_files").Items()) {
		result.source_files.push_back(ReadCompactionFileEntry(file));
	}
	for (auto &file : in.TryGet("written_files").Items()) {
		result.written_files.push_back(ReadDataFile(file));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeDeleteScanEntry &entry) {
	Write(out.Obj("file"), entry.file);
	if (!entry.delete_file.path.empty()) {
		Write(out.Obj("delete_file"), entry.delete_file);
	}
	if (!entry.previous_delete_file.path.empty()) {
		Write(out.Obj("previous_delete_file"), entry.previous_delete_file);
	}
	out.SetUInt("record_count", entry.row_count);
	out.SetOpt("row_id_start", entry.row_id_start);
	SetIndex(out, "mapping_id", entry.mapping_id);
	out.SetOpt("snapshot_id", entry.snapshot_id);
	out.SetBool("file_deleted", entry.file_deleted);
	out.SetOpt("start_snapshot", entry.start_snapshot);
	out.SetOpt("end_snapshot", entry.end_snapshot);
	SetIndex(out, "file_id", entry.file_id);
	auto deletions = out.Arr("inlined_file_deletions");
	for (auto &deletion : entry.inlined_file_deletions) {
		auto deletion_out = deletions.AppendObj();
		deletion_out.SetUInt("row_id", deletion.first);
		deletion_out.SetUInt("snapshot_id", deletion.second);
	}
}

DuckLakeDeleteScanEntry DuckLakeRESTSerializer::ReadDeleteScanEntry(RESTJsonIn in) {
	DuckLakeDeleteScanEntry result;
	result.file = ReadFileData(in.Get("file"));
	auto delete_file = in.TryGet("delete_file");
	if (!delete_file.IsNull()) {
		result.delete_file = ReadFileData(delete_file);
	}
	auto previous_delete_file = in.TryGet("previous_delete_file");
	if (!previous_delete_file.IsNull()) {
		result.previous_delete_file = ReadFileData(previous_delete_file);
	}
	result.row_count = in.UInt("record_count");
	result.row_id_start = in.Opt("row_id_start");
	result.mapping_id = GetIndex<MappingIndex>(in, "mapping_id");
	result.snapshot_id = in.Opt("snapshot_id");
	result.file_deleted = in.Bool("file_deleted", false);
	result.start_snapshot = in.Opt("start_snapshot");
	result.end_snapshot = in.Opt("end_snapshot");
	result.file_id = GetIndex<DataFileIndex>(in, "file_id");
	for (auto &deletion : in.TryGet("inlined_file_deletions").Items()) {
		result.inlined_file_deletions[deletion.UInt("row_id")] = deletion.UInt("snapshot_id");
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeTableSizeInfo &table_size) {
	SetIndex(out, "schema_id", table_size.schema_id);
	SetIndex(out, "table_id", table_size.table_id);
	out.SetStr("table_name", table_size.table_name);
	out.SetStr("table_uuid", table_size.table_uuid);
	out.SetUInt("file_size_bytes", table_size.file_size_bytes);
	out.SetUInt("delete_file_size_bytes", table_size.delete_file_size_bytes);
	out.SetUInt("file_count", table_size.file_count);
	out.SetUInt("delete_file_count", table_size.delete_file_count);
}

DuckLakeTableSizeInfo DuckLakeRESTSerializer::ReadTableSizeInfo(RESTJsonIn in) {
	DuckLakeTableSizeInfo result;
	result.schema_id = GetIndex<SchemaIndex>(in, "schema_id");
	result.table_id = GetIndex<TableIndex>(in, "table_id");
	result.table_name = in.Str("table_name");
	result.table_uuid = in.Str("table_uuid", string());
	result.file_size_bytes = in.UInt("file_size_bytes", 0);
	result.delete_file_size_bytes = in.UInt("delete_file_size_bytes", 0);
	result.file_count = in.UInt("file_count", 0);
	result.delete_file_count = in.UInt("delete_file_count", 0);
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeColumnMappingInfo &mapping) {
	SetIndex(out, "table_id", mapping.table_id);
	SetIndex(out, "mapping_id", mapping.mapping_id);
	out.SetStr("type", mapping.map_type);
	auto columns = out.Arr("columns");
	for (auto &column : mapping.map_columns) {
		auto column_out = columns.AppendObj();
		column_out.SetUInt("column_id", column.column_id);
		column_out.SetStr("source_name", column.source_name);
		SetIndex(column_out, "target_field_id", column.target_field_id);
		column_out.SetBool("hive_partition", column.hive_partition);
		column_out.SetOpt("parent_column", column.parent_column);
	}
}

DuckLakeColumnMappingInfo DuckLakeRESTSerializer::ReadColumnMappingInfo(RESTJsonIn in) {
	DuckLakeColumnMappingInfo result;
	result.table_id = GetIndex<TableIndex>(in, "table_id");
	result.mapping_id = GetIndex<MappingIndex>(in, "mapping_id");
	result.map_type = in.Str("type", "map_by_name");
	for (auto &column_in : in.TryGet("columns").Items()) {
		DuckLakeNameMapColumnInfo column;
		column.column_id = column_in.UInt("column_id");
		column.source_name = column_in.Str("source_name");
		column.target_field_id = GetIndex<FieldIndex>(column_in, "target_field_id");
		column.hive_partition = column_in.Bool("hive_partition", false);
		column.parent_column = column_in.Opt("parent_column");
		result.map_columns.push_back(std::move(column));
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeSnapshotInfo &snapshot) {
	out.SetUInt("snapshot_id", snapshot.id);
	out.SetStr("snapshot_time", Value::TIMESTAMPTZ(snapshot.time).ToString());
	out.SetUInt("schema_version", snapshot.schema_version);
	out.SetUInt("next_file_id", snapshot.next_file_id);
	out.SetStr("changes_made", snapshot.change_info.changes_made);
	out.SetValue("author", snapshot.author);
	out.SetValue("commit_message", snapshot.commit_message);
	out.SetValue("commit_extra_info", snapshot.commit_extra_info);
}

DuckLakeSnapshotInfo DuckLakeRESTSerializer::ReadSnapshotInfo(RESTJsonIn in) {
	DuckLakeSnapshotInfo result;
	result.id = in.UInt("snapshot_id");
	result.time = Value(in.Str("snapshot_time")).DefaultCastAs(LogicalType::TIMESTAMP_TZ).GetValue<timestamp_tz_t>();
	result.schema_version = in.UInt("schema_version");
	result.next_file_id = in.UInt("next_file_id");
	result.change_info.changes_made = in.Str("changes_made", string());
	result.author = in.GetValue("author");
	result.commit_message = in.GetValue("commit_message");
	result.commit_extra_info = in.GetValue("commit_extra_info");
	return result;
}

//===--------------------------------------------------------------------===//
// Filters
//===--------------------------------------------------------------------===//
//! Filters are DuckDB table filters in the DuckDB binary serialization format, base64 encoded
static bool TryWriteColumnFilter(RESTJsonOut out, const ColumnFilterInfo &filter) {
	if (!filter.table_filter || ExpressionFilter::GetOptionalDynamicFilterData(*filter.table_filter)) {
		// dynamic filters change at runtime
		return false;
	}
	string serialized;
	try {
		MemoryStream stream;
		BinarySerializer::Serialize(static_cast<const TableFilter &>(*filter.table_filter), stream);
		serialized = Blob::ToBase64(string_t(const_char_ptr_cast(stream.GetData()), stream.GetPosition()));
	} catch (std::exception &) {
		return false;
	}
	out.SetUInt("column_id", filter.column_field_index);
	out.SetRaw("type", RESTJson::TypeToJson(*out.writer, filter.column_type));
	out.SetStr("filter", serialized);
	return true;
}

static unique_ptr<ColumnFilterInfo> TryReadColumnFilter(ClientContext &context, RESTJsonIn in) {
	try {
		auto type = RESTJson::TypeFromJson(in.Get("type"));
		auto serialized = Blob::FromBase64(string_t(in.Str("filter")));
		MemoryStream stream(data_ptr_cast(&serialized[0]), serialized.size());
		bound_parameter_map_t parameters;
		auto filter = BinaryDeserializer::Deserialize<TableFilter>(stream, context, parameters);
		auto expression_filter = ExpressionFilter::FromTableFilter(*filter, type);
		return make_uniq<ColumnFilterInfo>(in.UInt("column_id"), std::move(type), std::move(expression_filter));
	} catch (std::exception &) {
		return nullptr;
	}
}

static bool TryWriteFilterNode(RESTJsonOut out, const DuckLakeFilterNode &node) {
	switch (node.type) {
	case DuckLakeFilterNodeType::COLUMN_FILTER:
		out.SetStr("type", "column_filter");
		return TryWriteColumnFilter(out.Obj("column_filter"), *node.column_filter);
	case DuckLakeFilterNodeType::CONJUNCTION_AND:
	case DuckLakeFilterNodeType::CONJUNCTION_OR: {
		out.SetStr("type", node.type == DuckLakeFilterNodeType::CONJUNCTION_AND ? "and" : "or");
		auto children = out.Arr("children");
		for (auto &child : node.children) {
			if (!TryWriteFilterNode(children.AppendObj(), *child)) {
				return false;
			}
		}
		return true;
	}
	case DuckLakeFilterNodeType::MATCH_NONE:
		out.SetStr("type", "match_none");
		return true;
	default:
		return false;
	}
}

static unique_ptr<DuckLakeFilterNode> TryReadFilterNode(ClientContext &context, RESTJsonIn in) {
	auto type = in.Str("type");
	if (type == "column_filter") {
		auto filter = TryReadColumnFilter(context, in.Get("column_filter"));
		if (!filter) {
			return nullptr;
		}
		return make_uniq<DuckLakeFilterNode>(std::move(*filter));
	}
	if (type == "match_none") {
		return make_uniq<DuckLakeFilterNode>(DuckLakeFilterNodeType::MATCH_NONE);
	}
	if (type != "and" && type != "or") {
		return nullptr;
	}
	auto result = make_uniq<DuckLakeFilterNode>(type == "and" ? DuckLakeFilterNodeType::CONJUNCTION_AND
	                                                          : DuckLakeFilterNodeType::CONJUNCTION_OR);
	for (auto &child_in : in.Get("children").Items()) {
		auto child = TryReadFilterNode(context, child_in);
		if (!child) {
			return nullptr;
		}
		result->children.push_back(std::move(child));
	}
	return result;
}

void DuckLakeRESTSerializer::WriteFilters(RESTJsonOut out, const FilterPushdownInfo &filters) {
	auto column_filters = out.Arr("column_filters");
	for (auto &entry : filters.column_filters) {
		RESTJsonWriter filter_writer;
		if (TryWriteColumnFilter(filter_writer.Root(), entry.second)) {
			RESTJsonReader filter(filter_writer.ToString());
			column_filters.AppendRaw(out.writer->CreateCopy(filter.Root().val));
		}
	}
	auto filter_trees = out.Arr("filter_trees");
	for (auto &tree : filters.filter_trees) {
		// a tree that is not complete could prune files that match it, so it is left out as a whole
		RESTJsonWriter tree_writer;
		if (TryWriteFilterNode(tree_writer.Root(), *tree.root)) {
			RESTJsonReader tree_json(tree_writer.ToString());
			filter_trees.AppendRaw(out.writer->CreateCopy(tree_json.Root().val));
		}
	}
	vector<idx_t> runtime_filter_columns = filters.runtime_filter_columns;
	auto dynamic_filters = out.Arr("dynamic_filters");
	for (auto &entry : filters.column_filters) {
		auto &filter = entry.second;
		if (!filter.table_filter) {
			continue;
		}
		auto dynamic_filter_data = ExpressionFilter::GetOptionalDynamicFilterData(*filter.table_filter);
		if (!dynamic_filter_data) {
			continue;
		}
		ExpressionType comparison_type;
		{
			lock_guard<mutex> guard(dynamic_filter_data->lock);
			comparison_type = dynamic_filter_data->comparison_type;
		}
		auto dynamic_filter = dynamic_filters.AppendObj();
		dynamic_filter.SetUInt("column_id", filter.column_field_index);
		dynamic_filter.SetStr("comparison_type", EnumUtil::ToString(comparison_type));
		dynamic_filter.SetRaw("type", RESTJson::TypeToJson(*out.writer, filter.column_type));
		runtime_filter_columns.push_back(filter.column_field_index);
	}
	auto runtime_columns = out.Arr("runtime_filter_columns");
	for (auto column_id : runtime_filter_columns) {
		runtime_columns.AppendUInt(column_id);
	}
}

unique_ptr<FilterPushdownInfo> DuckLakeRESTSerializer::ReadFilters(ClientContext &context, RESTJsonIn in) {
	auto result = make_uniq<FilterPushdownInfo>();
	for (auto &filter_in : in.TryGet("column_filters").Items()) {
		auto filter = TryReadColumnFilter(context, filter_in);
		if (filter) {
			auto column_id = filter->column_field_index;
			result->column_filters.emplace(column_id, std::move(*filter));
		}
	}
	for (auto &tree_in : in.TryGet("filter_trees").Items()) {
		auto root = TryReadFilterNode(context, tree_in);
		if (!root) {
			continue;
		}
		DuckLakeFilterTree tree;
		tree.root = std::move(root);
		tree.source = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		result->filter_trees.push_back(std::move(tree));
	}
	for (auto &column_id : in.TryGet("runtime_filter_columns").Items()) {
		result->runtime_filter_columns.push_back(column_id.UInt());
	}
	for (auto &dynamic_filter : in.TryGet("dynamic_filters").Items()) {
		result->dynamic_filters.push_back({dynamic_filter.UInt("column_id"),
		                                   EnumUtil::FromString<ExpressionType>(dynamic_filter.Str("comparison_type")),
		                                   RESTJson::TypeFromJson(dynamic_filter.Get("type"))});
	}
	return result;
}

void DuckLakeRESTSerializer::Write(RESTJsonOut out, const DuckLakeSnapshotCommit &commit) {
	out.SetValue("author", commit.author);
	out.SetValue("commit_message", commit.commit_message);
	out.SetValue("commit_extra_info", commit.commit_extra_info);
	out.SetBool("is_commit_info_set", commit.is_commit_info_set);
}

DuckLakeSnapshotCommit DuckLakeRESTSerializer::ReadSnapshotCommit(RESTJsonIn in) {
	DuckLakeSnapshotCommit result;
	result.author = in.GetValue("author");
	result.commit_message = in.GetValue("commit_message");
	result.commit_extra_info = in.GetValue("commit_extra_info");
	result.is_commit_info_set = in.Bool("is_commit_info_set", false);
	return result;
}

} // namespace duckdb
