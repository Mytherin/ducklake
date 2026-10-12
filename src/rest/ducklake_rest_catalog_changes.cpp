#include "rest/ducklake_rest_catalog_changes.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

static void WritePath(RESTJsonOut out, const string &key, const vector<Identifier> &path) {
	auto path_out = out.Arr(key);
	for (auto &name : path) {
		path_out.AppendStr(name.GetIdentifierName());
	}
}

static vector<Identifier> ReadPath(RESTJsonIn in, const string &key) {
	vector<Identifier> result;
	for (auto &name : in.Get(key).Items()) {
		result.emplace_back(name.Str());
	}
	return result;
}

static unique_ptr<ParsedExpression> ReadExpression(RESTJsonIn in, const string &key) {
	auto expression = in.TryGet(key);
	if (expression.IsNull()) {
		return nullptr;
	}
	return Parser::GetBuiltinParser().ParseSingleExpression(expression.Str());
}

RESTColumnDefinition DuckLakeRESTCatalogChanges::WriteColumn(const ColumnDefinition &column, bool not_null) {
	RESTColumnDefinition result;
	result.column_name = column.GetName().GetIdentifierName();
	result.column_type = column.GetType().ToString();
	if (column.HasDefaultValue()) {
		result.default_value = column.DefaultValue().ToString();
	}
	if (!column.Comment().IsNull()) {
		result.comment = column.Comment().ToString();
	}
	result.not_null = not_null;
	return result;
}

ColumnDefinition DuckLakeRESTCatalogChanges::ReadColumn(ClientContext &context, const RESTColumnDefinition &column) {
	auto type = TransformStringToLogicalType(column.column_type, context);
	ColumnDefinition result(Identifier(column.column_name), std::move(type));
	if (!column.default_value.empty()) {
		result.SetDefaultValue(Parser::GetBuiltinParser().ParseSingleExpression(column.default_value));
	}
	if (column.comment.has_value) {
		result.SetComment(Value(column.comment.value));
	}
	return result;
}

static void WriteColumnDefinition(RESTJsonOut out, const string &key, const ColumnDefinition &column, bool not_null) {
	out.SetRaw(key,
	           DuckLakeRESTSerializer::Write(*out.writer, DuckLakeRESTCatalogChanges::WriteColumn(column, not_null)));
}

static RESTColumnDefinition ReadColumnDefinition(RESTJsonIn in, const string &key) {
	return DuckLakeRESTSerializer::Read<RESTColumnDefinition>(in.Get(key));
}

void DuckLakeRESTCatalogChanges::WriteAlter(RESTJsonOut out, const AlterInfo &info) {
	switch (info.type) {
	case AlterType::ALTER_TABLE: {
		auto &alter = info.Cast<AlterTableInfo>();
		switch (alter.alter_table_type) {
		case AlterTableType::RENAME_TABLE:
			out.SetStr("type", "rename_table");
			out.SetStr("new_name", alter.Cast<RenameTableInfo>().new_table_name.GetIdentifierName());
			return;
		case AlterTableType::RENAME_COLUMN: {
			auto &rename = alter.Cast<RenameColumnInfo>();
			out.SetStr("type", "rename_column");
			out.SetStr("old_name", rename.old_name.GetIdentifierName());
			out.SetStr("new_name", rename.new_name.GetIdentifierName());
			return;
		}
		case AlterTableType::RENAME_FIELD: {
			auto &rename = alter.Cast<RenameFieldInfo>();
			out.SetStr("type", "rename_field");
			WritePath(out, "column_path", rename.column_path);
			out.SetStr("new_name", rename.new_name.GetIdentifierName());
			return;
		}
		case AlterTableType::ADD_COLUMN: {
			auto &add = alter.Cast<AddColumnInfo>();
			out.SetStr("type", "add_column");
			WriteColumnDefinition(out, "column", add.new_column, add.add_column_constraints.add_not_null);
			out.SetBool("if_not_exists", add.if_column_not_exists);
			return;
		}
		case AlterTableType::ADD_FIELD: {
			auto &add = alter.Cast<AddFieldInfo>();
			out.SetStr("type", "add_field");
			WritePath(out, "column_path", add.column_path);
			WriteColumnDefinition(out, "field", add.new_field, false);
			out.SetBool("if_not_exists", add.if_field_not_exists);
			return;
		}
		case AlterTableType::REMOVE_COLUMN: {
			auto &remove = alter.Cast<RemoveColumnInfo>();
			out.SetStr("type", "remove_column");
			out.SetStr("column_name", remove.removed_column.GetIdentifierName());
			out.SetBool("if_exists", remove.if_column_exists);
			out.SetBool("cascade", remove.cascade);
			return;
		}
		case AlterTableType::REMOVE_FIELD: {
			auto &remove = alter.Cast<RemoveFieldInfo>();
			out.SetStr("type", "remove_field");
			WritePath(out, "column_path", remove.column_path);
			out.SetBool("if_exists", remove.if_column_exists);
			out.SetBool("cascade", remove.cascade);
			return;
		}
		case AlterTableType::ALTER_COLUMN_TYPE: {
			auto &change = alter.Cast<ChangeColumnTypeInfo>();
			out.SetStr("type", "change_column_type");
			WritePath(out, "column_path", change.column_path);
			out.SetStr("column_type", change.target_type.ToString());
			if (change.expression) {
				out.SetStr("expression", change.expression->ToString());
			}
			return;
		}
		case AlterTableType::SET_DEFAULT: {
			auto &set_default = alter.Cast<SetDefaultInfo>();
			out.SetStr("type", "set_default");
			WritePath(out, "column_path", set_default.column_path);
			if (set_default.expression) {
				out.SetStr("expression", set_default.expression->ToString());
			}
			return;
		}
		case AlterTableType::SET_NOT_NULL:
			out.SetStr("type", "set_not_null");
			WritePath(out, "column_path", alter.Cast<SetNotNullInfo>().column_path);
			return;
		case AlterTableType::DROP_NOT_NULL:
			out.SetStr("type", "drop_not_null");
			WritePath(out, "column_path", alter.Cast<DropNotNullInfo>().column_path);
			return;
		case AlterTableType::SET_PARTITIONED_BY: {
			out.SetStr("type", "set_partitioned_by");
			auto keys = out.Arr("partitioned_by");
			for (auto &key : alter.Cast<SetPartitionedByInfo>().partition_keys) {
				keys.AppendStr(key->ToString());
			}
			return;
		}
		case AlterTableType::SET_SORTED_BY: {
			out.SetStr("type", "set_sorted_by");
			auto keys = out.Arr("sorted_by");
			for (auto &order : alter.Cast<SetSortedByInfo>().orders) {
				keys.AppendStr(order.ToString());
			}
			return;
		}
		case AlterTableType::SET_TABLE_OPTIONS: {
			out.SetStr("type", "set_options");
			auto options = out.Obj("options");
			for (auto &option : alter.Cast<SetTableOptionsInfo>().table_options) {
				options.SetStr(option.first, option.second->ToString());
			}
			return;
		}
		case AlterTableType::RESET_TABLE_OPTIONS: {
			out.SetStr("type", "reset_options");
			auto options = out.Arr("options");
			for (auto &option : alter.Cast<ResetTableOptionsInfo>().table_options) {
				options.AppendStr(option.GetIdentifierName());
			}
			return;
		}
		default:
			break;
		}
		throw NotImplementedException("DuckLake REST catalogs do not support ALTER TABLE type %s",
		                              EnumUtil::ToString(alter.alter_table_type));
	}
	case AlterType::ALTER_VIEW: {
		auto &alter = info.Cast<AlterViewInfo>();
		if (alter.alter_view_type != AlterViewType::RENAME_VIEW) {
			break;
		}
		out.SetStr("type", "rename_view");
		out.SetStr("new_name", alter.Cast<RenameViewInfo>().new_view_name.GetIdentifierName());
		return;
	}
	case AlterType::SET_COMMENT: {
		auto &comment = info.Cast<SetCommentInfo>();
		out.SetStr("type", "set_comment");
		out.SetStr("entry_type", EnumUtil::ToString(comment.entry_catalog_type));
		out.SetValue("comment", comment.comment_value);
		return;
	}
	case AlterType::SET_COLUMN_COMMENT: {
		auto &comment = info.Cast<SetColumnCommentInfo>();
		out.SetStr("type", "set_column_comment");
		out.SetStr("entry_type", EnumUtil::ToString(comment.catalog_entry_type));
		out.SetStr("column_name", comment.column_name.GetIdentifierName());
		out.SetValue("comment", comment.comment_value);
		return;
	}
	default:
		break;
	}
	throw NotImplementedException("DuckLake REST catalogs do not support ALTER type %s", EnumUtil::ToString(info.type));
}

unique_ptr<AlterInfo> DuckLakeRESTCatalogChanges::ReadAlter(ClientContext &context, RESTJsonIn in,
                                                            const AlterEntryData &entry) {
	auto type = in.Str("type");
	auto parser = Parser::GetBuiltinParser();
	if (type == "rename_table") {
		return make_uniq<RenameTableInfo>(entry, Identifier(in.Str("new_name")));
	}
	if (type == "rename_column") {
		return make_uniq<RenameColumnInfo>(entry, Identifier(in.Str("old_name")), Identifier(in.Str("new_name")));
	}
	if (type == "rename_field") {
		return make_uniq<RenameFieldInfo>(entry, ReadPath(in, "column_path"), Identifier(in.Str("new_name")));
	}
	if (type == "add_column") {
		auto column = ReadColumnDefinition(in, "column");
		AddColumnConstraints constraints;
		constraints.add_not_null = column.not_null;
		return make_uniq<AddColumnInfo>(entry, ReadColumn(context, column), in.Bool("if_not_exists", false),
		                                constraints);
	}
	if (type == "add_field") {
		return make_uniq<AddFieldInfo>(entry, ReadPath(in, "column_path"),
		                               ReadColumn(context, ReadColumnDefinition(in, "field")),
		                               in.Bool("if_not_exists", false));
	}
	if (type == "remove_column") {
		return make_uniq<RemoveColumnInfo>(entry, in.Str("column_name"), in.Bool("if_exists", false),
		                                   in.Bool("cascade", false));
	}
	if (type == "remove_field") {
		return make_uniq<RemoveFieldInfo>(entry, ReadPath(in, "column_path"), in.Bool("if_exists", false),
		                                  in.Bool("cascade", false));
	}
	if (type == "change_column_type") {
		auto target_type = TransformStringToLogicalType(in.Str("column_type"), context);
		return make_uniq<ChangeColumnTypeInfo>(entry, ReadPath(in, "column_path"), std::move(target_type),
		                                       ReadExpression(in, "expression"));
	}
	if (type == "set_default") {
		return make_uniq<SetDefaultInfo>(entry, ReadPath(in, "column_path"), ReadExpression(in, "expression"));
	}
	if (type == "set_not_null") {
		return make_uniq<SetNotNullInfo>(entry, ReadPath(in, "column_path"));
	}
	if (type == "drop_not_null") {
		return make_uniq<DropNotNullInfo>(entry, ReadPath(in, "column_path"));
	}
	if (type == "set_partitioned_by") {
		vector<unique_ptr<ParsedExpression>> keys;
		for (auto &key : in.Get("partitioned_by").Items()) {
			keys.push_back(parser.ParseSingleExpression(key.Str()));
		}
		return make_uniq<SetPartitionedByInfo>(entry, std::move(keys));
	}
	if (type == "set_sorted_by") {
		vector<OrderByNode> orders;
		for (auto &key : in.Get("sorted_by").Items()) {
			for (auto &order : parser.ParseOrderList(key.Str())) {
				orders.push_back(std::move(order));
			}
		}
		return make_uniq<SetSortedByInfo>(entry, std::move(orders));
	}
	if (type == "set_options") {
		case_insensitive_map_t<unique_ptr<ParsedExpression>> options;
		for (auto &option : in.Get("options").Members()) {
			options[option.first] = parser.ParseSingleExpression(option.second.Str());
		}
		return make_uniq<SetTableOptionsInfo>(entry, std::move(options));
	}
	if (type == "reset_options") {
		identifier_set_t options;
		for (auto &option : in.Get("options").Items()) {
			options.insert(Identifier(option.Str()));
		}
		return make_uniq<ResetTableOptionsInfo>(entry, std::move(options));
	}
	if (type == "rename_view") {
		return make_uniq<RenameViewInfo>(entry, Identifier(in.Str("new_name")));
	}
	if (type == "set_comment") {
		auto entry_type = EnumUtil::FromString<CatalogType>(in.Str("entry_type"));
		auto result = make_uniq<SetCommentInfo>(entry_type, Identifier(), Identifier(), Identifier(),
		                                        in.GetValue("comment"), entry.if_not_found);
		result->SetQualifiedName(entry.qualified_name);
		return std::move(result);
	}
	if (type == "set_column_comment") {
		auto result =
		    make_uniq<SetColumnCommentInfo>(Identifier(), Identifier(), Identifier(), Identifier(in.Str("column_name")),
		                                    in.GetValue("comment"), entry.if_not_found);
		result->catalog_entry_type = EnumUtil::FromString<CatalogType>(in.Str("entry_type"));
		result->SetQualifiedName(entry.qualified_name);
		return std::move(result);
	}
	throw NotImplementedException("Unsupported alter type \"%s\"", type);
}

} // namespace duckdb
