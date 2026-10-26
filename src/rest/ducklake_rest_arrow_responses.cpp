#include "rest/ducklake_rest_arrow_responses.hpp"

#include "rest/ducklake_rest_arrow.hpp"
#include "rest/ducklake_rest_arrow_files.hpp"
#include "rest/ducklake_rest_server.hpp"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

const char *ARROW_STREAM = "application/vnd.apache.arrow.stream";

//===--------------------------------------------------------------------===//
// Multipart
//===--------------------------------------------------------------------===//
//! The parts of a multipart/mixed body, which is written and read in memory
class MultipartWriter {
public:
	MultipartWriter() {
		static atomic<idx_t> counter {0};
		RandomEngine random(counter++ ^
		                    NumericCast<idx_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
		boundary = StringUtil::Format("ducklake-%llx-%llx", random.NextRandomInteger(), random.NextRandomInteger());
	}

	void AddPart(const string &name, const string &content_type, const string &content) {
		body += "--" + boundary + "\r\n";
		string headers =
		    "Content-Type: " + content_type + "\r\nContent-Disposition: form-data; name=\"" + name + "\"\r\n";
		// the content of the part starts at a multiple of 8 bytes, as Arrow arrays have to be aligned
		auto data_offset = body.size() + headers.size() + 2;
		auto padding = (8 - data_offset % 8) % 8;
		if (padding > 0) {
			// a header of at least 6 bytes: "X: " and the line break
			padding += padding < 5 ? 8 : 0;
			headers += "X:" + string(padding - 4, ' ') + "\r\n";
		}
		body += headers + "\r\n" + content + "\r\n";
	}

	string Finish(string &content_type) {
		body += "--" + boundary + "--\r\n";
		content_type = "multipart/mixed; boundary=" + boundary;
		return std::move(body);
	}

private:
	string boundary;
	string body;
};

struct MultipartPart {
	string name;
	string content_type;
	idx_t offset;
	idx_t size;
};

vector<MultipartPart> ParseMultipart(const string &body, const string &content_type) {
	auto boundary_start = content_type.find("boundary=");
	if (boundary_start == string::npos) {
		throw InvalidInputException("The multipart response of the DuckLake REST server has no boundary");
	}
	auto boundary = content_type.substr(boundary_start + 9);
	if (!boundary.empty() && boundary.front() == '"') {
		boundary = boundary.substr(1, boundary.find('"', 1) - 1);
	}
	auto delimiter = "--" + boundary;
	vector<MultipartPart> parts;
	auto position = body.find(delimiter);
	while (position != string::npos) {
		position += delimiter.size();
		if (body.compare(position, 2, "--") == 0) {
			break;
		}
		position = body.find("\r\n", position);
		if (position == string::npos) {
			break;
		}
		position += 2;
		auto headers_end = body.find("\r\n\r\n", position - 2);
		if (headers_end == string::npos) {
			throw InvalidInputException("The multipart response of the DuckLake REST server is malformed");
		}
		MultipartPart part;
		// the headers
		auto headers = body.substr(position, headers_end + 2 - position);
		for (auto &line : StringUtil::Split(headers, "\r\n")) {
			auto colon = line.find(':');
			if (colon == string::npos) {
				continue;
			}
			auto key = StringUtil::Lower(line.substr(0, colon));
			auto value = line.substr(colon + 1);
			StringUtil::Trim(value);
			if (key == "content-type") {
				part.content_type = value;
			} else if (key == "content-disposition") {
				auto name_start = value.find("name=\"");
				if (name_start != string::npos) {
					name_start += 6;
					part.name = value.substr(name_start, value.find('"', name_start) - name_start);
				}
			}
		}
		part.offset = headers_end + 4;
		auto next = body.find("\r\n" + delimiter, part.offset);
		if (next == string::npos) {
			throw InvalidInputException("The multipart response of the DuckLake REST server is malformed");
		}
		part.size = next - part.offset;
		parts.push_back(std::move(part));
		position = next + 2;
	}
	return parts;
}

const MultipartPart &FindPart(const vector<MultipartPart> &parts, const string &name) {
	for (auto &part : parts) {
		if (part.name == name) {
			return part;
		}
	}
	throw InvalidInputException("The multipart response of the DuckLake REST server has no part \"%s\"", name);
}

optional_ptr<const MultipartPart> TryFindPart(const vector<MultipartPart> &parts, const string &name) {
	for (auto &part : parts) {
		if (part.name == name) {
			return part;
		}
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Rows
//===--------------------------------------------------------------------===//
//! The types of the columns of inlined rows
vector<LogicalType> GetTypes(const RESTInlinedRows &rows) {
	vector<LogicalType> result;
	for (auto &column : rows.columns) {
		result.push_back(column.type);
	}
	return result;
}

//! Whether the rows are sent as an Arrow stream: rows without values stay in the JSON document
bool HasArrowRows(const unique_ptr<RESTInlinedRows> &rows) {
	return rows && rows->rows.rows && rows->rows.rows->Count() > 0;
}

//! Moves the inlined rows to an Arrow stream, and leaves their columns in the JSON document
string ExtractInlinedRows(ClientContext &context, unique_ptr<RESTInlinedRows> &rows) {
	vector<string> names;
	for (auto &column : rows->columns) {
		names.push_back(column.name);
	}
	auto stream = RESTArrow::Write(context, names, *rows->rows.rows);
	rows->rows = RESTRows(make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), GetTypes(*rows)));
	return stream;
}

void ReadInlinedRows(ClientContext &context, const shared_ptr<const string> &body, const MultipartPart &part,
                     unique_ptr<RESTInlinedRows> &rows) {
	if (!rows) {
		throw InvalidInputException("The DuckLake REST server sent rows without their columns");
	}
	vector<string> names;
	rows->rows = RESTRows(RESTArrow::Read(context, body, part.offset, part.size, GetTypes(*rows), names));
}

} // namespace

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTSelectResponse>(DuckLakeRESTRequest &request, RESTSelectResponse &value) {
	bool has_lists = !value.files.empty() || !value.extended_files.empty() || HasArrowRows(value.inlined_data);
	if (!request.accepts_arrow || !has_lists) {
		return ToJSON(value);
	}
	auto &context = *request.connection.context;
	MultipartWriter multipart;
	vector<DuckLakeFileListEntry> files;
	files.swap(value.files);
	vector<DuckLakeFileListExtendedEntry> extended_files;
	extended_files.swap(value.extended_files);
	string inlined_stream;
	bool has_inlined_rows = HasArrowRows(value.inlined_data);
	if (has_inlined_rows) {
		inlined_stream = ExtractInlinedRows(context, value.inlined_data);
	}
	multipart.AddPart("response", "application/json", ToJSON(value));
	if (!files.empty()) {
		multipart.AddPart("files", ARROW_STREAM, RESTArrowFiles::Write(context, files));
	}
	if (!extended_files.empty()) {
		multipart.AddPart("extended_files", ARROW_STREAM, RESTArrowFiles::Write(context, extended_files));
	}
	if (has_inlined_rows) {
		multipart.AddPart("inlined_data", ARROW_STREAM, inlined_stream);
	}
	return multipart.Finish(request.content_type);
}

template <>
RESTSelectResponse DuckLakeRESTSerializer::ReadResponse<RESTSelectResponse>(RESTHttpResponse &response) {
	if (!StringUtil::StartsWith(response.content_type, "multipart/mixed")) {
		RESTJsonReader document(response.body);
		return Read<RESTSelectResponse>(document.Root());
	}
	auto body = make_shared_ptr<const string>(std::move(response.body));
	auto parts = ParseMultipart(*body, response.content_type);
	auto &json = FindPart(parts, "response");
	RESTJsonReader document(body->substr(json.offset, json.size));
	auto result = Read<RESTSelectResponse>(document.Root());
	auto &context = *response.context;
	auto files = TryFindPart(parts, "files");
	if (files) {
		RESTArrowFiles::Read(context, body, files->offset, files->size, result.files);
	}
	auto extended_files = TryFindPart(parts, "extended_files");
	if (extended_files) {
		RESTArrowFiles::Read(context, body, extended_files->offset, extended_files->size, result.extended_files);
	}
	auto inlined_data = TryFindPart(parts, "inlined_data");
	if (inlined_data) {
		ReadInlinedRows(context, body, *inlined_data, result.inlined_data);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// TableChanges
//===--------------------------------------------------------------------===//
template <>
string DuckLakeRESTSerializer::ToResponse<RESTTableChangesResponse>(DuckLakeRESTRequest &request,
                                                                    RESTTableChangesResponse &value) {
	bool has_lists = !value.insertions.empty() || !value.deletions.empty() || HasArrowRows(value.inlined_data);
	if (!request.accepts_arrow || !has_lists) {
		return ToJSON(value);
	}
	auto &context = *request.connection.context;
	MultipartWriter multipart;
	vector<DuckLakeFileListEntry> insertions;
	insertions.swap(value.insertions);
	vector<DuckLakeDeleteScanEntry> deletions;
	deletions.swap(value.deletions);
	string inlined_stream;
	bool has_inlined_rows = HasArrowRows(value.inlined_data);
	if (has_inlined_rows) {
		inlined_stream = ExtractInlinedRows(context, value.inlined_data);
	}
	multipart.AddPart("response", "application/json", ToJSON(value));
	if (!insertions.empty()) {
		multipart.AddPart("insertions", ARROW_STREAM, RESTArrowFiles::Write(context, insertions));
	}
	if (!deletions.empty()) {
		multipart.AddPart("deletions", ARROW_STREAM, RESTArrowFiles::Write(context, deletions));
	}
	if (has_inlined_rows) {
		multipart.AddPart("inlined_data", ARROW_STREAM, inlined_stream);
	}
	return multipart.Finish(request.content_type);
}

template <>
RESTTableChangesResponse DuckLakeRESTSerializer::ReadResponse<RESTTableChangesResponse>(RESTHttpResponse &response) {
	if (!StringUtil::StartsWith(response.content_type, "multipart/mixed")) {
		RESTJsonReader document(response.body);
		return Read<RESTTableChangesResponse>(document.Root());
	}
	auto body = make_shared_ptr<const string>(std::move(response.body));
	auto parts = ParseMultipart(*body, response.content_type);
	auto &json = FindPart(parts, "response");
	RESTJsonReader document(body->substr(json.offset, json.size));
	auto result = Read<RESTTableChangesResponse>(document.Root());
	auto &context = *response.context;
	auto insertions = TryFindPart(parts, "insertions");
	if (insertions) {
		RESTArrowFiles::Read(context, body, insertions->offset, insertions->size, result.insertions);
	}
	auto deletions = TryFindPart(parts, "deletions");
	if (deletions) {
		RESTArrowFiles::Read(context, body, deletions->offset, deletions->size, result.deletions);
	}
	auto inlined_data = TryFindPart(parts, "inlined_data");
	if (inlined_data) {
		ReadInlinedRows(context, body, *inlined_data, result.inlined_data);
	}
	return result;
}

} // namespace duckdb
