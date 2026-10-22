$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppType
use ducklake.rest.codegen#noPrefix

/// Returns how a client connects to a catalog of the server. This is the first request of a client.
///
/// The client merges the properties of the response with its own: the `defaults` are used unless the client has a
/// value of its own, and the `overrides` are used whatever the client has. The `prefix` override is the prefix of the
/// catalog in the URLs of all other requests. The `endpoints` are the requests that the server supports, as the method and
/// the path without prefix (e.g. `GET /v1/snapshots`).
@http(method: "GET", uri: "/v1/config")
@readonly
@noPrefix
operation Config {
    input := {
        /// The name that the catalog is served under. The catalog of the server if it serves exactly one.
        @httpQuery("catalog")
        catalog_name: String
    }

    output := {
        /// Properties that the client uses if it has no value for them.
        defaults: StringMap = {}

        /// Properties that the client uses instead of its own values, e.g. `prefix`.
        overrides: StringMap = {}

        /// The requests that the server supports.
        endpoints: StringList = []
    }
}

/// Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).
///
/// The `data_path` is the resolved data path of the served DuckLake.
@http(method: "GET", uri: "/v1/metadata")
@readonly
operation GetMetadata {
    output: Metadata
}

/// Returns a snapshot together with (optionally) the catalog and the table statistics of that snapshot.
///
/// Clients pass `cached_metadata_version` and `cached_data_version` when they start a transaction, so they get the
/// latest snapshot and, if it changed since, its catalog and statistics in a single round-trip.
@http(method: "POST", uri: "/v1/snapshots/load")
@readonly
operation SnapshotData {
    input := with [SnapshotReference] {
        /// Find the snapshot of an `AT` clause instead.
        at: AtClause

        /// Whether a timestamp of `at` finds the last snapshot before or the first snapshot after it.
        bound: SnapshotBound = "upper"

        /// The newest snapshot that `at` can find, snapshots committed after a client's transaction started are not
        /// visible to it.
        newest_snapshot_id: UInt64

        /// Return the catalog of the snapshot.
        include_catalog: Boolean = false

        /// Return the table statistics of the snapshot.
        include_stats: Boolean = false

        /// The schema version of the latest catalog the client has loaded. If the snapshot has a different schema
        /// version, its catalog is returned.
        cached_metadata_version: UInt64

        /// The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a
        /// different next file id, its statistics are returned.
        cached_data_version: UInt64
    }

    output := {
        /// The snapshot.
        @required
        snapshot: Snapshot

        /// The catalog of the snapshot.
        catalog: CatalogInfo

        /// The statistics of the tables of the snapshot.
        @cppType("RESTOptional<vector<DuckLakeGlobalStatsInfo>>")
        stats: GlobalTableStatsList
    }
}

/// Plans a scan of a table.
///
/// Returns the data files of the table that are visible in a snapshot, together with their delete files and
/// inlined deletions, and optionally the rows of the table that are inlined in the catalog. With
/// `include_inlined_data`, `inlined_data` holds the visible inlined rows, with the row id, the begin and end snapshot
/// and the columns of the table at the snapshot, whatever the schema of the table was when the rows were written. It is
/// absent if the table has no inlined rows.
///
/// A scan of many files is returned in pages. The response has the first page of files, and `page_tokens` has the token
/// of every other page. A request with a `page_token` and no other member (except for the table) returns the page of
/// the token: the other members are those of the request that returned the token, and are not repeated. The pages of a
/// response can be requested in parallel and in any order. They belong to the catalog and the table of the request, and
/// expire after a while: the request fails with a `404`, and a client plans the scan again.
@http(method: "POST", uri: "/v1/tables/{table_id}/scan")
@readonly
operation Select {
    input := with [SnapshotReference] {
        /// The table to scan.
        @required
        @httpLabel
        table_id: UInt64

        /// A page token of an earlier response: returns that page of files. The request has no other members then.
        page_token: String

        /// Return the extended file list used to plan deletes and updates in `extended_files`, instead of `files`.
        extended: Boolean = false

        /// Filters to prune files with.
        filters: ScanFilters

        /// Also return the inlined rows of the table that are visible in the snapshot.
        include_inlined_data: Boolean = false

        /// The time zone of the client, which the inlined rows use when a column was changed to `TIMESTAMPTZ`. The
        /// time zone of the server if absent.
        time_zone: String

        /// The ids of the column mappings that the client has: the response only has the other column mappings that its
        /// files refer to.
        @cppType("set<idx_t>")
        known_column_mappings: UInt64List = []

        /// The columns that the client reads, as the field ids of top-level columns. The inlined rows only have these
        /// columns, besides the row id and the begin and end snapshot. All columns of the table if absent.
        @cppType("RESTOptional<vector<idx_t>>")
        columns: UInt64List
    }

    output := {
        /// The files of the scan.
        files: FileListEntryList = []

        /// The files of the scan, for `extended` scans.
        extended_files: FileListExtendedEntryList

        /// The inlined rows of the table, for `include_inlined_data`.
        inlined_data: InlinedRows

        /// The tokens of the other pages of the files of the scan, if there are many. The files of the scan are the files
        /// of the response and the files of all its pages.
        page_tokens: StringList = []

        /// The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does
        /// not have. They map the columns of files that were added with `ducklake_add_data_files` by name.
        column_mappings: NameMapList = []
    }
}

/// Counts the rows of a table that are visible in a snapshot.
@http(method: "POST", uri: "/v1/tables/{table_id}/row-count")
@readonly
operation RowCount {
    input := with [SnapshotReference] {
        /// What to count.
        @required
        count: RowCountType

        /// The table.
        @required
        @httpLabel
        table_id: UInt64
    }

    output := {
        /// The number of rows.
        row_count: UInt64
    }
}

/// Returns the snapshots, optionally filtered.
@http(method: "GET", uri: "/v1/snapshots")
@readonly
operation ListSnapshots {
    input := {
        /// Exclude the latest snapshot.
        @httpQuery("exclude_latest")
        exclude_latest: Boolean = false

        /// Only snapshots created before this timestamp.
        @cppType("string")
        @httpQuery("older_than")
        older_than: Timestamp

        /// Only these snapshots.
        @cppType("RESTOptional<vector<idx_t>>")
        @httpQuery("snapshot_id")
        snapshot_ids: UInt64List

        /// The maximum number of entries to return. The server picks a number if absent, and can return fewer entries.
        @httpQuery("page_size")
        page_size: UInt64

        /// The `next_page_token` of the previous page. The first page if absent.
        @httpQuery("page_token")
        page_token: String
    }

    output := {
        /// The snapshots, ordered by their ids.
        snapshots: SnapshotInfoList = []

        /// The token of the next page, absent on the last page.
        next_page_token: String
    }
}

/// Returns the storage used by each table at a snapshot.
@http(method: "POST", uri: "/v1/table-sizes")
@readonly
operation TableSizes {
    input := with [SnapshotReference] {
        /// The maximum number of entries to return. The server picks a number if absent, and can return fewer entries.
        @httpQuery("page_size")
        page_size: UInt64

        /// The `next_page_token` of the previous page. The first page if absent.
        @httpQuery("page_token")
        page_token: String
    }

    output := {
        /// The tables, ordered by their ids.
        tables: TableSizeList = []

        /// The token of the next page, absent on the last page.
        next_page_token: String
    }
}

/// Returns the files that hold the changes of a table between two snapshots, for change data feeds.
///
/// With `include_inlined_data`, `inlined_data` holds the inlined rows that were inserted (`insertions`) or deleted
/// (`deletions`) in the range, with the row id, the begin and end snapshot and the columns of the table at the last
/// snapshot of the range. It is absent if the table has no inlined rows.
///
/// Many files are returned in pages, see `scan`: a request is either the request for the changes, or has a `page_token`
/// and no other member (except for the table).
@http(method: "POST", uri: "/v1/tables/{table_id}/changes")
@readonly
operation TableChanges {
    input := with [SnapshotReference] {
        /// The table. The snapshot is the last snapshot of the range.
        @required
        @httpLabel
        table_id: UInt64

        /// The first snapshot of the range.
        start_snapshot: Snapshot

        /// The id of the first snapshot of the range.
        start_snapshot_id: UInt64

        /// A page token of an earlier response: returns that page of files. The request has no other members then.
        page_token: String

        /// The changes to return. Required, unless the request has a `page_token`.
        changes: TableChangeType

        /// Also return the inlined rows that changed.
        include_inlined_data: Boolean = false

        /// The time zone of the client, which the inlined rows use when a column was changed to `TIMESTAMPTZ`. The
        /// time zone of the server if absent.
        time_zone: String

        /// The ids of the column mappings that the client has: the response only has the other column mappings that its
        /// files refer to.
        @cppType("set<idx_t>")
        known_column_mappings: UInt64List = []

        /// The columns that the client reads, as the field ids of top-level columns. The inlined rows only have these
        /// columns, besides the row id and the begin and end snapshot. All columns of the table if absent.
        @cppType("RESTOptional<vector<idx_t>>")
        columns: UInt64List
    }

    output := {
        /// For `insertions`: the files with inserted rows.
        insertions: FileListEntryList = []

        /// For `deletions`: the files with deleted rows.
        deletions: DeleteScanEntryList = []

        /// The inlined rows that changed, for `include_inlined_data`.
        inlined_data: InlinedRows

        /// The tokens of the other pages of the files, see `scan`.
        page_tokens: StringList = []

        /// The column mappings that the files of the response refer to (by their `mapping_id`), and that the client does
        /// not have. They map the columns of files that were added with `ducklake_add_data_files` by name.
        column_mappings: NameMapList = []
    }
}

/// Returns the names of the tables of the DuckLake specification (`ducklake_snapshot`, `ducklake_data_file`, ...).
///
/// In SQL, `ducklake_metadata_table('lake', 'ducklake_snapshot')` reads a table of the metadata catalog of any
/// DuckLake.
@http(method: "GET", uri: "/v1/metadata-tables")
@readonly
operation DuckLakeTables {
    output := {
        /// The tables.
        table_names: StringList = []
    }
}

/// Returns the contents of a table of the DuckLake specification, as it is stored in the catalog.
///
/// This makes it possible to inspect a DuckLake or to export all of its metadata.
@http(method: "GET", uri: "/v1/metadata-tables/{table_name}")
@readonly
operation DuckLakeTable {
    input := {
        /// The table, e.g. `ducklake_snapshot`.
        @required
        @httpLabel
        table_name: String

        /// The maximum number of entries to return. The server picks a number if absent, and can return fewer entries.
        @httpQuery("page_size")
        page_size: UInt64

        /// The `next_page_token` of the previous page. The first page if absent.
        @httpQuery("page_token")
        page_token: String
    }

    output := with [RowSet] {
        /// The token of the next page, absent on the last page. The pages hold consecutive rows.
        next_page_token: String
    }
}

/// The unit of an `AT` clause.
enum AtUnit {
    /// A snapshot id.
    VERSION = "version"

    /// A timestamp.
    TIMESTAMP = "timestamp"
}

/// The `AT` clause of a time travel query.
structure AtClause {
    /// The unit of the value.
    @required
    unit: AtUnit

    /// The snapshot id or timestamp.
    @required
    value: Value
}

/// How a timestamp finds a snapshot.
enum SnapshotBound {
    /// The last snapshot before the timestamp.
    UPPER = "upper"

    /// The first snapshot after the timestamp.
    LOWER = "lower"
}

/// A filter whose value is only known while the scan runs (e.g. of a top-N).
structure DynamicFilter {
    /// The column.
    @required
    column_id: UInt64

    /// The comparison, as a DuckDB `ExpressionType` (e.g. `COMPARE_GREATERTHAN`).
    @required
    comparison_type: String

    /// The type of the column.
    @required
    type: LogicalType
}

/// Filters of a scan. Filters are optional: a server may ignore them, and a client applies them to the scanned rows
/// regardless.
///
/// `expressions` are SQL expressions that all hold for the rows of the scan. They refer to the columns of the table
/// by name, and to the fields of structs by their path (`st.a`). The server prunes files on the statistics of their
/// columns with the parts of the expressions it understands: comparisons of a column with a constant, `IN` lists of
/// constants, `IS [NOT] NULL`, and `AND` / `OR` combinations of those. The server ignores the other parts, and leaves
/// out an `OR` entirely if it does not understand one of its parts.
///
/// `dynamic_filters` let the server order the files so that these filters can prune files early. For
/// `runtime_filter_columns` the server returns the min/max statistics of the files in `column_min_max`, so the client
/// can prune files at runtime.
///
/// ```json
/// {"expressions": ["i >= 150", "s = 'a' OR st.a IN (1, 2)"],
///  "dynamic_filters": [{"column_id": 1, "comparison_type": "COMPARE_GREATERTHAN", "type": {"id": "INTEGER"}}],
///  "runtime_filter_columns": [1]}
/// ```
structure ScanFilters {
    /// SQL expressions that hold for all rows of the scan.
    expressions: StringList = []

    /// Filters whose value is only known while the scan runs.
    dynamic_filters: DynamicFilterList = []

    /// The columns to return the min/max statistics of.
    runtime_filter_columns: UInt64List = []
}

/// What `row_count` counts.
enum RowCountType {
    /// The rows in data files minus the deleted rows.
    DATA_FILES = "data_files"

    /// The same, absent if it cannot be computed exactly.
    DATA_FILES_EXACT = "data_files_exact"

    /// The inlined rows of a table.
    INLINED = "inlined"
}

/// The changes that `table_changes` returns.
enum TableChangeType {
    /// The files with inserted rows.
    INSERTIONS = "insertions"

    /// The files with deleted rows.
    DELETIONS = "deletions"
}
