$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppType

/// Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).
///
/// The `data_path` is the resolved data path of the served DuckLake.
@http(method: "POST", uri: "/v1/config")
@readonly
operation Config {
    output: Metadata
}

/// Returns a snapshot together with (optionally) the catalog and the table statistics of that snapshot.
///
/// Clients pass `cached_metadata_version` and `cached_data_version` when they start a transaction, so they get the
/// latest snapshot and, if it changed since, its catalog and statistics in a single round-trip.
@http(method: "POST", uri: "/v1/snapshot_data")
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
/// `include_inlined_data`, `inlined_data` holds the visible rows of the table for each schema version that it has
/// inlined rows for, with the row id, the begin and end snapshot and all columns of the table at that schema version.
@http(method: "POST", uri: "/v1/select")
@readonly
operation Select {
    input := with [SnapshotReference] {
        /// The table to scan.
        @required
        table_id: UInt64

        /// Return the extended file list used to plan deletes and updates in `extended_files`, instead of `files`.
        extended: Boolean = false

        /// Filters to prune files with.
        filters: ScanFilters

        /// Also return the inlined rows of the table that are visible in the snapshot.
        include_inlined_data: Boolean = false

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
        inlined_data: InlinedTableRowsList
    }
}

/// Reads the inlined rows of a table that a flush writes.
///
/// These are all rows that were inserted up to the snapshot, including the rows that were deleted, in the order in
/// which they are flushed. The rows that the transaction of the client deleted are not flushed.
@http(method: "POST", uri: "/v1/inlined_data_for_flush")
@readonly
operation InlinedDataForFlush {
    input := with [SnapshotReference] {
        /// The table.
        @required
        table_id: UInt64

        /// The schema version of the table whose columns the rows have (see `inlined_tables`).
        @required
        schema_version: UInt64

        /// The columns to read, e.g. `["i"]`. Columns can be cast to the type to read them as, e.g. `"i::BIGINT"`.
        @required
        columns: StringList

        /// The sort order of the table, as an `ORDER BY` list (e.g. `i DESC NULLS LAST`). The rows are ordered by it,
        /// then by their row id.
        order_by: String = ""

        /// Rows that the transaction of the client deleted, which are not flushed.
        @cppType("set<idx_t>")
        deleted_row_ids: UInt64List = []
    }

    output: InlinedRows
}

/// Returns the deleted versions of the inlined rows of a table that a flush writes.
///
/// The client writes delete files for the flushed rows with them.
@http(method: "POST", uri: "/v1/deleted_inlined_rows")
@readonly
operation DeletedInlinedRows {
    input := with [SnapshotReference] {
        /// The table.
        @required
        table_id: UInt64

        /// The schema version of the inlined rows.
        @required
        schema_version: UInt64

        /// Rows that the transaction of the client deleted, which are not flushed.
        @cppType("set<idx_t>")
        deleted_row_ids: UInt64List = []
    }

    output := {
        /// The deleted rows.
        rows: DeletedRowVersionList = []
    }
}

/// Returns the deletions of rows of data files that are inlined in the catalog.
///
/// Each deletion comes with the delete file of its data file, so that a client can write them to delete files.
@http(method: "POST", uri: "/v1/inlined_file_deletions")
@readonly
operation InlinedFileDeletions {
    input := with [SnapshotReference] {
        /// The table.
        @required
        table_id: UInt64
    }

    output := {
        /// The deletions.
        deletions: InlinedFileDeletionList = []
    }
}

/// Returns the data files of a table that are candidates for compaction, together with their delete files.
@http(method: "POST", uri: "/v1/files_for_compaction")
@readonly
operation FilesForCompaction {
    input := with [SnapshotReference] {
        /// The table.
        @required
        table_id: UInt64

        /// The type of the compaction.
        @required
        type: CompactionType

        /// For `rewrite_delete`: the fraction of deleted rows above which files are rewritten.
        delete_threshold: Double

        /// Only consider files of at least this size.
        min_file_size: UInt64

        /// Only consider files of at most this size.
        max_file_size: UInt64

        /// The size of the compacted files.
        @required
        target_file_size: UInt64

        /// Only consider files written at or after this timestamp.
        newer_than: Value

        /// Only consider files written before this timestamp.
        older_than: Value
    }

    output := {
        /// The candidates.
        files: CompactionFileEntryList = []
    }
}

/// Returns when a table and its schema versions were created.
@http(method: "POST", uri: "/v1/table_schema_versions")
@readonly
operation TableSchemaVersions {
    input := with [SnapshotReference] {
        /// The table. With a snapshot, the first and last schema version of the table at it are also returned.
        @required
        table_id: UInt64

        /// Also return the snapshot that created this schema version.
        schema_version: UInt64
    }

    output := {
        /// The snapshot that created the table.
        @required
        begin_snapshot: UInt64

        /// The first schema version of the table at the snapshot.
        creation: UInt64

        /// The last schema version of the table at the snapshot.
        last_change: UInt64

        /// The snapshot that created `schema_version`.
        schema_version_begin_snapshot: UInt64
    }
}

/// Counts the rows of a table that are visible in a snapshot.
@http(method: "POST", uri: "/v1/row_count")
@readonly
operation RowCount {
    input := with [SnapshotReference] {
        /// What to count.
        @required
        count: RowCountType

        /// The table.
        @required
        table_id: UInt64

        /// For inlined counts: the schema version of the inlined rows.
        schema_version: UInt64
    }

    output := {
        /// The number of rows.
        row_count: UInt64
    }
}

/// Returns the schema versions that a table has inlined rows for.
@http(method: "POST", uri: "/v1/inlined_tables")
@readonly
operation InlinedTables {
    input := with [SnapshotReference] {
        /// The table.
        @required
        table_id: UInt64
    }

    output := {
        /// The schema versions of the table whose columns inlined rows have. Rows are inlined per schema version
        /// and read with `select`.
        schema_versions: UInt64List = []
    }
}

/// Returns the next free column (field) id of a table.
@http(method: "POST", uri: "/v1/next_column_id")
@readonly
operation NextColumnId {
    input := {
        /// The table.
        @required
        table_id: UInt64
    }

    output := {
        /// The next free column id.
        @required
        next_column_id: UInt64
    }
}

/// Returns the snapshots, optionally filtered.
@http(method: "POST", uri: "/v1/list_snapshots")
@readonly
operation ListSnapshots {
    input := {
        /// Exclude the latest snapshot.
        exclude_latest: Boolean = false

        /// Only snapshots created before this timestamp.
        @cppType("string")
        older_than: Timestamp

        /// Only these snapshots.
        @cppType("RESTOptional<vector<idx_t>>")
        snapshot_ids: UInt64List
    }

    output := {
        /// The snapshots.
        snapshots: SnapshotInfoList = []
    }
}

/// Returns the storage used by each table at a snapshot.
@http(method: "POST", uri: "/v1/table_sizes")
@readonly
operation TableSizes {
    input := with [SnapshotReference] {}

    output := {
        /// The tables.
        tables: TableSizeList = []
    }
}

/// Returns the tables that have data files that are not deleted.
@http(method: "POST", uri: "/v1/tables_with_live_data_files")
@readonly
operation TablesWithLiveDataFiles {
    input := {}

    output := {
        /// The tables.
        table_ids: UInt64List = []
    }
}

/// Returns the name maps of files that were added with `ducklake_add_data_files`, starting from a mapping id.
@http(method: "POST", uri: "/v1/column_mappings")
@readonly
operation ColumnMappings {
    input := {
        /// The first mapping id to return.
        start_from: UInt64
    }

    output := {
        /// The name maps.
        mappings: NameMapList = []
    }
}

/// Returns the files that hold the changes of a table between two snapshots, for change data feeds.
///
/// With `include_inlined_data`, `inlined_data` holds the inlined rows that were inserted (`insertions`) or deleted
/// (`deletions`) in the range for each schema version that the table has inlined rows for, with the row id, the begin
/// and end snapshot and all columns of the table at that schema version.
@http(method: "POST", uri: "/v1/table_changes")
@readonly
operation TableChanges {
    input := with [SnapshotReference] {
        /// The table. The snapshot is the last snapshot of the range.
        @required
        table_id: UInt64

        /// The first snapshot of the range.
        start_snapshot: Snapshot

        /// The id of the first snapshot of the range.
        start_snapshot_id: UInt64

        /// The changes to return.
        @required
        changes: TableChangeType

        /// Also return the inlined rows that changed.
        include_inlined_data: Boolean = false

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
        inlined_data: InlinedTableRowsList
    }
}

/// Returns the names of the tables of the DuckLake specification (`ducklake_snapshot`, `ducklake_data_file`, ...).
///
/// In SQL, `ducklake_metadata_table('lake', 'ducklake_snapshot')` reads a table of the metadata catalog of any
/// DuckLake.
@http(method: "POST", uri: "/v1/ducklake_tables")
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
@http(method: "POST", uri: "/v1/ducklake_table")
@readonly
operation DuckLakeTable {
    input := {
        /// The table, e.g. `ducklake_snapshot`.
        @required
        table_name: String
    }

    output: InlinedRows
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

/// A deleted version of an inlined row.
structure DeletedRowVersion {
    /// The row id.
    @required
    row_id: Long

    /// The snapshot that inserted the row.
    @required
    begin_snapshot: Long

    /// The snapshot that deleted the row.
    @required
    end_snapshot: Long
}

/// What `row_count` counts.
enum RowCountType {
    /// The rows in data files minus the deleted rows.
    DATA_FILES = "data_files"

    /// The same, absent if it cannot be computed exactly.
    DATA_FILES_EXACT = "data_files_exact"

    /// The inlined rows of a table at a schema version.
    INLINED = "inlined"
}

/// The changes that `table_changes` returns.
enum TableChangeType {
    /// The files with inserted rows.
    INSERTIONS = "insertions"

    /// The files with deleted rows.
    DELETIONS = "deletions"
}
