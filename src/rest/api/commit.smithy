$version: "2"
$operationInputSuffix: "Request"
$operationOutputSuffix: "Response"

namespace ducklake.rest

use ducklake.rest.codegen#cppMap
use ducklake.rest.codegen#cppType
use ducklake.rest.codegen#rowTypes

/// Commits a set of changes atomically.
///
/// If the transaction conflicts with a transaction that committed after its snapshot, the server retries the
/// commit when that is safe; otherwise it fails with a `409`. The snapshot is the snapshot the changes are based
/// on: the server detects conflicts with every snapshot committed after it. Without a snapshot, the changes are
/// based on the latest snapshot.
@http(method: "POST", uri: "/v1/commit_transaction")
operation CommitTransaction {
    input := with [SnapshotReference] {
        /// The commit information of the snapshot.
        commit_info: CommitInfo

        /// How the server retries the commit on conflicts.
        retry: RetryConfig

        /// The changes, applied in order, e.g. `[{"create_table": {...}}, {"insert_rows": {...}}]`.
        requests: CommitRequestList = []
    }

    output := {
        /// The committed snapshot, absent if the requests did not create a snapshot (e.g. if they only set options).
        snapshot_id: UInt64

        /// The schema version of the committed snapshot.
        schema_version: UInt64

        /// The ids of the tables the transaction created.
        table_ids: CreatedTableList = []

        /// The ids of the created entries whose requests had an `entry_reference`.
        entry_ids: CreatedEntryList = []

        /// The options of the tables the transaction created or altered, with the ids the commit gave the tables.
        table_options: OptionSettingList = []
    }
}

/// Creates a schema.
@http(method: "POST", uri: "/v1/create_schema")
operation CreateSchema {
    input: CreateSchemaRequest
}

/// The parameters of `create_schema`.
structure CreateSchemaRequest {
    /// A number of the client's choosing, which `entry_ids` returns with the id the commit gave the entry. A client can
    /// use it to keep track of the entries it created, e.g. to keep their oids stable.
    entry_reference: UInt64

    /// The path of the schema, e.g. `["parent", "child"]`.
    @required
    schema_path: StringList

    /// The uuid of the schema.
    schema_uuid: String

    /// The data path of the schema.
    path: DataPath

    /// Do nothing if the schema exists.
    if_not_exists: Boolean = false
}

/// Creates a table.
///
/// Columns get the field ids `1..n` in the order of the columns (nested fields are numbered depth-first), as in `CREATE
/// TABLE`.
@http(method: "POST", uri: "/v1/create_table")
operation CreateTable {
    input: CreateTableRequest
}

/// The parameters of `create_table`.
structure CreateTableRequest {
    /// The placeholder id of the table.
    table_id: Long

    /// See `create_schema`.
    entry_reference: UInt64

    /// The schema of the table, e.g. `["main"]`.
    schema_path: StringList = []

    /// The name of the table.
    @required
    table_name: String

    /// The uuid of the table.
    table_uuid: String

    /// The data path of the table.
    path: DataPath

    /// The columns of the table.
    @required
    columns: ColumnDefinitionList

    /// The partition keys as SQL expressions, e.g. `["year(ts)"]`.
    partitioned_by: StringList = []

    /// The sort keys as SQL expressions.
    sorted_by: StringList = []

    /// Table options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`.
    options: StringMap = {}

    /// The comment of the table.
    @cppType("RESTOptional<string>")
    comment: String

    /// An id of the client for the partitioning of the table. Files that are inserted later in the transaction refer to
    /// the partitioning with it.
    partition_id: UInt64
}

/// Creates a view.
@http(method: "POST", uri: "/v1/create_view")
operation CreateView {
    input: CreateViewRequest
}

/// The parameters of `create_view`.
structure CreateViewRequest {
    /// See `create_schema`.
    entry_reference: UInt64

    /// The schema of the view.
    schema_path: StringList = []

    /// The name of the view.
    @required
    view_name: String

    /// The uuid of the view.
    view_uuid: String

    /// The SQL dialect of the view, only `duckdb` for now.
    dialect: String = "duckdb"

    /// The query of the view.
    @required
    sql: String

    /// The names of the columns of the view.
    column_aliases: StringList = []

    /// The comment of the view.
    @cppType("RESTOptional<string>")
    comment: String

    /// The comments of the columns of the view, by column name.
    column_comments: StringMap = {}
}

/// Creates a macro.
@http(method: "POST", uri: "/v1/create_macro")
operation CreateMacro {
    input: CreateMacroRequest
}

/// The parameters of `create_macro`.
structure CreateMacroRequest {
    /// See `create_schema`.
    entry_reference: UInt64

    /// The schema of the macro.
    schema_path: StringList = []

    /// The macro (its ids are ignored).
    @required
    macro: MacroInfo
}

/// Alters a table.
@http(method: "POST", uri: "/v1/alter_table")
operation AlterTable {
    input: AlterTableRequest
}

/// The parameters of `alter_table`.
structure AlterTableRequest {
    /// The schema of the table.
    schema_path: StringList = []

    /// The name of the table.
    @required
    name: String

    /// The change.
    @required
    alter: Alteration

    /// For `set_partitioned_by`: an id of the client for the new partitioning (as for `create_table`).
    partition_id: UInt64
}

/// Alters a view.
@http(method: "POST", uri: "/v1/alter_view")
operation AlterView {
    input: AlterViewRequest
}

/// The parameters of `alter_view`.
structure AlterViewRequest {
    /// The schema of the view.
    schema_path: StringList = []

    /// The name of the view.
    @required
    name: String

    /// The change.
    @required
    alter: Alteration
}

/// Drops a schema, which has to be empty.
@http(method: "POST", uri: "/v1/drop_schema")
operation DropSchema {
    input: DropSchemaRequest
}

/// The parameters of `drop_schema`.
structure DropSchemaRequest {
    /// The path of the schema.
    @required
    schema_path: StringList
}

/// Drops a table.
@http(method: "POST", uri: "/v1/drop_table")
operation DropTable {
    input: DropTableRequest
}

/// The parameters of `drop_table`.
structure DropTableRequest {
    /// The schema of the table.
    schema_path: StringList = []

    /// The name of the table.
    @required
    name: String
}

/// Drops a view.
@http(method: "POST", uri: "/v1/drop_view")
operation DropView {
    input: DropViewRequest
}

/// The parameters of `drop_view`.
structure DropViewRequest {
    /// The schema of the view.
    schema_path: StringList = []

    /// The name of the view.
    @required
    name: String
}

/// Drops a macro.
@http(method: "POST", uri: "/v1/drop_macro")
operation DropMacro {
    input: DropMacroRequest
}

/// The parameters of `drop_macro`.
structure DropMacroRequest {
    /// The schema of the macro.
    schema_path: StringList = []

    /// The name of the macro.
    @required
    name: String

    /// The type of the macro.
    macro_type: MacroType = "scalar"
}

/// Sets or resets a DuckLake option (`ducklake_metadata`).
@http(method: "POST", uri: "/v1/set_option")
operation SetOption {
    input: SetOptionRequest
}

/// The parameters of `set_option`.
structure SetOptionRequest {
    /// The option.
    @required
    option: OptionSetting

    /// Reset the option instead of setting it.
    reset: Boolean = false
}

/// Registers a name map for files that are inserted later in the transaction.
///
/// `mapping.mapping_id` is an id of the client, which files of the transaction refer to with their `mapping_id`.
@http(method: "POST", uri: "/v1/create_name_map")
operation CreateNameMap {
    input: CreateNameMapRequest
}

/// The parameters of `create_name_map`.
structure CreateNameMapRequest {
    /// The table.
    @required
    table_id: Long

    /// The name map.
    @required
    mapping: NameMap
}

/// Inserts rows that are inlined in the catalog.
@http(method: "POST", uri: "/v1/insert_rows")
operation InsertRows {
    input: InsertRowsRequest
}

/// The parameters of `insert_rows`.
structure InsertRowsRequest {
    /// The table.
    @required
    table_id: Long

    /// The types of the columns of the rows, in the order of the columns of the table.
    @required
    column_types: LogicalTypeList

    /// The rows, as arrays of values.
    @required
    @rowTypes("column_types")
    rows: Rows

    /// The row ids of the rows (for updates that keep the row ids of the updated rows).
    row_ids: LongList = []

    /// The statistics of the rows per column.
    @cppType("map<FieldIndex, DuckLakeColumnStats>")
    @cppMap(keyField: "column_id", value: ColumnStats)
    column_stats: ColumnStatsEntryList = []
}

/// Registers Parquet files that were written by the client.
@http(method: "POST", uri: "/v1/insert_files")
operation InsertFiles {
    input: InsertFilesRequest
}

/// The parameters of `insert_files`.
structure InsertFilesRequest {
    /// The table.
    @required
    table_id: Long

    /// The files.
    @required
    files: DataFileList
}

/// Deletes rows from a table.
@http(method: "POST", uri: "/v1/delete_data")
operation DeleteData {
    input: DeleteDataRequest
}

/// The parameters of `delete_data`.
structure DeleteDataRequest {
    /// The table.
    @required
    table_id: Long

    /// Delete files written by the client.
    delete_files: DeleteFileList = []

    /// Deletes of inlined rows.
    inlined_deletes: InlinedRowDeletesList = []

    /// Deletes of rows of data files that are inlined in the catalog.
    file_row_deletes: FileRowDeletesList = []

    /// Data files that are deleted entirely.
    dropped_files: DroppedFileList = []

    /// Whether a delete was evaluated against the table, even if it did not delete any rows. This makes the commit
    /// conflict with concurrent inserts.
    delete_attempted: Boolean = false
}

/// Marks inlined data as flushed.
///
/// The rows are deleted from the inlined data tables when the transaction commits. The files that hold the flushed rows
/// are inserted with `insert_files`.
@http(method: "POST", uri: "/v1/flush_inlined_data")
operation FlushInlinedData {
    input: FlushInlinedDataRequest
}

/// The parameters of `flush_inlined_data`.
structure FlushInlinedDataRequest {
    /// The inlined data tables whose rows up to the snapshot were flushed.
    inlined_tables: FlushedInlinedTableList = []

    /// The tables whose inlined deletions of data file rows up to the snapshot were flushed to delete files (registered
    /// with `delete_data`).
    file_deletions: FlushedFileDeletionsList = []
}

/// Replaces data files with compacted files that were written by the client.
@http(method: "POST", uri: "/v1/compact")
operation Compact {
    input: CompactRequest
}

/// The parameters of `compact`.
structure CompactRequest {
    /// The table.
    @required
    table_id: Long

    /// The compactions.
    @required
    compactions: CompactionList
}

/// How a commit is retried on conflicts.
structure RetryConfig {
    /// The number of retries.
    @required
    max_retry_count: UInt64

    /// The wait before the first retry.
    @required
    retry_wait_ms: UInt64

    /// The factor the wait grows with on every retry.
    @required
    retry_backoff: Double
}

/// A table that a commit created.
structure CreatedTable {
    /// The placeholder id of the table in the request.
    placeholder: Long

    /// The id the commit gave the table.
    @required
    table_id: UInt64
}

/// An entry that a commit created.
structure CreatedEntry {
    /// The `entry_reference` of the request that created the entry.
    @required
    entry_reference: UInt64

    /// The type of the entry, e.g. `TABLE_ENTRY`.
    @required
    type: String

    /// The id the commit gave the entry.
    @required
    id: UInt64
}

/// An option of the DuckLake.
structure OptionSetting {
    /// The option, e.g. `data_inlining_row_limit`.
    @required
    key: String

    /// The value of the option.
    value: String = ""

    /// The schema, for options of a schema.
    schema_id: UInt64

    /// The table, for options of a table. Can be a placeholder.
    table_id: Long
}

/// Deletes of inlined rows.
structure InlinedRowDeletes {
    /// The schema version of the inlined rows.
    @required
    schema_version: UInt64

    /// The row ids of the deleted rows.
    @required
    @cppType("set<idx_t>")
    row_ids: UInt64List
}

/// Deletes of rows of a data file that are inlined in the catalog.
structure FileRowDeletes {
    /// The data file.
    @required
    data_file_id: UInt64

    /// The positions of the deleted rows in the file.
    @required
    @cppType("set<idx_t>")
    row_ids: UInt64List
}

/// A data file that is deleted entirely.
structure DroppedFile {
    /// The data file.
    @required
    data_file_id: UInt64

    /// The path of the file.
    @required
    path: DataPath

    /// The number of rows of the file.
    @required
    record_count: UInt64

    /// The size of the file.
    @required
    file_size_bytes: UInt64
}

/// The inlined rows of a table at a schema version that were flushed.
structure FlushedInlinedTable {
    /// The table.
    @required
    table_id: Long

    /// The schema version of the inlined rows.
    @required
    schema_version: UInt64

    /// The rows up to this snapshot were flushed.
    @required
    flush_snapshot_id: UInt64

    /// Whether the flush wrote data files, the rows of a flush that wrote none are not deleted.
    wrote_files: Boolean = true
}

/// The inlined deletions of rows of data files of a table that were flushed.
structure FlushedFileDeletions {
    /// The table.
    @required
    table_id: Long

    /// The deletions up to this snapshot were flushed.
    @required
    flush_snapshot_id: UInt64
}
