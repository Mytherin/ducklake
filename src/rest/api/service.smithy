$version: "2"

metadata suppressions = [
    {
        id: "HttpMethodSemantics.UnnecessaryReadonlyTrait"
        namespace: "ducklake.rest"
        reason: "All endpoints take a JSON body, so they are POST requests - read endpoints also accept GET"
    }
]

namespace ducklake.rest

use ducklake.rest.codegen#cppIncludes
use ducklake.rest.codegen#docSections
use ducklake.rest.codegen#wireType

/// The DuckLake REST API.
@cppIncludes([
    "common/ducklake_snapshot.hpp"
    "common/ducklake_data_file.hpp"
    "storage/ducklake_metadata_info.hpp"
    "storage/ducklake_stats.hpp"
])
@docSections([
    {
        title: "Read Endpoints"
        kind: "read"
        documentation: """
            These endpoints read the catalog. Each request runs in a transaction of its own on the server.
            """
    }
    {
        title: "Maintenance Endpoints"
        kind: "maintenance"
        documentation: """
            These endpoints change the catalog directly, outside of a transaction.
            """
    }
    {
        title: "Committing Transactions"
        kind: "commit"
        documentation: """
            A client records the changes of a transaction and sends them to `commit_transaction` when it commits. The changes
            are a list of requests that are applied in order; later requests can refer to entries created by earlier requests
            through negative placeholder ids: `-1` is the first entry (schema, table, view, ...) created in the transaction,
            `-2` the second one, etc.

            Every request of a commit can also be sent on its own to the endpoint of its name (e.g. `POST /v1/create_table`),
            which commits a transaction with only that request, based on the latest snapshot.

            Clients send the deletes of committed data first, then the catalog changes in the order in which they were made,
            and then the inserted data in the format of the tables at the end of the transaction. This order matters: e.g. a
            `SET NOT NULL` that follows a delete of the `NULL` rows only succeeds after the delete.

            Requests that refer to existing entries identify them by `schema_path` and `name`. They are applied in order, so a
            name always refers to the entry as it is at that point of the transaction. An empty `schema_path` refers to the
            default schema.
            """
    }
    {
        title: "Types"
        kind: "types"
        documentation: """
            The structures that the endpoints share. Fields that are optional can be `null` or absent; the server and the
            client leave them out when they have no value.
            """
    }
])
service DuckLake {
    version: "1"
    operations: [
        Config
        SnapshotData
        Select
        ReadInlinedData
        DeletedInlinedRows
        InlinedFileDeletions
        FilesForCompaction
        TableSchemaVersions
        RowCount
        InlinedTables
        NextColumnId
        ColumnCreatedWithTable
        ListSnapshots
        TableSizes
        ColumnMappings
        TableChanges
        DuckLakeTables
        DuckLakeTable
        ExpireSnapshots
        FilesForCleanup
        RemoveCleanedUpFiles
        KnownFiles
        CommitTransaction
        CreateSchema
        CreateTable
        CreateView
        CreateMacro
        AlterTable
        AlterView
        DropSchema
        DropTable
        DropView
        DropMacro
        SetOption
        CreateNameMap
        InsertRows
        InsertFiles
        DeleteData
        FlushInlinedData
        Compact
    ]
}

/// A request of a commit, by the endpoint it can also be sent to on its own.
union CommitRequest {
    create_schema: CreateSchemaRequest
    create_table: CreateTableRequest
    create_view: CreateViewRequest
    create_macro: CreateMacroRequest
    alter_table: AlterTableRequest
    alter_view: AlterViewRequest
    drop_schema: DropSchemaRequest
    drop_table: DropTableRequest
    drop_view: DropViewRequest
    drop_macro: DropMacroRequest
    set_option: SetOptionRequest
    create_name_map: CreateNameMapRequest
    insert_rows: InsertRowsRequest
    insert_files: InsertFilesRequest
    delete_data: DeleteDataRequest
    flush_inlined_data: FlushInlinedDataRequest
    compact: CompactRequest
}

/// An unsigned integer.
@wireType("uint64")
long UInt64

/// A timestamp with time zone, e.g. `"2025-01-01 00:00:00+00"`.
@wireType("timestamp")
string Timestamp

/// A value without a known type: `null`, a string for `VARCHAR` values, or `{"type": <type>, "value": <value>}`.
@wireType("value")
document Value

/// A DuckDB type, e.g. `{"id": "DECIMAL", "width": 18, "scale": 3}`.
@wireType("logical_type")
document LogicalType

/// Rows, as arrays of values of the types of their columns.
@wireType("rows")
document Rows
