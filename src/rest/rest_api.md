# DuckLake REST API

The DuckLake REST API exposes a DuckLake catalog over HTTP. Clients read the catalog and commit transactions through
a small set of structured endpoints, without sending SQL to the server.

This document describes the API as implemented by the reference server in this repository
(`ducklake_rest_serve`) and the client (`RESTMetadataManager`, used by `ATTACH 'ducklake:rest:<url>'`).

## Usage

### Starting a server

```sql
-- serve an attached DuckLake catalog
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data/');
SELECT listen_uri FROM ducklake_rest_serve('lake', host := 'localhost', port := 8181);

-- or attach the DuckLake in a database of its own, isolated from the caller
SELECT listen_uri FROM ducklake_rest_serve('ducklake:metadata.ducklake', port := 8181,
                                           attach_options := 'DATA_PATH ''data/''');
```

| Parameter        | Description                                                                              |
|------------------|------------------------------------------------------------------------------------------|
| `catalog`        | The name of an attached DuckLake catalog, or a `ducklake:` path to attach in isolation.  |
| `host`           | The host to listen on (default `localhost`).                                             |
| `port`           | The port to listen on, `0` (the default) picks a free port.                             |
| `attach_options` | Options for the isolated `ATTACH`, e.g. `DATA_PATH '...'`.                               |
| `wait`           | Block until the server is stopped, e.g. to run a server from the CLI.                    |
| `token`          | Require requests to carry this token (`Authorization: Bearer <token>`).                  |

The function returns the `listen_uri` of the server. Starting a server on a port that is in use by another server of
the same process replaces that server. `ducklake_rest_stop(listen_uri)` stops a server.

`ducklake_rest_request(listen_uri, endpoint, body)` sends a request to a server and returns the status code and the
JSON response, which is useful to explore the API:

```sql
SELECT response FROM ducklake_rest_request('http://localhost:8181', 'snapshot_data', '{}');
```

### Connecting to a server

```sql
ATTACH 'ducklake:rest:http://localhost:8181' AS lake;
```

`ducklake:rest:` only connects to a server that is already running (see [Starting a server](#starting-a-server)): it never
starts one.

A server that requires a token is attached with `ATTACH 'ducklake:rest:http://localhost:8181' AS lake (META_TOKEN '...')`.

The client reads the data path and the options of the DuckLake from the server. A `DATA_PATH` in the `ATTACH` of the
client is the view of the client of the data path, e.g. when it has mounted the data path somewhere else: the client
maps the paths between its data path and the data path of the server (`DataPath` fields below), so the server only ever
sees its own data path. Paths outside of the data path are not mapped. Data files are read and written by the
client directly, so the data path has to be accessible from both the client and the server.

## Conventions

* All endpoints live under `/v1/<endpoint>`. Requests and responses are JSON objects. Every endpoint accepts `POST`
  with a JSON body; read-only endpoints also accept `GET` with a JSON body.
* Each request runs in a transaction of its own on the server.
* Errors are reported with a non-200 status code and a body of the form
  `{"error": {"type": "<DuckDB exception type>", "message": "..."}}`. The status code is `409` for transaction
  conflicts, `404` for catalog errors (e.g. entries that do not exist), `400` for invalid input, `401` for a missing or
  invalid token and `500` otherwise.
* Identifiers (`snapshot_id`, `table_id`, `column_id`, ...) are the ids of the DuckLake specification.
* Optional values are `null` or absent.
* Read requests identify the snapshot to read with a `snapshot` object (as returned by `snapshot_data`), or with a
  `snapshot_id`. A `snapshot` is used as-is, which also allows reading at snapshots that have been expired since the
  client loaded them.
* Paths of files are absolute.

### Types

Types are structured JSON objects. `id` is the name of the DuckDB `LogicalTypeId`; nested and parameterized types
have additional fields:

```json
{"id": "INTEGER"}
{"id": "DECIMAL", "width": 18, "scale": 3}
{"id": "LIST", "child": {"id": "VARCHAR"}}
{"id": "ARRAY", "child": {"id": "DOUBLE"}, "size": 3}
{"id": "MAP", "key": {"id": "VARCHAR"}, "value": {"id": "BIGINT"}}
{"id": "STRUCT", "children": [{"name": "a", "type": {"id": "INTEGER"}}]}
{"id": "GEOMETRY", "crs": "EPSG:4326"}
{"id": "VARCHAR", "alias": "JSON"}
```

Column types in table definitions use the type names of the DuckLake specification (e.g. `int32`, `varchar`), as they
are stored in `ducklake_column`. Column types in `create_table` requests use DuckDB SQL type names
(e.g. `INTEGER`, `STRUCT(a INTEGER)`).

### Values

Values whose type is known from the context (e.g. the rows of a table) are encoded as follows:

| Type                             | Encoding                                                        |
|----------------------------------|-----------------------------------------------------------------|
| `NULL`                           | `null`                                                          |
| `BOOLEAN`                        | JSON boolean                                                    |
| integers up to 64 bits           | JSON number                                                     |
| `FLOAT`, `DOUBLE`                | JSON number, or a string (`"nan"`, `"inf"`, `"-inf"`)          |
| `STRUCT`                         | JSON object with a field per child                              |
| `LIST`, `ARRAY`                  | JSON array                                                      |
| `MAP`                            | JSON array of `{"key": ..., "value": ...}` objects              |
| `INTERVAL`                       | `{"months": 1, "days": 2, "micros": 3}`                         |
| `VARIANT`                        | the value the variant holds, as a value without a known type (see below) |
| everything else                  | the string representation of the value, e.g. `"2025-01-01"`    |

Values without a known type (e.g. tags) are `null`, a JSON string for `VARCHAR` values, or an object
`{"type": <type>, "value": <value>}`.

## Specification

The API is specified as a [Smithy](https://smithy.io) model in `src/rest/api/*.smithy`: the `DuckLake` service, an
operation per endpoint (`POST /v1/<endpoint>`) and the shapes they exchange. The reference below, the request and
response structures of the server and the client, and their JSON conversions are generated from it with
`python3 scripts/generate_rest_api.py`, which reads the model with the
[Smithy CLI](https://smithy.io/2.0/guides/smithy-cli/cli_installation.html) (`smithy` on the `PATH`, or `SMITHY`).
`--check` verifies that the generated files are up to date. To change the API, change the model and run the script.

Operations that are `@readonly` read the catalog, the members of the `CommitRequest` union are the requests of a commit
(`commit_transaction`), and the other operations are maintenance endpoints. The traits of `src/rest/api/codegen.smithy`
bind shapes to the C++ classes of DuckLake; they do not change the JSON of the API. Members are encoded as follows:

| Type           | Smithy shape     | JSON                                                                         |
|----------------|------------------|------------------------------------------------------------------------------|
| `string`       | `String`         | a string                                                                     |
| `bool`         | `Boolean`        | a boolean                                                                    |
| `uint64`       | `UInt64`         | a non-negative integer                                                       |
| `int64`        | `Long`           | an integer                                                                   |
| `double`       | `Double`         | a number                                                                     |
| `path`         | `DataPath`       | a path of a file or directory, which clients map between their data path and the data path of the server |
| `timestamp`    | `Timestamp`      | a timestamp with time zone as a string, e.g. `"2025-01-01 00:00:00+00"`      |
| `value`        | `Value`          | a [value without a known type](#values)                                      |
| `logical_type` | `LogicalType`    | a [type](#types)                                                             |
| `rows`         | `Rows`           | an array of rows, each an array of [values](#values) of the types of the columns |
| `json`         | `Document`       | any JSON value, specified by the description                                 |
| `list of T`    | `list`           | an array                                                                     |
| `object of T`  | `map`            | an object with arbitrary keys                                                |

Members that are not `@required` and have no default are optional: they can be `null` or absent. Unions, like
`CommitRequest`, are objects with exactly one member, e.g. `{"create_table": {...}}`.

<!-- BEGIN GENERATED REFERENCE: edit src/rest/api/*.smithy and run scripts/generate_rest_api.py -->

## Read Endpoints

These endpoints read the catalog. Each request runs in a transaction of its own on the server.

### `column_created_with_table`

Returns whether a column was created together with its table (rather than added later).

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_name` | `string` | The table. |
| `column_name` | `string` | The column. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `created_with_table` | `bool` | Whether the column was created with the table. |

### `column_mappings`

Returns the name maps of files that were added with `ducklake_add_data_files`, starting from a mapping id.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `start_from` | `uint64` (optional) | The first mapping id to return. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `mappings` | list of [`NameMap`](#namemap) (default `[]`) | The name maps. |

### `config`

Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).

The `data_path` is the resolved data path of the served DuckLake.

Request: an empty object.

Response: [`Metadata`](#metadata).

### `deleted_inlined_rows`

Returns the deleted versions of the inlined rows of a table that a flush writes.

The client writes delete files for the flushed rows with them.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. |
| `schema_version` | `uint64` | The schema version of the inlined rows. |
| `deleted_row_ids` | list of `uint64` (default `[]`) | Rows that the transaction of the client deleted, which are not flushed. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `rows` | list of [`DeletedRowVersion`](#deletedrowversion) (default `[]`) | The deleted rows. |

### `ducklake_table`

Returns the contents of a table of the DuckLake specification, as it is stored in the catalog.

This makes it possible to inspect a DuckLake or to export all of its metadata.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_name` | `string` | The table, e.g. `ducklake_snapshot`. |

Response: [`InlinedRows`](#inlinedrows).

### `ducklake_tables`

Returns the names of the tables of the DuckLake specification (`ducklake_snapshot`, `ducklake_data_file`, ...).

In SQL, `ducklake_metadata_table('lake', 'ducklake_snapshot')` reads a table of the metadata catalog of any
DuckLake.

Request: an empty object.

Response:

| Field | Type | Description |
|-------|------|-------------|
| `table_names` | list of `string` (default `[]`) | The tables. |

### `files_for_compaction`

Returns the data files of a table that are candidates for compaction, together with their delete files.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. |
| `type` | [`CompactionType`](#compactiontype) | The type of the compaction. |
| `delete_threshold` | `double` (optional) | For `rewrite_delete`: the fraction of deleted rows above which files are rewritten. |
| `min_file_size` | `uint64` (optional) | Only consider files of at least this size. |
| `max_file_size` | `uint64` (optional) | Only consider files of at most this size. |
| `target_file_size` | `uint64` | The size of the compacted files. |
| `newer_than` | `value` (optional) | Only consider files written at or after this timestamp. |
| `older_than` | `value` (optional) | Only consider files written before this timestamp. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `files` | list of [`CompactionFileEntry`](#compactionfileentry) (default `[]`) | The candidates. |

### `inlined_file_deletions`

Returns the deletions of rows of data files that are inlined in the catalog.

Each deletion comes with the delete file of its data file, so that a client can write them to delete files.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `deletions` | list of [`InlinedFileDeletion`](#inlinedfiledeletion) (default `[]`) | The deletions. |

### `inlined_tables`

Returns the schema versions that a table has inlined rows for.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `schema_versions` | list of `uint64` (default `[]`) | The schema versions of the table whose columns inlined rows have. Rows are inlined per schema version and read with `read_inlined_data`. |

### `list_snapshots`

Returns the snapshots, optionally filtered.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `exclude_latest` | `bool` (default `false`) | Exclude the latest snapshot. |
| `older_than` | `timestamp` (optional) | Only snapshots created before this timestamp. |
| `snapshot_ids` | list of `uint64` (optional) | Only these snapshots. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `snapshots` | list of [`SnapshotInfo`](#snapshotinfo) (default `[]`) | The snapshots. |

### `next_column_id`

Returns the next free column (field) id of a table.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` | The table. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `next_column_id` | `uint64` | The next free column id. |

### `read_inlined_data`

Reads the rows that are inlined in the catalog for a table at a schema version.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. |
| `schema_version` | `uint64` | The schema version of the table whose columns the rows have (see `inlined_tables`). |
| `columns` | list of `string` | The columns to read, e.g. `["i"]`. Columns can be cast to the type to read them as, e.g. `"i::BIGINT"`. |
| `mode` | [`InlinedReadMode`](#inlinedreadmode) (default `"scan"`) | Which rows to read. |
| `start_snapshot` | [`Snapshot`](#snapshot) (optional) | For `insertions` and `deletions`: the first snapshot of the range. |
| `start_snapshot_id` | `uint64` (optional) | For `insertions` and `deletions`: the id of the first snapshot of the range. |
| `order_by` | `string` (default `""`) | For `flush`: the sort order of the table, as an `ORDER BY` list (e.g. `i DESC NULLS LAST`). The rows are ordered by it, then by their row id. |
| `deleted_row_ids` | list of `uint64` (default `[]`) | For `flush`: rows that the transaction of the client deleted, which are not flushed. |

Response: [`InlinedRows`](#inlinedrows).

### `row_count`

Counts the rows of a table that are visible in a snapshot.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `count` | [`RowCountType`](#rowcounttype) | What to count. |
| `table_id` | `uint64` | The table. |
| `schema_version` | `uint64` (optional) | For inlined counts: the schema version of the inlined rows. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `row_count` | `uint64` (optional) | The number of rows. |

### `select`

Plans a scan of a table.

Returns the data files of the table that are visible in a snapshot, together with their delete files and
inlined deletions, and optionally the rows of the table that are inlined in the catalog. With
`include_inlined_data`, `inlined_data` holds the visible rows of the table for each schema version that it has
inlined rows for, with the row id, the begin and end snapshot and all columns of the table at that schema version.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table to scan. |
| `extended` | `bool` (default `false`) | Return the extended file list used to plan deletes and updates in `extended_files`, instead of `files`. |
| `filters` | [`ScanFilters`](#scanfilters) (optional) | Filters to prune files with. |
| `include_inlined_data` | `bool` (default `false`) | Also return the inlined rows of the table that are visible in the snapshot. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `files` | list of [`FileListEntry`](#filelistentry) (default `[]`) | The files of the scan. |
| `extended_files` | list of [`FileListExtendedEntry`](#filelistextendedentry) (optional) | The files of the scan, for `extended` scans. |
| `inlined_data` | list of [`InlinedTableRows`](#inlinedtablerows) (optional) | The inlined rows of the table, for `include_inlined_data`. |

### `snapshot_data`

Returns a snapshot together with (optionally) the catalog and the table statistics of that snapshot.

Clients pass `cached_metadata_version` and `cached_data_version` when they start a transaction, so they get the
latest snapshot and, if it changed since, its catalog and statistics in a single round-trip.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `at` | [`AtClause`](#atclause) (optional) | Find the snapshot of an `AT` clause instead. |
| `bound` | [`SnapshotBound`](#snapshotbound) (default `"upper"`) | Whether a timestamp of `at` finds the last snapshot before or the first snapshot after it. |
| `include_catalog` | `bool` (default `false`) | Return the catalog of the snapshot. |
| `include_stats` | `bool` (default `false`) | Return the table statistics of the snapshot. |
| `cached_metadata_version` | `uint64` (optional) | The schema version of the latest catalog the client has loaded. If the snapshot has a different schema version, its catalog is returned. |
| `cached_data_version` | `uint64` (optional) | The next file id of the snapshot of the latest statistics the client has loaded. If the snapshot has a different next file id, its statistics are returned. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) | The snapshot. |
| `catalog` | [`CatalogInfo`](#cataloginfo) (optional) | The catalog of the snapshot. |
| `stats` | list of [`GlobalTableStats`](#globaltablestats) (optional) | The statistics of the tables of the snapshot. |

### `table_changes`

Returns the files that hold the changes of a table between two snapshots, for change data feeds.

Inlined changes are read with `read_inlined_data` in `insertions` / `deletions` mode.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. The snapshot is the last snapshot of the range. |
| `start_snapshot` | [`Snapshot`](#snapshot) (optional) | The first snapshot of the range. |
| `start_snapshot_id` | `uint64` (optional) | The id of the first snapshot of the range. |
| `changes` | [`TableChangeType`](#tablechangetype) | The changes to return. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `insertions` | list of [`FileListEntry`](#filelistentry) (default `[]`) | For `insertions`: the files with inserted rows. |
| `deletions` | list of [`DeleteScanEntry`](#deletescanentry) (default `[]`) | For `deletions`: the files with deleted rows. |

### `table_schema_versions`

Returns when a table and its schema versions were created.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `table_id` | `uint64` | The table. With a snapshot, the first and last schema version of the table at it are also returned. |
| `schema_version` | `uint64` (optional) | Also return the snapshot that created this schema version. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `begin_snapshot` | `uint64` | The snapshot that created the table. |
| `creation` | `uint64` (optional) | The first schema version of the table at the snapshot. |
| `last_change` | `uint64` (optional) | The last schema version of the table at the snapshot. |
| `schema_version_begin_snapshot` | `uint64` (optional) | The snapshot that created `schema_version`. |

### `table_sizes`

Returns the storage used by each table at a snapshot.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `tables` | list of [`TableSize`](#tablesize) (default `[]`) | The tables. |

## Maintenance Endpoints

These endpoints change the catalog directly, outside of a transaction.

### `expire_snapshots`

Expires snapshots.

Removes the snapshots, together with the catalog entries and files that are only visible in them. The data files are
scheduled for deletion.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_ids` | list of `uint64` | The snapshots to expire. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `expired_snapshots` | `uint64` | The number of expired snapshots. |

### `files_for_cleanup`

Returns the files scheduled for deletion.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `older_than` | `timestamp` (optional) | Only files scheduled for deletion before this timestamp. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `files` | list of [`FileForCleanup`](#fileforcleanup) (default `[]`) | The files. |

### `known_files`

Returns the paths of all files that the catalog refers to.

These are the data files, delete files and files scheduled for deletion, so a client can find orphaned files in the
data path.

Request: an empty object.

Response:

| Field | Type | Description |
|-------|------|-------------|
| `paths` | list of `path` (default `[]`) | The paths. |

### `remove_cleaned_up_files`

Removes files from the files scheduled for deletion, after the client deleted them.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `data_file_ids` | list of `uint64` | The files. |

Response: an empty object.

## Committing Transactions

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

### `alter_table`

Alters a table.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` (default `[]`) | The schema of the table. |
| `name` | `string` | The name of the table. |
| `alter` | [`Alteration`](#alteration) | The change. |
| `partition_id` | `uint64` (optional) | For `set_partitioned_by`: an id of the client for the new partitioning (as for `create_table`). |

Response: as for [`commit_transaction`](#commit_transaction).

### `alter_view`

Alters a view.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` (default `[]`) | The schema of the view. |
| `name` | `string` | The name of the view. |
| `alter` | [`Alteration`](#alteration) | The change. |

Response: as for [`commit_transaction`](#commit_transaction).

### `commit_transaction`

Commits a set of changes atomically.

If the transaction conflicts with a transaction that committed after its snapshot, the server retries the
commit when that is safe; otherwise it fails with a `409`. The snapshot is the snapshot the changes are based
on: the server detects conflicts with every snapshot committed after it. Without a snapshot, the changes are
based on the latest snapshot.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |
| `commit_info` | [`CommitInfo`](#commitinfo) (optional) | The commit information of the snapshot. |
| `retry` | [`RetryConfig`](#retryconfig) (optional) | How the server retries the commit on conflicts. |
| `requests` | list of `commit_request` (default `[]`) | The changes, applied in order, e.g. `[{"create_table": {...}}, {"insert_rows": {...}}]`. |

Response:

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | `uint64` (optional) | The committed snapshot, absent if the requests did not create a snapshot (e.g. if they only set options). |
| `schema_version` | `uint64` (optional) | The schema version of the committed snapshot. |
| `table_ids` | list of [`CreatedTable`](#createdtable) (default `[]`) | The ids of the tables the transaction created. |
| `entry_ids` | list of [`CreatedEntry`](#createdentry) (default `[]`) | The ids of the created entries whose requests had an `entry_reference`. |

### `compact`

Replaces data files with compacted files that were written by the client.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `compactions` | list of [`Compaction`](#compaction) | The compactions. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_macro`

Creates a macro.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `entry_reference` | `uint64` (optional) | See `create_schema`. |
| `schema_path` | list of `string` (default `[]`) | The schema of the macro. |
| `macro` | [`MacroInfo`](#macroinfo) | The macro (its ids are ignored). |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_name_map`

Registers a name map for files that are inserted later in the transaction.

`mapping.mapping_id` is an id of the client, which files of the transaction refer to with their `mapping_id`.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `mapping` | [`NameMap`](#namemap) | The name map. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_schema`

Creates a schema.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `entry_reference` | `uint64` (optional) | A number of the client's choosing, which `entry_ids` returns with the id the commit gave the entry. A client can use it to keep track of the entries it created, e.g. to keep their oids stable. |
| `schema_path` | list of `string` | The path of the schema, e.g. `["parent", "child"]`. |
| `schema_uuid` | `string` (optional) | The uuid of the schema. |
| `path` | `path` (optional) | The data path of the schema. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the schema exists. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_table`

Creates a table.

Columns get the field ids `1..n` in the order of the columns (nested fields are numbered depth-first), as in `CREATE
TABLE`.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` (optional) | The placeholder id of the table. |
| `entry_reference` | `uint64` (optional) | See `create_schema`. |
| `schema_path` | list of `string` (default `[]`) | The schema of the table, e.g. `["main"]`. |
| `table_name` | `string` | The name of the table. |
| `table_uuid` | `string` (optional) | The uuid of the table. |
| `path` | `path` (optional) | The data path of the table. |
| `columns` | list of [`ColumnDefinition`](#columndefinition) | The columns of the table. |
| `partitioned_by` | list of `string` (default `[]`) | The partition keys as SQL expressions, e.g. `["year(ts)"]`. |
| `sorted_by` | list of `string` (default `[]`) | The sort keys as SQL expressions. |
| `options` | object of `string` (default `{}`) | Table options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`. |
| `comment` | `string` (optional) | The comment of the table. |
| `partition_id` | `uint64` (optional) | An id of the client for the partitioning of the table. Files that are inserted later in the transaction refer to the partitioning with it. |

Response: as for [`commit_transaction`](#commit_transaction).

### `create_view`

Creates a view.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `entry_reference` | `uint64` (optional) | See `create_schema`. |
| `schema_path` | list of `string` (default `[]`) | The schema of the view. |
| `view_name` | `string` | The name of the view. |
| `view_uuid` | `string` (optional) | The uuid of the view. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the view, only `duckdb` for now. |
| `sql` | `string` | The query of the view. |
| `column_aliases` | list of `string` (default `[]`) | The names of the columns of the view. |
| `comment` | `string` (optional) | The comment of the view. |

Response: as for [`commit_transaction`](#commit_transaction).

### `delete_data`

Deletes rows from a table.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `delete_files` | list of [`DeleteFile`](#deletefile) (default `[]`) | Delete files written by the client. |
| `inlined_deletes` | list of [`InlinedRowDeletes`](#inlinedrowdeletes) (default `[]`) | Deletes of inlined rows. |
| `file_row_deletes` | list of [`FileRowDeletes`](#filerowdeletes) (default `[]`) | Deletes of rows of data files that are inlined in the catalog. |
| `dropped_files` | list of [`DroppedFile`](#droppedfile) (default `[]`) | Data files that are deleted entirely. |
| `delete_attempted` | `bool` (default `false`) | Whether a delete was evaluated against the table, even if it did not delete any rows. This makes the commit conflict with concurrent inserts. |

Response: as for [`commit_transaction`](#commit_transaction).

### `drop_macro`

Drops a macro.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` (default `[]`) | The schema of the macro. |
| `name` | `string` | The name of the macro. |
| `macro_type` | [`MacroType`](#macrotype) (default `"scalar"`) | The type of the macro. |

Response: as for [`commit_transaction`](#commit_transaction).

### `drop_schema`

Drops a schema, which has to be empty.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` | The path of the schema. |

Response: as for [`commit_transaction`](#commit_transaction).

### `drop_table`

Drops a table.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` (default `[]`) | The schema of the table. |
| `name` | `string` | The name of the table. |

Response: as for [`commit_transaction`](#commit_transaction).

### `drop_view`

Drops a view.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `schema_path` | list of `string` (default `[]`) | The schema of the view. |
| `name` | `string` | The name of the view. |

Response: as for [`commit_transaction`](#commit_transaction).

### `flush_inlined_data`

Marks inlined data as flushed.

The rows are deleted from the inlined data tables when the transaction commits. The files that hold the flushed rows
are inserted with `insert_files`.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `inlined_tables` | list of [`FlushedInlinedTable`](#flushedinlinedtable) (default `[]`) | The inlined data tables whose rows up to the snapshot were flushed. |
| `file_deletions` | list of [`FlushedFileDeletions`](#flushedfiledeletions) (default `[]`) | The tables whose inlined deletions of data file rows up to the snapshot were flushed to delete files (registered with `delete_data`). |

Response: as for [`commit_transaction`](#commit_transaction).

### `insert_files`

Registers Parquet files that were written by the client.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `files` | list of [`DataFile`](#datafile) | The files. |

Response: as for [`commit_transaction`](#commit_transaction).

### `insert_rows`

Inserts rows that are inlined in the catalog.

Request:

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `column_types` | list of `logical_type` | The types of the columns of the rows, in the order of the columns of the table. |
| `rows` | `rows` | The rows, as arrays of values. |
| `row_ids` | list of `int64` (default `[]`) | The row ids of the rows (for updates that keep the row ids of the updated rows). |
| `column_stats` | list of [`ColumnStats`](#columnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the rows per column. |

Response: as for [`commit_transaction`](#commit_transaction).

### `set_option`

Sets or resets a DuckLake option (`ducklake_metadata`).

Request:

| Field | Type | Description |
|-------|------|-------------|
| `option` | [`OptionSetting`](#optionsetting) | The option. |
| `reset` | `bool` (default `false`) | Reset the option instead of setting it. |

Response: as for [`commit_transaction`](#commit_transaction).

## Types

The structures that the endpoints share. Fields that are optional can be `null` or absent; the server and the
client leave them out when they have no value.

### `AddColumn`

| Field | Type | Description |
|-------|------|-------------|
| `column` | [`ColumnDefinition`](#columndefinition) | The column. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the column exists. |

### `AddField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the parent column. |
| `field` | [`ColumnDefinition`](#columndefinition) | The field. |
| `if_not_exists` | `bool` (default `false`) | Do nothing if the field exists. |

### `Alteration`

The change of an `alter_table` or `alter_view` request: an object with exactly one member, the name of the change.
Columns are referred to by name; nested fields by their path from the top-level column, e.g. `["s", "a"]`.
Expressions are SQL expressions.

```json
{"rename_column": {"old_name": "i", "new_name": "j"}}
```

An object with exactly one member, the name of the variant:

| Variant | Type |
|---------|------|
| `rename_table` | [`RenameTable`](#renametable) |
| `rename_view` | [`RenameView`](#renameview) |
| `rename_column` | [`RenameColumn`](#renamecolumn) |
| `rename_field` | [`RenameField`](#renamefield) |
| `add_column` | [`AddColumn`](#addcolumn) |
| `add_field` | [`AddField`](#addfield) |
| `remove_column` | [`RemoveColumn`](#removecolumn) |
| `remove_field` | [`RemoveField`](#removefield) |
| `change_column_type` | [`ChangeColumnType`](#changecolumntype) |
| `set_default` | [`SetDefault`](#setdefault) |
| `set_not_null` | [`SetNotNull`](#setnotnull) |
| `drop_not_null` | [`DropNotNull`](#dropnotnull) |
| `set_partitioned_by` | [`SetPartitionedBy`](#setpartitionedby) |
| `set_sorted_by` | [`SetSortedBy`](#setsortedby) |
| `set_options` | [`SetOptions`](#setoptions) |
| `reset_options` | [`ResetOptions`](#resetoptions) |
| `set_comment` | [`SetComment`](#setcomment) |
| `set_column_comment` | [`SetColumnComment`](#setcolumncomment) |

### `AtClause`

The `AT` clause of a time travel query.

| Field | Type | Description |
|-------|------|-------------|
| `unit` | [`AtUnit`](#atunit) | The unit of the value. |
| `value` | `value` | The snapshot id or timestamp. |

### `AtUnit`

The unit of an `AT` clause.

| Value | Description |
|-------|-------------|
| `version` | A snapshot id. |
| `timestamp` | A timestamp. |

### `CatalogEntryType`

The type of a catalog entry.

| Value | Description |
|-------|-------------|
| `TABLE_ENTRY` |  |
| `VIEW_ENTRY` |  |

### `CatalogInfo`

The catalog at a snapshot.

| Field | Type | Description |
|-------|------|-------------|
| `schemas` | list of [`SchemaInfo`](#schemainfo) (default `[]`) | The schemas. |
| `tables` | list of [`TableInfo`](#tableinfo) (default `[]`) | The tables. |
| `views` | list of [`ViewInfo`](#viewinfo) (default `[]`) | The views. |
| `macros` | list of [`MacroInfo`](#macroinfo) (default `[]`) | The macros. |
| `partitions` | list of [`PartitionInfo`](#partitioninfo) (default `[]`) | The partitionings of the tables. |
| `sorts` | list of [`SortInfo`](#sortinfo) (default `[]`) | The sort orders of the tables. |

### `ChangeColumnType`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |
| `column_type` | `string` | The new type, as a DuckDB SQL type name (e.g. `BIGINT`). |
| `expression` | `string` (optional) | The `USING` expression that converts the old values. |

### `ColumnDefinition`

The definition of a column in a catalog change.

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The name of the column. |
| `column_type` | `string` | The type of the column, as a DuckDB SQL type name (e.g. `INTEGER`, `STRUCT(a INTEGER)`). |
| `default` | `string` (optional) | The default of the column, as a SQL expression. |
| `comment` | `string` (optional) | The comment of the column. |
| `not_null` | `bool` (default `false`) | Whether the column is `NOT NULL`. |

### `ColumnInfo`

A column of a table, as stored in `ducklake_column`.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` (optional) | The field id of the column. |
| `column_name` | `string` | The name of the column. |
| `column_type` | `string` | The type of the column, as a type name of the DuckLake specification (e.g. `int32`, `varchar`). |
| `initial_default` | `value` (optional) | The value of the column for rows that were written before the column was added. |
| `default_value` | `value` (optional) | The default value of the column. |
| `default_value_type` | `string` (default `""`) | Whether the default is a `literal` or an `expression`. |
| `nulls_allowed` | `bool` (default `true`) | Whether the column can hold `NULL`. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the column. |
| `children` | list of [`ColumnInfo`](#columninfo) (default `[]`) | The fields of nested columns. |

### `ColumnStats`

The statistics of a column of a file or of inserted rows.

```json
{"type": {"id": "INTEGER"}, "min_value": "1", "max_value": "2", "null_count": 0, "value_count": 2,
 "column_size_bytes": 10, "any_valid": true, "bounds_unknown": false, "min_is_exact": true, "max_is_exact": true}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | `logical_type` | The type of the column. |
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `null_count` | `uint64` (optional) | The number of `NULL` values. |
| `value_count` | `uint64` (optional) | The number of values. |
| `column_size_bytes` | `uint64` (default `0`) | The size of the column. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |
| `any_valid` | `bool` (default `true`) | Whether the column contains any value that is not `NULL`. |
| `bounds_unknown` | `bool` (default `false`) | Whether the minimum and maximum are unknown. |
| `min_is_exact` | `bool` (default `false`) | Whether the minimum is exact. |
| `max_is_exact` | `bool` (default `false`) | Whether the maximum is exact. |
| `extra_stats` | `string` (optional) | Type-specific statistics. |

### `CommitInfo`

The commit information of a transaction.

| Field | Type | Description |
|-------|------|-------------|
| `author` | `value` (optional) | The author of the commit. |
| `commit_message` | `value` (optional) | The commit message. |
| `commit_extra_info` | `value` (optional) | Extra information of the commit. |
| `is_commit_info_set` | `bool` (default `false`) | Whether the commit information was set. |

### `Compaction`

Data files that are replaced by compacted files.

| Field | Type | Description |
|-------|------|-------------|
| `type` | [`CompactionType`](#compactiontype) | The type of the compaction. |
| `row_id_start` | `uint64` (optional) | The row id of the first row of the compacted files. |
| `source_files` | list of [`CompactionFileEntry`](#compactionfileentry) (default `[]`) | The files that are replaced, as returned by `files_for_compaction`. |
| `written_files` | list of [`DataFile`](#datafile) (default `[]`) | The compacted files. |

### `CompactionBaseFile`

A file of a compaction.

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | `uint64` (optional) | The id of the file. |
| `file` | [`FileData`](#filedata) | The file. |
| `record_count` | `uint64` | The number of rows. |
| `begin_snapshot` | `uint64` | The snapshot that added the file. |
| `end_snapshot` | `uint64` (optional) | The snapshot that removed the file. |
| `max_row_count` | `uint64` (optional) | The number of rows of the file to read. |

### `CompactionDataFile`

A data file that is a candidate for compaction.

Has the fields of [`CompactionBaseFile`](#compactionbasefile), and:

| Field | Type | Description |
|-------|------|-------------|
| `row_id_start` | `uint64` (optional) | The row id of the first row. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `partition_id` | `uint64` (optional) | The partitioning of the file. |
| `partition_values` | list of `value` (default `[]`) | The values of the partition keys. |

### `CompactionDeleteFile`

A delete file of a data file that is a candidate for compaction.

Has the fields of [`CompactionBaseFile`](#compactionbasefile), and:

| Field | Type | Description |
|-------|------|-------------|
| `delete_file_id` | `uint64` (optional) | The id of the delete file. |
| `max_snapshot` | `uint64` (optional) | The last snapshot whose deletes the file holds. |

### `CompactionFileEntry`

A data file that is a candidate for compaction, together with its delete files.

| Field | Type | Description |
|-------|------|-------------|
| `data_file` | [`CompactionDataFile`](#compactiondatafile) | The data file. |
| `delete_files` | list of [`CompactionDeleteFile`](#compactiondeletefile) (default `[]`) | The delete files of the data file. |
| `max_partial_file_snapshot` | `uint64` (optional) | The last snapshot whose rows the file holds, for partial files. |
| `schema_version` | `uint64` | The schema version the file was written with. |
| `partition_schema_version` | `uint64` (optional) | The schema version of the partitioning of the file. |
| `inlined_file_deletions` | list of `uint64` (default `[]`) | The positions of the deleted rows of the file that are inlined in the catalog. |
| `has_inlined_deletions` | `bool` (default `false`) | Whether rows of the file are deleted by inlined deletions. |
| `delete_ratio` | `double` (default `0`) | The fraction of deleted rows. |

### `CompactionType`

The type of a compaction.

| Value | Description |
|-------|-------------|
| `merge_adjacent` | Merges small adjacent files. |
| `rewrite_delete` | Rewrites files with many deleted rows. |

### `CreatedEntry`

An entry that a commit created.

| Field | Type | Description |
|-------|------|-------------|
| `entry_reference` | `uint64` | The `entry_reference` of the request that created the entry. |
| `type` | `string` | The type of the entry, e.g. `TABLE_ENTRY`. |
| `id` | `uint64` | The id the commit gave the entry. |

### `CreatedTable`

A table that a commit created.

| Field | Type | Description |
|-------|------|-------------|
| `placeholder` | `int64` (optional) | The placeholder id of the table in the request. |
| `table_id` | `uint64` | The id the commit gave the table. |

### `DataFile`

A Parquet file written by a client. The file includes the statistics of its columns, so the server does not need to
read it.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `path` | The path of the file. |
| `record_count` | `uint64` | The number of rows. |
| `file_size_bytes` | `uint64` | The size of the file. |
| `footer_size` | `uint64` (optional) | The size of the footer. |
| `row_group_count` | `uint64` (optional) | The number of row groups. |
| `partition_id` | `uint64` (optional) | The partitioning the file was written for. Partitionings set in the transaction are referred to by the `partition_id` of their request. |
| `encryption_key` | `string` (default `""`) | The key the file is encrypted with, if any. |
| `mapping_id` | `uint64` (optional) | The name map of the file. Name maps created in the transaction are referred to by the `mapping_id` of their request. |
| `begin_snapshot` | `uint64` (optional) | The snapshot of the rows, if it is not the snapshot of the commit. |
| `max_partial_file_snapshot` | `uint64` (optional) | The last snapshot whose rows the file holds, for partial files. |
| `flush_row_id_start` | `uint64` (optional) | The row id of the first row, for files that hold flushed inlined rows. |
| `created_by_ducklake` | `bool` (default `false`) | Whether the file was written by DuckLake (rather than added with `ducklake_add_data_files`). |
| `column_stats` | list of [`ColumnStats`](#columnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the columns of the file. |
| `partition_values` | list of [`FilePartitionValue`](#filepartitionvalue) (default `[]`) | The values of the partition keys. |
| `delete_files` | list of [`DeleteFile`](#deletefile) (default `[]`) | Delete files for the rows of the file. |

### `DataType`

Where the rows of a file list entry are stored.

| Value | Description |
|-------|-------------|
| `data_file` | In a data file. |
| `inlined_data` | In an inlined data table. |
| `transaction_local_inlined_data` | In the inlined data of the transaction. |

### `DeleteFile`

A delete file written by a client, which replaces the previous delete file of its data file.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` (optional) | The data file whose rows are deleted. |
| `data_file_path` | `path` (default `""`) | The path of the data file. |
| `path` | `path` | The path of the delete file. |
| `format` | [`FileFormat`](#fileformat) (default `"parquet"`) | The format of the delete file. |
| `delete_count` | `uint64` | The number of deleted rows. |
| `file_size_bytes` | `uint64` | The size of the delete file. |
| `footer_size` | `uint64` (default `0`) | The size of the footer of the delete file. |
| `row_group_count` | `uint64` (optional) | The number of row groups of the delete file. |
| `encryption_key` | `string` (default `""`) | The key the file is encrypted with, if any. |
| `overwrites_existing_delete` | `bool` (default `false`) | Whether the delete file replaces a delete file of the data file. |
| `overwritten_delete_file_id` | `uint64` (optional) | The delete file that is replaced. |
| `overwritten_delete_file_path` | `path` (default `""`) | The path of the delete file that is replaced. |
| `begin_snapshot` | `uint64` (optional) | The snapshot of the deletes, if it is not the snapshot of the commit. |
| `max_snapshot` | `uint64` (optional) | The last snapshot whose deletes the file holds, for partial delete files. |
| `source` | [`DeleteFileSource`](#deletefilesource) (default `"regular"`) | Why the delete file was written. |

### `DeleteFileSource`

Why a delete file was written.

| Value | Description |
|-------|-------------|
| `regular` | By a delete. |
| `flush` | By a flush of inlined deletions. |

### `DeleteScanEntry`

A data file whose rows were deleted between two snapshots.

| Field | Type | Description |
|-------|------|-------------|
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file at the end of the range. |
| `previous_delete_file` | [`FileData`](#filedata) (optional) | The delete file at the start of the range. |
| `record_count` | `uint64` | The number of rows of the data file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot of the deletes. |
| `file_deleted` | `bool` (default `false`) | Whether the data file was deleted entirely. |
| `start_snapshot` | `uint64` (optional) | The first snapshot of the range. |
| `end_snapshot` | `uint64` (optional) | The last snapshot of the range. |
| `file_id` | `uint64` (optional) | The id of the data file. |
| `inlined_file_deletions` | list of `uint64` (default `[]`) | Entries of `row_id` and `snapshot_id`. The deleted rows of the file that are inlined in the catalog, with the snapshot that deleted them. |

### `DeletedRowVersion`

A deleted version of an inlined row.

| Field | Type | Description |
|-------|------|-------------|
| `row_id` | `int64` | The row id. |
| `begin_snapshot` | `int64` | The snapshot that inserted the row. |
| `end_snapshot` | `int64` | The snapshot that deleted the row. |

### `DropNotNull`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |

### `DroppedFile`

A data file that is deleted entirely.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The data file. |
| `path` | `path` | The path of the file. |
| `record_count` | `uint64` | The number of rows of the file. |
| `file_size_bytes` | `uint64` | The size of the file. |

### `DynamicFilter`

A filter whose value is only known while the scan runs (e.g. of a top-N).

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` | The column. |
| `comparison_type` | `string` | The comparison, as a DuckDB `ExpressionType` (e.g. `COMPARE_GREATERTHAN`). |
| `type` | `logical_type` | The type of the column. |

### `ErrorInfo`

The details of an error.

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` (default `"Error"`) | The type of the DuckDB exception, e.g. `Catalog`. |
| `message` | `string` (default `""`) | The message of the error. |

### `ErrorResponse`

The body of a response with a status code other than 200: `409` for transaction conflicts, `404` for catalog errors
(e.g. entries that do not exist), `400` for invalid input, `401` for a missing or invalid token and `500` otherwise.

```json
{"error": {"type": "Catalog", "message": "Table with id 1000 does not exist at snapshot 3"}}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | [`ErrorInfo`](#errorinfo) | The error. |

### `FileColumnStats`

The min/max statistics of a column of a file.

| Field | Type | Description |
|-------|------|-------------|
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |

### `FileData`

A file that a client reads.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `path` | The absolute path of the file. |
| `encryption_key` | `string` (default `""`) | The key the file is encrypted with, if any. |
| `file_size_bytes` | `uint64` (default `0`) | The size of the file. |
| `footer_size` | `uint64` (optional) | The size of the footer of the file. |
| `format` | [`FileFormat`](#fileformat) (default `"parquet"`) | The format of the file. |

### `FileForCleanup`

A file that is scheduled for deletion.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The id of the file. |
| `path` | `path` | The path of the file. |
| `schedule_start` | `timestamp` | When the file was scheduled for deletion. |

### `FileFormat`

The format of a file.

| Value | Description |
|-------|-------------|
| `parquet` | A Parquet file. |
| `puffin` | A deletion vector in a Puffin file (Iceberg V3). |

### `FileListEntry`

A data file of a table that is visible in a snapshot, together with its deletes.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` (optional) | The id of the data file. |
| `file_id` | `uint64` (optional) | The id of the file. |
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file of the data file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot that added the file. |
| `max_row_count` | `uint64` (optional) | The number of rows of the file to read. |
| `snapshot_filter_max` | `uint64` (optional) | Only read rows that were inserted up to this snapshot. |
| `snapshot_filter_min` | `uint64` (optional) | Only read rows that were inserted from this snapshot. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `data_type` | [`DataType`](#datatype) (default `"data_file"`) | Where the rows are stored. |
| `inlined_file_deletions` | list of `uint64` (default `[]`) | The positions of the rows of the file that are deleted by deletions inlined in the catalog. |
| `column_min_max` | list of [`FileColumnStats`](#filecolumnstats) (default `[]`) | Each entry has an additional field `column_id`. The statistics of the columns of the scan's `runtime_filter_columns`. |

### `FileListExtendedEntry`

A data file of a table with the ids of its files, used to plan deletes and updates.

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | `uint64` (optional) | The id of the data file. |
| `delete_file_id` | `uint64` (optional) | The id of the delete file. |
| `file` | [`FileData`](#filedata) | The data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The delete file. |
| `row_id_start` | `uint64` (optional) | The row id of the first row of the file. |
| `snapshot_id` | `uint64` (optional) | The snapshot that added the file. |
| `delete_file_begin_snapshot` | `uint64` (optional) | The snapshot that added the delete file. |
| `record_count` | `uint64` | The number of rows of the file. |
| `delete_count` | `uint64` (default `0`) | The number of deleted rows of the file. |
| `mapping_id` | `uint64` (optional) | The name map of the file. |
| `data_type` | [`DataType`](#datatype) (default `"data_file"`) | Where the rows are stored. |

### `FilePartitionValue`

The value of a partition key for the rows of a file.

| Field | Type | Description |
|-------|------|-------------|
| `partition_key_index` | `uint64` | The partition key. |
| `partition_value` | `value` (optional) | The value. |

### `FileRowDeletes`

Deletes of rows of a data file that are inlined in the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The data file. |
| `row_ids` | list of `uint64` | The positions of the deleted rows in the file. |

### `FlushedFileDeletions`

The inlined deletions of rows of data files of a table that were flushed.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `flush_snapshot_id` | `uint64` | The deletions up to this snapshot were flushed. |

### `FlushedInlinedTable`

The inlined rows of a table at a schema version that were flushed.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `int64` | The table. |
| `schema_version` | `uint64` | The schema version of the inlined rows. |
| `flush_snapshot_id` | `uint64` | The rows up to this snapshot were flushed. |

### `GlobalColumnStats`

The statistics of a column of a table.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` (optional) | The column. |
| `contains_null` | `bool` (optional) | Whether the column contains `NULL` values. |
| `contains_nan` | `bool` (optional) | Whether the column contains `NaN` values. |
| `min_value` | `string` (optional) | The minimum value. |
| `max_value` | `string` (optional) | The maximum value. |
| `extra_stats` | `string` (optional) | Type-specific statistics. |
| `min_is_exact` | `bool` (default `false`) | Whether the minimum is exact (rather than a bound). |
| `max_is_exact` | `bool` (default `false`) | Whether the maximum is exact (rather than a bound). |

### `GlobalTableStats`

The statistics of a table.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `initialized` | `bool` (default `true`) | Whether the statistics have been initialized. |
| `record_count` | `uint64` (optional) | The number of rows of the table, absent if unknown. |
| `next_row_id` | `uint64` (default `0`) | The next free row id of the table. |
| `file_size_bytes` | `uint64` (optional) | The size of the files of the table, absent if unknown. |
| `column_stats` | list of [`GlobalColumnStats`](#globalcolumnstats) (default `[]`) | The statistics of the columns. |

### `InlinedFileDeletion`

A deletion of a row of a data file that is inlined in the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `data_file_id` | `uint64` | The data file. |
| `data_file_path` | `path` | The path of the data file. |
| `row_id` | `uint64` | The position of the row in the file. |
| `begin_snapshot` | `uint64` | The snapshot that deleted the row. |
| `delete_file_id` | `uint64` (optional) | The current delete file of the data file. |
| `delete_file` | [`FileData`](#filedata) (optional) | The current delete file. |
| `delete_file_begin_snapshot` | `uint64` (default `0`) | The snapshot that added the current delete file. |

### `InlinedReadMode`

Which inlined rows to read.

| Value | Description |
|-------|-------------|
| `scan` | The rows visible in the snapshot. |
| `insertions` | The rows inserted between the start snapshot and the snapshot. |
| `deletions` | The rows deleted between the start snapshot and the snapshot. |
| `flush` | All rows that were inserted up to the snapshot (including deleted rows), in the order in which they are flushed. |

### `InlinedRowDeletes`

Deletes of inlined rows.

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | `uint64` | The schema version of the inlined rows. |
| `row_ids` | list of `uint64` | The row ids of the deleted rows. |

### `InlinedRows`

A set of rows, e.g. the rows of an inlined data table.

```json
{"columns": [{"name": "row_id", "type": {"id": "BIGINT"}}, {"name": "i", "type": {"id": "INTEGER"}}],
 "rows": [[0, 1], [1, 2]]}
```

Has the fields of [`RowSet`](#rowset).

### `InlinedTableRows`

The rows of the inlined data of a table at a schema version.

Has the fields of [`RowSet`](#rowset), and:

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | `uint64` | The schema version of the table whose columns the rows have. |

### `MacroImplementation`

An implementation (overload) of a macro.

| Field | Type | Description |
|-------|------|-------------|
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the implementation. |
| `sql` | `string` | The body of the macro. |
| `type` | [`MacroType`](#macrotype) | The type of the macro. |
| `parameters` | list of [`MacroParameter`](#macroparameter) (default `[]`) | The parameters of the macro. |

### `MacroInfo`

A macro of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The schema of the macro. |
| `macro_id` | `uint64` (optional) | The id of the macro. |
| `macro_name` | `string` | The name of the macro. |
| `implementations` | list of [`MacroImplementation`](#macroimplementation) (default `[]`) | The implementations of the macro. |

### `MacroParameter`

A parameter of a macro.

| Field | Type | Description |
|-------|------|-------------|
| `parameter_name` | `string` | The name of the parameter. |
| `parameter_type` | `string` (default `""`) | The type of the parameter, `unknown` if it has none. |
| `default_value` | `value` (optional) | The default value of the parameter. |
| `default_value_type` | `string` (default `""`) | Whether the default is a `literal` or an `expression`. |

### `MacroType`

The type of a macro.

| Value | Description |
|-------|-------------|
| `scalar` | A scalar macro. |
| `table` | A table macro. |

### `Metadata`

The global metadata of the DuckLake (the contents of `ducklake_metadata`).

```json
{
  "tags": [{"key": "version", "value": "1.1-dev1"}, {"key": "data_path", "value": "/data/"}],
  "table_settings": [{"table_id": 1, "key": "data_inlining_row_limit", "value": "10"}]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tags` | list of [`Tag`](#tag) (default `[]`) | The global options, including the `version` and the `data_path` of the DuckLake. |
| `schema_settings` | list of [`SchemaSetting`](#schemasetting) (default `[]`) | The options set for schemas. |
| `table_settings` | list of [`TableSetting`](#tablesetting) (default `[]`) | The options set for tables. |

### `NameMap`

A name map (`ducklake_column_mapping` / `ducklake_name_mapping`) of files that were added with
`ducklake_add_data_files`.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `mapping_id` | `uint64` (optional) | The id of the name map. |
| `type` | `string` (default `"map_by_name"`) | The type of the name map. |
| `columns` | list of [`NameMapColumn`](#namemapcolumn) (default `[]`) | The columns of the name map. |

### `NameMapColumn`

A column of a name map: maps a column of a file to a field of the table.

| Field | Type | Description |
|-------|------|-------------|
| `column_id` | `uint64` | The id of the entry in the name map. |
| `source_name` | `string` | The name of the column in the file. |
| `target_field_id` | `uint64` (optional) | The field of the table. |
| `hive_partition` | `bool` (default `false`) | Whether the column is a hive partition of the file path. |
| `parent_column` | `uint64` (optional) | The entry of the parent column, for nested columns. |

### `NullOrder`

Where a sort key orders `NULL` values.

| Value | Description |
|-------|-------------|
| `NULLS_FIRST` |  |
| `NULLS_LAST` |  |
| `ORDER_DEFAULT` |  |

### `OptionSetting`

An option of the DuckLake.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `string` | The option, e.g. `data_inlining_row_limit`. |
| `value` | `string` (default `""`) | The value of the option. |
| `schema_id` | `uint64` (optional) | The schema, for options of a schema. |
| `table_id` | `int64` (optional) | The table, for options of a table. Can be a placeholder. |

### `PartitionField`

A partition key of a table.

| Field | Type | Description |
|-------|------|-------------|
| `partition_key_index` | `uint64` | The position of the key in the partitioning. |
| `column_id` | `uint64` (optional) | The column the key is computed from. |
| `transform` | `string` | The transform of the column, e.g. `identity` or `year`. |

### `PartitionInfo`

The partitioning of a table.

| Field | Type | Description |
|-------|------|-------------|
| `partition_id` | `uint64` (optional) | The id of the partitioning. |
| `table_id` | `uint64` (optional) | The table. |
| `fields` | list of [`PartitionField`](#partitionfield) (default `[]`) | The partition keys. |

### `RemoveColumn`

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The name of the column. |
| `if_exists` | `bool` (default `false`) | Do nothing if the column does not exist. |
| `cascade` | `bool` (default `false`) | Also drop what depends on the column. |

### `RemoveField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the field. |
| `if_exists` | `bool` (default `false`) | Do nothing if the field does not exist. |
| `cascade` | `bool` (default `false`) | Also drop what depends on the field. |

### `RenameColumn`

| Field | Type | Description |
|-------|------|-------------|
| `old_name` | `string` | The current name of the column. |
| `new_name` | `string` | The new name of the column. |

### `RenameField`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the field. |
| `new_name` | `string` | The new name of the field. |

### `RenameTable`

| Field | Type | Description |
|-------|------|-------------|
| `new_name` | `string` | The new name of the table. |

### `RenameView`

| Field | Type | Description |
|-------|------|-------------|
| `new_name` | `string` | The new name of the view. |

### `ResetOptions`

| Field | Type | Description |
|-------|------|-------------|
| `options` | list of `string` (default `[]`) | The options to reset. |

### `ResultColumn`

A column of a set of rows.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | The name of the column. |
| `type` | `logical_type` | The type of the column. |

### `RetryConfig`

How a commit is retried on conflicts.

| Field | Type | Description |
|-------|------|-------------|
| `max_retry_count` | `uint64` | The number of retries. |
| `retry_wait_ms` | `uint64` | The wait before the first retry. |
| `retry_backoff` | `double` | The factor the wait grows with on every retry. |

### `RowCountType`

What `row_count` counts.

| Value | Description |
|-------|-------------|
| `data_files` | The rows in data files minus the deleted rows. |
| `data_files_exact` | The same, absent if it cannot be computed exactly. |
| `inlined` | The inlined rows of a table at a schema version. |

### `RowSet`

The columns and rows of a set of rows.

| Field | Type | Description |
|-------|------|-------------|
| `columns` | list of [`ResultColumn`](#resultcolumn) | The columns of the rows. |
| `rows` | `rows` | The rows, as arrays of values of the types of the columns. |

### `ScanFilters`

Filters of a scan. Filters are optional: a server may ignore them, and a client applies them to the scanned rows
regardless.

`expressions` are SQL expressions that all hold for the rows of the scan. They refer to the columns of the table
by name, and to the fields of structs by their path (`st.a`). The server prunes files on the statistics of their
columns with the parts of the expressions it understands: comparisons of a column with a constant, `IN` lists of
constants, `IS [NOT] NULL`, and `AND` / `OR` combinations of those. The server ignores the other parts, and leaves
out an `OR` entirely if it does not understand one of its parts.

`dynamic_filters` let the server order the files so that these filters can prune files early. For
`runtime_filter_columns` the server returns the min/max statistics of the files in `column_min_max`, so the client
can prune files at runtime.

```json
{"expressions": ["i >= 150", "s = 'a' OR st.a IN (1, 2)"],
 "dynamic_filters": [{"column_id": 1, "comparison_type": "COMPARE_GREATERTHAN", "type": {"id": "INTEGER"}}],
 "runtime_filter_columns": [1]}
```

| Field | Type | Description |
|-------|------|-------------|
| `expressions` | list of `string` (default `[]`) | SQL expressions that hold for all rows of the scan. |
| `dynamic_filters` | list of [`DynamicFilter`](#dynamicfilter) (default `[]`) | Filters whose value is only known while the scan runs. |
| `runtime_filter_columns` | list of `uint64` (default `[]`) | The columns to return the min/max statistics of. |

### `SchemaInfo`

A schema of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The id of the schema. |
| `parent_schema_id` | `uint64` (optional) | The parent schema, for nested schemas. |
| `schema_uuid` | `string` (default `""`) | The uuid of the schema. |
| `schema_name` | `string` | The name of the schema. |
| `path` | `path` (default `""`) | The data path of the schema. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the schema. |

### `SchemaSetting`

An option of the DuckLake that is set for a schema.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The schema. |
| `key` | `string` | The option. |
| `value` | `string` (default `""`) | The value of the option. |

### `SetColumnComment`

| Field | Type | Description |
|-------|------|-------------|
| `entry_type` | [`CatalogEntryType`](#catalogentrytype) | Whether the column is of a table or a view. |
| `column_name` | `string` | The name of the column. |
| `comment` | `value` (optional) | The comment, absent to drop the comment. |

### `SetComment`

| Field | Type | Description |
|-------|------|-------------|
| `entry_type` | [`CatalogEntryType`](#catalogentrytype) | Whether the comment is of a table or a view. |
| `comment` | `value` (optional) | The comment, absent to drop the comment. |

### `SetDefault`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |
| `expression` | `string` (optional) | The default, absent to drop the default. |

### `SetNotNull`

| Field | Type | Description |
|-------|------|-------------|
| `column_path` | list of `string` | The path of the column. |

### `SetOptions`

| Field | Type | Description |
|-------|------|-------------|
| `options` | object of `string` (default `{}`) | The options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`. |

### `SetPartitionedBy`

| Field | Type | Description |
|-------|------|-------------|
| `partitioned_by` | list of `string` (default `[]`) | The partition keys as SQL expressions, empty to reset the partitioning. |

### `SetSortedBy`

| Field | Type | Description |
|-------|------|-------------|
| `sorted_by` | list of `string` (default `[]`) | The sort keys, e.g. `["j DESC NULLS LAST"]`, empty to reset the sort order. |

### `Snapshot`

A snapshot of the DuckLake, which identifies the state of the catalog to read.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | `uint64` | The id of the snapshot. |
| `schema_version` | `uint64` | The schema version of the snapshot, which changes with every change of the catalog. |
| `next_catalog_id` | `uint64` | The next free id of a catalog entry. |
| `next_file_id` | `uint64` | The next free id of a file. |

### `SnapshotBound`

How a timestamp finds a snapshot.

| Value | Description |
|-------|-------------|
| `upper` | The last snapshot before the timestamp. |
| `lower` | The first snapshot after the timestamp. |

### `SnapshotInfo`

A snapshot, with its commit information.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot_id` | `uint64` | The id of the snapshot. |
| `snapshot_time` | `timestamp` | When the snapshot was committed. |
| `schema_version` | `uint64` | The schema version of the snapshot. |
| `next_file_id` | `uint64` | The next free file id of the snapshot. |
| `changes_made` | `string` (default `""`) | The changes of the snapshot, as in `ducklake_snapshot_changes`. |
| `author` | `value` (optional) | The author of the commit. |
| `commit_message` | `value` (optional) | The commit message. |
| `commit_extra_info` | `value` (optional) | Extra information of the commit. |

### `SnapshotReference`

The snapshot that a request reads. A `snapshot` is used as-is, which also allows reading at snapshots that
have been expired since the client loaded them. Without `snapshot` and `snapshot_id`, the latest snapshot is read.

| Field | Type | Description |
|-------|------|-------------|
| `snapshot` | [`Snapshot`](#snapshot) (optional) | The snapshot, as returned by `snapshot_data`. |
| `snapshot_id` | `uint64` (optional) | The id of the snapshot. |

### `SortDirection`

The direction of a sort key.

| Value | Description |
|-------|-------------|
| `ASCENDING` |  |
| `DESCENDING` |  |
| `ORDER_DEFAULT` |  |

### `SortField`

A sort key of a table.

| Field | Type | Description |
|-------|------|-------------|
| `sort_key_index` | `uint64` | The position of the key in the sort order. |
| `expression` | `string` | The expression to sort by. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the expression. |
| `sort_direction` | [`SortDirection`](#sortdirection) | The direction. |
| `null_order` | [`NullOrder`](#nullorder) | The order of `NULL` values. |

### `SortInfo`

The sort order of a table.

| Field | Type | Description |
|-------|------|-------------|
| `sort_id` | `uint64` (optional) | The id of the sort order. |
| `table_id` | `uint64` (optional) | The table. |
| `fields` | list of [`SortField`](#sortfield) (default `[]`) | The sort keys. |

### `TableChangeType`

The changes that `table_changes` returns.

| Value | Description |
|-------|-------------|
| `insertions` | The files with inserted rows. |
| `deletions` | The files with deleted rows. |

### `TableInfo`

A table of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The id of the table. |
| `schema_id` | `uint64` (optional) | The schema of the table. |
| `table_uuid` | `string` (default `""`) | The uuid of the table. |
| `table_name` | `string` | The name of the table. |
| `path` | `path` (default `""`) | The data path of the table. |
| `columns` | list of [`ColumnInfo`](#columninfo) (default `[]`) | The columns of the table. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the table. |

### `TableSetting`

An option of the DuckLake that is set for a table.

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | `uint64` (optional) | The table. |
| `key` | `string` | The option. |
| `value` | `string` (default `""`) | The value of the option. |

### `TableSize`

The storage used by a table.

| Field | Type | Description |
|-------|------|-------------|
| `schema_id` | `uint64` (optional) | The schema of the table. |
| `table_id` | `uint64` (optional) | The table. |
| `table_name` | `string` | The name of the table. |
| `table_uuid` | `string` (default `""`) | The uuid of the table. |
| `file_size_bytes` | `uint64` (default `0`) | The size of the data files. |
| `delete_file_size_bytes` | `uint64` (default `0`) | The size of the delete files. |
| `file_count` | `uint64` (default `0`) | The number of data files. |
| `delete_file_count` | `uint64` (default `0`) | The number of delete files. |

### `Tag`

A key / value pair, e.g. a tag of a catalog entry or an option.

| Field | Type | Description |
|-------|------|-------------|
| `key` | `string` | The key. |
| `value` | `string` (default `""`) | The value. |

### `ViewColumnTag`

A tag of a column of a view.

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | `string` | The column. |
| `key` | `string` | The key of the tag. |
| `value` | `value` (optional) | The value of the tag. |

### `ViewInfo`

A view of the catalog.

| Field | Type | Description |
|-------|------|-------------|
| `view_id` | `uint64` (optional) | The id of the view. |
| `schema_id` | `uint64` (optional) | The schema of the view. |
| `view_uuid` | `string` (default `""`) | The uuid of the view. |
| `view_name` | `string` | The name of the view. |
| `dialect` | `string` (default `"duckdb"`) | The SQL dialect of the view. |
| `column_aliases` | list of `string` (default `[]`) | The names of the columns of the view. |
| `sql` | `string` | The query of the view. |
| `tags` | list of [`Tag`](#tag) (default `[]`) | The tags of the view. |
| `column_tags` | list of [`ViewColumnTag`](#viewcolumntag) (default `[]`) | The tags of the columns of the view. |

<!-- END GENERATED REFERENCE -->

## Reference Implementation

The server (`src/rest/ducklake_rest_server.cpp`, `src/rest/ducklake_rest_endpoints.cpp`) serves a DuckLake catalog
that is attached to DuckDB, with any metadata backend. Every request runs in a DuckLake transaction of its own:

* Read endpoints call the SQL metadata manager of the served catalog.
* `commit_transaction` pins its transaction to the snapshot of the client, replays the requests through the DuckLake
  catalog (catalog changes) and transaction (data changes), and commits it. Commits therefore get the same conflict
  detection, retries, id assignment and statistics maintenance as regular DuckLake commits.

The client (`RESTMetadataManager`, `src/metadata_manager/rest_metadata_manager.cpp`) records the catalog changes of a
transaction as requests while they are made, and sends them together with the data changes of the transaction when it
commits. Data files are written and read by the client.

Both use the code that is generated from the [specification](#specification) (`src/include/rest/ducklake_rest_api.hpp`):
the server implements the handlers of `DuckLakeRESTEndpoints` and `DuckLakeRESTCommitHandler`, the client calls the
endpoints through `DuckLakeRESTClientAPI`. The JSON conversions that are not generated are in
`src/rest/ducklake_rest_serialization.cpp` (column statistics, rows) and `src/rest/ducklake_rest_catalog_changes.cpp`
(alterations).

### Testing

* `test/sql/rest/` tests the API, with a server that `test/template/rest_setup.test_template` starts in a database of
  its own and `test/template/rest_teardown.test_template` stops.
* `test/configs/rest.json` runs the DuckDB tests on a DuckLake that is served over REST (like
  `test/configs/attach_ducklake.json`):
  `build/release/test/unittest --test-config test/configs/rest.json --test-dir duckdb`
* `test/configs/rest_ducklake.json` runs the DuckLake tests with a REST server that its `on_init` starts for every test, and
  `DUCKLAKE_CONNECTION` pointing to the server. Tests that read the metadata catalog directly are skipped, as it is
  attached by the server:
  `build/release/test/unittest --test-config test/configs/rest_ducklake.json "test/sql/*"`
