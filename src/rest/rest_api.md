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

The function returns the `listen_uri` of the server. Starting a server on a port that is in use by another server of
the same process replaces that server. `ducklake_rest_stop(listen_uri)` stops a server.

### Connecting to a server

```sql
ATTACH 'ducklake:rest:http://localhost:8181' AS lake;
```

The client reads the data path and the options of the DuckLake from the server. Data files are read and written by the
client directly, so the data path has to be accessible from both the client and the server.

## Conventions

* All endpoints live under `/v1/<endpoint>`. Requests and responses are JSON objects. Every endpoint accepts `POST`
  with a JSON body; read-only endpoints also accept `GET` with a JSON body.
* Each request runs in a transaction of its own on the server.
* Errors are reported with a non-200 status code and a body of the form
  `{"error": {"type": "<DuckDB exception type>", "message": "..."}}`. The status code is `409` for transaction
  conflicts, `404` for catalog errors (e.g. entries that do not exist), `400` for invalid input and `500` otherwise.
* Identifiers (`snapshot_id`, `table_id`, `column_id`, ...) are the ids of the DuckLake specification.
* Optional values are `null` or absent.
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
| everything else                  | the string representation of the value, e.g. `"2025-01-01"`    |

Values without a known type (e.g. tags) are `null`, a JSON string for `VARCHAR` values, or an object
`{"type": <type>, "value": <value>}`.

### Snapshots

```json
{"snapshot_id": 3, "schema_version": 1, "next_catalog_id": 2, "next_file_id": 1}
```

## Read Endpoints

### `config`

Returns the global metadata of the DuckLake (the contents of `ducklake_metadata`).

Response:

```json
{
  "tags": [{"key": "version", "value": "1.1-dev1"}, {"key": "data_path", "value": "/data/"}],
  "schema_settings": [{"schema_id": 0, "key": "...", "value": "..."}],
  "table_settings": [{"table_id": 1, "key": "...", "value": "..."}]
}
```

### `snapshot_data`

Returns a snapshot together with (optionally) the catalog and the table statistics of that snapshot, so a client can
start a transaction in a single round-trip.

| Parameter         | Description                                                                              |
|-------------------|------------------------------------------------------------------------------------------|
| `snapshot_id`     | The snapshot to return, the latest snapshot if absent.                                   |
| `at`              | `{"unit": "version" \| "timestamp", "value": <value>}` - find the snapshot of an `AT` clause. |
| `bound`           | `"upper"` (default) or `"lower"`: whether a timestamp finds the last snapshot before or the first snapshot after it. |
| `include_catalog` | Whether to return the catalog (schemas, tables, views, macros, partitioning, sorting).   |
| `include_stats`   | Whether to return the table statistics.                                                  |
| `include_record_counts` | Whether to return the record counts of the tables (`record_counts`: `[{"table_id": 1, "record_count": 10}]`). |

Response:

```json
{
  "snapshot": {"snapshot_id": 1, "schema_version": 1, "next_catalog_id": 2, "next_file_id": 0},
  "catalog": {
    "schemas": [{"schema_id": 0, "parent_schema_id": null, "schema_uuid": "...", "schema_name": "main",
                 "path": "/data/main/", "tags": []}],
    "tables": [{
      "table_id": 1, "schema_id": 0, "table_uuid": "...", "table_name": "t", "path": "/data/main/t/",
      "columns": [{"column_id": 1, "column_name": "i", "column_type": "int32", "initial_default": null,
                   "default_value": null, "default_value_type": "", "nulls_allowed": true, "tags": [],
                   "children": []}],
      "tags": [],
      "inlined_data_tables": [{"table_name": "ducklake_inlined_data_1_1", "schema_version": 1}]
    }],
    "views": [{"view_id": 2, "schema_id": 0, "view_uuid": "...", "view_name": "v", "dialect": "duckdb",
               "column_aliases": [], "sql": "SELECT ...", "tags": [], "column_tags": []}],
    "macros": [{"schema_id": 0, "macro_id": 3, "macro_name": "m", "implementations": [
      {"dialect": "duckdb", "sql": "a + 1", "type": "scalar",
       "parameters": [{"parameter_name": "a", "parameter_type": "unknown", "default_value": null,
                       "default_value_type": ""}]}]}],
    "partitions": [{"partition_id": 4, "table_id": 1,
                    "fields": [{"partition_key_index": 0, "column_id": 1, "transform": "identity"}]}],
    "sorts": [{"sort_id": 5, "table_id": 1, "fields": [{"sort_key_index": 0, "expression": "i",
               "dialect": "duckdb", "sort_direction": "ASCENDING", "null_order": "NULLS_LAST"}]}]
  },
  "stats": [{
    "table_id": 1, "initialized": true, "record_count": 2, "next_row_id": 2, "file_size_bytes": 0,
    "column_stats": [{"column_id": 1, "contains_null": false, "contains_nan": null, "min_value": "1",
                      "max_value": "2", "extra_stats": null, "min_is_exact": true, "max_is_exact": true}]
  }]
}
```

### `select`

Plans a scan of a table: returns the data files of the table that are visible in a snapshot, together with their
delete files and inlined deletions.

| Parameter     | Description                                                                           |
|---------------|---------------------------------------------------------------------------------------|
| `snapshot_id` | The snapshot to scan.                                                                 |
| `table_id`    | The table to scan.                                                                    |
| `extended`    | Return the extended file list used to plan deletes and updates (see below).          |

Response:

```json
{
  "files": [{
    "data_file_id": 1, "file_id": 1,
    "file": {"path": "/data/main/t/f1.parquet", "encryption_key": "", "file_size_bytes": 84, "footer_size": 20,
             "format": "parquet"},
    "delete_file": {"path": "/data/main/t/d1.parquet", "file_size_bytes": 40, "footer_size": 20, ...},
    "row_id_start": 0, "snapshot_id": 2, "max_row_count": null, "snapshot_filter_max": null,
    "snapshot_filter_min": null, "mapping_id": null, "data_type": "data_file",
    "inlined_file_deletions": [3, 4], "column_min_max": []
  }]
}
```

The rows of a table that are inlined in the catalog are read with `read_inlined_data`.

The extended file list (`"extended": true`) returns the ids of the data and delete files and the number of deleted
rows of each file instead:

```json
{"files": [{"file_id": 1, "delete_file_id": 2, "file": {...}, "delete_file": {...}, "row_id_start": 0,
            "snapshot_id": 2, "delete_file_begin_snapshot": 3, "record_count": 100, "delete_count": 10,
            "mapping_id": null, "data_type": "data_file"}]}
```

### `read_inlined_data`

Reads the rows of an inlined data table that are visible in a snapshot.

| Parameter     | Description                                                       |
|---------------|-------------------------------------------------------------------|
| `snapshot_id` | The snapshot to read.                                             |
| `table_name`  | The inlined data table (from `inlined_data_tables` of the table). |
| `columns`     | The columns to read, e.g. `["row_id", "i"]`.                      |

Response:

```json
{"columns": [{"name": "row_id", "type": {"id": "BIGINT"}}, {"name": "i", "type": {"id": "INTEGER"}}],
 "rows": [[0, 1], [1, 2]]}
```

### `table_schema_versions`

Returns when a table and its schema versions were created.

| Parameter        | Description                                                                   |
|------------------|-------------------------------------------------------------------------------|
| `table_id`       | The table.                                                                    |
| `snapshot_id`    | If set, also return the first and last schema version of the table at it.    |
| `schema_version` | If set, also return the snapshot that created this schema version.           |

Response: `{"begin_snapshot": 1, "creation": 1, "last_change": 3, "schema_version_begin_snapshot": 5}`

### `row_count`

Counts the rows of a table that are visible in a snapshot.

| Parameter     | Description                                                                                       |
|---------------|---------------------------------------------------------------------------------------------------|
| `snapshot_id` | The snapshot.                                                                                     |
| `count`       | `"data_files"` (rows in data files minus deleted rows), `"data_files_exact"` (the same, `null` if it cannot be computed exactly) or `"inlined"` (rows of an inlined data table). |
| `table_id`    | The table, for data file counts.                                                                  |
| `table_name`  | The inlined data table, for inlined counts.                                                       |

Response: `{"row_count": 42}`

### `inlined_tables`

Returns the names of all inlined data tables of a table (of any schema version).

Request: `{"table_id": 1}`, response: `{"table_names": ["ducklake_inlined_data_1_1"]}`

### `next_column_id`

Returns the next free column (field) id of a table. Request: `{"table_id": 1}`, response: `{"next_column_id": 4}`

### `column_created_with_table`

Returns whether a column was created together with its table (rather than added later).
Request: `{"table_name": "t", "column_name": "i"}`, response: `{"created_with_table": true}`

### `list_snapshots`

Returns all snapshots.

Response:

```json
{"snapshots": [{"snapshot_id": 0, "snapshot_time": "2026-01-01 00:00:00+00", "schema_version": 0,
                "next_file_id": 0, "changes_made": "created_schema:\"main\"", "author": null,
                "commit_message": null, "commit_extra_info": null}]}
```

## Committing Transactions

### `commit_transaction`

Commits a set of changes atomically. The changes are a list of requests that are applied in order; later requests
can refer to entries created by earlier requests through negative placeholder ids: `-1` is the first entry (schema,
table, view, ...) created in the transaction, `-2` the second one, etc.

| Parameter                    | Description                                                                               |
|------------------------------|-------------------------------------------------------------------------------------------|
| `snapshot_id`                | The snapshot the changes are based on. The server detects conflicts with every snapshot committed after it. If absent, the changes are based on the latest snapshot. |
| `commit_info`                | `{"author": ..., "commit_message": ..., "commit_extra_info": ..., "is_commit_info_set": bool}` |
| `requires_new_inlined_table` | Whether the commit inlines rows into a table that has no inlined data table yet.          |
| `requests`                   | The changes: `[{"path": "<request>", "parameters": {...}}]`.                               |

Response: the committed snapshot, `{"snapshot_id": 4, "schema_version": 2}`. Both are `null` if the requests did not
change anything.

If the transaction conflicts with a transaction that committed after `snapshot_id` the server retries the commit
when that is safe; otherwise it fails with a `409`.

#### `create_schema`

| Parameter     | Description                                                     |
|---------------|-----------------------------------------------------------------|
| `schema_path` | The path of the schema, e.g. `["parent", "child"]`.             |
| `schema_uuid` | Optional, the uuid of the schema.                                |
| `path`        | Optional, the data path of the schema.                           |

#### `create_table`

| Parameter        | Description                                                                         |
|------------------|-------------------------------------------------------------------------------------|
| `table_id`       | The placeholder id of the table.                                                    |
| `schema_path`    | The schema of the table, e.g. `["main"]`.                                           |
| `table_name`     | The name of the table.                                                              |
| `table_uuid`     | Optional, the uuid of the table.                                                    |
| `path`           | Optional, the data path of the table.                                               |
| `columns`        | `[{"column_name": "i", "column_type": "INTEGER", "default": "42", "not_null": false, "comment": "..."}]`. `default` is a SQL expression. |
| `partitioned_by` | Optional, the partition keys as SQL expressions, e.g. `["year(ts)"]`.               |
| `sorted_by`      | Optional, the sort keys as SQL expressions.                                         |
| `options`        | Optional, table options as SQL expressions, e.g. `{"data_inlining_row_limit": "10"}`. |
| `comment`        | Optional, the comment of the table.                                                 |

Columns get the field ids `1..n` in the order of the columns (nested fields are numbered depth-first), as in
`CREATE TABLE`.

Requests that refer to existing entries identify them by `schema_path` and `name`. They are applied in order, so a
name always refers to the entry as it is at that point of the transaction.

#### `drop_schema`, `drop_table`, `drop_view`, `drop_macro`

| Parameter     | Description                                                                    |
|---------------|--------------------------------------------------------------------------------|
| `schema_path` | The schema of the entry (for `drop_schema`: the path of the schema itself).    |
| `name`        | The name of the entry.                                                         |
| `macro_type`  | For `drop_macro`: `"scalar"` (default) or `"table"`.                           |

Schemas have to be empty when they are dropped.

#### `create_view`

| Parameter        | Description                                          |
|------------------|------------------------------------------------------|
| `schema_path`    | The schema of the view.                              |
| `view_name`      | The name of the view.                                |
| `view_uuid`      | Optional, the uuid of the view.                      |
| `dialect`        | The SQL dialect of the view, only `duckdb` for now.  |
| `sql`            | The query of the view.                               |
| `column_aliases` | Optional, the names of the columns of the view.      |
| `comment`        | Optional, the comment of the view.                   |

#### `create_macro`

| Parameter     | Description                                                                                  |
|---------------|----------------------------------------------------------------------------------------------|
| `schema_path` | The schema of the macro.                                                                     |
| `macro`       | The macro, in the format of the `macros` of the [catalog](#snapshot_data) (without ids).      |

#### `alter_table`, `alter_view`

| Parameter     | Description                                 |
|---------------|---------------------------------------------|
| `schema_path` | The schema of the table or view.            |
| `name`        | The name of the table or view.              |
| `alter`       | The change, one of the alterations below.   |

Columns are referred to by name; nested fields by their path from the top-level column, e.g. `["s", "a"]`.
Expressions are SQL expressions.

| `type`               | Fields                                                                         |
|----------------------|--------------------------------------------------------------------------------|
| `rename_table`       | `new_name`                                                                     |
| `rename_view`        | `new_name`                                                                     |
| `rename_column`      | `old_name`, `new_name`                                                         |
| `rename_field`       | `column_path`, `new_name`                                                      |
| `add_column`         | `column` (as in `create_table`), `if_not_exists`                               |
| `add_field`          | `column_path` (of the parent), `field` (as in `create_table`), `if_not_exists` |
| `remove_column`      | `column_name`, `if_exists`, `cascade`                                          |
| `remove_field`       | `column_path`, `if_exists`, `cascade`                                          |
| `change_column_type` | `column_path`, `column_type`, `expression` (optional, the `USING` expression)  |
| `set_default`        | `column_path`, `expression` (absent to drop the default)                       |
| `set_not_null`       | `column_path`                                                                  |
| `drop_not_null`      | `column_path`                                                                  |
| `set_partitioned_by` | `partitioned_by`: the partition keys (empty to reset the partitioning)         |
| `set_sorted_by`      | `sorted_by`: the sort keys, e.g. `["j DESC NULLS LAST"]` (empty to reset)      |
| `set_options`        | `options`: `{"<option>": "<expression>"}`                                      |
| `reset_options`      | `options`: `["<option>"]`                                                      |
| `set_comment`        | `entry_type` (`TABLE_ENTRY` / `VIEW_ENTRY`), `comment`                         |
| `set_column_comment` | `entry_type`, `column_name`, `comment`                                         |

#### `insert_rows`

Inserts rows that are inlined in the catalog.

| Parameter      | Description                                                                                  |
|----------------|----------------------------------------------------------------------------------------------|
| `table_id`     | The table.                                                                                   |
| `column_types` | The types of the columns of the rows, in the order of the columns of the table.              |
| `rows`         | The rows, as arrays of values.                                                               |
| `row_ids`      | Optional, the row ids of the rows (for updates that keep the row ids of the updated rows).   |
| `column_stats` | The statistics of the rows per column, see [Column statistics](#column-statistics).          |

#### `insert_files`

Registers Parquet files that were written by the client. The files have to include the statistics of their columns,
so the server does not need to read them.

| Parameter  | Description            |
|------------|------------------------|
| `table_id` | The table.             |
| `files`    | The files, see below.  |

```json
{
  "path": "/data/main/t/f1.parquet", "record_count": 2, "file_size_bytes": 512, "footer_size": 200,
  "row_group_count": 1, "partition_id": null, "encryption_key": "", "mapping_id": null, "begin_snapshot": null,
  "max_partial_file_snapshot": null, "flush_row_id_start": null, "created_by_ducklake": true,
  "column_stats": [{"column_id": 1, ...}],
  "partition_values": [{"partition_key_index": 0, "partition_value": "2025"}],
  "delete_files": []
}
```

#### Column statistics

```json
{"column_id": 1, "type": {"id": "INTEGER"}, "min_value": "1", "max_value": "2", "null_count": 0,
 "value_count": 2, "column_size_bytes": 10, "contains_nan": null, "any_valid": true, "bounds_unknown": false,
 "min_is_exact": true, "max_is_exact": true, "extra_stats": null}
```

#### `delete_data`

Deletes rows from a table.

| Parameter          | Description                                                                                  |
|--------------------|----------------------------------------------------------------------------------------------|
| `table_id`         | The table.                                                                                   |
| `delete_files`     | Delete files written by the client, see below.                                               |
| `inlined_deletes`  | Deletes of inlined rows: `[{"table_name": "<inlined data table>", "row_ids": [1, 2]}]`.      |
| `file_row_deletes` | Deletes of rows of data files that are inlined in the catalog: `[{"data_file_id": 1, "row_ids": [0, 5]}]`. `row_ids` are the positions of the rows in the file. |
| `dropped_files`    | Data files that are deleted entirely: `[{"data_file_id": 1, "path": "...", "record_count": 10, "file_size_bytes": 100}]`. |
| `delete_attempted` | Whether a delete was evaluated against the table, even if it did not delete any rows. This makes the commit conflict with concurrent inserts. |

A delete file replaces the previous delete file of its data file:

```json
{"data_file_id": 1, "data_file_path": "/data/main/t/f1.parquet", "path": "/data/main/t/d1.parquet",
 "format": "parquet", "delete_count": 10, "file_size_bytes": 300, "footer_size": 100, "row_group_count": 1,
 "encryption_key": "", "overwrites_existing_delete": true, "overwritten_delete_file_id": 3,
 "overwritten_delete_file_path": "/data/main/t/d0.parquet", "begin_snapshot": null, "max_snapshot": null,
 "source": "regular"}
```
