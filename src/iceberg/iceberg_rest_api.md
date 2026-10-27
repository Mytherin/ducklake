# Iceberg REST catalog API for DuckLake

A DuckLake can be served to Iceberg clients through the [Iceberg REST catalog API](https://iceberg.apache.org/rest-catalog-spec/).
The clients (the `iceberg` extension of DuckDB, PyIceberg, Spark, Trino, ...) see Iceberg tables, and read and write them.
DuckLake does not store any Iceberg metadata: the metadata files of Iceberg are emulated.

- **When a client commits** (`POST .../tables/{table}`), it has written data files, a manifest list and manifests to the
  storage, and it sends the updates of the table metadata. The server reads the manifest list and the manifests, compares
  the files of the new snapshot with the files of the table, and applies the difference as DuckLake operations in one
  DuckLake transaction.
- **When a client loads a table** (`GET .../tables/{table}`), the server generates the Iceberg metadata of the table from
  the DuckLake catalog: the table metadata (JSON) and, for each snapshot, a manifest list and manifests (Avro), and
  Parquet files for delete files and for data that DuckLake keeps in its catalog. The files are not part of DuckLake:
  they are written to the `metadata_path` of the server (the default), or they are *virtual* (`virtual_files := true`).

## Usage

```sql
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data/');
SELECT * FROM ducklake_iceberg_serve('lake', port := 8181, metadata_path := '/tmp/iceberg_metadata');

-- or serve a DuckLake in a database of its own, isolated from the caller
SELECT * FROM ducklake_iceberg_serve('ducklake:metadata.ducklake', port := 8181,
                                     attach_options := 'DATA_PATH ''data/''', wait := true);
```

| Parameter | Description |
|---|---|
| `catalog` | The name of an attached DuckLake catalog, or a `ducklake:` path to attach in isolation. |
| `host`, `port` | The address to listen on (default `localhost`, a free port). |
| `prefix` | The prefix of the catalog in the URLs, `/v1/<prefix>/...` (default: the name of the catalog). The `warehouse` of a client selects the catalog. A server can serve several catalogs. |
| `token` | Requests have to carry this token (`Authorization: Bearer <token>`). The OAuth endpoint hands the token out for the client secret. |
| `metadata_path` | The directory (or object store prefix) where the generated metadata files go. **The clients have to be able to read it**: use a path of a shared file system or an object store if the clients run on other machines. With virtual files, it is only a scratch directory. |
| `virtual_files` | If `true`, the files do not exist: their URLs are addresses of the server (`<base>/v1/<prefix>/metadata/t<table id>/<name>`) and the server generates a file from the DuckLake catalog when it is requested. The name of a file has what it takes to generate it (the table, the snapshot, the manifest, ...), so the server keeps no list of files, and the same name is always the same content. The files can be read with ranges and HEAD requests; if the server has a token, the URLs are signed (`?sig=`). **Clients have to read `http://` locations**: the Iceberg extension of DuckDB does, the stock file IO of PyIceberg and the Hadoop HTTP file system of Spark do not. |
| `public_uri` | The address of the server in the URLs of virtual files (default: the `Host` of the request), e.g. when the server is behind a proxy. |
| `snapshot_limit` | The number of snapshots of the history that are exposed (default 100). |
| `table_config` | Properties for the storage of the tables that clients get with every table, e.g. `{'s3.endpoint': '...', 's3.access-key-id': '...'}` (they are in the `config` and the `storage-credentials` of a loaded table). |
| `defaults`, `overrides` | The properties that clients get from `/v1/config`. |
| `attach_options`, `wait` | As for `ducklake_rest_serve`. |

`ducklake_iceberg_stop(listen_uri)` stops a server.

```sql
-- the Iceberg extension of DuckDB
ATTACH 'lake' AS ice (TYPE ICEBERG, ENDPOINT 'http://localhost:8181', AUTHORIZATION_TYPE 'none');
CREATE TABLE ice.main.t(i INTEGER);
INSERT INTO ice.main.t VALUES (1), (2);
```

```python
# PyIceberg
from pyiceberg.catalog import load_catalog
catalog = load_catalog('rest', uri='http://localhost:8181', warehouse='lake')
catalog.load_table('main.t').scan().to_arrow()
```

The server needs no more than the DuckLake: the data stays where DuckLake has it, in the `DATA_PATH`, and Iceberg
clients write their data files to the location of the table (`<DATA_PATH>/<schema>/<table>`).

## Endpoints

| Endpoint | Support |
|---|---|
| `GET /v1/config`, `POST /v1/oauth/tokens` | yes |
| namespaces: list, create, load, exists, drop, update properties | yes. Nested namespaces are nested schemas of DuckLake. The properties are stored by the server (the `location` is the path of the schema). |
| tables: list, create (also `stage-create`), load, exists, update (commit), drop, rename, register | yes |
| `POST /v1/{prefix}/transactions/commit` | yes, the tables are committed in one DuckLake transaction |
| `POST .../metrics`, `GET .../credentials` | yes |
| scan planning: `POST .../plan`, `GET`/`DELETE .../plan/{plan-id}` | yes, plans are complete and synchronous (filters are not evaluated: the client filters). `POST .../tasks` has nothing to fetch. |
| views: list, create, load, exists, replace, drop, rename | yes. A view is a DuckLake view: it has one version, the SQL of which has to be a `SELECT` that DuckDB can run. |

## Mapping

| Iceberg | DuckLake |
|---|---|
| namespace | schema |
| table | table; the field ids are the ids of the columns, the snapshot ids are the ids of the snapshots |
| snapshot | each snapshot of DuckLake that changed the data of the table (the data, the deletes, inlined data, compaction) |
| schema history | the columns of the table in each snapshot |
| `identity`, `year`, `month`, `day`, `hour`, `bucket[N]` | `identity`, `epoch_year`, `epoch_month`, `epoch_day`, `epoch_hour`, `bucket(N)`. The partitioning with the other transforms of DuckLake (`year`, ...) is not exposed. |
| sort order | `SORTED BY` of the table, when it only has columns. |
| position delete files | delete files (the delete files of DuckLake are rewritten with the ids of the columns of Iceberg, deletion vectors are written as position delete files) |
| data inlined in the catalog | written to a Parquet file for the latest snapshot |
| table properties, references (tags, branches), namespace and view properties | stored by the server in the `ducklake_iceberg_property` table in the metadata catalog |
| `remove-snapshots`, statistics, `set-location` | ignored: the history of DuckLake is managed with `ducklake_expire_snapshots` |

The updates of a table are applied like this:

- `add-schema` + `set-current-schema`: columns are added, dropped, renamed, promoted (`ALTER TABLE`). The columns are matched by field id. A new column gets the id that DuckLake gives it: clients get the schema back in the response.
- `add-spec` + `set-default-spec`: `ALTER TABLE ... SET PARTITIONED BY`.
- `add-snapshot` + `set-snapshot-ref` (main): the files of the snapshot are compared with the files of the table. New data files are added with `ducklake_add_data_files` (with the partition values of the manifest), data files that the snapshot does not have are dropped, position delete files are merged into the delete file of their data file. Setting the main branch to a snapshot of the history (rollback) works the same way.
- `set-snapshot-ref` for other references: tags and branches that point to snapshots of the history. New snapshots cannot be added to another branch than main.
- `set-properties`, `remove-properties`, `add-sort-order` + `set-default-sort-order`.

The requirements (`assert-ref-snapshot-id`, `assert-table-uuid`, `assert-current-schema-id`, ...) are checked first, a failing requirement is a `409 CommitFailedException`. Commits of a table are serialized by the server, and DuckLake detects conflicts with other writers.

## Limitations

- The data files have to be Parquet files with the columns of the table (DuckLake maps them by name), they can be anywhere the server can read them. Equality deletes are not supported.
- Format version 2: types without equivalent in Iceberg v2 (`UBIGINT`, `HUGEINT`, `INTERVAL`, `TIMETZ`, `VARIANT`, `GEOMETRY`, nanosecond timestamps ...) make the table fail to load. Encrypted DuckLakes are not supported.
- The snapshots of the history have the files of their snapshot, but the snapshots whose files DuckLake has combined with `ducklake_merge_adjacent_files` (partial files) or whose rows are inlined in the catalog are not exposed (the latest snapshot always is).
- The properties and references are written after the commit of the DuckLake transaction: they are not part of it.
- The server has no authorization besides the token: every client can run the `SELECT` of a view and read the files that the server can read (`register`, manifests). Only serve DuckLakes to trusted clients.

## Tests

- `test/sql/iceberg/*.test` read a DuckLake with requests to the API (`ducklake_rest_request`) and inspect the generated manifests (`ducklake_avro_read`).
- `test/iceberg/run_tests.py` runs the Iceberg extension of DuckDB and PyIceberg against a served DuckLake (see `test/iceberg/harness.py`). It needs the Python packages `duckdb`, `pyiceberg`, `pyiceberg-core` and `pyarrow` (`ICEBERG_PYTHONPATH` can point to a directory with them, `DUCKLAKE_SHELL` to the DuckDB shell with DuckLake).
