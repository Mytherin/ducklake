import duckdb
import os
import glob
import json
from dataclasses import dataclass
import fastavro
import datetime
from pprint import pprint


ducklake_snapshot = 1
schema_id = 0
table_id = 1
data_file_id = 1
table_name = 'lineitem'

iceberg_file = '/Users/myth/Programs/duckdb-iceberg/data/persistent/iceberg/lineitem_iceberg'

os.system('cp backup.db file.db')

con = duckdb.connect('file.db')


# read the json metadata
# first figure out which version to use
metadata_dir = os.path.join(iceberg_file, 'metadata')

# try to load the version hint
version_hint = os.path.join(metadata_dir, 'version-hint.text')
version = None
version_name_format = 'v%s.metadata.json'
if os.path.exists(version_hint):
    with open(version_hint, 'r') as f:
        version = f.read().strip()
else:
    raise Exception("No version hint")

version_file = os.path.join(metadata_dir, version_name_format % (version,))

with open(version_file, 'r') as f:
    version_json = json.load(f)


@dataclass
class Column:
    field_id: int
    name: str
    type: str
    required: bool
    child_columns: list
@dataclass
class Schema:
    schema_id: int
    columns: list

def type_conversion(type):
    if type == 'long':
        return 'int64'
    if type == 'string':
        return 'varchar'
    return type

def parse_column(field):
    type = type_conversion(field['type'])
    name = field['name']
    required = field['required']
    if type == 'list':
        raise Exception("eek")
    if type == 'map':
        raise Exception("eek")
    if type == 'struct':
        raise Exception("eek")
    # primitive type
    id = field['id']
    return Column(field_id = id, name = name, type = type, required = required, child_columns = [])

def parse_schema(schema):
    schema_id = schema['schema-id']
    columns = []
    for field in schema['fields']:
        columns.append(parse_column(field))
    return Schema(schema_id = schema_id, columns = columns)

table_uuid = version_json['table-uuid']

schemas = {}
for schema in version_json["schemas"]:
    result = parse_schema(schema)
    schemas[result.schema_id] = result


# write a snapshot that creates the empty table with the first schema id
first_schema_id = min(schemas.keys())
first_schema = schemas[first_schema_id]

# FIXME: this NOW should be replaced with a time RIGHT before the very first snapshot time
sql = f'''
INSERT INTO ducklake_snapshot VALUES ({ducklake_snapshot}, NOW(), 1, 1, {data_file_id});
INSERT INTO ducklake_snapshot_changes VALUES ({ducklake_snapshot}, 'created_table:"main"."{table_name}"');
INSERT INTO ducklake_table VALUES ({table_id}, '{table_uuid}', {ducklake_snapshot}, NULL, {schema_id}, '{table_name}');
'''

# iterate over the columns
column_order = 1
for column in first_schema.columns:
    sql += f'''INSERT INTO ducklake_column VALUES ({column.field_id}, {ducklake_snapshot}, NULL, {table_id}, {column_order}, '{column.name}', '{column.type}', NULL, NULL, {column.required}, NULL);
'''

con.sql(sql)
ducklake_snapshot += 1

@dataclass
class Snapshot:
    sequence_number: int
    timestamp: int
    schema_id: int
    manifest_list: str
def parse_snapshot(snapshot):
    sequence_number = snapshot['sequence-number']
    timestamp = snapshot['timestamp-ms']
    schema_id = snapshot['schema-id']
    manifest_list = snapshot['manifest-list']
    return Snapshot(sequence_number=sequence_number, timestamp=timestamp, schema_id=schema_id, manifest_list=manifest_list)


snapshots = {}
for snapshot in version_json['snapshots']:
    result = parse_snapshot(snapshot)
    snapshots[result.sequence_number] = result

if version_json['format-version'] > 2:
    raise Exception("Unsupported Iceberg version for conversion to DuckLake")

# process the snapshots in-order
snapshot_keys = sorted(snapshots.keys())


def get_manifest_path(path):
    return os.path.normpath(os.path.join(metadata_dir, '..', '..', path))


@dataclass
class ColumnStats:
    column_id: int
    column_size_bytes: int = 'NULL'
    value_count: int = 'NULL'
    null_count: int = 'NULL'
    nan_count: int = 'NULL'
    min_value: str = 'NULL'
    max_value: str = 'NULL'

def parse_iceberg_value(val, type):
    if type == 'int64':
        return str(int.from_bytes(val, byteorder='little'))
    if 'decimal' in type:
        number = str(int.from_bytes(val, byteorder='big', signed=True))
        scale = int(type.split(',')[-1].replace(')', '').strip())
        if number == '0':
            return '0'
        return number[:-scale] + '.' + number[len(number) - scale:]
    if type == 'varchar':
        return val.decode('utf8')
    if type == 'date':
        return str(datetime.datetime(year=1970, month = 1, day = 1) + datetime.timedelta(days=int.from_bytes(val, byteorder='little')))
    raise Exception(f"Unknown type for deserializing Iceberg type {val} {type}")


def parse_min_max_value(val, column_id, schema_id):
    column_type = None
    for column in schemas[schema_id].columns:
        if column.field_id == column_id:
            column_type = column.type
            break
    if column_type is None:
        raise Exception(f"Could not find type for column {column_id}")
    val = parse_iceberg_value(val, column_type)
    return val


def parse_optional_map(data_file, key, column_stats, column_stats_key, schema_id = None):
    if key not in data_file:
        return
    for entry in data_file[key]:
        column_id = entry['key']
        stats_value = entry['value']
        if column_id not in column_stats:
            column_stats[column_id] = ColumnStats(column_id = column_id)
        if schema_id is not None:
            stats_value = parse_min_max_value(stats_value, column_id, schema_id)
        setattr(column_stats[column_id], column_stats_key, stats_value)

@dataclass
class DataFile:
    path: str
    record_count: int
    file_size_bytes: int
    row_id_start: int
    column_stats: dict

def parse_data_file(schema_id, data_file):
    if data_file['file_format'] != 'PARQUET':
        raise Exception(f"Unsupported format {data_file['file_format']} - only Iceberg with Parquet files is supported for DuckLake conversion")
    if len(data_file['partition']) > 0:
        raise Exception("FIXME: partitioning")
    data_file_path = get_manifest_path(data_file['file_path'])
    record_count = data_file['record_count']
    file_size_bytes = data_file['file_size_in_bytes']
    row_id_start = data_file.get('first_row_id', 'NULL')
    column_stats = {}

    parse_optional_map(data_file,'column_sizes', column_stats, 'column_size_bytes')
    parse_optional_map(data_file,'value_counts', column_stats, 'value_count')
    parse_optional_map(data_file,'null_value_counts', column_stats, 'null_count')
    parse_optional_map(data_file,'nan_value_counts', column_stats, 'nan_count')
    parse_optional_map(data_file,'lower_bounds', column_stats, 'min_value', schema_id)
    parse_optional_map(data_file,'upper_bounds', column_stats, 'max_value', schema_id)
    return DataFile(path=data_file_path, record_count=record_count, file_size_bytes=file_size_bytes, row_id_start=row_id_start, column_stats=column_stats)

def parse_delete_file(schema_id, data_file):
    raise Exception("eek")

@dataclass
class ManifestFile:
    file_list: list
    delete_list: list

def convert_manifest_file(schema_id, path):
    file_list = []
    delete_list = []
    with open(path, 'rb') as f:
        avro_reader = fastavro.reader(f)
        for record in avro_reader:
            status = record['status']
            data_file = record['data_file']
            content_type = data_file['content']
            if status == 0:
                # this file was added by a previous snapshot - skip
                continue
            if status == 2:
                # this file was deleted by this snapshot - add a delete
                delete_list.append(get_manifest_path(data_file['file_path']))
                continue
            if content_type == 0:
                file_list.append(parse_data_file(schema_id, data_file))
            elif content_type == 1:
                file_list.append(parse_delete_file(schema_id, data_file))
            elif content_type == 2:
                raise Exception("Equality deletes are not supported in DuckLake")
            else:
                raise Exception("Unknown content type")
    return ManifestFile(file_list=file_list, delete_list = delete_list)

@dataclass
class ManifestList:
    manifest_files: list

def convert_manifest_list(snapshot):
    manifest_list = get_manifest_path(snapshot.manifest_list)
    file_list = []
    with open(manifest_list, 'rb') as f:
        avro_reader = fastavro.reader(f)
        for record in avro_reader:
            file_list.append(convert_manifest_file(snapshot.schema_id, get_manifest_path(record['manifest_path'])))
    return ManifestList(manifest_files = file_list)

for sequence in snapshot_keys:
    snapshot = snapshots[sequence]
    manifest_list = convert_manifest_list(snapshots[sequence])
    snapshot_time = str(datetime.datetime.fromtimestamp(snapshot.timestamp / 1000.0))
    # FIXME: fix schema id and data file id
    # FIXME: detect if we inserted or deleted or both for snapshot_changes
    sql = f'''
INSERT INTO ducklake_snapshot VALUES ({ducklake_snapshot}, '{snapshot_time}', 1, 1, {data_file_id});
INSERT INTO ducklake_snapshot_changes VALUES ({ducklake_snapshot}, 'inserted_into_table:{table_id}');
'''
    data_file_sql = ''
    column_stats_sql = ''
    for manifest_file in manifest_list.manifest_files:
        for file in manifest_file.file_list:
            if len(data_file_sql) > 0:
                data_file_sql += ", "
            data_file_sql += f"({data_file_id}, {table_id}, {ducklake_snapshot}, NULL, {data_file_id}, '{file.path}', false, 'parquet', {file.record_count}, {file.file_size_bytes}, NULL, {file.row_id_start}, NULL, NULL, NULL)"
            data_file_id += 1
            for column_id in file.column_stats:
                stats = file.column_stats[column_id]
                if len(column_stats_sql) > 0:
                    column_stats_sql += ", "
                contains_nan = 'NULL'
                if stats.nan_count != 'NULL':
                    contains_nan = stats.nan_count != '0'
                column_stats_sql += f"({data_file_id}, {table_id}, {column_id}, {stats.column_size_bytes}, {stats.value_count}, {stats.null_count}, '{stats.min_value}', '{stats.max_value}', {contains_nan})"

    sql += "INSERT INTO ducklake_data_file VALUES " + data_file_sql + ";\n"
    sql += "INSERT INTO ducklake_file_column_statistics VALUES " + column_stats_sql + ";\n"
    print(sql)

    # next snapshot
    ducklake_snapshot += 1
    con.query(sql)
