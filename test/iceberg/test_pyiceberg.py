"""PyIceberg reads and writes DuckLake through the Iceberg REST API."""
import datetime
import unittest

from clients import HAVE_PYICEBERG, pyiceberg_catalog, requires_pyiceberg
from harness import IcebergTestCase

if HAVE_PYICEBERG:
    import pyarrow as pa
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
    from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestampType


@requires_pyiceberg
class PyIcebergTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'

    def catalog(self):
        return pyiceberg_catalog(self.uri)

    def test_namespaces(self):
        cat = self.catalog()
        name = self.unique('ns')
        cat.create_namespace(name)
        self.assertIn((name,), cat.list_namespaces())
        with self.assertRaises(NamespaceAlreadyExistsError):
            cat.create_namespace(name)
        cat.drop_namespace(name)
        self.assertNotIn((name,), cat.list_namespaces())
        with self.assertRaises(NoSuchNamespaceError):
            cat.load_namespace_properties(name)

    def test_create_append_read(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('ns1')
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        table = cat.create_table('ns1.t1', schema)
        table.append(pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']}, schema=schema))
        table = cat.load_table('ns1.t1')
        self.assertEqual(table.scan().to_arrow().sort_by('id').to_pydict(), {'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        self.assertEqual(self.shell.sql('SELECT * FROM dl.ns1.t1 ORDER BY id'), ['1,a', '2,b', '3,c'])
        # rows that are added through DuckLake
        self.shell.sql("INSERT INTO dl.ns1.t1 VALUES (4, 'd')")
        table = cat.load_table('ns1.t1')
        self.assertEqual(table.scan().to_arrow().num_rows, 4)
        self.assertEqual(len(table.snapshots()), 2)

    def test_missing_table(self):
        cat = self.catalog()
        with self.assertRaises(NoSuchTableError):
            cat.load_table('main.does_not_exist')

    def test_drop_and_rename(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('r')
        schema = pa.schema([('id', pa.int64())])
        cat.create_table('r.a', schema)
        cat.rename_table('r.a', 'r.b')
        self.assertEqual(cat.list_tables('r'), [('r', 'b')])
        cat.drop_table('r.b')
        self.assertEqual(cat.list_tables('r'), [])

    def test_partitioned(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('p')
        schema = Schema(NestedField(1, 'id', LongType(), required=False),
                        NestedField(2, 'ts', TimestampType(), required=False),
                        NestedField(3, 'cat', StringType(), required=False))
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name='ts_day'),
                             PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name='cat'))
        table = cat.create_table('p.pt', schema, partition_spec=spec)
        data = pa.table({'id': [1, 2, 3, 4],
                         'ts': [datetime.datetime(2021, 3, 4, 1), datetime.datetime(2021, 3, 4, 2),
                                datetime.datetime(2021, 3, 5, 1), datetime.datetime(2021, 3, 6)],
                         'cat': ['a', 'a', 'b', 'b']}, schema=table.schema().as_arrow())
        table.append(data)
        table = cat.load_table('p.pt')
        self.assertEqual(len(table.spec().fields), 2)
        self.assertEqual(table.scan().to_arrow().sort_by('id')['id'].to_pylist(), [1, 2, 3, 4])
        self.assertEqual(table.scan(row_filter="cat = 'a'").to_arrow().sort_by('id')['id'].to_pylist(), [1, 2])
        self.assertEqual(self.shell.sql('SELECT count(*) FROM dl.p.pt'), ['4'])
        # the partition values are the ones of the specification
        values = self.shell.sql("SELECT partition_value FROM __ducklake_metadata_dl.ducklake_file_partition_value "
                                "ORDER BY data_file_id, partition_key_index")
        self.assertEqual(values, ['18690', 'a', '18691', 'b', '18692', 'b'])

    def test_overwrite_and_delete(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('od')
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        table = cat.create_table('od.t', schema)
        table.append(pa.table({'id': [1, 2, 3], 'name': ['a', 'b', 'c']}, schema=schema))
        table.delete('id = 2')
        self.assertEqual(self.shell.sql('SELECT * FROM dl.od.t ORDER BY id'), ['1,a', '3,c'])
        table.append(pa.table({'id': [4], 'name': ['d']}, schema=schema))
        table.overwrite(pa.table({'id': [10], 'name': ['z']}, schema=schema), overwrite_filter='id = 1')
        table = cat.load_table('od.t')
        self.assertEqual(table.scan().to_arrow().sort_by('id')['id'].to_pylist(), [3, 4, 10])
        self.assertEqual(self.shell.sql('SELECT id FROM dl.od.t ORDER BY id'), ['3', '4', '10'])

    def test_schema_evolution(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('e')
        schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        table = cat.create_table('e.t', schema)
        table.append(pa.table({'id': [1, 2], 'name': ['a', 'b']}, schema=schema))
        with table.update_schema() as update:
            update.add_column('extra', DoubleType())
            update.rename_column('name', 'nm')
            update.update_column('id', LongType())
        table = cat.load_table('e.t')
        self.assertEqual([f.name for f in table.schema().fields], ['id', 'nm', 'extra'])
        self.assertEqual(str(table.schema().fields[0].field_type), 'long')
        table.append(pa.table({'id': pa.array([3], pa.int64()), 'nm': ['c'], 'extra': [1.5]},
                              schema=table.schema().as_arrow()))
        table = cat.load_table('e.t')
        self.assertEqual(table.scan().to_arrow().sort_by('id').to_pydict(),
                         {'id': [1, 2, 3], 'nm': ['a', 'b', 'c'], 'extra': [None, None, 1.5]})
        self.assertEqual(self.shell.sql('SELECT * FROM dl.e.t ORDER BY id'), ['1,a,NULL', '2,b,NULL', '3,c,1.5'])
        with table.update_schema() as update:
            update.delete_column('extra')
        table = cat.load_table('e.t')
        self.assertEqual([f.name for f in table.schema().fields], ['id', 'nm'])

    def test_properties(self):
        cat = self.catalog()
        cat.create_namespace('pp', {'owner': 'me'})
        self.assertEqual(cat.load_namespace_properties('pp')['owner'], 'me')
        summary = cat.update_namespace_properties('pp', removals={'owner', 'missing'}, updates={'k': 'v'})
        self.assertEqual(summary.removed, ['owner'])
        self.assertEqual(summary.missing, ['missing'])
        self.assertEqual(cat.load_namespace_properties('pp')['k'], 'v')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('pp.t', schema, properties={'a': 'b'})
        self.assertEqual(table.properties, {'a': 'b'})
        with table.transaction() as txn:
            txn.set_properties({'c': 'd'})
            txn.remove_properties('a')
        self.assertEqual(cat.load_table('pp.t').properties, {'c': 'd'})

    def test_tags_and_branches(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('tags')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('tags.t', schema)
        table.append(pa.table({'id': [1]}, schema=schema))
        table.append(pa.table({'id': [2]}, schema=schema))
        table = cat.load_table('tags.t')
        snapshots = table.snapshots()
        self.assertEqual(len(snapshots), 2)
        table.manage_snapshots().create_tag(snapshots[0].snapshot_id, 'v1').commit()
        table = cat.load_table('tags.t')
        self.assertEqual(table.metadata.refs['v1'].snapshot_id, snapshots[0].snapshot_id)
        self.assertEqual(table.scan(snapshot_id=snapshots[0].snapshot_id).to_arrow()['id'].to_pylist(), [1])
        table.manage_snapshots().remove_tag('v1').commit()
        self.assertNotIn('v1', cat.load_table('tags.t').metadata.refs)

    def test_nested_namespaces(self):
        cat = self.catalog()
        cat.create_namespace(('a',))
        cat.create_namespace(('a', 'b'))
        self.assertEqual(cat.list_namespaces(('a',)), [('a', 'b')])
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table(('a', 'b', 't'), schema)
        table.append(pa.table({'id': [1]}, schema=schema))
        self.assertEqual(cat.load_table(('a', 'b', 't')).scan().to_arrow()['id'].to_pylist(), [1])
        self.assertEqual(self.shell.sql('SELECT * FROM dl.a.b.t'), ['1'])

    def test_create_table_transaction(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('g')
        schema = pa.schema([('id', pa.int64()), ('name', pa.string())])
        with cat.create_table_transaction('g.staged', schema) as txn:
            txn.append(pa.table({'id': [1, 2], 'name': ['a', 'b']}, schema=schema))
        self.assertEqual(cat.load_table('g.staged').scan().to_arrow().sort_by('id')['id'].to_pylist(), [1, 2])

    def test_register_table(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('g')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('g.orig', schema)
        table.append(pa.table({'id': [5, 6]}, schema=schema))
        table = cat.load_table('g.orig')
        registered = cat.register_table('g.reg', table.metadata_location)
        self.assertEqual(registered.scan().to_arrow().sort_by('id')['id'].to_pylist(), [5, 6])
        self.assertEqual(self.shell.sql('SELECT id FROM dl.g.reg ORDER BY id'), ['5', '6'])


    def test_sort_order(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('so')
        schema = Schema(NestedField(1, 'id', LongType(), required=False), NestedField(2, 'v', StringType(), required=False))
        order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.DESC,
                                    null_order=NullOrder.NULLS_LAST))
        table = cat.create_table('so.t', schema, sort_order=order)
        self.assertEqual(len(table.sort_order().fields), 1)
        self.assertEqual(table.sort_order().fields[0].direction, SortDirection.DESC)
        self.assertEqual(self.shell.sql("SELECT expression, sort_direction FROM __ducklake_metadata_dl.ducklake_sort_expression"),
                         ['v,DESC'])
        with table.update_sort_order() as update:
            update.asc("id", IdentityTransform())
        table = cat.load_table('so.t')
        self.assertEqual([f.source_id for f in table.sort_order().fields], [1])
        self.assertEqual(self.shell.sql("SELECT expression, sort_direction FROM __ducklake_metadata_dl.ducklake_sort_expression "
                                        "WHERE sort_id = (SELECT max(sort_id) FROM __ducklake_metadata_dl.ducklake_sort_info)"),
                         ['id,ASC'])


    def test_rollback(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('rb')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('rb.t', schema)
        table.append(pa.table({'id': [1, 2]}, schema=schema))
        first = cat.load_table('rb.t').current_snapshot().snapshot_id
        table.append(pa.table({'id': [3]}, schema=schema))
        table.delete('id = 1')
        self.assertEqual(self.shell.sql('SELECT id FROM dl.rb.t ORDER BY id'), ['2', '3'])
        cat.load_table('rb.t').manage_snapshots().rollback_to_snapshot(first).commit()
        self.assertEqual(self.shell.sql('SELECT id FROM dl.rb.t ORDER BY id'), ['1', '2'])
        self.assertEqual(cat.load_table('rb.t').scan().to_arrow().sort_by('id')['id'].to_pylist(), [1, 2])

    def test_client_metadata_is_deleted(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('cm')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('cm.t', schema)
        table.append(pa.table({'id': [1, 2]}, schema=schema))
        table.append(pa.table({'id': [3]}, schema=schema))
        table.delete('id = 1')
        # the manifests and manifest lists of the client are gone, the data files are not
        self.assertEqual(self.avro_files('cm'), [])
        self.assertEqual(cat.load_table('cm.t').scan().to_arrow().sort_by('id')['id'].to_pylist(), [2, 3])
        self.assertEqual(self.shell.sql('SELECT id FROM dl.cm.t ORDER BY id'), ['2', '3'])

    def test_branch_writes_are_not_supported(self):
        cat = self.catalog()
        cat.create_namespace_if_not_exists('rb')
        schema = pa.schema([('id', pa.int64())])
        table = cat.create_table('rb.branch', schema)
        table.append(pa.table({'id': [1]}, schema=schema))
        with self.assertRaises(Exception) as raised:
            cat.load_table('rb.branch').append(pa.table({'id': [2]}, schema=schema), branch='dev')
        self.assertIn('main branch', str(raised.exception))
        self.assertEqual(self.shell.sql('SELECT id FROM dl.rb.branch'), ['1'])


@requires_pyiceberg
class KeepClientMetadataTest(IcebergTestCase):
    serve_options = ', cleanup_client_metadata := false'

    def test_client_metadata_is_kept(self):
        cat = pyiceberg_catalog(self.uri)
        cat.create_namespace_if_not_exists('cm')
        schema = pa.schema([('id', pa.int64())])
        cat.create_table('cm.kept', schema).append(pa.table({'id': [1]}, schema=schema))
        self.assertGreaterEqual(len(self.avro_files('cm')), 2)


@requires_pyiceberg
class DeletionVectorTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0, WRITE_DELETION_VECTORS true'

    def test_deletion_vectors_are_position_deletes(self):
        self.shell.sql('CREATE TABLE dl.main.dv(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.dv SELECT i FROM range(100) t(i)')
        self.shell.sql('DELETE FROM dl.main.dv WHERE i % 10 = 0')
        self.assertEqual(self.shell.sql("SELECT count(*) FROM __ducklake_metadata_dl.ducklake_delete_file WHERE format = 'puffin'"), ['1'])
        table = pyiceberg_catalog(self.uri).load_table('main.dv')
        values = table.scan().to_arrow()['i'].to_pylist()
        self.assertEqual(len(values), 90)
        self.assertNotIn(10, values)


@requires_pyiceberg
class TokenTest(IcebergTestCase):
    token = 'secret'

    def test_token(self):
        cat = pyiceberg_catalog(self.uri, token='secret')
        cat.create_namespace('tok')
        self.assertIn(('tok',), cat.list_namespaces())

    def test_oauth_credentials(self):
        # PyIceberg gets a token from the OAuth endpoint with the credentials
        from pyiceberg.catalog import load_catalog
        cat = load_catalog('rest', **{'uri': self.uri, 'warehouse': 'dl', 'credential': 'client:secret',
                                      'oauth2-server-uri': self.uri + '/v1/oauth/tokens'})
        self.assertIn(('main',), cat.list_namespaces())

    def test_wrong_token(self):
        from pyiceberg.exceptions import RESTError, UnauthorizedError
        with self.assertRaises((UnauthorizedError, RESTError)):
            pyiceberg_catalog(self.uri, token='wrong').list_namespaces()


if __name__ == '__main__':
    unittest.main()
