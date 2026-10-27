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


if __name__ == '__main__':
    unittest.main()
