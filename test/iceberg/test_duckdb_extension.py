"""The Iceberg extension of DuckDB reads and writes DuckLake through the Iceberg REST API."""
import os
import unittest

from clients import duckdb_iceberg_connection, requires_duckdb_iceberg
from harness import IcebergTestCase


@requires_duckdb_iceberg
class DuckDBExtensionTest(IcebergTestCase):
    # keep the rows out of the catalog, so that the data is in files
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'

    def connect(self):
        return duckdb_iceberg_connection(self.uri)

    def test_read_ducklake_data(self):
        self.shell.sql("CREATE TABLE dl.main.types(b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, "
                       "f FLOAT, d DOUBLE, dec DECIMAL(10,2), dt DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ, "
                       "s VARCHAR, u UUID, bl BLOB, l INTEGER[], st STRUCT(a INTEGER, b VARCHAR), "
                       "m MAP(VARCHAR, INTEGER))")
        self.shell.sql("INSERT INTO dl.main.types VALUES (true, 1, 2, 3, 4, 1.5, 2.5, 12345.67, '2020-01-02', "
                       "'01:02:03', '2020-01-02 03:04:05', '2020-01-02 03:04:05+00', 'str', "
                       "'12345678-1234-5678-1234-567812345678', 'ab'::BLOB, [1,2,3], {'a': 1, 'b': 'x'}, "
                       "MAP {'k': 1}), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                       "NULL, NULL, NULL, NULL, NULL)")
        con = self.connect()
        rows = con.execute('SELECT * FROM ice.main.types ORDER BY i32 NULLS LAST').fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][:5], (True, 1, 2, 3, 4))
        self.assertEqual(rows[0][12], 'str')
        self.assertEqual(rows[0][15], [1, 2, 3])
        self.assertEqual(rows[0][16], {'a': 1, 'b': 'x'})
        self.assertEqual(rows[0][17], {'k': 1})
        self.assertEqual(rows[1], (None,) * 18)

    def test_deletes_and_inlined_data(self):
        self.shell.sql("CREATE TABLE dl.main.d(i INTEGER, s VARCHAR)")
        self.shell.sql("INSERT INTO dl.main.d SELECT i, 'v' || i FROM range(10) t(i)")
        self.shell.sql("DELETE FROM dl.main.d WHERE i IN (2, 3)")
        self.assertEqual(self.connect().execute('SELECT count(*), sum(i) FROM ice.main.d').fetchall(), [(8, 40)])
        # the rows are inlined in the catalog
        self.shell.sql("CALL dl.set_option('data_inlining_row_limit', 100)")
        self.shell.sql("CREATE TABLE dl.main.inl(i INTEGER)")
        self.shell.sql("INSERT INTO dl.main.inl VALUES (1), (2), (3)")
        self.shell.sql("DELETE FROM dl.main.inl WHERE i = 2")
        self.assertEqual(self.connect().execute('SELECT i FROM ice.main.inl ORDER BY i').fetchall(), [(1,), (3,)])

    def test_write_through_iceberg(self):
        con = self.connect()
        con.execute('CREATE SCHEMA ice.w')
        con.execute('CREATE TABLE ice.w.t(id INTEGER, v VARCHAR)')
        con.execute("INSERT INTO ice.w.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.w.t ORDER BY id'), ['1,a', '2,b', '3,c'])
        con.execute('DELETE FROM ice.w.t WHERE id = 2')
        self.assertEqual(self.shell.sql('SELECT * FROM dl.w.t ORDER BY id'), ['1,a', '3,c'])
        con.execute("UPDATE ice.w.t SET v = 'zz' WHERE id = 3")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.w.t ORDER BY id'), ['1,a', '3,zz'])
        con.execute('ALTER TABLE ice.w.t ADD COLUMN n INTEGER')
        con.execute("INSERT INTO ice.w.t VALUES (4, 'd', 7)")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.w.t ORDER BY id'), ['1,a,NULL', '3,zz,NULL', '4,d,7'])
        self.assertEqual(con.execute('SELECT * FROM ice.w.t ORDER BY id').fetchall(),
                         [(1, 'a', None), (3, 'zz', None), (4, 'd', 7)])
        con.execute('DROP TABLE ice.w.t')
        self.assertEqual(self.shell.sql("SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'w' AND table_name = 't'"), ['0'])

    def test_client_metadata_is_deleted(self):
        con = self.connect()
        con.execute('CREATE SCHEMA ice.cm')
        con.execute('CREATE TABLE ice.cm.t(i INTEGER)')
        con.execute('INSERT INTO ice.cm.t VALUES (1), (2)')
        con.execute('DELETE FROM ice.cm.t WHERE i = 1')
        self.assertEqual(self.avro_files('cm'), [])
        self.assertEqual(self.shell.sql('SELECT * FROM dl.cm.t'), ['2'])

    def test_schema_evolution(self):
        self.shell.sql('CREATE TABLE dl.main.x(a INTEGER, b INTEGER)')
        self.shell.sql('INSERT INTO dl.main.x VALUES (1, 2)')
        self.shell.sql('ALTER TABLE dl.main.x ADD COLUMN c VARCHAR')
        self.shell.sql("INSERT INTO dl.main.x VALUES (3, 4, 'z')")
        self.shell.sql('ALTER TABLE dl.main.x RENAME COLUMN b TO bb')
        self.shell.sql('ALTER TABLE dl.main.x DROP COLUMN a')
        self.assertEqual(self.connect().execute('SELECT * FROM ice.main.x ORDER BY bb').fetchall(),
                         [(2, None), (4, 'z')])


@requires_duckdb_iceberg
class DuckDBExtensionVirtualFilesTest(DuckDBExtensionTest):
    """The same tests, with metadata files that the server generates when they are requested"""
    serve_options = ', virtual_files := true'

    def test_no_files_are_written(self):
        self.shell.sql('CREATE TABLE dl.main.virtual_t(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.virtual_t SELECT i FROM range(10) t(i)')
        self.shell.sql('DELETE FROM dl.main.virtual_t WHERE i < 3')
        self.assertEqual(self.connect().execute('SELECT count(*) FROM ice.main.virtual_t').fetchall(), [(7,)])
        written = []
        for root, _, files in os.walk(self.shell.metadata_path):
            written += [name for name in files if name.endswith(('.avro', '.json'))]
        self.assertEqual(written, [])


@requires_duckdb_iceberg
class DuckDBExtensionTokenTest(IcebergTestCase):
    token = 'secret'

    def test_token(self):
        con = duckdb_iceberg_connection(self.uri, token='secret')
        con.execute('CREATE SCHEMA ice.tok')
        con.execute('CREATE TABLE ice.tok.t(i INTEGER)')
        con.execute('INSERT INTO ice.tok.t VALUES (42)')
        self.assertEqual(self.shell.sql('SELECT * FROM dl.tok.t'), ['42'])
        with self.assertRaises(Exception):
            duckdb_iceberg_connection(self.uri, token='wrong')


if __name__ == '__main__':
    unittest.main()
