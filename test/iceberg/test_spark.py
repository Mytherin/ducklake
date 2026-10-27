"""Spark with the Iceberg runtime reads and writes DuckLake through the Iceberg REST API.

Needs PySpark and the Iceberg runtime jar for Spark (ICEBERG_SPARK_JAR, e.g. iceberg-spark-runtime-3.5_2.12-1.10.0.jar).
"""
import os
import unittest

import clients  # noqa: F401 (puts ICEBERG_PYTHONPATH on the path)
from harness import IcebergTestCase

try:
    from pyspark.sql import SparkSession
    HAVE_SPARK = True
except ImportError:  # pragma: no cover
    HAVE_SPARK = False

SPARK_JAR = os.environ.get('ICEBERG_SPARK_JAR')


@unittest.skipUnless(HAVE_SPARK and SPARK_JAR and os.path.exists(SPARK_JAR),
                     'PySpark or the Iceberg runtime jar (ICEBERG_SPARK_JAR) is not available')
class SparkTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = (SparkSession.builder.master('local[2]').appName('ducklake_iceberg')
                     .config('spark.jars', SPARK_JAR)
                     .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
                     .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
                     .config('spark.sql.catalog.lake.type', 'rest')
                     .config('spark.sql.catalog.lake.uri', cls.uri)
                     .config('spark.sql.catalog.lake.warehouse', 'dl')
                     .config('spark.sql.catalog.lake.cache-enabled', 'false')
                     .config('spark.ui.enabled', 'false')
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        super().tearDownClass()

    def sql(self, query):
        return [tuple(row) for row in self.spark.sql(query).collect()]

    def test_read_ducklake_data(self):
        self.shell.sql('CREATE TABLE dl.main.fromdl(i INTEGER, s VARCHAR)')
        self.shell.sql("INSERT INTO dl.main.fromdl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        self.shell.sql('DELETE FROM dl.main.fromdl WHERE i = 2')
        self.assertIn(('main',), self.sql('SHOW NAMESPACES IN lake'))
        self.assertEqual(self.sql('SELECT * FROM lake.main.fromdl ORDER BY i'), [(1, 'a'), (3, 'c')])
        # the history of the table
        self.assertEqual(len(self.sql('SELECT * FROM lake.main.fromdl.snapshots')), 2)
        # the data file and the delete file
        self.assertEqual(len(self.sql('SELECT * FROM lake.main.fromdl.files')), 2)

    def test_write(self):
        self.sql('CREATE NAMESPACE IF NOT EXISTS lake.sp')
        self.sql('CREATE TABLE lake.sp.t (id bigint, name string) USING iceberg')
        self.sql("INSERT INTO lake.sp.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.sp.t ORDER BY id'), ['1,a', '2,b', '3,c'])
        self.sql('DELETE FROM lake.sp.t WHERE id = 2')
        self.assertEqual(self.shell.sql('SELECT * FROM dl.sp.t ORDER BY id'), ['1,a', '3,c'])
        self.sql("UPDATE lake.sp.t SET name = 'zz' WHERE id = 3")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.sp.t ORDER BY id'), ['1,a', '3,zz'])
        self.sql('ALTER TABLE lake.sp.t ADD COLUMN extra double')
        self.sql("INSERT INTO lake.sp.t VALUES (4, 'd', 1.5)")
        self.assertEqual(self.shell.sql('SELECT * FROM dl.sp.t ORDER BY id'), ['1,a,NULL', '3,zz,NULL', '4,d,1.5'])
        self.sql("MERGE INTO lake.sp.t USING (SELECT 4 AS id, 'merged' AS name) s ON t.id = s.id "
                 'WHEN MATCHED THEN UPDATE SET name = s.name')
        self.assertEqual(self.shell.sql('SELECT * FROM dl.sp.t ORDER BY id'), ['1,a,NULL', '3,zz,NULL', '4,merged,1.5'])
        self.assertEqual(self.sql('SELECT id FROM lake.sp.t ORDER BY id'), [(1,), (3,), (4,)])

    def test_partitioned(self):
        self.sql('CREATE NAMESPACE IF NOT EXISTS lake.sp')
        self.sql('CREATE TABLE lake.sp.p (id bigint, ts timestamp, cat string) USING iceberg '
                 'PARTITIONED BY (days(ts), cat, bucket(4, id))')
        self.sql("INSERT INTO lake.sp.p VALUES (1, timestamp '2021-03-04 05:06:07', 'a'), "
                 "(2, timestamp '2021-03-05 05:06:07', 'b'), (3, timestamp '2021-03-05 06:06:07', 'b')")
        self.assertEqual(self.shell.sql('SELECT id, cat FROM dl.sp.p ORDER BY id'), ['1,a', '2,b', '3,b'])
        self.assertEqual(self.sql("SELECT id FROM lake.sp.p WHERE cat = 'b' ORDER BY id"), [(2,), (3,)])
        self.assertEqual(len(self.sql('SELECT * FROM lake.sp.p.files')), 3)
        # DuckLake and Spark agree on the partition values
        self.shell.sql("INSERT INTO dl.sp.p VALUES (4, '2021-03-06 00:00:00', 'c')")
        self.assertEqual(self.sql("SELECT id FROM lake.sp.p WHERE ts >= timestamp '2021-03-06 00:00:00' "
                                  "ORDER BY id"), [(4,)])

    def test_time_travel(self):
        self.sql('CREATE NAMESPACE IF NOT EXISTS lake.sp')
        self.sql('CREATE TABLE lake.sp.tt (id bigint) USING iceberg')
        self.sql('INSERT INTO lake.sp.tt VALUES (1)')
        first = self.sql('SELECT snapshot_id FROM lake.sp.tt.snapshots ORDER BY committed_at')[0][0]
        self.sql('INSERT INTO lake.sp.tt VALUES (2)')
        self.assertEqual(self.sql('SELECT id FROM lake.sp.tt ORDER BY id'), [(1,), (2,)])
        self.assertEqual(self.sql('SELECT id FROM lake.sp.tt VERSION AS OF %d' % first), [(1,)])
        self.sql("ALTER TABLE lake.sp.tt CREATE TAG v1 AS OF VERSION %d" % first)
        self.assertEqual(self.sql("SELECT id FROM lake.sp.tt VERSION AS OF 'v1'"), [(1,)])

    def test_properties_and_rename(self):
        self.sql('CREATE NAMESPACE IF NOT EXISTS lake.sp')
        self.sql("CREATE TABLE lake.sp.props (id bigint) USING iceberg TBLPROPERTIES ('team'='me')")
        self.sql("ALTER TABLE lake.sp.props SET TBLPROPERTIES ('k'='v')")
        properties = dict((row[0], row[1]) for row in self.sql('SHOW TBLPROPERTIES lake.sp.props'))
        self.assertEqual(properties['team'], 'me')
        self.assertEqual(properties['k'], 'v')
        self.sql('ALTER TABLE lake.sp.props RENAME TO sp.renamed')
        self.assertEqual(self.sql('SELECT count(*) FROM lake.sp.renamed'), [(0,)])
        self.sql('DROP TABLE lake.sp.renamed')


if __name__ == '__main__':
    unittest.main()
