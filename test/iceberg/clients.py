"""Imports of the Iceberg clients that the tests use, which are skipped if they are not available."""
import os
import sys
import unittest

for extra_path in reversed(os.environ.get('ICEBERG_PYTHONPATH', '').split(os.pathsep)):
    if extra_path:
        sys.path.insert(0, extra_path)

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None

try:
    import pyarrow as pa
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    HAVE_PYICEBERG = True
except ImportError:  # pragma: no cover
    HAVE_PYICEBERG = False

_HAVE_DUCKDB_ICEBERG = None


def have_duckdb_iceberg():
    """Whether the Python DuckDB can load the Iceberg extension"""
    global _HAVE_DUCKDB_ICEBERG
    if _HAVE_DUCKDB_ICEBERG is None:
        try:
            con = duckdb.connect()
            con.execute('INSTALL iceberg')
            con.execute('LOAD iceberg')
            _HAVE_DUCKDB_ICEBERG = True
        except Exception:
            _HAVE_DUCKDB_ICEBERG = False
    return _HAVE_DUCKDB_ICEBERG


def duckdb_iceberg_connection(uri, warehouse='dl', token=None):
    """A DuckDB connection with the DuckLake attached as an Iceberg REST catalog as `ice`"""
    con = duckdb.connect()
    con.execute('LOAD iceberg')
    auth = "AUTHORIZATION_TYPE 'none'" if token is None else "TOKEN '%s'" % token
    con.execute("ATTACH '%s' AS ice (TYPE ICEBERG, ENDPOINT '%s', %s)" % (warehouse, uri, auth))
    return con


def pyiceberg_catalog(uri, warehouse='dl', token=None):
    props = {'uri': uri, 'warehouse': warehouse}
    if token:
        props['token'] = token
    return load_catalog('rest', **props)


def requires_duckdb_iceberg(test):
    return unittest.skipUnless(duckdb is not None and have_duckdb_iceberg(),
                               'the Iceberg extension of DuckDB is not available')(test)


def requires_pyiceberg(test):
    return unittest.skipUnless(HAVE_PYICEBERG, 'PyIceberg is not available')(test)
