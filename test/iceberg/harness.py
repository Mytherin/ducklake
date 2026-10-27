"""Helpers to run the interop tests of the Iceberg REST API of DuckLake.

The tests start a DuckDB shell with the DuckLake extension, which attaches a DuckLake and serves it through the Iceberg
REST API. Clients (the Iceberg extension of DuckDB, PyIceberg) connect to it. The shell can also run SQL on the
DuckLake itself, to compare what both sides see.

Environment:
  DUCKLAKE_SHELL   the DuckDB shell with the DuckLake extension (default: build/reldebug/duckdb)
  ICEBERG_PYTHONPATH   directories (separated like PATH) with the duckdb, pyiceberg, pyarrow and pyspark Python packages
                       (optional)
  ICEBERG_SPARK_JAR    the Iceberg runtime jar for Spark (optional, for the Spark tests)
"""
import os
import queue
import shutil
import subprocess
import tempfile
import threading
import unittest
import uuid

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def shell_path():
    return os.environ.get('DUCKLAKE_SHELL', os.path.join(ROOT, 'build', 'reldebug', 'duckdb'))


class DuckLakeShell:
    """A DuckDB shell that serves a DuckLake through the Iceberg REST API."""

    def __init__(self, directory, port, token=None, attach_options='', serve_options=''):
        self.directory = directory
        self.port = port
        self.data_path = os.path.join(directory, 'data')
        self.metadata_path = os.path.join(directory, 'iceberg_metadata')
        self.proc = subprocess.Popen([shell_path(), '-no-agent', '-csv', '-noheader'], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.lines = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.counter = 0
        self.sql("ATTACH 'ducklake:%s' AS dl (DATA_PATH '%s'%s)" % (
            os.path.join(directory, 'meta.ducklake'), self.data_path, attach_options))
        options = ", metadata_path := '%s'" % self.metadata_path
        if token:
            options += ", token := '%s'" % token
        self.sql("SELECT listen_uri FROM ducklake_iceberg_serve('dl', port := %d%s%s)" % (port, options, serve_options))
        self.uri = 'http://localhost:%d' % port

    def _read(self):
        for line in self.proc.stdout:
            self.lines.put(line.rstrip('\n'))
        self.lines.put(None)

    def sql(self, query, timeout=120):
        """Runs SQL in the shell, returns the output lines. Raises if the output has an error."""
        self.counter += 1
        marker = '__END_%d__' % self.counter
        self.proc.stdin.write(query.rstrip().rstrip(';') + ';\n.print %s\n' % marker)
        self.proc.stdin.flush()
        out = []
        while True:
            line = self.lines.get(timeout=timeout)
            if line is None:
                raise RuntimeError('the shell died: %s' % '\n'.join(out))
            if line == marker:
                break
            out.append(line)
        text = '\n'.join(out)
        if 'Error' in text and ('Exception' in text or 'Error:' in text):
            raise RuntimeError('%s\n%s' % (query, text))
        return out

    def close(self):
        try:
            self.proc.stdin.write('.exit\n')
            self.proc.stdin.flush()
            self.proc.wait(timeout=20)
        except Exception:
            self.proc.kill()


class IcebergTestCase(unittest.TestCase):
    """A test with a DuckLake that is served over the Iceberg REST API."""
    port_counter = 18300
    token = None
    attach_options = ''
    serve_options = ''

    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.mkdtemp(prefix='ducklake_iceberg_test_')
        IcebergTestCase.port_counter += 1
        cls.shell = DuckLakeShell(cls.directory, IcebergTestCase.port_counter, cls.token, cls.attach_options,
                                  cls.serve_options)
        cls.uri = cls.shell.uri

    @classmethod
    def tearDownClass(cls):
        cls.shell.close()
        shutil.rmtree(cls.directory, ignore_errors=True)

    def avro_files(self, schema):
        """The Avro files in the data path of a schema, i.e. the manifests that clients wrote"""
        found = []
        for root, _, files in os.walk(os.path.join(self.shell.data_path, schema)):
            found += [os.path.join(root, name) for name in files if name.endswith('.avro')]
        return found

    def unique(self, prefix='t'):
        return '%s_%s' % (prefix, uuid.uuid4().hex[:8])
