"""The metadata files of the Iceberg API can be virtual: the server generates them when they are requested."""
import json
import unittest
import urllib.error
import urllib.request

from harness import IcebergTestCase


def fetch(url, headers=None, method='GET'):
    request = urllib.request.Request(url, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


class VirtualFilesTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'
    serve_options = ', virtual_files := true'

    def load(self, name):
        status, _, body = fetch('%s/v1/dl/namespaces/main/tables/%s' % (self.uri, name))
        self.assertEqual(status, 200)
        return json.loads(body)

    def test_urls_are_addresses_of_the_server(self):
        self.shell.sql('CREATE TABLE dl.main.vf(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.vf SELECT i FROM range(10) t(i)')
        self.shell.sql('DELETE FROM dl.main.vf WHERE i = 3')
        table = self.load('vf')
        prefix = self.uri + '/v1/dl/metadata/'
        self.assertTrue(table['metadata-location'].startswith(prefix))
        manifest_list = table['metadata']['snapshots'][-1]['manifest-list']
        self.assertTrue(manifest_list.startswith(prefix))
        # the file is generated from the catalog when it is requested
        status, headers, content = fetch(manifest_list)
        self.assertEqual(status, 200)
        self.assertEqual(content[:4], b'Obj\x01')
        # the same file every time, with a length for HEAD requests and with ranges
        self.assertEqual(fetch(manifest_list)[2], content)
        status, headers, empty = fetch(manifest_list, method='HEAD')
        self.assertEqual(int(headers['Content-Length']), len(content))
        status, headers, part = fetch(manifest_list, {'Range': 'bytes=4-19'})
        self.assertEqual(status, 206)
        self.assertEqual(part, content[4:20])
        self.assertEqual(headers['Content-Range'], 'bytes 4-19/%d' % len(content))
        status, headers, tail = fetch(manifest_list, {'Range': 'bytes=-8'})
        self.assertEqual(tail, content[-8:])
        # the metadata file is the metadata of the response
        status, _, metadata = fetch(table['metadata-location'])
        self.assertEqual(json.loads(metadata)['table-uuid'], table['metadata']['table-uuid'])
        # nothing was written
        self.assertEqual(self.shell.sql("SELECT count(*) FROM glob('%s/**/*.avro')" % self.shell.metadata_path), ['0'])

    def test_files_of_old_snapshots(self):
        self.shell.sql('CREATE TABLE dl.main.vf_history(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.vf_history VALUES (1)')
        first = self.load('vf_history')['metadata']['snapshots'][0]['manifest-list']
        _, _, before = fetch(first)
        self.shell.sql('INSERT INTO dl.main.vf_history VALUES (2)')
        self.shell.sql('DELETE FROM dl.main.vf_history WHERE i = 1')
        # a file of an earlier snapshot does not change
        self.assertEqual(fetch(first)[2], before)
        self.assertEqual(len(self.load('vf_history')['metadata']['snapshots']), 3)

    def test_unknown_files(self):
        self.shell.sql('CREATE TABLE dl.main.vf_unknown(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.vf_unknown VALUES (1)')
        url = self.load('vf_unknown')['metadata']['snapshots'][0]['manifest-list']
        for bad in [url.replace('snap-', 'snip-'), url.replace('/metadata/t', '/metadata/t9'),
                    url.rsplit('/', 1)[0] + '/m-999-0-m.avro', url.replace('/metadata/t', '/metadata/x')]:
            status, _, body = fetch(bad)
            self.assertEqual(status, 404, bad)
        status, _, _ = fetch(self.uri + '/v1/other/metadata/t1/snap-1-none.avro')
        self.assertEqual(status, 404)


class VirtualFilesTokenTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'
    serve_options = ', virtual_files := true'
    token = 'secret'

    def test_signed_urls(self):
        self.shell.sql('CREATE TABLE dl.main.signed(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.signed VALUES (1)')
        status, _, body = fetch('%s/v1/dl/namespaces/main/tables/signed' % self.uri,
                                {'Authorization': 'Bearer secret'})
        self.assertEqual(status, 200)
        url = json.loads(body)['metadata']['snapshots'][0]['manifest-list']
        self.assertIn('?sig=', url)
        # the clients read the files without the token: the signature is enough
        self.assertEqual(fetch(url)[0], 200)
        self.assertEqual(fetch(url.split('?')[0])[0], 401)
        self.assertEqual(fetch(url.split('?')[0] + '?sig=0000')[0], 403)
        self.assertEqual(fetch(url.split('?')[0], {'Authorization': 'Bearer secret'})[0], 200)


if __name__ == '__main__':
    unittest.main()
