"""Requests to the Iceberg REST API of DuckLake that are sent without an Iceberg client."""
import json
import unittest
import urllib.error
import urllib.parse
import urllib.request

from harness import IcebergTestCase


def request(method, url, body=None, token=None, form=False):
    data = None
    headers = {}
    if body is not None:
        data = urllib.parse.urlencode(body).encode() if form else json.dumps(body).encode()
        headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
    if token:
        headers['Authorization'] = 'Bearer ' + token
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            text = response.read().decode()
            return response.status, json.loads(text) if text else None
    except urllib.error.HTTPError as error:
        text = error.read().decode()
        return error.code, json.loads(text) if text else None


class RestApiTest(IcebergTestCase):
    attach_options = ', DATA_INLINING_ROW_LIMIT 0'

    @property
    def base(self):
        return self.uri + '/v1/dl'

    def test_config(self):
        status, config = request('GET', self.uri + '/v1/config?warehouse=dl')
        self.assertEqual(status, 200)
        self.assertEqual(config['overrides']['prefix'], 'dl')
        self.assertIn('POST /v1/{prefix}/namespaces/{namespace}/tables/{table}', config['endpoints'])
        # without a warehouse, the only catalog that is served is used
        status, config = request('GET', self.uri + '/v1/config')
        self.assertEqual(config['overrides']['prefix'], 'dl')
        status, error = request('GET', self.uri + '/v1/config?warehouse=other')
        self.assertEqual(status, 200)

    def test_errors(self):
        status, error = request('GET', self.base + '/namespaces/missing')
        self.assertEqual(status, 404)
        self.assertEqual(error['error']['type'], 'NoSuchNamespaceException')
        self.assertEqual(error['error']['code'], 404)
        status, error = request('GET', self.base + '/namespaces/main/tables/missing')
        self.assertEqual(status, 404)
        self.assertEqual(error['error']['type'], 'NoSuchTableException')
        status, error = request('POST', self.base + '/namespaces', {'namespace': ['main']})
        self.assertEqual(status, 409)
        self.assertEqual(error['error']['type'], 'AlreadyExistsException')
        status, error = request('GET', self.uri + '/v1/unknown/namespaces')
        self.assertEqual(status, 404)
        status, error = request('GET', self.uri + '/v2/nothing')
        self.assertEqual(status, 404)

    def test_namespaces(self):
        status, result = request('POST', self.base + '/namespaces', {'namespace': ['n1'], 'properties': {'a': 'b'}})
        self.assertEqual(status, 200)
        self.assertEqual(result['namespace'], ['n1'])
        self.assertEqual(result['properties']['a'], 'b')
        status, result = request('POST', self.base + '/namespaces', {'namespace': ['n1', 'n2']})
        self.assertEqual(status, 200)
        status, result = request('GET', self.base + '/namespaces?parent=n1')
        self.assertEqual(result['namespaces'], [['n1', 'n2']])
        status, result = request('GET', self.base + '/namespaces/n1%1Fn2')
        self.assertEqual(result['namespace'], ['n1', 'n2'])
        status, result = request('POST', self.base + '/namespaces/n1/properties',
                                 {'removals': ['a', 'missing'], 'updates': {'c': 'd'}})
        self.assertEqual(result, {'updated': ['c'], 'removed': ['a'], 'missing': ['missing']})
        status, error = request('DELETE', self.base + '/namespaces/n1')
        self.assertEqual(status, 409)
        self.assertEqual(error['error']['type'], 'NamespaceNotEmptyException')
        status, _ = request('DELETE', self.base + '/namespaces/n1%1Fn2')
        self.assertEqual(status, 204)
        status, _ = request('DELETE', self.base + '/namespaces/n1')
        self.assertEqual(status, 204)
        status, _ = request('GET', self.base + '/namespaces/n1')
        self.assertEqual(status, 404)

    def create_table(self, namespace, name, fields=None):
        fields = fields or [{'id': 1, 'name': 'id', 'required': False, 'type': 'long'}]
        return request('POST', self.base + '/namespaces/%s/tables' % namespace, {
            'name': name, 'schema': {'type': 'struct', 'schema-id': 0, 'fields': fields}})

    def test_tables(self):
        request('POST', self.base + '/namespaces', {'namespace': ['tt']})
        status, _ = self.create_table('tt', 't1')
        self.assertEqual(status, 200)
        status, result = request('GET', self.base + '/namespaces/tt/tables/t1')
        self.assertEqual(status, 200)
        metadata = result['metadata']
        self.assertEqual(metadata['format-version'], 2)
        self.assertEqual(metadata['current-snapshot-id'], -1)
        self.assertEqual(metadata['schemas'][0]['fields'][0]['name'], 'id')
        self.assertTrue(result['metadata-location'].endswith('.metadata.json'))
        status, error = self.create_table('tt', 't1')
        self.assertEqual(status, 409)
        status, result = request('GET', self.base + '/namespaces/tt/tables')
        self.assertEqual(result['identifiers'], [{'namespace': ['tt'], 'name': 't1'}])
        status, _ = request('POST', self.base + '/tables/rename', {
            'source': {'namespace': ['tt'], 'name': 't1'}, 'destination': {'namespace': ['tt'], 'name': 't2'}})
        self.assertEqual(status, 204)
        status, _ = request('DELETE', self.base + '/namespaces/tt/tables/t1')
        self.assertEqual(status, 404)
        status, _ = request('DELETE', self.base + '/namespaces/tt/tables/t2')
        self.assertEqual(status, 204)

    def test_commit_requirements(self):
        self.create_table('main', 'req')
        status, result = request('GET', self.base + '/namespaces/main/tables/req')
        uuid = result['metadata']['table-uuid']
        url = self.base + '/namespaces/main/tables/req'
        status, _ = request('POST', url, {'requirements': [{'type': 'assert-table-uuid', 'uuid': uuid},
                                                          {'type': 'assert-current-schema-id', 'current-schema-id': 0},
                                                          {'type': 'assert-ref-snapshot-id', 'ref': 'main',
                                                           'snapshot-id': None}], 'updates': []})
        self.assertEqual(status, 200)
        status, error = request('POST', url, {'requirements': [{'type': 'assert-table-uuid', 'uuid': 'wrong'}],
                                              'updates': []})
        self.assertEqual(status, 409)
        self.assertEqual(error['error']['type'], 'CommitFailedException')
        status, error = request('POST', url, {'requirements': [
            {'type': 'assert-ref-snapshot-id', 'ref': 'main', 'snapshot-id': 12345}], 'updates': []})
        self.assertEqual(status, 409)
        status, error = request('POST', url, {'requirements': [], 'updates': [{'action': 'unknown'}]})
        self.assertEqual(status, 400)

    def test_multi_table_commit(self):
        self.create_table('main', 'm1')
        self.create_table('main', 'm2')
        changes = []
        for name in ['m1', 'm2']:
            changes.append({'identifier': {'namespace': ['main'], 'name': name}, 'requirements': [],
                            'updates': [{'action': 'set-properties', 'updates': {'owner': name}}]})
        status, _ = request('POST', self.base + '/transactions/commit', {'table-changes': changes})
        self.assertEqual(status, 204)
        for name in ['m1', 'm2']:
            status, result = request('GET', self.base + '/namespaces/main/tables/' + name)
            self.assertEqual(result['metadata']['properties']['owner'], name)
        # a requirement that fails fails the whole commit
        changes[1]['requirements'] = [{'type': 'assert-table-uuid', 'uuid': 'wrong'}]
        changes[0]['updates'] = [{'action': 'set-properties', 'updates': {'owner': 'changed'}}]
        status, error = request('POST', self.base + '/transactions/commit', {'table-changes': changes})
        self.assertEqual(status, 409)

    def test_views(self):
        self.shell.sql('CREATE TABLE dl.main.base(i INTEGER)')
        self.shell.sql('INSERT INTO dl.main.base VALUES (1), (2), (3)')
        version = {'version-id': 1, 'timestamp-ms': 1, 'schema-id': 0, 'summary': {}, 'default-namespace': ['main'],
                   'representations': [{'type': 'sql', 'sql': 'SELECT i FROM main.base WHERE i > 1',
                                        'dialect': 'duckdb'}]}
        schema = {'type': 'struct', 'schema-id': 0, 'fields': [{'id': 1, 'name': 'i', 'type': 'int',
                                                                'required': False}]}
        status, result = request('POST', self.base + '/namespaces/main/views', {
            'name': 'v', 'schema': schema, 'view-version': version, 'properties': {'p': 'q'}})
        self.assertEqual(status, 200)
        self.assertEqual(result['metadata']['properties'], {'p': 'q'})
        self.assertEqual(self.shell.sql('SELECT * FROM dl.main.v ORDER BY i'), ['2', '3'])
        status, result = request('GET', self.base + '/namespaces/main/views')
        self.assertEqual(result['identifiers'], [{'namespace': ['main'], 'name': 'v'}])
        status, result = request('GET', self.base + '/namespaces/main/views/v')
        self.assertEqual(result['metadata']['versions'][0]['representations'][0]['dialect'], 'duckdb')
        self.assertEqual(result['metadata']['schemas'][0]['fields'][0]['name'], 'i')
        version['version-id'] = 2
        version['representations'][0]['sql'] = 'SELECT i FROM main.base WHERE i > 2'
        status, _ = request('POST', self.base + '/namespaces/main/views/v', {
            'requirements': [], 'updates': [{'action': 'add-view-version', 'view-version': version},
                                            {'action': 'set-current-view-version', 'view-version-id': -1}]})
        self.assertEqual(status, 200)
        self.assertEqual(self.shell.sql('SELECT * FROM dl.main.v'), ['3'])
        status, _ = request('POST', self.base + '/views/rename', {
            'source': {'namespace': ['main'], 'name': 'v'}, 'destination': {'namespace': ['main'], 'name': 'w'}})
        self.assertEqual(status, 204)
        status, _ = request('DELETE', self.base + '/namespaces/main/views/w')
        self.assertEqual(status, 204)
        status, error = request('GET', self.base + '/namespaces/main/views/w')
        self.assertEqual(status, 404)
        self.assertEqual(error['error']['type'], 'NoSuchViewException')


class TokenTest(IcebergTestCase):
    token = 'secret'

    def test_token(self):
        status, error = request('GET', self.uri + '/v1/dl/namespaces')
        self.assertEqual(status, 401)
        self.assertEqual(error['error']['type'], 'NotAuthorizedException')
        status, result = request('GET', self.uri + '/v1/dl/namespaces', token='secret')
        self.assertEqual(status, 200)
        status, error = request('GET', self.uri + '/v1/dl/namespaces', token='wrong')
        self.assertEqual(status, 401)
        # the OAuth endpoint hands out the token for the secret
        status, result = request('POST', self.uri + '/v1/oauth/tokens', {
            'grant_type': 'client_credentials', 'client_id': 'a', 'client_secret': 'secret'}, form=True)
        self.assertEqual(status, 200)
        self.assertEqual(result['access_token'], 'secret')
        status, error = request('POST', self.uri + '/v1/oauth/tokens', {
            'grant_type': 'client_credentials', 'client_id': 'a', 'client_secret': 'wrong'}, form=True)
        self.assertEqual(status, 401)


if __name__ == '__main__':
    unittest.main()
