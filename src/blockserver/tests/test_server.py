import json
from functools import partial

import pytest
from prometheus_client import REGISTRY
from tornado.httpclient import HTTPError, HTTPRequest
from tornado.options import options
from glinda.testing import services
from unittest.mock import call

from tornado.websocket import websocket_connect

from blockserver.backend.auth import DummyAuth


def stat_by_name(stat_name):
    return partial(REGISTRY.get_sample_value, stat_name)


@pytest.mark.gen_test
def test_dummy_auth_arbitrary_post(backend, http_client, base_url, headers):
    url = base_url + '/api/v0/files/some_arbitrary_prefix/some_file'
    body = b'Dummy'
    response = yield http_client.fetch(url, method='POST', body=body, headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_no_body(backend, http_client, path, headers, temp_check):
    with temp_check:
        response = yield http_client.fetch(path, method='POST', body=b'', headers=headers,
                                           raise_error=False)
    assert response.code == 204, response.body.decode('utf-8')


@pytest.mark.gen_test
def test_no_auth_get(backend, http_client, path, cache):
    cache.flush()
    response = yield http_client.fetch(path, method='GET', raise_error=False)
    assert response.code == 404


@pytest.mark.gen_test
def test_no_auth_post(http_client, path, cache):
    cache.flush()
    response = yield http_client.fetch(path, method='POST', body=b'Dummy',
                                       raise_error=False)
    assert response.code == 403


@pytest.mark.gen_test
def test_normal_cycle(backend, http_client, path, headers, temp_check):
    temp_check.forget()
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204
    response = yield http_client.fetch(path, method='GET', headers=headers)
    assert response.body == b'Dummy'
    assert int(response.headers['Content-Length']) == len(b'Dummy')
    response = yield http_client.fetch(path, method='DELETE', headers=headers)
    assert response.code == 204
    # deleting again works fine
    response = yield http_client.fetch(path, method='DELETE', headers=headers)
    assert response.code == 204
    response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 404
    temp_check.assert_clean()


@pytest.mark.gen_test
def test_not_found(backend, http_client, base_url, path, headers):
    response = yield http_client.fetch(path, headers=headers, raise_error=False)
    assert response.code == 404


@pytest.mark.gen_test
def test_etag_set_on_post(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204
    etag = response.headers.get('ETag', None)
    assert etag


@pytest.mark.gen_test
def test_etag_post_if_match_aborts_on_non_existing_file(backend, http_client, path, headers):
    headers['If-Match'] = 'something i just made up'
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers, raise_error=False)
    assert response.code == 412, response.body
    assert 'ETag' not in response.headers


@pytest.mark.gen_test
def test_etag_post_if_match_aborts_on_mismatch(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    headers['If-Match'] = response.headers['ETag'] + 'something i just made up'
    response = yield http_client.fetch(path, method='POST', body=b'OtherDummy', headers=headers, raise_error=False)
    assert response.code == 412
    del headers['If-Match']
    headers['If-None-Match'] = response.headers['Etag']
    response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 304


@pytest.mark.gen_test
def test_etag_post_if_match(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    headers['If-Match'] = response.headers['ETag']
    response = yield http_client.fetch(path, method='POST', body=b'OtherDummy', headers=headers)
    assert response.code == 204
    stored_etag = response.headers['ETag']
    response = yield http_client.fetch(path, method='GET', headers=headers)
    assert response.body == b'OtherDummy'
    del headers['If-Match']
    headers['If-None-Match'] = stored_etag
    response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 304


@pytest.mark.gen_test
def test_etag_set_on_get(backend, http_client, path, headers, temp_check):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    etag = response.headers['ETag']
    assert len(etag) > 0
    with temp_check.no_new_temp():
        response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 200
    assert response.headers['ETag'] == etag


@pytest.mark.gen_test
def test_etag_not_modified(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204
    etag = response.headers['ETag']
    headers['If-None-Match'] = etag
    response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 304
    assert len(response.body) == 0


@pytest.mark.gen_test
def test_etag_modified(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204
    headers['If-None-Match'] = "anothertag"
    response = yield http_client.fetch(path, method='GET', headers=headers)
    assert response.code == 200, response.body == b'Dummy'


@pytest.mark.gen_test
def test_auth_backend_called(backend, cache, http_client, path, auth_path, headers, auth_server, file_path):
    body = b'Dummy'
    _, prefix, file_name = file_path.split('/')
    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(200, body=b'{"user_id": 0, "active": true,'
                                                         b'"block_quota": 123, "monthly_traffic_quota": 789}'))
    response = yield http_client.fetch(path, method='POST', body=body, headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert auth_request.headers['APISECRET'] == options.apisecret
    assert response.code == 204, response.body


@pytest.mark.gen_test
def test_no_long_path(backend, http_client, path, headers):
    response = yield http_client.fetch(path + '/blocks/foobar', method='POST', body=b'', headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_prefix_auth(backend, http_client, prefix_path, user_id, pg_db, auth_server, auth_path):
    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(404))
    response = yield http_client.fetch(prefix_path, method='POST', raise_error=False,
                                       allow_nonstandard_methods=True)
    assert response.code == 403
    response = yield http_client.fetch(prefix_path, method='POST', raise_error=False,
                                       headers={'Authorization': 'Token Foobar'},
                                       allow_nonstandard_methods=True)
    assert response.code == 403


@pytest.mark.gen_test
def test_create_prefixes(app, http_client, prefix_path, user_id, headers, pg_db):
    response = yield http_client.fetch(prefix_path, method='POST', headers=headers,
                                       allow_nonstandard_methods=True)
    assert 'application/json' in response.headers['Content-Type']
    prefixes = pg_db.get_prefixes(user_id)
    parsed_response = json.loads(response.body.decode('utf-8'))
    assert parsed_response == {'prefix': prefixes[0]}


@pytest.mark.gen_test
def test_get_prefixes(app, http_client, prefix_path, user_id, headers, pg_db):
    pg_db.create_prefix(user_id)
    pg_db.create_prefix(user_id)
    pg_db.create_prefix(user_id)
    response = yield http_client.fetch(prefix_path, method='GET', headers=headers,
                                       allow_nonstandard_methods=True)
    assert 'application/json' in response.headers['Content-Type']
    prefixes = pg_db.get_prefixes(user_id)
    parsed_response = json.loads(response.body.decode('utf-8'))
    assert parsed_response == {'prefixes': prefixes}


@pytest.mark.gen_test
def test_log_and_monitoring(backend, mocker, http_client, path, auth_path, headers,
                            auth_server, file_path, prefix):
    mon_traffic = stat_by_name('block_traffic_by_request_sum')
    mon_quota = stat_by_name('block_quota_by_request_sum')

    traffic_before = mon_traffic() or 0
    quota_before = mon_quota({'type': 'increase'}) or 0
    quota_dec_before = mon_quota({'type': 'decrease'}) or 0

    quota_log = mocker.patch(
        'blockserver.backend.database.PostgresUserDatabase.update_size')
    trafifc_log = mocker.patch(
        'blockserver.backend.database.PostgresUserDatabase.update_traffic')
    body = b'Dummy'
    size = len(body)
    _, prefix_name, file_name = file_path.split('/')
    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(200, body=b'{"user_id": 0, "active": true,'
                                                         b'"block_quota": 123, "monthly_traffic_quota": 789}'))
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    quota = mon_quota({'type': 'increase'})
    assert quota - quota_before == size
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    quota = mon_quota({'type': 'increase'})
    assert quota - quota_before == size
    yield http_client.fetch(path, method='GET', headers=headers)
    yield http_client.fetch(path, method='DELETE', headers=headers)

    assert mon_traffic() - traffic_before == size

    assert (mon_quota({'type': 'decrease'}) - quota_dec_before) == size

    expected_quota = [call(prefix, size), call(prefix, -size)]
    assert quota_log.call_args_list == expected_quota

    expected_traffic = [call(prefix, size)]
    assert trafifc_log.call_args_list == expected_traffic


@pytest.mark.gen_test
def test_normal_cycle_with_quota_changes(backend, http_client, path, quota_path, headers, temp_check):
    temp_check.forget()
    size = 0
    async def check_quota():
        response = await http_client.fetch(quota_path, method='GET', headers=headers)
        assert 'application/json' in response.headers['Content-Type']
        data = json.loads(response.body.decode('utf-8'))
        assert data['size'] == size
    yield check_quota()
    body = b'Dummy'
    response = yield http_client.fetch(path, method='POST', body=body, headers=headers)
    assert response.code == 204
    size += len(body)
    yield check_quota()
    response = yield http_client.fetch(path, method='GET', headers=headers)
    assert response.body == body
    assert int(response.headers['Content-Length']) == len(body)
    yield check_quota()
    response = yield http_client.fetch(path, method='DELETE', headers=headers)
    assert response.code == 204
    size = 0
    yield check_quota()
    # deleting again works fine
    response = yield http_client.fetch(path, method='DELETE', headers=headers)
    assert response.code == 204
    yield check_quota()
    response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
    assert response.code == 404
    yield check_quota()
    temp_check.assert_clean()


@pytest.mark.gen_test
def test_quota_reached_and_upload_denied(backend, http_client, block_path, headers, pg_db, user_id, temp_check,
                                         monkeypatch):
    monkeypatch.setattr(DummyAuth, 'QUOTA', 0)
    body = b'Dummy'
    with temp_check:
        response = yield http_client.fetch(block_path, method='POST', body=body, headers=headers, raise_error=False)
    assert response.code == 402
    assert b'Quota reached' in response.body


@pytest.mark.gen_test
def test_quota_reached_but_meta_files_allowed(backend, http_client, path, headers, pg_db, user_id, temp_check,
                                              monkeypatch):
    body = b'Dummy'
    with temp_check:
        yield http_client.fetch(path, method='POST', body=body, headers=headers)
        monkeypatch.setattr(DummyAuth, 'QUOTA', 0)
        response = yield http_client.fetch(path, method='POST', body=body, headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_quota_reached_meta_files_size_limit(backend, http_client, path, headers, pg_db, user_id, temp_check,
                                             monkeypatch):
    monkeypatch.setattr(DummyAuth, 'QUOTA', 0)
    body = b'+' * 151 * 1024
    with temp_check:
        response = yield http_client.fetch(path, method='POST', body=body, headers=headers,
                                           raise_error=False)
    assert response.code == 402
    assert b'Quota reached' in response.body


@pytest.mark.gen_test
def test_quota_delete_and_download(backend, http_client, prefix, path, headers, pg_db, user_id, temp_check):
    pg_db.update_traffic(prefix, 100 * 1024**3 + 1)
    body = b'Dummy'
    with temp_check:
        response = yield http_client.fetch(path, method='POST', body=body, headers=headers)
        assert response.code == 204
        response = yield http_client.fetch(path, method='GET', headers=headers, raise_error=False)
        assert response.code == 402
        assert b'Quota reached' in response.body
        response = yield http_client.fetch(path, method='DELETE', headers=headers)
        assert response.code == 204


@pytest.mark.gen_test
def test_denies_too_big_body(app_options, backend, http_client, path, headers, temp_check):
    app_options.max_body_size = 1
    body = b'12'
    with temp_check, pytest.raises(HTTPError):
        yield http_client.fetch(path, method='POST', body=body, headers=headers)


@pytest.mark.gen_test
def test_allows_allowed_body_size(app_options, backend, http_client, path, headers, temp_check):
    app_options.max_body_size = 2
    body = b'12'
    with temp_check:
        response = yield http_client.fetch(path, method='POST', body=body, headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_get_before_post(backend, http_client, base_url):
    url = base_url + '/api/v0/files/randomprefix/foobar'
    response = yield http_client.fetch(url, method='GET', raise_error=False)
    assert response.code == 404


@pytest.mark.gen_test
def test_database_finish_called_in_files(backend, http_client, base_url, mocker):
    finish_db = mocker.patch('blockserver.server.DatabaseMixin.finish_database')
    url = base_url + '/api/v0/files/randomprefix/foobar'
    yield http_client.fetch(url, method='GET', raise_error=False)
    finish_db.assert_called_with()


@pytest.mark.gen_test
def test_database_finish_called_in_prefix(backend, http_client, base_url, mocker, headers):
    finish_db = mocker.patch('blockserver.server.DatabaseMixin.finish_database')
    url = base_url + '/api/v0/prefix/'
    yield http_client.fetch(url, method='GET', headers=headers)
    finish_db.assert_called_with()


@pytest.mark.gen_test
def test_database_finish_called_in_quota(backend, http_client, headers, base_url, mocker):
    finish_db = mocker.patch('blockserver.server.DatabaseMixin.finish_database')
    url = base_url + '/api/v0/quota/'
    response = yield http_client.fetch(url, method='GET', headers=headers, raise_error=True)
    assert response.code == 200, response.body.decode('utf-8')
    finish_db.assert_called_with()


@pytest.fixture()
def websocket_headers():
    return {'Sec-WebSocket-Protocol': 'v0.ws.block.qabel.de'}


@pytest.fixture()
def websocket_file_connector(path, websocket_headers):
    path = path.replace('http://', 'ws://').replace('/api/v0/files', '/api/v0/websocket')
    return websocket_connect(HTTPRequest(url=path, headers=websocket_headers))


@pytest.fixture()
def websocket_prefix_connector(base_url, prefix, websocket_headers):
    def connector(extra_headers=None):
        if extra_headers is None:
            extra_headers = {}
        path = base_url.replace('http://', 'ws://') + '/api/v0/websocket/' + prefix
        headers = websocket_headers
        headers.update(extra_headers)
        return websocket_connect(HTTPRequest(url=path, headers=headers))
    return connector


@pytest.mark.gen_test
def test_ws_post(backend, http_client, path, file_path, websocket_file_connector, headers, prefix):
    conn = yield websocket_file_connector

    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204

    msg = yield conn.read_message()
    assert msg
    msg = json.loads(msg)
    assert msg['operation'] == 'POST'
    assert msg['prefix'] == prefix
    assert file_path.startswith('/')
    assert msg['path'] == file_path[1:]  # file_path is /..., path is just the path, no leading slash
    assert msg['etag'] == response.headers['ETag']


@pytest.mark.gen_test
def test_ws_delete(backend, http_client, path, file_path, websocket_file_connector, headers, prefix):
    # n.b. moving this line around wouldn't matter much -- it's largely undefined when subscribers start to get messages
    # in redis' pubsub mechanism. In this setup it works out nicely, though, just like you'd expect a buffering server to behave.
    conn = yield websocket_file_connector

    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204

    response = yield http_client.fetch(path, method='DELETE', headers=headers)
    assert response.code == 204

    msg = yield conn.read_message()
    msg = json.loads(msg)
    assert msg['operation'] == 'POST'

    msg = yield conn.read_message()
    msg = json.loads(msg)
    assert msg['operation'] == 'DELETE'

    assert msg['prefix'] == prefix
    assert file_path.startswith('/')
    assert msg['path'] == file_path[1:]  # file_path is /..., path is just the path, no leading slash
    assert 'etag' not in msg


@pytest.mark.gen_test
def test_ws_not_on_blocks(backend, http_server, path, websocket_headers):
    try:
        path = path.replace('http://', 'ws://').replace('/api/v0/files', '/api/v0/websocket')
        yield websocket_connect(HTTPRequest(url=path + '/blocks/asdf', headers=websocket_headers))
        assert False, 'Did not raise'
    except HTTPError as error:
        assert error.code == 405


@pytest.mark.gen_test
def test_ws_prefix_post(backend, http_client, path, file_path, websocket_prefix_connector, headers, prefix):
    conn = yield websocket_prefix_connector(extra_headers=headers)

    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204

    msg = yield conn.read_message()
    assert msg
    msg = json.loads(msg)
    assert msg['operation'] == 'POST'
    assert msg['prefix'] == prefix
    assert file_path.startswith('/')
    assert msg['path'] == file_path[1:]  # file_path is /..., path is just the path, no leading slash
    assert msg['etag'] == response.headers['ETag']


@pytest.mark.gen_test
def test_ws_prefix_no_authorization(backend, http_server, websocket_prefix_connector):
    try:
        yield websocket_prefix_connector()
        assert False, 'Did not raise'
    except HTTPError as error:
        assert error.code == 403


@pytest.mark.gen_test
def test_ws_prefix_wrong_authorization(backend, http_server, websocket_prefix_connector):
    try:
        yield websocket_prefix_connector(extra_headers={'Authorization': 'Token elite haxx0r'})
        assert False, 'Did not raise'
    except HTTPError as error:
        assert error.code == 403
