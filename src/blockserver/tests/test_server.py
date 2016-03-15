import json

import pytest
from tornado.options import options
from glinda.testing import services
from unittest.mock import call

@pytest.mark.gen_test
def test_not_found(backend, http_client, path):
    response = yield http_client.fetch(path, raise_error=False)
    assert response.code == 404


@pytest.mark.gen_test
def test_no_body(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'', headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_no_auth_get(http_client, path, cache):
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
def test_normal_cycle(backend, http_client, path, headers):
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
def test_etag_set_on_get(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    etag = response.headers['ETag']
    assert len(etag) > 0
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
    assert response.code == 200


@pytest.mark.gen_test
def test_auth_backend_called(app, cache, http_client, path, auth_path, headers, auth_server, file_path):
    body = b'Dummy'
    _, prefix, file_name = file_path.split('/')
    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(200, body=b'{"user_id": 0, "active":true}'))
    response = yield http_client.fetch(path, method='POST', body=body, headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert auth_request.headers['APISECRET'] == options.apisecret
    assert response.code == 204


@pytest.mark.gen_test
def test_no_long_path(backend, http_client, path, headers):
    response = yield http_client.fetch(path+'/blocks/foobar', method='POST', body=b'', headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_prefix_auth(app, http_client, prefix_path, user_id, pg_db, auth_server, auth_path):
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
    prefixes = pg_db.get_prefixes(user_id)
    parsed_response = json.loads(response.body.decode('utf-8'))
    assert parsed_response == {'prefixes': prefixes}


@pytest.mark.gen_test
def test_save_log(app, mocker, http_client, path, auth_path, headers,
                  auth_server, file_path, prefix):
    quota_log = mocker.patch(
        'blockserver.backend.database.PostgresUserDatabase.update_size')
    trafifc_log = mocker.patch(
        'blockserver.backend.database.PostgresUserDatabase.update_traffic')
    body = b'Dummy'
    size = len(body)
    _, prefix_name, file_name = file_path.split('/')
    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(200, body=b'{"user_id": 0, "active":true}'))
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    yield http_client.fetch(path, method='GET', headers=headers)
    yield http_client.fetch(path, method='DELETE', headers=headers)

    expected_quota = [call(prefix, size), call(prefix, -size)]
    assert quota_log.call_args_list == expected_quota

    expected_traffic = [call(prefix, size)]
    assert trafifc_log.call_args_list == expected_traffic


@pytest.mark.gen_test
def test_normal_cycle_with_quota_changes(backend, http_client, path, quota_path, headers):
    size = 0
    async def check_quota():
        response = await http_client.fetch(quota_path, method='GET', headers=headers)
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


@pytest.mark.gen_test
def test_quota_reached(backend, http_client, block_path, headers, pg_db, user_id):
    pg_db.set_quota(user_id, 0)
    body = b'Dummy'
    response = yield http_client.fetch(block_path, method='POST', body=body, headers=headers)
    assert response.code == 402
    assert response.body == 'Quota reached'


@pytest.mark.gen_test
def test_quota_reached_but_meta_files_allowed(backend, http_client, path, headers, pg_db, user_id):
    pg_db.set_quota(user_id, 0)
    body = b'Dummy'
    response = yield http_client.fetch(path, method='POST', body=body, headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_quota_reached_meta_files_size_limit(backend, http_client, path, headers, pg_db, user_id):
    assert False


@pytest.mark.gen_test
def test_quota_delete_and_download(backend, http_client, path, headers, pg_db, user_id):
    assert False
