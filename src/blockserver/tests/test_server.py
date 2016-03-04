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
def test_no_auth(http_client, path, cache):
    cache.flush()
    response = yield http_client.fetch(path, method='GET', raise_error=False)
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
def test_create_prefix():
    pytest.fail('Not implemented')


@pytest.mark.gen_test
def test_upload_denied():
    pytest.fail("Not implemented")


@pytest.mark.gen_test
def test_upload_successful():
    pytest.fail("Not implemented")


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
    yield http_client.fetch(path, method='GET', headers=headers)
    yield http_client.fetch(path, method='DELETE', headers=headers)

    expected_quota = [call(prefix, size), call(prefix, -size)]
    assert quota_log.call_args_list == expected_quota

    expected_traffic = [call(prefix, size)]
    assert trafifc_log.call_args_list == expected_traffic


@pytest.mark.gen_test
def test_log_handles_overwrites(app, mocker, auth_server, auth_path,
                                http_client, path, headers, prefix):
    body = b'Dummy'
    body_larger = b'DummyDummy'
    size = len(body)
    quota_log = mocker.patch(
        'blockserver.backend.database.PostgresUserDatabase.update_size')

    auth_server.add_response(services.Request('POST', auth_path),
                             services.Response(200, body=b'{"user_id": 0, "active":true}'))
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    yield http_client.fetch(path, method='POST', body=body_larger, headers=headers)
    yield http_client.fetch(path, method='POST', body=body, headers=headers)

    expected_quota = [call(prefix, size), call(prefix, size)]
    assert quota_log.call_args_list == expected_quota
