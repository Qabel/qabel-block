import pytest
import json
from tornado.options import options
from glinda.testing import services

API_QUOTA = '/api/v0/quota'
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
    size = len(body)
    auth_server.add_response(services.Request('POST', auth_path), services.Response(204))
    auth_server.add_response(services.Request('DELETE', auth_path), services.Response(204))
    auth_server.add_response(services.Request('POST', API_QUOTA), services.Response(204))
    response = yield http_client.fetch(path, method='POST', body=body, headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert len(auth_request.body) == 0
    assert auth_request.headers['Authorization'] == headers['Authorization']
    assert auth_request.headers['APISECRET'] == options.apisecret
    assert response.code == 204

    log_request = auth_server.get_request(API_QUOTA)
    body = json.loads(log_request.body.decode('UTF-8'))
    assert body == {'prefix': prefix, 'file_path': file_name, 'action': 'store', 'size': size}
    assert log_request.headers['Authorization'] == headers['Authorization']
    assert log_request.headers['APISECRET'] == options.apisecret

    response = yield http_client.fetch(path, method='DELETE', headers=headers, raise_error=False)
    assert response.code == 204


@pytest.mark.gen_test
def test_auth_backend_called_for_get(app, http_client, path, auth_path, headers, auth_server):
    auth_server.add_response(services.Request('GET', auth_path), services.Response(204))
    auth_server.add_response(services.Request('POST', '/api/v0/quota'), services.Response(204))
    yield http_client.fetch(path, method='GET', headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert len(auth_request.body) == 0
    assert auth_request.headers['Authorization'] == headers['Authorization']


@pytest.mark.gen_test
def test_auth_backend_called_for_post_and_denied(
        app, cache, http_client, path, auth_path, headers, auth_server):
    auth_server.add_response(services.Request('POST', auth_path), services.Response(403))
    response = yield http_client.fetch(path, method='POST', headers=headers, body=b'Dummy',
                                       raise_error=False)
    assert response.code == 403


@pytest.mark.gen_test
def test_log_is_called(app, http_client, path, headers, mock_log):
    body = b'Dummy'
    size = len(body)
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    log = mock_log.log
    assert len(log) == 1
    assert log[0][0] == headers['Authorization']
    assert log[0][2] == 'store'
    assert log[0][3] == size
    yield http_client.fetch(path, method='GET', headers=headers)
    assert len(log) == 2
    assert log[1][2] == 'get'
    assert log[1][3] == size
    yield http_client.fetch(path, method='DELETE', headers=headers)
    assert len(log) == 3
    assert log[2][2] == 'store'
    assert log[2][3] == -size


@pytest.mark.gen_test
def test_send_log(app, http_client, path, auth_path, headers, auth_server, file_path):
    body = b'Dummy'
    size = len(body)
    _, prefix, file_name = file_path.split('/')
    auth_server.add_response(services.Request('POST', API_QUOTA), services.Response(204))
    auth_server.add_response(services.Request('POST', auth_path), services.Response(204))
    auth_server.add_response(services.Request('GET', auth_path), services.Response(204))
    auth_server.add_response(services.Request('DELETE', auth_path), services.Response(204))
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    yield http_client.fetch(path, method='GET', headers=headers)
    yield http_client.fetch(path, method='DELETE', headers=headers)
    (store, s_body), (get, g_body), (delete, d_body) = (
        (request, json.loads(request.body.decode('UTF-8')))
        for request in auth_server.get_requests_for(API_QUOTA))

    for request in (store, get, delete):
        assert request.headers['Authorization'] == headers['Authorization']
        assert request.headers['APISECRET'] == options.apisecret

    for body in (s_body, g_body, d_body):
        assert body['prefix'] == prefix
        assert body['file_path'] == file_name

    assert s_body['size'] == size
    assert g_body['size'] == size
    assert d_body['size'] == -size

    assert s_body['action'] == 'store'
    assert g_body['action'] == 'get'
    assert d_body['action'] == 'store'


@pytest.mark.gen_test
def test_log_handles_overwrites(app, http_client, path, headers, mock_log):
    body = b'Dummy'
    body_larger = b'DummyDummy'
    size = len(body)
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    yield http_client.fetch(path, method='POST', body=body_larger, headers=headers)
    yield http_client.fetch(path, method='POST', body=body, headers=headers)
    sizes = [entry[3] for entry in mock_log.log]
    assert sizes == [size, size, -size]


@pytest.mark.gen_test
def test_no_long_path(backend, http_client, path, headers):
    response = yield http_client.fetch(path+'/blocks/foobar', method='POST', body=b'', headers=headers)
    assert response.code == 204
