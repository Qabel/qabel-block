import pytest
from blockserver import server
from tornado.options import options
from glinda.testing import services

from blockserver.backends import dummy
from blockserver.backends import s3

options.debug = True
options.dummy_auth = True


@pytest.fixture
def app():
    return server.make_app(
            transfer_cls=lambda: dummy.Transfer if options.dummy else s3.Transfer,
            auth_callback=lambda: server.dummy_auth if options.dummy_auth else server.check_auth,
            log_callback=lambda: None, debug=False)


@pytest.mark.gen_test
def test_not_found(backend, http_client, path):
    response = yield http_client.fetch(path, raise_error=False)
    assert response.code == 404


@pytest.mark.gen_test
def test_no_body(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'', headers=headers)
    assert response.code == 204


@pytest.mark.gen_test
def test_no_auth(http_client, path):
    response = yield http_client.fetch(path, method='GET', raise_error=False)
    assert response.code == 403


@pytest.mark.gen_test
def test_normal_cycle(backend, http_client, path, headers):
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers)
    assert response.code == 204
    response = yield http_client.fetch(path, method='GET', headers=headers)
    assert response.body == b'Dummy'
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
def test_auth_backend_called(app, http_client, path, auth_path, headers, auth_server):
    auth_server.add_response(services.Request('POST', auth_path), services.Response(204))
    response = yield http_client.fetch(path, method='POST', body=b'Dummy', headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert len(auth_request.body) == 0
    assert auth_request.headers['Authorization'] == headers['Authorization']
    assert response.code == 204


@pytest.mark.gen_test
def test_auth_backend_called_for_get(app, http_client, path, auth_path, headers, auth_server):
    auth_server.add_response(services.Request('GET', auth_path), services.Response(204))
    yield http_client.fetch(path, method='GET', headers=headers,
                                       raise_error=False)
    auth_request = auth_server.get_request(auth_path)
    assert len(auth_request.body) == 0
    assert auth_request.headers['Authorization'] == headers['Authorization']


@pytest.mark.gen_test
def test_auth_backend_called_for_post_and_denied(
        app, http_client, path, auth_path, headers, auth_server):
    auth_server.add_response(services.Request('POST', auth_path), services.Response(403))
    response = yield http_client.fetch(path, method='POST', headers=headers, body=b'Dummy',
                                       raise_error=False)
    assert response.code == 403
