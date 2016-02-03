import random
import string

import pytest
from tornado.options import options
from blockserver import server

options.debug = True
options.dummy_auth = True
application = server.make_app()


@pytest.fixture
def headers():
    return {'Authorization': 'Token MAGICFARYDUST'}


@pytest.fixture
def path(base_url):
    random_name = ''.join(random.choice(string.ascii_lowercase + string.digits)
                          for _ in range(12))
    return base_url + '/files/test/' + random_name


@pytest.yield_fixture
def backend(request):
    switch_to = (request.param == 'dummy')
    before = options.dummy
    options.dummy = switch_to
    if options.dummy:
        from blockserver.backends import dummy
        dummy.files = {}
    yield
    options.dummy = before


@pytest.fixture
def app():
    return application


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
