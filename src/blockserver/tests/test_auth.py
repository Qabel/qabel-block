from unittest.mock import Mock

import pytest
from glinda.testing import services
from pytest import fixture

from blockserver.backend import auth
from blockserver.backend.auth import DummyAuth, Auth
from conftest import make_coroutine


@pytest.mark.gen_test
def test_dummy_auth():
    assert (yield DummyAuth.auth("Token RandomStuff")) == 0
    assert (yield DummyAuth.auth("Foobar")) == 0
    assert (yield DummyAuth.auth(None)) == 0


@fixture
def mock_auth(mocker):
    mock = Mock()
    mocker.patch('blockserver.backend.auth.AccountingServerAuth.request',
                 new=make_coroutine(mock))
    return mock


@fixture
def mock_cache(mocker):
    return mocker.patch('blockserver.backend.auth.CacheAuth.auth')


@pytest.mark.gen_test
def test_auth(mock_auth, mock_cache):
    mock_cache.return_value = None
    token = Mock()
    ret = Mock()
    mock_auth.return_value = ret
    assert (yield Auth.auth(token)) is ret
    mock_auth.assert_called_once_with(token)
    mock_cache.assert_called_once_with(token)


@pytest.mark.gen_test
def test_auth_cache(mock_auth, mock_cache):
    token = Mock()
    ret = Mock()
    mock_cache.return_value = ret
    assert (yield Auth.auth(token)) is ret
    mock_cache.assert_called_once_with(token)
    mock_auth.assert_not_called()


@pytest.mark.gen_test
def test_user_not_found(mock_auth):
    mock_auth.return_value = None
    uid = Mock()
    with pytest.raises(auth.UserNotFound):
        yield Auth.auth(uid)


@pytest.mark.gen_test
def test_auth_request(mocker):
    fetch_mock = Mock()
    mocker.patch('blockserver.backend.auth.AccountingServerAuth.send_request',
                 new=make_coroutine(fetch_mock))
    token = 'Token foobar'
    ret = Mock()
    ret.content = '{"user_id": 0}'
    fetch_mock.return_value = ret
    response = yield auth.AccountingServerAuth.request(token)
    assert response == 0
    fetch_mock.assert_called_once_with('{"auth": "Token foobar"}')


@pytest.mark.gen_test
def test_auth_send_request(app, http_client, auth_server):
    path = '/api/v0/auth/'
    body = b'{"user_id": 0}'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(200, body=body))
    response = yield auth.AccountingServerAuth.send_request(b'foobar')
    assert response.code == 200
    assert response.body == body


@pytest.mark.gen_test
def test_auth_send_request_not_found(app, http_client, auth_server):
    path = '/api/v0/auth/'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(404))
    with pytest.raises(auth.UserNotFound):
        yield auth.AccountingServerAuth.send_request(b'foobar')


@pytest.mark.gen_test
def test_auth_send_request_error_propagation(app, http_client, auth_server):
    path = '/api/v0/auth/'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(500))
    with pytest.raises(auth.AuthError):
        yield auth.AccountingServerAuth.send_request(b'foobar')
