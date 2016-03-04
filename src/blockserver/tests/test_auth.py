from unittest.mock import Mock, sentinel

import pytest
from glinda.testing import services
from pytest import fixture

from blockserver.backend import auth
from blockserver.backend.auth import DummyAuth, Auth
from blockserver.backend.util import User
from conftest import make_coroutine


@pytest.mark.gen_test
def test_dummy_auth():
    cache = Mock()
    user = User(user_id=0, is_active=True)
    assert (yield DummyAuth(cache).auth("Token RandomStuff")) == user
    assert (yield DummyAuth(cache).auth("Foobar")) == user
    assert (yield DummyAuth(cache).auth(None)) == user


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
    mock_cache.side_effect = KeyError
    token = sentinel.token
    ret = sentinel.ret
    mock_auth.return_value = ret
    cache = Mock()
    assert (yield Auth(cache).auth(token)) is ret
    mock_auth.assert_called_once_with(token)
    mock_cache.assert_called_once_with(cache, token)


@pytest.mark.gen_test
def test_auth_cache_called(mock_auth, mock_cache):
    token = sentinel.token
    ret = sentinel.ret
    mock_cache.return_value = ret
    assert (yield Auth(sentinel.cache_backend).auth(token)) is ret
    mock_cache.assert_called_once_with(sentinel.cache_backend, token)
    mock_auth.assert_not_called()


@pytest.mark.gen_test
def test_auth_cache(mock_auth, cache):
    token = sentinel.mock
    ret = auth.User(0, True)
    mock_auth.return_value = ret
    assert (yield Auth(cache).auth(token)) == ret
    mock_auth.assert_called_once_with(token)
    mock_auth.reset_mock()
    assert (yield Auth(cache).auth(token)) == ret
    mock_auth.assert_not_called()


@pytest.mark.gen_test
def test_user_not_found(mock_auth, cache):
    mock_auth.side_effect = auth.UserNotFound
    auth_header = "Token foobar"
    with pytest.raises(auth.UserNotFound):
        yield Auth(cache).auth(auth_header)


@pytest.mark.gen_test
def test_auth_request(mocker):
    fetch_mock = Mock()
    mocker.patch('blockserver.backend.auth.AccountingServerAuth.send_request',
                 new=make_coroutine(fetch_mock))
    token = 'Token foobar'
    ret = sentinel.ret
    ret.body = b'{"user_id": 0, "active": true}'
    fetch_mock.return_value = ret
    response = yield auth.AccountingServerAuth.request(token)
    assert response == auth.User(user_id=0, is_active=True)
    fetch_mock.assert_called_once_with('{"auth": "Token foobar"}')


@pytest.mark.gen_test
def test_auth_send_request(app, http_client, auth_server):
    path = '/api/v0/auth/'
    body = b'{"user_id": 0}'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(200, body=body))
    response = yield auth.AccountingServerAuth.send_request('foobar')
    assert response.code == 200
    assert response.body == body


@pytest.mark.gen_test
def test_auth_send_request_not_found(app, http_client, auth_server):
    path = '/api/v0/auth/'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(404))
    with pytest.raises(auth.UserNotFound):
        yield auth.AccountingServerAuth.send_request('foobar')


@pytest.mark.gen_test
def test_auth_send_request_error_propagation(app, http_client, auth_server):
    path = '/api/v0/auth/'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(500))
    with pytest.raises(auth.AuthError):
        yield auth.AccountingServerAuth.send_request('foobar')


@pytest.mark.gen_test
def test_auth_returns_user_object(app, http_client, auth_server):
    path = '/api/v0/auth/'
    body = b'{"user_id": 0, "active": true}'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(200, body=body))
    user = yield auth.AccountingServerAuth.request('foobar')
    assert isinstance(user, auth.User)
    assert user.user_id == 0
    assert user.is_active


@pytest.mark.gen_test
def test_auth_returns_inactive_user_object(app, http_client, auth_server):
    path = '/api/v0/auth/'
    body = b'{"user_id": 0, "active": false}'
    auth_server.add_response(services.Request('POST', path),
                             services.Response(200, body=body))
    user = yield auth.AccountingServerAuth.request('foobar')
    assert user.user_id == 0
    assert not user.is_active
