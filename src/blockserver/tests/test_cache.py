import pytest

from blockserver.backend.transfer import StorageObject
from blockserver.backend.auth import User

with_etag = StorageObject('foo', 'bar', 'etag', size=10)
without_etag = with_etag._replace(etag=None, size=None)  # type: StorageObject


def test_storage_cache_basics(cache):
    with pytest.raises(KeyError):
        cache.get_storage(without_etag)
    with pytest.raises(ValueError):
        cache.set_storage(without_etag)
    cache.set_storage(with_etag)
    assert cache.get_storage(without_etag) == with_etag


AUTH_TOKEN = 'MAGICFAIRYTALE'


def test_auth_cache_basics(cache):
    with pytest.raises(KeyError):
        cache.get_auth(AUTH_TOKEN)
    with pytest.raises(ValueError):
        cache.set_auth(AUTH_TOKEN, None)
    user = User(0, True, 123, 456)
    cache.set_auth(AUTH_TOKEN, user)
    assert cache.get_auth(AUTH_TOKEN) == user
    user_2 = User(2, False, 456, 123789)
    cache.set_auth(AUTH_TOKEN, user_2)
    assert cache.get_auth(AUTH_TOKEN) == user_2


def test_auth_cache_old_data(cache):
    cache._set('some_token', user_id=3, is_active=True)
    with pytest.raises(KeyError):
        cache.get_auth('some_token')
