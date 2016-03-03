import pytest

from blockserver.backend.transfer import StorageObject

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
    cache.set_auth(AUTH_TOKEN, 1)
    assert cache.get_auth(AUTH_TOKEN) == 1
    cache.set_auth(AUTH_TOKEN, 2)
    assert cache.get_auth(AUTH_TOKEN) == 2

