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


token_and_prefix_get = 'MAGICFAIRYTALE', 'EXAMPLEPREFIX', 'get'
token_and_prefix_post = 'MAGICFAIRYTALE', 'EXAMPLEPREFIX', 'post'
token_and_prefix_delete = 'MAGICFAIRYTALE', 'EXAMPLEPREFIX', 'delete'


def test_auth_cache_basics(cache):
    with pytest.raises(KeyError):
        cache.get_auth(*token_and_prefix_get)
    with pytest.raises(ValueError):
        cache.set_auth(*token_and_prefix_get, None)
    cache.set_auth(*token_and_prefix_get, True)
    assert cache.get_auth(*token_and_prefix_get) == True
    cache.set_auth(*token_and_prefix_get, False)
    assert cache.get_auth(*token_and_prefix_get) == False

