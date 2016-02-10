import pytest

from blockserver.backend.transfer import StorageObject

with_etag = StorageObject('foo', 'bar', 'etag', size=10)
without_etag = with_etag._replace(etag=None, size=None)  # type: StorageObject


def test_cache_basics(cache):
    with pytest.raises(KeyError):
        cache.get(without_etag)
    with pytest.raises(ValueError):
        cache.set(without_etag)
    cache.set(with_etag)
    assert cache.get(without_etag) == with_etag



