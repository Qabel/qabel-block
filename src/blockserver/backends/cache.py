from typing import Tuple

from abc import abstractmethod, ABC

import redis
from blockserver.backends.util import StorageObject, file_key


class AbstractCache(ABC):

    def set(self, storage_object: StorageObject):
        """
        Saves the etag and size of a StorageObject
        """
        key = file_key(storage_object)
        if storage_object.etag is None:
            raise ValueError('No etag set in StorageObject')
        if storage_object.size is None:
            raise ValueError('No size set in StorageObject')
        self._set(key, storage_object.etag.encode(), storage_object.size)

    def get(self, storage_object: StorageObject) -> StorageObject:
        """
        Gets the etag and size of a StorageObject according to the cache

        Raises a KeyError if the etag is not known
        """
        key = file_key(storage_object)
        etag, size = self._get(key)
        if etag is None or size is None:
            raise KeyError("Element not found")
        etag = etag.decode('UTF-8')
        size = int(size)
        return storage_object._replace(etag=etag, size=size)

    @abstractmethod
    def _set(self, key: str, etag: bytes, size: int):
        pass

    @abstractmethod
    def _get(self, key: str) -> Tuple[bytes, int]:
        pass


class DummyCache(AbstractCache):
    """
    Local cache implemented with dict
    """

    def __init__(self):
        self._cache = {}

    def _set(self, key, etag, size):
        self._cache[key] = etag, size

    def _get(self, key):
        return self._cache.get(key, (None, None))


class RedisCache(AbstractCache):
    """
    Cache ETags from StorageObjects in redis
    """

    def __init__(self, host, port):
        self._cache = redis.StrictRedis(host=host, port=port)

    def flush(self):
        self._cache.flushdb()

    def _set(self, key, etag, size):
        return self._cache.hmset(key, {'etag': etag, 'size': size})

    def _get(self, key):
        return self._cache.hmget(key, ('etag', 'size'))

