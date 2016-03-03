from typing import Dict, List

from abc import abstractmethod, ABC

import redis
from blockserver.backend.transfer import StorageObject, file_key

AUTH_CACHE_EXPIRE = 60


class AbstractCache(ABC):

    STORAGE_PREFIX = 'storage_'
    AUTH_PREFIX = 'auth_'

    def set_storage(self, storage_object: StorageObject):
        """
        Saves the etag and size of a StorageObject
        """
        key = self._storage_key(storage_object)
        if storage_object.etag is None:
            raise ValueError('No etag set in StorageObject')
        if storage_object.size is None:
            raise ValueError('No size set in StorageObject')
        self._set(key, etag=storage_object.etag.encode(), size=storage_object.size)

    def get_storage(self, storage_object: StorageObject) -> StorageObject:
        """
        Gets the etag and size of a StorageObject according to the cache

        Raises a KeyError if the etag is not known
        """
        key = self._storage_key(storage_object)
        etag, size = self._get(key, 'etag', 'size')
        if etag is None or size is None:
            raise KeyError("Element not found")
        etag = etag.decode('UTF-8')
        size = int(size)
        return storage_object._replace(etag=etag, size=size)

    def set_auth(self, authentication_token: str, value: int):
        if not isinstance(value, int):
            raise ValueError('Need an integer value')
        self._set_single(authentication_token, str(value).encode('utf-8'), AUTH_CACHE_EXPIRE)

    def get_auth(self, authentication_token: str) -> int:
        user_id = self._get_single(authentication_token)
        if user_id is None:
            raise KeyError('Element not found')
        return int(user_id)

    def _storage_key(self, storage_object):
        return self.STORAGE_PREFIX + file_key(storage_object)

    def _auth_key(self, authentication_token, prefix, method):
        return self.AUTH_PREFIX + '_'.join((authentication_token, prefix, method))

    @abstractmethod
    def _set(self, key: str, **values: Dict[str, str]):
        pass

    @abstractmethod
    def _get(self, key: str, *keys: List[str]) -> Dict[str, str]:
        pass

    @abstractmethod
    def _set_single(self, key, param, time_to_live):
        pass

    @abstractmethod
    def _get_single(self, key):
        pass


class DummyCache(AbstractCache):
    """
    Local cache implemented with dict
    """
    def __init__(self):
        self._cache = {}

    def _set(self, key, **values):
        converted_values = {k: v.encode('UTF-8') if isinstance(v, str) else v
                            for k, v in values.items()}
        self._cache[key] = converted_values

    def _get(self, key, *keys):
        try:
            values = self._cache[key]
        except KeyError:
            return [None] * len(keys)
        else:
            return [values[k] for k in keys]

    def _set_single(self, key, param, time_to_live):
        self._cache[key] = param

    def _get_single(self, key):
        return self._cache[key]

    def flush(self):
        self._cache = {}


class RedisCache(AbstractCache):
    """
    Cache ETags from StorageObjects in redis
    """

    def _set_single(self, key, param, time_to_live):
        self._cache.setex(key, time_to_live, param)

    def _get_single(self, key):
        return self._cache.get(key)

    def __init__(self, host, port):
        self._cache = redis.StrictRedis(host=host, port=port)

    def flush(self):
        self._cache.flushdb()

    def _set(self, key, **values):
        return self._cache.hmset(key, values)

    def _get(self, key, *keys):
        return self._cache.hmget(key, keys)

