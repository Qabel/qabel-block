import redis
from typing import Dict, List

from abc import abstractmethod, ABC
from blockserver.backend.util import User
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

    def set_auth(self, authentication_token: str, user: User):
        if not isinstance(user, User):
            raise ValueError('Need a User object')
        self._set(authentication_token, user_id=str(user.user_id).encode('utf-8'),
                  is_active=str(int(user.is_active)).encode('utf-8'))
        self._set_expire(authentication_token, AUTH_CACHE_EXPIRE)

    def get_auth(self, authentication_token: str) -> int:
        user_info = self._get(authentication_token, 'user_id', 'is_active')
        user_id, is_active = user_info
        if user_id is None:
            raise KeyError('Element not found')
        return User(user_id=int(user_id.decode('utf-8')), is_active=(is_active == b'1'))

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
    def _set_expire(self, key, time_to_live):
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

    def _set_expire(self, key, time_to_live):
        pass

    def flush(self):
        self._cache = {}


class RedisCache(AbstractCache):
    """
    Cache ETags from StorageObjects in redis
    """

    def __init__(self, host, port):
        self._cache = redis.StrictRedis(host=host, port=port)

    def _set_expire(self, key, time_to_live):
        self._cache.expire(key, time_to_live)

    def flush(self):
        self._cache.flushdb()

    def _set(self, key, **values):
        return self._cache.hmset(key, values)

    def _get(self, key, *keys):
        return self._cache.hmget(key, keys)
