from __future__ import annotations
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

    def _set_user(self, key, user):
        self._set(key,
                  user_id=str(user.user_id).encode(),
                  is_active=str(int(user.is_active)).encode(),
                  quota=str(int(user.quota)).encode(),
                  traffic_quota=str(int(user.traffic_quota)).encode())
        self._set_expire(key, AUTH_CACHE_EXPIRE)

    def set_auth(self, authentication_token: str, user: User):
        if not isinstance(user, User):
            raise ValueError('Need a User object')
        self._set_user(authentication_token, user)
        self._set_user('user-%d' % user.user_id, user)

    def set_user(self, user: User):
        if not isinstance(user, User):
            raise ValueError('Need a User object')
        self._set_user('user-%d' % user.user_id, user)

    def _get_user(self, key: str) -> User:
        user_info = self._get(key, 'user_id', 'is_active', 'quota', 'traffic_quota')
        user_id, is_active, quota, traffic_quota = user_info
        if any(attr is None for attr in user_info):
            raise KeyError('Element not found')
        return User(user_id=int(user_id.decode('utf-8')),
                    is_active=(is_active == b'1'),
                    quota=int(quota.decode()),
                    traffic_quota=int(traffic_quota.decode()))

    def get_auth(self, authentication_token: str) -> User:
        return self._get_user(authentication_token)

    def get_user(self, user_id: int) -> User:
        return self._get_user('user-%d' % user_id)

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


class RedisCache(AbstractCache):
    """
    Cache ETags from StorageObjects in redis
    """

    def __init__(self, **redis_kwargs):
        self._cache = redis.StrictRedis(**redis_kwargs)

    def _set_expire(self, key, time_to_live):
        self._cache.expire(key, time_to_live)

    def flush(self):
        self._cache.flushdb()

    def _set(self, key, **values):
        return self._cache.hmset(key, values)

    def _get(self, key, *keys):
        return self._cache.hmget(key, keys)
