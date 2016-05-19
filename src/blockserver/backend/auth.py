from tornado.httpclient import AsyncHTTPClient, HTTPError
from blockserver.server import options
from blockserver.backend.cache import AbstractCache
from blockserver.backend.util import User
from blockserver import monitoring as mon

import json


class AuthError(Exception):
    pass


class UserNotFound(AuthError):
    pass


class BypassAuth(AuthError):

    def __init__(self, user):
        self.user = user


class DummyAuth:
    QUOTA = 2 * 1024**3
    TRAFFIC_QUOTA = 20 * 1024**3

    def __init__(self, cache_backend):
        pass

    async def auth(self, auth_header: str) -> User:
        if auth_header == 'Token {}'.format(options.dummy_auth):
            raise BypassAuth(User(user_id=0,
                                  is_active=True,
                                  quota=self.QUOTA,
                                  traffic_quota=self.TRAFFIC_QUOTA))
        else:
            raise UserNotFound()

    async def get_user(self, user_id: int) -> User:
        if user_id == 0:
            return User(user_id=0,
                        is_active=True,
                        quota=self.QUOTA,
                        traffic_quota=self.TRAFFIC_QUOTA)
        else:
            raise UserNotFound


class Auth:

    def __init__(self, cache_backend):
        self.cache_backend = cache_backend

    async def auth(self, auth_header: str) -> User:
        try:
            user = CacheAuth.auth(self.cache_backend, auth_header)
        except KeyError:
            user = await AccountingServerAuth.request_auth(auth_header)
            CacheAuth.set(self.cache_backend, auth_header, user)
            mon.COUNT_AUTH_CACHE_SETS.inc()
        else:
            mon.COUNT_AUTH_CACHE_HITS.inc()
        return user

    async def get_user(self, user_id: int) -> User:
        try:
            user = CacheAuth.get_user(self.cache_backend, user_id)
        except KeyError:
            user = await AccountingServerAuth.request_info(user_id)
            CacheAuth.set_user(self.cache_backend, user)
            mon.COUNT_AUTH_CACHE_SETS.inc()
        else:
            mon.COUNT_AUTH_CACHE_HITS.inc()
        return user


class AccountingServerAuth:

    @staticmethod
    @mon.time(mon.WAIT_FOR_AUTH)
    async def request_auth(auth_header: str) -> User:
        return await AccountingServerAuth.api_request({'auth': auth_header},
                                                      )

    @staticmethod
    @mon.time(mon.WAIT_FOR_AUTH)
    async def request_info(user_id: int) -> User:
        return await AccountingServerAuth.api_request({'user_id': user_id})

    @staticmethod
    async def api_request(request_data):
        request_body = json.dumps(request_data)
        response = await AccountingServerAuth.send_request(request_body)
        try:
            body = json.loads(response.body.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise AuthError(e)

        try:
            return User(user_id=body['user_id'],
                        is_active=body['active'],
                        quota=body['block_quota'],
                        traffic_quota=body['monthly_traffic_quota'])
        except KeyError:
            raise UserNotFound('Invalid response from accounting server')

    @staticmethod
    async def send_request(request_body):
        http_client = AsyncHTTPClient()
        url = AccountingServerAuth.auth_url()
        try:
            return await http_client.fetch(
                url, headers={
                    'APISECRET': AccountingServerAuth.api_secret(),
                    'Content-Type': 'application/json'},
                body=request_body, method='POST')
        except HTTPError as e:
            if e.code == 404:
                raise UserNotFound(request_body)
            else:
                raise AuthError(e.message)

    @staticmethod
    def api_secret():
        return options.apisecret

    @staticmethod
    def auth_url():
        return options.accounting_host + '/api/v0/internal/user/'


class CacheAuth:

    @staticmethod
    def auth(cache_backend: AbstractCache, auth_header: str) -> User:
        return cache_backend.get_auth(auth_header)

    @staticmethod
    def get_user(cache_backend: AbstractCache, user_id: int) -> User:
        return cache_backend.get_user(user_id)

    @staticmethod
    def set(cache_backend: AbstractCache, auth_header: str, user: User):
        return cache_backend.set_auth(auth_header, user)

    @staticmethod
    def set_user(cache_backend: AbstractCache, user: User):
        return cache_backend.set_user(user)
