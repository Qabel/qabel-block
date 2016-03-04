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


class DummyAuth:

    def __init__(self, cache_backend):
        pass

    async def auth(self, auth_header: str) -> int:
        return 0


class Auth:

    def __init__(self, cache_backend):
        self.cache_backend = cache_backend

    async def auth(self, auth_header: str) -> User:
        try:
            user = CacheAuth.auth(self.cache_backend, auth_header)
        except KeyError:
            user = await AccountingServerAuth.request(auth_header)
            CacheAuth.set(self.cache_backend, auth_header, user)
            mon.COUNT_AUTH_CACHE_SETS.inc()
        else:
            mon.COUNT_AUTH_CACHE_HITS.inc()
        return user


class AccountingServerAuth:

    @staticmethod
    @mon.time(mon.WAIT_FOR_AUTH)
    async def request(auth_header: str) -> User:
        request_body = json.dumps({'auth': auth_header})
        response = await AccountingServerAuth.send_request(request_body)
        try:
            body = json.loads(response.body.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise AuthError(e)

        try:
            return User(user_id=body.get('user_id'), is_active=body.get('active'))
        except KeyError:
            raise UserNotFound('Invalid response from accounting server')

    @staticmethod
    async def send_request(request_body):
        http_client = AsyncHTTPClient()
        url = AccountingServerAuth.auth_url()
        try:
            return await http_client.fetch(
                url, headers={'APISECRET': AccountingServerAuth.api_secret()},
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
        return options.accounting_host + '/api/v0/auth/'


class CacheAuth:

    @staticmethod
    def auth(cache_backend: AbstractCache, auth_header: str) -> User:
        return cache_backend.get_auth(auth_header)

    @staticmethod
    def set(cache_backend: AbstractCache, auth_header: str, user: User):
        return cache_backend.set_auth(auth_header, user)
