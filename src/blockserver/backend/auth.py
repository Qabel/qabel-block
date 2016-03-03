from blockserver import monitoring as mon
from tornado.httpclient import AsyncHTTPClient, HTTPError
from blockserver.server import options
import json


class AuthError(Exception):
    pass


class UserNotFound(AuthError):
    pass


class DummyAuth:

    @staticmethod
    async def auth(auth_header: str) -> int:
        return 0


class Auth:

    @staticmethod
    async def auth(auth_header: str) -> int:
        user = CacheAuth.auth(auth_header)
        if user is None:
            user = await AccountingServerAuth.request(auth_header)
        if user is None:
            raise UserNotFound(auth_header)
        return user


class AccountingServerAuth:

    @staticmethod
    @mon.time(mon.WAIT_FOR_AUTH)
    async def request(auth_header: str) -> int:
        request_body = json.dumps({'auth': auth_header})
        response = await AccountingServerAuth.send_request(request_body)
        body = json.loads(response.content)
        return body.get('user_id', None)

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
    def auth(auth_header: str) -> int:
        pass
