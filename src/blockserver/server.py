import json
import logging
import logging.config
from time import perf_counter
from prometheus_client import start_http_server

import tempfile
import tornado
import tornado.httpserver
from functools import partial
from tornado import concurrent
from tornado import gen
from tornado.options import define, options
from tornado.web import Application, RequestHandler, stream_request_body

from blockserver.backend import cache, auth
from blockserver.backend.transfer import StorageObject, S3Transfer, DummyTransfer
from blockserver.backend.database import PostgresUserDatabase
from psycopg2.pool import SimpleConnectionPool
from blockserver import monitoring as mon

define('debug', help="Enable debug output for tornado", default=False)
define('asyncio', help="Run on the asyncio loop instead of the tornado IOLoop", default=False)
define('transfers', help="Thread pool size for transfers", default=10)
define('port', help="Port of this server", default=8888)
define('address', help="Address of this server", default="localhost")
define('apisecret', help="API_SECRET of the accounting server", default='secret')
define('psql_dsn', help="libq connection string for postgresql",
        default='postgresql://postgres:postgres@localhost/qabel-block')
define('dummy_auth',
       help="Authenticate with this authentication token [Example: MAGICFARYDUST] "
            "for the prefix 'test'", default=None, type=str)
define('accounting_host',
       help="Base url to the accounting server", default="http://localhost:8000")
define('dummy',
       help="Use a local and temporary storage backend instead of s3 backend", default=False)
define('dummy_log', help="Instead of calling the accounting server for logging, log to stdout",
       default=False)
define('dummy_cache', help="Use an in memory cache instead of redis",
       default=False)
define('redis_host', help="Hostname of the redis server", default='localhost')
define('redis_port', help="Port of the redis server", default=6379)
define('prometheus_port', help="Port to start the prometheus metrics server on",
       default=None, type=int)
define('logging_config',
       help="Config file for logging, "
            "see https://docs.python.org/3.5/library/logging.config.html",
       default='../logging.json')

logger = logging.getLogger(__name__)


class DatabaseMixin:

    @property
    def database(self):
        if self._connection is None:
            self._connection = self.database_pool.getconn()
            self._database = PostgresUserDatabase(self._connection)
        return self._database

    def finish(self, chunk=None):
        if self._connection is not None:
            self.database_pool.putconn(self._connection)


# noinspection PyMethodOverriding
@stream_request_body
class FileHandler(RequestHandler, DatabaseMixin):
    auth = None
    streamer = None

    def initialize(self, transfer_cls, get_auth_cls, get_cache_cls, database_pool,
                   concurrent_transfers: int=10):
        """
        :param get_cache_class: A funciton that returns a Cache class
        :param get_auth_cls: A function that returns a callback used for authorization
        :param database_pool: Postgresql database pool
        :param transfer_cls: A function that returns a Transfer class
        :param concurrent_transfers: Size of the thread pool used for transfers
        :return:
        """
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(options.transfers)
        self.cache = get_cache_cls()()  # type: cache.AbstractCache
        self.transfer = transfer_cls()(cache=self.cache)
        self.auth_callback = get_auth_cls()(self.cache)
        self.database_pool = database_pool
        self._connection = None

    async def prepare(self):
        self._start_time = perf_counter()
        mon.REQ_IN_PROGRESS.inc()
        self.auth = None
        self.streamer = None
        self.auth = await self._authorize_request()
        if not self.auth:
            mon.COUNT_ACCESS_DENIED.inc()
            return
        if self.request.method == 'POST':
            self.temp = tempfile.NamedTemporaryFile()

    async def _authorize_request(self):
        try:
            prefix = self.path_kwargs['prefix']
        except KeyError:
            self.send_error(400, reason="No correct prefix given")
            return False

        if self.request.method == 'GET':
            return True
        else:
            auth_header = self.request.headers.get('Authorization', None)
            if auth_header is None:
                self.send_error(403, reason="No authorization given")
                return False

        try:
            user = await self.auth_callback.auth(auth_header)
        except auth.UserNotFound:
            authorized = False
        except auth.BypassAuth:
            return True
        else:
            authorized = self.database.has_prefix(user.user_id, prefix)

        if not authorized:
            self.send_error(403, reason="Not authorized for this prefix")
        return authorized

    async def data_received(self, chunk):
        if not self.auth:
            self.send_error()
            return
        self.temp.write(chunk)

    @gen.coroutine
    def get(self, prefix, file_path):
        etag = self.request.headers.get('If-None-Match', None)
        storage_object = yield self.retrieve_file(prefix, file_path, etag)
        if storage_object is None:
            self.send_error(404)
            return
        self.set_header('ETag', storage_object.etag)
        if storage_object.local_file is None:
            self.set_status(304)
        else:
            self.set_header('Content-Length', storage_object.size)
            with open(storage_object.local_file, 'rb') as f_in:
                for chunk in iter(lambda: f_in.read(8192), b''):
                    self.write(chunk)
                size = f_in.tell()
            mon.TRAFFIC_RESPONSE.inc(size)
            self.save_traffic_log(prefix, size)
        self.finish()

    @gen.coroutine
    def post(self, prefix, file_path):
        self.temp.seek(0)
        storage_object, size_diff = yield self.store_file(
                prefix, file_path, self.temp.name)
        self.temp.close()
        mon.TRAFFIC_REQUEST.inc(storage_object.size)
        self.save_size_log(prefix, size_diff)
        self.set_status(204)
        self.set_header('ETag', storage_object.etag)
        self.finish()

    @gen.coroutine
    def delete(self, prefix, file_path):
        size = yield self.delete_file(prefix, file_path)
        self.save_size_log(prefix, -size)
        self.set_status(204)
        self.finish()

    @concurrent.run_on_executor(executor='_thread_pool')
    def delete_file(self, prefix, file_path):
        return self.transfer.delete(StorageObject(prefix, file_path, None, None))

    @concurrent.run_on_executor(executor='_thread_pool')
    def store_file(self, prefix, file_path, filename):
        return self.transfer.store(StorageObject(prefix, file_path, None, filename))

    @concurrent.run_on_executor(executor='_thread_pool')
    def retrieve_file(self, prefix, file_path, etag):
        return self.transfer.retrieve(StorageObject(prefix, file_path, etag, None))

    def finish(self, chunk=None):
        super().finish(chunk)
        if self._connection is not None:
            self.database_pool.putconn(self._connection)
        mon.REQ_IN_PROGRESS.dec()
        mon.REQ_RESPONSE.observe(perf_counter() - self._start_time)

    def save_traffic_log(self, prefix, traffic):
        if traffic > 0:
            self.database.update_traffic(prefix, traffic)

    def save_size_log(self, prefix, size):
        if size != 0:
            self.database.update_size(prefix, size)


class AuthorizationMixin:

    async def prepare(self):
        self.authorized = False
        auth_header = self.request.headers.get('Authorization', None)
        if auth_header is None:
            self.send_error(403, reason="No authorization given")
            return
        try:
            user = await self.auth_callback.auth(auth_header)
        except auth.UserNotFound:
            self.send_error(403, reason="User not found")
            return
        except auth.BypassAuth as e:
            user = e.user
        self.authorized = True
        self.user = user


# noinspection PyMethodOverriding
class PrefixHandler(AuthorizationMixin, RequestHandler, DatabaseMixin):
    def data_received(self, chunk):
        pass

    def initialize(self, get_auth_cls, get_cache_cls, database_pool):
        self.cache = get_cache_cls()()
        self.database_pool = database_pool
        self._connection = None
        self.auth_callback = get_auth_cls()(self.cache)

    async def prepare(self):
        await super().prepare()

    def finish(self, chunk=None):
        super().finish(chunk)

    @gen.coroutine
    def get(self):
        if not self.authorized:
            self.finish()
            return
        self.set_status(200)
        prefixes = self.database.get_prefixes(self.user.user_id)
        self.write(json.dumps({'prefixes': prefixes}))
        self.finish()

    @gen.coroutine
    def post(self):
        if not self.authorized:
            self.finish()
            return
        self.set_status(201)
        new_prefix = self.database.create_prefix(self.user.user_id)
        self.write(json.dumps({'prefix': new_prefix}))
        self.finish()


# noinspection PyMethodOverriding
class QuotaHandler(AuthorizationMixin, RequestHandler, DatabaseMixin):
    def data_received(self, chunk):
        pass

    def initialize(self, get_auth_cls, get_cache_cls, database_pool):
        self.cache = get_cache_cls()()
        self.database_pool = database_pool
        self._connection = None
        self.auth_callback = get_auth_cls()(self.cache)

    async def prepare(self):
        await super().prepare()

    def finish(self, chunk=None):
        super().finish(chunk)

    @gen.coroutine
    def get(self):
        if not self.authorized:
            self.finish()
            return
        self.set_status(200)
        quota, size = self.database.get_size(self.user.user_id)
        self.write(json.dumps({'quota': quota, 'size': size}))
        self.finish()


def main():
    application = make_app(debug=options.debug)

    with open(options.logging_config, 'r') as conf:
        conf_dictionary = json.load(conf)
        logging.config.dictConfig(conf_dictionary)

    if options.prometheus_port:
        start_http_server(options.prometheus_port)

    if options.debug:
        application.listen(address=options.address, port=options.port)
    else:
        server = tornado.httpserver.HTTPServer(application)
        server.bind(options.port)
        server.start()
    if options.asyncio:
        logger.info('Using asyncio')
        from tornado.platform.asyncio import AsyncIOMainLoop
        AsyncIOMainLoop.current().start()
    else:
        logger.info('Using IOLoop')
        from tornado.ioloop import IOLoop
        IOLoop.current().start()


def make_app(cache_cls=None, database_pool=None, debug=False):
    if options.dummy and not debug:
        raise RuntimeError("Dummy backend is only allowed in debug mode")

    def get_auth_class():
        if options.dummy_auth:
            return auth.DummyAuth
        else:
            return auth.Auth

    if cache_cls is None:
        def cache_cls():
            if options.dummy_cache:
                return cache.DummyCache
            else:
                return partial(cache.RedisCache, host=options.redis_host, port=options.redis_port)

    def get_transfer_cls():
        return DummyTransfer if options.dummy else S3Transfer

    if database_pool is None:
        database_pool = SimpleConnectionPool(1, 20, dsn=options.psql_dsn)

    application = Application([
        (r'^/api/v0/files/(?P<prefix>[\d\w-]+)/(?P<file_path>[/\d\w-]+)', FileHandler, dict(
            transfer_cls=get_transfer_cls,
            get_auth_cls=get_auth_class,
            get_cache_cls=cache_cls,
            database_pool=database_pool,
            concurrent_transfers=options.transfers,
        )),
        (r'^/api/v0/prefix/', PrefixHandler, dict(
            get_cache_cls=cache_cls,
            get_auth_cls=get_auth_class,
            database_pool=database_pool,
        )),
        (r'^/api/v0/quota/', QuotaHandler, dict(
            get_cache_cls=cache_cls,
            get_auth_cls=get_auth_class,
            database_pool=database_pool,
        ))
    ], debug=debug)
    return application
