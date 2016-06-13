import shutil
import psycopg2
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
from tornado.web import Application, RequestHandler, stream_request_body, Finish, HTTPError

from blockserver.backend import cache, auth
from blockserver.backend.transfer import StorageObject, S3Transfer, DummyTransfer
from blockserver.backend.database import PostgresUserDatabase
from psycopg2.pool import SimpleConnectionPool
from blockserver import monitoring as mon
from blockserver.backend.quota import QuotaPolicy

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
define('max_body_size', help="Maximum size for uploads", default=2147483648)
define('prometheus_port', help="Port to start the prometheus metrics server on",
       default=None, type=int)
define('logging_config',
       help="Config file for logging, "
            "see https://docs.python.org/3.5/library/logging.config.html",
       default='../logging.json')

logger = logging.getLogger(__name__)


class DatabaseMixin:

    async def get_database(self):
        while self._connection is None:
            try:
                self._connection = self.database_pool.getconn()
            except psycopg2.pool.PoolError:
                mon.DB_WAIT_FOR_CONNECTIONS.inc(0.5)
                logger.warning('Waiting for db connection')
                await gen.sleep(0.5)
            self._database = PostgresUserDatabase(self._connection)
        return self._database

    def finish_database(self):
        if self._connection is not None:
            self.database_pool.putconn(self._connection)
            self._connection = None

    def on_finish(self):
        self.finish_database()


# noinspection PyMethodOverriding
@stream_request_body
class FileHandler(DatabaseMixin, RequestHandler):
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
        self.temp = None

    async def prepare(self):
        self._start_time = perf_counter()
        mon.REQ_IN_PROGRESS.inc()
        self.auth = None
        self.streamer = None
        await self._authorize_request()
        if self.request.method == 'POST':
            self.remaining_upload_size = options.max_body_size
            self.temp = tempfile.NamedTemporaryFile()
        self.finish_database()

    def write_error(self, status_code, **kwargs):
        if 'exc_info' in kwargs:
            _, exc, _ = kwargs['exc_info']
            mon.HTTP_ERROR.labels(str(exc)).inc()
        else:
            mon.HTTP_ERROR.labels('unknown').inc()
        super().write_error(status_code, **kwargs)

    async def _authorize_request(self):
        prefix = await self._get_prefix()

        if self.request.method == 'GET':
            await self._authorize_get_request(prefix)
        else:
            try:
                auth_header = self.request.headers.get('Authorization', None)
            except KeyError:
                raise HTTPError(403, reason="No authorization supplied")
            await self._authorize_write_request(auth_header, prefix)

    async def _authorize_write_request(self, auth_header, prefix):
        try:
            self.user = await self.auth_callback.auth(auth_header)
        except auth.UserNotFound:
            raise HTTPError(403, reason="User not found")
        except auth.BypassAuth as bypass_auth:
            self.user = bypass_auth.args[0]
        else:
            db = await self.get_database()
            if not db.has_prefix(self.user.user_id, prefix):
                raise HTTPError(403, reason="Not authorized for this prefix")

    async def _authorize_get_request(self, prefix):
        await self._check_download_traffic((await self.get_database()), prefix)

    async def _get_prefix(self):
        try:
            return self.path_kwargs['prefix']
        except KeyError:
            raise HTTPError(400, reason="No correct prefix supplied")

    async def _check_download_traffic(self, db, prefix):
        current_traffic = db.get_traffic_by_prefix(prefix)
        prefix_owner = db.get_prefix_owner(prefix)
        if prefix_owner is None:
            return  # prefix does not exist, will 404 later
        permitted_traffic = (await self.auth_callback.get_user(prefix_owner)).traffic_quota
        if current_traffic > permitted_traffic:
            # TODO: the download traffic quota should probably be a soft-quota, not hard (i.e. limit bandwidth or
            # TODO: insert a delay to annoy people [less].)
            self._quota_error()

    def _quota_error(self):
        raise HTTPError(402, reason="Quota reached")

    async def data_received(self, chunk):
        self.remaining_upload_size -= len(chunk)
        if self.remaining_upload_size < 0:
            self.temp.close()
            mon.CONTENT_LENGTH_ERROR.inc()
            raise HTTPError(400, reason="Content-Length too large")
        self.temp.write(chunk)

    @gen.coroutine
    def get(self, prefix, file_path):
        etag = self.request.headers.get('If-None-Match', None)
        storage_object = yield self.retrieve_file(prefix, file_path, etag)
        if storage_object is None:
            raise HTTPError(404, reason="File not found")
        self.set_header('ETag', storage_object.etag)
        if storage_object.fd is None:
            self.set_status(304)
            raise Finish

        size = storage_object.size
        self.set_header('Content-Length', size)
        shutil.copyfileobj(storage_object.fd, self)
        storage_object.fd.close()
        mon.TRAFFIC_RESPONSE.inc(size)
        yield self.save_traffic_log(prefix, size)
        self.finish()

    @gen.coroutine
    def post(self, prefix, file_path):
        if not self.check_post_etag(prefix, file_path, self.request.headers.get('If-Match')):
            return

        file_size = self.temp.tell()
        yield self._authorize_upload_request(file_path, file_size, prefix)
        self.finish_database()

        self.temp.seek(0)
        storage_object, size_diff = yield self.store_file(prefix, file_path, self.temp.name)
        self.temp.close()
        mon.TRAFFIC_REQUEST.inc(storage_object.size)
        yield self.save_size_log(prefix, size_diff)
        self.set_status(204)
        self.set_header('ETag', storage_object.etag)
        self.finish()

    def check_post_etag(self, prefix, file_path, etag):
        if not etag:
            return True
        stored_object = self.transfer.meta(StorageObject(prefix, file_path))
        if not stored_object:
            self.set_status(412, reason='If-Match ETag did not match: object does not exist.')
            self.finish()
            return False
        elif stored_object.etag != etag:
            self.set_status(412, reason='If-Match ETag did not match stored object')
            self.set_header('ETag', stored_object.etag)
            self.finish()
            return False
        return True

    async def _authorize_upload_request(self, file_path, file_size, prefix):
        used_quota = (await self.get_database()).get_size(self.user.user_id)
        quota_reached = used_quota + file_size > self.user.quota
        is_block = file_path.startswith('block/')
        old_size = self.transfer.get_size(StorageObject(prefix, file_path))
        if old_size is None:
            is_overwrite = False
            size_change = file_size
        else:
            is_overwrite = True
            size_change = file_size - old_size
        if not QuotaPolicy.upload(quota_reached, size_change, is_block, is_overwrite):
            self.temp.close()
            self._quota_error()

    @gen.coroutine
    def delete(self, prefix, file_path):
        size = yield self.delete_file(prefix, file_path)
        yield self.save_size_log(prefix, -size)
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

    def on_finish(self):
        super().on_finish()
        if self.temp:
            self.temp.close()
        mon.REQ_IN_PROGRESS.dec()
        mon.REQ_RESPONSE.observe(perf_counter() - self._start_time)

    async def save_traffic_log(self, prefix, traffic):
        if traffic > 0:
            (await self.get_database()).update_traffic(prefix, traffic)
            mon.TRAFFIC_BY_REQUEST.observe(traffic)

    async def save_size_log(self, prefix, size):
        if size != 0:
            (await self.get_database()).update_size(prefix, size)
            if size > 0:
                mon.QUOTA_BY_REQUEST.labels({'type': 'increase'}).observe(size)
            else:
                mon.QUOTA_BY_REQUEST.labels({'type': 'decrease'}).observe(-size)


class AuthorizationMixin:

    async def prepare(self):
        auth_header = self.request.headers.get('Authorization', None)
        if auth_header is None:
            raise HTTPError(403, reason="No authorization given")
        try:
            self.user = await self.auth_callback.auth(auth_header)
        except auth.UserNotFound:
            raise HTTPError(403, reason="User not found")
        except auth.BypassAuth as bypass_auth:
            self.user = bypass_auth.args[0]


# noinspection PyMethodOverriding,PyAbstractClass
class PrefixHandler(AuthorizationMixin, DatabaseMixin, RequestHandler):

    def initialize(self, get_auth_cls, get_cache_cls, database_pool):
        self.cache = get_cache_cls()()
        self.database_pool = database_pool
        self._connection = None
        self.auth_callback = get_auth_cls()(self.cache)

    @gen.coroutine
    def get(self):
        self.set_status(200)
        db = yield self.get_database()
        prefixes = db.get_prefixes(self.user.user_id)
        self.write({'prefixes': prefixes})
        self.finish()

    @gen.coroutine
    def post(self):
        self.set_status(201)
        db = yield self.get_database()
        new_prefix = db.create_prefix(self.user.user_id)
        self.write({'prefix': new_prefix})
        self.finish()


# noinspection PyMethodOverriding,PyAbstractClass
class QuotaHandler(AuthorizationMixin, DatabaseMixin, RequestHandler):

    def initialize(self, get_auth_cls, get_cache_cls, database_pool):
        self.cache = get_cache_cls()()
        self.database_pool = database_pool
        self._connection = None
        self.auth_callback = get_auth_cls()(self.cache)

    @gen.coroutine
    def get(self):
        self.set_status(200)
        db = yield self.get_database()
        size = db.get_size(self.user.user_id)
        self.write({
            'quota': self.user.quota,
            'size': size
        })
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
        server = tornado.httpserver.HTTPServer(application,
                                               xheaders=True)
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
