import json
import logging
import logging.config
from time import perf_counter
from typing import Callable
from prometheus_client import start_http_server

import tempfile
import tornado
import tornado.httpserver
from functools import partial
from tornado import concurrent
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.options import define, options
from tornado.web import Application, RequestHandler, stream_request_body

from blockserver.backend import cache, auth
from blockserver.backend.transfer import AbstractTransfer, StorageObject, S3Transfer, DummyTransfer
from blockserver import monitoring as mon

define('debug', help="Enable debug output for tornado", default=False)
define('asyncio', help="Run on the asyncio loop instead of the tornado IOLoop", default=False)
define('transfers', help="Thread pool size for transfers", default=10)
define('port', help="Port of this server", default=8888)
define('apisecret', help="API_SECRET of the accounting server", default='secret')
define('noauth', help="Disable authentication", default=False)
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


async def console_log(auth, storage_object: StorageObject, action: str, size: int):
    print("{prefix} / {file_path} {action} {size}".format(
        prefix=storage_object.prefix,
        file_path=storage_object.file_path,
        action=action,
        size=size
    ))


@mon.time(mon.WAIT_FOR_QUOTA)
async def send_log(auth, storage_object: StorageObject, action: str, size: int):
    http_client = AsyncHTTPClient()
    url = options.accounting_host + '/api/v0/quota/'
    payload = {'prefix': storage_object.prefix, 'file_path': storage_object.file_path,
               'action': action, 'size': size}
    response = await http_client.fetch(url, method='POST',
                            headers={'Authorization': auth,
                                     'APISECRET': options.apisecret,
                                     'Content-Type': 'application/json'},
                            body=json.dumps(payload), raise_error=False)
    if response.code >= 300:
        mon.COUNT_QUOTA_ERROR.inc()
        logger.error('Could not send log: {}'.format(response.body))


@stream_request_body
class FileHandler(RequestHandler):
    auth = None
    streamer = None

    def initialize(self,
                   transfer_cls: Callable[[], Callable[[], AbstractTransfer]]=None,
                   auth_callback: Callable[[], Callable[[str, str, str, str], bool]]=None,
                   log_callback: Callable[[], Callable[[StorageObject, str, int], None]]=None,
                   cache_cls: Callable[[], Callable[[], cache.AbstractCache]]=None,
                   concurrent_transfers: int=10):
        """
        :param transfer_cls: A function that returns a Transfer class
        :param auth_callback: A function that returns a callback used for authorization
        :param log_callback: A function that returns a callback used to log an action
        :param cache_cls: A funciton that returns a Cache class
        :param concurrent_transfers: Size of the thread pool used for transfers
        :return:
        """
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(options.transfers)
        self.cache = cache_cls()()  # type: cache.AbstractCache
        self.transfer = transfer_cls()(cache=self.cache)
        self.auth_callback = auth_callback()
        self.log_callback = log_callback()

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
            file_path = self.path_kwargs['file_path']
        except KeyError:
            self.send_error(403, reason="No correct prefix given")
            return False

        self.auth_header = self.request.headers.get('Authorization', None)
        if self.auth_header is None:
            self.send_error(403, reason="No authorization given")
            return False

        try:
            user_id = self.cache.get_auth(self.auth_header)
        except KeyError:
            user_id = await self.auth_callback(self.auth_header)
            self.cache.set_auth(self.auth_header, user_id)
            mon.COUNT_AUTH_CACHE_SETS.inc()
        else:
            mon.COUNT_AUTH_CACHE_HITS.inc()

        authorized = True

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
            yield self.log_callback(self.auth_header, storage_object, 'get', size)
        self.finish()

    @gen.coroutine
    def post(self, prefix, file_path):
        self.temp.seek(0)
        storage_object, size_diff = yield self.store_file(
                prefix, file_path, self.temp.name)
        self.temp.close()
        mon.TRAFFIC_REQUEST.inc(storage_object.size)
        yield self.log_callback(self.auth_header,
                                StorageObject(prefix, file_path, None, None),
                                'store', size_diff)
        self.set_status(204)
        self.set_header('ETag', storage_object.etag)
        self.finish()

    @gen.coroutine
    def delete(self, prefix, file_path):
        size = yield self.delete_file(prefix, file_path)
        yield self.log_callback(self.auth_header,
                                StorageObject(prefix, file_path, None, None),
                                'store', -size)
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
        mon.REQ_IN_PROGRESS.dec()
        mon.REQ_RESPONSE.observe(perf_counter() - self._start_time)


def main():
    application = make_app(debug=options.debug)

    with open(options.logging_config, 'r') as conf:
        conf_dictionary = json.load(conf)
        logging.config.dictConfig(conf_dictionary)

    if options.prometheus_port:
        start_http_server(options.prometheus_port)

    if options.debug:
        application.listen(options.port)
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


def make_app(cache_cls=None, log_callback=None, debug=False):
    if options.dummy and not debug:
        raise RuntimeError("Dummy backend is only allowed in debug mode")

    def get_auth_func():
        if options.noauth:
            async def noauth(*args):
                return True
            return noauth
        if options.dummy_auth:
            return auth.DummyAuth.auth
        else:
            return auth.AccountingServerAuth.auth

    if cache_cls is None:
        def cache_cls():
            if options.dummy_cache:
                return cache.DummyCache
            else:
                return partial(cache.RedisCache, host=options.redis_host, port=options.redis_port)

    if log_callback is None:
        def log_callback():
            return console_log if options.dummy_log else send_log

    def get_transfer_cls():
        return DummyTransfer if options.dummy else S3Transfer

    application = Application([
        (r'^/api/v0/files/(?P<prefix>[\d\w-]+)/(?P<file_path>[/\d\w-]+)', FileHandler, dict(
            transfer_cls=get_transfer_cls,
            auth_callback=get_auth_func,
            log_callback=log_callback,
            cache_cls=cache_cls,
            concurrent_transfers=options.transfers,
        ))
    ], debug=debug)
    return application
