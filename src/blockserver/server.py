import logging

import tornado
import tornado.httpserver
from tornado import concurrent
from tornado import gen
from tornado.options import define, options
from tornado.web import Application, RequestHandler, stream_request_body
import tempfile

define('debug', default=False)
define('asyncio', default=False)
define('dummy', default=False)
define('transfers', default=10)
define('port', default='8888')
define('noauth', default=False)
define('magicauth', default="Token MAGICFARYDUST")

logger = logging.getLogger(__name__)

async def check_auth(auth, prefix, file_path, action):
    gen.sleep(1)
    if action == 'POST':
        return options.noauth or auth == options.magicauth
    else:
        return True


@stream_request_body
class FileHandler(RequestHandler):
    auth = None
    streamer = None

    def initialize(self):
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(options.transfers)
        if options.dummy:
            from .backends.dummy import Transfer
        else:
            from .backends.s3 import Transfer
        self.transfer = Transfer()

    async def prepare(self):
        self.auth = None
        self.streamer = None
        try:
            prefix = self.path_kwargs['prefix']
            file_path = self.path_kwargs['file_path']
        except KeyError:
            self.send_error(403)
            return
        auth = self.request.headers.get('Authorization', None)
        if not await check_auth(auth, prefix, file_path, self.request.method):
            self.auth = False
            self.send_error(403, reason="Not authorized for this prefix")
            return
        else:
            self.auth = True
        if self.request.method == 'POST':
            self.temp = tempfile.NamedTemporaryFile(delete=False)

    async def data_received(self, chunk):
        if not self.auth:
            self.send_error()
            return
        self.temp.write(chunk)

    @gen.coroutine
    def get(self, prefix, file_path):
        path = yield self.retrieve_file(prefix, file_path)
        if path is None:
            self.send_error(404)
            return
        self.write(open(path, 'rb').read())
        self.finish()

    @gen.coroutine
    def post(self, prefix, file_path):
        if self.temp:
            self.temp.close()
            yield self.store_file(prefix, file_path, self.temp.name)

    async def head(self, prefix, file_path):
        self.set_status(304)
        self.finish()

    @concurrent.run_on_executor(executor='_thread_pool')
    def store_file(self, prefix, file_path, filename):
        return self.transfer.store(prefix, file_path, filename)

    @concurrent.run_on_executor(executor='_thread_pool')
    def retrieve_file(self, prefix, file_path):
        return self.transfer.retrieve(prefix, file_path)


def main():
    tornado.options.parse_command_line()
    application = make_app()
    if options.debug:
        application.listen(options.port)
    else:
        server = tornado.httpserver.HTTPServer(application)
        server.bind(options.port)
        server.start(0)
    if options.asyncio:
        logger.info('Using asyncio')
        from tornado.platform.asyncio import AsyncIOMainLoop
        AsyncIOMainLoop.current().start()
    else:
        logger.info('Using IOLoop')
        from tornado.ioloop import IOLoop
        IOLoop.current().start()


def make_app():
    application = Application([
        (r'^/files/(?P<prefix>[\d\w-]+)/(?P<file_path>[\d\w-]+)', FileHandler),
    ], debug=options.debug)
    return application
