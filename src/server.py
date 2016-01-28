import logging

import tornado
from tornado.web import Application, RequestHandler, stream_request_body
from tornadostreamform.multipart_streamer import MultiPartStreamer
from tornado import gen
from tornado import concurrent
import tornado.httpserver

from tornado.options import define, options

define('debug', default=False)
define('asyncio', default=False)
define('dummy', default=False)
define('transfers', default=10)
define('port', default='8888')
define('noauth', default=False)

logger = logging.getLogger(__name__)

async def check_auth(auth, prefix, file_path, action):
    gen.sleep(1)
    if action == 'POST':
        return options.noauth or auth == "Token MAGICFARYDUST"
    else:
        return True


@stream_request_body
class FileHandler(RequestHandler):
    auth = None
    streamer = None

    def initialize(self):
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(options.transfers)
        if options.dummy:
            from backends.dummy import Transfer
        else:
            from backends.s3 import Transfer
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
        else:
            self.auth = True
        if self.request.method == 'POST':
            self.streamer = MultiPartStreamer(0)

    async def data_received(self, chunk):
        if not self.auth:
            self.send_error()
        self.streamer.data_received(chunk)

    @gen.coroutine
    def get(self, prefix, file_path):
        body = yield self.retrieve_file(prefix, file_path)
        if body is None:
            self.send_error(404)
            return
        with open(body, 'rb') as f_in:
            content = f_in.read()
            self.write(content)
        self.finish()

    @gen.coroutine
    def post(self, prefix, file_path):
        if self.streamer:
            self.streamer.data_complete()
            if not len(self.streamer.parts) == 1:
                self.send_error(400, reason="No file found")
                return
            yield self.store_file(prefix, file_path, self.streamer.parts[0].f_out.name)
            self.streamer.release_parts()

    async def head(self, prefix, file_path):
        self.set_status(304)
        self.finish()

    @concurrent.run_on_executor(executor='_thread_pool')
    def store_file(self, prefix, file_path, filename):
        return self.transfer.store_file(prefix, file_path, filename)

    @concurrent.run_on_executor(executor='_thread_pool')
    def retrieve_file(self, prefix, file_path):
        return self.transfer.retrieve_file(prefix, file_path)


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


if __name__ == '__main__':
    main()
