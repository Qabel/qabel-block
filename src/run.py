from tornado.ioloop import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop

from blockserver import server
from tornado import options
import signal


def sigint_handler(sig, frame):
    asyncio_loop = AsyncIOMainLoop.current()
    asyncio_loop.add_callback_from_signal(asyncio_loop.stop)
    io_loop = IOLoop.current()
    io_loop.add_callback_from_signal(io_loop.stop)

signal.signal(signal.SIGINT, sigint_handler)

if __name__ == "__main__":
    options.parse_command_line()
    server.main()

