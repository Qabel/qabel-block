"""
Plumbing to bolt tornado onto uWSGI while retaining asynchronous request handling.

It takes the same options as the run.py script, but here these are passed with the --pyargv uWSGI option.
Alternatively you can set options in the uWSGI worker configuration by prefixing them with "block_".

The prometheus-port option is the _base_ port. Each worker uses it's own port (prometheus_port + worker_id).

Examples:

Run locally in full-dummy mode (assuming cwd==src):

$ uwsgi --http-socket :9090 --plugin python --master -p 4 --python-worker-override uwsgi_plumbing.py \
    --pyargv '--dummy --debug --dummy-auth=MAGICFAIRY'

$ # post a file
$ curl -H 'Authorization: Token MAGICFAIRY' -v -d test http://127.0.0.1:9090/api/v0/files/123451234/testfile
$ # retrieve a file
$ # NOTE: with the dummy backend the multiple instances don't share the same file store, i.e. this randomly
  #       fails *in this specific example*. Use one worker process to avoid that.
$ curl -H 'Authorization: Token MAGICFAIRY' http://127.0.0.1:9090/api/v0/files/123451234/testfile
test%
$

Do the same thing, but use a uWSGI config file (use as basis for e.g. emperor-based setups)

$ cat uwsgi-block.ini
[uwsgi]
http-socket=:9090
plugin=python
master=true
processes=4
python-worker-override=uwsgi_plumbing.py

# You can mix _ and - as you like
block_dummy=true
block_debug=true
block_dummy_auth=MAGICFAIRY
$ uwsgi uwsgi-block.ini
"""

import faulthandler
import json
import logging.config
import linecache
import os
import os.path
import signal
import socket
import sys
import tracemalloc

import uwsgi

import tornado
from tornado.options import options
from tornado.httpserver import HTTPServer

from prometheus_client import start_http_server

from blockserver.server import make_app


class MemoryTracer:
    def __init__(self, top_lines=10):
        self.top_lines = top_lines

    def __call__(self, signo, frame):
        if not tracemalloc.is_tracing():
            print('Starting acquisition...')
            tracemalloc.start()
        else:
            print('Collecting snapshot...')
            snapshot = tracemalloc.take_snapshot()
            current, peak = tracemalloc.get_traced_memory()
            tracer_usage = tracemalloc.get_tracemalloc_memory()
            print('Stopping acquisition...')
            tracemalloc.stop()

            print('Currently mapped', current, 'bytes; peak was', peak, 'bytes')
            print('Tracer used', tracer_usage, 'bytes (before stopping)')
            self.print_top(snapshot, limit=self.top_lines)

    def print_top(self, snapshot, group_by='lineno', limit=10):
        snapshot = snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        ))
        top_stats = snapshot.statistics(group_by)

        print("Top %s allocators" % limit)
        for index, stat in enumerate(top_stats[:limit], 1):
            frame = stat.traceback[0]
            # replace "/path/to/module/file.py" with "module/file.py"
            filename = os.sep.join(frame.filename.split(os.sep)[-2:])
            print("#%s: %s:%s: %.1f KiB"
                  % (index, filename, frame.lineno, stat.size / 1024))
            line = linecache.getline(frame.filename, frame.lineno).strip()
            if line:
                print('    %s' % line)

        other = top_stats[limit:]
        if other:
            size = sum(stat.size for stat in other)
            print("%s other: %.1f KiB" % (len(other), size / 1024))
        total = sum(stat.size for stat in top_stats)
        print("Total allocated size: %.1f KiB" % (total / 1024))


def spawn_on_socket(fd):
    worker_id = uwsgi.worker_id()
    application = make_app(debug=options.debug)
    server = HTTPServer(application, xheaders=True, max_body_size=options.max_body_size)
    sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
    server.add_sockets([sock])

    if options.prometheus_port:
        prometheus_port = options.prometheus_port + worker_id
        uwsgi.log('starting prometheus server on port %d' % prometheus_port)
        start_http_server(prometheus_port)
    uwsgi.log('tornado plumber reporting for duty on uWSGI worker %s' % worker_id)


def stop_ioloop(sig, frame):
    print('uWSGI worker', uwsgi.worker_id(), 'received signal', sig)
    loop = tornado.ioloop.IOLoop.current()
    loop.add_callback_from_signal(loop.stop)


def apply_config_dict(config_dict, prefix=''):
    for name, value in config_dict.items():
        if '_' in name:
            name = name.replace('_', '-')
        if not name.startswith(prefix):
            continue
        name = name[len(prefix):]
        if name not in options:
            uwsgi.log('Invalid block_ server option in uWSGI config file: {}{}'.format(prefix, name))
            sys.exit(1)
        options._options[name].parse(value.decode())


def parse_arguments(argv):
    if len(argv) == 2 and not argv[1].startswith('--'):
        options.parse_config_file(argv[1])
    else:
        options.parse_command_line()


def configure_logging():
    file = options.logging_config
    if not os.path.exists(file):
        print('logging configuration {} not found, ignoring'.format(file))
        return
    with open(file, 'r') as conf:
        conf_dictionary = json.load(conf)
        logging.config.dictConfig(conf_dictionary)

tornado.platform.asyncio.AsyncIOMainLoop().install()

# Parse configuration from uWSGI config
apply_config_dict(uwsgi.opt, prefix='block-')
# Parse command line as well (from --pyargv)
# Overrides configuration file
parse_arguments(sys.argv)

configure_logging()

# Set up faulthandler (USRn are also *manual only* control signals for uWSGI)
faulthandler.register(signal.SIGUSR1)
faulthandler.enable()

memtracer = MemoryTracer(uwsgi.opt.get('block-memtracer-limit', 10))
signal.signal(signal.SIGUSR2, memtracer)

# uWSGI control signals
signal.signal(signal.SIGINT, stop_ioloop)
signal.signal(signal.SIGHUP, stop_ioloop)

# spawn a handler for every uWSGI socket
for fd in uwsgi.sockets:
    spawn_on_socket(fd)

loop = tornado.ioloop.IOLoop.current()

# set_blocking_log_threshold is an unique feature of Tornado's own IO loop, and not available with the asyncio implementations
# loop.set_blocking_log_threshold(1)
loop.start()
uwsgi.log('Worker %s dead.' % uwsgi.worker_id())
