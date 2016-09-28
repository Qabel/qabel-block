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

import json
import logging.config
import os.path
import signal
import socket
import sys

import uwsgi

from tornado.options import options
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer

from prometheus_client import start_http_server

from blockserver.server import make_app


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
    loop = IOLoop.current()
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

# Parse configuration from uWSGI config
apply_config_dict(uwsgi.opt, prefix='block-')
# Parse command line as well (from --pyargv)
# Overrides configuration file
parse_arguments(sys.argv)

configure_logging()

signal.signal(signal.SIGINT, stop_ioloop)
signal.signal(signal.SIGHUP, stop_ioloop)

# spawn a handler for every uWSGI socket
for fd in uwsgi.sockets:
    spawn_on_socket(fd)
loop = IOLoop.current()
loop.set_blocking_log_threshold(1)
loop.start()
uwsgi.log('Worker %s dead.' % uwsgi.worker_id())
