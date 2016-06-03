"""
Plumbing to bolt tornado onto uWSGI while retaining asynchronous request handling.

It takes the same options as the run.py script, but here these are passed with the --pyargv uWSGI option.

The --prometheus-port option is the _base_ port. Each worker uses it's own port (prometheus_port + worker_id).

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
pyargv=--dummy --debug --dummy-auth=MAGICFAIRY
$ uwsgi uwsgi-block.ini
"""

import json
import logging.config
import socket
import sys

import uwsgi

from tornado.options import options
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer

from prometheus_client import start_http_server

from blockserver.server import make_app


def init(fd):
    worker_id = uwsgi.worker_id()
    application = make_app(debug=options.debug)
    server = HTTPServer(application, xheaders=True)
    sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
    server.add_sockets([sock])

    if options.prometheus_port:
        prometheus_port = options.prometheus_port + worker_id
        print('starting prometheus server on port %d' % prometheus_port)
        start_http_server(prometheus_port)
    print('uwsgi plumber reporting for duty on uWSGI worker %s' % worker_id)


# Parse command line
if len(sys.argv) == 2 and not sys.argv[1].startswith('--'):
    options.parse_config_file(sys.argv[1])
else:
    options.parse_command_line()

with open(options.logging_config, 'r') as conf:
    conf_dictionary = json.load(conf)
    logging.config.dictConfig(conf_dictionary)

# TODO: handle signals
# SIGINT = destroy
# SIGHUP = graceful reload

# spawn a handler for every uWSGI socket
for fd in uwsgi.sockets:
    init(fd)
IOLoop.current().start()
