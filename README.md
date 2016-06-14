# Qabel Block Server

This server handles uploads, downloads and deletes on the storage backend for Qabel Box.

## Requirements

* Postgresql >= 9.5
* Python >= 3.5
* Redis

## Installation

	cd src
	virtualenv ../venv --python=python3.5
	source ../venv/bin/activate
	pip install -r ../requirements.txt

The server needs a PostgreSQL (>=9.5) database that needs to me initialized by

	alembic -x "url=postgresql://username:password@localhost/dbname" upgrade head

Note: If the DSN for your PostgreSQL is different, then you need to adjust it both for Alembic and set the `--psql-dsn`
option accordingly.

## Running

- The tests

	The tests also need a redis server on localhost, running on port 6379. They start their own postgresql instance
	given it is installed.

	And you need to place AWS credentials in `~/.aws`, the
	[boto docs](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)
	document details and alternatives.

      src$ py.test

- The server directly

      src$ python run.py <options>

- The server in the uWSGI application server: see `src/uwsgi_plumbing.py`. Read the part about how you pass options
  to the server.

## Storage backends

Available storage backends:

- S3

  S3 is the default and requires AWS credentials in `~/.aws`, the
  [boto docs](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration) document details and
  alternatives.

- Local storage

  Local storage requires nothing special, just a file system. The option is `--local-storage` (on the command line)
  and takes the directory to store files in as a sole parameter

    Note: ETags are currently generated from the mtime, therefore the
    filesystem should support high resolution (nanosecond) timestamps for
    production systems. I.e. no OSX, FAT etc.

- Dummy (temporary), `--dummy`, requires no parameters and is an amnesiac.

## Options reference

(from `python run.py --help`)

    Tornado Logging options:

      --log-file-max-size              max size of log files before rollover
                                       (default 100000000)
      --log-file-num-backups           number of log files to keep (default 10)
      --log-file-prefix=PATH           Path prefix for log files. Note that if you
                                       are running multiple tornado processes,
                                       log_file_prefix must be different for each
                                       of them (e.g. include the port number)
      --log-rotate-interval            The interval value of timed rotating
                                       (default 1)
      --log-rotate-mode                The mode of rotating files(time or size)
                                       (default size)
      --log-rotate-when                specify the type of TimedRotatingFileHandler
                                       interval other options:('S', 'M', 'H', 'D',
                                       'W0'-'W6') (default midnight)
      --log-to-stderr                  Send log output to stderr (colorized if
                                       possible). By default use stderr if
                                       --log_file_prefix is not set and no other
                                       logging is configured.
      --logging=debug|info|warning|error|none
                                       Set the Python log level. If 'none', tornado
                                       won't touch the logging configuration.
                                       (default info)

    Blocks server options:

      --accounting-host                Base url to the accounting server (default
                                       http://localhost:8000)
      --address                        Address of this server (default localhost)
      --apisecret                      API_SECRET of the accounting server (default
                                       secret)
      --asyncio                        Run on the asyncio loop instead of the
                                       tornado IOLoop (default False)
      --debug                          Enable debug output for tornado (default
                                       False)
      --dummy                          Use a local and temporary storage backend
                                       instead of s3 backend (default False)
      --dummy-auth                     Authenticate with this authentication token
                                       [Example: MAGICFARYDUST] for the prefix
                                       'test'
      --dummy-cache                    Use an in memory cache instead of redis
                                       (default False)
      --dummy-log                      Instead of calling the accounting server for
                                       logging, log to stdout (default False)
      --local-storage                  Store files locally in *specified directory*
                                       instead of S3
      --logging-config                 Config file for logging, see https://docs.py
                                       thon.org/3.5/library/logging.config.html
                                       (default ../logging.json)
      --max-body-size                  Maximum size for uploads (default
                                       2147483648)
      --port                           Port of this server (default 8888)
      --prometheus-port                Port to start the prometheus metrics server
                                       on
      --psql-dsn                       libq connection string for postgresql
                                       (default postgresql://postgres:postgres@loca
                                       lhost/qabel-block)
      --redis-host                     Hostname of the redis server (default
                                       localhost)
      --redis-port                     Port of the redis server (default 6379)
      --transfers                      Thread pool size for transfers (default 10)
