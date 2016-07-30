# Qabel Block Server

This server handles uploads, downloads and deletes on the storage backend for Qabel Box.

## Requirements:


- Python 3.5 (which *always* includes pip, unless your distribution is
  badly broken; try running `[sudo] python -m ensurepip` or use
  pyenv. Also, report your distribution.)
- virtualenv
- PostgreSQL >=9.5
- Redis

## <a name="installation"></a> Installation:

    # Creates a virtualenv where invoke et al live
    ./bootstrap.sh
    # Activates the virtualenv. Note: the leading "." is required.
    . ./activate.sh
    # Run simple standalone server
    cd src
	python run.py

Note: If the DSN for your PostgreSQL is different from the default
(`postgresql://postgres:postgres@localhost/qabel-block`),
change it in your local configuration (see below).

## <a name="configuration"></a> Configuration

If you intend to use the simple server script (`run.py`) then [configuration options](#opts) are passed
as command line parameters, for example:

    src$ python run.py --prometheus-port 1234 --local-storage /some/directory

When using our deployment system based on invoke the configuration happens in configuration files instead. The search
path is:

* /etc/invoke.yaml, /etc/qabel.yaml
* ~/.invoke.yaml, ~/.qabel.yaml
* ./invoke.yaml, ./qabel.yaml

Note that the `invoke.yaml` file in this directory is under version control and shouldn't be edited for local
configuration, instead, create a `qabel.yaml` file (either in this directory or in one of the places listed above).

The block server is configured in the `qabel.block` section, e.g.

    qabel:
        block:
            # options for the block server
            psql_dsn: postgresql://postgres:verysecure@pgsql.local/qabel-block
            accounting-host: http://testing.example.net/
            local-storage: /storage/directory

            uwsgi:
                # general uWSGI options
                processes: 4
                http-socket: /tmp/block-server.sock

The options directly in the `block` section are passed to the server (not over a command line, therefore not visible to
any other users on the system), while the `uwsgi` subsection is intended for any uWSGI options needed for your
particular setup. This example instructs uWSGI to use 4 worker processes and create a HTTP UDS socket at
`/tmp/block-server.sock`, which may be typical when using a reverse proxy.

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

### Production deployment

Use uWSGI as the application server (for multiple applications the emperor mode is highly recommended).

Initial setup is covered under [installation](#installation) above, also read the [configuration](#configuration)
section.

Upgrades:

    . ./activate.sh  # if not done yet
    git pull         # if git is not updated yet
    invoke deploy

uWSGI will reload workers automatically.

Rollbacks:

    . ./activate.sh
    invoke deploy --commit HEAD~1  # or another known-good commit
    # alternatively a short notation can be used:
    inv deploy -c HEAD~1

Note that rollbacks are very fast (basically changing a symlink and
waiting for uWSGI to restart workers), unless you deleted the old tree
in the `trees/` directory. Database migrations are also run.

For advanced configurations, see
[The Art Of Graceful Reloading](http://uwsgi-docs.readthedocs.io/en/latest/articles/TheArtOfGracefulReloading.html).

## <a name="opts"></a> Storage backends

Available storage backends:

- S3

    S3 is the default and requires AWS credentials in `~/.aws`, the
    [boto docs](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration) document details and
    alternatives.

- Local storage

    Local storage requires nothing special, just a file system. The option is `--local-storage` (on the command line)
    and takes the directory to store files in as a sole parameter.

    Note: ETags are currently generated from the mtime, therefore the
    filesystem should support high resolution (nanosecond) timestamps for
    production systems. I.e. no OSX, FAT etc.

- Dummy (temporary), `--dummy`, requires no parameters and is an amnesiac.

## Options reference

(from `python run.py --help`)

    Block server options:

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
