import functools
import os
import pytest
import random
import shutil
import string
import tempfile
import traceback
from tempfile import NamedTemporaryFile

import psycopg2
import tornado
from alembic.command import upgrade
from alembic.config import Config
from glinda.testing import services
from pytest_postgresql.factories import get_config, init_postgresql_database
from tornado.options import options

import blockserver.backend.cache as cache_backends
import blockserver.server
from blockserver.backend import quota
from blockserver.backend import transfer as transfer_module
from blockserver.backend.database import PostgresUserDatabase

BASEDIR = os.path.abspath(os.path.dirname(__file__))
ALEMBIC_CONFIG = os.path.join(BASEDIR, 'alembic.ini')

options.s3_bucket = 'qabelbox'


@pytest.fixture
def quota_policy():
    return quota.QuotaPolicy


@pytest.fixture
def auth_token():
    return 'Token MAGICFARYDUST'


@pytest.fixture
def headers(auth_token):
    return {'Authorization': auth_token}


@pytest.yield_fixture
def testfile():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        temp.write(b'Dummy\n')
    yield temp.name
    os.remove(temp.name)


@pytest.fixture
def service_layer():
    return services.ServiceLayer()


@pytest.fixture
def io_loop(request):

    """Create an instance of the `tornado.ioloop.IOLoop` for each test case.
    """
    IOLoop = tornado.platform.asyncio.AsyncIOMainLoop
    io_loop = IOLoop.current()
    io_loop.make_current()

    def _close():
        io_loop.clear_current()
        if not IOLoop.initialized() or io_loop is not IOLoop.instance():
            io_loop.close(all_fds=True)

    request.addfinalizer(_close)
    return io_loop


@pytest.yield_fixture
def auth_server(service_layer):
    prev_auth = options.dummy_auth
    options.dummy_auth = None
    options.dummy_log = False
    dummy_acc = options.accounting_host
    auth = service_layer['auth']  # type: services.Service
    options.accounting_host = 'http://' + auth.host
    yield auth
    options.dummy_auth = prev_auth
    options.dummy_log = True
    options.accounting_host = dummy_acc


@pytest.fixture
def file_path(prefix):
    return '/{}/'.format(prefix) + ''.join(random.choice(string.ascii_lowercase + string.digits)
                                           for _ in range(12))


@pytest.fixture
def prefix_path(base_url):
    return base_url + '/api/v0/prefix/'


@pytest.fixture
def auth_path():
    return '/api/v0/internal/user/'


@pytest.fixture
def block_path(base_url, prefix):
    return base_url + '/api/v0/files/{}/block/foobar'.format(prefix)


@pytest.fixture
def path(base_url, file_path):
    return base_url + '/api/v0/files' + file_path


@pytest.fixture
def quota_path(base_url):
    return base_url + '/api/v0/quota/'


@pytest.yield_fixture
def backend(request, cache):
    switch_to = (request.param == 'dummy')
    before = options.dummy
    options.dummy = switch_to
    yield request.param
    options.dummy = before


@pytest.fixture
def cache(request):
    cache_backend = request.param
    if cache_backend == 'dummy':
        cache_object = cache_backends.DummyCache()
    elif cache_backend == 'redis':
        cache_object = cache_backends.RedisCache(host='localhost', port='6379')
    else:
        raise ValueError('Unknown backend')
    cache_object.flush()
    return cache_object


def apply_migrations(user, host, port, db):
    os.chdir(BASEDIR)
    config = Config(ALEMBIC_CONFIG)

    url = "postgresql://{}@{}:{}/{}".format(
        user, host, port, db)
    os.environ['BLOCK_DATABASE_URI'] = url
    upgrade(config, 'head')


@pytest.fixture(scope='session')
def pg_connection(request, postgresql_proc):
    config = get_config(request)
    pg_user = config['user']
    pg_host = postgresql_proc.host
    pg_port = postgresql_proc.port
    pg_db = config['db'] = 'tests'

    init_postgresql_database(pg_user, pg_host, pg_port, pg_db)
    apply_migrations(pg_user, pg_host, pg_port, pg_db)
    conn = psycopg2.connect(dbname=pg_db, user=pg_user,
                            host=pg_host, port=pg_port)
    return conn


@pytest.fixture
def pg_pool(pg_connection):
    class TestPool:

        def getconn(self):
            return pg_connection

        def putconn(self, conn):
            pass
    return TestPool()


@pytest.fixture
def pg_db(pg_connection):
    db = PostgresUserDatabase(pg_connection)
    db._flush_all()
    return db


@pytest.yield_fixture
def app(cache, pg_pool, tmpdir):
    prev_auth = options.dummy_auth
    prev_log = options.dummy_log
    prev_dummy = options.dummy
    prev_lost = options.local_storage
    options.dummy_auth = 'MAGICFARYDUST'
    options.dummy_log = True
    if options.dummy:
        # http_client fixture creates a new app for *every* request by default, therefore dummy mode wouldn't work.
        options.dummy = False
        options.local_storage = str(tmpdir)
    yield blockserver.server.make_app(
        cache_cls=lambda: (lambda: cache),
        database_pool=pg_pool,
        debug=True)
    options.dummy_auth = prev_auth
    options.dummy_log = prev_log
    options.dummy = prev_dummy
    options.local_storage = prev_lost


@pytest.yield_fixture()
def app_options():
    original_settings = dict(options.items())
    yield options
    for key, value in original_settings.items():
        setattr(options, key, value)


@pytest.yield_fixture()
def transfer(request, cache, tmpdir):
    transfer_backend = request.param
    if transfer_backend == 'dummy':
        tmpdir = str(tmpdir)
        yield transfer_module.LocalTransfer(tmpdir, cache)
        shutil.rmtree(tmpdir)
    if transfer_backend == 's3':
        yield transfer_module.S3Transfer(cache)


@pytest.fixture
def user_id():
    return 0


@pytest.fixture
def prefix(pg_db, user_id):
    return pg_db.create_prefix(user_id)


@pytest.fixture
def temp_check(monkeypatch):
    """
    Check for leftover NamedTemporaryFile()s. Use as a context manager, or the forget() / assert_clean()
    methods.
    """
    temp_files = []

    @functools.wraps(NamedTemporaryFile)
    def named_temp_file(*args, **kwargs):
        temp = NamedTemporaryFile(*args, **kwargs)
        temp_files.append((temp, traceback.format_stack(limit=6)))
        return temp

    class TempCheckMethods:
        class no_new_temp:
            def __enter__(self):
                self.oldtmp = temp_files

            def __exit__(self, exc_type, exc_val, exc_tb):
                assert temp_files == self.oldtmp

        def assert_clean(self):
            assert temp_files, "No temporary files created since forget(), but assert_clean() called"
            for temp, stacktrace in temp_files:
                if os.path.exists(temp.name):
                    print(''.join(stacktrace))
                    # the below assert also makes py.test print the temporary path (among other things)
                    assert not os.path.exists(temp.name)

        def forget(self):
            temp_files.clear()

        __enter__ = forget

        def __exit__(self, exc_type, exc_val, exc_tb):
            assert temp_files, "No temporary files created in 'with temp_check' block"
            self.assert_clean()

    monkeypatch.setattr(tempfile, 'NamedTemporaryFile', named_temp_file)
    return TempCheckMethods()


def pytest_configure(config):
    tornado.platform.asyncio.AsyncIOMainLoop().install()


def pytest_addoption(parser):
    parser.addoption("--dummy", action="store_true",
                     help="run only with the dummy backend")
    parser.addoption("--dummy-cache", action="store_true",
                     help="run only with the dummy cache")


def pytest_generate_tests(metafunc):
    if 'backend' in metafunc.fixturenames:
        backends = ['dummy']
        if not metafunc.config.option.dummy:
            backends.append('s3')
        metafunc.parametrize("backend", backends, indirect=True)
    if 'transfer' in metafunc.fixturenames:
        backends = ['dummy']
        if not metafunc.config.option.dummy:
            backends.append('s3')
        metafunc.parametrize("transfer", backends, indirect=True)
    if 'cache' in metafunc.fixturenames:
        backends = ['dummy']
        if not metafunc.config.option.dummy_cache:
            backends.append('redis')
        metafunc.parametrize("cache", backends, indirect=True)


def make_coroutine(mock):
    async def coroutine(*args, **kwargs):
        return mock(*args, **kwargs)

    return coroutine
