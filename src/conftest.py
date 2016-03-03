import pytest
import tempfile
import os
import random
import string
import blockserver.backend.cache as cache_backends
import blockserver.server
from blockserver.backend import transfer as transfer_module
from pytest_dbfixtures.factories.postgresql import init_postgresql_database
from pytest_dbfixtures.utils import try_import

from glinda.testing import services
from tornado.options import options

from blockserver.backend.database import PostgresUserDatabase


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
def file_path():
    return '/test/' + ''.join(random.choice(string.ascii_lowercase + string.digits)
                          for _ in range(12))


@pytest.fixture
def auth_path(file_path):
    return '/api/v0/auth' + file_path


@pytest.fixture
def path(base_url, file_path):
    return base_url + '/api/v0/files' + file_path


@pytest.yield_fixture
def backend(request, cache):
    switch_to = (request.param == 'dummy')
    before = options.dummy
    options.dummy = switch_to
    if options.dummy:
        transfer.files = {}
    yield
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


@pytest.fixture(scope='session')
def pg_connection(request, postgresql_proc):
    psycopg2, config = try_import('psycopg2', request)
    pg_host = postgresql_proc.host
    pg_port = postgresql_proc.port
    pg_db = config.postgresql.db

    init_postgresql_database(
            psycopg2, config.postgresql.user, pg_host, pg_port, pg_db
    )
    conn = psycopg2.connect(
            dbname=pg_db,
            user=config.postgresql.user,
            host=pg_host,
            port=pg_port
    )
    return conn


@pytest.fixture
def pg_db(pg_connection):
    db = PostgresUserDatabase(pg_connection)
    db.drop_db()
    db.init_db()
    return db


@pytest.fixture
def mock_log():
    async def log(*args):
        log.log.append(args)
    log.log = []
    return log


@pytest.yield_fixture
def app(mock_log, cache):
    prev_auth = options.dummy_auth
    prev_log = options.dummy_log
    options.dummy_auth = 'MAGICFARYDUST'
    options.dummy_log = True
    yield blockserver.server.make_app(
            cache_cls=lambda: (lambda: cache),
            log_callback=lambda: mock_log if options.dummy_log else blockserver.server.send_log,
            debug=True)
    options.dummy_auth = prev_auth
    options.dummy_log = prev_log


@pytest.yield_fixture()
def transfer(request, cache):
    transfer_backend = request.param
    if transfer_backend == 'dummy':
        transfer_module.files = {}
        yield transfer_module.DummyTransfer(cache)
        transfer_module.files = {}
    if transfer_backend == 's3':
        yield transfer_module.S3Transfer(cache)


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